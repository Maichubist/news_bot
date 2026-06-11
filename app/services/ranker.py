from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Mapping, Optional

log = logging.getLogger("services.ranker")

LAST_CYCLE_STATE_KEY = "ranking_last_cycle_utc"

# Can be overridden at runtime via /prompt_set ranking_prompt.
DEFAULT_RANKING_PROMPT = """Ти головний редактор українського новинного Telegram-каналу.
Нижче — пул кандидатів (новини, що пройшли дедуплікацію і первинну класифікацію).
Обери {max_picks} найкращі для публікації ПРОСТО ЗАРАЗ. Поверни лише JSON.

Критерії вибору (за пріоритетом):
1) Важливість і вплив події — реальний факт, а не шум.
2) Свіжість — менший вік у хвилинах кращий за рівних умов.
3) Баланс Україна/світ: поточна частка опублікованого — ua {ua_share}%, world {world_share}%
   (ціль ~50/50). Якщо один origin домінує, віддавай перевагу іншому.
4) Різноманітність тем: останні опубліковані topic_key: {recent_topics}.
   Не обирай кандидата з тим самим topic_key, якщо є гідна альтернатива.
5) Підтвердження кількома джерелами (source_count) підсилює кандидата.

Якщо гідних кандидатів менше за {max_picks} — обери менше або нікого (порожній picks).

Кандидати:
{candidates}

Формат JSON:
- picks: масив обраних, кожен {{"id": <id кандидата>, "reason": "<1 речення чому>"}}
- reasoning: 2-4 речення про логіку всього вибору"""


@dataclass(frozen=True)
class RankResult:
    picks: List[Dict[str, Any]]  # [{"id": int, "reason": str}]
    reasoning: str = ""


@dataclass
class RankCycleStats:
    ran: bool = False
    candidates: int = 0
    picked: int = 0
    processed: int = 0
    expired: int = 0
    used_fallback: bool = False
    pick_ids: List[int] = field(default_factory=list)


class RankingLLM:
    """Single strict-JSON ranking call. Retries once on broken JSON, then gives up
    (the coordinator falls back to the legacy threshold path for that cycle)."""

    def __init__(self, http, api_key: str, model: str, prompt_provider: Callable[[str], str] | None = None):
        self.http = http
        self.api_key = api_key
        self.model = model
        self.prompt_provider = prompt_provider

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    def _build_prompt(self, candidates: List[Mapping[str, Any]], context: Mapping[str, Any], max_picks: int) -> str:
        lines = []
        for c in candidates:
            lines.append(
                f"id={c['id']} | {c.get('title','')}\n"
                f"  факт: {c.get('fact_summary') or '—'}\n"
                f"  джерело: {c.get('source','')} | origin: {c.get('origin','world')} | "
                f"вік: {int(c.get('age_minutes', 0))} хв | джерел у кластері: {int(c.get('source_count', 1))} | "
                f"тема: {c.get('topic_key') or '—'}"
            )
        template = ""
        if self.prompt_provider:
            template = (self.prompt_provider("ranking_prompt") or "").strip()
        template = template or DEFAULT_RANKING_PROMPT
        return template.format(
            max_picks=int(max_picks),
            ua_share=int(round(float(context.get("ua_share", 0.0)) * 100)),
            world_share=int(round(float(context.get("world_share", 0.0)) * 100)),
            recent_topics=", ".join(context.get("recent_topics") or []) or "—",
            candidates="\n\n".join(lines),
        )

    def rank(self, candidates: List[Mapping[str, Any]], context: Mapping[str, Any], max_picks: int) -> Optional[RankResult]:
        if not candidates:
            return RankResult(picks=[], reasoning="no candidates")
        prompt = self._build_prompt(candidates, context, max_picks)
        schema: Dict[str, Any] = {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "picks": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "id": {"type": "integer"},
                            "reason": {"type": "string"},
                        },
                        "required": ["id", "reason"],
                    },
                },
                "reasoning": {"type": "string"},
            },
            "required": ["picks", "reasoning"],
        }
        payload: Dict[str, Any] = {
            "model": self.model,
            "input": [{"role": "user", "content": [{"type": "input_text", "text": prompt}]}],
            "text": {
                "format": {
                    "type": "json_schema",
                    "name": "ranking_picks_v1",
                    "schema": schema,
                    "strict": True,
                }
            },
        }

        valid_ids = {int(c["id"]) for c in candidates}
        for attempt in (1, 2):  # retry broken JSON exactly once
            obj = self._call(payload)
            if obj is None:
                log.warning("Ranking LLM call failed (attempt %d/2)", attempt)
                continue
            try:
                picks = []
                for p in (obj.get("picks") or [])[: int(max_picks)]:
                    pid = int(p.get("id"))
                    if pid not in valid_ids:
                        log.warning("Ranking LLM picked unknown id=%s, dropping", pid)
                        continue
                    picks.append({"id": pid, "reason": str(p.get("reason") or "").strip()})
                return RankResult(picks=picks, reasoning=str(obj.get("reasoning") or "").strip())
            except Exception as ex:
                log.warning("Ranking LLM response mapping failed (attempt %d/2): %s", attempt, ex)
        return None

    def _call(self, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            r = self.http.post("https://api.openai.com/v1/responses", json=payload, headers=self._headers())
        except Exception as ex:
            log.warning("Ranking request crashed: %s", ex)
            return None
        if not getattr(r, "ok", False):
            log.warning("Ranking request error: %s %s", getattr(r, "status_code", "?"), (getattr(r, "text", "") or "")[:400])
            return None
        try:
            data = r.json()
            out_text = ""
            for item in data.get("output", []):
                for c in item.get("content", []):
                    if c.get("type") == "output_text" and c.get("text"):
                        out_text += c["text"]
            return json.loads((out_text or "").strip())
        except Exception as ex:
            log.warning("Ranking JSON parse failed: %s", ex)
            return None


class NewsRanker:
    """
    Periodic comparative selection over the candidate pool.

    Every cycle_minutes:
      1. collect candidates from the last window_hours;
      2. one LLM call picks top max_picks (importance, freshness, origin balance,
         topic diversity — context numbers go into the prompt);
      3. winners are handed to winner_processor (full post generation + editorial
         post-check with veto rights);
      4. candidates older than max_age_hours are routed to wrap/digest;
      5. the cycle is logged to ranking_log.
    If the LLM returns broken JSON twice, this cycle falls back to the legacy
    absolute-threshold pick so the channel never stalls.
    """

    def __init__(
        self,
        cfg_ranking,
        repo,
        llm: RankingLLM,
        winner_processor: Callable[[str, str], bool],
        expire_router: Callable[[Mapping[str, Any]], None],
        fallback_min_score: float = 0.80,
        origin_lookback_hours: int = 12,
        source_count_fn: Callable[[str, str, Optional[str]], int] | None = None,
    ):
        self.cfg = cfg_ranking
        self.repo = repo
        self.llm = llm
        self.winner_processor = winner_processor
        self.expire_router = expire_router
        self.fallback_min_score = float(fallback_min_score)
        self.origin_lookback_hours = int(origin_lookback_hours)
        # Pluggable event-identity source counter: with clustering enabled the
        # pipeline passes a cluster-aware implementation (Phase 4).
        self.source_count_fn = source_count_fn

        self.enabled = bool(getattr(cfg_ranking, "enabled", False))
        self.cycle_minutes = int(getattr(cfg_ranking, "cycle_minutes", 25) or 25)
        self.window_hours = int(getattr(cfg_ranking, "window_hours", 3) or 3)
        self.max_picks = int(getattr(cfg_ranking, "max_picks", 2) or 2)
        self.max_age_hours = int(getattr(cfg_ranking, "max_age_hours", 8) or 8)

    # ------------------------------------------------------------------

    def maybe_run(self) -> RankCycleStats:
        stats = RankCycleStats()
        if not self.enabled:
            return stats
        now = datetime.now(timezone.utc)
        last = self.repo.get_bot_state(LAST_CYCLE_STATE_KEY, None)
        if last:
            try:
                if now - datetime.fromisoformat(last) < timedelta(minutes=self.cycle_minutes):
                    return stats
            except Exception:
                pass
        stats = self.run_cycle()
        self.repo.set_bot_state(LAST_CYCLE_STATE_KEY, now.isoformat(timespec="seconds"))
        return stats

    def run_cycle(self) -> RankCycleStats:
        stats = RankCycleStats(ran=True)

        candidates = self._collect_candidates()
        stats.candidates = len(candidates)

        picks: List[Dict[str, Any]] = []
        reasoning = ""
        if candidates:
            context = self._build_context()
            result = self.llm.rank(candidates, context, self.max_picks)
            if result is None:
                stats.used_fallback = True
                picks = self._fallback_picks(candidates)
                reasoning = f"fallback: LLM JSON broken twice, threshold {self.fallback_min_score} applied"
                log.warning("Ranking cycle fell back to threshold path (%d picks)", len(picks))
            else:
                picks = result.picks
                reasoning = result.reasoning

        stats.picked = len(picks)
        stats.pick_ids = [int(p["id"]) for p in picks]

        by_id = {int(c["id"]): c for c in candidates}
        for pick in picks:
            cand = by_id.get(int(pick["id"]))
            if cand is None:
                continue
            try:
                ok = self.winner_processor(str(cand["item_hash"]), str(pick.get("reason") or ""))
                if ok:
                    stats.processed += 1
                log.info(
                    "Ranking winner id=%s hash=%s processed=%s reason=%s",
                    pick["id"], str(cand["item_hash"])[:12], ok, (pick.get("reason") or "")[:120],
                )
            except Exception:
                log.exception("Winner processing failed for id=%s", pick["id"])

        stats.expired = self._expire_old_candidates()

        try:
            self.repo.save_ranking_log(
                candidates_json=json.dumps(
                    [
                        {k: c.get(k) for k in ("id", "item_hash", "title", "source", "origin", "fact_summary",
                                                "topic_key", "score", "age_minutes", "source_count")}
                        for c in candidates
                    ],
                    ensure_ascii=False,
                ),
                picks_json=json.dumps(picks, ensure_ascii=False),
                reasoning=reasoning or None,
            )
        except Exception:
            log.exception("Failed to write ranking_log")

        log.info(
            "Ranking cycle: candidates=%d picked=%d processed=%d expired=%d fallback=%s",
            stats.candidates, stats.picked, stats.processed, stats.expired, stats.used_fallback,
        )
        return stats

    # ------------------------------------------------------------------

    def _collect_candidates(self) -> List[Dict[str, Any]]:
        rows = self.repo.get_ranking_candidates(window_hours=self.window_hours)
        now = datetime.now(timezone.utc)
        out: List[Dict[str, Any]] = []
        for r in rows:
            c = dict(r)
            try:
                created = datetime.fromisoformat(str(r.get("created_at_utc")))
                c["age_minutes"] = max(0, int((now - created).total_seconds() // 60))
            except Exception:
                c["age_minutes"] = 0
            try:
                root = str(r.get("same_event_of") or r.get("item_hash"))
                if self.source_count_fn is not None:
                    c["source_count"] = int(self.source_count_fn(str(r["item_hash"]), root, r.get("event_key")))
                else:
                    c["source_count"] = int(self.repo.get_event_source_count(root_hash=root, event_key=r.get("event_key")))
            except Exception:
                c["source_count"] = 1
            out.append(c)
        return out

    def _build_context(self) -> Dict[str, Any]:
        ua_share = world_share = 0.0
        try:
            mix = self.repo.get_origin_share(lookback_hours=self.origin_lookback_hours)
            total = int(mix.get("total", 0) or 0)
            if total > 0:
                ua_share = int(mix.get("ua", 0) or 0) / total
                world_share = int(mix.get("world", 0) or 0) / total
        except Exception:
            log.exception("origin share lookup failed")
        try:
            recent = self.repo.get_recent_posted_topic_keys(limit=10)
        except Exception:
            recent = []
        return {"ua_share": ua_share, "world_share": world_share, "recent_topics": recent}

    def _fallback_picks(self, candidates: List[Mapping[str, Any]]) -> List[Dict[str, Any]]:
        """Legacy path for one cycle: absolute score threshold, best first."""
        eligible = [c for c in candidates if float(c.get("score") or 0.0) >= self.fallback_min_score]
        eligible.sort(key=lambda c: float(c.get("score") or 0.0), reverse=True)
        return [
            {"id": int(c["id"]), "reason": f"fallback-threshold:{float(c.get('score') or 0.0):.2f}"}
            for c in eligible[: self.max_picks]
        ]

    def _expire_old_candidates(self) -> int:
        expired = 0
        try:
            rows = self.repo.get_expired_candidates(max_age_hours=self.max_age_hours)
        except Exception:
            log.exception("Expired candidates lookup failed")
            return 0
        for row in rows:
            try:
                self.expire_router(row)
                expired += 1
            except Exception:
                log.exception("Expire routing failed for %s", str(row.get("item_hash"))[:12])
        return expired

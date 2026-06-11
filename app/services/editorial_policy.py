from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Mapping


CONSEQUENCE_HINTS = {
    "killed", "killeds", "dead", "injured", "injury", "wounded", "damage",
    "shutdown", "outage", "halt", "cuts", "cut", "block", "blocked",
    "withdraw", "withdrawal", "tariff", "sanction", "sanctions", "ban",
    "approved", "rejected", "passed", "signed", "deployed", "evacuated",
    "surged", "fell", "drop", "rose", "slowed", "expanded",
    # --- українські маркери наслідків ---
    "загиблі", "загинули", "загинув", "поранені", "поранено", "постраждалі",
    "збитки", "руйнування", "пошкоджено", "пошкодження",
    "відключення", "знеструмлено", "блекаут",
    "заборона", "заборонено", "санкції", "мито", "тарифи",
    "ухвалено", "схвалено", "відхилено", "підписано",
    "евакуація", "евакуйовано", "мобілізація",
    "зросли", "впали", "подорожчання", "здешевшання", "інфляція",
    "звільнення", "призначення", "відставка",
}
CRITICAL_CATEGORIES = {"war", "politics"}
DEFAULT_HIGH_TRUST = {"reuters", "bloomberg", "financial times", "ft", "associated press", "ap"}


@dataclass(frozen=True)
class OriginBalanceCfg:
    enabled: bool = True
    lookback_hours: int = 12
    max_origin_share: float = 0.60
    min_posts: int = 6
    ua_relevance_boost: float = 0.10


@dataclass(frozen=True)
class EditorialDecision:
    mode: str
    should_post: bool
    status: str
    news_type: str
    has_new_fact: bool
    topic_key: str
    delay_until_utc: str | None
    source_trust: str
    source_count: int
    reasons: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class TopicQuotaCfg:
    window1_hours: int
    max_posts_window1: int
    window2_hours: int
    max_posts_window2: int


@dataclass(frozen=True)
class SaturationCfg:
    lookback_hours: int
    max_topic_share: float
    min_posts: int


@dataclass(frozen=True)
class DelayCfg:
    min_minutes: int
    max_minutes: int


def _safe_set(value: Any) -> set[str]:
    if value is None:
        return set()
    if isinstance(value, str):
        return {value.strip().lower()} if value.strip() else set()
    out: set[str] = set()
    for item in value:
        s = str(item).strip().lower()
        if s:
            out.add(s)
    return out


def _stable_delay_minutes(topic_key: str, min_minutes: int, max_minutes: int) -> int:
    if max_minutes <= min_minutes:
        return int(min_minutes)
    span = max_minutes - min_minutes + 1
    digest = hashlib.sha1(topic_key.encode("utf-8", errors="ignore")).digest()
    offset = int.from_bytes(digest[:2], "big") % span
    return int(min_minutes + offset)


def _parse_iso(ts: str | None) -> datetime | None:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts)
    except Exception:
        return None


def _strip_generic_tokens(parts: Iterable[str]) -> list[str]:
    bad = {
        "update", "updates", "breaking", "news", "latest", "live", "analysis",
        "commentary", "comment", "report", "reports", "says", "say",
    }
    out: list[str] = []
    for part in parts:
        s = str(part).strip().lower()
        if len(s) < 3 or s in bad:
            continue
        out.append(s)
    return out


def normalize_topic_key(event_key: str, title: str, category: str, entities: Iterable[str] | None = None, keywords: Iterable[str] | None = None) -> str:
    ek = re.sub(r"[^a-z0-9\-]+", "-", (event_key or "").strip().lower()).strip("-")
    if ek:
        return ek[:80]

    seed = _strip_generic_tokens(list(entities or []) + list(keywords or []))
    if not seed:
        seed = _strip_generic_tokens(re.findall(r"[a-zA-Zа-яА-ЯіІїЇєЄ0-9][a-zA-Zа-яА-ЯіІїЇєЄ0-9\-']+", title or ""))
    if not seed:
        seed = [category or "other"]
    topic = "-".join(seed[:5])
    topic = re.sub(r"-+", "-", topic).strip("-")
    return topic[:80] or (category or "other")


def has_new_fact(item: Mapping[str, Any], cluster: list[Mapping[str, Any]]) -> bool:
    if not cluster:
        return True

    item_numbers = _safe_set(item.get("numbers"))
    item_verbs = _safe_set(item.get("event_verbs"))
    item_entities = _safe_set(item.get("entities"))
    item_keywords = _safe_set(item.get("keywords"))

    old_numbers: set[str] = set()
    old_verbs: set[str] = set()
    old_entities: set[str] = set()
    old_keywords: set[str] = set()
    for row in cluster:
        old_numbers |= _safe_set(row.get("numbers"))
        old_verbs |= _safe_set(row.get("event_verbs"))
        old_entities |= _safe_set(row.get("entities"))
        old_keywords |= _safe_set(row.get("keywords"))

    if item_numbers - old_numbers:
        return True
    if item_verbs - old_verbs:
        return True
    new_entities = item_entities - old_entities
    if new_entities:
        return True

    consequence_terms = {t for t in item_keywords if t in CONSEQUENCE_HINTS}
    if consequence_terms - old_keywords:
        return True

    # if the article has both a shared topic and at least one new specific keyword, count it as a new angle
    if (item_keywords & old_keywords) and (item_keywords - old_keywords):
        return True

    return False


def topic_post_limit_exceeded(topic_key: str, history: Mapping[str, int], quota_cfg: TopicQuotaCfg) -> bool:
    c1 = int(history.get(f"{quota_cfg.window1_hours}h", 0) or 0)
    c2 = int(history.get(f"{quota_cfg.window2_hours}h", 0) or 0)
    return c1 >= quota_cfg.max_posts_window1 or c2 >= quota_cfg.max_posts_window2


def source_trust_for(source: str, high_trust_sources: Iterable[str]) -> str:
    source_l = (source or "").strip().lower()
    for token in high_trust_sources:
        t = str(token).strip().lower()
        if t and t in source_l:
            return "high"
    return "normal"


def _is_breaking(decision: Any) -> bool:
    return bool(getattr(decision, "is_breaking", False) or float(getattr(decision, "impact_score", 0.0) or 0.0) >= 0.85)


def decide_publish_mode(
    *,
    decision: Any,
    item_row: Mapping[str, Any],
    cluster_rows: list[Mapping[str, Any]],
    topic_history: Mapping[str, int],
    topic_mix: Mapping[str, Any],
    same_event: bool,
    wrap_rule: Any,
    quota_cfg: TopicQuotaCfg,
    saturation_cfg: SaturationCfg,
    delay_cfg: DelayCfg,
    high_trust_sources: Iterable[str],
    origin: str = "world",
    origin_mix: Mapping[str, int] | None = None,
    origin_cfg: OriginBalanceCfg | None = None,
) -> EditorialDecision:
    now_utc = datetime.now(timezone.utc)
    news_type = str(getattr(decision, "news_type", "noise") or "noise").strip().lower()
    topic_key = normalize_topic_key(
        getattr(decision, "topic_key", "") or getattr(decision, "event_key", ""),
        str(item_row.get("title") or ""),
        str(getattr(decision, "category", "other") or "other"),
        item_row.get("entities") or [],
        item_row.get("keywords") or [],
    )
    source_count = int(item_row.get("source_count") or 1)
    source_trust = source_trust_for(str(item_row.get("source") or ""), high_trust_sources)
    reasons: list[str] = []

    llm_new_fact = bool(getattr(decision, "has_new_fact", False))
    code_new_fact = has_new_fact(item_row, cluster_rows)
    effective_new_fact = code_new_fact or (llm_new_fact and same_event is False)

    category = str(getattr(decision, "category", "other") or "other").strip().lower()
    critical = category in CRITICAL_CATEGORIES
    source_gate_ok = True
    if critical and not (source_trust == "high" or source_count >= 2):
        source_gate_ok = False
        reasons.append("critical-news-needs-high-trust-or-2-sources")

    if news_type in {"noise", "promo"}:
        return EditorialDecision("drop", False, "filtered", news_type, effective_new_fact, topic_key, None, source_trust, source_count, reasons + ["news-type-drop"])
    if news_type in {"commentary", "analysis"}:
        return EditorialDecision("drop", False, "filtered", news_type, effective_new_fact, topic_key, None, source_trust, source_count, reasons + ["non-news-genre"])

    if same_event and not effective_new_fact:
        reasons.append("same-event-without-new-fact")
        if wrap_rule:
            return EditorialDecision("wrap", False, "pending_wrap", news_type, False, topic_key, None, source_trust, source_count, reasons)
        return EditorialDecision("digest", False, "digest_only", news_type, False, topic_key, None, source_trust, source_count, reasons)

    if news_type != "hard_news":
        reasons.append("only-hard-news-can-be-standalone-post")
        if wrap_rule and news_type == "followup":
            return EditorialDecision("wrap", False, "pending_wrap", news_type, effective_new_fact, topic_key, None, source_trust, source_count, reasons)
        return EditorialDecision("digest", False, "digest_only", news_type, effective_new_fact, topic_key, None, source_trust, source_count, reasons)

    if not effective_new_fact:
        reasons.append("new-fact-gate-failed")
        if wrap_rule:
            return EditorialDecision("wrap", False, "pending_wrap", news_type, False, topic_key, None, source_trust, source_count, reasons)
        return EditorialDecision("digest", False, "digest_only", news_type, False, topic_key, None, source_trust, source_count, reasons)

    if topic_post_limit_exceeded(topic_key, topic_history, quota_cfg):
        reasons.append("topic-quota-exceeded")
        if wrap_rule:
            return EditorialDecision("wrap", False, "pending_wrap", news_type, True, topic_key, None, source_trust, source_count, reasons)
        return EditorialDecision("digest", False, "digest_only", news_type, True, topic_key, None, source_trust, source_count, reasons)

    total_posts = int(topic_mix.get("total_posts", 0) or 0)
    dominant_posts = int(topic_mix.get("topic_posts", 0) or 0)
    share = (dominant_posts / total_posts) if total_posts > 0 else 0.0
    if total_posts >= saturation_cfg.min_posts and share > saturation_cfg.max_topic_share:
        reasons.append(f"topic-saturated:{share:.2f}")
        return EditorialDecision("digest", False, "digest_only", news_type, True, topic_key, None, source_trust, source_count, reasons)

    if not source_gate_ok:
        if wrap_rule:
            return EditorialDecision("wrap", False, "pending_wrap", news_type, True, topic_key, None, source_trust, source_count, reasons)
        return EditorialDecision("digest", False, "digest_only", news_type, True, topic_key, None, source_trust, source_count, reasons)

    # --- UA/world balance: soft quota -----------------------------------
    # If this item's origin already dominates recent posts, demote non-breaking
    # items to wrap/digest so the channel keeps roughly the configured mix.
    if (
        origin_cfg is not None
        and origin_cfg.enabled
        and origin_mix is not None
        and not _is_breaking(decision)
    ):
        total = int(origin_mix.get("total", 0) or 0)
        mine = int(origin_mix.get(origin, 0) or 0)
        if total >= origin_cfg.min_posts:
            share = mine / total if total else 0.0
            if share > origin_cfg.max_origin_share:
                reasons.append(f"origin-balance:{origin}:{share:.2f}")
                if wrap_rule:
                    return EditorialDecision("wrap", False, "pending_wrap", news_type, True, topic_key, None, source_trust, source_count, reasons)
                return EditorialDecision("digest", False, "digest_only", news_type, True, topic_key, None, source_trust, source_count, reasons)

    if _is_breaking(decision):
        reasons.append("breaking")
        return EditorialDecision("post", True, "pending_post", news_type, True, topic_key, now_utc.isoformat(timespec="seconds"), source_trust, source_count, reasons)

    delay_minutes = _stable_delay_minutes(topic_key, delay_cfg.min_minutes, delay_cfg.max_minutes)
    delay_until = (now_utc + timedelta(minutes=delay_minutes)).isoformat(timespec="seconds")
    reasons.append(f"delay:{delay_minutes}m")
    return EditorialDecision("post", True, "pending_post", news_type, True, topic_key, delay_until, source_trust, source_count, reasons)

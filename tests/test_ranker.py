from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from app.services.ranker import LAST_CYCLE_STATE_KEY, NewsRanker, RankingLLM

from conftest import seed_item


def ranking_cfg(**overrides):
    cfg = SimpleNamespace(
        enabled=True,
        cycle_minutes=25,
        window_hours=3,
        model="gpt-4o",
        max_picks=2,
        max_age_hours=8,
    )
    for k, v in overrides.items():
        setattr(cfg, k, v)
    return cfg


class FakeRankingLLM:
    """Deterministic stand-in for RankingLLM."""

    def __init__(self, pick_ids=None, fail=False):
        self.pick_ids = pick_ids or []
        self.fail = fail
        self.calls = []

    def rank(self, candidates, context, max_picks):
        self.calls.append({"candidates": candidates, "context": context, "max_picks": max_picks})
        if self.fail:
            return None
        valid = {int(c["id"]) for c in candidates}
        picks = [{"id": i, "reason": f"pick-{i}"} for i in self.pick_ids if i in valid][:max_picks]
        return SimpleNamespace(picks=picks, reasoning="fake reasoning")


def seed_candidate(repo, item_hash, score=0.5, age_minutes=10, category_slug="other", origin="ua"):
    row = seed_item(repo, item_hash=item_hash, status="candidate", category_slug=category_slug,
                    score=score, origin=origin)
    con = repo._connect()
    created = (datetime.now(timezone.utc) - timedelta(minutes=age_minutes)).isoformat(timespec="seconds")
    con.execute(
        "UPDATE news_items SET created_at_utc=?, post_text=NULL, should_post=0, fact_summary='Короткий факт.' WHERE item_hash=?",
        (created, item_hash),
    )
    con.commit()
    return repo.get_item_full(item_hash)


def make_ranker(repo, llm, processed=None, expired=None, **cfg_overrides):
    processed = processed if processed is not None else []
    expired = expired if expired is not None else []
    return NewsRanker(
        cfg_ranking=ranking_cfg(**cfg_overrides),
        repo=repo,
        llm=llm,
        winner_processor=lambda h, reason: processed.append((h, reason)) or True,
        expire_router=lambda row: expired.append(str(row["item_hash"])),
        fallback_min_score=0.80,
    )


# ---------------------------------------------------------------------------
# Deterministic picks
# ---------------------------------------------------------------------------

def test_ranker_processes_llm_picks(repo):
    a = seed_candidate(repo, "cand-a")
    b = seed_candidate(repo, "cand-b")
    seed_candidate(repo, "cand-c")
    processed = []
    llm = FakeRankingLLM(pick_ids=[a["id"], b["id"]])
    ranker = make_ranker(repo, llm, processed=processed)

    stats = ranker.run_cycle()

    assert stats.candidates == 3
    assert stats.picked == 2
    assert stats.processed == 2
    assert [h for h, _ in processed] == ["cand-a", "cand-b"]
    # cycle is logged for calibration
    con = repo._connect()
    log_row = con.execute("SELECT * FROM ranking_log ORDER BY id DESC LIMIT 1").fetchone()
    assert log_row is not None
    assert len(json.loads(log_row["candidates_json"])) == 3
    assert len(json.loads(log_row["picks_json"])) == 2
    assert log_row["reasoning"] == "fake reasoning"


def test_ranker_payload_contains_context_numbers(repo):
    seed_candidate(repo, "cand-a")
    # one posted item -> origin share + recent topics in context
    seed_item(repo, item_hash="posted-1", status="posted")
    repo.mark_posted("posted-1")
    llm = FakeRankingLLM(pick_ids=[])
    ranker = make_ranker(repo, llm)

    ranker.run_cycle()

    call = llm.calls[0]
    cand = call["candidates"][0]
    assert "age_minutes" in cand and "source_count" in cand
    assert "ua_share" in call["context"] and "recent_topics" in call["context"]
    assert call["context"]["recent_topics"] == ["test-topic"]


def test_maybe_run_respects_cycle_interval(repo):
    seed_candidate(repo, "cand-a")
    llm = FakeRankingLLM(pick_ids=[])
    ranker = make_ranker(repo, llm)

    assert ranker.maybe_run().ran is True
    assert ranker.maybe_run().ran is False  # within cycle_minutes

    past = (datetime.now(timezone.utc) - timedelta(minutes=30)).isoformat(timespec="seconds")
    repo.set_bot_state(LAST_CYCLE_STATE_KEY, past)
    assert ranker.maybe_run().ran is True


def test_disabled_ranker_never_runs(repo):
    llm = FakeRankingLLM(pick_ids=[])
    ranker = make_ranker(repo, llm, enabled=False)
    assert ranker.maybe_run().ran is False
    assert llm.calls == []


# ---------------------------------------------------------------------------
# Broken JSON -> retry once -> fallback to legacy threshold for the cycle
# ---------------------------------------------------------------------------

class BrokenJsonHttp:
    def __init__(self):
        self.calls = 0

    def post(self, url, *, json=None, headers=None, data=None, files=None):
        self.calls += 1
        return SimpleNamespace(
            ok=True,
            status_code=200,
            text="",
            json=lambda: {"output": [{"content": [{"type": "output_text", "text": "{not valid json"}]}]},
        )


def test_ranking_llm_retries_broken_json_once_then_gives_up():
    http = BrokenJsonHttp()
    llm = RankingLLM(http=http, api_key="k", model="gpt-4o")

    result = llm.rank([{"id": 1, "title": "t"}], {"ua_share": 0.5, "world_share": 0.5, "recent_topics": []}, max_picks=2)

    assert result is None
    assert http.calls == 2  # exactly one retry


def test_cycle_falls_back_to_threshold_when_llm_fails(repo):
    seed_candidate(repo, "cand-low", score=0.5)
    seed_candidate(repo, "cand-high", score=0.9)
    seed_candidate(repo, "cand-mid", score=0.85)
    processed = []
    ranker = make_ranker(repo, FakeRankingLLM(fail=True), processed=processed)

    stats = ranker.run_cycle()

    assert stats.used_fallback is True
    # legacy threshold 0.80: high + mid pass, best first, low is out
    assert [h for h, _ in processed] == ["cand-high", "cand-mid"]
    assert all("fallback-threshold" in r for _, r in processed)


def test_llm_picks_with_unknown_ids_are_dropped():
    class OkHttp:
        def __init__(self, payload_text):
            self.payload_text = payload_text

        def post(self, url, *, json=None, headers=None, data=None, files=None):
            return SimpleNamespace(
                ok=True, status_code=200, text="",
                json=lambda: {"output": [{"content": [{"type": "output_text", "text": self.payload_text}]}]},
            )

    body = json.dumps({"picks": [{"id": 999, "reason": "ghost"}, {"id": 1, "reason": "ok"}], "reasoning": "r"})
    llm = RankingLLM(http=OkHttp(body), api_key="k", model="gpt-4o")

    result = llm.rank([{"id": 1, "title": "t"}], {"ua_share": 0, "world_share": 0, "recent_topics": []}, max_picks=2)

    assert [p["id"] for p in result.picks] == [1]


# ---------------------------------------------------------------------------
# Expiry: losers survive into wrap/digest
# ---------------------------------------------------------------------------

def test_expired_candidates_are_routed(repo):
    seed_candidate(repo, "cand-old", age_minutes=9 * 60)   # beyond max_age_hours=8
    seed_candidate(repo, "cand-new", age_minutes=10)
    expired = []
    ranker = make_ranker(repo, FakeRankingLLM(pick_ids=[]), expired=expired)

    stats = ranker.run_cycle()

    assert stats.expired == 1
    assert expired == ["cand-old"]

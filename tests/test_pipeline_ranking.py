from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import numpy as np
import pytest

from app.dedup.exact import ExactDeduper
from app.models import NewsItem
from app.services.news_pipeline import NewsPipeline
from app.services.openai_postmaker import ClassifyDecision, PostDecision
from app.services.ranker import NewsRanker

from conftest import get_status
from test_ranker import FakeRankingLLM, ranking_cfg


class FakeEmbedder:
    def embed(self, text):
        return np.ones(8, dtype=np.float32)


class FakeSemantic:
    def find_best_match(self, vec):
        return None, 0.0


class RankingFakePostmaker:
    """classify() + make() with deterministic, configurable results."""

    def __init__(self):
        self.classify_calls = []
        self.make_calls = []
        self.classify_result = ClassifyDecision(
            score=0.7,
            category="other",
            tier="B",
            event_key="evt-1",
            topic_key="topic-1",
            impact_score=0.5,
            ua_relevance_score=0.4,
            news_type="hard_news",
            has_new_fact=True,
            is_breaking=False,
            fact_summary="Сталася подія: ключовий факт.",
        )
        self.make_result = PostDecision(
            score=0.9,
            should_post=True,
            post_text="Повний текст поста.\n\nКонтекст і значення події.",
            why=["важлива подія"],
            category="other",
            tier="B",
            publish_mode="post",
            event_key="evt-1",
            topic_key="topic-1",
            impact_score=0.5,
            ua_relevance_score=0.4,
            news_type="hard_news",
            has_new_fact=True,
            is_breaking=False,
        )

    def classify(self, title, summary, source, url, article_text=None):
        self.classify_calls.append(title)
        return self.classify_result

    def make(self, title, summary, source, url, article_text=None, extra_instruction=None):
        self.make_calls.append({"title": title, "article_text": article_text})
        return self.make_result


def make_cfg(ranking_enabled: bool, wrap_rules=None) -> SimpleNamespace:
    return SimpleNamespace(
        editorial=None,
        digest_hour_local=99,  # never fires in tests
        posting=SimpleNamespace(
            max_posts_per_run=10,
            only_last_hours=24,
            cluster_wait_minutes=5,
            breaking_sources_threshold=3,
            wrap_rules=list(wrap_rules or []),
        ),
        images=SimpleNamespace(og_fetch=False),
        text_processing=SimpleNamespace(enabled=False, matching_window_hours=24, max_candidates=10),
        embeddings=SimpleNamespace(window_hours=24, threshold=0.84, require_good_summary=False),
        articles=SimpleNamespace(fetch_full_text=False, max_chars=3000),
        media=SimpleNamespace(enable_video=True, max_image_bytes=5 * 1024 * 1024, max_video_bytes=20 * 1024 * 1024),
        sources=[],
        analytics=SimpleNamespace(timezone="Europe/Kyiv"),
        app=SimpleNamespace(sleep_between_posts_sec=0, log_level="INFO"),
        openai=SimpleNamespace(model="text-embedding-3-small"),
        db=SimpleNamespace(path=":memory:", keep_days=14),
        categories=[],
        ranking=SimpleNamespace(enabled=ranking_enabled),
    )


def news_item(title="Тестова подія дня", source="Суспільне") -> NewsItem:
    return NewsItem(
        source=source,
        title=title,
        link=f"https://example.com/{abs(hash(title)) % 10_000}",
        summary="Опис події",
        published_at=datetime.now(timezone.utc),
        origin="ua",
    )


def build_pipeline(repo, fake_tg, formatter, publisher, items, ranking_enabled, wrap_rules=None):
    postmaker = RankingFakePostmaker()
    pipeline = NewsPipeline(
        cfg=make_cfg(ranking_enabled, wrap_rules=wrap_rules),
        repo=repo,
        rss=SimpleNamespace(fetch=lambda sources: list(items), http=None),
        exact=ExactDeduper(),
        embedder=FakeEmbedder(),
        semantic=FakeSemantic(),
        tg=fake_tg,
        formatter=formatter,
        postmaker=postmaker,
        digestmaker=None,
        publisher=publisher,
        moderation=None,
    )
    return pipeline, postmaker


def item_hash_for(pipeline, item) -> str:
    return pipeline.exact.make_hash(item.title, item.link)


# ---------------------------------------------------------------------------
# E2E: both flag states
# ---------------------------------------------------------------------------

def test_run_once_legacy_path_when_ranking_off(repo, fake_tg, formatter, publisher):
    item = news_item()
    pipeline, postmaker = build_pipeline(repo, fake_tg, formatter, publisher, [item], ranking_enabled=False)

    pipeline.run_once()

    h = item_hash_for(pipeline, item)
    assert postmaker.classify_calls == []          # cheap pass not used
    assert len(postmaker.make_calls) == 1          # full generation per item, as before
    assert get_status(repo, h) == "pending_post"   # delayed non-breaking post
    row = repo.get_item_full(h)
    assert row["post_text"] == postmaker.make_result.post_text


def test_run_once_collects_candidates_when_ranking_on(repo, fake_tg, formatter, publisher):
    item = news_item()
    pipeline, postmaker = build_pipeline(repo, fake_tg, formatter, publisher, [item], ranking_enabled=True)

    pipeline.run_once()

    h = item_hash_for(pipeline, item)
    assert len(postmaker.classify_calls) == 1
    assert postmaker.make_calls == []              # NO post_text at collect time
    row = repo.get_item_full(h)
    assert row["status"] == "candidate"
    assert row["post_text"] is None
    assert row["fact_summary"] == "Сталася подія: ключовий факт."


def test_ranking_winner_gets_full_post_and_editorial_check(repo, fake_tg, formatter, publisher):
    item = news_item()
    pipeline, postmaker = build_pipeline(repo, fake_tg, formatter, publisher, [item], ranking_enabled=True)
    pipeline.run_once()
    h = item_hash_for(pipeline, item)
    row = repo.get_item_full(h)

    llm = FakeRankingLLM(pick_ids=[row["id"]])
    ranker = NewsRanker(
        cfg_ranking=ranking_cfg(),
        repo=repo,
        llm=llm,
        winner_processor=pipeline.process_ranking_winner,
        expire_router=pipeline._route_expired_candidate,
        fallback_min_score=0.80,
    )
    stats = ranker.run_cycle()

    assert stats.processed == 1
    assert len(postmaker.make_calls) == 1          # full text generated for the winner
    fresh = repo.get_item_full(h)
    assert fresh["status"] == "pending_post"       # editorial approved, delayed post
    assert fresh["post_text"] == postmaker.make_result.post_text
    assert "ranking-pick" in (fresh["decision_reasons_json"] or "")


def test_noise_is_filtered_in_ranking_mode(repo, fake_tg, formatter, publisher):
    item = news_item("Реклама подкасту")
    pipeline, postmaker = build_pipeline(repo, fake_tg, formatter, publisher, [item], ranking_enabled=True)
    postmaker.classify_result = ClassifyDecision(score=0.1, category="other", news_type="promo", fact_summary="")

    pipeline.run_once()

    h = item_hash_for(pipeline, item)
    assert get_status(repo, h) == "filtered"


# ---------------------------------------------------------------------------
# Breaking bypasses the queue
# ---------------------------------------------------------------------------

def test_breaking_with_two_sources_skips_ranking_queue(repo, fake_tg, formatter, publisher):
    item = news_item("Термінова подія", source="Суспільне")
    pipeline, postmaker = build_pipeline(repo, fake_tg, formatter, publisher, [item], ranking_enabled=True)
    postmaker.classify_result = ClassifyDecision(
        score=0.95, category="other", event_key="evt-breaking", topic_key="evt-breaking",
        impact_score=0.9, news_type="hard_news", has_new_fact=True, is_breaking=True,
        fact_summary="Термінова подія.",
    )
    postmaker.make_result = PostDecision(
        score=0.95, should_post=True, post_text="Терміновий пост.", why=[], category="other",
        event_key="evt-breaking", topic_key="evt-breaking", impact_score=0.9,
        news_type="hard_news", has_new_fact=True, is_breaking=True,
    )
    # second source already confirmed the same event_key
    from conftest import seed_item
    seed_item(repo, item_hash="confirm-1", status="new")
    con = repo._connect()
    con.execute("UPDATE news_items SET event_key='evt-breaking', source='НВ' WHERE item_hash='confirm-1'")
    con.commit()

    pipeline.run_once()

    h = item_hash_for(pipeline, item)
    row = repo.get_item_full(h)
    assert row["status"] == "pending_post"          # immediate path, not candidate
    assert len(postmaker.make_calls) == 1
    assert "breaking-bypass-ranking" in (row["decision_reasons_json"] or "")


def test_breaking_with_one_source_waits_in_pool(repo, fake_tg, formatter, publisher):
    item = news_item("Неперевірена термінова подія")
    pipeline, postmaker = build_pipeline(repo, fake_tg, formatter, publisher, [item], ranking_enabled=True)
    postmaker.classify_result = ClassifyDecision(
        score=0.95, category="other", event_key="evt-solo", impact_score=0.9,
        news_type="hard_news", has_new_fact=True, is_breaking=True, fact_summary="Подія.",
    )

    pipeline.run_once()

    h = item_hash_for(pipeline, item)
    assert get_status(repo, h) == "candidate"       # one source -> no bypass
    assert postmaker.make_calls == []


# ---------------------------------------------------------------------------
# Losers survive into wrap/digest
# ---------------------------------------------------------------------------

def wrap_rule_economy():
    return SimpleNamespace(
        key="economy_wrap", title="Market Wrap", categories=["other"],
        min_items=3, lookback_hours=6, cooldown_minutes=90, min_sources=2,
        source_label="Market Wrap", hashtag_slug="other", prompt_template_key="wrap_prompt",
        prompt_template="",
    )


def test_expired_candidate_routed_to_wrap(repo, fake_tg, formatter, publisher):
    pipeline, _ = build_pipeline(
        repo, fake_tg, formatter, publisher, [], ranking_enabled=True, wrap_rules=[wrap_rule_economy()]
    )
    from test_ranker import seed_candidate
    seed_candidate(repo, "cand-old", age_minutes=9 * 60, category_slug="other")

    ranker = NewsRanker(
        cfg_ranking=ranking_cfg(),
        repo=repo,
        llm=FakeRankingLLM(pick_ids=[]),
        winner_processor=pipeline.process_ranking_winner,
        expire_router=pipeline._route_expired_candidate,
    )
    stats = ranker.run_cycle()

    assert stats.expired == 1
    row = repo.get_item_full("cand-old")
    assert row["status"] == "pending_wrap"
    assert row["wrap_name"] == "economy_wrap"
    assert row["should_post"] == 1
    assert (row["post_text"] or "").strip() == "Короткий факт."  # synthesized from fact_summary


def test_expired_candidate_without_wrap_rule_goes_to_digest(repo, fake_tg, formatter, publisher):
    pipeline, _ = build_pipeline(repo, fake_tg, formatter, publisher, [], ranking_enabled=True)
    from test_ranker import seed_candidate
    seed_candidate(repo, "cand-old2", age_minutes=9 * 60, category_slug="war")

    pipeline._route_expired_candidate(repo.get_item_full("cand-old2"))

    row = repo.get_item_full("cand-old2")
    assert row["status"] == "digest_only"
    assert (row["post_text"] or "").strip() != ""   # nothing is lost for the digest

from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.services.news_pipeline import NewsPipeline

from conftest import get_status, seed_item


def make_cfg() -> SimpleNamespace:
    """Minimal config namespace covering everything the pipeline ctor touches."""
    return SimpleNamespace(
        editorial=None,
        digest_hour_local=21,
        posting=SimpleNamespace(
            max_posts_per_run=10,
            only_last_hours=24,
            cluster_wait_minutes=0,  # items are due immediately in tests
            breaking_sources_threshold=3,
            wrap_rules=[],
        ),
        images=SimpleNamespace(og_fetch=False),
        text_processing=None,
        embeddings=SimpleNamespace(window_hours=24, threshold=0.84, require_good_summary=False),
        articles=SimpleNamespace(fetch_full_text=False, max_chars=3000),
        media=SimpleNamespace(enable_video=True, max_image_bytes=5 * 1024 * 1024, max_video_bytes=20 * 1024 * 1024),
        sources=[],
        analytics=SimpleNamespace(timezone="Europe/Kyiv"),
        app=SimpleNamespace(sleep_between_posts_sec=0, log_level="INFO"),
        openai=SimpleNamespace(model="text-embedding-3-small"),
        db=SimpleNamespace(path=":memory:", keep_days=14),
        categories=[],
    )


@pytest.fixture
def pipeline(repo, fake_tg, formatter, publisher, moderation):
    return NewsPipeline(
        cfg=make_cfg(),
        repo=repo,
        rss=SimpleNamespace(http=None),
        exact=None,
        embedder=None,
        semantic=None,
        tg=fake_tg,
        formatter=formatter,
        postmaker=None,
        digestmaker=None,
        publisher=publisher,
        moderation=moderation,
    )


def test_due_item_goes_to_review_when_moderation_on(pipeline, repo, fake_tg):
    row = seed_item(repo)

    posted = pipeline._post_pending_roots(only_last_hours=24)

    assert posted == 0
    assert get_status(repo, row["item_hash"]) == "pending_review"
    assert len(fake_tg.admin_messages()) == 1
    assert fake_tg.channel_posts() == []


def test_due_item_published_directly_when_moderation_off(pipeline, repo, fake_tg, moderation):
    moderation.set_enabled(False)
    row = seed_item(repo)

    posted = pipeline._post_pending_roots(only_last_hours=24)

    assert posted == 1
    assert get_status(repo, row["item_hash"]) == "posted"
    assert len(fake_tg.channel_posts()) == 1
    assert fake_tg.admin_messages() == []


def test_reviewed_item_not_picked_again(pipeline, repo, fake_tg):
    row = seed_item(repo)
    pipeline._post_pending_roots(only_last_hours=24)
    fake_tg.sent.clear()

    # second cycle: the item sits in pending_review and must not be re-sent
    posted = pipeline._post_pending_roots(only_last_hours=24)

    assert posted == 0
    assert fake_tg.sent == []
    assert get_status(repo, row["item_hash"]) == "pending_review"


def test_run_cycle_resolves_timeouts_via_pipeline(pipeline, repo, fake_tg, moderation):
    from conftest import age_review

    row = seed_item(repo)
    pipeline._post_pending_roots(only_last_hours=24)
    age_review(repo, row["item_hash"], minutes=60)
    fake_tg.sent.clear()

    handled = moderation.check_timeouts()  # what run_once does each cycle

    assert handled == 1
    assert get_status(repo, row["item_hash"]) == "rejected"  # on_timeout: skip

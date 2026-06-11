from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import numpy as np
import pytest

from app.dedup.clusters import IncrementalClusterer
from app.dedup.semantic import pack_vec, unpack_vec
from app.services.news_pipeline import DEFAULT_CLUSTER_WRAP_PROMPT, NewsPipeline
from app.storage.sqlite_repo import utc_now_iso

from conftest import CHANNEL_CHAT_ID


def vec(*xs) -> np.ndarray:
    return np.array(xs, dtype=np.float32)


def seed_embedded(
    repo,
    item_hash: str,
    v: np.ndarray,
    source: str = "Суспільне",
    title: str = "Подія",
    article_text: str | None = None,
    published_offset_min: int = 0,
    status: str = "new",
    event_key: str | None = None,
    fact_summary: str | None = None,
):
    published = (datetime.now(timezone.utc) - timedelta(minutes=published_offset_min)).isoformat(timespec="seconds")
    repo.upsert_item(
        item_hash=item_hash, source=source, title=title, link=f"https://example.com/{item_hash}",
        summary="Опис", published_at_utc=published, origin="ua",
    )
    repo.set_embedding_and_dup(
        item_hash=item_hash, embedding_blob=pack_vec(v), embedding_dim=int(v.shape[0]),
        embedding_model="test", dup_of=None, dup_score=None,
    )
    con = repo._connect()
    con.execute(
        "UPDATE news_items SET status=?, article_text=?, event_key=?, fact_summary=?, category_id=(SELECT id FROM categories WHERE slug='war') WHERE item_hash=?",
        (status, article_text, event_key, fact_summary, item_hash),
    )
    con.commit()


# ---------------------------------------------------------------------------
# Incremental clustering
# ---------------------------------------------------------------------------

def test_first_item_opens_cluster(repo):
    clusterer = IncrementalClusterer(repo, threshold=0.80)
    seed_embedded(repo, "i-1", vec(1, 0, 0, 0))

    a = clusterer.assign("i-1", vec(1, 0, 0, 0))

    assert a.created_new is True
    assert repo.get_item_cluster_id("i-1") == a.cluster_id
    clusters = repo.get_active_clusters(window_hours=48)
    assert len(clusters) == 1
    assert clusters[0]["canonical_hash"] == "i-1"


def test_similar_item_joins_and_centroid_updates(repo):
    clusterer = IncrementalClusterer(repo, threshold=0.80)
    seed_embedded(repo, "i-1", vec(1, 0, 0, 0), source="Суспільне")
    seed_embedded(repo, "i-2", vec(0.9, 0.1, 0, 0), source="НВ")
    a1 = clusterer.assign("i-1", vec(1, 0, 0, 0))

    a2 = clusterer.assign("i-2", vec(0.9, 0.1, 0, 0))

    assert a2.created_new is False
    assert a2.cluster_id == a1.cluster_id
    assert a2.similarity >= 0.80
    cl = repo.get_active_clusters(window_hours=48)[0]
    assert cl["item_count"] == 2
    assert cl["source_count"] == 2  # two distinct sources
    centroid = unpack_vec(cl["centroid_blob"])
    np.testing.assert_allclose(centroid, [(1 + 0.9) / 2, 0.05, 0, 0], atol=1e-6)


def test_dissimilar_item_opens_new_cluster(repo):
    clusterer = IncrementalClusterer(repo, threshold=0.80)
    seed_embedded(repo, "i-1", vec(1, 0, 0, 0))
    seed_embedded(repo, "i-2", vec(0, 1, 0, 0))
    a1 = clusterer.assign("i-1", vec(1, 0, 0, 0))

    a2 = clusterer.assign("i-2", vec(0, 1, 0, 0))

    assert a2.created_new is True
    assert a2.cluster_id != a1.cluster_id
    assert len(repo.get_active_clusters(window_hours=48)) == 2


def test_canonical_is_earliest_with_fullest_article(repo):
    clusterer = IncrementalClusterer(repo, threshold=0.80)
    # i-old is earliest but has a short article; i-full is later with the fullest text;
    # i-late has equally full text but is later than i-full.
    seed_embedded(repo, "i-old", vec(1, 0, 0, 0), published_offset_min=120, article_text="abc")
    seed_embedded(repo, "i-full", vec(0.99, 0.05, 0, 0), published_offset_min=60, article_text="x" * 500)
    seed_embedded(repo, "i-late", vec(0.98, 0.06, 0, 0), published_offset_min=10, article_text="y" * 500)

    clusterer.assign("i-old", vec(1, 0, 0, 0))
    clusterer.assign("i-full", vec(0.99, 0.05, 0, 0))
    clusterer.assign("i-late", vec(0.98, 0.06, 0, 0))

    cl = repo.get_active_clusters(window_hours=48)[0]
    assert cl["canonical_hash"] == "i-full"  # fullest article, earliest among equals


# ---------------------------------------------------------------------------
# Migration from event_key logic: decisions go through cluster_id
# ---------------------------------------------------------------------------

def make_cfg(clustering_enabled: bool) -> SimpleNamespace:
    return SimpleNamespace(
        editorial=None,
        digest_hour_local=99,
        posting=SimpleNamespace(
            max_posts_per_run=10, only_last_hours=24, cluster_wait_minutes=5,
            breaking_sources_threshold=3, wrap_rules=[],
        ),
        images=SimpleNamespace(og_fetch=False),
        text_processing=None,
        embeddings=SimpleNamespace(window_hours=24, threshold=0.84, require_good_summary=False),
        articles=SimpleNamespace(fetch_full_text=False, max_chars=3000),
        media=SimpleNamespace(enable_video=True, max_image_bytes=5 * 1024 * 1024, max_video_bytes=20 * 1024 * 1024),
        sources=[],
        analytics=SimpleNamespace(timezone="Europe/Kyiv"),
        app=SimpleNamespace(sleep_between_posts_sec=0, log_level="INFO"),
        openai=SimpleNamespace(model="test"),
        db=SimpleNamespace(path=":memory:", keep_days=14),
        categories=[],
        clustering=SimpleNamespace(
            enabled=clustering_enabled, threshold=0.80, window_hours=48,
            wrap=SimpleNamespace(min_items=3, min_sources=2, lookback_hours=6, cooldown_minutes=90),
        ),
    )


def make_pipeline(repo, fake_tg, formatter, publisher, clustering_enabled: bool, wrapmaker=None):
    return NewsPipeline(
        cfg=make_cfg(clustering_enabled),
        repo=repo,
        rss=SimpleNamespace(fetch=lambda s: [], http=None),
        exact=None,
        embedder=None,
        semantic=None,
        tg=fake_tg,
        formatter=formatter,
        postmaker=None,
        digestmaker=None,
        wrapmaker=wrapmaker,
        publisher=publisher,
        clusterer=IncrementalClusterer(repo, threshold=0.80),
    )


def test_source_count_uses_cluster_when_enabled(repo, fake_tg, formatter, publisher):
    # Same story from two sources, but the LLM gave them DIFFERENT event_keys —
    # the old event_key matching would miss the confirmation, the cluster catches it.
    clusterer = IncrementalClusterer(repo, threshold=0.80)
    seed_embedded(repo, "s-1", vec(1, 0, 0, 0), source="Суспільне", event_key="evt-a")
    seed_embedded(repo, "s-2", vec(0.95, 0.05, 0, 0), source="НВ", event_key="evt-b")
    clusterer.assign("s-1", vec(1, 0, 0, 0))
    clusterer.assign("s-2", vec(0.95, 0.05, 0, 0))

    on = make_pipeline(repo, fake_tg, formatter, publisher, clustering_enabled=True)
    off = make_pipeline(repo, fake_tg, formatter, publisher, clustering_enabled=False)

    assert on._event_source_count("s-1", "s-1", "evt-a") == 2   # cluster-based
    assert off._event_source_count("s-1", "s-1", "evt-a") == 1  # legacy event_key-based


def test_cluster_rows_for_new_fact_gate_use_cluster_members(repo, fake_tg, formatter, publisher):
    clusterer = IncrementalClusterer(repo, threshold=0.80)
    seed_embedded(repo, "s-1", vec(1, 0, 0, 0), source="Суспільне", event_key="evt-a")
    seed_embedded(repo, "s-2", vec(0.95, 0.05, 0, 0), source="НВ", event_key="evt-b")
    clusterer.assign("s-1", vec(1, 0, 0, 0))
    clusterer.assign("s-2", vec(0.95, 0.05, 0, 0))

    on = make_pipeline(repo, fake_tg, formatter, publisher, clustering_enabled=True)
    rows = on._event_cluster_rows("s-1", "s-1")

    assert {r["item_hash"] for r in rows} == {"s-1", "s-2"}


# ---------------------------------------------------------------------------
# Story wraps over clusters (fake LLM)
# ---------------------------------------------------------------------------

class FakeWrapmaker:
    def __init__(self):
        self.calls = []

    def make(self, wrap_name, items, prompt_template=""):
        self.calls.append({"wrap_name": wrap_name, "items": items, "prompt_template": prompt_template})
        return SimpleNamespace(post_text=f"Розвиток сюжету: {wrap_name}")


def seed_story_cluster(repo, n_items=3, sources=("Суспільне", "НВ", "Суспільне")):
    clusterer = IncrementalClusterer(repo, threshold=0.80)
    cid = None
    for i in range(n_items):
        h = f"story-{i}"
        seed_embedded(
            repo, h, vec(1, 0.01 * i, 0, 0), source=sources[i % len(sources)],
            title=f"Подія, частина {i + 1}",
            article_text="text " * (i + 1),
            published_offset_min=(n_items - i) * 30,
            status="pending_wrap" if i % 2 == 0 else "digest_only",
            fact_summary=f"Факт {i + 1}.",
        )
        con = repo._connect()
        con.execute("UPDATE news_items SET post_text=?, should_post=1 WHERE item_hash=?", (f"Факт {i + 1}.", h))
        con.commit()
        a = clusterer.assign(h, vec(1, 0.01 * i, 0, 0))
        cid = a.cluster_id
    return cid


def test_cluster_wrap_posts_story(repo, fake_tg, formatter, publisher):
    wrapmaker = FakeWrapmaker()
    pipeline = make_pipeline(repo, fake_tg, formatter, publisher, clustering_enabled=True, wrapmaker=wrapmaker)
    cid = seed_story_cluster(repo)

    posted = pipeline._process_wraps(limit=5)

    assert posted == 1
    # one story post went to the channel
    channel = fake_tg.channel_posts()
    assert len(channel) == 1
    assert "Розвиток сюжету" in channel[0]["text"]
    # chronology went into the prompt in time order with facts
    call = wrapmaker.calls[0]
    assert call["prompt_template"] == DEFAULT_CLUSTER_WRAP_PROMPT
    assert [it["item_hash"] for it in call["items"]] == ["story-0", "story-1", "story-2"]
    assert call["items"][0]["summary"] == "Факт 1."
    # members marked wrapped; wrap saved under the cluster key
    con = repo._connect()
    statuses = {r["status"] for r in con.execute("SELECT status FROM news_items WHERE item_hash LIKE 'story-%'").fetchall()}
    assert statuses == {"wrapped"}
    wrap_row = con.execute("SELECT wrap_name FROM market_wrap_posts ORDER BY id DESC LIMIT 1").fetchone()
    assert wrap_row["wrap_name"] == f"cluster:{cid}"


def test_small_cluster_does_not_wrap(repo, fake_tg, formatter, publisher):
    wrapmaker = FakeWrapmaker()
    pipeline = make_pipeline(repo, fake_tg, formatter, publisher, clustering_enabled=True, wrapmaker=wrapmaker)
    seed_story_cluster(repo, n_items=2, sources=("Суспільне", "НВ"))  # below min_items=3

    posted = pipeline._process_wraps(limit=5)

    assert posted == 0
    assert wrapmaker.calls == []


def test_cluster_wrap_respects_cooldown(repo, fake_tg, formatter, publisher):
    wrapmaker = FakeWrapmaker()
    pipeline = make_pipeline(repo, fake_tg, formatter, publisher, clustering_enabled=True, wrapmaker=wrapmaker)
    cid = seed_story_cluster(repo)
    repo.save_wrap_post(f"cluster:{cid}", item_hashes=["x"], source_count=2, post_text="попередній wrap")

    posted = pipeline._process_wraps(limit=5)

    assert posted == 0  # cooldown_minutes=90 has not passed


def test_category_wraps_unchanged_when_clustering_off(repo, fake_tg, formatter, publisher):
    wrapmaker = FakeWrapmaker()
    pipeline = make_pipeline(repo, fake_tg, formatter, publisher, clustering_enabled=False, wrapmaker=wrapmaker)
    seed_story_cluster(repo)

    posted = pipeline._process_wraps(limit=5)

    # no category wrap_rules configured -> legacy path does nothing, clusters ignored
    assert posted == 0
    assert wrapmaker.calls == []

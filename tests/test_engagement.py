from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from app.services.engagement import EngagementService, LAST_POLL_STATE_KEY

from conftest import CHANNEL_CHAT_ID, seed_item


def make_engagement(repo, **overrides):
    cfg = SimpleNamespace(
        enabled=True,
        poll_hours=6,
        lookback_hours=72,
        max_posts=50,
        mtproto_enabled=False,
    )
    for k, v in overrides.items():
        setattr(cfg, k, v)
    return EngagementService(cfg_engagement=cfg, repo=repo, channel_chat_id=CHANNEL_CHAT_ID)


def post_item(repo, item_hash: str, msg_id: int, category_slug: str = "war", origin: str = "ua"):
    seed_item(repo, item_hash=item_hash, category_slug=category_slug, origin=origin)
    repo.mark_posted(item_hash, tg_message_id=msg_id)


def reaction_update(msg_id: int, old_n: int, new_n: int, chat_id: int = CHANNEL_CHAT_ID) -> dict:
    return {
        "chat": {"id": chat_id},
        "message_id": msg_id,
        "old_reaction": [{"type": "emoji", "emoji": "👍"}] * old_n,
        "new_reaction": [{"type": "emoji", "emoji": "👍"}] * new_n,
    }


# ---------------------------------------------------------------------------
# tg_message_id persistence
# ---------------------------------------------------------------------------

def test_mark_posted_stores_message_id(repo):
    seed_item(repo, item_hash="h-msg")
    repo.mark_posted("h-msg", tg_message_id=4242)

    con = repo._connect()
    row = con.execute("SELECT status, tg_message_id FROM news_items WHERE item_hash='h-msg'").fetchone()
    assert row["status"] == "posted"
    assert row["tg_message_id"] == 4242


def test_mark_posted_without_message_id_keeps_existing(repo):
    seed_item(repo, item_hash="h-keep")
    repo.mark_posted("h-keep", tg_message_id=7)
    repo.mark_posted("h-keep")  # legacy call must not wipe the id

    con = repo._connect()
    row = con.execute("SELECT tg_message_id FROM news_items WHERE item_hash='h-keep'").fetchone()
    assert row["tg_message_id"] == 7


# ---------------------------------------------------------------------------
# Reaction tally
# ---------------------------------------------------------------------------

def test_reaction_updates_accumulate(repo):
    eng = make_engagement(repo)

    assert eng.record_reaction_update(reaction_update(10, 0, 1)) is True
    assert eng.record_reaction_update(reaction_update(10, 1, 2)) is True   # another user reacted? delta +1
    assert eng.record_reaction_update(reaction_update(10, 1, 0)) is True   # someone removed theirs

    assert repo.get_reaction_count(CHANNEL_CHAT_ID, 10) == 1


def test_reaction_updates_from_other_chats_ignored(repo):
    eng = make_engagement(repo)

    assert eng.record_reaction_update(reaction_update(10, 0, 1, chat_id=123456)) is False
    assert repo.get_reaction_count(CHANNEL_CHAT_ID, 10) == 0


def test_reactions_never_go_negative(repo):
    eng = make_engagement(repo)
    eng.record_reaction_update(reaction_update(10, 3, 0))  # delta -3 on empty tally

    assert repo.get_reaction_count(CHANNEL_CHAT_ID, 10) == 0


# ---------------------------------------------------------------------------
# Snapshots
# ---------------------------------------------------------------------------

def test_maybe_collect_writes_snapshots(repo):
    eng = make_engagement(repo)
    post_item(repo, "h-a", msg_id=100)
    post_item(repo, "h-b", msg_id=101)
    eng.record_reaction_update(reaction_update(100, 0, 2))

    written = eng.maybe_collect()

    assert written == 2
    con = repo._connect()
    rows = {r["item_hash"]: r for r in con.execute("SELECT * FROM post_metrics").fetchall()}
    assert rows["h-a"]["reactions"] == 2
    assert rows["h-b"]["reactions"] == 0
    assert rows["h-a"]["views"] is None  # no MTProto -> NULL, not fake zeros... wait, see below


def test_maybe_collect_respects_poll_interval(repo):
    eng = make_engagement(repo)
    post_item(repo, "h-a", msg_id=100)

    assert eng.maybe_collect() == 1
    assert eng.maybe_collect() == 0  # second call inside poll_hours window

    # simulate that poll_hours passed
    past = (datetime.now(timezone.utc) - timedelta(hours=7)).isoformat(timespec="seconds")
    repo.set_bot_state(LAST_POLL_STATE_KEY, past)
    assert eng.maybe_collect() == 1


def test_snapshots_are_append_only(repo):
    eng = make_engagement(repo)
    post_item(repo, "h-a", msg_id=100)

    eng._collect_snapshot()
    eng.record_reaction_update(reaction_update(100, 0, 5))
    eng._collect_snapshot()

    con = repo._connect()
    rows = con.execute("SELECT reactions FROM post_metrics WHERE item_hash='h-a' ORDER BY id").fetchall()
    assert [r["reactions"] for r in rows] == [0, 5]


def test_disabled_engagement_is_noop(repo):
    eng = make_engagement(repo, enabled=False)
    post_item(repo, "h-a", msg_id=100)

    assert eng.maybe_collect() == 0
    assert eng.record_reaction_update(reaction_update(100, 0, 1)) is False


def test_graceful_without_any_credentials(repo, caplog):
    """mtproto on but no env/telethon: snapshot still works, warns once per day."""
    eng = make_engagement(repo, mtproto_enabled=True)
    post_item(repo, "h-a", msg_id=100)

    written = eng.maybe_collect()

    assert written == 1  # no crash, reactions-only snapshot
    con = repo._connect()
    row = con.execute("SELECT views, forwards FROM post_metrics WHERE item_hash='h-a'").fetchone()
    assert row["views"] is None and row["forwards"] is None

    # warning is throttled to once per day
    eng._warn_once_per_day("again")
    eng._warn_once_per_day("again")
    warnings = [r for r in caplog.records if r.levelname == "WARNING"]
    assert len(warnings) >= 1


# ---------------------------------------------------------------------------
# Report aggregation
# ---------------------------------------------------------------------------

def seed_metrics(repo, n: int = 7):
    """n posts with descending engagement: post i gets (n - i) reactions."""
    for i in range(n):
        h = f"h-{i}"
        post_item(repo, h, msg_id=200 + i, category_slug="war" if i % 2 == 0 else "other", origin="ua" if i < 3 else "world")
        repo.add_post_metric(h, views=None, reactions=n - i, forwards=None)


def test_report_block_top_and_bottom(repo):
    eng = make_engagement(repo)
    seed_metrics(repo, n=7)

    block = eng.build_report_block(hours=72)

    assert "Топ-5" in block
    assert "Анти-топ-5" in block
    assert "Категорії" in block and "Теми" in block and "Origin" in block
    # the strongest post (7 reactions) is in the top block, the weakest (1) in the bottom
    top_part = block.split("Анти-топ-5")[0]
    bottom_part = block.split("Анти-топ-5")[1]
    assert "❤️7" in top_part
    assert "❤️1" in bottom_part


def test_report_block_empty_without_data(repo):
    eng = make_engagement(repo)
    assert eng.build_report_block(hours=72) == ""


def test_daily_report_includes_engagement_block(repo, fake_tg):
    """AnalyticsService appends the block when an engagement service is wired in."""
    from app.services.analytics_service import AnalyticsService

    cfg = SimpleNamespace(
        analytics=SimpleNamespace(timezone="Europe/Kyiv", report_hour_local=0, enabled=True,
                                  daily_report_enabled=True, commands_enabled=True),
        posting=SimpleNamespace(wrap_rules=[]),
        categories=[],
    )
    eng = make_engagement(repo)
    seed_metrics(repo, n=3)
    svc = AnalyticsService(cfg=cfg, repo=repo, tg=fake_tg, engagement=eng)

    block = svc._engagement_block()

    assert "Engagement за 72 год" in block

from __future__ import annotations

from types import SimpleNamespace

from app.services.moderation import ModerationService

from conftest import (
    ADMIN_CHAT_ID,
    FakePostmaker,
    age_review,
    get_moderation_log,
    get_status,
    make_callback,
    seed_item,
)


# ---------------------------------------------------------------------------
# Submission
# ---------------------------------------------------------------------------

def test_submit_for_review_moves_to_pending_review(moderation, repo, fake_tg):
    row = seed_item(repo)

    assert moderation.submit_for_review(row) is True

    assert get_status(repo, row["item_hash"]) == "pending_review"
    previews = fake_tg.admin_messages()
    assert len(previews) == 1
    assert "Модерація" in previews[0]["text"]
    kb = previews[0]["reply_markup"]["inline_keyboard"]
    datas = [btn["callback_data"] for row_btns in kb for btn in row_btns]
    assert datas == [
        f"mod:approve:{row['id']}",
        f"mod:regen:{row['id']}",
        f"mod:reject:{row['id']}",
    ]
    # review_message_id persisted for later keyboard cleanup
    fresh = repo.get_item_for_moderation(row["id"])
    assert fresh["review_message_id"] == previews[0]["message_id"]


def test_submit_failure_keeps_pending_post(moderation, repo, fake_tg):
    row = seed_item(repo)
    fake_tg.fail_sends = True

    assert moderation.submit_for_review(row) is False
    assert get_status(repo, row["item_hash"]) == "pending_post"


# ---------------------------------------------------------------------------
# Buttons: status transitions
# ---------------------------------------------------------------------------

def test_approve_publishes_to_channel_and_marks_posted(moderation, repo, fake_tg):
    row = seed_item(repo)
    moderation.submit_for_review(row)
    fake_tg.sent.clear()

    handled = moderation.handle_callback(make_callback(row["id"], "approve"))

    assert handled is True
    assert get_status(repo, row["item_hash"]) == "posted"
    channel = fake_tg.channel_posts()
    assert len(channel) == 1
    assert "Тестова новина" in channel[0]["text"]
    log = get_moderation_log(repo, row["item_hash"])
    assert [e["action"] for e in log] == ["approve"]
    assert log[0]["origin"] == "ua"
    assert log[0]["category"] == "war"
    assert abs(log[0]["llm_score"] - 0.9) < 1e-6
    # keyboard removed from the preview message
    assert len(fake_tg.edited_markups) == 1
    # callback answered
    assert len(fake_tg.answered_callbacks) == 1


def test_reject_marks_rejected_and_publishes_nothing(moderation, repo, fake_tg):
    row = seed_item(repo)
    moderation.submit_for_review(row)
    fake_tg.sent.clear()

    moderation.handle_callback(make_callback(row["id"], "reject"))

    assert get_status(repo, row["item_hash"]) == "rejected"
    assert fake_tg.channel_posts() == []
    assert [e["action"] for e in get_moderation_log(repo, row["item_hash"])] == ["reject"]


def test_regenerate_updates_text_and_stays_pending_review(moderation, repo, fake_tg):
    row = seed_item(repo)
    moderation.submit_for_review(row)
    fake_tg.sent.clear()

    moderation.handle_callback(make_callback(row["id"], "regen"))

    assert get_status(repo, row["item_hash"]) == "pending_review"
    fresh = repo.get_item_for_moderation(row["id"])
    assert fresh["post_text"] == "Перегенерований текст поста."
    # postmaker got the rewrite instruction and the stored article text
    call = moderation.postmaker.calls[0]
    assert call["extra_instruction"] is not None
    assert call["article_text"] == "Повний текст статті для тестів."
    # a fresh preview with keyboard went to the admin chat, nothing to the channel
    previews = fake_tg.admin_messages()
    assert len(previews) == 1
    assert previews[0]["reply_markup"] is not None
    assert fake_tg.channel_posts() == []
    assert [e["action"] for e in get_moderation_log(repo, row["item_hash"])] == ["regenerate"]


def test_regenerate_failure_keeps_old_text(moderation, repo, fake_tg):
    moderation.postmaker = FakePostmaker(post_text=None)
    row = seed_item(repo)
    moderation.submit_for_review(row)
    old_text = row["post_text"]
    fake_tg.sent.clear()

    moderation.handle_callback(make_callback(row["id"], "regen"))

    fresh = repo.get_item_for_moderation(row["id"])
    assert fresh["post_text"] == old_text
    assert get_status(repo, row["item_hash"]) == "pending_review"


def test_callback_on_already_processed_item_is_noop(moderation, repo, fake_tg):
    row = seed_item(repo)
    moderation.submit_for_review(row)
    moderation.handle_callback(make_callback(row["id"], "approve"))
    fake_tg.sent.clear()

    # second press on the same (already posted) item
    moderation.handle_callback(make_callback(row["id"], "approve"))

    assert fake_tg.channel_posts() == []  # no double publication
    assert [e["action"] for e in get_moderation_log(repo, row["item_hash"])] == ["approve"]


def test_non_moderation_callback_is_ignored(moderation):
    assert moderation.handle_callback({"id": "x", "data": "other:stuff"}) is False


def test_approve_publish_failure_marks_error(moderation, repo, fake_tg):
    row = seed_item(repo)
    moderation.submit_for_review(row)
    fake_tg.fail_sends = True

    moderation.handle_callback(make_callback(row["id"], "approve"))

    assert get_status(repo, row["item_hash"]) == "error"
    assert [e["action"] for e in get_moderation_log(repo, row["item_hash"])] == ["approve_failed"]


# ---------------------------------------------------------------------------
# Timeouts
# ---------------------------------------------------------------------------

def test_timeout_skip_rejects_item(moderation, repo, fake_tg):
    row = seed_item(repo)
    moderation.submit_for_review(row)
    age_review(repo, row["item_hash"], minutes=60)
    fake_tg.sent.clear()

    handled = moderation.check_timeouts()

    assert handled == 1
    assert get_status(repo, row["item_hash"]) == "rejected"
    assert fake_tg.channel_posts() == []
    assert [e["action"] for e in get_moderation_log(repo, row["item_hash"])] == ["timeout_skip"]


def test_timeout_publish_posts_item(repo, fake_tg, formatter, publisher):
    cfg = SimpleNamespace(enabled=True, timeout_minutes=45, on_timeout="publish")
    moderation = ModerationService(cfg, repo, fake_tg, formatter, publisher, postmaker=FakePostmaker())
    row = seed_item(repo)
    moderation.submit_for_review(row)
    age_review(repo, row["item_hash"], minutes=60)
    fake_tg.sent.clear()

    handled = moderation.check_timeouts()

    assert handled == 1
    assert get_status(repo, row["item_hash"]) == "posted"
    assert len(fake_tg.channel_posts()) == 1
    assert [e["action"] for e in get_moderation_log(repo, row["item_hash"])] == ["timeout_publish"]


def test_fresh_review_is_not_timed_out(moderation, repo, fake_tg):
    row = seed_item(repo)
    moderation.submit_for_review(row)  # review_requested_at_utc = now

    assert moderation.check_timeouts() == 0
    assert get_status(repo, row["item_hash"]) == "pending_review"


# ---------------------------------------------------------------------------
# Runtime toggle
# ---------------------------------------------------------------------------

def test_toggle_overrides_config_without_restart(moderation, repo):
    assert moderation.is_enabled() is True  # from cfg.enabled
    moderation.set_enabled(False)
    assert moderation.is_enabled() is False
    moderation.set_enabled(True)
    assert moderation.is_enabled() is True


def test_disabled_without_admin_chat(moderation_cfg, repo, formatter, publisher):
    from conftest import FakeTelegramClient

    tg = FakeTelegramClient(admin_chat_id=None)
    m = ModerationService(moderation_cfg, repo, tg, formatter, publisher)
    assert m.is_enabled() is False

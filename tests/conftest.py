from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from app.services.moderation import ModerationService
from app.services.publisher import ChannelPublisher
from app.storage.sqlite_repo import SqliteNewsRepository, utc_now_iso
from app.telegram.formatter import PostFormatter


CHANNEL_CHAT_ID = -100500
ADMIN_CHAT_ID = 999


class DummyHttp:
    """Raises on any use: tests must not hit the network."""

    def get(self, *a, **kw):
        raise AssertionError("Unexpected HTTP GET in test")

    def post(self, *a, **kw):
        raise AssertionError("Unexpected HTTP POST in test")


class FakeTelegramClient:
    """Records every send; mimics the real client's return values."""

    def __init__(self, admin_chat_id: int | None = ADMIN_CHAT_ID):
        self.chat_id = CHANNEL_CHAT_ID
        self.admin_chat_id = admin_chat_id
        self.sent = []  # dicts with kind/text/chat_id/reply_markup/message_id
        self.answered_callbacks = []
        self.edited_markups = []
        self.fail_sends = False
        self._next_msg_id = 100

    def _mk_id(self) -> int:
        self._next_msg_id += 1
        return self._next_msg_id

    def _record(self, kind: str, text: str, chat_id, reply_markup):
        mid = self._mk_id()
        self.sent.append(
            {
                "kind": kind,
                "text": text,
                "chat_id": chat_id if chat_id is not None else self.chat_id,
                "reply_markup": reply_markup,
                "message_id": mid,
            }
        )
        return True, mid

    def send_message_with_id(self, text, disable_preview=False, chat_id=None, reply_markup=None):
        if self.fail_sends:
            return False, None
        return self._record("message", text, chat_id, reply_markup)

    def send_photo_with_id(self, photo_bytes, caption_text, disable_preview=True, filename="image.jpg", chat_id=None, reply_markup=None):
        if self.fail_sends:
            return False, None
        return self._record("photo", caption_text, chat_id, reply_markup)

    def send_video_with_id(self, video_bytes, caption_text, filename="video.mp4", chat_id=None, reply_markup=None):
        if self.fail_sends:
            return False, None
        return self._record("video", caption_text, chat_id, reply_markup)

    def send_document(self, path, caption="", chat_id=None):
        return True, None

    def answer_callback_query(self, callback_query_id, text=None, show_alert=False):
        self.answered_callbacks.append({"id": callback_query_id, "text": text})
        return True

    def edit_message_reply_markup(self, chat_id, message_id, reply_markup=None):
        self.edited_markups.append({"chat_id": chat_id, "message_id": message_id, "reply_markup": reply_markup})
        return True

    def get_updates(self, offset=None, timeout=0):
        return []

    # Helpers for assertions
    def channel_posts(self):
        return [m for m in self.sent if m["chat_id"] == CHANNEL_CHAT_ID]

    def admin_messages(self):
        return [m for m in self.sent if m["chat_id"] == self.admin_chat_id]


class FakePostmaker:
    def __init__(self, post_text="Перегенерований текст поста."):
        self.post_text = post_text
        self.calls = []

    def make(self, title, summary, source, url, article_text=None, extra_instruction=None):
        self.calls.append(
            {
                "title": title,
                "summary": summary,
                "source": source,
                "url": url,
                "article_text": article_text,
                "extra_instruction": extra_instruction,
            }
        )
        if self.post_text is None:
            return None
        return SimpleNamespace(post_text=self.post_text, score=0.9, should_post=True)


@pytest.fixture
def repo(tmp_path):
    r = SqliteNewsRepository(db_path=str(tmp_path / "test.db"))
    r.init_db()
    r.ensure_categories(
        [
            SimpleNamespace(slug="war", title="Війна", hashtag="#війна"),
            SimpleNamespace(slug="other", title="Інше", hashtag="#інше"),
        ]
    )
    return r


@pytest.fixture
def fake_tg():
    return FakeTelegramClient()


@pytest.fixture
def publisher(fake_tg):
    return ChannelPublisher(
        http=DummyHttp(),
        tg=fake_tg,
        enable_video=True,
        max_image_bytes=5 * 1024 * 1024,
        max_video_bytes=20 * 1024 * 1024,
        enable_og_image=False,  # no network in tests
    )


@pytest.fixture
def formatter():
    return PostFormatter(include_source=True)


@pytest.fixture
def moderation_cfg():
    return SimpleNamespace(enabled=True, timeout_minutes=45, on_timeout="skip")


@pytest.fixture
def moderation(moderation_cfg, repo, fake_tg, formatter, publisher):
    return ModerationService(
        cfg_moderation=moderation_cfg,
        repo=repo,
        tg=fake_tg,
        formatter=formatter,
        publisher=publisher,
        postmaker=FakePostmaker(),
    )


def seed_item(
    repo: SqliteNewsRepository,
    item_hash: str = "hash-1",
    status: str = "pending_post",
    post_text: str = "Тестова новина: щось сталося.\n\nКонтекст події у двох реченнях.",
    category_slug: str = "war",
    score: float = 0.9,
    origin: str = "ua",
) -> dict:
    """Insert a scored item ready for publication and return its full row."""
    repo.upsert_item(
        item_hash=item_hash,
        source="Суспільне",
        title="Тестова новина",
        link="https://example.com/news/1",
        summary="Короткий опис",
        published_at_utc=utc_now_iso(),
        origin=origin,
    )
    con = repo._connect()
    cat_id = repo.category_id(category_slug)
    # Backdate created_at so the item is past cluster_wait_minutes and due for posting.
    created_past = (datetime.now(timezone.utc) - timedelta(minutes=30)).isoformat(timespec="seconds")
    con.execute(
        """
        UPDATE news_items
        SET status=?, post_text=?, should_post=1, score=?, topic_key='test-topic',
            decision_mode='post', category_id=?, article_text='Повний текст статті для тестів.',
            created_at_utc=?
        WHERE item_hash=?
        """,
        (status, post_text, score, cat_id, created_past, item_hash),
    )
    con.commit()
    row = con.execute(
        """
        SELECT ni.*, c.slug AS category_slug, c.hashtag AS category_hashtag
        FROM news_items ni LEFT JOIN categories c ON c.id = ni.category_id
        WHERE item_hash=?
        """,
        (item_hash,),
    ).fetchone()
    return dict(row)


def get_status(repo: SqliteNewsRepository, item_hash: str) -> str:
    con = repo._connect()
    row = con.execute("SELECT status FROM news_items WHERE item_hash=?", (item_hash,)).fetchone()
    return str(row["status"]) if row else "<missing>"


def get_moderation_log(repo: SqliteNewsRepository, item_hash: str) -> list[dict]:
    con = repo._connect()
    rows = con.execute(
        "SELECT * FROM moderation_log WHERE item_hash=? ORDER BY id", (item_hash,)
    ).fetchall()
    return [dict(r) for r in rows]


def make_callback(item_id: int, action: str, msg_id: int = 111, chat_id: int = ADMIN_CHAT_ID) -> dict:
    return {
        "id": "cb-test-1",
        "data": f"mod:{action}:{item_id}",
        "from": {"id": chat_id},
        "message": {"message_id": msg_id, "chat": {"id": chat_id}},
    }


def age_review(repo: SqliteNewsRepository, item_hash: str, minutes: int) -> None:
    """Backdate review_requested_at_utc to simulate admin silence."""
    past = (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat(timespec="seconds")
    con = repo._connect()
    con.execute("UPDATE news_items SET review_requested_at_utc=? WHERE item_hash=?", (past, item_hash))
    con.commit()

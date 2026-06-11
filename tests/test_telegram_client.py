from __future__ import annotations

import json
from types import SimpleNamespace

from app.telegram.client import TelegramClient


class CapturingHttp:
    """Captures Bot API calls and returns a canned OK response."""

    def __init__(self):
        self.calls = []

    def post(self, url, *, json=None, data=None, files=None, headers=None):
        self.calls.append({"url": url, "json": json, "data": data, "files": files})
        return SimpleNamespace(
            ok=True,
            status_code=200,
            text="",
            json=lambda: {"ok": True, "result": {"message_id": 42}},
        )

    def get(self, *a, **kw):
        raise AssertionError("Unexpected GET")


def make_client():
    http = CapturingHttp()
    tg = TelegramClient(http=http, token="TOKEN", chat_id=-100500, admin_chat_id=999)
    return http, tg


def test_send_message_attaches_reply_markup():
    http, tg = make_client()
    kb = {"inline_keyboard": [[{"text": "ok", "callback_data": "x"}]]}

    ok, msg_id = tg.send_message_with_id("Привіт", chat_id=999, reply_markup=kb)

    assert ok and msg_id == 42
    payload = http.calls[0]["json"]
    assert payload["chat_id"] == 999
    assert payload["reply_markup"] == kb


def test_send_photo_serializes_reply_markup():
    http, tg = make_client()
    kb = {"inline_keyboard": [[{"text": "ok", "callback_data": "x"}]]}

    ok, _ = tg.send_photo_with_id(b"bytes", caption_text="cap", reply_markup=kb)

    assert ok
    data = http.calls[0]["data"]
    assert json.loads(data["reply_markup"]) == kb


def test_answer_callback_query_payload():
    http, tg = make_client()

    assert tg.answer_callback_query("cb-id-1", text="Готово") is True
    payload = http.calls[0]["json"]
    assert http.calls[0]["url"].endswith("/answerCallbackQuery")
    assert payload["callback_query_id"] == "cb-id-1"
    assert payload["text"] == "Готово"


def test_edit_message_reply_markup_removes_keyboard():
    http, tg = make_client()

    assert tg.edit_message_reply_markup(999, 42, reply_markup=None) is True
    payload = http.calls[0]["json"]
    assert http.calls[0]["url"].endswith("/editMessageReplyMarkup")
    assert payload["reply_markup"] == {"inline_keyboard": []}


def test_send_message_without_markup_unchanged():
    http, tg = make_client()

    ok, msg_id = tg.send_message_with_id("Текст у канал")

    assert ok and msg_id == 42
    payload = http.calls[0]["json"]
    assert payload["chat_id"] == -100500
    assert "reply_markup" not in payload

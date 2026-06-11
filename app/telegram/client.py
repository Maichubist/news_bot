from __future__ import annotations

import json
import logging
import html
import os
import re
from typing import List, Tuple, Optional

log = logging.getLogger("telegram.client")


class TelegramClient:
    def __init__(self, http, token: str, chat_id: int, admin_chat_id: int | None = None):
        self.http = http
        self.token = token
        self.chat_id = chat_id
        self.admin_chat_id = admin_chat_id

    _SRC_MARK_RE = re.compile(r"^—\s*SRC\[(?P<name>[^\]]+)\]\((?P<url>[^)]+)\)(?P<extra>.*)$")

    def _format_message(self, text: str) -> str:
        """
        Робимо перший рядок (заголовок) жирним.
        Решту тексту залишаємо як є.
        """
        if not text:
            return text

        lines = text.strip().split("\n")

        if not lines:
            return text

        title = html.escape(lines[0].strip())

        body_lines = []
        for l in lines[1:]:
            m = self._SRC_MARK_RE.match(l.strip())
            if m:
                name = html.escape(m.group("name").strip())
                url = html.escape(m.group("url").strip(), quote=True)
                extra = html.escape((m.group("extra") or "").strip())
                if extra:
                    body_lines.append(f"— <a href=\"{url}\">{name}</a> {extra}")
                else:
                    body_lines.append(f"— <a href=\"{url}\">{name}</a>")
            else:
                body_lines.append(html.escape(l))

        body = "\n".join(body_lines)

        if body:
            return f"<b>{title}</b>\n\n{body}"
        else:
            return f"<b>{title}</b>"


    def _split_text(self, text: str, max_len: int = 3500) -> List[str]:
        cleaned = (text or "").strip()
        if len(cleaned) <= max_len:
            return [cleaned]

        parts: List[str] = []
        current = ""
        for block in cleaned.split("\n\n"):
            candidate = block if not current else current + "\n\n" + block
            if len(candidate) <= max_len:
                current = candidate
                continue
            if current:
                parts.append(current)
                current = ""
            while len(block) > max_len:
                parts.append(block[:max_len])
                block = block[max_len:]
            current = block
        if current:
            parts.append(current)
        return parts or [cleaned]

    def send_message_with_id(
        self,
        text: str,
        disable_preview: bool = False,
        chat_id: int | None = None,
        reply_markup: dict | None = None,
    ) -> Tuple[bool, Optional[int]]:

        url = f"https://api.telegram.org/bot{self.token}/sendMessage"

        chunks = self._split_text(text)
        first_msg_id = None
        for i, chunk in enumerate(chunks):
            formatted_text = self._format_message(chunk)
            payload = {
                "chat_id": chat_id if chat_id is not None else self.chat_id,
                "text": formatted_text,
                "parse_mode": "HTML",
                "disable_web_page_preview": disable_preview,
            }
            # Keyboard goes on the last chunk so it sits under the visible end of the post.
            if reply_markup is not None and i == len(chunks) - 1:
                payload["reply_markup"] = reply_markup

            try:
                r = self.http.post(url, json=payload)
            except Exception:
                log.exception("Telegram request crashed")
                return False, first_msg_id

            log.info("Sending TG message len=%d", len(chunk))

            if not r.ok:
                log.warning(
                    "Telegram error: %s %s",
                    r.status_code,
                    (r.text or "")[:800],
                )
                return False, first_msg_id

            try:
                data = r.json()
                msg_id = data.get("result", {}).get("message_id")
                if first_msg_id is None:
                    first_msg_id = msg_id
                # When a keyboard is attached, the caller needs the id of the
                # message that carries it (the last chunk) to edit it later.
                if reply_markup is not None and i == len(chunks) - 1 and msg_id is not None:
                    first_msg_id = msg_id
            except Exception:
                pass

        return True, first_msg_id

    def send_photo_with_id(
        self,
        photo_bytes: bytes,
        caption_text: str,
        disable_preview: bool = True,
        filename: str = "image.jpg",
        chat_id: int | None = None,
        reply_markup: dict | None = None,
    ) -> Tuple[bool, Optional[int]]:
        url = f"https://api.telegram.org/bot{self.token}/sendPhoto"

        caption = self._format_message(caption_text)

        data = {
            "chat_id": str(chat_id if chat_id is not None else self.chat_id),
            "caption": caption,
            "parse_mode": "HTML",
            "disable_web_page_preview": disable_preview,
        }
        if reply_markup is not None:
            data["reply_markup"] = json.dumps(reply_markup, ensure_ascii=False)
        files = {
            "photo": (filename, photo_bytes),
        }

        try:
            r = self.http.post(url, data=data, files=files)
        except Exception:
            log.exception("Telegram photo request crashed")
            return False, None

        log.info("Sending TG photo caption len=%d", len(caption_text))

        if not r.ok:
            log.warning("Telegram photo error: %s %s", r.status_code, (r.text or "")[:800])
            return False, None

        try:
            dataj = r.json()
            msg_id = dataj.get("result", {}).get("message_id")
        except Exception:
            msg_id = None

        return True, msg_id

    def send_video_with_id(
        self,
        video_bytes: bytes,
        caption_text: str,
        filename: str = "video.mp4",
        chat_id: int | None = None,
        reply_markup: dict | None = None,
    ) -> Tuple[bool, Optional[int]]:
        """
        Send a video file with HTML caption. Telegram bots accept uploads up to 50 MB;
        we enforce a smaller cap upstream in the pipeline.
        """
        url = f"https://api.telegram.org/bot{self.token}/sendVideo"

        caption = self._format_message(caption_text)

        data = {
            "chat_id": str(chat_id if chat_id is not None else self.chat_id),
            "caption": caption,
            "parse_mode": "HTML",
            "supports_streaming": "true",
        }
        if reply_markup is not None:
            data["reply_markup"] = json.dumps(reply_markup, ensure_ascii=False)
        files = {
            "video": (filename, video_bytes),
        }

        try:
            r = self.http.post(url, data=data, files=files)
        except Exception:
            log.exception("Telegram video request crashed")
            return False, None

        log.info("Sending TG video size=%dKB caption len=%d", len(video_bytes) // 1024, len(caption_text))

        if not r.ok:
            log.warning("Telegram video error: %s %s", r.status_code, (r.text or "")[:800])
            return False, None

        try:
            dataj = r.json()
            msg_id = dataj.get("result", {}).get("message_id")
        except Exception:
            msg_id = None

        return True, msg_id

    def answer_callback_query(self, callback_query_id: str, text: str | None = None, show_alert: bool = False) -> bool:
        url = f"https://api.telegram.org/bot{self.token}/answerCallbackQuery"
        payload: dict = {"callback_query_id": str(callback_query_id)}
        if text:
            payload["text"] = text[:200]
        if show_alert:
            payload["show_alert"] = True
        try:
            r = self.http.post(url, json=payload)
        except Exception:
            log.exception("Telegram answerCallbackQuery crashed")
            return False
        if not getattr(r, "ok", False):
            log.warning("Telegram answerCallbackQuery error: %s %s", r.status_code, (r.text or "")[:400])
            return False
        return True

    def edit_message_reply_markup(self, chat_id: int, message_id: int, reply_markup: dict | None = None) -> bool:
        """Replace or remove (reply_markup=None) the inline keyboard of an existing message."""
        url = f"https://api.telegram.org/bot{self.token}/editMessageReplyMarkup"
        payload: dict = {
            "chat_id": int(chat_id),
            "message_id": int(message_id),
            "reply_markup": reply_markup if reply_markup is not None else {"inline_keyboard": []},
        }
        try:
            r = self.http.post(url, json=payload)
        except Exception:
            log.exception("Telegram editMessageReplyMarkup crashed")
            return False
        if not getattr(r, "ok", False):
            log.warning("Telegram editMessageReplyMarkup error: %s %s", r.status_code, (r.text or "")[:400])
            return False
        return True

    def get_updates(self, offset: int | None = None, timeout: int = 0, allowed_updates: list[str] | None = None):
        url = f"https://api.telegram.org/bot{self.token}/getUpdates"
        payload = {"timeout": int(timeout)}
        if offset is not None:
            payload["offset"] = int(offset)
        # "message_reaction" is delivered only when explicitly requested here
        # (and only if the bot is an admin of the channel).
        if allowed_updates is not None:
            payload["allowed_updates"] = list(allowed_updates)
        try:
            r = self.http.post(url, json=payload)
        except Exception:
            log.exception("Telegram getUpdates crashed")
            return []
        if not getattr(r, "ok", False):
            log.warning("Telegram getUpdates error: %s %s", r.status_code, (r.text or "")[:800])
            return []
        try:
            data = r.json()
            return data.get("result", []) or []
        except Exception:
            return []

    def send_document(self, path: str, caption: str = "", chat_id: int | None = None) -> Tuple[bool, Optional[int]]:
        url = f"https://api.telegram.org/bot{self.token}/sendDocument"
        data = {
            "chat_id": str(chat_id if chat_id is not None else self.chat_id),
            "caption": caption or "",
        }
        with open(path, 'rb') as f:
            files = {"document": (os.path.basename(path), f.read())}
        try:
            r = self.http.post(url, data=data, files=files)
        except Exception:
            log.exception("Telegram document request crashed")
            return False, None
        if not getattr(r, 'ok', False):
            log.warning("Telegram document error: %s %s", r.status_code, (r.text or "")[:800])
            return False, None
        try:
            dataj = r.json()
            return True, dataj.get("result", {}).get("message_id")
        except Exception:
            return True, None

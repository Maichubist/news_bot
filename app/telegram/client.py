from __future__ import annotations

import logging
import html
import re
from typing import Tuple, Optional

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

    def send_message_with_id(
        self,
        text: str,
        disable_preview: bool = False,
        chat_id: int | None = None,
    ) -> Tuple[bool, Optional[int]]:

        url = f"https://api.telegram.org/bot{self.token}/sendMessage"

        formatted_text = self._format_message(text)

        payload = {
            "chat_id": chat_id if chat_id is not None else self.chat_id,
            "text": formatted_text,
            "parse_mode": "HTML",
            "disable_web_page_preview": disable_preview,
        }

        try:
            r = self.http.post(url, json=payload)
        except Exception:
            log.exception("Telegram request crashed")
            return False, None

        log.info("Sending TG message len=%d", len(text))

        if not r.ok:
            log.warning(
                "Telegram error: %s %s",
                r.status_code,
                (r.text or "")[:800],
            )
            return False, None

        try:
            data = r.json()
            msg_id = data.get("result", {}).get("message_id")
        except Exception:
            msg_id = None

        return True, msg_id

    def send_photo_with_id(
        self,
        photo_bytes: bytes,
        caption_text: str,
        disable_preview: bool = True,
        filename: str = "image.jpg",
        chat_id: int | None = None,
    ) -> Tuple[bool, Optional[int]]:
        url = f"https://api.telegram.org/bot{self.token}/sendPhoto"

        caption = self._format_message(caption_text)

        data = {
            "chat_id": str(chat_id if chat_id is not None else self.chat_id),
            "caption": caption,
            "parse_mode": "HTML",
            "disable_web_page_preview": disable_preview,
        }
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

    def get_updates(self, offset: int | None = None, timeout: int = 0):
        url = f"https://api.telegram.org/bot{self.token}/getUpdates"
        payload = {"timeout": int(timeout)}
        if offset is not None:
            payload["offset"] = int(offset)
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
            files = {"document": (path.split('/')[-1], f.read())}
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

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

log = logging.getLogger("digest.openai")


@dataclass(frozen=True)
class DailyDigest:
    post_text: str


class OpenAIDailyDigestMaker:
    """
    One call per day:
    takes list of posted texts and produces a readable daily digest.
    """

    def __init__(self, http, api_key: str, model: str = "gpt-4o-mini", prompt: str | None = None):
        self.http = http
        self.api_key = api_key
        self.model = model
        self.prompt = (prompt or "").strip()

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    def make(self, day_label: str, posts: List[str]) -> Optional[DailyDigest]:
        posts = [p.strip() for p in posts if p and p.strip()]
        if len(posts) < 3:
            return None

        # обмежимо обсяг у промпті (дешево і стабільно)
        posts = posts[-60:]

        joined = "\n\n---\n\n".join(posts)

        if self.prompt:
            prompt = self.prompt.format(day_label=day_label, posts=joined)
        else:
            prompt = (
                "Ти редактор-аналітик щоденного Telegram-дайджесту про геополітику та економіку.\n"
                f"Зроби підсумок дня: {day_label}\n\n"
                "Вимоги:\n"
                "- українською\n"
                "- дуже читабельно, без води\n"
                "- 25–45 рядків\n"
                "- структура:\n"
                "  1) Заголовок\n"
                "  2) 5–9 ключових подій (кожна: 1 рядок що сталося + 1 рядок що це означає)\n"
                "  3) 'Тренд дня' (3–5 рядків)\n"
                "  4) 'Що відстежувати завтра' (3–6 булітів)\n"
                "- не вигадуй фактів, спирайся лише на тексти нижче\n\n"
                "Пости за день:\n"
                f"{joined}\n"
            )

        schema: Dict[str, Any] = {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "post_text": {"type": "string"},
            },
            "required": ["post_text"],
        }

        payload: Dict[str, Any] = {
            "model": self.model,
            "input": [
                {
                    "role": "user",
                    "content": [{"type": "input_text", "text": prompt}],
                }
            ],
            "text": {
                "format": {
                    "type": "json_schema",
                    "name": "daily_digest_v1",
                    "schema": schema,
                    "strict": True,
                }
            },
        }

        try:
            r = self.http.post(
                "https://api.openai.com/v1/responses",
                json=payload,
                headers=self._headers(),
            )
        except Exception as ex:
            log.warning("OpenAI digest error: %s", ex)
            return None

        if not getattr(r, "ok", False):
            try:
                log.warning("OpenAI digest error: %s %s", r.status_code, r.text)
            except Exception:
                log.warning("OpenAI digest error: non-ok response")
            return None

        try:
            data = r.json()
        except Exception:
            log.warning("OpenAI digest error: cannot decode JSON")
            return None

        try:
            out_text = ""
            for item in data.get("output", []):
                for c in item.get("content", []):
                    if c.get("type") == "output_text" and c.get("text"):
                        out_text += c["text"]
            out_text = (out_text or "").strip()
            obj = json.loads(out_text)
            post_text = (obj.get("post_text") or "").strip()
            if not post_text:
                return None
            return DailyDigest(post_text=post_text)
        except Exception as ex:
            log.warning("OpenAI digest parse error: %s", ex)
            return None
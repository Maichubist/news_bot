from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Optional, Any, Dict

log = logging.getLogger("translate.openai")


@dataclass(frozen=True)
class UaTranslation:
    title_ua: str
    summary_ua: Optional[str]


class OpenAIUaTranslator:
    """
    Translates title + summary to Ukrainian via OpenAI Responses API.
    Uses Structured Outputs (json_schema) so we can parse deterministically.
    """

    def __init__(self, http, api_key: str, model: str = "gpt-5-mini"):
        self.http = http
        self.api_key = api_key
        self.model = model

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    def translate(self, title: str, summary: Optional[str], max_chars_summary: int = 350) -> Optional[UaTranslation]:
        title = (title or "").strip()
        summary = (summary or "").strip() if summary else None
        if not title:
            return None

        if summary and len(summary) > max_chars_summary:
            summary = summary[:max_chars_summary].rstrip() + "…"

        prompt_parts = [
            "Переклади українською, зберігаючи зміст, імена, цифри та назви брендів.",
            "Не додавай від себе фактів. Не пиши вступів/пояснень — тільки результат у заданій JSON-структурі.",
            "",
            f"TITLE: {title}",
        ]
        if summary:
            prompt_parts.append(f"SUMMARY: {summary}")

        prompt = "\n".join(prompt_parts)

        schema = {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "title_ua": {"type": "string"},
                "summary_ua": {"type": ["string", "null"]},
            },
            "required": ["title_ua", "summary_ua"],
        }

        payload: Dict[str, Any] = {
            "model": self.model,
            "input": [
                {
                    "role": "user",
                    "content": [{"type": "input_text", "text": prompt}],
                }
            ],
            # Structured Outputs in Responses API -> text.format
            "text": {
                "format": {
                    "type": "json_schema",
                    "name": "ua_news_translation",   # <-- ОЦЕ якраз і вимагалось
                    "schema": schema,
                    "strict": True,
                }
            },
        }

        try:
            # ВАЖЛИВО: тут твій http має вміти POST JSON.
            # Якщо в тебе метод називається інакше — заміни на свій.
            r = self.http.post(
                "https://api.openai.com/v1/responses",
                json=payload,
                headers=self._headers(),
            )
        except Exception as ex:
            log.warning("OpenAI translate error: %s", ex)
            return None

        if not getattr(r, "ok", False):
            try:
                log.warning("OpenAI translate error: %s %s", r.status_code, r.text)
            except Exception:
                log.warning("OpenAI translate error: non-ok response")
            return None

        try:
            data = r.json()
        except Exception:
            log.warning("OpenAI translate error: cannot decode JSON response")
            return None

        # витягаємо текст з output (Responses API)
        try:
            out_text = ""
            for item in data.get("output", []):
                for c in item.get("content", []):
                    if c.get("type") == "output_text" and c.get("text"):
                        out_text += c["text"]

            out_text = (out_text or "").strip()
            if not out_text:
                log.warning("OpenAI translate error: empty output_text")
                return None

            obj = json.loads(out_text)
            title_ua = (obj.get("title_ua") or "").strip()
            summary_ua = obj.get("summary_ua", None)
            summary_ua = summary_ua.strip() if isinstance(summary_ua, str) else None

            if not title_ua:
                return None

            return UaTranslation(title_ua=title_ua, summary_ua=summary_ua)

        except Exception as ex:
            log.warning("OpenAI translate parse error: %s", ex)
            return None
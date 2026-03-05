from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

log = logging.getLogger("editor.openai")


DEFAULT_PROMPT = (
    "Ти професійний новинний редактор Telegram-каналу.\n\n"
    "Твоє завдання: (1) оцінити, чи варто публікувати матеріал як новину, (2) якщо так — написати короткий пост.\n\n"
    "Шкала score: число від 0.0 до 1.0 (0 = точно не варто, 1 = максимально важливо/цікаво).\n"
    "should_post = false, якщо це реклама/партнерський матеріал/дайджест/розсилка/подкаст/відео/купон/знижка/оголошення/біржові котирування без новини/поради купити.\n\n"
    "Перепиши новину українською мовою у короткий та привабливий формат для Telegram.\n\n"
    "Правила:\n"
    "1. Заголовок має бути чіпким і зрозумілим, підкреслювати конфлікт, наслідки або важливість події.\n"
    "2. Уникай бюрократичних формулювань.\n"
    "3. Пиши коротко і просто.\n"
    "4. Максимальний обсяг — 550 символів.\n"
    "5. Використовуй структуру:\n\n"
    "Заголовок\n\n"
    "1–2 короткі речення про подію (без міток типу 'Що сталося:').\n\n"
    "2–3 короткі пункти, чому це важливо (кожен пункт — одне речення).\n\n"
    "6. Не вигадуй факти — використовуй лише інформацію з джерела.\n"
    "7. НЕ пиши слово 'Заголовок' буквально.\n\n"
    "Новина:\n{news_text}"
)


@dataclass(frozen=True)
class PostDecision:
    score: float
    should_post: bool
    post_text: str
    why: List[str]


class OpenAINewsPostMaker:
    def __init__(self, http, api_key: str, model: str, prompt: str | None = None):
        self.http = http
        self.api_key = api_key
        self.model = model
        self.prompt = (prompt or "").strip() or DEFAULT_PROMPT

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    def make(self, title: str, summary: Optional[str], source: str, url: str) -> Optional[PostDecision]:
        news_text = title.strip()
        if summary:
            news_text += "\n\n" + summary.strip()

        # Provide a little context to the model (helps it flag video/promo pages)
        news_text += f"\n\nДжерело: {source}\nURL: {url}"

        prompt = self.prompt.format(news_text=news_text)

        schema: Dict[str, Any] = {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "score": {"type": "number", "minimum": 0, "maximum": 1},
                "should_post": {"type": "boolean"},
                "why": {"type": "array", "items": {"type": "string"}},
                "post_text": {"type": "string"},
            },
            "required": ["score", "should_post", "why", "post_text"],
        }

        payload: Dict[str, Any] = {
            "model": self.model,
            "input": [{"role": "user", "content": [{"type": "input_text", "text": prompt}]}],
            "text": {
                "format": {
                    "type": "json_schema",
                    "name": "post_decision_v2",
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
            log.warning("OpenAI postmaker error: %s", ex)
            return None

        if not getattr(r, "ok", False):
            try:
                log.warning("OpenAI postmaker error: %s %s", r.status_code, (r.text or "")[:800])
            except Exception:
                log.warning("OpenAI postmaker error: non-ok response")
            return None

        try:
            data = r.json()
        except Exception:
            log.warning("OpenAI postmaker error: cannot decode JSON")
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

            # last sanity check against literal placeholder
            first_line = post_text.split("\n", 1)[0].strip().lower()
            if first_line in ("заголовок", "headline", "title"):
                post_text = f"{title.strip()}\n\n" + "\n".join(post_text.split("\n")[1:]).strip()

            return PostDecision(
                score=float(obj.get("score") or 0.0),
                should_post=bool(obj.get("should_post")),
                post_text=post_text,
                why=[str(x) for x in (obj.get("why") or [])][:6],
            )
        except Exception as ex:
            log.warning("OpenAI postmaker parse error: %s", ex)
            return None

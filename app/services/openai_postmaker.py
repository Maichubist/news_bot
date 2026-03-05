from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

log = logging.getLogger("editor.openai")


@dataclass(frozen=True)
class PostDecision:
    score: float
    should_post: bool
    post_text: str
    why: List[str]
    category: str


class OpenAINewsPostMaker:
    def __init__(self, http, api_key: str, model: str, prompt: str | None = None, categories: Optional[List[Dict[str, str]]] = None):
        self.http = http
        self.api_key = api_key
        self.model = model
        self.prompt = (prompt or "").strip()
        self.categories = categories or []

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

        # Category instructions (kept short, schema enforces the enum)
        if self.categories:
            cats = ", ".join([f"{c['slug']} ({c['title']})" for c in self.categories if c.get('slug') and c.get('title')])
            prompt += "\n\nДодатково: обери категорію category зі списку: " + cats + "."

        schema: Dict[str, Any] = {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "score": {"type": "number", "minimum": 0, "maximum": 1},
                "should_post": {"type": "boolean"},
                "why": {"type": "array", "items": {"type": "string"}},
                "post_text": {"type": "string"},
                "category": {"type": "string"},
            },
            "required": ["score", "should_post", "why", "post_text", "category"],
        }

        # If we have configured categories, enforce enum.
        slugs = [str(c.get("slug")) for c in (self.categories or []) if str(c.get("slug") or "").strip()]
        if slugs:
            schema["properties"]["category"]["enum"] = slugs

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
                category=str(obj.get("category") or "other").strip() or "other",
            )
        except Exception as ex:
            log.warning("OpenAI postmaker parse error: %s", ex)
            return None

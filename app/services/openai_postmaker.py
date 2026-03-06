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
    tier: str = "C"
    publish_mode: str = "digest"
    event_key: str = ""
    novelty_score: float = 0.0
    impact_score: float = 0.0
    ua_relevance_score: float = 0.0


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
        news_text += f"\n\nДжерело: {source}\nURL: {url}"

        prompt = self.prompt.format(news_text=news_text)
        if self.categories:
            cats = ", ".join([f"{c['slug']} ({c['title']})" for c in self.categories if c.get("slug") and c.get("title")])
            prompt += "\n\nОбери category лише зі списку: " + cats + "."

        schema: Dict[str, Any] = {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "score": {"type": "number", "minimum": 0, "maximum": 1},
                "should_post": {"type": "boolean"},
                "why": {"type": "array", "items": {"type": "string"}},
                "post_text": {"type": "string"},
                "category": {"type": "string"},
                "tier": {"type": "string", "enum": ["A", "B", "C", "D"]},
                "publish_mode": {"type": "string", "enum": ["post", "wrap_candidate", "digest", "drop"]},
                "event_key": {"type": "string"},
                "novelty_score": {"type": "number", "minimum": 0, "maximum": 1},
                "impact_score": {"type": "number", "minimum": 0, "maximum": 1},
                "ua_relevance_score": {"type": "number", "minimum": 0, "maximum": 1},
            },
            "required": [
                "score",
                "should_post",
                "why",
                "post_text",
                "category",
                "tier",
                "publish_mode",
                "event_key",
                "novelty_score",
                "impact_score",
                "ua_relevance_score",
            ],
        }

        slugs = [str(c.get("slug")) for c in (self.categories or []) if str(c.get("slug") or "").strip()]
        if slugs:
            schema["properties"]["category"]["enum"] = slugs

        payload: Dict[str, Any] = {
            "model": self.model,
            "input": [{"role": "user", "content": [{"type": "input_text", "text": prompt}]}],
            "text": {
                "format": {
                    "type": "json_schema",
                    "name": "post_decision_v3",
                    "schema": schema,
                    "strict": True,
                }
            },
        }

        try:
            r = self.http.post("https://api.openai.com/v1/responses", json=payload, headers=self._headers())
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
            obj = json.loads((out_text or "").strip())
            post_text = (obj.get("post_text") or "").strip()
            first_line = post_text.split("\n", 1)[0].strip().lower() if post_text else ""
            if first_line in ("заголовок", "headline", "title"):
                body = "\n".join(post_text.split("\n")[1:]).strip()
                post_text = f"{title.strip()}\n\n{body}".strip()

            return PostDecision(
                score=float(obj.get("score") or 0.0),
                should_post=bool(obj.get("should_post")),
                post_text=post_text,
                why=[str(x) for x in (obj.get("why") or [])][:6],
                category=str(obj.get("category") or "other").strip() or "other",
                tier=str(obj.get("tier") or "C").strip() or "C",
                publish_mode=str(obj.get("publish_mode") or "digest").strip() or "digest",
                event_key=str(obj.get("event_key") or "").strip(),
                novelty_score=float(obj.get("novelty_score") or 0.0),
                impact_score=float(obj.get("impact_score") or 0.0),
                ua_relevance_score=float(obj.get("ua_relevance_score") or 0.0),
            )
        except Exception as ex:
            log.warning("OpenAI postmaker parse error: %s", ex)
            return None

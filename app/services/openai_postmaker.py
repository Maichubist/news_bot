from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

log = logging.getLogger("editor.openai")


# Cheap first pass for ranking mode: classification + fact summary, NO post_text.
# Can be overridden at runtime via /prompt_set classify_prompt.
DEFAULT_CLASSIFY_PROMPT = """Ти редактор українського новинного Telegram-каналу.
Оціни і класифікуй новину СТРОГО за фактами з наданого тексту. Поверни лише JSON.

Завдання:
1) класифікуй news_type: hard_news | followup | commentary | analysis | noise | promo
2) напиши fact_summary українською: 1-2 речення, лише головний факт —
   хто, що зробив, ключова цифра. Без оцінок і прикметників.
3) оціни важливість (score), новизну, вплив і дотичність до України.

Формат JSON:
- score: 0..1 (загальна важливість для каналу)
- category: одна категорія зі списку
- tier: A/B/C/D
- event_key: короткий стабільний ключ події англійською lowercase через дефіс
- topic_key: короткий ключ теми англійською lowercase через дефіс
- novelty_score: 0..1
- impact_score: 0..1
- ua_relevance_score: 0..1
- news_type: hard_news | followup | commentary | analysis | noise | promo
- has_new_fact: boolean
- is_breaking: boolean
- fact_summary: string (1-2 речення українською)

Новина:
{news_text}"""


@dataclass(frozen=True)
class ClassifyDecision:
    """Result of the cheap per-item pass (ranking mode): no post_text yet."""
    score: float
    category: str
    tier: str = "C"
    event_key: str = ""
    topic_key: str = ""
    novelty_score: float = 0.0
    impact_score: float = 0.0
    ua_relevance_score: float = 0.0
    news_type: str = "noise"
    has_new_fact: bool = False
    is_breaking: bool = False
    fact_summary: str = ""


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
    topic_key: str = ""
    novelty_score: float = 0.0
    impact_score: float = 0.0
    ua_relevance_score: float = 0.0
    news_type: str = "noise"
    has_new_fact: bool = False
    is_breaking: bool = False


class OpenAINewsPostMaker:
    def __init__(self, http, api_key: str, model: str, prompt: str | None = None, categories: Optional[List[Dict[str, str]]] = None, prompt_provider: Callable[[str], str] | None = None):
        self.http = http
        self.api_key = api_key
        self.model = model
        self.prompt = (prompt or "").strip()
        self.categories = categories or []
        self.prompt_provider = prompt_provider

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    def _post_json(self, prompt: str, schema: Dict[str, Any], schema_name: str, model: str | None = None) -> Optional[Dict[str, Any]]:
        """One strict-JSON Responses API call; returns the parsed object or None."""
        payload: Dict[str, Any] = {
            "model": model or self.model,
            "input": [{"role": "user", "content": [{"type": "input_text", "text": prompt}]}],
            "text": {
                "format": {
                    "type": "json_schema",
                    "name": schema_name,
                    "schema": schema,
                    "strict": True,
                }
            },
        }
        try:
            r = self.http.post("https://api.openai.com/v1/responses", json=payload, headers=self._headers())
        except Exception as ex:
            log.warning("OpenAI %s error: %s", schema_name, ex)
            return None
        if not getattr(r, "ok", False):
            try:
                log.warning("OpenAI %s error: %s %s", schema_name, r.status_code, (r.text or "")[:800])
            except Exception:
                log.warning("OpenAI %s error: non-ok response", schema_name)
            return None
        try:
            data = r.json()
            out_text = ""
            for item in data.get("output", []):
                for c in item.get("content", []):
                    if c.get("type") == "output_text" and c.get("text"):
                        out_text += c["text"]
            return json.loads((out_text or "").strip())
        except Exception as ex:
            log.warning("OpenAI %s parse error: %s", schema_name, ex)
            return None

    def _build_news_text(self, title: str, summary: Optional[str], source: str, url: str, article_text: Optional[str]) -> str:
        news_text = title.strip()
        if summary:
            news_text += "\n\n" + summary.strip()
        if article_text:
            body = article_text.strip()
            if body and body not in news_text:
                news_text += "\n\nПовний текст статті:\n" + body
        news_text += f"\n\nДжерело: {source}\nURL: {url}"
        return news_text

    def _category_slugs(self) -> List[str]:
        return [str(c.get("slug")) for c in (self.categories or []) if str(c.get("slug") or "").strip()]

    def classify(
        self,
        title: str,
        summary: Optional[str],
        source: str,
        url: str,
        article_text: Optional[str] = None,
    ) -> Optional[ClassifyDecision]:
        """Cheap per-item pass for ranking mode: classification + fact summary, no post_text."""
        news_text = self._build_news_text(title, summary, source, url, article_text)

        prompt_template = self.prompt_provider("classify_prompt") if self.prompt_provider else ""
        prompt = (prompt_template or DEFAULT_CLASSIFY_PROMPT).format(news_text=news_text)
        if self.categories:
            cats = ", ".join([f"{c['slug']} ({c['title']})" for c in self.categories if c.get("slug") and c.get("title")])
            prompt += "\n\nОбери category лише зі списку: " + cats + "."

        schema: Dict[str, Any] = {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "score": {"type": "number", "minimum": 0, "maximum": 1},
                "category": {"type": "string"},
                "tier": {"type": "string", "enum": ["A", "B", "C", "D"]},
                "event_key": {"type": "string"},
                "topic_key": {"type": "string"},
                "novelty_score": {"type": "number", "minimum": 0, "maximum": 1},
                "impact_score": {"type": "number", "minimum": 0, "maximum": 1},
                "ua_relevance_score": {"type": "number", "minimum": 0, "maximum": 1},
                "news_type": {"type": "string", "enum": ["hard_news", "followup", "commentary", "analysis", "noise", "promo"]},
                "has_new_fact": {"type": "boolean"},
                "is_breaking": {"type": "boolean"},
                "fact_summary": {"type": "string"},
            },
            "required": [
                "score", "category", "tier", "event_key", "topic_key", "novelty_score",
                "impact_score", "ua_relevance_score", "news_type", "has_new_fact",
                "is_breaking", "fact_summary",
            ],
        }
        slugs = self._category_slugs()
        if slugs:
            schema["properties"]["category"]["enum"] = slugs

        obj = self._post_json(prompt, schema, "classify_decision_v1")
        if obj is None:
            return None
        try:
            return ClassifyDecision(
                score=float(obj.get("score") or 0.0),
                category=str(obj.get("category") or "other").strip() or "other",
                tier=str(obj.get("tier") or "C").strip() or "C",
                event_key=str(obj.get("event_key") or "").strip(),
                topic_key=str(obj.get("topic_key") or obj.get("event_key") or "").strip(),
                novelty_score=float(obj.get("novelty_score") or 0.0),
                impact_score=float(obj.get("impact_score") or 0.0),
                ua_relevance_score=float(obj.get("ua_relevance_score") or 0.0),
                news_type=str(obj.get("news_type") or "noise").strip() or "noise",
                has_new_fact=bool(obj.get("has_new_fact")),
                is_breaking=bool(obj.get("is_breaking")),
                fact_summary=str(obj.get("fact_summary") or "").strip(),
            )
        except Exception as ex:
            log.warning("OpenAI classify mapping error: %s", ex)
            return None

    def make(
        self,
        title: str,
        summary: Optional[str],
        source: str,
        url: str,
        article_text: Optional[str] = None,
        extra_instruction: Optional[str] = None,
    ) -> Optional[PostDecision]:
        news_text = self._build_news_text(title, summary, source, url, article_text)

        prompt_template = self.prompt_provider("post_prompt") if self.prompt_provider else self.prompt
        prompt = (prompt_template or self.prompt).format(news_text=news_text)
        if self.categories:
            cats = ", ".join([f"{c['slug']} ({c['title']})" for c in self.categories if c.get("slug") and c.get("title")])
            prompt += "\n\nОбери category лише зі списку: " + cats + "."
        if extra_instruction:
            # Used by the moderation "regenerate" button to ask for a rewrite.
            prompt += "\n\nДодаткова інструкція редактора: " + extra_instruction.strip()

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
                "topic_key": {"type": "string"},
                "novelty_score": {"type": "number", "minimum": 0, "maximum": 1},
                "impact_score": {"type": "number", "minimum": 0, "maximum": 1},
                "ua_relevance_score": {"type": "number", "minimum": 0, "maximum": 1},
                "news_type": {"type": "string", "enum": ["hard_news", "followup", "commentary", "analysis", "noise", "promo"]},
                "has_new_fact": {"type": "boolean"},
                "is_breaking": {"type": "boolean"},
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
                "topic_key",
                "novelty_score",
                "impact_score",
                "ua_relevance_score",
                "news_type",
                "has_new_fact",
                "is_breaking",
            ],
        }

        slugs = self._category_slugs()
        if slugs:
            schema["properties"]["category"]["enum"] = slugs

        obj = self._post_json(prompt, schema, "post_decision_v4")
        if obj is None:
            return None

        try:
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
                topic_key=str(obj.get("topic_key") or obj.get("event_key") or "").strip(),
                novelty_score=float(obj.get("novelty_score") or 0.0),
                impact_score=float(obj.get("impact_score") or 0.0),
                ua_relevance_score=float(obj.get("ua_relevance_score") or 0.0),
                news_type=str(obj.get("news_type") or "noise").strip() or "noise",
                has_new_fact=bool(obj.get("has_new_fact")),
                is_breaking=bool(obj.get("is_breaking")),
            )
        except Exception as ex:
            log.warning("OpenAI postmaker parse error: %s", ex)
            return None

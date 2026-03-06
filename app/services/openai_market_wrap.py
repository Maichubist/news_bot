from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, List

log = logging.getLogger("editor.wrap")


@dataclass(frozen=True)
class WrapDecision:
    post_text: str


class OpenAIMarketWrapMaker:
    def __init__(self, http, api_key: str, model: str, default_prompt: str = ""):
        self.http = http
        self.api_key = api_key
        self.model = model
        self.default_prompt = (default_prompt or "").strip()

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    def make(self, wrap_name: str, items: List[Dict[str, Any]], prompt_template: str = "") -> WrapDecision | None:
        if not items:
            return None
        lines: List[str] = []
        for i, it in enumerate(items[:12], start=1):
            line = f"{i}. [{it.get('source','')}] {it.get('title','').strip()}"
            summary = (it.get("summary") or "").strip()
            if summary:
                line += f"\n{summary[:300]}"
            line += f"\nURL: {it.get('link','').strip()}"
            lines.append(line)

        prompt = (prompt_template or self.default_prompt or "").format(
            wrap_name=wrap_name,
            items="\n\n".join(lines),
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
            "input": [{"role": "user", "content": [{"type": "input_text", "text": prompt}]}],
            "text": {
                "format": {
                    "type": "json_schema",
                    "name": "topic_wrap_post",
                    "schema": schema,
                    "strict": True,
                }
            },
        }
        try:
            r = self.http.post("https://api.openai.com/v1/responses", json=payload, headers=self._headers())
        except Exception as ex:
            log.warning("OpenAI wrap error: %s", ex)
            return None
        if not getattr(r, "ok", False):
            try:
                log.warning("OpenAI wrap error: %s %s", r.status_code, (r.text or "")[:800])
            except Exception:
                log.warning("OpenAI wrap error: non-ok response")
            return None
        try:
            data = r.json()
            out_text = ""
            for item in data.get("output", []):
                for c in item.get("content", []):
                    if c.get("type") == "output_text" and c.get("text"):
                        out_text += c["text"]
            obj = json.loads((out_text or "").strip())
            post_text = (obj.get("post_text") or "").strip()
            if not post_text:
                return None
            return WrapDecision(post_text=post_text)
        except Exception as ex:
            log.warning("OpenAI wrap parse error: %s", ex)
            return None

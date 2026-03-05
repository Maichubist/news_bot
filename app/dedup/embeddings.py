from __future__ import annotations

import numpy as np
from typing import Optional
import logging

from app.http import RequestsSession

log = logging.getLogger("dedup.embeddings")


class OpenAIEmbeddingClient:
    def __init__(self, http: RequestsSession, api_key: str, model: str):
        self.http = http
        self.api_key = api_key
        self.model = model
        self.disabled = False  # <-- breaker

    def embed(self, text: str) -> Optional[np.ndarray]:
        if self.disabled:
            return None

        text = (text or "").strip()
        if len(text) < 40:
            return None

        r = self.http.post(
            "https://api.openai.com/v1/embeddings",
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": self.model,
                "input": text,
                "encoding_format": "float",
            },
        )

        if r.status_code == 429:
            # якщо це insufficient_quota — відключаємо ембедінги до кінця запуску
            try:
                j = r.json()
                msg = (j.get("error", {}) or {}).get("message", "")
                err_type = (j.get("error", {}) or {}).get("type", "")
            except Exception:
                msg, err_type = "", ""

            if "insufficient_quota" in err_type or "billing" in msg.lower() or "quota" in msg.lower():
                log.error("OpenAI quota/billing issue. Disabling embeddings for this run.")
                self.disabled = True
                return None

            # інші 429 (rate limit) — можна просто пропустити
            log.warning("OpenAI rate-limited (429). Skipping this embedding.")
            return None

        if not r.ok:
            log.warning("OpenAI embeddings error: %s %s", r.status_code, r.text[:300])
            r.raise_for_status()

        data = r.json()
        vec = np.array(data["data"][0]["embedding"], dtype=np.float32)
        return vec
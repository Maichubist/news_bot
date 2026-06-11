from __future__ import annotations

import threading
from typing import Callable, Dict, List


class PromptManager:
    SUPPORTED_KEYS = [
        "post_prompt",
        "digest_prompt",
        "wrap_prompt",
        "market_wrap_prompt",
        "geopolitical_wrap_prompt",
        "tech_wrap_prompt",
        # Phase 3: ranking mode
        "classify_prompt",
        "ranking_prompt",
        # Phase 4: story wraps over event clusters
        "cluster_wrap_prompt",
    ]

    def __init__(self, repo_factory: Callable[[], object], defaults: Dict[str, str]):
        self._repo_factory = repo_factory
        self._defaults = {k: (defaults.get(k) or "").strip() for k in self.SUPPORTED_KEYS}
        self._overrides: Dict[str, str] = {}
        self._lock = threading.RLock()
        self.refresh_from_repo()

    def refresh_from_repo(self) -> None:
        repo = self._repo_factory()
        with self._lock:
            for key in self.SUPPORTED_KEYS:
                value = repo.get_prompt_override(key)
                if value not in (None, ""):
                    self._overrides[key] = str(value).strip()
                else:
                    self._overrides.pop(key, None)

    def get(self, key: str) -> str:
        key = str(key or "").strip()
        with self._lock:
            if key in self._overrides:
                return self._overrides[key]
            return self._defaults.get(key, "")

    def set(self, key: str, value: str) -> None:
        key = str(key or "").strip()
        if key not in self.SUPPORTED_KEYS:
            raise ValueError(f"Unsupported prompt key: {key}")
        cleaned = (value or "").strip()
        repo = self._repo_factory()
        repo.set_prompt_override(key, cleaned)
        with self._lock:
            self._overrides[key] = cleaned

    def reset(self, key: str) -> None:
        key = str(key or "").strip()
        if key not in self.SUPPORTED_KEYS:
            raise ValueError(f"Unsupported prompt key: {key}")
        repo = self._repo_factory()
        repo.delete_prompt_override(key)
        with self._lock:
            self._overrides.pop(key, None)

    def list_keys(self) -> List[str]:
        return list(self.SUPPORTED_KEYS)

    def describe(self) -> str:
        lines = []
        with self._lock:
            for key in self.SUPPORTED_KEYS:
                mode = "override" if key in self._overrides else "default"
                current = self._overrides.get(key) or self._defaults.get(key) or ""
                preview = current.replace("\n", " ")[:90]
                if len(current) > 90:
                    preview += "..."
                lines.append(f"- {key} [{mode}] — {preview or 'empty'}")
        return "\n".join(lines)

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple
import numpy as np


def cosine_sim(a: np.ndarray, b: np.ndarray) -> float:
    denom = float(np.linalg.norm(a) * np.linalg.norm(b))
    if denom == 0.0:
        return 0.0
    return float(np.dot(a, b) / denom)


def pack_vec(vec: np.ndarray) -> bytes:
    return vec.astype(np.float32).tobytes()


def unpack_vec(blob: bytes, dim: int) -> np.ndarray:
    return np.frombuffer(blob, dtype=np.float32, count=dim)


class SemanticDeduper:
    def __init__(self, repo, window_hours: int, threshold: float):
        self.repo = repo
        self.window_hours = window_hours
        self.threshold = threshold

    def find_dup(self, vec: np.ndarray) -> Tuple[Optional[str], Optional[float]]:
        since_iso = (datetime.now(timezone.utc) - timedelta(hours=self.window_hours)).isoformat(timespec="seconds")
        candidates = self.repo.get_recent_embeddings(since_iso=since_iso)

        best_h = None
        best_s = 0.0

        for h, blob, dim in candidates:
            if dim != vec.shape[0]:
                continue
            other = unpack_vec(blob, dim)
            s = cosine_sim(vec, other)
            if s > best_s:
                best_s = s
                best_h = h

        if best_h is not None and best_s >= self.threshold:
            return best_h, float(best_s)

        return None, None
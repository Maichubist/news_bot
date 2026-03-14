from __future__ import annotations

from typing import Optional, Tuple
import numpy as np


def pack_vec(vec: np.ndarray) -> bytes:
    arr = np.asarray(vec, dtype=np.float32)
    return arr.tobytes()


def unpack_vec(blob: bytes) -> np.ndarray:
    return np.frombuffer(blob, dtype=np.float32)


def cosine(a: np.ndarray, b: np.ndarray) -> float:
    denom = np.linalg.norm(a) * np.linalg.norm(b)
    if denom == 0:
        return 0.0
    return float(np.dot(a, b) / denom)


class SemanticDeduper:

    def __init__(self, repo, threshold: float = 0.86, window_hours: int = 48):
        self.repo = repo
        self.threshold = threshold
        self.window_hours = window_hours

    def _decode_vec(self, blob: bytes) -> np.ndarray:
        return unpack_vec(blob)

    def find_best_match(self, vec: np.ndarray) -> Tuple[Optional[str], float]:

        best_hash = None
        best_score = 0.0

        candidates = self.repo.get_recent_embeddings(self.window_hours)

        for row in candidates:

            if isinstance(row, dict):
                h = row["item_hash"]
                blob = row["embedding_blob"]
            else:
                h = row[0]
                blob = row[1]

            other = self._decode_vec(blob)
            score = cosine(vec, other)

            if score > best_score:
                best_score = score
                best_hash = h

        return best_hash, best_score
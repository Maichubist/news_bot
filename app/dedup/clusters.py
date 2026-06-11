from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

import numpy as np

from app.dedup.semantic import cosine, pack_vec, unpack_vec

log = logging.getLogger("dedup.clusters")


@dataclass(frozen=True)
class ClusterAssignment:
    cluster_id: int
    created_new: bool
    similarity: float


class IncrementalClusterer:
    """
    Incremental event clustering over existing embeddings, pure numpy.

    A new item either joins the nearest active cluster (cosine to the centroid
    >= threshold; centroid becomes the incremental mean of member vectors) or
    opens a new cluster. The canonical representative is re-picked on every
    join: the earliest item with the fullest article_text.
    """

    def __init__(self, repo, threshold: float = 0.80, window_hours: int = 48):
        self.repo = repo
        self.threshold = float(threshold)
        self.window_hours = int(window_hours)

    def assign(self, item_hash: str, vec: np.ndarray) -> Optional[ClusterAssignment]:
        v = np.asarray(vec, dtype=np.float32)
        if v.ndim != 1 or v.size == 0:
            return None

        best_id: Optional[int] = None
        best_sim = -1.0
        best_centroid: Optional[np.ndarray] = None
        best_count = 0

        for cl in self.repo.get_active_clusters(self.window_hours):
            centroid = unpack_vec(cl["centroid_blob"])
            if centroid.shape != v.shape:
                continue
            sim = cosine(v, centroid)
            if sim > best_sim:
                best_sim = sim
                best_id = int(cl["cluster_id"])
                best_centroid = centroid
                best_count = int(cl["item_count"] or 1)

        if best_id is not None and best_sim >= self.threshold:
            return self._join(item_hash, v, best_id, best_centroid, best_count, best_sim)
        return self._open(item_hash, v, best_sim)

    def _join(
        self,
        item_hash: str,
        vec: np.ndarray,
        cluster_id: int,
        centroid: np.ndarray,
        item_count: int,
        similarity: float,
    ) -> ClusterAssignment:
        self.repo.set_item_cluster(item_hash, cluster_id)

        # Incremental mean keeps the centroid stable without re-reading all vectors.
        new_centroid = (centroid.astype(np.float64) * item_count + vec.astype(np.float64)) / (item_count + 1)
        new_centroid = new_centroid.astype(np.float32)

        stats = self.repo.get_cluster_stats(cluster_id)
        canonical = self.repo.pick_cluster_canonical(cluster_id) or item_hash
        self.repo.update_cluster(
            cluster_id=cluster_id,
            centroid_blob=pack_vec(new_centroid),
            item_count=int(stats.get("item_count") or item_count + 1),
            source_count=int(stats.get("source_count") or 1),
            canonical_hash=canonical,
        )
        log.info("Item %s joined cluster %d (sim=%.3f)", item_hash[:12], cluster_id, similarity)
        return ClusterAssignment(cluster_id=cluster_id, created_new=False, similarity=similarity)

    def _open(self, item_hash: str, vec: np.ndarray, best_sim: float) -> ClusterAssignment:
        cluster_id = self.repo.create_cluster(
            centroid_blob=pack_vec(vec),
            centroid_dim=int(vec.shape[0]),
            canonical_hash=item_hash,
        )
        self.repo.set_item_cluster(item_hash, cluster_id)
        log.info("Item %s opened cluster %d (best_sim=%.3f)", item_hash[:12], cluster_id, max(best_sim, 0.0))
        return ClusterAssignment(cluster_id=cluster_id, created_new=True, similarity=max(best_sim, 0.0))

from __future__ import annotations

from dataclasses import dataclass

from app.dedup.lexical import LexicalMatch


@dataclass(frozen=True)
class EventMatchDecision:
    match_type: str  # duplicate | same_event | new_event
    matched_item_hash: str | None
    semantic_score: float
    lexical_score: float
    combined_score: float
    reason: str


class EventMatcher:
    def __init__(
        self,
        duplicate_combined_threshold: float = 0.84,
        duplicate_semantic_threshold: float = 0.92,
        duplicate_lexical_threshold: float = 0.46,
        same_event_combined_threshold: float = 0.68,
        same_event_semantic_threshold: float = 0.82,
        same_event_lexical_threshold: float = 0.28,
    ):
        self.duplicate_combined_threshold = duplicate_combined_threshold
        self.duplicate_semantic_threshold = duplicate_semantic_threshold
        self.duplicate_lexical_threshold = duplicate_lexical_threshold
        self.same_event_combined_threshold = same_event_combined_threshold
        self.same_event_semantic_threshold = same_event_semantic_threshold
        self.same_event_lexical_threshold = same_event_lexical_threshold

    def decide(self, semantic_item_hash: str | None, semantic_score: float | None, lexical: LexicalMatch) -> EventMatchDecision:
        sem = float(semantic_score or 0.0)
        lex = float(lexical.score or 0.0)
        combined = 0.62 * sem + 0.38 * lex
        matched_hash = semantic_item_hash or lexical.item_hash

        duplicate_signal = matched_hash and (
            (sem >= self.duplicate_semantic_threshold and combined >= self.duplicate_combined_threshold and (lex >= self.duplicate_lexical_threshold or lexical.entity_overlap >= 0.72 or lexical.keyword_overlap >= 0.72))
            or (sem >= 0.96 and lexical.entity_overlap >= 0.60 and lexical.keyword_overlap >= 0.55)
        )
        if duplicate_signal:
            return EventMatchDecision('duplicate', matched_hash, sem, lex, combined, 'high_semantic_and_lexical_overlap')

        same_event_signal = (
            (matched_hash and combined >= self.same_event_combined_threshold and sem >= self.same_event_semantic_threshold and (lex >= self.same_event_lexical_threshold or lexical.entity_overlap >= 0.40))
            or (matched_hash and sem >= 0.88 and lexical.entity_overlap >= 0.45 and lexical.keyword_overlap >= 0.30)
            or (matched_hash and sem >= 0.84 and lexical.keyword_overlap >= 0.55)
        )
        if same_event_signal:
            return EventMatchDecision('same_event', matched_hash, sem, lex, combined, 'same_event_cluster_candidate')

        return EventMatchDecision('new_event', None, sem, lex, combined, 'no_strong_event_match')

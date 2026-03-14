from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class EventDecision:
    match_type: str  # "duplicate" | "same_event" | "new_event"
    matched_item_hash: Optional[str]
    semantic_score: float
    lexical_score: float
    combined_score: float


class EventMatcher:
    def __init__(
        self,
        duplicate_combined_threshold: float = 0.88,
        duplicate_semantic_threshold: float = 0.90,
        duplicate_lexical_threshold: float = 0.70,
        same_event_combined_threshold: float = 0.77,
        same_event_semantic_threshold: float = 0.78,
        same_event_lexical_threshold: float = 0.52,
    ):
        self.duplicate_combined_threshold = duplicate_combined_threshold
        self.duplicate_semantic_threshold = duplicate_semantic_threshold
        self.duplicate_lexical_threshold = duplicate_lexical_threshold
        self.same_event_combined_threshold = same_event_combined_threshold
        self.same_event_semantic_threshold = same_event_semantic_threshold
        self.same_event_lexical_threshold = same_event_lexical_threshold

    def decide(
        self,
        semantic_match_hash: Optional[str],
        semantic_score: float,
        lexical_match,
    ) -> EventDecision:
        semantic_score = float(semantic_score or 0.0)
        lexical_score = float(getattr(lexical_match, "score", 0.0) or 0.0)
        lexical_hash = getattr(lexical_match, "item_hash", None)

        # Якщо lexical знайшов кандидата, а semantic ні — теж можна матчити
        matched_hash = semantic_match_hash or lexical_hash

        # Semantic важливіший, lexical — підсилює same-event
        combined = 0.65 * semantic_score + 0.35 * lexical_score

        # duplicate:
        # 1) дуже сильний semantic
        # 2) або сильний combined + lexical теж достатньо високий
        if matched_hash and (
            semantic_score >= self.duplicate_semantic_threshold
            or (
                combined >= self.duplicate_combined_threshold
                and lexical_score >= self.duplicate_lexical_threshold
            )
        ):
            return EventDecision(
                match_type="duplicate",
                matched_item_hash=matched_hash,
                semantic_score=semantic_score,
                lexical_score=lexical_score,
                combined_score=combined,
            )

        # same_event:
        # м'якші пороги, щоб ловити різні переписані версії однієї події
        if matched_hash and (
            combined >= self.same_event_combined_threshold
            or semantic_score >= self.same_event_semantic_threshold
            or lexical_score >= self.same_event_lexical_threshold
        ):
            return EventDecision(
                match_type="same_event",
                matched_item_hash=matched_hash,
                semantic_score=semantic_score,
                lexical_score=lexical_score,
                combined_score=combined,
            )

        return EventDecision(
            match_type="new_event",
            matched_item_hash=None,
            semantic_score=semantic_score,
            lexical_score=lexical_score,
            combined_score=combined,
        )
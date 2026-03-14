from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional
from collections import Counter
import math
import re


TOKEN_RE = re.compile(r"[a-zA-Zа-яА-ЯіІїЇєЄ0-9][a-zA-Zа-яА-ЯіІїЇєЄ0-9\-']+")


STOPWORDS = {
    "the", "and", "for", "with", "from", "that", "this", "what", "when",
    "after", "before", "have", "has", "had", "about", "into", "over",
    "under", "while", "where", "will", "would", "could", "should",
    "they", "them", "their", "said", "says", "amid", "than",
    "also", "more", "less", "just", "still", "already",
    "update", "updates", "live", "news", "latest"
}


@dataclass
class LexicalCandidate:
    item_hash: str
    normalized_text: str
    keywords: list[str]
    entities: list[str]
    numbers: list[str]
    event_verbs: list[str]


@dataclass
class LexicalMatch:
    item_hash: Optional[str]
    score: float
    text_score: float
    keyword_overlap: float
    entity_overlap: float
    number_overlap: float
    verb_overlap: float


def tokenize(text: str) -> list[str]:
    return _tokenize(text)


def _tokenize(text: str) -> list[str]:
    tokens = TOKEN_RE.findall((text or "").lower())
    out: list[str] = []

    for t in tokens:
        if len(t) < 3:
            continue
        if t in STOPWORDS:
            continue
        if t.isdigit():
            continue
        out.append(t)

    return out


def _counter_cosine(a: Counter, b: Counter) -> float:
    if not a or not b:
        return 0.0

    common = set(a.keys()) & set(b.keys())
    num = sum(a[k] * b[k] for k in common)

    den_a = math.sqrt(sum(v * v for v in a.values()))
    den_b = math.sqrt(sum(v * v for v in b.values()))
    den = den_a * den_b

    if den == 0:
        return 0.0

    return float(num / den)


def _jaccard(a: Iterable[str], b: Iterable[str]) -> float:
    sa = {str(x).strip().lower() for x in (a or []) if str(x).strip()}
    sb = {str(x).strip().lower() for x in (b or []) if str(x).strip()}

    if not sa or not sb:
        return 0.0

    inter = sa & sb
    union = sa | sb

    if not union:
        return 0.0

    return float(len(inter) / len(union))


class LexicalDeduper:
    def __init__(
        self,
        max_keywords: int = 12,
        same_event_lexical_threshold: float = 0.52,
        duplicate_lexical_threshold: float = 0.70,
        max_candidates: int | None = None,
        **kwargs,
    ):
        # max_candidates тут приймаємо лише для сумісності з bootstrap.py
        self.max_keywords = max_keywords
        self.same_event_lexical_threshold = same_event_lexical_threshold
        self.duplicate_lexical_threshold = duplicate_lexical_threshold
        self.max_candidates = max_candidates

    def _text_score(self, left: str, right: str) -> float:
        lt = Counter(_tokenize(left))
        rt = Counter(_tokenize(right))
        return _counter_cosine(lt, rt)

    def score_pair(self, left: LexicalCandidate, right: LexicalCandidate) -> LexicalMatch:
        text_score = self._text_score(left.normalized_text, right.normalized_text)

        keyword_overlap = _jaccard(
            left.keywords[: self.max_keywords],
            right.keywords[: self.max_keywords],
        )
        entity_overlap = _jaccard(left.entities, right.entities)
        number_overlap = _jaccard(left.numbers, right.numbers)
        verb_overlap = _jaccard(left.event_verbs, right.event_verbs)

        score = (
            0.42 * text_score
            + 0.28 * keyword_overlap
            + 0.20 * entity_overlap
            + 0.05 * number_overlap
            + 0.05 * verb_overlap
        )

        if entity_overlap >= 0.50 and keyword_overlap >= 0.40:
            score += 0.06

        if text_score >= 0.75 and keyword_overlap >= 0.50:
            score += 0.04

        score = min(float(score), 1.0)

        return LexicalMatch(
            item_hash=right.item_hash,
            score=score,
            text_score=text_score,
            keyword_overlap=keyword_overlap,
            entity_overlap=entity_overlap,
            number_overlap=number_overlap,
            verb_overlap=verb_overlap,
        )

    def find_best_match(
        self,
        candidate: LexicalCandidate,
        candidates: list[LexicalCandidate],
    ) -> Optional[LexicalMatch]:
        best: Optional[LexicalMatch] = None

        # Якщо список дуже великий і max_candidates заданий — обрізаємо
        if self.max_candidates and self.max_candidates > 0:
            candidates = candidates[: self.max_candidates]

        for other in candidates:
            if not other.item_hash or other.item_hash == candidate.item_hash:
                continue

            match = self.score_pair(candidate, other)

            if best is None or match.score > best.score:
                best = match

        return best
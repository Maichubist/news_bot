from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List

try:
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity
    SKLEARN_OK = True
except Exception:  # pragma: no cover
    TfidfVectorizer = None
    cosine_similarity = None
    SKLEARN_OK = False


@dataclass(frozen=True)
class LexicalCandidate:
    item_hash: str
    normalized_text: str
    keywords: list[str]
    entities: list[str]
    numbers: list[str]
    event_verbs: list[str]


@dataclass(frozen=True)
class LexicalMatch:
    item_hash: str | None
    score: float
    word_score: float
    char_score: float
    keyword_overlap: float
    entity_overlap: float
    number_overlap: float
    verb_overlap: float


class LexicalDeduper:
    def __init__(self, max_candidates: int = 150):
        self.max_candidates = max_candidates

    def find_best_match(self, current: LexicalCandidate, candidates: Iterable[LexicalCandidate]) -> LexicalMatch:
        pool = [c for c in candidates if c.item_hash != current.item_hash][: self.max_candidates]
        if not pool:
            return LexicalMatch(None, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)

        word_scores = self._tfidf_scores(current.normalized_text, [c.normalized_text for c in pool], analyzer='word', ngram_range=(1, 2))
        char_scores = self._tfidf_scores(current.normalized_text, [c.normalized_text for c in pool], analyzer='char_wb', ngram_range=(3, 5))

        best: LexicalMatch | None = None
        for idx, cand in enumerate(pool):
            keyword_overlap = self._jaccard(current.keywords, cand.keywords)
            entity_overlap = self._jaccard(self._norm_list(current.entities), self._norm_list(cand.entities))
            number_overlap = self._jaccard(current.numbers, cand.numbers)
            verb_overlap = self._jaccard(current.event_verbs, cand.event_verbs)
            word_score = word_scores[idx]
            char_score = char_scores[idx]
            score = (
                0.24 * word_score
                + 0.18 * char_score
                + 0.18 * max(word_score, char_score)
                + 0.18 * keyword_overlap
                + 0.17 * entity_overlap
                + 0.03 * number_overlap
                + 0.02 * verb_overlap
            )
            match = LexicalMatch(
                item_hash=cand.item_hash,
                score=float(score),
                word_score=float(word_score),
                char_score=float(char_score),
                keyword_overlap=float(keyword_overlap),
                entity_overlap=float(entity_overlap),
                number_overlap=float(number_overlap),
                verb_overlap=float(verb_overlap),
            )
            if best is None or match.score > best.score:
                best = match

        return best or LexicalMatch(None, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)

    def _tfidf_scores(self, current_text: str, candidate_texts: List[str], analyzer: str, ngram_range: tuple[int, int]) -> list[float]:
        if not candidate_texts:
            return []
        if SKLEARN_OK and TfidfVectorizer is not None and cosine_similarity is not None:
            try:
                vectorizer = TfidfVectorizer(analyzer=analyzer, ngram_range=ngram_range, min_df=1)
                matrix = vectorizer.fit_transform([current_text] + candidate_texts)
                sims = cosine_similarity(matrix[0:1], matrix[1:]).flatten()
                return [float(x) for x in sims.tolist()]
            except Exception:
                pass
        return [self._fallback_overlap(current_text, txt) for txt in candidate_texts]

    def _fallback_overlap(self, a: str, b: str) -> float:
        sa = set(a.split())
        sb = set(b.split())
        if not sa or not sb:
            return 0.0
        return len(sa & sb) / len(sa | sb)

    def _jaccard(self, a: Iterable[str], b: Iterable[str]) -> float:
        sa = {str(x).strip().lower() for x in a if str(x).strip()}
        sb = {str(x).strip().lower() for x in b if str(x).strip()}
        if not sa or not sb:
            return 0.0
        return len(sa & sb) / len(sa | sb)

    def _norm_list(self, xs: Iterable[str]) -> list[str]:
        return [str(x).strip().lower() for x in xs if str(x).strip()]

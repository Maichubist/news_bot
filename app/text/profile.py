from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List

from app.text.normalize import normalize_article_text

STOPWORDS = {
    "a", "an", "and", "are", "as", "at", "be", "been", "but", "by", "for", "from", "had", "has", "have",
    "he", "her", "his", "if", "in", "into", "is", "it", "its", "of", "on", "or", "said", "says", "say",
    "that", "the", "their", "them", "they", "this", "to", "was", "were", "will", "with", "would", "after",
    "amid", "among", "before", "can", "could", "during", "into", "more", "most", "new", "over", "than",
    "through", "under", "up", "via", "what", "when", "where", "which", "who", "why", "you", "your",
    "about", "news", "update", "updates", "live", "breaking", "latest", "today", "yesterday", "tomorrow",
}

EVENT_VERBS = {
    "approve", "approves", "approved", "ban", "bans", "banned", "block", "blocks", "blocked", "buy", "buys", "bought",
    "cut", "cuts", "cutting", "delay", "delays", "delayed", "deploy", "deploys", "deployed", "elect", "elected",
    "expand", "expands", "expanded", "impose", "imposes", "imposed", "increase", "increases", "increased",
    "launch", "launches", "launched", "limit", "limits", "limited", "meet", "meets", "met", "open", "opens", "opened",
    "pass", "passes", "passed", "plan", "plans", "planned", "prepare", "prepares", "prepared", "raise", "raises", "raised",
    "reject", "rejects", "rejected", "resume", "resumes", "resumed", "sanction", "sanctions", "sanctioned", "sell", "sells", "sold",
    "sign", "signs", "signed", "slash", "slashes", "slashed", "start", "starts", "started", "strike", "strikes", "struck",
    "suspend", "suspends", "suspended", "target", "targets", "targeted", "vote", "votes", "voted", "warn", "warns", "warned",
}

ENTITY_PAT = re.compile(r"\b[A-Z][a-z]+(?:[ -][A-Z][a-z]+){0,3}\b")
NUMBER_PAT = re.compile(r"(?:[$€£¥]|usd|eur|uah)?\s?\d[\d,\.]*\s?(?:%|bn|billion|mn|million|trillion|k)?", re.IGNORECASE)
TOKEN_PAT = re.compile(r"[a-z0-9][a-z0-9\-]{1,}")


@dataclass(frozen=True)
class TextProfile:
    normalized_title: str
    normalized_summary: str
    normalized_text: str
    keywords: List[str]
    keyphrases: List[str]
    entities: List[str]
    numbers: List[str]
    event_verbs: List[str]


class TextProfileBuilder:
    def __init__(self, max_keywords: int = 12, max_keyphrases: int = 8):
        self.max_keywords = max_keywords
        self.max_keyphrases = max_keyphrases

    def build(self, title: str, summary: str | None = None) -> TextProfile:
        title = (title or "").strip()
        summary = (summary or "").strip()
        normalized_title = normalize_article_text(title)
        normalized_summary = normalize_article_text(summary)
        normalized_text = normalized_title if not normalized_summary else f"{normalized_title}\n\n{normalized_summary}"

        title_tokens = self._tokens(normalized_title)
        summary_tokens = self._tokens(normalized_summary)
        all_tokens = title_tokens + summary_tokens
        keywords = self._keywords(title_tokens, summary_tokens)
        keyphrases = self._keyphrases(title, summary)
        entities = self._entities(title, summary)
        numbers = self._numbers(title + "\n" + summary)
        event_verbs = sorted({tok for tok in all_tokens if tok in EVENT_VERBS})[:6]

        return TextProfile(
            normalized_title=normalized_title,
            normalized_summary=normalized_summary,
            normalized_text=normalized_text,
            keywords=keywords,
            keyphrases=keyphrases,
            entities=entities,
            numbers=numbers,
            event_verbs=event_verbs,
        )

    def _tokens(self, text: str) -> List[str]:
        out = []
        for tok in TOKEN_PAT.findall(text.lower()):
            if tok in STOPWORDS:
                continue
            if len(tok) <= 2:
                continue
            if tok.isdigit() and len(tok) <= 2:
                continue
            out.append(tok)
        return out

    def _keywords(self, title_tokens: List[str], summary_tokens: List[str]) -> List[str]:
        scores: dict[str, float] = {}
        for tok in title_tokens:
            scores[tok] = scores.get(tok, 0.0) + 2.0
        for tok in summary_tokens:
            scores[tok] = scores.get(tok, 0.0) + 1.0
        ranked = sorted(scores.items(), key=lambda kv: (-kv[1], kv[0]))
        return [k for k, _ in ranked[: self.max_keywords]]

    def _keyphrases(self, title: str, summary: str) -> List[str]:
        text = f"{title} {summary}".strip()
        phrases: list[str] = []
        for m in re.finditer(r"\b([A-Za-z][A-Za-z0-9'’\-]+(?:\s+[A-Za-z][A-Za-z0-9'’\-]+){1,2})\b", text):
            phrase = normalize_article_text(m.group(1))
            parts = [p for p in phrase.split() if p not in STOPWORDS]
            if len(parts) < 2:
                continue
            if phrase not in phrases:
                phrases.append(phrase)
            if len(phrases) >= self.max_keyphrases:
                break
        return phrases

    def _entities(self, title: str, summary: str) -> List[str]:
        text = f"{title}. {summary}".strip()
        entities: list[str] = []
        for m in ENTITY_PAT.finditer(text):
            ent = m.group(0).strip()
            low = ent.lower()
            if low in STOPWORDS or len(ent) < 3:
                continue
            if ent not in entities:
                entities.append(ent)
        return entities[:12]

    def _numbers(self, text: str) -> List[str]:
        seen: list[str] = []
        for m in NUMBER_PAT.finditer(text):
            num = re.sub(r"\s+", "", m.group(0).lower())
            if len(num) < 2:
                continue
            if num not in seen:
                seen.append(num)
        return seen[:10]

from __future__ import annotations

from dataclasses import dataclass
import re

from app.text.normalize import normalize_text
from app.dedup.lexical import tokenize


EVENT_VERB_HINTS = {
    "attack", "attacks", "attacked",
    "strike", "strikes", "struck",
    "hit", "hits",
    "launch", "launched", "launches",
    "approve", "approves", "approved",
    "reject", "rejects", "rejected",
    "vote", "votes", "voted",
    "sanction", "sanctions", "sanctioned",
    "ban", "bans", "banned",
    "impose", "imposes", "imposed",
    "warn", "warns", "warned",
    "say", "says", "said",
    "announce", "announces", "announced",
    "confirm", "confirms", "confirmed",
    "suspend", "suspends", "suspended",
    "resume", "resumes", "resumed",
    "deploy", "deploys", "deployed",
    "kill", "kills", "killed",
    "injure", "injures", "injured",
    "arrest", "arrests", "arrested",
    "evacuate", "evacuates", "evacuated",
    "raise", "raises", "raised",
    "cut", "cuts",
    "surge", "surges", "surged",
    "fall", "falls", "fell",
    # --- українські дієслова-маркери подій ---
    "атакували", "атакував", "атакувала",
    "обстріляли", "обстріляв", "обстріл",
    "вдарили", "вдарив", "удар",
    "запустили", "запустив",
    "ухвалили", "ухвалив", "ухвалила", "ухвалено",
    "схвалили", "схвалив", "схвалено",
    "відхилили", "відхилив", "відхилено",
    "проголосували", "проголосував",
    "заборонили", "заборонив", "заборонено",
    "запровадили", "запровадив", "запроваджено",
    "попередили", "попередив", "попередила",
    "заявив", "заявила", "заявили",
    "оголосили", "оголосив", "оголосила", "оголошено",
    "підтвердили", "підтвердив", "підтвердила", "підтверджено",
    "призупинили", "призупинив", "призупинено",
    "відновили", "відновив", "відновлено",
    "розгорнули", "розгорнув",
    "загинули", "загинув", "загинула",
    "поранили", "поранено", "поранені",
    "затримали", "затримав", "затримано",
    "заарештували", "заарештовано",
    "евакуювали", "евакуйовано",
    "підвищили", "підвищив", "підвищено",
    "скоротили", "скоротив", "скорочено",
    "зросли", "зріс", "зросла", "зростання",
    "впали", "впав", "впала", "падіння",
    "підписали", "підписав", "підписала", "підписано",
    "звільнили", "звільнив", "звільнено",
    "призначили", "призначив", "призначено",
    "знеструмлено", "знеструмили",
    "збили", "збив", "збито",
}


# Latin OR Cyrillic capitalized tokens count as entity candidates.
ENTITY_TOKEN_RE = re.compile(
    r"\b(?:[A-Z][A-Za-z0-9&.\-]{2,}|[А-ЯІЇЄҐ][А-Яа-яІіЇїЄєҐґ0-9&.\-']{2,})\b"
)
NUMBER_RE = re.compile(
    r"\b(?:\d+(?:[.,]\d+)?%?|\$\d+(?:[.,]\d+)?(?:bn|m|million|billion)?|\d+(?:[.,]\d+)?(?:bn|m|million|billion))\b",
    re.IGNORECASE,
)


@dataclass
class TextProfile:
    normalized_text: str
    keywords: list[str]
    entities: list[str]
    numbers: list[str]
    event_verbs: list[str]


class TextProfileBuilder:
    def __init__(self, max_keywords: int = 12):
        self.max_keywords = max_keywords

    def _extract_entities(self, title: str, summary: str | None) -> list[str]:
        raw = f"{title or ''} {summary or ''}".strip()
        entities: list[str] = []
        seen: set[str] = set()

        for m in ENTITY_TOKEN_RE.finditer(raw):
            val = m.group(0).strip()
            key = val.lower()
            if len(val) < 3:
                continue
            if key in seen:
                continue
            seen.add(key)
            entities.append(val)

        return entities[:10]

    def _extract_numbers(self, title: str, summary: str | None) -> list[str]:
        raw = f"{title or ''} {summary or ''}".strip()
        nums: list[str] = []
        seen: set[str] = set()

        for m in NUMBER_RE.finditer(raw):
            val = m.group(0).strip()
            key = val.lower()
            if key in seen:
                continue
            seen.add(key)
            nums.append(val)

        return nums[:10]

    def _extract_event_verbs(self, normalized_text: str) -> list[str]:
        toks = tokenize(normalized_text)
        verbs: list[str] = []
        seen: set[str] = set()

        for t in toks:
            if t in EVENT_VERB_HINTS and t not in seen:
                seen.add(t)
                verbs.append(t)

        return verbs[:8]

    def _build_keywords(
        self,
        title: str,
        summary: str | None,
        entities: list[str],
        numbers: list[str],
    ) -> list[str]:
        norm_title = normalize_text(title or "")
        norm_summary = normalize_text(summary or "")

        title_tokens = tokenize(norm_title)
        summary_tokens = tokenize(norm_summary)

        weighted: list[str] = []
        weighted.extend(title_tokens)
        weighted.extend(title_tokens)
        weighted.extend(title_tokens)
        weighted.extend(summary_tokens)

        for ent in entities:
            ent_norm = normalize_text(ent)
            weighted.extend(tokenize(ent_norm))

        for num in numbers:
            num_norm = normalize_text(num)
            weighted.extend(tokenize(num_norm))

        score: dict[str, int] = {}
        for tok in weighted:
            if len(tok) < 3:
                continue
            score[tok] = score.get(tok, 0) + 1

        ranked = sorted(score.items(), key=lambda x: (-x[1], x[0]))
        return [k for k, _ in ranked[: self.max_keywords]]

    def build(self, title: str, summary: str | None) -> TextProfile:
        normalized_title = normalize_text(title or "")
        normalized_summary = normalize_text(summary or "")
        normalized_text = (normalized_title + " " + normalized_summary).strip()

        entities = self._extract_entities(title or "", summary or "")
        numbers = self._extract_numbers(title or "", summary or "")
        event_verbs = self._extract_event_verbs(normalized_text)
        keywords = self._build_keywords(title or "", summary or "", entities, numbers)

        return TextProfile(
            normalized_text=normalized_text,
            keywords=keywords,
            entities=entities,
            numbers=numbers,
            event_verbs=event_verbs,
        )
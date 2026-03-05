import re
from typing import Optional


_HTML_TAG_RE = re.compile(r"<[^>]+>")
_URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)

_BAD_SUMMARY_FRAGMENTS = [
    "read more",
    "continue reading",
    "click here",
    "view on",
    "the post appeared first on",
    "appeared first on",
    "подробиці",
    "читати далі",
    "перейти",
]


def strip_html(s: str) -> str:
    s = _HTML_TAG_RE.sub(" ", s or "")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def clean_summary(raw: str) -> str:
    s = strip_html(raw)
    s = _URL_RE.sub("", s)
    s = re.sub(r"\s+", " ", s).strip(" -•\t\n\r")
    return s


def is_good_summary(s: Optional[str]) -> bool:
    if not s:
        return False

    s_norm = s.strip()
    if len(s_norm) < 80:
        return False

    lower = s_norm.lower()
    for frag in _BAD_SUMMARY_FRAGMENTS:
        if frag in lower:
            return False

    letters = sum(ch.isalpha() for ch in s_norm)
    if letters / max(1, len(s_norm)) < 0.55:
        return False

    return True


def truncate(s: str, max_len: int = 350) -> str:
    s = (s or "").strip()
    if len(s) <= max_len:
        return s
    return s[:max_len].rstrip() + "…"
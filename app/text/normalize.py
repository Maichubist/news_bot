from __future__ import annotations

import re
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode


NOISE_PATTERNS = [
    r"\bwhat we know\b",
    r"\bwhat is happening\b",
    r"\blive updates?\b",
    r"\bas it happened\b",
    r"\bminute[- ]by[- ]minute\b",
    r"\bday \d+\b",
]

TRACKING_QUERY_KEYS = {
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "utm_id",
    "gclid",
    "fbclid",
    "mc_cid",
    "mc_eid",
    "ref",
    "ref_src",
    "ref_url",
    "src",
    "source",
    "s",
    "smid",
    "cmpid",
    "taid",
}


def normalize_text(text: str | None) -> str:
    text = (text or "").strip().lower()

    for pattern in NOISE_PATTERNS:
        text = re.sub(pattern, " ", text, flags=re.IGNORECASE)

    # прибираємо html entities-подібний шум
    text = text.replace("&nbsp;", " ")
    text = text.replace("&amp;", "&")

    # прибираємо зайві числа/коди типу 039, 2026 тощо як окремі токени
    text = re.sub(r"\b\d+\b", " ", text)

    # прибираємо все, що не букви/цифри/пробіли/базова пунктуація
    text = re.sub(r"[^\w\s\-:/.,%$€£]", " ", text, flags=re.UNICODE)

    # схлопуємо пробіли
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize(text: str | None) -> str:
    """
    Залишаємо короткий alias для сумісності з новим кодом.
    """
    return normalize_text(text)


def canonicalize_url(url: str | None) -> str:
    url = (url or "").strip()
    if not url:
        return ""

    try:
        parts = urlsplit(url)
    except Exception:
        return url

    scheme = (parts.scheme or "https").lower()
    netloc = (parts.netloc or "").lower()
    path = parts.path or "/"

    # прибираємо trailing slash, але не ламаємо root
    if path != "/":
        path = path.rstrip("/")

    # викидаємо tracking query params
    clean_query = []
    for k, v in parse_qsl(parts.query, keep_blank_values=True):
        if k.lower() in TRACKING_QUERY_KEYS:
            continue
        clean_query.append((k, v))

    query = urlencode(clean_query, doseq=True)

    # fragment для news dedup не потрібен
    fragment = ""

    return urlunsplit((scheme, netloc, path, query, fragment))
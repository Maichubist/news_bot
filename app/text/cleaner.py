from __future__ import annotations

import re

BAD_PHRASES = [
    "це означає",
    "це свідчить",
    "це може означати",
    "це може свідчити",
    "може призвести",
    "ймовірно",
    "імовірно",
]


def clean_post_text(text: str) -> str:
    t = (text or "").strip()
    if not t:
        return ""

    for phrase in BAD_PHRASES:
        t = re.sub(re.escape(phrase), "", t, flags=re.IGNORECASE)

    t = re.sub(r"\s+", " ", t)
    t = re.sub(r"\s+([,.;:!?])", r"", t)
    t = re.sub(r"([.!?])\s*([A-ZА-ЯІЇЄ])", r" ", t)
    return t.strip()

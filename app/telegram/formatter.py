from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
from app.text.summary import is_good_summary, truncate


@dataclass
class PostFormatter:
    def __init__(self, include_source: bool = True):
        self.include_source = include_source

    def format(self, title: str, link: str, source: str, summary: Optional[str]) -> str:
        # Legacy fallback formatter (used if no LLM post_text)
        lines = [title.strip()]

        if is_good_summary(summary):
            lines.append("")
            lines.append(truncate(summary, max_len=350))

        if self.include_source:
            lines.append("")
            lines.append(f"— SRC[{source}]({link})")

        return "\n".join(lines)

    def format_row(self, row) -> str:
        base = (row["post_text"] or "").strip()
        link = row["link"]
        source = row["source"]

        lines = [base]
        if self.include_source:
            lines.extend(["", f"— SRC[{source}]({link})"])
        return "\n".join(lines)
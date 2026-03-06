from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
from app.text.summary import is_good_summary, truncate


@dataclass
class PostFormatter:
    def __init__(self, include_source: bool = True):
        self.include_source = include_source

    def format(self, title: str, link: str, source: str, summary: Optional[str]) -> str:
        lines = [title.strip()]
        if is_good_summary(summary):
            lines.extend(["", truncate(summary, max_len=350)])
        if self.include_source and source and link:
            lines.extend(["", f"— SRC[{source}]({link})"])
        elif self.include_source and source:
            lines.extend(["", f"— {source}"])
        return "\n".join(lines)

    def format_row(self, row) -> str:
        base = (row["post_text"] or "").strip()
        link = row.get("link") if hasattr(row, "get") else row["link"]
        source = row.get("source") if hasattr(row, "get") else row["source"]
        try:
            hashtag = (row.get("category_hashtag") or "").strip() if hasattr(row, "get") else (row["category_hashtag"] or "").strip()
        except Exception:
            hashtag = ""
        lines = [base]
        if hashtag:
            lines.extend(["", hashtag])
        if self.include_source and source and link:
            lines.extend(["", f"— SRC[{source}]({link})"])
        elif self.include_source and source:
            lines.extend(["", f"— {source}"])
        return "\n".join(lines)

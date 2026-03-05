from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(frozen=True)
class NewsItem:
    source: str
    title: str
    link: str
    summary: Optional[str]
    published_at: Optional[datetime]
    image_url: Optional[str] = None
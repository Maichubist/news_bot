from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Tuple


@dataclass(frozen=True)
class NewsItem:
    source: str
    title: str
    link: str
    summary: Optional[str]
    published_at: Optional[datetime]
    image_url: Optional[str] = None
    # NEW: origin of the source ("ua" | "world"), used for 50/50 balance
    origin: str = "world"
    # NEW: best video attached to the item (from RSS enclosures / media:content)
    video_url: Optional[str] = None
    # NEW: all candidate images from the feed entry, best-first
    images: Tuple[str, ...] = field(default_factory=tuple)

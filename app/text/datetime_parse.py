from datetime import datetime, timezone
from typing import Any, Optional
from dateutil import parser as dtparser


def parse_datetime(entry: Any) -> Optional[datetime]:
    if getattr(entry, "published_parsed", None):
        try:
            dt = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
            return dt
        except Exception:
            pass

    for key in ("published", "updated", "pubDate"):
        val = getattr(entry, key, None)
        if val:
            try:
                dt = dtparser.parse(val)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc)
            except Exception:
                continue

    return None
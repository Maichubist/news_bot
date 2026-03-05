from __future__ import annotations

import logging
import re
from typing import List

import feedparser

from app.config import SourceCfg
from app.models import NewsItem
from app.http import RequestsSession
from app.text.normalize import canonicalize_url
from app.text.summary import clean_summary
from app.text.datetime_parse import parse_datetime


log = logging.getLogger("rss.fetcher")


class RssFetcher:
    def __init__(self, http: RequestsSession):
        self.http = http

        # Pre-compiled global filters (optional, may be missing in older configs)
        self._global_deny_title: list[re.Pattern] = []
        self._global_deny_url: list[re.Pattern] = []
        self._global_deny_summary: list[re.Pattern] = []

    def set_global_filters(self, deny_title: list[str], deny_url: list[str], deny_summary: list[str]) -> None:
        def _compile_many(items: list[str]) -> list[re.Pattern]:
            out: list[re.Pattern] = []
            for pat in items or []:
                try:
                    out.append(re.compile(pat, re.IGNORECASE))
                except re.error:
                    log.warning("Invalid regex pattern ignored: %s", pat)
            return out

        self._global_deny_title = _compile_many(deny_title)
        self._global_deny_url = _compile_many(deny_url)
        self._global_deny_summary = _compile_many(deny_summary)

    def _is_denied(self, src: SourceCfg, title: str, link: str, summary: str | None) -> bool:
        t = (title or "").strip()
        u = (link or "").strip()
        s = (summary or "").strip()

        # Global patterns
        for p in self._global_deny_title:
            if p.search(t):
                return True
        for p in self._global_deny_url:
            if p.search(u):
                return True
        if s:
            for p in self._global_deny_summary:
                if p.search(s):
                    return True

        # Per-source patterns
        for pat in (src.deny_title_regex or []):
            try:
                if re.search(pat, t, flags=re.IGNORECASE):
                    return True
            except re.error:
                continue
        for pat in (src.deny_url_regex or []):
            try:
                if re.search(pat, u, flags=re.IGNORECASE):
                    return True
            except re.error:
                continue

        return False

    def _pick_rss_image(self, entry) -> str | None:
        # Try common RSS fields: media:content, media_thumbnail, enclosures
        try:
            media = getattr(entry, "media_content", None)
            if media and isinstance(media, list):
                for m in media:
                    url = (m.get("url") or "").strip()
                    if url:
                        return canonicalize_url(url)
        except Exception:
            pass

        try:
            thumbs = getattr(entry, "media_thumbnail", None)
            if thumbs and isinstance(thumbs, list):
                for t in thumbs:
                    url = (t.get("url") or "").strip()
                    if url:
                        return canonicalize_url(url)
        except Exception:
            pass

        try:
            enclosures = getattr(entry, "enclosures", None)
            if enclosures and isinstance(enclosures, list):
                for e in enclosures:
                    url = (e.get("href") or e.get("url") or "").strip()
                    if url:
                        return canonicalize_url(url)
        except Exception:
            pass

        return None

    def _fetch_with_fallback_ua(self, url: str, user_agents: list[str] | None) -> str | None:
        # Some feeds block generic agents; try a small UA rotation on 401/403/406.
        uas = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"]
        if user_agents:
            uas = list(dict.fromkeys([*user_agents, *uas]))

        base_headers = {
            "Accept": "application/rss+xml,application/xml;q=0.9,text/xml;q=0.8,*/*;q=0.7",
            "Accept-Language": "en-US,en;q=0.9,uk-UA;q=0.8,uk;q=0.7",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }

        last_status: int | None = None
        for ua in uas[:4]:
            try:
                r = self.http.get(url, headers={**base_headers, "User-Agent": ua})
            except Exception as ex:
                log.warning("Feed fetch error for %s: %s", url, ex)
                return None

            last_status = r.status_code
            if r.status_code in (401, 403, 406):
                continue
            if not r.ok:
                log.warning("Feed HTTP error %s for %s", r.status_code, url)
                return None
            return r.text

        log.warning("Feed blocked (last HTTP %s): %s", last_status, url)
        return None

    def fetch(self, sources: List[SourceCfg], limit_per_feed: int = 50) -> List[NewsItem]:
        items: List[NewsItem] = []

        for src in sources:
            try:
                feed_text = self._fetch_with_fallback_ua(src.url, None)
                if not feed_text:
                    continue

                feed = feedparser.parse(feed_text)
                if getattr(feed, "bozo", False):
                    log.warning("Bozo feed: %s", src.url)
                    continue

                for e in feed.entries[:limit_per_feed]:
                    title = getattr(e, "title", "").strip()
                    link = getattr(e, "link", "").strip()
                    if not title or not link:
                        continue

                    published_at = parse_datetime(e)

                    raw_summary = getattr(e, "summary", "") or getattr(e, "description", "") or ""
                    summary = clean_summary(raw_summary) if raw_summary else None

                    # Fast filtering (ads/promos/videos/newsletters/etc.)
                    if self._is_denied(src, title=title, link=link, summary=summary):
                        continue

                    image_url = self._pick_rss_image(e)

                    items.append(
                        NewsItem(
                            source=src.name,
                            title=title,
                            link=canonicalize_url(link),
                            summary=summary,
                            published_at=published_at,
                            image_url=image_url,
                        )
                    )
            except Exception as ex:
                log.exception("Failed to fetch feed %s: %s", src.url, ex)

        return items
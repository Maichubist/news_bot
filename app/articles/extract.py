from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import List, Optional

log = logging.getLogger("articles.extract")

try:
    import trafilatura  # type: ignore

    _HAS_TRAFILATURA = True
except Exception:  # pragma: no cover
    trafilatura = None
    _HAS_TRAFILATURA = False
    log.warning("trafilatura is not installed; falling back to naive HTML text extraction")


_HTML_TAG_RE = re.compile(r"<[^>]+>")
_SCRIPT_STYLE_RE = re.compile(r"<(script|style|noscript)[^>]*>.*?</\1>", re.IGNORECASE | re.DOTALL)

# <meta property="og:image" content="..."> and friends; attribute order may vary
_META_RE = re.compile(
    r"<meta\s+[^>]*?(?:property|name)\s*=\s*[\"\'](?P<key>[^\"\']+)[\"\'][^>]*?content\s*=\s*[\"\'](?P<val>[^\"\']+)[\"\']",
    re.IGNORECASE,
)
_META_RE_REV = re.compile(
    r"<meta\s+[^>]*?content\s*=\s*[\"\'](?P<val>[^\"\']+)[\"\'][^>]*?(?:property|name)\s*=\s*[\"\'](?P<key>[^\"\']+)[\"\']",
    re.IGNORECASE,
)

_IMAGE_KEYS = ("og:image", "og:image:url", "og:image:secure_url", "twitter:image", "twitter:image:src")
_VIDEO_KEYS = (
    "og:video",
    "og:video:url",
    "og:video:secure_url",
    "twitter:player:stream",
)

_VIDEO_EXT_RE = re.compile(r"\.(mp4|mov|webm|m4v)(\?|#|$)", re.IGNORECASE)
_IMAGE_EXT_RE = re.compile(r"\.(jpe?g|png|webp|gif|avif)(\?|#|$)", re.IGNORECASE)

# Phase 5: embeddable players (YouTube/Vimeo). Linked in the post, NEVER downloaded.
_EMBED_KEYS = ("og:video", "og:video:url", "og:video:secure_url", "twitter:player")
_EMBED_DOMAIN_RE = re.compile(
    r"https?://(?:[\w.-]*\.)?(?:youtube\.com|youtube-nocookie\.com|youtu\.be|vimeo\.com)/",
    re.IGNORECASE,
)
_IFRAME_SRC_RE = re.compile(r"<iframe[^>]+src\s*=\s*[\"\'](?P<src>[^\"\']+)[\"\']", re.IGNORECASE)


@dataclass
class ArticleContent:
    text: Optional[str] = None
    images: List[str] = field(default_factory=list)
    video_url: Optional[str] = None
    # Player URL (YouTube/Vimeo) for a "▶️ Відео:" link in the post
    embed_video_url: Optional[str] = None


def _collect_meta(html_text: str) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for rx in (_META_RE, _META_RE_REV):
        for m in rx.finditer(html_text):
            key = m.group("key").strip().lower()
            val = m.group("val").strip()
            if not val:
                continue
            out.setdefault(key, [])
            if val not in out[key]:
                out[key].append(val)
    return out


def _naive_text(html_text: str, max_chars: int) -> Optional[str]:
    body = _SCRIPT_STYLE_RE.sub(" ", html_text)
    # Prefer <article>/<p> content when present
    paras = re.findall(r"<p[^>]*>(.*?)</p>", body, re.IGNORECASE | re.DOTALL)
    if paras:
        text = "\n".join(_HTML_TAG_RE.sub(" ", p) for p in paras)
    else:
        text = _HTML_TAG_RE.sub(" ", body)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n\n", text).strip()
    if len(text) < 200:
        return None
    return text[:max_chars]


def extract_article(html_text: str, url: str = "", max_chars: int = 3000) -> ArticleContent:
    """
    Extract main article text + media candidates from a fetched HTML page.
    Designed to be called once per item (the same fetch we already do for og:image).
    """
    out = ArticleContent()
    html_text = (html_text or "")[:600_000]
    if not html_text:
        return out

    meta = _collect_meta(html_text)

    # Images, best-first, deduped
    seen: set[str] = set()
    for key in _IMAGE_KEYS:
        for val in meta.get(key, []):
            v = val.strip()
            if v and v not in seen and not v.startswith("data:"):
                seen.add(v)
                out.images.append(v)

    # Video: only direct-file URLs are usable for Telegram sendVideo.
    # Player/iframe URLs (youtube embeds etc.) are skipped on purpose.
    for key in _VIDEO_KEYS:
        for val in meta.get(key, []):
            v = val.strip()
            if v and _VIDEO_EXT_RE.search(v):
                out.video_url = v
                break
        if out.video_url:
            break

    # Embeddable player (YouTube/Vimeo): keep the URL for a text link,
    # do not download anything.
    for key in _EMBED_KEYS:
        for val in meta.get(key, []):
            v = val.strip()
            if v and _EMBED_DOMAIN_RE.match(v):
                out.embed_video_url = v
                break
        if out.embed_video_url:
            break
    if not out.embed_video_url:
        for m in _IFRAME_SRC_RE.finditer(html_text):
            src = m.group("src").strip()
            if _EMBED_DOMAIN_RE.match(src):
                out.embed_video_url = src
                break

    # Full text
    if _HAS_TRAFILATURA:
        try:
            text = trafilatura.extract(
                html_text,
                url=url or None,
                include_comments=False,
                include_tables=False,
                favor_recall=True,
            )
            if text and len(text.strip()) >= 200:
                out.text = text.strip()[:max_chars]
        except Exception:
            log.exception("trafilatura extract failed for %s", url)

    if not out.text:
        out.text = _naive_text(html_text, max_chars=max_chars)

    return out


def looks_like_image_url(url: str) -> bool:
    return bool(_IMAGE_EXT_RE.search(url or ""))


def looks_like_video_url(url: str) -> bool:
    return bool(_VIDEO_EXT_RE.search(url or ""))

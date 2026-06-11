from __future__ import annotations

import json
import logging
import re
from typing import List, Optional, Tuple

log = logging.getLogger("services.publisher")


class ChannelPublisher:
    """
    Media download + send logic shared by the pipeline and the moderation flow.
    Extracted from NewsPipeline without behaviour changes: video -> first working
    image -> plain text, every step degrades gracefully.
    """

    def __init__(
        self,
        http,
        tg,
        enable_video: bool = True,
        max_image_bytes: int = 5 * 1024 * 1024,
        max_video_bytes: int = 20 * 1024 * 1024,
        enable_og_image: bool = True,
    ):
        self.http = http
        self.tg = tg
        self.enable_video = bool(enable_video)
        self.max_image_bytes = int(max_image_bytes)
        self.max_video_bytes = int(max_video_bytes)
        self.enable_og_image = bool(enable_og_image)

    # ------------------------------------------------------------------
    # Downloads
    # ------------------------------------------------------------------

    def download_binary(self, url: str, accept: str, max_bytes: int) -> Optional[Tuple[bytes, str]]:
        """
        Download a media file with a hard size cap. Checks Content-Length first,
        then enforces the cap on the actual body as a fallback.
        Returns (bytes, content_type) or None.
        """
        try:
            r = self.http.get(url, headers={"Accept": accept})
        except Exception:
            return None
        if not getattr(r, "ok", False):
            return None
        try:
            declared = int(r.headers.get("Content-Length") or 0)
        except Exception:
            declared = 0
        if declared and declared > max_bytes:
            log.info("Media too large by Content-Length (%d > %d): %s", declared, max_bytes, url)
            return None
        content = r.content or b""
        if not content or len(content) > max_bytes:
            if content:
                log.info("Media too large after download (%d > %d): %s", len(content), max_bytes, url)
            return None
        ct = (r.headers.get("Content-Type") or "").lower()
        return content, ct

    def download_image(self, url: str) -> Optional[Tuple[bytes, str]]:
        dl = self.download_binary(
            url,
            accept="image/avif,image/webp,image/*,*/*;q=0.8",
            max_bytes=self.max_image_bytes,
        )
        if not dl:
            return None
        content, ct = dl
        if ct and not ct.startswith("image/"):
            return None
        ext = ".jpg"
        if "png" in ct:
            ext = ".png"
        elif "webp" in ct:
            ext = ".webp"
        elif "gif" in ct:
            ext = ".gif"
        return content, f"image{ext}"

    def download_video(self, url: str) -> Optional[Tuple[bytes, str]]:
        dl = self.download_binary(
            url,
            accept="video/mp4,video/*;q=0.9,*/*;q=0.5",
            max_bytes=self.max_video_bytes,
        )
        if not dl:
            return None
        content, ct = dl
        if ct and not (ct.startswith("video/") or ct == "application/octet-stream"):
            return None
        ext = ".mp4"
        if "webm" in ct:
            ext = ".webm"
        elif "quicktime" in ct:
            ext = ".mov"
        return content, f"video{ext}"

    # ------------------------------------------------------------------
    # Sending
    # ------------------------------------------------------------------

    def send_media(
        self,
        text: str,
        video_url: Optional[str] = None,
        image_urls: Optional[List[str]] = None,
        chat_id: Optional[int] = None,
        reply_markup: Optional[dict] = None,
    ) -> Tuple[bool, Optional[int]]:
        """
        Media priority: video -> first working image -> plain text.
        A broken media URL never blocks the post. Returns (ok, message_id).
        """
        # 1) Video
        if self.enable_video and video_url:
            dl = self.download_video(video_url)
            if dl:
                bts, fname = dl
                ok, msg_id = self.tg.send_video_with_id(
                    bts, caption_text=text, filename=fname, chat_id=chat_id, reply_markup=reply_markup
                )
                if ok:
                    return True, msg_id
                log.warning("sendVideo failed, falling back to image/text")

        # 2) Images: try candidates in order until one downloads and sends
        for img_url in (image_urls or [])[:4]:
            if not img_url:
                continue
            dl = self.download_image(img_url)
            if not dl:
                continue
            bts, fname = dl
            ok, msg_id = self.tg.send_photo_with_id(
                bts,
                caption_text=text,
                disable_preview=True,
                filename=fname,
                chat_id=chat_id,
                reply_markup=reply_markup,
            )
            if ok:
                return True, msg_id
            log.warning("sendPhoto failed for %s, trying next candidate", img_url)

        # 3) Plain text
        return self.tg.send_message_with_id(text, disable_preview=True, chat_id=chat_id, reply_markup=reply_markup)

    # ------------------------------------------------------------------
    # Row helpers
    # ------------------------------------------------------------------

    def extract_og_image(self, url: str) -> Optional[str]:
        if not self.enable_og_image or not url:
            return None
        try:
            r = self.http.get(url)
        except Exception:
            return None
        if not getattr(r, "ok", False):
            return None
        html_text = (getattr(r, "text", "") or "")[:200000]
        m = re.search(
            r'<meta\s+property=["\']og:image["\']\s+content=["\']([^"\']+)["\']',
            html_text,
            re.IGNORECASE,
        )
        if not m:
            m = re.search(
                r'<meta\s+name=["\']twitter:image["\']\s+content=["\']([^"\']+)["\']',
                html_text,
                re.IGNORECASE,
            )
        return m.group(1).strip() if m else None

    def media_from_row(self, row) -> Tuple[Optional[str], List[str]]:
        """Build (video_url, image_candidates) from a DB row, og-fetch as last resort."""

        def _get(key: str):
            try:
                return row.get(key) if hasattr(row, "get") else row[key]
            except Exception:
                return None

        video_url = _get("video_url") if self.enable_video else None

        images: List[str] = []
        primary = _get("image_url")
        if primary:
            images.append(str(primary))
        raw = _get("images_json")
        if raw:
            try:
                for u in json.loads(raw) or []:
                    su = str(u)
                    if su and su not in images:
                        images.append(su)
            except Exception:
                pass

        if not images and not video_url:
            og = self.extract_og_image(str(_get("link") or ""))
            if og:
                images.append(og)

        return (str(video_url) if video_url else None), images

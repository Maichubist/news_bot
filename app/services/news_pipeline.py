from __future__ import annotations

import logging
import re
import time
from datetime import datetime, timezone
from typing import Optional, Tuple

from app.text.summary import is_good_summary, truncate
from app.dedup.semantic import pack_vec


log = logging.getLogger("services.pipeline")


class NewsPipeline:
    def __init__(self, cfg, repo, rss, exact, embedder, semantic, tg, formatter, postmaker, digestmaker):
        self.cfg = cfg
        self.repo = repo
        self.rss = rss
        self.exact = exact
        self.embedder = embedder
        self.semantic = semantic
        self.tg = tg
        self.formatter = formatter
        self.postmaker = postmaker
        self.digestmaker = digestmaker

        # дефолти без зміни config.yaml
        self.score_threshold = float(getattr(cfg, "score_threshold", 0.72))
        self.digest_hour_local = int(getattr(cfg, "digest_hour_local", 21))

        posting_cfg = getattr(cfg, "posting", None)
        self.cluster_wait_minutes = int(getattr(posting_cfg, "cluster_wait_minutes", 5) or 5)
        self.breaking_sources_threshold = int(getattr(posting_cfg, "breaking_sources_threshold", 3) or 3)

        images_cfg = getattr(cfg, "images", None)
        self.enable_og_image = bool(getattr(images_cfg, "og_fetch", True))

    def run_once(self) -> None:
        items = self.rss.fetch(self.cfg.sources)

        inserted = 0
        embedded = 0
        semantic_dups = 0
        scored = 0
        approved = 0

        # cutoff для економії LLM: обробляємо (скоринг/пост) лише свіжі новини
        now_utc = datetime.now(timezone.utc)
        cutoff_hours = int(getattr(self.cfg.posting, "only_last_hours", 0) or 0)

        for it in items:
            h = self.exact.make_hash(it.title, it.link)
            published_iso = (
                it.published_at.astimezone(timezone.utc).isoformat(timespec="seconds")
                if it.published_at
                else None
            )

            is_new = self.repo.upsert_item(
                item_hash=h,
                source=it.source,
                title=it.title,
                link=it.link,
                summary=it.summary,
                published_at_utc=published_iso,
                image_url=getattr(it, "image_url", None),
            )
            if not is_new:
                continue
            inserted += 1

            # якщо новина занадто стара — зберігаємо в БД, але не витрачаємо embeddings/LLM
            if cutoff_hours > 0 and it.published_at:
                age_h = (now_utc - it.published_at.astimezone(timezone.utc)).total_seconds() / 3600.0
                if age_h > cutoff_hours:
                    continue

            if self.cfg.embeddings.require_good_summary and not is_good_summary(it.summary):
                continue

            # Embeddings should be robust: always include summary if present (even if короткий).
            text_for_vec = (it.title or "").strip()
            if it.summary:
                text_for_vec += "\n\n" + truncate(it.summary, max_len=500)

            try:
                vec = self.embedder.embed(text_for_vec)
            except Exception as ex:
                log.warning("Embedding failed: %s", ex)
                vec = None

            if vec is None:
                continue

            embedded += 1
            dup_of, dup_score = self.semantic.find_dup(vec)
            if dup_of is not None:
                semantic_dups += 1

            self.repo.set_embedding_and_dup(
                item_hash=h,
                embedding_blob=pack_vec(vec),
                embedding_dim=int(vec.shape[0]),
                embedding_model=self.cfg.openai.model,
                dup_of=dup_of,
                dup_score=dup_score,
            )

            # якщо це семантичний дубль — не скоримо (публікуватимемо root)
            if dup_of is not None:
                continue

            # 2) rank+write лише для root новини
            decision = self.postmaker.make(
                title=it.title,
                summary=it.summary,
                source=it.source,
                url=it.link,
            )
            if not decision:
                continue

            scored += 1
            should_post = bool(decision.should_post and decision.score >= self.score_threshold and decision.post_text)
            if should_post:
                approved += 1

            self.repo.set_score_and_posttext(
                item_hash=h,
                score=decision.score,
                should_post=should_post,
                post_text=decision.post_text if should_post else None,
                why=decision.why,
            )

        posted = self._post_pending_roots(only_last_hours=cutoff_hours)

        log.info(
            "Collected=%d InsertedNew=%d Embedded=%d SemanticDups=%d Scored=%d Approved=%d Posted=%d threshold=%.2f",
            len(items),
            inserted,
            embedded,
            semantic_dups,
            scored,
            approved,
            posted,
            self.score_threshold,
        )

        removed = self.repo.cleanup_old(self.cfg.db.keep_days)
        if removed:
            log.info("DB cleanup removed %d rows", removed)

        self.maybe_post_daily_digest()

    def _extract_og_image(self, url: str) -> Optional[str]:
        if not self.enable_og_image:
            return None
        try:
            r = self.rss.http.get(url)
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
        if not m:
            return None
        return m.group(1).strip()

    def _download_image(self, url: str) -> Optional[Tuple[bytes, str]]:
        try:
            r = self.rss.http.get(url, headers={"Accept": "image/avif,image/webp,image/*,*/*;q=0.8"})
        except Exception:
            return None
        if not getattr(r, "ok", False):
            return None
        ct = (r.headers.get("Content-Type") or "").lower()
        ext = ".jpg"
        if "png" in ct:
            ext = ".png"
        elif "webp" in ct:
            ext = ".webp"
        return r.content, f"image{ext}"

    def _post_pending_roots(self, only_last_hours: int) -> int:
        if only_last_hours <= 0:
            only_last_hours = 24

        now_utc = datetime.now(timezone.utc)
        posted = 0

        pending = self.repo.pick_pending_roots(only_last_hours=only_last_hours, limit=50)
        for row in pending:
            root_hash = row["item_hash"]
            created_iso = row["created_at_utc"]
            try:
                created_dt = datetime.fromisoformat(created_iso)
            except Exception:
                created_dt = now_utc

            sources_cnt = self.repo.cluster_sources_count(root_hash)
            age_min = (now_utc - created_dt).total_seconds() / 60.0

            breaking = sources_cnt >= self.breaking_sources_threshold
            can_post = breaking or age_min >= float(self.cluster_wait_minutes)
            if not can_post:
                continue

            post_text = (row["post_text"] or "").strip()
            if not post_text:
                continue

            # Prevent silly "Заголовок" as title
            lines = [l.rstrip() for l in post_text.split("\n")]
            if lines and lines[0].strip().lower() in ("заголовок", "headline", "title"):
                lines[0] = (row["title"] or "").strip() or "Новина"
                post_text = "\n".join(lines).strip()

            if breaking and not post_text.startswith("⚡"):
                post_text = "⚡ " + post_text

            extra = "" if sources_cnt <= 1 else f"· ще {sources_cnt - 1} джерел"
            text = self.formatter.format_row({"post_text": post_text, "link": row["link"], "source": row["source"]})
            if extra:
                text = text + " " + extra

            # pick image: prefer OG over RSS image_url
            img_url = row["image_url"]
            og = self._extract_og_image(row["link"])
            if og:
                img_url = og
                self.repo.update_image_url(root_hash, og)

            ok = False
            if img_url:
                dl = self._download_image(img_url)
                if dl:
                    bts, fname = dl
                    ok, _ = self.tg.send_photo_with_id(bts, caption_text=text, disable_preview=True, filename=fname)
                else:
                    ok, _ = self.tg.send_message_with_id(text, disable_preview=True)
            else:
                ok, _ = self.tg.send_message_with_id(text, disable_preview=True)

            if not ok:
                self.repo.mark_error(root_hash)
                break

            self.repo.mark_posted(root_hash)
            posted += 1
            time.sleep(self.cfg.app.sleep_between_posts_sec)

            if posted >= self.cfg.posting.max_posts_per_run:
                break

        return posted

    def maybe_post_daily_digest(self) -> None:
        now_local = datetime.now()
        if now_local.hour != self.digest_hour_local:
            return

        day_utc = datetime.now(timezone.utc).date().isoformat()
        if self.repo.daily_summary_exists(day_utc):
            return

        posts = self.repo.get_post_texts_for_day(day_utc)
        digest = self.digestmaker.make(day_label=day_utc, posts=posts)
        if not digest:
            return

        ok, _ = self.tg.send_message_with_id(digest.post_text, disable_preview=True)
        if ok:
            self.repo.save_daily_summary(day_utc, digest.post_text)
            log.info("Daily digest posted for %s", day_utc)

from __future__ import annotations

import logging
import re
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, Tuple

from app.dedup.semantic import pack_vec
from app.text.summary import is_good_summary, truncate

log = logging.getLogger("services.pipeline")


class NewsPipeline:
    def __init__(self, cfg, repo, rss, exact, embedder, semantic, tg, formatter, postmaker, digestmaker, wrapmaker=None):
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
        self.wrapmaker = wrapmaker

        self.score_threshold = float(getattr(cfg, "score_threshold", 0.72))
        self.digest_hour_local = int(getattr(cfg, "digest_hour_local", 21))
        posting_cfg = getattr(cfg, "posting", None)
        self.cluster_wait_minutes = int(getattr(posting_cfg, "cluster_wait_minutes", 5) or 5)
        self.breaking_sources_threshold = int(getattr(posting_cfg, "breaking_sources_threshold", 3) or 3)
        self.wrap_rules = list(getattr(posting_cfg, "wrap_rules", []) or [])
        self.wrap_category_map: Dict[str, object] = {}
        for rule in self.wrap_rules:
            for cat in getattr(rule, "categories", []) or []:
                self.wrap_category_map.setdefault(cat, rule)
        images_cfg = getattr(cfg, "images", None)
        self.enable_og_image = bool(getattr(images_cfg, "og_fetch", True))

    def run_once(self) -> None:
        inserted = embedded = semantic_dups = scored = approved = posted = 0
        now_utc = datetime.now(timezone.utc)
        cutoff_hours = int(getattr(self.cfg.posting, "only_last_hours", 0) or 0)

        try:
            if posted < self.cfg.posting.max_posts_per_run:
                posted += self._post_pending_roots(only_last_hours=cutoff_hours, limit=1)
        except Exception:
            log.exception("Pre-post pending roots failed")

        items = self.rss.fetch(self.cfg.sources)

        for it in items:
            h = self.exact.make_hash(it.title, it.link)
            published_iso = it.published_at.astimezone(timezone.utc).isoformat(timespec="seconds") if it.published_at else None
            is_new = self.repo.upsert_item(item_hash=h, source=it.source, title=it.title, link=it.link, summary=it.summary, published_at_utc=published_iso, image_url=getattr(it, "image_url", None))
            if not is_new:
                continue
            inserted += 1

            if cutoff_hours > 0 and it.published_at:
                age_h = (now_utc - it.published_at.astimezone(timezone.utc)).total_seconds() / 3600.0
                if age_h > cutoff_hours:
                    continue

            if self.cfg.embeddings.require_good_summary and not is_good_summary(it.summary):
                continue

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
            self.repo.set_embedding_and_dup(item_hash=h, embedding_blob=pack_vec(vec), embedding_dim=int(vec.shape[0]), embedding_model=self.cfg.openai.model, dup_of=dup_of, dup_score=dup_score)
            if dup_of is not None:
                continue

            decision = self.postmaker.make(title=it.title, summary=it.summary, source=it.source, url=it.link)
            if not decision:
                continue
            scored += 1

            should_post = bool(decision.should_post and decision.score >= self.score_threshold and decision.post_text)
            if should_post:
                approved += 1

            cat_slug = (getattr(decision, "category", "") or "").strip() or "other"
            cat_id = self.repo.category_id(cat_slug) or self.repo.category_id("other")
            wrap_rule = self.wrap_category_map.get(cat_slug)
            requested_mode = (getattr(decision, "publish_mode", "") or "digest").strip() or "digest"
            effective_mode = requested_mode
            status_override = None
            wrap_name = None
            if should_post and wrap_rule and requested_mode == "wrap_candidate":
                effective_mode = "wrap_candidate"
                status_override = "pending_wrap"
                wrap_name = getattr(wrap_rule, "name", None)
            elif not should_post and requested_mode != "drop":
                effective_mode = "digest"

            self.repo.set_score_and_posttext(
                item_hash=h,
                score=decision.score,
                should_post=should_post,
                post_text=decision.post_text if decision.post_text else None,
                why=decision.why,
                category_id=cat_id,
                tier=getattr(decision, "tier", "C"),
                publish_mode=effective_mode,
                event_key=getattr(decision, "event_key", ""),
                novelty_score=getattr(decision, "novelty_score", 0.0),
                impact_score=getattr(decision, "impact_score", 0.0),
                ua_relevance_score=getattr(decision, "ua_relevance_score", 0.0),
                wrap_name=wrap_name,
                status=status_override,
            )

            if should_post and effective_mode == "post" and posted < self.cfg.posting.max_posts_per_run:
                ok = self._post_now(item_hash=h, title=it.title, source=it.source, link=it.link, image_url=getattr(it, "image_url", None), post_text=decision.post_text, category_slug=cat_slug)
                if ok:
                    posted += 1

        if posted < self.cfg.posting.max_posts_per_run:
            posted += self._process_wraps(limit=self.cfg.posting.max_posts_per_run - posted)

        log.info(
            "Collected=%d InsertedNew=%d Embedded=%d SemanticDups=%d Scored=%d Approved=%d Posted=%d threshold=%.2f",
            len(items), inserted, embedded, semantic_dups, scored, approved, posted, self.score_threshold,
        )

        removed = self.repo.cleanup_old(self.cfg.db.keep_days)
        if removed:
            log.info("DB cleanup removed %d rows", removed)
        self.maybe_post_daily_digest()

    def _post_now(self, item_hash: str, title: str, source: str, link: str, image_url: Optional[str], post_text: str, category_slug: str) -> bool:
        try:
            text = (post_text or "").strip()
            if not text:
                return False
            lines = [l.rstrip() for l in text.split("\n")]
            if lines and lines[0].strip().lower() in ("заголовок", "headline", "title"):
                lines[0] = (title or "").strip() or "Новина"
                text = "\n".join(lines).strip()
            hashtag = self._hashtag_for_slug(category_slug)
            formatted = self.formatter.format_row({"post_text": text, "link": link, "source": source, "category_hashtag": hashtag})
            img_url = image_url
            og = self._extract_og_image(link)
            if og:
                img_url = og
                try:
                    self.repo.update_image_url(item_hash, og)
                except Exception:
                    pass
            ok = self._send_with_optional_photo(img_url=img_url, text=formatted)
            if not ok:
                self.repo.mark_error(item_hash)
                return False
            self.repo.mark_posted(item_hash)
            time.sleep(self.cfg.app.sleep_between_posts_sec)
            return True
        except Exception:
            log.exception("Immediate post failed")
            try:
                self.repo.mark_error(item_hash)
            except Exception:
                pass
            return False

    def _send_with_optional_photo(self, img_url: Optional[str], text: str) -> bool:
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
        return ok

    def _hashtag_for_slug(self, slug: str) -> str:
        for c in getattr(self.cfg, "categories", None) or []:
            if (getattr(c, "slug", "") or "").strip() == slug:
                return (getattr(c, "hashtag", "") or "").strip()
        for c in getattr(self.cfg, "categories", None) or []:
            if (getattr(c, "slug", "") or "").strip() == "other":
                return (getattr(c, "hashtag", "") or "").strip()
        return ""

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
        m = re.search(r'<meta\s+property=["\']og:image["\']\s+content=["\']([^"\']+)["\']', html_text, re.IGNORECASE)
        if not m:
            m = re.search(r'<meta\s+name=["\']twitter:image["\']\s+content=["\']([^"\']+)["\']', html_text, re.IGNORECASE)
        return m.group(1).strip() if m else None

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

    def _post_pending_roots(self, only_last_hours: int, limit: int = 50) -> int:
        if only_last_hours <= 0:
            only_last_hours = 24
        now_utc = datetime.now(timezone.utc)
        posted = 0
        pending = self.repo.pick_pending_roots(only_last_hours=only_last_hours, limit=int(limit))
        for row in pending:
            root_hash = row["item_hash"]
            try:
                created_dt = datetime.fromisoformat(row["created_at_utc"])
            except Exception:
                created_dt = now_utc
            sources_cnt = self.repo.cluster_sources_count(root_hash)
            age_min = (now_utc - created_dt).total_seconds() / 60.0
            breaking = sources_cnt >= self.breaking_sources_threshold
            if not (breaking or age_min >= float(self.cluster_wait_minutes)):
                continue
            post_text = (row["post_text"] or "").strip()
            if not post_text:
                continue
            if breaking and not post_text.startswith("⚡"):
                post_text = "⚡ " + post_text
            formatted = self.formatter.format_row({
                "post_text": post_text,
                "link": row["link"],
                "source": row["source"],
                "category_hashtag": (row["category_hashtag"] or "").strip() if row["category_hashtag"] else "",
            })
            ok = self._send_with_optional_photo(img_url=row["image_url"] or self._extract_og_image(row["link"]), text=formatted)
            if not ok:
                self.repo.mark_error(root_hash)
                break
            self.repo.mark_posted(root_hash)
            posted += 1
            time.sleep(self.cfg.app.sleep_between_posts_sec)
            if posted >= self.cfg.posting.max_posts_per_run:
                break
        return posted

    def _process_wraps(self, limit: int) -> int:
        if not self.wrapmaker or not self.wrap_rules or limit <= 0:
            return 0
        posted = 0
        now_utc = datetime.now(timezone.utc)
        for rule in self.wrap_rules:
            if posted >= limit:
                break
            last_posted = self.repo.get_last_wrap_posted_at(rule.name)
            if last_posted:
                try:
                    last_dt = datetime.fromisoformat(last_posted)
                    if now_utc - last_dt < timedelta(minutes=int(rule.cooldown_minutes)):
                        continue
                except Exception:
                    pass
            rows = self.repo.pick_wrap_candidates(wrap_name=rule.name, lookback_hours=int(rule.lookback_hours), limit=12)
            if len(rows) < int(rule.min_items):
                continue
            sources = {str(r["source"]) for r in rows}
            if len(sources) < int(rule.min_sources):
                continue
            items = [dict(r) for r in rows]
            decision = self.wrapmaker.make(wrap_name=rule.name, items=items, prompt_template=getattr(rule, "prompt_template", ""))
            if not decision or not decision.post_text.strip():
                continue
            hashtag = self._hashtag_for_slug(getattr(rule, "hashtag_slug", "") or "other")
            lead_row = rows[0]
            formatted = self.formatter.format_row({
                "post_text": decision.post_text.strip(),
                "link": lead_row["link"],
                "source": f"{lead_row['source']} +{max(0, len(sources)-1)}",
                "category_hashtag": hashtag,
            })
            ok, _ = self.tg.send_message_with_id(formatted, disable_preview=True)
            if not ok:
                break
            item_hashes = [str(r["item_hash"]) for r in rows]
            wrap_post_id = self.repo.save_wrap_post(rule.name, item_hashes=item_hashes, source_count=len(sources), post_text=decision.post_text.strip())
            self.repo.mark_wrapped(item_hashes=item_hashes, wrap_post_id=wrap_post_id)
            posted += 1
            time.sleep(self.cfg.app.sleep_between_posts_sec)
        return posted

    def maybe_post_daily_digest(self) -> None:
        now_local = datetime.now()
        if now_local.hour != self.digest_hour_local:
            return
        day_utc = datetime.now(timezone.utc).date().isoformat()
        if self.repo.daily_summary_exists(day_utc):
            return
        posts = self.repo.get_post_texts_for_day(day_utc)
        if not posts:
            return
        digest = self.digestmaker.make(day_label=day_utc, posts=posts)
        if not digest:
            return
        ok, _ = self.tg.send_message_with_id(digest.post_text, disable_preview=True)
        if ok:
            self.repo.save_daily_summary(day_utc, digest.post_text)
            log.info("Daily digest posted for %s", day_utc)

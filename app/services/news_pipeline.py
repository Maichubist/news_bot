from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone, timedelta
from types import SimpleNamespace
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from app.articles.extract import extract_article, ArticleContent
from app.dedup.lexical import LexicalCandidate
from app.dedup.semantic import pack_vec
from app.services.editorial_policy import DelayCfg, TopicQuotaCfg, SaturationCfg, OriginBalanceCfg, decide_publish_mode
from app.text.summary import is_good_summary, truncate

log = logging.getLogger("services.pipeline")

# Story-development wrap for an event cluster (clustering mode).
# Can be overridden at runtime via /prompt_set cluster_wrap_prompt.
DEFAULT_CLUSTER_WRAP_PROMPT = """Ти редактор українського Telegram-каналу. Нижче — хронологія одного сюжету:
повідомлення різних джерел про ту саму подію в порядку появи для {wrap_name}.

Напиши пост «розвиток сюжету» українською:
- 1 сильний заголовок про суть сюжету
- 2-4 короткі речення: як подія розвивалася — що сталося спочатку,
  що додалося, куди рухається
- покажи РОЗВИТОК (нові факти, цифри, реакції), а не переказуй кожне повідомлення
- максимум 650 символів
- без вигаданих фактів, без води

Хронологія:
{items}"""


class NewsPipeline:
    def __init__(
        self,
        cfg,
        repo,
        rss,
        exact,
        embedder,
        semantic,
        tg,
        formatter,
        postmaker,
        digestmaker,
        wrapmaker=None,
        prompt_manager=None,
        profile_builder=None,
        lexical=None,
        event_matcher=None,
        publisher=None,
        moderation=None,
        engagement=None,
        clusterer=None,
    ):
        self.cfg = cfg
        self.repo = repo
        self.rss = rss
        self.exact = exact
        self.embedder = embedder
        self.semantic = semantic
        self.profile_builder = profile_builder
        self.lexical = lexical
        self.event_matcher = event_matcher
        self.tg = tg
        self.formatter = formatter
        self.postmaker = postmaker
        self.digestmaker = digestmaker
        self.wrapmaker = wrapmaker
        self.prompt_manager = prompt_manager
        self.moderation = moderation
        self.engagement = engagement

        # Phase 3: comparative ranking. The ranker is attached in bootstrap after
        # construction (it needs pipeline callbacks). With the flag off the legacy
        # absolute-threshold path below runs unchanged.
        ranking_cfg = getattr(cfg, "ranking", None)
        self.ranking_enabled = bool(getattr(ranking_cfg, "enabled", False))
        self.ranker = None

        # Phase 4: embedding-based event clusters. With the flag off, event_key
        # matching and category wraps keep working exactly as before.
        clustering_cfg = getattr(cfg, "clustering", None)
        self.clustering_enabled = bool(getattr(clustering_cfg, "enabled", False))
        self.clusterer = clusterer
        cw = getattr(clustering_cfg, "wrap", None)
        self.cluster_wrap_min_items = int(getattr(cw, "min_items", 3) or 3)
        self.cluster_wrap_min_sources = int(getattr(cw, "min_sources", 2) or 2)
        self.cluster_wrap_lookback_hours = int(getattr(cw, "lookback_hours", 6) or 6)
        self.cluster_wrap_cooldown_minutes = int(getattr(cw, "cooldown_minutes", 90) or 90)

        self.score_threshold = float(getattr(getattr(cfg, "editorial", None), "min_post_score", 0.84))
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

        tp_cfg = getattr(cfg, "text_processing", None)
        self.text_processing_enabled = bool(getattr(tp_cfg, "enabled", True))
        self.matching_window_hours = int(
            getattr(tp_cfg, "matching_window_hours", getattr(cfg.embeddings, "window_hours", 36))
            or getattr(cfg.embeddings, "window_hours", 36)
        )
        self.max_matching_candidates = int(getattr(tp_cfg, "max_candidates", 150) or 150)

        editorial = getattr(cfg, "editorial", None)
        tq = getattr(editorial, "topic_quota", None)
        ts = getattr(editorial, "topic_saturation", None)
        dd = getattr(editorial, "delay_non_breaking", None)
        sw = getattr(editorial, "source_weighting", None)
        ob = getattr(editorial, "origin_balance", None)
        self.topic_quota_cfg = TopicQuotaCfg(
            window1_hours=int(getattr(tq, "window1_hours", 4) or 4),
            max_posts_window1=int(getattr(tq, "max_posts_window1", 1) or 1),
            window2_hours=int(getattr(tq, "window2_hours", 12) or 12),
            max_posts_window2=int(getattr(tq, "max_posts_window2", 2) or 2),
        )
        self.topic_saturation_cfg = SaturationCfg(
            lookback_hours=int(getattr(ts, "lookback_hours", 12) or 12),
            max_topic_share=float(getattr(ts, "max_topic_share", 0.40) or 0.40),
            min_posts=int(getattr(ts, "min_posts", 5) or 5),
        )
        self.delay_cfg = DelayCfg(
            min_minutes=int(getattr(dd, "min_minutes", 10) or 10),
            max_minutes=int(getattr(dd, "max_minutes", 20) or 20),
        )
        self.high_trust_sources = list(getattr(sw, "high_trust_sources", []) or [])
        self.origin_balance_cfg = OriginBalanceCfg(
            enabled=bool(getattr(ob, "enabled", True)),
            lookback_hours=int(getattr(ob, "lookback_hours", 12) or 12),
            max_origin_share=float(getattr(ob, "max_origin_share", 0.60) or 0.60),
            min_posts=int(getattr(ob, "min_posts", 6) or 6),
            ua_relevance_boost=float(getattr(ob, "ua_relevance_boost", 0.10) or 0.0),
        )

        articles_cfg = getattr(cfg, "articles", None)
        self.fetch_full_text = bool(getattr(articles_cfg, "fetch_full_text", True))
        self.article_max_chars = int(getattr(articles_cfg, "max_chars", 3000) or 3000)

        media_cfg = getattr(cfg, "media", None)
        self.enable_video = bool(getattr(media_cfg, "enable_video", True))
        self.max_image_bytes = int(getattr(media_cfg, "max_image_bytes", 5 * 1024 * 1024) or 5 * 1024 * 1024)
        self.max_video_bytes = int(getattr(media_cfg, "max_video_bytes", 20 * 1024 * 1024) or 20 * 1024 * 1024)

        # Media download/send logic lives in ChannelPublisher (shared with moderation).
        if publisher is not None:
            self.publisher = publisher
        else:
            from app.services.publisher import ChannelPublisher

            self.publisher = ChannelPublisher(
                http=self.rss.http,
                tg=self.tg,
                enable_video=self.enable_video,
                max_image_bytes=self.max_image_bytes,
                max_video_bytes=self.max_video_bytes,
                enable_og_image=self.enable_og_image,
            )

        # source name -> origin ("ua"/"world") for items already in DB rows
        self.source_origin: Dict[str, str] = {
            str(getattr(s, "name", "")): str(getattr(s, "origin", "world") or "world")
            for s in (getattr(cfg, "sources", None) or [])
        }

        tz_name = str(getattr(getattr(cfg, "analytics", None), "timezone", "") or "Europe/Kyiv")
        try:
            self.local_tz = ZoneInfo(tz_name)
        except Exception:
            self.local_tz = timezone.utc

    def run_once(self) -> None:
        """One pipeline cycle: thin orchestration over the private stage methods."""
        now_utc = datetime.now(timezone.utc)
        cutoff_hours = int(getattr(self.cfg.posting, "only_last_hours", 0) or 0)
        counters: Dict[str, int] = {
            "fetched": 0, "new": 0, "embedded": 0, "dups": 0, "candidates": 0,
            "scored": 0, "approved": 0, "posted": 0, "wraps": 0,
            "review_timeouts": 0, "retried": 0, "failed": 0,
        }

        # Resolve stale pending_review items first (admin stayed silent past the timeout).
        if self.moderation is not None:
            try:
                counters["review_timeouts"] = self.moderation.check_timeouts()
                if counters["review_timeouts"]:
                    log.info("Moderation timeouts handled: %d", counters["review_timeouts"])
            except Exception:
                log.exception("Moderation timeout check failed")

        posted = 0
        try:
            if posted < self.cfg.posting.max_posts_per_run:
                posted += self._post_pending_roots(only_last_hours=cutoff_hours, limit=max(1, self.cfg.posting.max_posts_per_run - posted))
        except Exception:
            log.exception("Pre-post pending roots failed")

        for it, h in self._ingest(now_utc, cutoff_hours, counters):
            match = self._match_and_dedup(it, h, counters)
            if match is None:
                continue
            self._score_or_collect(it, h, match, counters)

        if self.ranker is not None and self.ranking_enabled:
            try:
                self.ranker.maybe_run()
            except Exception:
                log.exception("Ranking cycle failed")

        posted += self._publish_due(cutoff_hours, posted, counters)
        counters["posted"] = posted

        log.info(
            "Funnel: %s threshold=%.2f",
            " ".join(f"{k}={v}" for k, v in counters.items()),
            self.score_threshold,
        )
        try:
            self.repo.add_funnel_counts(counters)
        except Exception:
            log.exception("Funnel persist failed")

        if self.engagement is not None:
            try:
                captured = self.engagement.maybe_collect()
                if captured:
                    log.info("Engagement snapshot captured for %d posts", captured)
            except Exception:
                log.exception("Engagement collection failed")

        removed = self.repo.cleanup_old(self.cfg.db.keep_days)
        if removed:
            log.info("DB cleanup removed %d rows", removed)
        self.maybe_post_daily_digest()

    def _ingest(self, now_utc: datetime, cutoff_hours: int, counters: Dict[str, int]) -> List[Tuple[object, str]]:
        """Fetch feeds, upsert items, return new ones inside the freshness window."""
        items = self.rss.fetch(self.cfg.sources)
        counters["fetched"] = len(items)

        out: List[Tuple[object, str]] = []
        for it in items:
            h = self.exact.make_hash(it.title, it.link)
            published_iso = it.published_at.astimezone(timezone.utc).isoformat(timespec="seconds") if it.published_at else None

            is_new = self.repo.upsert_item(
                item_hash=h,
                source=it.source,
                title=it.title,
                link=it.link,
                summary=it.summary,
                published_at_utc=published_iso,
                image_url=getattr(it, "image_url", None),
                origin=getattr(it, "origin", "world") or "world",
                video_url=getattr(it, "video_url", None),
                images=list(getattr(it, "images", ()) or ()),
            )
            if not is_new:
                continue
            counters["new"] += 1

            if cutoff_hours > 0 and it.published_at:
                age_h = (now_utc - it.published_at.astimezone(timezone.utc)).total_seconds() / 3600.0
                if age_h > cutoff_hours:
                    continue

            out.append((it, h))
        return out

    def _match_and_dedup(self, it, h: str, counters: Dict[str, int]):
        """
        Text profile + embedding + semantic/lexical matching + cluster assignment.
        Returns a match context for scoring, or None when the item is skipped
        (bad summary, embedding failure, duplicate).
        """
        if self.cfg.embeddings.require_good_summary and not is_good_summary(it.summary):
            return None

        text_for_vec = (it.title or "").strip()
        if it.summary:
            text_for_vec += "\n\n" + truncate(it.summary, max_len=500)

        profile = self.profile_builder.build(it.title, it.summary) if self.profile_builder else None
        if profile is not None:
            try:
                self.repo.set_text_profile(
                    item_hash=h,
                    normalized_text=profile.normalized_text,
                    keywords=profile.keywords,
                    entities=profile.entities,
                    numbers=profile.numbers,
                    event_verbs=profile.event_verbs,
                )
            except Exception:
                log.exception("Failed to persist text profile")

        try:
            vec = self.embedder.embed(text_for_vec)
        except Exception as ex:
            log.warning("Embedding failed: %s", ex)
            vec = None
        if vec is None:
            return None

        counters["embedded"] += 1
        semantic_match_hash, semantic_match_score = self.semantic.find_best_match(vec)
        lexical_match = None

        if self.text_processing_enabled and self.lexical and profile is not None:
            try:
                since_iso = (datetime.now(timezone.utc) - timedelta(hours=self.matching_window_hours)).isoformat(timespec="seconds")
                recent_items = self.repo.get_recent_items_for_matching(since_iso=since_iso, limit=self.max_matching_candidates)
                lexical_candidates = [
                    LexicalCandidate(
                        item_hash=str(r.get("item_hash") or ""),
                        normalized_text=str(r.get("normalized_text") or ""),
                        keywords=list(r.get("keywords") or []),
                        entities=list(r.get("entities") or []),
                        numbers=list(r.get("numbers") or []),
                        event_verbs=list(r.get("event_verbs") or []),
                    )
                    for r in recent_items
                    if str(r.get("item_hash") or "") and str(r.get("item_hash") or "") != h
                ]
                lexical_match = self.lexical.find_best_match(
                    LexicalCandidate(
                        item_hash=h,
                        normalized_text=profile.normalized_text,
                        keywords=profile.keywords,
                        entities=profile.entities,
                        numbers=profile.numbers,
                        event_verbs=profile.event_verbs,
                    ),
                    lexical_candidates,
                )
            except Exception:
                log.exception("Lexical matching failed")

        event_decision = self.event_matcher.decide(semantic_match_hash, semantic_match_score, lexical_match) if (self.event_matcher and lexical_match is not None) else None

        dup_of = None
        dup_score = float(semantic_match_score or 0.0) if semantic_match_hash else None
        lexical_score = float(getattr(lexical_match, "score", 0.0) or 0.0) if lexical_match is not None else None
        event_match_type = getattr(event_decision, "match_type", None)
        same_event_of = None

        if event_decision is not None:
            if event_decision.match_type == "duplicate":
                dup_of = event_decision.matched_item_hash
                dup_score = max(float(semantic_match_score or 0.0), float(event_decision.combined_score or 0.0))
                counters["dups"] += 1
            elif event_decision.match_type == "same_event":
                same_event_of = event_decision.matched_item_hash
        elif semantic_match_hash is not None and float(semantic_match_score or 0.0) >= self.cfg.embeddings.threshold:
            dup_of = semantic_match_hash
            counters["dups"] += 1

        self.repo.set_embedding_and_dup(
            item_hash=h,
            embedding_blob=pack_vec(vec),
            embedding_dim=int(vec.shape[0]),
            embedding_model=self.cfg.openai.model,
            dup_of=dup_of,
            dup_score=dup_score,
            lexical_score=lexical_score,
            event_match_type=event_match_type,
            same_event_of=same_event_of,
        )

        # Phase 4: assign an event cluster to every embedded item (duplicates
        # included — they confirm the event from another source).
        if self.clusterer is not None and self.clustering_enabled:
            try:
                assignment = self.clusterer.assign(h, vec)
                if assignment is not None:
                    log.info(
                        "Cluster: item %s -> cluster %d (%s, sim=%.3f)",
                        h[:12], assignment.cluster_id,
                        "new" if assignment.created_new else "joined", assignment.similarity,
                    )
            except Exception:
                log.exception("Cluster assignment failed for %s", h[:12])

        if dup_of is not None:
            return None

        return SimpleNamespace(profile=profile, same_event_of=same_event_of)

    def _score_or_collect(self, it, h: str, match, counters: Dict[str, int]) -> None:
        """
        Article fetch + LLM stage. Ranking mode: cheap classify into the candidate
        pool; legacy mode: full post generation + editorial decision (the decide
        step itself lives in _decide_and_store).
        """
        # ---- Fetch the article page ONCE: full text for the LLM + og media.
        article: Optional[ArticleContent] = None
        if self.fetch_full_text:
            article = self._fetch_article(it.link)
            if article is not None:
                if article.text:
                    try:
                        self.repo.set_article_text(h, article.text)
                    except Exception:
                        log.exception("Failed to persist article text")
                if getattr(article, "embed_video_url", None):
                    try:
                        self.repo.set_embed_video_url(h, article.embed_video_url)
                    except Exception:
                        log.exception("Failed to persist embed video url")
                page_images = list(article.images or [])
                page_video = article.video_url if self.enable_video else None
                if page_images or page_video:
                    try:
                        self.repo.update_media(
                            item_hash=h,
                            image_url=page_images[0] if page_images and not getattr(it, "image_url", None) else None,
                            video_url=page_video,
                            images=(list(getattr(it, "images", ()) or ()) + page_images) or None,
                        )
                    except Exception:
                        log.exception("Failed to persist page media")

        profile = match.profile
        profile_fields = {
            "keywords": list(getattr(profile, "keywords", []) or []),
            "entities": list(getattr(profile, "entities", []) or []),
            "numbers": list(getattr(profile, "numbers", []) or []),
            "event_verbs": list(getattr(profile, "event_verbs", []) or []),
        }
        item_origin = getattr(it, "origin", "world") or "world"

        if self.ranking_enabled:
            # Cheap classify pass; the item joins the candidate pool (or takes
            # the immediate breaking path). No post_text is generated here.
            outcome = self._classify_and_collect(
                it=it, item_hash=h, profile_fields=profile_fields,
                article=article, same_event_of=match.same_event_of, origin=item_origin,
            )
            if outcome:
                counters["scored"] += 1
            if outcome == "candidate":
                counters["candidates"] += 1
            return

        decision = self.postmaker.make(
            title=it.title,
            summary=it.summary,
            source=it.source,
            url=it.link,
            article_text=(article.text if article else None),
        )
        if not decision:
            return
        counters["scored"] += 1

        _, should_post = self._decide_and_store(
            item_hash=h,
            decision=decision,
            title=it.title,
            source=it.source,
            origin=item_origin,
            profile_fields=profile_fields,
            same_event_of=match.same_event_of,
            gate="threshold",
        )
        if should_post:
            counters["approved"] += 1

    def _publish_due(self, cutoff_hours: int, already_posted: int, counters: Dict[str, int]) -> int:
        """Error retries + due pending posts + wraps, within max_posts_per_run."""
        self._retry_errors(counters)

        posted = 0
        if already_posted + posted < self.cfg.posting.max_posts_per_run:
            posted += self._post_pending_roots(
                only_last_hours=cutoff_hours,
                limit=max(0, self.cfg.posting.max_posts_per_run - already_posted - posted),
            )

        if already_posted + posted < self.cfg.posting.max_posts_per_run:
            wraps = self._process_wraps(limit=self.cfg.posting.max_posts_per_run - already_posted - posted)
            counters["wraps"] = wraps
            posted += wraps

        return posted

    def _retry_errors(self, counters: Dict[str, int]) -> None:
        """
        Error items retry with exponential backoff (10/20/40 min). After
        MAX_RETRIES failures the item goes terminal ('failed') with an admin alert.
        """
        try:
            rows = self.repo.get_error_items_due()
        except Exception:
            log.exception("Error retry lookup failed")
            return
        for row in rows:
            item_hash = str(row["item_hash"])
            try:
                if int(row.get("retry_count") or 0) >= self.repo.MAX_RETRIES:
                    self.repo.mark_failed(item_hash)
                    counters["failed"] += 1
                    log.warning("Item %s failed after %d retries", item_hash[:12], self.repo.MAX_RETRIES)
                    if self.tg.admin_chat_id is not None:
                        self.tg.send_message_with_id(
                            f"⚠️ Не вдалося опублікувати після {self.repo.MAX_RETRIES} спроб:\n"
                            f"{row.get('title')}\n{row.get('link')}",
                            disable_preview=True,
                            chat_id=self.tg.admin_chat_id,
                        )
                else:
                    self.repo.requeue_for_posting(item_hash)
                    counters["retried"] += 1
                    log.info("Item %s requeued for retry #%s", item_hash[:12], row.get("retry_count"))
            except Exception:
                log.exception("Retry handling failed for %s", item_hash[:12])

    def _event_source_count(self, item_hash: str, root_hash: str, event_key: Optional[str]) -> int:
        """
        Distinct sources confirming the event. With clustering on, the embedding
        cluster defines event identity; event_key stays as a reference field only.
        """
        if self.clustering_enabled:
            try:
                cluster_id = self.repo.get_item_cluster_id(item_hash)
                if cluster_id is not None:
                    return int(self.repo.get_cluster_stats(cluster_id).get("source_count") or 1)
            except Exception:
                log.exception("Cluster source count failed for %s", item_hash[:12])
        return self.repo.get_event_source_count(root_hash=root_hash, event_key=event_key)

    def _event_cluster_rows(self, item_hash: str, root_hash: str) -> List[Dict]:
        """Sibling items of the same event for the new-fact gate."""
        lookback = max(self.matching_window_hours, 48)
        if self.clustering_enabled:
            try:
                cluster_id = self.repo.get_item_cluster_id(item_hash)
                if cluster_id is not None:
                    return self.repo.get_cluster_member_rows(cluster_id, lookback_hours=lookback)
            except Exception:
                log.exception("Cluster member rows failed for %s", item_hash[:12])
        return self.repo.get_cluster_rows(root_hash=root_hash, lookback_hours=lookback)

    def _decide_and_store(
        self,
        item_hash: str,
        decision,
        title: str,
        source: str,
        origin: str,
        profile_fields: Dict[str, list],
        same_event_of: Optional[str],
        gate: str = "threshold",
        extra_reasons: Optional[List[str]] = None,
    ):
        """
        Editorial policy + persistence, shared by the legacy threshold path and
        ranking winners. gate="threshold" keeps the legacy min_post_score check;
        gate="ranking" trusts the comparative pick and leaves editorial the veto.
        Returns (editorial_decision, should_post).
        """
        cat_slug = (getattr(decision, "category", "") or "").strip() or "other"
        cat_id = self.repo.category_id(cat_slug) or self.repo.category_id("other")
        wrap_rule = self.wrap_category_map.get(cat_slug)
        root_hash = same_event_of or item_hash
        cluster_rows = self._event_cluster_rows(item_hash, root_hash)
        source_count = self._event_source_count(item_hash, root_hash, getattr(decision, "event_key", None))
        topic_key = getattr(decision, "topic_key", "") or getattr(decision, "event_key", "") or item_hash[:16]
        topic_history = self.repo.get_topic_post_history(topic_key=topic_key, window_hours=[self.topic_quota_cfg.window1_hours, self.topic_quota_cfg.window2_hours])
        topic_mix = self.repo.get_topic_share(topic_key=topic_key, lookback_hours=self.topic_saturation_cfg.lookback_hours)

        origin_mix = None
        if self.origin_balance_cfg.enabled:
            try:
                origin_mix = self.repo.get_origin_share(lookback_hours=self.origin_balance_cfg.lookback_hours)
            except Exception:
                origin_mix = None

        editorial_decision = decide_publish_mode(
            decision=decision,
            item_row={
                "title": title,
                "source": source,
                "keywords": list(profile_fields.get("keywords") or []),
                "entities": list(profile_fields.get("entities") or []),
                "numbers": list(profile_fields.get("numbers") or []),
                "event_verbs": list(profile_fields.get("event_verbs") or []),
                "source_count": source_count,
            },
            cluster_rows=cluster_rows,
            topic_history=topic_history,
            topic_mix=topic_mix,
            same_event=bool(same_event_of),
            wrap_rule=wrap_rule,
            quota_cfg=self.topic_quota_cfg,
            saturation_cfg=self.topic_saturation_cfg,
            delay_cfg=self.delay_cfg,
            high_trust_sources=self.high_trust_sources,
            origin=origin,
            origin_mix=origin_mix,
            origin_cfg=self.origin_balance_cfg,
        )

        if gate == "ranking":
            # The comparative ranker already made the importance call; editorial
            # policy above keeps full veto power, the absolute threshold does not apply.
            llm_gate = bool(decision.post_text)
        else:
            # World news that directly matters for Ukraine pass the gate easier.
            effective_score = float(decision.score or 0.0)
            if origin == "world":
                effective_score += self.origin_balance_cfg.ua_relevance_boost * float(getattr(decision, "ua_relevance_score", 0.0) or 0.0)
            llm_gate = bool(decision.should_post and effective_score >= self.score_threshold and decision.post_text)

        should_post = bool(editorial_decision.should_post and llm_gate)

        wrap_name = getattr(wrap_rule, "key", None) if editorial_decision.mode == "wrap" and wrap_rule else None
        reasons = list(editorial_decision.reasons) + list(extra_reasons or [])
        self.repo.set_score_and_posttext(
            item_hash=item_hash,
            score=decision.score,
            should_post=should_post or editorial_decision.mode == "wrap",
            post_text=decision.post_text if decision.post_text else None,
            why=decision.why,
            category_id=cat_id,
            tier=getattr(decision, "tier", "C"),
            publish_mode=getattr(decision, "publish_mode", "digest"),
            event_key=getattr(decision, "event_key", ""),
            novelty_score=getattr(decision, "novelty_score", 0.0),
            impact_score=getattr(decision, "impact_score", 0.0),
            ua_relevance_score=getattr(decision, "ua_relevance_score", 0.0),
            wrap_name=wrap_name,
            status=editorial_decision.status,
            news_type=editorial_decision.news_type,
            llm_has_new_fact=bool(getattr(decision, "has_new_fact", False)),
            topic_key=editorial_decision.topic_key,
            decision_mode=editorial_decision.mode,
            decision_reasons=reasons,
            delay_until_utc=editorial_decision.delay_until_utc,
            source_trust=editorial_decision.source_trust,
            source_count=editorial_decision.source_count,
        )
        return editorial_decision, should_post

    def _classify_and_collect(
        self,
        it,
        item_hash: str,
        profile_fields: Dict[str, list],
        article,
        same_event_of: Optional[str],
        origin: str,
    ) -> Optional[str]:
        """
        Ranking mode per-item pass: cheap classification, no post_text.
        noise/promo -> filtered; confirmed breaking -> immediate full path;
        everything else joins the candidate pool for the next ranking cycle.
        Returns the outcome ("filtered" | "breaking" | "candidate") or None.
        """
        cls = self.postmaker.classify(
            title=it.title,
            summary=it.summary,
            source=it.source,
            url=it.link,
            article_text=(article.text if article else None),
        )
        if not cls:
            return None

        cat_slug = (cls.category or "").strip() or "other"
        cat_id = self.repo.category_id(cat_slug) or self.repo.category_id("other")
        root_hash = same_event_of or item_hash
        source_count = self._event_source_count(item_hash, root_hash, cls.event_key or None)

        if cls.news_type in ("noise", "promo"):
            self.repo.set_score_and_posttext(
                item_hash=item_hash, score=cls.score, should_post=False, post_text=None, why=[],
                category_id=cat_id, tier=cls.tier, publish_mode="drop", event_key=cls.event_key,
                novelty_score=cls.novelty_score, impact_score=cls.impact_score,
                ua_relevance_score=cls.ua_relevance_score, status="filtered", news_type=cls.news_type,
                llm_has_new_fact=cls.has_new_fact, topic_key=cls.topic_key or cls.event_key,
                decision_mode="drop", decision_reasons=["classify-noise-promo"],
                source_count=source_count, fact_summary=cls.fact_summary or None,
            )
            log.info("Classify: %s filtered as %s", item_hash[:12], cls.news_type)
            return "filtered"

        # Breaking with >= 2 confirming sources bypasses the ranking queue:
        # full post generation + editorial right now, exactly like the legacy path.
        if cls.is_breaking and source_count >= 2:
            decision = self.postmaker.make(
                title=it.title, summary=it.summary, source=it.source, url=it.link,
                article_text=(article.text if article else None),
            )
            if decision:
                self._decide_and_store(
                    item_hash=item_hash, decision=decision, title=it.title, source=it.source,
                    origin=origin, profile_fields=profile_fields, same_event_of=same_event_of,
                    gate="threshold", extra_reasons=["breaking-bypass-ranking"],
                )
                log.info("Classify: %s is breaking (%d sources), bypassed ranking queue", item_hash[:12], source_count)
                return "breaking"
            log.warning("Breaking post generation failed for %s, falling back to candidate pool", item_hash[:12])

        self.repo.set_score_and_posttext(
            item_hash=item_hash, score=cls.score, should_post=False, post_text=None, why=[],
            category_id=cat_id, tier=cls.tier, publish_mode=None, event_key=cls.event_key,
            novelty_score=cls.novelty_score, impact_score=cls.impact_score,
            ua_relevance_score=cls.ua_relevance_score, status="candidate", news_type=cls.news_type,
            llm_has_new_fact=cls.has_new_fact, topic_key=cls.topic_key or cls.event_key,
            decision_mode=None, decision_reasons=["candidate-pool"],
            source_count=source_count, fact_summary=cls.fact_summary or None,
        )
        log.info("Classify: %s joined candidate pool (score=%.2f type=%s)", item_hash[:12], cls.score, cls.news_type)
        return "candidate"

    def process_ranking_winner(self, item_hash: str, reason: str = "") -> bool:
        """
        Ranking winner: generate the full post with the strong model and run the
        editorial post-check (policy keeps veto). Returns True when the item
        ends up approved for standalone posting.
        """
        row = self.repo.get_item_full(item_hash)
        if row is None:
            log.warning("Ranking winner %s not found", item_hash[:12])
            return False
        if str(row.get("status") or "") != "candidate":
            log.info("Ranking winner %s skipped: status=%s", item_hash[:12], row.get("status"))
            return False

        decision = self.postmaker.make(
            title=str(row.get("title") or ""),
            summary=row.get("summary"),
            source=str(row.get("source") or ""),
            url=str(row.get("link") or ""),
            article_text=row.get("article_text"),
        )
        if decision is None:
            # Transient LLM failure: leave the candidate for the next cycle.
            log.warning("Winner post generation failed for %s, kept in pool", item_hash[:12])
            return False
        if not (decision.post_text or "").strip():
            # The strong model judged it not post-worthy after all -> digest.
            self.repo.set_score_and_posttext(
                item_hash=item_hash, score=decision.score, should_post=False, post_text=None,
                why=decision.why, category_id=self.repo.category_id((decision.category or "other").strip() or "other"),
                tier=decision.tier, publish_mode="digest", event_key=decision.event_key,
                status="digest_only", news_type=decision.news_type, topic_key=decision.topic_key,
                decision_mode="digest", decision_reasons=["ranking-winner-empty-post"],
                fact_summary=row.get("fact_summary"),
            )
            log.info("Ranking winner %s demoted to digest (empty post_text)", item_hash[:12])
            return False

        profile_fields = {
            "keywords": json.loads(row.get("keywords_json") or "[]"),
            "entities": json.loads(row.get("entities_json") or "[]"),
            "numbers": json.loads(row.get("numbers_json") or "[]"),
            "event_verbs": json.loads(row.get("event_verbs_json") or "[]"),
        }
        editorial_decision, should_post = self._decide_and_store(
            item_hash=item_hash,
            decision=decision,
            title=str(row.get("title") or ""),
            source=str(row.get("source") or ""),
            origin=str(row.get("origin") or "world"),
            profile_fields=profile_fields,
            same_event_of=row.get("same_event_of"),
            gate="ranking",
            extra_reasons=[f"ranking-pick:{reason}"[:200]] if reason else ["ranking-pick"],
        )
        log.info(
            "Ranking winner %s: editorial=%s should_post=%s",
            item_hash[:12], editorial_decision.mode, should_post,
        )
        return should_post

    def _route_expired_candidate(self, row) -> None:
        """
        Candidate that never won within max_age_hours: route to wrap/digest so
        nothing is lost. Wrap/digest pickers require a non-empty post_text, so we
        synthesize one from the classify fact summary (no extra LLM call).
        """
        item_hash = str(row["item_hash"])
        cat_slug = str(row.get("category_slug") or "other")
        wrap_rule = self.wrap_category_map.get(cat_slug)
        post_text = (
            (row.get("post_text") or "").strip()
            or (row.get("fact_summary") or "").strip()
            or str(row.get("title") or "").strip()
        )
        if wrap_rule is not None:
            status, mode, wrap_name, should_post = "pending_wrap", "wrap", getattr(wrap_rule, "key", None), True
        else:
            status, mode, wrap_name, should_post = "digest_only", "digest", None, False

        self.repo.set_score_and_posttext(
            item_hash=item_hash,
            score=float(row.get("score") or 0.0),
            should_post=should_post,
            post_text=post_text or None,
            why=[],
            category_id=row.get("category_id"),
            tier=row.get("tier"),
            publish_mode=mode,
            event_key=row.get("event_key"),
            novelty_score=row.get("novelty_score"),
            impact_score=row.get("impact_score"),
            ua_relevance_score=row.get("ua_relevance_score"),
            wrap_name=wrap_name,
            status=status,
            news_type=row.get("news_type"),
            llm_has_new_fact=bool(row.get("llm_has_new_fact")),
            topic_key=row.get("topic_key"),
            decision_mode=mode,
            decision_reasons=["ranking-expired"],
            fact_summary=row.get("fact_summary"),
        )
        log.info("Expired candidate %s routed to %s", item_hash[:12], status)

    def _post_now(
        self,
        item_hash: str,
        title: str,
        source: str,
        link: str,
        image_url: Optional[str],
        post_text: str,
        category_slug: str,
        video_url: Optional[str] = None,
        image_urls: Optional[List[str]] = None,
    ) -> bool:
        try:
            text = (post_text or "").strip()
            if not text:
                return False

            lines = [l.rstrip() for l in text.split("\n")]
            if lines and lines[0].strip().lower() in ("заголовок", "headline", "title"):
                lines[0] = (title or "").strip() or "Новина"
                text = "\n".join(lines).strip()

            hashtag = self._hashtag_for_slug(category_slug)
            formatted = self.formatter.format_row(
                {
                    "post_text": text,
                    "link": link,
                    "source": source,
                    "category_hashtag": hashtag,
                }
            )

            candidates: List[str] = list(image_urls or [])
            if image_url and image_url not in candidates:
                candidates.insert(0, image_url)
            if not candidates and not video_url:
                og = self._extract_og_image(link)
                if og:
                    candidates.append(og)
                    try:
                        self.repo.update_image_url(item_hash, og)
                    except Exception:
                        pass

            ok, msg_id = self.publisher.send_media(text=formatted, video_url=video_url, image_urls=candidates)
            if not ok:
                self.repo.mark_error(item_hash)
                return False

            self.repo.mark_posted(item_hash, tg_message_id=msg_id)
            time.sleep(self.cfg.app.sleep_between_posts_sec)
            return True
        except Exception:
            log.exception("Immediate post failed")
            try:
                self.repo.mark_error(item_hash)
            except Exception:
                pass
            return False

    def _fetch_article(self, url: str) -> Optional[ArticleContent]:
        """Single page fetch reused for both article text and og media."""
        try:
            r = self.rss.http.get(url)
        except Exception:
            return None
        if not getattr(r, "ok", False):
            return None
        ct = (r.headers.get("Content-Type") or "").lower()
        if ct and "html" not in ct and "xml" not in ct:
            return None
        html_text = getattr(r, "text", "") or ""
        if not html_text:
            return None
        try:
            return extract_article(html_text, url=url, max_chars=self.article_max_chars)
        except Exception:
            log.exception("Article extraction failed for %s", url)
            return None

    def _send_media(
        self,
        text: str,
        video_url: Optional[str] = None,
        image_urls: Optional[List[str]] = None,
    ) -> bool:
        ok, _ = self.publisher.send_media(text=text, video_url=video_url, image_urls=image_urls)
        return bool(ok)

    def _hashtag_for_slug(self, slug: str) -> str:
        for c in getattr(self.cfg, "categories", None) or []:
            if (getattr(c, "slug", "") or "").strip() == slug:
                return (getattr(c, "hashtag", "") or "").strip()
        for c in getattr(self.cfg, "categories", None) or []:
            if (getattr(c, "slug", "") or "").strip() == "other":
                return (getattr(c, "hashtag", "") or "").strip()
        return ""

    def _extract_og_image(self, url: str) -> Optional[str]:
        return self.publisher.extract_og_image(url)

    def _media_from_row(self, row) -> Tuple[Optional[str], List[str]]:
        return self.publisher.media_from_row(row)

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

            # Human-in-the-loop: instead of publishing, the due item goes to the
            # admin chat for review. Status moves to pending_review so it is not
            # picked again; the admin callback (or timeout) finishes the flow.
            if self.moderation is not None and self.moderation.is_enabled():
                try:
                    if post_text != (row["post_text"] or "").strip():
                        # Persist the breaking "⚡" prefix so preview == published text.
                        self.repo.update_post_text(root_hash, post_text)
                    submitted = self.moderation.submit_for_review(
                        {**dict(row), "post_text": post_text}
                    )
                    if submitted:
                        log.info("Item %s sent to moderation instead of publishing", root_hash[:12])
                except Exception:
                    log.exception("Moderation submit failed for %s, item stays pending_post", root_hash[:12])
                continue

            formatted = self.formatter.format_row(
                {
                    "post_text": post_text,
                    "link": row["link"],
                    "source": row["source"],
                    "category_hashtag": (row["category_hashtag"] or "").strip()
                    if row["category_hashtag"]
                    else "",
                    "embed_video_url": row["embed_video_url"],
                }
            )

            video_url, image_candidates = self._media_from_row(row)
            ok, msg_id = self.publisher.send_media(
                text=formatted,
                video_url=video_url,
                image_urls=image_candidates,
            )
            if not ok:
                self.repo.mark_error(root_hash)
                break

            self.repo.mark_posted(root_hash, tg_message_id=msg_id)
            posted += 1
            time.sleep(self.cfg.app.sleep_between_posts_sec)

            if posted >= self.cfg.posting.max_posts_per_run:
                break

        return posted

    def _process_wraps(self, limit: int) -> int:
        # Clustering mode: the wrap unit is a story cluster, not a category.
        if self.clustering_enabled:
            return self._process_cluster_wraps(limit)
        if not self.wrapmaker or not self.wrap_rules or limit <= 0:
            return 0

        posted = 0
        now_utc = datetime.now(timezone.utc)

        for rule in self.wrap_rules:
            if posted >= limit:
                break

            last_posted = self.repo.get_last_wrap_posted_at(rule.key)
            if last_posted:
                try:
                    last_dt = datetime.fromisoformat(last_posted)
                    if now_utc - last_dt < timedelta(minutes=int(rule.cooldown_minutes)):
                        continue
                except Exception:
                    pass

            rows = self.repo.pick_wrap_candidates(
                wrap_name=rule.key,
                lookback_hours=int(rule.lookback_hours),
                limit=12,
            )
            if len(rows) < int(rule.min_items):
                continue

            sources = {str(r["source"]) for r in rows}
            if len(sources) < int(rule.min_sources):
                continue

            items = [dict(r) for r in rows]
            decision = self.wrapmaker.make(
                wrap_name=rule.key,
                items=items,
                prompt_template=self._prompt_template_for_wrap_rule(rule),
            )
            if not decision or not decision.post_text.strip():
                continue

            hashtag = self._hashtag_for_slug(getattr(rule, "hashtag_slug", "") or "other")
            lead_row = rows[0]
            formatted = self.formatter.format_row(
                {
                    "post_text": decision.post_text.strip(),
                    "link": lead_row["link"],
                    "source": f"{lead_row['source']} +{max(0, len(sources) - 1)}",
                    "category_hashtag": hashtag,
                }
            )

            ok, _ = self.tg.send_message_with_id(formatted, disable_preview=True)
            if not ok:
                break

            item_hashes = [str(r["item_hash"]) for r in rows]
            wrap_post_id = self.repo.save_wrap_post(
                rule.key,
                item_hashes=item_hashes,
                source_count=len(sources),
                post_text=decision.post_text.strip(),
            )
            self.repo.mark_wrapped(item_hashes=item_hashes, wrap_post_id=wrap_post_id)

            posted += 1
            time.sleep(self.cfg.app.sleep_between_posts_sec)

        return posted

    def _process_cluster_wraps(self, limit: int) -> int:
        """
        Story wraps: a cluster that gathered >= min_items items from >= min_sources
        sources within the lookback becomes one "story development" post built
        from the cluster chronology (titles + facts ordered by time).
        """
        if not self.wrapmaker or limit <= 0:
            return 0

        posted = 0
        now_utc = datetime.now(timezone.utc)
        clusters = self.repo.get_clusters_for_wrap(
            min_items=self.cluster_wrap_min_items,
            min_sources=self.cluster_wrap_min_sources,
            lookback_hours=self.cluster_wrap_lookback_hours,
        )

        for cl in clusters:
            if posted >= limit:
                break
            cluster_id = int(cl["cluster_id"])
            wrap_key = f"cluster:{cluster_id}"

            last_posted = self.repo.get_last_wrap_posted_at(wrap_key)
            if last_posted:
                try:
                    last_dt = datetime.fromisoformat(last_posted)
                    if now_utc - last_dt < timedelta(minutes=self.cluster_wrap_cooldown_minutes):
                        continue
                except Exception:
                    pass

            rows = self.repo.pick_cluster_wrap_items(
                cluster_id=cluster_id,
                lookback_hours=self.cluster_wrap_lookback_hours,
                limit=12,
            )
            # The cluster is big enough, but the wrap itself needs at least two
            # wrappable members to tell a story.
            if len(rows) < 2:
                continue

            # Chronology for the prompt: title + fact per step, ordered by time.
            items = []
            for r in rows:
                d = dict(r)
                d["summary"] = (d.get("fact_summary") or d.get("summary") or "").strip()
                items.append(d)

            story_title = str(cl.get("canonical_title") or rows[0]["title"] or wrap_key)
            template = ""
            if self.prompt_manager is not None:
                template = (self.prompt_manager.get("cluster_wrap_prompt") or "").strip()
            decision = self.wrapmaker.make(
                wrap_name=story_title,
                items=items,
                prompt_template=template or DEFAULT_CLUSTER_WRAP_PROMPT,
            )
            if not decision or not decision.post_text.strip():
                continue

            hashtags = [str(r["category_hashtag"]) for r in rows if r["category_hashtag"]]
            hashtag = hashtags[0] if hashtags else self._hashtag_for_slug("other")
            sources = {str(r["source"]) for r in rows}
            lead_row = rows[0]
            formatted = self.formatter.format_row(
                {
                    "post_text": decision.post_text.strip(),
                    "link": lead_row["link"],
                    "source": f"{lead_row['source']} +{max(0, len(sources) - 1)}",
                    "category_hashtag": hashtag,
                }
            )

            ok, _ = self.tg.send_message_with_id(formatted, disable_preview=True)
            if not ok:
                break

            item_hashes = [str(r["item_hash"]) for r in rows]
            wrap_post_id = self.repo.save_wrap_post(
                wrap_key,
                item_hashes=item_hashes,
                source_count=len(sources),
                post_text=decision.post_text.strip(),
            )
            self.repo.mark_wrapped(item_hashes=item_hashes, wrap_post_id=wrap_post_id)
            log.info("Cluster wrap posted: cluster=%d items=%d sources=%d", cluster_id, len(rows), len(sources))

            posted += 1
            time.sleep(self.cfg.app.sleep_between_posts_sec)

        return posted

    def _prompt_template_for_wrap_rule(self, rule) -> str:
        key = (getattr(rule, "prompt_template_key", "") or "").strip()
        if key and self.prompt_manager is not None:
            active = (self.prompt_manager.get(key) or "").strip()
            if active:
                return active
        return (getattr(rule, "prompt_template", "") or "").strip()

    def maybe_post_daily_digest(self) -> None:
        # Use the configured channel timezone (analytics.timezone, e.g. Europe/Kyiv)
        # instead of the server's local clock, which is UTC on most hosting.
        now_local = datetime.now(self.local_tz)
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
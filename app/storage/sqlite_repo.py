from __future__ import annotations

import json
import os
import sqlite3
import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

log = logging.getLogger("storage.sqlite")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


SCHEMA_V5 = """

PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS categories (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    slug TEXT NOT NULL UNIQUE,
    title TEXT NOT NULL,
    hashtag TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS news_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    item_hash TEXT NOT NULL UNIQUE,
    source TEXT NOT NULL,
    title TEXT NOT NULL,
    link TEXT NOT NULL,
    summary TEXT NULL,
    image_url TEXT NULL,
    published_at_utc TEXT NULL,
    created_at_utc TEXT NOT NULL,
    posted_at_utc TEXT NULL,
    status TEXT NOT NULL DEFAULT 'new',
    category_id INTEGER NULL,
    embedding_blob BLOB NULL,
    embedding_dim INTEGER NULL,
    embedding_model TEXT NULL,
    dup_of TEXT NULL,
    dup_score REAL NULL,
    score REAL NULL,
    should_post INTEGER NOT NULL DEFAULT 0,
    post_text TEXT NULL,
    why_json TEXT NULL,
    scored_at_utc TEXT NULL,
    tier TEXT NULL,
    publish_mode TEXT NULL,
    event_key TEXT NULL,
    novelty_score REAL NULL,
    impact_score REAL NULL,
    ua_relevance_score REAL NULL,
    wrap_name TEXT NULL,
    wrap_post_id INTEGER NULL,
    normalized_text TEXT NULL,
    keywords_json TEXT NULL,
    entities_json TEXT NULL,
    numbers_json TEXT NULL,
    event_verbs_json TEXT NULL,
    lexical_score REAL NULL,
    event_match_type TEXT NULL,
    same_event_of TEXT NULL,
    news_type TEXT NULL,
    llm_has_new_fact INTEGER NOT NULL DEFAULT 0,
    topic_key TEXT NULL,
    decision_mode TEXT NULL,
    decision_reasons_json TEXT NULL,
    delay_until_utc TEXT NULL,
    source_trust TEXT NULL,
    source_count INTEGER NOT NULL DEFAULT 1,
    FOREIGN KEY(category_id) REFERENCES categories(id)
);

CREATE INDEX IF NOT EXISTS idx_news_status_created ON news_items(status, created_at_utc);
CREATE INDEX IF NOT EXISTS idx_news_published ON news_items(published_at_utc);
CREATE INDEX IF NOT EXISTS idx_news_dup_of ON news_items(dup_of);
CREATE INDEX IF NOT EXISTS idx_news_should_post ON news_items(should_post, status);
CREATE INDEX IF NOT EXISTS idx_news_category ON news_items(category_id);
CREATE INDEX IF NOT EXISTS idx_news_wrap_status ON news_items(wrap_name, status, created_at_utc);
CREATE INDEX IF NOT EXISTS idx_news_event_key ON news_items(event_key);
CREATE INDEX IF NOT EXISTS idx_news_topic_key ON news_items(topic_key, posted_at_utc);
CREATE INDEX IF NOT EXISTS idx_news_delay_status ON news_items(status, delay_until_utc);

CREATE TABLE IF NOT EXISTS daily_summaries (
    day_utc TEXT PRIMARY KEY,
    posted_at_utc TEXT NOT NULL,
    post_text TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS market_wrap_posts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    wrap_name TEXT NOT NULL,
    item_hashes_json TEXT NOT NULL,
    source_count INTEGER NOT NULL DEFAULT 0,
    post_text TEXT NOT NULL,
    created_at_utc TEXT NOT NULL,
    posted_at_utc TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_market_wrap_name_posted ON market_wrap_posts(wrap_name, posted_at_utc);

CREATE TABLE IF NOT EXISTS analytics_reports (
    day_local TEXT PRIMARY KEY,
    posted_at_utc TEXT NOT NULL,
    post_text TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS bot_state (
    key TEXT PRIMARY KEY,
    value TEXT NULL
);
"""

DESIRED_COLS = [
    "id", "item_hash", "source", "title", "link", "summary", "image_url", "published_at_utc",
    "created_at_utc", "posted_at_utc", "status", "category_id", "embedding_blob", "embedding_dim",
    "embedding_model", "dup_of", "dup_score", "score", "should_post", "post_text", "why_json",
    "scored_at_utc", "tier", "publish_mode", "event_key", "novelty_score", "impact_score",
    "ua_relevance_score", "wrap_name", "wrap_post_id", "normalized_text", "keywords_json",
    "entities_json", "numbers_json", "event_verbs_json", "lexical_score", "event_match_type", "same_event_of",
    "news_type", "llm_has_new_fact", "topic_key", "decision_mode", "decision_reasons_json", "delay_until_utc", "source_trust", "source_count",
]


class SqliteNewsRepository:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.con: Optional[sqlite3.Connection] = None

    def _connect(self) -> sqlite3.Connection:
        if self.con is not None:
            return self.con
        os.makedirs(os.path.dirname(self.db_path) or ".", exist_ok=True)
        con = sqlite3.connect(self.db_path)
        con.row_factory = sqlite3.Row
        self.con = con
        return con

    def init_db(self) -> None:
        con = self._connect()
        con.executescript(SCHEMA_V5)
        con.commit()
        self._migrate_news_items(con)
        self._ensure_extra_columns(con)

    # Lightweight, idempotent ALTER-based migration for columns added after v5.
    # Keeps DESIRED_COLS untouched so the heavy rebuild path never retriggers.
    _EXTRA_COLS = {
        "video_url": "TEXT NULL",
        "images_json": "TEXT NULL",
        "origin": "TEXT NULL",
        "article_text": "TEXT NULL",
        # Phase 1: human-in-the-loop moderation
        "review_requested_at_utc": "TEXT NULL",
        "review_message_id": "INTEGER NULL",
        # Phase 2: engagement metrics — message id of the published channel post
        "tg_message_id": "INTEGER NULL",
        # Phase 3: short fact summary from the cheap classify pass (ranking mode)
        "fact_summary": "TEXT NULL",
        # Phase 4: embedding-based event cluster id (event_key stays as reference only)
        "cluster_id": "INTEGER NULL",
        # Phase 5: retry with exponential backoff for error items
        "retry_count": "INTEGER NOT NULL DEFAULT 0",
        "next_retry_utc": "TEXT NULL",
        # Phase 5: embeddable player URL (YouTube/Vimeo) — linked, never downloaded
        "embed_video_url": "TEXT NULL",
    }

    def _ensure_extra_columns(self, con: sqlite3.Connection) -> None:
        cols = {r["name"] for r in con.execute("PRAGMA table_info(news_items)").fetchall()}
        for name, decl in self._EXTRA_COLS.items():
            if name not in cols:
                log.info("Adding column news_items.%s", name)
                con.execute(f"ALTER TABLE news_items ADD COLUMN {name} {decl}")
        con.execute("CREATE INDEX IF NOT EXISTS idx_news_origin_posted ON news_items(origin, status, posted_at_utc)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_news_review_status ON news_items(status, review_requested_at_utc)")
        # Admin moderation decisions; future dataset for threshold calibration.
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS moderation_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                item_hash TEXT NOT NULL,
                action TEXT NOT NULL,
                decided_at_utc TEXT NOT NULL,
                llm_score REAL NULL,
                origin TEXT NULL,
                topic_key TEXT NULL,
                category TEXT NULL
            )
            """
        )
        con.execute("CREATE INDEX IF NOT EXISTS idx_moderation_log_item ON moderation_log(item_hash, decided_at_utc)")
        # Phase 2: append-only engagement snapshots + live reaction tally
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS post_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                item_hash TEXT NOT NULL,
                captured_at_utc TEXT NOT NULL,
                views INTEGER NULL,
                reactions INTEGER NULL,
                forwards INTEGER NULL
            )
            """
        )
        con.execute("CREATE INDEX IF NOT EXISTS idx_post_metrics_item ON post_metrics(item_hash, captured_at_utc)")
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS reaction_tally (
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                reactions INTEGER NOT NULL DEFAULT 0,
                updated_at_utc TEXT NOT NULL,
                PRIMARY KEY (chat_id, message_id)
            )
            """
        )
        # Phase 3: one row per ranking cycle, for debugging and calibration
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS ranking_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cycle_at_utc TEXT NOT NULL,
                candidates_json TEXT NOT NULL,
                picks_json TEXT NOT NULL,
                reasoning TEXT NULL
            )
            """
        )
        # Phase 4: incremental event clusters over embeddings
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS event_clusters (
                cluster_id INTEGER PRIMARY KEY AUTOINCREMENT,
                centroid_blob BLOB NOT NULL,
                centroid_dim INTEGER NOT NULL,
                created_at_utc TEXT NOT NULL,
                canonical_hash TEXT NOT NULL,
                item_count INTEGER NOT NULL DEFAULT 1,
                source_count INTEGER NOT NULL DEFAULT 1
            )
            """
        )
        # Phase 5: per-day funnel stage counters
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS funnel_daily (
                day_utc TEXT NOT NULL,
                stage TEXT NOT NULL,
                count INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (day_utc, stage)
            )
            """
        )
        con.execute("CREATE INDEX IF NOT EXISTS idx_news_cluster ON news_items(cluster_id, status)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_event_clusters_created ON event_clusters(created_at_utc)")
        con.commit()

    def _migrate_news_items(self, con: sqlite3.Connection) -> None:
        cols = [r["name"] for r in con.execute("PRAGMA table_info(news_items)").fetchall()]
        if not cols or set(cols) == set(DESIRED_COLS):
            return
        log.info("Rebuilding DB schema to v5")
        con.executescript(
            """
            CREATE TABLE IF NOT EXISTS news_items_v5 (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                item_hash TEXT NOT NULL UNIQUE,
                source TEXT NOT NULL,
                title TEXT NOT NULL,
                link TEXT NOT NULL,
                summary TEXT NULL,
                image_url TEXT NULL,
                published_at_utc TEXT NULL,
                created_at_utc TEXT NOT NULL,
                posted_at_utc TEXT NULL,
                status TEXT NOT NULL DEFAULT 'new',
                category_id INTEGER NULL,
                embedding_blob BLOB NULL,
                embedding_dim INTEGER NULL,
                embedding_model TEXT NULL,
                dup_of TEXT NULL,
                dup_score REAL NULL,
                score REAL NULL,
                should_post INTEGER NOT NULL DEFAULT 0,
                post_text TEXT NULL,
                why_json TEXT NULL,
                scored_at_utc TEXT NULL,
                tier TEXT NULL,
                publish_mode TEXT NULL,
                event_key TEXT NULL,
                novelty_score REAL NULL,
                impact_score REAL NULL,
                ua_relevance_score REAL NULL,
                wrap_name TEXT NULL,
                wrap_post_id INTEGER NULL,
                normalized_text TEXT NULL,
                keywords_json TEXT NULL,
                entities_json TEXT NULL,
                numbers_json TEXT NULL,
                event_verbs_json TEXT NULL,
                lexical_score REAL NULL,
                event_match_type TEXT NULL,
                same_event_of TEXT NULL,
                news_type TEXT NULL,
                llm_has_new_fact INTEGER NOT NULL DEFAULT 0,
                topic_key TEXT NULL,
                decision_mode TEXT NULL,
                decision_reasons_json TEXT NULL,
                delay_until_utc TEXT NULL,
                source_trust TEXT NULL,
                source_count INTEGER NOT NULL DEFAULT 1,
                FOREIGN KEY(category_id) REFERENCES categories(id)
            );
            """
        )
        legacy = set(cols)
        emb_expr = "embedding_blob" if "embedding_blob" in legacy else ("embedding" if "embedding" in legacy else "NULL")
        img_expr = "image_url" if "image_url" in legacy else "NULL"
        why_expr = "why_json" if "why_json" in legacy else ("score_json" if "score_json" in legacy else "NULL")
        cat_expr = "category_id" if "category_id" in legacy else "NULL"
        status_expr = "status" if "status" in legacy else "'new'"
        con.execute(
            f"""
            INSERT OR IGNORE INTO news_items_v5 (
                item_hash, source, title, link, summary, image_url, published_at_utc, created_at_utc,
                posted_at_utc, status, category_id, embedding_blob, embedding_dim, embedding_model,
                dup_of, dup_score, score, should_post, post_text, why_json, scored_at_utc,
                tier, publish_mode, event_key, novelty_score, impact_score, ua_relevance_score,
                wrap_name, wrap_post_id, normalized_text, keywords_json, entities_json, numbers_json, event_verbs_json, lexical_score, event_match_type, same_event_of,
                news_type, llm_has_new_fact, topic_key, decision_mode, decision_reasons_json, delay_until_utc, source_trust, source_count
            )
            SELECT
                item_hash, source, title, link, summary, {img_expr}, published_at_utc, created_at_utc,
                posted_at_utc, {status_expr}, {cat_expr}, {emb_expr}, embedding_dim, embedding_model,
                dup_of, dup_score, score, COALESCE(should_post, 0), post_text, {why_expr}, scored_at_utc,
                NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                NULL, 0, NULL, NULL, NULL, NULL, NULL, 1
            FROM news_items
            """
        )
        con.execute("DROP TABLE news_items")
        con.execute("ALTER TABLE news_items_v5 RENAME TO news_items")
        con.executescript(
            """
            CREATE INDEX IF NOT EXISTS idx_news_status_created ON news_items(status, created_at_utc);
            CREATE INDEX IF NOT EXISTS idx_news_published ON news_items(published_at_utc);
            CREATE INDEX IF NOT EXISTS idx_news_dup_of ON news_items(dup_of);
            CREATE INDEX IF NOT EXISTS idx_news_should_post ON news_items(should_post, status);
            CREATE INDEX IF NOT EXISTS idx_news_category ON news_items(category_id);
            CREATE INDEX IF NOT EXISTS idx_news_wrap_status ON news_items(wrap_name, status, created_at_utc);
            CREATE INDEX IF NOT EXISTS idx_news_event_key ON news_items(event_key);
CREATE INDEX IF NOT EXISTS idx_news_topic_key ON news_items(topic_key, posted_at_utc);
CREATE INDEX IF NOT EXISTS idx_news_delay_status ON news_items(status, delay_until_utc);
            """
        )
        con.commit()

    def ensure_categories(self, categories) -> None:
        con = self._connect()
        for c in categories:
            con.execute(
                """
                INSERT INTO categories(slug, title, hashtag)
                VALUES (?, ?, ?)
                ON CONFLICT(slug) DO UPDATE SET title=excluded.title, hashtag=excluded.hashtag
                """,
                (str(c.slug), str(c.title), str(c.hashtag)),
            )
        con.commit()

    def category_id(self, slug: str) -> Optional[int]:
        con = self._connect()
        row = con.execute("SELECT id FROM categories WHERE slug=?", (slug,)).fetchone()
        return int(row["id"]) if row else None

    def upsert_item(
        self,
        item_hash: str,
        source: str,
        title: str,
        link: str,
        summary: Optional[str],
        published_at_utc: Optional[str],
        image_url: Optional[str] = None,
        origin: str = "world",
        video_url: Optional[str] = None,
        images: Optional[list[str]] = None,
    ) -> bool:
        con = self._connect()
        cur = con.execute(
            """
            INSERT INTO news_items (item_hash, source, title, link, summary, image_url, published_at_utc, created_at_utc, status, origin, video_url, images_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'new', ?, ?, ?)
            ON CONFLICT(item_hash) DO NOTHING
            """,
            (
                item_hash, source, title, link, summary, image_url, published_at_utc, utc_now_iso(),
                origin or "world", video_url,
                json.dumps(list(images or []), ensure_ascii=False) if images else None,
            ),
        )
        con.commit()
        return cur.rowcount == 1

    def update_media(
        self,
        item_hash: str,
        image_url: Optional[str] = None,
        video_url: Optional[str] = None,
        images: Optional[list[str]] = None,
    ) -> None:
        """Update media found later in the pipeline (og:image/og:video from the article page)."""
        con = self._connect()
        sets, params = [], []
        if image_url:
            sets.append("image_url=?")
            params.append(image_url)
        if video_url:
            sets.append("video_url=COALESCE(video_url, ?)")
            params.append(video_url)
        if images:
            sets.append("images_json=?")
            params.append(json.dumps(list(images), ensure_ascii=False))
        if not sets:
            return
        params.append(item_hash)
        con.execute(f"UPDATE news_items SET {', '.join(sets)} WHERE item_hash=?", params)
        con.commit()

    def set_article_text(self, item_hash: str, article_text: Optional[str]) -> None:
        if not article_text:
            return
        con = self._connect()
        con.execute("UPDATE news_items SET article_text=? WHERE item_hash=?", (article_text, item_hash))
        con.commit()

    def get_origin_share(self, lookback_hours: int) -> Dict[str, int]:
        """Posted counts per origin within the window; for the 50/50 UA/world balance."""
        con = self._connect()
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=int(lookback_hours))).isoformat(timespec="seconds")
        rows = con.execute(
            """
            SELECT COALESCE(origin,'world') AS o, COUNT(*) AS c
            FROM news_items
            WHERE status='posted' AND posted_at_utc IS NOT NULL AND posted_at_utc >= ?
            GROUP BY COALESCE(origin,'world')
            """,
            (cutoff,),
        ).fetchall()
        out = {"ua": 0, "world": 0}
        for r in rows:
            key = str(r["o"] or "world")
            out[key] = out.get(key, 0) + int(r["c"] or 0)
        out["total"] = out.get("ua", 0) + out.get("world", 0)
        return out

    def set_text_profile(self, item_hash: str, normalized_text: str, keywords: list[str], entities: list[str], numbers: list[str], event_verbs: list[str]) -> None:
        con = self._connect()
        con.execute(
            """
            UPDATE news_items
            SET normalized_text=?, keywords_json=?, entities_json=?, numbers_json=?, event_verbs_json=?
            WHERE item_hash=?
            """,
            (
                normalized_text,
                json.dumps(keywords, ensure_ascii=False),
                json.dumps(entities, ensure_ascii=False),
                json.dumps(numbers, ensure_ascii=False),
                json.dumps(event_verbs, ensure_ascii=False),
                item_hash,
            ),
        )
        con.commit()

    def get_recent_items_for_matching(self, since_iso: str, limit: int = 150):
        con = self._connect()
        rows = con.execute(
            """
            SELECT item_hash, normalized_text, keywords_json, entities_json, numbers_json, event_verbs_json
            FROM news_items
            WHERE item_hash IS NOT NULL
              AND normalized_text IS NOT NULL
              AND dup_of IS NULL
              AND (published_at_utc IS NULL OR published_at_utc >= ?)
            ORDER BY COALESCE(published_at_utc, created_at_utc) DESC
            LIMIT ?
            """,
            (since_iso, int(limit)),
        ).fetchall()
        out = []
        for r in rows:
            out.append({
                "item_hash": r["item_hash"],
                "normalized_text": r["normalized_text"] or "",
                "keywords": json.loads(r["keywords_json"] or "[]"),
                "entities": json.loads(r["entities_json"] or "[]"),
                "numbers": json.loads(r["numbers_json"] or "[]"),
                "event_verbs": json.loads(r["event_verbs_json"] or "[]"),
            })
        return out

    def update_image_url(self, item_hash: str, image_url: Optional[str]) -> None:
        con = self._connect()
        con.execute("UPDATE news_items SET image_url=? WHERE item_hash=?", (image_url, item_hash))
        con.commit()

    def set_embedding_and_dup(
        self,
        item_hash: str,
        embedding_blob: Optional[bytes],
        embedding_dim: Optional[int],
        embedding_model: Optional[str],
        dup_of: Optional[str],
        dup_score: Optional[float],
        lexical_score: Optional[float] = None,
        event_match_type: Optional[str] = None,
        same_event_of: Optional[str] = None,
    ) -> None:
        con = self._connect()
        con.execute(
            """
            UPDATE news_items
            SET embedding_blob=?, embedding_dim=?, embedding_model=?, dup_of=?, dup_score=?, lexical_score=?, event_match_type=?, same_event_of=?
            WHERE item_hash=?
            """,
            (embedding_blob, embedding_dim, embedding_model, dup_of, dup_score, lexical_score, event_match_type, same_event_of, item_hash),
        )
        con.commit()

    def get_recent_embeddings(self, since_iso: str) -> List[Tuple[str, bytes, int]]:
        con = self._connect()
        rows = con.execute(
            """
            SELECT item_hash, embedding_blob, embedding_dim
            FROM news_items
            WHERE embedding_blob IS NOT NULL
              AND embedding_dim IS NOT NULL
              AND dup_of IS NULL
              AND (published_at_utc IS NULL OR published_at_utc >= ?)
            """,
            (since_iso,),
        ).fetchall()
        return [(r["item_hash"], r["embedding_blob"], int(r["embedding_dim"])) for r in rows]

    def set_score_and_posttext(
        self,
        item_hash: str,
        score: float,
        should_post: bool,
        post_text: Optional[str],
        why: List[str],
        category_id: Optional[int] = None,
        tier: str | None = None,
        publish_mode: str | None = None,
        event_key: str | None = None,
        novelty_score: float | None = None,
        impact_score: float | None = None,
        ua_relevance_score: float | None = None,
        wrap_name: str | None = None,
        status: str | None = None,
        news_type: str | None = None,
        llm_has_new_fact: bool | None = None,
        topic_key: str | None = None,
        decision_mode: str | None = None,
        decision_reasons: List[str] | None = None,
        delay_until_utc: str | None = None,
        source_trust: str | None = None,
        source_count: int | None = None,
        fact_summary: str | None = None,
    ) -> None:
        con = self._connect()
        con.execute(
            """
            UPDATE news_items
            SET score=?, should_post=?, post_text=?, why_json=?, scored_at_utc=?, category_id=?,
                tier=?, publish_mode=?, event_key=?, novelty_score=?, impact_score=?, ua_relevance_score=?,
                wrap_name=?, status=COALESCE(?, status), news_type=?, llm_has_new_fact=?, topic_key=?,
                decision_mode=?, decision_reasons_json=?, delay_until_utc=?, source_trust=?, source_count=COALESCE(?, source_count),
                fact_summary=COALESCE(?, fact_summary)
            WHERE item_hash=?
            """,
            (
                float(score), 1 if should_post else 0, post_text, json.dumps(why[:6], ensure_ascii=False), utc_now_iso(), category_id,
                tier, publish_mode, event_key, novelty_score, impact_score, ua_relevance_score, wrap_name, status,
                news_type, 1 if llm_has_new_fact else 0, topic_key, decision_mode, json.dumps((decision_reasons or [])[:10], ensure_ascii=False),
                delay_until_utc, source_trust, source_count, fact_summary, item_hash,
            ),
        )
        con.commit()

    def mark_posted(self, item_hash: str, tg_message_id: Optional[int] = None) -> None:
        con = self._connect()
        con.execute(
            "UPDATE news_items SET status='posted', posted_at_utc=?, tg_message_id=COALESCE(?, tg_message_id) WHERE item_hash=?",
            (utc_now_iso(), tg_message_id, item_hash),
        )
        con.commit()

    # Base delay for error retries; attempt n waits BASE * 2^(n-1) minutes (10/20/40).
    RETRY_BASE_MINUTES = 10
    MAX_RETRIES = 3

    def mark_error(self, item_hash: str) -> None:
        """Record a failure and schedule an exponential-backoff retry."""
        con = self._connect()
        row = con.execute("SELECT retry_count FROM news_items WHERE item_hash=?", (item_hash,)).fetchone()
        retry_count = int(row["retry_count"] or 0) + 1 if row else 1
        delay_min = self.RETRY_BASE_MINUTES * (2 ** (retry_count - 1))
        next_retry = (datetime.now(timezone.utc) + timedelta(minutes=delay_min)).isoformat(timespec="seconds")
        con.execute(
            "UPDATE news_items SET status='error', retry_count=?, next_retry_utc=? WHERE item_hash=?",
            (retry_count, next_retry, item_hash),
        )
        con.commit()

    def mark_failed(self, item_hash: str) -> None:
        """Terminal state after MAX_RETRIES exhausted."""
        con = self._connect()
        con.execute("UPDATE news_items SET status='failed', next_retry_utc=NULL WHERE item_hash=?", (item_hash,))
        con.commit()

    def get_error_items_due(self, limit: int = 25) -> List[Dict[str, Any]]:
        con = self._connect()
        rows = con.execute(
            """
            SELECT ni.*, c.slug AS category_slug, c.hashtag AS category_hashtag
            FROM news_items ni
            LEFT JOIN categories c ON c.id = ni.category_id
            WHERE ni.status='error'
              AND ni.next_retry_utc IS NOT NULL
              AND ni.next_retry_utc <= ?
            ORDER BY ni.next_retry_utc ASC
            LIMIT ?
            """,
            (utc_now_iso(), int(limit)),
        ).fetchall()
        return [dict(r) for r in rows]

    def requeue_for_posting(self, item_hash: str) -> None:
        """Put an error item back on the publish path for another attempt."""
        con = self._connect()
        con.execute("UPDATE news_items SET status='pending_post', next_retry_utc=NULL WHERE item_hash=?", (item_hash,))
        con.commit()

    # ------------------------------------------------------------------
    # Phase 5: funnel metrics
    # ------------------------------------------------------------------

    def add_funnel_counts(self, counts: Dict[str, int]) -> None:
        con = self._connect()
        day = datetime.now(timezone.utc).date().isoformat()
        for stage, count in counts.items():
            if not count:
                continue
            con.execute(
                """
                INSERT INTO funnel_daily(day_utc, stage, count) VALUES (?, ?, ?)
                ON CONFLICT(day_utc, stage) DO UPDATE SET count = count + excluded.count
                """,
                (day, str(stage), int(count)),
            )
        con.commit()

    def get_funnel_sums(self, days: int = 2) -> Dict[str, Dict[str, int]]:
        """{day_utc: {stage: count}} for the last `days` days (UTC)."""
        con = self._connect()
        cutoff = (datetime.now(timezone.utc) - timedelta(days=int(days) - 1)).date().isoformat()
        rows = con.execute(
            "SELECT day_utc, stage, count FROM funnel_daily WHERE day_utc >= ? ORDER BY day_utc DESC",
            (cutoff,),
        ).fetchall()
        out: Dict[str, Dict[str, int]] = {}
        for r in rows:
            out.setdefault(str(r["day_utc"]), {})[str(r["stage"])] = int(r["count"] or 0)
        return out

    def set_embed_video_url(self, item_hash: str, url: Optional[str]) -> None:
        if not url:
            return
        con = self._connect()
        con.execute("UPDATE news_items SET embed_video_url=? WHERE item_hash=?", (url, item_hash))
        con.commit()

    # ------------------------------------------------------------------
    # Phase 1: moderation (human-in-the-loop)
    # ------------------------------------------------------------------

    def mark_pending_review(self, item_hash: str, review_message_id: Optional[int] = None) -> None:
        con = self._connect()
        con.execute(
            "UPDATE news_items SET status='pending_review', review_requested_at_utc=?, review_message_id=? WHERE item_hash=?",
            (utc_now_iso(), review_message_id, item_hash),
        )
        con.commit()

    def mark_rejected(self, item_hash: str) -> None:
        con = self._connect()
        con.execute("UPDATE news_items SET status='rejected' WHERE item_hash=?", (item_hash,))
        con.commit()

    def update_post_text(self, item_hash: str, post_text: str) -> None:
        con = self._connect()
        con.execute("UPDATE news_items SET post_text=? WHERE item_hash=?", (post_text, item_hash))
        con.commit()

    def get_item_for_moderation(self, item_id: int) -> Optional[Dict[str, Any]]:
        """Full row by numeric id (callback_data is limited to 64 bytes, hashes don't fit)."""
        con = self._connect()
        row = con.execute(
            """
            SELECT ni.*, c.slug AS category_slug, c.hashtag AS category_hashtag
            FROM news_items ni
            LEFT JOIN categories c ON c.id = ni.category_id
            WHERE ni.id=?
            """,
            (int(item_id),),
        ).fetchone()
        return dict(row) if row else None

    def get_review_timeouts(self, older_than_iso: str, limit: int = 50):
        con = self._connect()
        rows = con.execute(
            """
            SELECT ni.*, c.slug AS category_slug, c.hashtag AS category_hashtag
            FROM news_items ni
            LEFT JOIN categories c ON c.id = ni.category_id
            WHERE ni.status='pending_review'
              AND ni.review_requested_at_utc IS NOT NULL
              AND ni.review_requested_at_utc <= ?
            ORDER BY ni.review_requested_at_utc ASC
            LIMIT ?
            """,
            (older_than_iso, int(limit)),
        ).fetchall()
        return [dict(r) for r in rows]

    def add_moderation_log(
        self,
        item_hash: str,
        action: str,
        llm_score: Optional[float] = None,
        origin: Optional[str] = None,
        topic_key: Optional[str] = None,
        category: Optional[str] = None,
    ) -> None:
        con = self._connect()
        con.execute(
            """
            INSERT INTO moderation_log(item_hash, action, decided_at_utc, llm_score, origin, topic_key, category)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (item_hash, action, utc_now_iso(), llm_score, origin, topic_key, category),
        )
        con.commit()

    # ------------------------------------------------------------------
    # Phase 3: comparative ranking
    # ------------------------------------------------------------------

    def get_item_full(self, item_hash: str) -> Optional[Dict[str, Any]]:
        con = self._connect()
        row = con.execute(
            """
            SELECT ni.*, c.slug AS category_slug, c.hashtag AS category_hashtag
            FROM news_items ni
            LEFT JOIN categories c ON c.id = ni.category_id
            WHERE ni.item_hash=?
            """,
            (item_hash,),
        ).fetchone()
        return dict(row) if row else None

    def get_ranking_candidates(self, window_hours: int, limit: int = 100) -> List[Dict[str, Any]]:
        """Pool of classify-passed items awaiting a ranking decision."""
        con = self._connect()
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=int(window_hours))).isoformat(timespec="seconds")
        rows = con.execute(
            """
            SELECT ni.id, ni.item_hash, ni.title, ni.source, COALESCE(ni.origin,'world') AS origin,
                   ni.fact_summary, ni.topic_key, ni.event_key, ni.same_event_of, ni.score,
                   ni.created_at_utc, c.slug AS category_slug
            FROM news_items ni
            LEFT JOIN categories c ON c.id = ni.category_id
            WHERE ni.status='candidate'
              AND ni.dup_of IS NULL
              AND ni.created_at_utc >= ?
            ORDER BY ni.created_at_utc DESC
            LIMIT ?
            """,
            (cutoff, int(limit)),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_expired_candidates(self, max_age_hours: int, limit: int = 100) -> List[Dict[str, Any]]:
        """Candidates that never won a ranking cycle; they go to wrap/digest."""
        con = self._connect()
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=int(max_age_hours))).isoformat(timespec="seconds")
        rows = con.execute(
            """
            SELECT ni.*, c.slug AS category_slug, c.hashtag AS category_hashtag
            FROM news_items ni
            LEFT JOIN categories c ON c.id = ni.category_id
            WHERE ni.status='candidate'
              AND ni.created_at_utc < ?
            ORDER BY ni.created_at_utc ASC
            LIMIT ?
            """,
            (cutoff, int(limit)),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_recent_posted_topic_keys(self, limit: int = 10) -> List[str]:
        con = self._connect()
        rows = con.execute(
            """
            SELECT topic_key FROM news_items
            WHERE status='posted' AND topic_key IS NOT NULL AND posted_at_utc IS NOT NULL
            ORDER BY posted_at_utc DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        return [str(r["topic_key"]) for r in rows if r["topic_key"]]

    def save_ranking_log(self, candidates_json: str, picks_json: str, reasoning: Optional[str]) -> None:
        con = self._connect()
        con.execute(
            "INSERT INTO ranking_log(cycle_at_utc, candidates_json, picks_json, reasoning) VALUES (?, ?, ?, ?)",
            (utc_now_iso(), candidates_json, picks_json, reasoning),
        )
        con.commit()

    # ------------------------------------------------------------------
    # Phase 4: event clusters
    # ------------------------------------------------------------------

    def get_active_clusters(self, window_hours: int) -> List[Dict[str, Any]]:
        con = self._connect()
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=int(window_hours))).isoformat(timespec="seconds")
        rows = con.execute(
            """
            SELECT cluster_id, centroid_blob, centroid_dim, item_count, source_count, canonical_hash
            FROM event_clusters
            WHERE created_at_utc >= ?
            ORDER BY cluster_id DESC
            """,
            (cutoff,),
        ).fetchall()
        return [dict(r) for r in rows]

    def create_cluster(self, centroid_blob: bytes, centroid_dim: int, canonical_hash: str) -> int:
        con = self._connect()
        cur = con.execute(
            """
            INSERT INTO event_clusters(centroid_blob, centroid_dim, created_at_utc, canonical_hash, item_count, source_count)
            VALUES (?, ?, ?, ?, 1, 1)
            """,
            (centroid_blob, int(centroid_dim), utc_now_iso(), canonical_hash),
        )
        con.commit()
        return int(cur.lastrowid)

    def update_cluster(
        self,
        cluster_id: int,
        centroid_blob: bytes,
        item_count: int,
        source_count: int,
        canonical_hash: str,
    ) -> None:
        con = self._connect()
        con.execute(
            """
            UPDATE event_clusters
            SET centroid_blob=?, item_count=?, source_count=?, canonical_hash=?
            WHERE cluster_id=?
            """,
            (centroid_blob, int(item_count), int(source_count), canonical_hash, int(cluster_id)),
        )
        con.commit()

    def set_item_cluster(self, item_hash: str, cluster_id: int) -> None:
        con = self._connect()
        con.execute("UPDATE news_items SET cluster_id=? WHERE item_hash=?", (int(cluster_id), item_hash))
        con.commit()

    def get_item_cluster_id(self, item_hash: str) -> Optional[int]:
        con = self._connect()
        row = con.execute("SELECT cluster_id FROM news_items WHERE item_hash=?", (item_hash,)).fetchone()
        return int(row["cluster_id"]) if row and row["cluster_id"] is not None else None

    def get_cluster_stats(self, cluster_id: int) -> Dict[str, int]:
        con = self._connect()
        row = con.execute(
            "SELECT COUNT(*) AS items, COUNT(DISTINCT source) AS sources FROM news_items WHERE cluster_id=?",
            (int(cluster_id),),
        ).fetchone()
        return {"item_count": int(row["items"] or 0), "source_count": int(row["sources"] or 0)}

    def pick_cluster_canonical(self, cluster_id: int) -> Optional[str]:
        """Earliest item with the fullest article_text represents the cluster."""
        con = self._connect()
        row = con.execute(
            """
            SELECT item_hash FROM news_items
            WHERE cluster_id=?
            ORDER BY LENGTH(COALESCE(article_text,'')) DESC,
                     COALESCE(published_at_utc, created_at_utc) ASC
            LIMIT 1
            """,
            (int(cluster_id),),
        ).fetchone()
        return str(row["item_hash"]) if row else None

    def get_cluster_member_rows(self, cluster_id: int, lookback_hours: int = 48) -> List[Dict[str, Any]]:
        """Same shape as get_cluster_rows, keyed by cluster_id instead of event_key."""
        con = self._connect()
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=int(lookback_hours))).isoformat(timespec="seconds")
        rows = con.execute(
            """
            SELECT item_hash, source, title, created_at_utc, published_at_utc,
                   keywords_json, entities_json, numbers_json, event_verbs_json
            FROM news_items
            WHERE cluster_id=?
              AND COALESCE(created_at_utc, published_at_utc) >= ?
            ORDER BY COALESCE(published_at_utc, created_at_utc) ASC
            """,
            (int(cluster_id), cutoff),
        ).fetchall()
        out: List[Dict[str, Any]] = []
        for r in rows:
            out.append({
                "item_hash": r["item_hash"],
                "source": r["source"],
                "title": r["title"],
                "created_at_utc": r["created_at_utc"],
                "published_at_utc": r["published_at_utc"],
                "keywords": json.loads(r["keywords_json"] or "[]"),
                "entities": json.loads(r["entities_json"] or "[]"),
                "numbers": json.loads(r["numbers_json"] or "[]"),
                "event_verbs": json.loads(r["event_verbs_json"] or "[]"),
            })
        return out

    def get_clusters_for_wrap(self, min_items: int, min_sources: int, lookback_hours: int) -> List[Dict[str, Any]]:
        """Clusters big enough within the lookback to become a story wrap."""
        con = self._connect()
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=int(lookback_hours))).isoformat(timespec="seconds")
        rows = con.execute(
            """
            SELECT ni.cluster_id AS cluster_id,
                   COUNT(*) AS item_count,
                   COUNT(DISTINCT ni.source) AS source_count,
                   ec.canonical_hash AS canonical_hash,
                   (SELECT title FROM news_items WHERE item_hash=ec.canonical_hash) AS canonical_title
            FROM news_items ni
            JOIN event_clusters ec ON ec.cluster_id = ni.cluster_id
            WHERE ni.cluster_id IS NOT NULL
              AND COALESCE(ni.published_at_utc, ni.created_at_utc) >= ?
            GROUP BY ni.cluster_id
            HAVING COUNT(*) >= ? AND COUNT(DISTINCT ni.source) >= ?
            ORDER BY COUNT(*) DESC
            """,
            (cutoff, int(min_items), int(min_sources)),
        ).fetchall()
        return [dict(r) for r in rows]

    def pick_cluster_wrap_items(self, cluster_id: int, lookback_hours: int, limit: int = 12):
        """Wrappable members of a cluster in chronological order (story timeline)."""
        con = self._connect()
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=int(lookback_hours))).isoformat(timespec="seconds")
        return con.execute(
            """
            SELECT ni.*, c.slug AS category_slug, c.title AS category_title, c.hashtag AS category_hashtag
            FROM news_items ni
            LEFT JOIN categories c ON c.id = ni.category_id
            WHERE ni.cluster_id=?
              AND ni.status IN ('pending_wrap', 'digest_only')
              AND ni.dup_of IS NULL
              AND COALESCE(ni.published_at_utc, ni.created_at_utc) >= ?
            ORDER BY COALESCE(ni.published_at_utc, ni.created_at_utc) ASC
            LIMIT ?
            """,
            (int(cluster_id), cutoff, int(limit)),
        ).fetchall()

    # ------------------------------------------------------------------
    # Phase 2: engagement metrics
    # ------------------------------------------------------------------

    def get_recent_posted_for_engagement(self, since_iso: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Recently published posts that have a channel message id to track."""
        con = self._connect()
        rows = con.execute(
            """
            SELECT ni.item_hash, ni.tg_message_id, ni.title, ni.topic_key,
                   COALESCE(ni.origin,'world') AS origin, c.slug AS category_slug
            FROM news_items ni
            LEFT JOIN categories c ON c.id = ni.category_id
            WHERE ni.status='posted'
              AND ni.tg_message_id IS NOT NULL
              AND ni.posted_at_utc IS NOT NULL
              AND ni.posted_at_utc >= ?
            ORDER BY ni.posted_at_utc DESC
            LIMIT ?
            """,
            (since_iso, int(limit)),
        ).fetchall()
        return [dict(r) for r in rows]

    def apply_reaction_delta(self, chat_id: int, message_id: int, delta: int) -> None:
        con = self._connect()
        con.execute(
            """
            INSERT INTO reaction_tally(chat_id, message_id, reactions, updated_at_utc)
            VALUES (?, ?, MAX(0, ?), ?)
            ON CONFLICT(chat_id, message_id)
            DO UPDATE SET reactions=MAX(0, reaction_tally.reactions + ?), updated_at_utc=excluded.updated_at_utc
            """,
            (int(chat_id), int(message_id), int(delta), utc_now_iso(), int(delta)),
        )
        con.commit()

    def get_reaction_count(self, chat_id: int, message_id: int) -> int:
        con = self._connect()
        row = con.execute(
            "SELECT reactions FROM reaction_tally WHERE chat_id=? AND message_id=?",
            (int(chat_id), int(message_id)),
        ).fetchone()
        return int(row["reactions"]) if row else 0

    def has_any_reaction_tally(self) -> bool:
        con = self._connect()
        return con.execute("SELECT 1 FROM reaction_tally LIMIT 1").fetchone() is not None

    def add_post_metric(
        self,
        item_hash: str,
        views: Optional[int] = None,
        reactions: Optional[int] = None,
        forwards: Optional[int] = None,
    ) -> None:
        con = self._connect()
        con.execute(
            "INSERT INTO post_metrics(item_hash, captured_at_utc, views, reactions, forwards) VALUES (?, ?, ?, ?, ?)",
            (item_hash, utc_now_iso(), views, reactions, forwards),
        )
        con.commit()

    def get_engagement_summary(self, since_iso: str, limit: int = 200) -> List[Dict[str, Any]]:
        """
        Latest metric snapshot per post published within the window, joined with
        item attributes for the category/topic/origin breakdown in reports.
        """
        con = self._connect()
        rows = con.execute(
            """
            SELECT ni.item_hash, ni.title, ni.topic_key, COALESCE(ni.origin,'world') AS origin,
                   c.slug AS category_slug, pm.views, pm.reactions, pm.forwards, pm.captured_at_utc
            FROM news_items ni
            JOIN post_metrics pm ON pm.item_hash = ni.item_hash
            LEFT JOIN categories c ON c.id = ni.category_id
            WHERE ni.posted_at_utc IS NOT NULL AND ni.posted_at_utc >= ?
              AND pm.id = (SELECT MAX(id) FROM post_metrics WHERE item_hash = ni.item_hash)
            ORDER BY COALESCE(pm.views,0) + COALESCE(pm.reactions,0)*10 + COALESCE(pm.forwards,0)*20 DESC
            LIMIT ?
            """,
            (since_iso, int(limit)),
        ).fetchall()
        return [dict(r) for r in rows]

    def mark_wrapped(self, item_hashes: List[str], wrap_post_id: int) -> None:
        con = self._connect()
        con.executemany(
            "UPDATE news_items SET status='wrapped', wrap_post_id=? WHERE item_hash=?",
            [(wrap_post_id, h) for h in item_hashes],
        )
        con.commit()

    def cleanup_old(self, keep_days: int) -> int:
        con = self._connect()
        cutoff_iso = (datetime.now(timezone.utc) - timedelta(days=int(keep_days))).isoformat(timespec="seconds")
        cur = con.execute("DELETE FROM news_items WHERE created_at_utc < ?", (cutoff_iso,))
        con.commit()
        return int(cur.rowcount or 0)

    def cluster_sources_count(self, root_hash: str) -> int:
        con = self._connect()
        row = con.execute(
            """
            SELECT COUNT(DISTINCT source) AS c
            FROM news_items WHERE item_hash=? OR dup_of=?
            """,
            (root_hash, root_hash),
        ).fetchone()
        return int(row["c"] or 0)

    def pick_pending_roots(self, only_last_hours: int, limit: int = 25):
        con = self._connect()
        cutoff_iso = (datetime.now(timezone.utc) - timedelta(hours=int(only_last_hours))).isoformat(timespec="seconds")
        return con.execute(
            """
            SELECT ni.*, c.slug AS category_slug, c.title AS category_title, c.hashtag AS category_hashtag
            FROM news_items ni
            LEFT JOIN categories c ON c.id = ni.category_id
            WHERE status='pending_post' AND dup_of IS NULL AND should_post=1 AND post_text IS NOT NULL
              AND COALESCE(decision_mode, publish_mode,'post')='post'
              AND (delay_until_utc IS NULL OR delay_until_utc <= ?)
              AND (published_at_utc IS NULL OR published_at_utc >= ?)
            ORDER BY created_at_utc ASC
            LIMIT ?
            """,
            (utc_now_iso(), cutoff_iso, limit),
        ).fetchall()

    def pick_wrap_candidates(self, wrap_name: str, lookback_hours: int, limit: int = 20):
        con = self._connect()
        cutoff_iso = (datetime.now(timezone.utc) - timedelta(hours=int(lookback_hours))).isoformat(timespec="seconds")
        return con.execute(
            """
            SELECT ni.*, c.slug AS category_slug, c.title AS category_title, c.hashtag AS category_hashtag
            FROM news_items ni
            LEFT JOIN categories c ON c.id = ni.category_id
            WHERE status='pending_wrap'
              AND wrap_name=?
              AND dup_of IS NULL
              AND should_post=1
              AND post_text IS NOT NULL
              AND (published_at_utc IS NULL OR published_at_utc >= ? OR created_at_utc >= ?)
            ORDER BY COALESCE(score,0) DESC, created_at_utc ASC
            LIMIT ?
            """,
            (wrap_name, cutoff_iso, cutoff_iso, limit),
        ).fetchall()

    def get_last_wrap_posted_at(self, wrap_name: str) -> Optional[str]:
        con = self._connect()
        row = con.execute(
            "SELECT posted_at_utc FROM market_wrap_posts WHERE wrap_name=? ORDER BY posted_at_utc DESC LIMIT 1",
            (wrap_name,),
        ).fetchone()
        return str(row["posted_at_utc"]) if row and row["posted_at_utc"] else None

    def save_wrap_post(self, wrap_name: str, item_hashes: List[str], source_count: int, post_text: str) -> int:
        con = self._connect()
        cur = con.execute(
            """
            INSERT INTO market_wrap_posts(wrap_name, item_hashes_json, source_count, post_text, created_at_utc, posted_at_utc)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (wrap_name, json.dumps(item_hashes, ensure_ascii=False), int(source_count), post_text, utc_now_iso(), utc_now_iso()),
        )
        con.commit()
        return int(cur.lastrowid)

    def get_cluster_rows(self, root_hash: str, lookback_hours: int = 48) -> List[Dict[str, Any]]:
        con = self._connect()
        cutoff_iso = (datetime.now(timezone.utc) - timedelta(hours=int(lookback_hours))).isoformat(timespec="seconds")
        rows = con.execute(
            """
            SELECT item_hash, source, title, created_at_utc, published_at_utc, keywords_json, entities_json, numbers_json, event_verbs_json
            FROM news_items
            WHERE (item_hash=? OR same_event_of=? OR event_key=(SELECT event_key FROM news_items WHERE item_hash=? LIMIT 1))
              AND COALESCE(created_at_utc, published_at_utc) >= ?
            ORDER BY COALESCE(published_at_utc, created_at_utc) ASC
            """,
            (root_hash, root_hash, root_hash, cutoff_iso),
        ).fetchall()
        out: List[Dict[str, Any]] = []
        for r in rows:
            out.append({
                "item_hash": r["item_hash"],
                "source": r["source"],
                "title": r["title"],
                "created_at_utc": r["created_at_utc"],
                "published_at_utc": r["published_at_utc"],
                "keywords": json.loads(r["keywords_json"] or "[]"),
                "entities": json.loads(r["entities_json"] or "[]"),
                "numbers": json.loads(r["numbers_json"] or "[]"),
                "event_verbs": json.loads(r["event_verbs_json"] or "[]"),
            })
        return out

    def get_topic_post_history(self, topic_key: str, window_hours: List[int]) -> Dict[str, int]:
        con = self._connect()
        now = datetime.now(timezone.utc)
        out: Dict[str, int] = {}
        for hours in window_hours:
            cutoff = (now - timedelta(hours=int(hours))).isoformat(timespec="seconds")
            row = con.execute(
                """
                SELECT COUNT(*) AS c
                FROM news_items
                WHERE topic_key=?
                  AND status='posted'
                  AND posted_at_utc IS NOT NULL
                  AND posted_at_utc >= ?
                """,
                (topic_key, cutoff),
            ).fetchone()
            out[f"{int(hours)}h"] = int(row["c"] or 0) if row else 0
        return out

    def get_topic_share(self, topic_key: str, lookback_hours: int) -> Dict[str, int]:
        con = self._connect()
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=int(lookback_hours))).isoformat(timespec="seconds")
        total = con.execute(
            "SELECT COUNT(*) AS c FROM news_items WHERE status='posted' AND posted_at_utc IS NOT NULL AND posted_at_utc >= ?",
            (cutoff,),
        ).fetchone()
        topic = con.execute(
            "SELECT COUNT(*) AS c FROM news_items WHERE status='posted' AND topic_key=? AND posted_at_utc IS NOT NULL AND posted_at_utc >= ?",
            (topic_key, cutoff),
        ).fetchone()
        return {"total_posts": int(total["c"] or 0) if total else 0, "topic_posts": int(topic["c"] or 0) if topic else 0}

    def get_event_source_count(self, root_hash: str, event_key: str | None = None) -> int:
        con = self._connect()
        row = con.execute(
            """
            SELECT COUNT(DISTINCT source) AS c
            FROM news_items
            WHERE item_hash=? OR same_event_of=? OR (? IS NOT NULL AND event_key=?)
            """,
            (root_hash, root_hash, event_key, event_key),
        ).fetchone()
        return int(row["c"] or 0) if row else 0

    def daily_summary_exists(self, day_utc: str) -> bool:
        con = self._connect()
        return con.execute("SELECT 1 FROM daily_summaries WHERE day_utc=?", (day_utc,)).fetchone() is not None

    def save_daily_summary(self, day_utc: str, post_text: str) -> None:
        con = self._connect()
        con.execute("INSERT OR REPLACE INTO daily_summaries(day_utc, posted_at_utc, post_text) VALUES (?, ?, ?)", (day_utc, utc_now_iso(), post_text))
        con.commit()

    def get_post_texts_for_day(self, day_utc: str) -> List[str]:
        con = self._connect()
        rows = con.execute(
            """
            SELECT post_text, posted_at_utc FROM news_items
            WHERE status='posted' AND post_text IS NOT NULL AND posted_at_utc IS NOT NULL AND substr(posted_at_utc,1,10)=?
            UNION ALL
            SELECT post_text, posted_at_utc FROM market_wrap_posts
            WHERE substr(posted_at_utc,1,10)=?
            ORDER BY posted_at_utc ASC
            """,
            (day_utc, day_utc),
        ).fetchall()
        out = [(r["post_text"] or "").strip() for r in rows if (r["post_text"] or "").strip()]
        if out:
            return out
        # Fallback: build digest from scored-but-not-posted rows if channel was quiet
        rows = con.execute(
            """
            SELECT post_text FROM news_items
            WHERE post_text IS NOT NULL AND substr(created_at_utc,1,10)=?
            ORDER BY COALESCE(score,0) DESC, created_at_utc ASC
            LIMIT 20
            """,
            (day_utc,),
        ).fetchall()
        return [(r["post_text"] or "").strip() for r in rows if (r["post_text"] or "").strip()]


    def analytics_report_exists(self, day_local: str) -> bool:
        con = self._connect()
        return con.execute("SELECT 1 FROM analytics_reports WHERE day_local=?", (day_local,)).fetchone() is not None

    def save_analytics_report(self, day_local: str, post_text: str) -> None:
        con = self._connect()
        con.execute(
            "INSERT OR REPLACE INTO analytics_reports(day_local, posted_at_utc, post_text) VALUES (?, ?, ?)",
            (day_local, utc_now_iso(), post_text),
        )
        con.commit()

    def set_bot_state(self, key: str, value: str) -> None:
        con = self._connect()
        con.execute(
            "INSERT INTO bot_state(key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        con.commit()

    def get_bot_state(self, key: str, default: str | None = None) -> str | None:
        con = self._connect()
        row = con.execute("SELECT value FROM bot_state WHERE key=?", (key,)).fetchone()
        if not row:
            return default
        return row["value"] if row["value"] is not None else default

    def get_bot_state_int(self, key: str, default: int = 0) -> int:
        val = self.get_bot_state(key, None)
        try:
            return int(val) if val is not None else int(default)
        except Exception:
            return int(default)

    def delete_bot_state(self, key: str) -> None:
        con = self._connect()
        con.execute("DELETE FROM bot_state WHERE key=?", (key,))
        con.commit()

    def set_prompt_override(self, key: str, value: str) -> None:
        self.set_bot_state(f"prompt_override:{key}", value)

    def get_prompt_override(self, key: str, default: str | None = None) -> str | None:
        return self.get_bot_state(f"prompt_override:{key}", default)

    def delete_prompt_override(self, key: str) -> None:
        self.delete_bot_state(f"prompt_override:{key}")

    def _utc_range_for_local_day(self, day_local: str, tz_name: str) -> tuple[str, str]:
        from datetime import datetime, timedelta, timezone
        from zoneinfo import ZoneInfo

        tz = ZoneInfo(tz_name)
        start_local = datetime.fromisoformat(day_local).replace(tzinfo=tz)
        end_local = start_local + timedelta(days=1)
        start_utc = start_local.astimezone(timezone.utc).isoformat(timespec="seconds")
        end_utc = end_local.astimezone(timezone.utc).isoformat(timespec="seconds")
        return start_utc, end_utc

    def get_analytics_snapshot(self, day_local: str, tz_name: str) -> Dict[str, int]:
        con = self._connect()
        start_utc, end_utc = self._utc_range_for_local_day(day_local, tz_name)

        row = con.execute(
            "SELECT COUNT(*) AS c FROM news_items WHERE scored_at_utc IS NOT NULL AND scored_at_utc >= ? AND scored_at_utc < ?",
            (start_utc, end_utc),
        ).fetchone()
        scored = int(row["c"] or 0) if row else 0

        row = con.execute(
            "SELECT COUNT(*) AS c FROM news_items WHERE status='posted' AND posted_at_utc IS NOT NULL AND posted_at_utc >= ? AND posted_at_utc < ?",
            (start_utc, end_utc),
        ).fetchone()
        posted_news = int(row["c"] or 0) if row else 0

        row = con.execute(
            "SELECT COUNT(*) AS c FROM market_wrap_posts WHERE posted_at_utc >= ? AND posted_at_utc < ?",
            (start_utc, end_utc),
        ).fetchone()
        posted_wraps = int(row["c"] or 0) if row else 0

        total_news = con.execute("SELECT COUNT(*) AS c FROM news_items WHERE status='posted'").fetchone()
        total_wraps = con.execute("SELECT COUNT(*) AS c FROM market_wrap_posts").fetchone()

        return {
            'posts_today': posted_news + posted_wraps,
            'news_scored_today': scored,
            'total_posts_all_time': int(total_news["c"] or 0) + int(total_wraps["c"] or 0),
        }

    def export_table_to_csv(self, table_name: str, out_path: str) -> None:
        import csv

        allowed = {'news_items', 'market_wrap_posts'}
        if table_name not in allowed:
            raise ValueError(f'Unsupported export table: {table_name}')

        con = self._connect()
        rows = con.execute(f'SELECT * FROM {table_name}').fetchall()
        cols = [r[1] for r in con.execute(f'PRAGMA table_info({table_name})').fetchall()]
        os.makedirs(os.path.dirname(out_path) or '.', exist_ok=True)
        with open(out_path, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(cols)
            for row in rows:
                writer.writerow([row[c] for c in cols])

    def get_post_counts_by_day(self, days: int, tz_name: str) -> List[Dict[str, Any]]:
        from collections import OrderedDict
        from zoneinfo import ZoneInfo

        tz = ZoneInfo(tz_name)
        now_local = datetime.now(tz)
        base = OrderedDict()
        for i in range(days - 1, -1, -1):
            day = (now_local - timedelta(days=i)).date().isoformat()
            base[day] = 0

        start_day = next(iter(base.keys()))
        start_utc, _ = self._utc_range_for_local_day(start_day, tz_name)
        con = self._connect()
        rows = con.execute(
            """
            SELECT posted_at_utc AS ts FROM news_items WHERE status='posted' AND posted_at_utc IS NOT NULL AND posted_at_utc >= ?
            UNION ALL
            SELECT posted_at_utc AS ts FROM market_wrap_posts WHERE posted_at_utc IS NOT NULL AND posted_at_utc >= ?
            """,
            (start_utc, start_utc),
        ).fetchall()
        for row in rows:
            try:
                day = datetime.fromisoformat(row['ts']).astimezone(tz).date().isoformat()
            except Exception:
                continue
            if day in base:
                base[day] += 1
        return [{'day': k, 'posts': v} for k, v in base.items()]

    def get_hashtag_post_counts(self, days: int, tz_name: str, wrap_hashtag_by_name: Dict[str, str]) -> List[Dict[str, Any]]:
        from collections import defaultdict
        from zoneinfo import ZoneInfo

        tz = ZoneInfo(tz_name)
        now_local = datetime.now(tz)
        start_day = (now_local - timedelta(days=days - 1)).date().isoformat()
        start_utc, _ = self._utc_range_for_local_day(start_day, tz_name)
        con = self._connect()
        counts = defaultdict(int)

        rows = con.execute(
            """
            SELECT COALESCE(c.hashtag, '#інше') AS hashtag
            FROM news_items ni
            LEFT JOIN categories c ON c.id = ni.category_id
            WHERE ni.status='posted' AND ni.posted_at_utc IS NOT NULL AND ni.posted_at_utc >= ?
            """,
            (start_utc,),
        ).fetchall()
        for row in rows:
            counts[str(row['hashtag'] or '#інше')] += 1

        rows = con.execute(
            "SELECT wrap_name FROM market_wrap_posts WHERE posted_at_utc IS NOT NULL AND posted_at_utc >= ?",
            (start_utc,),
        ).fetchall()
        for row in rows:
            counts[str(wrap_hashtag_by_name.get(str(row['wrap_name'] or ''), '#інше'))] += 1

        return [
            {'hashtag': tag, 'posts': counts[tag]}
            for tag in sorted(counts.keys(), key=lambda x: (-counts[x], x))
        ]

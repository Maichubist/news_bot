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


SCHEMA_V4 = """
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
    FOREIGN KEY(category_id) REFERENCES categories(id)
);

CREATE INDEX IF NOT EXISTS idx_news_status_created ON news_items(status, created_at_utc);
CREATE INDEX IF NOT EXISTS idx_news_published ON news_items(published_at_utc);
CREATE INDEX IF NOT EXISTS idx_news_dup_of ON news_items(dup_of);
CREATE INDEX IF NOT EXISTS idx_news_should_post ON news_items(should_post, status);
CREATE INDEX IF NOT EXISTS idx_news_category ON news_items(category_id);
CREATE INDEX IF NOT EXISTS idx_news_wrap_status ON news_items(wrap_name, status, created_at_utc);
CREATE INDEX IF NOT EXISTS idx_news_event_key ON news_items(event_key);

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
"""

DESIRED_COLS = [
    "id", "item_hash", "source", "title", "link", "summary", "image_url", "published_at_utc",
    "created_at_utc", "posted_at_utc", "status", "category_id", "embedding_blob", "embedding_dim",
    "embedding_model", "dup_of", "dup_score", "score", "should_post", "post_text", "why_json",
    "scored_at_utc", "tier", "publish_mode", "event_key", "novelty_score", "impact_score",
    "ua_relevance_score", "wrap_name", "wrap_post_id",
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
        con.executescript(SCHEMA_V4)
        con.commit()
        self._migrate_news_items(con)

    def _migrate_news_items(self, con: sqlite3.Connection) -> None:
        cols = [r["name"] for r in con.execute("PRAGMA table_info(news_items)").fetchall()]
        if not cols or set(cols) == set(DESIRED_COLS):
            return
        log.info("Rebuilding DB schema to v4")
        con.executescript(
            """
            CREATE TABLE IF NOT EXISTS news_items_v4 (
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
            INSERT OR IGNORE INTO news_items_v4 (
                item_hash, source, title, link, summary, image_url, published_at_utc, created_at_utc,
                posted_at_utc, status, category_id, embedding_blob, embedding_dim, embedding_model,
                dup_of, dup_score, score, should_post, post_text, why_json, scored_at_utc,
                tier, publish_mode, event_key, novelty_score, impact_score, ua_relevance_score,
                wrap_name, wrap_post_id
            )
            SELECT
                item_hash, source, title, link, summary, {img_expr}, published_at_utc, created_at_utc,
                posted_at_utc, {status_expr}, {cat_expr}, {emb_expr}, embedding_dim, embedding_model,
                dup_of, dup_score, score, COALESCE(should_post, 0), post_text, {why_expr}, scored_at_utc,
                NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
            FROM news_items
            """
        )
        con.execute("DROP TABLE news_items")
        con.execute("ALTER TABLE news_items_v4 RENAME TO news_items")
        con.executescript(
            """
            CREATE INDEX IF NOT EXISTS idx_news_status_created ON news_items(status, created_at_utc);
            CREATE INDEX IF NOT EXISTS idx_news_published ON news_items(published_at_utc);
            CREATE INDEX IF NOT EXISTS idx_news_dup_of ON news_items(dup_of);
            CREATE INDEX IF NOT EXISTS idx_news_should_post ON news_items(should_post, status);
            CREATE INDEX IF NOT EXISTS idx_news_category ON news_items(category_id);
            CREATE INDEX IF NOT EXISTS idx_news_wrap_status ON news_items(wrap_name, status, created_at_utc);
            CREATE INDEX IF NOT EXISTS idx_news_event_key ON news_items(event_key);
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

    def upsert_item(self, item_hash: str, source: str, title: str, link: str, summary: Optional[str], published_at_utc: Optional[str], image_url: Optional[str] = None) -> bool:
        con = self._connect()
        cur = con.execute(
            """
            INSERT INTO news_items (item_hash, source, title, link, summary, image_url, published_at_utc, created_at_utc, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'new')
            ON CONFLICT(item_hash) DO NOTHING
            """,
            (item_hash, source, title, link, summary, image_url, published_at_utc, utc_now_iso()),
        )
        con.commit()
        return cur.rowcount == 1

    def update_image_url(self, item_hash: str, image_url: Optional[str]) -> None:
        con = self._connect()
        con.execute("UPDATE news_items SET image_url=? WHERE item_hash=?", (image_url, item_hash))
        con.commit()

    def set_embedding_and_dup(self, item_hash: str, embedding_blob: Optional[bytes], embedding_dim: Optional[int], embedding_model: Optional[str], dup_of: Optional[str], dup_score: Optional[float]) -> None:
        con = self._connect()
        con.execute(
            """
            UPDATE news_items
            SET embedding_blob=?, embedding_dim=?, embedding_model=?, dup_of=?, dup_score=?
            WHERE item_hash=?
            """,
            (embedding_blob, embedding_dim, embedding_model, dup_of, dup_score, item_hash),
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
    ) -> None:
        con = self._connect()
        con.execute(
            """
            UPDATE news_items
            SET score=?, should_post=?, post_text=?, why_json=?, scored_at_utc=?, category_id=?,
                tier=?, publish_mode=?, event_key=?, novelty_score=?, impact_score=?, ua_relevance_score=?,
                wrap_name=?, status=COALESCE(?, status)
            WHERE item_hash=?
            """,
            (
                float(score), 1 if should_post else 0, post_text, json.dumps(why[:6], ensure_ascii=False), utc_now_iso(), category_id,
                tier, publish_mode, event_key, novelty_score, impact_score, ua_relevance_score, wrap_name, status, item_hash,
            ),
        )
        con.commit()

    def mark_posted(self, item_hash: str) -> None:
        con = self._connect()
        con.execute("UPDATE news_items SET status='posted', posted_at_utc=? WHERE item_hash=?", (utc_now_iso(), item_hash))
        con.commit()

    def mark_error(self, item_hash: str) -> None:
        con = self._connect()
        con.execute("UPDATE news_items SET status='error' WHERE item_hash=?", (item_hash,))
        con.commit()

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
            WHERE status='new' AND dup_of IS NULL AND should_post=1 AND post_text IS NOT NULL
              AND COALESCE(publish_mode,'post')='post'
              AND (published_at_utc IS NULL OR published_at_utc >= ?)
            ORDER BY created_at_utc ASC
            LIMIT ?
            """,
            (cutoff_iso, limit),
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

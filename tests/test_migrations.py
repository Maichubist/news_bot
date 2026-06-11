from __future__ import annotations

import sqlite3

from app.storage.sqlite_repo import SCHEMA_V5, SqliteNewsRepository


def _columns(con: sqlite3.Connection, table: str) -> set[str]:
    return {r[1] for r in con.execute(f"PRAGMA table_info({table})").fetchall()}


def test_fresh_db_has_moderation_schema(tmp_path):
    repo = SqliteNewsRepository(db_path=str(tmp_path / "fresh.db"))
    repo.init_db()
    con = repo._connect()

    cols = _columns(con, "news_items")
    assert "review_requested_at_utc" in cols
    assert "review_message_id" in cols

    tables = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    assert "moderation_log" in tables


def test_init_db_is_idempotent(tmp_path):
    repo = SqliteNewsRepository(db_path=str(tmp_path / "idem.db"))
    repo.init_db()
    repo.init_db()  # must not raise or rebuild
    con = repo._connect()
    assert "review_message_id" in _columns(con, "news_items")


def test_previous_version_db_migrates_and_keeps_data(tmp_path):
    """Simulate a DB created by the pre-moderation version (v5 + old extra cols)."""
    db_path = str(tmp_path / "legacy.db")
    con = sqlite3.connect(db_path)
    con.executescript(SCHEMA_V5)
    # old _ensure_extra_columns set (before Phase 1)
    for col in ("video_url TEXT NULL", "images_json TEXT NULL", "origin TEXT NULL", "article_text TEXT NULL"):
        con.execute(f"ALTER TABLE news_items ADD COLUMN {col}")
    con.execute(
        """
        INSERT INTO news_items (item_hash, source, title, link, created_at_utc, status)
        VALUES ('legacy-hash', 'Src', 'Title', 'https://x', '2026-06-01T00:00:00+00:00', 'posted')
        """
    )
    con.commit()
    con.close()

    repo = SqliteNewsRepository(db_path=db_path)
    repo.init_db()
    con = repo._connect()

    cols = _columns(con, "news_items")
    assert "review_requested_at_utc" in cols and "review_message_id" in cols
    row = con.execute("SELECT * FROM news_items WHERE item_hash='legacy-hash'").fetchone()
    assert row is not None
    assert row["status"] == "posted"
    # heavy rebuild must not have been triggered (id preserved, single row)
    assert con.execute("SELECT COUNT(*) AS c FROM news_items").fetchone()["c"] == 1


def test_moderation_log_append(tmp_path):
    repo = SqliteNewsRepository(db_path=str(tmp_path / "log.db"))
    repo.init_db()

    repo.add_moderation_log("h1", "approve", llm_score=0.91, origin="ua", topic_key="t", category="war")
    repo.add_moderation_log("h1", "reject", llm_score=0.91, origin="ua", topic_key="t", category="war")

    con = repo._connect()
    rows = con.execute("SELECT action FROM moderation_log WHERE item_hash='h1' ORDER BY id").fetchall()
    assert [r["action"] for r in rows] == ["approve", "reject"]

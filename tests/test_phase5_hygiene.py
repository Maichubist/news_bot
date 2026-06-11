from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.articles.extract import extract_article
from app.telegram.formatter import PostFormatter

from conftest import get_status, seed_item


# ---------------------------------------------------------------------------
# 5.1 Retry with exponential backoff
# ---------------------------------------------------------------------------

def backdate_retry(repo, item_hash):
    con = repo._connect()
    past = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat(timespec="seconds")
    con.execute("UPDATE news_items SET next_retry_utc=? WHERE item_hash=?", (past, item_hash))
    con.commit()


def test_mark_error_schedules_backoff(repo):
    seed_item(repo, item_hash="e-1")

    repo.mark_error("e-1")

    row = repo.get_item_full("e-1")
    assert row["status"] == "error"
    assert row["retry_count"] == 1
    assert row["next_retry_utc"] is not None

    repo.mark_error("e-1")
    row2 = repo.get_item_full("e-1")
    assert row2["retry_count"] == 2
    # second delay (20m) is longer than the first (10m)
    assert row2["next_retry_utc"] > row["next_retry_utc"]


def test_retry_requeues_until_max_then_fails_with_alert(repo, fake_tg, formatter, publisher):
    from test_pipeline_ranking import build_pipeline

    pipeline, _ = build_pipeline(repo, fake_tg, formatter, publisher, [], ranking_enabled=False)
    seed_item(repo, item_hash="e-2")

    # failures 1 and 2 -> requeued for another attempt
    for expected_retry in (1, 2):
        repo.mark_error("e-2")
        backdate_retry(repo, "e-2")
        counters = {"retried": 0, "failed": 0}
        pipeline._retry_errors(counters)
        assert counters["retried"] == 1, f"retry #{expected_retry} should requeue"
        assert get_status(repo, "e-2") == "pending_post"

    # failure 3 -> terminal 'failed' + admin alert
    repo.mark_error("e-2")
    backdate_retry(repo, "e-2")
    counters = {"retried": 0, "failed": 0}
    pipeline._retry_errors(counters)

    assert counters["failed"] == 1
    assert get_status(repo, "e-2") == "failed"
    alerts = [m for m in fake_tg.admin_messages() if "⚠️" in m["text"]]
    assert len(alerts) == 1


def test_error_not_retried_before_backoff_expires(repo, fake_tg, formatter, publisher):
    from test_pipeline_ranking import build_pipeline

    pipeline, _ = build_pipeline(repo, fake_tg, formatter, publisher, [], ranking_enabled=False)
    seed_item(repo, item_hash="e-3")
    repo.mark_error("e-3")  # next_retry in 10 minutes

    counters = {"retried": 0, "failed": 0}
    pipeline._retry_errors(counters)

    assert counters["retried"] == 0
    assert get_status(repo, "e-3") == "error"


# ---------------------------------------------------------------------------
# 5.2 Funnel metrics
# ---------------------------------------------------------------------------

def test_funnel_counts_accumulate_per_day(repo):
    repo.add_funnel_counts({"fetched": 10, "new": 3, "posted": 1})
    repo.add_funnel_counts({"fetched": 5, "new": 2, "zero_stage": 0})

    sums = repo.get_funnel_sums(days=2)
    today = datetime.now(timezone.utc).date().isoformat()
    assert sums[today]["fetched"] == 15
    assert sums[today]["new"] == 5
    assert sums[today]["posted"] == 1
    assert "zero_stage" not in sums[today]  # zeros are not stored


def test_run_once_writes_funnel(repo, fake_tg, formatter, publisher):
    from test_pipeline_ranking import build_pipeline, news_item

    pipeline, _ = build_pipeline(repo, fake_tg, formatter, publisher, [news_item()], ranking_enabled=False)
    pipeline.run_once()

    sums = repo.get_funnel_sums(days=1)
    today = datetime.now(timezone.utc).date().isoformat()
    assert sums[today]["fetched"] == 1
    assert sums[today]["new"] == 1
    assert sums[today]["scored"] == 1


def test_funnel_command_renders(repo, fake_tg):
    from types import SimpleNamespace

    from app.services.telegram_analytics_commands import TelegramAnalyticsCommands

    repo.add_funnel_counts({"fetched": 7, "posted": 2})
    svc = TelegramAnalyticsCommands(
        repo=repo, tg=fake_tg,
        analytics_service=SimpleNamespace(enabled=True, commands_enabled=True),
    )
    svc._handle_funnel(chat_id=999)

    msg = fake_tg.admin_messages()[-1]["text"]
    assert "fetched=7" in msg and "posted=2" in msg


# ---------------------------------------------------------------------------
# 5.4 Embed video (YouTube/Vimeo): link, never download
# ---------------------------------------------------------------------------

def test_extract_detects_youtube_og_video():
    html = """<html><head>
    <meta property="og:video" content="https://www.youtube.com/embed/abc123">
    </head><body><p>%s</p></body></html>""" % ("текст " * 60)

    art = extract_article(html, url="https://example.com/a")

    assert art.embed_video_url == "https://www.youtube.com/embed/abc123"
    assert art.video_url is None  # not downloadable, must not go to sendVideo


def test_extract_detects_vimeo_iframe():
    html = """<html><body>
    <iframe src="https://player.vimeo.com/video/987654" width="640"></iframe>
    <p>%s</p></body></html>""" % ("текст " * 60)

    art = extract_article(html, url="https://example.com/b")

    assert art.embed_video_url == "https://player.vimeo.com/video/987654"


def test_extract_ignores_non_embed_players():
    html = """<html><head>
    <meta property="og:video" content="https://cdn.example.com/player/clip.html">
    </head><body><p>%s</p></body></html>""" % ("текст " * 60)

    art = extract_article(html, url="https://example.com/c")

    assert art.embed_video_url is None


def test_formatter_appends_video_line():
    fmt = PostFormatter(include_source=True)

    text = fmt.format_row({
        "post_text": "Текст поста.",
        "link": "https://example.com/n",
        "source": "Суспільне",
        "category_hashtag": "#війна",
        "embed_video_url": "https://youtu.be/abc",
    })

    lines = text.split("\n")
    assert "▶️ Відео: https://youtu.be/abc" in lines
    # video line comes after the text but before hashtag/source
    assert lines.index("▶️ Відео: https://youtu.be/abc") < lines.index("#війна")


def test_formatter_without_video_unchanged():
    fmt = PostFormatter(include_source=True)
    text = fmt.format_row({
        "post_text": "Текст поста.",
        "link": "https://example.com/n",
        "source": "Суспільне",
        "category_hashtag": "#війна",
    })
    assert "▶️" not in text

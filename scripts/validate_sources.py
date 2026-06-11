"""
Валідатор RSS-джерел.

Перевіряє кожен фід з config.yaml:
  - HTTP-статус і час відповіді
  - кількість entries (з урахуванням bozo-фідів, які все одно парсяться)
  - свіжість найновішого item
  - наявність медіа (картинки/відео) у фіді

Запуск:
    python scripts/validate_sources.py            # звіт у консоль
    python scripts/validate_sources.py --telegram # + надіслати звіт в admin-чат

Рекомендовано ганяти перед додаванням нових джерел і раз на тиждень.
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import feedparser  # noqa: E402
from dotenv import load_dotenv  # noqa: E402

from app.config import AppConfig  # noqa: E402
from app.http import RequestsSession, build_verify_option  # noqa: E402
from app.text.datetime_parse import parse_datetime  # noqa: E402


def check_feed(http: RequestsSession, name: str, url: str) -> dict:
    out = {
        "name": name,
        "url": url,
        "ok": False,
        "status": None,
        "entries": 0,
        "bozo": False,
        "newest_age_h": None,
        "with_media": 0,
        "elapsed_ms": None,
        "error": None,
    }
    t0 = time.time()
    try:
        r = http.get(url, headers={
            "Accept": "application/rss+xml,application/xml;q=0.9,text/xml;q=0.8,*/*;q=0.7",
        })
        out["status"] = r.status_code
        out["elapsed_ms"] = int((time.time() - t0) * 1000)
        if not r.ok:
            out["error"] = f"HTTP {r.status_code}"
            return out
        feed = feedparser.parse(r.text)
        out["bozo"] = bool(getattr(feed, "bozo", False))
        entries = list(getattr(feed, "entries", []) or [])
        out["entries"] = len(entries)
        if not entries:
            out["error"] = "no entries"
            return out

        now = datetime.now(timezone.utc)
        newest = None
        media_cnt = 0
        for e in entries[:50]:
            dt = parse_datetime(e)
            if dt is not None:
                dtu = dt.astimezone(timezone.utc)
                if newest is None or dtu > newest:
                    newest = dtu
            has_media = bool(
                getattr(e, "media_content", None)
                or getattr(e, "media_thumbnail", None)
                or getattr(e, "enclosures", None)
            )
            if has_media:
                media_cnt += 1
        out["with_media"] = media_cnt
        if newest is not None:
            out["newest_age_h"] = round((now - newest).total_seconds() / 3600.0, 1)
        out["ok"] = True
        return out
    except Exception as ex:
        out["elapsed_ms"] = int((time.time() - t0) * 1000)
        out["error"] = str(ex)[:120]
        return out


def format_report(results: list[dict]) -> str:
    lines = ["Перевірка джерел RSS", ""]
    alive = [r for r in results if r["ok"]]
    dead = [r for r in results if not r["ok"]]

    lines.append(f"Живі: {len(alive)}/{len(results)}")
    lines.append("")
    for r in sorted(alive, key=lambda x: -(x["entries"] or 0)):
        age = f", свіже {r['newest_age_h']}h тому" if r["newest_age_h"] is not None else ""
        bozo = " [bozo]" if r["bozo"] else ""
        lines.append(
            f"✅ {r['name']}: {r['entries']} items, медіа у {r['with_media']}{age}{bozo} ({r['elapsed_ms']}ms)"
        )
    if dead:
        lines.append("")
        lines.append("Проблемні:")
        for r in dead:
            lines.append(f"❌ {r['name']}: {r['error']} ({r['url']})")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--telegram", action="store_true", help="надіслати звіт в admin-чат")
    args = parser.parse_args()

    load_dotenv()
    cfg = AppConfig.load(args.config)
    http = RequestsSession(timeout_sec=cfg.network.timeout_sec, verify_opt=build_verify_option(cfg.network.verify))

    results = []
    for src in cfg.sources:
        print(f"... перевіряю {src.name}", flush=True)
        results.append(check_feed(http, src.name, src.url))

    report = format_report(results)
    print()
    print(report)

    if args.telegram:
        from app.telegram.client import TelegramClient

        tg = TelegramClient(http=http, token=cfg.telegram.token, chat_id=cfg.telegram.chat_id, admin_chat_id=cfg.telegram.admin_chat_id)
        target = cfg.telegram.admin_chat_id or cfg.telegram.chat_id
        ok, _ = tg.send_message_with_id(report, disable_preview=True, chat_id=target)
        print(f"\nЗвіт у Telegram: {'надіслано' if ok else 'помилка'}")


if __name__ == "__main__":
    main()

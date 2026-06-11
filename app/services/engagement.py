from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Mapping, Optional

log = logging.getLogger("services.engagement")

# Update types we ask Telegram for when engagement is on. Without listing
# "message_reaction" explicitly the Bot API never delivers reaction updates.
ALLOWED_UPDATES_WITH_REACTIONS = ["message", "edited_message", "callback_query", "message_reaction"]

LAST_POLL_STATE_KEY = "engagement_last_poll_utc"
WARN_DATE_STATE_KEY = "engagement_warned_date"


class EngagementService:
    """
    Collects post engagement within Bot API limits:
      * reactions — accumulated from message_reaction updates (bot must be a
        channel admin; updates are routed here by the command poller);
      * views/forwards — unavailable via Bot API; an OPTIONAL MTProto path
        (telethon + env credentials) fills them when explicitly enabled.
    Snapshots are append-only rows in post_metrics, taken every poll_hours.
    With no usable signal the module degrades silently (one warning per day).
    """

    def __init__(self, cfg_engagement, repo, channel_chat_id: int):
        self.cfg = cfg_engagement
        self.repo = repo
        self.channel_chat_id = int(channel_chat_id)
        self.enabled = bool(getattr(cfg_engagement, "enabled", True))
        self.poll_hours = int(getattr(cfg_engagement, "poll_hours", 6) or 6)
        self.lookback_hours = int(getattr(cfg_engagement, "lookback_hours", 72) or 72)
        self.max_posts = int(getattr(cfg_engagement, "max_posts", 50) or 50)
        self.mtproto_enabled = bool(getattr(cfg_engagement, "mtproto_enabled", False))

    # ------------------------------------------------------------------
    # Reaction updates (called from the command poller thread)
    # ------------------------------------------------------------------

    def record_reaction_update(self, mr: Mapping[str, Any]) -> bool:
        """Apply a MessageReactionUpdated delta to the running tally."""
        if not self.enabled:
            return False
        chat = mr.get("chat") or {}
        chat_id = int(chat.get("id") or 0)
        if chat_id != self.channel_chat_id:
            return False
        message_id = int(mr.get("message_id") or 0)
        if not message_id:
            return False
        old_n = len(mr.get("old_reaction") or [])
        new_n = len(mr.get("new_reaction") or [])
        delta = new_n - old_n
        if delta == 0:
            return False
        self.repo.apply_reaction_delta(chat_id, message_id, delta)
        log.debug("Reaction delta %+d for message %d", delta, message_id)
        return True

    # ------------------------------------------------------------------
    # Periodic snapshots (called from run_once)
    # ------------------------------------------------------------------

    def maybe_collect(self) -> int:
        """Take a snapshot if poll_hours have passed. Returns rows written."""
        if not self.enabled:
            return 0
        now = datetime.now(timezone.utc)
        last = self.repo.get_bot_state(LAST_POLL_STATE_KEY, None)
        if last:
            try:
                if now - datetime.fromisoformat(last) < timedelta(hours=self.poll_hours):
                    return 0
            except Exception:
                pass

        written = self._collect_snapshot()
        self.repo.set_bot_state(LAST_POLL_STATE_KEY, now.isoformat(timespec="seconds"))
        return written

    def _collect_snapshot(self) -> int:
        since_iso = (datetime.now(timezone.utc) - timedelta(hours=self.lookback_hours)).isoformat(timespec="seconds")
        posts = self.repo.get_recent_posted_for_engagement(since_iso=since_iso, limit=self.max_posts)
        if not posts:
            return 0

        mtproto_stats = self._fetch_mtproto_stats([int(p["tg_message_id"]) for p in posts])

        have_any_signal = bool(mtproto_stats) or self.repo.has_any_reaction_tally()
        if not have_any_signal:
            self._warn_once_per_day(
                "Engagement: no metric source available (no reaction updates seen, MTProto off) — "
                "snapshots will contain zeros until the bot gets channel admin rights or MTProto is enabled"
            )

        written = 0
        for p in posts:
            msg_id = int(p["tg_message_id"])
            reactions = self.repo.get_reaction_count(self.channel_chat_id, msg_id)
            extra = mtproto_stats.get(msg_id, {})
            self.repo.add_post_metric(
                item_hash=str(p["item_hash"]),
                views=extra.get("views"),
                reactions=reactions,
                forwards=extra.get("forwards"),
            )
            written += 1
        log.info("Engagement snapshot: %d posts captured (mtproto=%s)", written, bool(mtproto_stats))
        return written

    # ------------------------------------------------------------------
    # Optional MTProto path — STRICTLY optional, never a hard dependency
    # ------------------------------------------------------------------

    def _fetch_mtproto_stats(self, message_ids: List[int]) -> Dict[int, Dict[str, Optional[int]]]:
        if not self.mtproto_enabled or not message_ids:
            return {}
        api_id = os.getenv("TELEGRAM_API_ID")
        api_hash = os.getenv("TELEGRAM_API_HASH")
        session = os.getenv("TELEGRAM_MTPROTO_SESSION")
        if not (api_id and api_hash and session):
            self._warn_once_per_day(
                "Engagement: mtproto_enabled but TELEGRAM_API_ID/TELEGRAM_API_HASH/"
                "TELEGRAM_MTPROTO_SESSION are not set — views/forwards skipped"
            )
            return {}
        try:
            from telethon.sessions import StringSession  # type: ignore
            from telethon.sync import TelegramClient as TelethonClient  # type: ignore
        except ImportError:
            self._warn_once_per_day("Engagement: mtproto_enabled but telethon is not installed — views/forwards skipped")
            return {}

        out: Dict[int, Dict[str, Optional[int]]] = {}
        try:
            with TelethonClient(StringSession(session), int(api_id), api_hash) as client:
                msgs = client.get_messages(self.channel_chat_id, ids=message_ids)
                for m in msgs or []:
                    if m is None:
                        continue
                    out[int(m.id)] = {
                        "views": int(getattr(m, "views", 0) or 0),
                        "forwards": int(getattr(m, "forwards", 0) or 0),
                    }
        except Exception as ex:
            self._warn_once_per_day(f"Engagement: MTProto fetch failed ({ex}) — views/forwards skipped")
            return {}
        return out

    def _warn_once_per_day(self, message: str) -> None:
        today = datetime.now(timezone.utc).date().isoformat()
        if self.repo.get_bot_state(WARN_DATE_STATE_KEY, None) == today:
            return
        self.repo.set_bot_state(WARN_DATE_STATE_KEY, today)
        log.warning(message)

    # ------------------------------------------------------------------
    # Report block (used by the daily admin report)
    # ------------------------------------------------------------------

    @staticmethod
    def _score(row: Mapping[str, Any]) -> int:
        # Views are the broadest signal, forwards the strongest endorsement.
        return (
            int(row.get("views") or 0)
            + int(row.get("reactions") or 0) * 10
            + int(row.get("forwards") or 0) * 20
        )

    def build_report_block(self, hours: Optional[int] = None) -> str:
        """Top-5 / bottom-5 posts plus category/topic/origin breakdown. '' if no data."""
        window = int(hours or self.lookback_hours)
        since_iso = (datetime.now(timezone.utc) - timedelta(hours=window)).isoformat(timespec="seconds")
        try:
            rows = self.repo.get_engagement_summary(since_iso=since_iso)
        except Exception:
            log.exception("Engagement summary query failed")
            return ""
        if not rows:
            return ""

        ranked = sorted(rows, key=self._score, reverse=True)

        def fmt(row: Mapping[str, Any]) -> str:
            title = (str(row.get("title") or ""))[:60]
            parts = []
            if row.get("views") is not None:
                parts.append(f"👁{row['views']}")
            parts.append(f"❤️{int(row.get('reactions') or 0)}")
            if row.get("forwards") is not None:
                parts.append(f"↗️{row['forwards']}")
            return f"• {title} — {' '.join(parts)}"

        lines = [f"Engagement за {window} год ({len(ranked)} постів):", "", "Топ-5:"]
        lines += [fmt(r) for r in ranked[:5]]
        if len(ranked) > 5:
            lines += ["", "Анти-топ-5:"]
            lines += [fmt(r) for r in ranked[-5:]]

        for field, label in (("category_slug", "Категорії"), ("topic_key", "Теми"), ("origin", "Origin")):
            agg: Dict[str, List[int]] = {}
            for r in ranked:
                key = str(r.get(field) or "—")
                agg.setdefault(key, []).append(self._score(r))
            top = sorted(agg.items(), key=lambda kv: -(sum(kv[1]) / len(kv[1])))[:5]
            lines += ["", f"{label} (середній engagement):"]
            lines += [f"• {k}: {sum(v) / len(v):.0f} ({len(v)} постів)" for k, v in top]

        return "\n".join(lines)

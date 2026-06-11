from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Optional

log = logging.getLogger("services.moderation")

REGEN_INSTRUCTION = (
    "Попередній варіант посту не підійшов редактору. Перепиши пост суттєво інакше: "
    "зміни структуру і формулювання, але СТРОГО збережи факти з наданого тексту."
)


class ModerationService:
    """
    Human-in-the-loop review: items approved by editorial policy go to the admin
    chat with an inline keyboard instead of straight to the channel.

    Status flow: pending_post -> pending_review -> posted | rejected.
    Every admin decision is appended to moderation_log (future calibration dataset).
    Runtime toggle lives in bot_state so /moderation on|off works without restart.
    """

    STATE_KEY = "moderation_enabled"

    def __init__(self, cfg_moderation, repo, tg, formatter, publisher, postmaker=None):
        self.cfg = cfg_moderation
        self.repo = repo
        self.tg = tg
        self.formatter = formatter
        self.publisher = publisher
        self.postmaker = postmaker
        self.timeout_minutes = int(getattr(cfg_moderation, "timeout_minutes", 45) or 45)
        self.on_timeout = str(getattr(cfg_moderation, "on_timeout", "skip") or "skip").strip().lower()

    # ------------------------------------------------------------------
    # Toggle
    # ------------------------------------------------------------------

    def is_enabled(self) -> bool:
        if self.tg.admin_chat_id is None:
            return False
        state = self.repo.get_bot_state(self.STATE_KEY, None)
        if state is not None:
            return state == "1"
        return bool(getattr(self.cfg, "enabled", False))

    def set_enabled(self, on: bool) -> None:
        self.repo.set_bot_state(self.STATE_KEY, "1" if on else "0")
        log.info("Moderation switched %s via admin command", "on" if on else "off")

    # ------------------------------------------------------------------
    # Review submission (called from the pipeline)
    # ------------------------------------------------------------------

    def submit_for_review(self, row: Mapping[str, Any]) -> bool:
        """Send a post preview with the moderation keyboard to the admin chat."""
        item_id = int(row["id"])
        item_hash = str(row["item_hash"])
        post_text = (self._get(row, "post_text") or "").strip()
        if not post_text:
            return False

        preview = self._format_preview(row, post_text)
        video_url, images = self.publisher.media_from_row(row)
        ok, msg_id = self.publisher.send_media(
            text=preview,
            video_url=video_url,
            image_urls=images,
            chat_id=self.tg.admin_chat_id,
            reply_markup=self._keyboard(item_id),
        )
        if not ok:
            log.warning("Failed to send review preview for %s", item_hash)
            return False

        self.repo.mark_pending_review(item_hash, review_message_id=msg_id)
        log.info("Sent item %s (id=%d) for review, msg_id=%s", item_hash[:12], item_id, msg_id)
        return True

    # ------------------------------------------------------------------
    # Callback handling (called from the command poller thread)
    # ------------------------------------------------------------------

    def handle_callback(self, cq: Mapping[str, Any]) -> bool:
        """Process a callback_query; returns True if it was a moderation callback."""
        data = str(cq.get("data") or "")
        if not data.startswith("mod:"):
            return False
        cq_id = str(cq.get("id") or "")

        try:
            _, action, sid = data.split(":", 2)
            item_id = int(sid)
        except Exception:
            self.tg.answer_callback_query(cq_id, "Невалідний callback")
            return True

        row = self.repo.get_item_for_moderation(item_id)
        if row is None:
            self.tg.answer_callback_query(cq_id, "Item не знайдено в БД")
            return True
        if str(row.get("status") or "") != "pending_review":
            self.tg.answer_callback_query(cq_id, f"Вже оброблено (status={row.get('status')})")
            self._remove_keyboard_from_callback(cq)
            return True

        if action == "approve":
            self._do_approve(cq, cq_id, row)
        elif action == "reject":
            self._do_reject(cq, cq_id, row)
        elif action == "regen":
            self._do_regenerate(cq, cq_id, row)
        else:
            self.tg.answer_callback_query(cq_id, f"Невідома дія: {action}")
        return True

    def _do_approve(self, cq: Mapping[str, Any], cq_id: str, row: Mapping[str, Any]) -> None:
        item_hash = str(row["item_hash"])
        ok, msg_id = self._publish_row(row)
        if ok:
            self.repo.mark_posted(item_hash, tg_message_id=msg_id)
            self._log(row, "approve")
            self.tg.answer_callback_query(cq_id, "Опубліковано ✅")
            log.info("Moderation approve: %s posted", item_hash[:12])
        else:
            self.repo.mark_error(item_hash)
            self._log(row, "approve_failed")
            self.tg.answer_callback_query(cq_id, "Помилка публікації, item у status=error")
            log.warning("Moderation approve failed to publish %s", item_hash[:12])
        self._remove_keyboard_from_callback(cq)

    def _do_reject(self, cq: Mapping[str, Any], cq_id: str, row: Mapping[str, Any]) -> None:
        item_hash = str(row["item_hash"])
        self.repo.mark_rejected(item_hash)
        self._log(row, "reject")
        self.tg.answer_callback_query(cq_id, "Відхилено ❌")
        self._remove_keyboard_from_callback(cq)
        log.info("Moderation reject: %s", item_hash[:12])

    def _do_regenerate(self, cq: Mapping[str, Any], cq_id: str, row: Mapping[str, Any]) -> None:
        item_hash = str(row["item_hash"])
        if self.postmaker is None:
            self.tg.answer_callback_query(cq_id, "Перегенерація недоступна")
            return

        # Answer first: the LLM call takes seconds and the button would keep spinning.
        self.tg.answer_callback_query(cq_id, "Генерую новий варіант…")

        decision = self.postmaker.make(
            title=str(self._get(row, "title") or ""),
            summary=self._get(row, "summary"),
            source=str(self._get(row, "source") or ""),
            url=str(self._get(row, "link") or ""),
            article_text=self._get(row, "article_text"),
            extra_instruction=REGEN_INSTRUCTION,
        )
        new_text = (getattr(decision, "post_text", "") or "").strip() if decision else ""
        if not new_text:
            self.tg.send_message_with_id(
                "Не вдалося перегенерувати пост, лишаю попередній варіант.",
                disable_preview=True,
                chat_id=self.tg.admin_chat_id,
            )
            log.warning("Moderation regen failed for %s", item_hash[:12])
            return

        self.repo.update_post_text(item_hash, new_text)
        self._log(row, "regenerate")
        self._remove_keyboard_from_callback(cq)

        # Re-send the preview with a fresh keyboard and reset the review timer.
        fresh = self.repo.get_item_for_moderation(int(row["id"])) or dict(row)
        fresh["post_text"] = new_text
        preview = self._format_preview(fresh, new_text)
        video_url, images = self.publisher.media_from_row(fresh)
        ok, msg_id = self.publisher.send_media(
            text=preview,
            video_url=video_url,
            image_urls=images,
            chat_id=self.tg.admin_chat_id,
            reply_markup=self._keyboard(int(row["id"])),
        )
        if ok:
            self.repo.mark_pending_review(item_hash, review_message_id=msg_id)
        log.info("Moderation regen: new preview for %s sent=%s", item_hash[:12], ok)

    # ------------------------------------------------------------------
    # Timeouts (called from run_once)
    # ------------------------------------------------------------------

    def check_timeouts(self) -> int:
        """Resolve pending_review items older than timeout_minutes. Returns handled count."""
        cutoff = (datetime.now(timezone.utc) - timedelta(minutes=self.timeout_minutes)).isoformat(timespec="seconds")
        rows = self.repo.get_review_timeouts(older_than_iso=cutoff)
        handled = 0
        for row in rows:
            item_hash = str(row["item_hash"])
            try:
                if self.on_timeout == "publish":
                    ok, msg_id = self._publish_row(row)
                    if ok:
                        self.repo.mark_posted(item_hash, tg_message_id=msg_id)
                        self._log(row, "timeout_publish")
                        log.info("Review timeout: %s auto-published", item_hash[:12])
                    else:
                        self.repo.mark_error(item_hash)
                        self._log(row, "timeout_publish_failed")
                        log.warning("Review timeout: %s publish failed", item_hash[:12])
                else:
                    self.repo.mark_rejected(item_hash)
                    self._log(row, "timeout_skip")
                    log.info("Review timeout: %s skipped", item_hash[:12])
                handled += 1
                self._remove_keyboard(row)
            except Exception:
                log.exception("Review timeout handling failed for %s", item_hash[:12])
        return handled

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @staticmethod
    def _get(row: Any, key: str):
        try:
            return row.get(key) if hasattr(row, "get") else row[key]
        except Exception:
            return None

    @staticmethod
    def _keyboard(item_id: int) -> dict:
        return {
            "inline_keyboard": [
                [{"text": "✅ Опублікувати", "callback_data": f"mod:approve:{item_id}"}],
                [{"text": "✏️ Перегенерувати", "callback_data": f"mod:regen:{item_id}"}],
                [{"text": "❌ Відхилити", "callback_data": f"mod:reject:{item_id}"}],
            ]
        }

    def _format_channel_text(self, row: Mapping[str, Any], post_text: str) -> str:
        return self.formatter.format_row(
            {
                "post_text": post_text,
                "link": self._get(row, "link"),
                "source": self._get(row, "source"),
                "category_hashtag": (self._get(row, "category_hashtag") or "").strip(),
                "embed_video_url": self._get(row, "embed_video_url"),
            }
        )

    def _format_preview(self, row: Mapping[str, Any], post_text: str) -> str:
        # The first line is bolded by the client formatter, so it reads as a header.
        score = self._get(row, "score")
        origin = self._get(row, "origin") or "world"
        meta = f"🛂 Модерація · score={float(score or 0.0):.2f} · {origin}"
        return meta + "\n\n" + self._format_channel_text(row, post_text)

    def _publish_row(self, row: Mapping[str, Any]) -> tuple[bool, Optional[int]]:
        post_text = (self._get(row, "post_text") or "").strip()
        if not post_text:
            return False, None
        formatted = self._format_channel_text(row, post_text)
        video_url, images = self.publisher.media_from_row(row)
        ok, msg_id = self.publisher.send_media(text=formatted, video_url=video_url, image_urls=images)
        return bool(ok), msg_id

    def _log(self, row: Mapping[str, Any], action: str) -> None:
        try:
            self.repo.add_moderation_log(
                item_hash=str(row["item_hash"]),
                action=action,
                llm_score=float(self._get(row, "score") or 0.0),
                origin=self._get(row, "origin"),
                topic_key=self._get(row, "topic_key"),
                category=self._get(row, "category_slug"),
            )
        except Exception:
            log.exception("Failed to write moderation_log for %s", row.get("item_hash"))

    def _remove_keyboard_from_callback(self, cq: Mapping[str, Any]) -> None:
        msg = cq.get("message") or {}
        chat = msg.get("chat") or {}
        chat_id = chat.get("id")
        message_id = msg.get("message_id")
        if chat_id is not None and message_id is not None:
            try:
                self.tg.edit_message_reply_markup(int(chat_id), int(message_id), reply_markup=None)
            except Exception:
                log.debug("Failed to remove keyboard from callback message", exc_info=True)

    def _remove_keyboard(self, row: Mapping[str, Any]) -> None:
        msg_id = self._get(row, "review_message_id")
        if msg_id is not None and self.tg.admin_chat_id is not None:
            try:
                self.tg.edit_message_reply_markup(int(self.tg.admin_chat_id), int(msg_id), reply_markup=None)
            except Exception:
                log.debug("Failed to remove keyboard from review message", exc_info=True)

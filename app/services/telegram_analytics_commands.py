from __future__ import annotations

import logging
from typing import Optional

log = logging.getLogger("services.analytics_commands")


class TelegramAnalyticsCommands:
    def __init__(self, repo, tg, analytics_service, prompt_manager=None, moderation=None, engagement=None):
        self.repo = repo
        self.tg = tg
        self.analytics = analytics_service
        self.prompt_manager = prompt_manager
        self.moderation = moderation
        self.engagement = engagement

    def _help_text(self) -> str:
        base = [
            "Доступні команди:",
            "/analytics — аналітика за сьогодні",
            "/export_news_items — CSV таблиці news_items",
            "/export_wrap_posts — CSV таблиці market_wrap_posts",
            "/analytics_chart — графік постів за 7 днів і за хештегами",
            "/moderation on|off — режим модерації перед публікацією",
            "/funnel — funnel-метрики пайплайна за сьогодні/вчора",
            "/prompt_list — список промтів",
            "/prompt_get <key> — показати поточний промт",
            "/prompt_set <key> <text> — оновити промт",
            "/prompt_set <key> — оновити промт текстом з reply",
            "/prompt_reset <key> — скинути override",
            "/help — список команд",
        ]
        return "\n".join(base)

    def poll_once(self) -> None:
        commands_on = bool(self.analytics.enabled and self.analytics.commands_enabled)
        # Moderation callbacks and reaction updates must be processed even when
        # analytics commands are off.
        if not commands_on and self.moderation is None and self.engagement is None:
            return

        allowed = None
        if self.engagement is not None and getattr(self.engagement, "enabled", False):
            from app.services.engagement import ALLOWED_UPDATES_WITH_REACTIONS

            allowed = ALLOWED_UPDATES_WITH_REACTIONS

        last_update_id = self.repo.get_bot_state_int("telegram_last_update_id", default=0)
        updates = self.tg.get_updates(offset=last_update_id + 1, timeout=0, allowed_updates=allowed)
        max_seen = last_update_id
        for upd in updates:
            upd_id = int(upd.get("update_id") or 0)
            max_seen = max(max_seen, upd_id)

            cq = upd.get("callback_query")
            if cq and self.moderation is not None:
                self._handle_callback_query(cq)
                continue

            mr = upd.get("message_reaction")
            if mr and self.engagement is not None:
                try:
                    self.engagement.record_reaction_update(mr)
                except Exception:
                    log.exception("Reaction update handling failed")
                continue

            if not commands_on:
                continue

            msg = upd.get("message") or upd.get("edited_message") or {}
            text = (msg.get("text") or "").strip()
            if not text.startswith("/"):
                continue

            chat = msg.get("chat") or {}
            chat_id = int(chat.get("id") or 0)
            if self.tg.admin_chat_id is not None and chat_id != int(self.tg.admin_chat_id):
                continue

            self._handle_command(msg=msg, chat_id=chat_id)

        if max_seen > last_update_id:
            self.repo.set_bot_state("telegram_last_update_id", str(max_seen))

    def _handle_callback_query(self, cq: dict) -> None:
        # Only the admin can press moderation buttons.
        from_user = cq.get("from") or {}
        msg = cq.get("message") or {}
        chat = msg.get("chat") or {}
        actor_ids = {int(from_user.get("id") or 0), int(chat.get("id") or 0)}
        if self.tg.admin_chat_id is not None and int(self.tg.admin_chat_id) not in actor_ids:
            log.warning("Callback from non-admin ignored: %s", actor_ids)
            return
        try:
            self.moderation.handle_callback(cq)
        except Exception:
            log.exception("Moderation callback handling failed")

    def _handle_command(self, msg: dict, chat_id: int) -> None:
        text = (msg.get("text") or "").strip()
        cmd = text.split()[0].split("@")[0].lower()
        args = text.split(maxsplit=2)

        if cmd in ("/help", "/start"):
            self.tg.send_message_with_id(self._help_text(), disable_preview=True, chat_id=chat_id)
            return

        if cmd == "/analytics":
            snapshot = self.analytics.build_snapshot_for_today()
            self.tg.send_message_with_id(self.analytics.render_snapshot_text(snapshot), disable_preview=True, chat_id=chat_id)
            return

        if cmd == "/export_news_items":
            path = self.analytics.export_news_items_csv()
            self.tg.send_document(path=str(path), caption="CSV: news_items", chat_id=chat_id)
            return

        if cmd == "/export_wrap_posts":
            path = self.analytics.export_wrap_posts_csv()
            self.tg.send_document(path=str(path), caption="CSV: market_wrap_posts", chat_id=chat_id)
            return

        if cmd == "/analytics_chart":
            png = self.analytics.build_weekly_chart_png(days=7)
            self.tg.send_photo_with_id(
                png,
                caption_text="Графік постів за 7 днів і розподіл за хештегами",
                disable_preview=True,
                filename="analytics_chart.png",
                chat_id=chat_id,
            )
            return

        if cmd == "/moderation":
            self._handle_moderation_toggle(chat_id=chat_id, args=args)
            return

        if cmd == "/funnel":
            self._handle_funnel(chat_id=chat_id)
            return

        if cmd == "/prompt_list":
            if not self.prompt_manager:
                self.tg.send_message_with_id("Менеджер промтів не підключений.", disable_preview=True, chat_id=chat_id)
                return
            self.tg.send_message_with_id("Промти:\n" + self.prompt_manager.describe(), disable_preview=True, chat_id=chat_id)
            return

        if cmd == "/prompt_get":
            key = args[1].strip() if len(args) > 1 else ""
            self._handle_prompt_get(chat_id=chat_id, key=key)
            return

        if cmd == "/prompt_set":
            self._handle_prompt_set(chat_id=chat_id, msg=msg)
            return

        if cmd == "/prompt_reset":
            key = args[1].strip() if len(args) > 1 else ""
            self._handle_prompt_reset(chat_id=chat_id, key=key)
            return

        self.tg.send_message_with_id("Невідома команда. Надішли /help", disable_preview=True, chat_id=chat_id)

    # Pipeline stage order for readable /funnel output
    _FUNNEL_STAGES = [
        "fetched", "new", "embedded", "dups", "candidates", "scored",
        "approved", "posted", "wraps", "expired", "review_timeouts", "retried", "failed",
    ]

    def _handle_funnel(self, chat_id: int) -> None:
        sums = self.repo.get_funnel_sums(days=2)
        if not sums:
            self.tg.send_message_with_id("Funnel-метрик ще немає.", disable_preview=True, chat_id=chat_id)
            return
        lines = ["Funnel пайплайна (UTC):"]
        for day in sorted(sums.keys(), reverse=True):
            stages = sums[day]
            parts = [f"{s}={stages[s]}" for s in self._FUNNEL_STAGES if s in stages]
            parts += [f"{s}={c}" for s, c in stages.items() if s not in self._FUNNEL_STAGES]
            lines.append(f"\n{day}:")
            lines.append("  " + " → ".join(parts) if parts else "  —")
        self.tg.send_message_with_id("\n".join(lines), disable_preview=True, chat_id=chat_id)

    def _handle_moderation_toggle(self, chat_id: int, args: list) -> None:
        if not self.moderation:
            self.tg.send_message_with_id("Модерація не підключена.", disable_preview=True, chat_id=chat_id)
            return
        arg = (args[1].strip().lower() if len(args) > 1 else "")
        if arg in ("on", "off"):
            self.moderation.set_enabled(arg == "on")
        elif arg:
            self.tg.send_message_with_id("Використання: /moderation on|off", disable_preview=True, chat_id=chat_id)
            return
        state = "увімкнена ✅" if self.moderation.is_enabled() else "вимкнена ⏸"
        self.tg.send_message_with_id(f"Модерація {state}.", disable_preview=True, chat_id=chat_id)

    def _handle_prompt_get(self, chat_id: int, key: str) -> None:
        if not self.prompt_manager:
            self.tg.send_message_with_id("Менеджер промтів не підключений.", disable_preview=True, chat_id=chat_id)
            return
        if key not in self.prompt_manager.list_keys():
            self.tg.send_message_with_id("Невірний key. Надішли /prompt_list", disable_preview=True, chat_id=chat_id)
            return
        value = self.prompt_manager.get(key)
        self.tg.send_message_with_id(f"Промт {key}:\n\n{value or '[empty]'}", disable_preview=True, chat_id=chat_id)

    def _handle_prompt_set(self, chat_id: int, msg: dict) -> None:
        if not self.prompt_manager:
            self.tg.send_message_with_id("Менеджер промтів не підключений.", disable_preview=True, chat_id=chat_id)
            return

        text = (msg.get("text") or "").strip()
        parts = text.split(maxsplit=2)
        key = parts[1].strip() if len(parts) > 1 else ""
        value = parts[2] if len(parts) > 2 else ""

        if not value:
            reply = msg.get("reply_to_message") or {}
            value = (reply.get("text") or reply.get("caption") or "").strip()

        if key not in self.prompt_manager.list_keys():
            self.tg.send_message_with_id("Невірний key. Надішли /prompt_list", disable_preview=True, chat_id=chat_id)
            return
        if not value.strip():
            self.tg.send_message_with_id(
                "Передай текст так: /prompt_set <key> <text> або reply на повідомлення з промтом.",
                disable_preview=True,
                chat_id=chat_id,
            )
            return

        self.prompt_manager.set(key, value)
        self.tg.send_message_with_id(f"Оновлено промт {key}.", disable_preview=True, chat_id=chat_id)

    def _handle_prompt_reset(self, chat_id: int, key: str) -> None:
        if not self.prompt_manager:
            self.tg.send_message_with_id("Менеджер промтів не підключений.", disable_preview=True, chat_id=chat_id)
            return
        if key not in self.prompt_manager.list_keys():
            self.tg.send_message_with_id("Невірний key. Надішли /prompt_list", disable_preview=True, chat_id=chat_id)
            return
        self.prompt_manager.reset(key)
        self.tg.send_message_with_id(f"Скинуто override для {key}.", disable_preview=True, chat_id=chat_id)

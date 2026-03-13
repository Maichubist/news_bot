from __future__ import annotations

import logging

log = logging.getLogger('services.analytics_commands')


class TelegramAnalyticsCommands:
    def __init__(self, repo, tg, analytics_service):
        self.repo = repo
        self.tg = tg
        self.analytics = analytics_service

    def _help_text(self) -> str:
        return (
            'Доступні команди:\n'
            '/analytics — аналітика за сьогодні\n'
            '/export_news_items — CSV таблиці news_items\n'
            '/export_wrap_posts — CSV таблиці market_wrap_posts\n'
            '/analytics_chart — графік постів за 7 днів і за хештегами\n'
            '/help — список команд'
        )

    def poll_once(self) -> None:
        if not self.analytics.enabled or not self.analytics.commands_enabled:
            return
        last_update_id = self.repo.get_bot_state_int('telegram_last_update_id', default=0)
        updates = self.tg.get_updates(offset=last_update_id + 1, timeout=0)
        max_seen = last_update_id
        for upd in updates:
            upd_id = int(upd.get('update_id') or 0)
            max_seen = max(max_seen, upd_id)
            msg = upd.get('message') or upd.get('edited_message') or {}
            text = (msg.get('text') or '').strip()
            if not text.startswith('/'):
                continue
            chat = msg.get('chat') or {}
            chat_id = int(chat.get('id') or 0)
            if self.tg.admin_chat_id is not None and chat_id != int(self.tg.admin_chat_id):
                continue
            cmd = text.split()[0].split('@')[0].lower()
            self._handle_command(cmd=cmd, chat_id=chat_id)
        if max_seen > last_update_id:
            self.repo.set_bot_state('telegram_last_update_id', str(max_seen))

    def _handle_command(self, cmd: str, chat_id: int) -> None:
        if cmd in ('/help', '/start'):
            self.tg.send_message_with_id(self._help_text(), disable_preview=True, chat_id=chat_id)
            return

        if cmd == '/analytics':
            snapshot = self.analytics.build_snapshot_for_today()
            self.tg.send_message_with_id(self.analytics.render_snapshot_text(snapshot), disable_preview=True, chat_id=chat_id)
            return

        if cmd == '/export_news_items':
            path = self.analytics.export_news_items_csv()
            self.tg.send_document(path=str(path), caption='CSV: news_items', chat_id=chat_id)
            return

        if cmd == '/export_wrap_posts':
            path = self.analytics.export_wrap_posts_csv()
            self.tg.send_document(path=str(path), caption='CSV: market_wrap_posts', chat_id=chat_id)
            return

        if cmd == '/analytics_chart':
            png = self.analytics.build_weekly_chart_png(days=7)
            self.tg.send_photo_with_id(png, caption_text='Графік постів за 7 днів і розподіл за хештегами', disable_preview=True, filename='analytics_chart.png', chat_id=chat_id)
            return

        self.tg.send_message_with_id('Невідома команда. Надішли /help', disable_preview=True, chat_id=chat_id)

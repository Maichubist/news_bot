from __future__ import annotations

import io
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional
from zoneinfo import ZoneInfo

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

log = logging.getLogger('services.analytics')


@dataclass(frozen=True)
class AnalyticsSnapshot:
    day_local: str
    posts_today: int
    news_scored_today: int
    total_posts_all_time: int


class AnalyticsService:
    def __init__(self, cfg, repo, tg):
        self.cfg = cfg
        self.repo = repo
        self.tg = tg
        self.tz_name = getattr(cfg.analytics, 'timezone', 'Europe/Zaporozhye')
        self.tz = ZoneInfo(self.tz_name)
        self.report_hour_local = int(getattr(cfg.analytics, 'report_hour_local', 22) or 22)
        self.enabled = bool(getattr(cfg.analytics, 'enabled', True))
        self.daily_enabled = bool(getattr(cfg.analytics, 'daily_report_enabled', True))
        self.commands_enabled = bool(getattr(cfg.analytics, 'commands_enabled', True))
        self.wrap_hashtag_by_name: Dict[str, str] = {
            str(getattr(rule, 'key', '') or ''): self._hashtag_for_slug(getattr(rule, 'hashtag_slug', '') or 'other')
            for rule in (getattr(cfg.posting, 'wrap_rules', []) or [])
        }

    def _hashtag_for_slug(self, slug: str) -> str:
        for c in getattr(self.cfg, 'categories', []) or []:
            if (getattr(c, 'slug', '') or '').strip() == slug:
                return (getattr(c, 'hashtag', '') or '').strip()
        return '#інше'

    def now_local(self) -> datetime:
        return datetime.now(self.tz)

    def build_snapshot_for_today(self) -> AnalyticsSnapshot:
        now = self.now_local()
        day_local = now.date().isoformat()
        stats = self.repo.get_analytics_snapshot(day_local=day_local, tz_name=self.tz_name)
        return AnalyticsSnapshot(
            day_local=day_local,
            posts_today=int(stats.get('posts_today', 0)),
            news_scored_today=int(stats.get('news_scored_today', 0)),
            total_posts_all_time=int(stats.get('total_posts_all_time', 0)),
        )

    def render_snapshot_text(self, snapshot: AnalyticsSnapshot) -> str:
        return (
            f"Аналітика за {snapshot.day_local}\n\n"
            f"Запощено постів за день: {snapshot.posts_today}\n"
            f"Оцінено новин за день: {snapshot.news_scored_today}\n"
            f"Сукупно зроблено постів: {snapshot.total_posts_all_time}"
        )

    def maybe_send_daily_report(self) -> None:
        if not self.enabled or not self.daily_enabled:
            return
        now = self.now_local()
        if now.hour != self.report_hour_local:
            return
        day_local = now.date().isoformat()
        if self.repo.analytics_report_exists(day_local):
            return
        snapshot = self.build_snapshot_for_today()
        text = self.render_snapshot_text(snapshot)
        ok, _ = self.tg.send_message_with_id(text, disable_preview=True, chat_id=self.tg.admin_chat_id)
        if ok:
            self.repo.save_analytics_report(day_local=day_local, post_text=text)
            log.info('Daily analytics report sent for %s', day_local)

    def export_news_items_csv(self) -> Path:
        out = Path('data') / 'exports'
        out.mkdir(parents=True, exist_ok=True)
        path = out / f"news_items_{self.now_local().strftime('%Y%m%d_%H%M%S')}.csv"
        self.repo.export_table_to_csv('news_items', str(path))
        return path

    def export_wrap_posts_csv(self) -> Path:
        out = Path('data') / 'exports'
        out.mkdir(parents=True, exist_ok=True)
        path = out / f"market_wrap_posts_{self.now_local().strftime('%Y%m%d_%H%M%S')}.csv"
        self.repo.export_table_to_csv('market_wrap_posts', str(path))
        return path

    def build_weekly_chart_png(self, days: int = 7) -> bytes:
        series = self.repo.get_post_counts_by_day(days=days, tz_name=self.tz_name)
        hashtag_counts = self.repo.get_hashtag_post_counts(days=days, tz_name=self.tz_name, wrap_hashtag_by_name=self.wrap_hashtag_by_name)

        fig, axes = plt.subplots(2, 1, figsize=(12, 9))

        labels = [x['day'] for x in series]
        values = [x['posts'] for x in series]
        axes[0].bar(labels, values)
        axes[0].set_title(f'Кількість постів за останні {days} днів')
        axes[0].set_ylabel('Пости')
        axes[0].tick_params(axis='x', rotation=20)

        h_labels = [x['hashtag'] for x in hashtag_counts] or ['немає даних']
        h_values = [x['posts'] for x in hashtag_counts] or [0]
        axes[1].bar(h_labels, h_values)
        axes[1].set_title(f'Пости за хештегами за останні {days} днів')
        axes[1].set_ylabel('Пости')
        axes[1].tick_params(axis='x', rotation=20)

        fig.tight_layout()
        buf = io.BytesIO()
        fig.savefig(buf, format='png', dpi=150, bbox_inches='tight')
        plt.close(fig)
        return buf.getvalue()

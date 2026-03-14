from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from app.config import AppConfig
from app.dedup.embeddings import OpenAIEmbeddingClient
from app.dedup.event_matcher import EventMatcher
from app.dedup.exact import ExactDeduper
from app.dedup.lexical import LexicalDeduper
from app.dedup.semantic import SemanticDeduper
from app.http import RequestsSession, build_verify_option
from app.rss.fetcher import RssFetcher
from app.services.analytics_service import AnalyticsService
from app.services.news_pipeline import NewsPipeline
from app.services.openai_daily_digest import OpenAIDailyDigestMaker
from app.services.openai_market_wrap import OpenAIMarketWrapMaker
from app.services.openai_postmaker import OpenAINewsPostMaker
from app.services.prompt_manager import PromptManager
from app.services.telegram_analytics_commands import TelegramAnalyticsCommands
from app.storage.sqlite_repo import SqliteNewsRepository
from app.telegram.client import TelegramClient
from app.telegram.formatter import PostFormatter
from app.text.profile import TextProfileBuilder


@dataclass
class AppRuntime:
    cfg: AppConfig
    http: RequestsSession
    repo: SqliteNewsRepository
    tg: TelegramClient
    pipeline: NewsPipeline
    analytics_service: AnalyticsService
    command_service_factory: Callable[[], TelegramAnalyticsCommands]
    prompt_manager: PromptManager



def build_runtime(config_path: str = "config.yaml") -> AppRuntime:
    cfg = AppConfig.load(config_path)
    verify_opt = build_verify_option(cfg.network.verify)
    http = RequestsSession(timeout_sec=cfg.network.timeout_sec, verify_opt=verify_opt)

    def repo_factory() -> SqliteNewsRepository:
        repo = SqliteNewsRepository(db_path=cfg.db.path)
        repo.init_db()
        repo.ensure_categories(cfg.categories)
        return repo

    repo = repo_factory()

    prompt_manager = PromptManager(
        repo_factory=repo_factory,
        defaults={
            "post_prompt": cfg.llm.post_prompt,
            "digest_prompt": cfg.llm.digest_prompt,
            "wrap_prompt": cfg.llm.wrap_prompt,
            "market_wrap_prompt": cfg.llm.market_wrap_prompt,
            "geopolitical_wrap_prompt": cfg.llm.geopolitical_wrap_prompt,
            "tech_wrap_prompt": cfg.llm.tech_wrap_prompt,
        },
    )

    rss = RssFetcher(http=http)
    try:
        rss.set_global_filters(
            deny_title=list(cfg.filters.deny_title_regex),
            deny_url=list(cfg.filters.deny_url_regex),
            deny_summary=list(cfg.filters.deny_summary_regex),
        )
    except Exception:
        pass

    exact = ExactDeduper()
    embedder = OpenAIEmbeddingClient(http=http, api_key=cfg.openai.api_key, model=cfg.openai.model)
    semantic = SemanticDeduper(repo=repo, window_hours=cfg.embeddings.window_hours, threshold=cfg.embeddings.threshold)
    profile_builder = TextProfileBuilder(max_keywords=cfg.text_processing.max_keywords)
    lexical = LexicalDeduper(max_candidates=cfg.text_processing.max_candidates)
    event_matcher = EventMatcher(
        duplicate_combined_threshold=cfg.text_processing.duplicate_combined_threshold,
        duplicate_semantic_threshold=cfg.text_processing.duplicate_semantic_threshold,
        duplicate_lexical_threshold=cfg.text_processing.duplicate_lexical_threshold,
        same_event_combined_threshold=cfg.text_processing.same_event_combined_threshold,
        same_event_semantic_threshold=cfg.text_processing.same_event_semantic_threshold,
        same_event_lexical_threshold=cfg.text_processing.same_event_lexical_threshold,
    )
    tg = TelegramClient(http=http, token=cfg.telegram.token, chat_id=cfg.telegram.chat_id, admin_chat_id=cfg.telegram.admin_chat_id)
    fmt = PostFormatter(include_source=cfg.posting.include_source_name)

    postmaker = OpenAINewsPostMaker(
        http=http,
        api_key=cfg.openai.api_key,
        model=cfg.llm.post_model,
        prompt=cfg.llm.post_prompt,
        categories=[{"slug": c.slug, "title": c.title} for c in cfg.categories],
        prompt_provider=prompt_manager.get,
    )
    digestmaker = OpenAIDailyDigestMaker(
        http=http,
        api_key=cfg.openai.api_key,
        model=cfg.llm.digest_model,
        prompt=cfg.llm.digest_prompt,
        prompt_provider=prompt_manager.get,
    )
    wrapmaker = OpenAIMarketWrapMaker(
        http=http,
        api_key=cfg.openai.api_key,
        model=cfg.llm.wrap_model,
        default_prompt=cfg.llm.wrap_prompt,
        prompt_provider=prompt_manager.get,
    )

    analytics_service = AnalyticsService(cfg=cfg, repo=repo, tg=tg)

    def command_service_factory() -> TelegramAnalyticsCommands:
        local_repo = repo_factory()
        local_analytics = AnalyticsService(cfg=cfg, repo=local_repo, tg=tg)
        return TelegramAnalyticsCommands(
            repo=local_repo,
            tg=tg,
            analytics_service=local_analytics,
            prompt_manager=prompt_manager,
        )

    pipeline = NewsPipeline(
        cfg=cfg,
        repo=repo,
        rss=rss,
        exact=exact,
        embedder=embedder,
        semantic=semantic,
        profile_builder=profile_builder,
        lexical=lexical,
        event_matcher=event_matcher,
        tg=tg,
        formatter=fmt,
        postmaker=postmaker,
        digestmaker=digestmaker,
        wrapmaker=wrapmaker,
        prompt_manager=prompt_manager,
    )

    return AppRuntime(
        cfg=cfg,
        http=http,
        repo=repo,
        tg=tg,
        pipeline=pipeline,
        analytics_service=analytics_service,
        command_service_factory=command_service_factory,
        prompt_manager=prompt_manager,
    )

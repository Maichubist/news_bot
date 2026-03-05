import time
import logging

from dotenv import load_dotenv

from app.config import AppConfig
from app.logging_setup import setup_logging
from app.http import build_verify_option, RequestsSession

from app.rss.fetcher import RssFetcher
from app.dedup.exact import ExactDeduper
from app.dedup.embeddings import OpenAIEmbeddingClient
from app.dedup.semantic import SemanticDeduper
from app.storage.sqlite_repo import SqliteNewsRepository
from app.telegram.client import TelegramClient
from app.telegram.formatter import PostFormatter
from app.services.news_pipeline import NewsPipeline
# from app.translate.openai_ua import OpenAIUaTranslator
from app.services.openai_postmaker import OpenAINewsPostMaker
from app.services.openai_daily_digest import OpenAIDailyDigestMaker

log = logging.getLogger("main")

def main():

    # Load secrets from .env (TELEGRAM_TOKEN, OPENAI_API_KEY, etc.)
    load_dotenv()

    cfg = AppConfig.load("config.yaml")

    setup_logging(cfg.app.log_level)

    verify_opt = build_verify_option(cfg.network.verify)
    translator = None

    http = RequestsSession(timeout_sec=cfg.network.timeout_sec, verify_opt=verify_opt)

    repo = SqliteNewsRepository(db_path=cfg.db.path)
    repo.init_db()
    repo.ensure_categories(cfg.categories)

    rss = RssFetcher(http=http)
    # Global RSS filters (ads/promos/video/newsletters/etc.)
    try:
        rss.set_global_filters(
            deny_title=list(cfg.filters.deny_title_regex),
            deny_url=list(cfg.filters.deny_url_regex),
            deny_summary=list(cfg.filters.deny_summary_regex),
        )
    except Exception:
        # Older configs may not have filters section
        pass
    exact = ExactDeduper()
    embedder = OpenAIEmbeddingClient(
        http=http,
        api_key=cfg.openai.api_key,
        model=cfg.openai.model,
    )
    semantic = SemanticDeduper(
        repo=repo,
        window_hours=cfg.embeddings.window_hours,
        threshold=cfg.embeddings.threshold,
    )

    tg = TelegramClient(
        http=http,
        token=cfg.telegram.token,
        chat_id=cfg.telegram.chat_id,
    )
    fmt = PostFormatter(include_source=cfg.posting.include_source_name)
    # if cfg.translate.enabled:
    #     translator = OpenAIUaTranslator(http=http, api_key=cfg.openai.api_key, model=cfg.translate.model)
    
    postmaker = OpenAINewsPostMaker(
        http=http,
        api_key=cfg.openai.api_key,
        model=cfg.llm.post_model,
        prompt=cfg.llm.post_prompt,
        categories=[{"slug": c.slug, "title": c.title} for c in cfg.categories],
    )
    digestmaker = OpenAIDailyDigestMaker(
        http=http,
        api_key=cfg.openai.api_key,
        model=cfg.llm.digest_model,
        prompt=cfg.llm.digest_prompt,
    )

    pipeline = NewsPipeline(
        cfg=cfg,
        repo=repo,
        rss=rss,
        exact=exact,
        embedder=embedder,
        semantic=semantic,
        tg=tg,
        formatter=fmt,
        # translator=translator,
        postmaker=postmaker,
        digestmaker=digestmaker
    )

    every = cfg.monitor.every_seconds

    while True:
        t0 = time.time()
        try:
            pipeline.run_once()
        except Exception as ex:
            log.exception("Run failed: %s", ex)

        dt = time.time() - t0
        sleep_for = max(1, every - dt)
        time.sleep(sleep_for)

if __name__ == "__main__":
    main()
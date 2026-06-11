from __future__ import annotations

from dataclasses import dataclass
from typing import Any
import os
import yaml


def _req(d: dict[str, Any], key: str, section: str = "") -> Any:
    if key not in d or d[key] in (None, ""):
        prefix = f"{section}." if section else ""
        raise ValueError(f"Missing required config key: {prefix}{key}")
    return d[key]


def _as_dict(v: Any, name: str) -> dict[str, Any]:
    if not isinstance(v, dict):
        raise ValueError(f"Config section '{name}' must be a mapping/object")
    return v


def _as_list(v: Any, name: str) -> list[Any]:
    if not isinstance(v, list):
        raise ValueError(f"Config key '{name}' must be a list")
    return v


def _as_str_list(v: Any) -> list[str]:
    if v is None:
        return []
    if isinstance(v, str):
        s = v.strip()
        return [s] if s else []
    if isinstance(v, list):
        out: list[str] = []
        for x in v:
            s = str(x).strip()
            if s:
                out.append(s)
        return out
    raise ValueError("Expected string or list of strings")


def _as_bool(v: Any, name: str) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        s = v.strip().lower()
        if s in {"true", "1", "yes", "y", "on"}:
            return True
        if s in {"false", "0", "no", "n", "off"}:
            return False
    if isinstance(v, (int, float)):
        return bool(v)
    raise ValueError(f"Config key '{name}' must be a boolean")


def _as_int(v: Any, name: str) -> int:
    try:
        return int(v)
    except Exception as ex:
        raise ValueError(f"Config key '{name}' must be an integer, got: {v!r}") from ex


def _as_float(v: Any, name: str) -> float:
    try:
        return float(v)
    except Exception as ex:
        raise ValueError(f"Config key '{name}' must be a float, got: {v!r}") from ex


@dataclass(frozen=True)
class TelegramCfg:
    token: str
    chat_id: int
    admin_chat_id: int | None


@dataclass(frozen=True)
class OpenAICfg:
    api_key: str
    model: str


@dataclass(frozen=True)
class CategoryCfg:
    slug: str
    title: str
    hashtag: str


@dataclass(frozen=True)
class SourceCfg:
    name: str
    url: str
    deny_title_regex: list[str]
    deny_url_regex: list[str]
    origin: str = "world"  # "ua" | "world"


@dataclass(frozen=True)
class FiltersCfg:
    deny_title_regex: list[str]
    deny_url_regex: list[str]
    deny_summary_regex: list[str]


@dataclass(frozen=True)
class DbCfg:
    path: str
    keep_days: int


@dataclass(frozen=True)
class WrapRuleCfg:
    key: str
    title: str
    categories: list[str]
    min_items: int
    lookback_hours: int
    cooldown_minutes: int
    min_sources: int
    source_label: str
    hashtag_slug: str
    prompt_template_key: str


@dataclass(frozen=True)
class PostingCfg:
    max_posts_per_run: int
    only_last_hours: int
    include_source_name: bool
    cluster_wait_minutes: int
    breaking_sources_threshold: int
    wrap_rules: list[WrapRuleCfg]


@dataclass(frozen=True)
class TopicQuotaCfg:
    window1_hours: int
    max_posts_window1: int
    window2_hours: int
    max_posts_window2: int


@dataclass(frozen=True)
class TopicSaturationCfg:
    lookback_hours: int
    max_topic_share: float
    min_posts: int


@dataclass(frozen=True)
class EditorialDelayCfg:
    min_minutes: int
    max_minutes: int


@dataclass(frozen=True)
class SourceWeightingCfg:
    high_trust_sources: list[str]


@dataclass(frozen=True)
class OriginBalanceCfg:
    enabled: bool
    lookback_hours: int
    max_origin_share: float
    min_posts: int
    ua_relevance_boost: float


@dataclass(frozen=True)
class EditorialCfg:
    min_post_score: float
    topic_quota: TopicQuotaCfg
    topic_saturation: TopicSaturationCfg
    delay_non_breaking: EditorialDelayCfg
    source_weighting: SourceWeightingCfg
    origin_balance: OriginBalanceCfg


@dataclass(frozen=True)
class ImagesCfg:
    og_fetch: bool


@dataclass(frozen=True)
class ArticlesCfg:
    fetch_full_text: bool
    max_chars: int


@dataclass(frozen=True)
class MediaCfg:
    enable_video: bool
    max_image_bytes: int
    max_video_bytes: int


@dataclass(frozen=True)
class LlmCfg:
    post_model: str
    digest_model: str
    wrap_model: str
    post_prompt: str
    wrap_prompt: str
    digest_prompt: str
    market_wrap_prompt: str
    geopolitical_wrap_prompt: str
    tech_wrap_prompt: str


@dataclass(frozen=True)
class NetworkCfg:
    timeout_sec: int
    verify: Any


@dataclass(frozen=True)
class EmbeddingsCfg:
    window_hours: int
    threshold: float
    require_good_summary: bool


@dataclass(frozen=True)
class AppCfg:
    sleep_between_posts_sec: float
    log_level: str


@dataclass(frozen=True)
class MonitorCfg:
    every_seconds: int
    command_poll_seconds: int


@dataclass(frozen=True)
class TranslateCfg:
    enabled: bool
    model: str
    max_chars_summary: int


@dataclass(frozen=True)
class TextProcessingCfg:
    enabled: bool
    matching_window_hours: int
    max_candidates: int
    max_keywords: int
    duplicate_combined_threshold: float
    duplicate_semantic_threshold: float
    duplicate_lexical_threshold: float
    same_event_combined_threshold: float
    same_event_semantic_threshold: float
    same_event_lexical_threshold: float


@dataclass(frozen=True)
class AnalyticsCfg:
    enabled: bool
    daily_report_enabled: bool
    commands_enabled: bool
    report_hour_local: int
    timezone: str


@dataclass(frozen=True)
class ModerationCfg:
    # When the section is absent the feature stays off so old configs behave as before.
    enabled: bool = False
    timeout_minutes: int = 45
    on_timeout: str = "skip"  # "skip" | "publish"


@dataclass(frozen=True)
class RankingCfg:
    # Comparative batch ranking instead of an absolute score threshold.
    # Off by default: the legacy min_post_score path keeps working untouched.
    enabled: bool = False
    cycle_minutes: int = 25
    window_hours: int = 3
    model: str = "gpt-4o"
    max_picks: int = 2
    max_age_hours: int = 8


@dataclass(frozen=True)
class ClusterWrapCfg:
    # Story-wrap thresholds used when clustering is on (replaces category rules).
    min_items: int = 3
    min_sources: int = 2
    lookback_hours: int = 6
    cooldown_minutes: int = 90


@dataclass(frozen=True)
class ClusteringCfg:
    # Embedding-based event identity instead of the LLM event_key.
    # Off by default: event_key matching and category wraps keep working.
    enabled: bool = False
    threshold: float = 0.80
    window_hours: int = 48
    wrap: ClusterWrapCfg = ClusterWrapCfg()


@dataclass(frozen=True)
class EngagementCfg:
    enabled: bool = True
    poll_hours: int = 6
    lookback_hours: int = 72
    max_posts: int = 50
    # Optional MTProto path (Telethon) for views/forwards; requires env credentials
    # and the telethon package. Never a hard dependency.
    mtproto_enabled: bool = False


@dataclass(frozen=True)
class AppConfig:
    telegram: TelegramCfg
    openai: OpenAICfg
    categories: list[CategoryCfg]
    sources: list[SourceCfg]
    db: DbCfg
    posting: PostingCfg
    network: NetworkCfg
    embeddings: EmbeddingsCfg
    app: AppCfg
    monitor: MonitorCfg
    translate: TranslateCfg
    images: ImagesCfg
    llm: LlmCfg
    filters: FiltersCfg
    analytics: AnalyticsCfg
    text_processing: TextProcessingCfg
    editorial: EditorialCfg
    articles: ArticlesCfg
    media: MediaCfg
    moderation: ModerationCfg
    engagement: EngagementCfg
    ranking: RankingCfg
    clustering: ClusteringCfg

    @staticmethod
    def load(path: str = "config.yaml") -> "AppConfig":
        with open(path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}

        tg = _as_dict(_req(raw, "telegram"), "telegram")
        oa = _as_dict(_req(raw, "openai"), "openai")
        db = _as_dict(_req(raw, "db"), "db")
        posting = _as_dict(_req(raw, "posting"), "posting")
        network = _as_dict(_req(raw, "network"), "network")
        emb = _as_dict(_req(raw, "embeddings"), "embeddings")
        app = _as_dict(_req(raw, "app"), "app")
        monitor = _as_dict(_req(raw, "monitor"), "monitor")
        translate = _as_dict(_req(raw, "translate"), "translate")
        images = _as_dict(_req(raw, "images"), "images")
        llm = _as_dict(_req(raw, "llm"), "llm")
        filters = _as_dict(_req(raw, "filters"), "filters")
        analytics = _as_dict(_req(raw, "analytics"), "analytics")
        text_processing = _as_dict(raw.get("text_processing") or {}, "text_processing")
        editorial = _as_dict(raw.get("editorial") or {}, "editorial")

        tg_token = os.getenv("TELEGRAM_TOKEN") or os.getenv("TG_TOKEN")
        oa_key = os.getenv("OPENAI_API_KEY")

        if not tg_token:
            raise ValueError("Missing TELEGRAM_TOKEN in environment/.env")
        if not oa_key:
            raise ValueError("Missing OPENAI_API_KEY in environment/.env")

        admin_chat_env = os.getenv("TELEGRAM_ADMIN_CHAT_ID")
        admin_chat_id = (
            _as_int(admin_chat_env, "TELEGRAM_ADMIN_CHAT_ID")
            if admin_chat_env not in (None, "")
            else (
                _as_int(tg["admin_chat_id"], "telegram.admin_chat_id")
                if tg.get("admin_chat_id") not in (None, "")
                else None
            )
        )

        categories_raw = _as_list(_req(raw, "categories"), "categories")
        categories_cfg: list[CategoryCfg] = []
        for idx, item in enumerate(categories_raw):
            c = _as_dict(item, f"categories[{idx}]")
            categories_cfg.append(
                CategoryCfg(
                    slug=str(_req(c, "slug", f"categories[{idx}]")).strip(),
                    title=str(_req(c, "title", f"categories[{idx}]")).strip(),
                    hashtag=str(_req(c, "hashtag", f"categories[{idx}]")).strip(),
                )
            )

        if not categories_cfg:
            raise ValueError("Config key 'categories' must be a non-empty list")

        sources_raw = _as_list(_req(raw, "sources"), "sources")
        sources_cfg: list[SourceCfg] = []
        for idx, item in enumerate(sources_raw):
            s = _as_dict(item, f"sources[{idx}]")
            origin = str(s.get("origin") or "world").strip().lower()
            if origin not in ("ua", "world"):
                raise ValueError(f"sources[{idx}].origin must be 'ua' or 'world', got: {origin!r}")
            sources_cfg.append(
                SourceCfg(
                    name=str(_req(s, "name", f"sources[{idx}]")).strip(),
                    url=str(_req(s, "url", f"sources[{idx}]")).strip(),
                    deny_title_regex=_as_str_list(s.get("deny_title_regex")),
                    deny_url_regex=_as_str_list(s.get("deny_url_regex")),
                    origin=origin,
                )
            )

        if not sources_cfg:
            raise ValueError("Config key 'sources' must be a non-empty list")

        llm_cfg = LlmCfg(
            post_model=str(_req(llm, "post_model", "llm")).strip(),
            digest_model=str(_req(llm, "digest_model", "llm")).strip(),
            wrap_model=str(_req(llm, "wrap_model", "llm")).strip(),
            post_prompt=str(_req(llm, "post_prompt", "llm")),
            wrap_prompt=str(_req(llm, "wrap_prompt", "llm")),
            digest_prompt=str(_req(llm, "digest_prompt", "llm")),
            market_wrap_prompt=str(llm.get("market_wrap_prompt", "")),
            geopolitical_wrap_prompt=str(llm.get("geopolitical_wrap_prompt", "")),
            tech_wrap_prompt=str(llm.get("tech_wrap_prompt", "")),
        )

        prompt_key_by_wrap: dict[str, str] = {
            "economy_wrap": "market_wrap_prompt",
            "economy": "market_wrap_prompt",
            "market_wrap": "market_wrap_prompt",
            "market": "market_wrap_prompt",
            "geopolitics_wrap": "geopolitical_wrap_prompt",
            "geopolitics": "geopolitical_wrap_prompt",
            "geopolitical_wrap": "geopolitical_wrap_prompt",
            "war_wrap": "geopolitical_wrap_prompt",
            "politics_wrap": "geopolitical_wrap_prompt",
            "technology_wrap": "tech_wrap_prompt",
            "technology": "tech_wrap_prompt",
            "tech_wrap": "tech_wrap_prompt",
            "tech": "tech_wrap_prompt",
            "science_wrap": "tech_wrap_prompt",
        }


        editorial_topic_quota = _as_dict(editorial.get("topic_quota") or {}, "editorial.topic_quota")
        editorial_topic_saturation = _as_dict(editorial.get("topic_saturation") or {}, "editorial.topic_saturation")
        editorial_delay = _as_dict(editorial.get("delay_non_breaking") or {}, "editorial.delay_non_breaking")
        editorial_sources = _as_dict(editorial.get("source_weighting") or {}, "editorial.source_weighting")

        wrap_rules_raw = _as_list(_req(posting, "wrap_rules", "posting"), "posting.wrap_rules")
        wrap_rules_cfg: list[WrapRuleCfg] = []
        for idx, item in enumerate(wrap_rules_raw):
            wr = _as_dict(item, f"posting.wrap_rules[{idx}]")
            key = str(wr.get("key") or wr.get("name") or "").strip()
            if not key:
                raise ValueError(f"Missing required config key: posting.wrap_rules[{idx}].key")

            prompt_template_key = str(wr.get("prompt_template_key") or "").strip()
            if not prompt_template_key:
                prompt_template_key = prompt_key_by_wrap.get(key.lower(), "wrap_prompt")

            wrap_rules_cfg.append(
                WrapRuleCfg(
                    key=key,
                    title=str(wr.get("title") or key).strip(),
                    categories=_as_str_list(_req(wr, "categories", f"posting.wrap_rules[{idx}]")),
                    min_items=_as_int(_req(wr, "min_items", f"posting.wrap_rules[{idx}]"), f"posting.wrap_rules[{idx}].min_items"),
                    lookback_hours=_as_int(_req(wr, "lookback_hours", f"posting.wrap_rules[{idx}]"), f"posting.wrap_rules[{idx}].lookback_hours"),
                    cooldown_minutes=_as_int(_req(wr, "cooldown_minutes", f"posting.wrap_rules[{idx}]"), f"posting.wrap_rules[{idx}].cooldown_minutes"),
                    min_sources=_as_int(_req(wr, "min_sources", f"posting.wrap_rules[{idx}]"), f"posting.wrap_rules[{idx}].min_sources"),
                    source_label=str(wr.get("source_label") or str(wr.get("title") or key)).strip(),
                    hashtag_slug=str(_req(wr, "hashtag_slug", f"posting.wrap_rules[{idx}]")).strip(),
                    prompt_template_key=prompt_template_key,
                )
            )

        return AppConfig(
            telegram=TelegramCfg(
                token=str(tg_token),
                chat_id=_as_int(_req(tg, "chat_id", "telegram"), "telegram.chat_id"),
                admin_chat_id=admin_chat_id,
            ),
            openai=OpenAICfg(
                api_key=str(oa_key),
                model=str(_req(oa, "model", "openai")).strip(),
            ),
            categories=categories_cfg,
            sources=sources_cfg,
            db=DbCfg(
                path=str(_req(db, "path", "db")).strip(),
                keep_days=_as_int(_req(db, "keep_days", "db"), "db.keep_days"),
            ),
            posting=PostingCfg(
                max_posts_per_run=_as_int(_req(posting, "max_posts_per_run", "posting"), "posting.max_posts_per_run"),
                only_last_hours=_as_int(_req(posting, "only_last_hours", "posting"), "posting.only_last_hours"),
                include_source_name=_as_bool(_req(posting, "include_source_name", "posting"), "posting.include_source_name"),
                cluster_wait_minutes=_as_int(_req(posting, "cluster_wait_minutes", "posting"), "posting.cluster_wait_minutes"),
                breaking_sources_threshold=_as_int(_req(posting, "breaking_sources_threshold", "posting"), "posting.breaking_sources_threshold"),
                wrap_rules=wrap_rules_cfg,
            ),
            network=NetworkCfg(
                timeout_sec=_as_int(_req(network, "timeout_sec", "network"), "network.timeout_sec"),
                verify=_req(network, "verify", "network"),
            ),
            embeddings=EmbeddingsCfg(
                window_hours=_as_int(_req(emb, "window_hours", "embeddings"), "embeddings.window_hours"),
                threshold=_as_float(_req(emb, "threshold", "embeddings"), "embeddings.threshold"),
                require_good_summary=_as_bool(_req(emb, "require_good_summary", "embeddings"), "embeddings.require_good_summary"),
            ),
            app=AppCfg(
                sleep_between_posts_sec=_as_float(_req(app, "sleep_between_posts_sec", "app"), "app.sleep_between_posts_sec"),
                log_level=str(_req(app, "log_level", "app")).strip(),
            ),
            monitor=MonitorCfg(
                every_seconds=_as_int(_req(monitor, "every_seconds", "monitor"), "monitor.every_seconds"),
                command_poll_seconds=_as_int(_req(monitor, "command_poll_seconds", "monitor"), "monitor.command_poll_seconds"),
            ),
            translate=TranslateCfg(
                enabled=_as_bool(_req(translate, "enabled", "translate"), "translate.enabled"),
                model=str(_req(translate, "model", "translate")).strip(),
                max_chars_summary=_as_int(_req(translate, "max_chars_summary", "translate"), "translate.max_chars_summary"),
            ),
            images=ImagesCfg(
                og_fetch=_as_bool(_req(images, "og_fetch", "images"), "images.og_fetch"),
            ),
            llm=llm_cfg,
            filters=FiltersCfg(
                deny_title_regex=_as_str_list(_req(filters, "deny_title_regex", "filters")),
                deny_url_regex=_as_str_list(_req(filters, "deny_url_regex", "filters")),
                deny_summary_regex=_as_str_list(_req(filters, "deny_summary_regex", "filters")),
            ),
            analytics=AnalyticsCfg(
                enabled=_as_bool(_req(analytics, "enabled", "analytics"), "analytics.enabled"),
                daily_report_enabled=_as_bool(_req(analytics, "daily_report_enabled", "analytics"), "analytics.daily_report_enabled"),
                commands_enabled=_as_bool(_req(analytics, "commands_enabled", "analytics"), "analytics.commands_enabled"),
                report_hour_local=_as_int(_req(analytics, "report_hour_local", "analytics"), "analytics.report_hour_local"),
                timezone=str(_req(analytics, "timezone", "analytics")).strip(),
            ),
            text_processing=TextProcessingCfg(
                enabled=_as_bool(text_processing.get("enabled", True), "text_processing.enabled"),
                matching_window_hours=_as_int(text_processing.get("matching_window_hours", emb.get("window_hours", 36)), "text_processing.matching_window_hours"),
                max_candidates=_as_int(text_processing.get("max_candidates", 150), "text_processing.max_candidates"),
                max_keywords=_as_int(text_processing.get("max_keywords", 12), "text_processing.max_keywords"),
                duplicate_combined_threshold=_as_float(text_processing.get("duplicate_combined_threshold", 0.88), "text_processing.duplicate_combined_threshold"),
                duplicate_semantic_threshold=_as_float(text_processing.get("duplicate_semantic_threshold", 0.90), "text_processing.duplicate_semantic_threshold"),
                duplicate_lexical_threshold=_as_float(text_processing.get("duplicate_lexical_threshold", 0.70), "text_processing.duplicate_lexical_threshold"),
                same_event_combined_threshold=_as_float(text_processing.get("same_event_combined_threshold", 0.77), "text_processing.same_event_combined_threshold"),
                same_event_semantic_threshold=_as_float(text_processing.get("same_event_semantic_threshold", 0.78), "text_processing.same_event_semantic_threshold"),
                same_event_lexical_threshold=_as_float(text_processing.get("same_event_lexical_threshold", 0.52), "text_processing.same_event_lexical_threshold"),
            ),
            editorial=EditorialCfg(
                min_post_score=_as_float(editorial.get("min_post_score", 0.84), "editorial.min_post_score"),
                topic_quota=TopicQuotaCfg(
                    window1_hours=_as_int(editorial_topic_quota.get("window1_hours", 4), "editorial.topic_quota.window1_hours"),
                    max_posts_window1=_as_int(editorial_topic_quota.get("max_posts_window1", 1), "editorial.topic_quota.max_posts_window1"),
                    window2_hours=_as_int(editorial_topic_quota.get("window2_hours", 12), "editorial.topic_quota.window2_hours"),
                    max_posts_window2=_as_int(editorial_topic_quota.get("max_posts_window2", 2), "editorial.topic_quota.max_posts_window2"),
                ),
                topic_saturation=TopicSaturationCfg(
                    lookback_hours=_as_int(editorial_topic_saturation.get("lookback_hours", 12), "editorial.topic_saturation.lookback_hours"),
                    max_topic_share=_as_float(editorial_topic_saturation.get("max_topic_share", 0.40), "editorial.topic_saturation.max_topic_share"),
                    min_posts=_as_int(editorial_topic_saturation.get("min_posts", 5), "editorial.topic_saturation.min_posts"),
                ),
                delay_non_breaking=EditorialDelayCfg(
                    min_minutes=_as_int(editorial_delay.get("min_minutes", 10), "editorial.delay_non_breaking.min_minutes"),
                    max_minutes=_as_int(editorial_delay.get("max_minutes", 20), "editorial.delay_non_breaking.max_minutes"),
                ),
                source_weighting=SourceWeightingCfg(
                    high_trust_sources=_as_str_list(editorial_sources.get("high_trust_sources") or ["Reuters", "Bloomberg", "Financial Times", "Associated Press"]),
                ),
                origin_balance=OriginBalanceCfg(
                    enabled=_as_bool(_as_dict(editorial.get("origin_balance") or {}, "editorial.origin_balance").get("enabled", True), "editorial.origin_balance.enabled"),
                    lookback_hours=_as_int(_as_dict(editorial.get("origin_balance") or {}, "editorial.origin_balance").get("lookback_hours", 12), "editorial.origin_balance.lookback_hours"),
                    max_origin_share=_as_float(_as_dict(editorial.get("origin_balance") or {}, "editorial.origin_balance").get("max_origin_share", 0.60), "editorial.origin_balance.max_origin_share"),
                    min_posts=_as_int(_as_dict(editorial.get("origin_balance") or {}, "editorial.origin_balance").get("min_posts", 6), "editorial.origin_balance.min_posts"),
                    ua_relevance_boost=_as_float(_as_dict(editorial.get("origin_balance") or {}, "editorial.origin_balance").get("ua_relevance_boost", 0.10), "editorial.origin_balance.ua_relevance_boost"),
                ),
            ),
            articles=ArticlesCfg(
                fetch_full_text=_as_bool(_as_dict(raw.get("articles") or {}, "articles").get("fetch_full_text", True), "articles.fetch_full_text"),
                max_chars=_as_int(_as_dict(raw.get("articles") or {}, "articles").get("max_chars", 3000), "articles.max_chars"),
            ),
            media=MediaCfg(
                enable_video=_as_bool(_as_dict(raw.get("media") or {}, "media").get("enable_video", True), "media.enable_video"),
                max_image_bytes=_as_int(_as_dict(raw.get("media") or {}, "media").get("max_image_bytes", 5 * 1024 * 1024), "media.max_image_bytes"),
                max_video_bytes=_as_int(_as_dict(raw.get("media") or {}, "media").get("max_video_bytes", 20 * 1024 * 1024), "media.max_video_bytes"),
            ),
            moderation=_load_moderation_cfg(_as_dict(raw.get("moderation") or {}, "moderation")),
            engagement=_load_engagement_cfg(_as_dict(raw.get("engagement") or {}, "engagement")),
            ranking=_load_ranking_cfg(_as_dict(raw.get("ranking") or {}, "ranking")),
            clustering=_load_clustering_cfg(_as_dict(raw.get("clustering") or {}, "clustering")),
        )


def _load_clustering_cfg(section: dict[str, Any]) -> ClusteringCfg:
    wrap = _as_dict(section.get("wrap") or {}, "clustering.wrap")
    return ClusteringCfg(
        enabled=_as_bool(section.get("enabled", False), "clustering.enabled"),
        threshold=_as_float(section.get("threshold", 0.80), "clustering.threshold"),
        window_hours=_as_int(section.get("window_hours", 48), "clustering.window_hours"),
        wrap=ClusterWrapCfg(
            min_items=_as_int(wrap.get("min_items", 3), "clustering.wrap.min_items"),
            min_sources=_as_int(wrap.get("min_sources", 2), "clustering.wrap.min_sources"),
            lookback_hours=_as_int(wrap.get("lookback_hours", 6), "clustering.wrap.lookback_hours"),
            cooldown_minutes=_as_int(wrap.get("cooldown_minutes", 90), "clustering.wrap.cooldown_minutes"),
        ),
    )


def _load_ranking_cfg(section: dict[str, Any]) -> RankingCfg:
    return RankingCfg(
        enabled=_as_bool(section.get("enabled", False), "ranking.enabled"),
        cycle_minutes=_as_int(section.get("cycle_minutes", 25), "ranking.cycle_minutes"),
        window_hours=_as_int(section.get("window_hours", 3), "ranking.window_hours"),
        model=str(section.get("model") or "gpt-4o").strip(),
        max_picks=_as_int(section.get("max_picks", 2), "ranking.max_picks"),
        max_age_hours=_as_int(section.get("max_age_hours", 8), "ranking.max_age_hours"),
    )


def _load_engagement_cfg(section: dict[str, Any]) -> EngagementCfg:
    return EngagementCfg(
        enabled=_as_bool(section.get("enabled", True), "engagement.enabled"),
        poll_hours=_as_int(section.get("poll_hours", 6), "engagement.poll_hours"),
        lookback_hours=_as_int(section.get("lookback_hours", 72), "engagement.lookback_hours"),
        max_posts=_as_int(section.get("max_posts", 50), "engagement.max_posts"),
        mtproto_enabled=_as_bool(section.get("mtproto_enabled", False), "engagement.mtproto_enabled"),
    )


def _load_moderation_cfg(section: dict[str, Any]) -> ModerationCfg:
    on_timeout = str(section.get("on_timeout") or "skip").strip().lower()
    if on_timeout not in ("skip", "publish"):
        raise ValueError(f"moderation.on_timeout must be 'skip' or 'publish', got: {on_timeout!r}")
    return ModerationCfg(
        enabled=_as_bool(section.get("enabled", False), "moderation.enabled"),
        timeout_minutes=_as_int(section.get("timeout_minutes", 45), "moderation.timeout_minutes"),
        on_timeout=on_timeout,
    )
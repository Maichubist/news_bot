from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List
import os
import yaml


def _req(d: Dict[str, Any], key: str):
    if key not in d or d[key] in (None, ""):
        raise ValueError(f"Missing required config key: {key}")
    return d[key]


@dataclass(frozen=True)
class TelegramCfg:
    token: str
    chat_id: int


@dataclass(frozen=True)
class OpenAICfg:
    api_key: str
    model: str = "text-embedding-3-small"


@dataclass(frozen=True)
class CategoryCfg:
    slug: str
    title: str
    hashtag: str


@dataclass(frozen=True)
class SourceCfg:
    name: str
    url: str
    deny_title_regex: List[str] | None = None
    deny_url_regex: List[str] | None = None


@dataclass(frozen=True)
class FiltersCfg:
    deny_title_regex: List[str]
    deny_url_regex: List[str]
    deny_summary_regex: List[str]


@dataclass(frozen=True)
class DbCfg:
    path: str = "data/news.db"
    keep_days: int = 14


@dataclass(frozen=True)
class WrapRuleCfg:
    key: str
    title: str
    categories: List[str]
    min_items: int = 3
    lookback_hours: int = 6
    cooldown_minutes: int = 90
    min_sources: int = 2
    source_label: str = "Topic Wrap"
    hashtag_slug: str = "other"
    prompt_template: str = ""


@dataclass(frozen=True)
class PostingCfg:
    max_posts_per_run: int = 5
    only_last_hours: int = 24
    include_source_name: bool = True
    cluster_wait_minutes: int = 5
    breaking_sources_threshold: int = 3
    wrap_rules: List[WrapRuleCfg] | None = None


@dataclass(frozen=True)
class ImagesCfg:
    og_fetch: bool = True


@dataclass(frozen=True)
class LlmCfg:
    post_model: str = "gpt-4o-mini"
    digest_model: str = "gpt-4o-mini"
    wrap_model: str = "gpt-4o-mini"
    post_prompt: str = ""
    wrap_prompt: str = ""
    digest_prompt: str = ""
    market_wrap_prompt: str = ""
    geopolitical_wrap_prompt: str = ""
    tech_wrap_prompt: str = ""


@dataclass(frozen=True)
class NetworkCfg:
    timeout_sec: int = 25
    verify: Any = "certifi"


@dataclass(frozen=True)
class EmbeddingsCfg:
    window_hours: int = 24
    threshold: float = 0.90
    require_good_summary: bool = False


@dataclass(frozen=True)
class AppCfg:
    sleep_between_posts_sec: float = 1.2
    log_level: str = "INFO"


@dataclass(frozen=True)
class MonitorCfg:
    every_seconds: int = 120


@dataclass(frozen=True)
class TranslateCfg:
    enabled: bool = True
    model: str = "gpt-5-mini"
    max_chars_summary: int = 350


@dataclass(frozen=True)
class AppConfig:
    telegram: TelegramCfg
    openai: OpenAICfg
    categories: List[CategoryCfg]
    sources: List[SourceCfg]
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

    @staticmethod
    def load(path: str = "config.yaml") -> "AppConfig":
        with open(path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}

        mon = raw.get("monitor", {})
        tr = raw.get("translate", {})

        monitor_cfg = MonitorCfg(every_seconds=int(mon.get("every_seconds", 120)))
        translate_cfg = TranslateCfg(
            enabled=bool(tr.get("enabled", True)),
            model=str(tr.get("model", "gpt-5-mini")),
            max_chars_summary=int(tr.get("max_chars_summary", 350)),
        )

        tg = raw.get("telegram", {})
        oa = raw.get("openai", {})
        db = raw.get("db", {})
        posting = raw.get("posting", {})
        network = raw.get("network", {})
        emb = raw.get("embeddings", {})
        app = raw.get("app", {})
        images = raw.get("images", {})
        llm = raw.get("llm", {})
        filt = raw.get("filters", {})

        tg_token = os.getenv("TELEGRAM_TOKEN") or os.getenv("TG_TOKEN")
        oa_key = os.getenv("OPENAI_API_KEY")
        if not tg_token:
            raise ValueError("Missing TELEGRAM_TOKEN in environment/.env")
        if not oa_key:
            raise ValueError("Missing OPENAI_API_KEY in environment/.env")

        cats_raw = raw.get("categories")
        if not isinstance(cats_raw, list) or not cats_raw:
            cats_raw = [
                {"slug": "war", "title": "Війна", "hashtag": "#війна"},
                {"slug": "politics", "title": "Політика", "hashtag": "#політика"},
                {"slug": "economy", "title": "Економіка", "hashtag": "#економіка"},
                {"slug": "technology", "title": "Технології", "hashtag": "#технології"},
                {"slug": "business", "title": "Бізнес", "hashtag": "#бізнес"},
                {"slug": "society", "title": "Суспільство", "hashtag": "#суспільство"},
                {"slug": "science", "title": "Наука", "hashtag": "#наука"},
                {"slug": "other", "title": "Інше", "hashtag": "#інше"},
            ]

        categories_cfg = [
            CategoryCfg(
                slug=str(c.get("slug") or "").strip(),
                title=str(c.get("title") or "").strip(),
                hashtag=str(c.get("hashtag") or "").strip(),
            )
            for c in cats_raw
            if str(c.get("slug") or "").strip()
        ]
        if not categories_cfg:
            raise ValueError("Config key 'categories' must be a non-empty list")

        sources_raw = raw.get("sources", [])
        if not isinstance(sources_raw, list) or not sources_raw:
            raise ValueError("Config key 'sources' must be a non-empty list")

        def _as_list(v) -> List[str]:
            if v is None:
                return []
            if isinstance(v, str):
                return [v]
            if isinstance(v, list):
                return [str(x) for x in v if str(x).strip()]
            return []

        llm_cfg = LlmCfg(
            post_model=str(llm.get("post_model", "gpt-4o-mini")),
            digest_model=str(llm.get("digest_model", "gpt-4o-mini")),
            wrap_model=str(llm.get("wrap_model", llm.get("market_wrap_model", "gpt-4o-mini"))),
            post_prompt=str(llm.get("post_prompt", "")),
            wrap_prompt=str(llm.get("wrap_prompt", "")),
            digest_prompt=str(llm.get("digest_prompt", "")),
            market_wrap_prompt=str(llm.get("market_wrap_prompt", "")),
            geopolitical_wrap_prompt=str(llm.get("geopolitical_wrap_prompt", "")),
            tech_wrap_prompt=str(llm.get("tech_wrap_prompt", "")),
        )

        def _resolve_prompt_template(value: Any, rule_key: str = "") -> str:
            key = str(value or "").strip()
            if key:
                if hasattr(llm_cfg, key):
                    return str(getattr(llm_cfg, key) or "").strip()
                return key

            # Якщо prompt_template не задано, підбираємо найкращий дефолт
            rk = (rule_key or "").lower()
            if rk in ("economy_wrap", "economy", "market_wrap", "market"):
                return str(llm_cfg.market_wrap_prompt or llm_cfg.wrap_prompt or "").strip()
            if rk in ("geopolitics_wrap", "geopolitics", "geopolitical_wrap", "war_wrap", "politics_wrap"):
                return str(llm_cfg.geopolitical_wrap_prompt or llm_cfg.wrap_prompt or "").strip()
            if rk in ("technology_wrap", "technology", "tech_wrap", "tech", "science_wrap"):
                return str(llm_cfg.tech_wrap_prompt or llm_cfg.wrap_prompt or "").strip()

            return str(llm_cfg.wrap_prompt or "").strip()

        wrap_rules_raw = posting.get("wrap_rules")
        wrap_rules: List[WrapRuleCfg] = []
        if isinstance(wrap_rules_raw, list) and wrap_rules_raw:
            for item in wrap_rules_raw:
                if not isinstance(item, dict):
                    continue

                key = str(item.get("key") or item.get("name") or "").strip()
                if not key:
                    continue

                title = str(item.get("title") or key).strip()

                wrap_rules.append(
                    WrapRuleCfg(
                        key=key,
                        title=title,
                        categories=_as_list(item.get("categories")),
                        min_items=int(item.get("min_items", 3)),
                        lookback_hours=int(item.get("lookback_hours", 6)),
                        cooldown_minutes=int(item.get("cooldown_minutes", 90)),
                        min_sources=int(item.get("min_sources", 2)),
                        source_label=str(item.get("source_label") or title).strip(),
                        hashtag_slug=str(item.get("hashtag_slug") or "other").strip() or "other",
                        prompt_template=_resolve_prompt_template(item.get("prompt_template"), key),
                    )
                )

        if not wrap_rules:
            wrap_rules = [
                WrapRuleCfg(
                    key="economy_wrap",
                    title="Market Wrap",
                    categories=["economy", "business"],
                    min_items=3,
                    lookback_hours=6,
                    cooldown_minutes=90,
                    min_sources=2,
                    source_label="Market Wrap",
                    hashtag_slug="economy",
                    prompt_template=str(llm_cfg.market_wrap_prompt or llm_cfg.wrap_prompt or "").strip(),
                ),
                WrapRuleCfg(
                    key="geopolitics_wrap",
                    title="Geopolitics Wrap",
                    categories=["war", "politics"],
                    min_items=3,
                    lookback_hours=6,
                    cooldown_minutes=120,
                    min_sources=2,
                    source_label="Geopolitics Wrap",
                    hashtag_slug="war",
                    prompt_template=str(llm_cfg.geopolitical_wrap_prompt or llm_cfg.wrap_prompt or "").strip(),
                ),
                WrapRuleCfg(
                    key="technology_wrap",
                    title="Tech Wrap",
                    categories=["technology", "science"],
                    min_items=3,
                    lookback_hours=8,
                    cooldown_minutes=180,
                    min_sources=2,
                    source_label="Tech Wrap",
                    hashtag_slug="technology",
                    prompt_template=str(llm_cfg.tech_wrap_prompt or llm_cfg.wrap_prompt or "").strip(),
                ),
            ]

        return AppConfig(
            telegram=TelegramCfg(
                token=str(tg_token),
                chat_id=int(_req(tg, "chat_id")),
            ),
            openai=OpenAICfg(
                api_key=str(oa_key),
                model=str(oa.get("model", "text-embedding-3-small")),
            ),
            categories=categories_cfg,
            sources=[
                SourceCfg(
                    name=str(s["name"]),
                    url=str(s["url"]),
                    deny_title_regex=_as_list(s.get("deny_title_regex")),
                    deny_url_regex=_as_list(s.get("deny_url_regex")),
                )
                for s in sources_raw
            ],
            db=DbCfg(
                path=str(db.get("path", "data/news.db")),
                keep_days=int(db.get("keep_days", 14)),
            ),
            posting=PostingCfg(
                max_posts_per_run=int(posting.get("max_posts_per_run", 5)),
                only_last_hours=int(posting.get("only_last_hours", 24)),
                include_source_name=bool(posting.get("include_source_name", True)),
                cluster_wait_minutes=int(posting.get("cluster_wait_minutes", 5)),
                breaking_sources_threshold=int(posting.get("breaking_sources_threshold", 3)),
                wrap_rules=wrap_rules,
            ),
            network=NetworkCfg(
                timeout_sec=int(network.get("timeout_sec", 25)),
                verify=network.get("verify", "certifi"),
            ),
            embeddings=EmbeddingsCfg(
                window_hours=int(emb.get("window_hours", 24)),
                threshold=float(emb.get("threshold", 0.90)),
                require_good_summary=bool(emb.get("require_good_summary", False)),
            ),
            app=AppCfg(
                sleep_between_posts_sec=float(app.get("sleep_between_posts_sec", 1.2)),
                log_level=str(app.get("log_level", "INFO")),
            ),
            monitor=monitor_cfg,
            translate=translate_cfg,
            images=ImagesCfg(og_fetch=bool(images.get("og_fetch", True))),
            llm=llm_cfg,
            filters=FiltersCfg(
                deny_title_regex=_as_list(filt.get("deny_title_regex")),
                deny_url_regex=_as_list(filt.get("deny_url_regex")),
                deny_summary_regex=_as_list(filt.get("deny_summary_regex")),
            ),
        )
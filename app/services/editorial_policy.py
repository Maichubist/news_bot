from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo


KYIV_TZ = ZoneInfo("Europe/Kyiv")
CRITICAL_CATEGORIES = {"war", "politics"}
DEFAULT_HIGH_TRUST = {
    "reuters", "associated press", "ap", "bloomberg", "financial times", "ft",
    "українська правда", "економічна правда", "суспільне",
}
DEFAULT_MEDIUM_TRUST = {
    "guardian", "the guardian", "cnbc", "politico", "al jazeera",
    "нв", "liga.net", "liga", "forbes ukraine", "бабель",
}
NON_NEWS_PATTERNS = [
    "how ", "why ", "what to know", "explainer",
    "analysis", "opinion", "column",
    "review", "guide", "feature",
    "experts say", "likely", "could", "may",
]
CONSEQUENCE_HINTS = {
    "killed", "dead", "injured", "wounded", "damage", "shutdown", "outage", "halt",
    "cuts", "cut", "block", "blocked", "withdraw", "withdrawal", "tariff", "sanction",
    "sanctions", "ban", "approved", "rejected", "passed", "signed", "deployed", "evacuated",
    "surged", "fell", "drop", "rose", "slowed", "expanded", "won", "lost", "confirmed",
}


@dataclass(frozen=True)
class EditorialDecision:
    mode: str
    should_post: bool
    status: str
    news_type: str
    has_new_fact: bool
    topic_key: str
    delay_until_utc: str | None
    source_trust: str
    source_count: int
    reasons: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class TopicQuotaCfg:
    window1_hours: int
    max_posts_window1: int
    window2_hours: int
    max_posts_window2: int


@dataclass(frozen=True)
class SaturationCfg:
    lookback_hours: int
    max_topic_share: float
    min_posts: int


@dataclass(frozen=True)
class DelayCfg:
    min_minutes: int
    max_minutes: int


def _safe_set(value: Any) -> set[str]:
    if value is None:
        return set()
    if isinstance(value, str):
        return {value.strip().lower()} if value.strip() else set()
    out: set[str] = set()
    for item in value:
        s = str(item).strip().lower()
        if s:
            out.add(s)
    return out


def _stable_delay_minutes(topic_key: str, min_minutes: int, max_minutes: int) -> int:
    if max_minutes <= min_minutes:
        return int(min_minutes)
    span = max_minutes - min_minutes + 1
    digest = hashlib.sha1(topic_key.encode("utf-8", errors="ignore")).digest()
    offset = int.from_bytes(digest[:2], "big") % span
    return int(min_minutes + offset)


def is_today_news(published_at: datetime | None, now: datetime | None = None, tz: ZoneInfo = KYIV_TZ) -> bool:
    if not published_at:
        return False
    if published_at.tzinfo is None:
        published_at = published_at.replace(tzinfo=timezone.utc)
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    return published_at.astimezone(tz).date() == now.astimezone(tz).date()


def is_non_news_title(title: str) -> bool:
    if not title or not title.strip():
        return True
    t = title.strip().lower()
    return any(p in t for p in NON_NEWS_PATTERNS)


def _strip_generic_tokens(parts: Iterable[str]) -> list[str]:
    bad = {
        "update", "updates", "breaking", "news", "latest", "live", "analysis",
        "commentary", "comment", "report", "reports", "says", "say", "guide", "feature",
    }
    out: list[str] = []
    for part in parts:
        s = str(part).strip().lower()
        if len(s) < 3 or s in bad:
            continue
        out.append(s)
    return out


def normalize_topic_key(event_key: str, title: str, category: str, entities: Iterable[str] | None = None, keywords: Iterable[str] | None = None) -> str:
    ek = re.sub(r"[^a-z0-9\-]+", "-", (event_key or "").strip().lower()).strip("-")
    if ek:
        return ek[:80]

    seed = _strip_generic_tokens(list(entities or []) + list(keywords or []))
    if not seed:
        seed = _strip_generic_tokens(re.findall(r"[a-zA-Zа-яА-ЯіІїЇєЄ0-9][a-zA-Zа-яА-ЯіІїЇєЄ0-9\-']+", title or ""))
    if not seed:
        seed = [category or "other"]
    topic = "-".join(seed[:5])
    topic = re.sub(r"-+", "-", topic).strip("-")
    return topic[:80] or (category or "other")


def has_new_fact(item: Mapping[str, Any], cluster: list[Mapping[str, Any]]) -> bool:
    if not cluster:
        return True

    item_numbers = _safe_set(item.get("numbers"))
    item_verbs = _safe_set(item.get("event_verbs"))
    item_entities = _safe_set(item.get("entities"))
    item_keywords = _safe_set(item.get("keywords"))

    old_numbers: set[str] = set()
    old_verbs: set[str] = set()
    old_entities: set[str] = set()
    old_keywords: set[str] = set()
    for row in cluster:
        old_numbers |= _safe_set(row.get("numbers"))
        old_verbs |= _safe_set(row.get("event_verbs"))
        old_entities |= _safe_set(row.get("entities"))
        old_keywords |= _safe_set(row.get("keywords"))

    if item_numbers - old_numbers:
        return True
    if item_entities - old_entities:
        return True
    if item_verbs - old_verbs:
        return True

    consequence_terms = {t for t in item_keywords if t in CONSEQUENCE_HINTS}
    if consequence_terms - old_keywords:
        return True

    return False


def topic_post_limit_exceeded(topic_key: str, history: Mapping[str, int], quota_cfg: TopicQuotaCfg) -> bool:
    c1 = int(history.get(f"{quota_cfg.window1_hours}h", 0) or 0)
    c2 = int(history.get(f"{quota_cfg.window2_hours}h", 0) or 0)
    return c1 >= quota_cfg.max_posts_window1 or c2 >= quota_cfg.max_posts_window2


def source_trust_for(source: str, high_trust_sources: Iterable[str]) -> str:
    source_l = (source or "").strip().lower()
    configured_high = {str(x).strip().lower() for x in (high_trust_sources or []) if str(x).strip()}
    high = DEFAULT_HIGH_TRUST | configured_high
    for token in high:
        if token and token in source_l:
            return "high"
    for token in DEFAULT_MEDIUM_TRUST:
        if token and token in source_l:
            return "medium"
    return "low"


def _is_breaking(decision: Any) -> bool:
    return bool(getattr(decision, "is_breaking", False) or float(getattr(decision, "impact_score", 0.0) or 0.0) >= 0.90)


def decide_publish_mode(
    *,
    decision: Any,
    item_row: Mapping[str, Any],
    cluster_rows: list[Mapping[str, Any]],
    topic_history: Mapping[str, int],
    topic_mix: Mapping[str, Any],
    same_event: bool,
    wrap_rule: Any,
    quota_cfg: TopicQuotaCfg,
    saturation_cfg: SaturationCfg,
    delay_cfg: DelayCfg,
    high_trust_sources: Iterable[str],
) -> EditorialDecision:
    now_utc = datetime.now(timezone.utc)
    published_at = item_row.get("published_at")
    if not is_today_news(published_at, now_utc, KYIV_TZ):
        return EditorialDecision("drop", False, "filtered", "noise", False, "", None, "low", 0, ["not-today"])

    title = str(item_row.get("title") or "")
    if is_non_news_title(title):
        return EditorialDecision("drop", False, "filtered", str(getattr(decision, "news_type", "noise") or "noise"), False, "", None, "low", 0, ["non-news-title"])

    news_type = str(getattr(decision, "news_type", "noise") or "noise").strip().lower()
    if news_type not in {"hard_news", "followup", "analysis", "commentary", "feature", "advice", "promo", "noise"}:
        news_type = "noise"

    topic_key = normalize_topic_key(
        getattr(decision, "topic_key", "") or getattr(decision, "event_key", ""),
        title,
        str(getattr(decision, "category", "other") or "other"),
        item_row.get("entities") or [],
        item_row.get("keywords") or [],
    )
    source_count = int(item_row.get("source_count") or 1)
    source_trust = source_trust_for(str(item_row.get("source") or ""), high_trust_sources)
    reasons: list[str] = []

    effective_new_fact = has_new_fact(item_row, cluster_rows)
    if same_event and not effective_new_fact:
        reasons.append("same-event-without-new-fact")
        if wrap_rule:
            return EditorialDecision("wrap", False, "pending_wrap", news_type, False, topic_key, None, source_trust, source_count, reasons)
        return EditorialDecision("drop", False, "filtered", news_type, False, topic_key, None, source_trust, source_count, reasons)

    if news_type != "hard_news":
        reasons.append("only-hard-news-can-post")
        if news_type == "followup":
            if wrap_rule:
                return EditorialDecision("wrap", False, "pending_wrap", news_type, effective_new_fact, topic_key, None, source_trust, source_count, reasons)
            return EditorialDecision("digest", False, "digest_only", news_type, effective_new_fact, topic_key, None, source_trust, source_count, reasons)
        return EditorialDecision("drop", False, "filtered", news_type, effective_new_fact, topic_key, None, source_trust, source_count, reasons)

    if not effective_new_fact:
        reasons.append("new-fact-gate-failed")
        if wrap_rule:
            return EditorialDecision("wrap", False, "pending_wrap", news_type, False, topic_key, None, source_trust, source_count, reasons)
        return EditorialDecision("drop", False, "filtered", news_type, False, topic_key, None, source_trust, source_count, reasons)

    if topic_post_limit_exceeded(topic_key, topic_history, quota_cfg):
        reasons.append("topic-quota-exceeded")
        if wrap_rule:
            return EditorialDecision("wrap", False, "pending_wrap", news_type, True, topic_key, None, source_trust, source_count, reasons)
        return EditorialDecision("digest", False, "digest_only", news_type, True, topic_key, None, source_trust, source_count, reasons)

    total_posts = int(topic_mix.get("total_posts", 0) or 0)
    dominant_posts = int(topic_mix.get("topic_posts", 0) or 0)
    share = (dominant_posts / total_posts) if total_posts > 0 else 0.0
    if total_posts >= saturation_cfg.min_posts and share > saturation_cfg.max_topic_share:
        reasons.append(f"topic-saturated:{share:.2f}")
        return EditorialDecision("digest", False, "digest_only", news_type, True, topic_key, None, source_trust, source_count, reasons)

    category = str(getattr(decision, "category", "other") or "other").strip().lower()
    if category in CRITICAL_CATEGORIES and not (source_trust == "high" or source_count >= 2):
        reasons.append("critical-needs-high-trust-or-2-sources")
        return EditorialDecision("pending_confirmation", False, "pending_confirmation", news_type, True, topic_key, None, source_trust, source_count, reasons)

    if source_trust != "high" and source_count < 2:
        reasons.append("awaiting-confirmation")
        return EditorialDecision("pending_confirmation", False, "pending_confirmation", news_type, True, topic_key, None, source_trust, source_count, reasons)

    if _is_breaking(decision):
        reasons.append("breaking")
        return EditorialDecision("post", True, "pending_post", news_type, True, topic_key, now_utc.isoformat(timespec="seconds"), source_trust, source_count, reasons)

    delay_minutes = _stable_delay_minutes(topic_key, delay_cfg.min_minutes, delay_cfg.max_minutes)
    delay_until = (now_utc + timedelta(minutes=delay_minutes)).isoformat(timespec="seconds")
    reasons.append(f"delay:{delay_minutes}m")
    return EditorialDecision("post", True, "pending_post", news_type, True, topic_key, delay_until, source_trust, source_count, reasons)

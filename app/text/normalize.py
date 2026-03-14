import re
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode


_UTM_KEYS_PREFIX = ("utm_",)
_DROP_KEYS_EXACT = {
    "fbclid", "gclid", "yclid", "mc_cid", "mc_eid",
    "igshid", "mkt_tok",
    "ref", "referrer", "source", "src", "cmpid", "ocid", "sr",
    "spm", "mkt", "campaign", "campaignid", "fb_action_ids", "fb_action_types",
}

_DROP_TEXT_PATTERNS = [
    r"(?i)read more",
    r"(?i)click here",
    r"(?i)newsletter",
    r"(?i)subscribe now",
    r"(?i)live updates?",
    r"(?i)as it happened",
]


def canonicalize_url(url: str) -> str:
    try:
        parts = urlsplit(url.strip())
        scheme = parts.scheme.lower() or "https"
        netloc = parts.netloc.lower()

        path = parts.path or ""
        path = re.sub(r"/{2,}", "/", path)
        if path != "/" and path.endswith("/"):
            path = path[:-1]

        query_pairs = []
        for k, v in parse_qsl(parts.query, keep_blank_values=True):
            kl = k.lower()
            if kl in _DROP_KEYS_EXACT:
                continue
            if any(kl.startswith(p) for p in _UTM_KEYS_PREFIX):
                continue
            query_pairs.append((k, v))

        query = urlencode(query_pairs, doseq=True)
        netloc = netloc.replace(":80", "").replace(":443", "")
        return urlunsplit((scheme, netloc, path, query, ""))
    except Exception:
        return url.strip()


def normalize_text(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def strip_noise_text(s: str) -> str:
    text = (s or "").strip()
    for pat in _DROP_TEXT_PATTERNS:
        text = re.sub(pat, " ", text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_article_text(s: str) -> str:
    text = strip_noise_text(s)
    text = text.replace("’", "'")
    text = text.lower()
    text = re.sub(r"[^\w\s\-\$€£¥%\.]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()

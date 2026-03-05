import re
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode


_UTM_KEYS_PREFIX = ("utm_",)
_DROP_KEYS_EXACT = {
    "fbclid", "gclid", "yclid", "mc_cid", "mc_eid",
    "igshid", "mkt_tok",
    "ref", "referrer", "source", "src", "cmpid", "ocid", "sr",
    "spm", "mkt", "campaign", "campaignid", "fb_action_ids", "fb_action_types",
}


def canonicalize_url(url: str) -> str:
    try:
        parts = urlsplit(url.strip())
        scheme = parts.scheme.lower() or "https"
        netloc = parts.netloc.lower()

        # Normalize path: collapse multiple slashes, keep trailing slash stable.
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

        # Drop fragments: they create exact-dup misses and almost never matter for articles.
        return urlunsplit((scheme, netloc, path, query, ""))
    except Exception:
        return url.strip()


def normalize_text(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s
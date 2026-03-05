import hashlib
from app.text.normalize import normalize_text, canonicalize_url


class ExactDeduper:
    def make_hash(self, title: str, link: str) -> str:
        link_c = canonicalize_url(link)
        base = normalize_text(title) + "|" + normalize_text(link_c)
        return hashlib.sha256(base.encode("utf-8")).hexdigest()
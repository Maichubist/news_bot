from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Optional, Dict
import time
import requests
import certifi


DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def build_verify_option(v: Any):
    if v is True or v is False:
        return v
    if isinstance(v, str):
        if v.lower() == "false":
            return False
        if v.lower() == "certifi":
            return certifi.where()
        return v
    return certifi.where()


@dataclass
class RequestsSession:
    timeout_sec: int = 25
    verify_opt: Any = certifi.where()
    default_headers: Dict[str, str] = field(default_factory=lambda: {"User-Agent": DEFAULT_UA})
    max_retries: int = 2
    backoff_sec: float = 0.6

    def _merge_headers(self, headers: Optional[Dict[str, str]]) -> Dict[str, str]:
        h = dict(self.default_headers)
        if headers:
            h.update(headers)
        return h

    def get(self, url: str, *, headers: Optional[Dict[str, str]] = None) -> requests.Response:
        hdrs = self._merge_headers(headers)
        last_exc = None
        for i in range(self.max_retries + 1):
            try:
                return requests.get(url, headers=hdrs, timeout=self.timeout_sec, verify=self.verify_opt)
            except requests.RequestException as e:
                last_exc = e
                time.sleep(self.backoff_sec * (2 ** i))
        raise last_exc  # type: ignore[misc]

    def post(self, url: str, *, headers: Optional[Dict[str, str]] = None, json: Any = None, data: Any = None, files: Any = None) -> requests.Response:
        hdrs = self._merge_headers(headers)
        last_exc = None
        for i in range(self.max_retries + 1):
            try:
                return requests.post(
                    url,
                    headers=hdrs,
                    json=json,
                    data=data,
                    files=files,
                    timeout=self.timeout_sec,
                    verify=self.verify_opt,
                )
            except requests.RequestException as e:
                last_exc = e
                time.sleep(self.backoff_sec * (2 ** i))
        raise last_exc  # type: ignore[misc]
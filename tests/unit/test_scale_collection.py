from unittest.mock import Mock

import pytest
import requests
from crawlers.common.rate_limiter import RateLimiter
from crawlers.common.steam_store import get_json


def test_last_retry_429_is_counted(monkeypatch):
    limiter = RateLimiter(0)
    monkeypatch.setattr(requests, "get", lambda *a, **kw: Mock(status_code=429))
    monkeypatch.setattr("time.sleep", lambda *a: None)
    assert get_json("http://test", {}, limiter, retries=2) is None
    assert (limiter.request_count, limiter.request_errors, limiter.http_429) == (3, 3, 3)


def test_transport_and_http_errors(monkeypatch):
    limiter = RateLimiter(0)
    monkeypatch.setattr(requests, "get", Mock(side_effect=requests.Timeout()))
    with pytest.raises(requests.Timeout):
        limiter.request_get("http://test")
    monkeypatch.setattr(requests, "get", lambda *a, **kw: Mock(status_code=503))
    limiter.request_get("http://test")
    monkeypatch.setattr(requests, "get", lambda *a, **kw: Mock(status_code=200))
    limiter.request_get("http://test")
    assert (limiter.request_errors, limiter.http_429) == (2, 0)

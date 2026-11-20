import gzip
import importlib.util
import json
from pathlib import Path
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


def test_nested_subsets_preserve_records(tmp_path):
    spec = importlib.util.spec_from_file_location("subsets", Path(__file__).parents[1] / "scalability/make_scale_subsets.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    raw = tmp_path / "snapshot"
    part = raw / "steam_reviews_raw/dt=2026-11-17/part-1.jsonl.gz"
    part.parent.mkdir(parents=True)
    rows = [{"appid": a, "review": str(a)} for a in [3, 1, 999, 2, 1]]
    with gzip.open(part, "wt", encoding="utf8") as h:
        for row in rows:
            h.write(json.dumps(row) + "\n")
    targets = tmp_path / "targets.json"
    targets.write_text(json.dumps([1, 2, 3]))
    out = tmp_path / "scale"
    module.make_subsets(raw, targets, out, scales=(1, 2, 3))
    for n in (1, 2, 3):
        with gzip.open(out / f"raw_{n}" / part.relative_to(raw), "rt", encoding="utf8") as h:
            assert [json.loads(l) for l in h] == [r for r in rows if r["appid"] in range(1, n + 1)]
    with pytest.raises(ValueError, match="already exists"):
        module.make_subsets(raw, targets, out, scales=(1, 2, 3))

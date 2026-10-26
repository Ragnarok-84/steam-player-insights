import gzip
import json

import pytest

from crawlers.common.targets import is_assigned_to_worker, load_target_apps
from crawlers.replay_jsonl_to_kafka import iter_files, iter_records
from processing.common.aspect_dictionary import extract_aspects
from processing.common.text_preprocessor import clean_review_text, is_spam_or_empty


def test_clean_review_text_strips_urls_ascii_art_and_spaces():
    assert clean_review_text("Great  game\n\n see https://x.y/z ⣿⣿⣿ ok") == "Great game see ok"
    assert clean_review_text(None) == ""


def test_spam_detection():
    assert is_spam_or_empty("")
    assert is_spam_or_empty("⣿⣿⣿⣿")
    assert is_spam_or_empty("good")
    assert not is_spam_or_empty("really good game")


def test_extract_aspects():
    aspects = extract_aspects("Constant lag and the servers disconnect, way overpriced")
    assert {"performance", "server", "price"} <= set(aspects)
    assert extract_aspects("") == []


def test_worker_sharding_covers_every_app_once():
    apps = range(1, 101)
    owners = [[w for w in range(3) if is_assigned_to_worker(a, w, 3)] for a in apps]
    assert all(len(o) == 1 for o in owners)
    with pytest.raises(ValueError):
        is_assigned_to_worker(1, 3, 3)


def test_load_target_apps_dedupes_and_limits(tmp_path):
    f = tmp_path / "t.json"
    f.write_text(json.dumps([{"appid": 730}, {"appid": "730"}, 570, {"appid": 440}]))
    assert load_target_apps(f, 2) == [730, 570]


def test_replay_reads_main_and_retry_dirs_skipping_tmp(tmp_path):
    for sub in ("steam_reviews_raw/dt=2026-01-01", "retry-reviews/steam_reviews_raw/dt=2026-01-02"):
        d = tmp_path / sub
        d.mkdir(parents=True)
        with gzip.open(d / "part-1.jsonl.gz", "wt") as fh:
            fh.write(json.dumps({"appid": 1}) + "\n")
        (d / ".part-2.jsonl.gz.tmp").write_text("garbage")
    files = list(iter_files(tmp_path, "steam_reviews_raw"))
    assert len(files) == 2
    assert [r["appid"] for f in files for r in iter_records(f)] == [1, 1]

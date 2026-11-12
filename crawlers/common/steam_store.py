"""Steam catalog helpers: batched store items (reviews, release, tags), tag names and the most-played chart.

All endpoints are official Steam Web API methods that work without a key.
"""
import json
import logging
import time

import requests

from crawlers.config import STEAM_WEB_API_BASE

logger = logging.getLogger(__name__)

ITEMS_URL = f"{STEAM_WEB_API_BASE}/IStoreBrowseService/GetItems/v1/"
TAGS_URL = f"{STEAM_WEB_API_BASE}/IStoreService/GetTagList/v1/"
MOST_PLAYED_URL = f"{STEAM_WEB_API_BASE}/ISteamChartsService/GetMostPlayedGames/v1/"
ITEMS_BATCH = 200


def get_json(url, params, limiter=None, retries=3, timeout=30):
    """GET with 429/5xx backoff. Returns the decoded body, or None after a non-retryable error."""
    for attempt in range(retries + 1):
        if limiter is not None:
            limiter.wait()
        try:
            resp = requests.get(url, params=params, timeout=timeout)
        except requests.RequestException as exc:
            logger.warning("Request failed (%s)", type(exc).__name__)
            resp = None
        if resp is not None and resp.status_code == 200:
            if limiter is not None:
                limiter.record_success()
            return resp.json()
        if resp is not None and resp.status_code not in (429,) and resp.status_code < 500:
            logger.warning("HTTP %s from %s", resp.status_code, url)
            return None
        if attempt < retries:
            if limiter is not None and resp is not None and resp.status_code == 429:
                limiter.record_rate_limit(cooldown_seconds=30)
            else:
                time.sleep(2 * 2**attempt)
    return None


def fetch_store_items(appids, limiter=None, tag_count=20):
    """Return {appid: raw store item} for every appid Steam knows; batched 200 per call."""
    result = {}
    appids = [int(a) for a in appids]
    for i in range(0, len(appids), ITEMS_BATCH):
        chunk = appids[i:i + ITEMS_BATCH]
        body = get_json(ITEMS_URL, {"input_json": json.dumps({
            "ids": [{"appid": a} for a in chunk],
            "context": {"language": "english", "country_code": "US"},
            "data_request": {"include_reviews": True, "include_release": True, "include_platforms": True,
                             "include_basic_info": True, "include_tag_count": tag_count}})}, limiter)
        if body is None:
            raise RuntimeError("Steam GetItems failed; no partial result is used")
        for item in body.get("response", {}).get("store_items", []):
            if item.get("appid"):
                result[int(item["appid"])] = item
    return result


def fetch_tag_names(limiter=None):
    body = get_json(TAGS_URL, {"language": "english"}, limiter)
    if body is None:
        raise RuntimeError("Steam GetTagList failed")
    return {int(t["tagid"]): t["name"] for t in body.get("response", {}).get("tags", [])}


def fetch_most_played(limiter=None):
    """Steam Charts top ~100 by daily peak CCU: [{rank, appid, last_week_rank, peak_in_game}]."""
    body = get_json(MOST_PLAYED_URL, {}, limiter)
    if body is None:
        raise RuntimeError("Steam GetMostPlayedGames failed")
    resp = body.get("response", {})
    return [{**r, "rollup_date": resp.get("rollup_date")} for r in resp.get("ranks", [])]


def review_count(item):
    return int(item.get("reviews", {}).get("summary_filtered", {}).get("review_count") or 0)


def is_released_game(item, now=None):
    """Visible, released (not 'coming soon'), item type game."""
    now = now or time.time()
    release = item.get("release") or {}
    released_at = release.get("steam_release_date") or release.get("original_release_date")
    return (item.get("success") == 1 and item.get("visible", False) and item.get("type", 0) == 0
            and not release.get("is_coming_soon") and bool(released_at) and released_at <= now)


def parse_store_item(item, tag_names, now=None):
    """Flatten one store item into a metadata record (topic steam.games.meta, source=steam_store_browse)."""
    release = item.get("release") or {}
    summary = item.get("reviews", {}).get("summary_filtered", {})
    platforms = item.get("platforms") or {}
    tags = sorted(item.get("tags") or [], key=lambda t: -t.get("weight", 0))
    return {
        "appid": int(item["appid"]),
        "name": item.get("name"),
        "review_count": int(summary.get("review_count") or 0),
        "percent_positive": summary.get("percent_positive"),
        "review_score": summary.get("review_score"),
        "review_score_label": summary.get("review_score_label"),
        "steam_release_ts": release.get("steam_release_date"),
        "is_coming_soon": bool(release.get("is_coming_soon")),
        "is_early_access": bool(release.get("is_early_access")),
        "tags": [tag_names.get(int(t["tagid"]), str(t["tagid"])) for t in tags],
        "tag_weights": {tag_names.get(int(t["tagid"]), str(t["tagid"])): int(t.get("weight", 0)) for t in tags},
        "steam_deck_compat": platforms.get("steam_deck_compat_category"),
        "timestamp": int(now or time.time()),
        "source": "steam_store_browse",
    }

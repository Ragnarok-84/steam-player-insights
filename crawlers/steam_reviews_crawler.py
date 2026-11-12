"""Incremental Steam review crawler (IUserReviewsService/GetAppReviews, newest first).

Each run fetches only reviews written since the previous run, then continues the history backfill
until --review-days, within a per-game page budget. See crawlers.common.review_state for the segments.
"""
import json
import logging
import os
import time

import requests

from crawlers.config import (
    STEAM_WEB_API_BASE,
    KAFKA_TOPIC_REVIEWS,
    CRAWLER_WORKER_ID,
    CRAWLER_TOTAL_WORKERS,
    CRAWLER_RATE_LIMIT_DELAY,
    STATE_DIR,
    TARGET_APP_LIST_PATH,
    CRAWLER_TARGET_LIMIT,
    CRAWLER_REVIEW_BATCHES,
    CRAWLER_REVIEW_DAYS,
)
from crawlers.common.rate_limiter import RateLimiter
from crawlers.common.review_state import ReviewStateStore, segment, filter_page, lower_bound
from crawlers.common.targets import load_target_apps, is_assigned_to_worker
from crawlers.common.jsonl_producer import create_producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("SteamReviewsCrawler")

BASE_REVIEW_URL = f"{STEAM_WEB_API_BASE}/IUserReviewsService/GetAppReviews/v1/"
REVIEW_STATE_SUBDIR = "reviews_v2"


def to_record(appid, rev, now):
    author = rev.get("author", {})
    wvs = rev.get("weighted_vote_score")
    return {
        "recommendationid": str(rev["recommendationid"]),
        "appid": appid,
        "language": rev.get("language"),
        "review": rev.get("review"),
        "voted_up": rev.get("voted_up"),
        "timestamp_created": rev.get("timestamp_created"),
        "timestamp_updated": rev.get("timestamp_updated"),
        "votes_up": rev.get("votes_up"),
        "votes_funny": rev.get("votes_funny"),
        "weighted_vote_score": float(wvs) if wvs is not None else None,
        "comment_count": rev.get("comment_count"),
        "steam_purchase": rev.get("steam_purchase"),
        "received_for_free": rev.get("received_for_free"),
        "refunded": rev.get("refunded"),
        "written_during_early_access": rev.get("written_during_early_access"),
        "primarily_steam_deck": rev.get("primarily_steam_deck"),
        "timestamp_dev_responded": rev.get("timestamp_dev_responded"),
        "author_steamid": str(author["steamid"]) if author.get("steamid") is not None else None,
        "num_games_owned": author.get("num_games_owned"),  # Usually absent from GetAppReviews
        "num_reviews": author.get("num_reviews"),
        "playtime_forever": author.get("playtime_forever"),
        "playtime_at_review": author.get("playtime_at_review"),
        "playtime_last_two_weeks": author.get("playtime_last_two_weeks"),
        "deck_playtime_at_review": author.get("deck_playtime_at_review"),
        "last_played": author.get("last_played"),
        "ingest_timestamp": now,
        "source": "steam_live",
    }


def fetch_page(appid, cursor, rate_limiter, max_retries):
    """Return (reviews, next_cursor) or None on a non-retryable error / retries exhausted."""
    params = {"input_json": json.dumps({
        "appid": appid, "filter": 1, "languages": ["all"], "cursor": cursor,
        "num_per_page": 100, "review_type": 0, "purchase_type": 1})}  # recent, all reviews, all purchases
    for attempt in range(max_retries + 1):
        rate_limiter.wait()
        try:
            resp = requests.get(BASE_REVIEW_URL, params=params, timeout=15)
        except requests.RequestException as exc:
            logger.warning("[AppID %s] Request failed (%s), attempt %s", appid, type(exc).__name__, attempt + 1)
            if attempt < max_retries:
                time.sleep(5 * 2**attempt)
            continue
        if resp.status_code == 429:
            if attempt < max_retries:
                rate_limiter.record_rate_limit(cooldown_seconds=60)
            continue
        if resp.status_code != 200:
            logger.error("[AppID %s] HTTP Error %s", appid, resp.status_code)
            return None
        data = resp.json().get("response", {})
        reviews = data.get("reviews", [])
        if not isinstance(reviews, list):
            logger.error("[AppID %s] Invalid reviews list", appid)
            return None
        rate_limiter.record_success()
        return reviews, data.get("cursor")
    logger.error("[AppID %s] Retry limit reached (rate limit or network)", appid)
    return None


def crawl_reviews_for_app(appid, producer, rate_limiter, store, max_pages=10, since_ts=None, max_retries=3):
    """Crawl one game incrementally within max_pages requests. Returns False on an API error.

    since_ts: oldest review creation time to backfill (None = no history limit).
    """
    if max_pages < 1 or max_retries < 0:
        raise ValueError("Invalid review page or retry limit")
    st = store.load(appid)
    queue = []
    if st["newest_ts"] is not None:
        queue.append(("head", segment(stop_ts=st["newest_ts"], stop_ids=st["newest_ids"])))
    queue += [("gap", g) for g in st["gaps"]]
    if since_ts is not None:  # never emit reviews older than the window, whichever segment meets them
        queue = [(k, {**s, "stop_ts": since_ts, "stop_ids": []} if since_ts > s["stop_ts"] else s) for k, s in queue]
    reached = st["backfill_reached_ts"]
    if st["newest_ts"] is None or (not st["history_exhausted"] and (reached is None or (since_ts or 0) < reached)):
        b = st["backfill"] or segment()
        queue.append(("backfill", {**b, "stop_ts": since_ts, "stop_ids": []}))

    pages, emitted_total, ok = 0, 0, True
    done = set()
    seen = {int(ts): set(ids) for ts, ids in st["boundary_ids"].items()}
    try:
        for i, (kind, seg) in enumerate(queue):
            while True:
                if pages >= max_pages:
                    break
                page = fetch_page(appid, seg["cursor"], rate_limiter, max_retries)
                if page is None:
                    ok = False
                    break
                pages += 1
                reviews, next_cursor = page
                if seg["cursor"] == "*" and reviews:
                    top = max(int(r["timestamp_created"]) for r in reviews)
                    ids = [str(r["recommendationid"]) for r in reviews if int(r["timestamp_created"]) == top]
                    if st["newest_ts"] is None or top > st["newest_ts"]:
                        st["newest_ts"], st["newest_ids"] = top, ids
                    elif top == st["newest_ts"]:
                        st["newest_ids"] = sorted(set(st["newest_ids"]) | set(ids))
                covered, reached_stop = filter_page(seg, reviews)
                emit = [r for r in covered
                        if str(r["recommendationid"]) not in seen.get(int(r["timestamp_created"]), ())]
                now = int(time.time())
                for rev in emit:
                    producer.send(topic=KAFKA_TOPIC_REVIEWS, key=str(appid), value=to_record(appid, rev, now))
                producer.flush()
                for rev in emit:  # pruned to boundary seconds on save
                    seen.setdefault(int(rev["timestamp_created"]), set()).add(str(rev["recommendationid"]))
                lower_bound(seg, covered)
                emitted_total += len(emit)
                if emit:
                    oldest = min(int(r["timestamp_created"]) for r in emit)
                    st["oldest_ts"] = oldest if st["oldest_ts"] is None else min(st["oldest_ts"], oldest)
                if reached_stop:
                    if kind == "backfill":
                        # Keep this page's cursor: a longer window later resumes here; upper bound skips repeats.
                        st["backfill_reached_ts"] = since_ts
                    done.add(i)
                    break
                if not reviews or not next_cursor or next_cursor == seg["cursor"]:
                    if kind == "backfill" and seg["cursor"] != "*":  # an empty first page is "no reviews yet"
                        st["history_exhausted"] = True
                    done.add(i)
                    break
                seg["cursor"] = next_cursor
            if i not in done:
                break  # budget or error: later segments keep their saved position
    finally:
        gaps = []
        for i, (kind, seg) in enumerate(queue):
            if kind == "backfill":
                st["backfill"] = {k: seg[k] for k in ("cursor", "upper_ts", "upper_ids")}
            elif i not in done and not (kind == "head" and seg["cursor"] == "*"):
                gaps.append(seg)  # unfinished head/gap; an untouched head is simply redone next run
        st["gaps"] = gaps
        # Segments can only overlap at their boundary seconds; keep emitted ids for exactly those.
        keep = {st["newest_ts"]} | {g[k] for g in gaps for k in ("stop_ts", "upper_ts")}
        if st["backfill"] and not st["history_exhausted"]:
            keep.add(st["backfill"]["upper_ts"])
        st["boundary_ids"] = {str(ts): sorted(ids) for ts, ids in seen.items() if ts in keep}
        st["reviews_emitted"] += emitted_total
        st["updated_at"] = int(time.time())
        store.save(appid, st)
    logger.info("[AppID %s] %s new reviews in %s pages (gaps=%s, backfill_done=%s)", appid, emitted_total,
                pages, len(st["gaps"]), st["history_exhausted"] or st["backfill_reached_ts"] is not None)
    return ok


def since_from_days(days, now=None):
    return None if not days else int((now or time.time()) - days * 86400)


def main():
    producer = create_producer()
    rate_limiter = RateLimiter(default_interval=CRAWLER_RATE_LIMIT_DELAY)
    store = ReviewStateStore(os.path.join(STATE_DIR, REVIEW_STATE_SUBDIR))
    target_apps = load_target_apps(TARGET_APP_LIST_PATH, CRAWLER_TARGET_LIMIT)
    assigned_apps = [a for a in target_apps if is_assigned_to_worker(a, CRAWLER_WORKER_ID, CRAWLER_TOTAL_WORKERS)]
    logger.info("Worker %s/%s handling %s apps", CRAWLER_WORKER_ID, CRAWLER_TOTAL_WORKERS, len(assigned_apps))
    since = since_from_days(CRAWLER_REVIEW_DAYS)
    for appid in assigned_apps:
        crawl_reviews_for_app(appid, producer, rate_limiter, store, max_pages=CRAWLER_REVIEW_BATCHES, since_ts=since)
    producer.close()


if __name__ == "__main__":
    main()

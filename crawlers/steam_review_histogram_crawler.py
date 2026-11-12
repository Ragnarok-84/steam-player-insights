"""Daily review counts per game from the Steam store review histogram (all languages).

store.steampowered.com/appreviewhistogram/{appid} returns up/down counts for each of the last ~30 days
plus monthly or weekly rollups for the whole lifetime. One request per game; B2 keeps the latest snapshot
per (appid, date), so overlapping daily runs do not double count.
"""
import logging
import time

from crawlers.config import (STEAM_STORE_API_BASE, KAFKA_TOPIC_META, CRAWLER_WORKER_ID, CRAWLER_TOTAL_WORKERS,
                             CRAWLER_RATE_LIMIT_DELAY, TARGET_APP_LIST_PATH, CRAWLER_TARGET_LIMIT)
from crawlers.common.rate_limiter import RateLimiter
from crawlers.common.steam_store import get_json
from crawlers.common.targets import load_target_apps, is_assigned_to_worker
from crawlers.common.jsonl_producer import create_producer

logger = logging.getLogger("SteamReviewHistogramCrawler")


def histogram_url(appid):
    return f"{STEAM_STORE_API_BASE}/appreviewhistogram/{int(appid)}"


def _points(rows):
    return [{"date": int(r["date"]), "up": int(r.get("recommendations_up", 0)),
             "down": int(r.get("recommendations_down", 0))} for r in rows or [] if r.get("date") is not None]


def crawl_review_histogram(appid, producer, rate_limiter):
    body = get_json(histogram_url(appid), {"l": "english", "review_score_preference": 0}, rate_limiter, retries=2)
    if body is None or body.get("success") != 1 or not isinstance(body.get("results"), dict):
        logger.warning("[AppID %s] Review histogram unavailable", appid)
        return False
    res = body["results"]
    producer.send(topic=KAFKA_TOPIC_META, key=f"{appid}:histogram", value={  # meta topic is compacted by key
        "appid": int(appid),
        "daily": _points(res.get("recent")),
        "rollups": _points(res.get("rollups")),
        "rollup_type": res.get("rollup_type"),
        "count_all_reviews": body.get("count_all_reviews"),
        "timestamp": int(time.time()),
        "source": "steam_review_histogram",
    })
    return True


def main():
    producer = create_producer()
    limiter = RateLimiter(default_interval=CRAWLER_RATE_LIMIT_DELAY)
    apps = [a for a in load_target_apps(TARGET_APP_LIST_PATH, CRAWLER_TARGET_LIMIT)
            if is_assigned_to_worker(a, CRAWLER_WORKER_ID, CRAWLER_TOTAL_WORKERS)]
    for appid in apps:
        crawl_review_histogram(appid, producer, limiter)
    producer.close()


if __name__ == "__main__":
    main()

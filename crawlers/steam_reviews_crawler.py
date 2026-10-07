import time
import json
import os
import requests
import logging
from crawlers.config import (
    KAFKA_TOPIC_REVIEWS,
    CRAWLER_WORKER_ID,
    CRAWLER_TOTAL_WORKERS,
    CRAWLER_RATE_LIMIT_DELAY,
    STATE_DIR,
    TARGET_APP_LIST_PATH,
    CRAWLER_TARGET_LIMIT,
    CRAWLER_REVIEW_BATCHES,
)
from crawlers.common.kafka_producer import SteamKafkaProducer
from crawlers.common.rate_limiter import RateLimiter
from crawlers.common.cursor_manager import CursorManager
from crawlers.common.targets import load_target_apps, is_assigned_to_worker
from crawlers.common.jsonl_producer import create_producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("SteamReviewsCrawler")

BASE_REVIEW_URL = "https://api.steampowered.com/IUserReviewsService/GetAppReviews/v1/"

def crawl_reviews_for_app(appid: int, producer: SteamKafkaProducer, rate_limiter: RateLimiter, cursor_mgr: CursorManager, max_batches: int = 50, max_retries: int = 3):
    if max_batches < 1 or max_retries < 0:
        raise ValueError("Invalid review page or retry limit")
    cursor = cursor_mgr.get_cursor(appid)
    logger.info(f"[AppID {appid}] Starting crawl from cursor: {cursor}")

    batch_count = 0
    total_reviews_fetched = 0
    retry_count = 0
    succeeded = True

    while batch_count < max_batches:
        rate_limiter.wait()
        params = {
            "input_json": json.dumps({
                "appid": appid,
                "filter": 1,  # Recent
                "languages": ["all"],
                "cursor": cursor,
                "num_per_page": 100,
                "review_type": 0,  # All
                "purchase_type": 1,  # All
            }),
        }

        try:
            resp = requests.get(BASE_REVIEW_URL, params=params, timeout=15)
            if resp.status_code == 429:
                if retry_count >= max_retries:
                    logger.error(f"[AppID {appid}] Rate limit retry limit reached")
                    succeeded = False
                    break
                retry_count += 1
                rate_limiter.record_rate_limit(cooldown_seconds=60)
                continue
            elif resp.status_code != 200:
                logger.error(f"[AppID {appid}] HTTP Error {resp.status_code}")
                succeeded = False
                break

            data = resp.json()["response"]
            reviews = data.get("reviews", [])
            if not isinstance(reviews, list):
                raise ValueError("Reviews API returned an invalid reviews list")
            new_cursor = data.get("cursor")
            rate_limiter.record_success()
            retry_count = 0

            if not reviews:
                logger.info(f"[AppID {appid}] No more reviews returned.")
                break

            for rev in reviews:
                author = rev.get("author", {})
                record = {
                    "recommendationid": str(rev["recommendationid"]),
                    "appid": appid,
                    "language": rev.get("language"),
                    "review": rev.get("review"),
                    "voted_up": rev.get("voted_up"),
                    "timestamp_created": rev.get("timestamp_created"),
                    "timestamp_updated": rev.get("timestamp_updated"),
                    "votes_up": rev.get("votes_up"),
                    "votes_funny": rev.get("votes_funny"),
                    "weighted_vote_score": float(rev["weighted_vote_score"]) if rev.get("weighted_vote_score") is not None else None,
                    "steam_purchase": rev.get("steam_purchase"),
                    "received_for_free": rev.get("received_for_free"),
                    "written_during_early_access": rev.get("written_during_early_access"),
                    # Thông tin tác giả review
                    "author_steamid": str(author["steamid"]) if author.get("steamid") is not None else None,
                    "num_games_owned": None,  # No longer returned by GetAppReviews.
                    "num_reviews": author.get("num_reviews"),
                    "playtime_forever": author.get("playtime_forever"),
                    "playtime_at_review": author.get("playtime_at_review"),
                    "playtime_last_two_weeks": author.get("playtime_last_two_weeks"),
                    "ingest_timestamp": int(time.time()),
                    "source": "steam_live",
                }
                producer.send(topic=KAFKA_TOPIC_REVIEWS, key=str(appid), value=record)

            producer.flush()
            total_reviews_fetched += len(reviews)
            batch_count += 1

            if new_cursor:
                cursor_mgr.save_cursor(appid, new_cursor, last_updated=int(time.time()))
                if new_cursor == cursor:
                    # Cursor không đổi -> đã hết trang
                    break
                cursor = new_cursor
            else:
                break

        except Exception as e:
            logger.error(f"[AppID {appid}] Exception during crawl: {e}")
            succeeded = False
            break

    logger.info(f"[AppID {appid}] Completed. Total reviews fetched this run: {total_reviews_fetched}")
    return succeeded

def main():
    producer = create_producer()
    rate_limiter = RateLimiter(default_interval=CRAWLER_RATE_LIMIT_DELAY)
    # Keep the new API's cursors separate from legacy /appreviews cursors.
    cursor_mgr = CursorManager(state_dir=os.path.join(STATE_DIR, "reviews_v1"))

    target_apps = load_target_apps(TARGET_APP_LIST_PATH, CRAWLER_TARGET_LIMIT)
    
    assigned_apps = [
        app for app in target_apps 
        if is_assigned_to_worker(app, CRAWLER_WORKER_ID, CRAWLER_TOTAL_WORKERS)
    ]
    logger.info(f"Worker {CRAWLER_WORKER_ID}/{CRAWLER_TOTAL_WORKERS} handling apps: {assigned_apps}")

    for appid in assigned_apps:
        crawl_reviews_for_app(appid, producer, rate_limiter, cursor_mgr, max_batches=CRAWLER_REVIEW_BATCHES)

    producer.close()

if __name__ == "__main__":
    main()

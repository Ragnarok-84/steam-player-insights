import time
import urllib.parse
import requests
import logging
from crawlers.config import (
    KAFKA_TOPIC_REVIEWS,
    CRAWLER_WORKER_ID,
    CRAWLER_TOTAL_WORKERS,
    CRAWLER_RATE_LIMIT_DELAY,
)
from crawlers.common.kafka_producer import SteamKafkaProducer
from crawlers.common.rate_limiter import RateLimiter
from crawlers.common.cursor_manager import CursorManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("SteamReviewsCrawler")

BASE_REVIEW_URL = "https://store.steampowered.com/appreviews/{appid}?json=1"

def is_assigned_to_worker(appid: int, worker_id: int, total_workers: int) -> bool:
    """Phân chia danh sách game cho worker theo hash(appid)"""
    return (hash(str(appid)) % total_workers) == worker_id

def crawl_reviews_for_app(appid: int, producer: SteamKafkaProducer, rate_limiter: RateLimiter, cursor_mgr: CursorManager, max_batches: int = 50):
    cursor = cursor_mgr.get_cursor(appid)
    logger.info(f"[AppID {appid}] Starting crawl from cursor: {cursor}")

    batch_count = 0
    total_reviews_fetched = 0

    while batch_count < max_batches:
        rate_limiter.wait()
        url = BASE_REVIEW_URL.format(appid=appid)
        params = {
            "filter": "recent",
            "language": "all",
            "cursor": cursor,
            "num_per_page": 100,
            "purchase_type": "all",
        }

        try:
            resp = requests.get(url, params=params, timeout=15)
            if resp.status_code == 429:
                rate_limiter.record_rate_limit(cooldown_seconds=60)
                continue
            elif resp.status_code != 200:
                logger.error(f"[AppID {appid}] HTTP Error {resp.status_code}")
                break

            data = resp.json()
            rate_limiter.record_success()

            reviews = data.get("reviews", [])
            new_cursor = data.get("cursor")

            if not reviews:
                logger.info(f"[AppID {appid}] No more reviews returned.")
                break

            for rev in reviews:
                author = rev.get("author", {})
                record = {
                    "recommendationid": rev.get("recommendationid"),
                    "appid": appid,
                    "language": rev.get("language"),
                    "review": rev.get("review"),
                    "voted_up": rev.get("voted_up"),
                    "timestamp_created": rev.get("timestamp_created"),
                    "timestamp_updated": rev.get("timestamp_updated"),
                    "votes_up": rev.get("votes_up"),
                    "votes_funny": rev.get("votes_funny"),
                    "weighted_vote_score": rev.get("weighted_vote_score"),
                    "steam_purchase": rev.get("steam_purchase"),
                    "received_for_free": rev.get("received_for_free"),
                    "written_during_early_access": rev.get("written_during_early_access"),
                    # Thông tin tác giả review
                    "author_steamid": author.get("steamid"),
                    "num_games_owned": author.get("num_games_owned"),
                    "num_reviews": author.get("num_reviews"),
                    "playtime_forever": author.get("playtime_forever"),
                    "playtime_at_review": author.get("playtime_at_review"),
                    "playtime_last_two_weeks": author.get("playtime_last_two_weeks"),
                    "ingest_timestamp": int(time.time()),
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
            break

    logger.info(f"[AppID {appid}] Completed. Total reviews fetched this run: {total_reviews_fetched}")

def main():
    producer = SteamKafkaProducer()
    rate_limiter = RateLimiter(default_interval=CRAWLER_RATE_LIMIT_DELAY)
    cursor_mgr = CursorManager()

    # Danh sách game mẫu phổ biến (sẽ mở rộng đọc từ file target_apps.json)
    target_apps = [730, 570, 1086940, 1091500, 271590, 1172470, 252490, 359550]
    
    assigned_apps = [
        app for app in target_apps 
        if is_assigned_to_worker(app, CRAWLER_WORKER_ID, CRAWLER_TOTAL_WORKERS)
    ]
    logger.info(f"Worker {CRAWLER_WORKER_ID}/{CRAWLER_TOTAL_WORKERS} handling apps: {assigned_apps}")

    for appid in assigned_apps:
        crawl_reviews_for_app(appid, producer, rate_limiter, cursor_mgr)

    producer.close()

if __name__ == "__main__":
    main()

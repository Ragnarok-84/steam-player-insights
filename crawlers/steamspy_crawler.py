import time
import requests
import logging
from crawlers.config import (
    KAFKA_TOPIC_META,
    CRAWLER_WORKER_ID,
    CRAWLER_TOTAL_WORKERS,
    CRAWLER_RATE_LIMIT_DELAY,
    TARGET_APP_LIST_PATH,
    CRAWLER_TARGET_LIMIT,
)
from crawlers.common.kafka_producer import SteamKafkaProducer
from crawlers.common.rate_limiter import RateLimiter
from crawlers.common.targets import load_target_apps, is_assigned_to_worker
from crawlers.common.jsonl_producer import create_producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("SteamSpyCrawler")

STEAM_SPY_URL = "https://steamspy.com/api.php"

def crawl_steamspy(appid: int, producer: SteamKafkaProducer, rate_limiter: RateLimiter):
    rate_limiter.wait()
    params = {"request": "appdetails", "appid": appid}
    try:
        resp = requests.get(STEAM_SPY_URL, params=params, timeout=15)
        if resp.status_code == 429:
            rate_limiter.record_rate_limit(cooldown_seconds=60)
            return False
        elif resp.status_code != 200:
            logger.warning(f"[AppID {appid}] SteamSpy returned status {resp.status_code}")
            return False

        data = resp.json()
        if not data or not data.get("name"):
            logger.warning(f"[AppID {appid}] No valid SteamSpy data returned.")
            return False

        record = {
            "appid": appid,
            "name": data.get("name"),
            "developer": data.get("developer"),
            "publisher": data.get("publisher"),
            "positive_reviews_total": data.get("positive", 0),
            "negative_reviews_total": data.get("negative", 0),
            "owners_range": data.get("owners", ""),
            "average_forever": data.get("average_forever", 0),
            "average_2weeks": data.get("average_2weeks", 0),
            "median_forever": data.get("median_forever", 0),
            "median_2weeks": data.get("median_2weeks", 0),
            "tags": data.get("tags", {}),
            "source": "steamspy",
            "timestamp": int(time.time()),
        }

        producer.send(topic=KAFKA_TOPIC_META, key=str(appid), value=record)
        rate_limiter.record_success()
        logger.info(f"[AppID {appid}] SteamSpy tags & owners saved.")
        return True

    except Exception as e:
        logger.error(f"[AppID {appid}] Error fetching SteamSpy: {e}")
        return False

def main():
    producer = create_producer()
    rate_limiter = RateLimiter(default_interval=1.0)
    target_apps = load_target_apps(TARGET_APP_LIST_PATH, CRAWLER_TARGET_LIMIT)

    assigned_apps = [
        app for app in target_apps 
        if is_assigned_to_worker(app, CRAWLER_WORKER_ID, CRAWLER_TOTAL_WORKERS)
    ]
    logger.info(f"Worker {CRAWLER_WORKER_ID} crawling SteamSpy for {len(assigned_apps)} apps")

    for appid in assigned_apps:
        crawl_steamspy(appid, producer, rate_limiter)

    producer.flush()
    producer.close()

if __name__ == "__main__":
    main()

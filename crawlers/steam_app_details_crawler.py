import time
import requests
import logging
from crawlers.config import (
    KAFKA_TOPIC_META,
    CRAWLER_WORKER_ID,
    CRAWLER_TOTAL_WORKERS,
    CRAWLER_RATE_LIMIT_DELAY,
)
from crawlers.common.kafka_producer import SteamKafkaProducer
from crawlers.common.rate_limiter import RateLimiter

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("SteamAppDetailsCrawler")

BASE_APPDETAILS_URL = "https://store.steampowered.com/api/appdetails"

def is_assigned_to_worker(appid: int, worker_id: int, total_workers: int) -> bool:
    return (hash(str(appid)) % total_workers) == worker_id

def crawl_app_details(appid: int, producer: SteamKafkaProducer, rate_limiter: RateLimiter):
    rate_limiter.wait()
    params = {"appids": appid, "cc": "us", "l": "en"}
    try:
        resp = requests.get(BASE_APPDETAILS_URL, params=params, timeout=15)
        if resp.status_code == 429:
            rate_limiter.record_rate_limit(cooldown_seconds=60)
            return False
        elif resp.status_code != 200:
            logger.error(f"[AppID {appid}] HTTP Error {resp.status_code}")
            return False

        data = resp.json()
        rate_limiter.record_success()

        app_info = data.get(str(appid), {})
        if not app_info.get("success"):
            logger.warning(f"[AppID {appid}] API returned success=False (region lock, removed, or invalid)")
            return False

        details = app_info.get("data", {})
        price_overview = details.get("price_overview", {})
        release_date = details.get("release_date", {})
        metacritic = details.get("metacritic", {})

        record = {
            "appid": appid,
            "name": details.get("name"),
            "type": details.get("type"),
            "is_free": details.get("is_free", False),
            "developer": details.get("developers", []),
            "publisher": details.get("publishers", []),
            "genres": [g.get("description") for g in details.get("genres", [])],
            "categories": [c.get("description") for c in details.get("categories", [])],
            "price": price_overview.get("final", 0) / 100.0 if price_overview else 0.0,
            "initial_price": price_overview.get("initial", 0) / 100.0 if price_overview else 0.0,
            "discount_percent": price_overview.get("discount_percent", 0) if price_overview else 0,
            "currency": price_overview.get("currency", "USD") if price_overview else "USD",
            "release_date": release_date.get("date"),
            "platforms": details.get("platforms", {}),
            "metacritic_score": metacritic.get("score"),
            "required_age": details.get("required_age", 0),
            "timestamp": int(time.time()),
        }

        producer.send(topic=KAFKA_TOPIC_META, key=str(appid), value=record)
        producer.flush()
        logger.info(f"[AppID {appid}] Successfully fetched metadata: {record['name']}")
        return True

    except Exception as e:
        logger.error(f"[AppID {appid}] Error fetching details: {e}")
        return False

def main():
    producer = SteamKafkaProducer()
    rate_limiter = RateLimiter(default_interval=CRAWLER_RATE_LIMIT_DELAY)
    target_apps = [730, 570, 1086940, 1091500, 271590, 1172470, 252490, 359550]

    assigned_apps = [
        app for app in target_apps 
        if is_assigned_to_worker(app, CRAWLER_WORKER_ID, CRAWLER_TOTAL_WORKERS)
    ]
    logger.info(f"Worker {CRAWLER_WORKER_ID} crawling metadata for {len(assigned_apps)} apps")

    for appid in assigned_apps:
        crawl_app_details(appid, producer, rate_limiter)

    producer.close()

if __name__ == "__main__":
    main()

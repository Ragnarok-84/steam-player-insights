import re
import time
import requests
import logging
from crawlers.config import (
    STEAM_STORE_API_BASE,
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
logger = logging.getLogger("SteamAppDetailsCrawler")

BASE_APPDETAILS_URL = f"{STEAM_STORE_API_BASE}/api/appdetails"


def _int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _language_count(html):
    # "English<strong>*</strong>, French, German<br>..." -> 3
    if not html:
        return 0
    text = re.sub(r"<br>.*$", "", html, flags=re.S)
    text = re.sub(r"<[^>]+>|\*", "", text)
    return len([p for p in text.split(",") if p.strip()])

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
            "coming_soon": release_date.get("coming_soon", False),
            "platforms": details.get("platforms", {}),
            "metacritic_score": metacritic.get("score"),
            "required_age": _int(details.get("required_age")),
            "controller_support": details.get("controller_support"),
            "dlc_count": len(details.get("dlc") or []),
            "achievements_total": (details.get("achievements") or {}).get("total", 0),
            "recommendations_total": (details.get("recommendations") or {}).get("total"),
            "supported_languages_count": _language_count(details.get("supported_languages")),
            "content_descriptor_ids": (details.get("content_descriptors") or {}).get("ids") or [],
            "short_description": details.get("short_description"),
            "timestamp": int(time.time()),
            "source": "steam_live",
        }

        producer.send(topic=KAFKA_TOPIC_META, key=str(appid), value=record)
        logger.info(f"[AppID {appid}] Successfully fetched metadata: {record['name']}")
        return True

    except Exception as e:
        logger.error(f"[AppID {appid}] Error fetching details: {e}")
        return False

def main():
    producer = create_producer()
    rate_limiter = RateLimiter(default_interval=CRAWLER_RATE_LIMIT_DELAY)
    target_apps = load_target_apps(TARGET_APP_LIST_PATH, CRAWLER_TARGET_LIMIT)

    assigned_apps = [
        app for app in target_apps 
        if is_assigned_to_worker(app, CRAWLER_WORKER_ID, CRAWLER_TOTAL_WORKERS)
    ]
    logger.info(f"Worker {CRAWLER_WORKER_ID} crawling metadata for {len(assigned_apps)} apps")

    for appid in assigned_apps:
        crawl_app_details(appid, producer, rate_limiter)

    if hasattr(producer, "flush_all"):
        producer.flush_all()
    producer.close()

if __name__ == "__main__":
    main()

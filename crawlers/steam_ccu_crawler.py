import time
import requests
import logging
from crawlers.config import (
    KAFKA_TOPIC_CCU,
    CRAWLER_WORKER_ID,
    CRAWLER_TOTAL_WORKERS,
    CRAWLER_RATE_LIMIT_DELAY,
)
from crawlers.common.kafka_producer import SteamKafkaProducer
from crawlers.common.rate_limiter import RateLimiter

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("SteamCCUCrawler")

CCU_API_URL = "https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/"

def is_assigned_to_worker(appid: int, worker_id: int, total_workers: int) -> bool:
    return (hash(str(appid)) % total_workers) == worker_id

def crawl_ccu(appid: int, producer: SteamKafkaProducer, rate_limiter: RateLimiter):
    rate_limiter.wait()
    params = {"appid": appid}
    try:
        resp = requests.get(CCU_API_URL, params=params, timeout=10)
        if resp.status_code == 429:
            rate_limiter.record_rate_limit(cooldown_seconds=30)
            return
        elif resp.status_code != 200:
            logger.warning(f"[AppID {appid}] CCU API returned status {resp.status_code}")
            return

        data = resp.json().get("response", {})
        result = data.get("result")
        player_count = data.get("player_count", 0)

        if result == 1:
            record = {
                "appid": appid,
                "timestamp": int(time.time()),
                "player_count": player_count,
            }
            producer.send(topic=KAFKA_TOPIC_CCU, key=str(appid), value=record)
            logger.info(f"[AppID {appid}] CCU: {player_count}")
        else:
            logger.warning(f"[AppID {appid}] Unable to fetch CCU (result != 1)")

    except Exception as e:
        logger.error(f"[AppID {appid}] Error fetching CCU: {e}")

def main():
    producer = SteamKafkaProducer()
    rate_limiter = RateLimiter(default_interval=0.5) # CCU API nhẹ hơn
    target_apps = [730, 570, 1086940, 1091500, 271590, 1172470, 252490, 359550]

    assigned_apps = [
        app for app in target_apps 
        if is_assigned_to_worker(app, CRAWLER_WORKER_ID, CRAWLER_TOTAL_WORKERS)
    ]
    logger.info(f"Worker {CRAWLER_WORKER_ID} crawling CCU for {len(assigned_apps)} apps")

    for appid in assigned_apps:
        crawl_ccu(appid, producer, rate_limiter)

    producer.flush()
    producer.close()

if __name__ == "__main__":
    main()

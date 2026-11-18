"""Publish Steam Store Browse snapshots for the configured targets."""
import logging
import time

from crawlers.common import steam_store
from crawlers.common.jsonl_producer import create_producer
from crawlers.common.rate_limiter import RateLimiter
from crawlers.common.targets import is_assigned_to_worker, load_target_apps
from crawlers.config import (
    CRAWLER_RATE_LIMIT_DELAY, CRAWLER_TARGET_LIMIT, CRAWLER_TOTAL_WORKERS,
    CRAWLER_WORKER_ID, KAFKA_TOPIC_META, TARGET_APP_LIST_PATH,
)


def collect_store(apps, producer, limiter):
    """Return the appids missing from a batch of Store Browse snapshots."""
    tag_names = steam_store.fetch_tag_names(limiter)
    items = steam_store.fetch_store_items(apps, limiter)
    now = time.time()
    missing = []
    pending = []
    for appid in apps:
        item = items.get(appid)
        if not item or item.get("success") != 1:
            missing.append(appid)
            continue
        # Keep store, appdetails and histogram distinct in the compacted Kafka topic.
        result = producer.send(KAFKA_TOPIC_META, f"{appid}:store", steam_store.parse_store_item(item, tag_names, now))
        if result is not None:
            pending.append(result)
    if hasattr(producer, "flush_all"):
        producer.flush_all()
    else:
        producer.flush()
    for result in pending:
        result.get(timeout=30)
    return missing


def main():
    logging.basicConfig(level=logging.INFO)
    apps = [
        appid for appid in load_target_apps(TARGET_APP_LIST_PATH, CRAWLER_TARGET_LIMIT)
        if is_assigned_to_worker(appid, CRAWLER_WORKER_ID, CRAWLER_TOTAL_WORKERS)
    ]
    if not apps:
        return
    producer = create_producer()
    try:
        missing = collect_store(apps, producer, RateLimiter(CRAWLER_RATE_LIMIT_DELAY))
    finally:
        producer.close()
    logging.info("Store snapshots published for %d/%d targets", len(apps) - len(missing), len(apps))
    if len(missing) == len(apps):
        raise SystemExit("Steam returned no usable store snapshots")
    if missing:
        logging.warning("Missing store snapshots for appids %s", missing)


if __name__ == "__main__":
    main()

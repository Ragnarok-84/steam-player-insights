import os
import csv
import json
import time
import argparse
import logging
from crawlers.config import KAFKA_TOPIC_REVIEWS
from crawlers.common.kafka_producer import SteamKafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("KaggleReplayProducer")

def replay_csv(file_path: str, producer: SteamKafkaProducer, target_rate_per_sec: int = 500, max_records: int = None):
    """
    Replay tập dữ liệu CSV đánh giá Steam vào topic Kafka để backfill hoặc kiểm thử tải.
    """
    if not os.path.exists(file_path):
        logger.error(f"Dataset file not found: {file_path}")
        return

    logger.info(f"Starting replay from {file_path} at target rate: {target_rate_per_sec} msgs/sec")
    count = 0
    start_time = time.time()

    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        reader = csv.DictReader(f)
        for row in reader:
            appid = row.get("app_id") or row.get("appid") or "0"
            rec_id = row.get("review_id") or row.get("recommendationid") or str(count)

            record = {
                "recommendationid": rec_id,
                "appid": int(appid),
                "language": row.get("language", "english"),
                "review": row.get("review_text") or row.get("review", ""),
                "voted_up": row.get("review_score", "1") in ["1", "True", "true", "True"],
                "timestamp_created": int(row.get("timestamp_created") or time.time()),
                "timestamp_updated": int(row.get("timestamp_updated") or time.time()),
                "votes_up": int(row.get("votes_up") or 0),
                "votes_funny": int(row.get("votes_funny") or 0),
                "weighted_vote_score": float(row.get("weighted_vote_score") or 0.0),
                "steam_purchase": True,
                "received_for_free": False,
                "written_during_early_access": False,
                "author_steamid": row.get("author_steamid", "0"),
                "num_games_owned": int(row.get("author_num_games_owned") or 0),
                "num_reviews": int(row.get("author_num_reviews") or 0),
                "playtime_forever": float(row.get("author_playtime_forever") or 0.0),
                "playtime_at_review": float(row.get("author_playtime_at_review") or 0.0),
                "playtime_last_two_weeks": float(row.get("author_playtime_last_two_weeks") or 0.0),
                "is_replay": True,
                "replay_timestamp": int(time.time()),
            }

            producer.send(topic=KAFKA_TOPIC_REVIEWS, key=str(appid), value=record)
            count += 1

            if target_rate_per_sec > 0 and count % target_rate_per_sec == 0:
                producer.flush()
                time.sleep(1.0)
                logger.info(f"Replayed {count} messages. Elapsed: {time.time() - start_time:.2f}s")

            if max_records and count >= max_records:
                logger.info(f"Reached max records limit: {max_records}")
                break

    producer.flush()
    logger.info(f"Completed replay! Total: {count} messages sent.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Replay Kaggle reviews into Kafka")
    parser.add_argument("--file", type=str, default="data/kaggle_reviews_sample.csv", help="Path to CSV dataset")
    parser.add_argument("--rate", type=int, default=200, help="Messages per second")
    parser.add_argument("--limit", type=int, default=10000, help="Maximum records to send")
    args = parser.parse_args()

    producer = SteamKafkaProducer()
    replay_csv(args.file, producer, target_rate_per_sec=args.rate, max_records=args.limit)
    producer.close()

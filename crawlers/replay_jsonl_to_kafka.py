"""Replay dữ liệu đã thu thập (data/raw/steam_*/dt=*/part-*.jsonl.gz) vào Kafka.

Dùng để: nạp dữ liệu crawl local / backfill lịch sử vào Kafka cho B1 và speed layer; tạo tải cho SC3.
--retime: dời timestamp sự kiện về thời điểm hiện tại để speed layer xử lý như dữ liệu mới.
"""
import argparse
import gzip
import json
import logging
import time
from pathlib import Path

from crawlers.common.kafka_producer import SteamKafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("ReplayJsonl")

TOPICS = {
    "steam_reviews_raw": ("steam.reviews.raw", "timestamp_created"),
    "steam_players_ccu": ("steam.players.ccu", "timestamp"),
    "steam_games_meta": ("steam.games.meta", "timestamp"),
}


def iter_files(raw_root: Path, folder: str):
    # cả data/raw/<folder> và data/raw/<sub>/<folder> (retry-*); bỏ file ẩn/.tmp
    for base in [raw_root / folder, *raw_root.glob(f"*/{folder}")]:
        for f in sorted(base.rglob("*.jsonl*")):
            if not f.name.startswith("."):
                yield f


def iter_records(path: Path):
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                yield json.loads(line)


def replay(producer, raw_root: Path, folders, rate: int, limit: int, retime: bool):
    sent, started = 0, time.time()
    for folder in folders:
        topic, ts_field = TOPICS[folder]
        for path in iter_files(raw_root, folder):
            for rec in iter_records(path):
                if retime:
                    now = int(time.time())
                    if folder == "steam_reviews_raw":
                        rec["timestamp_updated"] = now
                    rec[ts_field] = now
                    rec["is_replay"] = True
                producer.send(topic, key=str(rec.get("appid")), value=rec)
                sent += 1
                if rate > 0 and sent % rate == 0:
                    producer.flush()
                    elapsed = time.time() - started
                    target = sent / rate
                    if target > elapsed:
                        time.sleep(target - elapsed)
                if sent % 10000 == 0:
                    logger.info("Sent %s records (%.0f rec/s)", sent, sent / max(time.time() - started, 1e-6))
                if limit and sent >= limit:
                    producer.flush()
                    return sent
    producer.flush()
    return sent


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw", default="data/raw")
    parser.add_argument("--topics", nargs="+", choices=list(TOPICS), default=list(TOPICS))
    parser.add_argument("--rate", type=int, default=1000, help="bản ghi/giây; 0 = không giới hạn")
    parser.add_argument("--limit", type=int, default=0, help="0 = tất cả")
    parser.add_argument("--retime", action="store_true")
    args = parser.parse_args()
    producer = SteamKafkaProducer()
    n = replay(producer, Path(args.raw), args.topics, args.rate, args.limit, args.retime)
    producer.close()
    logger.info("Done: %s records", n)

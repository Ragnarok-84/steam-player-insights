"""Chạy pipeline Steam Player Insights (Windows/Linux/WSL, không cần make).

Ví dụ:
  python pipeline.py batch                       # B2 -> B5 -> B3 -> B4 -> B6 -> B7 -> B8 trên data/raw
  python pipeline.py batch --raw D:/steam_data/raw
  python pipeline.py s1 --source file            # speed layer đọc file JSONL (không cần Kafka)
  python pipeline.py s2 -- --source kafka        # tham số sau `--` chuyển thẳng cho job
  python pipeline.py --profile cluster batch     # dùng đường dẫn HDFS/ES từ .env

Profile local: raw = data/raw (output crawlers.collect_local), clean/curated/checkpoints = data/lake/*.
Profile cluster: giữ nguyên biến môi trường (.env): HDFS_NAMENODE_URL, HDFS_*_PATH, ELASTICSEARCH_HOSTS...
"""
import argparse
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent

JOBS = {
    "b1": "processing.batch.b1_ingest_raw",
    "b2": "processing.batch.b2_clean_raw",
    "b3": "processing.batch.b3_game_daily_stats",
    "b4": "processing.batch.b4_playtime_buckets",
    "b5": "processing.batch.b5_aspect_sentiment",
    "b6": "processing.batch.b6_trending_games",
    "b7": "processing.batch.b7_baselines",
    "b8": "processing.batch.b8_game_clusters",
    "s1": "processing.streaming.s1_review_metrics",
    "s2": "processing.streaming.s2_review_bombing_detector",
    "s3": "processing.streaming.s3_ccu_spike_detector",
    "merge": "processing.serving.merge_views",
    "es-setup": "storage.elasticsearch.setup_indices",
    "kibana-setup": "dashboards.setup_kibana",
    "replay": "crawlers.replay_jsonl_to_kafka",
    "feedback": "crawlers.update_targets_from_trending",
    "applist": "crawlers.steam_applist_crawler",
    "crawl": "crawlers.collect_local",
}
# B5 trước B3 (B3 dùng cảm xúc từ B5); B7 sau B3 (baseline cho S2/S3)
BATCH_ORDER = ["b2", "b5", "b3", "b4", "b6", "b7", "b8"]


def configure(profile: str, raw: str, lake: str):
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    env["PYTHONPATH"] = os.pathsep.join(filter(None, [str(ROOT), env.get("PYTHONPATH")]))
    # Executor Python phải trùng driver (tránh lỗi khác phiên bản trên Windows/venv)
    env.setdefault("PYSPARK_PYTHON", sys.executable)
    env.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    if profile == "local":
        lake_path = Path(lake).resolve()
        env.update({
            "HDFS_NAMENODE_URL": "",
            "HIVE_METASTORE_URI": env.get("HIVE_METASTORE_URI_LOCAL", ""),
            "HDFS_RAW_PATH": Path(raw).resolve().as_uri(),
            "HDFS_CLEAN_PATH": (lake_path / "clean").as_uri(),
            "HDFS_CURATED_PATH": (lake_path / "curated").as_uri(),
            "HDFS_CHECKPOINT_PATH": (lake_path / "checkpoints").as_uri(),
            "SPARK_MASTER": env.get("SPARK_MASTER") or "local[*]",
        })
    return env


def run(module: str, args, env) -> int:
    cmd = [sys.executable, "-m", module, *args]
    print("+", " ".join(cmd), flush=True)
    return subprocess.call(cmd, cwd=ROOT, env=env)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=["batch", *JOBS])
    parser.add_argument("--profile", choices=["local", "cluster"], default=os.getenv("PIPELINE_PROFILE", "local"))
    parser.add_argument("--raw", default=os.getenv("RAW_DIR", "data/raw"), help="Thư mục raw (profile local)")
    parser.add_argument("--lake", default=os.getenv("LAKE_DIR", "data/lake"), help="Thư mục clean/curated (profile local)")
    parser.add_argument("--es", action="store_true", help="Ghi Elasticsearch (WRITE_ES=1) và hợp nhất view sau batch")
    parser.add_argument("--skip", nargs="*", default=[], help="Bỏ qua job trong batch, vd. --skip b8")
    args, extra = parser.parse_known_args()
    extra = [x for x in extra if x != "--"]

    env = configure(args.profile, args.raw, args.lake)
    if args.es:
        env["WRITE_ES"] = "1"

    if args.command == "batch":
        for job in BATCH_ORDER:
            if job in args.skip:
                continue
            code = run(JOBS[job], [], env)
            if code != 0:
                sys.exit(f"{job} failed with exit code {code}")
        if args.es:
            sys.exit(run(JOBS["merge"], [], env))
        return
    sys.exit(run(JOBS[args.command], extra, env))


if __name__ == "__main__":
    main()

"""SC1 & SC2: khả năng mở rộng batch layer.

SC1: cố định dữ liệu, tăng số executor (1, 2, 4, 6)  -> thời gian chạy, speedup.
SC2: cố định executor, tăng khối lượng dữ liệu (1x, 2x, 5x, 10x) -> thời gian theo kích thước.

Chế độ chạy (--mode):
  local : "executor" = số core Spark local (local[N]); hợp để thử trên 1 máy/Docker.
  k8s   : chạy trong pod có spark-defaults (k8s/common/pipeline-config.yaml); tắt dynamic allocation và
          đặt spark.executor.instances=N.
Dữ liệu SC2: sinh bằng tests/make_sample_data.py với số game = base_games x scale (cùng số ngày),
hoặc dùng --raw-template "data/scale_{scale}x" nếu đã chuẩn bị sẵn các bản dữ liệu thật theo tỷ lệ.
Kết quả: tests/results/sc1_sc2_<timestamp>.csv
"""
import argparse
import csv
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
JOBS = ["b2", "b5", "b3", "b4"]


def run_job(job, raw, lake, mode, executors, extra_env):
    env = os.environ.copy()
    env.update(extra_env)
    if mode == "local":
        env["SPARK_MASTER"] = f"local[{executors}]"
        env["SPARK_SHUFFLE_PARTITIONS"] = str(max(4, executors * 4))
        profile = ["--profile", "local", "--raw", raw, "--lake", lake]
    else:
        env["SPARK_EXTRA_CONF"] = (f"spark.dynamicAllocation.enabled=false;spark.executor.instances={executors}")
        env["SPARK_SHUFFLE_PARTITIONS"] = str(max(4, executors * 4))
        env["HDFS_CLEAN_PATH"] = f"{lake}/clean"
        env["HDFS_CURATED_PATH"] = f"{lake}/curated"
        env["HDFS_RAW_PATH"] = raw
        profile = ["--profile", "cluster"]
    start = time.time()
    code = subprocess.call([sys.executable, "pipeline.py", *profile, job], cwd=ROOT, env=env,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return round(time.time() - start, 2), code


def ensure_sample(scale, base_games, days):
    out = ROOT / "data" / "bench" / f"raw_{scale}x"
    if not (out / "steam_reviews_raw").exists():
        subprocess.check_call([sys.executable, "tests/make_sample_data.py", "--out", str(out),
                               "--games", str(base_games * scale), "--days", str(days)], cwd=ROOT)
    return str(out.relative_to(ROOT)).replace("\\", "/")


def count_reviews(raw):
    import gzip
    return sum(1 for f in (ROOT / raw / "steam_reviews_raw").rglob("*.jsonl.gz") for _ in gzip.open(f, "rt"))


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--scenario", choices=["sc1", "sc2", "both"], default="both")
    p.add_argument("--mode", choices=["local", "k8s"], default="local")
    p.add_argument("--executors", nargs="+", type=int, default=[1, 2, 4, 6])
    p.add_argument("--scales", nargs="+", type=int, default=[1, 2, 5, 10])
    p.add_argument("--sc2-executors", type=int, default=4)
    p.add_argument("--sc1-scale", type=int, default=5)
    p.add_argument("--base-games", type=int, default=30)
    p.add_argument("--days", type=int, default=30)
    p.add_argument("--raw-template", default=None, help='vd. "/steam/bench/raw_{scale}x" (bỏ qua bộ sinh dữ liệu)')
    p.add_argument("--lake-root", default="data/bench", help="Thư mục clean/curated cho benchmark (k8s: đường dẫn HDFS)")
    p.add_argument("--repeat", type=int, default=1)
    args = p.parse_args()

    plan = []
    if args.scenario in ("sc1", "both"):
        plan += [("SC1", args.sc1_scale, e) for e in args.executors]
    if args.scenario in ("sc2", "both"):
        plan += [("SC2", s, args.sc2_executors) for s in args.scales]

    out = ROOT / "tests" / "results" / f"sc1_sc2_{time.strftime('%Y%m%d_%H%M%S')}.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    fields = ["scenario", "scale", "executors", "reviews", "repeat", "job", "seconds", "exit_code"]
    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for scenario, scale, executors in plan:
            raw = args.raw_template.format(scale=scale) if args.raw_template else ensure_sample(scale, args.base_games, args.days)
            reviews = count_reviews(raw) if not args.raw_template else ""
            lake = f"{args.lake_root}/lake_{scenario}_{scale}x_{executors}e"
            for r in range(args.repeat):
                total = 0.0
                for job in JOBS:
                    secs, code = run_job(job, raw, lake, args.mode, executors, {})
                    total += secs
                    w.writerow(dict(scenario=scenario, scale=scale, executors=executors, reviews=reviews, repeat=r,
                                    job=job, seconds=secs, exit_code=code))
                    f.flush()
                    print(f"{scenario} scale={scale}x exec={executors} {job}: {secs}s (exit {code})", flush=True)
                    if code != 0:
                        raise SystemExit(f"{job} failed; xem log bằng: python pipeline.py {job}")
                w.writerow(dict(scenario=scenario, scale=scale, executors=executors, reviews=reviews, repeat=r,
                                job="B2-B5 total", seconds=round(total, 2), exit_code=0))
    print(f"Results: {out}")


if __name__ == "__main__":
    main()

"""SC3: khả năng mở rộng speed layer.

Đẩy review vào Kafka với tốc độ tăng dần (bậc --rates, mỗi bậc --step-seconds giây, timestamp dời về hiện tại)
trong khi job streaming (mặc định S1) chạy với --metrics-file. Sau mỗi bậc đo:
  - thông lượng xử lý (processedRowsPerSecond)    - offset tụt hậu lớn nhất (consumer lag)
  - thời gian micro-batch (xấp xỉ độ trễ end-to-end = trigger + batch duration)
Lặp lại với số partition / executor khác nhau (đổi topic partitions, SPARK_MASTER=local[N] hoặc executor K8s)
và so sánh các file CSV trong tests/results/.

Ví dụ (Docker, Kafka steam-kafka trong mạng steamnet):
  python tests/scalability/sc3_streaming_load_test.py --brokers steam-kafka:9092 --rates 200 500 1000 2000 --label p3_e2
"""
import argparse
import csv
import json
import os
import statistics
import subprocess
import sys
import time
from pathlib import Path

from kafka import KafkaProducer

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from crawlers.replay_jsonl_to_kafka import iter_files, iter_records  # noqa: E402


def review_source(raw: Path):
    while True:  # lặp lại dữ liệu nếu cần nhiều bản ghi hơn
        for path in iter_files(raw, "steam_reviews_raw"):
            yield from iter_records(path)


def send_step(producer, records, rate, seconds, counter):
    end = time.time() + seconds
    sent = 0
    while time.time() < end:
        tick = time.time()
        for _ in range(rate):
            rec = next(records)
            now = int(time.time())
            counter[0] += 1
            rec.update(timestamp_created=now, timestamp_updated=now, recommendationid=f"sc3-{counter[0]}")
            producer.send("steam.reviews.raw", key=str(rec.get("appid")).encode(), value=json.dumps(rec).encode())
            sent += 1
        producer.flush()
        time.sleep(max(0.0, 1.0 - (time.time() - tick)))
    return sent


def read_metrics(path: Path, since: float):
    if not path.exists():
        return []
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    return [r for r in rows if r["ts"] >= since and (r.get("num_input_rows") or 0) > 0]


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--brokers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    p.add_argument("--raw", default="data/sample_raw")
    p.add_argument("--rates", nargs="+", type=int, default=[200, 500, 1000, 2000, 5000])
    p.add_argument("--step-seconds", type=int, default=60)
    p.add_argument("--job", default="s1", choices=["s1", "s2"])
    p.add_argument("--trigger", default="10 seconds")
    p.add_argument("--label", default="default", help="Nhãn cấu hình, vd. p6_e4 (6 partition, 4 executor)")
    p.add_argument("--no-start-job", action="store_true", help="Job streaming đã chạy sẵn (vd. trên K8s) với --metrics-file")
    p.add_argument("--metrics-file", default="data/bench/sc3_metrics.jsonl")
    args = p.parse_args()

    metrics = ROOT / args.metrics_file
    metrics.parent.mkdir(parents=True, exist_ok=True)
    job = None
    if not args.no_start_job:
        metrics.unlink(missing_ok=True)
        checkpoint = f"data/bench/sc3_checkpoint_{int(time.time())}"
        job = subprocess.Popen([sys.executable, "pipeline.py", args.job, "--", "--source", "kafka", "--brokers", args.brokers,
                                "--trigger", args.trigger, "--metrics-file", str(metrics), "--checkpoint", checkpoint],
                               cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        print("Waiting 45s for streaming job to start...", flush=True)
        time.sleep(45)

    producer = KafkaProducer(bootstrap_servers=args.brokers.split(","), linger_ms=20, acks=1)
    records = review_source(ROOT / args.raw)
    counter = [0]
    out = ROOT / "tests" / "results" / f"sc3_{args.label}_{time.strftime('%Y%m%d_%H%M%S')}.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    fields = ["label", "target_rate", "sent", "batches", "avg_processed_rows_per_sec", "max_processed_rows_per_sec",
              "avg_batch_ms", "p95_batch_ms", "max_offsets_behind", "keeps_up"]
    try:
        with out.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            for rate in args.rates:
                start = time.time()
                sent = send_step(producer, records, rate, args.step_seconds, counter)
                time.sleep(15)  # để micro-batch cuối của bậc hoàn tất
                rows = read_metrics(metrics, start)
                durations = sorted(r["batch_duration_ms"] or 0 for r in rows)
                processed = [r["processed_rows_per_sec"] or 0 for r in rows]
                behind = max((r["max_offsets_behind_latest"] or 0) for r in rows) if rows else None
                row = dict(label=args.label, target_rate=rate, sent=sent, batches=len(rows),
                           avg_processed_rows_per_sec=round(statistics.mean(processed), 1) if processed else None,
                           max_processed_rows_per_sec=round(max(processed), 1) if processed else None,
                           avg_batch_ms=round(statistics.mean(durations)) if durations else None,
                           p95_batch_ms=durations[int(0.95 * (len(durations) - 1))] if durations else None,
                           max_offsets_behind=behind,
                           # theo kịp nếu không còn tụt hậu đáng kể sau bậc tải
                           keeps_up=behind is not None and behind < rate * 2)
                w.writerow(row)
                f.flush()
                print(row, flush=True)
    finally:
        producer.close()
        if job:
            job.terminate()
    print(f"Results: {out}")


if __name__ == "__main__":
    main()

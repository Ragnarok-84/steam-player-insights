"""SC3: khả năng mở rộng speed layer.

Đẩy bản ghi của một tập dữ liệu (mặc định data/scale/raw_<scale>) vào Kafka với tốc độ tăng dần (bậc --rates,
mỗi bậc --step-seconds giây, timestamp dời về hiện tại) trong khi job streaming (S1, S2 hoặc S3) chạy với
--metrics-file và --latency-file. Sau mỗi bậc đo:
  - thông lượng xử lý (processedRowsPerSecond trung bình)
  - consumer lag = offset cuối của topic Kafka (lấy mẫu mỗi giây) - offset query đã xử lý xong: lớn nhất trong bậc
    (max_lag, bình thường cỡ tải x (trigger + thời gian batch)) và còn lại sau khi ngừng gửi (final_lag)
  - độ trễ end-to-end p50/p95/p99: từ lúc message vào Kafka tới lúc foreachBatch ghi xong sink
    (ES khi có --es), xem processing.common.spark_utils.with_e2e_latency
Chi tiết ghi tests/results/sc3_<label>_<timestamp>.csv; số chính ghi thêm vào mẫu chung (tests/results/common/)
qua tests/results_log.record, mỗi bậc tải một run (notes ghi target_rate). Chạy mỗi cấu hình 3 lần (--repeat 1..3).

Ví dụ (máy local, Kafka + ES bằng scripts/demo.sh):
  python tests/scalability/sc3_streaming_load_test.py --job s1 --scale 100 --es --repeat 1 \
      --dataset steam_scale_1000g_20261120-2300 --rates 200 500 1000 2000
"""
import argparse
import csv
import json
import os
import signal
import statistics
import subprocess
import sys
import threading
import time
from pathlib import Path

from kafka import KafkaConsumer, KafkaProducer, TopicPartition

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from crawlers.replay_jsonl_to_kafka import iter_files, iter_records  # noqa: E402
from tests.results_log import record  # noqa: E402

# job -> (thư mục raw, topic, trường thời gian sự kiện cần dời về hiện tại)
JOB_SOURCES = {
    "s1": ("steam_reviews_raw", "steam.reviews.raw", "timestamp_created"),
    "s2": ("steam_reviews_raw", "steam.reviews.raw", "timestamp_created"),
    "s3": ("steam_players_ccu", "steam.players.ccu", "timestamp"),
}


def source(raw: Path, folder: str, cache_limit: int = 50_000):
    """Lặp vô hạn qua bản ghi của tập. Tập nhỏ (vd. CCU: mỗi file 1 bản ghi) giữ trong RAM sau lượt đầu,
    vì mở một file gzip cho mỗi bản ghi làm producer không đạt tốc độ mục tiêu; tập lớn đọc lại từ file."""
    cache = []
    for path in iter_files(raw, folder):
        for rec in iter_records(path):
            if cache is not None:
                cache.append(rec)
                if len(cache) > cache_limit:
                    cache = None
            yield dict(rec)
    if cache == []:
        raise SystemExit(f"Không có bản ghi {folder} trong {raw}")
    while True:
        if cache is not None:
            yield from (dict(r) for r in cache)
        else:
            for path in iter_files(raw, folder):
                yield from iter_records(path)


def send_step(producer, records, job, rate, seconds, counter):
    _, topic, ts_field = JOB_SOURCES[job]
    end = time.time() + seconds
    sent = 0
    while time.time() < end:
        tick = time.time()
        for _ in range(rate):
            rec = next(records)
            now = int(time.time())
            counter[0] += 1
            rec[ts_field] = now
            if topic == "steam.reviews.raw":
                rec.update(timestamp_updated=now, recommendationid=f"sc3-{counter[0]}")
            producer.send(topic, key=str(rec.get("appid")).encode(), value=json.dumps(rec).encode())
            sent += 1
        producer.flush()
        time.sleep(max(0.0, 1.0 - (time.time() - tick)))
    return sent


def read_jsonl(path: Path, since: float):
    if not path.exists():
        return []
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    return [r for r in rows if r["ts"] >= since]


class EndOffsetSampler(threading.Thread):
    """Lấy mẫu tổng offset cuối của topic mỗi giây: samples = [(thời điểm, tổng offset)]."""

    def __init__(self, brokers, topic):
        super().__init__(daemon=True)
        self.brokers, self.topic, self.samples = brokers, topic, []
        self._halt = threading.Event()

    def run(self):
        consumer, warned = None, False
        while not self._halt.is_set():
            try:
                if consumer is None:
                    consumer = KafkaConsumer(bootstrap_servers=self.brokers)
                parts = [TopicPartition(self.topic, i) for i in consumer.partitions_for_topic(self.topic) or ()]
                if parts:
                    ends = consumer.end_offsets(parts)
                    self.samples.append((time.time(), sum(ends.values())))
            except Exception as e:  # mất mẫu không được làm hỏng lượt đo
                if not warned:
                    print(f"WARN: không lấy được end offset: {e}", flush=True)
                    warned = True
            self._halt.wait(1.0)
        if consumer is not None:
            consumer.close()

    def stop(self):
        self._halt.set()
        self.join(timeout=10)


def consumer_lag(samples, progress, topic, start):
    """(max_lag, final_lag) của query chậm nhất, trên các mẫu từ `start`; (None, None) nếu thiếu dữ liệu.

    progress: dòng metrics của lượt chạy này (gồm cả bậc trước), không lấy dòng của lượt cũ trong cùng file.
    Mỗi mẫu so với offset query đã xử lý xong gần nhất trước đó; mẫu trước batch đầu tiên của query bị bỏ.
    """
    queries = {}
    for r in sorted(progress, key=lambda r: r["ts"]):
        offset = (r.get("end_offsets") or {}).get(topic)
        if offset is not None:
            queries.setdefault(r.get("name"), []).append((r["ts"], offset))
    samples = [s for s in samples if s[0] >= start]
    peaks, finals = [], []
    for done in queries.values():
        lags = []
        for ts, end in samples:
            before = [offset for t, offset in done if t <= ts]
            if before:
                lags.append(max(0, end - before[-1]))
        if lags:
            peaks.append(max(lags))
            finals.append(lags[-1])
    return (max(peaks), max(finals)) if peaks else (None, None)


def trigger_seconds(text: str) -> float:
    value, unit = text.split()
    return float(value) * {"second": 1, "minute": 60, "hour": 3600}[unit.rstrip("s")]


def keeps_up(final_lag, p95_batch_ms, rate, trigger_s):
    """Theo kịp: hết tải thì xử lý hết tồn đọng (còn < 1 s tải) và batch không dài hơn trigger."""
    if final_lag is None or p95_batch_ms is None:
        return False
    return final_lag < rate and p95_batch_ms <= trigger_s * 1000


def pct(values, q):
    return values[min(len(values) - 1, int(q * len(values)))] if values else None


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--brokers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    p.add_argument("--scale", required=True, choices=["100", "500", "1000"])
    p.add_argument("--raw", default=None, help="Mặc định data/scale/raw_<scale>")
    p.add_argument("--dataset", default="", help="Tên gói dữ liệu, vd. steam_scale_1000g_20261120-2300")
    p.add_argument("--env", default="local", choices=["local", "k3s"])
    p.add_argument("--executors", type=int, default=None, help="Số executor Spark (để trống khi local[N])")
    p.add_argument("--repeat", type=int, default=1)
    p.add_argument("--rates", nargs="+", type=int, default=[200, 500, 1000, 2000, 5000])
    p.add_argument("--step-seconds", type=int, default=60)
    p.add_argument("--job", default="s1", choices=list(JOB_SOURCES))
    p.add_argument("--trigger", default="10 seconds")
    p.add_argument("--es", action="store_true", help="Job ghi Elasticsearch (độ trễ tính tới lúc ghi xong ES)")
    p.add_argument("--label", default=None, help="Nhãn file chi tiết; mặc định <job>_<scale>g_<env>")
    p.add_argument("--no-start-job", action="store_true",
                   help="Job streaming đã chạy sẵn (vd. trên K8s) với --metrics-file/--latency-file trỏ tới cùng file")
    p.add_argument("--metrics-file", default="data/bench/sc3_metrics.jsonl")
    p.add_argument("--latency-file", default="data/bench/sc3_latency.jsonl")
    p.add_argument("--no-record", action="store_true", help="Không ghi vào mẫu chung (chạy thử)")
    args = p.parse_args()

    trigger_s = trigger_seconds(args.trigger)
    raw = ROOT / (args.raw or f"data/scale/raw_{args.scale}")
    label = args.label or f"{args.job}_{args.scale}g_{args.env}"
    metrics, latency = ROOT / args.metrics_file, ROOT / args.latency_file
    metrics.parent.mkdir(parents=True, exist_ok=True)
    job = None
    if not args.no_start_job:
        metrics.unlink(missing_ok=True)
        latency.unlink(missing_ok=True)
        checkpoint = f"data/bench/sc3_checkpoint_{args.job}_{int(time.time())}"
        cmd = [sys.executable, "pipeline.py", args.job, *(["--es"] if args.es else []), "--",
               "--source", "kafka", "--brokers", args.brokers, "--trigger", args.trigger,
               "--metrics-file", str(metrics), "--latency-file", str(latency), "--checkpoint", checkpoint]
        # nhóm tiến trình riêng: pipeline.py chạy job ở tiến trình con, phải dừng cả nhóm khi xong
        job = subprocess.Popen(cmd, cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                               start_new_session=True)
        print("Waiting 45s for streaming job to start...", flush=True)
        time.sleep(45)
        if job.poll() is not None:
            raise SystemExit(f"Job {args.job} đã thoát (code {job.returncode}); chạy tay để xem log: {' '.join(cmd)}")

    producer = KafkaProducer(bootstrap_servers=args.brokers.split(","), linger_ms=20, acks=1)
    records = source(raw, JOB_SOURCES[args.job][0])
    topic = JOB_SOURCES[args.job][1]
    sampler = EndOffsetSampler(args.brokers.split(","), topic)
    run_start = time.time()
    sampler.start()
    counter = [0]
    out = ROOT / "tests" / "results" / f"sc3_{label}_{time.strftime('%Y%m%d_%H%M%S')}.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    fields = ["label", "job", "scale", "repeat", "target_rate", "sent", "batches", "avg_processed_rows_per_sec",
              "max_processed_rows_per_sec", "avg_batch_ms", "p95_batch_ms", "max_lag", "final_lag",
              "e2e_samples", "e2e_p50_ms", "e2e_p95_ms", "e2e_p99_ms", "keeps_up"]
    try:
        with out.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            for rate in args.rates:
                start = time.time()
                sent = send_step(producer, records, args.job, rate, args.step_seconds, counter)
                time.sleep(max(15.0, trigger_s * 1.5))  # để micro-batch cuối của bậc hoàn tất
                rows = [r for r in read_jsonl(metrics, start) if (r.get("num_input_rows") or 0) > 0]
                lat = sorted(x for r in read_jsonl(latency, start) for x in r["latency_ms"])
                durations = sorted(r["batch_duration_ms"] or 0 for r in rows)
                processed = [r["processed_rows_per_sec"] or 0 for r in rows]
                max_lag, final_lag = consumer_lag(list(sampler.samples), read_jsonl(metrics, run_start), topic, start)
                row = dict(label=label, job=args.job, scale=args.scale, repeat=args.repeat, target_rate=rate,
                           sent=sent, batches=len(rows),
                           avg_processed_rows_per_sec=round(statistics.mean(processed), 1) if processed else None,
                           max_processed_rows_per_sec=round(max(processed), 1) if processed else None,
                           avg_batch_ms=round(statistics.mean(durations)) if durations else None,
                           p95_batch_ms=pct(durations, .95),
                           max_lag=max_lag, final_lag=final_lag,
                           e2e_samples=len(lat), e2e_p50_ms=pct(lat, .5), e2e_p95_ms=pct(lat, .95),
                           e2e_p99_ms=pct(lat, .99),
                           keeps_up=keeps_up(final_lag, pct(durations, .95), rate, trigger_s))
                w.writerow(row)
                f.flush()
                print(row, flush=True)
                if not args.no_record:
                    ok = bool(rows) and bool(lat) and max_lag is not None
                    record(member="C", experiment="streaming", scale=args.scale, env=args.env, job=args.job,
                           executors=args.executors, repeat=args.repeat, dataset=args.dataset,
                           status="ok" if ok else "fail",
                           notes=(f"target_rate={rate}; step_s={args.step_seconds}; trigger={args.trigger}; "
                                  f"sink={'es' if args.es else 'console'}; keeps_up={row['keeps_up']}; final_lag={final_lag}"
                                  + ("" if ok else "; thiếu micro-batch/độ trễ/consumer lag trong bậc")),
                           metrics={"throughput_rps": row["avg_processed_rows_per_sec"],
                                    "latency_p50_ms": row["e2e_p50_ms"], "latency_p95_ms": row["e2e_p95_ms"],
                                    "latency_p99_ms": row["e2e_p99_ms"], "consumer_lag": max_lag,
                                    "records": sent})
    finally:
        sampler.stop()
        producer.close()
        if job:
            if hasattr(os, "killpg"):
                os.killpg(job.pid, signal.SIGTERM)
            else:
                job.terminate()
    print(f"Results: {out}")


if __name__ == "__main__":
    main()

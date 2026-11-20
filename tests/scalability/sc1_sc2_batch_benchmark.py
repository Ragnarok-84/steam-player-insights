"""SC1 & SC2: benchmark hiệu năng batch layer B2 -> B5 -> B3 -> B4 -> B6 -> B7 -> B8 (+ B9).

Ma trận đo: Quy mô (--scales) x Số executor (--executors) x Số lần lặp (--runs).
  SC1: cố định quy mô, tăng executor -> thời gian, speedup = median(e=1) / median(e=N).
  SC2: cố định executor, tăng quy mô (100 / 500 / 1000 game) -> thời gian theo khối lượng dữ liệu.

Mỗi lượt (scale, executors, run) chạy trong MỘT tiến trình con riêng với MỘT SparkSession:
  spark_startup  = thời gian tạo SparkSession (profile cluster: tính cả lúc chờ đủ executor đăng ký)
  b2 ... b8      = thời gian từng job, gọi trực tiếp hàm của processing/batch/* trên cùng session
  b9             = Logistic Regression vs VADER (mặc định có; --no-b9 để bỏ). Tính riêng, KHÔNG cộng vào
                   total_pipeline để tổng B2–B8 so sánh được với các lần đo trước khi có B9.
                   Chỉ số mô hình (accuracy/f1/auc, train_s/predict_s) ghi vào batch_ml_scale{scale}_{ngày}.csv
                   và, với --log-common, vào mẫu chung experiment=ml (job b9 và vader).
  total_pipeline = spark_startup + tổng B2–B8 (không tính thời gian đếm records/bytes sau mỗi job)
Lưu ý: `python pipeline.py batch` chạy mỗi job một tiến trình nên trả phí khởi động Spark 7 lần;
benchmark này tách phần đó ra để thấy riêng chi phí tính toán + ghi Parquet.
B1 bị bỏ qua (B1 đọc Kafka; benchmark đọc thẳng thư mục snapshot raw). Mặc định KHÔNG ghi Elasticsearch
(bật bằng --write-es). Thư mục lake của mỗi lượt bị xoá sạch trước khi chạy.

Profile:
  local          : SPARK_MASTER=local[N] (N = số executor/core), đường dẫn file local (WSL2).
  cluster | k3s  : giữ spark-defaults của pod (k8s/common/pipeline-config.yaml), thêm
                   spark.dynamicAllocation.enabled=false; spark.executor.instances=N.
                   Đường dẫn raw/lake là HDFS (vd. hdfs:///steam/bench/raw_{scale}).

Ví dụ:
  python tests/scalability/sc1_sc2_batch_benchmark.py --scales 100 500 1000 --executors 1 2 4 --runs 3
  python tests/scalability/sc1_sc2_batch_benchmark.py --profile k3s --topology 4-vm-k3s \\
      --image-tag k3s-20261115 --raw-template hdfs:///steam/bench/raw_{scale} \\
      --lake-template hdfs:///steam/bench/lake_{scale}_e{executors} --kubectl-top

Kết quả (tests/results/):
  batch_raw_scale{scale}_{YYYYMMDD}.csv          cột chuẩn: scale,stage,job,executors,run,seconds,records,bytes,peak_mem
  batch_raw_scale{scale}_details_{YYYYMMDD}.csv  thêm status/lỗi/parquet_files/peak_cpu/pod metrics/metadata
  batch_raw_scale{scale}_summary_{YYYYMMDD}.csv  trung vị qua các run + speedup
peak_mem (MB) / peak_cpu (%, cộng mọi core nên có thể > 100) đo trên cây tiến trình driver (Python + JVM;
profile local là toàn bộ Spark), lấy mẫu mỗi 0.5 s bằng psutil hoặc /proc.
"""
import argparse
import csv
import datetime as dt
import json
import os
import shutil
import statistics
import subprocess
import sys
import threading
import time
from pathlib import Path
from urllib.parse import unquote, urlparse

ROOT = Path(__file__).resolve().parents[2]
STAGE = "batch"
PIPELINE_JOBS = ["b2", "b5", "b3", "b4", "b6", "b7", "b8"]  # đúng thứ tự phụ thuộc dữ liệu của pipeline.BATCH_ORDER
JOBS = [*PIPELINE_JOBS, "b9"]  # set_jobs() bỏ b9 khi --no-b9
ALL_JOBS = ["spark_startup", *JOBS, "total_pipeline"]
ML_COLUMNS = ["scale", "executors", "run", "model", "split", "train_fraction", "train_rows", "test_rows",
              "test_positive_ratio", "accuracy", "f1", "f1_negative", "auc", "train_s", "predict_s"]


def set_jobs(with_b9: bool):
    global JOBS, ALL_JOBS
    JOBS = [*PIPELINE_JOBS, "b9"] if with_b9 else list(PIPELINE_JOBS)
    ALL_JOBS = ["spark_startup", *JOBS, "total_pipeline"]

# Bảng Parquet mỗi job sinh ra (tầng, tên thư mục) — dùng để đếm records / bytes / số file .parquet
JOB_OUTPUTS = {
    "b2": [("clean", t) for t in ("reviews", "ccu", "games_history", "games", "review_daily_counts")],
    "b5": [("curated", t) for t in ("review_sentiment", "aspect_stats", "keywords")],
    "b3": [("curated", t) for t in ("game_daily_stats", "game_period_stats", "segment_stats", "language_stats")],
    "b4": [("curated", t) for t in ("playtime_buckets", "game_retention")],
    "b6": [("curated", t) for t in ("trending", "trending_top", "event_impact")],
    "b7": [("curated", t) for t in ("review_baseline", "ccu_baseline")],
    "b8": [("curated", t) for t in ("game_clusters", "cluster_profiles")],
    "b9": [("curated", t) for t in ("sentiment_model_metrics", "sentiment_model_terms")],
}

RAW_COLUMNS = ["scale", "stage", "job", "executors", "run", "seconds", "records", "bytes", "peak_mem"]
DETAIL_COLUMNS = RAW_COLUMNS + ["parquet_files", "peak_cpu", "pod_peak_cpu_m", "pod_peak_mem_mib", "status",
                                "error", "profile", "topology", "image_tag", "raw", "lake", "measured_at"]
SUMMARY_COLUMNS = ["scale", "stage", "job", "executors", "runs_ok", "runs_total", "median_seconds", "speedup",
                   "baseline_executors", "records", "bytes", "output_mib", "parquet_files", "peak_mem", "peak_cpu",
                   "pod_peak_cpu_m", "pod_peak_mem_mib", "profile", "topology", "image_tag"]

EXIT_RAW_MISSING = 3


# ----------------------------------------------------------------------------------------------------------------
# Đường dẫn
# ----------------------------------------------------------------------------------------------------------------
def is_remote(path: str) -> bool:
    """hdfs://, s3a://... (không phải file local)."""
    return "://" in path and not path.startswith("file:")


def local_path(path: str) -> Path:
    if path.startswith("file:"):
        return Path(unquote(urlparse(path).path))
    p = Path(path)
    return p if p.is_absolute() else ROOT / p


def fill(template: str, **values) -> str:
    """Thay {scale}/{executors}/{run}/{profile}; các dấu ngoặc khác giữ nguyên (không dùng str.format)."""
    for k, v in values.items():
        template = template.replace("{" + k + "}", str(v))
    return template


def has_raw_reviews(raw: str) -> bool:
    p = local_path(raw)
    return (p / "steam_reviews_raw").is_dir() or any(p.glob("*/steam_reviews_raw"))


def safe_rmtree(lake: Path, raw: Path):
    """Xoá thư mục lake của lượt chạy; từ chối nếu trỏ vào repo, thư mục gốc hoặc chứa/trùng dữ liệu raw."""
    lake, raw = lake.resolve(), raw.resolve()
    forbidden = {ROOT.resolve(), Path(lake.anchor), Path.home().resolve(), (ROOT / "data").resolve()}
    if lake in forbidden or lake == raw or lake in raw.parents or raw in lake.parents:
        raise SystemExit(f"Từ chối xoá lake={lake} (trùng/chứa raw={raw} hoặc thư mục quan trọng)")
    if lake.exists():
        shutil.rmtree(lake)


# ----------------------------------------------------------------------------------------------------------------
# Lấy mẫu tài nguyên (chạy trong tiến trình worker)
# ----------------------------------------------------------------------------------------------------------------
class _ProcTree:
    """Tổng RSS (MB) và %CPU của tiến trình hiện tại + mọi tiến trình con (JVM, pyspark.daemon)."""

    def __init__(self):
        self.pid = os.getpid()
        try:
            import psutil
            self.psutil = psutil
            self.procs = {}
        except ImportError:
            self.psutil = None
            self.clk = os.sysconf("SC_CLK_TCK") if hasattr(os, "sysconf") and Path("/proc/self/stat").exists() else None
            self.page = os.sysconf("SC_PAGE_SIZE") if self.clk else None
            self.last = {}  # pid -> (cpu_ticks, wall)

    @property
    def available(self):
        return self.psutil is not None or self.clk is not None

    def sample(self):
        """-> (mem_mb, cpu_pct) hoặc (None, None)."""
        if self.psutil is not None:
            return self._sample_psutil()
        if self.clk:
            return self._sample_proc()
        return None, None

    def _sample_psutil(self):
        ps = self.psutil
        try:
            root = ps.Process(self.pid)
            tree = [root, *root.children(recursive=True)]
        except ps.Error:
            return None, None
        mem = cpu = 0.0
        alive = set()
        for p in tree:
            proc = self.procs.setdefault(p.pid, p)  # giữ object để cpu_percent() tính theo khoảng giữa 2 lần gọi
            alive.add(p.pid)
            try:
                mem += proc.memory_info().rss
                cpu += proc.cpu_percent(None)
            except ps.Error:
                pass
        self.procs = {k: v for k, v in self.procs.items() if k in alive}
        return mem / 1024 / 1024, cpu

    def _proc_tree(self):
        children = {}
        for stat in Path("/proc").glob("[0-9]*/stat"):
            try:
                fields = stat.read_text().rsplit(")", 1)[1].split()
                children.setdefault(int(fields[1]), []).append(int(stat.parent.name))
            except (OSError, IndexError, ValueError):
                pass
        out, todo = [], [self.pid]
        while todo:
            pid = todo.pop()
            out.append(pid)
            todo.extend(children.get(pid, []))
        return out

    def _sample_proc(self):
        mem = cpu = 0.0
        now = time.monotonic()
        seen = {}
        for pid in self._proc_tree():
            try:
                fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
                ticks = int(fields[11]) + int(fields[12])  # utime + stime
                mem += int(fields[21]) * self.page  # rss (pages)
            except (OSError, IndexError, ValueError):
                continue
            seen[pid] = (ticks, now)
            if pid in self.last:
                t0, w0 = self.last[pid]
                if now > w0:
                    cpu += (ticks - t0) / self.clk / (now - w0) * 100
        self.last = seen
        return mem / 1024 / 1024, cpu


class _KubectlTop:
    """Tổng CPU (millicore) / RAM (MiB) các pod theo selector qua `kubectl top pods` (cần metrics-server)."""

    def __init__(self, namespace, selector):
        self.cmd = ["kubectl", "top", "pods", "--no-headers"]
        if namespace:
            self.cmd += ["-n", namespace]
        if selector:
            self.cmd += ["-l", selector]
        self.enabled = shutil.which("kubectl") is not None
        if not self.enabled:
            print("[bench] WARN: không thấy kubectl; bỏ qua pod metrics", flush=True)

    @staticmethod
    def _qty(v, unit):
        v = v.strip()
        if unit == "cpu":
            return float(v[:-1]) if v.endswith("m") else float(v) * 1000
        for suf, mul in (("Ki", 1 / 1024), ("Mi", 1), ("Gi", 1024), ("Ti", 1024 * 1024)):
            if v.endswith(suf):
                return float(v[:-2]) * mul
        return float(v) / 1024 / 1024

    def sample(self):
        if not self.enabled:
            return None, None
        try:
            out = subprocess.run(self.cmd, capture_output=True, text=True, timeout=20)
        except (OSError, subprocess.TimeoutExpired):
            return None, None
        if out.returncode != 0:
            print(f"[bench] WARN: kubectl top lỗi, tắt pod metrics: {out.stderr.strip()[:200]}", flush=True)
            self.enabled = False
            return None, None
        cpu = mem = 0.0
        for line in out.stdout.splitlines():
            parts = line.split()
            if len(parts) >= 3:
                try:
                    cpu += self._qty(parts[1], "cpu")
                    mem += self._qty(parts[2], "mem")
                except ValueError:
                    pass
        return cpu, mem


class PeakSampler:
    """Luồng nền lấy mẫu mỗi `interval` giây, giữ giá trị đỉnh trong khoảng start()..stop()."""

    def __init__(self, tree, kube=None, interval=0.5, kube_interval=5.0):
        self.tree, self.kube = tree, kube
        self.interval, self.kube_interval = interval, kube_interval

    def __enter__(self):
        self.peak_mem = self.peak_cpu = self.pod_cpu = self.pod_mem = None
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self.tree.sample()  # mồi cpu_percent
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        self._thread.join(timeout=30)
        self._take_local()  # lấy thêm một mẫu cuối cho job quá ngắn

    @staticmethod
    def _max(a, b):
        return b if a is None else a if b is None else max(a, b)

    def _take_local(self):
        mem, cpu = self.tree.sample()
        self.peak_mem, self.peak_cpu = self._max(self.peak_mem, mem), self._max(self.peak_cpu, cpu)

    def _loop(self):
        next_kube = 0.0
        while not self._stop.wait(self.interval):
            self._take_local()
            if self.kube is not None and self.kube.enabled and time.monotonic() >= next_kube:
                next_kube = time.monotonic() + self.kube_interval
                c, m = self.kube.sample()
                self.pod_cpu, self.pod_mem = self._max(self.pod_cpu, c), self._max(self.pod_mem, m)


# ----------------------------------------------------------------------------------------------------------------
# Worker: một lượt (scale, executors, run) trong một SparkSession
# ----------------------------------------------------------------------------------------------------------------
def _output_stats(spark, path: str):
    """-> (records, bytes, parquet_files) của một bảng Parquet; None nếu bảng không tồn tại."""
    from processing.common.spark_utils import path_exists
    if not path_exists(spark, path):
        return None
    size = files = 0
    if is_remote(path):
        jvm = spark._jvm
        hpath = jvm.org.apache.hadoop.fs.Path(path)
        fs = hpath.getFileSystem(spark._jsc.hadoopConfiguration())
        it = fs.listFiles(hpath, True)
        while it.hasNext():
            st = it.next()
            name = st.getPath().getName()
            if not name.startswith(("_", ".")):
                size += st.getLen()
                files += name.endswith(".parquet")
    else:
        for dirpath, _, names in os.walk(local_path(path)):
            for name in names:
                if not name.startswith(("_", ".")):
                    size += os.path.getsize(os.path.join(dirpath, name))
                    files += name.endswith(".parquet")
    records = spark.read.parquet(path).count()  # dùng footer Parquet, nhanh
    return records, size, files


def _job_fn(job, raw, clean, curated, es):
    from processing.batch import (b2_clean_raw as b2, b3_game_daily_stats as b3, b4_playtime_buckets as b4,
                                  b5_aspect_sentiment as b5, b6_trending_games as b6, b7_baselines as b7,
                                  b8_game_clusters as b8, b9_sentiment_classifier as b9)

    def run_b2(spark):
        b2.clean_reviews(spark, raw, clean)
        b2.clean_ccu(spark, raw, clean)
        b2.clean_games(spark, raw, clean)

    return {
        "b2": run_b2,
        "b5": lambda s: b5.analyze_aspects_and_sentiment(s, clean, curated, es),
        "b3": lambda s: b3.compute_game_daily_stats(s, clean, curated, es),
        "b4": lambda s: b4.analyze_playtime_buckets(s, clean, curated, es),
        "b6": lambda s: b6.compute_trending_games(s, clean, curated, es, 50),
        "b7": lambda s: b7.compute_baselines(s, clean, curated, 7),
        "b8": lambda s: b8.cluster_games(s, clean, curated, 6, es),
        "b9": lambda s: b9.train_sentiment_classifier(s, clean, curated),
    }[job]


def _wait_executors(spark, n, timeout):
    deadline = time.monotonic() + timeout
    tracker = spark.sparkContext._jsc.sc().statusTracker()
    while len(tracker.getExecutorInfos()) < n + 1:  # có cả driver
        if time.monotonic() >= deadline:
            raise RuntimeError(f"Chỉ {len(tracker.getExecutorInfos()) - 1}/{n} executor đăng ký sau {timeout}s "
                               "(xem pod Pending/OOM/image)")
        time.sleep(1)


def worker(cfg_path: str) -> int:
    cfg = json.loads(Path(cfg_path).read_text(encoding="utf-8"))
    set_jobs(cfg.get("with_b9", True))
    sys.path.insert(0, str(ROOT))
    out = Path(cfg["result_file"])
    raw, lake, es = cfg["raw"], cfg["lake"], cfg["write_es"]
    clean, curated = f"{lake}/clean", f"{lake}/curated"

    def emit(**row):
        with out.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row) + "\n")

    tree = _ProcTree()
    if not tree.available:
        print("[bench] WARN: không có psutil và /proc; peak_mem/peak_cpu để trống", flush=True)
    kube = _KubectlTop(cfg.get("k8s_namespace"), cfg.get("pod_selector")) if cfg.get("kubectl_top") else None

    def sampler():
        return PeakSampler(tree, kube, cfg.get("sample_interval", 0.5))

    from processing.common.spark_utils import get_spark_session, raw_topic_paths

    with sampler() as smp:
        t0 = time.perf_counter()
        spark = get_spark_session(f"Bench_batch_s{cfg['scale']}_e{cfg['executors']}_r{cfg['run']}")
        if cfg["profile"] == "cluster" and cfg.get("executor_wait", 0) > 0:
            _wait_executors(spark, cfg["executors"], cfg["executor_wait"])
        startup = time.perf_counter() - t0
    emit(job="spark_startup", status="ok", seconds=startup, peak_mem=smp.peak_mem, peak_cpu=smp.peak_cpu,
         pod_cpu=smp.pod_cpu, pod_mem=smp.pod_mem)
    print(f"[bench] spark_startup {startup:.2f}s", flush=True)

    code = 0
    try:
        if not raw_topic_paths(spark, raw, "steam.reviews.raw"):
            print(f"[bench] Không có steam_reviews_raw dưới {raw}", flush=True)
            return EXIT_RAW_MISSING
        if is_remote(lake):  # lake local đã được tiến trình cha xoá
            hp = spark._jvm.org.apache.hadoop.fs.Path(lake)
            hp.getFileSystem(spark._jsc.hadoopConfiguration()).delete(hp, True)

        failed = None
        for job in JOBS:
            if failed:
                emit(job=job, status="skipped", error=f"{failed} failed")
                continue
            fn = _job_fn(job, raw, clean, curated, es)
            err = None
            with sampler() as smp:
                t = time.perf_counter()
                try:
                    ret = fn(spark)
                except BaseException as e:  # B2 dùng SystemExit khi thiếu dữ liệu
                    if isinstance(e, KeyboardInterrupt):
                        raise
                    err = f"{type(e).__name__}: {e}"[:500]
                secs = time.perf_counter() - t
            spark.catalog.clearCache()  # không để cache của job trước làm lợi job sau
            row = dict(job=job, seconds=secs, peak_mem=smp.peak_mem, peak_cpu=smp.peak_cpu,
                       pod_cpu=smp.pod_cpu, pod_mem=smp.pod_mem)
            if err:
                failed, code = job, 1
                emit(**row, status="fail", error=err)
                print(f"[bench] {job} FAIL sau {secs:.2f}s: {err}", flush=True)
                continue
            records = size = files = 0
            for layer, table in JOB_OUTPUTS[job]:
                st = _output_stats(spark, f"{lake}/{layer}/{table}")
                if st:
                    records, size, files = records + st[0], size + st[1], files + st[2]
            if job == "b9":  # chỉ số mô hình đi kèm dòng b9
                row["ml"] = [{k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in m.items()}
                             for m in (ret or [])]
            emit(**row, status="ok", records=records, bytes=size, parquet_files=files)
            print(f"[bench] {job} {secs:.2f}s records={records} bytes={size} files={files}", flush=True)
    finally:
        spark.stop()
    return code


# ----------------------------------------------------------------------------------------------------------------
# Orchestrator
# ----------------------------------------------------------------------------------------------------------------
def worker_env(args, raw, lake, executors):
    if args.profile == "local":
        sys.path.insert(0, str(ROOT))
        from pipeline import configure  # cùng cách pipeline.py đặt đường dẫn local
        env = configure("local", str(local_path(raw)), str(local_path(lake)))
        env["SPARK_MASTER"] = f"local[{executors}]"
    else:
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        env["PYTHONPATH"] = os.pathsep.join(filter(None, [str(ROOT), env.get("PYTHONPATH")]))
        env.update({"HDFS_RAW_PATH": raw, "HDFS_CLEAN_PATH": f"{lake}/clean",
                    "HDFS_CURATED_PATH": f"{lake}/curated", "HDFS_CHECKPOINT_PATH": f"{lake}/checkpoints"})
        conf = [c for c in env.get("SPARK_EXTRA_CONF", "").split(";")
                if c and c.split("=")[0].strip() not in ("spark.dynamicAllocation.enabled", "spark.executor.instances")]
        conf += ["spark.dynamicAllocation.enabled=false", f"spark.executor.instances={executors}"]
        env["SPARK_EXTRA_CONF"] = ";".join(conf)
    env["SPARK_SHUFFLE_PARTITIONS"] = str(args.shuffle_partitions or max(4, executors * 4))
    env["WRITE_ES"] = "1" if args.write_es else "0"
    return env


def run_once(args, scale, executors, run, raw, lake, log_dir):
    """Chạy một lượt trong tiến trình con. -> (rows theo job, exit_code)."""
    if not is_remote(lake):
        safe_rmtree(local_path(lake), local_path(raw))
    tag = f"scale{scale}_e{executors}_r{run}"
    log_dir.mkdir(parents=True, exist_ok=True)
    result_file, cfg_file, log_file = (log_dir / f"{tag}.jsonl", log_dir / f"{tag}.cfg.json", log_dir / f"{tag}.log")
    result_file.unlink(missing_ok=True)
    cfg = dict(scale=scale, executors=executors, run=run, raw=raw, lake=lake, profile=args.profile,
               write_es=args.write_es, result_file=str(result_file), executor_wait=args.executor_wait,
               kubectl_top=args.kubectl_top, k8s_namespace=args.k8s_namespace, pod_selector=args.pod_selector,
               sample_interval=args.sample_interval, with_b9=not args.no_b9)
    cfg_file.write_text(json.dumps(cfg), encoding="utf-8")
    env = worker_env(args, raw, lake, executors)
    with log_file.open("w", encoding="utf-8") as log:
        try:
            code = subprocess.call([sys.executable, str(Path(__file__).resolve()), "--_worker", str(cfg_file)],
                                   cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT, timeout=args.timeout)
        except subprocess.TimeoutExpired:
            code = "timeout"
    rows = {}
    if result_file.exists():
        for line in result_file.read_text(encoding="utf-8").splitlines():
            r = json.loads(line)
            rows[r["job"]] = r
    for job in ["spark_startup", *JOBS]:  # tiến trình chết giữa chừng -> job còn thiếu ghi là fail
        rows.setdefault(job, dict(job=job, status="timeout" if code == "timeout" else "fail",
                                  error=f"worker exit={code}; xem {log_file}"))
    return rows, code, log_file


def total_row(rows):
    parts = [rows[j] for j in ["spark_startup", *PIPELINE_JOBS]]
    ok = all(p["status"] == "ok" for p in parts)

    def agg(key, fn):
        vals = [p.get(key) for p in parts if p.get(key) is not None]
        return fn(vals) if vals else None

    return dict(job="total_pipeline", status="ok" if ok else "fail",
                seconds=agg("seconds", sum) if ok else None,
                records=agg("records", sum), bytes=agg("bytes", sum), parquet_files=agg("parquet_files", sum),
                peak_mem=agg("peak_mem", max), peak_cpu=agg("peak_cpu", max),
                pod_cpu=agg("pod_cpu", max), pod_mem=agg("pod_mem", max),
                error="" if ok else "một hoặc nhiều job không ok")


def _r(v, nd=2):
    if v is None:
        return ""
    return int(round(v)) if nd == 0 else round(v, nd)


def detail_record(args, scale, executors, run, raw, lake, r):
    ok = r["status"] == "ok"
    return {
        "scale": scale, "stage": STAGE, "job": r["job"], "executors": executors, "run": run,
        "seconds": _r(r.get("seconds"), 3) if ok else "",
        "records": r.get("records", ""), "bytes": r.get("bytes", ""), "peak_mem": _r(r.get("peak_mem"), 1),
        "parquet_files": r.get("parquet_files", ""), "peak_cpu": _r(r.get("peak_cpu"), 1),
        "pod_peak_cpu_m": _r(r.get("pod_cpu"), 0), "pod_peak_mem_mib": _r(r.get("pod_mem"), 0),
        "status": r["status"], "error": r.get("error", ""), "profile": args.profile_label,
        "topology": args.topology, "image_tag": args.image_tag, "raw": raw, "lake": lake,
        "measured_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
    }


def _median(values):
    vals = [float(v) for v in values if v not in ("", None)]
    return statistics.median(vals) if vals else None


def summarize(details, args):
    """details: các dòng detail của MỘT quy mô -> dòng summary (trung vị qua các run, speedup so với e=1)."""
    groups = {}
    for d in details:
        groups.setdefault((d["job"], int(d["executors"])), []).append(d)
    executors = sorted({e for _, e in groups})
    base_e = 1 if 1 in executors else (executors[0] if executors else None)
    out = []
    for job in ALL_JOBS:
        base = _median(d["seconds"] for d in groups.get((job, base_e), []) if d["status"] == "ok")
        for e in executors:
            rows = groups.get((job, e), [])
            if not rows:
                continue
            ok = [d for d in rows if d["status"] == "ok"]
            med = _median(d["seconds"] for d in ok)
            size = _median(d["bytes"] for d in ok)
            out.append({
                "scale": rows[0]["scale"], "stage": STAGE, "job": job, "executors": e,
                "runs_ok": len(ok), "runs_total": len(rows), "median_seconds": _r(med, 3),
                "speedup": _r(base / med, 3) if base and med else "", "baseline_executors": base_e,
                "records": _r(_median(d["records"] for d in ok), 0), "bytes": _r(size, 0),
                "output_mib": _r(size / 1024 / 1024 if size is not None else None, 3),
                "parquet_files": _r(_median(d["parquet_files"] for d in ok), 0),
                "peak_mem": _r(_median(d["peak_mem"] for d in ok), 1),
                "peak_cpu": _r(_median(d["peak_cpu"] for d in ok), 1),
                "pod_peak_cpu_m": _r(_median(d["pod_peak_cpu_m"] for d in ok), 0),
                "pod_peak_mem_mib": _r(_median(d["pod_peak_mem_mib"] for d in ok), 0),
                "profile": args.profile_label, "topology": args.topology, "image_tag": args.image_tag,
            })
    return out


def print_table(scale, summary):
    executors = sorted({int(s["executors"]) for s in summary})
    if not executors:
        return
    by = {(s["job"], int(s["executors"])): s for s in summary}
    head = f"{'job':<15}" + "".join(f"{'e=' + str(e) + ' (s / x)':>20}" for e in executors) + f"{'records':>12}{'MiB':>10}{'files':>7}{'peakMB':>9}"
    print(f"\n=== Batch scale={scale}: median seconds / speedup (baseline e={summary[0]['baseline_executors']}) ===")
    print(head)
    print("-" * len(head))
    for job in ALL_JOBS:
        cells, last = "", None
        for e in executors:
            s = by.get((job, e))
            if s is None or s["median_seconds"] == "":
                cells += f"{'FAIL' if s else '-':>20}"
                continue
            sp = f"{s['speedup']:.2f}x" if s["speedup"] != "" else "-"
            cells += f"{s['median_seconds']:>11.2f} / {sp:>6}"
            last = s
        extra = (f"{last['records']:>12}{last['output_mib']:>10}{last['parquet_files']:>7}{last['peak_mem']:>9}"
                 if last and job != "spark_startup" else f"{'':>12}{'':>10}{'':>7}{(last or {}).get('peak_mem', ''):>9}")
        print(f"{job:<15}{cells}{extra}")


def unique_path(path: Path) -> Path:
    """Không ghi đè kết quả cũ cùng ngày: thêm _HHMMSS nếu file đã tồn tại."""
    return path if not path.exists() else path.with_name(f"{path.stem}_{time.strftime('%H%M%S')}{path.suffix}")


def write_csv(path, columns, rows):
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def log_common(args, scale, details):
    """Ghi thêm các số chính vào mẫu chung tests/results/common/ (tests/results_log.py)."""
    sys.path.insert(0, str(ROOT))
    from tests.results_log import record
    env = "local" if args.profile == "local" else "k3s"
    for d in details:
        metric = "startup_s" if d["job"] == "spark_startup" else "duration_s"
        metrics = {metric: d["seconds"] if d["seconds"] != "" else None}
        if d["status"] == "ok":
            pairs = [("peak_mem", "peak_mem_mb"), ("peak_cpu", "cpu_peak_pct")]
            if d["job"] != "spark_startup":
                pairs += [("records", "records"), ("bytes", "bytes"), ("parquet_files", "files")]
            metrics.update({name: d[src] for src, name in pairs if d[src] not in ("", None)})
        status = d["status"] if d["status"] in ("ok", "timeout") else "fail"
        record(member=args.member, experiment="batch", scale=scale, env=env, job=d["job"], metrics=metrics,
               repeat=int(d["run"]), executors=int(d["executors"]), dataset=args.dataset, status=status,
               notes="; ".join(filter(None, [f"topology={args.topology}" if args.topology else "",
                                             f"image={args.image_tag}" if args.image_tag else "", d["error"]])))


def log_common_ml(args, scale, ml_rows):
    """Chỉ số B9 vào mẫu chung: experiment=ml, job b9 (logreg) hoặc vader (tests/results/README.md, thành viên B)."""
    sys.path.insert(0, str(ROOT))
    from tests.results_log import record
    env = "local" if args.profile == "local" else "k3s"
    for m in ml_rows:
        if m.get("train_fraction") not in (None, 1.0):
            continue  # mẫu chung chỉ nhận mô hình train đủ dữ liệu
        names = ["accuracy", "f1", "auc"] + (["train_s", "predict_s"] if m["model"] == "logreg" else [])
        record(member=args.member, experiment="ml", scale=scale, env=env,
               job="b9" if m["model"] == "logreg" else "vader", repeat=int(m["run"]), executors=int(m["executors"]),
               dataset=args.dataset, metrics={k: m[k] for k in names if m.get(k) is not None},
               notes="; ".join(filter(None, [f"split={m['split']}", f"f1_negative={m['f1_negative']:.4f}",
                                             f"test_rows={m['test_rows']}",
                                             f"topology={args.topology}" if args.topology else "",
                                             f"image={args.image_tag}" if args.image_tag else ""])))


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--raw-template", default="data/scale/raw_{scale}",
                   help="Thư mục raw theo quy mô, {scale} được thay (mặc định data/scale/raw_{scale})")
    p.add_argument("--lake-template", default="data/scale/lake_{scale}",
                   help="Thư mục clean/curated của mỗi lượt; hỗ trợ {scale} {executors} {run} {profile}. "
                        "Bị XOÁ SẠCH trước mỗi lượt.")
    p.add_argument("--scales", nargs="+", type=int, default=[100])
    p.add_argument("--executors", nargs="+", type=int, default=[1, 2, 4])
    p.add_argument("--runs", "--repeat", dest="runs", type=int, default=3, help="Số lần lặp mỗi cấu hình (trung vị)")
    p.add_argument("--profile", choices=["local", "cluster", "k3s"], default="local")
    p.add_argument("--mode", choices=["local", "k8s"], help=argparse.SUPPRESS)  # tên cũ, tương đương --profile
    p.add_argument("--image-tag", default="", help="Metadata: tag image Spark (vd. k3s-20261115)")
    p.add_argument("--topology", default="", help="Metadata: mô tả hạ tầng (vd. 4-vm-k3s, wsl2-8c16g)")
    p.add_argument("--no-b9", action="store_true", help="Không đo B9 (Logistic Regression vs VADER)")
    p.add_argument("--write-es", action="store_true", help="Ghi Elasticsearch trong lúc đo (mặc định tắt)")
    p.add_argument("--shuffle-partitions", type=int, default=None, help="Mặc định max(4, executors*4)")
    p.add_argument("--executor-wait", type=int, default=180,
                   help="cluster: chờ tối đa N giây cho đủ executor đăng ký (tính vào spark_startup); 0 = không chờ")
    p.add_argument("--timeout", type=int, default=None, help="Giới hạn giây cho mỗi lượt")
    p.add_argument("--sample-interval", type=float, default=0.5)
    p.add_argument("--kubectl-top", action="store_true", help="cluster: lấy đỉnh CPU/RAM pod qua `kubectl top pods`")
    p.add_argument("--k8s-namespace", default=os.getenv("K8S_NAMESPACE", ""))
    p.add_argument("--pod-selector", default="spark-role=executor", help="Label selector cho kubectl top")
    p.add_argument("--results-dir", default="tests/results")
    p.add_argument("--log-dir", default="data/bench/logs", help="Log Spark + kết quả tạm của từng lượt")
    p.add_argument("--log-common", action="store_true",
                   help="Ghi thêm vào mẫu chung tests/results/common/ (cần --member, quy mô 100/500/1000)")
    p.add_argument("--member", default="B")
    p.add_argument("--dataset", default="")
    p.add_argument("--_worker", metavar="CFG", help=argparse.SUPPRESS)
    args = p.parse_args(argv)
    if args.mode and "--profile" not in (argv if argv is not None else sys.argv):
        args.profile = "cluster" if args.mode == "k8s" else "local"
    args.profile_label = args.profile
    if args.profile == "k3s":
        args.profile = "cluster"
    if min(args.executors) < 1 or args.runs < 1:
        p.error("--executors và --runs phải >= 1")
    return args


def main(argv=None):
    args = parse_args(argv)
    set_jobs(not args.no_b9)
    if args._worker:
        sys.exit(worker(args._worker))

    results_dir, log_dir = ROOT / args.results_dir, local_path(args.log_dir)
    results_dir.mkdir(parents=True, exist_ok=True)
    day = time.strftime("%Y%m%d")
    written, skipped = [], []
    for scale in args.scales:
        raw = fill(args.raw_template, scale=scale)
        if not is_remote(raw) and not has_raw_reviews(raw):
            print(f"[bench] WARN: scale={scale}: không có {raw}/steam_reviews_raw -> bỏ qua quy mô này", flush=True)
            skipped.append(scale)
            continue
        details, ml_rows, raw_missing = [], [], False
        for executors in args.executors:
            for run in range(1, args.runs + 1):
                lake = fill(args.lake_template, scale=scale, executors=executors, run=run, profile=args.profile_label)
                print(f"[bench] scale={scale} executors={executors} run={run}/{args.runs} raw={raw} lake={lake}",
                      flush=True)
                t = time.perf_counter()
                rows, code, log_file = run_once(args, scale, executors, run, raw, lake, log_dir)
                if code == EXIT_RAW_MISSING:
                    print(f"[bench] WARN: scale={scale}: không có dữ liệu review dưới {raw} -> bỏ qua", flush=True)
                    raw_missing = True
                    break
                rows["total_pipeline"] = total_row(rows)
                ml_rows += [dict(scale=scale, executors=executors, run=run, **m) for m in rows.get("b9", {}).get("ml", [])]
                for job in ALL_JOBS:
                    details.append(detail_record(args, scale, executors, run, raw, lake, rows[job]))
                status = " ".join(f"{j}={rows[j]['seconds']:.1f}s" if rows[j]["status"] == "ok" else f"{j}={rows[j]['status']}"
                                  for j in ALL_JOBS)
                print(f"[bench]   {status}  (wall {time.perf_counter() - t:.1f}s)", flush=True)
                if code != 0:
                    print(f"[bench]   WARN: lượt lỗi (exit={code}); log: {log_file}", flush=True)
            if raw_missing:
                break
        if raw_missing:
            skipped.append(scale)
            if not details:
                continue
        raw_csv = unique_path(results_dir / f"batch_raw_scale{scale}_{day}.csv")
        det_csv = unique_path(results_dir / f"batch_raw_scale{scale}_details_{day}.csv")
        sum_csv = unique_path(results_dir / f"batch_raw_scale{scale}_summary_{day}.csv")
        summary = summarize(details, args)
        write_csv(raw_csv, RAW_COLUMNS, details)
        write_csv(det_csv, DETAIL_COLUMNS, details)
        write_csv(sum_csv, SUMMARY_COLUMNS, summary)
        written += [raw_csv, det_csv, sum_csv]
        if ml_rows:
            ml_csv = unique_path(results_dir / f"batch_ml_scale{scale}_{day}.csv")
            write_csv(ml_csv, ML_COLUMNS, ml_rows)
            written.append(ml_csv)
        print_table(scale, summary)
        if args.log_common:
            try:
                log_common(args, scale, details)
                log_common_ml(args, scale, ml_rows)
            except ValueError as e:
                print(f"[bench] WARN: không ghi được mẫu chung: {e}", flush=True)

    print()
    for path in written:
        print(f"Results: {path.relative_to(ROOT) if ROOT in path.parents else path}")
    if skipped:
        print(f"Bỏ qua quy mô (thiếu raw): {skipped}")
    if not written:
        sys.exit("Không có quy mô nào được đo.")


if __name__ == "__main__":
    main()

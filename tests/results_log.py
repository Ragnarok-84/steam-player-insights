"""Ghi kết quả đo theo MẪU CHUNG của nhóm (tests/results/README.md).

Mỗi dòng là một số đo (định dạng dài: metric + value), nên mọi thí nghiệm dùng chung một bảng
và notebook chỉ cần pivot. Đơn vị suy ra từ tên metric, không tự đặt.

Dùng trong Python:
    from tests.results_log import record
    record(member="B", experiment="batch", scale=500, env="local", job="b2", executors=2, repeat=1,
           metrics={"duration_s": 41.2, "peak_mem_mb": 1830})

Dùng từ shell:
    python tests/results_log.py add --member C --experiment streaming --scale 100 --env local \
        --job s1 --repeat 1 latency_p95_ms=850 throughput_rps=1200
    python tests/results_log.py import-crawl --member A --scale 100 data/raw/collection-worker-0.json
    python tests/results_log.py check            # kiểm tra mọi file trong tests/results/common/
    python tests/results_log.py merge out.csv    # gộp tất cả thành một bảng cho notebook
"""
import argparse
import csv
import datetime as dt
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "tests" / "results" / "common"

COLUMNS = ["run_id", "measured_at", "member", "experiment", "scale", "env", "dataset", "job",
           "executors", "repeat", "metric", "value", "unit", "status", "git_commit", "notes"]

MEMBERS = {"A", "B", "C", "D", "lead"}
EXPERIMENTS = {"crawl", "storage", "batch", "streaming", "es_query", "fault", "ml", "resource"}
ENVS = {"local", "k3s"}
SCALES = {"100", "500", "1000"}
STATUSES = {"ok", "fail", "timeout"}

# metric -> đơn vị. Muốn thêm metric: thêm vào đây (qua PR) để cả nhóm dùng chung tên.
METRICS = {
    # thời gian
    "duration_s": "s", "startup_s": "s", "recovery_s": "s", "green_wait_s": "s",
    "train_s": "s", "predict_s": "s",
    # khối lượng
    "records": "count", "requests": "count", "games_covered": "count", "files": "count",
    "doc_count": "count", "bytes": "bytes",
    # tốc độ
    "requests_per_s": "1/s", "throughput_rps": "rows/s", "qps": "1/s",
    # độ trễ
    "latency_p50_ms": "ms", "latency_p95_ms": "ms", "latency_p99_ms": "ms",
    # lỗi
    "errors": "count", "http_429": "count", "data_loss": "count", "consumer_lag": "messages",
    # tài nguyên, chi phí
    "peak_mem_mb": "MB", "cpu_peak_pct": "%", "vm_hours": "h", "cost_usd": "USD",
    # chất lượng mô hình / phân tích
    "accuracy": "ratio", "f1": "ratio", "auc": "ratio", "silhouette": "ratio",
    "alerts": "count", "speedup": "x",
}


def _git_commit():
    try:
        return subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], cwd=ROOT, text=True).strip()
    except Exception:
        return ""


def _validate(row):
    errs = []
    if row["member"] not in MEMBERS:
        errs.append(f"member phải thuộc {sorted(MEMBERS)}")
    if row["experiment"] not in EXPERIMENTS:
        errs.append(f"experiment phải thuộc {sorted(EXPERIMENTS)}")
    if str(row["scale"]) not in SCALES:
        errs.append(f"scale phải thuộc {sorted(SCALES, key=int)}")
    if row["env"] not in ENVS:
        errs.append(f"env phải thuộc {sorted(ENVS)}")
    if row["metric"] not in METRICS:
        errs.append(f"metric '{row['metric']}' chưa có trong METRICS (tests/results_log.py)")
    elif row["unit"] != METRICS[row["metric"]]:
        errs.append(f"unit của {row['metric']} phải là {METRICS[row['metric']]}")
    if row["status"] not in STATUSES:
        errs.append(f"status phải thuộc {sorted(STATUSES)}")
    if not row["job"]:
        errs.append("job không được trống")
    try:
        if int(row["repeat"]) < 1:
            raise ValueError
    except (TypeError, ValueError):
        errs.append("repeat là số nguyên >= 1")
    if row["executors"] not in ("", None):
        try:
            int(row["executors"])
        except ValueError:
            errs.append("executors là số nguyên hoặc để trống")
    if row["value"] not in ("", None):
        try:
            float(row["value"])
        except ValueError:
            errs.append("value phải là số")
    elif row["status"] == "ok":
        errs.append("value trống nhưng status=ok")
    return errs


def record(member, experiment, scale, env, job, metrics, repeat=1, executors=None, dataset="",
           status="ok", notes="", run_id=None, out_dir=OUT_DIR):
    """Ghi một lần đo (nhiều metric) vào tests/results/common/<experiment>_<scale>_<env>_<ngày>_<member>.csv."""
    now = dt.datetime.now(dt.timezone.utc)
    run_id = run_id or f"{now:%Y%m%d-%H%M%S}-{member}-{experiment}-{job}"
    commit = _git_commit()
    rows = []
    for metric, value in metrics.items():
        row = {
            "run_id": run_id, "measured_at": now.isoformat(timespec="seconds"), "member": member,
            "experiment": experiment, "scale": str(scale), "env": env, "dataset": dataset, "job": job,
            "executors": "" if executors is None else str(executors), "repeat": str(repeat),
            "metric": metric, "value": "" if value is None else value, "unit": METRICS.get(metric, ""),
            "status": status, "git_commit": commit, "notes": notes,
        }
        errs = _validate(row)
        if errs:
            raise ValueError(f"{metric}: " + "; ".join(errs))
        rows.append(row)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{experiment}_{scale}_{env}_{now:%Y%m%d}_{member}.csv"
    new = not path.exists()
    with path.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS)
        if new:
            w.writeheader()
        w.writerows(rows)
    return path


def import_crawl(member, scale, report_path, dataset="", repeat=1):
    """Chuyển báo cáo crawl (data/raw/collection-worker-*.json) sang mẫu chung."""
    d = json.loads(Path(report_path).read_text())
    elapsed = d.get("elapsed_seconds") or 0
    metrics = {
        "duration_s": elapsed,
        "requests": d.get("requests", 0),
        "records": d.get("records", 0),
        "games_covered": d.get("completed_games", 0),
        "errors": d.get("failures", 0),
        "requests_per_s": round(d.get("requests", 0) / elapsed, 3) if elapsed else None,
    }
    job = "+".join(d.get("sources") or ["all"])
    status = "ok" if d.get("status") == "finished" else "fail"
    return record(member, "crawl", scale, "local", job, metrics, repeat=repeat, dataset=dataset,
                  status=status, notes=f"report={Path(report_path).name}; status={d.get('status')}")


def check(paths):
    bad = 0
    for path in paths:
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames != COLUMNS:
                print(f"{path}: sai cột. Cần đúng thứ tự: {','.join(COLUMNS)}")
                bad += 1
                continue
            for i, row in enumerate(reader, start=2):
                for e in _validate(row):
                    print(f"{path}:{i}: {e}")
                    bad += 1
    print("OK" if not bad else f"{bad} lỗi")
    return bad


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    a = sub.add_parser("add", help="ghi một lần đo; metric dạng name=value")
    for k in ("member", "experiment", "env", "job"):
        a.add_argument(f"--{k}", required=True)
    a.add_argument("--scale", required=True)
    a.add_argument("--repeat", type=int, default=1)
    a.add_argument("--executors", type=int)
    a.add_argument("--dataset", default="")
    a.add_argument("--status", default="ok")
    a.add_argument("--notes", default="")
    a.add_argument("metrics", nargs="+")

    c = sub.add_parser("import-crawl", help="nhập báo cáo collection-worker-*.json")
    c.add_argument("--member", required=True)
    c.add_argument("--scale", required=True)
    c.add_argument("--dataset", default="")
    c.add_argument("--repeat", type=int, default=1)
    c.add_argument("reports", nargs="+")

    k = sub.add_parser("check", help="kiểm tra file theo mẫu (mặc định: tests/results/common/*.csv)")
    k.add_argument("files", nargs="*")

    m = sub.add_parser("merge", help="gộp mọi file trong tests/results/common/ thành một CSV")
    m.add_argument("output")

    args = ap.parse_args()
    if args.cmd == "add":
        metrics = {}
        for item in args.metrics:
            name, _, value = item.partition("=")
            metrics[name] = value
        print(record(args.member, args.experiment, args.scale, args.env, args.job, metrics, repeat=args.repeat,
                     executors=args.executors, dataset=args.dataset, status=args.status, notes=args.notes))
    elif args.cmd == "import-crawl":
        for i, r in enumerate(args.reports):
            print(import_crawl(args.member, args.scale, r, dataset=args.dataset, repeat=args.repeat + i))
    elif args.cmd == "check":
        files = args.files or sorted(OUT_DIR.glob("*.csv"))
        sys.exit(1 if check(files) else 0)
    elif args.cmd == "merge":
        files = sorted(OUT_DIR.glob("*.csv"))
        with open(args.output, "w", newline="", encoding="utf-8") as out:
            w = csv.DictWriter(out, fieldnames=COLUMNS)
            w.writeheader()
            for p in files:
                with p.open(newline="", encoding="utf-8") as f:
                    w.writerows(csv.DictReader(f))
        print(f"{len(files)} file -> {args.output}")


if __name__ == "__main__":
    main()

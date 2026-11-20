"""Số đo lưu trữ theo quy mô (thành viên B): dung lượng + số file của raw (tập con), clean và curated.

Đọc thẳng hệ thống file local, không cần Spark. Chạy sau sc1_sc2_batch_benchmark (lake của lượt cuối còn lại).
Ghi vào mẫu chung tests/results/common/ (experiment=storage, job = raw | clean | curated | clean/<bảng> ...).

Ví dụ:
  python tests/scalability/storage_stats.py --raw-template ~/steam-bench/scale/raw_{scale} \
      --lake-template ~/steam-bench/lake/lake_{scale} --scales 100 500 1000 --dataset steam_1000g_20261118 --log-common
"""
import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def walk(path: Path, suffix: str):
    """(bytes, số file dữ liệu) dưới path; bỏ file ẩn/_SUCCESS/.crc."""
    size = files = 0
    for dirpath, _, names in os.walk(path):
        for n in names:
            if n.startswith((".", "_")):
                continue
            size += os.path.getsize(os.path.join(dirpath, n))
            files += n.endswith(suffix)
    return size, files


def collect(raw: Path, lake: Path):
    rows = [("raw", *walk(raw, ".jsonl.gz"))]
    for layer in ("clean", "curated"):
        base = lake / layer
        if not base.exists():
            continue
        rows.append((layer, *walk(base, ".parquet")))
        for table in sorted(p for p in base.iterdir() if p.is_dir()):
            rows.append((f"{layer}/{table.name}", *walk(table, ".parquet")))
    return rows


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--raw-template", default="data/scale/raw_{scale}")
    p.add_argument("--lake-template", default="data/scale/lake_{scale}")
    p.add_argument("--scales", nargs="+", type=int, default=[100, 500, 1000])
    p.add_argument("--dataset", default="")
    p.add_argument("--env", default="local", choices=["local", "k3s"])
    p.add_argument("--member", default="B")
    p.add_argument("--log-common", action="store_true", help="Ghi vào tests/results/common/")
    a = p.parse_args()
    if a.log_common:
        sys.path.insert(0, str(ROOT))
        from tests.results_log import record
    for scale in a.scales:
        raw = Path(os.path.expanduser(a.raw_template.replace("{scale}", str(scale))))
        lake = Path(os.path.expanduser(a.lake_template.replace("{scale}", str(scale))))
        print(f"=== scale={scale}")
        for job, size, files in collect(raw, lake):
            print(f"  {job:<38}{size / 1024 / 1024:>10.2f} MiB {files:>6} file")
            if a.log_common and "/" not in job:
                record(member=a.member, experiment="storage", scale=scale, env=a.env, job=job,
                       metrics={"bytes": size, "files": files}, dataset=a.dataset,
                       notes="raw = tập con đã gộp (1 file/nguồn/dt)" if job == "raw" else "Parquet snappy")


if __name__ == "__main__":
    main()

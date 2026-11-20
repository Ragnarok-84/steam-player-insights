"""Filter a frozen raw snapshot into nested game scales; never read live crawler output.

python tests/scalability/make_scale_subsets.py --snapshot data/snapshot/raw --targets data/target_apps.json
"""
import argparse
import collections
import gzip
import hashlib
import json
from pathlib import Path


def make_subsets(snapshot, targets, output, scales=(100, 500, 1000)):
    snapshot, targets, output = Path(snapshot).resolve(), Path(targets).resolve(), Path(output).resolve()
    if not snapshot.is_dir():
        raise ValueError("Snapshot directory does not exist")
    if snapshot == output or snapshot in output.parents or output in snapshot.parents:
        raise ValueError("Snapshot and output must be separate directories")
    entries = json.loads(targets.read_text(encoding="utf-8-sig"))
    ids = [int(e["appid"] if isinstance(e, dict) else e) for e in entries]
    if len(ids) != len(set(ids)) or any(i <= 0 for i in ids):
        raise ValueError("Targets must contain unique positive appids")
    if len(ids) < max(scales):
        raise ValueError("Not enough targets for all requested scales")
    destinations = {n: output / f"raw_{n}" for n in scales}
    if any(p.exists() for p in destinations.values()):
        raise ValueError("Output already exists; choose a new output directory")
    files = sorted(snapshot.glob("steam_*/dt=*/*.jsonl.gz"))
    if not files:
        raise ValueError("Snapshot has no raw gzip parts")
    hashes = {str(f.relative_to(snapshot)): hashlib.sha256(f.read_bytes()).hexdigest() for f in files}
    rank = {appid: i for i, appid in enumerate(ids)}
    counts = {n: collections.Counter() for n in scales}
    coverage = {n: collections.defaultdict(set) for n in scales}
    for f in files:
        rows = []
        with gzip.open(f, "rt", encoding="utf-8") as h:
            for line in h:
                row = json.loads(line)
                rows.append((int(row["appid"]), line))
        for n, dest in destinations.items():
            selected = [(a, line) for a, line in rows if rank.get(a, len(ids)) < n]
            if not selected:
                continue
            out = dest / f.relative_to(snapshot)
            out.parent.mkdir(parents=True, exist_ok=True)
            with gzip.open(out, "wt", encoding="utf-8") as h:
                for appid, line in selected:
                    h.write(line)
                    counts[n][f.relative_to(snapshot).parts[0]] += 1
                    coverage[n][f.relative_to(snapshot).parts[0]].add(appid)
    after_files = sorted(snapshot.glob("steam_*/dt=*/*.jsonl.gz"))
    after = {str(f.relative_to(snapshot)): hashlib.sha256(f.read_bytes()).hexdigest() for f in after_files}
    if hashes != after:
        raise RuntimeError("Snapshot changed during filtering; discard outputs and use a frozen copy")
    for n, dest in destinations.items():
        dest.mkdir(parents=True, exist_ok=True)
        manifest = {"scale": n, "appids": ids[:n], "snapshot": str(snapshot),
                    "targets_sha256": hashlib.sha256(targets.read_bytes()).hexdigest(),
                    "source_sha256": hashes, "records": dict(counts[n]),
                    "coverage": {k: len(v) for k, v in coverage[n].items()},
                    "bytes": sum(f.stat().st_size for f in dest.rglob("*.gz"))}
        (dest / "scale_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return {n: dict(c) for n, c in counts.items()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", required=True)
    parser.add_argument("--targets", required=True)
    parser.add_argument("--output", default="data/scale")
    args = parser.parse_args()
    print(json.dumps(make_subsets(args.snapshot, args.targets, args.output), indent=2))
"""Cắt MỘT snapshot raw thành các tập con lồng nhau theo appid (100 / 500 / 1000 game đầu danh sách).

Danh sách game do crawlers.build_targets chỉ thêm vào cuối, nên N game đầu của target_apps.json là đúng tập
đã chọn ở mức N (docs/BigData.md, mục 2 phần phân công). Ba tập con cùng khoảng thời gian thu, nên khác biệt
khi benchmark chỉ đến từ số game.

Đầu ra: <out>/raw_{N}/<nguồn>/dt=YYYY-MM-DD/part-00000.jsonl.gz (đã gộp: một file / nguồn / dt, như
scripts/compact_raw.py) + <out>/raw_{N}/target_apps.json (N game) + <out>/raw_{N}/subset_manifest.json.
Bản ghi không có appid hoặc appid ngoài danh sách bị bỏ và được đếm trong manifest. Chỉ dùng thư viện chuẩn.

Ví dụ:
  python tests/scalability/make_scale_subsets.py --raw data/incoming/data-1000g/raw \
      --targets data/incoming/data-1000g/target_apps.json --out data/scale --scales 100 500 1000
"""
import argparse
import datetime as dt
import gzip
import json
import shutil
import sys
from pathlib import Path

SOURCES = ("steam_reviews_raw", "steam_players_ccu", "steam_games_meta")


def appid_of(line: str):
    try:
        v = json.loads(line).get("appid")
        return int(v) if v is not None else None
    except (ValueError, AttributeError, TypeError):
        return None


def cut(raw: Path, targets: list, out: Path, scales: list, dataset: str) -> dict:
    order = [int(t["appid"]) for t in targets]
    if len(set(order)) != len(order):
        sys.exit("target_apps.json có appid trùng")
    if max(scales) > len(order):
        sys.exit(f"Danh sách chỉ có {len(order)} game, không cắt được mức {max(scales)}")
    rank = {a: i for i, a in enumerate(order)}  # vị trí trong danh sách -> thuộc mọi mức N > vị trí

    dirs = {n: out / f"raw_{n}" for n in scales}
    for d in dirs.values():
        if d.exists():
            shutil.rmtree(d)
        d.mkdir(parents=True)
    stats = {n: {s: {"records": 0, "files": 0, "dt": {}} for s in SOURCES} for n in scales}
    dropped = {s: {"no_appid": 0, "not_in_targets": 0} for s in SOURCES}
    source_files = {}

    for source in SOURCES:
        for part_dir in sorted((raw / source).glob("dt=*")):
            files = sorted(p for p in part_dir.glob("*.jsonl.gz") if not p.name.startswith((".", "_")))
            source_files[f"{source}/{part_dir.name}"] = len(files)
            writers = {}
            try:
                for p in files:
                    with gzip.open(p, "rt", encoding="utf-8") as f:
                        for line in f:
                            if not line.strip():
                                continue
                            a = appid_of(line)
                            if a is None:
                                dropped[source]["no_appid"] += 1
                                continue
                            r = rank.get(a)
                            if r is None:
                                dropped[source]["not_in_targets"] += 1
                                continue
                            line = line if line.endswith("\n") else line + "\n"
                            for n in scales:
                                if r < n:
                                    w = writers.get(n)
                                    if w is None:
                                        target = dirs[n] / source / part_dir.name / "part-00000.jsonl.gz"
                                        target.parent.mkdir(parents=True, exist_ok=True)
                                        w = writers[n] = gzip.open(target, "wt", encoding="utf-8")
                                        stats[n][source]["files"] += 1
                                    w.write(line)
                                    st = stats[n][source]
                                    st["records"] += 1
                                    st["dt"][part_dir.name] = st["dt"].get(part_dir.name, 0) + 1
            finally:
                for w in writers.values():
                    w.close()

    created = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
    for n in scales:
        (dirs[n] / "target_apps.json").write_text(json.dumps(targets[:n], ensure_ascii=False, indent=1),
                                                   encoding="utf-8")
        size = sum(p.stat().st_size for p in dirs[n].rglob("*.jsonl.gz"))
        manifest = {"dataset": dataset, "scale": n, "games": n, "created_at": created,
                    "source_raw": str(raw), "raw_bytes": size,
                    "raw_files": sum(stats[n][s]["files"] for s in SOURCES), "sources": stats[n],
                    "dropped_from_snapshot": dropped, "snapshot_files": source_files}
        (dirs[n] / "subset_manifest.json").write_text(json.dumps(manifest, indent=1), encoding="utf-8")
    return {"stats": stats, "dropped": dropped}


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--raw", required=True, help="Thư mục raw của snapshot (chứa steam_reviews_raw/...)")
    p.add_argument("--targets", required=True, help="target_apps.json của snapshot")
    p.add_argument("--out", default="data/scale", help="Ghi ra <out>/raw_{N} (bị XOÁ và tạo lại)")
    p.add_argument("--scales", nargs="+", type=int, default=[100, 500, 1000])
    p.add_argument("--dataset", default="", help="Tên gói dữ liệu, ghi vào manifest")
    a = p.parse_args()
    targets = json.loads(Path(a.targets).read_text(encoding="utf-8"))
    res = cut(Path(a.raw), targets, Path(a.out), sorted(set(a.scales)), a.dataset)
    prev = None
    for n in sorted(res["stats"]):
        counts = {s: res["stats"][n][s]["records"] for s in SOURCES}
        print(f"raw_{n}: " + "  ".join(f"{s}={c:,}" for s, c in counts.items()))
        if prev and any(counts[s] < prev[s] for s in SOURCES):
            sys.exit("Tập con không lồng nhau (mức lớn có ít bản ghi hơn mức nhỏ)")
        prev = counts
    print("Bỏ khỏi snapshot:", res["dropped"])


if __name__ == "__main__":
    main()

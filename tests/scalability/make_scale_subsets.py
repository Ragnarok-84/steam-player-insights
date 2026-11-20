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


if __name__ == "__main__":
    main()

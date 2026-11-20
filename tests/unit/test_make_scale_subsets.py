"""make_scale_subsets: tập con lồng nhau theo thứ tự target_apps.json, một file / nguồn / dt, đếm bản ghi bị bỏ."""
import gzip
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tests" / "scalability"))

import make_scale_subsets as mss  # noqa: E402


def write_part(path: Path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")


def read_all(path: Path):
    return [json.loads(line) for p in sorted(path.rglob("*.jsonl.gz")) for line in gzip.open(p, "rt")]


def test_nested_subsets(tmp_path):
    raw, out = tmp_path / "raw", tmp_path / "scale"
    targets = [{"appid": a} for a in (30, 10, 20)]  # thứ tự danh sách, không phải thứ tự appid
    write_part(raw / "steam_reviews_raw" / "dt=2026-11-17" / "part-a.jsonl.gz",
               [{"appid": 30, "recommendationid": "1"}, {"appid": 20, "recommendationid": "2"}])
    write_part(raw / "steam_reviews_raw" / "dt=2026-11-17" / "part-b.jsonl.gz",
               [{"appid": 10, "recommendationid": "3"}, {"appid": 99, "recommendationid": "4"}, {"x": 1}])
    write_part(raw / "steam_players_ccu" / "dt=2026-11-18" / "part-a.jsonl.gz", [{"appid": 20, "player_count": 5}])

    res = mss.cut(raw, targets, out, [1, 2, 3], "test")

    assert {r["appid"] for r in read_all(out / "raw_1")} == {30}
    assert {r["appid"] for r in read_all(out / "raw_2")} == {30, 10}
    assert {r["appid"] for r in read_all(out / "raw_3")} == {30, 10, 20}
    # một file / nguồn / dt; mức 3 có CCU, mức 1 thì không
    assert len(list((out / "raw_3" / "steam_reviews_raw" / "dt=2026-11-17").glob("*.gz"))) == 1
    assert not (out / "raw_1" / "steam_players_ccu").exists()
    assert res["dropped"]["steam_reviews_raw"] == {"no_appid": 1, "not_in_targets": 1}
    manifest = json.loads((out / "raw_2" / "subset_manifest.json").read_text())
    assert manifest["sources"]["steam_reviews_raw"]["records"] == 2
    assert len(json.loads((out / "raw_2" / "target_apps.json").read_text())) == 2

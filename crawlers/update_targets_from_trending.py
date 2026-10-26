"""Vòng phản hồi: thêm game trending (output B6: curated/trending_top) vào danh sách game theo dõi.

Đọc Parquet bằng pandas/pyarrow (không cần Spark). Giữ tổng số game <= 2000 (giới hạn của crawlers.common.targets):
game trending mới thay cho game cuối danh sách (thứ hạng thấp nhất) không nằm trong top trending.
"""
import argparse
import json
from pathlib import Path

import pandas as pd

from crawlers.common.public_datasets import replace_file

MAX_TARGETS = 2000


def update(targets_path: Path, trending_path: str, top: int, max_targets: int):
    targets = json.loads(targets_path.read_text(encoding="utf-8"))
    trending = pd.read_parquet(trending_path).sort_values("trending_rank").head(top)
    known = {int(t["appid"] if isinstance(t, dict) else t) for t in targets}
    new = [{"appid": int(r.appid), "name": r.name if isinstance(r.name, str) else "", "source": "trending",
            "trending_score": float(r.trending_score)} for r in trending.itertuples() if int(r.appid) not in known]
    trending_ids = set(trending.appid.astype(int))
    overflow = len(targets) + len(new) - max_targets
    if overflow > 0:
        keep, dropped = [], 0
        for t in reversed(targets):
            appid = int(t["appid"] if isinstance(t, dict) else t)
            if dropped < overflow and appid not in trending_ids:
                dropped += 1
                continue
            keep.append(t)
        targets = list(reversed(keep))
    targets.extend(new)
    tmp = targets_path.with_name("." + targets_path.name + ".tmp")
    tmp.write_text(json.dumps(targets, ensure_ascii=False, indent=2), encoding="utf-8")
    replace_file(tmp, targets_path)
    return [n["appid"] for n in new]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--targets", default="data/target_apps.json")
    parser.add_argument("--trending", default="data/lake/curated/trending_top")
    parser.add_argument("--top", type=int, default=50)
    parser.add_argument("--max-targets", type=int, default=MAX_TARGETS)
    args = parser.parse_args()
    added = update(Path(args.targets), args.trending, args.top, args.max_targets)
    print(f"Added {len(added)} trending games: {added}")

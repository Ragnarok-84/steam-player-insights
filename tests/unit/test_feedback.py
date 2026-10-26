import json

import pandas as pd

from crawlers.update_targets_from_trending import update


def test_trending_games_added_and_list_capped(tmp_path):
    targets = tmp_path / "targets.json"
    targets.write_text(json.dumps([{"appid": i} for i in range(1, 6)]))
    trending = tmp_path / "trending.parquet"
    pd.DataFrame({"appid": [3, 100, 101], "name": ["c", "x", "y"], "trending_rank": [1, 2, 3],
                  "trending_score": [3.0, 2.0, 1.0]}).to_parquet(trending)
    added = update(targets, str(trending), top=3, max_targets=6)
    result = [t["appid"] for t in json.loads(targets.read_text())]
    assert added == [100, 101]
    assert len(result) == 6
    assert 3 in result and 100 in result and 101 in result  # game trending sẵn có không bị loại
    assert 5 not in result  # game cuối danh sách bị thay

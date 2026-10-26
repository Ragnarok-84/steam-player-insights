"""Hợp nhất Lambda: khi batch view mới xong, xoá phần realtime view đã được batch bao phủ.

Batch layer tính trọn tới hết ngày mới nhất trong batch_game_daily (cutoff = max(dt) + 1 ngày).
Realtime doc có window_end <= cutoff bị xoá; dashboard đọc batch view trước cutoff và realtime view sau cutoff.
Mốc cutoff lưu ở ES index serving_state (id=current).
"""
import argparse
import datetime
from processing.common.es_writer import es_client


def merge(es, cutoff: str = None):
    if cutoff is None:
        res = es.search(index="batch_game_daily", size=0, aggs={"max_dt": {"max": {"field": "dt"}}})
        max_ms = res["aggregations"]["max_dt"]["value"]
        if max_ms is None:
            print("batch_game_daily is empty; nothing to merge.")
            return None
        cutoff = (datetime.datetime.fromtimestamp(max_ms / 1000, datetime.timezone.utc).date()
                  + datetime.timedelta(days=1)).isoformat()
    deleted = es.delete_by_query(index="realtime_game_metrics", query={"range": {"window_end": {"lte": cutoff}}},
                                 conflicts="proceed", refresh=True)["deleted"]
    es.index(index="serving_state", id="current", document={
        "batch_cutoff": cutoff, "updated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "deleted_realtime_docs": deleted})
    print(f"cutoff={cutoff}, deleted {deleted} realtime docs")
    return cutoff


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cutoff", help="YYYY-MM-DD; mặc định max(dt) của batch_game_daily + 1 ngày")
    merge(es_client(), parser.parse_args().cutoff)

"""SC4: khả năng mở rộng serving layer (Elasticsearch).

Chạy tải truy vấn đồng thời mô phỏng dashboard (tổng hợp batch view, tìm kiếm toàn văn review, đọc cảnh báo)
và ghi độ trễ p50/p95/p99 + thông lượng. Quy trình kịch bản:
  1. ES 2 node:  kubectl patch elasticsearch steam-elasticsearch -n elastic-system --type merge \
                   -p '{"spec":{"nodeSets":[{"name":"default","count":2}]}}'   rồi chạy script (--label es2)
  2. ES 3 node:  đổi count=3, script tự đo thời gian tới khi cluster green (--wait-green) rồi chạy tải (--label es3)
Kết quả: tests/results/sc4_<label>_<timestamp>.csv
"""
import argparse
import random
import statistics
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from processing.common.es_writer import es_client  # noqa: E402

WORDS = ["lag", "crash", "server", "story", "graphics", "price", "cheater", "bug", "fun", "boring"]


def queries(appids):
    appid = random.choice(appids)
    return random.choice([
        ("daily_by_game", "batch_game_daily", {"size": 0, "query": {"term": {"appid": appid}},
                                               "aggs": {"d": {"date_histogram": {"field": "dt", "calendar_interval": "week"},
                                                              "aggs": {"r": {"sum": {"field": "total_reviews"}}}}}}),
        ("top_ccu", "batch_game_daily", {"size": 0, "aggs": {"g": {"terms": {"field": "appid", "size": 20, "order": {"p": "desc"}},
                                                                    "aggs": {"p": {"max": {"field": "peak_ccu"}}}}}}),
        ("fulltext", "reviews_search", {"size": 20, "query": {"bool": {"must": [{"match": {"cleaned_review": random.choice(WORDS)}}],
                                                                        "filter": [{"term": {"appid": appid}}]}}}),
        ("aspects", "batch_aspect_stats", {"size": 0, "query": {"term": {"appid": appid}},
                                           "aggs": {"a": {"terms": {"field": "aspect"}, "aggs": {"n": {"sum": {"field": "negative_count"}}}}}}),
        ("alerts", "alerts", {"size": 50, "sort": [{"detected_at": "desc"}]}),
    ])


def worker(es, appids, stop_at, latencies, errors, lock):
    while time.time() < stop_at:
        name, index, body = queries(appids)
        t = time.perf_counter()
        try:
            es.options(request_timeout=30).search(index=index, body=body)
            ms = (time.perf_counter() - t) * 1000
            with lock:
                latencies.setdefault(name, []).append(ms)
        except Exception:  # noqa: BLE001 - đếm lỗi là số liệu của kịch bản
            with lock:
                errors[0] += 1


def pct(values, q):
    values = sorted(values)
    return round(values[min(len(values) - 1, int(q * len(values)))], 1) if values else None


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--label", default="default")
    p.add_argument("--concurrency", nargs="+", type=int, default=[4, 16, 32])
    p.add_argument("--seconds", type=int, default=60)
    p.add_argument("--wait-green", action="store_true", help="Đo thời gian tới khi cluster green trước khi chạy tải")
    args = p.parse_args()

    es = es_client()
    recovery = None
    if args.wait_green:
        t = time.time()
        es.options(request_timeout=1900).cluster.health(wait_for_status="green", timeout="30m")
        recovery = round(time.time() - t, 1)
        print(f"Cluster green after {recovery}s")
    nodes = len(es.cat.nodes(format="json"))
    res = es.search(index="batch_game_daily", body={"size": 0, "aggs": {"a": {"terms": {"field": "appid", "size": 2000}}}})
    appids = [b["key"] for b in res["aggregations"]["a"]["buckets"]] or [0]

    out = ROOT / "tests" / "results" / f"sc4_{args.label}_{time.strftime('%Y%m%d_%H%M%S')}.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8") as f:
        f.write("label,es_nodes,concurrency,query,count,qps,p50_ms,p95_ms,p99_ms,errors,green_wait_s\n")
        for conc in args.concurrency:
            latencies, errors, lock = {}, [0], threading.Lock()
            stop_at = time.time() + args.seconds
            threads = [threading.Thread(target=worker, args=(es, appids, stop_at, latencies, errors, lock)) for _ in range(conc)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            all_ms = [x for v in latencies.values() for x in v]
            for name, values in [("ALL", all_ms), *sorted(latencies.items())]:
                line = (f"{args.label},{nodes},{conc},{name},{len(values)},{round(len(values) / args.seconds, 1)},"
                        f"{pct(values, .5)},{pct(values, .95)},{pct(values, .99)},{errors[0] if name == 'ALL' else ''},"
                        f"{recovery if recovery is not None else ''}\n")
                f.write(line)
            print(f"conc={conc}: {len(all_ms)} queries, p50={pct(all_ms, .5)}ms p95={pct(all_ms, .95)}ms "
                  f"mean={round(statistics.mean(all_ms), 1) if all_ms else None}ms errors={errors[0]}", flush=True)
    print(f"Results: {out}")


if __name__ == "__main__":
    main()

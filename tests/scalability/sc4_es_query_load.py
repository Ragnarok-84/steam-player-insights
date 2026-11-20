"""SC4: khả năng mở rộng serving layer (Elasticsearch) theo dashboard D1–D4.

Chạy lại đúng các truy vấn ES của từng panel Kibana (lấy từ dashboards/setup_kibana.py: data.url của panel Vega
và saved search), với bộ lọc dashboard để trống, khoảng thời gian theo dashboard (D1/D2 5 năm, D3 24 giờ, D4 1 năm)
và D2 lọc một appid ngẫu nhiên như khi người dùng gõ appid:<id>. --concurrency luồng chạy song song, mỗi luồng
lặp: chọn ngẫu nhiên một panel của dashboard đang đo rồi gửi truy vấn. Đo cho từng dashboard:
  qps, latency p50/p95/p99 (ms), errors; doc_count và bytes (primary store) của các index mà dashboard đọc.
Chi tiết ghi tests/results/sc4_<label>_<timestamp>.csv (thêm từng panel); số chính ghi vào mẫu chung
(tests/results/common/) qua tests/results_log.record với job d1…d4. Chạy 3 lần (--repeat 1..3) ở mỗi mức.

Trên cụm, đổi số node ES:
  kubectl patch elasticsearch steam-elasticsearch -n elastic-system --type merge \
    -p '{"spec":{"nodeSets":[{"name":"default","count":2}]}}'   rồi chạy với --wait-green.

Ví dụ:
  python tests/scalability/sc4_es_query_load.py --scale 100 --repeat 1 --dataset steam_scale_1000g_20261120-2300
"""
import argparse
import copy
import random
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from dashboards.setup_kibana import DASHBOARDS, PANELS, SEARCHES  # noqa: E402
from processing.common.es_writer import es_client  # noqa: E402
from tests.results_log import record  # noqa: E402

DASH_JOB = {"steam-d1-overview": "d1", "steam-d2-game": "d2", "steam-d3-realtime": "d3", "steam-d4-alerts": "d4"}
SEARCH_TIME_FIELD = {"alerts": "detected_at", "batch_trending": "dt", "reviews_search": "dt"}
SEARCH_SIZE = 500  # số dòng mặc định Kibana lấy cho saved search (discover:sampleSize)


def fill(node, time_from, appid):
    """Thay placeholder của Kibana Vega bằng giá trị thật (bộ lọc dashboard rỗng, D2 lọc appid)."""
    if node == "%dashboard_context-must_clause%":
        return {"match_all": {}}
    if node == "%dashboard_context-filter_clause%":
        return {"term": {"appid": appid}} if appid is not None else {"match_all": {}}
    if isinstance(node, dict):
        if node.get("%timefilter%"):
            return {"gte": time_from, "lte": "now"}
        return {k: fill(v, time_from, appid) for k, v in node.items()}
    if isinstance(node, list):
        return [fill(v, time_from, appid) for v in node if v != "%dashboard_context-must_not_clause%"]
    return node


def panel_requests(pid):
    """(index, body) của mọi nguồn dữ liệu ES trong một panel."""
    if pid in SEARCHES:
        _, index, _, sort = SEARCHES[pid]
        must = ["%dashboard_context-must_clause%"]
        if index in SEARCH_TIME_FIELD:
            must.append({"range": {SEARCH_TIME_FIELD[index]: {"%timefilter%": True}}})
        body = {"size": SEARCH_SIZE, "sort": [{f: o} for f, o in sort],
                "query": {"bool": {"must": must, "filter": ["%dashboard_context-filter_clause%"]}}}
        return [(index, body)]
    data = PANELS[pid].get("data")
    data = data if isinstance(data, list) else [data] if data else []
    return [(d["url"]["index"], d["url"]["body"]) for d in data if isinstance(d.get("url"), dict)]


def dashboard_queries(did):
    """[(panel_id, index, body_template)] của một dashboard."""
    return [(pid, index, body) for pid in DASHBOARDS[did][2] for index, body in panel_requests(pid)]


def worker(es, queries, time_from, appids, stop_at, latencies, errors, lock):
    while time.time() < stop_at:
        pid, index, body = random.choice(queries)
        body = fill(copy.deepcopy(body), time_from, random.choice(appids) if appids else None)
        t = time.perf_counter()
        try:
            es.options(request_timeout=30).search(index=index, body=body)
            ms = (time.perf_counter() - t) * 1000
            with lock:
                latencies.setdefault(pid, []).append(ms)
        except Exception:  # noqa: BLE001 - đếm lỗi là số liệu của kịch bản
            with lock:
                errors[pid] = errors.get(pid, 0) + 1


def pct(values, q):
    values = sorted(values)
    return round(values[min(len(values) - 1, int(q * len(values)))], 1) if values else None


def index_stats(es, indices):
    """(doc_count, bytes) cộng trên các index tồn tại (primary, không tính replica)."""
    existing = [i for i in indices if es.indices.exists(index=i)]
    if not existing:
        return 0, 0
    st = es.indices.stats(index=",".join(existing), metric="docs,store")["_all"]["primaries"]
    return st["docs"]["count"], st["store"]["size_in_bytes"]


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--scale", required=True, choices=["100", "500", "1000"])
    p.add_argument("--dataset", default="")
    p.add_argument("--env", default="local", choices=["local", "k3s"])
    p.add_argument("--repeat", type=int, default=1)
    p.add_argument("--dashboards", nargs="+", default=list(DASH_JOB.values()), choices=list(DASH_JOB.values()))
    p.add_argument("--concurrency", type=int, default=8, help="Số luồng truy vấn đồng thời")
    p.add_argument("--seconds", type=int, default=60, help="Thời gian tải cho mỗi dashboard")
    p.add_argument("--label", default=None, help="Nhãn file chi tiết; mặc định <scale>g_<env>")
    p.add_argument("--wait-green", action="store_true", help="Đo thời gian tới khi cluster green trước khi chạy tải")
    p.add_argument("--no-record", action="store_true", help="Không ghi vào mẫu chung (chạy thử)")
    args = p.parse_args()

    es = es_client()
    recovery = None
    if args.wait_green:
        t = time.time()
        es.options(request_timeout=1900).cluster.health(wait_for_status="green", timeout="30m")
        recovery = round(time.time() - t, 1)
        print(f"Cluster green after {recovery}s")
    nodes = len(es.cat.nodes(format="json"))
    res = es.search(index="batch_game_daily", body={"size": 0, "aggs": {"a": {"terms": {"field": "appid", "size": 5000}}}})
    appids = [b["key"] for b in res["aggregations"]["a"]["buckets"]]

    label = args.label or f"{args.scale}g_{args.env}"
    out = ROOT / "tests" / "results" / f"sc4_{label}_{time.strftime('%Y%m%d_%H%M%S')}.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8") as f:
        f.write("label,scale,repeat,es_nodes,concurrency,dashboard,panel,count,qps,p50_ms,p95_ms,p99_ms,errors,"
                "doc_count,bytes,green_wait_s\n")
        for did, job in DASH_JOB.items():
            if job not in args.dashboards:
                continue
            queries = dashboard_queries(did)
            time_from = f"{DASHBOARDS[did][1]}"
            docs, size = index_stats(es, sorted({q[1] for q in queries}))
            latencies, errors, lock = {}, {}, threading.Lock()
            stop_at = time.time() + args.seconds
            threads = [threading.Thread(target=worker, args=(es, queries, time_from, appids if job == "d2" else None,
                                                             stop_at, latencies, errors, lock))
                       for _ in range(args.concurrency)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            all_ms = [x for v in latencies.values() for x in v]
            n_err = sum(errors.values())
            for name, values in [("ALL", all_ms), *sorted(latencies.items())]:
                err = n_err if name == "ALL" else errors.get(name, 0)
                f.write(f"{label},{args.scale},{args.repeat},{nodes},{args.concurrency},{job},{name},{len(values)},"
                        f"{round(len(values) / args.seconds, 1)},{pct(values, .5)},{pct(values, .95)},{pct(values, .99)},"
                        f"{err},{docs if name == 'ALL' else ''},{size if name == 'ALL' else ''},"
                        f"{recovery if recovery is not None else ''}\n")
            f.flush()
            print(f"{job}: {len(all_ms)} queries, qps={round(len(all_ms) / args.seconds, 1)} "
                  f"p50={pct(all_ms, .5)}ms p95={pct(all_ms, .95)}ms errors={n_err} docs={docs} bytes={size}", flush=True)
            if not args.no_record:
                metrics = {"qps": round(len(all_ms) / args.seconds, 2), "latency_p50_ms": pct(all_ms, .5),
                           "latency_p95_ms": pct(all_ms, .95), "latency_p99_ms": pct(all_ms, .99),
                           "errors": n_err, "doc_count": docs, "bytes": size}
                if recovery is not None:
                    metrics["green_wait_s"] = recovery
                record(member="C", experiment="es_query", scale=args.scale, env=args.env, job=job,
                       repeat=args.repeat, dataset=args.dataset, status="ok" if all_ms else "fail",
                       notes=f"concurrency={args.concurrency}; seconds={args.seconds}; es_nodes={nodes}; "
                             f"panels={len({q[0] for q in queries})}",
                       metrics=metrics)
    print(f"Results: {out}")


if __name__ == "__main__":
    main()

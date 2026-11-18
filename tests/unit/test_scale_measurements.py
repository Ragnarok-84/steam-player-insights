import datetime as dt
import json


def test_e2e_latency_drops_column_and_logs(spark, tmp_path):
    from processing.common.spark_utils import with_e2e_latency
    now = dt.datetime.now(dt.timezone.utc)
    df = spark.createDataFrame([(1, now - dt.timedelta(seconds=2)), (2, now - dt.timedelta(seconds=5)), (3, None)],
                               "appid int, ingest_ts_max timestamp")
    seen = []
    path = tmp_path / "lat.jsonl"
    with_e2e_latency(lambda b, i: seen.append(b.columns), "s1_5m", str(path))(df, 7)
    assert seen == [["appid"]]  # sink không nhận cột đo
    row = json.loads(path.read_text())
    assert row["name"] == "s1_5m" and row["batch_id"] == 7
    lat = sorted(row["latency_ms"])  # dòng ingest_ts_max null bị bỏ
    assert len(lat) == 2 and 1500 < lat[0] < 4500 and 4500 < lat[1] < 7500


def test_e2e_latency_off_only_drops_column(spark, monkeypatch):
    from processing.common.spark_utils import with_e2e_latency
    monkeypatch.delenv("STREAM_LATENCY_FILE", raising=False)
    df = spark.createDataFrame([(1, dt.datetime.now())], "appid int, ingest_ts_max timestamp")
    seen = []
    with_e2e_latency(lambda b, i: seen.append(b.columns), "s2_bombing")(df, 0)
    assert seen == [["appid"]]


def test_sc4_panel_queries_have_no_kibana_placeholders():
    import copy
    from tests.scalability.sc4_es_query_load import DASH_JOB, dashboard_queries, fill
    for did in DASH_JOB:
        queries = dashboard_queries(did)
        assert queries, did
        for pid, index, body in queries:
            filled = json.dumps(fill(copy.deepcopy(body), "now-1y", 730))
            assert "%" not in filled, pid
    d2 = fill(copy.deepcopy(dashboard_queries("steam-d2-game")[0][2]), "now-5y", 730)
    assert d2["query"]["bool"]["filter"] == [{"term": {"appid": 730}}]
    assert d2["query"]["bool"]["must_not"] == []

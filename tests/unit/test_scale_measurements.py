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


def test_committed_offsets_sums_partitions_and_ignores_files():
    from processing.common.spark_utils import committed_offsets
    assert committed_offsets({"sources": [{"endOffset": {"t": {"0": 5, "1": 7}}}]}) == {"t": 12}
    assert committed_offsets({"sources": [{"endOffset": '{"t": {"0": 3}}'}]}) == {"t": 3}
    assert committed_offsets({"sources": [{"endOffset": {"logOffset": 2}}, {"endOffset": None}]}) == {}


def test_consumer_lag_detects_backlog_of_slowest_query():
    from tests.scalability.sc3_streaming_load_test import consumer_lag
    samples = [(5, 999), (11, 100), (15, 500), (20, 900)]  # mẫu ts=5 thuộc bậc trước
    progress = [{"ts": 10, "name": "fast", "end_offsets": {"t": 100}},
                {"ts": 10, "name": "slow", "end_offsets": {"t": 100}},
                {"ts": 18, "name": "fast", "end_offsets": {"t": 900}},
                {"ts": 18, "name": "slow", "end_offsets": {"t": 300}}]
    assert consumer_lag(samples, progress, "t", 10) == (600, 600)
    assert consumer_lag(samples, progress[::2], "t", 10) == (400, 0)
    assert consumer_lag(samples, [], "t", 10) == (None, None)


def test_keeps_up_can_fail():
    from tests.scalability.sc3_streaming_load_test import keeps_up, trigger_seconds
    assert trigger_seconds("10 seconds") == 10 and trigger_seconds("1 minute") == 60
    assert keeps_up(0, 2000, 1000, 10)
    assert not keeps_up(5000, 2000, 1000, 10)   # còn tồn đọng
    assert not keeps_up(0, 15000, 1000, 10)     # batch dài hơn trigger
    assert not keeps_up(None, 2000, 1000, 10)   # không đo được lag


def test_split_rate_keeps_total():
    from tests.scalability.sc3_streaming_load_test import split_rate
    assert split_rate(5000, 3) == [1667, 1667, 1666]
    assert sum(split_rate(1000, 4)) == 1000

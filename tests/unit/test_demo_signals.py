"""Histogram-based trending/baselines and the demo injection path end to end (B2 filter, S2/S3 alerts)."""
import datetime as dt
import math

import pytest

from crawlers.common.jsonl_producer import JsonlProducer
from crawlers.demo_inject import bombing_records, ccu_records, DEMO_SOURCE

DAY = 86400
T0 = 1_790_000_000 - 1_790_000_000 % DAY  # UTC midnight


def hist_df(spark, rows):
    """rows: (appid, day_offset, up, down, partial)"""
    return spark.createDataFrame(
        [(a, dt.date.fromtimestamp(T0 + d * DAY), up, down, up + down, p) for a, d, up, down, p in rows],
        "appid int, dt date, reviews_up long, reviews_down long, reviews_total long, is_partial boolean")


def test_b6_prefers_histogram_counts_and_fills_gaps_with_zero(spark, tmp_path):
    from processing.batch.b6_trending_games import with_review_signal
    clean = tmp_path / "clean"
    hist_df(spark, [(1, 0, 90, 10, False), (1, 2, 40, 10, False), (1, 3, 5, 0, True)]).write.parquet(
        str(clean / "review_daily_counts"))
    daily = spark.createDataFrame(
        [(1, dt.date.fromtimestamp(T0 + d * DAY), n, n, "G1") for d, n in ((-5, 7), (0, 3), (1, 4), (3, 9))]
        + [(2, dt.date.fromtimestamp(T0), 6, 5, "G2")],
        "appid int, dt date, total_reviews long, positive_reviews long, name string")
    got = {(r.appid, (r.dt - dt.date.fromtimestamp(T0)).days): (r.review_signal, r.review_source)
           for r in with_review_signal(spark, daily, str(clean)).collect()}
    assert got[(1, 0)] == (100, "steam_histogram")   # histogram wins over the crawled sample (3)
    assert got[(1, 1)] == (0, "steam_histogram")     # inside coverage, missing day = 0 reviews
    assert got[(1, 2)] == (50, "steam_histogram")    # histogram-only day appears
    assert got[(1, -5)] == (7, "crawled")            # before coverage: crawled count
    assert got[(1, 3)] == (9, "crawled")             # partial day excluded from histogram
    assert got[(2, 0)] == (6, "crawled")             # game without histogram


def test_b7_histogram_baseline(spark, tmp_path):
    from processing.batch.b7_baselines import histogram_baseline
    clean = tmp_path / "clean"
    # 7 complete days with 24 or 48 negatives/day, plus a partial day that must be ignored
    rows = [(1, d, 100, 24 if d % 2 else 48, False) for d in range(7)] + [(1, 7, 1, 500, True)]
    hist_df(spark, rows).write.parquet(str(clean / "review_daily_counts"))
    b = histogram_baseline(spark, str(clean), 7).collect()[0]
    down = sum(r[3] for r in rows[:7])
    mean_h = down / 168
    day_var = sum(r[3] ** 2 for r in rows[:7]) / 7 - (down / 7) ** 2
    assert b.neg_per_hour_mean == pytest.approx(mean_h)
    assert b.neg_per_hour_std == pytest.approx(math.sqrt(mean_h + day_var / 576))
    assert b.baseline_positive_ratio == pytest.approx(round(700 * 100 / (700 + down), 2))
    assert b.baseline_source == "steam_histogram"


def test_injected_events_trigger_alerts_but_never_reach_batch(spark, tmp_path, monkeypatch):
    monkeypatch.setenv("HIVE_METASTORE_URI", "")
    from processing.batch.b2_clean_raw import clean_reviews, clean_ccu
    from processing.streaming.s2_review_bombing_detector import detect as s2
    from processing.streaming.s3_ccu_spike_detector import detect as s3
    now = T0 + 10 * 3600 + 1800
    bomb = bombing_records(730, 80, 20, now=now, seed=1)
    assert all(r["source"] == DEMO_SOURCE and not r["voted_up"] for r in bomb)

    # S2 sees a 60-minute window made of the injected reviews -> REVIEW_BOMBING
    w = dt.datetime.fromtimestamp(T0 + 10 * 3600, dt.timezone.utc)
    window = spark.createDataFrame([(730, w, w + dt.timedelta(hours=1), 80, 80, 0.0)],
                                   ["appid", "window_start", "window_end", "total_reviews", "negative_reviews", "positive_ratio"])
    base = spark.createDataFrame([(730, 2.0, 1.5, 85.0)],
                                 ["appid", "neg_per_hour_mean", "neg_per_hour_std", "baseline_positive_ratio"])
    assert [r.alert_type for r in s2(window, base).collect()] == ["REVIEW_BOMBING"]

    # S3: 3x the hourly baseline -> CCU_SPIKE
    ccu = ccu_records(730, 3000, now=now)
    win = spark.createDataFrame([(730, w, w, 3000.0)], ["appid", "window_start", "window_end", "avg_players_window"])
    hb = spark.createDataFrame([(730, 10, 1000.0)], ["appid", "hour", "baseline_ccu"])
    assert [r.alert_type for r in s3(win, hb).collect()] == ["CCU_SPIKE"]

    # Batch: B2 drops every injected record even after B1 copied them to raw
    producer = JsonlProducer(tmp_path / "raw")
    real = {"recommendationid": "1", "appid": 730, "review": "really good game here", "voted_up": True,
            "timestamp_created": now, "language": "english", "source": "steam_live"}
    for r in bomb + [real]:
        producer.send("steam.reviews.raw", "730", r)
    for r in ccu + [{"appid": 730, "timestamp": now, "player_count": 1000, "source": "steam_live"}]:
        producer.send("steam.players.ccu", "730", r)
    producer.close()
    raw, clean = (tmp_path / "raw").as_uri(), (tmp_path / "clean").as_uri()
    clean_reviews(spark, raw, clean)
    clean_ccu(spark, raw, clean)
    assert [r.recommendationid for r in spark.read.parquet(str(tmp_path / "clean/reviews")).collect()] == ["1"]
    assert [r.player_count for r in spark.read.parquet(str(tmp_path / "clean/ccu")).collect()] == [1000]


def test_merge_cutoff_never_drops_todays_realtime():
    from processing.serving.merge_views import batch_cutoff
    ms = lambda d: int(dt.datetime(2026, 11, d, tzinfo=dt.timezone.utc).timestamp() * 1000)
    today = dt.date(2026, 11, 12)
    assert batch_cutoff(ms(12), today) == "2026-11-12"   # batch has part of today -> keep today's realtime
    assert batch_cutoff(ms(10), today) == "2026-11-11"   # complete older day


def test_es_timestamps_are_written_in_utc(spark):
    """Spark hands back naive local datetimes; ES must receive the same instant in UTC."""
    from processing.common.es_writer import _jsonable
    ts = 1_794_460_000
    row = spark.createDataFrame([(ts,)], ["t"]).selectExpr("CAST(t AS TIMESTAMP) AS w").collect()[0]
    written = dt.datetime.fromisoformat(_jsonable(row.w))
    assert written.utcoffset() == dt.timedelta(0) and written.timestamp() == ts
    assert _jsonable(dt.date(2026, 11, 12)) == "2026-11-12"


def test_merge_query_uses_exact_instant():
    from processing.serving.merge_views import cutoff_query
    assert cutoff_query("2026-11-12") == {"range": {"window_end": {"lte": "2026-11-12T00:00:00Z"}}}


def test_b2_partition_and_file_optimization(spark, tmp_path, monkeypatch):
    """Kiểm chứng sau khi tối ưu:
    - clean/reviews phân vùng theo 'ym', mỗi tháng 1 file Parquet, giữ nguyên cột dt và số dòng.
    - clean/ccu phân vùng theo 'dt', mỗi ngày 1 file Parquet.
    - clean/games, clean/games_history, clean/review_daily_counts được coalesce(1) tạo đúng 1 file Parquet.
    """
    monkeypatch.setenv("HIVE_METASTORE_URI", "")
    from processing.batch.b2_clean_raw import clean_reviews, clean_ccu, clean_games

    producer = JsonlProducer(tmp_path / "raw")
    # 4 reviews trên 4 ngày khác nhau thuộc 2 tháng (2026-01 và 2026-02)
    ts_jan1 = int(dt.datetime(2026, 1, 10, 12, 0, tzinfo=dt.timezone.utc).timestamp())
    ts_jan2 = int(dt.datetime(2026, 1, 15, 12, 0, tzinfo=dt.timezone.utc).timestamp())
    ts_feb1 = int(dt.datetime(2026, 2, 5, 12, 0, tzinfo=dt.timezone.utc).timestamp())
    ts_feb2 = int(dt.datetime(2026, 2, 20, 12, 0, tzinfo=dt.timezone.utc).timestamp())

    reviews = [
        {"recommendationid": "r1", "appid": 730, "review": "great game 1", "voted_up": True, "timestamp_created": ts_jan1, "language": "english"},
        {"recommendationid": "r2", "appid": 730, "review": "great game 2", "voted_up": True, "timestamp_created": ts_jan2, "language": "english"},
        {"recommendationid": "r3", "appid": 730, "review": "bad game 1", "voted_up": False, "timestamp_created": ts_feb1, "language": "english"},
        {"recommendationid": "r4", "appid": 730, "review": "bad game 2", "voted_up": False, "timestamp_created": ts_feb2, "language": "english"},
    ]
    for r in reviews:
        producer.send("steam.reviews.raw", "730", r)

    # CCU trên 2 ngày
    for t, p in [(ts_jan1, 500), (ts_jan1 + 3600, 600), (ts_feb1, 700)]:
        producer.send("steam.players.ccu", "730", {"appid": 730, "timestamp": t, "player_count": p, "source": "steam_live"})

    # Metadata games & histogram
    producer.send("steam.games.meta", "730", {"appid": 730, "name": "CS", "source": "steam_live", "timestamp": ts_jan1, "price": 14.99})
    producer.send("steam.games.meta", "730", {"appid": 730, "source": "steam_review_histogram", "timestamp": ts_jan1, "daily": [{"date": 86400, "up": 10, "down": 2}]})
    producer.close()

    raw_uri, clean_uri = (tmp_path / "raw").as_uri(), (tmp_path / "clean").as_uri()
    clean_reviews(spark, raw_uri, clean_uri)
    clean_ccu(spark, raw_uri, clean_uri)
    clean_games(spark, raw_uri, clean_uri)

    clean_dir = tmp_path / "clean"
    # 1. clean/reviews: phân vùng theo ym (2026-01, 2026-02), mỗi partition đúng 1 file Parquet
    review_parts = list((clean_dir / "reviews").glob("ym=*"))
    assert len(review_parts) == 2, f"Kỳ vọng 2 thư mục partition ym, thực tế: {[p.name for p in review_parts]}"
    for part in review_parts:
        part_files = list(part.glob("*.parquet"))
        assert len(part_files) == 1, f"Partition {part.name} phải có đúng 1 file Parquet, thực tế có {len(part_files)}"

    reviews_df = spark.read.parquet(str(clean_dir / "reviews"))
    assert reviews_df.count() == 4, "Tổng số dòng clean reviews phải giữ nguyên chính xác"
    assert "dt" in reviews_df.columns, "Cột dt phải được giữ nguyên trong schema"
    assert "ym" in reviews_df.columns, "Cột ym phải có trong schema"

    # 2. clean/ccu: phân vùng theo dt, mỗi partition đúng 1 file Parquet
    ccu_parts = list((clean_dir / "ccu").glob("dt=*"))
    assert len(ccu_parts) == 2, f"Kỳ vọng 2 thư mục partition dt cho CCU, thực tế: {[p.name for p in ccu_parts]}"
    for part in ccu_parts:
        part_files = list(part.glob("*.parquet"))
        assert len(part_files) == 1, f"Partition {part.name} của CCU phải có đúng 1 file Parquet, thực tế có {len(part_files)}"

    # 3. clean/games, clean/games_history, clean/review_daily_counts: coalesce(1) -> đúng 1 file Parquet mỗi bảng
    for tbl in ["games", "games_history", "review_daily_counts"]:
        tbl_files = list((clean_dir / tbl).glob("*.parquet"))
        assert len(tbl_files) == 1, f"Bảng {tbl} phải có đúng 1 file Parquet do coalesce(1), thực tế có {len(tbl_files)}"

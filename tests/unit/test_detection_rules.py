import datetime as dt

import pytest

from processing.batch.b4_playtime_buckets import bucket_expr

W = dt.datetime(2026, 1, 1, 10, 0)


def windows(spark, rows):
    cols = ["appid", "window_start", "window_end", "total_reviews", "negative_reviews", "positive_ratio"]
    return spark.createDataFrame([(a, W, W + dt.timedelta(hours=1), t, n, p) for a, t, n, p in rows], cols)


def test_s2_baseline_rule(spark):
    from processing.streaming.s2_review_bombing_detector import detect
    baseline = spark.createDataFrame(
        [(1, 2.0, 1.0, 80.0), (2, 2.0, 1.0, 80.0), (3, 2.0, 1.0, 80.0)],
        ["appid", "neg_per_hour_mean", "neg_per_hour_std", "baseline_positive_ratio"])
    w = windows(spark, [
        (1, 40, 30, 25.0),   # vượt 3σ, giảm 55 điểm -> cảnh báo
        (2, 40, 30, 70.0),   # vượt 3σ nhưng % tích cực chỉ giảm 10 điểm -> không
        (3, 6, 4, 33.0),     # giảm mạnh nhưng ít review tiêu cực (< MIN_NEGATIVE) -> không
    ])
    alerts = {r.appid: r for r in detect(w, baseline).collect()}
    assert set(alerts) == {1}
    assert alerts[1].alert_type == "REVIEW_BOMBING" and alerts[1].rule == "baseline_3sigma"
    assert alerts[1].negative_threshold == pytest.approx(5.0)


def test_s2_fallback_without_baseline(spark):
    from processing.streaming.s2_review_bombing_detector import detect
    w = windows(spark, [(9, 60, 50, 20.0), (8, 20, 18, 10.0)])
    alerts = [r.appid for r in detect(w, None).collect()]
    assert alerts == [9]  # 8 có ít hơn FALLBACK_MIN_REVIEWS


def test_s3_spike_and_drop(spark):
    from processing.streaming.s3_ccu_spike_detector import detect
    w = spark.createDataFrame([(1, W, W, 3000.0), (2, W, W, 400.0), (3, W, W, 1100.0), (4, W, W, 30.0)],
                              ["appid", "window_start", "window_end", "avg_players_window"])
    base = spark.createDataFrame([(a, 10, 1000.0) for a in (1, 2, 3)] + [(4, 10, 10.0)], ["appid", "hour", "baseline_ccu"])
    alerts = {r.appid: r.alert_type for r in detect(w, base).collect()}
    # 3: +10% bình thường; 4: baseline < MIN_BASELINE_CCU nên bỏ qua
    assert alerts == {1: "CCU_SPIKE", 2: "CCU_DROP"}


def test_b4_buckets(spark):
    df = spark.createDataFrame([(h,) for h in (0.5, 2.0, 9.9, 10.0, 49.0, 50.0, 199.0, 200.0, 999.0)], ["h"])
    got = [r.b for r in df.select(bucket_expr(df.h).alias("b")).collect()]
    assert got == ["<2h", "2-10h", "2-10h", "10-50h", "10-50h", "50-200h", "50-200h", ">200h", ">200h"]

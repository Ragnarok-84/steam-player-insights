import argparse
from pyspark.sql.functions import (
    col, count, when, sum as spark_sum, avg, lit, sqrt, greatest, round as spark_round,
    max as spark_max, floor, date_sub, unix_timestamp, concat
)
from processing.common.spark_utils import get_spark_session, path_exists, write_table, CLEAN_PATH, CURATED_PATH



# Dưới ngưỡng này, % tích cực trong cửa sổ baseline quá nhiễu; dùng % tích cực trọn đời Steam công bố (clean/games)
MIN_BASELINE_REVIEWS = 30

BASELINE_COLS = ["appid", "reviews_in_baseline", "neg_per_hour_mean", "neg_per_hour_std",
                 "baseline_positive_ratio", "as_of", "baseline_source"]


def histogram_baseline(spark, clean_root: str, days: int):
    """Baseline review từ clean/review_daily_counts (số review/ngày của Steam), `days` ngày đủ dữ liệu gần nhất.

    Chỉ có số theo ngày, nên phương sai theo giờ ước lượng = Poisson + dao động giữa các ngày:
    Var(giờ) = mean_giờ + Var(ngày) / 24².
    """
    path = f"{clean_root}/review_daily_counts"
    if not path_exists(spark, path):
        return None
    h = spark.read.parquet(path).filter(~col("is_partial"))
    last = h.agg(spark_max("dt")).collect()[0][0]
    if last is None:
        return None
    w = h.filter((col("dt") > date_sub(lit(last), days)) & (col("dt") <= lit(last)))
    hours = days * 24
    return (
        h.select("appid").distinct().join(
            w.groupBy("appid").agg(spark_sum("reviews_down").alias("down"), spark_sum("reviews_up").alias("up"),
                                   spark_sum(col("reviews_down") * col("reviews_down")).alias("down_sq")),
            "appid", "left")
        .fillna(0, subset=["down", "up", "down_sq"])
        .withColumn("reviews_in_baseline", col("down") + col("up"))
        .withColumn("neg_per_hour_mean", col("down") / lit(hours))
        .withColumn("_day_var", greatest(lit(0.0), col("down_sq") / lit(days) - (col("down") / lit(days)) ** 2))
        .withColumn("neg_per_hour_std", sqrt(col("neg_per_hour_mean") + col("_day_var") / lit(24.0 * 24.0)))
        .withColumn("baseline_positive_ratio",
                    when(col("reviews_in_baseline") > 0,
                         spark_round(col("up") * 100.0 / col("reviews_in_baseline"), 2)))
        .withColumn("as_of", unix_timestamp(lit(last).cast("timestamp")) + lit(86400))
        .withColumn("baseline_source", lit("steam_histogram"))
        .select(*BASELINE_COLS)
    )


def with_lifetime_ratio(spark, baseline, clean_root: str):
    """Game ít review trong cửa sổ baseline: thay % tích cực bằng percent_positive trọn đời của Steam."""
    games_path = f"{clean_root}/games"
    if not path_exists(spark, games_path) or "percent_positive" not in spark.read.parquet(games_path).columns:
        return baseline
    life = spark.read.parquet(games_path).select("appid", col("percent_positive").cast("double").alias("_life"))
    small = col("reviews_in_baseline") < MIN_BASELINE_REVIEWS
    return (baseline.join(life, "appid", "left")
            .withColumn("baseline_source", when(small & col("_life").isNotNull(),
                                                concat(col("baseline_source"), lit("+steam_lifetime_ratio")))
                        .otherwise(col("baseline_source")))
            .withColumn("baseline_positive_ratio", when(small & col("_life").isNotNull(), col("_life"))
                        .otherwise(col("baseline_positive_ratio")))
            .drop("_life"))


def compute_baselines(spark, clean_root: str, curated_root: str, days: int):
    """
    Job B7: baseline cho speed layer (S2, S3) — tính lại mỗi lần chạy batch.
    - review_baseline (appid): số review tiêu cực/giờ trong `days` ngày gần nhất -> mean, std (tính cả giờ = 0);
      % tích cực baseline. S2 cảnh báo khi negative_60m > mean + 3σ và % tích cực giảm > 20 điểm.
    - ccu_baseline (appid, hour): CCU TB cùng khung giờ các ngày trước. S3 cảnh báo khi lệch > ±50%.
    Mốc thời gian = bản ghi mới nhất trong dữ liệu (chạy được cả với dữ liệu lịch sử).
    """
    hours = days * 24
    reviews = spark.read.parquet(f"{clean_root}/reviews")
    as_of = reviews.agg(spark_max("timestamp_created")).collect()[0][0]
    recent = reviews.filter(col("timestamp_created") > as_of - hours * 3600) \
        .withColumn("hour_bucket", floor(col("timestamp_created") / 3600))
    hourly = recent.groupBy("appid", "hour_bucket").agg(
        count(when(~col("voted_up"), 1)).alias("neg"), count("*").alias("total"),
        count(when(col("voted_up"), 1)).alias("pos"))
    review_baseline = (
        hourly.groupBy("appid").agg(
            spark_sum("neg").alias("neg_sum"), spark_sum(col("neg") * col("neg")).alias("neg_sq"),
            spark_sum("total").alias("reviews_in_baseline"), spark_sum("pos").alias("pos_sum"))
        .withColumn("neg_per_hour_mean", col("neg_sum") / lit(hours))
        .withColumn("neg_per_hour_std",
                    sqrt(greatest(lit(0.0), col("neg_sq") / lit(hours) - col("neg_per_hour_mean") ** 2)))
        .withColumn("baseline_positive_ratio", spark_round(col("pos_sum") * 100.0 / col("reviews_in_baseline"), 2))
        .withColumn("as_of", lit(as_of))
        .withColumn("baseline_source", lit("crawled"))
        .select(*BASELINE_COLS)
    )
    hist = histogram_baseline(spark, clean_root, days)
    if hist is not None:
        # Histogram đếm đủ mọi review; review đã crawl chỉ dùng cho game không có histogram
        review_baseline = hist.unionByName(review_baseline.join(hist.select("appid"), "appid", "left_anti"))
    review_baseline = with_lifetime_ratio(spark, review_baseline, clean_root)
    write_table(spark, review_baseline, f"{curated_root}/review_baseline", "review_baseline")

    ccu_path = f"{clean_root}/ccu"
    if path_exists(spark, ccu_path):
        ccu = spark.read.parquet(ccu_path)
        ccu_as_of = ccu.agg(spark_max("timestamp")).collect()[0][0]
        ccu_baseline = (
            ccu.filter(col("timestamp") > ccu_as_of - days * 86400)
            .groupBy("appid", "hour").agg(avg("player_count").alias("baseline_ccu"), count("*").alias("samples"))
            .withColumn("as_of", lit(ccu_as_of))
        )
        write_table(spark, ccu_baseline, f"{curated_root}/ccu_baseline", "ccu_baseline")
    print(f"Job B7 finished (reviews as_of={as_of}).")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="B7: Baselines for realtime anomaly detection")
    parser.add_argument("--clean", default=CLEAN_PATH)
    parser.add_argument("--curated", default=CURATED_PATH)
    parser.add_argument("--days", type=int, default=7)
    args = parser.parse_args()

    spark = get_spark_session("B7_Baselines")
    compute_baselines(spark, args.clean, args.curated, args.days)
    spark.stop()

import argparse
from pyspark.sql.functions import (
    col, sum as spark_sum, avg, when, round as spark_round, lit, max as spark_max, lag,
    datediff, coalesce, mean, stddev, row_number, count
)
from pyspark.sql.window import Window
from processing.common.spark_utils import get_spark_session, path_exists, write_table, CLEAN_PATH, CURATED_PATH
from processing.common.es_writer import es_enabled, write_es

EVENT_WINDOW_DAYS = 7


def _growth(cur, prev):
    return when(prev > 0, spark_round((cur - prev) * 100.0 / prev, 2)).otherwise(lit(None))


def compute_trending_games(spark, clean_root: str, curated_root: str, es: bool, top_n: int):
    """
    Job B6:
    - trending: theo (appid, dt) tổng review và CCU TB của 7 ngày gần nhất so với 7 ngày trước đó (cửa sổ theo
      ngày thật, không theo số dòng), z-score toàn cụm -> trending_score; trending_rank tại ngày mới nhất.
    - event_impact: % tích cực 7 ngày trước/sau ngày bắt đầu giảm giá (lấy từ lịch sử snapshot metadata).
    """
    daily = spark.read.parquet(f"{curated_root}/game_daily_stats")
    d = daily.withColumn("day_idx", datediff(col("dt"), lit("1970-01-01")))

    def rng(lo, hi):
        return Window.partitionBy("appid").orderBy("day_idx").rangeBetween(lo, hi)

    cur, prev = rng(-6, 0), rng(-13, -7)
    trend = (
        d.withColumn("reviews_7d", spark_sum("total_reviews").over(cur))
        .withColumn("reviews_prev_7d", spark_sum("total_reviews").over(prev))
        .withColumn("positive_7d", spark_sum("positive_reviews").over(cur))
        .withColumn("avg_ccu_7d", avg("avg_ccu").over(cur))
        .withColumn("avg_ccu_prev_7d", avg("avg_ccu").over(prev))
        .withColumn("review_growth_rate", _growth(col("reviews_7d"), col("reviews_prev_7d")))
        .withColumn("ccu_growth_rate", _growth(col("avg_ccu_7d"), col("avg_ccu_prev_7d")))
        .withColumn("positive_ratio_7d",
                    when(col("reviews_7d") > 0, spark_round(col("positive_7d") * 100.0 / col("reviews_7d"), 2)))
    )

    stats = trend.agg(
        mean("review_growth_rate").alias("rg_mean"), stddev("review_growth_rate").alias("rg_std"),
        mean("ccu_growth_rate").alias("cg_mean"), stddev("ccu_growth_rate").alias("cg_std"),
    ).collect()[0]

    def z(c, m, s):
        return when(col(c).isNotNull() & lit(s is not None and s > 0), (col(c) - lit(m or 0.0)) / lit(s or 1.0)).otherwise(0.0)

    trend = trend.withColumn(
        "trending_score",
        spark_round(0.5 * z("review_growth_rate", stats.rg_mean, stats.rg_std)
                    + 0.5 * z("ccu_growth_rate", stats.cg_mean, stats.cg_std)
                    # bỏ qua game quá ít review để tránh nhiễu tăng trưởng từ mẫu nhỏ
                    , 3) * when(col("reviews_7d") >= 10, 1.0).otherwise(0.0)
    )
    latest_dt = trend.agg(spark_max("dt")).collect()[0][0]
    rank_w = Window.partitionBy("dt").orderBy(col("trending_score").desc(), col("reviews_7d").desc())
    trend = trend.withColumn("trending_rank", row_number().over(rank_w)).drop("day_idx")
    write_table(spark, trend, f"{curated_root}/trending", "trending")

    top = trend.filter((col("dt") == lit(latest_dt)) & (col("trending_rank") <= top_n))
    write_table(spark, top, f"{curated_root}/trending_top", "trending_top")

    impact = None
    history_path = f"{clean_root}/games_history"
    if path_exists(spark, history_path):
        # Ngày bắt đầu đợt giảm giá = snapshot có discount > 0 mà snapshot trước đó (cùng appid) không giảm
        h = spark.read.parquet(history_path).select("appid", "snapshot_dt", "discount_percent")
        w = Window.partitionBy("appid").orderBy("snapshot_dt")
        starts = (h.withColumn("prev_disc", lag("discount_percent").over(w))
                  .filter((col("discount_percent") > 0) & (coalesce(col("prev_disc"), lit(0)) == 0))
                  .select("appid", col("snapshot_dt").alias("event_dt"), "discount_percent")
                  .withColumn("event_type", lit("sale_start")))
        reviews = spark.read.parquet(f"{clean_root}/reviews").select("appid", "dt", "voted_up")
        j = reviews.join(starts, "appid").withColumn("offset", datediff(col("dt"), col("event_dt"))) \
            .filter((col("offset") >= -EVENT_WINDOW_DAYS) & (col("offset") < EVENT_WINDOW_DAYS))
        impact = (
            j.groupBy("appid", "event_dt", "event_type", "discount_percent")
            .agg(
                count(when(col("offset") < 0, 1)).alias("reviews_before"),
                count(when(col("offset") >= 0, 1)).alias("reviews_after"),
                spark_round(avg(when(col("offset") < 0, col("voted_up").cast("double"))) * 100, 2).alias("positive_before"),
                spark_round(avg(when(col("offset") >= 0, col("voted_up").cast("double"))) * 100, 2).alias("positive_after"),
            )
            .withColumn("positive_delta", spark_round(col("positive_after") - col("positive_before"), 2))
        )
        write_table(spark, impact, f"{curated_root}/event_impact", "event_impact")

    if es:
        write_es(spark.read.parquet(f"{curated_root}/trending"), "batch_trending", ["appid", "dt"])
        if impact is not None:
            write_es(spark.read.parquet(f"{curated_root}/event_impact"), "batch_event_impact", ["appid", "event_dt", "event_type"])
    print(f"Job B6 completed. Latest dt={latest_dt}.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="B6: Trending games & event impact")
    parser.add_argument("--clean", default=CLEAN_PATH)
    parser.add_argument("--curated", default=CURATED_PATH)
    parser.add_argument("--top-n", type=int, default=50)
    parser.add_argument("--es-nodes", default=None)
    args = parser.parse_args()

    spark = get_spark_session("B6_Trending_Games")
    compute_trending_games(spark, args.clean, args.curated, es_enabled(args.es_nodes), args.top_n)
    spark.stop()

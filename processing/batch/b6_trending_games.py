import argparse
from pyspark.sql.functions import (
    col, sum as spark_sum, avg, when, round as spark_round, lit, max as spark_max, lag,
    datediff, coalesce, mean, stddev, row_number, count, min as spark_min
)
from pyspark.sql.window import Window
from processing.common.spark_utils import get_spark_session, path_exists, write_table, CLEAN_PATH, CURATED_PATH
from processing.common.es_writer import es_enabled, write_es

EVENT_WINDOW_DAYS = 7


def _growth(cur, prev):
    return when(prev > 0, spark_round((cur - prev) * 100.0 / prev, 2)).otherwise(lit(None))


def with_review_signal(spark, daily, clean_root):
    """Số review mỗi ngày dùng cho trending.

    Ưu tiên clean/review_daily_counts (histogram Steam: đếm đủ mọi review, mọi ngôn ngữ). Review đã crawl chỉ là
    mẫu bị giới hạn số trang ở game lớn, nên chỉ dùng cho game/ngày nằm ngoài vùng histogram phủ.
    Trong vùng phủ, ngày không có trong histogram = 0 review. Bỏ ngày đang đếm dở (is_partial).
    """
    hist_path = f"{clean_root}/review_daily_counts"
    if not path_exists(spark, hist_path):
        return (daily.withColumn("review_signal", col("total_reviews"))
                .withColumn("positive_signal", col("positive_reviews"))
                .withColumn("review_source", lit("crawled")))
    hist = (spark.read.parquet(hist_path).filter(~col("is_partial"))
            .select("appid", "dt", col("reviews_total").alias("hist_total"), col("reviews_up").alias("hist_up")))
    cover = hist.groupBy("appid").agg(spark_min("dt").alias("hist_from"), spark_max("dt").alias("hist_to"))
    games_path = f"{clean_root}/games"
    out = daily.join(hist, ["appid", "dt"], "full_outer").join(cover, "appid", "left")
    in_cover = col("hist_from").isNotNull() & (col("dt") >= col("hist_from")) & (col("dt") <= col("hist_to"))
    out = (out
           .withColumn("review_signal", when(in_cover, coalesce(col("hist_total"), lit(0)))
                       .otherwise(coalesce(col("total_reviews"), lit(0))))
           .withColumn("positive_signal", when(in_cover, coalesce(col("hist_up"), lit(0)))
                       .otherwise(coalesce(col("positive_reviews"), lit(0))))
           .withColumn("review_source", when(in_cover, lit("steam_histogram")).otherwise(lit("crawled")))
           .drop("hist_total", "hist_up", "hist_from", "hist_to"))
    out = out.fillna(0, subset=[c for c in ("total_reviews", "positive_reviews", "negative_reviews") if c in out.columns])
    if path_exists(spark, games_path):  # ngày chỉ có trong histogram chưa có tên game
        names = spark.read.parquet(games_path).select("appid", col("name").alias("_name"))
        out = out.join(names, "appid", "left").withColumn("name", coalesce(col("name"), col("_name"))).drop("_name")
    return out


def compute_trending_games(spark, clean_root: str, curated_root: str, es: bool, top_n: int):
    """
    Job B6:
    - trending: theo (appid, dt) tổng review (histogram Steam, xem with_review_signal) và CCU TB của 7 ngày gần
      nhất so với 7 ngày trước đó (cửa sổ theo
      ngày thật, không theo số dòng), z-score toàn cụm -> trending_score; trending_rank tại ngày mới nhất.
    - event_impact: % tích cực 7 ngày trước/sau ngày bắt đầu giảm giá (lấy từ lịch sử snapshot metadata).
    """
    daily = spark.read.parquet(f"{curated_root}/game_daily_stats")
    daily = with_review_signal(spark, daily, clean_root)
    d = daily.withColumn("day_idx", datediff(col("dt"), lit("1970-01-01")))

    def rng(lo, hi):
        return Window.partitionBy("appid").orderBy("day_idx").rangeBetween(lo, hi)

    cur, prev = rng(-6, 0), rng(-13, -7)
    trend = (
        d.withColumn("reviews_7d", spark_sum("review_signal").over(cur))
        .withColumn("reviews_prev_7d", spark_sum("review_signal").over(prev))
        .withColumn("positive_7d", spark_sum("positive_signal").over(cur))
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
                    , 3) * when(col("reviews_7d") >= 10, 1.0)  # null -> xếp cuối (desc = nulls last)
    )
    # Xếp hạng tại ngày đủ dữ liệu cuối cùng của histogram (hôm nay mới đếm một phần)
    complete = trend.filter(col("review_source") == "steam_histogram")
    latest_dt = (complete if complete.limit(1).count() else trend).agg(spark_max("dt")).collect()[0][0]
    rank_w = Window.partitionBy("dt").orderBy(col("trending_score").desc(), col("reviews_7d").desc())
    trend = trend.withColumn("trending_rank", row_number().over(rank_w)).drop("day_idx")
    write_table(spark, trend, f"{curated_root}/trending", "trending")

    top = trend.filter((col("dt") == lit(latest_dt)) & (col("trending_rank") <= top_n))
    write_table(spark, top, f"{curated_root}/trending_top", "trending_top")

    impact = None
    history_path = f"{clean_root}/games_history"
    if path_exists(spark, history_path):
        # Ngày bắt đầu đợt giảm giá = snapshot có discount > 0 mà snapshot trước đó (cùng appid) không giảm.
        # Cần quan sát được bước chuyển: game đã giảm giá ngay ở snapshot đầu tiên không tính là "bắt đầu".
        h = spark.read.parquet(history_path).select("appid", "snapshot_dt", "discount_percent")
        w = Window.partitionBy("appid").orderBy("snapshot_dt")
        starts = (h.withColumn("prev_disc", lag("discount_percent").over(w))
                  .filter((col("discount_percent") > 0) & (col("prev_disc") == 0))
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

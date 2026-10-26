import argparse
from pyspark.sql.functions import (
    col, count, when, avg, max as spark_max, round as spark_round, lit, percentile_approx,
    date_trunc, to_date, explode_outer, coalesce
)
from processing.common.spark_utils import get_spark_session, path_exists, write_table, CLEAN_PATH, CURATED_PATH
from processing.common.es_writer import es_enabled, write_es


def review_aggs():
    return [
        count("*").alias("total_reviews"),
        count(when(col("voted_up"), 1)).alias("positive_reviews"),
        count(when(~col("voted_up"), 1)).alias("negative_reviews"),
        spark_round(avg(when(col("voted_up"), 1.0).otherwise(0.0)) * 100, 2).alias("positive_ratio_percent"),
        spark_round(avg("votes_up"), 2).alias("avg_votes_up"),
        spark_round(percentile_approx(col("playtime_at_review") / 60.0, 0.5), 2).alias("median_playtime_hours"),
        spark_round(avg("sentiment_score"), 3).alias("avg_sentiment"),
        count(when(col("is_suspicious"), 1)).alias("suspicious_reviews"),
    ]


def compute_game_daily_stats(spark, clean_root: str, curated_root: str, es: bool):
    """
    Job B3: thống kê tổng hợp.
    - game_daily_stats: (appid, dt) số review, % tích cực, giờ chơi trung vị, cảm xúc TB, CCU TB/đỉnh, tên/thể loại/giá
    - game_period_stats: rollup (appid, week|month)
    - segment_stats: (genre | developer | price_bucket, month) -> % tích cực
    - language_stats: phân bố review theo ngôn ngữ và ngày (D1)
    Cần chạy sau B2 và B5 (cảm xúc lấy từ curated/review_sentiment nếu có).
    """
    reviews = spark.read.parquet(f"{clean_root}/reviews")
    sentiment_path = f"{curated_root}/review_sentiment"
    if path_exists(spark, sentiment_path):
        sent = spark.read.parquet(sentiment_path).select("recommendationid", "sentiment_score")
        reviews = reviews.join(sent, "recommendationid", "left")
    else:
        reviews = reviews.withColumn("sentiment_score", lit(None).cast("float"))
    reviews = reviews.cache()

    daily = reviews.groupBy("appid", "dt").agg(*review_aggs())

    ccu_path = f"{clean_root}/ccu"
    if path_exists(spark, ccu_path):
        ccu = spark.read.parquet(ccu_path).groupBy("appid", "dt").agg(
            spark_round(avg("player_count"), 0).alias("avg_ccu"),
            spark_max("player_count").alias("peak_ccu"),
        )
        # full outer: ngày chỉ có CCU (không có review) vẫn có mặt cho B6
        daily = daily.join(ccu, ["appid", "dt"], "full_outer")
    else:
        print(f"No CCU at {ccu_path}; avg_ccu/peak_ccu = null.")
        daily = daily.withColumn("avg_ccu", lit(None).cast("double")).withColumn("peak_ccu", lit(None).cast("int"))
    daily = daily.fillna(0, subset=["total_reviews", "positive_reviews", "negative_reviews", "suspicious_reviews"])

    games_path = f"{clean_root}/games"
    if path_exists(spark, games_path):
        games = spark.read.parquet(games_path).select("appid", "name", "genres", "developer", "price", "price_bucket", "is_free")
    else:
        games = None
    if games is not None:
        daily = daily.join(games, "appid", "left")
    else:
        daily = (daily.withColumn("name", lit(None).cast("string"))
                 .withColumn("genres", lit(None).cast("array<string>"))
                 .withColumn("developer", lit(None).cast("array<string>"))
                 .withColumn("price", lit(None).cast("double"))
                 .withColumn("price_bucket", lit(None).cast("string"))
                 .withColumn("is_free", lit(None).cast("boolean")))

    write_table(spark, daily, f"{curated_root}/game_daily_stats", "game_daily_stats")

    period = None
    for unit in ("week", "month"):
        p = (reviews.withColumn("period_start", to_date(date_trunc(unit, col("dt"))))
             .groupBy("appid", "period_start").agg(*review_aggs())
             .withColumn("period", lit(unit)))
        period = p if period is None else period.unionByName(p)
    write_table(spark, period, f"{curated_root}/game_period_stats", "game_period_stats")

    if games is not None:
        r = reviews.join(games, "appid", "left").withColumn("month", to_date(date_trunc("month", col("dt"))))
        segs = None
        for dim, expr in (("genre", explode_outer("genres")), ("developer", explode_outer("developer")),
                          ("price_bucket", col("price_bucket"))):
            s = (r.withColumn("segment", expr).withColumn("segment", coalesce(col("segment"), lit("unknown")))
                 .groupBy("segment", "month").agg(*review_aggs())
                 .withColumn("dimension", lit(dim)))
            segs = s if segs is None else segs.unionByName(s)
        write_table(spark, segs, f"{curated_root}/segment_stats", "segment_stats")

    lang = reviews.groupBy("language", "dt").agg(count("*").alias("total_reviews"))
    write_table(spark, lang, f"{curated_root}/language_stats", "language_stats")

    if es:
        write_es(spark.read.parquet(f"{curated_root}/game_daily_stats"), "batch_game_daily", ["appid", "dt"])
        write_es(spark.read.parquet(f"{curated_root}/game_period_stats"), "batch_game_period", ["appid", "period", "period_start"])
        if games is not None:
            write_es(spark.read.parquet(f"{curated_root}/segment_stats"), "batch_segment_stats", ["dimension", "segment", "month"])
        write_es(spark.read.parquet(f"{curated_root}/language_stats"), "batch_language_stats", ["language", "dt"])
    reviews.unpersist()
    print("Job B3 finished successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="B3: Game daily / period / segment stats")
    parser.add_argument("--clean", default=CLEAN_PATH)
    parser.add_argument("--curated", default=CURATED_PATH)
    parser.add_argument("--es-nodes", default=None, help="host:port; hoặc đặt WRITE_ES=1")
    args = parser.parse_args()

    spark = get_spark_session("B3_Game_Daily_Stats")
    compute_game_daily_stats(spark, args.clean, args.curated, es_enabled(args.es_nodes))
    spark.stop()

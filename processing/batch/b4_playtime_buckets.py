import argparse
from pyspark.sql.functions import col, when, count, avg, round as spark_round, percentile_approx, lit, coalesce
from processing.common.spark_utils import get_spark_session, path_exists, write_table, CLEAN_PATH, CURATED_PATH
from processing.common.es_writer import es_enabled, write_es

BUCKETS = [(2, "<2h"), (10, "2-10h"), (50, "10-50h"), (200, "50-200h")]


def bucket_expr(hours):
    expr = None
    for limit, label in BUCKETS:
        expr = when(hours < limit, label) if expr is None else expr.when(hours < limit, label)
    return expr.otherwise(">200h")


def analyze_playtime_buckets(spark, clean_root: str, curated_root: str, es: bool):
    """
    Job B4: giờ chơi khi review <-> đánh giá.
    - playtime_buckets: % tích cực theo (appid, nhóm giờ chơi)
    - game_retention: early_negative_ratio (review tiêu cực khi chơi <2h / tổng review) = chỉ báo bỏ game;
      giờ chơi trung vị; value_score = giờ chơi trung vị / giá ("đáng tiền")
    """
    df = spark.read.parquet(f"{clean_root}/reviews").withColumn("playtime_hours", col("playtime_at_review") / 60.0)
    bucketed = df.withColumn("playtime_bucket", bucket_expr(col("playtime_hours")))

    buckets = (
        bucketed.groupBy("appid", "playtime_bucket")
        .agg(
            count("*").alias("total_reviews"),
            count(when(col("voted_up"), 1)).alias("positive_reviews"),
            count(when(~col("voted_up"), 1)).alias("negative_reviews"),
            spark_round(avg(when(col("voted_up"), 1.0).otherwise(0.0)) * 100, 2).alias("positive_ratio_percent"),
            spark_round(avg("playtime_hours"), 2).alias("avg_playtime_in_bucket"),
        )
    )
    write_table(spark, buckets, f"{curated_root}/playtime_buckets", "playtime_buckets")

    retention = (
        df.groupBy("appid")
        .agg(
            count("*").alias("total_reviews"),
            spark_round(avg(when(col("voted_up"), 1.0).otherwise(0.0)) * 100, 2).alias("positive_ratio_percent"),
            spark_round(count(when((col("playtime_hours") < 2) & ~col("voted_up"), 1)) * 100.0 / count("*"), 2)
            .alias("early_negative_ratio"),
            spark_round(percentile_approx("playtime_hours", 0.5), 2).alias("median_playtime_hours"),
            spark_round(percentile_approx(col("playtime_forever") / 60.0, 0.5), 2).alias("median_playtime_forever_hours"),
        )
    )
    games_path = f"{clean_root}/games"
    if path_exists(spark, games_path):
        retention = retention.join(spark.read.parquet(games_path).select("appid", "name", "price", "price_bucket"), "appid", "left")
    else:
        retention = retention.withColumn("name", lit(None).cast("string")).withColumn("price", lit(None).cast("double")) \
            .withColumn("price_bucket", lit(None).cast("string"))
    retention = retention.withColumn(
        "value_score",
        when(coalesce(col("price"), lit(0.0)) > 0, spark_round(col("median_playtime_forever_hours") / col("price"), 3))
    )
    write_table(spark, retention, f"{curated_root}/game_retention", "game_retention")

    if es:
        write_es(spark.read.parquet(f"{curated_root}/playtime_buckets"), "batch_playtime_buckets", ["appid", "playtime_bucket"])
        write_es(spark.read.parquet(f"{curated_root}/game_retention"), "batch_game_retention", ["appid"])
    print("Job B4 finished successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="B4: Playtime buckets & retention")
    parser.add_argument("--clean", default=CLEAN_PATH)
    parser.add_argument("--curated", default=CURATED_PATH)
    parser.add_argument("--es-nodes", default=None)
    args = parser.parse_args()

    spark = get_spark_session("B4_Playtime_Buckets")
    analyze_playtime_buckets(spark, args.clean, args.curated, es_enabled(args.es_nodes))
    spark.stop()

import argparse
from pyspark.sql.functions import (
    col, count, when, sum as spark_sum, avg, lit, sqrt, greatest, round as spark_round,
    max as spark_max, floor
)
from processing.common.spark_utils import get_spark_session, path_exists, write_table, CLEAN_PATH, CURATED_PATH



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
        .select("appid", "reviews_in_baseline", "neg_per_hour_mean", "neg_per_hour_std", "baseline_positive_ratio", "as_of")
    )
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

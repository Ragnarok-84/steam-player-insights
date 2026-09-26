import os
import argparse
from pyspark.sql.functions import col, when, count, avg, round as spark_round
from processing.common.spark_utils import get_spark_session

def analyze_playtime_buckets(spark, clean_reviews_path: str, curated_path: str):
    """
    Job B4: Phân tích tương quan giữa số giờ chơi và mức độ đánh giá:
    - Nhóm giờ chơi:
      * < 2h: nhóm trải nghiệm ban đầu / có thể hoàn tiền (refund)
      * 2 - 10h: nhóm làm quen
      * 10 - 50h: người chơi thông thường
      * 50 - 200h: người chơi trung thành
      * > 200h: hardcore gamer
    - % đánh giá tích cực của từng nhóm
    - Tỷ lệ phàn nàn/bỏ game sớm (<2h)
    """
    print(f"Reading clean reviews from {clean_reviews_path}...")
    df = spark.read.parquet(clean_reviews_path)

    # Chuyển playtime_forever và playtime_at_review sang giờ (dữ liệu Steam tính bằng phút)
    df_hours = df.withColumn("playtime_hours", col("playtime_at_review") / 60.0)

    # Phân nhóm giờ chơi
    bucketed_df = df_hours.withColumn(
        "playtime_bucket",
        when(col("playtime_hours") < 2.0, "<2h")
        .when((col("playtime_hours") >= 2.0) & (col("playtime_hours") < 10.0), "2-10h")
        .when((col("playtime_hours") >= 10.0) & (col("playtime_hours") < 50.0), "10-50h")
        .when((col("playtime_hours") >= 50.0) & (col("playtime_hours") < 200.0), "50-200h")
        .otherwise(">200h")
    )

    stats_df = (
        bucketed_df.groupBy("appid", "playtime_bucket")
        .agg(
            count("recommendationid").alias("total_reviews"),
            count(when(col("voted_up") == True, 1)).alias("positive_reviews"),
            count(when(col("voted_up") == False, 1)).alias("negative_reviews"),
            spark_round(avg(when(col("voted_up") == True, 1.0).otherwise(0.0)) * 100, 2).alias("positive_ratio_percent"),
            spark_round(avg("playtime_hours"), 2).alias("avg_playtime_in_bucket")
        )
    )

    out_path = os.path.join(curated_path, "playtime_buckets")
    print(f"Writing playtime buckets analysis to {out_path}...")
    stats_df.write.mode("overwrite").parquet(out_path)
    print("Job B4 completed successfully.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="B4: Playtime Buckets Analysis")
    parser.add_argument("--clean-reviews", default="/steam/clean/reviews", help="Clean reviews path")
    parser.add_argument("--curated", default="/steam/curated", help="Curated output path")
    args = parser.parse_args()

    spark = get_spark_session("B4_Playtime_Buckets")
    analyze_playtime_buckets(spark, args.clean_reviews, args.curated)
    spark.stop()

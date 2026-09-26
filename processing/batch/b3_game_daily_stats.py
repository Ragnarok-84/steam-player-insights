import os
import argparse
from pyspark.sql.functions import (
    col, count, when, avg, max as spark_max, round as spark_round, lit
)
from processing.common.spark_utils import get_spark_session

def compute_game_daily_stats(spark, clean_reviews_path: str, ccu_path: str, curated_path: str, es_nodes: str = None):
    """
    Job B3: Thống kê tổng hợp theo game và ngày:
    - Tổng số review
    - Tỷ lệ % tích cực (voted_up)
    - Số vote hữu ích trung bình
    - CCU trung bình & CCU đỉnh
    Ghi vào HDFS curated và đẩy vào Elasticsearch (batch_game_daily).
    """
    print(f"Reading clean reviews from {clean_reviews_path}...")
    reviews_df = spark.read.parquet(clean_reviews_path)

    # Thống kê review theo appid và ngày dt
    review_stats = (
        reviews_df.groupBy("appid", "dt")
        .agg(
            count("recommendationid").alias("total_reviews"),
            count(when(col("voted_up") == True, 1)).alias("positive_reviews"),
            count(when(col("voted_up") == False, 1)).alias("negative_reviews"),
            spark_round(avg(when(col("voted_up") == True, 1.0).otherwise(0.0)) * 100, 2).alias("positive_ratio_percent"),
            spark_round(avg("votes_up"), 2).alias("avg_votes_up"),
            spark_round(avg("playtime_at_review") / 60.0, 2).alias("avg_playtime_hours_at_review")
        )
    )

    final_df = review_stats

    # Nếu có dữ liệu CCU thì tính thêm CCU avg và CCU max
    if os.path.exists(ccu_path) or ccu_path.startswith("hdfs://"):
        try:
            ccu_df = spark.read.parquet(ccu_path)
            ccu_stats = (
                ccu_df.groupBy("appid", "dt")
                .agg(
                    spark_round(avg("player_count"), 0).alias("avg_ccu"),
                    spark_max("player_count").alias("peak_ccu")
                )
            )
            final_df = review_stats.join(ccu_stats, on=["appid", "dt"], how="left")
        except Exception as e:
            print(f"CCU data not available or failed to join: {e}")
            final_df = final_df.withColumn("avg_ccu", lit(0)).withColumn("peak_ccu", lit(0))

    # Ghi vào HDFS Curated
    out_curated = os.path.join(curated_path, "game_daily_stats")
    print(f"Writing curated daily stats to {out_curated}...")
    final_df.write.mode("overwrite").parquet(out_curated)

    # Ghi vào Elasticsearch nếu có kết nối
    if es_nodes:
        print(f"Writing to Elasticsearch index: batch_game_daily at {es_nodes}...")
        try:
            (
                final_df.write
                .format("org.elasticsearch.spark.sql")
                .option("es.nodes", es_nodes)
                .option("es.resource", "batch_game_daily")
                .option("es.mapping.id", "appid")
                .mode("append")
                .save()
            )
        except Exception as es_err:
            print(f"Warning: Failed to write to Elasticsearch: {es_err}")

    print("Job B3 finished successfully.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="B3: Compute Game Daily Stats")
    parser.add_argument("--clean-reviews", default="/steam/clean/reviews", help="Clean reviews path")
    parser.add_argument("--clean-ccu", default="/steam/clean/ccu", help="Clean CCU path")
    parser.add_argument("--curated", default="/steam/curated", help="Curated storage path")
    parser.add_argument("--es-nodes", default=os.getenv("ELASTICSEARCH_HOSTS"), help="ES host:port")
    args = parser.parse_args()

    spark = get_spark_session("B3_Game_Daily_Stats")
    compute_game_daily_stats(spark, args.clean_reviews, args.clean_ccu, args.curated, args.es_nodes)
    spark.stop()

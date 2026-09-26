import os
import argparse
from pyspark.sql.functions import (
    col, from_json, window, count, when, avg, round as spark_round, to_timestamp, from_unixtime
)
from processing.common.spark_utils import get_spark_session
from processing.batch.b2_clean_raw import REVIEW_SCHEMA

def run_realtime_review_metrics(bootstrap_servers: str, es_nodes: str = None, checkpoint_dir: str = None):
    """
    Job S1: Spark Structured Streaming
    Cửa sổ trượt 60 phút, bước 5 phút; watermark 10 phút.
    Tính số review mới và % tích cực theo appid trong thời gian thực.
    Ghi vào Elasticsearch realtime_game_metrics.
    """
    spark = get_spark_session("S1_Realtime_Review_Metrics")

    # Đọc stream từ Kafka
    kafka_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", "steam.reviews.raw")
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = (
        kafka_stream.select(
            from_json(col("value").cast("string"), REVIEW_SCHEMA).alias("data")
        ).select("data.*")
    )

    # Thêm timestamp event-time từ timestamp_created
    with_event_time = (
        parsed
        .withColumn("event_time", to_timestamp(from_unixtime(col("timestamp_created"))))
        .withWatermark("event_time", "10 minutes")
    )

    # Cửa sổ trượt 60 phút, bước trượt 5 phút
    aggregated = (
        with_event_time
        .groupBy(
            window(col("event_time"), "60 minutes", "5 minutes"),
            col("appid")
        )
        .agg(
            count("recommendationid").alias("review_count_window"),
            count(when(col("voted_up") == True, 1)).alias("positive_count_window"),
            count(when(col("voted_up") == False, 1)).alias("negative_count_window"),
            spark_round(avg(when(col("voted_up") == True, 1.0).otherwise(0.0)) * 100, 2).alias("positive_ratio_percent")
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("appid"),
            col("review_count_window"),
            col("positive_count_window"),
            col("negative_count_window"),
            col("positive_ratio_percent")
        )
    )

    # Ghi ra console hoặc Elasticsearch
    chk_path = checkpoint_dir or os.path.join(os.getenv("HDFS_CHECKPOINT_PATH", "/tmp/checkpoints"), "s1_metrics")

    query = (
        aggregated.writeStream
        .outputMode("update")
        .format("console")
        .option("checkpointLocation", chk_path)
        .start()
    )

    query.awaitTermination()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="S1: Realtime Review Metrics")
    parser.add_argument("--brokers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"), help="Kafka servers")
    parser.add_argument("--checkpoint", default="/steam/checkpoints/s1_metrics", help="Checkpoint dir")
    args = parser.parse_args()

    run_realtime_review_metrics(args.brokers, checkpoint_dir=args.checkpoint)

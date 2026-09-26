import os
import json
import argparse
from pyspark.sql.functions import (
    col, from_json, window, count, when, avg, to_timestamp, from_unixtime, to_json, struct
)
from processing.common.spark_utils import get_spark_session
from processing.batch.b2_clean_raw import REVIEW_SCHEMA

def detect_review_bombing(bootstrap_servers: str, alerts_topic: str = "steam.alerts", checkpoint_dir: str = None):
    """
    Job S2: Phát hiện review bombing thời gian thực
    Cửa sổ 60 phút: nếu số review tiêu cực tăng đột biến (vượt ngưỡng hoặc baseline)
    và tỷ lệ tích cực giảm mạnh, phát sinh cảnh báo vào topic steam.alerts và Elasticsearch.
    """
    spark = get_spark_session("S2_Review_Bombing_Detector")

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", "steam.reviews.raw")
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = (
        raw_stream.select(
            from_json(col("value").cast("string"), REVIEW_SCHEMA).alias("data")
        ).select("data.*")
        .withColumn("event_time", to_timestamp(from_unixtime(col("timestamp_created"))))
        .withWatermark("event_time", "15 minutes")
    )

    windowed = (
        parsed
        .groupBy(
            window(col("event_time"), "60 minutes", "10 minutes"),
            col("appid")
        )
        .agg(
            count("recommendationid").alias("total_reviews"),
            count(when(col("voted_up") == False, 1)).alias("negative_reviews"),
            avg(when(col("voted_up") == True, 1.0).otherwise(0.0)).alias("positive_ratio")
        )
    )

    # Điều kiện phát hiện nghi vấn Review Bombing:
    # Ví dụ: > 50 review trong 1 giờ và tỷ lệ tích cực dưới 30% (hoặc negative review chiếm áp đảo)
    alerts = (
        windowed
        .filter((col("total_reviews") >= 50) & (col("positive_ratio") <= 0.35))
        .select(
            col("appid").cast("string").alias("key"),
            to_json(struct(
                col("appid"),
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("total_reviews"),
                col("negative_reviews"),
                col("positive_ratio"),
                col("appid").alias("alert_type") # "REVIEW_BOMBING_DETECTED"
            )).alias("value")
        )
    )

    chk_path = checkpoint_dir or os.path.join(os.getenv("HDFS_CHECKPOINT_PATH", "/tmp/checkpoints"), "s2_alerts")

    # Gửi cảnh báo ngược lại vào Kafka topic steam.alerts
    query = (
        alerts.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("topic", alerts_topic)
        .option("checkpointLocation", chk_path)
        .outputMode("update")
        .start()
    )

    query.awaitTermination()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="S2: Review Bombing Detector")
    parser.add_argument("--brokers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"), help="Kafka brokers")
    parser.add_argument("--checkpoint", default="/steam/checkpoints/s2_bombing", help="Checkpoint dir")
    args = parser.parse_args()

    detect_review_bombing(args.brokers, checkpoint_dir=args.checkpoint)

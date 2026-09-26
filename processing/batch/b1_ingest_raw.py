import os
import argparse
from datetime import datetime
from pyspark.sql.functions import col, from_utc_timestamp, date_format
from processing.common.spark_utils import get_spark_session

def ingest_kafka_to_raw(spark, topic_name: str, raw_target_dir: str, bootstrap_servers: str):
    """
    Job B1: Đọc Kafka batch và ghi nguyên trạng (immutable raw) xuống HDFS phân vùng theo ngày dt=YYYY-MM-DD.
    Định dạng: JSON Lines nén gzip.
    """
    print(f"Reading from Kafka topic {topic_name} at {bootstrap_servers}...")
    df = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic_name)
        .option("startingOffsets", "earliest")
        .load()
    )

    # Chuyển message value dạng string
    raw_df = df.selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as json_value", "timestamp")
    
    # Tạo cột ngày để phân vùng
    raw_df = raw_df.withColumn("dt", date_format(col("timestamp"), "yyyy-MM-dd"))

    # Ghi dạng text/json với nén gzip
    output_path = os.path.join(raw_target_dir, topic_name.replace(".", "_"))
    print(f"Writing raw immutable JSON gzip to: {output_path}")

    (
        raw_df.write
        .mode("append")
        .partitionBy("dt")
        .option("compression", "gzip")
        .text(output_path)
    )
    print("Ingestion completed successfully.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="B1: Ingest Kafka into HDFS Raw Layer")
    parser.add_argument("--topic", default="steam.reviews.raw", help="Kafka topic to ingest")
    parser.add_argument("--target", default="/steam/raw", help="Target HDFS path")
    parser.add_argument("--brokers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"), help="Kafka brokers")
    args = parser.parse_args()

    spark = get_spark_session(f"B1_Ingest_{args.topic}")
    ingest_kafka_to_raw(spark, args.topic, args.target, args.brokers)
    spark.stop()

import os
import json
import argparse
from pyspark.sql.functions import col, date_format
from processing.common.spark_utils import (
    get_spark_session, path_exists, RAW_PATH, CHECKPOINT_PATH, TOPIC_DIRS
)


def _read_offsets(spark, path: str):
    if not path_exists(spark, path):
        return None
    return spark.read.text(path).collect()[0][0]


def _write_offsets(spark, path: str, offsets: dict):
    spark.createDataFrame([(json.dumps(offsets),)], ["value"]).coalesce(1).write.mode("overwrite").text(path)


def ingest_kafka_to_raw(spark, topic_name: str, raw_target_dir: str, bootstrap_servers: str, offsets_dir: str):
    """
    Job B1: Đọc Kafka (batch) và ghi nguyên trạng xuống vùng raw, phân vùng dt=YYYY-MM-DD theo thời điểm nhập.
    Định dạng JSON Lines gzip, cùng layout với crawlers/collect_local.py.
    Offset cuối được lưu ở {offsets_dir}/{topic}; lần chạy sau đọc tiếp từ đó nên không ghi trùng.
    """
    offsets_path = f"{offsets_dir}/{TOPIC_DIRS[topic_name]}"
    starting = _read_offsets(spark, offsets_path) or "earliest"
    print(f"Reading {topic_name} from {bootstrap_servers}, startingOffsets={starting}")

    df = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic_name)
        .option("startingOffsets", starting)
        .option("endingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
        .cache()
    )
    if df.isEmpty():
        print("No new messages.")
        return

    raw_df = (
        df.select(col("value").cast("string").alias("json_value"),
                  date_format(col("timestamp"), "yyyy-MM-dd").alias("dt"))
    )
    output_path = f"{raw_target_dir}/{TOPIC_DIRS[topic_name]}"
    print(f"Writing raw JSON gzip to: {output_path}")
    (
        raw_df.write.mode("append")
        .partitionBy("dt")
        .option("compression", "gzip")
        .text(output_path)
    )

    # Offset tiếp theo = max(offset) + 1 cho từng partition
    last = df.groupBy("partition").max("offset").collect()
    offsets = {topic_name: {str(r["partition"]): r["max(offset)"] + 1 for r in last}}
    previous = json.loads(starting) if starting != "earliest" else {topic_name: {}}
    previous[topic_name].update(offsets[topic_name])
    _write_offsets(spark, offsets_path, previous)
    df.unpersist()
    print(f"Ingested {sum(1 for _ in last)} partitions; offsets saved to {offsets_path}.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="B1: Ingest Kafka into Raw Layer")
    parser.add_argument("--topic", default="all", choices=["all"] + list(TOPIC_DIRS), help="Kafka topic, hoặc all")
    parser.add_argument("--target", default=RAW_PATH, help="Raw root")
    parser.add_argument("--brokers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--offsets-dir", default=f"{CHECKPOINT_PATH}/b1_offsets")
    args = parser.parse_args()

    spark = get_spark_session("B1_Ingest_Raw")
    topics = list(TOPIC_DIRS) if args.topic == "all" else [args.topic]
    for topic in topics:
        ingest_kafka_to_raw(spark, topic, args.target, args.brokers, args.offsets_dir)
    spark.stop()

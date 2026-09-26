import os
import argparse
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
from pyspark.sql.functions import (
    col, from_json, window, avg, max as spark_max, to_timestamp, from_unixtime, to_json, struct
)
from processing.common.spark_utils import get_spark_session

CCU_SCHEMA = StructType([
    StructField("appid", IntegerType(), True),
    StructField("timestamp", LongType(), True),
    StructField("player_count", IntegerType(), True),
])

def detect_ccu_spikes(bootstrap_servers: str, alerts_topic: str = "steam.alerts", checkpoint_dir: str = None):
    """
    Job S3: Giám sát CCU và phát hiện bất thường:
    - Theo dõi CCU qua cửa sổ 10 phút
    - Phát hiện sụt giảm hoặc đột biến người chơi bất thường (như sập server, ra mắt update lớn)
    """
    spark = get_spark_session("S3_CCU_Spike_Detector")

    stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", "steam.players.ccu")
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = (
        stream.select(
            from_json(col("value").cast("string"), CCU_SCHEMA).alias("data")
        ).select("data.*")
        .withColumn("event_time", to_timestamp(from_unixtime(col("timestamp"))))
        .withWatermark("event_time", "10 minutes")
    )

    windowed = (
        parsed
        .groupBy(
            window(col("event_time"), "10 minutes", "5 minutes"),
            col("appid")
        )
        .agg(
            avg("player_count").alias("avg_players_window"),
            spark_max("player_count").alias("max_players_window")
        )
    )

    chk_path = checkpoint_dir or os.path.join(os.getenv("HDFS_CHECKPOINT_PATH", "/tmp/checkpoints"), "s3_ccu")

    query = (
        windowed.writeStream
        .outputMode("update")
        .format("console")
        .option("checkpointLocation", chk_path)
        .start()
    )

    query.awaitTermination()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="S3: CCU Spike Detector")
    parser.add_argument("--brokers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"), help="Kafka brokers")
    parser.add_argument("--checkpoint", default="/steam/checkpoints/s3_ccu", help="Checkpoint dir")
    args = parser.parse_args()

    detect_ccu_spikes(args.brokers, checkpoint_dir=args.checkpoint)

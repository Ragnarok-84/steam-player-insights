import os
import argparse
from pyspark.sql.functions import (
    col, from_json, window, avg, max as spark_max, to_timestamp, to_json, struct, lit, hour, when,
    round as spark_round, current_timestamp
)
from processing.common.spark_utils import (
    get_spark_session, read_stream, add_stream_args, attach_progress_logger, path_exists, CHECKPOINT_PATH
)
from processing.common.es_writer import es_enabled, write_es
from processing.batch.b2_clean_raw import CCU_SCHEMA

QUERY_NAME = "s3_ccu"
CHANGE_THRESHOLD = float(os.getenv("S3_CHANGE_THRESHOLD", "0.5"))
MIN_BASELINE_CCU = float(os.getenv("S3_MIN_BASELINE_CCU", "100"))


def detect(windowed, baseline):
    """CCU cửa sổ hiện tại so với CCU TB cùng khung giờ các ngày trước (curated/ccu_baseline từ B7)."""
    j = (windowed.withColumn("hour", hour(col("window_start")))
         .join(baseline, ["appid", "hour"], "inner")
         .withColumn("change_ratio", spark_round((col("avg_players_window") - col("baseline_ccu")) / col("baseline_ccu"), 3)))
    return (
        j.filter((col("baseline_ccu") >= MIN_BASELINE_CCU) & ~col("change_ratio").between(-CHANGE_THRESHOLD, CHANGE_THRESHOLD))
        .select(
            "appid",
            when(col("change_ratio") > 0, "CCU_SPIKE").otherwise("CCU_DROP").alias("alert_type"),
            lit("hourly_baseline_50pct").alias("rule"),
            "window_start", "window_end", "avg_players_window", "baseline_ccu", "change_ratio",
            current_timestamp().alias("detected_at"),
        )
    )


def run(args):
    """
    Job S3: CCU theo cửa sổ 10 phút; ghi CCU hiện tại vào realtime_game_metrics và
    cảnh báo CCU_SPIKE / CCU_DROP khi lệch > ±50% so với cùng khung giờ các ngày trước.
    """
    spark = get_spark_session("S3_CCU_Spike_Detector")
    attach_progress_logger(spark, args.metrics_file)
    raw = read_stream(spark, "steam.players.ccu", args.source, args.brokers, args.raw, args.max_files, args.starting_offsets)
    windowed = (
        raw.select(from_json(col("value"), CCU_SCHEMA).alias("d")).select("d.*")
        .filter(col("appid").isNotNull() & col("timestamp").isNotNull())
        .withColumn("event_time", to_timestamp(col("timestamp")))
        .withWatermark("event_time", "10 minutes")
        .groupBy(window(col("event_time"), "10 minutes"), col("appid"))
        .agg(spark_round(avg("player_count"), 0).alias("avg_players_window"),
             spark_max("player_count").alias("max_players_window"))
        .select(col("window.start").alias("window_start"), col("window.end").alias("window_end"), "*")
        .drop("window")
    )
    baseline_path = f"{args.curated}/ccu_baseline"
    es = es_enabled(args.es_nodes)

    def handle(batch_df, batch_id):
        batch_df = batch_df.cache()
        metrics = batch_df.withColumn("metric_type", lit("ccu")).withColumn("window_size", lit("10m")) \
            .withColumn("current_ccu", col("avg_players_window"))
        if es:
            write_es(metrics, "realtime_game_metrics", ["metric_type", "appid", "window_size", "window_start"])
        else:
            metrics.orderBy(col("current_ccu").desc()).show(10, truncate=False)
        if path_exists(spark, baseline_path):
            alerts = detect(batch_df, spark.read.parquet(baseline_path)).cache()
            if not alerts.isEmpty():
                if args.source == "kafka":
                    (alerts.select(col("appid").cast("string").alias("key"), to_json(struct("*")).alias("value"))
                     .write.format("kafka").option("kafka.bootstrap.servers", args.brokers)
                     .option("topic", args.alerts_topic).save())
                if es:
                    write_es(alerts, "alerts", ["alert_type", "appid", "window_start"])
                else:
                    alerts.show(truncate=False)
            alerts.unpersist()
        batch_df.unpersist()

    (windowed.writeStream.queryName(QUERY_NAME).outputMode("update").foreachBatch(handle)
     .option("checkpointLocation", args.checkpoint).trigger(processingTime=args.trigger).start()
     .awaitTermination())


if __name__ == "__main__":
    parser = add_stream_args(argparse.ArgumentParser(description="S3: CCU Spike Detector"))
    parser.add_argument("--checkpoint", default=f"{CHECKPOINT_PATH}/s3_ccu")
    parser.add_argument("--alerts-topic", default=os.getenv("KAFKA_TOPIC_ALERTS", "steam.alerts"))
    run(parser.parse_args())

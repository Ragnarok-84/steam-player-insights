import os
import argparse
from pyspark.sql.functions import (
    col, from_json, window, count, when, avg, to_timestamp, to_json, struct, lit, coalesce,
    round as spark_round, current_timestamp
)
from processing.common.spark_utils import (
    get_spark_session, read_stream, add_stream_args, path_exists, CHECKPOINT_PATH
)
from processing.common.es_writer import es_enabled, write_es
from processing.batch.b2_clean_raw import REVIEW_SCHEMA

SIGMA = float(os.getenv("S2_SIGMA", "3"))
DROP_POINTS = float(os.getenv("S2_POSITIVE_DROP_POINTS", "20"))
MIN_NEGATIVE = int(os.getenv("S2_MIN_NEGATIVE", "10"))
# Khi chưa có baseline (game mới / chưa chạy B7): ngưỡng tuyệt đối dự phòng
FALLBACK_MIN_REVIEWS = int(os.getenv("S2_FALLBACK_MIN_REVIEWS", "50"))
FALLBACK_MAX_POSITIVE = float(os.getenv("S2_FALLBACK_MAX_POSITIVE", "35"))


def detect(windowed, baseline):
    """Áp luật review bombing lên DataFrame tĩnh (dùng trong foreachBatch và unit test)."""
    j = windowed.join(baseline, "appid", "left") if baseline is not None else \
        windowed.withColumn("neg_per_hour_mean", lit(None).cast("double")) \
                .withColumn("neg_per_hour_std", lit(None).cast("double")) \
                .withColumn("baseline_positive_ratio", lit(None).cast("double"))
    has_base = col("neg_per_hour_mean").isNotNull()
    threshold = col("neg_per_hour_mean") + lit(SIGMA) * col("neg_per_hour_std")
    rule_baseline = (
        has_base
        & (col("negative_reviews") >= MIN_NEGATIVE)
        & (col("negative_reviews") > threshold)
        & (col("positive_ratio") < col("baseline_positive_ratio") - DROP_POINTS)
    )
    rule_fallback = (~has_base) & (col("total_reviews") >= FALLBACK_MIN_REVIEWS) & (col("positive_ratio") <= FALLBACK_MAX_POSITIVE)
    return (
        j.filter(rule_baseline | rule_fallback)
        .select(
            col("appid"),
            lit("REVIEW_BOMBING").alias("alert_type"),
            when(rule_baseline, "baseline_3sigma").otherwise("fallback_threshold").alias("rule"),
            "window_start", "window_end", "total_reviews", "negative_reviews", "positive_ratio",
            spark_round(threshold, 2).alias("negative_threshold"),
            col("baseline_positive_ratio"),
            spark_round(coalesce(col("baseline_positive_ratio"), lit(None)) - col("positive_ratio"), 2).alias("positive_drop_points"),
            current_timestamp().alias("detected_at"),
        )
    )


def run(args):
    """
    Job S2: cửa sổ 60 phút (trượt 10 phút) so với baseline 7 ngày (curated/review_baseline từ B7):
    cảnh báo khi review tiêu cực > mean + 3σ VÀ % tích cực giảm > 20 điểm so với baseline.
    Ghi Kafka steam.alerts (khi nguồn là Kafka) và ES alerts.
    """
    spark = get_spark_session("S2_Review_Bombing_Detector")
    raw = read_stream(spark, "steam.reviews.raw", args.source, args.brokers, args.raw, args.max_files, args.starting_offsets)
    windowed = (
        raw.select(from_json(col("value"), REVIEW_SCHEMA).alias("d")).select("d.*")
        .filter(col("appid").isNotNull() & col("timestamp_created").isNotNull())
        .withColumn("event_time", to_timestamp(col("timestamp_created")))
        .withWatermark("event_time", "15 minutes")
        .groupBy(window(col("event_time"), "60 minutes", "10 minutes"), col("appid"))
        .agg(
            count("*").alias("total_reviews"),
            count(when(~col("voted_up"), 1)).alias("negative_reviews"),
            spark_round(avg(when(col("voted_up"), 1.0).otherwise(0.0)) * 100, 2).alias("positive_ratio"),
        )
        .select(col("window.start").alias("window_start"), col("window.end").alias("window_end"), "*")
        .drop("window")
    )
    baseline_path = f"{args.curated}/review_baseline"
    es = es_enabled(args.es_nodes)

    def handle(batch_df, batch_id):
        # Đọc lại baseline mỗi micro-batch để nhận bản mới sau mỗi lần chạy batch layer
        baseline = spark.read.parquet(baseline_path) if path_exists(spark, baseline_path) else None
        alerts = detect(batch_df, baseline).cache()
        if alerts.isEmpty():
            alerts.unpersist()
            return
        if args.source == "kafka":
            (alerts.select(col("appid").cast("string").alias("key"), to_json(struct("*")).alias("value"))
             .write.format("kafka").option("kafka.bootstrap.servers", args.brokers).option("topic", args.alerts_topic).save())
        if es:
            write_es(alerts, "alerts", ["alert_type", "appid", "window_start"])
        else:
            alerts.show(truncate=False)
        alerts.unpersist()

    (windowed.writeStream.outputMode("update").foreachBatch(handle)
     .option("checkpointLocation", args.checkpoint).trigger(processingTime=args.trigger).start()
     .awaitTermination())


if __name__ == "__main__":
    parser = add_stream_args(argparse.ArgumentParser(description="S2: Review Bombing Detector"))
    parser.add_argument("--checkpoint", default=f"{CHECKPOINT_PATH}/s2_bombing")
    parser.add_argument("--alerts-topic", default=os.getenv("KAFKA_TOPIC_ALERTS", "steam.alerts"))
    run(parser.parse_args())

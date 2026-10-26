import argparse
from pyspark.sql.functions import (
    col, from_json, window, count, when, avg, round as spark_round, to_timestamp, lit
)
from processing.common.spark_utils import get_spark_session, read_stream, add_stream_args, CHECKPOINT_PATH
from processing.common.es_writer import es_enabled, write_es
from processing.batch.b2_clean_raw import REVIEW_SCHEMA


def window_agg(events, duration: str, slide: str, label: str):
    return (
        events.groupBy(window(col("event_time"), duration, slide), col("appid"))
        .agg(
            count("*").alias("review_count_window"),
            count(when(col("voted_up"), 1)).alias("positive_count_window"),
            count(when(~col("voted_up"), 1)).alias("negative_count_window"),
            spark_round(avg(when(col("voted_up"), 1.0).otherwise(0.0)) * 100, 2).alias("positive_ratio_percent"),
        )
        .select(col("window.start").alias("window_start"), col("window.end").alias("window_end"),
                lit(label).alias("window_size"), lit("reviews").alias("metric_type"), "*")
        .drop("window")
    )


def sink(es: bool):
    def write(batch_df, batch_id):
        if es:
            write_es(batch_df, "realtime_game_metrics", ["metric_type", "appid", "window_size", "window_start"])
        else:
            batch_df.orderBy(col("review_count_window").desc()).show(20, truncate=False)
    return write


def run(args):
    """
    Job S1: số review mới và % tích cực theo appid, cửa sổ trượt 60 phút/bước 5 phút và cửa sổ 5 phút;
    watermark 10 phút. Ghi ES realtime_game_metrics (upsert theo cửa sổ) hoặc console.
    """
    spark = get_spark_session("S1_Realtime_Review_Metrics")
    raw = read_stream(spark, "steam.reviews.raw", args.source, args.brokers, args.raw, args.max_files, args.starting_offsets)
    events = (
        raw.select(from_json(col("value"), REVIEW_SCHEMA).alias("d")).select("d.*")
        .filter(col("appid").isNotNull() & col("timestamp_created").isNotNull())
        .withColumn("event_time", to_timestamp(col("timestamp_created")))
        .withWatermark("event_time", "10 minutes")
    )
    es = es_enabled(args.es_nodes)
    queries = []
    for duration, slide, label in (("60 minutes", "5 minutes", "60m"), ("5 minutes", "5 minutes", "5m")):
        queries.append(
            window_agg(events, duration, slide, label).writeStream
            .outputMode("update")
            .foreachBatch(sink(es))
            .option("checkpointLocation", f"{args.checkpoint}/{label}")
            .trigger(processingTime=args.trigger)
            .start()
        )
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    parser = add_stream_args(argparse.ArgumentParser(description="S1: Realtime Review Metrics"))
    parser.add_argument("--checkpoint", default=f"{CHECKPOINT_PATH}/s1_metrics")
    run(parser.parse_args())

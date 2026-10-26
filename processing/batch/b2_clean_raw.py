import argparse
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, FloatType, DoubleType,
    BooleanType, ArrayType, MapType
)
from pyspark.sql.functions import (
    col, from_json, row_number, udf, to_date, from_unixtime, hour, lit, coalesce,
    get_json_object, count, when, length, lower, sha2, concat_ws
)
from pyspark.sql.window import Window
from processing.common.spark_utils import (
    get_spark_session, read_raw_json_lines, write_table, RAW_PATH, CLEAN_PATH
)
from processing.common.text_preprocessor import clean_review_text, is_spam_or_empty

REVIEW_SCHEMA = StructType([
    StructField("recommendationid", StringType(), True),
    StructField("appid", IntegerType(), True),
    StructField("language", StringType(), True),
    StructField("review", StringType(), True),
    StructField("voted_up", BooleanType(), True),
    StructField("timestamp_created", LongType(), True),
    StructField("timestamp_updated", LongType(), True),
    StructField("votes_up", IntegerType(), True),
    StructField("votes_funny", IntegerType(), True),
    StructField("weighted_vote_score", DoubleType(), True),
    StructField("steam_purchase", BooleanType(), True),
    StructField("received_for_free", BooleanType(), True),
    StructField("written_during_early_access", BooleanType(), True),
    StructField("author_steamid", StringType(), True),
    StructField("num_games_owned", IntegerType(), True),
    StructField("num_reviews", IntegerType(), True),
    StructField("playtime_forever", DoubleType(), True),
    StructField("playtime_at_review", DoubleType(), True),
    StructField("playtime_last_two_weeks", DoubleType(), True),
    StructField("ingest_timestamp", LongType(), True),
    StructField("source", StringType(), True),
])

CCU_SCHEMA = StructType([
    StructField("appid", IntegerType(), True),
    StructField("timestamp", LongType(), True),
    StructField("player_count", IntegerType(), True),
])

APPDETAILS_SCHEMA = StructType([
    StructField("appid", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("type", StringType(), True),
    StructField("is_free", BooleanType(), True),
    StructField("developer", ArrayType(StringType()), True),
    StructField("publisher", ArrayType(StringType()), True),
    StructField("genres", ArrayType(StringType()), True),
    StructField("categories", ArrayType(StringType()), True),
    StructField("price", DoubleType(), True),
    StructField("initial_price", DoubleType(), True),
    StructField("discount_percent", IntegerType(), True),
    StructField("currency", StringType(), True),
    StructField("release_date", StringType(), True),
    StructField("platforms", MapType(StringType(), BooleanType()), True),
    StructField("metacritic_score", IntegerType(), True),
    StructField("timestamp", LongType(), True),
])

STEAMSPY_SCHEMA = StructType([
    StructField("appid", IntegerType(), True),
    StructField("owners_range", StringType(), True),
    StructField("average_forever", IntegerType(), True),
    StructField("median_forever", IntegerType(), True),
    StructField("positive_reviews_total", LongType(), True),
    StructField("negative_reviews_total", LongType(), True),
    StructField("tags", MapType(StringType(), LongType()), True),
    StructField("timestamp", LongType(), True),
])

# Review có giờ chơi lúc viết dưới ngưỡng này (phút) bị xem là đáng ngờ
SUSPICIOUS_PLAYTIME_MINUTES = 15
# Cùng một nội dung xuất hiện >= ngưỡng này lần trong toàn bộ dữ liệu = copy-paste
DUPLICATE_TEXT_THRESHOLD = 5


def latest_per_key(df, keys, order_cols):
    w = Window.partitionBy(*keys).orderBy(*[col(c).desc_nulls_last() for c in order_cols])
    return df.withColumn("_rn", row_number().over(w)).filter(col("_rn") == 1).drop("_rn")


def clean_reviews(spark, raw_root: str, clean_root: str):
    """
    B2a: raw review JSON -> clean Parquet phân vùng theo dt (ngày tạo review).
    Tính lại toàn bộ từ master dataset rồi ghi đè -> chạy lại không sinh trùng.
    """
    raw = read_raw_json_lines(spark, raw_root, "steam.reviews.raw")
    if raw is None:
        raise SystemExit(f"No raw reviews under {raw_root} (cần thư mục steam_reviews_raw/)")
    parsed = (
        raw.select(from_json(col("value"), REVIEW_SCHEMA).alias("d")).select("d.*")
        .filter(col("recommendationid").isNotNull() & col("appid").isNotNull() & col("timestamp_created").isNotNull())
        .withColumn("timestamp_updated", coalesce(col("timestamp_updated"), col("timestamp_created")))
        .withColumn("source", coalesce(col("source"), lit("steam_live")))
    )
    # Upsert: giữ bản sửa mới nhất, hoà thì giữ bản nhập sau
    dedup = latest_per_key(parsed, ["recommendationid"], ["timestamp_updated", "ingest_timestamp"])

    clean_udf = udf(clean_review_text, StringType())
    spam_udf = udf(is_spam_or_empty, BooleanType())
    processed = (
        dedup
        .withColumn("language", coalesce(lower(col("language")), lit("unknown")))
        .withColumn("cleaned_review", clean_udf(col("review")))
        .withColumn("review_length", length(col("cleaned_review")))
        .withColumn("is_english", col("language") == "english")
        # Review không phải tiếng Anh vẫn giữ để dùng nhãn voted_up; chỉ lọc spam/rỗng
        .filter(~spam_udf(col("cleaned_review")) | ~col("is_english"))
        .withColumn("text_hash", sha2(lower(col("cleaned_review")), 256))
    )
    dup_counts = processed.groupBy("text_hash").agg(count("*").alias("text_dup_count"))
    flagged = (
        processed.join(dup_counts, "text_hash", "left")
        .withColumn(
            "is_suspicious",
            (coalesce(col("playtime_at_review"), lit(0.0)) < SUSPICIOUS_PLAYTIME_MINUTES)
            | (coalesce(col("num_reviews"), lit(0)) == 1)
            | coalesce(col("received_for_free"), lit(False))
            | ((col("text_dup_count") >= DUPLICATE_TEXT_THRESHOLD) & (col("review_length") > 0))
        )
        .withColumn(
            "suspicious_reason",
            concat_ws(",",
                      when(coalesce(col("playtime_at_review"), lit(0.0)) < SUSPICIOUS_PLAYTIME_MINUTES, lit("low_playtime")),
                      when(coalesce(col("num_reviews"), lit(0)) == 1, lit("single_review_account")),
                      when(coalesce(col("received_for_free"), lit(False)), lit("free_copy")),
                      when((col("text_dup_count") >= DUPLICATE_TEXT_THRESHOLD) & (col("review_length") > 0), lit("duplicate_text")))
        )
        .drop("text_hash")
        .withColumn("dt", to_date(from_unixtime(col("timestamp_created"))))
    )
    out = f"{clean_root}/reviews"
    print(f"Writing clean reviews to {out}...")
    write_table(spark, flagged, out, "clean_reviews", partition_by=["dt"])


def clean_ccu(spark, raw_root: str, clean_root: str):
    """B2b: CCU snapshot -> Parquet (appid, ts, dt, hour, player_count), khử trùng theo (appid, timestamp)."""
    raw = read_raw_json_lines(spark, raw_root, "steam.players.ccu")
    if raw is None:
        print(f"No raw CCU under {raw_root}; skip.")
        return
    ccu = (
        raw.select(from_json(col("value"), CCU_SCHEMA).alias("d")).select("d.*")
        .filter(col("appid").isNotNull() & col("timestamp").isNotNull() & col("player_count").isNotNull())
        .dropDuplicates(["appid", "timestamp"])
        .withColumn("ts", from_unixtime(col("timestamp")).cast("timestamp"))
        .withColumn("dt", to_date(col("ts")))
        .withColumn("hour", hour(col("ts")))
    )
    out = f"{clean_root}/ccu"
    print(f"Writing clean CCU to {out}...")
    write_table(spark, ccu, out, "clean_ccu", partition_by=["dt"])


def clean_games(spark, raw_root: str, clean_root: str):
    """
    B2c: metadata. Topic meta chứa 2 loại bản ghi (appdetails và SteamSpy, source=steamspy).
    - games_history: mọi snapshot appdetails (giá/giảm giá theo thời gian, dùng cho B6)
    - games: snapshot mới nhất mỗi appid, gộp tags/owners từ SteamSpy
    """
    raw = read_raw_json_lines(spark, raw_root, "steam.games.meta")
    if raw is None:
        print(f"No raw metadata under {raw_root}; skip.")
        return
    tagged = raw.withColumn("_source", coalesce(get_json_object(col("value"), "$.source"), lit("appdetails")))

    details = (
        tagged.filter(col("_source") == "appdetails")
        .select(from_json(col("value"), APPDETAILS_SCHEMA).alias("d")).select("d.*")
        .filter(col("appid").isNotNull() & col("timestamp").isNotNull())
        .dropDuplicates(["appid", "timestamp"])
        .withColumn("snapshot_dt", to_date(from_unixtime(col("timestamp"))))
    )
    write_table(spark, details, f"{clean_root}/games_history", "clean_games_history")

    spy = (
        tagged.filter(col("_source") == "steamspy")
        .select(from_json(col("value"), STEAMSPY_SCHEMA).alias("d")).select("d.*")
        .filter(col("appid").isNotNull())
    )
    spy_latest = latest_per_key(spy, ["appid"], ["timestamp"]).drop("timestamp")
    latest = latest_per_key(details, ["appid"], ["timestamp"])

    games = (
        latest.join(spy_latest, "appid", "full_outer")
        .withColumn("price_bucket",
                    when(coalesce(col("is_free"), lit(False)) | (coalesce(col("price"), lit(0.0)) == 0), "free")
                    .when(col("price") < 10, "<10$")
                    .when(col("price") < 20, "10-20$")
                    .when(col("price") < 40, "20-40$")
                    .otherwise(">=40$"))
    )
    out = f"{clean_root}/games"
    print(f"Writing clean games to {out}...")
    write_table(spark, games, out, "clean_games")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="B2: Clean raw reviews, CCU and metadata into Parquet")
    parser.add_argument("--raw", default=RAW_PATH, help="Raw root (chứa steam_reviews_raw/, steam_players_ccu/, steam_games_meta/)")
    parser.add_argument("--clean", default=CLEAN_PATH, help="Clean root")
    parser.add_argument("--only", choices=["reviews", "ccu", "games"], nargs="+", default=["reviews", "ccu", "games"])
    args = parser.parse_args()

    spark = get_spark_session("B2_Clean_Raw")
    for part in args.only:
        {"reviews": clean_reviews, "ccu": clean_ccu, "games": clean_games}[part](spark, args.raw, args.clean)
    spark.stop()

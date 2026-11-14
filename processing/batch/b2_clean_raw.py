import argparse
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, FloatType, DoubleType,
    BooleanType, ArrayType, MapType
)
from pyspark.sql.functions import (
    col, from_json, row_number, udf, to_date, from_unixtime, hour, lit, coalesce,
    get_json_object, count, when, length, lower, sha2, concat_ws, explode, date_format
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
    StructField("comment_count", IntegerType(), True),
    StructField("steam_purchase", BooleanType(), True),
    StructField("received_for_free", BooleanType(), True),
    StructField("refunded", BooleanType(), True),
    StructField("written_during_early_access", BooleanType(), True),
    StructField("primarily_steam_deck", BooleanType(), True),
    StructField("timestamp_dev_responded", LongType(), True),
    StructField("author_steamid", StringType(), True),
    StructField("num_games_owned", IntegerType(), True),
    StructField("num_reviews", IntegerType(), True),
    StructField("playtime_forever", DoubleType(), True),
    StructField("playtime_at_review", DoubleType(), True),
    StructField("playtime_last_two_weeks", DoubleType(), True),
    StructField("deck_playtime_at_review", DoubleType(), True),
    StructField("last_played", LongType(), True),
    StructField("ingest_timestamp", LongType(), True),
    StructField("source", StringType(), True),
])

CCU_SCHEMA = StructType([
    StructField("appid", IntegerType(), True),
    StructField("timestamp", LongType(), True),
    StructField("player_count", IntegerType(), True),
    StructField("source", StringType(), True),
])

# Sự kiện bơm vào Kafka để demo cảnh báo (crawlers/demo_inject.py): chỉ cho speed layer, không vào batch
DEMO_SOURCE = "demo_injection"

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
    StructField("coming_soon", BooleanType(), True),
    StructField("platforms", MapType(StringType(), BooleanType()), True),
    StructField("metacritic_score", IntegerType(), True),
    StructField("required_age", IntegerType(), True),
    StructField("controller_support", StringType(), True),
    StructField("dlc_count", IntegerType(), True),
    StructField("achievements_total", IntegerType(), True),
    StructField("recommendations_total", LongType(), True),
    StructField("supported_languages_count", IntegerType(), True),
    StructField("content_descriptor_ids", ArrayType(IntegerType()), True),
    StructField("short_description", StringType(), True),
    StructField("timestamp", LongType(), True),
])

# Snapshot IStoreBrowseService/GetItems (source=steam_store_browse): tag chính thức của Steam, tổng review
STORE_SCHEMA = StructType([
    StructField("appid", IntegerType(), True),
    StructField("review_count", LongType(), True),
    StructField("percent_positive", IntegerType(), True),
    StructField("review_score", IntegerType(), True),
    StructField("review_score_label", StringType(), True),
    StructField("steam_release_ts", LongType(), True),
    StructField("is_early_access", BooleanType(), True),
    StructField("tags", ArrayType(StringType()), True),
    StructField("tag_weights", MapType(StringType(), LongType()), True),
    StructField("steam_deck_compat", IntegerType(), True),
    StructField("timestamp", LongType(), True),
])

# Histogram review theo ngày (source=steam_review_histogram)
_POINT = StructType([StructField("date", LongType(), True), StructField("up", LongType(), True),
                     StructField("down", LongType(), True)])
HISTOGRAM_SCHEMA = StructType([
    StructField("appid", IntegerType(), True),
    StructField("daily", ArrayType(_POINT), True),
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
    B2a: raw review JSON -> clean Parquet phân vùng theo ym (tháng tạo review), giữ nguyên cột dt.
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
        .filter(col("source") != DEMO_SOURCE)
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
        .withColumn("ym", date_format(col("dt"), "yyyy-MM"))
    )
    out = f"{clean_root}/reviews"
    print(f"Writing clean reviews to {out}...")
    write_table(spark, flagged.repartition("ym"), out, "clean_reviews", partition_by=["ym"])


def clean_ccu(spark, raw_root: str, clean_root: str):
    """B2b: CCU snapshot -> Parquet (appid, ts, dt, hour, player_count), khử trùng theo (appid, timestamp)."""
    raw = read_raw_json_lines(spark, raw_root, "steam.players.ccu")
    if raw is None:
        print(f"No raw CCU under {raw_root}; skip.")
        return
    ccu = (
        raw.select(from_json(col("value"), CCU_SCHEMA).alias("d")).select("d.*")
        .filter(col("appid").isNotNull() & col("timestamp").isNotNull() & col("player_count").isNotNull())
        .filter(coalesce(col("source"), lit("steam_live")) != DEMO_SOURCE)
        .drop("source")
        .dropDuplicates(["appid", "timestamp"])
        .withColumn("ts", from_unixtime(col("timestamp")).cast("timestamp"))
        .withColumn("dt", to_date(col("ts")))
        .withColumn("hour", hour(col("ts")))
    )
    out = f"{clean_root}/ccu"
    print(f"Writing clean CCU to {out}...")
    write_table(spark, ccu.repartition("dt"), out, "clean_ccu", partition_by=["dt"])


def clean_games(spark, raw_root: str, clean_root: str):
    """
    B2c: metadata. Topic meta chứa 3 loại bản ghi theo trường source:
    - steam_live (appdetails) -> games_history: mọi snapshot (giá/giảm giá theo thời gian, dùng cho B6)
    - steam_store_browse      -> tag + trọng số, tổng review, early access (snapshot mới nhất gộp vào games)
    - steam_review_histogram  -> review_daily_counts: số review up/down theo ngày, giữ snapshot mới nhất mỗi (appid, ngày)
    games = snapshot appdetails mới nhất mỗi appid + snapshot store mới nhất.
    """
    raw = read_raw_json_lines(spark, raw_root, "steam.games.meta")
    if raw is None:
        print(f"No raw metadata under {raw_root}; skip.")
        return
    tagged = raw.withColumn("_source", coalesce(get_json_object(col("value"), "$.source"), lit("steam_live")))

    details = (
        tagged.filter(col("_source") == "steam_live")
        .select(from_json(col("value"), APPDETAILS_SCHEMA).alias("d")).select("d.*")
        .filter(col("appid").isNotNull() & col("timestamp").isNotNull())
        .dropDuplicates(["appid", "timestamp"])
        .withColumn("snapshot_dt", to_date(from_unixtime(col("timestamp"))))
    )
    write_table(spark, details.coalesce(1), f"{clean_root}/games_history", "clean_games_history")

    store = (
        tagged.filter(col("_source") == "steam_store_browse")
        .select(from_json(col("value"), STORE_SCHEMA).alias("d")).select("d.*")
        .filter(col("appid").isNotNull() & col("timestamp").isNotNull())
    )
    store_latest = latest_per_key(store, ["appid"], ["timestamp"]).drop("timestamp")
    latest = latest_per_key(details, ["appid"], ["timestamp"])

    games = (
        latest.join(store_latest, "appid", "full_outer")
        .withColumn("price_bucket",
                    when(coalesce(col("is_free"), lit(False)) | (coalesce(col("price"), lit(0.0)) == 0), "free")
                    .when(col("price") < 10, "<10$")
                    .when(col("price") < 20, "10-20$")
                    .when(col("price") < 40, "20-40$")
                    .otherwise(">=40$"))
    )
    out = f"{clean_root}/games"
    print(f"Writing clean games to {out}...")
    write_table(spark, games.coalesce(1), out, "clean_games")

    hist = tagged.filter(col("_source") == "steam_review_histogram")
    if hist.limit(1).count():
        daily = (
            hist.select(from_json(col("value"), HISTOGRAM_SCHEMA).alias("d")).select("d.*")
            .filter(col("appid").isNotNull() & col("timestamp").isNotNull())
            .select("appid", "timestamp", explode("daily").alias("p"))
            .select("appid", col("timestamp").alias("snapshot_ts"), col("p.date").alias("day_ts"),
                    col("p.up").alias("reviews_up"), col("p.down").alias("reviews_down"))
        )
        # Ngày cuối trong snapshot cũ còn đang chạy; snapshot mới nhất của mỗi ngày là số đầy đủ nhất
        daily = (latest_per_key(daily, ["appid", "day_ts"], ["snapshot_ts"])
                 .withColumn("dt", to_date(from_unixtime(col("day_ts"))))
                 .withColumn("reviews_total", col("reviews_up") + col("reviews_down"))
                 # Ngày chứa thời điểm lấy snapshot mới đếm được một phần
                 .withColumn("is_partial", col("dt") >= to_date(from_unixtime(col("snapshot_ts"))))
                 .drop("day_ts"))
        write_table(spark, daily.coalesce(1), f"{clean_root}/review_daily_counts", "clean_review_daily_counts")


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

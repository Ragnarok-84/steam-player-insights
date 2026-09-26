import os
import argparse
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, FloatType, BooleanType
)
from pyspark.sql.functions import (
    col, from_json, row_number, udf, to_date, from_unixtime
)
from pyspark.sql.window import Window
from processing.common.spark_utils import get_spark_session
from processing.common.text_preprocessor import clean_review_text, is_spam_or_empty

# Schema cho raw review JSON
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
    StructField("weighted_vote_score", FloatType(), True),
    StructField("steam_purchase", BooleanType(), True),
    StructField("received_for_free", BooleanType(), True),
    StructField("written_during_early_access", BooleanType(), True),
    StructField("author_steamid", StringType(), True),
    StructField("num_games_owned", IntegerType(), True),
    StructField("num_reviews", IntegerType(), True),
    StructField("playtime_forever", FloatType(), True),
    StructField("playtime_at_review", FloatType(), True),
    StructField("playtime_last_two_weeks", FloatType(), True),
    StructField("ingest_timestamp", LongType(), True),
])

def clean_raw_reviews(spark, raw_path: str, clean_path: str):
    """
    Job B2: Làm sạch dữ liệu đánh giá
    1. Parse JSON từ raw
    2. Khử trùng lặp theo recommendationid (giữ bản ghi có timestamp_updated lớn nhất)
    3. Lọc bỏ review rỗng hoặc spam
    4. Chuẩn hóa văn bản review
    5. Ghi ra Parquet snappy phân vùng theo ngày (dt)
    """
    print(f"Reading raw data from {raw_path}...")
    raw_df = spark.read.text(raw_path)

    parsed_df = (
        raw_df.select(from_json(col("value"), REVIEW_SCHEMA).alias("data"))
        .select("data.*")
        .filter(col("recommendationid").isNotNull() & col("appid").isNotNull())
    )

    # 1. Khử trùng lặp theo recommendationid, giữ bản ghi có timestamp_updated lớn nhất
    window_spec = Window.partitionBy("recommendationid").orderBy(col("timestamp_updated").desc())
    dedup_df = (
        parsed_df
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

    # UDFs xử lý văn bản
    clean_text_udf = udf(clean_review_text, StringType())
    is_spam_udf = udf(is_spam_or_empty, BooleanType())

    # 2. Làm sạch văn bản và lọc spam
    processed_df = (
        dedup_df
        .withColumn("cleaned_review", clean_text_udf(col("review")))
        .withColumn("is_spam", is_spam_udf(col("cleaned_review")))
        .filter(col("is_spam") == False)
        .drop("is_spam")
    )

    # 3. Tạo cột ngày tạo review dt=YYYY-MM-DD
    final_df = processed_df.withColumn(
        "dt", to_date(from_unixtime(col("timestamp_created")))
    )

    print(f"Writing clean Parquet data to {clean_path}...")
    (
        final_df.write
        .mode("append")
        .partitionBy("dt")
        .option("compression", "snappy")
        .parquet(clean_path)
    )
    print("Clean job B2 finished successfully.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="B2: Clean Raw Reviews into Parquet")
    parser.add_argument("--raw", default="/steam/raw/steam_reviews_raw", help="Path to raw reviews on HDFS")
    parser.add_argument("--clean", default="/steam/clean/reviews", help="Target path for clean Parquet on HDFS")
    args = parser.parse_args()

    spark = get_spark_session("B2_Clean_Raw_Reviews")
    clean_raw_reviews(spark, args.raw, args.clean)
    spark.stop()

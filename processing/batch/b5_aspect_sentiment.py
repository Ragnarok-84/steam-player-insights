import os
import argparse
from pyspark.sql.functions import col, udf, explode, count, when, avg, round as spark_round, date_format
from pyspark.sql.types import ArrayType, StringType, FloatType
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from processing.common.spark_utils import get_spark_session
from processing.common.aspect_dictionary import extract_aspects

vader_analyzer = SentimentIntensityAnalyzer()

def get_vader_compound(text: str) -> float:
    if not text:
        return 0.0
    scores = vader_analyzer.polarity_scores(text)
    return float(scores["compound"])

def analyze_aspects_and_sentiment(spark, clean_reviews_path: str, curated_path: str, es_nodes: str = None):
    """
    Job B5: Phân tích khía cạnh phàn nàn và cảm xúc người chơi:
    - Gán khía cạnh qua từ điển
    - Tính điểm cảm xúc (VADER compound score)
    - Thống kê tỷ lệ tiêu cực theo từng khía cạnh theo tháng
    - Ghi vào Elasticsearch reviews_search và batch_aspect_stats
    """
    print(f"Reading clean reviews from {clean_reviews_path}...")
    df = spark.read.parquet(clean_reviews_path)

    # Đăng ký UDFs
    extract_aspects_udf = udf(extract_aspects, ArrayType(StringType()))
    vader_udf = udf(get_vader_compound, FloatType())

    # Chỉ phân tích sâu văn bản cho tiếng Anh theo quy định trong đề tài
    en_df = df.filter(col("language") == "english")

    enriched_df = (
        en_df
        .withColumn("aspects", extract_aspects_udf(col("cleaned_review")))
        .withColumn("sentiment_score", vader_udf(col("cleaned_review")))
        .withColumn("month", date_format(col("dt"), "yyyy-MM"))
    )

    # 1. Thống kê theo khía cạnh phàn nàn
    exploded_df = enriched_df.withColumn("aspect", explode(col("aspects")))

    aspect_stats = (
        exploded_df.groupBy("appid", "month", "aspect")
        .agg(
            count("recommendationid").alias("mention_count"),
            count(when(col("voted_up") == False, 1)).alias("negative_count"),
            count(when(col("voted_up") == True, 1)).alias("positive_count"),
            spark_round(avg("sentiment_score"), 3).alias("avg_sentiment"),
            spark_round(
                count(when(col("voted_up") == False, 1)) * 100.0 / count("recommendationid"), 2
            ).alias("negative_ratio_percent")
        )
    )

    out_aspects = os.path.join(curated_path, "aspect_stats")
    print(f"Writing aspect stats to {out_aspects}...")
    aspect_stats.write.mode("overwrite").parquet(out_aspects)

    # Ghi vào Elasticsearch nếu có cấu hình
    if es_nodes:
        print("Writing to Elasticsearch indices: batch_aspect_stats and reviews_search...")
        try:
            (
                aspect_stats.write
                .format("org.elasticsearch.spark.sql")
                .option("es.nodes", es_nodes)
                .option("es.resource", "batch_aspect_stats")
                .mode("append")
                .save()
            )

            # Ghi các review có gắn nhãn vào reviews_search để tìm kiếm toàn văn
            (
                enriched_df.select(
                    "recommendationid", "appid", "cleaned_review", "voted_up",
                    "sentiment_score", "aspects", "dt"
                ).write
                .format("org.elasticsearch.spark.sql")
                .option("es.nodes", es_nodes)
                .option("es.resource", "reviews_search")
                .option("es.mapping.id", "recommendationid")
                .mode("append")
                .save()
            )
        except Exception as e:
            print(f"Failed to write to ES: {e}")

    print("Job B5 completed successfully.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="B5: Aspect & Sentiment Analysis")
    parser.add_argument("--clean-reviews", default="/steam/clean/reviews", help="Clean reviews path")
    parser.add_argument("--curated", default="/steam/curated", help="Curated storage path")
    parser.add_argument("--es-nodes", default=os.getenv("ELASTICSEARCH_HOSTS"), help="ES host:port")
    args = parser.parse_args()

    spark = get_spark_session("B5_Aspect_Sentiment_Analysis")
    analyze_aspects_and_sentiment(spark, args.clean_reviews, args.curated, args.es_nodes)
    spark.stop()

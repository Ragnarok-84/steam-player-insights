import argparse
from pyspark.sql.functions import (
    col, udf, explode, count, when, avg, round as spark_round, date_format, posexplode, collect_list
)
from pyspark.sql.types import ArrayType, StringType, FloatType, StructType, StructField, DoubleType
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer, IDF
from processing.common.spark_utils import get_spark_session, write_table, CLEAN_PATH, CURATED_PATH
from processing.common.aspect_dictionary import extract_aspects
from processing.common.es_writer import es_enabled, write_es

_vader = None


def get_vader_compound(text: str) -> float:
    global _vader
    if not text:
        return 0.0
    if _vader is None:
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
        _vader = SentimentIntensityAnalyzer()
    return float(_vader.polarity_scores(text)["compound"])


def sentiment_label(score):
    if score is None:
        return None
    return "positive" if score >= 0.05 else "negative" if score <= -0.05 else "neutral"


def top_keywords(spark, negative_df, top_k: int, vocab_size: int):
    """TF-IDF trên review tiêu cực, gộp theo game (mỗi game = 1 văn bản) -> top_k từ khóa/game."""
    tokens = RegexTokenizer(inputCol="cleaned_review", outputCol="words", pattern="[^a-zA-Z']+", minTokenLength=3) \
        .transform(negative_df)
    tokens = StopWordsRemover(inputCol="words", outputCol="terms").transform(tokens)
    per_game = tokens.select("appid", explode("terms").alias("t")).groupBy("appid").agg(collect_list("t").alias("terms"))
    if per_game.isEmpty():
        return None
    cv_model = CountVectorizer(inputCol="terms", outputCol="tf", vocabSize=vocab_size, minDF=2).fit(per_game)
    tfidf = IDF(inputCol="tf", outputCol="tfidf").fit(cv_model.transform(per_game)).transform(cv_model.transform(per_game))
    vocab = cv_model.vocabulary

    schema = ArrayType(StructType([StructField("keyword", StringType()), StructField("score", DoubleType())]))

    @udf(schema)
    def top_terms(vec):
        pairs = sorted(zip(vec.indices.tolist(), vec.values.tolist()), key=lambda p: -p[1])[:top_k]
        return [(vocab[i], float(round(v, 4))) for i, v in pairs]

    return (
        tfidf.select("appid", posexplode(top_terms("tfidf")).alias("rank", "kw"))
        .select("appid", (col("rank") + 1).alias("rank"), col("kw.keyword").alias("keyword"), col("kw.score").alias("score"))
    )


def analyze_aspects_and_sentiment(spark, clean_root: str, curated_root: str, es: bool, top_k: int = 30):
    """
    Job B5:
    - Khía cạnh qua từ điển, điểm cảm xúc VADER (chỉ review tiếng Anh)
    - aspect_stats theo (appid, tháng, khía cạnh)
    - review_sentiment (cảm xúc từng review, B3 dùng tính cảm xúc trung bình)
    - keywords: TF-IDF top từ khóa của review tiêu cực theo game (word cloud D2)
    - ES: batch_aspect_stats, reviews_search, game_keywords
    """
    df = spark.read.parquet(f"{clean_root}/reviews")
    en_df = df.filter(col("is_english"))

    vader_udf = udf(get_vader_compound, FloatType())
    label_udf = udf(sentiment_label, StringType())
    enriched = (
        en_df
        .withColumn("aspects", udf(extract_aspects, ArrayType(StringType()))(col("cleaned_review")))
        .withColumn("sentiment_score", vader_udf(col("cleaned_review")))
        .withColumn("sentiment_label", label_udf(col("sentiment_score")))
        .withColumn("month", date_format(col("dt"), "yyyy-MM"))
        .cache()
    )

    sentiment = enriched.select("recommendationid", "appid", "dt", "sentiment_score", "sentiment_label", "aspects")
    write_table(spark, sentiment, f"{curated_root}/review_sentiment", "review_sentiment", partition_by=["dt"])

    aspect_stats = (
        enriched.withColumn("aspect", explode(col("aspects")))
        .groupBy("appid", "month", "aspect")
        .agg(
            count("*").alias("mention_count"),
            count(when(~col("voted_up"), 1)).alias("negative_count"),
            count(when(col("voted_up"), 1)).alias("positive_count"),
            spark_round(avg("sentiment_score"), 3).alias("avg_sentiment"),
            spark_round(count(when(~col("voted_up"), 1)) * 100.0 / count("*"), 2).alias("negative_ratio_percent"),
        )
    )
    write_table(spark, aspect_stats, f"{curated_root}/aspect_stats", "aspect_stats")

    keywords = top_keywords(spark, enriched.filter(~col("voted_up")), top_k, vocab_size=20000)
    if keywords is not None:
        write_table(spark, keywords, f"{curated_root}/keywords", "keywords")

    if es:
        write_es(spark.read.parquet(f"{curated_root}/aspect_stats"), "batch_aspect_stats", ["appid", "month", "aspect"])
        write_es(
            enriched.select("recommendationid", "appid", "language", "cleaned_review", "voted_up",
                            "sentiment_score", "sentiment_label", "aspects", "playtime_at_review",
                            "votes_up", "is_suspicious", "dt"),
            "reviews_search", ["recommendationid"])
        if keywords is not None:
            write_es(spark.read.parquet(f"{curated_root}/keywords"), "game_keywords", ["appid", "rank"])
    enriched.unpersist()
    print("Job B5 completed successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="B5: Aspect, Sentiment & TF-IDF keywords")
    parser.add_argument("--clean", default=CLEAN_PATH)
    parser.add_argument("--curated", default=CURATED_PATH)
    parser.add_argument("--es-nodes", default=None, help="host:port; hoặc đặt WRITE_ES=1")
    parser.add_argument("--top-k", type=int, default=30)
    args = parser.parse_args()

    spark = get_spark_session("B5_Aspect_Sentiment")
    analyze_aspects_and_sentiment(spark, args.clean, args.curated, es_enabled(args.es_nodes), args.top_k)
    spark.stop()

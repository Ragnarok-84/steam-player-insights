import argparse
from pyspark.sql.functions import col, coalesce, lit, count, sum as spark_sum, first
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from processing.common.spark_utils import get_spark_session, path_exists, write_table, CLEAN_PATH, CURATED_PATH
from processing.common.es_writer import es_enabled, write_es

ASPECTS = ["performance", "bug", "price", "server", "gameplay", "story", "graphics", "cheating"]


def cluster_games(spark, clean_root: str, curated_root: str, k: int, es: bool):
    """
    Job B8 (mô hình nhẹ): K-Means trên hồ sơ trải nghiệm mỗi game:
    % tích cực, early_negative_ratio, giờ chơi trung vị, giá, tỷ lệ nhắc từng khía cạnh.
    """
    features = spark.read.parquet(f"{curated_root}/game_retention").select(
        "appid", "name", "positive_ratio_percent", "early_negative_ratio", "median_playtime_hours", "price", "total_reviews")

    aspect_path = f"{curated_root}/aspect_stats"
    if path_exists(spark, aspect_path):
        a = spark.read.parquet(aspect_path).groupBy("appid", "aspect").agg(spark_sum("mention_count").alias("m"))
        pivot = a.groupBy("appid").pivot("aspect", ASPECTS).agg(first("m"))
        # đổi tên tránh trùng cột (vd. khía cạnh "price" với giá game)
        pivot = pivot.select("appid", *[col(asp).alias(f"asp_{asp}") for asp in ASPECTS])
        features = features.join(pivot, "appid", "left")
        for asp in ASPECTS:
            features = features.withColumn(f"{asp}_share", coalesce(col(f"asp_{asp}"), lit(0)) / col("total_reviews"))                 .drop(f"asp_{asp}")
    cols = ["positive_ratio_percent", "early_negative_ratio", "median_playtime_hours", "price"] + \
        [c for c in features.columns if c.endswith("_share")]
    features = features.fillna(0.0, subset=cols).filter(col("total_reviews") >= 20)

    n = features.count()
    if n < k:
        print(f"Only {n} games with >= 20 reviews; need at least k={k}. Skip clustering.")
        return
    vec = VectorAssembler(inputCols=cols, outputCol="raw_features").transform(features)
    scaled = StandardScaler(inputCol="raw_features", outputCol="features", withMean=True).fit(vec).transform(vec)
    model = KMeans(k=k, seed=42, featuresCol="features", predictionCol="cluster").fit(scaled)
    result = model.transform(scaled).drop("raw_features", "features")
    write_table(spark, result, f"{curated_root}/game_clusters", "game_clusters")

    profile = result.groupBy("cluster").agg(count("*").alias("games"),
                                            *[spark_sum(c).alias(c) for c in cols])
    for c in cols:
        profile = profile.withColumn(c, col(c) / col("games"))
    profile.orderBy("cluster").show(truncate=False)
    write_table(spark, profile, f"{curated_root}/cluster_profiles", "cluster_profiles")
    if es:
        write_es(spark.read.parquet(f"{curated_root}/game_clusters"), "batch_game_clusters", ["appid"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="B8: K-Means clustering of game experience profiles")
    parser.add_argument("--clean", default=CLEAN_PATH)
    parser.add_argument("--curated", default=CURATED_PATH)
    parser.add_argument("--k", type=int, default=6)
    parser.add_argument("--es-nodes", default=None)
    args = parser.parse_args()

    spark = get_spark_session("B8_Game_Clusters")
    cluster_games(spark, args.clean, args.curated, args.k, es_enabled(args.es_nodes))
    spark.stop()

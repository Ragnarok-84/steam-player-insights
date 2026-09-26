import os
import argparse
from pyspark.sql.functions import col, lag, when, round as spark_round
from pyspark.sql.window import Window
from processing.common.spark_utils import get_spark_session

def compute_trending_games(spark, curated_path: str):
    """
    Job B6: Tính toán xu hướng game (Trending Games):
    - Tốc độ tăng trưởng lượng review theo tuần
    - Tốc độ tăng trưởng CCU
    - Điểm số trending tổng hợp
    """
    daily_stats_path = os.path.join(curated_path, "game_daily_stats")
    print(f"Reading daily stats from {daily_stats_path}...")
    df = spark.read.parquet(daily_stats_path)

    # Window theo appid sắp xếp theo ngày dt
    window_spec = Window.partitionBy("appid").orderBy("dt")

    trend_df = (
        df
        .withColumn("prev_total_reviews", lag("total_reviews", 7).over(window_spec))
        .withColumn("prev_avg_ccu", lag("avg_ccu", 7).over(window_spec))
        .withColumn(
            "review_growth_rate",
            when(col("prev_total_reviews") > 0,
                 spark_round((col("total_reviews") - col("prev_total_reviews")) * 100.0 / col("prev_total_reviews"), 2))
            .otherwise(0.0)
        )
        .withColumn(
            "ccu_growth_rate",
            when(col("prev_avg_ccu") > 0,
                 spark_round((col("avg_ccu") - col("prev_avg_ccu")) * 100.0 / col("prev_avg_ccu"), 2))
            .otherwise(0.0)
        )
    )

    out_trending = os.path.join(curated_path, "trending")
    print(f"Writing trending games data to {out_trending}...")
    trend_df.write.mode("overwrite").parquet(out_trending)
    print("Job B6 completed successfully.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="B6: Trending Games Analysis")
    parser.add_argument("--curated", default="/steam/curated", help="Curated storage path")
    args = parser.parse_args()

    spark = get_spark_session("B6_Trending_Games")
    compute_trending_games(spark, args.curated)
    spark.stop()

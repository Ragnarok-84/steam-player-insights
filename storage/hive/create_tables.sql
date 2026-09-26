-- DDL Khởi tạo cơ sở dữ liệu và bảng trên Hive Metastore (Spark SQL)
-- Cho phép truy vấn SQL ad-hoc trên các bảng Parquet ở HDFS

CREATE DATABASE IF NOT EXISTS steam_analytics;
USE steam_analytics;

-- 1. Bảng Review đã làm sạch (Clean Layer)
CREATE EXTERNAL TABLE IF NOT EXISTS clean_reviews (
    recommendationid STRING,
    appid INT,
    language STRING,
    review STRING,
    cleaned_review STRING,
    voted_up BOOLEAN,
    timestamp_created BIGINT,
    timestamp_updated BIGINT,
    votes_up INT,
    votes_funny INT,
    weighted_vote_score FLOAT,
    steam_purchase BOOLEAN,
    received_for_free BOOLEAN,
    written_during_early_access BOOLEAN,
    author_steamid STRING,
    num_games_owned INT,
    num_reviews INT,
    playtime_forever FLOAT,
    playtime_at_review FLOAT,
    playtime_last_two_weeks FLOAT
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '/steam/clean/reviews';

-- 2. Bảng Thống kê chỉ số ngày theo Game (Curated Layer)
CREATE EXTERNAL TABLE IF NOT EXISTS game_daily_stats (
    appid INT,
    dt STRING,
    total_reviews BIGINT,
    positive_reviews BIGINT,
    negative_reviews BIGINT,
    positive_ratio_percent DOUBLE,
    avg_votes_up DOUBLE,
    avg_playtime_hours_at_review DOUBLE,
    avg_ccu DOUBLE,
    peak_ccu INT
)
STORED AS PARQUET
LOCATION '/steam/curated/game_daily_stats';

-- 3. Bảng Phân tích khía cạnh người chơi phàn nàn (Curated Layer)
CREATE EXTERNAL TABLE IF NOT EXISTS aspect_stats (
    appid INT,
    month STRING,
    aspect STRING,
    mention_count BIGINT,
    negative_count BIGINT,
    positive_count BIGINT,
    avg_sentiment DOUBLE,
    negative_ratio_percent DOUBLE
)
STORED AS PARQUET
LOCATION '/steam/curated/aspect_stats';

-- 4. Bảng Phân tích tương quan giờ chơi và tỷ lệ bỏ game (Curated Layer)
CREATE EXTERNAL TABLE IF NOT EXISTS playtime_buckets (
    appid INT,
    playtime_bucket STRING,
    total_reviews BIGINT,
    positive_reviews BIGINT,
    negative_reviews BIGINT,
    positive_ratio_percent DOUBLE,
    avg_playtime_in_bucket DOUBLE
)
STORED AS PARQUET
LOCATION '/steam/curated/playtime_buckets';

-- 5. Bảng Xếp hạng game xu hướng (Curated Layer)
CREATE EXTERNAL TABLE IF NOT EXISTS trending_games (
    appid INT,
    dt STRING,
    total_reviews BIGINT,
    avg_ccu DOUBLE,
    prev_total_reviews BIGINT,
    prev_avg_ccu DOUBLE,
    review_growth_rate DOUBLE,
    ccu_growth_rate DOUBLE
)
STORED AS PARQUET
LOCATION '/steam/curated/trending';

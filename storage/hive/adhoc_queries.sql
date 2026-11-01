-- Bảng Hive (database steam_analytics) được các job batch tự đăng ký khi đặt HIVE_METASTORE_URI
-- (processing/common/spark_utils.py: write_table -> external Parquet + MSCK REPAIR cho bảng phân vùng).
-- Chạy ad-hoc: spark-sql --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore.hive.svc.cluster.local:9083 -f storage/hive/adhoc_queries.sql
-- Bảng: clean_reviews(dt), clean_ccu(dt), clean_games, clean_games_history, review_sentiment(dt), aspect_stats, keywords,
--       game_daily_stats, game_period_stats, segment_stats, language_stats, playtime_buckets, game_retention,
--       trending, trending_top, event_impact, review_baseline, ccu_baseline, game_clusters, cluster_profiles

-- 1. Thể loại nào được đánh giá tốt nhất theo tháng?
SELECT segment AS genre, month, total_reviews, positive_ratio_percent
FROM steam_analytics.segment_stats WHERE dimension = 'genre' ORDER BY month DESC, positive_ratio_percent DESC;

-- 2. Khía cạnh bị phàn nàn nhiều nhất trên toàn bộ game
SELECT aspect, SUM(negative_count) AS negative_mentions, ROUND(AVG(avg_sentiment), 3) AS sentiment
FROM steam_analytics.aspect_stats GROUP BY aspect ORDER BY negative_mentions DESC;

-- 3. Game "giữ chân" tốt: ít review tiêu cực sớm, giờ chơi trung vị cao
SELECT appid, name, early_negative_ratio, median_playtime_forever_hours, value_score
FROM steam_analytics.game_retention WHERE total_reviews >= 100 ORDER BY early_negative_ratio ASC, median_playtime_forever_hours DESC LIMIT 20;

-- 4. Top trending ngày mới nhất
SELECT trending_rank, appid, name, reviews_7d, review_growth_rate, ccu_growth_rate, trending_score
FROM steam_analytics.trending_top ORDER BY trending_rank;

-- 5. Review đáng ngờ theo game
SELECT appid, COUNT(*) AS reviews, SUM(CASE WHEN is_suspicious THEN 1 ELSE 0 END) AS suspicious
FROM steam_analytics.clean_reviews GROUP BY appid ORDER BY suspicious DESC LIMIT 20;

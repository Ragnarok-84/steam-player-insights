#!/bin/bash
# Script khởi tạo cấu trúc thư mục trên HDFS theo kiến trúc Lambda của đề tài

echo "=== Initializing HDFS Directories for Steam Player Insights ==="

# 1. Thư mục Raw Layer (bất biến, JSON gzip)
hdfs dfs -mkdir -p /steam/raw/reviews
hdfs dfs -mkdir -p /steam/raw/ccu
hdfs dfs -mkdir -p /steam/raw/meta

# 2. Thư mục Clean Layer (Parquet Snappy)
hdfs dfs -mkdir -p /steam/clean/reviews
hdfs dfs -mkdir -p /steam/clean/ccu
hdfs dfs -mkdir -p /steam/clean/games

# 3. Thư mục Curated Layer (Phục vụ Hive Metastore & Batch Views)
hdfs dfs -mkdir -p /steam/curated/game_daily_stats
hdfs dfs -mkdir -p /steam/curated/aspect_stats
hdfs dfs -mkdir -p /steam/curated/playtime_buckets
hdfs dfs -mkdir -p /steam/curated/trending

# 4. Thư mục Checkpoint cho Spark Streaming
hdfs dfs -mkdir -p /steam/checkpoints/s1_metrics
hdfs dfs -mkdir -p /steam/checkpoints/s2_bombing
hdfs dfs -mkdir -p /steam/checkpoints/s3_ccu

# 5. Thiết lập hệ số nhân bản (Replication Factor = 3) theo yêu cầu đề tài
echo "Setting replication factor to 3 on HDFS..."
hdfs dfs -setrep -R 3 /steam/raw
hdfs dfs -setrep -R 3 /steam/clean
hdfs dfs -setrep -R 3 /steam/curated

# Phân quyền thư mục
hdfs dfs -chmod -R 775 /steam

echo "=== HDFS Directories Initialized Successfully ==="
hdfs dfs -ls -R /steam

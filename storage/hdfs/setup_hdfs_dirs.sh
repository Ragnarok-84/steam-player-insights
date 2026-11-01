#!/bin/bash
# Khởi tạo thư mục HDFS theo kiến trúc Lambda. Chạy trong pod NameNode:
#   kubectl exec -i -n hdfs hdfs-namenode-0 -- bash < storage/hdfs/setup_hdfs_dirs.sh
set -e
echo "=== Initializing HDFS directories ==="
# Raw (bất biến, JSON Lines gzip) — tên thư mục trùng output B1 và crawlers.collect_local
hdfs dfs -mkdir -p /steam/raw/steam_reviews_raw /steam/raw/steam_players_ccu /steam/raw/steam_games_meta
hdfs dfs -mkdir -p /steam/clean /steam/curated /steam/checkpoints /steam/warehouse
hdfs dfs -setrep -R 3 /steam
hdfs dfs -chmod -R 777 /steam
hdfs dfs -ls /steam

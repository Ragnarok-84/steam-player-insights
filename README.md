# Steam Player Insights - Big Data Lambda Architecture

Hệ thống phân tích trải nghiệm người chơi đối với game trên Steam theo **Kiến trúc Lambda (Lambda Architecture)** chạy trên nền tảng **Kubernetes**.

---

## 1. Tổng quan kiến trúc hệ thống

Dự án được thiết kế đáp ứng các yêu cầu của Bài tập lớn môn *Lưu trữ và Xử lý Dữ liệu lớn* (Hướng 1: Xây dựng và cài đặt kiến trúc dữ liệu lớn):

```
+---------------------------------------------------------------------------------------+
|                                    DATA SOURCES                                       |
|  Steam Store Reviews API | Steam Web API (CCU/AppList) | SteamSpy API | Kaggle Dataset |
+-------------------------------------------+-------------------------------------------+
                                            |
                                            v
+---------------------------------------------------------------------------------------+
|                               INGESTION & BUFFER LAYER                                |
|   Python Crawlers (K8s CronJobs/Deployments)  &  Kaggle Replay Producer (Test/Backfill)  |
|                                           |                                           |
|                            Apache Kafka (Strimzi Operator)                            |
|     Topics: steam.reviews.raw | steam.players.ccu | steam.games.meta | steam.alerts   |
+------------------------------------+-------------------------------------+------------+
                                     |                                     |
                [Batch Pipeline]     |                                     | [Speed Pipeline]
                                     v                                     v
+-----------------------------------------------+ +-------------------------------------+
|                  BATCH LAYER                  | |             SPEED LAYER             |
|                                               | |                                     |
|  1. Ingestion: Kafka -> HDFS Raw (/steam/raw) | |  Spark Structured Streaming         |
|  2. Clean: Raw JSON -> Parquet (/steam/clean) | |  - Window 5m/60m, Watermark 10m     |
|  3. Batch Analytics (Spark Batch):           | |  - Realtime metrics: CCU, Reviews   |
|     - B3: Daily stats (Rating %, CCU peak)    | |  - Realtime Review Bombing Detector |
|     - B4: Playtime buckets analysis           | |  - CCU Spikes / Drop Detector       |
|     - B5: Aspect & Sentiment (VADER/Keywords) | |                                     |
|     - B6: Trending games ranking              | |                                     |
|                                               | |                                     |
|  Output: HDFS Curated (Hive Metastore)        | |  Output: Elasticsearch              |
|          & Elasticsearch (Batch Views)        | |          (Realtime Views & Alerts)  |
+-----------------------------------------------+ +-------------------------------------+
                                     \                                     /
                                      \                                   /
                                       v                                 v
+---------------------------------------------------------------------------------------+
|                               SERVING & PRESENTATION LAYER                            |
|                                                                                       |
|  - Elasticsearch (ECK): batch_game_daily, batch_aspect_stats, realtime_game_metrics,  |
|                         reviews_search, alerts                                        |
|  - Spark SQL + Hive Metastore (PostgreSQL): Ad-hoc Analytics trên HDFS Parquet        |
|  - Dashboards: Kibana (D1-D4: Overview, Game Detail, Realtime, Alerts)               |
|  - Monitoring: Prometheus + Grafana (D5: System Resources, Kafka Lag, HDFS health)    |
|  - Exploration: Jupyter Notebooks                                                     |
+---------------------------------------------------------------------------------------+
```

---

## 2. Cấu trúc thư mục dự án

```
steam-player-insights/
├── crawlers/                     # Thu thập dữ liệu từ Steam, SteamSpy & Kaggle Replay
│   ├── common/                   # Base client, rate limiter, cursor manager, kafka producer
│   ├── steam_reviews_crawler.py  # Thu thập review theo cursor
│   ├── steam_app_details_crawler.py # Thu thập metadata game
│   ├── steam_ccu_crawler.py      # Thu thập số người chơi đồng thời (CCU)
│   ├── steamspy_crawler.py       # Thu thập ước lượng người sở hữu, tags
│   ├── kaggle_replay_producer.py # Replay dữ liệu lịch sử vào Kafka để backfill/load test
│   └── config.py                 # Cấu hình crawler & Steam API Key
│
├── processing/                   # Mã nguồn xử lý dữ liệu Spark (Batch & Streaming)
│   ├── common/                   # Hàm tiện ích chung: SparkSession, từ điển khía cạnh, text utils
│   │   ├── spark_utils.py
│   │   ├── aspect_dictionary.py  # Từ điển khía cạnh: bug, performance, story, server...
│   │   └── text_preprocessor.py
│   ├── batch/                    # Batch layer (Spark Jobs B1 -> B6)
│   │   ├── b1_ingest_raw.py      # Đọc Kafka -> HDFS raw JSON gzip
│   │   ├── b2_clean_raw.py       # Raw JSON -> Parquet Clean (khử trùng, lọc spam)
│   │   ├── b3_game_daily_stats.py # Thống kê ngày: % positive, CCU peak/avg
│   │   ├── b4_playtime_buckets.py # Phân tích giờ chơi vs đánh giá (<2h, 2-10h, ...)
│   │   ├── b5_aspect_sentiment.py # Phân tích cảm xúc & khía cạnh phàn nàn
│   │   └── b6_trending_games.py   # Xếp hạng game xu hướng & tác động update/sale
│   └── streaming/                # Speed layer (Spark Structured Streaming S1 -> S3)
│       ├── s1_review_metrics.py  # Cửa sổ trượt 60p tính số review & % positive
│       ├── s2_review_bombing_detector.py # Phát hiện đột biến review tiêu cực (+3σ)
│       └── s3_ccu_spike_detector.py      # Phát hiện đột biến tăng/giảm CCU (>50%)
│
├── storage/                      # Thiết kế lược đồ lưu trữ & script khởi tạo
│   ├── hdfs/                     # Cấu trúc thư mục HDFS & schema partitioning
│   │   └── setup_hdfs_dirs.sh
│   ├── hive/                     # DDL tạo bảng Hive Metastore cho lớp Curated
│   │   └── create_tables.sql
│   └── elasticsearch/            # Mappings & templates cho 5 Elasticsearch indices
│       ├── indices_mapping.json
│       └── setup_indices.py
│
├── k8s/                          # Kubernetes Manifests, Helm Values & Operators
│   ├── kafka/                    # Cấu hình Kafka Strimzi (Cluster 3 brokers + Topics)
│   ├── hdfs/                     # Helm values triển khai HDFS (1 NameNode + 3 DataNodes)
│   ├── elasticsearch/            # ECK Operator cấu hình ES 3 nodes + Kibana
│   ├── hive/                     # Hive Metastore + PostgreSQL backend
│   ├── spark/                    # RBAC và SparkApplication CRDs
│   ├── crawlers/                 # K8s CronJob & Deployment cho các crawler workers
│   └── monitoring/               # kube-prometheus-stack values & Grafana config
│
├── dashboards/                   # Cấu hình trực quan hóa
│   ├── kibana/                   # Kibana Saved Objects (D1, D2, D3, D4)
│   ├── grafana/                  # Grafana Dashboard JSON (D5: Cluster, Kafka Lag, HDFS)
│   └── README.md
│
├── notebooks/                    # Jupyter Notebooks phân tích khám phá (EDA)
│   ├── 01_eda_raw_reviews.ipynb
│   ├── 02_sentiment_aspect_exploration.ipynb
│   └── 03_scalability_fault_tolerance_analysis.ipynb
│
├── tests/                        # Kịch bản kiểm thử mở rộng & chịu lỗi
│   ├── scalability/              # Kịch bản SC1 -> SC4 (benchmark executor, scale data, streaming load)
│   │   ├── sc1_sc2_batch_benchmark.py
│   │   └── sc3_streaming_load_test.py
│   └── fault_tolerance/          # Kịch bản FT1 -> FT6 (chaos test: kill pod broker/datanode/spark)
│       └── chaos_test_scenarios.sh
│
├── containers/                   # Dockerfiles đóng gói container images cho Kubernetes
│   ├── Dockerfile.crawler
│   └── Dockerfile.spark
│
├── docs/                         # Tài liệu kỹ thuật & báo cáo
│   ├── architecture.md           # Chi tiết kiến trúc Lambda & luồng dữ liệu
│   └── deployment_guide.md       # Hướng dẫn dựng cụm K8s từ đầu
│
├── scripts/                      # Shell scripts tự động hóa vận hành
│   ├── setup_cluster.sh
│   ├── deploy_all.sh
│   └── teardown.sh
│
├── .env.example                  # Template biến môi trường
├── requirements.txt              # Thư viện Python phụ thuộc
└── README.md                     # Tài liệu tổng quan
```

---

## 3. Các phân vùng dữ liệu và Topics

### 3.1. Kafka Topics
* `steam.reviews.raw` (6 partitions, key: `appid`, retention: 7 ngày)
* `steam.players.ccu` (3 partitions, key: `appid`, retention: 7 ngày)
* `steam.games.meta` (3 partitions, key: `appid`, log compaction)
* `steam.alerts` (1 partition, key: `appid`, retention: 7 ngày)

### 3.2. HDFS Directory Layout
* Raw Layer: `/steam/raw/{reviews|ccu|meta}/dt=YYYY-MM-DD/` (JSON Lines gzip)
* Clean Layer:
  * `/steam/clean/reviews/dt=YYYY-MM-DD/` (Parquet Snappy)
  * `/steam/clean/ccu/dt=YYYY-MM-DD/` (Parquet)
  * `/steam/clean/games/` (Parquet)
* Curated Layer (Hive): `/steam/curated/{game_daily_stats|aspect_stats|playtime_buckets|trending}/`
* Streaming Checkpoints: `/steam/checkpoints/{job_name}/`

### 3.3. Elasticsearch Indices
* `batch_game_daily`: Thống kê ngày theo game
* `batch_aspect_stats`: Thống kê khía cạnh phàn nàn theo tháng
* `realtime_game_metrics`: Chỉ số thời gian thực (cửa sổ 5/60 phút)
* `reviews_search`: Chỉ mục toàn văn review tiếng Anh kèm điểm cảm xúc
* `alerts`: Cảnh báo review bombing và đột biến người chơi

---

## 4. Hướng dẫn bắt đầu nhanh (Quick Start)

### 4.1. Thiết lập biến môi trường
```bash
cp .env.example .env
# Chỉnh sửa file .env để điền STEAM_API_KEY và các cấu hình tương ứng
```

### 4.2. Cài đặt môi trường Python cục bộ (để phát triển crawler & scripts)
```bash
python -m venv venv
# Windows:
.\venv\Scripts\activate
# Linux/macOS:
source venv/bin/activate

pip install -r requirements.txt
```

### 4.3. Khởi tạo Elasticsearch Indices & HDFS Paths
```bash
# Khởi tạo HDFS directories
bash storage/hdfs/setup_hdfs_dirs.sh

# Khởi tạo Elasticsearch mappings
python storage/elasticsearch/setup_indices.py
```

### 4.4. Đóng gói Container Images cho Kubernetes
Hệ thống sử dụng các container image tùy biến được định nghĩa trong thư mục `containers/`:
```bash
# Build image cho Crawler (chạy dạng K8s CronJob/Deployment)
docker build -t steam-crawler:latest -f containers/Dockerfile.crawler .

# Build image cho PySpark Jobs (chạy trên K8s Spark Operator)
docker build -t steam-spark:latest -f containers/Dockerfile.spark .
```

### 4.5. Triển khai tự động lên cụm Kubernetes
Toàn bộ quy trình cài đặt Operators và cấu hình dịch vụ đã được tự động hóa qua thư mục `scripts/`:
```bash
# 1. Cài đặt các Operators (Strimzi Kafka, ECK Elasticsearch, Spark Operator)
bash scripts/setup_cluster.sh

# 2. Khởi tạo tài nguyên, cấu hình Kafka cluster, HDFS, Elasticsearch & Crawlers
bash scripts/deploy_all.sh
```
> Chi tiết hướng dẫn từng bước thủ công xem tại [docs/deployment_guide.md](file:///d:/BigData/steam-player-insights/docs/deployment_guide.md).

### 4.6. Vận hành luồng xử lý Spark
```bash
# Khởi chạy Speed Layer (Streaming)
spark-submit processing/streaming/s1_review_metrics.py

# Khởi chạy Batch Layer (Jobs B1 -> B6)
python processing/batch/b1_ingest_raw.py
python processing/batch/b2_clean_raw.py
python processing/batch/b3_game_daily_stats.py
python processing/batch/b4_playtime_buckets.py
python processing/batch/b5_aspect_sentiment.py
python processing/batch/b6_trending_games.py
```

### 4.7. Kiểm thử Scalability & Chịu lỗi (Fault-Tolerance)
```bash
# Đo đạc mở rộng Batch (1, 2, 4, 6 executors) theo kịch bản SC1 & SC2
python tests/scalability/sc1_sc2_batch_benchmark.py

# Kiểm thử chịu lỗi hạ tầng (FT1: mất Kafka broker, FT2: mất HDFS DataNode, FT4: mất ES node)
bash tests/fault_tolerance/chaos_test_scenarios.sh all
```


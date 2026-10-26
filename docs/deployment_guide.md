# Hướng Dẫn Triển Khai Hệ Thống Trên Kubernetes

Tài liệu hướng dẫn triển khai từ đầu (from scratch) toàn bộ kiến trúc Lambda phân tán trên cụm Kubernetes (k3s / kubeadm / GKE / AKS) có tối thiểu 3 worker nodes.

---

## 1. Yêu cầu chuẩn bị môi trường

* **Cụm Kubernetes:** Cụm 1 Master node + 3 Worker nodes (k3s hoặc kubeadm).
* **Công cụ cài đặt:**
  * `kubectl`
  * `helm` (v3+)
  * `python` (>= 3.10)
* **Tổng tài nguyên đề xuất:** 12–16 CPU Cores, 28–36 GB RAM.

---

## 2. Bước 1: Cài đặt các Kubernetes Operators

### 2.1. Cài đặt Strimzi Kafka Operator
```bash
helm repo add strimzi https://strimzi.io/charts/
helm repo update
helm install strimzi-kafka-operator strimzi/strimzi-kafka-cli --namespace kafka --create-namespace
```

### 2.2. Cài đặt Elastic Cloud on Kubernetes (ECK) Operator
```bash
kubectl create -f https://download.elastic.co/downloads/eck/2.11.1/crds.yaml
kubectl apply -f https://download.elastic.co/downloads/eck/2.11.1/operator.yaml
```

### 2.3. Cài đặt Spark on K8s Operator
```bash
helm repo add spark-operator https://kubeflow.github.io/spark-operator
helm repo update
helm install spark-operator spark-operator/spark-operator --namespace spark-operator --create-namespace --set webhook.enable=true
```

---

## 3. Bước 2: Triển khai các dịch vụ lưu trữ và hàng đợi

### 3.1. Triển khai Cụm Kafka & Topics
```bash
# Tạo cụm Kafka 3 brokers với RF=3
kubectl apply -f k8s/kafka/strimzi-kafka-cluster.yaml

# Đợi Kafka cluster sẵn sàng
kubectl wait kafka/steam-kafka-cluster --for=condition=Ready --timeout=300s -n kafka

# Tạo 4 topics: reviews, ccu, meta, alerts
kubectl apply -f k8s/kafka/kafka-topics.yaml
```

### 3.2. Triển khai HDFS Cluster
```bash
helm repo add gradiant https://gradiant.github.io/charts/
helm install hdfs gradiant/hdfs -f k8s/hdfs/hdfs-values.yaml --namespace hdfs --create-namespace

# Sau khi DataNodes sẵn sàng, chạy script tạo cấu trúc thư mục
kubectl exec -it hdfs-namenode-0 -n hdfs -- bash -c "$(cat storage/hdfs/setup_hdfs_dirs.sh)"
```

### 3.3. Triển khai Elasticsearch & Kibana
```bash
kubectl apply -f k8s/elasticsearch/eck-elasticsearch.yaml

# Khởi tạo các index mappings
python storage/elasticsearch/setup_indices.py
```

---

## 4. Bước 3: Đóng gói Container Images & Triển khai Ingestion

### 4.1. Đóng gói Container Images cho Kubernetes
```bash
# Build image cho Crawler (được dùng bởi Crawler CronJobs)
docker build -t steam-crawler:latest -f containers/Dockerfile.crawler .

# Build image cho PySpark Jobs (được dùng bởi Spark Operator)
docker build -t steam-spark:latest -f containers/Dockerfile.spark .
```

### 4.2. Triển khai RBAC và Crawler CronJobs
```bash
# Áp dụng RBAC cho Spark
kubectl apply -f k8s/spark/spark-operator-rbac.yaml

# Triển khai Crawler CronJobs
kubectl apply -f k8s/crawlers/crawler-cronjobs.yaml
```

---

## 5. Bước 4: Chạy các Job Xử Lý Dữ Liệu (Spark)

### 5.1. Khởi chạy Speed Layer (Streaming)

Các job đọc Kafka cần connector `spark-sql-kafka-0-10_2.12:3.5.7` (có sẵn trong `containers/Dockerfile.spark`, hoặc đặt `SPARK_PACKAGES`). Đặt `WRITE_ES=1` để ghi `realtime_game_metrics` và `alerts`:

```bash
python -m processing.streaming.s1_review_metrics   # chỉ số review cửa sổ 5/60 phút
python -m processing.streaming.s2_review_bombing_detector
python -m processing.streaming.s3_ccu_spike_detector
```

Repo chưa có manifest SparkApplication/Deployment cho S1–S3; cần bổ sung khi triển khai bằng Spark Operator.

### 5.2. Chạy Batch Layer

Thứ tự: B1 → B2 → B5 → B3 → B4 → B6 → B7 → B8 (B3 dùng cảm xúc từ B5; B7 tạo baseline cho S2/S3). Tương đương `make batch` hoặc `python pipeline.py --profile cluster batch` (pipeline bỏ B1 vì đọc thẳng raw).

```bash
python -m processing.batch.b1_ingest_raw          # Kafka -> /steam/raw/steam_*/dt=
python -m processing.batch.b2_clean_raw           # raw -> /steam/clean/{reviews,ccu,games,games_history}
python -m processing.batch.b5_aspect_sentiment    # khía cạnh, VADER, TF-IDF
python -m processing.batch.b3_game_daily_stats    # thống kê ngày/tuần/tháng, phân khúc
python -m processing.batch.b4_playtime_buckets    # giờ chơi, early_negative_ratio
python -m processing.batch.b6_trending_games      # trending, tác động giảm giá
python -m processing.batch.b7_baselines           # baseline cho S2/S3
python -m processing.batch.b8_game_clusters       # K-Means
python -m processing.serving.merge_views          # xoá realtime view đã được batch bao phủ (khi ghi ES)
```

---

## 6. Bước 5: Kiểm Thử Scalability & Chịu Lỗi (Fault-Tolerance)

```bash
# Benchmark khả năng mở rộng (SC1, SC2)
python tests/scalability/sc1_sc2_batch_benchmark.py

# Test khả năng chịu lỗi (FT1, FT2, FT4)
bash tests/fault_tolerance/chaos_test_scenarios.sh all
```

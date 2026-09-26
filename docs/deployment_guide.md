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
```bash
spark-submit \
  --master k8s://https://kubernetes.default.svc \
  --deploy-mode cluster \
  --name realtime-review-metrics \
  processing/streaming/s1_review_metrics.py
```

### 5.2. Chạy Batch Layer
```bash
# B1: Đọc Kafka -> HDFS Raw
python processing/batch/b1_ingest_raw.py

# B2: Làm sạch Raw -> Parquet Clean
python processing/batch/b2_clean_raw.py

# B3: Thống kê Game Daily Stats
python processing/batch/b3_game_daily_stats.py

# B4: Phân tích tương quan giờ chơi
python processing/batch/b4_playtime_buckets.py

# B5: Phân tích khía cạnh & cảm xúc
python processing/batch/b5_aspect_sentiment.py

# B6: Xếp hạng game trending
python processing/batch/b6_trending_games.py
```

---

## 6. Bước 5: Kiểm Thử Scalability & Chịu Lỗi (Fault-Tolerance)

```bash
# Benchmark khả năng mở rộng (SC1, SC2)
python tests/scalability/sc1_sc2_batch_benchmark.py

# Test khả năng chịu lỗi (FT1, FT2, FT4)
bash tests/fault_tolerance/chaos_test_scenarios.sh all
```

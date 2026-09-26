#!/bin/bash
# Script triển khai toàn bộ hạ tầng và pipeline dữ liệu lên Kubernetes

set -e

echo "=== 1. Deploying Kafka Cluster & Topics ==="
kubectl apply -f k8s/kafka/strimzi-kafka-cluster.yaml
echo "Waiting for Kafka cluster ready..."
kubectl wait kafka/steam-kafka-cluster --for=condition=Ready --timeout=300s -n kafka || true
kubectl apply -f k8s/kafka/kafka-topics.yaml

echo "=== 2. Deploying HDFS Cluster ==="
helm install hdfs gradiant/hdfs -f k8s/hdfs/hdfs-values.yaml --namespace hdfs --create-namespace || true

echo "=== 3. Deploying Elasticsearch & Kibana ==="
kubectl apply -f k8s/elasticsearch/eck-elasticsearch.yaml

echo "=== 4. Building Container Images ==="
docker build -t steam-crawler:latest -f containers/Dockerfile.crawler .
docker build -t steam-spark:latest -f containers/Dockerfile.spark .

echo "=== 5. Deploying RBAC & Crawlers ==="
kubectl apply -f k8s/spark/spark-operator-rbac.yaml
kubectl apply -f k8s/crawlers/crawler-cronjobs.yaml

echo "=== All services deployed! Check status with: kubectl get pods -A ==="

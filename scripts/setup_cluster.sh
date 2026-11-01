#!/bin/bash
# Cài các Operator / Helm chart nền tảng trên cụm Kubernetes (chạy 1 lần, tại gốc repo)
set -euo pipefail

echo "=== 1. Helm repositories ==="
helm repo add strimzi https://strimzi.io/charts/
helm repo add spark-operator https://kubeflow.github.io/spark-operator
helm repo add gradiant https://gradiant.github.io/charts/
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update

echo "=== 2. Strimzi Kafka Operator (KRaft) ==="
helm upgrade --install strimzi-kafka-operator strimzi/strimzi-kafka-operator --version 0.45.0 \
  --namespace kafka --create-namespace

echo "=== 3. ECK Operator (Elasticsearch + Kibana) ==="
kubectl apply -f https://download.elastic.co/downloads/eck/2.11.1/crds.yaml
kubectl apply -f https://download.elastic.co/downloads/eck/2.11.1/operator.yaml

echo "=== 4. Spark Operator (tuỳ chọn: pipeline dùng spark-submit client mode, xem k8s/spark/) ==="
helm upgrade --install spark-operator spark-operator/spark-operator \
  --namespace spark-operator --create-namespace --set "spark.jobNamespaces={spark-jobs}"

echo "=== 5. kube-prometheus-stack + Elasticsearch exporter ==="
kubectl create namespace monitoring --dry-run=client -o yaml | kubectl apply -f -
helm upgrade --install monitoring prometheus-community/kube-prometheus-stack \
  --namespace monitoring -f k8s/monitoring/kube-prometheus-values.yaml
helm upgrade --install es-exporter prometheus-community/prometheus-elasticsearch-exporter \
  --namespace monitoring -f k8s/monitoring/es-exporter-values.yaml

echo "=== Operators installed ==="
kubectl get pods -A | grep -E "kafka|elastic|spark|monitoring" || true

#!/bin/bash
# Script cài đặt các Operators cần thiết trên cụm Kubernetes

set -e

echo "=== 1. Adding Helm Repositories ==="
helm repo add strimzi https://strimzi.io/charts/
helm repo add spark-operator https://kubeflow.github.io/spark-operator
helm repo add gradiant https://gradiant.github.io/charts/
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update

echo "=== 2. Installing Strimzi Kafka Operator ==="
helm install strimzi-kafka-operator strimzi/strimzi-kafka-cli \
  --namespace kafka --create-namespace

echo "=== 3. Installing ECK (Elastic Cloud on Kubernetes) Operator ==="
kubectl create -f https://download.elastic.co/downloads/eck/2.11.1/crds.yaml
kubectl apply -f https://download.elastic.co/downloads/eck/2.11.1/operator.yaml

echo "=== 4. Installing Spark on K8s Operator ==="
helm install spark-operator spark-operator/spark-operator \
  --namespace spark-operator --create-namespace --set webhook.enable=true

echo "=== Cluster Operators Installed Successfully! ==="
kubectl get pods -A | grep -E "kafka|elastic|spark"

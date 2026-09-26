#!/bin/bash
# Script dọn dẹp tài nguyên trên cụm Kubernetes

echo "=== Tearing down Steam Player Insights pipeline ==="

kubectl delete -f k8s/crawlers/crawler-cronjobs.yaml --ignore-not-found
kubectl delete -f k8s/spark/spark-operator-rbac.yaml --ignore-not-found
kubectl delete -f k8s/elasticsearch/eck-elasticsearch.yaml --ignore-not-found
helm uninstall hdfs -n hdfs || true
kubectl delete -f k8s/kafka/kafka-topics.yaml --ignore-not-found
kubectl delete -f k8s/kafka/strimzi-kafka-cluster.yaml --ignore-not-found

echo "=== Teardown completed. ==="

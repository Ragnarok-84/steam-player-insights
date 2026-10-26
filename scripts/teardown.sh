#!/bin/bash
# Gỡ pipeline khỏi cụm Kubernetes. Mặc định GIỮ PVC (dữ liệu Kafka/HDFS/ES/Postgres).
# DELETE_DATA=1 ./scripts/teardown.sh  -> xoá luôn PVC (mất dữ liệu, không khôi phục được).
set -uo pipefail
echo "=== Tearing down Steam Player Insights pipeline ==="

kubectl delete -f k8s/crawlers/crawler-cronjobs.yaml --ignore-not-found
kubectl delete -f k8s/spark/steam-jobs.yaml --ignore-not-found
kubectl delete -f k8s/monitoring/kafka-podmonitor.yaml --ignore-not-found
kubectl delete configmap grafana-d5-system -n monitoring --ignore-not-found
kubectl delete -f k8s/hive/hive-metastore.yaml --ignore-not-found
kubectl delete -f k8s/elasticsearch/eck-elasticsearch.yaml --ignore-not-found
helm uninstall hdfs -n hdfs || true
kubectl delete -f k8s/kafka/kafka-topics.yaml --ignore-not-found
kubectl delete -f k8s/kafka/strimzi-kafka-cluster.yaml --ignore-not-found
kubectl delete -f k8s/spark/spark-operator-rbac.yaml --ignore-not-found
kubectl delete -f k8s/common/pipeline-config.yaml --ignore-not-found

if [ "${DELETE_DATA:-0}" = "1" ]; then
  echo "=== Deleting PVCs (data loss) ==="
  for ns in kafka hdfs elastic-system hive default; do
    kubectl delete pvc --all -n "$ns" --ignore-not-found
  done
fi
echo "=== Teardown completed. Operators (Strimzi/ECK/Spark/monitoring) vẫn giữ; gỡ bằng helm uninstall nếu cần. ==="

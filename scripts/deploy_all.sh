#!/bin/bash
# Triển khai hạ tầng + pipeline lên Kubernetes (chạy sau scripts/setup_cluster.sh, tại gốc repo).
# REGISTRY=registry.example.com/steam ./scripts/deploy_all.sh   -> build + push image lên registry
# Không đặt REGISTRY: image build local; với k3s chạy thêm: K3S_IMPORT=1 (import vào containerd của node hiện tại).
set -euo pipefail
TAG=${TAG:-1.0.0}
REGISTRY=${REGISTRY:-}
SPARK_IMAGE=${REGISTRY:+$REGISTRY/}steam-spark:$TAG
CRAWLER_IMAGE=${REGISTRY:+$REGISTRY/}steam-crawler:$TAG

echo "=== 1. Kafka (KRaft, 3 node) & topics ==="
kubectl apply -f k8s/kafka/strimzi-kafka-cluster.yaml
kubectl wait kafka/steam-kafka-cluster --for=condition=Ready --timeout=600s -n kafka
kubectl apply -f k8s/kafka/kafka-topics.yaml

echo "=== 2. HDFS (1 NameNode + 3 DataNode) ==="
helm upgrade --install hdfs gradiant/hdfs -f k8s/hdfs/hdfs-values.yaml --namespace hdfs --create-namespace --wait --timeout 10m
kubectl exec -i -n hdfs hdfs-namenode-0 -- bash < storage/hdfs/setup_hdfs_dirs.sh

echo "=== 3. Elasticsearch (3 node) & Kibana ==="
kubectl apply -f k8s/elasticsearch/eck-elasticsearch.yaml
kubectl wait elasticsearch/steam-elasticsearch -n elastic-system --for=jsonpath='{.status.health}'=green --timeout=900s

echo "=== 4. Secret mật khẩu ES cho spark-jobs và monitoring ==="
kubectl apply -f k8s/common/pipeline-config.yaml
ES_PASS=$(kubectl get secret steam-elasticsearch-es-elastic-user -n elastic-system -o jsonpath='{.data.elastic}' | base64 -d)
for ns in spark-jobs monitoring; do
  kubectl create namespace "$ns" --dry-run=client -o yaml | kubectl apply -f -
  kubectl create secret generic steam-es-credentials -n "$ns" \
    --from-literal=elastic="$ES_PASS" --from-literal=username=elastic --dry-run=client -o yaml | kubectl apply -f -
done

echo "=== 5. Hive Metastore + PostgreSQL ==="
kubectl apply -f k8s/hive/hive-metastore.yaml
kubectl rollout status statefulset/hive-postgres -n hive --timeout=300s
kubectl rollout status deployment/hive-metastore -n hive --timeout=600s

echo "=== 6. Container images ($SPARK_IMAGE, $CRAWLER_IMAGE) ==="
docker build -t "$SPARK_IMAGE" -f containers/Dockerfile.spark .
docker build -t "$CRAWLER_IMAGE" -f containers/Dockerfile.crawler .
if [ -n "$REGISTRY" ]; then
  docker push "$SPARK_IMAGE"; docker push "$CRAWLER_IMAGE"
elif [ "${K3S_IMPORT:-0}" = "1" ]; then
  docker save "$SPARK_IMAGE" "$CRAWLER_IMAGE" | sudo k3s ctr images import -
fi

echo "=== 7. Spark RBAC, batch CronJob, streaming Deployments, crawler CronJobs ==="
kubectl apply -f k8s/spark/spark-operator-rbac.yaml
sed "s#steam-spark:1.0.0#$SPARK_IMAGE#g" k8s/common/pipeline-config.yaml | kubectl apply -f -
sed "s#steam-spark:1.0.0#$SPARK_IMAGE#g" k8s/spark/steam-jobs.yaml | kubectl apply -f -
sed "s#steam-crawler:1.0.0#$CRAWLER_IMAGE#g" k8s/crawlers/crawler-cronjobs.yaml | kubectl apply -f -

echo "=== 8. ES indices + Kibana data views/dashboards (job một lần) ==="
kubectl delete pod steam-es-setup -n spark-jobs --ignore-not-found
kubectl run steam-es-setup -n spark-jobs --image="$SPARK_IMAGE" --restart=Never \
  --overrides='{"spec":{"containers":[{"name":"steam-es-setup","image":"'"$SPARK_IMAGE"'","command":["sh","-c","python3 pipeline.py --profile cluster es-setup && python3 pipeline.py --profile cluster kibana-setup"],"envFrom":[{"configMapRef":{"name":"steam-pipeline-config"}}],"env":[{"name":"ELASTICSEARCH_PASSWORD","valueFrom":{"secretKeyRef":{"name":"steam-es-credentials","key":"elastic"}}},{"name":"KIBANA_HOST","value":"https://steam-kibana-kb-http.elastic-system.svc.cluster.local:5601"}]}]}}'
kubectl wait pod/steam-es-setup -n spark-jobs --for=jsonpath='{.status.phase}'=Succeeded --timeout=300s

echo "=== 9. Monitoring: Kafka PodMonitor + Grafana dashboard D5 ==="
kubectl apply -f k8s/monitoring/kafka-podmonitor.yaml
kubectl create configmap grafana-d5-system -n monitoring --from-file=dashboards/grafana/d5_system.json \
  --dry-run=client -o yaml | kubectl label --local -f - grafana_dashboard=1 -o yaml | kubectl apply -f -

echo "=== Done. kubectl get pods -A ; Kibana: kubectl port-forward -n elastic-system svc/steam-kibana-kb-http 5601 ==="

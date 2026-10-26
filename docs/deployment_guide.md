# Hướng dẫn triển khai trên Kubernetes

Triển khai toàn bộ kiến trúc Lambda lên cụm Kubernetes (k3s / kubeadm / GKE / AKS) có ít nhất 3 worker node.

> Phần K8s đã được kiểm tra schema (kubeconform, kể cả CRD Strimzi/ECK/Prometheus). Các thành phần (Spark job, Kafka, ES, Kibana, Hive Metastore 3.1.3 + PostgreSQL) đã chạy thử trong Docker với cùng cấu hình. Chưa chạy thử trên một cụm K8s thật, nên lần triển khai đầu cần theo dõi log từng bước.

## 1. Chuẩn bị

| Hạng mục | Yêu cầu |
| --- | --- |
| Cụm | 1 control-plane + ≥ 3 worker; tổng ≈ 12–16 CPU, 28–36 GB RAM (thiết kế §5.4) |
| Storage | StorageClass mặc định (k3s: `local-path`). Crawler review chạy 2 worker cần PVC `ReadWriteMany` (NFS/Longhorn); nếu chỉ có `local-path` thì đặt 1 worker |
| Công cụ | `kubectl`, `helm` v3, `docker`; chạy lệnh tại gốc repo trong Bash (Linux/WSL) |
| Image | Registry mà các node kéo được (`REGISTRY=...`), hoặc k3s import trực tiếp (`K3S_IMPORT=1`) |

Dựng k3s nhanh trên 3–4 VM (Multipass/VirtualBox):

```bash
# node 1 (server)
curl -sfL https://get.k3s.io | sh -
sudo cat /var/lib/rancher/k3s/server/node-token
# các node còn lại (agent)
curl -sfL https://get.k3s.io | K3S_URL=https://<ip-node-1>:6443 K3S_TOKEN=<token> sh -
# trên máy điều khiển
mkdir -p ~/.kube && sudo cp /etc/rancher/k3s/k3s.yaml ~/.kube/config && sudo chown $USER ~/.kube/config
kubectl get nodes
```

## 2. Cài operator và monitoring (1 lần)

```bash
./scripts/setup_cluster.sh
```

Script cài: Strimzi 0.45 (Kafka KRaft), ECK 2.11.1, Spark Operator (tuỳ chọn), kube-prometheus-stack và Elasticsearch exporter. Trước khi chạy, đổi `grafana.adminPassword` trong `k8s/monitoring/kube-prometheus-values.yaml`.

## 3. Triển khai pipeline

```bash
# Có registry:
REGISTRY=registry.example.com/steam ./scripts/deploy_all.sh
# k3s 1 node điều khiển (import image vào containerd của node hiện tại; các node khác cần import tương tự):
K3S_IMPORT=1 ./scripts/deploy_all.sh
```

`deploy_all.sh` lần lượt thực hiện:

1. Dựng Kafka 3 node KRaft (RF=3, `min.insync.replicas=2`) và 4 topic.
2. Dựng HDFS gồm 1 NameNode và 3 DataNode, rồi tạo thư mục `/steam/...` với replication 3.
3. Dựng Elasticsearch 3 node và Kibana, rồi chép mật khẩu `elastic` sang secret `steam-es-credentials` trong `spark-jobs` và `monitoring`.
4. Dựng Hive Metastore 3.1.3 với PostgreSQL. Đổi mật khẩu trong `k8s/hive/hive-metastore.yaml` trước khi chạy.
5. Build image `steam-spark` và `steam-crawler`.
6. Tạo RBAC, CronJob batch hằng ngày, 3 Deployment streaming và các CronJob crawler.
7. Tạo index Elasticsearch, data view và dashboard Kibana D1–D4.
8. Tạo PodMonitor cho Kafka và dashboard Grafana D5.

API key Steam (cần cho `steam-applist-crawler`; CCU dùng key nếu có):

```bash
kubectl create secret generic steam-api -n default --from-literal=STEAM_API_KEY=<key>
```

## 4. Cách Spark chạy trên cụm

- Driver chạy trong pod CronJob/Deployment ở namespace `spark-jobs`, dùng `spark-submit` client mode. Cấu hình nằm ở ConfigMap `spark-defaults` (`k8s/common/pipeline-config.yaml`).
- Executor là pod do Spark tạo. Dynamic allocation co giãn từ 2 đến 6 pod, mỗi pod 1 CPU và 2 GB.
- Batch chạy lúc 01:30 UTC: B1 (Kafka → `/steam/raw`), sau đó B2 → B5 → B3 → B4 → B6 → B7 → B8, rồi ghi ES và hợp nhất view. Chạy tay:

  ```bash
  kubectl create job batch-now -n spark-jobs --from=cronjob/steam-batch-daily
  ```

- Streaming gồm `steam-s1`, `steam-s2`, `steam-s3`. Checkpoint nằm ở `/steam/checkpoints` trên HDFS. Pod bị xoá sẽ được tạo lại và đọc tiếp từ checkpoint.
- Ngưỡng cảnh báo (`S2_SIGMA`, `S2_POSITIVE_DROP_POINTS`, `S3_CHANGE_THRESHOLD`) nằm trong ConfigMap `steam-pipeline-config`. Sửa xong thì chạy:

  ```bash
  kubectl rollout restart deploy/steam-s2 -n spark-jobs
  ```

## 5. Nạp dữ liệu đã thu thập (JSONL) vào cụm

Chọn một trong hai cách:

```bash
# Cách 1: chép thẳng vào HDFS raw (batch đọc được ngay)
kubectl cp data/raw hdfs/hdfs-namenode-0:/tmp/raw
kubectl exec -n hdfs hdfs-namenode-0 -- bash -c "hdfs dfs -put -f /tmp/raw/steam_* /steam/raw/"

# Cách 2: replay qua Kafka (đi qua cả B1 và speed layer), chạy trong một pod trong cụm
kubectl run replay -n spark-jobs --image=steam-spark:1.0.0 --restart=Never --command -- sleep infinity
kubectl cp data/raw spark-jobs/replay:/tmp/raw
kubectl exec -n spark-jobs replay -- env KAFKA_BOOTSTRAP_SERVERS=steam-kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  python3 -m crawlers.replay_jsonl_to_kafka --raw /tmp/raw --rate 2000
kubectl delete pod replay -n spark-jobs
```

## 6. Truy cập giao diện

```bash
# Kibana: https://localhost:5601, user elastic, mật khẩu:
kubectl get secret steam-elasticsearch-es-elastic-user -n elastic-system -o jsonpath='{.data.elastic}' | base64 -d
kubectl port-forward -n elastic-system svc/steam-kibana-kb-http 5601
# Grafana: http://localhost:3000 (admin / mật khẩu trong kube-prometheus-values.yaml)
kubectl port-forward -n monitoring svc/monitoring-grafana 3000:80
# Spark SQL ad-hoc qua Hive
kubectl run sql -n spark-jobs -it --rm --image=steam-spark:1.0.0 --overrides='{"spec":{"serviceAccountName":"spark-operator-sa"}}' \
  -- /opt/spark/bin/spark-sql --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore.hive.svc.cluster.local:9083
```

Câu truy vấn mẫu nằm ở `storage/hive/adhoc_queries.sql`.

## 7. Kiểm thử mở rộng và chịu lỗi

Xem mục 6 của [README](../README.md). Script chịu lỗi xoá pod thật, nên chỉ chạy trên cụm thử nghiệm.

## 8. Gỡ

```bash
./scripts/teardown.sh                 # giữ PVC (dữ liệu)
DELETE_DATA=1 ./scripts/teardown.sh   # xoá luôn PVC, không khôi phục được
```

## 9. Khi thiếu RAM (thiết kế §9)

- Giảm Elasticsearch xuống 2 node (`count: 2`) và giới hạn bộ nhớ ở 2 Gi.
- Giảm Kafka JVM heap xuống `-Xmx512m`.
- Đặt `spark.dynamicAllocation.maxExecutors` = 3.
- Tạm tắt `steam-s3` khi không cần cảnh báo CCU:

  ```bash
  kubectl scale deploy/steam-s3 -n spark-jobs --replicas=0
  ```

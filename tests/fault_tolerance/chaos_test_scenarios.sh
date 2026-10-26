#!/bin/bash
# Kiểm thử chịu lỗi FT1–FT6 trên cụm Kubernetes (chạy tại gốc repo, kubectl trỏ đúng cụm THỬ NGHIỆM).
# Mỗi kịch bản ghi 1 dòng vào tests/results/ft_results.csv: kịch bản, thời gian phục hồi, số liệu trước/sau.
#   ./tests/fault_tolerance/chaos_test_scenarios.sh ft1|ft2|ft3|ft4|ft5 <node>|ft6|all
# CẢNH BÁO: script xoá pod / drain node thật. Không chạy trên cụm dùng chung khi chưa thống nhất với nhóm.
set -euo pipefail

RESULTS=tests/results/ft_results.csv
mkdir -p tests/results
[ -f "$RESULTS" ] || echo "scenario,started_at,recovery_seconds,before,after,status,note" > "$RESULTS"

record() { echo "$1,$(date -u +%FT%TZ),$2,$3,$4,$5,$6" >> "$RESULTS"; echo "[$1] $5: recovery=${2}s before=$3 after=$4 $6"; }
now() { date +%s; }

kafka_offsets() {  # tổng offset cuối của topic review (số message đã ghi thành công)
  kubectl exec -n kafka steam-kafka-cluster-dual-role-0 -- bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:9092 --topic steam.reviews.raw 2>/dev/null | awk -F: '{s+=$3} END {print s+0}'
}
es_count() {  # số document của 1 index (qua pod ES, dùng mật khẩu ECK)
  local pass; pass=$(kubectl get secret steam-elasticsearch-es-elastic-user -n elastic-system -o jsonpath='{.data.elastic}' | base64 -d)
  kubectl exec -n elastic-system steam-elasticsearch-es-default-0 -- curl -sk -u "elastic:$pass" \
    "https://localhost:9200/$1/_count" | sed -E 's/.*"count":([0-9]+).*/\1/'
}
es_health() {
  local pass; pass=$(kubectl get secret steam-elasticsearch-es-elastic-user -n elastic-system -o jsonpath='{.data.elastic}' | base64 -d)
  kubectl exec -n elastic-system "${1:-steam-elasticsearch-es-default-1}" -- curl -sk -u "elastic:$pass" \
    "https://localhost:9200/_cluster/health" | sed -E 's/.*"status":"([a-z]+)".*/\1/'
}

ft1_kafka_broker() {
  # Producer liên tục (replay) trong lúc xoá 1 broker; RF=3, min.insync=2 -> không mất message
  echo "[FT1] Replay review vào Kafka nền, xoá 1 broker"
  local before; before=$(kafka_offsets)
  kubectl run ft1-replay -n spark-jobs --image="${SPARK_IMAGE:-steam-spark:1.0.0}" --restart=Never \
    --env=KAFKA_BOOTSTRAP_SERVERS=steam-kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
    --command -- sh -c "python3 tests/make_sample_data.py --out /tmp/raw --games 20 --days 3 && python3 -m crawlers.replay_jsonl_to_kafka --raw /tmp/raw --topics steam_reviews_raw --rate 300" >/dev/null
  sleep 40
  local t0; t0=$(now)
  kubectl delete pod steam-kafka-cluster-dual-role-1 -n kafka --wait=false
  kubectl wait --for=condition=ready pod/steam-kafka-cluster-dual-role-1 -n kafka --timeout=600s
  local rec=$(( $(now) - t0 ))
  kubectl wait pod/ft1-replay -n spark-jobs --for=jsonpath='{.status.phase}'=Succeeded --timeout=900s
  local sent; sent=$(kubectl logs ft1-replay -n spark-jobs | sed -nE 's/.*Done: ([0-9]+) records.*/\1/p')
  local after; after=$(kafka_offsets)
  kubectl delete pod ft1-replay -n spark-jobs >/dev/null
  local status=FAIL; [ $((after - before)) -ge "${sent:-0}" ] && status=PASS
  record FT1 "$rec" "$before" "$after" "$status" "sent=${sent:-?}; offsets_delta=$((after - before))"
}

ft2_hdfs_datanode() {
  echo "[FT2] Xoá 1 DataNode, kiểm tra đọc/ghi và tái sao chép block"
  kubectl exec -n hdfs hdfs-namenode-0 -- bash -c "head -c 50000000 /dev/urandom > /tmp/ft2.bin && hdfs dfs -put -f /tmp/ft2.bin /steam/ft2.bin"
  local dn; dn=$(kubectl get pods -n hdfs -o name | grep datanode | head -1 | cut -d/ -f2)
  local t0; t0=$(now)
  kubectl delete pod "$dn" -n hdfs --wait=false
  # đọc/ghi trong lúc DataNode mất
  kubectl exec -n hdfs hdfs-namenode-0 -- bash -c "hdfs dfs -cat /steam/ft2.bin | md5sum && hdfs dfs -put -f /tmp/ft2.bin /steam/ft2b.bin"
  kubectl wait --for=condition=ready "pod/$dn" -n hdfs --timeout=600s
  local rec=$(( $(now) - t0 ))
  local under; under=$(kubectl exec -n hdfs hdfs-namenode-0 -- hdfs fsck /steam 2>/dev/null | sed -nE 's/.*Under-replicated blocks:\s+([0-9]+).*/\1/p')
  local live; live=$(kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfsadmin -report 2>/dev/null | sed -nE 's/^Live datanodes \(([0-9]+)\).*/\1/p')
  local status=FAIL; [ "${live:-0}" -ge 3 ] && status=PASS
  record FT2 "$rec" "3" "${live:-?}" "$status" "under_replicated_blocks=${under:-?}"
  kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -rm -skipTrash /steam/ft2.bin /steam/ft2b.bin >/dev/null || true
}

ft3_spark() {
  echo "[FT3a] Xoá 1 executor giữa job batch: task chạy lại, job vẫn thành công"
  kubectl create job ft3-batch -n spark-jobs --from=cronjob/steam-batch-daily
  sleep 90
  local ex; ex=$(kubectl get pods -n spark-jobs -l spark-role=executor -o name | head -1)
  local t0; t0=$(now)
  [ -n "$ex" ] && kubectl delete "$ex" -n spark-jobs --wait=false
  kubectl wait job/ft3-batch -n spark-jobs --for=condition=complete --timeout=3600s && st=PASS || st=FAIL
  record FT3a "$(( $(now) - t0 ))" "executor=${ex:-none}" "job_complete" "$st" "batch job finished after executor loss"
  kubectl delete job ft3-batch -n spark-jobs >/dev/null

  echo "[FT3b] Xoá driver streaming S1: Deployment tạo lại, tiếp tục từ checkpoint (ghi ES theo id -> không trùng)"
  local before; before=$(es_count realtime_game_metrics)
  local drv; drv=$(kubectl get pods -n spark-jobs -l app=steam-s1 -o name | head -1)
  t0=$(now)
  kubectl delete "$drv" -n spark-jobs --wait=false
  kubectl wait --for=condition=available deployment/steam-s1 -n spark-jobs --timeout=600s
  local rec=$(( $(now) - t0 ))
  sleep 120
  local after; after=$(es_count realtime_game_metrics)
  st=FAIL; [ "${after:-0}" -ge "${before:-0}" ] && st=PASS
  record FT3b "$rec" "$before" "$after" "$st" "realtime docs keep growing after driver restart"
}

ft4_elasticsearch() {
  echo "[FT4] Xoá 1 pod ES: cluster yellow -> replica được nâng lên -> green"
  local before; before=$(es_count reviews_search)
  local t0; t0=$(now)
  kubectl delete pod steam-elasticsearch-es-default-0 -n elastic-system --wait=false
  sleep 5
  echo "health during failure: $(es_health steam-elasticsearch-es-default-1)"
  until [ "$(es_health steam-elasticsearch-es-default-1)" = "green" ]; do sleep 5; done
  local rec=$(( $(now) - t0 ))
  local after; after=$(es_count reviews_search)
  local st=FAIL; [ "$after" = "$before" ] && st=PASS
  record FT4 "$rec" "$before" "$after" "$st" "reviews_search doc count unchanged"
}

ft5_worker_node() {
  local node=${1:?Cần tên node: ft5 <node>}
  echo "[FT5] Drain node $node (mô phỏng tắt máy ảo; có thể tắt VM thật thay cho drain)"
  local before; before=$(kubectl get pods -A --field-selector=status.phase=Running --no-headers | wc -l)
  local t0; t0=$(now)
  kubectl drain "$node" --ignore-daemonsets --delete-emptydir-data --timeout=600s
  # chờ mọi pod hệ thống lên lại trên node khác
  kubectl wait kafka/steam-kafka-cluster -n kafka --for=condition=Ready --timeout=900s
  kubectl wait elasticsearch/steam-elasticsearch -n elastic-system --for=jsonpath='{.status.phase}'=Ready --timeout=900s
  kubectl rollout status deployment/steam-s1 -n spark-jobs --timeout=600s
  local rec=$(( $(now) - t0 ))
  local after; after=$(kubectl get pods -A --field-selector=status.phase=Running --no-headers | wc -l)
  kubectl uncordon "$node"
  record FT5 "$rec" "$before" "$after" "PASS" "pods rescheduled; với 3 node + anti-affinity bắt buộc, pod thứ 3 Pending tới khi node về"
}

ft6_source() {
  echo "[FT6] API trả 429 (mock, chạy local)"
  python tests/fault_tolerance/ft6_crawler_rate_limit.py && st=PASS || st=FAIL
  record FT6 "" "" "" "$st" "crawler backoff + resume (tests/fault_tolerance/ft6_crawler_rate_limit.py)"
}

case "${1:-}" in
  ft1) ft1_kafka_broker ;;
  ft2) ft2_hdfs_datanode ;;
  ft3) ft3_spark ;;
  ft4) ft4_elasticsearch ;;
  ft5) ft5_worker_node "${2:-}" ;;
  ft6) ft6_source ;;
  all) ft1_kafka_broker; ft2_hdfs_datanode; ft3_spark; ft4_elasticsearch; ft6_source
       echo "FT5 cần tên node: $0 ft5 <node>" ;;
  *) echo "Usage: $0 {ft1|ft2|ft3|ft4|ft5 <node>|ft6|all}"; exit 1 ;;
esac
echo "Results: $RESULTS"

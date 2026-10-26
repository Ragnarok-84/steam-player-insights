#!/usr/bin/env bash
set -euo pipefail
REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"
command -v kubectl >/dev/null || { echo "Configure kubectl on the server VM first."; exit 1; }
ns=steam-demo

init_kafka() {
  for topic in steam.reviews.raw steam.players.ccu steam.games.meta steam.alerts; do
    topic_config=(--config retention.ms=86400000)
    if [[ "$topic" == steam.games.meta ]]; then topic_config=(--config cleanup.policy=compact); fi
    kubectl -n "$ns" exec kafka-0 -- /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server kafka:9092 --create --if-not-exists --topic "$topic" \
      --partitions 1 --replication-factor 1 "${topic_config[@]}"
  done
}

case "${1:-}" in
  label-nodes)
    [[ $# == 4 ]] || { echo "Usage: bash scripts/k3s-demo.sh label-nodes SERVER WORKER1 WORKER2"; exit 2; }
    [[ "$2" != "$3" && "$2" != "$4" && "$3" != "$4" ]] || { echo "Use three different nodes."; exit 2; }
    for node in "$2" "$3" "$4"; do kubectl get node "$node" >/dev/null; done
    kubectl label node "$2" steam.insights/role=storage --overwrite
    kubectl label node "$3" steam.insights/role=ingest --overwrite
    kubectl label node "$4" steam.insights/role=compute --overwrite
    ;;
  render) kubectl kustomize k8s/demo ;;
  deploy)
    for role in storage ingest compute; do
      count="$(kubectl get nodes -l "steam.insights/role=$role" -o name | wc -l)"
      [[ "$count" == 1 ]] || { echo "Expected exactly one node for role $role; label nodes first."; exit 1; }
      kubectl wait --for=condition=Ready node -l "steam.insights/role=$role" --timeout=60s
    done
    kubectl get storageclass local-path >/dev/null
    kubectl apply -k k8s/demo
    for app in namenode datanode kafka elasticsearch; do
      kubectl -n "$ns" rollout status "statefulset/$app" --timeout=600s
    done
    kubectl -n "$ns" rollout status deployment/kibana --timeout=600s
    kubectl -n "$ns" exec namenode-0 -- hdfs dfs -mkdir -p \
      /steam/raw /steam/clean /steam/curated /steam/checkpoints /steam/demo
    init_kafka
    kubectl -n "$ns" get pods,pvc -o wide
    ;;
  status)
    kubectl get nodes -o wide
    kubectl -n "$ns" get pods,pvc -o wide
    kubectl -n "$ns" get events --sort-by=.lastTimestamp
    ;;
  upload-raw)
    for source in steam_games_meta steam_players_ccu steam_reviews_raw; do
      [[ -d "data/raw/$source" ]] || { echo "Missing data/raw/$source"; exit 1; }
      if kubectl -n "$ns" exec namenode-0 -- hdfs dfs -test -e "/steam/raw/$source"; then
        echo "HDFS target /steam/raw/$source already exists; refusing duplicate upload."; exit 1
      fi
    done
    kubectl -n "$ns" exec namenode-0 -- mkdir -p /tmp/steam-demo-input
    kubectl -n "$ns" cp data/raw/. namenode-0:/tmp/steam-demo-input
    for source in steam_games_meta steam_players_ccu steam_reviews_raw; do
      kubectl -n "$ns" exec namenode-0 -- hdfs dfs -put "/tmp/steam-demo-input/$source" /steam/raw/
    done
    kubectl -n "$ns" exec namenode-0 -- hdfs dfs -ls /steam/raw
    ;;
  smoke|b2)
    job="demo-$1"
    if kubectl -n "$ns" get job "$job" >/dev/null 2>&1; then
      echo "Job $job already exists. Inspect logs and delete that Job explicitly before rerunning."; exit 1
    fi
    if [[ "$1" == b2 ]]; then
      # B2 tính lại toàn bộ từ raw và ghi đè clean (reviews/ccu/games), chạy lại không sinh trùng
      kubectl -n "$ns" exec namenode-0 -- hdfs dfs -test -e /steam/raw/steam_reviews_raw
    fi
    kubectl apply -f "k8s/demo/jobs/$1.yaml"
    if ! kubectl -n "$ns" wait --for=condition=complete "job/$job" --timeout=1800s; then
      kubectl -n "$ns" describe "job/$job"
      kubectl -n "$ns" logs "job/$job" --all-containers=true || true
      exit 1
    fi
    kubectl -n "$ns" logs "job/$job"
    ;;
  kibana) kubectl -n "$ns" port-forward --address 127.0.0.1 svc/kibana 5601:5601 ;;
  es) kubectl -n "$ns" port-forward --address 127.0.0.1 svc/elasticsearch 9200:9200 ;;
  *) echo "Usage: bash scripts/k3s-demo.sh {label-nodes|render|deploy|status|upload-raw|smoke|b2|kibana|es}"; exit 2 ;;
esac

#!/usr/bin/env bash
set -euo pipefail
REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"
command -v kubectl >/dev/null || { echo "Configure kubectl on the server VM first."; exit 1; }
source scripts/k3s-images.sh
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

run_demo_job() {
  local job
  job="$(kubectl -n "$ns" create -f - -o name)"
  if ! kubectl -n "$ns" wait --for=condition=complete "$job" --timeout=1830s; then
    kubectl -n "$ns" describe "$job"
    kubectl -n "$ns" logs "$job" --all-containers=true || true
    return 1
  fi
  kubectl -n "$ns" get pods -l "batch.kubernetes.io/job-name=${job##*/}" -o wide
  kubectl -n "$ns" logs "$job"
}

case "${1:-}" in
  label-nodes)
    [[ $# == 4 || $# == 5 ]] || { echo "Usage: bash scripts/k3s-demo.sh label-nodes SERVER WORKER1 WORKER2 [WORKER3]"; exit 2; }
    count="$(printf '%s\n' "${@:2}" | sort -u | wc -l)"
    [[ "$count" == "$(($# - 1))" ]] || { echo "Use different names for every node."; exit 2; }
    for node in "${@:2}"; do kubectl get node "$node" >/dev/null; done
    kubectl label node "$2" steam.insights/role=storage --overwrite
    kubectl label node "$3" steam.insights/role=ingest --overwrite
    kubectl label node "$4" steam.insights/role=compute --overwrite
    if [[ $# == 5 ]]; then
      kubectl label node "$5" steam.insights/compute=true --overwrite
    fi
    ;;
  render) kubectl kustomize k8s/demo ;;
  render-crawlers) kubectl kustomize k8s/demo/crawlers | render_steam_images ;;
  deploy)
    for role in storage ingest compute; do
      count="$(kubectl get nodes -l "steam.insights/role=$role" -o name | wc -l)"
      [[ "$count" == 1 ]] || { echo "Expected exactly one node for role $role; label nodes first."; exit 1; }
      kubectl wait --for=condition=Ready node -l "steam.insights/role=$role" --timeout=60s
    done
    kubectl get storageclass local-path >/dev/null
    kubectl apply -f k8s/demo/namespace.yaml
    kubectl apply --dry-run=server -k k8s/demo
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
  deploy-crawlers)
    # Bind the crawler's local-path PVC on the ingest node where its image is imported.
    kubectl kustomize k8s/demo/crawlers | render_steam_images | kubectl apply --dry-run=server -f -
    kubectl kustomize k8s/demo/crawlers | render_steam_images | kubectl apply -f -
    seed_crawler_targets "$ns"
    kubectl -n "$ns" get cronjobs,pvc
    ;;
  phase1)
    [[ $# == 5 ]] || { echo "Usage: bash scripts/k3s-demo.sh phase1 SERVER WORKER1 WORKER2 WORKER3"; exit 2; }
    bash scripts/k3s-demo.sh label-nodes "${@:2}"
    for node in "${@:2}"; do kubectl wait --for=condition=Ready "node/$node" --timeout=180s; done
    bash scripts/k3s-demo.sh deploy
    bash scripts/k3s-demo.sh deploy-crawlers
    bash scripts/k3s-demo.sh smoke
    bash scripts/k3s-demo.sh smoke-extra "$5"
    kubectl get nodes -o wide
    kubectl -n "$ns" get pods,pvc,cronjobs -o wide
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
  smoke)
    render_demo_job k8s/demo/jobs/smoke.yaml | run_demo_job
    ;;
  smoke-extra)
    [[ $# == 2 ]] || { echo "Usage: bash scripts/k3s-demo.sh smoke-extra NODE"; exit 2; }
    hostname="$(kubectl get node "$2" -o 'jsonpath={.metadata.labels.kubernetes\.io/hostname}')"
    [[ "$hostname" =~ ^[a-zA-Z0-9][a-zA-Z0-9_.-]*$ ]] || { echo "Node $2 needs a Kubernetes hostname label."; exit 1; }
    patch='{"metadata":{"generateName":"demo-smoke-extra-"},"spec":{"template":{"spec":{"nodeSelector":{"steam.insights/role":null,"kubernetes.io/hostname":"'"$hostname"'"}}}}}'
    render_demo_job k8s/demo/jobs/smoke.yaml |
      kubectl patch --local -f - --type=merge -p "$patch" -o yaml |
      run_demo_job
    ;;
  b2)
    job=demo-b2
    if kubectl -n "$ns" get job "$job" >/dev/null 2>&1; then
      echo "Job $job already exists. Inspect logs and delete that Job explicitly before rerunning."; exit 1
    fi
    kubectl -n "$ns" exec namenode-0 -- hdfs dfs -test -e /steam/raw/steam_reviews_raw
    render_demo_job k8s/demo/jobs/b2.yaml | run_demo_job
    ;;
  kibana) kubectl -n "$ns" port-forward --address 127.0.0.1 svc/kibana 5601:5601 ;;
  es) kubectl -n "$ns" port-forward --address 127.0.0.1 svc/elasticsearch 9200:9200 ;;
  *) echo "Usage: bash scripts/k3s-demo.sh {phase1|label-nodes|render|render-crawlers|deploy|deploy-crawlers|status|upload-raw|smoke|smoke-extra|b2|kibana|es}"; exit 2 ;;
esac

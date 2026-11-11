#!/usr/bin/env bash
# Explicit test environment; never mutates steam-demo or stops a node automatically.
set -euo pipefail
REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"
command -v kubectl >/dev/null
ns=steam-ft
python_cmd="${PYTHON:-python}"

case "${1:-}" in
  label-nodes)
    [[ $# == 4 && "$2" != "$3" && "$2" != "$4" && "$3" != "$4" ]] || { echo "Supply three different node names: SERVER WORKER1 WORKER2"; exit 2; }
    for node in "$2" "$3" "$4"; do kubectl get node "$node" >/dev/null; done
    kubectl label node "$2" steam.insights/role=storage steam.insights/ft-storage=true steam.insights/ft-hdfs=true --overwrite
    kubectl label node "$3" steam.insights/role=ingest steam.insights/ft-storage=true steam.insights/ft-hdfs=true --overwrite
    kubectl label node "$4" steam.insights/role=compute steam.insights/ft-storage=true steam.insights/compute=true --overwrite
    ;;
  add-compute)
    [[ $# == 2 ]] || { echo "Supply the new worker node name."; exit 2; }
    kubectl wait --for=condition=Ready "node/$2" --timeout=120s
    kubectl label node "$2" steam.insights/compute=true --overwrite
    echo "Import localhost/steam-spark-demo:k3s-20261026 on this worker before benchmarking."
    ;;
  render) kubectl kustomize k8s/fault-test ;;
  deploy)
    # Avoid running both full stacks on the same small VMs.
    demo_pods="$(kubectl -n steam-demo get pods --field-selector='status.phase!=Succeeded,status.phase!=Failed' -o name --ignore-not-found)"
    [[ -z "$demo_pods" ]] || { echo "Pause demo workloads first, keeping its PVCs. See the fault-test guide."; exit 1; }
    count="$(kubectl get nodes -l steam.insights/ft-storage=true -o name | wc -l)"
    [[ "$count" == 3 ]] || { echo "Exactly three ft-storage nodes are required."; exit 1; }
    hdfs_count="$(kubectl get nodes -l steam.insights/ft-hdfs=true -o name | wc -l)"
    [[ "$hdfs_count" == 2 ]] || { echo "Exactly two ft-hdfs nodes are required."; exit 1; }
    kubectl wait --for=condition=Ready nodes -l steam.insights/ft-storage=true --timeout=120s
    kubectl get storageclass local-path >/dev/null
    kubectl apply -k k8s/fault-test
    for app in kafka namenode datanode elasticsearch; do
      kubectl -n "$ns" rollout status "statefulset/$app" --timeout=600s
    done
    kubectl -n "$ns" rollout status deployment/kibana --timeout=600s
    kubectl -n "$ns" exec namenode-0 -- hdfs dfs -mkdir -p /steam/raw /steam/clean /steam/curated /steam/checkpoints /steam/demo /steam/ft-probe
    for topic in steam.reviews.raw steam.players.ccu steam.games.meta steam.alerts; do
      config=(--config retention.ms=86400000 --config min.insync.replicas=2)
      [[ "$topic" != steam.games.meta ]] || config=(--config cleanup.policy=compact --config min.insync.replicas=2)
      kubectl -n "$ns" exec kafka-0 -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
        --create --if-not-exists --topic "$topic" --partitions 3 --replication-factor 3 "${config[@]}"
    done
    bash scripts/k3s-fault-test.sh status
    ;;
  status)
    kubectl -n "$ns" get pods,pvc -o wide
    kubectl -n "$ns" exec kafka-0 -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe
    kubectl -n "$ns" exec kafka-0 -- /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server kafka:9092 describe --status
    kubectl -n "$ns" exec namenode-0 -- hdfs dfsadmin -report
    ;;
  prepare-probe)
    # Input is a dedicated test marker, unrelated to crawled data.
    kubectl -n "$ns" exec namenode-0 -- hdfs dfs -mkdir -p /steam/ft-probe
    printf 'steam-ft-probe\n' | kubectl -n "$ns" exec -i namenode-0 -- hdfs dfs -put -f - /steam/ft-probe/marker.txt
    kubectl -n "$ns" exec namenode-0 -- hdfs dfs -setrep -w 2 /steam/ft-probe/marker.txt
    kubectl -n "$ns" exec namenode-0 -- hdfs fsck /steam/ft-probe/marker.txt -files -blocks -locations
    ;;
  smoke|watch)
    job="ft-$1"
    if kubectl -n "$ns" get job "$job" >/dev/null 2>&1; then echo "Inspect and delete existing Job $job explicitly before rerunning."; exit 1; fi
    kubectl apply -f "k8s/fault-test/jobs/$1.yaml"
    echo "Follow logs: kubectl -n $ns logs -f job/$job"
    ;;
  benchmark)
    executors="${2:-}"; rows="${3:-1000000}"
    [[ "$executors" =~ ^(1|2|4)$ && "$rows" =~ ^[1-9][0-9]*$ ]] || { echo "Usage: benchmark {1|2|4} [ROWS]"; exit 2; }
    kubectl wait --for=condition=Ready nodes -l steam.insights/compute=true --timeout=120s
    compute_count="$(kubectl get nodes -l steam.insights/compute=true -o name | wc -l)"
    if [[ "$executors" == 4 && "$compute_count" -lt 2 ]]; then
      echo "Add a compute worker before requesting four executors on small VMs."; exit 1
    fi
    if [[ -n "$(kubectl -n "$ns" get pods -l 'spark-role in (driver,executor)' --field-selector='status.phase!=Succeeded,status.phase!=Failed' -o name)" ]]; then
      echo "A distributed Spark run is active; run benchmarks sequentially."; exit 1
    fi
    job_file="$(mktemp)"
    trap 'rm -f "$job_file"' EXIT
    "$python_cmd" infra/k3s/render_benchmark.py --executors "$executors" --rows "$rows" > "$job_file"
    kubectl apply -f "$job_file"
    ;;
  *) echo "Usage: bash scripts/k3s-fault-test.sh {label-nodes|add-compute|render|deploy|status|prepare-probe|smoke|watch|benchmark}"; exit 2 ;;
esac

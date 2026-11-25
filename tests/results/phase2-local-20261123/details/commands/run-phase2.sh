#!/usr/bin/env bash
set -euo pipefail

k() { sudo k3s kubectl "$@"; }
kd() { k -n steam-demo "$@"; }

output="$HOME/phase2-evidence"
mkdir -p "$output"
cd "$HOME/phase2-repo"
tag=k3s-20261120-af8377d

wait_job() {
  local name="$1" duration="$2" deadline=$((SECONDS + $2))
  while (( SECONDS < deadline )); do
    status="$(kd get "job/$name" -o 'jsonpath={range .status.conditions[*]}{.type}={.status}{" "}{end}')"
    if [[ "$status" == *Complete=True* ]]; then return 0; fi
    if [[ "$status" == *Failed=True* ]]; then break; fi
    sleep 5
  done
  kd describe "job/$name" >&2 || true
  kd get pods -o wide >&2 || true
  kd logs "job/$name" --all-containers >&2 || true
  return 1
}

capture_kafka() {
  kd exec kafka-0 -- /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --time latest
}

run_app_job() {
  local stage="$1" name="$2"
  shift 2
  if ! kd get "job/$name" -o name >/dev/null 2>&1; then
    python3 "$HOME/phase2-job.py" "$stage" --name "$name" "$@" | kd create -f -
  fi
  if [[ "$stage" == b1 || "$stage" == batch ]]; then
    (
      while true; do
        date -u
        kd get pods -l spark-role=executor -o wide || true
        sleep 6
      done
    ) >> "$output/$name-executors.txt" 2>&1 &
    monitor=$!
  else
    monitor=
  fi
  local rc=0
  wait_job "$name" 3300 || rc=$?
  if [[ -n "$monitor" ]]; then kill "$monitor" 2>/dev/null || true; fi
  kd logs "job/$name" > "$output/$name.log" 2>&1 || true
  tail -15 "$output/$name.log"
  return "$rc"
}

case "${1:-}" in
  smoke)
    sudo env TAG="$tag" timeout 420 bash scripts/k3s-demo.sh smoke 2>&1 | tee "$output/smoke-worker-2.log"
    grep -F 'PASS: integration run' "$output/smoke-worker-2.log"
    sudo env TAG="$tag" timeout 420 bash scripts/k3s-demo.sh smoke-extra steam-worker-3 2>&1 | tee "$output/smoke-worker-3.log"
    grep -F 'PASS: integration run' "$output/smoke-worker-3.log"
    ;;
  batch)
    wait_job phase2-replay-1000 1400
    kd logs job/phase2-replay-1000 > "$output/replay.log"
    capture_kafka | tee "$output/kafka-offsets-snapshot.txt"
    grep -Fx 'steam.reviews.raw:0:932293' "$output/kafka-offsets-snapshot.txt"
    grep -Fx 'steam.players.ccu:0:1700' "$output/kafka-offsets-snapshot.txt"
    grep -Fx 'steam.games.meta:0:4800' "$output/kafka-offsets-snapshot.txt"
    sed 's/spark-jobs/steam-demo/g' k8s/spark/spark-operator-rbac.yaml | k apply -f -
    run_app_job b1 phase2-b1
    kd exec namenode-0 -- hdfs dfs -ls -R /steam/raw | tee "$output/hdfs-raw.txt"
    run_app_job batch phase2-batch
    kd exec namenode-0 -- hdfs dfs -ls /steam/curated | tee "$output/hdfs-curated.txt"
    ;;
  streaming)
    kd create configmap phase2-scripts --from-file="$HOME/phase2-stream.py" --dry-run=client -o yaml | k apply -f -
    offsets="$output/kafka-offsets-snapshot.txt"
    review_start="$(awk -F: '$1=="steam.reviews.raw" && $2==0 {print $3}' "$offsets")"
    ccu_start="$(awk -F: '$1=="steam.players.ccu" && $2==0 {print $3}' "$offsets")"
    [[ "$review_start" =~ ^[0-9]+$ && "$ccu_start" =~ ^[0-9]+$ ]]
    run_app_job inject phase2-inject
    capture_kafka | tee "$output/kafka-offsets-after-inject.txt"
    for stage in s1 s2; do
      run_app_job "$stage" "phase2-$stage" --starting-offsets "{\"steam.reviews.raw\":{\"0\":$review_start}}"
    done
    run_app_job s3 phase2-s3 --starting-offsets "{\"steam.players.ccu\":{\"0\":$ccu_start}}"
    run_app_job kibana-setup phase2-kibana-setup
    ;;
  evidence)
    k get nodes -o wide | tee "$output/nodes.txt"
    k get pods -A -o wide | tee "$output/pods.txt"
    kd get jobs -o wide | tee "$output/jobs.txt"
    python3 - "$output" <<'PY' | tee "$output/es-doc-counts.json"
import json
from pathlib import Path
import subprocess
import sys

def query(path):
    return json.loads(subprocess.check_output([
        'sudo', 'k3s', 'kubectl', '-n', 'steam-demo', 'exec', 'elasticsearch-0', '-c', 'elasticsearch',
        '--', 'curl', '-fsS', 'http://localhost:9200/' + path,
    ]))

indices = query('_cat/indices?format=json&h=index&s=index')
counts = {item['index']: query(item['index'] + '/_count')['count'] for item in indices
          if not item['index'].startswith('.')}
alerts = {name: query('alerts/_count?q=alert_type:' + name)['count'] for name in ('REVIEW_BOMBING', 'CCU_SPIKE')}
if not all(counts.get(name, 0) for name in ('batch_game_daily', 'reviews_search', 'realtime_game_metrics', 'alerts')):
    raise SystemExit(f'Missing expected ES documents: {counts}')
if not all(alerts.values()):
    raise SystemExit(f'Missing expected alert documents: {alerts}')
print(json.dumps({'pipeline_documents': sum(value for name, value in counts.items() if name != 'demo_smoke'),
                  'documents_by_index': counts, 'alerts_by_type': alerts}, ensure_ascii=False, indent=2))
PY
    ;;
  *) echo 'Usage: run-phase2.sh {smoke|batch|streaming|evidence}' >&2; exit 2 ;;
esac

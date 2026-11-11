#!/usr/bin/env bash
# Run from any directory; no automatic .env loading or data deletion.
set -euo pipefail
DEMO_REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$DEMO_REPO_ROOT"
compose=(docker compose --env-file /dev/null -f compose.demo.yaml)

usage() {
  echo "Usage: bash scripts/demo.sh {up|stop} {batch|streaming|dashboard}"
  echo "       bash scripts/demo.sh {config|status|logs|init-kafka|init-hdfs|init-es}"
}

case "${1:-}" in
  up|stop)
    case "${2:-}" in
      batch) services=(namenode datanode) ;;
      streaming) services=(kafka) ;;
      dashboard) services=(elasticsearch kibana) ;;
      *) usage; exit 2 ;;
    esac
    if [[ "$1" == up ]]; then
      "${compose[@]}" --profile "$2" up -d --wait --wait-timeout 300 "${services[@]}"
      case "$2" in
        batch) bash scripts/demo.sh init-hdfs ;;
        streaming) bash scripts/demo.sh init-kafka ;;
        dashboard) echo "Kibana: http://localhost:5601; run init-es in your Python environment." ;;
      esac
    else
      "${compose[@]}" --profile "$2" stop "${services[@]}"
    fi
    ;;
  config) "${compose[@]}" --profile '*' config ;;
  status) "${compose[@]}" --profile '*' ps -a ;;
  logs) "${compose[@]}" --profile '*' logs --tail 80 ;;
  init-kafka)
    for topic in steam.reviews.raw steam.players.ccu steam.games.meta steam.alerts; do
      topic_config=(--config retention.ms=86400000)
      if [[ "$topic" == steam.games.meta ]]; then
        topic_config=(--config cleanup.policy=compact)
      fi
      "${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server kafka:19092 --create --if-not-exists \
        --topic "$topic" --partitions 1 --replication-factor 1 "${topic_config[@]}"
    done
    "${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server kafka:19092 --describe
    ;;
  init-hdfs)
    "${compose[@]}" exec -T namenode hdfs dfs -mkdir -p \
      /steam/raw /steam/clean /steam/curated /steam/checkpoints
    "${compose[@]}" exec -T namenode hdfs dfsadmin -report
    ;;
  init-es)
    # Explicitly disable credentials inherited from the repo's .env for local demo.
    ELASTICSEARCH_HOSTS=http://localhost:9200 ELASTICSEARCH_USER='' ELASTICSEARCH_PASSWORD='' \
      "${PYTHON:-python}" - <<'PY'
from storage.elasticsearch.setup_indices import init_indices
if not init_indices('infra/demo/elasticsearch-indices.json'):
    raise SystemExit('Elasticsearch is not reachable')
PY
    ;;
  *) usage; exit 2 ;;
esac

#!/usr/bin/env bash
# Run from any directory; no automatic .env loading or data deletion.
set -euo pipefail
DEMO_REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$DEMO_REPO_ROOT"
compose=(docker compose --env-file /dev/null -f compose.demo.yaml)

usage() {
  echo "Usage: bash scripts/demo.sh {up|stop} {batch|streaming|dashboard}"
  echo "       bash scripts/demo.sh {config|status|logs|init-kafka|reset-kafka|init-hdfs|init-es}"
  echo "       bash scripts/demo.sh {stream-start|stream-stop|stream-status} [--reset]"
}

# Speed layer S1-S3 chạy nền trên máy host (Kafka -> Spark -> ES). Log và PID ở data/logs/.
STREAM_JOBS=(s1 s2 s3)
LOG_DIR="$DEMO_REPO_ROOT/data/logs"
LAKE="${LAKE:-data/lake}"

stream_env() {
  export SPARK_MASTER="${SPARK_MASTER:-local[2]}" SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-1g}"
  export SPARK_SHUFFLE_PARTITIONS="${SPARK_SHUFFLE_PARTITIONS:-4}" HIVE_METASTORE_URI_LOCAL=''
  export PYSPARK_PYTHON="${PYSPARK_PYTHON:-$(command -v "${PYTHON:-python}")}" PYSPARK_DRIVER_PYTHON="${PYSPARK_DRIVER_PYTHON:-$(command -v "${PYTHON:-python}")}"
  export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
  export SPARK_PACKAGES="${SPARK_PACKAGES:-org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7}"
  export ELASTICSEARCH_HOSTS=http://localhost:9200 ELASTICSEARCH_USER='' ELASTICSEARCH_PASSWORD=''
}

stream_pid() { [[ -f "$LOG_DIR/$1.pid" ]] && kill -0 "$(cat "$LOG_DIR/$1.pid")" 2>/dev/null && cat "$LOG_DIR/$1.pid"; }

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
        dashboard) echo "Kibana: http://localhost:5601/app/dashboards; run init-es in your Python environment." ;;
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
  reset-kafka)
    # Xoá rồi tạo lại 4 topic: bỏ dữ liệu thử tải (SC3) / replay cũ trước buổi demo
    for topic in steam.reviews.raw steam.players.ccu steam.games.meta steam.alerts; do
      "${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server kafka:19092 --delete --if-exists --topic "$topic"
    done
    sleep 5
    bash scripts/demo.sh init-kafka
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
  stream-start)
    stream_env
    mkdir -p "$LOG_DIR"
    if [[ "${2:-}" == --reset ]]; then
      # Đọc lại từ đầu những gì đang có trong Kafka (checkpoint mới) và xoá view realtime/cảnh báo cũ
      rm -rf "$LAKE/checkpoints/s1_metrics" "$LAKE/checkpoints/s2_bombing" "$LAKE/checkpoints/s3_ccu"
      for index in realtime_game_metrics alerts; do
        curl -fsS -X POST "localhost:9200/$index/_delete_by_query?refresh=true" -H 'Content-Type: application/json' \
          -d '{"query":{"match_all":{}}}' >/dev/null
      done
    fi
    for job in "${STREAM_JOBS[@]}"; do
      if pid="$(stream_pid "$job")"; then echo "$job already running (pid $pid)"; continue; fi
      nohup "${PYTHON:-python}" pipeline.py --profile local "$job" --lake "$LAKE" --es -- \
        --source kafka --brokers localhost:9092 --starting-offsets earliest --trigger "20 seconds" \
        >"$LOG_DIR/$job.log" 2>&1 &
      echo $! >"$LOG_DIR/$job.pid"
      echo "$job started (pid $!), log $LOG_DIR/$job.log"
      sleep 15  # tránh ba JVM khởi động cùng lúc trên máy ít RAM
    done
    ;;
  stream-stop)
    for job in "${STREAM_JOBS[@]}"; do
      if pid="$(stream_pid "$job")"; then
        pkill -TERM -P "$pid" 2>/dev/null || true
        kill "$pid" 2>/dev/null || true
        echo "$job stopped"
      fi
      rm -f "$LOG_DIR/$job.pid"
    done
    ;;
  stream-status)
    for job in "${STREAM_JOBS[@]}"; do
      if pid="$(stream_pid "$job")"; then echo "$job running (pid $pid)"; else echo "$job stopped"; fi
    done
    curl -fsS 'localhost:9200/_cat/indices/realtime_game_metrics,alerts?h=index,docs.count' || true
    ;;
  *) usage; exit 2 ;;
esac

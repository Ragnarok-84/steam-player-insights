#!/usr/bin/env bash
# Executor launch contract for Spark 3.5.7's KubernetesExecutorBackend.
set -euo pipefail
if [[ "${1:-}" != executor ]]; then exec "$@"; fi
java_opts=()
while IFS= read -r option_name; do
  java_opts+=("${!option_name}")
done < <(compgen -e | grep '^SPARK_JAVA_OPT_[0-9]\+$' | sort -V)
classpath="${SPARK_HOME:?}/jars/*:${SPARK_HOME}/conf:$PWD"
if [[ -n "${SPARK_CONF_DIR:-}" ]]; then classpath="$SPARK_CONF_DIR:$classpath"; fi
if [[ -n "${HADOOP_CONF_DIR:-}" ]]; then classpath="$HADOOP_CONF_DIR:$classpath"; fi
classpath="$classpath:${SPARK_EXTRA_CLASSPATH:-}:${SPARK_DIST_CLASSPATH:-}"
exec /usr/bin/tini -s -- java "${java_opts[@]}" \
  "-Xms${SPARK_EXECUTOR_MEMORY:?}" "-Xmx${SPARK_EXECUTOR_MEMORY:?}" \
  -cp "$classpath" org.apache.spark.scheduler.cluster.k8s.KubernetesExecutorBackend \
  --driver-url "${SPARK_DRIVER_URL:?}" --executor-id "${SPARK_EXECUTOR_ID:?}" \
  --cores "${SPARK_EXECUTOR_CORES:?}" --app-id "${SPARK_APPLICATION_ID:?}" \
  --hostname "${SPARK_EXECUTOR_POD_IP:?}" \
  --resourceProfileId "${SPARK_RESOURCE_PROFILE_ID:-0}" --podName "${SPARK_EXECUTOR_POD_NAME:?}"

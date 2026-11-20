#!/usr/bin/env bash
# Source from the repo root so every build and manifest uses the same image references.
if [[ -z "${TAG:-}" ]]; then
  TAG="k3s-$(git show -s --format=%cd --date=format:%Y%m%d HEAD)-$(git rev-parse --short HEAD)"
fi
REGISTRY="${REGISTRY:-localhost}"
REGISTRY="${REGISTRY%/}"
if [[ ! "$TAG" =~ ^[a-zA-Z0-9_][a-zA-Z0-9_.-]{0,127}$ || ! "$REGISTRY" =~ ^[a-zA-Z0-9][a-zA-Z0-9_./:-]*$ ]]; then
  echo "Invalid TAG or REGISTRY for the Steam images." >&2
  return 2
fi
SPARK_DEMO_IMAGE="$REGISTRY/steam-spark-demo:$TAG"
SPARK_IMAGE="$REGISTRY/steam-spark:$TAG"
CRAWLER_IMAGE="$REGISTRY/steam-crawler:$TAG"
DEMO_PULL_POLICY=IfNotPresent
if [[ "$REGISTRY" == localhost ]]; then DEMO_PULL_POLICY=Never; fi

render_steam_images() {
  sed -e "s#localhost/steam-spark-demo:local#$SPARK_DEMO_IMAGE#g" \
      -e "s#steam-spark:local#$SPARK_IMAGE#g" \
      -e "s#steam-crawler:local#$CRAWLER_IMAGE#g" "$@"
}

render_demo_job() {
  render_steam_images "$1" | sed "s/imagePullPolicy: Never/imagePullPolicy: $DEMO_PULL_POLICY/"
}

seed_crawler_targets() {
  local namespace="$1" manifest job
  manifest="$(render_steam_images k8s/crawlers/jobs/seed-targets.yaml)"
  if [[ "$namespace" == steam-demo ]]; then
    manifest="$(printf '%s\n' "$manifest" | kubectl patch --local -f - --type=merge -o yaml \
      -p '{"spec":{"template":{"spec":{"nodeSelector":{"steam.insights/role":"ingest"}}}}}')"
  fi
  job="$(printf '%s\n' "$manifest" | kubectl -n "$namespace" create -f - -o name)"
  if ! kubectl -n "$namespace" wait --for=condition=complete "$job" --timeout=330s; then
    kubectl -n "$namespace" describe "$job"
    kubectl -n "$namespace" logs "$job" || true
    return 1
  fi
  kubectl -n "$namespace" logs "$job"
}

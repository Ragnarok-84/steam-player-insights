#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."
source scripts/k3s-images.sh

builder="${CONTAINER_BUILDER:-podman}"
if [[ "$builder" != podman && "$builder" != docker ]]; then
  echo "CONTAINER_BUILDER must be podman or docker." >&2
  exit 2
fi
command -v "$builder" >/dev/null
out="${1:-data/bench/k3s-images}"
mkdir -p -- "$out"
revision="$(git rev-parse HEAD)"
if [[ -n "$(git status --porcelain --untracked-files=normal -- containers crawlers processing storage dashboards tests data infra pipeline.py requirements*.txt)" ]]; then
  echo "Build context includes local changes. Commit the image sources before publishing a release." >&2
fi

build() {
  local image="$1" dockerfile="$2"
  "$builder" build --label "org.opencontainers.image.revision=$revision" -f "$dockerfile" -t "$image" .
  "$builder" image inspect --format '{{.Id}}' "$image"
}

build "$SPARK_DEMO_IMAGE" infra/k3s/Dockerfile.spark-demo
build "$SPARK_IMAGE" containers/Dockerfile.spark
build "$CRAWLER_IMAGE" containers/Dockerfile.crawler

if [[ "$REGISTRY" != localhost ]]; then
  for image in "$SPARK_DEMO_IMAGE" "$SPARK_IMAGE" "$CRAWLER_IMAGE"; do "$builder" push "$image"; done
else
  save_options=()
  if [[ "$builder" == podman ]]; then save_options=(--format docker-archive --multi-image-archive); fi
  "$builder" save "${save_options[@]}" -o "$out/steam-k3s-$TAG.tar" "$SPARK_DEMO_IMAGE" "$SPARK_IMAGE" "$CRAWLER_IMAGE"
  echo "Import $out/steam-k3s-$TAG.tar on each target k3s node with sudo k3s ctr images import."
fi
printf 'TAG=%s\nSPARK_DEMO_IMAGE=%s\nSPARK_IMAGE=%s\nCRAWLER_IMAGE=%s\n' "$TAG" "$SPARK_DEMO_IMAGE" "$SPARK_IMAGE" "$CRAWLER_IMAGE"

#!/usr/bin/env bash
# Environment (optional):
#   IMAGE_GROUP          - quarkus.container-image.group (default: local)
#   IMAGE_TAG            - quarkus.container-image.tag (default: root project version from Gradle)
#   IMAGE_REGISTRY       - quarkus.container-image.registry (empty = local tag only)
#   QUARKUS_JIB_PLATFORMS - e.g. linux/arm64/v8 or linux/amd64,linux/arm64/v8
#   QUARKUS_JIB_BASE_JVM_IMAGE - override base image, e.g. eclipse-temurin:21-jre-jammy
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

: "${IMAGE_GROUP:=local}"
: "${IMAGE_REGISTRY:=}"
: "${IMAGE_TAG:=}"

if [[ -z "${IMAGE_TAG}" ]]; then
  IMAGE_TAG="$(./gradlew -q properties --console=plain 2>/dev/null | awk -F': ' '$1=="version"{gsub(/^ +| +$/,"",$2); print $2; exit}')"
fi
if [[ -z "${IMAGE_TAG}" ]]; then
  IMAGE_TAG="0.1.0"
fi

#MODULES=(avro orc parquet iceberg paimon)
MODULES=(avro)

GRADLE_ARGS=(
  -Dquarkus.container-image.build=true
  -Dquarkus.container-image.push=false
  -Dquarkus.container-image.group="${IMAGE_GROUP}"
  -Dquarkus.container-image.tag="${IMAGE_TAG}"
)

if [[ -n "${IMAGE_REGISTRY}" ]]; then
  GRADLE_ARGS+=(-Dquarkus.container-image.registry="${IMAGE_REGISTRY}")
fi

if [[ -n "${QUARKUS_JIB_PLATFORMS:-}" ]]; then
  GRADLE_ARGS+=(-Dquarkus.jib.platforms="${QUARKUS_JIB_PLATFORMS}")
fi

if [[ -n "${QUARKUS_JIB_BASE_JVM_IMAGE:-}" ]]; then
  GRADLE_ARGS+=(-Dquarkus.jib.base-jvm-image="${QUARKUS_JIB_BASE_JVM_IMAGE}")
fi

for m in "${MODULES[@]}"; do
  echo "==> Building container image :modules:${m} with args ${GRADLE_ARGS[@]}"
  ./gradlew ":modules:${m}:build" -x test "${GRADLE_ARGS[@]}"
done

echo "Done. Images use names from each module's quarkus.container-image.name under group '${IMAGE_GROUP}' and tag '${IMAGE_TAG}'."

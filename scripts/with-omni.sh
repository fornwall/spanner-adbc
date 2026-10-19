#!/usr/bin/env bash
# Run a command against a disposable Spanner Omni deployment.
# Usage: scripts/with-omni.sh cargo test --test integration --test ffi_lifecycle
set -euo pipefail

# Latest published release: https://docs.cloud.google.com/spanner-omni/download
IMAGE="${SPANNER_OMNI_IMAGE:-us-docker.pkg.dev/spanner-omni/images/spanner-omni:2026.r4-lts}"
CONTAINER="${SPANNER_OMNI_CONTAINER:-spanner-adbc-omni}"

if [ "$#" -eq 0 ]; then
  echo "usage: $0 <command> [args...]" >&2
  exit 2
fi
for dependency in docker timeout; do
  command -v "$dependency" >/dev/null 2>&1 || {
    echo "required command not found: $dependency" >&2
    exit 1
  }
done

# Public images need no registry credentials; bypass broken credential helpers.
# The replacement config has no contexts, so keep the active context's endpoint.
if [ -z "${DOCKER_HOST:-}" ]; then
  DOCKER_HOST="$(docker context inspect --format '{{.Endpoints.docker.Host}}' 2>/dev/null || true)"
  [ -n "$DOCKER_HOST" ] && export DOCKER_HOST
fi
unset DOCKER_CONTEXT
DOCKER_CONFIG="$(mktemp -d)"
export DOCKER_CONFIG
echo '{}' > "$DOCKER_CONFIG/config.json"

cleanup() {
  if [ -s "$DOCKER_CONFIG/omni.cid" ]; then
    # Include the anonymous /spanner volume created by the image.
    docker rm -fv "$(cat "$DOCKER_CONFIG/omni.cid")" >/dev/null 2>&1 || true
  fi
  rm -rf "$DOCKER_CONFIG"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

echo ">> starting Spanner Omni ($IMAGE) as '$CONTAINER'"
docker run -d --pull always --name "$CONTAINER" --cidfile "$DOCKER_CONFIG/omni.cid" \
  -p 127.0.0.1:15000-15025:15000-15025 \
  "$IMAGE" start-single-server --listen-addresses=0.0.0.0 >/dev/null
container_id="$(cat "$DOCKER_CONFIG/omni.cid")"

# Omni has no REST API. Probe its gRPC admin API with the bundled CLI.
echo ">> waiting for the Omni admin API"
ready=0
for ((deadline=SECONDS+120; SECONDS<deadline; )); do
  if timeout 5 docker exec "$container_id" /google/spanner/bin/spanner \
      databases list --timeout=3s >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done
if [ "$ready" -ne 1 ]; then
  echo "Omni admin API not serving within 120s" >&2
  docker logs "$container_id" >&2 2>&1 || true
  exit 1
fi

timeout 65 docker exec "$container_id" /google/spanner/bin/spanner \
  databases create adbc-test --timeout=60s
unset SPANNER_EMULATOR_HOST SPANNER_GCP_DATABASE
export SPANNER_OMNI_ENDPOINT=http://localhost:15000
echo ">> SPANNER_OMNI_ENDPOINT=$SPANNER_OMNI_ENDPOINT"
echo ">> running: $*"
"$@"

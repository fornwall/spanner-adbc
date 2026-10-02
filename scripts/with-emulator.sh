#!/usr/bin/env bash
# Run a command against a disposable Cloud Spanner emulator.
# Usage: scripts/with-emulator.sh cargo test
set -euo pipefail

IMAGE="${SPANNER_EMULATOR_IMAGE:-gcr.io/cloud-spanner-emulator/emulator}"
CONTAINER="${SPANNER_EMULATOR_CONTAINER:-spanner-adbc-emulator}"
GRPC_PORT="${SPANNER_EMULATOR_GRPC_PORT:-9010}"
REST_PORT="${SPANNER_EMULATOR_REST_PORT:-9020}"

if [ "$#" -eq 0 ]; then
  echo "usage: $0 <command> [args...]" >&2
  exit 2
fi
for dependency in docker curl; do
  command -v "$dependency" >/dev/null 2>&1 || {
    echo "required command not found: $dependency" >&2
    exit 1
  }
done

# Public images need no registry credentials; bypass broken credential helpers.
DOCKER_CONFIG="$(mktemp -d)"
export DOCKER_CONFIG
echo '{}' > "$DOCKER_CONFIG/config.json"

cleanup() {
  # Docker writes the ID after creation, even if starting the container fails.
  # A name collision creates no ID, so it cannot remove someone else's container.
  if [ -s "$DOCKER_CONFIG/emulator.cid" ]; then
    docker rm -f "$(cat "$DOCKER_CONFIG/emulator.cid")" >/dev/null 2>&1 || true
  fi
  rm -rf "$DOCKER_CONFIG"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

echo ">> starting Spanner emulator ($IMAGE) as '$CONTAINER'"
docker run -d --pull always --name "$CONTAINER" --cidfile "$DOCKER_CONFIG/emulator.cid" \
  -p "127.0.0.1:${GRPC_PORT}:9010" -p "127.0.0.1:${REST_PORT}:9020" \
  "$IMAGE" >/dev/null

# TCP-open precedes admin readiness; wait for a successful API response.
echo ">> waiting for the emulator admin API (REST port ${REST_PORT})"
ready=0
for ((deadline=SECONDS+60; SECONDS<deadline; )); do
  if [ "$(curl --noproxy '*' --max-time 1 -s -o /dev/null -w '%{http_code}' \
      "http://127.0.0.1:${REST_PORT}/v1/projects/emulator/instanceConfigs" 2>/dev/null)" = "200" ]; then
    ready=1
    break
  fi
  sleep 0.5
done
if [ "$ready" -ne 1 ]; then
  echo "emulator admin API (REST port ${REST_PORT}) not serving within 60s" >&2
  docker logs "$(cat "$DOCKER_CONFIG/emulator.cid")" >&2 2>&1 || true
  exit 1
fi

export SPANNER_EMULATOR_HOST="localhost:${GRPC_PORT}"
echo ">> SPANNER_EMULATOR_HOST=${SPANNER_EMULATOR_HOST}"
echo ">> running: $*"
"$@"

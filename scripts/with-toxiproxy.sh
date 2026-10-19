#!/usr/bin/env bash
# Run transport-fault tests through Toxiproxy; schema setup bypasses the proxy.
# The emulator uses its bridge IP to preserve :9010 -> :9020 admin-port mapping.
# Requires a Docker host reachable by bridge IP (normally Linux).
# Usage: scripts/with-toxiproxy.sh cargo test --test resilience -- --test-threads=1
set -euo pipefail

EMULATOR_IMAGE="${SPANNER_EMULATOR_IMAGE:-gcr.io/cloud-spanner-emulator/emulator}"
EMULATOR_CONTAINER="${SPANNER_EMULATOR_CONTAINER:-adbc-emu-fault}"
TOXIPROXY_IMAGE="${TOXIPROXY_IMAGE:-ghcr.io/shopify/toxiproxy}"
TOXIPROXY_CONTAINER="${TOXIPROXY_CONTAINER:-adbc-toxiproxy-fault}"
TOXIPROXY_LISTEN_PORT="${TOXIPROXY_LISTEN_PORT:-8666}"
TOXIPROXY_ADMIN_PORT="${TOXIPROXY_ADMIN_PORT:-8475}"
TOXIPROXY_PROXY="${TOXIPROXY_PROXY:-spanner}"

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
  # Remove only containers created by this invocation, including failed starts.
  for cidfile in "$DOCKER_CONFIG/toxiproxy.cid" "$DOCKER_CONFIG/emulator.cid"; do
    if [ -s "$cidfile" ]; then
      docker rm -f "$(cat "$cidfile")" >/dev/null 2>&1 || true
    fi
  done
  rm -rf "$DOCKER_CONFIG"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

wait_for_api() {
  local url="$1" cidfile="$2" deadline
  for ((deadline=SECONDS+60; SECONDS<deadline; )); do
    if [ "$(curl --noproxy '*' --max-time 1 -s -o /dev/null -w '%{http_code}' "$url" 2>/dev/null)" = "200" ]; then
      return
    fi
    sleep 0.5
  done
  echo "API did not become ready within 60s: $url" >&2
  docker logs "$(cat "$cidfile")" >&2 || true
  exit 1
}

echo ">> starting Spanner emulator ($EMULATOR_IMAGE) as '$EMULATOR_CONTAINER'"
docker run -d --pull always --name "$EMULATOR_CONTAINER" \
  --cidfile "$DOCKER_CONFIG/emulator.cid" "$EMULATOR_IMAGE" >/dev/null

EMULATOR_IP="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' \
  "$(cat "$DOCKER_CONFIG/emulator.cid")")"
if [ -z "$EMULATOR_IP" ]; then
  echo "could not determine the emulator container IP" >&2
  docker logs "$(cat "$DOCKER_CONFIG/emulator.cid")" >&2 || true
  exit 1
fi
echo ">> waiting for emulator admin API (http://${EMULATOR_IP}:9020)"
wait_for_api "http://${EMULATOR_IP}:9020/v1/projects/emulator/instanceConfigs" "$DOCKER_CONFIG/emulator.cid"

echo ">> starting Toxiproxy ($TOXIPROXY_IMAGE) as '$TOXIPROXY_CONTAINER'"
docker run -d --name "$TOXIPROXY_CONTAINER" \
  -p "127.0.0.1:${TOXIPROXY_ADMIN_PORT}:${TOXIPROXY_ADMIN_PORT}" \
  -p "127.0.0.1:${TOXIPROXY_LISTEN_PORT}:${TOXIPROXY_LISTEN_PORT}" \
  --cidfile "$DOCKER_CONFIG/toxiproxy.cid" "$TOXIPROXY_IMAGE" \
  -host 0.0.0.0 -port "${TOXIPROXY_ADMIN_PORT}" >/dev/null

TOXIPROXY_URL="http://127.0.0.1:${TOXIPROXY_ADMIN_PORT}"
echo ">> waiting for Toxiproxy admin API (${TOXIPROXY_URL})"
wait_for_api "${TOXIPROXY_URL}/version" "$DOCKER_CONFIG/toxiproxy.cid"

echo ">> creating proxy '${TOXIPROXY_PROXY}': 127.0.0.1:${TOXIPROXY_LISTEN_PORT} -> ${EMULATOR_IP}:9010"
create_code="$(curl --noproxy '*' --max-time 10 -s -o /dev/null -w '%{http_code}' \
  -X POST "${TOXIPROXY_URL}/proxies" -H 'Content-Type: application/json' \
  -d "{\"name\":\"${TOXIPROXY_PROXY}\",\"listen\":\"0.0.0.0:${TOXIPROXY_LISTEN_PORT}\",\"upstream\":\"${EMULATOR_IP}:9010\",\"enabled\":true}")"
if [ "$create_code" != "201" ] && [ "$create_code" != "200" ]; then
  echo "failed to create Toxiproxy proxy (HTTP ${create_code})" >&2
  docker logs "$(cat "$DOCKER_CONFIG/toxiproxy.cid")" >&2 || true
  exit 1
fi

unset SPANNER_OMNI_ENDPOINT SPANNER_GCP_DATABASE
export SPANNER_EMULATOR_HOST="127.0.0.1:${TOXIPROXY_LISTEN_PORT}"
export SPANNER_EMULATOR_DIRECT="${EMULATOR_IP}:9010"
export TOXIPROXY_URL TOXIPROXY_PROXY
echo ">> SPANNER_EMULATOR_HOST=${SPANNER_EMULATOR_HOST} (via Toxiproxy)"
echo ">> SPANNER_EMULATOR_DIRECT=${SPANNER_EMULATOR_DIRECT} (schema setup)"
echo ">> TOXIPROXY_URL=${TOXIPROXY_URL} TOXIPROXY_PROXY=${TOXIPROXY_PROXY}"
echo ">> running: $*"
"$@"

#!/usr/bin/env bash
# Shared Docker setup and owned-container cleanup for the backend wrappers.

setup_backend_docker() {
  backend_containers=("$@")

  # Public images need no registry credentials; bypass broken credential helpers.
  # The replacement config has no contexts, so keep the active context's endpoint.
  if [ -z "${DOCKER_HOST:-}" ]; then
    DOCKER_HOST="$(docker context inspect --format '{{.Endpoints.docker.Host}}' 2>/dev/null || true)"
    [ -n "$DOCKER_HOST" ] && export DOCKER_HOST
  fi
  unset DOCKER_CONTEXT
  DOCKER_CONFIG="$(mktemp -d)"
  export DOCKER_CONFIG
  trap cleanup_backend_docker EXIT
  trap 'exit 130' INT
  trap 'exit 143' TERM
  echo '{}' > "$DOCKER_CONFIG/config.json"
}

cleanup_backend_docker() {
  local i cidfile
  for ((i=${#backend_containers[@]}-1; i>=0; i--)); do
    cidfile="$DOCKER_CONFIG/${backend_containers[$i]}.cid"
    # Failed starts can leave containers; name collisions never write a cidfile.
    if [ -s "$cidfile" ]; then
      # Omni's image creates an anonymous /spanner volume.
      docker rm -fv "$(cat "$cidfile")" >/dev/null 2>&1 || true
    fi
  done
  rm -rf "$DOCKER_CONFIG"
}

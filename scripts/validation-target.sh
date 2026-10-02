#!/usr/bin/env bash
# Source from either validation runner to configure ADBC_SPANNER_URI.

if [ -n "${SPANNER_EMULATOR_HOST:-}" ]; then
  ADBC_SPANNER_URI="spanner:///projects/test-project/instances/test-instance/databases/adbc-test"
  rest="http://${SPANNER_EMULATOR_HOST%:*}:${SPANNER_EMULATOR_REST_PORT:-9020}"
  echo ">> creating emulator instance/database via $rest"

  resources=(
    "projects/test-project/instances"
    "projects/test-project/instances/test-instance/databases"
  )
  # shellcheck disable=SC2016  # Backticks quote a SQL identifier inside JSON.
  bodies=(
    '{"instanceId":"test-instance","instance":{"config":"projects/test-project/instanceConfigs/emulator-config","displayName":"adbc","nodeCount":1}}'
    '{"createStatement":"CREATE DATABASE `adbc-test`"}'
  )
  for i in 0 1; do
    url="$rest/v1/${resources[$i]}"
    if ! response="$(curl --noproxy '*' --max-time 10 -sS -w $'\n%{http_code}' \
        -X POST "$url" -H 'Content-Type: application/json' -d "${bodies[$i]}")"; then
      echo "failed to reach emulator admin API: $url" >&2
      exit 1
    fi
    code="${response##*$'\n'}"
    case "$code" in
      200|201|409) ;; # Already-existing resources are safe to reuse.
      *)
        echo "emulator setup failed at $url (HTTP $code): ${response%$'\n'*}" >&2
        exit 1
        ;;
    esac
  done

  # GET the exact resource: a database with a similar name is not sufficient.
  ready=0
  for ((deadline=SECONDS+10; SECONDS<deadline; )); do
    if [ "$(curl --noproxy '*' --max-time 1 -s -o /dev/null -w '%{http_code}' \
        "$rest/v1/${resources[1]}/adbc-test" 2>/dev/null)" = "200" ]; then
      ready=1
      break
    fi
    sleep 0.25
  done
  if [ "$ready" -ne 1 ]; then
    echo "emulator database adbc-test did not become ready at $rest within 10s" >&2
    exit 1
  fi
else
  if [[ ! "${SPANNER_GCP_DATABASE:-}" =~ ^[^./[:space:]]+\.[^./[:space:]]+\.[^./[:space:]]+$ ]]; then
    echo "SPANNER_GCP_DATABASE must be project.instance.database; got '${SPANNER_GCP_DATABASE:-}'" >&2
    exit 2
  fi
  IFS='.' read -r project instance database <<<"$SPANNER_GCP_DATABASE"
  ADBC_SPANNER_URI="spanner:///projects/$project/instances/$instance/databases/$database"
fi
export ADBC_SPANNER_URI

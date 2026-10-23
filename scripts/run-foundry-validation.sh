#!/usr/bin/env bash
#
# Run the ADBC Driver Foundry validation suite (https://github.com/adbc-drivers/validation)
# against the spanner-adbc driver. See foundry-validation/README.md for coverage and skips.
#
# With no target configured, start an emulator and create a test instance/database.
#
#   scripts/run-foundry-validation.sh                      # emulator
#   SPANNER_EMULATOR_HOST=localhost:9010 scripts/run-foundry-validation.sh
#   scripts/run-foundry-validation.sh -k connection        # pass extra pytest args
#   scripts/run-foundry-validation.sh --prepare           # build/install without a server
#
# GoogleSQL overrides live in foundry-validation/queries/spanner/.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON="${PYTHON:-python3}"
# Anchor a relative interpreter path before changing directories; keep venv symlinks.
case "$PYTHON" in
  */*) PYTHON="$(cd "$(dirname "$PYTHON")" && pwd)/$(basename "$PYTHON")" ;;
esac
cd "$REPO_ROOT"

# Pin the upstream query corpus and test behavior for reproducible runs.
VALIDATION_REF="${ADBC_VALIDATION_REF:-9f9f4417a08d6c082193842bc5caaa36e65908a0}"
VALIDATION_REPO="${ADBC_VALIDATION_REPO:-adbc-drivers/validation}"

echo ">> building the spanner-adbc cdylib"
cargo build

# pip freeze includes the resolved git commit from direct_url metadata.
# Tolerate a missing pip here; the install below reports it.
installed_ref="$("$PYTHON" -m pip freeze 2>/dev/null \
  | sed -n 's/^adbc[-_]drivers[-_]validation @ git+.*@//p' | head -n 1 || true)"
if [ "$installed_ref" != "$VALIDATION_REF" ]; then
  echo ">> installing the validation suite at pinned $VALIDATION_REF (installed: ${installed_ref:-none})"
  "$PYTHON" -m pip install \
    "adbc_drivers_validation @ git+https://github.com/${VALIDATION_REPO}@${VALIDATION_REF}" \
    pyarrow pytest
fi

if [ "${1:-}" = --prepare ]; then
  exit 0
fi

# Start the server after builds and dependency installation to keep memory available.
if [ -z "${SPANNER_EMULATOR_HOST:-}" ] && [ -z "${SPANNER_OMNI_ENDPOINT:-}" ] && [ -z "${SPANNER_GCP_DATABASE:-}" ]; then
  exec "$REPO_ROOT/scripts/with-emulator.sh" "$REPO_ROOT/scripts/run-foundry-validation.sh" "$@"
fi

source "$REPO_ROOT/scripts/validation-target.sh"

echo ">> ADBC_SPANNER_URI=$ADBC_SPANNER_URI"
echo ">> running the Foundry validation suite"
cd "$REPO_ROOT/foundry-validation"
exec "$PYTHON" -m pytest -p no:cacheprovider "$@"

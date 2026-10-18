#!/usr/bin/env bash
#
# Run the ADBC Driver Foundry validation suite (https://github.com/adbc-drivers/validation)
# against the spanner-adbc driver. This is a *type/feature coverage* harness — complementary to
# scripts/run-adbc-validation.sh, which runs Apache arrow-adbc's C++ C-ABI conformance suite.
#
# With no Spanner target configured it starts a throwaway emulator (via scripts/with-emulator.sh),
# creates the test instance/database itself, then runs the suite:
#
#   scripts/run-foundry-validation.sh                      # emulator
#   SPANNER_EMULATOR_HOST=localhost:9010 scripts/run-foundry-validation.sh
#   scripts/run-foundry-validation.sh -k connection        # pass extra pytest args
#   scripts/run-foundry-validation.sh --prepare           # build/install without a server
#
# The suite's base query corpus assumes a portable SQL dialect (no mandatory PRIMARY KEY,
# INTEGER/BIGINT type names, positional $1 parameters); the Spanner-dialect overrides under
# foundry-validation/queries/spanner/ cover the corpus, so this run GATES CI
# (foundry-validation.yml). Every case passes or skips with a reason — no expected failures;
# see foundry-validation/README.md.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# Pin the upstream query corpus and test behavior for reproducible runs.
VALIDATION_REF="${ADBC_VALIDATION_REF:-68242ee13524e8f2b36038764e2f439d4cb49f33}"
VALIDATION_REPO="${ADBC_VALIDATION_REPO:-adbc-drivers/validation}"
PYTHON="${PYTHON:-python3}"

echo ">> building the spanner-adbc cdylib"
cargo build

# pip freeze includes the resolved git commit from direct_url metadata.
installed_ref="$("$PYTHON" -m pip freeze 2>/dev/null \
  | sed -n 's/^adbc[-_]drivers[-_]validation @ git+.*@//p' | head -n 1)"
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
  exec "$REPO_ROOT/scripts/with-emulator.sh" "$0" "$@"
fi

source "$REPO_ROOT/scripts/validation-target.sh"

echo ">> ADBC_SPANNER_URI=$ADBC_SPANNER_URI"
echo ">> running the Foundry validation suite"
cd "$REPO_ROOT/foundry-validation"
exec "$PYTHON" -m pytest -p no:cacheprovider "$@"

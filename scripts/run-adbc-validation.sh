#!/usr/bin/env bash
#
# Build the driver and the Arrow ADBC C++ validation suite (adbc-validation/),
# then run the suite against the driver through the ADBC driver manager.
# Without a configured target, rerun under scripts/with-emulator.sh.
#
#   scripts/run-adbc-validation.sh                # gate and guards (CI)
#   scripts/run-adbc-validation.sh --full         # every case, isolated; expect failures
#   scripts/run-adbc-validation.sh --check-drift  # build and stale guard only, no database
#   SPANNER_EMULATOR_HOST=localhost:9010 scripts/run-adbc-validation.sh
#   SPANNER_GCP_DATABASE=proj.inst.db scripts/run-adbc-validation.sh
#
# Sanitizers:
#   ADBC_VALIDATION_SANITIZE=address,undefined    # C++ harness and libraries
#   ADBC_VALIDATION_SANITIZE=address ADBC_VALIDATION_RUST_SANITIZE=address
#                                                 # also the cdylib (nightly, clang)
#
# The default run checks the EXCLUDED list below (see adbc-validation/README.md):
#   gate: every other case passes or skips, including new upstream cases;
#   expected-failure guard: no excluded case passes;
#   stale guard: every excluded case still exists upstream.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

SANITIZE="${ADBC_VALIDATION_SANITIZE:-}"
RUST_SANITIZE="${ADBC_VALIDATION_RUST_SANITIZE:-}"

# -Zbuild-std needs an explicit --target, placing the cdylib in target/<triple>/debug/.
RUST_TARGET="$(rustc -vV | sed -n 's/host: //p')"

# Rust ASan uses compiler-rt; GCC's libasan in the same process aborts at startup.
if [ -n "$RUST_SANITIZE" ]; then
  export CC="${CC:-clang}"
  export CXX="${CXX:-clang++}"
fi

# Separate build trees keep sanitizer modes and compilers from sharing a CMake cache.
build_suffix=""
[ -n "$SANITIZE" ] && build_suffix="-san"
[ -n "$RUST_SANITIZE" ] && build_suffix="-rustsan"
BUILD_DIR="${ADBC_VALIDATION_BUILD_DIR:-$REPO_ROOT/.adbc-validation-build$build_suffix}"

# Disable leak checks for the driver's process-lifetime runtime and connection pools.
# Keep ASan memory errors and UBSan reports fatal, including during gtest discovery.
if [ -n "$SANITIZE" ] || [ -n "$RUST_SANITIZE" ]; then
  export ASAN_OPTIONS="detect_leaks=0:abort_on_error=1:${ASAN_OPTIONS:-}"
  export UBSAN_OPTIONS="print_stacktrace=1:halt_on_error=1:${UBSAN_OPTIONS:-}"
fi

FULL=0
CHECK_DRIFT_ONLY=0
for arg in "$@"; do
  case "$arg" in
    --full) FULL=1 ;;
    --check-drift) CHECK_DRIFT_ONLY=1 ;;
    *)
      echo "!! unknown argument: $arg" >&2
      echo "usage: $0 [--full | --check-drift]" >&2
      exit 2
      ;;
  esac
done

# Prefer adapting dialect-specific SQL in SpannerQuirks::RewriteSql to excluding a case.
EXCLUDED=(
  # Duration and Interval have no supported column mapping; the emulator also
  # rejects INTERVAL columns. UInt64 cannot fit losslessly in INT64, and the
  # suite cannot express a NUMERIC/Decimal128 round-trip for this case.
  'SpannerStatementTest.SqlIngestDuration'
  'SpannerStatementTest.SqlIngestInterval'
  'SpannerStatementTest.SqlIngestUInt64'
)

# A leading '-' excludes these cases from gtest; without it, only these cases run.
EXCLUDED_FILTER="$(IFS=:; printf '%s' "${EXCLUDED[*]}")"

# The test-only canary writes past a C++-allocated buffer from Rust; ASan must report it.
run_rust_asan_canary() {
  local lib="$REPO_ROOT/target/$RUST_TARGET/debug/libspanner_adbc.so"
  local src="$REPO_ROOT/adbc-validation/asan_canary.cc"
  local bin="$BUILD_DIR/asan_canary"
  mkdir -p "$BUILD_DIR"

  echo ">> ASan canary: compiling the cross-boundary tripwire (${CXX:-clang++} -fsanitize=address)"
  # Match the CMake harness's warning flags.
  "${CXX:-clang++}" -std=c++17 -g -O0 -fsanitize=address -fno-omit-frame-pointer \
    -Wall -Wextra -Wpedantic -Wshadow -Wconversion -Wsign-conversion -Werror \
    "$src" -o "$bin" -ldl

  echo ">> ASan canary: running it against $lib (expecting a heap-buffer-overflow)"
  # Capture the expected abort so set -e does not stop the report check.
  local out rc=0
  out="$("$bin" "$lib" 2>&1)" || rc=$?

  if [ "$rc" -ne 0 ] \
      && printf '%s' "$out" | grep -q 'AddressSanitizer' \
      && printf '%s' "$out" | grep -q 'heap-buffer-overflow'; then
    echo ">> ASan canary OK: instrumented cdylib reported heap-buffer-overflow (exit $rc)"
    printf '%s\n' "$out" | grep -E 'heap-buffer-overflow|spanner_adbc_asan_canary' | head -n 4 \
      | sed 's/^/     /'
    return 0
  fi

  echo "!! ASan canary failed: expected a heap-buffer-overflow report from the Rust cdylib" >&2
  echo "!! canary exit=$rc; full output follows:" >&2
  printf '%s\n' "$out" | sed 's/^/     /' >&2
  return 1
}

build_harness() {
  if [ -n "$RUST_SANITIZE" ]; then
    echo ">> building the spanner-adbc cdylib with -Zsanitizer=$RUST_SANITIZE (nightly, -Zbuild-std, --target $RUST_TARGET, --cfg asan_canary)"
    RUSTFLAGS="-Zsanitizer=$RUST_SANITIZE --cfg asan_canary ${RUSTFLAGS:-}" \
      cargo +nightly build -Zbuild-std --target "$RUST_TARGET"
    run_rust_asan_canary
  else
    echo ">> building the spanner-adbc cdylib"
    cargo build
  fi

  echo ">> building the ADBC validation harness (fetches arrow-adbc + googletest)"
  [ -n "$SANITIZE" ] && echo ">> sanitizers: -fsanitize=$SANITIZE (C++ side${CC:+, CC=$CC CXX=$CXX})"
  [ -n "$RUST_SANITIZE" ] && echo ">> cdylib is ASan-instrumented (Rust -Zsanitizer=$RUST_SANITIZE); C++ side shares its compiler-rt runtime"
  cmake -S "$REPO_ROOT/adbc-validation" -B "$BUILD_DIR" \
    -DCMAKE_BUILD_TYPE=Release -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
    -DSPANNER_VALIDATION_SANITIZE="$SANITIZE" >/dev/null
  cmake --build "$BUILD_DIR" --target spanner_validation -j"$(nproc 2>/dev/null || echo 2)"
}

# Parse `Suite.\n  Case\n ...` from `--gtest_list_tests` into fully-qualified
# Suite.Case names (one per line). Lists only; connects to no database.
list_available_cases() {
  "$BUILD_DIR/spanner_validation" --gtest_list_tests 2>/dev/null | awk '
    /^[A-Za-z]/          { suite = $1; next }
    /^[[:space:]]/ && NF { print suite $1 }
  '
}

# Detect excluded tests renamed or removed upstream. Test enumeration needs no database.
run_stale_guard() {
  echo ">> stale guard: every EXCLUDED entry must still exist upstream"
  local bin="$BUILD_DIR/spanner_validation"
  if [ ! -x "$bin" ]; then
    echo "!! $bin not found; build the harness first" >&2
    return 1
  fi

  local -a available=()
  mapfile -t available < <(list_available_cases)
  if [ "${#available[@]}" -eq 0 ]; then
    echo "!! could not enumerate any test cases from $bin --gtest_list_tests" >&2
    return 1
  fi

  local -A avail=()
  local t
  for t in "${available[@]}"; do avail["$t"]=1; done

  local -a stale=()
  local e
  for e in "${EXCLUDED[@]}"; do
    [ -n "${avail[$e]:-}" ] || stale+=("$e")
  done

  if [ "${#stale[@]}" -ne 0 ]; then
    echo "!! ${#stale[@]} EXCLUDED entr(y/ies) no longer exposed by the harness" >&2
    echo "   (renamed or removed upstream after an ARROW_ADBC_TAG bump?):" >&2
    printf '     %s\n' "${stale[@]}" >&2
    return 1
  fi

  echo ">> stale guard OK: all ${#EXCLUDED[@]} EXCLUDED entries still present upstream"
  return 0
}

# Run all non-excluded tests, including new upstream cases.
run_gate() {
  echo ">> gate: running every non-EXCLUDED case (must all pass or self-skip)"
  "$BUILD_DIR/spanner_validation" --gtest_filter="-$EXCLUDED_FILTER"
}

# Unexpected passes must be removed from EXCLUDED so the normal run enforces them.
# gtest writes each <testcase> opening tag on one line. A pass has result="completed"
# and no <failure> child; skipped cases use result="skipped".
run_xfail_guard() {
  echo ">> expected-failure guard: no EXCLUDED case may actually pass"
  local bin="$BUILD_DIR/spanner_validation"
  local xml="$BUILD_DIR/excluded-results.xml"
  rm -f "$xml"

  local rc=0
  "$bin" --gtest_filter="$EXCLUDED_FILTER" --gtest_output="xml:$xml" >/dev/null 2>&1 || rc=$?
  if [ ! -f "$xml" ]; then
    echo "!! expected-failure guard: harness produced no XML at $xml (exit $rc)" >&2
    return 1
  fi

  local -a xpass=()
  mapfile -t xpass < <(awk '
    /<testcase /{
      cls = ""; nm = ""; res = ""; failed = 0; open = 1
      if (match($0, /classname="[^"]*"/)) cls = substr($0, RSTART + 11, RLENGTH - 12)
      if (match($0, /name="[^"]*"/))      nm  = substr($0, RSTART + 6,  RLENGTH - 7)
      if (match($0, /result="[^"]*"/))    res = substr($0, RSTART + 8,  RLENGTH - 9)
      # A self-closing <testcase .../> has no children => it ran and passed.
      if ($0 ~ /\/>[[:space:]]*$/) { if (res == "completed") print cls "." nm; open = 0 }
      next
    }
    open && /<failure/       { failed = 1 }
    open && /<\/testcase>/   { if (res == "completed" && failed == 0) print cls "." nm; open = 0 }
  ' "$xml")

  if [ "${#xpass[@]}" -ne 0 ]; then
    echo "!! ${#xpass[@]} EXCLUDED test(s) now pass; remove them from EXCLUDED so the" >&2
    echo "   gate enforces them:" >&2
    printf '     %s\n' "${xpass[@]}" >&2
    return 1
  fi

  echo ">> expected-failure guard OK: all ${#EXCLUDED[@]} EXCLUDED cases still fail or skip"
  return 0
}

# --check-drift needs no database, so handle it before starting an emulator.
if [ "$CHECK_DRIFT_ONLY" -eq 1 ]; then
  build_harness
  run_stale_guard
  exit $?
fi

# No target configured: run under a throwaway emulator, then re-enter this script.
if [ -z "${SPANNER_EMULATOR_HOST:-}" ] && [ -z "${SPANNER_OMNI_ENDPOINT:-}" ] && [ -z "${SPANNER_GCP_DATABASE:-}" ]; then
  exec "$REPO_ROOT/scripts/with-emulator.sh" "$REPO_ROOT/scripts/run-adbc-validation.sh" "$@"
fi

build_harness

# Run the stale guard before creating the database; --full is exploratory and skips it.
if [ "$FULL" -eq 0 ]; then
  run_stale_guard
fi

source "$REPO_ROOT/scripts/validation-target.sh"

if [ -n "$RUST_SANITIZE" ]; then
  export ADBC_SPANNER_LIBRARY="$REPO_ROOT/target/$RUST_TARGET/debug/libspanner_adbc.so"
else
  export ADBC_SPANNER_LIBRARY="$REPO_ROOT/target/debug/libspanner_adbc.so"
  [ -f "$ADBC_SPANNER_LIBRARY" ] || ADBC_SPANNER_LIBRARY="$REPO_ROOT/target/debug/libspanner_adbc.dylib"
fi

echo ">> ADBC_SPANNER_LIBRARY=$ADBC_SPANNER_LIBRARY"
echo ">> ADBC_SPANNER_URI=$ADBC_SPANNER_URI"

if [ "$FULL" -eq 1 ]; then
  echo ">> running the FULL validation suite (expect Spanner-specific failures/skips)"
  # Isolate each case so an abort cannot prevent the remaining cases from running.
  ctest --test-dir "$BUILD_DIR" --output-on-failure || true
else
  run_gate
  run_xfail_guard
fi

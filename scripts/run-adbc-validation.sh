#!/usr/bin/env bash
#
# Build the spanner-adbc cdylib and the canonical Apache Arrow ADBC C++
# validation suite (see adbc-validation/), then run the suite against the driver
# loaded through the ADBC driver manager.
#
# With no Spanner target configured it starts a throwaway emulator (via
# scripts/with-emulator.sh) and creates the test instance/database itself, so:
#
#   scripts/run-adbc-validation.sh            # emulator, CI checks (gate + guards)
#   scripts/run-adbc-validation.sh --full     # emulator, every test (exploration)
#   scripts/run-adbc-validation.sh --check-drift  # build + stale guard only (no DB)
#   SPANNER_EMULATOR_HOST=localhost:9010 scripts/run-adbc-validation.sh
#   SPANNER_GCP_DATABASE=proj.inst.db scripts/run-adbc-validation.sh
#
# Two sanitizer knobs (see the SANITIZE / RUST_SANITIZE block below):
#   ADBC_VALIDATION_SANITIZE=address,undefined scripts/run-adbc-validation.sh  # C++ side only
#   ADBC_VALIDATION_SANITIZE=address ADBC_VALIDATION_RUST_SANITIZE=address \
#     scripts/run-adbc-validation.sh   # + the Rust cdylib itself (nightly -Zsanitizer=address,
#                                      #   C++ side auto-built with clang to match the runtime)
#
# The default (CI) path is driven by the single EXCLUDED list below (cases known
# not to pass on Spanner; see adbc-validation/README.md) and runs three checks:
#
#   1. Gate            — everything EXCEPT the excluded cases must pass or skip;
#                        new upstream cases auto-enroll here.
#   2. Expected-failure guard — no excluded case may actually pass.
#   3. Stale guard     — every EXCLUDED entry must still exist upstream.
#
# `--full` runs the whole suite (per-test isolation) for local exploration;
# expect Spanner-specific failures/skips there.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# Instrument the C++ harness, driver manager and validation libraries.
SANITIZE="${ADBC_VALIDATION_SANITIZE:-}"

# Instrument Rust and std with nightly ASan. Requires address sanitization on
# the C++ side and clang so both sides share the compiler-rt runtime.
RUST_SANITIZE="${ADBC_VALIDATION_RUST_SANITIZE:-}"

# Host target triple. `-Zbuild-std` needs an explicit --target, which relocates the cdylib to
# target/<triple>/debug/; resolved once here and reused for both the build and the library path.
RUST_TARGET="$(rustc -vV | sed -n 's/host: //p')"

# Default to clang for runtime compatibility; respect explicit compiler choices.
if [ -n "$RUST_SANITIZE" ]; then
  export CC="${CC:-clang}"
  export CXX="${CXX:-clang++}"
fi

# One build tree per flag combination, so switching legs doesn't force a full arrow-adbc
# rebuild and a cached CI tree can't mix flags. The Rust-instrumented leg compiles the C++ side
# with clang rather than gcc, so it needs a tree of its own distinct from the C-side-only one.
build_suffix=""
[ -n "$SANITIZE" ] && build_suffix="-san"
[ -n "$RUST_SANITIZE" ] && build_suffix="-rustsan"
BUILD_DIR="${ADBC_VALIDATION_BUILD_DIR:-$REPO_ROOT/.adbc-validation-build$build_suffix}"

# Disable LeakSanitizer: the driver's shared Tokio runtime, gRPC connection pools and lazy
# globals are intentionally process-lifetime and would swamp the run with non-actionable
# "leaks". ASan memory errors and UBSan reports stay fatal. Exported for both the build (gtest
# discovery runs the binary) and the run.
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

# ---------------------------------------------------------------------------
# Excluded cases must still exist and fail or skip. All other cases run in the gate.
# Adapt dialect-specific SQL in SpannerQuirks::RewriteSql when possible.
EXCLUDED=(
  # Duration and Interval have no supported column mapping; the emulator also
  # rejects INTERVAL columns. UInt64 cannot fit losslessly in INT64, and the
  # suite cannot express a NUMERIC/Decimal128 round-trip for this case.
  'SpannerStatementTest.SqlIngestDuration'
  'SpannerStatementTest.SqlIngestInterval'
  'SpannerStatementTest.SqlIngestUInt64'

)

# The colon-joined --gtest_filter value for the EXCLUDED set. Prefixed with `-`
# it negates (the gate: run everything else); bare it selects only these (the
# expected-failure guard).
EXCLUDED_FILTER="$(IFS=:; printf '%s' "${EXCLUDED[*]}")"

# ---------------------------------------------------------------------------

# Verify Rust instrumentation by writing past a C++-allocated buffer.
# The test-only canary must trigger an ASan heap-buffer-overflow report.
run_rust_asan_canary() {
  local lib="$REPO_ROOT/target/$RUST_TARGET/debug/libspanner_adbc.so"
  local src="$REPO_ROOT/adbc-validation/asan_canary.cc"
  local bin="$BUILD_DIR/asan_canary"
  mkdir -p "$BUILD_DIR"

  echo ">> ASan canary: compiling the cross-boundary tripwire (${CXX:-clang++} -fsanitize=address)"
  # Same warning hygiene as the CMake target (this compile is deliberately not a CMake
  # target, so the flags are repeated here rather than inherited).
  "${CXX:-clang++}" -std=c++17 -g -O0 -fsanitize=address -fno-omit-frame-pointer \
    -Wall -Wextra -Wpedantic -Wshadow -Wconversion -Wsign-conversion -Werror \
    "$src" -o "$bin" -ldl

  echo ">> ASan canary: running it against $lib (expecting a heap-buffer-overflow)"
  # ASAN_OPTIONS carries abort_on_error=1, so a detected overflow aborts the process (non-zero
  # exit); capture that rather than letting `set -e` kill the script before we can assert on it.
  local out rc=0
  out="$("$bin" "$lib" 2>&1)" || rc=$?

  if [ "$rc" -ne 0 ] \
      && printf '%s' "$out" | grep -q 'AddressSanitizer' \
      && printf '%s' "$out" | grep -q 'heap-buffer-overflow'; then
    echo ">> ASan canary OK: cross-boundary heap-buffer-overflow reported (exit $rc) — the cdylib IS ASan-armed"
    printf '%s\n' "$out" | grep -E 'heap-buffer-overflow|spanner_adbc_asan_canary' | head -n 4 \
      | sed 's/^/     /'
    return 0
  fi

  echo "!! ASan canary did NOT trip — the Rust cdylib is not ASan-armed; the rust-asan leg is a no-op" >&2
  echo "!! (a -Zsanitizer=address / -Zbuild-std regression would let real in-Rust memory bugs pass silently)" >&2
  echo "!! canary exit=$rc; full output follows:" >&2
  printf '%s\n' "$out" | sed 's/^/     /' >&2
  return 1
}

build_harness() {
  if [ -n "$RUST_SANITIZE" ]; then
    # Instrument Rust and std, and include the test-only overflow canary.
    # The explicit target places the library in target/<triple>/debug/.
    echo ">> building the spanner-adbc cdylib with -Zsanitizer=$RUST_SANITIZE (nightly, -Zbuild-std, --target $RUST_TARGET, --cfg asan_canary)"
    RUSTFLAGS="-Zsanitizer=$RUST_SANITIZE --cfg asan_canary ${RUSTFLAGS:-}" \
      cargo +nightly build -Zbuild-std --target "$RUST_TARGET"
    # Verify instrumentation before running the full suite.
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

# Stale guard: every EXCLUDED entry must still be a case the harness exposes, so a
# rename/removal upstream (e.g. after bumping ARROW_ADBC_TAG) is caught instead of
# silently rotting. Needs no database (`--gtest_list_tests` only enumerates).
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
    echo "   (renamed/removed upstream? bumped ARROW_ADBC_TAG? — clean up EXCLUDED):" >&2
    printf '     %s\n' "${stale[@]}" >&2
    return 1
  fi

  echo ">> stale guard OK: all ${#EXCLUDED[@]} EXCLUDED entries still present upstream"
  return 0
}

# Gate: run every case EXCEPT the excluded ones; it must pass. This is where a
# newly-added upstream case auto-enrolls (it isn't excluded, so it runs here).
run_gate() {
  echo ">> gate: running every non-EXCLUDED case (must all pass or self-skip)"
  "$BUILD_DIR/spanner_validation" --gtest_filter="-$EXCLUDED_FILTER"
}

# Expected-failure guard (xfail-strict): run ONLY the excluded cases and assert none
# PASSED — one that starts passing must be removed from EXCLUDED so the gate enforces it.
# The run exits non-zero by design, so its status is captured, not propagated. The
# discriminator comes from the JUnit XML (gtest writes each <testcase> opening tag on one
# line): passed = result="completed" with no <failure> child, skipped = result="skipped" —
# so a skipped excluded case does not count as passing.
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
    echo "!! ${#xpass[@]} EXCLUDED test(s) now PASS — remove them from EXCLUDED so the" >&2
    echo "   gate enforces them (an expected-failure that started passing):" >&2
    printf '     %s\n' "${xpass[@]}" >&2
    return 1
  fi

  echo ">> expected-failure guard OK: all ${#EXCLUDED[@]} EXCLUDED cases still fail or skip"
  return 0
}

# --check-drift: build the harness and run only the stale guard. This needs no
# emulator/database, so it runs before the emulator gate below and exits.
if [ "$CHECK_DRIFT_ONLY" -eq 1 ]; then
  build_harness
  run_stale_guard
  exit $?
fi

# No target configured: run under a throwaway emulator, then re-enter this script.
if [ -z "${SPANNER_EMULATOR_HOST:-}" ] && [ -z "${SPANNER_OMNI_ENDPOINT:-}" ] && [ -z "${SPANNER_GCP_DATABASE:-}" ]; then
  exec "$REPO_ROOT/scripts/with-emulator.sh" "$0" "$@"
fi

build_harness

# The stale guard needs no database, so run it before the emulator DB creation.
# Skip it under --full (which is exploratory and runs every case anyway).
if [ "$FULL" -eq 0 ]; then
  run_stale_guard
fi

source "$REPO_ROOT/scripts/validation-target.sh"

# The instrumented leg builds with an explicit --target, so its artifact lives under
# target/<triple>/debug/ rather than target/debug/.
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
  # Per-test process isolation via ctest, so a failing assertion (which aborts the process
  # — see the README note on the upstream non-idempotent error release) only fails that one
  # test rather than hiding the rest.
  ctest --test-dir "$BUILD_DIR" --output-on-failure || true
else
  # Gate first (must pass), then assert the excluded set still all fails/skips.
  run_gate
  run_xfail_guard
fi

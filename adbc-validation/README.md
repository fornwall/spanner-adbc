# ADBC C++ validation

Runs Apache Arrow ADBC's driver-independent C++ validation suite against the shared library,
loaded through the ADBC driver manager. It tests the C ABI; see [testing](../docs/testing.md)
for the other suites.

## Running

```sh
scripts/run-adbc-validation.sh                 # the CI checks, on a disposable emulator
scripts/run-adbc-validation.sh --full          # every case, including known failures
scripts/run-adbc-validation.sh --check-drift   # build and check the exclusions; no database

SPANNER_EMULATOR_HOST=localhost:9010 scripts/run-adbc-validation.sh
SPANNER_GCP_DATABASE=my-project.my-instance.my-test-db scripts/run-adbc-validation.sh
```

You need Rust, Bash 4+, C and C++20 compilers, CMake 3.20+, git, Python 3 and curl, plus Docker
for the emulator. The script builds the driver and the harness, and creates the emulator
database. Without a configured target it starts an emulator. The target is chosen from
`SPANNER_OMNI_ENDPOINT`, then `SPANNER_EMULATOR_HOST`, then `SPANNER_GCP_DATABASE`. A Cloud
Spanner database must already exist and is reached with ADC; use a dedicated test database.

[CMakeLists.txt](CMakeLists.txt) pins arrow-adbc with `ARROW_ADBC_TAG` and fetches and builds
the C++ dependencies. Keep that revision in line with the arrow-adbc crates in `Cargo.toml`.
`ADBC_VALIDATION_BUILD_DIR` overrides the build directory.

`--full` runs each case in its own process through CTest and prints failures, but always exits
successfully. Use the default mode for CI.

## Coverage and exclusions

[`SpannerQuirks`](spanner_validation.cc) declares the driver's capabilities, readback type
conversions, identifier and parameter syntax, and GoogleSQL rewrites of the suite's SQL. Each
rewrite asserts the upstream SQL it expects, so a changed query needs review when the pin moves.

The suite covers database and connection lifetimes, metadata, transaction options, queries,
prepared parameters, row counts, partitioning, cancellation, error and stream lifetimes, and all
four ingest modes. It exercises native, widened, large, view, dictionary and list Arrow
types where the driver supports them.

[`run-adbc-validation.sh`](../scripts/run-adbc-validation.sh) excludes three cases:

| Excluded case | Reason |
| --- | --- |
| `SpannerStatementTest.SqlIngestUInt64` | Arrow `UInt64` doesn't fit in `INT64`, so the driver can't create a column for it. |
| `SpannerStatementTest.SqlIngestDuration` | No column mapping for ingest. |
| `SpannerStatementTest.SqlIngestInterval` | No column mapping for ingest. |

The default run then checks that:

1. every other case passes or skips itself, including cases added upstream;
2. every excluded case still fails or skips, so an unexpected pass means the exclusion should go;
3. every excluded case still exists, so renamed or removed cases are caught.

Some cases skip through quirks instead: `Transactions` expects behavior that conflicts with
immediate DDL and buffered manual writes, `SqlIngestPrimaryKey` expects ordered generated keys,
and the `SqlIngestTemporary*` cases need temporary tables. Check 2 doesn't cover these, so review
them when the driver's capabilities change.

## Sanitizers

The [CI workflow](../.github/workflows/adbc-validation.yml) runs the same checks in three modes:

| Mode | Instrumentation |
| --- | --- |
| `plain` | None |
| `asan-ubsan` | AddressSanitizer and UndefinedBehaviorSanitizer for C and C++; Rust is not instrumented |
| `rust-asan` | Rust and its standard library built with nightly ASan; C++ built with clang ASan |

```sh
ADBC_VALIDATION_SANITIZE=address,undefined scripts/run-adbc-validation.sh

ADBC_VALIDATION_SANITIZE=address ADBC_VALIDATION_RUST_SANITIZE=address \
  scripts/run-adbc-validation.sh
```

The Rust mode needs nightly with `rust-src`, clang and clang++. It builds with
`-Zsanitizer=address` and `-Zbuild-std`, defaults `CC` and `CXX` to clang, and loads the library
from `target/<host-triple>/debug/`. If you override the compilers, keep the C++ and Rust ASan
runtimes compatible. Rust mode only supports `address`; UBSan runs in the C++ mode.

C++ ASan catches errors that reach its allocator and memory interceptors, but can't see memory
accesses inside uninstrumented Rust. Rust ASan covers those. As a positive control, its
[canary](asan_canary.cc) calls a test-only Rust function that writes past a C++ allocation, and
the suite only runs if ASan reports the heap buffer overflow. The script enables that function
with `--cfg asan_canary`.

Leak detection is off, because the runtime and client state live for the whole process. ASan
memory errors and UBSan reports are still fatal. The Rust flag doesn't instrument the native TLS
C and assembly code. Each mode uses its own build directory.

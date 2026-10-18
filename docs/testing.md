# Testing

Run commands from the repository root. Target-dependent Rust and Python tests skip when their
required environment variables are unset. CI sets `ADBC_TEST_REQUIRE_TARGET=1` to make missing
targets or shared libraries fail instead; leave it unset for ordinary local runs.

| Suite | Local command | CI / details |
| --- | --- | --- |
| Unit tests and doctests | `cargo test` | [CI](../.github/workflows/ci.yml) |
| Mock gRPC server | `cargo test --test mock_spanner` | [CI](../.github/workflows/ci.yml), [tests](../tests/mock_spanner.rs) |
| Emulator integration | `scripts/with-emulator.sh cargo test` | [CI](../.github/workflows/ci.yml) |
| Omni integration | `scripts/with-omni.sh cargo test` | [CI](../.github/workflows/ci.yml) |
| Python package | See [Python setup](#python-package) below | [Python README](../python/README.md), [CI](../.github/workflows/ci.yml) |
| Transport faults | `scripts/with-toxiproxy.sh cargo test --test resilience -- --test-threads=1` | [Resilience guide](../tests/RESILIENCE.md) |
| Backend helper cleanup | `python3 -m unittest discover -s scripts/tests -v` | Offline; no Docker required |
| ADBC C++ conformance | `scripts/run-adbc-validation.sh` | [Harness guide](../adbc-validation/README.md) |
| Foundry type/feature coverage | `scripts/run-foundry-validation.sh` | [Harness guide](../foundry-validation/README.md) |
| Fuzzing | `cargo +nightly fuzz run <target>` | [Fuzz workflow](../.github/workflows/fuzz.yml) |
| Conversion benchmarks | `cargo bench --bench conversion` | [Benchmarks](../benches/conversion.rs); local only |

## Offline coverage and CI

Unit tests cover SQL parsing, options, value conversion and transaction state. The in-process
mock server also checks gRPC errors and details, cancellation, timeouts, retry limits, ingest
failures and request options on the wire. These need no emulator or cloud credentials.

[CI](../.github/workflows/ci.yml) runs on pushes to `main` and pull requests. It runs unit tests
with `--features fuzzing`, doctests with `--all-features`, mock tests, and the `integration` and
`ffi_lifecycle` binaries. Additional checks are formatting, Clippy, rustdoc with warnings denied,
a build without default features, actionlint, cargo-deny and cargo-machete. New target-dependent
test binaries must be added explicitly to the workflow.

Rust integration, Python, C++ validation and Foundry each have an `emulator` / `omni` matrix.
The Python job tests DBAPI, Arrow, DataFrame integrations and type mapping against Google's
Python client, on Python 3.11 and `3.x`. C++ validation runs plain, C++ ASan/UBSan and Rust ASan
builds on both backends. Foundry and resilience have separate workflows on pushes to `main` and pull requests.
Transport fault injection uses the emulator.
Resilience and fuzzing also run nightly and create or update a tracking issue on failure.

## Emulator, Omni and cloud targets

[`tests/integration.rs`](../tests/integration.rs) covers queries, DML, ingest, transactions,
metadata, partitioning and C-ABI conformance. [`tests/ffi_lifecycle.rs`](../tests/ffi_lifecycle.rs)
checks stream/handle lifetimes through the driver manager, raw C ABI and Rust API.
Both prefer a nonempty `SPANNER_OMNI_ENDPOINT`, then `SPANNER_EMULATOR_HOST`, then `SPANNER_GCP_DATABASE`.

```sh
cargo build  # required for tests that load the shared library
scripts/with-emulator.sh cargo test
```

The emulator helpers require Docker and curl, wait for the admin REST API, and remove only
containers they created. A container-name collision fails without disturbing the existing
container; choose another `SPANNER_EMULATOR_CONTAINER` (and `TOXIPROXY_CONTAINER` for fault tests)
if needed. Published ports bind to localhost. Tests use `test-project/test-instance/adbc-test`.

The emulator wrapper uses **9010/9020** because test provisioning still uses the pinned SDK's
REST admin endpoint. The driver itself uses gRPC for DDL. Parallel emulators can use distinct
Docker-network IPs with their internal ports and unique container names.

[`scripts/with-omni.sh`](../scripts/with-omni.sh) starts Omni `2026.r4-lts`, the latest published
[release](https://docs.cloud.google.com/spanner-omni/download) checked on 2026-10-18.
The version is pinned in that script; set `SPANNER_OMNI_IMAGE` to test another release.
It requires Docker and GNU `timeout`, publishes ports 15000–15025 on localhost, waits for gRPC
admin readiness, creates `default/default/adbc-test`, and removes its container and storage on exit.

```sh
scripts/with-omni.sh cargo test --test integration --test ffi_lifecycle
scripts/with-omni.sh scripts/run-adbc-validation.sh
scripts/with-omni.sh scripts/run-foundry-validation.sh
```

For an existing Omni deployment, provision `projects/default/instances/default/databases/adbc-test`
and set `SPANNER_OMNI_ENDPOINT=http://localhost:15000`. Omni uses `spanner.instance_type=omni`
and an explicit `spanner.endpoint`; it does not use emulator mode. Fixtures modify this database.

For a cloud database, unset `SPANNER_EMULATOR_HOST` and `SPANNER_OMNI_ENDPOINT` and use Application Default Credentials:

```sh
SPANNER_GCP_DATABASE=my-project.my-instance.my-test-db \
  cargo test --test integration --test ffi_lifecycle -- --nocapture
```

The instance must exist. Setup attempts to create the database and test tables; fixtures mutate
named tables and leave some schema behind, so use a dedicated test database. No CI workflow runs
against a cloud database.

The `auth_end_to_end` tests additionally use `SPANNER_TEST_KEYFILE` and/or
`SPANNER_TEST_IMPERSONATE_TARGET_PRINCIPAL` with `SPANNER_GCP_DATABASE`. These opt-in tests always
use the real target, independently of the normal emulator-first selection.

## Python package

In a Python 3.11+ virtual environment, build and install the current driver before testing.
These commands use the Linux library name; substitute the platform's library on macOS/Windows.

```sh
cargo build
cp target/debug/libspanner_adbc.so python/spanner_adbc/
python -m pip install ./python pyarrow pandas polars duckdb pytest google-cloud-spanner
scripts/with-emulator.sh python -m pytest python/tests -v
```

The suite includes executable README examples. Rebuild, re-copy and reinstall after driver changes.

## Fuzzing

Install cargo-fuzz and a nightly toolchain, then run a target:

```sh
cargo +nightly fuzz run sql
```

| Target | Coverage |
| --- | --- |
| `sql` | Statement splitting and DDL detection |
| `values` | DATE, TIMESTAMP and NUMERIC parsing |
| `like` | LIKE matching |
| `keyword` | Keyword classification |
| `options` | Option keys and values |
| `params` | Parameter extraction |
| `partition` | Partition descriptor decoding |
| `staleness` | Read staleness and duration grammar |
| `directed_read` | Directed-read grammar |
| `uri` | Connection URI parsing |

[`fuzz/`](../fuzz/) shares the root workspace and lockfile but is excluded from default build,
test and Clippy commands. The [workflow](../.github/workflows/fuzz.yml) derives its matrix from
`fuzz/Cargo.toml`, seeds runs from `fuzz/seeds/`, and caches the generated corpus. The unit test
`every_fuzz_target_is_wired_and_documented` checks harnesses, manifest entries, CI discovery and
target names in this page and `CLAUDE.md`.

## Benchmarks

[`benches/conversion.rs`](../benches/conversion.rs) measures wire-value-to-Arrow conversion using
synthetic 8192-row batches, without a database. Use `cargo bench --bench conversion -- --test` for
a quick sanity run. For end-to-end reads and profiling, see [Python benchmarks](../python/benchmarks/README.md).

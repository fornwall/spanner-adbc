# Testing

Run commands from the repository root. Tests that need a Spanner backend or the shared library
skip when none is configured. Set `ADBC_TEST_REQUIRE_TARGET=1`, as CI does, to make them fail
instead.

| Suite | Command | More |
| --- | --- | --- |
| Unit tests and doctests | `cargo test` | |
| Mock gRPC server | `cargo test --test mock_spanner` | [tests](../tests/mock_spanner.rs) |
| Emulator integration | `scripts/with-emulator.sh cargo test` | [below](#backends) |
| Omni integration | `scripts/with-omni.sh cargo test` | [below](#backends) |
| Python package | see [below](#python-package) | [Python README](../python/README.md) |
| Transport faults | `scripts/with-toxiproxy.sh cargo test --test resilience` | [RESILIENCE.md](../tests/RESILIENCE.md) |
| Scripts, workflows and pins | `python3 -m unittest discover -s scripts/tests -v` | No Docker needed |
| ADBC C++ conformance | `scripts/run-adbc-validation.sh` | [harness](../adbc-validation/README.md) |
| Foundry type and feature suite | `scripts/run-foundry-validation.sh` | [harness](../foundry-validation/README.md) |
| Fuzzing | `CARGO_PROFILE_RELEASE_LTO=false cargo +nightly fuzz run <target>` | [below](#fuzzing) |
| Conversion benchmarks | `cargo bench --bench conversion` | [below](#benchmarks) |

Unit tests cover SQL parsing, options, value conversion and transaction state. The in-process
mock server checks what goes over the wire: gRPC errors and details, cancellation, timeouts,
retry limits, ingest failures and request options. Neither needs a backend or credentials.

Run the focused authentication tests with `cargo test --lib driver::credentials::tests`.
They cover credential selection, construction, validation and error handling without a Cloud
Spanner database. The opt-in Cloud tests below verify real credentials and IAM permissions.

CI runs on pushes to `main` and on pull requests. It runs the Rust, Python, C++ validation and
Foundry suites against both the emulator and Omni, the C++ suite also under sanitizers, and
fault injection against the emulator. Fault injection also runs nightly, and fuzzing runs only
nightly. No CI job uses Cloud Spanner. CI lists the target-dependent test binaries explicitly,
so add new ones to [ci.yml](../.github/workflows/ci.yml); a script test fails until you do.

## Backends

[`tests/integration.rs`](../tests/integration.rs) covers queries, DML, ingest, transactions,
metadata, partitioning and C ABI conformance. [`tests/ffi_lifecycle.rs`](../tests/ffi_lifecycle.rs)
checks stream and handle lifetimes through the driver manager, the raw C ABI and the Rust API.
Both use the first non-empty variable of `SPANNER_OMNI_ENDPOINT`, `SPANNER_EMULATOR_HOST` and
`SPANNER_GCP_DATABASE`. Tests that load the shared library need a `cargo build` first.

### Emulator

`scripts/with-emulator.sh` needs Docker and curl. It starts an emulator with gRPC on port 9010
and REST on port 9020, bound to localhost, waits for the REST admin API, runs the command and
removes the container. The tests use `test-project/test-instance/adbc-test`.

The script never touches a container it didn't create. If the container name is taken, it
fails; pick another with `SPANNER_EMULATOR_CONTAINER` (and `TOXIPROXY_CONTAINER` for fault
tests). The tests set up their instance and database with the pinned client's REST admin API,
which it finds by replacing port 9010 in `SPANNER_EMULATOR_HOST` with 9020. So the script always
publishes those two ports and only one copy can run at a time. To run emulators in parallel,
start each container yourself under its own name and point `SPANNER_EMULATOR_HOST` at its Docker
network IP on port 9010.

### Omni

`scripts/with-omni.sh` needs Docker and GNU `timeout`. It runs Omni `2026.r4-lts` (set
`SPANNER_OMNI_IMAGE` to test another [release](https://docs.cloud.google.com/spanner-omni/download)),
publishes ports 15000 to 15025 on localhost, creates `default/default/adbc-test` and removes the
container and its storage on exit.

```sh
scripts/with-omni.sh cargo test --test integration --test ffi_lifecycle
scripts/with-omni.sh scripts/run-adbc-validation.sh
scripts/with-omni.sh scripts/run-foundry-validation.sh
```

To use an existing Omni deployment instead, create
`projects/default/instances/default/databases/adbc-test` and set
`SPANNER_OMNI_ENDPOINT=http://localhost:15000`. The tests connect with
`spanner.instance_type=omni` and an explicit endpoint, not emulator mode. Fixtures modify the
database.

### Cloud Spanner

Unset `SPANNER_EMULATOR_HOST` and `SPANNER_OMNI_ENDPOINT`, and use Application Default
Credentials:

```sh
SPANNER_GCP_DATABASE=my-project.my-instance.my-test-db \
  cargo test --test integration --test ffi_lifecycle -- --nocapture
```

The instance must exist. Setup creates the database and test tables if needed. Fixtures modify
tables and leave some schema behind, so use a dedicated test database.

The `auth_end_to_end` tests also need `SPANNER_TEST_KEYFILE`,
`SPANNER_TEST_IMPERSONATE_TARGET_PRINCIPAL` or both, together with `SPANNER_GCP_DATABASE`. They
always run against that database, even when an emulator or Omni endpoint is set.
Setup uses ADC to create fixtures, independently of the keyfile under test. The keyfile can
contain any supported credential JSON type. For impersonation, the ADC identity needs
`roles/iam.serviceAccountTokenCreator` on the target, and the target needs Spanner access.

```sh
SPANNER_GCP_DATABASE=my-project.my-instance.my-test-db \
  SPANNER_TEST_KEYFILE=/path/to/credentials.json \
  cargo test --test integration auth_end_to_end -- --nocapture
```

## Python package

In a Python 3.11+ virtual environment, build the driver, stage it in the package and install:

```sh
cargo build
cp target/debug/libspanner_adbc.so python/spanner_adbc/
python -m pip install ./python pyarrow pandas polars duckdb pytest google-cloud-spanner
scripts/with-emulator.sh python -m pytest python/tests -v
```

Use `libspanner_adbc.dylib` on macOS or `spanner_adbc.dll` on Windows. Repeat the build, copy
and install after changing the driver. The suite also runs the Python README's examples. It
runs against the emulator or Omni (`scripts/with-omni.sh`), not Cloud Spanner.

## Fuzzing

Install cargo-fuzz and a nightly toolchain, then run a target with LTO off:

```sh
CARGO_PROFILE_RELEASE_LTO=false cargo +nightly fuzz run sql
```

The release profile's thin LTO strips the coverage counters from the driver crate, which leaves
the fuzzer blind. The fuzz workflow turns it off the same way.

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
| `directed_read` | Directed read grammar |
| `uri` | Connection URI parsing |

Each target needs a harness in `fuzz/fuzz_targets/` and a `[[bin]]` entry in `fuzz/Cargo.toml`;
a unit test checks that they match. [`fuzz/`](../fuzz/) shares the root workspace and lockfile
but is left out of the default build and test commands. The required `cargo clippy --workspace`
check includes it. The [fuzz workflow](../.github/workflows/fuzz.yml) builds its matrix from
`fuzz/Cargo.toml` and seeds runs from `fuzz/seeds/`.

## Benchmarks

[`benches/conversion.rs`](../benches/conversion.rs) measures conversion from Spanner wire values
to Arrow on synthetic 8192-row batches, without a database. `cargo bench --bench conversion --
--test` does a quick sanity run. For end-to-end reads and profiling, see the
[Python benchmarks](../python/benchmarks/README.md).

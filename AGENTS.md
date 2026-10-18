# Agent guide

`spanner-adbc` is a Rust ADBC driver for Google Cloud Spanner and Spanner Omni.
It returns Arrow record batches through `adbc_core` traits and a hand-written C ABI
in `src/ffi/`. The cdylib exports `AdbcSpannerInit` and `AdbcDriverInit`.

Read [CONTRIBUTING.md](CONTRIBUTING.md) for coding conventions, dependency pins and releases.
Use the toolchain in `rust-toolchain.toml`; run commands from the repository root.

## Editing the driver

- Keep SQL lexing, splitting, quoting and parameter extraction in `src/sql.rs`.
- Reuse the driver's Tokio runtime. The C ABI shares a process-wide driver through
  `shared_driver()` in `src/ffi/database.rs`; do not create a runtime per call or handle.
- Confine `unsafe` to `src/ffi/` and preserve the build without default features.
- Define option keys as `pub const OPTION_*` in `src/lib.rs`. Update
  [docs/options.md](docs/options.md) and `python/spanner_adbc/_options.py` with option changes.
- Preserve transaction boundaries: DDL runs immediately through the admin API, before
  buffered DML, and rollback cannot undo it. See [transactions](docs/transactions.md).
- Internal metadata reads inherit priority, replica selection, retries and RPC timeouts,
  but remain untagged; request/transaction tags attribute user statements.
- Put unit tests in sibling `src/<module>/tests.rs` files. Explain non-obvious constraints
  in comments; omit review-ticket IDs and narratives about removed implementations.

## Validation

Run focused tests while editing. Before pushing code changes, run the contributor checks:

```sh
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
cargo build
cargo test
cargo test --lib --features fuzzing
scripts/with-emulator.sh cargo test
```

Build the shared library before tests that load it. After Rust changes, refresh the library
staged in `python/spanner_adbc/` before testing Python; see [Python setup](docs/testing.md#python-package).
For FFI or feature changes, also run `cargo build --no-default-features`; for rustdoc changes,
run `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features`.

- Use `scripts/with-emulator.sh <cmd>` or `scripts/with-omni.sh <cmd>` for local backends.
  They provision disposable targets and clean up their containers.
- Rust tests select `SPANNER_OMNI_ENDPOINT`, then `SPANNER_EMULATOR_HOST`, then
  `SPANNER_GCP_DATABASE` (`project.instance.database`, using ADC). Python supports both local backends.
  Without a target, integration tests skip; `ADBC_TEST_REQUIRE_TARGET=1` makes missing targets fail.
- Cloud and auth tests are opt-in and use a dedicated test database; fixtures mutate tables.
- Fault injection: `scripts/with-toxiproxy.sh cargo test --test resilience -- --test-threads=1`.
- Register new target-dependent test binaries in `.github/workflows/ci.yml`; CI lists them explicitly.
  See [testing](docs/testing.md) for C++ conformance, Foundry, backend setup and CI coverage.

### Fuzzing

Targets: `sql`, `values`, `like`, `options`, `keyword`, `params`, `partition`, `staleness`,
`directed_read`, `uri`. Each needs a harness in `fuzz/fuzz_targets/`, a `[[bin]]` in
`fuzz/Cargo.toml`, and a mention here and in [docs/testing.md](docs/testing.md#fuzzing).
The wiring test checks these; CI derives its matrix from the manifest. `fuzz/` shares the root
workspace and lockfile but is excluded from default build/test commands.

## Working with pinned dependencies

- Read revisions from `Cargo.toml`/`Cargo.lock`. Locate matching source with
  `cargo metadata --format-version 1 --locked`; do not guess checkout hashes or use API
  documentation for a different dependency revision.
- Keep `adbc_core`, `adbc_ffi`, `adbc_driver_manager` and CMake's `ARROW_ADBC_TAG` aligned.
  `src/ffi/abi.rs` transcribes that revision's `c/include/arrow-adbc/adbc.h`.
- Keep all Google Cloud crates, including `spanner-grpc-mock`, on one revision.
- ADBC tracks upstream `main` deliberately. The Arrow patches supply required
  `FFI_ArrowArrayStream` APIs; do not drop pins merely to use registry releases.
- Follow the [registry migration checklist](CONTRIBUTING.md#registry-migration-checklist)
  when a release supplies the required APIs. Each git dependency family blocks crates.io publishing.

## References

- [README.md](README.md): public behavior, type mapping and authentication.
- [docs/adbc.md](docs/adbc.md) and `src/ffi/mod.rs`: ADBC ownership, errors and C ABI design.
- [python/README.md](python/README.md): Python API; [docs/dbt.md](docs/dbt.md): dbt integration.
- [Releasing](CONTRIBUTING.md#releasing): use `cargo-release` for requested releases; do not
  manually bump versions or create release tags.

Keep this guide under 100 lines; link to detailed docs instead of duplicating them.

# Agent guide

`spanner-adbc` is a Rust ADBC driver for Google Cloud Spanner and Spanner Omni. It returns Arrow
record batches through the `adbc_core` traits and a hand-written C ABI in `src/ffi/`. The cdylib
exports `AdbcSpannerInit` and `AdbcDriverInit`.

Read [CONTRIBUTING.md](CONTRIBUTING.md) before changing code. Its coding conventions apply to
you, and its [checks](CONTRIBUTING.md#checks) must pass before you push code changes, including
the extra checks for FFI, feature and rustdoc changes. Use the toolchain in `rust-toolchain.toml`
and run commands from the repository root.

## Editing the driver

- Keep SQL lexing, splitting, quoting and parameter extraction in `src/sql.rs`.
- Reuse the driver's Tokio runtime. The C ABI shares one process-wide driver through
  `shared_driver()` in `src/ffi/database.rs`; don't create a runtime per call or handle.
- Confine `unsafe` to `src/ffi/` and keep the build without default features working.
- Define option keys as `pub const OPTION_*` in `src/lib.rs`, and update
  [docs/options.md](docs/options.md) and `python/spanner_adbc/_options.py` with option changes.
- DDL runs immediately through the admin API, before buffered DML, and rollback can't undo it.
  Preserve these [transaction boundaries](docs/transactions.md).
- Internal metadata reads inherit priority, replica selection, retries and RPC timeouts, but stay
  untagged; request and transaction tags are for user statements.
- Put unit tests in sibling `src/<module>/tests.rs` files.
- Comment only what the code can't say: a non-obvious reason, in one or two plain sentences.
  Don't narrate code, restate names or option keys, use em-dashes or bold, or mention review
  tickets and removed implementations.

## Testing

- Run focused tests while editing, and the full checks before pushing.
- Run `cargo build` before tests that load the shared library. After Rust changes, refresh the
  library staged in `python/spanner_adbc/` before running Python tests; see
  [Python setup](docs/testing.md#python-package).
- `scripts/with-emulator.sh <cmd>` and `scripts/with-omni.sh <cmd>` provision a disposable
  backend, run the command and remove their containers.
- Rust tests use `SPANNER_OMNI_ENDPOINT`, then `SPANNER_EMULATOR_HOST`, then
  `SPANNER_GCP_DATABASE` (`project.instance.database`, with ADC). Without a target, integration
  tests skip; `ADBC_TEST_REQUIRE_TARGET=1` makes a missing target fail instead.
- Cloud and auth tests are opt-in and need a dedicated test database, because fixtures modify
  tables.
- Fault injection: `scripts/with-toxiproxy.sh cargo test --test resilience`.
- CI lists target-dependent test binaries explicitly. Register new ones in
  `.github/workflows/ci.yml`.

[Testing](docs/testing.md) covers C++ conformance, Foundry validation, backend setup and CI.

### Fuzzing

Each fuzz target needs a harness in `fuzz/fuzz_targets/` and a `[[bin]]` entry in
`fuzz/Cargo.toml`. A unit test checks the wiring, and CI builds its fuzz matrix from the
manifest. `fuzz/` shares the root workspace and lockfile but is excluded from the default build
and test commands.

## Dependencies

[CONTRIBUTING.md](CONTRIBUTING.md#dependency-pins) explains why the Google Cloud and ADBC
crates are pinned to git revisions.

- Read revisions from `Cargo.toml` and `Cargo.lock`, and find the matching source with
  `cargo metadata --format-version 1 --locked`. Don't guess checkout hashes or rely on API
  documentation for another revision.
- Keep `adbc_core`, `adbc_ffi`, `adbc_driver_manager` and CMake's `ARROW_ADBC_TAG` on one
  revision; `src/ffi/abi.rs` transcribes that revision's `c/include/arrow-adbc/adbc.h`.
- Keep all Google Cloud crates, including `spanner-grpc-mock`, on one revision.
- Don't drop a pin just to use a registry release. When a release has the required APIs, follow
  the [registry migration checklist](CONTRIBUTING.md#registry-migration-checklist).

## Releases

Only release when asked, and use `cargo-release` as described in
[Releasing](CONTRIBUTING.md#releasing). Never bump versions or create release tags by hand.

## References

- [README.md](README.md): public behavior, type mapping and authentication.
- [docs/adbc.md](docs/adbc.md) and `src/ffi/mod.rs`: supported ADBC features, ownership, errors
  and C ABI design.
- [docs/transactions.md](docs/transactions.md): transaction boundaries and RPCs.
- [python/README.md](python/README.md): Python API.
- [docs/dbt.md](docs/dbt.md): notes for a future dbt adapter.

Keep this file under 100 lines. Link to detailed docs instead of copying them.

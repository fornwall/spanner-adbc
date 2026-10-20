# Contributing

## Building and testing

Use the toolchain pinned in `rust-toolchain.toml` and run commands from the repository root:

```sh
cargo build
cargo test
```

Tests that need a Spanner backend skip themselves unless one is configured.
`scripts/with-emulator.sh cargo test` runs them against a disposable emulator in Docker.
[Testing](docs/testing.md) covers the other backends and test suites.

### Checks

Run these before pushing code changes:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo build
cargo test
cargo test --lib --features fuzzing
scripts/with-emulator.sh cargo test
```

For FFI or feature changes, also run `cargo build --no-default-features`. For rustdoc changes,
run `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features`. For changes to `scripts/`,
run `python3 -m unittest discover -s scripts/tests`. CI runs these along with the Python tests,
the external validation suites and dependency checks.

## Coding conventions

- Put unit tests in a sibling `src/<module>/tests.rs` file, not inline.
- Use comments to explain constraints that aren't obvious from the code. Leave out review
  ticket IDs and accounts of removed code; git has the history.
- Keep SQL lexing, splitting, quoting and parameter extraction in `src/sql.rs`.
- Reuse the driver's Tokio runtime to call the async client. The C ABI shares one process-wide
  driver through `shared_driver()` in `src/ffi/database.rs`. Don't create a runtime per call or
  per handle.
- Keep `unsafe` code in `src/ffi/`. The `--no-default-features` build must have none.
- Define option keys as `pub const OPTION_*` in `src/lib.rs` and use the constants instead of
  string literals. When options change, update [docs/options.md](docs/options.md) and
  `python/spanner_adbc/_options.py`.
- DDL runs immediately through the admin API, before any buffered DML, and rollback can't undo
  it. Keep these transaction boundaries; see [transactions](docs/transactions.md).
- Internal metadata reads inherit priority, replica selection, retry settings and RPC timeouts,
  but carry no request or transaction tags. Tags identify user statements.

Keep pull requests focused.

## Dependency pins

`Cargo.toml` pins three dependency families to git revisions:

- The Google Cloud crates, all on one google-cloud-rust revision. This includes
  `spanner-grpc-mock`, which the mock-server tests use and which isn't published.
- `adbc_core`, `adbc_ffi` and `adbc_driver_manager`, which track arrow-adbc `main`. The C++
  validation suite (`ARROW_ADBC_TAG` in `adbc-validation/CMakeLists.txt`) uses the same revision,
  and `src/ffi/abi.rs` transcribes that revision's `adbc.h`.
- The `arrow-*` crates, patched to arrow-rs `main` in `[patch.crates-io]`. Arrow 60 lacks the
  `FFI_ArrowArrayStream` constructor and accessors that the C ABI needs.

Each family on its own prevents publishing to crates.io, so the release configuration sets
`publish = false`. Rust users of the crate have to match the pins; see the
[README](README.md#rust).

Keep each family on a single revision. Before removing a pin, check that a registry release
has the APIs the driver uses.

### Registry migration checklist

When moving a family to crates.io, update these together:

- The affected dependencies and dev-dependencies in `Cargo.toml`. Downstream users must use the
  same `adbc_core`.
- For ADBC, `ARROW_ADBC_TAG` in `adbc-validation/CMakeLists.txt` and the definitions in
  `src/ffi/abi.rs`.
- The `allow-git` entries in `deny.toml`, once a repository has no git dependencies left.
- The dependency notes in `README.md`, this file and [AGENTS.md](AGENTS.md#dependencies).

For Google Cloud, also check that `spanner-grpc-mock` is published and compatible. For Arrow,
revisit the version ranges so they keep matching `adbc_core`'s. Keep `publish = false` until
every family can come from the registry.

## Versioning

Versions follow [semantic versioning](https://semver.org/). Before 1.0, minor releases may contain
incompatible changes.

## Releasing

Releases use [`cargo-release`](https://github.com/crate-ci/cargo-release) (`cargo install
cargo-release`), configured in `Cargo.toml`. Don't bump versions or create release tags by hand.
Releasing needs push access to `main`.

```sh
cargo release patch            # dry run
cargo release patch --execute  # bump, commit, tag and push
```

The pre-release hook runs fmt, clippy and `cargo test`, and cargo-release also updates the
Python package version in `python/spanner_adbc/_version.py`.

Pushing a `vX.Y.Z` tag starts `.github/workflows/libraries.yml`. It checks that the tag matches
the crate version and that CI and both validation workflows passed for the tagged commit,
attaches the shared libraries to a GitHub release, and publishes wheels to PyPI.

PyPI publishing uses trusted publishing for the `spanner-adbc` project, with owner `fornwall`,
repository `spanner-adbc`, workflow `libraries.yml` and environment `pypi`.

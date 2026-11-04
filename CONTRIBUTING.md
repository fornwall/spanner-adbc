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
run `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features`. For changes to `scripts/`
or the dependency pins, run `python3 -m unittest discover -s scripts/tests`. CI runs these along with the Python tests,
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

`Cargo.toml` pins the Google Cloud crates to one google-cloud-rust revision. This includes
`spanner-grpc-mock`, which the mock-server tests use and which isn't published.

`adbc_core`, `adbc_ffi` and `adbc_driver_manager` use one crates.io release. The C++ validation
suite (`ARROW_ADBC_TAG` in `adbc-validation/CMakeLists.txt`) uses the matching
`apache-arrow-adbc-N` tag, where crate version `0.N.x` comes from release `N`, and
`src/ffi/abi.rs` transcribes that tag's `adbc.h`.

The `arrow-*` crates use crates.io releases starting at 60.1, which supplies the
`FFI_ArrowArrayStream` constructor and accessors that the C ABI needs.

The git pin prevents publishing to crates.io, so the release configuration sets
`publish = false`. Rust users of the crate have to match it; see the
[README](README.md#rust).

`scripts/tests/test_pins.py` checks that the Google Cloud crates share a revision and that the
ADBC crates and tag share a release. Before removing the pin, check that a registry release has
the APIs the driver uses.

### Registry migration checklist

When moving the Google Cloud crates to crates.io, update these together:

- The affected dependencies and dev-dependencies in `Cargo.toml`.
- The `allow-git` entry in `deny.toml`, once the repository has no git dependencies left.
- The dependency notes in `README.md`, this file and [AGENTS.md](AGENTS.md#dependencies).

Also check that `spanner-grpc-mock` is published and compatible. Keep `publish = false` until
every dependency can come from the registry.

When updating ADBC, update `ARROW_ADBC_TAG` and the definitions in `src/ffi/abi.rs` with the
crates, and keep Arrow's version ranges within `adbc_core`'s.

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

The pre-release hook runs the fmt, clippy, build and test checks, and cargo-release also updates
the Python package version in `python/spanner_adbc/_version.py`.

Pushing a `vX.Y.Z` tag starts `.github/workflows/libraries.yml`. It checks that the tag matches
the crate and Python package versions and that CI and both validation workflows passed for the
tagged commit, builds the shared libraries with the `strip-logging` feature, attaches them to a
GitHub release, and publishes wheels to PyPI.

PyPI publishing uses trusted publishing for the `spanner-adbc` project, with owner `fornwall`,
repository `spanner-adbc`, workflow `libraries.yml` and environment `pypi`.

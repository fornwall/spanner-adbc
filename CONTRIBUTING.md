# Contributing

Build and check changes with the commands below. See [docs/testing.md](docs/testing.md) for
backend setup, test suites and CI coverage.

## Building and testing

Use the toolchain pinned in `rust-toolchain.toml`. From the repository root:

```sh
cargo build
cargo test
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --all --check
scripts/with-emulator.sh cargo test  # requires Docker
```

Tests that need a Spanner target self-skip when none is configured. The emulator command runs
them against a disposable database; Toxiproxy fault tests and external validation harnesses
have separate commands in the testing guide. Rust integration tests can also use
`SPANNER_GCP_DATABASE=project.instance.database` with Application Default Credentials.

Before pushing code changes, run formatting, clippy, tests and emulator tests. CI also checks
rustdoc, the build without default features, Python integration, workflows and dependencies.
Keep pull requests focused and explain non-obvious constraints in nearby comments.

## Coding conventions

- Keep unit tests in sibling `src/<module>/tests.rs` files, not inline.
- Explain non-obvious reasons in comments; keep behavioral detail in nearby rustdoc.
  Omit review-ticket IDs and narratives about removed code; history belongs in git.
- Keep SQL lexing, splitting, quoting and parameter extraction in `src/sql.rs`.
- Reuse the driver's Tokio runtime to bridge synchronous ADBC methods to the async client.
  The C ABI uses one process-wide driver via `shared_driver()` in `src/ffi/database.rs`.
- Confine `unsafe` to `src/ffi/`; preserve the `--no-default-features` build without unsafe code.
- Define option keys as `pub const OPTION_*` in `src/lib.rs`; reuse constants instead of literals
  and document each option in [docs/options.md](docs/options.md).
- DDL executes immediately through the admin API, independently of manual transactions:
  rollback cannot undo it, and DDL runs before buffered DML. See [transactions](docs/transactions.md).
- Internal metadata reads inherit priority, replica selection, retry bounds and RPC timeouts,
  but remain untagged; request/transaction tags attribute user statements.

## Versioning

Use `MAJOR.MINOR.PATCH`: major for incompatible changes, minor for compatible functionality,
and patch for compatible fixes. Before 1.0, incompatible API changes may occur in minor releases.

## Releasing

Use [`cargo-release`](https://github.com/crate-ci/cargo-release), configured in `Cargo.toml`;
do not bump versions or create release commits/tags manually. Install it with
`cargo install cargo-release`; releases require push access to `main`.

```sh
cargo release patch            # dry run
cargo release patch --execute  # version, commit, tag and push
```

The pre-release hook runs fmt, clippy and `cargo test`. Cargo-release also updates the Python
fallback version. Its `publish = false` setting disables crates.io publishing while git pins remain.

A `vX.Y.Z` tag triggers `.github/workflows/libraries.yml`. Publishing requires a matching crate
version and successful CI for the tagged commit. The workflow attaches shared libraries to the
GitHub Release, then publishes Python wheels to PyPI through trusted publishing.

The PyPI project is `spanner-adbc`; the Python import package is `spanner_adbc`.
Its PyPI trusted publisher uses owner `fornwall`, repository `spanner-adbc`, workflow
`libraries.yml`, and GitHub environment `pypi`.

## Dependency pins

`Cargo.toml` pins the Google Cloud family and `adbc_core`/`adbc_ffi`/`adbc_driver_manager` to git,
and patches the `arrow-*` crates to arrow-rs `main` through `[patch.crates-io]`.
The Google Cloud pin keeps the client and unpublished `spanner-grpc-mock` on one revision.
ADBC deliberately tracks upstream `main`, including the C++ validation suite; Arrow supplies
the `FFI_ArrowArrayStream` constructor and accessors that arrow-rs 60 lacks.
Each family independently prevents crates.io publishing.

Downstream Rust projects must match the `adbc_core` source and revision. With the default `ffi`
feature enabled, they must also copy all four Arrow overrides from this repository's
`[patch.crates-io]` section into their workspace root `Cargo.toml`. Cargo only applies patches
from the top-level workspace; adding this crate as a git dependency does not inherit them.
Without the overrides, registry Arrow 60 lacks the stream constructor and callback accessors
used by the FFI implementation.

Read revisions from `Cargo.toml`/`Cargo.lock`. Keep each family on one revision, including the
ADBC C++ validation header. Check published API availability before removing a pin.

### Registry migration checklist

When moving a dependency family to crates.io, update these locations together:

- All affected dependencies and dev-dependencies in `Cargo.toml`; downstream `adbc_core` must match.
- `ARROW_ADBC_TAG` in `adbc-validation/CMakeLists.txt` and the definitions in `src/ffi/abi.rs`
  when changing the ADBC revision.
- `deny.toml` allow-git entries once a repository has no remaining git dependencies.
- Dependency notes in `README.md`, this guide and [AGENTS.md](AGENTS.md#working-with-pinned-dependencies).

Keep release `publish = false` until all families permit publishing. Check registry availability
and publish compatibility of `spanner-grpc-mock` too, and revisit Arrow version ranges with `adbc_core`.

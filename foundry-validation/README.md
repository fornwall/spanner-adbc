# Foundry validation

Runs the [ADBC Driver Foundry](https://github.com/adbc-drivers/validation) type and feature suite
against the shared library through the ADBC driver manager. The suite's queries are adapted to
GoogleSQL. ADBC conformance is tested separately by the [C++ harness](../adbc-validation/README.md);
see [testing](../docs/testing.md) for the other suites.

## Running

```sh
scripts/run-foundry-validation.sh
scripts/run-foundry-validation.sh -k ingest  # extra arguments go to pytest
scripts/run-foundry-validation.sh --prepare  # build and install only
```

You need Python 3.13+ with pip (required by the
[pinned suite](https://github.com/adbc-drivers/validation/blob/68242ee13524e8f2b36038764e2f439d4cb49f33/pyproject.toml)),
Rust, git and curl, plus Docker for the emulator. `PYTHON` selects the interpreter. The
[script](../scripts/run-foundry-validation.sh) builds the driver, installs the pinned suite if
needed, sets up the target database and runs pytest. `ADBC_VALIDATION_REF` and
`ADBC_VALIDATION_REPO` select a different upstream revision or repository. When running pytest
directly, set `ADBC_SPANNER_URI`. The library defaults to `target/debug/`; set
`ADBC_SPANNER_LIBRARY` to use another build.

The target is chosen from `SPANNER_OMNI_ENDPOINT`, then `SPANNER_EMULATOR_HOST`, then
`SPANNER_GCP_DATABASE`; with none set, the script starts a disposable emulator. For Omni, set
`SPANNER_OMNI_ENDPOINT=http://localhost:15000` and provide the database
`projects/default/instances/default/databases/adbc-test`. A Cloud Spanner database must already
exist and is reached with ADC. The quirks expect the database to be called `adbc-test`, so
adjust them for a different name.

The [workflow](../.github/workflows/foundry-validation.yml) runs on pushes to `main` and pull
requests. Every case must pass or skip with a reason; there are no expected failures.

## Adaptations

- [`tests/spanner.py`](tests/spanner.py) declares capabilities, connection options, identifier
  quoting, `@pN` parameters and DDL overrides. Metadata tests expect catalog `adbc-test` and the
  default schema `""`. Primary key, foreign key and CHECK constraints, XDBC type names and
  nullability, named-schema metadata and ingest, and exact statistics are enabled. The
  connection fixture creates `validation_schema` for schema-scoped tests.
- [`queries/spanner/`](queries/spanner/) overrides the upstream bind, select, literal and ingest
  cases with native type names, typed parameters, explicit `INSERT` column lists and
  deterministic ordering. The `.txtcase` files hold the expected schemas and skip reasons.
- [`tests/test_ingest.py`](tests/test_ingest.py) enables the long-value tests for string, large
  string, string view, binary, large binary and binary view. Its fixture applies each case's
  connection options to both ingest and readback, and restores the previous values afterwards.
  Other test modules reuse upstream classes and fixtures through [`tests/conftest.py`](tests/conftest.py).

Timestamp cases keep the upstream naive and timezone-aware inputs and expected instants, and
read back in UTC. Nanosecond cases keep nanosecond precision; lower-precision cases use the
microsecond option to cover years 1 to 9999. Expected integer timestamps are rescaled, and SQL
timestamp literals state UTC explicitly.

Type adaptations keep the original input schemas and values and assert what Spanner returns:
narrow integers widen to `INT64`, fixed-size binary comes back as variable-width binary,
`float16` as `FLOAT32`, and decimals (including negative scales) as `Decimal128(38,9)`. No schema
or value comparison is disabled.

The [skip list](skip_baseline.txt) contains only TIME-of-day cases and features the driver
lacks: temporary tables, cross-catalog operations, changing the current catalog or schema, and
UNIQUE table constraints.

## Skip baseline

CI sets two variables read by `tests/conftest.py`:

- `FOUNDRY_VALIDATION_REQUIRE_PASSES=1` fails a run in which no test passed.
- `FOUNDRY_VALIDATION_CHECK_SKIP_BASELINE=1` compares the skipped test IDs with
  `skip_baseline.txt` and fails on skips missing from the file and on entries that no longer
  skip. Reason annotations are ignored. Omni runs use the same list, with their backend label
  read as `emulator`.

The comparison is skipped for `-k` and `-m` runs, so check or update the baseline with a full
run. A case skipped in its `.txtcase` file never runs, so the check can't notice when it would
pass; review skip reasons when the driver gains features.

When a change to the skip set is expected, regenerate the file and review the diff:

```sh
FOUNDRY_UPDATE_SKIP_BASELINE=1 scripts/run-foundry-validation.sh
```

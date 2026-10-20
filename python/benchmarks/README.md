# Benchmarks

These scripts compare reading into Polars through this driver with the official
`google-cloud-spanner` Python client, on the Spanner emulator. Run them from the repository root.
They need `SPANNER_EMULATOR_HOST`, which `scripts/with-emulator.sh` sets while it runs a
temporary emulator in Docker.

## Setup (Linux)

```sh
python3 -m venv .venv
. .venv/bin/activate
pip install 'adbc-driver-manager[dbapi]' polars google-cloud-spanner
cargo build --release --features strip-logging
cp target/release/libspanner_adbc.so python/spanner_adbc/
```

The feature matches the released library. Copy the library again after each rebuild.

## End-to-end comparison

`benchmark_polars.py` reads a table of mixed column types into Polars and computes a column
mean. It checks that both clients return the same row count and mean, then reports the minimum
and median times after a warm-up run. Table setup isn't timed.

```sh
PYTHONPATH=python scripts/with-emulator.sh \
  .venv/bin/python python/benchmarks/benchmark_polars.py --rows 1000000 --repeat 3
```

## Stage timings

`profile_stages.py` times query execution, fetching, DataFrame construction and aggregation
separately, for several values of `spanner.rows_per_batch`. `PROFILE_ROWS` sets the row count
(default 200,000).

```sh
PYTHONPATH=python scripts/with-emulator.sh \
  .venv/bin/python python/benchmarks/profile_stages.py
```

## CPU profiling

`perf_read.py` creates a table, then reads it as Arrow repeatedly, without Polars or the
official client. `PERF_ROWS` and `PERF_LOOPS` default to 500,000 and 8. To profile with frame
pointers, rebuild and stage the library before recording:

```sh
RUSTFLAGS="-C force-frame-pointers=yes" cargo build --profile profiling --features strip-logging
cp target/profiling/libspanner_adbc.so python/spanner_adbc/
PYTHONPATH=python scripts/with-emulator.sh \
  perf record -F 199 --call-graph fp -o perf.data -- \
  .venv/bin/python python/benchmarks/perf_read.py
perf report -i perf.data
```

The recording includes setup, so look at the read loop when inspecting samples. When sharing
results, include the driver revision, dependency versions, row count and machine. Emulator
timings say little about throughput on Cloud Spanner.

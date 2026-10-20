#!/usr/bin/env python3
"""Compare ADBC and google-cloud-spanner reads into Polars on the emulator.

Both clients read the same mixed-type table and compute a column mean. ADBC
passes an Arrow table to ``pl.from_arrow``; google-cloud-spanner materializes
Python rows for ``pl.DataFrame``. Timings include connection setup, reading,
DataFrame construction and aggregation, but exclude table population.

Run from the repository root after following the benchmark README's setup:

    PYTHONPATH=python scripts/with-emulator.sh <venv>/bin/python python/benchmarks/benchmark_polars.py

Options: ``--rows N`` (default 100000), ``--repeat K`` (default 5),
``--batch B`` (ingest batch size for setup, default 20000).
"""

from __future__ import annotations

import argparse
import os
import statistics
import sys
import time

# Reuse the integration tests' emulator instance and database.
PROJECT = "test-project"
INSTANCE = "test-instance"
DATABASE = "adbc-test"
TABLE = "BenchPolars"

COLUMNS = "id, value, quantity, category, active"
DDL = (
    f"CREATE TABLE {TABLE} ("
    "  id INT64 NOT NULL,"
    "  value FLOAT64,"
    "  quantity INT64,"
    "  category STRING(64),"
    "  active BOOL,"
    ") PRIMARY KEY (id)"
)


def ensure_database() -> str:
    """Create the instance and database if absent; return the database path."""
    from google.api_core.exceptions import AlreadyExists
    from google.cloud import spanner

    instance = spanner.Client(project=PROJECT).instance(
        INSTANCE,
        configuration_name=f"projects/{PROJECT}/instanceConfigs/emulator-config",
        node_count=1,
    )
    database = instance.database(DATABASE)
    for resource in (instance, database):
        try:
            resource.create().result(timeout=60)
        except AlreadyExists:
            pass
    return database.name


def build_data(n: int):
    import pyarrow as pa

    ids = list(range(n))
    value = [((i * 7) % 10_000) / 100.0 for i in ids]
    quantity = [i % 500 for i in ids]
    category = [f"cat-{i % 20}" for i in ids]
    active = [(i % 2) == 0 for i in ids]
    return pa.table(
        {
            "id": pa.array(ids, type=pa.int64()),
            "value": pa.array(value, type=pa.float64()),
            "quantity": pa.array(quantity, type=pa.int64()),
            "category": pa.array(category, type=pa.string()),
            "active": pa.array(active, type=pa.bool_()),
        }
    )


def setup_table(database: str, n: int, batch: int) -> None:
    import spanner_adbc.dbapi as spanner_adbc
    from spanner_adbc import DatabaseOptions

    data = build_data(n)
    conn = spanner_adbc.connect(
        db_kwargs={
            DatabaseOptions.URI.value: f"spanner:///{database}",
            DatabaseOptions.EMULATOR.value: "true",
        },
        autocommit=True,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {TABLE}")
            cur.execute(DDL)
        # Slices are only for progress output; the driver splits commits itself.
        loaded = 0
        for start in range(0, n, batch):
            slice_ = data.slice(start, min(batch, n - start))
            with conn.cursor() as cur:
                cur.adbc_ingest(TABLE, slice_, mode="append")
            loaded += slice_.num_rows
            print(f"    populated {loaded}/{n} rows", end="\r", flush=True)
        print(f"    populated {loaded}/{n} rows")
    finally:
        conn.close()


def run_adbc(database: str):
    """Read an Arrow table through ADBC and return the Polars mean and row count."""
    import polars as pl

    import spanner_adbc.dbapi as spanner_adbc
    from spanner_adbc import DatabaseOptions

    conn = spanner_adbc.connect(
        db_kwargs={
            DatabaseOptions.URI.value: f"spanner:///{database}",
            DatabaseOptions.EMULATOR.value: "true",
        },
        autocommit=True,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT {COLUMNS} FROM {TABLE}")
            table = cur.fetch_arrow_table()
        df = pl.from_arrow(table)
        return df["value"].mean(), df.height
    finally:
        conn.close()


def run_official(database: str):
    """Read rows through google-cloud-spanner and return the Polars mean and row count."""
    import polars as pl
    from google.cloud import spanner

    schema = {
        "id": pl.Int64,
        "value": pl.Float64,
        "quantity": pl.Int64,
        "category": pl.Utf8,
        "active": pl.Boolean,
    }
    # SPANNER_EMULATOR_HOST makes the client use anonymous plaintext gRPC.
    _, project, _, instance_id, _, database_id = database.split("/")
    client = spanner.Client(project=project)
    instance = client.instance(instance_id)
    db = instance.database(database_id)
    with db.snapshot() as snapshot:
        result = snapshot.execute_sql(f"SELECT {COLUMNS} FROM {TABLE}")
        rows = list(result)
    df = pl.DataFrame(rows, schema=schema, orient="row")
    return df["value"].mean(), df.height


def timeit(fn, repeat: int):
    # Exclude the first run, which also loads imports and initializes client state.
    result = fn()
    times = []
    for _ in range(repeat):
        t0 = time.perf_counter()
        fn()
        times.append(time.perf_counter() - t0)
    return result, times


def summarize(name: str, times, rows: int) -> dict:
    best = min(times)
    med = statistics.median(times)
    return {
        "name": name,
        "min": best,
        "median": med,
        "rows_per_s": rows / best,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--repeat", type=int, default=5)
    parser.add_argument("--batch", type=int, default=20_000)
    args = parser.parse_args()

    if not os.environ.get("SPANNER_EMULATOR_HOST"):
        print(
            "SPANNER_EMULATOR_HOST is not set. Run under scripts/with-emulator.sh, e.g.\n"
            "  scripts/with-emulator.sh <venv>/bin/python "
            "python/benchmarks/benchmark_polars.py",
            file=sys.stderr,
        )
        return 2

    print(f">> emulator: {os.environ['SPANNER_EMULATOR_HOST']}")
    database = ensure_database()
    print(f">> database: {database}")

    print(f">> setup: creating {TABLE} and loading {args.rows} rows")
    setup_table(database, args.rows, args.batch)

    print(f">> benchmarking (repeat={args.repeat}): download whole table, Polars mean(value)")
    results = []
    for name, fn in (("adbc", run_adbc), ("official", run_official)):
        print(f"    running: {name} ...", flush=True)
        (mean, rows), times = timeit(lambda fn=fn: fn(database), args.repeat)
        results.append((summarize(name, times, rows), mean, rows))

    (_, mean_a, rows_a), (_, mean_b, rows_b) = results
    assert rows_a == rows_b == args.rows, (rows_a, rows_b, args.rows)
    assert abs(mean_a - mean_b) < 1e-9, (mean_a, mean_b)

    print()
    print(f"  rows            : {rows_a}")
    print(f"  mean(value)     : {mean_a:.6f}  (both engines agree)")
    print()
    header = f"  {'engine':<10}{'min (s)':>12}{'median (s)':>14}{'rows/s':>14}"
    print(header)
    print("  " + "-" * (len(header) - 2))
    summaries = [s for s, _, _ in results]
    for s in summaries:
        print(
            f"  {s['name']:<10}{s['min']:>12.4f}{s['median']:>14.4f}{s['rows_per_s']:>14,.0f}"
        )
    by_name = {s["name"]: s for s in summaries}
    speedup = by_name["official"]["min"] / by_name["adbc"]["min"]
    print()
    print(f"  official / ADBC minimum elapsed time: {speedup:.2f}x")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

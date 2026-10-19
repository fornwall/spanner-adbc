#!/usr/bin/env python3
"""Repeat ADBC reads for CPU profiling with ``perf record``.

Populate a table once, then read it as Arrow repeatedly. The recording includes
setup and fetching; use the printed read-loop marker to identify the latter.

    PYTHONPATH=python scripts/with-emulator.sh \
      perf record --call-graph dwarf -o perf.data -- \
      <venv>/bin/python python/benchmarks/perf_read.py
"""

from __future__ import annotations

import os
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))
import benchmark_polars as bp  # noqa: E402

N = int(os.environ.get("PERF_ROWS", "500000"))
LOOPS = int(os.environ.get("PERF_LOOPS", "8"))


def main() -> int:
    if not os.environ.get("SPANNER_EMULATOR_HOST"):
        print("SPANNER_EMULATOR_HOST not set", file=sys.stderr)
        return 2
    database = bp.ensure_database()
    print(f">> setup {N} rows", flush=True)
    bp.setup_table(database, N, 20000)

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
        print(f">> {LOOPS} read loops of {N} rows (profile window)", flush=True)
        t0 = time.perf_counter()
        total = 0
        for _ in range(LOOPS):
            with conn.cursor() as cur:
                cur.execute(f"SELECT {bp.COLUMNS} FROM {bp.TABLE}")
                table = cur.fetch_arrow_table()
            total += table.num_rows
        dt = time.perf_counter() - t0
        print(f">> read {total} rows in {dt:.2f}s ({total / dt:,.0f} rows/s)", flush=True)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

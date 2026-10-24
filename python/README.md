# spanner-adbc

> Experimental: this package is AI-generated and has not yet seen real-world use.

[![PyPI version](https://img.shields.io/pypi/v/spanner-adbc.svg)](https://pypi.org/project/spanner-adbc/)
[![Python versions](https://img.shields.io/pypi/pyversions/spanner-adbc.svg)](https://pypi.org/project/spanner-adbc/)
[![Wheel](https://img.shields.io/pypi/wheel/spanner-adbc.svg)](https://pypi.org/project/spanner-adbc/#files)
[![License](https://img.shields.io/pypi/l/spanner-adbc.svg)](https://github.com/fornwall/spanner-adbc/blob/main/LICENSE)
[![Build](https://github.com/fornwall/spanner-adbc/actions/workflows/libraries.yml/badge.svg)](https://github.com/fornwall/spanner-adbc/actions/workflows/libraries.yml)

A Python [ADBC](https://arrow.apache.org/adbc/) driver for Google Cloud Spanner and Spanner Omni.
It provides a [DBAPI 2.0](https://peps.python.org/pep-0249/) interface that returns
[Apache Arrow](https://arrow.apache.org/) data for pandas, Polars, DuckDB or PyArrow. Only
GoogleSQL databases are supported, not the PostgreSQL dialect.

## Install

Requires Python 3.11 or later and adbc-driver-manager 1.8 or later, which pip installs. Wheels
bundle the native driver; see [supported platforms](#supported-platforms).

```sh
pip install "spanner-adbc[dbapi]"  # includes PyArrow and pandas
```

Plain `spanner-adbc` installs only the low-level ADBC API.

## Quickstart

Connect to an existing GoogleSQL database using [Application Default Credentials][adc]. This
query needs no tables:

```python
import spanner_adbc.dbapi as spanner

uri = "spanner:///projects/my-project/instances/my-instance/databases/my-db"
with spanner.connect(uri) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT @name AS name", {"name": "Alice"})
        print(cur.fetch_arrow_table())
```

Parameters use the GoogleSQL `@name` syntax, and `?` placeholders are rejected. A dict binds
parameters by name. A tuple binds them in the order they first appear in the SQL.

The later table examples use this sample table:

```sql
CREATE TABLE Singers (
    SingerId INT64 NOT NULL,
    FirstName STRING(MAX)
) PRIMARY KEY (SingerId);
INSERT INTO Singers (SingerId, FirstName) VALUES (1, 'Alice'), (2, 'Bob');
```

Run the `CREATE TABLE` and `INSERT` separately with `autocommit=True`; the driver doesn't accept
DDL and DML in the same call.

## Connecting

The driver uses [Application Default Credentials][adc] unless `db_kwargs` sets other
credentials. The option enums name every key, and raw strings such as `"spanner.endpoint"` work
too:

```python docs-test: skip
import spanner_adbc.dbapi as spanner
from spanner_adbc import DatabaseOptions

with spanner.connect(
    "spanner:///projects/p/instances/i/databases/d",
    {DatabaseOptions.KEYFILE: "/path/to/service-account.json"},
) as conn:
    ...
```

To use the [emulator][emulator], set `SPANNER_EMULATOR_HOST`, or set `DatabaseOptions.EMULATOR`
to `"true"` and `DatabaseOptions.ENDPOINT` to its `host:port`. For Spanner Omni, set
`DatabaseOptions.INSTANCE_TYPE` to `"omni"` and `DatabaseOptions.ENDPOINT` to the server.
[Authentication][auth] covers the credential options and when the driver connects anonymously.

Connections don't carry over into a forked process. With `multiprocessing`'s `fork` start
method, the default on Linux before Python 3.14, connect in each worker; see
[forked processes][fork].

[adc]: https://docs.cloud.google.com/docs/authentication/application-default-credentials
[emulator]: https://docs.cloud.google.com/spanner/docs/emulator
[auth]: https://github.com/fornwall/spanner-adbc/blob/main/README.md#authentication
[fork]: https://github.com/fornwall/spanner-adbc/blob/main/docs/limitations.md#forked-processes

## Options

`connect(uri, db_kwargs, conn_kwargs, *, autocommit=False)` takes database and connection
options, and `conn.cursor(adbc_stmt_kwargs=...)` takes statement options. `DatabaseOptions`,
`ConnectionOptions` and `StatementOptions` list the keys. They are `StrEnum`s, so members work as
dictionary keys, but the typed driver manager getters, such as `get_option_int`, need `.value`.
Use string values in `db_kwargs` and `conn_kwargs` for compatibility with driver manager 1.8.
The `autocommit` keyword overrides `ConnectionOptions.AUTOCOMMIT` in `conn_kwargs`.
The [option reference][options] lists types, defaults and accepted values.

```python
import spanner_adbc.dbapi as spanner
from spanner_adbc import ConnectionOptions, StatementOptions

with spanner.connect(
    "spanner:///projects/p/instances/i/databases/d",
    conn_kwargs={ConnectionOptions.READ_STALENESS: "max:10s"},
    autocommit=True,
) as conn:
    with conn.cursor(adbc_stmt_kwargs={StatementOptions.ROWS_PER_BATCH: "1024"}) as cur:
        cur.execute("SELECT * FROM Singers")
```

[options]: https://github.com/fornwall/spanner-adbc/blob/main/docs/options.md

## Transactions

Following DBAPI, connections start with `autocommit=False`. Manual transactions differ from most
databases:

- A transaction runs either queries or writes, whichever comes first. Mixing them raises
  `ProgrammingError` until you commit or roll back. Queries share one snapshot, and there is no
  read-your-writes.
- DML is buffered and runs at `commit()`, so errors such as a duplicate key come from
  `commit()`, and `rowcount` is -1. A failed commit keeps the buffered writes. If a commit times
  out or loses its connection, it may already have applied; use `rollback()` to discard the
  buffer before deciding whether to retry the work.
- Closing the connection, including leaving its `with` block, discards uncommitted writes.
- DDL runs immediately, and rollback can't undo it.
- `THEN RETURN` and partitioned DML need `autocommit=True`.

```python
import spanner_adbc.dbapi as spanner

with spanner.connect("spanner:///projects/p/instances/i/databases/d") as conn:
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE AdbcExample (Id INT64 NOT NULL) PRIMARY KEY (Id)")
        cur.execute("INSERT INTO AdbcExample (Id) VALUES (1)")
    conn.commit()

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM AdbcExample")
        assert cur.fetchone() == (1,)
    conn.rollback()  # end the read snapshot

    with conn.cursor() as cur:
        cur.execute("DROP TABLE AdbcExample")
```

See [transactions](https://github.com/fornwall/spanner-adbc/blob/main/docs/transactions.md) for
the details.

## Errors

Errors are the PEP 249 exceptions that `adbc_driver_manager.dbapi` raises for each ADBC status,
also exported by `spanner_adbc.dbapi`. [Errors][errors] lists the status for each Spanner error
code, and `exc.details` includes the numeric code as `spanner.vendor_code`.

[errors]: https://github.com/fornwall/spanner-adbc/blob/main/docs/adbc.md#errors

## DataFrames

`fetch_arrow_table()` and `fetch_df()` load the whole result into memory, and
`fetch_record_batch()` streams it while the cursor is open. With Polars 2.0, `fetch_polars()`
returns a struct `Series` rather than a DataFrame, so use `pl.from_arrow(cur.fetch_arrow_table())`
or `pl.read_database()` instead.

```python
import spanner_adbc.dbapi as spanner

with spanner.connect("spanner:///projects/p/instances/i/databases/d") as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT SingerId, FirstName FROM Singers")
        with cur.fetch_record_batch() as reader:
            for batch in reader:
                print(batch.num_rows)
```

pandas and Polars accept the connection directly:

```python
import pandas as pd
import polars as pl
import spanner_adbc.dbapi as spanner

with spanner.connect("spanner:///projects/p/instances/i/databases/d", autocommit=True) as conn:
    frame = pd.read_sql("SELECT SingerId, FirstName FROM Singers", conn)
    frame.to_sql("SingersCopy", conn, if_exists="append", index=False)

    df = pl.read_database("SELECT SingerId, FirstName FROM Singers", connection=conn)
    df.write_database("SingersCopy", connection=conn, if_table_exists="append")
```

pandas can't pass `params` to an ADBC connection, so bind parameters with `cur.execute()` and
`cur.fetch_df()` instead. `pd.read_sql_table()` and `to_sql(if_exists="replace")` fail because
pandas quotes table names with `"`, which GoogleSQL reads as a string. Polars'
`read_database_uri()` doesn't work either, since it looks for a module named
`adbc_driver_spanner`. Both libraries write through [bulk ingest](#bulk-ingest), so use
autocommit for large writes. In manual mode, end a read with `conn.rollback()` before writing.

Results follow the [type mapping][types]. Polars 2.0 reads JSON columns as
`Extension('arrow.json', String)`. Spanner allows duplicate `STRUCT` field names: pandas and
DuckDB keep only the last field of each name, and Polars raises `DuplicateError` when unnesting
such a struct. pyarrow and Polars infer `List(Null)` for `[]` and `[None]`, which bind
[untyped][bound].

DuckDB can query fetched Arrow data in-process:

```python
import duckdb
import spanner_adbc.dbapi as spanner

with spanner.connect("spanner:///projects/p/instances/i/databases/d") as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT SingerId, FirstName FROM Singers")
        singers = cur.fetch_arrow_table()

print(duckdb.sql("SELECT COUNT(*) FROM singers").fetchall())
```

[types]: https://github.com/fornwall/spanner-adbc/blob/main/README.md#type-mapping
[bound]: https://github.com/fornwall/spanner-adbc/blob/main/docs/adbc.md#bound-types

## Bulk ingest

`cur.adbc_ingest(table, data, mode=...)` inserts Arrow data, or anything that exports the Arrow
PyCapsule interface, such as a Polars DataFrame:

```python
import pyarrow as pa
import spanner_adbc.dbapi as spanner

data = pa.table({"SingerId": [10, 11], "FirstName": ["Carol", "Dave"]})

with spanner.connect("spanner:///projects/p/instances/i/databases/d", autocommit=True) as conn:
    with conn.cursor() as cur:
        rows = cur.adbc_ingest("Singers", data, mode="append")
```

The modes are `create` (the default), `append`, `create_append` and `replace`. Tables that ingest
creates have Spanner's hidden `rowid` as primary key, so create the table yourself to choose the
key.

With `autocommit=True`, ingest commits in chunks, so a failed load can leave rows behind. In
manual mode, ingest returns -1 and the rows apply at `commit()` in one commit, which Spanner caps
at 80,000 mutations: one per cell, plus index entries. Use autocommit for large loads, including
pandas' `to_sql()` and Polars' `write_database()`.

## Partitioned reads

`adbc_execute_partitions()` splits a query into partitions that any connection can read with
`adbc_read_partition()`, in parallel if you like. On Cloud Spanner, set
`StatementOptions.DATA_BOOST` to `"true"` on the partitioning cursor to read them on
[Data Boost](https://docs.cloud.google.com/spanner/docs/databoost/databoost-overview).

```python
import spanner_adbc.dbapi as spanner

with spanner.connect("spanner:///projects/p/instances/i/databases/d") as conn:
    with conn.cursor() as cur:
        partitions, schema = cur.adbc_execute_partitions("SELECT SingerId FROM Singers")

    for partition in partitions:
        with conn.cursor() as cur:
            cur.adbc_read_partition(partition)
            table = cur.fetch_arrow_table()
```

Spanner only partitions some queries, such as simple scans; see
[reading data in parallel](https://docs.cloud.google.com/spanner/docs/reads#read_data_in_parallel).
Only read partitions from trusted sources: they aren't authenticated, as the
[limitations](https://github.com/fornwall/spanner-adbc/blob/main/README.md#limitations) explain.

## Loading by name

`spanner.connect()` loads the bundled library directly. To load the driver by name through
`adbc_driver_manager` 1.9.0 or later, install an [ADBC driver manifest][manifests]:

```sh
python -m spanner_adbc.manifest install  # or spanner-adbc-install-manifest
```

```python docs-test: skip
import adbc_driver_manager.dbapi

with adbc_driver_manager.dbapi.connect(
    driver="spanner", uri="spanner:///projects/p/instances/i/databases/d"
) as conn:
    ...
```

From version 1.11.0, `driver` can be left out, and the driver manager picks the driver from the
URI scheme. The manifest goes in `<sys.prefix>/etc/adbc/drivers` in a virtual environment and in
the user configuration directory otherwise, and `python -m spanner_adbc.manifest path` prints
where. It records the library's absolute path, so install it again after upgrading or moving the
environment. Use `install --dir` to write it into a directory on `ADBC_DRIVER_PATH` instead.

[manifests]: https://arrow.apache.org/adbc/current/format/driver_manifests.html

## Supported platforms

| Platform | Wheel tag |
| --- | --- |
| Linux x86-64 and aarch64, glibc 2.35+ | `manylinux_2_35` |
| Linux x86-64 and aarch64, musl 1.2+ | `musllinux_1_2` |
| macOS 11+ arm64 | `macosx_11_0_arm64` |
| macOS 10.15+ x86-64 | `macosx_10_15_x86_64` |
| Windows x86-64 and arm64 | `win_amd64`, `win_arm64` |

On other systems, build the library and package it yourself. The Python build doesn't compile
Rust; it packages the one library staged in `python/spanner_adbc/`, such as
`libspanner_adbc.so` on Linux:

```sh
cargo build --release --features strip-logging
cp target/release/libspanner_adbc.so python/spanner_adbc/
pip install './python[dbapi]'
```

The release wheels use the `strip-logging` feature too; without it, the library is larger but
behaves the same.

See [CONTRIBUTING.md](https://github.com/fornwall/spanner-adbc/blob/main/CONTRIBUTING.md) for
development and releases.

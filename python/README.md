# spanner-adbc

> **Experimental.** AI-generated and not yet validated in production use.

[![PyPI version](https://img.shields.io/pypi/v/spanner-adbc.svg)](https://pypi.org/project/spanner-adbc/)
[![Python versions](https://img.shields.io/pypi/pyversions/spanner-adbc.svg)](https://pypi.org/project/spanner-adbc/)
[![Wheel](https://img.shields.io/pypi/wheel/spanner-adbc.svg)](https://pypi.org/project/spanner-adbc/#files)
[![License](https://img.shields.io/pypi/l/spanner-adbc.svg)](https://github.com/fornwall/spanner-adbc/blob/main/LICENSE)
[![Build](https://github.com/fornwall/spanner-adbc/actions/workflows/libraries.yml/badge.svg)](https://github.com/fornwall/spanner-adbc/actions/workflows/libraries.yml)

A Python [ADBC](https://arrow.apache.org/adbc/) driver for Google Cloud Spanner. Query Spanner
through [DBAPI 2.0](https://peps.python.org/pep-0249/) and get [Apache Arrow](https://arrow.apache.org/)
results for pandas, Polars, DuckDB or PyArrow. Only GoogleSQL databases are supported, not the
PostgreSQL dialect.

## Install

Requires Python 3.11 or later. Wheels include the native driver; see
[supported platforms](#supported-platforms).

```sh
pip install "spanner-adbc[dbapi]"  # includes PyArrow and pandas
```

Install plain `spanner-adbc` for the low-level ADBC API without the DataFrame dependencies. The
import name is `spanner_adbc`.

## Quickstart

This example assumes an existing `Singers` table with `SingerId` and `FirstName` columns.

```python
import spanner_adbc.dbapi as spanner
from spanner_adbc import DatabaseOptions

with spanner.connect(
    db_kwargs={DatabaseOptions.URI: "spanner:///projects/my-project/instances/my-instance/databases/my-db"},
) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT SingerId, FirstName FROM Singers")
        df = cur.fetch_df()          # -> pandas.DataFrame
```

Queries use GoogleSQL `@name` parameters; `?` placeholders don't work. Pass the values as a
tuple, in the order the parameters first appear, rather than formatting them into the SQL:

```python
import spanner_adbc.dbapi as spanner
from spanner_adbc import DatabaseOptions

with spanner.connect(
    db_kwargs={DatabaseOptions.URI: "spanner:///projects/p/instances/i/databases/d"},
) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT @name AS name, @id AS id", ("Alice", 42))
        assert cur.fetchone() == ("Alice", 42)
```

`fetchone()` and `fetchall()` return Python tuples. The Arrow and DataFrame helpers are shown
[below](#working-with-dataframes).

## Driver manifest (`driver="spanner"`)

`spanner.connect()` loads the bundled library directly. To load the driver by name through the
generic `adbc_driver_manager` (version 1.8.0 or later), install an
[ADBC driver manifest][manifests]:

```sh
pip install --upgrade adbc-driver-manager
python -m spanner_adbc.manifest install
```

`spanner-adbc-install-manifest` does the same.

```python docs-test: skip
import adbc_driver_manager.dbapi

with adbc_driver_manager.dbapi.connect(
    driver="spanner",
    uri="spanner:///projects/my-project/instances/my-instance/databases/my-db",
) as conn:
    ...
```

Recent driver managers also infer `spanner` from the URI scheme if you leave out `driver`.

The manifest records the library's absolute path, so re-run the installer after upgrading,
reinstalling or moving the environment. It goes to `<sys.prefix>/etc/adbc/drivers` in a
virtual environment and to the platform's user configuration directory otherwise;
`python -m spanner_adbc.manifest path` prints it. Use `install --dir /path/to/drivers` to
install into a directory on `ADBC_DRIVER_PATH`. For a standalone shared library, edit the
`Driver.shared` paths in the repository's [spanner.toml][manifest-file].

[manifests]: https://arrow.apache.org/adbc/current/format/driver_manifests.html
[manifest-file]: https://github.com/fornwall/spanner-adbc/blob/main/spanner.toml

## Authentication

Without credential options, the driver connects to Cloud Spanner with
[Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials):
run `gcloud auth application-default login` locally, set `GOOGLE_APPLICATION_CREDENTIALS`, or
use an attached service account on Google Cloud.

To pass credentials explicitly, add one of these to `db_kwargs`:

| Option | Value |
| --- | --- |
| `DatabaseOptions.KEYFILE` | Path to a credential JSON file |
| `DatabaseOptions.KEYFILE_JSON` | Credential JSON contents |
| `DatabaseOptions.ACCESS_TOKEN` | OAuth bearer token, never refreshed |
| `DatabaseOptions.IMPERSONATE_TARGET_PRINCIPAL` | Service account to impersonate, using ADC or the explicit credentials |

`ACCESS_TOKEN` can't be combined with `KEYFILE`, `KEYFILE_JSON` or impersonation. The
[option reference][options] lists scopes and the other authentication settings.

For the [Spanner emulator](https://cloud.google.com/spanner/docs/emulator), connect anonymously:

```python docs-test: skip
import spanner_adbc.dbapi as spanner
from spanner_adbc import DatabaseOptions

with spanner.connect(db_kwargs={
    DatabaseOptions.URI: "spanner:///projects/p/instances/i/databases/d",
    DatabaseOptions.ENDPOINT: "localhost:9010",
    DatabaseOptions.EMULATOR: "true",
}) as conn:
    ...
```

In emulator mode the driver ignores ambient ADC and rejects explicit credential options and
`DatabaseOptions.QUOTA_PROJECT`.

For Spanner Omni, set `DatabaseOptions.INSTANCE_TYPE` to `"omni"` and `DatabaseOptions.ENDPOINT`
to the server, such as `"http://localhost:15000"`, and use
`spanner:///projects/default/instances/default/databases/<database>` as the URI. Leave
`DatabaseOptions.EMULATOR` unset. Omni and plaintext endpoints (any scheme but `https://`, or a
bare `host:port`) connect anonymously unless you pass explicit credentials, and reject
`QUOTA_PROJECT` without them. Explicit credentials sent to a plaintext endpoint are unencrypted.

## Options

| `connect()` argument | Purpose |
| --- | --- |
| `db_kwargs` | Database options, including the required `uri` |
| `conn_kwargs` | Connection options |
| `autocommit` | `False` by default; see [transactions](#transactions) |

Option keys are available as the `DatabaseOptions`, `ConnectionOptions` and `StatementOptions`
enums, or you can use the raw strings. The enums are `StrEnum`s, so members work as dictionary
keys. Pass `.value` to the typed driver manager getters, such as `get_option_int`, which need
a plain `str`. The [option reference][options] lists types, defaults and accepted values. Set
cursor options with `conn.cursor(adbc_stmt_kwargs={...})` or
`cur.adbc_statement.set_options(**{...})`.

[options]: https://github.com/fornwall/spanner-adbc/blob/main/docs/options.md

```python
import spanner_adbc.dbapi as spanner
from spanner_adbc import ConnectionOptions, DatabaseOptions, StatementOptions

with spanner.connect(
    db_kwargs={DatabaseOptions.URI: "spanner:///projects/p/instances/i/databases/d"},
    conn_kwargs={ConnectionOptions.READ_STALENESS: "max:10s"},
    autocommit=True,  # bounded staleness on a single-use read
) as conn:
    with conn.cursor(
        adbc_stmt_kwargs={StatementOptions.ROWS_PER_BATCH: "1024"}
    ) as cur:
        cur.execute("SELECT * FROM Singers")
```

In manual query transactions, `max:<duration>` and `min:<timestamp>` bounds become exact
staleness and a fixed read timestamp. Set `ConnectionOptions.READONLY` to `"true"` to reject
DML, DDL and ingest; rollback still works.

## Transactions

DBAPI defaults to `autocommit=False`. A manual transaction runs either queries or writes,
whichever comes first. Mixing the two raises `spanner.ProgrammingError` until you commit or
roll back. Queries share a snapshot. DML is buffered until `conn.commit()`, so queries can't see
buffered writes.

Buffered DML runs at commit, so errors such as a missing table or a duplicate key come from
`conn.commit()`, not `execute()`. A failed commit keeps the buffered writes and later commits
fail the same way. Call `conn.rollback()` to discard them.

Leaving a `with spanner.connect(...)` block closes the connection without committing. Call
`conn.commit()` before the end of the block to keep buffered writes.

DDL runs immediately: rollback can't undo it, and it runs before any buffered DML. Use
`autocommit=True` to commit each DML statement immediately, which `THEN RETURN` requires. See
[transactions](https://github.com/fornwall/spanner-adbc/blob/main/docs/transactions.md) for
bulk ingest and partitioned DML.

```python
import spanner_adbc.dbapi as spanner
from spanner_adbc import DatabaseOptions

with spanner.connect(
    db_kwargs={DatabaseOptions.URI: "spanner:///projects/my-project/instances/my-instance/databases/my-db"},
) as conn:
    with conn.cursor() as cur:
        # DDL applies immediately.
        cur.execute("DROP TABLE IF EXISTS Albums")
        cur.execute("CREATE TABLE Albums (Id INT64 NOT NULL) PRIMARY KEY (Id)")

        cur.execute("INSERT INTO Albums (Id) VALUES (1)")  # buffered until commit
    conn.commit()  # Commit writes before starting a query transaction.

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM Albums")
        assert cur.fetchone()[0] == 1
    conn.rollback()  # end the query snapshot
```

## Working with DataFrames

The quickstart uses pandas. The examples below use the same `Singers` table. Install `polars`
or `duckdb` separately for those examples.

Fetch a PyArrow table:

```python
import spanner_adbc.dbapi as spanner
from spanner_adbc import DatabaseOptions

with spanner.connect(
    db_kwargs={DatabaseOptions.URI: "spanner:///projects/my-project/instances/my-instance/databases/my-db"},
) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT SingerId, FirstName FROM Singers ORDER BY SingerId")
        table = cur.fetch_arrow_table()      # -> pyarrow.Table
```

`fetch_arrow_table()` and the DataFrame helpers load the whole result into memory. For large
results, iterate over record batches while the cursor and connection are open:

```python
import spanner_adbc.dbapi as spanner
from spanner_adbc import DatabaseOptions

with spanner.connect(
    db_kwargs={DatabaseOptions.URI: "spanner:///projects/p/instances/i/databases/d"},
) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT SingerId, FirstName FROM Singers")
        with cur.fetch_record_batch() as batches:
            for batch in batches:
                print(batch.num_rows)  # process or write each batch here
```

Polars can read directly from the connection:

```python
import polars as pl
import spanner_adbc.dbapi as spanner
from spanner_adbc import DatabaseOptions

with spanner.connect(
    db_kwargs={DatabaseOptions.URI: "spanner:///projects/my-project/instances/my-instance/databases/my-db"},
) as conn:
    df = pl.read_database(
        "SELECT SingerId, FirstName FROM Singers ORDER BY SingerId",
        connection=conn,                     # an ADBC connection, not a URI
    )
```

DuckDB can query a fetched Arrow table in-process:

```python
import duckdb
import spanner_adbc.dbapi as spanner
from spanner_adbc import DatabaseOptions

with spanner.connect(
    db_kwargs={DatabaseOptions.URI: "spanner:///projects/my-project/instances/my-instance/databases/my-db"},
) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT SingerId, FirstName FROM Singers")
        singers = cur.fetch_arrow_table()

# DuckDB can query the Arrow table by variable name.
top = duckdb.sql("SELECT COUNT(*) AS n, MIN(FirstName) AS first FROM singers").fetchone()
```

## Bulk insert a DataFrame

`cur.adbc_ingest(table, data, mode=...)` inserts Arrow-compatible data in bulk. Convert pandas
DataFrames to Arrow first:

```python
import pandas as pd
import pyarrow as pa
import spanner_adbc.dbapi as spanner
from spanner_adbc import DatabaseOptions

frame = pd.DataFrame({"SingerId": [10, 11], "FirstName": ["Carol", "Dave"]})

with spanner.connect(
    db_kwargs={DatabaseOptions.URI: "spanner:///projects/my-project/instances/my-instance/databases/my-db"},
    autocommit=True,                         # apply immediately; returns the row count
) as conn:
    with conn.cursor() as cur:
        # `append` inserts into an existing table (the default mode is `create`).
        rows = cur.adbc_ingest("Singers", pa.Table.from_pandas(frame), mode="append")
```

`mode` says what to do with the target table:

- `create` (the default): create the table from the data's Arrow schema, failing if it exists.
- `append`: insert into an existing table.
- `create_append`: create the table if it doesn't exist, then insert.
- `replace`: drop any existing table, recreate it from the schema, then insert.

Created tables have no declared primary key, so Spanner adds a [hidden `rowid` column][no-pk].
To choose the key, create the table with SQL first and use `mode="append"`. In manual mode,
inserts need `conn.commit()`, but creating or replacing the table happens immediately.
Autocommit loads may commit in several chunks, so a failed load can leave some rows written.

[no-pk]: https://cloud.google.com/spanner/docs/primary-key-default-value#tables-without-primary-keys

## Partitioned reads and Data Boost

The ADBC partitioned execution extension (`adbc_execute_partitions` and `adbc_read_partition`)
splits a large scan into partitions that can be read in parallel:

```python
import spanner_adbc.dbapi as spanner
from spanner_adbc import DatabaseOptions

with spanner.connect(
    db_kwargs={DatabaseOptions.URI: "spanner:///projects/my-project/instances/my-instance/databases/my-db"},
) as conn:
    with conn.cursor() as cur:
        partitions, schema = cur.adbc_execute_partitions("SELECT SingerId FROM Singers")

    # Each descriptor is opaque bytes; it can be shipped to another worker,
    # process, or connection and read independently.
    for token in partitions:
        with conn.cursor() as cur:
            cur.adbc_read_partition(token)
            table = cur.fetch_arrow_table()
            ...
```

On Cloud Spanner, partitions can run on serverless [Data Boost] compute. Enable it on the cursor
that creates the partitions with `adbc_stmt_kwargs={StatementOptions.DATA_BOOST: "true"}`.
Spanner Omni doesn't support Data Boost.

Spanner decides from the query plan whether a query can be partitioned. Simple scans work;
joins, ordering and aggregation can prevent it. See the [partitioning rules][parallel-reads].

Descriptors contain the SQL and transaction IDs and are not authenticated. They run with the
reading connection's credentials, so only read descriptors from trusted sources.

[Data Boost]: https://cloud.google.com/spanner/docs/databoost/databoost-overview
[parallel-reads]: https://cloud.google.com/spanner/docs/reads#read_data_in_parallel

## Supported platforms

`pip` picks the wheel for your OS and architecture:

| Platform | Wheel tag | Requires |
| --- | --- | --- |
| Linux x86-64 | `manylinux_2_35_x86_64` | glibc 2.35+ |
| Linux aarch64 | `manylinux_2_35_aarch64` | glibc 2.35+ |
| Linux x86-64 musl | `musllinux_1_2_x86_64` | musl libc 1.2+ |
| Linux aarch64 musl | `musllinux_1_2_aarch64` | musl libc 1.2+ |
| macOS arm64 | `macosx_11_0_arm64` | macOS 11.0+ |
| macOS x86-64 | `macosx_10_15_x86_64` | macOS 10.15+ |
| Windows x86-64 | `win_amd64` | 64-bit Windows |
| Windows arm64 | `win_arm64` | ARM64 Windows |

Older systems need a native driver and Python dependencies built for them. The Python build
doesn't compile Rust: it packages exactly one native driver library staged in
`python/spanner_adbc/`. To build from source on Linux:

```sh
cargo build --release
cp target/release/libspanner_adbc.so python/spanner_adbc/
pip install './python[dbapi]'
```

See [CONTRIBUTING.md](https://github.com/fornwall/spanner-adbc/blob/main/CONTRIBUTING.md) for
development and releases.

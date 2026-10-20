# ADBC interface

How `spanner-adbc` maps the [ADBC API](https://arrow.apache.org/adbc/current/format/specification.html)
onto Spanner. See the [README](../README.md) for usage, [options](options.md) for configuration
and [transactions](transactions.md) for transaction rules.

## ADBC support

The driver implements ADBC API 1.1.0, as defined by the `adbc.h` of the arrow-adbc revision
pinned in [Cargo.toml](../Cargo.toml). `AdbcSpannerInit` and the fallback `AdbcDriverInit` fill
in a 1.0.0 or 1.1.0 driver table and return `NOT_IMPLEMENTED` for any other version, so a driver
manager can retry with an older one. `get_info` reports `DriverAdbcVersion` as 1.1.0
(`1001000`).

Every entry of the driver table is set, including the 1.1.0 additions: typed option getters and
setters, cancellation, `ExecuteSchema`, statistics and error details. These parts of the
specification are not supported:

| Feature | Behavior |
| --- | --- |
| Substrait plans | `set_substrait_plan` returns `NotImplemented`. |
| Incremental partitioning | `adbc.statement.exec.incremental=true` returns `NotImplemented`. |
| Progress reporting | `adbc.statement.exec.progress` and `adbc.statement.exec.max_progress` return `NotFound`. |
| Temporary ingest tables | `adbc.ingest.temporary=true` returns `NotImplemented`. |
| Changing the current catalog or schema | Only the current values are accepted; see [catalogs](#catalogs). |
| `username` and `password` | Return `NotImplemented`; use the [authentication options](options.md#database-options). |

The driver also supports `adbc.statement.bind_by_name`, which the Python DBAPI sets when
parameters are passed as a mapping.

## Objects and loading

| Object | Responsibility | Source |
| --- | --- | --- |
| `SpannerDriver` | Owns the Tokio runtime shared by its databases, connections and statements. | [driver.rs](../src/driver.rs) |
| `SpannerDatabase` | Stores endpoint, database and credential options; lazily caches the client stack. | [driver.rs](../src/driver.rs) |
| `SpannerConnection` | Owns transaction state and metadata operations. Connections from one database share its client stack. | [connection.rs](../src/connection.rs) |
| `SpannerStatement` | Configures SQL or bulk ingest, binds data and executes work. | [statement.rs](../src/statement.rs) |

Rust callers start with `SpannerDriver::try_new()`. The synchronous ADBC methods run the async
Spanner client on the driver's runtime, so don't call them directly from an async task; see
[synchronous calls from Tokio](limitations.md#synchronous-calls-from-tokio).

Other languages load the shared library through an ADBC driver manager. The C ABI shares one
driver and runtime across the process, and once a driver is initialized the library stays loaded
until the process exits, so its runtime threads and open streams never outlive its code. It lives
in [src/ffi/](../src/ffi), which holds all of the crate's `unsafe` code; building with
`--no-default-features` leaves it out.

```python
import adbc_driver_manager

db = adbc_driver_manager.AdbcDatabase(
    driver="/path/to/libspanner_adbc.so",
    entrypoint="AdbcSpannerInit",
    uri="spanner:///projects/p/instances/i/databases/d",
)
```

The [Python package](../python/README.md) bundles the library and adds a DBAPI interface.

A database does no network I/O until its first connection, which builds the credentials,
channels and multiplexed session. Later connections reuse them. Setting a database option
discards the cache for future connections; existing connections keep theirs.

## Execution

```rust
let mut statement = connection.new_statement()?;
statement.set_sql_query("SELECT SingerId, FirstName FROM Singers")?;
for batch in statement.execute()? {
    println!("{} rows", batch?.num_rows());
}
```

Queries return a streaming `RecordBatchReader`. Execution fetches the first chunk to learn the
schema, and a background task prefetches later chunks as the reader advances. Batches hold up
to `spanner.rows_per_batch` rows (default 8192) and are also capped by size.

Both execution methods accept queries, DML, DDL and bulk ingest:

| Work | `execute()` | `execute_update()` |
| --- | --- | --- |
| Query | Arrow result stream | Drains and discards rows; returns `None`. |
| Plain DML | Empty result stream | Affected-row count in autocommit; `None` when buffered. |
| DML with `THEN RETURN` | Returned rows, materialized before commit completes | Discards returned rows; returns the affected-row count. |
| DDL | Empty result stream | `None` |
| Bulk ingest | Empty result stream | Ingested-row count in autocommit; `None` when buffered. |

Through the C ABI, `AdbcStatementExecuteQuery` sets `rows_affected` to the `execute_update()`
count, or -1 for `None`, whether or not the caller requests a result stream. The Python DBAPI
requests one for every `execute`, so `cursor.rowcount` reports DML counts too.

[sql.rs](../src/sql.rs) classifies SQL by its first keyword and splits `;`-separated batches. A
DML batch must contain only DML and runs atomically through `ExecuteBatchDml`. A DDL batch must
contain only DDL and goes through the Database Admin API. DML with `THEN RETURN` needs autocommit
and can't be part of a batch. Autocommit is the default; see
[transactions](transactions.md#adbc-transactions) for manual mode.

### Result types

The [README](../README.md#type-mapping) has the type table. In addition:

- SQL `NULL` becomes an Arrow null. A malformed non-null value is an error.
- Struct fields are matched by position, so duplicate and empty field names survive.
- JSON fields carry the `arrow.json` extension, including JSON elements of arrays.
- Timestamps are nanoseconds by default, which limits their range. See
  [timestamp precision](options.md#timestamp-precision).

## Parameters and ingest

`bind` takes an Arrow batch. `bind_stream` takes a reader but collects all of its batches in
memory before executing; for large loads, ingest bounded batches in separate autocommit calls
in append mode. An execution consumes the bound data, so bind again before the next one.

SQL parameters are bound positionally by default: the first column binds to the first distinct
`@name` parameter in the SQL, and so on. With `adbc.statement.bind_by_name=true`, column names
are matched to parameter names instead, case-insensitively as in Spanner. GoogleSQL has no `?`
placeholders. Binding several rows runs the statement once per row, and a query's runs share one
snapshot.

For ingest, `adbc.ingest.target_table` selects the table and `adbc.ingest.mode` decides whether
to create or replace it. Each row becomes an insert mutation. Autocommit ingest commits in
chunks, so a large ingest is not atomic; in a manual transaction all mutations wait for one
commit. Creating or replacing the table is DDL and happens immediately. The driver converts every
bound value first, so a bad value fails before the DDL runs or any chunk commits.

The driver checks table, schema and column names before sending anything. It rejects names
containing a backtick, a backslash or a control character, because Spanner's DDL parser doesn't
honour escapes inside backtick-quoted identifiers.

Setting SQL clears the ingest target table, schema and catalog, and setting a target table clears
the SQL. `prepare()` only checks that one of them is set; Spanner plans the SQL when it runs.

### Bound types

Parameters and ingested columns keep their Arrow type:

- `UInt8`, `UInt16` and `UInt32` widen to `INT64`, and `Float16` to `FLOAT32`. `FixedSizeBinary`
  maps to `BYTES`. Dictionary-encoded columns bind as their values.
- `UInt64`, structs and nested lists are not supported.
- `Date64` values must be whole days (multiples of 86,400,000 ms). Use an Arrow timestamp to keep
  a time of day.
- `Decimal128` values with a negative scale keep their value (`99` at scale `-2` is `9900`).
  Spanner checks the `NUMERIC` range, and values read back with scale 9.
- Timestamps of any unit keep their precision; `spanner.max_timestamp_precision` only affects
  reads.
- Strings tagged with the `arrow.json` extension bind and ingest as `JSON`. For an untagged
  string, write `PARSE_JSON(@p)` in the SQL.
- `ENUM`, `PROTO`, `INTERVAL` and `UUID` have no Arrow extension type, so parameters bind as their
  storage type (`Int64`, `Binary` or `Utf8`). Cast them to the intended SQL type. Ingest creates
  columns with the storage type.

To read `ENUM` or `PROTO` values as text, use `CAST(col AS STRING)`; otherwise decode the proto
bytes with your own descriptors.

## Metadata and planning

| Method | Result |
| --- | --- |
| `get_info` | Driver and vendor metadata. Codes without a value, such as the vendor version, are null; unknown codes are omitted. |
| `get_objects` | Catalogs, schemas, tables, columns and constraints from `INFORMATION_SCHEMA`. The `xdbc_*` column fields carry the Spanner type, nullability, `DEFAULT` expression and whether the column is generated. |
| `get_table_schema` | A table's columns as Arrow fields. A missing table returns `NotFound`. |
| `get_table_types` | `BASE TABLE`, `SYNONYM` and `VIEW`. |
| `get_statistics` | Exact row, distinct and null counts from one aggregate scan per table, even when approximate statistics are requested. |
| `get_statistic_names` | An empty, typed result: the driver defines no statistics of its own. |
| `get_parameter_schema` | The bound schema, or parameter names and types from a best-effort PLAN probe. Unresolved types are Arrow `Null`. |
| `execute_schema` | The result schema from a PLAN probe. DML and DDL are rejected. |

### Catalogs

The catalog is the database ID, `<d>` in `projects/<p>/instances/<i>/databases/<d>`. Metadata
results, `adbc.connection.catalog` and foreign-key references all use this name. Catalog filters
must match it, and an empty catalog filter matches nothing. `get_table_schema` and the ingest
catalog option take an exact name; `get_objects` and `get_statistics` accept patterns.

`adbc.connection.db_schema` is `""`. Named schemas work through qualified names and appear in
metadata, but you can't select one as the current schema. Setting either option to another value
returns `NotImplemented`.

## Partitioning and cancellation

`execute_partitions` returns descriptors for a root-partitionable query, and `read_partition`
runs one and streams its rows. Partitioning accepts at most one row of bound parameters and
always uses its own batch read-only transaction. Set the reading connection's timestamp
precision to match the producing statement.

Descriptors contain the SQL and the session and transaction IDs, and are not authenticated.
Only accept them from trusted sources.

Cancelling interrupts the handle's current blocking call and stays in effect for that
operation, including later result fetches. The next operation starts uncancelled. Cancelling an
idle handle succeeds; the driver doesn't track whether an operation is running.

## Errors

[error.rs](../src/error.rs) maps each gRPC status code to an ADBC status, and the message
starts with the code, as in `Spanner error: NOT_FOUND: Table not found: Singers`.

| gRPC code | ADBC status |
| --- | --- |
| `INVALID_ARGUMENT` | `InvalidArguments` |
| `NOT_FOUND` | `NotFound` |
| `ALREADY_EXISTS` | `Integrity` |
| `FAILED_PRECONDITION` | `InvalidState` |
| `OUT_OF_RANGE` | `InvalidData` |
| `UNAUTHENTICATED` | `Unauthenticated` |
| `PERMISSION_DENIED` | `Unauthorized` |
| `DEADLINE_EXCEEDED` | `Timeout` |
| `CANCELLED` | `Cancelled` |
| `UNAVAILABLE`, `ABORTED`, `RESOURCE_EXHAUSTED`, `DATA_LOSS` | `IO` |
| `UNIMPLEMENTED` | `NotImplemented` |
| `UNKNOWN` | `Unknown` |
| `INTERNAL` | `Internal` |

Errors without a gRPC status become `Unauthenticated` for credential failures, `Timeout` for
client deadlines, `IO` for connection and transport failures and `Internal` otherwise.

Constraint violations become `Integrity`, which Python raises as `IntegrityError`. Spanner reports
a duplicate primary or unique index key as `ALREADY_EXISTS`. NOT NULL, foreign key, CHECK and
missing parent row violations share their codes with unrelated errors, so the driver recognizes
them by message. Bulk ingest returns `AlreadyExists` only for a table that exists or doesn't match
the bound schema, as ADBC requires.

Rust callers and callers with a 1.0.0 `AdbcError` get the numeric gRPC code in `vendor_code`. A
1.1.0 `AdbcError` keeps `vendor_code` at `ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA`, so those callers
get the code as the error detail `adbc.spanner.vendor_code`, a decimal string. Each
`google.rpc.Status` detail, such as `ErrorInfo` or `RetryInfo`, becomes an error detail keyed by
its lowercased type name (for example `google.rpc.retryinfo`) with its ProtoJSON encoding as the
value.

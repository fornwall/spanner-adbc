> [!WARNING]
> Experimental: this project is AI-generated and has not yet seen real-world use.

# spanner-adbc

[![CI](https://github.com/fornwall/spanner-adbc/actions/workflows/ci.yml/badge.svg)](https://github.com/fornwall/spanner-adbc/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

An [ADBC](https://arrow.apache.org/adbc/) (Arrow Database Connectivity) driver for
[Google Cloud Spanner](https://cloud.google.com/spanner) and Spanner Omni. Query results come
back as Arrow record batches. The driver ships as a [Python package](https://pypi.org/project/spanner-adbc/),
a [shared library](#shared-library) for any ADBC driver manager, and a [Rust crate](#rust).
Only GoogleSQL databases are supported, not the PostgreSQL dialect.

CI tests the driver against the Spanner emulator and Spanner Omni. Cloud Spanner and
authentication tests run locally.

## Quickstart

Install the Python package (Python 3.11+):

```sh
pip install "spanner-adbc[dbapi]"
```

Connect to an existing database using [Application Default Credentials](#authentication):

```python
import spanner_adbc.dbapi as spanner

uri = "spanner:///projects/my-project/instances/my-instance/databases/my-db"
with spanner.connect(uri) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 AS one")
        print(cur.fetch_arrow_table())
```

The [Python guide](python/README.md) covers parameters, transactions, DataFrames, bulk ingest
and partitioned reads.

## Shared library

Other ADBC clients load the driver as a shared library: `libspanner_adbc.so` on Linux,
`libspanner_adbc.dylib` on macOS or `spanner_adbc.dll` on Windows. Its entrypoint is
`AdbcSpannerInit`, with `AdbcDriverInit` as a fallback.

Download it from a [release](https://github.com/fornwall/spanner-adbc/releases) or the
[shared libraries workflow](https://github.com/fornwall/spanner-adbc/actions/workflows/libraries.yml),
or build it with `cargo build --release`, which writes it to `target/release/`. To load it by
name (`driver="spanner"`), point the library paths in [spanner.toml](spanner.toml) at it and
install the file as a [driver manifest](https://arrow.apache.org/adbc/current/format/driver_manifests.html).

## Connecting

The database option `uri` selects the database and backend:

| Backend | `uri` |
| --- | --- |
| Cloud Spanner | `spanner:///projects/<p>/instances/<i>/databases/<d>` |
| Emulator | `spanner://localhost:9010/projects/<p>/instances/<i>/databases/<d>?spanner.emulator=true` |
| Spanner Omni | `spanner://localhost:15000/projects/default/instances/default/databases/<d>?spanner.instance_type=omni` |

A `host:port` authority sets `spanner.endpoint` and connects over plaintext. For TLS, set
`spanner.endpoint` to an `https://` URL instead. If `SPANNER_EMULATOR_HOST` is set, the Cloud
Spanner form connects to that emulator. See [connection URIs](docs/options.md#connection-uris).

Other options can be set on the database, connection or statement. Driver options start with
`spanner.` and standard ADBC options with `adbc.`. The [option reference](docs/options.md) lists
every option with its type and default.

### Authentication

Cloud Spanner connections use [Application Default Credentials](https://docs.cloud.google.com/docs/authentication/application-default-credentials)
unless one of these database options is set:

| Option | Credentials |
| --- | --- |
| `spanner.auth.keyfile` | A service account, authorized user, impersonated service account or external account JSON file |
| `spanner.auth.keyfile_json` | The same JSON, as a string |
| `spanner.auth.access_token` | A bearer token, sent as is and never refreshed |
| `spanner.auth.impersonate.target_principal` | A service account to impersonate, with the keyfile or ADC as base credentials |

`spanner.auth.quota_project` sets the project billed for quota. URIs are often logged, so the
driver rejects `keyfile_json` and `access_token` as URI query parameters.

The emulator connects without credentials and rejects credential options. Omni and plaintext
endpoints (any scheme but `https://`) ignore ADC and only send credentials you set explicitly,
which a plaintext endpoint sends unencrypted. See [anonymous connections](docs/options.md#anonymous-connections).

## Features

- Streaming queries, returned in Arrow batches of up to 8192 rows and prefetched in the
  background. See [execution](docs/adbc.md#execution).
- DML and DDL, including `;`-separated batches of one kind. DML batches are atomic, and
  autocommit DML supports `THEN RETURN`.
- Parameter binding from Arrow data to GoogleSQL `@name` parameters, by position or by name.
  See [parameters](docs/adbc.md#parameters-and-ingest).
- Bulk ingest in all four ADBC modes.
- Manual transactions. See [transactions](docs/transactions.md#adbc-transactions).
- Partitioned queries, optionally on [Data Boost](https://docs.cloud.google.com/spanner/docs/databoost/databoost-overview)
  (Cloud Spanner only).
- Result and parameter schemas from the query plan, without running the query.
- Metadata through `get_objects()`, `get_table_schema()`, `get_statistics()` and the other ADBC
  metadata calls. See [metadata](docs/adbc.md#metadata-and-planning).
- Cancellation of running calls and result fetches, and per-operation timeouts.
- Spanner-specific options such as stale reads, isolation level, request priority and tags,
  directed reads, retries, partitioned DML and BatchWrite ingest.
- Change streams through ordinary SQL: DDL to create them and `READ_<stream>` queries to read
  them.

[ADBC support](docs/adbc.md#adbc-support) lists the supported ADBC versions and features.

## Limitations

- A manual transaction either runs queries or buffers writes until commit. It can't mix the
  two, so there is no read-your-writes.
- DDL runs immediately, even inside a manual transaction, and rollback can't undo it. A
  multi-statement DDL batch is not atomic: if it fails partway, earlier statements can stay applied.
- A batch can't mix queries, DML and DDL. `THEN RETURN` needs autocommit and a single statement.
- Large autocommit ingests commit in chunks, so a failed ingest can leave rows behind.
  BatchWrite ingest can partially succeed even within a chunk.
- `bind_stream()` reads all input into memory before executing. For large loads, ingest
  bounded batches with separate calls in append mode.
- Tables created by ingest get Spanner's
  [hidden `rowid`](https://docs.cloud.google.com/spanner/docs/primary-key-default-value#tables-without-primary-keys)
  as primary key. To choose the key, create the table yourself and ingest in `append` mode.
- Partition descriptors contain the SQL and session and transaction IDs. They are not
  authenticated and run with the reading connection's credentials, so only accept them from
  trusted sources.
- The catalog is the connected database's ID and can't be changed. The current schema is always
  `""`, so use qualified names for named schemas.
- Substrait plans, incremental partitioning and temporary ingest tables are not supported.

[Known limitations](docs/limitations.md) describes restrictions of the pinned Google Cloud
client.

## Type mapping

| Spanner type | Arrow type |
| --- | --- |
| `BOOL` | `Boolean` |
| `INT64` | `Int64` |
| `FLOAT64` / `FLOAT32` | `Float64` / `Float32` |
| `DATE` | `Date32` |
| `TIMESTAMP` | `Timestamp(Nanosecond, "UTC")`, or microseconds by option |
| `NUMERIC` | `Decimal128(38, 9)` |
| `BYTES` | `Binary` |
| `STRING` / `UUID` / `INTERVAL` | `Utf8` |
| `JSON` | `Utf8` with the [`arrow.json`](https://arrow.apache.org/docs/format/CanonicalExtensions.html#json) extension |
| `ARRAY<T>` | `List<T>` |
| `STRUCT<...>` | `Struct<...>` |
| `ENUM` | `Int64` ordinal |
| `PROTO` | `Binary` serialized bytes |

Nanosecond timestamps only cover the years 1677 to 2262, and reading a value outside that range
fails. Set `spanner.max_timestamp_precision=microseconds` to read Spanner's full range (years 1
to 9999) at microsecond precision. See [timestamp precision](docs/options.md#timestamp-precision).

Binding and ingest accept most Arrow types and widen narrow ones. Some Spanner types, such as
`JSON`, `UUID` and `ENUM`, need a cast or an extension type on the parameter. See
[bound types](docs/adbc.md#bound-types).

## Rust

The crate isn't on crates.io because it depends on git revisions of the Google Cloud, ADBC and
Arrow crates. To use it, add it as a git dependency and use the same `adbc_core` revision as
this repository's [Cargo.toml](Cargo.toml). With the default `ffi` feature, also copy the four
`arrow-*` entries under `[patch.crates-io]` into your workspace's root `Cargo.toml`, since Cargo
ignores patches declared by dependencies. [Dependency pins](CONTRIBUTING.md#dependency-pins)
explains why they exist.

The ADBC Rust traits are synchronous. From async code on a multi-thread Tokio runtime, call the
driver inside `tokio::task::spawn_blocking`; see [synchronous calls from Tokio](docs/limitations.md#synchronous-calls-from-tokio).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for checks and releases, and [testing](docs/testing.md)
for the test suites.

## License

[Apache-2.0](LICENSE)

# dbt

There is no dbt adapter for Spanner that uses this driver. Installing the Python package or the
shared library doesn't give dbt a `type: spanner` profile. This page records what an adapter
built on the driver would have to handle.

## Where an adapter would live

dbt 1 adapters are Python packages that implement dbt's adapter interface. One could use the
driver through `spanner_adbc.dbapi`, but this repository doesn't provide one.

dbt 2 ([dbt-labs/dbt][dbt], in Rust) reaches warehouses through ADBC drivers in its `dbt-adbc`
crate. Its [`Backend`][backend] enum lists the supported databases, which don't include Spanner,
plus an experimental `Generic` variant that loads a library by name and entrypoint. `Generic`
could load this driver (library `spanner_adbc`, entrypoint `AdbcSpannerInit`), but a usable
adapter also needs upstream changes to dbt: a profile type and its authentication options, a SQL
dialect, metadata queries, and macros and materializations.
Use the C ABI rather than a Rust dependency on this crate, whose git-pinned ADBC and Arrow
crates differ from dbt's.

## Driver behavior that shapes an adapter

| Driver behavior | Consequence for an adapter |
| --- | --- |
| GoogleSQL only, with `@name` parameters | PostgreSQL-dialect databases need a separate effort. |
| ADBC autocommit defaults to on, and DBAPI's to off | Set autocommit explicitly. Don't send SQL `BEGIN` or `COMMIT`. |
| Autocommit DML statements and `;`-separated DML batches commit atomically | Keep statements that must apply together in one call. |
| A manual transaction either queries or buffers writes, with no read-your-writes | Don't query a table inside the transaction that writes it. |
| DDL runs immediately, outside transactions, and a multi-statement DDL batch can partially apply | Make each step restartable; after a timeout, inspect the schema before retrying. |
| `execute_schema()` returns a query's result schema from a plan, without running it | Use it to derive a model's columns, since GoogleSQL has no `CREATE TABLE ... AS SELECT`. |
| Arrow schemas lack Spanner type names, lengths and keys; `get_objects()` reports them | Read column types and constraints from `get_objects()` or `INFORMATION_SCHEMA`. |
| The catalog is the database ID, and the current schema is always `""` | Render `table` or `schema.table`, never a database prefix. |
| Ingest supports all four modes; tables it creates get a hidden `rowid` key | Create keyed tables with DDL first, then ingest with `append`. |
| Autocommit ingest commits in chunks | A seed isn't atomic; load it into a staging table and publish it afterwards. |

See [transactions](transactions.md), the [ADBC interface](adbc.md), [options](options.md) and
[known limitations](limitations.md) for the details.

## Spanner SQL for materializations

- Views: `CREATE OR REPLACE VIEW` requires `SQL SECURITY INVOKER` or `SQL SECURITY DEFINER`.
- Tables: create the table from the probed schema, fill it with `INSERT INTO ... SELECT`, which
  must fit in one transaction, and publish it with a single `RENAME TABLE model TO backup,
  staging TO model` statement, which Spanner applies atomically. Indexes, foreign keys,
  interleaved children, change streams and access grants follow the renamed table, while views
  keep referring to the name.
- Incremental models: GoogleSQL has no `MERGE`. `INSERT OR UPDATE` upserts by primary key, and
  a `;`-separated `DELETE` and `INSERT` batch runs atomically for delete-and-insert strategies.
  Partitioned DML handles large `UPDATE` and `DELETE` statements, but not `INSERT`, and isn't
  atomic.

See the [DDL][ddl] and [DML][dml] references, [renaming tables][rename] and
[partitioned DML][partitioned-dml].

[dbt]: https://github.com/dbt-labs/dbt
[backend]: https://github.com/dbt-labs/dbt/blob/00e5d9ef340bd07476f8f02e4e6d048fa8749993/crates/dbt-adbc/src/driver.rs#L27-L99
[ddl]: https://docs.cloud.google.com/spanner/docs/reference/standard-sql/data-definition-language
[dml]: https://docs.cloud.google.com/spanner/docs/reference/standard-sql/dml-syntax
[rename]: https://docs.cloud.google.com/spanner/docs/table-name-synonym
[partitioned-dml]: https://docs.cloud.google.com/spanner/docs/dml-partitioned

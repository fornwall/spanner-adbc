# Spanner adapter proposal for dbt 2

This repository provides the Spanner ADBC driver, but no dbt adapter. The proposed integration
with the Rust implementation at [dbt-labs/dbt][dbt] would use this driver's shared library.
Installing the Python package or shared library does not add `type: spanner` to dbt; the profile,
SQL and materialization designs below are not implemented.

Reviewed on 2026-10-18 against:

- spanner-adbc 0.9.0, repository commit
  [`90bd3c5`](https://github.com/fornwall/spanner-adbc/commit/90bd3c5).
- dbt v2.0.8, released October 14
  ([release][dbt-release], commit `43918198c2f02366ceb33f60006812872b0bab40`).
- dbt upstream `main`, commit
  [`00e5d9ef340bd07476f8f02e4e6d048fa8749993`][dbt-main].
  Source links pin this snapshot; `main` and the release differ. These observations need
  revalidation before implementation or issue reports. dbt v1 is outside this proposal.

## Potential upstream changes

The following source-review findings could benefit existing adapters as well as Spanner. They
need reproducers, regression tests and a check for existing upstream fixes before being reported
as bugs. They are independent of a local Spanner prototype.

### Correctness candidates

1. Make option debug formatting consistently redact credentials.
   The [database builder][database-debug] masks only a few option-name patterns; existing
   BigQuery/Databricks credential keys such as `auth_credentials`, `auth.access_token` and
   `oauth.client_secret` do not all match. The [connection builder][connection-debug] prints
   arbitrary option values. Define a shared redaction policy and test existing credential
   families, URI credentials and future unknown sensitive options. This is a formatter gap,
   not evidence that production logs have exposed credentials. Cover Spanner's inline JSON
   and token options when adding the adapter.

2. Canonicalize each relation component using its own quote policy.
   [`get_canonical_fqn`][canonical-fqn] checks `quote_policy().database` when normalizing the
   database, schema and identifier. Schema and identifier should use their own flags. Test
   all combinations with mixed-case Snowflake/Postgres names and relation-cache lookup, then
   keep Spanner's namespace/case rules explicit when introducing it.

3. Fix SQL literal escaping and make binding substitution SQL-aware.
   [`SqlLiteralFormatter`][formatter] escapes quotes but not existing backslashes for
   BigQuery/Databricks; its tests currently accept a trailing-backslash literal that needs
   closer validation. Separately, `format_sql_with_bindings` splits raw text on `%s` or `?`,
   including occurrences inside strings/comments, and unwraps malformed binding containers.
   Submit these as separate fixes: round-trip backslashes, quotes and control characters;
   recognize placeholders outside SQL literals/comments; return configuration errors for
   malformed binding containers and retain/test existing count checks. Reuse dialect-aware
   parsing in `dbt-adapter-sql`. Do not copy textual
   substitution into Spanner's native `@name` parameter path.

4. Preserve update counts and error identity in record/replay.
   [`RecordingStatement::execute_update`][record-update] returns a live affected-row count but
   does not record it, while [replay][replay-update] returns `None`. Update errors are reduced
   to `Internal` errors in these paths. Record optional counts and structured error identity,
   retaining compatibility with older recordings. Compare live, recorded and replayed results
   for zero/unknown counts, invalid SQL, authorization failures and cancellation. This benefits
   existing update paths and makes future Spanner fixtures meaningful.

The builder-formatting and update-replay patterns above were also checked in
the v2.0.8 source. The relation/formatter findings are pinned to reviewed `main`; their release
impact still needs a reproducer.

### Shared adapter features

5. Make execution completion and affected-row reporting explicit at the adapter layer.
   The [engine][engine-execute] uses `execute_update()` for selected backends, but usually calls
   `execute()` and can drop the reader when `fetch=false`. The [driver manager][execute-rows]
   discards the standard affected-row output on that query path. Establish when a non-fetching
   operation should use `execute_update`, consume a result, or preserve driver metadata.
   Spanner's ordinary DML commits before returning, so this is not a claim that its writes
   currently depend on draining. Test exact/unknown row counts, delayed fetch errors, and
   cancellation during execution and fetch. Preserve existing BigQuery and Snowflake behavior;
   [#16159][skip-drain] avoids unnecessary BigQuery DML draining. Do not replace
   this with unconditional draining or unconditional `execute_update()` for every adapter.

6. Use ADBC schema-only execution where supported, including record/replay.
   [`dbt-adbc::Statement`][statement-api] already exposes `execute_schema()`; a new low-level API
   is unnecessary. [Adapter schema inference][schema-inference] currently obtains schemas through
   query execution or backend-specific SQL such as DESCRIBE, and
   [`ReplayStatement::execute_schema`][replay-schema] is unimplemented. Add an opt-in adapter
   path to PLAN/schema probes, with a fallback only for unsupported capability, and record/replay
   support. Test CTEs, zero-row results, duplicate/unnamed output columns, typed nulls, nested
   types and permission errors without scanning model data. This prepares table construction
   on databases without CTAS, including Spanner.

7. Generalize the existing Arrow seed path where it is useful.
   [DuckDB seed ingestion][duckdb-seed] already provides an Arrow-based implementation;
   [#16560][seed-pr] is related ongoing work. Discuss extracting reusable batch/type/ingest
   handling instead of adding another SQL-literal seed loader. Keep target-schema selection,
   column overrides, explicit keys and staging publication adapter-owned. Test decimals,
   empty/all-null CSVs, large files, duplicate keys and a failure after a committed batch.
   Spanner's `bind_stream()` currently buffers its input, so the adapter must submit bounded
   batches if it promises bounded seed memory.

8. Document and test the route for a community ADBC-backed adapter.
   [Generic/system driver loading][driver-backend] exists, but it does not register a dbt
   adapter, profile or dialect. Document the required integration points, local development
   loading, experimental gating, supported platforms and eventual distribution/checksums.
   The [GizmoSQL adapter proposal][gizmosql-pr] is related work to coordinate with, not evidence
   that arbitrary drivers can already be selected in a released profile. Include a small
   fake-driver conformance suite for options, schema discovery, errors and cancellation.

The adapter should follow dbt's [architecture rules][architecture]: keep backend behavior above
`dbt-adbc` and use its existing primitives/options. Items 5–8 identify shared features to agree
with maintainers while prototyping; a general plugin-system redesign is outside this proposal.

## Integration boundary and driver behavior

Use the ADBC C ABI, loading `libspanner_adbc.so`, `libspanner_adbc.dylib` or `spanner_adbc.dll`
with `AdbcSpannerInit` (fallback `AdbcDriverInit`). The driver supports ADBC 1.0 and 1.1.
For an initial Rust harness, dbt's existing `Backend::Generic` can describe library name
`spanner_adbc` and the explicit entrypoint. A supported dbt adapter still needs registration.
See [shared-library setup](../README.md#shared-library) and the
[FFI implementation](../src/ffi/mod.rs).

This boundary avoids unifying this crate's git-pinned ADBC/Arrow types with dbt's different
[dependency pins][dbt-dependencies]. Do not add `spanner-adbc` as a native Rust dependency just
to obtain a connection. A Python DBAPI adapter belongs to a separate dbt v1 effort.

| Current driver capability | Consequence for dbt 2 |
| --- | --- |
| GoogleSQL parsing/quoting and `@name` parameters | Start with GoogleSQL databases. PostgreSQL-dialect support is outside this plan. |
| ADBC autocommit defaults to `true` | Set and verify it explicitly; Python DBAPI defaults do not apply. Do not issue generic SQL `BEGIN`/`COMMIT` hooks. |
| Ordinary autocommit DML statements and homogeneous DML batches commit atomically | Keep a multi-statement atomic DML operation in one driver call; verify dbt does not split it into separately committed calls. Opt-in partitioned DML commits partitions independently. |
| Manual mode allows snapshot queries or buffered writes, with no mixing/read-your-writes | Do not reuse a warehouse transaction wrapper that introspects after buffering writes. Buffered mutations run after all buffered DML. |
| DDL runs immediately through the admin API and waits for completion | DDL is outside manual transactions; rollback cannot undo it and a DDL batch can partially apply. |
| `execute_schema()` uses a query-only PLAN probe | Infer a compiled model's output without executing its data query. It does not accept DDL/DML. |
| `get_objects()` includes native type names, nullability and constraints | Combine it or `INFORMATION_SCHEMA` with Arrow schemas for contracts and schema changes. Query-derived Arrow fields alone lose important SQL information. |
| One catalog (database ID); current schema fixed to `""` | Derive dbt database identity from the URI; render `schema.table` or `table`, without a database prefix. Qualify named schemas explicitly. |
| Bulk ingest uses insert mutations and supports all four standard modes | Pre-create keyed/constrained targets and append. Automatically created tables rely on a hidden rowid; `replace` drops/recreates. |
| Query streaming and cancellation; ingest can span multiple commits | Test late stream errors, cancel/reuse and partial-write recovery. Do not infer whole-seed atomicity from one ingest call. |

Details: [transactions](transactions.md), [options](options.md), [metadata implementation](../src/objects.rs),
[binding](../src/bind.rs), and [ingest implementation](../src/statement/ingest.rs).

Arrow output is not a complete table definition. In particular, `get_table_schema()` uses a
zero-row query and query fields are nullable; native type names/lengths, keys and constraints need
metadata or explicit configuration. JSON carries `arrow.json` metadata. UUID/INTERVAL/ENUM/PROTO
use storage types without equivalent bind-side tags. Read support for nested structs/lists does
not imply they can be ingested: structs, nested lists and `UInt64` are unsupported inputs.
Preserve NUMERIC decimals and JSON metadata rather than silently converting them to floats/text.
See [type mapping](../README.md#type-mapping).

## Proposed implementation

### 1. Prove connectivity and settle the adapter contract

Build this driver's shared library and load it from a small dbt ADBC harness using existing
system/generic loading. Test ABI negotiation, `SELECT 1`, DDL/DML, metadata, schema probes,
bound parameters, errors and cancellation. Run against the emulator first, then Omni and a
managed Cloud Spanner test database. This does not require registering a full adapter.

Agree on a proposed `type: spanner` profile contract; these fields are not accepted by dbt today:

| Proposed input | Driver mapping / behavior |
| --- | --- |
| `uri` | Database option `uri`, e.g. `spanner:///projects/p/instances/i/databases/d`; derive logical database ID `d`. |
| `schema` | Explicit `""` for the default schema or a named schema; SQL qualification and `adbc.ingest.target_db_schema`, not a mutable current-schema option. |
| Credentials | ADC by default; map explicit sources to documented `spanner.auth.*` database options with dbt redaction. Let the driver refresh supported credential sources; fixed `spanner.auth.access_token` values require caller-managed validity. |
| Emulator / Omni | Map endpoint and mode explicitly. Emulator uses `spanner.emulator`; Omni uses `spanner.instance_type=omni` plus an endpoint. Omni and plaintext endpoints connect anonymously unless explicit credentials are set. |
| Threads and execution settings | dbt worker count plus correctly scoped driver priority, timeout, retry and request/transaction tag options. |
| Timestamp precision | Expose `spanner.max_timestamp_precision`; document and test the default. Microseconds cover years 1–9999 but truncate finer precision; nanoseconds have a narrower range. |

Set connection defaults before creating statements; shared options are copied at statement
creation. Keep access tokens and inline credential JSON out of URI query parameters. Validate
conflicting credentials/backend modes through the driver's documented rules. Initially reject
cross-database models and unsupported materializations/configuration with useful errors.

### 2. Add experimental dbt registration, metadata and SQL behavior

The reviewed dbt snapshot requires changes across these integration points:

| Area | Upstream starting point |
| --- | --- |
| Adapter identity, support gates and quote character | [`dbt-adapter-core/src/lib.rs`][adapter-types] |
| Typed profiles and profile setup | [`dbt-schemas/src/schemas/profiles.rs`][profiles], `dbt-profile-schemas` |
| Profile-to-ADBC options | [`dbt-auth/src/lib.rs`][auth] and backend configuration modules |
| Backend selection and connection setup | [`adapter_factory.rs`][adapter-factory], `dbt-adapter/src/engine/adbc.rs` |
| Metadata and relation/cache behavior | [`adapter_impl.rs`][adapter-impl], `dbt-adapter/src/metadata/`, `relation/` |
| SQL types and literals | [`sql_types.rs`][sql-types], `dbt-adapter-sql`, `dbt-sql-keywords` |
| Dialect selection and splitting | [`dbt-common/src/adapter.rs`][dialect], [`stmt_splitter.rs`][splitter], `dbt-tasks-sa/src/sql/dialect.rs` |
| Macro packages and materializations | [`dbt-loader/src/load_packages.rs`][macro-loader], `dbt-loader/src/dbt_macro_assets/` |
| Macro dispatch inheritance | [`dbt-jinja/minijinja/src/dispatch_object.rs`][macro-dispatch] (`get_adapter_prefixes`) |

Choose local and production loading explicitly: the reviewed [adapter engine][engine-loading]
selects `CdnCache` for ordinary non-DuckDB adapters, and generic-backend authentication is
unimplemented in `dbt-auth`. Successful generic/system loading in the harness does not supply
these adapter-level decisions. Macro-package loading and dispatch inheritance also need separate
registration.

Introduce the adapter behind dbt's existing experimental-adapter gate. Implement discovery of
schemas, tables, views, columns and keys; qualify/quote identifiers consistently and preserve
the empty default schema. Define explicit Arrow-to-Spanner DDL conversions, including
`STRING(MAX)`, `BYTES(MAX)` and `NUMERIC`, and reject query types that cannot become stored columns.

Evaluate reuse of BigQuery's lexer on a Spanner SQL corpus, including comments, backticks,
parameters, CTEs and batches. Shared GoogleSQL ancestry is not proof of analyzer compatibility.
Keep static analysis disabled initially unless a supported dialect path is demonstrated; adapter
identity, macro dispatch, tokenization and full SQL analysis are separate decisions. Do not
register Spanner as BigQuery or inherit its CTAS/MERGE materializations.

### 3. Deliver a small usable materialization set

Start with ephemeral models, views, data tests and bounded seeds. Views need an explicit
`SQL SECURITY INVOKER` or `DEFINER` policy in `CREATE [OR REPLACE] VIEW`; choose and document
the default. Add tables/full refresh once the following publication protocol is tested.
See the [Spanner DDL reference][ddl].

**Tables/full refresh.** GoogleSQL table creation has no CTAS form. Probe the compiled SELECT's
schema, validate configured keys/contracts, create a uniquely named staging table, then populate
it with `INSERT … SELECT` or bounded ingest. Explicit primary keys should be required for the
initial keyed-model strategy; hidden-rowid tables can be a separate opt-in policy.

Illustrative publication when the model already exists:

```sql
RENAME TABLE `my_model` TO `my_model__dbt_backup_run_id`,
             `my_model__dbt_stage_run_id` TO `my_model`;
```

This single statement renames atomically; the preceding creation/load and subsequent cleanup
are separate operations. On first creation, rename only the staging table. Prevent competing
runs from publishing to the same target and define restart behavior for every phase. After a
timeout, inspect actual schema state before retrying or dropping anything. See [atomic rename][rename-ddl]
and [table naming][rename].

Build indexes/constraints before publication where appropriate. References, interleaved children,
change streams and permissions stay associated with the original table rather than transferring
to a replacement. Views need explicit review; dependent objects can prevent dropping the backup.
Initially reject rebuilds with unsupported dependencies. Only delete a backup after checking
that publication succeeded and its dependencies are understood. See [schema updates][schema-updates].

A single `INSERT … SELECT` must fit transaction limits. Larger data movement requires a separately
designed bounded load into staging; [partitioned DML][partitioned-dml] supports UPDATE/DELETE,
not INSERT, and cannot provide atomic whole-load publication by itself.

**Seeds.** Pre-create the staging table for explicit types/keys, then use bounded Arrow ingest
in append mode. Ordinary ingest may commit multiple chunks; BatchWrite can partially succeed
even inside a chunk, so keep it disabled in the first implementation. Publish only after the
complete seed succeeds. Test the hidden-rowid policy separately if supporting unkeyed seeds.
Do not use ingest `replace` directly on a visible model when promising atomic publication.

### 4. Add incremental models, then snapshots

Begin with append and primary-key upsert strategies, after documenting duplicate-key behavior.
For upsert, require the configured `unique_key` to match the physical primary key, and validate
one source row per key. An illustrative statement is:

```sql
INSERT OR UPDATE INTO `my_model` (`id`, `payload`, `updated_at`)
SELECT `id`, `payload`, `updated_at`
FROM ( /* compiled incremental SELECT */ );
```

Only listed columns are overwritten. `ON CONFLICT … DO UPDATE` can support more explicit conflict
targets/conditions later; there is no GoogleSQL MERGE materialization to reuse. See [DML syntax][dml].
Use fresh reads for correctness-sensitive incremental predicates; read-staleness options do not
make the read side of `INSERT … SELECT` stale.

For delete-and-insert, stage the source and commit the staging load first. Then execute the final
DELETE and INSERT as one homogeneous DML batch within transaction limits, preserving it through
dbt's statement splitter. Separate autocommitted calls expose a missing-row interval. In manual
mode, ingest mutations are applied after DML, so buffering the stage ingest with its consuming DML
in one commit does not work. Start `on_schema_change` conservatively with explicit errors;
later compare native target metadata with model output/contracts and support individual compatible
changes. Spanner permits some in-place type changes; a universal rejection is too restrictive.

Snapshots need their own acceptance milestone: stage input once, assign a distinct ID to every
historical version, and batch closing old versions with inserting new ones atomically where
limits permit. Test timestamp/check strategies, hard deletes, a value changing back to an earlier
value, reruns, concurrent writers and timestamp sentinels. Hashing only the key and tracked values
does not uniquely identify a recurring historical version. Do not advertise snapshots until
their dbt semantics and recovery behavior are demonstrated.

### 5. Validate and package for an upstream submission

Use scoped unit tests in the touched dbt crates, mock/fake ADBC tests for shared fixes, and
dbt integration fixtures for `debug`, `parse`, `compile`, `seed`, `run`, `test` and `build`.
Exercise `ref`/`source`, named/default schemas, catalog generation, contracts, repeated runs,
view/table transitions, full refresh and incremental updates. Add snapshot coverage only when
that milestone is implemented. Check released v2.0.8 behavior separately from contribution tests
on current `main`; rebase the plan as the next v2 release arrives.

Test failures during loading, DDL, DML, fetch and rename; partial seed commits; unknown affected
row counts; cancellation and connection reuse; exact decimals/JSON; and timestamps beyond the
nanosecond range. DDL cancellation/timeouts can leave an applied operation, so verify recovery
rather than assuming rollback. Keep emulator, Omni and managed-cloud results distinct: this
driver already has emulator/Omni CI, while cloud/auth validation is opt-in. Reuse the existing
[test setup](testing.md) and [ADBC validation](../adbc-validation/README.md).

Submit registration/auth/metadata and a minimal macro set in reviewable stages after the shared
preparation work. Agree with maintainers on supported OS/architectures, library naming, release
artifacts, checksums and installation; the reviewed [driver distribution manifest][drivers]
does not automatically consume this repository's releases. Keep advanced loading, snapshots,
grants/dependency migration, PostgreSQL dialect and full static analysis as explicit later scope
unless their implementation and tests are included.

[dbt]: https://github.com/dbt-labs/dbt
[dbt-release]: https://github.com/dbt-labs/dbt/releases/tag/v2.0.8
[dbt-main]: https://github.com/dbt-labs/dbt/commit/00e5d9ef340bd07476f8f02e4e6d048fa8749993
[architecture]: https://github.com/dbt-labs/dbt/blob/00e5d9ef340bd07476f8f02e4e6d048fa8749993/.agents/adapters.md
[database-debug]: https://github.com/dbt-labs/dbt/blob/00e5d9ef340bd07476f8f02e4e6d048fa8749993/crates/dbt-adbc/src/database/builder.rs#L163-L177
[connection-debug]: https://github.com/dbt-labs/dbt/blob/00e5d9ef340bd07476f8f02e4e6d048fa8749993/crates/dbt-adbc/src/connection/builder.rs#L25-L32
[canonical-fqn]: https://github.com/dbt-labs/dbt/blob/00e5d9ef340bd07476f8f02e4e6d048fa8749993/crates/dbt-adapter/src/relation/relation_impl.rs#L311-L346
[formatter]: https://github.com/dbt-labs/dbt/blob/00e5d9ef340bd07476f8f02e4e6d048fa8749993/crates/dbt-adapter/src/formatter.rs
[record-update]: https://github.com/dbt-labs/dbt/blob/00e5d9ef340bd07476f8f02e4e6d048fa8749993/crates/adbc-record-replay/src/record.rs#L306-L347
[replay-update]: https://github.com/dbt-labs/dbt/blob/00e5d9ef340bd07476f8f02e4e6d048fa8749993/crates/adbc-record-replay/src/replay.rs#L320-L333
[engine-execute]: https://github.com/dbt-labs/dbt/blob/00e5d9ef340bd07476f8f02e4e6d048fa8749993/crates/dbt-adapter/src/engine/adapter_engine.rs#L397-L532
[execute-rows]: https://github.com/dbt-labs/dbt/blob/00e5d9ef340bd07476f8f02e4e6d048fa8749993/crates/dbt-adbc/src/driver_manager.rs#L1074-L1086
[skip-drain]: https://github.com/dbt-labs/dbt/pull/16159
[statement-api]: https://github.com/dbt-labs/dbt/blob/00e5d9ef340bd07476f8f02e4e6d048fa8749993/crates/dbt-adbc/src/statement.rs#L51-L61
[schema-inference]: https://github.com/dbt-labs/dbt/blob/00e5d9ef340bd07476f8f02e4e6d048fa8749993/crates/dbt-adapter/src/adapter/adapter_impl.rs#L3872
[replay-schema]: https://github.com/dbt-labs/dbt/blob/00e5d9ef340bd07476f8f02e4e6d048fa8749993/crates/adbc-record-replay/src/replay.rs#L335-L337
[duckdb-seed]: https://github.com/dbt-labs/dbt/blob/00e5d9ef340bd07476f8f02e4e6d048fa8749993/crates/dbt-adapter/src/engine/duckdb_seed.rs
[seed-pr]: https://github.com/dbt-labs/dbt/pull/16560
[driver-backend]: https://github.com/dbt-labs/dbt/blob/00e5d9ef340bd07476f8f02e4e6d048fa8749993/crates/dbt-adbc/src/driver.rs#L27-L99
[gizmosql-pr]: https://github.com/dbt-labs/dbt/pull/16289
[dbt-dependencies]: https://github.com/dbt-labs/dbt/blob/00e5d9ef340bd07476f8f02e4e6d048fa8749993/Cargo.toml
[adapter-types]: https://github.com/dbt-labs/dbt/blob/00e5d9ef340bd07476f8f02e4e6d048fa8749993/crates/dbt-adapter-core/src/lib.rs
[profiles]: https://github.com/dbt-labs/dbt/blob/00e5d9ef340bd07476f8f02e4e6d048fa8749993/crates/dbt-schemas/src/schemas/profiles.rs
[auth]: https://github.com/dbt-labs/dbt/blob/00e5d9ef340bd07476f8f02e4e6d048fa8749993/crates/dbt-auth/src/lib.rs
[adapter-factory]: https://github.com/dbt-labs/dbt/blob/00e5d9ef340bd07476f8f02e4e6d048fa8749993/crates/dbt-adapter/src/adapter/adapter_factory.rs
[adapter-impl]: https://github.com/dbt-labs/dbt/blob/00e5d9ef340bd07476f8f02e4e6d048fa8749993/crates/dbt-adapter/src/adapter/adapter_impl.rs
[sql-types]: https://github.com/dbt-labs/dbt/blob/00e5d9ef340bd07476f8f02e4e6d048fa8749993/crates/dbt-adapter/src/sql_types.rs
[dialect]: https://github.com/dbt-labs/dbt/blob/00e5d9ef340bd07476f8f02e4e6d048fa8749993/crates/dbt-common/src/adapter.rs
[splitter]: https://github.com/dbt-labs/dbt/blob/00e5d9ef340bd07476f8f02e4e6d048fa8749993/crates/dbt-adapter/src/stmt_splitter.rs
[macro-loader]: https://github.com/dbt-labs/dbt/blob/00e5d9ef340bd07476f8f02e4e6d048fa8749993/crates/dbt-loader/src/load_packages.rs
[macro-dispatch]: https://github.com/dbt-labs/dbt/blob/00e5d9ef340bd07476f8f02e4e6d048fa8749993/crates/dbt-jinja/minijinja/src/dispatch_object.rs
[engine-loading]: https://github.com/dbt-labs/dbt/blob/00e5d9ef340bd07476f8f02e4e6d048fa8749993/crates/dbt-adapter/src/engine/adbc.rs#L202-L227
[drivers]: https://github.com/dbt-labs/dbt/blob/00e5d9ef340bd07476f8f02e4e6d048fa8749993/crates/dbt-adbc/drivers.toml
[ddl]: https://docs.cloud.google.com/spanner/docs/reference/standard-sql/data-definition-language
[dml]: https://docs.cloud.google.com/spanner/docs/reference/standard-sql/dml-syntax
[rename-ddl]: https://docs.cloud.google.com/spanner/docs/reference/standard-sql/data-definition-language#rename_table
[rename]: https://docs.cloud.google.com/spanner/docs/table-name-synonym
[schema-updates]: https://docs.cloud.google.com/spanner/docs/schema-updates
[partitioned-dml]: https://docs.cloud.google.com/spanner/docs/dml-partitioned

# Configuration options

Set options with `set_option` or constructor options in Rust, the `Adbc*SetOption` functions in
C, or `db_kwargs`, `conn_kwargs` and `adbc_stmt_kwargs` in Python. Setting an unknown key
returns `NotImplemented`, and getting one returns `NotFound`. Setting a key on the wrong level
also returns `NotImplemented`, with a message naming the level that accepts it.

Options live at three levels:

| Level | Scope |
| --- | --- |
| [Database](#database-options) | Database path, endpoint and credentials. |
| [Connection](#connection-only-options) | Transactions, isolation and defaults for new statements. |
| [Statement](#statement-only-options) | SQL and ingest execution, and overrides of the [shared options](#shared-options-connection-and-statement). |

## Inheritance

A new statement copies the connection's shared options. Changing the connection afterwards
doesn't affect existing statements. Clearing a copied option on a statement resets it to the
option's default, not to the connection's value.

A few options behave differently. Existing statements see changes to `adbc.connection.readonly`
immediately. Commit statistics belong to each object and aren't copied. The transaction tag and
isolation level can only be set on the connection, but are copied into each new statement.

## Values and getters

- Booleans are the strings `true` and `false`. Integers and other spellings are rejected.
- Positive integers accept an integer or an integer string. `spanner.retry.max_attempts` also
  accepts whole-number doubles.
- Seconds accept numeric strings, integers or doubles within the range given in the table. They
  must be finite and fit in a duration.
- Everything else is a string. Enumerated values are case-sensitive. Tags are stored verbatim.
  Booleans, `spanner.instance_type`, isolation levels, ingest modes and `spanner.rows_per_batch`
  must match exactly; other parsed values, such as staleness, have surrounding whitespace trimmed.

Invalid values return `InvalidArguments`. Values the driver can't support, such as an unknown
ingest mode or `adbc.ingest.temporary=true`, return `NotImplemented`.

The Get column describes the string getter. "always" returns the value or its default; "set"
returns a value only once it has been set and otherwise `NotFound`; "write-only" always returns
`NotFound`. Typed getters parse the same string as an integer or double, or return its UTF-8
bytes. A failed numeric parse returns `InvalidArguments`.

In the C ABI, database and connection options set before `Init` are buffered, keeping the last
value for each key, and validated at `Init`. Getting any option before `Init` returns
`NotFound`. Statements have no such phase.

## Database options

The first connection builds the database's client stack, and later connections share it.
Setting any database option discards the cached stack for future connections; existing
connections keep theirs. Getters don't show values taken from the environment.

| Option | Values | Default | Get | Effect |
| --- | --- | --- | --- | --- |
| `uri` | `spanner://` URI | required | set, verbatim | Database path plus optional endpoint and options; see [connection URIs](#connection-uris). Without it, creating a connection fails with `InvalidState`. |
| `spanner.endpoint` | gRPC endpoint URL; `""` unsets | production endpoint | set | Overrides the endpoint from `SPANNER_EMULATOR_HOST`. A bare `host:port` uses `http://`. Only `https://` uses TLS; other schemes are plaintext. |
| `spanner.emulator` | boolean | `false` | always | Uses anonymous credentials. The emulator's address still comes from `spanner.endpoint` or `SPANNER_EMULATOR_HOST`; with neither, connecting fails with `InvalidState`. A non-empty `SPANNER_EMULATOR_HOST` also enables this mode. |
| `spanner.instance_type` | `cloud`, `omni` | `cloud` | always | Selects Cloud Spanner or Spanner Omni. Omni needs an explicit endpoint, ignores `SPANNER_EMULATOR_HOST` and can't be combined with `spanner.emulator=true`. Without explicit credentials, Omni connects anonymously; see [anonymous connections](#anonymous-connections). |
| `spanner.auth.keyfile` | credential JSON file path; `""` unsets | ADC | set | Supports `service_account`, `authorized_user`, `impersonated_service_account` and `external_account` JSON; see [authentication](#authentication). |
| `spanner.auth.keyfile_json` | inline credential JSON; `""` unsets | ADC | write-only | Same formats as `keyfile`, and takes precedence over it. Not allowed in URIs. |
| `spanner.auth.impersonate.target_principal` | service account email; `""` unsets | unset | set | Impersonates this account, using the keyfile or ADC as base credentials. |
| `spanner.auth.impersonate.delegates` | comma-separated service account emails, unique IDs or `projects/-/serviceAccounts/...` names | no delegates | set | Delegation chain. Entries are trimmed and empty entries dropped. Bare emails and IDs are expanded to resource names for IAM. Ignored without a target principal. |
| `spanner.auth.impersonate.scopes` | comma-separated OAuth scopes | `cloud-platform` | set | Scopes for the impersonated token. Entries are trimmed and empty entries dropped. Ignored without a target principal. |
| `spanner.auth.impersonate.lifetime` | integer seconds, `1` through `43200`; `""` unsets | `3600` | set | Lifetime of the impersonated token. Accepts an integer or integer string. Values above `3600` require an organization policy; see [authentication](#authentication). Ignored without a target principal. |
| `spanner.auth.access_token` | OAuth bearer token; `""` unsets | ADC | write-only | Sent as is and never refreshed. Not allowed in URIs. |
| `spanner.auth.quota_project` | project ID; `""` unsets | credential default | set | Sends `x-goog-user-project`; the caller needs `serviceusage.services.use`. With ADC, `GOOGLE_CLOUD_QUOTA_PROJECT` takes precedence. Rejected in emulator mode and [anonymous connections](#anonymous-connections). |
| `spanner.rpc.timeout_seconds.connect` | non-negative fractional seconds | `30` | set | Deadline for building the client stack, including session creation and its retries; see [connect timeout](#connect-timeout). |

In emulator mode, creating the client stack fails with `InvalidState` if a keyfile, inline JSON,
access token, impersonation target or quota project is set. Ambient ADC doesn't count. An access
token can't be combined with a keyfile, inline JSON or an impersonation target, which also fails
with `InvalidState`.

A keyfile, inline JSON or ADC that can't be read or parsed, for example because
`GOOGLE_APPLICATION_CREDENTIALS` names a missing file, fails with `InvalidArguments` when the
client stack is built. A token that can't be fetched or is rejected surfaces as `Unauthenticated`.
Errors while fetching credentials omit token endpoint response bodies and credential JSON.

### Authentication

ADC searches `GOOGLE_APPLICATION_CREDENTIALS`, the local file created by
`gcloud auth application-default login`, then the compute resource's attached identity through
the metadata server. The CLI's `gcloud auth login` credentials are separate. See Google's
[ADC setup guide](https://docs.cloud.google.com/docs/authentication/provide-credentials-adc).

Explicit JSON credentials select the same four credential types supported by ADC:

| JSON `type` | Use |
| --- | --- |
| `service_account` | A service account private key. |
| `authorized_user` | A user's OAuth client and refresh token, such as local gcloud ADC. |
| `impersonated_service_account` | A target service account and source credentials, such as ADC created with `gcloud auth application-default login --impersonate-service-account=ACCOUNT_EMAIL`. |
| `external_account` | A Workload or Workforce Identity Federation configuration. File, URL, AWS and executable credential sources are supported. Executable sources require `GOOGLE_EXTERNAL_ACCOUNT_ALLOW_EXECUTABLES=1`. |

Federation configurations can include `service_account_impersonation_url`. The pinned auth
library uses a one-hour token lifetime for that embedded impersonation flow. To choose a
lifetime explicitly, use a federation configuration without that field as the source and set
`spanner.auth.impersonate.target_principal` and `spanner.auth.impersonate.lifetime`.

For explicit external-account JSON or keyfiles, `quota_project_id` in the JSON supplies the
quota project unless `spanner.auth.quota_project` overrides it. For external accounts loaded
through ADC, the pinned auth library ignores the JSON quota project; set
`spanner.auth.quota_project` or `GOOGLE_CLOUD_QUOTA_PROJECT` instead.

For explicit impersonation, the source identity needs `iam.serviceAccounts.getAccessToken` on
the target service account, for example through `roles/iam.serviceAccountTokenCreator`.
With delegates, it needs that role on the first delegate, each delegate needs it on the next,
and the last needs it on the target. The target also needs the appropriate Spanner permissions.
Lifetimes above one hour require the target account to be allowed by
`constraints/iam.allowServiceAccountCredentialLifetimeExtension`; IAM allows at most twelve
hours. See [IAM's token API](https://docs.cloud.google.com/iam/docs/reference/credentials/rest/v1/projects.serviceAccounts/generateAccessToken).

ADC, JSON credentials and impersonation cache and refresh tokens through the shared client
stack. `spanner.auth.access_token` accepts the raw OAuth access token, without a `Bearer `
prefix, and never refreshes it. To replace a static token, set the option and create a new
connection; existing connections retain their original credentials.

### Anonymous connections

Omni and plaintext endpoints connect anonymously unless a keyfile, inline JSON, access token or
impersonation target is set. They never use ambient ADC or `GOOGLE_CLOUD_QUOTA_PROJECT`, so
Google credentials aren't sent to a non-Google or unencrypted host by default. A quota project
would need ADC there, so without one of those options creating the client stack fails with
`InvalidState`.

Explicit credentials are always sent. Over a plaintext endpoint, their bearer tokens travel
unencrypted, so use `https://` unless the host is local or on a trusted network.

### Connection URIs

The `spanner` scheme is required and case-insensitive. Bare database paths and other schemes
return `InvalidArguments`.

```text
spanner:///projects/<p>/instances/<i>/databases/<d>?spanner.emulator=true
spanner://localhost:9010/projects/<p>/instances/<i>/databases/<d>
```

The path names the database; its segments are percent-decoded. A `host:port` authority sets
`spanner.endpoint` and connects over plaintext `http://`; without one, use three slashes. A
plaintext endpoint without credential options [connects anonymously](#anonymous-connections), but
only `spanner.emulator` or `SPANNER_EMULATOR_HOST` turn on emulator mode.

Query parameters can set any database option except `uri`, `spanner.auth.keyfile_json` and
`spanner.auth.access_token`. Unknown or forbidden keys return `InvalidArguments`. Values are
percent-decoded, and `+` stays a literal plus. Fragments and userinfo (`user:password@host`) are
rejected: set secrets through the `spanner.auth.*` options instead. A keyfile path is allowed.

Setting a URI applies its options immediately and leaves other options alone; a later setting
of the same option wins. `get_option("uri")` returns the last accepted URI as given, while each
option's getter shows its current value.

## Connection-only options

| Option | Values | Default | Get | Effect |
| --- | --- | --- | --- | --- |
| `adbc.connection.autocommit` | boolean | `true` | always | `false` starts [manual transactions](transactions.md#adbc-transactions). Switching back to `true` commits pending work; if that fails, the work stays pending. |
| `adbc.connection.readonly` | boolean | `false` | always | Rejects DML, DDL, ingest and commits of buffered writes with `InvalidState`. Rollback and read-only or empty commits still work. |
| `adbc.connection.transaction.isolation_level` | full ADBC isolation constant; see below | `adbc.connection.transaction.isolation.default` | always, effective level | Isolation for read/write transactions. Copied into new statements. |
| `spanner.transaction.tag` | string; `""` unsets | unset | set | Tags read/write and write-only transactions and BatchWrite groups. Copied into new statements, which can't set it. |
| `adbc.connection.catalog` | database ID | current database ID | always | Only the current value is accepted; anything else returns `NotImplemented`. |
| `adbc.connection.db_schema` | `""` | `""` | always | Only `""` is accepted. Use qualified names for named schemas. |

### Isolation levels

Values need the `adbc.connection.transaction.isolation.` prefix, and unknown values return
`InvalidArguments`. [Isolation and locking](transactions.md#isolation-and-locking) maps each
level to Spanner's and says which transactions use it. The getter reports the level the driver
sends, `serializable` or `repeatable_read`, except that `default` stays `default`.

## Shared options (connection and statement)

Every settable option below accepts `""` to reset it. A statement's getters report its own
effective value, whether inherited or overridden.

| Option | Values | Default | Get | Effect |
| --- | --- | --- | --- | --- |
| `spanner.read.staleness` | [staleness grammar](#stale-reads) | strong read | set, trimmed | Timestamp bound for read-only queries. Clearing an inherited bound restores strong reads. |
| `spanner.max_timestamp_precision` | `nanoseconds_error_on_overflow`, `microseconds` | `nanoseconds_error_on_overflow` | always | Arrow timestamp unit and range; see [timestamp precision](#timestamp-precision). Also used for connection metadata schemas and partition reads. |
| `spanner.request.priority` | `low`, `medium`, `high` | service default: high | set | Priority for queries, DML, commits and BatchWrite, and for internal metadata reads. |
| `spanner.request.tag` | string | unset | set, verbatim | Tags user queries, DML and batch DML. Internal metadata reads and BatchWrite stay untagged. |
| `spanner.directed_read` | [replica selection grammar](#directed-reads) | service routing | set, trimmed | Replica selection for read-only queries, including metadata reads. |
| `spanner.query.optimizer_version` | version string or `latest` | database or service default | set, verbatim | Query optimizer version. |
| `spanner.query.optimizer_statistics_package` | package name | database default | set, verbatim | Query optimizer statistics package. |
| `spanner.commit.max_delay` | duration from `0` to `500ms` | unset | set, trimmed | Extra delay Spanner may add to a commit to batch it with others. Uses the staleness duration units. |
| `spanner.commit_stats` | boolean | `false` | always | Requests mutation counts for read/write and write-only commits. |
| `spanner.commit_stats.mutation_count` | read-only | unavailable | after a stats-enabled commit | The last recorded mutation count for this object: manual commits on the connection, autocommit DML and ingest on the statement. For chunked ingest, the last chunk's count. Setting it returns `NotImplemented`. |
| `spanner.transaction.exclude_from_change_streams` | boolean | `false` | always | Excludes writes from change streams created with `allow_txn_exclusion=true`. Applies to ordinary writes, BatchWrite and partitioned DML. |
| `spanner.rpc.timeout_seconds.query` | non-negative fractional seconds | unset | set | Deadline for starting queries, planning, partitioning and metadata reads; see [RPC timeouts](#rpc-timeouts). |
| `spanner.rpc.timeout_seconds.update` | non-negative fractional seconds | unset | set | Deadline for each write and DDL operation; see [RPC timeouts](#rpc-timeouts). |
| `spanner.rpc.timeout_seconds.fetch` | non-negative fractional seconds | unset | set | Deadline for each later result chunk; see [RPC timeouts](#rpc-timeouts). |
| `spanner.retry.max_attempts` | integer `1..=4294967295` | client default | set | Maximum attempts, including the first; `1` disables retries. See [retry tuning](#retry-tuning). |
| `spanner.retry.max_elapsed_seconds` | positive fractional seconds | unset | set | Time limit for retrying. Has no effect on resuming streamed queries and BatchWrite. |
| `spanner.retry.backoff.initial_seconds` | positive fractional seconds | `1` | set | Initial backoff delay, clamped at execution. |
| `spanner.retry.backoff.max_seconds` | positive fractional seconds | `60` | set | Maximum backoff delay, clamped at execution. |
| `spanner.retry.backoff.multiplier` | positive finite number | `2` | set | Backoff growth factor, clamped at execution. |

Commit delay and commit statistics apply to read/write and write-only commits, not to BatchWrite
or partitioned DML. Internal metadata reads use the priority, directed read, retry and timeout
settings, but no request or transaction tags.

## Statement-only options

| Option | Values | Default | Get | Effect |
| --- | --- | --- | --- | --- |
| `spanner.rows_per_batch` | positive integer; `""` resets | `8192` | always | Maximum rows per Arrow batch. An approximate byte budget can produce smaller batches; see [execution](adbc.md#execution). |
| `spanner.data_boost` | boolean; `""` resets | `false` | always | Runs partitioned queries on [Data Boost](https://docs.cloud.google.com/spanner/docs/databoost/databoost-overview). Cloud Spanner only; Omni doesn't support it. Carried in partition descriptors. |
| `adbc.statement.bind_by_name` | boolean | `false` | always | By default, columns bind to distinct parameters in SQL order. `true` matches column names to parameter names, case-insensitively as Spanner does; unknown names return `InvalidArguments`. |
| `adbc.statement.exec.incremental` | boolean, only `false` supported | `false` | always | `true` returns `NotImplemented`; incremental partitioning is not supported. |
| `adbc.ingest.target_table` | table name | unset | set | Selects bulk ingest and clears the SQL; setting SQL clears it. Ingest rejects an empty name, or one containing a backtick, backslash or control character, with `InvalidArguments`. |
| `adbc.ingest.target_db_schema` | schema name; `""` for the default schema | default schema | set | Schema of the target table. Setting SQL clears it. |
| `adbc.ingest.target_catalog` | connection's database ID | connection's catalog | set | Other values, including `""`, return `NotImplemented`. Setting SQL clears it. |
| `adbc.ingest.temporary` | boolean, only `false` supported | `false` | always | `true` returns `NotImplemented`; temporary tables are not supported. |
| `adbc.ingest.mode` | `adbc.ingest.mode.append`, `create`, `create_append`, `replace` (each with the prefix; bare suffixes are also accepted) | `adbc.ingest.mode.create` | always, full constant | `append` needs an existing table; `create` needs it to be absent; `create_append` creates it if absent; `replace` drops and recreates it. All modes then insert the rows. |
| `spanner.ingest.batch_write` | boolean; `""` resets | `false` | always | Uses non-atomic BatchWrite for autocommit ingest. Ignored in manual mode. |
| `spanner.dml.partitioned` | boolean; `""` resets | `false` | always | Runs DML as partitioned DML. Ignored for queries, DDL and ingest. |

[Parameters and ingest](adbc.md#parameters-and-ingest) describes the tables ingest creates.
[BatchWrite](transactions.md#batchwrite-and-partitioned-dml) sends one mutation group per row, so
a chunk can partially succeed. Priority, transaction tag, change-stream exclusion, retry settings
and the update timeout apply to it; request tags and commit delay and statistics don't.

[Partitioned DML](https://docs.cloud.google.com/spanner/docs/dml-partitioned) needs an idempotent
`UPDATE` or `DELETE`, because partitions commit independently and can run more than once. The
reported row count is a lower bound. It returns `InvalidState` in manual mode, and
`InvalidArguments` for multiple statements, multiple parameter rows or `THEN RETURN`. Priority,
request tag, optimizer settings, change-stream exclusion, retry settings and the update timeout
apply; commit settings, the transaction tag and the isolation level don't.

## Stale reads

Queries default to strong reads. `spanner.read.staleness` accepts:

| Form | Meaning |
| --- | --- |
| `exact:<duration>` | Read exactly that far in the past. |
| `max:<duration>` | Spanner picks a timestamp no older than the duration. |
| `read:<rfc3339>` or a bare RFC 3339 timestamp | Read at that timestamp. |
| `min:<rfc3339>` | Spanner picks that timestamp or a later one. |

Durations are non-negative numbers, in seconds unless followed by a unit: `s`, `ms`, `us` or
`µs`, `ns`, `m` or `h`. Examples: `exact:2.5s`, `max:1m`, `read:2026-07-07T00:00:00Z`. Prefixes
are lowercase, and surrounding whitespace is trimmed. Durations above 10,000 years and timestamps
before year 1 are rejected when the option is set.

Spanner only accepts `max:` and `min:` for single-use reads. Manual query transactions, queries
bound to several rows, metadata snapshots and `execute_partitions` convert them to the stalest
equivalent exact bound: `max:<d>` becomes `exact:<d>` and `min:<t>` becomes `read:<t>`.

## Directed reads

`spanner.directed_read` selects replicas for read-only queries:

```text
<mode> [ ":" <selection> ("," <selection>)* ] [ ";auto_failover_disabled" ]
```

- The mode is `include` (replicas in order of preference) or `exclude` (replicas to avoid).
- A selection is a location, `location:type` or `:type`. Types are `read_write`, `read_only` and
  `any`; no type means any. Each selection must name a location or a specific type.
- `;auto_failover_disabled` only works with `include` and stops Spanner from falling back to
  other replicas. `include;auto_failover_disabled` is the only form that may omit the selections.

Examples: `include:us-east1`, `include:us-east1:read_only,us-east4:read_write`,
`exclude:us-central1`, `include::read_only`, `include:us-east1;auto_failover_disabled`.
Values are trimmed, and invalid ones return `InvalidArguments`. See
[directed reads](https://docs.cloud.google.com/spanner/docs/directed-reads).

## Timestamp precision

Spanner timestamps cover the years 0001 to 9999. Arrow nanosecond timestamps are an `i64`, which
only covers about 1677 to 2262. `spanner.max_timestamp_precision` chooses between:

- `nanoseconds_error_on_overflow`: `Timestamp(Nanosecond, "UTC")`. Out-of-range values return
  `InvalidData` (Python `DataError`) naming the column and value.
- `microseconds`: `Timestamp(Microsecond, "UTC")`, covering Spanner's full range. Sub-microsecond
  digits are rounded toward negative infinity, so one nanosecond before the epoch becomes
  microsecond `-1`.

The mode applies to all results and schemas, including `THEN RETURN` rows, metadata schemas and
partition reads. `read_partition` uses the reading connection's mode, so match it to the
producing statement. Clearing the option restores `nanoseconds_error_on_overflow`. Bound
timestamps keep their own unit; this option only affects reads.

## RPC timeouts

`spanner.rpc.timeout_seconds.{query,update,fetch}` set an overall deadline for each operation
they cover, including the client's retries. `0` disables the deadline and `""` unsets it. When
a deadline expires, the call returns `Timeout`. A commit or DDL operation that timed out may
still have been applied.

| Option | Covers |
| --- | --- |
| `query` | The initial query request and first chunk, PLAN probes (including DML parameter planning), creating partitions and the first chunk of a partition read, and internal metadata reads. |
| `update` | DML transactions including replays after `ABORTED`, manual commits, each ingest chunk, BatchWrite, partitioned DML, and DDL submission plus polling, under one deadline. |
| `fetch` | Each later result chunk, including the later per-row runs of a query bound to several rows. |

Creating the client stack falls under the [connect timeout](#connect-timeout) instead, and
background session maintenance has no deadline. Starting a manual read-only transaction sends no
RPC, so it needs none.

### Connect timeout

`spanner.rpc.timeout_seconds.connect` is a database option that bounds building the client
stack: the gRPC client, credentials and the multiplexed session, including the client's retries.
Without it, an unreachable endpoint, such as a stopped emulator named by `SPANNER_EMULATOR_HOST`,
would be retried indefinitely. The 30-second default leaves room for a slow first token fetch and
several retries of a transient failure.

When the deadline expires, creating the connection returns `Timeout` with a message naming the
database and endpoint. If the credentials never supplied an access token, for example because
ADC found no credentials outside Google Cloud and fell back to the unreachable metadata server,
it returns `Unauthenticated` instead and names the cause. Nothing is cached, so the next
connection tries again. `0` disables the deadline and `""` restores the default.

Connecting can't be interrupted: `AdbcConnectionCancel` only applies after `Init`, and Python
handles Ctrl+C only once the call returns. Other connections on the same database wait while the
first builds the client stack. That wait is outside the timeout. If the build fails, the next
waiting connection starts another attempt with its own deadline.

## Retry tuning

`spanner.retry.*` configures retries of query and DML requests, batch DML, BatchWrite, and the
begin and commit RPCs of read/write and write-only transactions. The client's choice of
retryable errors is kept. These options don't affect session creation, `PartitionQuery` or DDL
requests.

When Spanner aborts a transaction, the client reruns the whole transaction under a separate
policy with no attempt limit. Use the `update` timeout to bound it.

### Attempt and elapsed limits

| Setting | Unary RPCs | Resuming query and BatchWrite streams |
| --- | --- | --- |
| `max_attempts=N` | At most N attempts | At most N attempts |
| `max_elapsed_seconds` | Limits retry time | No effect; see [known limitations](limitations.md#streaming-retries-ignore-the-elapsed-time-limit) |
| Neither set | No limit | At most 10 attempts |

If the first request of a query stream fails, the stream's resume loop isn't involved. Setting
only `max_elapsed_seconds` replaces the default streaming policy and its 10-attempt limit, so
also set `max_attempts` if you want an attempt limit. Use the `query` and `fetch` timeouts to
bound the initial request and each subsequent chunk, and the `update` timeout for BatchWrite.
The fetch deadline restarts for each chunk, so these settings don't bound the total time to read
all results.

### Backoff

Setting any backoff option switches to exponential backoff with jitter; unset components default
to 1 second initial delay, 60 seconds maximum and a factor of 2. The driver clamps effective
values: the maximum delay to between 1 second and 24 hours, the initial delay to between 1
millisecond and the effective maximum, and the multiplier to between 1 and 32. Getters return the
configured values. Backoff is independent of the attempt and time limits.

## Environment

- `SPANNER_EMULATOR_HOST` is read when the client stack is built. A non-empty value turns on
  emulator mode and supplies the endpoint unless `spanner.endpoint` is set. Omni mode ignores it.
  A bare `host:port` gets `http://` prepended; point it at the emulator's gRPC port, normally
  `9010`. The driver uses gRPC for queries and schema changes on every backend.
- Without explicit credentials, emulator mode, Omni mode or a plaintext endpoint, the driver uses
  [Application Default Credentials](https://docs.cloud.google.com/docs/authentication/application-default-credentials),
  including `GOOGLE_APPLICATION_CREDENTIALS`, gcloud's local credentials and the metadata server.
- `GOOGLE_CLOUD_QUOTA_PROJECT` overrides the quota project option when credentials come from ADC.
  Keyfile, impersonated and access token credentials use the option instead, though an ADC source
  used as the base for impersonation can still apply the environment value to its own requests.
  Emulator mode and anonymous connections ignore it.

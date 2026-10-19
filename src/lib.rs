//! # spanner-adbc
//!
//! An [ADBC](https://arrow.apache.org/adbc/) (Arrow Database Connectivity) driver for
//! [Google Cloud Spanner](https://cloud.google.com/spanner), built on the `google-cloud-spanner`
//! preview client and the [`adbc_core`] traits. Query results are returned as Arrow
//! [`RecordBatch`](arrow_array::RecordBatch)es.
//!
//! The driver follows the standard ADBC object hierarchy:
//!
//! ```text
//! SpannerDriver ──> SpannerDatabase ──> SpannerConnection ──> SpannerStatement
//! ```
//!
//! ## Configuration
//!
//! Every level is configured through ADBC options. See
//! [docs/options.md](https://github.com/fornwall/spanner-adbc/blob/main/docs/options.md) for the
//! full reference, including types, defaults and `get_option` behavior. Connection and statement
//! options set on a connection are inherited by the statements it creates, which can override
//! them.
//!
//! The database is selected with the standard
//! [`OptionDatabase::Uri`](adbc_core::options::OptionDatabase::Uri) option, which takes a
//! `spanner://` URI; a bare database path is not accepted. The URI path names the database, the
//! optional `//host:port` authority sets [`OPTION_ENDPOINT`], and query parameters set other
//! database options:
//!
//! ```text
//! spanner:///projects/<p>/instances/<i>/databases/<d>?spanner.emulator=true
//! ```
//!
//! The URI is expanded into individual options when it is set, so options set afterwards take
//! precedence. URIs are often logged, so [`OPTION_KEYFILE_JSON`] and [`OPTION_ACCESS_TOKEN`] are
//! rejected as query parameters and must be set as options. [`OPTION_KEYFILE`] is a path and may
//! appear in a URI.
//!
//! ## Transactions
//!
//! Connections start in autocommit mode. With `adbc.connection.autocommit` set to `false`, a
//! transaction holds either read-only queries sharing one snapshot or DML, depending on its first
//! statement; a statement of the other kind fails with `InvalidState` until `commit` or
//! `rollback`. DML and bulk-ingest mutations are buffered until `commit`, so a DML transaction
//! cannot read its own writes and reports unknown (`None`) row counts. DDL is not transactional
//! and always executes immediately.
//!
//! See [`SpannerConnection`] and
//! [docs/transactions.md](https://github.com/fornwall/spanner-adbc/blob/main/docs/transactions.md)
//! for details.
//!
//! ## Example
//!
//! ```no_run
//! use adbc_core::{Driver, Database, Connection, Statement};
//! use adbc_core::options::{OptionDatabase, OptionValue};
//! use spanner_adbc::SpannerDriver;
//! use arrow_array::RecordBatchReader;
//!
//! # fn main() -> adbc_core::error::Result<()> {
//! let mut driver = SpannerDriver::try_new()?;
//! let database = driver.new_database_with_opts([(
//!     OptionDatabase::Uri,
//!     OptionValue::String("spanner:///projects/p/instances/i/databases/d".into()),
//! )])?;
//! let mut connection = database.new_connection()?;
//! let mut statement = connection.new_statement()?;
//! statement.set_sql_query("SELECT 1 AS one")?;
//! let reader = statement.execute()?;
//! for batch in reader {
//!     let batch = batch?;
//!     println!("got {} rows", batch.num_rows());
//! }
//! # Ok(())
//! # }
//! ```

// Other crate-wide lints live in `[lints.rust]` in Cargo.toml; these two depend on features.
// Unsafe code is confined to the C ABI layer and the test-only ASan canary.
#![deny(unsafe_code)]
#![cfg_attr(not(feature = "ffi"), forbid(unsafe_code))]

// The validation suite enables this ASan check with `--cfg asan_canary`.
#[cfg(asan_canary)]
mod asan_canary;

mod bind;
mod connection;
mod conversion;
mod directed_read;
mod driver;
mod error;
#[cfg(feature = "ffi")]
#[allow(unsafe_code)] // The hand-written C ABI; see the module docs.
mod ffi;
mod info;
mod metadata;
mod nested;
mod objects;
mod options;
mod query_options;
mod request;
mod retry;
mod runtime;
mod sql;
mod staleness;
mod statement;
mod statistics;
mod timeout;

pub use connection::SpannerConnection;
pub use driver::{SpannerDatabase, SpannerDriver};
pub use statement::SpannerStatement;

/// Internal parsing helpers for the fuzz targets (the `fuzzing` feature). Not a stable API.
#[cfg(feature = "fuzzing")]
#[doc(hidden)]
pub mod fuzzing {
    /// Split a `;`-separated SQL batch into statements, honoring quotes and comments.
    pub fn split_statements(sql: &str) -> Vec<String> {
        crate::sql::split_statements(sql)
    }
    /// Whether the SQL begins with a DDL statement.
    pub fn is_ddl(sql: &str) -> bool {
        crate::sql::is_ddl(sql)
    }
    /// Whether the SQL contains a top-level `THEN RETURN` clause.
    pub fn is_dml_returning(sql: &str) -> bool {
        crate::sql::is_dml_returning(sql)
    }
    /// Parse a Spanner `DATE` string into Arrow `Date32` days.
    pub fn parse_date_days(s: &str) -> Option<i32> {
        crate::conversion::parse_date_days(s)
    }
    /// Parse a Spanner `TIMESTAMP` string into epoch nanoseconds.
    pub fn parse_timestamp_nanos(s: &str) -> Option<i64> {
        crate::conversion::parse_timestamp_nanos(s)
    }
    /// Parse a Spanner `NUMERIC` string into an unscaled `i128` (scale 9).
    pub fn parse_numeric_i128(s: &str) -> Option<i128> {
        crate::conversion::parse_numeric_i128(s)
    }
    /// Match an ADBC `LIKE` pattern against a value.
    pub fn like_match(pattern: &str, value: &str) -> bool {
        crate::metadata::like_match(pattern, value)
    }
    /// The first SQL keyword, uppercased, after whitespace, comments and `@{...}` statement hints.
    pub fn first_keyword(sql: &str) -> Option<String> {
        crate::sql::first_keyword(sql).map(str::to_ascii_uppercase)
    }
    /// Whether the SQL begins with a DML statement (`INSERT`/`UPDATE`/`DELETE`).
    pub fn is_dml(sql: &str) -> bool {
        crate::sql::is_dml(sql)
    }
    /// Strip trailing top-level `;` terminators from a single-statement query.
    pub fn strip_trailing_terminators(sql: &str) -> String {
        crate::sql::strip_trailing_terminators(sql)
    }
    /// The distinct `@name` bind parameters of a query, in first-appearance order.
    pub fn named_parameters(sql: &str) -> Vec<String> {
        crate::sql::named_parameters(sql)
    }
    /// Backtick-quote a Spanner identifier, or reject a name backticks cannot safely contain.
    pub fn quote_ident(ident: &str) -> Option<String> {
        crate::sql::quote_ident(ident).ok()
    }
    /// Pair the parameters of `sql` with the columns of a batch named `column_names` (typed
    /// `Int64`; pairing ignores types). Returns `None` after asserting that a rejection is
    /// `InvalidArguments`.
    pub fn resolve_parameter_names(
        sql: &str,
        column_names: &[String],
        bind_by_name: bool,
    ) -> Option<Vec<String>> {
        use arrow_array::RecordBatch;
        use arrow_schema::{DataType, Field, Schema};
        let fields: Vec<Field> = column_names
            .iter()
            .map(|name| Field::new(name, DataType::Int64, true))
            .collect();
        let batch = RecordBatch::new_empty(std::sync::Arc::new(Schema::new(fields)));
        match crate::bind::resolve_parameter_names(sql, &batch, bind_by_name) {
            Ok(names) => Some(names),
            Err(e) => {
                assert_eq!(
                    e.status,
                    adbc_core::error::Status::InvalidArguments,
                    "resolve_parameter_names must reject with InvalidArguments: {e:?}"
                );
                None
            }
        }
    }
    /// Decode an opaque partition descriptor, returning whether it decoded.
    ///
    /// A rejected descriptor must fail cleanly with `InvalidArguments`. An accepted one must
    /// reach a byte-stable fixed point under the driver's encoder. The check starts from the
    /// encoder's own output because serde does not preserve every input form: an integer literal
    /// too large for `i64`/`u64` parses to an imprecise `f64`. One normalizing pass suffices.
    pub fn decode_partition(descriptor: &[u8]) -> bool {
        match crate::connection::decode_partition(descriptor) {
            Ok(partition) => {
                use crate::connection::{decode_partition, encode_partition};
                let first = encode_partition(&partition).expect("a decoded partition re-encodes");
                let normalized =
                    decode_partition(&first).expect("a re-encoded partition descriptor decodes");
                let bytes = encode_partition(&normalized).expect("a decoded partition re-encodes");
                let again =
                    decode_partition(&bytes).expect("a re-encoded partition descriptor decodes");
                assert_eq!(
                    bytes,
                    encode_partition(&again).expect("a decoded partition re-encodes"),
                    "enveloped partition descriptor round-trip changed the bytes"
                );
                true
            }
            Err(e) => {
                assert_eq!(
                    e.status,
                    adbc_core::error::Status::InvalidArguments,
                    "decode_partition must reject with InvalidArguments: {e:?}"
                );
                assert!(
                    e.message.contains("invalid partition descriptor")
                        || e.message.contains("not supported by this driver"),
                    "unexpected rejection message: {}",
                    e.message
                );
                false
            }
        }
    }
    /// Normalize an emulator endpoint by adding an `http://` scheme when absent.
    pub fn ensure_scheme(host: &str) -> String {
        crate::driver::ensure_scheme(host)
    }

    /// Parse a `spanner.read.staleness` value and map it to client `TimestampBound`s.
    pub fn parse_read_staleness(value: &str) {
        use adbc_core::options::OptionValue;
        let mut staleness = crate::staleness::ReadStaleness::default();
        if staleness
            .set_staleness(OptionValue::String(value.to_string()))
            .is_ok()
        {
            let _ = staleness.timestamp_bound();
            let _ = staleness.multi_use_timestamp_bound();
        }
    }

    /// Parse a duration in the grammar shared by `spanner.read.staleness` and
    /// `spanner.commit.max_delay`.
    pub fn parse_duration(value: &str) {
        let _ = crate::staleness::parse_duration(value);
    }

    /// Parse a `spanner.directed_read` value.
    pub fn parse_directed_read(value: &str) {
        let _ = crate::directed_read::parse(value);
    }

    /// An arbitrary [`OptionValue`](adbc_core::options::OptionValue).
    #[derive(arbitrary::Arbitrary, Debug)]
    pub enum OptValue {
        Str(String),
        Int(i64),
        Double(f64),
        Bytes(Vec<u8>),
    }

    /// A new database from a driver built once per process, so that runtime construction does
    /// not dominate fuzzing throughput.
    fn new_database() -> Option<crate::SpannerDatabase> {
        use adbc_core::Driver;
        use std::sync::{Mutex, OnceLock};

        static DRIVER: OnceLock<Mutex<crate::SpannerDriver>> = OnceLock::new();
        let driver = DRIVER.get_or_init(|| {
            Mutex::new(crate::SpannerDriver::try_new().expect("driver construction is infallible"))
        });
        driver.lock().unwrap().new_database().ok()
    }

    /// Set and read back arbitrary database options, as a driver manager forwarding untrusted
    /// strings would. Stops before `connect()`, so there is no network I/O.
    pub fn exercise_database_options(ops: Vec<(String, OptValue)>) {
        use adbc_core::Optionable;
        use adbc_core::options::{OptionDatabase, OptionValue};

        let Some(mut database) = new_database() else {
            return;
        };
        for (key, value) in ops {
            let value = match value {
                OptValue::Str(s) => OptionValue::String(s),
                OptValue::Int(i) => OptionValue::Int(i),
                OptValue::Double(d) => OptionValue::Double(d),
                OptValue::Bytes(b) => OptionValue::Bytes(b),
            };
            let _ = database.set_option(OptionDatabase::Other(key.clone()), value);
            let _ = database.get_option_string(OptionDatabase::Other(key));
        }
    }

    /// Set an arbitrary `uri` option, which [`exercise_database_options`] cannot reach because it
    /// only sets `Other(key)`. No network I/O.
    pub fn expand_connection_uri(uri: &str) {
        use adbc_core::Optionable;
        use adbc_core::options::{OptionDatabase, OptionValue};

        let Some(mut database) = new_database() else {
            return;
        };
        let _ = database.set_option(OptionDatabase::Uri, OptionValue::String(uri.to_string()));
        let _ = database.get_option_string(OptionDatabase::Uri);
    }

    #[cfg(test)]
    mod tests {
        /// Keep fuzz harnesses, manifest entries and CI discovery in sync.
        #[test]
        fn every_fuzz_target_is_wired_and_documented() {
            let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));

            // Each `[[bin]]` is immediately followed by `name = "..."`, the shape that
            // fuzz.yml's `discover` job parses.
            let manifest = std::fs::read_to_string(root.join("fuzz/Cargo.toml")).unwrap();
            let mut lines = manifest.lines();
            let mut declared = std::collections::BTreeSet::new();
            while let Some(line) = lines.next() {
                if line.trim() != "[[bin]]" {
                    continue;
                }
                let name = lines
                    .next()
                    .expect("a [[bin]] section is followed by its name")
                    .trim()
                    .strip_prefix("name = \"")
                    .and_then(|rest| rest.strip_suffix('"'))
                    .expect("a [[bin]] section starts with `name = \"...\"`");
                declared.insert(name.to_string());
            }

            let harnesses: std::collections::BTreeSet<String> =
                std::fs::read_dir(root.join("fuzz/fuzz_targets"))
                    .expect("fuzz_targets directory exists")
                    .map(|entry| entry.unwrap().path())
                    .filter(|path| path.extension().is_some_and(|ext| ext == "rs"))
                    .map(|path| path.file_stem().unwrap().to_str().unwrap().to_string())
                    .collect();
            assert_eq!(
                declared, harnesses,
                "every fuzz_targets/<name>.rs needs a [[bin]] in fuzz/Cargo.toml and vice versa \
                 (cargo-fuzz only runs declared targets, and fuzz.yml's matrix derives from them)"
            );

            let workflow =
                std::fs::read_to_string(root.join(".github/workflows/fuzz.yml")).unwrap();
            assert!(
                workflow.contains("fromJson(needs.discover.outputs.targets)"),
                "fuzz.yml must derive its matrix from fuzz/Cargo.toml via the `discover` job; a \
                 hardcoded target list silently skips new targets"
            );
        }

        /// Run the partition oracle over the checked-in seed corpus, so a corpus/oracle mismatch
        /// fails `cargo test --features fuzzing` rather than only a fuzz run.
        #[test]
        fn partition_seed_corpus_satisfies_the_oracle() {
            let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("fuzz/seeds/partition");
            let mut verdicts = std::collections::BTreeMap::new();
            for entry in std::fs::read_dir(dir).expect("seed corpus directory exists") {
                let path = entry.unwrap().path();
                let bytes = std::fs::read(&path).unwrap();
                let name = path.file_name().unwrap().to_str().unwrap().to_string();
                verdicts.insert(name, super::decode_partition(&bytes));
            }
            let expected: std::collections::BTreeMap<String, bool> = [
                ("bare-bignum-query-descriptor", false),
                ("enveloped-bad-version", false),
                ("enveloped-bignum-query-descriptor", true),
                ("enveloped-query-descriptor", true),
                ("enveloped-query-with-row-type", true),
            ]
            .into_iter()
            .map(|(name, accepted)| (name.to_string(), accepted))
            .collect();
            assert_eq!(verdicts, expected);
        }
    }
}

/// Internal conversion entry point for the benchmarks in `benches/`, which can only reach the
/// public API. Not a stable API.
#[doc(hidden)]
pub mod bench_support {
    use arrow_array::ArrayRef;
    use arrow_schema::DataType;
    use google_cloud_spanner::value::Value;

    /// Build an Arrow array of `data_type` from one Spanner [`Value`] per row, as `execute` does
    /// for each streamed chunk.
    pub fn build_array(
        data_type: &DataType,
        values: &[Option<&Value>],
    ) -> adbc_core::error::Result<ArrayRef> {
        crate::conversion::build_array(data_type, values)
    }
}

/// Database option: an explicit gRPC endpoint, such as an emulator's `http://localhost:9010`.
/// When unset, the client connects to the production service.
pub const OPTION_ENDPOINT: &str = "spanner.endpoint";

/// Database option: when `true`, connect with anonymous credentials, as the Spanner emulator
/// expects. Enabled automatically when `SPANNER_EMULATOR_HOST` is set. Combining it with an
/// explicit credential option or [`OPTION_QUOTA_PROJECT`] fails at connect time instead of
/// silently ignoring them.
pub const OPTION_EMULATOR: &str = "spanner.emulator";

/// Database option: `cloud` (default) or `omni`. Omni requires an explicit [`OPTION_ENDPOINT`],
/// cannot be combined with [`OPTION_EMULATOR`], and uses anonymous credentials unless
/// authentication options are supplied.
pub const OPTION_INSTANCE_TYPE: &str = "spanner.instance_type";

/// Database option: path to a JSON credential file such as a service-account key (dbt's
/// `keyfile`). [`OPTION_KEYFILE_JSON`] takes precedence when both are set. A path is not a
/// secret, so it can be read back with `get_option` and used in a connection URI.
pub const OPTION_KEYFILE: &str = "spanner.auth.keyfile";

/// Database option: an inline JSON credential such as a service-account key (dbt's
/// `keyfile_json`). Without this or [`OPTION_KEYFILE`], and outside emulator mode, the driver uses
/// Application Default Credentials.
///
/// Write-only: the value is a private key, so `get_option` always fails with
/// [`Status::NotFound`](adbc_core::error::Status::NotFound), and a `spanner://` URI carrying it is
/// rejected with `InvalidArguments`.
pub const OPTION_KEYFILE_JSON: &str = "spanner.auth.keyfile_json";

/// Database option: a service-account email to impersonate, like gcloud's
/// `--impersonate-service-account`. The base credentials (ADC or a keyfile) mint short-lived
/// access tokens for this principal through IAM Credentials `generateAccessToken`.
pub const OPTION_IMPERSONATE_TARGET_PRINCIPAL: &str = "spanner.auth.impersonate.target_principal";

/// Database option: a comma-separated delegation chain of service-account emails for
/// impersonation. Each needs the Token Creator role on the next, and the last on
/// [`OPTION_IMPERSONATE_TARGET_PRINCIPAL`]. Ignored without a target principal.
pub const OPTION_IMPERSONATE_DELEGATES: &str = "spanner.auth.impersonate.delegates";

/// Database option: comma-separated OAuth 2.0 scopes for the impersonated token. Defaults to the
/// `cloud-platform` scope. Ignored without a target principal.
pub const OPTION_IMPERSONATE_SCOPES: &str = "spanner.auth.impersonate.scopes";

/// Database option: the lifetime of the impersonated access token in seconds. Defaults to 3600.
/// Ignored without a target principal.
pub const OPTION_IMPERSONATE_LIFETIME: &str = "spanner.auth.impersonate.lifetime";

/// Database option: an OAuth 2.0 access token, sent as `Authorization: Bearer <token>`. The
/// driver never refreshes it.
///
/// It cannot be combined with [`OPTION_KEYFILE`], [`OPTION_KEYFILE_JSON`] or
/// [`OPTION_IMPERSONATE_TARGET_PRINCIPAL`] (connecting fails with
/// [`Status::InvalidState`](adbc_core::error::Status::InvalidState)), nor with emulator mode.
/// Write-only and rejected in URIs, like [`OPTION_KEYFILE_JSON`].
pub const OPTION_ACCESS_TOKEN: &str = "spanner.auth.access_token";

/// Database option: the quota (billing) project charged for Spanner API usage, sent as the
/// `x-goog-user-project` header, like gcloud's `--billing-project`. The caller needs
/// `serviceusage.services.use` on it. Works with every credential source but is rejected in
/// emulator mode. `""` unsets it. `GOOGLE_CLOUD_QUOTA_PROJECT` takes precedence over it in the
/// auth library.
pub const OPTION_QUOTA_PROJECT: &str = "spanner.auth.quota_project";

/// Statement option: the number of rows in each Arrow [`RecordBatch`](arrow_array::RecordBatch)
/// streamed by [`Statement::execute`](adbc_core::Statement::execute). Larger batches use more
/// memory; smaller ones return the first batch sooner. A positive integer; defaults to 8192.
pub const OPTION_ROWS_PER_BATCH: &str = "spanner.rows_per_batch";

/// Statement option: run the partitions of
/// [`Statement::execute_partitions`](adbc_core::Statement::execute_partitions) on Data Boost,
/// Spanner's serverless, workload-isolated compute. The setting is stored in each partition
/// descriptor, so whichever connection reads a partition honors it. Boolean, default `false`.
/// Omni rejects Data Boost requests.
pub const OPTION_DATA_BOOST: &str = "spanner.data_boost";

/// Statement option: how bound Arrow columns pair with the query's `@name` parameters, following
/// the SQLite driver's `bind_by_name`
/// ([apache/arrow-adbc#3362](https://github.com/apache/arrow-adbc/issues/3362)). Boolean,
/// default `false`:
///
/// - `false`: positional. The nth column binds to the nth distinct `@name` parameter in query
///   order, and column names are ignored. This is the ordinal binding ADBC clients expect.
/// - `true`: by name. Each column binds to the parameter with its name (case-insensitively, as
///   Spanner resolves names), in any order; a column naming no parameter fails with
///   `InvalidArguments`.
pub const OPTION_BIND_BY_NAME: &str = "adbc.statement.bind_by_name";

/// Statement option: send autocommit bulk-ingest mutations through Spanner's BatchWrite RPC
/// instead of a write-only transaction, for high-throughput loads that do not need atomicity.
/// Boolean, default `false`; `""` unsets it.
///
/// BatchWrite applies each chunk's mutation groups independently and non-atomically. Insert
/// semantics, chunking, the row count, the read-only check and the append-mode
/// `NotFound`/`AlreadyExists` mapping are unchanged. Manual-mode ingests ignore the option and
/// commit atomically with their transaction. Priority and the transaction tag apply;
/// `spanner.request.tag`, `spanner.commit.max_delay` and `spanner.commit_stats` do not, since
/// BatchWrite ignores request tags and has no commit options.
pub const OPTION_INGEST_BATCH_WRITE: &str = "spanner.ingest.batch_write";

/// Statement option: run the statement as
/// [Partitioned DML](https://docs.cloud.google.com/spanner/docs/dml-partitioned), which applies
/// each partition independently and so avoids the per-commit mutation limit. Boolean, default
/// `false`; `""` unsets it.
///
/// Partitioned DML weakens two guarantees:
///
/// - It is not atomic. Each partition commits separately and may be applied more than once, so
///   the statement must be idempotent (`SET active = true`, not `SET n = n + 1`).
/// - The row count is a lower bound. `execute_update` returns Spanner's `row_count_lower_bound`,
///   which undercounts when a partition was retried.
///
/// Spanner allows one statement per transaction and no `THEN RETURN` (both `InvalidArguments`),
/// and partitioned DML cannot join a manual transaction (`InvalidState`). Non-DML statements and
/// bulk ingests ignore the option. Commit options, `spanner.transaction.tag` and the isolation
/// level have no effect, since there is no commit.
pub const OPTION_DML_PARTITIONED: &str = "spanner.dml.partitioned";

/// Connection and statement option: the
/// [read bound](https://docs.cloud.google.com/spanner/docs/timestamp-bounds) for read-only
/// queries. Unset (the default) or `""` means a strong read.
///
/// The value takes one of four forms:
///
/// - `exact:<duration>`: read at exactly `<duration>` in the past.
/// - `max:<duration>`: read at a timestamp within `<duration>` of now chosen by Spanner (bounded
///   staleness; single-use reads only).
/// - `read:<rfc3339>`, or a bare `<rfc3339>`: read at exactly that timestamp.
/// - `min:<rfc3339>`: read at that timestamp or later (bounded staleness; single-use reads only).
///
/// `<duration>` is a non-negative number with an optional unit suffix: `s` (seconds, the default),
/// `ms`, `us`/`µs`, `ns`, `m` (minutes) or `h` (hours). Examples: `exact:10`, `exact:2.5s`,
/// `max:500ms`, `read:2026-07-07T00:00:00Z`, `min:2026-07-07T00:00:00+02:00`.
pub const OPTION_READ_STALENESS: &str = "spanner.read.staleness";

/// Connection and statement option: the
/// [request priority](https://docs.cloud.google.com/spanner/docs/reference/rest/v1/RequestOptions),
/// `low`, `medium` or `high`. Applies to queries, DML, BatchWrite, read/write transaction commits
/// and driver-internal metadata reads. Unset (the default) or `""` leaves the service default
/// (high).
pub const OPTION_REQUEST_PRIORITY: &str = "spanner.request.priority";

/// Connection and statement option: a
/// [directed read](https://docs.cloud.google.com/spanner/docs/directed-reads) replica selection
/// for read-only queries. Spanner rejects directed reads in read/write transactions, so DML and
/// DDL ignore it. Unset by default (Spanner's own routing); `""` unsets; malformed values fail
/// with `InvalidArguments`.
///
/// The value has the form:
///
/// ```text
/// <mode> [ ":" <selection> ("," <selection>)* ] [ ";auto_failover_disabled" ]
/// ```
///
/// - `<mode>` is `include` (an ordered preference list) or `exclude` (replicas to avoid).
/// - `<selection>` is `<location>`, `<location>:<type>` or `:<type>`, where `<location>` is a
///   region such as `us-east1` and `<type>` is `read_write`, `read_only` or `any` (the default,
///   matching every replica type).
/// - `;auto_failover_disabled`, valid only with `include`, stops Spanner from falling back to a
///   replica outside the list when the listed ones are unavailable.
///
/// Keywords are lowercase. Examples: `include:us-east1`,
/// `include:us-east1:read_only,us-east4:read_write`, `exclude:us-central1`,
/// `include:us-east1;auto_failover_disabled`.
pub const OPTION_DIRECTED_READ: &str = "spanner.directed_read";

/// Connection and statement option: a request tag attached to each query, DML statement and
/// `ExecuteBatchDml` batch, and reported in Spanner's query and transaction statistics; see
/// [troubleshooting with tags](https://docs.cloud.google.com/spanner/docs/introspection/troubleshooting-with-tags).
/// Unset by default; `""` unsets.
pub const OPTION_REQUEST_TAG: &str = "spanner.request.tag";

/// Connection and statement option: the query optimizer version, such as `"6"` or `"latest"`,
/// sent unchanged in each query's
/// [`QueryOptions`](https://docs.cloud.google.com/spanner/docs/reference/rest/v1/ExecuteSqlRequest#queryoptions).
/// Unset (the default) or `""` leaves the database or service default.
pub const OPTION_QUERY_OPTIMIZER_VERSION: &str = "spanner.query.optimizer_version";

/// Connection and statement option: the query optimizer statistics package, sent unchanged in
/// each query's `QueryOptions`. Unset (the default) or `""` leaves the database default.
pub const OPTION_QUERY_OPTIMIZER_STATISTICS_PACKAGE: &str =
    "spanner.query.optimizer_statistics_package";

/// Connection and statement option: the query timeout in seconds. It bounds the initial execution
/// of a query: the `ExecuteStreamingSql` call and first chunk, the `execute_schema` and
/// `execute_partitions` probes, the first fetch of `read_partition`, and driver-internal metadata
/// reads. Later chunks are bounded by [`OPTION_RPC_TIMEOUT_FETCH`]. On expiry the call fails with
/// [`Status::Timeout`](adbc_core::error::Status::Timeout).
///
/// A finite, non-negative number of seconds (fractions allowed), set as a numeric string, an
/// integer or a double, and readable with `get_option` or `get_option_double`. `0` disables the
/// timeout and `""` unsets it; unset (the default) means no deadline.
pub const OPTION_RPC_TIMEOUT_QUERY: &str = "spanner.rpc.timeout_seconds.query";

/// Connection and statement option: the update timeout in seconds, an overall deadline for each
/// write. Writes are autocommit DML and batch-DML transactions, manual-mode commits (including
/// the one made by re-enabling autocommit), each bulk-ingest commit chunk, and DDL changes,
/// including the long-running-operation polling that is otherwise unbounded. Client retries
/// count against the deadline. On expiry the call fails with
/// [`Status::Timeout`](adbc_core::error::Status::Timeout), although Spanner may still have
/// committed, as with any timed-out commit. Values are handled as for
/// [`OPTION_RPC_TIMEOUT_QUERY`].
pub const OPTION_RPC_TIMEOUT_UPDATE: &str = "spanner.rpc.timeout_seconds.update";

/// Connection and statement option: the fetch timeout in seconds, a deadline for each result
/// chunk after the first (which [`OPTION_RPC_TIMEOUT_QUERY`] covers). It is enforced in the
/// background prefetch task, so a stalled stream fails the next batch with
/// [`Status::Timeout`](adbc_core::error::Status::Timeout) instead of hanging. For a query with
/// bound parameters it also bounds each per-row execution as the stream advances. Values are
/// handled as for [`OPTION_RPC_TIMEOUT_QUERY`].
pub const OPTION_RPC_TIMEOUT_FETCH: &str = "spanner.rpc.timeout_seconds.fetch";

/// Connection and statement option: the maximum number of attempts, including the first, that
/// the Spanner client makes for a retryable RPC. A positive integer; `1` disables retries. Unset
/// (the default) keeps the client's policy: unlimited on unary RPCs and 10 attempts on streaming
/// queries and BatchWrite. A set value applies to all three.
///
/// Set as an integer, a whole-valued double or a numeric string; readable with `get_option` or
/// `get_option_int`; `""` unsets it. Setting this or [`OPTION_RETRY_MAX_ELAPSED_SECONDS`] bounds
/// the client's default policy without changing which errors it retries, stopping at whichever
/// limit is reached first. These options bound the retry loop; the [`OPTION_RPC_TIMEOUT_QUERY`]
/// family bounds the overall time of each operation.
pub const OPTION_RETRY_MAX_ATTEMPTS: &str = "spanner.retry.max_attempts";

/// Connection and statement option: the maximum total time in seconds the Spanner client spends
/// retrying before returning the last error. A finite, strictly positive number; unset (the
/// default) keeps the client's policy, which has no time limit. Values, interaction with
/// [`OPTION_RETRY_MAX_ATTEMPTS`] and inheritance work as for that option; readable with
/// `get_option` or `get_option_double`.
///
/// Applies only to unary RPCs (DML, `ExecuteBatchDml`, begin and commit); streaming queries and
/// BatchWrite ignore it. Use [`OPTION_RPC_TIMEOUT_QUERY`] and [`OPTION_RPC_TIMEOUT_FETCH`] to bound
/// queries, and [`OPTION_RPC_TIMEOUT_UPDATE`] for BatchWrite.
pub const OPTION_RETRY_MAX_ELAPSED_SECONDS: &str = "spanner.retry.max_elapsed_seconds";

/// Connection and statement option: the initial delay in seconds of the Spanner client's
/// exponential retry backoff. A finite, strictly positive number, set as a numeric string, an
/// integer or a double, and readable with `get_option` or `get_option_double`; `""` unsets it.
/// Unset (the default) keeps the client's initial delay of 1 second.
///
/// Setting this, [`OPTION_RETRY_BACKOFF_MAX_SECONDS`] or [`OPTION_RETRY_BACKOFF_MULTIPLIER`]
/// replaces the client's backoff. Unset knobs take the client defaults (1 s initial, 60 s maximum,
/// multiplier 2.0), and all are clamped to the gax recommended ranges. Independent of
/// [`OPTION_RETRY_MAX_ATTEMPTS`] and [`OPTION_RETRY_MAX_ELAPSED_SECONDS`].
pub const OPTION_RETRY_BACKOFF_INITIAL_SECONDS: &str = "spanner.retry.backoff.initial_seconds";

/// Connection and statement option: the maximum delay in seconds of the Spanner client's retry
/// backoff. A finite, strictly positive number; unset (the default) keeps the client's maximum of
/// 60 seconds. The gax clamp raises a value below the effective initial delay to that delay.
/// Otherwise handled as [`OPTION_RETRY_BACKOFF_INITIAL_SECONDS`].
pub const OPTION_RETRY_BACKOFF_MAX_SECONDS: &str = "spanner.retry.backoff.max_seconds";

/// Connection and statement option: the factor by which the Spanner client's retry backoff
/// multiplies the delay after each attempt. A finite, strictly positive number; unset (the
/// default) keeps the client's `2.0`. The gax clamp raises values below `1.0` to `1.0`, a constant
/// delay. Otherwise handled as [`OPTION_RETRY_BACKOFF_INITIAL_SECONDS`].
pub const OPTION_RETRY_BACKOFF_MULTIPLIER: &str = "spanner.retry.backoff.multiplier";

/// Connection option: a transaction tag, applied to each read/write transaction the driver starts
/// and attached by Spanner to every operation in it. Unset by default; `""` unsets. There is no
/// statement-level override, since a transaction can span several statements.
pub const OPTION_TRANSACTION_TAG: &str = "spanner.transaction.tag";

/// Connection and statement option: the
/// [maximum commit delay](https://docs.cloud.google.com/spanner/docs/reference/rest/v1/TransactionOptions)
/// Spanner may add to each read/write commit so it can batch commits, trading latency for
/// throughput.
///
/// A duration in the [`OPTION_READ_STALENESS`] grammar (e.g. `100ms`, `0.2s`) between 0 and
/// 500ms. Malformed and out-of-range values fail with
/// [`Status::InvalidArguments`](adbc_core::error::Status::InvalidArguments). `0` means no delay
/// and `""` unsets it.
pub const OPTION_MAX_COMMIT_DELAY: &str = "spanner.commit.max_delay";

/// Connection and statement option: request
/// [commit statistics](https://docs.cloud.google.com/spanner/docs/commit-statistics) for the
/// read/write commits the driver makes. Boolean (`true` or `false`), default `false`; `""` unsets
/// it.
///
/// The mutation count of the latest such commit is readable through
/// [`OPTION_COMMIT_STATS_MUTATION_COUNT`]: on the statement for autocommit DML and bulk ingest,
/// and on the connection for a manual-mode commit.
pub const OPTION_COMMIT_STATS: &str = "spanner.commit_stats";

/// Read-only connection and statement option: the mutation count of the latest commit made with
/// [`OPTION_COMMIT_STATS`] enabled, readable with `get_option` or `get_option_int`.
///
/// Reading fails with [`Status::NotFound`](adbc_core::error::Status::NotFound) until such a
/// commit has run on the object: the statement for autocommit DML and bulk ingest, or the
/// connection for a manual-mode commit. When several commits run, as in a chunked ingest, it
/// reports the last one. Setting it fails with
/// [`Status::NotImplemented`](adbc_core::error::Status::NotImplemented).
pub const OPTION_COMMIT_STATS_MUTATION_COUNT: &str = "spanner.commit_stats.mutation_count";

/// Connection and statement option: whether to
/// [exclude a transaction's writes from change streams](https://docs.cloud.google.com/spanner/docs/reference/rest/v1/TransactionOptions).
/// Boolean (`true` or `false`), default `false`; `""` unsets it.
///
/// Applies to every write the driver makes, including `spanner.ingest.batch_write`. Spanner only
/// honors it for change streams created with the `allow_txn_exclusion = true` option; other
/// change streams record the writes regardless.
pub const OPTION_EXCLUDE_TXN_FROM_CHANGE_STREAMS: &str =
    "spanner.transaction.exclude_from_change_streams";

/// Connection and statement option: the maximum precision at which Spanner `TIMESTAMP` columns
/// are read into Arrow. `""` restores the default.
///
/// - `nanoseconds_error_on_overflow` (the default): `Timestamp(Nanosecond, "UTC")`, keeping full
///   precision. Arrow's `i64` nanoseconds only cover about 1677-09-21 to 2262-04-11, narrower than
///   Spanner's years 0001–9999, so a timestamp outside that window fails with `InvalidArguments`
///   naming the column and value.
/// - `microseconds`: `Timestamp(Microsecond, "UTC")`, covering Spanner's full range, like the
///   Snowflake driver's `adbc.snowflake.sql.client_option.max_timestamp_precision`.
///   Sub-microsecond digits are truncated toward negative infinity.
///
/// There is no wrapping nanosecond mode, since a wrapped timestamp is indistinguishable from real
/// data. The mode applies to every result and schema with timestamps; `read_partition` uses the
/// reading connection's setting.
pub const OPTION_MAX_TIMESTAMP_PRECISION: &str = "spanner.max_timestamp_precision";

/// The vendor name reported by [`Connection::get_info`](adbc_core::Connection::get_info).
pub const VENDOR_NAME: &str = "Google Cloud Spanner";

/// The driver name reported by [`Connection::get_info`](adbc_core::Connection::get_info).
pub const DRIVER_NAME: &str = "spanner-adbc";

/// The version of this driver.
pub const DRIVER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Every option key the driver handles must appear in `docs/options.md`.
#[cfg(test)]
mod options_doc_tests {
    #[test]
    fn every_handled_option_key_is_documented() {
        use adbc_core::constants;

        let doc = include_str!("../docs/options.md");
        let keys: &[&str] = &[
            crate::OPTION_ENDPOINT,
            crate::OPTION_EMULATOR,
            crate::OPTION_INSTANCE_TYPE,
            crate::OPTION_KEYFILE,
            crate::OPTION_KEYFILE_JSON,
            crate::OPTION_IMPERSONATE_TARGET_PRINCIPAL,
            crate::OPTION_IMPERSONATE_DELEGATES,
            crate::OPTION_IMPERSONATE_SCOPES,
            crate::OPTION_IMPERSONATE_LIFETIME,
            crate::OPTION_ACCESS_TOKEN,
            crate::OPTION_QUOTA_PROJECT,
            crate::OPTION_ROWS_PER_BATCH,
            crate::OPTION_DATA_BOOST,
            crate::OPTION_BIND_BY_NAME,
            crate::OPTION_INGEST_BATCH_WRITE,
            crate::OPTION_DML_PARTITIONED,
            crate::OPTION_READ_STALENESS,
            crate::OPTION_REQUEST_PRIORITY,
            crate::OPTION_REQUEST_TAG,
            crate::OPTION_DIRECTED_READ,
            crate::OPTION_QUERY_OPTIMIZER_VERSION,
            crate::OPTION_QUERY_OPTIMIZER_STATISTICS_PACKAGE,
            crate::OPTION_TRANSACTION_TAG,
            crate::OPTION_MAX_COMMIT_DELAY,
            crate::OPTION_COMMIT_STATS,
            crate::OPTION_COMMIT_STATS_MUTATION_COUNT,
            crate::OPTION_EXCLUDE_TXN_FROM_CHANGE_STREAMS,
            crate::OPTION_MAX_TIMESTAMP_PRECISION,
            crate::OPTION_RPC_TIMEOUT_QUERY,
            crate::OPTION_RPC_TIMEOUT_UPDATE,
            crate::OPTION_RPC_TIMEOUT_FETCH,
            crate::OPTION_RETRY_MAX_ATTEMPTS,
            crate::OPTION_RETRY_MAX_ELAPSED_SECONDS,
            crate::OPTION_RETRY_BACKOFF_INITIAL_SECONDS,
            crate::OPTION_RETRY_BACKOFF_MAX_SECONDS,
            crate::OPTION_RETRY_BACKOFF_MULTIPLIER,
            // Standard ADBC options.
            constants::ADBC_OPTION_URI,
            constants::ADBC_CONNECTION_OPTION_AUTOCOMMIT,
            constants::ADBC_CONNECTION_OPTION_READ_ONLY,
            constants::ADBC_CONNECTION_OPTION_ISOLATION_LEVEL,
            constants::ADBC_CONNECTION_OPTION_CURRENT_CATALOG,
            constants::ADBC_CONNECTION_OPTION_CURRENT_DB_SCHEMA,
            constants::ADBC_INGEST_OPTION_TARGET_TABLE,
            constants::ADBC_INGEST_OPTION_TARGET_DB_SCHEMA,
            constants::ADBC_INGEST_OPTION_TARGET_CATALOG,
            constants::ADBC_INGEST_OPTION_TEMPORARY,
            constants::ADBC_INGEST_OPTION_MODE,
        ];
        for key in keys {
            // Backticks keep a short key like `uri` from matching inside another word.
            assert!(
                doc.contains(&format!("`{key}`")),
                "option key `{key}` is handled by the driver but missing from docs/options.md"
            );
        }
    }
}

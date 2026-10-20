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
//! Every level is configured through ADBC options, and statements inherit the options of their
//! connection. See [docs/options.md](https://github.com/fornwall/spanner-adbc/blob/main/docs/options.md)
//! for the reference. The standard [`OptionDatabase::Uri`](adbc_core::options::OptionDatabase::Uri)
//! option selects the database with a `spanner://` URI, whose query parameters can set other
//! database options:
//!
//! ```text
//! spanner:///projects/<p>/instances/<i>/databases/<d>?spanner.emulator=true
//! ```
//!
//! ## Transactions
//!
//! Connections start in autocommit mode. [`SpannerConnection`] describes manual transactions,
//! which hold either queries or buffered writes.
//!
//! ## Async callers
//!
//! The ADBC traits are synchronous, and each call blocks on the driver's own Tokio runtime. From
//! async code, call the driver inside `tokio::task::spawn_blocking` or on another thread; a call
//! on a current-thread runtime's own thread panics. See
//! [synchronous calls from Tokio](https://github.com/fornwall/spanner-adbc/blob/main/docs/limitations.md#synchronous-calls-from-tokio).
//!
//! ## Example
//!
//! ```no_run
//! use adbc_core::{Driver, Database, Connection, Statement};
//! use adbc_core::options::{OptionDatabase, OptionValue};
//! use spanner_adbc::SpannerDriver;
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
#[cfg(test)]
mod mock_server;
mod nested;
mod objects;
mod options;
mod query_options;
mod reader;
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

    /// Parse a `spanner.read.staleness` value into client `TimestampBound`s.
    pub fn parse_read_staleness(value: &str) {
        use adbc_core::options::OptionValue;
        let mut staleness = crate::staleness::ReadStaleness::default();
        if staleness
            .set_staleness(OptionValue::String(value.to_string()))
            .is_ok()
        {
            assert_eq!(
                staleness.timestamp_bound().is_some(),
                staleness.multi_use_timestamp_bound().is_some()
            );
        }
    }

    /// Parse a duration in the grammar shared by `spanner.read.staleness` and
    /// `spanner.commit.max_delay`.
    pub fn parse_duration(value: &str) {
        let _ = crate::staleness::parse_duration(value, crate::OPTION_READ_STALENESS);
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
        fn every_fuzz_target_is_wired() {
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

/// Database option: the gRPC endpoint, such as an emulator's `localhost:9010`. A bare `host:port`
/// gets `http://`, and every scheme but `https://` connects over plaintext. Unset or `""` uses
/// `SPANNER_EMULATOR_HOST` if set, else the production service.
pub const OPTION_ENDPOINT: &str = "spanner.endpoint";

/// Database option: `true` connects with anonymous credentials, as the emulator expects. A
/// non-empty `SPANNER_EMULATOR_HOST` also turns it on. Connecting fails without an endpoint, or
/// with a credential option or [`OPTION_QUOTA_PROJECT`] set.
pub const OPTION_EMULATOR: &str = "spanner.emulator";

/// Database option: `cloud` (the default) or `omni`. Omni needs an explicit [`OPTION_ENDPOINT`],
/// can't be combined with [`OPTION_EMULATOR`], and connects anonymously unless a credential
/// option is set.
pub const OPTION_INSTANCE_TYPE: &str = "spanner.instance_type";

/// Database option: the path of a JSON credential file, such as a service-account key (dbt's
/// `keyfile`). [`OPTION_KEYFILE_JSON`] takes precedence. Unlike it, a path can be read back and
/// set in a URI.
pub const OPTION_KEYFILE: &str = "spanner.auth.keyfile";

/// Database option: an inline JSON credential, such as a service-account key (dbt's
/// `keyfile_json`). Write-only: getting it returns
/// [`Status::NotFound`](adbc_core::error::Status::NotFound), and a URI can't set it.
pub const OPTION_KEYFILE_JSON: &str = "spanner.auth.keyfile_json";

/// Database option: a service account to impersonate, like gcloud's
/// `--impersonate-service-account`. The keyfile or Application Default Credentials mint
/// short-lived tokens for it.
pub const OPTION_IMPERSONATE_TARGET_PRINCIPAL: &str = "spanner.auth.impersonate.target_principal";

/// Database option: a comma-separated delegation chain of service accounts for
/// [`OPTION_IMPERSONATE_TARGET_PRINCIPAL`]. Each needs the Token Creator role on the next, and
/// the last on the target.
pub const OPTION_IMPERSONATE_DELEGATES: &str = "spanner.auth.impersonate.delegates";

/// Database option: comma-separated OAuth 2.0 scopes for the impersonated token. Defaults to
/// `cloud-platform`.
pub const OPTION_IMPERSONATE_SCOPES: &str = "spanner.auth.impersonate.scopes";

/// Database option: the impersonated token's lifetime in whole seconds. Defaults to 3600.
pub const OPTION_IMPERSONATE_LIFETIME: &str = "spanner.auth.impersonate.lifetime";

/// Database option: an OAuth 2.0 access token, sent as a bearer token and never refreshed. It
/// can't be combined with a keyfile, inline JSON, impersonation or emulator mode, and is
/// write-only like [`OPTION_KEYFILE_JSON`].
pub const OPTION_ACCESS_TOKEN: &str = "spanner.auth.access_token";

/// Database option: the project billed for API quota, sent as `x-goog-user-project` like gcloud's
/// `--billing-project`; `""` unsets it. With Application Default Credentials,
/// `GOOGLE_CLOUD_QUOTA_PROJECT` takes precedence. Anonymous connections reject it.
pub const OPTION_QUOTA_PROJECT: &str = "spanner.auth.quota_project";

/// Database option: the deadline in seconds for building the client stack on the first
/// connection, including session creation, which otherwise retries without limit while the
/// endpoint is unreachable. Defaults to 30; `0` disables it and `""` restores the default.
pub const OPTION_RPC_TIMEOUT_CONNECT: &str = "spanner.rpc.timeout_seconds.connect";

/// Statement option: the maximum number of rows in each Arrow
/// [`RecordBatch`](arrow_array::RecordBatch) that `execute` returns, a positive integer that
/// defaults to 8192. Streamed batches are also capped by size.
pub const OPTION_ROWS_PER_BATCH: &str = "spanner.rows_per_batch";

/// Statement option: `true` runs the partitions of
/// [`Statement::execute_partitions`](adbc_core::Statement::execute_partitions) on
/// [Data Boost](https://docs.cloud.google.com/spanner/docs/databoost/databoost-overview). Each
/// partition descriptor carries the setting. Omni doesn't support it.
pub const OPTION_DATA_BOOST: &str = "spanner.data_boost";

/// Statement option: `true` binds each Arrow column to the `@name` parameter with its name,
/// case-insensitively, like the SQLite driver's option
/// ([apache/arrow-adbc#3362](https://github.com/apache/arrow-adbc/issues/3362)). The default,
/// `false`, binds the nth column to the nth distinct parameter in query order.
pub const OPTION_BIND_BY_NAME: &str = "adbc.statement.bind_by_name";

/// Statement option: `true` sends autocommit bulk ingest through Spanner's non-atomic BatchWrite
/// RPC instead of a write-only transaction. Manual-mode ingest ignores it. Request tags, the
/// commit delay and commit statistics don't apply to BatchWrite.
pub const OPTION_INGEST_BATCH_WRITE: &str = "spanner.ingest.batch_write";

/// Statement option: `true` runs DML as
/// [Partitioned DML](https://docs.cloud.google.com/spanner/docs/dml-partitioned), which avoids the
/// per-commit mutation limit. Partitions commit separately and may be applied more than once, so
/// the statement must be idempotent, and the row count is a lower bound. It needs autocommit and
/// one statement without `THEN RETURN`.
pub const OPTION_DML_PARTITIONED: &str = "spanner.dml.partitioned";

/// Connection and statement option: the
/// [read bound](https://docs.cloud.google.com/spanner/docs/timestamp-bounds) for read-only
/// queries. Unset or `""` means a strong read. The forms are:
///
/// - `exact:<duration>`: read exactly `<duration>` in the past.
/// - `max:<duration>`: Spanner picks a timestamp at most `<duration>` old.
/// - `read:<rfc3339>`, or a bare `<rfc3339>`: read at that timestamp.
/// - `min:<rfc3339>`: Spanner picks that timestamp or a later one.
///
/// A duration is a non-negative number with an optional unit, `s` (the default), `ms`,
/// `us`/`µs`, `ns`, `m` or `h`, as in `exact:2.5s` or `max:500ms`. Spanner allows `max:` and
/// `min:` only on single-use reads, so reads that share a transaction use the equivalent `exact:`
/// or `read:` bound.
pub const OPTION_READ_STALENESS: &str = "spanner.read.staleness";

/// Connection and statement option: the
/// [request priority](https://docs.cloud.google.com/spanner/docs/reference/rest/v1/RequestOptions)
/// of queries, DML, commits, BatchWrite and internal metadata reads: `low`, `medium` or `high`.
/// Unset or `""` leaves the service default, `high`.
pub const OPTION_REQUEST_PRIORITY: &str = "spanner.request.priority";

/// Connection and statement option: the
/// [directed read](https://docs.cloud.google.com/spanner/docs/directed-reads) replica selection
/// for read-only queries. Unset or `""` leaves the routing to Spanner. The form is
///
/// ```text
/// <mode> [ ":" <selection> ("," <selection>)* ] [ ";auto_failover_disabled" ]
/// ```
///
/// where `<mode>` is `include` (in order of preference) or `exclude`, and a `<selection>` is
/// `<location>`, `<location>:<type>` or `:<type>`, with `<type>` one of `read_write`, `read_only`
/// or `any` (the default). `;auto_failover_disabled`, valid only with `include`, stops Spanner
/// from falling back to replicas outside the list. Examples: `include:us-east1`,
/// `include:us-east1:read_only,us-east4:read_write`, `exclude:us-central1`.
pub const OPTION_DIRECTED_READ: &str = "spanner.directed_read";

/// Connection and statement option: the
/// [request tag](https://docs.cloud.google.com/spanner/docs/introspection/troubleshooting-with-tags)
/// of each query, DML statement and batch DML request. `""` unsets it.
pub const OPTION_REQUEST_TAG: &str = "spanner.request.tag";

/// Connection and statement option: the query optimizer version, such as `6` or `latest`, sent
/// in each query's
/// [`QueryOptions`](https://docs.cloud.google.com/spanner/docs/reference/rest/v1/ExecuteSqlRequest#queryoptions).
/// Unset or `""` leaves the database default.
pub const OPTION_QUERY_OPTIMIZER_VERSION: &str = "spanner.query.optimizer_version";

/// Connection and statement option: the query optimizer statistics package sent in each query's
/// `QueryOptions`. Unset or `""` leaves the database default.
pub const OPTION_QUERY_OPTIMIZER_STATISTICS_PACKAGE: &str =
    "spanner.query.optimizer_statistics_package";

/// Connection and statement option: the deadline in seconds for a query's first chunk, the
/// `execute_schema` and `execute_partitions` probes, a partition's first chunk and internal
/// metadata reads, including retries. Fractions are allowed, and `0` or `""` (the default) means
/// no deadline. Expiry fails with [`Status::Timeout`](adbc_core::error::Status::Timeout).
pub const OPTION_RPC_TIMEOUT_QUERY: &str = "spanner.rpc.timeout_seconds.query";

/// Connection and statement option: the deadline in seconds for each write, including retries: a
/// DML transaction, a manual commit, an ingest chunk, BatchWrite, partitioned DML or a DDL
/// operation. A write that timed out may still have been applied. Values are as for
/// [`OPTION_RPC_TIMEOUT_QUERY`].
pub const OPTION_RPC_TIMEOUT_UPDATE: &str = "spanner.rpc.timeout_seconds.update";

/// Connection and statement option: the deadline in seconds for each result chunk after the
/// first, so that a stalled stream fails instead of hanging. Values are as for
/// [`OPTION_RPC_TIMEOUT_QUERY`].
pub const OPTION_RPC_TIMEOUT_FETCH: &str = "spanner.rpc.timeout_seconds.fetch";

/// Connection and statement option: the maximum number of attempts, including the first, for
/// each query, DML, BatchWrite, begin or commit RPC. `1` disables retries. Unset keeps the
/// client's policy: no limit on unary RPCs and 10 attempts on streaming queries and BatchWrite.
pub const OPTION_RETRY_MAX_ATTEMPTS: &str = "spanner.retry.max_attempts";

/// Connection and statement option: the maximum time in seconds the client spends retrying a
/// DML, batch DML, begin or commit RPC. Unset means no limit. Streaming queries and BatchWrite
/// ignore it, so bound them with the [`OPTION_RPC_TIMEOUT_QUERY`] family instead.
pub const OPTION_RETRY_MAX_ELAPSED_SECONDS: &str = "spanner.retry.max_elapsed_seconds";

/// Connection and statement option: the initial retry backoff in seconds. Setting any backoff
/// option replaces the client's backoff, with the others at the client defaults: 1 second
/// initial, 60 seconds maximum and a multiplier of 2.
pub const OPTION_RETRY_BACKOFF_INITIAL_SECONDS: &str = "spanner.retry.backoff.initial_seconds";

/// Connection and statement option: the maximum retry backoff in seconds; see
/// [`OPTION_RETRY_BACKOFF_INITIAL_SECONDS`].
pub const OPTION_RETRY_BACKOFF_MAX_SECONDS: &str = "spanner.retry.backoff.max_seconds";

/// Connection and statement option: the factor by which the retry backoff grows after each
/// attempt; see [`OPTION_RETRY_BACKOFF_INITIAL_SECONDS`].
pub const OPTION_RETRY_BACKOFF_MULTIPLIER: &str = "spanner.retry.backoff.multiplier";

/// Connection option: the transaction tag of each read/write and write-only transaction and
/// BatchWrite request. `""` unsets it. Statements can't override it, since a transaction can span
/// several statements.
pub const OPTION_TRANSACTION_TAG: &str = "spanner.transaction.tag";

/// Connection and statement option: the
/// [maximum commit delay](https://docs.cloud.google.com/spanner/docs/reference/rest/v1/TransactionOptions)
/// that Spanner may add to read/write and write-only commits to batch them, a duration from `0`
/// to `500ms` written as for [`OPTION_READ_STALENESS`]. `""` unsets it.
pub const OPTION_MAX_COMMIT_DELAY: &str = "spanner.commit.max_delay";

/// Connection and statement option: `true` requests
/// [commit statistics](https://docs.cloud.google.com/spanner/docs/commit-statistics) for
/// read/write and write-only commits, reported by [`OPTION_COMMIT_STATS_MUTATION_COUNT`].
pub const OPTION_COMMIT_STATS: &str = "spanner.commit_stats";

/// Read-only connection and statement option: the mutation count of the latest commit made with
/// [`OPTION_COMMIT_STATS`]. A statement reports its autocommit DML and ingest commits, and a
/// connection its manual commits. Until then, getting it returns
/// [`Status::NotFound`](adbc_core::error::Status::NotFound).
pub const OPTION_COMMIT_STATS_MUTATION_COUNT: &str = "spanner.commit_stats.mutation_count";

/// Connection and statement option: `true`
/// [excludes the driver's writes](https://docs.cloud.google.com/spanner/docs/reference/rest/v1/TransactionOptions)
/// from change streams created with `allow_txn_exclusion = true`.
pub const OPTION_EXCLUDE_TXN_FROM_CHANGE_STREAMS: &str =
    "spanner.transaction.exclude_from_change_streams";

/// Connection and statement option: the Arrow type of Spanner `TIMESTAMP` columns.
///
/// - `nanoseconds_error_on_overflow` (the default): `Timestamp(Nanosecond, "UTC")`. A value
///   outside about 1677 to 2262, the range of `i64` nanoseconds, fails with `InvalidData`.
/// - `microseconds`: `Timestamp(Microsecond, "UTC")`, which covers Spanner's years 1 to 9999,
///   like the Snowflake driver's `max_timestamp_precision`. Sub-microsecond digits are floored.
///
/// `read_partition` uses the reading connection's setting.
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
        // Split so that this needle does not match itself.
        let driver_keys = include_str!("lib.rs")
            .split(concat!("pub const ", "OPTION_"))
            .skip(1)
            .map(|rest| {
                rest.split('"')
                    .nth(1)
                    .expect("an option constant is a string")
            });
        let standard_keys = [
            constants::ADBC_OPTION_URI,
            constants::ADBC_CONNECTION_OPTION_AUTOCOMMIT,
            constants::ADBC_CONNECTION_OPTION_READ_ONLY,
            constants::ADBC_CONNECTION_OPTION_ISOLATION_LEVEL,
            constants::ADBC_CONNECTION_OPTION_CURRENT_CATALOG,
            constants::ADBC_CONNECTION_OPTION_CURRENT_DB_SCHEMA,
            constants::ADBC_STATEMENT_OPTION_INCREMENTAL,
            constants::ADBC_INGEST_OPTION_TARGET_TABLE,
            constants::ADBC_INGEST_OPTION_TARGET_DB_SCHEMA,
            constants::ADBC_INGEST_OPTION_TARGET_CATALOG,
            constants::ADBC_INGEST_OPTION_TEMPORARY,
            constants::ADBC_INGEST_OPTION_MODE,
        ];
        let mut count = 0;
        for key in driver_keys.chain(standard_keys) {
            count += 1;
            // Backticks keep a short key like `uri` from matching inside another word.
            assert!(
                doc.contains(&format!("`{key}`")),
                "option key `{key}` is handled by the driver but missing from docs/options.md"
            );
        }
        assert!(count > standard_keys.len(), "found no OPTION_ constants");
    }
}

//! Fault injection against an in-process mock Spanner gRPC server. Needs no backend.
//!
//! The server is `spanner-grpc-mock`, the mockall/tonic mock the pinned `google-cloud-spanner`
//! client uses for its own tests, on the same revision so the protos match. The driver reaches it
//! as it would an emulator (`spanner.endpoint` plus `spanner.emulator=true`). Only the data-plane
//! `google.spanner.v1.Spanner` service is scripted; [`GatedEndpoint`] makes the admin channel
//! silent for DDL deadline and cancellation tests.
//!
//! mockall matches expectations in FIFO order. [`MockServer::start`] serves `CreateSession` before
//! the test's script and rejects every unscripted RPC with `UNIMPLEMENTED` after it: a mockall
//! panic in the server task would reach the client as a retryable transport error and hang.
//! Call counts such as `.times(n)` are only upper bounds: the mock is dropped with the aborted
//! server task, which swallows unmet expectations, so tests count calls themselves.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use adbc_core::error::Status as AdbcStatus;
use adbc_core::options::{OptionConnection, OptionDatabase, OptionStatement, OptionValue};
use adbc_core::{Connection, Database, Driver, Optionable, Statement};
use arrow_array::cast::AsArray;
use arrow_array::{Date32Array, Int64Array, RecordBatch, RecordBatchReader, StringArray};
use arrow_schema::{ArrowError, DataType, Field, Schema};
use prost::Message;
use spanner_adbc::{SpannerConnection, SpannerDatabase, SpannerDriver, SpannerStatement};
use spanner_grpc_mock::MockSpanner;
use spanner_grpc_mock::google::spanner::v1;

/// Not validated by the mock; session names are derived from it.
const DATABASE: &str = "projects/mock-project/instances/mock-instance/databases/mock-db";

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

/// A mock Spanner server on an ephemeral localhost port, with its own Tokio runtime.
struct MockServer {
    /// `http://127.0.0.1:<port>`
    endpoint: String,
    server: tokio::task::JoinHandle<()>,
    _runtime: tokio::runtime::Runtime,
}

impl MockServer {
    fn start(script: impl FnOnce(&mut MockSpanner)) -> Self {
        let mut mock = MockSpanner::new();
        serve_sessions(&mut mock);
        script(&mut mock);
        Self::serve(mock)
    }

    fn serve(mut mock: MockSpanner) -> Self {
        reject_unscripted_rpcs(&mut mock);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("build mock-server runtime");
        let (endpoint, server) = runtime
            .block_on(spanner_grpc_mock::start("127.0.0.1:0", mock))
            .expect("start mock Spanner server");
        Self {
            endpoint,
            server,
            _runtime: runtime,
        }
    }

    fn connect(&self) -> SpannerConnection {
        connect_to(&self.endpoint)
    }
}

fn connect_to(endpoint: &str) -> SpannerConnection {
    emulator_database(endpoint)
        .new_connection()
        .expect("connect to mock server")
}

/// A database in emulator mode (plaintext, anonymous credentials). The explicit endpoint overrides
/// any ambient `SPANNER_EMULATOR_HOST`.
fn emulator_database(endpoint: &str) -> SpannerDatabase {
    let mut driver = SpannerDriver::try_new().expect("create driver");
    driver
        .new_database_with_opts([
            (
                OptionDatabase::Uri,
                OptionValue::String(format!("spanner:///{DATABASE}")),
            ),
            (
                OptionDatabase::Other(spanner_adbc::OPTION_ENDPOINT.into()),
                OptionValue::String(endpoint.to_string()),
            ),
            (
                OptionDatabase::Other(spanner_adbc::OPTION_EMULATOR.into()),
                OptionValue::String("true".into()),
            ),
        ])
        .expect("create database")
}

impl Drop for MockServer {
    fn drop(&mut self) {
        self.server.abort();
    }
}

/// The client creates one multiplexed session per `DatabaseClient`, and a background task may
/// re-create it, so this expectation is unbounded.
fn serve_sessions(mock: &mut MockSpanner) {
    mock.expect_create_session().returning(|request| {
        let database = request.into_inner().database;
        Ok(tonic::Response::new(v1::Session {
            name: format!("{database}/sessions/mock-session"),
            multiplexed: true,
            ..Default::default()
        }))
    });
}

/// Catch-alls returning `UNIMPLEMENTED`, which the client does not retry.
fn reject_unscripted_rpcs(mock: &mut MockSpanner) {
    macro_rules! reject {
        ($($method:ident),+ $(,)?) => {
            $(
                mock.$method().returning(|_| {
                    Err(tonic::Status::unimplemented(concat!(
                        "mock spanner server: unscripted RPC ",
                        stringify!($method),
                    )))
                });
            )+
        };
    }
    reject!(
        expect_create_session,
        expect_batch_create_sessions,
        expect_get_session,
        expect_list_sessions,
        expect_delete_session,
        expect_execute_sql,
        expect_execute_streaming_sql,
        expect_execute_batch_dml,
        expect_read,
        expect_streaming_read,
        expect_begin_transaction,
        expect_commit,
        expect_rollback,
        expect_partition_query,
        expect_partition_read,
        expect_batch_write,
        expect_fetch_cache_update,
    );
}

/// Aborts the test process if `what` outlives `limit`, so a hang fails CI instead of stalling it.
struct Watchdog {
    disarmed: Arc<AtomicBool>,
}

impl Watchdog {
    fn arm(limit: Duration, what: &'static str) -> Self {
        let disarmed = Arc::new(AtomicBool::new(false));
        let flag = disarmed.clone();
        std::thread::spawn(move || {
            std::thread::sleep(limit);
            if !flag.load(Ordering::SeqCst) {
                eprintln!("watchdog: {what} still running after {limit:?}; aborting");
                std::process::abort();
            }
        });
        Self { disarmed }
    }
}

impl Drop for Watchdog {
    fn drop(&mut self) {
        self.disarmed.store(true, Ordering::SeqCst);
    }
}

/// Arms a [`Watchdog`] named after the enclosing function. The default limit is two minutes.
macro_rules! watchdog {
    () => {
        watchdog!(Duration::from_secs(120))
    };
    ($limit:expr) => {{
        fn marker() {}
        fn name_of<T>(_: T) -> &'static str {
            std::any::type_name::<T>()
        }
        // `type_name` of a nested fn item is `<crate>::<enclosing fn>::marker`.
        let path = name_of(marker);
        let enclosing = path
            .strip_suffix("::marker")
            .and_then(|path| path.rsplit("::").next())
            .unwrap_or(path);
        Watchdog::arm($limit, enclosing)
    }};
}

/// A TCP proxy in front of a [`MockServer`] that can stop answering new connections.
///
/// The data and admin clients share the endpoint but use separate channels. Connect first so
/// `CreateSession` gets through, then call [`go_silent`](Self::go_silent): the admin channel is
/// opened lazily, so its socket is accepted and never answered, and DDL waits until cancelled or
/// timed out.
struct GatedEndpoint {
    endpoint: String,
    silent: Arc<AtomicBool>,
}

impl GatedEndpoint {
    fn start(upstream: &str) -> Self {
        let upstream = upstream.trim_start_matches("http://").to_string();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind gate listener");
        let address = listener.local_addr().expect("gate address");
        let silent = Arc::new(AtomicBool::new(false));

        let gate_silent = silent.clone();
        std::thread::spawn(move || {
            // Held open until the process exits, so the peer sees a live connection that never
            // replies.
            let mut parked = Vec::new();
            for client in listener.incoming() {
                let Ok(client) = client else { continue };
                if gate_silent.load(Ordering::SeqCst) {
                    parked.push(client);
                    continue;
                }
                let Ok(server) = std::net::TcpStream::connect(&upstream) else {
                    continue;
                };
                // Pump both directions; each half ends when its side closes.
                for (mut from, mut to) in [
                    (
                        client.try_clone().expect("clone client socket"),
                        server.try_clone().expect("clone server socket"),
                    ),
                    (server, client),
                ] {
                    std::thread::spawn(move || {
                        let _ = std::io::copy(&mut from, &mut to);
                    });
                }
            }
        });

        Self {
            endpoint: format!("http://{address}"),
            silent,
        }
    }

    /// Black-holes new connections; established ones keep flowing.
    fn go_silent(&self) {
        self.silent.store(true, Ordering::SeqCst);
    }

    fn connect(&self) -> SpannerConnection {
        connect_to(&self.endpoint)
    }
}

// ---------------------------------------------------------------------------
// PartialResultSet scripting helpers
// ---------------------------------------------------------------------------

fn result_metadata(fields: &[(&str, v1::TypeCode)]) -> v1::ResultSetMetadata {
    v1::ResultSetMetadata {
        row_type: Some(v1::StructType {
            fields: fields
                .iter()
                .map(|(name, code)| v1::struct_type::Field {
                    name: name.to_string(),
                    r#type: Some(v1::Type {
                        code: *code as i32,
                        ..Default::default()
                    }),
                })
                .collect(),
        }),
        ..Default::default()
    }
}

fn string_value(s: &str) -> prost_types::Value {
    prost_types::Value {
        kind: Some(prost_types::value::Kind::StringValue(s.to_string())),
    }
}

/// A `PartialResultSet` of values for a single STRING column `c`. Only the first message of a
/// stream carries metadata.
fn partial_result_set(
    with_metadata: bool,
    values: &[&str],
    resume_token: &[u8],
    last: bool,
) -> v1::PartialResultSet {
    v1::PartialResultSet {
        metadata: with_metadata.then(|| result_metadata(&[("c", v1::TypeCode::String)])),
        values: values.iter().map(|s| string_value(s)).collect(),
        resume_token: resume_token.to_vec(),
        last,
        ..Default::default()
    }
}

type PartialResultSetSender = tokio::sync::mpsc::Sender<tonic::Result<v1::PartialResultSet>>;

/// A finished response stream carrying the given items.
fn stream_of(
    items: Vec<tonic::Result<v1::PartialResultSet>>,
) -> tonic::Response<tokio::sync::mpsc::Receiver<tonic::Result<v1::PartialResultSet>>> {
    let (tx, rx) = tokio::sync::mpsc::channel(items.len().max(1));
    for item in items {
        tx.try_send(item)
            .expect("scripted stream channel sized to fit");
    }
    tonic::Response::new(rx)
}

/// The driver error that a reader wraps in `ArrowError::ExternalError`.
fn driver_error(error: &ArrowError) -> &adbc_core::error::Error {
    let ArrowError::ExternalError(source) = error else {
        panic!("expected the reader to surface the driver error, got: {error}");
    };
    source
        .downcast_ref::<adbc_core::error::Error>()
        .expect("the reader error wraps the ADBC error")
}

/// Returns transaction `id` in the result metadata, as Spanner does for an inline begin.
fn set_transaction_id(result: &mut v1::PartialResultSet, id: impl Into<Vec<u8>>) {
    result
        .metadata
        .as_mut()
        .expect("first message carries metadata")
        .transaction = Some(v1::Transaction {
        id: id.into(),
        ..Default::default()
    });
}

fn begins_inline(transaction: Option<&v1::TransactionSelector>) -> bool {
    matches!(
        transaction.and_then(|t| t.selector.as_ref()),
        Some(v1::transaction_selector::Selector::Begin(_))
    )
}

/// Serves `ExecuteStreamingSql` with a single-message STRING result, optionally recording each
/// request. A statement in a manual transaction begins its transaction inline, and the client
/// requires the new transaction id in the first response, so requests with a `begin` selector get
/// `inline-txn` back.
fn serve_streaming_sql_begin_aware(
    mock: &mut MockSpanner,
    values: &'static [&'static str],
    record: Option<Arc<Mutex<Vec<v1::ExecuteSqlRequest>>>>,
) {
    mock.expect_execute_streaming_sql()
        .returning(move |request| {
            let request = request.into_inner();
            let mut first = partial_result_set(true, values, b"r-1", true);
            if begins_inline(request.transaction.as_ref()) {
                set_transaction_id(&mut first, b"inline-txn");
            }
            if let Some(record) = &record {
                record.lock().unwrap().push(request);
            }
            Ok(stream_of(vec![Ok(first)]))
        });
}

/// The transaction selector of each recorded request.
fn transaction_selectors(
    requests: &Mutex<Vec<v1::ExecuteSqlRequest>>,
) -> Vec<Option<v1::TransactionSelector>> {
    requests
        .lock()
        .unwrap()
        .iter()
        .map(|request| request.transaction.clone())
        .collect()
}

// ---------------------------------------------------------------------------
// google.rpc.Status / RetryInfo details
// ---------------------------------------------------------------------------

/// `google.rpc.RetryInfo`, which the mock crate does not generate.
#[derive(Clone, PartialEq, prost::Message)]
struct RetryInfo {
    #[prost(message, optional, tag = "1")]
    retry_delay: Option<prost_types::Duration>,
}

/// `google.rpc.ErrorInfo`, without the unused `metadata` field.
#[derive(Clone, PartialEq, prost::Message)]
struct ErrorInfo {
    #[prost(string, tag = "1")]
    reason: String,
    #[prost(string, tag = "2")]
    domain: String,
}

/// An `ABORTED` status with a `RetryInfo` detail, as Cloud Spanner sends on transaction aborts.
fn aborted_with_retry_info(message: &str) -> tonic::Status {
    let retry_info = RetryInfo {
        retry_delay: Some(prost_types::Duration {
            seconds: 0,
            nanos: 50_000_000,
        }),
    };
    let status_proto = spanner_grpc_mock::google::rpc::Status {
        code: tonic::Code::Aborted as i32,
        message: message.to_string(),
        details: vec![prost_types::Any {
            type_url: "type.googleapis.com/google.rpc.RetryInfo".to_string(),
            value: retry_info.encode_to_vec(),
        }],
    };
    tonic::Status::with_details(
        tonic::Code::Aborted,
        message,
        bytes::Bytes::from(status_proto.encode_to_vec()),
    )
}

// ---------------------------------------------------------------------------
// Bulk-ingest scripting helpers
// ---------------------------------------------------------------------------

fn disable_autocommit(connection: &mut SpannerConnection) {
    connection
        .set_option(OptionConnection::AutoCommit, "false".into())
        .expect("disable autocommit");
}

/// A batch of `n` rows in one STRING column `c`; each row is one insert mutation.
fn ingest_batch(n: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("c", DataType::Utf8, false)]));
    let column = StringArray::from((0..n).map(|i| format!("v{i}")).collect::<Vec<_>>());
    RecordBatch::try_new(schema, vec![Arc::new(column)]).expect("build ingest batch")
}

/// A statement ingesting into `MockTable` in `append` mode, which needs no admin RPCs.
fn append_ingest_statement(connection: &mut SpannerConnection) -> SpannerStatement {
    let mut statement = connection.new_statement().expect("new statement");
    statement
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::String("MockTable".into()),
        )
        .expect("set target table");
    statement
        .set_option(
            OptionStatement::IngestMode,
            OptionValue::String("append".into()),
        )
        .expect("set ingest mode append");
    statement
}

/// An `append` ingest through `BatchWrite` instead of a commit.
fn batch_write_ingest_statement(connection: &mut SpannerConnection) -> SpannerStatement {
    let mut statement = append_ingest_statement(connection);
    statement
        .set_option(
            OptionStatement::Other(spanner_adbc::OPTION_INGEST_BATCH_WRITE.into()),
            OptionValue::String("true".into()),
        )
        .expect("route the ingest through BatchWrite");
    statement
}

/// Spanner's mutation-limit error; the driver recognizes it by the "too many mutations" text.
fn too_many_mutations_status() -> tonic::Status {
    tonic::Status::invalid_argument(
        "The transaction contains too many mutations. Insert and update operations count with the \
         multiplicity of the number of columns they affect. The total mutation count includes any \
         changes to indexes that the transaction generates. Please reduce the number of writes, or \
         use fewer indexes. (Maximum number: 80000)",
    )
}

fn commit_ok() -> tonic::Result<tonic::Response<v1::CommitResponse>> {
    Ok(tonic::Response::new(v1::CommitResponse::default()))
}

/// A `BatchWriteResponse` failing group `index` with `ALREADY_EXISTS`. BatchWrite reports
/// per-group failures inside the response stream; the RPC itself succeeds.
///
/// The client reopens the stream for any group not acknowledged and bounds-checks indexes against
/// that smaller request, so acknowledge every group once.
fn batch_write_group_already_exists(index: i32) -> v1::BatchWriteResponse {
    let error_info = ErrorInfo {
        reason: "DUPLICATE_KEY".to_string(),
        domain: "spanner.googleapis.com".to_string(),
    };
    v1::BatchWriteResponse {
        indexes: vec![index],
        status: Some(spanner_grpc_mock::google::rpc::Status {
            code: tonic::Code::AlreadyExists as i32,
            message: "Row [v0] in table MockTable already exists".to_string(),
            details: vec![prost_types::Any {
                type_url: "type.googleapis.com/google.rpc.ErrorInfo".to_string(),
                value: error_info.encode_to_vec(),
            }],
        }),
        commit_timestamp: None,
    }
}

/// A `BatchWriteResponse` reporting that groups `indexes` committed.
fn batch_write_groups_ok(indexes: Vec<i32>) -> v1::BatchWriteResponse {
    v1::BatchWriteResponse {
        indexes,
        status: None,
        commit_timestamp: Some(prost_types::Timestamp::default()),
    }
}

type BatchDmlRequests = Arc<Mutex<Vec<v1::ExecuteBatchDmlRequest>>>;

/// Serves `ExecuteBatchDml`, recording each request and reporting one affected row per statement.
/// A batch that begins its transaction inline gets the transaction id back; without it the client
/// fails with "Transaction ID was not returned by Spanner".
fn serve_batch_dml(mock: &mut MockSpanner, record: BatchDmlRequests) {
    mock.expect_execute_batch_dml().returning(move |request| {
        let request = request.into_inner();
        let inline_begin = begins_inline(request.transaction.as_ref());
        let statements = request.statements.len().max(1);
        record.lock().unwrap().push(request);
        let result_sets = (0..statements)
            .map(|i| v1::ResultSet {
                metadata: Some(v1::ResultSetMetadata {
                    transaction: (i == 0 && inline_begin).then(|| v1::Transaction {
                        id: b"dml-txn".to_vec(),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                stats: Some(v1::ResultSetStats {
                    row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .collect();
        Ok(tonic::Response::new(v1::ExecuteBatchDmlResponse {
            result_sets,
            status: Some(spanner_grpc_mock::google::rpc::Status {
                code: 0,
                message: "OK".into(),
                details: vec![],
            }),
            ..Default::default()
        }))
    });
}

/// Write-only paths begin explicitly, and the client may also do so instead of beginning inline.
fn serve_begin_transaction(mock: &mut MockSpanner) {
    mock.expect_begin_transaction().returning(|_| {
        Ok(tonic::Response::new(v1::Transaction {
            id: b"mock-txn".to_vec(),
            ..Default::default()
        }))
    });
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn omni_connects_with_explicit_endpoint_and_default_anonymous_credentials() {
    let _watchdog = watchdog!();
    let server = MockServer::start(|mock| {
        mock.expect_execute_streaming_sql().returning(|_| {
            Ok(stream_of(vec![Ok(partial_result_set(
                true,
                &["omni"],
                b"",
                true,
            ))]))
        });
    });
    let mut driver = SpannerDriver::try_new().unwrap();
    let database = driver
        .new_database_with_opts([
            (OptionDatabase::Uri, format!("spanner:///{DATABASE}").into()),
            (
                OptionDatabase::Other(spanner_adbc::OPTION_INSTANCE_TYPE.into()),
                "omni".into(),
            ),
            (
                OptionDatabase::Other(spanner_adbc::OPTION_ENDPOINT.into()),
                server.endpoint.clone().into(),
            ),
        ])
        .unwrap();
    assert_eq!(
        database
            .get_option_string(OptionDatabase::Other(spanner_adbc::OPTION_EMULATOR.into()))
            .unwrap(),
        "false"
    );
    let mut connection = database.new_connection().unwrap();
    let mut statement = connection.new_statement().unwrap();
    statement.set_sql_query("SELECT 'omni'").unwrap();
    let batches = statement
        .execute()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(batches[0].column(0).as_string::<i32>().value(0), "omni");
}

#[test]
fn bound_dml_strips_leading_and_trailing_terminators() {
    let _watchdog = watchdog!();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let recorded = requests.clone();
    let server = MockServer::start(move |mock| {
        serve_batch_dml(mock, recorded);
        mock.expect_commit().returning(|_| commit_ok());
    });
    let mut connection = server.connect();
    for update in [false, true] {
        let mut statement = connection.new_statement().unwrap();
        statement
            .set_sql_query(
                "; -- empty\n; INSERT INTO MockTable (c) VALUES (@p);; -- trailing comment",
            )
            .unwrap();
        statement.bind(ingest_batch(2)).unwrap();
        if update {
            assert_eq!(statement.execute_update().unwrap(), Some(2));
        } else {
            assert_eq!(statement.execute().unwrap().count(), 0);
        }
    }
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    for request in requests.iter() {
        assert_eq!(request.statements.len(), 2);
        for statement in &request.statements {
            assert_eq!(statement.sql, "INSERT INTO MockTable (c) VALUES (@p)");
        }
    }
}

#[test]
fn bound_dml_returning_and_partitioned_preserve_parameters() {
    let _watchdog = watchdog!();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let recorded = requests.clone();
    let server = MockServer::start(move |mock| {
        serve_begin_transaction(mock);
        mock.expect_execute_streaming_sql()
            .returning(move |request| {
                let request = request.into_inner();
                let returning = request.sql.ends_with("THEN RETURN c");
                recorded.lock().unwrap().push(request);
                let mut result = partial_result_set(true, &["v"], b"dml-1", true);
                set_transaction_id(&mut result, b"dml-txn");
                result.stats = Some(v1::ResultSetStats {
                    row_count: Some(if returning {
                        v1::result_set_stats::RowCount::RowCountExact(1)
                    } else {
                        v1::result_set_stats::RowCount::RowCountLowerBound(1)
                    }),
                    ..Default::default()
                });
                Ok(stream_of(vec![Ok(result)]))
            });
        mock.expect_commit().returning(|_| commit_ok());
    });
    let mut connection = server.connect();
    for (sql, partitioned) in [
        ("INSERT INTO MockTable (c) VALUES (@p) THEN RETURN c", false),
        ("UPDATE MockTable SET c = @p WHERE TRUE", true),
    ] {
        let mut statement = connection.new_statement().unwrap();
        statement
            .set_sql_query(format!("{sql};; -- comment"))
            .unwrap();
        statement.bind(ingest_batch(1)).unwrap();
        statement
            .set_option(
                OptionStatement::Other(spanner_adbc::OPTION_DML_PARTITIONED.into()),
                partitioned.to_string().into(),
            )
            .unwrap();
        assert_eq!(statement.execute_update().unwrap(), Some(1));
        let requests = requests.lock().unwrap();
        let request = requests.last().unwrap();
        assert_eq!(request.sql, sql);
        assert!(request.params.as_ref().unwrap().fields.contains_key("p"));
    }
    assert_eq!(requests.lock().unwrap().len(), 2);
}

#[test]
fn bound_returning_dml_commits_with_the_explicit_transaction_id() {
    let _watchdog = watchdog!();
    let begins = Arc::new(Mutex::new(Vec::new()));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let commits = Arc::new(Mutex::new(Vec::new()));
    let (record_begins, record_requests, record_commits) =
        (begins.clone(), requests.clone(), commits.clone());
    let server = MockServer::start(move |mock| {
        mock.expect_begin_transaction()
            .once()
            .returning(move |request| {
                record_begins.lock().unwrap().push(request.into_inner());
                Ok(tonic::Response::new(v1::Transaction {
                    id: b"original-txn".to_vec(),
                    ..Default::default()
                }))
            });
        mock.expect_execute_streaming_sql()
            .times(2)
            .returning(move |request| {
                let request = request.into_inner();
                let value = format!("v{}", request.seqno - 1);
                let mut result = partial_result_set(true, &[&value], b"dml", true);
                // Omni can return a different opaque ID in each streaming DML response.
                set_transaction_id(&mut result, format!("response-txn-{}", request.seqno));
                result.precommit_token = Some(v1::MultiplexedSessionPrecommitToken {
                    precommit_token: format!("token-{}", request.seqno).into_bytes(),
                    seq_num: request.seqno.try_into().unwrap(),
                });
                record_requests.lock().unwrap().push(request);
                Ok(stream_of(vec![Ok(result)]))
            });
        mock.expect_commit().once().returning(move |request| {
            record_commits.lock().unwrap().push(request.into_inner());
            commit_ok()
        });
    });
    let mut connection = server.connect();
    let mut statement = connection.new_statement().unwrap();
    statement
        .set_sql_query("INSERT INTO MockTable (c) VALUES (@c) THEN RETURN c")
        .unwrap();
    statement.bind(ingest_batch(2)).unwrap();
    let batches = statement
        .execute()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(batches.len(), 2);
    for (index, batch) in batches.iter().enumerate() {
        assert_eq!(
            batch.column(0).as_string::<i32>().value(0),
            format!("v{index}")
        );
    }
    let begins = begins.lock().unwrap();
    assert_eq!(begins.len(), 1);
    assert!(matches!(
        begins[0].options.as_ref().unwrap().mode,
        Some(v1::transaction_options::Mode::ReadWrite(_))
    ));
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    for (index, request) in requests.iter().enumerate() {
        assert_eq!(request.seqno, index as i64 + 1);
        assert_eq!(
            request.transaction.as_ref().unwrap().selector,
            Some(v1::transaction_selector::Selector::Id(
                b"original-txn".to_vec()
            ))
        );
        assert_eq!(
            request.params.as_ref().unwrap().fields["c"],
            string_value(&format!("v{index}"))
        );
    }
    let commits = commits.lock().unwrap();
    assert_eq!(commits.len(), 1);
    assert_eq!(
        commits[0].transaction,
        Some(v1::commit_request::Transaction::TransactionId(
            b"original-txn".to_vec()
        ))
    );
    assert_eq!(
        commits[0].precommit_token,
        Some(v1::MultiplexedSessionPrecommitToken {
            precommit_token: b"token-2".to_vec(),
            seq_num: 2,
        })
    );
}

#[test]
fn bound_dml_rejects_batches_before_buffering_manual_work() {
    let _watchdog = watchdog!();
    let server = MockServer::start(|_| {});
    let mut connection = server.connect();
    disable_autocommit(&mut connection);
    for sql in [
        "INSERT INTO MockTable (c) VALUES (@p); SELECT 1",
        "INSERT INTO MockTable (c) VALUES (@p); DELETE FROM MockTable WHERE c = @p",
    ] {
        let mut statement = connection.new_statement().unwrap();
        statement.set_sql_query(sql).unwrap();
        statement.bind(ingest_batch(1)).unwrap();
        let error = statement.execute_update().unwrap_err();
        assert_eq!(error.status, AdbcStatus::InvalidArguments, "{error}");
        // No RPCs are scripted: committing succeeds only if the invalid batch was not buffered.
        connection.commit().unwrap();
    }
}

#[test]
fn mock_server_round_trips_a_query() {
    let _watchdog = watchdog!();
    let seen_sql: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let record_sql = seen_sql.clone();
    let server = MockServer::start(move |mock| {
        mock.expect_execute_streaming_sql()
            .returning(move |request| {
                *record_sql.lock().unwrap() = Some(request.into_inner().sql);
                Ok(stream_of(vec![Ok(partial_result_set(
                    true,
                    &["v1", "v2"],
                    b"rt-1",
                    true,
                ))]))
            });
    });

    let mut connection = server.connect();
    let mut statement = connection.new_statement().expect("new statement");
    statement
        .set_sql_query("SELECT c FROM MockTable")
        .expect("set query");
    let batches: Vec<_> = statement
        .execute()
        .expect("query against mock server")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect batches");

    assert_eq!(
        seen_sql.lock().unwrap().as_deref(),
        Some("SELECT c FROM MockTable"),
        "the mock server should have received the statement's SQL verbatim"
    );
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.schema().field(0).name(), "c");
    assert_eq!(batch.schema().field(0).data_type(), &DataType::Utf8);
    let column = batch.column(0).as_string::<i32>();
    assert_eq!(column.value(0), "v1");
    assert_eq!(column.value(1), "v2");
}

/// Unknown wire kinds must fail even on the UTF-8 fallback, including inside lists.
#[test]
fn unknown_wire_values_are_not_rendered_as_json() {
    let _watchdog = watchdog!();
    for nested in [false, true] {
        let server = MockServer::start(move |mock| {
            mock.expect_execute_streaming_sql().returning(move |_| {
                let mut value = prost_types::Value {
                    kind: Some(prost_types::value::Kind::StructValue(prost_types::Struct {
                        fields: [("x".into(), string_value("1"))].into_iter().collect(),
                    })),
                };
                if nested {
                    value = prost_types::Value {
                        kind: Some(prost_types::value::Kind::ListValue(
                            prost_types::ListValue {
                                values: vec![value],
                            },
                        )),
                    };
                }
                Ok(stream_of(vec![Ok(v1::PartialResultSet {
                    metadata: Some(result_metadata(&[("c", v1::TypeCode::String)])),
                    values: vec![value],
                    last: true,
                    ..Default::default()
                })]))
            });
        });
        let mut connection = server.connect();
        let mut statement = connection.new_statement().unwrap();
        statement.set_sql_query("SELECT c FROM MockTable").unwrap();
        let mut reader = statement.execute().expect("query metadata");
        let error = reader.next().unwrap().expect_err("unknown wire kind");
        let error = driver_error(&error);
        assert_eq!(error.status, AdbcStatus::InvalidData);
        assert!(error.message.contains("column \"c\""), "{error}");
    }
}

/// A keyed protobuf Struct cannot represent positional Spanner STRUCT fields with duplicate names.
#[test]
fn keyed_struct_wire_value_errors() {
    let _watchdog = watchdog!();
    let server = MockServer::start(|mock| {
        mock.expect_execute_streaming_sql().returning(|_| {
            // STRUCT<x INT64, x INT64>: a map cannot preserve both fields named x.
            let fields =
                result_metadata(&[("x", v1::TypeCode::Int64), ("x", v1::TypeCode::Int64)]).row_type;
            let metadata = v1::ResultSetMetadata {
                row_type: Some(v1::StructType {
                    fields: vec![v1::struct_type::Field {
                        name: "c".into(),
                        r#type: Some(v1::Type {
                            code: v1::TypeCode::Struct as i32,
                            struct_type: fields,
                            ..Default::default()
                        }),
                    }],
                }),
                ..Default::default()
            };
            let keyed = prost_types::Value {
                kind: Some(prost_types::value::Kind::StructValue(prost_types::Struct {
                    fields: [("x".into(), string_value("1"))].into_iter().collect(),
                })),
            };
            Ok(stream_of(vec![Ok(v1::PartialResultSet {
                metadata: Some(metadata),
                values: vec![keyed],
                last: true,
                ..Default::default()
            })]))
        });
    });
    let mut connection = server.connect();
    let mut statement = connection.new_statement().expect("new statement");
    statement.set_sql_query("SELECT c FROM MockTable").unwrap();
    let mut reader = statement.execute().expect("query metadata");
    let error = reader
        .next()
        .expect("first batch")
        .expect_err("keyed STRUCT must be rejected");
    let error = driver_error(&error);
    assert_eq!(error.status, AdbcStatus::InvalidData);
    assert!(error.message.contains("STRUCT"), "{}", error.message);
}

/// A query while DML is buffered would not see the buffered writes, so it fails with
/// `InvalidState`. Queries in autocommit mode or in a fresh manual transaction still run.
#[test]
fn query_while_dml_buffered_in_manual_txn_is_rejected() {
    let _watchdog = watchdog!();
    let server = MockServer::start(move |mock| {
        serve_streaming_sql_begin_aware(mock, &["v1", "v2"], None);
    });
    let mut connection = server.connect();

    let mut auto_q = connection.new_statement().expect("new statement");
    auto_q.set_sql_query("SELECT c FROM MockTable").unwrap();
    let batches: Vec<_> = auto_q
        .execute()
        .expect("autocommit query must run")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect autocommit batches");
    assert_eq!(batches[0].num_rows(), 2);

    disable_autocommit(&mut connection);

    // The query makes this a query transaction; roll back so the DML below starts a new one.
    let mut empty_q = connection.new_statement().expect("new statement");
    empty_q.set_sql_query("SELECT c FROM MockTable").unwrap();
    let batches: Vec<_> = empty_q
        .execute()
        .expect("query in a fresh manual transaction must run")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect empty-buffer batches");
    assert_eq!(batches[0].num_rows(), 2);
    connection.rollback().expect("end the query transaction");

    let mut insert = connection.new_statement().expect("new statement");
    insert
        .set_sql_query("INSERT INTO MockTable (c) VALUES ('x')")
        .unwrap();
    assert_eq!(
        insert.execute_update().expect("buffered insert"),
        None,
        "DML in manual mode buffers (returns None), not commits"
    );

    let mut guarded = connection.new_statement().expect("new statement");
    guarded.set_sql_query("SELECT c FROM MockTable").unwrap();
    let Err(error) = guarded.execute() else {
        panic!("a query while DML is buffered must be rejected, not silently run");
    };
    assert_eq!(
        error.status,
        AdbcStatus::InvalidState,
        "querying with buffered writes must fail with InvalidState (no read-your-writes)"
    );
    assert!(
        error.message.contains("read-your-writes"),
        "the error should explain the read-your-writes hazard: {:?}",
        error.message
    );

    let mut guarded_partitions = connection.new_statement().expect("new statement");
    guarded_partitions
        .set_sql_query("SELECT c FROM MockTable")
        .unwrap();
    let Err(error) = guarded_partitions.execute_partitions() else {
        panic!("execute_partitions while DML is buffered must be rejected");
    };
    assert_eq!(error.status, AdbcStatus::InvalidState);

    connection.rollback().expect("rollback buffered insert");
    let mut after = connection.new_statement().expect("new statement");
    after.set_sql_query("SELECT c FROM MockTable").unwrap();
    let batches: Vec<_> = after
        .execute()
        .expect("query after rollback must run")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect post-rollback batches");
    assert_eq!(batches[0].num_rows(), 2);
}

/// Queries in a manual transaction share one read-only transaction: the first begins it inline and
/// later statements reference it by id. DML is rejected while it is active, and commit needs no
/// RPC.
#[test]
fn manual_transaction_queries_share_one_read_only_transaction() {
    let _watchdog = watchdog!();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let server = {
        let record = requests.clone();
        MockServer::start(move |mock| {
            serve_streaming_sql_begin_aware(mock, &["v1"], Some(record));
        })
    };

    let mut connection = server.connect();
    disable_autocommit(&mut connection);

    for _ in 0..2 {
        let mut query = connection.new_statement().expect("new statement");
        query.set_sql_query("SELECT c FROM MockTable").unwrap();
        let batches: Vec<_> = query
            .execute()
            .expect("manual-transaction query")
            .collect::<Result<Vec<_>, _>>()
            .expect("collect batches");
        assert_eq!(batches[0].num_rows(), 1);
    }

    {
        let seen = transaction_selectors(&requests);
        assert_eq!(seen.len(), 2, "both queries must reach the wire");
        let first = seen[0]
            .as_ref()
            .and_then(|s| s.selector.as_ref())
            .expect("the first query must carry a transaction selector");
        let v1::transaction_selector::Selector::Begin(options) = first else {
            panic!("the first manual-mode query must begin the transaction, got: {first:?}");
        };
        assert!(
            matches!(
                options.mode,
                Some(v1::transaction_options::Mode::ReadOnly(_))
            ),
            "the begun transaction must be read-only: {options:?}"
        );
        let second = seen[1]
            .as_ref()
            .and_then(|s| s.selector.as_ref())
            .expect("the second query must carry a transaction selector");
        assert_eq!(
            second,
            &v1::transaction_selector::Selector::Id(b"inline-txn".to_vec()),
            "later queries must reuse the begun read-only transaction"
        );
    }

    // The write would commit in a separate read/write transaction, outside the reads' snapshot.
    let mut insert = connection.new_statement().expect("new statement");
    insert
        .set_sql_query("INSERT INTO MockTable (c) VALUES ('x')")
        .unwrap();
    let Err(error) = insert.execute_update() else {
        panic!("DML in a manual transaction that began with a query must be rejected");
    };
    assert_eq!(error.status, AdbcStatus::InvalidState);
    assert!(
        error.message.contains("began with a query"),
        "the rejection should name the active kind: {:?}",
        error.message
    );

    // Commit is not scripted, so this succeeds only without an RPC.
    connection
        .commit()
        .expect("committing a query transaction needs no RPC");

    let mut insert = connection.new_statement().expect("new statement");
    insert
        .set_sql_query("INSERT INTO MockTable (c) VALUES ('x')")
        .unwrap();
    assert_eq!(
        insert
            .execute_update()
            .expect("fresh transaction buffers DML"),
        None,
        "after commit the connection must accept a DML-kind transaction again"
    );
    connection.rollback().expect("discard the buffered insert");
}

/// `execute_update` on a SELECT, plain or bound, drains it through `ExecuteStreamingSql` and
/// returns no count. This backs the ADBC call with a NULL output stream.
#[test]
fn execute_update_routes_a_query_to_the_read_only_path() {
    let _watchdog = watchdog!();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let record = seen.clone();
    let server = MockServer::start(move |mock| {
        serve_streaming_sql_begin_aware(mock, &["v1", "v2"], Some(record));
    });

    let mut connection = server.connect();

    let mut statement = connection.new_statement().expect("new statement");
    statement.set_sql_query("SELECT c FROM MockTable").unwrap();
    assert_eq!(
        statement
            .execute_update()
            .expect("execute_update on a SELECT must run it as a read-only query"),
        None,
        "a read query has no affected-row count"
    );

    let mut bound = connection.new_statement().expect("new statement");
    bound
        .set_sql_query("SELECT c FROM MockTable WHERE c = @p")
        .unwrap();
    let param_schema = Arc::new(Schema::new(vec![Field::new("c", DataType::Utf8, false)]));
    let param_batch =
        RecordBatch::try_new(param_schema, vec![Arc::new(StringArray::from(vec!["v1"]))])
            .expect("build bound batch");
    bound.bind(param_batch).expect("bind parameter row");
    assert_eq!(
        bound
            .execute_update()
            .expect("execute_update on a bound SELECT must run the bound-query path"),
        None
    );

    let seen = seen.lock().unwrap();
    assert_eq!(
        seen.iter().map(|r| r.sql.as_str()).collect::<Vec<_>>(),
        vec![
            "SELECT c FROM MockTable",
            "SELECT c FROM MockTable WHERE c = @p",
        ],
        "both statements must arrive via ExecuteStreamingSql (never ExecuteBatchDml)"
    );
    assert_eq!(
        seen[1]
            .params
            .as_ref()
            .map(|p| p.fields.keys().map(String::as_str).collect::<Vec<_>>()),
        Some(vec!["p"]),
        "the bound row must be attached as the @p parameter"
    );
}

/// `execute_partitions` accepts at most one bound row. Rejected rows are still consumed so a reused
/// statement cannot apply them later.
#[test]
fn execute_partitions_rejects_multiple_bound_rows_and_consumes_them() {
    let _watchdog = watchdog!();
    let server = MockServer::start(|_| {});
    let mut connection = server.connect();

    let mut statement = connection.new_statement().expect("new statement");
    statement.set_sql_query("SELECT 1").unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("c", DataType::Utf8, false)]));
    let two_rows = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["a", "b"]))])
        .expect("build bound batch");
    statement.bind(two_rows).expect("bind two parameter rows");

    let Err(error) = statement.execute_partitions() else {
        panic!("execute_partitions with two bound rows must be rejected, not truncated to row 0");
    };
    assert_eq!(error.status, AdbcStatus::InvalidArguments);
    assert!(
        error
            .message
            .contains("at most one bound parameter row, but 2 rows"),
        "the error must name the limitation and the row count: {}",
        error.message
    );

    // Without bound data the schema comes from the SQL, which has no parameters.
    let params = statement.get_parameter_schema().expect("parameter schema");
    assert_eq!(
        params.fields().len(),
        0,
        "bound rows must not survive execute_partitions on a reused statement handle"
    );
}

#[test]
fn empty_partitions_retain_the_planned_types_at_the_readers_timestamp_precision() {
    let _watchdog = watchdog!();
    let partition_queries = Arc::new(Mutex::new(Vec::new()));
    let queries = Arc::new(Mutex::new(Vec::new()));
    let (record_partition_queries, record_queries) = (partition_queries.clone(), queries.clone());
    let server = MockServer::start(move |mock| {
        serve_begin_transaction(mock);
        mock.expect_partition_query()
            .once()
            .returning(move |request| {
                record_partition_queries
                    .lock()
                    .unwrap()
                    .push(request.into_inner().sql);
                Ok(tonic::Response::new(v1::PartitionResponse {
                    partitions: vec![v1::Partition {
                        partition_token: b"empty-partition".to_vec(),
                    }],
                    ..Default::default()
                }))
            });
        mock.expect_execute_streaming_sql()
            .times(2)
            .returning(move |request| {
                let request = request.into_inner();
                let metadata = if request.partition_token.is_empty() {
                    Some(result_metadata(&[
                        ("Id", v1::TypeCode::Int64),
                        ("Ts", v1::TypeCode::Timestamp),
                        ("Doc", v1::TypeCode::Json),
                    ]))
                } else {
                    // Omni can omit the row type for a partition with no rows.
                    Some(v1::ResultSetMetadata::default())
                };
                record_queries.lock().unwrap().push(request);
                Ok(stream_of(vec![Ok(v1::PartialResultSet {
                    metadata,
                    last: true,
                    ..Default::default()
                })]))
            });
    });
    let mut producer = server.connect();
    let mut statement = producer.new_statement().unwrap();
    statement
        .set_sql_query("SELECT Id, Ts, Doc FROM MockTable")
        .unwrap();
    let partitions = statement.execute_partitions().unwrap();
    assert_eq!(partitions.partitions.len(), 1);
    assert_eq!(
        partitions.schema.field(1).data_type(),
        &DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, Some("UTC".into()))
    );
    drop(statement);

    let mut consumer = server.connect();
    consumer
        .set_option(
            OptionConnection::Other(spanner_adbc::OPTION_MAX_TIMESTAMP_PRECISION.into()),
            "microseconds".into(),
        )
        .unwrap();
    let reader = consumer.read_partition(&partitions.partitions[0]).unwrap();
    let schema = reader.schema();
    assert_eq!(schema.fields().len(), 3);
    assert_eq!(schema.field(0).name(), "Id");
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    assert_eq!(schema.field(1).name(), "Ts");
    assert_eq!(
        schema.field(1).data_type(),
        &DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into()))
    );
    assert_eq!(schema.field(2), partitions.schema.field(2));
    assert_eq!(
        schema.field(2).metadata()["ARROW:extension:name"],
        "arrow.json"
    );
    let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
    assert_eq!(
        *partition_queries.lock().unwrap(),
        ["SELECT Id, Ts, Doc FROM MockTable"]
    );
    let queries = queries.lock().unwrap();
    assert_eq!(
        queries
            .iter()
            .map(|q| (q.query_mode, q.partition_token.as_slice()))
            .collect::<Vec<_>>(),
        [
            (v1::execute_sql_request::QueryMode::Plan as i32, &b""[..]),
            (
                v1::execute_sql_request::QueryMode::Normal as i32,
                &b"empty-partition"[..]
            ),
        ],
        "a PLAN for the schema, then a fetch of the partition that omits its row type"
    );
}

#[test]
fn execute_schema_binds_the_first_nonempty_batch() {
    let _watchdog = watchdog!();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let server = MockServer::start({
        let requests = requests.clone();
        move |mock| serve_streaming_sql_begin_aware(mock, &[], Some(requests))
    });
    let mut connection = server.connect();
    let mut statement = connection.new_statement().unwrap();
    statement.set_sql_query("SELECT @p AS c").unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("p", DataType::Utf8, false)]));
    let row = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec!["bound value"]))],
    )
    .unwrap();
    statement
        .bind_stream(Box::new(arrow_array::RecordBatchIterator::new(
            vec![Ok(RecordBatch::new_empty(schema.clone())), Ok(row)],
            schema.clone(),
        )))
        .unwrap();

    let result = statement.execute_schema().unwrap();
    assert_eq!(result.field(0).name(), "c");
    assert_eq!(result.field(0).data_type(), &DataType::Utf8);
    // Planning preserves the bindings for the later execution.
    assert_eq!(statement.get_parameter_schema().unwrap(), *schema);
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].query_mode,
        v1::execute_sql_request::QueryMode::Plan as i32
    );
    assert_eq!(
        requests[0].params.as_ref().unwrap().fields["p"],
        string_value("bound value")
    );
}

#[test]
fn bind_stream_rejects_schema_changes_without_replacing_previous_bindings() {
    let _watchdog = watchdog!();
    let server = MockServer::start(|_| {});
    let mut connection = server.connect();
    let mut statement = connection.new_statement().unwrap();
    let original = Arc::new(Schema::new(vec![Field::new(
        "original",
        DataType::Int64,
        false,
    )]));
    statement
        .bind(RecordBatch::new_empty(original.clone()))
        .unwrap();

    let declared = Arc::new(Schema::new(vec![Field::new("p", DataType::Utf8, false)]));
    for mismatched in [
        Arc::new(Schema::new(vec![Field::new("p", DataType::Int64, false)])),
        Arc::new(Schema::new(vec![Field::new(
            "renamed",
            DataType::Utf8,
            false,
        )])),
    ] {
        let reader = arrow_array::RecordBatchIterator::new(
            vec![
                Ok(RecordBatch::new_empty(declared.clone())),
                Ok(RecordBatch::new_empty(mismatched)),
            ],
            declared.clone(),
        );
        let error = statement.bind_stream(Box::new(reader)).unwrap_err();
        assert_eq!(error.status, AdbcStatus::InvalidData);
        assert!(error.message.contains("batch 2"), "{error}");
        assert!(error.message.contains("declared schema"), "{error}");
        assert_eq!(statement.get_parameter_schema().unwrap(), *original);
    }
}

/// A failed bound query still consumes its rows, so a reused statement cannot apply them later.
#[test]
fn failed_bound_query_still_consumes_bound_rows() {
    let _watchdog = watchdog!();
    let server = MockServer::start(|mock| {
        mock.expect_execute_streaming_sql().returning(|_| {
            Err(tonic::Status::failed_precondition(
                "the mock server rejected the bound query",
            ))
        });
    });

    let mut connection = server.connect();
    let mut statement = connection.new_statement().expect("new statement");
    statement
        .set_sql_query("SELECT c FROM MockTable WHERE c = @p")
        .unwrap();
    // Binding is positional, so column `c` maps to `@p`. While bound, the parameter schema would
    // report `c`; once consumed, it falls back to the SQL's `p`.
    let param_schema = Arc::new(Schema::new(vec![Field::new("c", DataType::Utf8, false)]));
    let param_batch =
        RecordBatch::try_new(param_schema, vec![Arc::new(StringArray::from(vec!["v1"]))])
            .expect("build bound batch");
    statement.bind(param_batch).expect("bind parameter row");

    let error = statement
        .execute()
        .err()
        .expect("the bound query must fail (the server rejects it), not hang or succeed");
    assert_eq!(error.status, AdbcStatus::InvalidState, "got error: {error}");

    let params = statement.get_parameter_schema().expect("parameter schema");
    assert_eq!(
        params.fields().len(),
        1,
        "the parameter schema must come from the SQL's @name references, not stale bound rows"
    );
    assert_eq!(
        params.field(0).name(),
        "p",
        "a surviving bound batch would report its own column name (`c`); consuming it falls back \
         to the SQL parameter name (`p`)"
    );
}

/// In manual mode, `execute_update` on a SELECT runs it immediately and buffers nothing. A mixed
/// DML/query batch is rejected before buffering, and a SELECT with DML buffered hits the same
/// read-your-writes guard as `execute`.
#[test]
fn execute_update_query_in_manual_mode_buffers_nothing() {
    let _watchdog = watchdog!();
    let server = MockServer::start(move |mock| {
        serve_streaming_sql_begin_aware(mock, &["v1"], None);
    });

    let mut connection = server.connect();
    disable_autocommit(&mut connection);

    let mut query = connection.new_statement().expect("new statement");
    query.set_sql_query("SELECT c FROM MockTable").unwrap();
    assert_eq!(
        query.execute_update().expect("query must run immediately"),
        None
    );

    let mut mixed = connection.new_statement().expect("new statement");
    mixed
        .set_sql_query("DELETE FROM MockTable WHERE true; SELECT 1")
        .unwrap();
    let Err(error) = mixed.execute_update() else {
        panic!("a mixed DML/query batch must be rejected");
    };
    assert_eq!(
        error.status,
        AdbcStatus::InvalidArguments,
        "mixed batch must fail with InvalidArguments: {:?}",
        error.message
    );
    assert!(
        error.message.contains("all-DML"),
        "the error should explain the all-DML batch requirement: {:?}",
        error.message
    );

    // Commit is not scripted, so this succeeds only if nothing was buffered.
    connection
        .commit()
        .expect("commit must succeed with an empty buffer (nothing may have been buffered)");

    let mut insert = connection.new_statement().expect("new statement");
    insert
        .set_sql_query("INSERT INTO MockTable (c) VALUES ('x')")
        .unwrap();
    assert_eq!(insert.execute_update().expect("buffered insert"), None);
    let mut guarded = connection.new_statement().expect("new statement");
    guarded.set_sql_query("SELECT c FROM MockTable").unwrap();
    let Err(error) = guarded.execute_update() else {
        panic!("execute_update on a query while DML is buffered must be rejected");
    };
    assert_eq!(
        error.status,
        AdbcStatus::InvalidState,
        "the read-your-writes guard must fire on execute_update's query arm too"
    );
    assert!(
        error.message.contains("read-your-writes"),
        "the error should explain the read-your-writes hazard: {:?}",
        error.message
    );
    connection.rollback().expect("discard the buffered insert");
}

/// An `ABORTED` query keeps gRPC code 10 in `vendor_code` and forwards its `RetryInfo` detail.
/// A query is used because the client's transaction runner retries an aborted commit itself.
/// Unlike the unit tests in `src/error.rs`, this decodes the detail from a real gRPC trailer.
#[test]
fn aborted_surfaces_vendor_code_and_retry_info_detail() {
    let _watchdog = watchdog!();
    let server = MockServer::start(|mock| {
        mock.expect_execute_streaming_sql().returning(|_| {
            Err(aborted_with_retry_info(
                "Transaction was aborted by the mock server",
            ))
        });
    });

    let mut connection = server.connect();
    let mut statement = connection.new_statement().expect("new statement");
    statement.set_sql_query("SELECT c FROM MockTable").unwrap();
    let error = statement
        .execute()
        .err()
        .expect("an ABORTED query must fail, not hang or succeed");

    assert_eq!(
        error.vendor_code, 10,
        "vendor_code must carry the numeric gRPC code (ABORTED = 10); got error: {error}"
    );
    assert_eq!(error.status, AdbcStatus::IO, "got error: {error}");
    assert!(
        error.message.contains("aborted by the mock server"),
        "the server's status message must survive into the ADBC error, got: {}",
        error.message
    );

    let details = error.details.as_ref().expect(
        "the ABORTED status carried a google.rpc.RetryInfo detail; Error.details must be Some",
    );
    let (_, value) = details
        .iter()
        .find(|(key, _)| key == "google.rpc.retryinfo")
        .unwrap_or_else(|| {
            panic!(
                "expected a `google.rpc.retryinfo` detail, got keys: {:?}",
                details.iter().map(|(k, _)| k).collect::<Vec<_>>()
            )
        });

    // The value is ProtoJSON; parse it so field order does not matter.
    let json: serde_json::Value =
        serde_json::from_slice(value).expect("the detail value is UTF-8 ProtoJSON");
    assert_eq!(
        json["@type"], "type.googleapis.com/google.rpc.RetryInfo",
        "the detail must self-describe as a RetryInfo, got: {json}"
    );
    assert_eq!(
        json["retryDelay"], "0.05s",
        "the RetryInfo's retryDelay must round-trip the 50ms the mock sent, got: {json}"
    );
}

/// After `UNAVAILABLE` mid-stream the client resumes from the last resume token. When the resume
/// fails with a non-retryable status, that error reaches the caller.
#[test]
fn unavailable_mid_stream_surfaces_a_clean_error() {
    let _watchdog = watchdog!();
    let calls = Arc::new(AtomicUsize::new(0));
    let resume_tokens: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
    let calls_in_mock = calls.clone();
    let tokens_in_mock = resume_tokens.clone();
    let server = MockServer::start(move |mock| {
        mock.expect_execute_streaming_sql()
            .returning(move |request| {
                if calls_in_mock.fetch_add(1, Ordering::SeqCst) == 0 {
                    Ok(stream_of(vec![
                        Ok(partial_result_set(true, &["v1"], b"resume-1", false)),
                        Err(tonic::Status::unavailable(
                            "mock server: connection lost mid-stream",
                        )),
                    ]))
                } else {
                    tokens_in_mock
                        .lock()
                        .unwrap()
                        .push(request.into_inner().resume_token);
                    Err(tonic::Status::internal(
                        "mock server: resume attempt refused",
                    ))
                }
            });
    });

    let mut connection = server.connect();
    let mut statement = connection.new_statement().expect("new statement");
    statement.set_sql_query("SELECT c FROM MockTable").unwrap();

    // `execute` fetches the first batch, so the failed resume surfaces here.
    let started = Instant::now();
    let error = statement
        .execute()
        .err()
        .expect("a query whose stream dies mid-flight must fail, not hang or succeed");
    let elapsed = started.elapsed();

    assert_eq!(
        error.vendor_code, 13,
        "the *resume* refusal (INTERNAL = 13) is what surfaces; got error: {error}"
    );
    assert!(
        error.message.contains("resume attempt refused"),
        "expected the resume refusal to surface, got: {}",
        error.message
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        resume_tokens.lock().unwrap().as_slice(),
        &[b"resume-1".to_vec()],
        "the resume attempt must carry the last resume token the stream delivered"
    );
    // Expect one backoff sleep (about 1 s) before the resume.
    assert!(
        elapsed < Duration::from_secs(60),
        "error took {elapsed:?} to surface; runaway retries?"
    );
}

/// Cancelling from another thread unblocks a reader waiting on a stream that sent one row and
/// went silent.
#[test]
fn cancel_unblocks_a_reader_hung_on_a_silent_stream() {
    let _watchdog = watchdog!();
    let (server, _open_streams) = silent_stream_server(1);
    let mut connection = server.connect();
    let mut statement = connection.new_statement().expect("new statement");
    // One row per batch, so `execute` returns and the next fetch blocks.
    statement
        .set_option(
            OptionStatement::Other(spanner_adbc::OPTION_ROWS_PER_BATCH.into()),
            OptionValue::Int(1),
        )
        .expect("set rows_per_batch");
    statement.set_sql_query("SELECT c FROM MockTable").unwrap();
    let mut reader = statement.execute().expect("execute settles the schema");

    let (tx, rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let first = reader.next();
        let second = reader.next();
        let _ = tx.send((first, second));
    });

    std::thread::sleep(Duration::from_millis(300));
    let cancel_at = Instant::now();
    statement.get_cancel_handle().try_cancel().expect("cancel");

    let (first, second) = rx
        .recv_timeout(Duration::from_secs(30))
        .expect("cancel did not unblock the reader stuck on a silent stream");
    let cancel_latency = cancel_at.elapsed();
    worker.join().expect("worker thread");

    let first = first
        .expect("first batch exists")
        .expect("first batch is the row delivered before the server went silent");
    assert_eq!(first.num_rows(), 1);

    let error = second
        .expect("the cancelled fetch yields an item")
        .expect_err("the fetch blocked on the silent stream must fail with the cancel");
    let adbc_error = driver_error(&error);
    assert_eq!(
        adbc_error.status,
        AdbcStatus::Cancelled,
        "got error: {adbc_error}"
    );
    assert!(
        cancel_latency < Duration::from_secs(10),
        "cancel took {cancel_latency:?} to unblock the reader"
    );
}

/// `spanner.rpc.timeout_seconds.update` bounds DDL, including the operation poll loop. A silent
/// admin endpoint makes this deterministic; emulator DDL can finish faster than a tiny deadline.
/// Without the driver deadline, the admin client's own ~60 s deadline would fail with `Internal`.
#[test]
fn ddl_update_timeout_fires_on_a_silent_admin_endpoint() {
    let _watchdog = watchdog!();
    let server = MockServer::start(|_mock| {});
    let gate = GatedEndpoint::start(&server.endpoint);
    let mut connection = gate.connect();
    gate.go_silent();

    let mut statement = connection.new_statement().expect("new statement");
    statement
        .set_option(
            OptionStatement::Other(spanner_adbc::OPTION_RPC_TIMEOUT_UPDATE.into()),
            OptionValue::Double(0.25),
        )
        .expect("set the update deadline");
    statement
        .set_sql_query("CREATE TABLE T (Id INT64) PRIMARY KEY (Id)")
        .unwrap();

    let started = Instant::now();
    let error = statement
        .execute_update()
        .expect_err("an unanswerable DDL must expire its update deadline");
    let elapsed = started.elapsed();

    assert_eq!(error.status, AdbcStatus::Timeout, "got error: {error}");
    assert!(
        error
            .message
            .contains(spanner_adbc::OPTION_RPC_TIMEOUT_UPDATE),
        "the DDL timeout error must name the update option: {}",
        error.message
    );
    // A refused or reset connection would fail within milliseconds.
    assert!(
        elapsed >= Duration::from_millis(250),
        "returned in {elapsed:?}, before the 0.25s deadline could fire; the DDL failed for some \
         other reason"
    );
}

/// A server whose streams send `rows` one-row messages and then stay open without sending more.
/// With `rows = 0`, `execute` itself blocks; with `rows = 1`, the next fetch does.
///
/// Hold the returned senders for as long as the streams must stay silent; dropping them ends the
/// streams.
fn silent_stream_server(rows: usize) -> (MockServer, Arc<Mutex<Vec<PartialResultSetSender>>>) {
    let open_streams: Arc<Mutex<Vec<PartialResultSetSender>>> = Arc::new(Mutex::new(Vec::new()));
    let streams_in_mock = open_streams.clone();
    let server = MockServer::start(move |mock| {
        mock.expect_execute_streaming_sql().returning(move |_| {
            let (tx, rx) = tokio::sync::mpsc::channel(rows.max(1));
            for i in 0..rows {
                tx.try_send(Ok(partial_result_set(i == 0, &["v1"], b"rt-1", false)))
                    .expect("scripted stream channel sized to fit");
            }
            streams_in_mock.lock().unwrap().push(tx);
            Ok(tonic::Response::new(rx))
        });
    });
    (server, open_streams)
}

/// Queries a [`silent_stream_server`] with one row per batch, so the second batch is the first
/// fetch after `execute` and blocks.
fn silent_stream_reader(
    connection: &mut SpannerConnection,
    fetch_seconds: Option<f64>,
) -> Box<dyn RecordBatchReader + Send + 'static> {
    let mut statement = connection.new_statement().expect("new statement");
    statement
        .set_option(
            OptionStatement::Other(spanner_adbc::OPTION_ROWS_PER_BATCH.into()),
            OptionValue::Int(1),
        )
        .expect("set rows_per_batch");
    if let Some(seconds) = fetch_seconds {
        statement
            .set_option(
                OptionStatement::Other(spanner_adbc::OPTION_RPC_TIMEOUT_FETCH.into()),
                OptionValue::Double(seconds),
            )
            .expect("set the fetch deadline");
    }
    statement.set_sql_query("SELECT c FROM MockTable").unwrap();
    statement.execute().expect("execute settles the schema")
}

/// `spanner.rpc.timeout_seconds.fetch` bounds fetches after the first batch. The stream sends one
/// row first so that `execute`, which the query deadline covers, has returned before anything
/// blocks.
#[test]
fn fetch_timeout_fires_on_a_silent_stream() {
    let _watchdog = watchdog!();
    let (server, _open_streams) = silent_stream_server(1);
    let mut connection = server.connect();

    // The fetch deadline cannot start before `execute`, so the call must outlast it from here.
    let started = Instant::now();
    let mut reader = silent_stream_reader(&mut connection, Some(0.5));

    let first = reader
        .next()
        .expect("first batch exists")
        .expect("first batch is the row delivered before the server went silent");
    assert_eq!(first.num_rows(), 1);

    let error = reader
        .next()
        .expect("the expired fetch deadline yields an item")
        .expect_err("the fetch blocked on the silent stream must expire its deadline");
    let elapsed = started.elapsed();

    let adbc_error = driver_error(&error);
    assert_eq!(
        adbc_error.status,
        AdbcStatus::Timeout,
        "got error: {adbc_error}"
    );
    assert!(
        adbc_error
            .message
            .contains(spanner_adbc::OPTION_RPC_TIMEOUT_FETCH),
        "the chunk-fetch timeout error must name the fetch option: {}",
        adbc_error.message
    );
    assert!(
        adbc_error.message.contains("timed out after 0.5s"),
        "the error must report the deadline that fired: {}",
        adbc_error.message
    );
    assert!(
        elapsed >= Duration::from_millis(500),
        "the reader failed after {elapsed:?}, before the 0.5s fetch deadline could fire; the \
         stream died for some other reason"
    );
}

/// Control for [`fetch_timeout_fires_on_a_silent_stream`]: without the fetch deadline, nothing
/// else turns the silence into an error.
#[test]
fn silent_stream_without_the_fetch_deadline_never_times_out() {
    let _watchdog = watchdog!();
    let (server, _open_streams) = silent_stream_server(1);
    let mut connection = server.connect();
    let mut reader = silent_stream_reader(&mut connection, None);

    // Not joined: the thread stays blocked until the server is dropped.
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let first = reader.next();
        let second = reader.next();
        let _ = tx.send((first, second));
    });

    // Four times the 0.5 s deadline used above.
    match rx.recv_timeout(Duration::from_secs(2)) {
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
        Err(other) => panic!("the reader thread died unexpectedly: {other}"),
        Ok((_, second)) => panic!(
            "with the fetch deadline unset, a silent stream must not produce an item; got: {:?}",
            second.map(|r| r.map(|b| b.num_rows()))
        ),
    }
}

/// `spanner.rpc.timeout_seconds.query` bounds `execute`, which waits for the first batch.
#[test]
fn query_timeout_fires_on_a_stream_that_is_silent_from_the_start() {
    let _watchdog = watchdog!();
    let (server, _open_streams) = silent_stream_server(0);
    let mut connection = server.connect();

    let mut statement = connection.new_statement().expect("new statement");
    statement
        .set_option(
            OptionStatement::Other(spanner_adbc::OPTION_RPC_TIMEOUT_QUERY.into()),
            OptionValue::Double(0.5),
        )
        .expect("set the query deadline");
    statement.set_sql_query("SELECT c FROM MockTable").unwrap();

    let started = Instant::now();
    let error = statement
        .execute()
        .err()
        .expect("an execute blocked on a silent stream must expire its query deadline");
    let elapsed = started.elapsed();

    assert_eq!(error.status, AdbcStatus::Timeout, "got error: {error}");
    assert!(
        error
            .message
            .contains(spanner_adbc::OPTION_RPC_TIMEOUT_QUERY),
        "the execute timeout error must name the query option (and so not the fetch one): {}",
        error.message
    );
    assert!(
        error.message.contains("timed out after 0.5s"),
        "the error must report the deadline that fired: {}",
        error.message
    );
    assert!(
        elapsed >= Duration::from_millis(500),
        "execute failed after {elapsed:?}, before the 0.5s query deadline could fire; the stream \
         died for some other reason"
    );
}

/// A new operation on the statement must not clear the cancel on an earlier reader. If it did, the
/// old reader could resume, or end cleanly after a buffered batch and truncate the result.
#[test]
fn new_operation_does_not_uncancel_an_earlier_streamed_reader() {
    let _watchdog = watchdog!();
    // With one row per batch, the old reader has row 2 buffered and row 3 pending at the cancel.
    let server = MockServer::start(move |mock| {
        mock.expect_execute_streaming_sql().returning(move |_| {
            Ok(stream_of(vec![Ok(partial_result_set(
                true,
                &["v1", "v2", "v3"],
                b"rt-1",
                true,
            ))]))
        });
    });

    let mut connection = server.connect();
    let mut statement = connection.new_statement().expect("new statement");
    statement
        .set_option(
            OptionStatement::Other(spanner_adbc::OPTION_ROWS_PER_BATCH.into()),
            OptionValue::Int(1),
        )
        .expect("set rows_per_batch");
    statement.set_sql_query("SELECT c FROM MockTable").unwrap();
    let mut old_reader = statement.execute().expect("first execute");

    let first = old_reader
        .next()
        .expect("first batch exists")
        .expect("first batch is clean");
    assert_eq!(first.num_rows(), 1);

    // Start a new operation before the old reader observes the cancel.
    statement.get_cancel_handle().try_cancel().expect("cancel");
    let new_batches: Vec<_> = statement
        .execute()
        .expect("a new operation after a cancel must start uncancelled")
        .collect::<Result<Vec<_>, _>>()
        .expect("the new operation's reader must stream to completion");
    assert_eq!(
        new_batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        3,
        "the new reader must deliver the full result"
    );

    let item = old_reader
        .next()
        .expect("the cancelled reader must yield an error item, not a clean end of stream");
    let error = item.expect_err("the cancelled reader must not resume streaming rows");
    let adbc_error = driver_error(&error);
    assert_eq!(
        adbc_error.status,
        AdbcStatus::Cancelled,
        "got error: {adbc_error}"
    );
    assert!(
        old_reader.next().is_none(),
        "a cancelled reader must not yield further batches"
    );
}

/// Secondary indexes count toward Spanner's mutation limit, so a chunk within the driver's budget
/// can still be rejected. The driver bisects it until commits succeed. The mock rejects commits
/// with more than 40 mutations.
#[test]
fn ingest_bisects_a_chunk_that_overshoots_the_mutation_limit() {
    let _watchdog = watchdog!();
    let commit_sizes: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));
    let sizes_in_mock = commit_sizes.clone();
    let server = MockServer::start(move |mock| {
        serve_begin_transaction(mock);
        mock.expect_commit().returning(move |request| {
            let mutations = request.into_inner().mutations.len();
            sizes_in_mock.lock().unwrap().push(mutations);
            if mutations > 40 {
                Err(too_many_mutations_status())
            } else {
                commit_ok()
            }
        });
    });

    let mut connection = server.connect();
    let mut statement = append_ingest_statement(&mut connection);
    statement.bind(ingest_batch(100)).expect("bind ingest data");

    let count = statement
        .execute_update()
        .expect("the ingest must self-heal, not fail on the over-limit chunk");

    assert_eq!(
        count,
        Some(100),
        "every bound row must land once the chunk is bisected under the limit"
    );

    // 100 rows bisect to 50, then 25.
    let sizes = commit_sizes.lock().unwrap();
    assert!(
        sizes.len() > 1,
        "the driver must have retried with smaller commits; saw commit sizes {sizes:?}"
    );
    let accepted: usize = sizes.iter().copied().filter(|&n| n <= 40).sum();
    assert_eq!(
        accepted, 100,
        "the accepted (<=40-mutation) commits must cover all 100 rows; saw {sizes:?}"
    );
    assert!(
        sizes.iter().any(|&n| n > 40),
        "the test must actually exercise an over-limit commit; saw {sizes:?}"
    );
}

/// Other commit errors propagate after a single attempt.
#[test]
fn ingest_does_not_bisect_a_non_mutation_limit_error() {
    let _watchdog = watchdog!();
    let commits = Arc::new(AtomicUsize::new(0));
    let commits_in_mock = commits.clone();
    let server = MockServer::start(move |mock| {
        serve_begin_transaction(mock);
        mock.expect_commit().returning(move |_| {
            commits_in_mock.fetch_add(1, Ordering::SeqCst);
            Err(tonic::Status::already_exists(
                "Row [v0] in table MockTable already exists",
            ))
        });
    });

    let mut connection = server.connect();
    let mut statement = append_ingest_statement(&mut connection);
    statement.bind(ingest_batch(100)).expect("bind ingest data");

    let error = statement
        .execute_update()
        .expect_err("a duplicate-key commit must fail the ingest, not be bisected away");

    assert_eq!(error.status, AdbcStatus::Integrity, "got error: {error}");
    assert!(
        error.message.contains("MockTable"),
        "the append remap should name the target table; got: {}",
        error.message
    );
    assert_eq!(
        commits.load(Ordering::SeqCst),
        1,
        "a non-mutation-limit error must not trigger the bisect retry"
    );
}

/// A cancelled commit may still apply on the server, so the error must say the outcome is unknown
/// and that retrying risks duplicates. The unscripted exists-probe fails, which leaves the
/// annotated error unchanged.
#[test]
fn cancelled_ingest_commit_reports_ambiguous_outcome() {
    let _watchdog = watchdog!();
    let commits = Arc::new(AtomicUsize::new(0));
    let commits_in_mock = commits.clone();
    let server = MockServer::start(move |mock| {
        serve_begin_transaction(mock);
        mock.expect_commit().returning(move |_| {
            commits_in_mock.fetch_add(1, Ordering::SeqCst);
            Err(tonic::Status::cancelled("commit cancelled by peer"))
        });
    });

    let mut connection = server.connect();
    let mut statement = append_ingest_statement(&mut connection);
    // A single chunk, so no rows were committed before it.
    statement.bind(ingest_batch(1)).expect("bind ingest data");

    let error = statement
        .execute_update()
        .expect_err("a cancelled commit must fail the ingest");

    assert_eq!(error.status, AdbcStatus::Cancelled, "got error: {error}");
    assert!(
        error.message.contains("outcome is unknown") && error.message.contains("duplicate rows"),
        "a cancelled chunk commit must flag its ambiguous outcome, not imply exact accounting: {}",
        error.message
    );
    assert_eq!(
        commits.load(Ordering::SeqCst),
        1,
        "a cancelled chunk commit must not be retried; it could duplicate rows"
    );
}

/// A failed `append` commit triggers an `INFORMATION_SCHEMA.TABLES` probe to choose between
/// `NotFound` and `AlreadyExists`. If the probe itself fails (for example, no read permission),
/// the original commit error is returned unchanged.
#[test]
fn ingest_append_keeps_the_original_error_when_the_exists_probe_fails() {
    let _watchdog = watchdog!();
    let probes = Arc::new(AtomicUsize::new(0));
    let probes_in_mock = probes.clone();
    let server = MockServer::start(move |mock| {
        serve_begin_transaction(mock);
        // Neither a constraint violation nor the mutation limit, both of which skip the probe.
        mock.expect_commit().returning(|_| {
            Err(tonic::Status::invalid_argument(
                "Invalid value for column c in table MockTable",
            ))
        });
        // The probe is the only query; rows are sent as mutations.
        mock.expect_execute_streaming_sql().returning(move |_| {
            probes_in_mock.fetch_add(1, Ordering::SeqCst);
            Err(tonic::Status::permission_denied(
                "spanner.databases.select denied on INFORMATION_SCHEMA",
            ))
        });
    });

    let mut connection = server.connect();
    let mut statement = append_ingest_statement(&mut connection);
    statement.bind(ingest_batch(1)).expect("bind ingest data");

    let error = statement
        .execute_update()
        .expect_err("the failing commit must fail the ingest");

    assert_eq!(
        probes.load(Ordering::SeqCst),
        1,
        "the append remap must run the exists probe"
    );
    assert_eq!(
        error.status,
        AdbcStatus::InvalidArguments,
        "the probe's own failure must not replace the ingest error; got: {error}"
    );
    assert!(
        error.message.contains("Invalid value for column c"),
        "the original commit error's message must survive; got: {}",
        error.message
    );
    assert!(
        !error.message.contains("INFORMATION_SCHEMA"),
        "the probe's error must not surface in the ingest error; got: {}",
        error.message
    );
    assert_eq!(
        error.vendor_code, 3,
        "the untouched original error keeps INVALID_ARGUMENT = 3; got: {error}"
    );
}

/// BatchWrite reports a failed group as a status inside the response stream, which takes a
/// different error path from gRPC errors. It must map the code, name the table and forward
/// details the same way.
#[test]
fn batch_write_group_failure_forwards_status_details() {
    let _watchdog = watchdog!();
    let batch_writes = Arc::new(AtomicUsize::new(0));
    let writes_in_mock = batch_writes.clone();
    let server = MockServer::start(move |mock| {
        mock.expect_batch_write().returning(move |_| {
            writes_in_mock.fetch_add(1, Ordering::SeqCst);
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            tx.try_send(Ok(batch_write_group_already_exists(0)))
                .expect("scripted stream channel sized to fit");
            Ok(tonic::Response::new(rx))
        });
    });

    let mut connection = server.connect();
    let mut statement = batch_write_ingest_statement(&mut connection);
    // One row per mutation group.
    statement.bind(ingest_batch(1)).expect("bind ingest data");

    let error = statement
        .execute_update()
        .expect_err("a failed mutation group must fail the ingest");

    assert_eq!(
        batch_writes.load(Ordering::SeqCst),
        1,
        "the ingest must have gone through the BatchWrite RPC, not a write-only Commit"
    );
    assert_eq!(error.status, AdbcStatus::Integrity, "got error: {error}");
    assert_eq!(
        error.vendor_code, 6,
        "vendor_code must carry ALREADY_EXISTS = 6; got error: {error}"
    );
    assert!(
        error.message.contains("MockTable"),
        "the append remap should name the target table; got: {}",
        error.message
    );

    let details = error.details.as_ref().unwrap_or_else(|| {
        panic!(
            "the failing group carried a google.rpc.ErrorInfo detail; Error.details must be Some"
        )
    });
    let (_, value) = details
        .iter()
        .find(|(key, _)| key == "google.rpc.errorinfo")
        .unwrap_or_else(|| {
            panic!(
                "expected a `google.rpc.errorinfo` detail, got keys: {:?}",
                details.iter().map(|(k, _)| k).collect::<Vec<_>>()
            )
        });
    let json: serde_json::Value =
        serde_json::from_slice(value).expect("the detail value is UTF-8 ProtoJSON");
    assert_eq!(
        json["@type"], "type.googleapis.com/google.rpc.ErrorInfo",
        "the detail must self-describe as an ErrorInfo, got: {json}"
    );
    assert_eq!(
        json["reason"], "DUPLICATE_KEY",
        "the ErrorInfo's reason must round-trip what the mock sent, got: {json}"
    );
    assert_eq!(
        json["domain"], "spanner.googleapis.com",
        "the ErrorInfo's domain must round-trip what the mock sent, got: {json}"
    );
}

/// BatchWrite is not atomic, so groups that applied before a failure in the same chunk count as
/// committed.
#[test]
fn batch_write_folds_same_chunk_applied_rows_into_committed_count() {
    let _watchdog = watchdog!();
    let server = MockServer::start(move |mock| {
        mock.expect_batch_write().returning(move |_| {
            let (tx, rx) = tokio::sync::mpsc::channel(2);
            tx.try_send(Ok(batch_write_groups_ok(vec![0, 1])))
                .expect("scripted stream channel sized to fit");
            tx.try_send(Ok(batch_write_group_already_exists(2)))
                .expect("scripted stream channel sized to fit");
            Ok(tonic::Response::new(rx))
        });
    });

    let mut connection = server.connect();
    let mut statement = batch_write_ingest_statement(&mut connection);
    statement.bind(ingest_batch(3)).expect("bind ingest data");

    let error = statement
        .execute_update()
        .expect_err("a failed mutation group must fail the ingest");

    assert_eq!(error.status, AdbcStatus::Integrity, "got error: {error}");
    assert!(
        error.message.contains("MockTable"),
        "the append remap should name the target table; got: {}",
        error.message
    );
    assert!(
        error
            .message
            .contains("2 row(s) from this bulk ingest were already committed"),
        "the same-chunk applied groups must be reported; got: {}",
        error.message
    );
}

/// A manual-mode ingest that fails converting a later row buffers none of its rows; otherwise a
/// later commit would apply a partial batch.
#[test]
fn manual_ingest_conversion_failure_leaves_txn_buffer_untouched() {
    let _watchdog = watchdog!();
    let commit_sizes: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));
    let sizes_in_mock = commit_sizes.clone();
    let server = MockServer::start(move |mock| {
        // Serves the append remap's exists-probe and the query after the failure.
        serve_streaming_sql_begin_aware(mock, &["v0"], None);
        serve_begin_transaction(mock);
        mock.expect_commit().returning(move |request| {
            sizes_in_mock
                .lock()
                .unwrap()
                .push(request.into_inner().mutations.len());
            commit_ok()
        });
    });

    let mut connection = server.connect();
    disable_autocommit(&mut connection);

    let mut statement = append_ingest_statement(&mut connection);

    let date_schema = Arc::new(Schema::new(vec![Field::new("d", DataType::Date32, false)]));
    // Row 1 is outside Spanner's DATE range, so conversion fails after row 0 was built.
    let poisoned = RecordBatch::try_new(
        date_schema.clone(),
        vec![Arc::new(Date32Array::from(vec![0, i32::MAX]))],
    )
    .expect("build poisoned ingest batch");
    statement.bind(poisoned).expect("bind poisoned batch");

    let error = statement
        .execute_update()
        .expect_err("an out-of-range date must fail the ingest");
    assert!(
        error.message.contains("out of range"),
        "the conversion failure must surface (folded into the append remap): {}",
        error.message
    );

    // A query is rejected while DML is buffered, so this succeeds only with an empty buffer.
    let mut probe = connection.new_statement().expect("new statement");
    probe.set_sql_query("SELECT c FROM MockTable").unwrap();
    let batches: Vec<_> = probe
        .execute()
        .expect("a query after the failed ingest must run; nothing may be buffered")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect probe batches");
    assert_eq!(batches[0].num_rows(), 1);
    connection
        .rollback()
        .expect("end the probe's query transaction");

    let fixed = RecordBatch::try_new(date_schema, vec![Arc::new(Date32Array::from(vec![0, 1]))])
        .expect("build fixed ingest batch");
    statement.bind(fixed).expect("bind fixed batch");
    assert_eq!(
        statement
            .execute_update()
            .expect("re-ingest of fixed data must buffer"),
        None,
        "a manual-mode ingest buffers (returns None), not commits"
    );

    connection.commit().expect("commit the fixed batch");
    assert_eq!(
        *commit_sizes.lock().unwrap(),
        [2],
        "the commit must carry only the fixed batch's rows, none from the failed batch"
    );
}

/// `spanner.commit_stats` requests commit stats and exposes the server's mutation count. The
/// emulator returns no stats, so only the mock covers the response. The ingest's commit also
/// carries `spanner.commit.max_delay`.
#[test]
fn commit_stats_mutation_count_is_captured_from_the_commit_response() {
    let _watchdog = watchdog!();
    // Not derivable from the two ingested rows.
    const SCRIPTED_MUTATION_COUNT: i64 = 4242;

    let commits: Arc<Mutex<Vec<v1::CommitRequest>>> = Arc::new(Mutex::new(Vec::new()));
    let record_commits = commits.clone();
    let server = MockServer::start(move |mock| {
        mock.expect_begin_transaction().returning(|_| {
            Ok(tonic::Response::new(v1::Transaction {
                id: b"mock-txn-1".to_vec(),
                ..Default::default()
            }))
        });
        // Without a precommit token the client commits once.
        mock.expect_commit().returning(move |request| {
            record_commits.lock().unwrap().push(request.into_inner());
            Ok(tonic::Response::new(v1::CommitResponse {
                commit_stats: Some(v1::commit_response::CommitStats {
                    mutation_count: SCRIPTED_MUTATION_COUNT,
                }),
                ..Default::default()
            }))
        });
    });

    let mut connection = server.connect();
    let mut statement = append_ingest_statement(&mut connection);
    statement
        .set_option(
            OptionStatement::Other(spanner_adbc::OPTION_COMMIT_STATS.into()),
            OptionValue::String("true".into()),
        )
        .expect("enable commit stats");
    statement
        .set_option(
            OptionStatement::Other(spanner_adbc::OPTION_MAX_COMMIT_DELAY.into()),
            OptionValue::String("100ms".into()),
        )
        .expect("set the commit delay");

    let rows = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("Id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![1_i64, 2]))],
    )
    .expect("build ingest batch");
    statement.bind(rows).expect("bind ingest rows");

    assert_eq!(
        statement.execute_update().expect("append ingest"),
        Some(2),
        "the ingest reports the number of rows it applied"
    );

    {
        let commits = commits.lock().unwrap();
        assert_eq!(commits.len(), 1, "the ingest chunk commits exactly once");
        assert!(
            commits[0].return_commit_stats,
            "spanner.commit_stats=true must make the driver set return_commit_stats on the \
             CommitRequest"
        );
        assert_eq!(
            commits[0].max_commit_delay,
            Some(prost_types::Duration {
                seconds: 0,
                nanos: 100_000_000,
            }),
            "spanner.commit.max_delay=100ms must reach the write-only ingest CommitRequest verbatim"
        );
    }
    assert_eq!(
        statement
            .get_option_int(OptionStatement::Other(
                spanner_adbc::OPTION_COMMIT_STATS_MUTATION_COUNT.into()
            ))
            .expect("mutation count must be readable after a stats-bearing commit"),
        SCRIPTED_MUTATION_COUNT,
        "the driver must read back the server's mutation_count verbatim"
    );
}

/// `spanner.commit.max_delay` on autocommit DML and manual commits: unset, inherited from the
/// connection, overridden per statement, and the connection's value on a manual commit.
#[test]
fn max_commit_delay_reaches_the_wire_on_runner_commits() {
    let _watchdog = watchdog!();
    const DML_SQL: &str = "INSERT INTO MockTable (c) VALUES ('x')";

    fn delay_millis(millis: i32) -> Option<prost_types::Duration> {
        Some(prost_types::Duration {
            seconds: 0,
            nanos: millis * 1_000_000,
        })
    }

    let commits: Arc<Mutex<Vec<v1::CommitRequest>>> = Arc::new(Mutex::new(Vec::new()));
    let record = commits.clone();
    let server = MockServer::start(move |mock| {
        serve_begin_transaction(mock);
        serve_batch_dml(mock, BatchDmlRequests::default());
        mock.expect_commit().returning(move |request| {
            record.lock().unwrap().push(request.into_inner());
            commit_ok()
        });
    });

    let mut connection = server.connect();

    let run_autocommit_dml = |connection: &mut SpannerConnection, delay: Option<&str>| {
        let mut statement = connection.new_statement().expect("new statement");
        if let Some(value) = delay {
            statement
                .set_option(
                    OptionStatement::Other(spanner_adbc::OPTION_MAX_COMMIT_DELAY.into()),
                    OptionValue::String(value.into()),
                )
                .expect("set statement-level commit delay");
        }
        statement.set_sql_query(DML_SQL).unwrap();
        assert_eq!(
            statement.execute_update().expect("autocommit DML"),
            Some(1),
            "the scripted DML affects one row"
        );
    };

    run_autocommit_dml(&mut connection, None);

    connection
        .set_option(
            OptionConnection::Other(spanner_adbc::OPTION_MAX_COMMIT_DELAY.into()),
            OptionValue::String("100ms".into()),
        )
        .expect("set connection-level commit delay");
    run_autocommit_dml(&mut connection, None);
    run_autocommit_dml(&mut connection, Some("250ms"));

    disable_autocommit(&mut connection);
    let mut buffered = connection.new_statement().expect("new statement");
    buffered.set_sql_query(DML_SQL).unwrap();
    assert_eq!(
        buffered.execute_update().expect("manual-mode DML buffers"),
        None,
        "manual-mode DML buffers (returns None), not commits"
    );
    connection.commit().expect("commit the buffered DML");

    let commits = commits.lock().unwrap();
    assert_eq!(
        commits.len(),
        4,
        "one commit per transaction: three autocommit DMLs, then the manual commit"
    );
    assert_eq!(
        commits[0].max_commit_delay, None,
        "with spanner.commit.max_delay unset, the driver must send no max_commit_delay at all"
    );
    assert_eq!(
        commits[1].max_commit_delay,
        delay_millis(100),
        "an autocommit DML must commit with the connection's inherited 100ms delay"
    );
    assert_eq!(
        commits[2].max_commit_delay,
        delay_millis(250),
        "the statement-level 250ms must override the connection's 100ms"
    );
    assert_eq!(
        commits[3].max_commit_delay,
        delay_millis(100),
        "the manual-mode commit must carry the connection's 100ms delay"
    );
}

/// The ADBC isolation level reaches the inline begin of autocommit DML. Spanner implements
/// `REPEATABLE_READ` as snapshot isolation; levels it lacks are promoted to the weakest supported
/// level that satisfies them.
#[test]
fn isolation_level_reaches_transaction_options_on_the_begin() {
    use adbc_core::constants::*;
    use v1::transaction_options::IsolationLevel;

    let _watchdog = watchdog!();
    const DML_SQL: &str = "INSERT INTO MockTable (c) VALUES ('x')";

    let batches: Arc<Mutex<Vec<v1::ExecuteBatchDmlRequest>>> = Arc::new(Mutex::new(Vec::new()));
    let record = batches.clone();
    let server = MockServer::start(move |mock| {
        serve_begin_transaction(mock);
        serve_batch_dml(mock, record.clone());
        mock.expect_commit().returning(|_| commit_ok());
    });

    // The requested level (`None` = never set) and the expected wire level. Unit tests in
    // src/connection/tests.rs cover the mapping of every ADBC level.
    let cases: [(Option<&str>, IsolationLevel); 4] = [
        (None, IsolationLevel::Unspecified),
        (
            Some(ADBC_OPTION_ISOLATION_LEVEL_REPEATABLE_READ),
            IsolationLevel::RepeatableRead,
        ),
        (
            Some(ADBC_OPTION_ISOLATION_LEVEL_SERIALIZABLE),
            IsolationLevel::Serializable,
        ),
        (
            Some(ADBC_OPTION_ISOLATION_LEVEL_DEFAULT),
            IsolationLevel::Unspecified,
        ),
    ];

    let mut connection = server.connect();
    for (requested, _) in &cases {
        if let Some(level) = requested {
            connection
                .set_option(
                    OptionConnection::IsolationLevel,
                    OptionValue::String((*level).into()),
                )
                .expect("set the connection's isolation level");
        }
        let mut statement = connection.new_statement().expect("new statement");
        statement.set_sql_query(DML_SQL).unwrap();
        assert_eq!(
            statement.execute_update().expect("autocommit DML"),
            Some(1),
            "the scripted DML affects one row"
        );
    }

    let batches = batches.lock().unwrap();
    assert_eq!(
        batches.len(),
        cases.len(),
        "one ExecuteBatchDml per autocommit DML"
    );
    for ((requested, expected), request) in cases.iter().zip(batches.iter()) {
        let Some(v1::transaction_selector::Selector::Begin(options)) = request
            .transaction
            .as_ref()
            .and_then(|t| t.selector.as_ref())
        else {
            panic!("an autocommit DML must begin its read/write transaction inline: {request:?}");
        };
        assert_eq!(
            options.isolation_level,
            *expected as i32,
            "isolation level {requested:?} must reach TransactionOptions as {}",
            expected.as_str_name()
        );
    }
}

/// A manual transaction with only mutations commits through the client's replay-protected
/// write-only transaction (a begin with `mutation_key`, no DML). With DML buffered, it uses the
/// read/write runner, which begins inline.
#[test]
fn mutations_only_manual_commit_uses_the_write_only_path() {
    let _watchdog = watchdog!();
    let begins: Arc<Mutex<Vec<v1::BeginTransactionRequest>>> = Arc::new(Mutex::new(Vec::new()));
    let commits: Arc<Mutex<Vec<v1::CommitRequest>>> = Arc::new(Mutex::new(Vec::new()));
    let batch_dml: BatchDmlRequests = Arc::new(Mutex::new(Vec::new()));
    let record_begins = begins.clone();
    let record_commits = commits.clone();
    let record_batch_dml = batch_dml.clone();
    let server = MockServer::start(move |mock| {
        mock.expect_begin_transaction().returning(move |request| {
            record_begins.lock().unwrap().push(request.into_inner());
            Ok(tonic::Response::new(v1::Transaction {
                id: b"mock-txn".to_vec(),
                ..Default::default()
            }))
        });
        serve_batch_dml(mock, record_batch_dml.clone());
        mock.expect_commit().returning(move |request| {
            record_commits.lock().unwrap().push(request.into_inner());
            commit_ok()
        });
    });

    let mut connection = server.connect();
    disable_autocommit(&mut connection);

    let mut ingest = append_ingest_statement(&mut connection);

    ingest.bind(ingest_batch(2)).expect("bind ingest rows");
    assert_eq!(
        ingest.execute_update().expect("manual-mode ingest buffers"),
        None,
        "a manual-mode ingest buffers (returns None), not commits"
    );
    connection
        .commit()
        .expect("commit the mutations-only transaction");

    {
        let begins = begins.lock().unwrap();
        assert_eq!(
            begins.len(),
            1,
            "a mutations-only commit begins exactly one (write-only) transaction"
        );
        assert!(
            begins[0].mutation_key.is_some(),
            "the write-only begin must carry a mutation_key: {:?}",
            begins[0]
        );
    }
    assert_eq!(
        batch_dml.lock().unwrap().len(),
        0,
        "a mutations-only commit must not issue ExecuteBatchDml"
    );
    {
        let commits = commits.lock().unwrap();
        assert_eq!(commits.len(), 1, "exactly one commit so far");
        assert_eq!(
            commits[0].mutations.len(),
            2,
            "the write-only commit must carry the ingest's two mutations"
        );
        assert!(
            matches!(
                commits[0].transaction,
                Some(v1::commit_request::Transaction::TransactionId(_))
            ),
            "the replay-protected write commits by transaction id (never single-use): {:?}",
            commits[0].transaction
        );
    }

    let mut dml = connection.new_statement().expect("new statement");
    dml.set_sql_query("UPDATE MockTable SET c = 'x' WHERE TRUE")
        .expect("set DML");
    assert_eq!(
        dml.execute_update().expect("manual-mode DML buffers"),
        None,
        "manual-mode DML buffers (returns None), not commits"
    );
    ingest.bind(ingest_batch(1)).expect("bind second ingest");
    assert_eq!(
        ingest.execute_update().expect("second ingest buffers"),
        None
    );
    connection.commit().expect("commit the DML transaction");

    assert_eq!(
        batch_dml.lock().unwrap().len(),
        1,
        "a commit with buffered DML must run it via ExecuteBatchDml (the read/write runner)"
    );
    {
        let begins = begins.lock().unwrap();
        assert_eq!(
            begins.len(),
            1,
            "the DML commit must begin inline, not with a second BeginTransaction: {begins:?}"
        );
    }
    {
        let commits = commits.lock().unwrap();
        assert_eq!(commits.len(), 2, "the DML transaction adds a second commit");
        assert_eq!(
            commits[1].mutations.len(),
            1,
            "the runner's commit must carry the second ingest's buffered mutation"
        );
    }
}

/// `adbc.connection.readonly` also blocks committing DML buffered before it was set, whether by
/// `commit()` or by re-enabling autocommit. The buffer survives for a later writable commit;
/// rollback, queries and query-transaction commits still work.
#[test]
fn read_only_connection_rejects_the_commit_of_buffered_dml() {
    let _watchdog = watchdog!();
    let batch_dml: BatchDmlRequests = Arc::new(Mutex::new(Vec::new()));
    let commits = Arc::new(AtomicUsize::new(0));
    let record_batch_dml = batch_dml.clone();
    let count_commits = commits.clone();
    let server = MockServer::start(move |mock| {
        serve_streaming_sql_begin_aware(mock, &["v0"], None);
        serve_begin_transaction(mock);
        serve_batch_dml(mock, record_batch_dml.clone());
        mock.expect_commit().returning(move |_| {
            count_commits.fetch_add(1, Ordering::SeqCst);
            commit_ok()
        });
    });

    let dml_sqls = || {
        batch_dml
            .lock()
            .unwrap()
            .iter()
            .flat_map(|request| request.statements.iter().map(|s| s.sql.clone()))
            .collect::<Vec<_>>()
    };

    let mut connection = server.connect();
    disable_autocommit(&mut connection);

    let mut dml = connection.new_statement().expect("new statement");
    dml.set_sql_query("UPDATE MockTable SET c = 'x' WHERE TRUE")
        .expect("set DML");
    assert_eq!(
        dml.execute_update().expect("manual-mode DML buffers"),
        None,
        "manual-mode DML buffers (returns None), not commits"
    );
    let set_read_only = |connection: &mut SpannerConnection, value: &str| {
        connection
            .set_option(
                OptionConnection::ReadOnly,
                OptionValue::String(value.into()),
            )
            .expect("set adbc.connection.readonly");
    };
    set_read_only(&mut connection, "true");

    let error = connection
        .commit()
        .expect_err("a read-only connection must not commit buffered DML");
    assert_eq!(error.status, AdbcStatus::InvalidState);
    assert!(
        error.message.contains("read-only"),
        "the rejection must name the read-only flag: {}",
        error.message
    );

    // Enabling autocommit would commit the pending work.
    let error = connection
        .set_option(
            OptionConnection::AutoCommit,
            OptionValue::String("true".into()),
        )
        .expect_err("enabling autocommit must not commit buffered DML on a read-only connection");
    assert_eq!(error.status, AdbcStatus::InvalidState);
    assert_eq!(
        connection
            .get_option_string(OptionConnection::AutoCommit)
            .expect("read back autocommit"),
        "false",
        "a rejected toggle must leave the connection in manual mode"
    );

    assert!(
        dml_sqls().is_empty(),
        "no DML may reach the wire while the connection is read-only: {:?}",
        dml_sqls()
    );
    assert_eq!(
        commits.load(Ordering::SeqCst),
        0,
        "no CommitRequest may reach the wire while the connection is read-only"
    );

    set_read_only(&mut connection, "false");
    connection
        .commit()
        .expect("a writable connection commits the still-buffered DML");
    assert_eq!(
        dml_sqls(),
        ["UPDATE MockTable SET c = 'x' WHERE TRUE"],
        "the commit must replay exactly the DML buffered before the flag was set"
    );
    assert_eq!(commits.load(Ordering::SeqCst), 1, "exactly one commit");

    dml.execute_update().expect("buffer DML again");
    set_read_only(&mut connection, "true");
    connection
        .rollback()
        .expect("rollback must work on a read-only connection");

    let mut query = connection.new_statement().expect("new statement");
    query
        .set_sql_query("SELECT c FROM MockTable")
        .expect("set query");
    let batches: Vec<_> = query
        .execute()
        .expect("queries still run on a read-only connection")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect query batches");
    assert_eq!(batches[0].num_rows(), 1);
    connection
        .commit()
        .expect("committing a query transaction writes nothing, so read-only must allow it");

    assert_eq!(
        dml_sqls(),
        ["UPDATE MockTable SET c = 'x' WHERE TRUE"],
        "the rolled-back DML must never reach the wire"
    );
    assert_eq!(
        commits.load(Ordering::SeqCst),
        1,
        "neither the rollback nor the query transaction's commit issues a CommitRequest"
    );
}

// ---------------------------------------------------------------------------
// Read options on the wire: spanner.read.staleness and spanner.directed_read
// ---------------------------------------------------------------------------

const READ_TIMESTAMP_RFC3339: &str = "2026-07-07T00:00:00Z";
const READ_TIMESTAMP_UNIX: i64 = 1_783_382_400;

fn single_use_read_only_bound(
    selector: Option<&v1::TransactionSelector>,
) -> v1::transaction_options::read_only::TimestampBound {
    let selector = selector
        .and_then(|s| s.selector.as_ref())
        .expect("the query must carry a transaction selector");
    let v1::transaction_selector::Selector::SingleUse(options) = selector else {
        panic!("a plain autocommit query must run single-use, got: {selector:?}");
    };
    read_only_bound(options)
}

fn read_only_bound(
    options: &v1::TransactionOptions,
) -> v1::transaction_options::read_only::TimestampBound {
    let Some(v1::transaction_options::Mode::ReadOnly(read_only)) = &options.mode else {
        panic!("expected read-only transaction options, got: {options:?}");
    };
    read_only
        .timestamp_bound
        .expect("the read-only transaction options must carry a timestamp bound")
}

/// Each `spanner.read.staleness` form reaches the single-use selector, inherited from the
/// connection or set on the statement.
#[test]
fn read_staleness_reaches_the_wire_on_single_use_queries() {
    use v1::transaction_options::read_only::TimestampBound as WireBound;

    let _watchdog = watchdog!();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let record = requests.clone();
    let server = MockServer::start(move |mock| {
        serve_streaming_sql_begin_aware(mock, &["v1"], Some(record));
    });

    let mut connection = server.connect();
    connection
        .set_option(
            OptionConnection::Other(spanner_adbc::OPTION_READ_STALENESS.into()),
            OptionValue::String("exact:10s".into()),
        )
        .expect("set connection-level staleness");

    let mut run_query = |staleness: Option<&str>| {
        let mut statement = connection.new_statement().expect("new statement");
        if let Some(value) = staleness {
            statement
                .set_option(
                    OptionStatement::Other(spanner_adbc::OPTION_READ_STALENESS.into()),
                    OptionValue::String(value.into()),
                )
                .expect("set statement-level staleness");
        }
        statement.set_sql_query("SELECT c FROM MockTable").unwrap();
        statement
            .execute()
            .expect("query against mock server")
            .collect::<Result<Vec<_>, _>>()
            .expect("collect batches");
    };
    run_query(None);
    run_query(Some("max:500ms"));
    run_query(Some(&format!("read:{READ_TIMESTAMP_RFC3339}")));
    run_query(Some(&format!("min:{READ_TIMESTAMP_RFC3339}")));

    let selectors = transaction_selectors(&requests);
    assert_eq!(selectors.len(), 4, "one request per staleness form");
    let bounds: Vec<WireBound> = selectors
        .iter()
        .map(|s| single_use_read_only_bound(s.as_ref()))
        .collect();
    assert_eq!(
        bounds[0],
        WireBound::ExactStaleness(prost_types::Duration {
            seconds: 10,
            nanos: 0,
        }),
        "exact:10s (inherited from the connection) must arrive as exact_staleness"
    );
    assert_eq!(
        bounds[1],
        WireBound::MaxStaleness(prost_types::Duration {
            seconds: 0,
            nanos: 500_000_000,
        }),
        "max:500ms (the statement's override of the connection value) must arrive as max_staleness"
    );
    assert_eq!(
        bounds[2],
        WireBound::ReadTimestamp(prost_types::Timestamp {
            seconds: READ_TIMESTAMP_UNIX,
            nanos: 0,
        }),
        "read:<rfc3339> must arrive as read_timestamp"
    );
    assert_eq!(
        bounds[3],
        WireBound::MinReadTimestamp(prost_types::Timestamp {
            seconds: READ_TIMESTAMP_UNIX,
            nanos: 0,
        }),
        "min:<rfc3339> must arrive as min_read_timestamp"
    );
}

/// Staleness also applies to `execute_schema` (single-use) and to the `get_objects` snapshot,
/// which begins a read-only transaction inline.
#[test]
fn read_staleness_reaches_the_wire_on_metadata_reads() {
    use adbc_core::options::ObjectDepth;
    use v1::transaction_options::read_only::TimestampBound as WireBound;

    let _watchdog = watchdog!();
    let expected = WireBound::ExactStaleness(prost_types::Duration {
        seconds: 10,
        nanos: 0,
    });
    let requests = Arc::new(Mutex::new(Vec::new()));
    let server = MockServer::start({
        let record = requests.clone();
        move |mock| serve_streaming_sql_begin_aware(mock, &["default"], Some(record))
    });

    let mut connection = server.connect();
    connection
        .set_option(
            OptionConnection::Other(spanner_adbc::OPTION_READ_STALENESS.into()),
            OptionValue::String("exact:10s".into()),
        )
        .expect("set connection-level staleness");

    let mut statement = connection.new_statement().expect("new statement");
    statement.set_sql_query("SELECT c FROM MockTable").unwrap();
    statement.execute_schema().expect("execute_schema probe");
    {
        let selectors = transaction_selectors(&requests);
        assert_eq!(selectors.len(), 1, "one PLAN probe request");
        assert_eq!(
            single_use_read_only_bound(selectors[0].as_ref()),
            expected,
            "the execute_schema PLAN probe must carry the connection's exact:10s staleness"
        );
    }

    // At `Schemas` depth, get_objects issues only the SCHEMATA query.
    let reader = connection
        .get_objects(ObjectDepth::Schemas, None, None, None, None, None)
        .expect("get_objects");
    reader
        .collect::<Result<Vec<_>, _>>()
        .expect("drain get_objects");
    {
        let selectors = transaction_selectors(&requests);
        let begin = selectors
            .iter()
            .find_map(|s| match s.as_ref().and_then(|s| s.selector.as_ref()) {
                Some(v1::transaction_selector::Selector::Begin(options)) => Some(options),
                _ => None,
            })
            .expect("get_objects must begin its read-only transaction inline");
        assert_eq!(
            read_only_bound(begin),
            expected,
            "the get_objects snapshot must carry the connection's exact:10s staleness"
        );
    }
}

/// Runs a query with two bound rows and returns each request's transaction selector. Multiple rows
/// share a multi-use read-only transaction that the first request begins inline.
fn bound_query_transaction_selectors(staleness: &str) -> Vec<Option<v1::TransactionSelector>> {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let record = requests.clone();
    let server = MockServer::start(move |mock| {
        serve_streaming_sql_begin_aware(mock, &["v"], Some(record));
    });

    let mut connection = server.connect();
    let mut statement = connection.new_statement().expect("new statement");
    statement
        .set_option(
            OptionStatement::Other(spanner_adbc::OPTION_READ_STALENESS.into()),
            OptionValue::String(staleness.into()),
        )
        .expect("set staleness");
    statement
        .set_sql_query("SELECT c FROM MockTable WHERE c = @val")
        .expect("set query");
    let rows = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("val", DataType::Utf8, false)])),
        vec![Arc::new(StringArray::from(vec!["a", "b"]))],
    )
    .expect("build bound batch");
    statement.bind(rows).expect("bind rows");
    let total: usize = statement
        .execute()
        .expect("bound query against mock server")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect batches")
        .iter()
        .map(RecordBatch::num_rows)
        .sum();
    assert_eq!(total, 2, "one result row per bound row ({staleness})");

    transaction_selectors(&requests)
}

/// Spanner allows bounded staleness only on single-use transactions, so a multi-row bound query
/// pins `max:<d>` to exact staleness `<d>` and `min:<t>` to read timestamp `<t>`.
#[test]
fn bounded_staleness_is_pinned_for_multi_use_bound_queries() {
    use v1::transaction_options::read_only::TimestampBound as WireBound;

    let _watchdog = watchdog!();
    let cases = [
        (
            "max:500ms",
            WireBound::ExactStaleness(prost_types::Duration {
                seconds: 0,
                nanos: 500_000_000,
            }),
        ),
        (
            &format!("min:{READ_TIMESTAMP_RFC3339}") as &str,
            WireBound::ReadTimestamp(prost_types::Timestamp {
                seconds: READ_TIMESTAMP_UNIX,
                nanos: 0,
            }),
        ),
    ];
    for (staleness, expected_pin) in cases {
        let selectors = bound_query_transaction_selectors(staleness);
        assert_eq!(
            selectors.len(),
            2,
            "two bound rows must produce two statements ({staleness})"
        );

        let first = selectors[0]
            .as_ref()
            .and_then(|s| s.selector.as_ref())
            .expect("the first statement must carry a transaction selector");
        let v1::transaction_selector::Selector::Begin(options) = first else {
            panic!("the first bound-row statement must begin the transaction, got: {first:?}");
        };
        assert_eq!(
            read_only_bound(options),
            expected_pin,
            "{staleness} must be pinned to its most-stale legal multi-use equivalent"
        );

        let second = selectors[1]
            .as_ref()
            .and_then(|s| s.selector.as_ref())
            .expect("the second statement must carry a transaction selector");
        assert_eq!(
            second,
            &v1::transaction_selector::Selector::Id(b"inline-txn".to_vec()),
            "later bound-row statements must reuse the begun transaction ({staleness})"
        );
    }
}

/// `spanner.directed_read` applies to queries but not DML: Spanner rejects directed reads in a
/// read/write transaction. `THEN RETURN` DML is the risky case because it uses `ExecuteSql` like a
/// query; plain DML uses `ExecuteBatchDml`, which has no directed-read field.
#[test]
fn directed_read_reaches_the_wire_on_queries_but_never_on_dml() {
    use v1::directed_read_options as dro;

    let _watchdog = watchdog!();
    const QUERY_SQL: &str = "SELECT c FROM MockTable";
    const OVERRIDE_QUERY_SQL: &str = "SELECT c FROM OtherMockTable";
    const INSERT_SQL: &str = "INSERT INTO MockTable (c) VALUES ('x')";
    const RETURNING_SQL: &str = "INSERT INTO MockTable (c) VALUES ('y') THEN RETURN c";

    let streaming = Arc::new(Mutex::new(Vec::new()));
    let batch_dml: Arc<Mutex<Vec<v1::ExecuteBatchDmlRequest>>> = Arc::new(Mutex::new(Vec::new()));
    let record_streaming = streaming.clone();
    let record_batch_dml = batch_dml.clone();
    let server = MockServer::start(move |mock| {
        serve_begin_transaction(mock);
        serve_streaming_sql_begin_aware(mock, &["v"], Some(record_streaming));
        serve_batch_dml(mock, record_batch_dml.clone());
        mock.expect_commit().returning(|_| commit_ok());
    });

    let mut connection = server.connect();
    connection
        .set_option(
            OptionConnection::Other(spanner_adbc::OPTION_DIRECTED_READ.into()),
            OptionValue::String("include:us-east1:read_only".into()),
        )
        .expect("set connection-level directed read");

    let mut query = connection.new_statement().expect("new statement");
    query.set_sql_query(QUERY_SQL).unwrap();
    query
        .execute()
        .expect("query against mock server")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect batches");

    let mut override_query = connection.new_statement().expect("new statement");
    override_query
        .set_option(
            OptionStatement::Other(spanner_adbc::OPTION_DIRECTED_READ.into()),
            OptionValue::String("exclude:eu-west1".into()),
        )
        .expect("set statement-level directed read");
    override_query.set_sql_query(OVERRIDE_QUERY_SQL).unwrap();
    override_query
        .execute()
        .expect("override query against mock server")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect batches");

    let mut insert = connection.new_statement().expect("new statement");
    insert.set_sql_query(INSERT_SQL).unwrap();
    assert_eq!(
        insert.execute_update().expect("autocommit insert"),
        Some(1),
        "the scripted batch-DML row count must surface"
    );

    let mut returning = connection.new_statement().expect("new statement");
    returning.set_sql_query(RETURNING_SQL).unwrap();
    assert_eq!(
        returning.execute_update().expect("THEN RETURN insert"),
        Some(1),
        "the drained THEN RETURN row count must surface"
    );

    let streaming = streaming.lock().unwrap();
    let directed_for = |sql: &str| -> Option<v1::DirectedReadOptions> {
        streaming
            .iter()
            .find(|request| request.sql == sql)
            .unwrap_or_else(|| {
                panic!(
                    "no ExecuteSql request seen for {sql:?}; saw: {:?}",
                    streaming
                        .iter()
                        .map(|request| &request.sql)
                        .collect::<Vec<_>>()
                )
            })
            .directed_read_options
            .clone()
    };

    assert_eq!(
        directed_for(QUERY_SQL),
        Some(v1::DirectedReadOptions {
            replicas: Some(dro::Replicas::IncludeReplicas(dro::IncludeReplicas {
                replica_selections: vec![dro::ReplicaSelection {
                    location: "us-east1".to_string(),
                    r#type: dro::replica_selection::Type::ReadOnly as i32,
                }],
                auto_failover_disabled: false,
            })),
        }),
        "the connection-level include list must land on ExecuteSqlRequest.directed_read_options"
    );

    assert_eq!(
        directed_for(OVERRIDE_QUERY_SQL),
        Some(v1::DirectedReadOptions {
            replicas: Some(dro::Replicas::ExcludeReplicas(dro::ExcludeReplicas {
                replica_selections: vec![dro::ReplicaSelection {
                    location: "eu-west1".to_string(),
                    r#type: dro::replica_selection::Type::Unspecified as i32,
                }],
            })),
        }),
        "a statement-level value must override the inherited connection-level one on the wire"
    );

    assert_eq!(
        directed_for(RETURNING_SQL),
        None,
        "DML must never carry directed-read options, even with the connection option set"
    );

    let batch_dml = batch_dml.lock().unwrap();
    assert_eq!(
        batch_dml.len(),
        1,
        "the plain INSERT must ride a single ExecuteBatchDml"
    );
    assert_eq!(batch_dml[0].statements.len(), 1);
    assert_eq!(batch_dml[0].statements[0].sql, INSERT_SQL);
}

/// The optimizer version and statistics package reach `query_options`, inherited, overridden and
/// partly overridden.
#[test]
fn query_optimizer_options_reach_the_wire_on_queries() {
    let _watchdog = watchdog!();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let record = requests.clone();
    let server = MockServer::start(move |mock| {
        serve_streaming_sql_begin_aware(mock, &["v1"], Some(record));
    });

    let mut connection = server.connect();
    for (key, value) in [
        (spanner_adbc::OPTION_QUERY_OPTIMIZER_VERSION, "6"),
        (
            spanner_adbc::OPTION_QUERY_OPTIMIZER_STATISTICS_PACKAGE,
            "auto_20260101",
        ),
    ] {
        connection
            .set_option(
                OptionConnection::Other(key.into()),
                OptionValue::String(value.into()),
            )
            .expect("set the connection-level query optimizer option");
    }

    let mut run_query = |sql: &str, overrides: &[(&str, &str)]| {
        let mut statement = connection.new_statement().expect("new statement");
        for (key, value) in overrides {
            statement
                .set_option(
                    OptionStatement::Other((*key).into()),
                    OptionValue::String((*value).into()),
                )
                .expect("set the statement-level query optimizer option");
        }
        statement.set_sql_query(sql).unwrap();
        statement
            .execute()
            .expect("query against mock server")
            .collect::<Result<Vec<_>, _>>()
            .expect("collect batches");
    };
    run_query("SELECT c FROM Inherited", &[]);
    run_query(
        "SELECT c FROM Overridden",
        &[
            (spanner_adbc::OPTION_QUERY_OPTIMIZER_VERSION, "latest"),
            (
                spanner_adbc::OPTION_QUERY_OPTIMIZER_STATISTICS_PACKAGE,
                "custom_package",
            ),
        ],
    );
    run_query(
        "SELECT c FROM PartlyOverridden",
        &[(spanner_adbc::OPTION_QUERY_OPTIMIZER_VERSION, "7")],
    );

    let requests = requests.lock().unwrap();
    let options_for = |sql: &str| {
        requests
            .iter()
            .find(|request| request.sql == sql)
            .unwrap_or_else(|| panic!("no ExecuteSqlRequest for {sql:?}"))
            .query_options
            .clone()
    };
    assert_eq!(requests.len(), 3, "one request per query");
    assert_eq!(
        options_for("SELECT c FROM Inherited"),
        Some(v1::execute_sql_request::QueryOptions {
            optimizer_version: "6".to_string(),
            optimizer_statistics_package: "auto_20260101".to_string(),
        }),
        "both connection-level values must be inherited onto the request's query_options"
    );
    assert_eq!(
        options_for("SELECT c FROM Overridden"),
        Some(v1::execute_sql_request::QueryOptions {
            optimizer_version: "latest".to_string(),
            optimizer_statistics_package: "custom_package".to_string(),
        }),
        "statement-level values must override the inherited connection-level ones on the wire"
    );
    assert_eq!(
        options_for("SELECT c FROM PartlyOverridden"),
        Some(v1::execute_sql_request::QueryOptions {
            optimizer_version: "7".to_string(),
            optimizer_statistics_package: "auto_20260101".to_string(),
        }),
        "the two knobs are independent: overriding the version must leave the inherited \
         statistics package in place"
    );
}

/// An autocommit DML batch is the whole transaction, so it sets `last_statements`. DML replayed by
/// a manual commit does not, since buffered mutations may follow it.
#[test]
fn autocommit_batch_dml_is_flagged_last_statements_but_manual_commit_is_not() {
    let _watchdog = watchdog!();
    const BATCH_SQL: &str =
        "DELETE FROM MockTable WHERE TRUE; INSERT INTO MockTable (c) VALUES ('x')";

    let batch_dml: Arc<Mutex<Vec<v1::ExecuteBatchDmlRequest>>> = Arc::new(Mutex::new(Vec::new()));
    let record = batch_dml.clone();
    let server = MockServer::start(move |mock| {
        serve_begin_transaction(mock);
        serve_batch_dml(mock, record.clone());
        mock.expect_commit().returning(|_| commit_ok());
    });

    let mut connection = server.connect();

    let mut batch = connection.new_statement().expect("new statement");
    batch.set_sql_query(BATCH_SQL).unwrap();
    assert_eq!(
        batch.execute_update().expect("autocommit `;`-batch"),
        Some(2),
        "the summed batch-DML row count must surface"
    );

    disable_autocommit(&mut connection);
    let mut buffered = connection.new_statement().expect("new statement");
    buffered.set_sql_query(BATCH_SQL).unwrap();
    assert_eq!(
        buffered.execute_update().expect("buffered `;`-batch"),
        None,
        "DML in manual mode buffers (returns None), not commits"
    );
    connection.commit().expect("commit the buffered DML");

    let seen = batch_dml.lock().unwrap();
    assert_eq!(
        seen.len(),
        2,
        "one ExecuteBatchDml per transaction: the autocommit batch, then the manual commit"
    );
    assert_eq!(
        seen[0].statements.len(),
        2,
        "both statements ride one batch"
    );
    assert!(
        seen[0].last_statements,
        "a mutation-free autocommit `;`-batch is the transaction's entire content, so it must \
         be flagged as the transaction's last request"
    );
    assert_eq!(seen[1].statements.len(), 2, "the commit replays the buffer");
    assert!(
        !seen[1].last_statements,
        "buffered manual-mode DML replayed at commit must NOT be flagged; the commit may still \
         apply buffered mutations after the batch executes"
    );
}

/// BatchWrite carries the priority, transaction tag and change-stream exclusion. Spanner ignores
/// request tags on BatchWrite, so none is sent.
#[test]
fn batch_write_carries_the_request_options_and_change_stream_exclusion() {
    let _watchdog = watchdog!();
    let requests: Arc<Mutex<Vec<v1::BatchWriteRequest>>> = Arc::new(Mutex::new(Vec::new()));
    let record = requests.clone();
    let server = MockServer::start(move |mock| {
        mock.expect_batch_write().returning(move |request| {
            record.lock().unwrap().push(request.into_inner());
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            tx.try_send(Ok(v1::BatchWriteResponse {
                indexes: vec![0],
                status: Some(spanner_grpc_mock::google::rpc::Status {
                    code: 0,
                    message: "OK".into(),
                    details: vec![],
                }),
                ..Default::default()
            }))
            .expect("scripted stream channel sized to fit");
            Ok(tonic::Response::new(rx))
        });
    });

    let mut connection = server.connect();
    for (key, value) in [
        (spanner_adbc::OPTION_REQUEST_PRIORITY, "high"),
        (spanner_adbc::OPTION_TRANSACTION_TAG, "nightly-etl"),
        (spanner_adbc::OPTION_REQUEST_TAG, "ignored-by-batch-write"),
    ] {
        connection
            .set_option(
                OptionConnection::Other(key.into()),
                OptionValue::String(value.into()),
            )
            .expect("set the connection-level request option");
    }

    let run_ingest = |connection: &mut SpannerConnection| {
        let mut statement = batch_write_ingest_statement(connection);
        statement.bind(ingest_batch(1)).expect("bind ingest data");
        assert_eq!(
            statement.execute_update().expect("BatchWrite ingest"),
            Some(1)
        );
    };

    run_ingest(&mut connection);
    {
        let seen = requests.lock().unwrap();
        assert_eq!(seen.len(), 1, "one chunk, one BatchWrite request");
        let options = seen[0]
            .request_options
            .as_ref()
            .expect("the BatchWrite must carry RequestOptions");
        assert_eq!(
            options.priority,
            v1::request_options::Priority::High as i32,
            "spanner.request.priority must reach the BatchWrite request"
        );
        assert_eq!(
            options.transaction_tag, "nightly-etl",
            "spanner.transaction.tag must tag the transactions the BatchWrite creates"
        );
        assert_eq!(
            options.request_tag, "",
            "spanner.request.tag must NOT be sent; Spanner ignores per-request tags on BatchWrite"
        );
        assert!(
            !seen[0].exclude_txn_from_change_streams,
            "with spanner.transaction.exclude_from_change_streams unset, the default (false) must \
             go out"
        );
    }

    connection
        .set_option(
            OptionConnection::Other(spanner_adbc::OPTION_EXCLUDE_TXN_FROM_CHANGE_STREAMS.into()),
            OptionValue::String("true".into()),
        )
        .expect("exclude the transaction from change streams");
    run_ingest(&mut connection);
    {
        let seen = requests.lock().unwrap();
        assert_eq!(seen.len(), 2, "the second ingest adds a second BatchWrite");
        assert!(
            seen[1].exclude_txn_from_change_streams,
            "spanner.transaction.exclude_from_change_streams=true must set \
             exclude_txn_from_change_streams on the BatchWrite request"
        );
    }
}

/// The change-stream exclusion reaches the transaction options of autocommit DML.
#[test]
fn exclude_from_change_streams_reaches_the_wire_on_runner_commits() {
    let _watchdog = watchdog!();
    const DML_SQL: &str = "INSERT INTO MockTable (c) VALUES ('x')";

    // Options from each begin, whether inline on `ExecuteBatchDml` or an explicit
    // `BeginTransaction`.
    let begins: Arc<Mutex<Vec<v1::TransactionOptions>>> = Arc::new(Mutex::new(Vec::new()));
    let record_explicit = begins.clone();
    let record_inline = begins.clone();
    let server = MockServer::start(move |mock| {
        mock.expect_begin_transaction().returning(move |request| {
            if let Some(options) = request.into_inner().options {
                record_explicit.lock().unwrap().push(options);
            }
            Ok(tonic::Response::new(v1::Transaction {
                id: b"dml-txn".to_vec(),
                ..Default::default()
            }))
        });
        mock.expect_execute_batch_dml().returning(move |request| {
            let request = request.into_inner();
            if let Some(v1::transaction_selector::Selector::Begin(options)) = request
                .transaction
                .as_ref()
                .and_then(|t| t.selector.as_ref())
            {
                record_inline.lock().unwrap().push(options.clone());
            }
            Ok(tonic::Response::new(v1::ExecuteBatchDmlResponse {
                result_sets: vec![v1::ResultSet {
                    metadata: Some(v1::ResultSetMetadata {
                        transaction: Some(v1::Transaction {
                            id: b"dml-txn".to_vec(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
                status: Some(spanner_grpc_mock::google::rpc::Status {
                    code: 0,
                    message: "OK".into(),
                    details: vec![],
                }),
                ..Default::default()
            }))
        });
        mock.expect_commit().returning(|_| commit_ok());
    });

    let mut connection = server.connect();

    let run_dml = |connection: &mut SpannerConnection| {
        let mut statement = connection.new_statement().expect("new statement");
        statement.set_sql_query(DML_SQL).unwrap();
        assert_eq!(
            statement.execute_update().expect("autocommit DML"),
            Some(1),
            "the scripted DML affects one row"
        );
    };

    run_dml(&mut connection);
    connection
        .set_option(
            OptionConnection::Other(spanner_adbc::OPTION_EXCLUDE_TXN_FROM_CHANGE_STREAMS.into()),
            OptionValue::String("true".into()),
        )
        .expect("exclude the transaction from change streams");
    run_dml(&mut connection);

    let seen = begins.lock().unwrap();
    assert_eq!(
        seen.len(),
        2,
        "each autocommit DML begins its read/write transaction inline on ExecuteBatchDml"
    );
    assert!(
        !seen[0].exclude_txn_from_change_streams,
        "with the option unset, the inline-begin options must carry the default (false)"
    );
    assert!(
        seen[1].exclude_txn_from_change_streams,
        "spanner.transaction.exclude_from_change_streams=true must set \
         exclude_txn_from_change_streams on the runner's inline-begin TransactionOptions"
    );
}

/// Priority and tags reach user queries, DML and commits. Metadata reads get the priority but no
/// tags: priority controls workload, while tags attribute user statements in Spanner's query and
/// transaction statistics. The emulator ignores `RequestOptions`, so only the mock covers this.
#[test]
fn request_priority_reaches_metadata_reads_but_tags_do_not() {
    let _watchdog = watchdog!();
    const USER_QUERY: &str = "SELECT c FROM MockTable";
    const DML_SQL: &str = "INSERT INTO MockTable (c) VALUES ('x')";

    let queries = Arc::new(Mutex::new(Vec::new()));
    let batch_dml: Arc<Mutex<Vec<v1::ExecuteBatchDmlRequest>>> = Arc::new(Mutex::new(Vec::new()));
    let commits: Arc<Mutex<Vec<v1::CommitRequest>>> = Arc::new(Mutex::new(Vec::new()));
    let (rec_q, rec_b, rec_c) = (queries.clone(), batch_dml.clone(), commits.clone());
    let server = MockServer::start(move |mock| {
        serve_begin_transaction(mock);
        serve_streaming_sql_begin_aware(mock, &["v"], Some(rec_q));
        serve_batch_dml(mock, rec_b.clone());
        mock.expect_commit().returning(move |request| {
            rec_c.lock().unwrap().push(request.into_inner());
            commit_ok()
        });
    });

    let mut connection = server.connect();
    for (key, value) in [
        (spanner_adbc::OPTION_REQUEST_PRIORITY, "high"),
        (spanner_adbc::OPTION_REQUEST_TAG, "user-req"),
        (spanner_adbc::OPTION_TRANSACTION_TAG, "user-txn"),
    ] {
        connection
            .set_option(
                OptionConnection::Other(key.into()),
                OptionValue::String(value.into()),
            )
            .unwrap_or_else(|_| panic!("set {key}"));
    }

    let mut query = connection.new_statement().expect("new statement");
    query.set_sql_query(USER_QUERY).unwrap();
    let reader = query.execute().expect("run the user query");
    for batch in reader {
        batch.expect("scripted query batch");
    }

    let mut dml = connection.new_statement().expect("new statement");
    dml.set_sql_query(DML_SQL).unwrap();
    assert_eq!(dml.execute_update().expect("autocommit DML"), Some(1));

    connection
        .get_table_schema(None, None, "MockTable")
        .expect("get_table_schema");

    let high = v1::request_options::Priority::High as i32;

    let queries = queries.lock().unwrap();
    let user = queries
        .iter()
        .find(|q| q.sql == USER_QUERY)
        .expect("the user query reached ExecuteStreamingSql");
    let user_options = user
        .request_options
        .as_ref()
        .expect("the user query must carry RequestOptions");
    assert_eq!(
        user_options.priority, high,
        "spanner.request.priority must reach the query's ExecuteStreamingSql RequestOptions"
    );
    assert_eq!(
        user_options.request_tag, "user-req",
        "spanner.request.tag must reach the query's ExecuteStreamingSql RequestOptions"
    );

    let metadata = queries
        .iter()
        .find(|q| q.sql != USER_QUERY)
        .expect("get_table_schema issued a metadata ExecuteStreamingSql");
    let metadata_options = metadata.request_options.clone().unwrap_or_default();
    assert_eq!(
        metadata_options.priority, high,
        "spanner.request.priority must reach a driver-internal metadata read"
    );
    assert_eq!(
        metadata_options.request_tag, "",
        "spanner.request.tag must NOT leak onto a driver-internal metadata read"
    );
    assert_eq!(
        metadata_options.transaction_tag, "",
        "spanner.transaction.tag must NOT leak onto a driver-internal metadata read"
    );

    let batch_dml = batch_dml.lock().unwrap();
    assert_eq!(
        batch_dml.len(),
        1,
        "the INSERT rides a single ExecuteBatchDml"
    );
    let dml_options = batch_dml[0]
        .request_options
        .as_ref()
        .expect("the ExecuteBatchDml must carry RequestOptions");
    assert_eq!(dml_options.priority, high);
    assert_eq!(dml_options.request_tag, "user-req");

    let commits = commits.lock().unwrap();
    assert_eq!(commits.len(), 1, "one autocommit DML, one Commit");
    let commit_options = commits[0]
        .request_options
        .as_ref()
        .expect("the Commit must carry RequestOptions");
    assert_eq!(
        commit_options.priority, high,
        "spanner.request.priority must reach the Commit as the commit priority"
    );
    assert_eq!(
        commit_options.transaction_tag, "user-txn",
        "spanner.transaction.tag must tag the read/write transaction's Commit"
    );
}

// ---------------------------------------------------------------------------
// Shared client stack
// ---------------------------------------------------------------------------

/// A server that counts `CreateSession` calls. The client creates one session per
/// `DatabaseClient`, so this counts client stacks.
fn start_counting_sessions(sessions: Arc<AtomicUsize>) -> MockServer {
    let mut mock = MockSpanner::new();
    mock.expect_create_session().returning(move |request| {
        sessions.fetch_add(1, Ordering::SeqCst);
        let database = request.into_inner().database;
        Ok(tonic::Response::new(v1::Session {
            name: format!(
                "{database}/sessions/mock-session-{}",
                sessions.load(Ordering::SeqCst)
            ),
            multiplexed: true,
            ..Default::default()
        }))
    });
    MockServer::serve(mock)
}

/// The client stack (channel pool, credentials, session, maintenance task) is expensive, so a
/// database builds it lazily and shares it between connections. Setting any database option
/// discards it.
#[test]
fn connections_share_one_client_stack_until_an_option_is_set() {
    let _watchdog = watchdog!();
    let sessions = Arc::new(AtomicUsize::new(0));
    let server = start_counting_sessions(sessions.clone());
    let mut database = emulator_database(&server.endpoint);

    assert_eq!(
        sessions.load(Ordering::SeqCst),
        0,
        "no client stack may be built before the first connection"
    );

    let _first = database.new_connection().expect("first connection");
    let _second = database.new_connection().expect("second connection");
    assert_eq!(
        sessions.load(Ordering::SeqCst),
        1,
        "two connections on one database must share one client stack (one CreateSession)"
    );

    let rendered = format!("{database:?}");
    assert!(
        rendered.contains(r#"connected: Some("<client stack>")"#),
        "the cached stack must render presence-only: {rendered}"
    );
    assert!(
        !rendered.contains("DatabaseClient"),
        "Debug must not delegate to the client's own Debug: {rendered}"
    );

    // Even re-setting the same value invalidates the cache.
    database
        .set_option(
            OptionDatabase::Other(spanner_adbc::OPTION_ENDPOINT.into()),
            OptionValue::String(server.endpoint.clone()),
        )
        .expect("re-set endpoint");
    let _third = database.new_connection().expect("third connection");
    assert_eq!(
        sessions.load(Ordering::SeqCst),
        2,
        "set_option must invalidate the cached stack (second CreateSession on the next connection)"
    );
}

/// Generic clients set every option to its default, so `adbc.statement.exec.incremental=false` is
/// accepted. Enabling it is not implemented.
#[test]
fn exec_incremental_spec_default_is_a_no_op() {
    let _watchdog = watchdog!();
    let server = MockServer::start(|_| {});
    let mut connection = server.connect();
    let mut statement = connection.new_statement().expect("new statement");

    statement
        .set_option(
            OptionStatement::Incremental,
            OptionValue::String("false".into()),
        )
        .expect("setting adbc.statement.exec.incremental=false is a no-op");

    assert_eq!(
        statement
            .get_option_string(OptionStatement::Incremental)
            .expect("incremental getter must report the default"),
        "false"
    );

    let error = statement
        .set_option(
            OptionStatement::Incremental,
            OptionValue::String("true".into()),
        )
        .expect_err("adbc.statement.exec.incremental=true must be rejected");
    assert_eq!(error.status, AdbcStatus::NotImplemented);
    assert!(
        error.message.contains("adbc.statement.exec.incremental"),
        "{error}"
    );
}

// ---------------------------------------------------------------------------
// Retry-limit accounting per RPC path
// ---------------------------------------------------------------------------

// The pinned client retries unary RPCs in gax's retry loop, where both limits are exact. Stream
// resumption builds a fresh `RetryState` per resume, so the attempt limit is exact but the
// elapsed-time limit never fires. These tests catch client updates that change either; see
// `src/retry.rs`.

/// Attempts a probe serves before failing permanently, so an inert limit cannot hang the test.
const RETRY_PROBE_CAP: usize = 20;

/// A constant 50 ms backoff instead of the default 1 s doubling. With full jitter, the probe cap
/// takes about half a second to reach, well beyond the elapsed budgets tested below.
fn set_constant_backoff(statement: &mut impl Statement) {
    for (key, value) in [
        (spanner_adbc::OPTION_RETRY_BACKOFF_INITIAL_SECONDS, 0.05),
        (spanner_adbc::OPTION_RETRY_BACKOFF_MULTIPLIER, 1.0),
    ] {
        statement
            .set_option(
                OptionStatement::Other(key.into()),
                OptionValue::Double(value),
            )
            .expect("set backoff knob");
    }
}

/// Counts `ExecuteStreamingSql` attempts with retry option `key` set to `value`. The fault arrives
/// mid-stream because the client never retries a failed initial streaming RPC.
fn streaming_attempts(key: &str, value: OptionValue) -> usize {
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_in_mock = calls.clone();
    let server = MockServer::start(move |mock| {
        mock.expect_execute_streaming_sql().returning(move |_| {
            let n = calls_in_mock.fetch_add(1, Ordering::SeqCst);
            if n >= RETRY_PROBE_CAP {
                return Err(tonic::Status::internal("mock server: probe cap reached"));
            }
            // A resumed stream does not repeat the metadata.
            Ok(stream_of(vec![
                Ok(partial_result_set(n == 0, &["v1"], b"rt-1", false)),
                Err(tonic::Status::unavailable(
                    "mock server: connection lost mid-stream",
                )),
            ]))
        });
    });

    let mut connection = server.connect();
    let mut statement = connection.new_statement().expect("new statement");
    statement
        .set_option(OptionStatement::Other(key.into()), value)
        .expect("set retry option");
    set_constant_backoff(&mut statement);
    statement.set_sql_query("SELECT c FROM MockTable").unwrap();
    statement
        .execute()
        .err()
        .expect("every attempt fails, so the query must fail");
    calls.load(Ordering::SeqCst)
}

/// Counts `ExecuteBatchDml` attempts with retry option `key` set to `value`.
fn unary_attempts(key: &str, value: OptionValue) -> usize {
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_in_mock = calls.clone();
    let server = MockServer::start(move |mock| {
        mock.expect_execute_batch_dml().returning(move |_| {
            let n = calls_in_mock.fetch_add(1, Ordering::SeqCst);
            if n >= RETRY_PROBE_CAP {
                return Err(tonic::Status::internal("mock server: probe cap reached"));
            }
            Err(tonic::Status::unavailable(
                "mock server: backend unavailable",
            ))
        });
    });

    let mut connection = server.connect();
    let mut statement = connection.new_statement().expect("new statement");
    statement
        .set_option(OptionStatement::Other(key.into()), value)
        .expect("set retry option");
    set_constant_backoff(&mut statement);
    statement
        .set_sql_query("UPDATE MockTable SET c = 'x' WHERE TRUE")
        .unwrap();
    statement
        .execute_update()
        .expect_err("every attempt fails, so the DML must fail");
    calls.load(Ordering::SeqCst)
}

#[test]
fn retry_max_attempts_is_exact_on_unary_rpcs() {
    let _watchdog = watchdog!();
    for max_attempts in [1_i64, 2, 3] {
        let attempts = unary_attempts(
            spanner_adbc::OPTION_RETRY_MAX_ATTEMPTS,
            OptionValue::Int(max_attempts),
        );
        assert_eq!(
            attempts, max_attempts as usize,
            "max_attempts={max_attempts} must permit {max_attempts} ExecuteBatchDml attempts"
        );
    }
}

#[test]
fn retry_max_attempts_is_exact_on_the_streaming_path() {
    let _watchdog = watchdog!();
    for max_attempts in [1_i64, 2, 3] {
        let attempts = streaming_attempts(
            spanner_adbc::OPTION_RETRY_MAX_ATTEMPTS,
            OptionValue::Int(max_attempts),
        );
        assert_eq!(
            attempts, max_attempts as usize,
            "max_attempts={max_attempts} must permit exactly {max_attempts} ExecuteStreamingSql \
             attempts; a change here means the pinned client's stream-resume accounting moved, \
             so update src/retry.rs and docs/options.md to match"
        );
    }
}

/// `spanner.retry.max_elapsed_seconds` bounds unary retries but not stream resumption. Streaming
/// callers can use `spanner.rpc.timeout_seconds.{query,fetch}` instead.
#[test]
fn retry_max_elapsed_seconds_bounds_unary_rpcs_but_is_inert_on_the_streaming_path() {
    let _watchdog = watchdog!(Duration::from_secs(240));

    let unbounded = unary_attempts(
        spanner_adbc::OPTION_RETRY_MAX_ELAPSED_SECONDS,
        OptionValue::String(String::new()),
    );
    assert_eq!(
        unbounded,
        RETRY_PROBE_CAP + 1,
        "with no elapsed budget the unary retry loop must run until the mock's cap ends it"
    );

    // With full jitter the count varies. At least two attempts shows the budget is not so small
    // that it stops at the first failure.
    let unary = unary_attempts(
        spanner_adbc::OPTION_RETRY_MAX_ELAPSED_SECONDS,
        OptionValue::Double(0.15),
    );
    assert!(
        (2..unbounded).contains(&unary),
        "a 150ms elapsed budget must bound the unary retry loop without collapsing it to a single \
         attempt, but it ran {unary} (unbounded: {unbounded})"
    );

    let streaming = streaming_attempts(
        spanner_adbc::OPTION_RETRY_MAX_ELAPSED_SECONDS,
        OptionValue::Double(0.05),
    );
    assert_eq!(
        streaming,
        RETRY_PROBE_CAP + 1,
        "a 50ms elapsed budget currently never exhausts the streaming retry loop, so the \
         mock's own cap is what stops it; a change here means the pinned client's stream-resume \
         accounting moved, so update src/retry.rs and docs/options.md to match"
    );
}

/// `get_statistics` runs discovery and every aggregate scan in one read-only transaction, so a
/// concurrently created table cannot fail the call and all counts share a timestamp.
#[test]
fn get_statistics_shares_one_read_only_transaction() {
    let _watchdog = watchdog!();
    let selectors: Arc<Mutex<Vec<Option<v1::TransactionSelector>>>> =
        Arc::new(Mutex::new(Vec::new()));
    let server = {
        let record = selectors.clone();
        MockServer::start(move |mock| {
            let record = record.clone();
            mock.expect_execute_streaming_sql()
                .returning(move |request| {
                    let request = request.into_inner();
                    let inline_begin = begins_inline(request.transaction.as_ref());
                    record.lock().unwrap().push(request.transaction);
                    // Columns are read by position, so only their count and types matter.
                    let (fields, values): (Vec<(&str, v1::TypeCode)>, Vec<&str>) =
                        if request.sql.contains("INFORMATION_SCHEMA") {
                            (
                                vec![
                                    ("TABLE_SCHEMA", v1::TypeCode::String),
                                    ("TABLE_NAME", v1::TypeCode::String),
                                    ("COLUMN_NAME", v1::TypeCode::String),
                                    ("SPANNER_TYPE", v1::TypeCode::String),
                                ],
                                vec!["", "MockTable", "c", "STRING(MAX)"],
                            )
                        } else {
                            // COUNT(*), COUNTIF(c IS NULL), COUNT(DISTINCT c)
                            (
                                vec![
                                    ("n", v1::TypeCode::Int64),
                                    ("nulls", v1::TypeCode::Int64),
                                    ("distinct", v1::TypeCode::Int64),
                                ],
                                vec!["3", "0", "3"],
                            )
                        };
                    let mut metadata = result_metadata(&fields);
                    if inline_begin {
                        metadata.transaction = Some(v1::Transaction {
                            id: b"stats-ro-txn".to_vec(),
                            ..Default::default()
                        });
                    }
                    let result = v1::PartialResultSet {
                        metadata: Some(metadata),
                        values: values.iter().map(|s| string_value(s)).collect(),
                        resume_token: b"stat-1".to_vec(),
                        last: true,
                        ..Default::default()
                    };
                    Ok(stream_of(vec![Ok(result)]))
                });
        })
    };

    let connection = server.connect();
    let reader = connection
        .get_statistics(None, None, None, false)
        .expect("get_statistics");
    let batches: Vec<_> = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("collect statistics");
    assert_eq!(batches.len(), 1, "one catalog batch");

    let seen = selectors.lock().unwrap();
    assert!(
        seen.len() >= 2,
        "discovery plus at least one aggregate scan, got {}",
        seen.len()
    );
    let first = seen[0]
        .as_ref()
        .and_then(|s| s.selector.as_ref())
        .expect("the first query must carry a transaction selector");
    let v1::transaction_selector::Selector::Begin(options) = first else {
        panic!("the first get_statistics query must begin the transaction, got: {first:?}");
    };
    assert!(
        matches!(
            options.mode,
            Some(v1::transaction_options::Mode::ReadOnly(_))
        ),
        "the begun transaction must be read-only: {options:?}"
    );
    for (i, selector) in seen.iter().enumerate().skip(1) {
        let sel = selector
            .as_ref()
            .and_then(|s| s.selector.as_ref())
            .unwrap_or_else(|| panic!("query {i} must carry a transaction selector"));
        assert_eq!(
            sel,
            &v1::transaction_selector::Selector::Id(b"stats-ro-txn".to_vec()),
            "query {i} must reuse the begun read-only transaction, not open its own"
        );
    }
}

/// The catalog is the database id, and catalog arguments accept it. `get_objects` at `Catalogs`
/// depth issues no RPC.
#[test]
fn the_catalog_level_reports_the_database_id_and_filters_round_trip() {
    use adbc_core::options::ObjectDepth;

    let _watchdog = watchdog!();
    let server = MockServer::start(|_mock| {});
    let mut connection = server.connect();

    let catalogs = |connection: &SpannerConnection, filter: Option<&str>| -> Vec<String> {
        let reader = connection
            .get_objects(ObjectDepth::Catalogs, filter, None, None, None, None)
            .expect("get_objects at Catalogs depth");
        reader
            .collect::<Result<Vec<_>, _>>()
            .expect("drain get_objects")
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_string::<i32>()
                    .iter()
                    .map(|name| name.expect("catalog_name is never null").to_owned())
                    .collect::<Vec<_>>()
            })
            .collect()
    };

    assert_eq!(catalogs(&connection, None), ["mock-db"]);
    assert_eq!(catalogs(&connection, Some("mock-db")), ["mock-db"]);
    assert_eq!(catalogs(&connection, Some("mock%")), ["mock-db"]);
    assert!(catalogs(&connection, Some("other-db")).is_empty());
    // "" selects objects without a catalog.
    assert!(catalogs(&connection, Some("")).is_empty());

    assert_eq!(
        connection
            .get_option_string(OptionConnection::CurrentCatalog)
            .expect("read the current catalog"),
        "mock-db"
    );
    connection
        .set_option(
            OptionConnection::CurrentCatalog,
            OptionValue::String("mock-db".into()),
        )
        .expect("setting the current catalog to its own value is a no-op");
    let rejected = connection
        .set_option(
            OptionConnection::CurrentCatalog,
            OptionValue::String("other-db".into()),
        )
        .expect_err("no other catalog is reachable");
    assert_eq!(rejected.status, AdbcStatus::NotImplemented);

    let mut statement = connection.new_statement().expect("new statement");
    statement
        .set_option(
            OptionStatement::TargetCatalog,
            OptionValue::String("mock-db".into()),
        )
        .expect("ingest into the connection's own catalog");
    let rejected = statement
        .set_option(
            OptionStatement::TargetCatalog,
            OptionValue::String(String::new()),
        )
        .expect_err("\"\" is no longer a catalog this connection has");
    assert_eq!(rejected.status, AdbcStatus::NotImplemented);
}

#[test]
fn ingest_operational_errors_keep_status_and_details_without_metadata_probes() {
    let _watchdog = watchdog!();
    for (code, expected) in [
        (tonic::Code::PermissionDenied, AdbcStatus::Unauthorized),
        (tonic::Code::Unauthenticated, AdbcStatus::Unauthenticated),
        (tonic::Code::Cancelled, AdbcStatus::Cancelled),
        (tonic::Code::DeadlineExceeded, AdbcStatus::Timeout),
        (tonic::Code::ResourceExhausted, AdbcStatus::IO),
        (tonic::Code::Unavailable, AdbcStatus::IO),
        (tonic::Code::Aborted, AdbcStatus::IO),
        (tonic::Code::Internal, AdbcStatus::Internal),
    ] {
        let probes = Arc::new(AtomicUsize::new(0));
        let record_probes = probes.clone();
        let server = MockServer::start(move |mock| {
            mock.expect_batch_write().returning(move |_| {
                let mut response = batch_write_group_already_exists(0);
                let status = response.status.as_mut().unwrap();
                status.code = code as i32;
                status.message = "injected operational failure".into();
                let (tx, rx) = tokio::sync::mpsc::channel(1);
                tx.try_send(Ok(response)).unwrap();
                Ok(tonic::Response::new(rx))
            });
            // An existence probe must not replace an operational error with AlreadyExists
            // or discard its vendor code and structured details.
            mock.expect_execute_streaming_sql().returning(move |_| {
                record_probes.fetch_add(1, Ordering::SeqCst);
                Ok(stream_of(vec![Ok(partial_result_set(
                    true,
                    &["MockTable"],
                    b"",
                    true,
                ))]))
            });
        });
        let mut connection = server.connect();
        let mut statement = batch_write_ingest_statement(&mut connection);
        statement.bind(ingest_batch(1)).unwrap();
        let error = statement.execute_update().unwrap_err();
        assert_eq!(error.status, expected, "{code:?}: {error}");
        assert_eq!(error.vendor_code, code as i32, "{error}");
        assert!(error.message.contains("injected operational failure"));
        assert!(error.details.as_ref().is_some_and(|details| {
            details.iter().any(|(key, _)| key == "google.rpc.errorinfo")
        }));
        assert_eq!(probes.load(Ordering::SeqCst), 0, "{code:?}");
    }
}

#[test]
fn cancel_before_first_batch_discards_rows_fetched_by_execute() {
    let _watchdog = watchdog!();
    let server = MockServer::start(|mock| {
        mock.expect_execute_streaming_sql().returning(|_| {
            Ok(stream_of(vec![Ok(partial_result_set(
                true,
                &["first", "second"],
                b"rt",
                true,
            ))]))
        });
    });
    let mut connection = server.connect();
    let mut statement = connection.new_statement().unwrap();
    statement.set_sql_query("SELECT c FROM MockTable").unwrap();
    let mut reader = statement.execute().unwrap();
    statement.get_cancel_handle().try_cancel().unwrap();

    let error = reader
        .next()
        .expect("cancel must yield an error")
        .unwrap_err();
    assert_eq!(driver_error(&error).status, AdbcStatus::Cancelled);
    assert!(reader.next().is_none());
}

#[test]
fn conversion_error_ends_the_reader_in_first_and_later_batches() {
    let _watchdog = watchdog!();
    for bad_row in 0..2 {
        let server = MockServer::start(move |mock| {
            mock.expect_execute_streaming_sql().returning(move |_| {
                let mut values = vec![string_value("1"), string_value("2"), string_value("3")];
                values[bad_row] = string_value("invalid integer");
                Ok(stream_of(vec![Ok(v1::PartialResultSet {
                    metadata: Some(result_metadata(&[("c", v1::TypeCode::Int64)])),
                    values,
                    last: true,
                    ..Default::default()
                })]))
            });
        });
        let mut connection = server.connect();
        let mut statement = connection.new_statement().unwrap();
        statement
            .set_option(
                OptionStatement::Other(spanner_adbc::OPTION_ROWS_PER_BATCH.into()),
                OptionValue::Int(1),
            )
            .unwrap();
        statement.set_sql_query("SELECT c FROM MockTable").unwrap();
        let mut reader = statement.execute().unwrap();
        for _ in 0..bad_row {
            assert_eq!(reader.next().unwrap().unwrap().num_rows(), 1);
        }
        let error = reader
            .next()
            .expect("invalid row must yield an error")
            .unwrap_err();
        assert!(error.to_string().contains("INT64"), "{error}");
        assert!(
            reader.next().is_none(),
            "rows after a conversion error must not be returned"
        );
    }
}

#[test]
fn interrupted_batch_write_reports_confirmed_rows_and_unknown_outcome() {
    let _watchdog = watchdog!();
    for cancel_operation in [false, true] {
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let server = MockServer::start(move |mock| {
            mock.expect_batch_write().returning(move |_| {
                let (tx, rx) = tokio::sync::mpsc::channel(1);
                tx.try_send(Ok(batch_write_groups_ok(vec![0, 1]))).unwrap();
                let _ = started_tx.send(());
                // Keep the stream open after two confirmed writes; group 2 has no outcome yet.
                tokio::spawn(async move { tx.closed().await });
                Ok(tonic::Response::new(rx))
            });
        });
        let mut connection = server.connect();
        let mut statement = batch_write_ingest_statement(&mut connection);
        // The cancel case gets a deadline it cannot reach, so a slow cancel cannot turn into Timeout.
        statement
            .set_option(
                OptionStatement::Other(spanner_adbc::OPTION_RPC_TIMEOUT_UPDATE.into()),
                OptionValue::Double(if cancel_operation { 60.0 } else { 1.0 }),
            )
            .unwrap();
        statement.bind(ingest_batch(3)).unwrap();
        let cancel = statement.get_cancel_handle();
        let canceller = std::thread::spawn(move || {
            started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            if cancel_operation {
                // Allow the first response to reach the driver before interrupting its next read.
                std::thread::sleep(Duration::from_millis(200));
                cancel.try_cancel().unwrap();
            }
        });
        let error = statement.execute_update().unwrap_err();
        canceller.join().unwrap();
        assert_eq!(
            error.status,
            if cancel_operation {
                AdbcStatus::Cancelled
            } else {
                AdbcStatus::Timeout
            },
            "{error}"
        );
        assert!(
            error
                .message
                .contains("2 row(s) from this bulk ingest were already committed"),
            "{error}"
        );
        assert!(error.message.contains("outcome is unknown"), "{error}");
    }
}

#[test]
fn ingest_table_probe_inherits_read_options_without_tags() {
    let _watchdog = watchdog!();
    let queries = Arc::new(Mutex::new(Vec::new()));
    let recorded = queries.clone();
    let server = MockServer::start(move |mock| {
        serve_streaming_sql_begin_aware(mock, &["MockTable"], Some(recorded));
    });
    let mut connection = server.connect();
    for (key, value) in [
        (spanner_adbc::OPTION_REQUEST_PRIORITY, "high"),
        (spanner_adbc::OPTION_REQUEST_TAG, "user-request"),
        (spanner_adbc::OPTION_TRANSACTION_TAG, "user-transaction"),
        (spanner_adbc::OPTION_DIRECTED_READ, "include:us-east1"),
    ] {
        connection
            .set_option(
                OptionConnection::Other(key.into()),
                OptionValue::String(value.into()),
            )
            .unwrap();
    }
    let mut statement = append_ingest_statement(&mut connection);
    statement.bind(ingest_batch(0)).unwrap();
    assert_eq!(statement.execute_update().unwrap(), Some(0));
    let queries = queries.lock().unwrap();
    assert_eq!(queries.len(), 1);
    let query = &queries[0];
    assert!(query.sql.contains("INFORMATION_SCHEMA.TABLES"));
    let options = query.request_options.as_ref().unwrap();
    assert_eq!(options.priority, v1::request_options::Priority::High as i32);
    assert!(options.request_tag.is_empty());
    assert!(options.transaction_tag.is_empty());
    let Some(v1::directed_read_options::Replicas::IncludeReplicas(replicas)) = query
        .directed_read_options
        .as_ref()
        .and_then(|options| options.replicas.as_ref())
    else {
        panic!("missing directed-read options")
    };
    assert_eq!(replicas.replica_selections[0].location, "us-east1");
}

#[test]
fn ingest_table_probe_honors_retry_limit() {
    let _watchdog = watchdog!();
    let attempts = Arc::new(AtomicUsize::new(0));
    let recorded = attempts.clone();
    let server = MockServer::start(move |mock| {
        mock.expect_execute_streaming_sql().returning(move |_| {
            if recorded.fetch_add(1, Ordering::SeqCst) == 0 {
                Err(tonic::Status::unavailable("probe unavailable"))
            } else {
                Ok(stream_of(vec![Ok(partial_result_set(
                    true,
                    &["MockTable"],
                    b"t",
                    true,
                ))]))
            }
        });
    });
    let mut connection = server.connect();
    connection
        .set_option(
            OptionConnection::Other(spanner_adbc::OPTION_RETRY_MAX_ATTEMPTS.into()),
            OptionValue::Int(1),
        )
        .unwrap();
    let mut statement = append_ingest_statement(&mut connection);
    statement.bind(ingest_batch(0)).unwrap();
    // A failed existence probe preserves the empty ingest's success, but must not retry.
    assert_eq!(statement.execute_update().unwrap(), Some(0));
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

#[test]
fn batch_write_honors_retry_attempt_limit() {
    let _watchdog = watchdog!();
    for limit in [1, 3] {
        let attempts = Arc::new(AtomicUsize::new(0));
        let recorded = attempts.clone();
        let server = MockServer::start(move |mock| {
            mock.expect_batch_write().returning(move |_| {
                recorded.fetch_add(1, Ordering::SeqCst);
                Err(tonic::Status::unavailable("batch write unavailable"))
            });
        });
        let mut connection = server.connect();
        let mut statement = batch_write_ingest_statement(&mut connection);
        for (key, value) in [
            (
                spanner_adbc::OPTION_RETRY_MAX_ATTEMPTS,
                OptionValue::Int(limit as i64),
            ),
            (
                spanner_adbc::OPTION_RETRY_BACKOFF_INITIAL_SECONDS,
                OptionValue::Double(0.001),
            ),
            (
                spanner_adbc::OPTION_RPC_TIMEOUT_UPDATE,
                OptionValue::Double(2.0),
            ),
        ] {
            statement
                .set_option(OptionStatement::Other(key.into()), value)
                .unwrap();
        }
        statement.bind(ingest_batch(1)).unwrap();
        let error = statement.execute_update().unwrap_err();
        assert_eq!(error.status, AdbcStatus::IO, "{error}");
        assert_eq!(attempts.load(Ordering::SeqCst), limit);
    }
}

#[test]
fn query_consumes_empty_stream_ingest_schema() {
    let _watchdog = watchdog!();
    let server = MockServer::start(|mock| {
        mock.expect_execute_streaming_sql().returning(|_| {
            Ok(stream_of(vec![Ok(partial_result_set(
                true,
                &["row"],
                b"",
                true,
            ))]))
        });
    });
    let mut connection = server.connect();
    for discard in [false, true] {
        let mut statement = connection.new_statement().unwrap();
        statement
            .bind_stream(Box::new(arrow_array::RecordBatchIterator::new(
                std::iter::empty::<Result<RecordBatch, ArrowError>>(),
                ingest_batch(0).schema(),
            )))
            .unwrap();
        statement.set_sql_query("SELECT c FROM MockTable").unwrap();
        if discard {
            assert_eq!(statement.execute_update().unwrap(), None);
        } else {
            let batches = statement
                .execute()
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        }
        statement
            .set_option(
                OptionStatement::TargetTable,
                OptionValue::String("MockTable".into()),
            )
            .unwrap();
        statement
            .set_option(
                OptionStatement::IngestMode,
                OptionValue::String("append".into()),
            )
            .unwrap();
        let error = statement.execute_update().unwrap_err();
        assert_eq!(error.status, AdbcStatus::InvalidState, "{error}");
        assert!(error.message.contains("no data has been bound"), "{error}");
    }
}

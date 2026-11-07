//! Fault injection against an in-process mock Spanner gRPC server. Needs no backend.
//!
//! The server is `spanner-grpc-mock`, the mockall/tonic mock the pinned `google-cloud-spanner`
//! client uses for its own tests, on the same revision so the protos match. The driver reaches it
//! as it would an emulator (`spanner.endpoint` plus `spanner.emulator=true`). Only the data-plane
//! `google.spanner.v1.Spanner` service is scripted; [`GatedEndpoint`] makes the admin channel
//! silent for DDL deadline and cancellation tests.
//!
//! mockall matches expectations in FIFO order. [`MockServer::serve`] adds `CreateSession` and an
//! `UNIMPLEMENTED` answer to every other RPC after the test's script: a mockall panic in the server
//! task would reach the client as a retryable transport error and hang. Call counts such as
//! `.times(n)` are only upper bounds: the mock is dropped with the aborted server task, which
//! swallows unmet expectations, so tests count calls themselves.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use adbc_core::error::{Error, Result as AdbcResult, Status as AdbcStatus};
use adbc_core::options::OptionValue;
use adbc_core::options::{ObjectDepth, OptionConnection, OptionDatabase, OptionStatement};
use adbc_core::{Connection, Database, Driver, Optionable, Statement};
use arrow_array::cast::AsArray;
use arrow_array::{
    ArrayRef, Date32Array, Int64Array, RecordBatch, RecordBatchIterator, RecordBatchReader,
    StringArray,
};
use arrow_schema::{ArrowError, DataType, Field, Schema, TimeUnit};
use prost::Message;
use spanner_adbc::*;
use spanner_grpc_mock::MockSpanner;
use spanner_grpc_mock::google::rpc;
use spanner_grpc_mock::google::spanner::v1;
use v1::transaction_options::read_only::TimestampBound as WireBound;
use v1::transaction_selector::Selector;

#[path = "../src/mock_server.rs"]
mod mock_server;
use mock_server::{DATABASE_URI, MockServer, emulator_database};

const SELECT: &str = "SELECT c FROM MockTable";
const INSERT: &str = "INSERT INTO MockTable (c) VALUES ('x')";
const UPDATE: &str = "UPDATE MockTable SET c = 'x' WHERE TRUE";

/// The requests a scripted RPC received, in order.
type Log<T> = Arc<Mutex<Vec<T>>>;

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
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
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
                    (client.try_clone().unwrap(), server.try_clone().unwrap()),
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
        emulator_database(&self.endpoint).new_connection().unwrap()
    }
}

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
fn stream_of<T>(
    items: Vec<tonic::Result<T>>,
) -> tonic::Response<tokio::sync::mpsc::Receiver<tonic::Result<T>>> {
    let (tx, rx) = tokio::sync::mpsc::channel(items.len().max(1));
    for item in items {
        tx.try_send(item).unwrap();
    }
    tonic::Response::new(rx)
}

/// The driver error that a reader wraps in `ArrowError::ExternalError`.
fn driver_error(error: &ArrowError) -> &Error {
    let ArrowError::ExternalError(source) = error else {
        panic!("expected the reader to surface the driver error, got: {error}");
    };
    source.downcast_ref::<Error>().unwrap()
}

/// Returns transaction `id` in the result metadata, as Spanner does for an inline begin.
fn set_transaction_id(result: &mut v1::PartialResultSet, id: impl Into<Vec<u8>>) {
    result.metadata.as_mut().unwrap().transaction = Some(v1::Transaction {
        id: id.into(),
        ..Default::default()
    });
}

fn begins_inline(transaction: Option<&v1::TransactionSelector>) -> bool {
    matches!(
        transaction.and_then(|t| t.selector.as_ref()),
        Some(Selector::Begin(_))
    )
}

/// Serves `ExecuteStreamingSql` with a single-message STRING result. A statement in a manual
/// transaction begins its transaction inline, and the client requires the new transaction id in
/// the first response, so requests with a `begin` selector get `inline-txn` back.
fn serve_queries(
    mock: &mut MockSpanner,
    values: &'static [&'static str],
) -> Log<v1::ExecuteSqlRequest> {
    let log = Log::default();
    let requests = log.clone();
    mock.expect_execute_streaming_sql()
        .returning(move |request| {
            let request = request.into_inner();
            let mut first = partial_result_set(true, values, b"r-1", true);
            if begins_inline(request.transaction.as_ref()) {
                set_transaction_id(&mut first, b"inline-txn");
            }
            requests.lock().unwrap().push(request);
            Ok(stream_of(vec![Ok(first)]))
        });
    log
}

/// A server that only answers queries, as [`serve_queries`] does.
fn query_server(values: &'static [&'static str]) -> (MockServer, Log<v1::ExecuteSqlRequest>) {
    let mut mock = MockSpanner::new();
    let requests = serve_queries(&mut mock, values);
    (MockServer::serve(mock), requests)
}

/// Answers every query with one message carrying exactly `metadata` and `values`.
fn serve_result(
    mock: &mut MockSpanner,
    metadata: v1::ResultSetMetadata,
    values: Vec<prost_types::Value>,
) {
    mock.expect_execute_streaming_sql().returning(move |_| {
        Ok(stream_of(vec![Ok(v1::PartialResultSet {
            metadata: Some(metadata.clone()),
            values: values.clone(),
            last: true,
            ..Default::default()
        })]))
    });
}

/// The transaction selector of each logged request.
fn selectors(requests: &Log<v1::ExecuteSqlRequest>) -> Vec<Selector> {
    let requests = requests.lock().unwrap();
    requests
        .iter()
        .map(|r| r.transaction.clone().and_then(|t| t.selector).unwrap())
        .collect()
}

/// Asserts that the first selector begins a read-only transaction and the others reuse `id`, and
/// returns the options it began with.
fn assert_shared_read_only<'a>(selectors: &'a [Selector], id: &[u8]) -> &'a v1::TransactionOptions {
    let Selector::Begin(options) = &selectors[0] else {
        panic!("the first request must begin the transaction: {selectors:?}");
    };
    assert!(
        matches!(
            options.mode,
            Some(v1::transaction_options::Mode::ReadOnly(_))
        ),
        "{options:?}"
    );
    for selector in &selectors[1..] {
        assert_eq!(selector, &Selector::Id(id.to_vec()));
    }
    options
}

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
    let status_proto = rpc::Status {
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

/// The ProtoJSON detail stored under `key`.
fn error_detail(error: &Error, key: &str) -> serde_json::Value {
    let details = error.details.as_ref().expect("the error carries details");
    let (_, value) = details
        .iter()
        .find(|(k, _)| k == key)
        .unwrap_or_else(|| panic!("no {key} in {details:?}"));
    serde_json::from_slice(value).unwrap()
}

fn prepare(connection: &mut SpannerConnection, sql: &str) -> SpannerStatement {
    let mut statement = connection.new_statement().unwrap();
    statement.set_sql_query(sql).unwrap();
    statement
}

fn update(connection: &mut SpannerConnection, sql: &str) -> Option<i64> {
    prepare(connection, sql).execute_update().unwrap()
}

fn fetch(statement: &mut SpannerStatement) -> Vec<RecordBatch> {
    let reader = statement.execute().unwrap();
    reader.collect::<Result<_, _>>().unwrap()
}

fn query(connection: &mut SpannerConnection, sql: &str) -> Vec<RecordBatch> {
    fetch(&mut prepare(connection, sql))
}

fn row_count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

fn configure<O: Optionable>(target: &mut O, options: &[(&str, &str)])
where
    O::Option: for<'a> From<&'a str>,
{
    for (key, value) in options {
        target.set_option((*key).into(), (*value).into()).unwrap();
    }
}

fn disable_autocommit(connection: &mut SpannerConnection) {
    connection
        .set_option(OptionConnection::AutoCommit, false.into())
        .unwrap();
}

/// A batch of `n` rows in one STRING column `c`; each row is one insert mutation.
fn ingest_batch(n: usize) -> RecordBatch {
    let column = StringArray::from((0..n).map(|i| format!("v{i}")).collect::<Vec<_>>());
    RecordBatch::try_from_iter([("c", Arc::new(column) as ArrayRef)]).unwrap()
}

fn target_mock_table(statement: &mut SpannerStatement) {
    configure(
        statement,
        &[
            ("adbc.ingest.target_table", "MockTable"),
            ("adbc.ingest.mode", "append"),
        ],
    );
}

/// A statement ingesting into `MockTable` in `append` mode, which needs no admin RPCs.
fn append_ingest_statement(connection: &mut SpannerConnection) -> SpannerStatement {
    let mut statement = connection.new_statement().unwrap();
    target_mock_table(&mut statement);
    statement
}

/// An `append` ingest through `BatchWrite` instead of a commit.
fn batch_write_ingest_statement(connection: &mut SpannerConnection) -> SpannerStatement {
    let mut statement = append_ingest_statement(connection);
    configure(&mut statement, &[(OPTION_INGEST_BATCH_WRITE, "true")]);
    statement
}

fn ingest(mut statement: SpannerStatement, rows: usize) -> AdbcResult<Option<i64>> {
    statement.bind(ingest_batch(rows)).unwrap();
    statement.execute_update()
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

fn serve_commits_with(
    mock: &mut MockSpanner,
    respond: impl Fn(&v1::CommitRequest) -> tonic::Result<tonic::Response<v1::CommitResponse>>
    + Send
    + 'static,
) -> Log<v1::CommitRequest> {
    let log = Log::default();
    let requests = log.clone();
    mock.expect_commit().returning(move |request| {
        let request = request.into_inner();
        let response = respond(&request);
        requests.lock().unwrap().push(request);
        response
    });
    log
}

fn serve_commits(mock: &mut MockSpanner) -> Log<v1::CommitRequest> {
    serve_commits_with(mock, |_| commit_ok())
}

/// Answers every `BatchWrite` with `responses`.
fn serve_batch_write(
    mock: &mut MockSpanner,
    responses: Vec<v1::BatchWriteResponse>,
) -> Log<v1::BatchWriteRequest> {
    let log = Log::default();
    let requests = log.clone();
    mock.expect_batch_write().returning(move |request| {
        requests.lock().unwrap().push(request.into_inner());
        Ok(stream_of(responses.iter().cloned().map(Ok).collect()))
    });
    log
}

fn ok_status() -> Option<rpc::Status> {
    Some(rpc::Status {
        code: 0,
        message: "OK".into(),
        details: vec![],
    })
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
        status: Some(rpc::Status {
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

/// Serves `ExecuteBatchDml`, reporting one affected row per statement. A batch that begins its
/// transaction inline gets the transaction id back; without it the client fails with "Transaction
/// ID was not returned by Spanner".
fn serve_batch_dml(mock: &mut MockSpanner) -> Log<v1::ExecuteBatchDmlRequest> {
    let log = Log::default();
    let requests = log.clone();
    mock.expect_execute_batch_dml().returning(move |request| {
        let request = request.into_inner();
        let inline_begin = begins_inline(request.transaction.as_ref());
        let statements = request.statements.len().max(1);
        requests.lock().unwrap().push(request);
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
            status: ok_status(),
            ..Default::default()
        }))
    });
    log
}

/// Write-only paths begin explicitly, and the client may also do so instead of beginning inline.
fn serve_begin_transaction(mock: &mut MockSpanner) -> Log<v1::BeginTransactionRequest> {
    let log = Log::default();
    let requests = log.clone();
    mock.expect_begin_transaction().returning(move |request| {
        requests.lock().unwrap().push(request.into_inner());
        Ok(tonic::Response::new(v1::Transaction {
            id: b"mock-txn".to_vec(),
            ..Default::default()
        }))
    });
    log
}

/// The options an autocommit DML batch began its read/write transaction with.
fn inline_begin_options(request: &v1::ExecuteBatchDmlRequest) -> &v1::TransactionOptions {
    let selector = request
        .transaction
        .as_ref()
        .and_then(|t| t.selector.as_ref());
    let Some(Selector::Begin(options)) = selector else {
        panic!("an autocommit DML must begin its transaction inline: {request:?}");
    };
    options
}

#[test]
fn omni_connects_with_explicit_endpoint_and_default_anonymous_credentials() {
    let (server, _) = query_server(&["omni"]);
    let mut driver = SpannerDriver::try_new().unwrap();
    let database = driver
        .new_database_with_opts([
            (OptionDatabase::Uri, OptionValue::from(DATABASE_URI)),
            (OPTION_INSTANCE_TYPE.into(), "omni".into()),
            (OPTION_ENDPOINT.into(), server.endpoint.clone().into()),
        ])
        .unwrap();
    assert_eq!(
        database.get_option_string(OPTION_EMULATOR.into()).unwrap(),
        "false"
    );
    let batches = query(&mut database.new_connection().unwrap(), "SELECT 'omni'");
    assert_eq!(batches[0].column(0).as_string::<i32>().value(0), "omni");
}

#[test]
fn bound_dml_strips_leading_and_trailing_terminators() {
    let mut mock = MockSpanner::new();
    let requests = serve_batch_dml(&mut mock);
    serve_commits(&mut mock);
    let server = MockServer::serve(mock);
    let mut connection = server.connect();
    for update in [false, true] {
        let mut statement = prepare(
            &mut connection,
            "; -- empty\n; INSERT INTO MockTable (c) VALUES (@p);; -- trailing comment",
        );
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
    let requests = Log::default();
    let recorded = requests.clone();
    let mut mock = MockSpanner::new();
    serve_begin_transaction(&mut mock);
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
    serve_commits(&mut mock);
    let server = MockServer::serve(mock);
    let mut connection = server.connect();
    for (sql, partitioned) in [
        ("INSERT INTO MockTable (c) VALUES (@p) THEN RETURN c", false),
        ("UPDATE MockTable SET c = @p WHERE TRUE", true),
    ] {
        let mut statement = prepare(&mut connection, &format!("{sql};; -- comment"));
        statement.bind(ingest_batch(1)).unwrap();
        statement
            .set_option(OPTION_DML_PARTITIONED.into(), partitioned.into())
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
    let requests = Log::default();
    let recorded = requests.clone();
    let mut mock = MockSpanner::new();
    let begins = serve_begin_transaction(&mut mock);
    mock.expect_execute_streaming_sql()
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
            recorded.lock().unwrap().push(request);
            Ok(stream_of(vec![Ok(result)]))
        });
    let commits = serve_commits(&mut mock);
    let server = MockServer::serve(mock);
    let mut connection = server.connect();
    let mut statement = prepare(
        &mut connection,
        "INSERT INTO MockTable (c) VALUES (@c) THEN RETURN c",
    );
    statement.bind(ingest_batch(2)).unwrap();
    let batches = fetch(&mut statement);
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
            Some(Selector::Id(b"mock-txn".to_vec()))
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
            b"mock-txn".to_vec()
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
    let server = MockServer::serve(MockSpanner::new());
    let mut connection = server.connect();
    disable_autocommit(&mut connection);
    for sql in [
        "INSERT INTO MockTable (c) VALUES (@p); SELECT 1",
        "INSERT INTO MockTable (c) VALUES (@p); DELETE FROM MockTable WHERE c = @p",
    ] {
        let mut statement = prepare(&mut connection, sql);
        statement.bind(ingest_batch(1)).unwrap();
        let error = statement.execute_update().unwrap_err();
        assert_eq!(error.status, AdbcStatus::InvalidArguments, "{error}");
        // No RPCs are scripted: committing succeeds only if the invalid batch was not buffered.
        connection.commit().unwrap();
    }
}

#[test]
fn mock_server_round_trips_a_query() {
    let (server, requests) = query_server(&["v1", "v2"]);
    let batches = query(&mut server.connect(), SELECT);

    assert_eq!(requests.lock().unwrap()[0].sql, SELECT);
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.schema().field(0).name(), "c");
    assert_eq!(batch.schema().field(0).data_type(), &DataType::Utf8);
    let column = batch.column(0).as_string::<i32>();
    assert_eq!(column.value(0), "v1");
    assert_eq!(column.value(1), "v2");
}

fn keyed_struct_value() -> prost_types::Value {
    prost_types::Value {
        kind: Some(prost_types::value::Kind::StructValue(prost_types::Struct {
            fields: [("x".into(), string_value("1"))].into_iter().collect(),
        })),
    }
}

/// The error in the first batch of a query against `server`, past `execute`.
fn first_batch_error(server: &MockServer) -> Error {
    let mut reader = prepare(&mut server.connect(), SELECT).execute().unwrap();
    driver_error(&reader.next().unwrap().unwrap_err()).clone()
}

/// Unknown wire kinds must fail even on the UTF-8 fallback, including inside lists.
#[test]
fn unknown_wire_values_are_not_rendered_as_json() {
    let nested = prost_types::Value {
        kind: Some(prost_types::value::Kind::ListValue(
            prost_types::ListValue {
                values: vec![keyed_struct_value()],
            },
        )),
    };
    for value in [keyed_struct_value(), nested] {
        let mut mock = MockSpanner::new();
        let metadata = result_metadata(&[("c", v1::TypeCode::String)]);
        serve_result(&mut mock, metadata, vec![value]);
        let error = first_batch_error(&MockServer::serve(mock));
        assert_eq!(error.status, AdbcStatus::InvalidData);
        assert!(error.message.contains("column \"c\""), "{error}");
    }
}

/// A keyed protobuf Struct cannot represent positional Spanner STRUCT fields with duplicate names.
#[test]
fn keyed_struct_wire_value_errors() {
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
    let mut mock = MockSpanner::new();
    serve_result(&mut mock, metadata, vec![keyed_struct_value()]);
    let error = first_batch_error(&MockServer::serve(mock));
    assert_eq!(error.status, AdbcStatus::InvalidData);
    assert!(error.message.contains("STRUCT"), "{error}");
}

/// A query while DML is buffered would not see the buffered writes, so it fails with
/// `InvalidState`. Queries in autocommit mode or in a fresh manual transaction still run.
#[test]
fn query_while_dml_buffered_in_manual_txn_is_rejected() {
    let (server, _) = query_server(&["v1", "v2"]);
    let mut connection = server.connect();
    assert_eq!(query(&mut connection, SELECT)[0].num_rows(), 2);

    disable_autocommit(&mut connection);

    // The query makes this a query transaction; roll back so the DML below starts a new one.
    assert_eq!(query(&mut connection, SELECT)[0].num_rows(), 2);
    connection.rollback().unwrap();

    assert_eq!(update(&mut connection, INSERT), None);

    let error = prepare(&mut connection, SELECT).execute().err().unwrap();
    assert_eq!(error.status, AdbcStatus::InvalidState);
    assert!(error.message.contains("read-your-writes"), "{error}");

    let mut partitions = prepare(&mut connection, SELECT);
    let error = partitions.execute_partitions().err().unwrap();
    assert_eq!(error.status, AdbcStatus::InvalidState);

    connection.rollback().unwrap();
    assert_eq!(query(&mut connection, SELECT)[0].num_rows(), 2);
}

/// Queries in a manual transaction share one read-only transaction: the first begins it inline and
/// later statements reference it by id. DML is rejected while it is active, and commit needs no
/// RPC.
#[test]
fn manual_transaction_queries_share_one_read_only_transaction() {
    let (server, requests) = query_server(&["v1"]);
    let mut connection = server.connect();
    disable_autocommit(&mut connection);

    for _ in 0..2 {
        assert_eq!(query(&mut connection, SELECT)[0].num_rows(), 1);
    }
    let selectors = selectors(&requests);
    assert_eq!(selectors.len(), 2);
    assert_shared_read_only(&selectors, b"inline-txn");

    // The write would commit in a separate read/write transaction, outside the reads' snapshot.
    let error = prepare(&mut connection, INSERT)
        .execute_update()
        .unwrap_err();
    assert_eq!(error.status, AdbcStatus::InvalidState);
    assert!(error.message.contains("began with a query"), "{error}");

    // Commit is not scripted, so this succeeds only without an RPC.
    connection.commit().unwrap();

    assert_eq!(update(&mut connection, INSERT), None);
    connection.rollback().unwrap();
}

/// `execute_update` on a SELECT, plain or bound, drains it through `ExecuteStreamingSql` and
/// returns no count. This backs the ADBC call with a NULL output stream.
#[test]
fn execute_update_routes_a_query_to_the_read_only_path() {
    const BOUND: &str = "SELECT c FROM MockTable WHERE c = @p";
    let (server, requests) = query_server(&["v1", "v2"]);
    let mut connection = server.connect();

    assert_eq!(update(&mut connection, SELECT), None);
    let mut bound = prepare(&mut connection, BOUND);
    bound.bind(ingest_batch(1)).unwrap();
    assert_eq!(bound.execute_update().unwrap(), None);

    let requests = requests.lock().unwrap();
    let sqls: Vec<_> = requests.iter().map(|r| r.sql.as_str()).collect();
    assert_eq!(sqls, [SELECT, BOUND]);
    let params = requests[1].params.as_ref().unwrap();
    assert_eq!(params.fields.keys().collect::<Vec<_>>(), ["p"]);
}

/// `execute_partitions` accepts at most one bound row. Rejected rows are still consumed so a reused
/// statement cannot apply them later.
#[test]
fn execute_partitions_rejects_multiple_bound_rows_and_consumes_them() {
    let server = MockServer::serve(MockSpanner::new());
    let mut connection = server.connect();
    let mut statement = prepare(&mut connection, "SELECT 1");
    statement.bind(ingest_batch(2)).unwrap();

    let error = statement.execute_partitions().err().unwrap();
    assert_eq!(error.status, AdbcStatus::InvalidArguments);
    assert!(
        error
            .message
            .contains("at most one bound parameter row, but 2 rows"),
        "{error}"
    );

    // Without bound data the schema comes from the SQL, which has no parameters.
    assert_eq!(statement.get_parameter_schema().unwrap().fields().len(), 0);
}

#[test]
fn empty_partitions_retain_the_planned_types_at_the_readers_timestamp_precision() {
    let partition_queries = Log::default();
    let queries = Log::default();
    let (record_partition_queries, record_queries) = (partition_queries.clone(), queries.clone());
    let mut mock = MockSpanner::new();
    serve_begin_transaction(&mut mock);
    mock.expect_partition_query().returning(move |request| {
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
    let server = MockServer::serve(mock);
    let mut producer = server.connect();
    let partitions = prepare(&mut producer, "SELECT Id, Ts, Doc FROM MockTable")
        .execute_partitions()
        .unwrap();
    assert_eq!(partitions.partitions.len(), 1);
    assert_eq!(
        partitions.schema.field(1).data_type(),
        &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
    );

    let mut consumer = server.connect();
    configure(
        &mut consumer,
        &[(OPTION_MAX_TIMESTAMP_PRECISION, "microseconds")],
    );
    let reader = consumer.read_partition(&partitions.partitions[0]).unwrap();
    let schema = reader.schema();
    assert_eq!(schema.fields().len(), 3);
    assert_eq!(schema.field(0).name(), "Id");
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    assert_eq!(schema.field(1).name(), "Ts");
    assert_eq!(
        schema.field(1).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );
    assert_eq!(schema.field(2), partitions.schema.field(2));
    assert_eq!(
        schema.field(2).metadata()["ARROW:extension:name"],
        "arrow.json"
    );
    assert_eq!(
        row_count(&reader.collect::<Result<Vec<_>, _>>().unwrap()),
        0
    );
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
    let (server, requests) = query_server(&[]);
    let mut connection = server.connect();
    let mut statement = prepare(&mut connection, "SELECT @p AS c");
    let value = StringArray::from(vec!["bound value"]);
    let row = RecordBatch::try_from_iter([("p", Arc::new(value) as ArrayRef)]).unwrap();
    let schema = row.schema();
    statement
        .bind_stream(Box::new(RecordBatchIterator::new(
            [Ok(RecordBatch::new_empty(schema.clone())), Ok(row)],
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
    let server = MockServer::serve(MockSpanner::new());
    let mut connection = server.connect();
    let mut statement = connection.new_statement().unwrap();
    let schema = |name, data_type| Arc::new(Schema::new(vec![Field::new(name, data_type, false)]));
    let original = schema("original", DataType::Int64);
    statement
        .bind(RecordBatch::new_empty(original.clone()))
        .unwrap();

    let declared = schema("p", DataType::Utf8);
    for mismatched in [
        schema("p", DataType::Int64),
        schema("renamed", DataType::Utf8),
    ] {
        let reader = RecordBatchIterator::new(
            [
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
    let mut mock = MockSpanner::new();
    mock.expect_execute_streaming_sql()
        .returning(|_| Err(tonic::Status::failed_precondition("rejected bound query")));
    let server = MockServer::serve(mock);

    let mut connection = server.connect();
    let mut statement = prepare(&mut connection, "SELECT c FROM MockTable WHERE c = @p");
    statement.bind(ingest_batch(1)).unwrap();

    let error = statement.execute().err().unwrap();
    assert_eq!(error.status, AdbcStatus::InvalidState, "{error}");

    // While bound, the parameter schema would report `@p` as the bound column's Utf8; once the
    // rows are consumed, the rejected probe leaves it untyped.
    assert_eq!(
        statement.get_parameter_schema().unwrap(),
        Schema::new(vec![Field::new("p", DataType::Null, true)])
    );
}

/// In manual mode, `execute_update` on a SELECT runs it immediately and buffers nothing. A mixed
/// DML/query batch is rejected before buffering, and a SELECT with DML buffered hits the same
/// read-your-writes guard as `execute`.
#[test]
fn execute_update_query_in_manual_mode_buffers_nothing() {
    let (server, _) = query_server(&["v1"]);
    let mut connection = server.connect();
    disable_autocommit(&mut connection);

    assert_eq!(update(&mut connection, SELECT), None);

    let error = prepare(
        &mut connection,
        "DELETE FROM MockTable WHERE true; SELECT 1",
    )
    .execute_update()
    .unwrap_err();
    assert_eq!(error.status, AdbcStatus::InvalidArguments, "{error}");
    assert!(error.message.contains("all-DML"), "{error}");

    // Commit is not scripted, so this succeeds only if nothing was buffered.
    connection.commit().unwrap();

    assert_eq!(update(&mut connection, INSERT), None);
    let error = prepare(&mut connection, SELECT)
        .execute_update()
        .unwrap_err();
    assert_eq!(error.status, AdbcStatus::InvalidState);
    assert!(error.message.contains("read-your-writes"), "{error}");
    connection.rollback().unwrap();
}

/// An `ABORTED` query keeps gRPC code 10 in `vendor_code` and forwards its `RetryInfo` detail.
/// A query is used because the client's transaction runner retries an aborted commit itself.
/// Unlike the unit tests in `src/error.rs`, this decodes the detail from a real gRPC trailer.
#[test]
fn aborted_surfaces_vendor_code_and_retry_info_detail() {
    let mut mock = MockSpanner::new();
    mock.expect_execute_streaming_sql()
        .returning(|_| Err(aborted_with_retry_info("aborted by the mock server")));
    let server = MockServer::serve(mock);

    let error = prepare(&mut server.connect(), SELECT)
        .execute()
        .err()
        .unwrap();
    assert_eq!(error.vendor_code, 10, "{error}");
    assert_eq!(error.status, AdbcStatus::IO, "{error}");
    assert!(
        error.message.contains("aborted by the mock server"),
        "{error}"
    );

    // The value is ProtoJSON; parse it so field order does not matter.
    let json = error_detail(&error, "google.rpc.retryinfo");
    assert_eq!(json["@type"], "type.googleapis.com/google.rpc.RetryInfo");
    assert_eq!(json["retryDelay"], "0.05s");
}

/// After `UNAVAILABLE` mid-stream the client resumes from the last resume token. When the resume
/// fails with a non-retryable status, that error reaches the caller.
#[test]
fn unavailable_mid_stream_surfaces_a_clean_error() {
    let requests = Log::default();
    let recorded = requests.clone();
    let mut mock = MockSpanner::new();
    mock.expect_execute_streaming_sql()
        .returning(move |request| {
            let mut requests = recorded.lock().unwrap();
            requests.push(request.into_inner().resume_token);
            if requests.len() == 1 {
                Ok(stream_of(vec![
                    Ok(partial_result_set(true, &["v1"], b"resume-1", false)),
                    Err(tonic::Status::unavailable("connection lost mid-stream")),
                ]))
            } else {
                Err(tonic::Status::internal("resume attempt refused"))
            }
        });
    let server = MockServer::serve(mock);

    // `execute` fetches the first batch, so the failed resume surfaces here.
    let error = prepare(&mut server.connect(), SELECT)
        .execute()
        .err()
        .unwrap();
    assert_eq!(error.vendor_code, 13, "{error}");
    assert!(error.message.contains("resume attempt refused"), "{error}");
    assert_eq!(*requests.lock().unwrap(), [vec![], b"resume-1".to_vec()]);
}

/// Cancelling from another thread unblocks a reader waiting on a stream that sent one row and
/// went silent.
#[test]
fn cancel_unblocks_a_reader_hung_on_a_silent_stream() {
    let (server, _open_streams) = silent_stream_server(1);
    let mut connection = server.connect();
    let mut statement = one_row_batches(&mut connection);
    let mut reader = statement.execute().unwrap();

    let (tx, rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let first = reader.next();
        let second = reader.next();
        let _ = tx.send((first, second));
    });

    std::thread::sleep(Duration::from_millis(300));
    let cancel_at = Instant::now();
    statement.get_cancel_handle().try_cancel().unwrap();

    let (first, second) = rx.recv_timeout(Duration::from_secs(30)).unwrap();
    let cancel_latency = cancel_at.elapsed();
    worker.join().unwrap();

    assert_eq!(first.unwrap().unwrap().num_rows(), 1);
    let error = second.unwrap().unwrap_err();
    assert_eq!(driver_error(&error).status, AdbcStatus::Cancelled);
    assert!(
        cancel_latency < Duration::from_secs(10),
        "{cancel_latency:?}"
    );
}

/// A timeout on `option` after `seconds`; a stream that died for another reason would fail sooner.
fn assert_deadline_fired(error: &Error, option: &str, seconds: f64, started: Instant) {
    let elapsed = started.elapsed();
    assert_eq!(error.status, AdbcStatus::Timeout, "{error}");
    assert!(error.message.contains(option), "{error}");
    assert!(
        error
            .message
            .contains(&format!("timed out after {seconds}s")),
        "{error}"
    );
    assert!(elapsed.as_secs_f64() >= seconds, "failed after {elapsed:?}");
}

/// `spanner.rpc.timeout_seconds.update` bounds DDL, including the operation poll loop. A silent
/// admin endpoint makes this deterministic; emulator DDL can finish faster than a tiny deadline.
/// Without the driver deadline, the admin client's own ~60 s deadline would fail with `Internal`.
#[test]
fn ddl_update_timeout_fires_on_a_silent_admin_endpoint() {
    let server = MockServer::serve(MockSpanner::new());
    let gate = GatedEndpoint::start(&server.endpoint);
    let mut connection = gate.connect();
    gate.go_silent();

    let mut statement = prepare(
        &mut connection,
        "CREATE TABLE T (Id INT64) PRIMARY KEY (Id)",
    );
    statement
        .set_option(OPTION_RPC_TIMEOUT_UPDATE.into(), 0.25.into())
        .unwrap();

    let started = Instant::now();
    let error = statement.execute_update().unwrap_err();
    assert_deadline_fired(&error, OPTION_RPC_TIMEOUT_UPDATE, 0.25, started);
}

/// A server whose streams send `rows` one-row messages and then stay open without sending more.
/// With `rows = 0`, `execute` itself blocks; with `rows = 1`, the next fetch does.
///
/// Hold the returned senders for as long as the streams must stay silent; dropping them ends the
/// streams.
fn silent_stream_server(rows: usize) -> (MockServer, Log<PartialResultSetSender>) {
    let open_streams = Log::default();
    let streams_in_mock = open_streams.clone();
    let mut mock = MockSpanner::new();
    mock.expect_execute_streaming_sql().returning(move |_| {
        let (tx, rx) = tokio::sync::mpsc::channel(rows.max(1));
        for i in 0..rows {
            tx.try_send(Ok(partial_result_set(i == 0, &["v1"], b"rt-1", false)))
                .unwrap();
        }
        streams_in_mock.lock().unwrap().push(tx);
        Ok(tonic::Response::new(rx))
    });
    (MockServer::serve(mock), open_streams)
}

/// A [`SELECT`] with one row per batch, so on a [`silent_stream_server`] `execute` returns and the
/// second batch blocks.
fn one_row_batches(connection: &mut SpannerConnection) -> SpannerStatement {
    let mut statement = prepare(connection, SELECT);
    statement
        .set_option(OPTION_ROWS_PER_BATCH.into(), 1_i64.into())
        .unwrap();
    statement
}

/// `spanner.rpc.timeout_seconds.fetch` bounds fetches after the first batch. The stream sends one
/// row first so that `execute`, which the query deadline covers, has returned before anything
/// blocks.
#[test]
fn fetch_timeout_fires_on_a_silent_stream() {
    let (server, _open_streams) = silent_stream_server(1);
    let mut connection = server.connect();

    // The fetch deadline cannot start before `execute`, so the call must outlast it from here.
    let started = Instant::now();
    let mut statement = one_row_batches(&mut connection);
    statement
        .set_option(OPTION_RPC_TIMEOUT_FETCH.into(), 0.5.into())
        .unwrap();
    let mut reader = statement.execute().unwrap();
    assert_eq!(reader.next().unwrap().unwrap().num_rows(), 1);

    let error = reader.next().unwrap().unwrap_err();
    assert_deadline_fired(driver_error(&error), OPTION_RPC_TIMEOUT_FETCH, 0.5, started);
}

/// Control for [`fetch_timeout_fires_on_a_silent_stream`]: without the fetch deadline, nothing
/// else turns the silence into an error.
#[test]
fn silent_stream_without_the_fetch_deadline_never_times_out() {
    let (server, _open_streams) = silent_stream_server(1);
    let mut connection = server.connect();
    let mut reader = one_row_batches(&mut connection).execute().unwrap();

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
    let (server, _open_streams) = silent_stream_server(0);
    let mut connection = server.connect();
    let mut statement = prepare(&mut connection, SELECT);
    statement
        .set_option(OPTION_RPC_TIMEOUT_QUERY.into(), 0.5.into())
        .unwrap();

    let started = Instant::now();
    let error = statement.execute().err().unwrap();
    assert_deadline_fired(&error, OPTION_RPC_TIMEOUT_QUERY, 0.5, started);
}

/// A new operation on the statement must not clear the cancel on an earlier reader. If it did, the
/// old reader could resume, or end cleanly after a buffered batch and truncate the result.
#[test]
fn new_operation_does_not_uncancel_an_earlier_streamed_reader() {
    // With one row per batch, the old reader has row 2 buffered and row 3 pending at the cancel.
    let (server, _) = query_server(&["v1", "v2", "v3"]);
    let mut connection = server.connect();
    let mut statement = one_row_batches(&mut connection);
    let mut old_reader = statement.execute().unwrap();
    assert_eq!(old_reader.next().unwrap().unwrap().num_rows(), 1);

    // Start a new operation before the old reader observes the cancel.
    statement.get_cancel_handle().try_cancel().unwrap();
    assert_eq!(row_count(&fetch(&mut statement)), 3);

    let error = old_reader.next().unwrap().unwrap_err();
    assert_eq!(driver_error(&error).status, AdbcStatus::Cancelled);
    assert!(old_reader.next().is_none());
}

/// Secondary indexes count toward Spanner's mutation limit, so a chunk within the driver's budget
/// can still be rejected. The driver bisects it until commits succeed. The mock rejects commits
/// with more than 40 mutations.
#[test]
fn ingest_bisects_a_chunk_that_overshoots_the_mutation_limit() {
    let mut mock = MockSpanner::new();
    serve_begin_transaction(&mut mock);
    let commits = serve_commits_with(&mut mock, |request| {
        if request.mutations.len() > 40 {
            Err(too_many_mutations_status())
        } else {
            commit_ok()
        }
    });
    let server = MockServer::serve(mock);

    let mut connection = server.connect();
    let count = ingest(append_ingest_statement(&mut connection), 100);
    assert_eq!(count.unwrap(), Some(100));

    // 100 rows bisect to 50, then 25.
    let commits = commits.lock().unwrap();
    let sizes: Vec<_> = commits.iter().map(|c| c.mutations.len()).collect();
    let accepted: usize = sizes.iter().filter(|&&n| n <= 40).sum();
    assert_eq!(accepted, 100, "{sizes:?}");
    assert!(sizes.iter().any(|&n| n > 40), "{sizes:?}");
}

/// Other commit errors propagate after a single attempt.
#[test]
fn ingest_does_not_bisect_a_non_mutation_limit_error() {
    let mut mock = MockSpanner::new();
    serve_begin_transaction(&mut mock);
    let commits = serve_commits_with(&mut mock, |_| {
        Err(tonic::Status::already_exists(
            "Row [v0] in table MockTable already exists",
        ))
    });
    let server = MockServer::serve(mock);

    let mut connection = server.connect();
    let error = ingest(append_ingest_statement(&mut connection), 100).unwrap_err();
    assert_eq!(error.status, AdbcStatus::Integrity, "{error}");
    assert!(error.message.contains("MockTable"), "{error}");
    assert_eq!(commits.lock().unwrap().len(), 1);
}

/// A cancelled commit may still apply on the server, so the error must say the outcome is unknown
/// and that retrying risks duplicates. The unscripted exists-probe fails, which leaves the
/// annotated error unchanged.
#[test]
fn cancelled_ingest_commit_reports_ambiguous_outcome() {
    let mut mock = MockSpanner::new();
    serve_begin_transaction(&mut mock);
    let commits = serve_commits_with(&mut mock, |_| {
        Err(tonic::Status::cancelled("commit cancelled by peer"))
    });
    let server = MockServer::serve(mock);

    // A single chunk, so no rows were committed before it.
    let mut connection = server.connect();
    let error = ingest(append_ingest_statement(&mut connection), 1).unwrap_err();
    assert_eq!(error.status, AdbcStatus::Cancelled, "{error}");
    assert!(error.message.contains("outcome is unknown"), "{error}");
    assert!(error.message.contains("duplicate rows"), "{error}");
    // A retry could duplicate rows.
    assert_eq!(commits.lock().unwrap().len(), 1);
}

/// A failed `append` commit triggers an `INFORMATION_SCHEMA.TABLES` probe to choose between
/// `NotFound` and `AlreadyExists`. If the probe itself fails (for example, no read permission),
/// the original commit error is returned unchanged.
#[test]
fn ingest_append_keeps_the_original_error_when_the_exists_probe_fails() {
    let probes = Arc::new(AtomicUsize::new(0));
    let probes_in_mock = probes.clone();
    let mut mock = MockSpanner::new();
    serve_begin_transaction(&mut mock);
    // Neither a constraint violation nor the mutation limit, both of which skip the probe.
    serve_commits_with(&mut mock, |_| {
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
    let server = MockServer::serve(mock);

    let mut connection = server.connect();
    let error = ingest(append_ingest_statement(&mut connection), 1).unwrap_err();
    assert_eq!(probes.load(Ordering::SeqCst), 1);
    assert_eq!(error.status, AdbcStatus::InvalidArguments, "{error}");
    assert!(
        error.message.contains("Invalid value for column c"),
        "{error}"
    );
    assert!(!error.message.contains("INFORMATION_SCHEMA"), "{error}");
    assert_eq!(error.vendor_code, 3, "{error}");
}

/// BatchWrite reports a failed group as a status inside the response stream, which takes a
/// different error path from gRPC errors. It must map the code, name the table and forward
/// details the same way.
#[test]
fn batch_write_group_failure_forwards_status_details() {
    let mut mock = MockSpanner::new();
    let writes = serve_batch_write(&mut mock, vec![batch_write_group_already_exists(0)]);
    let server = MockServer::serve(mock);

    // One row per mutation group.
    let mut connection = server.connect();
    let error = ingest(batch_write_ingest_statement(&mut connection), 1).unwrap_err();
    assert_eq!(writes.lock().unwrap().len(), 1);
    assert_eq!(error.status, AdbcStatus::Integrity, "{error}");
    assert_eq!(error.vendor_code, 6, "{error}");
    assert!(error.message.contains("MockTable"), "{error}");

    let json = error_detail(&error, "google.rpc.errorinfo");
    assert_eq!(json["@type"], "type.googleapis.com/google.rpc.ErrorInfo");
    assert_eq!(json["reason"], "DUPLICATE_KEY");
    assert_eq!(json["domain"], "spanner.googleapis.com");
}

/// BatchWrite is not atomic, so groups that applied before a failure in the same chunk count as
/// committed.
#[test]
fn batch_write_folds_same_chunk_applied_rows_into_committed_count() {
    let mut mock = MockSpanner::new();
    serve_batch_write(
        &mut mock,
        vec![
            batch_write_groups_ok(vec![0, 1]),
            batch_write_group_already_exists(2),
        ],
    );
    let server = MockServer::serve(mock);

    let mut connection = server.connect();
    let error = ingest(batch_write_ingest_statement(&mut connection), 3).unwrap_err();
    assert_eq!(error.status, AdbcStatus::Integrity, "{error}");
    assert!(error.message.contains("MockTable"), "{error}");
    assert!(
        error
            .message
            .contains("2 row(s) from this bulk ingest were already committed"),
        "{error}"
    );
}

/// A manual-mode ingest that fails converting a later row buffers none of its rows; otherwise a
/// later commit would apply a partial batch.
#[test]
fn manual_ingest_conversion_failure_leaves_txn_buffer_untouched() {
    let mut mock = MockSpanner::new();
    // Serves the append target's column probe and the query after the failure.
    serve_queries(&mut mock, &["d"]);
    serve_begin_transaction(&mut mock);
    let commits = serve_commits(&mut mock);
    let server = MockServer::serve(mock);

    let mut connection = server.connect();
    disable_autocommit(&mut connection);
    let mut statement = append_ingest_statement(&mut connection);
    let dates = |days: Vec<i32>| {
        RecordBatch::try_from_iter([("d", Arc::new(Date32Array::from(days)) as ArrayRef)]).unwrap()
    };

    // Row 1 is outside Spanner's DATE range, so conversion fails after row 0 was built.
    statement.bind(dates(vec![0, i32::MAX])).unwrap();
    let error = statement.execute_update().unwrap_err();
    assert!(error.message.contains("out of range"), "{error}");

    // A query is rejected while DML is buffered, so this succeeds only with an empty buffer.
    assert_eq!(query(&mut connection, SELECT)[0].num_rows(), 1);
    connection.rollback().unwrap();

    statement.bind(dates(vec![0, 1])).unwrap();
    assert_eq!(statement.execute_update().unwrap(), None);

    connection.commit().unwrap();
    let commits = commits.lock().unwrap();
    let sizes: Vec<_> = commits.iter().map(|c| c.mutations.len()).collect();
    assert_eq!(sizes, [2], "only the fixed batch's rows");
}

/// `spanner.commit_stats` requests commit stats and exposes the server's mutation count. The
/// emulator returns no stats, so only the mock covers the response. The ingest's commit also
/// carries `spanner.commit.max_delay`.
#[test]
fn commit_stats_mutation_count_is_captured_from_the_commit_response() {
    // Not derivable from the two ingested rows.
    const SCRIPTED_MUTATION_COUNT: i64 = 4242;

    let mut mock = MockSpanner::new();
    serve_begin_transaction(&mut mock);
    // Without a precommit token the client commits once.
    let commits = serve_commits_with(&mut mock, |_| {
        Ok(tonic::Response::new(v1::CommitResponse {
            commit_stats: Some(v1::commit_response::CommitStats {
                mutation_count: SCRIPTED_MUTATION_COUNT,
            }),
            ..Default::default()
        }))
    });
    let server = MockServer::serve(mock);

    let mut connection = server.connect();
    let mut statement = append_ingest_statement(&mut connection);
    configure(
        &mut statement,
        &[
            (OPTION_COMMIT_STATS, "true"),
            (OPTION_MAX_COMMIT_DELAY, "100ms"),
        ],
    );
    let ids = Int64Array::from(vec![1_i64, 2]);
    let rows = RecordBatch::try_from_iter([("Id", Arc::new(ids) as ArrayRef)]).unwrap();
    statement.bind(rows).unwrap();
    assert_eq!(statement.execute_update().unwrap(), Some(2));

    let commits = commits.lock().unwrap();
    assert_eq!(commits.len(), 1);
    assert!(commits[0].return_commit_stats);
    assert_eq!(
        commits[0].max_commit_delay,
        Some(prost_types::Duration {
            seconds: 0,
            nanos: 100_000_000,
        })
    );
    assert_eq!(
        statement
            .get_option_int(OPTION_COMMIT_STATS_MUTATION_COUNT.into())
            .unwrap(),
        SCRIPTED_MUTATION_COUNT
    );
}

/// `spanner.commit.max_delay` on autocommit DML and manual commits: unset, inherited from the
/// connection, overridden per statement, and the connection's value on a manual commit.
#[test]
fn max_commit_delay_reaches_the_wire_on_runner_commits() {
    let mut mock = MockSpanner::new();
    serve_begin_transaction(&mut mock);
    serve_batch_dml(&mut mock);
    let commits = serve_commits(&mut mock);
    let server = MockServer::serve(mock);
    let mut connection = server.connect();

    assert_eq!(update(&mut connection, INSERT), Some(1));

    configure(&mut connection, &[(OPTION_MAX_COMMIT_DELAY, "100ms")]);
    assert_eq!(update(&mut connection, INSERT), Some(1));
    let mut overriding = prepare(&mut connection, INSERT);
    configure(&mut overriding, &[(OPTION_MAX_COMMIT_DELAY, "250ms")]);
    assert_eq!(overriding.execute_update().unwrap(), Some(1));

    disable_autocommit(&mut connection);
    assert_eq!(update(&mut connection, INSERT), None);
    connection.commit().unwrap();

    let delay = |millis| {
        Some(prost_types::Duration {
            seconds: 0,
            nanos: millis * 1_000_000,
        })
    };
    let commits = commits.lock().unwrap();
    let delays: Vec<_> = commits.iter().map(|c| c.max_commit_delay).collect();
    assert_eq!(delays, [None, delay(100), delay(250), delay(100)]);
}

/// The ADBC isolation level reaches the inline begin of autocommit DML. Spanner implements
/// `REPEATABLE_READ` as snapshot isolation; levels it lacks are promoted to the weakest supported
/// level that satisfies them.
#[test]
fn isolation_level_reaches_transaction_options_on_the_begin() {
    use adbc_core::constants::*;
    use v1::transaction_options::IsolationLevel;

    let mut mock = MockSpanner::new();
    serve_begin_transaction(&mut mock);
    let batches = serve_batch_dml(&mut mock);
    serve_commits(&mut mock);
    let server = MockServer::serve(mock);

    // The requested level (`None` = never set) and the expected wire level. Unit tests in
    // src/connection/tests.rs cover the mapping of every ADBC level.
    let cases = [
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
    for (requested, expected) in cases {
        if let Some(level) = requested {
            connection
                .set_option(OptionConnection::IsolationLevel, level.into())
                .unwrap();
        }
        assert_eq!(update(&mut connection, INSERT), Some(1));
        assert_eq!(
            inline_begin_options(batches.lock().unwrap().last().unwrap()).isolation_level,
            expected as i32,
            "{requested:?}"
        );
    }
    assert_eq!(batches.lock().unwrap().len(), cases.len());
}

/// A manual transaction with only mutations commits through the client's replay-protected
/// write-only transaction (a begin with `mutation_key`, no DML). With DML buffered, it uses the
/// read/write runner, which begins inline.
#[test]
fn mutations_only_manual_commit_uses_the_write_only_path() {
    let mut mock = MockSpanner::new();
    let begins = serve_begin_transaction(&mut mock);
    let batch_dml = serve_batch_dml(&mut mock);
    let commits = serve_commits(&mut mock);
    let server = MockServer::serve(mock);

    let mut connection = server.connect();
    disable_autocommit(&mut connection);

    let mut ingest = append_ingest_statement(&mut connection);
    ingest.bind(ingest_batch(2)).unwrap();
    assert_eq!(ingest.execute_update().unwrap(), None);
    connection.commit().unwrap();

    {
        let begins = begins.lock().unwrap();
        assert_eq!(begins.len(), 1);
        assert!(begins[0].mutation_key.is_some(), "{:?}", begins[0]);
    }
    assert_eq!(batch_dml.lock().unwrap().len(), 0);
    {
        let commits = commits.lock().unwrap();
        assert_eq!(commits.len(), 1);
        assert_eq!(commits[0].mutations.len(), 2);
        // A replay-protected write commits by transaction id, never single-use.
        assert!(matches!(
            commits[0].transaction,
            Some(v1::commit_request::Transaction::TransactionId(_))
        ));
    }

    assert_eq!(update(&mut connection, UPDATE), None);
    ingest.bind(ingest_batch(1)).unwrap();
    assert_eq!(ingest.execute_update().unwrap(), None);
    connection.commit().unwrap();

    assert_eq!(batch_dml.lock().unwrap().len(), 1);
    assert_eq!(
        begins.lock().unwrap().len(),
        1,
        "the DML commit begins inline"
    );
    let commits = commits.lock().unwrap();
    assert_eq!(commits.len(), 2);
    assert_eq!(commits[1].mutations.len(), 1);
}

/// `adbc.connection.readonly` also blocks committing DML buffered before it was set, whether by
/// `commit()` or by re-enabling autocommit. The buffer survives for a later writable commit;
/// rollback, queries and query-transaction commits still work.
#[test]
fn read_only_connection_rejects_the_commit_of_buffered_dml() {
    let mut mock = MockSpanner::new();
    serve_queries(&mut mock, &["v0"]);
    serve_begin_transaction(&mut mock);
    let batch_dml = serve_batch_dml(&mut mock);
    let commits = serve_commits(&mut mock);
    let server = MockServer::serve(mock);

    let dml_sqls = || {
        let requests = batch_dml.lock().unwrap();
        let statements = requests.iter().flat_map(|request| &request.statements);
        statements.map(|s| s.sql.clone()).collect::<Vec<_>>()
    };
    let set_read_only = |connection: &mut SpannerConnection, value: bool| {
        connection
            .set_option(OptionConnection::ReadOnly, value.into())
            .unwrap();
    };

    let mut connection = server.connect();
    disable_autocommit(&mut connection);

    let mut dml = prepare(&mut connection, UPDATE);
    assert_eq!(dml.execute_update().unwrap(), None);
    set_read_only(&mut connection, true);

    let error = connection.commit().unwrap_err();
    assert_eq!(error.status, AdbcStatus::InvalidState);
    assert!(error.message.contains("read-only"), "{error}");

    // Enabling autocommit would commit the pending work.
    let error = connection
        .set_option(OptionConnection::AutoCommit, true.into())
        .unwrap_err();
    assert_eq!(error.status, AdbcStatus::InvalidState);
    assert_eq!(
        connection
            .get_option_string(OptionConnection::AutoCommit)
            .unwrap(),
        "false"
    );

    assert!(dml_sqls().is_empty());
    assert_eq!(commits.lock().unwrap().len(), 0);

    set_read_only(&mut connection, false);
    connection.commit().unwrap();
    assert_eq!(dml_sqls(), [UPDATE]);
    assert_eq!(commits.lock().unwrap().len(), 1);

    dml.execute_update().unwrap();
    set_read_only(&mut connection, true);
    connection.rollback().unwrap();

    assert_eq!(query(&mut connection, SELECT)[0].num_rows(), 1);
    // A query transaction writes nothing, so read-only allows its commit.
    connection.commit().unwrap();

    assert_eq!(dml_sqls(), [UPDATE]);
    assert_eq!(commits.lock().unwrap().len(), 1);
}

const READ_TIMESTAMP_RFC3339: &str = "2026-07-07T00:00:00Z";
const READ_TIMESTAMP: prost_types::Timestamp = prost_types::Timestamp {
    seconds: 1_783_382_400,
    nanos: 0,
};
const HALF_A_SECOND: prost_types::Duration = prost_types::Duration {
    seconds: 0,
    nanos: 500_000_000,
};
const TEN_SECONDS: prost_types::Duration = prost_types::Duration {
    seconds: 10,
    nanos: 0,
};

fn single_use_read_only_bound(selector: &Selector) -> WireBound {
    let Selector::SingleUse(options) = selector else {
        panic!("a plain autocommit query must run single-use, got: {selector:?}");
    };
    read_only_bound(options)
}

fn read_only_bound(options: &v1::TransactionOptions) -> WireBound {
    let Some(v1::transaction_options::Mode::ReadOnly(read_only)) = &options.mode else {
        panic!("expected read-only transaction options, got: {options:?}");
    };
    read_only.timestamp_bound.unwrap()
}

/// Each `spanner.read.staleness` form reaches the single-use selector, inherited from the
/// connection or set on the statement.
#[test]
fn read_staleness_reaches_the_wire_on_single_use_queries() {
    let (server, requests) = query_server(&["v1"]);
    let mut connection = server.connect();
    configure(&mut connection, &[(OPTION_READ_STALENESS, "exact:10s")]);

    let cases = [
        (None, WireBound::ExactStaleness(TEN_SECONDS)),
        (
            Some("max:500ms".to_string()),
            WireBound::MaxStaleness(HALF_A_SECOND),
        ),
        (
            Some(format!("read:{READ_TIMESTAMP_RFC3339}")),
            WireBound::ReadTimestamp(READ_TIMESTAMP),
        ),
        (
            Some(format!("min:{READ_TIMESTAMP_RFC3339}")),
            WireBound::MinReadTimestamp(READ_TIMESTAMP),
        ),
    ];
    for (index, (staleness, expected)) in cases.iter().enumerate() {
        let mut statement = prepare(&mut connection, SELECT);
        if let Some(value) = staleness {
            configure(&mut statement, &[(OPTION_READ_STALENESS, value)]);
        }
        fetch(&mut statement);
        assert_eq!(
            single_use_read_only_bound(&selectors(&requests)[index]),
            *expected,
            "{staleness:?}"
        );
    }
    assert_eq!(selectors(&requests).len(), cases.len());
}

/// Staleness also applies to `execute_schema` (single-use) and to the `get_objects` snapshot,
/// which begins a read-only transaction inline.
#[test]
fn read_staleness_reaches_the_wire_on_metadata_reads() {
    let expected = WireBound::ExactStaleness(TEN_SECONDS);
    let (server, requests) = query_server(&["default"]);
    let mut connection = server.connect();
    configure(&mut connection, &[(OPTION_READ_STALENESS, "exact:10s")]);

    prepare(&mut connection, SELECT).execute_schema().unwrap();
    let probes = selectors(&requests);
    assert_eq!(probes.len(), 1);
    assert_eq!(single_use_read_only_bound(&probes[0]), expected);

    // At `Schemas` depth, get_objects issues only the SCHEMATA query.
    let reader = connection
        .get_objects(ObjectDepth::Schemas, None, None, None, None, None)
        .unwrap();
    reader.collect::<Result<Vec<_>, _>>().unwrap();
    let begin = selectors(&requests).into_iter().find_map(|s| match s {
        Selector::Begin(options) => Some(options),
        _ => None,
    });
    assert_eq!(read_only_bound(&begin.unwrap()), expected);
}

/// Spanner allows bounded staleness only on single-use transactions, so a multi-row bound query
/// pins `max:<d>` to exact staleness `<d>` and `min:<t>` to read timestamp `<t>`. Multiple rows
/// share a multi-use read-only transaction that the first request begins inline.
#[test]
fn bounded_staleness_is_pinned_for_multi_use_bound_queries() {
    for (staleness, expected_pin) in [
        (
            "max:500ms".to_string(),
            WireBound::ExactStaleness(HALF_A_SECOND),
        ),
        (
            format!("min:{READ_TIMESTAMP_RFC3339}"),
            WireBound::ReadTimestamp(READ_TIMESTAMP),
        ),
    ] {
        let (server, requests) = query_server(&["v"]);
        let mut connection = server.connect();
        let mut statement = prepare(&mut connection, "SELECT c FROM MockTable WHERE c = @val");
        configure(&mut statement, &[(OPTION_READ_STALENESS, &staleness)]);
        statement.bind(ingest_batch(2)).unwrap();
        assert_eq!(row_count(&fetch(&mut statement)), 2, "{staleness}");

        let selectors = selectors(&requests);
        assert_eq!(selectors.len(), 2, "{staleness}");
        let options = assert_shared_read_only(&selectors, b"inline-txn");
        assert_eq!(read_only_bound(options), expected_pin, "{staleness}");
    }
}

/// `spanner.directed_read` applies to queries but not DML: Spanner rejects directed reads in a
/// read/write transaction. `THEN RETURN` DML is the risky case because it uses `ExecuteSql` like a
/// query; plain DML uses `ExecuteBatchDml`, which has no directed-read field.
#[test]
fn directed_read_reaches_the_wire_on_queries_but_never_on_dml() {
    use v1::directed_read_options as dro;

    const OVERRIDE_QUERY_SQL: &str = "SELECT c FROM OtherMockTable";
    const RETURNING_SQL: &str = "INSERT INTO MockTable (c) VALUES ('y') THEN RETURN c";

    let mut mock = MockSpanner::new();
    serve_begin_transaction(&mut mock);
    let streaming = serve_queries(&mut mock, &["v"]);
    let batch_dml = serve_batch_dml(&mut mock);
    serve_commits(&mut mock);
    let server = MockServer::serve(mock);

    let mut connection = server.connect();
    configure(
        &mut connection,
        &[(OPTION_DIRECTED_READ, "include:us-east1:read_only")],
    );

    query(&mut connection, SELECT);
    let mut override_query = prepare(&mut connection, OVERRIDE_QUERY_SQL);
    configure(
        &mut override_query,
        &[(OPTION_DIRECTED_READ, "exclude:eu-west1")],
    );
    fetch(&mut override_query);
    assert_eq!(update(&mut connection, INSERT), Some(1));
    assert_eq!(update(&mut connection, RETURNING_SQL), Some(1));

    let streaming = streaming.lock().unwrap();
    let directed_for = |sql: &str| {
        let request = streaming.iter().find(|request| request.sql == sql);
        request.unwrap().directed_read_options.clone()
    };

    assert_eq!(
        directed_for(SELECT),
        Some(v1::DirectedReadOptions {
            replicas: Some(dro::Replicas::IncludeReplicas(dro::IncludeReplicas {
                replica_selections: vec![dro::ReplicaSelection {
                    location: "us-east1".to_string(),
                    r#type: dro::replica_selection::Type::ReadOnly as i32,
                }],
                auto_failover_disabled: false,
            })),
        })
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
        })
    );
    assert_eq!(directed_for(RETURNING_SQL), None);

    let batch_dml = batch_dml.lock().unwrap();
    assert_eq!(batch_dml.len(), 1);
    assert_eq!(batch_dml[0].statements.len(), 1);
    assert_eq!(batch_dml[0].statements[0].sql, INSERT);
}

/// The optimizer version and statistics package reach `query_options`, inherited, overridden and
/// partly overridden.
#[test]
fn query_optimizer_options_reach_the_wire_on_queries() {
    const VERSION: &str = OPTION_QUERY_OPTIMIZER_VERSION;
    const PACKAGE: &str = OPTION_QUERY_OPTIMIZER_STATISTICS_PACKAGE;

    let (server, requests) = query_server(&["v1"]);
    let mut connection = server.connect();
    configure(
        &mut connection,
        &[(VERSION, "6"), (PACKAGE, "auto_20260101")],
    );

    let cases: [(&[_], _, _); 3] = [
        (&[], "6", "auto_20260101"),
        (
            &[(VERSION, "latest"), (PACKAGE, "custom_package")],
            "latest",
            "custom_package",
        ),
        (&[(VERSION, "7")], "7", "auto_20260101"),
    ];
    for (overrides, version, package) in cases {
        let mut statement = prepare(&mut connection, SELECT);
        configure(&mut statement, overrides);
        fetch(&mut statement);
        assert_eq!(
            requests.lock().unwrap().last().unwrap().query_options,
            Some(v1::execute_sql_request::QueryOptions {
                optimizer_version: version.to_string(),
                optimizer_statistics_package: package.to_string(),
            }),
            "{overrides:?}"
        );
    }
    assert_eq!(requests.lock().unwrap().len(), cases.len());
}

/// An autocommit DML batch is the whole transaction, so it sets `last_statements`. DML replayed by
/// a manual commit does not, since buffered mutations may follow it.
#[test]
fn autocommit_batch_dml_is_flagged_last_statements_but_manual_commit_is_not() {
    const BATCH_SQL: &str =
        "DELETE FROM MockTable WHERE TRUE; INSERT INTO MockTable (c) VALUES ('x')";

    let mut mock = MockSpanner::new();
    serve_begin_transaction(&mut mock);
    let batch_dml = serve_batch_dml(&mut mock);
    serve_commits(&mut mock);
    let server = MockServer::serve(mock);

    let mut connection = server.connect();
    assert_eq!(update(&mut connection, BATCH_SQL), Some(2));

    disable_autocommit(&mut connection);
    assert_eq!(update(&mut connection, BATCH_SQL), None);
    connection.commit().unwrap();

    let seen = batch_dml.lock().unwrap();
    assert_eq!(seen.len(), 2);
    assert_eq!(seen[0].statements.len(), 2);
    assert!(seen[0].last_statements);
    assert_eq!(seen[1].statements.len(), 2);
    assert!(!seen[1].last_statements);
}

/// BatchWrite carries the priority, transaction tag and change-stream exclusion. Spanner ignores
/// request tags on BatchWrite, so none is sent.
#[test]
fn batch_write_carries_the_request_options_and_change_stream_exclusion() {
    let mut mock = MockSpanner::new();
    let requests = serve_batch_write(
        &mut mock,
        vec![v1::BatchWriteResponse {
            indexes: vec![0],
            status: ok_status(),
            ..Default::default()
        }],
    );
    let server = MockServer::serve(mock);

    let mut connection = server.connect();
    configure(
        &mut connection,
        &[
            (OPTION_REQUEST_PRIORITY, "high"),
            (OPTION_TRANSACTION_TAG, "nightly-etl"),
            (OPTION_REQUEST_TAG, "ignored-by-batch-write"),
        ],
    );

    let statement = batch_write_ingest_statement(&mut connection);
    assert_eq!(ingest(statement, 1).unwrap(), Some(1));
    {
        let seen = requests.lock().unwrap();
        assert_eq!(seen.len(), 1);
        let options = seen[0].request_options.as_ref().unwrap();
        assert_eq!(options.priority, v1::request_options::Priority::High as i32);
        assert_eq!(options.transaction_tag, "nightly-etl");
        assert_eq!(options.request_tag, "");
        assert!(!seen[0].exclude_txn_from_change_streams);
    }

    configure(
        &mut connection,
        &[(OPTION_EXCLUDE_TXN_FROM_CHANGE_STREAMS, "true")],
    );
    let statement = batch_write_ingest_statement(&mut connection);
    assert_eq!(ingest(statement, 1).unwrap(), Some(1));
    let seen = requests.lock().unwrap();
    assert_eq!(seen.len(), 2);
    assert!(seen[1].exclude_txn_from_change_streams);
}

/// The change-stream exclusion reaches the transaction options of autocommit DML.
#[test]
fn exclude_from_change_streams_reaches_the_wire_on_runner_commits() {
    let mut mock = MockSpanner::new();
    serve_begin_transaction(&mut mock);
    let batch_dml = serve_batch_dml(&mut mock);
    serve_commits(&mut mock);
    let server = MockServer::serve(mock);

    let mut connection = server.connect();
    assert_eq!(update(&mut connection, INSERT), Some(1));
    configure(
        &mut connection,
        &[(OPTION_EXCLUDE_TXN_FROM_CHANGE_STREAMS, "true")],
    );
    assert_eq!(update(&mut connection, INSERT), Some(1));

    let seen = batch_dml.lock().unwrap();
    assert_eq!(seen.len(), 2);
    assert!(!inline_begin_options(&seen[0]).exclude_txn_from_change_streams);
    assert!(inline_begin_options(&seen[1]).exclude_txn_from_change_streams);
}

/// Priority and tags reach user queries, DML and commits. Metadata reads get the priority but no
/// tags: priority controls workload, while tags attribute user statements in Spanner's query and
/// transaction statistics. The emulator ignores `RequestOptions`, so only the mock covers this.
#[test]
fn request_priority_reaches_metadata_reads_but_tags_do_not() {
    let queries = Log::default();
    let recorded = queries.clone();
    let mut mock = MockSpanner::new();
    serve_begin_transaction(&mut mock);
    mock.expect_execute_streaming_sql()
        .returning(move |request| {
            let request = request.into_inner();
            // get_table_schema's INFORMATION_SCHEMA lookup reads names and nullability.
            let result = if request.sql.contains("INFORMATION_SCHEMA") {
                v1::PartialResultSet {
                    metadata: Some(result_metadata(&[
                        ("COLUMN_NAME", v1::TypeCode::String),
                        ("IS_NULLABLE", v1::TypeCode::String),
                    ])),
                    last: true,
                    ..Default::default()
                }
            } else {
                partial_result_set(true, &["v"], b"r-1", true)
            };
            recorded.lock().unwrap().push(request);
            Ok(stream_of(vec![Ok(result)]))
        });
    let batch_dml = serve_batch_dml(&mut mock);
    let commits = serve_commits(&mut mock);
    let server = MockServer::serve(mock);

    let mut connection = server.connect();
    configure(
        &mut connection,
        &[
            (OPTION_REQUEST_PRIORITY, "high"),
            (OPTION_REQUEST_TAG, "user-req"),
            (OPTION_TRANSACTION_TAG, "user-txn"),
        ],
    );

    query(&mut connection, SELECT);
    assert_eq!(update(&mut connection, INSERT), Some(1));
    connection
        .get_table_schema(None, None, "MockTable")
        .unwrap();

    let high = v1::request_options::Priority::High as i32;

    let queries = queries.lock().unwrap();
    let user = queries.iter().find(|q| q.sql == SELECT);
    let user_options = user.unwrap().request_options.as_ref().unwrap();
    assert_eq!(user_options.priority, high);
    assert_eq!(user_options.request_tag, "user-req");

    let metadata: Vec<_> = queries.iter().filter(|q| q.sql != SELECT).collect();
    assert_eq!(
        metadata.len(),
        2,
        "get_table_schema issues its query and an INFORMATION_SCHEMA lookup"
    );
    for read in metadata {
        let metadata_options = read.request_options.clone().unwrap_or_default();
        assert_eq!(metadata_options.priority, high);
        assert_eq!(metadata_options.request_tag, "");
        assert_eq!(metadata_options.transaction_tag, "");
    }

    let batch_dml = batch_dml.lock().unwrap();
    assert_eq!(batch_dml.len(), 1);
    let dml_options = batch_dml[0].request_options.as_ref().unwrap();
    assert_eq!(dml_options.priority, high);
    assert_eq!(dml_options.request_tag, "user-req");

    let commits = commits.lock().unwrap();
    assert_eq!(commits.len(), 1);
    let commit_options = commits[0].request_options.as_ref().unwrap();
    assert_eq!(commit_options.priority, high);
    assert_eq!(commit_options.transaction_tag, "user-txn");
}

/// The client stack (channel pool, credentials, session, maintenance task) is expensive, so a
/// database builds it lazily and shares it between connections. Setting any database option
/// discards it. The client creates one session per `DatabaseClient`, so sessions count stacks.
#[test]
fn connections_share_one_client_stack_until_an_option_is_set() {
    let sessions = Arc::new(AtomicUsize::new(0));
    let count = sessions.clone();
    let mut mock = MockSpanner::new();
    mock.expect_create_session().returning(move |request| {
        let n = count.fetch_add(1, Ordering::SeqCst) + 1;
        let database = request.into_inner().database;
        Ok(tonic::Response::new(v1::Session {
            name: format!("{database}/sessions/mock-session-{n}"),
            multiplexed: true,
            ..Default::default()
        }))
    });
    let server = MockServer::serve(mock);
    let mut database = emulator_database(&server.endpoint);

    assert_eq!(sessions.load(Ordering::SeqCst), 0);

    let _first = database.new_connection().unwrap();
    let _second = database.new_connection().unwrap();
    assert_eq!(sessions.load(Ordering::SeqCst), 1);

    let rendered = format!("{database:?}");
    assert!(
        rendered.contains(r#"connected: Some("<client stack>")"#),
        "{rendered}"
    );
    assert!(!rendered.contains("DatabaseClient"), "{rendered}");

    // Even re-setting the same value invalidates the cache.
    database
        .set_option(OPTION_ENDPOINT.into(), server.endpoint.clone().into())
        .unwrap();
    let _third = database.new_connection().unwrap();
    assert_eq!(sessions.load(Ordering::SeqCst), 2);
}

/// Generic clients set every option to its default, so `adbc.statement.exec.incremental=false` is
/// accepted. Enabling it is not implemented.
#[test]
fn exec_incremental_spec_default_is_a_no_op() {
    let server = MockServer::serve(MockSpanner::new());
    let mut connection = server.connect();
    let mut statement = connection.new_statement().unwrap();

    statement
        .set_option(OptionStatement::Incremental, false.into())
        .unwrap();
    assert_eq!(
        statement
            .get_option_string(OptionStatement::Incremental)
            .unwrap(),
        "false"
    );

    let error = statement
        .set_option(OptionStatement::Incremental, true.into())
        .unwrap_err();
    assert_eq!(error.status, AdbcStatus::NotImplemented);
    assert!(
        error.message.contains("adbc.statement.exec.incremental"),
        "{error}"
    );
}

// The pinned client retries unary RPCs in gax's retry loop, where both limits are exact. Stream
// resumption builds a fresh `RetryState` per resume, so the attempt limit is exact but the
// elapsed-time limit never fires. These tests catch client updates that change either; see
// `src/retry.rs`.

/// Attempts a probe serves before failing permanently, so an inert limit cannot hang the test.
const RETRY_PROBE_CAP: usize = 20;

/// A probe's statement with `options` over a constant 50 ms backoff instead of the default 1 s
/// doubling. With full jitter, the probe cap takes about half a second to reach.
fn retry_probe(
    connection: &mut SpannerConnection,
    sql: &str,
    options: Vec<(&str, OptionValue)>,
) -> SpannerStatement {
    let mut statement = prepare(connection, sql);
    let backoff = [
        (OPTION_RETRY_BACKOFF_INITIAL_SECONDS, 0.05.into()),
        (OPTION_RETRY_BACKOFF_MULTIPLIER, 1.0.into()),
    ];
    for (key, value) in backoff.into_iter().chain(options) {
        statement.set_option(key.into(), value).unwrap();
    }
    statement
}

/// Counts `ExecuteStreamingSql` attempts with retry `options`.
fn streaming_attempts(options: Vec<(&str, OptionValue)>) -> usize {
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_in_mock = calls.clone();
    let mut mock = MockSpanner::new();
    mock.expect_execute_streaming_sql().returning(move |_| {
        let n = calls_in_mock.fetch_add(1, Ordering::SeqCst);
        if n >= RETRY_PROBE_CAP {
            return Err(tonic::Status::internal("probe cap reached"));
        }
        // A resumed stream does not repeat the metadata.
        Ok(stream_of(vec![
            Ok(partial_result_set(n == 0, &["v1"], b"rt-1", false)),
            Err(tonic::Status::unavailable("connection lost mid-stream")),
        ]))
    });
    let server = MockServer::serve(mock);
    let mut statement = retry_probe(&mut server.connect(), SELECT, options);
    statement.execute().err().unwrap();
    calls.load(Ordering::SeqCst)
}

/// Counts `ExecuteBatchDml` attempts with retry `options`.
fn unary_attempts(options: Vec<(&str, OptionValue)>) -> usize {
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_in_mock = calls.clone();
    let mut mock = MockSpanner::new();
    mock.expect_execute_batch_dml().returning(move |_| {
        if calls_in_mock.fetch_add(1, Ordering::SeqCst) >= RETRY_PROBE_CAP {
            return Err(tonic::Status::internal("probe cap reached"));
        }
        Err(tonic::Status::unavailable("backend unavailable"))
    });
    let server = MockServer::serve(mock);
    let mut statement = retry_probe(&mut server.connect(), UPDATE, options);
    statement.execute_update().unwrap_err();
    calls.load(Ordering::SeqCst)
}

#[test]
fn retry_max_attempts_is_exact_on_unary_rpcs() {
    for max_attempts in [1_i64, 2, 3] {
        let attempts = unary_attempts(vec![(OPTION_RETRY_MAX_ATTEMPTS, max_attempts.into())]);
        assert_eq!(attempts, max_attempts as usize);
    }
}

#[test]
fn retry_max_attempts_is_exact_on_the_streaming_path() {
    for max_attempts in [1_i64, 2, 3] {
        let attempts = streaming_attempts(vec![(OPTION_RETRY_MAX_ATTEMPTS, max_attempts.into())]);
        assert_eq!(
            attempts, max_attempts as usize,
            "a change here means the pinned client's stream-resume accounting moved, so update \
             src/retry.rs and docs/options.md to match"
        );
    }
}

/// `spanner.retry.max_elapsed_seconds` bounds unary retries but not stream resumption. Streaming
/// callers can use `spanner.rpc.timeout_seconds.{query,fetch}` instead.
#[test]
fn retry_max_elapsed_seconds_bounds_unary_rpcs_but_is_inert_on_the_streaming_path() {
    let unbounded = unary_attempts(vec![(OPTION_RETRY_MAX_ELAPSED_SECONDS, "".into())]);
    assert_eq!(unbounded, RETRY_PROBE_CAP + 1);

    // With full jitter the count varies. At least two attempts shows the budget is not so small
    // that it stops at the first failure.
    let unary = unary_attempts(vec![(OPTION_RETRY_MAX_ELAPSED_SECONDS, 0.15.into())]);
    assert!((2..unbounded).contains(&unary), "{unary} of {unbounded}");

    let streaming = streaming_attempts(vec![(OPTION_RETRY_MAX_ELAPSED_SECONDS, 0.05.into())]);
    assert_eq!(
        streaming,
        RETRY_PROBE_CAP + 1,
        "a change here means the pinned client's stream-resume accounting moved, so update \
         src/retry.rs and docs/options.md to match"
    );
}

/// `get_statistics` runs discovery and every aggregate scan in one read-only transaction, so a
/// concurrently created table cannot fail the call and all counts share a timestamp.
#[test]
fn get_statistics_shares_one_read_only_transaction() {
    let requests = Log::default();
    let recorded = requests.clone();
    let mut mock = MockSpanner::new();
    mock.expect_execute_streaming_sql()
        .returning(move |request| {
            let request = request.into_inner();
            let inline_begin = begins_inline(request.transaction.as_ref());
            // Columns are read by position, so only their count and types matter.
            let (fields, values): (&[(&str, v1::TypeCode)], &[&str]) =
                if request.sql.contains("INFORMATION_SCHEMA") {
                    (
                        &[
                            ("TABLE_SCHEMA", v1::TypeCode::String),
                            ("TABLE_NAME", v1::TypeCode::String),
                            ("COLUMN_NAME", v1::TypeCode::String),
                            ("SPANNER_TYPE", v1::TypeCode::String),
                        ],
                        &["", "MockTable", "c", "STRING(MAX)"],
                    )
                } else {
                    // COUNT(*), COUNTIF(c IS NULL), COUNT(DISTINCT c)
                    (
                        &[
                            ("n", v1::TypeCode::Int64),
                            ("nulls", v1::TypeCode::Int64),
                            ("distinct", v1::TypeCode::Int64),
                        ],
                        &["3", "0", "3"],
                    )
                };
            recorded.lock().unwrap().push(request);
            let mut result = v1::PartialResultSet {
                metadata: Some(result_metadata(fields)),
                values: values.iter().map(|s| string_value(s)).collect(),
                resume_token: b"stat-1".to_vec(),
                last: true,
                ..Default::default()
            };
            if inline_begin {
                set_transaction_id(&mut result, b"stats-ro-txn");
            }
            Ok(stream_of(vec![Ok(result)]))
        });
    let server = MockServer::serve(mock);

    let reader = server
        .connect()
        .get_statistics(None, None, None, false)
        .unwrap();
    assert_eq!(reader.collect::<Result<Vec<_>, _>>().unwrap().len(), 1);

    let selectors = selectors(&requests);
    assert!(selectors.len() >= 2, "discovery and an aggregate scan");
    assert_shared_read_only(&selectors, b"stats-ro-txn");
}

/// The catalog is the database id, and catalog arguments accept it. `get_objects` at `Catalogs`
/// depth issues no RPC.
#[test]
fn the_catalog_level_reports_the_database_id_and_filters_round_trip() {
    let server = MockServer::serve(MockSpanner::new());
    let mut connection = server.connect();

    let catalogs = |connection: &SpannerConnection, filter: Option<&str>| -> Vec<String> {
        let reader = connection
            .get_objects(ObjectDepth::Catalogs, filter, None, None, None, None)
            .unwrap();
        let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
        let names = batches.iter().flat_map(|batch| {
            let column = batch.column(0).as_string::<i32>();
            column
                .iter()
                .map(|name| name.unwrap().to_owned())
                .collect::<Vec<_>>()
        });
        names.collect()
    };

    assert_eq!(catalogs(&connection, None), ["d"]);
    assert_eq!(catalogs(&connection, Some("d")), ["d"]);
    assert_eq!(catalogs(&connection, Some("d%")), ["d"]);
    assert!(catalogs(&connection, Some("other-db")).is_empty());
    // "" selects objects without a catalog.
    assert!(catalogs(&connection, Some("")).is_empty());

    assert_eq!(
        connection
            .get_option_string(OptionConnection::CurrentCatalog)
            .unwrap(),
        "d"
    );
    connection
        .set_option(OptionConnection::CurrentCatalog, "d".into())
        .unwrap();
    let rejected = connection
        .set_option(OptionConnection::CurrentCatalog, "other-db".into())
        .unwrap_err();
    assert_eq!(rejected.status, AdbcStatus::NotImplemented);

    let mut statement = connection.new_statement().unwrap();
    statement
        .set_option(OptionStatement::TargetCatalog, "d".into())
        .unwrap();
    let rejected = statement
        .set_option(OptionStatement::TargetCatalog, "".into())
        .unwrap_err();
    assert_eq!(rejected.status, AdbcStatus::NotImplemented);
}

#[test]
fn ingest_operational_errors_keep_status_and_details_without_metadata_probes() {
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
        let mut response = batch_write_group_already_exists(0);
        let status = response.status.as_mut().unwrap();
        status.code = code as i32;
        status.message = "injected operational failure".into();

        let mut mock = MockSpanner::new();
        serve_batch_write(&mut mock, vec![response]);
        // An existence probe must not replace an operational error with AlreadyExists or discard
        // its vendor code and structured details.
        let probes = serve_queries(&mut mock, &["MockTable"]);
        let server = MockServer::serve(mock);

        let mut connection = server.connect();
        let error = ingest(batch_write_ingest_statement(&mut connection), 1).unwrap_err();
        assert_eq!(error.status, expected, "{code:?}: {error}");
        assert_eq!(error.vendor_code, code as i32, "{error}");
        assert!(
            error.message.contains("injected operational failure"),
            "{code:?}: {error}"
        );
        let detail = error_detail(&error, "google.rpc.errorinfo");
        assert_eq!(detail["reason"], "DUPLICATE_KEY", "{code:?}");
        assert_eq!(probes.lock().unwrap().len(), 0, "{code:?}");
    }
}

#[test]
fn cancel_before_first_batch_discards_rows_fetched_by_execute() {
    let (server, _) = query_server(&["first", "second"]);
    let mut connection = server.connect();
    let mut statement = prepare(&mut connection, SELECT);
    let mut reader = statement.execute().unwrap();
    statement.get_cancel_handle().try_cancel().unwrap();

    let error = reader.next().unwrap().unwrap_err();
    assert_eq!(driver_error(&error).status, AdbcStatus::Cancelled);
    assert!(reader.next().is_none());
}

#[test]
fn conversion_error_ends_the_reader_in_first_and_later_batches() {
    for bad_row in 0..2 {
        let mut values = vec![string_value("1"), string_value("2"), string_value("3")];
        values[bad_row] = string_value("invalid integer");
        let mut mock = MockSpanner::new();
        serve_result(
            &mut mock,
            result_metadata(&[("c", v1::TypeCode::Int64)]),
            values,
        );
        let server = MockServer::serve(mock);
        let mut connection = server.connect();
        let mut reader = one_row_batches(&mut connection).execute().unwrap();
        for _ in 0..bad_row {
            assert_eq!(reader.next().unwrap().unwrap().num_rows(), 1);
        }
        let error = reader.next().unwrap().unwrap_err();
        assert!(error.to_string().contains("INT64"), "{error}");
        assert!(reader.next().is_none(), "bad row {bad_row}");
    }
}

#[test]
fn interrupted_batch_write_reports_confirmed_rows_and_unknown_outcome() {
    for cancel_operation in [false, true] {
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let mut mock = MockSpanner::new();
        mock.expect_batch_write().returning(move |_| {
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            tx.try_send(Ok(batch_write_groups_ok(vec![0, 1]))).unwrap();
            let _ = started_tx.send(());
            // Keep the stream open after two confirmed writes; group 2 has no outcome yet.
            tokio::spawn(async move { tx.closed().await });
            Ok(tonic::Response::new(rx))
        });
        let server = MockServer::serve(mock);
        let mut connection = server.connect();
        let mut statement = batch_write_ingest_statement(&mut connection);
        // The cancel case gets a deadline it cannot reach, so a slow cancel cannot turn into Timeout.
        let deadline = if cancel_operation { 60.0 } else { 1.0 };
        statement
            .set_option(OPTION_RPC_TIMEOUT_UPDATE.into(), deadline.into())
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
        let expected = if cancel_operation {
            AdbcStatus::Cancelled
        } else {
            AdbcStatus::Timeout
        };
        assert_eq!(error.status, expected, "{error}");
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
    // The probe finds the ingested column.
    let (server, queries) = query_server(&["c"]);
    let mut connection = server.connect();
    configure(
        &mut connection,
        &[
            (OPTION_REQUEST_PRIORITY, "high"),
            (OPTION_REQUEST_TAG, "user-request"),
            (OPTION_TRANSACTION_TAG, "user-transaction"),
            (OPTION_DIRECTED_READ, "include:us-east1"),
        ],
    );
    let count = ingest(append_ingest_statement(&mut connection), 0);
    assert_eq!(count.unwrap(), Some(0));
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
    let attempts = Arc::new(AtomicUsize::new(0));
    let recorded = attempts.clone();
    let mut mock = MockSpanner::new();
    mock.expect_execute_streaming_sql().returning(move |_| {
        if recorded.fetch_add(1, Ordering::SeqCst) == 0 {
            Err(tonic::Status::unavailable("probe unavailable"))
        } else {
            let result = partial_result_set(true, &["MockTable"], b"t", true);
            Ok(stream_of(vec![Ok(result)]))
        }
    });
    let server = MockServer::serve(mock);
    let mut connection = server.connect();
    connection
        .set_option(OPTION_RETRY_MAX_ATTEMPTS.into(), 1_i64.into())
        .unwrap();
    // A failed existence probe preserves the empty ingest's success, but must not retry.
    let count = ingest(append_ingest_statement(&mut connection), 0);
    assert_eq!(count.unwrap(), Some(0));
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

#[test]
fn batch_write_honors_retry_attempt_limit() {
    for limit in [1_i64, 3] {
        let attempts = Arc::new(AtomicUsize::new(0));
        let recorded = attempts.clone();
        let mut mock = MockSpanner::new();
        mock.expect_batch_write().returning(move |_| {
            recorded.fetch_add(1, Ordering::SeqCst);
            Err(tonic::Status::unavailable("batch write unavailable"))
        });
        let server = MockServer::serve(mock);
        let mut connection = server.connect();
        let mut statement = batch_write_ingest_statement(&mut connection);
        for (key, value) in [
            (OPTION_RETRY_MAX_ATTEMPTS, limit.into()),
            (OPTION_RETRY_BACKOFF_INITIAL_SECONDS, 0.001.into()),
            (OPTION_RPC_TIMEOUT_UPDATE, 2.0.into()),
        ] {
            statement.set_option(key.into(), value).unwrap();
        }
        let error = ingest(statement, 1).unwrap_err();
        assert_eq!(error.status, AdbcStatus::IO, "{error}");
        assert_eq!(attempts.load(Ordering::SeqCst), limit as usize);
    }
}

#[test]
fn query_consumes_empty_stream_ingest_schema() {
    let (server, _) = query_server(&["row"]);
    let mut connection = server.connect();
    for discard in [false, true] {
        let mut statement = connection.new_statement().unwrap();
        statement
            .bind_stream(Box::new(RecordBatchIterator::new(
                std::iter::empty::<Result<RecordBatch, ArrowError>>(),
                ingest_batch(0).schema(),
            )))
            .unwrap();
        statement.set_sql_query(SELECT).unwrap();
        if discard {
            assert_eq!(statement.execute_update().unwrap(), None);
        } else {
            assert_eq!(row_count(&fetch(&mut statement)), 1);
        }
        target_mock_table(&mut statement);
        let error = statement.execute_update().unwrap_err();
        assert_eq!(error.status, AdbcStatus::InvalidState, "{error}");
        assert!(error.message.contains("no data has been bound"), "{error}");
    }
}

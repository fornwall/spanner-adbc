//! Offline tests for isolation levels, the fixed current catalog/schema, partition descriptors,
//! and metadata reads against a mock server.

use adbc_core::error::Status;
use adbc_core::options::OptionValue;
use google_cloud_spanner::model::transaction_options::IsolationLevel;

use super::{
    check_fixed_catalog_or_schema, decode_partition, encode_partition, isolation_to_adbc_string,
    parse_isolation_level,
};

/// Every ADBC level maps to a Spanner level, and the getter reports the effective one.
#[test]
fn isolation_levels_map_to_the_effective_spanner_level() {
    use adbc_core::constants::*;
    let cases = [
        (
            ADBC_OPTION_ISOLATION_LEVEL_DEFAULT,
            IsolationLevel::Unspecified,
            ADBC_OPTION_ISOLATION_LEVEL_DEFAULT,
        ),
        (
            ADBC_OPTION_ISOLATION_LEVEL_SERIALIZABLE,
            IsolationLevel::Serializable,
            ADBC_OPTION_ISOLATION_LEVEL_SERIALIZABLE,
        ),
        (
            ADBC_OPTION_ISOLATION_LEVEL_LINEARIZABLE,
            IsolationLevel::Serializable,
            ADBC_OPTION_ISOLATION_LEVEL_SERIALIZABLE,
        ),
        (
            ADBC_OPTION_ISOLATION_LEVEL_REPEATABLE_READ,
            IsolationLevel::RepeatableRead,
            ADBC_OPTION_ISOLATION_LEVEL_REPEATABLE_READ,
        ),
        (
            ADBC_OPTION_ISOLATION_LEVEL_SNAPSHOT,
            IsolationLevel::RepeatableRead,
            ADBC_OPTION_ISOLATION_LEVEL_REPEATABLE_READ,
        ),
        (
            ADBC_OPTION_ISOLATION_LEVEL_READ_COMMITTED,
            IsolationLevel::RepeatableRead,
            ADBC_OPTION_ISOLATION_LEVEL_REPEATABLE_READ,
        ),
        (
            ADBC_OPTION_ISOLATION_LEVEL_READ_UNCOMMITTED,
            IsolationLevel::RepeatableRead,
            ADBC_OPTION_ISOLATION_LEVEL_REPEATABLE_READ,
        ),
    ];
    for (requested, level, effective) in cases {
        let parsed = parse_isolation_level(OptionValue::String(requested.into())).unwrap();
        assert_eq!(parsed, level, "{requested}");
        assert_eq!(isolation_to_adbc_string(&parsed), effective, "{requested}");
    }
    for invalid in [
        OptionValue::String("serializable".into()),
        OptionValue::Int(1),
    ] {
        let error = parse_isolation_level(invalid).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        assert!(
            error
                .message
                .contains(ADBC_CONNECTION_OPTION_ISOLATION_LEVEL),
            "{}",
            error.message
        );
    }
}

#[test]
fn setting_current_catalog_or_schema_accepts_only_the_reported_value() {
    use adbc_core::constants::{
        ADBC_CONNECTION_OPTION_CURRENT_CATALOG, ADBC_CONNECTION_OPTION_CURRENT_DB_SCHEMA,
    };
    let set = |s: &str| {
        check_fixed_catalog_or_schema(
            OptionValue::String(s.to_string()),
            ADBC_CONNECTION_OPTION_CURRENT_CATALOG,
            "adbc-test",
        )
    };
    assert!(set("adbc-test").is_ok());
    assert!(
        check_fixed_catalog_or_schema(
            OptionValue::String(String::new()),
            ADBC_CONNECTION_OPTION_CURRENT_DB_SCHEMA,
            ""
        )
        .is_ok()
    );
    let err = set("foo").unwrap_err();
    assert_eq!(err.status, Status::NotImplemented);
    assert!(err.message.contains("\"foo\""), "{}", err.message);
    assert!(err.message.contains("\"adbc-test\""), "{}", err.message);
    let err = check_fixed_catalog_or_schema(
        OptionValue::Int(1),
        ADBC_CONNECTION_OPTION_CURRENT_DB_SCHEMA,
        "",
    )
    .unwrap_err();
    assert_eq!(err.status, Status::InvalidArguments);
    assert!(
        err.message
            .contains(ADBC_CONNECTION_OPTION_CURRENT_DB_SCHEMA),
        "{}",
        err.message
    );
}

/// `read_partition` takes caller-supplied bytes, so decoding must reject anything else with
/// `InvalidArguments`, and a descriptor of another version on its version, not its payload.
/// `lib.rs`'s fuzzing oracle checks the accepted seeds' round trip.
#[test]
fn malformed_partition_descriptors_are_invalid_arguments() {
    let cases: [(&[u8], &str); 12] = [
        (b"", "invalid partition descriptor: EOF"),
        (b"\xff\xfe\x00 not json", "invalid partition descriptor"),
        (b"{", "invalid partition descriptor: EOF"),
        (b"[1, 2, 3]", "missing \"v\" version field"),
        (
            br#"{"inner":{"Query":{"sql":"SELECT 1"}}}"#,
            "missing \"v\" version field",
        ),
        (
            br#"{"v":"one"}"#,
            "version \"one\" is not an unsigned integer",
        ),
        (br#"{"v":-1}"#, "version -1 is not an unsigned integer"),
        (
            br#"{"v":2,"partition":{"future":"format"}}"#,
            "version 2 is not supported by this driver, which reads version 1",
        ),
        (br#"{"v":0}"#, "version 0 is not supported by this driver"),
        (br#"{"v":1}"#, "missing \"partition\" field"),
        (br#"{"v":1,"partition":{}}"#, "invalid partition descriptor"),
        (
            br#"{"v":1,"partition":{"version":1,"inner":{"Query":{"sql":"SELECT 1"}}}}"#,
            "missing session",
        ),
    ];
    for (descriptor, expected) in cases {
        let error = decode_partition(descriptor).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "{descriptor:?}");
        assert!(
            error.message.contains(expected),
            "{descriptor:?}: {}",
            error.message
        );
    }
}

#[test]
fn partition_descriptor_preserves_fallback_types_and_reader_precision() {
    use crate::conversion::TimestampPrecision;
    use arrow_schema::DataType;
    use google_cloud_spanner::model::{StructType, Type, TypeCode, struct_type};

    let mut descriptor = decode_partition(include_bytes!(
        "../../fuzz/seeds/partition/enveloped-query-descriptor"
    ))
    .unwrap();
    assert!(
        descriptor
            .schema(TimestampPrecision::default())
            .unwrap()
            .is_none()
    );
    descriptor.row_type = Some(
        StructType::new().set_fields([
            struct_type::Field::new()
                .set_name("ts")
                .set_type(Type::new().set_code(TypeCode::Timestamp)),
            struct_type::Field::new()
                .set_name("payload")
                .set_type(Type::new().set_code(TypeCode::Json)),
        ]),
    );
    let bytes = encode_partition(&descriptor).unwrap();
    let decoded = decode_partition(&bytes).unwrap();
    assert_eq!(encode_partition(&decoded).unwrap(), bytes);
    let schema = decoded
        .schema(TimestampPrecision::Microseconds)
        .unwrap()
        .unwrap();
    assert_eq!(
        schema.field(0).data_type(),
        &DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into()))
    );
    assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
    assert_eq!(
        schema.field(1).metadata()["ARROW:extension:name"],
        "arrow.json"
    );

    let mut malformed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    malformed["row_type"] = serde_json::json!(17);
    let error = decode_partition(&serde_json::to_vec(&malformed).unwrap()).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
}

/// `get_table_schema` against an in-process mock Spanner server, for its existence probe.
mod table_schema_probe {
    use std::sync::{Arc, Mutex};

    use adbc_core::error::Status;
    use adbc_core::options::{OptionConnection, OptionValue};
    use adbc_core::{Connection, Optionable};
    use spanner_grpc_mock::MockSpanner;
    use spanner_grpc_mock::google::spanner::v1;

    use crate::SpannerConnection;

    pub(super) type Requests = Arc<Mutex<Vec<v1::ExecuteSqlRequest>>>;
    pub(super) type Sender = tokio::sync::mpsc::Sender<tonic::Result<v1::PartialResultSet>>;

    /// What the mock answers to the `SELECT * ... LIMIT 0` schema query.
    enum SchemaQuery {
        Fails(tonic::Code),
        /// Never answers, so the query timeout fires.
        Silent,
    }

    pub(super) struct MockServer {
        server: crate::mock_server::MockServer,
        pub(super) requests: Requests,
    }

    /// Serves sessions, answers the schema query per `schema_query`, and answers any
    /// `INFORMATION_SCHEMA` probe with no rows: the table does not exist.
    fn start(schema_query: SchemaQuery) -> MockServer {
        let requests = Requests::default();
        let silent: Arc<Mutex<Vec<Sender>>> = Arc::default();
        let mut mock = MockSpanner::new();
        let record = requests.clone();
        mock.expect_execute_streaming_sql()
            .returning(move |request| {
                let request = request.into_inner();
                let probe = request.sql.contains("INFORMATION_SCHEMA");
                record.lock().unwrap().push(request);
                let (tx, rx) = tokio::sync::mpsc::channel(1);
                if probe {
                    tx.try_send(Ok(v1::PartialResultSet {
                        metadata: Some(v1::ResultSetMetadata {
                            row_type: Some(v1::StructType {
                                fields: vec![v1::struct_type::Field {
                                    name: "TABLE_NAME".into(),
                                    r#type: Some(v1::Type {
                                        code: v1::TypeCode::String as i32,
                                        ..Default::default()
                                    }),
                                }],
                            }),
                            ..Default::default()
                        }),
                        last: true,
                        ..Default::default()
                    }))
                    .unwrap();
                    return Ok(tonic::Response::new(rx));
                }
                match &schema_query {
                    SchemaQuery::Fails(code) => Err(tonic::Status::new(*code, "mock failure")),
                    SchemaQuery::Silent => {
                        silent.lock().unwrap().push(tx);
                        Ok(tonic::Response::new(rx))
                    }
                }
            });
        serve(mock, requests)
    }

    /// Serves `mock`'s expectations, which record into `requests`.
    pub(super) fn serve(mock: MockSpanner, requests: Requests) -> MockServer {
        MockServer {
            server: crate::mock_server::MockServer::serve(mock),
            requests,
        }
    }

    pub(super) fn connect(server: &MockServer) -> SpannerConnection {
        server.server.connect()
    }

    fn single_use_bound(
        request: &v1::ExecuteSqlRequest,
    ) -> Option<v1::transaction_options::read_only::TimestampBound> {
        let Some(v1::transaction_selector::Selector::SingleUse(options)) = request
            .transaction
            .as_ref()
            .and_then(|t| t.selector.as_ref())
        else {
            panic!("expected a single-use read: {request:?}");
        };
        let Some(v1::transaction_options::Mode::ReadOnly(read_only)) = &options.mode else {
            panic!("expected read-only options: {options:?}");
        };
        read_only.timestamp_bound
    }

    /// A table created after a stale read's timestamp is missing at that timestamp, so the probe
    /// must read at the same bound and report NotFound.
    #[test]
    fn probe_reads_at_the_connections_staleness() {
        use v1::transaction_options::read_only::TimestampBound as WireBound;

        let server = start(SchemaQuery::Fails(tonic::Code::InvalidArgument));
        let mut connection = connect(&server);
        connection
            .set_option(
                OptionConnection::Other(crate::OPTION_READ_STALENESS.into()),
                OptionValue::String("exact:10s".into()),
            )
            .unwrap();
        let error = connection
            .get_table_schema(None, None, "Missing")
            .unwrap_err();
        assert_eq!(error.status, Status::NotFound, "{}", error.message);

        let requests = server.requests.lock().unwrap();
        assert_eq!(requests.len(), 2, "the schema query and one probe");
        let expected = Some(WireBound::ExactStaleness(prost_types::Duration {
            seconds: 10,
            nanos: 0,
        }));
        assert_eq!(single_use_bound(&requests[0]), expected);
        assert!(requests[1].sql.contains("INFORMATION_SCHEMA"));
        assert_eq!(single_use_bound(&requests[1]), expected);
    }

    /// Spanner resolves names case-insensitively, but `INFORMATION_SCHEMA` keeps the declared
    /// case, so the probe must lowercase both sides; otherwise it would report `singers` missing
    /// while `Singers` exists. The mock can't evaluate SQL, so check the probe's text.
    #[test]
    fn probe_compares_names_case_insensitively() {
        let server = start(SchemaQuery::Fails(tonic::Code::InvalidArgument));
        let error = connect(&server)
            .get_table_schema(None, Some("App"), "singers")
            .unwrap_err();
        assert_eq!(error.status, Status::NotFound, "{}", error.message);
        assert!(
            error.message.contains("table `App`.`singers` not found"),
            "the message names the schema: {}",
            error.message
        );

        let requests = server.requests.lock().unwrap();
        let probe = &requests[1];
        for comparison in [
            "LOWER(TABLE_SCHEMA) = LOWER(@schema)",
            "LOWER(TABLE_NAME) = LOWER(@table)",
        ] {
            assert!(probe.sql.contains(comparison), "{}", probe.sql);
        }
        let params = &probe.params.as_ref().expect("bound names").fields;
        for (name, value) in [("schema", "App"), ("table", "singers")] {
            assert_eq!(
                params[name].kind,
                Some(prost_types::value::Kind::StringValue(value.into())),
                "{name}"
            );
        }
    }

    /// Only a missing-table status is refined. Probing after a timeout would add a second query
    /// deadline, and after a permission error the probe cannot see the table.
    #[test]
    fn probe_is_skipped_for_other_failures() {
        let server = start(SchemaQuery::Fails(tonic::Code::PermissionDenied));
        let error = connect(&server)
            .get_table_schema(None, None, "Hidden")
            .unwrap_err();
        assert_eq!(error.status, Status::Unauthorized, "{}", error.message);
        assert_eq!(server.requests.lock().unwrap().len(), 1, "no probe");

        let server = start(SchemaQuery::Silent);
        let mut connection = connect(&server);
        connection
            .set_option(
                OptionConnection::Other(crate::OPTION_RPC_TIMEOUT_QUERY.into()),
                OptionValue::Double(0.2),
            )
            .unwrap();
        let error = connection.get_table_schema(None, None, "Slow").unwrap_err();
        assert_eq!(error.status, Status::Timeout, "{}", error.message);
        assert_eq!(server.requests.lock().unwrap().len(), 1, "no probe");
    }
}

/// `get_statistics` against a mock server whose every query answers after a delay.
mod statistics_deadline {
    use std::time::Duration;

    use adbc_core::error::Status;
    use adbc_core::options::{OptionConnection, OptionValue};
    use adbc_core::{Connection, Optionable};
    use spanner_grpc_mock::MockSpanner;
    use spanner_grpc_mock::google::spanner::v1;

    use super::table_schema_probe::{Requests, connect, serve};

    const DELAY: Duration = Duration::from_millis(200);

    fn wire_type(code: v1::TypeCode) -> Option<v1::Type> {
        Some(v1::Type {
            code: code as i32,
            ..Default::default()
        })
    }

    /// One table `t` with no visible columns, so the only scan is `COUNT(*)`.
    fn delayed_response(request: &v1::ExecuteSqlRequest) -> v1::PartialResultSet {
        use v1::TypeCode::{Int64, String};
        let (fields, values): (Vec<(&str, v1::TypeCode)>, Vec<Option<&str>>) =
            if request.sql.contains("INFORMATION_SCHEMA") {
                let names = ["TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "SPANNER_TYPE"];
                (
                    names.map(|name| (name, String)).to_vec(),
                    vec![Some(""), Some("t"), None, None],
                )
            } else {
                (vec![("n", Int64)], vec![Some("3")])
            };
        let inline_begin = matches!(
            request
                .transaction
                .as_ref()
                .and_then(|t| t.selector.as_ref()),
            Some(v1::transaction_selector::Selector::Begin(_))
        );
        v1::PartialResultSet {
            metadata: Some(v1::ResultSetMetadata {
                row_type: Some(v1::StructType {
                    fields: fields
                        .into_iter()
                        .map(|(name, code)| v1::struct_type::Field {
                            name: name.into(),
                            r#type: wire_type(code),
                        })
                        .collect(),
                }),
                // The client needs the id of a transaction begun inline.
                transaction: inline_begin.then(|| v1::Transaction {
                    id: b"stats-txn".to_vec(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            values: values
                .into_iter()
                .map(|value| prost_types::Value {
                    kind: Some(match value {
                        Some(value) => prost_types::value::Kind::StringValue(value.into()),
                        None => prost_types::value::Kind::NullValue(0),
                    }),
                })
                .collect(),
            last: true,
            ..Default::default()
        }
    }

    /// The query timeout is one deadline for the whole call. Discovery and the scan each fit in
    /// it, but together they don't.
    #[test]
    fn one_query_deadline_covers_discovery_and_scans() {
        let requests = Requests::default();
        let mut mock = MockSpanner::new();
        let record = requests.clone();
        mock.expect_execute_streaming_sql()
            .returning(move |request| {
                let request = request.into_inner();
                let response = delayed_response(&request);
                record.lock().unwrap().push(request);
                let (tx, rx) = tokio::sync::mpsc::channel(1);
                tokio::spawn(async move {
                    tokio::time::sleep(DELAY).await;
                    let _ = tx.send(Ok(response)).await;
                });
                Ok(tonic::Response::new(rx))
            });
        let server = serve(mock, requests);
        let mut connection = connect(&server);

        let statistics = connection.get_statistics(None, None, None, false);
        assert!(statistics.is_ok(), "no deadline: {:?}", statistics.err());
        assert_eq!(
            server.requests.lock().unwrap().len(),
            2,
            "discovery, one scan"
        );

        connection
            .set_option(
                OptionConnection::Other(crate::OPTION_RPC_TIMEOUT_QUERY.into()),
                OptionValue::Double(1.5 * DELAY.as_secs_f64()),
            )
            .unwrap();
        let error = connection
            .get_statistics(None, None, None, false)
            .err()
            .expect("two sequential delays exceed one deadline");
        assert_eq!(error.status, Status::Timeout, "{}", error.message);
    }
}

/// `read_partition` against a mock server whose partition stream always breaks mid-stream.
mod partition_retry {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use adbc_core::options::{OptionConnection, OptionValue};
    use adbc_core::{Connection, Optionable};
    use spanner_grpc_mock::MockSpanner;
    use spanner_grpc_mock::google::spanner::v1;

    use super::table_schema_probe::{Requests, connect, serve};

    /// Attempts served before failing permanently, so an ignored limit cannot hang the test.
    const CAP: usize = 4;

    fn rows(with_metadata: bool) -> v1::PartialResultSet {
        v1::PartialResultSet {
            metadata: with_metadata.then(|| v1::ResultSetMetadata {
                row_type: Some(v1::StructType {
                    fields: vec![v1::struct_type::Field {
                        name: "c".into(),
                        r#type: Some(v1::Type {
                            code: v1::TypeCode::String as i32,
                            ..Default::default()
                        }),
                    }],
                }),
                ..Default::default()
            }),
            values: vec![prost_types::Value {
                kind: Some(prost_types::value::Kind::StringValue("v".into())),
            }],
            resume_token: b"rt".to_vec(),
            ..Default::default()
        }
    }

    /// A descriptor carries no retry policy, so the reading connection's limit must apply.
    #[test]
    fn read_partition_honors_the_reading_connections_retry_limit() {
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();
        let mut mock = MockSpanner::new();
        mock.expect_execute_streaming_sql().returning(move |_| {
            let n = counter.fetch_add(1, Ordering::SeqCst);
            if n >= CAP {
                return Err(tonic::Status::internal("mock: attempt cap reached"));
            }
            let (tx, rx) = tokio::sync::mpsc::channel(2);
            // A resumed stream does not repeat the metadata.
            tx.try_send(Ok(rows(n == 0))).unwrap();
            tx.try_send(Err(tonic::Status::unavailable("mock: connection lost")))
                .unwrap();
            Ok(tonic::Response::new(rx))
        });
        let server = serve(mock, Requests::default());
        let mut connection = connect(&server);
        for (key, value) in [
            (crate::OPTION_RETRY_MAX_ATTEMPTS, OptionValue::Int(2)),
            (
                crate::OPTION_RETRY_BACKOFF_INITIAL_SECONDS,
                OptionValue::Double(0.01),
            ),
        ] {
            connection
                .set_option(OptionConnection::Other(key.into()), value)
                .unwrap();
        }

        let descriptor = include_bytes!("../../fuzz/seeds/partition/enveloped-query-descriptor");
        let failed = match connection.read_partition(descriptor) {
            Ok(reader) => reader.collect::<Result<Vec<_>, _>>().is_err(),
            Err(_) => true,
        };
        assert!(failed, "every attempt breaks, so the read must fail");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}

/// A manual commit against a mock server that rejects every commit as over the mutation limit.
mod manual_commit_limit {
    use std::sync::Arc;

    use adbc_core::constants::ADBC_INGEST_OPTION_MODE_APPEND;
    use adbc_core::error::Status;
    use adbc_core::options::{OptionConnection, OptionStatement, OptionValue};
    use adbc_core::{Connection, Optionable, Statement};
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use spanner_grpc_mock::MockSpanner;
    use spanner_grpc_mock::google::spanner::v1;

    use super::table_schema_probe::{Requests, connect, serve};

    /// Retrying such a commit can't help, so the error says how to get the rows in.
    #[test]
    fn an_oversized_manual_commit_points_to_autocommit_ingest() {
        let mut mock = MockSpanner::new();
        mock.expect_begin_transaction().returning(|_| {
            Ok(tonic::Response::new(v1::Transaction {
                id: b"txn".to_vec(),
                ..Default::default()
            }))
        });
        mock.expect_commit().returning(|_| {
            Err(tonic::Status::invalid_argument(
                "The transaction contains too many mutations. (Maximum number: 80000)",
            ))
        });
        // The append's check that its table exists finds it.
        mock.expect_execute_streaming_sql().returning(|_| {
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            tx.try_send(Ok(v1::PartialResultSet {
                metadata: Some(v1::ResultSetMetadata {
                    row_type: Some(v1::StructType {
                        fields: vec![v1::struct_type::Field {
                            name: "TABLE_NAME".into(),
                            r#type: Some(v1::Type {
                                code: v1::TypeCode::String as i32,
                                ..Default::default()
                            }),
                        }],
                    }),
                    ..Default::default()
                }),
                values: vec![prost_types::Value {
                    kind: Some(prost_types::value::Kind::StringValue("t".into())),
                }],
                last: true,
                ..Default::default()
            }))
            .unwrap();
            Ok(tonic::Response::new(rx))
        });
        let server = serve(mock, Requests::default());
        let mut connection = connect(&server);
        connection
            .set_option(OptionConnection::AutoCommit, "false".into())
            .unwrap();

        let mut ingest = connection.new_statement().unwrap();
        ingest
            .set_option(OptionStatement::TargetTable, "t".into())
            .unwrap();
        ingest
            .set_option(
                OptionStatement::IngestMode,
                ADBC_INGEST_OPTION_MODE_APPEND.into(),
            )
            .unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("Id", DataType::Int64, false)]));
        let rows = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))]);
        ingest.bind(rows.unwrap()).unwrap();
        assert_eq!(ingest.execute_update().unwrap(), None, "buffered");

        let error = connection.commit().unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        assert!(
            error.message.contains("too many mutations")
                && error.message.contains("ingest with autocommit enabled"),
            "{}",
            error.message
        );
        connection.rollback().unwrap();
        connection
            .set_option(OptionConnection::AutoCommit, OptionValue::from("true"))
            .expect("nothing is left to commit");
    }
}

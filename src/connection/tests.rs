//! Offline tests for isolation levels, the fixed current catalog/schema, partition descriptors,
//! and metadata reads against a mock server.

use adbc_core::error::Status;
use adbc_core::options::OptionValue;
use google_cloud_spanner::model::transaction_options::IsolationLevel;

use super::{
    PARTITION_DESCRIPTOR_VERSION, check_fixed_catalog_or_schema, decode_partition,
    encode_partition, isolation_to_adbc_string, parse_isolation_level,
};

#[test]
fn parses_supported_isolation_levels() {
    use adbc_core::constants::*;
    let parse = |s: &str| parse_isolation_level(OptionValue::String(s.to_string()));
    assert_eq!(
        parse(ADBC_OPTION_ISOLATION_LEVEL_SERIALIZABLE).unwrap(),
        IsolationLevel::Serializable
    );
    assert_eq!(
        parse(ADBC_OPTION_ISOLATION_LEVEL_REPEATABLE_READ).unwrap(),
        IsolationLevel::RepeatableRead
    );
    // Spanner's REPEATABLE_READ is snapshot isolation.
    assert_eq!(
        parse(ADBC_OPTION_ISOLATION_LEVEL_SNAPSHOT).unwrap(),
        IsolationLevel::RepeatableRead
    );
    // No level is sent; Spanner defaults to SERIALIZABLE.
    assert_eq!(
        parse(ADBC_OPTION_ISOLATION_LEVEL_DEFAULT).unwrap(),
        IsolationLevel::Unspecified
    );
}

#[test]
fn promotes_unsupported_isolation_levels() {
    use adbc_core::constants::*;
    let parse = |s: &str| parse_isolation_level(OptionValue::String(s.to_string()));
    assert_eq!(
        parse(ADBC_OPTION_ISOLATION_LEVEL_READ_UNCOMMITTED).unwrap(),
        IsolationLevel::RepeatableRead
    );
    assert_eq!(
        parse(ADBC_OPTION_ISOLATION_LEVEL_READ_COMMITTED).unwrap(),
        IsolationLevel::RepeatableRead
    );
    assert_eq!(
        parse(ADBC_OPTION_ISOLATION_LEVEL_LINEARIZABLE).unwrap(),
        IsolationLevel::Serializable
    );
    assert_eq!(
        parse("not-a-level").unwrap_err().status,
        Status::InvalidArguments
    );
    assert_eq!(
        parse_isolation_level(OptionValue::Int(1))
            .unwrap_err()
            .status,
        Status::InvalidArguments
    );
}

#[test]
fn isolation_level_round_trips_to_adbc_string() {
    use adbc_core::constants::*;
    assert_eq!(
        isolation_to_adbc_string(&IsolationLevel::Serializable),
        ADBC_OPTION_ISOLATION_LEVEL_SERIALIZABLE
    );
    assert_eq!(
        isolation_to_adbc_string(&IsolationLevel::RepeatableRead),
        ADBC_OPTION_ISOLATION_LEVEL_REPEATABLE_READ
    );
    assert_eq!(
        isolation_to_adbc_string(&IsolationLevel::Unspecified),
        ADBC_OPTION_ISOLATION_LEVEL_DEFAULT
    );
}

#[test]
fn promoted_isolation_level_round_trips_to_effective_level() {
    use adbc_core::constants::*;
    // `get_option` reports the level that will run, not the one that was set.
    let effective = |s: &str| {
        let level = parse_isolation_level(OptionValue::String(s.to_string())).expect("parses");
        isolation_to_adbc_string(&level)
    };
    assert_eq!(
        effective(ADBC_OPTION_ISOLATION_LEVEL_READ_UNCOMMITTED),
        ADBC_OPTION_ISOLATION_LEVEL_REPEATABLE_READ
    );
    assert_eq!(
        effective(ADBC_OPTION_ISOLATION_LEVEL_READ_COMMITTED),
        ADBC_OPTION_ISOLATION_LEVEL_REPEATABLE_READ
    );
    assert_eq!(
        effective(ADBC_OPTION_ISOLATION_LEVEL_SNAPSHOT),
        ADBC_OPTION_ISOLATION_LEVEL_REPEATABLE_READ
    );
    assert_eq!(
        effective(ADBC_OPTION_ISOLATION_LEVEL_LINEARIZABLE),
        ADBC_OPTION_ISOLATION_LEVEL_SERIALIZABLE
    );
}

#[test]
fn setting_current_catalog_or_schema_accepts_only_the_reported_value() {
    let set = |s: &str| {
        check_fixed_catalog_or_schema(
            OptionValue::String(s.to_string()),
            "current catalog",
            "adbc-test",
        )
    };
    assert!(set("adbc-test").is_ok());
    assert!(
        check_fixed_catalog_or_schema(OptionValue::String(String::new()), "current schema", "")
            .is_ok()
    );
    let err = set("foo").unwrap_err();
    assert_eq!(err.status, Status::NotImplemented);
    assert!(err.message.contains("\"foo\""), "{}", err.message);
    assert!(err.message.contains("\"adbc-test\""), "{}", err.message);
    assert_eq!(
        check_fixed_catalog_or_schema(OptionValue::Int(1), "current schema", "")
            .unwrap_err()
            .status,
        Status::InvalidArguments
    );
}

/// `read_partition` takes caller-supplied bytes, so decoding must reject garbage cleanly.
#[test]
fn garbage_partition_descriptors_error_cleanly() {
    let cases: [&[u8]; 6] = [
        b"",                      // empty
        b"\xff\xfe\x00 not json", // non-UTF-8, non-JSON bytes
        b"{",                     // truncated JSON
        b"{}",                    // valid JSON object missing every descriptor field
        br#"{"hello": "world"}"#, // valid JSON object of the wrong shape
        b"[1, 2, 3]",             // valid JSON that is not an object
    ];
    for descriptor in cases {
        let error = decode_partition(descriptor).unwrap_err();
        assert_eq!(
            error.status,
            Status::InvalidArguments,
            "descriptor {descriptor:?}"
        );
        assert!(
            error.message.contains("invalid partition descriptor"),
            "unexpected message for {descriptor:?}: {}",
            error.message
        );
    }
}

/// `encode_partition` writes the versioned envelope, and decode then encode is a fixed point.
#[test]
fn partition_descriptor_envelope_round_trips() {
    let descriptor = include_bytes!("../../fuzz/seeds/partition/enveloped-query-descriptor");
    let partition = decode_partition(descriptor).expect("enveloped descriptor decodes");

    let encoded = encode_partition(&partition).expect("encode");
    let value: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(value["v"], PARTITION_DESCRIPTOR_VERSION);
    assert!(
        value.get("partition").is_some(),
        "envelope carries the partition payload: {value}"
    );

    let again = decode_partition(&encoded).expect("enveloped descriptor decodes");
    assert_eq!(encode_partition(&again).expect("re-encode"), encoded);
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

/// A payload without the versioned envelope is rejected.
#[test]
fn bare_partition_descriptor_is_rejected() {
    let bare: &[u8] = br#"{"inner":{"Query":{"sql":"SELECT 1"}}}"#;
    let error = decode_partition(bare).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(
        error.message.contains("missing \"v\" version field"),
        "unexpected message: {}",
        error.message
    );
}

/// An unknown version is rejected on the version, not on the payload shape.
#[test]
fn unsupported_partition_descriptor_version_errors_cleanly() {
    for descriptor in [
        br#"{"v":2,"partition":{"future":"format"}}"#.as_slice(),
        br#"{"v":0}"#.as_slice(),
    ] {
        let error = decode_partition(descriptor).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        assert!(
            error.message.contains("partition descriptor version")
                && error.message.contains("not supported by this driver"),
            "unexpected message for {descriptor:?}: {}",
            error.message
        );
    }
    let error = decode_partition(br#"{"v":2,"partition":{}}"#).unwrap_err();
    assert!(
        error
            .message
            .contains("partition descriptor version 2 not supported by this driver"),
        "{}",
        error.message
    );
}

/// Non-integer versions and missing or malformed payloads are `InvalidArguments`.
#[test]
fn malformed_partition_descriptor_envelopes_error_cleanly() {
    let cases: [&[u8]; 4] = [
        br#"{"v":"one"}"#,            // non-integer version
        br#"{"v":-1}"#,               // negative version
        br#"{"v":1}"#,                // missing "partition" payload
        br#"{"v":1,"partition":{}}"#, // payload of the wrong shape
    ];
    for descriptor in cases {
        let error = decode_partition(descriptor).unwrap_err();
        assert_eq!(
            error.status,
            Status::InvalidArguments,
            "descriptor {descriptor:?}"
        );
        assert!(
            error.message.contains("invalid partition descriptor"),
            "unexpected message for {descriptor:?}: {}",
            error.message
        );
    }
}

#[test]
fn partition_descriptor_round_trips_large_floats() {
    // Integer literals beyond i64/u64 become f64 values in `_unknown_fields`. serde_json's
    // `float_roundtrip` feature keeps re-encoding them byte-stable.
    let descriptor = br#"{"v": 1, "partition": {"version": 1, "inner": {"Query": {"session":"projects/p/instances/i/databases/d/sessions/s","transaction":{"id":"dHg="},"partitionToken":"dG9rZW4=","sql":"SELECT 1","con[": 44444424444444444249, "/+%n":4444440000000000000000074074764, "/s%n": "prns/s"}}}}"#;

    let partition = decode_partition(descriptor).expect("descriptor decodes");
    let first = encode_partition(&partition).expect("a decoded partition re-encodes");
    let normalized = decode_partition(&first).expect("re-encoded descriptor decodes");
    let again = encode_partition(&normalized).expect("a decoded partition re-encodes");
    assert_eq!(
        first, again,
        "re-encoding the encoder's output must be byte-stable"
    );
}

/// `get_table_schema` against an in-process mock Spanner server, for its existence probe.
mod table_schema_probe {
    use std::sync::{Arc, Mutex};

    use adbc_core::error::Status;
    use adbc_core::options::{OptionConnection, OptionDatabase, OptionValue};
    use adbc_core::{Connection, Database, Driver, Optionable};
    use spanner_grpc_mock::MockSpanner;
    use spanner_grpc_mock::google::spanner::v1;

    use crate::{SpannerConnection, SpannerDriver};

    pub(super) type Requests = Arc<Mutex<Vec<v1::ExecuteSqlRequest>>>;
    pub(super) type Sender = tokio::sync::mpsc::Sender<tonic::Result<v1::PartialResultSet>>;

    /// What the mock answers to the `SELECT * ... LIMIT 0` schema query.
    enum SchemaQuery {
        Fails(tonic::Code),
        /// Never answers, so the query timeout fires.
        Silent,
    }

    pub(super) struct MockServer {
        endpoint: String,
        pub(super) requests: Requests,
        server: tokio::task::JoinHandle<()>,
        _runtime: tokio::runtime::Runtime,
    }

    impl Drop for MockServer {
        fn drop(&mut self) {
            self.server.abort();
        }
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

    /// Serves sessions plus `mock`'s expectations on an ephemeral port. `requests` is what the
    /// expectations record.
    pub(super) fn serve(mut mock: MockSpanner, requests: Requests) -> MockServer {
        mock.expect_create_session().returning(|request| {
            Ok(tonic::Response::new(v1::Session {
                name: format!("{}/sessions/mock", request.into_inner().database),
                multiplexed: true,
                ..Default::default()
            }))
        });
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let (endpoint, server) = runtime
            .block_on(spanner_grpc_mock::start("127.0.0.1:0", mock))
            .unwrap();
        MockServer {
            endpoint,
            requests,
            server,
            _runtime: runtime,
        }
    }

    pub(super) fn connect(server: &MockServer) -> SpannerConnection {
        let mut driver = SpannerDriver::try_new().unwrap();
        driver
            .new_database_with_opts([
                (
                    OptionDatabase::Uri,
                    OptionValue::String("spanner:///projects/p/instances/i/databases/d".into()),
                ),
                (
                    OptionDatabase::Other(crate::OPTION_ENDPOINT.into()),
                    OptionValue::String(server.endpoint.clone()),
                ),
                (
                    OptionDatabase::Other(crate::OPTION_EMULATOR.into()),
                    OptionValue::String("true".into()),
                ),
            ])
            .unwrap()
            .new_connection()
            .unwrap()
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
        let (fields, values): (Vec<(&str, v1::TypeCode)>, Vec<&str>) =
            if request.sql.contains("INFORMATION_SCHEMA.TABLES") {
                (
                    vec![("TABLE_SCHEMA", String), ("TABLE_NAME", String)],
                    vec!["", "t"],
                )
            } else if request.sql.contains("INFORMATION_SCHEMA.COLUMNS") {
                let names = ["TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "SPANNER_TYPE"];
                (names.map(|name| (name, String)).to_vec(), Vec::new())
            } else {
                (vec![("n", Int64)], vec!["3"])
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
                    kind: Some(prost_types::value::Kind::StringValue(value.into())),
                })
                .collect(),
            last: true,
            ..Default::default()
        }
    }

    /// The query timeout is one deadline for the whole call. Discovery (two sequential queries)
    /// and the scan each fit in it, but together they don't.
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
            3,
            "TABLES, COLUMNS, one scan"
        );

        connection
            .set_option(
                OptionConnection::Other(crate::OPTION_RPC_TIMEOUT_QUERY.into()),
                OptionValue::Double(2.5 * DELAY.as_secs_f64()),
            )
            .unwrap();
        let error = connection
            .get_statistics(None, None, None, false)
            .err()
            .expect("three sequential delays exceed one deadline");
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

//! Creating a statement requires a connection, which opens a session with an RPC. Most of these
//! tests need no live statement: argument validation, released handles and partition export.
//! [`MockStatement`] serves the remaining tests from a mock Spanner server.

use std::ffi::CString;
use std::sync::Arc;

use adbc_core::error::{AdbcStatusCode, Status};

use arrow_array::ffi::to_ffi;
use arrow_array::{Array, ArrayRef, Int64Array, StructArray};

use super::*;
use crate::ffi::abi::{ADBC_STATUS_OK, AdbcDatabase};
use crate::ffi::options::{
    get_option_bytes, get_option_double, get_option_int, get_option_string, release_handle,
    set_option_bytes, set_option_double, set_option_int, set_option_string,
};
use crate::ffi::test_support::null;

const TARGET_TABLE: &str = "adbc.ingest.target_table";

/// A statement after `AdbcStatementRelease`, or one never passed to `AdbcStatementNew`.
fn released_handle() -> AdbcStatement {
    AdbcStatement {
        private_data: null(),
        private_driver: null(),
    }
}

fn key(name: &str) -> CString {
    CString::new(name).unwrap()
}

#[test]
fn rejects_a_null_statement_handle() {
    assert_eq!(
        unsafe { statement_new(null(), null(), null()) },
        AdbcStatusCode::from(Status::InvalidArguments)
    );
    assert_eq!(
        unsafe { release_handle::<AdbcStatement>(null(), null()) },
        AdbcStatusCode::from(Status::InvalidArguments)
    );
    // Entry points using `with_state` treat a null handle as uninitialized.
    assert_eq!(
        unsafe { statement_prepare(null(), null()) },
        AdbcStatusCode::from(Status::InvalidState)
    );
}

#[test]
fn every_entry_point_refuses_a_released_handle() {
    let mut handle = released_handle();
    let s = &raw mut handle;
    let name = key(TARGET_TABLE);
    let (mut length, mut integer, mut double) = (0_usize, 0_i64, 0.0_f64);

    for (label, status) in [
        ("prepare", unsafe { statement_prepare(s, null()) }),
        ("cancel", unsafe { statement_cancel(s, null()) }),
        ("set_sql_query", unsafe {
            statement_set_sql_query(s, name.as_ptr(), null())
        }),
        ("set_option", unsafe {
            set_option_string(s, name.as_ptr(), name.as_ptr(), null())
        }),
        ("set_option_int", unsafe {
            set_option_int(s, name.as_ptr(), 1, null())
        }),
        ("set_option_double", unsafe {
            set_option_double(s, name.as_ptr(), 1.0, null())
        }),
        ("set_option_bytes", unsafe {
            set_option_bytes(s, name.as_ptr(), b"x".as_ptr(), 1, null())
        }),
        ("set_substrait_plan", unsafe {
            statement_set_substrait_plan(s, b"x".as_ptr(), 1, null())
        }),
        ("get_option", unsafe {
            get_option_string(s, name.as_ptr(), null(), &raw mut length, null())
        }),
        ("get_option_bytes", unsafe {
            get_option_bytes(s, name.as_ptr(), null(), &raw mut length, null())
        }),
        ("get_option_int", unsafe {
            get_option_int(s, name.as_ptr(), &raw mut integer, null())
        }),
        ("get_option_double", unsafe {
            get_option_double(s, name.as_ptr(), &raw mut double, null())
        }),
        ("execute_query", unsafe {
            statement_execute_query(s, null(), null(), null())
        }),
        ("execute_schema", unsafe {
            statement_execute_schema(s, null(), null())
        }),
        ("execute_partitions", unsafe {
            statement_execute_partitions(s, null(), null(), null(), null())
        }),
        ("get_parameter_schema", unsafe {
            statement_get_parameter_schema(s, null(), null())
        }),
        ("bind", unsafe { statement_bind(s, null(), null(), null()) }),
        ("bind_stream", unsafe {
            statement_bind_stream(s, null(), null())
        }),
    ] {
        assert_eq!(
            status,
            AdbcStatusCode::from(Status::InvalidState),
            "{label}"
        );
    }
}

#[test]
fn exported_partitions_expose_their_payloads_and_release_idempotently() {
    let payloads = vec![b"first".to_vec(), Vec::new(), b"third-partition".to_vec()];
    let mut exported = export_partitions(payloads.clone());

    assert_eq!(exported.num_partitions, payloads.len());
    assert!(!exported.private_data.is_null());
    assert!(exported.release.is_some());
    for (index, expected) in payloads.iter().enumerate() {
        let pointer = unsafe { *exported.partitions.add(index) };
        let length = unsafe { *exported.partition_lengths.add(index) };
        assert_eq!(length, expected.len(), "partition {index}");
        // An empty payload's pointer is unspecified.
        if expected.is_empty() {
            continue;
        }
        assert_eq!(
            unsafe { std::slice::from_raw_parts(pointer, length) },
            expected.as_slice(),
            "partition {index}"
        );
    }

    let release = exported.release.unwrap();
    unsafe { release(&raw mut exported) };
    assert!(exported.private_data.is_null());
    assert!(exported.partitions.is_null());
    assert!(exported.partition_lengths.is_null());
    assert_eq!(exported.num_partitions, 0);
    assert!(exported.release.is_none());

    // adbc.h allows a second release, which must be a no-op.
    unsafe { release(&raw mut exported) };
    assert!(exported.private_data.is_null());
    assert_eq!(exported.num_partitions, 0);

    unsafe { release(null()) };
}

#[test]
fn exporting_no_partitions_still_releases_cleanly() {
    let mut exported = export_partitions(Vec::new());
    assert_eq!(exported.num_partitions, 0);
    assert!(!exported.private_data.is_null());
    // Not an empty `Vec`'s dangling pointer.
    assert!(exported.partitions.is_null());
    assert!(exported.partition_lengths.is_null());
    let release = exported.release.unwrap();
    unsafe { release(&raw mut exported) };
    assert!(exported.private_data.is_null());
}

/// The exported pointers must point into the payloads the box owns, not dropped copies.
#[test]
fn exported_partitions_point_into_the_owned_payloads() {
    let payloads = vec![vec![9_u8; 4096]];
    let mut exported = export_partitions(payloads);
    let owner = unsafe { &*exported.private_data.cast::<PartitionsOwner>() };
    assert_eq!(owner._payloads.len(), 1);
    assert_eq!(
        unsafe { *exported.partitions },
        owner._payloads[0].as_ptr(),
        "the exported pointer must be the payload's own buffer"
    );
    assert_eq!(unsafe { *exported.partition_lengths }, 4096);
    let release = exported.release.unwrap();
    unsafe { release(&raw mut exported) };
}

/// A live statement over a mock Spanner server, created through the C ABI without a backend.
struct MockStatement {
    statement: AdbcStatement,
    connection: AdbcConnection,
    database: AdbcDatabase,
    server: tokio::task::JoinHandle<()>,
    _runtime: tokio::runtime::Runtime,
}

impl MockStatement {
    fn start() -> Self {
        Self::start_with(|_| {})
    }

    /// `script` adds expectations after the one serving sessions.
    fn start_with(script: impl FnOnce(&mut spanner_grpc_mock::MockSpanner)) -> Self {
        use spanner_grpc_mock::MockSpanner;
        use spanner_grpc_mock::google::spanner::v1;

        use crate::ffi::database::database_init;
        use crate::ffi::options::new_handle;

        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|request| {
            Ok(tonic::Response::new(v1::Session {
                name: format!("{}/sessions/mock", request.into_inner().database),
                multiplexed: true,
                ..Default::default()
            }))
        });
        script(&mut mock);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let (endpoint, server) = runtime
            .block_on(spanner_grpc_mock::start("127.0.0.1:0", mock))
            .unwrap();

        let mut database = AdbcDatabase {
            private_data: null(),
            private_driver: null(),
        };
        assert_eq!(
            unsafe { new_handle(&raw mut database, null()) },
            ADBC_STATUS_OK
        );
        for (name, value) in [
            ("uri", "spanner:///projects/p/instances/i/databases/d"),
            (crate::OPTION_ENDPOINT, endpoint.as_str()),
            (crate::OPTION_EMULATOR, "true"),
        ] {
            let (name, value) = (key(name), key(value));
            assert_eq!(
                unsafe {
                    set_option_string(&raw mut database, name.as_ptr(), value.as_ptr(), null())
                },
                ADBC_STATUS_OK
            );
        }
        assert_eq!(
            unsafe { database_init(&raw mut database, null()) },
            ADBC_STATUS_OK
        );

        let mut connection = AdbcConnection {
            private_data: null(),
            private_driver: null(),
        };
        assert_eq!(
            unsafe { new_handle(&raw mut connection, null()) },
            ADBC_STATUS_OK
        );
        assert_eq!(
            unsafe {
                crate::ffi::connection::connection_init(
                    &raw mut connection,
                    &raw mut database,
                    null(),
                )
            },
            ADBC_STATUS_OK
        );

        let mut statement = released_handle();
        assert_eq!(
            unsafe { statement_new(&raw mut connection, &raw mut statement, null()) },
            ADBC_STATUS_OK
        );
        Self {
            statement,
            connection,
            database,
            server,
            _runtime: runtime,
        }
    }
}

impl Drop for MockStatement {
    fn drop(&mut self) {
        unsafe {
            release_handle(&raw mut self.statement, null());
            release_handle(&raw mut self.connection, null());
            release_handle(&raw mut self.database, null());
        }
        self.server.abort();
    }
}

/// A struct array and a schema that disagree are a caller mistake: Arrow's importer asserts on
/// them, and that panic must not poison the statement.
#[test]
fn binding_an_array_that_disagrees_with_its_schema_keeps_the_statement_usable() {
    use arrow_schema::{DataType, Field, Fields};

    use crate::ffi::test_support::{error_message, release_error, zeroed_error};

    let mut mock = MockStatement::start();
    let s = &raw mut mock.statement;
    let one = Fields::from(vec![Field::new("p", DataType::Int64, false)]);
    let two = StructArray::new(
        Fields::from(vec![
            Field::new("p", DataType::Int64, false),
            Field::new("q", DataType::Int64, false),
        ]),
        vec![
            Arc::new(Int64Array::from(vec![1])) as ArrayRef,
            Arc::new(Int64Array::from(vec![2])) as ArrayRef,
        ],
        None,
    );
    let (mut values, _) = to_ffi(&two.to_data()).unwrap();
    let mut schema = FFI_ArrowSchema::try_from(&DataType::Struct(one.clone())).unwrap();
    let mut error = zeroed_error();
    assert_eq!(
        unsafe { statement_bind(s, &raw mut values, &raw mut schema, &raw mut error) },
        AdbcStatusCode::from(Status::InvalidArguments),
        "{:?}",
        error_message(&error)
    );
    let message = error_message(&error).unwrap();
    assert!(message.contains("not a valid Arrow array"), "{message}");
    release_error(&mut error);
    // Ownership transferred: both inputs are released.
    assert!(values.release().is_none());
    assert!(schema.release().is_none());

    let valid = StructArray::new(
        one,
        vec![Arc::new(Int64Array::from(vec![1])) as ArrayRef],
        None,
    );
    let (mut values, mut schema) = to_ffi(&valid.to_data()).unwrap();
    assert_eq!(
        unsafe { statement_bind(s, &raw mut values, &raw mut schema, &raw mut error) },
        ADBC_STATUS_OK,
        "{:?}",
        error_message(&error)
    );
    let query = key("SELECT @p");
    assert_eq!(
        unsafe { statement_set_sql_query(s, query.as_ptr(), null()) },
        ADBC_STATUS_OK
    );
}

/// Serves batch DML reporting one affected row per statement, commits, and queries returning one
/// row.
fn serve_dml_and_queries(mock: &mut spanner_grpc_mock::MockSpanner) {
    use spanner_grpc_mock::google::spanner::v1;

    mock.expect_execute_batch_dml().returning(|request| {
        let request = request.into_inner();
        let inline_begin = matches!(
            request
                .transaction
                .as_ref()
                .and_then(|t| t.selector.as_ref()),
            Some(v1::transaction_selector::Selector::Begin(_))
        );
        let result_sets = (0..request.statements.len())
            .map(|i| v1::ResultSet {
                metadata: Some(v1::ResultSetMetadata {
                    transaction: (i == 0 && inline_begin).then(|| v1::Transaction {
                        id: b"rw-txn".to_vec(),
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
            ..Default::default()
        }))
    });
    mock.expect_commit()
        .returning(|_| Ok(tonic::Response::new(v1::CommitResponse::default())));
    mock.expect_execute_streaming_sql().returning(|_| {
        let row = v1::PartialResultSet {
            metadata: Some(v1::ResultSetMetadata {
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
            last: true,
            ..Default::default()
        };
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tx.try_send(Ok(row)).unwrap();
        Ok(tonic::Response::new(rx))
    });
}

/// Run `sql` through `AdbcStatementExecuteQuery`, with a result stream unless `stream` is false.
/// Returns `rows_affected`, which starts at a sentinel so a missing write shows, and the rows read.
fn execute_query(statement: *mut AdbcStatement, sql: &str, stream: bool) -> (i64, usize) {
    use arrow_array::ffi_stream::RecordBatchStreamReader;

    use crate::ffi::test_support::{error_message, zeroed_error};

    let query = key(sql);
    assert_eq!(
        unsafe { statement_set_sql_query(statement, query.as_ptr(), null()) },
        ADBC_STATUS_OK
    );
    let mut out = FFI_ArrowArrayStream::empty();
    let out_ptr = if stream { &raw mut out } else { null() };
    let mut rows_affected = 42_i64;
    let mut error = zeroed_error();
    assert_eq!(
        unsafe {
            statement_execute_query(statement, out_ptr, &raw mut rows_affected, &raw mut error)
        },
        ADBC_STATUS_OK,
        "{sql}: {:?}",
        error_message(&error)
    );
    if !stream {
        return (rows_affected, 0);
    }
    let rows = RecordBatchStreamReader::try_new(out)
        .unwrap()
        .map(|batch| batch.unwrap().num_rows())
        .sum();
    (rows_affected, rows)
}

/// The Python DBAPI runs every `execute` through `AdbcStatementExecuteQuery` with a result stream,
/// so DML must report its affected-row count there too, for `cursor.rowcount`.
#[test]
fn execute_query_reports_the_dml_row_count_with_or_without_a_stream() {
    let mut mock = MockStatement::start_with(serve_dml_and_queries);
    let s = &raw mut mock.statement;
    let update = "UPDATE t SET c = 'x' WHERE TRUE";
    let batch = "UPDATE t SET c = 'x' WHERE TRUE; DELETE FROM t WHERE TRUE";
    for stream in [true, false] {
        assert_eq!(execute_query(s, update, stream), (1, 0), "stream: {stream}");
        assert_eq!(execute_query(s, batch, stream), (2, 0), "stream: {stream}");
        // A query's row count is unknown; -1 overwrites the sentinel.
        let rows = usize::from(stream);
        assert_eq!(
            execute_query(s, "SELECT c FROM t", stream),
            (-1, rows),
            "stream: {stream}"
        );
    }

    // Buffered DML has not run yet, so its count is unknown until commit.
    let autocommit = key("adbc.connection.autocommit");
    let disabled = key("false");
    assert_eq!(
        unsafe {
            set_option_string(
                &raw mut mock.connection,
                autocommit.as_ptr(),
                disabled.as_ptr(),
                null(),
            )
        },
        ADBC_STATUS_OK
    );
    for stream in [true, false] {
        assert_eq!(
            execute_query(s, update, stream),
            (-1, 0),
            "stream: {stream}"
        );
    }
}

/// `AdbcStatementNew` into a live statement must refuse rather than leak or replace it, and a
/// connection's metadata entry points export their streams.
#[test]
fn a_live_connection_exports_metadata_and_refuses_a_second_statement_in_one_struct() {
    use arrow_array::RecordBatchReader;
    use arrow_array::ffi_stream::{FFI_ArrowArrayStream, RecordBatchStreamReader};

    use crate::ffi::connection::connection_get_statistic_names;
    use crate::ffi::test_support::{error_message, release_error, zeroed_error};

    let mut mock = MockStatement::start();
    let live = mock.statement.private_data;
    let mut error = zeroed_error();
    let status = unsafe {
        statement_new(
            &raw mut mock.connection,
            &raw mut mock.statement,
            &mut error,
        )
    };
    assert_eq!(status, AdbcStatusCode::from(Status::InvalidState));
    let message = error_message(&error).unwrap();
    assert!(
        message.contains("already holds a driver object"),
        "{message}"
    );
    release_error(&mut error);
    assert_eq!(mock.statement.private_data, live);

    let mut out = FFI_ArrowArrayStream::empty();
    let status =
        unsafe { connection_get_statistic_names(&raw mut mock.connection, &raw mut out, null()) };
    assert_eq!(status, ADBC_STATUS_OK);
    let reader = RecordBatchStreamReader::try_new(out).unwrap();
    assert_eq!(
        reader.schema(),
        adbc_core::schemas::GET_STATISTIC_NAMES_SCHEMA.clone()
    );
    assert_eq!(reader.count(), 0);
}

//! Creating a statement requires a connection, which opens a session with an RPC. Most of these
//! tests need no live statement: argument validation, released handles, partition export and
//! bound stream import. [`MockStatement`] serves the rest from a mock Spanner server.

use std::ffi::{CString, c_int};
use std::sync::Arc;

use adbc_core::error::{AdbcStatusCode, Status};

use arrow_array::ffi::to_ffi;
use arrow_array::{Array, ArrayRef, Int64Array, RecordBatchIterator};

use super::*;
use crate::ffi::abi::AdbcDatabase;
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

type GetSchema = unsafe extern "C" fn(*mut FFI_ArrowArrayStream, *mut FFI_ArrowSchema) -> c_int;
type GetNext = unsafe extern "C" fn(*mut FFI_ArrowArrayStream, *mut FFI_ArrowArray) -> c_int;
type GetLastError = unsafe extern "C" fn(*mut FFI_ArrowArrayStream) -> *const c_char;

/// Rebuild `stream` with the given callbacks swapped in, keeping its private data and release.
fn replace_callbacks(
    stream: FFI_ArrowArrayStream,
    get_schema: Option<GetSchema>,
    get_next: Option<GetNext>,
    get_last_error: Option<GetLastError>,
) -> FFI_ArrowArrayStream {
    // The rebuilt stream takes over the private data.
    let stream = std::mem::ManuallyDrop::new(stream);
    // SAFETY: the replacements only ignore or write their outputs; the retained release still
    // matches the retained private data.
    unsafe {
        FFI_ArrowArrayStream::new_unchecked(
            get_schema.or(stream.get_schema()),
            get_next.or(stream.get_next()),
            get_last_error.or(stream.get_last_error()),
            stream.release(),
            stream.private_data(),
        )
    }
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
fn bound_stream_schema_failures_preserve_diagnostics_and_release_ownership() {
    unsafe extern "C" fn failed_schema(
        _: *mut FFI_ArrowArrayStream,
        _: *mut FFI_ArrowSchema,
    ) -> std::ffi::c_int {
        libc::EIO
    }
    unsafe extern "C" fn description(_: *mut FFI_ArrowArrayStream) -> *const c_char {
        c"producer schema unavailable".as_ptr()
    }
    unsafe extern "C" fn no_description(_: *mut FFI_ArrowArrayStream) -> *const c_char {
        null()
    }
    unsafe extern "C" fn released_schema(
        _: *mut FFI_ArrowArrayStream,
        _: *mut FFI_ArrowSchema,
    ) -> std::ffi::c_int {
        0
    }

    for (get_schema, get_last_error, expected) in [
        (
            failed_schema as unsafe extern "C" fn(_, _) -> _,
            description as unsafe extern "C" fn(_) -> _,
            "producer schema unavailable",
        ),
        (failed_schema, no_description, "no error description"),
        (released_schema, no_description, "released schema"),
    ] {
        let lifetime = Arc::new(());
        let retained = lifetime.clone();
        let schema = Arc::new(Schema::empty());
        let batch = RecordBatch::new_empty(schema.clone());
        let stream = FFI_ArrowArrayStream::new(Box::new(RecordBatchIterator::new(
            std::iter::once_with(move || {
                drop(retained);
                Ok(batch)
            }),
            schema,
        )));
        let mut stream = replace_callbacks(stream, Some(get_schema), None, Some(get_last_error));
        assert_eq!(Arc::strong_count(&lifetime), 2);
        let failure = unsafe { BoundStreamReader::from_raw(&raw mut stream) }.unwrap_err();
        assert_eq!(failure.status, Status::InvalidArguments);
        assert!(failure.message.contains(expected), "{}", failure.message);
        if expected != "released schema" {
            assert!(failure.message.contains(&libc::EIO.to_string()));
        }
        assert!(stream.release().is_none());
        assert_eq!(Arc::strong_count(&lifetime), 1);
    }
}

#[test]
fn imported_stream_batches_preserve_schema_rows_and_buffers_after_release() {
    use arrow_array::{RecordBatchOptions, RecordBatchReader};
    use arrow_schema::{DataType, Field};

    for fields in [vec![], vec![Field::new("value", DataType::Int64, true)]] {
        let schema = Arc::new(
            Schema::new(fields).with_metadata(std::collections::HashMap::from([(
                "source".to_owned(),
                "caller".to_owned(),
            )])),
        );
        let columns = if schema.fields().is_empty() {
            vec![]
        } else {
            vec![Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])) as ArrayRef]
        };
        let batch = RecordBatch::try_new_with_options(
            schema.clone(),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(3)),
        )
        .unwrap();
        let mut stream = FFI_ArrowArrayStream::new(Box::new(RecordBatchIterator::new(
            [
                Ok(RecordBatch::new_empty(schema.clone())),
                Ok(batch.clone()),
            ],
            schema.clone(),
        )));
        let mut reader = unsafe { BoundStreamReader::from_raw(&raw mut stream) }.unwrap();
        assert!(stream.release().is_none());
        assert_eq!(reader.schema(), schema);
        assert_eq!(reader.next().unwrap().unwrap().num_rows(), 0);
        let found = reader.next().unwrap().unwrap();
        assert_eq!(found, batch);
        drop(batch);
        assert!(reader.next().is_none());
        assert!(reader.next().is_none());
        drop(reader);
        assert_eq!(found.num_rows(), 3);
        assert_eq!(found.schema(), schema);
        if !found.columns().is_empty() {
            let values = found
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(values, &Int64Array::from(vec![Some(1), None, Some(3)]));
        }
    }
}

#[test]
fn imported_streams_reject_null_rows_and_mismatched_column_counts() {
    use arrow_schema::{DataType, Field};

    unsafe extern "C" fn null_row(
        _: *mut FFI_ArrowArrayStream,
        out: *mut FFI_ArrowArray,
    ) -> std::ffi::c_int {
        let array =
            StructArray::new_null(vec![Field::new("value", DataType::Int64, true)].into(), 1);
        let (array, _) = to_ffi(&array.to_data()).unwrap();
        unsafe { std::ptr::write(out, array) };
        0
    }

    for (fields, expected) in [
        (vec![], "returned 1 columns, but its schema declares 0"),
        (
            vec![Field::new("value", DataType::Int64, true)],
            "top-level null rows",
        ),
    ] {
        let stream = FFI_ArrowArrayStream::new(Box::new(RecordBatchIterator::new(
            std::iter::empty(),
            Arc::new(Schema::new(fields)),
        )));
        let mut stream = replace_callbacks(stream, None, Some(null_row), None);
        let mut reader = unsafe { BoundStreamReader::from_raw(&raw mut stream) }.unwrap();
        let failure = reader.next().unwrap().unwrap_err();
        assert!(failure.to_string().contains(expected), "{failure}");
        assert!(reader.next().is_none());
    }
}

/// A struct with one non-UTF-8 value, long enough to be stored out of line from its view.
fn out_of_line_non_utf8_struct() -> arrow_data::ArrayData {
    use arrow_array::BinaryViewArray;
    use arrow_schema::{DataType, Field};

    let values = BinaryViewArray::from_iter_values([[0xff_u8; 16].as_slice()]);
    StructArray::new(
        vec![Field::new("payload", DataType::BinaryView, false)].into(),
        vec![Arc::new(values) as ArrayRef],
        None,
    )
    .to_data()
}

/// Arrow's C importer skips validation, while safe accessors such as `StringViewArray::value`
/// assume valid data, so a malformed import would be undefined behavior downstream.
///
/// An out-of-bounds view cannot be built through a safe API, so this sends `BinaryView` data
/// under a `Utf8View` schema instead: the layouts are identical and only the UTF-8 invariant is
/// broken.
#[test]
fn imported_arrays_are_validated_and_name_the_column_that_fails() {
    use arrow_schema::{DataType, Field};

    unsafe extern "C" fn non_utf8_view(
        _: *mut FFI_ArrowArrayStream,
        out: *mut FFI_ArrowArray,
    ) -> std::ffi::c_int {
        let (array, _) = to_ffi(&out_of_line_non_utf8_struct()).unwrap();
        unsafe { std::ptr::write(out, array) };
        0
    }

    let declared = vec![Field::new("payload", DataType::Utf8View, false)];

    let stream = FFI_ArrowArrayStream::new(Box::new(RecordBatchIterator::new(
        std::iter::empty(),
        Arc::new(Schema::new(declared)),
    )));
    let mut stream = replace_callbacks(stream, None, Some(non_utf8_view), None);
    let mut reader = unsafe { BoundStreamReader::from_raw(&raw mut stream) }.unwrap();
    let failure = reader.next().unwrap().unwrap_err().to_string();
    assert!(
        failure.contains(r#"column "payload" is invalid"#),
        "{failure}"
    );
    assert!(failure.contains("non-UTF-8"), "{failure}");
    assert!(reader.next().is_none());
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

/// Arrow's importer asserts that a struct's children match its fields, at every nesting level.
/// The top-level count is checked before import, but a nested mismatch reaches the importer.
#[test]
fn a_nested_column_that_disagrees_with_the_bound_schema_is_an_error_not_a_panic() {
    use arrow_array::RecordBatchReader;
    use arrow_schema::{DataType, Field, Fields};

    let x = Field::new("x", DataType::Int64, false);
    let y = Field::new("y", DataType::Int64, false);
    let declared = Arc::new(Schema::new(vec![Field::new(
        "a",
        DataType::Struct(Fields::from(vec![x.clone()])),
        false,
    )]));
    let wider = StructArray::new(
        Fields::from(vec![x, y]),
        vec![
            Arc::new(Int64Array::from(vec![1])) as ArrayRef,
            Arc::new(Int64Array::from(vec![2])) as ArrayRef,
        ],
        None,
    );
    let actual = Arc::new(Schema::new(vec![Field::new(
        "a",
        wider.data_type().clone(),
        false,
    )]));
    let batch = RecordBatch::try_new(actual, vec![Arc::new(wider) as ArrayRef]).unwrap();
    // `RecordBatchIterator` does not check its batches against the schema it reports.
    let mut stream = FFI_ArrowArrayStream::new(Box::new(RecordBatchIterator::new(
        [Ok(batch)],
        declared.clone(),
    )));
    let mut reader = unsafe { BoundStreamReader::from_raw(&raw mut stream) }.unwrap();
    assert_eq!(reader.schema(), declared);
    let failure = reader.next().unwrap().unwrap_err().to_string();
    assert!(failure.contains("invalid Arrow array"), "{failure}");
    assert!(reader.next().is_none());
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

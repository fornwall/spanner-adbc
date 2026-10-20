//! Stream lifetime and handle lifecycle tests, mostly through the C ABI.
//!
//! A result stream is an independent C ABI object: consuming or releasing it must stay safe after
//! the statement, connection or database that produced it is released or reused. This covers the
//! C++ `adbc_validation` suite's TestResultIndependence and TestResultInvalidation scenarios.
//!
//! Queries generate rows with `GENERATE_ARRAY`, so no tables are needed. Tests skip unless
//! `SPANNER_OMNI_ENDPOINT`, `SPANNER_EMULATOR_HOST` or `SPANNER_GCP_DATABASE` is set:
//!
//! ```sh
//! scripts/with-emulator.sh cargo test --test ffi_lifecycle -- --nocapture
//! ```
//!
//! C ABI tests also need the cdylib built next to the test binary (`cargo build`). They skip
//! without it, unless `ADBC_TEST_REQUIRE_TARGET` is set.

use std::path::{Path, PathBuf};

use adbc_core::error::Status;
use adbc_core::options::{AdbcVersion, OptionDatabase, OptionStatement, OptionValue};
use adbc_core::{Connection, Driver, Optionable, Statement};
use adbc_driver_manager::ManagedDriver;
use arrow_array::{RecordBatch, RecordBatchReader};
use spanner_adbc::SpannerDriver;

// Shares its target and database names with `tests/integration.rs`.
mod common;
use common::{TestTarget, cdylib_path, connect_with_retry, ensure_database_once, test_target};

/// The test target with its database provisioned, or `None` to skip.
fn setup(test: &str) -> Option<TestTarget> {
    let Some(target) = test_target() else {
        eprintln!("no Spanner target set; skipping {test}");
        return None;
    };
    ensure_database_once(&target, &[]);
    Some(target)
}

/// Like [`setup`], but also requires the built cdylib.
fn ffi_setup(test: &str) -> Option<(TestTarget, PathBuf)> {
    let target = setup(test)?;
    let Some(cdylib) = cdylib_path() else {
        eprintln!("cdylib not built; skipping {test} (run `cargo build` first)");
        return None;
    };
    Some((target, cdylib))
}

/// SQL producing `n` ordered INT64 rows without touching any table.
fn rows_sql(n: usize) -> String {
    format!("SELECT n FROM UNNEST(GENERATE_ARRAY(1, {n})) AS n ORDER BY n")
}

/// Load the driver through the driver manager and open a database and connection.
fn ffi_connect(
    cdylib: &Path,
    target: &TestTarget,
) -> (
    ManagedDriver,
    adbc_driver_manager::ManagedDatabase,
    adbc_driver_manager::ManagedConnection,
) {
    let mut driver = ManagedDriver::load_dynamic_from_filename(
        cdylib,
        Some(b"AdbcSpannerInit"),
        AdbcVersion::V110,
    )
    .expect("load driver via the driver manager");
    let database = driver
        .new_database_with_opts([(
            OptionDatabase::Uri,
            OptionValue::String(target.database_uri()),
        )])
        .expect("new_database via FFI");
    let connection = connect_with_retry(&database);
    (driver, database, connection)
}

/// A statement querying `rows` rows in batches of `batch`, so the result streams across several
/// fetched chunks.
fn streaming_statement<C: Connection>(
    connection: &mut C,
    rows: usize,
    batch: i64,
) -> C::StatementType {
    let mut statement = connection.new_statement().expect("new statement");
    statement
        .set_option(
            OptionStatement::Other(spanner_adbc::OPTION_ROWS_PER_BATCH.into()),
            OptionValue::Int(batch),
        )
        .expect("set rows_per_batch");
    statement.set_sql_query(rows_sql(rows)).unwrap();
    statement
}

/// Total row count of a reader, failing on any stream error.
fn drain_rows(reader: Box<dyn RecordBatchReader + Send>) -> usize {
    reader
        .collect::<Result<Vec<_>, _>>()
        .expect("collect result stream")
        .iter()
        .map(RecordBatch::num_rows)
        .sum()
}

/// TestResultIndependence: a result stream keeps working after its statement, connection and
/// database are released. The exported reader holds the runtime and client, so releasing the
/// handles must not shut down the runtime it blocks on.
#[test]
fn ffi_stream_survives_statement_connection_database_release() {
    let test = "ffi_stream_survives_statement_connection_database_release";
    let Some((target, cdylib)) = ffi_setup(test) else {
        return;
    };

    let (driver, database, mut connection) = ffi_connect(&cdylib, &target);
    let mut statement = streaming_statement(&mut connection, 1000, 100);
    let mut reader = statement.execute().expect("execute via FFI");
    let first = reader.next().expect("first batch").expect("first batch ok");
    assert_eq!(first.num_rows(), 100);

    // The driver must outlive the stream: dropping the last `ManagedDriver` unloads the library
    // the stream's callbacks live in. The C driver manager likewise keeps the library loaded
    // until `AdbcDriverRelease`.
    drop(statement);
    drop(connection);
    drop(database);

    let rest = drain_rows(reader);
    assert_eq!(first.num_rows() + rest, 1000);
    drop(driver);
}

/// The native Rust variant of [`ffi_stream_survives_statement_connection_database_release`].
/// With no library to keep loaded, the driver can be dropped too.
#[test]
fn stream_survives_statement_connection_database_release() {
    let Some(target) = setup("stream_survives_statement_connection_database_release") else {
        return;
    };

    let mut driver = SpannerDriver::try_new().expect("create driver");
    let database = driver
        .new_database_with_opts([(
            OptionDatabase::Uri,
            OptionValue::String(target.database_uri()),
        )])
        .expect("create database");
    let mut connection = connect_with_retry(&database);
    let mut statement = streaming_statement(&mut connection, 1000, 100);

    let mut reader = statement.execute().expect("execute");
    let first = reader.next().expect("first batch").expect("first batch ok");
    assert_eq!(first.num_rows(), 100);

    drop(statement);
    drop(connection);
    drop(database);
    drop(driver);

    let rest = drain_rows(reader);
    assert_eq!(first.num_rows() + rest, 1000);
}

/// TestResultInvalidation: executing a statement again while its previous stream is open. The
/// second result must be complete; draining the first may yield rows or an error, but must not
/// crash.
#[test]
fn ffi_execute_twice_with_first_stream_still_open() {
    let Some((target, cdylib)) = ffi_setup("ffi_execute_twice_with_first_stream_still_open") else {
        return;
    };

    let (_driver, _database, mut connection) = ffi_connect(&cdylib, &target);
    let mut statement = streaming_statement(&mut connection, 600, 50);
    let mut first_reader = statement.execute().expect("first execute");
    let batch = first_reader
        .next()
        .expect("first batch")
        .expect("first batch ok");
    assert_eq!(batch.num_rows(), 50);

    let second_reader = statement.execute().expect("second execute");
    assert_eq!(drain_rows(second_reader), 600);

    for item in first_reader {
        if item.is_err() {
            break;
        }
    }
}

/// One thread releases a live stream while another runs a new query on the same connection.
#[test]
fn ffi_stream_release_races_new_query() {
    let Some((target, cdylib)) = ffi_setup("ffi_stream_release_races_new_query") else {
        return;
    };

    let (_driver, _database, mut connection) = ffi_connect(&cdylib, &target);
    for _ in 0..50 {
        let mut statement = streaming_statement(&mut connection, 500, 50);
        let reader = statement.execute().expect("execute racing stream");
        // Dropping the reader runs the C release callback, which stops the prefetch task.
        let releaser = std::thread::spawn(move || drop(reader));
        let mut probe = connection.new_statement().expect("new statement");
        probe.set_sql_query("SELECT 1 AS one").unwrap();
        assert_eq!(drain_rows(probe.execute().expect("racing query")), 1);
        releaser
            .join()
            .expect("stream releaser thread must not panic");
    }
}

/// Calls on a statement without SQL report `InvalidState` and leave it usable. Cancel after
/// release is covered by `ffi_double_release_and_error_struct_reuse`.
#[test]
fn ffi_unhappy_paths_error_instead_of_crash() {
    let Some((target, cdylib)) = ffi_setup("ffi_unhappy_paths_error_instead_of_crash") else {
        return;
    };

    let (_driver, _database, mut connection) = ffi_connect(&cdylib, &target);
    let mut statement = connection.new_statement().expect("new statement");

    let Err(error) = statement.execute() else {
        panic!("execute without SQL must fail");
    };
    assert_eq!(error.status, Status::InvalidState, "{error:?}");
    let error = statement
        .execute_update()
        .expect_err("execute_update without SQL must fail");
    assert_eq!(error.status, Status::InvalidState, "{error:?}");
    let error = statement
        .get_parameter_schema()
        .expect_err("get_parameter_schema without SQL must fail");
    assert_eq!(error.status, Status::InvalidState, "{error:?}");
    let error = statement
        .prepare()
        .expect_err("prepare without SQL must fail");
    assert_eq!(error.status, Status::InvalidState, "{error:?}");

    // The parameter schema needs no `prepare`, which is a no-op for this driver.
    statement.set_sql_query("SELECT @p AS v").unwrap();
    let schema = statement
        .get_parameter_schema()
        .expect("parameter schema before prepare");
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).name(), "p");

    statement.set_sql_query("SELECT 1 AS one").unwrap();
    assert_eq!(
        drain_rows(statement.execute().expect("execute after failures")),
        1
    );
}

/// Releasing a half-consumed stream also aborts its prefetch task (`spawn_prefetch` in
/// `src/runtime.rs`); the statement and connection keep working. Repeated so leaked tasks would
/// accumulate.
#[test]
fn ffi_drop_half_consumed_stream_then_connection_still_works() {
    let test = "ffi_drop_half_consumed_stream_then_connection_still_works";
    let Some((target, cdylib)) = ffi_setup(test) else {
        return;
    };

    let (_driver, _database, mut connection) = ffi_connect(&cdylib, &target);
    for _ in 0..5 {
        // 20 chunks, of which only the first is read. GENERATE_ARRAY allows at most 16000 rows.
        let mut statement = streaming_statement(&mut connection, 10_000, 500);
        let mut reader = statement.execute().expect("execute large query");
        let first = reader.next().expect("first batch").expect("first batch ok");
        assert_eq!(first.num_rows(), 500);
        drop(reader);
        statement.set_sql_query("SELECT 1 AS one").unwrap();
        assert_eq!(drain_rows(statement.execute().expect("re-execute")), 1);
    }
    let mut probe = streaming_statement(&mut connection, 1000, 100);
    assert_eq!(drain_rows(probe.execute().expect("fresh query")), 1000);
}

/// The native Rust variant of [`ffi_drop_half_consumed_stream_then_connection_still_works`].
#[test]
fn drop_half_consumed_reader_then_connection_still_works() {
    let Some(target) = setup("drop_half_consumed_reader_then_connection_still_works") else {
        return;
    };

    let mut driver = SpannerDriver::try_new().expect("create driver");
    let database = driver
        .new_database_with_opts([(
            OptionDatabase::Uri,
            OptionValue::String(target.database_uri()),
        )])
        .expect("create database");
    let mut connection = connect_with_retry(&database);

    for _ in 0..5 {
        let mut statement = streaming_statement(&mut connection, 10_000, 500);
        let mut reader = statement.execute().expect("execute large query");
        let first = reader.next().expect("first batch").expect("first batch ok");
        assert_eq!(first.num_rows(), 500);
        drop(reader);
    }

    let mut probe = connection.new_statement().expect("new statement");
    probe.set_sql_query(rows_sql(1000)).unwrap();
    assert_eq!(drain_rows(probe.execute().expect("fresh query")), 1000);
}

/// Releasing each object twice reports `INVALID_STATE`, as does cancelling a released statement.
/// One `FFI_AdbcError` is reused throughout, released twice between uses to check idempotency.
/// The managed wrappers cannot do this: their `Drop` releases once.
#[cfg(feature = "ffi")]
#[test]
fn ffi_double_release_and_error_struct_reuse() {
    use std::ffi::{CStr, CString};
    use std::os::raw::c_void;

    use adbc_core::constants::{ADBC_STATUS_INVALID_STATE, ADBC_STATUS_OK, ADBC_VERSION_1_1_0};
    use adbc_ffi::{
        FFI_AdbcConnection, FFI_AdbcDatabase, FFI_AdbcDriver, FFI_AdbcDriverInitFunc,
        FFI_AdbcError, FFI_AdbcStatement,
    };

    let Some((target, cdylib)) = ffi_setup("ffi_double_release_and_error_struct_reuse") else {
        return;
    };

    /// Release `error` twice; the second call checks idempotency.
    fn release_error_twice(error: &mut FFI_AdbcError) {
        assert!(
            error.release.is_some(),
            "a failed call must set a releasable error"
        );
        for _ in 0..2 {
            if let Some(release) = error.release {
                unsafe { release(error) };
            }
        }
        assert!(error.message.is_null(), "release must null the message");
    }

    fn error_message(error: &FFI_AdbcError) -> String {
        if error.message.is_null() {
            return String::new();
        }
        unsafe { CStr::from_ptr(error.message) }
            .to_string_lossy()
            .into_owned()
    }

    // Load the library directly, as a C driver manager would. Declared first so it is dropped
    // last: the driver's function pointers and the error's release callback point into it.
    let library = unsafe { libloading::Library::new(&cdylib) }.expect("load cdylib");
    let init: libloading::Symbol<FFI_AdbcDriverInitFunc> =
        unsafe { library.get(b"AdbcSpannerInit") }.expect("resolve AdbcSpannerInit");

    let mut error = FFI_AdbcError::default();

    let mut driver = FFI_AdbcDriver::default();
    let status = unsafe {
        init(
            ADBC_VERSION_1_1_0,
            &mut driver as *mut _ as *mut c_void,
            &mut error,
        )
    };
    assert_eq!(status, ADBC_STATUS_OK);

    let mut database = FFI_AdbcDatabase::default();
    let status = unsafe { driver.DatabaseNew.unwrap()(&mut database, &mut error) };
    assert_eq!(status, ADBC_STATUS_OK);
    let key = CString::new("uri").unwrap();
    let value = CString::new(target.database_uri()).unwrap();
    let status = unsafe {
        driver.DatabaseSetOption.unwrap()(&mut database, key.as_ptr(), value.as_ptr(), &mut error)
    };
    assert_eq!(status, ADBC_STATUS_OK);
    let status = unsafe { driver.DatabaseInit.unwrap()(&mut database, &mut error) };
    assert_eq!(status, ADBC_STATUS_OK);

    let mut connection = FFI_AdbcConnection::default();
    let status = unsafe { driver.ConnectionNew.unwrap()(&mut connection, &mut error) };
    assert_eq!(status, ADBC_STATUS_OK);
    // Retry, as in `connect_with_retry`.
    let mut status = ADBC_STATUS_OK;
    for _ in 0..20 {
        status =
            unsafe { driver.ConnectionInit.unwrap()(&mut connection, &mut database, &mut error) };
        if status == ADBC_STATUS_OK {
            break;
        }
        release_error_twice(&mut error);
        std::thread::sleep(std::time::Duration::from_millis(250));
    }
    assert_eq!(status, ADBC_STATUS_OK, "raw FFI connect failed");

    let mut statement = FFI_AdbcStatement::default();
    let status =
        unsafe { driver.StatementNew.unwrap()(&mut connection, &mut statement, &mut error) };
    assert_eq!(status, ADBC_STATUS_OK);

    let statement_release = driver.StatementRelease.unwrap();
    let status = unsafe { statement_release(&mut statement, &mut error) };
    assert_eq!(status, ADBC_STATUS_OK);
    let status = unsafe { statement_release(&mut statement, &mut error) };
    assert_eq!(
        status, ADBC_STATUS_INVALID_STATE,
        "double statement release must error, not crash"
    );
    assert!(
        error_message(&error).contains("already released"),
        "unexpected double-release error: {:?}",
        error_message(&error)
    );
    release_error_twice(&mut error);
    let status = unsafe { driver.StatementCancel.unwrap()(&mut statement, &mut error) };
    assert_eq!(
        status, ADBC_STATUS_INVALID_STATE,
        "cancel after release must error, not crash"
    );
    release_error_twice(&mut error);

    let connection_release = driver.ConnectionRelease.unwrap();
    let status = unsafe { connection_release(&mut connection, &mut error) };
    assert_eq!(status, ADBC_STATUS_OK);
    let status = unsafe { connection_release(&mut connection, &mut error) };
    assert_eq!(
        status, ADBC_STATUS_INVALID_STATE,
        "double connection release must error, not crash"
    );
    release_error_twice(&mut error);

    let database_release = driver.DatabaseRelease.unwrap();
    let status = unsafe { database_release(&mut database, &mut error) };
    assert_eq!(status, ADBC_STATUS_OK);
    let status = unsafe { database_release(&mut database, &mut error) };
    assert_eq!(
        status, ADBC_STATUS_INVALID_STATE,
        "double database release must error, not crash"
    );
    release_error_twice(&mut error);

    // Through a saved callback: the first release clears the field, which also makes the
    // struct's `Drop` a no-op.
    let driver_release = driver.release.unwrap();
    let status = unsafe { driver_release(&mut driver, &mut error) };
    assert_eq!(status, ADBC_STATUS_OK);
    let status = unsafe { driver_release(&mut driver, &mut error) };
    assert_eq!(
        status, ADBC_STATUS_INVALID_STATE,
        "double driver release must error, not crash"
    );
    release_error_twice(&mut error);
}

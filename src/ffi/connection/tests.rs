use std::ffi::CString;

use adbc_core::options::OptionValue;

use super::super::abi::{ADBC_STATUS_INVALID_ARGUMENT, ADBC_STATUS_INVALID_STATE, ADBC_STATUS_OK};
use super::super::options::{
    get_option_bytes, get_option_double, get_option_int, get_option_string, new_handle,
    release_handle, set_option_bytes, set_option_double, set_option_int, set_option_string,
};
use super::*;
use crate::ffi::test_support::{error_message, null, release_error, zeroed_error};

/// A connection after `AdbcConnectionNew`. Argument validation and the readiness check need no
/// backend.
fn pending() -> AdbcConnection {
    let mut connection = AdbcConnection {
        private_data: null(),
        private_driver: null(),
    };
    assert_eq!(
        unsafe { new_handle(&raw mut connection, null()) },
        ADBC_STATUS_OK
    );
    assert!(!connection.private_data.is_null());
    connection
}

fn drop_pending(connection: &mut AdbcConnection) {
    assert_eq!(
        unsafe { release_handle(&raw mut *connection, null()) },
        ADBC_STATUS_OK
    );
}

/// The options buffered by a pending connection.
fn buffered(connection: &mut AdbcConnection) -> Vec<(OptionConnection, OptionValue)> {
    let mut state =
        unsafe { super::super::handle::lock_state::<State>(connection.private_data, KIND) }
            .unwrap();
    match &mut *state {
        State::Pending(options) => options.entries.clone(),
        State::Ready(_) => panic!("expected a pending connection"),
    }
}

#[test]
fn a_null_handle_is_a_bad_argument_not_a_crash() {
    assert_eq!(
        unsafe { new_handle::<AdbcConnection>(null(), null()) },
        ADBC_STATUS_INVALID_ARGUMENT
    );
    assert_eq!(
        unsafe { release_handle::<AdbcConnection>(null(), null()) },
        ADBC_STATUS_INVALID_ARGUMENT
    );
    // An unpopulated `private_data` is an object used out of order, not a bad argument.
    let mut connection = AdbcConnection {
        private_data: null(),
        private_driver: null(),
    };
    assert_eq!(
        unsafe { connection_commit(&raw mut connection, null()) },
        ADBC_STATUS_INVALID_STATE
    );
}

#[test]
fn setters_reject_a_null_or_non_utf8_key_or_value() {
    let mut connection = pending();
    let key = CString::new(adbc_core::constants::ADBC_CONNECTION_OPTION_AUTOCOMMIT).unwrap();
    let value = CString::new("true").unwrap();
    // 0xFF never occurs in UTF-8.
    let invalid = [0xFF_u8, 0x00];
    let invalid = invalid.as_ptr().cast::<c_char>();

    for status in [
        unsafe { set_option_string(&raw mut connection, null(), value.as_ptr(), null()) },
        unsafe { set_option_string(&raw mut connection, key.as_ptr(), null(), null()) },
        unsafe { set_option_string(&raw mut connection, invalid, value.as_ptr(), null()) },
        unsafe { set_option_int(&raw mut connection, invalid, 1, null()) },
        unsafe { set_option_bytes(&raw mut connection, key.as_ptr(), null(), 3, null()) },
    ] {
        assert_eq!(status, ADBC_STATUS_INVALID_ARGUMENT);
    }

    assert!(
        buffered(&mut connection).is_empty(),
        "a rejected setter must not record anything"
    );
    drop_pending(&mut connection);
}

#[test]
fn transaction_cancel_and_init_entry_points_check_their_state() {
    let mut connection = pending();
    for status in [
        unsafe { connection_commit(&raw mut connection, null()) },
        unsafe { connection_rollback(&raw mut connection, null()) },
        unsafe { connection_cancel(&raw mut connection, null()) },
        // A null database.
        unsafe { connection_init(&raw mut connection, null(), null()) },
    ] {
        assert_eq!(status, ADBC_STATUS_INVALID_STATE);
    }
    drop_pending(&mut connection);
}

#[test]
fn metadata_streams_require_an_initialized_connection() {
    let mut connection = pending();
    let mut out = std::mem::MaybeUninit::<FFI_ArrowArrayStream>::zeroed();
    for status in [
        unsafe { connection_get_info(&raw mut connection, null(), 0, out.as_mut_ptr(), null()) },
        unsafe { connection_get_table_types(&raw mut connection, out.as_mut_ptr(), null()) },
        unsafe { connection_get_statistic_names(&raw mut connection, out.as_mut_ptr(), null()) },
        unsafe {
            connection_get_statistics(
                &raw mut connection,
                null(),
                null(),
                null(),
                1,
                out.as_mut_ptr(),
                null(),
            )
        },
        unsafe {
            connection_read_partition(
                &raw mut connection,
                b"plan".as_ptr(),
                4,
                out.as_mut_ptr(),
                null(),
            )
        },
    ] {
        assert_eq!(status, ADBC_STATUS_INVALID_STATE);
    }

    // Caught before the state check.
    assert_eq!(
        unsafe {
            connection_read_partition(&raw mut connection, null(), 7, out.as_mut_ptr(), null())
        },
        ADBC_STATUS_INVALID_ARGUMENT
    );
    drop_pending(&mut connection);
}

#[test]
fn metadata_outputs_are_checked_before_initialization_or_network_work() {
    type MetadataCall = fn(*mut AdbcConnection, *mut AdbcError) -> AdbcStatusCode;

    let mut connection = pending();
    let mut error = zeroed_error();
    let connection_ptr = &raw mut connection;
    let calls: [(&str, MetadataCall); 7] = [
        ("out", |connection, error| unsafe {
            connection_get_info(connection, null(), 0, null(), error)
        }),
        ("out", |connection, error| unsafe {
            connection_get_objects(
                connection,
                0,
                null(),
                null(),
                null(),
                null(),
                null(),
                null(),
                error,
            )
        }),
        ("schema", |connection, error| unsafe {
            connection_get_table_schema(
                connection,
                null(),
                null(),
                c"Singers".as_ptr(),
                null(),
                error,
            )
        }),
        ("out", |connection, error| unsafe {
            connection_get_table_types(connection, null(), error)
        }),
        ("out", |connection, error| unsafe {
            connection_get_statistics(connection, null(), null(), null(), 1, null(), error)
        }),
        ("out", |connection, error| unsafe {
            connection_get_statistic_names(connection, null(), error)
        }),
        ("out", |connection, error| unsafe {
            connection_read_partition(connection, null(), 0, null(), error)
        }),
    ];
    for (output, call) in calls {
        assert_eq!(
            // A fresh pointer each time: `release_error` reborrows `error` uniquely.
            call(connection_ptr, &raw mut error),
            ADBC_STATUS_INVALID_ARGUMENT
        );
        assert_eq!(
            error_message(&error).unwrap(),
            format!("{output} must not be null")
        );
        release_error(&mut error);
    }

    assert!(buffered(&mut connection).is_empty());
    drop_pending(&mut connection);
}

#[test]
fn get_objects_accepts_every_documented_depth_and_rejects_the_rest() {
    let mut connection = pending();
    let mut out = std::mem::MaybeUninit::<FFI_ArrowArrayStream>::zeroed();
    let types = [
        CString::new("TABLE").unwrap(),
        CString::new("VIEW").unwrap(),
    ];
    let type_list = [types[0].as_ptr(), types[1].as_ptr(), null()];

    let mut get_objects = |depth: c_int, error: *mut AdbcError| unsafe {
        connection_get_objects(
            &raw mut connection,
            depth,
            null(),
            null(),
            null(),
            type_list.as_ptr(),
            null(),
            out.as_mut_ptr(),
            error,
        )
    };

    // 0..=3 are ADBC_OBJECT_DEPTH_{ALL,CATALOGS,DB_SCHEMAS,TABLES}; COLUMNS is an alias of ALL.
    // A valid depth gets as far as the state check.
    for depth in 0..=3 {
        assert_eq!(get_objects(depth, null()), ADBC_STATUS_INVALID_STATE);
    }
    for depth in [-1, 4, 99] {
        assert_eq!(get_objects(depth, null()), ADBC_STATUS_INVALID_ARGUMENT);
    }

    // The message must not suggest 4 for columns.
    let mut error = zeroed_error();
    assert_eq!(get_objects(4, &raw mut error), ADBC_STATUS_INVALID_ARGUMENT);
    let message = error_message(&error).expect("the rejection carries a message");
    release_error(&mut error);
    assert!(!message.contains("4 (columns)"), "{message}");
    assert!(message.contains("0 (all, including columns)"), "{message}");

    drop_pending(&mut connection);
}

#[test]
fn get_table_schema_requires_a_table_name() {
    let mut connection = pending();
    let mut schema = std::mem::MaybeUninit::<FFI_ArrowSchema>::zeroed();
    let table = CString::new("Singers").unwrap();
    let invalid = [0xFF_u8, 0x00];

    let mut get_table_schema = |db_schema: *const c_char, table_name: *const c_char| unsafe {
        connection_get_table_schema(
            &raw mut connection,
            null(),
            db_schema,
            table_name,
            schema.as_mut_ptr(),
            null(),
        )
    };

    assert_eq!(
        get_table_schema(null(), null()),
        ADBC_STATUS_INVALID_ARGUMENT
    );
    assert_eq!(
        get_table_schema(invalid.as_ptr().cast::<c_char>(), table.as_ptr()),
        ADBC_STATUS_INVALID_ARGUMENT,
        "a non-UTF-8 db_schema is still a bad argument even though the field is optional"
    );
    assert_eq!(
        get_table_schema(null(), table.as_ptr()),
        ADBC_STATUS_INVALID_STATE
    );

    drop_pending(&mut connection);
}

/// Before `Init` every key is buffered, including unknown ones; the driver's setter rejects
/// unknown keys when `Init` replays the buffer.
#[test]
fn every_key_is_buffered_before_init_whether_the_driver_knows_it_or_not() {
    let mut connection = pending();
    let keys = [
        OptionConnection::AutoCommit,
        OptionConnection::CurrentCatalog,
        OptionConnection::CurrentSchema,
        OptionConnection::ReadOnly,
        OptionConnection::IsolationLevel,
        OptionConnection::Other("adbc.connection.no_such_option".to_owned()),
        OptionConnection::Other("spanner.read.stalenes".to_owned()),
        OptionConnection::Other(crate::OPTION_READ_STALENESS.to_owned()),
        OptionConnection::Other(crate::OPTION_REQUEST_PRIORITY.to_owned()),
        OptionConnection::Other(crate::OPTION_TRANSACTION_TAG.to_owned()),
    ];
    for key in &keys {
        let ffi_key = CString::new(key.as_ref()).unwrap();
        let value = CString::new("true").unwrap();
        let status = unsafe {
            set_option_string(
                &raw mut connection,
                ffi_key.as_ptr(),
                value.as_ptr(),
                null(),
            )
        };
        assert_eq!(status, ADBC_STATUS_OK, "{key:?}: {status}");
    }

    assert_eq!(buffered(&mut connection).len(), keys.len());
    drop_pending(&mut connection);
}

/// A released handle has null `private_data`, which every entry point must check before use.
#[test]
fn every_entry_point_refuses_a_released_handle() {
    let mut handle = AdbcConnection {
        private_data: null(),
        private_driver: null(),
    };
    let c = &raw mut handle;
    let name = CString::new(adbc_core::constants::ADBC_CONNECTION_OPTION_AUTOCOMMIT).unwrap();
    let (mut length, mut integer, mut double) = (0_usize, 0_i64, 0.0_f64);
    let mut database = AdbcDatabase {
        private_data: null(),
        private_driver: null(),
    };

    for (label, status) in [
        ("init", unsafe {
            connection_init(c, &raw mut database, null())
        }),
        ("commit", unsafe { connection_commit(c, null()) }),
        ("rollback", unsafe { connection_rollback(c, null()) }),
        ("cancel", unsafe { connection_cancel(c, null()) }),
        ("get_info", unsafe {
            connection_get_info(c, null(), 0, null(), null())
        }),
        ("get_objects", unsafe {
            connection_get_objects(c, 0, null(), null(), null(), null(), null(), null(), null())
        }),
        ("get_table_schema", unsafe {
            connection_get_table_schema(c, null(), null(), name.as_ptr(), null(), null())
        }),
        ("get_table_types", unsafe {
            connection_get_table_types(c, null(), null())
        }),
        ("get_statistics", unsafe {
            connection_get_statistics(c, null(), null(), null(), 0, null(), null())
        }),
        ("get_statistic_names", unsafe {
            connection_get_statistic_names(c, null(), null())
        }),
        ("read_partition", unsafe {
            connection_read_partition(c, b"x".as_ptr(), 1, null(), null())
        }),
        ("set_option", unsafe {
            set_option_string(c, name.as_ptr(), name.as_ptr(), null())
        }),
        ("set_option_int", unsafe {
            set_option_int(c, name.as_ptr(), 1, null())
        }),
        ("set_option_double", unsafe {
            set_option_double(c, name.as_ptr(), 1.0, null())
        }),
        ("set_option_bytes", unsafe {
            set_option_bytes(c, name.as_ptr(), b"x".as_ptr(), 1, null())
        }),
        ("get_option", unsafe {
            get_option_string(c, name.as_ptr(), null(), &raw mut length, null())
        }),
        ("get_option_bytes", unsafe {
            get_option_bytes(c, name.as_ptr(), null(), &raw mut length, null())
        }),
        ("get_option_int", unsafe {
            get_option_int(c, name.as_ptr(), &raw mut integer, null())
        }),
        ("get_option_double", unsafe {
            get_option_double(c, name.as_ptr(), &raw mut double, null())
        }),
        ("release", unsafe { release_handle(c, null()) }),
    ] {
        assert_eq!(status, ADBC_STATUS_INVALID_STATE, "{label}");
    }
}

/// `AdbcStatementNew` borrows the connection without `dispatch`, so it checks readiness itself.
#[test]
fn creating_a_statement_needs_an_initialized_connection() {
    let mut connection = pending();
    let mut statement = crate::ffi::abi::AdbcStatement {
        private_data: null(),
        private_driver: null(),
    };
    let mut error = zeroed_error();
    assert_eq!(
        unsafe {
            crate::ffi::statement::statement_new(
                &raw mut connection,
                &raw mut statement,
                &raw mut error,
            )
        },
        ADBC_STATUS_INVALID_STATE
    );
    let message = error_message(&error).expect("the refusal must say why");
    release_error(&mut error);
    assert!(message.contains("connection"), "{message}");
    assert!(statement.private_data.is_null(), "nothing was allocated");
    drop_pending(&mut connection);
}

/// `AdbcConnectionInit` borrows the database without `dispatch`, so it checks readiness itself.
#[test]
fn init_against_an_allocated_but_uninitialized_database_is_refused() {
    let mut connection = pending();
    let mut database = AdbcDatabase {
        private_data: null(),
        private_driver: null(),
    };
    assert_eq!(
        unsafe { new_handle(&raw mut database, null()) },
        ADBC_STATUS_OK
    );

    let mut error = zeroed_error();
    assert_eq!(
        unsafe { connection_init(&raw mut connection, &raw mut database, &raw mut error) },
        ADBC_STATUS_INVALID_STATE
    );
    let message = error_message(&error).expect("the refusal must say why");
    release_error(&mut error);
    assert!(message.contains("database"), "{message}");

    // The connection stays pending, so `Init` can be retried.
    assert!(matches!(
        &*unsafe { super::super::handle::lock_state::<State>(connection.private_data, KIND) }
            .unwrap(),
        State::Pending(_)
    ));

    assert_eq!(
        unsafe { release_handle(&raw mut database, null()) },
        ADBC_STATUS_OK
    );
    drop_pending(&mut connection);
}

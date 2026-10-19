use super::*;
use crate::ffi::test_support::{null, zeroed_error};

/// Every operation slot in header order, and whether it is populated.
fn slots(table: &AdbcDriver) -> Vec<(&'static str, bool)> {
    macro_rules! slots {
        ($($field:ident),+ $(,)?) => {
            vec![$((stringify!($field), table.$field.is_some())),+]
        };
    }
    slots![
        // --- ADBC 1.0.0 ---
        DatabaseInit,
        DatabaseNew,
        DatabaseSetOption,
        DatabaseRelease,
        ConnectionCommit,
        ConnectionGetInfo,
        ConnectionGetObjects,
        ConnectionGetTableSchema,
        ConnectionGetTableTypes,
        ConnectionInit,
        ConnectionNew,
        ConnectionSetOption,
        ConnectionReadPartition,
        ConnectionRelease,
        ConnectionRollback,
        StatementBind,
        StatementBindStream,
        StatementExecuteQuery,
        StatementExecutePartitions,
        StatementGetParameterSchema,
        StatementNew,
        StatementPrepare,
        StatementRelease,
        StatementSetOption,
        StatementSetSqlQuery,
        StatementSetSubstraitPlan,
        // --- ADBC 1.1.0 ---
        ErrorGetDetailCount,
        ErrorGetDetail,
        ErrorFromArrayStream,
        DatabaseGetOption,
        DatabaseGetOptionBytes,
        DatabaseGetOptionDouble,
        DatabaseGetOptionInt,
        DatabaseSetOptionBytes,
        DatabaseSetOptionDouble,
        DatabaseSetOptionInt,
        ConnectionCancel,
        ConnectionGetOption,
        ConnectionGetOptionBytes,
        ConnectionGetOptionDouble,
        ConnectionGetOptionInt,
        ConnectionGetStatistics,
        ConnectionGetStatisticNames,
        ConnectionSetOptionBytes,
        ConnectionSetOptionDouble,
        ConnectionSetOptionInt,
        StatementCancel,
        StatementExecuteSchema,
        StatementGetOption,
        StatementGetOptionBytes,
        StatementGetOptionDouble,
        StatementGetOptionInt,
        StatementSetOptionBytes,
        StatementSetOptionDouble,
        StatementSetOptionInt,
    ]
}

/// How many of [`slots`] a 1.0.0 caller's allocation covers.
const PREFIX_SLOTS: usize =
    (ADBC_DRIVER_1_0_0_SIZE - 3 * size_of::<*const c_void>()) / size_of::<*const c_void>();

#[test]
fn populates_a_1_1_0_vtable() {
    let mut table = std::mem::MaybeUninit::<AdbcDriver>::zeroed();
    let mut error = zeroed_error();
    let status = unsafe {
        AdbcSpannerInit(
            ADBC_VERSION_1_1_0,
            table.as_mut_ptr().cast::<c_void>(),
            &raw mut error,
        )
    };
    assert_eq!(status, ADBC_STATUS_OK);
    let table = unsafe { table.assume_init() };
    let slots = slots(&table);
    // Catch a field added to `AdbcDriver` but not to `slots`. All but the first three fields are
    // slots.
    assert_eq!(
        slots.len(),
        size_of::<AdbcDriver>() / size_of::<*const c_void>() - 3,
        "every AdbcDriver function field must be listed"
    );
    for (name, populated) in slots {
        assert!(populated, "{name} is missing from the vtable");
    }
}

#[test]
fn populates_every_slot_a_1_0_0_caller_can_see() {
    // Allocated at the 1.1.0 size so the tail can be inspected.
    let mut table = std::mem::MaybeUninit::<AdbcDriver>::zeroed();
    let mut error = zeroed_error();
    assert_eq!(
        unsafe {
            AdbcSpannerInit(
                ADBC_VERSION_1_0_0,
                table.as_mut_ptr().cast::<c_void>(),
                &raw mut error,
            )
        },
        ADBC_STATUS_OK
    );
    let table = unsafe { table.assume_init() };
    for (name, populated) in slots(&table).into_iter().take(PREFIX_SLOTS) {
        assert!(populated, "{name} is missing from the 1.0.0 prefix");
    }
    for (name, populated) in slots(&table).into_iter().skip(PREFIX_SLOTS) {
        assert!(!populated, "{name} was written past the 1.0.0 prefix");
    }
}

/// Entry points with the same signature could be transposed in `vtable()` without the compiler
/// noticing, so drive the database and connection lifecycle and option slots through the table
/// and check their effects. (`Commit`, `Rollback` and `Cancel` behave identically before `Init`.)
#[test]
fn the_vtable_slots_do_what_their_names_say() {
    use adbc_core::options::{OptionConnection, OptionDatabase, OptionValue};

    let mut table = std::mem::MaybeUninit::<AdbcDriver>::zeroed();
    let mut error = zeroed_error();
    assert_eq!(
        unsafe {
            AdbcSpannerInit(
                ADBC_VERSION_1_1_0,
                table.as_mut_ptr().cast::<c_void>(),
                &raw mut error,
            )
        },
        ADBC_STATUS_OK
    );
    let table = unsafe { table.assume_init() };
    let name = |value: &str| std::ffi::CString::new(value).unwrap();

    let mut database = abi::AdbcDatabase {
        private_data: null(),
        private_driver: null(),
    };
    // New allocates; Init and Release on an unallocated handle would not.
    assert_eq!(
        unsafe { table.DatabaseNew.unwrap()(&raw mut database, null()) },
        ADBC_STATUS_OK
    );
    assert!(!database.private_data.is_null());

    // Each typed setter buffers its own variant, so a miswired slot shows up as the wrong
    // payload. Values need not suit their keys: nothing is validated before `Init`.
    let uri = name(adbc_core::constants::ADBC_OPTION_URI);
    let endpoint = name(crate::OPTION_ENDPOINT);
    let emulator = name(crate::OPTION_EMULATOR);
    let keyfile = name(crate::OPTION_KEYFILE);
    let value = name("http://127.0.0.1:1");
    assert_eq!(
        unsafe {
            table.DatabaseSetOption.unwrap()(
                &raw mut database,
                endpoint.as_ptr(),
                value.as_ptr(),
                null(),
            )
        },
        ADBC_STATUS_OK
    );
    assert_eq!(
        unsafe { table.DatabaseSetOptionInt.unwrap()(&raw mut database, uri.as_ptr(), 7, null()) },
        ADBC_STATUS_OK
    );
    assert_eq!(
        unsafe {
            table.DatabaseSetOptionDouble.unwrap()(
                &raw mut database,
                emulator.as_ptr(),
                0.5,
                null(),
            )
        },
        ADBC_STATUS_OK
    );
    assert_eq!(
        unsafe {
            table.DatabaseSetOptionBytes.unwrap()(
                &raw mut database,
                keyfile.as_ptr(),
                b"xy".as_ptr(),
                2,
                null(),
            )
        },
        ADBC_STATUS_OK
    );
    let buffered = {
        let mut state =
            unsafe { handle::lock_state::<database::State>(database.private_data, database::KIND) }
                .unwrap();
        match &mut *state {
            handle::Staged::Pending(options) => options.entries.clone(),
            handle::Staged::Ready(_) => panic!("the database must still be pending"),
        }
    };
    assert_eq!(buffered.len(), 4);
    assert!(matches!(&buffered[0].1, OptionValue::String(v) if v == "http://127.0.0.1:1"));
    assert_eq!(buffered[1].0, OptionDatabase::Uri);
    assert!(matches!(buffered[1].1, OptionValue::Int(7)));
    assert!(matches!(buffered[2].1, OptionValue::Double(v) if v == 0.5));
    assert!(matches!(&buffered[3].1, OptionValue::Bytes(v) if v == b"xy"));

    // Release nulls the slot; Init would have left it populated.
    assert_eq!(
        unsafe { table.DatabaseRelease.unwrap()(&raw mut database, null()) },
        ADBC_STATUS_OK
    );
    assert!(database.private_data.is_null());

    // The same for the connection.
    let mut connection = abi::AdbcConnection {
        private_data: null(),
        private_driver: null(),
    };
    assert_eq!(
        unsafe { table.ConnectionNew.unwrap()(&raw mut connection, null()) },
        ADBC_STATUS_OK
    );
    let autocommit = name(adbc_core::constants::ADBC_CONNECTION_OPTION_AUTOCOMMIT);
    let priority = name(crate::OPTION_REQUEST_PRIORITY);
    let staleness = name(crate::OPTION_READ_STALENESS);
    let transaction_tag = name(crate::OPTION_TRANSACTION_TAG);
    let truth = name("true");
    assert_eq!(
        unsafe {
            table.ConnectionSetOption.unwrap()(
                &raw mut connection,
                autocommit.as_ptr(),
                truth.as_ptr(),
                null(),
            )
        },
        ADBC_STATUS_OK
    );
    assert_eq!(
        unsafe {
            table.ConnectionSetOptionInt.unwrap()(&raw mut connection, priority.as_ptr(), 4, null())
        },
        ADBC_STATUS_OK
    );
    assert_eq!(
        unsafe {
            table.ConnectionSetOptionDouble.unwrap()(
                &raw mut connection,
                staleness.as_ptr(),
                1.5,
                null(),
            )
        },
        ADBC_STATUS_OK
    );
    assert_eq!(
        unsafe {
            table.ConnectionSetOptionBytes.unwrap()(
                &raw mut connection,
                transaction_tag.as_ptr(),
                b"z".as_ptr(),
                1,
                null(),
            )
        },
        ADBC_STATUS_OK
    );
    let buffered = {
        let mut state = unsafe {
            handle::lock_state::<connection::State>(connection.private_data, connection::KIND)
        }
        .unwrap();
        match &mut *state {
            handle::Staged::Pending(options) => options.entries.clone(),
            handle::Staged::Ready(_) => panic!("the connection must still be pending"),
        }
    };
    assert_eq!(buffered.len(), 4);
    assert_eq!(buffered[0].0, OptionConnection::AutoCommit);
    assert!(matches!(&buffered[0].1, OptionValue::String(v) if v == "true"));
    assert!(matches!(buffered[1].1, OptionValue::Int(4)));
    assert!(matches!(buffered[2].1, OptionValue::Double(v) if v == 1.5));
    assert!(matches!(&buffered[3].1, OptionValue::Bytes(v) if v == b"z"));

    // Before `Init` all four getters report NOT_FOUND, which is as far as they get without a
    // backend.
    let mut length = 0_usize;
    let mut integer = 0_i64;
    let mut double = 0_f64;
    for status in [
        unsafe {
            table.ConnectionGetOption.unwrap()(
                &raw mut connection,
                autocommit.as_ptr(),
                null(),
                &raw mut length,
                null(),
            )
        },
        unsafe {
            table.ConnectionGetOptionBytes.unwrap()(
                &raw mut connection,
                autocommit.as_ptr(),
                null(),
                &raw mut length,
                null(),
            )
        },
        unsafe {
            table.ConnectionGetOptionInt.unwrap()(
                &raw mut connection,
                priority.as_ptr(),
                &raw mut integer,
                null(),
            )
        },
        unsafe {
            table.ConnectionGetOptionDouble.unwrap()(
                &raw mut connection,
                priority.as_ptr(),
                &raw mut double,
                null(),
            )
        },
    ] {
        assert_eq!(status, abi::ADBC_STATUS_NOT_FOUND);
    }
    assert_eq!(
        unsafe { table.ConnectionRelease.unwrap()(&raw mut connection, null()) },
        ADBC_STATUS_OK
    );
    assert!(connection.private_data.is_null());
}

#[test]
fn writes_only_the_prefix_for_a_1_0_0_caller() {
    let mut buffer = [0xAA_u8; size_of::<AdbcDriver>()];
    let mut error = zeroed_error();
    let status = unsafe {
        AdbcSpannerInit(
            ADBC_VERSION_1_0_0,
            buffer.as_mut_ptr().cast::<c_void>(),
            &raw mut error,
        )
    };
    assert_eq!(status, ADBC_STATUS_OK);
    assert!(
        buffer[ADBC_DRIVER_1_0_0_SIZE..]
            .iter()
            .all(|byte| *byte == 0xAA),
        "the 1.1.0 tail must be untouched"
    );
    assert!(
        buffer[..ADBC_DRIVER_1_0_0_SIZE]
            .iter()
            .any(|byte| *byte != 0xAA),
        "the 1.0.0 prefix must be populated"
    );
}

#[test]
fn rejects_an_unknown_version_as_not_implemented() {
    let mut table = std::mem::MaybeUninit::<AdbcDriver>::zeroed();
    let mut error = zeroed_error();
    let status =
        unsafe { AdbcSpannerInit(999, table.as_mut_ptr().cast::<c_void>(), &raw mut error) };
    assert_eq!(status, abi::ADBC_STATUS_NOT_IMPLEMENTED);
    let message = unsafe { std::ffi::CStr::from_ptr(error.message) }
        .to_string_lossy()
        .into_owned();
    assert!(
        message.contains("unsupported ADBC version 999"),
        "{message}"
    );
    unsafe { (error.release.unwrap())(&raw mut error) };
}

#[test]
fn rejects_a_null_vtable() {
    let mut error = zeroed_error();
    let status = unsafe { AdbcSpannerInit(ADBC_VERSION_1_1_0, null(), &raw mut error) };
    assert_eq!(status, abi::ADBC_STATUS_INVALID_ARGUMENT);
    unsafe { (error.release.unwrap())(&raw mut error) };
}

#[test]
fn releasing_the_driver_twice_reports_invalid_state() {
    let mut table = std::mem::MaybeUninit::<AdbcDriver>::zeroed();
    let mut error = zeroed_error();
    unsafe {
        AdbcSpannerInit(
            ADBC_VERSION_1_1_0,
            table.as_mut_ptr().cast::<c_void>(),
            &raw mut error,
        )
    };
    let mut table = unsafe { table.assume_init() };
    let release = table.release.unwrap();
    assert_eq!(unsafe { release(&raw mut table, null()) }, ADBC_STATUS_OK);
    assert_eq!(
        unsafe { release(&raw mut table, &raw mut error) },
        abi::ADBC_STATUS_INVALID_STATE
    );
    unsafe { (error.release.unwrap())(&raw mut error) };
}

use std::ffi::CString;

use super::*;
use crate::ffi::abi::{
    ADBC_STATUS_INVALID_ARGUMENT, ADBC_STATUS_INVALID_STATE, ADBC_STATUS_NOT_FOUND,
    ADBC_STATUS_NOT_IMPLEMENTED, ADBC_STATUS_OK,
};
use crate::ffi::options::{
    get_option_int, get_option_string, new_handle, release_handle, set_option_int,
    set_option_string,
};
use crate::ffi::test_support::{null, release_error, zeroed_error};

/// Nothing here connects, so the database need not exist.
const DATABASE: &str = "projects/a-project/instances/an-instance/databases/a-database";

fn pending() -> AdbcDatabase {
    let mut database = AdbcDatabase {
        private_data: null(),
        private_driver: null(),
    };
    let mut error = zeroed_error();
    assert_eq!(
        unsafe { new_handle(&raw mut database, &raw mut error) },
        ADBC_STATUS_OK
    );
    database
}

fn set(database: &mut AdbcDatabase, key: &str, value: &str) -> AdbcStatusCode {
    let key = CString::new(key).unwrap();
    let value = CString::new(value).unwrap();
    let mut error = zeroed_error();
    let status = unsafe {
        set_option_string(
            &raw mut *database,
            key.as_ptr(),
            value.as_ptr(),
            &raw mut error,
        )
    };
    release_error(&mut error);
    status
}

fn get(database: &mut AdbcDatabase, key: &str) -> std::result::Result<String, AdbcStatusCode> {
    let key = CString::new(key).unwrap();
    let mut error = zeroed_error();
    let mut length = 0_usize;
    // Ask for the size first.
    let status = unsafe {
        get_option_string(
            &raw mut *database,
            key.as_ptr(),
            null(),
            &raw mut length,
            &raw mut error,
        )
    };
    if status != ADBC_STATUS_OK {
        release_error(&mut error);
        return Err(status);
    }
    let mut buffer = vec![0_i8; length];
    let status = unsafe {
        get_option_string(
            &raw mut *database,
            key.as_ptr(),
            buffer.as_mut_ptr(),
            &raw mut length,
            &raw mut error,
        )
    };
    release_error(&mut error);
    if status != ADBC_STATUS_OK {
        return Err(status);
    }
    Ok(unsafe { std::ffi::CStr::from_ptr(buffer.as_ptr()) }
        .to_string_lossy()
        .into_owned())
}

/// An emulator-mode database pointed at an endpoint nothing listens on. `Init` makes no network
/// calls; the client is built by the first connection.
fn initialized() -> AdbcDatabase {
    let mut database = pending();
    for (key, value) in [
        (
            adbc_core::constants::ADBC_OPTION_URI,
            &*format!("spanner:///{DATABASE}"),
        ),
        (crate::OPTION_ENDPOINT, "http://127.0.0.1:1"),
        (crate::OPTION_EMULATOR, "true"),
    ] {
        assert_eq!(set(&mut database, key, value), ADBC_STATUS_OK);
    }
    let mut error = zeroed_error();
    assert_eq!(
        unsafe { database_init(&raw mut database, &raw mut error) },
        ADBC_STATUS_OK
    );
    database
}

fn release(database: &mut AdbcDatabase) -> AdbcStatusCode {
    let mut error = zeroed_error();
    let status = unsafe { release_handle(&raw mut *database, &raw mut error) };
    release_error(&mut error);
    status
}

/// Overwriting a populated handle would leak its object; the refusal leaves it releasable.
#[test]
fn a_second_new_on_a_populated_handle_is_refused() {
    let mut database = pending();
    let first = database.private_data;
    let mut error = zeroed_error();
    assert_eq!(
        unsafe { new_handle(&raw mut database, &raw mut error) },
        ADBC_STATUS_INVALID_STATE
    );
    release_error(&mut error);
    assert_eq!(database.private_data, first);
    assert_eq!(release(&mut database), ADBC_STATUS_OK);
}

#[test]
fn setting_the_same_option_twice_keeps_the_later_value() {
    let mut database = pending();
    assert_eq!(
        set(
            &mut database,
            adbc_core::constants::ADBC_OPTION_URI,
            "spanner:///projects/p/instances/i/databases/first"
        ),
        ADBC_STATUS_OK
    );
    assert_eq!(
        set(
            &mut database,
            adbc_core::constants::ADBC_OPTION_URI,
            "spanner:///projects/p/instances/i/databases/second"
        ),
        ADBC_STATUS_OK
    );
    assert_eq!(
        set(&mut database, crate::OPTION_EMULATOR, "true"),
        ADBC_STATUS_OK
    );
    // Nothing is readable before `Init`.
    assert_eq!(
        get(&mut database, adbc_core::constants::ADBC_OPTION_URI).unwrap_err(),
        ADBC_STATUS_NOT_FOUND
    );
    let mut error = zeroed_error();
    assert_eq!(
        unsafe { database_init(&raw mut database, &raw mut error) },
        ADBC_STATUS_OK
    );
    assert_eq!(
        get(&mut database, adbc_core::constants::ADBC_OPTION_URI).unwrap(),
        "spanner:///projects/p/instances/i/databases/second"
    );
    assert_eq!(release(&mut database), ADBC_STATUS_OK);
}

#[test]
fn repeated_options_keep_their_order_relative_to_uri_expansion() {
    let uri = adbc_core::constants::ADBC_OPTION_URI;
    let endpoint = crate::OPTION_ENDPOINT;
    for (options, expected) in [
        (
            [
                (endpoint, "http://first:9010"),
                (uri, "spanner://uri:9010/projects/p/instances/i/databases/d"),
                (endpoint, "http://last:9010"),
            ],
            "http://last:9010",
        ),
        (
            [
                (
                    uri,
                    "spanner://first:9010/projects/p/instances/i/databases/d",
                ),
                (endpoint, "http://explicit:9010"),
                (
                    uri,
                    "spanner://last:9010/projects/p/instances/i/databases/d",
                ),
            ],
            "last:9010",
        ),
    ] {
        let mut database = pending();
        for (key, value) in options {
            assert_eq!(set(&mut database, key, value), ADBC_STATUS_OK);
        }
        assert_eq!(
            unsafe { database_init(&raw mut database, null()) },
            ADBC_STATUS_OK
        );
        let found = get(&mut database, endpoint).unwrap();
        assert_eq!(release(&mut database), ADBC_STATUS_OK);
        assert_eq!(found, expected);
    }
}

#[test]
fn a_failed_init_keeps_the_buffered_options_for_a_retry() {
    let mut database = pending();
    assert_eq!(
        set(
            &mut database,
            adbc_core::constants::ADBC_OPTION_URI,
            &format!("spanner:///{DATABASE}")
        ),
        ADBC_STATUS_OK
    );
    assert_eq!(
        set(&mut database, crate::OPTION_ENDPOINT, "http://127.0.0.1:1"),
        ADBC_STATUS_OK
    );
    // Buffered now, rejected by `Init`.
    assert_eq!(
        set(&mut database, crate::OPTION_EMULATOR, "maybe"),
        ADBC_STATUS_OK
    );
    let mut error = zeroed_error();
    assert_ne!(
        unsafe { database_init(&raw mut database, &raw mut error) },
        ADBC_STATUS_OK
    );
    release_error(&mut error);

    assert_eq!(
        set(&mut database, crate::OPTION_EMULATOR, "true"),
        ADBC_STATUS_OK
    );
    let mut error = zeroed_error();
    assert_eq!(
        unsafe { database_init(&raw mut database, &raw mut error) },
        ADBC_STATUS_OK
    );
    assert_eq!(
        get(&mut database, adbc_core::constants::ADBC_OPTION_URI).unwrap(),
        format!("spanner:///{DATABASE}")
    );
    assert_eq!(
        get(&mut database, crate::OPTION_ENDPOINT).unwrap(),
        "http://127.0.0.1:1"
    );
    assert_eq!(release(&mut database), ADBC_STATUS_OK);
}

#[test]
fn initializing_twice_is_an_invalid_state() {
    let mut database = initialized();
    let mut error = zeroed_error();
    assert_eq!(
        unsafe { database_init(&raw mut database, &raw mut error) },
        ADBC_STATUS_INVALID_STATE
    );
    release_error(&mut error);
    assert_eq!(release(&mut database), ADBC_STATUS_OK);
}

#[test]
fn a_released_handle_rejects_every_call_including_a_second_release() {
    let mut database = initialized();
    assert_eq!(release(&mut database), ADBC_STATUS_OK);
    assert!(database.private_data.is_null());
    assert_eq!(release(&mut database), ADBC_STATUS_INVALID_STATE);
    assert_eq!(
        set(&mut database, crate::OPTION_ENDPOINT, "http://127.0.0.1:2"),
        ADBC_STATUS_INVALID_STATE
    );
    assert_eq!(
        get(&mut database, crate::OPTION_ENDPOINT).unwrap_err(),
        ADBC_STATUS_INVALID_STATE
    );
}

#[test]
fn null_handles_and_keys_are_rejected() {
    let mut error = zeroed_error();
    assert_eq!(
        unsafe { new_handle::<AdbcDatabase>(null(), &raw mut error) },
        ADBC_STATUS_INVALID_ARGUMENT
    );
    release_error(&mut error);

    let mut database = pending();
    let value = CString::new("x").unwrap();
    let mut error = zeroed_error();
    assert_eq!(
        unsafe { set_option_string(&raw mut database, null(), value.as_ptr(), &raw mut error) },
        ADBC_STATUS_INVALID_ARGUMENT
    );
    release_error(&mut error);

    // Rejected before the handle is consulted.
    let key = CString::new(adbc_core::constants::ADBC_OPTION_URI).unwrap();
    let mut error = zeroed_error();
    assert_eq!(
        unsafe { set_option_string(&raw mut database, key.as_ptr(), null(), &raw mut error) },
        ADBC_STATUS_INVALID_ARGUMENT
    );
    release_error(&mut error);
    assert_eq!(release(&mut database), ADBC_STATUS_OK);
}

/// Every database option is a string, so the typed getters must fail rather than return a
/// default.
#[test]
fn typed_getters_reject_string_options() {
    let mut database = initialized();
    let key = CString::new(crate::OPTION_ENDPOINT).unwrap();
    let mut value = 0_i64;
    let mut error = zeroed_error();
    assert_ne!(
        unsafe {
            get_option_int(
                &raw mut database,
                key.as_ptr(),
                &raw mut value,
                &raw mut error,
            )
        },
        ADBC_STATUS_OK
    );
    release_error(&mut error);
    assert_eq!(release(&mut database), ADBC_STATUS_OK);
}

/// Pre-`Init` sets are unvalidated; the live database checks the value type when `Init` replays
/// the buffer.
#[test]
fn a_typed_set_is_validated_by_the_live_database() {
    let mut database = pending();
    let key = CString::new(adbc_core::constants::ADBC_OPTION_URI).unwrap();
    assert_eq!(
        unsafe { set_option_int(&raw mut database, key.as_ptr(), 1, null()) },
        ADBC_STATUS_OK
    );

    let mut error = zeroed_error();
    assert_eq!(
        unsafe { database_init(&raw mut database, &raw mut error) },
        ADBC_STATUS_INVALID_ARGUMENT
    );
    release_error(&mut error);

    assert_eq!(
        set(
            &mut database,
            adbc_core::constants::ADBC_OPTION_URI,
            &format!("spanner:///{DATABASE}")
        ),
        ADBC_STATUS_OK
    );
    let mut error = zeroed_error();
    assert_eq!(
        unsafe { database_init(&raw mut database, &raw mut error) },
        ADBC_STATUS_OK
    );
    release_error(&mut error);
    assert_eq!(release(&mut database), ADBC_STATUS_OK);
}

/// adbc.h prefers `SetOption` to reject an unknown key immediately with `NOT_IMPLEMENTED`. Here
/// the database's setter is the only validator (`adbc.uri` expands into several keys), so a typo
/// is buffered and reported by `Init`, which names the key.
#[test]
fn a_typo_before_init_is_reported_when_init_replays_it() {
    let mut database = pending();
    assert_eq!(
        set(&mut database, "spanner.endpoin", "http://127.0.0.1:1"),
        ADBC_STATUS_OK
    );
    assert_eq!(
        set(
            &mut database,
            adbc_core::constants::ADBC_OPTION_URI,
            &format!("spanner:///{DATABASE}")
        ),
        ADBC_STATUS_OK
    );
    let mut error = zeroed_error();
    assert_eq!(
        unsafe { database_init(&raw mut database, &raw mut error) },
        ADBC_STATUS_NOT_IMPLEMENTED
    );
    let message = crate::ffi::test_support::error_message(&error)
        .expect("the refusal must name the offending key");
    release_error(&mut error);
    assert!(message.contains("spanner.endpoin"), "{message}");
    assert_eq!(release(&mut database), ADBC_STATUS_OK);
}

/// Before `Init` every key is buffered; afterwards the live database rejects the unknown ones.
#[test]
fn a_pre_init_set_accepts_every_key_the_live_database_classifies() {
    let mut live = initialized();
    let keys = [
        OptionDatabase::Uri,
        OptionDatabase::Username,
        OptionDatabase::Password,
        OptionDatabase::Other("spanner.no.such.option".to_owned()),
        OptionDatabase::Other(crate::OPTION_ENDPOINT.to_owned()),
        OptionDatabase::Other(crate::OPTION_EMULATOR.to_owned()),
        OptionDatabase::Other(crate::OPTION_KEYFILE.to_owned()),
        OptionDatabase::Other(crate::OPTION_KEYFILE_JSON.to_owned()),
        OptionDatabase::Other(crate::OPTION_ACCESS_TOKEN.to_owned()),
        OptionDatabase::Other(crate::OPTION_QUOTA_PROJECT.to_owned()),
        OptionDatabase::Other(crate::OPTION_IMPERSONATE_TARGET_PRINCIPAL.to_owned()),
        OptionDatabase::Other(crate::OPTION_IMPERSONATE_DELEGATES.to_owned()),
        OptionDatabase::Other(crate::OPTION_IMPERSONATE_SCOPES.to_owned()),
        OptionDatabase::Other(crate::OPTION_IMPERSONATE_LIFETIME.to_owned()),
    ];
    let mut unknown = 0;
    for key in &keys {
        let mut database = pending();
        assert_eq!(
            set(&mut database, key.as_ref(), ""),
            ADBC_STATUS_OK,
            "{}: a pre-init set buffers every key",
            key.as_ref()
        );
        assert_eq!(release(&mut database), ADBC_STATUS_OK);
        // An unknown key is `NOT_IMPLEMENTED`; an invalid value such as `""` for a URI is not.
        if set(&mut live, key.as_ref(), "") == ADBC_STATUS_NOT_IMPLEMENTED {
            unknown += 1;
        }
    }
    assert_eq!(
        unknown, 3,
        "username, password and the made-up key are the ones the live database does not know"
    );
    assert_eq!(release(&mut live), ADBC_STATUS_OK);
}

/// Every `AdbcDatabaseInit` uses the process-wide [`shared_driver`], so databases share one
/// Tokio runtime. `src/driver/tests.rs` checks that one driver's databases share its runtime.
#[test]
fn every_database_is_initialized_through_one_shared_driver() {
    let before = shared_driver().unwrap();
    let mut first = initialized();
    let mut second = initialized();
    let after = shared_driver().unwrap();

    assert!(
        std::ptr::eq(before, after),
        "AdbcDatabaseInit must not replace the process-wide driver"
    );
    assert_eq!(release(&mut first), ADBC_STATUS_OK);
    assert_eq!(release(&mut second), ADBC_STATUS_OK);
    // Releasing every database keeps the shared driver.
    assert!(std::ptr::eq(before, shared_driver().unwrap()));
}

//! Tests that load the exported entry point through `adbc_driver_manager`, as a host would.
//!
//! They stop at the database, since `AdbcConnectionInit` opens a session with a real RPC.
//! Connection and statement round trips run against a backend in `tests/integration.rs` and
//! `tests/ffi_lifecycle.rs`.

use std::ffi::{c_int, c_void};

use adbc_core::error::Status;
use adbc_core::options::{AdbcVersion, OptionDatabase};
use adbc_core::{Database, Driver, Optionable};
use adbc_driver_manager::ManagedDriver;

use super::abi::{AdbcError, AdbcStatusCode};

fn load_driver() -> ManagedDriver {
    // `load_static` wants `adbc_ffi`'s init function type. Ours differs only in the error
    // pointer's type, and both error structs have the same `#[repr(C)]` layout.
    type OurInit = unsafe extern "C" fn(c_int, *mut c_void, *mut AdbcError) -> AdbcStatusCode;
    let ours: OurInit = super::AdbcSpannerInit;
    let init: adbc_ffi::FFI_AdbcDriverInitFunc = unsafe { std::mem::transmute(ours) };
    ManagedDriver::load_static(&init, AdbcVersion::V110).unwrap()
}

/// An emulator-mode database pointed at an endpoint nothing listens on. `AdbcDatabaseInit` makes
/// no network calls, so no backend or credentials are needed.
fn anonymous_database() -> impl Database {
    load_driver()
        .new_database_with_opts([
            (
                OptionDatabase::Uri,
                "spanner:///projects/a-project/instances/an-instance/databases/a-database".into(),
            ),
            (
                OptionDatabase::Other(crate::OPTION_ENDPOINT.to_owned()),
                "http://127.0.0.1:1".into(),
            ),
            (
                OptionDatabase::Other(crate::OPTION_EMULATOR.to_owned()),
                "true".into(),
            ),
        ])
        .unwrap()
}

#[test]
fn exported_driver_database_options_round_trip() {
    let mut database = anonymous_database();
    assert_eq!(
        database
            .get_option_string(OptionDatabase::Other(crate::OPTION_ENDPOINT.to_owned()))
            .unwrap(),
        "http://127.0.0.1:1"
    );

    // Set after `Init`.
    database
        .set_option(
            OptionDatabase::Other(crate::OPTION_ENDPOINT.to_owned()),
            "http://127.0.0.1:2".into(),
        )
        .unwrap();
    assert_eq!(
        database
            .get_option_string(OptionDatabase::Other(crate::OPTION_ENDPOINT.to_owned()))
            .unwrap(),
        "http://127.0.0.1:2"
    );
}

#[test]
fn exported_driver_reports_unknown_options_with_a_message() {
    let mut driver = load_driver();
    let database = driver.new_database_with_opts([(
        OptionDatabase::Uri,
        "spanner:///projects/a-project/instances/an-instance/databases/a-database".into(),
    )]);
    let database = database.unwrap();
    let error = database
        .get_option_string(OptionDatabase::Other("spanner.nope".into()))
        .unwrap_err();
    assert!(!error.message.is_empty(), "the message must survive export");
}

/// Options are stored as strings: `get_option_bytes` returns the bytes, while the int and double
/// getters parse them and report `InvalidArguments` for a non-number.
#[test]
fn exported_driver_option_getters_reinterpret_the_stored_string() {
    let database = anonymous_database();
    let endpoint = OptionDatabase::Other(crate::OPTION_ENDPOINT.to_owned());

    assert_eq!(
        database.get_option_bytes(endpoint.clone()).unwrap(),
        b"http://127.0.0.1:1".to_vec()
    );
    for (label, error) in [
        (
            "string option through get_option_int",
            database.get_option_int(endpoint.clone()).unwrap_err(),
        ),
        (
            "string option through get_option_double",
            database.get_option_double(endpoint.clone()).unwrap_err(),
        ),
    ] {
        assert_eq!(error.status, Status::InvalidArguments, "{label}");
        assert!(!error.message.is_empty(), "{label}");
    }

    // adbc.h's documented failure for a key with no value.
    let unset = OptionDatabase::Other(crate::OPTION_KEYFILE.to_owned());
    assert_eq!(
        database.get_option_string(unset).unwrap_err().status,
        Status::NotFound
    );
}

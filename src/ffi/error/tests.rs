use adbc_core::error::Status;

use super::*;
use crate::ffi::abi::AdbcDriver;
use crate::ffi::test_support::{null, zeroed_error};

fn message_of(error: &AdbcError) -> String {
    unsafe { std::ffi::CStr::from_ptr(error.message) }
        .to_string_lossy()
        .into_owned()
}

/// A 1.0.0 error whose trailing bytes, outside the caller's allocation, hold the given values.
fn v100_error(private_data: *mut c_void, private_driver: *const AdbcDriver) -> AdbcError {
    AdbcError {
        message: null(),
        vendor_code: 0,
        sqlstate: [0; 5],
        release: None,
        private_data,
        private_driver,
    }
}

/// Run the 1.0.0 release callback `export_error` installed.
fn release_v100(out: &mut AdbcError) {
    let release: unsafe extern "C" fn(*mut AdbcErrorV100) =
        unsafe { std::mem::transmute(out.release.unwrap()) };
    unsafe { release((&raw mut *out).cast::<AdbcErrorV100>()) };
}

#[test]
fn exports_a_message_and_releases_idempotently() {
    let mut out = zeroed_error();
    unsafe {
        export_error(
            &raw mut out,
            Error::with_message_and_status("boom", Status::IO),
        );
    }
    assert_eq!(message_of(&out), "boom");
    assert_eq!(out.vendor_code, ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA);

    let release = out.release.expect("driver sets a release callback");
    unsafe { release(&raw mut out) };
    assert!(out.message.is_null());
    assert!(out.release.is_none());
    // A second release is a no-op.
    unsafe { release(&raw mut out) };
}

#[test]
fn reusing_an_out_parameter_releases_the_previous_error() {
    let mut out = zeroed_error();
    unsafe {
        export_error(
            &raw mut out,
            Error::with_message_and_status("first", Status::IO),
        );
        export_error(
            &raw mut out,
            Error::with_message_and_status("second", Status::Internal),
        );
    }
    assert_eq!(message_of(&out), "second");
    unsafe { (out.release.unwrap())(&raw mut out) };
}

/// A 1.0.0 caller's bytes after the prefix stay untouched. Mirrors the C++ suite's
/// `StatementTest.ErrorCompatibility`.
#[test]
fn a_1_0_0_caller_keeps_its_trailing_bytes() {
    let sentinel_driver = 0x5eed_usize as *const AdbcDriver;
    let sentinel_data = 0xf00d_usize as *mut c_void;
    let mut out = v100_error(sentinel_data, sentinel_driver);
    let mut error = Error::with_message_and_status("legacy", Status::NotFound);
    error.vendor_code = 42;
    unsafe { export_error(&raw mut out, error) };

    assert_eq!(message_of(&out), "legacy");
    assert_eq!(out.vendor_code, 42);
    assert_eq!(out.private_data, sentinel_data);
    assert_eq!(out.private_driver, sentinel_driver);

    release_v100(&mut out);
    assert_eq!(out.private_data, sentinel_data);
}

/// A vendor code equal to the sentinel, stored in a 1.0.0 struct, would make the next export
/// write a 1.1.0 struct past the caller's allocation.
#[test]
fn a_1_0_0_caller_is_not_relabelled_by_a_sentinel_valued_vendor_code() {
    let sentinel_driver = 0x5eed_usize as *const AdbcDriver;
    let sentinel_data = 0xf00d_usize as *mut c_void;
    let mut out = v100_error(sentinel_data, sentinel_driver);

    let mut error = Error::with_message_and_status("first", Status::Internal);
    error.vendor_code = ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA;
    unsafe { export_error(&raw mut out, error) };
    assert_ne!(out.vendor_code, ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA);

    unsafe {
        export_error(
            &raw mut out,
            Error::with_message_and_status("second", Status::Internal),
        );
    }
    assert_eq!(message_of(&out), "second");
    assert_eq!(out.private_data, sentinel_data);
    assert_eq!(out.private_driver, sentinel_driver);

    release_v100(&mut out);
}

#[test]
fn round_trips_error_details() {
    let mut error = Error::with_message_and_status("detailed", Status::Internal);
    error.details = Some(vec![
        ("k1".to_string(), b"v1".to_vec()),
        ("k2".to_string(), b"".to_vec()),
    ]);
    let mut out = zeroed_error();
    unsafe { export_error(&raw mut out, error) };

    assert_eq!(unsafe { error_get_detail_count(&raw const out) }, 2);
    let first = unsafe { error_get_detail(&raw const out, 0) };
    assert_eq!(
        unsafe { std::ffi::CStr::from_ptr(first.key) }
            .to_str()
            .unwrap(),
        "k1"
    );
    assert_eq!(
        unsafe { std::slice::from_raw_parts(first.value, first.value_length) },
        b"v1"
    );

    // Out-of-range indices yield an empty detail.
    assert!(unsafe { error_get_detail(&raw const out, 2) }.key.is_null());
    assert!(
        unsafe { error_get_detail(&raw const out, -1) }
            .key
            .is_null()
    );

    unsafe { (out.release.unwrap())(&raw mut out) };
    assert_eq!(unsafe { error_get_detail_count(&raw const out) }, 0);
}

/// Without the sentinel, `private_data` is outside the caller's allocation. A 1.0.0 error
/// keeps its gRPC code in `vendor_code` and exposes no details.
#[test]
fn the_detail_getters_ignore_private_data_on_a_1_0_0_error() {
    let sentinel_data = 0xf00d_usize as *mut c_void;
    let mut out = v100_error(sentinel_data, null());
    let mut error = Error::with_message_and_status("aborted", Status::IO);
    error.vendor_code = 10; // gRPC ABORTED
    error.details = Some(vec![("google.rpc.retryinfo".to_string(), b"{}".to_vec())]);
    unsafe { export_error(&raw mut out, error) };

    assert_eq!(out.vendor_code, 10);
    assert_eq!(out.private_data, sentinel_data);
    assert_eq!(unsafe { error_get_detail_count(&raw const out) }, 0);
    assert!(unsafe { error_get_detail(&raw const out, 0) }.key.is_null());

    release_v100(&mut out);
}

#[test]
fn null_out_parameters_are_tolerated() {
    unsafe {
        export_error(
            null(),
            Error::with_message_and_status("dropped", Status::IO),
        );
    }
    assert_eq!(unsafe { error_get_detail_count(null()) }, 0);
    assert!(unsafe { error_get_detail(null(), 0) }.key.is_null());
}

#[test]
fn a_message_with_an_interior_nul_is_still_reported() {
    let mut out = zeroed_error();
    unsafe {
        export_error(
            &raw mut out,
            Error::with_message_and_status("before\0after", Status::IO),
        );
    }
    assert_eq!(message_of(&out), "before?after");
    unsafe { (out.release.unwrap())(&raw mut out) };
}

/// adbc.h: `private_data` "is NULLPTR iff the error is uninitialized/freed".
#[test]
fn a_detail_free_error_still_marks_itself_initialized() {
    let mut out = zeroed_error();
    unsafe {
        export_error(
            &raw mut out,
            Error::with_message_and_status("plain", Status::IO),
        );
    }
    assert!(!out.private_data.is_null());
    assert_eq!(unsafe { error_get_detail_count(&raw const out) }, 0);

    unsafe { (out.release.unwrap())(&raw mut out) };
    assert!(out.private_data.is_null());
}

/// The driver manager sets `private_driver` before the call and reads it afterwards, so it must
/// survive releasing a previous error, even with a callback that zeroes the whole struct as the
/// reference C++ framework's does.
#[test]
fn a_foreign_release_cannot_take_private_driver_with_it() {
    unsafe extern "C" fn clearing_release(error: *mut AdbcError) {
        unsafe { std::ptr::write_bytes(error.cast::<u8>(), 0, size_of::<AdbcError>()) };
    }

    let sentinel_driver = 0x5eed_usize as *const AdbcDriver;
    let mut out = AdbcError {
        message: null(),
        vendor_code: ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA,
        sqlstate: [0; 5],
        release: Some(clearing_release),
        private_data: null(),
        private_driver: sentinel_driver,
    };
    unsafe {
        export_error(
            &raw mut out,
            Error::with_message_and_status("boom", Status::IO),
        );
    }
    assert_eq!(out.private_driver, sentinel_driver);
    assert_eq!(out.vendor_code, ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA);
    unsafe { (out.release.unwrap())(&raw mut out) };
}

/// The error's details, read through the getter entry points.
fn details_of(error: &AdbcError) -> Vec<(String, Vec<u8>)> {
    (0..unsafe { error_get_detail_count(&raw const *error) })
        .map(|index| {
            let detail = unsafe { error_get_detail(&raw const *error, index) };
            let key = unsafe { std::ffi::CStr::from_ptr(detail.key) }
                .to_string_lossy()
                .into_owned();
            let value = unsafe { std::slice::from_raw_parts(detail.value, detail.value_length) };
            (key, value.to_vec())
        })
        .collect()
}

/// The 1.1.0 sentinel occupies `vendor_code`, so the gRPC code becomes a detail, appended after
/// the forwarded `google.rpc.*` details so their indices stay stable.
#[test]
fn a_1_1_0_error_reports_its_vendor_code_as_a_detail() {
    let mut error = Error::with_message_and_status("aborted", Status::IO);
    error.vendor_code = 10; // gRPC ABORTED
    error.details = Some(vec![(
        "google.rpc.retryinfo".to_string(),
        br#"{"@type":"type.googleapis.com/google.rpc.RetryInfo"}"#.to_vec(),
    )]);
    let mut out = zeroed_error();
    unsafe { export_error(&raw mut out, error) };

    assert_eq!(out.vendor_code, ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA);
    let details = details_of(&out);
    assert_eq!(
        details
            .iter()
            .map(|(key, _)| key.as_str())
            .collect::<Vec<_>>(),
        ["google.rpc.retryinfo", VENDOR_CODE_DETAIL_KEY],
        "the forwarded detail must stay at index 0"
    );
    // Text, since the key has no `-bin` suffix.
    assert_eq!(details[1].1, b"10");

    unsafe { (out.release.unwrap())(&raw mut out) };
}

/// `from_spanner` uses vendor code 0 when there was no gRPC status; reporting it would claim
/// `OK`.
#[test]
fn a_zero_vendor_code_adds_no_detail() {
    let mut error = Error::with_message_and_status("transport failure", Status::Internal);
    error.details = Some(vec![("k1".to_string(), b"v1".to_vec())]);
    assert_eq!(error.vendor_code, 0, "the default is the no-status value");
    let mut out = zeroed_error();
    unsafe { export_error(&raw mut out, error) };

    assert_eq!(
        details_of(&out),
        [("k1".to_string(), b"v1".to_vec())],
        "a zero vendor code must not add a detail"
    );

    unsafe { (out.release.unwrap())(&raw mut out) };
}

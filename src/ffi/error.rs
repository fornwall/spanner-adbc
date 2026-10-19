//! Writing a driver [`Error`] into a caller-owned `AdbcError`.
//!
//! The caller allocates the struct, so its size depends on the ADBC revision it was built
//! against. A 1.1.0 caller presets `vendor_code` to [`ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA`].
//! Otherwise only the 1.0.0 prefix exists, and the two trailing fields must not be touched.

use std::ffi::{CString, c_int, c_void};

use adbc_core::error::Error;

use super::abi::{ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA, AdbcError, AdbcErrorDetail, AdbcErrorV100};
use super::guard::sanitized_cstring;

/// Backing store for [`error_get_detail`], whose pointers must stay valid until the error is
/// released.
type ErrorDetails = Vec<(CString, Vec<u8>)>;

/// Detail key for the gRPC code that the 1.1.0 sentinel displaces from `vendor_code`.
const VENDOR_CODE_DETAIL_KEY: &str = "adbc.spanner.vendor_code";

/// # Safety
/// `error` must be a pointer this driver previously wrote a 1.1.0 error into.
// Guard-exempt: freeing a `CString` and the details box cannot panic.
unsafe extern "C" fn release_error(error: *mut AdbcError) {
    let Some(error) = (unsafe { error.as_mut() }) else {
        return;
    };
    if !error.message.is_null() {
        drop(unsafe { CString::from_raw(error.message) });
        error.message = std::ptr::null_mut();
    }
    if !error.private_data.is_null() {
        drop(unsafe { Box::from_raw(error.private_data.cast::<ErrorDetails>()) });
        error.private_data = std::ptr::null_mut();
    }
    // adbc.h requires release to be idempotent.
    error.release = None;
}

/// # Safety
/// `error` must be a pointer this driver previously wrote a 1.0.0 error into.
// Guard-exempt: freeing a `CString` cannot panic.
unsafe extern "C" fn release_error_v100(error: *mut AdbcErrorV100) {
    let Some(error) = (unsafe { error.as_mut() }) else {
        return;
    };
    if !error.message.is_null() {
        drop(unsafe { CString::from_raw(error.message) });
        error.message = std::ptr::null_mut();
    }
    error.release = None;
}

/// Write `error` into the caller's out parameter, respecting whichever ABI revision it allocated.
///
/// # Safety
/// `out` must be null or point to a zero-initialized or previously-populated `AdbcError` whose
/// size matches the revision its `vendor_code` sentinel advertises.
pub(crate) unsafe fn export_error(out: *mut AdbcError, error: Error) {
    if out.is_null() {
        return;
    }

    // Preserve the ABI sentinel and manager vtable before release, which may zero the struct.
    // Read the vtable only for 1.1 callers: the field is outside a 1.0 allocation.
    let is_v110 = unsafe { (*out).vendor_code } == ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA;
    let private_driver = if is_v110 {
        unsafe { (*out).private_driver }
    } else {
        std::ptr::null()
    };

    // The out parameter may still hold an earlier error.
    if let Some(release) = unsafe { (*out).release } {
        unsafe { release(out) };
    }

    if is_v110 {
        // ADBC requires non-null private_data for initialized errors, even without details.
        let mut details: ErrorDetails = error
            .details
            .unwrap_or_default()
            .into_iter()
            .map(|(key, value)| (sanitized_cstring(&key), value))
            .collect();
        // Preserve a nonzero vendor code as a decimal-text detail; the ABI sentinel occupies
        // vendor_code in 1.1. Append it to keep forwarded detail indices stable.
        if error.vendor_code != 0 {
            details.push((
                sanitized_cstring(VENDOR_CODE_DETAIL_KEY),
                error.vendor_code.to_string().into_bytes(),
            ));
        }
        let value = AdbcError {
            message: sanitized_cstring(&error.message).into_raw(),
            vendor_code: ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA,
            sqlstate: error.sqlstate,
            release: Some(release_error),
            private_data: Box::into_raw(Box::new(details)).cast::<c_void>(),
            private_driver,
        };
        unsafe { std::ptr::write_unaligned(out, value) };
    } else {
        let value = AdbcErrorV100 {
            message: sanitized_cstring(&error.message).into_raw(),
            // Never mark a 1.0 allocation as 1.1: a later export would write beyond its end.
            vendor_code: if error.vendor_code == ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA {
                0
            } else {
                error.vendor_code
            },
            sqlstate: error.sqlstate,
            release: Some(release_error_v100),
        };
        // The caller allocated only the 1.0.0 prefix.
        unsafe { std::ptr::write_unaligned(out.cast::<AdbcErrorV100>(), value) };
    }
}

/// The detail box behind `error`, if it is a 1.1.0 error.
///
/// adbc.h forbids reading `private_data` unless `vendor_code` holds the sentinel, since a 1.0.0
/// allocation ends before it. For the same reason, reads go through field addresses rather than
/// a reference to the whole struct.
///
/// # Safety
/// `error` must be null or point to at least the 1.0.0 prefix of a valid `AdbcError`.
unsafe fn details<'a>(error: *const AdbcError) -> Option<&'a ErrorDetails> {
    if error.is_null() {
        return None;
    }
    if unsafe { std::ptr::read_unaligned(&raw const (*error).vendor_code) }
        != ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA
    {
        return None;
    }
    let private_data = unsafe { std::ptr::read_unaligned(&raw const (*error).private_data) };
    unsafe { private_data.cast::<ErrorDetails>().as_ref() }
}

/// # Safety
/// `error` must be null or point to a valid `AdbcError`.
// Guard-exempt: reads only, with a saturating conversion.
pub(crate) unsafe extern "C" fn error_get_detail_count(error: *const AdbcError) -> c_int {
    let Some(details) = (unsafe { details(error) }) else {
        return 0;
    };
    c_int::try_from(details.len()).unwrap_or(c_int::MAX)
}

/// # Safety
/// `error` must be null or point to a valid `AdbcError`.
// Guard-exempt: reads only, through checked lookups.
pub(crate) unsafe extern "C" fn error_get_detail(
    error: *const AdbcError,
    index: c_int,
) -> AdbcErrorDetail {
    let empty = AdbcErrorDetail {
        key: std::ptr::null(),
        value: std::ptr::null(),
        value_length: 0,
    };
    let Some(details) = (unsafe { details(error) }) else {
        return empty;
    };
    let Ok(index) = usize::try_from(index) else {
        return empty;
    };
    let Some((key, value)) = details.get(index) else {
        return empty;
    };
    AdbcErrorDetail {
        key: key.as_ptr(),
        value: value.as_ptr(),
        value_length: value.len(),
    }
}

#[cfg(test)]
mod tests;

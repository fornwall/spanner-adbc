//! Helpers shared by the C ABI unit tests.

use super::abi::AdbcError;

/// A zeroed `AdbcError` as a 1.1.0 caller passes it, with the sentinel `vendor_code`.
pub(super) fn zeroed_error() -> AdbcError {
    AdbcError::default()
}

/// A zero-initialized ABI struct, as `New` expects and `Release` leaves behind.
pub(super) fn zeroed<T>() -> T {
    unsafe { std::mem::zeroed() }
}

/// Run the error's release callback, if set.
pub(super) fn release_error(error: &mut AdbcError) {
    if let Some(release) = error.release {
        unsafe { release(&raw mut *error) };
    }
}

/// The error's message, if set.
pub(super) fn error_message(error: &AdbcError) -> Option<String> {
    if error.message.is_null() {
        return None;
    }
    Some(
        unsafe { std::ffi::CStr::from_ptr(error.message) }
            .to_string_lossy()
            .into_owned(),
    )
}

/// Null for any pointer argument; `*mut T` coerces to `*const T`.
pub(super) fn null<T>() -> *mut T {
    std::ptr::null_mut()
}

/// A panic payload whose destructor panics with another `PanicOnDrop`.
pub(super) struct PanicOnDrop;

impl Drop for PanicOnDrop {
    fn drop(&mut self) {
        std::panic::panic_any(Self);
    }
}

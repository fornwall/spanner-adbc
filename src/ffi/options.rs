//! The eight option entry points, shared by all three objects as generic `extern "C"` functions
//! over [`FfiHandle`] and [`OptionTarget`].

use std::ffi::c_char;

use adbc_core::Optionable;
use adbc_core::error::Result;
use adbc_core::options::OptionValue;

use super::abi::{AdbcError, AdbcStatusCode};
use super::guard::{byte_slice, required_str, write_bytes, write_out, write_string};
use super::handle::{FfiHandle, Staged, finish, with_state};
use crate::error::not_found;

/// How options reach the state behind a handle. A [`Staged`] object buffers sets and refuses gets
/// until `Init`; a statement has no `Init` and does neither.
pub(super) trait OptionTarget {
    /// The typed option key, parsed from the caller's C string.
    type Key: for<'a> From<&'a str>;
    /// The live object the typed getters read from.
    type Options: Optionable<Option = Self::Key>;

    /// Apply an option, or buffer it before `Init`.
    fn set(&mut self, key: Self::Key, value: OptionValue) -> Result<()>;

    /// The initialized object, or `NOT_FOUND` when the option cannot be read yet.
    fn options(&mut self, kind: &str, key: &Self::Key) -> Result<&mut Self::Options>;
}

impl<K, S> OptionTarget for Staged<K, S>
where
    K: PartialEq + for<'a> From<&'a str> + AsRef<str>,
    S: Optionable<Option = K>,
{
    type Key = K;
    type Options = S;

    fn set(&mut self, key: K, value: OptionValue) -> Result<()> {
        match self {
            Self::Pending(options) => {
                options.set(key, value);
                Ok(())
            }
            Self::Ready(state) => state.set_option(key, value),
        }
    }

    /// Buffered options are unvalidated, so they read as `NOT_FOUND` until `Init`.
    fn options(&mut self, kind: &str, key: &K) -> Result<&mut S> {
        match self {
            Self::Ready(state) => Ok(state),
            Self::Pending(_) => Err(not_found(format!(
                "Spanner {kind} option {} cannot be read before the {kind} is initialized; \
                 initialize it first",
                key.as_ref()
            ))),
        }
    }
}

type KeyOf<H> = <<H as FfiHandle>::State as OptionTarget>::Key;

/// # Safety
/// `key` must be null or a NUL-terminated string.
unsafe fn parse_key<H: FfiHandle<State: OptionTarget>>(key: *const c_char) -> Result<KeyOf<H>> {
    Ok(KeyOf::<H>::from(unsafe { required_str(key, "key") }?))
}

/// Apply an already-converted option value.
///
/// # Safety
/// `handle` and `key` must be null or valid for the call.
unsafe fn set_option<H: FfiHandle<State: OptionTarget>>(
    handle: *mut H,
    key: *const c_char,
    value: OptionValue,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe {
        with_state(handle, error, |state| {
            let key = parse_key::<H>(key)?;
            state.set(key, value)
        })
    }
}

/// # Safety
/// `handle`, `key` and `value` must be null or valid for the call.
pub(super) unsafe extern "C" fn set_option_string<H: FfiHandle<State: OptionTarget>>(
    handle: *mut H,
    key: *const c_char,
    value: *const c_char,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    // Checked before the handle, so a null value is a bad argument even on a released handle.
    let value = match unsafe { required_str(value, "value") } {
        Ok(value) => OptionValue::String(value.to_string()),
        Err(failure) => return unsafe { finish(Err(failure), error) },
    };
    unsafe { set_option(handle, key, value, error) }
}

/// # Safety
/// `handle` and `key` must be null or valid for the call, and `value` null or point to `length`
/// initialized bytes.
pub(super) unsafe extern "C" fn set_option_bytes<H: FfiHandle<State: OptionTarget>>(
    handle: *mut H,
    key: *const c_char,
    value: *const u8,
    length: usize,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    let value = match unsafe { byte_slice(value, length, "value") } {
        Ok(value) => OptionValue::Bytes(value.to_vec()),
        Err(failure) => return unsafe { finish(Err(failure), error) },
    };
    unsafe { set_option(handle, key, value, error) }
}

/// # Safety
/// `handle` and `key` must be null or valid for the call.
pub(super) unsafe extern "C" fn set_option_int<H: FfiHandle<State: OptionTarget>>(
    handle: *mut H,
    key: *const c_char,
    value: i64,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe { set_option(handle, key, OptionValue::Int(value), error) }
}

/// # Safety
/// `handle` and `key` must be null or valid for the call.
pub(super) unsafe extern "C" fn set_option_double<H: FfiHandle<State: OptionTarget>>(
    handle: *mut H,
    key: *const c_char,
    value: f64,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe { set_option(handle, key, OptionValue::Double(value), error) }
}

/// # Safety
/// `handle`, `key`, `value` and `length` must be null or valid for the call.
pub(super) unsafe extern "C" fn get_option_string<H: FfiHandle<State: OptionTarget>>(
    handle: *mut H,
    key: *const c_char,
    value: *mut c_char,
    length: *mut usize,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe {
        with_state(handle, error, |state| {
            let key = parse_key::<H>(key)?;
            let found = state.options(H::KIND, &key)?.get_option_string(key)?;
            write_string(&found, value, length)
        })
    }
}

/// # Safety
/// `handle`, `key`, `value` and `length` must be null or valid for the call.
pub(super) unsafe extern "C" fn get_option_bytes<H: FfiHandle<State: OptionTarget>>(
    handle: *mut H,
    key: *const c_char,
    value: *mut u8,
    length: *mut usize,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe {
        with_state(handle, error, |state| {
            let key = parse_key::<H>(key)?;
            let found = state.options(H::KIND, &key)?.get_option_bytes(key)?;
            write_bytes(&found, value, length)
        })
    }
}

/// # Safety
/// `handle`, `key` and `value` must be null or valid for the call.
pub(super) unsafe extern "C" fn get_option_int<H: FfiHandle<State: OptionTarget>>(
    handle: *mut H,
    key: *const c_char,
    value: *mut i64,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe {
        with_state(handle, error, |state| {
            let key = parse_key::<H>(key)?;
            let found = state.options(H::KIND, &key)?.get_option_int(key)?;
            write_out(value, found, "value")
        })
    }
}

/// # Safety
/// `handle`, `key` and `value` must be null or valid for the call.
pub(super) unsafe extern "C" fn get_option_double<H: FfiHandle<State: OptionTarget>>(
    handle: *mut H,
    key: *const c_char,
    value: *mut f64,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe {
        with_state(handle, error, |state| {
            let key = parse_key::<H>(key)?;
            let found = state.options(H::KIND, &key)?.get_option_double(key)?;
            write_out(value, found, "value")
        })
    }
}

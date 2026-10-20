//! `New`, `Release` and the eight option entry points, shared by all three objects as generic
//! `extern "C"` functions over [`FfiHandle`] and [`OptionTarget`].

use std::ffi::{c_char, c_void};
use std::sync::MutexGuard;

use adbc_core::Optionable;
use adbc_core::error::Result;
use adbc_core::options::OptionValue;

use super::abi::{ADBC_STATUS_OK, AdbcError, AdbcStatusCode};
use super::guard::{
    byte_slice, missing_argument, required_str, write_bytes, write_out, write_string,
};
use super::handle::{Exported, Staged, dispatch, finish};
use crate::error::{invalid_state, not_found};

/// An ABI handle struct: `AdbcDatabase`, `AdbcConnection` or `AdbcStatement`.
pub(super) trait FfiHandle {
    /// What lives behind `private_data`.
    type State: OptionTarget + Send;
    /// The object's name in diagnostics: "database", "connection" or "statement".
    const KIND: &'static str;
    fn private_data(&self) -> *mut c_void;
    fn private_data_mut(&mut self) -> &mut *mut c_void;
}

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

/// The handle's `private_data`, or null for a null handle.
///
/// # Safety
/// `handle` must be null or point to a valid ABI struct.
pub(super) unsafe fn slot_of<H: FfiHandle>(handle: *mut H) -> *mut c_void {
    unsafe { handle.as_ref() }.map_or(std::ptr::null_mut(), FfiHandle::private_data)
}

/// Reject a null handle pointer as a bad argument. A null `private_data` is instead an object
/// used out of order.
///
/// # Safety
/// `handle` must be null or point to a valid ABI struct.
unsafe fn checked<'a, H: FfiHandle>(handle: *mut H) -> Result<&'a mut H> {
    unsafe { handle.as_mut() }.ok_or_else(|| missing_argument(H::KIND))
}

/// Run `f` on the state behind `handle` through [`dispatch`].
///
/// # Safety
/// `handle` must be null or point to a valid ABI struct.
pub(super) unsafe fn with_state<H, F>(handle: *mut H, error: *mut AdbcError, f: F) -> AdbcStatusCode
where
    H: FfiHandle,
    F: FnOnce(&mut H::State) -> Result<()>,
{
    unsafe { dispatch::<H::State, _>(slot_of(handle), H::KIND, error, f) }
}

/// Refuse `*New` on a populated handle, which would leak its object. ADBC requires a
/// zero-initialized struct.
pub(super) fn already_populated(kind: &str) -> adbc_core::error::Error {
    invalid_state(format!(
        "{kind} already holds a driver object; release it before creating another, and pass a \
         zero-initialized struct to a first one"
    ))
}

/// `AdbcDatabaseNew`/`AdbcConnectionNew`. Statements have no `Init` and are built by
/// [`super::statement::statement_new`] instead.
///
/// # Safety
/// `handle` must be null or point to a valid ABI struct.
// Guard-exempt: relies on the `Staged` default allocating nothing; allocation failure for
// `Exported` aborts rather than panics.
pub(super) unsafe extern "C" fn new_handle<H: FfiHandle>(
    handle: *mut H,
    error: *mut AdbcError,
) -> AdbcStatusCode
where
    H::State: Default,
{
    match unsafe { checked(handle) } {
        Ok(handle) if !handle.private_data().is_null() => {
            finish(Err(already_populated(H::KIND)), error)
        }
        Ok(handle) => {
            *handle.private_data_mut() = Exported::into_private_data(H::State::default());
            ADBC_STATUS_OK
        }
        Err(failure) => finish(Err(failure), error),
    }
}

/// Every `*Release`.
///
/// # Safety
/// `handle` must be null or point to a valid ABI struct, and no other call on the object may be
/// in flight.
pub(super) unsafe extern "C" fn release_handle<H: FfiHandle>(
    handle: *mut H,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    match unsafe { checked(handle) } {
        Ok(handle) => unsafe {
            super::handle::release::<H::State>(handle.private_data_mut(), H::KIND, error)
        },
        Err(failure) => finish(Err(failure), error),
    }
}

/// Lock the state behind a second handle that an entry point needs besides the one it dispatches
/// on. The caller must still check that the state is ready.
///
/// # Safety
/// `handle` must be null or point to a valid ABI struct, and the guard must not outlive it.
pub(super) unsafe fn borrow<'a, H: FfiHandle>(handle: *mut H) -> Result<MutexGuard<'a, H::State>> {
    unsafe { super::handle::lock_state::<H::State>(slot_of(handle), H::KIND) }
}

/// # Safety
/// `key` must be null or a NUL-terminated string.
unsafe fn parse_key<H: FfiHandle>(key: *const c_char) -> Result<KeyOf<H>> {
    Ok(KeyOf::<H>::from(unsafe { required_str(key, "key") }?))
}

/// Apply an already-converted option value.
///
/// # Safety
/// `handle` and `key` must be null or valid for the call.
unsafe fn set_option<H: FfiHandle>(
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
pub(super) unsafe extern "C" fn set_option_string<H: FfiHandle>(
    handle: *mut H,
    key: *const c_char,
    value: *const c_char,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    // Checked before the handle, so a null value is a bad argument even on a released handle.
    let value = match unsafe { required_str(value, "value") } {
        Ok(value) => OptionValue::String(value.to_string()),
        Err(failure) => return finish(Err(failure), error),
    };
    unsafe { set_option(handle, key, value, error) }
}

/// # Safety
/// `handle` and `key` must be null or valid for the call, and `value` null or point to `length`
/// initialized bytes.
pub(super) unsafe extern "C" fn set_option_bytes<H: FfiHandle>(
    handle: *mut H,
    key: *const c_char,
    value: *const u8,
    length: usize,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    let value = match unsafe { byte_slice(value, length, "value") } {
        Ok(value) => OptionValue::Bytes(value.to_vec()),
        Err(failure) => return finish(Err(failure), error),
    };
    unsafe { set_option(handle, key, value, error) }
}

/// # Safety
/// `handle` and `key` must be null or valid for the call.
pub(super) unsafe extern "C" fn set_option_int<H: FfiHandle>(
    handle: *mut H,
    key: *const c_char,
    value: i64,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe { set_option(handle, key, OptionValue::Int(value), error) }
}

/// # Safety
/// `handle` and `key` must be null or valid for the call.
pub(super) unsafe extern "C" fn set_option_double<H: FfiHandle>(
    handle: *mut H,
    key: *const c_char,
    value: f64,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe { set_option(handle, key, OptionValue::Double(value), error) }
}

/// # Safety
/// `handle`, `key`, `value` and `length` must be null or valid for the call.
pub(super) unsafe extern "C" fn get_option_string<H: FfiHandle>(
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
pub(super) unsafe extern "C" fn get_option_bytes<H: FfiHandle>(
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
pub(super) unsafe extern "C" fn get_option_int<H: FfiHandle>(
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
pub(super) unsafe extern "C" fn get_option_double<H: FfiHandle>(
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

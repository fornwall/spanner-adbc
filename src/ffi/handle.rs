//! The object behind each handle's `private_data`, and how entry points reach it.
//!
//! Each handle owns an [`Arc<Exported<S>>`]. Calls serialize on its state mutex, and a panic
//! poisons every state the panicking call locked. [`cancel`] avoids that lock so it can interrupt
//! a blocked call.
//!
//! Callers must not overlap `Release` with any other call, including `Cancel`: reading
//! `private_data` and taking a reference are not atomic. Once taken, the cancel reference keeps
//! the state alive until cancellation returns.

use std::ffi::c_void;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use adbc_core::CancelHandle;
use adbc_core::error::{Error, Result, Status};
use adbc_core::options::OptionValue;

use super::abi::{ADBC_STATUS_OK, AdbcError, AdbcStatusCode};
use super::error::export_error;
use super::guard::{catch, missing_argument};
use crate::error::invalid_state;

/// An ABI handle struct: `AdbcDatabase`, `AdbcConnection` or `AdbcStatement`.
pub(super) trait FfiHandle {
    /// What lives behind `private_data`.
    type State: Send;
    /// The object's name in diagnostics: "database", "connection" or "statement".
    const KIND: &'static str;
    /// The `private_data` field, as a raw place because calls on a handle can overlap.
    fn private_data(handle: *mut Self) -> *mut *mut c_void;
}

/// A database or connection before or after `Init`. ADBC splits construction across
/// `New`/`SetOption`/`Init`, while the driver's constructors take all options at once.
pub(crate) enum Staged<K, S> {
    /// Options set before `Init`, ordered by each key's latest set: a URI expands into individual
    /// options, so an endpoint set after it must override it, even if also set before.
    Pending(Vec<(K, OptionValue)>),
    Ready(S),
}

/// The state `New` installs.
impl<K, S> Default for Staged<K, S> {
    fn default() -> Self {
        Self::Pending(Vec::new())
    }
}

impl<K, S> Staged<K, S> {
    /// The initialized state, or `InvalidState` before `Init`.
    pub(crate) fn ready(&mut self, kind: &str) -> Result<&mut S> {
        match self {
            Self::Ready(state) => Ok(state),
            Self::Pending(_) => Err(uninitialized(kind)),
        }
    }

    /// The buffered options, or `InvalidState` after `Init`. `Init` clones them rather than taking
    /// them, so a failed `Init` can be retried.
    pub(crate) fn pending(&self, kind: &str) -> Result<&[(K, OptionValue)]> {
        match self {
            Self::Pending(options) => Ok(options),
            Self::Ready(_) => Err(Error::with_message_and_status(
                format!("{kind} is already initialized"),
                Status::InvalidState,
            )),
        }
    }
}

/// What a handle's `private_data` points to.
///
/// The caller may share the handle between threads, so everything is reached through
/// `&Exported<S>` and interior mutability. A `&mut` to the whole struct would alias when calls
/// overlap, which adbc.h permits for `Cancel` and the option getters.
pub(crate) struct Exported<S> {
    /// Set once the state is initialized. Kept outside `state` so [`cancel`] works while another
    /// call holds the state lock.
    cancel: OnceLock<Box<dyn CancelHandle>>,
    /// Taken by every call except [`cancel`].
    state: Mutex<S>,
}

// Raw pointers bypass `Send`/`Sync` checks at call sites. Fail to compile if a new field makes
// sharing a handle between threads unsound.
fn _exported_is_shareable<S: Send>(exported: &Exported<S>) -> &(dyn Send + Sync) {
    exported
}

// `S: Send` because the caller may use the object from several threads; the mutex prevents
// concurrent access, so `S: Sync` is not needed.
impl<S: Send> Exported<S> {
    pub(crate) fn into_private_data(state: S) -> *mut c_void {
        Arc::into_raw(Arc::new(Self {
            cancel: OnceLock::new(),
            state: Mutex::new(state),
        }))
        .cast::<c_void>()
        .cast_mut()
    }
}

/// For a null slot, a pending state or a missing cancel handle.
#[cold]
#[inline(never)]
fn uninitialized(kind: &str) -> Error {
    Error::with_message_and_status(
        format!("{kind} is uninitialized or already released"),
        Status::InvalidState,
    )
}

#[cold]
#[inline(never)]
fn poisoned(kind: &str) -> Error {
    Error::with_message_and_status(
        format!("{kind} is unusable after an earlier panic in the driver"),
        Status::Internal,
    )
}

/// The object behind `handle`, or null for a null handle or an unpopulated slot.
///
/// # Safety
/// `handle` must be null or point to a valid ABI struct.
unsafe fn object<H: FfiHandle>(handle: *mut H) -> *const Exported<H::State> {
    if handle.is_null() {
        return std::ptr::null();
    }
    unsafe { *H::private_data(handle) }.cast_const().cast()
}

/// The `private_data` field of `handle`, rejecting a null pointer as a bad argument. A null field
/// is instead an object used out of order.
///
/// # Safety
/// `handle` must be null or point to a valid ABI struct that outlives `'a`.
unsafe fn slot<'a, H: FfiHandle>(handle: *mut H) -> Result<&'a mut *mut c_void> {
    if handle.is_null() {
        return Err(missing_argument(H::KIND));
    }
    Ok(unsafe { &mut *H::private_data(handle) })
}

/// The empty `private_data` field that a `New` call populates. A populated handle is refused, as
/// overwriting it would leak its object; ADBC requires a zero-initialized struct.
///
/// # Safety
/// `handle` must be null or point to a valid ABI struct that outlives `'a`.
pub(super) unsafe fn unpopulated<'a, H: FfiHandle>(handle: *mut H) -> Result<&'a mut *mut c_void> {
    let slot = unsafe { slot(handle) }?;
    if !slot.is_null() {
        return Err(invalid_state(format!(
            "{} already holds a driver object; release it before creating another, and pass a \
             zero-initialized struct to a first one",
            H::KIND
        )));
    }
    Ok(slot)
}

/// Lock the state behind `handle`, refusing a state poisoned by a panic, including one in another
/// handle's call that borrowed it. `AdbcConnectionInit` borrows its database this way and
/// `AdbcStatementNew` its connection, which must serialize with calls on the borrowed object.
///
/// # Safety
/// `handle` must be null or point to a valid ABI struct, and the guard must not outlive it.
pub(super) unsafe fn lock_state<'a, H: FfiHandle>(
    handle: *mut H,
) -> Result<MutexGuard<'a, H::State>> {
    let exported = unsafe { object(handle).as_ref() }.ok_or_else(|| uninitialized(H::KIND))?;
    exported.state.lock().map_err(|_| poisoned(H::KIND))
}

/// Run `f` against the state behind `handle`, containing panics and reporting errors through
/// `error`.
///
/// # Safety
/// `handle` must be null or point to a valid ABI struct.
pub(super) unsafe fn with_state<H, F>(handle: *mut H, error: *mut AdbcError, f: F) -> AdbcStatusCode
where
    H: FfiHandle,
    F: FnOnce(&mut H::State) -> Result<()>,
{
    let outcome = catch(|| {
        let mut state = unsafe { lock_state(handle) }?;
        // The guard must unwind before `catch` handles a panic so the mutex records poisoning.
        f(&mut state)
    });
    unsafe { finish(outcome, error) }
}

/// Every `*Cancel`. Avoids the state lock so a blocked call can be cancelled, even after a panic
/// poisoned the state. Before `Init` there is no cancel handle (`InvalidState`); with nothing in
/// flight it returns OK (see `SlotCancelHandle`).
///
/// # Safety
/// `handle` must be null or point to a valid ABI struct.
pub(super) unsafe extern "C" fn cancel<H: FfiHandle>(
    handle: *mut H,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    let outcome = catch(|| {
        let pointer = unsafe { object(handle) };
        if pointer.is_null() {
            return Err(uninitialized(H::KIND));
        }
        // Owned, so a `Release` after this point cannot free the object mid-cancel. The handle
        // keeps its own count; this adds one for the `Arc`.
        let exported = unsafe {
            Arc::increment_strong_count(pointer);
            Arc::from_raw(pointer)
        };
        match exported.cancel.get() {
            Some(cancel) => cancel.try_cancel(),
            None => Err(uninitialized(H::KIND)),
        }
    });
    unsafe { finish(outcome, error) }
}

/// Install the cancel handle of a newly initialized state, which happens at most once.
///
/// # Safety
/// `handle` must point to a valid ABI struct holding an object.
pub(super) unsafe fn install_cancel_handle<H: FfiHandle>(
    handle: *mut H,
    cancel: Box<dyn CancelHandle>,
) {
    let _ = unsafe { &*object(handle) }.cancel.set(cancel);
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
    let outcome = unsafe { unpopulated(handle) }
        .map(|slot| *slot = Exported::into_private_data(H::State::default()));
    unsafe { finish(outcome, error) }
}

/// Every `*Release`. Drops the handle's reference and nulls the slot, so a second release is an
/// error instead of a double free. An in-flight [`cancel`] can defer destruction until it returns.
///
/// # Safety
/// `handle` must be null or point to a valid ABI struct, and no other call on the object may be
/// in flight.
pub(super) unsafe extern "C" fn release_handle<H: FfiHandle>(
    handle: *mut H,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    let outcome = unsafe { slot(handle) }.and_then(|slot| {
        let taken = std::mem::replace(slot, std::ptr::null_mut());
        if taken.is_null() {
            return Err(uninitialized(H::KIND));
        }
        // Dropping driver state runs arbitrary code, such as closing clients.
        catch(|| {
            drop(unsafe { Arc::from_raw(taken.cast_const().cast::<Exported<H::State>>()) });
            Ok(())
        })
    });
    unsafe { finish(outcome, error) }
}

/// Convert a driver result into a status code, exporting the error when there is one.
///
/// # Safety
/// `error` must be null or point to an `AdbcError` that [`export_error`] accepts.
pub(crate) unsafe fn finish(result: Result<()>, error: *mut AdbcError) -> AdbcStatusCode {
    match result {
        Ok(()) => ADBC_STATUS_OK,
        Err(failure) => {
            let status = AdbcStatusCode::from(failure.status);
            unsafe { export_error(error, failure) };
            status
        }
    }
}

#[cfg(test)]
mod tests;

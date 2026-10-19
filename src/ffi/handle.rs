//! The object behind each handle's `private_data`, and how entry points reach it.
//!
//! Each handle owns an [`Arc<Exported<S>>`]. Calls serialize on its state mutex, and a panic
//! poisons every state the panicking call locked. [`cancel`] uses a separate lock so it can
//! interrupt a blocked call.
//!
//! Callers must not overlap `Release` with any other call, including `Cancel`: reading
//! `private_data` and taking a reference are not atomic. Once taken, the cancel reference keeps
//! the state alive until cancellation returns.

use std::ffi::c_void;
use std::mem::ManuallyDrop;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use adbc_core::CancelHandle;
use adbc_core::error::{Error, Result, Status};
use adbc_core::options::OptionValue;

use super::abi::{ADBC_STATUS_OK, AdbcError, AdbcStatusCode};
use super::error::export_error;
use super::guard::catch;

/// Options set before `Init`, ordered by each key's latest set.
///
/// Order matters because a URI expands into individual options: an endpoint set after the URI
/// must override it, even if the endpoint was also set before.
pub(crate) struct OptionBuffer<K> {
    /// Cloned, not taken, by `Init`, so a failed `Init` can be retried.
    pub(super) entries: Vec<(K, OptionValue)>,
}

impl<K: PartialEq> OptionBuffer<K> {
    pub(crate) fn set(&mut self, key: K, value: OptionValue) {
        if let Some(index) = self
            .entries
            .iter()
            .position(|(existing, _)| *existing == key)
        {
            self.entries.remove(index);
        }
        self.entries.push((key, value));
    }
}

/// A database or connection before or after `Init`. ADBC splits construction across
/// `New`/`SetOption`/`Init`, while the driver's constructors take all options at once.
pub(crate) enum Staged<K, S> {
    Pending(OptionBuffer<K>),
    Ready(S),
}

/// The state `New` installs.
impl<K: PartialEq, S> Default for Staged<K, S> {
    fn default() -> Self {
        Self::Pending(OptionBuffer {
            entries: Vec::new(),
        })
    }
}

impl<K: PartialEq, S> Staged<K, S> {
    /// The initialized state, or `InvalidState` before `Init`.
    pub(crate) fn ready(&mut self, kind: &str) -> Result<&mut S> {
        match self {
            Self::Ready(state) => Ok(state),
            Self::Pending(_) => Err(uninitialized(kind)),
        }
    }

    /// The buffered options, or `InvalidState` after `Init`.
    pub(crate) fn pending(&mut self, kind: &str) -> Result<&mut OptionBuffer<K>> {
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
    cancel: Mutex<Option<Box<dyn CancelHandle>>>,
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
        Self::into_private_data_with_cancel(state, None)
    }

    /// Store a cancel handle separately so [`cancel`] can run while the state is locked.
    /// Objects initialized later use [`install_cancel_handle`] instead.
    pub(crate) fn into_private_data_with_cancel(
        state: S,
        cancel: Option<Box<dyn CancelHandle>>,
    ) -> *mut c_void {
        Arc::into_raw(Arc::new(Self {
            cancel: Mutex::new(cancel),
            state: Mutex::new(state),
        }))
        .cast::<c_void>()
        .cast_mut()
    }
}

impl<S> Exported<S> {
    /// Take the state lock, refusing a state poisoned by a panic, including one in another
    /// handle's call that borrowed it.
    fn lock_state(&self, kind: &str) -> Result<MutexGuard<'_, S>> {
        self.state.lock().map_err(|_| poisoned(kind))
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

/// Borrow the object behind `slot` without taking a reference count.
///
/// # Safety
/// `slot` must be null or a pointer produced by [`Exported::<S>::into_private_data`] that has not
/// been released, and the reference must not outlive the object.
unsafe fn borrowed<'a, S>(slot: *mut c_void, kind: &str) -> Result<&'a Exported<S>> {
    unsafe { slot.cast::<Exported<S>>().as_ref() }.ok_or_else(|| uninitialized(kind))
}

/// Take an owned reference to the object behind `slot`, which keeps it alive through a later
/// `Release`.
///
/// # Safety
/// `slot` must be null or a pointer produced by [`Exported::<S>::into_private_data`] that has not
/// been released.
unsafe fn strong<S>(slot: *mut c_void, kind: &str) -> Result<Arc<Exported<S>>> {
    let pointer: *const Exported<S> = slot.cast_const().cast::<Exported<S>>();
    if pointer.is_null() {
        return Err(uninitialized(kind));
    }
    // The handle keeps its own reference; this reconstructs it only to clone from, so it must not
    // be dropped.
    let handle = ManuallyDrop::new(unsafe { Arc::from_raw(pointer) });
    Ok(Arc::clone(&handle))
}

/// Run `f` against the object behind `slot`, containing panics and reporting errors through
/// `error`.
///
/// # Safety
/// `slot` must be null or a pointer produced by [`Exported::<S>::into_private_data`] that has not
/// been released.
pub(crate) unsafe fn dispatch<S, F>(
    slot: *mut c_void,
    kind: &'static str,
    error: *mut AdbcError,
    f: F,
) -> AdbcStatusCode
where
    F: FnOnce(&mut S) -> Result<()>,
{
    let outcome = catch(|| {
        let exported = unsafe { borrowed::<S>(slot, kind) }?;
        let mut state = exported.lock_state(kind)?;
        // The guard must unwind before `catch` handles a panic so the mutex records poisoning.
        f(&mut state)
    });
    finish(outcome, error)
}

/// Cancel the object's in-flight operation without taking its state lock, so a blocked call can
/// be cancelled, even after a panic poisoned the state.
///
/// # Safety
/// `slot` must be null or a pointer produced by [`Exported::<S>::into_private_data`] that has not
/// been released.
pub(crate) unsafe fn cancel<S>(
    slot: *mut c_void,
    kind: &'static str,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    let outcome = catch(|| {
        // Owned, so a `Release` after this point cannot free the object mid-cancel.
        let exported = unsafe { strong::<S>(slot, kind) }?;
        let handle = exported
            .cancel
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        match handle.as_ref() {
            Some(handle) => handle.try_cancel(),
            None => Err(uninitialized(kind)),
        }
    });
    finish(outcome, error)
}

/// Install the cancel handle of a newly initialized state. Statements pass theirs to
/// [`Exported::into_private_data_with_cancel`] instead.
///
/// # Safety
/// `slot` must be null or a pointer produced by [`Exported::<S>::into_private_data`] that has not
/// been released.
pub(crate) unsafe fn install_cancel_handle<S>(slot: *mut c_void, handle: Box<dyn CancelHandle>) {
    if let Some(exported) = unsafe { slot.cast::<Exported<S>>().as_ref() } {
        *exported
            .cancel
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(handle);
    }
}

/// Lock the state behind `slot` without dispatching a call on it.
///
/// `AdbcConnectionInit` borrows its database and `AdbcStatementNew` its connection; both must
/// serialize with calls on the borrowed object and respect its poisoning.
///
/// # Safety
/// `slot` must be null or a pointer produced by [`Exported::<S>::into_private_data`] that has not
/// been released, and the guard must not outlive it.
pub(crate) unsafe fn lock_state<'a, S>(slot: *mut c_void, kind: &str) -> Result<MutexGuard<'a, S>> {
    unsafe { borrowed::<S>(slot, kind) }?.lock_state(kind)
}

/// Drop the handle's reference and null the slot, so a second release is an error instead of a
/// double free. An in-flight [`cancel`] can defer destruction until it returns.
///
/// # Safety
/// `slot` must point to a handle field holding null or a pointer produced by
/// [`Exported::<S>::into_private_data`]. No borrowed state or new acquisition may be in flight.
pub(crate) unsafe fn release<S>(
    slot: &mut *mut c_void,
    kind: &'static str,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    let taken = std::mem::replace(slot, std::ptr::null_mut());
    if taken.is_null() {
        return finish(Err(uninitialized(kind)), error);
    }
    // Dropping driver state runs arbitrary code, such as closing clients.
    let outcome = catch(|| {
        drop(unsafe { Arc::from_raw(taken.cast_const().cast::<Exported<S>>()) });
        Ok(())
    });
    finish(outcome, error)
}

/// Convert a driver result into a status code, exporting the error when there is one.
pub(crate) fn finish(result: Result<()>, error: *mut AdbcError) -> AdbcStatusCode {
    match result {
        Ok(()) => ADBC_STATUS_OK,
        Err(failure) => {
            let status = AdbcStatusCode::from(failure.status);
            // Exporting can run the caller's previous release callback. If that panics, the
            // status still reports the original failure.
            let contained = catch(|| {
                unsafe { export_error(error, failure) };
                Ok(())
            });
            drop(contained);
            status
        }
    }
}

#[cfg(test)]
mod tests;

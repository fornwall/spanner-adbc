//! How an exported object is represented behind `private_data`, and how entry points reach it.
//!
//! Each handle owns an [`Arc<Exported<S>>`]. Calls serialize through its state mutex, and a panic
//! poisons every state locked by that call. [`cancel`] uses a separate lock so it can interrupt a
//! blocked operation.
//!
//! Callers must exclude `Release` from all other calls, including `Cancel`: reading the caller's
//! `private_data` and acquiring an owned reference are not atomic. Once acquired, the cancel
//! reference keeps the state alive until cancellation completes.

use std::ffi::c_void;
use std::mem::ManuallyDrop;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use adbc_core::CancelHandle;
use adbc_core::error::{Error, Result, Status};
use adbc_core::options::OptionValue;

use super::abi::{ADBC_STATUS_OK, AdbcError, AdbcStatusCode};
use super::error::export_error;
use super::guard::catch;

/// Options accumulated before init, retaining each key's last value and position.
///
/// Position matters because a URI can also set individual options: an explicit endpoint set
/// after a URI must be applied after it, even if that endpoint was set earlier too.
pub(crate) struct OptionBuffer<K> {
    /// Cloned rather than taken by the fallible constructors, so a failed `Init` keeps the
    /// options and the handle can be initialized again.
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

/// An exported object is either still collecting options or fully initialized. ADBC splits
/// construction across `New`/`SetOption`/`Init`, and the driver's own constructors want all the
/// options at once, so the pre-init options are buffered until `Init`.
pub(crate) enum Staged<K, S> {
    Pending(OptionBuffer<K>),
    Ready(S),
}

/// A fresh handle is pending with an empty option buffer, which is what `New` installs.
impl<K: PartialEq, S> Default for Staged<K, S> {
    fn default() -> Self {
        Self::Pending(OptionBuffer {
            entries: Vec::new(),
        })
    }
}

impl<K: PartialEq, S> Staged<K, S> {
    /// The initialized state, or the uniform refusal for an object that has none.
    pub(crate) fn ready(&mut self, kind: &str) -> Result<&mut S> {
        match self {
            Self::Ready(state) => Ok(state),
            Self::Pending(_) => Err(uninitialized(kind)),
        }
    }

    /// The mirror of [`Self::ready`]: the options buffered before `Init`, for the entry point
    /// that performs it.
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

/// The contents of an exported handle's `private_data`.
///
/// The raw pointer the caller holds is shared freely between its threads, so everything here is
/// reached through `&Exported<S>` and interior mutability; forming a `&mut` to the whole struct
/// would alias when two calls overlap, which adbc.h explicitly permits for `Cancel` and the
/// option getters.
pub(crate) struct Exported<S> {
    /// The object's cancel handle, populated once the state is initialized. Kept beside — not
    /// inside — the state so [`cancel`] can reach the driver's thread-safe cancellation machinery
    /// while another call holds the state lock.
    cancel: Mutex<Option<Box<dyn CancelHandle>>>,
    /// The driver object itself. Every call that touches it takes this lock, so overlapping calls
    /// serialize instead of racing; [`cancel`] bypasses it.
    state: Mutex<S>,
}

// Raw FFI pointers bypass these trait checks at call sites. Keep a compile-time assertion that
// adding a field cannot make sharing a handle between caller threads unsound.
fn _exported_is_shareable<S: Send>(exported: &Exported<S>) -> &(dyn Send + Sync) {
    exported
}

// `S: Send` because the caller may create the object on one thread and use it from another; the
// mutex already prevents concurrent access, so `Sync` is not required of the state itself.
impl<S: Send> Exported<S> {
    pub(crate) fn into_private_data(state: S) -> *mut c_void {
        Self::into_private_data_with_cancel(state, None)
    }

    /// As [`Self::into_private_data`], also storing the handle the one lock-free entry point --
    /// [`cancel`] -- reaches without the state. Objects whose state is only initialized later
    /// install it via [`install_cancel_handle`] instead.
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
    /// Take the state lock, refusing a poisoned object.
    ///
    /// This also rejects a state borrowed by another handle's call when that call panicked.
    fn lock_state(&self, kind: &str) -> Result<MutexGuard<'_, S>> {
        self.state.lock().map_err(|_| poisoned(kind))
    }
}

/// A null slot, pending state or missing cancel handle is not usable yet, or has been released.
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

/// Borrow the object behind `slot` without taking a reference of one's own.
///
/// This is what every entry point but [`cancel`] uses: adbc.h leaves a `Release` that overlaps
/// another call on the same object undefined, so borrowing the handle's own reference is exactly
/// as strong a claim as those entry points are entitled to make.
///
/// # Safety
/// `slot` must be null or a pointer produced by [`Exported::<S>::into_private_data`] that has not
/// been released, and the reference must not outlive the object.
unsafe fn borrowed<'a, S>(slot: *mut c_void, kind: &str) -> Result<&'a Exported<S>> {
    unsafe { slot.cast::<Exported<S>>().as_ref() }.ok_or_else(|| uninitialized(kind))
}

/// Take a reference of one's own to the object behind `slot`, keeping it alive independently of
/// the handle.
///
/// Used by [`cancel`] to retain the state for the duration of cancellation.
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

/// Cancel the object's in-flight operation without taking its dispatch turn.
///
/// Uses the separate cancel lock, including after the state has been poisoned, so a blocked or
/// panicked operation can still be cancelled.
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
        // A reference of this call's own, not the handle's: see [`strong`].
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

/// Install cancellation before publishing an initialized state. Statements supply the cancel
/// handle at construction through [`Exported::into_private_data_with_cancel`] instead.
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
/// This exists for the entry points that need a second object beyond the one they dispatch on —
/// `AdbcConnectionInit` reads the database while dispatching on the connection, and
/// `AdbcStatementNew` has no statement yet when it reaches into the connection. The lock makes
/// that access serialize with the other object's own calls.
///
/// # Safety
/// `slot` must be null or a pointer produced by [`Exported::<S>::into_private_data`] that has not
/// been released, and the guard must not outlive it.
pub(crate) unsafe fn lock_state<'a, S>(slot: *mut c_void, kind: &str) -> Result<MutexGuard<'a, S>> {
    unsafe { borrowed::<S>(slot, kind) }?.lock_state(kind)
}

/// Drop the handle's reference to the object and null the slot out, so a second release is an
/// error rather than a double free.
///
/// An already acquired cancel reference can defer destruction until cancellation finishes.
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
    // Dropping driver state can run arbitrary code (closing clients, joining tasks), so it is
    // contained like any other call.
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
            // Exporting allocates and may run a stale release callback the caller left in the
            // struct, so it is contained like driver code. On a caught panic the caller still
            // gets the status code, with the error struct left as it was.
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

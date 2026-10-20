//! Bridges synchronous ADBC calls to the async Spanner client. Handles share the driver's
//! runtime through an [`Arc`], keeping client background tasks alive until the last handle drops.

use std::future::Future;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use adbc_core::CancelHandle;
use adbc_core::error::{Error, Result, Status};
use tokio::runtime::{Handle, Runtime, RuntimeFlavor};
use tokio::sync::{Notify, mpsc};
use tokio::task::JoinHandle;

use crate::error::{err, invalid_state};

pub(crate) type SharedRuntime = Arc<DriverRuntime>;

/// The driver's Tokio runtime.
///
/// Tokio panics when a runtime is dropped inside another runtime's context, such as the last
/// handle dropped by a Rust caller's async task, so it then shuts down in the background.
#[derive(Debug)]
pub(crate) struct DriverRuntime(Option<Runtime>);

impl Deref for DriverRuntime {
    type Target = Runtime;

    fn deref(&self) -> &Runtime {
        self.0
            .as_ref()
            .expect("the runtime is only taken when dropped")
    }
}

impl Drop for DriverRuntime {
    fn drop(&mut self) {
        if let Some(runtime) = self.0.take()
            && Handle::try_current().is_ok()
        {
            runtime.shutdown_background();
        }
    }
}

/// A latched cancellation signal shared by an operation and its streamed readers.
///
/// Cancellation stays set between fetches. [`CancelSlot::begin_operation`] replaces the signal
/// for new work without reviving older cancelled readers or cancelling the new operation.
#[derive(Clone)]
pub(crate) struct CancelSignal(Arc<CancelInner>);

impl std::fmt::Debug for CancelSignal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CancelSignal")
            .field("cancelled", &self.0.cancelled.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

struct CancelInner {
    /// Set by `signal()` and never cleared.
    cancelled: AtomicBool,
    /// Wakes tasks waiting in [`CancelSignal::cancelled`].
    notify: Notify,
}

impl CancelSignal {
    pub(crate) fn new() -> Self {
        Self(Arc::new(CancelInner {
            cancelled: AtomicBool::new(false),
            notify: Notify::new(),
        }))
    }

    /// Latch cancellation and wake current waiters.
    pub(crate) fn signal(&self) {
        // Set the flag before waking so woken waiters observe it.
        self.0.cancelled.store(true, Ordering::Release);
        self.0.notify.notify_waiters();
    }

    /// Wait until this signal is cancelled. Completes immediately if it already is.
    pub(crate) async fn cancelled(&self) {
        loop {
            if self.0.cancelled.load(Ordering::Acquire) {
                return;
            }
            let notified = self.0.notify.notified();
            tokio::pin!(notified);
            // Re-check after registering: `notify_waiters` only wakes registered waiters, so a
            // `signal()` since the first check would otherwise be missed.
            notified.as_mut().enable();
            if self.0.cancelled.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }
}

/// The current operation's cancellation signal, shared with the object's cancel handle.
#[derive(Debug)]
pub(crate) struct CancelSlot(std::sync::Mutex<CancelSignal>);

impl CancelSlot {
    pub(crate) fn new() -> Self {
        Self(std::sync::Mutex::new(CancelSignal::new()))
    }

    /// Replace the current signal without changing signals held by earlier readers.
    pub(crate) fn begin_operation(&self) -> CancelSignal {
        let fresh = CancelSignal::new();
        *self.0.lock().unwrap() = fresh.clone();
        fresh
    }

    /// Clone the current signal for a wait or streamed reader.
    pub(crate) fn current(&self) -> CancelSignal {
        self.0.lock().unwrap().clone()
    }

    /// Cancel the current operation.
    pub(crate) fn signal(&self) {
        self.0.lock().unwrap().signal();
    }
}

/// A long-lived ADBC cancel handle that always targets the current operation.
///
/// Cancelling while idle succeeds, although adbc.h asks for `InvalidState`: operations live on in
/// their readers, and materialized metadata readers carry no signal, so idle cannot be detected.
/// The C++ `MetadataGetObjectsCancel` test also expects success. The next operation replaces the
/// signal, so it does not inherit an idle cancel.
#[derive(Debug)]
pub(crate) struct SlotCancelHandle(Arc<CancelSlot>);

impl SlotCancelHandle {
    pub(crate) fn new(slot: Arc<CancelSlot>) -> Self {
        Self(slot)
    }
}

impl CancelHandle for SlotCancelHandle {
    fn try_cancel(&self) -> Result<()> {
        self.0.signal();
        Ok(())
    }
}

fn cancelled_err() -> Error {
    err(
        "operation cancelled by the ADBC cancel handle for this connection or statement",
        Status::Cancelled,
    )
}

/// Run `future` on the shared runtime, giving latched cancellation priority over completion.
pub(crate) fn block_on_cancellable<T>(
    runtime: &Runtime,
    cancel: &CancelSignal,
    future: impl Future<Output = Result<T>>,
) -> Result<T> {
    // Client/retry/conversion futures can exceed a host thread's 2 MiB stack in debug builds.
    // Boxing keeps the synchronous bridge's frame small.
    let future = Box::pin(future);
    block_on(runtime, async move {
        tokio::select! {
            // Check/register the cancellation waiter before polling the operation.
            biased;
            _ = cancel.cancelled() => Err(cancelled_err()),
            result = future => result,
        }
    })
}

/// Run `future` on `runtime` from synchronous code.
///
/// Tokio panics when this happens inside another runtime's context, as it does for a Rust caller
/// in an async task. A multi-thread caller runtime can hand off the blocked thread's work, but a
/// current-thread one cannot.
pub(crate) fn block_on<T>(runtime: &Runtime, future: impl Future<Output = Result<T>>) -> Result<T> {
    let Ok(caller) = Handle::try_current() else {
        return runtime.block_on(future);
    };
    if caller.runtime_flavor() == RuntimeFlavor::CurrentThread {
        return Err(invalid_state(
            "the synchronous ADBC API blocks, so it cannot run on a current-thread Tokio runtime \
             such as the `#[tokio::test]` default; call it through `tokio::task::spawn_blocking` \
             or from a multi-thread runtime",
        ));
    }
    tokio::task::block_in_place(|| runtime.block_on(future))
}

/// A source of row chunks for [`spawn_prefetch`]. An empty chunk ends the stream.
pub(crate) trait ChunkSource: Send + 'static {
    type Row: Send + 'static;

    fn next_chunk(&mut self) -> impl Future<Output = Result<Vec<Self::Row>>> + Send;
}

/// Receives prefetched chunks, or the error that ended the stream.
pub(crate) type ChunkReceiver<T> = mpsc::Receiver<Result<Vec<T>>>;

/// Prefetch chunks while the consumer processes earlier rows. The channel holds one chunk;
/// the task can hold one more while waiting to send it.
///
/// Fetch errors are sent before the task exits. Cancellation stops both fetches and sends;
/// consumers must check the same signal so buffered rows cannot mask cancellation. After the
/// channel closes, check the returned task for panic before reporting EOF. Abort it when the
/// reader drops to stop an in-flight fetch.
pub(crate) fn spawn_prefetch<S: ChunkSource>(
    runtime: &Runtime,
    cancel: CancelSignal,
    source: S,
) -> (ChunkReceiver<S::Row>, JoinHandle<()>) {
    let (tx, rx) = mpsc::channel(1);
    let task = runtime.spawn(prefetch_loop(source, tx, cancel));
    (rx, task)
}

async fn prefetch_loop<S: ChunkSource>(
    mut source: S,
    tx: mpsc::Sender<Result<Vec<S::Row>>>,
    cancel: CancelSignal,
) {
    loop {
        let chunk = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                // Best effort: with a full channel, the consumer's cancel-aware `recv` sees the
                // latched signal before the buffered chunk.
                let _ = tx.try_send(Err(cancelled_err()));
                return;
            }
            pulled = source.next_chunk() => pulled,
        };
        if chunk.as_ref().is_ok_and(Vec::is_empty) {
            return;
        }
        let failed = chunk.is_err();
        // Also watch the signal while parked on a full channel, so a cancel is not stalled behind
        // a consumer that has stopped draining.
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return,
            sent = tx.send(chunk) => {
                if sent.is_err() || failed {
                    return;
                }
            }
        }
    }
}

/// Create a new multi-thread runtime for the driver.
pub(crate) fn new_runtime() -> Result<SharedRuntime> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("spanner-adbc")
        .build()
        .map_err(|e| {
            err(
                format!("failed to build Tokio runtime: {e}"),
                Status::Internal,
            )
        })?;
    Ok(Arc::new(DriverRuntime(Some(runtime))))
}

#[cfg(test)]
mod tests;

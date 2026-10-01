//! Bridges synchronous ADBC calls to the async Spanner client. Handles share the driver's
//! runtime through an [`Arc`], keeping client background tasks alive until the last handle drops.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use adbc_core::CancelHandle;
use adbc_core::error::{Error, Result, Status};
use tokio::runtime::Runtime;
use tokio::sync::{Notify, mpsc};
use tokio::task::JoinHandle;

use crate::error::err;

/// A reference-counted handle to the driver's Tokio runtime.
pub(crate) type SharedRuntime = Arc<Runtime>;

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
    /// Latched cancellation state; `true` from `signal()` on, forever (never cleared).
    cancelled: AtomicBool,
    /// Wakes an operation currently parked in [`block_on_cancellable`].
    notify: Notify,
}

impl CancelSignal {
    pub(crate) fn new() -> Self {
        Self(Arc::new(CancelInner {
            cancelled: AtomicBool::new(false),
            notify: Notify::new(),
        }))
    }

    /// Latch cancellation and wake all waiters, including future fetches on this signal.
    pub(crate) fn signal(&self) {
        // Order matters: latch the flag before waking, so a woken waiter always observes it.
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
            // Register the waiter, then re-check the flag: a `signal()` that lands between the
            // check above and this registration would otherwise be missed (`notify_waiters` only
            // wakes already-registered waiters).
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

    /// Forward a cancel to the current operation's signal, latching it forever.
    pub(crate) fn signal(&self) {
        self.0.lock().unwrap().signal();
    }
}

/// A long-lived ADBC cancel handle that always targets the current operation.
///
/// Idle cancellation succeeds, deviating from adbc.h's `InvalidState` requirement. Operation
/// lifetimes extend through their readers, and materialized metadata readers carry no signal to
/// track. Accepting cancellation also satisfies the C++ `MetadataGetObjectsCancel` test.
/// The next operation replaces the idle signal, so it cannot inherit that cancellation.
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

/// Shared diagnostic for synchronous waits and background fetches.
fn cancelled_err() -> Error {
    err(
        "operation cancelled: the ADBC cancel handle for this connection or statement was \
         signalled; execute the operation again to retry it",
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
    runtime.block_on(async move {
        tokio::select! {
            // Check/register the cancellation waiter before polling the operation.
            biased;
            _ = cancel.cancelled() => Err(cancelled_err()),
            result = future => result,
        }
    })
}

/// A pull-based source of row chunks that a background prefetch task ([`spawn_prefetch`]) can own
/// and drain. An **empty** chunk means the source is exhausted.
pub(crate) trait ChunkSource: Send + 'static {
    /// The row type carried by each chunk.
    type Row: Send + 'static;

    /// Pull the next chunk of rows; an empty chunk signals the end of the source.
    fn next_chunk(&mut self) -> impl Future<Output = Result<Vec<Self::Row>>> + Send;
}

/// The receiving end of a [`spawn_prefetch`] channel: each item is a prefetched chunk of rows, or
/// the error that ended the stream.
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

/// The body of the [`spawn_prefetch`] task. See there for the termination conditions.
async fn prefetch_loop<S: ChunkSource>(
    mut source: S,
    tx: mpsc::Sender<Result<Vec<S::Row>>>,
    cancel: CancelSignal,
) {
    loop {
        let chunk = tokio::select! {
            // Check the (sticky) signal before polling the fetch, mirroring `block_on_cancellable`.
            biased;
            _ = cancel.cancelled() => {
                // Cancelled mid-fetch: drop the in-flight pull and surface the cancellation.
                // Best-effort — if the channel is full the consumer hits the latched signal itself
                // on its next cancel-aware `recv`, before it would ever see the buffered chunk.
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
    Ok(Arc::new(runtime))
}

#[cfg(test)]
mod tests;

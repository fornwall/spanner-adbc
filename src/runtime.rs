//! Bridges synchronous ADBC calls to the async Spanner client. Handles share the driver's
//! runtime through an [`Arc`], keeping client background tasks alive until the last handle drops.

use std::future::Future;
use std::ops::Deref;
use std::sync::Arc;

use adbc_core::CancelHandle;
use adbc_core::error::{Error, Result, Status};
use tokio::runtime::{Handle, Runtime, RuntimeFlavor};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use crate::error::{err, invalid_state};

pub(crate) type SharedRuntime = Arc<DriverRuntime>;

/// The driver's Tokio runtime.
///
/// A forked child inherits the runtime but none of its threads, so nothing would ever drive its
/// tasks or timers there; [`block_on`] refuses it instead of hanging.
#[derive(Debug)]
pub(crate) struct DriverRuntime {
    /// Taken only when dropped.
    runtime: Option<Runtime>,
    /// The process that started the runtime's threads.
    pid: u32,
}

impl DriverRuntime {
    /// Whether this process is a fork of the one that started the runtime.
    pub(crate) fn forked(&self) -> bool {
        std::process::id() != self.pid
    }
}

impl Deref for DriverRuntime {
    type Target = Runtime;

    fn deref(&self) -> &Runtime {
        self.runtime
            .as_ref()
            .expect("the runtime is only taken when dropped")
    }
}

impl Drop for DriverRuntime {
    fn drop(&mut self) {
        let Some(runtime) = self.runtime.take() else {
            return;
        };
        if self.forked() {
            // Shutting down would wait for threads that only exist in the parent.
            std::mem::forget(runtime);
        } else if Handle::try_current().is_ok() {
            // Tokio panics when a runtime is dropped inside another runtime's context, such as
            // the last handle dropped by a Rust caller's async task.
            runtime.shutdown_background();
        }
    }
}

/// A latched cancellation signal shared by an operation and its streamed readers.
///
/// Cancellation stays set between fetches. [`CancelSlot::begin_operation`] replaces the signal
/// for new work without reviving older cancelled readers or cancelling the new operation.
#[derive(Clone, Debug)]
pub(crate) struct CancelSignal(Arc<watch::Sender<bool>>);

impl CancelSignal {
    pub(crate) fn new() -> Self {
        Self(Arc::new(watch::Sender::new(false)))
    }

    /// Latch cancellation and wake current waiters.
    pub(crate) fn signal(&self) {
        self.0.send_replace(true);
    }

    /// Wait until this signal is cancelled. Completes immediately if it already is.
    pub(crate) async fn cancelled(&self) {
        // Holding the sender, `self` keeps the channel open.
        let _ = self.0.subscribe().wait_for(|&cancelled| cancelled).await;
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
pub(crate) struct SlotCancelHandle(pub(crate) Arc<CancelSlot>);

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
    runtime: &DriverRuntime,
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
/// A task on a multi-thread caller runtime hands its worker's other tasks off first. On a
/// current-thread runtime's own thread this panics, as other blocking clients such as
/// `reqwest::blocking` do, while its `spawn_blocking` threads can block.
pub(crate) fn block_on<T>(
    runtime: &DriverRuntime,
    future: impl Future<Output = Result<T>>,
) -> Result<T> {
    if runtime.forked() {
        return Err(invalid_state(
            "this handle was created before the process forked, and the driver's threads only \
             exist in the parent; create new databases and connections in the child",
        ));
    }
    match Handle::try_current() {
        Ok(caller) if caller.runtime_flavor() != RuntimeFlavor::CurrentThread => {
            tokio::task::block_in_place(|| runtime.block_on(future))
        }
        _ => runtime.block_on(future),
    }
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
    let pump = async {
        loop {
            let chunk = source.next_chunk().await;
            if chunk.as_ref().is_ok_and(Vec::is_empty) {
                return;
            }
            let failed = chunk.is_err();
            if tx.send(chunk).await.is_err() || failed {
                return;
            }
        }
    };
    tokio::select! {
        biased;
        _ = cancel.cancelled() => {
            // Closing the channel silently would read as the end of the stream to a consumer that
            // checked the signal just before it was set. Best effort: with a full channel, that
            // consumer gets the buffered chunk and sees the latched signal on its next call.
            let _ = tx.try_send(Err(cancelled_err()));
        }
        () = pump => {}
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
    Ok(Arc::new(DriverRuntime {
        runtime: Some(runtime),
        pid: std::process::id(),
    }))
}

#[cfg(test)]
pub(crate) mod tests;

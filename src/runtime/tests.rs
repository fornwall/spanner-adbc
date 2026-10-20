use super::*;
use std::time::Duration;

/// A Rust caller may use the synchronous API, and drop the last handle, from an async task.
#[test]
fn works_and_drops_inside_a_multi_thread_runtime() {
    let runtime = new_runtime().unwrap();
    let outer = tokio::runtime::Builder::new_multi_thread().build().unwrap();
    let cancel = CancelSignal::new();
    let in_task = runtime.clone();
    outer
        .block_on(outer.spawn(async move {
            let result: Result<i32> =
                block_on_cancellable(&in_task, &CancelSignal::new(), async { Ok(1) });
            assert_eq!(result.unwrap(), 1);
        }))
        .unwrap();
    outer.block_on(async move {
        let result: Result<i32> = block_on_cancellable(&runtime, &cancel, async { Ok(2) });
        assert_eq!(result.unwrap(), 2);
        drop(runtime);
    });
}

#[test]
fn a_current_thread_caller_runtime_gets_an_error_instead_of_a_panic() {
    let runtime = new_runtime().unwrap();
    let outer = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    outer.block_on(async move {
        let result: Result<i32> =
            block_on_cancellable(&runtime, &CancelSignal::new(), async { Ok(1) });
        let error = result.unwrap_err();
        assert_eq!(error.status, Status::InvalidState);
        assert!(
            error.message.contains("spawn_blocking"),
            "{}",
            error.message
        );
        drop(runtime);
    });
}

#[test]
fn signal_from_another_thread_cancels_the_operation() {
    let runtime = new_runtime().unwrap();
    let cancel = CancelSignal::new();
    let signaller = cancel.clone();
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(100));
        signaller.signal();
    });
    let result: Result<()> = block_on_cancellable(&runtime, &cancel, async {
        tokio::time::sleep(Duration::from_secs(30)).await;
        Ok(())
    });
    assert_eq!(result.unwrap_err().status, Status::Cancelled);
}

// A cancel between two fetches of a stream still cancels the next fetch.
#[test]
fn signal_between_operations_cancels_the_next_one() {
    let runtime = new_runtime().unwrap();
    let cancel = CancelSignal::new();
    let first: Result<i32> = block_on_cancellable(&runtime, &cancel, async { Ok(1) });
    assert_eq!(first.unwrap(), 1);
    cancel.signal(); // nothing in flight
    let second: Result<i32> = block_on_cancellable(&runtime, &cancel, async { Ok(2) });
    assert_eq!(second.unwrap_err().status, Status::Cancelled);
    let third: Result<i32> = block_on_cancellable(&runtime, &cancel, async { Ok(3) });
    assert_eq!(third.unwrap_err().status, Status::Cancelled);
}

#[test]
fn begin_operation_shields_a_new_operation_from_a_stale_cancel() {
    let runtime = new_runtime().unwrap();
    let slot = CancelSlot::new();
    slot.signal();
    let cancel = slot.begin_operation();
    let result: Result<i32> = block_on_cancellable(&runtime, &cancel, async { Ok(7) });
    assert_eq!(result.unwrap(), 7);
}

// A streamed reader holding the old signal keeps failing.
#[test]
fn begin_operation_does_not_uncancel_an_earlier_operations_signal() {
    let runtime = new_runtime().unwrap();
    let slot = CancelSlot::new();
    let old = slot.begin_operation();
    slot.signal();
    let fresh = slot.begin_operation();
    let revived: Result<i32> = block_on_cancellable(&runtime, &old, async { Ok(1) });
    assert_eq!(revived.unwrap_err().status, Status::Cancelled);
    let new_op: Result<i32> = block_on_cancellable(&runtime, &fresh, async { Ok(2) });
    assert_eq!(new_op.unwrap(), 2);
    slot.signal();
    let cancelled: Result<i32> = block_on_cancellable(&runtime, &slot.current(), async { Ok(3) });
    assert_eq!(cancelled.unwrap_err().status, Status::Cancelled);
}

// See `SlotCancelHandle` for why idle cancellation succeeds.
#[test]
fn cancel_handle_reports_ok_when_nothing_is_in_flight() {
    let runtime = new_runtime().unwrap();
    let slot = Arc::new(CancelSlot::new());
    let handle = SlotCancelHandle::new(slot.clone());
    assert!(
        handle.try_cancel().is_ok(),
        "an idle cancel still reports Ok"
    );
    let cancel = slot.begin_operation();
    let result: Result<i32> = block_on_cancellable(&runtime, &cancel, async { Ok(5) });
    assert_eq!(
        result.unwrap(),
        5,
        "the idle cancel did not arm the next operation"
    );
}

/// One step of a [`ScriptedSource`]: a ready chunk (or error), or a fetch that never completes.
enum Step {
    Chunk(Result<Vec<i32>>),
    NeverCompletes,
}

/// Replays a script of chunks, counting fetches. Past the end it returns empty chunks.
struct ScriptedSource {
    steps: std::collections::VecDeque<Step>,
    calls: Arc<std::sync::atomic::AtomicUsize>,
}

impl ScriptedSource {
    fn new(steps: Vec<Step>) -> (Self, Arc<std::sync::atomic::AtomicUsize>) {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let source = Self {
            steps: steps.into(),
            calls: calls.clone(),
        };
        (source, calls)
    }
}

impl ChunkSource for ScriptedSource {
    type Row = i32;

    fn next_chunk(&mut self) -> impl Future<Output = Result<Vec<i32>>> + Send {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let step = self.steps.pop_front();
        async move {
            match step {
                Some(Step::Chunk(chunk)) => chunk,
                Some(Step::NeverCompletes) => std::future::pending().await,
                None => Ok(Vec::new()),
            }
        }
    }
}

/// Wait, bounded, until the background task has made `at_least` fetches.
fn wait_for_calls(calls: &std::sync::atomic::AtomicUsize, at_least: usize) {
    for _ in 0..1000 {
        if calls.load(Ordering::SeqCst) >= at_least {
            return;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    panic!(
        "background task never reached {at_least} fetches (got {})",
        calls.load(Ordering::SeqCst)
    );
}

#[test]
fn prefetch_delivers_chunks_in_order_and_runs_ahead() {
    let runtime = new_runtime().unwrap();
    let (source, calls) = ScriptedSource::new(vec![
        Step::Chunk(Ok(vec![1])),
        Step::Chunk(Ok(vec![2, 3])),
        Step::Chunk(Ok(vec![4])),
    ]);
    let (mut rx, task) = spawn_prefetch(&runtime, CancelSignal::new(), source);
    // Chunk 1 is buffered and chunk 2 is being fetched before anything is consumed.
    wait_for_calls(&calls, 2);
    assert_eq!(rx.blocking_recv().unwrap().unwrap(), vec![1]);
    assert_eq!(rx.blocking_recv().unwrap().unwrap(), vec![2, 3]);
    assert_eq!(rx.blocking_recv().unwrap().unwrap(), vec![4]);
    assert!(
        rx.blocking_recv().is_none(),
        "drained source closes the channel"
    );
    runtime.block_on(task).unwrap();
}

#[test]
fn prefetch_surfaces_a_fetch_error_then_stops() {
    let runtime = new_runtime().unwrap();
    let (source, _calls) = ScriptedSource::new(vec![
        Step::Chunk(Ok(vec![1])),
        Step::Chunk(Err(err("stream broke", Status::IO))),
    ]);
    let (mut rx, task) = spawn_prefetch(&runtime, CancelSignal::new(), source);
    assert_eq!(rx.blocking_recv().unwrap().unwrap(), vec![1]);
    let error = rx.blocking_recv().unwrap().unwrap_err();
    assert_eq!(error.status, Status::IO);
    assert!(
        rx.blocking_recv().is_none(),
        "an errored source closes the channel"
    );
    runtime.block_on(task).unwrap();
}

#[test]
fn prefetch_cancel_aborts_an_in_flight_fetch() {
    let runtime = new_runtime().unwrap();
    let (source, calls) = ScriptedSource::new(vec![Step::NeverCompletes]);
    let cancel = CancelSignal::new();
    let (mut rx, task) = spawn_prefetch(&runtime, cancel.clone(), source);
    wait_for_calls(&calls, 1);
    cancel.signal();
    let error = rx.blocking_recv().unwrap().unwrap_err();
    assert_eq!(error.status, Status::Cancelled);
    assert!(rx.blocking_recv().is_none());
    runtime.block_on(task).unwrap();
}

#[test]
fn prefetch_cancel_unblocks_a_fetch_error_waiting_on_a_full_channel() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (source, calls) = ScriptedSource::new(vec![
        Step::Chunk(Ok(vec![1])),
        Step::Chunk(Err(err("stream broke", Status::IO))),
    ]);
    let cancel = CancelSignal::new();
    let (_rx, task) = spawn_prefetch(&runtime, cancel.clone(), source);
    runtime.block_on(async {
        // On one runtime thread the task reaches its blocked send before yielding back here.
        while calls.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
        cancel.signal();
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("cancel must release a task waiting to send an error")
            .unwrap();
    });
}

#[test]
fn prefetch_stops_when_the_receiver_is_dropped() {
    let runtime = new_runtime().unwrap();
    let (source, calls) = ScriptedSource::new(vec![
        Step::Chunk(Ok(vec![1])),
        Step::Chunk(Ok(vec![2])),
        Step::Chunk(Ok(vec![3])),
    ]);
    let (rx, task) = spawn_prefetch(&runtime, CancelSignal::new(), source);
    drop(rx);
    runtime.block_on(task).unwrap();
    // At most the chunk whose send failed plus the one in flight.
    assert!(calls.load(Ordering::SeqCst) <= 2);
}

use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};

use super::*;
use crate::ffi::test_support::null;
use adbc_core::options::OptionDatabase;

/// A handle over a plain counter, so these tests need no driver state.
struct Counter {
    private_data: *mut c_void,
}

impl FfiHandle for Counter {
    type State = u32;
    const KIND: &'static str = "statement";

    fn private_data(handle: *mut Self) -> *mut *mut c_void {
        unsafe { &raw mut (*handle).private_data }
    }
}

impl Counter {
    fn new() -> Self {
        Self {
            private_data: Exported::into_private_data(0_u32),
        }
    }

    fn with_cancel(cancel: impl CancelHandle + 'static) -> Self {
        let mut handle = Self::new();
        unsafe { install_cancel_handle(&raw mut handle, Box::new(cancel)) };
        handle
    }
}

impl Drop for Counter {
    fn drop(&mut self) {
        unsafe { release_handle(self, null()) };
    }
}

/// A cancel handle that records whether it fired.
struct RecordingCancel(Arc<AtomicBool>);

impl CancelHandle for RecordingCancel {
    fn try_cancel(&self) -> Result<()> {
        self.0.store(true, Ordering::SeqCst);
        Ok(())
    }
}

#[test]
fn option_buffer_keeps_order_and_collapses_repeats() {
    let mut buffer = OptionBuffer {
        entries: Vec::new(),
    };
    buffer.set(OptionDatabase::Uri, "first".into());
    buffer.set(OptionDatabase::Other("x".into()), "y".into());
    buffer.set(OptionDatabase::Uri, "second".into());

    let entries = buffer.entries.clone();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].0, OptionDatabase::Other("x".into()));
    assert_eq!(entries[1].0, OptionDatabase::Uri);
    assert!(
        matches!(&entries[1].1, OptionValue::String(value) if value == "second"),
        "the later write must win"
    );
}

#[test]
fn a_pending_object_is_not_ready() {
    let mut staged: Staged<OptionDatabase, u8> = Staged::default();
    let error = staged.ready("database").unwrap_err();
    assert_eq!(error.status, Status::InvalidState);

    staged = Staged::Ready(3);
    assert_eq!(*staged.ready("database").unwrap(), 3);
}

#[test]
fn a_panicking_call_poisons_only_that_object() {
    let mut first = Counter::new();
    let mut second = Counter::new();

    let status = unsafe { with_state(&raw mut first, null(), |_| panic!("inside the driver")) };
    assert_eq!(status, AdbcStatusCode::from(Status::Internal));

    let status = unsafe {
        with_state(&raw mut first, null(), |state| {
            *state = 1;
            Ok(())
        })
    };
    assert_eq!(status, AdbcStatusCode::from(Status::Internal));

    // Another object is unaffected.
    let status = unsafe {
        with_state(&raw mut second, null(), |state| {
            *state = 1;
            Ok(())
        })
    };
    assert_eq!(status, ADBC_STATUS_OK);
}

#[test]
fn a_panicking_call_also_poisons_borrowed_handles() {
    let mut connection = Counter::new();
    let mut database = Counter::new();
    let borrowed = &raw mut database;

    let status = unsafe {
        with_state(&raw mut connection, null(), |state| {
            let mut borrowed = lock_state(borrowed)?;
            *state = 1;
            *borrowed = 1;
            panic!("panic while initializing from a borrowed database");
        })
    };
    assert_eq!(status, AdbcStatusCode::from(Status::Internal));

    for (handle, kind) in [
        (&raw mut connection, "connection"),
        (&raw mut database, "database"),
    ] {
        let mut called = false;
        let status = unsafe {
            with_state(handle, null(), |_| {
                called = true;
                Ok(())
            })
        };
        assert_eq!(status, AdbcStatusCode::from(Status::Internal), "{kind}");
        assert!(!called, "must not reuse the poisoned {kind}");
        assert_eq!(
            unsafe { lock_state(handle) }.unwrap_err().status,
            Status::Internal,
        );
        assert_eq!(unsafe { release_handle(handle, null()) }, ADBC_STATUS_OK);
        assert!(unsafe { *Counter::private_data(handle) }.is_null());
    }
}

/// A cancel handle that blocks in `try_cancel` until the test resumes it.
struct BlockingCancel {
    entered: Sender<()>,
    resume: StdMutex<Receiver<()>>,
}

impl CancelHandle for BlockingCancel {
    fn try_cancel(&self) -> Result<()> {
        self.entered.send(()).expect("test channel");
        self.resume
            .lock()
            .expect("test mutex")
            .recv()
            .expect("test channel");
        Ok(())
    }
}

/// Once cancellation holds an owned reference, `Release` must not free the object under it.
#[test]
fn a_release_during_a_cancel_does_not_free_the_object_under_it() {
    let (entered_sender, entered_receiver) = channel::<()>();
    let (resume_sender, resume_receiver) = channel::<()>();
    let mut handle = Counter::with_cancel(BlockingCancel {
        entered: entered_sender,
        resume: StdMutex::new(resume_receiver),
    });

    let address = (&raw mut handle) as usize;
    let canceller = std::thread::spawn(move || unsafe { cancel(address as *mut Counter, null()) });
    // The cancel is now inside `try_cancel`.
    entered_receiver.recv().expect("test channel");

    assert_eq!(
        unsafe { release_handle(&raw mut handle, null()) },
        ADBC_STATUS_OK
    );
    assert!(handle.private_data.is_null());

    // Finishing the cancel frees the object.
    resume_sender.send(()).expect("test channel");
    assert_eq!(canceller.join().expect("cancel thread"), ADBC_STATUS_OK);

    assert_eq!(
        unsafe { cancel(&raw mut handle, null()) },
        AdbcStatusCode::from(Status::InvalidState)
    );
}

#[test]
fn returning_an_error_does_not_poison() {
    let mut handle = Counter::new();
    let status = unsafe {
        with_state(&raw mut handle, null(), |_| {
            Err(Error::with_message_and_status("nope", Status::NotFound))
        })
    };
    assert_eq!(status, AdbcStatusCode::from(Status::NotFound));

    let status = unsafe {
        with_state(&raw mut handle, null(), |state| {
            *state = 5;
            Ok(())
        })
    };
    assert_eq!(status, ADBC_STATUS_OK);
}

#[test]
fn dispatching_on_a_released_handle_is_an_invalid_state() {
    let mut handle = Counter::new();
    assert_eq!(
        unsafe { release_handle(&raw mut handle, null()) },
        ADBC_STATUS_OK
    );
    assert!(handle.private_data.is_null());
    assert_eq!(
        unsafe { release_handle(&raw mut handle, null()) },
        AdbcStatusCode::from(Status::InvalidState)
    );
    assert_eq!(
        unsafe { with_state(&raw mut handle, null(), |_| Ok(())) },
        AdbcStatusCode::from(Status::InvalidState)
    );
}

/// The main `Cancel` use case in adbc.h: one thread blocks in a call while another cancels it.
#[test]
fn cancel_does_not_wait_for_a_held_dispatch_turn() {
    let cancelled = Arc::new(AtomicBool::new(false));
    let mut handle = Counter::with_cancel(RecordingCancel(cancelled.clone()));
    let (hold_sender, hold_receiver) = channel::<()>();
    let (started_sender, started_receiver) = channel::<()>();

    let address = (&raw mut handle) as usize;
    let worker = std::thread::spawn(move || unsafe {
        with_state(address as *mut Counter, null(), |_| {
            started_sender.send(()).expect("test channel");
            hold_receiver.recv().expect("test channel");
            Ok(())
        })
    });
    started_receiver.recv().unwrap();

    // The state lock is held; cancel must not wait for it.
    let status = unsafe { cancel(&raw mut handle, null()) };
    assert_eq!(status, ADBC_STATUS_OK);
    assert!(cancelled.load(Ordering::SeqCst));

    hold_sender.send(()).unwrap();
    assert_eq!(worker.join().unwrap(), ADBC_STATUS_OK);
}

#[test]
fn concurrent_dispatches_serialize_instead_of_racing() {
    const THREADS: usize = 8;
    const CALLS: u32 = 100;

    let mut handle = Counter::new();
    let address = (&raw mut handle) as usize;
    let workers: Vec<_> = (0..THREADS)
        .map(|_| {
            std::thread::spawn(move || {
                for _ in 0..CALLS {
                    let status = unsafe {
                        with_state(address as *mut Counter, null(), |state| {
                            *state += 1;
                            Ok(())
                        })
                    };
                    assert_eq!(status, ADBC_STATUS_OK);
                }
            })
        })
        .collect();
    for worker in workers {
        worker.join().unwrap();
    }

    let total = *unsafe { lock_state(&raw mut handle) }.unwrap();
    assert_eq!(total, u32::try_from(THREADS).unwrap() * CALLS);
}

#[test]
fn cancel_still_works_after_a_panic_poisoned_the_object() {
    let cancelled = Arc::new(AtomicBool::new(false));
    let mut handle = Counter::with_cancel(RecordingCancel(cancelled.clone()));
    let status = unsafe { with_state(&raw mut handle, null(), |_| panic!("inside the driver")) };
    assert_eq!(status, AdbcStatusCode::from(Status::Internal));

    let status = unsafe { cancel(&raw mut handle, null()) };
    assert_eq!(status, ADBC_STATUS_OK);
    assert!(cancelled.load(Ordering::SeqCst));
}

#[test]
fn cancel_without_an_installed_handle_is_an_invalid_state() {
    assert_eq!(
        unsafe { cancel::<Counter>(null(), null()) },
        AdbcStatusCode::from(Status::InvalidState)
    );
    let mut empty = Counter {
        private_data: null(),
    };
    assert_eq!(
        unsafe { cancel(&raw mut empty, null()) },
        AdbcStatusCode::from(Status::InvalidState)
    );
    // A live object whose `Init` has not installed a handle yet.
    let mut handle = Counter::new();
    assert_eq!(
        unsafe { cancel(&raw mut handle, null()) },
        AdbcStatusCode::from(Status::InvalidState)
    );

    let cancelled = Arc::new(AtomicBool::new(false));
    let cancel_handle = Box::new(RecordingCancel(cancelled.clone()));
    unsafe { install_cancel_handle(&raw mut handle, cancel_handle) };
    assert_eq!(unsafe { cancel(&raw mut handle, null()) }, ADBC_STATUS_OK);
    assert!(cancelled.load(Ordering::SeqCst));
}

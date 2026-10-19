use std::ffi::CStr;
use std::sync::Arc;

use adbc_core::error::Error;
use arrow_array::ffi_stream::RecordBatchStreamReader;
use arrow_array::{ArrayRef, Int64Array, RecordBatch, RecordBatchIterator};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use super::*;
use crate::ffi::test_support::null;

/// Export with no `private_driver`, as for a directly linked caller.
fn exported(reader: Box<dyn RecordBatchReader + Send>) -> FFI_ArrowArrayStream {
    export_reader(reader, null())
}

struct PanickingReader {
    schema: SchemaRef,
    panic_in_schema: bool,
}

impl Iterator for PanickingReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        panic!("next contains a NUL: \0")
    }
}

impl RecordBatchReader for PanickingReader {
    fn schema(&self) -> SchemaRef {
        assert!(!self.panic_in_schema, "schema failed");
        self.schema.clone()
    }
}

#[test]
fn exports_an_ordinary_reader() {
    let batch =
        RecordBatch::try_from_iter([("value", Arc::new(Int64Array::from(vec![7])) as ArrayRef)])
            .unwrap();
    let schema = batch.schema();
    let mut stream = exported(Box::new(RecordBatchIterator::new(
        std::iter::once(Ok(batch.clone())),
        schema,
    )));
    let mut reader = unsafe { RecordBatchStreamReader::from_raw(&raw mut stream) }.unwrap();
    assert_eq!(reader.next().unwrap().unwrap(), batch);
    assert!(reader.next().is_none());
}

/// The C++ suite's `SqlQueryCancel` requires `get_next` to return `ECANCELED` after a cancel.
#[test]
fn exports_adbc_statuses_as_errno() {
    for (status, expected) in [
        (Status::Ok, EIO),
        (Status::Unknown, EIO),
        (Status::NotImplemented, ENOTSUP),
        (Status::NotFound, ENOENT),
        (Status::AlreadyExists, EEXIST),
        (Status::InvalidArguments, EINVAL),
        (Status::InvalidState, EINVAL),
        (Status::InvalidData, EIO),
        (Status::Integrity, EIO),
        (Status::Internal, EIO),
        (Status::IO, EIO),
        (Status::Cancelled, ECANCELED),
        (Status::Timeout, ETIMEDOUT),
        (Status::Unauthenticated, EACCES),
        (Status::Unauthorized, EACCES),
    ] {
        let error = Error::with_message_and_status("ADBC stream failure", status);
        let mut stream = error_stream(ArrowError::ExternalError(Box::new(error)));
        let mut out = FFI_ArrowArray::empty();
        assert_eq!(
            unsafe { stream.get_next().unwrap()(&raw mut stream, &raw mut out) },
            expected,
            "{status:?}"
        );
        let message = unsafe { CStr::from_ptr(stream.get_last_error().unwrap()(&raw mut stream)) }
            .to_str()
            .unwrap();
        assert!(message.contains("ADBC stream failure"), "{message}");
    }

    // Found even when nested in another external error.
    let error = Error::with_message_and_status("cancelled", Status::Cancelled);
    let error = ArrowError::ExternalError(Box::new(ArrowError::ExternalError(Box::new(error))));
    let mut stream = error_stream(error);
    let mut out = FFI_ArrowArray::empty();
    assert_eq!(
        unsafe { stream.get_next().unwrap()(&raw mut stream, &raw mut out) },
        ECANCELED
    );

    // A plain Arrow error keeps Arrow's errno mapping.
    let mut stream = error_stream(ArrowError::NotYetImplemented("not supported".into()));
    assert_eq!(
        unsafe { stream.get_next().unwrap()(&raw mut stream, &raw mut out) },
        ENOSYS
    );
    let message = unsafe { CStr::from_ptr(stream.get_last_error().unwrap()(&raw mut stream)) }
        .to_str()
        .unwrap();
    assert!(message.contains("not supported"), "{message}");
}

/// A nonconforming host may call a saved callback after release.
#[test]
fn get_last_error_tolerates_a_released_or_null_stream() {
    let mut stream = error_stream(ArrowError::ComputeError("boom".to_owned()));
    let saved = stream.get_last_error().unwrap();
    unsafe { stream.release().unwrap()(&raw mut stream) };
    assert!(unsafe { saved(&raw mut stream) }.is_null());
    assert!(unsafe { saved(null()) }.is_null());
}

fn error_stream(error: ArrowError) -> FFI_ArrowArrayStream {
    exported(Box::new(RecordBatchIterator::new(
        std::iter::once(Err(error)),
        Arc::new(Schema::empty()),
    )))
}

#[test]
fn error_from_array_stream_exposes_the_pending_error() {
    use super::super::abi::ADBC_STATUS_CANCELLED;
    use super::super::error::{error_get_detail, error_get_detail_count};

    let mut error = Error::with_message_and_status("stream blew up", Status::Cancelled);
    // Shaped like the details `error::from_spanner` forwards.
    error.details = Some(vec![(
        "google.rpc.retryinfo".to_string(),
        br#"{"retryDelay":"1s"}"#.to_vec(),
    )]);
    let mut stream = error_stream(ArrowError::ExternalError(Box::new(error)));
    let mut out = FFI_ArrowArray::empty();
    assert_eq!(
        unsafe { stream.get_next().unwrap()(&raw mut stream, &raw mut out) },
        ECANCELED
    );

    let mut status: AdbcStatusCode = 0xEE;
    let exported = unsafe { error_from_array_stream(&raw mut stream, &raw mut status) };
    assert!(!exported.is_null());
    assert_eq!(status, ADBC_STATUS_CANCELLED);
    // The driver's message, without the Arrow prefix `get_last_error` shows.
    let message = unsafe { CStr::from_ptr((*exported).message) }
        .to_str()
        .unwrap();
    assert_eq!(message, "stream blew up");
    assert_eq!(unsafe { error_get_detail_count(exported) }, 1);
    let detail = unsafe { error_get_detail(exported, 0) };
    assert_eq!(
        unsafe { CStr::from_ptr(detail.key) }.to_str().unwrap(),
        "google.rpc.retryinfo"
    );
    assert_eq!(
        unsafe { std::slice::from_raw_parts(detail.value, detail.value_length) },
        br#"{"retryDelay":"1s"}"#
    );

    // Exporting again must not leak; a null status is not written.
    let exported = unsafe { error_from_array_stream(&raw mut stream, null()) };
    assert!(!exported.is_null());
    assert_eq!(unsafe { error_get_detail_count(exported) }, 1);

    // A released stream is no longer recognized.
    unsafe { stream.release().unwrap()(&raw mut stream) };
    let mut status: AdbcStatusCode = 0xEE;
    assert!(unsafe { error_from_array_stream(&raw mut stream, &raw mut status) }.is_null());
    assert_eq!(
        status, 0xEE,
        "status must stay untouched for unrecognized streams"
    );
}

#[test]
fn error_from_array_stream_reports_ok_when_nothing_failed() {
    use super::super::abi::ADBC_STATUS_OK;

    let mut stream = exported(Box::new(RecordBatchIterator::new(
        std::iter::empty(),
        Arc::new(Schema::empty()),
    )));
    let mut status: AdbcStatusCode = 0xEE;
    let exported = unsafe { error_from_array_stream(&raw mut stream, &raw mut status) };
    assert!(!exported.is_null());
    assert_eq!(status, ADBC_STATUS_OK);
    assert!(unsafe { (*exported).message.is_null() });
    unsafe { stream.release().unwrap()(&raw mut stream) };
}

#[test]
fn error_from_array_stream_rejects_foreign_and_null_streams() {
    unsafe extern "C" fn foreign_release(_stream: *mut FFI_ArrowArrayStream) {}

    let mut status: AdbcStatusCode = 0xEE;
    assert!(unsafe { error_from_array_stream(null(), &raw mut status) }.is_null());

    // Another library's stream.
    let mut foreign = unsafe {
        FFI_ArrowArrayStream::new_unchecked(
            None,
            None,
            None,
            Some(foreign_release),
            (&raw mut status).cast::<c_void>(),
        )
    };
    assert!(unsafe { error_from_array_stream(&raw mut foreign, &raw mut status) }.is_null());
    assert_eq!(
        status, 0xEE,
        "status must stay untouched for unrecognized streams"
    );
}

#[test]
fn contains_reader_panics_in_get_next() {
    let mut stream = exported(Box::new(PanickingReader {
        schema: Arc::new(Schema::empty()),
        panic_in_schema: false,
    }));
    let mut out = FFI_ArrowArray::empty();
    assert_eq!(
        unsafe { stream.get_next().unwrap()(&raw mut stream, &raw mut out) },
        EINVAL
    );
    let message = unsafe { CStr::from_ptr(stream.get_last_error().unwrap()(&raw mut stream)) }
        .to_str()
        .unwrap();
    assert!(message.contains("next contains a NUL: ?"), "{message}");

    // A reader that panicked is not entered again.
    assert_eq!(
        unsafe { stream.get_next().unwrap()(&raw mut stream, &raw mut out) },
        EINVAL
    );
}

#[test]
fn contains_reader_panics_in_get_schema() {
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
    let mut stream = exported(Box::new(PanickingReader {
        schema,
        panic_in_schema: true,
    }));
    let mut out = FFI_ArrowSchema::empty();
    assert_eq!(
        unsafe { stream.get_schema().unwrap()(&raw mut stream, &raw mut out) },
        EINVAL
    );
    let message = unsafe { CStr::from_ptr(stream.get_last_error().unwrap()(&raw mut stream)) }
        .to_str()
        .unwrap();
    assert!(message.contains("schema failed"), "{message}");
}

#[test]
fn contains_reader_panics_in_release() {
    struct DropPanic(PanickingReader);

    impl Iterator for DropPanic {
        type Item = Result<RecordBatch, ArrowError>;

        fn next(&mut self) -> Option<Self::Item> {
            self.0.next()
        }
    }

    impl RecordBatchReader for DropPanic {
        fn schema(&self) -> SchemaRef {
            self.0.schema()
        }
    }

    impl Drop for DropPanic {
        fn drop(&mut self) {
            panic!("drop failed")
        }
    }

    let mut stream = exported(Box::new(DropPanic(PanickingReader {
        schema: Arc::new(Schema::empty()),
        panic_in_schema: false,
    })));
    unsafe { stream.release().unwrap()(&raw mut stream) };
    assert!(stream.release().is_none());
    assert!(stream.private_data().is_null());
}

/// The driver manager reads error details through `private_driver`, which nothing but the driver
/// can fill in for an error in driver-owned storage.
#[test]
fn a_stream_error_carries_the_handles_driver_vtable() {
    let vtable = 0x5eed_usize as *const AdbcDriver;
    let mut stream = export_reader(
        Box::new(RecordBatchIterator::new(
            std::iter::once(Err(ArrowError::MemoryError("no room".to_owned()))),
            Arc::new(Schema::empty()),
        )),
        vtable,
    );
    let mut out = FFI_ArrowArray::empty();
    assert_eq!(
        unsafe { stream.get_next().unwrap()(&raw mut stream, &raw mut out) },
        ENOMEM
    );

    let mut status: AdbcStatusCode = 0xEE;
    let exported = unsafe { error_from_array_stream(&raw mut stream, &raw mut status) };
    assert!(!exported.is_null());
    assert_eq!(unsafe { (*exported).private_driver }, vtable);
    unsafe { stream.release().unwrap()(&raw mut stream) };
}

/// A nonconforming host may release a stream twice or call saved callbacks after release.
#[test]
fn a_released_stream_survives_a_second_release_and_stale_callbacks() {
    let batch =
        RecordBatch::try_from_iter([("value", Arc::new(Int64Array::from(vec![1])) as ArrayRef)])
            .unwrap();
    let schema = batch.schema();
    let mut stream = exported(Box::new(RecordBatchIterator::new(
        std::iter::once(Ok(batch)),
        schema,
    )));
    let (get_next, get_schema, release) = (
        stream.get_next().unwrap(),
        stream.get_schema().unwrap(),
        stream.release().unwrap(),
    );

    unsafe { release(&raw mut stream) };
    assert!(stream.private_data().is_null());
    assert!(stream.get_next().is_none());
    assert!(stream.get_schema().is_none());
    assert!(stream.release().is_none());

    // A second release is a no-op.
    unsafe { release(&raw mut stream) };
    assert!(stream.private_data().is_null());

    // Saved callbacks report EINVAL.
    let mut array = FFI_ArrowArray::empty();
    let mut out_schema = FFI_ArrowSchema::empty();
    assert_eq!(unsafe { get_next(&raw mut stream, &raw mut array) }, EINVAL);
    assert_eq!(
        unsafe { get_schema(&raw mut stream, &raw mut out_schema) },
        EINVAL
    );
    // Likewise for a null stream.
    assert_eq!(unsafe { get_next(null(), &raw mut array) }, EINVAL);
    assert_eq!(unsafe { get_schema(null(), &raw mut out_schema) }, EINVAL);
    unsafe { release(null()) };
}

/// After a successful read, neither `get_last_error` nor `AdbcErrorFromArrayStream` reports the
/// earlier failure.
#[test]
fn a_successful_read_clears_the_previous_failure() {
    use super::super::abi::ADBC_STATUS_OK;

    let batch =
        RecordBatch::try_from_iter([("value", Arc::new(Int64Array::from(vec![7])) as ArrayRef)])
            .unwrap();
    let schema = batch.schema();
    let mut stream = exported(Box::new(RecordBatchIterator::new(
        std::iter::once(Ok(batch)),
        schema,
    )));

    // Fail with a null output.
    assert_eq!(
        unsafe { stream.get_next().unwrap()(&raw mut stream, null()) },
        EINVAL
    );
    assert!(!unsafe { stream.get_last_error().unwrap()(&raw mut stream) }.is_null());

    let mut array = FFI_ArrowArray::empty();
    assert_eq!(
        unsafe { stream.get_next().unwrap()(&raw mut stream, &raw mut array) },
        0
    );
    assert!(unsafe { stream.get_last_error().unwrap()(&raw mut stream) }.is_null());

    let mut status: AdbcStatusCode = 0xEE;
    let exported = unsafe { error_from_array_stream(&raw mut stream, &raw mut status) };
    assert_eq!(status, ADBC_STATUS_OK);
    assert!(unsafe { (*exported).message.is_null() });

    unsafe { array.release().unwrap()(&raw mut array) };
    unsafe { stream.release().unwrap()(&raw mut stream) };
}

/// `get_schema` may be called at any time and consumes nothing, so it must not clear a read
/// failure the caller has yet to report.
#[test]
fn reading_the_schema_after_a_failed_read_keeps_the_failure() {
    use super::super::abi::ADBC_STATUS_CANCELLED;

    let error = Error::with_message_and_status("the query was cancelled", Status::Cancelled);
    let mut stream = error_stream(ArrowError::ExternalError(Box::new(error)));

    let mut array = FFI_ArrowArray::empty();
    assert_eq!(
        unsafe { stream.get_next().unwrap()(&raw mut stream, &raw mut array) },
        ECANCELED
    );

    let mut schema = FFI_ArrowSchema::empty();
    assert_eq!(
        unsafe { stream.get_schema().unwrap()(&raw mut stream, &raw mut schema) },
        0,
        "the schema is still readable after a failed read"
    );

    let pending = unsafe { stream.get_last_error().unwrap()(&raw mut stream) };
    assert!(
        !pending.is_null(),
        "the read failure was cleared by get_schema"
    );
    let message = unsafe { CStr::from_ptr(pending) }.to_str().unwrap();
    assert!(message.contains("the query was cancelled"), "{message}");

    let mut status: AdbcStatusCode = 0xEE;
    let exported = unsafe { error_from_array_stream(&raw mut stream, &raw mut status) };
    assert_eq!(
        status, ADBC_STATUS_CANCELLED,
        "the read failure was reported as ADBC_STATUS_OK"
    );
    assert!(!unsafe { (*exported).message.is_null() });

    unsafe { stream.release().unwrap()(&raw mut stream) };
}

#[test]
fn contains_panicking_payloads_in_callbacks_and_release() {
    use crate::ffi::test_support::PanicOnDrop;

    struct Reader;

    impl Iterator for Reader {
        type Item = Result<RecordBatch, ArrowError>;

        fn next(&mut self) -> Option<Self::Item> {
            std::panic::panic_any(PanicOnDrop)
        }
    }

    impl RecordBatchReader for Reader {
        fn schema(&self) -> SchemaRef {
            std::panic::panic_any(PanicOnDrop)
        }
    }

    impl Drop for Reader {
        fn drop(&mut self) {
            std::panic::panic_any(PanicOnDrop)
        }
    }

    for schema_callback in [false, true] {
        let mut stream = exported(Box::new(Reader));
        let code = if schema_callback {
            let mut out = FFI_ArrowSchema::empty();
            unsafe { stream.get_schema().unwrap()(&raw mut stream, &raw mut out) }
        } else {
            let mut out = FFI_ArrowArray::empty();
            unsafe { stream.get_next().unwrap()(&raw mut stream, &raw mut out) }
        };
        assert_eq!(code, EINVAL);
        let message = unsafe { CStr::from_ptr(stream.get_last_error().unwrap()(&raw mut stream)) }
            .to_str()
            .unwrap();
        assert!(message.contains("unknown panic payload"), "{message}");
        let mut status = ADBC_STATUS_OK;
        assert!(!unsafe { error_from_array_stream(&raw mut stream, &raw mut status) }.is_null());
        assert_eq!(status, AdbcStatusCode::from(Status::Internal));
        unsafe { stream.release().unwrap()(&raw mut stream) };
        assert!(stream.release().is_none());
        assert!(stream.private_data().is_null());
    }
}

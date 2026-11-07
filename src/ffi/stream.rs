//! Arrow C stream export with panic containment.
//!
//! Arrow-rs's exporter calls the [`RecordBatchReader`] straight from its `extern "C"` callbacks,
//! so a reader panic would abort the host. Every stream the driver exports uses these callbacks
//! instead.

use std::ffi::{CString, c_char, c_int, c_void};
use std::panic::{AssertUnwindSafe, catch_unwind};

use adbc_core::error::{Error, Status};
use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::{Array, RecordBatchReader, StructArray};
use arrow_schema::{ArrowError, Schema};

use libc::{EACCES, ECANCELED, EEXIST, EINVAL, EIO, ENOENT, ENOMEM, ENOSYS, ENOTSUP, ETIMEDOUT};

use super::abi::{ADBC_STATUS_OK, AdbcError, AdbcStatusCode};
use super::error::export_error;
use super::guard::{drop_panic_payload, panic_message, sanitized_cstring};
use crate::error::{chain, err, invalid_argument};

/// A callback failure, as the C string `get_last_error` returns and as the structured error
/// [`error_from_array_stream`] exports.
struct LastError {
    message: CString,
    error: Error,
}

struct PrivateData {
    reader: Box<dyn RecordBatchReader + Send>,
    last_error: Option<LastError>,
    /// The error returned by [`error_from_array_stream`], which must stay valid until the stream
    /// is released.
    exported_error: AdbcError,
    panicked: bool,
}

impl Drop for PrivateData {
    fn drop(&mut self) {
        if let Some(release) = self.exported_error.release {
            unsafe { release(&raw mut self.exported_error) };
        }
    }
}

/// Export a driver-produced schema. Failure is a driver bug, hence `Internal`.
pub(super) fn export_schema(schema: &Schema) -> Result<FFI_ArrowSchema, Error> {
    FFI_ArrowSchema::try_from(schema).map_err(|failure| {
        err(
            format!("failed to export a schema over the C data interface: {failure}"),
            Status::Internal,
        )
    })
}

pub(super) fn export_reader(reader: Box<dyn RecordBatchReader + Send>) -> FFI_ArrowArrayStream {
    let private_data = Box::new(PrivateData {
        reader,
        last_error: None,
        exported_error: AdbcError::default(),
        panicked: false,
    });
    // SAFETY: the callbacks below implement the Arrow C Stream contract over a `PrivateData`,
    // which the stream owns until `release` frees it exactly once and marks the stream released.
    unsafe {
        FFI_ArrowArrayStream::new_unchecked(
            Some(get_schema),
            Some(get_next),
            Some(get_last_error),
            Some(release),
            Box::into_raw(private_data).cast::<c_void>(),
        )
    }
}

unsafe extern "C" fn get_schema(
    stream: *mut FFI_ArrowArrayStream,
    out: *mut FFI_ArrowSchema,
) -> c_int {
    unsafe {
        catch_callback(stream, |private| {
            if out.is_null() {
                return private.fail("Arrow C Stream schema output must not be null");
            }
            match FFI_ArrowSchema::try_from(private.reader.schema().as_ref()) {
                Ok(schema) => {
                    std::ptr::write_unaligned(out, schema);
                    0
                }
                Err(error) => private.arrow_error(&error),
            }
        })
    }
}

unsafe extern "C" fn get_next(
    stream: *mut FFI_ArrowArrayStream,
    out: *mut FFI_ArrowArray,
) -> c_int {
    unsafe {
        catch_callback(stream, |private| {
            // Each read supersedes the previous error; every failing path records a new one.
            // `get_schema` leaves it intact so inspecting the schema cannot hide a read failure.
            private.last_error = None;
            if out.is_null() {
                return private.fail("Arrow C Stream array output must not be null");
            }
            match private.reader.next() {
                None => {
                    std::ptr::write_unaligned(out, FFI_ArrowArray::empty());
                    0
                }
                Some(Ok(batch)) => {
                    let array = StructArray::from(batch);
                    std::ptr::write_unaligned(out, FFI_ArrowArray::new(&array.to_data()));
                    0
                }
                Some(Err(error)) => private.arrow_error(&error),
            }
        })
    }
}

// Guard-exempt: reads only.
unsafe extern "C" fn get_last_error(stream: *mut FFI_ArrowArrayStream) -> *const c_char {
    // A released or zeroed stream has null `private_data`, which must not be dereferenced.
    if stream.is_null() || unsafe { (*stream).private_data().is_null() } {
        return std::ptr::null();
    }
    unsafe { private_data(stream) }
        .last_error
        .as_ref()
        .map_or(std::ptr::null(), |error| error.message.as_ptr())
}

/// `AdbcErrorFromArrayStream`: the structured error behind a failed stream callback, which the
/// Arrow C stream interface can only report as an errno and a message.
///
/// Streams from [`export_reader`] are recognized by their [`release`] callback. Foreign and
/// released streams get null, with `status` untouched, as adbc.h specifies.
///
/// # Safety
/// `stream` must be null or point to a valid or released `ArrowArrayStream`; `status` must be
/// null or writable.
// Guard-exempt: only clones an error and formats it for export.
pub(super) unsafe extern "C" fn error_from_array_stream(
    stream: *mut FFI_ArrowArrayStream,
    status: *mut AdbcStatusCode,
) -> *const AdbcError {
    if stream.is_null() || unsafe { (*stream).private_data().is_null() } {
        return std::ptr::null();
    }
    let ours = unsafe { (*stream).release() }.is_some_and(|callback| {
        std::ptr::fn_addr_eq(
            callback,
            release as unsafe extern "C" fn(*mut FFI_ArrowArrayStream),
        )
    });
    if !ours {
        return std::ptr::null();
    }
    let private = unsafe { private_data(stream) };
    let exported = &raw mut private.exported_error;
    let code = match private.last_error.as_ref() {
        // Nothing pending: OK and an empty error, as the reference C++ drivers do.
        None => {
            if let Some(release) = private.exported_error.release {
                unsafe { release(exported) };
            }
            ADBC_STATUS_OK
        }
        // Exporting again releases the previous error first.
        Some(last) => {
            unsafe { export_error(exported, last.error.clone()) };
            AdbcStatusCode::from(last.error.status)
        }
    };
    if !status.is_null() {
        unsafe { std::ptr::write_unaligned(status, code) };
    }
    // The driver manager writes `private_driver` through the returned pointer.
    exported.cast_const()
}

// `inline(never)` keeps one copy of this function, so its address identifies our streams in
// `error_from_array_stream`. An inlinable function may be duplicated per codegen unit, and Miri
// gives each reification of one a distinct address.
#[inline(never)]
unsafe extern "C" fn release(stream: *mut FFI_ArrowArrayStream) {
    if stream.is_null() {
        return;
    }
    let private_data = unsafe { (*stream).private_data() };
    // Clear the callbacks, marking the stream released. `write` skips dropping the old value,
    // whose `Drop` would re-enter this callback.
    unsafe { std::ptr::write(stream, FFI_ArrowArrayStream::empty()) };
    if !private_data.is_null() {
        // The reader's destructor runs arbitrary code and must not unwind through `release`.
        if let Err(cause) = catch_unwind(AssertUnwindSafe(|| {
            drop(unsafe { Box::from_raw(private_data.cast::<PrivateData>()) });
        })) {
            drop_panic_payload(cause);
        }
    }
}

unsafe fn catch_callback(
    stream: *mut FFI_ArrowArrayStream,
    callback: impl FnOnce(&mut PrivateData) -> c_int,
) -> c_int {
    if stream.is_null() || unsafe { (*stream).private_data().is_null() } {
        return EINVAL;
    }
    let private = unsafe { private_data(stream) };
    if private.panicked {
        return EINVAL;
    }
    match catch_unwind(AssertUnwindSafe(|| callback(private))) {
        Ok(code) => code,
        Err(cause) => {
            private.panicked = true;
            let message = format!(
                "Uncaught panic in the Spanner Arrow stream: {}",
                panic_message(cause.as_ref())
            );
            drop_panic_payload(cause);
            private.set_error(err(message, Status::Internal));
            EINVAL
        }
    }
}

unsafe fn private_data<'a>(stream: *mut FFI_ArrowArrayStream) -> &'a mut PrivateData {
    unsafe { &mut *((*stream).private_data().cast::<PrivateData>()) }
}

impl PrivateData {
    fn set_error(&mut self, error: Error) {
        self.last_error = Some(LastError {
            message: sanitized_cstring(&error.message),
            error,
        });
    }

    fn fail(&mut self, message: &str) -> c_int {
        self.set_error(invalid_argument(message));
        EINVAL
    }

    fn arrow_error(&mut self, error: &ArrowError) -> c_int {
        // Arrow's errno mapping, unless an ADBC error embedded in the chain decides both values.
        let (errno, status) = match error {
            ArrowError::NotYetImplemented(_) => (ENOSYS, Status::NotImplemented),
            ArrowError::MemoryError(_) => (ENOMEM, Status::Internal),
            ArrowError::IoError(_, _) => (EIO, Status::IO),
            _ => (EINVAL, Status::Unknown),
        };
        let embedded = chain(error)
            .find_map(|source| source.downcast_ref::<Error>())
            .cloned();
        let errno = embedded
            .as_ref()
            .map_or(errno, |embedded| adbc_status_to_errno(embedded.status));
        let structured = embedded.unwrap_or_else(|| err(error.to_string(), status));
        // `get_last_error` shows the whole Arrow chain; the structured error keeps the embedded
        // driver error's message and details.
        self.last_error = Some(LastError {
            message: sanitized_cstring(&error.to_string()),
            error: structured,
        });
        errno
    }
}

fn adbc_status_to_errno(status: Status) -> c_int {
    match status {
        Status::Ok | Status::Unknown => EIO,
        Status::NotImplemented => ENOTSUP,
        Status::NotFound => ENOENT,
        Status::AlreadyExists => EEXIST,
        Status::InvalidArguments | Status::InvalidState => EINVAL,
        Status::InvalidData | Status::Integrity | Status::Internal | Status::IO => EIO,
        Status::Cancelled => ECANCELED,
        Status::Timeout => ETIMEDOUT,
        Status::Unauthenticated | Status::Unauthorized => EACCES,
    }
}

#[cfg(test)]
mod tests;

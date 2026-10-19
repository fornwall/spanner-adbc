//! Import of the Arrow C streams passed to `AdbcStatementBindStream`.

use std::ffi::{CStr, c_int};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use adbc_core::error::Result;
use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema, from_ffi_and_data_type};
use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::{RecordBatch, RecordBatchOptions, RecordBatchReader, StructArray};
use arrow_data::ArrayData;
use arrow_schema::{ArrowError, DataType, Fields, Schema, SchemaRef};

use super::guard::{drop_panic_payload, panic_message};
use crate::error::invalid_argument;

#[derive(Debug)]
pub(super) struct BoundStreamReader {
    stream: FFI_ArrowArrayStream,
    schema: SchemaRef,
    finished: bool,
}

impl BoundStreamReader {
    /// # Safety
    /// `stream` must be null or point to a valid Arrow C stream. The reader takes ownership once
    /// the callbacks are validated; a stream with missing callbacks stays with the caller.
    pub(super) unsafe fn from_raw(stream: *mut FFI_ArrowArrayStream) -> Result<Self> {
        let Some(stream_ref) = (unsafe { stream.as_ref() }) else {
            return Err(invalid_argument("stream must not be null"));
        };
        if stream_ref.release().is_none() {
            return Err(invalid_argument("stream has already been released"));
        }
        let get_schema = stream_ref
            .get_schema()
            .ok_or_else(|| invalid_argument("stream is missing its get_schema callback"))?;
        if stream_ref.get_next().is_none() {
            return Err(invalid_argument("stream is missing its get_next callback"));
        }
        if stream_ref.get_last_error().is_none() {
            return Err(invalid_argument(
                "stream is missing its get_last_error callback",
            ));
        }
        let mut stream = unsafe { FFI_ArrowArrayStream::from_raw(stream) };
        let mut schema = FFI_ArrowSchema::empty();
        let code = unsafe { get_schema(&raw mut stream, &raw mut schema) };
        if code != 0 {
            return Err(invalid_argument(stream_error(
                &mut stream,
                "get_schema",
                code,
            )));
        }
        if schema.release().is_none() {
            return Err(invalid_argument(
                "bound stream get_schema returned a released schema",
            ));
        }
        let schema = contain_import(|| Schema::try_from(&schema)).map_err(|failure| {
            invalid_argument(format!(
                "bound stream returned an invalid schema: {failure}"
            ))
        })?;
        Ok(Self {
            stream,
            schema: Arc::new(schema),
            finished: false,
        })
    }
}

impl Iterator for BoundStreamReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        let mut array = FFI_ArrowArray::empty();
        let get_next = self
            .stream
            .get_next()
            .expect("validated when the stream was bound");
        let code = unsafe { get_next(&raw mut self.stream, &raw mut array) };
        if code != 0 {
            self.finished = true;
            return Some(Err(ArrowError::CDataInterface(stream_error(
                &mut self.stream,
                "get_next",
                code,
            ))));
        }
        if array.release().is_none() {
            self.finished = true;
            return None;
        }
        if array.num_children() != self.schema.fields().len() {
            self.finished = true;
            return Some(Err(ArrowError::CDataInterface(format!(
                "bound stream returned {} columns, but its schema declares {}",
                array.num_children(),
                self.schema.fields().len(),
            ))));
        }
        let result = contain_import(|| unsafe {
            from_ffi_and_data_type(array, DataType::Struct(self.schema.fields().clone()))
        })
        .and_then(|data| {
            validate_imported(&data, self.schema.fields()).map_err(|failure| {
                ArrowError::CDataInterface(format!(
                    "bound stream returned an invalid Arrow array: {failure}"
                ))
            })?;
            if data.null_count() != 0 {
                return Err(ArrowError::CDataInterface(
                    "bound stream must not contain top-level null rows; put null values in the \
                     individual columns instead"
                        .to_owned(),
                ));
            }
            let row_count = data.len();
            RecordBatch::try_new_with_options(
                self.schema.clone(),
                StructArray::from(data).into_parts().1,
                &RecordBatchOptions::new().with_row_count(Some(row_count)),
            )
        });
        self.finished = result.is_err();
        Some(result)
    }
}

impl RecordBatchReader for BoundStreamReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Run an Arrow C data import. Arrow's importer asserts instead of returning an error on some
/// malformed input, such as a struct whose children disagree with its fields at any depth.
/// Catching the panic here keeps a caller's mistake from poisoning the statement in `dispatch`.
/// Unwinding drops the imported structs, which runs their release callbacks.
pub(super) fn contain_import<T>(
    import: impl FnOnce() -> std::result::Result<T, ArrowError>,
) -> std::result::Result<T, ArrowError> {
    std::panic::catch_unwind(AssertUnwindSafe(import)).unwrap_or_else(|cause| {
        let message = format!(
            "invalid Arrow array or schema: {}",
            panic_message(cause.as_ref())
        );
        drop_panic_payload(cause);
        Err(ArrowError::CDataInterface(message))
    })
}

/// Validate imported data, naming the offending column. Arrow's C importer uses
/// `ArrayData::new_unchecked`, so offsets, view indices, UTF-8 and null counts must be checked
/// before safe accessors rely on them.
///
/// Buffer bounds cannot be checked: the C interface passes pointers without allocation lengths,
/// so producers must still uphold `from_ffi`'s safety contract.
pub(super) fn validate_imported(
    data: &ArrayData,
    fields: &Fields,
) -> std::result::Result<(), String> {
    // The struct has no values of its own; `validate_full` checks each column recursively.
    data.validate_data()
        .map_err(|failure| format!("the top-level struct is invalid: {failure}"))?;
    for (field, column) in fields.iter().zip(data.child_data()) {
        column
            .validate_full()
            .map_err(|failure| format!("column {:?} is invalid: {failure}", field.name()))?;
    }
    Ok(())
}

fn stream_error(stream: &mut FFI_ArrowArrayStream, callback: &str, code: c_int) -> String {
    let get_last_error = stream
        .get_last_error()
        .expect("validated when the stream was bound");
    let message = unsafe { get_last_error(stream) };
    let detail = if message.is_null() {
        "producer supplied no error description".into()
    } else {
        unsafe { CStr::from_ptr(message) }.to_string_lossy()
    };
    format!("bound Arrow C stream {callback} failed with error code {code}: {detail}")
}

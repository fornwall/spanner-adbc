//! `AdbcStatement*` entry points.
//!
//! Statements have no `Init`: `AdbcStatementNew` builds a live [`SpannerStatement`] from its
//! connection, and options apply directly instead of through a [`super::handle::Staged`] buffer.

use std::ffi::{c_char, c_void};

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::{OptionStatement, OptionValue};
use adbc_core::{Connection, Optionable, Statement};
use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema, from_ffi};
use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::{RecordBatch, StructArray};
use arrow_schema::{DataType, Schema};

use super::abi::{
    ADBC_STATUS_OK, AdbcConnection, AdbcDriver, AdbcError, AdbcPartitions, AdbcStatement,
    AdbcStatusCode,
};
use super::guard::{byte_slice, catch, required_output, required_str, write_out};
use super::handle::{Exported, finish};
use super::import::{BoundStreamReader, contain_import, validate_imported};
use super::options::{FfiHandle, already_populated, driver_of, slot_of, with_state};
use crate::error::invalid_argument;
use crate::statement::SpannerStatement;

pub(super) const KIND: &str = "statement";

pub(super) type State = SpannerStatement;

impl FfiHandle for AdbcStatement {
    type State = State;
    const KIND: &'static str = KIND;

    fn private_data(&self) -> *mut c_void {
        self.private_data
    }

    fn private_data_mut(&mut self) -> &mut *mut c_void {
        &mut self.private_data
    }

    fn private_driver(&self) -> *const AdbcDriver {
        self.private_driver
    }
}

impl super::options::OptionTarget for SpannerStatement {
    type Key = OptionStatement;
    type Options = Self;

    fn set(&mut self, key: OptionStatement, value: OptionValue) -> Result<()> {
        self.set_option(key, value)
    }

    fn options(&mut self, _kind: &str, _key: &OptionStatement) -> Result<&mut Self> {
        Ok(self)
    }
}

/// # Safety
/// `connection` must be null or point to a valid, initialized `AdbcConnection`, and `statement`
/// must be null or point to a valid `AdbcStatement`.
pub(super) unsafe extern "C" fn statement_new(
    connection: *mut AdbcConnection,
    statement: *mut AdbcStatement,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    let Some(statement) = (unsafe { statement.as_mut() }) else {
        return finish(Err(invalid_argument("statement must not be null")), error);
    };
    if !statement.private_data.is_null() {
        return finish(Err(already_populated(KIND)), error);
    }
    // A panic here poisons the borrowed connection; the statement has no state yet.
    let created = catch(|| {
        let mut connection = unsafe { super::options::borrow(connection) }?;
        let state = connection.ready(super::connection::KIND)?.new_statement()?;
        // Stored outside the state lock so a running call can be cancelled.
        let cancel = state.get_cancel_handle();
        Ok((state, cancel))
    });
    match created {
        Ok((state, cancel)) => {
            statement.private_data = Exported::into_private_data_with_cancel(state, Some(cancel));
            ADBC_STATUS_OK
        }
        Err(failure) => finish(Err(failure), error),
    }
}

/// # Safety
/// `statement` must be null or point to a valid `AdbcStatement`, and `query` null or a
/// NUL-terminated string.
pub(super) unsafe extern "C" fn statement_set_sql_query(
    statement: *mut AdbcStatement,
    query: *const c_char,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe {
        with_state(statement, error, |state| {
            state.set_sql_query(required_str(query, "query")?)
        })
    }
}

/// # Safety
/// `statement` must be null or point to a valid `AdbcStatement`, and `plan` null or point to
/// `length` initialized bytes.
pub(super) unsafe extern "C" fn statement_set_substrait_plan(
    statement: *mut AdbcStatement,
    plan: *const u8,
    length: usize,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe {
        with_state(statement, error, |state| {
            state.set_substrait_plan(byte_slice(plan, length, "plan")?)
        })
    }
}

/// # Safety
/// `statement` must be null or point to a valid `AdbcStatement`.
pub(super) unsafe extern "C" fn statement_prepare(
    statement: *mut AdbcStatement,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe { with_state(statement, error, State::prepare) }
}

/// # Safety
/// `statement` must be null or point to a valid `AdbcStatement`.
pub(super) unsafe extern "C" fn statement_cancel(
    statement: *mut AdbcStatement,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    // Bypasses `dispatch` so it can interrupt a running call. A released handle reports
    // `InvalidState`; with nothing in flight it returns OK (see `SlotCancelHandle`).
    unsafe { super::handle::cancel::<State>(slot_of(statement), KIND, error) }
}

/// # Safety
/// `statement` must be null or point to a valid `AdbcStatement`, and `values`/`schema` null or
/// point to a valid, non-released Arrow C data interface array and schema.
pub(super) unsafe extern "C" fn statement_bind(
    statement: *mut AdbcStatement,
    values: *mut FFI_ArrowArray,
    schema: *mut FFI_ArrowSchema,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe {
        with_state(statement, error, |state| {
            // Arrow's importer can panic on released inputs. Reject them before taking ownership,
            // so a caller mistake does not consume the other input.
            let Some(values_ref) = values.as_ref() else {
                return Err(invalid_argument("values must not be null"));
            };
            if values_ref.release().is_none() {
                return Err(invalid_argument("values has already been released"));
            }
            let Some(schema_ref) = schema.as_ref() else {
                return Err(invalid_argument("schema must not be null"));
            };
            if schema_ref.release().is_none() {
                return Err(invalid_argument("schema has already been released"));
            }
            // ADBC transfers ownership of both inputs.
            let values = FFI_ArrowArray::from_raw(values);
            let schema = FFI_ArrowSchema::from_raw(schema);
            // The importer also asserts on other malformed input; `contain_import` reports that
            // as an error instead of a panic that would poison the statement.
            let data = contain_import(|| from_ffi(values, &schema)).map_err(|failure| {
                invalid_argument(format!(
                    "bind values are not a valid Arrow array: {failure}"
                ))
            })?;
            let DataType::Struct(fields) = data.data_type() else {
                return Err(invalid_argument(format!(
                    "bind values must be a struct array, not {}",
                    data.data_type()
                )));
            };
            validate_imported(&data, fields).map_err(|failure| {
                invalid_argument(format!(
                    "bind values are not a valid Arrow array: {failure}"
                ))
            })?;
            if data.null_count() != 0 {
                return Err(invalid_argument(
                    "bind values must not contain top-level null rows; put null values in the \
                     individual columns instead",
                ));
            }
            state.bind(RecordBatch::from(StructArray::from(data)))
        })
    }
}

/// # Safety
/// `statement` must be null or point to a valid `AdbcStatement`, and `stream` null or point to a
/// valid, non-released Arrow C stream.
pub(super) unsafe extern "C" fn statement_bind_stream(
    statement: *mut AdbcStatement,
    stream: *mut FFI_ArrowArrayStream,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe {
        with_state(statement, error, |state| {
            let reader = BoundStreamReader::from_raw(stream)?;
            state.bind_stream(Box::new(reader))
        })
    }
}

/// # Safety
/// `statement` must be null or point to a valid `AdbcStatement`; `out` must be null or point to
/// writable storage for an `FFI_ArrowArrayStream`, and `rows_affected` null or writable.
pub(super) unsafe extern "C" fn statement_execute_query(
    statement: *mut AdbcStatement,
    out: *mut FFI_ArrowArrayStream,
    rows_affected: *mut i64,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe {
        with_state(statement, error, |state| {
            // A null `out` asks for an update without results, the only path that can report a
            // row count.
            if out.is_null() {
                let affected = state.execute_update()?.unwrap_or(-1);
                write_rows_affected(rows_affected, affected);
                return Ok(());
            }
            // The row count of a stream is unknown; -1 also overwrites any previous count.
            let reader = state.execute()?;
            std::ptr::write_unaligned(
                out,
                super::stream::export_reader(reader, driver_of(statement)),
            );
            write_rows_affected(rows_affected, -1);
            Ok(())
        })
    }
}

/// # Safety
/// `statement` must be null or point to a valid `AdbcStatement`, and `schema` null or point to
/// writable storage for an `FFI_ArrowSchema`.
pub(super) unsafe extern "C" fn statement_execute_schema(
    statement: *mut AdbcStatement,
    schema: *mut FFI_ArrowSchema,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe {
        with_state(statement, error, |state| {
            required_output(schema, "schema")?;
            let found = state.execute_schema()?;
            write_out(schema, export_schema(&found)?, "schema")
        })
    }
}

/// # Safety
/// `statement` must be null or point to a valid `AdbcStatement`, and `schema` null or point to
/// writable storage for an `FFI_ArrowSchema`.
pub(super) unsafe extern "C" fn statement_get_parameter_schema(
    statement: *mut AdbcStatement,
    schema: *mut FFI_ArrowSchema,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe {
        with_state(statement, error, |state| {
            required_output(schema, "schema")?;
            let found = state.get_parameter_schema()?;
            write_out(schema, export_schema(&found)?, "schema")
        })
    }
}

/// # Safety
/// `statement` must be null or point to a valid `AdbcStatement`; `schema` and `partitions` must be
/// null or point to writable storage for their types, and `rows_affected` null or writable.
pub(super) unsafe extern "C" fn statement_execute_partitions(
    statement: *mut AdbcStatement,
    schema: *mut FFI_ArrowSchema,
    partitions: *mut AdbcPartitions,
    rows_affected: *mut i64,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe {
        with_state(statement, error, |state| {
            // Check outputs before running a query whose result the caller cannot collect.
            required_output(schema, "schema")?;
            required_output(partitions, "partitions")?;

            let result = state.execute_partitions()?;
            // Export the schema before writing either output, so a failure leaves both untouched.
            let exported = export_schema(&result.schema)?;
            std::ptr::write_unaligned(schema, exported);
            std::ptr::write_unaligned(partitions, export_partitions(result.partitions));
            write_rows_affected(rows_affected, result.rows_affected);
            Ok(())
        })
    }
}

/// `rows_affected` is optional, so null is ignored.
///
/// # Safety
/// `dst` must be null or point to a writable `i64`.
unsafe fn write_rows_affected(dst: *mut i64, value: i64) {
    if !dst.is_null() {
        unsafe { std::ptr::write_unaligned(dst, value) };
    }
}

/// Export a driver-produced schema. Failure is a driver bug, hence `Internal`.
fn export_schema(schema: &Schema) -> Result<FFI_ArrowSchema> {
    FFI_ArrowSchema::try_from(schema).map_err(|failure| {
        Error::with_message_and_status(
            format!("failed to export the result schema over the C data interface: {failure}"),
            Status::Internal,
        )
    })
}

/// Owns the partition payloads and both C arrays until the release callback runs. Keeping the
/// vectors whole lets the callback free them without reconstructing their capacities.
struct PartitionsOwner {
    /// Keeps the bytes referenced by `pointers` alive.
    _payloads: Vec<Vec<u8>>,
    pointers: Vec<*const u8>,
    lengths: Vec<usize>,
}

fn export_partitions(payloads: Vec<Vec<u8>>) -> AdbcPartitions {
    let num_partitions = payloads.len();
    let pointers: Vec<*const u8> = payloads.iter().map(|payload| payload.as_ptr()).collect();
    let lengths: Vec<usize> = payloads.iter().map(Vec::len).collect();
    let mut owner = Box::new(PartitionsOwner {
        _payloads: payloads,
        pointers,
        lengths,
    });
    // Null rather than an empty `Vec`'s dangling pointer.
    let (partitions, partition_lengths) = if num_partitions == 0 {
        (std::ptr::null_mut(), std::ptr::null_mut())
    } else {
        (owner.pointers.as_mut_ptr(), owner.lengths.as_mut_ptr())
    };
    AdbcPartitions {
        num_partitions,
        partitions,
        partition_lengths,
        private_data: Box::into_raw(owner).cast::<c_void>(),
        release: Some(release_partitions),
    }
}

/// # Safety
/// `partitions` must be null or point to an `AdbcPartitions` produced by [`export_partitions`].
// Guard-exempt: dropping the owner box frees plain memory and cannot panic.
unsafe extern "C" fn release_partitions(partitions: *mut AdbcPartitions) {
    let Some(partitions) = (unsafe { partitions.as_mut() }) else {
        return;
    };
    if !partitions.private_data.is_null() {
        drop(unsafe { Box::from_raw(partitions.private_data.cast::<PartitionsOwner>()) });
        partitions.private_data = std::ptr::null_mut();
    }
    // The arrays pointed into the freed box.
    partitions.num_partitions = 0;
    partitions.partitions = std::ptr::null_mut();
    partitions.partition_lengths = std::ptr::null_mut();
    // adbc.h requires release to be idempotent.
    partitions.release = None;
}

#[cfg(test)]
mod tests;

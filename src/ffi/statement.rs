//! `AdbcStatement*` entry points.
//!
//! Statements have no `Init`: `AdbcStatementNew` builds a live [`SpannerStatement`] from its
//! connection, and options apply directly instead of through a [`super::handle::Staged`] buffer.

use std::ffi::{c_char, c_void};

use adbc_core::error::Result;
use adbc_core::options::{OptionStatement, OptionValue};
use adbc_core::{Connection, Optionable, Statement};
use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::ffi_stream::FFI_ArrowArrayStream;

use super::abi::{AdbcConnection, AdbcError, AdbcPartitions, AdbcStatement, AdbcStatusCode};
use super::guard::{byte_slice, catch, required_output, required_str, write_out};
use super::handle::{
    Exported, FfiHandle, finish, install_cancel_handle, lock_state, unpopulated, with_state,
};
use super::import::{BoundStreamReader, import_batch};
use super::stream::{export_reader, export_schema};
use crate::statement::SpannerStatement;

pub(super) const KIND: &str = "statement";

pub(super) type State = SpannerStatement;

impl FfiHandle for AdbcStatement {
    type State = State;
    const KIND: &'static str = KIND;

    fn private_data(handle: *mut Self) -> *mut *mut c_void {
        unsafe { &raw mut (*handle).private_data }
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
    // A panic here poisons the borrowed connection; the statement has no state yet.
    let outcome = catch(|| {
        let slot = unsafe { unpopulated(statement) }?;
        let mut connection = unsafe { lock_state(connection) }?;
        let state = connection.ready(super::connection::KIND)?.new_statement()?;
        let cancel = state.get_cancel_handle();
        *slot = Exported::into_private_data(state);
        unsafe { install_cancel_handle(statement, cancel) };
        Ok(())
    });
    unsafe { finish(outcome, error) }
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
            state.bind(import_batch(values, schema)?)
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
            // A null `out` asks for an update without results. DML and ingest report their
            // count with a stream too, since the Python DBAPI runs every `execute` this way.
            let affected = if out.is_null() {
                state.execute_update()?
            } else {
                let (reader, affected) = state.execute_counted()?;
                std::ptr::write_unaligned(out, export_reader(reader));
                affected
            };
            // Queries, DDL and buffered writes report -1 (unknown), which also overwrites any
            // previous count.
            write_rows_affected(rows_affected, affected.unwrap_or(-1));
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

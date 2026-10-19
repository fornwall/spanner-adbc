//! `AdbcConnection*` entry points.

use std::collections::HashSet;
use std::ffi::{c_char, c_int, c_void};

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::{InfoCode, ObjectDepth, OptionConnection};
use adbc_core::{Connection, Database};
use arrow_array::RecordBatchReader;
use arrow_array::ffi::FFI_ArrowSchema;
use arrow_array::ffi_stream::FFI_ArrowArrayStream;

use super::abi::{AdbcConnection, AdbcDatabase, AdbcDriver, AdbcError, AdbcStatusCode};
use super::guard::{
    byte_slice, optional_str, optional_str_list, required_output, required_str, write_out,
};
use super::handle::{Staged, install_cancel_handle};
use super::options::{FfiHandle, driver_of, slot_of, with_state};
use crate::connection::SpannerConnection;
use crate::error::invalid_argument;

pub(super) const KIND: &str = "connection";

pub(super) type State = Staged<OptionConnection, SpannerConnection>;

impl FfiHandle for AdbcConnection {
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

/// # Safety
/// `connection` must be null or point to a valid `AdbcConnection`, and `database` must be null or
/// point to a valid, initialized `AdbcDatabase` that outlives the connection.
pub(super) unsafe extern "C" fn connection_init(
    connection: *mut AdbcConnection,
    database: *mut AdbcDatabase,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    let slot = unsafe { slot_of(connection) };
    unsafe {
        with_state(connection, error, |state| {
            let options = state.pending(KIND)?;
            let mut database = super::options::borrow(database)?;
            let database = database.ready(super::database::KIND)?;
            let ready = database.new_connection_with_opts(options.entries.clone())?;
            // Install cancellation before publishing a state that can start operations.
            install_cancel_handle::<State>(slot, ready.get_cancel_handle());
            *state = State::Ready(ready);
            Ok(())
        })
    }
}

/// Export `reader` as an Arrow C stream carrying the connection's `private_driver` (see
/// [`driver_of`]).
///
/// # Safety
/// `connection` must be null or point to a valid `AdbcConnection`, and `out` must be null or
/// point to a writable `FFI_ArrowArrayStream`.
unsafe fn export_stream(
    connection: *mut AdbcConnection,
    reader: Box<dyn RecordBatchReader + Send>,
    out: *mut FFI_ArrowArrayStream,
) -> Result<()> {
    let stream = super::stream::export_reader(reader, unsafe { driver_of(connection) });
    // On a null `out`, ownership never transferred and the stream is dropped here.
    unsafe { write_out(out, stream, "out") }
}

/// # Safety
/// `connection` must be null or point to a valid `AdbcConnection`.
pub(super) unsafe extern "C" fn connection_commit(
    connection: *mut AdbcConnection,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe { with_state(connection, error, |state| state.ready(KIND)?.commit()) }
}

/// # Safety
/// `connection` must be null or point to a valid `AdbcConnection`.
pub(super) unsafe extern "C" fn connection_rollback(
    connection: *mut AdbcConnection,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe { with_state(connection, error, |state| state.ready(KIND)?.rollback()) }
}

/// # Safety
/// `connection` must be null or point to a valid `AdbcConnection`.
pub(super) unsafe extern "C" fn connection_cancel(
    connection: *mut AdbcConnection,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    // Bypasses `dispatch` so it can interrupt a running call. Before `Init` there is no cancel
    // handle (`InvalidState`); with nothing in flight it returns OK (see `SlotCancelHandle`).
    unsafe { super::handle::cancel::<State>(slot_of(connection), KIND, error) }
}

/// # Safety
/// `connection` and `out` must be null or valid for the call, and `info_codes` must be null or
/// point to `info_codes_length` initialized `u32`s.
pub(super) unsafe extern "C" fn connection_get_info(
    connection: *mut AdbcConnection,
    info_codes: *const u32,
    info_codes_length: usize,
    out: *mut FFI_ArrowArrayStream,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe {
        with_state(connection, error, |state| {
            required_output(out, "out")?;
            // Null requests all supported codes; an empty non-null array requests none.
            let codes = if info_codes.is_null() {
                None
            } else {
                Some(
                    std::slice::from_raw_parts(info_codes, info_codes_length)
                        .iter()
                        .copied()
                        .map(InfoCode::from)
                        .collect::<HashSet<_>>(),
                )
            };
            let reader = state.ready(KIND)?.get_info(codes)?;
            export_stream(connection, reader, out)
        })
    }
}

/// # Safety
/// `connection` and `out` must be null or valid for the call, the string arguments must be null or
/// NUL-terminated, and `table_type` must be null or a NULL-terminated array of such strings.
#[allow(
    clippy::too_many_arguments,
    reason = "the signature is fixed by the ADBC C API"
)]
pub(super) unsafe extern "C" fn connection_get_objects(
    connection: *mut AdbcConnection,
    depth: c_int,
    catalog: *const c_char,
    db_schema: *const c_char,
    table_name: *const c_char,
    table_type: *const *const c_char,
    column_name: *const c_char,
    out: *mut FFI_ArrowArrayStream,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe {
        with_state(connection, error, |state| {
            required_output(out, "out")?;
            // An unknown depth is a caller error, not `try_from`'s `InvalidData`.
            // ADBC_OBJECT_DEPTH_COLUMNS aliases ALL (0), so 4 is not a valid depth.
            let depth = ObjectDepth::try_from(depth).map_err(|_| {
                invalid_argument(format!(
                    "AdbcConnectionGetObjects depth {depth} is not a valid ADBC object depth; \
                     use 0 (all, including columns), 1 (catalogs), 2 (db_schemas), or 3 (tables)"
                ))
            })?;
            let catalog = optional_str(catalog, "catalog")?;
            let db_schema = optional_str(db_schema, "db_schema")?;
            let table_name = optional_str(table_name, "table_name")?;
            let table_type = optional_str_list(table_type, "table_type")?;
            let column_name = optional_str(column_name, "column_name")?;

            let reader = state.ready(KIND)?.get_objects(
                depth,
                catalog,
                db_schema,
                table_name,
                table_type,
                column_name,
            )?;
            export_stream(connection, reader, out)
        })
    }
}

/// # Safety
/// `connection` and `schema` must be null or valid for the call, and the string arguments must be
/// null or NUL-terminated.
pub(super) unsafe extern "C" fn connection_get_table_schema(
    connection: *mut AdbcConnection,
    catalog: *const c_char,
    db_schema: *const c_char,
    table_name: *const c_char,
    schema: *mut FFI_ArrowSchema,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe {
        with_state(connection, error, |state| {
            required_output(schema, "schema")?;
            let catalog = optional_str(catalog, "catalog")?;
            let db_schema = optional_str(db_schema, "db_schema")?;
            // Not a filter as in GetObjects: this names one table.
            let table_name = required_str(table_name, "table_name")?;

            let found = state
                .ready(KIND)?
                .get_table_schema(catalog, db_schema, table_name)?;
            let exported = FFI_ArrowSchema::try_from(&found).map_err(|failure| {
                Error::with_message_and_status(
                    format!(
                        "failed to export the table schema over the C data interface: {failure}"
                    ),
                    Status::Internal,
                )
            })?;
            write_out(schema, exported, "schema")
        })
    }
}

/// # Safety
/// `connection` and `out` must be null or valid for the call.
pub(super) unsafe extern "C" fn connection_get_table_types(
    connection: *mut AdbcConnection,
    out: *mut FFI_ArrowArrayStream,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe {
        with_state(connection, error, |state| {
            required_output(out, "out")?;
            let reader = state.ready(KIND)?.get_table_types()?;
            export_stream(connection, reader, out)
        })
    }
}

/// # Safety
/// `connection` and `out` must be null or valid for the call, and the string arguments must be
/// null or NUL-terminated.
pub(super) unsafe extern "C" fn connection_get_statistics(
    connection: *mut AdbcConnection,
    catalog: *const c_char,
    db_schema: *const c_char,
    table_name: *const c_char,
    approximate: c_char,
    out: *mut FFI_ArrowArrayStream,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe {
        with_state(connection, error, |state| {
            required_output(out, "out")?;
            let catalog = optional_str(catalog, "catalog")?;
            let db_schema = optional_str(db_schema, "db_schema")?;
            let table_name = optional_str(table_name, "table_name")?;

            let reader = state.ready(KIND)?.get_statistics(
                catalog,
                db_schema,
                table_name,
                // The header declares a C boolean as `char`; anything non-zero is true.
                approximate != 0,
            )?;
            export_stream(connection, reader, out)
        })
    }
}

/// # Safety
/// `connection` and `out` must be null or valid for the call.
pub(super) unsafe extern "C" fn connection_get_statistic_names(
    connection: *mut AdbcConnection,
    out: *mut FFI_ArrowArrayStream,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe {
        with_state(connection, error, |state| {
            required_output(out, "out")?;
            let reader = state.ready(KIND)?.get_statistic_names()?;
            export_stream(connection, reader, out)
        })
    }
}

/// # Safety
/// `connection` and `out` must be null or valid for the call, and `serialized_partition` must be
/// null or point to `serialized_length` initialized bytes.
pub(super) unsafe extern "C" fn connection_read_partition(
    connection: *mut AdbcConnection,
    serialized_partition: *const u8,
    serialized_length: usize,
    out: *mut FFI_ArrowArrayStream,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe {
        with_state(connection, error, |state| {
            required_output(out, "out")?;
            let partition = byte_slice(
                serialized_partition,
                serialized_length,
                "serialized_partition",
            )?;
            let reader = state.ready(KIND)?.read_partition(partition)?;
            export_stream(connection, reader, out)
        })
    }
}

#[cfg(test)]
mod tests;

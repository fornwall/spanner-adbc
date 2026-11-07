//! The ADBC C ABI, transcribed from `arrow-adbc/c/include/arrow-adbc/adbc.h`.
//!
//! Declarations only, so the file can be compared with the header. Field order in [`AdbcDriver`]
//! is the vtable layout: never reorder it, and append only at the end, as upstream does.

#![allow(
    non_snake_case,
    reason = "field and function names mirror the C header verbatim"
)]

use std::os::raw::{c_char, c_int, c_void};

pub(crate) use adbc_core::constants::ADBC_STATUS_OK;
#[cfg(test)]
pub(crate) use adbc_core::constants::{
    ADBC_STATUS_ALREADY_EXISTS, ADBC_STATUS_CANCELLED, ADBC_STATUS_INTEGRITY, ADBC_STATUS_INTERNAL,
    ADBC_STATUS_INVALID_ARGUMENT, ADBC_STATUS_INVALID_DATA, ADBC_STATUS_INVALID_STATE,
    ADBC_STATUS_IO, ADBC_STATUS_NOT_FOUND, ADBC_STATUS_NOT_IMPLEMENTED, ADBC_STATUS_TIMEOUT,
    ADBC_STATUS_UNAUTHENTICATED, ADBC_STATUS_UNAUTHORIZED, ADBC_STATUS_UNKNOWN,
};
pub(crate) use adbc_core::error::AdbcStatusCode;
use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::ffi_stream::FFI_ArrowArrayStream;

pub(crate) const ADBC_VERSION_1_0_0: c_int = 1_000_000;
pub(crate) const ADBC_VERSION_1_1_0: c_int = 1_001_000;

/// [`AdbcError::vendor_code`] value marking the struct as 1.1.0-sized. Only then do the 1.1.0
/// fields exist.
pub(crate) const ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA: i32 = i32::MIN;

#[repr(C)]
pub(crate) struct AdbcError {
    pub(crate) message: *mut c_char,
    pub(crate) vendor_code: i32,
    pub(crate) sqlstate: [c_char; 5],
    pub(crate) release: Option<unsafe extern "C" fn(*mut Self)>,
    /// Added in ADBC 1.1.0.
    pub(crate) private_data: *mut c_void,
    /// Added in ADBC 1.1.0.
    pub(crate) private_driver: *const AdbcDriver,
}

#[repr(C)]
pub(crate) struct AdbcErrorDetail {
    pub(crate) key: *const c_char,
    pub(crate) value: *const u8,
    pub(crate) value_length: usize,
}

#[repr(C)]
pub(crate) struct AdbcDatabase {
    pub(crate) private_data: *mut c_void,
    pub(crate) private_driver: *const AdbcDriver,
}

#[repr(C)]
pub(crate) struct AdbcConnection {
    pub(crate) private_data: *mut c_void,
    pub(crate) private_driver: *const AdbcDriver,
}

#[repr(C)]
pub(crate) struct AdbcStatement {
    pub(crate) private_data: *mut c_void,
    pub(crate) private_driver: *const AdbcDriver,
}

#[repr(C)]
pub(crate) struct AdbcPartitions {
    pub(crate) num_partitions: usize,
    pub(crate) partitions: *mut *const u8,
    pub(crate) partition_lengths: *mut usize,
    pub(crate) private_data: *mut c_void,
    pub(crate) release: Option<unsafe extern "C" fn(*mut Self)>,
}

/// An `AdbcStatusCode (*)(args..., struct AdbcError*)` field of [`AdbcDriver`].
macro_rules! slot {
    ($($arg:ty),+) => {
        Option<unsafe extern "C" fn($($arg,)+ *mut AdbcError) -> AdbcStatusCode>
    };
}

/// The driver vtable. A `None` slot means "not implemented".
#[repr(C)]
pub(crate) struct AdbcDriver {
    pub(crate) private_data: *mut c_void,
    pub(crate) private_manager: *const c_void,
    pub(crate) release: slot!(*mut Self),

    // --- ADBC 1.0.0 ---
    pub(crate) DatabaseInit: slot!(*mut AdbcDatabase),
    pub(crate) DatabaseNew: slot!(*mut AdbcDatabase),
    pub(crate) DatabaseSetOption: slot!(*mut AdbcDatabase, *const c_char, *const c_char),
    pub(crate) DatabaseRelease: slot!(*mut AdbcDatabase),
    pub(crate) ConnectionCommit: slot!(*mut AdbcConnection),
    pub(crate) ConnectionGetInfo: slot!(
        *mut AdbcConnection,
        *const u32,
        usize,
        *mut FFI_ArrowArrayStream
    ),
    pub(crate) ConnectionGetObjects: slot!(
        *mut AdbcConnection,
        c_int,
        *const c_char,
        *const c_char,
        *const c_char,
        *const *const c_char,
        *const c_char,
        *mut FFI_ArrowArrayStream
    ),
    pub(crate) ConnectionGetTableSchema: slot!(
        *mut AdbcConnection,
        *const c_char,
        *const c_char,
        *const c_char,
        *mut FFI_ArrowSchema
    ),
    pub(crate) ConnectionGetTableTypes: slot!(*mut AdbcConnection, *mut FFI_ArrowArrayStream),
    pub(crate) ConnectionInit: slot!(*mut AdbcConnection, *mut AdbcDatabase),
    pub(crate) ConnectionNew: slot!(*mut AdbcConnection),
    pub(crate) ConnectionSetOption: slot!(*mut AdbcConnection, *const c_char, *const c_char),
    pub(crate) ConnectionReadPartition: slot!(
        *mut AdbcConnection,
        *const u8,
        usize,
        *mut FFI_ArrowArrayStream
    ),
    pub(crate) ConnectionRelease: slot!(*mut AdbcConnection),
    pub(crate) ConnectionRollback: slot!(*mut AdbcConnection),
    pub(crate) StatementBind: slot!(
        *mut AdbcStatement,
        *mut FFI_ArrowArray,
        *mut FFI_ArrowSchema
    ),
    pub(crate) StatementBindStream: slot!(*mut AdbcStatement, *mut FFI_ArrowArrayStream),
    pub(crate) StatementExecuteQuery:
        slot!(*mut AdbcStatement, *mut FFI_ArrowArrayStream, *mut i64),
    pub(crate) StatementExecutePartitions: slot!(
        *mut AdbcStatement,
        *mut FFI_ArrowSchema,
        *mut AdbcPartitions,
        *mut i64
    ),
    pub(crate) StatementGetParameterSchema: slot!(*mut AdbcStatement, *mut FFI_ArrowSchema),
    pub(crate) StatementNew: slot!(*mut AdbcConnection, *mut AdbcStatement),
    pub(crate) StatementPrepare: slot!(*mut AdbcStatement),
    pub(crate) StatementRelease: slot!(*mut AdbcStatement),
    pub(crate) StatementSetOption: slot!(*mut AdbcStatement, *const c_char, *const c_char),
    pub(crate) StatementSetSqlQuery: slot!(*mut AdbcStatement, *const c_char),
    pub(crate) StatementSetSubstraitPlan: slot!(*mut AdbcStatement, *const u8, usize),

    // --- ADBC 1.1.0 ---
    pub(crate) ErrorGetDetailCount: Option<unsafe extern "C" fn(*const AdbcError) -> c_int>,
    pub(crate) ErrorGetDetail:
        Option<unsafe extern "C" fn(*const AdbcError, c_int) -> AdbcErrorDetail>,
    pub(crate) ErrorFromArrayStream: Option<
        unsafe extern "C" fn(*mut FFI_ArrowArrayStream, *mut AdbcStatusCode) -> *const AdbcError,
    >,
    pub(crate) DatabaseGetOption: slot!(*mut AdbcDatabase, *const c_char, *mut c_char, *mut usize),
    pub(crate) DatabaseGetOptionBytes: slot!(*mut AdbcDatabase, *const c_char, *mut u8, *mut usize),
    pub(crate) DatabaseGetOptionDouble: slot!(*mut AdbcDatabase, *const c_char, *mut f64),
    pub(crate) DatabaseGetOptionInt: slot!(*mut AdbcDatabase, *const c_char, *mut i64),
    pub(crate) DatabaseSetOptionBytes: slot!(*mut AdbcDatabase, *const c_char, *const u8, usize),
    pub(crate) DatabaseSetOptionDouble: slot!(*mut AdbcDatabase, *const c_char, f64),
    pub(crate) DatabaseSetOptionInt: slot!(*mut AdbcDatabase, *const c_char, i64),
    pub(crate) ConnectionCancel: slot!(*mut AdbcConnection),
    pub(crate) ConnectionGetOption:
        slot!(*mut AdbcConnection, *const c_char, *mut c_char, *mut usize),
    pub(crate) ConnectionGetOptionBytes:
        slot!(*mut AdbcConnection, *const c_char, *mut u8, *mut usize),
    pub(crate) ConnectionGetOptionDouble: slot!(*mut AdbcConnection, *const c_char, *mut f64),
    pub(crate) ConnectionGetOptionInt: slot!(*mut AdbcConnection, *const c_char, *mut i64),
    pub(crate) ConnectionGetStatistics: slot!(
        *mut AdbcConnection,
        *const c_char,
        *const c_char,
        *const c_char,
        c_char,
        *mut FFI_ArrowArrayStream
    ),
    pub(crate) ConnectionGetStatisticNames: slot!(*mut AdbcConnection, *mut FFI_ArrowArrayStream),
    pub(crate) ConnectionSetOptionBytes:
        slot!(*mut AdbcConnection, *const c_char, *const u8, usize),
    pub(crate) ConnectionSetOptionDouble: slot!(*mut AdbcConnection, *const c_char, f64),
    pub(crate) ConnectionSetOptionInt: slot!(*mut AdbcConnection, *const c_char, i64),
    pub(crate) StatementCancel: slot!(*mut AdbcStatement),
    pub(crate) StatementExecuteSchema: slot!(*mut AdbcStatement, *mut FFI_ArrowSchema),
    pub(crate) StatementGetOption:
        slot!(*mut AdbcStatement, *const c_char, *mut c_char, *mut usize),
    pub(crate) StatementGetOptionBytes:
        slot!(*mut AdbcStatement, *const c_char, *mut u8, *mut usize),
    pub(crate) StatementGetOptionDouble: slot!(*mut AdbcStatement, *const c_char, *mut f64),
    pub(crate) StatementGetOptionInt: slot!(*mut AdbcStatement, *const c_char, *mut i64),
    pub(crate) StatementSetOptionBytes: slot!(*mut AdbcStatement, *const c_char, *const u8, usize),
    pub(crate) StatementSetOptionDouble: slot!(*mut AdbcStatement, *const c_char, f64),
    pub(crate) StatementSetOptionInt: slot!(*mut AdbcStatement, *const c_char, i64),
}

/// `offsetof(struct AdbcDriver, ErrorGetDetailCount)`: the size of a 1.0.0 caller's allocation.
pub(crate) const ADBC_DRIVER_1_0_0_SIZE: usize =
    std::mem::offset_of!(AdbcDriver, ErrorGetDetailCount);

#[cfg(test)]
mod tests;

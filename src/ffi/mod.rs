//! The ADBC C ABI.
//!
//! The shared library exports [`AdbcSpannerInit`] and [`AdbcDriverInit`], the fallback a driver
//! manager tries when given no entrypoint name. Both fill in an ADBC 1.0.0 or 1.1.0 vtable.
//!
//! * [`abi`]: the C header, transcribed. Declarations only.
//! * [`error`]: writes driver errors into the caller's `AdbcError` at the revision it allocated.
//! * [`guard`]: panic containment and conversion of pointer and string arguments.
//! * [`handle`]: the object behind each handle's `private_data`, and how calls reach it.
//! * [`options`]: `New`, `Release` and the option entry points, written once as generic
//!   `extern "C"` functions that the vtable instantiates per object.
//! * [`database`], [`connection`], [`statement`]: the remaining entry points, each a thin
//!   translation onto the driver's own types.
//! * [`stream`], [`import`]: Arrow C stream export and import.
//!
//! Each handle's `private_data` holds an `Arc` to its driver object behind a mutex, so calls on
//! one handle serialize and a panic poisons only the objects its call locked. Entry points contain
//! panics with [`guard::catch`] and report errors through the caller's `AdbcError`. Exported
//! streams own their readers and stay valid after the handles that produced them are released.

mod abi;
mod connection;
mod database;
mod error;
mod guard;
mod handle;
mod import;
mod options;
#[cfg(test)]
mod roundtrip;
mod statement;
mod stream;
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

use std::ffi::{c_int, c_void};
use std::sync::Once;

use adbc_core::error::{Error, Status};

use abi::{
    ADBC_DRIVER_1_0_0_SIZE, ADBC_STATUS_OK, ADBC_VERSION_1_0_0, ADBC_VERSION_1_1_0, AdbcConnection,
    AdbcDatabase, AdbcDriver, AdbcError, AdbcStatement, AdbcStatusCode,
};
use handle::finish;

/// Every slot is populated; unsupported operations report `NOT_IMPLEMENTED` from their callback.
fn vtable() -> AdbcDriver {
    AdbcDriver {
        private_data: std::ptr::null_mut(),
        private_manager: std::ptr::null(),
        release: Some(release_driver),

        // --- ADBC 1.0.0 ---
        DatabaseInit: Some(database::database_init),
        DatabaseNew: Some(options::new_handle::<AdbcDatabase>),
        DatabaseSetOption: Some(options::set_option_string::<AdbcDatabase>),
        DatabaseRelease: Some(options::release_handle::<AdbcDatabase>),
        ConnectionCommit: Some(connection::connection_commit),
        ConnectionGetInfo: Some(connection::connection_get_info),
        ConnectionGetObjects: Some(connection::connection_get_objects),
        ConnectionGetTableSchema: Some(connection::connection_get_table_schema),
        ConnectionGetTableTypes: Some(connection::connection_get_table_types),
        ConnectionInit: Some(connection::connection_init),
        ConnectionNew: Some(options::new_handle::<AdbcConnection>),
        ConnectionSetOption: Some(options::set_option_string::<AdbcConnection>),
        ConnectionReadPartition: Some(connection::connection_read_partition),
        ConnectionRelease: Some(options::release_handle::<AdbcConnection>),
        ConnectionRollback: Some(connection::connection_rollback),
        StatementBind: Some(statement::statement_bind),
        StatementBindStream: Some(statement::statement_bind_stream),
        StatementExecuteQuery: Some(statement::statement_execute_query),
        StatementExecutePartitions: Some(statement::statement_execute_partitions),
        StatementGetParameterSchema: Some(statement::statement_get_parameter_schema),
        StatementNew: Some(statement::statement_new),
        StatementPrepare: Some(statement::statement_prepare),
        StatementRelease: Some(options::release_handle::<AdbcStatement>),
        StatementSetOption: Some(options::set_option_string::<AdbcStatement>),
        StatementSetSqlQuery: Some(statement::statement_set_sql_query),
        StatementSetSubstraitPlan: Some(statement::statement_set_substrait_plan),

        // --- ADBC 1.1.0 ---
        ErrorGetDetailCount: Some(error::error_get_detail_count),
        ErrorGetDetail: Some(error::error_get_detail),
        ErrorFromArrayStream: Some(stream::error_from_array_stream),
        DatabaseGetOption: Some(options::get_option_string::<AdbcDatabase>),
        DatabaseGetOptionBytes: Some(options::get_option_bytes::<AdbcDatabase>),
        DatabaseGetOptionDouble: Some(options::get_option_double::<AdbcDatabase>),
        DatabaseGetOptionInt: Some(options::get_option_int::<AdbcDatabase>),
        DatabaseSetOptionBytes: Some(options::set_option_bytes::<AdbcDatabase>),
        DatabaseSetOptionDouble: Some(options::set_option_double::<AdbcDatabase>),
        DatabaseSetOptionInt: Some(options::set_option_int::<AdbcDatabase>),
        ConnectionCancel: Some(connection::connection_cancel),
        ConnectionGetOption: Some(options::get_option_string::<AdbcConnection>),
        ConnectionGetOptionBytes: Some(options::get_option_bytes::<AdbcConnection>),
        ConnectionGetOptionDouble: Some(options::get_option_double::<AdbcConnection>),
        ConnectionGetOptionInt: Some(options::get_option_int::<AdbcConnection>),
        ConnectionGetStatistics: Some(connection::connection_get_statistics),
        ConnectionGetStatisticNames: Some(connection::connection_get_statistic_names),
        ConnectionSetOptionBytes: Some(options::set_option_bytes::<AdbcConnection>),
        ConnectionSetOptionDouble: Some(options::set_option_double::<AdbcConnection>),
        ConnectionSetOptionInt: Some(options::set_option_int::<AdbcConnection>),
        StatementCancel: Some(statement::statement_cancel),
        StatementExecuteSchema: Some(statement::statement_execute_schema),
        StatementGetOption: Some(options::get_option_string::<AdbcStatement>),
        StatementGetOptionBytes: Some(options::get_option_bytes::<AdbcStatement>),
        StatementGetOptionDouble: Some(options::get_option_double::<AdbcStatement>),
        StatementGetOptionInt: Some(options::get_option_int::<AdbcStatement>),
        StatementSetOptionBytes: Some(options::set_option_bytes::<AdbcStatement>),
        StatementSetOptionDouble: Some(options::set_option_double::<AdbcStatement>),
        StatementSetOptionInt: Some(options::set_option_int::<AdbcStatement>),
    }
}

/// # Safety
/// `driver` must be null or point to a vtable this driver initialized, at either revision.
// Guard-exempt: only pointer operations on the caller's struct.
unsafe extern "C" fn release_driver(
    driver: *mut AdbcDriver,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    if driver.is_null() {
        return ADBC_STATUS_OK;
    }
    // A field place rather than `&mut AdbcDriver`, which would span the 1.1.0 tail that a 1.0.0
    // caller did not allocate.
    if unsafe { (*driver).release.take() }.is_none() {
        let failure =
            Error::with_message_and_status("driver is already released", Status::InvalidState);
        return unsafe { finish(Err(failure), error) };
    }
    ADBC_STATUS_OK
}

/// Populate the caller's vtable at the revision it asked for.
///
/// # Safety
/// `driver` must point to an `AdbcDriver` allocation of at least the size implied by `version`.
// Guard-exempt, as are the two exported wrappers: builds and copies a table of function pointers.
unsafe fn init(version: c_int, driver: *mut c_void, error: *mut AdbcError) -> AdbcStatusCode {
    if driver.is_null() {
        let failure = crate::error::invalid_argument("driver must not be null");
        return unsafe { finish(Err(failure), error) };
    }
    let size = match version {
        ADBC_VERSION_1_1_0 => size_of::<AdbcDriver>(),
        // Truncating leaks nothing: the 1.1.0 tail holds only function pointers.
        ADBC_VERSION_1_0_0 => ADBC_DRIVER_1_0_0_SIZE,
        _ => {
            // adbc.h tells callers to retry with an older version on NOT_IMPLEMENTED.
            let failure = Error::with_message_and_status(
                format!(
                    "unsupported ADBC version {version}; this driver implements \
                     {ADBC_VERSION_1_0_0} and {ADBC_VERSION_1_1_0}"
                ),
                Status::NotImplemented,
            );
            return unsafe { finish(Err(failure), error) };
        }
    };
    pin_library();
    let table = vtable();
    unsafe {
        std::ptr::copy_nonoverlapping((&raw const table).cast::<u8>(), driver.cast::<u8>(), size);
    }
    ADBC_STATUS_OK
}

/// Keep this library loaded until the process exits. The shared runtime's worker threads, and
/// streams or errors that outlive the driver, still run its code after a driver manager would
/// unload it. On failure the library stays unloadable, as without the pin.
fn pin_library() {
    static PINNED: Once = Once::new();
    PINNED.call_once(|| unsafe { pin_module_containing((&raw const PINNED).cast()) });
}

/// # Safety
/// `address` must lie inside this library.
#[cfg(unix)]
unsafe fn pin_module_containing(address: *const c_void) {
    let mut info: libc::Dl_info = unsafe { std::mem::zeroed() };
    if unsafe { libc::dladdr(address, &raw mut info) } != 0 && !info.dli_fname.is_null() {
        // The handle is deliberately never closed.
        let flags = libc::RTLD_NOW | libc::RTLD_NOLOAD | libc::RTLD_NODELETE;
        unsafe { libc::dlopen(info.dli_fname, flags) };
    }
}

/// # Safety
/// `address` must lie inside this library.
#[cfg(windows)]
unsafe fn pin_module_containing(address: *const c_void) {
    const GET_MODULE_HANDLE_EX_FLAG_PIN: u32 = 0x1;
    const GET_MODULE_HANDLE_EX_FLAG_FROM_ADDRESS: u32 = 0x4;
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetModuleHandleExW(flags: u32, name: *const u16, module: *mut *mut c_void) -> i32;
    }
    let mut module = std::ptr::null_mut();
    let flags = GET_MODULE_HANDLE_EX_FLAG_FROM_ADDRESS | GET_MODULE_HANDLE_EX_FLAG_PIN;
    unsafe { GetModuleHandleExW(flags, address.cast(), &raw mut module) };
}

/// The driver's own entry point, for callers that name it.
///
/// # Safety
/// See [`init`].
#[unsafe(no_mangle)]
pub(crate) unsafe extern "C" fn AdbcSpannerInit(
    version: c_int,
    driver: *mut c_void,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe { init(version, driver, error) }
}

/// The generic entry point, for driver managers given no symbol name.
///
/// # Safety
/// See [`init`].
#[unsafe(no_mangle)]
pub(crate) unsafe extern "C" fn AdbcDriverInit(
    version: c_int,
    driver: *mut c_void,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe { init(version, driver, error) }
}

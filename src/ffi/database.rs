//! `AdbcDatabase*` entry points.

use std::ffi::c_void;
use std::sync::{Mutex, OnceLock};

use adbc_core::Driver;
use adbc_core::error::{Error, Result, Status};
use adbc_core::options::OptionDatabase;

use super::abi::{AdbcDatabase, AdbcDriver, AdbcError, AdbcStatusCode};
use super::handle::Staged;
use super::options::{FfiHandle, with_state};
use crate::driver::{SpannerDatabase, SpannerDriver};

pub(super) const KIND: &str = "database";

pub(super) type State = Staged<OptionDatabase, SpannerDatabase>;

impl FfiHandle for AdbcDatabase {
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

/// One driver, and so one Tokio runtime, shared by every database in the process. A failure to
/// create it leaves the cell empty, so a later `Init` can retry.
fn shared_driver() -> Result<&'static Mutex<SpannerDriver>> {
    static DRIVER: OnceLock<Mutex<SpannerDriver>> = OnceLock::new();

    if let Some(driver) = DRIVER.get() {
        return Ok(driver);
    }
    // Built outside `get_or_init` so errors can be returned. Racing first callers may build extra
    // runtimes, which are dropped.
    let driver = SpannerDriver::try_new()?;
    Ok(DRIVER.get_or_init(|| Mutex::new(driver)))
}

/// # Safety
/// `database` must be null or point to a valid `AdbcDatabase`.
pub(super) unsafe extern "C" fn database_init(
    database: *mut AdbcDatabase,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe {
        with_state(database, error, |state| {
            let options = state.pending(KIND)?;
            let driver = shared_driver()?;
            let mut driver = driver.lock().map_err(|_| {
                Error::with_message_and_status(
                    "the shared Spanner driver is unusable after an earlier panic",
                    Status::Internal,
                )
            })?;
            let database = driver.new_database_with_opts(options.entries.clone())?;
            *state = State::Ready(database);
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests;

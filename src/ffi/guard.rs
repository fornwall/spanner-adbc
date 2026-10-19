//! Panic containment and the small pointer/string conversions every entry point needs.
//!
//! A Rust panic escaping an `extern "C"` function aborts the process. Entry points that can run
//! driver code use [`catch`]; [`super::handle`] poisons the states locked by a panicking call.
//! Entry points without panic containment say why they cannot panic in a `Guard-exempt:` comment.

use std::any::Any;
use std::ffi::{CStr, CString, c_char, c_void};
use std::panic::AssertUnwindSafe;

use adbc_core::error::{Error, Result, Status};

use crate::error::invalid_argument;

/// Run `f`, turning a panic into an `Internal` error.
pub(crate) fn catch<T>(f: impl FnOnce() -> Result<T>) -> Result<T> {
    std::panic::catch_unwind(AssertUnwindSafe(f)).unwrap_or_else(|cause| Err(caught_panic(cause)))
}

// Outlined so the instantiations of `catch` share one copy.
#[cold]
#[inline(never)]
fn caught_panic(cause: Box<dyn Any + Send>) -> Error {
    let failure = crate::error::err(
        format!(
            "Uncaught panic in the Spanner driver: {}",
            panic_message(cause.as_ref())
        ),
        Status::Internal,
    );
    drop_panic_payload(cause);
    failure
}

/// Drop a panic payload, whose destructor may itself panic. A second payload is leaked, since
/// dropping it could panic again indefinitely.
#[cold]
#[inline(never)]
pub(crate) fn drop_panic_payload(cause: Box<dyn Any + Send>) {
    if let Err(nested) = std::panic::catch_unwind(AssertUnwindSafe(|| drop(cause))) {
        std::mem::forget(nested);
    }
}

/// Extract a string panic payload for entry-point and stream diagnostics.
pub(crate) fn panic_message(cause: &(dyn Any + Send)) -> &str {
    cause
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| cause.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("unknown panic payload")
}

/// Convert to a C string, replacing interior NULs with `?` so the text after them survives.
pub(crate) fn sanitized_cstring(text: &str) -> CString {
    CString::new(text.replace('\0', "?")).unwrap_or_default()
}

/// Borrow a required C string argument.
///
/// # Safety
/// `ptr` must be null or point to a NUL-terminated string that outlives `'a`.
pub(crate) unsafe fn required_str<'a>(ptr: *const c_char, name: &str) -> Result<&'a str> {
    if ptr.is_null() {
        return Err(missing_argument(name));
    }
    unsafe { CStr::from_ptr(ptr) }
        .to_str()
        .map_err(|error| invalid_argument(format!("{name} must be valid UTF-8: {error}")))
}

/// Borrow an optional C string argument, where null means "not specified".
///
/// # Safety
/// `ptr` must be null or point to a NUL-terminated string that outlives `'a`.
pub(crate) unsafe fn optional_str<'a>(ptr: *const c_char, name: &str) -> Result<Option<&'a str>> {
    if ptr.is_null() {
        return Ok(None);
    }
    unsafe { required_str(ptr, name) }.map(Some)
}

/// Borrow a NULL-terminated array of C strings, as `AdbcConnectionGetObjects` takes for
/// `table_types`.
///
/// # Safety
/// `ptr` must be null or point to a NULL-terminated array of NUL-terminated strings, all of which
/// outlive `'a`.
pub(crate) unsafe fn optional_str_list<'a>(
    ptr: *const *const c_char,
    name: &str,
) -> Result<Option<Vec<&'a str>>> {
    if ptr.is_null() {
        return Ok(None);
    }
    let mut items = Vec::new();
    let mut cursor = ptr;
    loop {
        let entry = unsafe { *cursor };
        if entry.is_null() {
            break;
        }
        items.push(unsafe { required_str(entry, name) }?);
        cursor = unsafe { cursor.add(1) };
    }
    Ok(Some(items))
}

/// Borrow a pointer/length byte argument. A null pointer is accepted only with a zero length.
///
/// # Safety
/// `ptr` must be null or point to `len` initialized bytes that outlive `'a`.
pub(crate) unsafe fn byte_slice<'a>(ptr: *const u8, len: usize, name: &str) -> Result<&'a [u8]> {
    if ptr.is_null() {
        if len == 0 {
            return Ok(&[]);
        }
        return Err(invalid_argument(format!(
            "{name} must not be null for a length of {len}"
        )));
    }
    Ok(unsafe { std::slice::from_raw_parts(ptr, len) })
}

/// Validate a GetOption buffer. A null buffer is valid only as a zero-length size probe:
/// success with sufficient capacity tells the caller the value was written.
///
/// # Safety
/// `length` must be null or point to a readable, writable `usize`.
unsafe fn available_space(dst: *const c_void, length: *mut usize) -> Result<usize> {
    if length.is_null() {
        return Err(missing_argument("length"));
    }
    let available = unsafe { *length };
    if dst.is_null() && available > 0 {
        return Err(invalid_argument(format!(
            "value must not be null for a buffer length of {available}; pass a length of 0 to ask \
             only for the size required"
        )));
    }
    Ok(available)
}

/// Write a string following the ADBC GetOption protocol: `length` is in/out and always set to the
/// space required, including the NUL. The buffer is written only if it is big enough; otherwise
/// the caller retries with the reported size.
///
/// # Safety
/// `length` must be non-null, and `dst` null or point to at least `*length` writable bytes.
pub(crate) unsafe fn write_string(value: &str, dst: *mut c_char, length: *mut usize) -> Result<()> {
    let available = unsafe { available_space(dst.cast::<c_void>(), length) }?;
    let bytes = value.as_bytes();
    if let Some(position) = bytes.iter().position(|&byte| byte == 0) {
        return Err(Error::with_message_and_status(
            format!("option value contains an interior NUL at byte {position}"),
            Status::Internal,
        ));
    }
    let required = bytes.len() + 1;
    // A buffer with room for the terminator is non-null after `available_space`.
    if required <= available {
        unsafe {
            std::ptr::copy_nonoverlapping(bytes.as_ptr().cast::<c_char>(), dst, bytes.len());
            dst.add(bytes.len()).write(0);
        }
    }
    unsafe { *length = required };
    Ok(())
}

/// Like [`write_string`], without a NUL terminator.
///
/// # Safety
/// `length` must be non-null, and `dst` null or point to at least `*length` writable bytes.
pub(crate) unsafe fn write_bytes(value: &[u8], dst: *mut u8, length: *mut usize) -> Result<()> {
    let available = unsafe { available_space(dst.cast::<c_void>(), length) }?;
    // `copy_nonoverlapping` requires a non-null destination even for zero bytes.
    if !dst.is_null() && value.len() <= available {
        unsafe { std::ptr::copy_nonoverlapping(value.as_ptr(), dst, value.len()) };
    }
    unsafe { *length = value.len() };
    Ok(())
}

/// Reject a missing output before performing work that the caller cannot collect.
pub(crate) fn required_output<T>(dst: *mut T, name: &str) -> Result<()> {
    if dst.is_null() {
        return Err(missing_argument(name));
    }
    Ok(())
}

// Outlined from the generic callers.
#[cold]
#[inline(never)]
pub(crate) fn missing_argument(name: &str) -> Error {
    invalid_argument(format!("{name} must not be null"))
}

/// Write a value into a caller-provided out parameter, rejecting a null pointer.
///
/// # Safety
/// `dst` must be null or point to a writable, correctly aligned `T`.
pub(crate) unsafe fn write_out<T>(dst: *mut T, value: T, name: &str) -> Result<()> {
    required_output(dst, name)?;
    unsafe { std::ptr::write_unaligned(dst, value) };
    Ok(())
}

#[cfg(test)]
mod tests;

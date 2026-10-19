//! Helpers for producing [`adbc_core`] errors and translating Spanner client errors.

use adbc_core::error::{Error, Status};
use google_cloud_gax::error::rpc::{Code, StatusDetails};
use google_cloud_wkt::Any;

pub(crate) fn err(message: impl Into<String>, status: Status) -> Error {
    Error::with_message_and_status(message, status)
}

/// A `NotImplemented` error for a noun phrase, such as `not_implemented("ingest mode \"upsert\"")`.
/// Use [`unsupported`] for a complete sentence.
pub(crate) fn not_implemented(what: &str) -> Error {
    err(
        format!("{what} is not supported by the Spanner ADBC driver"),
        Status::NotImplemented,
    )
}

/// A `NotImplemented` error with `message` as is.
pub(crate) fn unsupported(message: impl Into<String>) -> Error {
    err(message, Status::NotImplemented)
}

/// The error for setting an unknown option on a `"database"`, `"connection"` or `"statement"`.
pub(crate) fn unknown_option(object_kind: &str, key: &str) -> Error {
    unsupported(format!("unsupported Spanner {object_kind} option: {key}"))
}

/// The error for getting an option that is unset or unknown. Getters cannot tell the two apart,
/// so the message names both.
pub(crate) fn option_not_set(key: &str) -> Error {
    not_found(format!(
        "option {key} is not set or not recognized by this driver"
    ))
}

/// Rewrite an error's message, keeping its status, `vendor_code` and details.
pub(crate) fn annotate(mut error: Error, f: impl FnOnce(&str) -> String) -> Error {
    error.message = f(&error.message);
    error
}

/// An `InvalidState` error, for operations called out of order.
pub(crate) fn invalid_state(message: impl Into<String>) -> Error {
    err(message, Status::InvalidState)
}

pub(crate) fn invalid_argument(message: impl Into<String>) -> Error {
    err(message, Status::InvalidArguments)
}

pub(crate) fn not_found(message: impl Into<String>) -> Error {
    err(message, Status::NotFound)
}

/// Walk an error and everything it wraps, outermost first.
///
/// Readers can only report [`ArrowError`](arrow_schema::ArrowError)s, so the driver boxes its
/// [`Error`] in `ArrowError::ExternalError`. The C stream export uses this to recover the ADBC
/// status, for example to report `ECANCELED` for a cancelled read.
#[cfg(feature = "ffi")]
pub(crate) fn chain<'a>(
    source: &'a (dyn std::error::Error + 'static),
) -> impl Iterator<Item = &'a (dyn std::error::Error + 'static)> {
    std::iter::successors(Some(source), |error| error.source())
}

/// Translate a Spanner client error into an ADBC error.
///
/// A gRPC status maps to the closest [`Status`], and its numeric code is kept in `vendor_code` so
/// callers can tell apart codes that share a status (for example `ABORTED` = 10). Errors without
/// a status become [`Status::Internal`] with `vendor_code` 0. The C ABI's 1.1.0 error layout
/// reserves `vendor_code`, so `src/ffi/error.rs` passes a non-zero code there as the detail
/// `adbc.spanner.vendor_code` instead.
///
/// # Structured error details
///
/// Each `google.rpc.Status` detail (such as `ErrorInfo`, `BadRequest` or `RetryInfo`) becomes a
/// `(key, value)` entry in `details`:
///
/// - key: the lowercased protobuf type name, e.g. `google.rpc.retryinfo`. There is no `-bin`
///   suffix because the value is text.
/// - value: the detail's ProtoJSON encoding, which names its type in `"@type"`, e.g.
///   `{"@type":"type.googleapis.com/google.rpc.RetryInfo","retryDelay":"0.010s"}`.
///
/// This differs from the Flight SQL driver, which sends the whole status as one binary
/// `grpc-status-details-bin` detail; the client only offers a ProtoJSON encoding of details.
/// `RetryInfo` on `ABORTED` is rare because the client's transaction runner consumes it. Without
/// details, `details` is `None`, never `Some(vec![])`.
pub(crate) fn from_spanner(error: google_cloud_spanner::Error) -> Error {
    // The emulator's admin REST API returns a bare google.rpc.Status, while gax expects an
    // {"error": ...} envelope.
    let http_status = error
        .http_payload()
        .filter(|_| error.status().is_none())
        .and_then(|payload| {
            let value: serde_json::Value = serde_json::from_slice(payload).ok()?;
            let code = value.get("code")?.as_i64()?;
            if !(1..=16).contains(&code) || !value.get("message")?.is_string() {
                return None;
            }
            serde_json::from_value::<google_cloud_gax::error::rpc::Status>(value).ok()
        });
    let (status, vendor_code, details, code) = error.status().or(http_status.as_ref()).map_or(
        (Status::Internal, 0, None, None),
        |status| {
            (
                status_for_grpc_code(status.code),
                status.code as i32,
                details_for_adbc(&status.details),
                Some(status.code),
            )
        },
    );
    let mut message = format!("Spanner error: {error}");
    if code == Some(Code::PermissionDenied) {
        message.push_str(PERMISSION_DENIED_GUIDANCE);
    }
    let mut adbc = err(message, status);
    adbc.vendor_code = vendor_code;
    adbc.details = details;
    adbc
}

/// Appended to `PERMISSION_DENIED` messages, which already name the missing permission. It names
/// no role, since suggesting one risks steering callers to an over-broad grant.
const PERMISSION_DENIED_GUIDANCE: &str =
    "; grant an IAM role that includes it — see https://cloud.google.com/spanner/docs/iam";

/// Map status details as described on [`from_spanner`]. Details that cannot be mapped are
/// skipped rather than failing the error path.
fn details_for_adbc(details: &[StatusDetails]) -> Option<Vec<(String, Vec<u8>)>> {
    let mapped: Vec<(String, Vec<u8>)> = details.iter().filter_map(map_detail).collect();
    (!mapped.is_empty()).then_some(mapped)
}

/// Map one detail to its ADBC `(key, value)` pair. Taking the key from the serialized `@type`
/// rather than matching [`StatusDetails`] variants also covers types added upstream.
fn map_detail(detail: &StatusDetails) -> Option<(String, Vec<u8>)> {
    let value = serde_json::to_value(detail).ok()?;
    let type_url = value.get("@type")?.as_str()?;
    let key = type_url
        .rsplit('/')
        .next()
        .unwrap_or(type_url)
        .to_ascii_lowercase();
    Some((key, serde_json::to_vec(&value).ok()?))
}

/// Build an ADBC error from a `google.rpc.Status` embedded in a `BatchWriteResponse`, matching
/// [`from_spanner`] so bulk-ingest error handling is the same for both ingest paths.
pub(crate) fn from_status_parts(code: i32, message: &str, details: &[Any]) -> Error {
    let status = status_for_grpc_code(Code::from(code));
    let mut full = format!("Spanner batch-write error: {message}");
    if Code::from(code) == Code::PermissionDenied {
        full.push_str(PERMISSION_DENIED_GUIDANCE);
    }
    let decoded: Vec<StatusDetails> = details.iter().map(StatusDetails::from).collect();
    let mut adbc = err(full, status);
    adbc.vendor_code = code;
    adbc.details = details_for_adbc(&decoded);
    adbc
}

/// Translate a client builder error, which has no gRPC status, into an `Internal` error.
pub(crate) fn from_builder<E: std::fmt::Display>(error: E) -> Error {
    err(format!("Spanner error: {error}"), Status::Internal)
}

/// Map a gRPC status [`Code`] to the closest ADBC [`Status`], or `Internal` when none fits.
fn status_for_grpc_code(code: Code) -> Status {
    match code {
        Code::NotFound => Status::NotFound,
        Code::AlreadyExists => Status::AlreadyExists,
        Code::Unauthenticated => Status::Unauthenticated,
        Code::PermissionDenied => Status::Unauthorized,
        Code::InvalidArgument => Status::InvalidArguments,
        // Spanner raises this for bad values during execution, such as numeric overflow, which
        // matches adbc.h's InvalidData and the PostgreSQL driver's SQLSTATE class 22 mapping.
        Code::OutOfRange => Status::InvalidData,
        // Not Integrity: Spanner uses this code for constraint violations but also for plain
        // wrong-state failures, and only the message text tells them apart. Callers that need the
        // distinction have `vendor_code` and the `PreconditionFailure` detail.
        Code::FailedPrecondition => Status::InvalidState,
        Code::DeadlineExceeded => Status::Timeout,
        Code::Cancelled => Status::Cancelled,
        // adbc.h: IO covers "a remote service may be unavailable".
        Code::Unavailable => Status::IO,
        Code::Unimplemented => Status::NotImplemented,
        // Transient contention, normally retried by the client's transaction runner. Internal
        // would suggest a driver bug, and ADBC has nothing closer than IO.
        Code::Aborted => Status::IO,
        Code::Unknown => Status::Unknown,
        // As in the Flight SQL driver.
        Code::DataLoss => Status::IO,
        // ResourceExhausted, Internal and Ok.
        _ => Status::Internal,
    }
}

#[cfg(test)]
mod tests;

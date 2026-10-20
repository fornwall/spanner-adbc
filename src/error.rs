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

/// The error for setting an option that a `"database"`, `"connection"` or `"statement"` doesn't
/// accept.
pub(crate) fn unknown_option(object_kind: &str, key: &str) -> Error {
    unsupported(if key == crate::OPTION_COMMIT_STATS_MUTATION_COUNT {
        format!("option {key} is read-only")
    } else if let Some(level) = crate::options::option_level(key) {
        format!("option {key} is a {level} option and can't be set on a {object_kind}")
    } else {
        format!("unsupported Spanner {object_kind} option: {key}")
    })
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
pub(crate) fn chain<'a>(
    source: &'a (dyn std::error::Error + 'static),
) -> impl Iterator<Item = &'a (dyn std::error::Error + 'static)> {
    std::iter::successors(Some(source), |error| error.source())
}

/// Translate a Spanner client error into an ADBC error.
///
/// A gRPC status maps to the closest [`Status`], and its numeric code is kept in `vendor_code` so
/// callers can tell apart codes that share a status (for example `ABORTED` = 10). A duplicate key
/// and other constraint violations become [`Status::Integrity`]. Errors without a status have
/// `vendor_code` 0 and become [`Status::Unauthenticated`] for credential failures,
/// [`Status::Timeout`] for client deadlines, [`Status::IO`] for connection and transport failures
/// and [`Status::Internal`] otherwise. The C ABI's 1.1.0 error layout reserves `vendor_code`, so
/// `src/ffi/error.rs` passes a non-zero code there as the detail `adbc.spanner.vendor_code`
/// instead.
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
    // An exhausted retry policy wraps the last error instead of carrying its kind and status.
    let client_errors =
        || chain(&error).filter_map(|e| e.downcast_ref::<google_cloud_spanner::Error>());
    if let Some(status) = client_errors().find_map(|e| e.status()) {
        return service_error(
            status.code as i32,
            &status.message,
            details_for_adbc(&status.details),
        );
    }
    let status = if client_errors().any(google_cloud_spanner::Error::is_authentication) {
        Status::Unauthenticated
    } else if client_errors().any(google_cloud_spanner::Error::is_timeout) {
        Status::Timeout
    } else if client_errors().any(|e| e.is_connect() || e.is_transport()) {
        // adbc.h: IO covers "a remote service may be unavailable".
        Status::IO
    } else {
        Status::Internal
    };
    err(format!("Spanner error: {error}"), status)
}

/// Build an ADBC error from a `google.rpc.Status` embedded in a `BatchWriteResponse`, matching
/// [`from_spanner`] so bulk-ingest error handling is the same for both ingest paths.
pub(crate) fn from_status_parts(code: i32, message: &str, details: &[Any]) -> Error {
    let decoded: Vec<StatusDetails> = details.iter().map(StatusDetails::from).collect();
    service_error(code, message, details_for_adbc(&decoded))
}

/// The error for a gRPC status, formatted `CODE: message` like grpc-java exceptions.
fn service_error(code: i32, message: &str, details: Option<Vec<(String, Vec<u8>)>>) -> Error {
    let grpc_code = Code::from(code);
    let mut text = format!("Spanner error: {grpc_code}: {message}");
    // The message already names the missing permission. Naming a role could steer callers to an
    // over-broad grant.
    if grpc_code == Code::PermissionDenied {
        text.push_str(
            " (grant an IAM role that includes it; see https://cloud.google.com/spanner/docs/iam)",
        );
    }
    let mut error = err(text, status_for_rpc(grpc_code, message));
    error.vendor_code = code;
    error.details = details;
    error
}

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

/// Translate a client builder error, which has no gRPC status, into an `Internal` error.
pub(crate) fn from_builder<E: std::fmt::Display>(error: E) -> Error {
    err(format!("Spanner error: {error}"), Status::Internal)
}

/// Map a gRPC status to an ADBC [`Status`]: [`Status::Integrity`] for a violated data constraint,
/// otherwise by code alone.
fn status_for_rpc(code: Code, message: &str) -> Status {
    if violates_constraint(code, message) {
        Status::Integrity
    } else {
        status_for_grpc_code(code)
    }
}

/// Whether an error with a code that Spanner also uses for unrelated failures reports a violated
/// NOT NULL, foreign key, unique index, CHECK or interleaving constraint. Only the message tells
/// them apart, so match the wording of both Spanner (as Omni reports it) and the emulator,
/// ignoring case. The unit tests list both.
fn violates_constraint(code: Code, message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    let has = |needle: &str| message.contains(needle);
    match code {
        Code::FailedPrecondition => {
            // NOT NULL, including DDL that adds it over existing NULLs.
            has("must not be null")
                || has("non-null value for")
                || has("null value for column")
                || has("has a null value")
                || (has("foreign key") && has("violat"))
                // DDL that adds a unique index over existing duplicates.
                || has("uniqueness violation")
        }
        Code::OutOfRange => has("check constraint") && has("violated"),
        // An interleaved row whose parent row is missing.
        Code::NotFound => (has("parent row") && has("missing")) || has("not found in parent table"),
        _ => false,
    }
}

/// Map a gRPC status [`Code`] to the closest ADBC [`Status`], or `Internal` when none fits.
fn status_for_grpc_code(code: Code) -> Status {
    match code {
        Code::NotFound => Status::NotFound,
        // Spanner returns this only for a duplicate primary or unique index key. An existing
        // schema object is FAILED_PRECONDITION ("Duplicate name in schema"); the ingest create
        // path remaps that to AlreadyExists.
        Code::AlreadyExists => Status::Integrity,
        Code::Unauthenticated => Status::Unauthenticated,
        Code::PermissionDenied => Status::Unauthorized,
        Code::InvalidArgument => Status::InvalidArguments,
        // Spanner raises this for bad values during execution, such as numeric overflow, which
        // matches adbc.h's InvalidData and the PostgreSQL driver's SQLSTATE class 22 mapping.
        Code::OutOfRange => Status::InvalidData,
        // Also used for constraint violations, which `violates_constraint` recognizes.
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
        // Quota, throttling and memory limits. Like PostgreSQL's SQLSTATE class 53, which that
        // driver reports as IO, and PEP 249's OperationalError.
        Code::ResourceExhausted => Status::IO,
        // Internal and Ok.
        _ => Status::Internal,
    }
}

#[cfg(test)]
mod tests;

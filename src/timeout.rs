//! Overall deadlines for query, update and fetch operations, including internal metadata and
//! DDL. Each deadline covers the operation's retries and returns [`Status::Timeout`] on expiry.
//! Unset or zero values disable the deadline. See `docs/options.md` for option details.

use std::future::Future;
use std::time::Duration;

use adbc_core::error::{Result, Status};

use crate::error::err;

/// The `spanner.rpc.timeout_seconds.*` settings of a connection or statement, in seconds. Values
/// are stored as configured so `get_option` round-trips them; the accessors return `None` for
/// unset or `0`, both meaning no timeout.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct RpcTimeouts {
    pub(crate) query: Option<f64>,
    pub(crate) update: Option<f64>,
    pub(crate) fetch: Option<f64>,
}

impl RpcTimeouts {
    pub(crate) fn query_timeout(&self) -> Option<Duration> {
        as_duration(self.query)
    }

    pub(crate) fn update_timeout(&self) -> Option<Duration> {
        as_duration(self.update)
    }

    pub(crate) fn fetch_timeout(&self) -> Option<Duration> {
        as_duration(self.fetch)
    }
}

/// Values were validated when set, so conversion cannot fail.
fn as_duration(seconds: Option<f64>) -> Option<Duration> {
    seconds
        .filter(|&seconds| seconds > 0.0)
        .and_then(|seconds| Duration::try_from_secs_f64(seconds).ok())
}

/// Run `future` under an optional deadline. The timeout error names `option`, the setting that
/// imposed the deadline.
pub(crate) async fn with_timeout<T>(
    limit: Option<Duration>,
    option: &'static str,
    future: impl Future<Output = Result<T>>,
) -> Result<T> {
    let Some(limit) = limit else {
        return future.await;
    };
    match tokio::time::timeout(limit, future).await {
        Ok(result) => result,
        Err(_) => Err(err(
            format!(
                "operation timed out after {}s ({option})",
                limit.as_secs_f64()
            ),
            Status::Timeout,
        )),
    }
}

#[cfg(test)]
mod tests;

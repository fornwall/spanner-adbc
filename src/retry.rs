//! Retry limits and exponential backoff. Configured limits wrap the client's
//! [`SpannerRetryPolicy`], preserving its transport-error retries on idempotent requests.
//! Unset options retain client defaults; see `docs/options.md` for the option contract.
//!
//! The pinned client's query and BatchWrite streaming retry loops reset their start time on
//! each attempt, so `max_elapsed_seconds` only bounds unary retries. Use the query/fetch/update
//! timeouts for streaming deadlines. `max_attempts` works on all three paths.

use std::time::Duration;

use adbc_core::error::Result;
use adbc_core::options::OptionValue;
use google_cloud_gax::backoff_policy::BackoffPolicyArg;
use google_cloud_gax::exponential_backoff::ExponentialBackoffBuilder;
use google_cloud_gax::retry_policy::{RetryPolicyArg, RetryPolicyExt as _};
use google_cloud_spanner::batch::Partition;
use google_cloud_spanner::builder::{
    BatchDmlBuilder, BatchWriteTransactionBuilder, TransactionRunnerBuilder,
    WriteOnlyTransactionBuilder,
};
use google_cloud_spanner::retry_policy::SpannerRetryPolicy;
use google_cloud_spanner::statement::StatementBuilder;

use crate::error::invalid_argument;

/// About 30 years, tokio's own far-future deadline: indistinguishable from no limit, and safe to
/// add to an `Instant` on every platform.
const MAX_ELAPSED_LIMIT: Duration = Duration::from_secs(30 * 365 * 24 * 60 * 60);

// Client transaction builders share Begin/Commit policy setters but no common trait.
macro_rules! apply_to_commit_builder {
    ($(#[$attr:meta])* $name:ident($builder:ty)) => {
        $(#[$attr])*
        #[must_use]
        pub(crate) fn $name(&self, mut builder: $builder) -> $builder {
            if let Some(policy) = self.retry_policy_arg() {
                builder = builder
                    .with_begin_retry_policy(policy.clone())
                    .with_commit_retry_policy(policy);
            }
            if let Some(backoff) = self.backoff_policy_arg() {
                builder = builder
                    .with_begin_backoff_policy(backoff.clone())
                    .with_commit_backoff_policy(backoff);
            }
            builder
        }
    };
}

// Statement and batch builders each accept one retry and backoff policy.
macro_rules! apply_to_request_builder {
    ($(#[$attr:meta])* $name:ident($builder:ty)) => {
        $(#[$attr])*
        #[must_use]
        pub(crate) fn $name(&self, mut builder: $builder) -> $builder {
            if let Some(policy) = self.retry_policy_arg() {
                builder = builder.with_retry_policy(policy);
            }
            if let Some(backoff) = self.backoff_policy_arg() {
                builder = builder.with_backoff_policy(backoff);
            }
            builder
        }
    };
}

/// The `spanner.retry.*` settings of a connection or statement. Values are stored as configured
/// so `get_option` round-trips them; unset values keep the client defaults.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct RetryConfig {
    /// Total attempts, including the first.
    pub(crate) max_attempts: Option<u32>,
    pub(crate) max_elapsed_seconds: Option<f64>,
    pub(crate) backoff_initial_seconds: Option<f64>,
    pub(crate) backoff_max_seconds: Option<f64>,
    pub(crate) backoff_multiplier: Option<f64>,
}

impl RetryConfig {
    /// Set `spanner.retry.max_attempts`; an empty string unsets it.
    pub(crate) fn set_max_attempts(&mut self, value: OptionValue) -> Result<()> {
        self.max_attempts = parse_max_attempts(value)?;
        Ok(())
    }

    /// The retry time budget. The value was validated when set, so conversion cannot fail.
    ///
    /// Capped at [`MAX_ELAPSED_LIMIT`]: gax adds the limit to an `Instant` before every attempt,
    /// which panics on overflow for limits that still fit in a `Duration`.
    fn max_elapsed_duration(&self) -> Option<Duration> {
        self.max_elapsed_seconds
            .and_then(|seconds| Duration::try_from_secs_f64(seconds).ok())
            .map(|limit| limit.min(MAX_ELAPSED_LIMIT))
    }

    /// The client's default [`SpannerRetryPolicy`] bounded by the configured limits, or `None`
    /// when no limit is set.
    fn retry_policy_arg(&self) -> Option<RetryPolicyArg> {
        match (self.max_attempts, self.max_elapsed_duration()) {
            (None, None) => None,
            (Some(attempts), None) => Some(
                SpannerRetryPolicy::new()
                    .with_attempt_limit(attempts)
                    .into(),
            ),
            (None, Some(elapsed)) => {
                Some(SpannerRetryPolicy::new().with_time_limit(elapsed).into())
            }
            (Some(attempts), Some(elapsed)) => Some(
                SpannerRetryPolicy::new()
                    .with_time_limit(elapsed)
                    .with_attempt_limit(attempts)
                    .into(),
            ),
        }
    }

    /// The configured exponential backoff, or `None` when no backoff option is set. Unset values
    /// use the client defaults (1s, 60s, x2.0). [`ExponentialBackoffBuilder::clamp`] forces the
    /// combination into the valid ranges, so building cannot fail.
    fn backoff_policy_arg(&self) -> Option<BackoffPolicyArg> {
        if self.backoff_initial_seconds.is_none()
            && self.backoff_max_seconds.is_none()
            && self.backoff_multiplier.is_none()
        {
            return None;
        }
        let mut builder = ExponentialBackoffBuilder::new();
        if let Some(initial) = self.backoff_initial_seconds {
            // Validated as a representable Duration when set, so this cannot panic.
            builder = builder.with_initial_delay(Duration::from_secs_f64(initial));
        }
        if let Some(maximum) = self.backoff_max_seconds {
            builder = builder.with_maximum_delay(Duration::from_secs_f64(maximum));
        }
        if let Some(multiplier) = self.backoff_multiplier {
            builder = builder.with_scaling(multiplier);
        }
        Some(builder.clamp().into())
    }

    apply_to_request_builder! {
        /// Apply the policies to a query or DML statement.
        apply_to_statement(StatementBuilder)
    }

    apply_to_request_builder! {
        /// Apply the policies to an `ExecuteBatchDml` request.
        apply_to_batch_dml(BatchDmlBuilder)
    }

    apply_to_request_builder! {
        /// Bound retries of unacknowledged BatchWrite mutation groups.
        apply_to_batch_write(BatchWriteTransactionBuilder)
    }

    apply_to_request_builder! {
        /// Apply the policies to a partition read. Descriptors do not carry them, so the reading
        /// connection's settings apply.
        apply_to_partition(Partition)
    }

    apply_to_commit_builder! {
        /// Apply the policies to a read/write transaction's Begin and Commit RPCs. Retrying
        /// aborted transactions is a separate policy left at the client default.
        apply_to_runner(TransactionRunnerBuilder)
    }

    apply_to_commit_builder! {
        /// Apply the policies to the Begin and Commit RPCs of a bulk-ingest write-only transaction.
        apply_to_write_only(WriteOnlyTransactionBuilder)
    }
}

/// Parse `spanner.retry.max_attempts` from an integer, a whole double or a numeric string. `1`
/// disables retries; an empty string unsets it.
fn parse_max_attempts(value: OptionValue) -> Result<Option<u32>> {
    let reject = || {
        invalid_argument(format!(
            "option {} must be a positive integer number of attempts (>= 1)",
            crate::OPTION_RETRY_MAX_ATTEMPTS
        ))
    };
    let attempts: i64 = match value {
        OptionValue::String(s) => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                return Ok(None);
            }
            trimmed.parse::<i64>().map_err(|_| reject())?
        }
        OptionValue::Int(i) => i,
        // Accept the `set_option_double` shape only when it is a whole number.
        OptionValue::Double(d) if d.is_finite() && d.fract() == 0.0 => d as i64,
        _ => return Err(reject()),
    };
    if !(1..=i64::from(u32::MAX)).contains(&attempts) {
        return Err(reject());
    }
    Ok(Some(attempts as u32))
}

#[cfg(test)]
mod tests;

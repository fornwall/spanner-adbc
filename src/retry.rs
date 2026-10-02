//! Retry limits and exponential backoff. Configured limits wrap the client's
//! [`SpannerRetryPolicy`], preserving its transport-error retries on idempotent requests.
//! Unset options retain client defaults; see `docs/options.md` for the option contract.
//!
//! The pinned client's streaming retry loop resets its start time on each attempt, so
//! `max_elapsed_seconds` only bounds unary RPC retries. Use the query/fetch RPC timeouts for
//! a streaming deadline. `max_attempts` works on both paths; `tests/mock_spanner.rs` covers this.

use std::time::Duration;

use adbc_core::error::Result;
use adbc_core::options::OptionValue;
use google_cloud_gax::backoff_policy::BackoffPolicyArg;
use google_cloud_gax::exponential_backoff::ExponentialBackoffBuilder;
use google_cloud_gax::retry_policy::{RetryPolicyArg, RetryPolicyExt as _};
use google_cloud_spanner::builder::{
    BatchDmlBuilder, TransactionRunnerBuilder, WriteOnlyTransactionBuilder,
};
use google_cloud_spanner::retry_policy::SpannerRetryPolicy;
use google_cloud_spanner::statement::StatementBuilder;

use crate::error::invalid_argument;

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

// Statement and batch-DML builders each accept one retry and backoff policy.
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

/// The retry-tuning configuration held by a connection or statement
/// (`spanner.retry.max_attempts` / `spanner.retry.max_elapsed_seconds` and the backoff knobs
/// `spanner.retry.backoff.{initial_seconds,max_seconds,multiplier}`).
///
/// A connection's value is cloned into each statement it creates (which may then override either
/// knob). Values are stored exactly as configured so `get_option` / `get_option_int` /
/// `get_option_double` round-trip them; [`retry_policy_arg`](Self::retry_policy_arg) turns them into
/// a gax [`RetryPolicyArg`] (or `None`, leaving the client's default policy) at apply time.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct RetryConfig {
    /// `spanner.retry.max_attempts`, when set: the maximum number of attempts (>= 1).
    pub(crate) max_attempts: Option<u32>,
    /// `spanner.retry.max_elapsed_seconds`, when set: the total wall-clock retry budget, in seconds.
    pub(crate) max_elapsed_seconds: Option<f64>,
    /// `spanner.retry.backoff.initial_seconds`, when set: the first inter-attempt delay, in seconds.
    pub(crate) backoff_initial_seconds: Option<f64>,
    /// `spanner.retry.backoff.max_seconds`, when set: the ceiling on the inter-attempt delay, in
    /// seconds.
    pub(crate) backoff_max_seconds: Option<f64>,
    /// `spanner.retry.backoff.multiplier`, when set: the per-attempt growth factor for the delay.
    pub(crate) backoff_multiplier: Option<f64>,
}

impl RetryConfig {
    /// Handle a `set_option` for `spanner.retry.max_attempts`. An empty string unsets it. The one
    /// option here whose parse is more than a coercion, so the dispatch goes through this setter
    /// rather than assigning the field itself.
    pub(crate) fn set_max_attempts(&mut self, value: OptionValue) -> Result<()> {
        self.max_attempts = parse_max_attempts(value)?;
        Ok(())
    }

    /// The effective total retry budget as a [`Duration`] (`None` when unset). Conversion cannot
    /// fail — [`f64_option`](crate::options::f64_option) validated it at set time.
    fn max_elapsed_duration(&self) -> Option<Duration> {
        self.max_elapsed_seconds
            .and_then(|seconds| Duration::try_from_secs_f64(seconds).ok())
    }

    /// The gax retry policy for this configuration, or `None` when neither knob is set (leaving the
    /// client's default [`SpannerRetryPolicy`] in place). When either is set that same policy is
    /// re-applied explicitly, bounded by the configured attempt / elapsed-time limits.
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

    /// The gax backoff policy for this configuration, or `None` when none of the three backoff knobs
    /// is set (leaving the client's default exponential backoff in place). Unset knobs fall back to
    /// the client's defaults (1s, 60s, ×2.0) and the combination is clamped via
    /// [`ExponentialBackoffBuilder::clamp`] — initial delay ≥ 1ms, maximum in `[1s, 24h]` and ≥ the
    /// initial, multiplier in `[1.0, 32.0]` — so building it can never fail. Independent of
    /// [`retry_policy_arg`](Self::retry_policy_arg).
    fn backoff_policy_arg(&self) -> Option<BackoffPolicyArg> {
        if self.backoff_initial_seconds.is_none()
            && self.backoff_max_seconds.is_none()
            && self.backoff_multiplier.is_none()
        {
            return None;
        }
        let mut builder = ExponentialBackoffBuilder::new();
        if let Some(initial) = self.backoff_initial_seconds {
            // `f64_option` validated Duration-representability, so this cannot fail.
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
        /// Apply the retry and backoff policies to a statement builder (queries and DML alike).
        apply_to_statement(StatementBuilder)
    }

    apply_to_request_builder! {
        /// Apply the retry and backoff policies to an `ExecuteBatchDml` batch builder.
        apply_to_batch_dml(BatchDmlBuilder)
    }

    apply_to_commit_builder! {
        /// Apply the retry and backoff policies to a read/write transaction runner builder (its
        /// Begin and Commit RPCs). The transaction-level abort retry (Spanner's
        /// optimistic-concurrency re-run) is a separate policy left at the client default.
        apply_to_runner(TransactionRunnerBuilder)
    }

    apply_to_commit_builder! {
        /// Apply the retry and backoff policies to a write-only transaction builder (the bulk-ingest
        /// commit path): its Begin and Commit RPCs.
        apply_to_write_only(WriteOnlyTransactionBuilder)
    }
}

/// Parse a `spanner.retry.max_attempts` value: a positive integer (the first attempt plus retries;
/// `1` disables retrying), accepted as an integer, a whole-valued double, or a numeric string.
/// Zero, negatives, fractions, values above [`u32::MAX`] and non-numeric input are rejected with
/// `InvalidArguments`; an empty string yields `None` (unset).
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

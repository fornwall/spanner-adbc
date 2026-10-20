//! Option value parsing, and the configuration shared by connections and statements. The
//! parsers take the option key and name it in their errors.

use std::time::Duration;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use adbc_core::error::Result;
use adbc_core::options::OptionValue;
use google_cloud_spanner::model::transaction_options::IsolationLevel;

use crate::conversion::TimestampPrecision;
use crate::directed_read::DirectedRead;
use crate::error::invalid_argument;
use crate::query_options::QueryOptionsConfig;
use crate::request::{CommitStats, RequestConfig};
use crate::retry::RetryConfig;
use crate::staleness::ReadStaleness;
use crate::timeout::RpcTimeouts;

/// Describe a rejected value for an error message.
pub(crate) fn shown(value: &OptionValue) -> String {
    match value {
        OptionValue::String(s) => format!("{s:?}"),
        OptionValue::Int(i) => format!("the integer {i}"),
        OptionValue::Double(d) => format!("the double {d}"),
        OptionValue::Bytes(_) => "a bytes value".to_string(),
        _ => "an unsupported value type".to_string(),
    }
}

/// Parse a boolean option from the string `true` or `false`.
pub(crate) fn bool_option(value: OptionValue, key: &str) -> Result<bool> {
    match value {
        OptionValue::String(s) if s == "true" => Ok(true),
        OptionValue::String(s) if s == "false" => Ok(false),
        // An integer would not round-trip, since the getters return the string form.
        other => Err(invalid_argument(format!(
            "option {key} expects the string \"true\" or \"false\", got {}",
            shown(&other)
        ))),
    }
}

/// Like [`bool_option`], but a blank string resets the option to `false`.
pub(crate) fn bool_option_unsettable(value: OptionValue, key: &str) -> Result<bool> {
    match &value {
        OptionValue::String(s) if s.trim().is_empty() => Ok(false),
        _ => bool_option(value, key),
    }
}

/// Parse a string option.
pub(crate) fn string_option(value: OptionValue, key: &str) -> Result<String> {
    match value {
        OptionValue::String(s) => Ok(s),
        other => Err(invalid_argument(format!(
            "option {key} expects a string, got {}",
            shown(&other)
        ))),
    }
}

/// Parse a string option where `""` means unset. The value is not trimmed.
pub(crate) fn non_empty_string_option(value: OptionValue, key: &str) -> Result<Option<String>> {
    Ok(Some(string_option(value, key)?).filter(|s| !s.is_empty()))
}

/// An option's string, kept for `get_option`, alongside its parsed value. Both change together
/// through [`set`](Self::set).
#[derive(Debug, Clone)]
pub(crate) struct RawParsed<T> {
    raw: Option<String>,
    parsed: Option<T>,
}

impl<T> Default for RawParsed<T> {
    fn default() -> Self {
        Self {
            raw: None,
            parsed: None,
        }
    }
}

impl<T> RawParsed<T> {
    /// Trim and parse `value`; a blank string unsets. A rejected value leaves the state unchanged.
    pub(crate) fn set(
        &mut self,
        value: OptionValue,
        key: &str,
        parse: impl FnOnce(&str) -> Result<T>,
    ) -> Result<()> {
        let raw = string_option(value, key)?;
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            self.raw = None;
            self.parsed = None;
            return Ok(());
        }
        self.parsed = Some(parse(trimmed)?);
        self.raw = Some(trimmed.to_string());
        Ok(())
    }

    pub(crate) fn raw(&self) -> Option<&str> {
        self.raw.as_deref()
    }

    pub(crate) fn parsed(&self) -> Option<&T> {
        self.parsed.as_ref()
    }
}

/// The accepted range of an [`f64_option`]. The seconds variants must also fit in a [`Duration`],
/// so later `Duration::from_secs_f64` conversions cannot panic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum F64Range {
    PositiveSeconds,
    /// Zero is accepted; the RPC timeouts treat it as disabled.
    NonNegativeSeconds,
    /// A plain number rather than seconds.
    PositiveFactor,
}

/// Parse a finite number in `range` from a numeric string, an integer or a double. A blank string
/// yields `None` (unset).
pub(crate) fn f64_option(value: OptionValue, key: &str, range: F64Range) -> Result<Option<f64>> {
    let expected = match range {
        F64Range::PositiveSeconds => "a finite, strictly positive number of seconds",
        F64Range::NonNegativeSeconds => "a finite, non-negative number of seconds",
        F64Range::PositiveFactor => "a finite, strictly positive number",
    };
    let reject = || {
        invalid_argument(format!(
            "option {key} expects {expected}, got {}",
            shown(&value)
        ))
    };
    let n = match &value {
        OptionValue::String(s) if s.trim().is_empty() => return Ok(None),
        OptionValue::String(s) => s.trim().parse::<f64>().map_err(|_| reject())?,
        OptionValue::Double(d) => *d,
        OptionValue::Int(i) => *i as f64,
        _ => return Err(reject()),
    };
    let in_range = match range {
        F64Range::NonNegativeSeconds => n >= 0.0,
        F64Range::PositiveSeconds | F64Range::PositiveFactor => n > 0.0,
    };
    let fits = range == F64Range::PositiveFactor || Duration::try_from_secs_f64(n).is_ok();
    if n.is_finite() && in_range && fits {
        Ok(Some(n))
    } else {
        Err(reject())
    }
}

/// Parse a positive integer from an integer or a numeric string.
pub(crate) fn positive_usize(value: OptionValue, key: &str) -> Result<usize> {
    let n = match &value {
        OptionValue::Int(i) => usize::try_from(*i).ok(),
        OptionValue::String(s) => s.parse::<usize>().ok(),
        _ => None,
    };
    n.filter(|&n| n > 0).ok_or_else(|| {
        invalid_argument(format!(
            "option {key} expects a positive integer, got {}",
            shown(&value)
        ))
    })
}

/// Parse a `get_option_string` result for `get_option_int`.
///
/// Lookup errors pass through unchanged, since ADBC's `NotFound` means unset or unknown. A value
/// that is not an integer is `InvalidArguments`.
pub(crate) fn int_from_stored_string(stored: Result<String>, key: &str) -> Result<i64> {
    let value = stored?;
    value
        .parse::<i64>()
        .map_err(|_| invalid_argument(format!("option {key} value {value:?} is not an integer")))
}

/// Parse a `get_option_string` result for `get_option_double`, as [`int_from_stored_string`].
pub(crate) fn double_from_stored_string(stored: Result<String>, key: &str) -> Result<f64> {
    let value = stored?;
    value
        .parse::<f64>()
        .map_err(|_| invalid_argument(format!("option {key} value {value:?} is not a number")))
}

/// Implement `get_option_bytes`, `get_option_int` and `get_option_double` in terms of
/// `get_option_string`, since every readable option has a string form.
macro_rules! impl_typed_option_getters {
    () => {
        fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
            Ok(self.get_option_string(key)?.into_bytes())
        }

        fn get_option_int(&self, key: Self::Option) -> Result<i64> {
            let name = key.as_ref().to_string();
            crate::options::int_from_stored_string(self.get_option_string(key), &name)
        }

        fn get_option_double(&self, key: Self::Option) -> Result<f64> {
            let name = key.as_ref().to_string();
            crate::options::double_from_stored_string(self.get_option_string(key), &name)
        }
    };
}
pub(crate) use impl_typed_option_getters;

/// Option-settable configuration carried by a
/// [`SpannerConnection`](crate::connection::SpannerConnection) and inherited by each
/// [`SpannerStatement`](crate::statement::SpannerStatement) it creates.
#[derive(Debug, Clone)]
pub(crate) struct SharedConfig {
    /// `adbc.connection.readonly`: writes, including commits of buffered work, fail with
    /// `InvalidState`. Shared so that toggling it on the connection reaches existing statements.
    pub(crate) read_only: Arc<AtomicBool>,
    /// Set only on the connection; statements inherit it.
    pub(crate) isolation: IsolationLevel,
    pub(crate) read_staleness: ReadStaleness,
    pub(crate) request: RequestConfig,
    pub(crate) directed_read: DirectedRead,
    pub(crate) query_options: QueryOptionsConfig,
    pub(crate) timestamp_precision: TimestampPrecision,
    pub(crate) timeouts: RpcTimeouts,
    pub(crate) retry: RetryConfig,
    /// Per object, not inherited: a statement records its autocommit commits, a connection its
    /// manual-mode commit.
    pub(crate) commit_stats: CommitStats,
}

impl Default for SharedConfig {
    fn default() -> Self {
        Self {
            read_only: Arc::new(AtomicBool::new(false)),
            // `IsolationLevel` has no `Default`, so this impl cannot be derived.
            isolation: IsolationLevel::Unspecified,
            read_staleness: ReadStaleness::default(),
            request: RequestConfig::default(),
            directed_read: DirectedRead::default(),
            query_options: QueryOptionsConfig::default(),
            timestamp_precision: TimestampPrecision::default(),
            timeouts: RpcTimeouts::default(),
            retry: RetryConfig::default(),
            commit_stats: CommitStats::default(),
        }
    }
}

impl SharedConfig {
    /// The config for a new statement: a copy sharing `read_only`, with fresh `commit_stats`.
    pub(crate) fn inherit(&self) -> Self {
        Self {
            commit_stats: CommitStats::default(),
            ..self.clone()
        }
    }

    /// The current `adbc.connection.readonly` value.
    pub(crate) fn is_read_only(&self) -> bool {
        self.read_only.load(Ordering::Acquire)
    }

    /// Apply a shared option. Returns `false` for keys this configuration does not handle.
    pub(crate) fn set_option(&mut self, key: &str, value: OptionValue) -> Result<bool> {
        match key {
            crate::OPTION_READ_STALENESS => self.read_staleness.set_staleness(value)?,
            crate::OPTION_REQUEST_PRIORITY => self.request.set_priority(value)?,
            crate::OPTION_REQUEST_TAG => self.request.set_request_tag(value)?,
            crate::OPTION_DIRECTED_READ => self.directed_read.set(value)?,
            crate::OPTION_MAX_COMMIT_DELAY => self.request.set_max_commit_delay(value)?,
            crate::OPTION_COMMIT_STATS => self.request.set_commit_stats(value)?,
            crate::OPTION_EXCLUDE_TXN_FROM_CHANGE_STREAMS => {
                self.request.set_exclude_txn_from_change_streams(value)?
            }
            crate::OPTION_QUERY_OPTIMIZER_VERSION => {
                self.query_options.optimizer_version = non_empty_string_option(value, key)?
            }
            crate::OPTION_QUERY_OPTIMIZER_STATISTICS_PACKAGE => {
                self.query_options.optimizer_statistics_package =
                    non_empty_string_option(value, key)?
            }
            crate::OPTION_MAX_TIMESTAMP_PRECISION => {
                self.timestamp_precision = TimestampPrecision::parse_option(value)?
            }
            crate::OPTION_RPC_TIMEOUT_QUERY => {
                self.timeouts.query = f64_option(value, key, F64Range::NonNegativeSeconds)?
            }
            crate::OPTION_RPC_TIMEOUT_UPDATE => {
                self.timeouts.update = f64_option(value, key, F64Range::NonNegativeSeconds)?
            }
            crate::OPTION_RPC_TIMEOUT_FETCH => {
                self.timeouts.fetch = f64_option(value, key, F64Range::NonNegativeSeconds)?
            }
            crate::OPTION_RETRY_MAX_ATTEMPTS => self.retry.set_max_attempts(value)?,
            crate::OPTION_RETRY_MAX_ELAPSED_SECONDS => {
                self.retry.max_elapsed_seconds = f64_option(value, key, F64Range::PositiveSeconds)?
            }
            crate::OPTION_RETRY_BACKOFF_INITIAL_SECONDS => {
                self.retry.backoff_initial_seconds =
                    f64_option(value, key, F64Range::PositiveSeconds)?
            }
            crate::OPTION_RETRY_BACKOFF_MAX_SECONDS => {
                self.retry.backoff_max_seconds = f64_option(value, key, F64Range::PositiveSeconds)?
            }
            crate::OPTION_RETRY_BACKOFF_MULTIPLIER => {
                self.retry.backoff_multiplier = f64_option(value, key, F64Range::PositiveFactor)?
            }
            _ => return Ok(false),
        }
        Ok(true)
    }

    /// Return a shared option, or `NotFound` when it is unset or unknown.
    pub(crate) fn option_string(&self, key: &str) -> Result<String> {
        let value: Option<String> = match key {
            crate::OPTION_READ_STALENESS => {
                self.read_staleness.staleness_string().map(str::to_string)
            }
            crate::OPTION_REQUEST_PRIORITY => self.request.priority_string().map(str::to_string),
            crate::OPTION_REQUEST_TAG => self.request.request_tag_string().map(str::to_string),
            crate::OPTION_DIRECTED_READ => self.directed_read.option_string().map(str::to_string),
            crate::OPTION_MAX_COMMIT_DELAY => {
                self.request.max_commit_delay_string().map(str::to_string)
            }
            crate::OPTION_COMMIT_STATS => Some(self.request.commit_stats_string().to_string()),
            crate::OPTION_EXCLUDE_TXN_FROM_CHANGE_STREAMS => Some(
                self.request
                    .exclude_txn_from_change_streams_string()
                    .to_string(),
            ),
            crate::OPTION_COMMIT_STATS_MUTATION_COUNT => {
                self.commit_stats.mutation_count().map(|n| n.to_string())
            }
            crate::OPTION_QUERY_OPTIMIZER_VERSION => self.query_options.optimizer_version.clone(),
            crate::OPTION_QUERY_OPTIMIZER_STATISTICS_PACKAGE => {
                self.query_options.optimizer_statistics_package.clone()
            }
            crate::OPTION_MAX_TIMESTAMP_PRECISION => {
                Some(self.timestamp_precision.as_str().to_string())
            }
            crate::OPTION_RPC_TIMEOUT_QUERY => self.timeouts.query.map(|s| s.to_string()),
            crate::OPTION_RPC_TIMEOUT_UPDATE => self.timeouts.update.map(|s| s.to_string()),
            crate::OPTION_RPC_TIMEOUT_FETCH => self.timeouts.fetch.map(|s| s.to_string()),
            crate::OPTION_RETRY_MAX_ATTEMPTS => self.retry.max_attempts.map(|n| n.to_string()),
            crate::OPTION_RETRY_MAX_ELAPSED_SECONDS => {
                self.retry.max_elapsed_seconds.map(|s| s.to_string())
            }
            crate::OPTION_RETRY_BACKOFF_INITIAL_SECONDS => {
                self.retry.backoff_initial_seconds.map(|s| s.to_string())
            }
            crate::OPTION_RETRY_BACKOFF_MAX_SECONDS => {
                self.retry.backoff_max_seconds.map(|s| s.to_string())
            }
            crate::OPTION_RETRY_BACKOFF_MULTIPLIER => {
                self.retry.backoff_multiplier.map(|m| m.to_string())
            }
            _ => None,
        };
        value.ok_or_else(|| crate::error::option_not_set(key))
    }
}

#[cfg(test)]
mod tests;

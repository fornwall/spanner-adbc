//! Request priority, tags and commit options. See `docs/options.md` for the option contract.
//! Internal metadata queries inherit priority but omit tags, which attribute user statements.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use adbc_core::error::Result;
use adbc_core::options::OptionValue;
use google_cloud_spanner::builder::{
    BatchDmlBuilder, BatchWriteTransactionBuilder, TransactionRunnerBuilder,
    WriteOnlyTransactionBuilder,
};
use google_cloud_spanner::model::request_options::Priority;
use google_cloud_spanner::statement::StatementBuilder;

use crate::error::invalid_argument;
use crate::options::{RawParsed, bool_option_unsettable, non_empty_string_option};
use crate::staleness::parse_duration;

/// Spanner caps `max_commit_delay` at 500 milliseconds (values above are rejected server-side); we
/// validate the same bound at set time for a clean `InvalidArguments` instead of a commit error.
const MAX_COMMIT_DELAY_CAP: Duration = Duration::from_millis(500);

fn parse_priority(value: &str) -> Result<Priority> {
    match value {
        "low" => Ok(Priority::Low),
        "medium" => Ok(Priority::Medium),
        "high" => Ok(Priority::High),
        other => Err(invalid_argument(format!(
            "unknown request priority {other:?}; expected \"low\", \"medium\" or \"high\" \
             (or an empty string to unset)"
        ))),
    }
}

// Client request builders share setters but no common trait.
macro_rules! apply_to_request_builder {
    ($(#[$attr:meta])* $name:ident($builder:ty)) => {
        $(#[$attr])*
        #[must_use]
        pub(crate) fn $name(&self, mut builder: $builder) -> $builder {
            if let Some(priority) = self.priority.parsed() {
                builder = builder.set_priority(priority.clone());
            }
            if let Some(tag) = &self.request_tag {
                builder = builder.set_request_tag(tag.as_str());
            }
            builder
        }
    };
}

// The read/write and write-only builders share these commit options.
macro_rules! apply_to_commit_builder {
    ($(#[$attr:meta])* $name:ident($builder:ty)) => {
        $(#[$attr])*
        #[must_use]
        pub(crate) fn $name(&self, mut builder: $builder) -> $builder {
            if let Some(priority) = self.priority.parsed() {
                builder = builder.set_commit_priority(priority.clone());
            }
            if let Some(tag) = &self.transaction_tag {
                builder = builder.set_transaction_tag(tag.as_str());
            }
            if let Some(delay) = self.commit_delay() {
                builder = builder.set_max_commit_delay(delay);
            }
            if self.return_commit_stats {
                builder = builder.set_return_commit_stats(true);
            }
            if self.exclude_txn_from_change_streams {
                builder = builder.set_exclude_txn_from_change_streams(true);
            }
            builder
        }
    };
}

/// The request priority / tag configuration held by a connection or statement.
///
/// A connection's value is cloned into each statement it creates, which may then override the
/// priority and request tag; the transaction tag stays connection-level.
#[derive(Debug, Clone, Default)]
pub(crate) struct RequestConfig {
    /// Parsed `spanner.request.priority`, with the raw option string kept for `get_option`
    /// round-trip (`None` leaves the client/service default).
    priority: RawParsed<Priority>,
    /// Raw `spanner.request.tag` value, when set.
    request_tag: Option<String>,
    /// Raw `spanner.transaction.tag` value, when set (connection-level only).
    transaction_tag: Option<String>,
    /// Parsed `spanner.commit.max_delay`, with the raw option string kept for `get_option`
    /// round-trip. Applied as the commit delay wherever a read/write commit is built.
    max_commit_delay: RawParsed<Duration>,
    /// `spanner.commit_stats`: whether to request Spanner return commit statistics on the read/write
    /// commits the driver builds. `false` (the default) leaves them off.
    return_commit_stats: bool,
    /// `spanner.transaction.exclude_from_change_streams`: whether to exclude this transaction's
    /// writes from change-stream capture. `false` (the default) records writes as normal.
    exclude_txn_from_change_streams: bool,
}

impl RequestConfig {
    /// Handle a `set_option` for `spanner.request.priority`. An empty value unsets it.
    pub(crate) fn set_priority(&mut self, value: OptionValue) -> Result<()> {
        self.priority
            .set(value, crate::OPTION_REQUEST_PRIORITY, parse_priority)
    }

    /// Handle a `set_option` for `spanner.request.tag`. An empty value unsets it.
    pub(crate) fn set_request_tag(&mut self, value: OptionValue) -> Result<()> {
        self.request_tag = non_empty_string_option(value, crate::OPTION_REQUEST_TAG)?;
        Ok(())
    }

    /// Handle a `set_option` for `spanner.transaction.tag`. An empty value unsets it.
    pub(crate) fn set_transaction_tag(&mut self, value: OptionValue) -> Result<()> {
        self.transaction_tag = non_empty_string_option(value, crate::OPTION_TRANSACTION_TAG)?;
        Ok(())
    }

    /// Handle a `set_option` for `spanner.commit.max_delay`. An empty value unsets it; a malformed
    /// value or one outside `0..=500ms` is rejected with `InvalidArguments`.
    pub(crate) fn set_max_commit_delay(&mut self, value: OptionValue) -> Result<()> {
        self.max_commit_delay.set(
            value,
            crate::OPTION_MAX_COMMIT_DELAY,
            parse_max_commit_delay,
        )
    }

    /// Handle a `set_option` for `spanner.commit_stats`. An empty string unsets it (back to the
    /// default of not requesting stats); otherwise a boolean string (exactly `true`/`false`).
    pub(crate) fn set_commit_stats(&mut self, value: OptionValue) -> Result<()> {
        self.return_commit_stats =
            bool_option_unsettable(value, &format!("option {}", crate::OPTION_COMMIT_STATS))?;
        Ok(())
    }

    /// The canonical `spanner.commit_stats` value (`"true"`/`"false"`), for `get_option` round-trip.
    /// Always reported (the effective boolean), like the other plain-boolean statement options.
    pub(crate) fn commit_stats_string(&self) -> &'static str {
        if self.return_commit_stats {
            "true"
        } else {
            "false"
        }
    }

    /// Handle a `set_option` for `spanner.transaction.exclude_from_change_streams`. An empty string
    /// unsets it (back to the default of *not* excluding); otherwise a boolean string (exactly
    /// `true`/`false`).
    pub(crate) fn set_exclude_txn_from_change_streams(&mut self, value: OptionValue) -> Result<()> {
        self.exclude_txn_from_change_streams = bool_option_unsettable(
            value,
            &format!("option {}", crate::OPTION_EXCLUDE_TXN_FROM_CHANGE_STREAMS),
        )?;
        Ok(())
    }

    /// The canonical `spanner.transaction.exclude_from_change_streams` value (`"true"`/`"false"`),
    /// for `get_option` round-trip. Always reported (the effective boolean).
    pub(crate) fn exclude_txn_from_change_streams_string(&self) -> &'static str {
        if self.exclude_txn_from_change_streams {
            "true"
        } else {
            "false"
        }
    }

    /// The `spanner.request.priority` value, for `get_option` round-trip. The parser accepts only
    /// the canonical spellings, so the stored raw value *is* the canonical one.
    pub(crate) fn priority_string(&self) -> Option<&str> {
        self.priority.raw()
    }

    /// The raw `spanner.request.tag` value, for `get_option` round-trip.
    pub(crate) fn request_tag_string(&self) -> Option<&str> {
        self.request_tag.as_deref()
    }

    /// The raw `spanner.transaction.tag` value, for `get_option` round-trip.
    pub(crate) fn transaction_tag_string(&self) -> Option<&str> {
        self.transaction_tag.as_deref()
    }

    /// The raw `spanner.commit.max_delay` value, for `get_option` round-trip.
    pub(crate) fn max_commit_delay_string(&self) -> Option<&str> {
        self.max_commit_delay.raw()
    }

    /// The commit delay, validated to `0..=500ms` at set time.
    fn commit_delay(&self) -> Option<Duration> {
        self.max_commit_delay.parsed().copied()
    }

    apply_to_request_builder! {
        /// Apply the priority and request tag to a statement builder (queries and DML alike).
        apply_to_statement(StatementBuilder)
    }

    /// Apply priority without attributing internal metadata queries to user tags.
    #[must_use]
    pub(crate) fn apply_priority_to_statement(
        &self,
        builder: StatementBuilder,
    ) -> StatementBuilder {
        match self.priority.parsed() {
            Some(priority) => builder.set_priority(priority.clone()),
            None => builder,
        }
    }

    apply_to_request_builder! {
        /// Apply the priority and request tag to an `ExecuteBatchDml` batch builder. The batch
        /// request carries a single request-level `RequestOptions`, so both apply to the batch as a
        /// whole (the runner's commit priority, from [`Self::apply_to_runner`], still covers the
        /// transaction's commit).
        apply_to_batch_dml(BatchDmlBuilder)
    }

    /// Apply BatchWrite options. The service ignores request tags on this path, and the
    /// builder has no commit-delay or commit-stats setters.
    #[must_use]
    pub(crate) fn apply_to_batch_write(
        &self,
        mut builder: BatchWriteTransactionBuilder,
    ) -> BatchWriteTransactionBuilder {
        if let Some(priority) = self.priority.parsed() {
            builder = builder.set_priority(priority.clone());
        }
        if let Some(tag) = &self.transaction_tag {
            builder = builder.set_transaction_tag(tag.as_str());
        }
        if self.exclude_txn_from_change_streams {
            builder = builder.set_exclude_txn_from_change_streams(true);
        }
        builder
    }

    apply_to_commit_builder! {
        /// Apply the commit priority, transaction tag and commit delay to a read/write transaction
        /// runner builder.
        apply_to_runner(TransactionRunnerBuilder)
    }

    apply_to_commit_builder! {
        /// Apply the commit priority, transaction tag and commit delay to a write-only transaction
        /// builder (the mutation-based bulk-ingest commit path).
        apply_to_write_only(WriteOnlyTransactionBuilder)
    }
}

/// The latest mutation count returned by a commit with statistics enabled.
/// Connections and statements each own a cell, shared with their execution paths.
#[derive(Debug, Clone, Default)]
pub(crate) struct CommitStats {
    mutation_count: Arc<Mutex<Option<i64>>>,
}

impl CommitStats {
    /// Record a commit's mutation count when the server returned commit statistics. A `None`
    /// (stats not requested, or requested but not returned) leaves any prior value untouched. When
    /// several commits run (e.g. a chunked bulk ingest), the most recent one wins.
    pub(crate) fn record(&self, count: Option<i64>) {
        if let Some(count) = count {
            *self.mutation_count.lock().unwrap() = Some(count);
        }
    }

    /// The most recent recorded mutation count, or `None` if no commit with stats has run.
    pub(crate) fn mutation_count(&self) -> Option<i64> {
        *self.mutation_count.lock().unwrap()
    }
}

/// Parse a `spanner.commit.max_delay` value: a duration (the staleness duration grammar — `s`
/// default, `ms`, `us`/`µs`, `ns`, `m`, `h`) that must fall within Spanner's `0..=500ms` range.
fn parse_max_commit_delay(value: &str) -> Result<Duration> {
    let duration = parse_duration(value)?;
    if duration > MAX_COMMIT_DELAY_CAP {
        return Err(invalid_argument(format!(
            "spanner.commit.max_delay {value:?} exceeds Spanner's maximum of 500ms"
        )));
    }
    Ok(duration)
}

#[cfg(test)]
mod tests;

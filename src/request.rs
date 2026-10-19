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

/// Spanner rejects larger values at commit; checking at set time gives a clearer error.
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

/// Request priority, tags and commit options for a connection or statement. Statements may
/// override everything except the transaction tag.
#[derive(Debug, Clone, Default)]
pub(crate) struct RequestConfig {
    priority: RawParsed<Priority>,
    request_tag: Option<String>,
    transaction_tag: Option<String>,
    max_commit_delay: RawParsed<Duration>,
    return_commit_stats: bool,
    exclude_txn_from_change_streams: bool,
}

impl RequestConfig {
    pub(crate) fn set_priority(&mut self, value: OptionValue) -> Result<()> {
        self.priority
            .set(value, crate::OPTION_REQUEST_PRIORITY, parse_priority)
    }

    pub(crate) fn set_request_tag(&mut self, value: OptionValue) -> Result<()> {
        self.request_tag = non_empty_string_option(value, crate::OPTION_REQUEST_TAG)?;
        Ok(())
    }

    pub(crate) fn set_transaction_tag(&mut self, value: OptionValue) -> Result<()> {
        self.transaction_tag = non_empty_string_option(value, crate::OPTION_TRANSACTION_TAG)?;
        Ok(())
    }

    pub(crate) fn set_max_commit_delay(&mut self, value: OptionValue) -> Result<()> {
        self.max_commit_delay.set(
            value,
            crate::OPTION_MAX_COMMIT_DELAY,
            parse_max_commit_delay,
        )
    }

    pub(crate) fn set_commit_stats(&mut self, value: OptionValue) -> Result<()> {
        self.return_commit_stats =
            bool_option_unsettable(value, &format!("option {}", crate::OPTION_COMMIT_STATS))?;
        Ok(())
    }

    pub(crate) fn commit_stats_string(&self) -> &'static str {
        if self.return_commit_stats {
            "true"
        } else {
            "false"
        }
    }

    pub(crate) fn set_exclude_txn_from_change_streams(&mut self, value: OptionValue) -> Result<()> {
        self.exclude_txn_from_change_streams = bool_option_unsettable(
            value,
            &format!("option {}", crate::OPTION_EXCLUDE_TXN_FROM_CHANGE_STREAMS),
        )?;
        Ok(())
    }

    pub(crate) fn exclude_txn_from_change_streams_string(&self) -> &'static str {
        if self.exclude_txn_from_change_streams {
            "true"
        } else {
            "false"
        }
    }

    pub(crate) fn priority_string(&self) -> Option<&str> {
        self.priority.raw()
    }

    pub(crate) fn request_tag_string(&self) -> Option<&str> {
        self.request_tag.as_deref()
    }

    pub(crate) fn transaction_tag_string(&self) -> Option<&str> {
        self.transaction_tag.as_deref()
    }

    pub(crate) fn max_commit_delay_string(&self) -> Option<&str> {
        self.max_commit_delay.raw()
    }

    fn commit_delay(&self) -> Option<Duration> {
        self.max_commit_delay.parsed().copied()
    }

    apply_to_request_builder! {
        /// Apply the priority and request tag to a query or DML statement.
        apply_to_statement(StatementBuilder)
    }

    /// Apply only the priority, for internal metadata queries, which must not carry user tags.
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
        /// Apply the priority and request tag to a whole `ExecuteBatchDml` batch.
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
        /// Apply the commit options to a read/write transaction runner.
        apply_to_runner(TransactionRunnerBuilder)
    }

    apply_to_commit_builder! {
        /// Apply the commit options to a write-only transaction, used by bulk ingest.
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
    /// Record a commit's mutation count. `None`, when no statistics were returned, keeps the
    /// previous value.
    pub(crate) fn record(&self, count: Option<i64>) {
        if let Some(count) = count {
            *self.mutation_count.lock().unwrap() = Some(count);
        }
    }

    pub(crate) fn mutation_count(&self) -> Option<i64> {
        *self.mutation_count.lock().unwrap()
    }
}

/// Parse a `spanner.commit.max_delay` duration of at most 500ms.
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

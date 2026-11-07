//! DML execution: manual-transaction buffering, autocommit, `THEN RETURN` and partitioned DML.

use std::sync::Arc;

use adbc_core::error::{Error, Result};
use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use google_cloud_spanner::statement::Statement as SpannerSql;
use google_cloud_spanner::transaction::{BeginTransactionOption, ReadWriteTransaction};

use super::{SpannerStatement, bound_statements, check_partitioned_dml};
use crate::connection::{lock_txn, runner_builder};
use crate::conversion::rows_to_batch;
use crate::error::{
    annotate, commit_outcome_unknown, from_spanner, invalid_argument, invalid_state,
    is_mutation_limit_exceeded, unsupported,
};
use crate::runtime::block_on_cancellable;
use crate::timeout::with_timeout;

/// The result of routing DML through [`SpannerStatement::run_dml`].
pub(super) enum DmlOutcome {
    /// Plain DML via `ExecuteBatchDml`: the affected-row count, or `None` when the statements
    /// were buffered for a manual-transaction commit.
    Plain(Option<i64>),
    /// DML with `THEN RETURN`: the returned rows and the affected-row count from the stats.
    Returning {
        batches: Vec<RecordBatch>,
        schema: SchemaRef,
        affected: i64,
    },
}

impl SpannerStatement {
    fn build_dml_statements(&self, parts: &[&str]) -> Result<Vec<SpannerSql>> {
        if !self.bound.is_empty() {
            return match parts {
                [] => Ok(Vec::new()),
                [sql] => {
                    bound_statements(sql, self.sql_builder(sql), &self.bound, self.bind_by_name)?
                        .collect()
                }
                _ => Err(invalid_argument(
                    "bound parameters require a single DML statement; execute each statement separately",
                )),
            };
        }
        Ok(parts.iter().map(|s| self.sql_builder(s).build()).collect())
    }

    /// Buffer manual DML for commit, or execute an autocommit batch atomically and report its count.
    fn run_or_buffer(&self, statements: Vec<SpannerSql>) -> Result<Option<i64>> {
        {
            let mut txn = lock_txn(&self.txn);
            if !txn.autocommit() {
                txn.buffer_dml(statements)?;
                return Ok(None);
            }
        }
        // Autocommit DML is the transaction's last request and carries no buffered mutations.
        let count = crate::connection::run_batch_txn(
            &self.runtime,
            &self.client,
            &self.cancel.current(),
            &self.config,
            &statements,
            &[],
            true,
        )
        .map_err(note_dml_failure)?;
        Ok(Some(count))
    }

    /// Execute `THEN RETURN` through `ExecuteStreamingSql`; `ExecuteBatchDml` cannot return rows.
    /// Drain results before commit and preserve client errors so transaction aborts retry.
    fn execute_returning_dml(
        &self,
        statements: Vec<SpannerSql>,
    ) -> Result<(Vec<RecordBatch>, SchemaRef, i64)> {
        let client = self.client.clone();
        let isolation = self.config.isolation.clone();
        let request = self.config.request.clone();
        let retry = self.config.retry;
        let precision = self.config.timestamp_precision;
        // The update timeout covers the whole transaction.
        let update_timeout = self.config.timeouts.update_timeout();
        let transaction = async move {
            let mut runner = runner_builder(&client, isolation, &request, retry);
            if statements.len() > 1 {
                // Omni can return a different metadata transaction ID for each streaming DML.
                // Explicit begin pins the original ID across the batch and its atomic commit.
                runner = runner.set_begin_transaction_option(BeginTransactionOption::ExplicitBegin);
            }
            let outcome = runner
                .build()
                .run(move |transaction: ReadWriteTransaction| {
                    let statements = statements.clone();
                    async move {
                        let mut results = Vec::with_capacity(statements.len());
                        for statement in statements {
                            let mut result_set = transaction.execute_query(statement).await?;
                            let mut rows = Vec::new();
                            while let Some(row) = result_set.next().await {
                                rows.push(row?);
                            }
                            // Stats (including the affected-row count) arrive with the end of
                            // the stream. `THEN RETURN` yields one row per affected row, so the
                            // drained row count is the fallback.
                            let count = result_set.update_count().unwrap_or(rows.len() as i64);
                            // Failing here rolls back; after the commit it would report applied
                            // DML as failed.
                            let batch = rows_to_batch(result_set.metadata(), &rows, precision)
                                .map_err(google_cloud_spanner::Error::deser)?;
                            results.push((batch, count));
                        }
                        Ok(results)
                    }
                })
                .await
                .map_err(from_spanner)?;
            let mutation_count = outcome
                .commit_response
                .commit_stats
                .as_ref()
                .map(|stats| stats.mutation_count);
            Ok::<_, Error>((outcome.result, mutation_count))
        };
        let (results, mutation_count) = block_on_cancellable(
            &self.runtime,
            &self.cancel.current(),
            with_timeout(
                update_timeout,
                crate::OPTION_RPC_TIMEOUT_UPDATE,
                transaction,
            ),
        )
        .map_err(note_dml_failure)?;
        self.config.commit_stats.record(mutation_count);

        let schema = results
            .first()
            .map_or_else(|| Arc::new(Schema::empty()), |(batch, _)| batch.schema());
        let affected = results.iter().map(|(_, count)| count).sum();
        let batches = results.into_iter().map(|(batch, _)| batch).collect();
        Ok((batches, schema, affected))
    }

    /// Enforce writable mode and route plain, returning or partitioned DML.
    /// Manual transactions cannot return rows because they defer DML until commit.
    pub(super) fn run_dml(&self, sql: &str, parts: &[&str]) -> Result<DmlOutcome> {
        if self.config.is_read_only() {
            return Err(invalid_state(
                "cannot execute DML: the connection is read-only",
            ));
        }
        if self.dml_partitioned {
            return Ok(DmlOutcome::Plain(Some(
                self.run_partitioned_dml(sql, parts)?,
            )));
        }
        if !crate::sql::is_dml_returning(sql) {
            let statements = self.build_dml_statements(parts)?;
            return Ok(DmlOutcome::Plain(self.run_or_buffer(statements)?));
        }
        if !lock_txn(&self.txn).autocommit() {
            return Err(invalid_state(
                "DML with THEN RETURN cannot run in a manual transaction: buffered DML is applied \
                 via ExecuteBatchDml on commit, which does not support THEN RETURN. Re-enable \
                 autocommit to run it",
            ));
        }
        if self.bound.is_empty() && parts.len() > 1 {
            return Err(unsupported(
                "THEN RETURN is not supported in a `;`-separated DML batch, whose statements' \
                 rows can't share one result: execute each statement separately",
            ));
        }
        let statements = self.build_dml_statements(parts)?;
        // Zero bound rows leave nothing to execute, as on the batch DML path; the runner would
        // still begin and commit an empty transaction.
        if statements.is_empty() {
            return Ok(DmlOutcome::Returning {
                batches: Vec::new(),
                schema: Arc::new(Schema::empty()),
                affected: 0,
            });
        }
        let (batches, schema, affected) = self.execute_returning_dml(statements)?;
        Ok(DmlOutcome::Returning {
            batches,
            schema,
            affected,
        })
    }

    /// Execute one partitioned DML statement and return its lower-bound affected-row count.
    fn run_partitioned_dml(&self, sql: &str, parts: &[&str]) -> Result<i64> {
        let bound_rows = self.bound.iter().map(RecordBatch::num_rows).sum();
        check_partitioned_dml(sql, lock_txn(&self.txn).autocommit(), bound_rows)?;
        let mut statements = self.build_dml_statements(parts)?;
        // Zero bound rows leave nothing to execute, as on the batch DML path.
        let Some(statement) = statements.pop() else {
            return Ok(0);
        };
        let exclude_from_change_streams = self.config.request.exclude_txn_from_change_streams;
        let client = self.client.clone();
        block_on_cancellable(
            &self.runtime,
            &self.cancel.current(),
            with_timeout(
                self.config.timeouts.update_timeout(),
                crate::OPTION_RPC_TIMEOUT_UPDATE,
                async move {
                    let transaction = client
                        .partitioned_dml_transaction()
                        .set_exclude_txn_from_change_streams(exclude_from_change_streams)
                        .build();
                    transaction
                        .execute_update(statement)
                        .await
                        .map_err(from_spanner)
                },
            ),
        )
    }
}

/// Explain the two autocommit DML failures whose remedy isn't obvious: a commit whose outcome is
/// unknown, and Spanner's per-transaction mutation limit, whose message doesn't know the driver's
/// ways past it. Every bound row and `;`-separated statement shares one transaction.
fn note_dml_failure(error: Error) -> Error {
    let note = if commit_outcome_unknown(&error) {
        "the transaction may still have been committed, so executing it again could apply it twice"
    } else if is_mutation_limit_exceeded(&error) {
        "all bound rows and statements of one execution run in one transaction: execute fewer at \
         a time, set spanner.dml.partitioned to true for a large UPDATE or DELETE, or load inserts \
         with bulk ingest, which commits in chunks"
    } else {
        return error;
    };
    annotate(error, |message| format!("{message} ({note})"))
}

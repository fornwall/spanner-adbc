//! DML execution: manual-transaction buffering, autocommit, `THEN RETURN` and partitioned DML.

use std::sync::Arc;

use adbc_core::error::{Error, Result};
use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use google_cloud_spanner::statement::Statement as SpannerSql;
use google_cloud_spanner::transaction::{BeginTransactionOption, ReadWriteTransaction};

use super::{SpannerStatement, check_all_dml_batch, check_partitioned_dml};
use crate::bind;
use crate::connection::{lock_txn, runner_builder};
use crate::error::{from_spanner, invalid_argument, invalid_state, not_implemented};
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
    /// Bind each row to the single DML statement in `sql`. Directed reads apply only to read-only
    /// queries, so this uses `sql_builder`.
    fn build_bound_statements(&self, sql: &str) -> Result<Vec<SpannerSql>> {
        let parts = crate::sql::split_statements(sql);
        check_all_dml_batch(&parts)?;
        if parts.len() > 1 {
            return Err(invalid_argument(
                "bound parameters require a single DML statement; execute each statement separately",
            ));
        }
        let Some(sql) = parts.first() else {
            return Ok(Vec::new());
        };
        let builder = self.sql_builder(sql);
        let mut statements = Vec::new();
        for batch in &self.bound {
            if batch.num_rows() == 0 {
                continue;
            }
            // Resolving names lexes the SQL, so do it once per batch.
            let names = bind::resolve_parameter_names(sql, batch, self.bind_by_name)?;
            for row in 0..batch.num_rows() {
                statements.push(bind::bind_params(builder.clone(), &names, batch, row)?.build());
            }
        }
        Ok(statements)
    }

    /// Bind a single statement or split an unbound DML batch, rejecting mixed SQL before buffering.
    fn build_dml_statements(&self, sql: &str) -> Result<Vec<SpannerSql>> {
        if !self.bound.is_empty() {
            return self.build_bound_statements(sql);
        }
        let statements = crate::sql::split_statements(sql);
        check_all_dml_batch(&statements)?;
        Ok(statements
            .into_iter()
            .map(|s| self.sql_builder(&s).build())
            .collect())
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
        )?;
        Ok(Some(count))
    }

    /// Execute `THEN RETURN` through `ExecuteSql`; `ExecuteBatchDml` cannot return rows.
    /// Drain results before commit and preserve client errors so transaction aborts retry.
    fn execute_returning_dml(
        &self,
        statements: Vec<SpannerSql>,
    ) -> Result<(Vec<RecordBatch>, SchemaRef, i64)> {
        let client = self.client.clone();
        let isolation = self.config.isolation.clone();
        let request = self.config.request.clone();
        let retry = self.config.retry;
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
                            results.push((result_set.metadata().clone(), rows, count));
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
        )?;
        self.config.commit_stats.record(mutation_count);

        let mut schema = None;
        let mut batches = Vec::with_capacity(results.len());
        let mut affected = 0i64;
        for (metadata, rows, count) in &results {
            let (sch, batch) = crate::conversion::rows_to_batch(
                Some(metadata),
                rows,
                self.config.timestamp_precision,
            )?;
            schema.get_or_insert(sch);
            batches.push(batch);
            affected += count;
        }
        let schema = schema.unwrap_or_else(|| Arc::new(Schema::empty()));
        Ok((batches, schema, affected))
    }

    /// Enforce writable mode and route plain, returning or partitioned DML.
    /// Manual transactions cannot return rows because they defer DML until commit.
    pub(super) fn run_dml(&self, sql: &str) -> Result<DmlOutcome> {
        if self.config.is_read_only() {
            return Err(invalid_state(
                "cannot execute DML: the connection is read-only",
            ));
        }
        if self.dml_partitioned {
            return Ok(DmlOutcome::Plain(Some(self.run_partitioned_dml(sql)?)));
        }
        if !crate::sql::is_dml_returning(sql) {
            let statements = self.build_dml_statements(sql)?;
            return Ok(DmlOutcome::Plain(self.run_or_buffer(statements)?));
        }
        if !lock_txn(&self.txn).autocommit() {
            return Err(invalid_state(
                "DML with THEN RETURN cannot run in a manual transaction: buffered DML is applied \
                 via ExecuteBatchDml on commit, which does not support THEN RETURN. Re-enable \
                 autocommit to run it",
            ));
        }
        let statements = if self.bound.is_empty() {
            let parts = crate::sql::split_statements(sql);
            if parts.len() > 1 {
                return Err(not_implemented(
                    "THEN RETURN in a multi-statement (`;`-separated) DML batch",
                ));
            }
            parts
                .into_iter()
                .map(|s| self.sql_builder(&s).build())
                .collect()
        } else {
            self.build_bound_statements(sql)?
        };
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
    fn run_partitioned_dml(&self, sql: &str) -> Result<i64> {
        let bound_rows = self.bound.iter().map(RecordBatch::num_rows).sum();
        check_partitioned_dml(sql, lock_txn(&self.txn).autocommit(), bound_rows)?;
        let mut statements = self.build_dml_statements(sql)?;
        // Zero bound rows leave nothing to execute, as on the batch DML path.
        let Some(statement) = statements.pop() else {
            return Ok(0);
        };
        let exclude_from_change_streams =
            self.config.request.exclude_txn_from_change_streams_string() == "true";
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

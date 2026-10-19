//! Read/write transaction execution: isolation levels, batch DML and write-only commits.

use adbc_core::error::{Error, Result};
use adbc_core::options::OptionValue;
use google_cloud_spanner::builder::{BatchDmlBuilder, TransactionRunnerBuilder};
use google_cloud_spanner::client::DatabaseClient;
use google_cloud_spanner::model::transaction_options::IsolationLevel;
use google_cloud_spanner::mutation::Mutation;
use google_cloud_spanner::statement::Statement as SpannerSql;
use google_cloud_spanner::transaction::ReadWriteTransaction;

use crate::error::{from_spanner, invalid_argument};
use crate::options::SharedConfig;
use crate::request::RequestConfig;
use crate::retry::RetryConfig;
use crate::runtime::{CancelSignal, SharedRuntime, block_on_cancellable};
use crate::timeout::with_timeout;

/// Apply an explicit isolation level; unspecified leaves Spanner's SERIALIZABLE default.
#[must_use]
pub(super) fn apply_isolation(
    builder: TransactionRunnerBuilder,
    isolation: IsolationLevel,
) -> TransactionRunnerBuilder {
    match isolation {
        IsolationLevel::Unspecified => builder,
        level => builder.set_isolation_level(level),
    }
}

/// Configure a read/write transaction runner with isolation, request options and retry bounds.
pub(crate) fn runner_builder(
    client: &DatabaseClient,
    isolation: IsolationLevel,
    request: &RequestConfig,
    retry: RetryConfig,
) -> TransactionRunnerBuilder {
    retry.apply_to_runner(
        request.apply_to_runner(apply_isolation(client.read_write_transaction(), isolation)),
    )
}

/// Map ADBC isolation levels to Spanner's SERIALIZABLE or REPEATABLE_READ.
/// Spanner's REPEATABLE_READ provides snapshot isolation, so weaker levels are promoted to it.
/// SERIALIZABLE is externally consistent and satisfies linearizable requests. Getters report the
/// effective level, or `default` when no level is sent.
///
/// REPEATABLE_READ detects write-write conflicts only: DML that reads rows it does not write can
/// exhibit write skew, even in a single autocommit statement.
pub(super) fn parse_isolation_level(value: OptionValue) -> Result<IsolationLevel> {
    use adbc_core::constants::*;
    let OptionValue::String(s) = value else {
        return Err(invalid_argument(
            "expected a string isolation-level option value",
        ));
    };
    match s.as_str() {
        ADBC_OPTION_ISOLATION_LEVEL_DEFAULT => Ok(IsolationLevel::Unspecified),
        ADBC_OPTION_ISOLATION_LEVEL_SERIALIZABLE => Ok(IsolationLevel::Serializable),
        ADBC_OPTION_ISOLATION_LEVEL_REPEATABLE_READ => Ok(IsolationLevel::RepeatableRead),
        // A native match, not a promotion: REPEATABLE_READ is snapshot isolation.
        ADBC_OPTION_ISOLATION_LEVEL_SNAPSHOT => Ok(IsolationLevel::RepeatableRead),
        // Promote to the weakest supported level that satisfies the request.
        ADBC_OPTION_ISOLATION_LEVEL_READ_UNCOMMITTED
        | ADBC_OPTION_ISOLATION_LEVEL_READ_COMMITTED => Ok(IsolationLevel::RepeatableRead),
        ADBC_OPTION_ISOLATION_LEVEL_LINEARIZABLE => Ok(IsolationLevel::Serializable),
        other => Err(invalid_argument(format!(
            "unknown isolation level {other:?}"
        ))),
    }
}

/// The ADBC option value for a stored isolation level.
pub(super) fn isolation_to_adbc_string(isolation: &IsolationLevel) -> &'static str {
    use adbc_core::constants::*;
    match isolation {
        IsolationLevel::Serializable => ADBC_OPTION_ISOLATION_LEVEL_SERIALIZABLE,
        IsolationLevel::RepeatableRead => ADBC_OPTION_ISOLATION_LEVEL_REPEATABLE_READ,
        // Unspecified, and any future variant, sends no level.
        _ => ADBC_OPTION_ISOLATION_LEVEL_DEFAULT,
    }
}

/// Apply DML `statements` and `mutations` atomically in one read/write transaction, returning the
/// statements' total affected-row count.
///
/// The statements run as one `ExecuteBatchDml`; Spanner applies the mutations at commit, after
/// every statement. The runner replays the closure on abort.
///
/// `last_statements` marks the batch as the transaction's last request, letting Spanner commit in
/// the same round trip. Only autocommit DML may pass `true`; a manual commit with buffered
/// mutations has more to apply.
///
/// The update timeout covers the whole transaction, including abort retries. A commit that timed
/// out may still have landed.
pub(crate) fn run_batch_txn(
    runtime: &SharedRuntime,
    client: &DatabaseClient,
    cancel: &CancelSignal,
    config: &SharedConfig,
    statements: &[SpannerSql],
    mutations: &[Mutation],
    last_statements: bool,
) -> Result<i64> {
    if statements.is_empty() && mutations.is_empty() {
        return Ok(0);
    }
    let isolation = config.isolation.clone();
    let request = config.request.clone();
    let retry = config.retry;
    let timeout = config.timeouts.update_timeout();
    let client = client.clone();
    let transaction = async move {
        // Commit priority and transaction tag go on the runner, the request tag on the batch.
        let runner = runner_builder(&client, isolation, &request, retry).build();
        let outcome = runner
            .run(move |transaction: ReadWriteTransaction| {
                let request = request.clone();
                async move {
                    transaction.buffer(mutations.iter().cloned())?;
                    if statements.is_empty() {
                        return Ok(0);
                    }
                    let mut batch = retry
                        .apply_to_batch_dml(request.apply_to_batch_dml(BatchDmlBuilder::new()))
                        .set_last_statements(last_statements);
                    for statement in statements {
                        batch = batch.add_statement(statement.clone());
                    }
                    let counts = transaction.execute_batch_update(batch.build()).await?;
                    Ok(counts.into_iter().sum::<i64>())
                }
            })
            .await
            .map_err(from_spanner)?;
        // Present only when `spanner.commit_stats` is enabled.
        let mutation_count = outcome
            .commit_response
            .commit_stats
            .as_ref()
            .map(|stats| stats.mutation_count);
        Ok::<(i64, Option<i64>), Error>((outcome.result, mutation_count))
    };
    let (count, mutation_count) = block_on_cancellable(
        runtime,
        cancel,
        with_timeout(timeout, crate::OPTION_RPC_TIMEOUT_UPDATE, transaction),
    )?;
    config.commit_stats.record(mutation_count);
    Ok(count)
}

/// Commit `mutations` without DML in one write-only transaction. Used by mutations-only manual
/// commits and by each chunk of an autocommit bulk ingest.
///
/// A read/write runner that replays its commit after an ambiguous transport failure can apply the
/// mutations twice. `write` begins the transaction with a mutation key and retries `ABORTED`
/// internally, so success means the mutations were applied once. Isolation does not apply: the
/// transaction performs no reads, and the builder has no isolation setter.
pub(crate) fn write_mutations_txn(
    runtime: &SharedRuntime,
    client: &DatabaseClient,
    cancel: &CancelSignal,
    config: &SharedConfig,
    mutations: Vec<Mutation>,
) -> Result<()> {
    if mutations.is_empty() {
        return Ok(());
    }
    let request = config.request.clone();
    let retry = config.retry;
    let timeout = config.timeouts.update_timeout();
    let client = client.clone();
    let transaction = async move {
        let response = retry
            .apply_to_write_only(request.apply_to_write_only(client.write_only_transaction()))
            .build()
            .write(mutations)
            .await
            .map_err(from_spanner)?;
        // Present only when `spanner.commit_stats` is enabled.
        Ok::<Option<i64>, Error>(
            response
                .commit_stats
                .as_ref()
                .map(|stats| stats.mutation_count),
        )
    };
    let mutation_count = block_on_cancellable(
        runtime,
        cancel,
        with_timeout(timeout, crate::OPTION_RPC_TIMEOUT_UPDATE, transaction),
    )?;
    config.commit_stats.record(mutation_count);
    Ok(())
}

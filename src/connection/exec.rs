//! Read/write transaction execution: isolation levels, batch DML and write-only commits.

use adbc_core::error::Result;
use adbc_core::options::OptionValue;
use google_cloud_spanner::builder::{BatchDmlBuilder, TransactionRunnerBuilder};
use google_cloud_spanner::client::DatabaseClient;
use google_cloud_spanner::model::transaction_options::IsolationLevel;
use google_cloud_spanner::mutation::Mutation;
use google_cloud_spanner::statement::Statement as SpannerSql;
use google_cloud_spanner::transaction::ReadWriteTransaction;

use crate::error::{from_spanner, invalid_argument};
use crate::options::{SharedConfig, string_option};
use crate::request::RequestConfig;
use crate::retry::RetryConfig;
use crate::runtime::{CancelSignal, SharedRuntime, block_on_cancellable};
use crate::timeout::with_timeout;

/// Configure a read/write transaction runner with isolation, request options and retry bounds.
/// `Unspecified` is the wire default, so it leaves Spanner's SERIALIZABLE default.
pub(crate) fn runner_builder(
    client: &DatabaseClient,
    isolation: IsolationLevel,
    request: &RequestConfig,
    retry: RetryConfig,
) -> TransactionRunnerBuilder {
    let builder = client
        .read_write_transaction()
        .set_isolation_level(isolation);
    retry.apply_to_runner(request.apply_to_runner(builder))
}

/// Map an ADBC isolation level to Spanner's SERIALIZABLE or REPEATABLE_READ, promoting a level
/// Spanner lacks to the weakest one that satisfies it. REPEATABLE_READ is snapshot isolation, and
/// SERIALIZABLE is externally consistent, which satisfies linearizable requests. Getters report
/// the effective level, or `default` when no level is sent.
///
/// REPEATABLE_READ detects write-write conflicts only: DML that reads rows it does not write can
/// exhibit write skew, even in a single autocommit statement.
pub(super) fn parse_isolation_level(value: OptionValue) -> Result<IsolationLevel> {
    use adbc_core::constants::*;
    match string_option(value, ADBC_CONNECTION_OPTION_ISOLATION_LEVEL)?.as_str() {
        ADBC_OPTION_ISOLATION_LEVEL_DEFAULT => Ok(IsolationLevel::Unspecified),
        ADBC_OPTION_ISOLATION_LEVEL_SERIALIZABLE | ADBC_OPTION_ISOLATION_LEVEL_LINEARIZABLE => {
            Ok(IsolationLevel::Serializable)
        }
        ADBC_OPTION_ISOLATION_LEVEL_REPEATABLE_READ
        | ADBC_OPTION_ISOLATION_LEVEL_SNAPSHOT
        | ADBC_OPTION_ISOLATION_LEVEL_READ_COMMITTED
        | ADBC_OPTION_ISOLATION_LEVEL_READ_UNCOMMITTED => Ok(IsolationLevel::RepeatableRead),
        other => Err(invalid_argument(format!(
            "unknown isolation level {other:?} for option {ADBC_CONNECTION_OPTION_ISOLATION_LEVEL}; \
             expected an ADBC isolation constant such as {ADBC_OPTION_ISOLATION_LEVEL_SERIALIZABLE:?}"
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
    // Commit priority and transaction tag go on the runner, the request tag on the batch.
    let batch = (!statements.is_empty()).then(|| {
        let mut batch = config
            .retry
            .apply_to_batch_dml(config.request.apply_to_batch_dml(BatchDmlBuilder::new()))
            .set_last_statements(last_statements);
        for statement in statements {
            batch = batch.add_statement(statement.clone());
        }
        batch.build()
    });
    let runner = runner_builder(
        client,
        config.isolation.clone(),
        &config.request,
        config.retry,
    )
    .build();
    let transaction = async {
        let outcome = runner
            .run(async |transaction: ReadWriteTransaction| {
                transaction.buffer(mutations.iter().cloned())?;
                let Some(batch) = &batch else {
                    return Ok(0);
                };
                let counts = transaction.execute_batch_update(batch.clone()).await?;
                Ok(counts.into_iter().sum::<i64>())
            })
            .await
            .map_err(from_spanner)?;
        // Present only when `spanner.commit_stats` is enabled.
        let stats = outcome.commit_response.commit_stats;
        config
            .commit_stats
            .record(stats.map(|stats| stats.mutation_count));
        Ok(outcome.result)
    };
    block_on_cancellable(
        runtime,
        cancel,
        with_timeout(
            config.timeouts.update_timeout(),
            crate::OPTION_RPC_TIMEOUT_UPDATE,
            transaction,
        ),
    )
}

/// Commit `mutations` without DML in one write-only transaction. Used by mutations-only manual
/// commits and by each chunk of an autocommit bulk ingest.
///
/// A read/write runner that replays its commit after an ambiguous transport failure can apply the
/// mutations twice. `write` begins the transaction with a mutation key and retries `ABORTED`
/// internally, so success means the mutations were applied once. Isolation does not apply: the
/// transaction performs no reads, and the builder has no isolation setter.
///
/// The client converts each mutation as it consumes them, so a caller that must keep its
/// mutations can pass a cloning iterator instead of a cloned buffer.
pub(crate) fn write_mutations_txn<I>(
    runtime: &SharedRuntime,
    client: &DatabaseClient,
    cancel: &CancelSignal,
    config: &SharedConfig,
    mutations: I,
) -> Result<()>
where
    I: IntoIterator<Item = Mutation>,
    I::IntoIter: ExactSizeIterator,
{
    let mutations = mutations.into_iter();
    if mutations.len() == 0 {
        return Ok(());
    }
    let transaction = config
        .retry
        .apply_to_write_only(
            config
                .request
                .apply_to_write_only(client.write_only_transaction()),
        )
        .build();
    let commit = async {
        let response = transaction.write(mutations).await.map_err(from_spanner)?;
        // Present only when `spanner.commit_stats` is enabled.
        config
            .commit_stats
            .record(response.commit_stats.map(|stats| stats.mutation_count));
        Ok(())
    };
    block_on_cancellable(
        runtime,
        cancel,
        with_timeout(
            config.timeouts.update_timeout(),
            crate::OPTION_RPC_TIMEOUT_UPDATE,
            commit,
        ),
    )
}

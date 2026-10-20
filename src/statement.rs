//! [`SpannerStatement`]: an ADBC statement that runs SQL against Spanner and returns Arrow.
//!
//! [`Statement::execute`] and [`Statement::execute_update`] accept any statement. Queries stream
//! from a read-only transaction, DML runs in a read/write transaction or is buffered in a manual
//! one, and DDL goes through the admin API. `execute_update` discards a query's rows, since
//! `adbc.h` allows executing any statement without a result set. `THEN RETURN` DML runs through
//! `ExecuteStreamingSql`, because `ExecuteBatchDml` cannot return rows.

mod dml;
mod ingest;

use dml::DmlOutcome;
pub(crate) use ingest::is_mutation_limit_exceeded;
use ingest::{check_target_catalog, ingest_batch_write_option, ingest_mode_option};

use std::collections::BTreeMap;
use std::sync::Arc;

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::{IngestMode, OptionStatement, OptionValue};
use adbc_core::{CancelHandle, Optionable, PartitionedResult, Statement};
use arrow_array::{RecordBatch, RecordBatchIterator, RecordBatchReader};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use google_cloud_lro::Poller as _;
use google_cloud_spanner::client::DatabaseClient;
use google_cloud_spanner::model::PartitionOptions;
use google_cloud_spanner::model::execute_sql_request::QueryMode;
use google_cloud_spanner::statement::{Statement as SpannerSql, StatementBuilder};
use google_cloud_spanner::transaction::{MultiUseReadOnlyTransaction, ReadWriteTransaction};

use crate::bind;
use crate::connection::{SharedTxn, TxnKind, lock_txn, runner_builder};
use crate::conversion::{
    BoundStatementSource, result_set_to_batch, stream_bound_query, stream_query,
};
use crate::driver::SharedDatabaseAdmin;
use crate::error::{
    err, from_spanner, invalid_argument, invalid_state, option_not_set, unknown_option, unsupported,
};
use crate::options::{SharedConfig, bool_option, impl_typed_option_getters};
use crate::runtime::{CancelSlot, SharedRuntime, SlotCancelHandle, block_on_cancellable};
use crate::timeout::with_timeout;

/// Default for [`OPTION_ROWS_PER_BATCH`](crate::OPTION_ROWS_PER_BATCH). Also used by
/// `Connection::read_partition`, which has no per-statement batch size.
pub(crate) const DEFAULT_ROWS_PER_BATCH: usize = 8192;

/// The reader [`Statement::execute`] returns.
type ResultReader = Box<dyn RecordBatchReader + Send + 'static>;

/// Builds each statement of a multi-row bound query only when the reader asks for it, so a large
/// `executemany` SELECT doesn't hold one statement per row in memory.
struct LazyBoundStatements {
    /// Fully configured read-only builder, cloned per row before binding.
    base_builder: StatementBuilder,
    /// Resolved parameter names for each non-empty bound batch.
    groups: Vec<(Vec<String>, RecordBatch)>,
    group: usize,
    /// Next row to bind within `groups[group]`.
    row: usize,
}

impl BoundStatementSource for LazyBoundStatements {
    fn next_statement(&mut self) -> Option<Result<SpannerSql>> {
        loop {
            let (names, batch) = self.groups.get(self.group)?;
            if self.row >= batch.num_rows() {
                self.group += 1;
                self.row = 0;
                continue;
            }
            let row = self.row;
            self.row += 1;
            return Some(
                bind::bind_params(self.base_builder.clone(), names, batch, row).map(|b| b.build()),
            );
        }
    }
}

/// An ADBC statement bound to a Spanner [`DatabaseClient`].
#[derive(Debug)]
pub struct SpannerStatement {
    runtime: SharedRuntime,
    client: DatabaseClient,
    database: String,
    /// Lazily built admin client for DDL, shared by every handle on the database.
    admin: SharedDatabaseAdmin,
    /// Options inherited from the connection at creation and overridable per statement.
    config: SharedConfig,
    txn: SharedTxn,
    sql: Option<String>,
    /// Parameter rows or bulk-ingest data from `bind` or `bind_stream`.
    bound: Vec<RecordBatch>,
    /// Schema of a `bind_stream` that yielded no batches; used only by bulk ingest. Kept out of
    /// `bound` so an empty stream isn't mistaken for bound parameters.
    ingest_schema: Option<SchemaRef>,
    target_table: Option<String>,
    /// Named schema of the ingest target; `None` or empty selects the default schema.
    target_db_schema: Option<String>,
    /// Only the connection's database is accepted; stored so `get_option` round-trips it.
    target_catalog: Option<String>,
    /// `None` means the spec default, `create`.
    ingest_mode: Option<IngestMode>,
    /// Send autocommit ingest chunks through BatchWrite instead of write-only transactions.
    ingest_batch_write: bool,
    /// Run DML as Partitioned DML instead of in a read/write transaction.
    dml_partitioned: bool,
    /// Bind columns to parameters by name instead of by position.
    bind_by_name: bool,
    rows_per_batch: usize,
    data_boost: bool,
    /// Each operation starts a fresh cancel signal here. A cancel latches the current signal, so a
    /// cancelled streaming reader stays cancelled; handles from `get_cancel_handle` share the slot.
    cancel: Arc<CancelSlot>,
}

impl SpannerStatement {
    pub(crate) fn new(
        runtime: SharedRuntime,
        client: DatabaseClient,
        database: String,
        admin: SharedDatabaseAdmin,
        config: SharedConfig,
        txn: SharedTxn,
    ) -> Self {
        Self {
            runtime,
            client,
            database,
            admin,
            config,
            txn,
            sql: None,
            bound: Vec::new(),
            ingest_schema: None,
            target_table: None,
            target_db_schema: None,
            target_catalog: None,
            ingest_mode: None,
            ingest_batch_write: false,
            dml_partitioned: false,
            bind_by_name: false,
            rows_per_batch: DEFAULT_ROWS_PER_BATCH,
            data_boost: false,
            cancel: Arc::new(CancelSlot::new()),
        }
    }

    /// The connection's database id: the only catalog, as `get_objects` reports it.
    fn catalog(&self) -> &str {
        crate::metadata::database_catalog(&self.database)
    }

    /// A builder for `sql` with this statement's request options, query options and retry policy.
    /// Every query and DML statement is built here so these options apply uniformly.
    fn sql_builder(&self, sql: &str) -> StatementBuilder {
        self.config.retry.apply_to_statement(
            self.config.query_options.apply_to_statement(
                self.config
                    .request
                    .apply_to_statement(SpannerSql::builder(sql)),
            ),
        )
    }

    /// [`sql_builder`](Self::sql_builder) plus directed reads, which Spanner rejects in read/write
    /// transactions.
    fn read_sql_builder(&self, sql: &str) -> StatementBuilder {
        self.config
            .directed_read
            .apply_to_statement(self.sql_builder(sql))
    }

    /// Schema of the ingest data, falling back to the schema of an empty bound stream.
    fn bound_ingest_schema(&self) -> Option<SchemaRef> {
        self.bound
            .first()
            .map(RecordBatch::schema)
            .or_else(|| self.ingest_schema.clone())
    }

    /// Discard bound data once an execution consumes it, so a reused handle can't re-apply stale
    /// rows. `set_sql_query` and the ingest setters keep it, since binding may come first.
    fn clear_bound(&mut self) {
        self.bound.clear();
        self.ingest_schema = None;
    }

    /// Return the result schema of `sql` from a `QueryMode::Plan` probe, which scans no rows.
    /// Binds the first bound parameter row so `@param` references resolve.
    fn plan_query_schema(&self, sql: &str) -> Result<SchemaRef> {
        let client = self.client.clone();
        // The schema is versioned too, so probe at the same staleness as the data read.
        let read_bound = self.config.read_staleness.timestamp_bound()?;
        let precision = self.config.timestamp_precision;
        let statement = self.build_query_statement(sql, true)?;
        block_on_cancellable(
            &self.runtime,
            &self.cancel.current(),
            with_timeout(
                self.config.timeouts.query_timeout(),
                crate::OPTION_RPC_TIMEOUT_QUERY,
                async move {
                    let transaction = crate::staleness::single_use(&client, read_bound);
                    let result_set = transaction
                        .execute_query(statement)
                        .await
                        .map_err(from_spanner)?;
                    let (schema, _batch) = result_set_to_batch(result_set, precision).await?;
                    Ok::<SchemaRef, Error>(schema)
                },
            ),
        )
    }

    /// An empty result reader (empty schema, no rows), for statements that yield no result set.
    fn empty_reader() -> ResultReader {
        let schema = Arc::new(Schema::empty());
        let empty: Vec<std::result::Result<RecordBatch, ArrowError>> = Vec::new();
        Box::new(RecordBatchIterator::new(empty, schema))
    }

    /// Run a query once per bound row, streaming the concatenated results. All rows share one
    /// multi-use read-only transaction, so they read the same snapshot.
    fn execute_multi_row_query(&self, sql: &str) -> Result<ResultReader> {
        // Resolve parameter names up front so a mismatch fails before any statement runs; only
        // the per-row binding is deferred to the reader.
        let groups = self
            .bound
            .iter()
            .filter(|batch| batch.num_rows() > 0)
            .map(|batch| {
                let names = bind::resolve_parameter_names(sql, batch, self.bind_by_name)?;
                Ok((names, batch.clone()))
            })
            .collect::<Result<Vec<_>>>()?;
        let statements: Box<dyn BoundStatementSource> = Box::new(LazyBoundStatements {
            base_builder: self.read_sql_builder(sql),
            groups,
            group: 0,
            row: 0,
        });
        let bound = self.config.read_staleness.multi_use_timestamp_bound()?;
        let manual_txn = self.manual_read_transaction()?;
        let client = self.client.clone();
        let runtime = self.runtime.clone();
        let cancel = self.cancel.current();
        let batch_size = self.rows_per_batch;
        let precision = self.config.timestamp_precision;
        let fetch_timeout = self.config.timeouts.fetch_timeout();
        let txn = manual_txn.clone();
        let reader = block_on_cancellable(
            &self.runtime,
            &self.cancel.current(),
            with_timeout(
                self.config.timeouts.query_timeout(),
                crate::OPTION_RPC_TIMEOUT_QUERY,
                async move {
                    let transaction = match txn {
                        Some(txn) => txn,
                        None => Arc::new(crate::staleness::multi_use(&client, bound).await?),
                    };
                    stream_bound_query(
                        runtime,
                        cancel,
                        transaction,
                        statements,
                        batch_size,
                        precision,
                        fetch_timeout,
                    )
                    .await
                },
            ),
        )
        .inspect_err(|error| self.discard_unbegun_read_txn(manual_txn.as_ref(), error))?;
        Ok(Box::new(reader))
    }

    /// Run `sql` as a read-only query and return a streaming reader.
    ///
    /// Trailing `;` terminators are stripped: Spanner's query API rejects them, but clients often
    /// append one. In a manual transaction the query uses the transaction's shared snapshot.
    /// Otherwise a single statement uses a single-use transaction, which unlike a multi-use one
    /// supports bounded staleness.
    fn execute_query_reader(&self, sql: &str) -> Result<ResultReader> {
        let sql = crate::sql::strip_trailing_terminators(sql);
        self.ensure_query_allowed()?;
        let bound_rows: usize = self.bound.iter().map(RecordBatch::num_rows).sum();
        if bound_rows > 1 {
            return self.execute_multi_row_query(&sql);
        }
        if bound_rows == 0 && !self.bound.is_empty() {
            // `executemany` with no parameter rows runs nothing, but the reader still has the
            // query's schema.
            let schema = self.plan_query_schema(&sql)?;
            let empty: Vec<std::result::Result<RecordBatch, ArrowError>> = Vec::new();
            return Ok(Box::new(RecordBatchIterator::new(empty, schema)));
        }
        // Bind before taking the manual read transaction: a bind error after it would leave the
        // transaction installed but unbegun, fixing the manual transaction's kind to queries.
        let statement = self.build_query_statement(&sql, false)?;
        let bound = self.config.read_staleness.timestamp_bound()?;
        let manual_txn = self.manual_read_transaction()?;
        let client = self.client.clone();
        let runtime = self.runtime.clone();
        let cancel = self.cancel.current();
        let batch_size = self.rows_per_batch;
        let precision = self.config.timestamp_precision;
        let fetch_timeout = self.config.timeouts.fetch_timeout();
        let txn = manual_txn.clone();
        // `stream_query` fetches the first chunk to settle the schema, within the query timeout;
        // the reader converts the rest one chunk at a time, each within the fetch timeout.
        let reader = block_on_cancellable(
            &self.runtime,
            &self.cancel.current(),
            with_timeout(
                self.config.timeouts.query_timeout(),
                crate::OPTION_RPC_TIMEOUT_QUERY,
                async move {
                    let result_set = match txn {
                        Some(txn) => txn.execute_query(statement).await,
                        None => {
                            crate::staleness::single_use(&client, bound)
                                .execute_query(statement)
                                .await
                        }
                    }
                    .map_err(from_spanner)?;
                    stream_query(
                        runtime,
                        cancel,
                        result_set,
                        batch_size,
                        precision,
                        fetch_timeout,
                        None,
                    )
                    .await
                },
            ),
        )
        .inspect_err(|error| self.discard_unbegun_read_txn(manual_txn.as_ref(), error))?;
        Ok(Box::new(reader))
    }

    /// After a failed query, forget the manual read transaction it used if that never began.
    ///
    /// A driver cancel or timeout drops the query instead, and the client then keeps an unbegun
    /// transaction usable, so leave it in place: a concurrent query may be beginning it.
    fn discard_unbegun_read_txn(
        &self,
        txn: Option<&Arc<MultiUseReadOnlyTransaction>>,
        error: &Error,
    ) {
        let dropped =
            error.vendor_code == 0 && matches!(error.status, Status::Cancelled | Status::Timeout);
        if let Some(txn) = txn
            && !dropped
        {
            lock_txn(&self.txn).discard_unbegun_read_txn(txn);
        }
    }

    /// Submit DDL through `UpdateDatabaseDdl`. Statements in a batch are not transactional.
    ///
    /// DDL runs immediately in any transaction mode: Spanner DDL is never transactional, so it
    /// neither joins nor is rejected by a manual transaction, and rollback cannot undo it. The
    /// update timeout covers the long-running operation's poll loop.
    fn run_ddl(&self, statements: Vec<String>) -> Result<()> {
        if self.config.is_read_only() {
            return Err(invalid_state(
                "cannot execute DDL: the connection is read-only",
            ));
        }
        let admin_cell = self.admin.clone();
        let database = self.database.clone();
        block_on_cancellable(
            &self.runtime,
            &self.cancel.current(),
            with_timeout(
                self.config.timeouts.update_timeout(),
                crate::OPTION_RPC_TIMEOUT_UPDATE,
                async move {
                    // Built once per client stack to avoid repeating endpoint and credential
                    // setup; a failed build is not cached.
                    let admin = admin_cell.client().await?;
                    admin
                        .update_database_ddl()
                        .set_database(database)
                        .set_statements(statements)
                        .poller()
                        .until_done()
                        .await
                        .map_err(from_spanner)?;
                    Ok::<(), Error>(())
                },
            ),
        )
    }

    fn sql(&self) -> Result<String> {
        self.sql
            .clone()
            .ok_or_else(|| invalid_state("no SQL query set on statement; call set_sql_query first"))
    }

    /// Body of `execute` for SQL, split out so the caller clears bound data however it ends.
    /// Also returns the affected-row count, as [`execute_counted`](Self::execute_counted) does.
    fn run_sql(&self, sql: &str) -> Result<(ResultReader, Option<i64>)> {
        let statements = crate::sql::split_statements(sql);
        check_batch(&statements)?;
        if crate::sql::is_ddl(sql) {
            self.run_ddl(statements)?;
            return Ok((Self::empty_reader(), None));
        }
        // Clients such as the Python DBAPI and R send every statement through `ExecuteQuery`, so
        // DML must take the read/write path here; the read-only path would reject it.
        // `THEN RETURN` returns its rows; plain DML yields an empty result.
        if crate::sql::is_dml(sql) {
            return match self.run_dml(sql)? {
                DmlOutcome::Returning {
                    batches,
                    schema,
                    affected,
                } => {
                    let batches: Vec<std::result::Result<RecordBatch, ArrowError>> =
                        split_batches(batches, self.rows_per_batch)
                            .into_iter()
                            .map(Ok)
                            .collect();
                    let reader: ResultReader = Box::new(RecordBatchIterator::new(batches, schema));
                    Ok((reader, Some(affected)))
                }
                DmlOutcome::Plain(count) => Ok((Self::empty_reader(), count)),
            };
        }
        Ok((self.execute_query_reader(sql)?, None))
    }

    /// [`Statement::execute`], plus the affected-row count of the DML or bulk ingest it ran, as
    /// `execute_update` reports it. `None` means unknown: queries, DDL, and writes buffered in a
    /// manual transaction, which run only at commit. The C ABI returns it as `rows_affected`,
    /// since clients such as the Python DBAPI run DML through `ExecuteQuery` with a result stream.
    pub(crate) fn execute_counted(&mut self) -> Result<(ResultReader, Option<i64>)> {
        self.cancel.begin_operation();
        // FFI callers may run a bulk ingest through `ExecuteQuery` with a non-null stream
        // pointer; it yields an empty stream.
        if self.sql.is_none()
            && let Some(table) = self.target_table.clone()
        {
            let count = self.run_ingest(&table)?;
            return Ok((Self::empty_reader(), count));
        }
        self.check_bound_has_destination()?;
        let sql = self.sql()?;
        // Bound rows are consumed even if execution fails, so a reused handle can't apply them
        // to a later statement.
        let result = self.run_sql(&sql);
        self.clear_bound();
        result
    }

    /// Build a query statement for `sql`, binding the first bound parameter row if there is one;
    /// `plan` selects `QueryMode::Plan`.
    fn build_query_statement(&self, sql: &str, plan: bool) -> Result<SpannerSql> {
        let mut builder = self.read_sql_builder(sql);
        if plan {
            builder = builder.set_query_mode(QueryMode::Plan);
        }
        if let Some(batch) = self.bound.iter().find(|batch| batch.num_rows() > 0) {
            let names = bind::resolve_parameter_names(sql, batch, self.bind_by_name)?;
            builder = bind::bind_params(builder, &names, batch, 0)?;
        }
        Ok(builder.build())
    }

    /// Partitioned execution has no per-row fan-out, and descriptors can't be attributed to rows,
    /// so reject several bound rows instead of silently using the first.
    fn check_single_bound_row(&self) -> Result<()> {
        let rows: usize = self.bound.iter().map(RecordBatch::num_rows).sum();
        if rows > 1 {
            return Err(invalid_argument(format!(
                "execute_partitions supports at most one bound parameter row, but {rows} rows \
                 are bound: partitioned execution has no per-row fan-out; bind a single row per \
                 execute_partitions call"
            )));
        }
        Ok(())
    }

    /// Body of `execute_partitions`, split out so the caller clears bound data however it ends.
    fn run_partition_query(&self, sql: &str) -> Result<PartitionedResult> {
        check_query_only(
            sql,
            "execute_partitions",
            "partitioned in a batch read-only transaction",
        )?;
        let sql = &crate::sql::strip_trailing_terminators(sql);
        // Partitioned reads never use a manual transaction's snapshot, but they would still miss
        // its buffered writes.
        self.ensure_query_allowed()?;
        self.check_single_bound_row()?;
        // Each partition carries its session, transaction id and token, so it serializes into a
        // self-contained descriptor. The multiplexed session lives as long as the
        // `DatabaseClient`, so descriptors outlive this statement.
        let plan_stmt = self.build_query_statement(sql, true)?;
        let query_stmt = self.build_query_statement(sql, false)?;
        let client = self.client.clone();
        let data_boost = self.data_boost;
        // `read_partition` decodes with the reading connection's `spanner.max_timestamp_precision`,
        // so the two connections should use the same setting.
        let precision = self.config.timestamp_precision;
        // Baked into the batch transaction, so every partition reads at this bound.
        let bound = self.config.read_staleness.timestamp_bound()?;

        let partition_op = async move {
            let plan_rs = crate::staleness::single_use(&client, bound.clone())
                .execute_query(plan_stmt)
                .await
                .map_err(from_spanner)?;
            // Column types travel in each descriptor so an empty partition still has a schema.
            let metadata = plan_rs.metadata();
            let row_type = google_cloud_spanner::model::StructType::new().set_fields(
                metadata
                    .column_names()
                    .iter()
                    .zip(metadata.column_types())
                    .map(|(name, ty)| {
                        google_cloud_spanner::model::struct_type::Field::new()
                            .set_name(name)
                            .set_type(google_cloud_spanner::model::Type::from(ty.clone()))
                    }),
            );
            let (schema, _batch) = result_set_to_batch(plan_rs, precision).await?;

            let mut txn_builder = client.batch_read_only_transaction();
            if let Some(b) = bound {
                txn_builder = txn_builder.set_timestamp_bound(b);
            }
            let transaction = txn_builder.build().await.map_err(from_spanner)?;
            let partitions = transaction
                .partition_query(query_stmt, PartitionOptions::default())
                .await
                .map_err(from_spanner)?;

            // Data Boost is part of the serialized partition, so it applies wherever it executes.
            let tokens = partitions
                .into_iter()
                .map(|partition| {
                    crate::connection::encode_partition(&crate::connection::PartitionDescriptor {
                        partition: partition.set_data_boost(data_boost),
                        row_type: Some(row_type.clone()),
                    })
                })
                .collect::<Result<Vec<Vec<u8>>>>()?;
            Ok::<_, Error>((schema, tokens))
        };
        let (schema, partitions) = block_on_cancellable(
            &self.runtime,
            &self.cancel.current(),
            with_timeout(
                self.config.timeouts.query_timeout(),
                crate::OPTION_RPC_TIMEOUT_QUERY,
                partition_op,
            ),
        )?;

        Ok(PartitionedResult {
            partitions,
            schema: (*schema).clone(),
            // ADBC's "unknown" row count.
            rows_affected: -1,
        })
    }

    /// Type the statement's `@name` parameters with a `QueryMode::Plan` probe, which reports
    /// undeclared parameters with the types the SQL implies. Parameters Spanner can't type are
    /// absent from the map.
    ///
    /// DML can only be planned in a read/write transaction, so it goes through the transaction
    /// runner and commits nothing. DDL, and DML on a read-only connection, yield an empty map.
    /// Both probes use the query timeout.
    fn plan_parameter_types(
        &self,
        sql: &str,
    ) -> Result<BTreeMap<String, google_cloud_spanner::value::Type>> {
        // Spanner rejects a trailing `;` in a query.
        let sql = crate::sql::strip_trailing_terminators(sql);
        if crate::sql::is_ddl(&sql) {
            return Ok(BTreeMap::new());
        }
        self.cancel.begin_operation();
        if crate::sql::is_dml(&sql) {
            if self.config.is_read_only() {
                return Ok(BTreeMap::new());
            }
            return self.plan_dml_parameter_types(&sql);
        }
        let plan_builder = self.read_sql_builder(&sql).set_query_mode(QueryMode::Plan);
        let client = self.client.clone();
        // Plan against the schema version the query will read, as `plan_query_schema` does.
        let read_bound = self.config.read_staleness.timestamp_bound()?;
        block_on_cancellable(
            &self.runtime,
            &self.cancel.current(),
            with_timeout(
                self.config.timeouts.query_timeout(),
                crate::OPTION_RPC_TIMEOUT_QUERY,
                async move {
                    let transaction = crate::staleness::single_use(&client, read_bound);
                    let result_set = transaction
                        .execute_query(plan_builder.build())
                        .await
                        .map_err(from_spanner)?;
                    // Metadata arrives with the first response; a PLAN has no rows to drain.
                    Ok(result_set.metadata().undeclared_parameters().clone())
                },
            ),
        )
    }

    /// DML arm of [`plan_parameter_types`](Self::plan_parameter_types). The closure keeps the
    /// client's error type so an aborted transaction retries the plan.
    fn plan_dml_parameter_types(
        &self,
        sql: &str,
    ) -> Result<BTreeMap<String, google_cloud_spanner::value::Type>> {
        let plan_stmt = self
            .sql_builder(sql)
            .set_query_mode(QueryMode::Plan)
            .build();
        let client = self.client.clone();
        let isolation = self.config.isolation.clone();
        let request = self.config.request.clone();
        let retry = self.config.retry;
        block_on_cancellable(
            &self.runtime,
            &self.cancel.current(),
            with_timeout(
                self.config.timeouts.query_timeout(),
                crate::OPTION_RPC_TIMEOUT_QUERY,
                async move {
                    let runner = runner_builder(&client, isolation, &request, retry).build();
                    let outcome = runner
                        .run(move |transaction: ReadWriteTransaction| {
                            let statement = plan_stmt.clone();
                            async move {
                                let result_set = transaction.execute_query(statement).await?;
                                Ok(result_set.metadata().undeclared_parameters().clone())
                            }
                        })
                        .await
                        .map_err(from_spanner)?;
                    Ok(outcome.result)
                },
            ),
        )
    }

    /// Data may be bound before the ingest target is set, so a missing destination is only
    /// detectable at execution. The error names both remedies instead of just the missing query.
    fn check_bound_has_destination(&self) -> Result<()> {
        if self.sql.is_none()
            && self.target_table.is_none()
            && (!self.bound.is_empty() || self.ingest_schema.is_some())
        {
            return Err(invalid_state(
                "data has been bound but no SQL query or bulk-ingest target is set; call \
                 set_sql_query or set the adbc.ingest.target_table option before executing",
            ));
        }
        Ok(())
    }

    /// Reject a query in a manual transaction that began with DML or ingest. Buffered writes only
    /// run at commit, so the query would silently miss them (an `INSERT` followed by
    /// `SELECT COUNT(*)` would report the old count).
    fn ensure_query_allowed(&self) -> Result<()> {
        lock_txn(&self.txn).check_kind_allowed(TxnKind::Read)
    }

    /// The shared read-only transaction of a manual transaction, or `None` in autocommit mode.
    ///
    /// The first query of a manual transaction creates it at this statement's read bound, fixing
    /// the transaction's kind to queries; later queries reuse it and their staleness settings are
    /// ignored. Building issues no RPC (the begin is inlined into the first query), so a failed
    /// query never leaves a transaction begun; the caller then discards the unbegun transaction.
    fn manual_read_transaction(&self) -> Result<Option<Arc<MultiUseReadOnlyTransaction>>> {
        {
            let st = lock_txn(&self.txn);
            if st.autocommit() {
                return Ok(None);
            }
            if let Some(txn) = st.read_txn() {
                return Ok(Some(txn));
            }
        }
        let bound = self.config.read_staleness.multi_use_timestamp_bound()?;
        let client = self.client.clone();
        let built = block_on_cancellable(&self.runtime, &self.cancel.current(), async move {
            crate::staleness::multi_use(&client, bound).await
        })?;
        let mut st = lock_txn(&self.txn);
        if st.autocommit() {
            // Autocommit was enabled while building; the unused transaction issued no RPC.
            return Ok(None);
        }
        // While the lock was released, a concurrent statement may have fixed the kind to DML
        // (rejected here) or installed its own read transaction (returned instead).
        st.start_read_txn(Arc::new(built)).map(Some)
    }
}

impl Optionable for SpannerStatement {
    type Option = OptionStatement;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        match &key {
            OptionStatement::TargetTable => {
                self.target_table = Some(string_option(&key, value)?);
                // An ingest target and a SQL query are mutually exclusive; the DBAPI cursor reuses
                // handles for both.
                self.sql = None;
            }
            OptionStatement::TargetDbSchema => {
                self.target_db_schema = Some(string_option(&key, value)?);
            }
            OptionStatement::TargetCatalog => {
                self.target_catalog = Some(check_target_catalog(
                    string_option(&key, value)?,
                    self.catalog(),
                )?);
            }
            OptionStatement::Temporary => {
                // Spanner has no temporary tables. Accept the default `false` so generic clients
                // that always set the option keep working.
                check_unsupported_true(
                    value,
                    key.as_ref(),
                    "setting adbc.ingest.temporary to true: Spanner has no temporary tables; \
                     leave it unset or false",
                )?;
            }
            OptionStatement::Incremental => {
                check_unsupported_true(
                    value,
                    key.as_ref(),
                    "setting adbc.statement.exec.incremental to true: incremental \
                     execute_partitions is not implemented; leave it unset or false",
                )?;
            }
            OptionStatement::IngestMode => {
                self.ingest_mode = Some(ingest_mode_option(&key, value)?);
            }
            OptionStatement::Other(k) if k == crate::OPTION_INGEST_BATCH_WRITE => {
                self.ingest_batch_write = ingest_batch_write_option(value)?;
            }
            OptionStatement::Other(k) if k == crate::OPTION_DML_PARTITIONED => {
                self.dml_partitioned = dml_partitioned_option(value)?;
            }
            OptionStatement::Other(k) if k == crate::OPTION_BIND_BY_NAME => {
                self.bind_by_name = bool_option(value, k)?;
            }
            OptionStatement::Other(k) if k == crate::OPTION_ROWS_PER_BATCH => {
                self.rows_per_batch = crate::options::positive_usize(value, k)?;
            }
            OptionStatement::Other(k) if k == crate::OPTION_DATA_BOOST => {
                self.data_boost = bool_option(value, k)?;
            }
            // Options shared with the connection. Unknown keys, including connection-only ones
            // such as `spanner.transaction.tag`, are reported as not implemented.
            OptionStatement::Other(k) => {
                if !self.config.set_option(k, value)? {
                    return Err(unknown_option("statement", k));
                }
            }
            other => {
                return Err(unknown_option("statement", other.as_ref()));
            }
        }
        Ok(())
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        let value = match &key {
            OptionStatement::TargetTable => self.target_table.clone(),
            OptionStatement::TargetDbSchema => self.target_db_schema.clone(),
            OptionStatement::TargetCatalog => self.target_catalog.clone(),
            // `set_option` only ever accepts `false`.
            OptionStatement::Temporary | OptionStatement::Incremental => Some(false.to_string()),
            // Canonical `adbc.ingest.mode.*` spelling; unset reports the default.
            OptionStatement::IngestMode => {
                Some(String::from(self.ingest_mode.unwrap_or(IngestMode::Create)))
            }
            OptionStatement::Other(k) if k == crate::OPTION_INGEST_BATCH_WRITE => {
                Some(self.ingest_batch_write.to_string())
            }
            OptionStatement::Other(k) if k == crate::OPTION_DML_PARTITIONED => {
                Some(self.dml_partitioned.to_string())
            }
            OptionStatement::Other(k) if k == crate::OPTION_BIND_BY_NAME => {
                Some(self.bind_by_name.to_string())
            }
            OptionStatement::Other(k) if k == crate::OPTION_ROWS_PER_BATCH => {
                Some(self.rows_per_batch.to_string())
            }
            OptionStatement::Other(k) if k == crate::OPTION_DATA_BOOST => {
                Some(self.data_boost.to_string())
            }
            // Shared options report the effective value and return NotFound for unset keys.
            OptionStatement::Other(k) => return self.config.option_string(k),
            _ => None,
        };
        value.ok_or_else(|| option_not_set(key.as_ref()))
    }

    impl_typed_option_getters!();
}

impl Statement for SpannerStatement {
    fn bind(&mut self, batch: RecordBatch) -> Result<()> {
        self.bound = vec![batch];
        self.ingest_schema = None;
        Ok(())
    }

    fn bind_stream(&mut self, reader: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        // A stream with no batches still declares a schema, which is enough for an ingest to
        // create the table and commit zero rows.
        let schema = reader.schema();
        let mut batches = Vec::new();
        for (index, batch) in reader.enumerate() {
            let batch = batch.map_err(|e| {
                err(
                    format!("failed to read bound stream batch {}: {e}", index + 1),
                    Status::InvalidData,
                )
            })?;
            if batch.schema() != schema {
                return Err(err(
                    format!(
                        "bound stream batch {} does not match the stream's declared schema",
                        index + 1
                    ),
                    Status::InvalidData,
                ));
            }
            batches.push(batch);
        }
        self.ingest_schema = batches.is_empty().then_some(schema);
        self.bound = batches;
        Ok(())
    }

    fn execute(&mut self) -> Result<ResultReader> {
        self.execute_counted().map(|(reader, _)| reader)
    }

    /// `adbc.h` allows executing any statement without a result set, so a query runs as in
    /// `execute` and its rows, like those of `THEN RETURN`, are discarded.
    fn execute_update(&mut self) -> Result<Option<i64>> {
        let (reader, count) = self.execute_counted()?;
        drain_discarding_rows(reader)?;
        Ok(count)
    }

    fn execute_schema(&mut self) -> Result<Schema> {
        self.cancel.begin_operation();
        let sql = self.sql()?;
        check_query_only(
            &sql,
            "execute_schema",
            "planned in a read-only schema probe",
        )?;
        // Spanner rejects a trailing `;` in a query.
        let sql = crate::sql::strip_trailing_terminators(&sql);
        Ok((*self.plan_query_schema(&sql)?).clone())
    }

    /// Partition this query into opaque descriptors for `Connection::read_partition`.
    ///
    /// At most one bound parameter row is supported, since partitioned execution has no per-row
    /// fan-out; more fail with `InvalidArguments` before any RPC. Bound data is consumed either
    /// way.
    ///
    /// # Security
    ///
    /// Descriptors are opaque but executable: a versioned JSON envelope around the client's
    /// serialized `Partition`, which carries the SQL text and the session and transaction
    /// identity. Anyone who can pass a descriptor to `Connection::read_partition` can run
    /// arbitrary SQL with that connection's credentials; the envelope is not authenticated. Only
    /// move descriptors over trusted channels, and never accept one from an untrusted source.
    fn execute_partitions(&mut self) -> Result<PartitionedResult> {
        self.cancel.begin_operation();
        let sql = self.sql()?;
        let result = self.run_partition_query(&sql);
        self.clear_bound();
        result
    }

    fn get_parameter_schema(&self) -> Result<Schema> {
        // Bound columns are the parameters, with known types.
        if let Some(batch) = self.bound.first() {
            return Ok((*batch.schema()).clone());
        }
        // Otherwise take the names from the SQL and the types from a PLAN probe. Parameters the
        // probe can't type, or a failed probe, get `Null`, which ADBC defines as "unknown type".
        let sql = self.sql()?;
        let names = crate::sql::named_parameters(&sql);
        if names.is_empty() {
            return Ok(Schema::new(Vec::<Field>::new()));
        }
        let types = self.plan_parameter_types(&sql).unwrap_or_default();
        let fields: Vec<Field> = names
            .into_iter()
            .map(|name| {
                // GoogleSQL parameter names are case-insensitive, and the planner reports each
                // under the first spelling the SQL uses.
                let ty = types.get(&name).or_else(|| {
                    types
                        .iter()
                        .find_map(|(k, v)| k.eq_ignore_ascii_case(&name).then_some(v))
                });
                match ty {
                    // Same mapping as result columns, so JSON parameters carry the `arrow.json`
                    // tag that binding recognizes.
                    Some(ty) => crate::conversion::arrow_field(
                        &name,
                        ty,
                        true,
                        self.config.timestamp_precision,
                    ),
                    None => Ok(Field::new(name, DataType::Null, true)),
                }
            })
            .collect::<Result<_>>()?;
        Ok(Schema::new(fields))
    }

    fn prepare(&mut self) -> Result<()> {
        // ADBC requires InvalidState when there is nothing to prepare. Otherwise this is a no-op:
        // Spanner plans statements when they execute.
        if self.sql.is_none() && self.target_table.is_none() {
            return Err(invalid_state(
                "cannot prepare: no SQL query set on statement; call set_sql_query first",
            ));
        }
        Ok(())
    }

    fn set_sql_query(&mut self, query: impl AsRef<str>) -> Result<()> {
        self.sql = Some(query.as_ref().to_string());
        // A query and an ingest target are mutually exclusive; see `set_option`. Clear the whole
        // target, as the reference drivers do: the DBAPI only sets the schema when given one, so
        // a stale schema would redirect a reused cursor's next ingest to another table.
        self.target_table = None;
        self.target_db_schema = None;
        self.target_catalog = None;
        Ok(())
    }

    fn set_substrait_plan(&mut self, _plan: impl AsRef<[u8]>) -> Result<()> {
        Err(unsupported(
            "setting a Substrait plan: use set_sql_query with GoogleSQL text instead",
        ))
    }

    fn get_cancel_handle(&self) -> Box<dyn CancelHandle> {
        // Cancels the current operation, including later chunk fetches of its streaming reader.
        // The next operation gets a fresh signal, so a cancel while idle doesn't affect it.
        Box::new(SlotCancelHandle::new(self.cancel.clone()))
    }
}

/// Parse a string option, naming `key` in the error.
fn string_option(key: &OptionStatement, value: OptionValue) -> Result<String> {
    crate::options::string_option(value, key.as_ref())
}

/// Reject DDL and DML in the query-only entry points. DML, including `THEN RETURN`, would
/// otherwise fail with Spanner's read-write transaction error, which suggests the transaction mode
/// is at fault. `dml_rationale` completes "DML (INSERT/UPDATE/DELETE) cannot be".
fn check_query_only(sql: &str, entry_point: &str, dml_rationale: &str) -> Result<()> {
    if crate::sql::is_ddl(sql) {
        return Err(invalid_argument(format!(
            "{entry_point} is only valid for queries"
        )));
    }
    if crate::sql::is_dml(sql) {
        return Err(invalid_argument(format!(
            "{entry_point} only supports queries: DML (INSERT/UPDATE/DELETE) cannot be \
             {dml_rationale}; run it via execute or execute_update instead"
        )));
    }
    Ok(())
}

/// Reject a `;`-separated batch that mixes statement kinds before anything runs or is buffered.
/// `UpdateDatabaseDdl` accepts only DDL and `ExecuteBatchDml` only DML, and one bad statement
/// buffered in a manual transaction would block its commit until rollback. Queries can't be
/// batched at all. Single statements pass.
fn check_batch(statements: &[String]) -> Result<()> {
    let [first, _, ..] = statements else {
        return Ok(());
    };
    let ddl = crate::sql::is_ddl(first);
    let Some(other) = statements.iter().find(|s| {
        if ddl {
            !crate::sql::is_ddl(s)
        } else {
            !crate::sql::is_dml(s)
        }
    }) else {
        return Ok(());
    };
    Err(invalid_argument(if ddl {
        format!(
            "a `;`-separated DDL batch must be all DDL, but it contains {other:?}: run DML and \
             queries as individual statements"
        )
    } else {
        format!(
            "a `;`-separated statement batch must be all-DML (INSERT/UPDATE/DELETE), but it \
             contains {other:?}: run queries and DDL as individual statements"
        )
    }))
}

/// Reject what Partitioned DML can't express, with a clearer error than Spanner's. It runs one
/// statement in its own transaction type and returns no rows, which rules out manual
/// transactions, `THEN RETURN`, `;`-batches and multiple bound rows.
fn check_partitioned_dml(sql: &str, autocommit: bool, bound_rows: usize) -> Result<()> {
    if !autocommit {
        return Err(invalid_state(
            "option spanner.dml.partitioned is set, but the connection is in a manual transaction: \
             partitioned DML is its own transaction type and cannot join one. Commit (or re-enable \
             adbc.connection.autocommit) first, or unset the option",
        ));
    }
    if crate::sql::is_dml_returning(sql) {
        return Err(invalid_argument(
            "option spanner.dml.partitioned is set, but the statement has a THEN RETURN clause: \
             partitioned DML returns no rows. Unset the option to run it in a read/write \
             transaction",
        ));
    }
    let statements = crate::sql::split_statements(sql).len();
    if statements > 1 {
        return Err(invalid_argument(format!(
            "option spanner.dml.partitioned is set, but the SQL is a `;`-separated batch of \
             {statements} statements: partitioned DML runs exactly one statement. Execute each \
             statement separately, or unset the option"
        )));
    }
    if bound_rows > 1 {
        return Err(invalid_argument(format!(
            "option spanner.dml.partitioned is set, but {bound_rows} parameter rows are bound: \
             partitioned DML runs exactly one statement. Bind a single row, or unset the option"
        )));
    }
    Ok(())
}

/// Slice `batches` so none exceeds `max_rows`, as query results are. Slices share buffers.
fn split_batches(batches: Vec<RecordBatch>, max_rows: usize) -> Vec<RecordBatch> {
    let mut out = Vec::with_capacity(batches.len());
    for batch in batches {
        if batch.num_rows() <= max_rows {
            out.push(batch);
            continue;
        }
        let mut offset = 0;
        while offset < batch.num_rows() {
            let len = max_rows.min(batch.num_rows() - offset);
            out.push(batch.slice(offset, len));
            offset += len;
        }
    }
    out
}

/// Drain a query's rows, discarding them; mid-stream failures still surface. The streaming layer
/// wraps ADBC errors in `ArrowError::ExternalError`, so unwrap them to report what `execute`
/// would.
fn drain_discarding_rows(reader: ResultReader) -> Result<()> {
    for batch in reader {
        batch.map_err(|e| {
            if let ArrowError::ExternalError(inner) = e {
                match inner.downcast::<Error>() {
                    Ok(adbc_error) => *adbc_error,
                    Err(other) => err(
                        format!("query failed while its discarded result set was drained: {other}"),
                        Status::Internal,
                    ),
                }
            } else {
                err(
                    format!("query failed while its discarded result set was drained: {e}"),
                    Status::Internal,
                )
            }
        })?;
    }
    Ok(())
}

/// Accept the spec default `false` as a no-op and reject `true` with `rejection`. `what` names the
/// option in boolean parse errors.
fn check_unsupported_true(value: OptionValue, what: &str, rejection: &str) -> Result<()> {
    if bool_option(value, what)? {
        Err(unsupported(rejection))
    } else {
        Ok(())
    }
}

/// Parse `spanner.dml.partitioned`; an empty or whitespace-only string resets it to `false`.
fn dml_partitioned_option(value: OptionValue) -> Result<bool> {
    crate::options::bool_option_unsettable(value, crate::OPTION_DML_PARTITIONED)
}

#[cfg(test)]
mod tests;

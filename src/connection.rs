//! [`SpannerConnection`], an ADBC connection backed by a Spanner [`DatabaseClient`].
//!
//! The transaction model is documented on [`SpannerConnection`]. The invariants that keep it
//! sound under concurrent statements are documented on `TxnState` in [`txn`].

use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::{InfoCode, ObjectDepth, OptionConnection, OptionValue};
use adbc_core::{CancelHandle, Connection, Optionable};
use arrow_array::{ArrayRef, RecordBatch, RecordBatchIterator, RecordBatchReader, StringArray};
use arrow_schema::{DataType, Field, Schema};
use google_cloud_spanner::batch::Partition;
use google_cloud_spanner::client::DatabaseClient;
use google_cloud_spanner::model::StructType;

use crate::conversion::{TimestampPrecision, result_set_to_batch, stream_query};
use crate::driver::{Connected, SharedDatabaseAdmin};
use crate::error::{
    annotate, err, from_spanner, invalid_argument, invalid_state, option_not_set, unknown_option,
    unsupported,
};
use crate::options::{SharedConfig, impl_typed_option_getters};
use crate::runtime::{CancelSlot, SharedRuntime, SlotCancelHandle, block_on_cancellable};
use crate::sql::qualified_table;
use crate::statement::{DEFAULT_ROWS_PER_BATCH, SpannerStatement};
use crate::timeout::with_timeout;

mod exec;
mod txn;

use crate::metadata::{check_lookup_catalog, like_match, metadata_sql_builder, table_exists_at};
use exec::{isolation_to_adbc_string, parse_isolation_level};
pub(crate) use exec::{run_batch_txn, runner_builder, write_mutations_txn};
use txn::{ManualTxn, check_commit_writable};
pub(crate) use txn::{SharedTxn, TxnKind, TxnState, lock_txn};

/// An ADBC connection to a Spanner database.
///
/// # Transactions
///
/// The connection starts in autocommit mode. Setting `adbc.connection.autocommit` to `false`
/// enters manual mode. A manual transaction holds either queries or DML, decided by its first
/// statement; work of the other kind fails with [`Status::InvalidState`] until
/// [`Connection::commit`] or [`Connection::rollback`]:
///
/// - Queries share one multi-use read-only transaction: a consistent snapshot pinned at the first
///   query's `spanner.read.staleness` bound. Commit and rollback just drop it.
/// - DML and bulk-ingest mutations are buffered and applied atomically in one read/write
///   transaction on commit, because the Spanner client exposes read/write transactions only
///   through a closure-based runner. A transaction that buffered only mutations commits through
///   the client's replay-protected write-only transaction, so an ambiguous transport failure
///   cannot apply it twice.
///
/// Spanner DDL is not transactional. Like the ADBC BigQuery driver, this driver runs DDL
/// immediately through the admin API and leaves the transaction alone, so DDL issued after
/// buffered DML executes before it.
///
/// On an `adbc.connection.readonly` connection, committing buffered DML or ingest work also fails
/// with [`Status::InvalidState`] and keeps the buffer. Committing a query transaction and rolling
/// back still work.
///
/// See [docs/transactions.md](https://github.com/fornwall/spanner-adbc/blob/main/docs/transactions.md)
/// for the full model: no read-your-writes, `None` DML counts before commit, and replay after a
/// failed commit.
#[derive(Debug)]
pub struct SpannerConnection {
    runtime: SharedRuntime,
    client: DatabaseClient,
    database: String,
    /// Lazily built admin client for DDL, shared with statements and with other connections to
    /// the same database.
    admin: SharedDatabaseAdmin,
    /// Option values; each new statement starts from an inherited copy.
    config: SharedConfig,
    txn: SharedTxn,
    /// Cancellation for metadata, commit and partition reads. Each operation begins a fresh
    /// signal and a cancel latches it, so a cancelled `read_partition` stream stays cancelled.
    cancel: Arc<CancelSlot>,
}

impl SpannerConnection {
    pub(crate) fn new(runtime: SharedRuntime, connected: Connected) -> Self {
        Self {
            runtime,
            client: connected.client,
            database: connected.database,
            admin: connected.admin,
            config: SharedConfig::default(),
            txn: Arc::new(Mutex::new(TxnState::new())),
            cancel: Arc::new(CancelSlot::new()),
        }
    }

    /// The connection's only catalog: the `<d>` of `projects/<p>/instances/<i>/databases/<d>`, or
    /// the whole configured path if it has another form.
    pub(crate) fn catalog(&self) -> &str {
        crate::metadata::database_catalog(&self.database)
    }

    /// Apply a manual transaction's buffered DML and mutations atomically. Read-only and empty
    /// transactions have nothing to apply. On failure the caller keeps the buffer, so the commit
    /// can be retried.
    fn apply_manual_txn(&self, work: &ManualTxn) -> Result<()> {
        check_commit_writable(self.config.is_read_only(), work)?;
        let ManualTxn::Dml {
            statements,
            mutations,
        } = work
        else {
            return Ok(());
        };
        self.cancel.begin_operation();
        if statements.is_empty() {
            // Mutations-only commits use the client's replay protection.
            return write_mutations_txn(
                &self.runtime,
                &self.client,
                &self.cancel.current(),
                &self.config,
                mutations.iter().cloned(),
            )
            .map_err(note_commit_outcome_unknown);
        }
        run_batch_txn(
            &self.runtime,
            &self.client,
            &self.cancel.current(),
            &self.config,
            statements,
            mutations,
            // Not the transaction's last request: Spanner applies the mutations at commit.
            false,
        )
        .map_err(note_commit_outcome_unknown)?;
        Ok(())
    }
}

impl Optionable for SpannerConnection {
    type Option = OptionConnection;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        match &key {
            OptionConnection::AutoCommit => {
                let enable = crate::options::bool_option(value, key.as_ref())?;
                // Enabling autocommit commits the active manual transaction. `enter_autocommit`
                // flips the mode and takes the state under one lock, so concurrent statements
                // cannot strand work behind the flip. Like a failed `commit`, a failed apply keeps
                // the work: `restore_manual` puts it back.
                let pending = {
                    let mut st = lock_txn(&self.txn);
                    if enable && !st.autocommit {
                        Some(st.enter_autocommit())
                    } else {
                        st.autocommit = enable;
                        None
                    }
                };
                if let Some(work) = pending
                    && let Err(e) = self.apply_manual_txn(&work)
                {
                    lock_txn(&self.txn).restore_manual(work);
                    return Err(e);
                }
            }
            OptionConnection::ReadOnly => self.config.read_only.store(
                crate::options::bool_option(value, key.as_ref())?,
                Ordering::Release,
            ),
            OptionConnection::IsolationLevel => {
                self.config.isolation = parse_isolation_level(value)?
            }
            // Connection-only: the tag applies to the whole read/write transaction.
            OptionConnection::Other(k) if k == crate::OPTION_TRANSACTION_TAG => {
                self.config.request.set_transaction_tag(value)?;
            }
            // Options shared with statements.
            OptionConnection::Other(k) => {
                if !self.config.set_option(k, value)? {
                    return Err(unknown_option("connection", key.as_ref()));
                }
            }
            // Fixed in Spanner: a connection reaches one database and has no session schema.
            OptionConnection::CurrentCatalog => {
                check_fixed_catalog_or_schema(value, "current catalog", self.catalog())?;
            }
            OptionConnection::CurrentSchema => {
                check_fixed_catalog_or_schema(value, "current schema", "")?;
            }
            other => {
                return Err(unknown_option("connection", other.as_ref()));
            }
        }
        Ok(())
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        match &key {
            OptionConnection::AutoCommit => Ok(lock_txn(&self.txn).autocommit.to_string()),
            OptionConnection::ReadOnly => Ok(self.config.is_read_only().to_string()),
            OptionConnection::IsolationLevel => {
                Ok(isolation_to_adbc_string(&self.config.isolation).to_string())
            }
            OptionConnection::Other(k) if k == crate::OPTION_TRANSACTION_TAG => self
                .config
                .request
                .transaction_tag_string()
                .map(str::to_string)
                .ok_or_else(|| option_not_set(crate::OPTION_TRANSACTION_TAG)),
            OptionConnection::Other(k) => self.config.option_string(k),
            // The default schema is unnamed: "" in INFORMATION_SCHEMA.
            OptionConnection::CurrentCatalog => Ok(self.catalog().to_string()),
            OptionConnection::CurrentSchema => Ok(String::new()),
            other => Err(option_not_set(other.as_ref())),
        }
    }

    impl_typed_option_getters!();
}

impl Connection for SpannerConnection {
    type StatementType = SpannerStatement;

    fn new_statement(&mut self) -> Result<Self::StatementType> {
        Ok(SpannerStatement::new(
            self.runtime.clone(),
            self.client.clone(),
            self.database.clone(),
            self.admin.clone(),
            self.config.inherit(),
            self.txn.clone(),
        ))
    }

    fn get_cancel_handle(&self) -> Box<dyn CancelHandle> {
        // The signal is sticky, so a cancel between two chunk fetches still cancels the next one.
        // Statements have their own signal and are unaffected.
        Box::new(SlotCancelHandle::new(self.cancel.clone()))
    }

    /// Driver and vendor metadata from static constants; makes no RPC.
    ///
    /// `codes = None` returns every code the driver can answer. Otherwise there is one row per
    /// requested code, with a null value for codes it cannot answer.
    fn get_info(
        &self,
        codes: Option<HashSet<InfoCode>>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let batch = crate::info::build(codes)?;
        let schema = batch.schema();
        Ok(Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema)))
    }

    /// Catalog, schema, table and column metadata from `INFORMATION_SCHEMA`.
    ///
    /// The database id is the only catalog. Name arguments are ADBC `LIKE` patterns.
    fn get_objects(
        &self,
        depth: ObjectDepth,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        table_type: Option<Vec<&str>>,
        column_name: Option<&str>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        self.cancel.begin_operation();
        let out_schema = adbc_core::schemas::GET_OBJECTS_SCHEMA.clone();
        if catalog.is_some_and(|c| !like_match(c, self.catalog())) {
            return Ok(Box::new(RecordBatchIterator::new(Vec::new(), out_schema)));
        }
        let schemas = crate::objects::collect_objects(
            &self.runtime,
            &self.client,
            &self.cancel.current(),
            &self.config,
            depth,
            &crate::objects::ObjectFilters {
                db_schema,
                table_name,
                table_type: table_type.as_deref(),
                column_name,
            },
        )?;
        let batch = crate::objects::build(depth, self.catalog(), schemas)?;
        Ok(Box::new(RecordBatchIterator::new(
            vec![Ok(batch)],
            out_schema,
        )))
    }

    /// Run `SELECT * FROM <table> LIMIT 0` and map its column metadata as for query results.
    ///
    /// `catalog` must be `None` or the database id; any other catalog fails with
    /// [`Status::NotFound`].
    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> Result<Schema> {
        self.cancel.begin_operation();
        check_lookup_catalog(catalog, self.catalog())?;
        let table = qualified_table(db_schema, table_name)?;
        let sql = format!("SELECT * FROM {table} LIMIT 0");
        let client = self.client.clone();
        let bound = self.config.read_staleness.timestamp_bound()?;
        // Use the connection's precision so the schema matches what its queries stream.
        let precision = self.config.timestamp_precision;
        let statement = metadata_sql_builder(&self.config, sql).build();
        let probe_bound = bound.clone();
        let result = block_on_cancellable(
            &self.runtime,
            &self.cancel.current(),
            with_timeout(
                self.config.timeouts.query_timeout(),
                crate::OPTION_RPC_TIMEOUT_QUERY,
                async move {
                    let transaction = crate::staleness::single_use(&client, bound);
                    let result_set = transaction
                        .execute_query(statement)
                        .await
                        .map_err(from_spanner)?;
                    result_set_to_batch(result_set, precision).await
                },
            ),
        );
        match result {
            Ok((schema, _batch)) => Ok((*schema).clone()),
            // A missing table is INVALID_ARGUMENT ("Table not found") from the query analyzer, but
            // ADBC wants NotFound. Probe INFORMATION_SCHEMA only on failure, so the common case
            // stays one query. Other statuses skip the probe: after a timeout it would add a
            // second deadline, and INFORMATION_SCHEMA hides tables a role cannot read, which
            // would turn a permission error into NotFound.
            Err(error) if matches!(error.status, Status::InvalidArguments | Status::NotFound) => {
                // At the query's bound, so a table created after a stale read is still missing.
                let exists = table_exists_at(
                    &self.runtime,
                    &self.client,
                    &self.cancel.current(),
                    &self.config,
                    probe_bound,
                    db_schema.unwrap_or(""),
                    table_name,
                );
                match exists {
                    Ok(false) => Err(err(
                        format!("table {table_name:?} not found"),
                        Status::NotFound,
                    )),
                    // The query failed for another reason, or the probe failed too.
                    Ok(true) | Err(_) => Err(error),
                }
            }
            Err(error) => Err(error),
        }
    }

    /// Spanner's `INFORMATION_SCHEMA.TABLES.TABLE_TYPE` values. `get_objects` reports the same
    /// values, so each works as its `table_type` filter.
    fn get_table_types(&self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "table_type",
            DataType::Utf8,
            false,
        )]));
        let array = Arc::new(StringArray::from(vec!["BASE TABLE", "VIEW"])) as ArrayRef;
        let batch = RecordBatch::try_new(schema.clone(), vec![array]).map_err(|e| {
            err(
                format!("failed to build table types batch: {e}"),
                Status::Internal,
            )
        })?;
        Ok(Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema)))
    }

    /// Empty: the driver reports only standard ADBC statistics.
    fn get_statistic_names(&self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        Ok(Box::new(RecordBatchIterator::new(
            Vec::new(),
            adbc_core::schemas::GET_STATISTIC_NAMES_SCHEMA.clone(),
        )))
    }

    /// Exact statistics from aggregate scans: `ROW_COUNT`, plus `NULL_COUNT` and `DISTINCT_COUNT`
    /// per column. Name arguments are ADBC `LIKE` patterns.
    ///
    /// `approximate` is ignored. Spanner keeps no precomputed statistics, and exact values satisfy
    /// an approximate request; rows report `statistic_is_approximate = false`.
    fn get_statistics(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        approximate: bool,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        self.cancel.begin_operation();
        let out_schema = adbc_core::schemas::GET_STATISTICS_SCHEMA.clone();
        if catalog.is_some_and(|c| !like_match(c, self.catalog())) {
            return Ok(Box::new(RecordBatchIterator::new(Vec::new(), out_schema)));
        }
        let _ = approximate;
        let schemas = crate::statistics::collect_statistics(
            &self.runtime,
            &self.client,
            &self.cancel.current(),
            &self.config,
            db_schema,
            table_name,
        )?;
        let batch = crate::statistics::build(self.catalog(), schemas, out_schema.clone())?;
        Ok(Box::new(RecordBatchIterator::new(
            vec![Ok(batch)],
            out_schema,
        )))
    }

    fn commit(&mut self) -> Result<()> {
        // Apply a clone and clear the buffer only after success. After a failed apply (e.g.
        // ABORTED once the runner's retries run out), a retried commit replays the work instead of
        // reporting success with nothing written, and rollback can still discard it.
        let work = {
            let st = lock_txn(&self.txn);
            if st.autocommit {
                return Err(invalid_state(
                    "commit invoked with autocommit enabled; no active transaction",
                ));
            }
            st.txn.clone()
        };
        self.apply_manual_txn(&work)?;
        lock_txn(&self.txn).finish_commit(&work);
        Ok(())
    }

    fn rollback(&mut self) -> Result<()> {
        let mut st = lock_txn(&self.txn);
        if st.autocommit {
            return Err(invalid_state(
                "rollback invoked with autocommit enabled; no active transaction",
            ));
        }
        // Neither buffered DML nor a read-only snapshot needs a rollback RPC.
        st.txn = ManualTxn::Unset;
        Ok(())
    }

    /// Execute a partition descriptor from `Statement::execute_partitions` and stream its rows.
    ///
    /// # Security
    ///
    /// A descriptor is opaque but executable: a versioned JSON envelope around the client's
    /// serialized `Partition`, whose `ExecuteSqlRequest` holds the SQL text, session and
    /// transaction. It runs with this connection's credentials, so a crafted descriptor can execute
    /// arbitrary SQL as the connection's principal. The version guards only against format drift
    /// between driver versions; there is no authentication. Never accept a descriptor from an
    /// untrusted source.
    fn read_partition(
        &self,
        partition: impl AsRef<[u8]>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        self.cancel.begin_operation();
        // The descriptor carries the session, transaction id, partition token and Data Boost flag,
        // so it runs on this connection's client, which shares the multiplexed session.
        let descriptor = decode_partition(partition.as_ref())?;
        let client = self.client.clone();
        let runtime = self.runtime.clone();
        let cancel = self.cancel.current();
        // Timestamp precision and retry settings come from the reading connection. The precision
        // must match the producing statement's for the schemas to agree. The query timeout bounds
        // the execute and first chunk, the fetch timeout each later chunk.
        let precision = self.config.timestamp_precision;
        let fallback_schema = descriptor.schema(precision)?;
        let partition = self.config.retry.apply_to_partition(descriptor.partition);
        let fetch_timeout = self.config.timeouts.fetch_timeout();
        let reader = block_on_cancellable(
            &self.runtime,
            &self.cancel.current(),
            with_timeout(
                self.config.timeouts.query_timeout(),
                crate::OPTION_RPC_TIMEOUT_QUERY,
                async move {
                    let result_set = partition.execute(&client).await.map_err(from_spanner)?;
                    stream_query(
                        runtime,
                        cancel,
                        result_set,
                        DEFAULT_ROWS_PER_BATCH,
                        precision,
                        fetch_timeout,
                        fallback_schema,
                    )
                    .await
                },
            ),
        )?;
        Ok(Box::new(reader))
    }
}

/// Envelope version written by [`encode_partition`].
///
/// The payload is the client's [`Partition`] serde form, which this driver does not control.
/// Bump the version on incompatible changes so an older driver rejects newer descriptors with a
/// clear error instead of a shape mismatch.
pub(crate) const PARTITION_DESCRIPTOR_VERSION: u64 = 1;

/// The executable SDK partition plus optional column types for empty partition responses.
#[derive(Debug)]
pub(crate) struct PartitionDescriptor {
    pub(crate) partition: Partition,
    pub(crate) row_type: Option<StructType>,
}

impl PartitionDescriptor {
    fn schema(&self, precision: TimestampPrecision) -> Result<Option<Arc<Schema>>> {
        self.row_type
            .as_ref()
            .map(|row_type| {
                let fields = row_type
                    .fields
                    .iter()
                    .map(|field| {
                        let ty = field.r#type.as_deref().cloned().unwrap_or_default().into();
                        crate::conversion::arrow_field(&field.name, &ty, true, precision)
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(Arc::new(Schema::new(fields)))
            })
            .transpose()
    }
}

/// Encode a descriptor as `{"v":1,"partition":<SDK partition>,"row_type":<column metadata>}`.
/// The optional `row_type` preserves column types when an empty partition returns no metadata.
pub(crate) fn encode_partition(descriptor: &PartitionDescriptor) -> Result<Vec<u8>> {
    let internal = |e: serde_json::Error| {
        err(
            format!("failed to serialize partition descriptor: {e}"),
            Status::Internal,
        )
    };
    let payload = serde_json::to_value(&descriptor.partition).map_err(internal)?;
    let mut envelope =
        serde_json::json!({ "v": PARTITION_DESCRIPTOR_VERSION, "partition": payload });
    if let Some(row_type) = &descriptor.row_type {
        envelope["row_type"] = serde_json::to_value(row_type).map_err(internal)?;
    }
    serde_json::to_vec(&envelope).map_err(internal)
}

/// Inverse of [`encode_partition`]. A missing or unsupported version and undecodable input are
/// [`Status::InvalidArguments`].
pub(crate) fn decode_partition(descriptor: &[u8]) -> Result<PartitionDescriptor> {
    let invalid =
        |e: serde_json::Error| invalid_argument(format!("invalid partition descriptor: {e}"));
    let value: serde_json::Value = serde_json::from_slice(descriptor).map_err(invalid)?;
    // Check the version first, so a future-format descriptor fails on the version and not on its
    // payload shape.
    let v = value.get("v").ok_or_else(|| {
        invalid_argument("invalid partition descriptor: missing \"v\" version field")
    })?;
    let v = v.as_u64().ok_or_else(|| {
        invalid_argument(format!(
            "invalid partition descriptor: version {v} is not an unsigned integer"
        ))
    })?;
    if v != PARTITION_DESCRIPTOR_VERSION {
        return Err(invalid_argument(format!(
            "partition descriptor version {v} not supported by this driver"
        )));
    }
    let payload = value.get("partition").cloned().ok_or_else(|| {
        invalid_argument("invalid partition descriptor: missing \"partition\" field")
    })?;
    let partition = serde_json::from_value(payload).map_err(invalid)?;
    let row_type = value
        .get("row_type")
        .map(|row_type| serde_json::from_value(row_type.clone()).map_err(invalid))
        .transpose()?;
    Ok(PartitionDescriptor {
        partition,
        row_type,
    })
}

/// A timeout or cancel drops an in-flight commit, which may still land. The buffer is kept, so
/// warn that committing it again could apply the work twice.
fn note_commit_outcome_unknown(error: Error) -> Error {
    if !matches!(error.status, Status::Timeout | Status::Cancelled) {
        return error;
    }
    annotate(error, |message| {
        format!(
            "{message} (the commit may still have been applied; the buffered work is kept, so \
             committing again could apply it twice; roll back to discard it)"
        )
    })
}

/// Accept setting the current catalog or schema to its `fixed` value as a no-op. Other values are
/// `NotImplemented`, as in the C++ PostgreSQL driver.
fn check_fixed_catalog_or_schema(value: OptionValue, what: &str, fixed: &str) -> Result<()> {
    let OptionValue::String(s) = value else {
        return Err(invalid_argument(format!("expected a string {what} value")));
    };
    if s == fixed {
        Ok(())
    } else {
        Err(unsupported(format!(
            "setting the {what} to {s:?}: Spanner has no settable {what}; only {fixed:?} is valid"
        )))
    }
}

#[cfg(test)]
mod tests;

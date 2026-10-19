//! Bulk ingest: `adbc.ingest.*` option parsing, create/replace table DDL, and the mutation write
//! paths (chunked write-only commits, or non-atomic BatchWrite).

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::{IngestMode, OptionStatement, OptionValue};
use arrow_array::{Array, RecordBatch};
use google_cloud_spanner::mutation::{Mutation, MutationGroup};

use super::{SpannerStatement, string_option};
use crate::bind;
use crate::connection::{TxnKind, lock_txn, write_mutations_txn};
use crate::error::{
    annotate, err, from_spanner, from_status_parts, invalid_state, not_implemented, unsupported,
};
use crate::runtime::block_on_cancellable;
use crate::timeout::with_timeout;

impl SpannerStatement {
    /// DDL to run before an ingest in the create/replace modes; `None` for append.
    ///
    /// `create` (the default) fails if the table exists, `create_append` creates it if absent, and
    /// `replace` drops any existing table first. Columns come from the bound data's schema.
    fn build_ingest_table_ddl(
        &self,
        table: &str,
        mode: Option<IngestMode>,
    ) -> Result<Option<Vec<String>>> {
        // Unset means `create`, the spec default.
        let (if_not_exists, drop_first) = match mode {
            Some(IngestMode::Append) => return Ok(None),
            None | Some(IngestMode::Create) => (false, false),
            Some(IngestMode::CreateAppend) => (true, false),
            Some(IngestMode::Replace) => (false, true),
        };
        let schema = self
            .bound_ingest_schema()
            .ok_or_else(|| invalid_state("cannot create the ingest table: no data is bound"))?;
        let db_schema = self.target_db_schema.as_deref();
        let mut statements = Vec::new();
        if drop_first {
            statements.push(format!(
                "DROP TABLE IF EXISTS {}",
                crate::sql::qualified_table(db_schema, table)?
            ));
        }
        statements.push(bind::create_table_sql(
            table,
            db_schema,
            &schema,
            if_not_exists,
        )?);
        Ok(Some(statements))
    }

    /// Whether `table` exists in the ingest target schema.
    fn ingest_table_exists(&self, table: &str) -> Result<bool> {
        crate::metadata::table_exists(
            &self.runtime,
            &self.client,
            &self.cancel.current(),
            &self.config,
            self.target_db_schema.as_deref().unwrap_or(""),
            table,
        )
    }

    /// Map a failed `append` or `create_append` insert onto the statuses ADBC requires: `NotFound`
    /// for a missing table, `AlreadyExists` for a schema mismatch. `create_append` guarantees the
    /// table exists, so only the mismatch applies to it. `create` and `replace` keep the raw error;
    /// their DDL step handles table existence.
    ///
    /// A constraint violation (`Integrity`, such as a duplicate primary key) only gets the table
    /// name added. Operational failures keep their original status and details.
    fn remap_ingest_append_error(&self, table: &str, error: Error) -> Error {
        if !matches!(
            self.ingest_mode,
            Some(IngestMode::Append) | Some(IngestMode::CreateAppend)
        ) {
            return error;
        }
        // Table existence says nothing about an authorization, cancellation, timeout, quota or
        // transport failure, and probing could delay cancellation.
        if !matches!(
            error.status,
            Status::InvalidArguments | Status::InvalidData | Status::NotFound | Status::Integrity
        ) {
            return error;
        }
        // The rows conflict with the table's data or constraints, not its schema. Skip the probe,
        // whose "incompatible schema" message would misreport it, and keep the vendor code and
        // details.
        if error.status == Status::Integrity {
            return annotate(error, |message| {
                format!("bulk ingest append into table {table:?} failed: {message}")
            });
        }
        match self.ingest_table_exists(table) {
            Ok(true) => err(
                format!(
                    "bulk ingest append into table {table:?} failed: the bound data is \
                     incompatible with the existing table's schema ({})",
                    error.message
                ),
                Status::AlreadyExists,
            ),
            Ok(false) => err(
                format!(
                    "bulk ingest append target table {table:?} not found ({})",
                    error.message
                ),
                Status::NotFound,
            ),
            Err(_) => error,
        }
    }

    /// Ingest the bound rows into `table` in the configured mode. The create/replace modes first
    /// create a keyless table from the data's schema. Returns the row count, or `None` when the
    /// rows were buffered in a manual transaction.
    ///
    /// An ingest that fits in one chunk is atomic. Larger ones commit chunk by chunk, so a failure
    /// leaves earlier chunks committed; the error reports how many rows.
    pub(super) fn run_ingest(&mut self, table: &str) -> Result<Option<i64>> {
        // An empty bound stream still provides a schema, enough to create the table.
        if self.bound.is_empty() && self.ingest_schema.is_none() {
            return Err(invalid_state("cannot ingest: no data has been bound"));
        }
        let result = self.run_bound_ingest(table);
        self.clear_bound();
        result
    }

    /// Body of [`run_ingest`](Self::run_ingest), split out so the caller clears bound data on
    /// every exit path.
    fn run_bound_ingest(&self, table: &str) -> Result<Option<i64>> {
        if self.config.is_read_only() {
            return Err(invalid_state("cannot ingest: the connection is read-only"));
        }
        // Check the transaction kind before the DDL, which runs immediately and would otherwise
        // create or drop the table before the ingest is rejected. The check is repeated under the
        // lock at buffer time; holding the connection-wide lock across the admin RPC instead
        // would stall the connection for seconds.
        {
            let txn = lock_txn(&self.txn);
            if !txn.autocommit() {
                txn.check_kind_allowed(TxnKind::Dml)?;
            }
        }
        self.check_ingest_names(table)?;
        let ingest_ddl = self.build_ingest_table_ddl(table, self.ingest_mode)?;
        if let Some(ddl) = ingest_ddl {
            // DDL can't be rolled back, and `replace` has already dropped the old table when the
            // mutations are built, so a bad value must fail first.
            for batch in &self.bound {
                bind::check_ingest_values(batch)?;
            }
            self.run_ddl(ddl)
                .map_err(|error| self.remap_ingest_create_error(table, error))?;
        }
        self.run_ingest_mutations(table)
            .map_err(|error| self.remap_ingest_append_error(table, error))
    }

    /// Reject names Spanner can never accept with `InvalidArguments` in every mode. The create
    /// modes would catch them while building DDL; `append` would send them in mutations and get
    /// back a missing-table or missing-column error.
    fn check_ingest_names(&self, table: &str) -> Result<()> {
        crate::sql::qualified_table(self.target_db_schema.as_deref(), table)?;
        if let Some(schema) = self.bound_ingest_schema() {
            for field in schema.fields() {
                crate::sql::quote_ident(field.name()).map_err(|e| {
                    annotate(e, |m| format!("ingest column {:?}: {m}", field.name()))
                })?;
            }
        }
        Ok(())
    }

    /// Map a failed `create`-mode DDL onto `AlreadyExists`, as ADBC requires, when the table
    /// exists. Spanner reports it as a generic schema error ("Duplicate name in schema"), so
    /// confirm with a probe. If the table is absent or the probe fails, the DDL error stands.
    fn remap_ingest_create_error(&self, table: &str, error: Error) -> Error {
        if !matches!(self.ingest_mode, None | Some(IngestMode::Create)) {
            return error;
        }
        // `Integrity` covers a backend reporting the table as gRPC ALREADY_EXISTS.
        if !matches!(
            error.status,
            Status::InvalidArguments
                | Status::InvalidState
                | Status::NotFound
                | Status::AlreadyExists
                | Status::Integrity
        ) {
            return error;
        }
        match self.ingest_table_exists(table) {
            Ok(true) => err(
                format!(
                    "bulk ingest create target table {table:?} already exists ({})",
                    error.message
                ),
                Status::AlreadyExists,
            ),
            Ok(false) | Err(_) => error,
        }
    }

    /// Write the bound rows as insert mutations, which skip per-row SQL parsing and planning but
    /// keep `INSERT` semantics: a duplicate primary key fails with `ALREADY_EXISTS`.
    ///
    /// In a manual transaction the mutations are buffered for the commit, which applies them after
    /// the transaction's DML. All are built before any is buffered, so a conversion failure leaves
    /// the buffer unchanged. Manual ingests are not chunked; an oversized one fails at commit.
    ///
    /// In autocommit mode each chunk commits in its own write-only transaction, because Spanner
    /// limits a commit to about 80,000 mutations (roughly rows times columns, plus index entries)
    /// and 100 MB. An ingest that fits one chunk stays atomic. A chunk that still exceeds the
    /// mutation limit is split; see [`write_mutation_range`](Self::write_mutation_range).
    fn run_ingest_mutations(&self, table: &str) -> Result<Option<i64>> {
        let target = bind::mutation_table(self.target_db_schema.as_deref(), table);
        let manual = {
            let txn = lock_txn(&self.txn);
            if txn.autocommit() {
                false
            } else {
                txn.check_kind_allowed(TxnKind::Dml)?;
                true
            }
        };
        if manual {
            // Build outside the connection-wide txn lock, since it is O(rows).
            let rows = self.bound.iter().map(RecordBatch::num_rows).sum();
            let mutations = self.build_range_mutations(&target, 0, rows)?;
            // An empty append buffers nothing, so the commit would never reveal a missing table.
            self.check_empty_append_target(table, rows as i64)?;
            let mut txn = lock_txn(&self.txn);
            if !txn.autocommit() {
                // `buffer_mutation` re-checks the kind, since a concurrent statement may have
                // fixed it while unlocked; a rejection fails the first call, buffering nothing.
                for mutation in mutations {
                    txn.buffer_mutation(mutation)?;
                }
                return Ok(None);
            }
            // Autocommit was enabled (committing the manual transaction) while building; fall
            // through to the autocommit path.
        }
        // Cut the flattened rows into `[start, end)` chunks. Mutations are built from the batches
        // per chunk, so nothing is copied up front in case a chunk has to be split.
        let mut total = 0_i64;
        let mut budget = IngestChunkBudget::default();
        let mut chunk_start = 0_usize;
        let mut row_index = 0_usize;
        for batch in &self.bound {
            let columns = batch.num_columns();
            let row_bytes = estimated_row_bytes(batch);
            for _ in 0..batch.num_rows() {
                if !budget.fits(columns, row_bytes) {
                    total += self.commit_ingest_range(&target, chunk_start, row_index, total)?;
                    budget = IngestChunkBudget::default();
                    chunk_start = row_index;
                }
                budget.add(columns, row_bytes);
                row_index += 1;
            }
        }
        total += self.commit_ingest_range(&target, chunk_start, row_index, total)?;
        self.check_empty_append_target(table, total)?;
        Ok(Some(total))
    }

    /// Return `NotFound` for an empty `append` into a missing table. With no rows there is no
    /// insert error to remap, but ADBC requires `NotFound` regardless. The other modes create the
    /// table. If the probe fails, the empty ingest succeeds.
    fn check_empty_append_target(&self, table: &str, ingested: i64) -> Result<()> {
        if ingested == 0
            && matches!(self.ingest_mode, Some(IngestMode::Append))
            && matches!(self.ingest_table_exists(table), Ok(false))
        {
            return Err(err(
                format!("bulk ingest append target table {table:?} not found"),
                Status::NotFound,
            ));
        }
        Ok(())
    }

    /// Build insert mutations for the flattened row range `[start, end)` of the bound batches.
    fn build_range_mutations(
        &self,
        target: &str,
        start: usize,
        end: usize,
    ) -> Result<Vec<Mutation>> {
        let mut mutations = Vec::with_capacity(end.saturating_sub(start));
        let mut base = 0_usize;
        for batch in &self.bound {
            let rows = batch.num_rows();
            // Intersect with this batch's rows `[base, base + rows)`, as batch-local offsets.
            let lo = start.max(base).saturating_sub(base);
            let hi = end.min(base + rows).saturating_sub(base);
            for row in lo..hi {
                mutations.push(bind::insert_mutation(target, batch, row)?);
            }
            base += rows;
            if base >= end {
                break;
            }
        }
        Ok(mutations)
    }

    /// Commit rows `[start, end)` as one autocommit chunk, in a write-only transaction or, with
    /// `spanner.ingest.batch_write`, through BatchWrite. Returns the number of rows written.
    /// `prior_total` counts rows committed by earlier chunks, for the error if this one fails.
    fn commit_ingest_range(
        &self,
        target: &str,
        start: usize,
        end: usize,
        prior_total: i64,
    ) -> Result<i64> {
        if start >= end {
            return Ok(0);
        }
        if self.ingest_batch_write {
            // BatchWrite applies each row's group independently, so the per-commit mutation
            // limit, and the split that works around it, don't apply.
            let mutations = self
                .build_range_mutations(target, start, end)
                .map_err(|e| note_rows_already_committed(e, prior_total))?;
            self.batch_write_chunk(mutations, prior_total)
        } else {
            self.write_mutation_range(target, start, end, prior_total)
        }
    }

    /// Commit rows `[start, end)` in one write-only transaction, splitting the range in half and
    /// retrying if Spanner rejects it for too many mutations.
    ///
    /// Chunks are sized by rows times columns, but the commit also counts secondary-index entries
    /// the driver can't see, so an indexed table can still exceed the limit. Other errors, and a
    /// single row that is still too large, propagate. A split chunk is not atomic; `prior_total`
    /// is carried through the recursion so a failure reports every row committed before it.
    fn write_mutation_range(
        &self,
        target: &str,
        start: usize,
        end: usize,
        prior_total: i64,
    ) -> Result<i64> {
        let mutations = self
            .build_range_mutations(target, start, end)
            .map_err(|e| note_rows_already_committed(e, prior_total))?;
        // A commit reports no row count, but each insert mutation is one row.
        let count = mutations.len() as i64;
        // Write-only transactions have replay protection, so internal retries on `ABORTED` still
        // apply the mutations once.
        match write_mutations_txn(
            &self.runtime,
            &self.client,
            &self.cancel.current(),
            &self.config,
            mutations,
        ) {
            Ok(()) => Ok(count),
            Err(error) if end - start > 1 && is_mutation_limit_exceeded(&error) => {
                let mid = start + (end - start) / 2;
                let left = self.write_mutation_range(target, start, mid, prior_total)?;
                let right = self.write_mutation_range(target, mid, end, prior_total + left)?;
                Ok(left + right)
            }
            Err(error) => Err(note_rows_already_committed(error, prior_total)),
        }
    }

    /// Apply one mutation group per row through the non-atomic BatchWrite RPC. Keep draining after
    /// a group fails to count every confirmed write. Errors are annotated outside the
    /// cancellation/timeout boundary so interrupted streams also report confirmed rows.
    fn batch_write_chunk(&self, mutations: Vec<Mutation>, prior_total: i64) -> Result<i64> {
        if mutations.is_empty() {
            return Ok(0);
        }
        let groups: Vec<MutationGroup> = mutations
            .into_iter()
            .map(|m| MutationGroup::new(vec![m]))
            .collect();
        let transaction = self
            .config
            .retry
            .apply_to_batch_write(
                self.config
                    .request
                    .apply_to_batch_write(self.client.batch_write_transaction()),
            )
            .build();
        let mut applied = 0_i64;
        block_on_cancellable(
            &self.runtime,
            &self.cancel.current(),
            with_timeout(
                self.config.timeouts.update_timeout(),
                crate::OPTION_RPC_TIMEOUT_UPDATE,
                async {
                    let mut stream = transaction
                        .execute_streaming(groups)
                        .await
                        .map_err(from_spanner)?;
                    let mut first_error = None;
                    while let Some(response) = stream.next().await {
                        let response = response.map_err(from_spanner)?;
                        match response.status.as_ref().filter(|s| s.code != 0) {
                            None => applied += response.indexes.len() as i64,
                            Some(status) if first_error.is_none() => {
                                first_error = Some(from_status_parts(
                                    status.code,
                                    &status.message,
                                    &status.details,
                                ));
                            }
                            Some(_) => {}
                        }
                    }
                    match first_error {
                        Some(error) => Err(error),
                        None => Ok(applied),
                    }
                },
            ),
        )
        .map_err(|error| note_rows_already_committed(error, prior_total + applied))
    }
}

/// Validate `adbc.ingest.target_catalog` against `own`, the connection's database id. Ingest can't
/// cross databases, so any other name, including adbc.h's `""` for "no catalog", is unsupported.
pub(super) fn check_target_catalog(catalog: String, own: &str) -> Result<String> {
    if catalog == own {
        Ok(catalog)
    } else {
        Err(unsupported(format!(
            "ingest target catalog {catalog:?}: this connection can only write to {own:?}; \
             set adbc.ingest.target_catalog to that or leave it unset"
        )))
    }
}

/// Parse `adbc.ingest.mode`, accepting the canonical `adbc.ingest.mode.*` values and their short
/// forms (`append`, `create`, ...).
pub(super) fn ingest_mode_option(key: &OptionStatement, value: OptionValue) -> Result<IngestMode> {
    use adbc_core::constants::{
        ADBC_INGEST_OPTION_MODE_APPEND, ADBC_INGEST_OPTION_MODE_CREATE,
        ADBC_INGEST_OPTION_MODE_CREATE_APPEND, ADBC_INGEST_OPTION_MODE_REPLACE,
    };
    match string_option(key, value)?.as_str() {
        ADBC_INGEST_OPTION_MODE_APPEND | "append" => Ok(IngestMode::Append),
        ADBC_INGEST_OPTION_MODE_CREATE | "create" => Ok(IngestMode::Create),
        ADBC_INGEST_OPTION_MODE_CREATE_APPEND | "create_append" => Ok(IngestMode::CreateAppend),
        ADBC_INGEST_OPTION_MODE_REPLACE | "replace" => Ok(IngestMode::Replace),
        other => Err(not_implemented(&format!("ingest mode {other:?}"))),
    }
}

/// Parse `spanner.ingest.batch_write`; an empty or whitespace-only string resets it to `false`.
pub(super) fn ingest_batch_write_option(value: OptionValue) -> Result<bool> {
    crate::options::bool_option_unsettable(
        value,
        &format!("option {}", crate::OPTION_INGEST_BATCH_WRITE),
    )
}

/// Add to a failed autocommit ingest's error how many rows were already committed and remain in
/// the table. On the BatchWrite path `committed` includes the failing chunk's applied groups.
///
/// A timeout or cancellation drops the in-flight commit, which may still land, so the note then
/// also warns that the failing chunk's outcome is unknown. Status, vendor code and details are
/// preserved.
fn note_rows_already_committed(error: Error, committed: i64) -> Error {
    let outcome_unknown = matches!(error.status, Status::Timeout | Status::Cancelled);
    if committed == 0 && !outcome_unknown {
        return error;
    }
    let mut note = String::new();
    if committed > 0 {
        note.push_str(&format!(
            "{committed} row(s) from this bulk ingest were already committed and remain in the \
             table"
        ));
    }
    if outcome_unknown {
        if !note.is_empty() {
            note.push_str("; ");
        }
        note.push_str(
            "this chunk's own commit outcome is unknown: it may still have landed server-side, so \
             retrying could duplicate rows",
        );
    }
    annotate(error, |message| format!("{message} ({note})"))
}

/// Whether `error` is Spanner's per-commit mutation limit rejection, which an autocommit ingest
/// recovers from by splitting the chunk.
///
/// Spanner reports it as `INVALID_ARGUMENT` with "The transaction contains too many mutations",
/// wording that has survived each increase of the limit. Other `INVALID_ARGUMENT`s, such as bad
/// values or schema mismatches, must propagate to the ingest error remapping. The commit size
/// limit isn't matched: the byte budget keeps chunks well under it, and its wording is less stable.
fn is_mutation_limit_exceeded(error: &Error) -> bool {
    error.status == Status::InvalidArguments
        && error
            .message
            .to_ascii_lowercase()
            .contains("too many mutations")
}

/// The batch's Arrow bytes per row. Counts only the rows a sliced batch references: a slice keeps
/// its parent's buffers, so their capacity would make every row of a small slice look huge and
/// commit it alone. Dictionary values, list children and view buffers are still counted whole,
/// which errs high, the safe direction for a budget.
fn estimated_row_bytes(batch: &RecordBatch) -> usize {
    let bytes: usize = batch
        .columns()
        .iter()
        .map(|column| {
            column
                .to_data()
                .get_slice_memory_size()
                .unwrap_or_else(|_| column.get_array_memory_size())
        })
        .sum();
    bytes / batch.num_rows().max(1)
}

/// Mutations per ingest chunk. Spanner allows about 80,000 per commit, but secondary-index entries
/// the driver can't see count too, so stay at a quarter of that.
const INGEST_CHUNK_MUTATION_LIMIT: u64 = 20_000;

/// Approximate bytes per ingest chunk. Well under Spanner's 100 MB commit limit and typical 10 MB
/// gRPC request limits, because the per-row estimate is rough.
const INGEST_CHUNK_BYTE_BUDGET: u64 = 4 * 1024 * 1024;

/// Running mutation and byte totals of one ingest chunk; each row costs its column count in
/// mutations.
#[derive(Default)]
struct IngestChunkBudget {
    rows: u64,
    mutations: u64,
    bytes: u64,
}

impl IngestChunkBudget {
    /// Whether another row fits. The first row always fits, so an oversized row gets a chunk of its
    /// own instead of an empty chunk.
    fn fits(&self, columns: usize, row_bytes: usize) -> bool {
        self.rows == 0
            || (self.mutations.saturating_add(columns as u64) <= INGEST_CHUNK_MUTATION_LIMIT
                && self.bytes.saturating_add(row_bytes as u64) <= INGEST_CHUNK_BYTE_BUDGET)
    }

    fn add(&mut self, columns: usize, row_bytes: usize) {
        self.rows += 1;
        self.mutations = self.mutations.saturating_add(columns as u64);
        self.bytes = self.bytes.saturating_add(row_bytes as u64);
    }
}

#[cfg(test)]
mod tests;

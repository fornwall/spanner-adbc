//! Bulk ingest: `adbc.ingest.*` option parsing, create/replace table DDL, and the mutation write
//! paths (chunked write-only commits, or non-atomic BatchWrite).

use std::collections::HashSet;
use std::ops::Range;

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::{IngestMode, OptionStatement, OptionValue};
use arrow_array::cast::AsArray;
use arrow_array::{Array, RecordBatch};
use arrow_schema::Schema;
use google_cloud_spanner::mutation::{Mutation, MutationGroup};

use super::{SpannerStatement, string_option};
use crate::bind;
use crate::connection::{TxnKind, lock_txn, write_mutations_txn};
use crate::conversion::TimestampPrecision;
use crate::error::{
    annotate, commit_outcome_unknown, err, from_spanner, from_status_parts, invalid_state,
    is_mutation_limit_exceeded, unsupported,
};
use crate::metadata::{query_single_use, table_columns_query};

impl SpannerStatement {
    /// DDL to run before an ingest, which is none for append.
    ///
    /// `create` (the default) fails if the table exists, `create_append` creates it if absent, and
    /// `replace` drops any existing table first. Columns come from the bound data's schema.
    fn build_ingest_table_ddl(&self, table: &str, schema: &Schema) -> Result<Vec<String>> {
        let db_schema = self.target_db_schema.as_deref();
        let create =
            |if_not_exists| bind::create_table_sql(table, db_schema, schema, if_not_exists);
        Ok(match self.ingest_mode {
            IngestMode::Append => Vec::new(),
            IngestMode::Create => vec![create(false)?],
            IngestMode::CreateAppend => vec![create(true)?],
            IngestMode::Replace => {
                let target = crate::sql::qualified_table(db_schema, table)?;
                vec![format!("DROP TABLE IF EXISTS {target}"), create(false)?]
            }
        })
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
            IngestMode::Append | IngestMode::CreateAppend
        ) {
            return error;
        }
        match error.status {
            // The rows conflict with the table's data or constraints, not its schema. Skip the
            // probe, whose "incompatible schema" message would misreport it, and keep the vendor
            // code and details.
            Status::Integrity => {
                return annotate(error, |message| {
                    format!("bulk ingest append into table {table:?} failed: {message}")
                });
            }
            Status::InvalidArguments | Status::InvalidData | Status::NotFound => {}
            // Table existence says nothing about an authorization, cancellation, timeout, quota
            // or transport failure, and probing could delay cancellation.
            _ => return error,
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
    pub(super) fn run_ingest(&self, table: &str) -> Result<Option<i64>> {
        // An empty bound stream still provides a schema, enough to create the table.
        let Some(schema) = &self.bound_schema else {
            return Err(invalid_state("cannot ingest: no data has been bound"));
        };
        if self.config.is_read_only() {
            return Err(invalid_state("cannot ingest: the connection is read-only"));
        }
        // Check the transaction kind before the DDL, which runs immediately and would otherwise
        // create or drop the table before the ingest is rejected. The check is repeated under the
        // lock at buffer time; holding the connection-wide lock across the admin RPC instead
        // would stall the connection for seconds.
        lock_txn(&self.txn).check_kind_allowed(TxnKind::Dml)?;
        self.check_ingest_names(table, schema)?;
        let ddl = self.build_ingest_table_ddl(table, schema)?;
        // A bad value must fail before anything is written: DDL can't be rolled back, `replace`
        // drops the old table, and a chunked autocommit ingest would leave earlier chunks
        // committed.
        for batch in &self.bound {
            bind::check_ingest_values(batch)?;
        }
        if !ddl.is_empty() {
            self.run_ddl(ddl)
                .map_err(|error| self.remap_ingest_ddl_error(table, error))?;
        }
        // No insert reports a missing table or column at execution when there are no rows, or
        // when the inserts are buffered until commit.
        if self.bound_rows() == 0 || !lock_txn(&self.txn).autocommit() {
            self.check_append_target(table, schema)?;
        }
        self.run_ingest_mutations(table)
            .map_err(|error| self.remap_ingest_append_error(table, error))
    }

    /// Reject names Spanner can never accept with `InvalidArguments` in every mode. The create
    /// modes would catch them while building DDL; `append` would send them in mutations and get
    /// back a missing-table or missing-column error.
    fn check_ingest_names(&self, table: &str, schema: &Schema) -> Result<()> {
        crate::sql::qualified_table(self.target_db_schema.as_deref(), table)?;
        for field in schema.fields() {
            crate::sql::quote_ident(field.name())
                .map_err(|e| annotate(e, |m| format!("ingest column {:?}: {m}", field.name())))?;
        }
        Ok(())
    }

    /// Map a failed `create`-mode DDL onto `AlreadyExists`, as ADBC requires, when the table
    /// exists. Spanner reports it as a generic schema error ("Duplicate name in schema"), so
    /// confirm with a probe. If the table is absent or the probe fails, the DDL error stands.
    ///
    /// In `replace` mode, explain Spanner's refusal to drop a table that other schema objects
    /// depend on.
    fn remap_ingest_ddl_error(&self, table: &str, error: Error) -> Error {
        match self.ingest_mode {
            IngestMode::Replace
                if error.status == Status::InvalidState
                    && error
                        .message
                        .to_ascii_lowercase()
                        .contains("cannot drop table") =>
            {
                annotate(error, |message| {
                    format!(
                        "bulk ingest replace of table {table:?} failed: {message} Replace mode \
                         drops and recreates only the table, so first drop the indexes, \
                         interleaved tables, foreign keys or views that depend on it, or ingest \
                         with mode append"
                    )
                })
            }
            // `Integrity` covers a backend reporting the table as gRPC ALREADY_EXISTS.
            IngestMode::Create
                if matches!(
                    error.status,
                    Status::InvalidArguments
                        | Status::InvalidState
                        | Status::NotFound
                        | Status::AlreadyExists
                        | Status::Integrity
                ) =>
            {
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
            _ => error,
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
        if !lock_txn(&self.txn).autocommit() {
            // Build outside the connection-wide txn lock, since it is O(rows).
            let mutations = self.build_range_mutations(&target, 0..self.bound_rows())?;
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
        // Mutations are built from the batches per chunk, so nothing is copied up front in case a
        // chunk has to be split.
        let mut total = 0_i64;
        for chunk in chunk_ranges(&self.bound) {
            total += if self.ingest_batch_write {
                // BatchWrite applies each row's group independently, so the per-commit mutation
                // limit, and the split that works around it, don't apply.
                self.batch_write_chunk(self.build_range_mutations(&target, chunk)?, total)?
            } else {
                self.write_mutation_range(&target, chunk, total)?
            };
        }
        Ok(Some(total))
    }

    /// Check an `append` or `create_append` target for what ADBC requires: `NotFound` for a missing
    /// table and `AlreadyExists` for a bound column it lacks. The other modes just created the
    /// table from this schema. If the probe fails, the ingest proceeds.
    fn check_append_target(&self, table: &str, schema: &Schema) -> Result<()> {
        if !matches!(
            self.ingest_mode,
            IngestMode::Append | IngestMode::CreateAppend
        ) {
            return Ok(());
        }
        let columns = match self.append_target_columns(table) {
            Ok(Some(columns)) => columns,
            Ok(None) => {
                return Err(err(
                    format!("bulk ingest append target table {table:?} not found"),
                    Status::NotFound,
                ));
            }
            Err(_) => return Ok(()),
        };
        let mut names = schema.fields().iter().map(|field| field.name());
        match names.find(|name| !columns.contains(&name.to_ascii_lowercase())) {
            Some(name) => Err(err(
                format!(
                    "bulk ingest append into table {table:?} failed: the bound data is \
                     incompatible with the existing table's schema, which has no column {name:?}"
                ),
                Status::AlreadyExists,
            )),
            None => Ok(()),
        }
    }

    /// The lowercased column names of `table` in the ingest target schema, or `None` if there is
    /// no such table. Spanner names are case-insensitive ASCII.
    fn append_target_columns(&self, table: &str) -> Result<Option<HashSet<String>>> {
        let db_schema = self.target_db_schema.as_deref().unwrap_or("");
        let lookup = table_columns_query(&self.config, db_schema, table);
        let precision = TimestampPrecision::default();
        let found = self.block_on_query(query_single_use(&self.client, None, lookup, precision))?;
        // A table without columns has one row with a null name.
        Ok((found.num_rows() > 0).then(|| {
            let names = found.column(0).as_string::<i32>();
            names
                .iter()
                .flatten()
                .map(str::to_ascii_lowercase)
                .collect()
        }))
    }

    /// Build insert mutations for the flattened row range `rows` of the bound batches.
    fn build_range_mutations(&self, target: &str, rows: Range<usize>) -> Result<Vec<Mutation>> {
        let mut mutations = Vec::with_capacity(rows.len());
        let mut base = 0_usize;
        for batch in &self.bound {
            // Intersect with this batch's rows, as batch-local offsets.
            let lo = rows.start.max(base) - base;
            let hi = rows.end.min(base + batch.num_rows()) - base;
            for row in lo..hi {
                mutations.push(bind::insert_mutation(target, batch, row)?);
            }
            base += batch.num_rows();
            if base >= rows.end {
                break;
            }
        }
        Ok(mutations)
    }

    /// Commit `rows` in one write-only transaction, splitting the range in half and retrying if
    /// Spanner rejects it for too many mutations. Returns the number of rows written.
    ///
    /// Chunks are sized by rows times columns, but the commit also counts secondary-index entries
    /// the driver can't see, so an indexed table can still exceed the limit. Other errors, and a
    /// single row that is still too large, propagate. A split chunk is not atomic; `prior_total`
    /// counts the rows committed before this range, for the error if it fails.
    fn write_mutation_range(
        &self,
        target: &str,
        rows: Range<usize>,
        prior_total: i64,
    ) -> Result<i64> {
        let mutations = self.build_range_mutations(target, rows.clone())?;
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
            Err(error) if rows.len() > 1 && is_mutation_limit_exceeded(&error) => {
                let mid = rows.start + rows.len() / 2;
                let left = self.write_mutation_range(target, rows.start..mid, prior_total)?;
                let right = self.write_mutation_range(target, mid..rows.end, prior_total + left)?;
                Ok(left + right)
            }
            Err(error) => Err(note_rows_already_committed(error, prior_total)),
        }
    }

    /// Apply one mutation group per row through the non-atomic BatchWrite RPC. Keep draining after
    /// a group fails to count every confirmed write. Errors are annotated outside the
    /// cancellation/timeout boundary so interrupted streams also report confirmed rows.
    fn batch_write_chunk(&self, mutations: Vec<Mutation>, prior_total: i64) -> Result<i64> {
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
        self.block_on_update(async {
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
        })
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
        other => Err(unsupported(format!(
            "ingest mode {other:?} is not supported; use {ADBC_INGEST_OPTION_MODE_CREATE}, \
             {ADBC_INGEST_OPTION_MODE_APPEND}, {ADBC_INGEST_OPTION_MODE_CREATE_APPEND} or \
             {ADBC_INGEST_OPTION_MODE_REPLACE}"
        ))),
    }
}

/// Add to a failed autocommit ingest's error how many rows were already committed and remain in
/// the table. On the BatchWrite path `committed` includes the failing chunk's applied groups.
///
/// When [`commit_outcome_unknown`], the note also warns that the failing chunk may have landed.
/// Status, vendor code and details are preserved.
fn note_rows_already_committed(error: Error, committed: i64) -> Error {
    let mut notes = Vec::new();
    if committed > 0 {
        notes.push(format!(
            "{committed} row(s) from this bulk ingest were already committed and remain in the \
             table"
        ));
    }
    if commit_outcome_unknown(&error) {
        notes.push(
            "this chunk's own commit outcome is unknown: it may still have landed server-side, so \
             retrying could duplicate rows"
                .to_string(),
        );
    }
    if notes.is_empty() {
        return error;
    }
    annotate(error, |message| format!("{message} ({})", notes.join("; ")))
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

/// Cut the bound rows, numbered across batches, into non-empty chunks for one commit each. Each
/// row costs its column count in mutations.
fn chunk_ranges(batches: &[RecordBatch]) -> Vec<Range<usize>> {
    let mut chunks = Vec::new();
    let (mut start, mut row) = (0, 0);
    let (mut mutations, mut bytes) = (0_u64, 0_u64);
    for batch in batches {
        let columns = batch.num_columns() as u64;
        let row_bytes = estimated_row_bytes(batch) as u64;
        for _ in 0..batch.num_rows() {
            // A chunk's first row always fits, so an oversized row gets a chunk of its own.
            if row > start
                && (mutations + columns > INGEST_CHUNK_MUTATION_LIMIT
                    || bytes + row_bytes > INGEST_CHUNK_BYTE_BUDGET)
            {
                chunks.push(start..row);
                (start, mutations, bytes) = (row, 0, 0);
            }
            mutations += columns;
            bytes += row_bytes;
            row += 1;
        }
    }
    if row > start {
        chunks.push(start..row);
    }
    chunks
}

#[cfg(test)]
mod tests;

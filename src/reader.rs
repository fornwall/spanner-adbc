//! Streaming Arrow readers over Spanner result sets.
//!
//! Rows arrive in chunks: the first is pulled up front to settle the schema, and a background task
//! prefetches the rest while the caller converts earlier ones into record batches.

use std::sync::Arc;
use std::time::Duration;

use adbc_core::error::{Result, Status};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, Schema, SchemaRef};
use google_cloud_spanner::result::{ResultSet, Row};
use google_cloud_spanner::statement::Statement as SpannerSql;
use google_cloud_spanner::transaction::MultiUseReadOnlyTransaction;
use google_cloud_spanner::value::{Kind, Value};

use crate::conversion::{TimestampPrecision, build_batch, build_schema};
use crate::error::{err, from_spanner};
use crate::runtime::{
    CancelSignal, ChunkSource, SharedRuntime, block_on_cancellable, spawn_prefetch,
};
use crate::timeout::with_timeout;

/// Approximate cap on the row memory held per chunk, alongside the row-count cap.
const CHUNK_BYTE_BUDGET: usize = 32 * 1024 * 1024;

/// Pull up to `max` rows, stopping early at EOF or once [`CHUNK_BYTE_BUDGET`] is reached.
///
/// `rows` is not pre-reserved: `max` is only a cap, and `spanner.rows_per_batch` is unbounded.
async fn pull_chunk(rs: &mut ResultSet, max: usize) -> Result<Vec<Row>> {
    let mut rows = Vec::new();
    let mut bytes: usize = 0;
    while rows.len() < max {
        let Some(row) = rs.next().await else {
            break;
        };
        let row = row.map_err(from_spanner)?;
        bytes = bytes.saturating_add(approx_row_bytes(&row));
        rows.push(row);
        // Checked after pushing, so an oversized row still forms a one-row chunk.
        if bytes >= CHUNK_BYTE_BUDGET {
            break;
        }
    }
    Ok(rows)
}

/// Estimate retained memory, excluding shared metadata and unused allocation capacity.
fn approx_row_bytes(row: &Row) -> usize {
    size_of::<Row>()
        + row
            .raw_values()
            .iter()
            .map(approx_value_bytes)
            .sum::<usize>()
}

/// Counts each value's own storage, even for nulls and empty containers; counting only payload
/// bytes would leave large arrays of nulls or small scalars unbounded.
fn approx_value_bytes(value: &Value) -> usize {
    let payload = match value.kind() {
        Kind::String => value.as_str().expect("string kind").len(),
        Kind::List => value
            .as_list()
            .expect("list kind")
            .iter()
            .map(approx_value_bytes)
            .sum(),
        _ => 0,
    };
    size_of::<Value>() + payload
}

/// Stream a [`ResultSet`] as Arrow batches.
///
/// The first chunk is pulled here, under the caller's query deadline, because Spanner sends the
/// column metadata with it. A background task prefetches the rest, each fetch bounded by
/// `fetch_timeout`. `fallback_schema` is used when the result carries no columns.
pub(crate) async fn stream_query(
    runtime: SharedRuntime,
    cancel: CancelSignal,
    mut rs: ResultSet,
    batch_size: usize,
    precision: TimestampPrecision,
    fetch_timeout: Option<Duration>,
    fallback_schema: Option<SchemaRef>,
) -> Result<SpannerBatchReader> {
    let first = pull_chunk(&mut rs, batch_size).await?;
    let schema = match fallback_schema {
        Some(schema) if rs.metadata().column_names().is_empty() => schema,
        _ => build_schema(rs.metadata(), precision)?,
    };
    let source = ResultSetChunks {
        result_set: rs,
        batch_size,
        fetch_timeout,
    };
    Ok(SpannerBatchReader::new(
        runtime,
        cancel,
        schema,
        Some(first),
        source,
    ))
}

/// Prefetch source for a single result set.
struct ResultSetChunks {
    result_set: ResultSet,
    batch_size: usize,
    fetch_timeout: Option<Duration>,
}

impl ChunkSource for ResultSetChunks {
    type Row = Row;

    fn next_chunk(&mut self) -> impl std::future::Future<Output = Result<Vec<Row>>> + Send {
        with_timeout(
            self.fetch_timeout,
            crate::OPTION_RPC_TIMEOUT_FETCH,
            pull_chunk(&mut self.result_set, self.batch_size),
        )
    }
}

/// A streaming [`RecordBatchReader`] over Spanner rows.
///
/// Rows are converted in chunks of `spanner.rows_per_batch` rows, also capped by
/// [`CHUNK_BYTE_BUDGET`]. A background task keeps at most one undelivered chunk buffered plus one
/// in flight. The [`CancelSignal`] belongs to the producing operation, so a cancelled reader stays
/// cancelled and later operations on the statement or connection are unaffected.
pub(crate) struct SpannerBatchReader {
    runtime: SharedRuntime,
    cancel: CancelSignal,
    schema: SchemaRef,
    /// The chunk pulled up front to settle the schema.
    first: Option<Vec<Row>>,
    /// `None` once the stream has ended, failed or been cancelled.
    chunks: Option<crate::runtime::ChunkReceiver<Row>>,
    task: tokio::task::JoinHandle<()>,
}

impl SpannerBatchReader {
    /// Start prefetching the rest of `source`.
    fn new<S: ChunkSource<Row = Row>>(
        runtime: SharedRuntime,
        cancel: CancelSignal,
        schema: SchemaRef,
        first: Option<Vec<Row>>,
        source: S,
    ) -> Self {
        let (chunks, task) = spawn_prefetch(&runtime, cancel.clone(), source);
        Self {
            runtime,
            cancel,
            schema,
            first,
            chunks: Some(chunks),
            task,
        }
    }
}

impl Drop for SpannerBatchReader {
    fn drop(&mut self) {
        // Dropping the receiver would stop the task only at its next send; abort also cancels an
        // in-flight fetch.
        self.task.abort();
    }
}

/// Box a driver error as an [`ArrowError`]. The FFI stream export downcasts it to recover the
/// ADBC status, so it must not be stringified.
fn to_arrow_error(e: adbc_core::error::Error) -> ArrowError {
    ArrowError::ExternalError(Box::new(e))
}

impl Iterator for SpannerBatchReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let rx = self.chunks.as_mut()?;
        let first = &mut self.first;
        let task = &mut self.task;
        // Check cancellation before yielding any rows, including the chunk fetched by execute().
        let rows = block_on_cancellable(&self.runtime, &self.cancel, async {
            if let Some(rows) = first.take()
                && !rows.is_empty()
            {
                return Ok(Some(rows));
            }
            let chunk = rx.recv().await;
            if chunk.is_none() {
                // A task panic also closes the channel. Check completion before reporting EOF
                // so a failed background fetch cannot silently truncate a result.
                task.await.map_err(|error| {
                    err(
                        format!("background row fetch failed; result is incomplete: {error}"),
                        Status::Internal,
                    )
                })?;
            }
            chunk.transpose()
        });
        match rows.and_then(|rows| {
            rows.map(|rows| build_batch(self.schema.clone(), &rows))
                .transpose()
        }) {
            Ok(None) => {
                self.chunks = None;
                None
            }
            Ok(Some(batch)) => Some(Ok(batch)),
            Err(e) => {
                self.first = None;
                self.chunks = None;
                self.task.abort();
                Some(Err(to_arrow_error(e)))
            }
        }
    }
}

impl RecordBatchReader for SpannerBatchReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Builds a bound query's per-row statements lazily, so a large `executemany` SELECT holds one
/// statement at a time. A bind failure surfaces as `Some(Err(..))`.
pub(crate) trait BoundStatementSource: Send {
    fn next_statement(&mut self) -> Option<Result<SpannerSql>>;
}

/// Stream the results of a bound query's per-row statements as one reader, executing them all in
/// `transaction` so every row sees the same snapshot.
///
/// The first statement runs here to settle the schema, which all statements share since they run
/// the same SQL. The prefetch task owns `transaction` (the connection's snapshot in a manual
/// transaction, a dedicated one in autocommit) and keeps it alive while pulling chunks. Read-only
/// transactions need no commit, so dropping it is enough.
pub(crate) async fn stream_bound_query(
    runtime: SharedRuntime,
    cancel: CancelSignal,
    transaction: Arc<MultiUseReadOnlyTransaction>,
    mut statements: Box<dyn BoundStatementSource>,
    batch_size: usize,
    precision: TimestampPrecision,
    fetch_timeout: Option<Duration>,
) -> Result<SpannerBatchReader> {
    let mut result_set = match statements.next_statement() {
        Some(statement) => Some(
            transaction
                .execute_query(statement?)
                .await
                .map_err(from_spanner)?,
        ),
        // No bound rows: an empty reader.
        None => None,
    };
    let (first, schema) = match result_set.as_mut() {
        Some(rs) => {
            let rows = pull_chunk(rs, batch_size).await?;
            let schema = build_schema(rs.metadata(), precision)?;
            (Some(rows), schema)
        }
        None => (None, Arc::new(Schema::empty())),
    };
    let source = BoundQueryChunks {
        transaction,
        statements,
        result_set,
        batch_size,
        fetch_timeout,
    };
    Ok(SpannerBatchReader::new(
        runtime, cancel, schema, first, source,
    ))
}

/// Prefetch source for a bound query. The fetch timeout bounds each `next_chunk`, including any
/// execution of the next row's statement.
struct BoundQueryChunks {
    transaction: Arc<MultiUseReadOnlyTransaction>,
    statements: Box<dyn BoundStatementSource>,
    /// The result set currently being drained.
    result_set: Option<ResultSet>,
    batch_size: usize,
    fetch_timeout: Option<Duration>,
}

impl BoundQueryChunks {
    /// Pull the next non-empty chunk, executing further statements as result sets drain so a row
    /// with an empty result yields no empty batch. Empty, ending the stream, once all are drained.
    async fn pull(&mut self) -> Result<Vec<Row>> {
        loop {
            if let Some(rs) = self.result_set.as_mut() {
                let rows = pull_chunk(rs, self.batch_size).await?;
                if !rows.is_empty() {
                    return Ok(rows);
                }
                self.result_set = None;
            }
            let Some(statement) = self.statements.next_statement() else {
                return Ok(Vec::new());
            };
            let rs = self.transaction.execute_query(statement?).await;
            self.result_set = Some(rs.map_err(from_spanner)?);
        }
    }
}

impl ChunkSource for BoundQueryChunks {
    type Row = Row;

    fn next_chunk(&mut self) -> impl std::future::Future<Output = Result<Vec<Row>>> + Send {
        with_timeout(
            self.fetch_timeout,
            crate::OPTION_RPC_TIMEOUT_FETCH,
            self.pull(),
        )
    }
}

#[cfg(test)]
mod tests;

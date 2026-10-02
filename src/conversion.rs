//! Conversion from Spanner result sets to Arrow record batches.
//!
//! Spanner returns values over the wire in a JSON-ish protobuf encoding (see
//! [`google_cloud_spanner::value::Value`]): integers, dates, timestamps and numerics all arrive as
//! strings, floats as numbers, and so on. The column *types* are carried separately in the result
//! set metadata. We use that metadata to pick an Arrow [`DataType`] per column and then decode each
//! value accordingly.
//!
//! The type mapping is:
//!
//! | Spanner type                                | Arrow type                        |
//! |---------------------------------------------|-----------------------------------|
//! | `BOOL`                                      | `Boolean`                         |
//! | `INT64`                                     | `Int64`                           |
//! | `FLOAT64`                                   | `Float64`                         |
//! | `FLOAT32`                                   | `Float32`                         |
//! | `DATE`                                      | `Date32`                          |
//! | `TIMESTAMP`                                 | `Timestamp(Nanosecond, "UTC")` (default) or `Timestamp(Microsecond, "UTC")` |
//! | `NUMERIC`                                   | `Decimal128(38, 9)`               |
//! | `BYTES`                                     | `Binary`                          |
//! | `STRING`/`UUID`/`INTERVAL`                   | `Utf8`                            |
//! | `JSON`                                      | `Utf8` + `arrow.json` extension   |
//! | `ENUM`                                      | `Int64` (integer ordinal)         |
//! | `PROTO`                                     | `Binary` (raw serialized bytes)   |
//! | `ARRAY<T>`                                  | `List<T>`                         |
//! | `STRUCT<..>`                                | `Struct<..>`                      |
//!
//! `ARRAY` and `STRUCT` map to native Arrow `List`/`Struct` recursively, so nested shapes like
//! `ARRAY<STRUCT<..>>` round-trip with full type fidelity. (Struct field metadata comes from
//! [`Type::struct_type`](google_cloud_spanner::value::Type::struct_type).) Both Spanner and Arrow
//! address struct fields **positionally**, so a Spanner `STRUCT`'s duplicate or empty field names —
//! both legal, e.g. `STRUCT(1 AS x, 2 AS x)` or an unnamed `SELECT`ed expression — are carried over
//! verbatim, each field keeping its own value.
//!
//! `JSON` columns keep `Utf8` storage (the value bytes are the JSON text) but carry the canonical
//! `arrow.json` extension type as field metadata (`ARROW:extension:name` = `arrow.json`). The
//! extension lives on the [`Field`], not the [`DataType`]; for `ARRAY<JSON>` it sits on the list's
//! child (`item`) field. The other Utf8-backed codes stay plain, untagged `Utf8`.
//!
//! `ENUM` maps to `Int64` (its integer ordinal) and `PROTO` to `Binary` (its raw serialized proto2
//! wire bytes) — both lossless. The *structure* behind them (enum member names, proto field
//! layout) lives only in the database's proto descriptor bundle, not in the query metadata, so the
//! driver hands back the faithful primitive rather than a decoded `Dictionary`/`Struct`; a caller
//! who wants the decoded form can `CAST(col AS STRING)` in SQL.
//!
//! The `TIMESTAMP` mapping is selected by [`TimestampPrecision`].

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use adbc_core::error::{Result, Status};
use adbc_core::options::OptionValue;
use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, GenericByteBuilder, PrimitiveBuilder, StringBuilder,
};
use arrow_array::types::{
    ArrowPrimitiveType, ByteArrayType, Date32Type, Decimal128Type, Float32Type, Float64Type,
    Int64Type, TimestampMicrosecondType,
};
use arrow_array::{
    ArrayRef, ListArray, PrimitiveArray, RecordBatch, RecordBatchReader, StructArray,
    TimestampNanosecondArray,
};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{ArrowError, DataType, Field, FieldRef, Fields, Schema, SchemaRef, TimeUnit};
use base64::Engine;
use google_cloud_spanner::result::{ResultSet, ResultSetMetadata, Row};
use google_cloud_spanner::statement::Statement as SpannerSql;
use google_cloud_spanner::transaction::MultiUseReadOnlyTransaction;
use google_cloud_spanner::value::{Kind, Type, TypeCode, Value};

use crate::error::{err, from_spanner, invalid_argument};
use crate::runtime::{
    CancelSignal, ChunkSource, SharedRuntime, block_on_cancellable, spawn_prefetch,
};
use crate::timeout::with_timeout;

/// Field name used for the element of an Arrow `List` (the Arrow convention).
const LIST_ITEM: &str = "item";

/// Maximum nesting depth the server-provided `STRUCT`/`ARRAY` type walk ([`arrow_type`]) will
/// descend before failing loudly — defense-in-depth against a hostile endpoint returning
/// pathological `STRUCT<STRUCT<…>>` metadata, making the guarantee local to the driver's own
/// recursion rather than resting on prost's decode limit. `100` matches that limit; no legitimate
/// result type nests remotely this deep.
const MAX_TYPE_NESTING_DEPTH: usize = 100;

/// Precision and scale of Spanner's `NUMERIC` type (GoogleSQL `NUMERIC` is fixed at 38 / 9).
const NUMERIC_PRECISION: u8 = 38;
const NUMERIC_SCALE: i8 = 9;
/// Spanner `TIMESTAMP` values are absolute instants in UTC.
const TIMESTAMP_TZ: &str = "UTC";

/// Arrow field-metadata key naming a canonical extension type.
const ARROW_EXTENSION_NAME: &str = "ARROW:extension:name";
/// Arrow field-metadata key carrying an extension type's serialized parameters.
const ARROW_EXTENSION_METADATA: &str = "ARROW:extension:metadata";
/// Canonical Arrow extension name for JSON stored as a Utf8 (string) column.
const ARROW_JSON_EXTENSION: &str = "arrow.json";

/// How Spanner `TIMESTAMP` columns map to Arrow — the value of the
/// [`spanner.max_timestamp_precision`](crate::OPTION_MAX_TIMESTAMP_PRECISION) connection/statement
/// option.
///
/// Spanner timestamps span 0001-01-01 to 9999-12-31 at nanosecond precision; Arrow's
/// `Timestamp(Nanosecond)` `i64` spans only ~1677-09-21 to 2262-04-11. The two variants are the
/// two lossless-or-loud ways out of that mismatch; there is deliberately **no** silently-wrapping
/// mode, since a wrapped timestamp is indistinguishable from real data.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum TimestampPrecision {
    /// `Timestamp(Nanosecond, "UTC")`, preserving the wire value's full nanosecond precision. A
    /// well-formed instant outside the representable range is a loud error (naming the column and
    /// value), never a wrapped or null result. The default.
    #[default]
    NanosecondsErrorOnOverflow,
    /// `Timestamp(Microsecond, "UTC")`, covering Spanner's whole 0001–9999 range. Sub-microsecond
    /// wire digits are **truncated toward negative infinity** (`…12:34:56.789012345Z` becomes
    /// `…12:34:56.789012`).
    Microseconds,
}

impl TimestampPrecision {
    /// Option value selecting [`Self::NanosecondsErrorOnOverflow`].
    pub(crate) const NANOSECONDS_ERROR_ON_OVERFLOW: &'static str = "nanoseconds_error_on_overflow";
    /// Option value selecting [`Self::Microseconds`].
    pub(crate) const MICROSECONDS: &'static str = "microseconds";

    /// Parse the `spanner.max_timestamp_precision` option value. The empty string resets to the
    /// default (the staleness-option unset convention); anything else unknown is rejected.
    pub(crate) fn parse_option(value: OptionValue) -> Result<Self> {
        let what = format!("option {}", crate::OPTION_MAX_TIMESTAMP_PRECISION);
        let s = crate::options::string_option(value, &what)?;
        match s.trim() {
            "" => Ok(Self::default()),
            Self::NANOSECONDS_ERROR_ON_OVERFLOW => Ok(Self::NanosecondsErrorOnOverflow),
            Self::MICROSECONDS => Ok(Self::Microseconds),
            other => Err(invalid_argument(format!(
                "{what} expects {:?} or {:?} (or \"\" to reset to the default), got {other:?}",
                Self::NANOSECONDS_ERROR_ON_OVERFLOW,
                Self::MICROSECONDS,
            ))),
        }
    }

    /// The canonical option-value string, for `get_option` round-tripping.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::NanosecondsErrorOnOverflow => Self::NANOSECONDS_ERROR_ON_OVERFLOW,
            Self::Microseconds => Self::MICROSECONDS,
        }
    }

    /// The Arrow [`TimeUnit`] `TIMESTAMP` columns map to under this mode.
    fn time_unit(self) -> TimeUnit {
        match self {
            Self::NanosecondsErrorOnOverflow => TimeUnit::Nanosecond,
            Self::Microseconds => TimeUnit::Microsecond,
        }
    }
}

/// Drain a Spanner [`ResultSet`] and materialise it as a single Arrow [`RecordBatch`] together with
/// its schema.
pub(crate) async fn result_set_to_batch(
    mut rs: ResultSet,
    precision: TimestampPrecision,
) -> Result<(SchemaRef, RecordBatch)> {
    let mut rows: Vec<Row> = Vec::new();
    while let Some(row) = rs.next().await {
        rows.push(row.map_err(from_spanner)?);
    }
    // Metadata (including the row type) is delivered with the first partial result set and retained
    // by the ResultSet, so it is available here even for empty results.
    rows_to_batch(rs.metadata(), &rows, precision)
}

/// Materialise already-drained Spanner rows (plus their result-set metadata) as a single Arrow
/// [`RecordBatch`] together with its schema. Used where the rows had to be drained inside a
/// read/write transaction runner and are converted afterwards — e.g. DML with `THEN RETURN`.
pub(crate) fn rows_to_batch(
    metadata: Option<&ResultSetMetadata>,
    rows: &[Row],
    precision: TimestampPrecision,
) -> Result<(SchemaRef, RecordBatch)> {
    let schema = build_schema(metadata, rows.first(), precision)?;
    let batch = build_batch(schema.clone(), rows)?;
    Ok((schema, batch))
}

/// Additional per-chunk byte budget for [`pull_chunk`], on top of the `max` (row-count) cap.
///
/// The row cap alone bounds rows, not bytes: 8192 rows of `STRING(MAX)`/`BYTES(MAX)` would be tens
/// of GB per chunk, and a chunk is held roughly twice during conversion. So `pull_chunk` also cuts a
/// chunk once its accumulated (approximate) wire size crosses this budget. A single row larger than
/// the whole budget still forms its own one-row chunk (the check runs *after* the row is buffered),
/// so streaming never stalls or emits an empty chunk.
const CHUNK_BYTE_BUDGET: usize = 32 * 1024 * 1024;

/// Pull up to `max` rows from a Spanner result set, stopping early when the stream ends — or, as an
/// additional cap, once the accumulated rows exceed [`CHUNK_BYTE_BUDGET`] approximate bytes.
///
/// `rows` is deliberately not pre-reserved to `max`: `max` is a *cap*, not a prediction, and
/// reserving it would allocate for the cap regardless — unbounded for a large
/// `spanner.rows_per_batch`, which takes no upper bound.
async fn pull_chunk(rs: &mut ResultSet, max: usize) -> Result<Vec<Row>> {
    let mut rows = Vec::new();
    let mut bytes: usize = 0;
    while rows.len() < max {
        match rs.next().await {
            Some(row) => {
                let row = row.map_err(from_spanner)?;
                // Approximate (see `approx_row_bytes`); base64 BYTES over-estimate the decoded
                // size, which only makes the budget slightly more conservative.
                bytes = bytes.saturating_add(approx_row_bytes(&row));
                rows.push(row);
                // The row is already buffered, so an oversized single row still yields a one-row
                // chunk rather than looping forever or producing an empty chunk.
                if bytes >= CHUNK_BYTE_BUDGET {
                    break;
                }
            }
            None => break,
        }
    }
    Ok(rows)
}

/// Roughly estimate a row's byte size, used only to drive the [`CHUNK_BYTE_BUDGET`] early-cut —
/// never for correctness.
fn approx_row_bytes(row: &Row) -> usize {
    row.raw_values().iter().map(approx_value_bytes).sum()
}

/// Approximate the byte size of a single Spanner [`Value`] (see [`approx_row_bytes`]). Strings —
/// how Spanner ships most types over the wire — count their UTF-8 length; nested lists/structs
/// recurse; other scalars count as a small fixed size.
fn approx_value_bytes(value: &Value) -> usize {
    match value.kind() {
        Kind::Null => 0,
        Kind::Bool => 1,
        Kind::Number => 8,
        Kind::String => value.as_str().expect("string kind").len(),
        Kind::List => value
            .as_list()
            .expect("list kind")
            .iter()
            .map(approx_value_bytes)
            .sum(),
        Kind::Struct => value
            .as_struct()
            .expect("struct kind")
            .fields()
            .map(|(_, v)| approx_value_bytes(v))
            .sum(),
    }
}

/// Wrap a Spanner [`ResultSet`] as a streaming Arrow [`RecordBatchReader`].
///
/// The first chunk of rows is pulled here (Spanner delivers the column metadata with the first
/// partial result set, so this also settles the schema) and the rest is handed to a background
/// prefetch task; see [`SpannerBatchReader`] for the streaming model.
///
/// The first chunk is bounded by the caller's *query* deadline (the caller wraps this whole future
/// in `spanner.rpc.timeout_seconds.query`); each background fetch after it is bounded by
/// `fetch_timeout` (`spanner.rpc.timeout_seconds.fetch`) inside the prefetch task, so a stalled
/// stream surfaces [`Status::Timeout`](adbc_core::error::Status::Timeout) on the consumer's next
/// batch.
pub(crate) async fn stream_query(
    runtime: SharedRuntime,
    cancel: CancelSignal,
    mut rs: ResultSet,
    batch_size: usize,
    precision: TimestampPrecision,
    fetch_timeout: Option<Duration>,
) -> Result<SpannerBatchReader> {
    let first = pull_chunk(&mut rs, batch_size).await?;
    let schema = build_schema(rs.metadata(), first.first(), precision)?;
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

/// The prefetch task's view of a Spanner [`ResultSet`]: chunks of up to `batch_size` rows (plus
/// the [`CHUNK_BYTE_BUDGET`]), as pulled by [`pull_chunk`] — each pull bounded by the statement's
/// fetch timeout (`spanner.rpc.timeout_seconds.fetch`), when set.
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

/// A streaming [`RecordBatchReader`] over a Spanner [`ResultSet`].
///
/// Rows are fetched from the server and converted to Arrow in bounded chunks of
/// `spanner.rows_per_batch` rows (plus the [`CHUNK_BYTE_BUDGET`]), so a large result set is never
/// fully held in memory. A background task on the shared runtime **prefetches ahead of the
/// consumer**, keeping at most one undelivered chunk buffered plus one being fetched. The ADBC
/// traits are synchronous, so each `next` bridges to the async channel with a cancellable
/// `block_on`; a latched [`CancelSignal`] both fails the wait and stops the background fetch, and
/// dropping the reader aborts the task. The signal is the producing operation's **own**, so once
/// cancelled the reader stays cancelled — a later operation on the owning statement/connection can
/// neither clear it nor be affected by it.
pub(crate) struct SpannerBatchReader {
    runtime: SharedRuntime,
    cancel: CancelSignal,
    schema: SchemaRef,
    /// The first chunk of rows, fetched up front to settle the schema; emitted on the first `next`.
    first: Option<Vec<Row>>,
    /// Prefetched row chunks from the background task; `None` once the stream is exhausted, a
    /// chunk fetch errored, or the reader was cancelled.
    chunks: Option<crate::runtime::ChunkReceiver<Row>>,
    /// The background prefetch task, so `Drop` can abort a fetch still in flight.
    task: tokio::task::JoinHandle<()>,
}

impl SpannerBatchReader {
    /// Wrap an already-settled schema and first chunk plus the still-undrained `source`, whose
    /// remaining chunks a freshly spawned prefetch task pulls ahead of the consumer.
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
        // Dropping the `chunks` receiver alone would stop the task only at its *next* send; abort
        // also cuts a fetch still in flight, so a reader dropped mid-stream never leaves a task
        // pulling rows in the background. Aborting an already-finished task is a no-op.
        self.task.abort();
    }
}

/// Surface a driver error to a `RecordBatchReader` consumer, whose only error channel is
/// [`ArrowError`] (this preserves the message and, via the source chain, the ADBC status).
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

/// A lazy source of the per-bound-row query statements a [`BoundQueryChunks`] source executes, one
/// at a time, so a large `executemany` SELECT holds a single statement in memory rather than one
/// per bound row (implemented in `src/statement.rs`). `next_statement` yields `None` once every
/// bound row is drained; a per-row bind failure surfaces as `Some(Err(..))` at that point.
pub(crate) trait BoundStatementSource: Send {
    fn next_statement(&mut self) -> Option<Result<SpannerSql>>;
}

/// Wrap a lazy source of per-bound-row query statements as one streaming Arrow
/// [`RecordBatchReader`], executing every statement inside the same **multi-use read-only
/// transaction** so all bound rows see a single, mutually consistent snapshot.
///
/// The first statement is built and executed here and its first chunk pulled (settling the schema —
/// every statement is the same SQL, so the schema is shared); the rest streams through the same
/// machinery as [`stream_query`], with a [`BoundQueryChunks`] [`ChunkSource`] that also executes
/// the next bound row's statement when the current result set drains. The `transaction`
/// (`Arc`-shared: in a manual transaction the connection's shared snapshot, in autocommit a
/// dedicated one) is owned by the source, hence by the prefetch task, keeping the snapshot alive
/// for as long as chunks are pulled; Spanner read-only transactions need no commit/rollback, so
/// dropping it is cleanup enough.
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
        // No statements at all: an empty reader with an empty schema.
        None => None,
    };
    let (first, schema) = match result_set.as_mut() {
        Some(rs) => {
            let rows = pull_chunk(rs, batch_size).await?;
            let schema = build_schema(rs.metadata(), rows.first(), precision)?;
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

/// The prefetch task's view of a bound (parameterized) query: the successive per-bound-row result
/// sets, drained in bounded chunks, all inside one shared multi-use read-only transaction, whose
/// `Arc` it owns so the snapshot outlives lazy iteration. Each `next_chunk` is bounded by the fetch
/// timeout, covering both the chunk pull and the execution of the next bound row's statement.
struct BoundQueryChunks {
    /// The shared snapshot every statement executes in.
    transaction: Arc<MultiUseReadOnlyTransaction>,
    /// The lazy source of not-yet-built, not-yet-executed per-bound-row statements.
    statements: Box<dyn BoundStatementSource>,
    /// The live result set of the statement currently being drained, if any.
    result_set: Option<ResultSet>,
    batch_size: usize,
    fetch_timeout: Option<Duration>,
}

impl ChunkSource for BoundQueryChunks {
    type Row = Row;

    fn next_chunk(&mut self) -> impl std::future::Future<Output = Result<Vec<Row>>> + Send {
        let Self {
            transaction,
            statements,
            result_set,
            batch_size,
            fetch_timeout,
        } = self;
        with_timeout(
            *fetch_timeout,
            crate::OPTION_RPC_TIMEOUT_FETCH,
            async move {
                // A drained source (`None`) becomes an empty chunk, the `ChunkSource` end-of-stream
                // signal that closes the prefetch channel.
                Ok(
                    next_bound_chunk(transaction, statements.as_mut(), result_set, *batch_size)
                        .await?
                        .unwrap_or_default(),
                )
            },
        )
    }
}

/// Pull the next non-empty chunk for [`BoundQueryChunks`]: drain the current result set in bounded
/// chunks, and when it ends, build and execute the next statement in the same `transaction` —
/// looping so a bound row with an empty result never surfaces as a spurious empty batch. `None`
/// means everything is drained.
async fn next_bound_chunk(
    transaction: &MultiUseReadOnlyTransaction,
    statements: &mut dyn BoundStatementSource,
    result_set: &mut Option<ResultSet>,
    batch_size: usize,
) -> Result<Option<Vec<Row>>> {
    loop {
        if let Some(rs) = result_set.as_mut() {
            let rows = pull_chunk(rs, batch_size).await?;
            if !rows.is_empty() {
                return Ok(Some(rows));
            }
            *result_set = None;
        }
        match statements.next_statement() {
            Some(statement) => {
                *result_set = Some(
                    transaction
                        .execute_query(statement?)
                        .await
                        .map_err(from_spanner)?,
                );
            }
            None => return Ok(None),
        }
    }
}

/// Build the Arrow schema for a result set from Spanner's column metadata, falling back to
/// all-`Utf8` columns inferred from the first row's width when metadata is unavailable. Returns
/// `Result` (naming the offending column) so an unmappable type can be rejected cleanly; today
/// every Spanner type maps to some Arrow type.
pub(crate) fn build_schema(
    metadata: Option<&ResultSetMetadata>,
    first_row: Option<&Row>,
    precision: TimestampPrecision,
) -> Result<SchemaRef> {
    if let Some(md) = metadata {
        let names = md.column_names();
        if !names.is_empty() {
            let types = md.column_types();
            let fields: Vec<Field> = names
                .iter()
                .enumerate()
                .map(|(i, name)| match types.get(i) {
                    // Prefix the column name (as `build_column` does on the value path) so any
                    // future type-mapping rejection names the failing column in a wide result set.
                    Some(ty) => arrow_field(name, ty, true, precision).map_err(|mut e| {
                        e.message = format!("column {name:?}: {}", e.message);
                        e
                    }),
                    None => Ok(Field::new(name, DataType::Utf8, true)),
                })
                .collect::<Result<_>>()?;
            return Ok(Arc::new(Schema::new(fields)));
        }
    }

    let width = first_row.map_or(0, |r| r.raw_values().len());
    let fields: Vec<Field> = (0..width)
        .map(|i| Field::new(format!("col{i}"), DataType::Utf8, true))
        .collect();
    Ok(Arc::new(Schema::new(fields)))
}

/// Build an Arrow [`Field`] for a Spanner column [`Type`], attaching the canonical `arrow.json`
/// extension metadata when the column is `JSON`.
///
/// The storage type stays `Utf8` (the value bytes are the JSON text); only the field metadata marks
/// it as logical JSON. Only `TypeCode::Json` is tagged — the other Utf8-backed codes stay untagged.
///
/// Never fails today (every Spanner type maps to some Arrow type), but stays fallible so a future
/// unmappable type can be rejected here without a signature change rippling through the
/// schema-build path.
pub(crate) fn arrow_field(
    name: impl Into<String>,
    ty: &Type,
    nullable: bool,
    precision: TimestampPrecision,
) -> Result<Field> {
    arrow_field_at(name, ty, nullable, precision, 0)
}

/// [`arrow_field`] carrying the current nesting `depth` through the mutually-recursive type walk
/// (see [`MAX_TYPE_NESTING_DEPTH`]). Public callers enter at depth `0`.
fn arrow_field_at(
    name: impl Into<String>,
    ty: &Type,
    nullable: bool,
    precision: TimestampPrecision,
    depth: usize,
) -> Result<Field> {
    let field = Field::new(name, arrow_type(ty, precision, depth)?, nullable);
    Ok(if ty.code() == TypeCode::Json {
        field.with_metadata(json_extension_metadata())
    } else {
        field
    })
}

/// Whether an Arrow [`Field`] carries the canonical `arrow.json` extension on a string storage
/// type. The bind path uses this to send such values as Spanner `JSON`-typed parameters, and ingest
/// create modes to create `JSON` columns. The storage check matters: `arrow.json` is only defined
/// over utf8-family storage, so a tag on any other type is ignored rather than mis-bound. A
/// `Dictionary` looks through to its **value** type — the Arrow spec allows an extension array to be
/// dictionary-encoded, and the bind path decodes that transparently.
pub(crate) fn is_json_field(field: &Field) -> bool {
    let storage = match field.data_type() {
        DataType::Dictionary(_, value) => value.as_ref(),
        other => other,
    };
    matches!(
        storage,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    ) && field
        .metadata()
        .get(ARROW_EXTENSION_NAME)
        .map(String::as_str)
        == Some(ARROW_JSON_EXTENSION)
}

/// The two `ARROW:extension:*` field-metadata keys that mark a Utf8 column as canonical `arrow.json`.
/// The metadata value is the empty string, which is valid (and conventional) for `arrow.json`.
fn json_extension_metadata() -> HashMap<String, String> {
    HashMap::from([
        (
            ARROW_EXTENSION_NAME.to_string(),
            ARROW_JSON_EXTENSION.to_string(),
        ),
        (ARROW_EXTENSION_METADATA.to_string(), String::new()),
    ])
}

/// Map a Spanner column [`Type`] to an Arrow [`DataType`]. `precision` selects the `TIMESTAMP`
/// unit (see [`TimestampPrecision`]) and is threaded recursively, so a `TIMESTAMP` nested in an
/// `ARRAY`/`STRUCT` maps the same as a top-level column.
fn arrow_type(ty: &Type, precision: TimestampPrecision, depth: usize) -> Result<DataType> {
    // Defense-in-depth: cap the recursion so a hostile endpoint's pathological nested STRUCT/ARRAY
    // metadata fails loudly rather than exhausting the stack (the transport also bounds this via
    // prost's decode recursion limit).
    if depth > MAX_TYPE_NESTING_DEPTH {
        return Err(err(
            format!(
                "nested STRUCT/ARRAY type exceeds the maximum depth of {MAX_TYPE_NESTING_DEPTH}"
            ),
            Status::Internal,
        ));
    }
    Ok(match ty.code() {
        TypeCode::Bool => DataType::Boolean,
        TypeCode::Int64 => DataType::Int64,
        TypeCode::Float64 => DataType::Float64,
        TypeCode::Float32 => DataType::Float32,
        TypeCode::Bytes => DataType::Binary,
        TypeCode::Date => DataType::Date32,
        TypeCode::Timestamp => {
            DataType::Timestamp(precision.time_unit(), Some(TIMESTAMP_TZ.into()))
        }
        TypeCode::Numeric => DataType::Decimal128(NUMERIC_PRECISION, NUMERIC_SCALE),
        TypeCode::Struct => struct_arrow_type(ty, precision, depth)?,
        TypeCode::Array => match ty.array_element_type() {
            // ARRAY<T> → Arrow List<T> (recursively; T may itself be a STRUCT). The element field
            // is built via `arrow_field`, so an `ARRAY<JSON>` carries the `arrow.json` extension on
            // the list's child (`item`) field, not the top-level List. Spanner does not allow
            // arrays of arrays; fall back to JSON text for anything else.
            Some(element) if !matches!(element.code(), TypeCode::Array | TypeCode::Unspecified) => {
                DataType::List(Arc::new(arrow_field_at(
                    LIST_ITEM,
                    &element,
                    true,
                    precision,
                    depth + 1,
                )?))
            }
            _ => DataType::Utf8,
        },
        // ENUM's wire value is a bare ordinal (a decimal string, like INT64); PROTO's is the raw
        // serialized message base64-encoded (like BYTES). Both mappings are lossless; their
        // *structure* travels only in the database's proto descriptor bundle, not the query
        // metadata — see the module doc.
        TypeCode::Enum => DataType::Int64,
        TypeCode::Proto => DataType::Binary,
        // Unlike JSON (whose `arrow.json` tag also drives the bind path), ENUM/PROTO/INTERVAL/UUID
        // carry no such marker, so a value read back binds as its storage type and Spanner won't
        // coerce it into the column. Wrap the parameter in `CAST(@p AS …)` to round-trip one through
        // DML (bulk-ingest mutations are unaffected).
        //
        // STRING, JSON, UUID, INTERVAL and any future/unknown code are UTF-8 text.
        _ => DataType::Utf8,
    })
}

/// Map a Spanner `STRUCT` type to an Arrow `Struct`, using the field names and types from the
/// result metadata. Falls back to `Utf8` if the struct type is somehow unavailable.
fn struct_arrow_type(ty: &Type, precision: TimestampPrecision, depth: usize) -> Result<DataType> {
    match ty.struct_type() {
        Some(st) => Ok(DataType::Struct(struct_fields(st, precision, depth)?)),
        None => Ok(DataType::Utf8),
    }
}

/// Build the Arrow child fields for a Spanner struct type (names verbatim, including empties/dups).
fn struct_fields(
    st: &google_cloud_spanner::model::StructType,
    precision: TimestampPrecision,
    depth: usize,
) -> Result<Fields> {
    st.fields
        .iter()
        .map(|f| {
            let field_type = f
                .r#type
                .as_deref()
                .cloned()
                .map(Type::from)
                .unwrap_or_default();
            arrow_field_at(&f.name, &field_type, true, precision, depth + 1)
        })
        .collect()
}

fn build_batch(schema: SchemaRef, rows: &[Row]) -> Result<RecordBatch> {
    // Build each top-level column by iterating rows directly, reusing one scratch buffer of
    // `rows.len()` value pointers rather than materializing the whole O(rows × cols) column matrix
    // up front. Nested List/Struct columns still gather their own child slices when `build_array`
    // recurses.
    let mut scratch: Vec<Option<&Value>> = Vec::with_capacity(rows.len());
    let arrays: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| {
            scratch.clear();
            scratch.extend(rows.iter().map(|row| row.raw_values().get(i)));
            build_column(field, &scratch)
        })
        .collect::<Result<_>>()?;

    RecordBatch::try_new(schema, arrays).map_err(|e| {
        err(
            format!("failed to build record batch: {e}"),
            Status::Internal,
        )
    })
}

/// Build one result column via [`build_array`], prefixing any error with the column's name.
/// `build_array` only sees the type and the offending value; in a wide result the failing
/// *column* is what the caller needs to identify (e.g. an out-of-range `TIMESTAMP`).
fn build_column(field: &FieldRef, values: &[Option<&Value>]) -> Result<ArrayRef> {
    build_array(field.data_type(), values).map_err(|mut e| {
        e.message = format!("column {:?}: {}", field.name(), e.message);
        e
    })
}

/// Return the value unless it is a SQL `NULL` (or absent).
fn present(value: Option<&Value>) -> Option<&Value> {
    value.filter(|v| v.kind() != Kind::Null)
}

fn arrow_err(e: ArrowError) -> adbc_core::error::Error {
    err(
        format!("failed to build Arrow array: {e}"),
        Status::Internal,
    )
}

/// The error for a present (non-NULL) wire value that cannot be decoded as its column's Spanner
/// type. Decoding must fail loudly: mapping an undecodable value to NULL would silently corrupt
/// data (a value the caller cannot distinguish from a genuine SQL NULL).
fn decode_error(spanner_type: &str, value: &Value) -> adbc_core::error::Error {
    err(
        format!(
            "cannot decode Spanner {spanner_type} wire value {}",
            value_to_json(value)
        ),
        Status::InvalidData,
    )
}

/// An Arrow builder that can append a null slot — the one operation [`build_with`] needs besides
/// the caller's per-value append. (Arrow's own [`ArrayBuilder`](arrow_array::builder::ArrayBuilder)
/// trait has no `append_null`.)
trait NullableBuilder {
    fn append_null(&mut self);
}

impl NullableBuilder for BooleanBuilder {
    fn append_null(&mut self) {
        BooleanBuilder::append_null(self);
    }
}

impl<T: ArrowPrimitiveType> NullableBuilder for PrimitiveBuilder<T> {
    fn append_null(&mut self) {
        PrimitiveBuilder::append_null(self);
    }
}

impl<T: ByteArrayType> NullableBuilder for GenericByteBuilder<T> {
    fn append_null(&mut self) {
        GenericByteBuilder::append_null(self);
    }
}

/// Fill `builder` from one Spanner value per row: SQL NULLs become null slots and every present
/// value goes to `append`, whose error (see [`decode_error`]) is how an undecodable value fails
/// loudly instead of turning into a silent null.
#[inline]
fn build_with<B: NullableBuilder>(
    values: &[Option<&Value>],
    mut builder: B,
    mut append: impl FnMut(&mut B, &Value) -> Result<()>,
) -> Result<B> {
    for &value in values {
        match present(value) {
            None => builder.append_null(),
            Some(v) => append(&mut builder, v)?,
        }
    }
    Ok(builder)
}

/// Build a primitive Arrow array from one Spanner value per row, with [`build_with`]'s null and
/// strict-decode semantics. Open-codes that loop rather than calling it: routing the (cheap)
/// per-value primitive appends through a closure measured ~8% slower on the `dates_date32` and
/// `nested_array_int64_struct` chunks of `benches/conversion.rs`.
fn build_primitive<T: ArrowPrimitiveType>(
    values: &[Option<&Value>],
    spanner_type: &str,
    parse: impl Fn(&Value) -> Option<T::Native>,
) -> Result<PrimitiveArray<T>> {
    let mut builder = PrimitiveBuilder::<T>::with_capacity(values.len());
    for &value in values {
        match present(value) {
            None => builder.append_null(),
            Some(v) => builder.append_value(parse(v).ok_or_else(|| decode_error(spanner_type, v))?),
        }
    }
    Ok(builder.finish())
}

/// Build an Arrow array of the given `data_type` from one Spanner value per row.
///
/// SQL NULLs map to null slots. A present value that cannot be decoded as the column's type is an
/// **error**, not a null — every arm goes through [`build_primitive`]/[`build_with`], so a
/// wire-format surprise cannot silently masquerade as a SQL NULL.
///
/// `pub(crate)` (rather than private) only so `crate::bench_support` can expose it to `benches/`.
pub(crate) fn build_array(data_type: &DataType, values: &[Option<&Value>]) -> Result<ArrayRef> {
    Ok(match data_type {
        DataType::Boolean => Arc::new(
            build_with(
                values,
                BooleanBuilder::with_capacity(values.len()),
                |builder, v| {
                    builder.append_value(v.as_bool().ok_or_else(|| decode_error("BOOL", v))?);
                    Ok(())
                },
            )?
            .finish(),
        ),
        DataType::Int64 => Arc::new(build_primitive::<Int64Type>(values, "INT64", parse_int64)?),
        DataType::Float64 => Arc::new(build_primitive::<Float64Type>(
            values, "FLOAT64", parse_f64,
        )?),
        DataType::Float32 => Arc::new(build_primitive::<Float32Type>(values, "FLOAT32", |v| {
            parse_f64(v).map(|f| f as f32)
        })?),
        DataType::Date32 => Arc::new(build_primitive::<Date32Type>(values, "DATE", |v| {
            v.as_str().and_then(parse_date_days)
        })?),
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
            // A present value errors if it is not a timestamp string at all, or — since Arrow
            // stores nanoseconds as an `i64` — if it is a valid instant outside the representable
            // range.
            let array = build_with(
                values,
                TimestampNanosecondArray::builder(values.len()),
                |builder, v| {
                    let s = v.as_str().ok_or_else(|| decode_error("TIMESTAMP", v))?;
                    builder.append_value(parse_timestamp_nanos(s).ok_or_else(|| {
                        if chrono::DateTime::parse_from_rfc3339(s).is_ok() {
                            invalid_argument(format!(
                                "TIMESTAMP value {s:?} is outside the range representable as an \
                                 Arrow Timestamp(Nanosecond) (~1677-09-21 to 2262-04-11); set \
                                 {}={} to read it at microsecond precision instead",
                                crate::OPTION_MAX_TIMESTAMP_PRECISION,
                                TimestampPrecision::MICROSECONDS,
                            ))
                        } else {
                            decode_error("TIMESTAMP", v)
                        }
                    })?);
                    Ok(())
                },
            )?
            .finish();
            Arc::new(array.with_timezone_opt(tz.clone()))
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            // The `microseconds` mode of `spanner.max_timestamp_precision`. Every instant Spanner
            // can store fits an i64 of epoch microseconds, so this arm has no out-of-range case;
            // sub-microsecond digits truncate toward negative infinity. Undecodable strings error.
            let array = build_primitive::<TimestampMicrosecondType>(values, "TIMESTAMP", |v| {
                v.as_str().and_then(parse_timestamp_micros)
            })?;
            Arc::new(array.with_timezone_opt(tz.clone()))
        }
        DataType::Decimal128(precision, scale) => {
            let array = build_primitive::<Decimal128Type>(values, "NUMERIC", |v| {
                v.as_str().and_then(parse_numeric_i128)
            })?;
            Arc::new(
                array
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(arrow_err)?,
            )
        }
        // Spanner encodes BYTES as base64. One scratch buffer is reused across cells —
        // `decode_vec` *appends* into it, so it is cleared per cell — avoiding a heap
        // alloc/free per cell before `append_value` copies the bytes into the builder.
        DataType::Binary => {
            let mut scratch = Vec::new();
            Arc::new(
                build_with(values, BinaryBuilder::new(), |builder, v| {
                    let s = v.as_str().ok_or_else(|| decode_error("BYTES", v))?;
                    scratch.clear();
                    base64::engine::general_purpose::STANDARD
                        .decode_vec(s, &mut scratch)
                        .map_err(|_| decode_error("BYTES", v))?;
                    builder.append_value(&scratch);
                    Ok(())
                })?
                .finish(),
            )
        }
        DataType::List(field) => build_list(field, values)?,
        DataType::Struct(fields) => build_struct(fields, values)?,
        // Utf8 and every fallback (JSON, …): keep strings verbatim, render anything else (numbers,
        // bools, nested values) as JSON text.
        _ => Arc::new(
            build_with(values, StringBuilder::new(), |builder, v| {
                // Append the string slice directly (no per-value owned String); only the
                // JSON-render fallback allocates, and only for non-string values.
                match v.as_str() {
                    Some(s) => builder.append_value(s),
                    None => builder.append_value(value_to_json(v).to_string()),
                }
                Ok(())
            })?
            .finish(),
        ),
    })
}

/// Build an Arrow `List` array: each Spanner value is a list (or null) of the element type.
///
/// [`build_array`]'s strict-decode policy applies here too: a present value that is not a wire list
/// is an error, never a silent null. Elements recurse, so an undecodable element at any nesting
/// depth errors as well.
fn build_list(field: &FieldRef, values: &[Option<&Value>]) -> Result<ArrayRef> {
    let mut children: Vec<Option<&Value>> = Vec::new();
    let mut offsets: Vec<i32> = Vec::with_capacity(values.len() + 1);
    offsets.push(0);
    let mut validity: Vec<bool> = Vec::with_capacity(values.len());
    for &value in values {
        match present(value) {
            None => validity.push(false),
            Some(v) if v.kind() == Kind::List => {
                children.extend(v.as_list().expect("list kind").iter().map(Some));
                validity.push(true);
            }
            Some(v) => return Err(decode_error("ARRAY", v)),
        }
        // Checked, mirroring `nested::list_of_nullable`: `children.len()` is the running element
        // total, so a failed conversion is the offset accumulation overflowing i32 — error out
        // rather than wrap silently.
        let offset = i32::try_from(children.len()).map_err(|_| {
            err(
                "ARRAY result exceeds the i32 offset range",
                Status::Internal,
            )
        })?;
        offsets.push(offset);
    }
    let child = build_array(field.data_type(), &children)?;
    let list = ListArray::try_new(
        field.clone(),
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        child,
        Some(NullBuffer::from(validity)),
    )
    .map_err(arrow_err)?;
    Ok(Arc::new(list))
}

/// Build an Arrow `Struct` array. Spanner encodes struct values **positionally** — a `ListValue`
/// whose elements match the struct type's field order — and that is the sole encoding accepted
/// here, which is what makes Spanner's duplicate and empty field names decode correctly: a
/// name-keyed lookup would collapse two same-named fields onto one value, and a keyed
/// `google.protobuf.Struct` wire value could not carry them apart at all, being a map.
///
/// [`build_array`]'s strict-decode policy applies here too: a present value that is not a wire list
/// is an error, never a silent null, at any nesting depth.
fn build_struct(fields: &Fields, values: &[Option<&Value>]) -> Result<ArrayRef> {
    let mut rows = Vec::with_capacity(values.len());
    let mut validity: Vec<bool> = Vec::with_capacity(values.len());
    for &value in values {
        match present(value) {
            None => {
                rows.push(None);
                validity.push(false);
            }
            Some(v) if v.kind() == Kind::List => {
                let list = v.as_list().expect("list kind");
                if list.len() != fields.len() {
                    return Err(err(
                        format!(
                            "cannot decode Spanner STRUCT: expected {} fields, got {}",
                            fields.len(),
                            list.len()
                        ),
                        Status::InvalidData,
                    ));
                }
                rows.push(Some(list));
                validity.push(true);
            }
            Some(v) => return Err(decode_error("STRUCT", v)),
        }
    }
    // Reuse one column's pointers rather than allocating a rows × fields matrix.
    let mut scratch = Vec::with_capacity(values.len());
    let arrays = fields
        .iter()
        .enumerate()
        .map(|(i, field)| {
            scratch.clear();
            scratch.extend(rows.iter().map(|row| row.and_then(|list| list.get(i))));
            build_array(field.data_type(), &scratch)
        })
        .collect::<Result<Vec<_>>>()?;
    let array = StructArray::try_new_with_length(
        fields.clone(),
        arrays,
        Some(NullBuffer::from(validity)),
        values.len(),
    )
    .map_err(arrow_err)?;
    Ok(Arc::new(array))
}

/// Parse a Spanner `INT64` value. Integers always arrive as strings (Spanner encodes `INT64` as a
/// JSON string precisely so magnitudes above 2^53 survive), so only the string form is accepted:
/// falling back to a JSON number would silently round values above 2^53, since an `f64` cannot
/// represent every `i64`. A non-string wire value is a loud decode error instead.
fn parse_int64(value: &Value) -> Option<i64> {
    value.as_str()?.parse::<i64>().ok()
}

/// Parse a Spanner floating-point value. Finite values arrive as numbers; `NaN` and the infinities
/// arrive as strings.
fn parse_f64(value: &Value) -> Option<f64> {
    if let Some(f) = value.as_f64() {
        return Some(f);
    }
    match value.as_str()? {
        "NaN" => Some(f64::NAN),
        "Infinity" => Some(f64::INFINITY),
        "-Infinity" => Some(f64::NEG_INFINITY),
        s => s.parse::<f64>().ok(),
    }
}

/// Parse a Spanner `DATE` (`YYYY-MM-DD`) into days since the Unix epoch (Arrow `Date32`).
///
/// Spanner always sends a `DATE` as the fixed-width canonical `YYYY-MM-DD` (0001–9999), so this
/// reads the three integer fields at fixed byte offsets rather than running chrono's general
/// strftime interpreter, which re-tokenizes the format string on *every* call — over half the
/// temporal conversion path in profiling. Only that canonical shape is accepted: anything else
/// returns `None`, and a calendar-invalid date is rejected by `from_ymd_opt`.
pub(crate) fn parse_date_days(s: &str) -> Option<i32> {
    let b = s.as_bytes();
    if b.len() != 10 || b[4] != b'-' || b[7] != b'-' {
        return None;
    }
    let digit = |i: usize| -> Option<u32> {
        let d = b[i].wrapping_sub(b'0');
        (d < 10).then_some(u32::from(d))
    };
    let year = digit(0)? * 1000 + digit(1)? * 100 + digit(2)? * 10 + digit(3)?;
    let month = digit(5)? * 10 + digit(6)?;
    let day = digit(8)? * 10 + digit(9)?;
    let date = chrono::NaiveDate::from_ymd_opt(year as i32, month, day)?;
    // Days since the Unix epoch, in closed form: `num_days_from_ce` counts days from 0001-01-01
    // (day 1), and 1970-01-01 is day 719_163, so subtracting that constant yields epoch-relative
    // days directly — no second `NaiveDate` for the epoch and no `Duration` subtraction. The
    // result stays in `i32` for every valid Spanner DATE (0001-01-01 → -719_162, 9999-12-31 →
    // 2_932_896), so no `try_from` guard is needed.
    Some(chrono::Datelike::num_days_from_ce(&date) - 719_163)
}

/// Read two ASCII digits at `off` as a number, or `None` if either byte is not `0`–`9`.
fn two_digits(b: &[u8], off: usize) -> Option<u32> {
    let hi = b[off].wrapping_sub(b'0');
    let lo = b[off + 1].wrapping_sub(b'0');
    (hi < 10 && lo < 10).then(|| u32::from(hi) * 10 + u32::from(lo))
}

/// Scan a Spanner `TIMESTAMP` in its canonical UTC RFC 3339 form —
/// `YYYY-MM-DDTHH:MM:SS[.fraction]Z`, 1–9 fractional digits — into a [`chrono::NaiveDateTime`],
/// reading each field at a fixed byte offset instead of running chrono's general RFC 3339 parser.
/// Only the calendar arithmetic is delegated to chrono (`from_ymd_opt`/`and_hms_nano_opt`), which
/// validates the fields. Spanner always emits this exact shape, so any deviation returns `None`.
fn scan_timestamp_utc(s: &str) -> Option<chrono::NaiveDateTime> {
    let b = s.as_bytes();
    // Shortest legal form is `YYYY-MM-DDTHH:MM:SSZ` (20 bytes); the separators are fixed.
    if b.len() < 20
        || b[4] != b'-'
        || b[7] != b'-'
        || b[10] != b'T'
        || b[13] != b':'
        || b[16] != b':'
    {
        return None;
    }
    let year = two_digits(b, 0)? * 100 + two_digits(b, 2)?;
    let month = two_digits(b, 5)?;
    let day = two_digits(b, 8)?;
    let hour = two_digits(b, 11)?;
    let min = two_digits(b, 14)?;
    let sec = two_digits(b, 17)?;
    // After the seconds: either `Z` (no fraction) or `.<1..=9 digits>Z`.
    let nanos = match b[19] {
        b'Z' if b.len() == 20 => 0,
        // Need room for at least the trailing `Z` after the `.`; a bare `SS.` (len 20) has none, so
        // `b.len() > 20` keeps the `b[20..b.len() - 1]` slice below in-bounds (it would otherwise be
        // the backwards range `b[20..19]`). A too-short/empty fraction still returns `None` below.
        b'.' if b.len() > 20 => {
            // Fractional digits run from offset 20 up to the trailing `Z`.
            let frac = &b[20..b.len() - 1];
            if b[b.len() - 1] != b'Z' || frac.is_empty() || frac.len() > 9 {
                return None;
            }
            let mut nanos: u32 = 0;
            for &d in frac {
                let d = d.wrapping_sub(b'0');
                if d >= 10 {
                    return None;
                }
                nanos = nanos * 10 + u32::from(d);
            }
            // Scale the parsed fraction up to nanosecond resolution (e.g. `.789` → 789_000_000).
            nanos * 10u32.pow(9 - frac.len() as u32)
        }
        _ => return None,
    };
    chrono::NaiveDate::from_ymd_opt(year as i32, month, day)?
        .and_hms_nano_opt(hour, min, sec, nanos)
}

/// Parse a Spanner `TIMESTAMP` (RFC 3339, e.g. `2024-01-15T12:34:56.789012345Z`) into nanoseconds
/// since the Unix epoch (Arrow `Timestamp(Nanosecond)`), preserving full sub-microsecond precision.
///
/// Returns `None` for a malformed string, and — because Arrow stores nanoseconds as an `i64` — for
/// any otherwise-valid instant outside the representable range (~1677-09-21 to 2262-04-11), via
/// chrono's non-panicking [`DateTime::timestamp_nanos_opt`].
pub(crate) fn parse_timestamp_nanos(s: &str) -> Option<i64> {
    scan_timestamp_utc(s)?.and_utc().timestamp_nanos_opt()
}

/// Parse a Spanner `TIMESTAMP` (RFC 3339) into microseconds since the Unix epoch (Arrow
/// `Timestamp(Microsecond)`), **truncating** any sub-microsecond digits **toward negative
/// infinity**: chrono splits an instant into floor seconds plus a non-negative sub-second part,
/// so the integer division of the sub-second nanoseconds is a floor on the timeline (e.g.
/// `1969-12-31T23:59:59.9999999Z` → `-1` µs, not `0`).
///
/// Returns `None` for a malformed string. Every instant Spanner can store is representable — i64
/// microseconds span ~±292,000 years — so the checked arithmetic only guards against hypothetical
/// far-out-of-range inputs.
pub(crate) fn parse_timestamp_micros(s: &str) -> Option<i64> {
    let dt = scan_timestamp_utc(s)?.and_utc();
    dt.timestamp()
        .checked_mul(1_000_000)?
        .checked_add(i64::from(dt.timestamp_subsec_micros()))
}

/// Parse a Spanner `NUMERIC` (decimal string) into an unscaled `i128` at scale 9 (Arrow
/// `Decimal128(38, 9)`). Returns `None` on malformed input or i128 overflow.
pub(crate) fn parse_numeric_i128(s: &str) -> Option<i128> {
    // Spanner emits NUMERIC in one canonical shape: a bare decimal `[-]digits[.digits]` with fixed
    // scale 9 — no leading `+`, no surrounding whitespace, at most nine fractional digits, and
    // digits on both sides of any point (never `.5` or `5.`). Scan the bytes directly (rather than
    // `.trim()` + a `char`-pattern `split_once('.')`, which dominated NUMERIC decoding in
    // profiling) and reject anything outside that shape.
    let (negative, bytes) = match s.as_bytes().split_first() {
        Some((b'-', rest)) => (true, rest),
        _ => (false, s.as_bytes()),
    };
    // Integer / fractional split at the first '.' (a byte search, not a `char` pattern scan); a
    // point must be followed by at least one fractional digit, so a trailing `.` is malformed.
    let (int_part, frac_part) = match bytes.iter().position(|&b| b == b'.') {
        Some(i) if i + 1 < bytes.len() => (&bytes[..i], &bytes[i + 1..]),
        Some(_) => return None,
        None => (bytes, &bytes[bytes.len()..]),
    };
    // The integer part always carries digits (this rejects "", "-", and a leading `.`); the
    // fractional part carries at most `NUMERIC_SCALE` (the scale is fixed at 9).
    if int_part.is_empty() || frac_part.len() > NUMERIC_SCALE as usize {
        return None;
    }
    // Accumulate the integer part, validating each byte; `checked_*` yields `None` on i128 overflow.
    let mut int_val: i128 = 0;
    for &b in int_part {
        let d = b.wrapping_sub(b'0');
        if d >= 10 {
            return None;
        }
        int_val = int_val.checked_mul(10)?.checked_add(i128::from(d))?;
    }
    // The fractional digits (at most `NUMERIC_SCALE`, checked above) form the unscaled fraction,
    // scaled up by the number of missing low-order digits (e.g. `.25` -> 250_000_000).
    let mut frac_val: i128 = 0;
    for &b in frac_part {
        let d = b.wrapping_sub(b'0');
        if d >= 10 {
            return None;
        }
        frac_val = frac_val * 10 + i128::from(d);
    }
    frac_val *= 10_i128.pow(NUMERIC_SCALE as u32 - frac_part.len() as u32);
    let unscaled = int_val.checked_mul(1_000_000_000)?.checked_add(frac_val)?;
    Some(if negative { -unscaled } else { unscaled })
}

/// Recursively convert a Spanner [`Value`] into a [`serde_json::Value`], used to render arrays and
/// structs as text.
fn value_to_json(value: &Value) -> serde_json::Value {
    match value.kind() {
        Kind::Null => serde_json::Value::Null,
        Kind::Bool => serde_json::Value::Bool(value.as_bool().expect("bool kind")),
        Kind::Number => serde_json::json!(value.as_f64().expect("number kind")),
        Kind::String => serde_json::Value::String(value.as_str().expect("string kind").to_string()),
        Kind::List => serde_json::Value::Array(
            value
                .as_list()
                .expect("list kind")
                .iter()
                .map(value_to_json)
                .collect(),
        ),
        Kind::Struct => serde_json::Value::Object(
            value
                .as_struct()
                .expect("struct kind")
                .fields()
                .map(|(k, v)| (k.clone(), value_to_json(v)))
                .collect(),
        ),
    }
}

#[cfg(test)]
mod tests;

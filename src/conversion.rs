//! Conversion from Spanner result sets to Arrow record batches.
//!
//! Spanner sends INT64, DATE, TIMESTAMP and NUMERIC values as strings and floats as numbers, with
//! column types carried separately in the result set metadata. The metadata selects an Arrow type
//! per column (see the type mapping in `README.md`), and values are decoded strictly: a malformed
//! non-null value is an error, never a null.
//!
//! Struct fields are positional in both Spanner and Arrow, so duplicate and empty field names
//! carry over. ENUM and PROTO structure lives in the database's proto descriptors rather than the
//! query metadata, so they map to their wire primitives (an `Int64` ordinal and `Binary` bytes).

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
use arrow_buffer::{NullBufferBuilder, OffsetBuffer, ScalarBuffer};
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

/// Conventional name of an Arrow `List` item field.
const LIST_ITEM: &str = "item";

/// Bound the server-provided type walk to avoid stack exhaustion. Matches prost's decode limit.
const MAX_TYPE_NESTING_DEPTH: usize = 100;

/// Spanner `NUMERIC` has a fixed precision of 38 and scale of 9.
const NUMERIC_PRECISION: u8 = 38;
const NUMERIC_SCALE: i8 = 9;
/// Spanner `TIMESTAMP` values are absolute instants in UTC.
const TIMESTAMP_TZ: &str = "UTC";

const ARROW_EXTENSION_NAME: &str = "ARROW:extension:name";
const ARROW_EXTENSION_METADATA: &str = "ARROW:extension:metadata";
const ARROW_JSON_EXTENSION: &str = "arrow.json";

/// How Spanner `TIMESTAMP` columns map to Arrow, set by
/// [`spanner.max_timestamp_precision`](crate::OPTION_MAX_TIMESTAMP_PRECISION).
///
/// Spanner timestamps span years 0001-9999 at nanosecond precision, but Arrow's
/// `Timestamp(Nanosecond)` covers only ~1677-09-21 to 2262-04-11. There is no wrapping mode: a
/// wrapped timestamp is indistinguishable from real data.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum TimestampPrecision {
    /// `Timestamp(Nanosecond, "UTC")`. An instant outside the representable range is an error
    /// naming the column and value.
    #[default]
    NanosecondsErrorOnOverflow,
    /// `Timestamp(Microsecond, "UTC")`, covering Spanner's full range. Sub-microsecond digits are
    /// truncated toward negative infinity.
    Microseconds,
}

impl TimestampPrecision {
    /// Option value selecting [`Self::NanosecondsErrorOnOverflow`].
    pub(crate) const NANOSECONDS_ERROR_ON_OVERFLOW: &'static str = "nanoseconds_error_on_overflow";
    /// Option value selecting [`Self::Microseconds`].
    pub(crate) const MICROSECONDS: &'static str = "microseconds";

    /// Parse an option value; the empty string resets to the default.
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

    /// The option value, for `get_option`.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::NanosecondsErrorOnOverflow => Self::NANOSECONDS_ERROR_ON_OVERFLOW,
            Self::Microseconds => Self::MICROSECONDS,
        }
    }

    fn time_unit(self) -> TimeUnit {
        match self {
            Self::NanosecondsErrorOnOverflow => TimeUnit::Nanosecond,
            Self::Microseconds => TimeUnit::Microsecond,
        }
    }
}

/// Drain `rs` into a single record batch.
pub(crate) async fn result_set_to_batch(
    mut rs: ResultSet,
    precision: TimestampPrecision,
) -> Result<(SchemaRef, RecordBatch)> {
    let mut rows: Vec<Row> = Vec::new();
    while let Some(row) = rs.next().await {
        rows.push(row.map_err(from_spanner)?);
    }
    // The ResultSet retains the metadata sent with the first partial result set, so the schema is
    // known even for an empty result.
    rows_to_batch(Some(rs.metadata()), &rows, precision)
}

/// Convert rows drained elsewhere, such as DML `THEN RETURN` rows read inside a transaction
/// runner.
pub(crate) fn rows_to_batch(
    metadata: Option<&ResultSetMetadata>,
    rows: &[Row],
    precision: TimestampPrecision,
) -> Result<(SchemaRef, RecordBatch)> {
    let schema = build_schema(metadata, rows.first(), precision)?;
    let batch = build_batch(schema.clone(), rows)?;
    Ok((schema, batch))
}

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
    let schema = if first.is_empty() && rs.metadata().column_names().is_empty() {
        fallback_schema.unwrap_or_else(|| Arc::new(Schema::empty()))
    } else {
        build_schema(Some(rs.metadata()), first.first(), precision)?
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
            let schema = build_schema(Some(rs.metadata()), rows.first(), precision)?;
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
                // An empty chunk ends the stream.
                Ok(
                    next_bound_chunk(transaction, statements.as_mut(), result_set, *batch_size)
                        .await?
                        .unwrap_or_default(),
                )
            },
        )
    }
}

/// Pull the next non-empty chunk, executing further statements as result sets drain so a row
/// with an empty result yields no empty batch. `None` once everything is drained.
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

/// Build the result schema from column metadata, falling back to `Utf8` and the first row's
/// width when metadata is unavailable. Type errors include the column name.
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
                    // Name the column, as `build_column` does, so errors are traceable in wide
                    // results.
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

/// Build the Arrow field for a Spanner column type, tagging `JSON` with the `arrow.json`
/// extension. Rejects types nested deeper than [`MAX_TYPE_NESTING_DEPTH`].
pub(crate) fn arrow_field(
    name: impl Into<String>,
    ty: &Type,
    nullable: bool,
    precision: TimestampPrecision,
) -> Result<Field> {
    arrow_field_at(name, ty, nullable, precision, 0)
}

/// [`arrow_field`] at nesting `depth`.
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

/// Recognize `arrow.json` only on string storage, looking through dictionary encoding.
/// Binding and ingest DDL use this to preserve JSON types without accepting invalid tags.
pub(crate) fn is_json_field(field: &Field) -> bool {
    let mut storage = field.data_type();
    while let DataType::Dictionary(_, value) = storage {
        storage = value;
    }
    matches!(
        storage,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    ) && field
        .metadata()
        .get(ARROW_EXTENSION_NAME)
        .map(String::as_str)
        == Some(ARROW_JSON_EXTENSION)
}

/// Field metadata marking a Utf8 column as `arrow.json`, whose extension metadata is empty.
fn json_extension_metadata() -> HashMap<String, String> {
    HashMap::from([
        (
            ARROW_EXTENSION_NAME.to_string(),
            ARROW_JSON_EXTENSION.to_string(),
        ),
        (ARROW_EXTENSION_METADATA.to_string(), String::new()),
    ])
}

fn arrow_type(ty: &Type, precision: TimestampPrecision, depth: usize) -> Result<DataType> {
    // Fail on pathologically nested metadata from a hostile endpoint instead of exhausting the
    // stack.
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
            // Spanner has no arrays of arrays; anything unexpected falls back to JSON text.
            Some(element) if !matches!(element.code(), TypeCode::Array | TypeCode::Unspecified) => {
                DataType::List(Arc::new(arrow_field_at(
                    LIST_ITEM,
                    element,
                    true,
                    precision,
                    depth + 1,
                )?))
            }
            _ => DataType::Utf8,
        },
        // ENUM arrives as a decimal ordinal string (like INT64), PROTO as base64 (like BYTES).
        TypeCode::Enum => DataType::Int64,
        TypeCode::Proto => DataType::Binary,
        // STRING, JSON, UUID, INTERVAL and unknown codes. Only JSON is tagged, so UUID and
        // INTERVAL values (like ENUM and PROTO) bind back as their storage type, and DML needs
        // `CAST(@p AS ...)` to write one back. Bulk-ingest mutations are unaffected.
        _ => DataType::Utf8,
    })
}

/// Falls back to `Utf8` if the metadata lacks the struct's fields.
fn struct_arrow_type(ty: &Type, precision: TimestampPrecision, depth: usize) -> Result<DataType> {
    match ty.struct_type() {
        Some(st) => Ok(DataType::Struct(struct_fields(st, precision, depth)?)),
        None => Ok(DataType::Utf8),
    }
}

/// Child fields of a struct type. Names are kept verbatim, including empty and duplicate ones.
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
    // Reuse one column of value pointers instead of materializing a rows x columns matrix.
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

/// [`build_array`] for a top-level column, naming the column in errors.
fn build_column(field: &FieldRef, values: &[Option<&Value>]) -> Result<ArrayRef> {
    build_array(field.data_type(), values).map_err(|mut e| {
        e.message = format!("column {:?}: {}", field.name(), e.message);
        e
    })
}

/// The value, unless it is SQL `NULL` or absent.
fn present(value: Option<&Value>) -> Option<&Value> {
    value.filter(|v| v.kind() != Kind::Null)
}

fn arrow_err(e: ArrowError) -> adbc_core::error::Error {
    err(
        format!("failed to build Arrow array: {e}"),
        Status::Internal,
    )
}

/// Error for a non-NULL value that cannot be decoded as its type. Mapping it to NULL instead would
/// silently corrupt data.
fn decode_error(spanner_type: &str, value: &Value) -> adbc_core::error::Error {
    err(
        format!(
            "cannot decode Spanner {spanner_type} wire value {}",
            value_to_json(value).map_or_else(|_| format!("{value:?}"), |json| json.to_string())
        ),
        Status::InvalidData,
    )
}

/// Builders [`build_with`] can append nulls to; Arrow's `ArrayBuilder` trait has no `append_null`.
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

/// Fill `builder` from one value per row: NULLs become null slots, other values go to `append`.
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

/// [`build_with`] for primitive arrays, open-coded: routing each append through a closure measured
/// ~8% slower on the `dates_date32` and `nested_array_int64_struct` benchmarks.
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

/// Build an Arrow array of `data_type` from one Spanner value per row. SQL NULLs become null
/// slots; a value that cannot be decoded is an error. `pub(crate)` for `crate::bench_support`.
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
            let wide = parse_f64(v)?;
            let narrow = wide as f32;
            // Preserve explicit infinities, but never turn a finite wire value into one.
            (!wide.is_finite() || narrow.is_finite()).then_some(narrow)
        })?),
        DataType::Date32 => Arc::new(build_primitive::<Date32Type>(values, "DATE", |v| {
            v.as_str().and_then(parse_date_days)
        })?),
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
            // Distinguish malformed strings from valid instants outside the i64 nanosecond range.
            let array = build_with(
                values,
                TimestampNanosecondArray::builder(values.len()),
                |builder, v| {
                    let s = v.as_str().ok_or_else(|| decode_error("TIMESTAMP", v))?;
                    builder.append_value(parse_timestamp_nanos(s).ok_or_else(|| {
                        if scan_timestamp_utc(s).is_some() {
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
            // Every Spanner instant fits in i64 microseconds, so there is no out-of-range case.
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
        // BYTES arrive as base64. Decoding into one reused buffer avoids an allocation per cell;
        // `decode_vec` appends, so clear it first.
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
        // Utf8 and fallbacks: strings verbatim, anything else as JSON text.
        _ => Arc::new(
            build_with(values, StringBuilder::new(), |builder, v| {
                match v.as_str() {
                    Some(s) => builder.append_value(s),
                    None => builder.append_value(value_to_json(v)?.to_string()),
                }
                Ok(())
            })?
            .finish(),
        ),
    })
}

/// Build an Arrow `List` array. A non-NULL value that is not a list is an error.
fn build_list(field: &FieldRef, values: &[Option<&Value>]) -> Result<ArrayRef> {
    let mut children: Vec<Option<&Value>> = Vec::new();
    let mut offsets: Vec<i32> = Vec::with_capacity(values.len() + 1);
    offsets.push(0);
    let mut validity = NullBufferBuilder::new(values.len());
    for &value in values {
        match present(value) {
            None => validity.append_null(),
            Some(v) if v.kind() == Kind::List => {
                children.extend(v.as_list().expect("list kind").iter().map(Some));
                validity.append_non_null();
            }
            Some(v) => return Err(decode_error("ARRAY", v)),
        }
        // `children.len()` is the running element count, which may exceed the i32 offset range.
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
        validity.finish(),
    )
    .map_err(arrow_err)?;
    Ok(Arc::new(list))
}

/// Build an Arrow `Struct` array. Only Spanner's positional encoding (a list in field order) is
/// accepted, which keeps duplicate and empty field names apart.
fn build_struct(fields: &Fields, values: &[Option<&Value>]) -> Result<ArrayRef> {
    let mut rows = Vec::with_capacity(values.len());
    let mut validity = NullBufferBuilder::new(values.len());
    for &value in values {
        match present(value) {
            None => {
                rows.push(None);
                validity.append_null();
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
                validity.append_non_null();
            }
            Some(v) => return Err(decode_error("STRUCT", v)),
        }
    }
    // Reuse one column of pointers instead of allocating a rows x fields matrix.
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
    let array =
        StructArray::try_new_with_length(fields.clone(), arrays, validity.finish(), values.len())
            .map_err(arrow_err)?;
    Ok(Arc::new(array))
}

/// Spanner sends `INT64` as a string so values above 2^53 survive. A JSON number is rejected
/// because it may already have been rounded through `f64`.
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
        s => s.parse::<f64>().ok().filter(|f| f.is_finite()),
    }
}

/// Parse a Spanner `DATE` (`YYYY-MM-DD`) into days since the Unix epoch.
///
/// Reads fixed byte offsets instead of using chrono's strftime parser, which re-tokenizes its
/// format on every call and was over half the temporal conversion cost in profiling. Spanner
/// always sends this canonical shape; anything else returns `None`.
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
    if year == 0 {
        return None;
    }
    let month = digit(5)? * 10 + digit(6)?;
    let day = digit(8)? * 10 + digit(9)?;
    let date = chrono::NaiveDate::from_ymd_opt(year as i32, month, day)?;
    // `num_days_from_ce` counts 0001-01-01 as day 1, making 1970-01-01 day 719_163.
    Some(chrono::Datelike::num_days_from_ce(&date) - 719_163)
}

/// The two ASCII digits at `off` as a number.
fn two_digits(b: &[u8], off: usize) -> Option<u32> {
    let hi = b[off].wrapping_sub(b'0');
    let lo = b[off + 1].wrapping_sub(b'0');
    (hi < 10 && lo < 10).then(|| u32::from(hi) * 10 + u32::from(lo))
}

/// Scan the canonical UTC form Spanner emits, `YYYY-MM-DDTHH:MM:SS[.fraction]Z` with 1-9
/// fractional digits, at fixed offsets instead of using chrono's RFC 3339 parser. chrono still
/// validates the fields. Any other shape returns `None`.
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
    if year == 0 {
        return None;
    }
    let month = two_digits(b, 5)?;
    let day = two_digits(b, 8)?;
    let hour = two_digits(b, 11)?;
    let min = two_digits(b, 14)?;
    let sec = two_digits(b, 17)?;
    // After the seconds: either `Z` (no fraction) or `.<1..=9 digits>Z`.
    let nanos = match b[19] {
        b'Z' if b.len() == 20 => 0,
        // The length check keeps the slice below in bounds for a bare trailing `.`.
        b'.' if b.len() > 20 => {
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
            // `.789` is 789_000_000 nanoseconds.
            nanos * 10u32.pow(9 - frac.len() as u32)
        }
        _ => return None,
    };
    chrono::NaiveDate::from_ymd_opt(year as i32, month, day)?
        .and_hms_nano_opt(hour, min, sec, nanos)
}

/// Parse a Spanner `TIMESTAMP` into nanoseconds since the Unix epoch. `None` if malformed or
/// outside the i64 nanosecond range (~1677-09-21 to 2262-04-11).
pub(crate) fn parse_timestamp_nanos(s: &str) -> Option<i64> {
    scan_timestamp_utc(s)?.and_utc().timestamp_nanos_opt()
}

/// Parse a Spanner `TIMESTAMP` into microseconds since the Unix epoch, truncating sub-microsecond
/// digits toward negative infinity: chrono splits an instant into floor seconds plus a
/// non-negative fraction, so `1969-12-31T23:59:59.9999999Z` becomes -1. `None` if malformed.
pub(crate) fn parse_timestamp_micros(s: &str) -> Option<i64> {
    let dt = scan_timestamp_utc(s)?.and_utc();
    dt.timestamp()
        .checked_mul(1_000_000)?
        .checked_add(i64::from(dt.timestamp_subsec_micros()))
}

/// Parse a Spanner `NUMERIC` into an unscaled `i128` at scale 9. `None` if malformed or beyond
/// precision 38.
pub(crate) fn parse_numeric_i128(s: &str) -> Option<i128> {
    // Spanner emits only `[-]digits[.digits]` with at most nine fractional digits. Scanning bytes
    // avoids `trim` and `split_once('.')`, which dominated NUMERIC decoding in profiling.
    let (negative, bytes) = match s.as_bytes().split_first() {
        Some((b'-', rest)) => (true, rest),
        _ => (false, s.as_bytes()),
    };
    // A trailing `.` is malformed.
    let (int_part, frac_part) = match bytes.iter().position(|&b| b == b'.') {
        Some(i) if i + 1 < bytes.len() => (&bytes[..i], &bytes[i + 1..]),
        Some(_) => return None,
        None => (bytes, &bytes[bytes.len()..]),
    };
    // Rejects "", "-" and a leading `.`.
    if int_part.is_empty() || frac_part.len() > NUMERIC_SCALE as usize {
        return None;
    }
    let mut int_val: i128 = 0;
    for &b in int_part {
        let d = b.wrapping_sub(b'0');
        if d >= 10 {
            return None;
        }
        int_val = int_val.checked_mul(10)?.checked_add(i128::from(d))?;
    }
    let mut frac_val: i128 = 0;
    for &b in frac_part {
        let d = b.wrapping_sub(b'0');
        if d >= 10 {
            return None;
        }
        frac_val = frac_val * 10 + i128::from(d);
    }
    // `.25` is 250_000_000.
    frac_val *= 10_i128.pow(NUMERIC_SCALE as u32 - frac_part.len() as u32);
    let unscaled = int_val.checked_mul(1_000_000_000)?.checked_add(frac_val)?;
    // i128 also holds some 39-digit values, which Decimal128(38, 9) cannot represent.
    if unscaled >= 10_i128.pow(u32::from(NUMERIC_PRECISION)) {
        return None;
    }
    Some(if negative { -unscaled } else { unscaled })
}

/// Convert a Spanner value to JSON, for rendering non-string values as text.
fn value_to_json(value: &Value) -> Result<serde_json::Value> {
    Ok(match value.kind() {
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
                .collect::<Result<_>>()?,
        ),
        Kind::Unknown => {
            return Err(err(
                format!("cannot decode unexpected Spanner wire value {value:?}"),
                Status::InvalidData,
            ));
        }
    })
}

#[cfg(test)]
mod tests;

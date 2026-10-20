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
use std::ops::{Add, Mul};
use std::sync::Arc;

use adbc_core::error::{Result, Status};
use adbc_core::options::OptionValue;
use arrow_array::builder::{BinaryBuilder, BooleanBuilder, PrimitiveBuilder, StringBuilder};
use arrow_array::types::{
    ArrowPrimitiveType, Date32Type, Decimal128Type, Float32Type, Float64Type, Int64Type,
    TimestampMicrosecondType,
};
use arrow_array::{
    ArrayRef, ListArray, PrimitiveArray, RecordBatch, RecordBatchOptions, StructArray,
    TimestampNanosecondArray,
};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer, ScalarBuffer};
use arrow_schema::{ArrowError, DataType, Field, FieldRef, Fields, Schema, SchemaRef, TimeUnit};
use base64::Engine;
use google_cloud_spanner::result::{ResultSet, ResultSetMetadata, Row};
use google_cloud_spanner::value::{Kind, Type, TypeCode, Value};

use crate::error::{err, from_spanner, invalid_argument};

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
        let key = crate::OPTION_MAX_TIMESTAMP_PRECISION;
        let s = crate::options::string_option(value, key)?;
        match s.trim() {
            "" => Ok(Self::default()),
            Self::NANOSECONDS_ERROR_ON_OVERFLOW => Ok(Self::NanosecondsErrorOnOverflow),
            Self::MICROSECONDS => Ok(Self::Microseconds),
            other => Err(invalid_argument(format!(
                "option {key} expects {:?} or {:?} (or \"\" to reset to the default), got {other:?}",
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
    rows_to_batch(rs.metadata(), &rows, precision)
}

/// Convert rows drained elsewhere, such as DML `THEN RETURN` rows read inside a transaction
/// runner.
pub(crate) fn rows_to_batch(
    metadata: &ResultSetMetadata,
    rows: &[Row],
    precision: TimestampPrecision,
) -> Result<(SchemaRef, RecordBatch)> {
    let schema = build_schema(metadata, precision)?;
    let batch = build_batch(schema.clone(), rows)?;
    Ok((schema, batch))
}

/// Build the result schema from column metadata. The client rejects rows that arrive without
/// it, so every row has one value per column.
pub(crate) fn build_schema(
    metadata: &ResultSetMetadata,
    precision: TimestampPrecision,
) -> Result<SchemaRef> {
    let fields = metadata
        .column_names()
        .iter()
        .zip(metadata.column_types())
        .map(|(name, ty)| arrow_field(name, ty, true, precision).map_err(within("column", name)))
        .collect::<Result<Vec<_>>>()?;
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
        TypeCode::Struct => match ty.struct_type() {
            Some(st) => DataType::Struct(struct_fields(st, precision, depth)?),
            // Without its fields, a struct can only be shown as JSON text.
            None => DataType::Utf8,
        },
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

pub(crate) fn build_batch(schema: SchemaRef, rows: &[Row]) -> Result<RecordBatch> {
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

    // An explicit row count lets a result with no columns form a batch.
    let options = RecordBatchOptions::new().with_row_count(Some(rows.len()));
    RecordBatch::try_new_with_options(schema, arrays, &options).map_err(|e| {
        err(
            format!("failed to build record batch: {e}"),
            Status::Internal,
        )
    })
}

/// Prefix an error with the column or struct field it occurred in, so errors are traceable in
/// wide and nested results.
fn within(
    kind: &str,
    name: &str,
) -> impl FnOnce(adbc_core::error::Error) -> adbc_core::error::Error {
    move |mut e| {
        e.message = format!("{kind} {name:?}: {}", e.message);
        e
    }
}

fn build_column(field: &FieldRef, values: &[Option<&Value>]) -> Result<ArrayRef> {
    build_array(field.data_type(), values).map_err(within("column", field.name()))
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

/// Fill `builder` from one value per row: NULLs go to `append_null`, other values to `append`.
/// Arrow's `ArrayBuilder` trait has no `append_null`, so callers pass the builder's own.
#[inline]
fn build_with<B>(
    values: &[Option<&Value>],
    mut builder: B,
    append_null: impl Fn(&mut B),
    mut append: impl FnMut(&mut B, &Value) -> Result<()>,
) -> Result<B> {
    for &value in values {
        match present(value) {
            None => append_null(&mut builder),
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
                BooleanBuilder::append_null,
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
                PrimitiveBuilder::append_null,
                |builder, v| {
                    let s = v.as_str().ok_or_else(|| decode_error("TIMESTAMP", v))?;
                    builder.append_value(parse_timestamp_nanos(s).ok_or_else(|| {
                        if parse_timestamp(s).is_none() {
                            return decode_error("TIMESTAMP", v);
                        }
                        // A value out of range for its type is a data error, as in PEP 249.
                        err(
                            format!(
                                "TIMESTAMP {s:?} is outside the Arrow Timestamp(Nanosecond) \
                                 range (1677-09-21 to 2262-04-11); set {}={} to read it",
                                crate::OPTION_MAX_TIMESTAMP_PRECISION,
                                TimestampPrecision::MICROSECONDS,
                            ),
                            Status::InvalidData,
                        )
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
            let builder = BinaryBuilder::new();
            Arc::new(
                build_with(values, builder, BinaryBuilder::append_null, |builder, v| {
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
        _ => {
            let builder = StringBuilder::new();
            Arc::new(
                build_with(values, builder, StringBuilder::append_null, |builder, v| {
                    match v.as_str() {
                        Some(s) => builder.append_value(s),
                        None => builder.append_value(value_to_json(v)?.to_string()),
                    }
                    Ok(())
                })?
                .finish(),
            )
        }
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
            build_array(field.data_type(), &scratch).map_err(within("field", field.name()))
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

/// The value of a run of ASCII digits, or `None` if any byte is not a digit. Callers bound the
/// length so the result fits in `T`.
fn digits<T: From<u8> + Mul<Output = T> + Add<Output = T>>(b: &[u8]) -> Option<T> {
    b.iter().try_fold(T::from(0), |n, &c| {
        let d = c.wrapping_sub(b'0');
        (d < 10).then(|| n * T::from(10) + T::from(d))
    })
}

/// Days since 1970-01-01 of a `YYYY-MM-DD` date in years 1-9999, or `None` if it is malformed or
/// does not exist.
fn date_days(b: &[u8]) -> Option<i32> {
    if b.len() != 10 || b[4] != b'-' || b[7] != b'-' {
        return None;
    }
    let (year, month, day): (u32, u32, u32) =
        (digits(&b[..4])?, digits(&b[5..7])?, digits(&b[8..])?);
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let month_days = match month {
        2 => 28 + u32::from(leap),
        4 | 6 | 9 | 11 => 30,
        1..=12 => 31,
        _ => return None,
    };
    if year == 0 || day == 0 || day > month_days {
        return None;
    }
    // Howard Hinnant's `days_from_civil`: years start in March so that the leap day comes last,
    // and the calendar repeats every 400 years (146,097 days).
    let y = if month <= 2 { year - 1 } else { year };
    let (era, year_of_era) = (y / 400, y % 400);
    let day_of_year = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    Some((era * 146_097 + day_of_era) as i32 - 719_468)
}

/// Parse a Spanner `DATE` (`YYYY-MM-DD`, the only form Spanner sends) into days since the Unix
/// epoch.
pub(crate) fn parse_date_days(s: &str) -> Option<i32> {
    date_days(s.as_bytes())
}

/// Parse the canonical UTC form Spanner sends for a `TIMESTAMP`,
/// `YYYY-MM-DDTHH:MM:SS[.fraction]Z` with 1-9 fractional digits, into whole seconds since the
/// Unix epoch and the nanoseconds past them. Any other shape returns `None`.
fn parse_timestamp(s: &str) -> Option<(i64, u32)> {
    let b = s.as_bytes();
    if b.len() < 20 || b[10] != b'T' || b[13] != b':' || b[16] != b':' || b[b.len() - 1] != b'Z' {
        return None;
    }
    let days = date_days(&b[..10])?;
    let (hour, min, sec): (u32, u32, u32) = (
        digits(&b[11..13])?,
        digits(&b[14..16])?,
        digits(&b[17..19])?,
    );
    if hour > 23 || min > 59 || sec > 59 {
        return None;
    }
    let nanos = match &b[19..b.len() - 1] {
        [] => 0,
        [b'.', frac @ ..] if (1..=9).contains(&frac.len()) => {
            digits::<u32>(frac)? * 10_u32.pow(9 - frac.len() as u32)
        }
        _ => return None,
    };
    let secs = i64::from(days) * 86_400 + i64::from(hour * 3_600 + min * 60 + sec);
    Some((secs, nanos))
}

/// Parse a Spanner `TIMESTAMP` into nanoseconds since the Unix epoch. `None` if malformed or
/// outside the i64 nanosecond range (~1677-09-21 to 2262-04-11).
pub(crate) fn parse_timestamp_nanos(s: &str) -> Option<i64> {
    let (secs, nanos) = parse_timestamp(s)?;
    i64::try_from(i128::from(secs) * 1_000_000_000 + i128::from(nanos)).ok()
}

/// Parse a Spanner `TIMESTAMP` into microseconds since the Unix epoch, truncating sub-microsecond
/// digits toward negative infinity, so `1969-12-31T23:59:59.9999999Z` becomes -1. `None` if
/// malformed.
pub(crate) fn parse_timestamp_micros(s: &str) -> Option<i64> {
    let (secs, nanos) = parse_timestamp(s)?;
    Some(secs * 1_000_000 + i64::from(nanos / 1_000))
}

/// Parse a Spanner `NUMERIC` (`[-]digits[.digits]`, at most 29 integer and 9 fractional digits)
/// into an unscaled `i128` at scale 9. `None` if malformed or out of range.
pub(crate) fn parse_numeric_i128(s: &str) -> Option<i128> {
    let (negative, b) = match s.as_bytes() {
        [b'-', rest @ ..] => (true, rest),
        b => (false, b),
    };
    let (int, frac) = match b.iter().position(|&c| c == b'.') {
        Some(i) if i + 1 < b.len() => (&b[..i], &b[i + 1..]),
        Some(_) => return None,
        None => (b, &b[b.len()..]),
    };
    let int_digits = usize::from(NUMERIC_PRECISION) - NUMERIC_SCALE as usize;
    if !(1..=int_digits).contains(&int.len()) || frac.len() > NUMERIC_SCALE as usize {
        return None;
    }
    // `.25` is 250_000_000.
    let frac = digits::<u32>(frac)? * 10_u32.pow(NUMERIC_SCALE as u32 - frac.len() as u32);
    let unscaled = (digits::<u128>(int)? * 1_000_000_000 + u128::from(frac)) as i128;
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

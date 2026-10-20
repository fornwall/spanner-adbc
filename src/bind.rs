//! Convert Arrow parameter rows and bulk-ingest rows to Spanner values.
//!
//! [`resolve_parameter_names`] maps columns to named SQL parameters; [`cell_value`] supplies the
//! shared encoding for queries and mutations. Dictionary cells bind as their decoded values.
//! Lists, large lists and fixed-size lists of supported scalars, dictionary-encoded or not, become
//! arrays; nested arrays and structs are rejected.
//!
//! Integers widen to INT64, except UInt64, which can exceed its range. Float16/Float32 widen
//! losslessly. Strings, bytes, booleans, dates, timestamps and Decimal128 retain their values and
//! nulls. Timestamps preserve the source unit's precision; decimals must fit `NUMERIC` exactly.
//!
//! Parameters carry their Arrow logical type, including nulls and empty lists. `arrow.json`-tagged
//! strings bind as `JSON`; Arrow `Null` leaves inference to SQL. Mutations use the target column's
//! type instead.

use std::ops::Range;

use adbc_core::error::Result;
use arrow_array::cast::AsArray;
use arrow_array::types::{
    ArrowPrimitiveType, ArrowTimestampType, Date32Type, Date64Type, Float16Type, Float32Type,
    Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt8Type, UInt16Type,
    UInt32Type,
};
use arrow_array::{
    Array, Decimal128Array, OffsetSizeTrait, RecordBatch, downcast_dictionary_array,
};
use arrow_schema::{DataType, Field, TimeUnit};
use chrono::{DateTime, Datelike, NaiveDate, SecondsFormat, Utc};
use google_cloud_spanner::mutation::Mutation;
use google_cloud_spanner::statement::StatementBuilder;
use google_cloud_spanner::types::{self, Type};
use google_cloud_spanner::value::Value;

use crate::conversion::is_json_field;
use crate::error::{annotate, invalid_argument};
use crate::sql::{has_positional_placeholder, named_parameters, qualified_table, quote_ident};

/// Bind one row using parameter names resolved once for the SQL and batch schema.
pub(crate) fn bind_params(
    mut builder: StatementBuilder,
    names: &[String],
    batch: &RecordBatch,
    row: usize,
) -> Result<StatementBuilder> {
    let fields = batch.schema_ref().fields();
    for ((name, field), column) in names.iter().zip(fields).zip(batch.columns()) {
        builder = bind_one(builder, name, field, column.as_ref(), row)?;
    }
    Ok(builder)
}

/// Map bound columns to query parameters, resolving names once for reuse across rows.
///
/// Positional binding pairs columns with distinct parameters in query order and requires equal
/// counts. By-name binding matches case-insensitively, rejecting unknown or duplicate columns;
/// omitted query parameters remain unbound for Spanner to report.
///
/// A failure on SQL written with `?` placeholders says that GoogleSQL needs `@name` parameters.
pub(crate) fn resolve_parameter_names(
    sql: &str,
    batch: &RecordBatch,
    bind_by_name: bool,
) -> Result<Vec<String>> {
    resolve_names(sql, batch, bind_by_name).map_err(|error| {
        if has_positional_placeholder(sql) {
            annotate(error, |message| {
                format!("{message}; {POSITIONAL_PLACEHOLDER_HINT}")
            })
        } else {
            error
        }
    })
}

/// Appended to a failed parameter resolution when the SQL contains `?`.
const POSITIONAL_PLACEHOLDER_HINT: &str = "the SQL contains a `?` placeholder, which GoogleSQL \
     does not support: use named parameters such as `@p1`, `@p2` instead";

fn resolve_names(sql: &str, batch: &RecordBatch, bind_by_name: bool) -> Result<Vec<String>> {
    let schema = batch.schema();
    let params = named_parameters(sql);

    // Spanner matches case-insensitively; send each name using the query's spelling.
    if bind_by_name {
        let by_name: std::collections::HashMap<String, &str> = params
            .iter()
            .map(|p| (p.to_ascii_lowercase(), p.as_str()))
            .collect();
        let mut resolved = Vec::with_capacity(batch.num_columns());
        let mut bound = std::collections::HashSet::with_capacity(batch.num_columns());
        for field in schema.fields() {
            let column = field.name();
            let Some(param) = by_name.get(&column.to_ascii_lowercase()) else {
                return Err(invalid_argument(format!(
                    "cannot bind column {column:?}: adbc.statement.bind_by_name is true, so every \
                     bound column must be named after one of the query's parameters, which are \
                     {params:?}; rename the column or set adbc.statement.bind_by_name to false to \
                     bind positionally",
                )));
            };
            // Reject duplicate bindings before Spanner sees the request.
            if !bound.insert(*param) {
                return Err(invalid_argument(format!(
                    "cannot bind column {column:?}: it names the same query parameter {param:?} as \
                     an earlier column (parameter names are case-insensitive); bind one column per \
                     parameter",
                )));
            }
            resolved.push((*param).to_string());
        }
        return Ok(resolved);
    }

    if params.len() != batch.num_columns() {
        return Err(invalid_argument(format!(
            "parameter count mismatch: query references {} parameter(s) {:?} but {} column(s) were bound",
            params.len(),
            params,
            batch.num_columns(),
        )));
    }
    Ok(params)
}

/// Bind one cell with its Arrow logical type, preserving JSON extension metadata.
fn bind_one(
    builder: StatementBuilder,
    name: &str,
    field: &Field,
    column: &dyn Array,
    row: usize,
) -> Result<StatementBuilder> {
    let (value, param_type) = cell_value(Target::Parameter(name), field, column, row)?;
    let param_type = param_type.or_else(|| parameter_type(column.data_type()));
    Ok(match param_type {
        Some(t) => builder.add_typed_param(name, value, t),
        None => builder.add_param(name, value),
    })
}

/// Type an already-validated binding. Arrow `Null` (including list elements) needs SQL context.
/// Keep this separate from value encoding: mutation values take their type from the target table.
fn parameter_type(data_type: &DataType) -> Option<Type> {
    match dictionary_values(data_type) {
        DataType::List(item) | DataType::LargeList(item) | DataType::FixedSizeList(item, _) => {
            Some(types::array(parameter_type(item.data_type())?))
        }
        scalar => scalar_type(scalar).map(|(spanner_type, _)| spanner_type),
    }
}

/// The Spanner type and DDL column type of a scalar that [`scalar_binder`] accepts. Arrow `Null`
/// has neither.
fn scalar_type(data_type: &DataType) -> Option<(Type, &'static str)> {
    Some(match data_type {
        DataType::Boolean => (types::bool(), "BOOL"),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32 => (types::int64(), "INT64"),
        DataType::Float16 | DataType::Float32 => (types::float32(), "FLOAT32"),
        DataType::Float64 => (types::float64(), "FLOAT64"),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            (types::string(), "STRING(MAX)")
        }
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => (types::bytes(), "BYTES(MAX)"),
        DataType::Date32 | DataType::Date64 => (types::date(), "DATE"),
        DataType::Timestamp(_, _) => (types::timestamp(), "TIMESTAMP"),
        DataType::Decimal128(_, _) => (types::numeric(), "NUMERIC"),
        _ => return None,
    })
}

/// The value type of a possibly nested dictionary type.
fn dictionary_values(mut data_type: &DataType) -> &DataType {
    while let DataType::Dictionary(_, value) = data_type {
        data_type = value;
    }
    data_type
}

/// Build an insert mutation using the same value encoding as query parameters.
/// Mutation values take their type from the target column; column names are unquoted.
pub(crate) fn insert_mutation(table: &str, batch: &RecordBatch, row: usize) -> Result<Mutation> {
    let mut builder = Mutation::new_insert_builder(table);
    for (field, column) in batch.schema_ref().fields().iter().zip(batch.columns()) {
        let target = Target::Column(field.name());
        let (value, _param_type) = cell_value(target, field, column.as_ref(), row)?;
        builder = builder.set(field.name().clone()).to(value);
    }
    Ok(builder.build())
}

/// Run the mutation value conversion over `batch` and discard the values, so an ingest can reject
/// a bad value before its DDL runs. A column whose values always convert once its type is accepted
/// is checked on its first row only.
///
/// A non-nullable field's nulls are bad values too, since the create modes make it a `NOT NULL`
/// column. Arrow only checks physical nulls, which miss a valid dictionary key that selects a null
/// value.
pub(crate) fn check_ingest_values(batch: &RecordBatch) -> Result<()> {
    for (field, column) in batch.schema_ref().fields().iter().zip(batch.columns()) {
        if !field.is_nullable() && column.logical_null_count() > 0 {
            return Err(invalid_argument(format!(
                "ingest column {:?} is non-nullable but contains nulls; mark the field nullable \
                 or remove the nulls",
                field.name()
            )));
        }
        let rows = if values_always_convert(column.data_type()) {
            batch.num_rows().min(1)
        } else {
            batch.num_rows()
        };
        for row in 0..rows {
            cell_value(Target::Column(field.name()), field, column.as_ref(), row)?;
        }
    }
    Ok(())
}

/// Whether [`scalar_binder`] accepts every value of `data_type` once it accepts the type. Types
/// not listed here, such as dates, timestamps and decimals, are checked value by value.
fn values_always_convert(data_type: &DataType) -> bool {
    match dictionary_values(data_type) {
        DataType::Null
        | DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => true,
        DataType::List(item) | DataType::LargeList(item) | DataType::FixedSizeList(item, _) => {
            values_always_convert(item.data_type())
        }
        _ => false,
    }
}

/// Qualify a mutation table with its schema, without SQL identifier quoting.
pub(crate) fn mutation_table(db_schema: Option<&str>, table_name: &str) -> String {
    match db_schema.filter(|s| !s.is_empty()) {
        Some(schema) => format!("{schema}.{table_name}"),
        None => table_name.to_string(),
    }
}

/// Encode a cell for a query or mutation. Only JSON parameters need an explicit wire type.
fn cell_value(
    target: Target<'_>,
    field: &Field,
    column: &dyn Array,
    row: usize,
) -> Result<(Value, Option<Type>)> {
    let data_type = column.data_type();
    match data_type {
        DataType::List(item) => {
            list_cell_value(target, data_type, item, list_elements::<i32>(column, row))
        }
        DataType::LargeList(item) => {
            list_cell_value(target, data_type, item, list_elements::<i64>(column, row))
        }
        DataType::FixedSizeList(item, size) => {
            let list = column.as_fixed_size_list();
            let start = list.value_offset_at(row);
            let elements = list
                .is_valid(row)
                .then(|| (list.values().as_ref(), start..start + *size as usize));
            list_cell_value(target, data_type, item, elements)
        }
        // Decode recursively, validating the logical type for null keys too.
        DataType::Dictionary(_, _) => downcast_dictionary_array!(
            column => match column.key(row) {
                Some(value_row) => cell_value(target, field, column.values().as_ref(), value_row),
                None => null_dictionary_value(target, field, column.values().data_type()),
            },
            _ => unreachable!("downcast_dictionary_array dispatched a non-dictionary {data_type:?}")
        ),
        _ => {
            let bind = scalar_binder(data_type).ok_or_else(|| unsupported(target, data_type))?;
            let value = bind(target, column, row)?;
            Ok((value, is_json_field(field).then(types::json)))
        }
    }
}

/// The child values and element range of list cell `row`, or `None` for a null list.
fn list_elements<O: OffsetSizeTrait>(
    column: &dyn Array,
    row: usize,
) -> Option<(&dyn Array, Range<usize>)> {
    let list = column.as_list::<O>();
    let offsets = list.value_offsets();
    list.is_valid(row).then(|| {
        (
            list.values().as_ref(),
            offsets[row].as_usize()..offsets[row + 1].as_usize(),
        )
    })
}

/// What a converted value is bound to, for error messages.
#[derive(Clone, Copy)]
enum Target<'a> {
    Parameter(&'a str),
    Column(&'a str),
}

impl std::fmt::Display for Target<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Target::Parameter(name) => write!(f, "parameter {name:?}"),
            Target::Column(name) => write!(f, "ingest column {name:?}"),
        }
    }
}

fn unsupported(target: Target<'_>, data_type: &DataType) -> adbc_core::error::Error {
    invalid_argument(format!(
        "cannot bind {target}: unsupported Arrow type {data_type}"
    ))
}

/// Scalar encoder for one array type, reused across all elements of a list.
///
/// Encoders map a valid cell through `Option::into`, which consumes owned strings where
/// `ToValue` would copy them.
type ScalarBinder = fn(Target<'_>, &dyn Array, usize) -> Result<Value>;

/// Select the shared scalar encoder, or `None` for an unsupported type. Keep new type support
/// aligned with [`scalar_type`] and the read mapping.
fn scalar_binder(data_type: &DataType) -> Option<ScalarBinder> {
    Some(match data_type {
        // Spanner uses INT64; smaller integer and float widths widen losslessly.
        DataType::Int64 => primitive_binder::<Int64Type, i64>(),
        DataType::Int32 => primitive_binder::<Int32Type, i64>(),
        DataType::Int16 => primitive_binder::<Int16Type, i64>(),
        DataType::Int8 => primitive_binder::<Int8Type, i64>(),
        // UInt64 is excluded because its range exceeds INT64.
        DataType::UInt32 => primitive_binder::<UInt32Type, i64>(),
        DataType::UInt16 => primitive_binder::<UInt16Type, i64>(),
        DataType::UInt8 => primitive_binder::<UInt8Type, i64>(),
        DataType::Float64 => primitive_binder::<Float64Type, f64>(),
        DataType::Float32 => primitive_binder::<Float32Type, f64>(),
        DataType::Float16 => primitive_binder::<Float16Type, f64>(),
        DataType::Boolean => |_, a, i| Ok(a.is_valid(i).then(|| a.as_boolean().value(i)).into()),
        DataType::Utf8 => |_, a, i| Ok(a.is_valid(i).then(|| a.as_string::<i32>().value(i)).into()),
        DataType::LargeUtf8 => {
            |_, a, i| Ok(a.is_valid(i).then(|| a.as_string::<i64>().value(i)).into())
        }
        DataType::Utf8View => {
            |_, a, i| Ok(a.is_valid(i).then(|| a.as_string_view().value(i)).into())
        }
        DataType::Binary => {
            |_, a, i| Ok(a.is_valid(i).then(|| a.as_binary::<i32>().value(i)).into())
        }
        DataType::LargeBinary => {
            |_, a, i| Ok(a.is_valid(i).then(|| a.as_binary::<i64>().value(i)).into())
        }
        DataType::BinaryView => {
            |_, a, i| Ok(a.is_valid(i).then(|| a.as_binary_view().value(i)).into())
        }
        DataType::FixedSizeBinary(_) => |_, a, i| {
            Ok(a.is_valid(i)
                .then(|| a.as_fixed_size_binary().value(i))
                .into())
        },
        DataType::Date32 => |target, a, i| {
            a.is_valid(i)
                .then(|| date_string(target, a.as_primitive::<Date32Type>().value(i).into()))
                .transpose()
                .map(Value::from)
        },
        DataType::Date64 => |target, a, i| {
            a.is_valid(i)
                .then(|| {
                    date_string(
                        target,
                        date64_days(target, a.as_primitive::<Date64Type>().value(i))?,
                    )
                })
                .transpose()
                .map(Value::from)
        },
        DataType::Timestamp(TimeUnit::Second, _) => timestamp_binder::<TimestampSecondType>(),
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            timestamp_binder::<TimestampMillisecondType>()
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            timestamp_binder::<TimestampMicrosecondType>()
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            timestamp_binder::<TimestampNanosecondType>()
        }
        // A larger scale, possible in an imported schema, would overflow `numeric_string`'s i128
        // divisor.
        DataType::Decimal128(_, scale) if *scale <= 38 => |target, a, i| {
            a.is_valid(i)
                .then(|| numeric_string(target, a.as_primitive(), i))
                .transpose()
                .map(Value::from)
        },
        // NullArray has no validity buffer, so its physical `is_valid` returns true.
        DataType::Null => |_, _, _| Ok(Value::null()),
        // List elements, as in Polars' `List(Categorical)`. Top-level dictionaries are decoded
        // by `cell_value`, which also handles dictionaries of lists.
        DataType::Dictionary(_, value) if scalar_binder(value).is_some() => |target, a, i| {
            downcast_dictionary_array!(
                a => match a.key(i) {
                    Some(key) => {
                        let values = a.values();
                        let bind = scalar_binder(values.data_type())
                            .ok_or_else(|| unsupported(target, values.data_type()))?;
                        bind(target, values.as_ref(), key)
                    }
                    None => Ok(Value::null()),
                },
                _ => unreachable!("downcast_dictionary_array dispatched a non-dictionary"),
            )
        },
        _ => return None,
    })
}

/// Widen a primitive through `From`, preserving nulls and returning a plain function pointer.
fn primitive_binder<T, V>() -> ScalarBinder
where
    T: ArrowPrimitiveType,
    V: From<T::Native> + Into<Value>,
{
    |_, a, i| {
        Ok(a.is_valid(i)
            .then(|| V::from(a.as_primitive::<T>().value(i)))
            .into())
    }
}

fn timestamp_binder<T: ArrowTimestampType>() -> ScalarBinder {
    |target, a, i| {
        a.is_valid(i)
            .then(|| timestamp_string(target, T::UNIT, a.as_primitive::<T>().value(i)))
            .transpose()
            .map(Value::from)
    }
}

/// Validate a null dictionary cell's logical type and retain its JSON parameter type.
fn null_dictionary_value(
    target: Target<'_>,
    field: &Field,
    value_type: &DataType,
) -> Result<(Value, Option<Type>)> {
    let value_type = dictionary_values(value_type);
    match value_type {
        DataType::List(item) | DataType::LargeList(item) | DataType::FixedSizeList(item, _) => {
            list_cell_value(target, value_type, item, None)
        }
        _ => {
            scalar_binder(value_type).ok_or_else(|| unsupported(target, value_type))?;
            Ok((Value::null(), is_json_field(field).then(types::json)))
        }
    }
}

/// Encode a list cell from its borrowed child range, avoiding a sliced array allocation.
/// `None` means a null array. Validate element types even for empty and null arrays.
fn list_cell_value(
    target: Target<'_>,
    list_type: &DataType,
    item: &Field,
    elements: Option<(&dyn Array, Range<usize>)>,
) -> Result<(Value, Option<Type>)> {
    let bind = scalar_binder(item.data_type()).ok_or_else(|| unsupported(target, list_type))?;
    let value = match elements {
        None => Value::null(),
        // Consume the elements; `ToValue` would deep-copy them.
        Some((values, range)) => Value::from(
            range
                .map(|i| bind(target, values, i))
                .collect::<Result<Vec<Value>>>()?,
        ),
    };
    Ok((
        value,
        is_json_field(item).then(|| types::array(types::json())),
    ))
}

/// Convert Date64 milliseconds to days. Arrow requires whole days but its constructors don't
/// check.
fn date64_days(target: Target<'_>, millis: i64) -> Result<i64> {
    if millis % 86_400_000 != 0 {
        return Err(invalid_argument(format!(
            "cannot bind {target} as DATE: Date64 value {millis}ms is not a whole number of days; use a timestamp type to retain time of day"
        )));
    }
    Ok(millis / 86_400_000)
}

/// Format days since the Unix epoch as the Spanner `DATE` wire form, `YYYY-MM-DD`.
fn date_string(target: Target<'_>, days: i64) -> Result<String> {
    // Chrono counts 0001-01-01 as day 1; 1970-01-01 is day 719_163.
    let date = i32::try_from(days + 719_163)
        .ok()
        .and_then(NaiveDate::from_num_days_from_ce_opt)
        .filter(|date| (1..=9999).contains(&date.year()))
        .ok_or_else(|| {
            invalid_argument(format!(
                "cannot bind {target} as DATE: {days} days since 1970-01-01 is out of range; Spanner supports 0001-01-01 through 9999-12-31"
            ))
        })?;
    // `Display` writes `YYYY-MM-DD` for these years without parsing a format string per call.
    Ok(date.to_string())
}

/// Format a UTC timestamp at its source precision, rejecting years outside Spanner's range.
/// Negative subsecond values retain their exact instant (for example, -1 ns ends in .999999999Z).
fn timestamp_string(target: Target<'_>, unit: TimeUnit, value: i64) -> Result<String> {
    let (ts, format) = match unit {
        TimeUnit::Second => (
            DateTime::<Utc>::from_timestamp(value, 0),
            SecondsFormat::Secs,
        ),
        TimeUnit::Millisecond => (
            DateTime::<Utc>::from_timestamp_millis(value),
            SecondsFormat::Millis,
        ),
        TimeUnit::Microsecond => (
            DateTime::<Utc>::from_timestamp_micros(value),
            SecondsFormat::Micros,
        ),
        // `from_timestamp_nanos` is infallible: every `i64` nanosecond count is in range.
        TimeUnit::Nanosecond => (
            Some(DateTime::<Utc>::from_timestamp_nanos(value)),
            SecondsFormat::Nanos,
        ),
    };
    let ts = ts.filter(|ts| (1..=9999).contains(&ts.year())).ok_or_else(|| {
        invalid_argument(format!(
            "cannot bind {target} as TIMESTAMP: {value} ({unit:?}) is out of range; Spanner supports years 0001 through 9999"
        ))
    })?;
    Ok(ts.to_rfc3339_opts(format, true))
}

/// Format a `Decimal128` cell, whose scale is at most 38, as a Spanner `NUMERIC` at scale 9. A
/// value that `NUMERIC` can't hold exactly is an error naming it: Spanner would reject it with a
/// failed precondition, and rounding would change the data.
fn numeric_string(target: Target<'_>, array: &Decimal128Array, row: usize) -> Result<String> {
    let value = array.value(row);
    let shift = 9 - i32::from(array.scale());
    let unscaled = match u32::try_from(shift) {
        // A zero fits at any scale, even where the multiplier overflows.
        Ok(shift) => match 10_i128.checked_pow(shift) {
            Some(multiplier) => value.checked_mul(multiplier),
            None => (value == 0).then_some(0),
        },
        Err(_) => {
            let divisor = 10_i128.pow(shift.unsigned_abs());
            (value % divisor == 0).then(|| value / divisor)
        }
    }
    .filter(|unscaled| unscaled.unsigned_abs() < 10_u128.pow(38))
    .ok_or_else(|| {
        invalid_argument(format!(
            "cannot bind {target} as NUMERIC: {} does not fit Spanner's NUMERIC, which holds up \
             to 29 integer and 9 fractional digits (precision 38, scale 9)",
            array.value_as_string(row)
        ))
    })?;
    let magnitude = unscaled.unsigned_abs();
    Ok(format!(
        "{}{}.{:09}",
        if unscaled < 0 { "-" } else { "" },
        magnitude / 1_000_000_000,
        magnitude % 1_000_000_000
    ))
}

/// Map an ingest field to DDL, rejecting types the value encoder cannot bind before any DDL runs.
/// JSON-tagged strings, including list elements, become `JSON`.
fn spanner_column_type(field: &Field) -> Result<String> {
    if is_json_field(field) {
        return Ok("JSON".to_string());
    }
    match dictionary_values(field.data_type()) {
        // List binding accepts scalar elements only, including for null or empty lists.
        DataType::List(item) | DataType::LargeList(item) | DataType::FixedSizeList(item, _)
            if scalar_binder(item.data_type()).is_some() =>
        {
            Ok(format!("ARRAY<{}>", spanner_column_type(item)?))
        }
        other => match scalar_binder(other).and(scalar_type(other)) {
            Some((_, column_type)) => Ok(column_type.to_string()),
            None => Err(invalid_argument(format!(
                "cannot create a Spanner column for Arrow type {other}; cast it to a type with a \
                 Spanner equivalent before ingesting"
            ))),
        },
    }
}

/// Build ingest DDL, optionally qualified by schema and guarded by `IF NOT EXISTS`.
///
/// With no primary key in the Arrow schema, omit `PRIMARY KEY` so Spanner supplies its hidden
/// rowid key (<https://cloud.google.com/spanner/docs/primary-key-default-value#tables-without-primary-keys>).
/// Callers needing a specific key should create the table themselves and ingest in append mode.
pub(crate) fn create_table_sql(
    table: &str,
    db_schema: Option<&str>,
    schema: &arrow_schema::Schema,
    if_not_exists: bool,
) -> Result<String> {
    let mut columns: Vec<String> = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let column_type = spanner_column_type(field)
            .map_err(|e| annotate(e, |m| format!("ingest column {:?}: {m}", field.name())))?;
        let not_null = if field.is_nullable() { "" } else { " NOT NULL" };
        columns.push(format!(
            "{} {column_type}{not_null}",
            quote_ident(field.name())?
        ));
    }
    let guard = if if_not_exists { "IF NOT EXISTS " } else { "" };
    Ok(format!(
        "CREATE TABLE {guard}{} ({})",
        qualified_table(db_schema, table)?,
        columns.join(", "),
    ))
}

#[cfg(test)]
mod tests;

//! Convert Arrow parameter rows and bulk-ingest rows to Spanner values.
//!
//! [`resolve_parameter_names`] maps columns to named SQL parameters; [`cell_value`] supplies the
//! shared encoding for queries and mutations. Dictionaries are decoded before binding. Lists of
//! supported scalars become arrays; nested arrays and structs are rejected.
//!
//! Integers widen to INT64, except UInt64, which can exceed its range. Float16/Float32 widen
//! losslessly. Strings, bytes, booleans, dates, timestamps and Decimal128 retain their values and
//! nulls. Timestamps preserve the source unit's precision; decimal formatting uses the full i128.
//!
//! Parameters carry their Arrow logical type, including nulls and empty lists. `arrow.json`-tagged
//! strings bind as `JSON`; Arrow `Null` leaves inference to SQL. Mutations use the target column's
//! type instead.

use std::ops::Range;

use adbc_core::error::Result;
use arrow_array::cast::AsArray;
use arrow_array::types::{
    ArrowPrimitiveType, Date32Type, Date64Type, Decimal128Type, Float16Type, Float32Type,
    Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt8Type, UInt16Type,
    UInt32Type,
};
use arrow_array::{Array, RecordBatch, downcast_dictionary_array};
use arrow_schema::{DataType, Field, TimeUnit};
use chrono::{DateTime, Datelike, NaiveDate, SecondsFormat, Utc};
use google_cloud_spanner::mutation::Mutation;
use google_cloud_spanner::statement::StatementBuilder;
use google_cloud_spanner::types::{self, Type};
use google_cloud_spanner::value::{ToValue, Value};

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
    let schema = batch.schema();
    for (i, name) in names.iter().enumerate() {
        builder = bind_one(
            builder,
            name,
            schema.field(i),
            batch.column(i).as_ref(),
            row,
        )?;
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
    let (value, param_type) = cell_value(name, field, column, row)?;
    let param_type = param_type.or_else(|| parameter_type(column.data_type()));
    Ok(match param_type {
        Some(t) => builder.add_typed_param(name, value, t),
        None => builder.add_param(name, value),
    })
}

/// Type an already-validated binding. Arrow `Null` (including list elements) needs SQL context.
/// Keep this separate from value encoding: mutation values take their type from the target table.
fn parameter_type(mut data_type: &DataType) -> Option<Type> {
    while let DataType::Dictionary(_, value) = data_type {
        data_type = value;
    }
    Some(match data_type {
        DataType::Boolean => types::bool(),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32 => types::int64(),
        DataType::Float16 | DataType::Float32 => types::float32(),
        DataType::Float64 => types::float64(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => types::string(),
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => types::bytes(),
        DataType::Date32 | DataType::Date64 => types::date(),
        DataType::Timestamp(_, _) => types::timestamp(),
        DataType::Decimal128(_, _) => types::numeric(),
        DataType::List(item) | DataType::LargeList(item) => {
            types::array(parameter_type(item.data_type())?)
        }
        _ => return None,
    })
}

/// Build an insert mutation using the same value encoding as query parameters.
/// Mutation values take their type from the target column; column names are unquoted.
pub(crate) fn insert_mutation(table: &str, batch: &RecordBatch, row: usize) -> Result<Mutation> {
    let mut builder = Mutation::new_insert_builder(table);
    for (i, field) in batch.schema().fields().iter().enumerate() {
        let (value, _param_type) = cell_value(field.name(), field, batch.column(i).as_ref(), row)?;
        builder = builder.set(field.name().clone()).to(value);
    }
    Ok(builder.build())
}

/// Run the mutation value conversion over `batch` and discard the values, so an ingest can reject
/// a bad value before its DDL runs. A column whose values always convert once its type is accepted
/// is checked on its first row only.
pub(crate) fn check_ingest_values(batch: &RecordBatch) -> Result<()> {
    let schema = batch.schema();
    for (i, field) in schema.fields().iter().enumerate() {
        let column = batch.column(i).as_ref();
        let rows = if values_always_convert(column.data_type()) {
            batch.num_rows().min(1)
        } else {
            batch.num_rows()
        };
        for row in 0..rows {
            cell_value(field.name(), field, column, row)?;
        }
    }
    Ok(())
}

/// Whether [`scalar_binder`] accepts every value of `data_type` once it accepts the type. Types
/// not listed here, such as dates and timestamps, are checked value by value.
fn values_always_convert(data_type: &DataType) -> bool {
    match data_type {
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
        | DataType::FixedSizeBinary(_)
        | DataType::Decimal128(_, _) => true,
        DataType::Dictionary(_, value) => values_always_convert(value),
        DataType::List(item) | DataType::LargeList(item) => values_always_convert(item.data_type()),
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
    name: &str,
    field: &Field,
    column: &dyn Array,
    row: usize,
) -> Result<(Value, Option<Type>)> {
    let data_type = column.data_type();
    match data_type {
        DataType::List(item) => {
            let list = column.as_list::<i32>();
            let offsets = list.value_offsets();
            let elem = (!column.is_null(row)).then(|| {
                (
                    list.values().as_ref(),
                    offsets[row] as usize..offsets[row + 1] as usize,
                )
            });
            list_cell_value(name, item, elem)
        }
        DataType::LargeList(item) => {
            let list = column.as_list::<i64>();
            let offsets = list.value_offsets();
            let elem = (!column.is_null(row)).then(|| {
                (
                    list.values().as_ref(),
                    offsets[row] as usize..offsets[row + 1] as usize,
                )
            });
            list_cell_value(name, item, elem)
        }
        // Decode recursively, validating the logical type for null keys too.
        DataType::Dictionary(_, _) => downcast_dictionary_array!(
            column => match column.key(row) {
                Some(value_row) => cell_value(name, field, column.values().as_ref(), value_row),
                None => null_dictionary_value(name, field, column.values().data_type()),
            },
            _ => unreachable!("downcast_dictionary_array dispatched a non-dictionary {data_type:?}")
        ),
        _ => {
            let bind = scalar_binder(name, data_type)?;
            let value = bind(name, data_type, column, row)?;
            Ok((value, is_json_field(field).then(types::json)))
        }
    }
}

/// Allocation-free scalar encoder. The data type must match the validated type supplied to
/// [`scalar_binder`]; arrays reuse the encoder across all their elements.
type ScalarBinder = fn(&str, &DataType, &dyn Array, usize) -> Result<Value>;

/// Select the shared scalar encoder, rejecting unsupported types and decimal scales even for
/// null values. Keep new type support aligned with [`spanner_column_type`] and the read mapping.
fn scalar_binder(name: &str, data_type: &DataType) -> Result<ScalarBinder> {
    let bind: ScalarBinder = match data_type {
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
        DataType::Boolean => {
            |_, _, a, i| Ok(scalar_value(a.is_null(i), || a.as_boolean().value(i)))
        }
        DataType::Utf8 => {
            |_, _, a, i| Ok(scalar_value(a.is_null(i), || a.as_string::<i32>().value(i)))
        }
        DataType::LargeUtf8 => {
            |_, _, a, i| Ok(scalar_value(a.is_null(i), || a.as_string::<i64>().value(i)))
        }
        DataType::Utf8View => {
            |_, _, a, i| Ok(scalar_value(a.is_null(i), || a.as_string_view().value(i)))
        }
        DataType::Binary => {
            |_, _, a, i| Ok(scalar_value(a.is_null(i), || a.as_binary::<i32>().value(i)))
        }
        DataType::LargeBinary => {
            |_, _, a, i| Ok(scalar_value(a.is_null(i), || a.as_binary::<i64>().value(i)))
        }
        DataType::BinaryView => {
            |_, _, a, i| Ok(scalar_value(a.is_null(i), || a.as_binary_view().value(i)))
        }
        DataType::FixedSizeBinary(_) => |_, _, a, i| {
            Ok(scalar_value(a.is_null(i), || {
                a.as_fixed_size_binary().value(i)
            }))
        },
        DataType::Date32 => |name, _, a, i| {
            try_scalar_value(a.is_null(i), || {
                date_string(name, a.as_primitive::<Date32Type>().value(i))
            })
        },
        // `Date64` is milliseconds since the Unix epoch, constrained by Arrow to whole days.
        DataType::Date64 => |name, _, a, i| {
            try_scalar_value(a.is_null(i), || {
                date_string(
                    name,
                    date64_days(name, a.as_primitive::<Date64Type>().value(i))?,
                )
            })
        },
        DataType::Timestamp(_, _) => |name, dt, a, i| {
            let DataType::Timestamp(unit, _) = dt else {
                unreachable!("scalar_binder dispatched a Timestamp arm on {dt:?}")
            };
            try_scalar_value(a.is_null(i), || {
                timestamp_string(name, unit, timestamp_value(a, unit, i))
            })
        },
        DataType::Decimal128(_, scale) => {
            // Validate once per array cell, including null and empty arrays.
            validate_numeric_scale(name, *scale)?;
            |_, dt, a, i| {
                let DataType::Decimal128(_, scale) = dt else {
                    unreachable!("scalar_binder dispatched a Decimal128 arm on {dt:?}")
                };
                Ok(scalar_value(a.is_null(i), || {
                    numeric_string(a.as_primitive::<Decimal128Type>().value(i), *scale)
                }))
            }
        }
        // NullArray has no validity buffer, so its physical `is_null` returns false.
        DataType::Null => |_, _, _, _| Ok(null_value()),
        _ => {
            return Err(invalid_argument(format!(
                "cannot bind parameter {name:?}: unsupported Arrow type {data_type:?}"
            )));
        }
    };
    Ok(bind)
}

/// Widen a primitive through `From`, preserving nulls and returning a plain function pointer.
fn primitive_binder<T, V>() -> ScalarBinder
where
    T: ArrowPrimitiveType,
    V: From<T::Native> + Into<Value>,
{
    |_, _, a, i| {
        Ok(scalar_value(a.is_null(i), || {
            V::from(a.as_primitive::<T>().value(i))
        }))
    }
}

/// Validate a null dictionary cell's logical type and retain its JSON parameter type.
fn null_dictionary_value(
    name: &str,
    field: &Field,
    mut value_type: &DataType,
) -> Result<(Value, Option<Type>)> {
    while let DataType::Dictionary(_, value) = value_type {
        value_type = value;
    }
    match value_type {
        DataType::List(item) | DataType::LargeList(item) => list_cell_value(name, item, None),
        _ => {
            scalar_binder(name, value_type)?;
            Ok((null_value(), is_json_field(field).then(types::json)))
        }
    }
}

/// The Spanner SQL `NULL` wire value.
fn null_value() -> Value {
    None::<bool>.to_value()
}

/// Encode a scalar or null. `Into<Value>` consumes owned strings; `ToValue` would copy them.
fn scalar_value<T: Into<Value>>(is_null: bool, value: impl FnOnce() -> T) -> Value {
    if is_null {
        null_value()
    } else {
        value().into()
    }
}

/// Like [`scalar_value`] but the conversion is fallible (the string-formatted temporal types).
fn try_scalar_value<T: Into<Value>>(
    is_null: bool,
    value: impl FnOnce() -> Result<T>,
) -> Result<Value> {
    Ok(if is_null {
        null_value()
    } else {
        value()?.into()
    })
}

/// Encode a list cell from its borrowed child range, avoiding a sliced array allocation.
/// `None` means a null array. Validate element types even for empty and null arrays.
fn list_cell_value(
    name: &str,
    item: &Field,
    elem: Option<(&dyn Array, Range<usize>)>,
) -> Result<(Value, Option<Type>)> {
    let item_type = item.data_type();
    let bind = scalar_binder(name, item_type)
        .map_err(|error| crate::error::annotate(error, |m| format!("ARRAY element: {m}")))?;
    let value = match elem {
        None => null_value(),
        // Consume the elements; `ToValue` would deep-copy them.
        Some((values, range)) => Value::from(
            range
                .map(|i| bind(name, item_type, values, i))
                .collect::<Result<Vec<Value>>>()?,
        ),
    };
    Ok((
        value,
        is_json_field(item).then(|| types::array(types::json())),
    ))
}

/// Convert Date64 milliseconds to Date32 days, rejecting non-day values and overflow.
/// Arrow constructors do not enforce Date64's whole-day requirement.
fn date64_days(name: &str, millis: i64) -> Result<i32> {
    if millis % 86_400_000 != 0 {
        return Err(invalid_argument(format!(
            "cannot bind DATE parameter {name:?}: Date64 value {millis}ms is not a whole number of days; use a timestamp type to retain time of day"
        )));
    }
    i32::try_from(millis / 86_400_000).map_err(|_| {
        invalid_argument(format!(
            "cannot bind DATE parameter {name:?}: {millis}ms is out of range"
        ))
    })
}

/// Format an Arrow `Date32` (days since the Unix epoch) as the Spanner `DATE` wire form,
/// `YYYY-MM-DD`. `name` is used only for the out-of-range error message.
fn date_string(name: &str, days: i32) -> Result<String> {
    // Chrono counts 0001-01-01 as day 1; 1970-01-01 is day 719_163.
    let date = days
        .checked_add(719_163)
        .and_then(NaiveDate::from_num_days_from_ce_opt)
        .filter(|date| (1..=9999).contains(&date.year()))
        .ok_or_else(|| {
            invalid_argument(format!(
                "cannot bind DATE parameter {name:?}: {days} days since 1970-01-01 is out of range; Spanner supports 0001-01-01 through 9999-12-31"
            ))
        })?;
    Ok(date.format("%Y-%m-%d").to_string())
}

/// Read the raw `i64` at `row` from an Arrow timestamp `column` of the given `unit`.
fn timestamp_value(column: &dyn Array, unit: &TimeUnit, row: usize) -> i64 {
    match unit {
        TimeUnit::Second => column.as_primitive::<TimestampSecondType>().value(row),
        TimeUnit::Millisecond => column.as_primitive::<TimestampMillisecondType>().value(row),
        TimeUnit::Microsecond => column.as_primitive::<TimestampMicrosecondType>().value(row),
        TimeUnit::Nanosecond => column.as_primitive::<TimestampNanosecondType>().value(row),
    }
}

/// Validate the Arrow Decimal128 scale. Negative scales represent integer multiples of ten;
/// Spanner checks whether the formatted value fits NUMERIC's precision and range.
fn validate_numeric_scale(name: &str, scale: i8) -> Result<()> {
    if scale > 38 {
        return Err(invalid_argument(format!(
            "cannot bind NUMERIC parameter {name:?}: unsupported scale {scale}"
        )));
    }
    Ok(())
}

/// Format a UTC timestamp at its source precision, rejecting years outside Spanner's range.
/// Negative subsecond values retain their exact instant (for example, -1 ns ends in .999999999Z).
fn timestamp_string(name: &str, unit: &TimeUnit, value: i64) -> Result<String> {
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
            "cannot bind TIMESTAMP parameter {name:?}: {value} ({unit:?}) is out of range; Spanner supports years 0001 through 9999"
        ))
    })?;
    Ok(ts.to_rfc3339_opts(format, true))
}

/// Format an unscaled `Decimal128` value at the given scale as a plain decimal string, exact across
/// the whole `i128` range (which covers Spanner's `NUMERIC`). `scale` must be `<= 38`.
fn numeric_string(unscaled: i128, scale: i8) -> String {
    if scale <= 0 {
        let mut value = unscaled.to_string();
        if unscaled != 0 && scale < 0 {
            // Append zeros instead of multiplying: the decimal value may exceed i128,
            // and even i8::MIN is a valid negative Arrow scale.
            value.extend(std::iter::repeat_n('0', usize::from(scale.unsigned_abs())));
        }
        return value;
    }
    let negative = unscaled < 0;
    let magnitude = unscaled.unsigned_abs();
    let divisor = 10u128.pow(scale as u32); // 0 < scale <= 38, so this fits in u128
    format!(
        "{}{}.{:0width$}",
        if negative { "-" } else { "" },
        magnitude / divisor,
        magnitude % divisor,
        width = scale as usize
    )
}

/// Map an ingest type to DDL, rejecting types the value encoder cannot bind before any DDL runs.
fn spanner_column_type(mut data_type: &DataType) -> Result<String> {
    while let DataType::Dictionary(_, value) = data_type {
        data_type = value;
    }
    Ok(match data_type {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => "INT64".to_string(),
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 => "INT64".to_string(),
        DataType::Float16 | DataType::Float32 => "FLOAT32".to_string(),
        DataType::Float64 => "FLOAT64".to_string(),
        DataType::Boolean => "BOOL".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "STRING(MAX)".to_string(),
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => "BYTES(MAX)".to_string(),
        DataType::Date32 | DataType::Date64 => "DATE".to_string(),
        DataType::Timestamp(_, _) => "TIMESTAMP".to_string(),
        DataType::Decimal128(_, scale) => {
            validate_numeric_scale("ingest column", *scale)?;
            "NUMERIC".to_string()
        }
        DataType::List(field) | DataType::LargeList(field) => {
            // List binding accepts scalar children only, including for null or empty lists.
            scalar_binder(field.name(), field.data_type())?;
            format!("ARRAY<{}>", spanner_field_type(field)?)
        }
        other => {
            return Err(invalid_argument(format!(
                "cannot create a Spanner column for Arrow type {other:?}; cast it to a type with \
                 a Spanner equivalent before ingesting"
            )));
        }
    })
}

/// Preserve JSON field and list-element metadata when choosing an ingest column type.
fn spanner_field_type(field: &Field) -> Result<String> {
    if is_json_field(field) {
        return Ok("JSON".to_string());
    }
    spanner_column_type(field.data_type())
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
        let column_type = spanner_field_type(field).map_err(|e| {
            crate::error::annotate(e, |m| format!("ingest column {:?}: {m}", field.name()))
        })?;
        columns.push(format!("{} {}", quote_ident(field.name())?, column_type));
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

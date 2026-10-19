//! Builds the [`Connection::get_info`](adbc_core::Connection::get_info) result. Every value is
//! static, so `get_info` makes no RPC.

use std::collections::HashSet;
use std::sync::Arc;

use adbc_core::error::Result;
use adbc_core::options::InfoCode;
use adbc_core::schemas::GET_INFO_SCHEMA;
use arrow_array::{ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray, UInt32Array};
use arrow_schema::DataType;

use crate::nested::{arrow_err, dense_union, shape_err};
use crate::{DRIVER_NAME, DRIVER_VERSION, VENDOR_NAME};

/// Type ids of the `info_value` union branches this driver populates; see [`GET_INFO_SCHEMA`].
const STRING_VALUE: i8 = 0;
const BOOL_VALUE: i8 = 1;
const INT64_VALUE: i8 = 2;

/// ADBC API 1.1.0, reported for `DriverAdbcVersion`.
const ADBC_VERSION_1_1_0: i64 = 1_001_000;

/// The `arrow-array` version from `Cargo.lock` (read by `build.rs`), reported for
/// `DriverArrowVersion`.
const ARROW_VERSION: &str = concat!("v", env!("SPANNER_ADBC_ARROW_VERSION"));

/// The codes `get_info(None)` returns, in code order; explicit requests are filtered to this set.
/// It includes codes with null values: `adbc.h` reserves omission for unrecognized codes.
const REPORTED: &[InfoCode] = &[
    InfoCode::VendorName,
    InfoCode::VendorVersion,
    InfoCode::VendorArrowVersion,
    InfoCode::VendorSql,
    InfoCode::VendorSubstrait,
    InfoCode::VendorSubstraitMinVersion,
    InfoCode::VendorSubstraitMaxVersion,
    InfoCode::DriverName,
    InfoCode::DriverVersion,
    InfoCode::DriverArrowVersion,
    InfoCode::DriverAdbcVersion,
];

/// A single info value, tagged by which union branch it belongs to.
enum InfoValue {
    Str(&'static str),
    Bool(bool),
    Int(i64),
    /// A recognised code without a stable value, emitted as a null string.
    Null,
}

/// The value this driver reports for `code`.
fn value_for(code: InfoCode) -> InfoValue {
    match code {
        InfoCode::VendorName => InfoValue::Str(VENDOR_NAME),
        // Spanner accepts SQL text, not Substrait plans.
        InfoCode::VendorSql => InfoValue::Bool(true),
        InfoCode::VendorSubstrait => InfoValue::Bool(false),
        InfoCode::DriverName => InfoValue::Str(DRIVER_NAME),
        InfoCode::DriverVersion => InfoValue::Str(DRIVER_VERSION),
        InfoCode::DriverArrowVersion => InfoValue::Str(ARROW_VERSION),
        InfoCode::DriverAdbcVersion => InfoValue::Int(ADBC_VERSION_1_1_0),
        // Spanner exposes no server version and runs no Arrow library. `adbc.h` defines null as
        // the Substrait version bounds when Substrait is unsupported.
        _ => InfoValue::Null,
    }
}

/// Build the `get_info` batch for `codes`, or for all of [`REPORTED`] when `None`.
pub(crate) fn build(codes: Option<HashSet<InfoCode>>) -> Result<RecordBatch> {
    let mut names: Vec<u32> = Vec::with_capacity(REPORTED.len());
    let mut type_ids: Vec<i8> = Vec::with_capacity(REPORTED.len());
    let mut offsets: Vec<i32> = Vec::with_capacity(REPORTED.len());
    let mut strings: Vec<Option<&'static str>> = Vec::new();
    let mut bools: Vec<Option<bool>> = Vec::new();
    let mut ints: Vec<Option<i64>> = Vec::new();

    // Iterating REPORTED keeps code order and drops unrecognized codes.
    for code in REPORTED.iter().copied().filter(|code| {
        codes
            .as_ref()
            .is_none_or(|requested| requested.contains(code))
    }) {
        names.push(u32::from(&code));
        let (type_id, offset) = match value_for(code) {
            InfoValue::Str(s) => {
                strings.push(Some(s));
                (STRING_VALUE, strings.len() - 1)
            }
            InfoValue::Null => {
                strings.push(None);
                (STRING_VALUE, strings.len() - 1)
            }
            InfoValue::Bool(b) => {
                bools.push(Some(b));
                (BOOL_VALUE, bools.len() - 1)
            }
            InfoValue::Int(i) => {
                ints.push(Some(i));
                (INT64_VALUE, ints.len() - 1)
            }
        };
        type_ids.push(type_id);
        offsets.push(offset as i32);
    }

    let union_fields = match GET_INFO_SCHEMA.field(1).data_type() {
        DataType::Union(fields, _) => fields.clone(),
        _ => return Err(shape_err("`info_value` to be a union")),
    };

    let string_child: ArrayRef = Arc::new(StringArray::from_iter(strings));
    let bool_child: ArrayRef = Arc::new(BooleanArray::from_iter(bools));
    let int_child: ArrayRef = Arc::new(Int64Array::from_iter(ints));
    let info_value = dense_union(
        &union_fields,
        &[
            (STRING_VALUE, string_child),
            (BOOL_VALUE, bool_child),
            (INT64_VALUE, int_child),
        ],
        type_ids,
        offsets,
    )?;

    RecordBatch::try_new(
        GET_INFO_SCHEMA.clone(),
        vec![Arc::new(UInt32Array::from(names)), Arc::new(info_value)],
    )
    .map_err(arrow_err)
}

#[cfg(test)]
mod tests;

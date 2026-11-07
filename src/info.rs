//! Builds the [`Connection::get_info`](adbc_core::Connection::get_info) result. Every value is
//! static, so `get_info` makes no RPC.

use std::collections::HashSet;
use std::sync::Arc;

use adbc_core::constants::ADBC_VERSION_1_1_0;
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

/// The `arrow-array` version from `Cargo.lock` (read by `build.rs`), reported for
/// `DriverArrowVersion`.
const ARROW_VERSION: &str = concat!("v", env!("SPANNER_ADBC_ARROW_VERSION"));

/// A single info value, tagged by which union branch it belongs to.
enum InfoValue {
    Str(Option<&'static str>),
    Bool(bool),
    Int(i64),
}

use InfoValue::{Bool, Int, Str};

/// What `get_info(None)` returns, in code order; explicit requests are filtered to this set. It
/// includes codes with null values: `adbc.h` reserves omission for unrecognized codes.
const REPORTED: &[(InfoCode, InfoValue)] = &[
    (InfoCode::VendorName, Str(Some(VENDOR_NAME))),
    // Spanner exposes no server version and runs no Arrow library.
    (InfoCode::VendorVersion, Str(None)),
    (InfoCode::VendorArrowVersion, Str(None)),
    // Spanner accepts SQL text, not Substrait plans.
    (InfoCode::VendorSql, Bool(true)),
    (InfoCode::VendorSubstrait, Bool(false)),
    // `adbc.h` defines null as the version bounds when Substrait is unsupported.
    (InfoCode::VendorSubstraitMinVersion, Str(None)),
    (InfoCode::VendorSubstraitMaxVersion, Str(None)),
    (InfoCode::DriverName, Str(Some(DRIVER_NAME))),
    (InfoCode::DriverVersion, Str(Some(DRIVER_VERSION))),
    (InfoCode::DriverArrowVersion, Str(Some(ARROW_VERSION))),
    (InfoCode::DriverAdbcVersion, Int(ADBC_VERSION_1_1_0 as i64)),
];

/// Build the `get_info` batch for `codes`, or for all of [`REPORTED`] when `None`.
pub(crate) fn build(codes: Option<HashSet<InfoCode>>) -> Result<RecordBatch> {
    let (mut names, mut type_ids, mut offsets) = (Vec::new(), Vec::new(), Vec::new());
    let (mut strings, mut bools, mut ints) = (Vec::new(), Vec::new(), Vec::new());

    // Iterating REPORTED keeps code order and drops unrecognized codes.
    for (code, value) in REPORTED {
        if codes.as_ref().is_some_and(|c| !c.contains(code)) {
            continue;
        }
        names.push(u32::from(code));
        let (type_id, len) = match *value {
            Str(s) => {
                strings.push(s);
                (STRING_VALUE, strings.len())
            }
            Bool(b) => {
                bools.push(b);
                (BOOL_VALUE, bools.len())
            }
            Int(i) => {
                ints.push(i);
                (INT64_VALUE, ints.len())
            }
        };
        type_ids.push(type_id);
        offsets.push(len as i32 - 1);
    }

    let DataType::Union(union_fields, _) = GET_INFO_SCHEMA.field(1).data_type() else {
        return Err(shape_err("`info_value` to be a union"));
    };
    let info_value = dense_union(
        union_fields,
        &[
            (
                STRING_VALUE,
                Arc::new(StringArray::from(strings)) as ArrayRef,
            ),
            (BOOL_VALUE, Arc::new(BooleanArray::from(bools))),
            (INT64_VALUE, Arc::new(Int64Array::from(ints))),
        ],
        type_ids,
        offsets,
    )?;
    RecordBatch::try_new(
        GET_INFO_SCHEMA.clone(),
        vec![Arc::new(UInt32Array::from(names)), info_value],
    )
    .map_err(arrow_err)
}

#[cfg(test)]
mod tests;

//! Helpers for building the nested list, struct and union batches returned by `get_objects`,
//! `get_statistics` and `get_info`.
//!
//! The result schemas are `adbc_core` constants, so shape lookups cannot fail in practice. They
//! still return `Status::Internal` errors because a panic must not unwind into the C ABI.

use std::sync::Arc;

use adbc_core::error::{Error, Result, Status};
use arrow_array::{ArrayRef, ListArray, UnionArray, new_empty_array};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{ArrowError, DataType, FieldRef, Fields, UnionFields};

use crate::error::err;

pub(crate) fn arrow_err(e: ArrowError) -> Error {
    err(
        format!("failed to build metadata result batch: {e}"),
        Status::Internal,
    )
}

pub(crate) fn shape_err(expected: &str) -> Error {
    err(
        format!("unexpected ADBC result schema shape: expected {expected}"),
        Status::Internal,
    )
}

pub(crate) fn field(fields: &Fields, name: &str) -> Result<FieldRef> {
    fields
        .find(name)
        .map(|(_, f)| f.clone())
        .ok_or_else(|| shape_err(&format!("field `{name}`")))
}

pub(crate) fn list_item(list_field: &FieldRef) -> Result<FieldRef> {
    match list_field.data_type() {
        DataType::List(item) => Ok(item.clone()),
        _ => Err(shape_err(&format!("`{}` to be a list", list_field.name()))),
    }
}

pub(crate) fn struct_fields(struct_field: &FieldRef) -> Result<Fields> {
    match struct_field.data_type() {
        DataType::Struct(fs) => Ok(fs.clone()),
        _ => Err(shape_err(&format!(
            "`{}` to be a struct",
            struct_field.name()
        ))),
    }
}

/// Group `child` into non-null lists of the given `lengths`.
pub(crate) fn list_of(item: FieldRef, lengths: &[usize], child: ArrayRef) -> Result<ArrayRef> {
    list_of_nullable(item, lengths, child, None)
}

/// Like [`list_of`], with `nulls` marking null entries. A null entry's length must be 0.
pub(crate) fn list_of_nullable(
    item: FieldRef,
    lengths: &[usize],
    child: ArrayRef,
    nulls: Option<NullBuffer>,
) -> Result<ArrayRef> {
    let mut offsets = Vec::with_capacity(lengths.len() + 1);
    offsets.push(0i32);
    let mut acc = 0i32;
    for len in lengths {
        acc = i32::try_from(*len)
            .ok()
            .and_then(|len| acc.checked_add(len))
            .ok_or_else(|| {
                err(
                    "metadata result list exceeds the i32 offset range",
                    Status::Internal,
                )
            })?;
        offsets.push(acc);
    }
    let list = ListArray::try_new(
        item,
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        child,
        nulls,
    )
    .map_err(arrow_err)?;
    Ok(Arc::new(list))
}

/// Assemble a dense union of `fields`. `populated` supplies the children in use by type id; the
/// other branches get empty arrays so the union type still matches the schema.
pub(crate) fn dense_union(
    fields: &UnionFields,
    populated: &[(i8, ArrayRef)],
    type_ids: Vec<i8>,
    offsets: Vec<i32>,
) -> Result<ArrayRef> {
    let children: Vec<ArrayRef> = fields
        .iter()
        .map(|(id, f)| {
            populated
                .iter()
                .find(|(pid, _)| *pid == id)
                .map_or_else(|| new_empty_array(f.data_type()), |(_, arr)| arr.clone())
        })
        .collect();
    let union = UnionArray::try_new(
        fields.clone(),
        ScalarBuffer::from(type_ids),
        Some(ScalarBuffer::from(offsets)),
        children,
    )
    .map_err(arrow_err)?;
    Ok(Arc::new(union))
}

#[cfg(test)]
mod tests;

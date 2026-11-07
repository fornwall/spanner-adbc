use std::ffi::{c_char, c_int};

use adbc_core::error::Status;
use arrow_array::ffi::to_ffi;
use arrow_array::{ArrayRef, Int64Array, RecordBatchIterator};

use super::*;
use crate::ffi::test_support::null;

#[test]
fn imported_batches_preserve_rows_and_buffers_after_inputs_are_released() {
    use arrow_schema::Field;

    for fields in [vec![], vec![Field::new("value", DataType::Int64, true)]] {
        let columns = if fields.is_empty() {
            vec![]
        } else {
            vec![Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])) as ArrayRef]
        };
        let expected = RecordBatch::try_new_with_options(
            Arc::new(Schema::new(fields)),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(3)),
        )
        .unwrap();
        let (mut values, mut schema) =
            to_ffi(&StructArray::from(expected.clone()).to_data()).unwrap();
        let batch = unsafe { import_batch(&raw mut values, &raw mut schema) }.unwrap();
        assert!(values.release().is_none());
        assert!(schema.release().is_none());
        drop(values);
        drop(schema);
        assert_eq!(batch, expected);
        assert_eq!(batch.num_rows(), 3);
    }
}

#[test]
fn imported_batches_reject_non_structs_and_top_level_null_rows() {
    use arrow_schema::Field;

    for (data, expected) in [
        (
            Int64Array::from(vec![1]).to_data(),
            "must be a struct array",
        ),
        (
            StructArray::new_null(vec![Field::new("value", DataType::Int64, true)].into(), 1)
                .to_data(),
            "top-level null rows",
        ),
    ] {
        let (mut values, mut schema) = to_ffi(&data).unwrap();
        let failure = unsafe { import_batch(&raw mut values, &raw mut schema) }.unwrap_err();
        assert_eq!(failure.status, Status::InvalidArguments);
        assert!(failure.message.contains(expected), "{failure}");
        assert!(values.release().is_none());
        assert!(schema.release().is_none());
    }
}

type GetSchema = unsafe extern "C" fn(*mut FFI_ArrowArrayStream, *mut FFI_ArrowSchema) -> c_int;
type GetNext = unsafe extern "C" fn(*mut FFI_ArrowArrayStream, *mut FFI_ArrowArray) -> c_int;
type GetLastError = unsafe extern "C" fn(*mut FFI_ArrowArrayStream) -> *const c_char;

/// Rebuild `stream` with the given callbacks swapped in, keeping its private data and release.
fn replace_callbacks(
    stream: FFI_ArrowArrayStream,
    get_schema: Option<GetSchema>,
    get_next: Option<GetNext>,
    get_last_error: Option<GetLastError>,
) -> FFI_ArrowArrayStream {
    // The rebuilt stream takes over the private data.
    let stream = std::mem::ManuallyDrop::new(stream);
    // SAFETY: the replacements only ignore or write their outputs; the retained release still
    // matches the retained private data.
    unsafe {
        FFI_ArrowArrayStream::new_unchecked(
            get_schema.or(stream.get_schema()),
            get_next.or(stream.get_next()),
            get_last_error.or(stream.get_last_error()),
            stream.release(),
            stream.private_data(),
        )
    }
}

#[test]
fn bound_stream_schema_failures_preserve_diagnostics_and_release_ownership() {
    unsafe extern "C" fn failed_schema(
        _: *mut FFI_ArrowArrayStream,
        _: *mut FFI_ArrowSchema,
    ) -> std::ffi::c_int {
        libc::EIO
    }
    unsafe extern "C" fn description(_: *mut FFI_ArrowArrayStream) -> *const c_char {
        c"producer schema unavailable".as_ptr()
    }
    unsafe extern "C" fn no_description(_: *mut FFI_ArrowArrayStream) -> *const c_char {
        null()
    }
    unsafe extern "C" fn released_schema(
        _: *mut FFI_ArrowArrayStream,
        _: *mut FFI_ArrowSchema,
    ) -> std::ffi::c_int {
        0
    }

    for (get_schema, get_last_error, expected) in [
        (
            failed_schema as unsafe extern "C" fn(_, _) -> _,
            description as unsafe extern "C" fn(_) -> _,
            "producer schema unavailable",
        ),
        (failed_schema, no_description, "no error description"),
        (released_schema, no_description, "released schema"),
    ] {
        let lifetime = Arc::new(());
        let retained = lifetime.clone();
        let schema = Arc::new(Schema::empty());
        let batch = RecordBatch::new_empty(schema.clone());
        let stream = FFI_ArrowArrayStream::new(Box::new(RecordBatchIterator::new(
            std::iter::once_with(move || {
                drop(retained);
                Ok(batch)
            }),
            schema,
        )));
        let mut stream = replace_callbacks(stream, Some(get_schema), None, Some(get_last_error));
        assert_eq!(Arc::strong_count(&lifetime), 2);
        let failure = unsafe { BoundStreamReader::from_raw(&raw mut stream) }.unwrap_err();
        assert_eq!(failure.status, Status::InvalidArguments);
        assert!(failure.message.contains(expected), "{}", failure.message);
        if expected != "released schema" {
            assert!(failure.message.contains(&libc::EIO.to_string()));
        }
        assert!(stream.release().is_none());
        assert_eq!(Arc::strong_count(&lifetime), 1);
    }
}

#[test]
fn imported_stream_batches_preserve_schema_rows_and_buffers_after_release() {
    use arrow_array::{RecordBatchOptions, RecordBatchReader};
    use arrow_schema::{DataType, Field};

    for fields in [vec![], vec![Field::new("value", DataType::Int64, true)]] {
        let schema = Arc::new(
            Schema::new(fields).with_metadata(std::collections::HashMap::from([(
                "source".to_owned(),
                "caller".to_owned(),
            )])),
        );
        let columns = if schema.fields().is_empty() {
            vec![]
        } else {
            vec![Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])) as ArrayRef]
        };
        let batch = RecordBatch::try_new_with_options(
            schema.clone(),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(3)),
        )
        .unwrap();
        let mut stream = FFI_ArrowArrayStream::new(Box::new(RecordBatchIterator::new(
            [
                Ok(RecordBatch::new_empty(schema.clone())),
                Ok(batch.clone()),
            ],
            schema.clone(),
        )));
        let mut reader = unsafe { BoundStreamReader::from_raw(&raw mut stream) }.unwrap();
        assert!(stream.release().is_none());
        assert_eq!(reader.schema(), schema);
        assert_eq!(reader.next().unwrap().unwrap().num_rows(), 0);
        let found = reader.next().unwrap().unwrap();
        assert_eq!(found, batch);
        drop(batch);
        assert!(reader.next().is_none());
        assert!(reader.next().is_none());
        drop(reader);
        assert_eq!(found.num_rows(), 3);
        assert_eq!(found.schema(), schema);
        if !found.columns().is_empty() {
            let values = found
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(values, &Int64Array::from(vec![Some(1), None, Some(3)]));
        }
    }
}

#[test]
fn imported_streams_reject_null_rows_and_mismatched_column_counts() {
    use arrow_schema::{DataType, Field};

    unsafe extern "C" fn null_row(
        _: *mut FFI_ArrowArrayStream,
        out: *mut FFI_ArrowArray,
    ) -> std::ffi::c_int {
        let array =
            StructArray::new_null(vec![Field::new("value", DataType::Int64, true)].into(), 1);
        let (array, _) = to_ffi(&array.to_data()).unwrap();
        unsafe { std::ptr::write(out, array) };
        0
    }

    for (fields, expected) in [
        (vec![], "returned 1 columns, but its schema declares 0"),
        (
            vec![Field::new("value", DataType::Int64, true)],
            "top-level null rows",
        ),
    ] {
        let stream = FFI_ArrowArrayStream::new(Box::new(RecordBatchIterator::new(
            std::iter::empty(),
            Arc::new(Schema::new(fields)),
        )));
        let mut stream = replace_callbacks(stream, None, Some(null_row), None);
        let mut reader = unsafe { BoundStreamReader::from_raw(&raw mut stream) }.unwrap();
        let failure = reader.next().unwrap().unwrap_err();
        assert!(failure.to_string().contains(expected), "{failure}");
        assert!(reader.next().is_none());
    }
}

/// A struct with one non-UTF-8 value, long enough to be stored out of line from its view.
fn out_of_line_non_utf8_struct() -> arrow_data::ArrayData {
    use arrow_array::BinaryViewArray;
    use arrow_schema::{DataType, Field};

    let values = BinaryViewArray::from_iter_values([[0xff_u8; 16].as_slice()]);
    StructArray::new(
        vec![Field::new("payload", DataType::BinaryView, false)].into(),
        vec![Arc::new(values) as ArrayRef],
        None,
    )
    .to_data()
}

/// Arrow's C importer skips validation, while safe accessors such as `StringViewArray::value`
/// assume valid data, so a malformed import would be undefined behavior downstream.
///
/// An out-of-bounds view cannot be built through a safe API, so this sends `BinaryView` data
/// under a `Utf8View` schema instead: the layouts are identical and only the UTF-8 invariant is
/// broken.
#[test]
fn imported_arrays_are_validated_and_name_the_column_that_fails() {
    use arrow_schema::{DataType, Field};

    unsafe extern "C" fn non_utf8_view(
        _: *mut FFI_ArrowArrayStream,
        out: *mut FFI_ArrowArray,
    ) -> std::ffi::c_int {
        let (array, _) = to_ffi(&out_of_line_non_utf8_struct()).unwrap();
        unsafe { std::ptr::write(out, array) };
        0
    }

    let declared = vec![Field::new("payload", DataType::Utf8View, false)];
    let (mut values, _) = to_ffi(&out_of_line_non_utf8_struct()).unwrap();
    let mut schema = FFI_ArrowSchema::try_from(&DataType::Struct(declared.clone().into())).unwrap();
    let failure = unsafe { import_batch(&raw mut values, &raw mut schema) }.unwrap_err();
    assert_eq!(failure.status, Status::InvalidArguments);
    assert!(
        failure.message.contains(r#"column "payload" is invalid"#),
        "{failure}"
    );
    assert!(failure.message.contains("non-UTF-8"), "{failure}");
    assert!(values.release().is_none());
    assert!(schema.release().is_none());

    let stream = FFI_ArrowArrayStream::new(Box::new(RecordBatchIterator::new(
        std::iter::empty(),
        Arc::new(Schema::new(declared)),
    )));
    let mut stream = replace_callbacks(stream, None, Some(non_utf8_view), None);
    let mut reader = unsafe { BoundStreamReader::from_raw(&raw mut stream) }.unwrap();
    let failure = reader.next().unwrap().unwrap_err().to_string();
    assert!(
        failure.contains(r#"column "payload" is invalid"#),
        "{failure}"
    );
    assert!(failure.contains("non-UTF-8"), "{failure}");
    assert!(reader.next().is_none());
}

/// Arrow's importer asserts that a struct's children match its fields, at every nesting level.
/// The top-level count is checked before import, but a nested mismatch reaches the importer.
#[test]
fn a_nested_column_that_disagrees_with_the_bound_schema_is_an_error_not_a_panic() {
    use arrow_array::RecordBatchReader;
    use arrow_schema::{DataType, Field, Fields};

    let x = Field::new("x", DataType::Int64, false);
    let y = Field::new("y", DataType::Int64, false);
    let declared = Arc::new(Schema::new(vec![Field::new(
        "a",
        DataType::Struct(Fields::from(vec![x.clone()])),
        false,
    )]));
    let wider = StructArray::new(
        Fields::from(vec![x, y]),
        vec![
            Arc::new(Int64Array::from(vec![1])) as ArrayRef,
            Arc::new(Int64Array::from(vec![2])) as ArrayRef,
        ],
        None,
    );
    let actual = Arc::new(Schema::new(vec![Field::new(
        "a",
        wider.data_type().clone(),
        false,
    )]));
    let batch = RecordBatch::try_new(actual, vec![Arc::new(wider) as ArrayRef]).unwrap();
    // `RecordBatchIterator` does not check its batches against the schema it reports.
    let mut stream = FFI_ArrowArrayStream::new(Box::new(RecordBatchIterator::new(
        [Ok(batch)],
        declared.clone(),
    )));
    let mut reader = unsafe { BoundStreamReader::from_raw(&raw mut stream) }.unwrap();
    assert_eq!(reader.schema(), declared);
    let failure = reader.next().unwrap().unwrap_err().to_string();
    assert!(failure.contains("invalid Arrow array"), "{failure}");
    assert!(reader.next().is_none());
}

/// Validation can panic as well as import: Arrow expects a fixed-size list's value count to fit in
/// a `usize`. The panic must not reach `with_state`, where it would poison the statement.
#[test]
fn a_panic_during_validation_is_an_error() {
    use arrow_schema::Field;

    let rows = 1 << 40;
    let item = Arc::new(Field::new_list_field(DataType::Int64, true));
    let list_type = DataType::FixedSizeList(item, i32::MAX);
    let builder = ArrayData::builder(list_type.clone())
        .len(rows)
        .child_data(vec![Int64Array::from(vec![1]).to_data()]);
    let list = unsafe { builder.build_unchecked() };
    let fields = vec![Field::new("list", list_type, true)];
    let builder = ArrayData::builder(DataType::Struct(fields.into()))
        .len(rows)
        .child_data(vec![list]);
    let (mut values, mut schema) = to_ffi(&unsafe { builder.build_unchecked() }).unwrap();

    let failure = unsafe { import_batch(&raw mut values, &raw mut schema) }.unwrap_err();
    assert!(failure.message.contains("integer overflow"), "{failure}");
}

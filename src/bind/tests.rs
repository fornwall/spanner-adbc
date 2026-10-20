use std::sync::Arc;

use adbc_core::error::Status;
use arrow_array::builder::{ListBuilder, StringBuilder};
use arrow_array::{
    ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Date64Array,
    Decimal128Array, DictionaryArray, FixedSizeBinaryArray, FixedSizeListArray, Float16Array,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray,
    LargeListArray, LargeStringArray, ListArray, NullArray, StringArray, StringViewArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{Field, Schema};
use google_cloud_spanner::statement::Statement;

use super::*;

fn batch(fields: Vec<Field>, columns: Vec<ArrayRef>) -> RecordBatch {
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

/// A one-column batch named `p`.
fn column_batch(array: ArrayRef) -> RecordBatch {
    batch(
        vec![Field::new("p", array.data_type().clone(), true)],
        vec![array],
    )
}

/// The value and parameter type that binding `array[row]` as `@p` sends.
fn param(array: &dyn Array, row: usize) -> (Value, Option<Type>) {
    let field = Field::new("p", array.data_type().clone(), true);
    let (value, json_type) = cell_value("p", &field, array, row).unwrap();
    (
        value,
        json_type.or_else(|| parameter_type(array.data_type())),
    )
}

/// Bind every column of `batch` at `row` as `@p0`, `@p1`, ...
fn bind_row(batch: &RecordBatch, row: usize) -> Result<StatementBuilder> {
    let names: Vec<String> = (0..batch.num_columns()).map(|i| format!("p{i}")).collect();
    bind_params(Statement::builder("SELECT 1"), &names, batch, row)
}

/// A nullable field tagged with the `arrow.json` extension, as the read path produces for JSON
/// columns.
fn json_field(name: &str, data_type: DataType) -> Field {
    Field::new(name, data_type, true).with_metadata(std::collections::HashMap::from([(
        "ARROW:extension:name".to_string(),
        "arrow.json".to_string(),
    )]))
}

fn int_batch(names: &[&str]) -> RecordBatch {
    batch(
        names
            .iter()
            .map(|n| Field::new(*n, DataType::Int64, false))
            .collect(),
        names
            .iter()
            .map(|_| Arc::new(Int64Array::from(vec![1])) as ArrayRef)
            .collect(),
    )
}

#[test]
fn scalars_bind_with_their_spanner_types() {
    let f16 = <Float16Type as ArrowPrimitiveType>::Native::from_f32;
    let micros = 1_705_322_096_789_012_i64;
    let cases: Vec<(ArrayRef, Value, Type)> = vec![
        (
            Arc::new(BooleanArray::from(vec![Some(true), None])),
            Value::from(true),
            types::bool(),
        ),
        // Integers widen to INT64, which travels as a decimal string.
        (
            Arc::new(Int8Array::from(vec![Some(i8::MIN), None])),
            Value::from("-128"),
            types::int64(),
        ),
        (
            Arc::new(Int16Array::from(vec![Some(i16::MIN), None])),
            Value::from("-32768"),
            types::int64(),
        ),
        (
            Arc::new(Int32Array::from(vec![Some(i32::MIN), None])),
            Value::from("-2147483648"),
            types::int64(),
        ),
        (
            Arc::new(Int64Array::from(vec![Some(i64::MIN), None])),
            Value::from("-9223372036854775808"),
            types::int64(),
        ),
        (
            Arc::new(UInt8Array::from(vec![Some(u8::MAX), None])),
            Value::from("255"),
            types::int64(),
        ),
        (
            Arc::new(UInt16Array::from(vec![Some(u16::MAX), None])),
            Value::from("65535"),
            types::int64(),
        ),
        (
            Arc::new(UInt32Array::from(vec![Some(u32::MAX), None])),
            Value::from("4294967295"),
            types::int64(),
        ),
        // Narrow floats widen to f64 exactly.
        (
            Arc::new(Float16Array::from(vec![Some(f16(-1.5)), None])),
            Value::from(-1.5),
            types::float32(),
        ),
        (
            Arc::new(Float32Array::from(vec![Some(0.1), None])),
            Value::from(f64::from(0.1_f32)),
            types::float32(),
        ),
        (
            Arc::new(Float64Array::from(vec![Some(f64::NAN), None])),
            Value::from("NaN"),
            types::float64(),
        ),
        (
            Arc::new(StringArray::from(vec![Some("hello"), None])),
            Value::from("hello"),
            types::string(),
        ),
        (
            Arc::new(LargeStringArray::from(vec![Some("hello"), None])),
            Value::from("hello"),
            types::string(),
        ),
        (
            Arc::new(StringViewArray::from(vec![
                Some("longer than a view's twelve inline bytes"),
                None,
            ])),
            Value::from("longer than a view's twelve inline bytes"),
            types::string(),
        ),
        // BYTES travel as base64: "abcd" is "YWJjZA==".
        (
            Arc::new(BinaryArray::from(vec![Some(b"abcd".as_ref()), None])),
            Value::from("YWJjZA=="),
            types::bytes(),
        ),
        (
            Arc::new(LargeBinaryArray::from(vec![Some(b"abcd".as_ref()), None])),
            Value::from("YWJjZA=="),
            types::bytes(),
        ),
        (
            Arc::new(BinaryViewArray::from(vec![Some(b"abcd".as_ref()), None])),
            Value::from("YWJjZA=="),
            types::bytes(),
        ),
        (
            Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    [Some(b"abcd"), None].into_iter(),
                    4,
                )
                .unwrap(),
            ),
            Value::from("YWJjZA=="),
            types::bytes(),
        ),
        (
            Arc::new(Date32Array::from(vec![Some(19_737), None])),
            Value::from("2024-01-15"),
            types::date(),
        ),
        (
            Arc::new(Date64Array::from(vec![Some(19_737 * 86_400_000), None])),
            Value::from("2024-01-15"),
            types::date(),
        ),
        // Each timestamp unit keeps its own precision.
        (
            Arc::new(TimestampSecondArray::from(vec![
                Some(micros / 1_000_000),
                None,
            ])),
            Value::from("2024-01-15T12:34:56Z"),
            types::timestamp(),
        ),
        (
            Arc::new(TimestampMillisecondArray::from(vec![
                Some(micros / 1_000),
                None,
            ])),
            Value::from("2024-01-15T12:34:56.789Z"),
            types::timestamp(),
        ),
        (
            Arc::new(TimestampMicrosecondArray::from(vec![Some(micros), None])),
            Value::from("2024-01-15T12:34:56.789012Z"),
            types::timestamp(),
        ),
        (
            Arc::new(
                TimestampNanosecondArray::from(vec![Some(micros * 1_000 + 999), None])
                    .with_timezone("+02:00"),
            ),
            Value::from("2024-01-15T12:34:56.789012999Z"),
            types::timestamp(),
        ),
        (
            Arc::new(
                Decimal128Array::from(vec![Some(1_500_000_000), None])
                    .with_precision_and_scale(38, 9)
                    .unwrap(),
            ),
            Value::from("1.500000000"),
            types::numeric(),
        ),
        (
            Arc::new(
                Decimal128Array::from(vec![Some(-99), None])
                    .with_precision_and_scale(38, -2)
                    .unwrap(),
            ),
            Value::from("-9900"),
            types::numeric(),
        ),
    ];
    for (array, value, spanner_type) in cases {
        let data_type = array.data_type();
        assert_eq!(
            param(array.as_ref(), 0),
            (value, Some(spanner_type.clone())),
            "{data_type}"
        );
        // A null keeps the column's type.
        assert_eq!(
            param(array.as_ref(), 1),
            (Value::null(), Some(spanner_type)),
            "{data_type}"
        );
    }
    // Arrow `Null` leaves the type to the SQL.
    assert_eq!(param(&NullArray::new(1), 0), (Value::null(), None));
}

#[test]
fn numeric_string_formats_any_scale_exactly() {
    assert_eq!(numeric_string(-2_250_000_000, 9), "-2.250000000");
    assert_eq!(numeric_string(1, 9), "0.000000001");
    assert_eq!(numeric_string(-1, 9), "-0.000000001");
    assert_eq!(numeric_string(-7, 0), "-7");
    // 10^30 overflows a 96-bit decimal mantissa but is a valid NUMERIC.
    assert_eq!(
        numeric_string(10i128.pow(30), 9),
        "1000000000000000000000.000000000"
    );
    // Negative scales append zeros instead of multiplying, which could overflow.
    assert_eq!(numeric_string(0, i8::MIN), "0");
    assert_eq!(numeric_string(1, i8::MIN), format!("1{}", "0".repeat(128)));
    assert_eq!(
        numeric_string(i128::MIN, -1),
        "-1701411834604692317316873037158841057280"
    );
}

#[test]
fn timestamp_string_keeps_negative_subsecond_instants() {
    // One nanosecond before the epoch, not truncated toward zero.
    assert_eq!(
        timestamp_string("t", TimeUnit::Nanosecond, -1).unwrap(),
        "1969-12-31T23:59:59.999999999Z"
    );
    assert_eq!(
        timestamp_string("t", TimeUnit::Millisecond, -1).unwrap(),
        "1969-12-31T23:59:59.999Z"
    );
}

#[test]
fn temporal_binding_accepts_spanner_range_boundaries() {
    for date in ["0001-01-01", "9999-12-31"] {
        let days = crate::conversion::parse_date_days(date).unwrap();
        assert_eq!(date_string("d", days.into()).unwrap(), date);
    }
    for (unit, multiplier, fraction) in [
        (TimeUnit::Second, 1, ""),
        (TimeUnit::Millisecond, 1_000, ".999"),
        (TimeUnit::Microsecond, 1_000_000, ".999999"),
    ] {
        assert_eq!(
            timestamp_string("t", unit, -62_135_596_800 * multiplier).unwrap(),
            match unit {
                TimeUnit::Second => "0001-01-01T00:00:00Z",
                TimeUnit::Millisecond => "0001-01-01T00:00:00.000Z",
                _ => "0001-01-01T00:00:00.000000Z",
            }
        );
        assert_eq!(
            timestamp_string("t", unit, 253_402_300_800 * multiplier - 1).unwrap(),
            format!("9999-12-31T23:59:59{fraction}Z")
        );
    }
    for value in [i64::MIN, i64::MAX] {
        assert!(timestamp_string("t", TimeUnit::Nanosecond, value).is_ok());
    }
}

#[test]
fn temporal_binding_rejects_values_spanner_cannot_store() {
    let day = 86_400_000;
    let out_of_range: Vec<ArrayRef> = vec![
        Arc::new(Date32Array::from(vec![-719_163, 2_932_897])),
        Arc::new(Date64Array::from(vec![-719_163 * day, 2_932_897 * day])),
        // Beyond the Date32 range.
        Arc::new(Date64Array::from(vec![
            i64::MIN / day * day,
            i64::MAX / day * day,
        ])),
        Arc::new(TimestampSecondArray::from(vec![
            -62_135_596_801,
            253_402_300_800,
        ])),
        Arc::new(TimestampMillisecondArray::from(vec![
            -62_135_596_800_001,
            253_402_300_800_000,
        ])),
        Arc::new(TimestampMicrosecondArray::from(vec![
            -62_135_596_800_000_001,
            253_402_300_800_000_000,
        ])),
        Arc::new(ListArray::from_iter_primitive::<Date32Type, _, _>([
            Some(vec![Some(-719_163)]),
            Some(vec![Some(2_932_897)]),
        ])),
    ];
    // Arrow requires whole days in a Date64, but its constructors don't check.
    let time_of_day: Vec<ArrayRef> = vec![
        Arc::new(Date64Array::from(vec![1, -1])),
        Arc::new(ListArray::from_iter_primitive::<Date64Type, _, _>([
            Some(vec![Some(1)]),
            Some(vec![Some(-1)]),
        ])),
    ];
    for (arrays, reason) in [
        (out_of_range, "is out of range"),
        (time_of_day, "is not a whole number of days"),
    ] {
        for array in arrays {
            let b = batch(
                vec![Field::new("created_at", array.data_type().clone(), false)],
                vec![array],
            );
            for row in 0..2 {
                let error = bind_params(
                    Statement::builder("SELECT @created_at"),
                    &["created_at".into()],
                    &b,
                    row,
                )
                .unwrap_err();
                assert_eq!(error.status, Status::InvalidArguments);
                assert!(error.message.contains("\"created_at\""), "{error}");
                assert!(error.message.contains(reason), "{error}");
                assert_eq!(
                    insert_mutation("events", &b, row).unwrap_err().message,
                    error.message
                );
            }
        }
    }
}

#[test]
fn lists_bind_as_arrays_of_their_element_type() {
    let int = |v: i64| Value::from(v);
    let lists: Vec<ArrayRef> = vec![
        Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>([
            Some(vec![Some(1), None, Some(3)]),
            None,
            Some(vec![]),
        ])),
        Arc::new(LargeListArray::from_iter_primitive::<Int32Type, _, _>([
            Some(vec![Some(1), None, Some(3)]),
            None,
            Some(vec![]),
        ])),
    ];
    for list in lists {
        let data_type = list.data_type();
        let array_type = Some(types::array(types::int64()));
        assert_eq!(
            param(list.as_ref(), 0),
            (
                Value::from(vec![int(1), Value::null(), int(3)]),
                array_type.clone()
            ),
            "{data_type}"
        );
        // A null array and an empty array stay distinct.
        assert_eq!(
            param(list.as_ref(), 1),
            (Value::null(), array_type.clone()),
            "{data_type}"
        );
        assert_eq!(
            param(list.as_ref(), 2),
            (Value::from(Vec::<Value>::new()), array_type),
            "{data_type}"
        );
    }
    // Fixed-size lists, such as embeddings, bind the same way.
    let embeddings = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
        [
            Some(vec![Some(0.5), Some(1.5)]),
            None,
            Some(vec![Some(2.5), None]),
        ],
        2,
    );
    let array_type = Some(types::array(types::float32()));
    assert_eq!(
        param(&embeddings, 0),
        (
            Value::from(vec![Value::from(0.5), Value::from(1.5)]),
            array_type.clone()
        )
    );
    assert_eq!(param(&embeddings, 1), (Value::null(), array_type.clone()));
    assert_eq!(
        param(&embeddings.slice(2, 1), 0),
        (
            Value::from(vec![Value::from(2.5), Value::null()]),
            array_type
        )
    );
}

#[test]
fn binds_cells_of_sliced_arrays_at_their_offset() {
    // Slices are views into shared buffers, so row `i` must be read through the view. The ADBC
    // PostgreSQL driver had this bug for sliced lists (apache/arrow-adbc#4320).
    let ints: ArrayRef = Arc::new(Int64Array::from(vec![Some(10), Some(20), None]));
    let strings: ArrayRef = Arc::new(StringArray::from(vec![Some("alpha"), Some("beta"), None]));
    let dictionary: ArrayRef = Arc::new(
        vec![Some("alpha"), Some("beta"), None]
            .into_iter()
            .collect::<DictionaryArray<Int32Type>>(),
    );
    for (array, second) in [(ints, "20"), (strings, "beta"), (dictionary, "beta")] {
        let sliced = array.slice(1, 2);
        assert_eq!(param(sliced.as_ref(), 0).0, Value::from(second));
        assert_eq!(param(sliced.as_ref(), 1).0, Value::null());
    }

    // Both the list and its child values start at nonzero offsets.
    let values = Int64Array::from(vec![Some(99), Some(1), Some(2), None, Some(4)]).slice(1, 4);
    let lists = LargeListArray::new(
        Arc::new(Field::new("item", DataType::Int64, true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0_i64, 1, 4, 4, 4])),
        Arc::new(values),
        Some(NullBuffer::from(vec![true, true, true, false])),
    )
    .slice(1, 3);
    assert_eq!(
        param(&lists, 0).0,
        Value::from(vec![Value::from(2_i64), Value::null(), Value::from(4_i64)])
    );
    assert_eq!(param(&lists, 1).0, Value::from(Vec::<Value>::new()));
    assert_eq!(param(&lists, 2).0, Value::null());
}

#[test]
fn dictionary_cells_bind_as_their_values() {
    // A valid key can select a null value. The ADBC PostgreSQL driver once mishandled this
    // ("insufficient data left in message").
    let strings = DictionaryArray::new(
        Int8Array::from(vec![Some(1), None, Some(0)]),
        Arc::new(StringArray::from(vec![None, Some("a")])),
    );
    assert_eq!(
        param(&strings, 0),
        (Value::from("a"), Some(types::string()))
    );
    assert_eq!(param(&strings, 1), (Value::null(), Some(types::string())));
    assert_eq!(param(&strings, 2), (Value::null(), Some(types::string())));
    // Values keep their own widening.
    let ints = DictionaryArray::new(
        Int8Array::from(vec![Some(1)]),
        Arc::new(Int32Array::from(vec![7, 9])),
    );
    assert_eq!(param(&ints, 0), (Value::from("9"), Some(types::int64())));
    // A null key on a dictionary of lists binds a null array.
    let lists = DictionaryArray::new(
        Int8Array::from(vec![Some(0), None]),
        Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>([Some(
            vec![Some(1)],
        )])),
    );
    let array_type = Some(types::array(types::int64()));
    assert_eq!(
        param(&lists, 0),
        (Value::from(vec![Value::from(1_i64)]), array_type.clone())
    );
    assert_eq!(param(&lists, 1), (Value::null(), array_type));
    // Mutations decode them too.
    let dbg = format!(
        "{:?}",
        insert_mutation("t", &column_batch(Arc::new(strings)), 0).unwrap()
    );
    assert!(dbg.contains(r#"StringValue("a")"#), "{dbg}");
}

#[test]
fn rejects_unsupported_types_naming_the_parameter_and_type() {
    let mut nested = ListBuilder::new(ListBuilder::new(Int64Array::builder(0)));
    nested.values().values().append_value(1);
    nested.values().append(true);
    nested.append(true);
    let arrays: Vec<(ArrayRef, &str)> = vec![
        (Arc::new(UInt64Array::from(vec![1])), "UInt64"),
        (Arc::new(nested.finish()), "List(List(Int64))"),
        // Rejected on null keys too.
        (
            Arc::new(DictionaryArray::new(
                Int32Array::from(vec![None, Some(0)]),
                Arc::new(UInt64Array::from(vec![1])),
            )),
            "UInt64",
        ),
        (
            Arc::new(ListArray::new_null(
                Arc::new(Field::new(
                    "item",
                    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                    true,
                )),
                1,
            )),
            "List(Dictionary(Int32, Utf8))",
        ),
    ];
    for (array, type_name) in arrays {
        let b = column_batch(array.clone());
        for row in 0..array.len() {
            for error in [
                bind_row(&b, row).unwrap_err(),
                insert_mutation("t", &b, row).unwrap_err(),
            ] {
                assert_eq!(error.status, Status::InvalidArguments);
                assert!(
                    error.message.starts_with("cannot bind parameter \"p")
                        && error
                            .message
                            .ends_with(&format!("unsupported Arrow type {type_name}")),
                    "{error}"
                );
            }
        }
    }
    // Arrow arrays cannot have a scale above 38, but a C ABI schema can declare one.
    assert!(scalar_binder(&DataType::Decimal128(38, 39)).is_none());
}

#[test]
fn json_tagged_strings_bind_as_json() {
    let docs = Arc::new(StringArray::from(vec![Some(r#"{"a":1}"#), None]));
    let tags = {
        let mut lists = ListBuilder::new(StringBuilder::new())
            .with_field(Arc::new(json_field("item", DataType::Utf8)));
        lists.values().append_value(r#"{"a":1}"#);
        lists.values().append_null();
        lists.append(true);
        lists.append(false);
        lists.finish()
    };
    let dictionary_tags =
        DictionaryArray::new(Int8Array::from(vec![Some(0), None]), Arc::new(tags.clone()));
    // Arrow allows extension arrays to be dictionary-encoded, at any depth.
    let nested = DictionaryArray::new(
        Int8Array::from(vec![Some(0), Some(1), None]),
        Arc::new(DictionaryArray::new(
            Int8Array::from(vec![Some(0), None]),
            Arc::new(StringArray::from(vec![r#"{"a":1}"#])),
        )),
    );
    let b = batch(
        vec![
            json_field("doc", DataType::Utf8),
            Field::new("plain", DataType::Utf8, true),
            Field::new("tags", tags.data_type().clone(), true),
            Field::new("dictionary_tags", dictionary_tags.data_type().clone(), true),
            json_field("nested", nested.data_type().clone()),
            // The tag is only defined over string storage.
            json_field("n", DataType::Int64),
        ],
        vec![
            docs.clone(),
            docs,
            Arc::new(tags),
            Arc::new(dictionary_tags),
            Arc::new(nested.slice(0, 2)),
            Arc::new(Int64Array::from(vec![7, 8])),
        ],
    );
    for row in 0..2 {
        let dbg = format!("{:?}", bind_row(&b, row).unwrap().build());
        for expected in [
            r#""p0": Type(Type { code: Json"#,
            r#""p1": Type(Type { code: String"#,
            r#""p2": Type(Type { code: Array, array_element_type: Some(Type { code: Json"#,
            r#""p3": Type(Type { code: Array, array_element_type: Some(Type { code: Json"#,
            r#""p4": Type(Type { code: Json"#,
            r#""p5": Type(Type { code: Int64"#,
        ] {
            assert!(dbg.contains(expected), "row {row} lacks {expected}: {dbg}");
        }
    }
    // Spanner types mutation values from the target column, so JSON travels as a plain string.
    let dbg = format!("{:?}", insert_mutation("t", &b, 0).unwrap());
    assert!(dbg.contains(r#"StringValue("{\"a\":1}")"#), "{dbg}");
    // A null outer key keeps the JSON type as well.
    let field = json_field("nested", nested.data_type().clone());
    assert_eq!(
        cell_value("nested", &field, &nested, 2).unwrap(),
        (Value::null(), Some(types::json()))
    );
}

#[test]
fn maps_arrow_types_to_ingest_columns() {
    let list = |data_type| DataType::List(Arc::new(Field::new("item", data_type, true)));
    let dictionary =
        |data_type| DataType::Dictionary(Box::new(DataType::Int32), Box::new(data_type));
    let json_item = || Arc::new(json_field("item", DataType::Utf8));
    for (field, ddl) in [
        (Field::new("c", DataType::Int8, true), "INT64"),
        (Field::new("c", DataType::UInt32, true), "INT64"),
        (Field::new("c", DataType::Float16, true), "FLOAT32"),
        (Field::new("c", DataType::Float64, true), "FLOAT64"),
        (Field::new("c", DataType::Boolean, true), "BOOL"),
        (Field::new("c", DataType::Utf8View, true), "STRING(MAX)"),
        (
            Field::new("c", DataType::FixedSizeBinary(4), true),
            "BYTES(MAX)",
        ),
        (Field::new("c", DataType::Date64, true), "DATE"),
        (
            Field::new("c", DataType::Timestamp(TimeUnit::Second, None), true),
            "TIMESTAMP",
        ),
        (
            Field::new("c", DataType::Decimal128(38, -2), true),
            "NUMERIC",
        ),
        (
            Field::new("c", dictionary(DataType::Utf8), true),
            "STRING(MAX)",
        ),
        (Field::new("c", list(DataType::Date32), true), "ARRAY<DATE>"),
        (
            Field::new(
                "c",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 768),
                true,
            ),
            "ARRAY<FLOAT32>",
        ),
        (json_field("c", DataType::Utf8), "JSON"),
        (json_field("c", dictionary(DataType::Utf8)), "JSON"),
        (
            Field::new("c", DataType::List(json_item()), true),
            "ARRAY<JSON>",
        ),
        (
            Field::new("c", dictionary(DataType::LargeList(json_item())), true),
            "ARRAY<JSON>",
        ),
    ] {
        assert_eq!(
            spanner_column_type(&field).unwrap(),
            ddl,
            "{}",
            field.data_type()
        );
    }
}

#[test]
fn ingest_ddl_rejects_types_the_value_encoder_cannot_bind() {
    let list = |data_type| DataType::List(Arc::new(Field::new("item", data_type, true)));
    let dictionary =
        |data_type| DataType::Dictionary(Box::new(DataType::Int32), Box::new(data_type));
    for data_type in [
        DataType::UInt64,
        DataType::Null,
        DataType::Decimal128(38, 39),
        dictionary(DataType::Decimal128(38, 39)),
        list(DataType::Decimal128(38, 39)),
        list(DataType::Null),
        list(list(DataType::Int64)),
        dictionary(list(list(DataType::Int64))),
        list(dictionary(DataType::Utf8)),
    ] {
        let schema = Schema::new(vec![
            Field::new("ok", DataType::Int64, true),
            Field::new("bad", data_type.clone(), true),
        ]);
        let error = create_table_sql("t", None, &schema, false).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        assert!(
            error.message.starts_with(
                "ingest column \"bad\": cannot create a Spanner column for Arrow type"
            ),
            "{data_type}: {error}"
        );
    }
}

#[test]
fn builds_create_table_sql() {
    let schema = Schema::new(vec![
        Field::new("idx", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]);
    // No PRIMARY KEY: Spanner adds a hidden rowid key. Arrow nullability carries over.
    assert_eq!(
        create_table_sql("my_table", None, &schema, false).unwrap(),
        "CREATE TABLE `my_table` (`idx` INT64 NOT NULL, `name` STRING(MAX))"
    );
    assert!(
        create_table_sql("t", None, &schema, true)
            .unwrap()
            .starts_with("CREATE TABLE IF NOT EXISTS `t`")
    );
    assert!(
        create_table_sql("t", Some("app"), &schema, false)
            .unwrap()
            .starts_with("CREATE TABLE `app`.`t`")
    );
}

#[test]
fn builds_insert_mutations_with_raw_names() {
    let b = batch(
        vec![
            Field::new("index", DataType::Int64, false),
            Field::new("Name", DataType::Utf8, true),
        ],
        vec![
            Arc::new(Int64Array::from(vec![7, 8])),
            Arc::new(StringArray::from(vec![Some("Alice"), None])),
        ],
    );
    // Mutations carry raw names, so reserved words need no quoting.
    let dbg = format!("{:?}", insert_mutation("create", &b, 0).unwrap());
    for needle in [
        "Insert",
        "\"create\"",
        "\"index\"",
        "\"Name\"",
        r#"StringValue("7")"#,
        r#"StringValue("Alice")"#,
    ] {
        assert!(dbg.contains(needle), "missing {needle} in: {dbg}");
    }
    let dbg = format!("{:?}", insert_mutation("create", &b, 1).unwrap());
    assert!(dbg.contains("NullValue"), "no NULL for row 1: {dbg}");
}

/// The pre-DDL check must fail exactly where building the mutations would, including on a later
/// row and inside dictionaries and lists.
#[test]
fn ingest_value_check_fails_where_mutation_building_would() {
    let bad_second_rows: Vec<ArrayRef> = vec![
        Arc::new(Date32Array::from(vec![0, i32::MAX])),
        Arc::new(Date64Array::from(vec![0, 1])),
        Arc::new(TimestampSecondArray::from(vec![0, i64::MAX])),
        Arc::new(DictionaryArray::new(
            Int32Array::from(vec![0, 1]),
            Arc::new(Date32Array::from(vec![0, i32::MAX])),
        )),
        Arc::new(ListArray::from_iter_primitive::<Date32Type, _, _>([
            Some(vec![Some(0)]),
            Some(vec![Some(i32::MAX)]),
        ])),
        Arc::new(FixedSizeListArray::from_iter_primitive::<Date32Type, _, _>(
            [Some(vec![Some(0)]), Some(vec![Some(i32::MAX)])],
            1,
        )),
    ];
    for array in bad_second_rows {
        let b = batch(
            vec![
                Field::new("id", DataType::Int64, false),
                Field::new("d", array.data_type().clone(), false),
            ],
            vec![Arc::new(Int64Array::from(vec![1, 2])), array],
        );
        assert!(insert_mutation("t", &b, 0).is_ok());
        let expected = insert_mutation("t", &b, 1).unwrap_err();
        let error = check_ingest_values(&b).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        assert_eq!(error.message, expected.message);
        assert!(check_ingest_values(&b.slice(0, 1)).is_ok());
    }
    let unsupported = column_batch(Arc::new(UInt64Array::from(vec![1])));
    assert!(check_ingest_values(&unsupported).is_err());
    // A non-nullable column would be created NOT NULL. Arrow accepts this one because the
    // dictionary's keys are all valid.
    let null_value = DictionaryArray::new(
        Int8Array::from(vec![0, 1]),
        Arc::new(StringArray::from(vec![Some("a"), None])),
    );
    let b = batch(
        vec![Field::new("label", null_value.data_type().clone(), false)],
        vec![Arc::new(null_value)],
    );
    let error = check_ingest_values(&b).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(
        error.message.contains("\"label\" is non-nullable"),
        "{error}"
    );
    assert!(check_ingest_values(&b.slice(0, 1)).is_ok());
}

#[test]
fn mutation_table_joins_schema_with_a_dot() {
    assert_eq!(mutation_table(None, "Users"), "Users");
    assert_eq!(mutation_table(Some(""), "Users"), "Users");
    assert_eq!(mutation_table(Some("app"), "Users"), "app.Users");
}

#[test]
fn resolves_parameters_by_name() {
    // Columns bind in their own order under the query's spelling; @c stays unbound for Spanner
    // to report.
    assert_eq!(
        resolve_parameter_names("SELECT @a, @b, @c", &int_batch(&["b", "A"]), true).unwrap(),
        vec!["b", "a"],
    );
    let err = resolve_parameter_names("SELECT @a, @b", &int_batch(&["a", "x"]), true).unwrap_err();
    assert_eq!(err.status, Status::InvalidArguments);
    assert!(
        err.message.contains("cannot bind column \"x\"") && err.message.contains("[\"a\", \"b\"]"),
        "{}",
        err.message
    );
    // Names are case-insensitive, so two columns can name one parameter.
    let err =
        resolve_parameter_names("SELECT @NaMe", &int_batch(&["name", "NAME"]), true).unwrap_err();
    assert_eq!(err.status, Status::InvalidArguments);
    assert!(err.message.contains("same query parameter"), "{err}");
}

#[test]
fn resolves_parameters_positionally() {
    // Column names that happen to match parameters don't reorder them.
    assert_eq!(
        resolve_parameter_names("SELECT @a, @b", &int_batch(&["b", "a"]), false).unwrap(),
        vec!["a", "b"],
    );
    for (sql, columns) in [("SELECT @a, @b", &["a"][..]), ("SELECT @p1", &["x", "y"])] {
        let err = resolve_parameter_names(sql, &int_batch(columns), false).unwrap_err();
        assert_eq!(err.status, Status::InvalidArguments);
        assert!(
            err.message.contains("parameter count mismatch") && !err.message.contains('?'),
            "{sql}: {}",
            err.message
        );
    }
}

#[test]
fn a_failed_resolution_of_question_mark_sql_points_to_named_parameters() {
    for (sql, columns, by_name) in [
        ("SELECT * FROM t WHERE id = ?", &["x"][..], false),
        ("INSERT INTO t (a, b) VALUES (@a, ?)", &["x", "y"], false),
        ("SELECT * FROM t WHERE id = ?", &["x"], true),
    ] {
        let err = resolve_parameter_names(sql, &int_batch(columns), by_name).unwrap_err();
        assert_eq!(err.status, Status::InvalidArguments);
        assert!(
            err.message.contains("`?` placeholder") && err.message.contains("`@p1`"),
            "{sql}: {}",
            err.message
        );
    }
    // A `?` in a literal or comment is not a placeholder.
    let b = int_batch(&["x"]);
    let err = resolve_parameter_names("SELECT '?' -- ?", &b, false).unwrap_err();
    assert!(!err.message.contains("placeholder"), "{}", err.message);
    // Resolution that succeeds is unaffected.
    assert_eq!(
        resolve_parameter_names("SELECT @p, '?'", &b, false).unwrap(),
        vec!["p"]
    );
}

#[test]
fn bind_params_binds_each_row_under_the_resolved_names() {
    let b = batch(
        vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, true),
        ],
        vec![
            Arc::new(Int64Array::from(vec![1, 3])),
            Arc::new(StringArray::from(vec![Some("x"), None])),
        ],
    );
    let names = resolve_parameter_names("SELECT @a, @b", &b, false).unwrap();
    let bind = |row| {
        let built = bind_params(Statement::builder("SELECT @a, @b"), &names, &b, row).unwrap();
        format!("{:?}", built.build())
    };
    let row0 = bind(0);
    for needle in [
        r#""a": Value(Value { kind: Some(StringValue("1")) })"#,
        r#""b": Value(Value { kind: Some(StringValue("x")) })"#,
        r#""a": Type(Type { code: Int64"#,
        r#""b": Type(Type { code: String"#,
    ] {
        assert!(row0.contains(needle), "missing {needle} in: {row0}");
    }
    let row1 = bind(1);
    assert!(
        row1.contains(r#"StringValue("3")"#) && row1.contains("NullValue"),
        "{row1}"
    );
    assert!(!row1.contains(r#"StringValue("1")"#), "{row1}");
}

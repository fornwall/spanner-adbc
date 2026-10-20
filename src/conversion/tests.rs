use super::*;
use arrow_array::types::Int64Type;
use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int64Array, StringArray, TimestampMicrosecondArray,
};
use arrow_buffer::NullBuffer;
use google_cloud_spanner::model;
use google_cloud_spanner::types;
use google_cloud_spanner::value::ToValue;

const NUMERIC: DataType = DataType::Decimal128(NUMERIC_PRECISION, NUMERIC_SCALE);

fn timestamp_type(unit: TimeUnit) -> DataType {
    DataType::Timestamp(unit, Some(TIMESTAMP_TZ.into()))
}

fn list_of(item: DataType) -> DataType {
    DataType::List(Arc::new(Field::new(LIST_ITEM, item, true)))
}

fn struct_type(fields: Vec<(&str, Type)>) -> Type {
    let fields = fields.into_iter().map(|(name, ty)| {
        model::struct_type::Field::new()
            .set_name(name)
            .set_type(model::Type::from(ty))
    });
    model::Type::new()
        .set_code(model::TypeCode::Struct)
        .set_struct_type(model::StructType::new().set_fields(fields))
        .into()
}

fn mapped(precision: TimestampPrecision, ty: &Type) -> Field {
    arrow_field("c", ty, true, precision).unwrap()
}

/// Build `data_type` from `values` followed by an absent slot, which reads as NULL.
fn build(data_type: &DataType, values: &[Value]) -> Result<ArrayRef> {
    let mut slots: Vec<Option<&Value>> = values.iter().map(Some).collect();
    slots.push(None);
    build_array(data_type, &slots)
}

#[test]
fn spanner_types_map_to_arrow_types() {
    let code = |code: i32| Type::from(model::Type::new().set_code(model::TypeCode::from(code)));
    let row = struct_type(vec![
        ("x", types::int64()),
        ("x", types::string()),
        ("", types::array(types::bool())),
    ]);
    let row_fields = Fields::from(vec![
        Field::new("x", DataType::Int64, true),
        Field::new("x", DataType::Utf8, true),
        Field::new("", list_of(DataType::Boolean), true),
    ]);
    for (ty, expected) in [
        (types::bool(), DataType::Boolean),
        (types::int64(), DataType::Int64),
        (types::pg_oid(), DataType::Int64),
        (types::float32(), DataType::Float32),
        (types::float64(), DataType::Float64),
        (types::numeric(), NUMERIC),
        (types::string(), DataType::Utf8),
        (types::uuid(), DataType::Utf8),
        (types::interval(), DataType::Utf8),
        (types::bytes(), DataType::Binary),
        (types::date(), DataType::Date32),
        (types::timestamp(), timestamp_type(TimeUnit::Nanosecond)),
        (types::enum_type("pkg.Color"), DataType::Int64),
        (types::proto_type("pkg.Message"), DataType::Binary),
        (types::array(types::int64()), list_of(DataType::Int64)),
        (
            types::array(row.clone()),
            list_of(DataType::Struct(row_fields.clone())),
        ),
        (row, DataType::Struct(row_fields)),
        // Shapes Spanner does not send fall back to text.
        (types::array(types::array(types::int64())), DataType::Utf8),
        (types::array(Type::default()), DataType::Utf8),
        (code(9), DataType::Utf8), // STRUCT without fields
        (code(999), DataType::Utf8),
    ] {
        let field = mapped(TimestampPrecision::default(), &ty);
        assert_eq!(field.data_type(), &expected, "{ty:?}");
        assert!(field.metadata().is_empty(), "{ty:?}");
        // An empty result still builds columns of the schema's types.
        assert_eq!(build_array(&expected, &[]).unwrap().data_type(), &expected);
    }

    let micros = timestamp_type(TimeUnit::Microsecond);
    let field = mapped(
        TimestampPrecision::Microseconds,
        &types::array(types::timestamp()),
    );
    assert_eq!(field.data_type(), &list_of(micros));
}

#[test]
fn json_columns_and_items_are_tagged_arrow_json() {
    let tagged = |field: &Field| {
        assert_eq!(field.data_type(), &DataType::Utf8);
        assert_eq!(field.metadata(), &json_extension_metadata());
        assert!(is_json_field(field));
    };
    let precision = TimestampPrecision::default();
    tagged(&mapped(precision, &types::json()));
    tagged(&mapped(precision, &types::pg_jsonb()));

    // The tag belongs on the item or struct field holding the JSON, not its container.
    let list = mapped(precision, &types::array(types::json()));
    assert!(list.metadata().is_empty());
    let DataType::List(item) = list.data_type() else {
        panic!("{list:?}");
    };
    tagged(item);
    let row = mapped(precision, &struct_type(vec![("j", types::json())]));
    assert!(row.metadata().is_empty());
    let DataType::Struct(fields) = row.data_type() else {
        panic!("{row:?}");
    };
    tagged(&fields[0]);
}

#[test]
fn pathologically_nested_struct_type_fails_loudly_instead_of_overflowing() {
    // Built iteratively, so constructing the type does not recurse.
    let mut ty = model::Type::new().set_code(model::TypeCode::Int64);
    for _ in 0..(MAX_TYPE_NESTING_DEPTH + 5) {
        ty = model::Type::new()
            .set_code(model::TypeCode::Struct)
            .set_struct_type(
                model::StructType::new()
                    .set_fields([model::struct_type::Field::new().set_name("f").set_type(ty)]),
            );
    }
    let error = arrow_field("deep", &ty.into(), true, TimestampPrecision::default()).unwrap_err();
    assert_eq!(error.status, Status::Internal);
    assert!(error.message.contains("maximum depth"), "{error}");
}

#[test]
fn scalar_wire_values_decode_exactly() {
    let above_2_53 = (1_i64 << 53) + 1;
    let max_numeric = 10_i128.pow(38) - 1;
    let cases: Vec<(DataType, Vec<Value>, ArrayRef)> = vec![
        (
            DataType::Boolean,
            vec![true.to_value(), false.to_value(), Value::null()],
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                None,
                None,
            ])),
        ),
        (
            DataType::Int64,
            vec![above_2_53.to_value(), i64::MIN.to_value(), Value::null()],
            Arc::new(Int64Array::from(vec![
                Some(above_2_53),
                Some(i64::MIN),
                None,
                None,
            ])),
        ),
        (
            DataType::Float64,
            ["NaN", "Infinity", "-Infinity", "1e30"]
                .map(Value::from)
                .into_iter()
                .chain([1.25, -0.0].map(Value::from))
                .collect(),
            Arc::new(Float64Array::from(vec![
                Some(f64::NAN),
                Some(f64::INFINITY),
                Some(f64::NEG_INFINITY),
                Some(1e30),
                Some(1.25),
                Some(-0.0),
                None,
            ])),
        ),
        (
            DataType::Float32,
            [
                f32::MIN,
                f32::MAX,
                f32::from_bits(1),
                -0.0,
                f32::NAN,
                f32::INFINITY,
            ]
            .map(|f| Value::from(f64::from(f)))
            .to_vec(),
            Arc::new(Float32Array::from(vec![
                Some(f32::MIN),
                Some(f32::MAX),
                Some(f32::from_bits(1)),
                Some(-0.0),
                Some(f32::NAN),
                Some(f32::INFINITY),
                None,
            ])),
        ),
        (
            DataType::Date32,
            ["1970-01-01", "1969-12-31", "0001-01-01", "9999-12-31"]
                .map(Value::from)
                .to_vec(),
            Arc::new(Date32Array::from(vec![
                Some(0),
                Some(-1),
                Some(-719_162),
                Some(2_932_896),
                None,
            ])),
        ),
        (
            timestamp_type(TimeUnit::Nanosecond),
            [
                "1970-01-01T00:00:00Z",
                "1970-01-01T00:00:00.000000999Z",
                "1969-12-31T23:59:59.999999999Z",
                "2024-01-15T12:34:56.7Z",
                "2024-01-15T12:34:56.789012345Z",
            ]
            .map(Value::from)
            .to_vec(),
            Arc::new(
                TimestampNanosecondArray::from(vec![
                    Some(0),
                    Some(999),
                    Some(-1),
                    Some(1_705_322_096_700_000_000),
                    Some(1_705_322_096_789_012_345),
                    None,
                ])
                .with_timezone(TIMESTAMP_TZ),
            ),
        ),
        (
            timestamp_type(TimeUnit::Microsecond),
            [
                "0001-01-01T00:00:00Z",
                "9999-12-31T23:59:59.999999999Z",
                "1969-12-31T23:59:59.9999999Z",
                "2024-01-15T12:34:56.789012345Z",
            ]
            .map(Value::from)
            .to_vec(),
            Arc::new(
                TimestampMicrosecondArray::from(vec![
                    Some(-62_135_596_800_000_000),
                    Some(253_402_300_799_999_999),
                    Some(-1),
                    Some(1_705_322_096_789_012),
                    None,
                ])
                .with_timezone(TIMESTAMP_TZ),
            ),
        ),
        (
            NUMERIC,
            [
                "0",
                "-0",
                "1.5",
                "-2.25",
                "0.000000001",
                "99999999999999999999999999999.999999999",
                "-99999999999999999999999999999.999999999",
            ]
            .map(Value::from)
            .to_vec(),
            Arc::new(
                Decimal128Array::from(vec![
                    Some(0),
                    Some(0),
                    Some(1_500_000_000),
                    Some(-2_250_000_000),
                    Some(1),
                    Some(max_numeric),
                    Some(-max_numeric),
                    None,
                ])
                .with_precision_and_scale(NUMERIC_PRECISION, NUMERIC_SCALE)
                .unwrap(),
            ),
        ),
        (
            DataType::Binary,
            ["AQID", ""].map(Value::from).to_vec(),
            Arc::new(BinaryArray::from(vec![
                Some(&[1_u8, 2, 3][..]),
                Some(&[][..]),
                None,
            ])),
        ),
        (
            DataType::Utf8,
            vec![
                "naïve 日本語".to_value(),
                "".to_value(),
                Value::null(),
                true.to_value(),
            ],
            Arc::new(StringArray::from(vec![
                Some("naïve 日本語"),
                Some(""),
                None,
                Some("true"),
                None,
            ])),
        ),
    ];
    for (data_type, values, expected) in cases {
        let array = build(&data_type, &values).unwrap();
        assert_eq!(array.as_ref(), expected.as_ref(), "{data_type}");
    }
}

#[test]
fn malformed_values_are_errors_naming_the_type_and_value() {
    let nanos = timestamp_type(TimeUnit::Nanosecond);
    let row = DataType::Struct(Fields::from(vec![Field::new("id", DataType::Int64, true)]));
    for (data_type, value, spanner_type) in [
        (DataType::Boolean, "true".to_value(), "BOOL"),
        // A number may already have been rounded through f64.
        (DataType::Int64, 42.0.to_value(), "INT64"),
        (DataType::Int64, "9223372036854775808".to_value(), "INT64"),
        (DataType::Float64, "1e999".to_value(), "FLOAT64"),
        (DataType::Float32, f64::MAX.to_value(), "FLOAT32"),
        (DataType::Float32, "-1e39".to_value(), "FLOAT32"),
        (DataType::Date32, "0000-01-01".to_value(), "DATE"),
        (DataType::Date32, "2023-02-29".to_value(), "DATE"),
        // Malformed, not out of range.
        (
            nanos.clone(),
            "2024-13-45T99:99:99Z".to_value(),
            "TIMESTAMP",
        ),
        (nanos, "0000-01-01T00:00:00Z".to_value(), "TIMESTAMP"),
        (
            timestamp_type(TimeUnit::Microsecond),
            "2024-01-01T00:00:00+00:00".to_value(),
            "TIMESTAMP",
        ),
        (
            NUMERIC,
            "100000000000000000000000000000".to_value(),
            "NUMERIC",
        ),
        (NUMERIC, "NaN".to_value(), "NUMERIC"),
        (DataType::Binary, "!!!".to_value(), "BYTES"),
        (list_of(DataType::Int64), "scalar".to_value(), "ARRAY"),
        (row, "scalar".to_value(), "STRUCT"),
    ] {
        let error = build(&data_type, std::slice::from_ref(&value)).unwrap_err();
        assert_eq!(error.status, Status::InvalidData, "{error}");
        let shown = value_to_json(&value).unwrap().to_string();
        assert!(
            error.message.contains(spanner_type)
                && error.message.contains(&shown)
                && !error
                    .message
                    .contains(crate::OPTION_MAX_TIMESTAMP_PRECISION),
            "{data_type}: {error}"
        );
    }
}

#[test]
fn noncanonical_text_is_rejected() {
    for s in [
        "not-a-date",
        "2024/01/15",
        "2024-1-15",
        "2024-01-15 ",
        "+024-01-15",
        "2024-13-01",
        "2024-00-10",
        "2024-04-31",
    ] {
        assert_eq!(parse_date_days(s), None, "{s}");
    }
    for s in [
        "2024-01-15T12:34:56+02:00",
        "2024-01-15t12:34:56Z",
        "2024-01-15T12:34:56z",
        "2024-01-15T12:34:56",
        "2024-01-15T12:34:56.Z",
        "2024-01-15T12:34:56.",
        "2024-01-15T12:34:56.1234567890Z",
        "2024-01-15T12:34:5.6Z",
        "2024-01-15T24:00:00Z",
        "2024-01-15T23:60:00Z",
        "2024-01-15T23:59:60Z",
        "2024-01-15 12:34:56Z",
    ] {
        assert_eq!(parse_timestamp_micros(s), None, "{s}");
    }
    for s in [
        "",
        "-",
        ".",
        "-.",
        ".5",
        "-.5",
        "5.",
        "-5.",
        "+3",
        " 1",
        "1e5",
        "0.0000000001",
    ] {
        assert_eq!(parse_numeric_i128(s), None, "{s:?}");
    }
}

#[test]
fn temporal_parsers_match_chrono_across_spanner_range() {
    use chrono::{NaiveDate, NaiveDateTime};
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    for year in 0..=9999 {
        for month in 0..=13 {
            for day in [0, 1, 28, 29, 30, 31, 32] {
                let text = format!("{year:04}-{month:02}-{day:02}");
                let expected = NaiveDate::from_ymd_opt(year, month, day)
                    .filter(|_| year > 0)
                    .map(|d| (d - epoch).num_days() as i32);
                assert_eq!(parse_date_days(&text), expected, "{text}");
            }
        }
        let text = format!("{year:04}-02-28T23:59:59.987654321Z");
        let expected = NaiveDateTime::parse_from_str(&text, "%Y-%m-%dT%H:%M:%S%.fZ")
            .ok()
            .filter(|_| year > 0)
            .map(|t| t.and_utc());
        assert_eq!(
            parse_timestamp_nanos(&text),
            expected.and_then(|t| t.timestamp_nanos_opt()),
            "{text}"
        );
        assert_eq!(
            parse_timestamp_micros(&text),
            expected.map(|t| t.timestamp_micros()),
            "{text}"
        );
    }
    assert_eq!(
        parse_timestamp_nanos("1677-09-21T00:12:43.145224192Z"),
        Some(i64::MIN)
    );
    assert_eq!(
        parse_timestamp_nanos("1677-09-21T00:12:43.145224191Z"),
        None
    );
    assert_eq!(
        parse_timestamp_nanos("2262-04-11T23:47:16.854775807Z"),
        Some(i64::MAX)
    );
    assert_eq!(
        parse_timestamp_nanos("2262-04-11T23:47:16.854775808Z"),
        None
    );
}

#[test]
fn nanosecond_overflow_names_the_column_and_the_option() {
    let column = |unit| Arc::new(Field::new("CreatedAt", timestamp_type(unit), true));
    let values = ["1500-06-15T12:34:56.789012Z", "3000-01-01T00:00:00Z"].map(Value::from);
    for value in &values {
        let error = build_column(&column(TimeUnit::Nanosecond), &[Some(value)]).unwrap_err();
        assert_eq!(error.status, Status::InvalidData);
        for needle in [
            "column \"CreatedAt\"",
            value.as_str().unwrap(),
            crate::OPTION_MAX_TIMESTAMP_PRECISION,
            TimestampPrecision::MICROSECONDS,
        ] {
            assert!(error.message.contains(needle), "{error}");
        }
        build_column(&column(TimeUnit::Microsecond), &[Some(value)]).unwrap();
    }
}

#[test]
fn errors_name_the_column_and_struct_field() {
    let item = DataType::Struct(Fields::from(vec![Field::new("price", NUMERIC, true)]));
    let column = Arc::new(Field::new("Items", list_of(item), true));
    let value = vec![vec!["abc"].to_value()].to_value();
    let error = build_column(&column, &[Some(&value)]).unwrap_err();
    assert_eq!(
        error.message,
        r#"column "Items": field "price": cannot decode Spanner NUMERIC wire value "abc""#
    );
}

#[test]
fn arrays_and_structs_keep_nulls_and_positional_fields() {
    let array = build(
        &list_of(DataType::Int64),
        &[
            Value::null(),
            Vec::<i64>::new().to_value(),
            vec![Some(1_i64), None, Some(3)].to_value(),
        ],
    )
    .unwrap();
    let expected = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
        None,
        Some(vec![]),
        Some(vec![Some(1), None, Some(3)]),
        None,
    ]);
    assert_eq!(array.as_ref(), &expected as &dyn Array);

    // STRUCT<x INT64, x STRING, `` BOOL> in an ARRAY, the shape Spanner returns structs in.
    let fields = Fields::from(vec![
        Field::new("x", DataType::Int64, true),
        Field::new("x", DataType::Utf8, true),
        Field::new("", DataType::Boolean, true),
    ]);
    let rows = vec![
        vec!["1".to_value(), "a".to_value(), true.to_value()].to_value(),
        Value::null(),
        vec![Value::null(), Value::null(), Value::null()].to_value(),
    ]
    .to_value();
    let array = build(&list_of(DataType::Struct(fields.clone())), &[rows]).unwrap();
    let expected = StructArray::new(
        fields,
        vec![
            Arc::new(Int64Array::from(vec![Some(1), None, None])),
            Arc::new(StringArray::from(vec![Some("a"), None, None])),
            Arc::new(BooleanArray::from(vec![Some(true), None, None])),
        ],
        Some(NullBuffer::from(vec![true, false, true])),
    );
    let lists = array.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(lists.value(0).as_ref(), &expected as &dyn Array);
    assert!(lists.is_null(1));
}

#[test]
fn structs_reject_missing_and_extra_fields() {
    let ty = DataType::Struct(Fields::from(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]));
    for count in [0, 1, 3] {
        let value = vec!["1"; count].to_value();
        let error = build(&ty, &[value]).unwrap_err();
        assert_eq!(error.status, Status::InvalidData);
        assert!(
            error
                .message
                .contains(&format!("expected 2 fields, got {count}")),
            "{error}"
        );
    }
}

#[test]
fn empty_structs_preserve_length_and_validity() {
    let ty = DataType::Struct(Fields::empty());
    let empty = Vec::<Value>::new().to_value();
    let array = build(&ty, &[empty.clone(), Value::null(), empty]).unwrap();
    assert_eq!(array.data_type(), &ty);
    assert_eq!(array.len(), 4);
    assert_eq!(
        (0..4).map(|i| array.is_valid(i)).collect::<Vec<_>>(),
        [true, false, true, false]
    );
}

#[test]
fn timestamp_precision_option_parses_and_round_trips() {
    let parse = |s: &str| TimestampPrecision::parse_option(OptionValue::String(s.to_string()));
    for mode in [
        TimestampPrecision::NanosecondsErrorOnOverflow,
        TimestampPrecision::Microseconds,
    ] {
        assert_eq!(parse(mode.as_str()).unwrap(), mode);
    }
    assert_eq!(parse("").unwrap(), TimestampPrecision::default());
    assert_eq!(
        parse(" microseconds ").unwrap(),
        TimestampPrecision::Microseconds
    );
    // Snowflake's wrapping `nanoseconds` mode is not offered.
    for bad in ["nanoseconds", "millis"] {
        let error = parse(bad).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "{bad:?}");
        assert!(
            error
                .message
                .contains(crate::OPTION_MAX_TIMESTAMP_PRECISION)
                && error.message.contains(bad),
            "{error}"
        );
    }
    let error = TimestampPrecision::parse_option(OptionValue::Int(1)).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
}

/// A result with no columns, such as a PLAN probe of a statement that returns nothing, still
/// converts. Arrow cannot infer a batch's length from zero columns.
#[test]
fn zero_column_result_converts_to_an_empty_batch() {
    let batch = build_batch(Arc::new(Schema::empty()), &[]).unwrap();
    assert_eq!(batch.num_columns(), 0);
    assert_eq!(batch.num_rows(), 0);
}

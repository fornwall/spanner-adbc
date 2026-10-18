use super::*;
use arrow_array::{Array, StringArray, TimestampMicrosecondArray};
use google_cloud_spanner::value::ToValue;

struct PanickingSource;

impl ChunkSource for PanickingSource {
    type Row = Row;

    async fn next_chunk(&mut self) -> Result<Vec<Row>> {
        panic!("row decoder failed");
    }
}

#[test]
fn background_fetch_panic_is_an_error_not_end_of_stream() {
    let mut reader = SpannerBatchReader::new(
        crate::runtime::new_runtime().unwrap(),
        CancelSignal::new(),
        Arc::new(Schema::empty()),
        None,
        PanickingSource,
    );
    let error = reader
        .next()
        .expect("failed fetch must not report EOF")
        .unwrap_err();
    let ArrowError::ExternalError(error) = error else {
        panic!("expected an ADBC error");
    };
    let error = error.downcast_ref::<adbc_core::error::Error>().unwrap();
    assert_eq!(error.status, Status::Internal);
    assert!(error.message.contains("result is incomplete"), "{error}");
    assert!(error.message.contains("row decoder failed"), "{error}");
    assert!(reader.next().is_none());
}

/// A driver error reaching a `RecordBatchReader` consumer must stay a *boxed* ADBC error, not
/// a stringified one: the C ABI's stream export (`src/ffi/stream.rs`) recovers the ADBC status
/// by walking `ArrowError`'s source chain and downcasting back to
/// [`adbc_core::error::Error`]. Stringify it and every FFI unit test still passes while a
/// cancelled read stops reporting `ECANCELED` — the one thing that whole export layer exists
/// for. This is the assertion that ties the two ends together.
#[test]
fn to_arrow_error_boxes_the_adbc_error_for_the_ffi_stream_export() {
    let error = crate::error::err("operation cancelled", Status::Cancelled);
    let arrow = to_arrow_error(error);
    let ArrowError::ExternalError(boxed) = &arrow else {
        panic!("the driver error must stay an ExternalError, not be stringified: {arrow:?}")
    };
    let recovered = boxed
        .downcast_ref::<adbc_core::error::Error>()
        .expect("the ADBC error must be recoverable by downcast");
    assert_eq!(recovered.status, Status::Cancelled);
    assert_eq!(recovered.message, "operation cancelled");
    // And the message still reaches a consumer that only renders the ArrowError.
    assert!(arrow.to_string().contains("operation cancelled"), "{arrow}");
}

/// The `arrow.json` extension name attached to a Field's metadata, if any.
fn extension_name(field: &Field) -> Option<&str> {
    field
        .metadata()
        .get(ARROW_EXTENSION_NAME)
        .map(String::as_str)
}

#[test]
fn json_column_is_tagged_arrow_json() {
    let field = arrow_field(
        "payload",
        &google_cloud_spanner::types::json(),
        true,
        TimestampPrecision::default(),
    )
    .unwrap();
    // Storage type stays Utf8; only the field metadata marks it as logical JSON.
    assert_eq!(field.data_type(), &DataType::Utf8);
    assert_eq!(extension_name(&field), Some(ARROW_JSON_EXTENSION));
    // Both canonical extension keys are present (metadata value is the empty string).
    assert_eq!(
        field
            .metadata()
            .get(ARROW_EXTENSION_METADATA)
            .map(String::as_str),
        Some("")
    );
}

#[test]
fn string_column_is_not_tagged() {
    // Guards against over-tagging: plain STRING must stay untagged Utf8.
    let field = arrow_field(
        "name",
        &google_cloud_spanner::types::string(),
        true,
        TimestampPrecision::default(),
    )
    .unwrap();
    assert_eq!(field.data_type(), &DataType::Utf8);
    assert_eq!(extension_name(&field), None);
    assert!(field.metadata().is_empty());
}

#[test]
fn array_of_json_tags_the_item_field() {
    let element = google_cloud_spanner::types::json();
    let field = arrow_field(
        "tags",
        &google_cloud_spanner::types::array(element),
        true,
        TimestampPrecision::default(),
    )
    .unwrap();
    // Top-level List field carries no extension; its child (`item`) field does.
    assert_eq!(extension_name(&field), None);
    let DataType::List(item) = field.data_type() else {
        panic!("expected a List data type, got {:?}", field.data_type());
    };
    assert_eq!(item.data_type(), &DataType::Utf8);
    assert_eq!(extension_name(item), Some(ARROW_JSON_EXTENSION));
}

#[test]
fn proto_columns_map_to_binary() {
    use google_cloud_spanner::model;

    // The pinned client exposes no public constructor for PROTO types, so build one straight
    // from the generated model type and wrap it.
    let ty: Type = model::Type::new().set_code(model::TypeCode::Proto).into();

    // A scalar PROTO maps to its raw serialized bytes: Binary, no extension metadata.
    let field = arrow_field("payload", &ty, true, TimestampPrecision::default()).unwrap();
    assert_eq!(field.data_type(), &DataType::Binary);
    assert_eq!(extension_name(&field), None);

    // ARRAY<PROTO> maps to List<Binary> the same way, recursively.
    let array_ty = google_cloud_spanner::types::array(ty);
    let field = arrow_field("payloads", &array_ty, true, TimestampPrecision::default()).unwrap();
    let DataType::List(item) = field.data_type() else {
        panic!("expected a List data type, got {:?}", field.data_type());
    };
    assert_eq!(item.data_type(), &DataType::Binary);

    // The wire value is the serialized proto message base64-encoded (like BYTES); it decodes to
    // those exact bytes. "AQID" is base64 for [1, 2, 3].
    let value = "AQID".to_value();
    let array = build_array(&DataType::Binary, &[Some(&value)]).unwrap();
    let bytes = array
        .as_any()
        .downcast_ref::<arrow_array::BinaryArray>()
        .unwrap();
    assert_eq!(bytes.len(), 1);
    assert_eq!(bytes.value(0), &[1u8, 2, 3]);
}

#[test]
fn enum_columns_map_to_int64_ordinal() {
    use google_cloud_spanner::model;

    // The pinned client exposes no public constructor for ENUM types, so build one straight
    // from the generated model type and wrap it.
    let ty: Type = model::Type::new().set_code(model::TypeCode::Enum).into();

    // A scalar ENUM maps to its integer ordinal: Int64, no extension metadata.
    let field = arrow_field("color", &ty, true, TimestampPrecision::default()).unwrap();
    assert_eq!(field.data_type(), &DataType::Int64);
    assert_eq!(extension_name(&field), None);

    // ARRAY<ENUM> maps to List<Int64> the same way, recursively.
    let array_ty = google_cloud_spanner::types::array(ty);
    let field = arrow_field("colors", &array_ty, true, TimestampPrecision::default()).unwrap();
    let DataType::List(item) = field.data_type() else {
        panic!("expected a List data type, got {:?}", field.data_type());
    };
    assert_eq!(item.data_type(), &DataType::Int64);

    // The wire value is the ordinal as a decimal string (like INT64); it decodes to that i64.
    let value = "2".to_value();
    let array = build_array(&DataType::Int64, &[Some(&value)]).unwrap();
    let ints = array
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap();
    assert_eq!(ints.len(), 1);
    assert_eq!(ints.value(0), 2);
}

#[test]
fn pathologically_nested_struct_type_fails_loudly_instead_of_overflowing() {
    use google_cloud_spanner::model;

    // A hostile endpoint could return `STRUCT<STRUCT<…>>` metadata nested far deeper than any
    // legitimate result type. The type walk caps its recursion, so it must return a
    // clean error rather than exhausting the stack. Build the nested type bottom-up (iterative,
    // so *constructing* it doesn't recurse) well past MAX_TYPE_NESTING_DEPTH.
    let mut ty = model::Type::new().set_code(model::TypeCode::Int64);
    for _ in 0..(MAX_TYPE_NESTING_DEPTH + 5) {
        ty = model::Type::new()
            .set_code(model::TypeCode::Struct)
            .set_struct_type(
                model::StructType::new()
                    .set_fields([model::struct_type::Field::new().set_name("f").set_type(ty)]),
            );
    }
    let ty: Type = ty.into();

    let err = arrow_field("deep", &ty, true, TimestampPrecision::default()).unwrap_err();
    assert_eq!(err.status, Status::Internal);
    assert!(
        err.message.contains("maximum depth"),
        "unexpected message: {}",
        err.message
    );

    // A shallow struct still maps fine — the cap doesn't perturb the happy path.
    let shallow: Type = model::Type::new()
        .set_code(model::TypeCode::Struct)
        .set_struct_type(
            model::StructType::new().set_fields([model::struct_type::Field::new()
                .set_name("n")
                .set_type(model::Type::new().set_code(model::TypeCode::Int64))]),
        )
        .into();
    let field = arrow_field("s", &shallow, true, TimestampPrecision::default()).unwrap();
    assert!(matches!(field.data_type(), DataType::Struct(_)));
}

#[test]
fn json_value_round_trips_as_utf8_text() {
    // The value path is unchanged: JSON text is kept verbatim as a Utf8 string.
    let text = r#"{"a":1,"b":[true,null]}"#;
    let value = text.to_value();
    let array = build_array(&DataType::Utf8, &[Some(&value)]).unwrap();
    let strings = array.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(strings.len(), 1);
    assert_eq!(strings.value(0), text);
}

#[test]
fn string_array_round_trips_values_and_nulls() {
    // Built via StringBuilder (no per-value owned String on the string path). Both a SQL NULL
    // value and a missing slot become null; present strings are kept verbatim, incl. empty.
    let hello = "hello".to_value();
    let empty = "".to_value();
    let unicode = "naïve café — 日本語".to_value();
    let sql_null = None::<&str>.to_value();

    let array = build_array(
        &DataType::Utf8,
        &[
            Some(&hello),
            Some(&empty),
            Some(&unicode),
            Some(&sql_null),
            None,
        ],
    )
    .unwrap();
    let strings = array.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(strings.len(), 5);
    assert_eq!(strings.value(0), "hello");
    assert_eq!(strings.value(1), "");
    assert!(!strings.is_null(1)); // present empty string, not a null slot
    assert_eq!(strings.value(2), "naïve café — 日本語");
    assert!(strings.is_null(3)); // SQL NULL value
    assert!(strings.is_null(4)); // missing slot
}

#[test]
fn dates_to_epoch_days() {
    assert_eq!(parse_date_days("1970-01-01"), Some(0));
    assert_eq!(parse_date_days("1970-01-02"), Some(1));
    assert_eq!(parse_date_days("1969-12-31"), Some(-1));
    assert_eq!(parse_date_days("2024-01-15"), Some(19737));
    assert_eq!(parse_date_days("not-a-date"), None);
    // Spanner's DATE range endpoints, via the fixed-offset fast path.
    assert_eq!(parse_date_days("0001-01-01"), Some(-719162));
    assert_eq!(parse_date_days("9999-12-31"), Some(2932896));
    // Leap day, and the day after, exercise the closed-form epoch math across a Feb 29.
    assert_eq!(parse_date_days("2024-02-29"), Some(19782));
    assert_eq!(parse_date_days("2024-03-01"), Some(19783));
    // Malformed canonical-width inputs the fast path must reject (not misparse).
    assert_eq!(parse_date_days("2024-13-01"), None); // month out of range
    assert_eq!(parse_date_days("2024-02-30"), None); // day out of range for month
    assert_eq!(parse_date_days("2024-0x-15"), None); // non-digit in a field
    assert_eq!(parse_date_days("2024/01/15"), None); // wrong separators
    // Canonical-only: non-canonical widths (which Spanner never emits) are rejected outright.
    assert_eq!(parse_date_days("2024-1-15"), None); // single-digit month
    assert_eq!(parse_date_days("2024-01-15 "), None); // trailing byte
}

#[test]
fn timestamps_to_epoch_nanos() {
    assert_eq!(parse_timestamp_nanos("1970-01-01T00:00:00Z"), Some(0));
    assert_eq!(
        parse_timestamp_nanos("1970-01-01T00:00:00.000001Z"),
        Some(1_000)
    );
    assert_eq!(
        parse_timestamp_nanos("2024-01-15T12:34:56.789012Z"),
        Some(1_705_322_096_789_012_000)
    );
    // Sub-microsecond precision is preserved (not truncated): 999 nanoseconds stays 999.
    assert_eq!(
        parse_timestamp_nanos("1970-01-01T00:00:00.000000999Z"),
        Some(999)
    );
    // Full nine fractional digits round-trip.
    assert_eq!(
        parse_timestamp_nanos("2024-01-15T12:34:56.789012345Z"),
        Some(1_705_322_096_789_012_345)
    );
    assert_eq!(parse_timestamp_nanos("nope"), None);
    // Outside the i64-nanosecond range (~1677-09-21 to 2262-04-11): no representation, so None
    // rather than a wrapped/panicking value.
    assert_eq!(parse_timestamp_nanos("3000-01-01T00:00:00Z"), None);
    assert_eq!(parse_timestamp_nanos("1000-01-01T00:00:00Z"), None);
}

#[test]
fn timestamp_scanner_matches_chrono_on_canonical_utc() {
    // The hand-rolled scanner must return exactly what chrono's RFC 3339 parser did, for every
    // canonical UTC shape Spanner emits — across fractional-digit counts and pre/post epoch.
    for s in [
        "1970-01-01T00:00:00Z",
        "2024-01-15T12:34:56Z",
        "2024-01-15T12:34:56.7Z",
        "2024-01-15T12:34:56.789Z",
        "2024-01-15T12:34:56.789012Z",
        "2024-01-15T12:34:56.789012345Z",
        "1969-12-31T23:59:59.999999999Z",
        "2000-02-29T00:00:00Z", // leap day
    ] {
        let expected = chrono::DateTime::parse_from_rfc3339(s)
            .unwrap()
            .timestamp_nanos_opt();
        assert_eq!(parse_timestamp_nanos(s), expected, "mismatch for {s}");
    }
}

#[test]
fn timestamp_scanner_rejects_noncanonical() {
    // Spanner always emits UTC with a `Z` suffix; the scanner accepts only that exact shape.
    for s in [
        "2024-01-15T12:34:56+02:00",       // non-Z offset
        "2024-01-15t12:34:56Z",            // lowercase 'T'
        "2024-01-15T12:34:56z",            // lowercase 'z'
        "2024-01-15T12:34:56",             // missing 'Z'
        "2024-01-15T12:34:56.Z",           // empty fraction
        "2024-01-15T12:34:56.",            // trailing dot, no digits or `Z` (must not panic)
        "2024-01-15T12:34:56.1234567890Z", // >9 fractional digits
        "2024-13-15T00:00:00Z",            // month out of range
        "2024-01-15T25:00:00Z",            // hour out of range
    ] {
        assert_eq!(parse_timestamp_nanos(s), None, "should reject {s}");
    }
}

#[test]
fn timestamp_array_preserves_nulls_and_errors_out_of_range() {
    use arrow_array::Array;
    use google_cloud_spanner::value::ToValue;
    let ty = DataType::Timestamp(TimeUnit::Nanosecond, Some(TIMESTAMP_TZ.into()));
    let null = None::<&str>.to_value();
    let in_range = "1970-01-01T00:00:00.000000999Z".to_value();
    let out_of_range = "3000-01-01T00:00:00Z".to_value();

    // A SQL NULL maps to a null slot; a present in-range value keeps full nanosecond precision.
    let array = build_array(&ty, &[Some(&null), Some(&in_range), None]).unwrap();
    let ts = array
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();
    assert!(ts.is_null(0));
    assert_eq!(ts.value(1), 999);
    assert!(ts.is_null(2));

    // A present but out-of-range value is a real timestamp we cannot encode: it must error,
    // not become a silent null.
    let err = build_array(&ty, &[Some(&out_of_range)]).unwrap_err();
    assert_eq!(err.status, Status::InvalidArguments);
    assert!(err.message.contains("3000-01-01T00:00:00Z"));
}

#[test]
fn numerics_to_unscaled_i128() {
    assert_eq!(parse_numeric_i128("0"), Some(0));
    assert_eq!(parse_numeric_i128("1"), Some(1_000_000_000));
    assert_eq!(parse_numeric_i128("1.5"), Some(1_500_000_000));
    assert_eq!(parse_numeric_i128("-2.25"), Some(-2_250_000_000));
    assert_eq!(parse_numeric_i128("0.000000001"), Some(1));
    assert_eq!(parse_numeric_i128("9"), Some(9_000_000_000));
    assert_eq!(parse_numeric_i128("abc"), None);
    assert_eq!(parse_numeric_i128(""), None);
    // Strict canonical-only: Spanner never emits these, so they are rejected outright.
    assert_eq!(parse_numeric_i128("+3"), None); // leading '+'
    assert_eq!(parse_numeric_i128("0.0000000019"), None); // >9 fractional digits (scale is 9)
    assert_eq!(parse_numeric_i128(" 1"), None); // surrounding whitespace
    assert_eq!(parse_numeric_i128(".5"), None); // no digits before the point
    assert_eq!(parse_numeric_i128("-.5"), None); // no digits before the point (negative)
    assert_eq!(parse_numeric_i128("5."), None); // no digits after the point
    assert_eq!(parse_numeric_i128("-5."), None); // no digits after the point (negative)
    assert_eq!(parse_numeric_i128("."), None); // no digits at all
    assert_eq!(parse_numeric_i128("-."), None); // no digits at all (negative)
    assert_eq!(parse_numeric_i128("-"), None); // bare sign
}

#[test]
fn chunk_budget_counts_strings_and_nested_value_storage() {
    let nulls = Value::from(vec![None::<bool>; 4096]);
    let bools = Value::from(vec![false; 4096]);
    let empty_strings = Value::from(vec![""; 4096]);
    for value in [&nulls, &bools, &empty_strings] {
        assert!(approx_value_bytes(value) >= 4096 * size_of::<Value>());
    }
    let nested = Value::from(vec![nulls, bools, empty_strings]);
    assert!(approx_value_bytes(&nested) >= 3 * 4096 * size_of::<Value>());

    let short = Value::from("hello");
    let empty = Value::from("");
    assert_eq!(approx_value_bytes(&short) - approx_value_bytes(&empty), 5);

    let wide = Value::from("x".repeat(CHUNK_BYTE_BUDGET));
    assert!(approx_value_bytes(&wide) >= CHUNK_BYTE_BUDGET);
}

#[test]
fn float32_preserves_extrema_and_special_values_but_rejects_finite_overflow() {
    let values: Vec<Value> = [
        f64::from(f32::MIN),
        f64::from(f32::MAX),
        f64::from(f32::from_bits(1)),
        -0.0,
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
    ]
    .into_iter()
    .map(Value::from)
    .collect();
    let array = build_array(
        &DataType::Float32,
        &values.iter().map(Some).collect::<Vec<_>>(),
    )
    .unwrap();
    let array = array
        .as_any()
        .downcast_ref::<PrimitiveArray<Float32Type>>()
        .unwrap();
    assert_eq!(array.value(0), f32::MIN);
    assert_eq!(array.value(1), f32::MAX);
    assert_eq!(array.value(2).to_bits(), 1);
    assert_eq!(array.value(3).to_bits(), (-0.0f32).to_bits());
    assert!(array.value(4).is_nan());
    assert_eq!(array.value(5), f32::INFINITY);
    assert_eq!(array.value(6), f32::NEG_INFINITY);

    for value in [
        Value::from(f64::MAX),
        Value::from(-f64::MAX),
        Value::from("1e39"),
    ] {
        let error = build_array(&DataType::Float32, &[Some(&value)]).unwrap_err();
        assert_eq!(error.status, Status::InvalidData);
        assert!(error.message.contains("FLOAT32"), "{error}");
    }
}

#[test]
fn floating_point_strings_reject_overflow_and_preserve_explicit_special_values() {
    for data_type in [DataType::Float32, DataType::Float64] {
        for text in ["1e999", "-1e999"] {
            let value = Value::from(text);
            let error = build_array(&data_type, &[Some(&value)]).unwrap_err();
            assert_eq!(error.status, Status::InvalidData);
            assert!(error.message.contains(text), "{error}");
        }
    }
    for (text, expected) in [
        ("1.25", 1.25),
        ("1e30", 1e30),
        ("Infinity", f64::INFINITY),
        ("-Infinity", f64::NEG_INFINITY),
    ] {
        assert_eq!(parse_f64(&Value::from(text)), Some(expected));
    }
    assert!(parse_f64(&Value::from("NaN")).unwrap().is_nan());
}

/// A present wire value that cannot be decoded as the column's type must error (naming the
/// type and the offending value), never silently turn into a NULL slot the caller cannot
/// distinguish from a genuine SQL NULL.
#[test]
fn undecodable_values_error_instead_of_becoming_null() {
    use google_cloud_spanner::value::ToValue;
    let garbage = "not-a-value".to_value();
    for (data_type, spanner_type) in [
        (DataType::Boolean, "BOOL"),
        (DataType::Int64, "INT64"),
        (DataType::Float64, "FLOAT64"),
        (DataType::Float32, "FLOAT32"),
        (DataType::Date32, "DATE"),
        (
            DataType::Timestamp(TimeUnit::Nanosecond, Some(TIMESTAMP_TZ.into())),
            "TIMESTAMP",
        ),
        (
            DataType::Decimal128(NUMERIC_PRECISION, NUMERIC_SCALE),
            "NUMERIC",
        ),
    ] {
        let err = build_array(&data_type, &[Some(&garbage)]).expect_err(spanner_type);
        assert_eq!(err.status, Status::InvalidData, "{spanner_type}");
        assert!(
            err.message.contains(spanner_type) && err.message.contains("not-a-value"),
            "{spanner_type}: {}",
            err.message
        );
    }
    // BYTES: a string that is not valid base64.
    let bad_base64 = "!!!".to_value();
    let err = build_array(&DataType::Binary, &[Some(&bad_base64)]).unwrap_err();
    assert_eq!(err.status, Status::InvalidData);
    assert!(err.message.contains("BYTES"), "{}", err.message);
}

/// SQL NULLs (and absent values) still map to null slots — strict decoding only applies to
/// values that are actually present.
#[test]
fn nulls_still_map_to_null_slots_under_strict_decoding() {
    use arrow_array::Array;
    use google_cloud_spanner::value::ToValue;
    let null = None::<i64>.to_value();
    for data_type in [
        DataType::Boolean,
        DataType::Int64,
        DataType::Float64,
        DataType::Float32,
        DataType::Date32,
        DataType::Decimal128(NUMERIC_PRECISION, NUMERIC_SCALE),
        DataType::Binary,
    ] {
        let array = build_array(&data_type, &[Some(&null), None]).unwrap();
        assert_eq!(array.len(), 2, "{data_type}");
        assert!(array.is_null(0) && array.is_null(1), "{data_type}");
    }
}

/// A malformed TIMESTAMP string is a decode error; only a well-formed instant outside the
/// Arrow nanosecond range gets the more specific out-of-range message.
#[test]
fn malformed_timestamp_is_a_decode_error_not_out_of_range() {
    use google_cloud_spanner::value::ToValue;
    let ty = DataType::Timestamp(TimeUnit::Nanosecond, Some(TIMESTAMP_TZ.into()));
    let malformed = "2024-13-45T99:99:99Z".to_value();
    let err = build_array(&ty, &[Some(&malformed)]).unwrap_err();
    assert_eq!(err.status, Status::InvalidData);
    assert!(err.message.contains("cannot decode"), "{}", err.message);
    assert!(
        !err.message.contains("outside the range"),
        "{}",
        err.message
    );
}

/// The strict-decode policy applies inside `ARRAY` columns too: a present column value that is
/// not a wire list, and a present list *element* that cannot be decoded as the element type,
/// are both loud errors — never silent nulls.
#[test]
fn list_with_undecodable_element_errors() {
    use google_cloud_spanner::value::ToValue;
    let list_of_int64 = DataType::List(Arc::new(Field::new(LIST_ITEM, DataType::Int64, true)));

    // A present element that is not a valid INT64 errors via the element's scalar arm.
    let bad_element = vec!["42".to_value(), "not-an-int".to_value()].to_value();
    let err = build_array(&list_of_int64, &[Some(&bad_element)]).unwrap_err();
    assert_eq!(err.status, Status::InvalidData);
    assert!(
        err.message.contains("INT64") && err.message.contains("not-an-int"),
        "{}",
        err.message
    );

    // A present column value that is not a wire list at all errors as an ARRAY decode failure.
    let not_a_list = "scalar".to_value();
    let err = build_array(&list_of_int64, &[Some(&not_a_list)]).unwrap_err();
    assert_eq!(err.status, Status::InvalidData);
    assert!(
        err.message.contains("ARRAY") && err.message.contains("scalar"),
        "{}",
        err.message
    );
}

/// The strict-decode policy applies inside `STRUCT` columns too: a present column value that is
/// not a wire list (the positional encoding), and a present *field* value that cannot be
/// decoded as the field's type, are both loud errors — never silent nulls. This also covers
/// recursion: an undecodable struct nested inside a list errors as well.
#[test]
fn struct_with_undecodable_field_errors() {
    use google_cloud_spanner::value::ToValue;
    let struct_type = DataType::Struct(Fields::from(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]));

    // A present field value that is not a valid INT64 errors via the field's scalar arm.
    // (Spanner encodes struct values positionally, as a wire list in field order.)
    let bad_field = vec!["nope".to_value(), "abc".to_value()].to_value();
    let err = build_array(&struct_type, &[Some(&bad_field)]).unwrap_err();
    assert_eq!(err.status, Status::InvalidData);
    assert!(
        err.message.contains("INT64") && err.message.contains("nope"),
        "{}",
        err.message
    );

    // A present column value that is not a wire list errors as a STRUCT decode failure.
    let not_a_struct = "scalar".to_value();
    let err = build_array(&struct_type, &[Some(&not_a_struct)]).unwrap_err();
    assert_eq!(err.status, Status::InvalidData);
    assert!(
        err.message.contains("STRUCT") && err.message.contains("scalar"),
        "{}",
        err.message
    );

    // Recursion: ARRAY<STRUCT<..>> whose element is not a struct errors too (the element
    // recurses into the struct arm).
    let list_of_struct = DataType::List(Arc::new(Field::new(LIST_ITEM, struct_type.clone(), true)));
    let bad_nested = vec!["not-a-struct".to_value()].to_value();
    let err = build_array(&list_of_struct, &[Some(&bad_nested)]).unwrap_err();
    assert_eq!(err.status, Status::InvalidData);
    assert!(
        err.message.contains("STRUCT") && err.message.contains("not-a-struct"),
        "{}",
        err.message
    );
}

/// Spanner's positional wire encoding addresses fields by index, so duplicate and empty field
/// names — both legal in a Spanner `STRUCT` — keep their own distinct values.
#[test]
fn positional_struct_keeps_duplicate_and_anonymous_fields() {
    use arrow_array::{Array, Int64Array};

    // STRUCT<x INT64, x INT64, `` INT64>: two same-named fields plus an anonymous one.
    let fields = Fields::from(vec![
        Field::new("x", DataType::Int64, true),
        Field::new("x", DataType::Int64, true),
        Field::new("", DataType::Int64, true),
    ]);
    let value = vec!["1".to_value(), "2".to_value(), "3".to_value()].to_value();
    let array = build_array(&DataType::Struct(fields), &[Some(&value)]).unwrap();
    let s = array.as_any().downcast_ref::<StructArray>().unwrap();

    // Each field keeps its own value; nothing collapses onto the first "x".
    let col = |i: usize| {
        s.column(i)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0)
    };
    assert_eq!((col(0), col(1), col(2)), (1, 2, 3));
}

#[test]
fn scalar_struct_nested_in_struct_preserves_schema_values_and_nulls() {
    use google_cloud_spanner::model::{StructType, Type as WireType, TypeCode, struct_type};

    let child = WireType::new().set_code(TypeCode::Struct).set_struct_type(
        StructType::new().set_fields([
            struct_type::Field::new()
                .set_name("id")
                .set_type(WireType::new().set_code(TypeCode::Int64)),
            struct_type::Field::new()
                .set_name("label")
                .set_type(WireType::new().set_code(TypeCode::String)),
        ]),
    );
    let outer: Type = WireType::new()
        .set_code(TypeCode::Struct)
        .set_struct_type(
            StructType::new()
                .set_fields([struct_type::Field::new().set_name("child").set_type(child)]),
        )
        .into();
    let field = arrow_field("outer", &outer, true, TimestampPrecision::default()).unwrap();
    let expected = DataType::Struct(Fields::from(vec![Field::new(
        "child",
        DataType::Struct(Fields::from(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("label", DataType::Utf8, true),
        ])),
        true,
    )]));
    assert_eq!(field.data_type(), &expected);
    let empty = build_array(field.data_type(), &[]).unwrap();
    assert_eq!(empty.data_type(), &expected);
    assert!(empty.is_empty());

    let value = vec![vec![7i64.to_value(), "nested".to_value()].to_value()].to_value();
    let null_child = vec![None::<i64>.to_value()].to_value();
    let array = build_array(field.data_type(), &[Some(&value), Some(&null_child), None]).unwrap();
    let outer = array.as_any().downcast_ref::<StructArray>().unwrap();
    let child = outer
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert_eq!(
        child
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .value(0),
        7
    );
    assert_eq!(
        child
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "nested"
    );
    assert!(child.is_null(1));
    assert!(outer.is_null(2));
}

/// Genuine wire NULLs still round-trip as nulls under strict list/struct decoding: SQL NULL
/// (and absent) column values become null list/struct slots, and NULL elements/fields become
/// null child slots — only *present* undecodable values error.
#[test]
fn list_and_struct_nulls_round_trip_as_nulls() {
    use arrow_array::Array;
    use google_cloud_spanner::value::ToValue;
    let sql_null = None::<i64>.to_value();

    // List column: NULL / absent column values and a NULL element inside a present list.
    let list_of_int64 = DataType::List(Arc::new(Field::new(LIST_ITEM, DataType::Int64, true)));
    let with_null_element = vec![1i64.to_value(), sql_null.clone(), 3i64.to_value()].to_value();
    let array = build_array(
        &list_of_int64,
        &[Some(&sql_null), None, Some(&with_null_element)],
    )
    .unwrap();
    let list = array.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(list.len(), 3);
    assert!(list.is_null(0)); // SQL NULL column value
    assert!(list.is_null(1)); // missing slot
    assert!(!list.is_null(2));
    let elements = list.value(2);
    let ints = elements
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap();
    assert_eq!(ints.len(), 3);
    assert_eq!(ints.value(0), 1);
    assert!(ints.is_null(1)); // NULL element stays a null slot
    assert_eq!(ints.value(2), 3);

    // Struct column: NULL / absent column values and a NULL field inside a present struct.
    let struct_type = DataType::Struct(Fields::from(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]));
    let with_null_field = vec![7i64.to_value(), sql_null.clone()].to_value();
    let array = build_array(
        &struct_type,
        &[Some(&sql_null), None, Some(&with_null_field)],
    )
    .unwrap();
    let structs = array.as_any().downcast_ref::<StructArray>().unwrap();
    assert_eq!(structs.len(), 3);
    assert!(structs.is_null(0)); // SQL NULL column value
    assert!(structs.is_null(1)); // missing slot
    assert!(!structs.is_null(2));
    let ids = structs
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap();
    let names = structs
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(ids.value(2), 7);
    assert!(names.is_null(2)); // NULL field stays a null slot
}

/// The option values of `spanner.max_timestamp_precision` parse to the two modes; the empty
/// string resets to the default; anything else is rejected loudly.
#[test]
fn timestamp_precision_option_parses_and_round_trips() {
    let parse = |s: &str| TimestampPrecision::parse_option(OptionValue::String(s.to_string()));
    assert_eq!(
        parse("nanoseconds_error_on_overflow").unwrap(),
        TimestampPrecision::NanosecondsErrorOnOverflow
    );
    assert_eq!(
        parse("microseconds").unwrap(),
        TimestampPrecision::Microseconds
    );
    // "" unsets — i.e. resets to the default mode (the staleness-option convention).
    assert_eq!(parse("").unwrap(), TimestampPrecision::default());
    assert_eq!(
        parse(" microseconds ").unwrap(),
        TimestampPrecision::Microseconds
    ); // trimmed
    assert_eq!(
        TimestampPrecision::default(),
        TimestampPrecision::NanosecondsErrorOnOverflow
    );
    // Round-trip strings match what parse accepts.
    for mode in [
        TimestampPrecision::NanosecondsErrorOnOverflow,
        TimestampPrecision::Microseconds,
    ] {
        assert_eq!(parse(mode.as_str()).unwrap(), mode);
    }
    // Snowflake's silent-wraparound `nanoseconds` value is deliberately not offered: wrapped
    // timestamps are silent data corruption. It must be rejected, not quietly accepted.
    for bad in ["nanoseconds", "millis", "seconds", "true"] {
        let error = parse(bad).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "value {bad:?}");
        assert!(
            error.message.contains("spanner.max_timestamp_precision")
                && error.message.contains(bad),
            "{}",
            error.message
        );
    }
    // Non-string option values are rejected.
    assert_eq!(
        TimestampPrecision::parse_option(OptionValue::Int(1))
            .unwrap_err()
            .status,
        Status::InvalidArguments
    );
}

/// The schema mapping and the data path must agree on the `TIMESTAMP` unit in **both** modes —
/// the array a reader builds must have exactly the `DataType` the schema (query results,
/// `execute_schema`, the PLAN probe, `get_table_schema` — all of which go through
/// `arrow_field`) advertises, including for a `TIMESTAMP` nested inside an `ARRAY`.
#[test]
fn timestamp_schema_unit_matches_data_path_in_both_modes() {
    use google_cloud_spanner::value::ToValue;
    for (mode, unit) in [
        (
            TimestampPrecision::NanosecondsErrorOnOverflow,
            TimeUnit::Nanosecond,
        ),
        (TimestampPrecision::Microseconds, TimeUnit::Microsecond),
    ] {
        let expected = DataType::Timestamp(unit, Some(TIMESTAMP_TZ.into()));
        let field =
            arrow_field("t", &google_cloud_spanner::types::timestamp(), true, mode).unwrap();
        assert_eq!(field.data_type(), &expected, "{mode:?}");

        // The data path produces an array of exactly the advertised type.
        let v = "2024-01-15T12:34:56.789012Z".to_value();
        let array = build_array(field.data_type(), &[Some(&v)]).unwrap();
        assert_eq!(array.data_type(), &expected, "{mode:?}");

        // Nested: ARRAY<TIMESTAMP> carries the same unit on the item field.
        let list_field = arrow_field(
            "ts",
            &google_cloud_spanner::types::array(google_cloud_spanner::types::timestamp()),
            true,
            mode,
        )
        .unwrap();
        let DataType::List(item) = list_field.data_type() else {
            panic!("expected a List, got {:?}", list_field.data_type());
        };
        assert_eq!(item.data_type(), &expected, "{mode:?}");
    }
}

/// `parse_timestamp_micros` covers Spanner's whole 0001–9999 range and truncates
/// sub-microsecond digits toward negative infinity (a floor on the timeline, also for
/// pre-epoch instants).
#[test]
fn timestamps_to_epoch_micros_truncate_toward_negative_infinity() {
    assert_eq!(parse_timestamp_micros("1970-01-01T00:00:00Z"), Some(0));
    assert_eq!(
        parse_timestamp_micros("2024-01-15T12:34:56.789012Z"),
        Some(1_705_322_096_789_012)
    );
    // Sub-microsecond digits truncate: 789012345 ns → 789012 µs.
    assert_eq!(
        parse_timestamp_micros("2024-01-15T12:34:56.789012345Z"),
        Some(1_705_322_096_789_012)
    );
    // ... and toward negative infinity, not toward zero: 1 ns before the epoch is µs -1.
    assert_eq!(
        parse_timestamp_micros("1969-12-31T23:59:59.999999999Z"),
        Some(-1)
    );
    assert_eq!(
        parse_timestamp_micros("1970-01-01T00:00:00.000000999Z"),
        Some(0)
    );
    // Spanner's extremes — far outside the Arrow *nanosecond* range — are representable.
    let year_1500 = parse_timestamp_micros("1500-06-15T12:34:56.789012Z").unwrap();
    // Under floor semantics the sub-second part stays exactly 789012 µs, pre-epoch included.
    assert_eq!(year_1500.rem_euclid(1_000_000), 789_012);
    assert_eq!(
        parse_timestamp_micros("0001-01-01T00:00:00Z"),
        Some(-62_135_596_800_000_000)
    );
    assert_eq!(
        parse_timestamp_micros("9999-12-31T23:59:59.999999999Z"),
        Some(253_402_300_799_999_999)
    );
    assert_eq!(parse_timestamp_micros("nope"), None);
}

/// The `microseconds` mode reads values the default mode must reject (year 1500 / 9999),
/// truncates sub-microsecond precision, and keeps the strict-decode and null semantics.
#[test]
fn microsecond_mode_reads_full_range_and_default_mode_errors() {
    use arrow_array::Array;
    use google_cloud_spanner::value::ToValue;
    let micros_ty = DataType::Timestamp(TimeUnit::Microsecond, Some(TIMESTAMP_TZ.into()));
    let nanos_ty = DataType::Timestamp(TimeUnit::Nanosecond, Some(TIMESTAMP_TZ.into()));
    let null = None::<&str>.to_value();
    let year_1500 = "1500-06-15T12:34:56.789012Z".to_value();
    let year_9999 = "9999-01-01T00:00:00Z".to_value();
    let sub_micro = "2024-01-15T12:34:56.789012345Z".to_value();

    // Microseconds: everything decodes; sub-microsecond digits truncate; nulls survive.
    let array = build_array(
        &micros_ty,
        &[
            Some(&year_1500),
            Some(&year_9999),
            Some(&sub_micro),
            Some(&null),
            None,
        ],
    )
    .unwrap();
    let ts = array
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    assert_eq!(
        ts.value(0),
        parse_timestamp_micros("1500-06-15T12:34:56.789012Z").unwrap()
    );
    assert_eq!(
        ts.value(1),
        parse_timestamp_micros("9999-01-01T00:00:00Z").unwrap()
    );
    assert_eq!(ts.value(2), 1_705_322_096_789_012);
    assert!(ts.is_null(3));
    assert!(ts.is_null(4));

    // A malformed string is still a loud decode error in microseconds mode, never a null.
    let malformed = "2024-13-45T99:99:99Z".to_value();
    let err = build_array(&micros_ty, &[Some(&malformed)]).unwrap_err();
    assert_eq!(err.status, Status::InvalidData);

    // The default (nanoseconds) mode must reject the same out-of-range instants loudly,
    // pointing at the escape hatch.
    for value in [&year_1500, &year_9999] {
        let err = build_array(&nanos_ty, &[Some(value)]).unwrap_err();
        assert_eq!(err.status, Status::InvalidArguments);
        assert!(
            err.message.contains("spanner.max_timestamp_precision")
                && err.message.contains("microseconds"),
            "{}",
            err.message
        );
    }
}

/// A conversion error surfacing through a record batch names the failing **column** as well
/// as the offending value, so a wide result stays diagnosable.
#[test]
fn conversion_errors_name_the_column() {
    use google_cloud_spanner::value::ToValue;
    let field: FieldRef = Arc::new(Field::new(
        "CreatedAt",
        DataType::Timestamp(TimeUnit::Nanosecond, Some(TIMESTAMP_TZ.into())),
        true,
    ));
    let out_of_range = "9999-01-01T00:00:00Z".to_value();
    let err = build_column(&field, &[Some(&out_of_range)]).unwrap_err();
    assert_eq!(err.status, Status::InvalidArguments);
    assert!(
        err.message.contains("\"CreatedAt\"")
            && err.message.contains("9999-01-01T00:00:00Z")
            && err.message.contains("spanner.max_timestamp_precision"),
        "{}",
        err.message
    );
}

/// `INT64` arrives as a JSON string, so every `i64` — including magnitudes above 2^53 that an
/// `f64` cannot represent — round-trips exactly. A JSON number encoding must fail decoding,
/// because converting it through `f64` could lose integer precision.
#[test]
fn int64_string_round_trips_exactly_and_number_encoding_is_a_decode_error() {
    use arrow_array::Int64Array;
    use google_cloud_spanner::value::ToValue;

    // Ordinary value and a value above 2^53 both round-trip exactly via the string encoding.
    let above_2_53: i64 = (1i64 << 53) + 1;
    let small = 42i64.to_value();
    let big = above_2_53.to_value();
    let array = build_array(&DataType::Int64, &[Some(&small), Some(&big)]).unwrap();
    let ints = array.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(ints.value(0), 42);
    assert_eq!(ints.value(1), above_2_53);

    // A JSON-number encoding of the same magnitude would lose precision if cast through f64,
    // so it must fail decoding rather than return a rounded value.
    let as_number = (above_2_53 as f64).to_value();
    let err = build_array(&DataType::Int64, &[Some(&as_number)]).unwrap_err();
    assert_eq!(err.status, Status::InvalidData);
    assert!(err.message.contains("INT64"), "{}", err.message);
}

#[test]
fn struct_rejects_missing_and_extra_fields() {
    let ty = DataType::Struct(Fields::from(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]));
    for fields in [
        vec![],
        vec![1_i64.to_value()],
        vec![1_i64.to_value(), "name".to_value(), "extra".to_value()],
    ] {
        let count = fields.len();
        let value = Value::from(fields);
        let error = build_array(&ty, &[Some(&value)]).unwrap_err();
        assert_eq!(error.status, Status::InvalidData);
        assert!(
            error
                .message
                .contains(&format!("expected 2 fields, got {count}"))
        );
    }
}

#[test]
fn empty_structs_preserve_length_and_validity() {
    use arrow_array::Array;
    let ty = DataType::Struct(Fields::empty());
    let empty = Value::from(Vec::<Value>::new());
    let null = None::<i64>.to_value();
    let array = build_array(&ty, &[Some(&empty), Some(&null), None, Some(&empty)]).unwrap();
    assert_eq!(array.data_type(), &ty);
    assert_eq!(array.len(), 4);
    assert!(!array.is_null(0));
    assert!(array.is_null(1));
    assert!(array.is_null(2));
    assert!(!array.is_null(3));
    assert_eq!(build_array(&ty, &[]).unwrap().len(), 0);
}

#[test]
fn numeric_precision_limits_preserve_extrema_and_reject_overflow() {
    let maximum = 10_i128.pow(38) - 1;
    for (text, expected) in [
        ("99999999999999999999999999999.999999999", maximum),
        ("-99999999999999999999999999999.999999999", -maximum),
    ] {
        assert_eq!(parse_numeric_i128(text), Some(expected));
    }
    for text in [
        "100000000000000000000000000000",
        "-100000000000000000000000000000",
        "100000000000000000000000000000.000000001",
        "-100000000000000000000000000000.000000001",
    ] {
        assert_eq!(parse_numeric_i128(text), None, "{text}");
    }
}

#[test]
fn temporal_domain_excludes_year_zero_and_preserves_spanner_extrema() {
    assert_eq!(parse_date_days("0000-01-01"), None);
    assert_eq!(parse_timestamp_micros("0000-01-01T00:00:00Z"), None);
    assert_eq!(
        parse_timestamp_micros("0001-01-01T00:00:00Z"),
        Some(-62_135_596_800_000_000)
    );
    assert_eq!(
        parse_timestamp_micros("9999-12-31T23:59:59.999999999Z"),
        Some(253_402_300_799_999_999)
    );
}

#[test]
fn values_outside_spanner_domain_report_the_column_and_value() {
    for (ty, text) in [
        (DataType::Date32, "0000-01-01"),
        (
            DataType::Timestamp(TimeUnit::Microsecond, Some(TIMESTAMP_TZ.into())),
            "0000-01-01T00:00:00Z",
        ),
        (
            DataType::Timestamp(TimeUnit::Nanosecond, Some(TIMESTAMP_TZ.into())),
            "0000-01-01T00:00:00Z",
        ),
        (
            DataType::Timestamp(TimeUnit::Nanosecond, Some(TIMESTAMP_TZ.into())),
            "2024-01-01T00:00:00+00:00",
        ),
        (
            DataType::Decimal128(NUMERIC_PRECISION, NUMERIC_SCALE),
            "100000000000000000000000000000",
        ),
        (
            DataType::Decimal128(NUMERIC_PRECISION, NUMERIC_SCALE),
            "-100000000000000000000000000000",
        ),
    ] {
        let field = Arc::new(Field::new("bad_value", ty, true));
        let value = text.to_value();
        let error = build_column(&field, &[Some(&value)]).unwrap_err();
        assert_eq!(error.status, Status::InvalidData, "{}", error.message);
        assert!(error.message.contains("bad_value"), "{}", error.message);
        assert!(error.message.contains(text), "{}", error.message);
        assert!(
            !error
                .message
                .contains(crate::OPTION_MAX_TIMESTAMP_PRECISION)
        );
    }
}

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
        (types::proto_enum("pkg.Color"), DataType::Int64),
        (types::proto("pkg.Message"), DataType::Binary),
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
        (DataType::Float32, "abc".to_value(), "FLOAT32"),
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
        "2024-0a-15",
        "2024-01-1a",
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
        "2024-01-15T12:3a:56Z",
        "2024-01-15T12:34:5aZ",
        "2024-01-15T12:34:56.1aZ",
    ] {
        assert_eq!(parse_timestamp_micros(s), None, "{s}");
    }
    for s in [
        "",
        "-",
        "+",
        ".",
        "-.",
        "e5",
        ".e5",
        "1e",
        "1e+",
        "1e5e5",
        "1.2.3",
        "+-1",
        "--1",
        " 1",
        "1 ",
        "1.2a",
        "0x10",
        "NaN",
        "Infinity",
        "0.0000000001",
        "1e-10",
        "25e-10",
        "100000000000000000000000000000",
        "1e29",
        "1e2147483648",
    ] {
        assert_eq!(parse_numeric_i128(s), None, "{s:?}");
    }
}

/// The wire format allows a `+`, an empty integer or fraction part and an exponent, though
/// Spanner sends `[-]digits[.digits]`. Padding zeros don't count toward the 38 digits.
#[test]
fn numeric_accepts_every_form_of_the_wire_format() {
    for (s, unscaled) in [
        ("+3", 3_000_000_000),
        (".5", 500_000_000),
        ("-.5", -500_000_000),
        ("5.", 5_000_000_000),
        ("-5.", -5_000_000_000),
        ("1e5", 100_000_000_000_000),
        ("1E+5", 100_000_000_000_000),
        ("+.5e-2", 5_000_000),
        ("-1.e2", -100_000_000_000),
        ("20e-10", 2),
        ("1.5000000000", 1_500_000_000),
        ("0e2147483648", 0),
        ("-0.000", 0),
        ("0000000000000000000000000000000000000001.5", 1_500_000_000),
        (
            "99999999999999999999999999999999999999e-9",
            10_i128.pow(38) - 1,
        ),
        ("1e28", 10_i128.pow(37)),
    ] {
        assert_eq!(parse_numeric_i128(s), Some(unscaled), "{s:?}");
    }
}

/// The decoders against independent references over random and malformed input: chrono for
/// DATE and TIMESTAMP, and decimal point shifting on strings for NUMERIC.
mod differential {
    use chrono::{DateTime, Datelike, NaiveDate, Timelike, Utc};
    use proptest::prelude::*;

    use super::*;

    fn config() -> ProptestConfig {
        ProptestConfig {
            cases: 4096,
            failure_persistence: None,
            ..ProptestConfig::default()
        }
    }

    /// RFC 3339 `full-date` in Spanner's years 1-9999.
    fn reference_date(s: &str) -> Option<i32> {
        let b = s.as_bytes();
        let shaped = b.len() == 10
            && b.iter().enumerate().all(|(i, c)| match i {
                4 | 7 => *c == b'-',
                _ => c.is_ascii_digit(),
            });
        let date = NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()?;
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        (shaped && date.year() >= 1).then(|| (date - epoch).num_days() as i32)
    }

    /// RFC 3339 as Spanner sends it: uppercase `T` and `Z`, at most nine fractional digits, no
    /// leap second, years 1-9999. chrono accepts the rest of RFC 3339.
    fn reference_timestamp(s: &str) -> Option<DateTime<Utc>> {
        let time = s.get(11..)?.strip_suffix('Z')?;
        let fraction = time.split_once('.').map_or("", |(_, fraction)| fraction);
        let t = DateTime::parse_from_rfc3339(s).ok()?.to_utc();
        let spanner = s.as_bytes()[10] == b'T'
            && fraction.len() <= 9
            && t.year() >= 1
            && t.nanosecond() < 1_000_000_000;
        spanner.then_some(t)
    }

    /// The NUMERIC wire format, `[+-]` then digits with an optional `.` and at least one digit,
    /// then an optional `[eE][+-]digits`, as an unscaled value at scale 9 if it fits exactly.
    fn reference_numeric(s: &str) -> Option<i128> {
        let (negative, s) = match s.strip_prefix('-') {
            Some(rest) => (true, rest),
            None => (false, s.strip_prefix('+').unwrap_or(s)),
        };
        let (mantissa, exponent) = match s.find(['e', 'E']) {
            Some(i) => {
                let exponent = &s[i + 1..];
                let digits = exponent.strip_prefix(['+', '-']).unwrap_or(exponent);
                if digits.is_empty() || !digits.bytes().all(|c| c.is_ascii_digit()) {
                    return None;
                }
                // Past any representable value, only the exponent's sign matters.
                let huge = if exponent.starts_with('-') {
                    -1 << 40
                } else {
                    1 << 40
                };
                (&s[..i], exponent.parse::<i64>().unwrap_or(huge))
            }
            None => (s, 0),
        };
        let (int, frac) = mantissa.split_once('.').unwrap_or((mantissa, ""));
        let digits = format!("{int}{frac}");
        if digits.is_empty() || !digits.bytes().all(|c| c.is_ascii_digit()) {
            return None;
        }
        if digits.bytes().all(|c| c == b'0') {
            return Some(0);
        }
        // The value is `digits * 10^(exponent - frac.len())`.
        let shift = exponent + 9 - frac.len() as i64;
        let unscaled = if shift >= 0 {
            format!(
                "{digits}{}",
                "0".repeat(usize::try_from(shift.min(40)).unwrap())
            )
        } else {
            let kept = (digits.len() as i64 + shift).max(0) as usize;
            let (kept, dropped) = digits.split_at(kept);
            if dropped.bytes().any(|c| c != b'0') {
                return None;
            }
            kept.to_string()
        };
        let unscaled = unscaled.trim_start_matches('0');
        if unscaled.len() > 38 {
            return None;
        }
        let value: i128 = unscaled.parse().unwrap();
        Some(if negative { -value } else { value })
    }

    fn check_date(s: &str) -> std::result::Result<(), TestCaseError> {
        prop_assert_eq!(parse_date_days(s), reference_date(s), "{:?}", s);
        Ok(())
    }

    fn check_timestamp(s: &str) -> std::result::Result<(), TestCaseError> {
        let expected = reference_timestamp(s);
        let micros = expected.map(|t| t.timestamp_micros());
        prop_assert_eq!(parse_timestamp_micros(s), micros, "{:?}", s);
        let nanos = expected.and_then(|t| t.timestamp_nanos_opt());
        prop_assert_eq!(parse_timestamp_nanos(s), nanos, "{:?}", s);
        Ok(())
    }

    fn check_numeric(s: &str) -> std::result::Result<(), TestCaseError> {
        prop_assert_eq!(parse_numeric_i128(s), reference_numeric(s), "{:?}", s);
        Ok(())
    }

    /// `|unscaled|` written with an exponent of `exponent`, moving the decimal point to match.
    fn with_exponent(unscaled: u128, exponent: i32) -> String {
        let digits = unscaled.to_string();
        let after_point = 9 + exponent;
        if after_point <= 0 {
            return format!("{digits}{}e{exponent}", "0".repeat(-after_point as usize));
        }
        let padded = format!("{digits:0>width$}", width = after_point as usize);
        let (int, frac) = padded.split_at(padded.len() - after_point as usize);
        format!("{int}.{frac}e{exponent}")
    }

    proptest! {
        #![proptest_config(config())]

        #[test]
        fn dates_match_chrono(
            (year, month, day) in (0_u32..=9999, 0_u32..=13, 0_u32..=32),
            noise in "[0-9 +-]?[0-9]{3,5}-[0-9]{1,3}-[0-9]{1,3}[ T]?",
        ) {
            let text = format!("{year:04}-{month:02}-{day:02}");
            check_date(&text)?;
            check_date(&noise)?;
        }

        #[test]
        fn timestamps_match_chrono(
            date in (1_i32..=9999, 1_u32..=12, 1_u32..=31)
                .prop_filter_map("a real date", |(y, m, d)| NaiveDate::from_ymd_opt(y, m, d)),
            (hour, minute, second) in (0_u32..24, 0_u32..60, 0_u32..60),
            (nanos, digits) in (0_u32..1_000_000_000, 0_usize..=9),
        ) {
            let fraction = format!(".{nanos:09}");
            let fraction = if digits == 0 { "" } else { &fraction[..=digits] };
            let text = format!("{date}T{hour:02}:{minute:02}:{second:02}{fraction}Z");
            prop_assert!(parse_timestamp_micros(&text).is_some(), "{:?}", text);
            check_timestamp(&text)?;
        }

        #[test]
        fn malformed_timestamps_match_chrono(
            (year, month, day) in (0_u32..=9999, 0_u32..=13, 0_u32..=32),
            (hour, minute, second) in (0_u32..=25, 0_u32..=61, 0_u32..=61),
            separator in "[Tt ]",
            fraction in "(\\.[0-9]{0,11})?",
            zone in "Z|z|[+-][01][0-9]:[0-6][0-9]|",
            junk in "[0-9TZtz:. +-]{0,32}",
        ) {
            let text = format!(
                "{year:04}-{month:02}-{day:02}{separator}\
                 {hour:02}:{minute:02}:{second:02}{fraction}{zone}"
            );
            check_timestamp(&text)?;
            check_timestamp(&junk)?;
        }

        #[test]
        fn numerics_match_the_reference(
            (negative, digits) in (any::<bool>(), 0_u32..=38),
            magnitude in any::<u128>(),
            exponent in -45_i32..45,
            noise in "[+-]?[0-9]{0,32}(\\.[0-9]{0,12})?([eE][+-]?[0-9]{0,3})?",
            junk in "[0-9.eE+ -]{0,45}",
        ) {
            // Uniform in digit count, so short values are as common as long ones.
            let unscaled = magnitude % 10_u128.pow(digits).max(1);
            let signed = if negative { -(unscaled as i128) } else { unscaled as i128 };
            let sign = if negative { "-" } else { "" };
            let int = unscaled / 1_000_000_000;
            let frac = format!("{:09}", unscaled % 1_000_000_000);
            let frac = frac.trim_end_matches('0');
            let canonical = if frac.is_empty() {
                format!("{sign}{int}")
            } else {
                format!("{sign}{int}.{frac}")
            };
            prop_assert_eq!(parse_numeric_i128(&canonical), Some(signed), "{:?}", canonical);
            let scientific = format!("{sign}{}", with_exponent(unscaled, exponent));
            prop_assert_eq!(parse_numeric_i128(&scientific), Some(signed), "{:?}", scientific);
            check_numeric(&canonical)?;
            check_numeric(&scientific)?;
            check_numeric(&noise)?;
            check_numeric(&junk)?;
        }
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

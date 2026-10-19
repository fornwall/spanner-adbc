use std::sync::Arc;

use adbc_core::error::Status;
use arrow_array::{
    Date32Array, Decimal128Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray,
    ListArray, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow_schema::{Field, Schema};
use google_cloud_spanner::statement::Statement;

use super::*;

/// Bind every column of `batch` at `row` as `@p0`, `@p1`, ...
fn bind_row(batch: &RecordBatch, row: usize) -> Result<StatementBuilder> {
    let names: Vec<String> = (0..batch.num_columns()).map(|i| format!("p{i}")).collect();
    let sql = format!(
        "SELECT {}",
        names
            .iter()
            .map(|name| format!("@{name}"))
            .collect::<Vec<_>>()
            .join(", ")
    );
    bind_params(Statement::builder(sql), &names, batch, row)
}

/// Debug rendering of the statement [`bind_row`] builds, the only external view of its
/// parameters. INT64 renders as `StringValue("<digits>")`, FLOAT64 as `NumberValue`, BYTES as
/// base64 in a `StringValue`, and NULL as `NullValue`.
fn bound(batch: &RecordBatch, row: usize) -> String {
    format!("{:?}", bind_row(batch, row).unwrap().build())
}

#[test]
fn binds_narrow_integers_as_int64() {
    let b = batch(
        vec![
            Field::new("a", DataType::Int16, true),
            Field::new("b", DataType::Int32, true),
        ],
        vec![
            Arc::new(arrow_array::Int16Array::from(vec![Some(7i16), None])),
            Arc::new(Int32Array::from(vec![Some(9i32), None])),
        ],
    );
    let row0 = bound(&b, 0);
    assert!(row0.contains(r#"StringValue("7")"#), "Int16 7: {row0}");
    assert!(row0.contains(r#"StringValue("9")"#), "Int32 9: {row0}");
    assert!(!row0.contains("NumberValue"), "{row0}");
    let row1 = bound(&b, 1);
    assert_eq!(row1.matches("NullValue(NullValue)").count(), 2, "{row1}");
    assert!(!row1.contains("StringValue"), "{row1}");
}

#[test]
fn binds_unsigned_integers_as_int64() {
    let b = batch(
        vec![
            Field::new("a", DataType::UInt8, true),
            Field::new("b", DataType::UInt16, true),
            Field::new("c", DataType::UInt32, true),
        ],
        vec![
            Arc::new(arrow_array::UInt8Array::from(vec![Some(u8::MAX), None])),
            Arc::new(arrow_array::UInt16Array::from(vec![Some(u16::MAX), None])),
            Arc::new(arrow_array::UInt32Array::from(vec![Some(u32::MAX), None])),
        ],
    );
    let row0 = bound(&b, 0);
    assert!(row0.contains(r#"StringValue("4294967295")"#), "{row0}");
    assert!(row0.contains(r#"StringValue("255")"#), "{row0}");
    assert!(row0.contains(r#"StringValue("65535")"#), "{row0}");
    let row1 = bound(&b, 1);
    assert_eq!(row1.matches("NullValue(NullValue)").count(), 3, "{row1}");
}

#[test]
fn binds_half_floats_as_doubles() {
    // Every f16 converts to f64 without rounding.
    let b = batch(
        vec![Field::new("v", DataType::Float16, true)],
        vec![Arc::new(arrow_array::Float16Array::from(vec![
            Some(<Float16Type as ArrowPrimitiveType>::Native::from_f32(-1.5)),
            None,
        ]))],
    );
    let row0 = bound(&b, 0);
    assert!(row0.contains("-1.5"), "{row0}");
    let row1 = bound(&b, 1);
    assert!(row1.contains("NullValue(NullValue)"), "{row1}");
}

#[test]
fn binds_fixed_size_binary_as_bytes() {
    let values: Vec<Option<&[u8]>> = vec![Some(b"abcd"), None];
    let arr =
        arrow_array::FixedSizeBinaryArray::try_from_sparse_iter_with_size(values.into_iter(), 4)
            .unwrap();
    let b = batch(
        vec![Field::new("v", DataType::FixedSizeBinary(4), true)],
        vec![Arc::new(arr)],
    );
    let dbg = format!("{:?}", insert_mutation("t", &b, 0).unwrap());
    // "abcd" in base64.
    assert!(
        dbg.contains("StringValue") && dbg.contains("YWJjZA=="),
        "{dbg}"
    );
    let dbg = format!("{:?}", insert_mutation("t", &b, 1).unwrap());
    assert!(dbg.contains("NullValue"), "null fixed cell: {dbg}");
}

/// A nullable field tagged with the `arrow.json` extension, as the read path produces for JSON
/// columns.
fn json_field(name: &str, data_type: DataType) -> Field {
    Field::new(name, data_type, true).with_metadata(std::collections::HashMap::from([(
        "ARROW:extension:name".to_string(),
        "arrow.json".to_string(),
    )]))
}

#[test]
fn binds_json_tagged_strings_as_json_params() {
    let b = batch(
        vec![
            json_field("doc", DataType::Utf8),
            Field::new("plain", DataType::Utf8, true),
        ],
        vec![
            Arc::new(StringArray::from(vec![Some(r#"{"a":1}"#), None])),
            Arc::new(StringArray::from(vec![Some(r#"{"a":1}"#), None])),
        ],
    );
    let dbg = bound(&b, 0);
    assert!(dbg.contains(r#""p0": Type(Type { code: Json"#), "{dbg}");
    assert!(dbg.contains(r#""p1": Type(Type { code: String"#), "{dbg}");
    // A null keeps the JSON type.
    let dbg = bound(&b, 1);
    assert!(dbg.contains(r#""p0": Type(Type { code: Json"#), "{dbg}");
}

#[test]
fn null_parameters_keep_their_arrow_logical_types() {
    for (data_type, expected) in [
        (DataType::Boolean, types::bool()),
        (DataType::Int8, types::int64()),
        (DataType::Int16, types::int64()),
        (DataType::Int32, types::int64()),
        (DataType::Int64, types::int64()),
        (DataType::UInt8, types::int64()),
        (DataType::UInt16, types::int64()),
        (DataType::UInt32, types::int64()),
        (DataType::Float16, types::float32()),
        (DataType::Float32, types::float32()),
        (DataType::Float64, types::float64()),
        (DataType::Utf8, types::string()),
        (DataType::LargeUtf8, types::string()),
        (DataType::Utf8View, types::string()),
        (DataType::Binary, types::bytes()),
        (DataType::LargeBinary, types::bytes()),
        (DataType::BinaryView, types::bytes()),
        (DataType::FixedSizeBinary(4), types::bytes()),
        (DataType::Date32, types::date()),
        (DataType::Date64, types::date()),
        (
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            types::timestamp(),
        ),
        (DataType::Decimal128(38, 9), types::numeric()),
        (
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            types::string(),
        ),
        (
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            types::array(types::string()),
        ),
        (
            DataType::LargeList(Arc::new(Field::new("item", DataType::Int64, true))),
            types::array(types::int64()),
        ),
    ] {
        let null = arrow_array::new_null_array(&data_type, 1);
        let b = batch(vec![Field::new("p", data_type.clone(), true)], vec![null]);
        let actual = bound(&b, 0);
        assert!(
            actual.contains(&format!("\"p0\": {expected:?}")),
            "{data_type}: {actual}"
        );
    }
    // Arrow Null leaves the type to the SQL.
    let b = batch(
        vec![Field::new("p", DataType::Null, true)],
        vec![Arc::new(arrow_array::NullArray::new(1))],
    );
    let actual = bound(&b, 0);
    assert!(!actual.contains("\"p0\": Type"), "{actual}");
}

#[test]
fn binds_json_tagged_list_elements_as_json_arrays() {
    let docs = {
        let mut b =
            arrow_array::builder::ListBuilder::new(arrow_array::builder::StringBuilder::new())
                .with_field(Arc::new(json_field("item", DataType::Utf8)));
        b.values().append_value(r#"{"a":1}"#);
        b.values().append_null();
        b.append(true);
        b.finish()
    };
    let field = Field::new(
        "docs",
        DataType::List(Arc::new(json_field("item", DataType::Utf8))),
        true,
    );
    let b = batch(vec![field], vec![Arc::new(docs)]);
    let dbg = bound(&b, 0);
    assert!(
        dbg.contains(r#""p0": Type(Type { code: Array"#)
            && dbg.contains("array_element_type: Some(Type { code: Json"),
        "no ARRAY<JSON> param type in: {dbg}"
    );
}

#[test]
fn json_tag_is_ignored_on_non_string_storage() {
    // `arrow.json` is only defined over string storage.
    let b = batch(
        vec![json_field("n", DataType::Int64)],
        vec![Arc::new(Int64Array::from(vec![Some(7)]))],
    );
    let dbg = bound(&b, 0);
    assert!(!dbg.contains("Json"), "{dbg}");
}

#[test]
fn creates_json_columns_for_tagged_ingest_fields() {
    let schema = Schema::new(vec![
        json_field("doc", DataType::Utf8),
        Field::new(
            "docs",
            DataType::List(Arc::new(json_field("item", DataType::Utf8))),
            true,
        ),
        json_field(
            "cat",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        ),
        Field::new("plain", DataType::Utf8, true),
    ]);
    let sql = create_table_sql("t", None, &schema, false).unwrap();
    assert!(
        sql.contains("`doc` JSON, `docs` ARRAY<JSON>, `cat` JSON, `plain` STRING(MAX)"),
        "unexpected DDL: {sql}"
    );
}

#[test]
fn maps_arrow_to_spanner_column_types() {
    assert_eq!(spanner_column_type(&DataType::Int32).unwrap(), "INT64");
    assert_eq!(spanner_column_type(&DataType::Int64).unwrap(), "INT64");
    assert_eq!(spanner_column_type(&DataType::Float32).unwrap(), "FLOAT32");
    assert_eq!(spanner_column_type(&DataType::Float16).unwrap(), "FLOAT32");
    assert_eq!(spanner_column_type(&DataType::Utf8).unwrap(), "STRING(MAX)");
    assert_eq!(
        spanner_column_type(&DataType::LargeBinary).unwrap(),
        "BYTES(MAX)"
    );
    assert_eq!(
        spanner_column_type(&DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap(),
        "TIMESTAMP"
    );
    let list = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    assert_eq!(spanner_column_type(&list).unwrap(), "ARRAY<INT64>");
    assert_eq!(spanner_column_type(&DataType::UInt8).unwrap(), "INT64");
    assert_eq!(spanner_column_type(&DataType::UInt16).unwrap(), "INT64");
    assert_eq!(spanner_column_type(&DataType::UInt32).unwrap(), "INT64");
    assert_eq!(
        spanner_column_type(&DataType::FixedSizeBinary(4)).unwrap(),
        "BYTES(MAX)"
    );
    // u64::MAX doesn't fit INT64.
    assert!(spanner_column_type(&DataType::UInt64).is_err());
}

#[test]
fn dictionary_json_arrays_keep_their_type_when_creating_ingest_tables() {
    use arrow_array::builder::{GenericListBuilder, StringBuilder};

    fn check<O: arrow_array::OffsetSizeTrait>() {
        let mut lists = GenericListBuilder::<O, _>::new(StringBuilder::new())
            .with_field(Arc::new(json_field("item", DataType::Utf8)));
        lists.values().append_value(r#"{"a":1}"#);
        lists.append(true);
        let dictionary = arrow_array::DictionaryArray::new(
            Int32Array::from(vec![Some(0), None]),
            Arc::new(lists.finish()),
        );
        let b = batch(
            vec![Field::new("docs", dictionary.data_type().clone(), true)],
            vec![Arc::new(dictionary)],
        );
        assert_eq!(
            create_table_sql("events", None, b.schema().as_ref(), false).unwrap(),
            "CREATE TABLE `events` (`docs` ARRAY<JSON>)"
        );
        for row in 0..2 {
            assert!(bound(&b, row).contains("array_element_type: Some(Type { code: Json"));
        }
    }
    check::<i32>();
    check::<i64>();
}

#[test]
fn builds_create_table_sql() {
    let schema = Schema::new(vec![
        Field::new("idx", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]);
    // No PRIMARY KEY: Spanner adds a hidden rowid key.
    assert_eq!(
        create_table_sql("my_table", None, &schema, false).unwrap(),
        "CREATE TABLE `my_table` (`idx` INT64, `name` STRING(MAX))"
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
fn create_table_sql_names_the_column_it_cannot_map() {
    let schema = Schema::new(vec![
        Field::new("ok", DataType::Int64, true),
        Field::new("weird", DataType::UInt64, true),
    ]);
    let err = create_table_sql("t", None, &schema, false).unwrap_err();
    assert_eq!(err.status, Status::InvalidArguments);
    assert!(
        err.message.starts_with("ingest column \"weird\": "),
        "{}",
        err.message
    );
    assert!(err.message.contains("UInt64"), "{}", err.message);
}

#[test]
fn ingest_ddl_rejects_types_the_value_encoder_cannot_bind() {
    let list = |data_type| DataType::List(Arc::new(Field::new("item", data_type, true)));
    let dictionary =
        |data_type| DataType::Dictionary(Box::new(DataType::Int32), Box::new(data_type));
    for data_type in [
        DataType::Decimal128(38, 39),
        dictionary(DataType::Decimal128(38, 39)),
        list(DataType::Decimal128(38, 39)),
        list(list(DataType::Int64)),
        dictionary(list(list(DataType::Int64))),
        list(dictionary(DataType::Utf8)),
    ] {
        let schema = Schema::new(vec![Field::new("bad", data_type.clone(), true)]);
        let error = create_table_sql("t", None, &schema, false).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        assert!(
            error.message.starts_with("ingest column \"bad\": "),
            "{data_type}: {error}"
        );
    }
}

#[test]
fn builds_insert_mutations() {
    let b = batch(
        vec![
            Field::new("Id", DataType::Int64, false),
            Field::new("Name", DataType::Utf8, true),
        ],
        vec![
            Arc::new(Int64Array::from(vec![Some(7), Some(8)])),
            Arc::new(StringArray::from(vec![Some("Alice"), None])),
        ],
    );
    let dbg = format!("{:?}", insert_mutation("Users", &b, 0).unwrap());
    for needle in [
        "Insert",
        "\"Users\"",
        "\"Id\"",
        "\"Name\"",
        r#"StringValue("7")"#,
        r#"StringValue("Alice")"#,
    ] {
        assert!(dbg.contains(needle), "missing {needle} in: {dbg}");
    }
    let dbg = format!("{:?}", insert_mutation("Users", &b, 1).unwrap());
    assert!(dbg.contains("NullValue"), "no NULL for row 1: {dbg}");
    // Mutations carry raw names, so reserved words need no quoting.
    let odd = batch(
        vec![Field::new("index", DataType::Int64, false)],
        vec![Arc::new(Int64Array::from(vec![42]))],
    );
    let dbg = format!("{:?}", insert_mutation("create", &odd, 0).unwrap());
    assert!(
        dbg.contains("\"create\"") && dbg.contains("\"index\""),
        "{dbg}"
    );
    let bad = batch(
        vec![Field::new("x", DataType::UInt64, false)],
        vec![Arc::new(arrow_array::UInt64Array::from(vec![1u64]))],
    );
    assert!(insert_mutation("t", &bad, 0).is_err());
}

#[test]
fn json_tagged_strings_ingest_as_plain_strings_in_mutations() {
    // Spanner types mutation values from the target column, so JSON travels as a plain string.
    let b = batch(
        vec![json_field("doc", DataType::Utf8)],
        vec![Arc::new(StringArray::from(vec![Some(r#"{"a":1}"#)]))],
    );
    let dbg = format!("{:?}", insert_mutation("t", &b, 0).unwrap());
    assert!(dbg.contains(r#"StringValue("{\"a\":1}")"#), "{dbg}");
}

/// The pre-DDL check must fail exactly where building the mutations would, including on a later
/// row and inside dictionaries and lists.
#[test]
fn ingest_value_check_fails_where_mutation_building_would() {
    use arrow_array::{ArrayRef, Date64Array, DictionaryArray};

    let date_dictionary = DictionaryArray::new(
        Int32Array::from(vec![0, 1]),
        Arc::new(Date32Array::from(vec![0, i32::MAX])) as ArrayRef,
    );
    let bad_second_rows: Vec<ArrayRef> = vec![
        Arc::new(Date32Array::from(vec![0, i32::MAX])),
        Arc::new(Date64Array::from(vec![0, 1])),
        Arc::new(TimestampSecondArray::from(vec![0, i64::MAX])),
        Arc::new(date_dictionary),
        Arc::new(ListArray::from_iter_primitive::<Date32Type, _, _>([
            Some(vec![Some(0)]),
            Some(vec![Some(i32::MAX)]),
        ])),
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
    let unsupported = batch(
        vec![Field::new("x", DataType::UInt64, false)],
        vec![Arc::new(arrow_array::UInt64Array::from(vec![1u64]))],
    );
    assert!(check_ingest_values(&unsupported).is_err());
}

#[test]
fn mutation_table_joins_schema_with_a_dot() {
    assert_eq!(mutation_table(None, "Users"), "Users");
    assert_eq!(mutation_table(Some(""), "Users"), "Users");
    assert_eq!(mutation_table(Some("app"), "Users"), "app.Users");
}

fn batch(fields: Vec<Field>, columns: Vec<arrow_array::ArrayRef>) -> RecordBatch {
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

fn int_batch(names: &[&str]) -> RecordBatch {
    batch(
        names
            .iter()
            .map(|n| Field::new(*n, DataType::Int64, false))
            .collect(),
        names
            .iter()
            .map(|_| Arc::new(Int64Array::from(vec![1])) as arrow_array::ArrayRef)
            .collect(),
    )
}

#[test]
fn resolves_parameters_by_name_when_columns_match() {
    let b = int_batch(&["b", "a"]);
    assert_eq!(
        resolve_parameter_names("SELECT @a, @b", &b, true).unwrap(),
        vec!["b", "a"],
    );
}

#[test]
fn resolves_parameters_positionally_by_default() {
    let b = int_batch(&["res", "other"]);
    assert_eq!(
        resolve_parameter_names("INSERT INTO t VALUES (@p1, @p2)", &b, false).unwrap(),
        vec!["p1", "p2"],
    );
    let one = int_batch(&["res"]);
    assert_eq!(
        resolve_parameter_names("INSERT INTO t VALUES (@p1)", &one, false).unwrap(),
        vec!["p1"],
    );
}

#[test]
fn by_name_mode_binds_by_name_and_leaves_extra_params_unbound() {
    // @c stays unbound; Spanner rejects it at execution time.
    let b = int_batch(&["b", "a"]);
    assert_eq!(
        resolve_parameter_names("SELECT @a, @b, @c", &b, true).unwrap(),
        vec!["b", "a"],
    );
}

#[test]
fn by_name_mode_rejects_an_unmatched_column_naming_the_column() {
    let b = int_batch(&["a", "x"]);
    let err = resolve_parameter_names("SELECT @a, @b", &b, true).unwrap_err();
    assert_eq!(err.status, Status::InvalidArguments);
    assert!(
        err.message.contains("cannot bind column \"x\""),
        "{}",
        err.message
    );
    assert!(err.message.contains("[\"a\", \"b\"]"), "{}", err.message);
}

#[test]
fn positional_binding_ignores_coincidental_name_matches() {
    let b = int_batch(&["b", "a"]);
    assert_eq!(
        resolve_parameter_names("SELECT @a, @b", &b, false).unwrap(),
        vec!["a", "b"],
    );
    assert_eq!(
        resolve_parameter_names("SELECT @a, @b", &b, true).unwrap(),
        vec!["b", "a"],
    );
}

#[test]
fn positional_mode_still_requires_matching_counts() {
    // A column named after a parameter doesn't excuse a count mismatch.
    let b = int_batch(&["a"]);
    let err = resolve_parameter_names("SELECT @a, @b", &b, false).unwrap_err();
    assert!(
        err.message.contains("parameter count mismatch"),
        "unexpected error: {}",
        err.message
    );
}

#[test]
fn bind_params_reuses_resolved_names_across_rows() {
    let b = batch(
        vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ],
        vec![
            Arc::new(Int64Array::from(vec![1, 3])),
            Arc::new(Int64Array::from(vec![2, 4])),
        ],
    );
    let names = resolve_parameter_names("SELECT @a, @b", &b, false).unwrap();
    assert_eq!(names, vec!["a", "b"]);
    let bind = |row| {
        let built = bind_params(Statement::builder("SELECT @a, @b"), &names, &b, row).unwrap();
        format!("{:?}", built.build())
    };
    let row0 = bind(0);
    assert!(row0.contains(r#"StringValue("1")"#), "{row0}");
    assert!(row0.contains(r#"StringValue("2")"#), "{row0}");
    let row1 = bind(1);
    assert!(row1.contains(r#"StringValue("3")"#), "{row1}");
    assert!(row1.contains(r#"StringValue("4")"#), "{row1}");
    assert!(!row1.contains(r#"StringValue("1")"#), "{row1}");
}

#[test]
fn positional_binding_rejects_count_mismatch() {
    let b = int_batch(&["x", "y"]);
    let err = resolve_parameter_names("SELECT @p1", &b, false).unwrap_err();
    assert!(
        err.message.contains("parameter count mismatch"),
        "unexpected error: {}",
        err.message
    );
    assert!(!err.message.contains('?'), "{}", err.message);
}

#[test]
fn a_failed_resolution_of_question_mark_sql_points_to_named_parameters() {
    let b = int_batch(&["x"]);
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
    let err = resolve_parameter_names("SELECT '?' -- ?", &b, false).unwrap_err();
    assert!(!err.message.contains("placeholder"), "{}", err.message);
    // Resolution that succeeds is unaffected.
    assert_eq!(
        resolve_parameter_names("SELECT @p, '?'", &b, false).unwrap(),
        vec!["p"]
    );
}

#[test]
fn binds_date_timestamp_and_numeric() {
    let b = batch(
        vec![
            Field::new("d", DataType::Date32, false),
            Field::new("t", DataType::Timestamp(TimeUnit::Microsecond, None), false),
            Field::new("n", DataType::Decimal128(38, 9), false),
        ],
        vec![
            Arc::new(Date32Array::from(vec![19737])), // 2024-01-15
            Arc::new(TimestampMicrosecondArray::from(vec![1_705_322_096_789_012])),
            Arc::new(
                Decimal128Array::from(vec![1_500_000_000i128]) // 1.5 at scale 9
                    .with_precision_and_scale(38, 9)
                    .unwrap(),
            ),
        ],
    );
    let row0 = bound(&b, 0);
    for needle in [
        r#"StringValue("2024-01-15")"#,
        r#"StringValue("2024-01-15T12:34:56.789012Z")"#,
        // NUMERIC keeps its full scale.
        r#"StringValue("1.500000000")"#,
    ] {
        assert!(row0.contains(needle), "missing {needle} in: {row0}");
    }
}

#[test]
fn timestamp_string_preserves_full_precision() {
    let micros = 1_705_322_096_789_012i64;
    assert_eq!(
        timestamp_string("t", &TimeUnit::Microsecond, micros).unwrap(),
        "2024-01-15T12:34:56.789012Z"
    );
    assert_eq!(
        timestamp_string("t", &TimeUnit::Nanosecond, micros * 1_000 + 999).unwrap(),
        "2024-01-15T12:34:56.789012999Z"
    );
    assert_eq!(
        timestamp_string("t", &TimeUnit::Millisecond, 1_705_322_096_789).unwrap(),
        "2024-01-15T12:34:56.789Z"
    );
    assert_eq!(
        timestamp_string("t", &TimeUnit::Second, 1_705_322_096).unwrap(),
        "2024-01-15T12:34:56Z"
    );
    // One nanosecond before the epoch, not truncated toward zero.
    assert_eq!(
        timestamp_string("t", &TimeUnit::Nanosecond, -1).unwrap(),
        "1969-12-31T23:59:59.999999999Z"
    );
}

#[test]
fn temporal_binding_accepts_spanner_range_boundaries() {
    for date in ["0001-01-01", "9999-12-31"] {
        let days = crate::conversion::parse_date_days(date).unwrap();
        assert_eq!(date_string("d", days).unwrap(), date);
    }
    for (unit, multiplier, fraction) in [
        (TimeUnit::Second, 1, ""),
        (TimeUnit::Millisecond, 1_000, ".999"),
        (TimeUnit::Microsecond, 1_000_000, ".999999"),
    ] {
        assert_eq!(
            timestamp_string("t", &unit, -62_135_596_800 * multiplier).unwrap(),
            match unit {
                TimeUnit::Second => "0001-01-01T00:00:00Z",
                TimeUnit::Millisecond => "0001-01-01T00:00:00.000Z",
                _ => "0001-01-01T00:00:00.000000Z",
            }
        );
        assert_eq!(
            timestamp_string("t", &unit, 253_402_300_800 * multiplier - 1).unwrap(),
            format!("9999-12-31T23:59:59{fraction}Z")
        );
    }
    for value in [i64::MIN, i64::MAX] {
        assert!(timestamp_string("t", &TimeUnit::Nanosecond, value).is_ok());
    }
}

#[test]
fn temporal_binding_rejects_values_outside_spanner_range() {
    use arrow_array::{ArrayRef, Date64Array};

    let mut arrays: Vec<ArrayRef> = vec![
        Arc::new(Date32Array::from(vec![-719_163, 2_932_897])),
        Arc::new(Date64Array::from(vec![
            -719_163 * 86_400_000_i64,
            2_932_897 * 86_400_000_i64,
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
    ];
    arrays.push(Arc::new(
        ListArray::from_iter_primitive::<Date32Type, _, _>([
            Some(vec![Some(-719_163)]),
            Some(vec![Some(2_932_897)]),
        ]),
    ));
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
            assert!(error.message.contains("created_at"), "{error}");
            assert!(error.message.contains("0001"), "{error}");
            assert!(error.message.contains("9999"), "{error}");
            assert_eq!(
                insert_mutation("events", &b, row).unwrap_err().message,
                error.message
            );
        }
    }
}

#[test]
fn binds_timestamp_at_every_unit() {
    let ns = 1_705_322_096_789_012_999i64;
    let b = batch(
        vec![
            Field::new("s", DataType::Timestamp(TimeUnit::Second, None), false),
            Field::new("m", DataType::Timestamp(TimeUnit::Millisecond, None), false),
            Field::new(
                "n",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
        ],
        vec![
            Arc::new(TimestampSecondArray::from(vec![1_705_322_096])),
            Arc::new(TimestampMillisecondArray::from(vec![1_705_322_096_789])),
            Arc::new(TimestampNanosecondArray::from(vec![ns]).with_timezone("UTC")),
        ],
    );
    // Each unit formats at its own precision.
    let row0 = bound(&b, 0);
    for needle in [
        r#"StringValue("2024-01-15T12:34:56Z")"#,
        r#"StringValue("2024-01-15T12:34:56.789Z")"#,
        r#"StringValue("2024-01-15T12:34:56.789012999Z")"#,
    ] {
        assert!(row0.contains(needle), "missing {needle} in: {row0}");
    }
}

#[test]
fn binds_null_nanosecond_timestamp() {
    let b = batch(
        vec![Field::new(
            "n",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )],
        vec![Arc::new(TimestampNanosecondArray::from(vec![None::<i64>]))],
    );
    let row0 = bound(&b, 0);
    assert!(row0.contains("NullValue(NullValue)"), "{row0}");
}

#[test]
fn null_temporal_and_numeric_bind() {
    let b = batch(
        vec![
            Field::new("d", DataType::Date32, true),
            Field::new("n", DataType::Decimal128(38, 9), true),
        ],
        vec![
            Arc::new(Date32Array::from(vec![None::<i32>])),
            Arc::new(
                Decimal128Array::from(vec![None::<i128>])
                    .with_precision_and_scale(38, 9)
                    .unwrap(),
            ),
        ],
    );
    let row0 = bound(&b, 0);
    assert_eq!(row0.matches("NullValue(NullValue)").count(), 2, "{row0}");
}

#[test]
fn numeric_string_formats_full_i128_range() {
    assert_eq!(numeric_string(1_500_000_000, 9), "1.500000000");
    assert_eq!(numeric_string(-2_250_000_000, 9), "-2.250000000");
    assert_eq!(numeric_string(1, 9), "0.000000001");
    assert_eq!(numeric_string(-1, 9), "-0.000000001");
    assert_eq!(numeric_string(150, 2), "1.50");
    assert_eq!(numeric_string(-7, 0), "-7");
    // 10^30 overflows a 96-bit decimal mantissa but is a valid NUMERIC.
    assert_eq!(
        numeric_string(10i128.pow(30), 9),
        "1000000000000000000000.000000000"
    );
}

#[test]
fn numeric_string_preserves_negative_scale_without_integer_overflow() {
    assert_eq!(numeric_string(99, -2), "9900");
    assert_eq!(numeric_string(-99, -2), "-9900");
    assert_eq!(numeric_string(0, i8::MIN), "0");
    assert_eq!(numeric_string(1, i8::MIN), format!("1{}", "0".repeat(128)));
    assert_eq!(
        numeric_string(i128::MIN, -1),
        "-1701411834604692317316873037158841057280"
    );
}

#[test]
fn binds_numeric_beyond_a_96_bit_decimal() {
    let b = batch(
        vec![Field::new("n", DataType::Decimal128(38, 9), false)],
        vec![Arc::new(
            Decimal128Array::from(vec![10i128.pow(30)])
                .with_precision_and_scale(38, 9)
                .unwrap(),
        )],
    );
    let row0 = bound(&b, 0);
    assert!(
        row0.contains(r#"StringValue("1000000000000000000000.000000000")"#),
        "{row0}"
    );
}

#[test]
fn binds_large_string_and_binary() {
    let b = batch(
        vec![
            Field::new("s", DataType::LargeUtf8, true),
            Field::new("b", DataType::LargeBinary, true),
        ],
        vec![
            Arc::new(LargeStringArray::from(vec![Some("hello"), None])),
            Arc::new(LargeBinaryArray::from(vec![Some(b"bytes".as_ref()), None])),
        ],
    );
    let row0 = bound(&b, 0);
    assert!(row0.contains(r#"StringValue("hello")"#), "{row0}");
    // b"bytes" in base64.
    assert!(row0.contains(r#"StringValue("Ynl0ZXM=")"#), "{row0}");
    let row1 = bound(&b, 1);
    assert_eq!(row1.matches("NullValue(NullValue)").count(), 2, "{row1}");
}

#[test]
fn binds_view_strings_and_binaries() {
    let b = batch(
        vec![
            Field::new("s", DataType::Utf8View, true),
            Field::new("b", DataType::BinaryView, true),
        ],
        vec![
            Arc::new(arrow_array::StringViewArray::from(vec![
                Some("hello view"),
                None,
            ])),
            Arc::new(arrow_array::BinaryViewArray::from(vec![
                Some(b"view bytes".as_ref()),
                None,
            ])),
        ],
    );
    let row0 = bound(&b, 0);
    assert!(row0.contains(r#"StringValue("hello view")"#), "{row0}");
    // b"view bytes" in base64.
    assert!(
        row0.contains(r#"StringValue("dmlldyBieXRlcw==")"#),
        "{row0}"
    );
    let row1 = bound(&b, 1);
    assert_eq!(row1.matches("NullValue(NullValue)").count(), 2, "{row1}");
}

#[test]
fn binds_int8_and_date64() {
    let b = batch(
        vec![
            Field::new("i", DataType::Int8, true),
            Field::new("d", DataType::Date64, true),
        ],
        vec![
            Arc::new(arrow_array::Int8Array::from(vec![Some(7i8), None])),
            // 2024-01-15
            Arc::new(arrow_array::Date64Array::from(vec![
                Some(19_737i64 * 86_400_000),
                None,
            ])),
        ],
    );
    let row0 = bound(&b, 0);
    assert!(row0.contains(r#"StringValue("7")"#), "{row0}");
    assert!(row0.contains(r#"StringValue("2024-01-15")"#), "{row0}");
    let row1 = bound(&b, 1);
    assert_eq!(row1.matches("NullValue(NullValue)").count(), 2, "{row1}");
}

#[test]
fn date64_converts_to_days() {
    assert_eq!(date64_days("d", 0).unwrap(), 0);
    assert_eq!(date64_days("d", 19_737i64 * 86_400_000).unwrap(), 19737);
    assert_eq!(date64_days("d", -86_400_000).unwrap(), -1);
    // Out of Date32 range errors instead of wrapping.
    assert!(date64_days("d", (i64::from(i32::MAX) + 1) * 86_400_000).is_err());
    assert!(date64_days("d", (i64::from(i32::MIN) - 1) * 86_400_000).is_err());
}

#[test]
fn date64_binding_rejects_time_of_day_in_scalars_and_lists() {
    let arrays: Vec<arrow_array::ArrayRef> = vec![
        Arc::new(arrow_array::Date64Array::from(vec![1, -1])),
        Arc::new(ListArray::from_iter_primitive::<Date64Type, _, _>([
            Some(vec![Some(1)]),
            Some(vec![Some(-1)]),
        ])),
    ];
    for array in arrays {
        let b = batch(
            vec![Field::new("created_at", array.data_type().clone(), false)],
            vec![array],
        );
        for row in 0..2 {
            for error in [
                bind_params(
                    Statement::builder("SELECT @created_at"),
                    &["created_at".into()],
                    &b,
                    row,
                )
                .unwrap_err(),
                insert_mutation("events", &b, row).unwrap_err(),
            ] {
                assert_eq!(error.status, Status::InvalidArguments);
                assert!(error.message.contains("created_at"), "{error}");
                assert!(error.message.contains("whole number of days"), "{error}");
            }
        }
    }
}

#[test]
fn binds_view_arrays_as_list_elements() {
    use arrow_array::builder::{BinaryViewBuilder, ListBuilder, StringViewBuilder};
    let mut strings = ListBuilder::new(StringViewBuilder::new());
    strings.values().append_value("a");
    strings.values().append_null();
    strings.append(true);
    let mut bytes = ListBuilder::new(BinaryViewBuilder::new());
    bytes.values().append_value(b"b");
    bytes.append(true);
    let b = batch(
        vec![
            Field::new(
                "s",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8View, true))),
                true,
            ),
            Field::new(
                "b",
                DataType::List(Arc::new(Field::new("item", DataType::BinaryView, true))),
                true,
            ),
        ],
        vec![Arc::new(strings.finish()), Arc::new(bytes.finish())],
    );
    let row0 = bound(&b, 0);
    assert!(
        row0.contains(
            r#"ListValue { values: [Value { kind: Some(StringValue("a")) }, Value { kind: Some(NullValue(NullValue)) }] }"#
        ),
        "{row0}"
    );
    // b"b" in base64.
    assert!(
        row0.contains(r#"ListValue { values: [Value { kind: Some(StringValue("Yg==")) }] }"#),
        "{row0}"
    );
}

#[test]
fn rejects_unsupported_arrow_type() {
    let b = batch(
        vec![Field::new("x", DataType::UInt64, false)],
        vec![Arc::new(arrow_array::UInt64Array::from(vec![1u64]))],
    );
    assert!(bind_row(&b, 0).is_err());
}

// Dictionary-encoded columns, such as pandas categoricals.

#[test]
fn binds_dictionary_encoded_columns_as_their_values() {
    // Decoded values bind like their plain type; Int32 still widens to INT64.
    let strings: arrow_array::DictionaryArray<Int32Type> =
        vec![Some("a"), None, Some("b"), Some("a")]
            .into_iter()
            .collect();
    let ints = arrow_array::DictionaryArray::new(
        arrow_array::Int8Array::from(vec![Some(1i8), None, Some(0), Some(1)]),
        Arc::new(Int32Array::from(vec![7i32, 9])),
    );
    let b = batch(
        vec![
            Field::new("s", strings.data_type().clone(), true),
            Field::new("i", ints.data_type().clone(), true),
        ],
        vec![Arc::new(strings), Arc::new(ints)],
    );
    let row0 = bound(&b, 0);
    assert!(
        row0.contains(r#"StringValue("a")"#) && row0.contains(r#"StringValue("9")"#),
        "{row0}"
    );
    assert!(!row0.contains("NumberValue"), "{row0}");
    let row1 = bound(&b, 1);
    assert!(row1.contains("NullValue"), "{row1}");
    let row2 = bound(&b, 2);
    assert!(
        row2.contains(r#"StringValue("b")"#) && row2.contains(r#"StringValue("7")"#),
        "{row2}"
    );
}

#[test]
fn binds_dictionary_encoded_array_cells() {
    let lists = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
        Some(vec![Some(1i64), None]),
        Some(vec![Some(2)]),
    ]);
    let dict = arrow_array::DictionaryArray::new(
        arrow_array::Int8Array::from(vec![Some(0i8), None]),
        Arc::new(lists),
    );
    let b = batch(
        vec![Field::new("tags", dict.data_type().clone(), true)],
        vec![Arc::new(dict)],
    );
    let row0 = bound(&b, 0);
    assert!(
        row0.contains(r#"StringValue("1")"#) && row0.contains("NullValue"),
        "{row0}"
    );
    // A null key binds a null array.
    let row1 = bound(&b, 1);
    assert!(
        row1.contains("NullValue") && !row1.contains("ListValue"),
        "{row1}"
    );
}

#[test]
fn binds_dictionary_cells_of_a_sliced_batch_at_their_offset() {
    let dict: arrow_array::DictionaryArray<Int32Type> = vec![Some("alpha"), Some("beta"), None]
        .into_iter()
        .collect();
    let b = batch(
        vec![Field::new("name", dict.data_type().clone(), true)],
        vec![Arc::new(dict)],
    );
    let sliced = b.slice(1, 2);
    let row0 = bound(&sliced, 0);
    assert!(
        row0.contains(r#"StringValue("beta")"#) && !row0.contains("alpha"),
        "{row0}"
    );
    let row1 = bound(&sliced, 1);
    assert!(
        row1.contains("NullValue") && !row1.contains("beta"),
        "{row1}"
    );
}

#[test]
fn binds_non_null_key_pointing_at_null_dictionary_value_as_null() {
    // A valid key can select a null value. The ADBC PostgreSQL driver once mishandled this
    // ("insufficient data left in message").
    let dict = arrow_array::DictionaryArray::new(
        arrow_array::Int8Array::from(vec![Some(0i8), Some(1)]),
        Arc::new(StringArray::from(vec![None, Some("a")])),
    );
    let b = batch(
        vec![Field::new("s", dict.data_type().clone(), true)],
        vec![Arc::new(dict)],
    );
    let row0 = bound(&b, 0);
    assert!(
        row0.contains("NullValue") && !row0.contains("StringValue"),
        "{row0}"
    );
    let row1 = bound(&b, 1);
    assert!(row1.contains(r#"StringValue("a")"#), "{row1}");
}

#[test]
fn binds_json_tagged_dictionary_strings_as_json_params() {
    // Arrow allows extension arrays to be dictionary-encoded. Null cells keep the JSON type too.
    let dict = arrow_array::DictionaryArray::new(
        arrow_array::Int8Array::from(vec![Some(0i8), None]),
        Arc::new(StringArray::from(vec![Some(r#"{"a":1}"#)])),
    );
    let b = batch(
        vec![json_field("doc", dict.data_type().clone())],
        vec![Arc::new(dict)],
    );
    for row in 0..2 {
        let dbg = bound(&b, row);
        assert!(
            dbg.contains(r#""p0": Type(Type { code: Json"#),
            "row {row}: {dbg}"
        );
    }
}

#[test]
fn json_tag_is_ignored_on_non_string_dictionary_values() {
    let dict = arrow_array::DictionaryArray::new(
        arrow_array::Int8Array::from(vec![Some(0i8)]),
        Arc::new(Int64Array::from(vec![7i64])),
    );
    let b = batch(
        vec![json_field("n", dict.data_type().clone())],
        vec![Arc::new(dict)],
    );
    let dbg = bound(&b, 0);
    assert!(!dbg.contains("Json"), "{dbg}");
}

#[test]
fn rejects_unsupported_dictionary_value_type() {
    // Rejected on null cells too.
    let dict = arrow_array::DictionaryArray::new(
        arrow_array::Int32Array::from(vec![Some(0), None]),
        Arc::new(arrow_array::UInt64Array::from(vec![1u64])),
    );
    let b = batch(
        vec![Field::new("x", dict.data_type().clone(), true)],
        vec![Arc::new(dict)],
    );
    for row in 0..2 {
        let err = bind_row(&b, row).unwrap_err();
        assert!(
            err.message.contains("unsupported Arrow type UInt64"),
            "row {row}: unexpected error: {}",
            err.message
        );
    }
}

#[test]
fn ingests_dictionary_encoded_columns_decoded() {
    let dict: arrow_array::DictionaryArray<Int32Type> =
        vec![Some("Alice"), None].into_iter().collect();
    let data_type = dict.data_type().clone();
    let b = batch(
        vec![Field::new("Name", data_type.clone(), true)],
        vec![Arc::new(dict)],
    );
    let dbg = format!("{:?}", insert_mutation("Users", &b, 0).unwrap());
    assert!(dbg.contains(r#"StringValue("Alice")"#), "{dbg}");
    let dbg = format!("{:?}", insert_mutation("Users", &b, 1).unwrap());
    assert!(dbg.contains("NullValue"), "no NULL for row 1: {dbg}");
    assert_eq!(spanner_column_type(&data_type).unwrap(), "STRING(MAX)");
}

// ARRAY (List / LargeList) binding.

#[test]
fn array_elements_widen_and_keep_nulls_through_scalar_binder() {
    let arr = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
        Some(vec![Some(1i32), None, Some(3)]),
        None,
    ]);
    let b = batch(
        vec![Field::new("tags", arr.data_type().clone(), true)],
        vec![Arc::new(arr)],
    );
    let populated = bound(&b, 0);
    for needle in [r#"StringValue("1")"#, "NullValue", r#"StringValue("3")"#] {
        assert!(
            populated.contains(needle),
            "row 0 missing {needle}: {populated}"
        );
    }
    assert!(!populated.contains("NumberValue"), "{populated}");
    let null_array = bound(&b, 1);
    assert!(
        null_array.contains("NullValue") && !null_array.contains("ListValue"),
        "{null_array}"
    );
}

#[test]
fn binds_int64_array_including_null_array_and_null_element() {
    let arr = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
        Some(vec![Some(1i64), None, Some(3)]),
        None,
        Some(vec![]),
    ]);
    let b = batch(
        vec![Field::new("tags", arr.data_type().clone(), true)],
        vec![Arc::new(arr)],
    );
    let row0 = bound(&b, 0);
    assert!(
        row0.contains(
            r#"ListValue { values: [Value { kind: Some(StringValue("1")) }, Value { kind: Some(NullValue(NullValue)) }, Value { kind: Some(StringValue("3")) }] }"#
        ),
        "{row0}"
    );
    // A null array and an empty array stay distinct.
    let row1 = bound(&b, 1);
    assert!(row1.contains("NullValue(NullValue)"), "{row1}");
    assert!(!row1.contains("ListValue"), "{row1}");
    let row2 = bound(&b, 2);
    assert!(row2.contains("ListValue { values: [] }"), "{row2}");
    assert!(!row2.contains("NullValue"), "{row2}");
}

#[test]
fn binds_arrays_of_each_supported_element_type() {
    use arrow_array::builder::{
        BooleanBuilder, Date32Builder, Decimal128Builder, Float64Builder, ListBuilder,
        StringBuilder, TimestampMicrosecondBuilder,
    };

    let mut floats = ListBuilder::new(Float64Builder::new());
    floats.values().append_value(1.5);
    floats.values().append_null();
    floats.append(true);
    let floats = floats.finish();

    let mut bools = ListBuilder::new(BooleanBuilder::new());
    bools.values().append_value(true);
    bools.append(true);
    let bools = bools.finish();

    let mut strs = ListBuilder::new(StringBuilder::new());
    strs.values().append_value("hi");
    strs.append(true);
    let strs = strs.finish();

    let mut dates = ListBuilder::new(Date32Builder::new());
    dates.values().append_value(19737); // 2024-01-15
    dates.append(true);
    let dates = dates.finish();

    let mut ts = ListBuilder::new(TimestampMicrosecondBuilder::new());
    ts.values().append_value(1_705_322_096_789_012);
    ts.append(true);
    let ts = ts.finish();

    let mut nums = ListBuilder::new(
        Decimal128Builder::new()
            .with_precision_and_scale(38, 9)
            .unwrap(),
    );
    nums.values().append_value(1_500_000_000); // 1.5 at scale 9
    nums.append(true);
    let nums = nums.finish();

    let b = batch(
        vec![
            Field::new("f", floats.data_type().clone(), true),
            Field::new("bo", bools.data_type().clone(), true),
            Field::new("s", strs.data_type().clone(), true),
            Field::new("d", dates.data_type().clone(), true),
            Field::new("t", ts.data_type().clone(), true),
            Field::new("n", nums.data_type().clone(), true),
        ],
        vec![
            Arc::new(floats),
            Arc::new(bools),
            Arc::new(strs),
            Arc::new(dates),
            Arc::new(ts),
            Arc::new(nums),
        ],
    );
    let row0 = bound(&b, 0);
    for needle in [
        r#"ListValue { values: [Value { kind: Some(NumberValue(1.5)) }, Value { kind: Some(NullValue(NullValue)) }] }"#,
        r#"ListValue { values: [Value { kind: Some(BoolValue(true)) }] }"#,
        r#"ListValue { values: [Value { kind: Some(StringValue("hi")) }] }"#,
        r#"ListValue { values: [Value { kind: Some(StringValue("2024-01-15")) }] }"#,
        r#"ListValue { values: [Value { kind: Some(StringValue("2024-01-15T12:34:56.789012Z")) }] }"#,
        r#"ListValue { values: [Value { kind: Some(StringValue("1.500000000")) }] }"#,
    ] {
        assert!(row0.contains(needle), "missing {needle} in: {row0}");
    }
}

#[test]
fn rejects_nested_array_element_type() {
    use arrow_array::builder::{Int64Builder, ListBuilder};

    let mut lb = ListBuilder::new(ListBuilder::new(Int64Builder::new()));
    lb.values().values().append_value(1);
    lb.values().append(true);
    lb.append(true);
    let arr = lb.finish();
    let b = batch(
        vec![Field::new("x", arr.data_type().clone(), true)],
        vec![Arc::new(arr)],
    );
    let err = bind_row(&b, 0).unwrap_err();
    assert!(
        err.message.contains("unsupported Arrow type"),
        "unexpected error: {}",
        err.message
    );
}

#[test]
fn spanner_column_type_maps_array_element_types() {
    let list = |dt: DataType| DataType::List(Arc::new(Field::new("item", dt, true)));
    assert_eq!(
        spanner_column_type(&list(DataType::Utf8)).unwrap(),
        "ARRAY<STRING(MAX)>"
    );
    assert_eq!(
        spanner_column_type(&list(DataType::Date32)).unwrap(),
        "ARRAY<DATE>"
    );
}

// Sliced batches: arrays are views at a nonzero offset into shared buffers, so row `i` must be
// read through the view. The ADBC PostgreSQL driver had this bug for sliced lists
// (apache/arrow-adbc#4320).

#[test]
fn binds_rows_of_a_sliced_batch_at_their_offset() {
    let parent = batch(
        vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ],
        vec![
            Arc::new(Int64Array::from(vec![Some(10), Some(20), None])),
            Arc::new(arrow_array::StringArray::from(vec![
                Some("alpha"),
                Some("beta"),
                None,
            ])),
        ],
    );
    let sliced = parent.slice(1, 2);
    assert_eq!(sliced.num_rows(), 2);

    let row0 = bound(&sliced, 0);
    assert!(row0.contains(r#"StringValue("20")"#), "row 0: {row0}");
    assert!(row0.contains(r#"StringValue("beta")"#), "row 0: {row0}");
    assert!(
        !row0.contains("alpha") && !row0.contains(r#"StringValue("10")"#),
        "row 0 was read from the unsliced buffer: {row0}"
    );

    let row1 = bound(&sliced, 1);
    assert!(row1.contains("NullValue"), "row 1: {row1}");
    assert!(
        !row1.contains("beta") && !row1.contains(r#"StringValue("20")"#),
        "row 1 was read from the unsliced buffer: {row1}"
    );
}

#[test]
fn binds_list_cells_of_a_sliced_batch_at_their_offset() {
    let arr = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
        Some(vec![Some(1i64), Some(2)]),
        Some(vec![Some(3), None, Some(5)]),
        Some(vec![Some(6)]),
    ]);
    let b = batch(
        vec![Field::new("tags", arr.data_type().clone(), true)],
        vec![Arc::new(arr)],
    );
    let sliced = b.slice(1, 2);

    let row0 = bound(&sliced, 0);
    for needle in [r#"StringValue("3")"#, "NullValue", r#"StringValue("5")"#] {
        assert!(row0.contains(needle), "row 0 missing {needle}: {row0}");
    }
    assert!(
        !row0.contains(r#"StringValue("1")"#) && !row0.contains(r#"StringValue("2")"#),
        "row 0 was read from the unsliced buffer: {row0}"
    );

    let row1 = bound(&sliced, 1);
    assert!(row1.contains(r#"StringValue("6")"#), "row 1: {row1}");
    assert!(
        !row1.contains(r#"StringValue("3")"#),
        "row 1 was read from the unsliced buffer: {row1}"
    );
}

#[test]
fn binds_string_list_cells_of_a_sliced_batch_at_their_offset() {
    use arrow_array::builder::{ListBuilder, StringBuilder};

    // Both the list offsets and the child string offsets are shared with the sliced-away row.
    let mut sb = ListBuilder::new(StringBuilder::new());
    sb.values().append_value("skip");
    sb.append(true);
    sb.values().append_value("keep");
    sb.values().append_null();
    sb.append(true);
    let strings = sb.finish();
    let b = batch(
        vec![Field::new("names", strings.data_type().clone(), true)],
        vec![Arc::new(strings)],
    );
    let sliced = b.slice(1, 1);

    let row0 = bound(&sliced, 0);
    assert!(row0.contains(r#"StringValue("keep")"#), "row 0: {row0}");
    assert!(row0.contains("NullValue"), "row 0: {row0}");
    assert!(
        !row0.contains("skip"),
        "row 0 was read from the unsliced buffer: {row0}"
    );
}

#[test]
fn binds_sliced_large_lists_with_sliced_child_values() {
    use arrow_array::LargeListArray;
    use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};

    // Both the list and its child array start at nonzero offsets.
    let values = Int64Array::from(vec![Some(99), Some(1), Some(2), None, Some(4)]).slice(1, 4);
    let lists = LargeListArray::new(
        Arc::new(Field::new("item", DataType::Int64, true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0_i64, 1, 4, 4, 4])),
        Arc::new(values),
        Some(NullBuffer::from(vec![true, true, true, false])),
    );
    let batch = batch(
        vec![Field::new("items", lists.data_type().clone(), true)],
        vec![Arc::new(lists)],
    )
    .slice(1, 3);
    let row = bound(&batch, 0);
    for value in [r#"StringValue("2")"#, "NullValue", r#"StringValue("4")"#] {
        assert!(row.contains(value), "{row}");
    }
    assert!(!row.contains(r#"StringValue("1")"#), "{row}");
    assert!(!row.contains(r#"StringValue("99")"#), "{row}");
    let empty = bound(&batch, 1);
    assert!(empty.contains("ListValue"), "{empty}");
    assert!(!empty.contains("NullValue"), "{empty}");
    let null = bound(&batch, 2);
    assert!(null.contains("NullValue"), "{null}");
    assert!(!null.contains("ListValue"), "{null}");
}

#[test]
fn by_name_binding_rejects_case_insensitive_duplicate_columns() {
    let batch = batch(
        vec![
            Field::new("name", DataType::Int64, false),
            Field::new("NAME", DataType::Int64, false),
        ],
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![2])),
        ],
    );
    let error = resolve_parameter_names("SELECT @NaMe", &batch, true).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(error.message.contains("same query parameter"), "{error}");
}

#[test]
fn nested_dictionary_json_preserves_type_for_values_and_both_null_kinds() {
    let values = arrow_array::DictionaryArray::new(
        arrow_array::Int8Array::from(vec![Some(0), None]),
        Arc::new(StringArray::from(vec![r#"{"a":1}"#])),
    );
    let dictionary = arrow_array::DictionaryArray::new(
        arrow_array::Int8Array::from(vec![Some(0), Some(1), None]),
        Arc::new(values),
    );
    let field = json_field("doc", dictionary.data_type().clone());
    assert_eq!(spanner_field_type(&field).unwrap(), "JSON");
    for row in 0..3 {
        let (value, param_type) = cell_value("doc", &field, &dictionary, row).unwrap();
        assert_eq!(param_type, Some(types::json()));
        if row == 0 {
            assert_eq!(value.as_str(), Some(r#"{"a":1}"#));
        } else {
            assert_eq!(value.kind(), google_cloud_spanner::value::Kind::Null);
        }
    }
}

#[test]
fn negative_decimal_scale_binds_including_null_and_empty_containers() {
    use arrow_array::builder::{Decimal128Builder, ListBuilder};

    let decimal = Decimal128Array::from(vec![Some(-99), None])
        .with_precision_and_scale(38, -2)
        .unwrap();
    let dictionary = arrow_array::DictionaryArray::new(
        arrow_array::Int8Array::from(vec![Some(0), Some(1), None]),
        Arc::new(decimal.clone()),
    );
    let mut lists = ListBuilder::new(
        Decimal128Builder::new()
            .with_precision_and_scale(38, -2)
            .unwrap(),
    );
    lists.values().append_value(-99);
    lists.values().append_null();
    lists.append(true);
    lists.append(true);
    lists.append(false);
    let arrays: Vec<Arc<dyn Array>> = vec![
        Arc::new(decimal),
        Arc::new(dictionary),
        Arc::new(lists.finish()),
    ];
    for array in arrays {
        let field = Field::new("amount", array.data_type().clone(), true);
        let is_list = matches!(array.data_type(), DataType::List(_));
        assert_eq!(
            spanner_field_type(&field).unwrap(),
            if is_list { "ARRAY<NUMERIC>" } else { "NUMERIC" }
        );
        for row in 0..array.len() {
            let (value, _) = cell_value("amount", &field, array.as_ref(), row).unwrap();
            if is_list && row < 2 {
                let values = value.as_list().unwrap();
                if row == 0 {
                    assert_eq!(values.len(), 2);
                    assert_eq!(values.get(0).unwrap().as_str(), Some("-9900"));
                    assert_eq!(
                        values.get(1).unwrap().kind(),
                        google_cloud_spanner::value::Kind::Null
                    );
                } else {
                    assert!(values.is_empty());
                }
            } else if row == 0 {
                assert_eq!(value.as_str(), Some("-9900"));
            } else {
                assert_eq!(value.kind(), google_cloud_spanner::value::Kind::Null);
            }
        }
    }
}

#[test]
fn decimal_scale_above_arrow_limit_is_rejected() {
    let data_type = DataType::Decimal128(38, 39);
    for error in [
        scalar_binder("amount", &data_type).unwrap_err(),
        spanner_column_type(&data_type).unwrap_err(),
    ] {
        assert_eq!(error.status, Status::InvalidArguments);
        assert!(error.message.contains("unsupported scale 39"), "{error}");
    }
}

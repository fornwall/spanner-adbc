use super::*;
use adbc_core::constants::{
    ADBC_STATISTIC_DISTINCT_COUNT_KEY, ADBC_STATISTIC_NULL_COUNT_KEY, ADBC_STATISTIC_ROW_COUNT_KEY,
};
use adbc_core::schemas::GET_STATISTICS_SCHEMA;
use arrow_array::{ListArray, UnionArray};

const CATALOG: &str = "adbc-test";

/// The statistics of the first db schema in a [`build`] result.
fn only_schema_statistics(batch: &RecordBatch) -> StructArray {
    let list = |array: &dyn Array| {
        array
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("a list")
            .value(0)
    };
    let db_schemas = list(batch.column(1).as_ref());
    let db_schemas = db_schemas.as_any().downcast_ref::<StructArray>().unwrap();
    let stats = list(
        db_schemas
            .column_by_name("db_schema_statistics")
            .unwrap()
            .as_ref(),
    );
    stats
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap()
        .clone()
}

/// Statistics are exact counts, so `statistic_is_approximate` is always false.
#[test]
fn build_reports_each_statistic_exactly_and_never_as_approximate() {
    let schemas = vec![SchemaStatistics {
        db_schema: String::new(),
        statistics: vec![
            Statistic {
                table: "Users".into(),
                column: None,
                key: ADBC_STATISTIC_ROW_COUNT_KEY,
                value: 42,
            },
            Statistic {
                table: "Users".into(),
                column: Some("Name".into()),
                key: ADBC_STATISTIC_NULL_COUNT_KEY,
                value: 3,
            },
        ],
    }];
    let batch = build(CATALOG, schemas, GET_STATISTICS_SCHEMA.clone()).unwrap();
    assert_eq!(batch.schema(), GET_STATISTICS_SCHEMA.clone());
    assert_eq!(batch.num_rows(), 1); // one catalog

    let stats = only_schema_statistics(&batch);
    assert_eq!(stats.len(), 2);
    let column = |name: &str| stats.column_by_name(name).unwrap().clone();
    let tables = column("table_name");
    let tables = tables.as_any().downcast_ref::<StringArray>().unwrap();
    let columns = column("column_name");
    let columns = columns.as_any().downcast_ref::<StringArray>().unwrap();
    let keys = column("statistic_key");
    let keys = keys.as_any().downcast_ref::<Int16Array>().unwrap();
    let approximate = column("statistic_is_approximate");
    let approximate = approximate.as_any().downcast_ref::<BooleanArray>().unwrap();
    let values = column("statistic_value");
    let values = values.as_any().downcast_ref::<UnionArray>().unwrap();
    let int64 = |row: usize| -> i64 {
        assert_eq!(values.type_id(row), INT64_BRANCH, "row {row}");
        let child = values.value(row);
        child
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0)
    };

    assert_eq!(tables.value(0), "Users");
    assert!(columns.is_null(0), "ROW_COUNT is a table-level statistic");
    assert_eq!(keys.value(0), ADBC_STATISTIC_ROW_COUNT_KEY);
    assert_eq!(int64(0), 42);
    assert_eq!(tables.value(1), "Users");
    assert_eq!(columns.value(1), "Name");
    assert_eq!(keys.value(1), ADBC_STATISTIC_NULL_COUNT_KEY);
    assert_eq!(int64(1), 3);

    assert!(
        (0..stats.len()).all(|row| !approximate.value(row)),
        "every reported statistic is exact"
    );
}

/// `plan[i]` names the statistic at result column `i + 1`.
#[test]
fn build_table_query_pairs_each_aggregate_with_its_plan_entry() {
    let columns = vec![
        ("Id".to_string(), true),
        // Not groupable, like an ARRAY column: no COUNT(DISTINCT).
        ("Tags".to_string(), false),
        ("Name".to_string(), true),
    ];
    let (sql, plan) = build_table_query("", "Users", &columns).unwrap();
    assert_eq!(
        sql,
        "SELECT COUNT(*), COUNTIF(`Id` IS NULL), COUNT(DISTINCT `Id`), \
         COUNTIF(`Tags` IS NULL), COUNTIF(`Name` IS NULL), COUNT(DISTINCT `Name`) \
         FROM `Users`"
    );
    assert_eq!(
        plan,
        vec![
            ("Id".to_string(), ADBC_STATISTIC_NULL_COUNT_KEY),
            ("Id".to_string(), ADBC_STATISTIC_DISTINCT_COUNT_KEY),
            ("Tags".to_string(), ADBC_STATISTIC_NULL_COUNT_KEY),
            ("Name".to_string(), ADBC_STATISTIC_NULL_COUNT_KEY),
            ("Name".to_string(), ADBC_STATISTIC_DISTINCT_COUNT_KEY),
        ]
    );

    // A non-empty schema qualifies the table; a table with no columns is just the row count.
    let (sql, plan) = build_table_query("app", "Users", &[]).unwrap();
    assert_eq!(sql, "SELECT COUNT(*) FROM `app`.`Users`");
    assert!(plan.is_empty());
}

/// Distinct counts catch an off-by-one between `plan` and the result columns, which would
/// silently report one statistic's value as another's.
#[test]
fn parse_table_statistics_reads_each_plan_entry_at_its_own_aggregate() {
    let columns = vec![("Id".to_string(), true), ("Tags".to_string(), false)];
    let (_, plan) = build_table_query("", "Users", &columns).unwrap();
    // COUNT(*), COUNTIF(Id IS NULL), COUNT(DISTINCT Id), COUNTIF(Tags IS NULL).
    let counts = [100i64, 7, 93, 11];
    let batch = RecordBatch::try_from_iter(counts.iter().enumerate().map(|(i, c)| {
        (
            format!("f{i}"),
            Arc::new(Int64Array::from(vec![*c])) as ArrayRef,
        )
    }))
    .unwrap();

    let stats = parse_table_statistics(&batch, "Users", plan).unwrap();
    let seen: Vec<(Option<&str>, i16, i64)> = stats
        .iter()
        .map(|s| (s.column.as_deref(), s.key, s.value))
        .collect();
    assert_eq!(
        seen,
        vec![
            (None, ADBC_STATISTIC_ROW_COUNT_KEY, 100),
            (Some("Id"), ADBC_STATISTIC_NULL_COUNT_KEY, 7),
            (Some("Id"), ADBC_STATISTIC_DISTINCT_COUNT_KEY, 93),
            (Some("Tags"), ADBC_STATISTIC_NULL_COUNT_KEY, 11),
        ]
    );
    assert!(stats.iter().all(|s| s.table == "Users"));
}

/// A malformed aggregate result is an `Internal` error; nothing may panic across the C ABI.
#[test]
fn parse_table_statistics_rejects_a_result_that_is_not_one_row_of_integers() {
    let plan = vec![("Id".to_string(), ADBC_STATISTIC_NULL_COUNT_KEY)];
    let two_rows = RecordBatch::try_from_iter([
        ("a", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef),
        ("b", Arc::new(Int64Array::from(vec![3, 4])) as ArrayRef),
    ])
    .unwrap();
    let Err(error) = parse_table_statistics(&two_rows, "Users", plan.clone()) else {
        panic!("a two-row aggregate result must be rejected")
    };
    assert_eq!(error.status, Status::Internal);

    // The right shape, but a string aggregate.
    let wrong_type = RecordBatch::try_from_iter([
        ("a", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
        ("b", Arc::new(StringArray::from(vec!["3"])) as ArrayRef),
    ])
    .unwrap();
    let Err(error) = parse_table_statistics(&wrong_type, "Users", plan) else {
        panic!("a non-integer aggregate must be rejected")
    };
    assert_eq!(error.status, Status::Internal);
}

#[test]
fn parse_table_statistics_rejects_missing_or_extra_aggregates() {
    let batch = RecordBatch::try_from_iter([
        ("rows", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
        ("nulls", Arc::new(Int64Array::from(vec![0])) as ArrayRef),
    ])
    .unwrap();
    for plan in [
        Vec::new(),
        vec![("Id".to_string(), ADBC_STATISTIC_NULL_COUNT_KEY); 2],
    ] {
        let Err(error) = parse_table_statistics(&batch, "Users", plan) else {
            panic!("aggregate columns must match the query plan")
        };
        assert_eq!(error.status, Status::Internal);
    }
    let empty = RecordBatch::new_empty(Arc::new(arrow_schema::Schema::empty()));
    assert!(parse_table_statistics(&empty, "Users", Vec::new()).is_err());
}

#[test]
fn parse_table_statistics_rejects_null_and_negative_counts() {
    for count in [None, Some(-1)] {
        let batch = RecordBatch::try_from_iter([(
            "rows",
            Arc::new(Int64Array::from(vec![count])) as ArrayRef,
        )])
        .unwrap();
        let Err(error) = parse_table_statistics(&batch, "Users", Vec::new()) else {
            panic!("invalid counts must not be reported as statistics")
        };
        assert_eq!(error.status, Status::Internal);
    }
}

#[test]
fn groupable_types() {
    for t in [
        "INT64",
        "STRING(MAX)",
        "BOOL",
        "BYTES(10)",
        "NUMERIC",
        "TIMESTAMP",
        "DATE",
        // Verified on the emulator.
        "UUID",
    ] {
        assert!(is_groupable(t), "{t} should be groupable");
    }
    // COUNT(DISTINCT) over these fails the scan.
    for t in [
        "ARRAY<INT64>",
        "STRUCT<a INT64>",
        "JSON",
        "TOKENLIST",
        "INTERVAL",
        "PROTO<examples.spanner.music.SingerInfo>",
    ] {
        assert!(!is_groupable(t), "{t} should not be groupable");
    }
}

#[test]
fn empty_is_valid() {
    let batch = build(CATALOG, Vec::new(), GET_STATISTICS_SCHEMA.clone()).unwrap();
    assert_eq!(batch.schema(), GET_STATISTICS_SCHEMA.clone());
    assert_eq!(batch.num_rows(), 1);
}

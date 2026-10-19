use super::*;
use arrow_array::{Array, ListArray};

/// Filtered queries keep their `LIKE` parameters and use the metadata-read config: priority and
/// replica selection, but no request tag.
#[test]
fn filtered_queries_carry_the_connection_config() {
    let mut config = SharedConfig::default();
    config
        .request
        .set_priority(adbc_core::options::OptionValue::String("low".into()))
        .unwrap();
    config
        .request
        .set_request_tag(adbc_core::options::OptionValue::String("user-tag".into()))
        .unwrap();
    config
        .directed_read
        .set(adbc_core::options::OptionValue::String(
            "exclude::read_write".into(),
        ))
        .unwrap();
    let built = format!(
        "{:?}",
        filtered_query(
            &config,
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES",
            None,
            &[("TABLE_NAME", Some("User%"))],
            None,
        )
    );
    assert!(built.contains("priority: Low"), "{built}");
    assert!(built.contains("ExcludeReplicas"), "{built}");
    assert!(!built.contains("user-tag"), "{built}");
    assert!(built.contains("User%"), "{built}");
}

/// The SQL text of a built statement, as its `Debug` rendering carries it.
fn sql_of(statement: &SpannerSql) -> String {
    let built = format!("{statement:?}");
    let rest = built.split_once("sql: \"").expect("a sql field").1;
    rest.split_once('"').expect("a closing quote").0.to_string()
}

/// The hidden-column predicates, built as in `collect_objects`, open the `WHERE` ahead of the
/// `LIKE` filters.
#[test]
fn filtered_query_hides_hidden_columns_and_their_constraints() {
    let config = SharedConfig::default();
    let columns = filtered_query(
        &config,
        "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, \
         IS_NULLABLE, SPANNER_TYPE \
         FROM INFORMATION_SCHEMA.COLUMNS",
        Some(HIDE_HIDDEN_COLUMNS),
        &[
            ("TABLE_SCHEMA", None),
            ("TABLE_NAME", Some("User%")),
            ("COLUMN_NAME", None),
        ],
        Some("TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION"),
    );
    assert_eq!(
        sql_of(&columns),
        "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, \
         IS_NULLABLE, SPANNER_TYPE \
         FROM INFORMATION_SCHEMA.COLUMNS \
         WHERE NOT CAST(IS_HIDDEN AS BOOL) \
         AND TABLE_NAME LIKE @p0 \
         ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION"
    );

    // The subquery correlates on the outer query's `tc` alias.
    let constraints = filtered_query(
        &config,
        "SELECT TABLE_SCHEMA, TABLE_NAME, CONSTRAINT_NAME, CONSTRAINT_TYPE \
         FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc",
        Some(HIDE_HIDDEN_COLUMN_CONSTRAINTS),
        &[("TABLE_SCHEMA", None), ("TABLE_NAME", None)],
        None,
    );
    assert_eq!(
        sql_of(&constraints),
        "SELECT TABLE_SCHEMA, TABLE_NAME, CONSTRAINT_NAME, CONSTRAINT_TYPE \
         FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc \
         WHERE NOT EXISTS ( \
         SELECT 1 FROM INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS u \
         JOIN INFORMATION_SCHEMA.COLUMNS AS c \
         ON c.TABLE_SCHEMA = u.TABLE_SCHEMA AND c.TABLE_NAME = u.TABLE_NAME \
         AND c.COLUMN_NAME = u.COLUMN_NAME \
         WHERE u.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA \
         AND u.CONSTRAINT_NAME = tc.CONSTRAINT_NAME \
         AND CAST(c.IS_HIDDEN AS BOOL))"
    );

    // Without a base predicate, the first filter opens the `WHERE`.
    let schemata = filtered_query(
        &config,
        "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA",
        None,
        &[("SCHEMA_NAME", Some("app"))],
        None,
    );
    assert_eq!(
        sql_of(&schemata),
        "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME LIKE @p0"
    );
}

fn sample() -> Vec<DbSchema> {
    vec![DbSchema {
        name: String::new(),
        tables: vec![Table {
            name: "Users".to_string(),
            table_type: "BASE TABLE".to_string(),
            columns: vec![
                Column {
                    name: "Id".into(),
                    ordinal: 1,
                    nullable: false,
                    type_name: "INT64".into(),
                },
                Column {
                    name: "Name".into(),
                    ordinal: 2,
                    nullable: true,
                    type_name: "STRING(MAX)".into(),
                },
            ],
            constraints: vec![
                Constraint {
                    name: Some("PK_Users".into()),
                    constraint_type: "PRIMARY KEY".into(),
                    columns: vec!["Id".into()],
                    usages: Vec::new(),
                },
                Constraint {
                    name: Some("FK_Users_Org".into()),
                    constraint_type: "FOREIGN KEY".into(),
                    columns: vec!["OrgId".into()],
                    usages: vec![Usage {
                        db_schema: String::new(),
                        table: "Orgs".into(),
                        column: "Id".into(),
                    }],
                },
            ],
        }],
    }]
}

const CATALOG: &str = "adbc-test";

#[test]
fn build_full_depth_matches_schema() {
    let batch = build(ObjectDepth::All, CATALOG, sample()).unwrap();
    assert_eq!(batch.schema(), GET_OBJECTS_SCHEMA.clone());
    assert_eq!(batch.num_rows(), 1);
    let schemas = schemas_list(&batch);
    assert!(schemas.is_valid(0));
    assert_eq!(schemas.value(0).len(), 1);
}

#[test]
fn columns_carry_xdbc_type_name_and_nullability() {
    let batch = build(ObjectDepth::All, CATALOG, sample()).unwrap();
    let columns = table_child_list(&schemas_list(&batch), "table_columns").value(0);
    let columns = columns.as_any().downcast_ref::<StructArray>().unwrap();
    let string_child = |name: &str| {
        columns
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .clone()
    };

    let type_name = string_child("xdbc_type_name");
    assert_eq!(type_name.value(0), "INT64");
    assert_eq!(type_name.value(1), "STRING(MAX)");
    let is_nullable = string_child("xdbc_is_nullable");
    assert_eq!(is_nullable.value(0), "NO");
    assert_eq!(is_nullable.value(1), "YES");
}

/// The `catalog_db_schemas` column.
fn schemas_list(batch: &RecordBatch) -> ListArray {
    batch
        .column(1)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .clone()
}

/// `db_schema_tables` of the catalog's schemas.
fn tables_list(schemas: &ListArray) -> ListArray {
    let schemas = schemas.value(0);
    let schemas = schemas.as_any().downcast_ref::<StructArray>().unwrap();
    schemas
        .column_by_name("db_schema_tables")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .clone()
}

/// A list child, such as `table_columns`, of the first schema's tables.
fn table_child_list(schemas: &ListArray, name: &str) -> ListArray {
    let tables = tables_list(schemas).value(0);
    let tables = tables.as_any().downcast_ref::<StructArray>().unwrap();
    tables
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .clone()
}

#[test]
fn build_catalogs_depth_leaves_schemas_null() {
    let batch = build(ObjectDepth::Catalogs, CATALOG, sample()).unwrap();
    assert!(schemas_list(&batch).is_null(0));
}

#[test]
fn build_catalogs_depth_ignores_collected_schemas() {
    // `collect_objects` skips the queries at catalog depth and passes an empty Vec.
    let from_empty = build(ObjectDepth::Catalogs, CATALOG, Vec::new()).unwrap();
    let from_sample = build(ObjectDepth::Catalogs, CATALOG, sample()).unwrap();
    assert_eq!(from_empty, from_sample);
    assert_eq!(from_empty.num_rows(), 1, "one catalog: the database");
    let name = from_empty
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(name.value(0), CATALOG);
}

/// ADBC reserves NULL for list levels below the requested depth. A filter matching nothing
/// still yields empty lists at and above it; DuckDB got this wrong (duckdb/duckdb#21018).
#[test]
fn schema_filter_matching_nothing_yields_empty_schemas_list_not_null() {
    // A db_schema filter that excludes every schema.
    for depth in [
        ObjectDepth::Schemas,
        ObjectDepth::Tables,
        ObjectDepth::Columns,
        ObjectDepth::All,
    ] {
        let batch = build(depth, CATALOG, Vec::new()).unwrap();
        assert_eq!(batch.num_rows(), 1, "single catalog at {depth:?}");
        let schemas = schemas_list(&batch);
        assert!(
            schemas.is_valid(0),
            "catalog_db_schemas must not be NULL at {depth:?}"
        );
        assert_eq!(
            schemas.value(0).len(),
            0,
            "catalog_db_schemas must be empty at {depth:?}"
        );
    }
}

#[test]
fn table_filter_matching_nothing_keeps_schema_skeleton_with_empty_table_lists() {
    // A table_name or table_type filter that excludes every table.
    for depth in [ObjectDepth::Tables, ObjectDepth::Columns, ObjectDepth::All] {
        let schemas = vec![DbSchema {
            name: String::new(),
            tables: Vec::new(),
        }];
        let batch = build(depth, CATALOG, schemas).unwrap();
        let schemas = schemas_list(&batch);
        assert!(schemas.is_valid(0));
        assert_eq!(
            schemas.value(0).len(),
            1,
            "schema skeleton kept at {depth:?}"
        );
        let tables = tables_list(&schemas);
        assert!(
            tables.is_valid(0),
            "db_schema_tables must not be NULL at {depth:?}"
        );
        assert_eq!(
            tables.value(0).len(),
            0,
            "db_schema_tables must be empty at {depth:?}"
        );
    }
}

#[test]
fn column_filter_matching_nothing_yields_empty_column_and_constraint_lists() {
    // A column_name filter that excludes every column.
    for depth in [ObjectDepth::Columns, ObjectDepth::All] {
        let schemas = vec![DbSchema {
            name: String::new(),
            tables: vec![Table {
                name: "Users".to_string(),
                table_type: "BASE TABLE".to_string(),
                columns: Vec::new(),
                constraints: Vec::new(),
            }],
        }];
        let batch = build(depth, CATALOG, schemas).unwrap();
        let schemas = schemas_list(&batch);
        for name in ["table_columns", "table_constraints"] {
            let list = table_child_list(&schemas, name);
            assert!(list.is_valid(0), "{name} must not be NULL at {depth:?}");
            assert_eq!(list.value(0).len(), 0, "{name} must be empty at {depth:?}");
        }
    }
}

#[test]
fn schemas_depth_leaves_tables_null() {
    let batch = build(ObjectDepth::Schemas, CATALOG, sample()).unwrap();
    let schemas = schemas_list(&batch);
    assert!(schemas.is_valid(0));
    let tables = tables_list(&schemas);
    assert!(
        tables.is_null(0),
        "db_schema_tables must be NULL at Schemas depth"
    );
}

#[test]
fn tables_depth_leaves_columns_and_constraints_null() {
    let batch = build(ObjectDepth::Tables, CATALOG, sample()).unwrap();
    let schemas = schemas_list(&batch);
    for name in ["table_columns", "table_constraints"] {
        let list = table_child_list(&schemas, name);
        assert!(list.is_null(0), "{name} must be NULL at Tables depth");
    }
}

#[test]
fn sql_like_pattern_doubles_backslashes_only() {
    assert_eq!(sql_like_pattern("Sing%"), "Sing%");
    assert_eq!(sql_like_pattern("S_ngers"), "S_ngers");
    assert_eq!(sql_like_pattern(r"a\_b"), r"a\\_b");
    assert_eq!(sql_like_pattern(r"a\\b"), r"a\\\\b");
    assert_eq!(sql_like_pattern(""), "");
}

#[test]
fn filtered_sql_pushes_present_filters_as_bound_params() {
    // No filters: the base SQL is unchanged.
    let (sql, params) = filtered_sql(
        "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA",
        None,
        &[("SCHEMA_NAME", None)],
        None,
    );
    assert_eq!(sql, "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA");
    assert!(params.is_empty());

    // Pattern text goes only into the parameters.
    let (sql, params) = filtered_sql(
        "SELECT C FROM T",
        None,
        &[
            ("TABLE_SCHEMA", None),
            ("TABLE_NAME", Some("Sing%")),
            ("COLUMN_NAME", Some(r"evil'\--%")),
        ],
        Some("A, B"),
    );
    assert_eq!(
        sql,
        "SELECT C FROM T WHERE TABLE_NAME LIKE @p0 AND COLUMN_NAME LIKE @p1 ORDER BY A, B"
    );
    assert_eq!(
        params,
        vec![
            ("p0".to_string(), "Sing%".to_string()),
            ("p1".to_string(), r"evil'\\--%".to_string()),
        ]
    );
}

#[test]
fn filtered_sql_chains_the_base_predicate_before_the_filters() {
    // Alone, the base predicate is the whole `WHERE`.
    let (sql, params) = filtered_sql("SELECT C FROM T", Some("X"), &[("A", None)], None);
    assert_eq!(sql, "SELECT C FROM T WHERE X");
    assert!(params.is_empty());

    // Filters chain after it with `AND`.
    let (sql, params) = filtered_sql(
        "SELECT C FROM T",
        Some("X"),
        &[("A", Some("a%")), ("B", Some("b%"))],
        Some("A"),
    );
    assert_eq!(
        sql,
        "SELECT C FROM T WHERE X AND A LIKE @p0 AND B LIKE @p1 ORDER BY A"
    );
    assert_eq!(params.len(), 2);
}

#[test]
fn constraint_column_usage_is_null_for_non_foreign_keys() {
    let batch = build(ObjectDepth::All, CATALOG, sample()).unwrap();
    let constraints = table_child_list(&schemas_list(&batch), "table_constraints").value(0);
    let constraints = constraints.as_any().downcast_ref::<StructArray>().unwrap();

    let ctype = constraints
        .column_by_name("constraint_type")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let usage = constraints
        .column_by_name("constraint_column_usage")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();

    assert_eq!(ctype.value(0), "PRIMARY KEY");
    assert!(usage.is_null(0), "PRIMARY KEY usage must be NULL, not []");
    assert_eq!(ctype.value(1), "FOREIGN KEY");
    assert!(usage.is_valid(1));
    assert_eq!(usage.value(1).len(), 1);
}

#[test]
fn group_opt_groups_a_present_batch_and_defaults_an_absent_one() {
    let col = |vals: [&str; 3]| Arc::new(StringArray::from(vals.to_vec())) as ArrayRef;
    // Columns in `tables_col` order.
    let batch = RecordBatch::try_from_iter([
        ("TABLE_SCHEMA", col(["", "", "INFORMATION_SCHEMA"])),
        ("TABLE_NAME", col(["Users", "Orgs", "TABLES"])),
        ("TABLE_TYPE", col(["BASE TABLE", "BASE TABLE", "VIEW"])),
    ])
    .unwrap();

    let grouped = group_opt(Some(&batch), group_tables).unwrap();
    assert_eq!(grouped.len(), 2);
    // Batch order is kept within a schema.
    let names: Vec<_> = grouped[""].iter().map(|t| t.name).collect();
    assert_eq!(names, ["Users", "Orgs"]);
    assert_eq!(grouped["INFORMATION_SCHEMA"][0].table_type, "VIEW");

    assert!(group_opt(None, group_tables).unwrap().is_empty());
}

/// String columns in `*_col` order; `None` is NULL.
fn str_batch(columns: &[(&str, Vec<Option<&str>>)]) -> RecordBatch {
    RecordBatch::try_from_iter(columns.iter().map(|(name, values)| {
        (
            *name,
            Arc::new(StringArray::from(values.clone())) as ArrayRef,
        )
    }))
    .unwrap()
}

/// An `All`-depth fetch: the unnamed schema holds `Users` and `Orgs`, with a two-column foreign
/// key from `Users` to the primary key of `Orgs`, and `INFORMATION_SCHEMA` holds one view.
///
/// The foreign key is cross-ordered: its first column (`OrgId`) references the primary key's
/// second column (`Orgs.Id`) and vice versa, so usages built in the referenced key's order come
/// out reversed.
fn sample_batches() -> ObjectBatches {
    fn s(value: &str) -> Option<&str> {
        Some(value)
    }
    ObjectBatches {
        schemata: str_batch(&[("SCHEMA_NAME", vec![s(""), s("INFORMATION_SCHEMA")])]),
        tables: Some(str_batch(&[
            ("TABLE_SCHEMA", vec![s(""), s(""), s("INFORMATION_SCHEMA")]),
            ("TABLE_NAME", vec![s("Users"), s("Orgs"), s("TABLES")]),
            (
                "TABLE_TYPE",
                vec![s("BASE TABLE"), s("BASE TABLE"), s("VIEW")],
            ),
        ])),
        columns: Some(
            RecordBatch::try_from_iter([
                (
                    "TABLE_SCHEMA",
                    Arc::new(StringArray::from(vec![""; 5])) as ArrayRef,
                ),
                (
                    "TABLE_NAME",
                    Arc::new(StringArray::from(vec![
                        "Users", "Users", "Users", "Orgs", "Orgs",
                    ])) as ArrayRef,
                ),
                (
                    "COLUMN_NAME",
                    Arc::new(StringArray::from(vec![
                        "Id",
                        "OrgRegion",
                        "OrgId",
                        "Region",
                        "Id",
                    ])) as ArrayRef,
                ),
                (
                    "ORDINAL_POSITION",
                    Arc::new(Int64Array::from(vec![1, 2, 3, 1, 2])) as ArrayRef,
                ),
                (
                    "IS_NULLABLE",
                    Arc::new(StringArray::from(vec!["NO", "YES", "YES", "NO", "NO"])) as ArrayRef,
                ),
                (
                    "SPANNER_TYPE",
                    Arc::new(StringArray::from(vec![
                        "INT64",
                        "STRING(MAX)",
                        "INT64",
                        "STRING(MAX)",
                        "INT64",
                    ])) as ArrayRef,
                ),
            ])
            .unwrap(),
        ),
        constraints: Some(str_batch(&[
            ("TABLE_SCHEMA", vec![s(""), s(""), s("")]),
            ("TABLE_NAME", vec![s("Users"), s("Users"), s("Orgs")]),
            (
                "CONSTRAINT_NAME",
                vec![s("PK_Users"), s("FK_Users_Orgs"), s("PK_Orgs")],
            ),
            (
                "CONSTRAINT_TYPE",
                vec![s("PRIMARY KEY"), s("FOREIGN KEY"), s("PRIMARY KEY")],
            ),
        ])),
        // In the query's ORDER BY order.
        key_columns: Some(str_batch(&[
            ("CONSTRAINT_SCHEMA", vec![s(""); 5]),
            (
                "CONSTRAINT_NAME",
                vec![
                    s("FK_Users_Orgs"),
                    s("FK_Users_Orgs"),
                    s("PK_Orgs"),
                    s("PK_Orgs"),
                    s("PK_Users"),
                ],
            ),
            ("TABLE_SCHEMA", vec![s(""); 5]),
            (
                "TABLE_NAME",
                vec![s("Users"), s("Users"), s("Orgs"), s("Orgs"), s("Users")],
            ),
            (
                "COLUMN_NAME",
                vec![s("OrgId"), s("OrgRegion"), s("Region"), s("Id"), s("Id")],
            ),
            (
                "ORDINAL_POSITION",
                vec![s("1"), s("2"), s("1"), s("2"), s("1")],
            ),
            (
                "POSITION_IN_UNIQUE_CONSTRAINT",
                vec![s("2"), s("1"), None, None, None],
            ),
        ])),
        referential: Some(str_batch(&[
            ("CONSTRAINT_SCHEMA", vec![s("")]),
            ("CONSTRAINT_NAME", vec![s("FK_Users_Orgs")]),
            ("UNIQUE_CONSTRAINT_SCHEMA", vec![s("")]),
            ("UNIQUE_CONSTRAINT_NAME", vec![s("PK_Orgs")]),
        ])),
    }
}

fn no_filters() -> ObjectFilters<'static> {
    ObjectFilters {
        db_schema: None,
        table_name: None,
        table_type: None,
        column_name: None,
    }
}

#[test]
fn assemble_objects_builds_the_hierarchy_in_batch_order() {
    let schemas = assemble_objects(&sample_batches(), &no_filters()).unwrap();

    let names: Vec<_> = schemas.iter().map(|s| s.name.as_str()).collect();
    assert_eq!(names, ["", "INFORMATION_SCHEMA"]);
    let tables: Vec<_> = schemas[0].tables.iter().map(|t| t.name.as_str()).collect();
    assert_eq!(tables, ["Users", "Orgs"]);
    assert_eq!(schemas[1].tables[0].table_type, "VIEW");

    let users = &schemas[0].tables[0];
    let columns: Vec<_> = users.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(columns, ["Id", "OrgRegion", "OrgId"]);
    assert_eq!(users.columns[1].type_name, "STRING(MAX)");
    assert_eq!(users.columns[1].ordinal, 2);
    assert!(!users.columns[0].nullable);
    assert!(users.columns[1].nullable);
}

#[test]
fn assemble_objects_orders_foreign_key_usages_by_position_in_unique_constraint() {
    let schemas = assemble_objects(&sample_batches(), &no_filters()).unwrap();
    let users = &schemas[0].tables[0];

    let pk = &users.constraints[0];
    assert_eq!(pk.constraint_type, "PRIMARY KEY");
    assert_eq!(pk.columns, ["Id"]);
    assert!(pk.usages.is_empty(), "a primary key references nothing");

    // Usages follow POSITION_IN_UNIQUE_CONSTRAINT, not the referenced key's own order
    // (`Orgs(Region, Id)`).
    let fk = &users.constraints[1];
    assert_eq!(fk.name.as_deref(), Some("FK_Users_Orgs"));
    assert_eq!(fk.columns, ["OrgId", "OrgRegion"]);
    let usages: Vec<_> = fk
        .usages
        .iter()
        .map(|u| (u.db_schema.as_str(), u.table.as_str(), u.column.as_str()))
        .collect();
    assert_eq!(usages, [("", "Orgs", "Id"), ("", "Orgs", "Region")]);
}

#[test]
fn assemble_objects_applies_every_filter_client_side() {
    // The batches are unfiltered, as if the push-down had matched everything.
    let types = ["BASE TABLE"];
    let schemas = assemble_objects(
        &sample_batches(),
        &ObjectFilters {
            db_schema: Some(""),
            table_name: Some("User%"),
            table_type: Some(&types),
            column_name: Some("Org%"),
        },
    )
    .unwrap();
    assert_eq!(schemas.len(), 1, "INFORMATION_SCHEMA is filtered out");
    assert_eq!(schemas[0].name, "");
    let tables: Vec<_> = schemas[0].tables.iter().map(|t| t.name.as_str()).collect();
    assert_eq!(tables, ["Users"], "Orgs does not match `User%`");
    let columns: Vec<_> = schemas[0].tables[0]
        .columns
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    assert_eq!(columns, ["OrgRegion", "OrgId"], "Id does not match `Org%`");

    // A table_type filter matching nothing keeps the schemas, with no tables.
    let types = ["NO SUCH TYPE"];
    let schemas = assemble_objects(
        &sample_batches(),
        &ObjectFilters {
            table_type: Some(&types),
            ..no_filters()
        },
    )
    .unwrap();
    assert_eq!(schemas.len(), 2);
    assert!(schemas.iter().all(|s| s.tables.is_empty()));
}

#[test]
fn assemble_objects_yields_bare_schemas_when_only_schemata_was_fetched() {
    let batches = ObjectBatches {
        tables: None,
        columns: None,
        constraints: None,
        key_columns: None,
        referential: None,
        ..sample_batches()
    };
    let schemas = assemble_objects(&batches, &no_filters()).unwrap();
    assert_eq!(schemas.len(), 2);
    assert!(schemas.iter().all(|s| s.tables.is_empty()));
}

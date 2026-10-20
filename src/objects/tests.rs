use super::*;
use arrow_array::{ListArray, StructArray};

const CATALOG: &str = "adbc-test";

/// A string column; `None` is NULL.
fn strs(values: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(StringArray::from(values))
}

/// A batch of string columns, in SELECT-list order.
fn str_batch(columns: Vec<(&str, Vec<Option<&str>>)>) -> RecordBatch {
    RecordBatch::try_from_iter(
        columns
            .into_iter()
            .map(|(name, values)| (name, strs(values))),
    )
    .unwrap()
}

fn s(value: &str) -> Option<&str> {
    Some(value)
}

/// An `All`-depth fetch: the unnamed schema holds `Users` and `Orgs`, with a two-column foreign
/// key from `Users` to the primary key of `Orgs`, and `INFORMATION_SCHEMA` holds one view.
fn sample_batches() -> ObjectBatches {
    let columns = RecordBatch::try_from_iter([
        ("TABLE_SCHEMA", strs(vec![s(""); 5])),
        (
            "TABLE_NAME",
            strs(vec![
                s("Users"),
                s("Users"),
                s("Users"),
                s("Orgs"),
                s("Orgs"),
            ]),
        ),
        (
            "COLUMN_NAME",
            strs(vec![
                s("Id"),
                s("OrgRegion"),
                s("OrgId"),
                s("Region"),
                s("Id"),
            ]),
        ),
        (
            "IS_NULLABLE",
            strs(vec![s("NO"), s("YES"), s("YES"), s("NO"), s("NO")]),
        ),
        (
            "SPANNER_TYPE",
            strs(vec![
                s("INT64"),
                s("STRING(MAX)"),
                s("INT64"),
                s("STRING(10)"),
                s("INT64"),
            ]),
        ),
        (
            "COLUMN_DEFAULT",
            strs(vec![None, s("'eu'"), None, None, None]),
        ),
        (
            "IS_GENERATED",
            strs(vec![
                s("NEVER"),
                s("NEVER"),
                s("ALWAYS"),
                s("NEVER"),
                s("NEVER"),
            ]),
        ),
        (
            "ORDINAL_POSITION",
            Arc::new(Int64Array::from(vec![1, 2, 3, 1, 2])),
        ),
    ])
    .unwrap();
    ObjectBatches {
        schemata: str_batch(vec![("SCHEMA_NAME", vec![s(""), s("INFORMATION_SCHEMA")])]),
        tables: Some(str_batch(vec![
            ("TABLE_SCHEMA", vec![s(""), s(""), s("INFORMATION_SCHEMA")]),
            ("TABLE_NAME", vec![s("Users"), s("Orgs"), s("TABLES")]),
            (
                "TABLE_TYPE",
                vec![s("BASE TABLE"), s("BASE TABLE"), s("VIEW")],
            ),
        ])),
        columns: Some(columns),
        // In the query's ORDER BY order. The check constraint has no key columns.
        constraints: Some(str_batch(vec![
            ("TABLE_SCHEMA", vec![s(""); 6]),
            (
                "TABLE_NAME",
                vec![
                    s("Orgs"),
                    s("Orgs"),
                    s("Users"),
                    s("Users"),
                    s("Users"),
                    s("Users"),
                ],
            ),
            (
                "CONSTRAINT_NAME",
                vec![
                    s("PK_Orgs"),
                    s("PK_Orgs"),
                    s("CK_Users"),
                    s("FK_Users_Orgs"),
                    s("FK_Users_Orgs"),
                    s("PK_Users"),
                ],
            ),
            (
                "CONSTRAINT_TYPE",
                vec![
                    s("PRIMARY KEY"),
                    s("PRIMARY KEY"),
                    s("CHECK"),
                    s("FOREIGN KEY"),
                    s("FOREIGN KEY"),
                    s("PRIMARY KEY"),
                ],
            ),
            (
                "COLUMN_NAME",
                vec![
                    s("Region"),
                    s("Id"),
                    None,
                    s("OrgId"),
                    s("OrgRegion"),
                    s("Id"),
                ],
            ),
            ("REF_SCHEMA", vec![None, None, None, s(""), s(""), None]),
            (
                "REF_TABLE",
                vec![None, None, None, s("Orgs"), s("Orgs"), None],
            ),
            (
                "REF_COLUMN",
                vec![None, None, None, s("Id"), s("Region"), None],
            ),
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
    assert!(schemas[1].tables[0].columns.is_empty());

    let users = &schemas[0].tables[0];
    let columns: Vec<_> = users
        .columns
        .iter()
        .map(|c| (c.name.as_str(), c.ordinal, c.nullable, c.type_name.as_str()))
        .collect();
    assert_eq!(
        columns,
        [
            ("Id", 1, false, "INT64"),
            ("OrgRegion", 2, true, "STRING(MAX)"),
            ("OrgId", 3, true, "INT64"),
        ]
    );
}

#[test]
fn assemble_objects_groups_constraint_rows_into_constraints() {
    let schemas = assemble_objects(&sample_batches(), &no_filters()).unwrap();
    let constraints = |table: usize| {
        schemas[0].tables[table]
            .constraints
            .iter()
            .map(|c| {
                let usages = c.usages.as_ref().map(|usages| {
                    usages
                        .iter()
                        .map(|u| format!("{}.{}.{}", u.db_schema, u.table, u.column))
                        .collect::<Vec<_>>()
                });
                (
                    c.name.as_str(),
                    c.constraint_type.as_str(),
                    c.columns.clone(),
                    usages,
                )
            })
            .collect::<Vec<_>>()
    };

    assert_eq!(
        constraints(0),
        [
            ("CK_Users", "CHECK", vec![], None),
            (
                "FK_Users_Orgs",
                "FOREIGN KEY",
                vec!["OrgId".to_string(), "OrgRegion".to_string()],
                Some(vec![".Orgs.Id".to_string(), ".Orgs.Region".to_string()]),
            ),
            ("PK_Users", "PRIMARY KEY", vec!["Id".to_string()], None),
        ]
    );
    assert_eq!(
        constraints(1),
        [(
            "PK_Orgs",
            "PRIMARY KEY",
            vec!["Region".to_string(), "Id".to_string()],
            None
        )]
    );
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
    let users = &schemas[0].tables[0];
    let columns: Vec<_> = users.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(columns, ["OrgRegion", "OrgId"], "Id does not match `Org%`");
    assert_eq!(
        users.constraints.len(),
        3,
        "constraints ignore the column filter"
    );

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
        ..sample_batches()
    };
    let schemas = assemble_objects(&batches, &no_filters()).unwrap();
    assert_eq!(schemas.len(), 2);
    assert!(schemas.iter().all(|s| s.tables.is_empty()));
}

/// The `catalog_db_schemas` list.
fn schemas_list(batch: &RecordBatch) -> &ListArray {
    batch.column(1).as_any().downcast_ref().unwrap()
}

/// A list child of the first entry of a struct list, such as `db_schema_tables` of the first
/// schema.
fn child_list(list: &ListArray, name: &str) -> ListArray {
    let first = list.value(0);
    let first = first.as_any().downcast_ref::<StructArray>().unwrap();
    first
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .clone()
}

/// Each depth fills its own level and leaves the levels below it null.
#[test]
fn build_fills_levels_down_to_the_requested_depth() {
    let schemas = || assemble_objects(&sample_batches(), &no_filters()).unwrap();

    let batch = build(ObjectDepth::Catalogs, CATALOG, schemas()).unwrap();
    assert_eq!(batch.schema(), GET_OBJECTS_SCHEMA.clone());
    assert_eq!(batch.num_rows(), 1, "one catalog: the database");
    let catalog = batch.column(0).as_any().downcast_ref::<StringArray>();
    assert_eq!(catalog.unwrap().value(0), CATALOG);
    assert!(schemas_list(&batch).is_null(0));

    let batch = build(ObjectDepth::Schemas, CATALOG, schemas()).unwrap();
    assert_eq!(schemas_list(&batch).value(0).len(), 2);
    assert!(child_list(schemas_list(&batch), "db_schema_tables").is_null(0));

    let batch = build(ObjectDepth::Tables, CATALOG, schemas()).unwrap();
    let tables = child_list(schemas_list(&batch), "db_schema_tables");
    assert_eq!(tables.value(0).len(), 2);
    for name in ["table_columns", "table_constraints"] {
        assert!(
            child_list(&tables, name).is_null(0),
            "{name} at Tables depth"
        );
    }

    for depth in [ObjectDepth::Columns, ObjectDepth::All] {
        let batch = build(depth, CATALOG, schemas()).unwrap();
        assert_eq!(batch.schema(), GET_OBJECTS_SCHEMA.clone());
        let tables = child_list(schemas_list(&batch), "db_schema_tables");
        assert_eq!(child_list(&tables, "table_columns").value(0).len(), 3);
        assert_eq!(child_list(&tables, "table_constraints").value(0).len(), 3);
    }
}

/// ADBC reserves NULL for list levels below the requested depth. A filter matching nothing
/// still yields empty lists at and above it; DuckDB got this wrong (duckdb/duckdb#21018).
#[test]
fn filters_matching_nothing_yield_empty_lists_not_null() {
    for depth in [ObjectDepth::Schemas, ObjectDepth::Tables, ObjectDepth::All] {
        let batch = build(depth, CATALOG, Vec::new()).unwrap();
        let schemas = schemas_list(&batch);
        assert!(schemas.is_valid(0), "{depth:?}");
        assert_eq!(schemas.value(0).len(), 0, "{depth:?}");
    }

    let empty_schema = || {
        vec![DbSchema {
            name: String::new(),
            tables: Vec::new(),
        }]
    };
    let batch = build(ObjectDepth::Tables, CATALOG, empty_schema()).unwrap();
    let tables = child_list(schemas_list(&batch), "db_schema_tables");
    assert!(tables.is_valid(0));
    assert_eq!(tables.value(0).len(), 0);

    let mut schemas = empty_schema();
    schemas[0].tables.push(Table {
        name: "Users".into(),
        table_type: "BASE TABLE".into(),
        columns: Vec::new(),
        constraints: Vec::new(),
    });
    let batch = build(ObjectDepth::All, CATALOG, schemas).unwrap();
    let tables = child_list(schemas_list(&batch), "db_schema_tables");
    for name in ["table_columns", "table_constraints"] {
        let list = child_list(&tables, name);
        assert!(list.is_valid(0), "{name}");
        assert_eq!(list.value(0).len(), 0, "{name}");
    }
}

#[test]
fn build_reports_column_metadata_and_foreign_key_usages() {
    let schemas = assemble_objects(&sample_batches(), &no_filters()).unwrap();
    let batch = build(ObjectDepth::All, CATALOG, schemas).unwrap();
    let tables = child_list(schemas_list(&batch), "db_schema_tables");
    let child = |list: &ListArray, name: &str| {
        let first = list.value(0);
        let first = first.as_any().downcast_ref::<StructArray>().unwrap();
        first.column_by_name(name).unwrap().clone()
    };
    let string_child = |list: &ListArray, name: &str| {
        let strings = child(list, name);
        let strings = strings.as_any().downcast_ref::<StringArray>().unwrap();
        strings
            .iter()
            .map(|v| v.unwrap_or("NULL").to_string())
            .collect::<Vec<_>>()
    };

    let columns = child_list(&tables, "table_columns");
    assert_eq!(
        string_child(&columns, "xdbc_type_name"),
        ["INT64", "STRING(MAX)", "INT64"]
    );
    assert_eq!(
        string_child(&columns, "xdbc_is_nullable"),
        ["NO", "YES", "YES"]
    );
    let nullable = child(&columns, "xdbc_nullable");
    let nullable = nullable.as_any().downcast_ref::<Int16Array>().unwrap();
    assert_eq!(nullable.values().to_vec(), [0, 1, 1]);
    assert_eq!(
        string_child(&columns, "xdbc_column_def"),
        ["NULL", "'eu'", "NULL"]
    );
    let generated = child(&columns, "xdbc_is_generatedcolumn");
    let generated = generated.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert_eq!(
        generated.iter().collect::<Vec<_>>(),
        [Some(false), Some(false), Some(true)]
    );
    assert_eq!(child(&columns, "xdbc_data_type").null_count(), 3);

    // The spec and the validation suite expect a NULL usage list for constraints other than
    // foreign keys.
    let constraints = child_list(&tables, "table_constraints");
    assert_eq!(
        string_child(&constraints, "constraint_type"),
        ["CHECK", "FOREIGN KEY", "PRIMARY KEY"]
    );
    let first = constraints.value(0);
    let first = first.as_any().downcast_ref::<StructArray>().unwrap();
    let usages = first.column_by_name("constraint_column_usage").unwrap();
    let usages = usages.as_any().downcast_ref::<ListArray>().unwrap();
    assert!(usages.is_null(0) && usages.is_null(2));
    assert!(usages.is_valid(1));
    assert_eq!(
        string_child(&usages.slice(1, 1), "fk_catalog"),
        [CATALOG; 2]
    );
    assert_eq!(
        string_child(&usages.slice(1, 1), "fk_column_name"),
        ["Id", "Region"]
    );
}

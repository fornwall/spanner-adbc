//! Builds the nested [`Connection::get_objects`](adbc_core::Connection::get_objects) result from
//! `INFORMATION_SCHEMA`.
//!
//! Levels below the requested [`ObjectDepth`] are null. Lists at or above it are always valid,
//! possibly empty: per the ADBC spec, a filter that matches nothing still yields the parent
//! skeleton.

use std::collections::HashMap;
use std::sync::Arc;

use adbc_core::error::Result;
use adbc_core::options::ObjectDepth;
use adbc_core::schemas::GET_OBJECTS_SCHEMA;
use arrow_array::{
    Array, ArrayRef, BooleanArray, Int16Array, Int32Array, Int64Array, RecordBatch, StringArray,
    new_null_array,
};
use arrow_buffer::NullBuffer;
use arrow_schema::FieldRef;
use futures_util::try_join;
use google_cloud_spanner::client::DatabaseClient;
use google_cloud_spanner::statement::Statement as SpannerSql;
use google_cloud_spanner::transaction::MultiUseReadOnlyTransaction;

use crate::metadata::{LikeMatcher, column, filtered_query, query_txn, str_cols};
use crate::nested::{arrow_err, field, list_item, list_of, strings, struct_list};
use crate::options::SharedConfig;
use crate::runtime::{CancelSignal, SharedRuntime, block_on_cancellable};
use crate::timeout::with_timeout;

/// A column of a table, as returned by `get_objects`.
pub(crate) struct Column {
    name: String,
    ordinal: i32,
    nullable: bool,
    /// `SPANNER_TYPE`, e.g. `STRING(MAX)`; reported as `xdbc_type_name`.
    type_name: String,
    /// The `DEFAULT` expression.
    default: Option<String>,
    generated: bool,
}

/// A column that a foreign key references.
pub(crate) struct Usage {
    db_schema: String,
    table: String,
    column: String,
}

/// A constraint on a table.
pub(crate) struct Constraint {
    name: String,
    /// `CHECK`, `FOREIGN KEY`, `PRIMARY KEY` or `UNIQUE`.
    constraint_type: String,
    /// Key columns in key order, or the columns a check constraint reads, by name.
    columns: Vec<String>,
    /// A foreign key's referenced columns, in the order of `columns`. `None` for other
    /// constraints, which the spec reports with a null usage list.
    usages: Option<Vec<Usage>>,
}

/// A table with its columns and constraints.
pub(crate) struct Table {
    name: String,
    table_type: String,
    columns: Vec<Column>,
    constraints: Vec<Constraint>,
}

/// A database schema with its tables.
pub(crate) struct DbSchema {
    name: String,
    tables: Vec<Table>,
}

/// `get_objects` filters: ADBC `LIKE` patterns for names and an exact-match list of table types.
/// `None` means no filter. So does an empty `table_type` list, as in the C++ reference drivers;
/// the Go and Python driver managers pass an empty list as NULL.
pub(crate) struct ObjectFilters<'a> {
    pub db_schema: Option<&'a str>,
    pub table_name: Option<&'a str>,
    pub table_type: Option<&'a [&'a str]>,
    pub column_name: Option<&'a str>,
}

// The `collect_objects` queries. Each SELECT list starts with its string columns, in the order
// `assemble_objects` reads them.
const SCHEMATA_SQL: &str = "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA";
const TABLES_SQL: &str =
    "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES";
const COLUMNS_SQL: &str = "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, IS_NULLABLE, \
     SPANNER_TYPE, COLUMN_DEFAULT, IS_GENERATED, ORDINAL_POSITION \
     FROM INFORMATION_SCHEMA.COLUMNS";

/// Keeps only the columns `SELECT *` returns, so `get_objects` agrees with `get_table_schema`.
/// Spanner hides the implicit `rowid` key that bulk ingest creates for tables without a primary
/// key (see [`bind::create_table_sql`](crate::bind::create_table_sql)).
///
/// `IS_HIDDEN` is documented as the `STRING` `"TRUE"`/`"FALSE"`, but the emulator types it as a
/// `BOOL`; the `CAST` accepts both.
const COLUMNS_PREDICATE: &str = "NOT CAST(IS_HIDDEN AS BOOL)";

/// One row per constraint column, with the column a foreign key column references.
/// `POSITION_IN_UNIQUE_CONSTRAINT` is that column's position in the referenced constraint. Check
/// constraints have no key columns, so their columns come from `CONSTRAINT_COLUMN_USAGE`. A
/// constraint without columns is one row with null columns.
const CONSTRAINTS_SQL: &str = "SELECT tc.TABLE_SCHEMA, tc.TABLE_NAME, tc.CONSTRAINT_NAME, \
     tc.CONSTRAINT_TYPE, COALESCE(k.COLUMN_NAME, ck.COLUMN_NAME) AS COLUMN_NAME, \
     rk.TABLE_SCHEMA AS REF_SCHEMA, rk.TABLE_NAME AS REF_TABLE, rk.COLUMN_NAME AS REF_COLUMN \
     FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc \
     LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS k \
       ON k.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA AND k.CONSTRAINT_NAME = tc.CONSTRAINT_NAME \
     LEFT JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS ck \
       ON tc.CONSTRAINT_TYPE = 'CHECK' AND ck.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA \
      AND ck.CONSTRAINT_NAME = tc.CONSTRAINT_NAME \
     LEFT JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS r \
       ON r.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA AND r.CONSTRAINT_NAME = tc.CONSTRAINT_NAME \
     LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS rk \
       ON rk.CONSTRAINT_SCHEMA = r.UNIQUE_CONSTRAINT_SCHEMA \
      AND rk.CONSTRAINT_NAME = r.UNIQUE_CONSTRAINT_NAME \
      AND rk.ORDINAL_POSITION = k.POSITION_IN_UNIQUE_CONSTRAINT";

/// Keeps the constraint types the ADBC spec defines, which leaves out placement keys. Drops the
/// check Spanner lists for each NOT NULL column, which `xdbc_nullable` already reports; DDL can't
/// use its `CK_IS_NOT_NULL_` prefix. Also drops constraints on hidden columns, so no constraint
/// names a column `get_objects` omits.
///
/// A table without a primary key gets a `PK_<table>` primary key on its hidden `rowid`.
/// `CONSTRAINT_COLUMN_USAGE` names that column, and for a foreign key the referenced columns,
/// which are never hidden.
const CONSTRAINTS_PREDICATE: &str = "tc.CONSTRAINT_TYPE IN \
     ('CHECK', 'FOREIGN KEY', 'PRIMARY KEY', 'UNIQUE') \
     AND NOT STARTS_WITH(tc.CONSTRAINT_NAME, 'CK_IS_NOT_NULL_') \
     AND NOT EXISTS ( \
     SELECT 1 FROM INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS cu \
     JOIN INFORMATION_SCHEMA.COLUMNS AS c \
       ON c.TABLE_SCHEMA = cu.TABLE_SCHEMA AND c.TABLE_NAME = cu.TABLE_NAME \
      AND c.COLUMN_NAME = cu.COLUMN_NAME \
     WHERE cu.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA \
       AND cu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME \
       AND CAST(c.IS_HIDDEN AS BOOL))";

/// Batches fetched by [`collect_objects`]; `None` when the depth skips the query.
struct ObjectBatches {
    schemata: RecordBatch,
    tables: Option<RecordBatch>,
    columns: Option<RecordBatch>,
    constraints: Option<RecordBatch>,
}

/// Query `INFORMATION_SCHEMA` and assemble the schema/table/column hierarchy for `get_objects`.
pub(crate) fn collect_objects(
    runtime: &SharedRuntime,
    client: &DatabaseClient,
    cancel: &CancelSignal,
    config: &SharedConfig,
    depth: ObjectDepth,
    filters: &ObjectFilters<'_>,
) -> Result<Vec<DbSchema>> {
    // `build` ignores the schemas at catalog depth. Deeper levels fetch only what they populate.
    if depth == ObjectDepth::Catalogs {
        return Ok(Vec::new());
    }
    let with_tables = depth != ObjectDepth::Schemas;
    let with_columns = matches!(depth, ObjectDepth::All | ObjectDepth::Columns);
    let ObjectFilters {
        db_schema,
        table_name,
        column_name,
        ..
    } = *filters;

    // Pattern filters are pushed down only to cut transferred rows. `assemble_objects` applies
    // them authoritatively, so differences between GoogleSQL `LIKE` and ADBC patterns cannot
    // change the result.
    let schemata = filtered_query(
        config,
        SCHEMATA_SQL,
        None,
        &[("SCHEMA_NAME", db_schema)],
        "SCHEMA_NAME",
    );
    let tables = with_tables.then(|| {
        filtered_query(
            config,
            TABLES_SQL,
            None,
            &[("TABLE_SCHEMA", db_schema), ("TABLE_NAME", table_name)],
            "TABLE_SCHEMA, TABLE_NAME",
        )
    });
    let columns = with_columns.then(|| {
        filtered_query(
            config,
            COLUMNS_SQL,
            Some(COLUMNS_PREDICATE),
            &[
                ("TABLE_SCHEMA", db_schema),
                ("TABLE_NAME", table_name),
                ("COLUMN_NAME", column_name),
            ],
            "TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION",
        )
    });
    let constraints = with_columns.then(|| {
        filtered_query(
            config,
            CONSTRAINTS_SQL,
            Some(CONSTRAINTS_PREDICATE),
            &[
                ("tc.TABLE_SCHEMA", db_schema),
                ("tc.TABLE_NAME", table_name),
            ],
            "tc.TABLE_SCHEMA, tc.TABLE_NAME, tc.CONSTRAINT_NAME, k.ORDINAL_POSITION, \
             ck.COLUMN_NAME",
        )
    });

    let client = client.clone();
    let bound = config.read_staleness.multi_use_timestamp_bound();
    let batches = block_on_cancellable(
        runtime,
        cancel,
        with_timeout(
            config.timeouts.query_timeout(),
            crate::OPTION_RPC_TIMEOUT_QUERY,
            async move {
                // The queries run concurrently on one snapshot. The client's inline begin makes
                // concurrent first statements wait for the transaction id.
                let txn = crate::staleness::multi_use(&client, bound).await?;
                let (schemata, tables, columns, constraints) = try_join!(
                    query_txn(&txn, schemata),
                    query_txn_opt(&txn, tables),
                    query_txn_opt(&txn, columns),
                    query_txn_opt(&txn, constraints),
                )?;
                Ok(ObjectBatches {
                    schemata,
                    tables,
                    columns,
                    constraints,
                })
            },
        ),
    )?;

    assemble_objects(&batches, filters)
}

/// [`query_txn`] for a statement the depth may skip.
async fn query_txn_opt(
    txn: &MultiUseReadOnlyTransaction,
    statement: Option<SpannerSql>,
) -> Result<Option<RecordBatch>> {
    match statement {
        Some(statement) => query_txn(txn, statement).await.map(Some),
        None => Ok(None),
    }
}

/// Assemble the hierarchy from the fetched batches, in their order, and apply the filters. Kept
/// apart from the I/O in [`collect_objects`] so it can be tested offline.
fn assemble_objects(batches: &ObjectBatches, filters: &ObjectFilters<'_>) -> Result<Vec<DbSchema>> {
    let matcher = |pattern: Option<&str>| {
        let matcher = pattern.map(LikeMatcher::new);
        move |value: &str| matcher.as_ref().is_none_or(|m| m.matches(value))
    };
    let (schema_matches, table_matches, column_matches) = (
        matcher(filters.db_schema),
        matcher(filters.table_name),
        matcher(filters.column_name),
    );

    let mut columns: HashMap<(&str, &str), Vec<Column>> = HashMap::new();
    if let Some(batch) = &batches.columns {
        let [schema, table, name, nullable, type_name, default, generated] = str_cols(batch)?;
        let ordinal = column::<Int64Array>(batch, 7)?;
        for r in (0..batch.num_rows()).filter(|&r| column_matches(name.value(r))) {
            columns
                .entry((schema.value(r), table.value(r)))
                .or_default()
                .push(Column {
                    name: name.value(r).to_string(),
                    ordinal: ordinal.value(r) as i32,
                    nullable: nullable.value(r).eq_ignore_ascii_case("YES"),
                    type_name: type_name.value(r).to_string(),
                    default: default.is_valid(r).then(|| default.value(r).to_string()),
                    generated: generated.value(r) == "ALWAYS",
                });
        }
    }

    let mut constraints: HashMap<(&str, &str), Vec<Constraint>> = HashMap::new();
    if let Some(batch) = &batches.constraints {
        let [
            schema,
            table,
            name,
            constraint_type,
            key_column,
            ref_schema,
            ref_table,
            ref_column,
        ] = str_cols(batch)?;
        for r in 0..batch.num_rows() {
            let table_constraints = constraints
                .entry((schema.value(r), table.value(r)))
                .or_default();
            // A constraint's rows are adjacent: the query orders by constraint name.
            if table_constraints
                .last()
                .is_none_or(|c| c.name != name.value(r))
            {
                table_constraints.push(Constraint {
                    name: name.value(r).to_string(),
                    constraint_type: constraint_type.value(r).to_string(),
                    columns: Vec::new(),
                    usages: (constraint_type.value(r) == "FOREIGN KEY").then(Vec::new),
                });
            }
            let Some(constraint) = table_constraints.last_mut() else {
                continue;
            };
            if key_column.is_valid(r) {
                constraint.columns.push(key_column.value(r).to_string());
            }
            if let Some(usages) = &mut constraint.usages
                && ref_column.is_valid(r)
            {
                usages.push(Usage {
                    db_schema: ref_schema.value(r).to_string(),
                    table: ref_table.value(r).to_string(),
                    column: ref_column.value(r).to_string(),
                });
            }
        }
    }

    let mut tables: HashMap<&str, Vec<Table>> = HashMap::new();
    if let Some(batch) = &batches.tables {
        let [schema, name, table_type] = str_cols(batch)?;
        for r in 0..batch.num_rows() {
            let (schema, name, table_type) = (schema.value(r), name.value(r), table_type.value(r));
            if !table_matches(name)
                || filters
                    .table_type
                    .is_some_and(|types| !types.is_empty() && !types.contains(&table_type))
            {
                continue;
            }
            tables.entry(schema).or_default().push(Table {
                name: name.to_string(),
                table_type: table_type.to_string(),
                columns: columns.remove(&(schema, name)).unwrap_or_default(),
                constraints: constraints.remove(&(schema, name)).unwrap_or_default(),
            });
        }
    }

    let [names] = str_cols(&batches.schemata)?;
    Ok(names
        .iter()
        .flatten()
        .filter(|name| schema_matches(name))
        .map(|name| DbSchema {
            name: name.to_string(),
            tables: tables.remove(name).unwrap_or_default(),
        })
        .collect())
}

/// Build the `get_objects` batch for the connection's only catalog. `catalog` is also every
/// foreign-key usage's `fk_catalog`, since foreign keys cannot cross databases.
pub(crate) fn build(
    depth: ObjectDepth,
    catalog: &str,
    schemas: Vec<DbSchema>,
) -> Result<RecordBatch> {
    let out_schema = GET_OBJECTS_SCHEMA.clone();
    let db_schemas_field = field(out_schema.fields(), "catalog_db_schemas")?;
    let db_schemas = if depth == ObjectDepth::Catalogs {
        new_null_array(db_schemas_field.data_type(), 1)
    } else {
        let with_tables = depth != ObjectDepth::Schemas;
        let with_columns = matches!(depth, ObjectDepth::All | ObjectDepth::Columns);
        struct_list(&db_schemas_field, &[schemas.len()], None, |f| {
            match f.name().as_str() {
                "db_schema_name" => Ok(Some(strings(schemas.iter().map(|s| s.name.as_str())))),
                "db_schema_tables" if with_tables => {
                    tables_list(f, catalog, &schemas, with_columns).map(Some)
                }
                _ => Ok(None),
            }
        })?
    };
    RecordBatch::try_new(out_schema, vec![strings([catalog]), db_schemas]).map_err(arrow_err)
}

/// Each schema's `db_schema_tables`.
fn tables_list(
    field: &FieldRef,
    catalog: &str,
    schemas: &[DbSchema],
    with_columns: bool,
) -> Result<ArrayRef> {
    let lengths: Vec<usize> = schemas.iter().map(|s| s.tables.len()).collect();
    let tables: Vec<&Table> = schemas.iter().flat_map(|s| &s.tables).collect();
    struct_list(field, &lengths, None, |f| match f.name().as_str() {
        "table_name" => Ok(Some(strings(tables.iter().map(|t| t.name.as_str())))),
        "table_type" => Ok(Some(strings(tables.iter().map(|t| t.table_type.as_str())))),
        "table_columns" if with_columns => columns_list(f, &tables).map(Some),
        "table_constraints" if with_columns => constraints_list(f, catalog, &tables).map(Some),
        _ => Ok(None),
    })
}

/// Each table's `table_columns`. The other `xdbc_*` fields stay null.
fn columns_list(field: &FieldRef, tables: &[&Table]) -> Result<ArrayRef> {
    let lengths: Vec<usize> = tables.iter().map(|t| t.columns.len()).collect();
    let columns: Vec<&Column> = tables.iter().flat_map(|t| &t.columns).collect();
    struct_list(field, &lengths, None, |f| {
        Ok(match f.name().as_str() {
            "column_name" => Some(strings(columns.iter().map(|c| c.name.as_str()))),
            "ordinal_position" => Some(Arc::new(Int32Array::from_iter_values(
                columns.iter().map(|c| c.ordinal),
            ))),
            "xdbc_type_name" => Some(strings(columns.iter().map(|c| c.type_name.as_str()))),
            // JDBC's `columnNoNulls` (0) and `columnNullable` (1).
            "xdbc_nullable" => Some(Arc::new(Int16Array::from_iter_values(
                columns.iter().map(|c| i16::from(c.nullable)),
            ))),
            "xdbc_column_def" => Some(Arc::new(StringArray::from_iter(
                columns.iter().map(|c| c.default.as_deref()),
            ))),
            "xdbc_is_nullable" => Some(strings(
                columns
                    .iter()
                    .map(|c| if c.nullable { "YES" } else { "NO" }),
            )),
            "xdbc_is_generatedcolumn" => Some(Arc::new(BooleanArray::from_iter(
                columns.iter().map(|c| Some(c.generated)),
            ))),
            _ => None,
        })
    })
}

/// Each table's `table_constraints`.
fn constraints_list(field: &FieldRef, catalog: &str, tables: &[&Table]) -> Result<ArrayRef> {
    let lengths: Vec<usize> = tables.iter().map(|t| t.constraints.len()).collect();
    let constraints: Vec<&Constraint> = tables.iter().flat_map(|t| &t.constraints).collect();
    struct_list(field, &lengths, None, |f| match f.name().as_str() {
        "constraint_name" => Ok(Some(strings(constraints.iter().map(|c| c.name.as_str())))),
        "constraint_type" => Ok(Some(strings(
            constraints.iter().map(|c| c.constraint_type.as_str()),
        ))),
        "constraint_column_names" => {
            let lengths: Vec<usize> = constraints.iter().map(|c| c.columns.len()).collect();
            let names = strings(
                constraints
                    .iter()
                    .flat_map(|c| c.columns.iter().map(String::as_str)),
            );
            list_of(list_item(f)?, &lengths, names).map(Some)
        }
        "constraint_column_usage" => usages_list(f, catalog, &constraints).map(Some),
        _ => Ok(None),
    })
}

/// Each constraint's `constraint_column_usage`: null except for foreign keys.
fn usages_list(field: &FieldRef, catalog: &str, constraints: &[&Constraint]) -> Result<ArrayRef> {
    let lengths: Vec<usize> = constraints
        .iter()
        .map(|c| c.usages.as_ref().map_or(0, Vec::len))
        .collect();
    let nulls = NullBuffer::from_iter(constraints.iter().map(|c| c.usages.is_some()));
    let usages: Vec<&Usage> = constraints
        .iter()
        .flat_map(|c| c.usages.iter().flatten())
        .collect();
    struct_list(field, &lengths, Some(nulls), |f| {
        Ok(match f.name().as_str() {
            "fk_catalog" => Some(strings(usages.iter().map(|_| catalog))),
            "fk_db_schema" => Some(strings(usages.iter().map(|u| u.db_schema.as_str()))),
            "fk_table" => Some(strings(usages.iter().map(|u| u.table.as_str()))),
            "fk_column_name" => Some(strings(usages.iter().map(|u| u.column.as_str()))),
            _ => None,
        })
    })
}

#[cfg(test)]
mod tests;

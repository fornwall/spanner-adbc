//! Builds the nested [`Connection::get_objects`](adbc_core::Connection::get_objects) result from
//! `INFORMATION_SCHEMA`.
//!
//! Levels below the requested [`ObjectDepth`] are null. Lists at or above it are always valid,
//! possibly empty: per the ADBC spec, a filter that matches nothing still yields the parent
//! skeleton.

use std::collections::HashMap;
use std::sync::Arc;

use adbc_core::error::{Result, Status};
use adbc_core::options::ObjectDepth;
use adbc_core::schemas::GET_OBJECTS_SCHEMA;
use arrow_array::{
    Array, ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray, StructArray, new_null_array,
};
use arrow_buffer::NullBuffer;
use arrow_schema::Fields;
use google_cloud_spanner::client::DatabaseClient;

use futures_util::try_join;
use google_cloud_spanner::statement::Statement as SpannerSql;
use google_cloud_spanner::transaction::MultiUseReadOnlyTransaction;

use crate::error::err;
use crate::metadata::{LikeMatcher, metadata_sql_builder, query_txn, str_col};
use crate::nested::{arrow_err, field, list_item, list_of, list_of_nullable, struct_fields};
use crate::options::SharedConfig;
use crate::runtime::{CancelSignal, SharedRuntime, block_on_cancellable};
use crate::timeout::with_timeout;

/// A column of a table, as returned by `get_objects`.
pub(crate) struct Column {
    pub name: String,
    pub ordinal: i32,
    pub nullable: bool,
    /// `SPANNER_TYPE`, e.g. `STRING(MAX)`; reported as `xdbc_type_name`.
    pub type_name: String,
}

/// A column referenced by a foreign-key constraint (the parent side).
pub(crate) struct Usage {
    pub db_schema: String,
    pub table: String,
    pub column: String,
}

/// A constraint on a table (primary key, foreign key, unique, check).
pub(crate) struct Constraint {
    pub name: Option<String>,
    /// e.g. `"PRIMARY KEY"` or `"FOREIGN KEY"`.
    pub constraint_type: String,
    /// Key columns in key order; empty for check constraints.
    pub columns: Vec<String>,
    /// A foreign key's parent columns, in the order of `columns`.
    pub usages: Vec<Usage>,
}

/// A table with its columns and constraints.
pub(crate) struct Table {
    pub name: String,
    pub table_type: String,
    pub columns: Vec<Column>,
    pub constraints: Vec<Constraint>,
}

/// A database schema with its tables.
pub(crate) struct DbSchema {
    pub name: String,
    pub tables: Vec<Table>,
}

/// Column positions in the [`collect_objects`] queries; keep these aligned with their SELECT lists.
mod schemata_col {
    // `SELECT SCHEMA_NAME`
    pub(super) const SCHEMA_NAME: usize = 0;
}
mod tables_col {
    // `SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE`
    pub(super) const TABLE_SCHEMA: usize = 0;
    pub(super) const TABLE_NAME: usize = 1;
    pub(super) const TABLE_TYPE: usize = 2;
}
mod columns_col {
    // `SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, IS_NULLABLE, SPANNER_TYPE`
    pub(super) const TABLE_SCHEMA: usize = 0;
    pub(super) const TABLE_NAME: usize = 1;
    pub(super) const COLUMN_NAME: usize = 2;
    pub(super) const ORDINAL_POSITION: usize = 3;
    pub(super) const IS_NULLABLE: usize = 4;
    pub(super) const SPANNER_TYPE: usize = 5;
}
mod table_constraints_col {
    // `SELECT TABLE_SCHEMA, TABLE_NAME, CONSTRAINT_NAME, CONSTRAINT_TYPE`
    pub(super) const TABLE_SCHEMA: usize = 0;
    pub(super) const TABLE_NAME: usize = 1;
    pub(super) const CONSTRAINT_NAME: usize = 2;
    pub(super) const CONSTRAINT_TYPE: usize = 3;
}
mod key_column_usage_col {
    // `SELECT CONSTRAINT_SCHEMA, CONSTRAINT_NAME, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME,`
    // ` CAST(ORDINAL_POSITION AS STRING), CAST(POSITION_IN_UNIQUE_CONSTRAINT AS STRING)`
    pub(super) const CONSTRAINT_SCHEMA: usize = 0;
    pub(super) const CONSTRAINT_NAME: usize = 1;
    pub(super) const TABLE_SCHEMA: usize = 2;
    pub(super) const TABLE_NAME: usize = 3;
    pub(super) const COLUMN_NAME: usize = 4;
    pub(super) const ORDINAL_POSITION: usize = 5;
    pub(super) const POSITION_IN_UNIQUE_CONSTRAINT: usize = 6;
}
mod referential_constraints_col {
    // `SELECT CONSTRAINT_SCHEMA, CONSTRAINT_NAME, UNIQUE_CONSTRAINT_SCHEMA, UNIQUE_CONSTRAINT_NAME`
    pub(super) const CONSTRAINT_SCHEMA: usize = 0;
    pub(super) const CONSTRAINT_NAME: usize = 1;
    pub(super) const UNIQUE_CONSTRAINT_SCHEMA: usize = 2;
    pub(super) const UNIQUE_CONSTRAINT_NAME: usize = 3;
}

/// `get_objects` filters: ADBC `LIKE` patterns for names and an exact-match list of table types.
/// `None` means no filter; an empty `table_type` list matches nothing.
pub(crate) struct ObjectFilters<'a> {
    pub db_schema: Option<&'a str>,
    pub table_name: Option<&'a str>,
    pub table_type: Option<&'a [&'a str]>,
    pub column_name: Option<&'a str>,
}

/// Batches fetched by [`collect_objects`]; `None` when the depth skips the query.
struct ObjectBatches {
    schemata: RecordBatch,
    tables: Option<RecordBatch>,
    columns: Option<RecordBatch>,
    constraints: Option<RecordBatch>,
    key_columns: Option<RecordBatch>,
    referential: Option<RecordBatch>,
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
    let ObjectFilters {
        db_schema,
        table_name,
        column_name,
        ..
    } = *filters;
    // `build` ignores the schemas at catalog depth. Deeper levels fetch only what they populate.
    if depth == ObjectDepth::Catalogs {
        return Ok(Vec::new());
    }
    let populate_tables = matches!(
        depth,
        ObjectDepth::All | ObjectDepth::Tables | ObjectDepth::Columns
    );
    let populate_columns = matches!(depth, ObjectDepth::All | ObjectDepth::Columns);
    let client = client.clone();
    let timeout = config.timeouts.query_timeout();

    // Pattern filters are pushed down only to cut transferred rows. `assemble_objects` applies
    // them authoritatively, so differences between GoogleSQL `LIKE` and ADBC patterns cannot
    // change the result.
    let schemata_stmt = filtered_query(
        config,
        "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA",
        None,
        &[("SCHEMA_NAME", db_schema)],
        None,
    );
    let tables_stmt = populate_tables.then(|| {
        filtered_query(
            config,
            "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES",
            None,
            &[("TABLE_SCHEMA", db_schema), ("TABLE_NAME", table_name)],
            None,
        )
    });
    let columns_stmt = populate_columns.then(|| {
        filtered_query(
            config,
            "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, \
             IS_NULLABLE, SPANNER_TYPE \
             FROM INFORMATION_SCHEMA.COLUMNS",
            Some(HIDE_HIDDEN_COLUMNS),
            &[
                ("TABLE_SCHEMA", db_schema),
                ("TABLE_NAME", table_name),
                ("COLUMN_NAME", column_name),
            ],
            Some("TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION"),
        )
    });
    let constraints_stmt = populate_columns.then(|| {
        filtered_query(
            config,
            "SELECT TABLE_SCHEMA, TABLE_NAME, CONSTRAINT_NAME, CONSTRAINT_TYPE \
             FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc",
            Some(HIDE_HIDDEN_COLUMN_CONSTRAINTS),
            &[("TABLE_SCHEMA", db_schema), ("TABLE_NAME", table_name)],
            None,
        )
    });
    // Unfiltered: a foreign key on a kept table may reference a constraint on a table or schema
    // that the filters exclude.
    let key_columns_stmt = populate_columns.then(|| {
        metadata_sql_builder(
            config,
            "SELECT CONSTRAINT_SCHEMA, CONSTRAINT_NAME, TABLE_SCHEMA, TABLE_NAME, \
             COLUMN_NAME, CAST(ORDINAL_POSITION AS STRING), \
             CAST(POSITION_IN_UNIQUE_CONSTRAINT AS STRING) \
             FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE \
             ORDER BY CONSTRAINT_SCHEMA, CONSTRAINT_NAME, ORDINAL_POSITION",
        )
        .build()
    });
    let referential_stmt = populate_columns.then(|| {
        metadata_sql_builder(
            config,
            "SELECT CONSTRAINT_SCHEMA, CONSTRAINT_NAME, UNIQUE_CONSTRAINT_SCHEMA, \
             UNIQUE_CONSTRAINT_NAME \
             FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS",
        )
        .build()
    });

    let bound = config.read_staleness.multi_use_timestamp_bound()?;
    let batches = block_on_cancellable(
        runtime,
        cancel,
        with_timeout(timeout, crate::OPTION_RPC_TIMEOUT_QUERY, async move {
            // The queries run concurrently on one snapshot. The client's inline begin makes
            // concurrent first statements wait for the transaction id.
            let txn = crate::staleness::multi_use(&client, bound).await?;
            let (schemata, tables, columns, constraints, key_columns, referential) = try_join!(
                query_txn(&txn, schemata_stmt),
                query_txn_opt(&txn, tables_stmt),
                query_txn_opt(&txn, columns_stmt),
                query_txn_opt(&txn, constraints_stmt),
                query_txn_opt(&txn, key_columns_stmt),
                query_txn_opt(&txn, referential_stmt),
            )?;
            Ok(ObjectBatches {
                schemata,
                tables,
                columns,
                constraints,
                key_columns,
                referential,
            })
        }),
    )?;

    assemble_objects(&batches, filters)
}

/// Assemble the hierarchy from the fetched batches and apply the filters. Kept apart from the
/// I/O in [`collect_objects`] so it can be tested offline.
fn assemble_objects(batches: &ObjectBatches, filters: &ObjectFilters<'_>) -> Result<Vec<DbSchema>> {
    let schema_batch = &batches.schemata;
    let schema_names = str_col(schema_batch, schemata_col::SCHEMA_NAME)?;

    // Group each batch once, so assembly does lookups instead of rescanning per table. Groups
    // keep the queries' ORDER BY order.
    let tables_by_schema = group_opt(batches.tables.as_ref(), group_tables)?;
    let columns_by_table = group_opt(batches.columns.as_ref(), group_columns)?;
    let constraints_by_table = group_opt(batches.constraints.as_ref(), group_constraints)?;
    let key_columns_by_constraint = group_opt(batches.key_columns.as_ref(), group_key_columns)?;
    let referential_by_constraint = group_opt(batches.referential.as_ref(), group_referential)?;

    let db_schema_matcher = filters.db_schema.map(LikeMatcher::new);
    let table_name_matcher = filters.table_name.map(LikeMatcher::new);
    let column_name_matcher = filters.column_name.map(LikeMatcher::new);

    let mut result = Vec::new();
    for i in 0..schema_batch.num_rows() {
        let schema_name = schema_names.value(i);
        if db_schema_matcher
            .as_ref()
            .is_some_and(|m| !m.matches(schema_name))
        {
            continue;
        }
        let mut tables = Vec::new();
        for table in tables_by_schema.get(schema_name).into_iter().flatten() {
            let name = table.name;
            if table_name_matcher
                .as_ref()
                .is_some_and(|m| !m.matches(name))
            {
                continue;
            }
            let ttype = table.table_type;
            if filters
                .table_type
                .is_some_and(|types| !types.contains(&ttype))
            {
                continue;
            }
            let columns = collect_columns(
                &columns_by_table,
                schema_name,
                name,
                column_name_matcher.as_ref(),
            );
            let constraints = collect_constraints(
                &constraints_by_table,
                &key_columns_by_constraint,
                &referential_by_constraint,
                schema_name,
                name,
            );
            tables.push(Table {
                name: name.to_string(),
                table_type: ttype.to_string(),
                columns,
                constraints,
            });
        }
        result.push(DbSchema {
            name: schema_name.to_string(),
            tables,
        });
    }
    Ok(result)
}

/// Keeps only the columns `SELECT *` returns, so `get_objects` agrees with `get_table_schema`.
/// Spanner hides the implicit `rowid` key that bulk ingest creates for tables without a primary
/// key (see [`bind::create_table_sql`](crate::bind::create_table_sql)).
///
/// `IS_HIDDEN` is documented as the `STRING` `"TRUE"`/`"FALSE"`, but the emulator types it as a
/// `BOOL`; the `CAST` accepts both.
const HIDE_HIDDEN_COLUMNS: &str = "NOT CAST(IS_HIDDEN AS BOOL)";

/// Drops constraints on hidden columns, so no constraint names a column `get_objects` omits.
///
/// A table without a primary key gets a `PK_<table>` primary key and a
/// `CK_IS_NOT_NULL_<table>_rowid` check on its `rowid`. `CONSTRAINT_COLUMN_USAGE` covers both;
/// `KEY_COLUMN_USAGE` would miss the check. A foreign key's usage rows name its parent columns,
/// which are never hidden.
const HIDE_HIDDEN_COLUMN_CONSTRAINTS: &str = "NOT EXISTS ( \
     SELECT 1 FROM INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS u \
     JOIN INFORMATION_SCHEMA.COLUMNS AS c \
       ON c.TABLE_SCHEMA = u.TABLE_SCHEMA AND c.TABLE_NAME = u.TABLE_NAME \
      AND c.COLUMN_NAME = u.COLUMN_NAME \
     WHERE u.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA \
       AND u.CONSTRAINT_NAME = tc.CONSTRAINT_NAME \
       AND CAST(c.IS_HIDDEN AS BOOL))";

/// Escape an ADBC `LIKE` pattern for GoogleSQL.
///
/// ADBC patterns have no escape character, so `\` is a literal, but GoogleSQL treats it as an
/// escape. Doubling each backslash restores the ADBC meaning.
fn sql_like_pattern(pattern: &str) -> String {
    pattern.replace('\\', "\\\\")
}

/// SQL text and parameters for an `INFORMATION_SCHEMA` query: the driver-written
/// `base_predicate` plus `column LIKE @pN` for each present filter.
fn filtered_sql(
    base: &str,
    base_predicate: Option<&str>,
    filters: &[(&str, Option<&str>)],
    order_by: Option<&str>,
) -> (String, Vec<(String, String)>) {
    let mut sql = base.to_string();
    let mut predicates = 0;
    let mut params = Vec::new();
    if let Some(predicate) = base_predicate {
        sql = format!("{sql} WHERE {predicate}");
        predicates += 1;
    }
    for (column, pattern) in filters {
        let Some(pattern) = pattern else { continue };
        let name = format!("p{}", params.len());
        let keyword = if predicates == 0 { "WHERE" } else { "AND" };
        sql = format!("{sql} {keyword} {column} LIKE @{name}");
        predicates += 1;
        params.push((name, sql_like_pattern(pattern)));
    }
    if let Some(order_by) = order_by {
        sql = format!("{sql} ORDER BY {order_by}");
    }
    (sql, params)
}

/// Build an `INFORMATION_SCHEMA` statement with pattern filters as bound `LIKE` parameters.
/// Patterns are user data: formatting them into SQL caused a SQL injection in the Snowflake ADBC
/// driver (apache/arrow-adbc#1338).
fn filtered_query(
    config: &SharedConfig,
    base: &str,
    base_predicate: Option<&str>,
    filters: &[(&str, Option<&str>)],
    order_by: Option<&str>,
) -> SpannerSql {
    let (sql, params) = filtered_sql(base, base_predicate, filters, order_by);
    let mut builder = metadata_sql_builder(config, sql);
    for (name, value) in &params {
        builder = builder.add_param(name.as_str(), value);
    }
    builder.build()
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

/// One `INFORMATION_SCHEMA.TABLES` row.
struct TableRow<'a> {
    name: &'a str,
    table_type: &'a str,
}

/// Foreign key (schema, name) to the referenced constraint's (schema, name).
type ReferentialMap<'a> = HashMap<(&'a str, &'a str), (&'a str, &'a str)>;

/// One `INFORMATION_SCHEMA.COLUMNS` row.
struct ColumnRow<'a> {
    name: &'a str,
    ordinal: i32,
    nullable: bool,
    type_name: &'a str,
}

/// One `INFORMATION_SCHEMA.TABLE_CONSTRAINTS` row.
struct ConstraintRow<'a> {
    name: &'a str,
    constraint_type: &'a str,
}

/// One `INFORMATION_SCHEMA.KEY_COLUMN_USAGE` row. `position` is `POSITION_IN_UNIQUE_CONSTRAINT`,
/// set only for foreign-key columns.
struct KeyColumnRow<'a> {
    column: &'a str,
    table_schema: &'a str,
    table: &'a str,
    ordinal: &'a str,
    position: Option<&'a str>,
}

/// Group a batch, or return an empty map if the depth skipped its query.
fn group_opt<'a, T: Default>(
    batch: Option<&'a RecordBatch>,
    group: impl FnOnce(&'a RecordBatch) -> Result<T>,
) -> Result<T> {
    Ok(batch.map(group).transpose()?.unwrap_or_default())
}

/// Group `TABLES` rows by schema.
fn group_tables(batch: &RecordBatch) -> Result<HashMap<&str, Vec<TableRow<'_>>>> {
    let (ts, tn, tt) = (
        str_col(batch, tables_col::TABLE_SCHEMA)?,
        str_col(batch, tables_col::TABLE_NAME)?,
        str_col(batch, tables_col::TABLE_TYPE)?,
    );
    let mut map: HashMap<&str, Vec<TableRow>> = HashMap::new();
    for r in 0..batch.num_rows() {
        map.entry(ts.value(r)).or_default().push(TableRow {
            name: tn.value(r),
            table_type: tt.value(r),
        });
    }
    Ok(map)
}

/// Group `COLUMNS` rows by (schema, table).
fn group_columns(batch: &RecordBatch) -> Result<HashMap<(&str, &str), Vec<ColumnRow<'_>>>> {
    let (ts, tn, cn, nul, typ) = (
        str_col(batch, columns_col::TABLE_SCHEMA)?,
        str_col(batch, columns_col::TABLE_NAME)?,
        str_col(batch, columns_col::COLUMN_NAME)?,
        str_col(batch, columns_col::IS_NULLABLE)?,
        str_col(batch, columns_col::SPANNER_TYPE)?,
    );
    let ordinal = batch
        .column(columns_col::ORDINAL_POSITION)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| err("ORDINAL_POSITION is not an integer", Status::Internal))?;
    let mut map: HashMap<(&str, &str), Vec<ColumnRow>> = HashMap::new();
    for r in 0..batch.num_rows() {
        map.entry((ts.value(r), tn.value(r)))
            .or_default()
            .push(ColumnRow {
                name: cn.value(r),
                ordinal: ordinal.value(r) as i32,
                nullable: nul.value(r).eq_ignore_ascii_case("YES"),
                type_name: typ.value(r),
            });
    }
    Ok(map)
}

/// Group `TABLE_CONSTRAINTS` rows by (schema, table).
fn group_constraints(batch: &RecordBatch) -> Result<HashMap<(&str, &str), Vec<ConstraintRow<'_>>>> {
    let (ts, tn, cn, ct) = (
        str_col(batch, table_constraints_col::TABLE_SCHEMA)?,
        str_col(batch, table_constraints_col::TABLE_NAME)?,
        str_col(batch, table_constraints_col::CONSTRAINT_NAME)?,
        str_col(batch, table_constraints_col::CONSTRAINT_TYPE)?,
    );
    let mut map: HashMap<(&str, &str), Vec<ConstraintRow>> = HashMap::new();
    for r in 0..batch.num_rows() {
        map.entry((ts.value(r), tn.value(r)))
            .or_default()
            .push(ConstraintRow {
                name: cn.value(r),
                constraint_type: ct.value(r),
            });
    }
    Ok(map)
}

/// Group `KEY_COLUMN_USAGE` rows by constraint (schema, name).
fn group_key_columns(batch: &RecordBatch) -> Result<HashMap<(&str, &str), Vec<KeyColumnRow<'_>>>> {
    let (kcs, kcn, kts, ktn, kcol, kord, kpos) = (
        str_col(batch, key_column_usage_col::CONSTRAINT_SCHEMA)?,
        str_col(batch, key_column_usage_col::CONSTRAINT_NAME)?,
        str_col(batch, key_column_usage_col::TABLE_SCHEMA)?,
        str_col(batch, key_column_usage_col::TABLE_NAME)?,
        str_col(batch, key_column_usage_col::COLUMN_NAME)?,
        str_col(batch, key_column_usage_col::ORDINAL_POSITION)?,
        str_col(batch, key_column_usage_col::POSITION_IN_UNIQUE_CONSTRAINT)?,
    );
    let mut map: HashMap<(&str, &str), Vec<KeyColumnRow>> = HashMap::new();
    for r in 0..batch.num_rows() {
        map.entry((kcs.value(r), kcn.value(r)))
            .or_default()
            .push(KeyColumnRow {
                column: kcol.value(r),
                table_schema: kts.value(r),
                table: ktn.value(r),
                ordinal: kord.value(r),
                position: kpos.is_valid(r).then(|| kpos.value(r)),
            });
    }
    Ok(map)
}

/// Map each foreign key to its referenced constraint, keeping the first row per key.
fn group_referential(batch: &RecordBatch) -> Result<ReferentialMap<'_>> {
    let (rcs, rcn, rus, run) = (
        str_col(batch, referential_constraints_col::CONSTRAINT_SCHEMA)?,
        str_col(batch, referential_constraints_col::CONSTRAINT_NAME)?,
        str_col(batch, referential_constraints_col::UNIQUE_CONSTRAINT_SCHEMA)?,
        str_col(batch, referential_constraints_col::UNIQUE_CONSTRAINT_NAME)?,
    );
    let mut map: ReferentialMap = HashMap::new();
    for r in 0..batch.num_rows() {
        map.entry((rcs.value(r), rcn.value(r)))
            .or_insert_with(|| (rus.value(r), run.value(r)));
    }
    Ok(map)
}

/// One table's columns that match `filter`.
fn collect_columns<'a>(
    columns_by_table: &HashMap<(&'a str, &'a str), Vec<ColumnRow<'a>>>,
    schema: &'a str,
    table: &'a str,
    filter: Option<&LikeMatcher>,
) -> Vec<Column> {
    let mut columns = Vec::new();
    for column in columns_by_table.get(&(schema, table)).into_iter().flatten() {
        if filter.is_some_and(|m| !m.matches(column.name)) {
            continue;
        }
        columns.push(Column {
            name: column.name.to_string(),
            ordinal: column.ordinal,
            nullable: column.nullable,
            type_name: column.type_name.to_string(),
        });
    }
    columns
}

/// One table's constraints, with key columns in key order.
fn collect_constraints<'a>(
    constraints_by_table: &HashMap<(&'a str, &'a str), Vec<ConstraintRow<'a>>>,
    key_columns_by_constraint: &HashMap<(&'a str, &'a str), Vec<KeyColumnRow<'a>>>,
    referential_by_constraint: &ReferentialMap<'a>,
    schema: &'a str,
    table: &'a str,
) -> Vec<Constraint> {
    let mut out = Vec::new();
    for constraint in constraints_by_table
        .get(&(schema, table))
        .into_iter()
        .flatten()
    {
        let name = constraint.name;
        let columns = key_columns_by_constraint
            .get(&(schema, name))
            .into_iter()
            .flatten()
            .map(|k| k.column.to_string())
            .collect();
        let constraint_type = constraint.constraint_type.to_string();
        let usages = if constraint_type == "FOREIGN KEY" {
            foreign_key_usages(
                key_columns_by_constraint,
                referential_by_constraint,
                schema,
                name,
            )
        } else {
            Vec::new()
        };
        out.push(Constraint {
            name: Some(name.to_string()),
            constraint_type,
            columns,
            usages,
        });
    }
    out
}

/// A foreign key's parent columns, in the order of its own key columns.
///
/// `CONSTRAINT_COLUMN_USAGE` does not preserve that order. Instead, find the referenced constraint
/// in `REFERENTIAL_CONSTRAINTS`, index its key columns by ordinal, and map each FK column through
/// `POSITION_IN_UNIQUE_CONSTRAINT`.
fn foreign_key_usages<'a>(
    key_columns_by_constraint: &HashMap<(&'a str, &'a str), Vec<KeyColumnRow<'a>>>,
    referential_by_constraint: &ReferentialMap<'a>,
    schema: &'a str,
    fk_name: &'a str,
) -> Vec<Usage> {
    let Some(&(unique_schema, unique_name)) = referential_by_constraint.get(&(schema, fk_name))
    else {
        return Vec::new();
    };

    let referenced: HashMap<i64, &KeyColumnRow> = key_columns_by_constraint
        .get(&(unique_schema, unique_name))
        .into_iter()
        .flatten()
        .filter_map(|k| Some((k.ordinal.parse::<i64>().ok()?, k)))
        .collect();

    let mut fk_columns: Vec<(i64, i64)> = key_columns_by_constraint
        .get(&(schema, fk_name))
        .into_iter()
        .flatten()
        .filter_map(|k| Some((k.ordinal.parse().ok()?, k.position?.parse().ok()?)))
        .collect();
    fk_columns.sort_by_key(|&(ordinal, _)| ordinal);

    fk_columns
        .into_iter()
        .filter_map(|(_, position)| {
            referenced.get(&position).map(|k| Usage {
                db_schema: k.table_schema.to_string(),
                table: k.table.to_string(),
                column: k.column.to_string(),
            })
        })
        .collect()
}

/// Build the `get_objects` batch for the connection's only catalog. `catalog` is also every
/// foreign-key usage's `fk_catalog`, since foreign keys cannot cross databases.
pub(crate) fn build(
    depth: ObjectDepth,
    catalog: &str,
    schemas: Vec<DbSchema>,
) -> Result<RecordBatch> {
    let out_schema = GET_OBJECTS_SCHEMA.clone();
    let top_fields = out_schema.fields();

    let populate_schemas = depth != ObjectDepth::Catalogs;
    let populate_tables = matches!(
        depth,
        ObjectDepth::All | ObjectDepth::Tables | ObjectDepth::Columns
    );
    let populate_columns = matches!(depth, ObjectDepth::All | ObjectDepth::Columns);

    let db_schemas_field = field(top_fields, "catalog_db_schemas")?;
    let db_schema_item = list_item(&db_schemas_field)?;
    let db_schema_fields = struct_fields(&db_schema_item)?;

    let catalog_db_schemas: ArrayRef = if !populate_schemas {
        new_null_array(db_schemas_field.data_type(), 1)
    } else {
        let schema_names: ArrayRef = Arc::new(StringArray::from_iter_values(
            schemas.iter().map(|s| s.name.as_str()),
        ));

        let tables_field = field(&db_schema_fields, "db_schema_tables")?;
        let db_schema_tables: ArrayRef = if !populate_tables {
            new_null_array(tables_field.data_type(), schemas.len())
        } else {
            // Tables are flattened across schemas, then re-split by `lengths` below.
            let tables: Vec<&Table> = schemas.iter().flat_map(|s| s.tables.iter()).collect();
            let table_item = list_item(&tables_field)?;
            let table_fields = struct_fields(&table_item)?;

            let table_name: ArrayRef = Arc::new(StringArray::from_iter_values(
                tables.iter().map(|t| t.name.as_str()),
            ));
            let table_type: ArrayRef = Arc::new(StringArray::from_iter_values(
                tables.iter().map(|t| t.table_type.as_str()),
            ));

            let cols_field = field(&table_fields, "table_columns")?;
            let table_columns: ArrayRef = if !populate_columns {
                new_null_array(cols_field.data_type(), tables.len())
            } else {
                let col_item = list_item(&cols_field)?;
                let column_fields = struct_fields(&col_item)?;
                let columns: Vec<&Column> = tables.iter().flat_map(|t| t.columns.iter()).collect();
                let column_struct = build_column_struct(&column_fields, &columns)?;
                let lengths: Vec<usize> = tables.iter().map(|t| t.columns.len()).collect();
                list_of(col_item, &lengths, column_struct)?
            };
            let cons_field = field(&table_fields, "table_constraints")?;
            let table_constraints: ArrayRef = if !populate_columns {
                new_null_array(cons_field.data_type(), tables.len())
            } else {
                let cons_item = list_item(&cons_field)?;
                let constraint_fields = struct_fields(&cons_item)?;
                let constraints: Vec<&Constraint> =
                    tables.iter().flat_map(|t| t.constraints.iter()).collect();
                let constraint_struct =
                    build_constraint_struct(&constraint_fields, catalog, &constraints)?;
                let lengths: Vec<usize> = tables.iter().map(|t| t.constraints.len()).collect();
                list_of(cons_item, &lengths, constraint_struct)?
            };

            let table_struct: ArrayRef = Arc::new(
                StructArray::try_new(
                    table_fields.clone(),
                    vec![table_name, table_type, table_columns, table_constraints],
                    None,
                )
                .map_err(arrow_err)?,
            );
            let lengths: Vec<usize> = schemas.iter().map(|s| s.tables.len()).collect();
            list_of(table_item, &lengths, table_struct)?
        };

        let db_schema_struct: ArrayRef = Arc::new(
            StructArray::try_new(
                db_schema_fields.clone(),
                vec![schema_names, db_schema_tables],
                None,
            )
            .map_err(arrow_err)?,
        );
        list_of(db_schema_item, &[schemas.len()], db_schema_struct)?
    };

    let catalog_name: ArrayRef = Arc::new(StringArray::from(vec![catalog]));
    RecordBatch::try_new(out_schema, vec![catalog_name, catalog_db_schemas]).map_err(arrow_err)
}

/// Build the column struct array; unknown `xdbc_*` fields are null.
fn build_column_struct(column_fields: &Fields, columns: &[&Column]) -> Result<ArrayRef> {
    let n = columns.len();
    let arrays: Vec<ArrayRef> = column_fields
        .iter()
        .map(|f| match f.name().as_str() {
            "column_name" => Arc::new(StringArray::from_iter_values(
                columns.iter().map(|c| c.name.as_str()),
            )) as ArrayRef,
            "ordinal_position" => Arc::new(Int32Array::from_iter_values(
                columns.iter().map(|c| c.ordinal),
            )) as ArrayRef,
            "xdbc_type_name" => Arc::new(StringArray::from_iter_values(
                columns.iter().map(|c| c.type_name.as_str()),
            )) as ArrayRef,
            "xdbc_is_nullable" => Arc::new(StringArray::from_iter_values(
                columns
                    .iter()
                    .map(|c| if c.nullable { "YES" } else { "NO" }),
            )) as ArrayRef,
            _ => new_null_array(f.data_type(), n),
        })
        .collect();
    Ok(Arc::new(
        StructArray::try_new(column_fields.clone(), arrays, None).map_err(arrow_err)?,
    ))
}

/// Build the constraint struct array.
fn build_constraint_struct(
    constraint_fields: &Fields,
    catalog: &str,
    constraints: &[&Constraint],
) -> Result<ArrayRef> {
    let n = constraints.len();

    let names_field = field(constraint_fields, "constraint_column_names")?;
    let name_item = list_item(&names_field)?;
    let column_child: ArrayRef = Arc::new(StringArray::from_iter(
        constraints
            .iter()
            .flat_map(|c| c.columns.iter().map(String::as_str))
            .map(Some),
    ));
    let column_lengths: Vec<usize> = constraints.iter().map(|c| c.columns.len()).collect();
    let constraint_column_names = list_of(name_item, &column_lengths, column_child)?;

    // The ADBC spec and validation suite expect a NULL usage list for constraints other than
    // foreign keys.
    let usage_field = field(constraint_fields, "constraint_column_usage")?;
    let usage_item = list_item(&usage_field)?;
    let usage_fields = struct_fields(&usage_item)?;
    let flat_usages: Vec<&Usage> = constraints.iter().flat_map(|c| c.usages.iter()).collect();
    let usage_struct = build_usage_struct(&usage_fields, catalog, &flat_usages)?;
    let usage_lengths: Vec<usize> = constraints.iter().map(|c| c.usages.len()).collect();
    let usage_valid: Vec<bool> = constraints
        .iter()
        .map(|c| c.constraint_type == "FOREIGN KEY")
        .collect();
    let constraint_column_usage = list_of_nullable(
        usage_item,
        &usage_lengths,
        usage_struct,
        Some(NullBuffer::from(usage_valid)),
    )?;

    let arrays: Vec<ArrayRef> = constraint_fields
        .iter()
        .map(|f| match f.name().as_str() {
            "constraint_name" => Arc::new(StringArray::from_iter(
                constraints.iter().map(|c| c.name.as_deref()),
            )) as ArrayRef,
            "constraint_type" => Arc::new(StringArray::from_iter_values(
                constraints.iter().map(|c| c.constraint_type.as_str()),
            )) as ArrayRef,
            "constraint_column_names" => constraint_column_names.clone(),
            "constraint_column_usage" => constraint_column_usage.clone(),
            _ => new_null_array(f.data_type(), n),
        })
        .collect();
    Ok(Arc::new(
        StructArray::try_new(constraint_fields.clone(), arrays, None).map_err(arrow_err)?,
    ))
}

/// Build the `constraint_column_usage` struct array, one entry per parent column.
fn build_usage_struct(usage_fields: &Fields, catalog: &str, usages: &[&Usage]) -> Result<ArrayRef> {
    let n = usages.len();
    let arrays: Vec<ArrayRef> = usage_fields
        .iter()
        .map(|f| match f.name().as_str() {
            "fk_catalog" => Arc::new(StringArray::from_iter_values(
                usages.iter().map(|_| catalog),
            )) as ArrayRef,
            "fk_db_schema" => Arc::new(StringArray::from_iter_values(
                usages.iter().map(|u| u.db_schema.as_str()),
            )) as ArrayRef,
            "fk_table" => Arc::new(StringArray::from_iter_values(
                usages.iter().map(|u| u.table.as_str()),
            )) as ArrayRef,
            "fk_column_name" => Arc::new(StringArray::from_iter_values(
                usages.iter().map(|u| u.column.as_str()),
            )) as ArrayRef,
            _ => new_null_array(f.data_type(), n),
        })
        .collect();
    Ok(Arc::new(
        StructArray::try_new(usage_fields.clone(), arrays, None).map_err(arrow_err)?,
    ))
}

#[cfg(test)]
mod tests;

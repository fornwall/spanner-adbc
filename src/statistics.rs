//! Exact row, null and distinct counts for
//! [`Connection::get_statistics`](adbc_core::Connection::get_statistics), one aggregate scan per
//! table. Min/max values are omitted because the statistic union cannot represent every Spanner
//! type.

use std::collections::HashMap;
use std::sync::Arc;

use adbc_core::error::{Error, Result, Status};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Int16Array, Int64Array, RecordBatch, StringArray, StructArray,
    new_null_array,
};
use arrow_schema::{DataType, Fields, SchemaRef};
use futures_util::stream::{self, StreamExt, TryStreamExt};
use google_cloud_spanner::client::DatabaseClient;

use crate::error::err;
use crate::metadata::{LikeMatcher, metadata_sql_builder, query_txn, str_col};
use crate::nested::{arrow_err, dense_union, field, list_item, list_of, struct_fields};
use crate::options::SharedConfig;
use crate::runtime::{CancelSignal, SharedRuntime, block_on_cancellable};
use crate::sql::{qualified_table, quote_ident};
use crate::timeout::with_timeout;

/// The `int64` branch of the `statistic_value` union (see `STATISTIC_VALUE_SCHEMA`).
const INT64_BRANCH: i8 = 0;

/// Bound concurrent aggregate scans to limit load on Spanner.
const STATISTICS_SCAN_CONCURRENCY: usize = 8;

/// Column positions in the discovery queries; keep these aligned with their SELECT lists.
mod tables_col {
    // `SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES`
    pub(super) const TABLE_SCHEMA: usize = 0;
    pub(super) const TABLE_NAME: usize = 1;
}
mod columns_col {
    // `SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, SPANNER_TYPE FROM INFORMATION_SCHEMA.COLUMNS`
    pub(super) const TABLE_SCHEMA: usize = 0;
    pub(super) const TABLE_NAME: usize = 1;
    pub(super) const COLUMN_NAME: usize = 2;
    pub(super) const SPANNER_TYPE: usize = 3;
}

/// One computed statistic for a table (or one of its columns).
pub(crate) struct Statistic {
    pub table: String,
    /// `None` for table-level statistics such as `ROW_COUNT`.
    pub column: Option<String>,
    /// `ADBC_STATISTIC_*_KEY`.
    pub key: i16,
    pub value: i64,
}

/// The statistics of all tables in one db schema.
pub(crate) struct SchemaStatistics {
    pub db_schema: String,
    pub statistics: Vec<Statistic>,
}

/// Compute exact statistics for the base tables matching the `LIKE` filters: `ROW_COUNT` per
/// table, and per column `NULL_COUNT` plus `DISTINCT_COUNT` for groupable types.
pub(crate) fn collect_statistics(
    runtime: &SharedRuntime,
    client: &DatabaseClient,
    cancel: &CancelSignal,
    config: &SharedConfig,
    db_schema: Option<&str>,
    table_name: Option<&str>,
) -> Result<Vec<SchemaStatistics>> {
    let bound = config.read_staleness.multi_use_timestamp_bound()?;
    let client = client.clone();
    let db_schema_matcher = db_schema.map(LikeMatcher::new);
    let table_name_matcher = table_name.map(LikeMatcher::new);
    // One cancel signal and query deadline cover discovery and every scan, as for `get_objects`.
    let (prepared, batches) = block_on_cancellable(
        runtime,
        cancel,
        with_timeout(
            config.timeouts.query_timeout(),
            crate::OPTION_RPC_TIMEOUT_QUERY,
            async move {
                // Discovery and aggregate scans share one snapshot, including for stale reads.
                let txn = crate::staleness::multi_use(&client, bound).await?;
                let tables = query_txn(
                    &txn,
                    metadata_sql_builder(
                        config,
                        "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES \
                         WHERE TABLE_TYPE = 'BASE TABLE'",
                    ),
                )
                .await?;
                // Match get_objects: omit hidden columns, including implicit rowid keys.
                let columns = query_txn(
                    &txn,
                    metadata_sql_builder(
                        config,
                        "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, SPANNER_TYPE \
                         FROM INFORMATION_SCHEMA.COLUMNS \
                         WHERE NOT CAST(IS_HIDDEN AS BOOL) \
                         ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION",
                    ),
                )
                .await?;
                let prepared = prepare_tables(
                    &tables,
                    &columns,
                    db_schema_matcher.as_ref(),
                    table_name_matcher.as_ref(),
                )?;
                // `buffered` keeps input order.
                let batches: Vec<RecordBatch> = stream::iter(
                    prepared
                        .iter()
                        .map(|p| query_txn(&txn, metadata_sql_builder(config, p.sql.as_str()))),
                )
                .buffered(STATISTICS_SCAN_CONCURRENCY)
                .try_collect()
                .await?;
                Ok::<_, Error>((prepared, batches))
            },
        ),
    )?;

    // Group by schema in first-seen order without repeatedly scanning the output.
    let mut schemas: Vec<SchemaStatistics> = Vec::new();
    let mut schema_index: HashMap<String, usize> = HashMap::new();
    for (p, batch) in prepared.into_iter().zip(batches) {
        let stats = parse_table_statistics(&batch, &p.table, p.plan)?;
        match schema_index.get(&p.schema) {
            Some(&idx) => schemas[idx].statistics.extend(stats),
            None => {
                schema_index.insert(p.schema.clone(), schemas.len());
                schemas.push(SchemaStatistics {
                    db_schema: p.schema,
                    statistics: stats,
                });
            }
        }
    }
    Ok(schemas)
}

/// Build the aggregate query of each discovered base table that matches the filters, from the
/// `INFORMATION_SCHEMA.TABLES` and `COLUMNS` discovery batches.
fn prepare_tables(
    table_batch: &RecordBatch,
    column_batch: &RecordBatch,
    db_schema_matcher: Option<&LikeMatcher>,
    table_name_matcher: Option<&LikeMatcher>,
) -> Result<Vec<PreparedTable>> {
    let (ts, tn) = (
        str_col(table_batch, tables_col::TABLE_SCHEMA)?,
        str_col(table_batch, tables_col::TABLE_NAME)?,
    );
    let (cts, ctn, ccn, ctype) = (
        str_col(column_batch, columns_col::TABLE_SCHEMA)?,
        str_col(column_batch, columns_col::TABLE_NAME)?,
        str_col(column_batch, columns_col::COLUMN_NAME)?,
        str_col(column_batch, columns_col::SPANNER_TYPE)?,
    );

    // Group columns once, preserving ordinal order within each table.
    let mut columns_by_table: HashMap<(&str, &str), Vec<(String, bool)>> = HashMap::new();
    for c in 0..column_batch.num_rows() {
        columns_by_table
            .entry((cts.value(c), ctn.value(c)))
            .or_default()
            .push((ccn.value(c).to_string(), is_groupable(ctype.value(c))));
    }

    let mut prepared: Vec<PreparedTable> = Vec::new();
    for r in 0..table_batch.num_rows() {
        let schema = ts.value(r);
        let table = tn.value(r);
        if db_schema_matcher.is_some_and(|m| !m.matches(schema)) {
            continue;
        }
        if table_name_matcher.is_some_and(|m| !m.matches(table)) {
            continue;
        }
        let columns = columns_by_table
            .remove(&(schema, table))
            .unwrap_or_default();
        let (sql, plan) = build_table_query(schema, table, &columns)?;
        prepared.push(PreparedTable {
            schema: schema.to_string(),
            table: table.to_string(),
            sql,
            plan,
        });
    }
    Ok(prepared)
}

/// A per-table aggregate statistics query, prepared but not yet run.
struct PreparedTable {
    schema: String,
    table: String,
    /// The single-scan aggregate `SELECT`.
    sql: String,
    /// Maps each result column after the row count to its (column, statistic key).
    plan: Vec<(String, i16)>,
}

/// Build the single-scan aggregate query for one table: `COUNT(*)` plus per-column
/// `COUNTIF(... IS NULL)` and (for groupable columns) `COUNT(DISTINCT ...)`. Returns the SQL and a
/// `plan` mapping each result column after the row count to its (column name, statistic key).
fn build_table_query(
    schema: &str,
    table: &str,
    columns: &[(String, bool)],
) -> Result<(String, Vec<(String, i16)>)> {
    use adbc_core::constants::{ADBC_STATISTIC_DISTINCT_COUNT_KEY, ADBC_STATISTIC_NULL_COUNT_KEY};

    let mut exprs = vec!["COUNT(*)".to_string()];
    let mut plan: Vec<(String, i16)> = Vec::new();
    for (name, groupable) in columns {
        let quoted = quote_ident(name)?;
        exprs.push(format!("COUNTIF({quoted} IS NULL)"));
        plan.push((name.clone(), ADBC_STATISTIC_NULL_COUNT_KEY));
        if *groupable {
            exprs.push(format!("COUNT(DISTINCT {quoted})"));
            plan.push((name.clone(), ADBC_STATISTIC_DISTINCT_COUNT_KEY));
        }
    }
    let sql = format!(
        "SELECT {} FROM {}",
        exprs.join(", "),
        qualified_table(Some(schema), table)?
    );
    Ok((sql, plan))
}

/// Extract the `ROW_COUNT` and per-column `NULL_COUNT`/`DISTINCT_COUNT` statistics from a table's
/// single-row aggregate result, using the `plan` produced by [`build_table_query`].
fn parse_table_statistics(
    batch: &RecordBatch,
    table: &str,
    plan: Vec<(String, i16)>,
) -> Result<Vec<Statistic>> {
    use adbc_core::constants::ADBC_STATISTIC_ROW_COUNT_KEY;

    if batch.num_rows() != 1 || batch.num_columns() != plan.len() + 1 {
        return Err(err(
            "statistic aggregate has an unexpected number of rows or columns",
            Status::Internal,
        ));
    }
    let value = |index: usize| -> Result<i64> {
        batch
            .column(index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .filter(|a| !a.is_null(0))
            .map(|a| a.value(0))
            .filter(|&count| count >= 0)
            .ok_or_else(|| {
                err(
                    "statistic aggregate is not a non-null, non-negative integer",
                    Status::Internal,
                )
            })
    };
    let mut out = vec![Statistic {
        table: table.to_string(),
        column: None,
        key: ADBC_STATISTIC_ROW_COUNT_KEY,
        value: value(0)?,
    }];
    for (index, (column, key)) in plan.into_iter().enumerate() {
        out.push(Statistic {
            table: table.to_string(),
            column: Some(column),
            key,
            value: value(index + 1)?,
        });
    }
    Ok(out)
}

/// Whether a SPANNER_TYPE supports COUNT(DISTINCT). Unsupported types must be skipped
/// or the entire table scan fails.
fn is_groupable(spanner_type: &str) -> bool {
    let t = spanner_type.trim_start();
    !(t.starts_with("ARRAY")
        || t.starts_with("STRUCT")
        || t.starts_with("PROTO")
        || t == "JSON"
        || t == "TOKENLIST"
        || t == "INTERVAL")
}

/// Build the `get_statistics` batch for `catalog`, the connection's only catalog.
pub(crate) fn build(
    catalog: &str,
    schemas: Vec<SchemaStatistics>,
    out_schema: SchemaRef,
) -> Result<RecordBatch> {
    let top_fields = out_schema.fields();

    let db_schemas_field = field(top_fields, "catalog_db_schemas")?;
    let db_schema_item = list_item(&db_schemas_field)?;
    let db_schema_fields = struct_fields(&db_schema_item)?;

    // Flatten statistics across schemas into one struct array, then split by schema.
    let stats_field = field(&db_schema_fields, "db_schema_statistics")?;
    let stat_item = list_item(&stats_field)?;
    let stat_fields = struct_fields(&stat_item)?;
    let flat: Vec<&Statistic> = schemas.iter().flat_map(|s| s.statistics.iter()).collect();
    let stat_struct = build_statistic_struct(&stat_fields, &flat)?;
    let lengths: Vec<usize> = schemas.iter().map(|s| s.statistics.len()).collect();
    let db_schema_statistics = list_of(stat_item, &lengths, stat_struct)?;

    let schema_names: ArrayRef = Arc::new(StringArray::from_iter_values(
        schemas.iter().map(|s| s.db_schema.as_str()),
    ));
    let db_schema_struct: ArrayRef = Arc::new(
        StructArray::try_new(
            db_schema_fields.clone(),
            vec![schema_names, db_schema_statistics],
            None,
        )
        .map_err(arrow_err)?,
    );
    let catalog_db_schemas = list_of(db_schema_item, &[schemas.len()], db_schema_struct)?;

    let catalog_name: ArrayRef = Arc::new(StringArray::from(vec![catalog]));
    RecordBatch::try_new(out_schema, vec![catalog_name, catalog_db_schemas]).map_err(arrow_err)
}

/// Build the statistic struct array. All values are exact integers.
fn build_statistic_struct(stat_fields: &Fields, stats: &[&Statistic]) -> Result<ArrayRef> {
    let n = stats.len();

    let value_field = field(stat_fields, "statistic_value")?;
    let union_fields = match value_field.data_type() {
        DataType::Union(fields, _) => fields.clone(),
        _ => {
            return Err(err(
                "unexpected ADBC result schema shape: expected `statistic_value` to be a union",
                Status::Internal,
            ));
        }
    };
    let int64_values: ArrayRef =
        Arc::new(Int64Array::from_iter_values(stats.iter().map(|s| s.value)));
    // Every value is in the `int64` branch; `dense_union` fills the others with empty children.
    let type_ids = vec![INT64_BRANCH; n];
    let offsets: Vec<i32> = (0..n as i32).collect();
    let statistic_value = dense_union(
        &union_fields,
        &[(INT64_BRANCH, int64_values)],
        type_ids,
        offsets,
    )?;

    let arrays: Vec<ArrayRef> = stat_fields
        .iter()
        .map(|f| match f.name().as_str() {
            "table_name" => Arc::new(StringArray::from_iter_values(
                stats.iter().map(|s| s.table.as_str()),
            )) as ArrayRef,
            // NULL for table-level statistics.
            "column_name" => Arc::new(StringArray::from_iter(
                stats.iter().map(|s| s.column.as_deref()),
            )) as ArrayRef,
            "statistic_key" => {
                Arc::new(Int16Array::from_iter_values(stats.iter().map(|s| s.key))) as ArrayRef
            }
            "statistic_value" => statistic_value.clone(),
            "statistic_is_approximate" => Arc::new(BooleanArray::from(vec![false; n])) as ArrayRef,
            _ => new_null_array(f.data_type(), n),
        })
        .collect();

    Ok(Arc::new(
        StructArray::try_new(stat_fields.clone(), arrays, None).map_err(arrow_err)?,
    ))
}

#[cfg(test)]
mod tests;

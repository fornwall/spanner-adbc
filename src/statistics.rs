//! Exact row, null and distinct counts for
//! [`Connection::get_statistics`](adbc_core::Connection::get_statistics), one aggregate scan per
//! table. Min/max values are omitted because the statistic union cannot represent every Spanner
//! type.

use std::sync::Arc;

use adbc_core::error::{Error, Result, Status};
use adbc_core::schemas::GET_STATISTICS_SCHEMA;
use arrow_array::{
    Array, ArrayRef, BooleanArray, Int16Array, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, FieldRef};
use futures_util::stream::{self, StreamExt, TryStreamExt};
use google_cloud_spanner::client::DatabaseClient;

use crate::error::err;
use crate::metadata::{LikeMatcher, filtered_query, metadata_sql_builder, query_txn, str_cols};
use crate::nested::{arrow_err, dense_union, field, shape_err, strings, struct_list};
use crate::options::SharedConfig;
use crate::runtime::{CancelSignal, SharedRuntime, block_on_cancellable};
use crate::sql::{qualified_table, quote_ident};
use crate::timeout::with_timeout;

/// The `int64` branch of the `statistic_value` union (see `STATISTIC_VALUE_SCHEMA`).
const INT64_BRANCH: i8 = 0;

/// Bound concurrent aggregate scans to limit load on Spanner.
const STATISTICS_SCAN_CONCURRENCY: usize = 8;

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

/// One row per visible column of each base table, in ordinal order, or one row with null columns
/// for a table without any. Hidden columns, such as implicit `rowid` keys, are omitted as in
/// `get_objects`.
const DISCOVERY_SQL: &str = "SELECT t.TABLE_SCHEMA, t.TABLE_NAME, c.COLUMN_NAME, c.SPANNER_TYPE \
     FROM INFORMATION_SCHEMA.TABLES AS t \
     LEFT JOIN INFORMATION_SCHEMA.COLUMNS AS c \
       ON c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME \
      AND NOT CAST(c.IS_HIDDEN AS BOOL)";

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
    // As in `get_objects`, the pushed-down filters only cut transferred rows.
    let discovery = filtered_query(
        config,
        DISCOVERY_SQL,
        Some("t.TABLE_TYPE = 'BASE TABLE'"),
        &[("t.TABLE_SCHEMA", db_schema), ("t.TABLE_NAME", table_name)],
        "t.TABLE_SCHEMA, t.TABLE_NAME, c.ORDINAL_POSITION",
    );
    let (schema_matcher, table_matcher) = (
        db_schema.map(LikeMatcher::new),
        table_name.map(LikeMatcher::new),
    );
    let matches = move |schema: &str, table: &str| {
        schema_matcher.as_ref().is_none_or(|m| m.matches(schema))
            && table_matcher.as_ref().is_none_or(|m| m.matches(table))
    };
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
                let prepared = prepare_tables(&query_txn(&txn, discovery).await?, matches)?;
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

    // Discovery orders by schema, so each schema's tables are adjacent.
    let mut schemas: Vec<SchemaStatistics> = Vec::new();
    for (p, batch) in prepared.into_iter().zip(batches) {
        let stats = parse_table_statistics(&batch, &p.table, p.plan)?;
        match schemas.last_mut() {
            Some(last) if last.db_schema == p.schema => last.statistics.extend(stats),
            _ => schemas.push(SchemaStatistics {
                db_schema: p.schema,
                statistics: stats,
            }),
        }
    }
    Ok(schemas)
}

/// Build the aggregate query of each discovered base table that `matches` its schema and name.
fn prepare_tables(
    discovery: &RecordBatch,
    matches: impl Fn(&str, &str) -> bool,
) -> Result<Vec<PreparedTable>> {
    let [schema, table, column, spanner_type] = str_cols(discovery)?;
    let name = |r: usize| (schema.value(r), table.value(r));
    let rows: Vec<usize> = (0..discovery.num_rows()).collect();
    // Each table's rows are adjacent: discovery orders by schema and table.
    rows.chunk_by(|&a, &b| name(a) == name(b))
        .filter(|rows| {
            let (schema, table) = name(rows[0]);
            matches(schema, table)
        })
        .map(|rows| {
            let (schema, table) = name(rows[0]);
            let columns: Vec<(&str, bool)> = rows
                .iter()
                .filter(|&&r| column.is_valid(r))
                .map(|&r| (column.value(r), is_groupable(spanner_type.value(r))))
                .collect();
            let (sql, plan) = build_table_query(schema, table, &columns)?;
            Ok(PreparedTable {
                schema: schema.to_string(),
                table: table.to_string(),
                sql,
                plan,
            })
        })
        .collect()
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
    columns: &[(&str, bool)],
) -> Result<(String, Vec<(String, i16)>)> {
    use adbc_core::constants::{ADBC_STATISTIC_DISTINCT_COUNT_KEY, ADBC_STATISTIC_NULL_COUNT_KEY};

    let mut exprs = vec!["COUNT(*)".to_string()];
    let mut plan: Vec<(String, i16)> = Vec::new();
    for &(name, groupable) in columns {
        let quoted = quote_ident(name)?;
        exprs.push(format!("COUNTIF({quoted} IS NULL)"));
        plan.push((name.to_string(), ADBC_STATISTIC_NULL_COUNT_KEY));
        if groupable {
            exprs.push(format!("COUNT(DISTINCT {quoted})"));
            plan.push((name.to_string(), ADBC_STATISTIC_DISTINCT_COUNT_KEY));
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
pub(crate) fn build(catalog: &str, schemas: Vec<SchemaStatistics>) -> Result<RecordBatch> {
    let out_schema = GET_STATISTICS_SCHEMA.clone();
    let db_schemas_field = field(out_schema.fields(), "catalog_db_schemas")?;
    let db_schemas = struct_list(&db_schemas_field, &[schemas.len()], None, |f| {
        match f.name().as_str() {
            "db_schema_name" => Ok(Some(strings(schemas.iter().map(|s| s.db_schema.as_str())))),
            "db_schema_statistics" => statistics_list(f, &schemas).map(Some),
            _ => Ok(None),
        }
    })?;
    RecordBatch::try_new(out_schema, vec![strings([catalog]), db_schemas]).map_err(arrow_err)
}

/// Each schema's `db_schema_statistics`. All values are exact integers.
fn statistics_list(field: &FieldRef, schemas: &[SchemaStatistics]) -> Result<ArrayRef> {
    let lengths: Vec<usize> = schemas.iter().map(|s| s.statistics.len()).collect();
    let stats: Vec<&Statistic> = schemas.iter().flat_map(|s| &s.statistics).collect();
    struct_list(field, &lengths, None, |f| {
        Ok(Some(match f.name().as_str() {
            "table_name" => strings(stats.iter().map(|s| s.table.as_str())),
            // NULL for table-level statistics.
            "column_name" => Arc::new(StringArray::from_iter(
                stats.iter().map(|s| s.column.as_deref()),
            )),
            "statistic_key" => Arc::new(Int16Array::from_iter_values(stats.iter().map(|s| s.key))),
            "statistic_value" => {
                let DataType::Union(union_fields, _) = f.data_type() else {
                    return Err(shape_err("`statistic_value` to be a union"));
                };
                let values: ArrayRef =
                    Arc::new(Int64Array::from_iter_values(stats.iter().map(|s| s.value)));
                // Every value is in the `int64` branch; `dense_union` leaves the others empty.
                dense_union(
                    union_fields,
                    &[(INT64_BRANCH, values)],
                    vec![INT64_BRANCH; stats.len()],
                    (0..stats.len() as i32).collect(),
                )?
            }
            "statistic_is_approximate" => Arc::new(BooleanArray::from(vec![false; stats.len()])),
            _ => return Ok(None),
        }))
    })
}

#[cfg(test)]
mod tests;

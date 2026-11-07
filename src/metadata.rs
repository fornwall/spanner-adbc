//! Shared metadata helpers: the statement builders for internal reads, table and catalog
//! lookups, and the ADBC `LIKE` matcher.

use adbc_core::error::{Result, Status};
use arrow_array::{RecordBatch, StringArray};
use google_cloud_spanner::client::DatabaseClient;
use google_cloud_spanner::statement::{Statement as SpannerSql, StatementBuilder};
use google_cloud_spanner::transaction::{MultiUseReadOnlyTransaction, TimestampBound};

use crate::conversion::{TimestampPrecision, result_set_to_batch};
use crate::error::{err, from_spanner};
use crate::options::SharedConfig;
use crate::runtime::{CancelSignal, SharedRuntime};

/// The catalog name for a database path: the `<d>` of `projects/<p>/instances/<i>/databases/<d>`,
/// or the whole string otherwise.
pub(crate) fn database_catalog(database: &str) -> &str {
    database.rsplit_once('/').map_or(database, |(_, id)| id)
}

/// Check a lookup's `catalog` argument, which is not a pattern, against the connection's only
/// catalog. Any name other than `reported` is [`Status::NotFound`], like a missing table. That
/// includes `Some("")`, which adbc.h defines as objects without a catalog.
pub(crate) fn check_lookup_catalog(catalog: Option<&str>, reported: &str) -> Result<()> {
    match catalog {
        None => Ok(()),
        Some(name) if name == reported => Ok(()),
        Some(other) => Err(err(
            format!(
                "catalog {other:?} not found: this connection has only the catalog {reported:?}"
            ),
            Status::NotFound,
        )),
    }
}

/// One row per column of a table, or one row with null columns for a table without any, and no
/// rows when it doesn't exist. Spanner resolves schema and table names case-insensitively, but
/// `INFORMATION_SCHEMA` keeps their declared case.
const TABLE_COLUMNS_SQL: &str = "SELECT c.COLUMN_NAME, c.IS_NULLABLE \
     FROM INFORMATION_SCHEMA.TABLES AS t \
     LEFT JOIN INFORMATION_SCHEMA.COLUMNS AS c \
       ON c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME \
     WHERE LOWER(t.TABLE_SCHEMA) = LOWER(@schema) AND LOWER(t.TABLE_NAME) = LOWER(@table)";

/// The [`TABLE_COLUMNS_SQL`] lookup of a table. The default schema is `""` in Spanner.
pub(crate) fn table_columns_query(
    config: &SharedConfig,
    db_schema: &str,
    table_name: &str,
) -> SpannerSql {
    metadata_sql_builder(config, TABLE_COLUMNS_SQL)
        .add_param("schema", db_schema)
        .add_param("table", table_name)
        .build()
}

/// Whether a table exists now, read strongly as writes see it. The default schema is `""` in
/// Spanner.
///
/// Callers probe on error paths to refine an error into the status ADBC requires, such as
/// `NotFound` or `AlreadyExists`. If the probe itself fails (a transport error, a cancel, or no
/// `INFORMATION_SCHEMA` read permission), they return the original error: it comes from the
/// operation the user asked for, and the probe's failure would only hide the cause.
pub(crate) fn table_exists(
    runtime: &SharedRuntime,
    client: &DatabaseClient,
    cancel: &CancelSignal,
    config: &SharedConfig,
    db_schema: &str,
    table_name: &str,
) -> Result<bool> {
    let lookup = table_columns_query(config, db_schema, table_name);
    config.timeouts.block_on_query(runtime, cancel, async {
        let columns = query_single_use(client, None, lookup, TimestampPrecision::default()).await?;
        Ok(columns.num_rows() > 0)
    })
}

/// Run a statement as one single-use read at `bound`, where `None` is a strong read.
pub(crate) async fn query_single_use(
    client: &DatabaseClient,
    bound: Option<TimestampBound>,
    statement: SpannerSql,
    precision: TimestampPrecision,
) -> Result<RecordBatch> {
    let result_set = crate::staleness::single_use(client, bound)
        .execute_query(statement)
        .await
        .map_err(from_spanner)?;
    result_set_to_batch(result_set, precision).await
}

/// Statement builder for driver-internal metadata reads: `INFORMATION_SCHEMA` queries and
/// statistics scans.
///
/// A statistics scan runs `COUNT(*)` over every table, so these reads honour the connection's
/// retry bounds, directed reads (they are read-only) and priority. They stay untagged.
#[must_use]
pub(crate) fn metadata_sql_builder(
    config: &SharedConfig,
    sql: impl Into<String>,
) -> StatementBuilder {
    config.retry.apply_to_statement(
        config.directed_read.apply_to_statement(
            config
                .request
                .apply_priority_to_statement(SpannerSql::builder(sql)),
        ),
    )
}

/// An `INFORMATION_SCHEMA` query: `select`, filtered by the driver-written `predicate` and by
/// `column LIKE @pN` for each present pattern, then sorted by `order_by`.
///
/// Bind user patterns so filter text cannot change the generated SQL.
pub(crate) fn filtered_query(
    config: &SharedConfig,
    select: &str,
    predicate: Option<&str>,
    filters: &[(&str, Option<&str>)],
    order_by: &str,
) -> SpannerSql {
    let mut predicates: Vec<String> = predicate.map(str::to_string).into_iter().collect();
    let mut patterns = Vec::new();
    for (column, pattern) in filters {
        if let Some(pattern) = pattern {
            predicates.push(format!("{column} LIKE @p{}", patterns.len()));
            // ADBC patterns have no escape character, but GoogleSQL treats `\` as one.
            patterns.push(pattern.replace('\\', "\\\\"));
        }
    }
    let mut sql = select.to_string();
    if !predicates.is_empty() {
        sql = format!("{sql} WHERE {}", predicates.join(" AND "));
    }
    let mut builder = metadata_sql_builder(config, format!("{sql} ORDER BY {order_by}"));
    for (i, pattern) in patterns.into_iter().enumerate() {
        builder = builder.add_param(format!("p{i}"), pattern);
    }
    builder.build()
}

/// Run a metadata statement on a shared read-only transaction, so all reads of one `get_objects`
/// or `get_statistics` call see the same snapshot. Results hold no TIMESTAMPs, so the default
/// precision is fine.
pub(crate) async fn query_txn(
    txn: &MultiUseReadOnlyTransaction,
    statement: impl Into<SpannerSql>,
) -> Result<RecordBatch> {
    let result_set = txn
        .execute_query(statement.into())
        .await
        .map_err(from_spanner)?;
    result_set_to_batch(result_set, TimestampPrecision::default()).await
}

/// Column `index` of an `INFORMATION_SCHEMA` batch as a `T`.
pub(crate) fn column<T: 'static>(batch: &RecordBatch, index: usize) -> Result<&T> {
    batch
        .columns()
        .get(index)
        .and_then(|c| c.as_any().downcast_ref())
        .ok_or_else(|| {
            err(
                format!(
                    "INFORMATION_SCHEMA column {index} is missing or not a {}",
                    std::any::type_name::<T>()
                ),
                Status::Internal,
            )
        })
}

/// The first `N` columns of an `INFORMATION_SCHEMA` batch, which must be strings.
pub(crate) fn str_cols<const N: usize>(batch: &RecordBatch) -> Result<[&StringArray; N]> {
    let columns = (0..N)
        .map(|i| column(batch, i))
        .collect::<Result<Vec<_>>>()?;
    Ok(columns.try_into().expect("collected N columns"))
}

/// Match `value` against an ADBC `LIKE` pattern, where `%` matches any run and `_` one character.
/// Case-sensitive.
///
/// Matching backtracks iteratively in O(pattern * value) time, so patterns like `%a%a%a` cannot
/// blow up or overflow the stack.
pub(crate) fn like_match(pattern: &str, value: &str) -> bool {
    let (mut p, mut v) = (pattern.chars(), value.chars());
    // The rest of the pattern after the last `%`, and the value from where that `%` stopped.
    let mut retry = None;
    loop {
        let (mut next_p, mut next_v) = (p.clone(), v.clone());
        match (next_p.next(), next_v.next()) {
            (None, None) => return true,
            // Checked first so `%` stays a wildcard when the value char is also `%`.
            (Some('%'), _) => {
                retry = Some((next_p.clone(), v.clone()));
                p = next_p;
            }
            (Some(c), Some(ch)) if c == '_' || c == ch => (p, v) = (next_p, next_v),
            // Let the last `%` consume one more character and retry.
            _ => match &mut retry {
                Some((after_star, resume)) if !resume.as_str().is_empty() => {
                    resume.next();
                    (p, v) = (after_star.clone(), resume.clone());
                }
                _ => return false,
            },
        }
    }
}

#[cfg(test)]
mod tests;

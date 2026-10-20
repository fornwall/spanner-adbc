//! Shared metadata helpers: the statement builders for internal reads, table and catalog
//! lookups, and the ADBC `LIKE` matcher.

use adbc_core::error::{Error, Result, Status};
use arrow_array::{RecordBatch, StringArray};
use google_cloud_spanner::client::DatabaseClient;
use google_cloud_spanner::statement::{Statement as SpannerSql, StatementBuilder};
use google_cloud_spanner::transaction::{MultiUseReadOnlyTransaction, TimestampBound};

use crate::conversion::{TimestampPrecision, result_set_to_batch};
use crate::error::{err, from_spanner};
use crate::options::SharedConfig;
use crate::runtime::{CancelSignal, SharedRuntime, block_on_cancellable};
use crate::timeout::with_timeout;

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

/// The [`table_exists_at`] probe. Spanner resolves schema and table names case-insensitively, but
/// `INFORMATION_SCHEMA` keeps their declared case.
const TABLE_EXISTS_SQL: &str = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES \
     WHERE LOWER(TABLE_SCHEMA) = LOWER(@schema) AND LOWER(TABLE_NAME) = LOWER(@table)";

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
    table_exists_at(runtime, client, cancel, config, None, db_schema, table_name)
}

/// [`table_exists`] at a read `bound`, so a probe that refines a failed read sees the same
/// snapshot as that read. `None` is a strong read.
pub(crate) fn table_exists_at(
    runtime: &SharedRuntime,
    client: &DatabaseClient,
    cancel: &CancelSignal,
    config: &SharedConfig,
    bound: Option<TimestampBound>,
    db_schema: &str,
    table_name: &str,
) -> Result<bool> {
    let client = client.clone();
    let (schema, table) = (db_schema.to_string(), table_name.to_string());
    block_on_cancellable(
        runtime,
        cancel,
        with_timeout(
            config.timeouts.query_timeout(),
            crate::OPTION_RPC_TIMEOUT_QUERY,
            async move {
                let statement = metadata_sql_builder(config, TABLE_EXISTS_SQL)
                    .add_param("schema", &schema)
                    .add_param("table", &table)
                    .build();
                let transaction = crate::staleness::single_use(&client, bound);
                let result_set = transaction
                    .execute_query(statement)
                    .await
                    .map_err(from_spanner)?;
                let (_schema, batch) =
                    result_set_to_batch(result_set, TimestampPrecision::default()).await?;
                Ok::<bool, Error>(batch.num_rows() > 0)
            },
        ),
    )
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
/// Patterns are user data, so they are bound as parameters: formatting them into SQL caused a SQL
/// injection in the Snowflake ADBC driver (apache/arrow-adbc#1338).
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
    let (_schema, batch) = result_set_to_batch(result_set, TimestampPrecision::default()).await?;
    Ok(batch)
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

/// A compiled ADBC `LIKE` pattern, where `%` matches any run and `_` one character.
/// Case-sensitive.
///
/// Matching backtracks iteratively in O(pattern * value) time, so patterns like `%a%a%a` cannot
/// blow up or overflow the stack.
pub(crate) struct LikeMatcher {
    pattern: Vec<char>,
}

impl LikeMatcher {
    pub(crate) fn new(pattern: &str) -> Self {
        Self {
            pattern: pattern.chars().collect(),
        }
    }

    pub(crate) fn matches(&self, value: &str) -> bool {
        let p = &self.pattern;
        // Walk `value` by byte offset to avoid allocating; each step advances one character.
        let (mut pi, mut vi) = (0usize, 0usize);
        // Pattern index / value byte offset to backtrack to after the most recent `%`.
        let mut star: Option<(usize, usize)> = None;
        while vi < value.len() {
            let ch = value[vi..].chars().next().expect("vi is a char boundary");
            // Check `%` first so it stays a wildcard when the value char is also `%`.
            if pi < p.len() && p[pi] == '%' {
                star = Some((pi, vi));
                pi += 1;
            } else if pi < p.len() && (p[pi] == '_' || p[pi] == ch) {
                pi += 1;
                vi += ch.len_utf8();
            } else if let Some((sp, sv)) = star {
                // Let the last `%` consume one more character and retry.
                let skipped = value[sv..].chars().next().expect("sv is a char boundary");
                pi = sp + 1;
                vi = sv + skipped.len_utf8();
                star = Some((sp, vi));
            } else {
                return false;
            }
        }
        while pi < p.len() && p[pi] == '%' {
            pi += 1;
        }
        pi == p.len()
    }
}

/// Match one value against an ADBC `LIKE` pattern; see [`LikeMatcher`].
pub(crate) fn like_match(pattern: &str, value: &str) -> bool {
    LikeMatcher::new(pattern).matches(value)
}

#[cfg(test)]
mod tests;

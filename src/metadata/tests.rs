//! Offline tests for the metadata helpers.

use std::sync::Arc;

use adbc_core::error::Status;
use adbc_core::options::OptionValue;
use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};

use super::{
    check_lookup_catalog, column, database_catalog, filtered_query, like_match,
    metadata_sql_builder, str_cols,
};
use crate::options::SharedConfig;

#[test]
fn like_matching() {
    assert!(like_match("", ""));
    assert!(like_match("%", ""));
    assert!(like_match("%", "anything"));
    assert!(like_match("Singers", "Singers"));
    assert!(!like_match("Singers", "singers")); // case-sensitive
    assert!(like_match("Sing%", "Singers"));
    assert!(like_match("%ers", "Singers"));
    assert!(like_match("S_ngers", "Singers"));
    assert!(like_match("%a%a%", "banana"));
    assert!(!like_match("%x%", "banana"));
    assert!(!like_match("", "x"));
    // `%` stays a wildcard where the value has a literal `%`.
    assert!(like_match("%", "%foo"));
    assert!(like_match("%", "%^%?"));
    assert!(like_match("a%", "a%b"));
}

#[test]
fn like_matching_multibyte_utf8() {
    // The matcher walks bytes, but `_` and `%` backtracking must step over whole characters.
    assert!(like_match("_", "é")); // 2-byte char is one `_`
    assert!(!like_match("__", "é")); // ... not two
    assert!(like_match("caf_", "café"));
    assert!(like_match("_本_", "日本語")); // 3-byte chars
    assert!(!like_match("____", "日本語"));
    assert!(like_match("_", "🦀")); // 4-byte char
    assert!(like_match("%語", "日本語")); // `%` backtracks over multi-byte chars
    assert!(like_match("%本%", "日本語"));
    assert!(!like_match("%x%", "日本語"));
    assert!(like_match("日%語", "日本語"));
    assert!(like_match("é%é", "été"));
    assert!(!like_match("é_é", "été été")); // literal tail must land on the right char
}

#[test]
fn database_catalog_is_the_database_id() {
    assert_eq!(
        database_catalog("projects/p/instances/i/databases/adbc-test"),
        "adbc-test"
    );
    // Other forms are reported verbatim.
    assert_eq!(database_catalog("adbc-test"), "adbc-test");
    assert_eq!(database_catalog(""), "");
}

#[test]
fn lookup_catalog_accepts_only_the_connections_own_catalog() {
    assert!(check_lookup_catalog(None, "adbc-test").is_ok());
    assert!(check_lookup_catalog(Some("adbc-test"), "adbc-test").is_ok());
    // adbc.h defines `""` as objects with no catalog; every Spanner object has one.
    for absent in ["main", ""] {
        let err = check_lookup_catalog(Some(absent), "adbc-test").unwrap_err();
        assert_eq!(err.status, Status::NotFound);
        assert!(err.message.contains("adbc-test"), "{}", err.message);
    }
}

/// A batch that doesn't match its query's SELECT list is an `Internal` error, not a panic.
#[test]
fn column_lookups_reject_a_missing_or_mistyped_column() {
    let schema = Arc::new(Schema::new(vec![Field::new("c", DataType::Utf8, true)]));
    let col: ArrayRef = Arc::new(StringArray::from(vec![Some("x")]));
    let batch = RecordBatch::try_new(schema, vec![col]).unwrap();
    let [c] = str_cols(&batch).unwrap();
    assert_eq!(c.value(0), "x");

    assert_eq!(str_cols::<2>(&batch).unwrap_err().status, Status::Internal);
    let error = column::<Int64Array>(&batch, 0).unwrap_err();
    assert_eq!(error.status, Status::Internal);
    assert!(error.message.contains("column 0"), "{}", error.message);
}

#[test]
fn filtered_query_binds_each_present_pattern() {
    let config = SharedConfig::default();
    let unfiltered = filtered_query(&config, "SELECT A FROM T", None, &[("A", None)], "A");
    assert_eq!(unfiltered.sql(), "SELECT A FROM T ORDER BY A");

    // Pattern text goes only into the parameters, with `\` escaped for GoogleSQL.
    let filtered = filtered_query(
        &config,
        "SELECT A FROM T",
        Some("X"),
        &[("A", None), ("B", Some("Sing%")), ("C", Some(r"evil'\--%"))],
        "A, B",
    );
    assert_eq!(
        filtered.sql(),
        "SELECT A FROM T WHERE X AND B LIKE @p0 AND C LIKE @p1 ORDER BY A, B"
    );
    let built = format!("{filtered:?}");
    assert!(built.contains(r#"StringValue("Sing%")"#), "{built}");
    assert!(built.contains(r#"StringValue("evil'\\\\--%")"#), "{built}");
}

/// Sets every option `metadata_sql_builder` forwards, plus tags, which it must not forward.
fn configured() -> SharedConfig {
    let mut config = SharedConfig::default();
    config
        .request
        .set_priority(OptionValue::String("low".into()))
        .unwrap();
    config
        .request
        .set_request_tag(OptionValue::String("user-request-tag".into()))
        .unwrap();
    config
        .request
        .set_transaction_tag(OptionValue::String("user-txn-tag".into()))
        .unwrap();
    config
        .directed_read
        .set(OptionValue::String("include:eu-west1:read_only".into()))
        .unwrap();
    config.retry.set_max_attempts(OptionValue::Int(3)).unwrap();
    config
}

/// The client's `Statement` fields are private, so these tests inspect its `Debug` output.
#[test]
fn metadata_statements_carry_priority_replicas_and_retry_bounds() {
    let built = format!(
        "{:?}",
        metadata_sql_builder(&configured(), "SELECT 1").build()
    );
    assert!(built.contains("priority: Low"), "{built}");
    assert!(built.contains("location: \"eu-west1\""), "{built}");
    assert!(built.contains("maximum_attempts: 3"), "{built}");
}

/// Tags attribute the user's statements in Spanner's introspection tables, so internal reads
/// stay untagged.
#[test]
fn metadata_statements_are_never_tagged() {
    let built = format!(
        "{:?}",
        metadata_sql_builder(&configured(), "SELECT 1").build()
    );
    assert!(!built.contains("user-request-tag"), "{built}");
    assert!(!built.contains("user-txn-tag"), "{built}");
    assert!(built.contains("request_tag: \"\""), "{built}");
    assert!(built.contains("transaction_tag: \"\""), "{built}");
}

#[test]
fn unconfigured_metadata_statements_stay_bare() {
    let built = format!(
        "{:?}",
        metadata_sql_builder(&SharedConfig::default(), "SELECT 1").build()
    );
    assert!(built.contains("request_options: None"), "{built}");
    assert!(built.contains("directed_read_options: None"), "{built}");
    assert!(built.contains("retry_policy: None"), "{built}");
}

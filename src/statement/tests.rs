use super::*;
use adbc_core::error::Status;

#[test]
fn ingest_temporary_accepts_false_and_rejects_true() {
    let check_ingest_temporary = |value| {
        check_unsupported_true(
            value,
            "option adbc.ingest.temporary",
            "setting adbc.ingest.temporary to true: Spanner has no temporary tables; leave it \
             unset or false",
        )
    };
    check_ingest_temporary(OptionValue::String("false".into())).unwrap();
    let error = check_ingest_temporary(OptionValue::String("true".into())).unwrap_err();
    assert_eq!(error.status, Status::NotImplemented);
    // Invalid values fail boolean parsing before the feature check.
    for bad in ["maybe", "FALSE", "0", "no", "TRUE", "1", "yes"] {
        let error = check_ingest_temporary(OptionValue::String(bad.into())).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "{bad}");
    }
    for bad in [OptionValue::Int(0), OptionValue::Int(1)] {
        let error = check_ingest_temporary(bad).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
    }
}

#[test]
fn dml_partitioned_option_coerces_and_unsets_on_empty() {
    assert!(dml_partitioned_option(OptionValue::String("true".into())).unwrap());
    assert!(!dml_partitioned_option(OptionValue::String("false".into())).unwrap());
    for empty in ["", "   "] {
        assert!(!dml_partitioned_option(OptionValue::String(empty.into())).unwrap());
    }
    for bad in ["maybe", "TRUE", "1", "yes", "FALSE", "0", "no"] {
        let error = dml_partitioned_option(OptionValue::String(bad.into())).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "{bad}");
    }
    let error = dml_partitioned_option(OptionValue::Int(1)).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
}

#[test]
fn partitioned_dml_accepts_a_single_statement() {
    for sql in [
        "UPDATE Singers SET Active = true WHERE Active = false",
        "DELETE FROM Singers WHERE SingerId > 100",
        "DELETE FROM Singers WHERE SingerId > 100;",
    ] {
        assert!(check_partitioned_dml(sql, true, 0).is_ok(), "{sql}");
    }
    assert!(check_partitioned_dml("DELETE FROM Singers WHERE SingerId = @id", true, 1).is_ok());
}

#[test]
fn partitioned_dml_rejects_what_it_cannot_express() {
    let sql = "UPDATE Singers SET Active = true WHERE Active = false";
    let error = check_partitioned_dml(sql, false, 0).unwrap_err();
    assert_eq!(error.status, Status::InvalidState);
    assert!(
        error.message.contains("spanner.dml.partitioned")
            && error.message.contains("manual transaction"),
        "{}",
        error.message
    );
    let error = check_partitioned_dml(&format!("{sql}; {sql}"), true, 0).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(
        error.message.contains("spanner.dml.partitioned")
            && error.message.contains("batch of 2 statements"),
        "{}",
        error.message
    );
    let error = check_partitioned_dml(&format!("{sql} THEN RETURN SingerId"), true, 0).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(
        error.message.contains("spanner.dml.partitioned") && error.message.contains("THEN RETURN"),
        "{}",
        error.message
    );
    let error = check_partitioned_dml(sql, true, 3).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(
        error.message.contains("spanner.dml.partitioned")
            && error.message.contains("3 parameter rows"),
        "{}",
        error.message
    );
}

#[test]
fn execute_schema_guard_rejects_ddl_and_dml() {
    let check_schema =
        |sql: &str| check_query_only(sql, "execute_schema", "planned in a read-only schema probe");
    for sql in [
        "SELECT 1",
        "WITH cte AS (SELECT 1 AS a) SELECT a FROM cte",
        "(SELECT 1)",
        "@{USE_ADDITIONAL_PARALLELISM=true} SELECT 1",
        "GRAPH g MATCH (n) RETURN n.id",
    ] {
        check_schema(sql).unwrap_or_else(|e| panic!("query should pass: {sql}: {e}"));
    }
    let error = check_schema("CREATE TABLE t (id INT64) PRIMARY KEY (id)").unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    for sql in [
        "INSERT INTO t (id) VALUES (1)",
        "update t set c = 1 where true",
        "Delete From t Where true",
        "/* comment */ INSERT INTO t (id) VALUES (1)",
        "@{PDML_MAX_PARALLELISM=1} DELETE FROM t WHERE true",
        "INSERT INTO t (id) VALUES (1) THEN RETURN id",
    ] {
        let error = check_schema(sql).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "{sql}");
        assert!(
            error.message.contains("only supports queries"),
            "unexpected message for {sql}: {}",
            error.message
        );
    }
}

#[test]
fn execute_partitions_guard_rejects_ddl_and_dml() {
    let check_partitions = |sql: &str| {
        check_query_only(
            sql,
            "execute_partitions",
            "partitioned in a batch read-only transaction",
        )
    };
    for sql in [
        "SELECT 1",
        "WITH cte AS (SELECT 1 AS a) SELECT a FROM cte",
        "(SELECT 1)",
        "@{USE_ADDITIONAL_PARALLELISM=true} SELECT 1",
        "GRAPH g MATCH (n) RETURN n.id",
    ] {
        check_partitions(sql).unwrap_or_else(|e| panic!("query should pass: {sql}: {e}"));
    }
    let error = check_partitions("CREATE TABLE t (id INT64) PRIMARY KEY (id)").unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(
        error.message.contains("execute_partitions"),
        "unexpected message: {}",
        error.message
    );
    for sql in [
        "INSERT INTO t (id) VALUES (1)",
        "update t set c = 1 where true",
        "Delete From t Where true",
        "/* comment */ INSERT INTO t (id) VALUES (1)",
        "@{PDML_MAX_PARALLELISM=1} DELETE FROM t WHERE true",
        "INSERT INTO t (id) VALUES (1) THEN RETURN id",
    ] {
        let error = check_partitions(sql).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "{sql}");
        assert!(
            error
                .message
                .contains("execute_partitions only supports queries"),
            "unexpected message for {sql}: {}",
            error.message
        );
    }
}

#[test]
fn all_dml_batch_guard_rejects_mixed_batches_and_passes_single_statements() {
    let split = |sql: &str| crate::sql::split_statements(sql);
    // Single statements pass whatever their kind, as do all-DML batches.
    for sql in [
        "SELECT 1",
        "INSERT INTO t (id) VALUES (1)",
        "DELETE FROM t WHERE true; INSERT INTO t (id) VALUES (1)",
        "@{PDML_MAX_PARALLELISM=1} DELETE FROM t WHERE true; update t set c = 1 where true",
        "",
    ] {
        check_all_dml_batch(&split(sql)).unwrap_or_else(|e| panic!("should pass: {sql}: {e}"));
    }
    for (sql, offending) in [
        ("DELETE FROM t WHERE true; SELECT 1", "SELECT 1"),
        ("SELECT 1; DELETE FROM t WHERE true", "SELECT 1"),
        ("DELETE FROM t WHERE true; DROP TABLE t", "DROP TABLE t"),
        ("SELECT 1; SELECT 2", "SELECT 1"),
    ] {
        let error = check_all_dml_batch(&split(sql)).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "{sql}");
        assert!(
            error.message.contains("all-DML") && error.message.contains(offending),
            "unexpected message for {sql}: {}",
            error.message
        );
    }
    // A `;` inside a literal doesn't separate statements.
    check_all_dml_batch(&split("SELECT 'a;b'")).unwrap();
}

#[test]
fn string_option_requires_a_string_value() {
    let key = OptionStatement::TargetTable;
    assert_eq!(
        string_option(&key, OptionValue::String("hi".into())).unwrap(),
        "hi"
    );
    for value in [OptionValue::Int(1), OptionValue::Double(1.0)] {
        let error = string_option(&key, value).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        assert!(
            error.message.contains("adbc.ingest.target_table"),
            "{}",
            error.message
        );
    }
}

#[test]
fn bool_option_rejects_non_bool_values() {
    for bad in [
        "maybe", "", "2", "t", "on", "TRUE", "True", "FALSE", "False", "1", "0", "yes", "no",
        "YES", "NO",
    ] {
        let error = bool_option(OptionValue::String(bad.into()), "option o").unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
    }
    for bad in [
        OptionValue::Int(0),
        OptionValue::Int(1),
        OptionValue::Double(1.0),
    ] {
        let error = bool_option(bad, "option o").unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
    }
}

#[test]
fn rows_per_batch_option_accepts_positive_ints_and_strings() {
    let parse = |value| crate::options::positive_usize(value, "option spanner.rows_per_batch");
    assert_eq!(parse(OptionValue::Int(1)).unwrap(), 1);
    assert_eq!(parse(OptionValue::String("8192".into())).unwrap(), 8192);
}

#[test]
fn rows_per_batch_option_rejects_zero_negative_and_malformed() {
    let parse = |value| crate::options::positive_usize(value, "option spanner.rows_per_batch");
    let error = parse(OptionValue::Int(0)).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    let error = parse(OptionValue::Int(-8192)).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    for bad in ["0", "-1", "abc", "1.5", ""] {
        let error = parse(OptionValue::String(bad.into())).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
    }
    let error = parse(OptionValue::Double(3.0)).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
}

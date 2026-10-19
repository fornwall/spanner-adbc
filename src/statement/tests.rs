use super::*;
use adbc_core::error::Status;
use arrow_array::Array;

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

#[test]
fn split_batches_caps_rows_per_batch() {
    let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
    let batch = |n: i64| {
        RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow_array::Int64Array::from_iter_values(0..n))],
        )
        .unwrap()
    };
    let rows = |batches: &[RecordBatch]| {
        batches
            .iter()
            .map(RecordBatch::num_rows)
            .collect::<Vec<_>>()
    };
    assert_eq!(
        rows(&split_batches(vec![batch(5), batch(0), batch(2)], 2)),
        [2, 2, 1, 0, 2]
    );
    assert_eq!(rows(&split_batches(vec![batch(3)], 3)), [3]);
    let split = split_batches(vec![batch(5)], 2);
    let last = split[2]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>();
    assert_eq!(last.unwrap().values(), &[4]);
}

/// Statements on an in-process mock server that only serves sessions. It records queries and
/// counts transaction RPCs, rejecting both with a non-retryable status.
mod mock {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use adbc_core::error::Status;
    use adbc_core::options::{OptionConnection, OptionDatabase, OptionStatement, OptionValue};
    use adbc_core::{Connection, Database, Driver, Optionable, Statement};
    use arrow_array::{Date32Array, Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use spanner_grpc_mock::MockSpanner;
    use spanner_grpc_mock::google::spanner::v1;

    use crate::{SpannerConnection, SpannerDriver, SpannerStatement};

    type Requests = Arc<Mutex<Vec<v1::ExecuteSqlRequest>>>;

    struct MockServer {
        endpoint: String,
        requests: Requests,
        /// Transaction and DML RPCs, which the mock rejects without recording their content.
        other_rpcs: Arc<AtomicUsize>,
        server: tokio::task::JoinHandle<()>,
        _runtime: tokio::runtime::Runtime,
    }

    impl Drop for MockServer {
        fn drop(&mut self) {
            self.server.abort();
        }
    }

    fn start() -> MockServer {
        let requests = Requests::default();
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|request| {
            Ok(tonic::Response::new(v1::Session {
                name: format!("{}/sessions/mock", request.into_inner().database),
                multiplexed: true,
                ..Default::default()
            }))
        });
        let record = requests.clone();
        mock.expect_execute_streaming_sql()
            .returning(move |request| {
                record.lock().unwrap().push(request.into_inner());
                Err(tonic::Status::unimplemented("mock spanner server"))
            });
        // An unexpected call would panic the server task, which the client retries forever.
        let other_rpcs = Arc::new(AtomicUsize::new(0));
        macro_rules! reject_and_count {
            ($($method:ident),+) => {
                $(
                    let count = other_rpcs.clone();
                    mock.$method().returning(move |_| {
                        count.fetch_add(1, Ordering::SeqCst);
                        Err(tonic::Status::unimplemented("mock spanner server"))
                    });
                )+
            };
        }
        reject_and_count!(
            expect_begin_transaction,
            expect_commit,
            expect_rollback,
            expect_execute_batch_dml,
            expect_batch_write
        );
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let (endpoint, server) = runtime
            .block_on(spanner_grpc_mock::start("127.0.0.1:0", mock))
            .unwrap();
        MockServer {
            endpoint,
            requests,
            other_rpcs,
            server,
            _runtime: runtime,
        }
    }

    fn rpc_count(server: &MockServer) -> usize {
        server.requests.lock().unwrap().len() + server.other_rpcs.load(Ordering::SeqCst)
    }

    fn connect(server: &MockServer) -> SpannerConnection {
        let mut driver = SpannerDriver::try_new().unwrap();
        driver
            .new_database_with_opts([
                (
                    OptionDatabase::Uri,
                    OptionValue::String("spanner:///projects/p/instances/i/databases/d".into()),
                ),
                (
                    OptionDatabase::Other(crate::OPTION_ENDPOINT.into()),
                    OptionValue::String(server.endpoint.clone()),
                ),
                (
                    OptionDatabase::Other(crate::OPTION_EMULATOR.into()),
                    OptionValue::String("true".into()),
                ),
            ])
            .unwrap()
            .new_connection()
            .unwrap()
    }

    fn set_connection_option(connection: &mut SpannerConnection, key: OptionConnection, v: &str) {
        connection
            .set_option(key, OptionValue::String(v.into()))
            .unwrap();
    }

    fn one_row() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("p", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![1_i64]))],
        )
        .unwrap()
    }

    fn bound_statement(connection: &mut SpannerConnection, sql: &str) -> SpannerStatement {
        let mut statement = connection.new_statement().unwrap();
        statement.set_sql_query(sql).unwrap();
        statement.bind(one_row()).unwrap();
        statement
    }

    /// Every execution consumes bound rows, even one rejected before any RPC, so a reused handle
    /// cannot apply them to its next statement.
    #[test]
    fn rejected_executions_consume_bound_rows() {
        let server = start();
        let mut connection = connect(&server);

        let mut statement = bound_statement(&mut connection, "SELECT 1; DELETE FROM t WHERE true");
        let error = statement.execute_update().unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "{error}");
        assert!(
            statement.bound.is_empty(),
            "mixed batch kept its bound rows"
        );

        let mut statement = bound_statement(&mut connection, "DELETE FROM t WHERE c = @p");
        let error = statement.execute_partitions().unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "{error}");
        assert!(
            statement.bound.is_empty(),
            "execute_partitions kept its bound rows"
        );

        set_connection_option(&mut connection, OptionConnection::ReadOnly, "true");
        for update in [false, true] {
            let mut statement = bound_statement(&mut connection, "DROP TABLE t");
            let error = if update {
                statement.execute_update().unwrap_err()
            } else {
                statement.execute().err().unwrap()
            };
            assert_eq!(error.status, Status::InvalidState, "{error}");
            assert!(statement.bound.is_empty(), "failed DDL kept its bound rows");
        }
        let mut ingest = connection.new_statement().unwrap();
        ingest
            .set_option(
                OptionStatement::TargetTable,
                OptionValue::String("t".into()),
            )
            .unwrap();
        ingest.bind(one_row()).unwrap();
        let error = ingest.execute_update().unwrap_err();
        assert_eq!(error.status, Status::InvalidState, "{error}");
        assert!(
            ingest.bound.is_empty(),
            "read-only ingest kept its bound rows"
        );
        set_connection_option(&mut connection, OptionConnection::ReadOnly, "false");

        set_connection_option(&mut connection, OptionConnection::AutoCommit, "false");
        let mut insert = connection.new_statement().unwrap();
        insert
            .set_sql_query("INSERT INTO t (c) VALUES (1)")
            .unwrap();
        assert_eq!(insert.execute_update().unwrap(), None);
        let mut query = bound_statement(&mut connection, "SELECT c FROM t WHERE c = @p");
        let error = query.execute().err().unwrap();
        assert_eq!(error.status, Status::InvalidState, "{error}");
        assert!(
            query.bound.is_empty(),
            "query rejected by the DML guard kept its bound rows"
        );
        connection.rollback().unwrap();
        assert!(server.requests.lock().unwrap().is_empty());
    }

    /// The DBAPI reuses one statement for SQL and ingest and sets the schema only when given one,
    /// so setting SQL must clear the whole ingest target.
    #[test]
    fn setting_sql_clears_the_whole_ingest_target() {
        let server = start();
        let mut connection = connect(&server);
        let mut statement = connection.new_statement().unwrap();
        for (key, value) in [
            (OptionStatement::TargetDbSchema, "s1"),
            (OptionStatement::TargetCatalog, "d"),
            (OptionStatement::TargetTable, "t"),
        ] {
            statement
                .set_option(key, OptionValue::String(value.into()))
                .unwrap();
        }
        statement.set_sql_query("SELECT 1").unwrap();
        for key in [
            OptionStatement::TargetTable,
            OptionStatement::TargetDbSchema,
            OptionStatement::TargetCatalog,
        ] {
            let error = statement.get_option_string(key.clone()).unwrap_err();
            assert_eq!(error.status, Status::NotFound, "{key:?}");
        }
        statement
            .set_option(
                OptionStatement::TargetTable,
                OptionValue::String("t".into()),
            )
            .unwrap();
        assert_eq!(statement.target_db_schema, None);
    }

    /// Parameters are typed against the schema version the query reads, like `execute_schema`.
    #[test]
    fn parameter_plan_probe_reads_at_the_statements_staleness() {
        use v1::transaction_options::read_only::TimestampBound as WireBound;

        let server = start();
        let mut connection = connect(&server);
        let mut statement = connection.new_statement().unwrap();
        statement
            .set_option(
                OptionStatement::Other(crate::OPTION_READ_STALENESS.into()),
                OptionValue::String("exact:10s".into()),
            )
            .unwrap();
        statement
            .set_sql_query("SELECT c FROM t WHERE c = @p")
            .unwrap();
        // The mock rejects the probe, which leaves the type unknown.
        let schema = statement.get_parameter_schema().unwrap();
        assert_eq!(schema.field(0).data_type(), &DataType::Null);

        let requests = server.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].query_mode,
            v1::execute_sql_request::QueryMode::Plan as i32
        );
        let Some(v1::transaction_selector::Selector::SingleUse(options)) = requests[0]
            .transaction
            .as_ref()
            .and_then(|t| t.selector.as_ref())
        else {
            panic!("expected a single-use read: {:?}", requests[0]);
        };
        let Some(v1::transaction_options::Mode::ReadOnly(read_only)) = &options.mode else {
            panic!("expected read-only options: {options:?}");
        };
        assert_eq!(
            read_only.timestamp_bound,
            Some(WireBound::ExactStaleness(prost_types::Duration {
                seconds: 10,
                nanos: 0,
            }))
        );
    }

    fn ingest_statement(
        connection: &mut SpannerConnection,
        db_schema: Option<&str>,
        table: &str,
        mode: &str,
    ) -> SpannerStatement {
        let mut statement = connection.new_statement().unwrap();
        if let Some(db_schema) = db_schema {
            statement
                .set_option(
                    OptionStatement::TargetDbSchema,
                    OptionValue::String(db_schema.into()),
                )
                .unwrap();
        }
        for (key, value) in [
            (OptionStatement::TargetTable, table),
            (OptionStatement::IngestMode, mode),
            // Bounds the admin RPC if the DDL runs anyway; the mock serves no admin API.
            (
                OptionStatement::Other(crate::OPTION_RPC_TIMEOUT_UPDATE.into()),
                "5",
            ),
        ] {
            statement
                .set_option(key, OptionValue::String(value.into()))
                .unwrap();
        }
        statement
    }

    /// DDL can't be rolled back, and `replace` drops the existing table, so a value that fails
    /// to convert must fail the ingest before any DDL runs.
    #[test]
    fn ingest_value_errors_fail_before_the_table_ddl() {
        let server = start();
        let mut connection = connect(&server);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("d", DataType::Date32, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1_i64, 2])),
                Arc::new(Date32Array::from(vec![0, i32::MAX])),
            ],
        )
        .unwrap();
        for mode in ["replace", "create", "create_append"] {
            let mut statement = ingest_statement(&mut connection, None, "t", mode);
            statement.bind(batch.clone()).unwrap();
            let error = statement.execute_update().unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments, "{mode}: {error}");
            assert!(error.message.contains("out of range"), "{mode}: {error}");
        }
        assert_eq!(rpc_count(&server), 0);
    }

    /// Every mode rejects names Spanner can never accept, before any RPC. Append builds no DDL,
    /// so without the check it would send them in mutations and report `NotFound`.
    #[test]
    fn ingest_rejects_invalid_names_in_every_mode() {
        let server = start();
        let mut connection = connect(&server);
        let named = |column: &str| {
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    column,
                    DataType::Int64,
                    false,
                )])),
                vec![Arc::new(Int64Array::from(vec![1_i64]))],
            )
            .unwrap()
        };
        for mode in ["append", "create", "create_append", "replace"] {
            for (db_schema, table, column) in [
                (None, "", "c"),
                (None, "t`x", "c"),
                (None, "t\\x", "c"),
                (Some("s`"), "t", "c"),
                (None, "t", ""),
                (None, "t", "c`d"),
            ] {
                let mut statement = ingest_statement(&mut connection, db_schema, table, mode);
                statement.bind(named(column)).unwrap();
                let error = statement.execute_update().unwrap_err();
                assert_eq!(
                    error.status,
                    Status::InvalidArguments,
                    "{mode} {db_schema:?}.{table:?} column {column:?}: {error}"
                );
            }
        }
        assert_eq!(rpc_count(&server), 0);
    }

    /// Zero bound rows execute nothing, as plain DML does; the runner would otherwise begin and
    /// commit an empty read/write transaction.
    #[test]
    fn returning_dml_with_zero_bound_rows_sends_nothing() {
        let server = start();
        let mut connection = connect(&server);
        let sql = "UPDATE t SET c = 1 WHERE c = @p THEN RETURN c";
        for update in [true, false] {
            let mut statement = connection.new_statement().unwrap();
            statement.set_sql_query(sql).unwrap();
            statement.bind(one_row().slice(0, 0)).unwrap();
            if update {
                assert_eq!(statement.execute_update().unwrap(), Some(0));
            } else {
                let reader = statement.execute().unwrap();
                assert_eq!(reader.count(), 0);
            }
        }
        assert_eq!(rpc_count(&server), 0);
    }
}

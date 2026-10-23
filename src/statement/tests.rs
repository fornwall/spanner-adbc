use super::*;
use adbc_core::error::Status;
use arrow_array::Array;

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
fn query_only_guard_rejects_ddl_and_dml() {
    for (entry_point, rationale) in [
        ("execute_schema", "planned in a read-only schema probe"),
        (
            "execute_partitions",
            "partitioned in a batch read-only transaction",
        ),
    ] {
        let check = |sql: &str| check_query_only(sql, entry_point, rationale);
        for sql in [
            "SELECT 1",
            "WITH cte AS (SELECT 1 AS a) SELECT a FROM cte",
            "(SELECT 1)",
            "@{USE_ADDITIONAL_PARALLELISM=true} SELECT 1",
            "GRAPH g MATCH (n) RETURN n.id",
        ] {
            check(sql).unwrap_or_else(|e| panic!("query should pass: {sql}: {e}"));
        }
        for sql in [
            "CREATE TABLE t (id INT64) PRIMARY KEY (id)",
            "INSERT INTO t (id) VALUES (1)",
            "update t set c = 1 where true",
            "Delete From t Where true",
            "/* comment */ INSERT INTO t (id) VALUES (1)",
            "@{PDML_MAX_PARALLELISM=1} DELETE FROM t WHERE true",
            "INSERT INTO t (id) VALUES (1) THEN RETURN id",
        ] {
            let error = check(sql).unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments, "{sql}");
            assert!(
                error
                    .message
                    .contains(&format!("{entry_point} only supports queries")),
                "unexpected message for {sql}: {}",
                error.message
            );
        }
    }
}

#[test]
fn batch_guard_rejects_mixed_batches_and_passes_single_statements() {
    use crate::sql::split_statements as split;
    // Single statements pass whatever their kind, as do all-DML and all-DDL batches.
    for sql in [
        "SELECT 1",
        "INSERT INTO t (id) VALUES (1)",
        "DELETE FROM t WHERE true; INSERT INTO t (id) VALUES (1)",
        "@{PDML_MAX_PARALLELISM=1} DELETE FROM t WHERE true; update t set c = 1 where true",
        "DROP TABLE t; CREATE TABLE t (id INT64) PRIMARY KEY (id)",
        "",
    ] {
        check_batch(&split(sql)).unwrap_or_else(|e| panic!("should pass: {sql}: {e}"));
    }
    for (sql, offending, kind) in [
        ("DELETE FROM t WHERE true; SELECT 1", "SELECT 1", "all-DML"),
        ("SELECT 1; DELETE FROM t WHERE true", "SELECT 1", "all-DML"),
        (
            "DELETE FROM t WHERE true; DROP TABLE t",
            "DROP TABLE t",
            "all-DML",
        ),
        ("SELECT 1; SELECT 2", "SELECT 1", "all-DML"),
        (
            "CREATE TABLE t (id INT64) PRIMARY KEY (id); INSERT INTO t (id) VALUES (1)",
            "INSERT INTO t (id) VALUES (1)",
            "all DDL",
        ),
    ] {
        let error = check_batch(&split(sql)).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "{sql}");
        assert!(
            error.message.contains(kind) && error.message.contains(offending),
            "unexpected message for {sql}: {}",
            error.message
        );
    }
    // A `;` inside a literal doesn't separate statements.
    check_batch(&split("SELECT 'a;b'")).unwrap();
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
    use adbc_core::options::{OptionConnection, OptionStatement, OptionValue};
    use adbc_core::{Connection, Optionable, Statement};
    use arrow_array::{Date32Array, Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use spanner_grpc_mock::MockSpanner;
    use spanner_grpc_mock::google::spanner::v1;

    use crate::{SpannerConnection, SpannerStatement};

    type Requests = Arc<Mutex<Vec<v1::ExecuteSqlRequest>>>;

    struct MockServer {
        server: crate::mock_server::MockServer,
        requests: Requests,
        /// Transaction and DML RPCs, which the mock rejects without recording their content.
        other_rpcs: Arc<AtomicUsize>,
    }

    fn start() -> MockServer {
        let requests = Requests::default();
        let mut mock = MockSpanner::new();
        let record = requests.clone();
        mock.expect_execute_streaming_sql()
            .returning(move |request| {
                record.lock().unwrap().push(request.into_inner());
                Err(tonic::Status::unimplemented("mock spanner server"))
            });
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
        MockServer {
            server: crate::mock_server::MockServer::serve(mock),
            requests,
            other_rpcs,
        }
    }

    fn rpc_count(server: &MockServer) -> usize {
        server.requests.lock().unwrap().len() + server.other_rpcs.load(Ordering::SeqCst)
    }

    fn connect(server: &MockServer) -> SpannerConnection {
        server.server.connect()
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

    /// Like the shared options, the driver's statement-only options reset to their default on
    /// `""`.
    #[test]
    fn statement_only_options_reset_on_an_empty_string() {
        let server = start();
        let mut connection = connect(&server);
        let mut statement = connection.new_statement().unwrap();
        for (key, value, default) in [
            (crate::OPTION_ROWS_PER_BATCH, "5", "8192"),
            (crate::OPTION_DATA_BOOST, "true", "false"),
            (crate::OPTION_INGEST_BATCH_WRITE, "true", "false"),
            (crate::OPTION_DML_PARTITIONED, "true", "false"),
        ] {
            let key = OptionStatement::Other(key.into());
            for (set, expected) in [(value, value), ("", default)] {
                statement
                    .set_option(key.clone(), OptionValue::from(set))
                    .unwrap();
                assert_eq!(statement.get_option_string(key.clone()).unwrap(), expected);
            }
        }
    }

    /// Requests the driver can't serve fail with an actionable error before any RPC.
    #[test]
    fn unsupported_requests_fail_before_any_rpc() {
        let server = start();
        let mut connection = connect(&server);
        let mut statement = connection.new_statement().unwrap();

        // Generic clients set the spec default `false`.
        let temporary = |value: &str| OptionValue::String(value.into());
        statement
            .set_option(OptionStatement::Temporary, temporary("false"))
            .unwrap();
        let error = statement
            .set_option(OptionStatement::Temporary, temporary("true"))
            .unwrap_err();
        assert_eq!(error.status, Status::NotImplemented);
        assert!(error.message.contains("no temporary tables"), "{error}");
        // Invalid values fail boolean parsing before the feature check.
        let error = statement
            .set_option(OptionStatement::Temporary, temporary("TRUE"))
            .unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);

        let error = statement.set_substrait_plan(b"plan").unwrap_err();
        assert_eq!(error.status, Status::NotImplemented);
        assert!(error.message.contains("set_sql_query"), "{error}");

        statement
            .set_sql_query("INSERT INTO t (c) VALUES (1)")
            .unwrap();
        let error = statement.execute_schema().unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        assert!(
            error
                .message
                .contains("execute_schema only supports queries"),
            "{error}"
        );

        statement
            .set_sql_query(
                "INSERT INTO t (c) VALUES (1) THEN RETURN c; \
                 INSERT INTO t (c) VALUES (2) THEN RETURN c",
            )
            .unwrap();
        let error = statement.execute().err().unwrap();
        assert_eq!(error.status, Status::NotImplemented);
        assert!(error.message.contains("execute each statement"), "{error}");
        assert_eq!(rpc_count(&server), 0);
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

    /// `adbc.h` describes the statement's parameters, so the SQL names them, whatever the bound
    /// columns are called.
    #[test]
    fn parameter_schema_lists_the_sql_parameters_typed_by_bound_columns() {
        let server = start();
        let mut connection = connect(&server);
        let mut statement = connection.new_statement().unwrap();
        statement.set_sql_query("SELECT @b, @a, @B").unwrap();
        let batch = |names: &[&str]| {
            let fields = names
                .iter()
                .map(|name| Field::new(*name, DataType::Date32, true));
            let columns = names
                .iter()
                .map(|_| Arc::new(Date32Array::from(vec![0])) as _);
            RecordBatch::try_new(
                Arc::new(Schema::new(fields.collect::<Vec<_>>())),
                columns.collect(),
            )
            .unwrap()
        };
        statement.bind(batch(&["x", "y"])).unwrap();
        let expected = |b_type| {
            Schema::new(vec![
                Field::new("b", b_type, true),
                Field::new("a", DataType::Date32, true),
            ])
        };
        assert_eq!(
            statement.get_parameter_schema().unwrap(),
            expected(DataType::Date32)
        );
        assert_eq!(rpc_count(&server), 0, "bound types need no probe");

        // By name, an unbound parameter's type comes from the probe, which the mock rejects.
        statement
            .set_option(
                OptionStatement::Other(crate::OPTION_BIND_BY_NAME.into()),
                OptionValue::String("true".into()),
            )
            .unwrap();
        statement.bind(batch(&["A"])).unwrap();
        assert_eq!(
            statement.get_parameter_schema().unwrap(),
            expected(DataType::Null)
        );
        assert_eq!(rpc_count(&server), 1);
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

    /// A value that fails to convert must fail the ingest before anything is written: DDL can't
    /// be rolled back, `replace` drops the existing table, and these rows span two autocommit
    /// chunks, so an append would otherwise commit the first.
    #[test]
    fn ingest_value_errors_fail_before_anything_is_written() {
        let server = start();
        let mut connection = connect(&server);
        let rows = 10_001;
        let mut days = vec![0; rows];
        days[rows - 1] = i32::MAX;
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("d", DataType::Date32, false),
            ])),
            vec![
                Arc::new(Int64Array::from_iter_values(0..rows as i64)),
                Arc::new(Date32Array::from(days)),
            ],
        )
        .unwrap();
        for mode in ["replace", "create", "create_append", "append"] {
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

/// Manual-transaction behavior against a mock that serves queries, batch DML and commits.
mod manual_txn {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use adbc_core::error::Status;
    use adbc_core::options::{OptionConnection, OptionStatement, OptionValue};
    use adbc_core::{Connection, Optionable, Statement};
    use arrow_array::{RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use spanner_grpc_mock::MockSpanner;
    use spanner_grpc_mock::google::spanner::v1;

    use crate::{SpannerConnection, SpannerStatement};

    type Selectors = Arc<Mutex<Vec<(String, Option<v1::transaction_selector::Selector>)>>>;

    /// What the mock does with each `Commit`, by zero-based call index.
    type CommitScript = Box<dyn Fn(usize) -> CommitAction + Send + Sync>;

    enum CommitAction {
        Ok,
        Fail(tonic::Code),
        /// Reject the commit as Spanner does one over its mutation limit.
        TooManyMutations,
        /// Block until the test releases the gate, then fail with `UNAVAILABLE`.
        Hang,
    }

    #[derive(Default)]
    struct Recorded {
        batch_dml: Mutex<Vec<v1::ExecuteBatchDmlRequest>>,
        commits: Mutex<Vec<v1::CommitRequest>>,
    }

    struct MockServer {
        server: crate::mock_server::MockServer,
        recorded: Arc<Recorded>,
        selectors: Selectors,
        /// Releases hung commits, so dropping the server does not wait on them.
        gate: Arc<AtomicBool>,
    }

    impl Drop for MockServer {
        fn drop(&mut self) {
            self.gate.store(true, Ordering::SeqCst);
        }
    }

    fn result(inline_begin: bool, txn_id: &[u8]) -> v1::PartialResultSet {
        v1::PartialResultSet {
            metadata: Some(v1::ResultSetMetadata {
                row_type: Some(v1::StructType {
                    fields: vec![v1::struct_type::Field {
                        name: "c".into(),
                        r#type: Some(v1::Type {
                            code: v1::TypeCode::String as i32,
                            ..Default::default()
                        }),
                    }],
                }),
                // Spanner returns the read timestamp of a read-only transaction it begins.
                transaction: inline_begin.then(|| v1::Transaction {
                    id: txn_id.to_vec(),
                    read_timestamp: Some(prost_types::Timestamp {
                        seconds: 1_800_000_000,
                        nanos: 0,
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            values: vec![prost_types::Value {
                kind: Some(prost_types::value::Kind::StringValue("v".into())),
            }],
            last: true,
            ..Default::default()
        }
    }

    fn start() -> MockServer {
        start_with(Box::new(|_| CommitAction::Ok))
    }

    /// Queries on `Missing` fail with `INVALID_ARGUMENT`, as Spanner reports an unknown table, and
    /// a table probe finds no `Missing` table and column `C` in any other. Other queries return
    /// one row, and the transaction id when they begin inline. Batch DML reports one row per
    /// statement; commits follow `commits`.
    fn start_with(commits: CommitScript) -> MockServer {
        let recorded = Arc::new(Recorded::default());
        let selectors = Selectors::default();
        let gate = Arc::new(AtomicBool::new(false));
        let mut mock = MockSpanner::new();
        let record = selectors.clone();
        let begun = Arc::new(Mutex::new(0_u32));
        mock.expect_execute_streaming_sql()
            .returning(move |request| {
                let request = request.into_inner();
                let selector = request.transaction.and_then(|t| t.selector);
                let inline_begin =
                    matches!(selector, Some(v1::transaction_selector::Selector::Begin(_)));
                record.lock().unwrap().push((request.sql.clone(), selector));
                if request.sql.contains("Missing") {
                    return Err(tonic::Status::invalid_argument("Table not found: Missing"));
                }
                let probed_table = request
                    .params
                    .as_ref()
                    .and_then(|params| params.fields.get("table"))
                    .and_then(|table| match &table.kind {
                        Some(prost_types::value::Kind::StringValue(name)) => Some(name.clone()),
                        _ => None,
                    });
                let id = {
                    let mut begun = begun.lock().unwrap();
                    *begun += u32::from(inline_begin);
                    format!("ro-{begun}")
                };
                let mut response = result(inline_begin, id.as_bytes());
                match probed_table.as_deref() {
                    Some("Missing") => response.values.clear(),
                    Some(_) => {
                        response.values = vec![prost_types::Value {
                            kind: Some(prost_types::value::Kind::StringValue("C".into())),
                        }]
                    }
                    None => {}
                }
                let (tx, rx) = tokio::sync::mpsc::channel(1);
                tx.try_send(Ok(response)).unwrap();
                Ok(tonic::Response::new(rx))
            });
        let record = recorded.clone();
        mock.expect_execute_batch_dml().returning(move |request| {
            let request = request.into_inner();
            let inline_begin = matches!(
                request
                    .transaction
                    .as_ref()
                    .and_then(|t| t.selector.as_ref()),
                Some(v1::transaction_selector::Selector::Begin(_))
            );
            let result_sets = (0..request.statements.len())
                .map(|i| v1::ResultSet {
                    metadata: Some(v1::ResultSetMetadata {
                        transaction: (i == 0 && inline_begin).then(|| v1::Transaction {
                            id: b"rw-txn".to_vec(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                        ..Default::default()
                    }),
                    ..Default::default()
                })
                .collect();
            record.batch_dml.lock().unwrap().push(request);
            Ok(tonic::Response::new(v1::ExecuteBatchDmlResponse {
                result_sets,
                ..Default::default()
            }))
        });
        mock.expect_begin_transaction().returning(|_| {
            Ok(tonic::Response::new(v1::Transaction {
                id: b"wo-txn".to_vec(),
                ..Default::default()
            }))
        });
        mock.expect_rollback()
            .returning(|_| Ok(tonic::Response::new(())));
        let record = recorded.clone();
        let hung = gate.clone();
        mock.expect_commit().returning(move |request| {
            let index = {
                let mut commits = record.commits.lock().unwrap();
                commits.push(request.into_inner());
                commits.len() - 1
            };
            match commits(index) {
                CommitAction::Ok => Ok(tonic::Response::new(v1::CommitResponse::default())),
                CommitAction::Fail(code) => Err(tonic::Status::new(code, "mock commit failure")),
                CommitAction::TooManyMutations => Err(tonic::Status::invalid_argument(
                    "The transaction contains too many mutations. (Maximum number: 80000)",
                )),
                CommitAction::Hang => {
                    tokio::task::block_in_place(|| {
                        let deadline = Instant::now() + Duration::from_secs(30);
                        while !hung.load(Ordering::SeqCst) && Instant::now() < deadline {
                            std::thread::sleep(Duration::from_millis(10));
                        }
                    });
                    Err(tonic::Status::unavailable("released hung commit"))
                }
            }
        });
        MockServer {
            server: crate::mock_server::MockServer::serve(mock),
            recorded,
            selectors,
            gate,
        }
    }

    fn connect(server: &MockServer) -> SpannerConnection {
        server.server.connect()
    }

    fn manual(server: &MockServer) -> SpannerConnection {
        let mut connection = connect(server);
        connection
            .set_option(OptionConnection::AutoCommit, OptionValue::from("false"))
            .unwrap();
        connection
    }

    fn query(connection: &mut SpannerConnection, sql: &str) -> adbc_core::error::Result<usize> {
        let mut statement = connection.new_statement()?;
        statement.set_sql_query(sql)?;
        let reader = statement.execute()?;
        let mut rows = 0;
        for batch in reader {
            rows += batch
                .map_err(|e| crate::error::err(e.to_string(), Status::Internal))?
                .num_rows();
        }
        Ok(rows)
    }

    fn update(connection: &mut SpannerConnection, sql: &str) -> Option<i64> {
        let mut statement = connection.new_statement().unwrap();
        statement.set_sql_query(sql).unwrap();
        statement.execute_update().unwrap()
    }

    /// An `append` ingest of `rows` rows into `t`, which needs no admin RPC.
    fn ingest(connection: &mut SpannerConnection, rows: usize) -> SpannerStatement {
        let mut statement = connection.new_statement().unwrap();
        for (key, value) in [
            (OptionStatement::TargetTable, "t"),
            (OptionStatement::IngestMode, "append"),
        ] {
            statement
                .set_option(key, OptionValue::String(value.into()))
                .unwrap();
        }
        let values = StringArray::from((0..rows).map(|i| format!("v{i}")).collect::<Vec<_>>());
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("c", DataType::Utf8, false)])),
            vec![Arc::new(values)],
        )
        .unwrap();
        statement.bind(batch).unwrap();
        statement
    }

    /// Run `statement` through `execute_counted`, returning the rows read and the count.
    fn counted(mut statement: SpannerStatement) -> (usize, Option<i64>) {
        let (reader, count) = statement.execute_counted().unwrap();
        (reader.map(|batch| batch.unwrap().num_rows()).sum(), count)
    }

    fn sql_statement(connection: &mut SpannerConnection, sql: &str) -> SpannerStatement {
        let mut statement = connection.new_statement().unwrap();
        statement.set_sql_query(sql).unwrap();
        statement
    }

    /// `execute` knows the count of the writes it ran, as `execute_update` reports it, so the C
    /// ABI can return it alongside a result stream. Queries and buffered writes have none.
    #[test]
    fn execute_counts_the_rows_of_writes_that_ran() {
        let server = start();
        let mut connection = connect(&server);
        for (sql, expected) in [
            ("UPDATE t SET c = 'x' WHERE TRUE", (0, Some(1))),
            (
                "UPDATE t SET c = 'x' WHERE TRUE; DELETE FROM t WHERE TRUE",
                (0, Some(2)),
            ),
            ("INSERT INTO t (c) VALUES ('v') THEN RETURN c", (1, Some(1))),
            ("SELECT c FROM t", (1, None)),
        ] {
            assert_eq!(
                counted(sql_statement(&mut connection, sql)),
                expected,
                "{sql}"
            );
        }
        assert_eq!(counted(ingest(&mut connection, 3)), (0, Some(3)));

        let mut connection = manual(&server);
        assert_eq!(
            counted(sql_statement(
                &mut connection,
                "UPDATE t SET c = 'x' WHERE TRUE"
            )),
            (0, None)
        );
        assert_eq!(counted(ingest(&mut connection, 3)), (0, None));
        connection.commit().unwrap();
    }

    /// Spanner's mutation-limit error doesn't know the driver's remedies, so autocommit DML names
    /// them.
    #[test]
    fn autocommit_dml_over_the_mutation_limit_names_the_remedies() {
        let server = start_with(Box::new(|_| CommitAction::TooManyMutations));
        let mut connection = connect(&server);
        let mut statement = sql_statement(&mut connection, "UPDATE t SET c = 'x' WHERE TRUE");
        let error = statement.execute_update().unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "{error}");
        assert!(
            error.message.contains("too many mutations")
                && error.message.contains("spanner.dml.partitioned")
                && error.message.contains("bulk ingest"),
            "{error}"
        );
    }

    /// A commit that fails with `UNAVAILABLE` may have been applied, so retrying the DML could
    /// apply it twice.
    #[test]
    fn autocommit_dml_with_an_unknown_commit_outcome_says_so() {
        let server = start_with(Box::new(|_| CommitAction::Fail(tonic::Code::Unavailable)));
        let mut connection = connect(&server);
        let mut statement = sql_statement(&mut connection, "UPDATE t SET c = 'x' WHERE TRUE");
        statement
            .set_option(
                OptionStatement::Other(crate::OPTION_RETRY_MAX_ATTEMPTS.into()),
                OptionValue::from("1"),
            )
            .unwrap();
        let error = statement.execute_update().unwrap_err();
        assert_eq!(error.status, Status::IO, "{error}");
        assert!(
            error.message.contains("may still have been committed"),
            "{error}"
        );
    }

    /// A failed first query begins nothing, so it must not poison the manual transaction: with
    /// an inline begin the client marks the unstarted transaction permanently failed.
    #[test]
    fn failed_first_query_does_not_poison_the_manual_read_transaction() {
        let server = start();
        let mut connection = manual(&server);

        let error = query(&mut connection, "SELECT c FROM Missing").unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "{error}");
        assert_eq!(
            query(&mut connection, "SELECT c FROM t").expect("the next query runs"),
            1
        );
        // A begun transaction keeps its snapshot after a failed query.
        let error = query(&mut connection, "SELECT c FROM Missing").unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "{error}");
        assert_eq!(query(&mut connection, "SELECT c FROM t").unwrap(), 1);

        let selectors = server.selectors.lock().unwrap();
        assert_eq!(selectors.len(), 4, "{selectors:?}");
        for (index, (_, selector)) in selectors.iter().take(2).enumerate() {
            assert!(
                matches!(selector, Some(v1::transaction_selector::Selector::Begin(_))),
                "query {index} begins inline: {selector:?}"
            );
        }
        for (sql, selector) in &selectors[2..] {
            assert_eq!(
                selector,
                &Some(v1::transaction_selector::Selector::Id(b"ro-1".to_vec())),
                "{sql}: later queries share the transaction the first successful query began"
            );
        }
    }

    /// A query bound to several rows runs each row's statement in the manual transaction's
    /// snapshot, and a failed first statement leaves it unbegun, as for a single query.
    #[test]
    fn multi_row_query_shares_the_manual_read_transaction() {
        use v1::transaction_selector::Selector;

        let server = start();
        let mut connection = manual(&server);
        let mut bound_query = |sql: &str| {
            let mut statement = connection.new_statement().unwrap();
            statement.set_sql_query(sql).unwrap();
            let rows = arrow_array::Int64Array::from(vec![1, 2]);
            let schema = Schema::new(vec![Field::new("p", DataType::Int64, false)]);
            statement
                .bind(RecordBatch::try_new(Arc::new(schema), vec![Arc::new(rows)]).unwrap())
                .unwrap();
            let reader = statement.execute()?;
            Ok::<usize, adbc_core::error::Error>(reader.map(|b| b.unwrap().num_rows()).sum())
        };
        let error = bound_query("SELECT c FROM Missing WHERE c = @p").unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "{error}");
        assert_eq!(bound_query("SELECT c FROM t WHERE c = @p").unwrap(), 2);

        let selectors = server.selectors.lock().unwrap();
        let selectors: Vec<_> = selectors.iter().map(|(_, selector)| selector).collect();
        assert!(matches!(
            selectors[..],
            [Some(Selector::Begin(_)), Some(Selector::Begin(_)), _]
        ));
        assert_eq!(selectors[2], &Some(Selector::Id(b"ro-1".to_vec())));
    }

    /// ADBC requires `NotFound` for an append into a missing table and `AlreadyExists` for a
    /// column the table lacks, but a buffered insert would only fail at commit, so a manual
    /// ingest checks the table first and buffers nothing.
    #[test]
    fn manual_append_into_a_mismatched_table_fails_at_execution() {
        let server = start();
        let mut connection = manual(&server);
        let mut statement = ingest(&mut connection, 2);
        statement
            .set_option(OptionStatement::TargetTable, OptionValue::from("Missing"))
            .unwrap();
        let error = statement.execute_update().unwrap_err();
        assert_eq!(error.status, Status::NotFound, "{error}");

        let mut statement = connection.new_statement().unwrap();
        statement
            .set_option(OptionStatement::TargetTable, OptionValue::from("t"))
            .unwrap();
        statement
            .set_option(OptionStatement::IngestMode, OptionValue::from("append"))
            .unwrap();
        let schema = Schema::new(vec![
            Field::new("c", DataType::Utf8, true),
            Field::new("extra", DataType::Utf8, true),
        ]);
        statement
            .bind(RecordBatch::new_empty(Arc::new(schema)))
            .unwrap();
        let error = statement.execute_update().unwrap_err();
        assert_eq!(error.status, Status::AlreadyExists, "{error}");
        assert!(error.message.contains("no column \"extra\""), "{error}");

        connection.commit().unwrap();
        assert!(server.recorded.commits.lock().unwrap().is_empty());
    }

    /// A query whose parameter fails to bind runs nothing, so it must not fix the manual
    /// transaction's kind to queries.
    #[test]
    fn bind_failure_does_not_fix_the_manual_transaction_kind() {
        let server = start();
        let mut connection = manual(&server);
        let mut statement = sql_statement(&mut connection, "SELECT c FROM t WHERE c = @p");
        let unsupported = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("p", DataType::UInt64, false)])),
            vec![Arc::new(arrow_array::UInt64Array::from(vec![1_u64]))],
        )
        .unwrap();
        statement.bind(unsupported).unwrap();
        let error = statement.execute().err().unwrap();
        assert_eq!(error.status, Status::InvalidArguments, "{error}");
        assert_eq!(
            update(&mut connection, "UPDATE t SET c = 'x' WHERE TRUE"),
            None
        );
        connection.commit().unwrap();
        assert!(server.selectors.lock().unwrap().is_empty());
    }

    /// An `ABORTED` commit reruns the whole transaction: the batch DML and the buffered
    /// mutations go out again, the commit succeeds once, and the buffer is cleared.
    #[test]
    fn aborted_commit_replays_buffered_dml_and_mutations() {
        let server = start_with(Box::new(|index| {
            if index == 0 {
                CommitAction::Fail(tonic::Code::Aborted)
            } else {
                CommitAction::Ok
            }
        }));
        let mut connection = manual(&server);
        assert_eq!(
            update(&mut connection, "UPDATE t SET c = 'x' WHERE TRUE"),
            None
        );
        assert_eq!(ingest(&mut connection, 2).execute_update().unwrap(), None);
        connection.commit().expect("the rerun commits");

        let batch_dml = server.recorded.batch_dml.lock().unwrap().clone();
        let commits = server.recorded.commits.lock().unwrap().clone();
        assert_eq!(batch_dml.len(), 2, "one batch per attempt");
        assert_eq!(commits.len(), 2, "one commit per attempt");
        for (request, commit) in batch_dml.iter().zip(&commits) {
            assert_eq!(request.statements.len(), 1);
            assert_eq!(commit.mutations.len(), 2, "{commit:?}");
        }

        // The applied work is gone: committing again sends nothing.
        connection.commit().unwrap();
        assert_eq!(server.recorded.commits.lock().unwrap().len(), 2);
        assert_eq!(query(&mut connection, "SELECT c FROM t").unwrap(), 1);
    }

    /// A failed commit keeps the buffer, as documented. Rollback discards it, and the next
    /// transaction starts empty.
    #[test]
    fn rollback_after_a_failed_commit_discards_the_buffer() {
        let server = start_with(Box::new(|index| {
            if index == 0 {
                CommitAction::Fail(tonic::Code::FailedPrecondition)
            } else {
                CommitAction::Ok
            }
        }));
        let mut connection = manual(&server);
        update(&mut connection, "UPDATE t SET c = 'a' WHERE TRUE");
        let error = connection.commit().unwrap_err();
        assert_eq!(error.status, Status::InvalidState, "{error}");
        let error = query(&mut connection, "SELECT c FROM t").unwrap_err();
        assert_eq!(
            error.status,
            Status::InvalidState,
            "the buffer is kept: {error}"
        );
        connection.rollback().unwrap();

        update(&mut connection, "UPDATE t SET c = 'b' WHERE TRUE");
        connection.commit().unwrap();
        let batch_dml = server.recorded.batch_dml.lock().unwrap();
        let sqls: Vec<_> = batch_dml
            .iter()
            .flat_map(|request| request.statements.iter().map(|s| s.sql.as_str()))
            .collect();
        assert_eq!(
            sqls,
            [
                "UPDATE t SET c = 'a' WHERE TRUE",
                "UPDATE t SET c = 'b' WHERE TRUE"
            ]
        );
    }

    /// Cancelling a commit blocked in the RPC returns promptly and leaves the connection usable;
    /// the stale cancel does not reach the next commit.
    #[test]
    fn cancel_during_commit_leaves_the_connection_usable() {
        let server = start_with(Box::new(|index| {
            if index == 0 {
                CommitAction::Hang
            } else {
                CommitAction::Ok
            }
        }));
        let mut connection = manual(&server);
        update(&mut connection, "UPDATE t SET c = 'a' WHERE TRUE");
        let cancel = connection.get_cancel_handle();
        let canceller = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(300));
            cancel.try_cancel().unwrap();
        });
        let started = Instant::now();
        let error = connection.commit().unwrap_err();
        canceller.join().unwrap();
        assert_eq!(error.status, Status::Cancelled, "{error}");
        assert!(
            error.message.contains("may still have been applied"),
            "{error}"
        );
        assert!(started.elapsed() < Duration::from_secs(10));

        // Mockall serializes calls to one expectation, so free the hung handler.
        server.gate.store(true, Ordering::SeqCst);

        connection.rollback().unwrap();
        update(&mut connection, "UPDATE t SET c = 'b' WHERE TRUE");
        connection
            .commit()
            .expect("the next commit starts uncancelled");
        assert_eq!(server.recorded.commits.lock().unwrap().len(), 2);
    }
}

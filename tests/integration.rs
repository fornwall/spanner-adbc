//! End-to-end tests against the Spanner emulator, Spanner Omni or a Cloud Spanner database.
//! Tests skip unless a target is configured; see `docs/testing.md`.
//!
//! Setup uses the Spanner admin clients directly; everything else goes through the driver.

use std::cell::RefCell;
use std::sync::{Arc, Mutex, OnceLock};

use adbc_core::error::Status;
use adbc_core::options::{
    AdbcVersion, InfoCode, ObjectDepth, OptionConnection, OptionDatabase, OptionStatement,
    OptionValue,
};
use adbc_core::{Connection, Driver, Optionable, Statement};
use adbc_driver_manager::ManagedDriver;
use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int16Array, Int64Array, ListArray, RecordBatch, RecordBatchIterator, RecordBatchReader,
    StringArray, StructArray, TimestampMicrosecondArray, TimestampNanosecondArray, UInt32Array,
    UnionArray,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use chrono::{NaiveDate, SecondsFormat};
use google_cloud_lro::Poller;
use proptest::prelude::*;
use spanner_adbc::{SpannerConnection, SpannerDatabase, SpannerDriver, SpannerStatement};

mod common;
use common::{
    TestTarget, cdylib_path, connect_with_retry, ensure_database_once, gcp_target, omni,
    test_target,
};

/// Tables that several tests share. Setup creates them because the emulator rejects schema
/// changes while any read-write transaction is in flight, which would make parallel tests flaky.
const SETUP_DDL: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS Singers (SingerId INT64 NOT NULL, Name STRING(MAX), \
     Active BOOL, Score FLOAT64) PRIMARY KEY (SingerId)",
    "CREATE TABLE IF NOT EXISTS AdbcPropBind (Id INT64, IntCol INT64, FloatCol FLOAT64, \
     BoolCol BOOL, StrCol STRING(MAX), BytesCol BYTES(MAX), DateCol DATE, TsCol TIMESTAMP, \
     NumCol NUMERIC) PRIMARY KEY (Id)",
    "CREATE TABLE IF NOT EXISTS AdbcPropTypes (Id INT64, D DATE, T TIMESTAMP, N NUMERIC) \
     PRIMARY KEY (Id)",
    "CREATE TABLE IF NOT EXISTS AdbcReturn (Id INT64, Name STRING(MAX)) PRIMARY KEY (Id)",
    "CREATE TABLE IF NOT EXISTS AdbcHint (Id INT64, N INT64) PRIMARY KEY (Id)",
];

/// Configure the database through a `spanner:` URI and run a query. On the emulator the URI
/// carries a percent-encoded `spanner.endpoint` to exercise decoding.
#[test]
fn connect_via_connection_uri() {
    let Some(target) = test_target() else {
        eprintln!("no Spanner target configured; skipping Spanner integration test");
        return;
    };

    ensure_database_once(&target, SETUP_DDL);

    let uri = if target.is_emulator {
        let host = std::env::var("SPANNER_EMULATOR_HOST").unwrap();
        format!(
            "spanner:///{}?spanner.endpoint=http%3A%2F%2F{host}&spanner.emulator=true",
            target.database_path()
        )
    } else {
        target.database_uri()
    };

    let mut driver = SpannerDriver::try_new().expect("create driver");
    let database = driver
        .new_database_with_opts([(OptionDatabase::Uri, OptionValue::String(uri.clone()))])
        .expect("create database from connection URI");

    // `uri` reads back verbatim and its query parameters read back as individual options.
    assert_eq!(
        database.get_option_string(OptionDatabase::Uri).unwrap(),
        uri
    );
    if target.is_emulator {
        let host = std::env::var("SPANNER_EMULATOR_HOST").unwrap();
        assert_eq!(
            database
                .get_option_string(OptionDatabase::Other("spanner.endpoint".into()))
                .unwrap(),
            format!("http://{host}")
        );
        assert_eq!(
            database
                .get_option_string(OptionDatabase::Other("spanner.emulator".into()))
                .unwrap(),
            "true"
        );
    } else if let Some(endpoint) = &target.omni_endpoint {
        assert_eq!(
            database
                .get_option_string(OptionDatabase::Other(
                    spanner_adbc::OPTION_INSTANCE_TYPE.into()
                ))
                .unwrap(),
            "omni"
        );
        assert_eq!(
            database
                .get_option_string(OptionDatabase::Other(spanner_adbc::OPTION_ENDPOINT.into()))
                .unwrap(),
            *endpoint
        );
    }

    let mut connection = connect_with_retry(&database);
    let mut statement = connection.new_statement().expect("new statement");
    statement
        .set_sql_query("SELECT 1 AS one")
        .expect("set query");
    let batches: Vec<_> = statement
        .execute()
        .expect("execute over URI-configured connection")
        .collect::<Result<Vec<_>, _>>()
        .expect("read batches");
    assert_eq!(batches.len(), 1);
    let ones = col::<Int64Array>(batches[0].column(0));
    assert_eq!(ones.value(0), 1);
}

/// A manual transaction that starts with a query pins one read-only snapshot and rejects DML.
/// DDL runs immediately in any transaction state, ahead of buffered DML, and rollback does not
/// undo it.
#[test]
fn manual_transaction_kinds_round_trip() {
    let Some(mut fx) = fixture() else {
        return;
    };
    let database = fx.database;
    let connection = &mut fx.connection;
    // Autocommit connection acting as a concurrent writer.
    let mut writer = connect_with_retry(database);

    run(
        connection,
        "DROP INDEX IF EXISTS AdbcDdlBatchIdx; \
         DROP TABLE IF EXISTS AdbcDdlBatch; \
         DROP TABLE IF EXISTS AdbcDdlTxn; \
         DROP TABLE IF EXISTS AdbcReadTxn; \
         CREATE TABLE AdbcReadTxn (Id INT64) PRIMARY KEY (Id)",
    );
    let autocommit_insert = |connection: &mut SpannerConnection, id: i64| {
        let mut s = connection.new_statement().expect("new statement");
        s.set_sql_query(format!("INSERT INTO AdbcReadTxn (Id) VALUES ({id})"))
            .unwrap();
        assert_eq!(s.execute_update().expect("autocommit insert"), Some(1));
    };
    autocommit_insert(&mut writer, 1);

    set_autocommit(connection, false);

    // The first query pins the snapshot; a later commit elsewhere stays invisible.
    assert_eq!(count_rows(connection, "AdbcReadTxn"), 1);
    autocommit_insert(&mut writer, 2);
    assert_eq!(
        count_rows(connection, "AdbcReadTxn"),
        1,
        "a query transaction's snapshot must not move mid-transaction"
    );
    // A later failed query must not drop the begun transaction and its snapshot.
    let mut typo = connection.new_statement().expect("new statement");
    typo.set_sql_query("SELECT * FROM AdbcReadTxnMissing")
        .unwrap();
    assert!(
        typo.execute().is_err(),
        "a query on a missing table must fail"
    );
    assert_eq!(
        count_rows(connection, "AdbcReadTxn"),
        1,
        "a failed query must keep the transaction's snapshot"
    );

    let mut insert = connection.new_statement().expect("new statement");
    insert
        .set_sql_query("INSERT INTO AdbcReadTxn (Id) VALUES (99)")
        .unwrap();
    let Err(error) = insert.execute_update() else {
        panic!("DML in a manual transaction that began with a query must be rejected");
    };
    assert_eq!(error.status, Status::InvalidState);
    assert!(
        error.message.contains("began with a query"),
        "the rejection should name the active kind: {:?}",
        error.message
    );

    let mut txn_ddl = connection.new_statement().expect("new statement");
    txn_ddl
        .set_sql_query("CREATE TABLE AdbcDdlTxn (Id INT64) PRIMARY KEY (Id)")
        .unwrap();
    assert_eq!(
        txn_ddl.execute_update().expect("immediate DDL"),
        None,
        "DDL reports no count"
    );
    assert_eq!(
        count_rows(&mut writer, "AdbcDdlTxn"),
        0,
        "DDL must apply immediately, visible to other connections before any commit"
    );
    assert_eq!(
        count_rows(connection, "AdbcReadTxn"),
        1,
        "immediate DDL must not disturb the query transaction's pinned snapshot"
    );

    connection.commit().expect("commit a query transaction");
    assert_eq!(
        count_rows_ending_txn(connection, "AdbcReadTxn"),
        2,
        "a fresh transaction must see the row committed mid-way through the previous one"
    );

    assert_eq!(count_rows(connection, "AdbcReadTxn"), 2);
    autocommit_insert(&mut writer, 3);
    assert_eq!(
        count_rows(connection, "AdbcReadTxn"),
        2,
        "still pinned to the snapshot"
    );
    connection
        .rollback()
        .expect("roll back a query transaction");
    assert_eq!(
        count_rows_ending_txn(connection, "AdbcReadTxn"),
        3,
        "rollback must end the snapshot"
    );

    // DDL while DML is buffered.
    let mut pending = connection.new_statement().expect("new statement");
    pending
        .set_sql_query("INSERT INTO AdbcReadTxn (Id) VALUES (100)")
        .unwrap();
    assert_eq!(pending.execute_update().expect("buffered insert"), None);

    let mut ddl_batch = connection.new_statement().expect("new statement");
    ddl_batch
        .set_sql_query(
            "CREATE TABLE AdbcDdlBatch (Id INT64) PRIMARY KEY (Id); \
             CREATE INDEX AdbcDdlBatchIdx ON AdbcDdlBatch (Id)",
        )
        .unwrap();
    assert_eq!(
        ddl_batch
            .execute_update()
            .expect("DDL while DML is buffered runs immediately"),
        None
    );
    let mut index_q = writer.new_statement().expect("new statement");
    index_q
        .set_sql_query(
            "SELECT COUNT(*) AS n FROM INFORMATION_SCHEMA.INDEXES \
             WHERE TABLE_NAME = 'AdbcDdlBatch' AND INDEX_NAME = 'AdbcDdlBatchIdx'",
        )
        .unwrap();
    let batches: Vec<_> = index_q
        .execute()
        .expect("index metadata query")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        col::<Int64Array>(batches[0].column(0)).value(0),
        1,
        "the `;`-batch must have applied table and index together, before any commit"
    );

    // The DML transaction is still open, and rollback discards only the buffered insert.
    assert_query_guarded(connection, "AdbcReadTxn");
    connection
        .rollback()
        .expect("roll back the buffered insert");
    assert_eq!(
        count_rows_ending_txn(connection, "AdbcReadTxn"),
        3,
        "rollback must discard the buffered DML"
    );
    assert_eq!(
        count_rows(&mut writer, "AdbcDdlBatch"),
        0,
        "rollback must not undo immediately-applied DDL"
    );

    set_autocommit(connection, true);
    let mut cleanup = connection.new_statement().expect("new statement");
    cleanup
        .set_sql_query(
            "DROP INDEX AdbcDdlBatchIdx; DROP TABLE AdbcDdlBatch; \
             DROP TABLE AdbcDdlTxn; DROP TABLE AdbcReadTxn",
        )
        .unwrap();
    cleanup.execute_update().expect("drop scratch tables");
}

fn ingest_label_rows() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("Id", DataType::Int64, false),
            Field::new("Label", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![10, 20])),
            Arc::new(StringArray::from(vec!["x", "y"])),
        ],
    )
    .unwrap()
}

fn ingest_labels(
    connection: &mut SpannerConnection,
    table: &str,
    mode: &str,
) -> adbc_core::error::Result<Option<i64>> {
    let mut statement = ingest_stmt(connection, table, mode);
    statement
        .bind(ingest_label_rows())
        .expect("bind ingest rows");
    statement.execute_update()
}

/// Basic query and DML round trip with Arrow schema checks.
#[test]
fn query_and_dml_round_trip() {
    let Some(mut fx) = fixture() else {
        return;
    };
    let connection = &mut fx.connection;

    let table_types = connection.get_table_types().expect("get_table_types");
    assert_eq!(table_types.schema().field(0).name(), "table_type");

    run(connection, "DELETE FROM Singers WHERE true");

    let mut insert = connection.new_statement().expect("new statement");
    insert
        .set_sql_query(
            "INSERT INTO Singers (SingerId, Name, Active, Score) \
             VALUES (1, 'Alice', true, 4.5), (2, 'Bob', false, 3.25)",
        )
        .unwrap();
    assert_eq!(insert.execute_update().expect("insert"), Some(2));

    let mut query = connection.new_statement().expect("new statement");
    query
        .set_sql_query("SELECT SingerId, Name, Active, Score FROM Singers ORDER BY SingerId")
        .unwrap();
    let reader = query.execute().expect("query");
    let schema = reader.schema();
    assert_eq!(schema.field(0).name(), "SingerId");
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
    assert_eq!(schema.field(2).data_type(), &DataType::Boolean);
    assert_eq!(schema.field(3).data_type(), &DataType::Float64);

    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("collect batches");
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2, "expected two rows back");

    let batch = &batches[0];
    let ids = col::<Int64Array>(batch.column(0));
    let names = col::<StringArray>(batch.column(1));
    let active = col::<BooleanArray>(batch.column(2));
    let score = col::<Float64Array>(batch.column(3));

    assert_eq!(
        (
            ids.value(0),
            names.value(0),
            active.value(0),
            score.value(0)
        ),
        (1, "Alice", true, 4.5)
    );
    assert_eq!(
        (
            ids.value(1),
            names.value(1),
            active.value(1),
            score.value(1)
        ),
        (2, "Bob", false, 3.25)
    );

    // Autocommit DML is sent with `last_statements=true`; the count and commit must be unaffected.
    let mut update = connection.new_statement().expect("new statement");
    update
        .set_sql_query("UPDATE Singers SET Score = 9.5 WHERE SingerId = 1")
        .unwrap();
    assert_eq!(
        update.execute_update().expect("single-statement update"),
        Some(1),
        "a single-statement autocommit UPDATE must still report its affected-row count"
    );

    let mut check = connection.new_statement().expect("new statement");
    check
        .set_sql_query("SELECT Score FROM Singers WHERE SingerId = 1")
        .unwrap();
    let check_batches = check
        .execute()
        .expect("read back updated row")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect updated row");
    let updated_score = col::<Float64Array>(check_batches[0].column(0));
    assert_eq!(
        updated_score.value(0),
        9.5,
        "the last_statement-optimized UPDATE must be durably committed"
    );
}

/// `get_table_schema` reports column types, accepts the database id as catalog and rejects
/// hostile table names.
#[test]
fn get_table_schema_reports_column_types() {
    let Some(mut fx) = fixture_unguarded() else {
        return;
    };
    let catalog = fx.target.database.clone();
    let connection = &mut fx.connection;

    let singers_schema = connection
        .get_table_schema(None, None, "Singers")
        .expect("get_table_schema");
    assert_eq!(singers_schema.fields().len(), 4);
    assert_eq!(singers_schema.field(0).name(), "SingerId");
    assert_eq!(singers_schema.field(0).data_type(), &DataType::Int64);
    assert_eq!(singers_schema.field(1).data_type(), &DataType::Utf8);
    assert_eq!(singers_schema.field(2).data_type(), &DataType::Boolean);
    assert_eq!(singers_schema.field(3).data_type(), &DataType::Float64);

    // Backticks cannot be escaped for every Spanner parser, so such names are rejected.
    let hostile = connection
        .get_table_schema(None, None, "no`such`table")
        .expect_err("hostile table name must not resolve");
    assert_eq!(hostile.status, Status::InvalidArguments);
    let absent = connection
        .get_table_schema(None, None, "NoSuchTable")
        .expect_err("an absent table must not resolve");
    assert_eq!(absent.status, Status::NotFound);

    // The only catalog is the database id.
    let own_catalog = connection
        .get_table_schema(Some(&catalog), None, "Singers")
        .expect("get_table_schema with the connection's own catalog");
    assert_eq!(own_catalog, singers_schema);
    let bogus_catalog = connection
        .get_table_schema(Some("nosuchcatalog"), None, "Singers")
        .expect_err("a named catalog does not exist in Spanner");
    assert_eq!(bogus_catalog.status, Status::NotFound);
}

/// A `;`-separated DDL batch reports no count, and the new table is usable right away.
#[test]
fn ddl_batch_creates_an_immediately_usable_table() {
    let Some(mut fx) = fixture() else {
        return;
    };
    let connection = &mut fx.connection;

    let mut ddl = connection.new_statement().expect("new statement");
    ddl.set_sql_query(
        "DROP TABLE IF EXISTS AdbcDdl; \
         CREATE TABLE AdbcDdl (Id INT64, Note STRING(MAX)) PRIMARY KEY (Id)",
    )
    .unwrap();
    assert_eq!(ddl.execute_update().expect("ddl"), None);

    let mut ins = connection.new_statement().expect("new statement");
    ins.set_sql_query("INSERT INTO AdbcDdl (Id, Note) VALUES (7, 'hello')")
        .unwrap();
    assert_eq!(
        ins.execute_update().expect("insert into ddl table"),
        Some(1)
    );

    let mut check = connection.new_statement().expect("new statement");
    check
        .set_sql_query("SELECT Note FROM AdbcDdl WHERE Id = 7")
        .unwrap();
    let ddl_rows: Vec<_> = check
        .execute()
        .expect("select from ddl table")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(ddl_rows.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    let ddl_note = col::<StringArray>(ddl_rows[0].column(0));
    assert_eq!(ddl_note.value(0), "hello");

    drop_tables(connection, &["AdbcDdl"]);
}

/// Manual transactions buffer plain and parameterized DML until commit, rollback discards it, and
/// re-enabling autocommit commits pending work.
#[test]
fn manual_transaction_buffers_commits_and_rolls_back() {
    let Some(mut fx) = fixture() else {
        return;
    };
    let connection = &mut fx.connection;

    run(
        connection,
        "DROP TABLE IF EXISTS AdbcTxn; CREATE TABLE AdbcTxn (Id INT64) PRIMARY KEY (Id)",
    );

    set_autocommit(connection, false);

    // Buffered DML returns None because the count is unknown until commit.
    for id in [1, 2] {
        let mut s = connection.new_statement().expect("new statement");
        s.set_sql_query(format!("INSERT INTO AdbcTxn (Id) VALUES ({id})"))
            .unwrap();
        assert_eq!(s.execute_update().expect("buffered insert"), None);
    }
    // Queries cannot read buffered writes, so they are rejected.
    assert_query_guarded(connection, "AdbcTxn");

    connection.commit().expect("commit");
    assert_eq!(
        count_rows_ending_txn(connection, "AdbcTxn"),
        2,
        "rows must be visible after commit"
    );

    let mut rolled = connection.new_statement().expect("new statement");
    rolled
        .set_sql_query("INSERT INTO AdbcTxn (Id) VALUES (3)")
        .unwrap();
    assert_eq!(rolled.execute_update().expect("buffered insert"), None);
    connection.rollback().expect("rollback");
    assert_eq!(
        count_rows_ending_txn(connection, "AdbcTxn"),
        2,
        "rolled-back row must not appear"
    );

    let buffer_param_insert = |connection: &mut SpannerConnection, id: i64| {
        let row = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("Id", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![id]))],
        )
        .unwrap();
        let mut s = connection.new_statement().expect("new statement");
        s.set_sql_query("INSERT INTO AdbcTxn (Id) VALUES (@Id)")
            .unwrap();
        s.bind(row).expect("bind param");
        assert_eq!(
            s.execute_update().expect("buffered param insert"),
            None,
            "parameterized DML in manual mode must buffer (return None), not commit immediately"
        );
    };

    buffer_param_insert(connection, 3);
    assert_query_guarded(connection, "AdbcTxn");
    connection.commit().expect("commit param");
    assert_eq!(
        count_rows_ending_txn(connection, "AdbcTxn"),
        3,
        "parameterized row must be visible after commit"
    );

    buffer_param_insert(connection, 4);
    connection.rollback().expect("rollback param");
    assert_eq!(
        count_rows_ending_txn(connection, "AdbcTxn"),
        3,
        "rolled-back parameterized row must not appear"
    );

    // Re-enabling autocommit commits pending work instead of discarding it.
    let mut pending = connection.new_statement().expect("new statement");
    pending
        .set_sql_query("INSERT INTO AdbcTxn (Id) VALUES (5)")
        .unwrap();
    assert_eq!(pending.execute_update().expect("buffered insert"), None);
    assert_query_guarded(connection, "AdbcTxn");
    set_autocommit(connection, true);
    assert_eq!(
        count_rows(connection, "AdbcTxn"),
        4,
        "re-enabling autocommit must commit the buffered DML, not discard it"
    );

    // Setting autocommit again is a no-op.
    set_autocommit(connection, true);
    let mut immediate = connection.new_statement().expect("new statement");
    immediate
        .set_sql_query("INSERT INTO AdbcTxn (Id) VALUES (6)")
        .unwrap();
    assert_eq!(
        immediate.execute_update().expect("autocommit insert"),
        Some(1),
        "back in autocommit mode DML reports its count and applies immediately"
    );
    assert_eq!(count_rows(connection, "AdbcTxn"), 5);
    assert_eq!(
        connection
            .commit()
            .expect_err("commit without an active manual transaction must fail")
            .status,
        Status::InvalidState
    );

    drop_tables(connection, &["AdbcTxn"]);
}

/// A failed commit keeps buffered DML so the caller can retry or roll back. Enabling autocommit
/// is an implicit commit, so it must fail too and leave the connection in manual mode.
#[test]
fn failed_commit_keeps_the_transaction_replayable() {
    let Some(mut fx) = fixture() else {
        return;
    };
    let connection = &mut fx.connection;

    run(
        connection,
        "DROP TABLE IF EXISTS AdbcTxn; CREATE TABLE AdbcTxn (Id INT64) PRIMARY KEY (Id)",
    );
    let mut seed = connection.new_statement().expect("new statement");
    seed.set_sql_query("INSERT INTO AdbcTxn (Id) VALUES (1), (2), (3), (5), (6)")
        .unwrap();
    assert_eq!(
        seed.execute_update().expect("seed the transaction table"),
        Some(5)
    );

    // DML against an unknown table buffers without an RPC and fails at commit.
    set_autocommit(connection, false);
    buffer_dml(connection, "INSERT INTO AdbcTxn (Id) VALUES (7)");
    buffer_dml(connection, "INSERT INTO AdbcTxnNoSuchTable (Id) VALUES (7)");
    assert!(
        connection.commit().is_err(),
        "committing a batch with an unknown table must fail"
    );
    // The batch is still buffered. Re-committing `VALUES (7)` after the rollback below would hit
    // a duplicate key if the failed batch had partially applied.
    assert_query_guarded(connection, "AdbcTxn");
    assert!(
        connection.commit().is_err(),
        "a retried failed commit must replay the buffer and fail again, not report success \
         on an emptied buffer"
    );
    assert!(
        connection
            .set_option(
                OptionConnection::AutoCommit,
                OptionValue::String("true".into()),
            )
            .is_err(),
        "enabling autocommit must fail while the buffered batch cannot commit"
    );
    assert_eq!(
        connection
            .get_option_string(OptionConnection::AutoCommit)
            .expect("get autocommit"),
        "false",
        "a failed implicit commit must not flip the connection into autocommit mode"
    );
    connection.rollback().expect("rollback failed batch");
    buffer_dml(connection, "INSERT INTO AdbcTxn (Id) VALUES (7)");
    connection.commit().expect("commit after rollback");
    assert_eq!(
        count_rows(connection, "AdbcTxn"),
        6,
        "the replacement batch must commit normally after the failed one was rolled back"
    );
    set_autocommit(connection, true);

    drop_tables(connection, &["AdbcTxn"]);
}

/// Bulk ingest in a manual transaction buffers mutations that commit together with buffered DML
/// and are discarded by rollback.
#[test]
fn manual_transaction_commits_buffered_ingest_with_dml() {
    let Some(mut fx) = fixture() else {
        return;
    };
    let connection = &mut fx.connection;

    run(
        connection,
        "DROP TABLE IF EXISTS AdbcTxn; CREATE TABLE AdbcTxn (Id INT64) PRIMARY KEY (Id)",
    );
    let mut seed = connection.new_statement().expect("new statement");
    seed.set_sql_query("INSERT INTO AdbcTxn (Id) VALUES (1), (2), (3), (5), (6), (7)")
        .unwrap();
    assert_eq!(
        seed.execute_update().expect("seed the transaction table"),
        Some(6)
    );

    set_autocommit(connection, false);
    let buffer_ingest = |connection: &mut SpannerConnection, ids: &[i64]| {
        let rows = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("Id", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(ids.to_vec()))],
        )
        .unwrap();
        let mut s = ingest_stmt(connection, "AdbcTxn", "append");
        s.bind(rows).expect("bind manual-mode ingest rows");
        assert_eq!(
            s.execute_update().expect("buffered ingest"),
            None,
            "a manual-mode ingest must buffer its mutations (return None), not commit immediately"
        );
    };
    buffer_ingest(connection, &[100, 101]);
    buffer_dml(connection, "INSERT INTO AdbcTxn (Id) VALUES (102)");
    assert_query_guarded(connection, "AdbcTxn");
    connection.commit().expect("commit ingest + DML");
    assert_eq!(
        count_rows_ending_txn(connection, "AdbcTxn"),
        9,
        "commit must apply the buffered DML and the buffered ingest mutations atomically"
    );
    buffer_ingest(connection, &[103]);
    connection.rollback().expect("rollback buffered ingest");
    assert_eq!(
        count_rows(connection, "AdbcTxn"),
        9,
        "rolled-back ingest mutations must not appear"
    );
    set_autocommit(connection, true);

    drop_tables(connection, &["AdbcTxn"]);
}

/// The ingest mode defaults to `create`, so a second ingest into the same table fails with
/// `AlreadyExists`.
#[test]
fn bulk_ingest_defaults_to_create_mode() {
    let Some(mut fx) = fixture() else {
        return;
    };
    let connection = &mut fx.connection;

    run(connection, "DROP TABLE IF EXISTS AdbcDefaultMode");
    let default_mode_rows = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("Id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![7, 8]))],
    )
    .unwrap();
    let mut ingest_default = connection.new_statement().expect("new statement");
    ingest_default
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::String("AdbcDefaultMode".into()),
        )
        .unwrap();
    assert_eq!(
        ingest_default
            .get_option_string(OptionStatement::IngestMode)
            .unwrap(),
        "adbc.ingest.mode.create"
    );
    ingest_default
        .bind(default_mode_rows)
        .expect("bind default-mode ingest rows");
    assert_eq!(
        ingest_default
            .execute_update()
            .expect("default-mode ingest"),
        Some(2)
    );
    assert_eq!(count_rows(connection, "AdbcDefaultMode"), 2);
    let dup_rows = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("Id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![9]))],
    )
    .unwrap();
    let mut ingest_dup = connection.new_statement().expect("new statement");
    ingest_dup
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::String("AdbcDefaultMode".into()),
        )
        .unwrap();
    ingest_dup
        .bind(dup_rows)
        .expect("bind duplicate ingest rows");
    let dup_err = ingest_dup
        .execute_update()
        .expect_err("create onto an existing table must fail");
    assert_eq!(
        dup_err.status,
        Status::AlreadyExists,
        "default (create) ingest onto an existing table must be AlreadyExists, got: {dup_err:?}"
    );
    drop_tables(connection, &["AdbcDefaultMode"]);
}

/// Spanner resolves table names case-insensitively, so a create-mode ingest into a differently
/// cased name of an existing table is `AlreadyExists`, not the raw duplicate-name DDL error.
#[test]
fn ingest_create_onto_a_differently_cased_table_is_already_exists() {
    let Some(mut fx) = fixture() else {
        return;
    };
    let connection = &mut fx.connection;

    run(
        connection,
        "DROP TABLE IF EXISTS AdbcCaseProbe; \
         CREATE TABLE AdbcCaseProbe (Id INT64, Label STRING(MAX)) PRIMARY KEY (Id)",
    );
    let error = ingest_labels(connection, "adbccaseprobe", "create")
        .expect_err("create-mode ingest onto a differently cased existing table must fail");
    assert_eq!(error.status, Status::AlreadyExists, "{error:?}");
    assert!(
        error.message.contains("adbccaseprobe"),
        "the error must name the target table: {error:?}"
    );
    assert_eq!(count_rows(connection, "AdbcCaseProbe"), 0);

    drop_tables(connection, &["AdbcCaseProbe"]);
}

/// A failed `append` reports `NotFound` for a missing table and `AlreadyExists` for an
/// incompatible schema, as ADBC requires. The driver probes `INFORMATION_SCHEMA` to tell which.
#[test]
fn bulk_ingest_append_failures_remap_to_the_adbc_contract() {
    let Some(mut fx) = fixture() else {
        return;
    };
    let connection = &mut fx.connection;

    run(
        connection,
        "DROP TABLE IF EXISTS AdbcBind; \
         CREATE TABLE AdbcBind (Id INT64, Name STRING(MAX)) PRIMARY KEY (Id)",
    );
    let mut bind_seed = connection.new_statement().expect("new statement");
    bind_seed
        .set_sql_query("INSERT INTO AdbcBind (Id, Name) VALUES (1, 'Alice'), (2, 'Bob')")
        .unwrap();
    assert_eq!(
        bind_seed.execute_update().expect("seed the bind table"),
        Some(2)
    );

    let missing_rows = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("Id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![1]))],
    )
    .unwrap();
    let mut ingest_missing = ingest_stmt(connection, "AdbcNoSuchIngestTable", "append");
    ingest_missing
        .bind(missing_rows)
        .expect("bind rows for missing-table ingest");
    let missing_err = ingest_missing
        .execute_update()
        .expect_err("append onto a missing table must fail");
    assert_eq!(
        missing_err.status,
        Status::NotFound,
        "append onto a missing table must be NotFound, got: {missing_err:?}"
    );

    let mismatch_rows = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "NoSuchColumn",
            DataType::Int64,
            false,
        )])),
        vec![Arc::new(Int64Array::from(vec![1]))],
    )
    .unwrap();
    let mut ingest_mismatch = ingest_stmt(connection, "AdbcBind", "append");
    ingest_mismatch
        .bind(mismatch_rows)
        .expect("bind rows for schema-mismatch ingest");
    let mismatch_err = ingest_mismatch
        .execute_update()
        .expect_err("append with an incompatible schema must fail");
    assert_eq!(
        mismatch_err.status,
        Status::AlreadyExists,
        "append with an incompatible schema must be AlreadyExists, got: {mismatch_err:?}"
    );
    assert_eq!(count_rows(connection, "AdbcBind"), 2);

    drop_tables(connection, &["AdbcBind"]);
}

/// DML and bulk ingest through `execute()`, and statement reuse as the Python DBAPI does it.
/// Language bindings reach the driver through `ExecuteQuery`, not `execute_update`.
#[test]
fn execute_entry_point_and_statement_handle_reuse() {
    let Some(mut fx) = fixture() else {
        return;
    };
    let connection = &mut fx.connection;

    run(
        connection,
        "DROP TABLE IF EXISTS AdbcBind; \
         CREATE TABLE AdbcBind (Id INT64, Name STRING(MAX)) PRIMARY KEY (Id)",
    );
    let mut bind_seed = connection.new_statement().expect("new statement");
    bind_seed
        .set_sql_query("INSERT INTO AdbcBind (Id, Name) VALUES (1, 'Alice'), (2, 'Bob')")
        .unwrap();
    assert_eq!(
        bind_seed.execute_update().expect("seed the bind table"),
        Some(2)
    );

    // DML through `execute()` must use a read/write transaction.
    let mut dml_via_execute = connection.new_statement().expect("new statement");
    dml_via_execute
        .set_sql_query("INSERT INTO AdbcBind (Id, Name) VALUES (3, 'Carol')")
        .unwrap();
    let dml_rows = dml_via_execute
        .execute()
        .expect("DML via execute()")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(
        dml_rows.iter().all(|b| b.num_rows() == 0),
        "DML via execute() must yield an empty result set"
    );
    assert_eq!(count_rows(connection, "AdbcBind"), 3);

    // Bound data is consumed by the execute that uses it; replaying it would run a follow-up
    // query once per bound row.
    run(
        connection,
        "DROP TABLE IF EXISTS AdbcReuse; \
         CREATE TABLE AdbcReuse (Id INT64) PRIMARY KEY (Id)",
    );
    let reuse_rows = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("Id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![10, 20, 30]))],
    )
    .unwrap();
    let mut reuse = ingest_stmt(connection, "AdbcReuse", "append");
    reuse.bind(reuse_rows).expect("bind reuse rows");
    assert_eq!(reuse.execute_update().expect("ingest reuse"), Some(3));
    reuse
        .set_sql_query("SELECT Id FROM AdbcReuse ORDER BY Id")
        .unwrap();
    let reuse_out = reuse
        .execute()
        .expect("reuse query")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        reuse_out.iter().map(|b| b.num_rows()).sum::<usize>(),
        3,
        "stale bound rows must not replay the follow-up query"
    );
    drop_tables(connection, &["AdbcReuse"]);

    // Ingest through `execute()` returns an empty stream.
    let mut ingest_via_execute = ingest_stmt(connection, "AdbcBind", "append");
    let ingest_via_execute_rows = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("Id", DataType::Int64, false),
            Field::new("Name", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![4])),
            Arc::new(StringArray::from(vec!["Dave"])),
        ],
    )
    .unwrap();
    ingest_via_execute
        .bind(ingest_via_execute_rows)
        .expect("bind rows for ingest via execute");
    let ingest_stream = ingest_via_execute
        .execute()
        .expect("ingest via execute()")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(
        ingest_stream.iter().all(|b| b.num_rows() == 0),
        "ingest via execute() must yield an empty result set"
    );
    assert_eq!(count_rows(connection, "AdbcBind"), 4);

    // A DBAPI cursor sets an ingest target without clearing its previous query; the target wins.
    let mut reuse_query_then_ingest = connection.new_statement().expect("new statement");
    reuse_query_then_ingest
        .set_sql_query("SELECT 1")
        .expect("set stale query");
    reuse_query_then_ingest
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::String("AdbcBind".into()),
        )
        .unwrap();
    reuse_query_then_ingest
        .set_option(
            OptionStatement::IngestMode,
            OptionValue::String("append".into()),
        )
        .unwrap();
    let reuse_rows = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("Id", DataType::Int64, false),
            Field::new("Name", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![5])),
            Arc::new(StringArray::from(vec!["Erin"])),
        ],
    )
    .unwrap();
    reuse_query_then_ingest
        .bind(reuse_rows)
        .expect("bind rows for reuse ingest");
    reuse_query_then_ingest
        .execute_update()
        .expect("ingest after a prior query on a reused statement");
    assert_eq!(count_rows(connection, "AdbcBind"), 5);

    drop_tables(connection, &["AdbcBind"]);
}

/// `create`, `append` and `replace` ingest modes. Created tables have no primary key, so Spanner
/// adds a hidden key column; it and its constraints must not appear in results or metadata.
#[test]
fn ingest_create_modes_and_the_implicit_hidden_key() {
    let Some(mut fx) = fixture() else {
        return;
    };
    let connection = &mut fx.connection;

    run(connection, "DROP TABLE IF EXISTS AdbcCreate");
    assert_eq!(
        ingest_labels(connection, "AdbcCreate", "create").expect("create"),
        Some(2)
    );
    assert_eq!(count_rows(connection, "AdbcCreate"), 2);
    assert_eq!(
        ingest_labels(connection, "AdbcCreate", "append").expect("append"),
        Some(2)
    );
    assert_eq!(count_rows(connection, "AdbcCreate"), 4);
    assert_eq!(
        ingest_labels(connection, "AdbcCreate", "replace").expect("replace"),
        Some(2)
    );
    assert_eq!(count_rows(connection, "AdbcCreate"), 2);
    let mut read_create = connection.new_statement().expect("new statement");
    read_create
        .set_sql_query("SELECT * FROM AdbcCreate ORDER BY Id")
        .unwrap();
    let created_reader = read_create.execute().expect("read created");
    let created_cols: Vec<String> = created_reader
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(
        created_cols,
        vec!["Id", "Label"],
        "an ingest-created table must expose exactly the ingested columns: {created_cols:?}"
    );
    let created = created_reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(created.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
    let created_schema = connection
        .get_table_schema(None, None, "AdbcCreate")
        .expect("get_table_schema for the ingest-created table");
    assert_eq!(
        created_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>(),
        vec!["Id", "Label"]
    );
    let (objects_columns, objects_constraints) =
        table_columns_and_constraints(connection, "AdbcCreate");
    assert_eq!(
        objects_columns,
        vec!["Id", "Label"],
        "get_objects must not list the implicit hidden key column"
    );
    // Neither the implicit key nor the checks Spanner keeps for the non-nullable ingested
    // fields are listed.
    assert!(
        objects_constraints.is_empty(),
        "get_objects must list no constraint the user didn't declare: {objects_constraints:?}"
    );
    let created_ids = col::<Int64Array>(created[0].column(0));
    let created_labels = col::<StringArray>(created[0].column(1));
    assert_eq!(
        (0..created[0].num_rows())
            .map(|i| (created_ids.value(i), created_labels.value(i)))
            .collect::<Vec<_>>(),
        vec![(10, "x"), (20, "y")],
        "replace-mode ingest must leave exactly the replacement rows"
    );
    let create_on_existing = ingest_labels(connection, "AdbcCreate", "create")
        .expect_err("create-mode ingest onto an existing table must fail");
    assert_eq!(
        create_on_existing.status,
        Status::AlreadyExists,
        "create onto an existing table must be AlreadyExists, got: {create_on_existing:?}"
    );
    assert!(
        create_on_existing.message.contains("AdbcCreate"),
        "the error must name the target table: {create_on_existing:?}"
    );
    assert_eq!(
        count_rows(connection, "AdbcCreate"),
        2,
        "a failed create-mode ingest must leave the table unchanged \
         (got error: {create_on_existing:?})"
    );
    drop_tables(connection, &["AdbcCreate"]);
}

/// `create_append` creates a missing table, appends to an existing one, and reports
/// `AlreadyExists` when the existing table's schema does not match.
#[test]
fn ingest_create_append_mode_creates_then_appends() {
    let Some(mut fx) = fixture() else {
        return;
    };
    let connection = &mut fx.connection;

    run(connection, "DROP TABLE IF EXISTS AdbcCreateAppend");
    assert_eq!(
        ingest_labels(connection, "AdbcCreateAppend", "create_append").expect("create_append"),
        Some(2)
    );
    assert_eq!(count_rows(connection, "AdbcCreateAppend"), 2);
    assert_eq!(
        ingest_labels(connection, "AdbcCreateAppend", "create_append")
            .expect("create_append onto an existing table appends"),
        Some(2)
    );
    assert_eq!(count_rows(connection, "AdbcCreateAppend"), 4);
    let create_append_mismatch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "NoSuchColumn",
            DataType::Int64,
            false,
        )])),
        vec![Arc::new(Int64Array::from(vec![1]))],
    )
    .unwrap();
    let mut ingest_ca_mismatch = ingest_stmt(connection, "AdbcCreateAppend", "create_append");
    ingest_ca_mismatch
        .bind(create_append_mismatch)
        .expect("bind rows for create_append schema-mismatch ingest");
    let ca_mismatch_err = ingest_ca_mismatch
        .execute_update()
        .expect_err("create_append with an incompatible schema must fail");
    assert_eq!(
        ca_mismatch_err.status,
        Status::AlreadyExists,
        "create_append with an incompatible schema must be AlreadyExists, got: {ca_mismatch_err:?}"
    );
    assert_eq!(count_rows(connection, "AdbcCreateAppend"), 4);
    let mut read_create_append = connection.new_statement().expect("new statement");
    read_create_append
        .set_sql_query("SELECT Id, Label FROM AdbcCreateAppend ORDER BY Id")
        .unwrap();
    let create_appended = read_create_append
        .execute()
        .expect("read create_append")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        create_appended.iter().map(|b| b.num_rows()).sum::<usize>(),
        4
    );
    drop_tables(connection, &["AdbcCreateAppend"]);
}

/// Append ingest respects a table's user-defined primary key: duplicate keys fail with
/// `Integrity`. Keys are declared in DDL; `spanner.ingest.primary_key` is unsupported.
#[test]
fn ingest_appends_into_a_user_keyed_table() {
    let Some(mut fx) = fixture() else {
        return;
    };
    let connection = &mut fx.connection;

    run(
        connection,
        "DROP TABLE IF EXISTS AdbcIngestPk; \
         CREATE TABLE AdbcIngestPk (Id INT64, Label STRING(MAX)) PRIMARY KEY (Id)",
    );
    let keyed_ingest =
        |connection: &mut SpannerConnection| ingest_labels(connection, "AdbcIngestPk", "append");
    assert_eq!(
        keyed_ingest(connection).expect("append into the keyed table"),
        Some(2)
    );
    assert_eq!(count_rows(connection, "AdbcIngestPk"), 2);
    let dup_err = keyed_ingest(connection).expect_err("appending duplicate primary keys must fail");
    assert_eq!(
        dup_err.status,
        Status::Integrity,
        "duplicate primary key must be Integrity, got: {dup_err:?}"
    );
    drop_tables(connection, &["AdbcIngestPk"]);
    let mut gone = connection.new_statement().expect("new statement");
    let gone_err = gone
        .set_option(
            OptionStatement::Other("spanner.ingest.primary_key".into()),
            OptionValue::String("Id".into()),
        )
        .expect_err("an unsupported ingest primary-key option must be rejected");
    assert_eq!(
        gone_err.status,
        Status::NotImplemented,
        "an unsupported option must report NotImplemented, got: {gone_err:?}"
    );
}

/// Constraint violations are `Integrity` errors from DML, a manual commit and ingest. Spanner
/// reports most of them with codes it also uses for unrelated failures, so this checks the
/// driver's message matching against the backend's wording.
#[test]
fn constraint_violations_are_integrity_errors() {
    let Some(mut fx) = fixture() else {
        return;
    };
    let connection = &mut fx.connection;

    run(
        connection,
        "DROP TABLE IF EXISTS AdbcIntegrityChild; \
         DROP TABLE IF EXISTS AdbcIntegrityRef; \
         DROP INDEX IF EXISTS AdbcIntegrityByName; \
         DROP TABLE IF EXISTS AdbcIntegrity",
    );
    run(
        connection,
        "CREATE TABLE AdbcIntegrity (Id INT64 NOT NULL, Name STRING(MAX) NOT NULL, Age INT64, \
         CONSTRAINT AdbcIntegrityAge CHECK (Age >= 0)) PRIMARY KEY (Id); \
         CREATE UNIQUE INDEX AdbcIntegrityByName ON AdbcIntegrity (Name); \
         CREATE TABLE AdbcIntegrityRef (Id INT64 NOT NULL, ParentId INT64, \
         CONSTRAINT AdbcIntegrityFk FOREIGN KEY (ParentId) REFERENCES AdbcIntegrity (Id)) \
         PRIMARY KEY (Id); \
         CREATE TABLE AdbcIntegrityChild (Id INT64 NOT NULL, Sub INT64 NOT NULL) \
         PRIMARY KEY (Id, Sub), INTERLEAVE IN PARENT AdbcIntegrity ON DELETE CASCADE",
    );
    run(
        connection,
        "INSERT INTO AdbcIntegrity (Id, Name, Age) VALUES (1, 'a', 1)",
    );
    run(
        connection,
        "INSERT INTO AdbcIntegrityRef (Id, ParentId) VALUES (1, 1)",
    );

    for sql in [
        "INSERT INTO AdbcIntegrity (Id, Name, Age) VALUES (1, 'b', 1)",
        "INSERT INTO AdbcIntegrity (Id, Name, Age) VALUES (2, 'a', 1)",
        "INSERT INTO AdbcIntegrity (Id, Name, Age) VALUES (3, NULL, 1)",
        "INSERT INTO AdbcIntegrity (Id, Age) VALUES (4, 1)",
        "UPDATE AdbcIntegrity SET Name = NULL WHERE Id = 1",
        "INSERT INTO AdbcIntegrity (Id, Name, Age) VALUES (5, 'c', -1)",
        "INSERT INTO AdbcIntegrityRef (Id, ParentId) VALUES (2, 99)",
        "DELETE FROM AdbcIntegrity WHERE Id = 1",
        "INSERT INTO AdbcIntegrityChild (Id, Sub) VALUES (99, 1)",
    ] {
        let mut statement = connection.new_statement().expect("new statement");
        statement.set_sql_query(sql).unwrap();
        let error = statement.execute_update().expect_err(sql);
        assert_eq!(error.status, Status::Integrity, "{sql}: {error}");
    }

    set_autocommit(connection, false);
    run(
        connection,
        "INSERT INTO AdbcIntegrity (Id, Name, Age) VALUES (1, 'd', 1)",
    );
    let error = connection
        .commit()
        .expect_err("the buffered duplicate key fails at commit");
    assert_eq!(error.status, Status::Integrity, "{error}");
    connection.rollback().unwrap();
    set_autocommit(connection, true);

    let rows = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("Id", DataType::Int64, false),
            Field::new("Name", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![6])),
            Arc::new(StringArray::from(vec![None::<&str>])),
        ],
    )
    .unwrap();
    let mut ingest = ingest_stmt(connection, "AdbcIntegrity", "append");
    ingest.bind(rows).unwrap();
    let error = ingest
        .execute_update()
        .expect_err("a NULL in a NOT NULL column fails the ingest");
    assert_eq!(error.status, Status::Integrity, "{error}");
    assert!(error.message.contains("AdbcIntegrity"), "{error}");

    run(connection, "DROP INDEX AdbcIntegrityByName");
    drop_tables(
        connection,
        &["AdbcIntegrityChild", "AdbcIntegrityRef", "AdbcIntegrity"],
    );
}

/// Parameter binding for queries and DML: parameter schemas, positional and by-name binding, and
/// `Null`-typed bind columns.
#[test]
fn parameter_binding_round_trip() {
    let Some(mut fx) = fixture() else {
        return;
    };
    let connection = &mut fx.connection;

    run(
        connection,
        "DROP TABLE IF EXISTS AdbcBind; \
         CREATE TABLE AdbcBind (Id INT64, Name STRING(MAX)) PRIMARY KEY (Id)",
    );
    let mut bind_seed = connection.new_statement().expect("new statement");
    bind_seed
        .set_sql_query("INSERT INTO AdbcBind (Id, Name) VALUES (1, 'Alice'), (2, 'Bob')")
        .unwrap();
    assert_eq!(
        bind_seed.execute_update().expect("seed the bind table"),
        Some(2)
    );

    let param = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("Id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![2]))],
    )
    .unwrap();
    let mut pq = connection.new_statement().expect("new statement");
    pq.set_sql_query("SELECT Name FROM AdbcBind WHERE Id = @Id")
        .unwrap();
    // Before binding, parameter types come from planning the query.
    let ps = pq
        .get_parameter_schema()
        .expect("parameter schema from SQL");
    assert_eq!(ps.fields().len(), 1);
    assert_eq!(ps.field(0).name(), "Id");
    assert_eq!(ps.field(0).data_type(), &DataType::Int64);
    pq.bind(param).expect("bind query param");
    let ps = pq
        .get_parameter_schema()
        .expect("parameter schema from bound data");
    assert_eq!(ps.field(0).name(), "Id");
    assert_eq!(ps.field(0).data_type(), &DataType::Int64);
    let pq_batches = pq
        .execute()
        .expect("param query")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(pq_batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    assert_eq!(col::<StringArray>(pq_batches[0].column(0)).value(0), "Bob");

    // Columns bind by position by default, whatever their names.
    let positional = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )])),
        vec![Arc::new(Int64Array::from(vec![2]))],
    )
    .unwrap();
    let mut pp = connection.new_statement().expect("new statement");
    pp.set_sql_query("SELECT Name FROM AdbcBind WHERE Id = @p1")
        .unwrap();
    pp.bind(positional).expect("bind positional param");
    let pp_batches = pp
        .execute()
        .expect("positional query")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(pp_batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    assert_eq!(col::<StringArray>(pp_batches[0].column(0)).value(0), "Bob");

    // The bound columns are in a different order than the SQL parameters, so bind by name.
    let upd = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("Id", DataType::Int64, false),
            Field::new("Name", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["Alicia"])),
        ],
    )
    .unwrap();
    let mut pu = connection.new_statement().expect("new statement");
    pu.set_option(
        OptionStatement::Other(spanner_adbc::OPTION_BIND_BY_NAME.into()),
        OptionValue::String("true".into()),
    )
    .expect("set bind_by_name=true");
    pu.set_sql_query("UPDATE AdbcBind SET Name = @Name WHERE Id = @Id")
        .unwrap();
    // DML parameters are reported in SQL order.
    let ps = pu
        .get_parameter_schema()
        .expect("parameter schema from DML");
    assert_eq!(ps.fields().len(), 2);
    assert_eq!(ps.field(0).name(), "Name");
    assert_eq!(ps.field(0).data_type(), &DataType::Utf8);
    assert_eq!(ps.field(1).name(), "Id");
    assert_eq!(ps.field(1).data_type(), &DataType::Int64);
    pu.bind(upd).expect("bind update params");
    assert_eq!(pu.execute_update().expect("param update"), Some(1));

    // A `Null`-typed column binds NULL. pyarrow infers `Null` for all-None parameters, and
    // `get_parameter_schema` reports it for undetermined ones.
    let null_row = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("0", DataType::Int64, false),
            Field::new("1", DataType::Null, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![7001])),
            Arc::new(arrow_array::NullArray::new(1)),
        ],
    )
    .unwrap();
    let mut pn = connection.new_statement().expect("new statement");
    pn.set_sql_query("INSERT INTO AdbcBind (Id, Name) VALUES (@p1, @p2)")
        .unwrap();
    pn.bind(null_row).expect("bind null-typed params");
    assert_eq!(pn.execute_update().expect("null-typed insert"), Some(1));
    let mut pn_check = connection.new_statement().expect("new statement");
    pn_check
        .set_sql_query("SELECT Name FROM AdbcBind WHERE Id = 7001")
        .unwrap();
    let pn_batches = pn_check
        .execute()
        .expect("read back null-typed insert")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(pn_batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    assert!(
        pn_batches[0].column(0).is_null(0),
        "the null-typed bind column must have inserted NULL"
    );

    drop_tables(connection, &["AdbcBind"]);
}

#[test]
fn standalone_parameters_preserve_arrow_types_and_values() {
    let Some(mut fx) = fixture_unguarded() else {
        return;
    };
    let mut lists =
        arrow_array::builder::ListBuilder::new(arrow_array::builder::StringBuilder::new());
    lists.values().append_value("one");
    lists.values().append_null();
    lists.append(true);
    lists.append(true);
    let dictionary: arrow_array::DictionaryArray<arrow_array::types::Int32Type> =
        [Some("decoded"), None].into_iter().collect();
    let columns: Vec<arrow_array::ArrayRef> = vec![
        Arc::new(StringArray::from(vec![Some("Alice"), None])),
        Arc::new(BinaryArray::from(vec![Some(b"\0\xff".as_slice()), None])),
        Arc::new(Date32Array::from(vec![Some(0), None])),
        Arc::new(
            TimestampNanosecondArray::from(vec![Some(1_234_567_890), None]).with_timezone("UTC"),
        ),
        Arc::new(
            Decimal128Array::from(vec![Some(1_250_000_000), None])
                .with_precision_and_scale(38, 9)
                .unwrap(),
        ),
        Arc::new(BooleanArray::from(vec![Some(true), None])),
        Arc::new(Float32Array::from(vec![Some(1.5), None])),
        Arc::new(Float64Array::from(vec![Some(f64::INFINITY), None])),
        Arc::new(Int64Array::from(vec![Some(42), None])),
        Arc::new(lists.finish()),
        Arc::new(dictionary),
        Arc::new(arrow_array::NullArray::new(2)),
    ];
    let fields = columns
        .iter()
        .enumerate()
        .map(|(i, column)| Field::new(format!("p{i}"), column.data_type().clone(), true))
        .collect::<Vec<_>>();
    let input = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    let mut statement = fx.connection.new_statement().unwrap();
    let mut projections = (0..11)
        .map(|i| format!("@p{i} AS p{i}"))
        .collect::<Vec<_>>();
    projections.push("CAST(@p11 AS STRING) AS p11".to_string());
    statement
        .set_sql_query(format!("SELECT {}", projections.join(", ")))
        .unwrap();
    statement.bind(input.clone()).unwrap();
    let planned = statement.execute_schema().unwrap();
    let reader = statement.execute().unwrap();
    assert_eq!(*reader.schema(), planned);
    let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(batches.len(), 2);
    let decoded = StringArray::from(vec![Some("decoded"), None]);
    for (row, batch) in batches.iter().enumerate() {
        assert_eq!(batch.num_rows(), 1);
        for column in 0..10 {
            assert_eq!(
                batch.column(column).to_data(),
                input.column(column).slice(row, 1).to_data(),
                "row {row}, column {column}"
            );
        }
        assert_eq!(batch.column(10).to_data(), decoded.slice(row, 1).to_data());
        assert_eq!(batch.column(11).data_type(), &DataType::Utf8);
        assert!(batch.column(11).is_null(0));
    }
}

#[test]
fn negative_scale_numeric_parameters_round_trip() {
    let Some(mut fx) = fixture_unguarded() else {
        return;
    };
    for (scale, unscaled, expected) in [
        (-2, Some(99), Some(9_900_000_000_000)),
        (-2, Some(-99), Some(-9_900_000_000_000)),
        (-2, None, None),
        (i8::MIN, Some(0), Some(0)),
    ] {
        let input = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "amount",
                DataType::Decimal128(38, scale),
                true,
            )])),
            vec![Arc::new(
                Decimal128Array::from(vec![unscaled])
                    .with_precision_and_scale(38, scale)
                    .unwrap(),
            )],
        )
        .unwrap();
        let mut statement = fx.connection.new_statement().unwrap();
        statement.set_sql_query("SELECT @amount AS amount").unwrap();
        statement.bind(input).unwrap();
        let batches = statement
            .execute()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(batches.len(), 1);
        let result = col::<Decimal128Array>(batches[0].column(0));
        assert_eq!(result.data_type(), &DataType::Decimal128(38, 9));
        assert_eq!(result.iter().collect::<Vec<_>>(), vec![expected]);
    }
}

#[test]
fn prepare_requires_a_query_or_an_ingest_target() {
    let Some(mut fx) = fixture_unguarded() else {
        return;
    };
    let connection = &mut fx.connection;

    let mut unprepared = connection.new_statement().expect("new statement");
    assert_eq!(
        unprepared.prepare().unwrap_err().status,
        Status::InvalidState,
    );
    // An ingest target needs no query, and the table need not exist yet.
    let mut ingest_prepare = connection.new_statement().expect("new statement");
    ingest_prepare
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::String("AdbcBind".into()),
        )
        .unwrap();
    ingest_prepare.prepare().expect("prepare ingest statement");
}

/// Ingest quotes reserved words used as table (`create`) and column (`index`) names, both in the
/// generated `CREATE TABLE` and in the insert mutations.
#[test]
fn bulk_ingest_quotes_reserved_word_identifiers() {
    let Some(mut fx) = fixture() else {
        return;
    };
    let connection = &mut fx.connection;

    run(connection, "DROP TABLE IF EXISTS `create`");
    let esc_rows = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "index",
            DataType::Int64,
            false,
        )])),
        vec![Arc::new(Int64Array::from(vec![42, -42]))],
    )
    .unwrap();
    let mut esc_ingest = ingest_stmt(connection, "create", "create");
    esc_ingest.bind(esc_rows).expect("bind reserved-word rows");
    assert_eq!(
        esc_ingest.execute_update().expect("ingest reserved"),
        Some(2)
    );
    assert_eq!(count_rows(connection, "`create`"), 2);
    drop_tables(connection, &["`create`"]);
}

#[test]
fn bulk_ingest_replace_rejects_unbindable_schema_before_dropping_table() {
    let Some(mut fx) = fixture() else {
        return;
    };
    let connection = &mut fx.connection;
    let table = "AdbcInvalidReplace";
    let mut ddl = connection.new_statement().unwrap();
    ddl.set_sql_query(format!(
        "DROP TABLE IF EXISTS {table}; CREATE TABLE {table} (Id INT64) PRIMARY KEY (Id)"
    ))
    .unwrap();
    ddl.execute_update().unwrap();
    let mut insert = connection.new_statement().unwrap();
    insert
        .set_sql_query(format!("INSERT INTO {table} (Id) VALUES (42)"))
        .unwrap();
    assert_eq!(insert.execute_update().unwrap(), Some(1));

    for data_type in [
        DataType::UInt64,
        DataType::List(Arc::new(Field::new(
            "item",
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            true,
        ))),
    ] {
        let schema = Arc::new(Schema::new(vec![Field::new("bad", data_type, true)]));
        let mut replace = ingest_stmt(connection, table, "replace");
        replace.bind(RecordBatch::new_empty(schema)).unwrap();
        let error = replace.execute_update().unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        assert!(
            error.message.starts_with("ingest column \"bad\": "),
            "{error}"
        );
        assert_eq!(count_rows(connection, table), 1);
    }
    drop_tables(connection, &[table]);
}

/// `ARRAY<scalar>` columns map to an Arrow `List` of the element type.
#[test]
fn array_columns_map_to_native_arrow_lists() {
    let Some(mut fx) = fixture() else {
        return;
    };
    let connection = &mut fx.connection;

    run(
        connection,
        "DROP TABLE IF EXISTS AdbcArr; \
         CREATE TABLE AdbcArr (Id INT64, Nums ARRAY<INT64>, Tags ARRAY<STRING(MAX)>) \
         PRIMARY KEY (Id)",
    );

    let mut arr_ins = connection.new_statement().expect("new statement");
    arr_ins
        .set_sql_query("INSERT INTO AdbcArr (Id, Nums, Tags) VALUES (1, [10, 20, 30], ['a', 'b'])")
        .unwrap();
    assert_eq!(arr_ins.execute_update().expect("insert arrays"), Some(1));

    let mut arr_q = connection.new_statement().expect("new statement");
    arr_q
        .set_sql_query("SELECT Nums, Tags FROM AdbcArr WHERE Id = 1")
        .unwrap();
    let arr_reader = arr_q.execute().expect("array query");
    let arr_schema = arr_reader.schema();
    assert_eq!(
        arr_schema.field(0).data_type(),
        &DataType::List(Arc::new(Field::new("item", DataType::Int64, true)))
    );
    assert_eq!(
        arr_schema.field(1).data_type(),
        &DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
    );
    let arr_batches = arr_reader
        .collect::<Result<Vec<_>, _>>()
        .expect("collect arrays");
    let ab = &arr_batches[0];
    let nums = col::<ListArray>(ab.column(0));
    let nums0 = nums.value(0);
    let nums0 = col::<Int64Array>(&nums0);
    assert_eq!(nums0.values(), &[10, 20, 30]);
    let tags = col::<ListArray>(ab.column(1));
    let tags0 = tags.value(0);
    let tags0 = col::<StringArray>(&tags0);
    assert_eq!((tags0.value(0), tags0.value(1)), ("a", "b"));

    drop_tables(connection, &["AdbcArr"]);
}

/// `STRUCT` maps to an Arrow `Struct`, including duplicate and unnamed fields.
#[test]
fn struct_values_map_to_native_arrow_structs() {
    let Some(mut fx) = fixture_unguarded() else {
        return;
    };
    let connection = &mut fx.connection;

    // Cloud Spanner only returns structs as `ARRAY(SELECT AS STRUCT ...)`; the emulator also
    // accepts array literals of `STRUCT(...)`. The subquery needs an ORDER BY to be deterministic.
    let mut arr_struct_q = connection.new_statement().expect("new statement");
    arr_struct_q
        .set_sql_query(
            "SELECT ARRAY(\
                 SELECT AS STRUCT a, b FROM (\
                     SELECT 0 AS n, 1 AS a, 'x' AS b \
                     UNION ALL SELECT 1 AS n, 2 AS a, 'y' AS b\
                 ) ORDER BY n\
             ) AS arr",
        )
        .unwrap();
    let arr_struct_reader = arr_struct_q.execute().expect("array-of-struct query");
    match arr_struct_reader.schema().field(0).data_type() {
        DataType::List(field) => match field.data_type() {
            DataType::Struct(fields) => {
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].name(), "a");
                assert_eq!(fields[0].data_type(), &DataType::Int64);
                assert_eq!(fields[1].name(), "b");
                assert_eq!(fields[1].data_type(), &DataType::Utf8);
            }
            other => panic!("expected List<Struct>, got List<{other:?}>"),
        },
        other => panic!("expected List, got {other:?}"),
    }

    let arr_struct_batches = arr_struct_reader
        .collect::<Result<Vec<_>, _>>()
        .expect("collect array-of-struct");
    let list = col::<ListArray>(arr_struct_batches[0].column(0));
    let inner = list.value(0);
    let inner = col::<StructArray>(&inner);
    assert_eq!(inner.len(), 2);
    let a = col::<Int64Array>(inner.column(0));
    let b = col::<StringArray>(inner.column(1));
    assert_eq!((a.value(0), a.value(1)), (1, 2));
    assert_eq!((b.value(0), b.value(1)), ("x", "y"));

    // Struct fields are positional; decoding by name would merge the two `x` fields.
    let mut dup_struct_q = connection.new_statement().expect("new statement");
    dup_struct_q
        .set_sql_query("SELECT ARRAY(SELECT AS STRUCT 1 AS x, 2 AS x, 3) AS arr")
        .unwrap();
    let dup_struct_reader = dup_struct_q
        .execute()
        .expect("duplicate-field struct query");
    match dup_struct_reader.schema().field(0).data_type() {
        DataType::List(field) => match field.data_type() {
            DataType::Struct(fields) => {
                let names: Vec<_> = fields.iter().map(|f| f.name().as_str()).collect();
                assert_eq!(names, ["x", "x", ""]);
            }
            other => panic!("expected List<Struct>, got List<{other:?}>"),
        },
        other => panic!("expected List, got {other:?}"),
    }

    let dup_struct_batches = dup_struct_reader
        .collect::<Result<Vec<_>, _>>()
        .expect("collect duplicate-field struct");
    let dup_list = col::<ListArray>(dup_struct_batches[0].column(0));
    let dup_inner = dup_list.value(0);
    let dup_inner = col::<StructArray>(&dup_inner);
    let dup_col = |i: usize| col::<Int64Array>(dup_inner.column(i)).value(0);
    assert_eq!((dup_col(0), dup_col(1), dup_col(2)), (1, 2, 3));
}

/// `;`-separated DML runs as one `ExecuteBatchDml` and reports the summed row count.
#[test]
fn batched_multi_statement_dml_commits_together() {
    let Some(mut fx) = fixture() else {
        return;
    };
    let connection = &mut fx.connection;

    run(
        connection,
        "DROP TABLE IF EXISTS AdbcBatch; CREATE TABLE AdbcBatch (Id INT64) PRIMARY KEY (Id)",
    );

    let mut seed = connection.new_statement().expect("new statement");
    seed.set_sql_query("INSERT INTO AdbcBatch (Id) VALUES (1)")
        .unwrap();
    assert_eq!(seed.execute_update().expect("seed"), Some(1));

    let mut batch = connection.new_statement().expect("new statement");
    batch
        .set_sql_query(
            "DELETE FROM AdbcBatch WHERE true; INSERT INTO AdbcBatch (Id) VALUES (2), (3)",
        )
        .unwrap();
    assert_eq!(batch.execute_update().expect("batch dml"), Some(3));
    assert_eq!(count_rows(connection, "AdbcBatch"), 2);

    drop_tables(connection, &["AdbcBatch"]);
}

#[test]
fn execute_schema_returns_a_query_schema_without_running_it() {
    let Some(mut fx) = fixture_unguarded() else {
        return;
    };
    let connection = &mut fx.connection;

    let mut schema_stmt = connection.new_statement().expect("new statement");
    schema_stmt
        .set_sql_query("WITH cte AS (SELECT 1 AS a, 'x' AS b) SELECT a, b FROM cte")
        .unwrap();
    let planned = schema_stmt.execute_schema().expect("execute_schema");
    assert_eq!(planned.fields().len(), 2);
    assert_eq!(planned.field(0).name(), "a");
    assert_eq!(planned.field(0).data_type(), &DataType::Int64);
    assert_eq!(planned.field(1).name(), "b");
    assert_eq!(planned.field(1).data_type(), &DataType::Utf8);
}

/// `get_objects` at `All` depth; `xdbc_type_name` carries the Spanner type name.
#[test]
fn get_objects_reports_catalog_schema_table_columns() {
    let Some(mut fx) = fixture_unguarded() else {
        return;
    };
    let connection = &mut fx.connection;

    let objects = connection
        .get_objects(
            ObjectDepth::All,
            None,
            Some(""),
            Some("Singers"),
            None,
            None,
        )
        .expect("get_objects");
    let object_batches = objects
        .collect::<Result<Vec<_>, _>>()
        .expect("collect objects");
    let ob = &object_batches[0];
    assert_eq!(ob.num_rows(), 1, "single catalog");

    let schemas = col::<ListArray>(ob.column(1));
    let schemas = schemas.value(0);
    let schemas = col::<StructArray>(&schemas);
    assert_eq!(schemas.len(), 1);

    let tables = col::<ListArray>(schemas.column(1));
    let tables = tables.value(0);
    let tables = col::<StructArray>(&tables);
    assert_eq!(tables.len(), 1);
    let table_name = col::<StringArray>(tables.column(0));
    assert_eq!(table_name.value(0), "Singers");

    let columns = col::<ListArray>(tables.column(2));
    let columns = columns.value(0);
    let columns = col::<StructArray>(&columns);
    assert_eq!(columns.len(), 4);
    let column_name = col::<StringArray>(columns.column(0));
    assert_eq!(column_name.value(0), "SingerId");

    let type_name = col::<StringArray>(
        columns
            .column_by_name("xdbc_type_name")
            .expect("xdbc_type_name field"),
    );
    let type_names: Vec<&str> = (0..type_name.len()).map(|i| type_name.value(i)).collect();
    assert_eq!(type_names, ["INT64", "STRING(MAX)", "BOOL", "FLOAT64"]);

    // The primary key names its column. The check Spanner keeps for `SingerId NOT NULL` is left
    // out.
    let constraints = col::<ListArray>(tables.column(3));
    let constraints = constraints.value(0);
    let constraints = col::<StructArray>(&constraints);
    let constraint_type = col::<StringArray>(constraints.column(1));
    let column_names = col::<ListArray>(constraints.column(2));
    let mut seen: Vec<(String, Vec<String>)> = (0..constraints.len())
        .map(|i| {
            let names = column_names.value(i);
            let names = col::<StringArray>(&names);
            let names = (0..names.len()).map(|j| names.value(j).to_string());
            (constraint_type.value(i).to_string(), names.collect())
        })
        .collect();
    seen.sort();
    assert_eq!(
        seen,
        [("PRIMARY KEY".to_string(), vec!["SingerId".to_string()])]
    );
}

/// `get_objects` depth and filter handling. Levels below the requested depth are NULL; a filter
/// that matches nothing at the requested depth yields an empty list.
#[test]
fn get_objects_depth_and_filter_boundaries() {
    let Some(mut fx) = fixture_unguarded() else {
        return;
    };
    let catalog = fx.target.database.clone();
    let connection = &mut fx.connection;

    let catalogs = connection
        .get_objects(ObjectDepth::Catalogs, None, None, None, None, None)
        .expect("get_objects at Catalogs depth")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect catalogs");
    let cb = &catalogs[0];
    assert_eq!(cb.num_rows(), 1, "single catalog");
    let catalog_name = col::<StringArray>(cb.column(0));
    assert_eq!(
        catalog_name.value(0),
        catalog,
        "the connection's single catalog is the database id"
    );
    let cb_schemas = col::<ListArray>(cb.column(1));
    assert!(
        cb_schemas.is_null(0),
        "catalog_db_schemas must be NULL at Catalogs depth"
    );

    let none = connection
        .get_objects(ObjectDepth::Catalogs, Some("nope"), None, None, None, None)
        .expect("get_objects with excluding catalog filter")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect filtered catalogs");
    assert_eq!(
        none.iter().map(|b| b.num_rows()).sum::<usize>(),
        0,
        "a catalog filter matching no catalog must return nothing"
    );

    let db_schemas = connection
        .get_objects(ObjectDepth::Schemas, None, None, None, None, None)
        .expect("get_objects at Schemas depth")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect db schemas");
    let sb = &db_schemas[0];
    assert_eq!(sb.num_rows(), 1, "single catalog");
    let sb_schemas = col::<ListArray>(sb.column(1));
    assert!(
        sb_schemas.is_valid(0),
        "catalog_db_schemas must be populated at DBSchemas depth"
    );
    let sb_schemas = sb_schemas.value(0);
    let sb_schemas = col::<StructArray>(&sb_schemas);
    let schema_names = col::<StringArray>(sb_schemas.column(0));
    assert!(
        (0..schema_names.len()).any(|i| schema_names.value(i).is_empty()),
        "the default \"\" schema must be reported"
    );
    let schema_tables = col::<ListArray>(sb_schemas.column(1));
    assert!(
        (0..schema_tables.len()).all(|i| schema_tables.is_null(i)),
        "db_schema_tables must be NULL at DBSchemas depth"
    );

    // ADBC says `table_type` filter values come from `get_table_types`.
    let reported_types: Vec<String> = connection
        .get_table_types()
        .expect("get_table_types")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect table types")
        .iter()
        .flat_map(|b| {
            let col = col::<StringArray>(b.column(0));
            (0..col.len())
                .map(|i| col.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    assert!(
        reported_types.contains(&"BASE TABLE".to_string()),
        "get_table_types should report Spanner's INFORMATION_SCHEMA vocabulary: {reported_types:?}"
    );
    let filtered = connection
        .get_objects(
            ObjectDepth::Tables,
            None,
            None,
            Some("Singers"),
            Some(reported_types.iter().map(String::as_str).collect()),
            None,
        )
        .expect("get_objects with table_type filter");
    let filtered = filtered
        .collect::<Result<Vec<_>, _>>()
        .expect("collect filtered objects");
    assert_eq!(
        objects_table_names(&filtered),
        vec!["Singers".to_string()],
        "types reported by get_table_types must round-trip as get_objects filters"
    );

    // A table_type filter matching nothing still returns the catalog and schema rows, with empty
    // (not NULL) table lists.
    let no_such_type = connection
        .get_objects(
            ObjectDepth::Tables,
            None,
            None,
            None,
            Some(vec!["NO SUCH TYPE"]),
            None,
        )
        .expect("get_objects with a table_type filter matching nothing")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect type-filtered objects");
    let nb = &no_such_type[0];
    assert_eq!(nb.num_rows(), 1, "single catalog");
    let nb_schemas = col::<ListArray>(nb.column(1));
    assert!(
        nb_schemas.is_valid(0),
        "catalog_db_schemas must not be NULL at Tables depth"
    );
    let nb_schemas = nb_schemas.value(0);
    let nb_schemas = col::<StructArray>(&nb_schemas);
    assert!(
        !nb_schemas.is_empty(),
        "the db_schema skeleton must survive a table_type filter matching nothing"
    );
    let nb_tables = col::<ListArray>(nb_schemas.column(1));
    for i in 0..nb_tables.len() {
        assert!(
            nb_tables.is_valid(i),
            "db_schema_tables must be EMPTY, not NULL, when the filter matches nothing"
        );
        assert_eq!(nb_tables.value(i).len(), 0, "no tables may match");
    }
}

/// `spanner.commit_stats` exposes the commit's mutation count. The emulator never returns commit
/// statistics, so there the count stays `NotFound`.
#[test]
fn commit_stats_reports_mutation_count() {
    let Some(mut fx) = fixture() else {
        return;
    };
    let target = &fx.target;
    let connection = &mut fx.connection;

    let mut delete = connection.new_statement().expect("new statement");
    delete
        .set_sql_query("DELETE FROM Singers WHERE true")
        .unwrap();
    delete.execute_update().expect("delete");

    let mut insert = connection.new_statement().expect("new statement");
    insert
        .set_option(
            OptionStatement::Other(spanner_adbc::OPTION_COMMIT_STATS.to_string()),
            OptionValue::String("true".to_string()),
        )
        .expect("enable commit stats");
    assert_eq!(
        insert
            .get_option_string(OptionStatement::Other(
                spanner_adbc::OPTION_COMMIT_STATS.to_string()
            ))
            .expect("read commit_stats flag"),
        "true"
    );
    let before = insert
        .get_option_int(OptionStatement::Other(
            spanner_adbc::OPTION_COMMIT_STATS_MUTATION_COUNT.to_string(),
        ))
        .expect_err("mutation count must be NotFound before any commit");
    assert_eq!(before.status, Status::NotFound);

    insert
        .set_sql_query(
            "INSERT INTO Singers (SingerId, Name, Active, Score) \
             VALUES (1, 'Alice', true, 4.5), (2, 'Bob', false, 3.25)",
        )
        .unwrap();
    assert_eq!(insert.execute_update().expect("insert"), Some(2));

    let mutation_count = insert.get_option_int(OptionStatement::Other(
        spanner_adbc::OPTION_COMMIT_STATS_MUTATION_COUNT.to_string(),
    ));
    if target.is_emulator {
        assert_eq!(
            mutation_count
                .expect_err("emulator returns no commit stats")
                .status,
            Status::NotFound
        );
    } else {
        // The exact count is up to Spanner.
        let mutations = mutation_count.expect("mutation count present after a commit with stats");
        assert!(
            mutations > 0,
            "expected a positive mutation count, got {mutations}"
        );
    }

    let rejected = insert.set_option(
        OptionStatement::Other(spanner_adbc::OPTION_COMMIT_STATS_MUTATION_COUNT.to_string()),
        OptionValue::Int(1),
    );
    assert_eq!(
        rejected
            .expect_err("mutation-count key is read-only")
            .status,
        Status::NotImplemented
    );

    run(connection, "DELETE FROM Singers WHERE true");
}

/// An ingest larger than the driver's ~4 MiB per-chunk budget is split into several transactions;
/// the count must sum across chunks and every row must land once.
#[test]
fn bulk_ingest_chunks_past_the_byte_budget() {
    let Some(mut fx) = fixture() else {
        return;
    };
    let connection = &mut fx.connection;

    run(
        connection,
        "DROP TABLE IF EXISTS AdbcChunked; \
         CREATE TABLE AdbcChunked (Id INT64, Name STRING(MAX)) PRIMARY KEY (Id)",
    );

    // Six ~1.1 MB rows give about three rows per chunk, clear of both the budget edge and gRPC's
    // 4 MB message limit.
    const ROWS: usize = 6;
    const VALUE_LEN: usize = 1_100_000;
    let names: Vec<String> = (0..ROWS)
        .map(|i| char::from(b'a' + i as u8).to_string().repeat(VALUE_LEN))
        .collect();
    let rows = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("Id", DataType::Int64, false),
            Field::new("Name", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from((0..ROWS as i64).collect::<Vec<_>>())),
            Arc::new(StringArray::from(
                names.iter().map(String::as_str).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    let mut ingest = ingest_stmt(connection, "AdbcChunked", "append");
    ingest.bind(rows).expect("bind chunked ingest rows");
    assert_eq!(
        ingest.execute_update().expect("chunked ingest"),
        Some(ROWS as i64),
        "the affected-row count must sum across the ingest's chunk transactions"
    );

    let mut read = connection.new_statement().expect("new statement");
    read.set_sql_query(
        "SELECT Id, CHAR_LENGTH(Name) AS Len, SUBSTR(Name, 1, 1) AS Head \
         FROM AdbcChunked ORDER BY Id",
    )
    .unwrap();
    let batches = read
        .execute()
        .expect("read chunked rows")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let mut seen = 0_usize;
    for batch in &batches {
        let ids = col::<Int64Array>(batch.column(0));
        let lens = col::<Int64Array>(batch.column(1));
        let heads = col::<StringArray>(batch.column(2));
        for row in 0..batch.num_rows() {
            assert_eq!(ids.value(row), seen as i64);
            assert_eq!(lens.value(row), VALUE_LEN as i64);
            assert_eq!(heads.value(row), char::from(b'a' + seen as u8).to_string());
            seen += 1;
        }
    }
    assert_eq!(seen, ROWS, "all ingested rows must be readable back");

    drop_tables(connection, &["AdbcChunked"]);
}

/// `spanner.ingest.batch_write` ingests through the BatchWrite RPC with the same row count and
/// `Integrity` mapping as the transactional path.
#[test]
fn bulk_ingest_via_batch_write() {
    let Some(mut fx) = fixture() else {
        return;
    };
    let connection = &mut fx.connection;

    run(
        connection,
        "DROP TABLE IF EXISTS AdbcBatchWrite; \
         CREATE TABLE AdbcBatchWrite (Id INT64, Name STRING(MAX)) PRIMARY KEY (Id)",
    );

    const ROWS: usize = 5;
    let rows = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("Id", DataType::Int64, false),
            Field::new("Name", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from((0..ROWS as i64).collect::<Vec<_>>())),
            Arc::new(StringArray::from(
                (0..ROWS).map(|i| format!("row-{i}")).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();

    let mut ingest = ingest_stmt(connection, "AdbcBatchWrite", "append");
    ingest
        .set_option(
            OptionStatement::Other(spanner_adbc::OPTION_INGEST_BATCH_WRITE.into()),
            OptionValue::String("true".into()),
        )
        .unwrap();
    assert_eq!(
        ingest
            .get_option_string(OptionStatement::Other(
                spanner_adbc::OPTION_INGEST_BATCH_WRITE.into()
            ))
            .unwrap(),
        "true"
    );
    ingest.bind(rows.clone()).expect("bind batch-write rows");
    assert_eq!(
        ingest.execute_update().expect("batch-write ingest"),
        Some(ROWS as i64),
        "BatchWrite ingest must report the exact applied-row count"
    );

    let mut read = connection.new_statement().expect("new statement");
    read.set_sql_query("SELECT Id, Name FROM AdbcBatchWrite ORDER BY Id")
        .unwrap();
    let batches = read
        .execute()
        .expect("read batch-write rows")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let mut seen = 0_usize;
    for batch in &batches {
        let ids = col::<Int64Array>(batch.column(0));
        let names = col::<StringArray>(batch.column(1));
        for row in 0..batch.num_rows() {
            assert_eq!(ids.value(row), seen as i64);
            assert_eq!(names.value(row), format!("row-{seen}"));
            seen += 1;
        }
    }
    assert_eq!(
        seen, ROWS,
        "all BatchWrite-ingested rows must be readable back"
    );

    let mut dup = ingest_stmt(connection, "AdbcBatchWrite", "append");
    dup.set_option(
        OptionStatement::Other(spanner_adbc::OPTION_INGEST_BATCH_WRITE.into()),
        OptionValue::String("true".into()),
    )
    .unwrap();
    dup.bind(rows).expect("bind duplicate batch-write rows");
    let error = dup
        .execute_update()
        .expect_err("a duplicate primary key must fail the BatchWrite ingest");
    assert_eq!(error.status, Status::Integrity, "{error}");
    assert!(
        error.message.contains("AdbcBatchWrite"),
        "the Integrity error should name the target table: {error}"
    );

    drop_tables(connection, &["AdbcBatchWrite"]);
}

/// `spanner.max_timestamp_precision`: out-of-range timestamps error by default and read back in
/// `microseconds` mode, set per connection or statement.
#[test]
fn timestamp_precision_modes_round_trip() {
    let Some(mut fx) = fixture() else {
        return;
    };
    let connection = &mut fx.connection;

    run(
        connection,
        "DROP TABLE IF EXISTS AdbcTsPrecision; \
         CREATE TABLE AdbcTsPrecision (Id INT64, T TIMESTAMP) PRIMARY KEY (Id)",
    );

    // Arrow nanosecond timestamps cover roughly 1677-2262. The extra digits test truncation.
    let mut ins = connection.new_statement().expect("new statement");
    ins.set_sql_query(
        "INSERT INTO AdbcTsPrecision (Id, T) VALUES \
         (1, TIMESTAMP '9999-01-01T00:00:00.123456789Z'), \
         (2, TIMESTAMP '1500-06-15T12:34:56.789012Z'), \
         (3, NULL)",
    )
    .unwrap();
    assert_eq!(ins.execute_update().expect("insert timestamps"), Some(3));

    let micros_ty = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));

    let mut q = connection.new_statement().expect("new statement");
    q.set_sql_query("SELECT T FROM AdbcTsPrecision WHERE Id = 1")
        .unwrap();
    // The error may surface from `execute` or from the first batch.
    let msg = match q.execute() {
        Err(e) => e.message,
        Ok(reader) => match reader.collect::<Result<Vec<_>, _>>() {
            Err(e) => e.to_string(),
            Ok(_) => panic!("year-9999 TIMESTAMP must not decode as nanoseconds"),
        },
    };
    assert!(
        msg.contains("\"T\"")
            && msg.contains("9999-01-01")
            && msg.contains("spanner.max_timestamp_precision"),
        "error should name the column, the value and the escape hatch: {msg}"
    );

    connection
        .set_option(
            OptionConnection::Other("spanner.max_timestamp_precision".into()),
            OptionValue::String("microseconds".into()),
        )
        .expect("set connection timestamp precision");
    assert_eq!(
        connection
            .get_option_string(OptionConnection::Other(
                "spanner.max_timestamp_precision".into()
            ))
            .unwrap(),
        "microseconds"
    );

    let mut q = connection.new_statement().expect("new statement");
    assert_eq!(
        q.get_option_string(OptionStatement::Other(
            "spanner.max_timestamp_precision".into()
        ))
        .unwrap(),
        "microseconds",
        "statement inherits the connection's mode"
    );
    q.set_sql_query("SELECT T FROM AdbcTsPrecision ORDER BY Id")
        .unwrap();

    let planned = q.execute_schema().expect("execute_schema in micros mode");
    assert_eq!(planned.field(0).data_type(), &micros_ty);

    let reader = q.execute().expect("query in micros mode");
    assert_eq!(reader.schema().field(0).data_type(), &micros_ty);
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("collect micros batches");
    let ts = col::<TimestampMicrosecondArray>(batches[0].column(0));
    assert_eq!(ts.value(0), 253_370_764_800_123_456); // .123456789 truncated to .123456
    assert_eq!(ts.value(1), -14_817_468_303_210_988); // 1500-06-15T12:34:56.789012Z
    assert!(ts.is_null(2));

    let table_schema = connection
        .get_table_schema(None, None, "AdbcTsPrecision")
        .expect("get_table_schema");
    assert_eq!(
        table_schema
            .field_with_name("T")
            .expect("T column")
            .data_type(),
        &micros_ty
    );

    // A statement-level `""` resets to the default, overriding the connection.
    let mut q = connection.new_statement().expect("new statement");
    q.set_option(
        OptionStatement::Other("spanner.max_timestamp_precision".into()),
        OptionValue::String(String::new()),
    )
    .expect("reset statement precision");
    assert_eq!(
        q.get_option_string(OptionStatement::Other(
            "spanner.max_timestamp_precision".into()
        ))
        .unwrap(),
        "nanoseconds_error_on_overflow"
    );
    q.set_sql_query("SELECT T FROM AdbcTsPrecision WHERE Id = 1")
        .unwrap();
    let planned = q.execute_schema().expect("execute_schema in default mode");
    assert_eq!(
        planned.field(0).data_type(),
        &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
    );
    let failed = match q.execute() {
        Err(_) => true,
        Ok(reader) => reader.collect::<Result<Vec<_>, _>>().is_err(),
    };
    assert!(failed, "statement-level reset must restore the loud error");

    drop_tables(connection, &["AdbcTsPrecision"]);
}

/// Bulk-ingest edge cases: empty batches and streams, bind/option ordering, missing destination
/// or data, statement reuse across ingest and DML, and duplicate keys.
#[test]
fn bulk_ingest_edge_cases() {
    let Some(mut fx) = fixture() else {
        return;
    };
    let connection = &mut fx.connection;

    run(
        connection,
        "DROP TABLE IF EXISTS AdbcEdge; \
         DROP TABLE IF EXISTS AdbcEdgeCreate; \
         DROP TABLE IF EXISTS AdbcEdgeZero; \
         CREATE TABLE AdbcEdge (Id INT64, Name STRING(MAX)) PRIMARY KEY (Id)",
    );

    let schema = Arc::new(Schema::new(vec![
        Field::new("Id", DataType::Int64, false),
        Field::new("Name", DataType::Utf8, false),
    ]));
    let batch = |ids: &[i64]| {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(StringArray::from(
                    ids.iter().map(|i| format!("name-{i}")).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    };
    let empty = || RecordBatch::new_empty(schema.clone());
    let append_stmt =
        |connection: &mut SpannerConnection| ingest_stmt(connection, "AdbcEdge", "append");

    // Zero-row batches at the start, middle and end of a stream must not truncate the ingest.
    let mut zero_rows = append_stmt(connection);
    zero_rows
        .bind_stream(Box::new(RecordBatchIterator::new(
            [empty(), batch(&[1, 2]), empty(), batch(&[3]), empty()].map(Ok),
            schema.clone(),
        )))
        .expect("bind stream with zero-row batches");
    assert_eq!(
        zero_rows
            .execute_update()
            .expect("ingest around zero-row batches"),
        Some(3),
        "all rows from the non-empty batches must land; empty batches contribute nothing"
    );
    assert_eq!(count_rows(connection, "AdbcEdge"), 3);

    let mut all_empty = append_stmt(connection);
    all_empty
        .bind_stream(Box::new(RecordBatchIterator::new(
            [empty(), empty()].map(Ok),
            schema.clone(),
        )))
        .expect("bind all-empty stream");
    assert_eq!(
        all_empty
            .execute_update()
            .expect("all-empty ingest succeeds"),
        Some(0)
    );
    assert_eq!(count_rows(connection, "AdbcEdge"), 3);

    // The same in a manual transaction.
    set_autocommit(connection, false);
    let mut manual = append_stmt(connection);
    manual
        .bind_stream(Box::new(RecordBatchIterator::new(
            [empty(), batch(&[4]), empty(), batch(&[5]), empty()].map(Ok),
            schema.clone(),
        )))
        .expect("bind manual-mode stream with zero-row batches");
    assert_eq!(
        manual.execute_update().expect("buffered ingest"),
        None,
        "a manual-mode ingest must buffer its mutations"
    );
    connection.commit().expect("commit buffered ingest");
    assert_eq!(
        count_rows(connection, "AdbcEdge"),
        5,
        "every row around the zero-row batches must land on commit"
    );
    set_autocommit(connection, true);

    // In create mode the table schema can come from an empty first batch.
    let mut create = ingest_stmt(connection, "AdbcEdgeCreate", "create");
    create
        .bind_stream(Box::new(RecordBatchIterator::new(
            [empty(), batch(&[1])].map(Ok),
            schema.clone(),
        )))
        .expect("bind create-mode stream with an empty first batch");
    assert_eq!(
        create
            .execute_update()
            .expect("create-mode ingest with an empty first batch"),
        Some(1),
        "the table schema must come from the zero-row first batch"
    );
    drop_tables(connection, &["AdbcEdgeCreate"]);

    // A stream with no batches at all still declares a schema, so ingest succeeds (the C++
    // validation suite's `TestSqlIngestStreamZeroArrays`).
    let empty_stream = || {
        RecordBatchIterator::new(
            Vec::<std::result::Result<RecordBatch, arrow_schema::ArrowError>>::new(),
            schema.clone(),
        )
    };
    let mut create_empty = ingest_stmt(connection, "AdbcEdgeZero", "create");
    create_empty
        .bind_stream(Box::new(empty_stream()))
        .expect("bind a zero-batch stream");
    assert_eq!(
        create_empty
            .execute_update()
            .expect("create-mode ingest of a zero-batch stream must succeed"),
        Some(0),
        "a zero-batch ingest commits zero rows"
    );
    assert_eq!(
        count_rows(connection, "AdbcEdgeZero"),
        0,
        "the table must be created (from the stream's schema) with zero rows"
    );
    let mut append_empty = ingest_stmt(connection, "AdbcEdgeZero", "append");
    append_empty
        .bind_stream(Box::new(empty_stream()))
        .expect("bind a zero-batch stream for append");
    assert_eq!(
        append_empty
            .execute_update()
            .expect("append of a zero-batch stream must succeed"),
        Some(0)
    );
    assert_eq!(count_rows(connection, "AdbcEdgeZero"), 0);
    drop_tables(connection, &["AdbcEdgeZero"]);

    // Data may be bound before the ingest options are set.
    let mut before = connection.new_statement().expect("new statement");
    before.bind(batch(&[6])).expect("bind before options");
    before
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::String("AdbcEdge".into()),
        )
        .unwrap();
    before
        .set_option(
            OptionStatement::IngestMode,
            OptionValue::String("append".into()),
        )
        .unwrap();
    assert_eq!(
        before.execute_update().expect("bind-before-options ingest"),
        Some(1),
        "binding before setting the ingest target must work"
    );
    assert_eq!(count_rows(connection, "AdbcEdge"), 6);

    // Bound data without a target table or query fails on both entry points without consuming
    // the data.
    let mut nowhere = connection.new_statement().expect("new statement");
    nowhere.bind(batch(&[7])).expect("bind without destination");
    let update_err = nowhere
        .execute_update()
        .expect_err("execute_update with bound data but no destination must fail");
    let execute_err = nowhere
        .execute()
        .err()
        .expect("execute with bound data but no destination must fail");
    for error in [update_err, execute_err] {
        assert_eq!(
            error.status,
            Status::InvalidState,
            "bound data without a destination must be InvalidState, got: {error:?}"
        );
        assert!(
            error.message.contains("adbc.ingest.target_table"),
            "the error must name the missing ingest option: {error:?}"
        );
    }
    nowhere
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::String("AdbcEdge".into()),
        )
        .unwrap();
    nowhere
        .set_option(
            OptionStatement::IngestMode,
            OptionValue::String("append".into()),
        )
        .unwrap();
    assert_eq!(
        nowhere
            .execute_update()
            .expect("ingest after supplying the destination"),
        Some(1),
        "the failed no-destination attempts must not have consumed the bound data"
    );
    assert_eq!(count_rows(connection, "AdbcEdge"), 7);

    let mut nodata = append_stmt(connection);
    let error = nodata
        .execute_update()
        .expect_err("ingest with no bound data must fail");
    assert_eq!(
        error.status,
        Status::InvalidState,
        "ingest with no bound data must be InvalidState, got: {error:?}"
    );
    assert!(
        error.message.contains("no data has been bound"),
        "unexpected message: {error:?}"
    );
    let error = nodata
        .execute()
        .err()
        .expect("ingest via execute with no bound data must fail");
    assert_eq!(
        error.status,
        Status::InvalidState,
        "ingest via execute with no bound data must be InvalidState, got: {error:?}"
    );

    // Reuse one handle for ingest, then DML, then ingest; each switch clears the other mode.
    let mut handle = append_stmt(connection);
    handle
        .bind(batch(&[10, 11]))
        .expect("bind first reuse ingest");
    assert_eq!(
        handle.execute_update().expect("first reuse ingest"),
        Some(2)
    );
    handle
        .set_sql_query("INSERT INTO AdbcEdge (Id, Name) VALUES (12, 'dml')")
        .unwrap();
    assert!(
        handle
            .get_option_string(OptionStatement::TargetTable)
            .is_err(),
        "set_sql_query must clear the ingest target"
    );
    assert_eq!(
        handle.execute_update().expect("DML on the reused handle"),
        Some(1)
    );
    handle
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::String("AdbcEdge".into()),
        )
        .unwrap();
    handle
        .set_option(
            OptionStatement::IngestMode,
            OptionValue::String("append".into()),
        )
        .unwrap();
    handle
        .bind(batch(&[13, 14]))
        .expect("bind second reuse ingest");
    assert_eq!(
        handle.execute_update().expect("second reuse ingest"),
        Some(2),
        "the re-set ingest target must win over the stale DML"
    );
    assert_eq!(count_rows(connection, "AdbcEdge"), 12);

    let mut dup = append_stmt(connection);
    dup.bind(batch(&[1])).expect("bind duplicate-key row");
    let error = dup
        .execute_update()
        .expect_err("duplicate-PK ingest must fail");
    assert_eq!(
        error.status,
        Status::Integrity,
        "a duplicate primary key must surface as Integrity, got: {error:?}"
    );
    assert!(
        error.message.contains("AdbcEdge"),
        "the error must name the target table: {error:?}"
    );
    assert!(
        !error.message.contains("incompatible"),
        "a duplicate key must not be misreported as a schema mismatch: {error:?}"
    );
    assert_eq!(
        count_rows(connection, "AdbcEdge"),
        12,
        "the rejected duplicate must not change the table"
    );

    drop_tables(connection, &["AdbcEdge"]);
}

/// A zero-batch `bind_stream` supplies a schema for bulk ingest but no parameter rows. It must
/// leave the parameter buffer empty so subsequent DML and queries execute without bound rows.
#[test]
fn bulk_ingest_empty_stream_does_not_hijack_other_paths() {
    let Some(mut fx) = fixture() else {
        return;
    };
    let connection = &mut fx.connection;

    run(
        connection,
        "DROP TABLE IF EXISTS AdbcEmptyHijack; \
         DROP TABLE IF EXISTS AdbcStaleLeak; \
         DROP TABLE IF EXISTS AdbcEmptyReadTxn; \
         CREATE TABLE AdbcEmptyHijack (Id INT64, Name STRING(MAX)) PRIMARY KEY (Id)",
    );

    let schema = Arc::new(Schema::new(vec![
        Field::new("Id", DataType::Int64, false),
        Field::new("Name", DataType::Utf8, false),
    ]));
    let empty_stream = {
        let schema = schema.clone();
        move || {
            RecordBatchIterator::new(
                Vec::<std::result::Result<RecordBatch, arrow_schema::ArrowError>>::new(),
                schema.clone(),
            )
        }
    };

    let mut seed = connection.new_statement().expect("new statement");
    seed.set_sql_query(
        "INSERT INTO AdbcEmptyHijack (Id, Name) VALUES (1, 'a'), (2, 'b'), (3, 'c')",
    )
    .unwrap();
    assert_eq!(seed.execute_update().expect("seed rows"), Some(3));

    let mut delete_after_empty = connection.new_statement().expect("new statement");
    delete_after_empty
        .bind_stream(Box::new(empty_stream()))
        .expect("bind empty stream before DML");
    delete_after_empty
        .set_sql_query("DELETE FROM AdbcEmptyHijack WHERE Id IN (2, 3)")
        .unwrap();
    assert_eq!(
        delete_after_empty
            .execute_update()
            .expect("DELETE after an empty bind_stream must execute"),
        Some(2),
        "an empty bind_stream must not silently no-op a subsequent DELETE"
    );
    assert_eq!(count_rows(connection, "AdbcEmptyHijack"), 1);

    let mut select_after_empty = connection.new_statement().expect("new statement");
    select_after_empty
        .bind_stream(Box::new(empty_stream()))
        .expect("bind empty stream before query");
    select_after_empty
        .set_sql_query("SELECT Id FROM AdbcEmptyHijack")
        .unwrap();
    let select_rows: usize = select_after_empty
        .execute()
        .expect("SELECT after an empty bind_stream must execute")
        .map(|b| b.expect("batch").num_rows())
        .sum();
    assert_eq!(
        select_rows, 1,
        "an empty bind_stream must not make a subsequent SELECT return zero rows"
    );

    // A bound query with an empty result yields no batches.
    let mut empty_bound_query = connection.new_statement().expect("new statement");
    empty_bound_query
        .set_sql_query("SELECT Id FROM AdbcEmptyHijack WHERE Id = @p")
        .unwrap();
    empty_bound_query
        .bind(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("p", DataType::Int64, false)])),
                vec![Arc::new(Int64Array::from(vec![999_i64]))],
            )
            .unwrap(),
        )
        .expect("bind a param matching no row");
    let bound_query_batches = empty_bound_query
        .execute()
        .expect("bound query with an empty result must execute")
        .collect::<Result<Vec<_>, _>>()
        .expect("read bound-query batches");
    assert_eq!(
        bound_query_batches.len(),
        0,
        "an empty bound-query result must yield zero batches, not a spurious empty batch"
    );

    // With zero bound rows (an empty `executemany`) the query still reports its real schema.
    let param_schema = Arc::new(Schema::new(vec![Field::new("p", DataType::Int64, false)]));
    let mut one_row_bound = connection.new_statement().expect("new statement");
    one_row_bound
        .set_sql_query("SELECT Id FROM AdbcEmptyHijack WHERE Id = @p")
        .unwrap();
    one_row_bound
        .bind(
            RecordBatch::try_new(
                param_schema.clone(),
                vec![Arc::new(Int64Array::from(vec![1_i64]))],
            )
            .unwrap(),
        )
        .expect("bind one param row");
    let non_empty_schema = one_row_bound
        .execute()
        .expect("non-empty bound query")
        .schema();
    let mut zero_row_bound = connection.new_statement().expect("new statement");
    zero_row_bound
        .set_sql_query("SELECT Id FROM AdbcEmptyHijack WHERE Id = @p")
        .unwrap();
    zero_row_bound
        .bind(RecordBatch::new_empty(param_schema.clone()))
        .expect("bind a zero-row parameter set");
    let zero_row_reader = zero_row_bound
        .execute()
        .expect("zero-row bound query must execute");
    assert_eq!(
        zero_row_reader.schema(),
        non_empty_schema,
        "a zero-row bound query must advertise the query's real schema, not an empty one"
    );
    let zero_row_batches = zero_row_reader
        .collect::<Result<Vec<_>, _>>()
        .expect("read zero-row bound-query batches");
    assert_eq!(
        zero_row_batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        0,
        "a zero-row bound query returns no rows"
    );

    // An empty append sends no mutations, so the driver must check that the table exists.
    let mut append_missing_empty = ingest_stmt(connection, "AdbcNoSuchZeroTable", "append");
    append_missing_empty
        .bind_stream(Box::new(empty_stream()))
        .expect("bind empty stream for missing-table append");
    let append_missing_err = append_missing_empty
        .execute_update()
        .expect_err("empty append to a nonexistent table must fail");
    assert_eq!(
        append_missing_err.status,
        Status::NotFound,
        "empty append to a nonexistent table must be NotFound, got: {append_missing_err:?}"
    );

    // Executing DML clears the schema from an earlier empty `bind_stream`, so a later ingest on
    // the same handle fails instead of creating a table from the stale schema.
    let mut reused = connection.new_statement().expect("new statement");
    reused
        .bind_stream(Box::new(empty_stream()))
        .expect("bind empty stream on a reused handle");
    reused
        .set_sql_query("DELETE FROM AdbcEmptyHijack WHERE Id = -1")
        .unwrap();
    reused.execute_update().expect("run a DML on the handle");
    reused
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::String("AdbcStaleLeak".into()),
        )
        .unwrap();
    reused
        .set_option(
            OptionStatement::IngestMode,
            OptionValue::String("create".into()),
        )
        .unwrap();
    let stale_err = reused
        .execute_update()
        .expect_err("an ingest with no bound data (schema was cleared) must fail");
    assert_eq!(
        stale_err.status,
        Status::InvalidState,
        "a stale empty-stream schema must not survive a DML execution: {stale_err:?}"
    );
    // Creating the table now succeeds only if the failed ingest did not create it.
    let mut stale_probe = ingest_stmt(connection, "AdbcStaleLeak", "create");
    stale_probe
        .bind_stream(Box::new(empty_stream()))
        .expect("bind empty stream to confirm the table was not leaked");
    assert_eq!(
        stale_probe
            .execute_update()
            .expect("AdbcStaleLeak must not exist yet, so create must succeed"),
        Some(0),
        "the InvalidState ingest must not have created a table from the stale schema"
    );

    // The same existence check applies in a manual transaction.
    set_autocommit(connection, false);
    let mut manual_append_missing = ingest_stmt(connection, "AdbcNoSuchManualZeroTable", "append");
    manual_append_missing
        .bind_stream(Box::new(empty_stream()))
        .expect("bind empty stream for manual missing-table append");
    let manual_append_err = manual_append_missing
        .execute_update()
        .expect_err("manual-mode empty append to a nonexistent table must fail");
    assert_eq!(
        manual_append_err.status,
        Status::NotFound,
        "manual empty append to a nonexistent table must be NotFound, got: {manual_append_err:?}"
    );
    connection.rollback().expect("end the manual transaction");
    set_autocommit(connection, true);

    // A create-mode ingest in a read-only transaction is rejected before its DDL runs.
    set_autocommit(connection, false);
    let mut fix_read = connection.new_statement().expect("new statement");
    fix_read
        .set_sql_query("SELECT Id FROM AdbcEmptyHijack")
        .unwrap();
    drop(fix_read.execute().expect("open a read transaction"));
    let mut create_in_read = ingest_stmt(connection, "AdbcEmptyReadTxn", "create");
    create_in_read
        .bind_stream(Box::new(empty_stream()))
        .expect("bind empty stream for create in a read txn");
    let create_in_read_err = create_in_read
        .execute_update()
        .expect_err("create-mode ingest in a query transaction must be rejected");
    assert_eq!(
        create_in_read_err.status,
        Status::InvalidState,
        "a DML-kind ingest in a query transaction must be InvalidState, got: {create_in_read_err:?}"
    );
    connection.rollback().expect("end the read transaction");
    set_autocommit(connection, true);
    let mut probe_not_created = ingest_stmt(connection, "AdbcEmptyReadTxn", "create");
    probe_not_created
        .bind_stream(Box::new(empty_stream()))
        .expect("bind empty stream to confirm the table was not created");
    assert_eq!(
        probe_not_created
            .execute_update()
            .expect("the table must not exist yet, so create must succeed"),
        Some(0),
        "the rejected in-txn create must not have left a table behind"
    );

    drop_tables(
        connection,
        &["AdbcEmptyReadTxn", "AdbcStaleLeak", "AdbcEmptyHijack"],
    );
}

/// When a later chunk of a multi-chunk ingest fails, the error reports how many rows earlier
/// chunks already committed.
#[test]
fn bulk_ingest_mid_chunk_failure_reports_committed_rows() {
    let Some(mut fx) = fixture() else {
        return;
    };
    let connection = &mut fx.connection;

    run(
        connection,
        "DROP TABLE IF EXISTS AdbcMidFail; \
         CREATE TABLE AdbcMidFail (Id INT64, Name STRING(MAX)) PRIMARY KEY (Id)",
    );

    // Same shape as `bulk_ingest_chunks_past_the_byte_budget`. The last row duplicates the first
    // row's key, so the first chunk commits and the last one fails wherever the boundaries fall.
    const ROWS: usize = 6;
    const VALUE_LEN: usize = 1_100_000;
    let mut ids: Vec<i64> = (0..ROWS as i64).collect();
    ids[ROWS - 1] = 0;
    let names: Vec<String> = (0..ROWS)
        .map(|i| char::from(b'a' + i as u8).to_string().repeat(VALUE_LEN))
        .collect();
    let rows = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("Id", DataType::Int64, false),
            Field::new("Name", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(
                names.iter().map(String::as_str).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();

    let mut ingest = ingest_stmt(connection, "AdbcMidFail", "append");
    ingest.bind(rows).expect("bind mid-fail ingest rows");
    let error = ingest
        .execute_update()
        .expect_err("the duplicate key in a later chunk must fail the ingest");

    assert_eq!(
        error.status,
        Status::Integrity,
        "a duplicate key in a later chunk must be Integrity, got: {error:?}"
    );
    assert!(
        error.message.contains("AdbcMidFail"),
        "the error must name the target table: {error:?}"
    );

    let committed = count_rows(connection, "AdbcMidFail");
    assert!(
        committed > 0 && committed < ROWS as i64,
        "a mid-ingest failure must leave exactly the earlier chunks' rows, found {committed}"
    );
    assert!(
        error.message.contains(&format!(
            "{committed} row(s) from this bulk ingest were already committed"
        )),
        "the error must report the rows already committed \
         ({committed} found in the table): {error:?}"
    );

    drop_tables(connection, &["AdbcMidFail"]);
}

/// The non-NULL `db_schema_tables` lists in a collected `get_objects` result.
fn objects_tables(batches: &[RecordBatch]) -> Vec<StructArray> {
    let mut out = Vec::new();
    for batch in batches {
        let schema_lists = col::<ListArray>(batch.column(1));
        for c in 0..batch.num_rows() {
            let schemas = schema_lists.value(c);
            let schemas = col::<StructArray>(&schemas);
            let table_lists = col::<ListArray>(schemas.column(1));
            for s in 0..schemas.len() {
                if table_lists.is_valid(s) {
                    out.push(col::<StructArray>(&table_lists.value(s)).clone());
                }
            }
        }
    }
    out
}

/// Table names in a collected `get_objects` result, across all catalogs and schemas.
fn objects_table_names(batches: &[RecordBatch]) -> Vec<String> {
    let mut names = Vec::new();
    for tables in objects_tables(batches) {
        let table_names = col::<StringArray>(tables.column(0));
        names.extend((0..tables.len()).map(|r| table_names.value(r).to_string()));
    }
    names
}

/// Reference matcher for ADBC patterns (`%`, `_`, case-sensitive, no escapes), independent of the
/// driver's implementation.
fn adbc_like(pattern: &str, value: &str) -> bool {
    fn rec(p: &[char], v: &[char]) -> bool {
        match p.split_first() {
            None => v.is_empty(),
            Some((&'%', rest)) => (0..=v.len()).any(|k| rec(rest, &v[k..])),
            Some((&'_', rest)) => !v.is_empty() && rec(rest, &v[1..]),
            Some((c, rest)) => v.first() == Some(c) && rec(rest, &v[1..]),
        }
    }
    let p: Vec<char> = pattern.chars().collect();
    let v: Vec<char> = value.chars().collect();
    rec(&p, &v)
}

/// Column names of `table` in a collected `get_objects` result.
fn objects_column_names(batches: &[RecordBatch], table: &str) -> Vec<String> {
    let mut names = Vec::new();
    for tables in objects_tables(batches) {
        let table_names = col::<StringArray>(tables.column(0));
        let column_lists = col::<ListArray>(tables.column_by_name("table_columns").unwrap());
        for r in 0..tables.len() {
            if table_names.value(r) != table || column_lists.is_null(r) {
                continue;
            }
            let columns = column_lists.value(r);
            let columns = col::<StructArray>(&columns);
            let column_names = col::<StringArray>(columns.column(0));
            names.extend((0..columns.len()).map(|k| column_names.value(k).to_string()));
        }
    }
    names
}

/// `get_objects` pattern filters are pushed into `INFORMATION_SCHEMA` queries; results must match
/// filtering the unfiltered result client-side.
#[test]
fn get_objects_filter_pushdown_matches_client_filtering() {
    let Some(mut fx) = fixture() else {
        return;
    };
    let connection = &mut fx.connection;

    // `GoFilter_Beta` and `GoFilterXBeta` both match the `_` wildcard. `GoFilterChild` has a
    // foreign key to a table outside the child-only filter below.
    run(
        connection,
        "DROP TABLE IF EXISTS GoFilterChild; \
         DROP TABLE IF EXISTS GoFilterAlpha; \
         DROP TABLE IF EXISTS GoFilter_Beta; \
         DROP TABLE IF EXISTS GoFilterXBeta; \
         CREATE TABLE GoFilterAlpha (Id INT64, NameOne STRING(MAX), Name_Two STRING(MAX), \
         NameXTwo STRING(MAX)) PRIMARY KEY (Id); \
         CREATE TABLE GoFilter_Beta (Id INT64) PRIMARY KEY (Id); \
         CREATE TABLE GoFilterXBeta (Id INT64) PRIMARY KEY (Id); \
         CREATE TABLE GoFilterChild (Id INT64, AlphaId INT64, \
         CONSTRAINT FK_GoFilterChild_Alpha FOREIGN KEY (AlphaId) \
         REFERENCES GoFilterAlpha (Id)) PRIMARY KEY (Id)",
    );

    let collect = |connection: &mut SpannerConnection,
                   table_name: Option<&str>,
                   column_name: Option<&str>| {
        connection
            .get_objects(ObjectDepth::All, None, None, table_name, None, column_name)
            .expect("get_objects")
            .collect::<Result<Vec<_>, _>>()
            .expect("collect objects")
    };

    let unfiltered = collect(connection, None, None);
    let mut all_names = objects_table_names(&unfiltered);
    all_names.sort();

    let table_patterns = [
        "GoFilterAlpha", // exact name
        "GoFilter%",     // prefix
        "%Alpha%",       // substring
        "%ilter_B%",     // substring with `_` wildcard
        "GoFilter_Beta", // `_` wildcard: matches GoFilter_Beta AND GoFilterXBeta
        "GoFilterNone%", // matches nothing
    ];
    for pattern in table_patterns {
        let mut filtered = objects_table_names(&collect(connection, Some(pattern), None));
        filtered.sort();
        let expected: Vec<String> = all_names
            .iter()
            .filter(|n| adbc_like(pattern, n))
            .cloned()
            .collect();
        assert_eq!(
            filtered, expected,
            "server-filtered result must equal client-filtered result for {pattern:?}"
        );
    }

    let mut wildcarded = objects_table_names(&collect(connection, Some("GoFilter_Beta"), None));
    wildcarded.sort();
    assert_eq!(wildcarded, ["GoFilterXBeta", "GoFilter_Beta"]);

    let all_columns = objects_column_names(&unfiltered, "GoFilterAlpha");
    assert_eq!(all_columns, ["Id", "NameOne", "Name_Two", "NameXTwo"]);
    for pattern in ["Name%", "Name_Two", "%Two", "Id", "Zzz%"] {
        let filtered = collect(connection, Some("GoFilterAlpha"), Some(pattern));
        let expected: Vec<String> = all_columns
            .iter()
            .filter(|n| adbc_like(pattern, n))
            .cloned()
            .collect();
        assert_eq!(
            objects_column_names(&filtered, "GoFilterAlpha"),
            expected,
            "column filter {pattern:?}"
        );
    }

    // A table filter matching nothing keeps every schema, with an empty table list.
    let none = collect(connection, Some("GoFilterNone%"), None);
    assert!(objects_table_names(&none).is_empty());
    let batch = &none[0];
    let schema_lists = col::<ListArray>(batch.column(1));
    assert!(schema_lists.is_valid(0));
    let schemas = schema_lists.value(0);
    let schemas = col::<StructArray>(&schemas);
    assert!(!schemas.is_empty(), "schema skeletons must be kept");
    let table_lists = col::<ListArray>(schemas.column(1));
    for s in 0..schemas.len() {
        assert!(
            table_lists.is_valid(s) && table_lists.value(s).is_empty(),
            "each kept schema must carry an empty, non-null table list"
        );
    }

    let no_columns = collect(connection, Some("GoFilterAlpha"), Some("Zzz%"));
    assert_eq!(objects_table_names(&no_columns), ["GoFilterAlpha"]);
    assert!(objects_column_names(&no_columns, "GoFilterAlpha").is_empty());

    // The child's foreign key still names the parent's column, although the parent is filtered out.
    let child_only = collect(connection, Some("GoFilterChild"), None);
    assert_eq!(objects_table_names(&child_only), ["GoFilterChild"]);
    let mut fk_usage = None;
    for tables in objects_tables(&child_only) {
        let constraint_lists =
            col::<ListArray>(tables.column_by_name("table_constraints").unwrap());
        for r in 0..tables.len() {
            let constraints = constraint_lists.value(r);
            let constraints = col::<StructArray>(&constraints);
            let ctype = col::<StringArray>(constraints.column_by_name("constraint_type").unwrap());
            let usage_lists = col::<ListArray>(
                constraints
                    .column_by_name("constraint_column_usage")
                    .unwrap(),
            );
            for k in 0..constraints.len() {
                if ctype.value(k) == "FOREIGN KEY" {
                    assert!(usage_lists.is_valid(k), "FK usage list must be non-null");
                    let usage = usage_lists.value(k);
                    let usage = col::<StructArray>(&usage);
                    assert_eq!(usage.len(), 1);
                    let fk_table = col::<StringArray>(usage.column_by_name("fk_table").unwrap());
                    let fk_column =
                        col::<StringArray>(usage.column_by_name("fk_column_name").unwrap());
                    fk_usage = Some((
                        fk_table.value(0).to_string(),
                        fk_column.value(0).to_string(),
                    ));
                }
            }
        }
    }
    assert_eq!(
        fk_usage,
        Some(("GoFilterAlpha".to_string(), "Id".to_string())),
        "the FK must resolve its parent column even though the filter excludes the parent table"
    );

    drop_tables(
        connection,
        &[
            "GoFilterChild",
            "GoFilterAlpha",
            "GoFilter_Beta",
            "GoFilterXBeta",
        ],
    );
}

/// Cases per property; each case is several round trips.
const PROP_CASES: u32 = 64;

/// Fewer cases for [`prop_temporal_round_trip`], whose values [`prop_bind_round_trip`] already
/// covers through bound parameters.
const PROP_TEMPORAL_CASES: u32 = 20;

/// Persisted failure seeds are not useful because reproducing them needs a live backend.
fn prop_config_with(cases: u32) -> ProptestConfig {
    ProptestConfig {
        cases,
        failure_persistence: None,
        ..ProptestConfig::default()
    }
}

fn prop_config() -> ProptestConfig {
    prop_config_with(PROP_CASES)
}

fn run(connection: &mut SpannerConnection, sql: &str) {
    let mut s = connection.new_statement().expect("new statement");
    s.set_sql_query(sql).unwrap();
    s.execute_update()
        .unwrap_or_else(|e| panic!("failed to run {sql:?}: {e:?}"));
}

/// Scalar values bound as parameters read back unchanged, nulls included.
#[test]
fn prop_bind_round_trip() {
    let Some(fx) = fixture() else {
        return;
    };
    let connection = RefCell::new(fx.connection);
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    // NaN and infinities do not compare with `==`.
    let float = any::<f64>().prop_filter("finite", |f| f.is_finite());
    // Up to 24 non-control characters.
    let text = proptest::string::string_regex("\\PC{0,24}").unwrap();
    let bytes = proptest::collection::vec(any::<u8>(), 0..24);
    let date = (1i32..=9999, 1u32..=12, 1u32..=28);
    // Stay within the Arrow nanosecond range (1677-09-21 to 2262-04-11).
    let ts = (
        1678i32..=2261,
        1u32..=12,
        1u32..=28,
        0u32..24,
        0u32..60,
        0u32..60,
        0u32..1_000_000,
    );
    let numeric = (any::<bool>(), 0u128..10u128.pow(28), 0u32..1_000_000_000);

    proptest!(prop_config(), |(
        oi in proptest::option::of(any::<i64>()),
        of in proptest::option::of(float),
        ob in proptest::option::of(any::<bool>()),
        os in proptest::option::of(text),
        oby in proptest::option::of(bytes),
        od in proptest::option::of(date),
        ot in proptest::option::of(ts),
        on in proptest::option::of(numeric),
    )| {
        let mut conn = connection.borrow_mut();

        let exp_days = od.map(|(y, m, d)| {
            (NaiveDate::from_ymd_opt(y, m, d).unwrap() - epoch).num_days() as i32
        });
        let exp_micros = ot.map(|(y, mo, d, h, mi, s, us)| {
            NaiveDate::from_ymd_opt(y, mo, d)
                .unwrap()
                .and_hms_micro_opt(h, mi, s, us)
                .unwrap()
                .and_utc()
                .timestamp_micros()
        });
        let exp_unscaled = on.map(|(neg, int_mag, frac)| {
            let mag = int_mag as i128 * 1_000_000_000 + frac as i128;
            if neg { -mag } else { mag }
        });

        let num_col = Decimal128Array::from(vec![exp_unscaled])
            .with_precision_and_scale(38, 9)
            .unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("Id", DataType::Int64, false),
                Field::new("IntCol", DataType::Int64, true),
                Field::new("FloatCol", DataType::Float64, true),
                Field::new("BoolCol", DataType::Boolean, true),
                Field::new("StrCol", DataType::Utf8, true),
                Field::new("BytesCol", DataType::Binary, true),
                Field::new("DateCol", DataType::Date32, true),
                Field::new(
                    "TsCol",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                ),
                Field::new("NumCol", DataType::Decimal128(38, 9), true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1i64])),
                Arc::new(Int64Array::from(vec![oi])),
                Arc::new(Float64Array::from(vec![of])),
                Arc::new(BooleanArray::from(vec![ob])),
                Arc::new(StringArray::from(vec![os.as_deref()])),
                Arc::new(BinaryArray::from_opt_vec(vec![oby.as_deref()])),
                Arc::new(Date32Array::from(vec![exp_days])),
                Arc::new(TimestampMicrosecondArray::from(vec![exp_micros])),
                Arc::new(num_col),
            ],
        )
        .unwrap();

        run(&mut conn, "DELETE FROM AdbcPropBind WHERE true");
        let mut ins = conn.new_statement().expect("new statement");
        ins.set_sql_query(
            "INSERT INTO AdbcPropBind \
                 (Id, IntCol, FloatCol, BoolCol, StrCol, BytesCol, DateCol, TsCol, NumCol) \
             VALUES (@Id, @IntCol, @FloatCol, @BoolCol, @StrCol, @BytesCol, @DateCol, @TsCol, @NumCol)",
        )
        .unwrap();
        ins.bind(batch).expect("bind row");
        prop_assert_eq!(ins.execute_update().expect("insert"), Some(1));

        let mut q = conn.new_statement().expect("new statement");
        q.set_sql_query(
            "SELECT IntCol, FloatCol, BoolCol, StrCol, BytesCol, DateCol, TsCol, NumCol \
             FROM AdbcPropBind WHERE Id = 1",
        )
        .unwrap();
        let batches = q
            .execute()
            .expect("select")
            .collect::<Result<Vec<_>, _>>()
            .expect("collect");
        prop_assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
        let b = &batches[0];

        let i = col::<Int64Array>(b.column(0));
        let f = col::<Float64Array>(b.column(1));
        let bo = col::<BooleanArray>(b.column(2));
        let s = col::<StringArray>(b.column(3));
        let by = col::<BinaryArray>(b.column(4));
        let d = col::<Date32Array>(b.column(5));
        let t = col::<TimestampNanosecondArray>(b.column(6));
        let n = col::<Decimal128Array>(b.column(7));

        match oi {
            Some(v) => prop_assert_eq!(i.value(0), v),
            None => prop_assert!(i.is_null(0)),
        }
        match of {
            Some(v) => prop_assert_eq!(f.value(0), v),
            None => prop_assert!(f.is_null(0)),
        }
        match ob {
            Some(v) => prop_assert_eq!(bo.value(0), v),
            None => prop_assert!(bo.is_null(0)),
        }
        match &os {
            Some(v) => prop_assert_eq!(s.value(0), v.as_str()),
            None => prop_assert!(s.is_null(0)),
        }
        match &oby {
            Some(v) => prop_assert_eq!(by.value(0), v.as_slice()),
            None => prop_assert!(by.is_null(0)),
        }
        match exp_days {
            Some(v) => prop_assert_eq!(d.value(0), v),
            None => prop_assert!(d.is_null(0)),
        }
        // Bound as microseconds, read back as nanoseconds.
        match exp_micros {
            Some(v) => prop_assert_eq!(t.value(0), v * 1_000),
            None => prop_assert!(t.is_null(0)),
        }
        match exp_unscaled {
            Some(v) => prop_assert_eq!(n.value(0), v),
            None => prop_assert!(n.is_null(0)),
        }
    });
}

/// DATE, TIMESTAMP and NUMERIC values written as SQL literals read back as the expected Arrow
/// values.
#[test]
fn prop_temporal_round_trip() {
    let Some(fx) = fixture() else {
        return;
    };
    let connection = RefCell::new(fx.connection);
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();

    proptest!(prop_config_with(PROP_TEMPORAL_CASES), |(
        od in proptest::option::of((1i32..=9999, 1u32..=12, 1u32..=28)),
        ot in proptest::option::of((
            1678i32..=2261, 1u32..=12, 1u32..=28, 0u32..24, 0u32..60, 0u32..60, 0u32..1_000_000,
        )),
        on in proptest::option::of((any::<bool>(), 0u128..10u128.pow(28), 0u32..1_000_000_000)),
    )| {
        let mut conn = connection.borrow_mut();

        let (d_lit, exp_days) = match od {
            Some((y, m, d)) => {
                let date = NaiveDate::from_ymd_opt(y, m, d).unwrap();
                let days = (date - epoch).num_days() as i32;
                (format!("DATE '{}'", date.format("%Y-%m-%d")), Some(days))
            }
            None => ("NULL".to_string(), None),
        };
        let (t_lit, exp_nanos) = match ot {
            Some((y, mo, d, h, mi, s, us)) => {
                let dt = NaiveDate::from_ymd_opt(y, mo, d)
                    .unwrap()
                    .and_hms_micro_opt(h, mi, s, us)
                    .unwrap()
                    .and_utc();
                (
                    format!("TIMESTAMP '{}'", dt.to_rfc3339_opts(SecondsFormat::Micros, true)),
                    Some(dt.timestamp_nanos_opt().unwrap()),
                )
            }
            None => ("NULL".to_string(), None),
        };
        let (n_lit, exp_unscaled) = match on {
            Some((neg, int_mag, frac)) => {
                let mag = int_mag as i128 * 1_000_000_000 + frac as i128;
                let unscaled = if neg { -mag } else { mag };
                let sign = if neg { "-" } else { "" };
                (format!("NUMERIC '{sign}{int_mag}.{frac:09}'"), Some(unscaled))
            }
            None => ("NULL".to_string(), None),
        };

        run(&mut conn, "DELETE FROM AdbcPropTypes WHERE true");
        run(
            &mut conn,
            &format!(
                "INSERT INTO AdbcPropTypes (Id, D, T, N) VALUES (1, {d_lit}, {t_lit}, {n_lit})"
            ),
        );

        let mut q = conn.new_statement().expect("new statement");
        q.set_sql_query("SELECT D, T, N FROM AdbcPropTypes WHERE Id = 1")
            .unwrap();
        let batches = q
            .execute()
            .expect("select")
            .collect::<Result<Vec<_>, _>>()
            .expect("collect");
        prop_assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
        let b = &batches[0];

        let d = col::<Date32Array>(b.column(0));
        let t = col::<TimestampNanosecondArray>(b.column(1));
        let n = col::<Decimal128Array>(b.column(2));

        match exp_days {
            Some(v) => prop_assert_eq!(d.value(0), v),
            None => prop_assert!(d.is_null(0)),
        }
        match exp_nanos {
            Some(v) => prop_assert_eq!(t.value(0), v),
            None => prop_assert!(t.is_null(0)),
        }
        match exp_unscaled {
            Some(v) => prop_assert_eq!(n.value(0), v),
            None => prop_assert!(n.is_null(0)),
        }
    });
}

/// Bound rows must not survive a DDL statement on a reused statement handle.
#[test]
fn ddl_execute_clears_stale_bound_rows() {
    let Some(mut fx) = fixture() else {
        return;
    };
    let connection = &mut fx.connection;

    run(connection, "DROP TABLE IF EXISTS BoundLeak");

    let mut stmt = connection.new_statement().expect("new statement");
    let bound = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![900001_i64, 900002]))],
    )
    .unwrap();
    stmt.bind(bound).expect("bind two stale @id rows");

    stmt.set_sql_query("CREATE TABLE BoundLeak (id INT64) PRIMARY KEY (id)")
        .unwrap();
    stmt.execute_update()
        .expect("create BoundLeak via the bound handle");

    // The unbound parameter should make this fail; the row count is what matters.
    stmt.set_sql_query("INSERT INTO BoundLeak (id) VALUES (@id)")
        .unwrap();
    let _ = stmt.execute_update();

    assert_eq!(
        count_rows(connection, "BoundLeak"),
        0,
        "stale bound rows leaked past a DDL statement into the next parameterized DML"
    );

    drop_tables(connection, &["BoundLeak"]);
}

/// Metadata, queries, DML, transactions and ingest through the driver manager, checking the
/// canonical ADBC result schemas.
#[test]
fn conformance_via_driver_manager() {
    use adbc_core::schemas::{GET_INFO_SCHEMA, GET_OBJECTS_SCHEMA, GET_TABLE_TYPES_SCHEMA};

    let Some(target) = test_target() else {
        eprintln!("no Spanner target set; skipping conformance_via_driver_manager");
        return;
    };
    let Some(cdylib) = cdylib_path() else {
        eprintln!("shared library not built; skipping conformance test (run `cargo build` first)");
        return;
    };
    ensure_database_once(&target, SETUP_DDL);
    let _guard = serial_guard();

    let mut driver = ManagedDriver::load_dynamic_from_filename(
        &cdylib,
        Some(b"AdbcSpannerInit"),
        AdbcVersion::V110,
    )
    .expect("load driver via the driver manager");
    let database = driver
        .new_database_with_opts([(
            OptionDatabase::Uri,
            OptionValue::String(target.database_uri()),
        )])
        .expect("new_database via FFI");
    let mut connection = connect_with_retry(&database);

    let reader = connection.get_info(None).expect("get_info(None)");
    assert_eq!(reader.schema(), GET_INFO_SCHEMA.clone());
    let all_info = reader.collect::<Result<Vec<_>, _>>().expect("collect info");
    assert!(
        all_info.iter().map(|b| b.num_rows()).sum::<usize>() >= 4,
        "get_info should report at least the core codes"
    );
    let subset = connection
        .get_info(Some([InfoCode::VendorName, InfoCode::DriverName].into()))
        .expect("get_info(subset)");
    assert_eq!(subset.schema(), GET_INFO_SCHEMA.clone());
    let subset_rows: usize = subset
        .collect::<Result<Vec<_>, _>>()
        .expect("collect subset")
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(subset_rows, 2, "one row per explicitly requested code");
    // `adbc.h` says unrecognized codes are omitted, not rejected.
    let unknown = connection
        .get_info(Some(
            [
                InfoCode::Other(500),
                InfoCode::Other(10_042),
                InfoCode::DriverName,
            ]
            .into(),
        ))
        .expect("unrecognized info codes are ignored, not an error");
    assert_eq!(unknown.schema(), GET_INFO_SCHEMA.clone());
    let unknown_batches = unknown
        .collect::<Result<Vec<_>, _>>()
        .expect("collect unknown-code info");
    let unknown_codes: Vec<u32> = unknown_batches
        .iter()
        .flat_map(|b| {
            let col = col::<UInt32Array>(b.column(0));
            (0..col.len()).map(|i| col.value(i)).collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(
        unknown_codes,
        vec![u32::from(&InfoCode::DriverName)],
        "only the recognised code is answered; the unrecognized ones are omitted"
    );

    let reader = connection.get_table_types().expect("get_table_types");
    assert_eq!(reader.schema(), GET_TABLE_TYPES_SCHEMA.clone());
    let types: Vec<String> = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("collect table types")
        .iter()
        .flat_map(|b| {
            let col = col::<StringArray>(b.column(0));
            (0..col.len())
                .map(|i| col.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    assert!(types.contains(&"BASE TABLE".to_string()) && types.contains(&"VIEW".to_string()));

    run_ffi(
        &mut connection,
        "DROP TABLE IF EXISTS AdbcConf; \
         CREATE TABLE AdbcConf (Id INT64, Name STRING(MAX)) PRIMARY KEY (Id)",
    );

    let reader = connection
        .get_objects(ObjectDepth::All, None, None, None, None, None)
        .expect("get_objects");
    assert_eq!(reader.schema(), GET_OBJECTS_SCHEMA.clone());
    let objects = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("collect objects");
    assert_eq!(objects.iter().map(|b| b.num_rows()).sum::<usize>(), 1);

    let schema = connection
        .get_table_schema(None, None, "AdbcConf")
        .expect("get_table_schema");
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(names, vec!["Id", "Name"]);
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    assert_eq!(schema.field(1).data_type(), &DataType::Utf8);

    let mut s = connection.new_statement().expect("new statement");
    s.set_sql_query("INSERT INTO AdbcConf (Id, Name) VALUES (1, 'a')")
        .unwrap();
    assert_eq!(s.execute_update().expect("insert"), Some(1));
    assert_eq!(ffi_count(&mut connection, "AdbcConf"), 1);

    set_autocommit(&mut connection, false);
    let mut s = connection.new_statement().expect("new statement");
    s.set_sql_query("INSERT INTO AdbcConf (Id, Name) VALUES (2, 'b')")
        .unwrap();
    assert_eq!(s.execute_update().expect("buffered insert"), None);
    {
        let mut q = connection.new_statement().expect("new statement");
        q.set_sql_query("SELECT COUNT(*) AS n FROM AdbcConf")
            .unwrap();
        let Err(error) = q.execute() else {
            panic!("a query with buffered writes must be rejected through FFI");
        };
        assert_eq!(
            error.status,
            Status::InvalidState,
            "FFI query with buffered writes must fail with InvalidState"
        );
    }
    connection.commit().expect("commit");
    assert_eq!(
        ffi_count(&mut connection, "AdbcConf"),
        2,
        "visible after commit"
    );
    // The count opened a read-only transaction; end it before the next write.
    connection
        .rollback()
        .expect("end the query-kind transaction the count opened");
    let mut s = connection.new_statement().expect("new statement");
    s.set_sql_query("INSERT INTO AdbcConf (Id, Name) VALUES (3, 'c')")
        .unwrap();
    assert_eq!(s.execute_update().expect("buffered insert"), None);
    connection.rollback().expect("rollback");
    assert_eq!(
        ffi_count(&mut connection, "AdbcConf"),
        2,
        "rollback discards"
    );
    set_autocommit(&mut connection, true);

    let rows = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("Id", DataType::Int64, false),
            Field::new("Name", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![10, 11])),
            Arc::new(StringArray::from(vec!["x", "y"])),
        ],
    )
    .unwrap();
    let mut ingest = connection.new_statement().expect("new statement");
    ingest
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::String("AdbcConf".into()),
        )
        .unwrap();
    ingest
        .set_option(
            OptionStatement::IngestMode,
            OptionValue::String("append".into()),
        )
        .unwrap();
    ingest.bind(rows).expect("bind ingest rows");
    assert_eq!(ingest.execute_update().expect("ingest"), Some(2));
    assert_eq!(ffi_count(&mut connection, "AdbcConf"), 4);

    run_ffi(&mut connection, "DROP TABLE AdbcConf");
}

fn run_ffi(connection: &mut adbc_driver_manager::ManagedConnection, sql: &str) {
    let mut s = connection.new_statement().expect("new statement");
    s.set_sql_query(sql).unwrap();
    s.execute_update()
        .unwrap_or_else(|e| panic!("run {sql:?}: {e:?}"));
}

fn ffi_count(connection: &mut adbc_driver_manager::ManagedConnection, table: &str) -> i64 {
    let mut q = connection.new_statement().expect("new statement");
    q.set_sql_query(format!("SELECT COUNT(*) AS n FROM {table}"))
        .unwrap();
    let batches: Vec<_> = q
        .execute()
        .expect("count query")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    col::<Int64Array>(batches[0].column(0)).value(0)
}

/// Property graph DDL and GQL queries need no special driver support.
#[test]
fn gql_graph_query_round_trip() {
    let Some(mut fx) = fixture() else {
        return;
    };
    let connection = &mut fx.connection;

    // The property graph is created in a second batch, after its tables exist.
    let mut ddl = connection.new_statement().expect("new statement");
    ddl.set_sql_query(
        "DROP PROPERTY GRAPH IF EXISTS GqlFinGraph; \
         DROP TABLE IF EXISTS GqlTransfer; \
         DROP TABLE IF EXISTS GqlAccount; \
         CREATE TABLE GqlAccount (Id INT64 NOT NULL, Name STRING(MAX)) PRIMARY KEY (Id); \
         CREATE TABLE GqlTransfer ( \
             Id INT64 NOT NULL, ToId INT64 NOT NULL, Amount FLOAT64 \
         ) PRIMARY KEY (Id, ToId)",
    )
    .unwrap();
    assert_eq!(ddl.execute_update().expect("create graph tables"), None);

    let mut graph_ddl = connection.new_statement().expect("new statement");
    graph_ddl
        .set_sql_query(
            "CREATE PROPERTY GRAPH GqlFinGraph \
                 NODE TABLES (GqlAccount KEY (Id) LABEL Account PROPERTIES (Id, Name)) \
                 EDGE TABLES ( \
                     GqlTransfer \
                         KEY (Id, ToId) \
                         SOURCE KEY (Id) REFERENCES GqlAccount (Id) \
                         DESTINATION KEY (ToId) REFERENCES GqlAccount (Id) \
                         LABEL Transfer PROPERTIES (Amount) \
                 )",
        )
        .unwrap();
    assert_eq!(
        graph_ddl.execute_update().expect("create property graph"),
        None
    );

    let mut ins = connection.new_statement().expect("new statement");
    ins.set_sql_query(
        "INSERT INTO GqlAccount (Id, Name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')",
    )
    .unwrap();
    assert_eq!(ins.execute_update().expect("insert accounts"), Some(3));

    let mut ins_edges = connection.new_statement().expect("new statement");
    ins_edges
        .set_sql_query(
            "INSERT INTO GqlTransfer (Id, ToId, Amount) VALUES (1, 2, 100.0), (2, 3, 42.5)",
        )
        .unwrap();
    assert_eq!(
        ins_edges.execute_update().expect("insert transfers"),
        Some(2)
    );

    let mut gql = connection.new_statement().expect("new statement");
    gql.set_sql_query(
        "GRAPH GqlFinGraph \
         MATCH (a:Account)-[t:Transfer]->(b:Account) \
         RETURN a.Name AS src, b.Name AS dst, t.Amount AS amount",
    )
    .unwrap();
    let reader = gql.execute().expect("execute GQL graph query");
    let schema = reader.schema();
    assert_eq!(schema.field(0).name(), "src");
    assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
    assert_eq!(schema.field(1).name(), "dst");
    assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
    assert_eq!(schema.field(2).name(), "amount");
    assert_eq!(schema.field(2).data_type(), &DataType::Float64);

    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("collect GQL result batches");
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 2,
        "expected two edges back from the graph match"
    );

    // The match has no ORDER BY, so sort before comparing.
    let mut edges = Vec::new();
    for batch in &batches {
        let src = col::<StringArray>(batch.column(0));
        let dst = col::<StringArray>(batch.column(1));
        let amount = col::<Float64Array>(batch.column(2));
        for i in 0..batch.num_rows() {
            edges.push((
                src.value(i).to_string(),
                dst.value(i).to_string(),
                amount.value(i),
            ));
        }
    }
    edges.sort_by(|a, b| a.2.partial_cmp(&b.2).unwrap());
    assert_eq!(
        edges,
        vec![
            ("Bob".to_string(), "Carol".to_string(), 42.5),
            ("Alice".to_string(), "Bob".to_string(), 100.0),
        ],
        "the two transfer edges must round-trip through the graph match"
    );

    let mut cleanup = connection.new_statement().expect("new statement");
    cleanup
        .set_sql_query(
            "DROP PROPERTY GRAPH IF EXISTS GqlFinGraph; \
             DROP TABLE IF EXISTS GqlTransfer; \
             DROP TABLE IF EXISTS GqlAccount",
        )
        .unwrap();
    cleanup.execute_update().expect("drop graph schema");
}

/// A table in a named schema works with DML, queries, `get_table_schema`, `get_objects` and
/// `adbc.ingest.target_db_schema`. Other tests only use the default schema (`''`).
#[test]
fn named_schema_round_trip() {
    let Some(mut fx) = fixture() else {
        return;
    };
    let connection = &mut fx.connection;

    let mut ddl = connection.new_statement().expect("new statement");
    ddl.set_sql_query(
        "DROP TABLE IF EXISTS adbc_ns.Widget; \
         CREATE SCHEMA IF NOT EXISTS adbc_ns; \
         CREATE TABLE adbc_ns.Widget ( \
             Id INT64 NOT NULL, Label STRING(MAX) \
         ) PRIMARY KEY (Id)",
    )
    .unwrap();
    assert_eq!(
        ddl.execute_update().expect("create named schema + table"),
        None
    );

    let mut ins = connection.new_statement().expect("new statement");
    ins.set_sql_query("INSERT INTO adbc_ns.Widget (Id, Label) VALUES (1, 'alpha'), (2, 'beta')")
        .unwrap();
    assert_eq!(ins.execute_update().expect("insert widgets"), Some(2));
    assert_eq!(count_rows(connection, "adbc_ns.Widget"), 2);

    let widget_schema = connection
        .get_table_schema(None, Some("adbc_ns"), "Widget")
        .expect("get_table_schema for the named-schema table");
    assert_eq!(widget_schema.fields().len(), 2);
    assert_eq!(widget_schema.field(0).name(), "Id");
    assert_eq!(widget_schema.field(0).data_type(), &DataType::Int64);
    assert_eq!(widget_schema.field(1).name(), "Label");
    assert_eq!(widget_schema.field(1).data_type(), &DataType::Utf8);

    let missing_default = connection
        .get_table_schema(None, Some(""), "Widget")
        .expect_err("Widget lives only in adbc_ns, not the default schema");
    assert_eq!(missing_default.status, Status::NotFound);

    let objects = connection
        .get_objects(
            ObjectDepth::All,
            None,
            Some("adbc_ns"),
            Some("Widget"),
            None,
            None,
        )
        .expect("get_objects")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect objects");

    let ob = &objects[0];
    let schema_list = col::<ListArray>(ob.column(1));
    let schemas = schema_list.value(0);
    let schemas = col::<StructArray>(&schemas);
    let schema_names = col::<StringArray>(schemas.column(0));
    let idx = (0..schemas.len())
        .find(|&i| schema_names.value(i) == "adbc_ns")
        .expect("adbc_ns schema present in get_objects result");

    let table_lists = col::<ListArray>(schemas.column(1));
    let tables = table_lists.value(idx);
    let tables = col::<StructArray>(&tables);
    assert_eq!(tables.len(), 1);
    let table_name = col::<StringArray>(tables.column(0));
    assert_eq!(table_name.value(0), "Widget");

    let column_lists = col::<ListArray>(tables.column(2));
    let columns = column_lists.value(0);
    let columns = col::<StructArray>(&columns);
    let column_name = col::<StringArray>(columns.column(0));
    let column_names: Vec<&str> = (0..columns.len()).map(|i| column_name.value(i)).collect();
    assert_eq!(column_names, ["Id", "Label"]);

    let ingest_schema = Arc::new(Schema::new(vec![
        Field::new("Id", DataType::Int64, false),
        Field::new("Label", DataType::Utf8, true),
    ]));
    let ingest_batch = RecordBatch::try_new(
        ingest_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![3, 4])),
            Arc::new(StringArray::from(vec!["gamma", "delta"])),
        ],
    )
    .expect("build ingest batch");

    let mut ingest = ingest_stmt(connection, "Widget", "append");
    ingest
        .set_option(
            OptionStatement::TargetDbSchema,
            OptionValue::String("adbc_ns".into()),
        )
        .expect("set target_db_schema");
    ingest
        .bind_stream(Box::new(RecordBatchIterator::new(
            [Ok(ingest_batch)],
            ingest_schema,
        )))
        .expect("bind ingest stream");
    assert_eq!(
        ingest
            .execute_update()
            .expect("ingest into the named-schema table"),
        Some(2)
    );
    assert_eq!(
        count_rows(connection, "adbc_ns.Widget"),
        4,
        "the two ingested rows join the two inserted rows"
    );

    // The empty schema is left in place.
    run(connection, "DROP TABLE IF EXISTS adbc_ns.Widget");
}

/// Serializes tests that write. The emulator rejects schema changes while any read-write
/// transaction is in flight. Poisoning is ignored so one failure does not cascade.
fn serial_guard() -> std::sync::MutexGuard<'static, ()> {
    static SERIAL: Mutex<()> = Mutex::new(());
    SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// One driver and database for the whole binary, to avoid a runtime and channel pool per test.
/// Tests must not set database options on it; tests that need their own build their own driver.
fn shared_database(target: &TestTarget) -> &'static SpannerDatabase {
    static SHARED: OnceLock<(SpannerDriver, SpannerDatabase)> = OnceLock::new();
    &SHARED
        .get_or_init(|| {
            let mut driver = SpannerDriver::try_new().expect("create driver");
            let database = driver
                .new_database_with_opts([(
                    OptionDatabase::Uri,
                    OptionValue::String(target.database_uri()),
                )])
                .expect("create database");
            (driver, database)
        })
        .1
}

/// Fields are in drop order so the serial guard is released after the connection.
struct Fixture {
    connection: SpannerConnection,
    database: &'static SpannerDatabase,
    target: TestTarget,
    _serial: Option<std::sync::MutexGuard<'static, ()>>,
}

/// Connect to the test database holding the [`serial_guard`], or return `None` to skip when no
/// target is configured.
fn fixture() -> Option<Fixture> {
    build_fixture(true)
}

/// [`fixture`] without the [`serial_guard`], for read-only tests. Any write, even a single DML
/// statement, can make a concurrent DDL test fail on the emulator.
fn fixture_unguarded() -> Option<Fixture> {
    build_fixture(false)
}

fn build_fixture(serial: bool) -> Option<Fixture> {
    let Some(target) = test_target() else {
        eprintln!("no Spanner target configured; skipping Spanner integration test");
        return None;
    };
    ensure_database_once(&target, SETUP_DDL);
    let _serial = serial.then(serial_guard);

    let database = shared_database(&target);
    let connection = connect_with_retry(database);
    Some(Fixture {
        connection,
        database,
        target,
        _serial,
    })
}

/// Drop scratch tables in one DDL batch. No `IF EXISTS`, so a test that never created a table
/// fails here.
fn drop_tables(connection: &mut SpannerConnection, tables: &[&str]) {
    let sql = tables
        .iter()
        .map(|table| format!("DROP TABLE {table}"))
        .collect::<Vec<_>>()
        .join("; ");
    let mut statement = connection.new_statement().expect("new statement");
    statement.set_sql_query(sql).unwrap();
    statement
        .execute_update()
        .unwrap_or_else(|e| panic!("drop scratch tables {tables:?}: {e:?}"));
}

fn ingest_stmt(connection: &mut SpannerConnection, table: &str, mode: &str) -> SpannerStatement {
    let mut statement = connection.new_statement().expect("new statement");
    statement
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::String(table.into()),
        )
        .expect("set the ingest target table");
    statement
        .set_option(
            OptionStatement::IngestMode,
            OptionValue::String(mode.into()),
        )
        .expect("set the ingest mode");
    statement
}

/// The status of the driver error that a reader wraps in `ArrowError::ExternalError`.
fn reader_error_status(error: &arrow_schema::ArrowError) -> Status {
    let arrow_schema::ArrowError::ExternalError(source) = error else {
        panic!("expected the reader to surface the driver error, got: {error}");
    };
    source
        .downcast_ref::<adbc_core::error::Error>()
        .unwrap_or_else(|| panic!("the reader error does not wrap an ADBC error: {error}"))
        .status
}

/// Downcast a column, panicking with its actual data type on mismatch.
fn col<T: Array + 'static>(column: &dyn Array) -> &T {
    column.as_any().downcast_ref::<T>().unwrap_or_else(|| {
        panic!(
            "expected the column to be a {}, but it is {:?}",
            std::any::type_name::<T>(),
            column.data_type()
        )
    })
}

/// Column and constraint names that `get_objects` reports for `table`.
fn table_columns_and_constraints(
    connection: &mut SpannerConnection,
    table: &str,
) -> (Vec<String>, Vec<String>) {
    let batches = connection
        .get_objects(ObjectDepth::All, None, Some(""), Some(table), None, None)
        .expect("get_objects")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect objects");
    let list_struct = |array: &dyn Array| {
        let list = col::<ListArray>(array).value(0);
        col::<StructArray>(&list).clone()
    };
    let schemas = list_struct(batches[0].column(1));
    let tables = list_struct(schemas.column(1));
    assert_eq!(tables.len(), 1, "expected exactly one {table} row");
    let names = |strukt: &StructArray, field: &str| {
        let array = strukt.column_by_name(field).unwrap();
        let array = col::<StringArray>(&array);
        (0..array.len())
            .map(|i| array.value(i).to_string())
            .collect()
    };
    let columns: Vec<String> = names(&list_struct(tables.column(2)), "column_name");
    let constraints: Vec<String> = names(&list_struct(tables.column(3)), "constraint_name");
    (columns, constraints)
}

fn count_rows(connection: &mut SpannerConnection, table: &str) -> i64 {
    let mut q = connection.new_statement().expect("new statement");
    q.set_sql_query(format!("SELECT COUNT(*) AS n FROM {table}"))
        .unwrap();
    let batches: Vec<_> = q
        .execute()
        .expect("count query")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    col::<Int64Array>(batches[0].column(0)).value(0)
}

/// Count rows in a manual transaction, then end the read-only transaction the count opened so
/// the next statement can write.
fn count_rows_ending_txn(connection: &mut SpannerConnection, table: &str) -> i64 {
    let n = count_rows(connection, table);
    connection
        .rollback()
        .expect("end the query-kind transaction the count opened");
    n
}

/// Run DML in a manual transaction, which buffers it and reports no count.
fn buffer_dml(connection: &mut SpannerConnection, sql: &str) {
    let mut s = connection.new_statement().expect("new statement");
    s.set_sql_query(sql).unwrap();
    assert_eq!(s.execute_update().expect("buffer DML"), None);
}

fn set_autocommit<C: Optionable<Option = OptionConnection>>(connection: &mut C, enabled: bool) {
    connection
        .set_option(
            OptionConnection::AutoCommit,
            OptionValue::String(enabled.to_string()),
        )
        .unwrap_or_else(|e| panic!("set autocommit to {enabled}: {e:?}"));
}

/// Assert that a query is rejected while the manual transaction has buffered writes.
fn assert_query_guarded(connection: &mut SpannerConnection, table: &str) {
    let mut q = connection.new_statement().expect("new statement");
    q.set_sql_query(format!("SELECT COUNT(*) AS n FROM {table}"))
        .unwrap();
    let Err(error) = q.execute() else {
        panic!("a query with buffered writes must be rejected, not silently run");
    };
    assert_eq!(
        error.status,
        Status::InvalidState,
        "querying with buffered writes must fail with InvalidState (no read-your-writes)"
    );
}

/// (table, column, key, value) tuples from a `get_statistics` batch.
fn extract_statistics(batch: &RecordBatch) -> Vec<(String, Option<String>, i16, i64)> {
    let mut out = Vec::new();
    let db_schemas_list = col::<ListArray>(batch.column(1));
    let db_schemas = db_schemas_list.value(0);
    let db_schemas = col::<StructArray>(&db_schemas);
    let stats_list = col::<ListArray>(db_schemas.column_by_name("db_schema_statistics").unwrap());
    for i in 0..db_schemas.len() {
        let stats = stats_list.value(i);
        let stats = col::<StructArray>(&stats);
        let table = col::<StringArray>(stats.column_by_name("table_name").unwrap());
        let column = col::<StringArray>(stats.column_by_name("column_name").unwrap());
        let key = col::<Int16Array>(stats.column_by_name("statistic_key").unwrap());
        let value = col::<UnionArray>(stats.column_by_name("statistic_value").unwrap());
        for r in 0..stats.len() {
            let column_name = if column.is_null(r) {
                None
            } else {
                Some(column.value(r).to_string())
            };
            let v = value.value(r);
            let v = col::<Int64Array>(&v).value(0);
            out.push((table.value(r).to_string(), column_name, key.value(r), v));
        }
    }
    out
}

#[test]
fn get_statistics_reports_real_counts() {
    use adbc_core::constants::{
        ADBC_STATISTIC_DISTINCT_COUNT_KEY, ADBC_STATISTIC_NULL_COUNT_KEY,
        ADBC_STATISTIC_ROW_COUNT_KEY,
    };
    let Some(mut fx) = fixture() else {
        return;
    };
    let connection = &mut fx.connection;

    // UUID must be treated as groupable so it gets a distinct count. INTERVAL, which is not, is
    // covered by a unit test because the emulator rejects INTERVAL columns.
    run(
        connection,
        "DROP TABLE IF EXISTS AdbcStats; \
         CREATE TABLE AdbcStats (Id INT64, Name STRING(MAX), Tag UUID) PRIMARY KEY (Id)",
    );
    run(
        connection,
        "INSERT INTO AdbcStats (Id, Name, Tag) VALUES \
         (1, 'a', CAST('11111111-2222-3333-4444-555555555555' AS UUID)), \
         (2, 'a', CAST('11111111-2222-3333-4444-555555555555' AS UUID)), \
         (3, NULL, NULL)",
    );

    let batches = connection
        .get_statistics(None, None, Some("AdbcStats"), false)
        .expect("get_statistics")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect statistics");
    let stats = extract_statistics(&batches[0]);
    let has = |col: Option<&str>, key: i16, val: i64| {
        stats
            .iter()
            .any(|(t, c, k, v)| t == "AdbcStats" && c.as_deref() == col && *k == key && *v == val)
    };
    assert!(
        has(None, ADBC_STATISTIC_ROW_COUNT_KEY, 3),
        "row count 3: {stats:?}"
    );
    assert!(
        has(Some("Name"), ADBC_STATISTIC_NULL_COUNT_KEY, 1),
        "Name null 1: {stats:?}"
    );
    assert!(
        has(Some("Name"), ADBC_STATISTIC_DISTINCT_COUNT_KEY, 1),
        "Name distinct 1: {stats:?}"
    );
    assert!(
        has(Some("Id"), ADBC_STATISTIC_NULL_COUNT_KEY, 0),
        "Id null 0: {stats:?}"
    );
    assert!(
        has(Some("Id"), ADBC_STATISTIC_DISTINCT_COUNT_KEY, 3),
        "Id distinct 3: {stats:?}"
    );
    assert!(
        has(Some("Tag"), ADBC_STATISTIC_NULL_COUNT_KEY, 1),
        "Tag null 1: {stats:?}"
    );
    assert!(
        has(Some("Tag"), ADBC_STATISTIC_DISTINCT_COUNT_KEY, 1),
        "Tag distinct 1 (UUID is groupable): {stats:?}"
    );

    // Spanner has no cheaper source of approximate statistics.
    let approx = connection
        .get_statistics(None, None, Some("AdbcStats"), true)
        .expect("get_statistics approx")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect approx");
    assert_eq!(
        extract_statistics(&approx[0]),
        stats,
        "approximate=true must serve the same exact statistics"
    );

    drop_tables(connection, &["AdbcStats"]);
}

/// JSON maps to `Utf8` tagged `arrow.json`, and FLOAT32 to `Float32`, in reads, binds and ingest.
#[test]
fn json_and_float32_round_trip() {
    let Some(mut fx) = fixture() else {
        return;
    };
    let connection = &mut fx.connection;

    run(
        connection,
        "DROP TABLE IF EXISTS AdbcJson; \
         DROP TABLE IF EXISTS AdbcJsonIngest; \
         CREATE TABLE AdbcJson (Id INT64, Doc JSON, Ratio FLOAT32, Docs ARRAY<JSON>) \
         PRIMARY KEY (Id)",
    );
    run(
        connection,
        r#"INSERT INTO AdbcJson (Id, Doc, Ratio) VALUES
           (1, JSON '{"a":1,"b":"x"}', 1.5), (2, NULL, NULL)"#,
    );

    let mut query = connection.new_statement().expect("new statement");
    query
        .set_sql_query("SELECT Doc, Ratio FROM AdbcJson ORDER BY Id")
        .unwrap();
    let reader = query.execute().expect("query json/float32");

    let schema = reader.schema();
    let doc_field = schema.field(0);
    assert_eq!(doc_field.data_type(), &DataType::Utf8);
    assert_eq!(
        doc_field
            .metadata()
            .get("ARROW:extension:name")
            .map(String::as_str),
        Some("arrow.json"),
        "JSON column must carry the arrow.json extension: {doc_field:?}"
    );
    assert_eq!(schema.field(1).data_type(), &DataType::Float32);

    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("collect json/float32 batches");
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
    let batch = &batches[0];
    let docs = col::<StringArray>(batch.column(0));
    let ratios = col::<Float32Array>(batch.column(1));

    // Backends normalize JSON text differently.
    let doc = docs.value(0);
    assert!(
        doc.contains(r#""a":1"#) && doc.contains(r#""b":"x""#),
        "unexpected JSON text: {doc:?}"
    );
    assert!(docs.is_null(1), "NULL JSON must come back null");
    assert_eq!(ratios.value(0), 1.5f32);
    assert!(ratios.is_null(1), "NULL FLOAT32 must come back null");

    // Tagged parameters must bind as JSON; Spanner rejects STRING parameters for JSON columns.
    let json_field = |name: &str| {
        Field::new(name, DataType::Utf8, true).with_metadata(std::collections::HashMap::from([(
            "ARROW:extension:name".to_string(),
            "arrow.json".to_string(),
        )]))
    };
    let docs_item = Arc::new(json_field("item"));
    let mut docs_builder =
        arrow_array::builder::ListBuilder::new(arrow_array::builder::StringBuilder::new())
            .with_field(docs_item.clone());
    docs_builder.values().append_value(r#"{"d":true}"#);
    docs_builder.values().append_null();
    docs_builder.append(true);
    docs_builder.append(false);
    let bind_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("Id", DataType::Int64, false),
            json_field("Doc"),
            Field::new("Docs", DataType::List(docs_item), true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![3, 4])),
            Arc::new(StringArray::from(vec![Some(r#"{"c":[1,2]}"#), None])),
            Arc::new(docs_builder.finish()),
        ],
    )
    .unwrap();
    let mut insert = connection.new_statement().expect("new statement");
    insert
        .set_sql_query("INSERT INTO AdbcJson (Id, Doc, Docs) VALUES (@Id, @Doc, @Docs)")
        .unwrap();
    insert.bind(bind_batch.clone()).expect("bind json rows");
    insert.execute_update().expect("insert bound json rows");

    let mut verify = connection.new_statement().expect("new statement");
    verify
        .set_sql_query("SELECT Doc, Docs FROM AdbcJson WHERE Id >= 3 ORDER BY Id")
        .unwrap();
    let reader = verify.execute().expect("query bound json");
    let bound_schema = reader.schema();
    let DataType::List(read_item) = bound_schema.field(1).data_type() else {
        panic!("Docs must read back as a List: {bound_schema:?}");
    };
    assert_eq!(
        read_item
            .metadata()
            .get("ARROW:extension:name")
            .map(String::as_str),
        Some("arrow.json"),
        "ARRAY<JSON> item must carry the arrow.json extension"
    );
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("collect bound json batches");
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
    let batch = &batches[0];
    let doc_col = col::<StringArray>(batch.column(0));
    assert!(
        doc_col.value(0).contains(r#""c":[1,2]"#),
        "unexpected bound JSON text: {:?}",
        doc_col.value(0)
    );
    assert!(doc_col.is_null(1), "bound NULL JSON must come back null");
    let docs_col = col::<ListArray>(batch.column(1));
    let first_cell = docs_col.value(0);
    let first_cell = col::<StringArray>(&first_cell);
    assert_eq!(first_cell.len(), 2);
    assert!(
        first_cell.value(0).contains(r#""d":true"#),
        "unexpected bound ARRAY<JSON> element: {:?}",
        first_cell.value(0)
    );
    assert!(first_cell.is_null(1), "NULL array element must survive");
    assert!(docs_col.is_null(1), "bound NULL array must come back null");

    // Create-mode ingest makes tagged fields JSON columns.
    let mut ingest = ingest_stmt(connection, "AdbcJsonIngest", "create");
    ingest.bind(bind_batch).expect("bind json ingest rows");
    ingest.execute_update().expect("create-mode json ingest");
    let mut cols = connection.new_statement().expect("new statement");
    cols.set_sql_query(
        "SELECT COLUMN_NAME, SPANNER_TYPE FROM INFORMATION_SCHEMA.COLUMNS \
         WHERE TABLE_NAME = 'AdbcJsonIngest' AND COLUMN_NAME IN ('Doc', 'Docs') \
         ORDER BY COLUMN_NAME",
    )
    .unwrap();
    let cols_batches = cols
        .execute()
        .expect("query ingest column types")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect ingest column types");
    let cols_batch = &cols_batches[0];
    let names = col::<StringArray>(cols_batch.column(0));
    let types = col::<StringArray>(cols_batch.column(1));
    assert_eq!(
        (names.value(0), types.value(0)),
        ("Doc", "JSON"),
        "tagged scalar ingest column"
    );
    assert_eq!(
        (names.value(1), types.value(1)),
        ("Docs", "ARRAY<JSON>"),
        "tagged array ingest column"
    );

    drop_tables(connection, &["AdbcJson", "AdbcJsonIngest"]);
}

#[test]
fn execute_streams_in_batches() {
    let Some(mut fx) = fixture() else {
        return;
    };
    let connection = &mut fx.connection;

    run(
        connection,
        "DROP TABLE IF EXISTS AdbcStream; \
         CREATE TABLE AdbcStream (Id INT64) PRIMARY KEY (Id)",
    );
    run(
        connection,
        "INSERT INTO AdbcStream (Id) \
         SELECT n FROM UNNEST(GENERATE_ARRAY(1, 2500)) AS n",
    );

    let mut query = connection.new_statement().expect("new statement");
    query
        .set_option(
            OptionStatement::Other(spanner_adbc::OPTION_ROWS_PER_BATCH.into()),
            OptionValue::Int(1000),
        )
        .expect("set rows_per_batch");
    assert_eq!(
        query
            .get_option_int(OptionStatement::Other(
                spanner_adbc::OPTION_ROWS_PER_BATCH.into()
            ))
            .expect("get rows_per_batch"),
        1000
    );
    query
        .set_sql_query("SELECT Id FROM AdbcStream ORDER BY Id")
        .unwrap();
    let reader = query.execute().expect("streaming query");
    let schema = reader.schema();
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("collect streamed batches");

    assert_eq!(batches.len(), 3, "expected three streamed batches");
    assert!(batches.iter().all(|b| b.schema() == schema));
    let sizes: Vec<usize> = batches.iter().map(RecordBatch::num_rows).collect();
    assert_eq!(sizes, vec![1000, 1000, 500]);

    let total: usize = sizes.iter().sum();
    assert_eq!(total, 2500);
    let mut expected = 1i64;
    for batch in &batches {
        let ids = col::<Int64Array>(batch.column(0));
        for i in 0..ids.len() {
            assert_eq!(ids.value(i), expected);
            expected += 1;
        }
    }

    drop_tables(connection, &["AdbcStream"]);
}

/// `adbc.statement.bind_by_name` (from the SQLite driver): positional by default even when names
/// match; `true` binds by name and rejects unmatched columns.
#[test]
fn bind_by_name_modes() {
    let Some(mut fx) = fixture() else {
        return;
    };
    let connection = &mut fx.connection;

    let bind_by_name_key = || OptionStatement::Other(spanner_adbc::OPTION_BIND_BY_NAME.into());
    // Columns named after the parameters but in swapped order, so the mode is observable.
    let batch = |names: [&str; 2]| {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new(names[0], DataType::Int64, false),
                Field::new(names[1], DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![10i64])),
                Arc::new(Int64Array::from(vec![20i64])),
            ],
        )
        .unwrap()
    };
    // Returns (@a, @b) with `rows` bound; `None` leaves bind_by_name unset.
    let query_pair = |connection: &mut SpannerConnection,
                      rows: RecordBatch,
                      mode: Option<&str>|
     -> Result<(i64, i64), adbc_core::error::Error> {
        let mut s = connection.new_statement().expect("new statement");
        if let Some(value) = mode {
            s.set_option(bind_by_name_key(), OptionValue::String(value.into()))
                .expect("set bind_by_name");
            assert_eq!(
                s.get_option_string(bind_by_name_key())
                    .expect("get bind_by_name"),
                value,
                "bind_by_name must round-trip through get_option"
            );
        } else {
            assert_eq!(
                s.get_option_string(bind_by_name_key())
                    .expect("bind_by_name reads its default"),
                "false",
                "bind_by_name defaults to false (positional)"
            );
        }
        s.set_sql_query("SELECT @a AS a_value, @b AS b_value")
            .unwrap();
        s.bind(rows).expect("bind rows");
        let reader = s.execute()?;
        let batches = reader
            .collect::<Result<Vec<_>, _>>()
            .expect("collect bound query result");
        assert_eq!(batches.len(), 1, "one bound row -> one result batch");
        let ints = |i: usize| col::<Int64Array>(batches[0].column(i)).value(0);
        Ok((ints(0), ints(1)))
    };

    assert_eq!(
        query_pair(connection, batch(["b", "a"]), None).expect("default positional binding"),
        (10, 20),
        "the default (bind_by_name unset) must bind positionally"
    );
    assert_eq!(
        query_pair(connection, batch(["b", "a"]), Some("false")).expect("positional binding"),
        (10, 20),
        "bind_by_name=false must ignore coincidental name matches and bind positionally"
    );
    assert_eq!(
        query_pair(connection, batch(["b", "a"]), Some("true")).expect("by-name binding"),
        (20, 10),
        "bind_by_name=true must bind matching columns by name"
    );
    let error = query_pair(connection, batch(["a", "x"]), Some("true"))
        .expect_err("bind_by_name=true must reject an unmatched column");
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(
        error.message.contains("cannot bind column \"x\""),
        "error must name the unmatched column: {}",
        error.message
    );
    assert_eq!(
        query_pair(connection, batch(["a", "x"]), None).expect("default positional binding"),
        (10, 20),
        "a partial name match under the default binding must bind positionally"
    );
    // The option has no unset state.
    let mut invalid = connection.new_statement().expect("new statement");
    assert_eq!(
        invalid
            .set_option(bind_by_name_key(), OptionValue::String(String::new()))
            .expect_err("empty bind_by_name must be rejected")
            .status,
        Status::InvalidArguments,
    );
}

/// A query with several bound rows streams each row's result in `spanner.rows_per_batch` chunks.
#[test]
fn bound_query_streams_in_batches() {
    let Some(mut fx) = fixture() else {
        return;
    };
    let connection = &mut fx.connection;

    run(
        connection,
        "DROP TABLE IF EXISTS AdbcBoundStream; \
         CREATE TABLE AdbcBoundStream (Id INT64, Grp INT64) PRIMARY KEY (Id)",
    );
    run(
        connection,
        "INSERT INTO AdbcBoundStream (Id, Grp) \
         SELECT n, MOD(n, 3) FROM UNNEST(GENERATE_ARRAY(1, 1500)) AS n",
    );

    let mut query = connection.new_statement().expect("new statement");
    query
        .set_option(
            OptionStatement::Other(spanner_adbc::OPTION_ROWS_PER_BATCH.into()),
            OptionValue::Int(200),
        )
        .expect("set rows_per_batch");
    query
        .set_sql_query("SELECT Id FROM AdbcBoundStream WHERE Grp = @Grp ORDER BY Id")
        .unwrap();
    let params = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("Grp", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![0i64, 1, 2]))],
    )
    .unwrap();
    query.bind(params).expect("bind three parameter rows");

    let reader = query.execute().expect("bound streaming query");
    let schema = reader.schema();
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("collect streamed batches");

    let sizes: Vec<usize> = batches.iter().map(RecordBatch::num_rows).collect();
    assert_eq!(sizes, vec![200, 200, 100, 200, 200, 100, 200, 200, 100]);
    assert!(batches.iter().all(|b| b.schema() == schema));

    let ids: Vec<i64> = batches
        .iter()
        .flat_map(|batch| {
            let ids = col::<Int64Array>(batch.column(0));
            (0..ids.len()).map(|i| ids.value(i)).collect::<Vec<_>>()
        })
        .collect();
    let expected: Vec<i64> = (0..3)
        .flat_map(|group| (1..=1500i64).filter(move |id| id % 3 == group))
        .collect();
    assert_eq!(ids, expected);

    drop_tables(connection, &["AdbcBoundStream"]);
}

/// A cancel between chunk fetches still cancels the next fetch, and the next execute on the same
/// statement runs normally.
#[test]
fn cancel_between_stream_chunks_cancels_the_next_fetch() {
    let Some(mut fx) = fixture() else {
        return;
    };
    let connection = &mut fx.connection;

    run(
        connection,
        "DROP TABLE IF EXISTS AdbcCancel; \
         CREATE TABLE AdbcCancel (Id INT64) PRIMARY KEY (Id)",
    );
    run(
        connection,
        "INSERT INTO AdbcCancel (Id) \
         SELECT n FROM UNNEST(GENERATE_ARRAY(1, 300)) AS n",
    );

    let mut query = connection.new_statement().expect("new statement");
    query
        .set_option(
            OptionStatement::Other(spanner_adbc::OPTION_ROWS_PER_BATCH.into()),
            OptionValue::Int(100),
        )
        .expect("set rows_per_batch");
    query
        .set_sql_query("SELECT Id FROM AdbcCancel ORDER BY Id")
        .unwrap();
    let mut reader = query.execute().expect("streaming query");

    // `execute` already fetched the first chunk.
    let first = reader
        .next()
        .expect("first batch")
        .expect("first batch is ok");
    assert_eq!(first.num_rows(), 100);

    query.get_cancel_handle().try_cancel().expect("cancel");
    let error = reader
        .next()
        .expect("the cancelled fetch yields an item")
        .expect_err("the fetch after cancel must fail");
    assert_eq!(reader_error_status(&error), Status::Cancelled, "{error}");

    let batches = query
        .execute()
        .expect("re-execute after cancel")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect re-executed batches");
    let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total, 300);

    drop_tables(connection, &["AdbcCancel"]);
}

/// Dropping a reader while a prefetch is in flight leaves the statement usable.
#[test]
fn dropping_reader_mid_stream_aborts_the_prefetch() {
    let Some(mut fx) = fixture() else {
        return;
    };
    let connection = &mut fx.connection;

    run(
        connection,
        "DROP TABLE IF EXISTS AdbcReaderDrop; \
         CREATE TABLE AdbcReaderDrop (Id INT64) PRIMARY KEY (Id)",
    );
    run(
        connection,
        "INSERT INTO AdbcReaderDrop (Id) \
         SELECT n FROM UNNEST(GENERATE_ARRAY(1, 500)) AS n",
    );

    let mut query = connection.new_statement().expect("new statement");
    query
        .set_option(
            OptionStatement::Other(spanner_adbc::OPTION_ROWS_PER_BATCH.into()),
            OptionValue::Int(50),
        )
        .expect("set rows_per_batch");
    query
        .set_sql_query("SELECT Id FROM AdbcReaderDrop ORDER BY Id")
        .unwrap();
    let mut reader = query.execute().expect("streaming query");
    let first = reader
        .next()
        .expect("first batch")
        .expect("first batch is ok");
    assert_eq!(first.num_rows(), 50);
    drop(reader);

    let batches = query
        .execute()
        .expect("re-execute after dropping the reader")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect re-executed batches");
    let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total, 500);

    drop_tables(connection, &["AdbcReaderDrop"]);
}

/// `Utf8View` and `BinaryView` (polars' defaults), `Int8` and `Date64` parameters bind correctly.
#[test]
fn view_and_narrow_types_bind_round_trip() {
    let Some(mut fx) = fixture() else {
        return;
    };
    let connection = &mut fx.connection;

    run(
        connection,
        "DROP TABLE IF EXISTS AdbcView; \
         CREATE TABLE AdbcView (Id INT64, S STRING(MAX), B BYTES(MAX), D DATE) PRIMARY KEY (Id)",
    );

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int8, false),
            Field::new("s", DataType::Utf8View, true),
            Field::new("b", DataType::BinaryView, true),
            Field::new("d", DataType::Date64, true),
        ])),
        vec![
            Arc::new(arrow_array::Int8Array::from(vec![1i8, 2])),
            Arc::new(arrow_array::StringViewArray::from(vec![
                Some("view-hello"),
                None,
            ])),
            Arc::new(arrow_array::BinaryViewArray::from(vec![
                Some(b"view-bytes".as_ref()),
                None,
            ])),
            // 2024-01-15
            Arc::new(arrow_array::Date64Array::from(vec![
                Some(19_737i64 * 86_400_000),
                None,
            ])),
        ],
    )
    .unwrap();

    let mut insert = connection.new_statement().expect("new statement");
    insert
        .set_sql_query("INSERT INTO AdbcView (Id, S, B, D) VALUES (@id, @s, @b, @d)")
        .unwrap();
    insert.bind(batch).expect("bind view-typed batch");
    assert_eq!(insert.execute_update().expect("insert"), Some(2));

    let mut query = connection.new_statement().expect("new statement");
    query
        .set_sql_query("SELECT S, B, D FROM AdbcView ORDER BY Id")
        .unwrap();
    let batches = query
        .execute()
        .expect("query")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect");
    let all = &batches[0];
    assert_eq!(all.num_rows(), 2);
    let s = col::<StringArray>(all.column(0));
    assert_eq!(s.value(0), "view-hello");
    assert!(s.is_null(1));
    let b = col::<BinaryArray>(all.column(1));
    assert_eq!(b.value(0), b"view-bytes");
    assert!(b.is_null(1));
    let d = col::<Date32Array>(all.column(2));
    assert_eq!(d.value(0), 19_737);
    assert!(d.is_null(1));

    drop_tables(connection, &["AdbcView"]);
}

/// DML behind a leading `@{...}` statement hint is still recognized as DML, so `execute()` uses a
/// read/write transaction.
#[test]
fn hinted_dml_routes_to_the_read_write_path() {
    let Some(mut fx) = fixture() else {
        return;
    };
    let connection = &mut fx.connection;

    // `AdbcHint` is created during setup; see `SETUP_DDL`.
    run(connection, "DELETE FROM AdbcHint WHERE true");

    // The emulator rejects most statement hints, but accepts `LOCK_SCANNED_RANGES`.
    let mut insert = connection.new_statement().expect("new statement");
    insert
        .set_sql_query(
            "@{LOCK_SCANNED_RANGES=exclusive} INSERT INTO AdbcHint (Id, N) VALUES (1, 0)",
        )
        .unwrap();
    insert.execute().expect("hinted INSERT via execute");

    let mut update = connection.new_statement().expect("new statement");
    update
        .set_sql_query("@{LOCK_SCANNED_RANGES=exclusive} UPDATE AdbcHint SET N = 1 WHERE true")
        .unwrap();
    assert_eq!(
        update.execute_update().expect("hinted UPDATE"),
        Some(1),
        "the hinted INSERT committed exactly one row"
    );
}

/// `THEN RETURN` rows come back from `execute()`, with bound parameters too. Manual transactions
/// reject it because buffered DML commits through `ExecuteBatchDml`, which cannot return rows.
#[test]
fn dml_then_return_round_trip() {
    let Some(mut fx) = fixture() else {
        return;
    };
    let connection = &mut fx.connection;

    // `AdbcReturn` is created during setup; see `SETUP_DDL`.
    run(connection, "DELETE FROM AdbcReturn WHERE true");

    let mut insert = connection.new_statement().expect("new statement");
    insert
        .set_sql_query(
            "INSERT INTO AdbcReturn (Id, Name) VALUES (1, 'a'), (2, 'b') THEN RETURN Id, Name",
        )
        .unwrap();
    let reader = insert.execute().expect("insert then return");
    let schema = reader.schema();
    assert_eq!(schema.field(0).name(), "Id");
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    assert_eq!(schema.field(1).name(), "Name");
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("collect returned rows");
    let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total, 2, "one returned row per inserted row");
    let ids = col::<Int64Array>(batches[0].column(0));
    assert_eq!((ids.value(0), ids.value(1)), (1, 2));

    let mut update = connection.new_statement().expect("new statement");
    update
        .set_sql_query("UPDATE AdbcReturn SET Name = 'x' WHERE Id <= 2 THEN RETURN Id")
        .unwrap();
    assert_eq!(
        update.execute_update().expect("update then return"),
        Some(2)
    );

    let mut bound = connection.new_statement().expect("new statement");
    bound
        .set_sql_query("INSERT INTO AdbcReturn (Id, Name) VALUES (@id, @name) THEN RETURN Id")
        .unwrap();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![10, 11])),
            Arc::new(StringArray::from(vec!["x", "y"])),
        ],
    )
    .unwrap();
    bound.bind(batch.clone()).expect("bind");
    let batches = bound
        .execute()
        .expect("bound insert then return")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect bound returned rows");
    let ids: Vec<_> = batches
        .iter()
        .flat_map(|batch| col::<Int64Array>(batch.column(0)).values().iter().copied())
        .collect();
    assert_eq!(ids, [10, 11]);
    assert_eq!(count_rows(connection, "AdbcReturn"), 4);

    // A failed later row must roll back the entire bound batch, including its first insert.
    let duplicate = RecordBatch::try_new(
        batch.schema(),
        vec![
            Arc::new(Int64Array::from(vec![20, 20])),
            batch.column(1).clone(),
        ],
    )
    .unwrap();
    bound.bind(duplicate).unwrap();
    let Err(error) = bound.execute() else {
        panic!("duplicate second row must fail");
    };
    assert_eq!(error.status, Status::Integrity, "{error}");
    assert_eq!(count_rows(connection, "AdbcReturn"), 4);

    set_autocommit(connection, false);
    let mut manual = connection.new_statement().expect("new statement");
    manual
        .set_sql_query("DELETE FROM AdbcReturn WHERE true THEN RETURN Id")
        .unwrap();
    let error = manual
        .execute_update()
        .expect_err("THEN RETURN must be rejected in manual transaction mode");
    assert_eq!(error.status, Status::InvalidState);
    assert!(error.message.contains("THEN RETURN"), "{}", error.message);
    connection.rollback().expect("rollback");
    set_autocommit(connection, true);
}

/// `execute_partitions` and `read_partition` together return every row exactly once.
#[test]
fn execute_partitions_round_trip() {
    let Some(mut fx) = fixture() else {
        return;
    };
    let connection = &mut fx.connection;

    run(
        connection,
        "DROP TABLE IF EXISTS AdbcPartition; \
         CREATE TABLE AdbcPartition (Id INT64) PRIMARY KEY (Id)",
    );
    run(
        connection,
        "INSERT INTO AdbcPartition (Id) \
         SELECT n FROM UNNEST(GENERATE_ARRAY(1, 200)) AS n",
    );

    let mut statement = connection.new_statement().expect("new statement");
    let data_boost_key = || OptionStatement::Other(spanner_adbc::OPTION_DATA_BOOST.into());
    statement
        .set_option(data_boost_key(), OptionValue::String("true".into()))
        .expect("set data_boost");
    assert_eq!(
        statement
            .get_option_string(data_boost_key())
            .expect("get data_boost"),
        "true"
    );

    // ORDER BY would make the query non-partitionable.
    statement
        .set_sql_query("SELECT Id FROM AdbcPartition")
        .unwrap();
    let mut partitioned = statement.execute_partitions().expect("execute_partitions");
    assert_eq!(partitioned.schema.fields().len(), 1);
    assert_eq!(partitioned.schema.field(0).name(), "Id");
    assert_eq!(partitioned.schema.field(0).data_type(), &DataType::Int64);
    assert!(
        !partitioned.partitions.is_empty(),
        "expected at least one partition"
    );
    assert_eq!(partitioned.rows_affected, -1);

    // `read_partition` only learns about Data Boost from the descriptor.
    for token in &partitioned.partitions {
        let descriptor: serde_json::Value = serde_json::from_slice(token)
            .expect("a partition descriptor must be our JSON envelope");
        assert_eq!(
            descriptor["v"], 1,
            "unexpected descriptor version: {descriptor}"
        );
        assert_eq!(
            descriptor["partition"]["inner"]["Query"]["dataBoostEnabled"],
            serde_json::Value::Bool(true),
            "spanner.data_boost=true must be baked into the descriptor: {descriptor}"
        );
    }

    if fx.target.omni_endpoint.is_some() {
        // Omni supports partitioned reads but rejects the Cloud-only Data Boost service.
        let Err(error) = connection.read_partition(&partitioned.partitions[0]) else {
            panic!("Omni unexpectedly accepted Data Boost");
        };
        assert_eq!(error.status, Status::InvalidArguments);
        assert!(error.message.contains("DataBoost"), "{error}");
        statement
            .set_option(data_boost_key(), "false".into())
            .unwrap();
        partitioned = statement
            .execute_partitions()
            .expect("partition without Data Boost");
    }

    let mut seen: std::collections::BTreeSet<i64> = std::collections::BTreeSet::new();
    for token in &partitioned.partitions {
        let reader = connection.read_partition(token).expect("read_partition");
        assert_eq!(*reader.schema(), partitioned.schema);
        for batch in reader {
            let batch = batch.expect("partition batch");
            let ids = col::<Int64Array>(batch.column(0));
            for i in 0..ids.len() {
                assert!(
                    seen.insert(ids.value(i)),
                    "id {} appeared in more than one partition",
                    ids.value(i)
                );
            }
        }
    }
    assert_eq!(seen.len(), 200);
    assert_eq!(*seen.iter().next().unwrap(), 1);
    assert_eq!(*seen.iter().next_back().unwrap(), 200);

    // Spanner begins batch transactions only with exact bounds, so `max:` is pinned.
    statement
        .set_option(
            OptionStatement::Other(spanner_adbc::OPTION_READ_STALENESS.into()),
            "max:0s".into(),
        )
        .unwrap();
    statement
        .execute_partitions()
        .expect("execute_partitions with a bounded staleness");

    // Partitioning accepts exactly one bound parameter row.
    let mut bound_stmt = connection.new_statement().expect("new statement");
    bound_stmt
        .set_sql_query("SELECT Id FROM AdbcPartition WHERE Id <= @max")
        .unwrap();
    let param_schema = Arc::new(Schema::new(vec![Field::new("max", DataType::Int64, false)]));
    let one_row =
        RecordBatch::try_new(param_schema, vec![Arc::new(Int64Array::from(vec![50_i64]))])
            .expect("build parameter batch");
    bound_stmt.bind(one_row).expect("bind one parameter row");
    let partitioned = bound_stmt
        .execute_partitions()
        .expect("execute_partitions with one bound row");
    let mut seen: std::collections::BTreeSet<i64> = std::collections::BTreeSet::new();
    for token in &partitioned.partitions {
        let reader = connection.read_partition(token).expect("read_partition");
        for batch in reader {
            let batch = batch.expect("partition batch");
            let ids = col::<Int64Array>(batch.column(0));
            for i in 0..ids.len() {
                assert!(seen.insert(ids.value(i)), "duplicate id {}", ids.value(i));
            }
        }
    }
    assert_eq!(
        seen.len(),
        50,
        "the bound @max = 50 filter must be honoured"
    );
    assert_eq!(*seen.iter().next_back().unwrap(), 50);
    // `execute_partitions` consumes the bound row; `set_sql_query` alone would not clear it.
    bound_stmt.set_sql_query("SELECT 1").unwrap();
    let params = bound_stmt
        .get_parameter_schema()
        .expect("parameter schema after partitioning");
    assert_eq!(
        params.fields().len(),
        0,
        "the bound row must not survive execute_partitions on a reused statement handle"
    );

    drop_tables(connection, &["AdbcPartition"]);
}

#[test]
fn query_with_trailing_semicolons_returns_rows() {
    let Some(mut fx) = fixture_unguarded() else {
        return;
    };
    let connection = &mut fx.connection;

    // Spanner rejects a trailing `;`, so the driver strips them (`SqlQueryTrailingSemicolons`).
    let mut statement = connection.new_statement().expect("new statement");
    statement.set_sql_query("SELECT 1 AS n;;;").unwrap();
    let reader = statement.execute().expect("query with trailing semicolons");
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("collect batches");
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1, "expected one row back");
    let n = col::<Int64Array>(batches[0].column(0));
    assert_eq!(n.value(0), 1);

    let mut str_stmt = connection.new_statement().expect("new statement");
    str_stmt.set_sql_query("SELECT ';' AS s;").unwrap();
    let reader = str_stmt
        .execute()
        .expect("query with a semicolon string literal");
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("collect batches");
    let s = col::<StringArray>(batches[0].column(0));
    assert_eq!(s.value(0), ";");

    let mut schema_stmt = connection.new_statement().expect("new statement");
    schema_stmt.set_sql_query("SELECT 1 AS n;;;").unwrap();
    let planned = schema_stmt
        .execute_schema()
        .expect("execute_schema with trailing semicolons");
    assert_eq!(planned.fields().len(), 1);
    assert_eq!(planned.field(0).name(), "n");
    assert_eq!(planned.field(0).data_type(), &DataType::Int64);
}

/// `adbc.connection.readonly` allows queries and rejects DML, DDL and ingest. Toggling it affects
/// existing statements.
#[test]
fn readonly_connection_rejects_writes() {
    let Some(mut fx) = fixture() else {
        return;
    };
    let connection = &mut fx.connection;

    assert_eq!(
        connection
            .get_option_string(OptionConnection::ReadOnly)
            .expect("read default readonly"),
        "false",
        "adbc.connection.readonly defaults to false"
    );
    connection
        .set_option(
            OptionConnection::ReadOnly,
            OptionValue::String("true".into()),
        )
        .expect("enable readonly");
    assert_eq!(
        connection
            .get_option_string(OptionConnection::ReadOnly)
            .expect("read readonly back"),
        "true",
        "setting adbc.connection.readonly=true round-trips through get_option"
    );

    let mut query = connection.new_statement().expect("new statement");
    query.set_sql_query("SELECT 1 AS n").unwrap();
    let rows: usize = query
        .execute()
        .expect("SELECT on a read-only connection must run")
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .iter()
        .map(RecordBatch::num_rows)
        .sum();
    assert_eq!(
        rows, 1,
        "a query on a read-only connection returns its rows"
    );

    let mut dml = connection.new_statement().expect("new statement");
    dml.set_sql_query("DELETE FROM Singers WHERE true").unwrap();
    let dml_err = dml
        .execute_update()
        .expect_err("DML on a read-only connection must fail");
    assert_eq!(
        dml_err.status,
        Status::InvalidState,
        "DML on a read-only connection must be InvalidState, got: {dml_err:?}"
    );

    let mut ddl = connection.new_statement().expect("new statement");
    ddl.set_sql_query("CREATE TABLE AdbcReadOnlyDenied (Id INT64) PRIMARY KEY (Id)")
        .unwrap();
    let ddl_err = ddl
        .execute_update()
        .expect_err("DDL on a read-only connection must fail");
    assert_eq!(
        ddl_err.status,
        Status::InvalidState,
        "DDL on a read-only connection must be InvalidState, got: {ddl_err:?}"
    );

    let ingest_rows = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "SingerId",
            DataType::Int64,
            false,
        )])),
        vec![Arc::new(Int64Array::from(vec![1]))],
    )
    .unwrap();
    let mut ingest = connection.new_statement().expect("new statement");
    ingest
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::String("Singers".into()),
        )
        .unwrap();
    ingest.bind(ingest_rows).expect("bind ingest rows");
    let ingest_err = ingest
        .execute_update()
        .expect_err("ingest on a read-only connection must fail");
    assert_eq!(
        ingest_err.status,
        Status::InvalidState,
        "ingest on a read-only connection must be InvalidState, got: {ingest_err:?}"
    );

    // A statement created while read-only becomes writable when the flag is cleared, and
    // read-only again when it is set.
    let mut live = connection.new_statement().expect("new statement");
    connection
        .set_option(
            OptionConnection::ReadOnly,
            OptionValue::String("false".into()),
        )
        .expect("disable readonly");
    assert_eq!(
        connection
            .get_option_string(OptionConnection::ReadOnly)
            .expect("read readonly back"),
        "false",
        "setting adbc.connection.readonly=false round-trips through get_option"
    );
    live.set_sql_query("DELETE FROM Singers WHERE false")
        .unwrap();
    assert_eq!(
        live.execute_update()
            .expect("a pre-existing statement can write once readonly is cleared"),
        Some(0),
        "the read-only flag is live: clearing it on the connection immediately frees a statement \
         created while it was set"
    );

    connection
        .set_option(
            OptionConnection::ReadOnly,
            OptionValue::String("true".into()),
        )
        .expect("re-enable readonly");
    live.set_sql_query("DELETE FROM Singers WHERE true")
        .unwrap();
    let relock_err = live
        .execute_update()
        .expect_err("re-enabling readonly must immediately affect an existing statement");
    assert_eq!(
        relock_err.status,
        Status::InvalidState,
        "the read-only flag is live: re-enabling it on the connection immediately locks a \
         pre-existing statement, got: {relock_err:?}"
    );

    connection
        .set_option(
            OptionConnection::ReadOnly,
            OptionValue::String("false".into()),
        )
        .expect("disable readonly again");
}

/// Setting `adbc.connection.readonly` on another thread locks a statement created earlier.
#[test]
fn readonly_toggle_from_another_thread_locks_existing_statement() {
    use std::sync::mpsc;

    let Some(fx) = fixture() else {
        return;
    };
    let mut connection = fx.connection;

    let mut dml = connection.new_statement().expect("new statement");
    dml.set_sql_query("DELETE FROM Singers WHERE false")
        .unwrap();
    assert_eq!(
        dml.execute_update()
            .expect("a fresh statement on a writable connection can write"),
        Some(0),
        "the DML runs as a no-op while the connection is writable"
    );

    let (ready_tx, ready_rx) = mpsc::channel::<()>();
    let toggler = std::thread::spawn(move || {
        connection
            .set_option(
                OptionConnection::ReadOnly,
                OptionValue::String("true".into()),
            )
            .expect("enable readonly from another thread");
        ready_tx.send(()).expect("signal readonly set");
        connection
    });
    ready_rx.recv().expect("wait for the readonly toggle");

    let relock_err = dml
        .execute_update()
        .expect_err("readonly toggled from another thread must lock a pre-existing statement");
    assert_eq!(
        relock_err.status,
        Status::InvalidState,
        "the read-only flag propagates across threads: a toggle on the connection from another \
         thread immediately locks a statement created earlier on this thread, got: {relock_err:?}"
    );

    let mut connection = toggler.join().expect("toggler thread must not panic");
    connection
        .set_option(
            OptionConnection::ReadOnly,
            OptionValue::String("false".into()),
        )
        .expect("disable readonly");
}

/// `rollback()` fails in autocommit mode and is a no-op in an empty manual transaction.
#[test]
fn rollback_without_a_transaction_is_invalid_state() {
    let Some(mut fx) = fixture_unguarded() else {
        return;
    };
    let connection = &mut fx.connection;

    let error = connection
        .rollback()
        .expect_err("rollback in autocommit mode must fail");
    assert_eq!(error.status, Status::InvalidState, "got: {error:?}");
    assert!(
        error.message.contains("autocommit"),
        "the error should say why there is no transaction, got: {}",
        error.message
    );

    set_autocommit(connection, false);
    connection
        .rollback()
        .expect("rollback with nothing buffered is a no-op");
    connection
        .rollback()
        .expect("a repeated empty rollback is still a no-op");
    assert_eq!(
        connection
            .get_option_string(OptionConnection::AutoCommit)
            .expect("get autocommit"),
        "false",
        "rollback discards buffered work, not the transaction mode"
    );

    set_autocommit(connection, true);
    assert_eq!(
        connection
            .rollback()
            .expect_err("rollback must fail again in autocommit mode")
            .status,
        Status::InvalidState
    );
}

/// `get_statistic_names` is empty because Spanner has no named statistics.
#[test]
fn get_statistic_names_is_empty_and_correctly_typed() {
    let Some(mut fx) = fixture_unguarded() else {
        return;
    };
    let connection = &mut fx.connection;

    let mut reader = connection
        .get_statistic_names()
        .expect("get_statistic_names");
    let schema = reader.schema();
    assert_eq!(
        schema,
        adbc_core::schemas::GET_STATISTIC_NAMES_SCHEMA.clone(),
        "the reader must carry the canonical GET_STATISTIC_NAMES_SCHEMA"
    );
    assert!(
        reader.next().is_none(),
        "expected an empty result set (no batches at all)"
    );
}

/// A connection cancel between `read_partition` chunk fetches cancels the next fetch, does not
/// affect statements, and is cleared by the connection's next operation.
#[test]
fn connection_cancel_is_sticky_until_the_next_operation() {
    let Some(mut fx) = fixture() else {
        return;
    };
    let connection = &mut fx.connection;

    run(
        connection,
        "DROP TABLE IF EXISTS AdbcConnCancel; \
         CREATE TABLE AdbcConnCancel (Id INT64) PRIMARY KEY (Id)",
    );
    run(
        connection,
        "INSERT INTO AdbcConnCancel (Id) \
         SELECT n FROM UNNEST(GENERATE_ARRAY(1, 200)) AS n",
    );

    let mut statement = connection.new_statement().expect("new statement");
    statement
        .set_sql_query("SELECT Id FROM AdbcConnCancel")
        .unwrap();
    let partitioned = statement.execute_partitions().expect("execute_partitions");
    // The emulator can return empty partitions; find one with rows. Descriptors can be reread.
    let descriptor = partitioned
        .partitions
        .iter()
        .find(|token| {
            connection
                .read_partition(token)
                .expect("probe read_partition")
                .next()
                .is_some()
        })
        .expect("at least one non-empty partition");

    let mut reader = connection
        .read_partition(descriptor)
        .expect("read_partition");
    let first = reader
        .next()
        .expect("first batch")
        .expect("first batch is ok");
    assert!(first.num_rows() <= 200);
    connection.get_cancel_handle().try_cancel().expect("cancel");
    let error = reader
        .next()
        .expect("the cancelled fetch yields an item")
        .expect_err("the fetch after cancel must fail");
    assert_eq!(reader_error_status(&error), Status::Cancelled, "{error}");

    // Statements have their own cancel signal.
    assert_eq!(count_rows(connection, "AdbcConnCancel"), 200);

    let mut total = 0usize;
    for token in &partitioned.partitions {
        let reader = connection
            .read_partition(token)
            .expect("read_partition after cancel");
        for batch in reader {
            total += batch.expect("partition batch").num_rows();
        }
    }
    assert_eq!(
        total, 200,
        "after the reset the connection must stream every partition normally"
    );

    // A cancel with nothing in flight does not affect the next metadata call.
    connection
        .get_cancel_handle()
        .try_cancel()
        .expect("cancel with nothing in flight");
    let schema = connection
        .get_table_schema(None, None, "AdbcConnCancel")
        .expect("get_table_schema after a stale cancel");
    assert_eq!(schema.fields().len(), 1);

    drop_tables(connection, &["AdbcConnCancel"]);
}

/// Priority and tag options are inherited by statements and produce valid requests. The emulator
/// ignores them, so this only checks that requests succeed.
#[test]
fn request_priority_and_tags() {
    use spanner_adbc::{OPTION_REQUEST_PRIORITY, OPTION_REQUEST_TAG, OPTION_TRANSACTION_TAG};

    let Some(mut fx) = fixture() else {
        return;
    };
    let connection = &mut fx.connection;

    let conn_key = |k: &str| OptionConnection::Other(k.into());
    let stmt_key = |k: &str| OptionStatement::Other(k.into());

    connection
        .set_option(
            conn_key(OPTION_REQUEST_PRIORITY),
            OptionValue::String("medium".into()),
        )
        .expect("set connection priority");
    connection
        .set_option(
            conn_key(OPTION_REQUEST_TAG),
            OptionValue::String("adbc-test-request".into()),
        )
        .expect("set connection request tag");
    connection
        .set_option(
            conn_key(OPTION_TRANSACTION_TAG),
            OptionValue::String("adbc-test-txn".into()),
        )
        .expect("set connection transaction tag");
    assert_eq!(
        connection
            .get_option_string(conn_key(OPTION_REQUEST_PRIORITY))
            .unwrap(),
        "medium"
    );
    assert_eq!(
        connection
            .get_option_string(conn_key(OPTION_TRANSACTION_TAG))
            .unwrap(),
        "adbc-test-txn"
    );
    let mut statement = connection.new_statement().expect("new statement");
    assert_eq!(
        statement
            .get_option_string(stmt_key(OPTION_REQUEST_TAG))
            .unwrap(),
        "adbc-test-request",
        "the statement must inherit the connection's request tag"
    );
    statement
        .set_option(
            stmt_key(OPTION_REQUEST_PRIORITY),
            OptionValue::String("high".into()),
        )
        .expect("override priority on the statement");
    statement
        .set_option(
            stmt_key(OPTION_REQUEST_TAG),
            OptionValue::String(String::new()),
        )
        .expect("unset the inherited request tag with an empty value");

    run(
        connection,
        "DROP TABLE IF EXISTS AdbcReqOpts; \
         CREATE TABLE AdbcReqOpts (Id INT64) PRIMARY KEY (Id)",
    );
    let mut insert = connection.new_statement().expect("new statement");
    insert
        .set_sql_query("INSERT INTO AdbcReqOpts (Id) VALUES (1), (2)")
        .unwrap();
    assert_eq!(insert.execute_update().expect("tagged insert"), Some(2));
    assert_eq!(count_rows(connection, "AdbcReqOpts"), 2);

    statement
        .set_sql_query("SELECT Id FROM AdbcReqOpts ORDER BY Id")
        .unwrap();
    let batches = statement
        .execute()
        .expect("tagged query")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);

    let row = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("Id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![3]))],
    )
    .unwrap();
    let mut bound = connection.new_statement().expect("new statement");
    bound
        .set_sql_query("INSERT INTO AdbcReqOpts (Id) VALUES (@Id)")
        .unwrap();
    bound.bind(row).expect("bind param");
    assert_eq!(
        bound.execute_update().expect("tagged bound insert"),
        Some(1)
    );
    assert_eq!(count_rows(connection, "AdbcReqOpts"), 3);

    let error = statement
        .set_option(
            stmt_key(OPTION_TRANSACTION_TAG),
            OptionValue::String("nope".into()),
        )
        .expect_err("the transaction tag must not be settable on a statement");
    assert_eq!(error.status, Status::NotImplemented);

    drop_tables(connection, &["AdbcReqOpts"]);
}

/// A projected expression and the Arrow field every schema path must report for it.
struct SchemaFidelityCase {
    name: &'static str,
    expr: &'static str,
    field: Field,
}

/// Every mapped Spanner type: scalars, arrays of each, and arrays of structs with nested structs
/// and arrays.
fn schema_fidelity_cases() -> Vec<SchemaFidelityCase> {
    let json_metadata = || {
        std::collections::HashMap::from([
            ("ARROW:extension:name".to_string(), "arrow.json".to_string()),
            ("ARROW:extension:metadata".to_string(), String::new()),
        ])
    };
    let timestamp = || DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()));
    let list = |item: Field| DataType::List(Arc::new(item));
    let item = |data_type: DataType| Field::new("item", data_type, true);
    let case = |name: &'static str, expr: &'static str, data_type: DataType| SchemaFidelityCase {
        name,
        expr,
        field: Field::new(name, data_type, true),
    };
    let nested_child = DataType::Struct(
        vec![
            Field::new("d", DataType::Date32, true),
            Field::new("xs", list(item(DataType::Int64)), true),
        ]
        .into(),
    );
    vec![
        case("BoolCol", "TRUE", DataType::Boolean),
        case("Int64Col", "1", DataType::Int64),
        case("Float64Col", "1.5", DataType::Float64),
        case("Float32Col", "CAST(1.25 AS FLOAT32)", DataType::Float32),
        case("StringCol", "'x'", DataType::Utf8),
        case(
            "UuidCol",
            "CAST('11111111-2222-3333-4444-555555555555' AS UUID)",
            DataType::Utf8,
        ),
        case("BytesCol", "b'xyz'", DataType::Binary),
        case("DateCol", "DATE '2024-01-15'", DataType::Date32),
        case(
            "TsCol",
            "TIMESTAMP '2024-01-15T12:34:56.789012Z'",
            timestamp(),
        ),
        case("NumCol", "NUMERIC '1.5'", DataType::Decimal128(38, 9)),
        SchemaFidelityCase {
            name: "JsonCol",
            expr: r#"JSON '{"a":1}'"#,
            field: Field::new("JsonCol", DataType::Utf8, true).with_metadata(json_metadata()),
        },
        case("ArrBool", "[TRUE, FALSE]", list(item(DataType::Boolean))),
        case("ArrInt64", "[1, 2, 3]", list(item(DataType::Int64))),
        case("ArrFloat64", "[1.5, 2.5]", list(item(DataType::Float64))),
        case(
            "ArrFloat32",
            "[CAST(1.25 AS FLOAT32)]",
            list(item(DataType::Float32)),
        ),
        case("ArrStr", "['a', 'b']", list(item(DataType::Utf8))),
        case(
            "ArrUuid",
            "[CAST('11111111-2222-3333-4444-555555555555' AS UUID)]",
            list(item(DataType::Utf8)),
        ),
        case("ArrBytes", "[b'x']", list(item(DataType::Binary))),
        case(
            "ArrDate",
            "[DATE '2024-01-15']",
            list(item(DataType::Date32)),
        ),
        case(
            "ArrTs",
            "[TIMESTAMP '2024-01-15T12:34:56Z']",
            list(item(timestamp())),
        ),
        case(
            "ArrNum",
            "[NUMERIC '2.5']",
            list(item(DataType::Decimal128(38, 9))),
        ),
        SchemaFidelityCase {
            name: "ArrJson",
            expr: r#"[JSON '1', JSON '{"b":2}']"#,
            field: Field::new(
                "ArrJson",
                list(item(DataType::Utf8).with_metadata(json_metadata())),
                true,
            ),
        },
        case(
            "ArrStruct",
            "ARRAY(SELECT AS STRUCT 1 AS id, 'a' AS tag)",
            list(item(DataType::Struct(
                vec![
                    Field::new("id", DataType::Int64, true),
                    Field::new("tag", DataType::Utf8, true),
                ]
                .into(),
            ))),
        ),
        case(
            "ArrNestedStructArray",
            "ARRAY(SELECT AS STRUCT \
                 ARRAY(SELECT AS STRUCT DATE '2024-01-15' AS d, [1, 2] AS xs) AS child, \
                 2.5 AS ratio)",
            list(item(DataType::Struct(
                vec![
                    Field::new("child", list(item(nested_child)), true),
                    Field::new("ratio", DataType::Float64, true),
                ]
                .into(),
            ))),
        ),
    ]
}

/// Zero-row results, `execute_schema` and bound queries report the same schema as a query with
/// rows, for every mapped type. Comparing against expected fields catches all paths degrading to
/// the same fallback.
#[test]
fn zero_row_schema_fidelity() {
    let Some(mut fx) = fixture_unguarded() else {
        return;
    };
    let connection = &mut fx.connection;

    let cases = schema_fidelity_cases();
    let select_list = cases
        .iter()
        .map(|c| format!("{} AS {}", c.expr, c.name))
        .collect::<Vec<_>>()
        .join(", ");
    let with_rows_sql = format!("SELECT {select_list} FROM UNNEST([1]) AS r");
    let zero_rows_sql = format!("SELECT {select_list} FROM UNNEST(ARRAY<INT64>[]) AS r");

    // Compare per column so a failure names the column.
    let assert_expected_schema = |actual: &Schema, path: &str| {
        assert_eq!(
            actual.fields().len(),
            cases.len(),
            "{path}: column count diverges"
        );
        for (actual_field, case) in actual.fields().iter().zip(&cases) {
            assert_eq!(
                actual_field.as_ref(),
                &case.field,
                "{path}: column {} diverges from the expected Arrow field",
                case.name
            );
        }
    };

    let mut with_rows = connection.new_statement().expect("new statement");
    with_rows.set_sql_query(&with_rows_sql).unwrap();
    let reader = with_rows.execute().expect("execute with-rows projection");
    assert_expected_schema(&reader.schema(), "execute (with rows) reader schema");
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("collect with-rows batches");
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    for batch in &batches {
        assert_expected_schema(&batch.schema(), "execute (with rows) batch schema");
    }

    let mut zero_rows = connection.new_statement().expect("new statement");
    zero_rows.set_sql_query(&zero_rows_sql).unwrap();
    let reader = zero_rows.execute().expect("execute zero-row projection");
    assert_expected_schema(&reader.schema(), "execute (zero rows) reader schema");
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("collect zero-row batches");
    assert_eq!(
        batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        0,
        "the zero-row projection must return no rows"
    );
    for batch in &batches {
        assert_expected_schema(&batch.schema(), "execute (zero rows) empty-batch schema");
    }

    let mut plan = connection.new_statement().expect("new statement");
    plan.set_sql_query(&with_rows_sql).unwrap();
    let schema = plan.execute_schema().expect("execute_schema (with rows)");
    assert_expected_schema(&schema, "execute_schema (with rows)");
    plan.set_sql_query(&zero_rows_sql).unwrap();
    let schema = plan.execute_schema().expect("execute_schema (zero rows)");
    assert_expected_schema(&schema, "execute_schema (zero rows)");

    let bound_sql = format!("SELECT {select_list} FROM UNNEST([1]) AS n WHERE n = @N");
    let bind_values = |values: Vec<i64>| {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("N", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(values))],
        )
        .unwrap()
    };

    let mut bound_zero = connection.new_statement().expect("new statement");
    bound_zero.set_sql_query(&bound_sql).unwrap();
    bound_zero
        .bind(bind_values(vec![5, 6, 7]))
        .expect("bind three non-matching rows");
    let reader = bound_zero.execute().expect("bound zero-row query");
    assert_expected_schema(&reader.schema(), "bound query (zero rows) reader schema");
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("collect bound zero-row batches");
    assert_eq!(
        batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        0,
        "no bound row matches, so the bound query must return no rows"
    );
    for batch in &batches {
        assert_expected_schema(
            &batch.schema(),
            "bound query (zero rows) empty-batch schema",
        );
    }

    let mut bound_hits = connection.new_statement().expect("new statement");
    bound_hits.set_sql_query(&bound_sql).unwrap();
    bound_hits
        .bind(bind_values(vec![1, 1]))
        .expect("bind two matching rows");
    let reader = bound_hits.execute().expect("bound with-rows query");
    assert_expected_schema(&reader.schema(), "bound query (with rows) reader schema");
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("collect bound with-rows batches");
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
    for batch in &batches {
        assert_expected_schema(&batch.schema(), "bound query (with rows) batch schema");
    }
}

/// RPC timeout options: generous deadlines do not affect operations, and expired ones report
/// `Timeout`. The DDL deadline is tested in `tests/mock_spanner.rs`.
#[test]
fn rpc_timeouts() {
    use spanner_adbc::{
        OPTION_RPC_TIMEOUT_FETCH, OPTION_RPC_TIMEOUT_QUERY, OPTION_RPC_TIMEOUT_UPDATE,
    };

    let Some(mut fx) = fixture() else {
        return;
    };
    let connection = &mut fx.connection;

    let conn_key = |k: &str| OptionConnection::Other(k.into());
    let stmt_key = |k: &str| OptionStatement::Other(k.into());

    // The streamed query uses the query deadline for its first chunk and the fetch deadline after.
    for key in [
        OPTION_RPC_TIMEOUT_QUERY,
        OPTION_RPC_TIMEOUT_UPDATE,
        OPTION_RPC_TIMEOUT_FETCH,
    ] {
        connection
            .set_option(conn_key(key), OptionValue::Int(30))
            .expect("set a generous deadline");
    }
    run(
        connection,
        "DROP TABLE IF EXISTS AdbcRpcTimeout; \
         CREATE TABLE AdbcRpcTimeout (Id INT64, Value INT64) PRIMARY KEY (Id)",
    );
    let mut insert = connection.new_statement().expect("new statement");
    insert
        .set_sql_query(
            "INSERT INTO AdbcRpcTimeout (Id) SELECT n FROM UNNEST(GENERATE_ARRAY(1, 100)) AS n",
        )
        .unwrap();
    assert_eq!(
        insert.execute_update().expect("bounded insert"),
        Some(100),
        "a generous update deadline must not affect a fast DML"
    );
    let mut query = connection.new_statement().expect("new statement");
    query
        .set_option(
            OptionStatement::Other(spanner_adbc::OPTION_ROWS_PER_BATCH.into()),
            OptionValue::Int(40),
        )
        .expect("small batches so the fetch deadline path is exercised across chunks");
    query
        .set_sql_query("SELECT Id FROM AdbcRpcTimeout ORDER BY Id")
        .unwrap();
    let batches = query
        .execute()
        .expect("bounded streaming query")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect bounded streamed batches");
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        100
    );
    assert!(
        batches.len() > 1,
        "expected several chunks so the fetch deadline actually bounded later fetches"
    );

    let mut tiny = connection.new_statement().expect("new statement");
    tiny.set_option(
        stmt_key(OPTION_RPC_TIMEOUT_QUERY),
        OptionValue::String("0.000001".into()),
    )
    .expect("set a microsecond query deadline");
    tiny.set_sql_query("SELECT Id FROM AdbcRpcTimeout ORDER BY Id")
        .unwrap();
    // The query deadline covers the first chunk, so `execute` itself fails. A microsecond
    // deadline fires on the next ~1ms timer tick, which a fast backend occasionally beats, so
    // each microsecond deadline below gets a few attempts.
    let Some(error) = (0..5).find_map(|_| tiny.execute().err()) else {
        panic!("a microsecond query deadline must expire");
    };
    assert_eq!(error.status, Status::Timeout, "{error:?}");
    assert!(
        error.message.contains(OPTION_RPC_TIMEOUT_QUERY),
        "the timeout error must name the responsible option: {}",
        error.message
    );

    let mut tiny_dml = connection.new_statement().expect("new statement");
    tiny_dml
        .set_option(
            stmt_key(OPTION_RPC_TIMEOUT_UPDATE),
            OptionValue::Double(0.000001),
        )
        .expect("set a microsecond update deadline");
    tiny_dml
        .set_sql_query("UPDATE AdbcRpcTimeout SET Value = Value WHERE FALSE")
        .unwrap();
    let Some(error) = (0..5).find_map(|_| tiny_dml.execute_update().err()) else {
        panic!("a microsecond update deadline must expire");
    };
    assert_eq!(error.status, Status::Timeout, "{error:?}");
    assert!(
        error.message.contains(OPTION_RPC_TIMEOUT_UPDATE),
        "{}",
        error.message
    );

    tiny.set_option(stmt_key(OPTION_RPC_TIMEOUT_QUERY), OptionValue::Int(30))
        .expect("raise the query deadline");
    let batches = tiny
        .execute()
        .expect("query after a raised deadline")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        100
    );
    tiny_dml
        .set_option(stmt_key(OPTION_RPC_TIMEOUT_UPDATE), OptionValue::Int(30))
        .expect("raise the update deadline");
    assert_eq!(
        tiny_dml
            .execute_update()
            .expect("DML after a raised deadline"),
        Some(0)
    );

    // An expiring DDL deadline is not tested here: the emulator answers DDL faster than tokio's
    // ~1ms timer resolution, so the result would be racy.

    // The query deadline also bounds internal metadata reads.
    connection
        .set_option(
            conn_key(OPTION_RPC_TIMEOUT_QUERY),
            OptionValue::String("0.000001".into()),
        )
        .expect("set a microsecond query deadline on the connection");
    let Some(error) = (0..5).find_map(|_| {
        connection
            .get_objects(ObjectDepth::All, None, None, None, None, None)
            .err()
    }) else {
        panic!("a microsecond query deadline must expire on a metadata read");
    };
    assert_eq!(error.status, Status::Timeout, "{error:?}");
    assert!(
        error.message.contains(OPTION_RPC_TIMEOUT_QUERY),
        "the metadata timeout error must name the query option: {}",
        error.message
    );
    connection
        .set_option(conn_key(OPTION_RPC_TIMEOUT_QUERY), OptionValue::Int(30))
        .expect("raise the connection query deadline");
    connection
        .get_objects(ObjectDepth::All, None, None, None, None, None)
        .expect("get_objects after a raised deadline")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect get_objects batches");

    drop_tables(connection, &["AdbcRpcTimeout"]);
}

/// Change stream DDL, metadata and `READ_` queries work through plain SQL.
///
/// Reading data change records would mean following child partition tokens, which is timing
/// sensitive, so this only checks the first read's schema and that it returns a row.
#[test]
fn change_stream_via_plain_sql() {
    let Some(mut fx) = fixture() else {
        return;
    };
    let connection = &mut fx.connection;

    // A stream left by a failed run blocks dropping the table it tracks, and DDL has no
    // `DROP CHANGE STREAM IF EXISTS`.
    if count_rows(
        connection,
        "INFORMATION_SCHEMA.CHANGE_STREAMS WHERE CHANGE_STREAM_NAME = 'AdbcChangeStreamCs'",
    ) > 0
    {
        run(connection, "DROP CHANGE STREAM AdbcChangeStreamCs");
    }
    run(
        connection,
        "DROP TABLE IF EXISTS AdbcChangeStream; \
         CREATE TABLE AdbcChangeStream (Id INT64, Name STRING(MAX)) PRIMARY KEY (Id)",
    );
    run(
        connection,
        "CREATE CHANGE STREAM AdbcChangeStreamCs FOR AdbcChangeStream",
    );

    let mut streams = connection.new_statement().expect("new statement");
    streams
        .set_sql_query(
            "SELECT CHANGE_STREAM_NAME FROM INFORMATION_SCHEMA.CHANGE_STREAMS \
             WHERE CHANGE_STREAM_NAME = 'AdbcChangeStreamCs'",
        )
        .unwrap();
    let stream_batches: Vec<_> = streams
        .execute()
        .expect("query change stream metadata")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect change stream metadata");
    let stream_rows: usize = stream_batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        stream_rows, 1,
        "the change stream should be listed exactly once"
    );
    let names = col::<StringArray>(stream_batches[0].column(0));
    assert_eq!(names.value(0), "AdbcChangeStreamCs");

    let mut tables = connection.new_statement().expect("new statement");
    tables
        .set_sql_query(
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.CHANGE_STREAM_TABLES \
             WHERE CHANGE_STREAM_NAME = 'AdbcChangeStreamCs'",
        )
        .unwrap();
    let table_batches: Vec<_> = tables
        .execute()
        .expect("query change stream tables")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect change stream tables");
    let watched: Vec<String> = table_batches
        .iter()
        .flat_map(|b| {
            let col = col::<StringArray>(b.column(0));
            (0..col.len())
                .map(|i| col.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(
        watched,
        vec!["AdbcChangeStream".to_string()],
        "the change stream should watch exactly the AdbcChangeStream table"
    );

    run(
        connection,
        "INSERT INTO AdbcChangeStream (Id, Name) VALUES (1, 'alpha'), (2, 'beta')",
    );

    // The emulator keeps no history, so the window must start at or after now.
    let start = chrono::Utc::now() + chrono::Duration::milliseconds(500);
    let end = chrono::Utc::now() + chrono::Duration::seconds(3);
    let start_lit = start.to_rfc3339_opts(SecondsFormat::Nanos, true);
    let end_lit = end.to_rfc3339_opts(SecondsFormat::Nanos, true);
    let read_sql = format!(
        "SELECT ChangeRecord FROM READ_AdbcChangeStreamCs (\
             start_timestamp => TIMESTAMP '{start_lit}', \
             end_timestamp => TIMESTAMP '{end_lit}', \
             partition_token => NULL, \
             heartbeat_milliseconds => 2000)"
    );
    let mut reader_stmt = connection.new_statement().expect("new statement");
    reader_stmt.set_sql_query(read_sql).unwrap();
    let reader = reader_stmt.execute().expect("read change stream TVF");
    let schema = reader.schema();
    assert_eq!(schema.fields().len(), 1);
    let change_field = schema.field(0);
    assert_eq!(change_field.name(), "ChangeRecord");
    let DataType::List(elem) = change_field.data_type() else {
        panic!(
            "ChangeRecord should map to a List, got {:?}",
            change_field.data_type()
        );
    };
    let DataType::Struct(record_fields) = elem.data_type() else {
        panic!(
            "ChangeRecord element should be a Struct, got {:?}",
            elem.data_type()
        );
    };
    let record_field_names: Vec<&str> = record_fields.iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        record_field_names,
        vec![
            "data_change_record",
            "heartbeat_record",
            "child_partitions_record"
        ],
        "the change record struct should expose all three record variants"
    );

    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("collect change stream records");
    let record_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(
        record_rows >= 1,
        "the initial change-stream read should yield at least the child-partition record"
    );

    run(connection, "DROP CHANGE STREAM AdbcChangeStreamCs");
    run(connection, "DROP TABLE AdbcChangeStream");
}

/// Opt-in keyfile and impersonation auth tests against Cloud Spanner. They always use
/// `SPANNER_GCP_DATABASE`, because the emulator rejects these credentials, and also need
/// `SPANNER_TEST_KEYFILE` or `SPANNER_TEST_IMPERSONATE_TARGET_PRINCIPAL`.
mod auth_end_to_end {
    use super::*;
    use spanner_adbc::{OPTION_IMPERSONATE_TARGET_PRINCIPAL, OPTION_KEYFILE};

    fn non_empty_env(name: &str) -> Option<String> {
        std::env::var(name).ok().filter(|s| !s.is_empty())
    }

    fn assert_select_one(connection: &mut SpannerConnection) {
        let mut statement = connection.new_statement().expect("new statement");
        statement
            .set_sql_query("SELECT 1 AS one")
            .expect("set query");
        let batches: Vec<_> = statement
            .execute()
            .expect("execute SELECT 1 over the authenticated connection")
            .collect::<Result<Vec<_>, _>>()
            .expect("read batches");
        assert_eq!(batches.len(), 1);
        let ones = col::<Int64Array>(batches[0].column(0));
        assert_eq!(ones.value(0), 1);
    }

    #[test]
    fn keyfile_auth_end_to_end() {
        let Some(target) = gcp_target() else {
            eprintln!("SPANNER_GCP_DATABASE not set; skipping keyfile auth end-to-end test");
            return;
        };
        let Some(keyfile) = non_empty_env("SPANNER_TEST_KEYFILE") else {
            eprintln!("SPANNER_TEST_KEYFILE not set; skipping keyfile auth end-to-end test");
            return;
        };

        // Setup uses ADC, not the keyfile under test.
        ensure_database_once(&target, SETUP_DDL);

        let mut driver = SpannerDriver::try_new().expect("create driver");
        let database = driver
            .new_database_with_opts([
                (
                    OptionDatabase::Uri,
                    OptionValue::String(target.database_uri()),
                ),
                (
                    OptionDatabase::Other(OPTION_KEYFILE.into()),
                    OptionValue::String(keyfile),
                ),
            ])
            .expect("create database with keyfile credentials");
        let mut connection = connect_with_retry(&database);
        assert_select_one(&mut connection);
    }

    /// The ADC identity needs the Token Creator role on the target principal.
    #[test]
    fn impersonation_auth_end_to_end() {
        let Some(target) = gcp_target() else {
            eprintln!("SPANNER_GCP_DATABASE not set; skipping impersonation auth end-to-end test");
            return;
        };
        let Some(principal) = non_empty_env("SPANNER_TEST_IMPERSONATE_TARGET_PRINCIPAL") else {
            eprintln!(
                "SPANNER_TEST_IMPERSONATE_TARGET_PRINCIPAL not set; skipping impersonation auth \
                 end-to-end test"
            );
            return;
        };

        ensure_database_once(&target, SETUP_DDL);

        let mut driver = SpannerDriver::try_new().expect("create driver");
        let database = driver
            .new_database_with_opts([
                (
                    OptionDatabase::Uri,
                    OptionValue::String(target.database_uri()),
                ),
                (
                    OptionDatabase::Other(OPTION_IMPERSONATE_TARGET_PRINCIPAL.into()),
                    OptionValue::String(principal),
                ),
            ])
            .expect("create database with impersonation credentials");
        let mut connection = connect_with_retry(&database);
        assert_select_one(&mut connection);
    }
}

/// `adbc.test.Point` from [`adbc_test_proto_descriptors`].
#[derive(Clone, PartialEq, ::prost::Message)]
struct TestPoint {
    #[prost(int64, tag = "1")]
    pub x: i64,
    #[prost(int64, tag = "2")]
    pub y: i64,
    #[prost(string, tag = "3")]
    pub label: ::prost::alloc::string::String,
}

/// A `FileDescriptorSet` defining `adbc.test.Color` and `adbc.test.Point`, built by hand to avoid
/// a protoc step.
fn adbc_test_proto_descriptors() -> Vec<u8> {
    use prost::Message as _;
    use prost_types::field_descriptor_proto::{Label, Type as FieldType};
    use prost_types::{
        DescriptorProto, EnumDescriptorProto, EnumValueDescriptorProto, FieldDescriptorProto,
        FileDescriptorProto, FileDescriptorSet,
    };
    let enum_value = |name: &str, number: i32| EnumValueDescriptorProto {
        name: Some(name.to_string()),
        number: Some(number),
        options: None,
    };
    let field = |name: &str, number: i32, ty: FieldType| FieldDescriptorProto {
        name: Some(name.to_string()),
        number: Some(number),
        label: Some(Label::Optional as i32),
        r#type: Some(ty as i32),
        ..Default::default()
    };
    let file_descriptor_set = FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some("adbc_test.proto".to_string()),
            package: Some("adbc.test".to_string()),
            syntax: Some("proto3".to_string()),
            enum_type: vec![EnumDescriptorProto {
                name: Some("Color".to_string()),
                value: vec![
                    enum_value("COLOR_UNSPECIFIED", 0),
                    enum_value("RED", 1),
                    enum_value("GREEN", 2),
                    enum_value("BLUE", 3),
                ],
                ..Default::default()
            }],
            message_type: vec![DescriptorProto {
                name: Some("Point".to_string()),
                field: vec![
                    field("x", 1, FieldType::Int64),
                    field("y", 2, FieldType::Int64),
                    field("label", 3, FieldType::String),
                ],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let mut descriptors = Vec::new();
    file_descriptor_set.encode(&mut descriptors).unwrap();
    descriptors
}

/// Register the proto bundle used by the ENUM and PROTO tests, once per binary. The driver's DDL
/// path cannot pass `proto_descriptors`, and a database has only one bundle.
///
/// Transient admin errors (seen on CI) are retried; otherwise the later table DDL fails with an
/// obscure `Unrecognized ddl::ColumnDefinition` error.
fn ensure_proto_bundle_once(target: &TestTarget) {
    use google_cloud_gax::error::rpc::Code;

    static DONE: Mutex<bool> = Mutex::new(false);
    let mut done = DONE.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    if *done {
        return;
    }
    tokio::runtime::Runtime::new()
        .expect("failed to build setup runtime")
        .block_on(async {
            if let Some(endpoint) = &target.omni_endpoint {
                let result = omni::update_ddl(
                    endpoint,
                    target.database_path(),
                    vec!["CREATE PROTO BUNDLE (`adbc.test.Color`, `adbc.test.Point`)".to_string()],
                    adbc_test_proto_descriptors(),
                )
                .await;
                match result {
                    Ok(()) => return,
                    Err(error) if error.code() == tonic::Code::AlreadyExists => return,
                    Err(error) => panic!("register Omni proto bundle: {error}"),
                }
            }
            let spanner = target.spanner().await;
            let database_admin = spanner
                .database_admin_builder()
                .build()
                .await
                .expect("failed to build database admin client");
            let mut last_err = None;
            for attempt in 0..5 {
                match database_admin
                    .update_database_ddl()
                    .set_database(target.database_path())
                    .set_statements(vec![
                        "CREATE PROTO BUNDLE (`adbc.test.Color`, `adbc.test.Point`)".to_string(),
                    ])
                    .set_proto_descriptors(adbc_test_proto_descriptors())
                    .poller()
                    .until_done()
                    .await
                {
                    Ok(_) => return,
                    Err(e) if e.status().map(|s| s.code) == Some(Code::AlreadyExists) => return,
                    Err(e) => {
                        eprintln!("proto-bundle setup attempt {} failed: {e}", attempt + 1);
                        last_err = Some(e);
                        tokio::time::sleep(std::time::Duration::from_millis(500 * (attempt + 1)))
                            .await;
                    }
                }
            }
            panic!(
                "failed to register the shared proto bundle after retries: {}",
                last_err.expect("loop runs at least once before panicking")
            );
        });
    *done = true;
}

/// `ENUM` maps to its `Int64` ordinal and `ARRAY<ENUM>` to `List<Int64>`; `CAST(col AS STRING)`
/// gives the labels.
#[test]
fn enum_columns_round_trip_as_int64_ordinals() {
    let Some(mut fx) = fixture() else {
        return;
    };
    ensure_proto_bundle_once(&fx.target);
    let connection = &mut fx.connection;

    run(
        connection,
        "DROP TABLE IF EXISTS AdbcEnum; \
         CREATE TABLE AdbcEnum (\
         Id INT64, \
         Fav `adbc.test.Color`, \
         Faves ARRAY<`adbc.test.Color`>\
         ) PRIMARY KEY (Id)",
    );

    let mut insert = connection.new_statement().expect("new statement");
    insert
        .set_sql_query(
            "INSERT INTO AdbcEnum (Id, Fav, Faves) VALUES \
             (1, CAST(2 AS `adbc.test.Color`), \
                 [CAST(1 AS `adbc.test.Color`), CAST(3 AS `adbc.test.Color`)]), \
             (2, CAST(NULL AS `adbc.test.Color`), [CAST(2 AS `adbc.test.Color`)])",
        )
        .unwrap();
    assert_eq!(insert.execute_update().expect("insert enum rows"), Some(2));

    let mut query = connection.new_statement().expect("new statement");
    query
        .set_sql_query("SELECT Fav FROM AdbcEnum ORDER BY Id")
        .unwrap();
    let reader = query.execute().expect("query scalar enum");
    let schema = reader.schema();
    assert_eq!(
        schema.field(0).data_type(),
        &DataType::Int64,
        "a scalar ENUM column should map to Arrow Int64"
    );
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("collect scalar enum");
    let fav = col::<Int64Array>(batches[0].column(0));
    assert_eq!(fav.len(), 2);
    assert_eq!(fav.value(0), 2, "GREEN has ordinal 2");
    assert!(fav.is_null(1), "a NULL enum is a null slot, not 0");

    let mut array_query = connection.new_statement().expect("new statement");
    array_query
        .set_sql_query("SELECT Faves FROM AdbcEnum WHERE Id = 1")
        .unwrap();
    let reader = array_query.execute().expect("query array enum");
    let DataType::List(item) = reader.schema().field(0).data_type().clone() else {
        panic!(
            "ARRAY<ENUM> should map to a List data type, got {:?}",
            reader.schema().field(0).data_type()
        );
    };
    assert_eq!(
        item.data_type(),
        &DataType::Int64,
        "ARRAY<ENUM> should map to List<Int64>"
    );
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("collect array enum");
    let list = col::<ListArray>(batches[0].column(0));
    let elems = list.value(0);
    let elems = col::<Int64Array>(&elems);
    assert_eq!(
        (0..elems.len()).map(|i| elems.value(i)).collect::<Vec<_>>(),
        vec![1, 3],
        "[RED, BLUE] are ordinals [1, 3]"
    );

    let mut label_query = connection.new_statement().expect("new statement");
    label_query
        .set_sql_query("SELECT CAST(Fav AS STRING) AS FavName FROM AdbcEnum ORDER BY Id")
        .unwrap();
    let reader = label_query.execute().expect("query enum labels");
    assert_eq!(
        reader.schema().field(0).data_type(),
        &DataType::Utf8,
        "CAST(enum AS STRING) is a plain STRING column"
    );
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("collect enum labels");
    let names = col::<StringArray>(batches[0].column(0));
    assert_eq!(
        names.value(0),
        "GREEN",
        "ordinal 2 casts to the label GREEN"
    );
    assert!(names.is_null(1), "a NULL enum casts to a NULL label");
}

/// `PROTO` maps to `Binary` and `ARRAY<PROTO>` to `List<Binary>`, holding the serialized messages.
#[test]
fn proto_columns_round_trip_as_binary() {
    let Some(mut fx) = fixture() else {
        return;
    };
    ensure_proto_bundle_once(&fx.target);
    use prost::Message as _;
    let connection = &mut fx.connection;

    run(
        connection,
        "DROP TABLE IF EXISTS AdbcProto; \
         CREATE TABLE AdbcProto (\
         Id INT64, \
         P `adbc.test.Point`, \
         Ps ARRAY<`adbc.test.Point`>\
         ) PRIMARY KEY (Id)",
    );

    let proto_literal = |point: &TestPoint| {
        let mut bytes = Vec::new();
        point.encode(&mut bytes).unwrap();
        let escaped: String = bytes.iter().map(|b| format!("\\x{b:02x}")).collect();
        format!("CAST(b'{escaped}' AS `adbc.test.Point`)")
    };

    let scalar_point = TestPoint {
        x: 3,
        y: 4,
        label: "p".to_string(),
    };
    let arr0 = TestPoint {
        x: 1,
        y: 2,
        label: "a".to_string(),
    };
    let arr1 = TestPoint {
        x: 5,
        y: 6,
        label: "b".to_string(),
    };

    let mut insert = connection.new_statement().expect("new statement");
    insert
        .set_sql_query(format!(
            "INSERT INTO AdbcProto (Id, P, Ps) VALUES \
             (1, {p}, [{a0}, {a1}]), \
             (2, CAST(NULL AS `adbc.test.Point`), [])",
            p = proto_literal(&scalar_point),
            a0 = proto_literal(&arr0),
            a1 = proto_literal(&arr1),
        ))
        .unwrap();
    assert_eq!(insert.execute_update().expect("insert proto rows"), Some(2));

    let mut query = connection.new_statement().expect("new statement");
    query
        .set_sql_query("SELECT P FROM AdbcProto ORDER BY Id")
        .unwrap();
    let reader = query.execute().expect("query scalar proto");
    assert_eq!(
        reader.schema().field(0).data_type(),
        &DataType::Binary,
        "a scalar PROTO column should map to Arrow Binary"
    );
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("collect scalar proto");
    let points = col::<BinaryArray>(batches[0].column(0));
    assert_eq!(points.len(), 2);
    assert_eq!(
        TestPoint::decode(points.value(0)).expect("decode row 0 proto bytes"),
        scalar_point,
        "the returned bytes decode back to the inserted message"
    );
    assert!(
        points.is_null(1),
        "a NULL proto is a null slot, not empty bytes"
    );

    let mut array_query = connection.new_statement().expect("new statement");
    array_query
        .set_sql_query("SELECT Ps FROM AdbcProto WHERE Id = 1")
        .unwrap();
    let reader = array_query.execute().expect("query array proto");
    let DataType::List(item) = reader.schema().field(0).data_type().clone() else {
        panic!(
            "ARRAY<PROTO> should map to a List data type, got {:?}",
            reader.schema().field(0).data_type()
        );
    };
    assert_eq!(
        item.data_type(),
        &DataType::Binary,
        "ARRAY<PROTO> should map to List<Binary>"
    );
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("collect array proto");
    let list = col::<ListArray>(batches[0].column(0));
    let elems = list.value(0);
    let elems = col::<BinaryArray>(&elems);
    let decoded: Vec<TestPoint> = (0..elems.len())
        .map(|i| TestPoint::decode(elems.value(i)).expect("decode array proto element"))
        .collect();
    assert_eq!(
        decoded,
        vec![arr0, arr1],
        "the array elements decode back to the inserted messages, in order"
    );
}

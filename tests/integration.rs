//! End-to-end tests against the Spanner emulator, Spanner Omni or a Cloud Spanner database.
//! Tests skip unless a target is configured; see `docs/testing.md`.
//!
//! Setup uses the Spanner admin clients directly; everything else goes through the driver.

use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, OnceLock};

use adbc_core::error::{Result as AdbcResult, Status};
use adbc_core::options::{
    AdbcVersion, InfoCode, ObjectDepth, OptionConnection, OptionDatabase, OptionStatement,
    OptionValue,
};
use adbc_core::{Connection, Driver, Optionable, Statement};
use adbc_driver_manager::ManagedDriver;
use arrow_array::builder::{ListBuilder, StringBuilder};
use arrow_array::{
    Array, ArrayAccessor, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array,
    Float32Array, Float64Array, Int16Array, Int64Array, ListArray, RecordBatch,
    RecordBatchIterator, RecordBatchReader, StringArray, StructArray, TimestampMicrosecondArray,
    TimestampNanosecondArray, UInt32Array, UnionArray,
};
use arrow_schema::{ArrowError, DataType, Field, FieldRef, Fields, Schema, SchemaRef, TimeUnit};
use chrono::{DateTime, NaiveDate, SecondsFormat, Utc};
use google_cloud_lro::Poller;
use proptest::prelude::*;
use spanner_adbc::*;

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

    let host = std::env::var("SPANNER_EMULATOR_HOST").unwrap_or_default();
    let uri = if target.is_emulator {
        format!(
            "spanner:///{}?spanner.endpoint=http%3A%2F%2F{host}&spanner.emulator=true",
            target.database_path()
        )
    } else {
        target.database_uri()
    };

    let mut driver = SpannerDriver::try_new().unwrap();
    let database = driver
        .new_database_with_opts([(OptionDatabase::Uri, OptionValue::String(uri.clone()))])
        .unwrap();

    // `uri` reads back verbatim and its query parameters read back as individual options.
    let option = |key: &str| database.get_option_string(key.into()).unwrap();
    assert_eq!(
        database.get_option_string(OptionDatabase::Uri).unwrap(),
        uri
    );
    if target.is_emulator {
        assert_eq!(option("spanner.endpoint"), format!("http://{host}"));
        assert_eq!(option("spanner.emulator"), "true");
    } else if let Some(endpoint) = &target.omni_endpoint {
        assert_eq!(option(OPTION_INSTANCE_TYPE), "omni");
        assert_eq!(option(OPTION_ENDPOINT), *endpoint);
    }

    let batches = query(&mut connect_with_retry(&database), "SELECT 1 AS one");
    assert_eq!(batches.len(), 1);
    assert_eq!(ints(&batches, 0), [1]);
}

/// A manual transaction that starts with a query pins one read-only snapshot and rejects DML.
/// DDL runs immediately in any transaction state, ahead of buffered DML, and rollback does not
/// undo it.
#[test]
fn manual_transaction_kinds_round_trip() {
    let Some(mut fx) = fixture() else { return };
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
        let sql = format!("INSERT INTO AdbcReadTxn (Id) VALUES ({id})");
        assert_eq!(run(connection, sql), Some(1));
    };
    autocommit_insert(&mut writer, 1);

    set_autocommit(connection, false);

    // The first query pins the snapshot; a later commit elsewhere stays invisible.
    assert_eq!(count_rows(connection, "AdbcReadTxn"), 1);
    autocommit_insert(&mut writer, 2);
    assert_eq!(count_rows(connection, "AdbcReadTxn"), 1);
    // A later failed query must not drop the begun transaction and its snapshot.
    let mut typo = prepare(connection, "SELECT * FROM AdbcReadTxnMissing");
    assert!(typo.execute().is_err());
    assert_eq!(count_rows(connection, "AdbcReadTxn"), 1);

    let error = prepare(connection, "INSERT INTO AdbcReadTxn (Id) VALUES (99)")
        .execute_update()
        .unwrap_err();
    assert_eq!(error.status, Status::InvalidState);
    assert!(error.message.contains("began with a query"), "{error}");

    let ddl = "CREATE TABLE AdbcDdlTxn (Id INT64) PRIMARY KEY (Id)";
    assert_eq!(run(connection, ddl), None);
    assert_eq!(
        count_rows(&mut writer, "AdbcDdlTxn"),
        0,
        "DDL applies before any commit"
    );
    assert_eq!(
        count_rows(connection, "AdbcReadTxn"),
        1,
        "DDL leaves the pinned snapshot alone"
    );

    connection.commit().unwrap();
    assert_eq!(
        count_rows_ending_txn(connection, "AdbcReadTxn"),
        2,
        "a fresh transaction sees the row committed during the previous one"
    );

    assert_eq!(count_rows(connection, "AdbcReadTxn"), 2);
    autocommit_insert(&mut writer, 3);
    assert_eq!(count_rows(connection, "AdbcReadTxn"), 2);
    connection.rollback().unwrap();
    assert_eq!(count_rows_ending_txn(connection, "AdbcReadTxn"), 3);

    // DDL while DML is buffered.
    buffer_dml(connection, "INSERT INTO AdbcReadTxn (Id) VALUES (100)");
    let ddl_batch = "CREATE TABLE AdbcDdlBatch (Id INT64) PRIMARY KEY (Id); \
                     CREATE INDEX AdbcDdlBatchIdx ON AdbcDdlBatch (Id)";
    assert_eq!(run(connection, ddl_batch), None);
    let indexes = query(
        &mut writer,
        "SELECT COUNT(*) AS n FROM INFORMATION_SCHEMA.INDEXES \
         WHERE TABLE_NAME = 'AdbcDdlBatch' AND INDEX_NAME = 'AdbcDdlBatchIdx'",
    );
    assert_eq!(
        ints(&indexes, 0),
        [1],
        "the batch applied before any commit"
    );

    // The DML transaction is still open, and rollback discards only the buffered insert.
    assert_query_guarded(connection, "AdbcReadTxn");
    connection.rollback().unwrap();
    assert_eq!(count_rows_ending_txn(connection, "AdbcReadTxn"), 3);
    assert_eq!(count_rows(&mut writer, "AdbcDdlBatch"), 0);

    set_autocommit(connection, true);
    run(
        connection,
        "DROP INDEX AdbcDdlBatchIdx; DROP TABLE AdbcDdlBatch; \
         DROP TABLE AdbcDdlTxn; DROP TABLE AdbcReadTxn",
    );
}

fn label_rows() -> RecordBatch {
    id_text_batch("Label", &[10, 20], &["x", "y"])
}

fn ingest_labels(
    connection: &mut SpannerConnection,
    table: &str,
    mode: &str,
) -> AdbcResult<Option<i64>> {
    ingest(connection, table, mode, label_rows())
}

/// Basic query and DML round trip with Arrow schema checks.
#[test]
fn query_and_dml_round_trip() {
    let Some(mut fx) = fixture() else { return };
    let connection = &mut fx.connection;

    let table_types = connection.get_table_types().unwrap();
    assert_eq!(table_types.schema().field(0).name(), "table_type");

    run(connection, "DELETE FROM Singers WHERE true");

    let insert = "INSERT INTO Singers (SingerId, Name, Active, Score) \
                  VALUES (1, 'Alice', true, 4.5), (2, 'Bob', false, 3.25)";
    assert_eq!(run(connection, insert), Some(2));

    let reader = prepare(
        connection,
        "SELECT SingerId, Name, Active, Score FROM Singers ORDER BY SingerId",
    )
    .execute()
    .unwrap();
    assert_fields(
        reader.schema().fields(),
        &[
            ("SingerId", DataType::Int64),
            ("Name", DataType::Utf8),
            ("Active", DataType::Boolean),
            ("Score", DataType::Float64),
        ],
    );
    let batches = drain(reader);
    assert_eq!(row_count(&batches), 2);

    let batch = &batches[0];
    let ids = col::<Int64Array>(batch.column(0));
    let names = col::<StringArray>(batch.column(1));
    let active = col::<BooleanArray>(batch.column(2));
    let score = col::<Float64Array>(batch.column(3));
    let row = |i| {
        (
            ids.value(i),
            names.value(i),
            active.value(i),
            score.value(i),
        )
    };
    assert_eq!(row(0), (1, "Alice", true, 4.5));
    assert_eq!(row(1), (2, "Bob", false, 3.25));

    // Autocommit DML is sent with `last_statements=true`; the count and commit must be unaffected.
    let update = "UPDATE Singers SET Score = 9.5 WHERE SingerId = 1";
    assert_eq!(run(connection, update), Some(1));
    let check = query(connection, "SELECT Score FROM Singers WHERE SingerId = 1");
    assert_eq!(col::<Float64Array>(check[0].column(0)).value(0), 9.5);
}

/// `get_table_schema` reports column types, accepts the database id as catalog and rejects
/// hostile table names.
#[test]
fn get_table_schema_reports_column_types() {
    let Some(mut fx) = read_fixture() else { return };
    let catalog = fx.target.database.clone();
    let connection = &mut fx.connection;

    let singers_schema = connection.get_table_schema(None, None, "Singers").unwrap();
    assert_fields(
        singers_schema.fields(),
        &[
            ("SingerId", DataType::Int64),
            ("Name", DataType::Utf8),
            ("Active", DataType::Boolean),
            ("Score", DataType::Float64),
        ],
    );
    // `SingerId` is declared NOT NULL, which only INFORMATION_SCHEMA reports.
    assert_eq!(
        singers_schema
            .fields()
            .iter()
            .map(|f| f.is_nullable())
            .collect::<Vec<_>>(),
        [false, true, true, true]
    );

    // Backticks cannot be escaped for every Spanner parser, so such names are rejected.
    let hostile = connection
        .get_table_schema(None, None, "no`such`table")
        .unwrap_err();
    assert_eq!(hostile.status, Status::InvalidArguments);
    let absent = connection
        .get_table_schema(None, None, "NoSuchTable")
        .unwrap_err();
    assert_eq!(absent.status, Status::NotFound);

    // The only catalog is the database id.
    let own_catalog = connection
        .get_table_schema(Some(&catalog), None, "Singers")
        .unwrap();
    assert_eq!(own_catalog, singers_schema);
    let bogus_catalog = connection
        .get_table_schema(Some("nosuchcatalog"), None, "Singers")
        .unwrap_err();
    assert_eq!(bogus_catalog.status, Status::NotFound);
}

/// A `;`-separated DDL batch reports no count, and the new table is usable right away.
#[test]
fn ddl_batch_creates_an_immediately_usable_table() {
    let Some(mut fx) = fixture() else { return };
    let connection = &mut fx.connection;

    let ddl = "DROP TABLE IF EXISTS AdbcDdl; \
               CREATE TABLE AdbcDdl (Id INT64, Note STRING(MAX)) PRIMARY KEY (Id)";
    assert_eq!(run(connection, ddl), None);

    let insert = "INSERT INTO AdbcDdl (Id, Note) VALUES (7, 'hello')";
    assert_eq!(run(connection, insert), Some(1));

    let rows = query(connection, "SELECT Note FROM AdbcDdl WHERE Id = 7");
    assert_eq!(strings(&rows, 0), ["hello"]);

    drop_tables(connection, &["AdbcDdl"]);
}

/// Manual transactions buffer plain and parameterized DML until commit, rollback discards it, and
/// re-enabling autocommit commits pending work.
#[test]
fn manual_transaction_buffers_commits_and_rolls_back() {
    let Some(mut fx) = fixture() else { return };
    let connection = &mut fx.connection;

    recreate(connection, "AdbcTxn", "Id INT64");

    set_autocommit(connection, false);

    // Buffered DML returns None because the count is unknown until commit.
    buffer_dml(connection, "INSERT INTO AdbcTxn (Id) VALUES (1)");
    buffer_dml(connection, "INSERT INTO AdbcTxn (Id) VALUES (2)");
    // Queries cannot read buffered writes, so they are rejected.
    assert_query_guarded(connection, "AdbcTxn");

    connection.commit().unwrap();
    assert_eq!(count_rows_ending_txn(connection, "AdbcTxn"), 2);

    buffer_dml(connection, "INSERT INTO AdbcTxn (Id) VALUES (3)");
    connection.rollback().unwrap();
    assert_eq!(count_rows_ending_txn(connection, "AdbcTxn"), 2);

    let buffer_param_insert = |connection: &mut SpannerConnection, id: i64| {
        let mut s = prepare(connection, "INSERT INTO AdbcTxn (Id) VALUES (@Id)");
        s.bind(int_batch("Id", &[id])).unwrap();
        assert_eq!(s.execute_update().unwrap(), None);
    };

    buffer_param_insert(connection, 3);
    assert_query_guarded(connection, "AdbcTxn");
    connection.commit().unwrap();
    assert_eq!(count_rows_ending_txn(connection, "AdbcTxn"), 3);

    buffer_param_insert(connection, 4);
    connection.rollback().unwrap();
    assert_eq!(count_rows_ending_txn(connection, "AdbcTxn"), 3);

    // Re-enabling autocommit commits pending work instead of discarding it.
    buffer_dml(connection, "INSERT INTO AdbcTxn (Id) VALUES (5)");
    assert_query_guarded(connection, "AdbcTxn");
    set_autocommit(connection, true);
    assert_eq!(count_rows(connection, "AdbcTxn"), 4);

    // Setting autocommit again is a no-op.
    set_autocommit(connection, true);
    let immediate = "INSERT INTO AdbcTxn (Id) VALUES (6)";
    assert_eq!(run(connection, immediate), Some(1));
    assert_eq!(count_rows(connection, "AdbcTxn"), 5);
    assert_eq!(
        connection.commit().unwrap_err().status,
        Status::InvalidState
    );

    drop_tables(connection, &["AdbcTxn"]);
}

/// A failed commit keeps buffered DML so the caller can retry or roll back. Enabling autocommit
/// is an implicit commit, so it must fail too and leave the connection in manual mode.
#[test]
fn failed_commit_keeps_the_transaction_replayable() {
    let Some(mut fx) = fixture() else { return };
    let connection = &mut fx.connection;

    recreate(connection, "AdbcTxn", "Id INT64");
    let seed = "INSERT INTO AdbcTxn (Id) VALUES (1), (2), (3), (5), (6)";
    assert_eq!(run(connection, seed), Some(5));

    // DML against an unknown table buffers without an RPC and fails at commit.
    set_autocommit(connection, false);
    buffer_dml(connection, "INSERT INTO AdbcTxn (Id) VALUES (7)");
    buffer_dml(connection, "INSERT INTO AdbcTxnNoSuchTable (Id) VALUES (7)");
    assert!(connection.commit().is_err());
    // The batch is still buffered. Re-committing `VALUES (7)` after the rollback below would hit
    // a duplicate key if the failed batch had partially applied.
    assert_query_guarded(connection, "AdbcTxn");
    assert!(connection.commit().is_err(), "the retry replays the buffer");
    assert!(
        connection
            .set_option(OptionConnection::AutoCommit, true.into())
            .is_err()
    );
    assert_eq!(
        connection
            .get_option_string(OptionConnection::AutoCommit)
            .unwrap(),
        "false"
    );
    connection.rollback().unwrap();
    buffer_dml(connection, "INSERT INTO AdbcTxn (Id) VALUES (7)");
    connection.commit().unwrap();
    assert_eq!(count_rows(connection, "AdbcTxn"), 6);
    set_autocommit(connection, true);

    drop_tables(connection, &["AdbcTxn"]);
}

/// Bulk ingest in a manual transaction buffers mutations that commit together with buffered DML
/// and are discarded by rollback.
#[test]
fn manual_transaction_commits_buffered_ingest_with_dml() {
    let Some(mut fx) = fixture() else { return };
    let connection = &mut fx.connection;

    recreate(connection, "AdbcTxn", "Id INT64");
    let seed = "INSERT INTO AdbcTxn (Id) VALUES (1), (2), (3), (5), (6), (7)";
    assert_eq!(run(connection, seed), Some(6));

    set_autocommit(connection, false);
    let rows = int_batch("Id", &[100, 101]);
    assert_eq!(ingest(connection, "AdbcTxn", "append", rows).unwrap(), None);
    buffer_dml(connection, "INSERT INTO AdbcTxn (Id) VALUES (102)");
    assert_query_guarded(connection, "AdbcTxn");
    connection.commit().unwrap();
    assert_eq!(count_rows_ending_txn(connection, "AdbcTxn"), 9);
    let rows = int_batch("Id", &[103]);
    assert_eq!(ingest(connection, "AdbcTxn", "append", rows).unwrap(), None);
    connection.rollback().unwrap();
    assert_eq!(count_rows(connection, "AdbcTxn"), 9);
    set_autocommit(connection, true);

    drop_tables(connection, &["AdbcTxn"]);
}

/// The ingest mode defaults to `create`, so a second ingest into the same table fails with
/// `AlreadyExists`.
#[test]
fn bulk_ingest_defaults_to_create_mode() {
    let Some(mut fx) = fixture() else { return };
    let connection = &mut fx.connection;

    run(connection, "DROP TABLE IF EXISTS AdbcDefaultMode");
    let default_mode_ingest = |connection: &mut SpannerConnection, ids: &[i64]| {
        let mut statement = connection.new_statement().unwrap();
        statement
            .set_option(OptionStatement::TargetTable, "AdbcDefaultMode".into())
            .unwrap();
        assert_eq!(
            statement
                .get_option_string(OptionStatement::IngestMode)
                .unwrap(),
            "adbc.ingest.mode.create"
        );
        statement.bind(int_batch("Id", ids)).unwrap();
        statement.execute_update()
    };
    assert_eq!(default_mode_ingest(connection, &[7, 8]).unwrap(), Some(2));
    assert_eq!(count_rows(connection, "AdbcDefaultMode"), 2);
    let error = default_mode_ingest(connection, &[9]).unwrap_err();
    assert_eq!(error.status, Status::AlreadyExists, "{error}");
    drop_tables(connection, &["AdbcDefaultMode"]);
}

/// Spanner resolves table names case-insensitively, so a create-mode ingest into a differently
/// cased name of an existing table is `AlreadyExists`, not the raw duplicate-name DDL error.
#[test]
fn ingest_create_onto_a_differently_cased_table_is_already_exists() {
    let Some(mut fx) = fixture() else { return };
    let connection = &mut fx.connection;

    recreate(connection, "AdbcCaseProbe", "Id INT64, Label STRING(MAX)");
    let error = ingest_labels(connection, "adbccaseprobe", "create").unwrap_err();
    assert_eq!(error.status, Status::AlreadyExists, "{error}");
    assert!(error.message.contains("adbccaseprobe"), "{error}");
    assert_eq!(count_rows(connection, "AdbcCaseProbe"), 0);

    drop_tables(connection, &["AdbcCaseProbe"]);
}

/// Recreates `AdbcBind` with two rows.
#[track_caller]
fn recreate_bind_table(connection: &mut SpannerConnection) {
    recreate(connection, "AdbcBind", "Id INT64, Name STRING(MAX)");
    let seed = "INSERT INTO AdbcBind (Id, Name) VALUES (1, 'Alice'), (2, 'Bob')";
    assert_eq!(run(connection, seed), Some(2));
}

/// A failed `append` reports `NotFound` for a missing table and `AlreadyExists` for an
/// incompatible schema, as ADBC requires. The driver probes `INFORMATION_SCHEMA` to tell which.
#[test]
fn bulk_ingest_append_failures_remap_to_the_adbc_contract() {
    let Some(mut fx) = fixture() else { return };
    let connection = &mut fx.connection;

    recreate_bind_table(connection);

    let cases = [
        ("AdbcNoSuchIngestTable", "Id", Status::NotFound),
        ("AdbcBind", "NoSuchColumn", Status::AlreadyExists),
    ];
    for (table, column, status) in cases {
        let error = ingest(connection, table, "append", int_batch(column, &[1])).unwrap_err();
        assert_eq!(error.status, status, "{table}: {error}");
    }
    assert_eq!(count_rows(connection, "AdbcBind"), 2);

    // A manual ingest buffers its inserts, so it checks the table before buffering.
    set_autocommit(connection, false);
    for (table, column, status) in cases {
        let error = ingest(connection, table, "append", int_batch(column, &[1])).unwrap_err();
        assert_eq!(error.status, status, "{table}: {error}");
    }
    // Column names match case-insensitively.
    let rows = RecordBatch::try_from_iter([
        ("id", int_array(&[3])),
        (
            "NAME",
            Arc::new(StringArray::from(vec!["Carol"])) as ArrayRef,
        ),
    ])
    .unwrap();
    assert_eq!(
        ingest(connection, "AdbcBind", "append", rows).unwrap(),
        None
    );
    connection.commit().unwrap();
    set_autocommit(connection, true);
    assert_eq!(count_rows(connection, "AdbcBind"), 3);

    drop_tables(connection, &["AdbcBind"]);
}

/// DML and bulk ingest through `execute()`, and statement reuse as the Python DBAPI does it.
/// Language bindings reach the driver through `ExecuteQuery`, not `execute_update`.
#[test]
fn execute_entry_point_and_statement_handle_reuse() {
    let Some(mut fx) = fixture() else { return };
    let connection = &mut fx.connection;

    recreate_bind_table(connection);

    // DML through `execute()` must use a read/write transaction.
    let dml_rows = query(
        connection,
        "INSERT INTO AdbcBind (Id, Name) VALUES (3, 'Carol')",
    );
    assert_eq!(row_count(&dml_rows), 0);
    assert_eq!(count_rows(connection, "AdbcBind"), 3);

    // Bound data is consumed by the execute that uses it; replaying it would run a follow-up
    // query once per bound row.
    recreate(connection, "AdbcReuse", "Id INT64");
    let mut reuse = ingest_stmt(connection, "AdbcReuse", "append");
    reuse.bind(int_batch("Id", &[10, 20, 30])).unwrap();
    assert_eq!(reuse.execute_update().unwrap(), Some(3));
    reuse
        .set_sql_query("SELECT Id FROM AdbcReuse ORDER BY Id")
        .unwrap();
    assert_eq!(row_count(&fetch(&mut reuse)), 3);
    drop_tables(connection, &["AdbcReuse"]);

    // Ingest through `execute()` returns an empty stream.
    let mut ingest_via_execute = ingest_stmt(connection, "AdbcBind", "append");
    ingest_via_execute
        .bind(id_text_batch("Name", &[4], &["Dave"]))
        .unwrap();
    assert_eq!(row_count(&fetch(&mut ingest_via_execute)), 0);
    assert_eq!(count_rows(connection, "AdbcBind"), 4);

    // A DBAPI cursor sets an ingest target without clearing its previous query; the target wins.
    let mut reuse_query_then_ingest = prepare(connection, "SELECT 1");
    set_ingest_target(&mut reuse_query_then_ingest, "AdbcBind", "append");
    reuse_query_then_ingest
        .bind(id_text_batch("Name", &[5], &["Erin"]))
        .unwrap();
    reuse_query_then_ingest.execute_update().unwrap();
    assert_eq!(count_rows(connection, "AdbcBind"), 5);

    drop_tables(connection, &["AdbcBind"]);
}

/// `create`, `append` and `replace` ingest modes. Created tables have no primary key, so Spanner
/// adds a hidden key column; it and its constraints must not appear in results or metadata.
#[test]
fn ingest_create_modes_and_the_implicit_hidden_key() {
    let Some(mut fx) = fixture() else { return };
    let connection = &mut fx.connection;

    run(connection, "DROP TABLE IF EXISTS AdbcCreate");
    for (mode, rows) in [("create", 2), ("append", 4), ("replace", 2)] {
        let count = ingest_labels(connection, "AdbcCreate", mode);
        assert_eq!(count.unwrap(), Some(2), "{mode}");
        assert_eq!(count_rows(connection, "AdbcCreate"), rows, "{mode}");
    }
    let reader = prepare(connection, "SELECT * FROM AdbcCreate ORDER BY Id")
        .execute()
        .unwrap();
    assert_eq!(field_names(reader.schema().fields()), ["Id", "Label"]);
    let created = drain(reader);
    assert_eq!(row_count(&created), 2);
    let created_schema = connection
        .get_table_schema(None, None, "AdbcCreate")
        .unwrap();
    assert_eq!(created_schema.fields(), label_rows().schema().fields());
    let (objects_columns, objects_constraints) =
        table_columns_and_constraints(connection, "AdbcCreate");
    assert_eq!(objects_columns, ["Id", "Label"]);
    // Neither the implicit key nor the checks Spanner keeps for the non-nullable ingested
    // fields are listed.
    assert!(objects_constraints.is_empty(), "{objects_constraints:?}");
    assert_eq!(ints(&created, 0), [10, 20]);
    assert_eq!(strings(&created, 1), ["x", "y"]);
    let error = ingest_labels(connection, "AdbcCreate", "create").unwrap_err();
    assert_eq!(error.status, Status::AlreadyExists, "{error}");
    assert!(error.message.contains("AdbcCreate"), "{error}");
    assert_eq!(count_rows(connection, "AdbcCreate"), 2);
    drop_tables(connection, &["AdbcCreate"]);
}

/// `create_append` creates a missing table, appends to an existing one, and reports
/// `AlreadyExists` when the existing table's schema does not match.
#[test]
fn ingest_create_append_mode_creates_then_appends() {
    let Some(mut fx) = fixture() else { return };
    let connection = &mut fx.connection;
    let table = "AdbcCreateAppend";

    run(connection, "DROP TABLE IF EXISTS AdbcCreateAppend");
    for rows in [2, 4] {
        let count = ingest_labels(connection, table, "create_append");
        assert_eq!(count.unwrap(), Some(2));
        assert_eq!(count_rows(connection, table), rows);
    }
    let mismatch = int_batch("NoSuchColumn", &[1]);
    let error = ingest(connection, table, "create_append", mismatch).unwrap_err();
    assert_eq!(error.status, Status::AlreadyExists, "{error}");
    assert_eq!(count_rows(connection, table), 4);
    let rows = query(
        connection,
        "SELECT Id, Label FROM AdbcCreateAppend ORDER BY Id",
    );
    assert_eq!(row_count(&rows), 4);
    drop_tables(connection, &[table]);
}

/// Append ingest respects a table's user-defined primary key: duplicate keys fail with
/// `Integrity`. Keys are declared in DDL; `spanner.ingest.primary_key` is unsupported.
#[test]
fn ingest_appends_into_a_user_keyed_table() {
    let Some(mut fx) = fixture() else { return };
    let connection = &mut fx.connection;

    recreate(connection, "AdbcIngestPk", "Id INT64, Label STRING(MAX)");
    let count = ingest_labels(connection, "AdbcIngestPk", "append");
    assert_eq!(count.unwrap(), Some(2));
    assert_eq!(count_rows(connection, "AdbcIngestPk"), 2);
    let error = ingest_labels(connection, "AdbcIngestPk", "append").unwrap_err();
    assert_eq!(error.status, Status::Integrity, "{error}");
    drop_tables(connection, &["AdbcIngestPk"]);
    let error = connection
        .new_statement()
        .unwrap()
        .set_option("spanner.ingest.primary_key".into(), "Id".into())
        .unwrap_err();
    assert_eq!(error.status, Status::NotImplemented, "{error}");
}

/// Constraint violations are `Integrity` errors from DML, a manual commit and ingest. Spanner
/// reports most of them with codes it also uses for unrelated failures, so this checks the
/// driver's message matching against the backend's wording.
#[test]
fn constraint_violations_are_integrity_errors() {
    let Some(mut fx) = fixture() else { return };
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
        let error = prepare(connection, sql).execute_update().expect_err(sql);
        assert_eq!(error.status, Status::Integrity, "{sql}: {error}");
    }

    set_autocommit(connection, false);
    run(
        connection,
        "INSERT INTO AdbcIntegrity (Id, Name, Age) VALUES (1, 'd', 1)",
    );
    let error = connection.commit().unwrap_err();
    assert_eq!(error.status, Status::Integrity, "{error}");
    connection.rollback().unwrap();
    set_autocommit(connection, true);

    let rows = RecordBatch::try_from_iter_with_nullable([
        ("Id", int_array(&[6]), false),
        (
            "Name",
            Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
            true,
        ),
    ])
    .unwrap();
    let error = ingest(connection, "AdbcIntegrity", "append", rows).unwrap_err();
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
    let Some(mut fx) = fixture() else { return };
    let connection = &mut fx.connection;

    recreate_bind_table(connection);

    let mut by_id = prepare(connection, "SELECT Name FROM AdbcBind WHERE Id = @Id");
    // Before binding, parameter types come from planning the query.
    let planned = by_id.get_parameter_schema().unwrap();
    assert_fields(planned.fields(), &[("Id", DataType::Int64)]);
    by_id.bind(int_batch("Id", &[2])).unwrap();
    let bound = by_id.get_parameter_schema().unwrap();
    assert_fields(bound.fields(), &[("Id", DataType::Int64)]);
    assert_eq!(strings(&fetch(&mut by_id), 0), ["Bob"]);

    // Columns bind by position by default, whatever their names.
    let mut positional = prepare(connection, "SELECT Name FROM AdbcBind WHERE Id = @p1");
    positional.bind(int_batch("value", &[2])).unwrap();
    assert_eq!(strings(&fetch(&mut positional), 0), ["Bob"]);

    // The bound columns are in a different order than the SQL parameters, so bind by name.
    let mut by_name = prepare(
        connection,
        "UPDATE AdbcBind SET Name = @Name WHERE Id = @Id",
    );
    configure(&mut by_name, &[(OPTION_BIND_BY_NAME, "true")]);
    // DML parameters are reported in SQL order.
    let planned = by_name.get_parameter_schema().unwrap();
    assert_fields(
        planned.fields(),
        &[("Name", DataType::Utf8), ("Id", DataType::Int64)],
    );
    by_name
        .bind(id_text_batch("Name", &[1], &["Alicia"]))
        .unwrap();
    assert_eq!(by_name.execute_update().unwrap(), Some(1));

    // A `Null`-typed column binds NULL. pyarrow infers `Null` for all-None parameters, and
    // `get_parameter_schema` reports it for undetermined ones.
    let null_row = RecordBatch::try_from_iter_with_nullable([
        ("0", int_array(&[7001]), false),
        (
            "1",
            Arc::new(arrow_array::NullArray::new(1)) as ArrayRef,
            true,
        ),
    ])
    .unwrap();
    let mut null_insert = prepare(
        connection,
        "INSERT INTO AdbcBind (Id, Name) VALUES (@p1, @p2)",
    );
    null_insert.bind(null_row).unwrap();
    assert_eq!(null_insert.execute_update().unwrap(), Some(1));
    let rows = query(connection, "SELECT Name FROM AdbcBind WHERE Id = 7001");
    assert_eq!(row_count(&rows), 1);
    assert!(rows[0].column(0).is_null(0));

    drop_tables(connection, &["AdbcBind"]);
}

#[test]
fn standalone_parameters_preserve_arrow_types_and_values() {
    let Some(mut fx) = read_fixture() else { return };
    let mut lists = ListBuilder::new(StringBuilder::new());
    lists.values().append_value("one");
    lists.values().append_null();
    lists.append(true);
    lists.append(true);
    let dictionary: arrow_array::DictionaryArray<arrow_array::types::Int32Type> =
        [Some("decoded"), None].into_iter().collect();
    let columns: Vec<ArrayRef> = vec![
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
    let mut projections = (0..11)
        .map(|i| format!("@p{i} AS p{i}"))
        .collect::<Vec<_>>();
    projections.push("CAST(@p11 AS STRING) AS p11".to_string());
    let sql = format!("SELECT {}", projections.join(", "));
    let mut statement = prepare(&mut fx.connection, sql);
    statement.bind(input.clone()).unwrap();
    let planned = statement.execute_schema().unwrap();
    let reader = statement.execute().unwrap();
    assert_eq!(*reader.schema(), planned);
    let batches = drain(reader);
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

/// Decimals of any scale bind when `NUMERIC` holds them exactly, and fail before any RPC otherwise.
#[test]
fn numeric_parameters_of_any_scale_round_trip() {
    let Some(mut fx) = read_fixture() else { return };
    let max = 10_i128.pow(38) - 1;
    for (scale, unscaled, expected) in [
        (-2, Some(99), Some(Ok(9_900_000_000_000))),
        (-2, Some(-99), Some(Ok(-9_900_000_000_000))),
        (-2, None, None),
        (i8::MIN, Some(0), Some(Ok(0))),
        (9, Some(-max), Some(Ok(-max))),
        (10, Some(15_000_000_000), Some(Ok(1_500_000_000))),
        (10, Some(15_000_000_001), Some(Err(()))),
        (0, Some(10_i128.pow(29)), Some(Err(()))),
    ] {
        let amount = Decimal128Array::from(vec![unscaled])
            .with_precision_and_scale(38, scale)
            .unwrap();
        let input =
            RecordBatch::try_from_iter_with_nullable([("amount", Arc::new(amount) as _, true)])
                .unwrap();
        let mut statement = prepare(&mut fx.connection, "SELECT @amount AS amount");
        statement.bind(input).unwrap();
        let result = statement.execute();
        if let Some(Err(())) = expected {
            let error = result.err().unwrap();
            assert_eq!(error.status, Status::InvalidArguments, "{error}");
            continue;
        }
        let batches = drain(result.unwrap());
        assert_eq!(batches.len(), 1);
        let result = col::<Decimal128Array>(batches[0].column(0));
        assert_eq!(result.data_type(), &DataType::Decimal128(38, 9));
        let expected = expected.map(Result::unwrap);
        assert_eq!(result.iter().collect::<Vec<_>>(), vec![expected]);
    }
}

#[test]
fn prepare_requires_a_query_or_an_ingest_target() {
    let Some(mut fx) = read_fixture() else { return };
    let connection = &mut fx.connection;

    let mut unprepared = connection.new_statement().unwrap();
    assert_eq!(
        unprepared.prepare().unwrap_err().status,
        Status::InvalidState
    );
    // An ingest target needs no query, and the table need not exist yet.
    let mut ingest_prepare = connection.new_statement().unwrap();
    ingest_prepare
        .set_option(OptionStatement::TargetTable, "AdbcBind".into())
        .unwrap();
    ingest_prepare.prepare().unwrap();
}

/// Ingest quotes reserved words used as table (`create`) and column (`index`) names, both in the
/// generated `CREATE TABLE` and in the insert mutations.
#[test]
fn bulk_ingest_quotes_reserved_word_identifiers() {
    let Some(mut fx) = fixture() else { return };
    let connection = &mut fx.connection;

    run(connection, "DROP TABLE IF EXISTS `create`");
    let rows = int_batch("index", &[42, -42]);
    assert_eq!(
        ingest(connection, "create", "create", rows).unwrap(),
        Some(2)
    );
    assert_eq!(count_rows(connection, "`create`"), 2);
    drop_tables(connection, &["`create`"]);
}

#[test]
fn bulk_ingest_replace_rejects_unbindable_schema_before_dropping_table() {
    let Some(mut fx) = fixture() else { return };
    let connection = &mut fx.connection;
    let table = "AdbcInvalidReplace";
    recreate(connection, table, "Id INT64");
    let insert = format!("INSERT INTO {table} (Id) VALUES (42)");
    assert_eq!(run(connection, insert), Some(1));

    for data_type in [
        DataType::UInt64,
        DataType::List(Arc::new(Field::new(
            "item",
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            true,
        ))),
    ] {
        let schema = Arc::new(Schema::new(vec![Field::new("bad", data_type, true)]));
        let rows = RecordBatch::new_empty(schema);
        let error = ingest(connection, table, "replace", rows).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        assert!(
            error.message.starts_with("ingest column \"bad\": "),
            "{error}"
        );
        assert_eq!(count_rows(connection, table), 1);
    }
    drop_tables(connection, &[table]);
}

/// Spanner refuses to drop a table with dependent schema objects; the error says what to do.
#[test]
fn bulk_ingest_replace_of_an_indexed_table_names_the_remedy() {
    let Some(mut fx) = fixture() else { return };
    let connection = &mut fx.connection;
    let table = "AdbcIndexedReplace";
    run(
        connection,
        format!(
            "DROP INDEX IF EXISTS {table}ByName; DROP TABLE IF EXISTS {table}; \
             CREATE TABLE {table} (Id INT64, Name STRING(MAX)) PRIMARY KEY (Id); \
             CREATE INDEX {table}ByName ON {table} (Name)"
        ),
    );
    let schema = Arc::new(Schema::new(vec![Field::new("Id", DataType::Int64, true)]));
    let rows = RecordBatch::new_empty(schema);
    let error = ingest(connection, table, "replace", rows).unwrap_err();
    assert_eq!(error.status, Status::InvalidState, "{error}");
    for needle in [
        &format!("{table}ByName"),
        "Replace mode drops",
        "mode append",
    ] {
        assert!(error.message.contains(needle), "{error}");
    }
    run(
        connection,
        format!("DROP INDEX {table}ByName; DROP TABLE {table}"),
    );
}

/// `ARRAY<scalar>` columns map to an Arrow `List` of the element type.
#[test]
fn array_columns_map_to_native_arrow_lists() {
    let Some(mut fx) = fixture() else { return };
    let connection = &mut fx.connection;

    run(
        connection,
        "DROP TABLE IF EXISTS AdbcArr; \
         CREATE TABLE AdbcArr (Id INT64, Nums ARRAY<INT64>, Tags ARRAY<STRING(MAX)>) \
         PRIMARY KEY (Id)",
    );

    let insert = "INSERT INTO AdbcArr (Id, Nums, Tags) VALUES (1, [10, 20, 30], ['a', 'b'])";
    assert_eq!(run(connection, insert), Some(1));

    let reader = prepare(connection, "SELECT Nums, Tags FROM AdbcArr WHERE Id = 1")
        .execute()
        .unwrap();
    let list_of = |item| DataType::List(Arc::new(Field::new("item", item, true)));
    let schema = reader.schema();
    assert_eq!(schema.field(0).data_type(), &list_of(DataType::Int64));
    assert_eq!(schema.field(1).data_type(), &list_of(DataType::Utf8));
    let batches = drain(reader);
    let nums = col::<ListArray>(batches[0].column(0)).value(0);
    assert_eq!(col::<Int64Array>(&nums).values(), &[10, 20, 30]);
    let tags = col::<ListArray>(batches[0].column(1)).value(0);
    let tags = col::<StringArray>(&tags);
    assert_eq!((tags.value(0), tags.value(1)), ("a", "b"));

    drop_tables(connection, &["AdbcArr"]);
}

/// `STRUCT` maps to an Arrow `Struct`, including duplicate and unnamed fields.
#[test]
fn struct_values_map_to_native_arrow_structs() {
    let Some(mut fx) = read_fixture() else { return };
    let connection = &mut fx.connection;

    // Cloud Spanner only returns structs as `ARRAY(SELECT AS STRUCT ...)`; the emulator also
    // accepts array literals of `STRUCT(...)`. The subquery needs an ORDER BY to be deterministic.
    let reader = prepare(
        connection,
        "SELECT ARRAY(\
             SELECT AS STRUCT a, b FROM (\
                 SELECT 0 AS n, 1 AS a, 'x' AS b \
                 UNION ALL SELECT 1 AS n, 2 AS a, 'y' AS b\
             ) ORDER BY n\
         ) AS arr",
    )
    .execute()
    .unwrap();
    assert_fields(
        struct_fields(list_item(reader.schema().field(0).data_type()).data_type()),
        &[("a", DataType::Int64), ("b", DataType::Utf8)],
    );
    let batches = drain(reader);
    let inner = list_structs(batches[0].column(0));
    assert_eq!(inner.len(), 2);
    assert_eq!(col::<Int64Array>(inner.column(0)).values(), &[1, 2]);
    let b = col::<StringArray>(inner.column(1));
    assert_eq!((b.value(0), b.value(1)), ("x", "y"));

    // Struct fields are positional; decoding by name would merge the two `x` fields.
    let reader = prepare(
        connection,
        "SELECT ARRAY(SELECT AS STRUCT 1 AS x, 2 AS x, 3) AS arr",
    )
    .execute()
    .unwrap();
    assert_eq!(
        field_names(struct_fields(
            list_item(reader.schema().field(0).data_type()).data_type()
        )),
        ["x", "x", ""]
    );
    let batches = drain(reader);
    let inner = list_structs(batches[0].column(0));
    let value = |i: usize| col::<Int64Array>(inner.column(i)).value(0);
    assert_eq!((value(0), value(1), value(2)), (1, 2, 3));
}

/// `;`-separated DML runs as one `ExecuteBatchDml` and reports the summed row count.
#[test]
fn batched_multi_statement_dml_commits_together() {
    let Some(mut fx) = fixture() else { return };
    let connection = &mut fx.connection;

    recreate(connection, "AdbcBatch", "Id INT64");
    assert_eq!(
        run(connection, "INSERT INTO AdbcBatch (Id) VALUES (1)"),
        Some(1)
    );

    let batch = "DELETE FROM AdbcBatch WHERE true; INSERT INTO AdbcBatch (Id) VALUES (2), (3)";
    assert_eq!(run(connection, batch), Some(3));
    assert_eq!(count_rows(connection, "AdbcBatch"), 2);

    drop_tables(connection, &["AdbcBatch"]);
}

#[test]
fn execute_schema_returns_a_query_schema_without_running_it() {
    let Some(mut fx) = read_fixture() else { return };
    let planned = prepare(
        &mut fx.connection,
        "WITH cte AS (SELECT 1 AS a, 'x' AS b) SELECT a, b FROM cte",
    )
    .execute_schema()
    .unwrap();
    assert_fields(
        planned.fields(),
        &[("a", DataType::Int64), ("b", DataType::Utf8)],
    );
}

/// `get_objects` at `All` depth; `xdbc_type_name` carries the Spanner type name.
#[test]
fn get_objects_reports_catalog_schema_table_columns() {
    let Some(mut fx) = read_fixture() else { return };
    let connection = &mut fx.connection;

    let batches = drain(
        connection
            .get_objects(
                ObjectDepth::All,
                None,
                Some(""),
                Some("Singers"),
                None,
                None,
            )
            .unwrap(),
    );
    assert_eq!(batches[0].num_rows(), 1, "single catalog");

    let schemas = list_structs(batches[0].column(1));
    assert_eq!(schemas.len(), 1);

    let tables = list_structs(schemas.column(1));
    assert_eq!(tables.len(), 1);
    assert_eq!(col::<StringArray>(tables.column(0)).value(0), "Singers");

    let columns = list_structs(tables.column(2));
    assert_eq!(columns.len(), 4);
    assert_eq!(col::<StringArray>(columns.column(0)).value(0), "SingerId");

    let type_names = columns.column_by_name("xdbc_type_name").unwrap();
    assert_eq!(
        string_values(type_names),
        ["INT64", "STRING(MAX)", "BOOL", "FLOAT64"]
    );

    // The primary key names its column. The check Spanner keeps for `SingerId NOT NULL` is left
    // out.
    let constraints = list_structs(tables.column(3));
    let constraint_type = col::<StringArray>(constraints.column(1));
    let column_names = col::<ListArray>(constraints.column(2));
    let mut seen: Vec<(String, Vec<String>)> = (0..constraints.len())
        .map(|i| {
            let names = string_values(&column_names.value(i));
            (constraint_type.value(i).to_string(), names)
        })
        .collect();
    seen.sort();
    assert_eq!(
        seen,
        [("PRIMARY KEY".to_string(), vec!["SingerId".to_string()])]
    );
}

/// `get_objects` reports each column's default, generation and identity in its `xdbc_*` fields.
#[test]
fn get_objects_reports_column_defaults_generation_and_identity() {
    let Some(mut fx) = fixture() else { return };
    let connection = &mut fx.connection;
    run(
        connection,
        "DROP TABLE IF EXISTS AdbcColumnKinds; \
         CREATE TABLE AdbcColumnKinds ( \
           Id INT64 NOT NULL GENERATED BY DEFAULT AS IDENTITY (BIT_REVERSED_POSITIVE), \
           Region STRING(10) DEFAULT ('eu'), \
           Doubled INT64 AS (Id * 2) STORED \
         ) PRIMARY KEY (Id)",
    );
    let batches = drain(
        connection
            .get_objects(
                ObjectDepth::Columns,
                None,
                Some(""),
                Some("AdbcColumnKinds"),
                None,
                None,
            )
            .unwrap(),
    );
    let tables = list_structs(list_structs(batches[0].column(1)).column(1));
    let columns = list_structs(tables.column_by_name("table_columns").unwrap());
    let flags = |name: &str| {
        col::<BooleanArray>(columns.column_by_name(name).unwrap())
            .iter()
            .collect::<Vec<_>>()
    };
    assert_eq!(
        flags("xdbc_is_autoincrement"),
        [Some(true), Some(false), Some(false)]
    );
    assert_eq!(
        flags("xdbc_is_generatedcolumn"),
        [Some(false), Some(false), Some(true)]
    );
    let defaults = col::<StringArray>(columns.column_by_name("xdbc_column_def").unwrap());
    assert_eq!(defaults.value(1), "'eu'");
    assert_eq!(
        col::<Int16Array>(columns.column_by_name("xdbc_nullable").unwrap()).values()[..],
        [0, 1, 1]
    );
    drop_tables(connection, &["AdbcColumnKinds"]);
}

/// `get_objects` depth and filter handling. Levels below the requested depth are NULL; a filter
/// that matches nothing at the requested depth yields an empty list.
#[test]
fn get_objects_depth_and_filter_boundaries() {
    let Some(mut fx) = read_fixture() else { return };
    let catalog = fx.target.database.clone();
    let connection = &mut fx.connection;

    let catalogs = drain(
        connection
            .get_objects(ObjectDepth::Catalogs, None, None, None, None, None)
            .unwrap(),
    );
    assert_eq!(catalogs[0].num_rows(), 1, "single catalog");
    assert_eq!(col::<StringArray>(catalogs[0].column(0)).value(0), catalog);
    assert!(col::<ListArray>(catalogs[0].column(1)).is_null(0));

    let none = drain(
        connection
            .get_objects(ObjectDepth::Catalogs, Some("nope"), None, None, None, None)
            .unwrap(),
    );
    assert_eq!(row_count(&none), 0);

    let db_schemas = drain(
        connection
            .get_objects(ObjectDepth::Schemas, None, None, None, None, None)
            .unwrap(),
    );
    assert_eq!(db_schemas[0].num_rows(), 1, "single catalog");
    assert!(col::<ListArray>(db_schemas[0].column(1)).is_valid(0));
    let schemas = list_structs(db_schemas[0].column(1));
    assert!(
        string_values(schemas.column(0))
            .iter()
            .any(String::is_empty),
        "the default \"\" schema must be reported"
    );
    let schema_tables = col::<ListArray>(schemas.column(1));
    assert_eq!(schema_tables.null_count(), schema_tables.len());

    // ADBC says `table_type` filter values come from `get_table_types`.
    let reported_types = strings(&drain(connection.get_table_types().unwrap()), 0);
    assert!(
        reported_types.contains(&"BASE TABLE".to_string()),
        "{reported_types:?}"
    );
    let filtered = drain(
        connection
            .get_objects(
                ObjectDepth::Tables,
                None,
                None,
                Some("Singers"),
                Some(reported_types.iter().map(String::as_str).collect()),
                None,
            )
            .unwrap(),
    );
    assert_eq!(objects_table_names(&filtered), ["Singers"]);

    // A table_type filter matching nothing still returns the catalog and schema rows, with empty
    // (not NULL) table lists.
    let no_such_type = drain(
        connection
            .get_objects(
                ObjectDepth::Tables,
                None,
                None,
                None,
                Some(vec!["NO SUCH TYPE"]),
                None,
            )
            .unwrap(),
    );
    assert_eq!(no_such_type[0].num_rows(), 1, "single catalog");
    assert_empty_table_lists(&no_such_type[0]);
}

/// Asserts that every schema of the first catalog in `batch` survives with an empty, non-NULL
/// table list.
#[track_caller]
fn assert_empty_table_lists(batch: &RecordBatch) {
    assert!(col::<ListArray>(batch.column(1)).is_valid(0));
    let schemas = list_structs(batch.column(1));
    assert!(!schemas.is_empty());
    let table_lists = col::<ListArray>(schemas.column(1));
    for i in 0..table_lists.len() {
        assert!(table_lists.is_valid(i));
        assert_eq!(table_lists.value(i).len(), 0);
    }
}

/// `spanner.commit_stats` exposes the commit's mutation count. The emulator never returns commit
/// statistics, so there the count stays `NotFound`.
#[test]
fn commit_stats_reports_mutation_count() {
    let Some(mut fx) = fixture() else { return };
    let target = &fx.target;
    let connection = &mut fx.connection;

    run(connection, "DELETE FROM Singers WHERE true");

    let mut insert = prepare(
        connection,
        "INSERT INTO Singers (SingerId, Name, Active, Score) \
         VALUES (1, 'Alice', true, 4.5), (2, 'Bob', false, 3.25)",
    );
    configure(&mut insert, &[(OPTION_COMMIT_STATS, "true")]);
    assert_eq!(
        insert
            .get_option_string(OPTION_COMMIT_STATS.into())
            .unwrap(),
        "true"
    );
    let before = insert
        .get_option_int(OPTION_COMMIT_STATS_MUTATION_COUNT.into())
        .unwrap_err();
    assert_eq!(before.status, Status::NotFound);

    assert_eq!(insert.execute_update().unwrap(), Some(2));

    let mutation_count = insert.get_option_int(OPTION_COMMIT_STATS_MUTATION_COUNT.into());
    if target.is_emulator {
        assert_eq!(mutation_count.unwrap_err().status, Status::NotFound);
    } else {
        // The exact count is up to Spanner.
        let mutations = mutation_count.unwrap();
        assert!(mutations > 0, "{mutations}");
    }

    let rejected = insert.set_option(
        OPTION_COMMIT_STATS_MUTATION_COUNT.into(),
        OptionValue::Int(1),
    );
    assert_eq!(rejected.unwrap_err().status, Status::NotImplemented);

    run(connection, "DELETE FROM Singers WHERE true");
}

/// Six ~1.1 MB rows with `ids`: about three rows per chunk, clear of both the driver's ~4 MiB
/// per-chunk budget edge and gRPC's 4 MB message limit.
fn oversized_rows(ids: &[i64]) -> RecordBatch {
    let names: Vec<String> = (0..ids.len())
        .map(|i| char::from(b'a' + i as u8).to_string().repeat(OVERSIZED_LEN))
        .collect();
    id_text_batch("Name", ids, &names)
}

const OVERSIZED_LEN: usize = 1_100_000;

/// An ingest larger than the driver's ~4 MiB per-chunk budget is split into several transactions;
/// the count must sum across chunks and every row must land once.
#[test]
fn bulk_ingest_chunks_past_the_byte_budget() {
    let Some(mut fx) = fixture() else { return };
    let connection = &mut fx.connection;

    recreate(connection, "AdbcChunked", "Id INT64, Name STRING(MAX)");

    let rows = oversized_rows(&[0, 1, 2, 3, 4, 5]);
    let count = ingest(connection, "AdbcChunked", "append", rows).unwrap();
    assert_eq!(count, Some(6));

    let batches = query(
        connection,
        "SELECT Id, CHAR_LENGTH(Name) AS Len, SUBSTR(Name, 1, 1) AS Head \
         FROM AdbcChunked ORDER BY Id",
    );
    assert_eq!(ints(&batches, 0), [0, 1, 2, 3, 4, 5]);
    assert_eq!(ints(&batches, 1), [OVERSIZED_LEN as i64; 6]);
    assert_eq!(strings(&batches, 2), ["a", "b", "c", "d", "e", "f"]);

    drop_tables(connection, &["AdbcChunked"]);
}

/// `spanner.ingest.batch_write` ingests through the BatchWrite RPC with the same row count and
/// `Integrity` mapping as the transactional path.
#[test]
fn bulk_ingest_via_batch_write() {
    let Some(mut fx) = fixture() else { return };
    let connection = &mut fx.connection;

    recreate(connection, "AdbcBatchWrite", "Id INT64, Name STRING(MAX)");

    let names = ["row-0", "row-1", "row-2", "row-3", "row-4"];
    let rows = id_text_batch("Name", &[0, 1, 2, 3, 4], &names);

    let mut ingest = ingest_stmt(connection, "AdbcBatchWrite", "append");
    configure(&mut ingest, &[(OPTION_INGEST_BATCH_WRITE, "true")]);
    assert_eq!(
        ingest
            .get_option_string(OPTION_INGEST_BATCH_WRITE.into())
            .unwrap(),
        "true"
    );
    ingest.bind(rows.clone()).unwrap();
    assert_eq!(ingest.execute_update().unwrap(), Some(5));

    let batches = query(
        connection,
        "SELECT Id, Name FROM AdbcBatchWrite ORDER BY Id",
    );
    assert_eq!(ints(&batches, 0), [0, 1, 2, 3, 4]);
    assert_eq!(strings(&batches, 1), names);

    let mut dup = ingest_stmt(connection, "AdbcBatchWrite", "append");
    configure(&mut dup, &[(OPTION_INGEST_BATCH_WRITE, "true")]);
    dup.bind(rows).unwrap();
    let error = dup.execute_update().unwrap_err();
    assert_eq!(error.status, Status::Integrity, "{error}");
    assert!(error.message.contains("AdbcBatchWrite"), "{error}");

    drop_tables(connection, &["AdbcBatchWrite"]);
}

/// `spanner.max_timestamp_precision`: out-of-range timestamps error by default and read back in
/// `microseconds` mode, set per connection or statement.
#[test]
fn timestamp_precision_modes_round_trip() {
    let Some(mut fx) = fixture() else { return };
    let connection = &mut fx.connection;

    recreate(connection, "AdbcTsPrecision", "Id INT64, T TIMESTAMP");

    // Arrow nanosecond timestamps cover roughly 1677-2262. The extra digits test truncation.
    let insert = "INSERT INTO AdbcTsPrecision (Id, T) VALUES \
                  (1, TIMESTAMP '9999-01-01T00:00:00.123456789Z'), \
                  (2, TIMESTAMP '1500-06-15T12:34:56.789012Z'), \
                  (3, NULL)";
    assert_eq!(run(connection, insert), Some(3));

    let micros_ty = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));

    let mut q = prepare(connection, "SELECT T FROM AdbcTsPrecision WHERE Id = 1");
    // The error may surface from `execute` or from the first batch.
    let msg = match q.execute() {
        Err(e) => e.message,
        Ok(reader) => reader
            .collect::<Result<Vec<_>, _>>()
            .unwrap_err()
            .to_string(),
    };
    for needle in ["\"T\"", "9999-01-01", OPTION_MAX_TIMESTAMP_PRECISION] {
        assert!(msg.contains(needle), "{msg}");
    }

    configure(
        connection,
        &[(OPTION_MAX_TIMESTAMP_PRECISION, "microseconds")],
    );
    assert_eq!(
        connection
            .get_option_string(OPTION_MAX_TIMESTAMP_PRECISION.into())
            .unwrap(),
        "microseconds"
    );

    let mut q = prepare(connection, "SELECT T FROM AdbcTsPrecision ORDER BY Id");
    assert_eq!(
        q.get_option_string(OPTION_MAX_TIMESTAMP_PRECISION.into())
            .unwrap(),
        "microseconds",
        "statement inherits the connection's mode"
    );
    assert_eq!(q.execute_schema().unwrap().field(0).data_type(), &micros_ty);

    let reader = q.execute().unwrap();
    assert_eq!(reader.schema().field(0).data_type(), &micros_ty);
    let batches = drain(reader);
    let ts = col::<TimestampMicrosecondArray>(batches[0].column(0));
    assert_eq!(ts.value(0), 253_370_764_800_123_456); // .123456789 truncated to .123456
    assert_eq!(ts.value(1), -14_817_468_303_210_988); // 1500-06-15T12:34:56.789012Z
    assert!(ts.is_null(2));

    let table_schema = connection
        .get_table_schema(None, None, "AdbcTsPrecision")
        .unwrap();
    assert_eq!(
        table_schema.field_with_name("T").unwrap().data_type(),
        &micros_ty
    );

    // A statement-level `""` resets to the default, overriding the connection.
    let mut q = prepare(connection, "SELECT T FROM AdbcTsPrecision WHERE Id = 1");
    configure(&mut q, &[(OPTION_MAX_TIMESTAMP_PRECISION, "")]);
    assert_eq!(
        q.get_option_string(OPTION_MAX_TIMESTAMP_PRECISION.into())
            .unwrap(),
        "nanoseconds_error_on_overflow"
    );
    assert_eq!(
        q.execute_schema().unwrap().field(0).data_type(),
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
    let Some(mut fx) = fixture() else { return };
    let connection = &mut fx.connection;

    run(
        connection,
        "DROP TABLE IF EXISTS AdbcEdge; \
         DROP TABLE IF EXISTS AdbcEdgeCreate; \
         DROP TABLE IF EXISTS AdbcEdgeZero; \
         CREATE TABLE AdbcEdge (Id INT64, Name STRING(MAX)) PRIMARY KEY (Id)",
    );

    let batch = |ids: &[i64]| {
        let names: Vec<_> = ids.iter().map(|i| format!("name-{i}")).collect();
        id_text_batch("Name", ids, &names)
    };
    let schema = batch(&[]).schema();
    let empty = || RecordBatch::new_empty(schema.clone());
    let append = |connection: &mut SpannerConnection, batches| {
        ingest_stream(connection, "AdbcEdge", "append", &schema, batches)
    };

    // Zero-row batches at the start, middle and end of a stream must not truncate the ingest.
    let batches = vec![empty(), batch(&[1, 2]), empty(), batch(&[3]), empty()];
    assert_eq!(append(connection, batches).unwrap(), Some(3));
    assert_eq!(count_rows(connection, "AdbcEdge"), 3);

    assert_eq!(append(connection, vec![empty(), empty()]).unwrap(), Some(0));
    assert_eq!(count_rows(connection, "AdbcEdge"), 3);

    // The same in a manual transaction.
    set_autocommit(connection, false);
    let batches = vec![empty(), batch(&[4]), empty(), batch(&[5]), empty()];
    assert_eq!(append(connection, batches).unwrap(), None);
    connection.commit().unwrap();
    assert_eq!(count_rows(connection, "AdbcEdge"), 5);
    set_autocommit(connection, true);

    // In create mode the table schema can come from an empty first batch.
    let batches = vec![empty(), batch(&[1])];
    let count = ingest_stream(connection, "AdbcEdgeCreate", "create", &schema, batches);
    assert_eq!(count.unwrap(), Some(1));
    drop_tables(connection, &["AdbcEdgeCreate"]);

    // A stream with no batches at all still declares a schema, so ingest succeeds (the C++
    // validation suite's `TestSqlIngestStreamZeroArrays`).
    for mode in ["create", "append"] {
        let count = ingest_stream(connection, "AdbcEdgeZero", mode, &schema, vec![]);
        assert_eq!(count.unwrap(), Some(0), "{mode}");
        assert_eq!(count_rows(connection, "AdbcEdgeZero"), 0, "{mode}");
    }
    drop_tables(connection, &["AdbcEdgeZero"]);

    // Data may be bound before the ingest options are set.
    let mut before = connection.new_statement().unwrap();
    before.bind(batch(&[6])).unwrap();
    set_ingest_target(&mut before, "AdbcEdge", "append");
    assert_eq!(before.execute_update().unwrap(), Some(1));
    assert_eq!(count_rows(connection, "AdbcEdge"), 6);

    // Bound data without a target table or query fails on both entry points without consuming
    // the data.
    let mut nowhere = connection.new_statement().unwrap();
    nowhere.bind(batch(&[7])).unwrap();
    let update_err = nowhere.execute_update().unwrap_err();
    let execute_err = nowhere.execute().err().unwrap();
    for error in [update_err, execute_err] {
        assert_eq!(error.status, Status::InvalidState, "{error}");
        assert!(
            error.message.contains("adbc.ingest.target_table"),
            "{error}"
        );
    }
    set_ingest_target(&mut nowhere, "AdbcEdge", "append");
    assert_eq!(nowhere.execute_update().unwrap(), Some(1));
    assert_eq!(count_rows(connection, "AdbcEdge"), 7);

    let mut nodata = ingest_stmt(connection, "AdbcEdge", "append");
    let error = nodata.execute_update().unwrap_err();
    assert_eq!(error.status, Status::InvalidState, "{error}");
    assert!(error.message.contains("no data has been bound"), "{error}");
    let error = nodata.execute().err().unwrap();
    assert_eq!(error.status, Status::InvalidState, "{error}");

    // Reuse one handle for ingest, then DML, then ingest; each switch clears the other mode.
    let mut handle = ingest_stmt(connection, "AdbcEdge", "append");
    handle.bind(batch(&[10, 11])).unwrap();
    assert_eq!(handle.execute_update().unwrap(), Some(2));
    handle
        .set_sql_query("INSERT INTO AdbcEdge (Id, Name) VALUES (12, 'dml')")
        .unwrap();
    assert!(
        handle
            .get_option_string(OptionStatement::TargetTable)
            .is_err()
    );
    assert_eq!(handle.execute_update().unwrap(), Some(1));
    set_ingest_target(&mut handle, "AdbcEdge", "append");
    handle.bind(batch(&[13, 14])).unwrap();
    assert_eq!(handle.execute_update().unwrap(), Some(2));
    assert_eq!(count_rows(connection, "AdbcEdge"), 12);

    let error = ingest(connection, "AdbcEdge", "append", batch(&[1])).unwrap_err();
    assert_eq!(error.status, Status::Integrity, "{error}");
    assert!(error.message.contains("AdbcEdge"), "{error}");
    assert!(!error.message.contains("incompatible"), "{error}");
    assert_eq!(count_rows(connection, "AdbcEdge"), 12);

    drop_tables(connection, &["AdbcEdge"]);
}

/// A zero-batch `bind_stream` supplies a schema for bulk ingest but no parameter rows. It must
/// leave the parameter buffer empty so subsequent DML and queries execute without bound rows.
#[test]
fn bulk_ingest_empty_stream_does_not_hijack_other_paths() {
    const BOUND_QUERY: &str = "SELECT Id FROM AdbcEmptyHijack WHERE Id = @p";

    let Some(mut fx) = fixture() else { return };
    let connection = &mut fx.connection;

    run(
        connection,
        "DROP TABLE IF EXISTS AdbcEmptyHijack; \
         DROP TABLE IF EXISTS AdbcStaleLeak; \
         DROP TABLE IF EXISTS AdbcEmptyReadTxn; \
         CREATE TABLE AdbcEmptyHijack (Id INT64, Name STRING(MAX)) PRIMARY KEY (Id)",
    );

    let schema = id_text_batch("Name", &[], &[""; 0]).schema();
    let seed = "INSERT INTO AdbcEmptyHijack (Id, Name) VALUES (1, 'a'), (2, 'b'), (3, 'c')";
    assert_eq!(run(connection, seed), Some(3));

    let mut delete_after_empty = connection.new_statement().unwrap();
    delete_after_empty
        .bind_stream(batch_stream(&schema, vec![]))
        .unwrap();
    delete_after_empty
        .set_sql_query("DELETE FROM AdbcEmptyHijack WHERE Id IN (2, 3)")
        .unwrap();
    assert_eq!(delete_after_empty.execute_update().unwrap(), Some(2));
    assert_eq!(count_rows(connection, "AdbcEmptyHijack"), 1);

    let mut select_after_empty = connection.new_statement().unwrap();
    select_after_empty
        .bind_stream(batch_stream(&schema, vec![]))
        .unwrap();
    select_after_empty
        .set_sql_query("SELECT Id FROM AdbcEmptyHijack")
        .unwrap();
    assert_eq!(row_count(&fetch(&mut select_after_empty)), 1);

    // A bound query with an empty result yields no batches.
    let mut empty_bound_query = prepare(connection, BOUND_QUERY);
    empty_bound_query.bind(int_batch("p", &[999])).unwrap();
    assert_eq!(fetch(&mut empty_bound_query).len(), 0);

    // With zero bound rows (an empty `executemany`) the query still reports its real schema.
    let mut one_row_bound = prepare(connection, BOUND_QUERY);
    one_row_bound.bind(int_batch("p", &[1])).unwrap();
    let non_empty_schema = one_row_bound.execute().unwrap().schema();
    let mut zero_row_bound = prepare(connection, BOUND_QUERY);
    zero_row_bound.bind(int_batch("p", &[])).unwrap();
    let zero_row_reader = zero_row_bound.execute().unwrap();
    assert_eq!(zero_row_reader.schema(), non_empty_schema);
    assert_eq!(row_count(&drain(zero_row_reader)), 0);

    // An empty append sends no mutations, so the driver must check that the table exists.
    let error = ingest_stream(connection, "AdbcNoSuchZeroTable", "append", &schema, vec![]);
    assert_eq!(error.unwrap_err().status, Status::NotFound);

    // Executing DML clears the schema from an earlier empty `bind_stream`, so a later ingest on
    // the same handle fails instead of creating a table from the stale schema.
    let mut reused = connection.new_statement().unwrap();
    reused.bind_stream(batch_stream(&schema, vec![])).unwrap();
    reused
        .set_sql_query("DELETE FROM AdbcEmptyHijack WHERE Id = -1")
        .unwrap();
    reused.execute_update().unwrap();
    set_ingest_target(&mut reused, "AdbcStaleLeak", "create");
    assert_eq!(
        reused.execute_update().unwrap_err().status,
        Status::InvalidState
    );
    // Creating the table now succeeds only if the failed ingest did not create it.
    let count = ingest_stream(connection, "AdbcStaleLeak", "create", &schema, vec![]);
    assert_eq!(count.unwrap(), Some(0));

    // The same existence check applies in a manual transaction.
    set_autocommit(connection, false);
    let error = ingest_stream(
        connection,
        "AdbcNoSuchManualZeroTable",
        "append",
        &schema,
        vec![],
    );
    assert_eq!(error.unwrap_err().status, Status::NotFound);
    connection.rollback().unwrap();
    set_autocommit(connection, true);

    // A create-mode ingest in a read-only transaction is rejected before its DDL runs.
    set_autocommit(connection, false);
    query(connection, "SELECT Id FROM AdbcEmptyHijack");
    let error = ingest_stream(connection, "AdbcEmptyReadTxn", "create", &schema, vec![]);
    assert_eq!(error.unwrap_err().status, Status::InvalidState);
    connection.rollback().unwrap();
    set_autocommit(connection, true);
    let count = ingest_stream(connection, "AdbcEmptyReadTxn", "create", &schema, vec![]);
    assert_eq!(count.unwrap(), Some(0), "the rejected create left no table");

    drop_tables(
        connection,
        &["AdbcEmptyReadTxn", "AdbcStaleLeak", "AdbcEmptyHijack"],
    );
}

/// `create_append` onto a table without a bound column fails with `AlreadyExists` at execution,
/// as `append` does, even when no insert runs then to report it.
#[test]
fn bulk_ingest_create_append_reports_a_mismatch_at_execution() {
    let Some(mut fx) = fixture() else { return };
    let connection = &mut fx.connection;
    let table = "AdbcCreateAppendMismatch";
    recreate(connection, table, "Id INT64");
    let batch =
        RecordBatch::try_from_iter([("Id", int_array(&[1])), ("Extra", int_array(&[2]))]).unwrap();
    for (autocommit, batch) in [
        (true, batch.clone()),
        (true, batch.slice(0, 0)),
        (false, batch),
    ] {
        set_autocommit(connection, autocommit);
        let error = ingest(connection, table, "create_append", batch).unwrap_err();
        assert_eq!(error.status, Status::AlreadyExists, "{error}");
    }
    connection.rollback().unwrap();
    set_autocommit(connection, true);
    drop_tables(connection, &[table]);
}

/// When a later chunk of a multi-chunk ingest fails, the error reports how many rows earlier
/// chunks already committed.
#[test]
fn bulk_ingest_mid_chunk_failure_reports_committed_rows() {
    let Some(mut fx) = fixture() else { return };
    let connection = &mut fx.connection;

    recreate(connection, "AdbcMidFail", "Id INT64, Name STRING(MAX)");

    // The last row duplicates the first row's key, so the first chunk commits and the last one
    // fails wherever the boundaries fall.
    let rows = oversized_rows(&[0, 1, 2, 3, 4, 0]);
    let error = ingest(connection, "AdbcMidFail", "append", rows).unwrap_err();
    assert_eq!(error.status, Status::Integrity, "{error}");
    assert!(error.message.contains("AdbcMidFail"), "{error}");

    let committed = count_rows(connection, "AdbcMidFail");
    assert!((1..6).contains(&committed), "{committed}");
    let note = format!("{committed} row(s) from this bulk ingest were already committed");
    assert!(error.message.contains(&note), "{error}");

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
    let tables = objects_tables(batches);
    tables
        .iter()
        .flat_map(|t| string_values(t.column(0)))
        .collect()
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
            if table_names.value(r) == table && column_lists.is_valid(r) {
                let columns = column_lists.value(r);
                names.extend(string_values(col::<StructArray>(&columns).column(0)));
            }
        }
    }
    names
}

/// `get_objects` pattern filters are pushed into `INFORMATION_SCHEMA` queries; results must match
/// filtering the unfiltered result client-side.
#[test]
fn get_objects_filter_pushdown_matches_client_filtering() {
    let Some(mut fx) = fixture() else { return };
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
        drain(
            connection
                .get_objects(ObjectDepth::All, None, None, table_name, None, column_name)
                .unwrap(),
        )
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
        assert_eq!(filtered, expected, "{pattern:?}");
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
            "{pattern:?}"
        );
    }

    // A table filter matching nothing keeps every schema, with an empty table list.
    let none = collect(connection, Some("GoFilterNone%"), None);
    assert!(objects_table_names(&none).is_empty());
    assert_empty_table_lists(&none[0]);

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
                    assert!(usage_lists.is_valid(k));
                    let usage = usage_lists.value(k);
                    let usage = col::<StructArray>(&usage);
                    assert_eq!(usage.len(), 1);
                    let value = |name| {
                        col::<StringArray>(usage.column_by_name(name).unwrap())
                            .value(0)
                            .to_string()
                    };
                    fk_usage = Some((value("fk_table"), value("fk_column_name")));
                }
            }
        }
    }
    assert_eq!(
        fk_usage,
        Some(("GoFilterAlpha".to_string(), "Id".to_string()))
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
fn prop_config(cases: u32) -> ProptestConfig {
    ProptestConfig {
        cases,
        failure_persistence: None,
        ..ProptestConfig::default()
    }
}

fn date_strategy() -> impl Strategy<Value = NaiveDate> {
    (1i32..=9999, 1u32..=12, 1u32..=28)
        .prop_map(|(y, m, d)| NaiveDate::from_ymd_opt(y, m, d).unwrap())
}

/// Stays within the Arrow nanosecond range (1677-09-21 to 2262-04-11).
fn timestamp_strategy() -> impl Strategy<Value = DateTime<Utc>> {
    (
        1678i32..=2261,
        1u32..=12,
        1u32..=28,
        0u32..24,
        0u32..60,
        0u32..60,
        0u32..1_000_000,
    )
        .prop_map(|(y, mo, d, h, mi, s, us)| {
            NaiveDate::from_ymd_opt(y, mo, d)
                .unwrap()
                .and_hms_micro_opt(h, mi, s, us)
                .unwrap()
                .and_utc()
        })
}

/// An unscaled `NUMERIC`: nine fractional digits, up to 28 integer digits.
fn numeric_strategy() -> impl Strategy<Value = i128> {
    (any::<bool>(), 0i128..10i128.pow(37)).prop_map(|(neg, mag)| if neg { -mag } else { mag })
}

fn days_since_epoch(date: NaiveDate) -> i32 {
    (date - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32
}

/// The value in row 0, or `None` if it is null.
fn first<'a, T: Array + 'static>(column: &'a dyn Array) -> Option<<&'a T as ArrayAccessor>::Item>
where
    &'a T: ArrayAccessor,
{
    let array = col::<T>(column);
    array.is_valid(0).then(|| array.value(0))
}

/// Scalar values bound as parameters read back unchanged, nulls included.
#[test]
fn prop_bind_round_trip() {
    let Some(fx) = fixture() else { return };
    let connection = RefCell::new(fx.connection);
    // NaN and infinities do not compare with `==`.
    let float = any::<f64>().prop_filter("finite", |f| f.is_finite());
    // Up to 24 non-control characters.
    let text = proptest::string::string_regex("\\PC{0,24}").unwrap();
    let bytes = proptest::collection::vec(any::<u8>(), 0..24);

    proptest!(prop_config(PROP_CASES), |(
        oi in proptest::option::of(any::<i64>()),
        of in proptest::option::of(float),
        ob in proptest::option::of(any::<bool>()),
        os in proptest::option::of(text),
        oby in proptest::option::of(bytes),
        od in proptest::option::of(date_strategy()),
        ot in proptest::option::of(timestamp_strategy()),
        on in proptest::option::of(numeric_strategy()),
    )| {
        let mut conn = connection.borrow_mut();

        let exp_days = od.map(days_since_epoch);
        let exp_micros = ot.map(|t| t.timestamp_micros());
        let num_col = Decimal128Array::from(vec![on])
            .with_precision_and_scale(38, 9)
            .unwrap();
        let batch = RecordBatch::try_from_iter_with_nullable([
            ("Id", int_array(&[1]), false),
            ("IntCol", Arc::new(Int64Array::from(vec![oi])) as ArrayRef, true),
            ("FloatCol", Arc::new(Float64Array::from(vec![of])), true),
            ("BoolCol", Arc::new(BooleanArray::from(vec![ob])), true),
            ("StrCol", Arc::new(StringArray::from(vec![os.as_deref()])), true),
            ("BytesCol", Arc::new(BinaryArray::from_opt_vec(vec![oby.as_deref()])), true),
            ("DateCol", Arc::new(Date32Array::from(vec![exp_days])), true),
            ("TsCol", Arc::new(TimestampMicrosecondArray::from(vec![exp_micros])), true),
            ("NumCol", Arc::new(num_col), true),
        ])
        .unwrap();

        run(&mut *conn, "DELETE FROM AdbcPropBind WHERE true");
        let mut ins = prepare(
            &mut *conn,
            "INSERT INTO AdbcPropBind \
                 (Id, IntCol, FloatCol, BoolCol, StrCol, BytesCol, DateCol, TsCol, NumCol) \
             VALUES (@Id, @IntCol, @FloatCol, @BoolCol, @StrCol, @BytesCol, @DateCol, @TsCol, @NumCol)",
        );
        ins.bind(batch).unwrap();
        prop_assert_eq!(ins.execute_update().unwrap(), Some(1));

        let batches = query(
            &mut *conn,
            "SELECT IntCol, FloatCol, BoolCol, StrCol, BytesCol, DateCol, TsCol, NumCol \
             FROM AdbcPropBind WHERE Id = 1",
        );
        prop_assert_eq!(row_count(&batches), 1);
        let b = &batches[0];

        prop_assert_eq!(first::<Int64Array>(b.column(0)), oi);
        prop_assert_eq!(first::<Float64Array>(b.column(1)), of);
        prop_assert_eq!(first::<BooleanArray>(b.column(2)), ob);
        prop_assert_eq!(first::<StringArray>(b.column(3)), os.as_deref());
        prop_assert_eq!(first::<BinaryArray>(b.column(4)), oby.as_deref());
        prop_assert_eq!(first::<Date32Array>(b.column(5)), exp_days);
        // Bound as microseconds, read back as nanoseconds.
        prop_assert_eq!(
            first::<TimestampNanosecondArray>(b.column(6)),
            exp_micros.map(|v| v * 1_000)
        );
        prop_assert_eq!(first::<Decimal128Array>(b.column(7)), on);
    });
}

/// DATE, TIMESTAMP and NUMERIC values written as SQL literals read back as the expected Arrow
/// values.
#[test]
fn prop_temporal_round_trip() {
    let Some(fx) = fixture() else { return };
    let connection = RefCell::new(fx.connection);

    proptest!(prop_config(PROP_TEMPORAL_CASES), |(
        od in proptest::option::of(date_strategy()),
        ot in proptest::option::of(timestamp_strategy()),
        on in proptest::option::of(numeric_strategy()),
    )| {
        let mut conn = connection.borrow_mut();

        let literal = |value: Option<String>| value.unwrap_or_else(|| "NULL".to_string());
        let d_lit = literal(od.map(|d| format!("DATE '{}'", d.format("%Y-%m-%d"))));
        let t_lit = literal(ot.map(|t| {
            format!("TIMESTAMP '{}'", t.to_rfc3339_opts(SecondsFormat::Micros, true))
        }));
        let n_lit = literal(on.map(|n| {
            let (sign, mag) = (if n < 0 { "-" } else { "" }, n.unsigned_abs());
            let (int, frac) = (mag / 1_000_000_000, mag % 1_000_000_000);
            format!("NUMERIC '{sign}{int}.{frac:09}'")
        }));

        run(&mut *conn, "DELETE FROM AdbcPropTypes WHERE true");
        run(
            &mut *conn,
            format!(
                "INSERT INTO AdbcPropTypes (Id, D, T, N) VALUES (1, {d_lit}, {t_lit}, {n_lit})"
            ),
        );

        let batches = query(&mut *conn, "SELECT D, T, N FROM AdbcPropTypes WHERE Id = 1");
        prop_assert_eq!(row_count(&batches), 1);
        let b = &batches[0];

        prop_assert_eq!(first::<Date32Array>(b.column(0)), od.map(days_since_epoch));
        prop_assert_eq!(
            first::<TimestampNanosecondArray>(b.column(1)),
            ot.map(|t| t.timestamp_nanos_opt().unwrap())
        );
        prop_assert_eq!(first::<Decimal128Array>(b.column(2)), on);
    });
}

/// Bound rows must not survive a DDL statement on a reused statement handle.
#[test]
fn ddl_execute_clears_stale_bound_rows() {
    let Some(mut fx) = fixture() else { return };
    let connection = &mut fx.connection;

    run(connection, "DROP TABLE IF EXISTS BoundLeak");

    let mut stmt = connection.new_statement().unwrap();
    stmt.bind(int_batch("id", &[900001, 900002])).unwrap();

    stmt.set_sql_query("CREATE TABLE BoundLeak (id INT64) PRIMARY KEY (id)")
        .unwrap();
    stmt.execute_update().unwrap();

    // The unbound parameter should make this fail; the row count is what matters.
    stmt.set_sql_query("INSERT INTO BoundLeak (id) VALUES (@id)")
        .unwrap();
    let _ = stmt.execute_update();

    assert_eq!(count_rows(connection, "BoundLeak"), 0);

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
    .unwrap();
    let database = driver
        .new_database_with_opts([(
            OptionDatabase::Uri,
            OptionValue::String(target.database_uri()),
        )])
        .unwrap();
    let mut connection = connect_with_retry(&database);

    let reader = connection.get_info(None).unwrap();
    assert_eq!(reader.schema(), GET_INFO_SCHEMA.clone());
    assert!(row_count(&drain(reader)) >= 4, "at least the core codes");
    let subset = connection
        .get_info(Some([InfoCode::VendorName, InfoCode::DriverName].into()))
        .unwrap();
    assert_eq!(subset.schema(), GET_INFO_SCHEMA.clone());
    assert_eq!(row_count(&drain(subset)), 2);
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
        .unwrap();
    assert_eq!(unknown.schema(), GET_INFO_SCHEMA.clone());
    let unknown_codes: Vec<u32> = drain(unknown)
        .iter()
        .flat_map(|b| col::<UInt32Array>(b.column(0)).values().to_vec())
        .collect();
    assert_eq!(unknown_codes, [u32::from(&InfoCode::DriverName)]);

    let reader = connection.get_table_types().unwrap();
    assert_eq!(reader.schema(), GET_TABLE_TYPES_SCHEMA.clone());
    let types = strings(&drain(reader), 0);
    assert!(types.contains(&"BASE TABLE".to_string()) && types.contains(&"VIEW".to_string()));

    run(
        &mut connection,
        "DROP TABLE IF EXISTS AdbcConf; \
         CREATE TABLE AdbcConf (Id INT64, Name STRING(MAX)) PRIMARY KEY (Id)",
    );

    let reader = connection
        .get_objects(ObjectDepth::All, None, None, None, None, None)
        .unwrap();
    assert_eq!(reader.schema(), GET_OBJECTS_SCHEMA.clone());
    assert_eq!(row_count(&drain(reader)), 1);

    let schema = connection.get_table_schema(None, None, "AdbcConf").unwrap();
    assert_fields(
        schema.fields(),
        &[("Id", DataType::Int64), ("Name", DataType::Utf8)],
    );

    let insert = "INSERT INTO AdbcConf (Id, Name) VALUES (1, 'a')";
    assert_eq!(run(&mut connection, insert), Some(1));
    assert_eq!(count_rows(&mut connection, "AdbcConf"), 1);

    set_autocommit(&mut connection, false);
    let insert = "INSERT INTO AdbcConf (Id, Name) VALUES (2, 'b')";
    assert_eq!(run(&mut connection, insert), None);
    assert_query_guarded(&mut connection, "AdbcConf");
    connection.commit().unwrap();
    assert_eq!(count_rows_ending_txn(&mut connection, "AdbcConf"), 2);
    let insert = "INSERT INTO AdbcConf (Id, Name) VALUES (3, 'c')";
    assert_eq!(run(&mut connection, insert), None);
    connection.rollback().unwrap();
    assert_eq!(count_rows(&mut connection, "AdbcConf"), 2);
    set_autocommit(&mut connection, true);

    let rows = id_text_batch("Name", &[10, 11], &["x", "y"]);
    let count = ingest(&mut connection, "AdbcConf", "append", rows).unwrap();
    assert_eq!(count, Some(2));
    assert_eq!(count_rows(&mut connection, "AdbcConf"), 4);

    run(&mut connection, "DROP TABLE AdbcConf");
}

/// Property graph DDL and GQL queries need no special driver support.
#[test]
fn gql_graph_query_round_trip() {
    let Some(mut fx) = fixture() else { return };
    let connection = &mut fx.connection;

    // The property graph is created in a second batch, after its tables exist.
    let tables = "DROP PROPERTY GRAPH IF EXISTS GqlFinGraph; \
                  DROP TABLE IF EXISTS GqlTransfer; \
                  DROP TABLE IF EXISTS GqlAccount; \
                  CREATE TABLE GqlAccount (Id INT64 NOT NULL, Name STRING(MAX)) PRIMARY KEY (Id); \
                  CREATE TABLE GqlTransfer ( \
                      Id INT64 NOT NULL, ToId INT64 NOT NULL, Amount FLOAT64 \
                  ) PRIMARY KEY (Id, ToId)";
    assert_eq!(run(connection, tables), None);
    let graph = "CREATE PROPERTY GRAPH GqlFinGraph \
                     NODE TABLES (GqlAccount KEY (Id) LABEL Account PROPERTIES (Id, Name)) \
                     EDGE TABLES ( \
                         GqlTransfer \
                             KEY (Id, ToId) \
                             SOURCE KEY (Id) REFERENCES GqlAccount (Id) \
                             DESTINATION KEY (ToId) REFERENCES GqlAccount (Id) \
                             LABEL Transfer PROPERTIES (Amount) \
                     )";
    assert_eq!(run(connection, graph), None);

    let accounts =
        "INSERT INTO GqlAccount (Id, Name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')";
    assert_eq!(run(connection, accounts), Some(3));
    let transfers = "INSERT INTO GqlTransfer (Id, ToId, Amount) VALUES (1, 2, 100.0), (2, 3, 42.5)";
    assert_eq!(run(connection, transfers), Some(2));

    let reader = prepare(
        connection,
        "GRAPH GqlFinGraph \
         MATCH (a:Account)-[t:Transfer]->(b:Account) \
         RETURN a.Name AS src, b.Name AS dst, t.Amount AS amount",
    )
    .execute()
    .unwrap();
    assert_fields(
        reader.schema().fields(),
        &[
            ("src", DataType::Utf8),
            ("dst", DataType::Utf8),
            ("amount", DataType::Float64),
        ],
    );
    let batches = drain(reader);
    assert_eq!(row_count(&batches), 2);

    // The match has no ORDER BY, so sort before comparing.
    let mut edges = Vec::new();
    for batch in &batches {
        let src = col::<StringArray>(batch.column(0));
        let dst = col::<StringArray>(batch.column(1));
        let amount = col::<Float64Array>(batch.column(2));
        for i in 0..batch.num_rows() {
            edges.push((src.value(i), dst.value(i), amount.value(i)));
        }
    }
    edges.sort_by(|a, b| a.2.partial_cmp(&b.2).unwrap());
    assert_eq!(edges, [("Bob", "Carol", 42.5), ("Alice", "Bob", 100.0)]);

    run(
        connection,
        "DROP PROPERTY GRAPH IF EXISTS GqlFinGraph; \
         DROP TABLE IF EXISTS GqlTransfer; \
         DROP TABLE IF EXISTS GqlAccount",
    );
}

/// A table in a named schema works with DML, queries, `get_table_schema`, `get_objects` and
/// `adbc.ingest.target_db_schema`. Other tests only use the default schema (`''`).
#[test]
fn named_schema_round_trip() {
    let Some(mut fx) = fixture() else { return };
    let connection = &mut fx.connection;

    let ddl = "DROP TABLE IF EXISTS adbc_ns.Widget; \
               CREATE SCHEMA IF NOT EXISTS adbc_ns; \
               CREATE TABLE adbc_ns.Widget ( \
                   Id INT64 NOT NULL, Label STRING(MAX) \
               ) PRIMARY KEY (Id)";
    assert_eq!(run(connection, ddl), None);

    let insert = "INSERT INTO adbc_ns.Widget (Id, Label) VALUES (1, 'alpha'), (2, 'beta')";
    assert_eq!(run(connection, insert), Some(2));
    assert_eq!(count_rows(connection, "adbc_ns.Widget"), 2);

    let widget_schema = connection
        .get_table_schema(None, Some("adbc_ns"), "Widget")
        .unwrap();
    assert_fields(
        widget_schema.fields(),
        &[("Id", DataType::Int64), ("Label", DataType::Utf8)],
    );

    let missing_default = connection
        .get_table_schema(None, Some(""), "Widget")
        .unwrap_err();
    assert_eq!(missing_default.status, Status::NotFound);

    let objects = drain(
        connection
            .get_objects(
                ObjectDepth::All,
                None,
                Some("adbc_ns"),
                Some("Widget"),
                None,
                None,
            )
            .unwrap(),
    );

    let schemas = list_structs(objects[0].column(1));
    let idx = string_values(schemas.column(0))
        .iter()
        .position(|name| name == "adbc_ns")
        .unwrap();
    let table_lists = col::<ListArray>(schemas.column(1));
    let tables = table_lists.value(idx);
    let tables = col::<StructArray>(&tables);
    assert_eq!(string_values(tables.column(0)), ["Widget"]);
    let columns = list_structs(tables.column(2));
    assert_eq!(string_values(columns.column(0)), ["Id", "Label"]);

    let rows = id_text_batch("Label", &[3, 4], &["gamma", "delta"]);
    let schema = rows.schema();
    let mut ingest = ingest_stmt(connection, "Widget", "append");
    ingest
        .set_option(OptionStatement::TargetDbSchema, "adbc_ns".into())
        .unwrap();
    ingest
        .bind_stream(batch_stream(&schema, vec![rows]))
        .unwrap();
    assert_eq!(ingest.execute_update().unwrap(), Some(2));
    assert_eq!(count_rows(connection, "adbc_ns.Widget"), 4);

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
            let mut driver = SpannerDriver::try_new().unwrap();
            let database = driver
                .new_database_with_opts([(
                    OptionDatabase::Uri,
                    OptionValue::String(target.database_uri()),
                )])
                .unwrap();
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
fn read_fixture() -> Option<Fixture> {
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

#[track_caller]
fn prepare<C: Connection>(connection: &mut C, sql: impl AsRef<str>) -> C::StatementType {
    let mut statement = connection.new_statement().unwrap();
    statement.set_sql_query(sql).unwrap();
    statement
}

/// Runs DML or DDL and returns the affected-row count.
#[track_caller]
fn run<C: Connection>(connection: &mut C, sql: impl AsRef<str>) -> Option<i64> {
    let sql = sql.as_ref();
    prepare(connection, sql)
        .execute_update()
        .unwrap_or_else(|e| panic!("failed to run {sql:?}: {e:?}"))
}

#[track_caller]
fn drain(reader: impl Iterator<Item = Result<RecordBatch, ArrowError>>) -> Vec<RecordBatch> {
    reader.collect::<Result<_, _>>().unwrap()
}

#[track_caller]
fn fetch(statement: &mut impl Statement) -> Vec<RecordBatch> {
    drain(statement.execute().unwrap())
}

#[track_caller]
fn query<C: Connection>(connection: &mut C, sql: impl AsRef<str>) -> Vec<RecordBatch> {
    fetch(&mut prepare(connection, sql))
}

fn row_count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

/// The non-null values of an `Int64` column across `batches`.
#[track_caller]
fn ints(batches: &[RecordBatch], column: usize) -> Vec<i64> {
    let columns = batches.iter().map(|b| col::<Int64Array>(b.column(column)));
    columns.flat_map(|c| c.values().to_vec()).collect()
}

#[track_caller]
fn string_values(column: &dyn Array) -> Vec<String> {
    let column = col::<StringArray>(column);
    column.iter().map(|v| v.unwrap().to_string()).collect()
}

/// The values of a `Utf8` column across `batches`.
#[track_caller]
fn strings(batches: &[RecordBatch], column: usize) -> Vec<String> {
    batches
        .iter()
        .flat_map(|b| string_values(b.column(column)))
        .collect()
}

fn int_array(values: &[i64]) -> ArrayRef {
    Arc::new(Int64Array::from(values.to_vec()))
}

/// A batch with one non-nullable `Int64` column.
fn int_batch(name: &str, values: &[i64]) -> RecordBatch {
    RecordBatch::try_from_iter([(name, int_array(values))]).unwrap()
}

/// A batch of a non-nullable `Id` column and a non-nullable text column.
fn id_text_batch(text_column: &str, ids: &[i64], texts: &[impl AsRef<str>]) -> RecordBatch {
    let texts = StringArray::from_iter_values(texts.iter().map(|text| text.as_ref()));
    RecordBatch::try_from_iter([
        ("Id", int_array(ids)),
        (text_column, Arc::new(texts) as ArrayRef),
    ])
    .unwrap()
}

fn batch_stream(
    schema: &SchemaRef,
    batches: Vec<RecordBatch>,
) -> Box<dyn RecordBatchReader + Send> {
    Box::new(RecordBatchIterator::new(
        batches.into_iter().map(Ok),
        schema.clone(),
    ))
}

#[track_caller]
fn configure<O: Optionable>(target: &mut O, options: &[(&str, &str)])
where
    O::Option: for<'a> From<&'a str>,
{
    for (key, value) in options {
        target.set_option((*key).into(), (*value).into()).unwrap();
    }
}

/// Replaces `table` with an empty table of `columns`, keyed by `Id`.
#[track_caller]
fn recreate(connection: &mut SpannerConnection, table: &str, columns: &str) {
    run(
        connection,
        format!(
            "DROP TABLE IF EXISTS {table}; \
             CREATE TABLE {table} ({columns}) PRIMARY KEY (Id)"
        ),
    );
}

/// Replaces `table` with one holding `Id`s from 1 to `rows`.
#[track_caller]
fn recreate_numbered(connection: &mut SpannerConnection, table: &str, rows: i64) {
    recreate(connection, table, "Id INT64");
    run(
        connection,
        format!("INSERT INTO {table} (Id) SELECT n FROM UNNEST(GENERATE_ARRAY(1, {rows})) AS n"),
    );
}

/// Drop scratch tables in one DDL batch. No `IF EXISTS`, so a test that never created a table
/// fails here.
#[track_caller]
fn drop_tables(connection: &mut SpannerConnection, tables: &[&str]) {
    let drops: Vec<_> = tables.iter().map(|t| format!("DROP TABLE {t}")).collect();
    run(connection, drops.join("; "));
}

#[track_caller]
fn set_ingest_target(statement: &mut impl Statement, table: &str, mode: &str) {
    statement
        .set_option(OptionStatement::TargetTable, table.into())
        .unwrap();
    statement
        .set_option(OptionStatement::IngestMode, mode.into())
        .unwrap();
}

#[track_caller]
fn ingest_stmt<C: Connection>(connection: &mut C, table: &str, mode: &str) -> C::StatementType {
    let mut statement = connection.new_statement().unwrap();
    set_ingest_target(&mut statement, table, mode);
    statement
}

fn ingest<C: Connection>(
    connection: &mut C,
    table: &str,
    mode: &str,
    rows: RecordBatch,
) -> AdbcResult<Option<i64>> {
    let mut statement = ingest_stmt(connection, table, mode);
    statement.bind(rows).unwrap();
    statement.execute_update()
}

fn ingest_stream(
    connection: &mut SpannerConnection,
    table: &str,
    mode: &str,
    schema: &SchemaRef,
    batches: Vec<RecordBatch>,
) -> AdbcResult<Option<i64>> {
    let mut statement = ingest_stmt(connection, table, mode);
    statement
        .bind_stream(batch_stream(schema, batches))
        .unwrap();
    statement.execute_update()
}

/// The status of the driver error that a reader wraps in `ArrowError::ExternalError`.
fn reader_error_status(error: &ArrowError) -> Status {
    let ArrowError::ExternalError(source) = error else {
        panic!("expected the reader to surface the driver error, got: {error}");
    };
    source
        .downcast_ref::<adbc_core::error::Error>()
        .unwrap_or_else(|| panic!("the reader error does not wrap an ADBC error: {error}"))
        .status
}

/// Downcast a column, panicking with its actual data type on mismatch.
#[track_caller]
fn col<T: Array + 'static>(column: &dyn Array) -> &T {
    column.as_any().downcast_ref::<T>().unwrap_or_else(|| {
        panic!(
            "expected the column to be a {}, but it is {:?}",
            std::any::type_name::<T>(),
            column.data_type()
        )
    })
}

/// The struct elements of the first list in `array`.
#[track_caller]
fn list_structs(array: &dyn Array) -> StructArray {
    col::<StructArray>(&col::<ListArray>(array).value(0)).clone()
}

#[track_caller]
fn list_item(data_type: &DataType) -> &FieldRef {
    let DataType::List(item) = data_type else {
        panic!("expected a List, got {data_type:?}");
    };
    item
}

#[track_caller]
fn struct_fields(data_type: &DataType) -> &Fields {
    let DataType::Struct(fields) = data_type else {
        panic!("expected a Struct, got {data_type:?}");
    };
    fields
}

fn field_names(fields: &Fields) -> Vec<&str> {
    fields.iter().map(|f| f.name().as_str()).collect()
}

/// Asserts the names and types of `fields`.
#[track_caller]
fn assert_fields(fields: &Fields, expected: &[(&str, DataType)]) {
    let actual: Vec<_> = fields
        .iter()
        .map(|f| (f.name().as_str(), f.data_type().clone()))
        .collect();
    assert_eq!(actual, expected);
}

/// Column and constraint names that `get_objects` reports for `table`.
#[track_caller]
fn table_columns_and_constraints(
    connection: &mut SpannerConnection,
    table: &str,
) -> (Vec<String>, Vec<String>) {
    let batches = drain(
        connection
            .get_objects(ObjectDepth::All, None, Some(""), Some(table), None, None)
            .unwrap(),
    );
    let schemas = list_structs(batches[0].column(1));
    let tables = list_structs(schemas.column(1));
    assert_eq!(tables.len(), 1, "expected exactly one {table} row");
    let names =
        |strukt: &StructArray, field: &str| string_values(strukt.column_by_name(field).unwrap());
    (
        names(&list_structs(tables.column(2)), "column_name"),
        names(&list_structs(tables.column(3)), "constraint_name"),
    )
}

#[track_caller]
fn count_rows<C: Connection>(connection: &mut C, table: &str) -> i64 {
    ints(
        &query(connection, format!("SELECT COUNT(*) AS n FROM {table}")),
        0,
    )[0]
}

/// Count rows in a manual transaction, then end the read-only transaction the count opened so
/// the next statement can write.
#[track_caller]
fn count_rows_ending_txn<C: Connection>(connection: &mut C, table: &str) -> i64 {
    let n = count_rows(connection, table);
    connection.rollback().unwrap();
    n
}

/// Run DML in a manual transaction, which buffers it and reports no count.
#[track_caller]
fn buffer_dml(connection: &mut SpannerConnection, sql: &str) {
    assert_eq!(run(connection, sql), None);
}

#[track_caller]
fn set_autocommit<C: Optionable<Option = OptionConnection>>(connection: &mut C, enabled: bool) {
    connection
        .set_option(OptionConnection::AutoCommit, enabled.into())
        .unwrap_or_else(|e| panic!("set autocommit to {enabled}: {e:?}"));
}

/// Assert that a query is rejected while the manual transaction has buffered writes.
#[track_caller]
fn assert_query_guarded<C: Connection>(connection: &mut C, table: &str) {
    let mut q = prepare(connection, format!("SELECT COUNT(*) AS n FROM {table}"));
    let Err(error) = q.execute() else {
        panic!("a query with buffered writes must be rejected, not silently run");
    };
    assert_eq!(error.status, Status::InvalidState);
}

/// (table, column, key, value) tuples from a `get_statistics` batch.
fn extract_statistics(batch: &RecordBatch) -> Vec<(String, Option<String>, i16, i64)> {
    let mut out = Vec::new();
    let db_schemas = list_structs(batch.column(1));
    let stats_list = col::<ListArray>(db_schemas.column_by_name("db_schema_statistics").unwrap());
    for i in 0..db_schemas.len() {
        let stats = stats_list.value(i);
        let stats = col::<StructArray>(&stats);
        let table = col::<StringArray>(stats.column_by_name("table_name").unwrap());
        let column = col::<StringArray>(stats.column_by_name("column_name").unwrap());
        let key = col::<Int16Array>(stats.column_by_name("statistic_key").unwrap());
        let value = col::<UnionArray>(stats.column_by_name("statistic_value").unwrap());
        for r in 0..stats.len() {
            let column_name = column.is_valid(r).then(|| column.value(r).to_string());
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
        ADBC_STATISTIC_DISTINCT_COUNT_KEY as DISTINCT, ADBC_STATISTIC_NULL_COUNT_KEY as NULLS,
        ADBC_STATISTIC_ROW_COUNT_KEY as ROWS,
    };
    let Some(mut fx) = fixture() else { return };
    let connection = &mut fx.connection;

    // UUID must be treated as groupable so it gets a distinct count. INTERVAL, which is not, is
    // covered by a unit test because the emulator rejects INTERVAL columns.
    run(
        connection,
        "DROP TABLE IF EXISTS AdbcStats; \
         DROP TABLE IF EXISTS AdbcStatsEmpty; \
         CREATE TABLE AdbcStats (Id INT64, Name STRING(MAX), Tag UUID) PRIMARY KEY (Id); \
         CREATE TABLE AdbcStatsEmpty (Id INT64) PRIMARY KEY (Id)",
    );
    run(
        connection,
        "INSERT INTO AdbcStats (Id, Name, Tag) VALUES \
         (1, 'a', CAST('11111111-2222-3333-4444-555555555555' AS UUID)), \
         (2, 'a', CAST('11111111-2222-3333-4444-555555555555' AS UUID)), \
         (3, NULL, NULL)",
    );

    let batches = drain(
        connection
            .get_statistics(None, None, Some("AdbcStats"), false)
            .unwrap(),
    );
    let stats = extract_statistics(&batches[0]);
    for (column, key, value) in [
        (None, ROWS, 3),
        (Some("Name"), NULLS, 1),
        (Some("Name"), DISTINCT, 1),
        (Some("Id"), NULLS, 0),
        (Some("Id"), DISTINCT, 3),
        (Some("Tag"), NULLS, 1),
        (Some("Tag"), DISTINCT, 1),
    ] {
        assert!(
            stats.iter().any(|(t, c, k, v)| t == "AdbcStats"
                && c.as_deref() == column
                && *k == key
                && *v == value),
            "{column:?} {key} = {value}: {stats:?}"
        );
    }

    // Spanner has no cheaper source of approximate statistics.
    let approx = drain(
        connection
            .get_statistics(None, None, Some("AdbcStats"), true)
            .unwrap(),
    );
    assert_eq!(extract_statistics(&approx[0]), stats);

    // Both tables match the patterns and share the default schema's one entry.
    let both = drain(
        connection
            .get_statistics(None, Some(""), Some("AdbcStats%"), false)
            .unwrap(),
    );
    let db_schemas = col::<ListArray>(both[0].column(1)).value(0);
    assert_eq!(db_schemas.len(), 1, "one entry for the default schema");
    let both = extract_statistics(&both[0]);
    assert!(
        both.iter()
            .any(|(t, c, k, v)| t == "AdbcStatsEmpty" && c.is_none() && *k == ROWS && *v == 0),
        "{both:?}"
    );
    assert_eq!(
        both.iter()
            .filter(|(t, ..)| t == "AdbcStats")
            .cloned()
            .collect::<Vec<_>>(),
        stats
    );

    drop_tables(connection, &["AdbcStats", "AdbcStatsEmpty"]);
}

/// JSON maps to `Utf8` tagged `arrow.json`, and FLOAT32 to `Float32`, in reads, binds and ingest.
#[test]
fn json_and_float32_round_trip() {
    let Some(mut fx) = fixture() else { return };
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

    let extension = |field: &Field| field.metadata().get("ARROW:extension:name").cloned();
    let arrow_json = Some("arrow.json".to_string());

    let reader = prepare(connection, "SELECT Doc, Ratio FROM AdbcJson ORDER BY Id")
        .execute()
        .unwrap();
    let schema = reader.schema();
    assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
    assert_eq!(extension(schema.field(0)), arrow_json);
    assert_eq!(schema.field(1).data_type(), &DataType::Float32);

    let batches = drain(reader);
    assert_eq!(row_count(&batches), 2);
    let docs = col::<StringArray>(batches[0].column(0));
    let ratios = col::<Float32Array>(batches[0].column(1));

    // Backends normalize JSON text differently.
    let doc = docs.value(0);
    assert!(
        doc.contains(r#""a":1"#) && doc.contains(r#""b":"x""#),
        "{doc:?}"
    );
    assert!(docs.is_null(1));
    assert_eq!(ratios.value(0), 1.5f32);
    assert!(ratios.is_null(1));

    // Tagged parameters must bind as JSON; Spanner rejects STRING parameters for JSON columns.
    let json_field = |name: &str| {
        Field::new(name, DataType::Utf8, true).with_metadata(HashMap::from([(
            "ARROW:extension:name".to_string(),
            "arrow.json".to_string(),
        )]))
    };
    let docs_item = Arc::new(json_field("item"));
    let mut docs_builder = ListBuilder::new(StringBuilder::new()).with_field(docs_item.clone());
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
            int_array(&[3, 4]),
            Arc::new(StringArray::from(vec![Some(r#"{"c":[1,2]}"#), None])),
            Arc::new(docs_builder.finish()),
        ],
    )
    .unwrap();
    let mut insert = prepare(
        connection,
        "INSERT INTO AdbcJson (Id, Doc, Docs) VALUES (@Id, @Doc, @Docs)",
    );
    insert.bind(bind_batch.clone()).unwrap();
    insert.execute_update().unwrap();

    let reader = prepare(
        connection,
        "SELECT Doc, Docs FROM AdbcJson WHERE Id >= 3 ORDER BY Id",
    )
    .execute()
    .unwrap();
    assert_eq!(
        extension(list_item(reader.schema().field(1).data_type())),
        arrow_json
    );
    let batches = drain(reader);
    assert_eq!(row_count(&batches), 2);
    let doc_col = col::<StringArray>(batches[0].column(0));
    assert!(doc_col.value(0).contains(r#""c":[1,2]"#), "{doc_col:?}");
    assert!(doc_col.is_null(1));
    let docs_col = col::<ListArray>(batches[0].column(1));
    let first_cell = docs_col.value(0);
    let first_cell = col::<StringArray>(&first_cell);
    assert_eq!(first_cell.len(), 2);
    assert!(
        first_cell.value(0).contains(r#""d":true"#),
        "{first_cell:?}"
    );
    assert!(first_cell.is_null(1));
    assert!(docs_col.is_null(1));

    // Create-mode ingest makes tagged fields JSON columns.
    ingest(connection, "AdbcJsonIngest", "create", bind_batch).unwrap();
    let columns = query(
        connection,
        "SELECT COLUMN_NAME, SPANNER_TYPE FROM INFORMATION_SCHEMA.COLUMNS \
         WHERE TABLE_NAME = 'AdbcJsonIngest' AND COLUMN_NAME IN ('Doc', 'Docs') \
         ORDER BY COLUMN_NAME",
    );
    assert_eq!(strings(&columns, 0), ["Doc", "Docs"]);
    assert_eq!(strings(&columns, 1), ["JSON", "ARRAY<JSON>"]);

    drop_tables(connection, &["AdbcJson", "AdbcJsonIngest"]);
}

/// A statement running `sql` in batches of `rows`.
#[track_caller]
fn batched_query(connection: &mut SpannerConnection, sql: &str, rows: i64) -> SpannerStatement {
    let mut statement = connection.new_statement().unwrap();
    statement
        .set_option(OPTION_ROWS_PER_BATCH.into(), rows.into())
        .unwrap();
    assert_eq!(
        statement
            .get_option_int(OPTION_ROWS_PER_BATCH.into())
            .unwrap(),
        rows
    );
    statement.set_sql_query(sql).unwrap();
    statement
}

#[test]
fn execute_streams_in_batches() {
    let Some(mut fx) = fixture() else { return };
    let connection = &mut fx.connection;

    recreate_numbered(connection, "AdbcStream", 2500);

    let mut query = batched_query(connection, "SELECT Id FROM AdbcStream ORDER BY Id", 1000);
    let reader = query.execute().unwrap();
    let schema = reader.schema();
    let batches = drain(reader);

    assert!(batches.iter().all(|b| b.schema() == schema));
    let sizes: Vec<usize> = batches.iter().map(RecordBatch::num_rows).collect();
    assert_eq!(sizes, [1000, 1000, 500]);
    assert_eq!(ints(&batches, 0), (1..=2500).collect::<Vec<i64>>());

    drop_tables(connection, &["AdbcStream"]);
}

/// `adbc.statement.bind_by_name` (from the SQLite driver): positional by default even when names
/// match; `true` binds by name and rejects unmatched columns.
#[test]
fn bind_by_name_modes() {
    let Some(mut fx) = fixture() else { return };
    let connection = &mut fx.connection;

    // Columns named after the parameters but in swapped order, so the mode is observable.
    let batch = |names: [&str; 2]| {
        RecordBatch::try_from_iter([(names[0], int_array(&[10])), (names[1], int_array(&[20]))])
            .unwrap()
    };
    // Returns (@a, @b) with `rows` bound; `None` leaves bind_by_name unset.
    let query_pair = |connection: &mut SpannerConnection, rows: RecordBatch, mode: Option<&str>| {
        let mut s = prepare(connection, "SELECT @a AS a_value, @b AS b_value");
        if let Some(value) = mode {
            configure(&mut s, &[(OPTION_BIND_BY_NAME, value)]);
        }
        assert_eq!(
            s.get_option_string(OPTION_BIND_BY_NAME.into()).unwrap(),
            mode.unwrap_or("false")
        );
        s.bind(rows).unwrap();
        let batches = drain(s.execute()?);
        assert_eq!(batches.len(), 1, "one bound row, one result batch");
        Ok::<_, adbc_core::error::Error>((ints(&batches, 0)[0], ints(&batches, 1)[0]))
    };

    for (names, mode, expected) in [
        (["b", "a"], None, (10, 20)),
        (["b", "a"], Some("false"), (10, 20)),
        (["b", "a"], Some("true"), (20, 10)),
        (["a", "x"], None, (10, 20)),
    ] {
        let pair = query_pair(connection, batch(names), mode).unwrap();
        assert_eq!(pair, expected, "{names:?} with {mode:?}");
    }
    let error = query_pair(connection, batch(["a", "x"]), Some("true")).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(
        error.message.contains("cannot bind column \"x\""),
        "{error}"
    );
    // The option has no unset state.
    let error = connection
        .new_statement()
        .unwrap()
        .set_option(OPTION_BIND_BY_NAME.into(), "".into())
        .unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
}

/// A query with several bound rows streams each row's result in `spanner.rows_per_batch` chunks.
#[test]
fn bound_query_streams_in_batches() {
    let Some(mut fx) = fixture() else { return };
    let connection = &mut fx.connection;

    recreate(connection, "AdbcBoundStream", "Id INT64, Grp INT64");
    run(
        connection,
        "INSERT INTO AdbcBoundStream (Id, Grp) \
         SELECT n, MOD(n, 3) FROM UNNEST(GENERATE_ARRAY(1, 1500)) AS n",
    );

    let mut query = batched_query(
        connection,
        "SELECT Id FROM AdbcBoundStream WHERE Grp = @Grp ORDER BY Id",
        200,
    );
    query.bind(int_batch("Grp", &[0, 1, 2])).unwrap();

    let reader = query.execute().unwrap();
    let schema = reader.schema();
    let batches = drain(reader);

    let sizes: Vec<usize> = batches.iter().map(RecordBatch::num_rows).collect();
    assert_eq!(sizes, [200, 200, 100, 200, 200, 100, 200, 200, 100]);
    assert!(batches.iter().all(|b| b.schema() == schema));

    let expected: Vec<i64> = (0..3)
        .flat_map(|group| (1..=1500i64).filter(move |id| id % 3 == group))
        .collect();
    assert_eq!(ints(&batches, 0), expected);

    drop_tables(connection, &["AdbcBoundStream"]);
}

/// A cancel between chunk fetches still cancels the next fetch, and the next execute on the same
/// statement runs normally.
#[test]
fn cancel_between_stream_chunks_cancels_the_next_fetch() {
    let Some(mut fx) = fixture() else { return };
    let connection = &mut fx.connection;

    recreate_numbered(connection, "AdbcCancel", 300);

    let mut query = batched_query(connection, "SELECT Id FROM AdbcCancel ORDER BY Id", 100);
    let mut reader = query.execute().unwrap();

    // `execute` already fetched the first chunk.
    assert_eq!(reader.next().unwrap().unwrap().num_rows(), 100);

    query.get_cancel_handle().try_cancel().unwrap();
    let error = reader.next().unwrap().unwrap_err();
    assert_eq!(reader_error_status(&error), Status::Cancelled, "{error}");

    assert_eq!(row_count(&fetch(&mut query)), 300);

    drop_tables(connection, &["AdbcCancel"]);
}

/// Dropping a reader while a prefetch is in flight leaves the statement usable.
#[test]
fn dropping_reader_mid_stream_aborts_the_prefetch() {
    let Some(mut fx) = fixture() else { return };
    let connection = &mut fx.connection;

    recreate_numbered(connection, "AdbcReaderDrop", 500);

    let mut query = batched_query(connection, "SELECT Id FROM AdbcReaderDrop ORDER BY Id", 50);
    let mut reader = query.execute().unwrap();
    assert_eq!(reader.next().unwrap().unwrap().num_rows(), 50);
    drop(reader);

    assert_eq!(row_count(&fetch(&mut query)), 500);

    drop_tables(connection, &["AdbcReaderDrop"]);
}

/// `Utf8View` and `BinaryView` (polars' defaults), `Int8` and `Date64` parameters bind correctly.
#[test]
fn view_and_narrow_types_bind_round_trip() {
    let Some(mut fx) = fixture() else { return };
    let connection = &mut fx.connection;

    recreate(
        connection,
        "AdbcView",
        "Id INT64, S STRING(MAX), B BYTES(MAX), D DATE",
    );

    let batch = RecordBatch::try_from_iter_with_nullable([
        (
            "id",
            Arc::new(arrow_array::Int8Array::from(vec![1i8, 2])) as ArrayRef,
            false,
        ),
        (
            "s",
            Arc::new(arrow_array::StringViewArray::from(vec![
                Some("view-hello"),
                None,
            ])),
            true,
        ),
        (
            "b",
            Arc::new(arrow_array::BinaryViewArray::from(vec![
                Some(b"view-bytes".as_ref()),
                None,
            ])),
            true,
        ),
        // 2024-01-15
        (
            "d",
            Arc::new(arrow_array::Date64Array::from(vec![
                Some(19_737i64 * 86_400_000),
                None,
            ])),
            true,
        ),
    ])
    .unwrap();

    let mut insert = prepare(
        connection,
        "INSERT INTO AdbcView (Id, S, B, D) VALUES (@id, @s, @b, @d)",
    );
    insert.bind(batch).unwrap();
    assert_eq!(insert.execute_update().unwrap(), Some(2));

    let batches = query(connection, "SELECT S, B, D FROM AdbcView ORDER BY Id");
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
    let Some(mut fx) = fixture() else { return };
    let connection = &mut fx.connection;

    // `AdbcHint` is created during setup; see `SETUP_DDL`.
    run(connection, "DELETE FROM AdbcHint WHERE true");

    // The emulator rejects most statement hints, but accepts `LOCK_SCANNED_RANGES`.
    prepare(
        connection,
        "@{LOCK_SCANNED_RANGES=exclusive} INSERT INTO AdbcHint (Id, N) VALUES (1, 0)",
    )
    .execute()
    .unwrap();

    let update = "@{LOCK_SCANNED_RANGES=exclusive} UPDATE AdbcHint SET N = 1 WHERE true";
    assert_eq!(run(connection, update), Some(1));
}

/// `THEN RETURN` rows come back from `execute()`, with bound parameters too. Manual transactions
/// reject it because buffered DML commits through `ExecuteBatchDml`, which cannot return rows.
#[test]
fn dml_then_return_round_trip() {
    let Some(mut fx) = fixture() else { return };
    let connection = &mut fx.connection;

    // `AdbcReturn` is created during setup; see `SETUP_DDL`.
    run(connection, "DELETE FROM AdbcReturn WHERE true");

    let reader = prepare(
        connection,
        "INSERT INTO AdbcReturn (Id, Name) VALUES (1, 'a'), (2, 'b') THEN RETURN Id, Name",
    )
    .execute()
    .unwrap();
    assert_fields(
        reader.schema().fields(),
        &[("Id", DataType::Int64), ("Name", DataType::Utf8)],
    );
    assert_eq!(ints(&drain(reader), 0), [1, 2]);

    let update = "UPDATE AdbcReturn SET Name = 'x' WHERE Id <= 2 THEN RETURN Id";
    assert_eq!(run(connection, update), Some(2));

    let mut bound = prepare(
        connection,
        "INSERT INTO AdbcReturn (Id, Name) VALUES (@id, @name) THEN RETURN Id",
    );
    bound
        .bind(id_text_batch("name", &[10, 11], &["x", "y"]))
        .unwrap();
    assert_eq!(ints(&fetch(&mut bound), 0), [10, 11]);
    assert_eq!(count_rows(connection, "AdbcReturn"), 4);

    // A failed later row must roll back the entire bound batch, including its first insert.
    bound
        .bind(id_text_batch("name", &[20, 20], &["x", "y"]))
        .unwrap();
    let error = bound.execute().err().unwrap();
    assert_eq!(error.status, Status::Integrity, "{error}");
    assert_eq!(count_rows(connection, "AdbcReturn"), 4);

    set_autocommit(connection, false);
    let error = prepare(
        connection,
        "DELETE FROM AdbcReturn WHERE true THEN RETURN Id",
    )
    .execute_update()
    .unwrap_err();
    assert_eq!(error.status, Status::InvalidState);
    assert!(error.message.contains("THEN RETURN"), "{error}");
    connection.rollback().unwrap();
    set_autocommit(connection, true);
}

/// The ids in every partition of `partitioned`, each of which must appear once.
#[track_caller]
fn read_partition_ids(
    connection: &mut SpannerConnection,
    partitioned: &adbc_core::PartitionedResult,
) -> BTreeSet<i64> {
    let mut seen = BTreeSet::new();
    for token in &partitioned.partitions {
        let reader = connection.read_partition(token).unwrap();
        assert_eq!(*reader.schema(), partitioned.schema);
        for id in ints(&drain(reader), 0) {
            assert!(seen.insert(id), "id {id} appeared more than once");
        }
    }
    seen
}

/// `execute_partitions` and `read_partition` together return every row exactly once.
#[test]
fn execute_partitions_round_trip() {
    let Some(mut fx) = fixture() else { return };
    let connection = &mut fx.connection;

    recreate_numbered(connection, "AdbcPartition", 200);

    // ORDER BY would make the query non-partitionable.
    let mut statement = prepare(connection, "SELECT Id FROM AdbcPartition");
    configure(&mut statement, &[(OPTION_DATA_BOOST, "true")]);
    assert_eq!(
        statement
            .get_option_string(OPTION_DATA_BOOST.into())
            .unwrap(),
        "true"
    );

    let mut partitioned = statement.execute_partitions().unwrap();
    assert_fields(partitioned.schema.fields(), &[("Id", DataType::Int64)]);
    assert!(!partitioned.partitions.is_empty());
    assert_eq!(partitioned.rows_affected, -1);

    // `read_partition` only learns about Data Boost from the descriptor.
    for token in &partitioned.partitions {
        let descriptor: serde_json::Value = serde_json::from_slice(token).unwrap();
        assert_eq!(descriptor["v"], 1, "{descriptor}");
        assert_eq!(
            descriptor["partition"]["inner"]["Query"]["dataBoostEnabled"],
            serde_json::Value::Bool(true),
            "{descriptor}"
        );
    }

    if fx.target.omni_endpoint.is_some() {
        // Omni supports partitioned reads but rejects the Cloud-only Data Boost service.
        let error = connection
            .read_partition(&partitioned.partitions[0])
            .err()
            .unwrap();
        assert_eq!(error.status, Status::InvalidArguments);
        assert!(error.message.contains("DataBoost"), "{error}");
        configure(&mut statement, &[(OPTION_DATA_BOOST, "false")]);
        partitioned = statement.execute_partitions().unwrap();
    }

    assert_eq!(
        read_partition_ids(connection, &partitioned),
        (1..=200).collect()
    );

    // Spanner begins batch transactions only with exact bounds, so `max:` is pinned.
    configure(&mut statement, &[(OPTION_READ_STALENESS, "max:0s")]);
    statement.execute_partitions().unwrap();

    // Partitioning accepts exactly one bound parameter row.
    let mut bound_stmt = prepare(connection, "SELECT Id FROM AdbcPartition WHERE Id <= @max");
    bound_stmt.bind(int_batch("max", &[50])).unwrap();
    let partitioned = bound_stmt.execute_partitions().unwrap();
    assert_eq!(
        read_partition_ids(connection, &partitioned),
        (1..=50).collect()
    );
    // `execute_partitions` consumes the bound row; `set_sql_query` alone would not clear it.
    bound_stmt.set_sql_query("SELECT 1").unwrap();
    let params = bound_stmt.get_parameter_schema().unwrap();
    assert_eq!(params.fields().len(), 0);

    drop_tables(connection, &["AdbcPartition"]);
}

#[test]
fn query_with_trailing_semicolons_returns_rows() {
    let Some(mut fx) = read_fixture() else { return };
    let connection = &mut fx.connection;

    // Spanner rejects a trailing `;`, so the driver strips them (`SqlQueryTrailingSemicolons`).
    assert_eq!(ints(&query(connection, "SELECT 1 AS n;;;"), 0), [1]);
    assert_eq!(strings(&query(connection, "SELECT ';' AS s;"), 0), [";"]);

    let planned = prepare(connection, "SELECT 1 AS n;;;")
        .execute_schema()
        .unwrap();
    assert_fields(planned.fields(), &[("n", DataType::Int64)]);
}

#[track_caller]
fn set_read_only<C: Optionable<Option = OptionConnection>>(connection: &mut C, enabled: bool) {
    connection
        .set_option(OptionConnection::ReadOnly, enabled.into())
        .unwrap();
}

/// `adbc.connection.readonly` allows queries and rejects DML, DDL and ingest. Toggling it affects
/// existing statements.
#[test]
fn readonly_connection_rejects_writes() {
    let Some(mut fx) = fixture() else { return };
    let connection = &mut fx.connection;

    let read_only = |connection: &SpannerConnection| {
        connection
            .get_option_string(OptionConnection::ReadOnly)
            .unwrap()
    };
    assert_eq!(read_only(connection), "false");
    set_read_only(connection, true);
    assert_eq!(read_only(connection), "true");

    assert_eq!(row_count(&query(connection, "SELECT 1 AS n")), 1);

    for sql in [
        "DELETE FROM Singers WHERE true",
        "CREATE TABLE AdbcReadOnlyDenied (Id INT64) PRIMARY KEY (Id)",
    ] {
        let error = prepare(connection, sql).execute_update().unwrap_err();
        assert_eq!(error.status, Status::InvalidState, "{sql}: {error}");
    }

    let mut ingest = connection.new_statement().unwrap();
    ingest
        .set_option(OptionStatement::TargetTable, "Singers".into())
        .unwrap();
    ingest.bind(int_batch("SingerId", &[1])).unwrap();
    let error = ingest.execute_update().unwrap_err();
    assert_eq!(error.status, Status::InvalidState, "{error}");

    // A statement created while read-only becomes writable when the flag is cleared, and
    // read-only again when it is set.
    let mut live = connection.new_statement().unwrap();
    set_read_only(connection, false);
    assert_eq!(read_only(connection), "false");
    live.set_sql_query("DELETE FROM Singers WHERE false")
        .unwrap();
    assert_eq!(live.execute_update().unwrap(), Some(0));

    set_read_only(connection, true);
    live.set_sql_query("DELETE FROM Singers WHERE true")
        .unwrap();
    let error = live.execute_update().unwrap_err();
    assert_eq!(error.status, Status::InvalidState, "{error}");

    set_read_only(connection, false);
}

/// Setting `adbc.connection.readonly` on another thread locks a statement created earlier.
#[test]
fn readonly_toggle_from_another_thread_locks_existing_statement() {
    let Some(fx) = fixture() else { return };
    let mut connection = fx.connection;

    let mut dml = prepare(&mut connection, "DELETE FROM Singers WHERE false");
    assert_eq!(dml.execute_update().unwrap(), Some(0));

    let toggler = std::thread::spawn(move || {
        set_read_only(&mut connection, true);
        connection
    });
    let mut connection = toggler.join().unwrap();

    let error = dml.execute_update().unwrap_err();
    assert_eq!(error.status, Status::InvalidState, "{error}");

    set_read_only(&mut connection, false);
}

/// `rollback()` fails in autocommit mode and is a no-op in an empty manual transaction.
#[test]
fn rollback_without_a_transaction_is_invalid_state() {
    let Some(mut fx) = read_fixture() else { return };
    let connection = &mut fx.connection;

    let error = connection.rollback().unwrap_err();
    assert_eq!(error.status, Status::InvalidState, "{error}");
    assert!(error.message.contains("autocommit"), "{error}");

    set_autocommit(connection, false);
    connection.rollback().unwrap();
    connection.rollback().unwrap();
    assert_eq!(
        connection
            .get_option_string(OptionConnection::AutoCommit)
            .unwrap(),
        "false"
    );

    set_autocommit(connection, true);
    assert_eq!(
        connection.rollback().unwrap_err().status,
        Status::InvalidState
    );
}

/// `get_statistic_names` is empty because Spanner has no named statistics.
#[test]
fn get_statistic_names_is_empty_and_correctly_typed() {
    let Some(fx) = read_fixture() else { return };
    let mut reader = fx.connection.get_statistic_names().unwrap();
    assert_eq!(
        reader.schema(),
        adbc_core::schemas::GET_STATISTIC_NAMES_SCHEMA.clone()
    );
    assert!(reader.next().is_none());
}

/// A connection cancel between `read_partition` chunk fetches cancels the next fetch, does not
/// affect statements, and is cleared by the connection's next operation.
#[test]
fn connection_cancel_is_sticky_until_the_next_operation() {
    let Some(mut fx) = fixture() else { return };
    let connection = &mut fx.connection;

    recreate_numbered(connection, "AdbcConnCancel", 200);

    let partitioned = prepare(connection, "SELECT Id FROM AdbcConnCancel")
        .execute_partitions()
        .unwrap();
    // The emulator can return empty partitions; find one with rows. Descriptors can be reread.
    let descriptor = partitioned
        .partitions
        .iter()
        .find(|token| connection.read_partition(token).unwrap().next().is_some())
        .unwrap();

    let mut reader = connection.read_partition(descriptor).unwrap();
    assert!(reader.next().unwrap().unwrap().num_rows() <= 200);
    connection.get_cancel_handle().try_cancel().unwrap();
    let error = reader.next().unwrap().unwrap_err();
    assert_eq!(reader_error_status(&error), Status::Cancelled, "{error}");

    // Statements have their own cancel signal.
    assert_eq!(count_rows(connection, "AdbcConnCancel"), 200);

    assert_eq!(read_partition_ids(connection, &partitioned).len(), 200);

    // A cancel with nothing in flight does not affect the next metadata call.
    connection.get_cancel_handle().try_cancel().unwrap();
    let schema = connection
        .get_table_schema(None, None, "AdbcConnCancel")
        .unwrap();
    assert_eq!(schema.fields().len(), 1);

    drop_tables(connection, &["AdbcConnCancel"]);
}

/// Priority and tag options are inherited by statements and produce valid requests. The emulator
/// ignores them, so this only checks that requests succeed.
#[test]
fn request_priority_and_tags() {
    let Some(mut fx) = fixture() else { return };
    let connection = &mut fx.connection;

    configure(
        connection,
        &[
            (OPTION_REQUEST_PRIORITY, "medium"),
            (OPTION_REQUEST_TAG, "adbc-test-request"),
            (OPTION_TRANSACTION_TAG, "adbc-test-txn"),
        ],
    );
    for (key, value) in [
        (OPTION_REQUEST_PRIORITY, "medium"),
        (OPTION_TRANSACTION_TAG, "adbc-test-txn"),
    ] {
        assert_eq!(connection.get_option_string(key.into()).unwrap(), value);
    }
    let mut statement = connection.new_statement().unwrap();
    assert_eq!(
        statement
            .get_option_string(OPTION_REQUEST_TAG.into())
            .unwrap(),
        "adbc-test-request"
    );
    // An empty value unsets the inherited tag.
    configure(
        &mut statement,
        &[(OPTION_REQUEST_PRIORITY, "high"), (OPTION_REQUEST_TAG, "")],
    );

    recreate(connection, "AdbcReqOpts", "Id INT64");
    let insert = "INSERT INTO AdbcReqOpts (Id) VALUES (1), (2)";
    assert_eq!(run(connection, insert), Some(2));
    assert_eq!(count_rows(connection, "AdbcReqOpts"), 2);

    statement
        .set_sql_query("SELECT Id FROM AdbcReqOpts ORDER BY Id")
        .unwrap();
    assert_eq!(row_count(&fetch(&mut statement)), 2);

    let mut bound = prepare(connection, "INSERT INTO AdbcReqOpts (Id) VALUES (@Id)");
    bound.bind(int_batch("Id", &[3])).unwrap();
    assert_eq!(bound.execute_update().unwrap(), Some(1));
    assert_eq!(count_rows(connection, "AdbcReqOpts"), 3);

    let error = statement
        .set_option(OPTION_TRANSACTION_TAG.into(), "nope".into())
        .unwrap_err();
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
        HashMap::from([
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
    let Some(mut fx) = read_fixture() else { return };
    let connection = &mut fx.connection;

    let cases = schema_fidelity_cases();
    let select_list = cases
        .iter()
        .map(|c| format!("{} AS {}", c.expr, c.name))
        .collect::<Vec<_>>()
        .join(", ");
    let with_rows_sql = format!("SELECT {select_list} FROM UNNEST([1]) AS r");
    let zero_rows_sql = format!("SELECT {select_list} FROM UNNEST(ARRAY<INT64>[]) AS r");
    let bound_sql = format!("SELECT {select_list} FROM UNNEST([1]) AS n WHERE n = @N");

    // Compare per column so a failure names the column.
    let assert_expected_schema = |actual: &Schema, path: &str| {
        assert_eq!(actual.fields().len(), cases.len(), "{path}");
        for (actual_field, case) in actual.fields().iter().zip(&cases) {
            assert_eq!(
                actual_field.as_ref(),
                &case.field,
                "{path}: column {}",
                case.name
            );
        }
    };

    for (path, sql, bound, rows) in [
        ("with rows", &with_rows_sql, None, 1),
        ("zero rows", &zero_rows_sql, None, 0),
        ("bound, zero rows", &bound_sql, Some(&[5, 6, 7][..]), 0),
        ("bound, with rows", &bound_sql, Some(&[1, 1][..]), 2),
    ] {
        let mut statement = prepare(connection, sql);
        if let Some(values) = bound {
            statement.bind(int_batch("N", values)).unwrap();
        }
        let reader = statement.execute().unwrap();
        assert_expected_schema(&reader.schema(), path);
        let batches = drain(reader);
        assert_eq!(row_count(&batches), rows, "{path}");
        for batch in &batches {
            assert_expected_schema(&batch.schema(), path);
        }
    }

    let mut plan = prepare(connection, &with_rows_sql);
    assert_expected_schema(
        &plan.execute_schema().unwrap(),
        "execute_schema (with rows)",
    );
    plan.set_sql_query(&zero_rows_sql).unwrap();
    assert_expected_schema(
        &plan.execute_schema().unwrap(),
        "execute_schema (zero rows)",
    );
}

/// Asserts that `attempt`, run under a microsecond deadline, fails with a timeout naming `option`.
/// The deadline fires on the next ~1ms timer tick, and `tokio::time::timeout` polls the operation
/// before the timer, so a response that is already in wins. On a loaded runner a query did that
/// more than 100 times in a row. Attempts stop at the first failure, so the bound costs nothing
/// unless the deadline is ignored.
#[track_caller]
fn assert_deadline_expires(
    what: &str,
    option: &str,
    mut attempt: impl FnMut() -> Option<adbc_core::error::Error>,
) {
    let error = (0..2000)
        .find_map(|_| attempt())
        .unwrap_or_else(|| panic!("{what}: {option} never expired"));
    assert_eq!(error.status, Status::Timeout, "{what}: {error}");
    assert!(error.message.contains(option), "{what}: {error}");
}

/// RPC timeout options: generous deadlines do not affect operations, and expired ones report
/// `Timeout`. The DDL deadline is tested in `tests/mock_spanner.rs`.
#[test]
fn rpc_timeouts() {
    let Some(mut fx) = fixture() else { return };
    let connection = &mut fx.connection;

    // The streamed query uses the query deadline for its first chunk and the fetch deadline after.
    for key in [
        OPTION_RPC_TIMEOUT_QUERY,
        OPTION_RPC_TIMEOUT_UPDATE,
        OPTION_RPC_TIMEOUT_FETCH,
    ] {
        connection.set_option(key.into(), 30_i64.into()).unwrap();
    }
    recreate(connection, "AdbcRpcTimeout", "Id INT64, Value INT64");
    let insert =
        "INSERT INTO AdbcRpcTimeout (Id) SELECT n FROM UNNEST(GENERATE_ARRAY(1, 100)) AS n";
    assert_eq!(run(connection, insert), Some(100));
    let batches = fetch(&mut batched_query(
        connection,
        "SELECT Id FROM AdbcRpcTimeout ORDER BY Id",
        40,
    ));
    assert_eq!(row_count(&batches), 100);
    assert!(batches.len() > 1, "later fetches ran under the deadline");

    let mut tiny = prepare(connection, "SELECT Id FROM AdbcRpcTimeout ORDER BY Id");
    configure(&mut tiny, &[(OPTION_RPC_TIMEOUT_QUERY, "0.000001")]);
    // The query deadline covers the first chunk, so `execute` itself fails.
    assert_deadline_expires("query", OPTION_RPC_TIMEOUT_QUERY, || tiny.execute().err());

    let mut tiny_dml = prepare(
        connection,
        "UPDATE AdbcRpcTimeout SET Value = Value WHERE FALSE",
    );
    tiny_dml
        .set_option(OPTION_RPC_TIMEOUT_UPDATE.into(), 0.000001.into())
        .unwrap();
    assert_deadline_expires("DML", OPTION_RPC_TIMEOUT_UPDATE, || {
        tiny_dml.execute_update().err()
    });

    tiny.set_option(OPTION_RPC_TIMEOUT_QUERY.into(), 30_i64.into())
        .unwrap();
    assert_eq!(row_count(&fetch(&mut tiny)), 100);
    tiny_dml
        .set_option(OPTION_RPC_TIMEOUT_UPDATE.into(), 30_i64.into())
        .unwrap();
    assert_eq!(tiny_dml.execute_update().unwrap(), Some(0));

    // An expiring DDL deadline is not tested here: the emulator answers DDL faster than tokio's
    // ~1ms timer resolution, so the result would be racy.

    // The query deadline also bounds internal metadata reads.
    configure(connection, &[(OPTION_RPC_TIMEOUT_QUERY, "0.000001")]);
    assert_deadline_expires("metadata read", OPTION_RPC_TIMEOUT_QUERY, || {
        connection
            .get_objects(ObjectDepth::All, None, None, None, None, None)
            .err()
    });
    connection
        .set_option(OPTION_RPC_TIMEOUT_QUERY.into(), 30_i64.into())
        .unwrap();
    drain(
        connection
            .get_objects(ObjectDepth::All, None, None, None, None, None)
            .unwrap(),
    );

    drop_tables(connection, &["AdbcRpcTimeout"]);
}

/// Change stream DDL, metadata and `READ_` queries work through plain SQL.
///
/// Reading data change records would mean following child partition tokens, which is timing
/// sensitive, so this only checks the first read's schema and that it returns a row.
#[test]
fn change_stream_via_plain_sql() {
    let Some(mut fx) = fixture() else { return };
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
    recreate(connection, "AdbcChangeStream", "Id INT64, Name STRING(MAX)");
    run(
        connection,
        "CREATE CHANGE STREAM AdbcChangeStreamCs FOR AdbcChangeStream",
    );

    let streams = query(
        connection,
        "SELECT CHANGE_STREAM_NAME FROM INFORMATION_SCHEMA.CHANGE_STREAMS \
         WHERE CHANGE_STREAM_NAME = 'AdbcChangeStreamCs'",
    );
    assert_eq!(strings(&streams, 0), ["AdbcChangeStreamCs"]);

    let watched = query(
        connection,
        "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.CHANGE_STREAM_TABLES \
         WHERE CHANGE_STREAM_NAME = 'AdbcChangeStreamCs'",
    );
    assert_eq!(strings(&watched, 0), ["AdbcChangeStream"]);

    run(
        connection,
        "INSERT INTO AdbcChangeStream (Id, Name) VALUES (1, 'alpha'), (2, 'beta')",
    );

    // The emulator rejects a start in the past, so leave room for a stalled runner to send the
    // query in time. The read runs until the end of the window.
    let start = Utc::now() + chrono::Duration::seconds(2);
    let end = start + chrono::Duration::seconds(1);
    let start_lit = start.to_rfc3339_opts(SecondsFormat::Nanos, true);
    let end_lit = end.to_rfc3339_opts(SecondsFormat::Nanos, true);
    let read_sql = format!(
        "SELECT ChangeRecord FROM READ_AdbcChangeStreamCs (\
             start_timestamp => TIMESTAMP '{start_lit}', \
             end_timestamp => TIMESTAMP '{end_lit}', \
             partition_token => NULL, \
             heartbeat_milliseconds => 2000)"
    );
    let reader = prepare(connection, read_sql).execute().unwrap();
    let schema = reader.schema();
    assert_eq!(schema.fields().len(), 1);
    let change_field = schema.field(0);
    assert_eq!(change_field.name(), "ChangeRecord");
    assert_eq!(
        field_names(struct_fields(
            list_item(change_field.data_type()).data_type()
        )),
        [
            "data_change_record",
            "heartbeat_record",
            "child_partitions_record"
        ]
    );

    assert!(
        row_count(&drain(reader)) >= 1,
        "at least the child-partition record"
    );

    run(connection, "DROP CHANGE STREAM AdbcChangeStreamCs");
    run(connection, "DROP TABLE AdbcChangeStream");
}

/// Opt-in keyfile and impersonation auth tests against Cloud Spanner. They always use
/// `SPANNER_GCP_DATABASE`, because the emulator rejects these credentials, and also need
/// `SPANNER_TEST_KEYFILE` or `SPANNER_TEST_IMPERSONATE_TARGET_PRINCIPAL`.
mod auth_end_to_end {
    use super::*;

    /// Runs a query with `option` set to the value of the environment variable `env`.
    fn assert_auth_works(env: &str, option: &str) {
        let Some(target) = gcp_target() else {
            eprintln!("SPANNER_GCP_DATABASE not set; skipping auth end-to-end test");
            return;
        };
        let Some(value) = std::env::var(env).ok().filter(|s| !s.is_empty()) else {
            eprintln!("{env} not set; skipping auth end-to-end test");
            return;
        };

        // Setup uses ADC, not the credentials under test.
        ensure_database_once(&target, SETUP_DDL);

        let mut driver = SpannerDriver::try_new().unwrap();
        let database = driver
            .new_database_with_opts([
                (
                    OptionDatabase::Uri,
                    OptionValue::String(target.database_uri()),
                ),
                (option.into(), OptionValue::String(value)),
            ])
            .unwrap();
        let batches = query(&mut connect_with_retry(&database), "SELECT 1 AS one");
        assert_eq!(batches.len(), 1);
        assert_eq!(ints(&batches, 0), [1]);
    }

    #[test]
    fn keyfile_auth_end_to_end() {
        assert_auth_works("SPANNER_TEST_KEYFILE", OPTION_KEYFILE);
    }

    /// The ADC identity needs the Token Creator role on the target principal.
    #[test]
    fn impersonation_auth_end_to_end() {
        assert_auth_works(
            "SPANNER_TEST_IMPERSONATE_TARGET_PRINCIPAL",
            OPTION_IMPERSONATE_TARGET_PRINCIPAL,
        );
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
    FileDescriptorSet {
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
    }
    .encode_to_vec()
}

/// Register the proto bundle used by the ENUM and PROTO tests, once per binary. The driver's DDL
/// path cannot pass `proto_descriptors`, and a database has only one bundle.
///
/// Transient admin errors (seen on CI) are retried; otherwise the later table DDL fails with an
/// obscure `Unrecognized ddl::ColumnDefinition` error.
fn ensure_proto_bundle_once(target: &TestTarget) {
    use google_cloud_gax::error::rpc::Code;

    const CREATE_BUNDLE: &str = "CREATE PROTO BUNDLE (`adbc.test.Color`, `adbc.test.Point`)";

    static DONE: Mutex<bool> = Mutex::new(false);
    let mut done = DONE.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    if *done {
        return;
    }
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        if let Some(endpoint) = &target.omni_endpoint {
            let result = omni::update_ddl(
                endpoint,
                target.database_path(),
                vec![CREATE_BUNDLE.to_string()],
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
        let database_admin = spanner.database_admin_builder().build().await.unwrap();
        let mut last_err = None;
        for attempt in 0..5 {
            match database_admin
                .update_database_ddl()
                .set_database(target.database_path())
                .set_statements(vec![CREATE_BUNDLE.to_string()])
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
                    tokio::time::sleep(std::time::Duration::from_millis(500 * (attempt + 1))).await;
                }
            }
        }
        panic!(
            "failed to register the shared proto bundle after retries: {}",
            last_err.unwrap()
        );
    });
    *done = true;
}

/// `ENUM` maps to its `Int64` ordinal and `ARRAY<ENUM>` to `List<Int64>`; `CAST(col AS STRING)`
/// gives the labels.
#[test]
fn enum_columns_round_trip_as_int64_ordinals() {
    let Some(mut fx) = fixture() else { return };
    ensure_proto_bundle_once(&fx.target);
    let connection = &mut fx.connection;

    recreate(
        connection,
        "AdbcEnum",
        "Id INT64, Fav `adbc.test.Color`, Faves ARRAY<`adbc.test.Color`>",
    );

    let insert = "INSERT INTO AdbcEnum (Id, Fav, Faves) VALUES \
                  (1, CAST(2 AS `adbc.test.Color`), \
                      [CAST(1 AS `adbc.test.Color`), CAST(3 AS `adbc.test.Color`)]), \
                  (2, CAST(NULL AS `adbc.test.Color`), [CAST(2 AS `adbc.test.Color`)])";
    assert_eq!(run(connection, insert), Some(2));

    let reader = prepare(connection, "SELECT Fav FROM AdbcEnum ORDER BY Id")
        .execute()
        .unwrap();
    assert_eq!(reader.schema().field(0).data_type(), &DataType::Int64);
    let batches = drain(reader);
    let fav = col::<Int64Array>(batches[0].column(0));
    assert_eq!(fav.len(), 2);
    assert_eq!(fav.value(0), 2, "GREEN has ordinal 2");
    assert!(fav.is_null(1), "a NULL enum is a null slot, not 0");

    let reader = prepare(connection, "SELECT Faves FROM AdbcEnum WHERE Id = 1")
        .execute()
        .unwrap();
    assert_eq!(
        list_item(reader.schema().field(0).data_type()).data_type(),
        &DataType::Int64
    );
    let batches = drain(reader);
    let elems = col::<ListArray>(batches[0].column(0)).value(0);
    assert_eq!(
        col::<Int64Array>(&elems).values(),
        &[1, 3],
        "[RED, BLUE] are ordinals [1, 3]"
    );

    let reader = prepare(
        connection,
        "SELECT CAST(Fav AS STRING) AS FavName FROM AdbcEnum ORDER BY Id",
    )
    .execute()
    .unwrap();
    assert_eq!(reader.schema().field(0).data_type(), &DataType::Utf8);
    let batches = drain(reader);
    let names = col::<StringArray>(batches[0].column(0));
    assert_eq!(names.value(0), "GREEN");
    assert!(names.is_null(1));
}

/// `PROTO` maps to `Binary` and `ARRAY<PROTO>` to `List<Binary>`, holding the serialized messages.
#[test]
fn proto_columns_round_trip_as_binary() {
    use prost::Message as _;

    let Some(mut fx) = fixture() else { return };
    ensure_proto_bundle_once(&fx.target);
    let connection = &mut fx.connection;

    recreate(
        connection,
        "AdbcProto",
        "Id INT64, P `adbc.test.Point`, Ps ARRAY<`adbc.test.Point`>",
    );

    let proto_literal = |point: &TestPoint| {
        let escaped: String = point
            .encode_to_vec()
            .iter()
            .map(|b| format!("\\x{b:02x}"))
            .collect();
        format!("CAST(b'{escaped}' AS `adbc.test.Point`)")
    };
    let point = |x, y, label: &str| TestPoint {
        x,
        y,
        label: label.to_string(),
    };
    let scalar_point = point(3, 4, "p");
    let elements = [point(1, 2, "a"), point(5, 6, "b")];

    let insert = format!(
        "INSERT INTO AdbcProto (Id, P, Ps) VALUES \
         (1, {p}, [{a0}, {a1}]), \
         (2, CAST(NULL AS `adbc.test.Point`), [])",
        p = proto_literal(&scalar_point),
        a0 = proto_literal(&elements[0]),
        a1 = proto_literal(&elements[1]),
    );
    assert_eq!(run(connection, insert), Some(2));

    let reader = prepare(connection, "SELECT P FROM AdbcProto ORDER BY Id")
        .execute()
        .unwrap();
    assert_eq!(reader.schema().field(0).data_type(), &DataType::Binary);
    let batches = drain(reader);
    let points = col::<BinaryArray>(batches[0].column(0));
    assert_eq!(points.len(), 2);
    assert_eq!(TestPoint::decode(points.value(0)).unwrap(), scalar_point);
    assert!(
        points.is_null(1),
        "a NULL proto is a null slot, not empty bytes"
    );

    let reader = prepare(connection, "SELECT Ps FROM AdbcProto WHERE Id = 1")
        .execute()
        .unwrap();
    assert_eq!(
        list_item(reader.schema().field(0).data_type()).data_type(),
        &DataType::Binary
    );
    let batches = drain(reader);
    let list = col::<ListArray>(batches[0].column(0)).value(0);
    let list = col::<BinaryArray>(&list);
    let decoded: Vec<TestPoint> = (0..list.len())
        .map(|i| TestPoint::decode(list.value(i)).unwrap())
        .collect();
    assert_eq!(decoded, elements);
}

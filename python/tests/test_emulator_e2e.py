"""Exercise SQL, Arrow results, ingest and transactions on the configured Spanner target."""

import datetime

import pytest

from conftest import database_options

pa = pytest.importorskip("pyarrow")

import spanner_adbc.dbapi as spanner
from adbc_driver_manager import ProgrammingError
from spanner_adbc import StatementOptions


def _connect(database, *, autocommit):
    return spanner.connect(
        db_kwargs=database_options(database),
        autocommit=autocommit,
    )


def test_ddl_dml_query_roundtrip(spanner_database):
    with _connect(spanner_database, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS Singers")
            cur.execute(
                "CREATE TABLE Singers ("
                "  SingerId INT64 NOT NULL,"
                "  FirstName STRING(1024),"
                "  Active BOOL,"
                ") PRIMARY KEY (SingerId)"
            )
            cur.execute(
                "INSERT INTO Singers (SingerId, FirstName, Active) VALUES "
                "(1, 'Alice', true), (2, 'Bob', false)"
            )

            cur.execute("SELECT SingerId, FirstName, Active FROM Singers ORDER BY SingerId")
            table = cur.fetch_arrow_table()

        assert table.num_rows == 2
        assert table.column("SingerId").to_pylist() == [1, 2]
        assert table.column("FirstName").to_pylist() == ["Alice", "Bob"]
        assert table.column("Active").to_pylist() == [True, False]


def test_dbapi_type_objects_match_result_columns(spanner_database):
    with _connect(spanner_database, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT @n AS n, @s AS s, @b AS b, @d AS d, @t AS t",
                (
                    1,
                    "x",
                    spanner.Binary(b"\x00\xff"),
                    spanner.Date(2024, 2, 29),
                    spanner.Timestamp(2024, 2, 29, 12, 30, tzinfo=datetime.timezone.utc),
                ),
            )
            columns = {d[0]: d[1] for d in cur.description}
            row = cur.fetchone()
    assert columns["n"] == spanner.NUMBER
    assert columns["s"] == spanner.STRING
    assert columns["b"] == spanner.BINARY
    assert columns["d"] == spanner.DATETIME
    assert columns["t"] == spanner.DATETIME
    assert row[:4] == (1, "x", b"\x00\xff", spanner.Date(2024, 2, 29))


def test_execute_partitions_round_trip(spanner_database):
    with _connect(spanner_database, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS AdbcPyPartition")
            cur.execute("CREATE TABLE AdbcPyPartition (Id INT64 NOT NULL) PRIMARY KEY (Id)")
            cur.execute(
                "INSERT INTO AdbcPyPartition (Id) "
                "SELECT n FROM UNNEST(GENERATE_ARRAY(1, 200)) AS n"
            )

        with conn.cursor() as cur:
            cur.adbc_statement.set_options(**{StatementOptions.DATA_BOOST.value: "true"})
            # ORDER BY would make the query non-partitionable.
            partitions, schema = cur.adbc_execute_partitions(
                "SELECT Id FROM AdbcPyPartition"
            )
        assert len(partitions) >= 1
        assert schema is not None
        assert schema.names == ["Id"]

        if database_options(spanner_database).get("spanner.instance_type") == "omni":
            with conn.cursor() as cur:
                with pytest.raises(spanner.ProgrammingError, match="does not support DataBoost"):
                    cur.adbc_read_partition(partitions[0])
            # Omni supports partitioned reads on its own compute, without Data Boost.
            with conn.cursor() as cur:
                partitions, schema = cur.adbc_execute_partitions("SELECT Id FROM AdbcPyPartition")
            assert partitions
            assert schema.names == ["Id"]

        seen: set[int] = set()
        for token in partitions:
            with conn.cursor() as cur:
                cur.adbc_read_partition(token)
                table = cur.fetch_arrow_table()
            assert table.schema == schema
            ids = table.column("Id").to_pylist()
            assert seen.isdisjoint(ids)
            seen.update(ids)

        assert seen == set(range(1, 201))


def test_manual_commit_and_rollback(spanner_database):
    with _connect(spanner_database, autocommit=False) as conn:
        # DDL applies immediately, outside the manual transaction.
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS AdbcPyTxn")
            cur.execute("CREATE TABLE AdbcPyTxn (Id INT64 NOT NULL) PRIMARY KEY (Id)")

        with conn.cursor() as cur:
            cur.execute("INSERT INTO AdbcPyTxn (Id) VALUES (1)")
            # A query could not see the buffered write, so the driver rejects it.
            # The INSERT stays buffered and is committed below.
            with pytest.raises(ProgrammingError):
                cur.execute("SELECT COUNT(*) AS n FROM AdbcPyTxn")

        conn.commit()
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS n FROM AdbcPyTxn")
            assert cur.fetch_arrow_table().column("n").to_pylist() == [1]
        # End the read-only transaction opened by the count.
        conn.rollback()

        with conn.cursor() as cur:
            cur.execute("INSERT INTO AdbcPyTxn (Id) VALUES (2)")
        conn.rollback()
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS n FROM AdbcPyTxn")
            assert cur.fetch_arrow_table().column("n").to_pylist() == [1]


def test_failed_commit_keeps_buffer_until_rollback(spanner_database):
    with _connect(spanner_database, autocommit=False) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS AdbcPyFailedCommit")
            cur.execute("CREATE TABLE AdbcPyFailedCommit (Id INT64 NOT NULL) PRIMARY KEY (Id)")
            cur.execute("INSERT INTO AdbcPyFailedCommit (Id) VALUES (1)")
        conn.commit()

        with conn.cursor() as cur:
            # Buffered: the duplicate key is only detected when the commit runs the DML.
            cur.execute("INSERT INTO AdbcPyFailedCommit (Id) VALUES (1)")
            assert cur.rowcount == -1
        with pytest.raises(spanner.IntegrityError, match="(?i)exist"):
            conn.commit()
        # The failed work stays buffered, as the Python README says.
        with pytest.raises(spanner.IntegrityError, match="(?i)exist"):
            conn.commit()
        conn.rollback()

        with conn.cursor() as cur:
            cur.execute("INSERT INTO AdbcPyFailedCommit (Id) VALUES (2)")
        conn.commit()
        with conn.cursor() as cur:
            cur.execute("SELECT Id FROM AdbcPyFailedCommit ORDER BY Id")
            assert cur.fetchall() == [(1,), (2,)]
        conn.rollback()


def test_rowcount_after_execute(spanner_database):
    with _connect(spanner_database, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS AdbcPyRowcount")
            assert cur.rowcount == -1
            cur.execute(
                "CREATE TABLE AdbcPyRowcount (Id INT64 NOT NULL, V STRING(MAX)) PRIMARY KEY (Id)"
            )
            cur.execute("INSERT INTO AdbcPyRowcount (Id, V) VALUES (1, 'a'), (2, 'b'), (3, 'c')")
            assert cur.rowcount == 3
            cur.execute(
                "UPDATE AdbcPyRowcount SET V = 'x' WHERE Id < 3; "
                "DELETE FROM AdbcPyRowcount WHERE Id = 3"
            )
            assert cur.rowcount == 3
            cur.execute("UPDATE AdbcPyRowcount SET V = 'y' WHERE TRUE THEN RETURN Id")
            assert cur.rowcount == 2
            assert sorted(cur.fetch_arrow_table().column("Id").to_pylist()) == [1, 2]
            cur.execute("SELECT Id FROM AdbcPyRowcount")
            assert cur.rowcount == -1
            cur.fetchall()
        with conn.cursor() as cur:
            cur.adbc_statement.set_options(**{StatementOptions.DML_PARTITIONED.value: "true"})
            cur.execute("UPDATE AdbcPyRowcount SET V = 'p' WHERE TRUE")
            assert cur.rowcount == 2

    with _connect(spanner_database, autocommit=False) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM AdbcPyRowcount WHERE TRUE")
            # Buffered DML runs at commit, so its count is unknown.
            assert cur.rowcount == -1
        conn.commit()


def test_constraint_violations_raise_integrity_error(spanner_database):
    with _connect(spanner_database, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS AdbcPyIntegrity")
            cur.execute(
                "CREATE TABLE AdbcPyIntegrity (Id INT64 NOT NULL, Name STRING(MAX) NOT NULL) "
                "PRIMARY KEY (Id)"
            )
            cur.execute("INSERT INTO AdbcPyIntegrity (Id, Name) VALUES (1, 'a')")
            with pytest.raises(spanner.IntegrityError):
                cur.execute("INSERT INTO AdbcPyIntegrity (Id, Name) VALUES (1, 'b')")
            with pytest.raises(spanner.IntegrityError):
                cur.execute("INSERT INTO AdbcPyIntegrity (Id, Name) VALUES (2, NULL)")
            with pytest.raises(spanner.IntegrityError):
                cur.adbc_ingest(
                    "AdbcPyIntegrity",
                    pa.table({"Id": pa.array([1], pa.int64()), "Name": ["c"]}),
                    mode="append",
                )


def test_question_mark_placeholder_error_names_the_fix(spanner_database):
    with _connect(spanner_database, autocommit=True) as conn:
        with conn.cursor() as cur:
            with pytest.raises(spanner.ProgrammingError, match="@p1"):
                cur.execute("SELECT ? AS x", (1,))

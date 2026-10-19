"""Exercise SQL, Arrow results, ingest and transactions on the configured Spanner target."""

import pytest

from conftest import database_options

pa = pytest.importorskip("pyarrow")

import spanner_adbc
import spanner_adbc.dbapi as spanner
from adbc_driver_manager import ProgrammingError
from spanner_adbc import StatementOptions


def _connect(database, *, autocommit):
    return spanner.connect(
        db_kwargs=database_options(database),
        autocommit=autocommit,
    )


def test_driver_library_is_bundled():
    # Integration runs must test an installed library, not a bare source tree.
    assert spanner_adbc._driver_path()


def test_ddl_dml_query_roundtrip(spanner_database):
    conn = _connect(spanner_database, autocommit=True)
    try:
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
    finally:
        conn.close()


def test_fetch_dataframe(spanner_database):
    pytest.importorskip("pandas")
    conn = _connect(spanner_database, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS AdbcPyDf")
            cur.execute(
                "CREATE TABLE AdbcPyDf ("
                "  Id INT64 NOT NULL,"
                "  Name STRING(MAX),"
                ") PRIMARY KEY (Id)"
            )
            cur.execute("INSERT INTO AdbcPyDf (Id, Name) VALUES (1, 'Alice'), (2, 'Bob')")

            cur.execute("SELECT Id, Name FROM AdbcPyDf ORDER BY Id")
            df = cur.fetch_df()
        assert df["Id"].tolist() == [1, 2]
        assert list(df["Name"]) == ["Alice", "Bob"]
    finally:
        conn.close()


def test_bulk_ingest_append(spanner_database):
    conn = _connect(spanner_database, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS AdbcPyIngest")
            cur.execute(
                "CREATE TABLE AdbcPyIngest ("
                "  Id INT64 NOT NULL,"
                "  Name STRING(MAX),"
                ") PRIMARY KEY (Id)"
            )

            data = pa.table(
                {
                    "Id": pa.array([10, 20, 30], type=pa.int64()),
                    "Name": pa.array(["x", "y", "z"], type=pa.string()),
                }
            )
            count = cur.adbc_ingest("AdbcPyIngest", data, mode="append")
            assert count == 3

            cur.execute("SELECT Id, Name FROM AdbcPyIngest ORDER BY Id")
            out = cur.fetch_arrow_table()
        assert out.column("Id").to_pylist() == [10, 20, 30]
        assert out.column("Name").to_pylist() == ["x", "y", "z"]
    finally:
        conn.close()


def test_execute_partitions_round_trip(spanner_database):
    conn = _connect(spanner_database, autocommit=True)
    try:
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
    finally:
        conn.close()


def test_manual_commit_and_rollback(spanner_database):
    conn = _connect(spanner_database, autocommit=False)
    try:
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
    finally:
        conn.close()

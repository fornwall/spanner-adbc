"""Polars and pandas reads and ingest against the configured Spanner target."""

import datetime
import decimal

import pytest

from conftest import database_options

pytest.importorskip("pyarrow")
pl = pytest.importorskip("polars")
from polars.testing import assert_frame_equal

import spanner_adbc.dbapi as spanner


def _connect(database):
    return spanner.connect(
        db_kwargs=database_options(database),
        autocommit=True,
    )


ROWS = [
    {
        "Id": 1,
        "Name": "Alice",
        "Active": True,
        "Score": 1.5,
        "Ts": datetime.datetime(2024, 1, 15, 10, 30, 0, tzinfo=datetime.timezone.utc),
        "Dt": datetime.date(2024, 1, 15),
        "Amount": decimal.Decimal("123.456"),
        "Tags": [1, 2, 3],
    },
    {
        "Id": 2,
        "Name": "Bob",
        "Active": False,
        "Score": -2.25,
        "Ts": datetime.datetime(2020, 6, 1, 0, 0, 0, tzinfo=datetime.timezone.utc),
        "Dt": datetime.date(2020, 6, 1),
        "Amount": decimal.Decimal("-0.5"),
        "Tags": [4, 5],
    },
]

_DDL = (
    "CREATE TABLE {name} ("
    "  Id INT64 NOT NULL,"
    "  Name STRING(1024),"
    "  Active BOOL,"
    "  Score FLOAT64,"
    "  Ts TIMESTAMP,"
    "  Dt DATE,"
    "  Amount NUMERIC,"
    "  Tags ARRAY<INT64>,"
    ") PRIMARY KEY (Id)"
)

_COLUMNS = "Id, Name, Active, Score, Ts, Dt, Amount, Tags"


def _create_and_fill(cur, name):
    cur.execute(f"DROP TABLE IF EXISTS {name}")
    cur.execute(_DDL.format(name=name))
    columns = _COLUMNS.split(", ")
    parameters = ", ".join(f"@{column}" for column in columns)
    cur.executemany(
        f"INSERT INTO {name} ({_COLUMNS}) VALUES ({parameters})",
        [tuple(row[column] for column in columns) for row in ROWS],
    )


def test_polars_read_dtypes_and_values(spanner_database):
    with _connect(spanner_database) as conn:
        with conn.cursor() as cur:
            _create_and_fill(cur, "AdbcPlRead")

        df = pl.read_database(
            f"SELECT {_COLUMNS} FROM AdbcPlRead ORDER BY Id", connection=conn
        )

        assert isinstance(df, pl.DataFrame)
        assert df.height == 2

        assert df.schema["Id"] == pl.Int64
        assert df.schema["Name"] == pl.String
        assert df.schema["Active"] == pl.Boolean
        assert df.schema["Score"] == pl.Float64
        assert isinstance(df.schema["Ts"], pl.Datetime)
        assert df.schema["Ts"].time_zone == "UTC"
        assert df.schema["Dt"] == pl.Date
        assert isinstance(df.schema["Amount"], pl.Decimal)
        assert df.schema["Tags"] == pl.List(pl.Int64)

        assert df["Id"].to_list() == [1, 2]
        assert df["Name"].to_list() == ["Alice", "Bob"]
        assert df["Active"].to_list() == [True, False]
        assert df["Score"].to_list() == [1.5, -2.25]
        assert df["Ts"].to_list() == [r["Ts"] for r in ROWS]
        assert df["Dt"].to_list() == [r["Dt"] for r in ROWS]
        # Decimal(38, 9) compares equal to the shorter literals numerically.
        assert df["Amount"].to_list() == [r["Amount"] for r in ROWS]
        assert df["Tags"].to_list() == [r["Tags"] for r in ROWS]


def test_pandas_read_dtypes_and_values(spanner_database):
    pytest.importorskip("pandas")
    with _connect(spanner_database) as conn:
        with conn.cursor() as cur:
            _create_and_fill(cur, "AdbcPdRead")

            cur.execute(f"SELECT {_COLUMNS} FROM AdbcPdRead ORDER BY Id")
            df = cur.fetch_df()

        assert df["Id"].dtype == "int64"
        assert df["Active"].dtype == "bool"
        assert df["Score"].dtype == "float64"
        assert str(df["Ts"].dtype) == "datetime64[ns, UTC]"

        assert df["Id"].tolist() == [1, 2]
        assert df["Name"].tolist() == ["Alice", "Bob"]
        assert df["Active"].tolist() == [True, False]
        assert df["Score"].tolist() == [1.5, -2.25]
        assert [ts.to_pydatetime() for ts in df["Ts"]] == [r["Ts"] for r in ROWS]
        # DATE and NUMERIC become object columns of date and Decimal.
        assert df["Dt"].tolist() == [r["Dt"] for r in ROWS]
        assert df["Amount"].tolist() == [r["Amount"] for r in ROWS]
        assert [list(t) for t in df["Tags"]] == [r["Tags"] for r in ROWS]


def test_bulk_ingest_from_dataframe(spanner_database):
    with _connect(spanner_database) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS AdbcDfIngest")
            cur.execute(_DDL.format(name="AdbcDfIngest"))

        frame = pl.DataFrame(
            {
                "Id": [r["Id"] for r in ROWS],
                "Name": [r["Name"] for r in ROWS],
                "Active": [r["Active"] for r in ROWS],
                "Score": [r["Score"] for r in ROWS],
                "Ts": [r["Ts"] for r in ROWS],
                "Dt": [r["Dt"] for r in ROWS],
                "Amount": [r["Amount"] for r in ROWS],
                "Tags": [r["Tags"] for r in ROWS],
            },
            schema_overrides={
                "Amount": pl.Decimal(38, 9),
                # TIMESTAMP reads back with nanosecond precision; Polars defaults to us.
                "Ts": pl.Datetime("ns", "UTC"),
                "Tags": pl.List(pl.Int64),
            },
        )

        with conn.cursor() as cur:
            count = cur.adbc_ingest("AdbcDfIngest", frame.to_arrow(), mode="append")
            assert count == len(ROWS)

            out = pl.read_database(
                f"SELECT {_COLUMNS} FROM AdbcDfIngest ORDER BY Id", connection=conn
            )

        assert_frame_equal(out, frame)


def test_bulk_ingest_array_element_types(spanner_database):
    # DATE, TIMESTAMP and NUMERIC strings are coerced from the column context.
    with _connect(spanner_database) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS AdbcArrIngest")
            cur.execute(
                "CREATE TABLE AdbcArrIngest ("
                "  Id INT64 NOT NULL,"
                "  Ints ARRAY<INT64>,"
                "  Strs ARRAY<STRING(MAX)>,"
                "  Bools ARRAY<BOOL>,"
                "  Floats ARRAY<FLOAT64>,"
                "  Bytes ARRAY<BYTES(MAX)>,"
                "  Dates ARRAY<DATE>,"
                "  Stamps ARRAY<TIMESTAMP>,"
                "  Nums ARRAY<NUMERIC>,"
                ") PRIMARY KEY (Id)"
            )

        frame = pl.DataFrame(
            {
                "Id": [1, 2],
                "Ints": [[1, 2, 3], None],  # a fully-null array
                "Strs": [["a", None], ["c"]],  # a per-element null
                "Bools": [[True, False], [None]],
                "Floats": [[1.5, -2.25], None],
                "Bytes": [[b"xy", None], [b"z"]],
                "Dates": [
                    [datetime.date(2024, 1, 15), datetime.date(2020, 6, 1)],
                    [None],
                ],
                "Stamps": [
                    [datetime.datetime(2024, 1, 15, 10, 30, tzinfo=datetime.timezone.utc)],
                    None,
                ],
                "Nums": [[decimal.Decimal("1.5"), None], [decimal.Decimal("-0.5")]],
            },
            schema_overrides={
                "Ints": pl.List(pl.Int64),
                "Strs": pl.List(pl.String),
                "Bools": pl.List(pl.Boolean),
                "Floats": pl.List(pl.Float64),
                "Bytes": pl.List(pl.Binary),
                "Dates": pl.List(pl.Date),
                "Stamps": pl.List(pl.Datetime("ns", "UTC")),
                "Nums": pl.List(pl.Decimal(38, 9)),
            },
        )

        cols = "Id, Ints, Strs, Bools, Floats, Bytes, Dates, Stamps, Nums"
        with conn.cursor() as cur:
            count = cur.adbc_ingest("AdbcArrIngest", frame.to_arrow(), mode="append")
            assert count == 2

            out = pl.read_database(
                f"SELECT {cols} FROM AdbcArrIngest ORDER BY Id", connection=conn
            )

        assert_frame_equal(out, frame)

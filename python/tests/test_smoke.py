"""Import-level checks that don't need a Spanner instance."""

import adbc_driver_manager.dbapi
import pytest

import spanner_adbc
import spanner_adbc.dbapi

PEP_249 = (
    "connect", "apilevel", "threadsafety", "paramstyle",
    "Warning", "Error", "InterfaceError", "DatabaseError", "DataError", "OperationalError",
    "IntegrityError", "InternalError", "ProgrammingError", "NotSupportedError",
    "Date", "Time", "Timestamp", "DateFromTicks", "TimeFromTicks", "TimestampFromTicks", "Binary",
    "STRING", "BINARY", "NUMBER", "DATETIME", "ROWID",
)


def test_dbapi_module_follows_pep_249():
    dbapi = spanner_adbc.dbapi
    assert set(PEP_249) <= set(dbapi.__all__)
    # Spanner rejects `?`, so the driver manager's "qmark" would be wrong.
    assert dbapi.paramstyle == "named"
    assert dbapi.Binary is bytes
    for name in set(PEP_249) - {"connect", "paramstyle", "Binary"}:
        assert getattr(dbapi, name) is getattr(adbc_driver_manager.dbapi, name)


def test_missing_library_raises_clearly(tmp_path, monkeypatch):
    monkeypatch.setattr(spanner_adbc, "__file__", str(tmp_path / "__init__.py"))
    spanner_adbc._driver_path.cache_clear()
    try:
        with pytest.raises(RuntimeError, match="no bundled Spanner driver library"):
            spanner_adbc._driver_path()
    finally:
        spanner_adbc._driver_path.cache_clear()

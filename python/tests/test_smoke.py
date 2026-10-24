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


@pytest.fixture
def driver_directory(tmp_path, monkeypatch):
    monkeypatch.setattr(spanner_adbc, "__file__", str(tmp_path / "__init__.py"))
    spanner_adbc._driver_path.cache_clear()
    try:
        yield tmp_path
    finally:
        spanner_adbc._driver_path.cache_clear()


@pytest.mark.parametrize(
    "platform,library",
    [
        ("linux", "libspanner_adbc.so"),
        ("darwin", "libspanner_adbc.dylib"),
        ("win32", "spanner_adbc.dll"),
    ],
)
def test_driver_path_selects_current_platform(driver_directory, monkeypatch, platform, library):
    for name in ("libspanner_adbc.so", "libspanner_adbc.dylib", "spanner_adbc.dll"):
        (driver_directory / name).touch()
    monkeypatch.setattr(spanner_adbc.sys, "platform", platform)
    assert spanner_adbc._driver_path() == str(driver_directory / library)


@pytest.mark.parametrize("foreign_library", [None, "libspanner_adbc.dylib", "spanner_adbc.dll"])
def test_missing_library_raises_clearly(driver_directory, monkeypatch, foreign_library):
    monkeypatch.setattr(spanner_adbc.sys, "platform", "linux")
    if foreign_library:
        (driver_directory / foreign_library).touch()
    with pytest.raises(RuntimeError, match="expected libspanner_adbc.so"):
        spanner_adbc._driver_path()

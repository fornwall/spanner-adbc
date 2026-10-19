"""Import-level smoke tests that don't need a Spanner instance."""

import spanner_adbc
import spanner_adbc.dbapi
import adbc_driver_manager
import adbc_driver_manager.dbapi
import pytest


def test_entrypoint_constant():
    assert spanner_adbc.ENTRYPOINT == "AdbcSpannerInit"


def test_version_is_present():
    assert isinstance(spanner_adbc.__version__, str)
    assert spanner_adbc.__version__


def test_dbapi_exposes_connect():
    assert callable(spanner_adbc.dbapi.connect)


@pytest.mark.parametrize("name", [
    "Warning", "Error", "InterfaceError", "DatabaseError", "DataError",
    "OperationalError", "IntegrityError", "InternalError", "ProgrammingError",
    "NotSupportedError",
])
def test_dbapi_exports_driver_manager_exceptions(name):
    assert getattr(spanner_adbc.dbapi, name) is getattr(adbc_driver_manager, name)
    assert name in spanner_adbc.dbapi.__all__


def test_dbapi_module_globals_follow_pep_249():
    assert spanner_adbc.dbapi.apilevel == "2.0"
    assert spanner_adbc.dbapi.threadsafety == adbc_driver_manager.dbapi.threadsafety
    # Spanner rejects `?`, so the driver manager's "qmark" would be wrong.
    assert spanner_adbc.dbapi.paramstyle == "named"
    assert spanner_adbc.dbapi.Binary(b"\x00") == b"\x00"
    for name in (
        "apilevel", "threadsafety", "paramstyle", "Date", "Time", "Timestamp",
        "DateFromTicks", "TimeFromTicks", "TimestampFromTicks", "Binary",
        "STRING", "BINARY", "NUMBER", "DATETIME", "ROWID",
    ):
        assert name in spanner_adbc.dbapi.__all__
        if name not in ("paramstyle", "Binary"):
            assert getattr(spanner_adbc.dbapi, name) is getattr(adbc_driver_manager.dbapi, name)


def test_dbapi_errors_can_be_caught_through_the_driver_module(monkeypatch):
    def fail_connect(**kwargs):
        raise adbc_driver_manager.OperationalError(
            "connection unavailable", status_code=adbc_driver_manager.AdbcStatusCode.IO
        )

    monkeypatch.setattr(spanner_adbc.dbapi, "_driver_path", lambda: "/driver.so")
    monkeypatch.setattr(adbc_driver_manager.dbapi, "connect", fail_connect)
    with pytest.raises(spanner_adbc.dbapi.Error, match="connection unavailable"):
        spanner_adbc.dbapi.connect()


def test_missing_library_raises_clearly():
    import pathlib

    here = pathlib.Path(spanner_adbc.__file__).parent
    has_lib = any(
        (here / n).is_file()
        for n in ("libspanner_adbc.so", "libspanner_adbc.dylib", "spanner_adbc.dll")
    )
    if has_lib:
        assert spanner_adbc._driver_path()
    else:
        with pytest.raises(RuntimeError, match="no bundled Spanner driver library"):
            spanner_adbc._driver_path()

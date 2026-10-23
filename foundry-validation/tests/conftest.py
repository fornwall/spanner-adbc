import os
import sys
from pathlib import Path

import adbc_drivers_validation.model
import pytest

from . import spanner


def pytest_addoption(parser):
    parser.addoption("--vendor-version", action="store", default=None)


@pytest.fixture(scope="session")
def driver(request, pytestconfig) -> adbc_drivers_validation.model.DriverQuirks:
    return spanner.get_quirks(pytestconfig.getoption("vendor_version"))


@pytest.fixture(scope="session")
def driver_path(driver) -> str:
    if library := os.environ.get("ADBC_SPANNER_LIBRARY"):
        return str(Path(library).resolve())
    library = {
        "win32": "spanner_adbc.dll",
        "darwin": "libspanner_adbc.dylib",
    }.get(sys.platform, "libspanner_adbc.so")
    return str(Path(__file__).resolve().parents[2] / "target" / "debug" / library)


@pytest.fixture(scope="module")
def conn(conn_factory, driver):
    # Foundry expects secondary_schema to exist before schema-scoped tests run.
    # Keep connection setup lazy so query linting can still run without a database.
    with conn_factory() as connection:
        with connection.cursor() as cursor:
            schema = driver.quote_identifier(driver.features.secondary_schema)
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        yield connection

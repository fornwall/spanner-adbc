import adbc_drivers_validation.tests.query
from adbc_drivers_validation.tests.query import TestQuery  # noqa: F401

from . import spanner


def pytest_generate_tests(metafunc) -> None:
    quirks = [spanner.get_quirks(metafunc.config.getoption("vendor_version"))]
    return adbc_drivers_validation.tests.query.generate_tests(quirks, metafunc)

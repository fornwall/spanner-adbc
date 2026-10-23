import adbc_drivers_validation.tests.query
from adbc_drivers_validation.tests.query import TestQuery  # noqa: F401

from . import spanner


def pytest_generate_tests(metafunc) -> None:
    quirks = [spanner.get_quirks(metafunc.config.getoption("vendor_version"))]
    queries = {f"type/bind/{name}" for name in spanner.BIND_VARIANT_TYPES}
    return adbc_drivers_validation.tests.query.generate_tests(
        quirks, metafunc, bind_dictionary_queries=queries, bind_stream_queries=queries
    )

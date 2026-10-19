import adbc_drivers_validation.tests.ingest
import pytest

from . import spanner

# Exercise 1-128 KiB values in each variable-length string and binary layout.
LONG_VALUE_QUERIES = {
    "ingest/string",
    "ingest/large_string",
    "ingest/string_view",
    "ingest/binary",
    "ingest/large_binary",
    "ingest/binary_view",
}


def pytest_generate_tests(metafunc) -> None:
    quirks = [spanner.get_quirks(metafunc.config.getoption("vendor_version"))]
    return adbc_drivers_validation.tests.ingest.generate_tests(
        quirks, metafunc, long_value_queries=LONG_VALUE_QUERIES
    )


class TestIngest(adbc_drivers_validation.tests.ingest.TestIngest):
    @pytest.fixture(autouse=True)
    def connection_options(self, request, conn):
        # The upstream ingest tests only apply statement options. Apply query-specific
        # connection options before they create cursors, including for the readback.
        query = request.node.callspec.params.get("query")
        setup = query.metadata().setup.connection if query is not None else None
        if setup is None:
            yield
            return
        previous = {key: conn.adbc_connection.get_option(key) for key in setup.options}
        try:
            conn.adbc_connection.set_options(
                **{key: value.apply for key, value in setup.options.items()}
            )
            yield
        finally:
            conn.adbc_connection.set_options(**previous)

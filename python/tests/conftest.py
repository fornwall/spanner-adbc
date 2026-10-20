"""Shared targets for the same end-to-end tests against the emulator and Omni."""

import os
from urllib.parse import urlsplit

import pytest

from spanner_adbc import DatabaseOptions

DATABASE = "adbc-test"


def database_options(database):
    options = {DatabaseOptions.URI: f"spanner:///{database}"}
    if endpoint := os.environ.get("SPANNER_OMNI_ENDPOINT"):
        options[DatabaseOptions.INSTANCE_TYPE] = "omni"
        options[DatabaseOptions.ENDPOINT] = endpoint
    else:
        options[DatabaseOptions.EMULATOR] = "true"
    return options


def official_client(project):
    """A google-cloud-spanner client for the configured target."""
    from google.cloud import spanner

    if endpoint := os.environ.get("SPANNER_OMNI_ENDPOINT"):
        parsed = urlsplit(endpoint)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            raise ValueError("SPANNER_OMNI_ENDPOINT must be an http:// or https:// endpoint")
        return spanner.Client(
            project=project,
            instance_type="omni",
            client_options={"api_endpoint": parsed.netloc},
            use_plain_text=parsed.scheme == "http",
        )
    # The client connects anonymously to SPANNER_EMULATOR_HOST.
    return spanner.Client(project=project)


@pytest.fixture(scope="session")
def spanner_database():
    """Create the test instance and database if needed and return the database path."""
    omni = os.environ.get("SPANNER_OMNI_ENDPOINT")
    emulator = os.environ.get("SPANNER_EMULATOR_HOST")
    if omni and emulator:
        pytest.fail("Configure only one of SPANNER_OMNI_ENDPOINT and SPANNER_EMULATOR_HOST")
    if not omni and not emulator:
        if os.environ.get("ADBC_TEST_REQUIRE_TARGET", "").lower() in ("1", "true", "yes"):
            pytest.fail(
                "ADBC_TEST_REQUIRE_TARGET is set but no Spanner target is configured; "
                "set SPANNER_EMULATOR_HOST or SPANNER_OMNI_ENDPOINT."
            )
        pytest.skip("set SPANNER_EMULATOR_HOST or SPANNER_OMNI_ENDPOINT for integration tests")

    from google.api_core.exceptions import AlreadyExists

    if omni:
        instance = official_client("default").instance("default")
    else:
        instance = official_client("test-project").instance(
            "test-instance",
            configuration_name="projects/test-project/instanceConfigs/emulator-config",
            node_count=1,
        )
        try:
            instance.create().result(timeout=60)
        except AlreadyExists:
            pass
    database = instance.database(DATABASE)
    try:
        database.create().result(timeout=60)
    except AlreadyExists:
        pass
    return database.name

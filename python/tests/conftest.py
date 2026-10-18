"""Shared targets for the same end-to-end tests against the emulator and Omni."""

import json
import os
import time
import urllib.error
import urllib.request
from urllib.parse import urlsplit

import pytest

from spanner_adbc import DatabaseOptions

PROJECT = "test-project"
INSTANCE = "test-instance"
DATABASE = "adbc-test"


def _rest_base():
    """Base URL of the emulator's REST admin API.

    ``SPANNER_EMULATOR_HOST`` is the gRPC endpoint (``host:9010``); the REST admin
    API lives on ``host:9020`` (override the port with ``SPANNER_EMULATOR_REST_PORT``).
    """
    grpc = os.environ["SPANNER_EMULATOR_HOST"]
    host = grpc.rsplit(":", 1)[0] or "localhost"
    port = os.environ.get("SPANNER_EMULATOR_REST_PORT", "9020")
    return f"http://{host}:{port}"


def _post(url, body):
    """POST JSON; treat 409 (already exists) as success so setup is idempotent."""
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        url, data=data, method="POST", headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as exc:
        if exc.code == 409:
            return None
        raise


def _await_operation(op):
    """Poll a long-running operation to completion (emulator ops are near-instant)."""
    if op is None:  # Resource already exists (HTTP 409).
        return
    name = op.get("name")
    deadline = time.monotonic() + 60
    while True:
        if "error" in op:
            raise RuntimeError(f"operation {name or '<unnamed>'} failed: {op['error']}")
        if op.get("done"):
            return
        if not name:
            raise RuntimeError(f"emulator returned an incomplete operation without a name: {op}")
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise RuntimeError(f"operation {name} did not complete within 60s")
        with urllib.request.urlopen(f"{_rest_base()}/v1/{name}", timeout=min(30, remaining)) as resp:
            op = json.loads(resp.read() or b"{}")
        if not op.get("done") and "error" not in op:
            time.sleep(min(0.5, max(0, deadline - time.monotonic())))


def _require_target():
    """CI requires a configured target instead of silently skipping integration tests."""
    return os.environ.get("ADBC_TEST_REQUIRE_TARGET", "").lower() in ("1", "true", "yes")


def database_options(database):
    options = {DatabaseOptions.URI: f"spanner:///{database}"}
    if endpoint := os.environ.get("SPANNER_OMNI_ENDPOINT"):
        options[DatabaseOptions.INSTANCE_TYPE] = "omni"
        options[DatabaseOptions.ENDPOINT] = endpoint
    else:
        options[DatabaseOptions.EMULATOR] = "true"
    return options


def official_client(project):
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
    return spanner.Client(project=project)


@pytest.fixture(scope="session")
def spanner_database():
    """Ensure the instance + database exist and return the database path."""
    if os.environ.get("SPANNER_OMNI_ENDPOINT"):
        if os.environ.get("SPANNER_EMULATOR_HOST"):
            pytest.fail("Configure only one of SPANNER_OMNI_ENDPOINT and SPANNER_EMULATOR_HOST")
        from google.api_core.exceptions import AlreadyExists

        database = official_client("default").instance("default").database(DATABASE)
        try:
            database.create().result(timeout=60)
        except AlreadyExists:
            pass
        return f"projects/default/instances/default/databases/{DATABASE}"

    if not os.environ.get("SPANNER_EMULATOR_HOST"):
        if _require_target():
            pytest.fail(
                "ADBC_TEST_REQUIRE_TARGET is set but no Spanner target is configured; "
                "set SPANNER_EMULATOR_HOST or SPANNER_OMNI_ENDPOINT."
            )
        pytest.skip("set SPANNER_EMULATOR_HOST or SPANNER_OMNI_ENDPOINT for integration tests")

    base = _rest_base()
    _await_operation(
        _post(
            f"{base}/v1/projects/{PROJECT}/instances",
            {
                "instanceId": INSTANCE,
                "instance": {
                    "config": f"projects/{PROJECT}/instanceConfigs/emulator-config",
                    "displayName": "ADBC Python test instance",
                    "nodeCount": 1,
                },
            },
        )
    )
    _await_operation(
        _post(
            f"{base}/v1/projects/{PROJECT}/instances/{INSTANCE}/databases",
            {"createStatement": f"CREATE DATABASE `{DATABASE}`"},
        )
    )
    return f"projects/{PROJECT}/instances/{INSTANCE}/databases/{DATABASE}"

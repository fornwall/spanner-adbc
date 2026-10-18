"""Select the right test backend and surface setup failures before driver tests."""

import io
import json

import pytest

from conftest import _await_operation, database_options, official_client, spanner_database


@pytest.mark.parametrize("omni", [False, True])
def test_target_options_keep_omni_separate_from_emulator(monkeypatch, omni):
    monkeypatch.delenv("SPANNER_EMULATOR_HOST", raising=False)
    monkeypatch.delenv("SPANNER_OMNI_ENDPOINT", raising=False)
    endpoint = "http://localhost:15000"
    if omni:
        monkeypatch.setenv("SPANNER_OMNI_ENDPOINT", endpoint)
    else:
        monkeypatch.setenv("SPANNER_EMULATOR_HOST", "localhost:9010")
    options = database_options("projects/p/instances/i/databases/d")
    assert options.pop("uri") == "spanner:///projects/p/instances/i/databases/d"
    assert options == (
        {"spanner.instance_type": "omni", "spanner.endpoint": endpoint}
        if omni else {"spanner.emulator": "true"}
    )


def test_conflicting_targets_fail_before_connecting(monkeypatch):
    monkeypatch.setenv("SPANNER_EMULATOR_HOST", "localhost:9010")
    monkeypatch.setenv("SPANNER_OMNI_ENDPOINT", "http://localhost:15000")
    with pytest.raises(pytest.fail.Exception, match="Configure only one"):
        spanner_database.__wrapped__()


@pytest.mark.parametrize("scheme,plain_text", [("http", True), ("https", False)])
def test_official_client_uses_omni_transport(monkeypatch, scheme, plain_text):
    spanner = pytest.importorskip("google.cloud.spanner")
    monkeypatch.setenv("SPANNER_OMNI_ENDPOINT", f"{scheme}://localhost:15000")
    monkeypatch.delenv("SPANNER_EMULATOR_HOST", raising=False)
    monkeypatch.setattr(spanner, "Client", lambda **kwargs: kwargs)
    assert official_client("default") == {
        "project": "default",
        "instance_type": "omni",
        "client_options": {"api_endpoint": "localhost:15000"},
        "use_plain_text": plain_text,
    }


@pytest.mark.parametrize("polled", [False, True])
@pytest.mark.parametrize("error", [None, {"code": 3, "message": "invalid database"}])
def test_operation_completion(monkeypatch, polled, error):
    monkeypatch.setenv("SPANNER_EMULATOR_HOST", "localhost:9010")
    completed = {"name": "operations/create", "done": True}
    if error:
        completed["error"] = error
    calls = []

    def urlopen(url, *, timeout):
        calls.append(url)
        assert 0 < timeout <= 30
        return io.BytesIO(json.dumps(completed).encode())

    monkeypatch.setattr("urllib.request.urlopen", urlopen)
    initial = {"name": completed["name"]} if polled else completed
    if error:
        with pytest.raises(RuntimeError, match="operation operations/create failed:.*invalid database"):
            _await_operation(initial)
    else:
        _await_operation(initial)
    assert len(calls) == int(polled)


def test_incomplete_operation_requires_name():
    with pytest.raises(RuntimeError, match="incomplete operation without a name"):
        _await_operation({})


def test_existing_resource_requires_no_operation():
    _await_operation(None)

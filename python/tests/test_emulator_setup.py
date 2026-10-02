"""Emulator setup must surface operation failures before running driver tests."""

import io
import json

import pytest

from conftest import _await_operation


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

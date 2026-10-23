from pathlib import Path

import pytest


pytest_plugins = ["pytester"]
HARNESS = Path(__file__).resolve().parents[1]
SKIPPED = "tests/test_cases.py::test_skip[spanner:emulator]"


@pytest.fixture
def harness(pytester, monkeypatch):
    monkeypatch.setenv("FOUNDRY_VALIDATION_REQUIRE_PASSES", "1")
    monkeypatch.setenv("FOUNDRY_VALIDATION_CHECK_SKIP_BASELINE", "1")
    monkeypatch.delenv("FOUNDRY_UPDATE_SKIP_BASELINE", raising=False)
    pytester.makeconftest((HARNESS / "conftest.py").read_text())
    pytester.makeini("[pytest]\ntestpaths = tests\n")
    tests = pytester.path / "tests"
    tests.mkdir()
    (tests / "test_cases.py").write_text('''
import pytest

def test_pass():
    pass

def test_lint_query():
    assert True

@pytest.mark.parametrize("driver", ["spanner:omni"], ids=["spanner:omni"])
def test_skip(driver):
    pytest.skip("unsupported")
''')
    baseline = pytester.path / "skip_baseline.txt"
    baseline.write_text(f"{SKIPPED}  # unsupported\n")
    return pytester, baseline


def test_full_run_normalizes_backend(harness):
    runner, _ = harness
    result = runner.runpytest_subprocess("-q")
    result.assert_outcomes(passed=2, skipped=1)
    assert result.ret == 0


def test_normalizes_backend_after_stream_parameters(harness):
    runner, baseline = harness
    test = runner.path / "tests" / "test_cases.py"
    test.write_text(test.read_text().replace(
        'ids=["spanner:omni"]', 'ids=["multiple_batches-spanner:omni"]'
    ))
    baseline.write_text(baseline.read_text().replace(
        "[spanner:", "[multiple_batches-spanner:"
    ))
    assert runner.runpytest_subprocess("-q").ret == 0


@pytest.mark.parametrize("baseline_text", ["", "tests/test_removed.py::test_old\n"])
def test_skip_drift_fails(harness, baseline_text):
    runner, baseline = harness
    baseline.write_text(baseline_text)
    result = runner.runpytest_subprocess("-q")
    assert result.ret == 1
    result.stdout.fnmatch_lines(["*case(s) skipped but not in the baseline*"])
    if baseline_text:
        result.stdout.fnmatch_lines(["*did not skip*"])


def test_full_update_replaces_baseline(harness, monkeypatch):
    runner, baseline = harness
    baseline.write_text("stale\n")
    monkeypatch.setenv("FOUNDRY_UPDATE_SKIP_BASELINE", "1")
    result = runner.runpytest_subprocess("-q")
    assert result.ret == 0
    assert f"{SKIPPED}  # unsupported\n" in baseline.read_text()
    assert "stale" not in baseline.read_text().splitlines()


@pytest.mark.parametrize("args", [
    ("--collect-only",),
    ("-k", "test_pass"),
    ("-m", "not unused"),
    ("tests/test_cases.py",),
    ("tests/test_cases.py::test_pass",),
    ("--deselect=tests/test_cases.py::test_skip[spanner:omni]",),
    ("--ignore=tests/missing.py",),
    ("--lf",),
])
@pytest.mark.parametrize("update", [False, True])
def test_partial_runs_preserve_baseline(harness, monkeypatch, args, update):
    runner, baseline = harness
    baseline.write_text("stale\n")
    if update:
        monkeypatch.setenv("FOUNDRY_UPDATE_SKIP_BASELINE", "1")
    result = runner.runpytest_subprocess("-q", *args)
    assert result.ret == (2 if update else 0)
    assert baseline.read_text() == "stale\n"


@pytest.mark.parametrize("body", ["assert False", "pytest.skip('no backend')"])
def test_unsuccessful_update_preserves_baseline(harness, monkeypatch, body):
    runner, baseline = harness
    test = runner.path / "tests" / "test_cases.py"
    test.write_text(test.read_text().replace("    pass", f"    {body}"))
    monkeypatch.setenv("FOUNDRY_UPDATE_SKIP_BASELINE", "1")
    monkeypatch.delenv("FOUNDRY_VALIDATION_REQUIRE_PASSES")
    original = baseline.read_text()
    result = runner.runpytest_subprocess("-q")
    assert result.ret == 1
    assert baseline.read_text() == original

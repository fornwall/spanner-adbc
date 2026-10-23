import os
from pathlib import Path

import pytest

pytest_plugins = ["adbc_drivers_validation.tests.conftest"]


SKIP_BASELINE_PATH = Path(__file__).with_name("skip_baseline.txt")

_SKIP_BASELINE_HEADER = """\
# Foundry validation skip baseline: sorted pytest nodeids expected to skip.
# Optional trailing "  # <reason>" annotations are ignored when comparing nodeids.
# Reasons are maintained in the .txtcase files.
#
# CI sets FOUNDRY_VALIDATION_CHECK_SKIP_BASELINE to reject new or stale skips.
# Regenerate after changing the skip set:
#   FOUNDRY_UPDATE_SKIP_BASELINE=1 scripts/run-foundry-validation.sh
"""


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    # Never replace the inventory with results from an interrupted or failing run.
    if exitstatus != pytest.ExitCode.OK:
        return
    full_run = _full_suite_run(session)
    if not full_run:
        if os.environ.get("FOUNDRY_UPDATE_SKIP_BASELINE"):
            pytest.exit("Skip baseline updates require a full test run", returncode=2)
        if session.config.option.collectonly:
            return
    _require_passes_guard(session)
    if session.exitstatus == pytest.ExitCode.OK and full_run:
        _skip_baseline_guard(session)


def _full_suite_run(session: pytest.Session) -> bool:
    config = session.config
    # Explicit files, node IDs and diagnostic modes do not represent the inventory.
    tests = Path(__file__).with_name("tests").resolve()
    return (
        {Path(arg).resolve() for arg in config.args} == {tests}
        and not session.shouldstop
        and not any(
            getattr(config.option, name, False)
            for name in (
                "collectonly", "keyword", "markexpr", "deselect", "ignore",
                "ignore_glob", "lf", "repl", "show_queries",
            )
        )
    )


def _require_passes_guard(session: pytest.Session) -> None:
    # Pytest succeeds when every case skips, including when the backend is missing.
    if not (
        os.environ.get("FOUNDRY_VALIDATION_REQUIRE_PASSES")
        or os.environ.get("FOUNDRY_UPDATE_SKIP_BASELINE")
    ):
        return
    reporter = session.config.pluginmanager.get_plugin("terminalreporter")
    # Lint cases pass without connecting and cannot establish that the backend ran.
    reports = reporter.stats.get("passed", []) if reporter is not None else []
    passed = any(
        report.nodeid.rsplit("::", 1)[-1].split("[", 1)[0] != "test_lint_query"
        for report in reports
    )
    if session.testscollected > 0 and not passed:
        message = (
            f"FOUNDRY_VALIDATION_REQUIRE_PASSES: collected {session.testscollected} "
            "case(s) but no database test passed. Check the driver and connection setup."
        )
        if reporter is not None:
            reporter.write_line(message, red=True)
        else:  # pragma: no cover - terminal reporter is a default plugin
            print(message)
        session.exitstatus = int(pytest.ExitCode.TESTS_FAILED)


def _skip_reason(report: pytest.TestReport) -> str:
    # A skip report's longrepr is a (path, lineno, "Skipped: <reason>") tuple.
    longrepr = getattr(report, "longrepr", None)
    if isinstance(longrepr, tuple) and len(longrepr) == 3:
        reason = str(longrepr[2])
        prefix = "Skipped: "
        if reason.startswith(prefix):
            reason = reason[len(prefix) :]
        return reason.strip()
    return ""


def _collect_skipped(session: pytest.Session) -> dict[str, str]:
    # Keyed by nodeid so a case skipped in several phases appears once.
    reporter = session.config.pluginmanager.get_plugin("terminalreporter")
    skipped: dict[str, str] = {}
    if reporter is None:  # pragma: no cover - terminal reporter is a default plugin
        return skipped
    for report in reporter.stats.get("skipped", []):
        # Both backends run the same cases and share one reviewed skip inventory.
        nodeid = report.nodeid.replace("spanner:omni", "spanner:emulator", 1)
        skipped[nodeid] = _skip_reason(report)
    return skipped


def _load_baseline(path: Path) -> set[str]:
    # One nodeid per line, optionally followed by "  # <reason>".
    nodeids: set[str] = set()
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        nodeid = line.split(" #", 1)[0].strip()
        if nodeid:
            nodeids.add(nodeid)
    return nodeids


def _write_baseline(path: Path, skipped: dict[str, str]) -> None:
    lines = [_SKIP_BASELINE_HEADER]
    for nodeid in sorted(skipped):
        reason = skipped[nodeid]
        lines.append(f"{nodeid}  # {reason}" if reason else nodeid)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _skip_baseline_guard(session: pytest.Session) -> None:
    # Fail when the skip set changes, unless explicitly regenerating the baseline.
    reporter = session.config.pluginmanager.get_plugin("terminalreporter")

    def emit(message: str) -> None:
        if reporter is not None:
            reporter.write_line(message, red=True)
        else:  # pragma: no cover - terminal reporter is a default plugin
            print(message)

    if os.environ.get("FOUNDRY_UPDATE_SKIP_BASELINE"):
        skipped = _collect_skipped(session)
        _write_baseline(SKIP_BASELINE_PATH, skipped)
        if reporter is not None:
            reporter.write_line(
                f"FOUNDRY_UPDATE_SKIP_BASELINE: wrote {len(skipped)} nodeid(s) to "
                f"{SKIP_BASELINE_PATH.name}",
                green=True,
            )
        return

    if not os.environ.get("FOUNDRY_VALIDATION_CHECK_SKIP_BASELINE"):
        return
    if not SKIP_BASELINE_PATH.exists():
        emit(
            "FOUNDRY_VALIDATION_CHECK_SKIP_BASELINE: no baseline at "
            f"{SKIP_BASELINE_PATH}; regenerate with "
            "FOUNDRY_UPDATE_SKIP_BASELINE=1 scripts/run-foundry-validation.sh"
        )
        session.exitstatus = int(pytest.ExitCode.TESTS_FAILED)
        return

    baseline = _load_baseline(SKIP_BASELINE_PATH)
    live = set(_collect_skipped(session))
    unlisted = sorted(live - baseline)
    stale = sorted(baseline - live)
    if not unlisted and not stale:
        return

    if unlisted:
        emit(
            "FOUNDRY_VALIDATION_CHECK_SKIP_BASELINE: "
            f"{len(unlisted)} case(s) skipped but not in the baseline. Fix them, or "
            "add them with FOUNDRY_UPDATE_SKIP_BASELINE=1:"
        )
        for nodeid in unlisted:
            emit(f"  + {nodeid}")
    if stale:
        emit(
            "FOUNDRY_VALIDATION_CHECK_SKIP_BASELINE: "
            f"{len(stale)} baseline case(s) did not skip. Review cases that now pass "
            "or were renamed upstream, then regenerate the baseline:"
        )
        for nodeid in stale:
            emit(f"  - {nodeid}")
    session.exitstatus = int(pytest.ExitCode.TESTS_FAILED)

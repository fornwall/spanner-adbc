import os
import sys
from pathlib import Path

import adbc_drivers_validation.model
import adbc_drivers_validation.tests.conftest
import pytest
from adbc_drivers_validation.tests.conftest import (  # noqa: F401
    conn_factory,
    db_kwargs,
    manual_test,
    noci,
)

from . import spanner


def pytest_collection_modifyitems(
    session: pytest.Session, config: pytest.Config, items: list[pytest.Item]
) -> None:
    # The upstream module is not registered as a plugin, so call its hook explicitly.
    adbc_drivers_validation.tests.conftest.pytest_collection_modifyitems(
        session, config, items
    )


def pytest_addoption(parser):
    adbc_drivers_validation.tests.conftest.pytest_addoption(parser)
    parser.addoption("--vendor-version", action="store", default=None)


# The committed skip-inventory baseline (see _skip_baseline_guard below and the
# "Skip-inventory baseline guard" section of foundry-validation/README.md).
SKIP_BASELINE_PATH = Path(__file__).resolve().parents[1] / "skip_baseline.txt"

_SKIP_BASELINE_HEADER = """\
# Foundry validation skip baseline: sorted pytest nodeids expected to skip.
# Optional trailing "  # <reason>" annotations are ignored when comparing nodeids.
# Reasons are maintained in the .txtcase files.
#
# CI sets FOUNDRY_VALIDATION_CHECK_SKIP_BASELINE to reject new or stale skips.
# Regenerate after an intentional change to the skip set:
#   FOUNDRY_UPDATE_SKIP_BASELINE=1 scripts/run-foundry-validation.sh
"""


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    _require_passes_guard(session, exitstatus)
    _skip_baseline_guard(session)


def _require_passes_guard(session: pytest.Session, exitstatus: int) -> None:
    # Pytest succeeds when every case skips. CI requires at least one passing case;
    # local filtered runs leave this guard disabled.
    if exitstatus != int(pytest.ExitCode.OK):
        return
    if not os.environ.get("FOUNDRY_VALIDATION_REQUIRE_PASSES"):
        return
    reporter = session.config.pluginmanager.get_plugin("terminalreporter")
    passed = len(reporter.stats.get("passed", [])) if reporter is not None else 0
    if session.testscollected > 0 and passed == 0:
        message = (
            f"FOUNDRY_VALIDATION_REQUIRE_PASSES: collected {session.testscollected} "
            "case(s) but none passed — every case skipped or errored. This usually "
            "means the harness itself is broken (driver failed to load, connection "
            "setup failed), not an expected Spanner SQL-dialect skip."
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
    # Map each skipped nodeid to its (last-seen) reason. Keyed by nodeid so a case
    # reported skipped in more than one phase collapses to a single entry.
    reporter = session.config.pluginmanager.get_plugin("terminalreporter")
    skipped: dict[str, str] = {}
    if reporter is None:  # pragma: no cover - terminal reporter is a default plugin
        return skipped
    for report in reporter.stats.get("skipped", []):
        # Both backends run the same cases and share one reviewed skip inventory.
        nodeid = report.nodeid.replace("[spanner:omni", "[spanner:emulator", 1)
        skipped[nodeid] = _skip_reason(report)
    return skipped


def _load_baseline(path: Path) -> set[str]:
    # One nodeid per line. Blank lines and `#` comment lines are ignored; a trailing
    # "  # <reason>" annotation is stripped so only the nodeid is compared.
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
    # Filtered runs cannot be compared with the full skip inventory.
    if getattr(session.config.option, "keyword", "") or getattr(
        session.config.option, "markexpr", ""
    ):
        return
    if not SKIP_BASELINE_PATH.exists():
        emit(
            "FOUNDRY_VALIDATION_CHECK_SKIP_BASELINE: no baseline at "
            f"{SKIP_BASELINE_PATH} — regenerate with "
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
            f"{len(unlisted)} case(s) skipped but NOT in the baseline (unlisted "
            "skip) — a new or regressed case is skipping without being acknowledged. "
            "Triage it (fix it, or add it to the baseline with a reason via "
            "FOUNDRY_UPDATE_SKIP_BASELINE=1):"
        )
        for nodeid in unlisted:
            emit(f"  + {nodeid}")
    if stale:
        emit(
            "FOUNDRY_VALIDATION_CHECK_SKIP_BASELINE: "
            f"{len(stale)} baseline entr(y/ies) did NOT skip this run (stale entry) "
            "— the case no longer skips. If it now passes, remove its .txtcase skip "
            "and regenerate the baseline to enable the coverage; if it now fails or "
            "was renamed upstream, investigate:"
        )
        for nodeid in stale:
            emit(f"  - {nodeid}")
    session.exitstatus = int(pytest.ExitCode.TESTS_FAILED)


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

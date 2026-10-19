"""Option-key parity and option round trips through the Python wrappers."""

import re
from pathlib import Path

import pytest

from conftest import database_options

import spanner_adbc
import spanner_adbc.dbapi as spanner
from spanner_adbc import (
    ConnectionOptions,
    DatabaseOptions,
    StatementOptions,
)

_REPO_ROOT = Path(__file__).resolve().parents[2]
_LIB_RS = _REPO_ROOT / "src" / "lib.rs"
_OPTIONS_MD = _REPO_ROOT / "docs" / "options.md"

# Read-only options have no enum member because the enums configure handles.
_EXPECTED_MISSING = {
    # Available through get_option_int after a commit; setting it is unsupported.
    "spanner.commit_stats.mutation_count",
}

# Allow rustfmt to wrap a constant's value onto the next line.
_OPTION_CONST_RE = re.compile(r'pub const OPTION_[A-Z0-9_]+: &str =\s*"([^"]+)"\s*;')
_DOC_TABLE_KEY_RE = re.compile(r"^\| `([^`]+)` \|", re.M)

_LEVELS = [
    ("Database options", DatabaseOptions),
    ("Connection options", ConnectionOptions),
    ("Statement options", StatementOptions),
]

# Shared option sections contribute keys to both connection and statement enums.
_SHARED_SECTION = "Shared options (connection and statement)"
_LEVEL_SECTIONS = {
    "Database options": ["Database options"],
    "Connection options": ["Connection-only options", _SHARED_SECTION],
    "Statement options": ["Statement-only options", _SHARED_SECTION],
}


def _require_sources():
    if not _LIB_RS.is_file() or not _OPTIONS_MD.is_file():
        pytest.skip("driver sources not available (installed wheel, not a checkout)")


def _driver_option_keys():
    """Every option key the driver declares, parsed out of `src/lib.rs`."""
    keys = set(_OPTION_CONST_RE.findall(_LIB_RS.read_text()))
    assert keys, f"parsed no OPTION_* constants out of {_LIB_RS}"
    return keys


def _documented_keys_by_level():
    """Collect documented keys, including shared keys at both applicable levels."""
    keys_by_section = {}
    for section in re.split(r"^## ", _OPTIONS_MD.read_text(), flags=re.M):
        heading = section.split("\n", 1)[0].strip()
        keys_by_section[heading] = set(_DOC_TABLE_KEY_RE.findall(section))
    by_level = {}
    for level, sections in _LEVEL_SECTIONS.items():
        missing = [s for s in sections if s not in keys_by_section]
        assert not missing, f"docs/options.md has no section(s) {missing} (for {level!r})"
        by_level[level] = set().union(*(keys_by_section[s] for s in sections))
    return by_level


def test_every_driver_option_key_is_documented_at_some_level():
    """An undocumented key must not escape the enum parity checks below."""
    _require_sources()
    documented = set().union(*_documented_keys_by_level().values())
    undocumented = _driver_option_keys() - documented
    assert not undocumented, (
        f"option key(s) {sorted(undocumented)} are declared in src/lib.rs but appear in no "
        "level table in docs/options.md"
    )


@pytest.mark.parametrize("heading,enum_cls", _LEVELS, ids=lambda v: getattr(v, "__name__", v))
def test_every_driver_option_key_has_a_typed_enum_member(heading, enum_cls):
    """Every driver option documented at a level has a member in that level's enum."""
    _require_sources()
    # Standard ADBC keys (`uri`, `adbc.*`) are documented but not OPTION_* constants.
    expected = (
        _documented_keys_by_level()[heading] & _driver_option_keys()
    ) - _EXPECTED_MISSING
    present = {member.value for member in enum_cls}
    missing = expected - present
    assert not missing, (
        f"{enum_cls.__name__} has no member for driver option(s) {sorted(missing)}. "
        "Add one to python/spanner_adbc/_options.py, or list it with a reason in "
        "_EXPECTED_MISSING."
    )


@pytest.mark.parametrize("heading,enum_cls", _LEVELS, ids=lambda v: getattr(v, "__name__", v))
def test_typed_enum_members_are_real_keys_at_their_level(heading, enum_cls):
    """Catch typos and members filed under the wrong level."""
    _require_sources()
    documented = _documented_keys_by_level()[heading]
    bogus = {member.value for member in enum_cls} - documented
    assert not bogus, (
        f"{enum_cls.__name__} member(s) {sorted(bogus)} are not documented as "
        f"{heading.lower()} in docs/options.md"
    )


def test_option_enums_are_exported_without_aliases():
    for enum_cls in (DatabaseOptions, ConnectionOptions, StatementOptions):
        assert getattr(spanner, enum_cls.__name__) is enum_cls
        assert enum_cls.__name__ in spanner_adbc.__all__
        assert enum_cls.__name__ in spanner.__all__
        # Enum iteration skips aliases, so check all declared members.
        values = [member.value for member in enum_cls.__members__.values()]
        assert len(values) == len(set(values))
        assert all(str(member) == member.value for member in enum_cls)


def test_low_level_connect_accepts_option_members_directly(monkeypatch):
    calls = []
    database = object()

    def make_database(**kwargs):
        calls.append(kwargs)
        return database

    monkeypatch.setattr(spanner_adbc, "_driver_path", lambda: "/driver.so")
    monkeypatch.setattr(spanner_adbc.adbc_driver_manager, "AdbcDatabase", make_database)
    options = {
        DatabaseOptions.URI: "spanner:///projects/p/instances/i/databases/d",
        DatabaseOptions.EMULATOR: "true",
    }
    assert spanner_adbc.connect(db_kwargs=options) is database
    assert calls == [
        {
            "driver": "/driver.so",
            "entrypoint": spanner_adbc.ENTRYPOINT,
            "uri": "spanner:///projects/p/instances/i/databases/d",
            "spanner.emulator": "true",
        }
    ]


@pytest.mark.parametrize("autocommit", [False, True])
def test_dbapi_connect_forwards_options(monkeypatch, autocommit):
    monkeypatch.setattr(spanner, "_driver_path", lambda: "/driver.so")
    monkeypatch.setattr(spanner.adbc_driver_manager.dbapi, "connect", lambda **kwargs: kwargs)
    options = {DatabaseOptions.ACCESS_TOKEN: "token"}
    connection_options = {ConnectionOptions.REQUEST_TAG: "python-test"}

    assert spanner.connect(options, connection_options, autocommit=autocommit) == {
        "driver": "/driver.so",
        "entrypoint": spanner_adbc.ENTRYPOINT,
        "db_kwargs": {"spanner.auth.access_token": "token"},
        "conn_kwargs": {"spanner.request.tag": "python-test"},
        "autocommit": autocommit,
    }
    assert options == {DatabaseOptions.ACCESS_TOKEN: "token"}
    assert connection_options == {ConnectionOptions.REQUEST_TAG: "python-test"}


def test_connection_option_round_trips_through_kwargs(spanner_database):
    conn = spanner.connect(
        db_kwargs=database_options(spanner_database),
        conn_kwargs={ConnectionOptions.REQUEST_TAG: "adbc-py-e2e"},
        autocommit=True,
    )
    try:
        assert (
            conn.adbc_connection.get_option(ConnectionOptions.REQUEST_TAG.value)
            == "adbc-py-e2e"
        )
        with conn.cursor(adbc_stmt_kwargs={StatementOptions.ROWS_PER_BATCH: "1024"}) as cur:
            cur.execute("SELECT 1 AS one")
            assert cur.fetchone()[0] == 1
    finally:
        conn.close()

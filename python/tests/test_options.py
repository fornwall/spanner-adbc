"""Option-key parity and option round trips through the Python wrappers."""

import inspect
import re
from pathlib import Path

import pytest

from conftest import database_options

import spanner_adbc
import spanner_adbc.dbapi
import spanner_adbc.dbapi as spanner
from spanner_adbc import (
    ConnectionOptions,
    DatabaseOptions,
    StatementOptions,
)

# Derive option keys from Rust constants and levels from the reference tables,
# so additions need corresponding documentation and Python enum members.

_REPO_ROOT = Path(__file__).resolve().parents[2]
_LIB_RS = _REPO_ROOT / "src" / "lib.rs"
_OPTIONS_MD = _REPO_ROOT / "docs" / "options.md"

# Driver option keys that intentionally have NO typed enum member. Every entry
# needs a reason — this allowlist is what keeps the guard honest.
_EXPECTED_MISSING = {
    # Read-only introspection key, not a setting: it is *read back* after a commit
    # via `get_option_int`, and setting it returns NotImplemented. The enums exist to
    # populate db_kwargs / conn_kwargs / adbc_stmt_kwargs, so a member would only
    # invite a call that the driver rejects.
    "spanner.commit_stats.mutation_count",
}

# `pub const OPTION_FOO: &str = "spanner.foo";` — the value may wrap onto its own
# line (rustfmt does this for the longer keys), hence the newline-tolerant `\s*`.
_OPTION_CONST_RE = re.compile(r'pub const OPTION_[A-Z0-9_]+: &str =\s*"([^"]+)"\s*;')

# A row of a `docs/options.md` level table: `| `spanner.foo` | ... |`.
_DOC_TABLE_KEY_RE = re.compile(r"^\| `([^`]+)` \|", re.M)

_LEVELS = [
    ("Database options", DatabaseOptions),
    ("Connection options", ConnectionOptions),
    ("Statement options", StatementOptions),
]

# Each option *level* draws its documented keys from one or more `docs/options.md`
# sections. The docs split the shared options — the ones available on BOTH the
# connection and the statement — into their own `## Shared options …` section
# rather than repeating them under each level, so that one section feeds both the
# connection and the statement levels.
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
    assert keys, f"parsed no OPTION_* constants out of {_LIB_RS} — the guard would be vacuous"
    return keys


def _documented_keys_by_level():
    """`{level name: {key, ...}}` — the option keys documented at each level.

    A level aggregates the tables of one or more `docs/options.md` sections (see
    `_LEVEL_SECTIONS`); the shared section, documenting options available on both
    the connection and the statement, feeds both of those levels.
    """
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
    """Precondition for the parity check below: `docs/options.md` must place every
    `OPTION_*` key in a level table, otherwise a key could dodge the parity test by
    being undocumented rather than by being present at the wrong level.

    (The Rust test `every_handled_option_key_is_documented` in `src/lib.rs` checks
    documentation too, but against a hand-written key list; this one derives the
    key set, so it also covers keys that list forgets.)
    """
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
    # Intersecting with the OPTION_* keys drops the standard ADBC keys (`uri`,
    # `adbc.*`) that the docs also list: those are defined by adbc_driver_manager,
    # not by this crate, so the enums include them only for convenience.
    expected = (
        _documented_keys_by_level()[heading] & _driver_option_keys()
    ) - _EXPECTED_MISSING
    present = {member.value for member in enum_cls}
    missing = expected - present
    assert not missing, (
        f"{enum_cls.__name__} has no member for driver option(s) {sorted(missing)}. "
        "Add one to python/spanner_adbc/_options.py, or — if the omission is "
        "deliberate — record it with a reason in _EXPECTED_MISSING."
    )


@pytest.mark.parametrize("heading,enum_cls", _LEVELS, ids=lambda v: getattr(v, "__name__", v))
def test_typed_enum_members_are_real_keys_at_their_level(heading, enum_cls):
    """The reverse direction: no typos, and no member filed under the wrong level."""
    _require_sources()
    documented = _documented_keys_by_level()[heading]
    bogus = {member.value for member in enum_cls} - documented
    assert not bogus, (
        f"{enum_cls.__name__} member(s) {sorted(bogus)} are not documented as "
        f"{heading.lower()} in docs/options.md — typo, or filed at the wrong level?"
    )


def test_connect_functions_take_db_kwargs():
    """Both connect entry points take credentials through db_kwargs."""
    for fn in (spanner_adbc.connect, spanner_adbc.dbapi.connect):
        params = inspect.signature(fn).parameters
        assert "db_kwargs" in params
        assert "access_token" not in params
        assert "keyfile" not in params
        assert "uri" not in params


def test_option_enums_are_exported_and_well_formed():
    for enum_cls in (DatabaseOptions, ConnectionOptions, StatementOptions):
        values = [member.value for member in enum_cls]
        # No duplicate keys within a level.
        assert len(values) == len(set(values))
        # Every key uses a known ADBC prefix.
        assert all(v.startswith(("spanner.", "adbc.", "uri")) for v in values)


def test_option_enum_values_are_usable_as_kwargs_keys():
    # The whole point of the enums: .value is the raw string db_kwargs expects.
    assert DatabaseOptions.ACCESS_TOKEN.value == "spanner.auth.access_token"
    assert ConnectionOptions.READ_STALENESS.value == "spanner.read.staleness"
    assert StatementOptions.ROWS_PER_BATCH.value == "spanner.rows_per_batch"


@pytest.mark.parametrize("enum_cls", [DatabaseOptions, ConnectionOptions, StatementOptions])
def test_option_members_behave_as_wire_strings(enum_cls):
    for member in enum_cls:
        assert isinstance(member, str)
        assert member == member.value
        assert hash(member) == hash(member.value)
        assert str(member) == member.value
        assert "%s" % member == member.value
        assert f"{member}" == member.value
        assert "{}".format(member) == member.value
        assert f"{member:>80}" == f"{member.value:>80}"
        assert dict(**{member: "configured"}) == {member.value: "configured"}


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


def test_connection_option_round_trips_through_kwargs(spanner_database):
    """A ``spanner.*`` vendor option passed via ``conn_kwargs`` takes effect
    end-to-end: it round-trips through ``get_option`` and a query runs under it."""
    conn = spanner.connect(
        db_kwargs=database_options(spanner_database),
        conn_kwargs={ConnectionOptions.REQUEST_TAG: "adbc-py-e2e"},
        autocommit=True,
    )
    try:
        # The value the driver received round-trips back through get_option.
        assert (
            conn.adbc_connection.get_option(ConnectionOptions.REQUEST_TAG.value)
            == "adbc-py-e2e"
        )
        # And a query still runs with the option in effect.
        with conn.cursor(adbc_stmt_kwargs={StatementOptions.ROWS_PER_BATCH: "1024"}) as cur:
            cur.execute("SELECT 1 AS one")
            assert cur.fetchone()[0] == 1
    finally:
        conn.close()

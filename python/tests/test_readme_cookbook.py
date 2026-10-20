"""Run README Python examples against the configured Spanner target.

Blocks marked ``docs-test: skip`` or without ``spanner.connect`` are excluded.
Connection options are redirected to the test database; snippets run unchanged.
"""

import pathlib
import re

import pytest

from conftest import database_options

pytest.importorskip("pyarrow")  # every snippet returns Arrow

README = pathlib.Path(__file__).resolve().parents[1] / "README.md"


def _runnable_blocks():
    text = README.read_text()
    blocks = re.findall(r"```python([^\n]*)\n(.*?)```", text, re.DOTALL)
    runnable = []
    for info, block in blocks:
        if "docs-test: skip" in info or block.lstrip().startswith("# docs-test: skip"):
            continue
        if "spanner.connect" not in block:
            continue
        runnable.append(block)
    return runnable


RUNNABLE = _runnable_blocks()


def test_readme_has_runnable_snippets():
    # Guard against a parsing/format change silently dropping every example.
    assert RUNNABLE, "no runnable ```python blocks found in README.md"


@pytest.fixture(scope="module")
def cookbook_env(spanner_database):
    """Seed `Singers` and redirect the examples to the configured test database."""
    import spanner_adbc.dbapi as sp

    conn = sp.connect(
        db_kwargs=database_options(spanner_database),
        autocommit=True,
    )
    try:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS Singers")
            cur.execute(
                "CREATE TABLE Singers ("
                "  SingerId INT64 NOT NULL,"
                "  FirstName STRING(MAX),"
                ") PRIMARY KEY (SingerId)"
            )
            cur.execute("INSERT INTO Singers (SingerId, FirstName) VALUES (1, 'Alice'), (2, 'Bob')")
    finally:
        conn.close()

    original = sp.connect

    def redirected(uri=None, db_kwargs=None, *args, **kwargs):
        merged = {**(db_kwargs or {}), **database_options(spanner_database)}
        return original(None, merged, *args, **kwargs)

    sp.connect = redirected
    try:
        yield
    finally:
        sp.connect = original


@pytest.mark.parametrize(
    "code",
    RUNNABLE,
    ids=[f"block{i}" for i in range(len(RUNNABLE))],
)
def test_readme_snippet(code, cookbook_env):
    try:
        exec(compile(code, "<README snippet>", "exec"), {"__name__": "__readme__"})
    except ModuleNotFoundError as exc:
        pytest.skip(f"optional dependency not installed: {exc.name}")

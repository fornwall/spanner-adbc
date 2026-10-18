"""ADBC driver for Google Cloud Spanner, including the native shared library.

Use :func:`connect` for a low-level ADBC database handle or
:func:`spanner_adbc.dbapi.connect` for DBAPI cursors and Arrow/DataFrame helpers.
To load by name through ``adbc_driver_manager`` (``driver="spanner"``), register
the library with ``python -m spanner_adbc.manifest install``.
"""

import functools
import pathlib
from collections.abc import Mapping

import adbc_driver_manager

from ._options import ConnectionOptions, DatabaseOptions, StatementOptions
from ._version import __version__

__all__ = [
    "connect",
    "ENTRYPOINT",
    "DatabaseOptions",
    "ConnectionOptions",
    "StatementOptions",
    "__version__",
]

#: C entrypoint exported by the shared library (see src/ffi/mod.rs).
ENTRYPOINT = "AdbcSpannerInit"


def connect(
    db_kwargs: Mapping[str, str] | None = None,
) -> adbc_driver_manager.AdbcDatabase:
    """Create a low-level ADBC database handle for Spanner.

    Parameters
    ----------
    db_kwargs:
        Database options keyed by :class:`DatabaseOptions` members or raw strings.
        ``DatabaseOptions.URI`` requires a URI such as
        ``spanner:///projects/p/instances/i/databases/d``. Pass credentials and
        emulator settings here too; see ``docs/options.md``.

    For a DBAPI 2.0 connection, prefer :func:`spanner_adbc.dbapi.connect`.
    """
    return adbc_driver_manager.AdbcDatabase(
        driver=_driver_path(), entrypoint=ENTRYPOINT, **(db_kwargs or {})
    )


@functools.cache
def _driver_path() -> str:
    """Absolute path to the shared library bundled in this wheel."""
    here = pathlib.Path(__file__).resolve().parent
    for name in ("libspanner_adbc.so", "libspanner_adbc.dylib", "spanner_adbc.dll"):
        candidate = here / name
        if candidate.is_file():
            return str(candidate)
    raise RuntimeError(
        "spanner_adbc: no bundled Spanner driver library found next to "
        f"{here}. This usually means a source/sdist install without a matching "
        "platform wheel; install a prebuilt wheel for your platform instead."
    )

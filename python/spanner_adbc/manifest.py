"""Register the bundled library for ADBC discovery by name or URI scheme.

The manifest stores an absolute library path, so rerun after installing,
upgrading or moving the Python environment::

    python -m spanner_adbc.manifest install  # or: spanner-adbc-install-manifest
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import sys
import tempfile
from collections.abc import Sequence

from . import ENTRYPOINT, __version__, _driver_path

__all__ = [
    "MANIFEST_NAME",
    "default_install_dir",
    "install",
    "manifest_text",
]

#: Matches ``driver="spanner"`` and the ``spanner://`` URI scheme.
MANIFEST_NAME = "spanner.toml"


def _toml_string(value: str) -> str:
    # TOML rejects JSON's surrogate-pair escapes, so keep non-ASCII characters
    # literal. JSON leaves DEL unescaped, but TOML forbids it raw.
    return json.dumps(value, ensure_ascii=False).replace("\x7f", "\\u007f")


def manifest_text(library: str | os.PathLike[str] | None = None) -> str:
    """Render a driver manifest pointing at *library*, by default the bundled one."""
    path = pathlib.Path(library) if library is not None else pathlib.Path(_driver_path())
    path = path.resolve()
    return f"""\
# ADBC driver manifest for Google Cloud Spanner.
# Regenerate with `python -m spanner_adbc.manifest install` after upgrading
# or moving the Python environment: the library path is absolute.

manifest_version = 1

name = "Google Cloud Spanner"
publisher = "fornwall"
license = "Apache-2.0"
version = {_toml_string(__version__)}
url = "https://github.com/fornwall/spanner-adbc"
source = "spanner-adbc (Python wheel)"

[ADBC]
version = "1.1.0"

[Driver]
entrypoint = {_toml_string(ENTRYPOINT)}
shared = {_toml_string(str(path))}
"""


def default_install_dir() -> pathlib.Path:
    """A directory searched by the driver manager under its default load flags.

    Virtual environments use ``<sys.prefix>/etc/adbc/drivers``. Otherwise:

    * Linux: ``$XDG_CONFIG_HOME/adbc/drivers``, else ``~/.config/adbc/drivers``
    * macOS: ``~/Library/Application Support/ADBC/Drivers``
    * Windows: ``%LOCALAPPDATA%\\ADBC\\Drivers``
    """
    if sys.prefix != sys.base_prefix:
        return pathlib.Path(sys.prefix) / "etc" / "adbc" / "drivers"

    if sys.platform == "win32":
        local = os.environ.get("LOCALAPPDATA")
        if not local:
            raise RuntimeError(
                "spanner_adbc: %LOCALAPPDATA% is not set, so the driver "
                "manager's user config directory cannot be located; pass an "
                "explicit directory instead."
            )
        return pathlib.Path(local) / "ADBC" / "Drivers"

    if sys.platform == "darwin":
        return pathlib.Path.home() / "Library" / "Application Support" / "ADBC" / "Drivers"

    xdg = os.environ.get("XDG_CONFIG_HOME")
    base = pathlib.Path(xdg) if xdg else pathlib.Path.home() / ".config"
    return base / "adbc" / "drivers"


def install(
    directory: str | os.PathLike[str] | None = None,
    *,
    library: str | os.PathLike[str] | None = None,
) -> pathlib.Path:
    """Write ``spanner.toml`` into *directory* and return the path written.

    *directory* defaults to :func:`default_install_dir` and is created if
    missing. An existing manifest is overwritten.
    """
    text = manifest_text(library)
    target_dir = pathlib.Path(directory) if directory is not None else default_install_dir()
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / MANIFEST_NAME
    # Write then rename, so a failed write keeps the old manifest and readers
    # never see a partial file.
    with tempfile.TemporaryDirectory(dir=target_dir, prefix=".spanner-") as temporary:
        temporary_path = pathlib.Path(temporary) / MANIFEST_NAME
        temporary_path.write_text(text, encoding="utf-8")
        if target.exists():
            temporary_path.chmod(target.stat().st_mode & 0o777)
        temporary_path.replace(target)
    return target


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m spanner_adbc.manifest",
        description=(
            "Install an ADBC driver manifest so the Spanner driver resolves by "
            'name (driver="spanner") or by URI scheme (uri="spanner:///...").'
        ),
    )
    sub = parser.add_subparsers(dest="command")

    p_install = sub.add_parser("install", help="write the manifest (default)")
    p_install.add_argument(
        "-d",
        "--dir",
        dest="directory",
        default=None,
        help=(
            f"directory to write {MANIFEST_NAME} into (default: a directory the "
            "driver manager searches; see the `path` subcommand)"
        ),
    )
    sub.add_parser("print", help="print the manifest to stdout without writing it")
    sub.add_parser("path", help="print the default install path and exit")

    args = parser.parse_args(argv)
    command = args.command or "install"

    try:
        if command == "print":
            sys.stdout.write(manifest_text())
        elif command == "path":
            print(default_install_dir() / MANIFEST_NAME)
        else:
            directory = getattr(args, "directory", None)
            written = install(directory)
            print(f"Wrote {written}")
            if directory is not None:
                print(f"Add {written.parent.resolve()} to ADBC_DRIVER_PATH for discovery.")
    except (OSError, RuntimeError) as exc:
        parser.exit(1, f"{parser.prog}: {exc}\n")
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised via the console script
    raise SystemExit(main())

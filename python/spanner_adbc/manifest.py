"""Register the bundled library for ADBC discovery by name or URI scheme.

Run after installing, upgrading, or moving the Python environment::

    python -m spanner_adbc.manifest install
    # Equivalent console command:
    spanner-adbc-install-manifest

The manifest uses an absolute library path and is installed in a directory the
driver manager searches; see :func:`default_install_dir`.
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import platform
import sys
import sysconfig
import tempfile
from collections.abc import Sequence

from . import ENTRYPOINT, __version__, _driver_path

__all__ = [
    "MANIFEST_NAME",
    "default_install_dir",
    "install",
    "manifest_text",
    "platform_tuple",
]

#: Matches ``driver="spanner"`` and the ``spanner://`` URI scheme.
MANIFEST_NAME = "spanner.toml"

# ADBC architecture names for the platforms with published wheels.
_ARCHES = {
    "x86_64": "amd64",
    "amd64": "amd64",
    "aarch64": "arm64",
    "arm64": "arm64",
}

_OSES = {
    "linux": "linux",
    "darwin": "macos",
    "win32": "windows",
    "cygwin": "windows",
    "freebsd": "freebsd",
    "openbsd": "openbsd",
}


def _is_musl() -> bool:
    """Infer the driver manager wheel's libc from the interpreter's platform."""
    if sys.platform != "linux":
        return False
    # Some distributions include musl in the platform tag; Alpine does not.
    if "musl" in (sysconfig.get_platform() or ""):
        return True
    try:
        # glibc reports e.g. ("glibc", "2.35"); a musl build reports ("", "").
        name, _version = platform.libc_ver()
    except OSError:  # pragma: no cover - platform.libc_ver is very forgiving
        return False
    return not name


def platform_tuple() -> str:
    """The ``<os>_<arch>[_<env>]`` tuple the driver manager looks up.

    Mirrors ``InternalAdbcCurrentArch()`` in the C driver manager
    (``c/driver_manager/adbc_driver_manager.cc``) and ``arch_triplet()`` in the
    Rust one.
    """
    os_name = _OSES.get(sys.platform)
    if os_name is None and sys.platform.startswith("freebsd"):
        os_name = "freebsd"
    machine = platform.machine().lower()
    arch = _ARCHES.get(machine)
    if os_name is None or arch is None:
        raise RuntimeError(
            "spanner_adbc: cannot derive an ADBC platform tuple for "
            f"sys.platform={sys.platform!r} machine={platform.machine()!r}; "
            "this platform is not one the Spanner driver ships binaries for."
        )
    suffix = "_musl" if os_name == "linux" and _is_musl() else ""
    return f"{os_name}_{arch}{suffix}"


def _toml_string(value: str) -> str:
    """Escape paths, including control characters, as TOML basic strings."""
    # Keep non-BMP Unicode intact: JSON surrogate escapes are invalid in TOML.
    return json.dumps(value, ensure_ascii=False).replace("\x7f", "\\u007f")


def manifest_text(library: str | os.PathLike[str] | None = None) -> str:
    """Render a driver manifest for this installation.

    Parameters
    ----------
    library:
        Absolute path to the driver shared library. Defaults to the library
        bundled in this wheel.

    ``Driver.shared`` uses an absolute path, so platform-tuple detection is
    unnecessary. The manifest describes only this installation.
    """
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

    *directory* defaults to :func:`default_install_dir` and is created if it
    does not exist. An existing manifest is overwritten, so re-running this
    after an upgrade is the supported way to refresh a stale path.
    """
    text = manifest_text(library)
    target_dir = pathlib.Path(directory) if directory is not None else default_install_dir()
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / MANIFEST_NAME
    # Replace only after writing succeeds, so a failed upgrade leaves the old
    # manifest usable and concurrent readers never see a partial TOML file.
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

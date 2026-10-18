"""Tests for the ADBC driver manifest this package generates.

The offline tests check the manifest's shape against the specification
(``docs/source/format/driver_manifests.rst`` in apache/arrow-adbc). The emulator
test is the one that actually proves discovery works: it writes a manifest into
a scratch directory, points ``ADBC_DRIVER_PATH`` at it, and connects through the
plain ``adbc_driver_manager`` — once by ``driver="spanner"`` and once by URI
scheme alone, with no ``driver`` option at all.
"""

import os
import tomllib

import pytest

import spanner_adbc
from spanner_adbc import manifest as manifest_mod

# Architecture / OS names from the spec's authoritative platform-tuple table.
_SPEC_OSES = {"linux", "macos", "windows", "freebsd", "openbsd"}
_SPEC_ARCHES = {
    "x86",
    "amd64",
    "arm",
    "armbe",
    "arm64",
    "arm64be",
    "s390x",
    "powerpc",
    "powerpc64",
    "powerpc64le",
    "riscv",
    "riscv64",
    "sparc",
    "sparc64",
    "wasm32",
    "wasm64",
}
_SPEC_ENVS = {"musl", "mingw"}


def _has_library() -> bool:
    try:
        spanner_adbc._driver_path()
    except RuntimeError:
        return False
    return True


needs_library = pytest.mark.skipif(
    not _has_library(),
    reason="no bundled driver library in this checkout/install",
)


def test_platform_tuple_matches_the_spec_vocabulary():
    tuple_ = manifest_mod.platform_tuple()
    os_name, _, rest = tuple_.partition("_")
    arch, _, env = rest.partition("_")
    assert os_name in _SPEC_OSES, tuple_
    assert arch in _SPEC_ARCHES, tuple_
    if env:
        assert env in _SPEC_ENVS, tuple_


@needs_library
def test_manifest_parses_and_has_the_required_keys():
    doc = tomllib.loads(manifest_mod.manifest_text())

    # `manifest_version`, if present, must be 1; managers reject anything higher.
    assert doc["manifest_version"] == 1
    # The only *required* key per the spec.
    shared = doc["Driver"]["shared"]
    assert isinstance(shared, str)
    assert os.path.isabs(shared), shared
    assert os.path.isfile(shared), shared
    assert shared == spanner_adbc._driver_path()

    assert doc["Driver"]["entrypoint"] == spanner_adbc.ENTRYPOINT
    assert doc["version"] == spanner_adbc.__version__
    assert doc["ADBC"]["version"] == "1.1.0"


@needs_library
def test_install_writes_a_manifest_named_for_the_driver(tmp_path):
    written = manifest_mod.install(tmp_path)
    # The base name is what the driver manager matches against the driver name
    # and against a connection URI's scheme, so it must be exactly this.
    assert written.name == "spanner.toml"
    assert written.parent == tmp_path
    assert tomllib.loads(written.read_text())["Driver"]["shared"]

    # Re-running overwrites rather than failing: that is the upgrade path.
    again = manifest_mod.install(tmp_path)
    assert again == written


def test_manifest_escapes_backslashes(tmp_path):
    """A Windows path must survive the round trip through TOML."""
    text = manifest_mod.manifest_text(r"C:\Program Files\ADBC\spanner_adbc.dll")
    shared = tomllib.loads(text)["Driver"]["shared"]
    assert shared.endswith(r"ADBC\spanner_adbc.dll")
    assert "\\\\" not in shared


def test_manifest_does_not_require_a_known_platform(tmp_path, monkeypatch):
    # A source-built library can work on architectures without published wheels.
    monkeypatch.setattr(manifest_mod.platform, "machine", lambda: "riscv64")
    library = tmp_path / "libspanner_adbc.so"
    written = manifest_mod.install(tmp_path, library=library)
    assert tomllib.loads(written.read_text())["Driver"]["shared"] == str(library.resolve())


@pytest.mark.parametrize("name", ['quote"', "tab\t", "line\n", "control\x01", "del\x7f", "snow☃𐐀"])
def test_manifest_paths_round_trip_through_toml(tmp_path, name):
    library = tmp_path / name / "libspanner_adbc.so"
    doc = tomllib.loads(manifest_mod.manifest_text(library))
    assert doc["Driver"]["shared"] == str(library.resolve())


def test_failed_install_preserves_existing_manifest(tmp_path, monkeypatch):
    from pathlib import Path

    target = tmp_path / manifest_mod.MANIFEST_NAME
    target.write_text("original manifest", encoding="utf-8")

    def fail_replace(self, target):
        raise PermissionError("manifest is read-only")

    monkeypatch.setattr(Path, "replace", fail_replace)
    with pytest.raises(PermissionError, match="read-only"):
        manifest_mod.install(tmp_path, library=tmp_path / "driver.so")
    assert target.read_text(encoding="utf-8") == "original manifest"
    assert list(tmp_path.iterdir()) == [target]


def test_install_preserves_manifest_permissions(tmp_path):
    if os.name == "nt":
        pytest.skip("POSIX permissions")
    target = manifest_mod.install(tmp_path, library=tmp_path / "driver.so")
    target.chmod(0o640)
    manifest_mod.install(tmp_path, library=tmp_path / "updated.so")
    assert target.stat().st_mode & 0o777 == 0o640
    assert tomllib.loads(target.read_text())["Driver"]["shared"].endswith("updated.so")


def test_new_manifest_respects_creation_permissions(tmp_path):
    reference = tmp_path / "reference.toml"
    reference.write_text("", encoding="utf-8")
    target = manifest_mod.install(tmp_path, library=tmp_path / "driver.so")
    assert target.stat().st_mode & 0o777 == reference.stat().st_mode & 0o777


@pytest.mark.parametrize("error", [RuntimeError("driver library missing"), PermissionError("access denied")])
def test_cli_reports_install_errors_without_traceback(monkeypatch, capsys, error):
    def fail_install(directory):
        raise error

    monkeypatch.setattr(manifest_mod, "install", fail_install)
    with pytest.raises(SystemExit) as exc:
        manifest_mod.main(["install"])
    assert exc.value.code == 1
    captured = capsys.readouterr()
    assert not captured.out
    assert str(error) in captured.err
    assert "Traceback" not in captured.err


def test_cli_custom_directory_explains_discovery(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(manifest_mod, "_driver_path", lambda: str(tmp_path / "driver.so"))
    assert manifest_mod.main(["install", "--dir", str(tmp_path)]) == 0
    assert "ADBC_DRIVER_PATH" in capsys.readouterr().out


def test_cli_path_and_print(capsys):
    assert manifest_mod.main(["path"]) == 0
    printed = capsys.readouterr().out.strip()
    assert printed.endswith(os.sep + "spanner.toml")

    if _has_library():
        assert manifest_mod.main(["print"]) == 0
        assert tomllib.loads(capsys.readouterr().out)["Driver"]["shared"]


def test_default_install_dir_is_a_searched_directory():
    """The default target must be a directory a driver manager actually searches.

    In a venv the Python driver manager adds ``<sys.prefix>/etc/adbc/drivers``;
    outside one, the user config directory is searched under the default load
    flags.
    """
    import sys

    directory = manifest_mod.default_install_dir()
    if sys.prefix != sys.base_prefix:
        assert directory == __import__("pathlib").Path(sys.prefix) / "etc/adbc/drivers"
    else:
        assert "adbc" in str(directory).lower()


@needs_library
def test_manifest_makes_the_driver_discoverable_by_name(
    emulator_database, tmp_path, monkeypatch
):
    """End-to-end: resolve the driver through a manifest, not a library path."""
    pytest.importorskip("pyarrow")  # dbapi result fetching needs it
    import adbc_driver_manager.dbapi

    manifest_mod.install(tmp_path)
    monkeypatch.setenv("ADBC_DRIVER_PATH", str(tmp_path))

    db_kwargs = {"spanner.emulator": "true"}
    uri = f"spanner:///{emulator_database}"

    # (1) By driver name.
    with adbc_driver_manager.dbapi.connect(
        driver="spanner", uri=uri, db_kwargs=db_kwargs, autocommit=True
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            assert cur.fetchone() == (1,)

    # (2) By URI scheme alone — no `driver` option at all. This is what the
    # manifest buys over the bundled-path `spanner_adbc.connect()`.
    with adbc_driver_manager.dbapi.connect(
        uri=uri, db_kwargs=db_kwargs, autocommit=True
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            assert cur.fetchone() == (1,)

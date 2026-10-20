"""Validate manifest rendering and driver discovery by name and URI scheme."""

import os
import tomllib
from pathlib import Path

import pytest

from conftest import database_options

import spanner_adbc
from spanner_adbc import manifest as manifest_mod


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


@needs_library
def test_manifest_parses_and_has_the_required_keys():
    doc = tomllib.loads(manifest_mod.manifest_text())

    assert doc["manifest_version"] == 1
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
    # The driver manager matches this name against driver="spanner" and the URI scheme.
    assert written.name == "spanner.toml"
    assert written.parent == tmp_path
    assert tomllib.loads(written.read_text())["Driver"]["shared"]

    again = manifest_mod.install(tmp_path)
    assert again == written


@pytest.mark.parametrize(
    "name", ["back\\slash", 'quote"', "tab\t", "line\n", "control\x01", "del\x7f", "snow☃𐐀"]
)
def test_manifest_paths_round_trip_through_toml(tmp_path, name):
    library = tmp_path / name / "libspanner_adbc.so"
    doc = tomllib.loads(manifest_mod.manifest_text(library))
    assert doc["Driver"]["shared"] == str(library.resolve())


def test_failed_install_preserves_existing_manifest(tmp_path, monkeypatch):
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


@pytest.mark.parametrize("platform", ["linux", "darwin", "win32"])
def test_virtualenv_manifest_stays_in_the_environment(tmp_path, monkeypatch, platform):
    monkeypatch.setattr(manifest_mod.sys, "prefix", str(tmp_path / "venv"))
    monkeypatch.setattr(manifest_mod.sys, "base_prefix", str(tmp_path / "base"))
    monkeypatch.setattr(manifest_mod.sys, "platform", platform)
    assert manifest_mod.default_install_dir() == tmp_path / "venv/etc/adbc/drivers"


@pytest.mark.parametrize(
    "platform,env,expected",
    [
        ("linux", {}, "home/.config/adbc/drivers"),
        ("linux", {"XDG_CONFIG_HOME": "config"}, "config/adbc/drivers"),
        ("darwin", {}, "home/Library/Application Support/ADBC/Drivers"),
        ("win32", {"LOCALAPPDATA": "local"}, "local/ADBC/Drivers"),
    ],
)
def test_user_manifest_uses_platform_config_directory(tmp_path, monkeypatch, platform, env, expected):
    monkeypatch.setattr(manifest_mod.sys, "prefix", manifest_mod.sys.base_prefix)
    monkeypatch.setattr(manifest_mod.sys, "platform", platform)
    monkeypatch.setattr(Path, "home", lambda: tmp_path / "home")
    for key in ("XDG_CONFIG_HOME", "LOCALAPPDATA"):
        monkeypatch.delenv(key, raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, str(tmp_path / value))
    assert manifest_mod.default_install_dir() == tmp_path / expected


@needs_library
def test_manifest_makes_the_driver_discoverable_by_name(
    spanner_database, tmp_path, monkeypatch
):
    pytest.importorskip("pyarrow")
    import adbc_driver_manager.dbapi

    manifest_mod.install(tmp_path)
    monkeypatch.setenv("ADBC_DRIVER_PATH", str(tmp_path))

    db_kwargs = database_options(spanner_database)
    uri = db_kwargs.pop("uri")

    with adbc_driver_manager.dbapi.connect(
        driver="spanner", uri=uri, db_kwargs=db_kwargs, autocommit=True
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            assert cur.fetchone() == (1,)

    # Without a driver option, the URI scheme selects the manifest.
    with adbc_driver_manager.dbapi.connect(
        uri=uri, db_kwargs=db_kwargs, autocommit=True
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            assert cur.fetchone() == (1,)

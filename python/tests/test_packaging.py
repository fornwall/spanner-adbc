"""Wheel builds require one staged native library, including when cross-packaging."""

import os
import runpy
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

setuptools = pytest.importorskip("setuptools")
from setuptools.command.bdist_wheel import bdist_wheel
from setuptools.errors import SetupError


@pytest.mark.parametrize(
    "libraries",
    [(), ("libspanner_adbc.so", "spanner_adbc.dll"), ("spanner_adbc.dll",)],
)
def test_wheel_requires_one_native_library(tmp_path, monkeypatch, libraries):
    setup = Path(__file__).resolve().parents[1] / "setup.py"
    (tmp_path / "setup.py").write_text(setup.read_text(), encoding="utf-8")
    package = tmp_path / "spanner_adbc"
    package.mkdir()
    for name in libraries:
        (package / name).write_bytes(b"staged driver")

    metadata = {}
    monkeypatch.setattr(setuptools, "setup", lambda **kwargs: metadata.update(kwargs))
    runpy.run_path(str(tmp_path / "setup.py"))
    built = []
    monkeypatch.setattr(bdist_wheel, "run", lambda self: built.append(True))
    command = metadata["cmdclass"]["bdist_wheel"](metadata["distclass"]())

    if len(libraries) == 1:
        command.run()
        assert built == [True]
    else:
        with pytest.raises(SetupError, match="exactly one native Spanner driver library"):
            command.run()
        assert not built


def test_repeated_wheel_builds_use_only_the_current_native_library(tmp_path):
    source = Path(__file__).resolve().parents[1]
    for name in ("setup.py", "pyproject.toml", "README.md", "LICENSE"):
        shutil.copyfile(source / name, tmp_path / name)
    package = tmp_path / "spanner_adbc"
    package.mkdir()
    shutil.copyfile(source / "spanner_adbc" / "_version.py", package / "_version.py")
    (package / "__init__.py").touch()
    (package / "py.typed").touch()
    (package / "libspanner_adbc_backup.so").write_bytes(b"unused library")

    previous = None
    for name, platform, contents in (
        ("libspanner_adbc.so", "linux_x86_64", b"first library"),
        ("libspanner_adbc.so", "linux_aarch64", b"replacement library"),
        ("spanner_adbc.dll", "win_amd64", b"Windows library"),
    ):
        if previous is not None:
            previous.unlink()
            build_package = next((tmp_path / "build").glob("lib*/spanner_adbc"))
            (build_package / "stale_driver.so").write_bytes(b"stale build output")
        library = package / name
        library.write_bytes(contents)
        os.utime(library, (946684800, 946684800))
        result = subprocess.run(
            [sys.executable, "setup.py", "bdist_wheel", "--plat-name", platform],
            cwd=tmp_path, capture_output=True, text=True,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        wheel = next((tmp_path / "dist").glob(f"*-py3-none-{platform}.whl"))
        with zipfile.ZipFile(wheel) as archive:
            libraries = [n for n in archive.namelist() if n.endswith((".so", ".dylib", ".dll"))]
            assert libraries == [f"spanner_adbc/{name}"]
            assert archive.read(libraries[0]) == contents
            assert "spanner_adbc/py.typed" in archive.namelist()
        previous = library

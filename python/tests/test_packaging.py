"""Wheel builds require one staged native library, including when cross-packaging."""

import runpy
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

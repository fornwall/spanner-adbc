"""Package a prebuilt driver as an ABI-independent, platform-specific wheel."""

from pathlib import Path

from setuptools import setup
from setuptools.command.bdist_wheel import bdist_wheel
from setuptools.dist import Distribution
from setuptools.errors import SetupError


class BinaryDistribution(Distribution):
    """Force a platform (non-pure) wheel even without an ext module."""

    def has_ext_modules(self) -> bool:
        return True


class WheelPy3None(bdist_wheel):
    """Emit a ``py3-none-<platform>`` tag (ABI-agnostic, platform-specific)."""

    def get_tag(self):
        _python, _abi, plat = super().get_tag()
        return "py3", "none", plat

    def run(self):
        package = Path(__file__).resolve().parent / "spanner_adbc"
        libraries = [
            name for name in ("libspanner_adbc.so", "libspanner_adbc.dylib", "spanner_adbc.dll")
            if (package / name).is_file()
        ]
        if len(libraries) != 1:
            raise SetupError(
                "A wheel requires exactly one native Spanner driver library in spanner_adbc/. "
                "Run `cargo build --release` from the repository root and copy the resulting "
                ".so, .dylib, or .dll from target/release/ into python/spanner_adbc/. "
                f"Found: {', '.join(libraries) or 'none'}."
            )
        super().run()


setup(distclass=BinaryDistribution, cmdclass={"bdist_wheel": WheelPy3None})

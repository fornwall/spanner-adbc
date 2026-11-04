"""Check that the dependency pins stay aligned, as CONTRIBUTING.md asks."""

from pathlib import Path
import re
import unittest


ROOT = Path(__file__).resolve().parents[2]


def manifest():
    return (ROOT / "Cargo.toml").read_text(encoding="utf-8")


def cargo_revisions(repository):
    pattern = rf'git = "https://github.com/{re.escape(repository)}", rev = "([0-9a-f]+)"'
    return set(re.findall(pattern, manifest()))


def adbc_versions():
    return re.findall(r'^adbc_(?:core|ffi|driver_manager) = "([0-9.]+)"', manifest(), re.MULTILINE)


class Pins(unittest.TestCase):
    def test_google_cloud_crates_use_one_revision(self):
        self.assertEqual(len(cargo_revisions("googleapis/google-cloud-rust")), 1)

    def test_adbc_crates_use_one_registry_release(self):
        versions = adbc_versions()
        self.assertEqual(len(versions), 3)
        self.assertEqual(len(set(versions)), 1)

    def test_cpp_validation_suite_uses_the_adbc_crate_release(self):
        cmake = (ROOT / "adbc-validation" / "CMakeLists.txt").read_text(encoding="utf-8")
        tag = re.search(r'set\(ARROW_ADBC_TAG "apache-arrow-adbc-([0-9]+)"', cmake)[1]
        self.assertEqual(adbc_versions()[0].split(".")[1], tag)


if __name__ == "__main__":
    unittest.main()

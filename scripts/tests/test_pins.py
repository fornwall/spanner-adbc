"""Check that each git-pinned dependency family stays on one revision, as CONTRIBUTING.md asks."""

from pathlib import Path
import re
import unittest


ROOT = Path(__file__).resolve().parents[2]


def cargo_revisions(repository):
    manifest = (ROOT / "Cargo.toml").read_text(encoding="utf-8")
    pattern = rf'git = "https://github.com/{re.escape(repository)}", rev = "([0-9a-f]+)"'
    return set(re.findall(pattern, manifest))


class Pins(unittest.TestCase):
    def test_each_family_uses_one_revision(self):
        for repository in (
            "apache/arrow-adbc",
            "googleapis/google-cloud-rust",
        ):
            with self.subTest(repository=repository):
                self.assertEqual(len(cargo_revisions(repository)), 1)

    def test_cpp_validation_suite_uses_the_adbc_crate_revision(self):
        cmake = (ROOT / "adbc-validation" / "CMakeLists.txt").read_text(encoding="utf-8")
        tag = re.search(r'set\(ARROW_ADBC_TAG "([0-9a-f]+)"', cmake)[1]
        self.assertEqual(cargo_revisions("apache/arrow-adbc"), {tag})


if __name__ == "__main__":
    unittest.main()

"""Check workflow wiring that actionlint cannot see."""

from pathlib import Path
import re
import unittest


ROOT = Path(__file__).resolve().parents[2]


class Workflows(unittest.TestCase):
    def test_every_rust_test_binary_runs_in_ci(self):
        # CI names each binary with --test because the target-dependent ones fail without a
        # backend under ADBC_TEST_REQUIRE_TARGET, so a new tests/*.rs would otherwise never run.
        workflows = "\n".join(
            path.read_text(encoding="utf-8")
            for path in (ROOT / ".github" / "workflows").glob("*.yml")
        )
        registered = set(re.findall(r"--test (\w+)", workflows))
        binaries = {path.stem for path in (ROOT / "tests").glob("*.rs")}
        self.assertEqual(
            binaries - registered,
            set(),
            "add these test binaries to .github/workflows/ci.yml",
        )


if __name__ == "__main__":
    unittest.main()

"""Check that incomplete validation runs cannot count as expected failures."""

from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


CHECKER = Path(__file__).resolve().parents[1] / "check-adbc-exclusions.py"


def testcase(name="Unsupported", result="completed", status="run", children="<failure/>"):
    return (
        f'<testcase name="{name}" result="{result}"\n'
        f' classname="Suite" status="{status}">{children}</testcase>'
    )


def report(*cases):
    return f"<testsuites><testsuite>{''.join(cases)}</testsuite></testsuites>"


class AdbcExclusions(unittest.TestCase):
    def check(self, xml, returncode=1, expected=("Suite.Unsupported",)):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "results.xml"
            if xml is not None:
                path.write_text(xml)
            return subprocess.run(
                [sys.executable, str(CHECKER), str(path), str(returncode), *expected],
                capture_output=True, text=True, timeout=10,
            )

    def test_complete_failures_and_skips(self):
        for cases, returncode, expected in (
            ([testcase()], 1, ("Suite.Unsupported",)),
            ([testcase(result="skipped", children="<skipped/>")], 0,
             ("Suite.Unsupported",)),
            ([testcase(), testcase("Skipped", result="skipped", children="<skipped/>")],
             1, ("Suite.Unsupported", "Suite.Skipped")),
        ):
            with self.subTest(cases=cases):
                result = self.check(report(*cases), returncode, expected)
                self.assertEqual(result.returncode, 0, result.stderr)

    def test_missing_or_malformed_reports(self):
        for xml in (None, "", "<testsuites><testsuite>", "<unexpected/>"):
            with self.subTest(xml=xml):
                self.assertNotEqual(self.check(xml).returncode, 0)

    def test_every_expected_test_must_appear_exactly_once(self):
        for cases in ([], [testcase("Different")], [testcase(), testcase()]):
            with self.subTest(cases=cases):
                result = self.check(report(*cases))
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("incomplete or unexpected test results", result.stderr)

    def test_unexpected_pass_requires_removing_exclusion(self):
        result = self.check(report(testcase(children="")), returncode=0)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Suite.Unsupported now passes; remove it from EXCLUDED", result.stderr)

    def test_unexecuted_and_inconsistent_cases_are_not_skips(self):
        for case in (
            testcase(result="suppressed", status="notrun", children=""),
            testcase(result="skipped", children=""),
            testcase(children="<skipped/>"),
            testcase(children="<failure/><skipped/>"),
        ):
            with self.subTest(case=case):
                self.assertNotEqual(self.check(report(case)).returncode, 0)

    def test_crash_is_not_an_expected_failure_even_with_complete_xml(self):
        result = self.check(report(testcase()), returncode=139)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("harness exited with status 139", result.stderr)

    def test_status_must_agree_with_results(self):
        for case, returncode in (
            (testcase(), 0),
            (testcase(result="skipped", children="<skipped/>"), 1),
        ):
            with self.subTest(case=case):
                result = self.check(report(case), returncode)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("disagrees with its XML report", result.stderr)


if __name__ == "__main__":
    unittest.main()

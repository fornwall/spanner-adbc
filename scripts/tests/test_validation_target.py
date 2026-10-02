"""Validate test-target setup without starting Docker or contacting Spanner."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


HELPER = Path(__file__).resolve().parents[1] / "validation-target.sh"


@unittest.skipUnless(shutil.which("bash"), "requires Bash")
class ValidationTarget(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.log = self.root / "requests.jsonl"
        self.env = dict(os.environ, PATH=f"{self.root}{os.pathsep}{os.environ['PATH']}")
        self.env.update(SPANNER_EMULATOR_HOST="localhost:9010", REQUEST_LOG=str(self.log))
        for key in ("POST_CODE", "CURL_EXIT", "SPANNER_EMULATOR_REST_PORT", "SPANNER_GCP_DATABASE"):
            self.env.pop(key, None)
        curl = self.root / "curl"
        curl.write_text(f"#!{sys.executable}\n" + '''
import json, os, pathlib, sys
args = sys.argv[1:]
log = pathlib.Path(os.environ["REQUEST_LOG"])
previous = log.read_text().count('"curl"') if log.exists() else 0
with log.open("a") as stream:
    stream.write(json.dumps(["curl", *args]) + "\\n")
if os.environ.get("CURL_EXIT"):
    sys.exit(int(os.environ["CURL_EXIT"]))
if "POST" in args:
    print('{"message":"setup response"}')
    print(os.environ.get("POST_CODE", "200"), end="")
else:
    print("404" if previous == 2 else "200", end="")
''')
        curl.chmod(0o755)

    def run_helper(self):
        return subprocess.run(
            ["bash", "-eu", "-c", 'source "$1"; printf "%s\\n" "$ADBC_SPANNER_URI"', "bash", str(HELPER)],
            env=self.env, capture_output=True, text=True, timeout=5,
        )

    def test_success_and_existing_resources_wait_for_exact_database(self):
        for code in ("200", "201", "409"):
            with self.subTest(code=code):
                self.log.unlink(missing_ok=True)
                self.env["POST_CODE"] = code
                result = self.run_helper()
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn("spanner:///projects/test-project/instances/test-instance/databases/adbc-test", result.stdout)
                calls = [json.loads(line) for line in self.log.read_text().splitlines()]
                self.assertEqual(len(calls), 4)  # Two creates, one pending GET, one ready GET.
                for call in calls:
                    self.assertIn("--max-time", call)
                    self.assertEqual(call[call.index("--noproxy") + 1], "*")
                self.assertTrue(calls[-1][-1].endswith("/databases/adbc-test"))

    def test_http_creation_error_reports_response_and_stops(self):
        self.env["POST_CODE"] = "403"
        result = self.run_helper()
        self.assertEqual(result.returncode, 1)
        self.assertIn("HTTP 403", result.stderr)
        self.assertIn("setup response", result.stderr)
        self.assertEqual(len(self.log.read_text().splitlines()), 1)

    def test_transport_error_stops_setup(self):
        self.env["CURL_EXIT"] = "28"
        result = self.run_helper()
        self.assertEqual(result.returncode, 1)
        self.assertIn("failed to reach emulator admin API", result.stderr)

    def test_real_target_does_not_contact_emulator(self):
        self.env.pop("SPANNER_EMULATOR_HOST")
        self.env["SPANNER_GCP_DATABASE"] = "my-project.my-instance.my-db"
        result = self.run_helper()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "spanner:///projects/my-project/instances/my-instance/databases/my-db")
        self.assertFalse(self.log.exists())

    def test_malformed_real_targets_fail_clearly(self):
        self.env.pop("SPANNER_EMULATOR_HOST")
        for target in ("", "project.instance", "project..database", "p.i.d.extra", "p.i/d.db", "p.i.d b"):
            with self.subTest(target=target):
                self.env["SPANNER_GCP_DATABASE"] = target
                result = self.run_helper()
                self.assertEqual(result.returncode, 2)
                self.assertIn("must be project.instance.database", result.stderr)
        self.assertFalse(self.log.exists())


if __name__ == "__main__":
    unittest.main()

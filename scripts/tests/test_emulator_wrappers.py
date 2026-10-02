"""Exercise wrapper ownership and failure cleanup without Docker or a database."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


SCRIPTS = Path(__file__).resolve().parents[1]


@unittest.skipUnless(shutil.which("bash"), "requires Bash")
class EmulatorWrappers(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.log = self.root / "calls.jsonl"
        self.env = dict(os.environ, PATH=f"{self.root}{os.pathsep}{os.environ['PATH']}")
        self.env.update(WRAPPER_LOG=str(self.log), TMPDIR=str(self.root))
        self.env.pop("FAIL_CONTAINER", None)
        self.env.pop("FAIL_AFTER_CREATE", None)
        self.env.update(
            SPANNER_EMULATOR_CONTAINER="existing-emulator",
            TOXIPROXY_CONTAINER="existing-proxy",
        )
        self.tool("docker", '''
import json, os, pathlib, sys
args = sys.argv[1:]
with open(os.environ["WRAPPER_LOG"], "a") as log:
    log.write(json.dumps(["docker", *args]) + "\\n")
if args[0] == "run":
    name = args[args.index("--name") + 1]
    fail = name == os.environ.get("FAIL_CONTAINER")
    if not fail or os.environ.get("FAIL_AFTER_CREATE"):
        pathlib.Path(args[args.index("--cidfile") + 1]).write_text("owned-" + name)
    if fail:
        sys.exit(125)
elif args[0] == "inspect":
    print("172.17.0.2")
''')
        self.tool("curl", '''
import json, os, sys
with open(os.environ["WRAPPER_LOG"], "a") as log:
    log.write(json.dumps(["curl", *sys.argv[1:]]) + "\\n")
print("200", end="")
''')

    def tool(self, name, source):
        import sys

        executable = self.root / name
        executable.write_text(f"#!{sys.executable}\n{source}")
        executable.chmod(0o755)

    def run_wrapper(self, wrapper, exit_code=0):
        result = subprocess.run(
            ["bash", str(SCRIPTS / wrapper), "bash", "-c", f"exit {exit_code}"],
            env=self.env,
            capture_output=True,
            text=True,
            timeout=10,
        )
        self.calls = [json.loads(line) for line in self.log.read_text().splitlines()]
        # The temporary Docker config (including cidfiles) must always be removed.
        self.assertFalse(any(path.is_dir() for path in self.root.iterdir()))
        return result

    def removed_ids(self):
        return [call[-1] for call in self.calls if call[:2] == ["docker", "rm"]]

    def test_emulator_collision_preserves_existing_container(self):
        self.env["FAIL_CONTAINER"] = "existing-emulator"
        self.assertEqual(self.run_wrapper("with-emulator.sh").returncode, 125)
        self.assertEqual(self.removed_ids(), [])

    def test_proxy_emulator_collision_preserves_existing_container(self):
        self.env["FAIL_CONTAINER"] = "existing-emulator"
        self.assertEqual(self.run_wrapper("with-toxiproxy.sh").returncode, 125)
        self.assertEqual(self.removed_ids(), [])

    def test_proxy_collision_cleans_up_only_owned_emulator(self):
        self.env["FAIL_CONTAINER"] = "existing-proxy"
        self.assertEqual(self.run_wrapper("with-toxiproxy.sh").returncode, 125)
        self.assertEqual(self.removed_ids(), ["owned-existing-emulator"])

    def test_emulator_start_failure_cleans_up_created_container(self):
        self.env.update(FAIL_CONTAINER="existing-emulator", FAIL_AFTER_CREATE="1")
        self.assertEqual(self.run_wrapper("with-emulator.sh").returncode, 125)
        self.assertEqual(self.removed_ids(), ["owned-existing-emulator"])

    def test_proxy_start_failure_cleans_up_both_created_containers(self):
        self.env.update(FAIL_CONTAINER="existing-proxy", FAIL_AFTER_CREATE="1")
        self.assertEqual(self.run_wrapper("with-toxiproxy.sh").returncode, 125)
        self.assertEqual(
            self.removed_ids(), ["owned-existing-proxy", "owned-existing-emulator"]
        )

    def test_emulator_preserves_child_status_and_cleans_up(self):
        self.assertEqual(self.run_wrapper("with-emulator.sh", 23).returncode, 23)
        self.assertEqual(self.removed_ids(), ["owned-existing-emulator"])

    def test_proxy_preserves_child_status_and_cleans_up(self):
        self.assertEqual(self.run_wrapper("with-toxiproxy.sh", 23).returncode, 23)
        self.assertEqual(
            self.removed_ids(), ["owned-existing-proxy", "owned-existing-emulator"]
        )

    def test_http_requests_are_bounded_and_bypass_ambient_proxies(self):
        self.assertEqual(self.run_wrapper("with-toxiproxy.sh").returncode, 0)
        requests = [call for call in self.calls if call[0] == "curl"]
        self.assertGreaterEqual(len(requests), 3)
        for request in requests:
            self.assertIn("--max-time", request)
            self.assertEqual(request[request.index("--noproxy") + 1], "*")
        # A fresh proxy needs no DELETE request that could affect a different server.
        self.assertFalse(any("DELETE" in request for request in requests))

    def test_proxy_ports_are_reserved_by_docker_before_readiness_checks(self):
        self.assertEqual(self.run_wrapper("with-toxiproxy.sh").returncode, 0)
        proxy = next(
            call for call in self.calls
            if call[:2] == ["docker", "run"] and "existing-proxy" in call
        )
        self.assertNotIn("--network", proxy)
        self.assertIn("127.0.0.1:8475:8475", proxy)
        self.assertIn("127.0.0.1:8666:8666", proxy)


if __name__ == "__main__":
    unittest.main()

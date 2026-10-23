"""Exercise wrapper ownership and failure cleanup without Docker or a database."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


SCRIPTS = Path(__file__).resolve().parents[1]


@unittest.skipUnless(shutil.which("bash"), "requires Bash")
class BackendWrappers(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.log = self.root / "calls.jsonl"
        self.env = dict(os.environ, PATH=f"{self.root}{os.pathsep}{os.environ['PATH']}")
        self.env.update(WRAPPER_LOG=str(self.log), TMPDIR=str(self.root))
        for key in ("FAIL_CONTAINER", "FAIL_AFTER_CREATE", "DOCKER_HOST", "DOCKER_CONTEXT"):
            self.env.pop(key, None)
        self.env.update(
            SPANNER_EMULATOR_CONTAINER="existing-emulator",
            SPANNER_EMULATOR_GRPC_PORT="9010",
            SPANNER_EMULATOR_REST_PORT="9020",
            SPANNER_OMNI_CONTAINER="existing-omni",
            TOXIPROXY_CONTAINER="existing-proxy",
        )
        self.tool("docker", '''
import json, os, pathlib, sys
args = sys.argv[1:]
with open(os.environ["WRAPPER_LOG"], "a") as log:
    log.write(json.dumps(["docker", *args]) + "\\n")
if args[:2] == ["context", "inspect"]:
    print(os.environ.get("FAKE_CONTEXT_HOST", ""))
    sys.exit(0)
if os.environ.get("EXPECT_DOCKER_HOST", os.environ.get("DOCKER_HOST")) != os.environ.get("DOCKER_HOST"):
    sys.exit(99)
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
        executable = self.root / name
        executable.write_text(f"#!{sys.executable}\n{source}")
        executable.chmod(0o755)

    def run_wrapper(self, wrapper, exit_code=0, command=None):
        result = subprocess.run(
            ["bash", str(SCRIPTS / wrapper), *(command or ["bash", "-c", f"exit {exit_code}"])],
            env=self.env,
            capture_output=True,
            text=True,
            timeout=10,
        )
        self.calls = (
            [json.loads(line) for line in self.log.read_text().splitlines()]
            if self.log.exists() else []
        )
        # The temporary Docker config (including cidfiles) must always be removed.
        self.assertFalse(any(path.is_dir() for path in self.root.iterdir()))
        return result

    def removed_ids(self):
        return [call[-1] for call in self.calls if call[:2] == ["docker", "rm"]]

    def test_emulator_collision_preserves_existing_container(self):
        self.env["FAIL_CONTAINER"] = "existing-emulator"
        self.assertEqual(self.run_wrapper("with-emulator.sh").returncode, 125)
        self.assertEqual(self.removed_ids(), [])

    def test_omni_collision_preserves_existing_container(self):
        self.env["FAIL_CONTAINER"] = "existing-omni"
        self.assertEqual(self.run_wrapper("with-omni.sh").returncode, 125)
        self.assertEqual(self.removed_ids(), [])

    def test_omni_start_failure_cleans_up_created_container_and_volume(self):
        self.env.update(FAIL_CONTAINER="existing-omni", FAIL_AFTER_CREATE="1")
        self.assertEqual(self.run_wrapper("with-omni.sh").returncode, 125)
        self.assertIn(["docker", "rm", "-fv", "owned-existing-omni"], self.calls)

    def test_omni_preserves_child_status_and_cleans_up(self):
        self.assertEqual(self.run_wrapper("with-omni.sh", 23).returncode, 23)
        self.assertEqual(self.removed_ids(), ["owned-existing-omni"])
        start = next(call for call in self.calls if call[:2] == ["docker", "run"])
        self.assertIn("--listen-addresses=0.0.0.0", start)
        self.assertIn("127.0.0.1:15000-15025:15000-15025", start)
        commands = [call for call in self.calls if call[:2] == ["docker", "exec"]]
        self.assertEqual(len(commands), 2)
        self.assertIn("list", commands[0])
        self.assertIn("adbc-test", commands[1])

    def test_emulator_rejects_unusable_port_mapping_before_starting_docker(self):
        for variable, port in (
            ("SPANNER_EMULATOR_GRPC_PORT", "19010"),
            ("SPANNER_EMULATOR_REST_PORT", "19020"),
        ):
            with self.subTest(variable=variable):
                previous = self.env[variable]
                self.env[variable] = port
                result = self.run_wrapper("with-emulator.sh")
                self.env[variable] = previous
                self.assertEqual(result.returncode, 2)
                self.assertIn("requires emulator ports 9010", result.stderr)
                self.assertIn("unset", result.stderr)
                self.assertEqual(self.calls, [])

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

    def test_wrappers_override_ambient_backend_selection(self):
        for wrapper, target in (
            ("with-emulator.sh", {"SPANNER_EMULATOR_HOST": "localhost:9010"}),
            ("with-toxiproxy.sh", {"SPANNER_EMULATOR_HOST": "127.0.0.1:8666"}),
            ("with-omni.sh", {"SPANNER_OMNI_ENDPOINT": "http://localhost:15000"}),
        ):
            with self.subTest(wrapper=wrapper):
                self.env.update(
                    SPANNER_EMULATOR_HOST="old-emulator:9010",
                    SPANNER_OMNI_ENDPOINT="http://old-omni:15000",
                    SPANNER_GCP_DATABASE="old.project.database",
                )
                result = self.run_wrapper(wrapper, command=["env"])
                self.assertEqual(result.returncode, 0, result.stderr)
                actual = dict(
                    line.split("=", 1) for line in result.stdout.splitlines()
                    if line.startswith(("SPANNER_EMULATOR_HOST=", "SPANNER_OMNI_ENDPOINT=", "SPANNER_GCP_DATABASE="))
                )
                self.assertEqual(actual, target)

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

    def test_wrappers_keep_the_active_docker_context_endpoint(self):
        # The temporary DOCKER_CONFIG has no contexts, so pass the endpoint through DOCKER_HOST.
        self.env.update(
            DOCKER_CONTEXT="colima",
            FAKE_CONTEXT_HOST="unix:///colima.sock",
            EXPECT_DOCKER_HOST="unix:///colima.sock",
        )
        for wrapper in ("with-emulator.sh", "with-omni.sh", "with-toxiproxy.sh"):
            with self.subTest(wrapper=wrapper):
                result = self.run_wrapper(wrapper, command=["env"])
                self.assertEqual(result.returncode, 0, result.stderr)
                lines = result.stdout.splitlines()
                self.assertIn("DOCKER_HOST=unix:///colima.sock", lines)
                self.assertFalse(any(line.startswith("DOCKER_CONTEXT=") for line in lines))

    def test_wrappers_keep_an_explicit_docker_host(self):
        self.env.update(
            DOCKER_HOST="tcp://docker.example:2375",
            FAKE_CONTEXT_HOST="unix:///ignored.sock",
            EXPECT_DOCKER_HOST="tcp://docker.example:2375",
        )
        result = self.run_wrapper("with-emulator.sh", command=["env"])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("DOCKER_HOST=tcp://docker.example:2375", result.stdout.splitlines())

    def run_runner(self, script, **env):
        """Run a validation runner from outside the repository via a relative path."""
        self.tool("cargo", '''
import json, os, sys
with open(os.environ["WRAPPER_LOG"], "a") as log:
    log.write(json.dumps(["cargo", *sys.argv[1:]]) + "\\n")
sys.exit(int(os.environ.get("CARGO_EXIT", "0")))
''')
        self.tool("rustc", 'print("host: x86_64-unknown-linux-gnu")')
        for key in ("SPANNER_EMULATOR_HOST", "SPANNER_OMNI_ENDPOINT", "SPANNER_GCP_DATABASE"):
            self.env.pop(key, None)
        self.env.update(env)
        result = subprocess.run(
            ["bash", os.path.relpath(SCRIPTS / script, self.root)],
            cwd=self.root, env=self.env, capture_output=True, text=True, timeout=10,
        )
        self.calls = [json.loads(line) for line in self.log.read_text().splitlines()]
        return result

    def test_adbc_runner_reenters_under_the_emulator_from_any_directory(self):
        # The fake cargo stops the re-entered run at its first build.
        result = self.run_runner("run-adbc-validation.sh", CARGO_EXIT="3")
        self.assertEqual(result.returncode, 3, result.stderr)
        self.assertIn(["cargo", "build"], self.calls)
        self.assertEqual(self.removed_ids(), ["owned-existing-emulator"])

    def test_foundry_runner_reenters_with_a_relative_python(self):
        self.tool("fake-python", '''
import json, os, sys
if sys.argv[1:4] == ["-m", "pip", "freeze"]:
    print("adbc-drivers-validation @ git+https://github.com/adbc-drivers/validation@"
          "9f9f4417a08d6c082193842bc5caaa36e65908a0")
    sys.exit(0)
with open(os.environ["WRAPPER_LOG"], "a") as log:
    log.write(json.dumps(["python", os.getcwd(), *sys.argv[1:]]) + "\\n")
''')
        result = self.run_runner("run-foundry-validation.sh", PYTHON="./fake-python")
        self.assertEqual(result.returncode, 0, result.stderr)
        pytest = [call for call in self.calls if call[0] == "python"]
        self.assertEqual(len(pytest), 1)
        self.assertEqual(pytest[0][1], str(SCRIPTS.parent / "foundry-validation"))
        self.assertEqual(pytest[0][2:4], ["-m", "pytest"])
        self.assertEqual(self.removed_ids(), ["owned-existing-emulator"])

    def test_foundry_runner_reports_a_missing_pip(self):
        self.tool("fake-python", '''
import sys
print(f"{sys.argv[0]}: No module named pip", file=sys.stderr)
sys.exit(1)
''')
        result = self.run_runner("run-foundry-validation.sh", PYTHON="./fake-python")
        self.assertEqual(result.returncode, 1)
        self.assertIn("No module named pip", result.stderr)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "run.sh"

#: The directories the harness is allowed to find its tools in, in PATH order.
TOOL_DIRECTORIES = ("/usr/bin", "/bin")


class RunOptionsTest(unittest.TestCase):
    """Option handling, driven to the point where run.sh demands docker.

    These tests use "required command not found: docker" as the sentinel for
    "no earlier validation fired", so docker has to be genuinely absent from
    the PATH they hand the script. Pinning that PATH to /usr/bin:/bin hid
    docker only where docker is installed elsewhere -- true for Docker Desktop
    on macOS, which lands in /usr/local/bin, and false on Linux, where docker
    IS /usr/bin/docker. There the sentinel never fired, the script sailed past
    the check, and the "unit" tests ran a real benchmark lane.

    So the PATH is built instead: every tool those directories carry, minus
    docker. Same command set as before on every platform, docker excluded by
    construction rather than by where it happens to be installed.
    """

    @classmethod
    def setUpClass(cls) -> None:
        cls._path_directory = tempfile.TemporaryDirectory()
        mirror = Path(cls._path_directory.name)
        linked: set[str] = set()
        for source in TOOL_DIRECTORIES:
            directory = Path(source)
            if not directory.is_dir():
                continue
            for entry in sorted(directory.iterdir()):
                if entry.name == "docker" or entry.name in linked:
                    continue
                linked.add(entry.name)
                os.symlink(entry, mirror / entry.name)
        cls.docker_free_path = str(mirror)

    @classmethod
    def tearDownClass(cls) -> None:
        cls._path_directory.cleanup()

    def run_before_docker(self, *arguments: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["/bin/bash", str(SCRIPT), *arguments],
            check=False,
            capture_output=True,
            text=True,
            env={"PATH": self.docker_free_path},
        )

    def test_publishable_rejects_no_build_before_host_inspection(self) -> None:
        environment = {
            "PATH": self.docker_free_path,
        }
        result = subprocess.run(
            [
                "/bin/bash",
                str(SCRIPT),
                "--qualification",
                "publishable",
                "--no-build",
            ],
            check=False,
            capture_output=True,
            text=True,
            env=environment,
        )

        self.assertNotEqual(0, result.returncode)
        self.assertIn(
            "--qualification publishable cannot be combined with --no-build",
            result.stderr,
        )
        self.assertNotIn("Docker daemon", result.stderr)

    def test_renewal_profile_does_not_require_the_entire_prefetch_tail_in_one_lease(self) -> None:
        result = self.run_before_docker(
            "--queen-prefetch",
            "4",
            "--worker-timeout",
            "120",
            "--retry-after",
            "180",
        )

        self.assertNotEqual(0, result.returncode)
        self.assertIn("required command not found: docker", result.stderr)
        self.assertNotIn("prefetch multiplied", result.stderr)

    def test_retry_after_must_still_exceed_worker_timeout_with_renewal(self) -> None:
        result = self.run_before_docker(
            "--queen-prefetch",
            "4",
            "--worker-timeout",
            "120",
            "--retry-after",
            "120",
        )

        self.assertNotEqual(0, result.returncode)
        self.assertIn(
            "--retry-after must exceed --worker-timeout when lease renewal is enabled",
            result.stderr,
        )
        self.assertNotIn("Docker daemon", result.stderr)

    def test_publishable_horizon_requires_strict_aof_durability(self) -> None:
        result = self.run_before_docker("--qualification", "publishable")

        self.assertNotEqual(0, result.returncode)
        self.assertIn(
            "publishable requires Redis AOF yes with --redis-appendfsync always",
            result.stderr,
        )
        self.assertNotIn("Docker daemon", result.stderr)

    def test_strict_aof_setting_passes_the_durability_gate(self) -> None:
        result = self.run_before_docker(
            "--qualification",
            "publishable",
            "--redis-appendfsync",
            "always",
        )

        self.assertNotEqual(0, result.returncode)
        self.assertTrue(
            "required command not found: docker" in result.stderr
            or "requires a clean Git worktree" in result.stderr,
            result.stderr,
        )
        self.assertNotIn("Redis AOF", result.stderr)

    def test_recorded_redis_durability_is_exported_to_compose(self) -> None:
        source = SCRIPT.read_text(encoding="utf-8")

        self.assertIn('export BENCH_REDIS_APPENDONLY="$REDIS_APPENDONLY"', source)
        self.assertIn('export BENCH_REDIS_APPEND_FSYNC="$REDIS_APPEND_FSYNC"', source)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "run.sh"


class RunOptionsTest(unittest.TestCase):
    def run_before_docker(self, *arguments: str) -> subprocess.CompletedProcess[str]:
        # Do not assume where a host installs Docker. GitHub runners expose it
        # from /usr/bin, while macOS normally does not. A controlled client
        # makes this an offline preflight test on both hosts.
        with tempfile.TemporaryDirectory() as directory:
            docker = Path(directory) / "docker"
            docker.write_text("#!/bin/sh\nexit 1\n", encoding="utf-8")
            docker.chmod(0o755)

            return subprocess.run(
                ["/bin/bash", str(SCRIPT), *arguments],
                check=False,
                capture_output=True,
                text=True,
                env={**os.environ, "PATH": f"{directory}:{os.defpath}"},
            )

    def test_publishable_rejects_no_build_before_host_inspection(self) -> None:
        environment = {
            "PATH": "/usr/bin:/bin",
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
        self.assertIn("Docker daemon is unavailable", result.stderr)
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
            "Docker daemon is unavailable" in result.stderr
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

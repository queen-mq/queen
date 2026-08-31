from __future__ import annotations

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "infrastructure-faults.sh"


class InfrastructureFaultHarnessTest(unittest.TestCase):
    def run_script(
        self,
        *arguments: str,
        environment: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [str(SCRIPT), *arguments],
            check=False,
            capture_output=True,
            text=True,
            env=environment,
        )

    def test_default_dry_run_has_the_exact_compatibility_matrix(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "artifacts"
            result = self.run_script("--output", str(output), "--dry-run")
            self.assertEqual(0, result.returncode, result.stderr)
            plan = json.loads((output / "plan.json").read_text(encoding="utf-8"))
            metadata = json.loads(
                (output / "metadata.json").read_text(encoding="utf-8")
            )
            manifest = json.loads(
                (output / "artifact-manifest.json").read_text(encoding="utf-8")
            )

        pairs = {(lane["engine"], lane["scenario"]) for lane in plan["lanes"]}
        self.assertEqual(
            {
                ("horizon", "redis-restart"),
                ("horizon", "app-backend-network-partition"),
                ("queen-php", "broker-restart"),
                ("queen-rust", "broker-restart"),
                ("queen-php", "postgres-restart"),
                ("queen-rust", "postgres-restart"),
                ("queen-php", "app-backend-network-partition"),
                ("queen-rust", "app-backend-network-partition"),
                ("queen-php", "broker-postgres-network-partition"),
                ("queen-rust", "broker-postgres-network-partition"),
                ("horizon", "master-sigkill"),
                ("queen-php", "master-sigkill"),
                ("queen-rust", "master-sigkill"),
            },
            pairs,
        )
        self.assertTrue(metadata["settings"]["dry_run"])
        self.assertFalse(metadata["performance_comparable"])
        self.assertEqual("always", metadata["settings"]["redis_appendfsync"])
        self.assertFalse(
            metadata["separate_gates"]["disk_full"]["implemented"]
        )
        self.assertEqual(
            "queen.laravel-supervisors.artifact-manifest/v1", manifest["schema"]
        )
        self.assertEqual(
            {"metadata.json", "plan.json"},
            {entry["path"] for entry in manifest["files"]},
        )

    def test_dry_run_never_invokes_docker(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            binary_directory = root / "bin"
            binary_directory.mkdir()
            marker = root / "docker-was-called"
            docker = binary_directory / "docker"
            docker.write_text(
                "#!/bin/sh\nprintf called >\"$QUEEN_DOCKER_MARKER\"\nexit 97\n",
                encoding="utf-8",
            )
            docker.chmod(0o755)
            environment = os.environ.copy()
            environment["PATH"] = f"{binary_directory}{os.pathsep}{environment['PATH']}"
            environment["QUEEN_DOCKER_MARKER"] = str(marker)
            output = root / "artifacts"

            result = self.run_script(
                "--output", str(output), "--dry-run", environment=environment
            )

            self.assertEqual(0, result.returncode, result.stderr)
            self.assertFalse(marker.exists())

    def test_filtering_records_applicable_and_excluded_pairs(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "artifacts"
            result = self.run_script(
                "--output",
                str(output),
                "--engines",
                "horizon,queen-rust",
                "--scenarios",
                "redis-restart,broker-restart,app-backend-network-partition,master-sigkill",
                "--dry-run",
            )
            self.assertEqual(0, result.returncode, result.stderr)
            plan = json.loads((output / "plan.json").read_text(encoding="utf-8"))

        pairs = [(lane["engine"], lane["scenario"]) for lane in plan["lanes"]]
        self.assertEqual(
            [
                ("horizon", "redis-restart"),
                ("queen-rust", "broker-restart"),
                ("horizon", "app-backend-network-partition"),
                ("queen-rust", "app-backend-network-partition"),
                ("horizon", "master-sigkill"),
                ("queen-rust", "master-sigkill"),
            ],
            pairs,
        )
        excluded = {
            (item["engine"], item["scenario"]) for item in plan["excluded_pairs"]
        }
        self.assertIn(("horizon", "broker-restart"), excluded)
        self.assertIn(("queen-rust", "redis-restart"), excluded)

    def test_inapplicable_selection_fails_before_docker(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "artifacts"
            result = self.run_script(
                "--output",
                str(output),
                "--engines",
                "horizon",
                "--scenarios",
                "broker-restart",
                "--dry-run",
            )

        self.assertNotEqual(0, result.returncode)
        self.assertIn("does not apply to any selected engine", result.stderr)

    def test_disk_full_is_an_explicit_separate_gate(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            result = self.run_script(
                "--output",
                str(Path(temporary) / "artifacts"),
                "--scenarios",
                "disk-full",
                "--dry-run",
            )

        self.assertNotEqual(0, result.returncode)
        self.assertIn("not implemented", result.stderr)
        self.assertIn("disposable-storage qualification gate", result.stderr)

    def test_invalid_protocol_values_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            duplicate_engine = self.run_script(
                "--output",
                str(root / "duplicate"),
                "--engines",
                "horizon,horizon",
                "--dry-run",
            )
            whitespace = self.run_script(
                "--output",
                str(root / "whitespace"),
                "--scenarios",
                "redis-restart, broker-restart",
                "--dry-run",
            )
            insufficient_backlog = self.run_script(
                "--output",
                str(root / "backlog"),
                "--jobs",
                "15",
                "--workers",
                "2",
                "--dry-run",
            )

        self.assertNotEqual(0, duplicate_engine.returncode)
        self.assertIn("duplicate value", duplicate_engine.stderr)
        self.assertNotEqual(0, whitespace.returncode)
        self.assertIn("invalid value", whitespace.stderr)
        self.assertNotEqual(0, insufficient_backlog.returncode)
        self.assertIn("preserve a measured backlog", insufficient_backlog.stderr)

    def test_nonempty_output_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "artifacts"
            output.mkdir()
            (output / "existing.txt").write_text("retain\n", encoding="utf-8")
            result = self.run_script("--output", str(output), "--dry-run")

        self.assertNotEqual(0, result.returncode)
        self.assertIn("--output must be empty", result.stderr)

    def test_source_contains_the_fail_closed_fault_and_evidence_contract(self) -> None:
        source = SCRIPT.read_text(encoding="utf-8")
        required_fragments = (
            "BENCH_LEDGER_MODE=durable",
            "BENCH_REDIS_APPEND_FSYNC=always",
            "refusing to reuse pre-existing volume",
            "refusing to reuse pre-existing network",
            "verify_compose_target",
            "docker_bounded kill --signal KILL",
            "docker_bounded network disconnect",
            "docker_bounded network connect",
            "broker-postgres",
            "old_process_tree_gone_before_restart",
            "bench:ledger-checkpoint",
            "bench:queue-state",
            "--allow-retried-executions",
            "artifact-manifest.py",
            '"at_least_once"',
            '"idempotent_effect"',
            '"strict_execution"',
            '"exact_lane_coverage"',
        )
        for fragment in required_fragments:
            with self.subTest(fragment=fragment):
                self.assertIn(fragment, source)

    def test_script_passes_bash_syntax_validation(self) -> None:
        result = subprocess.run(
            ["bash", "-n", str(SCRIPT)],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(0, result.returncode, result.stderr)


if __name__ == "__main__":
    unittest.main()

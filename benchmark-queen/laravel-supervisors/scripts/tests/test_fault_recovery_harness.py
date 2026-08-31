from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "fault-recovery.sh"


class FaultRecoveryHarnessTest(unittest.TestCase):
    def run_script(self, *arguments: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [str(SCRIPT), *arguments],
            check=False,
            capture_output=True,
            text=True,
        )

    def test_dry_run_records_the_sterilized_reliability_environment(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "artifacts"
            result = subprocess.run(
                [str(SCRIPT), "--output", str(output), "--dry-run"],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(0, result.returncode, result.stderr)
            document = json.loads(
                (output / "metadata.json").read_text(encoding="utf-8")
            )

        settings = document["settings"]
        self.assertEqual(["benchmark"], settings["queues"])
        self.assertEqual("", settings["bench_queues_csv"])
        self.assertEqual("null", settings["failed_driver"])
        self.assertFalse(settings["lease_renewal"])
        self.assertEqual(
            {
                "BENCH_QUEUES": "",
                "BENCH_FAILED_DRIVER": "null",
                "BENCH_LEASE_RENEWAL": "false",
                "BENCH_LEASE_RENEWAL_INTERVAL": "",
            },
            document["method"]["sterilized_environment"],
        )
        self.assertEqual("worker-sigkill", settings["fault_scenario"])

    def test_prefetch_enables_the_production_renewal_fence(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "artifacts"
            result = subprocess.run(
                [
                    str(SCRIPT),
                    "--output",
                    str(output),
                    "--queen-prefetch",
                    "4",
                    "--queen-ack-batch",
                    "1",
                    "--dry-run",
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(0, result.returncode, result.stderr)
            document = json.loads(
                (output / "metadata.json").read_text(encoding="utf-8")
            )

        self.assertTrue(document["settings"]["lease_renewal"])
        self.assertEqual(
            "true",
            document["method"]["sterilized_environment"][
                "BENCH_LEASE_RENEWAL"
            ],
        )

    def test_helper_fault_requires_renewal_and_queen_only_engines(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            no_renewal = self.run_script(
                "--output",
                str(root / "no-renewal"),
                "--scenario",
                "renewal-helper-sigkill",
                "--engines",
                "queen-rust",
                "--dry-run",
            )
            horizon = self.run_script(
                "--output",
                str(root / "horizon"),
                "--scenario",
                "renewal-helper-sigkill",
                "--queen-prefetch",
                "4",
                "--dry-run",
            )

        self.assertNotEqual(0, no_renewal.returncode)
        self.assertIn("prefetch greater than one", no_renewal.stderr)
        self.assertNotEqual(0, horizon.returncode)
        self.assertIn("does not apply to Horizon", horizon.stderr)

    def test_helper_fault_protocol_names_the_watchdog_fence(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "artifacts"
            result = self.run_script(
                "--output",
                str(output),
                "--scenario",
                "renewal-helper-sigkill",
                "--engines",
                "queen-php,queen-rust",
                "--queen-prefetch",
                "4",
                "--queen-ack-batch",
                "1",
                "--dry-run",
            )

            self.assertEqual(0, result.returncode, result.stderr)
            document = json.loads(
                (output / "metadata.json").read_text(encoding="utf-8")
            )

        self.assertEqual(
            "renewal-helper-sigkill", document["settings"]["fault_scenario"]
        )
        self.assertIn("watchdog fence", document["method"]["fault"])
        self.assertEqual(
            "LeaseRenewalWorker::main",
            document["method"]["renewal_helper_command"],
        )


if __name__ == "__main__":
    unittest.main()

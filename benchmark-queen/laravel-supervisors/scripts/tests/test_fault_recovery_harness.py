from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "fault-recovery.sh"


class FaultRecoveryHarnessTest(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()

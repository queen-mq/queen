import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "feature-parity.sh"


class FeatureParityHarnessTest(unittest.TestCase):
    def run_script(self, *arguments: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [str(SCRIPT), *arguments],
            check=False,
            capture_output=True,
            text=True,
        )

    def compare_worker_snapshots(
        self,
        baseline: Path,
        final: Path,
        output: Path,
        *,
        engine: str = "queen-rust",
        expected: int = 2,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                str(SCRIPT),
                str(baseline),
                str(final),
                engine,
                str(expected),
                str(output),
            ],
            check=False,
            capture_output=True,
            text=True,
            env={**os.environ, "QUEEN_FEATURE_PARITY_COMPARE_ONLY": "1"},
        )

    @staticmethod
    def write_worker_snapshot(
        path: Path,
        identities: list[tuple[int, int]],
        *,
        engine: str = "queen-rust",
    ) -> None:
        path.write_text(
            json.dumps(
                {
                    "schema": "queen.laravel-supervisors.worker-snapshot/v1",
                    "engine": engine,
                    "workers": [
                        {
                            "pid": pid,
                            "start_ticks": start_ticks,
                            "state": "S",
                            "role": "worker",
                            "command": "queue:work",
                        }
                        for pid, start_ticks in identities
                    ],
                    "orchestrators": [],
                }
            )
            + "\n",
            encoding="utf-8",
        )

    def test_help_is_explicitly_non_performance(self) -> None:
        result = self.run_script("--help")

        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn("not a throughput or resource benchmark", result.stdout)
        self.assertIn("failed row + broker DLQ", result.stdout)

    def test_dry_run_writes_a_sanitized_protocol_without_docker(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "artifacts"
            result = self.run_script(
                "--output",
                str(output),
                "--dry-run",
                "--engines",
                "horizon,queen-rust",
                "--queues",
                "critical,default",
                "--jobs-per-queue",
                "3",
                "--workers",
                "2",
            )

            self.assertEqual(0, result.returncode, result.stderr)
            document = json.loads(
                (output / "metadata.json").read_text(encoding="utf-8")
            )
            self.assertEqual("diagnostic_feature_smoke", document["qualification"])
            self.assertFalse(document["performance_comparable"])
            self.assertEqual(["horizon", "queen-rust"], document["settings"]["engines"])
            self.assertEqual(6, document["settings"]["total_jobs_per_lane"])
            self.assertEqual(
                "critical,default", document["settings"]["bench_queues_csv"]
            )
            self.assertEqual(
                {"horizon": "null", "queen-rust": "file"},
                document["settings"]["failed_driver_by_engine"],
            )
            self.assertFalse(document["settings"]["lease_renewal"])
            self.assertEqual(
                "false",
                document["method"]["sterilized_environment"][
                    "BENCH_LEASE_RENEWAL"
                ],
            )
            self.assertEqual(
                ["queen-rust"], document["scenarios"]["failed_job_lifecycle"]
            )
            self.assertIn("never captured", document["artifact_policy"]["secrets"])

    def test_invalid_or_ambiguous_scope_fails_before_docker(self) -> None:
        cases = [
            ("--queues", "critical, default"),
            ("--queues", "critical,critical"),
            ("--engines", "horizon,horizon"),
        ]
        for option, value in cases:
            with (
                self.subTest(option=option, value=value),
                tempfile.TemporaryDirectory() as temporary,
            ):
                result = self.run_script(
                    "--output",
                    str(Path(temporary) / "artifacts"),
                    "--dry-run",
                    option,
                    value,
                )

                self.assertNotEqual(0, result.returncode)
                self.assertIn("error:", result.stderr)

    def test_queue_length_preserves_the_128_byte_job_id_limit(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            valid = "q" * 118
            accepted = self.run_script(
                "--output",
                str(Path(temporary) / "accepted"),
                "--dry-run",
                "--queues",
                f"{valid},default",
            )
            rejected = self.run_script(
                "--output",
                str(Path(temporary) / "rejected"),
                "--dry-run",
                "--queues",
                f"{valid}q,default",
            )

        self.assertEqual(0, accepted.returncode, accepted.stderr)
        self.assertNotEqual(0, rejected.returncode)
        self.assertIn("1..118", rejected.stderr)

    def test_output_directory_must_be_empty(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "artifacts"
            output.mkdir()
            (output / "keep.txt").write_text("do not overwrite\n", encoding="utf-8")

            result = self.run_script("--output", str(output), "--dry-run")

            self.assertNotEqual(0, result.returncode)
            self.assertIn("--output must be empty", result.stderr)
            self.assertEqual(
                "do not overwrite\n", (output / "keep.txt").read_text(encoding="utf-8")
            )

    def test_worker_integrity_accepts_the_same_process_identities(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            baseline = root / "baseline.json"
            final = root / "final.json"
            output = root / "result.json"
            identities = [(21, 1001), (22, 1002)]
            self.write_worker_snapshot(baseline, identities)
            self.write_worker_snapshot(final, list(reversed(identities)))

            result = self.compare_worker_snapshots(baseline, final, output)

            self.assertEqual(0, result.returncode, result.stderr)
            document = json.loads(output.read_text(encoding="utf-8"))
            self.assertTrue(document["passed"])
            self.assertTrue(document["stable_identities"])
            self.assertFalse(document["respawn_or_replacement_detected"])

    def test_worker_integrity_detects_pid_reuse_or_respawn(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            baseline = root / "baseline.json"
            final = root / "final.json"
            output = root / "result.json"
            self.write_worker_snapshot(baseline, [(21, 1001), (22, 1002)])
            # A PID alone is insufficient: Linux can reuse 22 for a new
            # process, while field 22 of /proc/<pid>/stat changes.
            self.write_worker_snapshot(final, [(21, 1001), (22, 2002)])

            result = self.compare_worker_snapshots(baseline, final, output)

            self.assertNotEqual(0, result.returncode)
            document = json.loads(output.read_text(encoding="utf-8"))
            self.assertFalse(document["passed"])
            self.assertFalse(document["stable_identities"])
            self.assertTrue(document["respawn_or_replacement_detected"])

    def test_worker_integrity_fails_closed_on_worker_loss(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            baseline = root / "baseline.json"
            final = root / "final.json"
            output = root / "result.json"
            self.write_worker_snapshot(baseline, [(21, 1001), (22, 1002)])
            self.write_worker_snapshot(final, [(21, 1001)])

            result = self.compare_worker_snapshots(baseline, final, output)

            self.assertNotEqual(0, result.returncode)
            document = json.loads(output.read_text(encoding="utf-8"))
            self.assertFalse(document["passed"])
            self.assertEqual(1, document["final_count"])
            self.assertTrue(any("expected 2" in error for error in document["errors"]))


if __name__ == "__main__":
    unittest.main()

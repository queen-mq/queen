from __future__ import annotations

import argparse
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "analyze.py"
SPEC = importlib.util.spec_from_file_location("laravel_supervisor_analyze", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
analyze = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = analyze
SPEC.loader.exec_module(analyze)


class QueueStateGateTest(unittest.TestCase):
    def make_run(self, root: Path, queue_size: int = 0, include_queue_state: bool = True) -> Path:
        run = root / "run"
        (run / "events").mkdir(parents=True)
        manifest = {
            "run_id": "fixture-run",
            "jobs": 1,
            "connection": "redis",
            "queue": "benchmark",
            "sleep_ms": 0,
            "cpu_iterations": 0,
            "dispatch_started_ns": 1_000_000_000,
            "dispatch_finished_ns": 1_000_000_100,
            "dispatch_duration_ns": 100,
        }
        (run / "dispatch.json").write_text(json.dumps(manifest) + "\n", encoding="utf-8")
        completion = {
            "run_id": "fixture-run",
            "job_id": "000000000",
            "connection": "redis",
            "queue": "benchmark",
            "enqueued_at_ns": 1_000_000_100,
            "work_started_at_ns": 1_000_000_200,
            "completed_at_ns": 1_000_001_000,
            "queue_latency_ns": 100,
            "end_to_end_ns": 900,
            "work_duration_ns": 800,
            "attempt": 1,
            "sink_lock_wait_ns": 1,
        }
        (run / "events" / "worker-1.jsonl").write_text(
            json.dumps(completion) + "\n", encoding="utf-8"
        )
        stats = [
            {
                "schema": "queen.laravel-supervisors.stats/v1",
                "type": "metadata",
                "monotonic_ns": 999_999_000,
                "interval_ns": 1_000_000_000,
                "pss_enabled": False,
            },
            {
                "schema": "queen.laravel-supervisors.stats/v1",
                "type": "sample",
                "sequence": 0,
                "monotonic_ns": 1_000_000_000,
                "sampling_duration_ns": 1,
                "targets": [],
            },
            {
                "schema": "queen.laravel-supervisors.stats/v1",
                "type": "end",
                "monotonic_ns": 2_000_000_000,
                "samples": 1,
                "reason": "signal",
            },
        ]
        (run / "stats.jsonl").write_text(
            "".join(json.dumps(record) + "\n" for record in stats), encoding="utf-8"
        )
        if include_queue_state:
            queue_state = {
                "schema": "queen.laravel-supervisors.queue-state/v1",
                "run_id": "fixture-run",
                "connection": "redis",
                "queue": "benchmark",
                "implementation": "Illuminate\\Queue\\RedisQueue",
                "started_at_ns": 2_000_000_000,
                "finished_at_ns": 3_000_000_000,
                "elapsed_ns": 1_000_000_000,
                "wait_ns": 30_000_000_000,
                "poll_ns": 100_000_000,
                "settle_ns": 1_000_000_000,
                "settled_for_ns": 1_000_000_000,
                "checks": 11,
                "quiescent": queue_size == 0,
                "timed_out": queue_size != 0,
                "state": {
                    "size": queue_size,
                    "ready": queue_size,
                    "reserved": 0,
                    "delayed": 0,
                },
                "supported": {"ready": True, "reserved": True, "delayed": True},
                "probe_errors": [],
                "probe_error_count": 0,
                "last_probe_error": None,
            }
            (run / "queue-state.final.json").write_text(
                json.dumps(queue_state) + "\n", encoding="utf-8"
            )
        return run

    def test_empty_queue_is_required_for_correct_run(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            summary = analyze.summarize_run(self.make_run(Path(temporary)))

        self.assertTrue(summary["queue_state"]["gate_passed"])
        self.assertTrue(summary["correctness"]["queue_quiescent"])
        self.assertTrue(summary["correctness"]["correct"])

    def test_nonempty_queue_fails_correctness(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            summary = analyze.summarize_run(self.make_run(Path(temporary), queue_size=1))

        self.assertFalse(summary["queue_state"]["gate_passed"])
        self.assertIn("state.size is not zero", summary["queue_state"]["validation_errors"])
        self.assertFalse(summary["correctness"]["correct"])

    def test_missing_queue_artifact_is_a_gate_failure_not_an_analysis_error(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            summary = analyze.summarize_run(
                self.make_run(Path(temporary), include_queue_state=False)
            )

        self.assertFalse(summary["queue_state"]["artifact_valid"])
        self.assertFalse(summary["correctness"]["correct"])

    def test_report_suppresses_ratios_when_queue_gate_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            valid_run = self.make_run(root / "valid")
            invalid_run = self.make_run(root / "invalid", queue_size=1)
            valid_summary = analyze.summarize_run(valid_run)
            invalid_summary = analyze.summarize_run(invalid_run)
            valid_path = root / "valid-summary.json"
            invalid_path = root / "invalid-summary.json"
            valid_path.write_text(json.dumps(valid_summary) + "\n", encoding="utf-8")
            invalid_path.write_text(json.dumps(invalid_summary) + "\n", encoding="utf-8")
            report_path = root / "report.json"
            args = argparse.Namespace(
                scenario=[("valid", valid_path), ("invalid", invalid_path)],
                max_ids=100,
                no_comparisons=False,
                output=str(root / "report.md"),
                json_output=str(report_path),
                allow_invalid=True,
            )
            self.assertEqual(0, analyze.report_command(args))
            report = json.loads(report_path.read_text(encoding="utf-8"))

        self.assertFalse(report["all_correct"])
        self.assertFalse(report["comparisons"][0]["eligible"])
        self.assertIsNone(report["comparisons"][0]["throughput_ratio"])


if __name__ == "__main__":
    unittest.main()

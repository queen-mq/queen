from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "analyze.py"
SPEC = importlib.util.spec_from_file_location("backend_operation_analyze", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
analyze = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = analyze
SPEC.loader.exec_module(analyze)


class BackendOperationSummaryTest(unittest.TestCase):
    def test_redis_excludes_the_info_observer_and_keeps_command_shape(self) -> None:
        before = """# Stats
total_commands_processed:100
# Commandstats
cmdstat_info:calls=1,usec=1,usec_per_call=1.00
cmdstat_eval:calls=20,usec=20,usec_per_call=1.00
"""
        after = """# Stats
total_commands_processed:143
# Commandstats
cmdstat_info:calls=2,usec=2,usec_per_call=1.00
cmdstat_eval:calls=60,usec=60,usec_per_call=1.00
cmdstat_zrem:calls=2,usec=2,usec_per_call=1.00
"""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "backend-metrics.before.redis-info.txt").write_text(
                before, encoding="utf-8"
            )
            (root / "backend-metrics.after.redis-info.txt").write_text(
                after, encoding="utf-8"
            )
            summary = analyze.backend_operation_summary(
                root, {"connection": "redis"}, completed=10
            )

        self.assertTrue(summary["available"])
        self.assertEqual(42, summary["operational_commands"])
        self.assertEqual(4.2, summary["operational_commands_per_completed_job"])
        self.assertEqual({"eval": 40, "info": 1, "zrem": 2}, summary["command_calls"])

    def test_queen_reports_consumer_batching_separately(self) -> None:
        def snapshot(push_r: int, pop_r: int, ack_r: int, messages: int) -> str:
            return "\n".join(
                (
                    f"queen_process_push_requests_total {push_r}",
                    f"queen_process_pop_requests_total {pop_r}",
                    f"queen_process_ack_requests_total {ack_r}",
                    f"queen_process_push_messages_total {messages}",
                    f"queen_process_pop_messages_total {messages}",
                    f"queen_process_ack_messages_total {messages}",
                    "",
                )
            )

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "backend-metrics.before.prom").write_text(
                snapshot(5, 3, 3, 40), encoding="utf-8"
            )
            (root / "backend-metrics.after.prom").write_text(
                snapshot(15, 5, 5, 120), encoding="utf-8"
            )
            summary = analyze.backend_operation_summary(
                root, {"connection": "queen"}, completed=80
            )

        self.assertTrue(summary["available"])
        self.assertEqual({"push": 10, "pop": 2, "ack": 2}, summary["requests"])
        self.assertEqual(0.05, summary["consumer_requests_per_completed_job"])
        self.assertEqual(40.0, summary["messages_per_request"]["pop"])

    def test_missing_snapshots_are_diagnostic_not_an_exception(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            summary = analyze.backend_operation_summary(
                Path(temporary), {"connection": "queen"}, completed=1
            )

        self.assertFalse(summary["available"])
        self.assertEqual(2, len(summary["validation_errors"]))

    def test_counter_reset_fails_closed(self) -> None:
        before = "\n".join(f"{name} 10" for name in analyze.QUEEN_PROCESS_COUNTERS)
        after = "\n".join(f"{name} 9" for name in analyze.QUEEN_PROCESS_COUNTERS)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "backend-metrics.before.prom").write_text(before, encoding="utf-8")
            (root / "backend-metrics.after.prom").write_text(after, encoding="utf-8")
            summary = analyze.backend_operation_summary(
                root, {"connection": "queen"}, completed=1
            )

        self.assertFalse(summary["available"])
        self.assertEqual(6, len(summary["validation_errors"]))

    def test_markdown_does_not_render_unavailable_counters_as_zero(self) -> None:
        summary = {
            "correctness": {
                "correct": True,
                "unique_completed": 1,
                "expected": 1,
                "missing": {"count": 0},
                "duplicates": {"count": 0},
                "failed": {"count": 0},
            },
            "queue_state": {"gate_passed": True, "state": {"size": 0}},
            "throughput": {"headline_jobs_per_second": 1.0},
            "latency": {
                "end_to_end": {"p50_ms": 1.0, "p95_ms": 1.0, "p99_ms": 1.0}
            },
            "scaling": {
                "worker_peak": 1,
                "time_to_peak_workers_ns": 0,
                "return_to_initial_after_completion_ns": 0,
            },
            "resources": {},
            "backend_operations": {
                "available": False,
                "source": "prometheus",
                "requests": {"push": 0, "pop": 0, "ack": 0},
                "validation_errors": ["counter reset: ack"],
            },
        }

        markdown = analyze.markdown_report(
            {"baseline": "queen", "scenarios": [{"label": "queen", "summary": summary}]}
        )

        row = next(line for line in markdown.splitlines() if "prometheus" in line)
        self.assertIn("prometheus (unavailable: counter reset: ack)", row)
        self.assertEqual(7, row.count("n/a"))


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path

SPEC = importlib.util.spec_from_file_location(
    "failure_matrix", Path(__file__).resolve().parents[1] / "failure_matrix.py"
)
assert SPEC is not None and SPEC.loader is not None
matrix = importlib.util.module_from_spec(SPEC)
sys.modules["failure_matrix"] = matrix
SPEC.loader.exec_module(matrix)


def report(jobs: dict[str, list[str]], failed: list[str] | None = None, dead_letter: int | None = None) -> dict:
    return {
        "jobs": {
            job: {"events": [[event, 1, float(i), None] for i, event in enumerate(events)], "pids": [7]}
            for job, events in jobs.items()
        },
        "failed_store": {"available": failed is not None, "job_ids": failed or []},
        "dead_letter": {"available": dead_letter is not None, "entries": dead_letter},
    }


class FakeLane:
    def __init__(self, connection: str) -> None:
        self.profile = matrix.Profile("p", "e", connection, {}, "master")


class FailureMatrixChecksTest(unittest.TestCase):
    def test_a_job_completed_twice_fails_the_duplicate_check(self) -> None:
        jobs = matrix.Jobs(report({"000000": ["started", "completed", "started", "completed"]}, failed=[]))

        checks = {c.name: c for c in matrix.completed_once(jobs, ["000000"])}

        self.assertTrue(checks["every job completed"].passed)
        self.assertFalse(checks["no job completed twice"].passed)
        self.assertIn("000000", checks["no job completed twice"].detail)

    def test_a_lost_job_fails_the_completion_check(self) -> None:
        jobs = matrix.Jobs(report({"000000": ["started", "completed"]}, failed=[]))

        checks = {c.name: c for c in matrix.completed_once(jobs, ["000000", "000001"])}

        self.assertFalse(checks["every job completed"].passed)

    def test_a_final_failure_needs_the_hook_the_row_and_on_queen_the_dead_letter(self) -> None:
        jobs = matrix.Jobs(report({"000000": ["started", "threw", "failed_hook"]}, failed=["000000"], dead_letter=1))

        queen = matrix.failed_finally(FakeLane("queen"), jobs, ["000000"])
        horizon = matrix.failed_finally(FakeLane("redis"), jobs, ["000000"])

        self.assertTrue(all(c.passed for c in queen))
        self.assertIn("one dead-letter entry each", [c.name for c in queen])
        self.assertNotIn("one dead-letter entry each", [c.name for c in horizon])

    def test_a_missing_failed_row_or_dead_letter_is_reported(self) -> None:
        jobs = matrix.Jobs(report({"000000": ["started", "threw", "failed_hook"]}, failed=[], dead_letter=0))

        failed = [c.name for c in matrix.failed_finally(FakeLane("queen"), jobs, ["000000"]) if not c.passed]

        self.assertEqual(["one failed-job row each", "one dead-letter entry each"], failed)

    def test_an_unreadable_failed_store_never_passes(self) -> None:
        jobs = matrix.Jobs(report({"000000": ["started", "completed"]}))

        checks = {c.name: c for c in matrix.completed_once(jobs, ["000000"])}

        self.assertFalse(checks["no failed-job row"].passed)

    def test_an_attempt_that_starts_before_the_previous_one_ends_overlaps(self) -> None:
        def timed(events: list[tuple[str, int, float]]) -> dict:
            return {"jobs": {"000000": {"events": [[e, a, t, None] for e, a, t in events], "pids": []}}}

        overlapping = matrix.Jobs(timed([("started", 1, 0), ("started", 2, 5), ("completed", 1, 10),
                                         ("completed", 2, 15)]))
        sequential = matrix.Jobs(timed([("started", 1, 0), ("completed", 1, 10), ("started", 2, 30),
                                        ("completed", 2, 40)]))
        killed = matrix.Jobs(timed([("started", 1, 0), ("started", 2, 5), ("completed", 2, 15)]))

        self.assertTrue(overlapping.overlapping("000000"))
        self.assertFalse(sequential.overlapping("000000"))
        self.assertFalse(killed.overlapping("000000"), "a killed attempt has no end to compare")

    def test_a_duplicate_is_tolerated_only_where_at_least_once_allows_it(self) -> None:
        jobs = matrix.Jobs(report({"000000": ["started", "completed", "started", "completed"]}, failed=[]))

        strict = {c.name: c.passed for c in matrix.completed_once(jobs, ["000000"])}
        lenient = [c for c in matrix.completed_once(jobs, ["000000"], duplicates_allowed=True)]

        self.assertFalse(strict["no job completed twice"])
        self.assertTrue(all(c.passed for c in lenient))
        self.assertIn("000000", lenient[2].detail)

    def test_the_summary_lists_failed_checks_and_errors(self) -> None:
        text = matrix.summary([
            {"scenario": "a", "profile": "queen", "passed": True, "checks": [{"name": "x", "passed": True}]},
            {"scenario": "b", "profile": "horizon", "passed": False, "checks": [{"name": "y", "passed": False}]},
            {"scenario": "c", "profile": "queen", "passed": False, "error": "RuntimeError: boom"},
        ])

        self.assertIn("| a | queen | pass |  |", text)
        self.assertIn("| b | horizon | FAIL | y |", text)
        self.assertIn("RuntimeError: boom", text)

    def test_every_scenario_name_is_unique_and_every_profile_is_known(self) -> None:
        names = [s.name for s in matrix.SCENARIOS]

        self.assertEqual(len(names), len(set(names)))
        self.assertEqual({"horizon", "queen-php", "queen-rust", "queen-rust-fast"}, set(matrix.PROFILES))
        self.assertEqual(("horizon", "queen-php", "queen-rust"), matrix.DEFAULT_PROFILES)

    def test_a_soak_report_with_empty_php_arrays_reads_as_maps(self) -> None:
        empty = matrix.soak_report({"summary": [], "failed_store": 0})
        clean = matrix.soak_report({"summary": {"ok": {"jobs": 2, "as_expected": 2, "anomalies": []}}})

        self.assertEqual({}, empty["summary"])
        self.assertEqual({}, clean["summary"]["ok"]["anomalies"])
        self.assertEqual(0, empty["failed_store"])


if __name__ == "__main__":
    unittest.main()

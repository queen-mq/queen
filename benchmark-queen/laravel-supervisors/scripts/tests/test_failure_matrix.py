from __future__ import annotations

import importlib.util
import json
import re
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

    def test_a_job_outcome_has_its_runs_its_failure_and_its_row(self) -> None:
        raw = report({"000000": ["started", "threw", "started", "threw", "failed_hook"], "000001": ["started", "completed"]},
                     failed=["000000"])
        raw["jobs"]["000000"]["events"][-1][3] = "Illuminate\\Queue\\TimeoutExceededException"
        jobs = matrix.Jobs(raw)

        self.assertEqual({"runs": 2, "completed": 0, "failed": 1, "failed_with": "Illuminate\\Queue\\TimeoutExceededException",
                          "failed_row": True}, jobs.outcome("000000"))
        self.assertEqual({"runs": 1, "completed": 1, "failed": 0, "failed_with": None, "failed_row": False},
                         jobs.outcome("000001"))

    def test_only_a_run_that_has_not_ended_is_killed_and_its_worker_is_known(self) -> None:
        def jobs(events: list[tuple[str, int, int]]) -> matrix.Jobs:
            return matrix.Jobs({"jobs": {"000000": {"events": [[e, a, float(i), None, pid] for i, (e, a, pid)
                                                               in enumerate(events)], "pids": []}}})

        running = jobs([("event_before", 1, 7), ("started", 1, 7), ("released", 1, 7), ("event_before", 2, 9),
                        ("started", 2, 9)])
        released = jobs([("event_before", 1, 7), ("started", 1, 7), ("released", 1, 7)])
        four_fields = matrix.Jobs({"jobs": {"000000": {"events": [["started", 1, 0.0, None]], "pids": [7]}}})

        self.assertEqual((2, 9), matrix.run_in_progress(running, "000000"))
        self.assertIsNone(matrix.run_in_progress(released, "000000"))
        self.assertIsNone(matrix.run_in_progress(four_fields, "000000"), "a report without pids names no worker")
        self.assertEqual([1, 2], running.attempts_of("000000", "event_before"))
        self.assertEqual([(1.0, 2.0), (4.0, None)], running.attempts("000000"), "five fields read as four")

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
        self.assertEqual({"horizon", "queen-php", "queen-rust", "queen-rust-fast", "queen-php-fast"}, set(matrix.PROFILES))
        self.assertEqual(("horizon", "queen-php", "queen-rust"), matrix.DEFAULT_PROFILES)

    def test_a_soak_report_with_empty_php_arrays_reads_as_maps(self) -> None:
        empty = matrix.soak_report({"summary": [], "failed_store": 0})
        clean = matrix.soak_report({"summary": {"ok": {"jobs": 2, "as_expected": 2, "anomalies": []}}})

        self.assertEqual({}, empty["summary"])
        self.assertEqual({}, clean["summary"]["ok"]["anomalies"])
        self.assertEqual(0, empty["failed_store"])

    def test_a_soak_is_drained_only_when_every_dispatched_job_has_ended(self) -> None:
        def report(jobs: int, anomalies: dict) -> dict:
            return {"summary": {"late": {"jobs": jobs, "as_expected": jobs, "anomalies": anomalies}}}

        self.assertFalse(matrix.soak_drained(report(90, {}), 93), "three delayed jobs are not due yet")
        self.assertFalse(matrix.soak_drained(report(93, {"late-0000001": "not completed yet"}), 93))
        self.assertTrue(matrix.soak_drained(report(93, {}), 93))
        self.assertTrue(matrix.soak_drained(report(93, {"late-0000001": "completed 2 times"}), 93))

    def test_the_compat_lanes_run_every_scenario_of_their_commands(self) -> None:
        commands = Path(__file__).resolve().parents[2] / "app" / "app" / "Console" / "Commands"

        for scenarios, php in ((matrix.COMPAT_SCENARIOS, "CompatCommand.php"),
                               (matrix.COMPAT_MORE_SCENARIOS, "CompatMoreCommand.php"),
                               (matrix.COMPAT_PARITY_SCENARIOS, "CompatParityCommand.php"),
                               (matrix.COMPAT_ROUTED_SCENARIOS, "CompatRoutedCommand.php")):
            constant = re.search(r"SCENARIOS = \[(.*?)\];", (commands / php).read_text(), re.S)
            assert constant is not None
            self.assertEqual(list(scenarios), re.findall(r"'([a-z-]+)'", constant.group(1)), php)
        names = [*matrix.COMPAT_SCENARIOS, *matrix.COMPAT_MORE_SCENARIOS, *matrix.COMPAT_PARITY_SCENARIOS,
                 *matrix.COMPAT_ROUTED_SCENARIOS]
        self.assertEqual(len(names), len(set(names)), "an outcome is kept by scenario name")
        self.assertEqual(("prune-failed", "fork-in-job"), matrix.COMPAT_MORE_SCENARIOS[-2:],
                         "prune-failed empties the failed-job store; fork-in-job can leave retries behind")

    def test_the_parity_runs_name_known_scenarios_and_never_empty_the_failed_store(self) -> None:
        known = {"bench:compat": matrix.COMPAT_SCENARIOS, "bench:compat-more": matrix.COMPAT_MORE_SCENARIOS,
                 "bench:compat-parity": matrix.COMPAT_PARITY_SCENARIOS,
                 "bench:compat-routed": matrix.COMPAT_ROUTED_SCENARIOS}

        for command, name in matrix.ROUTED_RUNS:
            self.assertIn(name, known[command], f"{command} {name}")
        self.assertNotIn(("bench:compat-more", "prune-failed"), matrix.PARITY_RUNS)
        self.assertNotIn(("bench:compat-more", "fork-in-job"), matrix.PARITY_RUNS)
        self.assertEqual(set(matrix.PARITY_SCENARIOS), set(matrix.PARITY_SCENARIOS) & {s.name for s in matrix.SCENARIOS})


class LaneProfilesTest(unittest.TestCase):
    def test_prefork_off_adds_a_named_variant_of_each_queen_profile_and_none_of_horizon(self) -> None:
        profiles = matrix.lane_profiles(["horizon", "queen-php", "queen-rust"], ["on", "off"])

        self.assertEqual(["horizon", "queen-php", "queen-php-prefork-off", "queen-rust", "queen-rust-prefork-off"],
                         [p.name for p in profiles])
        off = {p.name: p for p in profiles}["queen-rust-prefork-off"]
        self.assertEqual("false", off.env["BENCH_QUEEN_PREFORK"])
        self.assertEqual("queen-rust", off.engine)
        self.assertEqual(matrix.PROFILES["queen-rust"].env["QUEEN_PREFETCH"], off.env["QUEEN_PREFETCH"])

    def test_prefork_on_keeps_the_historical_profiles(self) -> None:
        profiles = matrix.lane_profiles(["horizon", "queen-php"], ["on"])

        self.assertEqual([matrix.PROFILES["horizon"], matrix.PROFILES["queen-php"]], profiles)

    def test_a_lane_sets_prefork_on_unless_its_profile_turns_it_off(self) -> None:
        on, off = matrix.lane_profiles(["queen-php"], ["on", "off"])

        self.assertEqual("true", matrix.Lane("s", on, {}, Path("/tmp")).env["BENCH_QUEEN_PREFORK"])
        self.assertEqual("false", matrix.Lane("s", off, {}, Path("/tmp")).env["BENCH_QUEEN_PREFORK"])

    def test_an_engine_setting_applies_to_that_engine_only(self) -> None:
        scenario = next(s for s in matrix.SCENARIOS if s.name == "stop-lease")

        self.assertEqual("90", scenario.env_for(matrix.PROFILES["horizon"])["BENCH_RETRY_AFTER"])
        self.assertEqual("60", scenario.env_for(matrix.PROFILES["horizon"])["BENCH_TIMEOUT"])
        self.assertEqual("30", scenario.env_for(matrix.PROFILES["queen-rust"])["BENCH_RETRY_AFTER"])
        self.assertEqual("25", scenario.env_for(matrix.PROFILES["queen-rust"])["BENCH_TIMEOUT"])


class StackTest(unittest.TestCase):
    def test_the_default_stack_changes_no_lane(self) -> None:
        self.assertEqual({}, matrix.stack_env("default", matrix.PROFILES["queen-rust"]))

    def test_cb3_balances_by_backlog_over_two_queues_with_the_cli_opcache_on_every_engine(self) -> None:
        queen = matrix.stack_env("cb3", matrix.PROFILES["queen-rust"])
        horizon = matrix.stack_env("cb3", matrix.PROFILES["horizon"])

        for env in (queen, horizon):
            self.assertEqual(("auto", "auto", "1"), (env["BENCH_PROFILE"], env["BENCH_ROUTED_BALANCE"],
                                                     env["BENCH_OPCACHE_CLI"]))
            self.assertGreater(len(env["BENCH_QUEUES"].split(",")), 1)
        self.assertEqual("2", queen["BENCH_MIN_WORKERS"])
        self.assertEqual("1", horizon["BENCH_MIN_WORKERS"], "Horizon's minProcesses counts per queue")

    def test_a_stack_setting_wins_over_the_scenario_and_the_lane(self) -> None:
        scenario = next(s for s in matrix.SCENARIOS if s.name == "laravel-parity")
        profile = matrix.PROFILES["horizon"]

        lane = matrix.Lane("s", profile, {**scenario.env_for(profile), **matrix.stack_env("cb3", profile)}, Path("/tmp"))

        self.assertEqual("1", lane.env["BENCH_OPCACHE_CLI"], "the lane alone turns it off on Horizon")
        self.assertEqual("4", lane.env["BENCH_MAX_WORKERS"])
        self.assertEqual("database", lane.env["BENCH_CACHE_STORE"])

    def test_stack_versions_come_from_bench_config_and_the_supervisor(self) -> None:
        config = json.dumps({"php": "8.4.13", "laravel": "v11.55.1", "horizon": "v5.48.3",
                             "queen_client": "dev-main", "opcache_cli": True, "benchmark": {}})

        self.assertEqual({"php": "8.4.13", "laravel": "v11.55.1", "horizon": "v5.48.3", "queen_client": "dev-main",
                          "opcache_cli": True, "supervisor": "0.8.0"},
                         matrix.stack_versions(config, "queen-supervisor 0.8.0\n"))
        self.assertIsNone(matrix.stack_versions(config)["supervisor"])
        self.assertIsNone(matrix.stack_versions("{}")["opcache_cli"], "an older image reports no opcache")


def outcome(same: dict | None = None, near: dict | None = None) -> dict:
    return {"same": same or {}, "near": {k: {"value": v, "tolerance": t} for k, (v, t) in (near or {}).items()}}


class ParityTest(unittest.TestCase):
    def test_equal_values_and_times_within_tolerance_are_the_same(self) -> None:
        horizon = outcome({"job p1": {"runs": [1, 2, 3]}, "checks": {"a": True}}, {"gap": (1.81, 1.5)})
        queen = outcome({"job p1": {"runs": [1, 2, 3]}, "checks": {"a": True}}, {"gap": (1.02, 1.5)})

        self.assertEqual(([], []), matrix.compare_outcomes(horizon, queen))

    def test_another_value_a_time_out_of_tolerance_or_a_failed_check_differs(self) -> None:
        horizon = outcome({"job j1": {"pickups": [1, 2, 3, 4]}, "checks": {"a": True}}, {"gap": (5.0, 1.5)})
        queen = outcome({"job j1": {"pickups": [1, 2, 3]}, "checks": {"a": False}}, {"gap": (0.1, 1.5)})

        divergences, _ = matrix.compare_outcomes(horizon, queen)

        self.assertEqual(3, len(divergences), divergences)
        self.assertTrue(any(d.startswith("job j1: Horizon") for d in divergences))
        self.assertTrue(any("Horizon passes, this lane fails" in d for d in divergences))
        self.assertTrue(any(d.startswith("gap: Horizon 5.0 s, this lane 0.1 s") for d in divergences))

    def test_a_check_only_one_backend_runs_is_a_note_but_a_missing_value_differs(self) -> None:
        horizon = outcome({"checks": {"a": True}, "batch state": {"finished": True}})
        queen = outcome({"checks": {"a": True, "queen: one partition": True}})

        divergences, notes = matrix.compare_outcomes(horizon, queen)

        self.assertEqual(["batch state: not recorded here"], divergences)
        self.assertEqual(["checked on one side only: queen: one partition"], notes)

    def test_php_empty_maps_compare_as_empty(self) -> None:
        self.assertEqual(([], []), matrix.compare_outcomes({"same": [], "near": []}, {"same": {}, "near": []}))

    def test_each_lane_is_compared_with_the_horizon_lane_of_its_scenario(self) -> None:
        same = {"x": outcome({"v": 1})}
        results = [
            {"scenario": "s", "profile": "horizon", "outcome": same},
            {"scenario": "s", "profile": "queen-php", "outcome": same},
            {"scenario": "s", "profile": "queen-rust-prefork-off", "outcome": {"x": outcome({"v": 2})}},
            {"scenario": "s", "profile": "queen-rust", "error": "RuntimeError: boom"},
            {"scenario": "t", "profile": "queen-php", "outcome": same},
        ]

        rows = {(r["scenario"], r["profile"]): r for r in matrix.parity(results)}

        self.assertEqual("same", rows[("s", "queen-php")]["status"])
        self.assertEqual("differs", rows[("s", "queen-rust-prefork-off")]["status"])
        self.assertEqual("missing", rows[("s", "queen-rust")]["status"], "a lane that crashed has no outcome")
        self.assertEqual("no Horizon lane", rows[("t", "queen-php")]["status"])
        self.assertNotIn(("s", "horizon"), rows)
        self.assertIn("| s | x | queen-rust-prefork-off | differs | v: Horizon 1, this lane 2 |",
                      matrix.parity_markdown(matrix.parity(results)))


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import copy
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "campaign-stats.py"
SPEC = importlib.util.spec_from_file_location("laravel_campaign_stats", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
campaign_stats = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = campaign_stats
SPEC.loader.exec_module(campaign_stats)


class CampaignStatsTest(unittest.TestCase):
    def metadata(self, runs: int) -> dict:
        return {
            "schema": "queen.laravel-supervisors.campaign/v1",
            "campaign_id": "fixture-campaign",
            "qualification": "candidate",
            "git": {"commit": "a" * 40, "branch": "fixture", "dirty": False},
            "host": {"machine": "x86_64", "platform": "Linux", "python": "3.11"},
            "docker": {
                "Architecture": "x86_64",
                "CgroupVersion": "2",
                "KernelVersion": "6.8.0",
                "ServerVersion": "29.0.0",
            },
            "images": {"app": "sha256:app", "broker": "sha256:broker"},
            "settings": {
                "engines": ["horizon", "queen-rust"],
                "profiles": ["fixed"],
                "runs": runs,
                "jobs": 100,
                "workers": 2,
                "min_workers": 1,
                "max_workers": 2,
                "sleep_ms": 10,
                "cpu_iterations": 0,
                "dispatch_mode": "single",
                "queen_prefetch": 1,
                "queen_ack_batch": 1,
                "queen_bulk_batch": 100,
                "queen_partitions": 64,
                "queen_pop_fusion": False,
                "sample_interval_seconds": 0.5,
                "post_drain_seconds_by_profile": {"fixed": 2},
                "autoscaling_strategy": "size",
                "balance_cooldown_seconds": 3,
                "balance_max_shift": 1,
                "target_jobs_per_process": 10,
                "target_clear_seconds": 1.0,
            },
        }

    def configuration(self, engine: str) -> dict:
        connection = "redis" if engine == "horizon" else "queen"
        return {
            "php": "8.3.0",
            "laravel": "v12.0.0",
            "horizon": "v5.0.0",
            "queen_client": "dev-fixture",
            "benchmark": {
                "profile": "fixed",
                "connection": connection,
                "queue": "benchmark",
                "consumer_group": "benchmark",
                "workers": 2,
                "min_workers": 2,
                "max_workers": 2,
                "strategy": "size",
                "balance_cooldown": 3,
                "balance_max_shift": 1,
                "scale_down_delay": 0,
                "target_jobs_per_process": 10,
                "target_clear_seconds": 1.0,
                "default_runtime_seconds": 0.01,
                "poll_interval": 1,
                "block_for": 1,
                "worker_sleep": 1,
                "timeout": 120,
                "retry_after": 180,
                "worker_memory": 128,
                "dispatch_mode": "single",
                "queen_prefetch": 1,
                "queen_ack_batch": 1,
                "queen_bulk_batch": 100,
                "queen_partitions": 64,
                "queen_pop_fusion": False,
            },
            "queen_connection": {
                "timeout": 30_000,
                "retry_attempts": 3,
                "retry_delay": 100,
                "load_balancing_strategy": "affinity",
                "enable_failover": True,
                "affinity_hash_ring": 150,
                "health_retry_after": 1_000,
                "retry_429": [],
                "partition_prefix": "benchmark",
                "after_commit": False,
            },
        }

    def summary(self, engine: str, repetition: str, throughput: float) -> dict:
        connection = "redis" if engine == "horizon" else "queen"
        return {
            "schema": "queen.laravel-supervisors.summary/v1",
            "run_id": f"{engine}-fixed-{repetition}",
            "manifest": {
                "run_id": f"{engine}-fixed-{repetition}",
                "jobs": 100,
                "connection": connection,
                "queue": "benchmark",
                "sleep_ms": 10,
                "cpu_iterations": 0,
                "dispatch_mode": "single",
                "dispatch_batch_size": 1,
            },
            "correctness": {
                "correct": True,
                "complete": True,
                "expected": 100,
                "unique_completed": 100,
                "missing": {"count": 0},
                "duplicates": {"count": 0},
                "failed": {"count": 0, "records": 0},
                "unexpected": {"count": 0},
                "attempts_valid": True,
                "foreign_records": 0,
                "invalid_records": 0,
                "malformed_lines": 0,
                "partial_lines_ignored": 0,
                "unreadable_files": 0,
                "queue_quiescent": True,
            },
            "queue_state": {
                "artifact_valid": True,
                "quiescent": True,
                "gate_passed": True,
                "state": {"size": 0, "ready": 0, "reserved": 0, "delayed": 0},
                "supported": {"ready": True, "reserved": True, "delayed": True},
                "validation_errors": [],
            },
            "throughput": {
                "completion_span_jobs_per_second": throughput,
                "headline_jobs_per_second": throughput * 0.98,
                "dispatch_jobs_per_second": 5_000.0,
            },
            "latency": {
                "end_to_end": {"p95_ms": 1_000 / throughput, "p99_ms": 1_100 / throughput}
            },
            "resources": {
                "headline_window": "measurement_with_drain",
                "stats_integrity": {
                    "expected_interval_ns": 500_000_000,
                    "pss_requested": True,
                    "pss_complete": True,
                    "integrity_errors": 0,
                    "oom_events": 0,
                    "samples": 20,
                },
                "orchestrator": {
                    "cpu_seconds": 1.0,
                    "pss_bytes": {"max": 16 * 1024 * 1024},
                    "rss_bytes": {"max": 20 * 1024 * 1024},
                },
                "workers": {"cpu_seconds": 2.0},
                "app": {
                    "cpu_seconds": 3.0,
                    "memory_current_bytes": {"max": 128 * 1024 * 1024},
                },
                "backend": {
                    "cpu_seconds": 4.0,
                    "memory_current_bytes": {"max": 256 * 1024 * 1024},
                },
                "stack": {
                    "cpu_seconds": 7.0,
                    "memory_current_bytes": {"max": 384 * 1024 * 1024},
                },
            },
            "warnings": [],
        }

    def compose(self, engine: str, backend_cpu: float = 2.0) -> str:
        if engine == "horizon":
            backend = f"""  redis:
    cpus: {backend_cpu}
    mem_limit: \"2147483648\"
    pids_limit: 128
"""
        else:
            half = backend_cpu / 2
            backend = f"""  broker:
    cpus: {half}
    mem_limit: \"1073741824\"
    pids_limit: 256
  postgres:
    cpus: {half}
    mem_limit: \"1073741824\"
    pids_limit: 256
"""
        return f"""name: fixture
services:
  {engine}:
    cpus: 4
    mem_limit: \"1073741824\"
    pids_limit: 512
    stop_grace_period: 2m30s
{backend}networks:
  default:
    name: fixture
"""

    def make_campaign(
        self,
        root: Path,
        horizon_values: list[float],
        queen_values: list[float],
    ) -> Path:
        campaign = root / "campaign"
        campaign.mkdir()
        (campaign / "metadata.json").write_text(
            json.dumps(self.metadata(len(horizon_values))) + "\n", encoding="utf-8"
        )
        for engine, values in (("horizon", horizon_values), ("queen-rust", queen_values)):
            for index, throughput in enumerate(values, start=1):
                repetition = f"r{index:02d}"
                run = campaign / engine / "fixed" / repetition
                run.mkdir(parents=True)
                (run / "summary.json").write_text(
                    json.dumps(self.summary(engine, repetition, throughput)) + "\n",
                    encoding="utf-8",
                )
                (run / "configuration.json").write_text(
                    json.dumps(self.configuration(engine)) + "\n", encoding="utf-8"
                )
                (run / "compose-resolved.yml").write_text(
                    self.compose(engine), encoding="utf-8"
                )
        return campaign

    def comparison(self, report: dict) -> dict:
        self.assertEqual(1, len(report["comparisons"]))
        return report["comparisons"][0]

    def test_aggregates_and_bootstrap_are_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            campaign = self.make_campaign(
                Path(temporary), [100.0, 100.0, 100.0], [200.0, 180.0, 220.0]
            )
            first = campaign_stats.build_report(campaign, seed=17, resamples=500)
            second = campaign_stats.build_report(campaign, seed=17, resamples=500)

        comparison = self.comparison(first)
        self.assertEqual(3, comparison["pairs_eligible"])
        ratios = comparison["metrics"]["completion_span_jobs_per_second"]
        self.assertEqual(3, ratios["n"])
        self.assertAlmostEqual(2.0, ratios["median"])
        self.assertAlmostEqual(1.9, ratios["q1"])
        self.assertAlmostEqual(2.1, ratios["q3"])
        self.assertEqual(
            ratios["bootstrap_ci_95"],
            self.comparison(second)["metrics"]["completion_span_jobs_per_second"][
                "bootstrap_ci_95"
            ],
        )
        queen_group = next(
            group for group in first["aggregates"] if group["engine"] == "queen-rust"
        )
        self.assertAlmostEqual(
            200.0,
            queen_group["metrics"]["completion_span_jobs_per_second"]["median"],
        )

    def test_non_quiescent_run_is_reported_and_pair_is_suppressed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            campaign = self.make_campaign(Path(temporary), [100.0, 100.0], [200.0, 200.0])
            path = campaign / "queen-rust" / "fixed" / "r02" / "summary.json"
            summary = json.loads(path.read_text(encoding="utf-8"))
            summary["correctness"]["correct"] = False
            summary["correctness"]["queue_quiescent"] = False
            summary["queue_state"]["quiescent"] = False
            summary["queue_state"]["gate_passed"] = False
            summary["queue_state"]["state"]["size"] = 1
            summary["queue_state"]["state"]["ready"] = 1
            path.write_text(json.dumps(summary) + "\n", encoding="utf-8")
            report = campaign_stats.build_report(campaign, seed=1, resamples=100)

        comparison = self.comparison(report)
        self.assertEqual(1, comparison["pairs_eligible"])
        pair = next(item for item in comparison["paired_runs"] if item["repetition"] == "r02")
        self.assertFalse(pair["eligible"])
        self.assertTrue(all(value is None for value in pair["ratios"].values()))
        self.assertTrue(any("queue" in reason for reason in pair["suppression_reasons"]))
        invalid = next(
            run
            for run in report["runs"]
            if run["engine"] == "queen-rust" and run["repetition"] == "r02"
        )
        self.assertFalse(invalid["validation"]["valid"])

    def test_configuration_mismatch_enumerates_path_and_suppresses_ratio(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            campaign = self.make_campaign(Path(temporary), [100.0], [200.0])
            path = campaign / "queen-rust" / "fixed" / "r01" / "configuration.json"
            configuration = json.loads(path.read_text(encoding="utf-8"))
            configuration["benchmark"]["worker_sleep"] = 2
            path.write_text(json.dumps(configuration) + "\n", encoding="utf-8")
            report = campaign_stats.build_report(campaign, seed=1, resamples=100)

        comparison = self.comparison(report)
        self.assertEqual(0, comparison["pairs_eligible"])
        reasons = comparison["paired_runs"][0]["suppression_reasons"]
        self.assertTrue(any("benchmark.worker_sleep" in reason for reason in reasons), reasons)
        self.assertTrue(
            all(
                value is None
                for value in comparison["paired_runs"][0]["ratios"].values()
            )
        )

    def test_extra_repetition_is_preserved_but_invalid_and_suppressed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            campaign = self.make_campaign(Path(temporary), [100.0], [200.0])
            run = campaign / "queen-rust" / "fixed" / "r99"
            run.mkdir(parents=True)
            (run / "summary.json").write_text(
                json.dumps(self.summary("queen-rust", "r99", 999.0)) + "\n",
                encoding="utf-8",
            )
            (run / "configuration.json").write_text(
                json.dumps(self.configuration("queen-rust")) + "\n", encoding="utf-8"
            )
            (run / "compose-resolved.yml").write_text(
                self.compose("queen-rust"), encoding="utf-8"
            )
            report = campaign_stats.build_report(campaign, seed=1, resamples=100)

        extra = next(run for run in report["runs"] if run["repetition"] == "r99")
        self.assertFalse(extra["validation"]["valid"])
        self.assertTrue(
            any("outside metadata.settings.runs" in reason for reason in extra["validation"]["errors"])
        )
        pair = next(
            pair
            for pair in self.comparison(report)["paired_runs"]
            if pair["repetition"] == "r99"
        )
        self.assertFalse(pair["eligible"])
        self.assertTrue(all(value is None for value in pair["ratios"].values()))

    def test_summary_and_manifest_run_ids_are_bound_to_directory(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            campaign = self.make_campaign(Path(temporary), [100.0], [200.0])
            path = campaign / "queen-rust" / "fixed" / "r01" / "summary.json"
            summary = json.loads(path.read_text(encoding="utf-8"))
            summary["run_id"] = "queen-rust-fixed-r77"
            summary["manifest"]["run_id"] = "queen-rust-auto-r01"
            path.write_text(json.dumps(summary) + "\n", encoding="utf-8")
            report = campaign_stats.build_report(campaign, seed=1, resamples=100)

        run = next(item for item in report["runs"] if item["engine"] == "queen-rust")
        self.assertFalse(run["validation"]["valid"])
        identity_errors = [
            reason for reason in run["validation"]["errors"] if "directory identity" in reason
        ]
        self.assertEqual(2, len(identity_errors))
        self.assertEqual(0, self.comparison(report)["pairs_eligible"])

    def test_missing_compose_fails_closed_but_absolute_statistics_remain(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            campaign = self.make_campaign(Path(temporary), [100.0], [200.0])
            (campaign / "queen-rust" / "fixed" / "r01" / "compose-resolved.yml").unlink()
            report = campaign_stats.build_report(campaign, seed=1, resamples=100)

        comparison = self.comparison(report)
        self.assertEqual(0, comparison["pairs_eligible"])
        reasons = comparison["paired_runs"][0]["suppression_reasons"]
        self.assertTrue(any("compose-resolved.yml" in reason for reason in reasons), reasons)
        queen_group = next(
            group for group in report["aggregates"] if group["engine"] == "queen-rust"
        )
        self.assertEqual(1, queen_group["runs_valid"])
        self.assertEqual(
            1, queen_group["metrics"]["completion_span_jobs_per_second"]["n"]
        )

    def test_resource_budget_mismatch_is_enumerated_and_suppresses_ratio(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            campaign = self.make_campaign(Path(temporary), [100.0], [200.0])
            path = campaign / "queen-rust" / "fixed" / "r01" / "compose-resolved.yml"
            path.write_text(self.compose("queen-rust", backend_cpu=4.0), encoding="utf-8")
            report = campaign_stats.build_report(campaign, seed=1, resamples=100)

        comparison = self.comparison(report)
        self.assertEqual(0, comparison["pairs_eligible"])
        reasons = comparison["paired_runs"][0]["suppression_reasons"]
        self.assertTrue(
            any("resources.backend_budget.cpus" in reason for reason in reasons), reasons
        )

    def test_sampler_setting_mismatch_fails_closed_for_that_pair(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            campaign = self.make_campaign(Path(temporary), [100.0], [200.0])
            path = campaign / "queen-rust" / "fixed" / "r01" / "summary.json"
            summary = json.loads(path.read_text(encoding="utf-8"))
            summary["resources"]["stats_integrity"]["expected_interval_ns"] = 250_000_000
            path.write_text(json.dumps(summary) + "\n", encoding="utf-8")
            report = campaign_stats.build_report(campaign, seed=1, resamples=100)

        comparison = self.comparison(report)
        self.assertEqual(0, comparison["pairs_eligible"])
        reasons = comparison["paired_runs"][0]["suppression_reasons"]
        self.assertTrue(
            any("sampler interval disagrees" in reason for reason in reasons), reasons
        )

    def test_metric_eligibility_counts_exclude_metric_errors(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            campaign = self.make_campaign(Path(temporary), [100.0], [200.0])
            path = campaign / "queen-rust" / "fixed" / "r01" / "summary.json"
            summary = json.loads(path.read_text(encoding="utf-8"))
            del summary["latency"]["end_to_end"]["p99_ms"]
            path.write_text(json.dumps(summary) + "\n", encoding="utf-8")
            report = campaign_stats.build_report(campaign, seed=1, resamples=100)

        comparison = self.comparison(report)
        self.assertEqual(1, comparison["pairs_eligible"])
        self.assertEqual(
            1, comparison["metrics"]["completion_span_jobs_per_second"]["pairs_eligible"]
        )
        self.assertEqual(0, comparison["metrics"]["end_to_end_p99_ms"]["pairs_eligible"])
        self.assertEqual(1, comparison["metrics"]["end_to_end_p99_ms"]["pairs_suppressed"])

    def test_configuration_drift_suppresses_multi_run_absolute_aggregate(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            campaign = self.make_campaign(
                Path(temporary), [100.0, 100.0], [200.0, 210.0]
            )
            path = campaign / "queen-rust" / "fixed" / "r02" / "configuration.json"
            configuration = json.loads(path.read_text(encoding="utf-8"))
            configuration["benchmark"]["worker_sleep"] = 2
            path.write_text(json.dumps(configuration) + "\n", encoding="utf-8")
            report = campaign_stats.build_report(campaign, seed=1, resamples=100)

        queen_group = next(
            group for group in report["aggregates"] if group["engine"] == "queen-rust"
        )
        self.assertTrue(queen_group["aggregate_suppressed"])
        self.assertEqual(0, queen_group["runs_aggregated"])
        self.assertEqual(
            0, queen_group["metrics"]["completion_span_jobs_per_second"]["n"]
        )

    def test_cli_writes_json_and_markdown_with_method_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            campaign = self.make_campaign(root, [100.0], [200.0])
            json_output = root / "out" / "stats.json"
            markdown_output = root / "out" / "stats.md"
            status = campaign_stats.main(
                [
                    str(campaign),
                    "--json-output",
                    str(json_output),
                    "--markdown-output",
                    str(markdown_output),
                    "--seed",
                    "123",
                    "--resamples",
                    "100",
                ]
            )
            payload = json.loads(json_output.read_text(encoding="utf-8"))
            markdown = markdown_output.read_text(encoding="utf-8")

        self.assertEqual(0, status)
        self.assertEqual(123, payload["methodology"]["bootstrap"]["seed"])
        self.assertEqual(100, payload["methodology"]["bootstrap"]["resamples"])
        self.assertIn("Every run", markdown)
        self.assertIn("Paired ratios versus Horizon", markdown)


if __name__ == "__main__":
    unittest.main()

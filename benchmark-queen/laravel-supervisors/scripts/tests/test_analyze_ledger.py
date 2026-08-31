from __future__ import annotations

import importlib.util
import io
import json
import sqlite3
import sys
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from types import SimpleNamespace


SCRIPT = Path(__file__).resolve().parents[1] / "analyze.py"
SPEC = importlib.util.spec_from_file_location(
    "laravel_supervisor_analyze_ledger", SCRIPT
)
assert SPEC is not None and SPEC.loader is not None
analyze = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = analyze
SPEC.loader.exec_module(analyze)


class EffectLedgerGateTest(unittest.TestCase):
    def make_ledger(
        self,
        root: Path,
        *,
        retried: bool = False,
        open_attempt: bool = False,
        include_effect: bool = True,
    ) -> tuple[dict, dict[str, list[dict]]]:
        run_id = "ledger-run"
        manifest = {"run_id": run_id, "jobs": 1, "ledger_mode": "durable"}
        (root / "dispatch.json").write_text(
            json.dumps(manifest) + "\n", encoding="utf-8"
        )
        database = sqlite3.connect(root / "ledger.sqlite3")
        database.executescript(
            """
            PRAGMA foreign_keys = ON;
            CREATE TABLE ledger_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID;
            CREATE TABLE attempts (
                attempt_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                job_id TEXT NOT NULL,
                attempt_number INTEGER NOT NULL,
                worker_pid INTEGER NOT NULL,
                worker_host TEXT NOT NULL,
                started_at_ns INTEGER NOT NULL,
                effect_outcome TEXT,
                observed_effect_id TEXT,
                effect_observed_at_ns INTEGER,
                outcome TEXT,
                outcome_at_ns INTEGER,
                error_class TEXT
            );
            CREATE TABLE effects (
                effect_id TEXT NOT NULL UNIQUE,
                run_id TEXT NOT NULL,
                job_id TEXT NOT NULL,
                created_by_attempt_id TEXT NOT NULL REFERENCES attempts(attempt_id),
                checksum TEXT NOT NULL,
                committed_at_ns INTEGER NOT NULL,
                PRIMARY KEY (run_id, job_id)
            ) WITHOUT ROWID;
            """
        )
        database.executemany(
            "INSERT INTO ledger_meta (key, value) VALUES (?, ?)",
            [
                ("schema", analyze.LEDGER_SCHEMA),
                ("run_id", run_id),
                ("semantics", "fixture-local"),
            ],
        )
        checksum = "ab" * 32
        attempt_id = "1" * 32
        effect_id = "2" * 32
        database.execute(
            "INSERT INTO attempts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                attempt_id,
                run_id,
                "000000000",
                1,
                42,
                "fixture",
                100,
                "created" if include_effect else None,
                effect_id if include_effect else None,
                200 if include_effect else None,
                None if open_attempt or retried else "completed",
                None if open_attempt or retried else 300,
                None,
            ),
        )
        if include_effect:
            database.execute(
                "INSERT INTO effects VALUES (?, ?, ?, ?, ?, ?)",
                (effect_id, run_id, "000000000", attempt_id, checksum, 200),
            )
        completion_attempt_id = attempt_id
        completion_outcome = "created"
        completion_created = True
        if retried:
            retry_attempt = "3" * 32
            database.execute(
                "INSERT INTO attempts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    retry_attempt,
                    run_id,
                    "000000000",
                    2,
                    43,
                    "fixture",
                    400,
                    "already_present",
                    effect_id,
                    500,
                    "completed",
                    600,
                    None,
                ),
            )
            completion_attempt_id = retry_attempt
            completion_outcome = "already_present"
            completion_created = False
        database.commit()
        database.close()
        completion = {
            "run_id": run_id,
            "job_id": "000000000",
            "checksum": checksum,
            "ledger_attempt_id": completion_attempt_id,
            "ledger_effect_id": effect_id,
            "ledger_effect_outcome": completion_outcome,
            "ledger_effect_created": completion_created,
        }
        return manifest, {"000000000": [completion]}

    def test_exact_conservation_and_duplicate_gate_pass(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            manifest, completions = self.make_ledger(root)
            result = analyze.effect_ledger_summary(root, manifest, completions, 1)

        self.assertTrue(result["conservation_pass"])
        self.assertTrue(result["idempotent_effect_pass"])
        self.assertTrue(result["no_duplicate_side_effects_pass"])
        self.assertTrue(result["attempt_integrity_pass"])
        self.assertTrue(result["strict_execution_pass"])
        self.assertTrue(result["gate_passed"])
        self.assertFalse(result["exactly_once_claim"])

    def test_multi_queue_manifest_uses_queue_prefixed_expected_ids(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            manifest, completions = self.make_ledger(root)
            manifest.update(
                {
                    "jobs": 2,
                    "jobs_per_queue": 1,
                    "queues_csv": "critical,default",
                    "dispatch_mode": "round-robin-single",
                }
            )
            (root / "dispatch.json").write_text(
                json.dumps(manifest) + "\n", encoding="utf-8"
            )

            first = completions.pop("000000000")[0]
            first["job_id"] = "critical:000000000"
            completions[first["job_id"]] = [first]
            database = sqlite3.connect(root / "ledger.sqlite3")
            database.execute(
                "UPDATE attempts SET job_id = ? WHERE attempt_id = ?",
                ("critical:000000000", first["ledger_attempt_id"]),
            )
            database.execute(
                "UPDATE effects SET job_id = ? WHERE effect_id = ?",
                ("critical:000000000", first["ledger_effect_id"]),
            )

            second_attempt = "3" * 32
            second_effect = "4" * 32
            checksum = "cd" * 32
            database.execute(
                "INSERT INTO attempts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    second_attempt,
                    manifest["run_id"],
                    "default:000000000",
                    1,
                    43,
                    "fixture",
                    400,
                    "created",
                    second_effect,
                    500,
                    "completed",
                    600,
                    None,
                ),
            )
            database.execute(
                "INSERT INTO effects VALUES (?, ?, ?, ?, ?, ?)",
                (
                    second_effect,
                    manifest["run_id"],
                    "default:000000000",
                    second_attempt,
                    checksum,
                    500,
                ),
            )
            database.commit()
            database.close()
            completions["default:000000000"] = [
                {
                    "run_id": manifest["run_id"],
                    "job_id": "default:000000000",
                    "checksum": checksum,
                    "ledger_attempt_id": second_attempt,
                    "ledger_effect_id": second_effect,
                    "ledger_effect_outcome": "created",
                    "ledger_effect_created": True,
                }
            ]

            result = analyze.effect_ledger_summary(root, manifest, completions, 2)

        self.assertTrue(result["gate_passed"])
        self.assertEqual(0, result["effects"]["missing"]["count"])
        self.assertEqual(0, result["effects"]["unexpected"]["count"])
        expected_ids, errors = analyze.expected_job_ids(manifest)
        self.assertEqual({"critical:000000000", "default:000000000"}, expected_ids)
        self.assertEqual([], errors)

    def test_multi_queue_manifest_count_mismatch_fails_closed(self) -> None:
        expected_ids, errors = analyze.expected_job_ids(
            {
                "jobs": 3,
                "jobs_per_queue": 1,
                "queues_csv": "critical,default",
                "dispatch_mode": "round-robin-single",
            }
        )

        self.assertEqual(set(), expected_ids)
        self.assertIn(
            "dispatch.jobs does not equal jobs_per_queue multiplied by the queue count",
            errors,
        )

    def test_weighted_multi_queue_manifest_uses_declared_counts(self) -> None:
        expected_ids, errors = analyze.expected_job_ids(
            {
                "jobs": 4,
                "jobs_per_queue": None,
                "jobs_by_queue": {"critical": 3, "default": 1},
                "queues_csv": "critical,default",
                "dispatch_mode": "weighted-round-robin-single",
            }
        )

        self.assertEqual(
            {
                "critical:000000000",
                "critical:000000001",
                "critical:000000002",
                "default:000000000",
            },
            expected_ids,
        )
        self.assertEqual([], errors)

    def test_weighted_multi_queue_manifest_fails_on_count_drift(self) -> None:
        expected_ids, errors = analyze.expected_job_ids(
            {
                "jobs": 5,
                "jobs_per_queue": None,
                "jobs_by_queue": {"critical": 3, "default": 1},
                "queues_csv": "critical,default",
                "dispatch_mode": "weighted-round-robin-single",
            }
        )

        self.assertEqual(set(), expected_ids)
        self.assertIn(
            "dispatch.jobs does not equal the sum of per-queue counts",
            errors,
        )

    def test_retry_is_observed_as_dedup_without_duplicate_effect(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            manifest, completions = self.make_ledger(root, retried=True)
            result = analyze.effect_ledger_summary(
                root, manifest, completions, 1, allow_open_attempts=True
            )

        self.assertTrue(result["conservation_pass"])
        self.assertTrue(result["idempotent_effect_pass"])
        self.assertTrue(result["no_duplicate_side_effects_pass"])
        self.assertEqual(1, result["attempts"]["already_present"]["count"])
        self.assertEqual(1, result["attempts"]["duplicate_executions"]["count"])
        self.assertFalse(result["strict_execution_pass"])
        self.assertFalse(result["gate_passed"])

    def test_missing_effect_fails_conservation(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            manifest, completions = self.make_ledger(root, include_effect=False)
            result = analyze.effect_ledger_summary(root, manifest, completions, 1)

        self.assertFalse(result["conservation_pass"])
        self.assertEqual(1, result["effects"]["missing"]["count"])

    def test_unexpected_attempt_fails_attempt_integrity_and_fault_gate(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            manifest, completions = self.make_ledger(root)
            database = sqlite3.connect(root / "ledger.sqlite3")
            attempt_id = "4" * 32
            effect_id = "5" * 32
            database.execute(
                "INSERT INTO attempts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    attempt_id,
                    manifest["run_id"],
                    "unexpected-job",
                    1,
                    44,
                    "fixture",
                    700,
                    "created",
                    effect_id,
                    800,
                    "completed",
                    900,
                    None,
                ),
            )
            database.execute(
                "INSERT INTO effects VALUES (?, ?, ?, ?, ?, ?)",
                (
                    effect_id,
                    manifest["run_id"],
                    "unexpected-job",
                    attempt_id,
                    "ef" * 32,
                    800,
                ),
            )
            database.commit()
            database.close()

            # Fault verification permits an interrupted expected attempt, but
            # it must never permit an execution outside the dispatch job set.
            result = analyze.effect_ledger_summary(
                root,
                manifest,
                completions,
                1,
                allow_open_attempts=True,
            )

        self.assertFalse(result["attempt_integrity_pass"])
        self.assertFalse(result["gate_passed"])
        self.assertEqual(1, result["attempts"]["unexpected_jobs"]["count"])

    def test_open_attempt_is_allowed_only_for_fault_verification(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            manifest, completions = self.make_ledger(root, open_attempt=True)
            strict = analyze.effect_ledger_summary(root, manifest, completions, 1)
            fault = analyze.effect_ledger_summary(
                root,
                manifest,
                completions,
                1,
                allow_open_attempts=True,
            )

        self.assertFalse(strict["attempt_integrity_pass"])
        self.assertTrue(fault["attempt_integrity_pass"])
        self.assertEqual(1, fault["attempts"]["open_or_interrupted"]["count"])

    def test_absent_legacy_ledger_is_not_silently_required(self) -> None:
        result = analyze.effect_ledger_summary(Path("/does/not/exist"), {}, {}, 1)
        self.assertEqual("not_requested", result["status"])
        self.assertTrue(result["gate_passed"])
        self.assertIsNone(result["conservation_pass"])

    def test_cli_expected_must_match_the_dispatch_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.make_ledger(root)
            stderr = io.StringIO()
            with redirect_stderr(stderr):
                status = analyze.ledger_command(
                    SimpleNamespace(run_directory=str(root), expected=2)
                )

        self.assertEqual(2, status)
        self.assertIn("--expected=2 does not match dispatch.jobs=1", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()

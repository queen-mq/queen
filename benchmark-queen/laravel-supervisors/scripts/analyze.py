#!/usr/bin/env python3
"""Wait for, summarize and compare Laravel supervisor benchmark runs.

Only the Python standard library is used. JSONL readers retain complete records
when a killed container or full disk leaves an incomplete final line, but both
partial and malformed lines are counted and make primary correctness fail.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sqlite3
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence


SUMMARY_SCHEMA = "queen.laravel-supervisors.summary/v1"
REPORT_SCHEMA = "queen.laravel-supervisors.report/v1"
QUEUE_STATE_SCHEMA = "queen.laravel-supervisors.queue-state/v1"
LEDGER_SCHEMA = "queen.laravel-supervisors.effect-ledger/v1"
FAILURE_WORDS = {"failed", "failure", "error", "exception", "dead", "timeout"}
ROLE_PRIORITY = {"worker": 5, "orchestrator": 4, "app": 3, "backend": 2, "stack": 1}
MAX_DISPATCH_JOBS = 1_000_000


@dataclass
class JsonlSnapshot:
    records: list[dict[str, Any]]
    malformed_lines: int = 0
    partial_lines: int = 0
    unreadable_files: int = 0

    def merge(self, other: "JsonlSnapshot") -> None:
        self.records.extend(other.records)
        self.malformed_lines += other.malformed_lines
        self.partial_lines += other.partial_lines
        self.unreadable_files += other.unreadable_files


def nonnegative_float(value: str) -> float:
    number = float(value)
    if not 0 <= number <= 604_800:
        raise argparse.ArgumentTypeError("must be between 0 and 604800 seconds")
    return number


def positive_float(value: str) -> float:
    number = float(value)
    if not 0.01 <= number <= 60:
        raise argparse.ArgumentTypeError("must be between 0.01 and 60 seconds")
    return number


def nonnegative_int(value: str) -> int:
    if not value.isascii() or not value.isdigit():
        raise argparse.ArgumentTypeError("must be a non-negative integer")
    return int(value)


def integer(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def nonnegative_integer(value: Any) -> int | None:
    result = integer(value)
    return result if result is not None and result >= 0 else None


def load_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise ValueError(f"missing JSON file: {path}") from None
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exception:
        raise ValueError(f"cannot read JSON file {path}: {exception}") from exception
    if not isinstance(value, dict):
        raise ValueError(f"JSON file is not an object: {path}")
    return value


def read_jsonl(path: Path) -> JsonlSnapshot:
    result = JsonlSnapshot([])
    try:
        stream = path.open("rb")
    except (FileNotFoundError, PermissionError, OSError):
        result.unreadable_files = 1
        return result

    try:
        while True:
            try:
                raw = stream.readline()
            except OSError:
                result.unreadable_files += 1
                break
            if raw == b"":
                break
            complete = raw.endswith(b"\n")
            if not raw.strip():
                continue
            try:
                value = json.loads(raw)
            except (UnicodeDecodeError, json.JSONDecodeError):
                if not complete:
                    result.partial_lines += 1
                else:
                    result.malformed_lines += 1
                continue
            if not isinstance(value, dict):
                result.malformed_lines += 1
                continue
            result.records.append(value)
    finally:
        stream.close()
    return result


def read_events(run_directory: Path) -> JsonlSnapshot:
    result = JsonlSnapshot([])
    events_directory = run_directory / "events"
    try:
        paths = sorted(events_directory.glob("*.jsonl"))
    except OSError:
        result.unreadable_files = 1
        return result
    for path in paths:
        result.merge(read_jsonl(path))
    return result


def read_stats(run_directory: Path) -> JsonlSnapshot:
    path = run_directory / "stats.jsonl"
    if not path.exists():
        return JsonlSnapshot([])
    return read_jsonl(path)


def is_failure(record: dict[str, Any]) -> bool:
    if record.get("success") is False:
        return True
    for key in ("status", "event", "type"):
        value = record.get(key)
        if not isinstance(value, str):
            continue
        words = {part for part in value.lower().replace("-", "_").split("_") if part}
        if words & FAILURE_WORDS:
            return True
    return nonnegative_integer(record.get("failed_at_ns")) is not None


def completion_record(record: dict[str, Any]) -> bool:
    return (
        not is_failure(record)
        and nonnegative_integer(record.get("completed_at_ns")) is not None
    )


def expected_job_ids(manifest: dict[str, Any]) -> tuple[set[str], list[str]]:
    """Resolve the exact job-id set declared by a dispatch manifest."""

    errors: list[str] = []
    expected = nonnegative_integer(manifest.get("jobs"))
    if expected is None:
        return set(), ["dispatch.jobs must be a non-negative integer"]
    if expected > MAX_DISPATCH_JOBS:
        return set(), [f"dispatch.jobs may not exceed {MAX_DISPATCH_JOBS}"]

    jobs_per_queue_raw = manifest.get("jobs_per_queue")
    queues_csv_raw = manifest.get("queues_csv")
    multi_queue = jobs_per_queue_raw is not None or queues_csv_raw is not None
    if not multi_queue:
        return {f"{index:09d}" for index in range(expected)}, errors

    jobs_per_queue = nonnegative_integer(jobs_per_queue_raw)
    if jobs_per_queue in {None, 0}:
        errors.append("dispatch.jobs_per_queue must be a positive integer")
    elif jobs_per_queue > MAX_DISPATCH_JOBS:
        errors.append(f"dispatch.jobs_per_queue may not exceed {MAX_DISPATCH_JOBS}")

    if manifest.get("dispatch_mode") != "round-robin-single":
        errors.append(
            "a multi-queue dispatch manifest must use dispatch_mode round-robin-single"
        )

    queues: list[str] = []
    if not isinstance(queues_csv_raw, str) or not queues_csv_raw:
        errors.append("dispatch.queues_csv must be a non-empty queue CSV")
    else:
        queues = queues_csv_raw.split(",")
        if len(queues) < 2:
            errors.append("dispatch.queues_csv must contain at least two queues")
        if len(queues) > 256:
            errors.append("dispatch.queues_csv may not contain more than 256 queues")
        seen: set[str] = set()
        for queue in queues:
            valid = (
                0 < len(queue) <= 118
                and queue.isascii()
                and all(
                    character.isalnum() or character in "._:-" for character in queue
                )
            )
            if not valid:
                errors.append(f"dispatch.queues_csv contains invalid queue {queue!r}")
            elif queue in seen:
                errors.append(f"dispatch.queues_csv contains duplicate queue {queue!r}")
            seen.add(queue)

    if jobs_per_queue is not None and jobs_per_queue > 0 and queues:
        declared = jobs_per_queue * len(queues)
        if declared != expected:
            errors.append(
                "dispatch.jobs does not equal jobs_per_queue multiplied by the queue count"
            )

    if errors or jobs_per_queue is None:
        return set(), errors
    return {
        f"{queue}:{index:09d}" for queue in queues for index in range(jobs_per_queue)
    }, errors


def event_snapshot(run_directory: Path, manifest: dict[str, Any]) -> dict[str, Any]:
    run_id = manifest.get("run_id")
    expected = nonnegative_integer(manifest.get("jobs"))
    expected_ids, manifest_errors = expected_job_ids(manifest)
    raw = read_events(run_directory)
    completions: dict[str, list[dict[str, Any]]] = defaultdict(list)
    failures: dict[str, list[dict[str, Any]]] = defaultdict(list)
    foreign = 0
    invalid = 0

    for record in raw.records:
        if isinstance(run_id, str) and record.get("run_id") != run_id:
            foreign += 1
            continue
        job_id = record.get("job_id")
        if not isinstance(job_id, str) or not job_id:
            invalid += 1
            continue
        if is_failure(record):
            failures[job_id].append(record)
        if completion_record(record):
            completions[job_id].append(record)
        elif not is_failure(record):
            invalid += 1

    selected: dict[str, dict[str, Any]] = {}
    for job_id, records in completions.items():
        records.sort(
            key=lambda item: (
                nonnegative_integer(item.get("completed_at_ns"))
                if nonnegative_integer(item.get("completed_at_ns")) is not None
                else sys.maxsize
            )
        )
        selected[job_id] = records[0]

    expected_completed = 0
    unexpected_job_ids: list[str] = []
    if expected is not None and not manifest_errors:
        for job_id in selected:
            if job_id in expected_ids:
                expected_completed += 1
            else:
                unexpected_job_ids.append(job_id)
    else:
        expected_completed = len(selected)

    return {
        "raw": raw,
        "run_id": run_id,
        "expected": expected,
        "completions": completions,
        "selected": selected,
        "failures": failures,
        "foreign": foreign,
        "invalid": invalid,
        "expected_completed": expected_completed,
        "expected_job_ids": expected_ids,
        "manifest_validation_errors": manifest_errors,
        "unexpected_job_ids": sorted(unexpected_job_ids),
    }


def nearest_rank(
    sorted_values: Sequence[int | float], percentile: float
) -> int | float | None:
    if not sorted_values:
        return None
    index = max(0, math.ceil(len(sorted_values) * percentile) - 1)
    return sorted_values[index]


def distribution(values: Iterable[int | float]) -> dict[str, int | float | None]:
    numbers = sorted(
        value for value in values if not isinstance(value, bool) and value >= 0
    )
    if not numbers:
        return {
            "count": 0,
            "min": None,
            "mean": None,
            "p50": None,
            "p95": None,
            "p99": None,
            "max": None,
        }
    return {
        "count": len(numbers),
        "min": numbers[0],
        "mean": sum(numbers) / len(numbers),
        "p50": nearest_rank(numbers, 0.50),
        "p95": nearest_rank(numbers, 0.95),
        "p99": nearest_rank(numbers, 0.99),
        "max": numbers[-1],
    }


def latency_distribution(values: Iterable[int]) -> dict[str, int | float | None]:
    result = distribution(values)
    output: dict[str, int | float | None] = {}
    for key, value in result.items():
        if key == "count":
            output[key] = value
        else:
            output[f"{key}_ns"] = value
            output[f"{key}_ms"] = value / 1_000_000 if value is not None else None
    return output


def duration_rate(count: int, duration_ns: int | None) -> float | None:
    if duration_ns is None or duration_ns <= 0:
        return None
    return count * 1_000_000_000 / duration_ns


def valid_samples(stats: JsonlSnapshot) -> tuple[list[dict[str, Any]], int]:
    invalid = 0
    indexed: dict[tuple[int, int], dict[str, Any]] = {}
    for order, record in enumerate(stats.records):
        if record.get("type") != "sample":
            continue
        timestamp = nonnegative_integer(record.get("monotonic_ns"))
        if timestamp is None or not isinstance(record.get("targets"), list):
            invalid += 1
            continue
        sequence = integer(record.get("sequence"))
        # A second copy of the same sequence is ignored rather than inflating
        # memory percentiles. Order disambiguates old/custom samplers.
        key = (timestamp, sequence if sequence is not None else order)
        indexed[key] = record
    return [indexed[key] for key in sorted(indexed)], invalid


def sample_time(sample: dict[str, Any]) -> int:
    return int(sample["monotonic_ns"])


def bracketed_samples(
    samples: list[dict[str, Any]], start_ns: int | None, end_ns: int | None
) -> list[dict[str, Any]]:
    if not samples:
        return []
    if start_ns is None and end_ns is None:
        return samples

    selected: list[dict[str, Any]] = []
    before: dict[str, Any] | None = None
    after: dict[str, Any] | None = None
    for sample in samples:
        timestamp = sample_time(sample)
        if start_ns is not None and timestamp < start_ns:
            before = sample
            continue
        if end_ns is not None and timestamp > end_ns:
            after = sample
            break
        selected.append(sample)
    if before is not None:
        selected.insert(0, before)
    if after is not None:
        selected.append(after)
    return selected


def process_rows(sample: dict[str, Any]) -> dict[tuple[int, Any], dict[str, Any]]:
    rows: dict[tuple[int, Any], dict[str, Any]] = {}
    for target in sample.get("targets", []):
        if not isinstance(target, dict) or not isinstance(
            target.get("processes"), list
        ):
            continue
        for process in target["processes"]:
            if not isinstance(process, dict):
                continue
            pid = nonnegative_integer(process.get("pid"))
            if pid is None:
                continue
            key = (pid, process.get("start_ticks"))
            existing = rows.get(key)
            if existing is None or ROLE_PRIORITY.get(
                str(process.get("role")), 0
            ) > ROLE_PRIORITY.get(str(existing.get("role")), 0):
                rows[key] = process
    return rows


def tracked_delta(track: dict[str, Any], key: str) -> int:
    maximum = track.get(f"max_{key}")
    baseline = track.get(f"baseline_{key}")
    if not isinstance(maximum, int) or not isinstance(baseline, int):
        return 0
    return max(0, maximum - baseline)


def process_resource_summary(
    selected: list[dict[str, Any]],
    requested_start_ns: int | None,
    requested_end_ns: int | None,
) -> dict[str, dict[str, Any]]:
    tracks: dict[tuple[int, Any], dict[str, Any]] = {}
    memory_series: dict[str, dict[str, list[int]]] = defaultdict(
        lambda: {
            "rss": [],
            "pss": [],
            "private": [],
            "count": [],
            "pss_count": [],
            "private_count": [],
        }
    )

    for index, sample in enumerate(selected):
        timestamp = sample_time(sample)
        inside = (requested_start_ns is None or timestamp >= requested_start_ns) and (
            requested_end_ns is None or timestamp <= requested_end_ns
        )
        per_role: dict[str, dict[str, int]] = defaultdict(
            lambda: {
                "rss": 0,
                "pss": 0,
                "private": 0,
                "count": 0,
                "pss_count": 0,
                "private_count": 0,
            }
        )
        for identity, process in process_rows(sample).items():
            role = str(process.get("role", "app"))
            schedstat = process.get("schedstat")
            runtime = None
            wait = None
            if isinstance(schedstat, dict):
                runtime = nonnegative_integer(schedstat.get("runtime_ns"))
                wait = nonnegative_integer(schedstat.get("runqueue_wait_ns"))

            track = tracks.get(identity)
            if track is None:
                at_baseline = (
                    index == 0
                    if requested_start_ns is None
                    else timestamp <= requested_start_ns
                )
                track = {"role": role}
                if runtime is not None:
                    track["baseline_runtime"] = runtime if at_baseline else 0
                    track["max_runtime"] = runtime
                if wait is not None:
                    track["baseline_wait"] = wait if at_baseline else 0
                    track["max_wait"] = wait
                tracks[identity] = track
            else:
                if ROLE_PRIORITY.get(role, 0) > ROLE_PRIORITY.get(
                    str(track["role"]), 0
                ):
                    track["role"] = role
                if runtime is not None:
                    track["max_runtime"] = max(
                        runtime, int(track.get("max_runtime", runtime))
                    )
                if wait is not None:
                    track["max_wait"] = max(wait, int(track.get("max_wait", wait)))

            if not inside:
                continue
            bucket = per_role[role]
            bucket["count"] += 1
            rss = nonnegative_integer(process.get("rss_bytes"))
            pss = nonnegative_integer(process.get("pss_bytes"))
            private = nonnegative_integer(process.get("private_bytes"))
            if rss is not None:
                bucket["rss"] += rss
            if pss is not None:
                bucket["pss"] += pss
                bucket["pss_count"] += 1
            if private is not None:
                bucket["private"] += private
                bucket["private_count"] += 1

        if inside:
            for role in ("orchestrator", "worker", "app", "backend", "stack"):
                bucket = per_role[role]
                memory_series[role]["count"].append(bucket["count"])
                memory_series[role]["pss_count"].append(bucket["pss_count"])
                memory_series[role]["private_count"].append(bucket["private_count"])
                memory_series[role]["rss"].append(bucket["rss"])
                if bucket["pss_count"]:
                    memory_series[role]["pss"].append(bucket["pss"])
                if bucket["private_count"]:
                    memory_series[role]["private"].append(bucket["private"])

    if len(selected) >= 2:
        duration_ns = sample_time(selected[-1]) - sample_time(selected[0])
    else:
        duration_ns = 0
    output: dict[str, dict[str, Any]] = {}
    for role in ("orchestrator", "worker", "app", "backend", "stack"):
        runtime_ns = sum(
            tracked_delta(track, "runtime")
            for track in tracks.values()
            if track["role"] == role
        )
        wait_ns = sum(
            tracked_delta(track, "wait")
            for track in tracks.values()
            if track["role"] == role
        )
        cpu_cores = runtime_ns / duration_ns if duration_ns > 0 else None
        process_observations = sum(memory_series[role]["count"])
        pss_observations = sum(memory_series[role]["pss_count"])
        private_observations = sum(memory_series[role]["private_count"])
        output[role] = {
            "scope": "processes",
            "observed_processes": sum(
                1 for track in tracks.values() if track["role"] == role
            ),
            "processes_peak": max(memory_series[role]["count"], default=0),
            "cpu_seconds": runtime_ns / 1_000_000_000,
            "cpu_average_cores": cpu_cores,
            "cpu_average_percent": cpu_cores * 100 if cpu_cores is not None else None,
            "runqueue_wait_seconds": wait_ns / 1_000_000_000,
            "rss_bytes": distribution(memory_series[role]["rss"]),
            "pss_bytes": distribution(memory_series[role]["pss"]),
            "pss_coverage": pss_observations / process_observations
            if process_observations
            else None,
            "private_bytes": distribution(memory_series[role]["private"]),
            "private_coverage": private_observations / process_observations
            if process_observations
            else None,
        }
    return output


def nested_nonnegative(mapping: Any, *keys: str) -> int | None:
    value = mapping
    for key in keys:
        if not isinstance(value, dict):
            return None
        value = value.get(key)
    return nonnegative_integer(value)


def cgroup_resource_summary(
    selected: list[dict[str, Any]],
    requested_start_ns: int | None,
    requested_end_ns: int | None,
) -> dict[str, dict[str, Any]]:
    tracks: dict[tuple[str, Any], dict[str, Any]] = {}
    series: dict[str, dict[str, list[int]]] = defaultdict(
        lambda: {"memory": [], "pids": [], "reported_peak": []}
    )

    for index, sample in enumerate(selected):
        timestamp = sample_time(sample)
        inside = (requested_start_ns is None or timestamp >= requested_start_ns) and (
            requested_end_ns is None or timestamp <= requested_end_ns
        )
        totals: dict[str, dict[str, int]] = defaultdict(
            lambda: {
                "memory": 0,
                "pids": 0,
                "reported_peak": 0,
                "memory_count": 0,
                "pids_count": 0,
                "peak_count": 0,
            }
        )
        for target in sample.get("targets", []):
            if not isinstance(target, dict):
                continue
            kind = str(target.get("kind", "app"))
            if kind not in {"app", "backend", "stack"}:
                kind = "app"
            label = str(target.get("label", "unknown"))
            cgroup = target.get("cgroup")
            if not isinstance(cgroup, dict):
                continue
            identity = (label, cgroup.get("inode"))
            at_baseline = (
                index == 0
                if requested_start_ns is None
                else timestamp <= requested_start_ns
            )
            track = tracks.get(identity)
            if track is None:
                track = {"kind": kind, "events": {}}
                tracks[identity] = track

            counters = {
                "usage_usec": nested_nonnegative(cgroup, "cpu", "usage_usec"),
                "user_usec": nested_nonnegative(cgroup, "cpu", "user_usec"),
                "system_usec": nested_nonnegative(cgroup, "cpu", "system_usec"),
                "throttled_usec": nested_nonnegative(cgroup, "cpu", "throttled_usec"),
                "nr_throttled": nested_nonnegative(cgroup, "cpu", "nr_throttled"),
            }
            for key, value in counters.items():
                if value is None:
                    continue
                baseline_key = f"baseline_{key}"
                maximum_key = f"max_{key}"
                if baseline_key not in track:
                    track[baseline_key] = value if at_baseline else 0
                track[maximum_key] = max(value, int(track.get(maximum_key, value)))

            events = (
                cgroup.get("memory", {}).get("events")
                if isinstance(cgroup.get("memory"), dict)
                else None
            )
            if isinstance(events, dict):
                for event, raw in events.items():
                    value = nonnegative_integer(raw)
                    if value is None:
                        continue
                    event_track = track["events"].setdefault(
                        str(event),
                        {"baseline": value if at_baseline else 0, "maximum": value},
                    )
                    event_track["maximum"] = max(value, event_track["maximum"])

            if inside:
                memory = nested_nonnegative(cgroup, "memory", "current_bytes")
                pids = nonnegative_integer(cgroup.get("pids_current"))
                reported_peak = nested_nonnegative(cgroup, "memory", "peak_bytes")
                # `stack` is the whole measured deployment, not merely an
                # optional target named stack. This makes comparisons fair
                # when Queen needs broker + Postgres while Horizon needs Redis.
                for bucket_kind in {kind, "stack"}:
                    bucket = totals[bucket_kind]
                    if memory is not None:
                        bucket["memory"] += memory
                        bucket["memory_count"] += 1
                    if pids is not None:
                        bucket["pids"] += pids
                        bucket["pids_count"] += 1
                    if reported_peak is not None:
                        bucket["reported_peak"] += reported_peak
                        bucket["peak_count"] += 1

        if inside:
            for kind in ("app", "backend", "stack"):
                if totals[kind]["memory_count"]:
                    series[kind]["memory"].append(totals[kind]["memory"])
                if totals[kind]["pids_count"]:
                    series[kind]["pids"].append(totals[kind]["pids"])
                if totals[kind]["peak_count"]:
                    series[kind]["reported_peak"].append(totals[kind]["reported_peak"])

    if len(selected) >= 2:
        duration_ns = sample_time(selected[-1]) - sample_time(selected[0])
    else:
        duration_ns = 0

    output: dict[str, dict[str, Any]] = {}
    for kind in ("app", "backend", "stack"):
        matching = (
            list(tracks.values())
            if kind == "stack"
            else [track for track in tracks.values() if track["kind"] == kind]
        )
        usage_usec = sum(tracked_delta(track, "usage_usec") for track in matching)
        user_usec = sum(tracked_delta(track, "user_usec") for track in matching)
        system_usec = sum(tracked_delta(track, "system_usec") for track in matching)
        throttled_usec = sum(
            tracked_delta(track, "throttled_usec") for track in matching
        )
        nr_throttled = sum(tracked_delta(track, "nr_throttled") for track in matching)
        event_deltas: dict[str, int] = defaultdict(int)
        for track in matching:
            for event, values in track["events"].items():
                event_deltas[event] += max(0, values["maximum"] - values["baseline"])
        cpu_cores = usage_usec * 1000 / duration_ns if duration_ns > 0 else None
        output[kind] = {
            "scope": "cgroup",
            "observed_cgroups": len(matching),
            "cpu_seconds": usage_usec / 1_000_000,
            "cpu_user_seconds": user_usec / 1_000_000,
            "cpu_system_seconds": system_usec / 1_000_000,
            "cpu_average_cores": cpu_cores,
            "cpu_average_percent": cpu_cores * 100 if cpu_cores is not None else None,
            "cpu_throttled_seconds": throttled_usec / 1_000_000,
            "cpu_nr_throttled": nr_throttled,
            "memory_current_bytes": distribution(series[kind]["memory"]),
            "memory_peak_reported_bytes": max(
                series[kind]["reported_peak"], default=None
            ),
            "pids_current": distribution(series[kind]["pids"]),
            "memory_events": dict(sorted(event_deltas.items())),
        }
    return output


def resource_window(
    samples: list[dict[str, Any]],
    start_ns: int | None,
    end_ns: int | None,
    name: str,
) -> dict[str, Any]:
    selected = bracketed_samples(samples, start_ns, end_ns)
    processes = process_resource_summary(selected, start_ns, end_ns)
    cgroups = cgroup_resource_summary(selected, start_ns, end_ns)
    observed_start = sample_time(selected[0]) if selected else None
    observed_end = sample_time(selected[-1]) if selected else None
    return {
        "window": name,
        "requested_start_ns": start_ns,
        "requested_end_ns": end_ns,
        "observed_start_ns": observed_start,
        "observed_end_ns": observed_end,
        "observed_duration_ns": observed_end - observed_start
        if observed_start is not None and observed_end is not None
        else None,
        "samples": len(selected),
        "orchestrator": processes["orchestrator"],
        "workers": processes["worker"],
        "app": cgroups["app"],
        "backend": cgroups["backend"],
        "stack": cgroups["stack"],
        "unclassified_processes": {
            role: processes[role] for role in ("app", "backend", "stack")
        },
    }


def worker_count(sample: dict[str, Any]) -> int:
    return sum(
        1
        for process in process_rows(sample).values()
        if process.get("role") == "worker"
    )


def scale_summary(
    samples: list[dict[str, Any]], start_ns: int | None, completed_ns: int | None
) -> dict[str, Any]:
    if not samples:
        return {
            "worker_peak": 0,
            "initial_workers": 0,
            "final_workers": 0,
            "time_to_first_worker_ns": None,
            "time_to_peak_workers_ns": None,
            "first_scale_down_ns": None,
            "drained_after_completion_ns": None,
            "return_to_initial_after_completion_ns": None,
            "worker_seconds": 0.0,
            "changes": [],
        }
    origin = start_ns if start_ns is not None else sample_time(samples[0])
    relevant = [sample for sample in samples if sample_time(sample) >= origin]
    if not relevant:
        relevant = samples
        origin = sample_time(samples[0])
    points = [(sample_time(sample), worker_count(sample)) for sample in relevant]
    peak = max((count for _timestamp, count in points), default=0)
    first_worker = next((timestamp for timestamp, count in points if count > 0), None)
    peak_at = (
        next((timestamp for timestamp, count in points if count == peak), None)
        if peak
        else None
    )
    scale_down = None
    if peak_at is not None:
        scale_down = next(
            (
                timestamp
                for timestamp, count in points
                if timestamp > peak_at and count < peak
            ),
            None,
        )
    drained = None
    if completed_ns is not None and peak:
        drained = next(
            (
                timestamp
                for timestamp, count in points
                if timestamp >= completed_ns and count == 0
            ),
            None,
        )
    initial_workers = points[0][1] if points else 0
    returned_to_initial = None
    if completed_ns is not None and peak > initial_workers:
        returned_to_initial = next(
            (
                timestamp
                for timestamp, count in points
                if timestamp >= completed_ns and count <= initial_workers
            ),
            None,
        )
    worker_seconds = 0.0
    for (left_time, left_count), (right_time, right_count) in zip(points, points[1:]):
        worker_seconds += ((left_count + right_count) / 2) * (
            (right_time - left_time) / 1_000_000_000
        )
    changes: list[dict[str, int]] = []
    previous = None
    for timestamp, count in points:
        if count != previous:
            changes.append({"offset_ns": timestamp - origin, "workers": count})
            previous = count
    return {
        "worker_peak": peak,
        "initial_workers": initial_workers,
        "final_workers": points[-1][1] if points else 0,
        "time_to_first_worker_ns": first_worker - origin
        if first_worker is not None
        else None,
        "time_to_peak_workers_ns": peak_at - origin if peak_at is not None else None,
        "first_scale_down_ns": scale_down - origin if scale_down is not None else None,
        "drained_after_completion_ns": drained - completed_ns
        if drained is not None and completed_ns is not None
        else None,
        "return_to_initial_after_completion_ns": returned_to_initial - completed_ns
        if returned_to_initial is not None and completed_ns is not None
        else None,
        "worker_seconds": worker_seconds,
        "changes": changes,
    }


def limited_ids(values: Iterable[str], maximum: int) -> dict[str, Any]:
    ordered = sorted(values)
    return {
        "count": len(ordered),
        "ids": ordered[:maximum],
        "truncated": len(ordered) > maximum,
    }


def final_queue_state(run_directory: Path, manifest: dict[str, Any]) -> dict[str, Any]:
    path = run_directory / "queue-state.final.json"
    errors: list[str] = []
    try:
        raw = load_object(path)
    except ValueError as exception:
        return {
            "artifact": str(path),
            "artifact_valid": False,
            "quiescent": False,
            "gate_passed": False,
            "state": {"size": None, "ready": None, "reserved": None, "delayed": None},
            "supported": {"ready": None, "reserved": None, "delayed": None},
            "validation_errors": [str(exception)],
        }

    if raw.get("schema") != QUEUE_STATE_SCHEMA:
        errors.append(f"schema must be {QUEUE_STATE_SCHEMA}")
    for key in ("run_id", "connection", "queue"):
        expected = manifest.get(key)
        actual = raw.get(key)
        if not isinstance(actual, str) or not actual:
            errors.append(f"{key} must be a non-empty string")
        elif isinstance(expected, str) and actual != expected:
            errors.append(f"{key} does not match dispatch manifest")
    if not isinstance(raw.get("implementation"), str) or not raw["implementation"]:
        errors.append("implementation must be a non-empty string")

    state = raw.get("state")
    if not isinstance(state, dict):
        errors.append("state must be an object")
        state = {}
    supported = raw.get("supported")
    if not isinstance(supported, dict):
        errors.append("supported must be an object")
        supported = {}

    normalized_state: dict[str, int | None] = {}
    size = nonnegative_integer(state.get("size"))
    normalized_state["size"] = size
    if size is None:
        errors.append("state.size must be a non-negative integer")
    elif size != 0:
        errors.append("state.size is not zero")

    normalized_supported: dict[str, bool | None] = {}
    for label in ("ready", "reserved", "delayed"):
        is_supported = supported.get(label)
        normalized_supported[label] = (
            is_supported if isinstance(is_supported, bool) else None
        )
        if not isinstance(is_supported, bool):
            errors.append(f"supported.{label} must be boolean")
        count = nonnegative_integer(state.get(label))
        normalized_state[label] = count
        if is_supported is True:
            if count is None:
                errors.append(
                    f"state.{label} must be a non-negative integer when supported"
                )
            elif count != 0:
                errors.append(f"state.{label} is not zero")

    probe_errors = raw.get("probe_errors")
    if not isinstance(probe_errors, list) or any(
        not isinstance(value, str) for value in probe_errors
    ):
        errors.append("probe_errors must be a list of strings")
        probe_errors = []
    elif probe_errors:
        errors.append("final queue probe contains errors")

    checks = nonnegative_integer(raw.get("checks"))
    if checks is None or checks < 1:
        errors.append("checks must be a positive integer")
    settle_ns = nonnegative_integer(raw.get("settle_ns"))
    settled_for_ns = nonnegative_integer(raw.get("settled_for_ns"))
    if settle_ns is None:
        errors.append("settle_ns must be a non-negative integer")
    if settled_for_ns is None:
        errors.append("settled_for_ns must be a non-negative integer")
    elif settle_ns is not None and settled_for_ns < settle_ns:
        errors.append("queue was not empty for the required settle interval")

    started_at_ns = nonnegative_integer(raw.get("started_at_ns"))
    finished_at_ns = nonnegative_integer(raw.get("finished_at_ns"))
    elapsed_ns = nonnegative_integer(raw.get("elapsed_ns"))
    if started_at_ns is None or finished_at_ns is None or elapsed_ns is None:
        errors.append("queue observation timestamps must be non-negative integers")
    elif finished_at_ns < started_at_ns or elapsed_ns != finished_at_ns - started_at_ns:
        errors.append("queue observation timestamps are inconsistent")

    quiescent = raw.get("quiescent") is True
    if not quiescent:
        errors.append("queue did not reach quiescence")
    if raw.get("timed_out") is not False:
        errors.append("queue observation timed out")

    artifact_valid = not errors
    return {
        "artifact": str(path),
        "artifact_valid": artifact_valid,
        "quiescent": quiescent,
        "gate_passed": artifact_valid and quiescent,
        "run_id": raw.get("run_id"),
        "connection": raw.get("connection"),
        "queue": raw.get("queue"),
        "implementation": raw.get("implementation"),
        "state": normalized_state,
        "supported": normalized_supported,
        "checks": checks,
        "elapsed_ns": elapsed_ns,
        "settle_ns": settle_ns,
        "settled_for_ns": settled_for_ns,
        "probe_error_count": nonnegative_integer(raw.get("probe_error_count")),
        "last_probe_error": raw.get("last_probe_error"),
        "validation_errors": errors,
    }


def effect_ledger_summary(
    run_directory: Path,
    manifest: dict[str, Any],
    completions: dict[str, list[dict[str, Any]]],
    expected: int | None,
    maximum_ids: int = 100,
    allow_open_attempts: bool = False,
) -> dict[str, Any]:
    """Verify the durable fixture-local effect ledger.

    The committed effect row is an auditable witness, not an exactly-once
    claim: it is not atomic with Laravel's queue ACK or an external system.
    """

    mode = manifest.get("ledger_mode", "off")
    path = run_directory / "ledger.sqlite3"
    base: dict[str, Any] = {
        "schema": LEDGER_SCHEMA,
        "mode": mode,
        "required": mode == "durable",
        "artifact": str(path),
        "allow_open_attempts": allow_open_attempts,
        "semantics": (
            "fixture-local idempotent effect keyed by run_id+job_id; not atomic with the queue ACK "
            "or arbitrary external effects"
        ),
        "exactly_once_claim": False,
    }
    if mode == "off":
        return base | {
            "status": "not_requested",
            "gate_passed": True,
            "conservation_pass": None,
            "idempotent_effect_pass": None,
            "no_duplicate_side_effects_pass": None,
            "attempt_integrity_pass": None,
            "strict_execution_pass": None,
            "validation_errors": [],
        }
    if mode != "durable":
        return base | {
            "status": "invalid_mode",
            "gate_passed": False,
            "conservation_pass": False,
            "idempotent_effect_pass": False,
            "no_duplicate_side_effects_pass": False,
            "attempt_integrity_pass": False,
            "strict_execution_pass": False,
            "validation_errors": ["dispatch ledger_mode must be off or durable"],
        }

    errors: list[str] = []
    attempts: list[dict[str, Any]] = []
    effects: list[dict[str, Any]] = []
    metadata: dict[str, str] = {}
    quick_check: list[str] = []
    foreign_key_violations: list[list[Any]] = []
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=30.0)
        connection.row_factory = sqlite3.Row
        quick_check = [str(row[0]) for row in connection.execute("PRAGMA quick_check")]
        foreign_key_violations = [
            list(row) for row in connection.execute("PRAGMA foreign_key_check")
        ]
        metadata = {
            str(row["key"]): str(row["value"])
            for row in connection.execute("SELECT key, value FROM ledger_meta")
        }
        attempts = [
            dict(row)
            for row in connection.execute(
                """
                SELECT attempt_id, run_id, job_id, attempt_number, worker_pid,
                       worker_host, started_at_ns, effect_outcome, observed_effect_id,
                       effect_observed_at_ns, outcome, outcome_at_ns, error_class
                FROM attempts ORDER BY started_at_ns, attempt_id
                """
            )
        ]
        effects = [
            dict(row)
            for row in connection.execute(
                """
                SELECT effect_id, run_id, job_id, created_by_attempt_id, checksum, committed_at_ns
                FROM effects ORDER BY committed_at_ns, effect_id
                """
            )
        ]
    except (OSError, sqlite3.Error) as exception:
        errors.append(f"cannot verify ledger database: {exception}")
    finally:
        if connection is not None:
            connection.close()

    run_id = manifest.get("run_id")
    if metadata.get("schema") != LEDGER_SCHEMA:
        errors.append(f"ledger metadata schema must be {LEDGER_SCHEMA}")
    if not isinstance(run_id, str) or metadata.get("run_id") != run_id:
        errors.append("ledger metadata run_id does not match dispatch manifest")
    if quick_check != ["ok"]:
        errors.append("SQLite quick_check did not return exactly one ok row")
    if foreign_key_violations:
        errors.append("SQLite foreign_key_check found violations")
    expected_ids, expected_id_errors = expected_job_ids(manifest)
    errors.extend(expected_id_errors)
    if expected is None or expected < 0:
        errors.append("dispatch manifest does not contain a valid expected job count")
    elif len(expected_ids) != expected and not expected_id_errors:
        errors.append("resolved ledger job IDs do not match the expected job count")

    def valid_token(value: Any) -> bool:
        return (
            isinstance(value, str)
            and len(value) == 32
            and all(character in "0123456789abcdef" for character in value)
        )

    attempt_by_id: dict[str, dict[str, Any]] = {}
    attempts_by_job: dict[str, list[dict[str, Any]]] = defaultdict(list)
    invalid_attempt_rows = 0
    for attempt in attempts:
        attempt_id = attempt.get("attempt_id")
        effect_outcome = attempt.get("effect_outcome")
        observed_effect_id = attempt.get("observed_effect_id")
        effect_observed_at_ns = attempt.get("effect_observed_at_ns")
        final_outcome = attempt.get("outcome")
        outcome_at_ns = attempt.get("outcome_at_ns")
        error_class = attempt.get("error_class")
        effect_fields_valid = (
            effect_outcome is None
            and observed_effect_id is None
            and effect_observed_at_ns is None
        ) or (
            effect_outcome in {"created", "already_present"}
            and valid_token(observed_effect_id)
            and nonnegative_integer(effect_observed_at_ns) is not None
        )
        outcome_fields_valid = (
            (final_outcome is None and outcome_at_ns is None and error_class is None)
            or (
                final_outcome == "completed"
                and nonnegative_integer(outcome_at_ns) is not None
                and error_class is None
            )
            or (
                final_outcome == "failed"
                and nonnegative_integer(outcome_at_ns) is not None
                and isinstance(error_class, str)
                and bool(error_class)
            )
        )
        if (
            not valid_token(attempt_id)
            or attempt.get("run_id") != run_id
            or not isinstance(attempt.get("job_id"), str)
            or nonnegative_integer(attempt.get("attempt_number")) in {None, 0}
            or nonnegative_integer(attempt.get("worker_pid")) in {None, 0}
            or nonnegative_integer(attempt.get("started_at_ns")) is None
            or not effect_fields_valid
            or not outcome_fields_valid
        ):
            invalid_attempt_rows += 1
            continue
        attempt_by_id[str(attempt_id)] = attempt
        attempts_by_job[str(attempt["job_id"])].append(attempt)

    effect_by_id: dict[str, dict[str, Any]] = {}
    effects_by_job: dict[str, list[dict[str, Any]]] = defaultdict(list)
    invalid_effect_rows = 0
    relational_errors = 0
    for effect in effects:
        effect_id = effect.get("effect_id")
        creator_id = effect.get("created_by_attempt_id")
        checksum = effect.get("checksum")
        creator = attempt_by_id.get(str(creator_id))
        if (
            not valid_token(effect_id)
            or not valid_token(creator_id)
            or effect.get("run_id") != run_id
            or not isinstance(effect.get("job_id"), str)
            or not isinstance(checksum, str)
            or len(checksum) != 64
            or any(character not in "0123456789abcdef" for character in checksum)
            or nonnegative_integer(effect.get("committed_at_ns")) is None
        ):
            invalid_effect_rows += 1
            continue
        if (
            creator is None
            or creator.get("run_id") != effect.get("run_id")
            or creator.get("job_id") != effect.get("job_id")
            or creator.get("effect_outcome") != "created"
            or creator.get("observed_effect_id") != effect_id
        ):
            relational_errors += 1
        effect_by_id[str(effect_id)] = effect
        effects_by_job[str(effect["job_id"])].append(effect)

    for attempt in attempt_by_id.values():
        observed_effect_id = attempt.get("observed_effect_id")
        if observed_effect_id is None:
            continue
        effect = effect_by_id.get(str(observed_effect_id))
        if effect is None or effect.get("job_id") != attempt.get("job_id"):
            relational_errors += 1

    observed_effect_jobs = set(effects_by_job)
    missing_effect_ids = expected_ids - observed_effect_jobs
    unexpected_effect_ids = observed_effect_jobs - expected_ids
    unexpected_attempt_jobs = set(attempts_by_job) - expected_ids
    duplicate_effects = {
        job_id: len(records) - 1
        for job_id, records in effects_by_job.items()
        if len(records) > 1
    }
    duplicate_effect_count = sum(duplicate_effects.values())
    duplicate_executions = {
        job_id: len(records) - 1
        for job_id, records in attempts_by_job.items()
        if len(records) > 1
    }
    duplicate_execution_count = sum(duplicate_executions.values())

    completion_link_errors = 0
    checksum_errors = 0
    completion_effect_ids: set[str] = set()
    expected_checksum_by_job: dict[str, str] = {}
    for job_id, records in completions.items():
        for record in records:
            checksum = record.get("checksum")
            if isinstance(checksum, str):
                expected_checksum_by_job.setdefault(job_id, checksum)
            attempt_id = record.get("ledger_attempt_id")
            effect_id = record.get("ledger_effect_id")
            effect_outcome = record.get("ledger_effect_outcome")
            effect_created = record.get("ledger_effect_created")
            attempt = attempt_by_id.get(str(attempt_id))
            effect = effect_by_id.get(str(effect_id))
            if (
                not valid_token(attempt_id)
                or not valid_token(effect_id)
                or effect_outcome not in {"created", "already_present"}
                or not isinstance(effect_created, bool)
                or attempt is None
                or effect is None
                or attempt.get("job_id") != job_id
                or effect.get("job_id") != job_id
                or attempt.get("observed_effect_id") != effect_id
                or attempt.get("effect_outcome") != effect_outcome
                or effect.get("checksum") != checksum
                or effect_created != (effect_outcome == "created")
            ):
                completion_link_errors += 1
            else:
                completion_effect_ids.add(str(effect_id))

    for job_id, job_effects in effects_by_job.items():
        expected_checksum = expected_checksum_by_job.get(job_id)
        if expected_checksum is None or any(
            effect.get("checksum") != expected_checksum for effect in job_effects
        ):
            checksum_errors += 1

    open_attempt_ids = {
        str(row["attempt_id"])
        for row in attempt_by_id.values()
        if row.get("outcome") is None
    }
    failed_attempt_ids = {
        str(row["attempt_id"])
        for row in attempt_by_id.values()
        if row.get("outcome") == "failed"
    }
    completed_attempt_ids = {
        str(row["attempt_id"])
        for row in attempt_by_id.values()
        if row.get("outcome") == "completed"
    }
    attempts_without_effect = {
        str(row["attempt_id"])
        for row in attempt_by_id.values()
        if row.get("effect_outcome") is None
    }
    completed_without_effect = attempts_without_effect & completed_attempt_ids
    created_attempt_ids = {
        str(row["attempt_id"])
        for row in attempt_by_id.values()
        if row.get("effect_outcome") == "created"
    }
    deduplicated_attempt_ids = {
        str(row["attempt_id"])
        for row in attempt_by_id.values()
        if row.get("effect_outcome") == "already_present"
    }
    effects_without_completion = set(effect_by_id) - completion_effect_ids

    structural_integrity = (
        not errors
        and invalid_attempt_rows == 0
        and invalid_effect_rows == 0
        and relational_errors == 0
        and completion_link_errors == 0
    )
    conservation_pass = (
        structural_integrity
        and not missing_effect_ids
        and not unexpected_effect_ids
        and checksum_errors == 0
    )
    idempotent_effect_pass = (
        structural_integrity
        and not missing_effect_ids
        and not unexpected_effect_ids
        and duplicate_effect_count == 0
        and len(effect_by_id) == (expected or 0)
    )
    attempt_integrity_pass = (
        structural_integrity
        and not unexpected_attempt_jobs
        and not failed_attempt_ids
        and not completed_without_effect
        and (allow_open_attempts or not open_attempt_ids)
    )
    strict_execution_pass = (
        structural_integrity
        and not unexpected_attempt_jobs
        and set(attempts_by_job) == expected_ids
        and duplicate_execution_count == 0
        and not deduplicated_attempt_ids
    )
    gate_passed = (
        conservation_pass
        and idempotent_effect_pass
        and attempt_integrity_pass
        and strict_execution_pass
    )

    return base | {
        "status": "verified" if structural_integrity else "invalid",
        "database": {
            "quick_check": quick_check,
            "foreign_key_violations": foreign_key_violations,
        },
        "attempts": {
            "records": len(attempts),
            "valid_records": len(attempt_by_id),
            "invalid_records": invalid_attempt_rows,
            "completed": len(completed_attempt_ids),
            "failed": limited_ids(failed_attempt_ids, maximum_ids),
            "open_or_interrupted": limited_ids(open_attempt_ids, maximum_ids),
            "without_effect": limited_ids(attempts_without_effect, maximum_ids),
            "created_effect": len(created_attempt_ids),
            "already_present": limited_ids(deduplicated_attempt_ids, maximum_ids),
            "duplicate_executions": {
                "count": duplicate_execution_count,
                "jobs": dict(list(sorted(duplicate_executions.items()))[:maximum_ids]),
                "truncated": len(duplicate_executions) > maximum_ids,
            },
            "unexpected_jobs": limited_ids(unexpected_attempt_jobs, maximum_ids),
        },
        "effects": {
            "idempotency_key": "run_id+job_id",
            "expected_unique_jobs": expected,
            "records": len(effects),
            "valid_records": len(effect_by_id),
            "invalid_records": invalid_effect_rows,
            "unique_jobs": len(observed_effect_jobs),
            "missing": limited_ids(missing_effect_ids, maximum_ids),
            "unexpected": limited_ids(unexpected_effect_ids, maximum_ids),
            "duplicates": {
                "count": duplicate_effect_count,
                "jobs": dict(list(sorted(duplicate_effects.items()))[:maximum_ids]),
                "truncated": len(duplicate_effects) > maximum_ids,
            },
            "without_completion_record": limited_ids(
                effects_without_completion, maximum_ids
            ),
            "checksum_error_jobs": checksum_errors,
            "relational_errors": relational_errors,
            "completion_link_errors": completion_link_errors,
        },
        "conservation_pass": conservation_pass,
        "idempotent_effect_pass": idempotent_effect_pass,
        "no_duplicate_side_effects_pass": idempotent_effect_pass,
        "attempt_integrity_pass": attempt_integrity_pass,
        "strict_execution_pass": strict_execution_pass,
        "gate_passed": gate_passed,
        "validation_errors": errors,
    }


def summarize_run(run_directory: Path, maximum_ids: int = 100) -> dict[str, Any]:
    manifest = load_object(run_directory / "dispatch.json")
    events = event_snapshot(run_directory, manifest)
    queue_state = final_queue_state(run_directory, manifest)
    expected = events["expected"]
    selected: dict[str, dict[str, Any]] = events["selected"]
    completions: dict[str, list[dict[str, Any]]] = events["completions"]
    failures: dict[str, list[dict[str, Any]]] = events["failures"]
    effect_ledger = effect_ledger_summary(
        run_directory,
        manifest,
        completions,
        events["expected"],
        maximum_ids,
    )

    completed_ids = set(selected)
    missing_count = (
        max(0, expected - events["expected_completed"]) if expected is not None else 0
    )
    missing_preview: list[str] = []
    if expected is not None and missing_count and maximum_ids > 0:
        for job_id in sorted(events["expected_job_ids"]):
            if job_id not in completed_ids:
                missing_preview.append(job_id)
                if len(missing_preview) >= maximum_ids:
                    break
    duplicate_ids = {
        job_id: len(records) - 1
        for job_id, records in completions.items()
        if len(records) > 1
    }

    queue_latencies: list[int] = []
    end_to_end_latencies: list[int] = []
    work_durations: list[int] = []
    sink_waits: list[int] = []
    enqueued: list[int] = []
    completed: list[int] = []
    attempts: list[int] = []
    for record in selected.values():
        for key, destination in (
            ("queue_latency_ns", queue_latencies),
            ("end_to_end_ns", end_to_end_latencies),
            ("work_duration_ns", work_durations),
            ("sink_lock_wait_ns", sink_waits),
            ("enqueued_at_ns", enqueued),
            ("completed_at_ns", completed),
            ("attempt", attempts),
        ):
            value = nonnegative_integer(record.get(key))
            if value is not None:
                destination.append(value)

    dispatch_started = nonnegative_integer(manifest.get("dispatch_started_ns"))
    dispatch_finished = nonnegative_integer(manifest.get("dispatch_finished_ns"))
    last_completed = max(completed, default=None)
    first_completed = min(completed, default=None)
    first_enqueued = min(enqueued, default=None)
    dispatch_to_complete_ns = (
        last_completed - dispatch_started
        if last_completed is not None
        and dispatch_started is not None
        and last_completed >= dispatch_started
        else None
    )
    enqueue_to_complete_ns = (
        last_completed - first_enqueued
        if last_completed is not None
        and first_enqueued is not None
        and last_completed >= first_enqueued
        else None
    )
    completion_span_ns = (
        last_completed - first_completed
        if last_completed is not None
        and first_completed is not None
        and last_completed >= first_completed
        else None
    )
    dispatch_duration_ns = (
        dispatch_finished - dispatch_started
        if dispatch_finished is not None
        and dispatch_started is not None
        and dispatch_finished >= dispatch_started
        else nonnegative_integer(manifest.get("dispatch_duration_ns"))
    )

    stats = read_stats(run_directory)
    samples, invalid_stats = valid_samples(stats)
    warnings: list[str] = []
    measurement_start = dispatch_started
    if samples and measurement_start is not None:
        first_stat = sample_time(samples[0])
        last_stat = sample_time(samples[-1])
        tolerance = 3_600 * 1_000_000_000
        if (
            measurement_start < first_stat - tolerance
            or measurement_start > last_stat + tolerance
        ):
            warnings.append(
                "dispatch and sampler monotonic clocks do not appear aligned; using all stats"
            )
            measurement_start = None
    if not samples:
        warnings.append("stats.jsonl has no usable samples")

    # Headline resources exclude warm-up, and deliberately extend to the final
    # available sample so supervisor scale-down/drain cost remains visible.
    headline_end = sample_time(samples[-1]) if samples else last_completed
    headline = resource_window(
        samples, measurement_start, headline_end, "measurement_with_drain"
    )
    active = resource_window(
        samples, measurement_start, last_completed, "dispatch_to_last_completion"
    )
    startup = (
        resource_window(samples, None, measurement_start, "startup")
        if measurement_start
        else None
    )
    all_window = resource_window(samples, None, None, "all_samples")
    scaling = scale_summary(samples, measurement_start, last_completed)

    event_integrity_errors = (
        events["raw"].malformed_lines
        + events["raw"].partial_lines
        + events["raw"].unreadable_files
        + events["invalid"]
        + events["foreign"]
    )
    metadata_records = [
        record for record in stats.records if record.get("type") == "metadata"
    ]
    end_records = [record for record in stats.records if record.get("type") == "end"]
    sampler_metadata = metadata_records[0] if len(metadata_records) == 1 else {}
    sampler_end = end_records[0] if len(end_records) == 1 else {}
    interval_ns = nonnegative_integer(sampler_metadata.get("interval_ns"))
    missing_sampling_durations = sum(
        1
        for sample in samples
        if nonnegative_integer(sample.get("sampling_duration_ns")) is None
    )
    sampling_overruns = 0
    cadence_gaps = 0
    if interval_ns is not None and interval_ns > 0:
        sampling_overruns = sum(
            1
            for sample in samples
            if (nonnegative_integer(sample.get("sampling_duration_ns")) or 0)
            > interval_ns
        )
        cadence_gaps = sum(
            1
            for left, right in zip(samples, samples[1:])
            if sample_time(right) - sample_time(left) > interval_ns * 1.5
        )
    sampler_shape_errors = 0
    if (
        len(metadata_records) != 1
        or sampler_metadata.get("schema") != "queen.laravel-supervisors.stats/v1"
    ):
        sampler_shape_errors += 1
    if (
        len(end_records) != 1
        or sampler_end.get("schema") != "queen.laravel-supervisors.stats/v1"
    ):
        sampler_shape_errors += 1
    if interval_ns is None or interval_ns <= 0:
        sampler_shape_errors += 1
    if nonnegative_integer(sampler_end.get("samples")) != len(samples):
        sampler_shape_errors += 1

    stats_integrity_errors = (
        stats.malformed_lines
        + stats.partial_lines
        + stats.unreadable_files
        + invalid_stats
        + sampler_shape_errors
        + missing_sampling_durations
        + sampling_overruns
        + cadence_gaps
    )
    target_errors = 0
    dead_targets = 0
    for sample in samples:
        for target in sample.get("targets", []):
            if not isinstance(target, dict):
                target_errors += 1
                continue
            errors = target.get("errors")
            if isinstance(errors, list):
                target_errors += len(errors)
            if target.get("alive") is not True:
                dead_targets += 1
    stats_integrity_errors += target_errors + dead_targets
    duplicate_count = sum(duplicate_ids.values())
    failed_records = sum(len(records) for records in failures.values())
    max_attempt = max(attempts, default=None)
    attempts_valid = len(attempts) == len(selected) and all(
        attempt == 1 for attempt in attempts
    )
    all_memory_events = value_at(all_window, "stack", "memory_events")
    oom_events = 0
    if isinstance(all_memory_events, dict):
        oom_events = sum(
            value
            for key, value in all_memory_events.items()
            if key in {"oom", "oom_kill", "oom_group_kill"}
            and isinstance(value, int)
            and value > 0
        )
    pss_requested = sampler_metadata.get("pss_enabled") is True
    pss_complete = not pss_requested or all(
        value_at(headline, role, "pss_coverage") == 1.0
        for role in ("orchestrator", "workers")
    )
    complete = expected is not None and events["expected_completed"] == expected
    correct = (
        complete
        and missing_count == 0
        and duplicate_count == 0
        and failed_records == 0
        and attempts_valid
        and not events["manifest_validation_errors"]
        and not events["unexpected_job_ids"]
        and event_integrity_errors == 0
        and stats_integrity_errors == 0
        and bool(samples)
        and oom_events == 0
        and pss_complete
        and queue_state["gate_passed"]
        and effect_ledger["gate_passed"]
    )

    return {
        "schema": SUMMARY_SCHEMA,
        "run_directory": str(run_directory.resolve()),
        "run_id": manifest.get("run_id"),
        "profile": manifest.get("profile") or manifest.get("connection"),
        "manifest": manifest,
        "correctness": {
            "correct": correct,
            "complete": complete,
            "expected": expected,
            "records": len(events["raw"].records),
            "completed_records": sum(len(records) for records in completions.values()),
            "unique_completed": len(selected),
            "expected_completed": events["expected_completed"],
            "missing": {
                "count": missing_count,
                "ids": missing_preview,
                "truncated": missing_count > len(missing_preview),
            },
            "duplicates": {
                "count": duplicate_count,
                "jobs": dict(list(sorted(duplicate_ids.items()))[:maximum_ids]),
                "truncated": len(duplicate_ids) > maximum_ids,
            },
            "failed": limited_ids(failures.keys(), maximum_ids)
            | {"records": failed_records},
            "unexpected": limited_ids(events["unexpected_job_ids"], maximum_ids),
            "foreign_records": events["foreign"],
            "invalid_records": events["invalid"],
            "malformed_lines": events["raw"].malformed_lines,
            "partial_lines_ignored": events["raw"].partial_lines,
            "unreadable_files": events["raw"].unreadable_files,
            "max_attempt": max_attempt,
            "attempt_records": len(attempts),
            "attempts_valid": attempts_valid,
            "manifest_validation_errors": events["manifest_validation_errors"],
            "queue_quiescent": queue_state["gate_passed"],
            "effect_ledger_required": effect_ledger["required"],
            "effect_ledger_gate_passed": effect_ledger["gate_passed"],
        },
        "queue_state": queue_state,
        "effect_ledger": effect_ledger,
        "throughput": {
            "headline_jobs_per_second": duration_rate(
                len(selected), dispatch_to_complete_ns
            ),
            "dispatch_to_last_completion_ns": dispatch_to_complete_ns,
            "enqueue_to_last_completion_ns": enqueue_to_complete_ns,
            "completion_span_ns": completion_span_ns,
            "completion_span_jobs_per_second": duration_rate(
                max(0, len(selected) - 1), completion_span_ns
            ),
            "dispatch_duration_ns": dispatch_duration_ns,
            "dispatch_jobs_per_second": duration_rate(
                expected or 0, dispatch_duration_ns
            ),
        },
        "latency": {
            "queue": latency_distribution(queue_latencies),
            "end_to_end": latency_distribution(end_to_end_latencies),
            "work": latency_distribution(work_durations),
            "sink_lock_wait": latency_distribution(sink_waits),
        },
        "resources": {
            "headline_window": headline["window"],
            "window": {
                key: headline[key]
                for key in headline
                if key.endswith("_ns") or key == "samples"
            },
            "orchestrator": headline["orchestrator"],
            "workers": headline["workers"],
            "app": headline["app"],
            "backend": headline["backend"],
            "stack": headline["stack"],
            "windows": {
                "active": active,
                "startup": startup,
                "all": all_window,
            },
            "stats_integrity": {
                "records": len(stats.records),
                "samples": len(samples),
                "invalid_samples": invalid_stats,
                "malformed_lines": stats.malformed_lines,
                "partial_lines_ignored": stats.partial_lines,
                "unreadable_files": stats.unreadable_files,
                "target_errors": target_errors,
                "dead_target_samples": dead_targets,
                "oom_events": oom_events,
                "metadata_records": len(metadata_records),
                "end_records": len(end_records),
                "end_reason": sampler_end.get("reason"),
                "expected_interval_ns": interval_ns,
                "sampling_overruns": sampling_overruns,
                "missing_sampling_durations": missing_sampling_durations,
                "cadence_gaps": cadence_gaps,
                "pss_requested": pss_requested,
                "pss_complete": pss_complete,
                "integrity_errors": stats_integrity_errors,
            },
        },
        "scaling": scaling,
        "warnings": warnings,
    }


def atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with temporary.open("x", encoding="utf-8") as stream:
        stream.write(content)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def json_text(value: Any, pretty: bool = True) -> str:
    return (
        json.dumps(
            value,
            indent=2 if pretty else None,
            separators=None if pretty else (",", ":"),
            sort_keys=True,
            ensure_ascii=True,
        )
        + "\n"
    )


def wait_command(args: argparse.Namespace) -> int:
    run_directory = Path(args.run_directory)
    manifest_path = run_directory / "dispatch.json"
    deadline = time.monotonic() + args.timeout
    manifest: dict[str, Any] | None = None
    reached_at: float | None = None

    while True:
        try:
            manifest = load_object(manifest_path)
        except ValueError:
            if time.monotonic() >= deadline:
                print(
                    json_text(
                        {"complete": False, "reason": "dispatch manifest unavailable"}
                    ),
                    end="",
                )
                return 1
            time.sleep(args.poll)
            continue
        if args.expected is not None:
            manifest = dict(manifest)
            manifest["jobs"] = args.expected
        snapshot = event_snapshot(run_directory, manifest)
        expected = snapshot["expected"]
        complete = (
            expected is not None
            and not snapshot["manifest_validation_errors"]
            and snapshot["expected_completed"] >= expected
        )
        if complete:
            if reached_at is None:
                reached_at = time.monotonic()
            if time.monotonic() - reached_at >= args.settle:
                payload = {
                    "complete": True,
                    "run_id": manifest.get("run_id"),
                    "expected": expected,
                    "unique_completed": len(snapshot["selected"]),
                    "duplicates": sum(
                        max(0, len(value) - 1)
                        for value in snapshot["completions"].values()
                    ),
                    "failed_records": sum(
                        len(value) for value in snapshot["failures"].values()
                    ),
                    "malformed_lines": snapshot["raw"].malformed_lines,
                    "partial_lines_ignored": snapshot["raw"].partial_lines,
                }
                print(json_text(payload, pretty=False), end="")
                return 0
        if time.monotonic() >= deadline:
            payload = {
                "complete": False,
                "run_id": manifest.get("run_id"),
                "expected": expected,
                "unique_completed": len(snapshot["selected"]),
                "failed_records": sum(
                    len(value) for value in snapshot["failures"].values()
                ),
                "reason": "timeout",
            }
            print(json_text(payload, pretty=False), end="")
            return 1
        time.sleep(args.poll)


def summarize_command(args: argparse.Namespace) -> int:
    try:
        summary = summarize_run(Path(args.run_directory), args.max_ids)
    except ValueError as exception:
        print(f"analyze.py: {exception}", file=sys.stderr)
        return 2
    content = json_text(summary, pretty=not args.compact)
    if args.output:
        atomic_write(Path(args.output), content)
    else:
        print(content, end="")
    return 0 if summary["correctness"]["correct"] or args.allow_incomplete else 1


def ledger_command(args: argparse.Namespace) -> int:
    try:
        run_directory = Path(args.run_directory)
        manifest = load_object(run_directory / "dispatch.json")
        manifest_jobs = nonnegative_integer(manifest.get("jobs"))
        if manifest_jobs is None:
            raise ValueError("dispatch.jobs must be a non-negative integer")
        if args.expected is not None and manifest_jobs != args.expected:
            raise ValueError(
                f"--expected={args.expected} does not match dispatch.jobs={manifest_jobs}"
            )
        events = event_snapshot(run_directory, manifest)
        summary = effect_ledger_summary(
            run_directory,
            manifest,
            events["completions"],
            events["expected"],
            args.max_ids,
            allow_open_attempts=args.allow_open_attempts,
        )
    except ValueError as exception:
        print(f"analyze.py: {exception}", file=sys.stderr)
        return 2

    selected_gate_passed = (
        summary["conservation_pass"] is True
        and summary["attempt_integrity_pass"] is True
        and summary["idempotent_effect_pass"] is True
        and (args.allow_retried_executions or summary["strict_execution_pass"] is True)
    )
    summary = summary | {
        "selected_gate_passed": selected_gate_passed,
        "retried_executions_allowed_by_selected_gate": args.allow_retried_executions,
    }
    content = json_text(summary, pretty=not args.compact)
    if args.output:
        atomic_write(Path(args.output), content)
    else:
        print(content, end="")
    return 0 if selected_gate_passed else 1


def parse_scenario(value: str) -> tuple[str, Path]:
    if "=" in value:
        label, raw_path = value.split("=", 1)
    else:
        raw_path = value
        label = Path(value).name
    if not label or not raw_path:
        raise argparse.ArgumentTypeError(
            "scenario must be LABEL=RUN_DIRECTORY_OR_SUMMARY"
        )
    return label, Path(raw_path)


def scenario_summary(path: Path, maximum_ids: int) -> dict[str, Any]:
    if path.is_dir():
        return summarize_run(path, maximum_ids)
    value = load_object(path)
    if value.get("schema") != SUMMARY_SCHEMA:
        raise ValueError(f"not a {SUMMARY_SCHEMA} document: {path}")
    return value


def value_at(mapping: Any, *keys: str) -> Any:
    value = mapping
    for key in keys:
        if not isinstance(value, dict):
            return None
        value = value.get(key)
    return value


def finite_number(value: Any) -> float | None:
    if (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
    ):
        return float(value)
    return None


def ratio(value: Any, baseline: Any) -> float | None:
    number = finite_number(value)
    base = finite_number(baseline)
    if number is None or base is None or base == 0:
        return None
    return number / base


def mib(value: Any) -> str:
    number = finite_number(value)
    return "n/a" if number is None else f"{number / 1_048_576:.1f}"


def decimal(value: Any, digits: int = 2) -> str:
    number = finite_number(value)
    return "n/a" if number is None else f"{number:.{digits}f}"


def milliseconds(value: Any) -> str:
    number = finite_number(value)
    return "n/a" if number is None else f"{number:.2f}"


def seconds_from_ns(value: Any) -> str:
    number = finite_number(value)
    return "n/a" if number is None else f"{number / 1_000_000_000:.2f}"


def orchestrator_memory(summary: dict[str, Any]) -> tuple[str, Any]:
    resources = value_at(summary, "resources", "orchestrator")
    coverage = finite_number(value_at(resources, "pss_coverage"))
    pss = value_at(resources, "pss_bytes", "max")
    if coverage == 1.0 and finite_number(pss) is not None:
        return "pss", pss
    return "rss", value_at(resources, "rss_bytes", "max")


def summary_is_correct(summary: dict[str, Any]) -> bool:
    return (
        value_at(summary, "correctness", "correct") is True
        and value_at(summary, "queue_state", "gate_passed") is True
    )


def markdown_report(report: dict[str, Any]) -> str:
    lines = [
        "# Laravel supervisor benchmark",
        "",
        "Correctness is a gate: resource and latency comparisons are not valid for incomplete, duplicate, failed or non-quiescent runs.",
        "",
        "| Scenario | Correct | Queue idle | Queue size | Completed | Missing | Duplicates | Failed | jobs/s | E2E p50 ms | p95 ms | p99 ms | Peak workers | To peak s | Return s |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for scenario in report["scenarios"]:
        summary = scenario["summary"]
        correctness = summary["correctness"]
        queue_state = summary.get("queue_state", {})
        latency = summary["latency"]["end_to_end"]
        scaling = summary["scaling"]
        lines.append(
            "| {label} | {correct} | {queue_idle} | {queue_size} | {completed}/{expected} | {missing} | {duplicates} | {failed} | {rate} | {p50} | {p95} | {p99} | {peak} | {to_peak} | {drain} |".format(
                label=scenario["label"].replace("|", "\\|"),
                correct="yes" if summary_is_correct(summary) else "NO",
                queue_idle="yes" if queue_state.get("gate_passed") is True else "NO",
                queue_size=decimal(value_at(queue_state, "state", "size"), 0),
                completed=correctness["unique_completed"],
                expected=correctness["expected"],
                missing=correctness["missing"]["count"],
                duplicates=correctness["duplicates"]["count"],
                failed=correctness["failed"]["count"],
                rate=decimal(summary["throughput"]["headline_jobs_per_second"]),
                p50=milliseconds(latency["p50_ms"]),
                p95=milliseconds(latency["p95_ms"]),
                p99=milliseconds(latency["p99_ms"]),
                peak=scaling["worker_peak"],
                to_peak=seconds_from_ns(scaling["time_to_peak_workers_ns"]),
                drain=seconds_from_ns(scaling["return_to_initial_after_completion_ns"]),
            )
        )

    lines.extend(
        [
            "",
            "## Resources (warm-up excluded, post-drain included)",
            "",
            "| Scenario | Orch CPU s | Orch peak PSS MiB | PSS cov. | Orch peak RSS MiB | Workers CPU s | Workers peak PSS MiB | Workers peak RSS MiB | App CPU s | App peak MiB | Backend CPU s | Backend peak MiB | Stack CPU s | Stack peak MiB |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for scenario in report["scenarios"]:
        resources = scenario["summary"]["resources"]
        lines.append(
            "| {label} | {ocpu} | {opss} | {ocov} | {orss} | {wcpu} | {wpss} | {wrss} | {acpu} | {amem} | {bcpu} | {bmem} | {scpu} | {smem} |".format(
                label=scenario["label"].replace("|", "\\|"),
                ocpu=decimal(value_at(resources, "orchestrator", "cpu_seconds"), 3),
                opss=mib(value_at(resources, "orchestrator", "pss_bytes", "max")),
                ocov=decimal(value_at(resources, "orchestrator", "pss_coverage"), 3),
                orss=mib(value_at(resources, "orchestrator", "rss_bytes", "max")),
                wcpu=decimal(value_at(resources, "workers", "cpu_seconds"), 3),
                wpss=mib(value_at(resources, "workers", "pss_bytes", "max")),
                wrss=mib(value_at(resources, "workers", "rss_bytes", "max")),
                acpu=decimal(value_at(resources, "app", "cpu_seconds"), 3),
                amem=mib(value_at(resources, "app", "memory_current_bytes", "max")),
                bcpu=decimal(value_at(resources, "backend", "cpu_seconds"), 3),
                bmem=mib(value_at(resources, "backend", "memory_current_bytes", "max")),
                scpu=decimal(value_at(resources, "stack", "cpu_seconds"), 3),
                smem=mib(value_at(resources, "stack", "memory_current_bytes", "max")),
            )
        )

    if report.get("comparisons"):
        lines.extend(
            [
                "",
                f"## Relative to {report['baseline']}",
                "",
                "Values above 1 mean more throughput, latency, CPU or memory respectively.",
                "",
                "| Scenario | Throughput x | E2E p95 x | Orch CPU x | Orch memory x | Orch metric | App CPU x | App peak memory x |",
                "|---|---:|---:|---:|---:|---:|---:|---:|",
            ]
        )
        for comparison in report["comparisons"]:
            lines.append(
                "| {label} | {throughput} | {latency} | {ocpu} | {omem} | {ometric} | {acpu} | {amem} |".format(
                    label=comparison["label"].replace("|", "\\|"),
                    throughput=decimal(comparison["throughput_ratio"], 3),
                    latency=decimal(comparison["end_to_end_p95_ratio"], 3),
                    ocpu=decimal(comparison["orchestrator_cpu_ratio"], 3),
                    omem=decimal(comparison["orchestrator_peak_memory_ratio"], 3),
                    ometric=comparison["orchestrator_memory_metric"] or "n/a",
                    acpu=decimal(comparison["app_cpu_ratio"], 3),
                    amem=decimal(comparison["app_peak_memory_ratio"], 3),
                )
            )
    lines.append("")
    return "\n".join(lines)


def report_command(args: argparse.Namespace) -> int:
    scenarios: list[dict[str, Any]] = []
    try:
        for label, path in args.scenario:
            scenarios.append(
                {"label": label, "summary": scenario_summary(path, args.max_ids)}
            )
    except ValueError as exception:
        print(f"analyze.py: {exception}", file=sys.stderr)
        return 2
    if not scenarios:
        print("analyze.py: at least one scenario is required", file=sys.stderr)
        return 2

    baseline_summary = scenarios[0]["summary"]
    baseline_correct = summary_is_correct(baseline_summary)
    baseline_memory_metric, baseline_memory = orchestrator_memory(baseline_summary)
    comparisons: list[dict[str, Any]] = []
    for scenario in [] if args.no_comparisons else scenarios[1:]:
        summary = scenario["summary"]
        scenario_memory_metric, scenario_memory = orchestrator_memory(summary)
        comparable_memory_metric = (
            baseline_memory_metric
            if baseline_memory_metric == scenario_memory_metric
            else None
        )
        comparison_eligible = baseline_correct and summary_is_correct(summary)
        comparisons.append(
            {
                "label": scenario["label"],
                "eligible": comparison_eligible,
                "throughput_ratio": ratio(
                    value_at(summary, "throughput", "headline_jobs_per_second"),
                    value_at(
                        baseline_summary, "throughput", "headline_jobs_per_second"
                    ),
                )
                if comparison_eligible
                else None,
                "end_to_end_p95_ratio": ratio(
                    value_at(summary, "latency", "end_to_end", "p95_ms"),
                    value_at(baseline_summary, "latency", "end_to_end", "p95_ms"),
                )
                if comparison_eligible
                else None,
                "orchestrator_cpu_ratio": ratio(
                    value_at(summary, "resources", "orchestrator", "cpu_seconds"),
                    value_at(
                        baseline_summary, "resources", "orchestrator", "cpu_seconds"
                    ),
                )
                if comparison_eligible
                else None,
                "orchestrator_peak_memory_ratio": ratio(
                    scenario_memory, baseline_memory
                )
                if comparable_memory_metric is not None and comparison_eligible
                else None,
                "orchestrator_memory_metric": comparable_memory_metric
                if comparison_eligible
                else None,
                "app_cpu_ratio": ratio(
                    value_at(summary, "resources", "app", "cpu_seconds"),
                    value_at(baseline_summary, "resources", "app", "cpu_seconds"),
                )
                if comparison_eligible
                else None,
                "app_peak_memory_ratio": ratio(
                    value_at(
                        summary, "resources", "app", "memory_current_bytes", "max"
                    ),
                    value_at(
                        baseline_summary,
                        "resources",
                        "app",
                        "memory_current_bytes",
                        "max",
                    ),
                )
                if comparison_eligible
                else None,
            }
        )
    report = {
        "schema": REPORT_SCHEMA,
        "baseline": scenarios[0]["label"],
        "scenarios": scenarios,
        "comparisons": comparisons,
        "all_correct": all(summary_is_correct(item["summary"]) for item in scenarios),
    }
    markdown = markdown_report(report)
    if args.output:
        atomic_write(Path(args.output), markdown)
    else:
        print(markdown, end="")
    if args.json_output:
        atomic_write(Path(args.json_output), json_text(report))
    return 0 if report["all_correct"] or args.allow_invalid else 1


def argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    wait = commands.add_parser("wait", help="wait for all expected unique completions")
    wait.add_argument("run_directory")
    wait.add_argument("--expected", type=nonnegative_int)
    wait.add_argument("--timeout", type=nonnegative_float, default=600.0)
    wait.add_argument("--poll", type=positive_float, default=0.1)
    wait.add_argument("--settle", type=nonnegative_float, default=0.2)
    wait.set_defaults(function=wait_command)

    summarize = commands.add_parser(
        "summarize", help="create a machine-readable run summary"
    )
    summarize.add_argument("run_directory")
    summarize.add_argument("--output")
    summarize.add_argument("--max-ids", type=nonnegative_int, default=100)
    summarize.add_argument("--compact", action="store_true")
    summarize.add_argument("--allow-incomplete", action="store_true")
    summarize.set_defaults(function=summarize_command)

    ledger = commands.add_parser(
        "ledger",
        help="verify attempt/effect conservation in a durable fixture ledger",
    )
    ledger.add_argument("run_directory")
    ledger.add_argument("--expected", type=nonnegative_int)
    ledger.add_argument("--max-ids", type=nonnegative_int, default=100)
    ledger.add_argument("--allow-open-attempts", action="store_true")
    ledger.add_argument("--allow-retried-executions", action="store_true")
    ledger.add_argument("--output")
    ledger.add_argument("--compact", action="store_true")
    ledger.set_defaults(function=ledger_command)

    report = commands.add_parser(
        "report", help="compare run directories or saved summaries"
    )
    report.add_argument(
        "scenario", nargs="+", type=parse_scenario, metavar="LABEL=PATH"
    )
    report.add_argument("--output", help="Markdown output path")
    report.add_argument("--json-output", help="full machine-readable comparison path")
    report.add_argument("--max-ids", type=nonnegative_int, default=100)
    report.add_argument(
        "--no-comparisons",
        action="store_true",
        help="emit scenario tables without ratios to the first scenario",
    )
    report.add_argument("--allow-invalid", action="store_true")
    report.set_defaults(function=report_command)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = argument_parser().parse_args(argv)
    try:
        return int(args.function(args))
    except BrokenPipeError:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Wait for, summarize and compare Laravel supervisor benchmark runs.

Only the Python standard library is used.  JSONL readers deliberately tolerate
an incomplete final line so results remain useful after a killed container or a
full disk; malformed complete lines are counted and make correctness fail.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence


SUMMARY_SCHEMA = "queen.laravel-supervisors.summary/v1"
REPORT_SCHEMA = "queen.laravel-supervisors.report/v1"
FAILURE_WORDS = {"failed", "failure", "error", "exception", "dead", "timeout"}
ROLE_PRIORITY = {"worker": 5, "orchestrator": 4, "app": 3, "backend": 2, "stack": 1}


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
    return not is_failure(record) and nonnegative_integer(record.get("completed_at_ns")) is not None


def event_snapshot(run_directory: Path, manifest: dict[str, Any]) -> dict[str, Any]:
    run_id = manifest.get("run_id")
    expected = nonnegative_integer(manifest.get("jobs"))
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
            key=lambda item: nonnegative_integer(item.get("completed_at_ns"))
            if nonnegative_integer(item.get("completed_at_ns")) is not None
            else sys.maxsize
        )
        selected[job_id] = records[0]

    expected_completed = 0
    unexpected_job_ids: list[str] = []
    if expected is not None:
        for job_id in selected:
            if len(job_id) == 9 and job_id.isascii() and job_id.isdigit() and int(job_id) < expected:
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
        "unexpected_job_ids": sorted(unexpected_job_ids),
    }


def nearest_rank(sorted_values: Sequence[int | float], percentile: float) -> int | float | None:
    if not sorted_values:
        return None
    index = max(0, math.ceil(len(sorted_values) * percentile) - 1)
    return sorted_values[index]


def distribution(values: Iterable[int | float]) -> dict[str, int | float | None]:
    numbers = sorted(value for value in values if not isinstance(value, bool) and value >= 0)
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
        if not isinstance(target, dict) or not isinstance(target.get("processes"), list):
            continue
        for process in target["processes"]:
            if not isinstance(process, dict):
                continue
            pid = nonnegative_integer(process.get("pid"))
            if pid is None:
                continue
            key = (pid, process.get("start_ticks"))
            existing = rows.get(key)
            if existing is None or ROLE_PRIORITY.get(str(process.get("role")), 0) > ROLE_PRIORITY.get(
                str(existing.get("role")), 0
            ):
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
            lambda: {"rss": 0, "pss": 0, "private": 0, "count": 0, "pss_count": 0, "private_count": 0}
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
                if ROLE_PRIORITY.get(role, 0) > ROLE_PRIORITY.get(str(track["role"]), 0):
                    track["role"] = role
                if runtime is not None:
                    track["max_runtime"] = max(runtime, int(track.get("max_runtime", runtime)))
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
            tracked_delta(track, "runtime") for track in tracks.values() if track["role"] == role
        )
        wait_ns = sum(
            tracked_delta(track, "wait") for track in tracks.values() if track["role"] == role
        )
        cpu_cores = runtime_ns / duration_ns if duration_ns > 0 else None
        process_observations = sum(memory_series[role]["count"])
        pss_observations = sum(memory_series[role]["pss_count"])
        private_observations = sum(memory_series[role]["private_count"])
        output[role] = {
            "scope": "processes",
            "observed_processes": sum(1 for track in tracks.values() if track["role"] == role),
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
            lambda: {"memory": 0, "pids": 0, "reported_peak": 0, "memory_count": 0, "pids_count": 0, "peak_count": 0}
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

            events = cgroup.get("memory", {}).get("events") if isinstance(cgroup.get("memory"), dict) else None
            if isinstance(events, dict):
                for event, raw in events.items():
                    value = nonnegative_integer(raw)
                    if value is None:
                        continue
                    event_track = track["events"].setdefault(
                        str(event), {"baseline": value if at_baseline else 0, "maximum": value}
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
        throttled_usec = sum(tracked_delta(track, "throttled_usec") for track in matching)
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
            "memory_peak_reported_bytes": max(series[kind]["reported_peak"], default=None),
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
    return sum(1 for process in process_rows(sample).values() if process.get("role") == "worker")


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
    peak_at = next((timestamp for timestamp, count in points if count == peak), None) if peak else None
    scale_down = None
    if peak_at is not None:
        scale_down = next(
            (timestamp for timestamp, count in points if timestamp > peak_at and count < peak), None
        )
    drained = None
    if completed_ns is not None and peak:
        drained = next(
            (timestamp for timestamp, count in points if timestamp >= completed_ns and count == 0),
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
        "time_to_first_worker_ns": first_worker - origin if first_worker is not None else None,
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
    return {"count": len(ordered), "ids": ordered[:maximum], "truncated": len(ordered) > maximum}


def summarize_run(run_directory: Path, maximum_ids: int = 100) -> dict[str, Any]:
    manifest = load_object(run_directory / "dispatch.json")
    events = event_snapshot(run_directory, manifest)
    expected = events["expected"]
    selected: dict[str, dict[str, Any]] = events["selected"]
    completions: dict[str, list[dict[str, Any]]] = events["completions"]
    failures: dict[str, list[dict[str, Any]]] = events["failures"]

    completed_ids = set(selected)
    missing_count = max(0, expected - events["expected_completed"]) if expected is not None else 0
    missing_preview: list[str] = []
    if expected is not None and missing_count and maximum_ids > 0:
        for index in range(expected):
            job_id = f"{index:09d}"
            if job_id not in completed_ids:
                missing_preview.append(job_id)
                if len(missing_preview) >= maximum_ids:
                    break
    duplicate_ids = {job_id: len(records) - 1 for job_id, records in completions.items() if len(records) > 1}

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
        if last_completed is not None and dispatch_started is not None and last_completed >= dispatch_started
        else None
    )
    enqueue_to_complete_ns = (
        last_completed - first_enqueued
        if last_completed is not None and first_enqueued is not None and last_completed >= first_enqueued
        else None
    )
    completion_span_ns = (
        last_completed - first_completed
        if last_completed is not None and first_completed is not None and last_completed >= first_completed
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
        if measurement_start < first_stat - tolerance or measurement_start > last_stat + tolerance:
            warnings.append("dispatch and sampler monotonic clocks do not appear aligned; using all stats")
            measurement_start = None
    if not samples:
        warnings.append("stats.jsonl has no usable samples")

    # Headline resources exclude warm-up, and deliberately extend to the final
    # available sample so supervisor scale-down/drain cost remains visible.
    headline_end = sample_time(samples[-1]) if samples else last_completed
    headline = resource_window(samples, measurement_start, headline_end, "measurement_with_drain")
    active = resource_window(samples, measurement_start, last_completed, "dispatch_to_last_completion")
    startup = resource_window(samples, None, measurement_start, "startup") if measurement_start else None
    all_window = resource_window(samples, None, None, "all_samples")
    scaling = scale_summary(samples, measurement_start, last_completed)

    event_integrity_errors = (
        events["raw"].malformed_lines
        + events["raw"].unreadable_files
        + events["invalid"]
        + events["foreign"]
    )
    metadata_records = [record for record in stats.records if record.get("type") == "metadata"]
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
            if (nonnegative_integer(sample.get("sampling_duration_ns")) or 0) > interval_ns
        )
        cadence_gaps = sum(
            1
            for left, right in zip(samples, samples[1:])
            if sample_time(right) - sample_time(left) > interval_ns * 1.5
        )
    sampler_shape_errors = 0
    if len(metadata_records) != 1 or sampler_metadata.get("schema") != "queen.laravel-supervisors.stats/v1":
        sampler_shape_errors += 1
    if len(end_records) != 1 or sampler_end.get("schema") != "queen.laravel-supervisors.stats/v1":
        sampler_shape_errors += 1
    if interval_ns is None or interval_ns <= 0:
        sampler_shape_errors += 1
    if nonnegative_integer(sampler_end.get("samples")) != len(samples):
        sampler_shape_errors += 1

    stats_integrity_errors = (
        stats.malformed_lines
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
    attempts_valid = len(attempts) == len(selected) and all(attempt == 1 for attempt in attempts)
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
        and not events["unexpected_job_ids"]
        and event_integrity_errors == 0
        and stats_integrity_errors == 0
        and bool(samples)
        and oom_events == 0
        and pss_complete
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
            "failed": limited_ids(failures.keys(), maximum_ids) | {"records": failed_records},
            "unexpected": limited_ids(events["unexpected_job_ids"], maximum_ids),
            "foreign_records": events["foreign"],
            "invalid_records": events["invalid"],
            "malformed_lines": events["raw"].malformed_lines,
            "partial_lines_ignored": events["raw"].partial_lines,
            "unreadable_files": events["raw"].unreadable_files,
            "max_attempt": max_attempt,
            "attempt_records": len(attempts),
            "attempts_valid": attempts_valid,
        },
        "throughput": {
            "headline_jobs_per_second": duration_rate(len(selected), dispatch_to_complete_ns),
            "dispatch_to_last_completion_ns": dispatch_to_complete_ns,
            "enqueue_to_last_completion_ns": enqueue_to_complete_ns,
            "completion_span_ns": completion_span_ns,
            "completion_span_jobs_per_second": duration_rate(max(0, len(selected) - 1), completion_span_ns),
            "dispatch_duration_ns": dispatch_duration_ns,
            "dispatch_jobs_per_second": duration_rate(expected or 0, dispatch_duration_ns),
        },
        "latency": {
            "queue": latency_distribution(queue_latencies),
            "end_to_end": latency_distribution(end_to_end_latencies),
            "work": latency_distribution(work_durations),
            "sink_lock_wait": latency_distribution(sink_waits),
        },
        "resources": {
            "headline_window": headline["window"],
            "window": {key: headline[key] for key in headline if key.endswith("_ns") or key == "samples"},
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
    return json.dumps(
        value,
        indent=2 if pretty else None,
        separators=None if pretty else (",", ":"),
        sort_keys=True,
        ensure_ascii=True,
    ) + "\n"


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
                print(json_text({"complete": False, "reason": "dispatch manifest unavailable"}), end="")
                return 1
            time.sleep(args.poll)
            continue
        if args.expected is not None:
            manifest = dict(manifest)
            manifest["jobs"] = args.expected
        snapshot = event_snapshot(run_directory, manifest)
        expected = snapshot["expected"]
        complete = expected is not None and snapshot["expected_completed"] >= expected
        if complete:
            if reached_at is None:
                reached_at = time.monotonic()
            if time.monotonic() - reached_at >= args.settle:
                payload = {
                    "complete": True,
                    "run_id": manifest.get("run_id"),
                    "expected": expected,
                    "unique_completed": len(snapshot["selected"]),
                    "duplicates": sum(max(0, len(value) - 1) for value in snapshot["completions"].values()),
                    "failed_records": sum(len(value) for value in snapshot["failures"].values()),
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
                "failed_records": sum(len(value) for value in snapshot["failures"].values()),
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


def parse_scenario(value: str) -> tuple[str, Path]:
    if "=" in value:
        label, raw_path = value.split("=", 1)
    else:
        raw_path = value
        label = Path(value).name
    if not label or not raw_path:
        raise argparse.ArgumentTypeError("scenario must be LABEL=RUN_DIRECTORY_OR_SUMMARY")
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
    if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value):
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


def markdown_report(report: dict[str, Any]) -> str:
    lines = [
        "# Laravel supervisor benchmark",
        "",
        "Correctness is a gate: resource and latency comparisons are not valid for incomplete, duplicate or failed runs.",
        "",
        "| Scenario | Correct | Completed | Missing | Duplicates | Failed | jobs/s | E2E p50 ms | p95 ms | p99 ms | Peak workers | To peak s | Return s |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for scenario in report["scenarios"]:
        summary = scenario["summary"]
        correctness = summary["correctness"]
        latency = summary["latency"]["end_to_end"]
        scaling = summary["scaling"]
        lines.append(
            "| {label} | {correct} | {completed}/{expected} | {missing} | {duplicates} | {failed} | {rate} | {p50} | {p95} | {p99} | {peak} | {to_peak} | {drain} |".format(
                label=scenario["label"].replace("|", "\\|"),
                correct="yes" if correctness["correct"] else "NO",
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
            scenarios.append({"label": label, "summary": scenario_summary(path, args.max_ids)})
    except ValueError as exception:
        print(f"analyze.py: {exception}", file=sys.stderr)
        return 2
    if not scenarios:
        print("analyze.py: at least one scenario is required", file=sys.stderr)
        return 2

    baseline_summary = scenarios[0]["summary"]
    baseline_memory_metric, baseline_memory = orchestrator_memory(baseline_summary)
    comparisons: list[dict[str, Any]] = []
    for scenario in [] if args.no_comparisons else scenarios[1:]:
        summary = scenario["summary"]
        scenario_memory_metric, scenario_memory = orchestrator_memory(summary)
        comparable_memory_metric = (
            baseline_memory_metric if baseline_memory_metric == scenario_memory_metric else None
        )
        comparisons.append(
            {
                "label": scenario["label"],
                "throughput_ratio": ratio(
                    value_at(summary, "throughput", "headline_jobs_per_second"),
                    value_at(baseline_summary, "throughput", "headline_jobs_per_second"),
                ),
                "end_to_end_p95_ratio": ratio(
                    value_at(summary, "latency", "end_to_end", "p95_ms"),
                    value_at(baseline_summary, "latency", "end_to_end", "p95_ms"),
                ),
                "orchestrator_cpu_ratio": ratio(
                    value_at(summary, "resources", "orchestrator", "cpu_seconds"),
                    value_at(baseline_summary, "resources", "orchestrator", "cpu_seconds"),
                ),
                "orchestrator_peak_memory_ratio": ratio(scenario_memory, baseline_memory)
                if comparable_memory_metric is not None
                else None,
                "orchestrator_memory_metric": comparable_memory_metric,
                "app_cpu_ratio": ratio(
                    value_at(summary, "resources", "app", "cpu_seconds"),
                    value_at(baseline_summary, "resources", "app", "cpu_seconds"),
                ),
                "app_peak_memory_ratio": ratio(
                    value_at(summary, "resources", "app", "memory_current_bytes", "max"),
                    value_at(baseline_summary, "resources", "app", "memory_current_bytes", "max"),
                ),
            }
        )
    report = {
        "schema": REPORT_SCHEMA,
        "baseline": scenarios[0]["label"],
        "scenarios": scenarios,
        "comparisons": comparisons,
        "all_correct": all(item["summary"]["correctness"]["correct"] for item in scenarios),
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

    summarize = commands.add_parser("summarize", help="create a machine-readable run summary")
    summarize.add_argument("run_directory")
    summarize.add_argument("--output")
    summarize.add_argument("--max-ids", type=nonnegative_int, default=100)
    summarize.add_argument("--compact", action="store_true")
    summarize.add_argument("--allow-incomplete", action="store_true")
    summarize.set_defaults(function=summarize_command)

    report = commands.add_parser("report", help="compare run directories or saved summaries")
    report.add_argument("scenario", nargs="+", type=parse_scenario, metavar="LABEL=PATH")
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

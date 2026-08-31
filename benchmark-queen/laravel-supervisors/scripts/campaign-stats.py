#!/usr/bin/env python3
"""Aggregate statistically paired Laravel supervisor benchmark campaigns.

The input contract is one campaign directory containing:

    metadata.json
    <engine>/<profile>/rXX/{summary,configuration}.json

Ratios are paired by the exact repetition label and are emitted only when both
runs pass correctness and queue-quiescence gates and their normalized
comparison keys match.  The implementation intentionally uses only the Python
standard library so it can run inside the benchmark image or on the host.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import os
import random
import re
import statistics
import sys
import tempfile
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence


REPORT_SCHEMA = "queen.laravel-supervisors.campaign-stats/v1"
CAMPAIGN_SCHEMA = "queen.laravel-supervisors.campaign/v1"
SUMMARY_SCHEMA = "queen.laravel-supervisors.summary/v1"
CONTAINER_ISOLATION_SCHEMA = "queen.laravel-supervisors.container-isolation/v1"
DEFAULT_BASELINE = "horizon"
DEFAULT_SEED = 20260829
DEFAULT_RESAMPLES = 10_000
CONFIDENCE_LEVEL = 0.95
REPETITION_PATTERN = re.compile(r"^r[0-9]{2,}$")


@dataclass(frozen=True)
class Metric:
    name: str
    label: str
    unit: str
    direction: str
    getter: Callable[[Mapping[str, Any]], float | None]


def value_at(value: Any, *path: str) -> Any:
    current = value
    for key in path:
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    return current


def finite_number(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def path_metric(*path: str) -> Callable[[Mapping[str, Any]], float | None]:
    return lambda summary: finite_number(value_at(summary, *path))


METRICS: tuple[Metric, ...] = (
    Metric(
        "completion_span_jobs_per_second",
        "Completion-span throughput",
        "jobs/s",
        "higher_is_better",
        path_metric("throughput", "completion_span_jobs_per_second"),
    ),
    Metric(
        "headline_jobs_per_second",
        "Dispatch-to-last throughput",
        "jobs/s",
        "higher_is_better",
        path_metric("throughput", "headline_jobs_per_second"),
    ),
    Metric(
        "dispatch_jobs_per_second",
        "Producer throughput",
        "jobs/s",
        "higher_is_better",
        path_metric("throughput", "dispatch_jobs_per_second"),
    ),
    Metric(
        "end_to_end_p95_ms",
        "End-to-end p95",
        "ms",
        "lower_is_better",
        path_metric("latency", "end_to_end", "p95_ms"),
    ),
    Metric(
        "end_to_end_p99_ms",
        "End-to-end p99",
        "ms",
        "lower_is_better",
        path_metric("latency", "end_to_end", "p99_ms"),
    ),
    Metric(
        "orchestrator_cpu_seconds",
        "Orchestrator CPU",
        "seconds",
        "lower_is_better",
        path_metric("resources", "orchestrator", "cpu_seconds"),
    ),
    Metric(
        "orchestrator_pss_max_bytes",
        "Orchestrator peak PSS",
        "bytes",
        "lower_is_better",
        path_metric("resources", "orchestrator", "pss_bytes", "max"),
    ),
    Metric(
        "orchestrator_rss_max_bytes",
        "Orchestrator peak RSS",
        "bytes",
        "lower_is_better",
        path_metric("resources", "orchestrator", "rss_bytes", "max"),
    ),
    Metric(
        "lease_renewer_cpu_seconds",
        "Lease-renewer CPU",
        "seconds",
        "lower_is_better",
        path_metric("resources", "lease_renewers", "cpu_seconds"),
    ),
    Metric(
        "lease_renewer_pss_max_bytes",
        "Lease-renewer peak PSS",
        "bytes",
        "lower_is_better",
        path_metric("resources", "lease_renewers", "pss_bytes", "max"),
    ),
    Metric(
        "lease_renewer_processes_peak",
        "Lease-renewer peak processes",
        "processes",
        "lower_is_better",
        path_metric("resources", "lease_renewers", "processes_peak"),
    ),
    Metric(
        "workers_cpu_seconds",
        "Worker CPU",
        "seconds",
        "lower_is_better",
        path_metric("resources", "workers", "cpu_seconds"),
    ),
    Metric(
        "app_cpu_seconds",
        "Application-container CPU",
        "seconds",
        "lower_is_better",
        path_metric("resources", "app", "cpu_seconds"),
    ),
    Metric(
        "app_memory_max_bytes",
        "Application peak memory.current",
        "bytes",
        "lower_is_better",
        path_metric("resources", "app", "memory_current_bytes", "max"),
    ),
    Metric(
        "backend_cpu_seconds",
        "Backend CPU",
        "seconds",
        "lower_is_better",
        path_metric("resources", "backend", "cpu_seconds"),
    ),
    Metric(
        "backend_memory_max_bytes",
        "Backend peak memory.current",
        "bytes",
        "lower_is_better",
        path_metric("resources", "backend", "memory_current_bytes", "max"),
    ),
    Metric(
        "stack_cpu_seconds",
        "Whole-stack CPU",
        "seconds",
        "lower_is_better",
        path_metric("resources", "stack", "cpu_seconds"),
    ),
    Metric(
        "stack_memory_max_bytes",
        "Whole-stack peak memory.current",
        "bytes",
        "lower_is_better",
        path_metric("resources", "stack", "memory_current_bytes", "max"),
    ),
)
METRIC_BY_NAME = {metric.name: metric for metric in METRICS}


BENCHMARK_KEY_FIELDS: tuple[str, ...] = (
    "profile",
    "queue",
    "queues",
    "consumer_group",
    "workers",
    "min_workers",
    "max_workers",
    "strategy",
    "balance_cooldown",
    "balance_max_shift",
    "scale_down_delay",
    "target_jobs_per_process",
    "target_clear_seconds",
    "default_runtime_seconds",
    "poll_interval",
    "block_for",
    "worker_sleep",
    "timeout",
    "retry_after",
    "worker_memory",
    "dispatch_mode",
    "ledger_mode",
    "failed_driver",
    "queen_prefetch",
    "queen_ack_batch",
    "queen_bulk_batch",
    "queen_partitions",
    "queen_pop_fusion",
)

QUEEN_CONNECTION_KEY_FIELDS: tuple[str, ...] = (
    "timeout",
    "retry_attempts",
    "retry_delay",
    "load_balancing_strategy",
    "enable_failover",
    "affinity_hash_ring",
    "health_retry_after",
    "retry_429",
    "partition_prefix",
    "after_commit",
    "lease_renewal",
)

METADATA_SETTING_FIELDS: tuple[str, ...] = (
    "profiles",
    "engines",
    "runs",
    "jobs",
    "workers",
    "min_workers",
    "max_workers",
    "sleep_ms",
    "cpu_iterations",
    "dispatch_mode",
    "ledger_mode",
    "durability",
    "queues",
    "failed_driver",
    "lease_renewal",
    "queen_prefetch",
    "queen_ack_batch",
    "queen_bulk_batch",
    "queen_partitions",
    "queen_pop_fusion",
    "sample_interval_seconds",
    "warmup_jobs",
    "completion_timeout_seconds",
    "pss_requested",
    "post_drain_seconds_by_profile",
    "autoscaling_strategy",
    "balance_cooldown_seconds",
    "balance_max_shift",
    "target_jobs_per_process",
    "target_clear_seconds",
)

EXPECTED_CONNECTION = {
    "horizon": "redis",
    "queen-php": "queen",
    "queen-rust": "queen",
}
EXPECTED_BACKENDS = {
    "horizon": ("redis",),
    "queen-php": ("broker", "postgres"),
    "queen-rust": ("broker", "postgres"),
}


class InputError(ValueError):
    """Raised for an unusable campaign input or CLI destination."""


def read_json_object(path: Path) -> tuple[dict[str, Any] | None, str | None]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None, f"missing artifact: {path.name}"
    except OSError as exception:
        return None, f"cannot read {path.name}: {exception}"
    except json.JSONDecodeError as exception:
        return None, f"invalid JSON in {path.name}: {exception}"
    if not isinstance(value, dict):
        return None, f"{path.name} must contain a JSON object"
    return value, None


def canonical_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): canonical_value(item) for key, item in sorted(value.items())}
    if isinstance(value, (list, tuple)):
        return [canonical_value(item) for item in value]
    if isinstance(value, float) and math.isfinite(value) and value.is_integer():
        return int(value)
    return value


def canonical_json(value: Any) -> str:
    return json.dumps(
        canonical_value(value),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def digest(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def require_fields(
    source: Any,
    fields: Iterable[str],
    prefix: str,
    errors: list[str],
) -> dict[str, Any]:
    if not isinstance(source, Mapping):
        errors.append(f"{prefix} must be an object")
        return {}
    selected: dict[str, Any] = {}
    for field in fields:
        if field not in source or source[field] is None:
            errors.append(f"missing comparison field: {prefix}.{field}")
        else:
            selected[field] = canonical_value(source[field])
    return selected


def scalar_text(raw: str) -> str:
    value = raw.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        value = value[1:-1]
    return value


def parse_compose_resources(path: Path) -> tuple[dict[str, dict[str, str]], list[str]]:
    """Parse only scalar resource keys from deterministic `docker compose config` YAML.

    This is deliberately not a general YAML parser.  An unfamiliar shape is a
    comparison-key error rather than an invitation to guess.
    """

    errors: list[str] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return {}, ["missing comparison artifact: compose-resolved.yml"]
    except OSError as exception:
        return {}, [f"cannot read compose-resolved.yml: {exception}"]

    in_services = False
    service: str | None = None
    resources: dict[str, dict[str, str]] = {}
    service_pattern = re.compile(r"^  ([A-Za-z0-9_.-]+):\s*$")
    scalar_pattern = re.compile(
        r"^    (cpus|mem_limit|pids_limit|stop_grace_period):\s*(.*?)\s*$"
    )
    for line in lines:
        if line == "services:":
            in_services = True
            service = None
            continue
        if in_services and line and not line.startswith(" "):
            break
        if not in_services:
            continue
        service_match = service_pattern.match(line)
        if service_match:
            service = service_match.group(1)
            resources.setdefault(service, {})
            continue
        scalar_match = scalar_pattern.match(line)
        if service is not None and scalar_match:
            resources[service][scalar_match.group(1)] = scalar_text(scalar_match.group(2))

    if not in_services:
        errors.append("compose-resolved.yml has no top-level services mapping")
    return resources, errors


def decimal_scalar(value: Any, label: str, errors: list[str]) -> Decimal | None:
    if not isinstance(value, str) or not value:
        errors.append(f"missing resource setting: {label}")
        return None
    try:
        number = Decimal(value)
    except InvalidOperation:
        errors.append(f"invalid decimal resource setting: {label}={value!r}")
        return None
    if not number.is_finite() or number < 0:
        errors.append(f"resource setting must be finite and non-negative: {label}")
        return None
    return number


def integer_scalar(value: Any, label: str, errors: list[str]) -> int | None:
    if not isinstance(value, str) or not value or not value.isdigit():
        errors.append(f"invalid integer resource setting: {label}={value!r}")
        return None
    return int(value)


def decimal_text(value: Decimal) -> str:
    text = format(value.normalize(), "f")
    return "0" if text in {"-0", ""} else text


def normalized_resources(
    engine: str,
    compose: Mapping[str, Mapping[str, str]],
    errors: list[str],
) -> tuple[dict[str, Any], dict[str, Any]]:
    if engine not in EXPECTED_BACKENDS:
        errors.append(f"unsupported engine for resource normalization: {engine}")
        return {}, {}
    app = compose.get(engine)
    if not isinstance(app, Mapping):
        errors.append(f"compose service missing for engine: {engine}")
        return {}, {}

    app_cpus = decimal_scalar(app.get("cpus"), f"services.{engine}.cpus", errors)
    app_memory = integer_scalar(app.get("mem_limit"), f"services.{engine}.mem_limit", errors)
    app_pids = integer_scalar(app.get("pids_limit"), f"services.{engine}.pids_limit", errors)
    stop_grace = app.get("stop_grace_period")
    if not isinstance(stop_grace, str) or not stop_grace:
        errors.append(f"missing resource setting: services.{engine}.stop_grace_period")

    backend_cpu = Decimal(0)
    backend_memory = 0
    backend_detail: dict[str, Any] = {}
    for backend_name in EXPECTED_BACKENDS[engine]:
        backend = compose.get(backend_name)
        if not isinstance(backend, Mapping):
            errors.append(f"compose backend service missing: {backend_name}")
            continue
        cpu = decimal_scalar(backend.get("cpus"), f"services.{backend_name}.cpus", errors)
        memory = integer_scalar(
            backend.get("mem_limit"), f"services.{backend_name}.mem_limit", errors
        )
        if cpu is not None:
            backend_cpu += cpu
        if memory is not None:
            backend_memory += memory
        backend_detail[backend_name] = {
            "cpus": decimal_text(cpu) if cpu is not None else None,
            "memory_bytes": memory,
            "pids_limit": integer_scalar(
                backend.get("pids_limit"), f"services.{backend_name}.pids_limit", errors
            ),
        }

    pair_resources = {
        "app": {
            "cpus": decimal_text(app_cpus) if app_cpus is not None else None,
            "memory_bytes": app_memory,
            "pids_limit": app_pids,
            "stop_grace_period": stop_grace,
        },
        # Backend topology is an expected product difference.  The normalized
        # comparison contract therefore requires equal aggregate CPU/memory.
        "backend_budget": {
            "cpus": decimal_text(backend_cpu),
            "memory_bytes": backend_memory,
        },
    }
    replicate_resources = pair_resources | {"backend_services": backend_detail}
    return pair_resources, replicate_resources


def nonnegative_integer(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else None


def validate_summary(summary: Mapping[str, Any] | None) -> tuple[bool, list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []
    if summary is None:
        return False, ["summary.json is unavailable"], warnings
    if summary.get("schema") != SUMMARY_SCHEMA:
        errors.append(f"summary schema must be {SUMMARY_SCHEMA}")

    correctness = summary.get("correctness")
    if not isinstance(correctness, Mapping):
        errors.append("correctness must be an object")
        correctness = {}
    if correctness.get("correct") is not True:
        errors.append("correctness.correct is not true")
    if correctness.get("complete") is not True:
        errors.append("correctness.complete is not true")
    expected = nonnegative_integer(correctness.get("expected"))
    completed = nonnegative_integer(correctness.get("unique_completed"))
    if expected is None or completed is None or completed != expected:
        errors.append("unique completion count does not equal expected")
    for name in ("missing", "duplicates", "unexpected"):
        count = nonnegative_integer(value_at(correctness, name, "count"))
        if count != 0:
            errors.append(f"correctness.{name}.count is not zero")
    for name in ("count", "records"):
        if nonnegative_integer(value_at(correctness, "failed", name)) != 0:
            errors.append(f"correctness.failed.{name} is not zero")
    if correctness.get("attempts_valid") is not True:
        errors.append("correctness.attempts_valid is not true")
    for name in (
        "foreign_records",
        "invalid_records",
        "malformed_lines",
        "partial_lines_ignored",
        "unreadable_files",
    ):
        if nonnegative_integer(correctness.get(name)) != 0:
            errors.append(f"correctness.{name} is not zero")
    if correctness.get("queue_quiescent") is not True:
        errors.append("correctness.queue_quiescent is not true")

    queue_state = summary.get("queue_state")
    if not isinstance(queue_state, Mapping):
        errors.append("queue_state artifact is missing from summary")
        queue_state = {}
    if queue_state.get("artifact_valid") is not True:
        errors.append("queue_state.artifact_valid is not true")
    if queue_state.get("quiescent") is not True:
        errors.append("queue_state.quiescent is not true")
    if queue_state.get("gate_passed") is not True:
        errors.append("queue_state.gate_passed is not true")
    queue_validation_errors = queue_state.get("validation_errors")
    if (
        not isinstance(queue_validation_errors, list)
        or any(not isinstance(item, str) for item in queue_validation_errors)
    ):
        errors.append("queue_state.validation_errors is not a list of strings")
    elif queue_validation_errors:
        errors.append("queue_state.validation_errors is not empty")
    if nonnegative_integer(value_at(queue_state, "state", "size")) != 0:
        errors.append("queue_state.state.size is not zero")
    for name in ("ready", "reserved", "delayed"):
        supported = value_at(queue_state, "supported", name)
        count = nonnegative_integer(value_at(queue_state, "state", name))
        if supported is True and count != 0:
            errors.append(f"queue_state.state.{name} is not zero")
        elif supported not in (True, False):
            errors.append(f"queue_state.supported.{name} is not boolean")

    integrity = value_at(summary, "resources", "stats_integrity")
    if not isinstance(integrity, Mapping):
        errors.append("resources.stats_integrity is missing")
    else:
        if nonnegative_integer(integrity.get("integrity_errors")) != 0:
            errors.append("sampler integrity_errors is not zero")
        if nonnegative_integer(integrity.get("oom_events")) != 0:
            errors.append("sampler observed OOM events")
        if (nonnegative_integer(integrity.get("samples")) or 0) < 1:
            errors.append("sampler has no samples")
        if integrity.get("pss_requested") is True and integrity.get("pss_complete") is not True:
            errors.append("requested PSS sampling is incomplete")

    raw_warnings = summary.get("warnings")
    if isinstance(raw_warnings, list):
        warnings.extend(str(value) for value in raw_warnings if isinstance(value, str))
    return not errors, errors, warnings


def validate_container_isolation(
    artifact: Mapping[str, Any] | None,
) -> tuple[bool, list[str]]:
    errors: list[str] = []
    if artifact is None:
        return False, ["continuous container-isolation artifact is unavailable"]
    if artifact.get("schema") != CONTAINER_ISOLATION_SCHEMA:
        errors.append(
            f"container isolation schema must be {CONTAINER_ISOLATION_SCHEMA}"
        )
    if artifact.get("mode") != "continuous_start_event_watch":
        errors.append("container isolation mode is not continuous_start_event_watch")
    if artifact.get("active") is not False:
        errors.append("container isolation watch did not record a final state")
    if artifact.get("ownership_mode") != "exact_container_ids":
        errors.append("container isolation watch did not freeze exact container IDs")
    allowed_ids = artifact.get("allowed_container_ids")
    if (
        not isinstance(allowed_ids, list)
        or not allowed_ids
        or any(not isinstance(item, str) or not item for item in allowed_ids)
    ):
        errors.append("container isolation allowed_container_ids is not a non-empty list")
    watch_errors = artifact.get("errors")
    if not isinstance(watch_errors, list) or any(
        not isinstance(item, str) for item in watch_errors
    ):
        errors.append("container isolation errors is not a string list")
    elif watch_errors:
        errors.append("container isolation watch reported errors")
    start_events = artifact.get("start_events")
    if not isinstance(start_events, Mapping):
        errors.append("container isolation start_events is missing")
    else:
        if nonnegative_integer(start_events.get("invalid_count")) != 0:
            errors.append("container isolation start_events contains invalid records")
        for field in ("record_count", "foreign_count"):
            if nonnegative_integer(start_events.get(field)) is None:
                errors.append(f"container isolation start_events.{field} is invalid")
    for phase in ("initial_inventory", "final_inventory"):
        inventory = artifact.get(phase)
        if not isinstance(inventory, Mapping) or inventory.get("gate_passed") is not True:
            errors.append(f"container isolation {phase} gate did not pass")
    foreign_detected = artifact.get("foreign_detected")
    restart_detected = artifact.get("restart_detected")
    override_requested = artifact.get("override_requested")
    if not isinstance(foreign_detected, bool):
        errors.append("container isolation foreign_detected is not boolean")
    if not isinstance(override_requested, bool):
        errors.append("container isolation override_requested is not boolean")
    if restart_detected is not False:
        errors.append("container isolation restart_detected is not false")
    if artifact.get("event_integrity_valid") is not True:
        errors.append("container isolation event_integrity_valid is not true")
    if foreign_detected is True and override_requested is not True:
        errors.append("foreign container activity was not covered by a diagnostic override")
    if artifact.get("gate_passed") is not True:
        errors.append("container isolation gate_passed is not true")
    return not errors, errors


def expected_cells(metadata: Mapping[str, Any] | None) -> set[tuple[str, str, str]]:
    settings = value_at(metadata, "settings")
    if not isinstance(settings, Mapping):
        return set()
    engines = settings.get("engines")
    profiles = settings.get("profiles")
    runs = nonnegative_integer(settings.get("runs"))
    if (
        not isinstance(engines, list)
        or not engines
        or any(not isinstance(item, str) or not item for item in engines)
        or not isinstance(profiles, list)
        or not profiles
        or any(not isinstance(item, str) or not item for item in profiles)
        or runs is None
        or runs < 1
    ):
        return set()
    return {
        (engine, profile, f"r{repetition:02d}")
        for engine in engines
        for profile in profiles
        for repetition in range(1, runs + 1)
    }


def discovered_cells(campaign: Path) -> set[tuple[str, str, str]]:
    cells: set[tuple[str, str, str]] = set()
    for path in campaign.glob("*/*/*/summary.json"):
        try:
            relative = path.relative_to(campaign)
        except ValueError:
            continue
        engine, profile, repetition, _ = relative.parts
        cells.add((engine, profile, repetition))
    return cells


def expected_dispatch_batch(mode: Any, config: Mapping[str, Any]) -> int | None:
    if mode == "single":
        return 1
    if mode == "bulk":
        return nonnegative_integer(config.get("queen_bulk_batch"))
    return None


def build_comparison_keys(
    *,
    campaign_metadata: Mapping[str, Any] | None,
    configuration: Mapping[str, Any] | None,
    summary: Mapping[str, Any] | None,
    compose_path: Path,
    engine: str,
    profile: str,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None, list[str]]:
    errors: list[str] = []
    if campaign_metadata is None:
        errors.append("campaign metadata is unavailable")
        metadata: Mapping[str, Any] = {}
    else:
        metadata = campaign_metadata
        if metadata.get("schema") != CAMPAIGN_SCHEMA:
            errors.append(f"campaign metadata schema must be {CAMPAIGN_SCHEMA}")
        if not isinstance(metadata.get("campaign_id"), str) or not metadata["campaign_id"]:
            errors.append("metadata.campaign_id must be a non-empty string")
        if value_at(metadata, "git", "dirty") is not False:
            errors.append("campaign git.dirty must be false for paired ratios")

    if configuration is None:
        errors.append("configuration.json is unavailable")
        configuration = {}
    if summary is None:
        errors.append("summary.json is unavailable")
        summary = {}

    benchmark = configuration.get("benchmark")
    benchmark_key = require_fields(
        benchmark, BENCHMARK_KEY_FIELDS, "configuration.benchmark", errors
    )
    queen_connection = require_fields(
        configuration.get("queen_connection"),
        QUEEN_CONNECTION_KEY_FIELDS,
        "configuration.queen_connection",
        errors,
    )
    versions = require_fields(
        configuration,
        ("php", "laravel", "horizon", "queen_client"),
        "configuration",
        errors,
    )
    settings = require_fields(
        metadata.get("settings"),
        METADATA_SETTING_FIELDS,
        "metadata.settings",
        errors,
    )
    campaign_environment = {
        "git_commit": value_at(metadata, "git", "commit"),
        "app_image": value_at(metadata, "images", "app"),
        "host_machine": value_at(metadata, "host", "machine"),
        "docker_architecture": value_at(metadata, "docker", "Architecture"),
        "docker_cgroup_version": value_at(metadata, "docker", "CgroupVersion"),
        "docker_kernel": value_at(metadata, "docker", "KernelVersion"),
        "docker_server": value_at(metadata, "docker", "ServerVersion"),
    }
    for key, value in campaign_environment.items():
        if value is None or value == "":
            errors.append(f"missing comparison field: metadata environment.{key}")

    manifest = summary.get("manifest")
    manifest_key = require_fields(
        manifest,
        (
            "jobs",
            "queue",
            "sleep_ms",
            "cpu_iterations",
            "dispatch_mode",
            "dispatch_batch_size",
            "ledger_mode",
        ),
        "summary.manifest",
        errors,
    )
    integrity = value_at(summary, "resources", "stats_integrity")
    sampling_key = require_fields(
        integrity,
        ("expected_interval_ns", "pss_requested"),
        "summary.resources.stats_integrity",
        errors,
    )
    headline_window = value_at(summary, "resources", "headline_window")
    if not isinstance(headline_window, str) or not headline_window:
        errors.append("missing comparison field: summary.resources.headline_window")
    else:
        sampling_key["headline_window"] = headline_window

    expected_connection = EXPECTED_CONNECTION.get(engine)
    actual_connections = {
        "configuration": value_at(benchmark, "connection"),
        "manifest": value_at(manifest, "connection"),
    }
    if expected_connection is None:
        errors.append(f"unsupported engine: {engine}")
    else:
        for source, actual in actual_connections.items():
            if actual != expected_connection:
                errors.append(
                    f"{source} connection {actual!r} does not match expected "
                    f"{expected_connection!r} for {engine}"
                )
    if value_at(benchmark, "profile") != profile:
        errors.append("configuration benchmark profile does not match directory profile")

    metadata_profiles = value_at(metadata, "settings", "profiles")
    if not isinstance(metadata_profiles, list) or profile not in metadata_profiles:
        errors.append("directory profile is absent from metadata.settings.profiles")
    metadata_engines = value_at(metadata, "settings", "engines")
    if not isinstance(metadata_engines, list) or engine not in metadata_engines:
        errors.append("directory engine is absent from metadata.settings.engines")

    consistency_pairs = (
        ("jobs", value_at(manifest, "jobs"), value_at(metadata, "settings", "jobs")),
        ("sleep_ms", value_at(manifest, "sleep_ms"), value_at(metadata, "settings", "sleep_ms")),
        (
            "cpu_iterations",
            value_at(manifest, "cpu_iterations"),
            value_at(metadata, "settings", "cpu_iterations"),
        ),
        (
            "dispatch_mode",
            value_at(manifest, "dispatch_mode"),
            value_at(metadata, "settings", "dispatch_mode"),
        ),
        (
            "manifest ledger_mode",
            value_at(manifest, "ledger_mode"),
            value_at(metadata, "settings", "ledger_mode"),
        ),
        (
            "benchmark ledger_mode",
            value_at(benchmark, "ledger_mode"),
            value_at(metadata, "settings", "ledger_mode"),
        ),
        (
            "queues",
            value_at(benchmark, "queues"),
            value_at(metadata, "settings", "queues"),
        ),
        (
            "failed_driver",
            value_at(benchmark, "failed_driver"),
            value_at(metadata, "settings", "failed_driver"),
        ),
        (
            "lease_renewal",
            value_at(configuration, "queen_connection", "lease_renewal"),
            value_at(metadata, "settings", "lease_renewal"),
        ),
        ("workers", value_at(benchmark, "workers"), value_at(metadata, "settings", "workers")),
        (
            "queen_prefetch",
            value_at(benchmark, "queen_prefetch"),
            value_at(metadata, "settings", "queen_prefetch"),
        ),
        (
            "queen_ack_batch",
            value_at(benchmark, "queen_ack_batch"),
            value_at(metadata, "settings", "queen_ack_batch"),
        ),
        (
            "queen_bulk_batch",
            value_at(benchmark, "queen_bulk_batch"),
            value_at(metadata, "settings", "queen_bulk_batch"),
        ),
        (
            "queen_partitions",
            value_at(benchmark, "queen_partitions"),
            value_at(metadata, "settings", "queen_partitions"),
        ),
        (
            "queen_pop_fusion",
            value_at(benchmark, "queen_pop_fusion"),
            value_at(metadata, "settings", "queen_pop_fusion"),
        ),
    )
    for label, actual, declared in consistency_pairs:
        if actual != declared:
            errors.append(
                f"run {label} disagrees with campaign metadata: run={actual!r}, metadata={declared!r}"
            )
    dispatch_mode = value_at(manifest, "dispatch_mode")
    declared_batch = expected_dispatch_batch(dispatch_mode, benchmark if isinstance(benchmark, Mapping) else {})
    if value_at(manifest, "dispatch_batch_size") != declared_batch:
        errors.append("manifest dispatch_batch_size disagrees with dispatch mode/configuration")

    sample_seconds = finite_number(value_at(metadata, "settings", "sample_interval_seconds"))
    sample_ns = nonnegative_integer(value_at(integrity, "expected_interval_ns"))
    if sample_seconds is None or sample_ns is None or abs(sample_ns - sample_seconds * 1e9) > 1:
        errors.append("sampler interval disagrees with campaign metadata")

    compose, compose_errors = parse_compose_resources(compose_path)
    errors.extend(compose_errors)
    pair_resources, replicate_resources = normalized_resources(engine, compose, errors)

    if errors:
        return None, None, errors
    pair_key = canonical_value(
        {
            "campaign_environment": campaign_environment,
            "campaign_settings": settings,
            "versions": versions,
            "workload": manifest_key,
            "benchmark": benchmark_key,
            "queen_transport": queen_connection,
            "sampling": sampling_key,
            "resources": pair_resources,
        }
    )
    replicate_key = canonical_value(
        pair_key
        | {
            "engine_identity": {
                "engine": engine,
                "connection": expected_connection,
                "resources": replicate_resources,
            }
        }
    )
    return pair_key, replicate_key, []


def extract_metrics(summary: Mapping[str, Any] | None) -> dict[str, float | None]:
    if summary is None:
        return {metric.name: None for metric in METRICS}
    return {metric.name: metric.getter(summary) for metric in METRICS}


def collect_run(
    campaign: Path,
    metadata: Mapping[str, Any] | None,
    engine: str,
    profile: str,
    repetition: str,
) -> dict[str, Any]:
    run_directory = campaign / engine / profile / repetition
    summary, summary_error = read_json_object(run_directory / "summary.json")
    configuration, configuration_error = read_json_object(run_directory / "configuration.json")
    isolation, isolation_error = read_json_object(
        run_directory / "container-isolation.measurement.json"
    )
    valid, validation_errors, validation_warnings = validate_summary(summary)
    if summary_error is not None:
        validation_errors.insert(0, summary_error)
        valid = False
    isolation_valid, isolation_errors = validate_container_isolation(isolation)
    if isolation_error is not None:
        isolation_errors.insert(0, isolation_error)
    if not isolation_valid or isolation_error is not None:
        validation_errors.extend(isolation_errors)
        valid = False
    repetition_match = REPETITION_PATTERN.fullmatch(repetition)
    if repetition_match is None:
        validation_errors.append("repetition directory must match rXX")
        valid = False
    else:
        declared_runs = nonnegative_integer(value_at(metadata, "settings", "runs"))
        repetition_number = int(repetition[1:])
        if (
            declared_runs is not None
            and (repetition_number < 1 or repetition_number > declared_runs)
        ):
            validation_errors.append(
                "repetition directory is outside metadata.settings.runs: "
                f"{repetition!r} not in r01..r{declared_runs:02d}"
            )
            valid = False
    expected_run_id = f"{engine}-{profile}-{repetition}"
    for label, actual_run_id in (
        ("summary.run_id", value_at(summary, "run_id")),
        ("summary.manifest.run_id", value_at(summary, "manifest", "run_id")),
    ):
        if actual_run_id != expected_run_id:
            validation_errors.append(
                f"{label} does not match directory identity: expected {expected_run_id!r}"
            )
            valid = False

    pair_key, replicate_key, comparison_errors = build_comparison_keys(
        campaign_metadata=metadata,
        configuration=configuration,
        summary=summary,
        compose_path=run_directory / "compose-resolved.yml",
        engine=engine,
        profile=profile,
    )
    if configuration_error is not None and configuration_error not in comparison_errors:
        comparison_errors.insert(0, configuration_error)

    queue_state = summary.get("queue_state") if isinstance(summary, Mapping) else None
    correctness = summary.get("correctness") if isinstance(summary, Mapping) else None
    return {
        "engine": engine,
        "profile": profile,
        "repetition": repetition,
        "run_directory": str(run_directory.resolve()),
        "summary_path": str((run_directory / "summary.json").resolve()),
        "validation": {
            "valid": valid,
            "errors": validation_errors,
            "warnings": validation_warnings,
            "correctness_correct": value_at(correctness, "correct") is True,
            "queue_quiescent": value_at(queue_state, "gate_passed") is True,
            "container_isolation_gate": value_at(isolation, "gate_passed") is True,
        },
        "metrics": extract_metrics(summary),
        "comparison": {
            "key_valid": pair_key is not None and replicate_key is not None,
            "errors": comparison_errors,
            "pair_key_hash": digest(pair_key) if pair_key is not None else None,
            "replicate_key_hash": digest(replicate_key) if replicate_key is not None else None,
            "pair_key": pair_key,
            "replicate_key": replicate_key,
            "group_consistent": None,
        },
    }


def repetition_sort_key(label: str) -> tuple[int, str]:
    match = re.fullmatch(r"r([0-9]+)", label)
    return (int(match.group(1)), label) if match else (sys.maxsize, label)


def run_sort_key(run: Mapping[str, Any], baseline: str) -> tuple[Any, ...]:
    return (
        str(run["profile"]),
        repetition_sort_key(str(run["repetition"])),
        0 if run["engine"] == baseline else 1,
        str(run["engine"]),
    )


def mark_group_consistency(runs: list[dict[str, Any]]) -> None:
    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for run in runs:
        groups.setdefault((run["engine"], run["profile"]), []).append(run)
    for group_runs in groups.values():
        hashes = {
            value_at(run, "comparison", "replicate_key_hash")
            for run in group_runs
            if value_at(run, "comparison", "replicate_key_hash") is not None
        }
        consistent = len(hashes) <= 1
        for run in group_runs:
            own_key_known = value_at(run, "comparison", "replicate_key_hash") is not None
            run["comparison"]["group_consistent"] = consistent if own_key_known else None
            if not consistent:
                run["comparison"]["errors"].append(
                    "replicate configuration/resources differ within the engine/profile group"
                )


def quantile(values: Sequence[float], probability: float) -> float:
    if not values:
        raise ValueError("quantile requires at least one value")
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] * (1 - fraction) + ordered[upper] * fraction


def descriptive(values: Iterable[float]) -> dict[str, Any]:
    clean = sorted(value for value in values if math.isfinite(value))
    if not clean:
        return {
            "n": 0,
            "median": None,
            "q1": None,
            "q3": None,
            "iqr": None,
            "min": None,
            "max": None,
        }
    q1 = quantile(clean, 0.25)
    q3 = quantile(clean, 0.75)
    return {
        "n": len(clean),
        "median": statistics.median(clean),
        "q1": q1,
        "q3": q3,
        "iqr": q3 - q1,
        "min": clean[0],
        "max": clean[-1],
    }


def derived_seed(seed: int, *labels: str) -> int:
    material = "\0".join((str(seed), *labels)).encode("utf-8")
    return int.from_bytes(hashlib.sha256(material).digest()[:8], "big")


def bootstrap_median_ci(
    values: Sequence[float],
    *,
    seed: int,
    resamples: int,
) -> dict[str, Any] | None:
    if not values:
        return None
    generator = random.Random(seed)
    count = len(values)
    estimates = [
        statistics.median(values[generator.randrange(count)] for _ in range(count))
        for _ in range(resamples)
    ]
    alpha = (1 - CONFIDENCE_LEVEL) / 2
    return {
        "method": "percentile bootstrap of paired-ratio median",
        "confidence_level": CONFIDENCE_LEVEL,
        "resamples": resamples,
        "derived_seed": seed,
        "low": quantile(estimates, alpha),
        "high": quantile(estimates, 1 - alpha),
    }


def aggregate_groups(runs: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[Mapping[str, Any]]] = {}
    for run in runs:
        grouped.setdefault((str(run["engine"]), str(run["profile"])), []).append(run)
    result: list[dict[str, Any]] = []
    for (engine, profile), group_runs in sorted(grouped.items(), key=lambda item: item[0]):
        valid_runs = [run for run in group_runs if value_at(run, "validation", "valid") is True]
        replicate_configuration_consistent = all(
            value_at(run, "comparison", "group_consistent") is True for run in group_runs
        )
        # A single run cannot be mixed with a divergent replicate. Preserve its
        # descriptive value even if its comparison key is unavailable, while
        # suppressing multi-run aggregates unless every replicate key agrees.
        aggregate_eligible = len(group_runs) == 1 or replicate_configuration_consistent
        aggregate_runs = valid_runs if aggregate_eligible else []
        metric_stats = {
            metric.name: descriptive(
                value
                for run in aggregate_runs
                if (value := finite_number(value_at(run, "metrics", metric.name))) is not None
            )
            for metric in METRICS
        }
        result.append(
            {
                "engine": engine,
                "profile": profile,
                "runs_total": len(group_runs),
                "runs_valid": len(valid_runs),
                "runs_invalid": len(group_runs) - len(valid_runs),
                "runs_aggregated": len(aggregate_runs),
                "replicate_configuration_consistent": replicate_configuration_consistent,
                "aggregate_suppressed": not aggregate_eligible,
                "aggregate_suppression_reason": (
                    None
                    if aggregate_eligible
                    else "replicate configuration/resources differ or are unverifiable"
                ),
                "metrics": metric_stats,
            }
        )
    return result


def flatten(value: Any, prefix: str = "") -> dict[str, Any]:
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key in sorted(value):
            child = f"{prefix}.{key}" if prefix else str(key)
            result.update(flatten(value[key], child))
        return result
    if isinstance(value, list):
        return {prefix: value}
    return {prefix: value}


def comparison_key_mismatches(baseline: Any, candidate: Any) -> list[str]:
    baseline_flat = flatten(baseline)
    candidate_flat = flatten(candidate)
    mismatches: list[str] = []
    for path in sorted(set(baseline_flat) | set(candidate_flat)):
        left = baseline_flat.get(path, "<missing>")
        right = candidate_flat.get(path, "<missing>")
        if left != right:
            mismatches.append(
                f"comparison key mismatch at {path}: horizon={left!r}, candidate={right!r}"
            )
    return mismatches


def pair_reasons(
    baseline: Mapping[str, Any] | None,
    candidate: Mapping[str, Any] | None,
) -> list[str]:
    reasons: list[str] = []
    if baseline is None:
        return ["baseline Horizon run is missing for this repetition"]
    if candidate is None:
        return ["candidate run is missing for this repetition"]
    if value_at(baseline, "validation", "valid") is not True:
        reasons.extend(
            f"baseline invalid: {reason}" for reason in value_at(baseline, "validation", "errors") or []
        )
    if value_at(candidate, "validation", "valid") is not True:
        reasons.extend(
            f"candidate invalid: {reason}" for reason in value_at(candidate, "validation", "errors") or []
        )
    for label, run in (("baseline", baseline), ("candidate", candidate)):
        if value_at(run, "comparison", "key_valid") is not True:
            errors = value_at(run, "comparison", "errors") or ["comparison key is unavailable"]
            reasons.extend(f"{label} not comparable: {reason}" for reason in errors)
        elif value_at(run, "comparison", "group_consistent") is not True:
            reasons.append(f"{label} replicate configuration is inconsistent")
    if not reasons:
        reasons.extend(
            comparison_key_mismatches(
                value_at(baseline, "comparison", "pair_key"),
                value_at(candidate, "comparison", "pair_key"),
            )
        )
    return reasons


def build_comparisons(
    runs: Sequence[Mapping[str, Any]],
    *,
    baseline_engine: str,
    seed: int,
    resamples: int,
) -> list[dict[str, Any]]:
    lookup = {
        (str(run["engine"]), str(run["profile"]), str(run["repetition"])): run
        for run in runs
    }
    profiles = sorted({str(run["profile"]) for run in runs})
    engines = sorted({str(run["engine"]) for run in runs if run["engine"] != baseline_engine})
    comparisons: list[dict[str, Any]] = []
    for profile in profiles:
        repetitions = sorted(
            {str(run["repetition"]) for run in runs if run["profile"] == profile},
            key=repetition_sort_key,
        )
        for engine in engines:
            if not any(run["engine"] == engine and run["profile"] == profile for run in runs):
                continue
            pairs: list[dict[str, Any]] = []
            ratio_values: dict[str, list[float]] = {metric.name: [] for metric in METRICS}
            for repetition in repetitions:
                baseline = lookup.get((baseline_engine, profile, repetition))
                candidate = lookup.get((engine, profile, repetition))
                reasons = pair_reasons(baseline, candidate)
                ratios: dict[str, float | None] = {metric.name: None for metric in METRICS}
                metric_errors: dict[str, str] = {}
                if not reasons and baseline is not None and candidate is not None:
                    for metric in METRICS:
                        left = finite_number(value_at(baseline, "metrics", metric.name))
                        right = finite_number(value_at(candidate, "metrics", metric.name))
                        if left is None or right is None:
                            metric_errors[metric.name] = "metric is unavailable in one or both runs"
                        elif left == 0:
                            metric_errors[metric.name] = "Horizon metric is zero; ratio is undefined"
                        else:
                            ratio = right / left
                            if math.isfinite(ratio):
                                ratios[metric.name] = ratio
                                ratio_values[metric.name].append(ratio)
                            else:
                                metric_errors[metric.name] = "ratio is not finite"
                pairs.append(
                    {
                        "repetition": repetition,
                        "eligible": not reasons,
                        "suppression_reasons": reasons,
                        "metric_errors": metric_errors,
                        "ratios": ratios,
                    }
                )

            metrics: dict[str, Any] = {}
            for metric in METRICS:
                values = ratio_values[metric.name]
                cell_seed = derived_seed(seed, profile, engine, metric.name)
                metrics[metric.name] = descriptive(values) | {
                    "pairs_total": len(pairs),
                    "pairs_eligible": len(values),
                    "pairs_suppressed": len(pairs) - len(values),
                    "bootstrap_ci_95": bootstrap_median_ci(
                        values, seed=cell_seed, resamples=resamples
                    )
                }
            eligible_pairs = sum(1 for pair in pairs if pair["eligible"])
            warnings: list[str] = []
            if eligible_pairs < 3:
                warnings.append(
                    "fewer than three eligible pairs; IQR and bootstrap CI are highly unstable"
                )
            comparisons.append(
                {
                    "baseline_engine": baseline_engine,
                    "candidate_engine": engine,
                    "profile": profile,
                    "pairs_total": len(pairs),
                    "pairs_eligible": eligible_pairs,
                    "pairs_suppressed": len(pairs) - eligible_pairs,
                    "paired_runs": pairs,
                    "metrics": metrics,
                    "warnings": warnings,
                }
            )
    return comparisons


def validate_campaign_metadata(
    metadata: Mapping[str, Any] | None, error: str | None
) -> list[str]:
    errors: list[str] = []
    if error is not None:
        errors.append(error)
        return errors
    assert metadata is not None
    if metadata.get("schema") != CAMPAIGN_SCHEMA:
        errors.append(f"metadata schema must be {CAMPAIGN_SCHEMA}")
    if not isinstance(metadata.get("campaign_id"), str) or not metadata["campaign_id"]:
        errors.append("metadata.campaign_id must be a non-empty string")
    return errors


def build_report(
    campaign: Path,
    *,
    baseline_engine: str = DEFAULT_BASELINE,
    seed: int = DEFAULT_SEED,
    resamples: int = DEFAULT_RESAMPLES,
) -> dict[str, Any]:
    campaign = campaign.resolve()
    if not campaign.is_dir():
        raise InputError(f"campaign directory does not exist: {campaign}")
    metadata, metadata_error = read_json_object(campaign / "metadata.json")
    metadata_errors = validate_campaign_metadata(metadata, metadata_error)
    cells = expected_cells(metadata) | discovered_cells(campaign)
    if not cells:
        raise InputError(
            "campaign contains no engine/profile/rXX/summary.json runs and metadata has no expected cells"
        )
    runs = [
        collect_run(campaign, metadata, engine, profile, repetition)
        for engine, profile, repetition in sorted(
            cells, key=lambda cell: (cell[1], repetition_sort_key(cell[2]), cell[0])
        )
    ]
    mark_group_consistency(runs)
    runs.sort(key=lambda run: run_sort_key(run, baseline_engine))
    comparisons = build_comparisons(
        runs,
        baseline_engine=baseline_engine,
        seed=seed,
        resamples=resamples,
    )
    return {
        "schema": REPORT_SCHEMA,
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
        "campaign_directory": str(campaign),
        "campaign": {
            "metadata_valid": not metadata_errors,
            "metadata_errors": metadata_errors,
            "campaign_id": value_at(metadata, "campaign_id"),
            "qualification": value_at(metadata, "qualification"),
            "git": value_at(metadata, "git"),
            "host": value_at(metadata, "host"),
            "docker": value_at(metadata, "docker"),
            "images": value_at(metadata, "images"),
            "settings": value_at(metadata, "settings"),
        },
        "methodology": {
            "baseline_engine": baseline_engine,
            "pairing": "exact profile and repetition label",
            "ratio_definition": "candidate divided by Horizon",
            "invalid_ratio_policy": "suppress each invalid or non-comparable pair",
            "descriptive_statistics": "median and R-7 linearly interpolated Q1/Q3/IQR",
            "bootstrap": {
                "method": "deterministic percentile bootstrap of paired-ratio median",
                "confidence_level": CONFIDENCE_LEVEL,
                "seed": seed,
                "resamples": resamples,
                "cell_seed_derivation": "first 64 bits of SHA-256(seed, profile, engine, metric)",
            },
            "resource_comparability": (
                "equal app limits and equal aggregate backend CPU/memory; backend topology is an "
                "expected Redis versus broker+PostgreSQL difference"
            ),
            "comparison_contract": (
                "per-run canonical keys cover campaign metadata, workload/profile, worker bounds, "
                "dispatch mode and batch, Queen knobs, transport configuration, sampling settings, "
                "and resolved resource limits"
            ),
            "metric_definitions": {
                metric.name: {
                    "label": metric.label,
                    "unit": metric.unit,
                    "direction": metric.direction,
                }
                for metric in METRICS
            },
        },
        "runs": runs,
        "aggregates": aggregate_groups(runs),
        "comparisons": comparisons,
        "quality": {
            "runs_total": len(runs),
            "runs_valid": sum(1 for run in runs if value_at(run, "validation", "valid") is True),
            "runs_invalid": sum(1 for run in runs if value_at(run, "validation", "valid") is not True),
            "all_runs_valid": all(
                value_at(run, "validation", "valid") is True for run in runs
            ),
            "all_queues_quiescent": all(
                value_at(run, "validation", "queue_quiescent") is True for run in runs
            ),
            "all_replicate_groups_consistent": all(
                value_at(run, "comparison", "group_consistent") is True for run in runs
            ),
        },
        "limits": [
            "Bootstrap intervals quantify run-to-run sampling uncertainty only; they do not remove host or design bias.",
            "Intervals with fewer than three eligible pairs are reported but are not reliable inferential evidence.",
            "No multiple-comparison correction is applied across metrics.",
            "Redis and Queen backend durability/topology differ; only aggregate resource budgets are normalized.",
            (
                "Resolved resource comparison deliberately parses only cpus, mem_limit, pids_limit, "
                "and stop_grace_period; missing or unfamiliar values suppress paired ratios."
            ),
            "A diagnostic host qualification remains diagnostic even when every statistical gate passes.",
        ],
    }


def markdown_escape(value: Any) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ")


def decimal(value: Any, digits: int = 2) -> str:
    number = finite_number(value)
    return "—" if number is None else f"{number:.{digits}f}"


def mib(value: Any) -> str:
    number = finite_number(value)
    return "—" if number is None else f"{number / (1024 * 1024):.1f}"


def stat_cell(stats: Mapping[str, Any] | None, *, scale: float = 1.0, digits: int = 2) -> str:
    if not isinstance(stats, Mapping) or not stats.get("n"):
        return "—"
    return (
        f"{float(stats['median']) / scale:.{digits}f} "
        f"[{float(stats['q1']) / scale:.{digits}f}, {float(stats['q3']) / scale:.{digits}f}]"
    )


def ratio_cell(stats: Mapping[str, Any] | None) -> str:
    if not isinstance(stats, Mapping) or not stats.get("n"):
        return "suppressed"
    ci = stats.get("bootstrap_ci_95")
    ci_text = "n/a"
    if isinstance(ci, Mapping):
        ci_text = f"{float(ci['low']):.3f}–{float(ci['high']):.3f}"
    return (
        f"{float(stats['median']):.3f}× "
        f"[{float(stats['q1']):.3f}, {float(stats['q3']):.3f}], CI {ci_text}, "
        f"n={stats['n']}"
    )


def render_markdown(report: Mapping[str, Any]) -> str:
    campaign = report["campaign"]
    method = report["methodology"]
    quality = report["quality"]
    lines = [
        "# Laravel supervisor campaign statistics",
        "",
        f"Campaign: `{markdown_escape(campaign.get('campaign_id') or 'unknown')}`  ",
        f"Qualification: `{markdown_escape(campaign.get('qualification') or 'unknown')}`  ",
        f"Runs valid: **{quality['runs_valid']}/{quality['runs_total']}**  ",
        (
            f"Bootstrap: seed `{method['bootstrap']['seed']}`, "
            f"{method['bootstrap']['resamples']} resamples, 95% percentile CI."
        ),
        "",
        "Ratios are candidate/Horizon and are paired by exact profile and repetition. "
        "Higher is better only for throughput; lower is better for latency, CPU and memory.",
        "",
        "## Every run",
        "",
        "| Engine | Profile | Rep | Valid | Queue idle | Comparable | Completion jobs/s | E2E p95 ms | Stack MiB | Issues |",
        "| --- | --- | --- | --- | --- | --- | ---: | ---: | ---: | --- |",
    ]
    for run in report["runs"]:
        issues = list(value_at(run, "validation", "errors") or [])
        issues.extend(value_at(run, "comparison", "errors") or [])
        lines.append(
            "| {engine} | {profile} | {repetition} | {valid} | {queue} | {comparable} | "
            "{throughput} | {latency} | {memory} | {issues} |".format(
                engine=markdown_escape(run["engine"]),
                profile=markdown_escape(run["profile"]),
                repetition=markdown_escape(run["repetition"]),
                valid="yes" if value_at(run, "validation", "valid") else "NO",
                queue="yes" if value_at(run, "validation", "queue_quiescent") else "NO",
                comparable=(
                    "yes"
                    if value_at(run, "comparison", "key_valid")
                    and value_at(run, "comparison", "group_consistent")
                    else "NO"
                ),
                throughput=decimal(value_at(run, "metrics", "completion_span_jobs_per_second")),
                latency=decimal(value_at(run, "metrics", "end_to_end_p95_ms")),
                memory=mib(value_at(run, "metrics", "stack_memory_max_bytes")),
                issues=markdown_escape("; ".join(issues) if issues else "—"),
            )
        )

    lines.extend(
        [
            "",
            "## Absolute aggregates",
            "",
            "Values are median [Q1, Q3] over valid runs only.",
            "",
            "| Engine | Profile | Aggregated/valid/total | Aggregate gate | Completion jobs/s | E2E p95 ms | Orchestrator PSS MiB | Renewer PSS MiB | Renewer processes | Stack CPU s | Stack MiB |",
            "| --- | --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for group in report["aggregates"]:
        metrics = group["metrics"]
        lines.append(
            "| {engine} | {profile} | {aggregated}/{valid}/{total} | {gate} | {throughput} | {latency} | "
            "{opss} | {rpss} | {rprocs} | {cpu} | {memory} |".format(
                engine=markdown_escape(group["engine"]),
                profile=markdown_escape(group["profile"]),
                valid=group["runs_valid"],
                total=group["runs_total"],
                aggregated=group["runs_aggregated"],
                gate="suppressed" if group["aggregate_suppressed"] else "pass",
                throughput=stat_cell(metrics["completion_span_jobs_per_second"]),
                latency=stat_cell(metrics["end_to_end_p95_ms"]),
                opss=stat_cell(
                    metrics["orchestrator_pss_max_bytes"], scale=1024 * 1024, digits=1
                ),
                rpss=stat_cell(
                    metrics["lease_renewer_pss_max_bytes"], scale=1024 * 1024, digits=1
                ),
                rprocs=stat_cell(metrics["lease_renewer_processes_peak"], digits=1),
                cpu=stat_cell(metrics["stack_cpu_seconds"], digits=3),
                memory=stat_cell(
                    metrics["stack_memory_max_bytes"], scale=1024 * 1024, digits=1
                ),
            )
        )

    lines.extend(
        [
            "",
            "## Paired ratios versus Horizon",
            "",
            "Each cell is median× [Q1, Q3], bootstrap 95% CI. Invalid or non-comparable pairs are omitted, never imputed.",
            "",
            "| Candidate | Profile | Eligible/total | Completion throughput | E2E p95 | Orchestrator PSS | Stack CPU | Stack memory |",
            "| --- | --- | ---: | --- | --- | --- | --- | --- |",
        ]
    )
    for comparison in report["comparisons"]:
        metrics = comparison["metrics"]
        lines.append(
            "| {engine} | {profile} | {eligible}/{total} | {throughput} | {latency} | "
            "{opss} | {cpu} | {memory} |".format(
                engine=markdown_escape(comparison["candidate_engine"]),
                profile=markdown_escape(comparison["profile"]),
                eligible=comparison["pairs_eligible"],
                total=comparison["pairs_total"],
                throughput=ratio_cell(metrics["completion_span_jobs_per_second"]),
                latency=ratio_cell(metrics["end_to_end_p95_ms"]),
                opss=ratio_cell(metrics["orchestrator_pss_max_bytes"]),
                cpu=ratio_cell(metrics["stack_cpu_seconds"]),
                memory=ratio_cell(metrics["stack_memory_max_bytes"]),
            )
        )

    suppressed = [
        (comparison, pair)
        for comparison in report["comparisons"]
        for pair in comparison["paired_runs"]
        if not pair["eligible"]
    ]
    if suppressed:
        lines.extend(
            [
                "",
                "## Suppressed pairs",
                "",
                "| Candidate | Profile | Rep | Reasons |",
                "| --- | --- | --- | --- |",
            ]
        )
        for comparison, pair in suppressed:
            lines.append(
                "| {engine} | {profile} | {rep} | {reasons} |".format(
                    engine=markdown_escape(comparison["candidate_engine"]),
                    profile=markdown_escape(comparison["profile"]),
                    rep=markdown_escape(pair["repetition"]),
                    reasons=markdown_escape("; ".join(pair["suppression_reasons"])),
                )
            )

    lines.extend(["", "## Limits", ""])
    lines.extend(f"- {limit}" for limit in report["limits"])
    lines.append("")
    return "\n".join(lines)


def atomic_write(path: Path, content: str) -> None:
    path = path.resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_name, path)
    except BaseException:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise


def positive_integer(raw: str) -> int:
    try:
        value = int(raw)
    except ValueError as exception:
        raise argparse.ArgumentTypeError("must be an integer") from exception
    if value < 1:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return value


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description=(
            "Aggregate engine/profile/rXX benchmark summaries with deterministic paired bootstrap CIs"
        )
    )
    result.add_argument("campaign", type=Path, help="campaign directory")
    result.add_argument("--json-output", required=True, type=Path, help="JSON report path")
    result.add_argument(
        "--markdown-output", required=True, type=Path, help="Markdown report path"
    )
    result.add_argument("--baseline", default=DEFAULT_BASELINE, help="baseline engine")
    result.add_argument("--seed", type=int, default=DEFAULT_SEED, help="bootstrap seed")
    result.add_argument(
        "--resamples",
        type=positive_integer,
        default=DEFAULT_RESAMPLES,
        help=f"bootstrap resamples (default: {DEFAULT_RESAMPLES})",
    )
    return result


def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if args.json_output.resolve() == args.markdown_output.resolve():
        parser().error("--json-output and --markdown-output must differ")
    try:
        report = build_report(
            args.campaign,
            baseline_engine=args.baseline,
            seed=args.seed,
            resamples=args.resamples,
        )
        atomic_write(
            args.json_output,
            json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        )
        atomic_write(args.markdown_output, render_markdown(report))
    except (InputError, OSError, ValueError) as exception:
        print(f"error: {exception}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

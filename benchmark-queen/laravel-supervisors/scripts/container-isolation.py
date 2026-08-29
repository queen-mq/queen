#!/usr/bin/env python3
"""Snapshot running Docker containers and fail closed on foreign workloads."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import select
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Mapping, Sequence


SCHEMA = "queen.laravel-supervisors.container-isolation/v1"
MAX_WATCH_EVENTS = 10_000
MAX_EVENT_BYTES = 1_048_576


def utc_now() -> str:
    return (
        dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")
    )


def atomic_write(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with temporary.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2, sort_keys=True)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def running_ids() -> list[str]:
    output = subprocess.check_output(
        ["docker", "container", "ls", "--quiet", "--no-trunc"],
        text=True,
        stderr=subprocess.STDOUT,
    )
    return sorted(line.strip() for line in output.splitlines() if line.strip())


def stable_inventory(max_attempts: int = 5) -> tuple[list[dict[str, Any]], int]:
    """Return an inspect snapshot bounded by identical daemon ID lists."""

    for attempt in range(1, max_attempts + 1):
        before = running_ids()
        inspected: list[dict[str, Any]] = []
        if before:
            try:
                raw = subprocess.check_output(
                    ["docker", "container", "inspect", *before],
                    text=True,
                    stderr=subprocess.STDOUT,
                )
                decoded = json.loads(raw)
                if not isinstance(decoded, list) or any(
                    not isinstance(item, dict) for item in decoded
                ):
                    raise RuntimeError("docker inspect did not emit an object array")
                inspected = decoded
            except (subprocess.CalledProcessError, json.JSONDecodeError, RuntimeError):
                if attempt == max_attempts:
                    raise
                time.sleep(0.05)
                continue
        after = running_ids()
        still_running = sorted(
            str(item.get("Id"))
            for item in inspected
            if isinstance(item.get("State"), dict)
            and item["State"].get("Running") is True
            and isinstance(item.get("Id"), str)
        )
        if before == after == still_running:
            return inspected, attempt
        if attempt < max_attempts:
            time.sleep(0.05)
    raise RuntimeError(
        "Docker container inventory changed during every snapshot attempt"
    )


def concise_container(item: dict[str, Any]) -> dict[str, Any]:
    configuration = item.get("Config") if isinstance(item.get("Config"), dict) else {}
    state = item.get("State") if isinstance(item.get("State"), dict) else {}
    labels = (
        configuration.get("Labels")
        if isinstance(configuration.get("Labels"), dict)
        else {}
    )
    return {
        "id": item.get("Id"),
        "name": str(item.get("Name", "")).lstrip("/"),
        "image": configuration.get("Image"),
        "image_id": item.get("Image"),
        "compose_project": labels.get("com.docker.compose.project"),
        "compose_service": labels.get("com.docker.compose.service"),
        "created": item.get("Created"),
        "started_at": state.get("StartedAt"),
        "status": state.get("Status"),
        "running": state.get("Running") is True,
    }


def concise_start_event(item: dict[str, Any]) -> dict[str, Any]:
    actor = item.get("Actor") if isinstance(item.get("Actor"), dict) else {}
    attributes = (
        actor.get("Attributes")
        if isinstance(actor.get("Attributes"), dict)
        else {}
    )
    identifier = item.get("id") or actor.get("ID")
    action = item.get("Action") or item.get("status")
    return {
        "id": identifier,
        "name": attributes.get("name"),
        "image": item.get("from") or attributes.get("image"),
        "compose_project": attributes.get("com.docker.compose.project"),
        "compose_service": attributes.get("com.docker.compose.service"),
        "action": action,
        "time": item.get("time"),
        "time_nano": item.get("timeNano"),
    }


def partition_inventory(
    normalized: list[dict[str, Any]],
    project: str,
    monitor: str,
    allowed_services: Sequence[str],
    allowed_container_ids: Sequence[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    owned: list[dict[str, Any]] = []
    foreign: list[dict[str, Any]] = []
    allowed_service_set = set(allowed_services)
    allowed_id_set = set(allowed_container_ids)
    for item in normalized:
        # A measurement watch receives the exact IDs established before the
        # window. Exact-ID mode rejects replacements and same-project one-off
        # tools; service matching remains the snapshot/backward-compatible mode.
        if allowed_id_set:
            is_owned = item.get("id") in allowed_id_set
        else:
            is_owned = item.get("compose_project") == project and (
                not allowed_service_set
                or item.get("compose_service") in allowed_service_set
            )
            is_owned = is_owned or bool(monitor and item.get("name") == monitor)
        (owned if is_owned else foreign).append(item)
    return owned, foreign


def evaluate_inventory(
    containers: list[dict[str, Any]],
    project: str,
    monitor: str,
    phase: str,
    allow_foreign: bool,
    snapshot_attempts: int = 1,
    allowed_services: Sequence[str] = (),
    allowed_container_ids: Sequence[str] = (),
) -> dict[str, Any]:
    normalized = [concise_container(item) for item in containers]
    allowed_service_set = set(allowed_services)
    allowed_id_set = set(allowed_container_ids)
    owned, foreign = partition_inventory(
        normalized,
        project,
        monitor,
        allowed_services,
        allowed_container_ids,
    )
    isolated = not foreign
    return {
        "schema": SCHEMA,
        "captured_at": utc_now(),
        "phase": phase,
        "expected_compose_project": project,
        "expected_monitor": monitor or None,
        "allowed_compose_services": sorted(allowed_service_set),
        "allowed_container_ids": sorted(allowed_id_set),
        "ownership_mode": "exact_container_ids"
        if allowed_id_set
        else "compose_project_and_service",
        "snapshot_attempts": snapshot_attempts,
        "running_count": len(normalized),
        "owned_count": len(owned),
        "foreign_count": len(foreign),
        "owned": owned,
        "foreign": foreign,
        "isolated": isolated,
        "override_requested": allow_foreign,
        "gate_passed": isolated or allow_foreign,
        "qualification": "isolated"
        if isolated
        else "diagnostic_foreign_container_override",
    }


def evaluate_start_events(
    events: list[dict[str, Any]],
    project: str,
    monitor: str,
    allowed_services: Sequence[str] = (),
    allowed_container_ids: Sequence[str] = (),
) -> dict[str, Any]:
    normalized: list[dict[str, Any]] = []
    invalid = 0
    for event in events:
        item = concise_start_event(event)
        if (
            event.get("Type") != "container"
            or item.get("action") != "start"
            or not isinstance(item.get("id"), str)
            or not item["id"]
        ):
            invalid += 1
            continue
        normalized.append(item)
    owned, foreign = partition_inventory(
        normalized,
        project,
        monitor,
        allowed_services,
        allowed_container_ids,
    )
    return {
        "records": normalized,
        "record_count": len(normalized),
        "invalid_count": invalid,
        "owned": owned,
        "owned_count": len(owned),
        "foreign": foreign,
        "foreign_count": len(foreign),
    }


def evaluate_watch_decision(
    initial: Mapping[str, Any],
    starts: Mapping[str, Any],
    final: Mapping[str, Any] | None,
    allow_foreign: bool,
    errors: Sequence[str],
) -> dict[str, Any]:
    foreign_detected = (
        int(initial.get("foreign_count", 0)) > 0
        or int(starts.get("foreign_count", 0)) > 0
        or int((final or {}).get("foreign_count", 0)) > 0
    )
    # All exact-ID containers are running before the watcher begins. A start
    # event for one of them is therefore a restart and invalidates attribution;
    # the foreign-container diagnostic override must not waive this gate.
    restart_detected = int(starts.get("owned_count", 0)) > 0
    event_integrity_valid = int(starts.get("invalid_count", 0)) == 0
    gate_passed = (
        not errors
        and event_integrity_valid
        and not restart_detected
        and (not foreign_detected or allow_foreign)
    )
    if errors or not event_integrity_valid:
        qualification = "invalid_inventory_watch"
    elif restart_detected:
        qualification = "allowed_container_restart_detected"
    elif foreign_detected and allow_foreign:
        qualification = "diagnostic_foreign_container_override"
    elif foreign_detected:
        qualification = "foreign_container_detected"
    else:
        qualification = "isolated"
    return {
        "foreign_detected": foreign_detected,
        "restart_detected": restart_detected,
        "event_integrity_valid": event_integrity_valid,
        "gate_passed": gate_passed,
        "qualification": qualification,
    }


def write_ready(path: Path, started_at: str) -> None:
    atomic_write(
        path,
        {
            "schema": SCHEMA,
            "state": "watching",
            "started_at": started_at,
        },
    )


def watch_inventory(args: argparse.Namespace) -> dict[str, Any]:
    started_at = utc_now()
    errors: list[str] = []
    events: list[dict[str, Any]] = []
    event_lines_seen = 0
    initial: dict[str, Any]

    def add_error(message: str) -> None:
        if message not in errors:
            errors.append(message)

    try:
        inventory, attempts = stable_inventory()
        initial = evaluate_inventory(
            inventory,
            args.project,
            args.monitor,
            f"{args.phase}-start",
            args.allow_foreign,
            attempts,
            args.allowed_service,
            args.allowed_container,
        )
    except (
        OSError,
        subprocess.CalledProcessError,
        json.JSONDecodeError,
        RuntimeError,
    ) as exception:
        initial = {"gate_passed": False, "foreign_count": 0}
        add_error(f"initial inventory failed: {exception}")

    command = [
        "docker",
        "events",
        "--since",
        started_at,
        "--format",
        "{{json .}}",
        "--filter",
        "type=container",
        "--filter",
        "event=start",
    ]
    process: subprocess.Popen[str] | None = None

    def event_evidence() -> dict[str, Any]:
        return evaluate_start_events(
            events,
            args.project,
            args.monitor,
            args.allowed_service,
            args.allowed_container,
        )

    def payload(active: bool, final: dict[str, Any] | None = None) -> dict[str, Any]:
        starts = event_evidence()
        decision = evaluate_watch_decision(
            initial,
            starts,
            final,
            args.allow_foreign,
            errors,
        )
        return {
            "schema": SCHEMA,
            "mode": "continuous_start_event_watch",
            "phase": args.phase,
            "started_at": started_at,
            "finished_at": None if active else utc_now(),
            "captured_at": utc_now(),
            "active": active,
            "expected_compose_project": args.project,
            "expected_monitor": args.monitor or None,
            "allowed_compose_services": sorted(set(args.allowed_service)),
            "allowed_container_ids": sorted(set(args.allowed_container)),
            "ownership_mode": "exact_container_ids"
            if args.allowed_container
            else "compose_project_and_service",
            "initial_inventory": initial,
            "start_events": starts,
            "event_lines_seen": event_lines_seen,
            "event_limit": MAX_WATCH_EVENTS,
            "final_inventory": final,
            "errors": list(errors),
            "foreign_detected": decision["foreign_detected"],
            "restart_detected": decision["restart_detected"],
            "event_integrity_valid": decision["event_integrity_valid"],
            "override_requested": args.allow_foreign,
            "gate_passed": decision["gate_passed"],
            "qualification": decision["qualification"],
        }

    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
        )
        if process.stdout is None:
            raise RuntimeError("docker events stdout pipe is unavailable")
        atomic_write(Path(args.output), payload(active=True))
        if args.ready_file:
            write_ready(Path(args.ready_file), started_at)

        while not Path(args.watch_until).exists():
            return_code = process.poll()
            if return_code is not None:
                stderr = process.stderr.read() if process.stderr is not None else ""
                add_error(
                    "docker events exited before the measurement window ended"
                    f" (status {return_code}): {stderr.strip()}"
                )
                break
            readable, _, _ = select.select([process.stdout], [], [], 0.1)
            if not readable:
                continue
            line = process.stdout.readline()
            if not line:
                continue
            event_lines_seen += 1
            encoded_size = len(line.encode("utf-8", errors="replace"))
            if encoded_size > MAX_EVENT_BYTES:
                add_error("docker event line exceeded the bounded artifact limit")
            elif len(events) >= MAX_WATCH_EVENTS:
                add_error("docker event count exceeded the bounded artifact limit")
            else:
                try:
                    decoded = json.loads(line)
                    if not isinstance(decoded, dict):
                        raise ValueError("event is not a JSON object")
                    events.append(decoded)
                except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as exception:
                    add_error(f"invalid docker start event: {exception}")
            atomic_write(Path(args.output), payload(active=True))

        if process.poll() is None:
            process.terminate()
        else:
            add_error("docker events was not running at the measurement stop boundary")
        try:
            remaining_stdout, remaining_stderr = process.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            remaining_stdout, remaining_stderr = process.communicate(timeout=5)
            add_error("docker events did not terminate within five seconds")
        for line in remaining_stdout.splitlines():
            if not line.strip():
                continue
            event_lines_seen += 1
            if len(line.encode("utf-8", errors="replace")) > MAX_EVENT_BYTES:
                add_error("docker event line exceeded the bounded artifact limit")
            elif len(events) >= MAX_WATCH_EVENTS:
                add_error("docker event count exceeded the bounded artifact limit")
            else:
                try:
                    decoded = json.loads(line)
                    if not isinstance(decoded, dict):
                        raise ValueError("event is not a JSON object")
                    events.append(decoded)
                except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as exception:
                    add_error(f"invalid docker start event: {exception}")
        if remaining_stderr.strip() and not Path(args.watch_until).exists():
            add_error(f"docker events stderr: {remaining_stderr.strip()}")
    except (OSError, RuntimeError) as exception:
        add_error(f"cannot monitor Docker start events: {exception}")
        if process is not None and process.poll() is None:
            process.kill()
            process.wait()

    try:
        inventory, attempts = stable_inventory()
        final = evaluate_inventory(
            inventory,
            args.project,
            args.monitor,
            f"{args.phase}-end",
            args.allow_foreign,
            attempts,
            args.allowed_service,
            args.allowed_container,
        )
    except (
        OSError,
        subprocess.CalledProcessError,
        json.JSONDecodeError,
        RuntimeError,
    ) as exception:
        final = {"gate_passed": False, "foreign_count": 0}
        add_error(f"final inventory failed: {exception}")

    result = payload(active=False, final=final)
    atomic_write(Path(args.output), result)
    return result


def argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", required=True)
    parser.add_argument("--project", required=True)
    parser.add_argument("--monitor", default="")
    parser.add_argument("--allowed-service", action="append", default=[])
    parser.add_argument("--allowed-container", action="append", default=[])
    parser.add_argument("--output", required=True)
    parser.add_argument("--allow-foreign", action="store_true")
    parser.add_argument(
        "--watch-until",
        help="continuously audit container start events until this file exists",
    )
    parser.add_argument("--ready-file")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = argument_parser().parse_args(argv)
    if args.ready_file and not args.watch_until:
        print("container-isolation.py: --ready-file requires --watch-until", file=sys.stderr)
        return 3
    if args.watch_until:
        result = watch_inventory(args)
        if result["gate_passed"]:
            if result["foreign_detected"]:
                print(
                    "container-isolation.py: diagnostic override retained foreign "
                    "container activity during the measurement window",
                    file=sys.stderr,
                )
            return 0
        if result["errors"]:
            print(
                "container-isolation.py: measurement watch failed: "
                + "; ".join(result["errors"]),
                file=sys.stderr,
            )
            return 3
        if result["restart_detected"]:
            print(
                "container-isolation.py: an allowed lane container restarted "
                "during the measurement window",
                file=sys.stderr,
            )
            return 2
        print(
            "container-isolation.py: foreign container activity occurred during "
            "the measurement window",
            file=sys.stderr,
        )
        return 2
    try:
        inventory, attempts = stable_inventory()
        result = evaluate_inventory(
            inventory,
            args.project,
            args.monitor,
            args.phase,
            args.allow_foreign,
            attempts,
            args.allowed_service,
            args.allowed_container,
        )
    except (
        OSError,
        subprocess.CalledProcessError,
        json.JSONDecodeError,
        RuntimeError,
    ) as exception:
        result = {
            "schema": SCHEMA,
            "captured_at": utc_now(),
            "phase": args.phase,
            "expected_compose_project": args.project,
            "expected_monitor": args.monitor or None,
            "allowed_compose_services": sorted(args.allowed_service),
            "allowed_container_ids": sorted(args.allowed_container),
            "isolated": False,
            "override_requested": args.allow_foreign,
            "gate_passed": False,
            "qualification": "invalid_inventory",
            "error": str(exception),
        }
        atomic_write(Path(args.output), result)
        print(f"container-isolation.py: {exception}", file=sys.stderr)
        return 3

    atomic_write(Path(args.output), result)
    if result["foreign_count"]:
        names = ", ".join(
            f"{item['name']} ({str(item['id'])[:12]})" for item in result["foreign"]
        )
        if args.allow_foreign:
            print(
                "container-isolation.py: diagnostic override retained foreign containers: "
                + names,
                file=sys.stderr,
            )
            return 0
        print(
            "container-isolation.py: foreign running containers: " + names,
            file=sys.stderr,
        )
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

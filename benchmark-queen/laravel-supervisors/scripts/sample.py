#!/usr/bin/env python3
"""Sample a benchmark stack from the host PID and cgroup namespaces.

The sampler is intended to run in a small container with::

    --pid=host --cgroupns=host -v /sys/fs/cgroup:/sys/fs/cgroup:ro

Targets are container init PIDs obtained from the host, expressed as
``label=pid``.  One JSON object is flushed per line, so a run interrupted while
writing can still be analysed up to its last complete line.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import signal
import sys
import time
from pathlib import Path
from typing import Any, Iterable


SCHEMA = "queen.laravel-supervisors.stats/v1"
LABEL_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")
WORKER_MARKERS = ("horizon:work", "queue:work")
LEASE_RENEWER_MARKERS = (
    "leaserenewalworker::main",
    "lease-renewal-worker",
    "lease-renewer",
)
ORCHESTRATOR_MARKERS = (
    "horizon:supervisor",
    "horizon:master",
    "queen:supervise",
    "queen-supervisor",
)
_STOP = False


def utc_now() -> str:
    return (
        dt.datetime.now(dt.timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def request_stop(_signum: int, _frame: object) -> None:
    global _STOP
    _STOP = True


def positive_float(value: str) -> float:
    number = float(value)
    if not 0.01 <= number <= 60.0:
        raise argparse.ArgumentTypeError("must be between 0.01 and 60 seconds")
    return number


def nonnegative_float(value: str) -> float:
    number = float(value)
    if not 0.0 <= number <= 604_800.0:
        raise argparse.ArgumentTypeError("must be between 0 and 604800 seconds")
    return number


def positive_int(value: str) -> int:
    if not value.isascii() or not value.isdigit() or int(value) < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return int(value)


def parse_target(value: str) -> tuple[str, int]:
    if "=" not in value:
        raise argparse.ArgumentTypeError("must use LABEL=HOST_PID")
    label, raw_pid = value.rsplit("=", 1)
    if not LABEL_RE.fullmatch(label):
        raise argparse.ArgumentTypeError(
            "label must contain only letters, digits, dot, underscore, colon or dash"
        )
    if not raw_pid.isascii() or not raw_pid.isdigit() or int(raw_pid) < 1:
        raise argparse.ArgumentTypeError("HOST_PID must be a positive integer")
    return label, int(raw_pid)


def target_kind(label: str) -> str:
    """Map conventional target labels to report resource buckets."""

    normalized = label.lower().replace("_", "-")
    head = re.split(r"[:.-]", normalized, maxsplit=1)[0]
    if head in {"backend", "broker"} or any(
        marker in normalized for marker in ("redis", "queen-broker", "queen-server")
    ):
        return "backend"
    if head == "stack" or any(
        marker in normalized for marker in ("postgres", "database", "loadgen", "producer")
    ):
        return "stack"
    return "app"


def read_text(path: Path, limit: int = 1_048_576) -> str:
    with path.open("rb") as stream:
        return stream.read(limit).decode("utf-8", errors="replace")


def parse_key_values(text: str) -> dict[str, int]:
    values: dict[str, int] = {}
    for line in text.splitlines():
        fields = line.split()
        if len(fields) != 2:
            continue
        try:
            value = int(fields[1])
        except ValueError:
            continue
        if value >= 0:
            values[fields[0]] = value
    return values


def read_optional_int(path: Path) -> int | None:
    try:
        raw = read_text(path, 128).strip()
        value = int(raw)
        return value if value >= 0 else None
    except (FileNotFoundError, PermissionError, OSError, ValueError):
        return None


def unified_cgroup(proc_root: Path, pid: int) -> str | None:
    try:
        for line in read_text(proc_root / str(pid) / "cgroup", 65_536).splitlines():
            fields = line.split(":", 2)
            if len(fields) == 3 and fields[0] == "0" and fields[1] == "":
                path = "/" + fields[2].lstrip("/")
                return os.path.normpath(path)
    except (FileNotFoundError, ProcessLookupError, PermissionError, OSError):
        return None
    return None


def cgroup_directory(cgroup_root: Path, cgroup_path: str | None) -> Path | None:
    if cgroup_path is None:
        return None
    candidate = (cgroup_root / cgroup_path.lstrip("/")).resolve(strict=False)
    try:
        candidate.relative_to(cgroup_root.resolve(strict=False))
    except ValueError:
        return None
    return candidate


def sample_cgroup(directory: Path | None) -> tuple[dict[str, Any] | None, list[str]]:
    if directory is None:
        return None, ["cgroup path is unavailable"]

    errors: list[str] = []
    try:
        cpu = parse_key_values(read_text(directory / "cpu.stat", 65_536))
    except (FileNotFoundError, PermissionError, OSError) as exception:
        cpu = {}
        errors.append(f"cpu.stat: {exception.__class__.__name__}")

    events: dict[str, int] = {}
    try:
        events = parse_key_values(read_text(directory / "memory.events", 65_536))
    except (FileNotFoundError, PermissionError, OSError) as exception:
        errors.append(f"memory.events: {exception.__class__.__name__}")

    local_events: dict[str, int] | None = None
    try:
        local_events = parse_key_values(
            read_text(directory / "memory.events.local", 65_536)
        )
    except (FileNotFoundError, PermissionError, OSError):
        pass

    memory: dict[str, Any] = {
        "current_bytes": read_optional_int(directory / "memory.current"),
        "peak_bytes": read_optional_int(directory / "memory.peak"),
        "swap_current_bytes": read_optional_int(directory / "memory.swap.current"),
        "events": events,
    }
    if local_events is not None:
        memory["events_local"] = local_events

    try:
        inode = directory.stat().st_ino
    except (FileNotFoundError, PermissionError, OSError):
        inode = None

    return (
        {
            "inode": inode,
            "cpu": cpu,
            "memory": memory,
            "pids_current": read_optional_int(directory / "pids.current"),
        },
        errors,
    )


def parse_status(text: str) -> dict[str, str]:
    result: dict[str, str] = {}
    wanted = {
        "Name",
        "State",
        "PPid",
        "Threads",
        "VmRSS",
        "RssAnon",
        "RssFile",
        "RssShmem",
    }
    for line in text.splitlines():
        key, separator, value = line.partition(":")
        if separator and key in wanted:
            result[key] = value.strip()
    return result


def kib_value(value: str | None) -> int | None:
    if value is None:
        return None
    fields = value.split()
    if not fields:
        return None
    try:
        number = int(fields[0])
    except ValueError:
        return None
    return number * 1024 if number >= 0 else None


def integer_value(value: str | None) -> int | None:
    if value is None:
        return None
    fields = value.split()
    if not fields:
        return None
    try:
        number = int(fields[0])
    except ValueError:
        return None
    return number if number >= 0 else None


def process_start_ticks(proc_root: Path, pid: int) -> int | None:
    try:
        stat = read_text(proc_root / str(pid) / "stat", 65_536)
    except (FileNotFoundError, ProcessLookupError, PermissionError, OSError):
        return None
    close = stat.rfind(")")
    if close < 0:
        return None
    fields = stat[close + 2 :].split()
    # fields[0] is proc(5) field 3; starttime is field 22.
    if len(fields) <= 19:
        return None
    try:
        value = int(fields[19])
    except ValueError:
        return None
    return value if value >= 0 else None


def process_command(proc_root: Path, pid: int, fallback: str) -> str:
    try:
        raw = (proc_root / str(pid) / "cmdline").read_bytes()[:65_536]
    except (FileNotFoundError, ProcessLookupError, PermissionError, OSError):
        return fallback
    command = raw.replace(b"\0", b" ").decode("utf-8", errors="replace").strip()
    return command or fallback


def sample_schedstat(proc_root: Path, pid: int) -> dict[str, int] | None:
    try:
        fields = read_text(proc_root / str(pid) / "schedstat", 256).split()
        if len(fields) < 3:
            return None
        values = [int(value) for value in fields[:3]]
        if any(value < 0 for value in values):
            return None
        return {
            "runtime_ns": values[0],
            "runqueue_wait_ns": values[1],
            "timeslices": values[2],
        }
    except (FileNotFoundError, ProcessLookupError, PermissionError, OSError, ValueError):
        return None


def sample_pss(proc_root: Path, pid: int) -> tuple[int | None, int | None]:
    try:
        pss = None
        private = 0
        private_seen = False
        for line in read_text(proc_root / str(pid) / "smaps_rollup", 65_536).splitlines():
            key, separator, raw = line.partition(":")
            if not separator:
                continue
            value = kib_value(raw.strip())
            if key == "Pss":
                pss = value
            elif key in {"Private_Clean", "Private_Dirty", "Private_Hugetlb"} and value is not None:
                private += value
                private_seen = True
        return pss, private if private_seen else None
    except (FileNotFoundError, ProcessLookupError, PermissionError, OSError):
        return None, None


def cgroup_process_ids(directory: Path | None, cgroup_root: Path) -> tuple[set[int], bool]:
    """Return TGIDs from a cgroup subtree without walking the host's /proc."""

    if directory is None or directory == cgroup_root:
        return set(), False
    found: set[int] = set()
    pending = [directory]
    read_any = False
    while pending:
        current = pending.pop()
        try:
            raw = read_text(current / "cgroup.procs", 16_777_216)
            read_any = True
            for value in raw.split():
                if value.isascii() and value.isdigit() and int(value) > 0:
                    found.add(int(value))
        except (FileNotFoundError, PermissionError, OSError):
            pass
        try:
            pending.extend(
                entry
                for entry in current.iterdir()
                if entry.is_dir() and not entry.is_symlink()
            )
        except (FileNotFoundError, PermissionError, OSError):
            continue
    return found, read_any


def scan_processes(
    proc_root: Path, candidate_pids: set[int] | None = None
) -> dict[int, dict[str, Any]]:
    processes: dict[int, dict[str, Any]] = {}
    try:
        entries: Iterable[Path] = (
            list(proc_root.iterdir())
            if candidate_pids is None
            else [proc_root / str(pid) for pid in sorted(candidate_pids)]
        )
    except (FileNotFoundError, PermissionError, OSError):
        return processes

    for entry in entries:
        if not entry.name.isascii() or not entry.name.isdigit():
            continue
        pid = int(entry.name)
        try:
            status = parse_status(read_text(entry / "status", 65_536))
        except (FileNotFoundError, ProcessLookupError, PermissionError, OSError):
            continue
        ppid = integer_value(status.get("PPid"))
        if ppid is None:
            continue
        name = status.get("Name", "unknown")[:256]
        process: dict[str, Any] = {
            "pid": pid,
            "ppid": ppid,
            "name": name,
            "state": status.get("State", "?").split(maxsplit=1)[0],
            "threads": integer_value(status.get("Threads")),
            "start_ticks": None,
            "cgroup_path": unified_cgroup(proc_root, pid),
            "rss_bytes": kib_value(status.get("VmRSS")),
            "rss_anon_bytes": kib_value(status.get("RssAnon")),
            "rss_file_bytes": kib_value(status.get("RssFile")),
            "rss_shmem_bytes": kib_value(status.get("RssShmem")),
            "schedstat": None,
            "_command": name,
            "_enriched": False,
        }
        processes[pid] = process
    return processes


def enrich_process(
    process: dict[str, Any], proc_root: Path, include_pss: bool
) -> None:
    """Read expensive per-process files only after target membership is known."""

    if process.get("_enriched"):
        return
    pid = int(process["pid"])
    process["_command"] = process_command(proc_root, pid, str(process["name"]))
    process["start_ticks"] = process_start_ticks(proc_root, pid)
    process["schedstat"] = sample_schedstat(proc_root, pid)
    if include_pss:
        pss, private = sample_pss(proc_root, pid)
        process["pss_bytes"] = pss
        process["private_bytes"] = private
    process["_enriched"] = True


def descendants(processes: dict[int, dict[str, Any]], root: int) -> set[int]:
    children: dict[int, list[int]] = {}
    for pid, process in processes.items():
        children.setdefault(process["ppid"], []).append(pid)
    found: set[int] = set()
    pending = [root]
    while pending:
        pid = pending.pop()
        if pid in found:
            continue
        found.add(pid)
        pending.extend(children.get(pid, ()))
    return found


def in_cgroup(process_path: str | None, target_path: str | None) -> bool:
    if process_path is None or target_path in (None, "/"):
        return False
    base = target_path.rstrip("/")
    return process_path == base or process_path.startswith(base + "/")


def classify_process(command: str, kind: str) -> str:
    normalized = command.lower()
    # Docker's tiny init retains the complete child command in its own argv.
    # Matching those arguments would falsely count docker-init as part of the
    # supervisor control plane.
    if re.match(r"^(?:/sbin/)?docker-init(?:\s|$)", normalized):
        return kind
    if any(marker in normalized for marker in WORKER_MARKERS):
        return "worker"
    if any(marker in normalized for marker in LEASE_RENEWER_MARKERS):
        return "lease-renewer"
    if any(marker in normalized for marker in ORCHESTRATOR_MARKERS):
        return "orchestrator"
    # `artisan horizon` is the Horizon master, but horizon:work was handled
    # first to avoid treating workers as orchestrators.
    if re.search(r"(?:^|\s)(?:php\s+)?(?:\S*/)?artisan\s+horizon(?:\s|$)", normalized):
        return "orchestrator"
    return kind


def public_process(process: dict[str, Any], role: str, include_command: bool) -> dict[str, Any]:
    result = {key: value for key, value in process.items() if not key.startswith("_")}
    result.pop("cgroup_path", None)
    result["role"] = role
    if include_command:
        result["command"] = process["_command"][:4096]
    return result


def build_sample(
    sequence: int,
    started_ns: int,
    targets: list[dict[str, Any]],
    proc_root: Path,
    cgroup_root: Path,
    include_pss: bool,
    include_command: bool,
) -> dict[str, Any]:
    sample_started_ns = time.monotonic_ns()
    candidate_pids: set[int] = {int(target["host_pid"]) for target in targets}
    needs_proc_scan = False
    for target in targets:
        pid = int(target["host_pid"])
        live_cgroup = unified_cgroup(proc_root, pid)
        if live_cgroup is not None:
            target["cgroup_path"] = live_cgroup
        directory = cgroup_directory(cgroup_root, target.get("cgroup_path"))
        members, reliable = cgroup_process_ids(directory, cgroup_root)
        candidate_pids.update(members)
        if not reliable:
            needs_proc_scan = True
    processes = scan_processes(proc_root, None if needs_proc_scan else candidate_pids)
    target_samples: list[dict[str, Any]] = []

    for target in targets:
        pid = target["host_pid"]
        cgroup_path = target.get("cgroup_path")
        member_pids = descendants(processes, pid)
        if cgroup_path not in (None, "/"):
            member_pids.update(
                process_pid
                for process_pid, process in processes.items()
                if in_cgroup(process.get("cgroup_path"), cgroup_path)
            )

        rows: list[dict[str, Any]] = []
        role_counts: dict[str, int] = {}
        for process_pid in sorted(member_pids):
            process = processes.get(process_pid)
            if process is None:
                continue
            enrich_process(process, proc_root, include_pss)
            role = classify_process(process["_command"], target["kind"])
            role_counts[role] = role_counts.get(role, 0) + 1
            rows.append(public_process(process, role, include_command))

        cgroup, errors = sample_cgroup(cgroup_directory(cgroup_root, cgroup_path))
        target_samples.append(
            {
                "label": target["label"],
                "kind": target["kind"],
                "host_pid": pid,
                "alive": pid in processes,
                "cgroup_path": cgroup_path,
                "cgroup": cgroup,
                "process_count": len(rows),
                "role_counts": role_counts,
                "processes": rows,
                "errors": errors,
            }
        )

    sampled_ns = time.monotonic_ns()
    return {
        "schema": SCHEMA,
        "type": "sample",
        "sequence": sequence,
        "wall_time": utc_now(),
        "monotonic_ns": sampled_ns,
        "elapsed_ns": sampled_ns - started_ns,
        "sampling_duration_ns": sampled_ns - sample_started_ns,
        "targets": target_samples,
    }


def write_ready_file(path: str | None) -> None:
    if path is None:
        return
    ready = Path(path)
    ready.parent.mkdir(parents=True, exist_ok=True)
    temporary = ready.with_name(f".{ready.name}.{os.getpid()}.tmp")
    temporary.write_text(f"{os.getpid()}\n", encoding="ascii")
    os.replace(temporary, ready)


def emit(stream: Any, record: dict[str, Any], fsync: bool = False) -> None:
    stream.write(json.dumps(record, separators=(",", ":"), ensure_ascii=True) + "\n")
    stream.flush()
    if fsync and stream is not sys.stdout:
        os.fsync(stream.fileno())


def argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--target",
        action="append",
        required=True,
        type=parse_target,
        metavar="LABEL=HOST_PID",
        help="repeat for each container or process tree to sample",
    )
    parser.add_argument("--output", required=True, help="JSONL path, or - for stdout")
    parser.add_argument("--interval", type=positive_float, default=0.2, help="seconds")
    parser.add_argument("--duration", type=nonnegative_float, default=0.0, help="0 runs until signalled")
    parser.add_argument("--max-samples", type=positive_int, default=None)
    parser.add_argument("--pss", action="store_true", help="read the more expensive smaps_rollup PSS")
    parser.add_argument(
        "--include-command",
        action="store_true",
        help="include process command lines (disabled by default to avoid leaking arguments)",
    )
    parser.add_argument("--exit-when-targets-exit", action="store_true")
    parser.add_argument("--ready-file", help="atomically create this file after the metadata line is flushed")
    parser.add_argument("--fsync-every", type=positive_int, default=None)
    parser.add_argument("--force", action="store_true", help="replace an existing output file")
    parser.add_argument("--proc-root", default="/proc", help=argparse.SUPPRESS)
    parser.add_argument("--cgroup-root", default="/sys/fs/cgroup", help=argparse.SUPPRESS)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = argument_parser().parse_args(argv)
    labels = [label for label, _pid in args.target]
    if len(labels) != len(set(labels)):
        raise SystemExit("target labels must be unique")

    proc_root = Path(args.proc_root).resolve()
    cgroup_root = Path(args.cgroup_root).resolve()
    if not (cgroup_root / "cgroup.controllers").is_file():
        raise SystemExit(f"{cgroup_root} is not a mounted cgroup v2 hierarchy")

    targets = [
        {
            "label": label,
            "kind": target_kind(label),
            "host_pid": pid,
            "cgroup_path": unified_cgroup(proc_root, pid),
        }
        for label, pid in args.target
    ]

    if args.output == "-":
        stream = sys.stdout
        close_stream = False
    else:
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        mode = "w" if args.force else "x"
        try:
            stream = output.open(mode, encoding="utf-8", buffering=1)
        except FileExistsError:
            raise SystemExit(f"output already exists: {output}; pass --force to replace it") from None
        close_stream = True

    for watched_signal in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        signal.signal(watched_signal, request_stop)

    started_ns = time.monotonic_ns()
    metadata = {
        "schema": SCHEMA,
        "type": "metadata",
        "wall_time": utc_now(),
        "monotonic_ns": started_ns,
        "sampler_pid": os.getpid(),
        "interval_ns": round(args.interval * 1_000_000_000),
        "pss_enabled": args.pss,
        "clock_ticks_per_second": os.sysconf("SC_CLK_TCK"),
        "page_size_bytes": os.sysconf("SC_PAGE_SIZE"),
        "targets": targets,
    }

    sequence = 0
    reason = "signal"
    deadline_ns = started_ns + round(args.duration * 1_000_000_000) if args.duration else None
    interval_ns = round(args.interval * 1_000_000_000)
    next_sample_ns = started_ns
    try:
        emit(stream, metadata, fsync=True)
        write_ready_file(args.ready_file)
        while not _STOP:
            now_ns = time.monotonic_ns()
            if deadline_ns is not None and now_ns >= deadline_ns:
                reason = "duration"
                break
            if now_ns < next_sample_ns:
                time.sleep(min((next_sample_ns - now_ns) / 1_000_000_000, 0.1))
                continue

            sample = build_sample(
                sequence,
                started_ns,
                targets,
                proc_root,
                cgroup_root,
                args.pss,
                args.include_command,
            )
            do_fsync = args.fsync_every is not None and (sequence + 1) % args.fsync_every == 0
            emit(stream, sample, fsync=do_fsync)
            sequence += 1

            if args.max_samples is not None and sequence >= args.max_samples:
                reason = "max_samples"
                break
            if args.exit_when_targets_exit and not any(
                target["alive"] for target in sample["targets"]
            ):
                reason = "targets_exited"
                break

            next_sample_ns += interval_ns
            # If scanning took longer than an interval, skip missed deadlines
            # instead of emitting a burst of misleading back-to-back samples.
            now_ns = time.monotonic_ns()
            if next_sample_ns < now_ns:
                missed = (now_ns - next_sample_ns) // interval_ns + 1
                next_sample_ns += missed * interval_ns

        ended_ns = time.monotonic_ns()
        emit(
            stream,
            {
                "schema": SCHEMA,
                "type": "end",
                "wall_time": utc_now(),
                "monotonic_ns": ended_ns,
                "elapsed_ns": ended_ns - started_ns,
                "samples": sequence,
                "reason": reason,
            },
            fsync=True,
        )
    except BrokenPipeError:
        return 0
    finally:
        if close_stream:
            stream.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

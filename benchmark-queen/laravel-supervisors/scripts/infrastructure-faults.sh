#!/usr/bin/env bash

set -Eeuo pipefail
IFS=$'\n\t'

SCRIPT_DIR="$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
BENCH_DIR="$(CDPATH='' cd -- "${SCRIPT_DIR}/.." && pwd)"
REPOSITORY_ROOT="$(CDPATH='' cd -- "${BENCH_DIR}/../.." && pwd)"
COMPOSE_FILE="${BENCH_DIR}/compose.yml"
APP_IMAGE="queen-laravel-supervisor-bench:local"
BROKER_IMAGE="queen-laravel-supervisor-broker:local"
REDIS_IMAGE="redis:7.4.2-alpine"
POSTGRES_IMAGE="postgres:16.10-bookworm"

ENGINES_CSV="horizon,queen-php,queen-rust"
SCENARIOS_CSV="redis-restart,broker-restart,postgres-restart,app-backend-network-partition,broker-postgres-network-partition,master-sigkill"
JOBS=32
WORKERS=2
SLEEP_MS=1000
CPU_ITERATIONS=0
JOB_TRIES=3
FAULT_HOLD_SECONDS=3
WORKER_TIMEOUT=10
RETRY_AFTER=12
READY_TIMEOUT=90
COMPLETION_TIMEOUT=180
BUILD_IMAGES=1
DRY_RUN=0
OUTPUT_DIRECTORY=""
DOCKER_OPERATION_TIMEOUT=60
STARTUP_TIMEOUT=240
BUILD_TIMEOUT=1200
DISPATCH_TIMEOUT=180

ACTIVE_PROJECT=""
ACTIVE_ENGINE=""
ACTIVE_VOLUME=""
ACTIVE_LANE_DIRECTORY=""
ACTIVE_RUN_ID=""
ACTIVE_SCENARIO=""
ACTIVE_OVERRIDE_FILE=""
ACTIVE_APP_NETWORK=""
ACTIVE_DB_NETWORK=""
ACTIVE_BACKEND_VOLUMES=""

usage() {
    cat <<'EOF'
Usage: scripts/infrastructure-faults.sh --output DIRECTORY [options]

Run isolated backend, network and supervisor-master fault scenarios against
fresh durable named volumes, then retain recovery and correctness evidence.

Options:
  --output DIRECTORY          Required, new or empty artifact directory
  --engines CSV               Subset of horizon,queen-php,queen-rust
  --scenarios CSV             Subset of redis-restart,broker-restart,
                              postgres-restart,app-backend-network-partition,
                              broker-postgres-network-partition,master-sigkill
  --jobs N                    Jobs per lane (default: 32)
  --workers N                 Fixed workers per lane (default: 2)
  --sleep-ms N                Runtime of every job (default: 1000)
  --cpu-iterations N          SHA-256 rounds per job (default: 0)
  --job-tries N               Maximum delivery attempts (default: 3)
  --fault-hold-seconds N      Fault interval before recovery (default: 3)
  --worker-timeout SECONDS    Laravel worker timeout (default: 10)
  --retry-after SECONDS       Queue visibility timeout (default: 12)
  --ready-timeout SECONDS     Bootstrap/recovery timeout (default: 90)
  --completion-timeout SEC    Completion and quiescence timeout (default: 180)
  --operation-timeout SEC     Outer deadline for ordinary Docker calls (default: 60)
  --startup-timeout SEC       Outer deadline for Compose startup (default: 240)
  --build-timeout SEC         Outer deadline for each image build (default: 1200)
  --dispatch-timeout SEC      Outer dispatch deadline (default: 180)
  --no-build                  Reuse local benchmark images
  --dry-run                   Validate and write the complete plan; never call Docker
  -h, --help                  Show this help

Applicability is explicit: redis-restart is Horizon-only; broker-restart,
postgres-restart and broker-postgres-network-partition are Queen-only;
app-backend-network-partition and master-sigkill apply to all engines.
Disk-full is intentionally not implemented by this harness.
EOF
}

die() {
    printf 'error: %s\n' "$*" >&2
    exit 1
}

require_command() {
    command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

require_uint() {
    case "$2" in
        ''|*[!0-9]*) die "$1 must be a non-negative integer" ;;
    esac
}

require_positive_int() {
    require_uint "$1" "$2"
    [ "$2" -gt 0 ] || die "$1 must be greater than zero"
}

parse_csv() {
    csv_value="$1"
    csv_label="$2"
    OLD_IFS="$IFS"
    IFS=',' read -r -a CSV_ITEMS <<EOF
${csv_value}
EOF
    IFS="$OLD_IFS"
    [ "${#CSV_ITEMS[@]}" -gt 0 ] || die "$csv_label must contain at least one value"
    csv_seen=','
    for csv_item in "${CSV_ITEMS[@]}"; do
        [ -n "$csv_item" ] || die "$csv_label must not contain empty values"
        case "$csv_item" in
            *[!a-z0-9-]*) die "$csv_label contains an invalid value: $csv_item" ;;
        esac
        case "$csv_seen" in
            *",${csv_item},"*) die "$csv_label contains a duplicate value: $csv_item" ;;
        esac
        csv_seen="${csv_seen}${csv_item},"
    done
}

is_applicable() {
    applicable_engine="$1"
    applicable_scenario="$2"
    case "${applicable_scenario}:${applicable_engine}" in
        redis-restart:horizon) return 0 ;;
        app-backend-network-partition:horizon|app-backend-network-partition:queen-php|app-backend-network-partition:queen-rust) return 0 ;;
        broker-restart:queen-php|broker-restart:queen-rust) return 0 ;;
        postgres-restart:queen-php|postgres-restart:queen-rust) return 0 ;;
        broker-postgres-network-partition:queen-php|broker-postgres-network-partition:queen-rust) return 0 ;;
        master-sigkill:horizon|master-sigkill:queen-php|master-sigkill:queen-rust) return 0 ;;
    esac
    return 1
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --output) OUTPUT_DIRECTORY="${2:?--output requires a value}"; shift 2 ;;
        --engines) ENGINES_CSV="${2:?--engines requires a value}"; shift 2 ;;
        --scenarios) SCENARIOS_CSV="${2:?--scenarios requires a value}"; shift 2 ;;
        --jobs) JOBS="${2:?--jobs requires a value}"; shift 2 ;;
        --workers) WORKERS="${2:?--workers requires a value}"; shift 2 ;;
        --sleep-ms) SLEEP_MS="${2:?--sleep-ms requires a value}"; shift 2 ;;
        --cpu-iterations) CPU_ITERATIONS="${2:?--cpu-iterations requires a value}"; shift 2 ;;
        --job-tries) JOB_TRIES="${2:?--job-tries requires a value}"; shift 2 ;;
        --fault-hold-seconds) FAULT_HOLD_SECONDS="${2:?--fault-hold-seconds requires a value}"; shift 2 ;;
        --worker-timeout) WORKER_TIMEOUT="${2:?--worker-timeout requires a value}"; shift 2 ;;
        --retry-after) RETRY_AFTER="${2:?--retry-after requires a value}"; shift 2 ;;
        --ready-timeout) READY_TIMEOUT="${2:?--ready-timeout requires a value}"; shift 2 ;;
        --completion-timeout) COMPLETION_TIMEOUT="${2:?--completion-timeout requires a value}"; shift 2 ;;
        --operation-timeout) DOCKER_OPERATION_TIMEOUT="${2:?--operation-timeout requires a value}"; shift 2 ;;
        --startup-timeout) STARTUP_TIMEOUT="${2:?--startup-timeout requires a value}"; shift 2 ;;
        --build-timeout) BUILD_TIMEOUT="${2:?--build-timeout requires a value}"; shift 2 ;;
        --dispatch-timeout) DISPATCH_TIMEOUT="${2:?--dispatch-timeout requires a value}"; shift 2 ;;
        --no-build) BUILD_IMAGES=0; shift ;;
        --dry-run) DRY_RUN=1; shift ;;
        -h|--help) usage; exit 0 ;;
        *) die "unknown option: $1" ;;
    esac
done

[ -n "$OUTPUT_DIRECTORY" ] || die "--output is required"
require_command git
require_command python3
require_positive_int "--jobs" "$JOBS"
require_positive_int "--workers" "$WORKERS"
require_positive_int "--sleep-ms" "$SLEEP_MS"
require_uint "--cpu-iterations" "$CPU_ITERATIONS"
require_positive_int "--job-tries" "$JOB_TRIES"
require_positive_int "--fault-hold-seconds" "$FAULT_HOLD_SECONDS"
require_positive_int "--worker-timeout" "$WORKER_TIMEOUT"
require_positive_int "--retry-after" "$RETRY_AFTER"
require_positive_int "--ready-timeout" "$READY_TIMEOUT"
require_positive_int "--completion-timeout" "$COMPLETION_TIMEOUT"
require_positive_int "--operation-timeout" "$DOCKER_OPERATION_TIMEOUT"
require_positive_int "--startup-timeout" "$STARTUP_TIMEOUT"
require_positive_int "--build-timeout" "$BUILD_TIMEOUT"
require_positive_int "--dispatch-timeout" "$DISPATCH_TIMEOUT"
[ "$JOB_TRIES" -ge 2 ] || die "--job-tries must be at least 2 for redelivery"
[ "$RETRY_AFTER" -gt "$WORKER_TIMEOUT" ] || die "--retry-after must exceed --worker-timeout"
[ "$JOBS" -ge $(( WORKERS * 8 )) ] || die "--jobs must be at least eight times --workers to preserve a measured backlog"

parse_csv "$ENGINES_CSV" "--engines"
ENGINES=("${CSV_ITEMS[@]}")
for engine in "${ENGINES[@]}"; do
    case "$engine" in
        horizon|queen-php|queen-rust) ;;
        *) die "unknown engine in --engines: $engine" ;;
    esac
done

parse_csv "$SCENARIOS_CSV" "--scenarios"
SCENARIOS=("${CSV_ITEMS[@]}")
for scenario in "${SCENARIOS[@]}"; do
    case "$scenario" in
        redis-restart|broker-restart|postgres-restart|app-backend-network-partition|broker-postgres-network-partition|master-sigkill) ;;
        disk-full) die "disk-full is not implemented; run it only as a separate disposable-storage qualification gate" ;;
        *) die "unknown scenario in --scenarios: $scenario" ;;
    esac
done

lane_count=0
for scenario in "${SCENARIOS[@]}"; do
    scenario_lanes=0
    for engine in "${ENGINES[@]}"; do
        if is_applicable "$engine" "$scenario"; then
            lane_count=$((lane_count + 1))
            scenario_lanes=$((scenario_lanes + 1))
        fi
    done
    [ "$scenario_lanes" -gt 0 ] || die "scenario $scenario does not apply to any selected engine"
done
[ "$lane_count" -gt 0 ] || die "the selected engine/scenario matrix has no applicable lanes"

mkdir -p "$OUTPUT_DIRECTORY"
OUTPUT_DIRECTORY="$(CDPATH='' cd -- "$OUTPUT_DIRECTORY" && pwd)"
if [ -n "$(find "$OUTPUT_DIRECTORY" -mindepth 1 -maxdepth 1 -print -quit)" ]; then
    die "--output must be empty: $OUTPUT_DIRECTORY"
fi

campaign_stamp="$(date -u +%Y%m%dT%H%M%SZ)"
git_short="$(git -C "$REPOSITORY_ROOT" rev-parse --short=10 HEAD)"
campaign_nonce="$(python3 -c 'import secrets; print(secrets.token_hex(12))')"
campaign_token="$campaign_nonce"
campaign_id="infra-${campaign_stamp}-${git_short}-${campaign_nonce}"

python3 - "$OUTPUT_DIRECTORY" "$REPOSITORY_ROOT" "$campaign_id" "$ENGINES_CSV" "$SCENARIOS_CSV" \
    "$JOBS" "$WORKERS" "$SLEEP_MS" "$CPU_ITERATIONS" "$JOB_TRIES" "$FAULT_HOLD_SECONDS" \
    "$WORKER_TIMEOUT" "$RETRY_AFTER" "$READY_TIMEOUT" "$COMPLETION_TIMEOUT" "$BUILD_IMAGES" "$DRY_RUN" \
    "$campaign_nonce" "$DOCKER_OPERATION_TIMEOUT" "$STARTUP_TIMEOUT" "$BUILD_TIMEOUT" "$DISPATCH_TIMEOUT" <<'PY'
import datetime as dt
import json
import platform
import subprocess
import sys
from pathlib import Path

(
    output, repository, campaign_id, engines_raw, scenarios_raw, jobs, workers,
    sleep_ms, cpu_iterations, job_tries, fault_hold, worker_timeout, retry_after,
    ready_timeout, completion_timeout, build_images, dry_run, campaign_nonce,
    operation_timeout, startup_timeout, build_timeout, dispatch_timeout,
) = sys.argv[1:]
engines = engines_raw.split(",")
scenarios = scenarios_raw.split(",")

def applies(engine: str, scenario: str) -> bool:
    if scenario == "redis-restart":
        return engine == "horizon"
    if scenario in {"app-backend-network-partition", "master-sigkill"}:
        return True
    return engine in {"queen-php", "queen-rust"}

def command(*arguments: str) -> str:
    return subprocess.check_output(arguments, text=True, stderr=subprocess.DEVNULL).strip()

lanes = [
    {
        "engine": engine,
        "scenario": scenario,
        "backend": "redis" if engine == "horizon" else "broker",
        "connection": "redis" if engine == "horizon" else "queen",
    }
    for scenario in scenarios
    for engine in engines
    if applies(engine, scenario)
]
excluded = [
    {"engine": engine, "scenario": scenario, "reason": "not_applicable"}
    for scenario in scenarios
    for engine in engines
    if not applies(engine, scenario)
]
metadata = {
    "schema": "queen.laravel-supervisors.infrastructure-fault-plan/v1",
    "qualification": "infrastructure_fault_diagnostic",
    "performance_comparable": False,
    "campaign_id": campaign_id,
    "campaign_nonce": campaign_nonce,
    "created_at": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
    "git": {
        "commit": command("git", "-C", repository, "rev-parse", "HEAD"),
        "branch": command("git", "-C", repository, "branch", "--show-current"),
        "dirty": bool(command("git", "-C", repository, "status", "--porcelain")),
    },
    "host": {"platform": platform.platform(), "machine": platform.machine()},
    "settings": {
        "engines": engines,
        "scenarios": scenarios,
        "jobs": int(jobs),
        "workers": int(workers),
        "sleep_ms": int(sleep_ms),
        "cpu_iterations": int(cpu_iterations),
        "job_tries": int(job_tries),
        "fault_hold_seconds": int(fault_hold),
        "worker_timeout_seconds": int(worker_timeout),
        "retry_after_seconds": int(retry_after),
        "ready_timeout_seconds": int(ready_timeout),
        "completion_timeout_seconds": int(completion_timeout),
        "docker_operation_timeout_seconds": int(operation_timeout),
        "startup_timeout_seconds": int(startup_timeout),
        "build_timeout_seconds": int(build_timeout),
        "dispatch_timeout_seconds": int(dispatch_timeout),
        "profile": "fixed",
        "ledger_mode": "durable",
        "queen_prefetch": 1,
        "queen_ack_batch": 1,
        "redis_appendonly": "yes",
        "redis_appendfsync": "always",
        "build_images": build_images == "1",
        "dry_run": dry_run == "1",
    },
    "method": {
        "isolation": "one fresh Compose project and fresh durable named volumes per lane",
        "topology": "dedicated app-backend network; Queen additionally uses a separate broker-postgres network",
        "fault_timing": "inject only after ready backlog and an open durable-ledger attempt are both observed",
        "recovery": "restore only the targeted dependency; master-sigkill restarts only its application lane",
        "completion": "exact deterministic job-id set plus a continuously empty final queue window",
        "evidence": [
            "durable SQLite effect ledger", "timeline with UTC and monotonic time",
            "container inspections", "process trees", "network membership",
            "service logs", "fresh-volume inspections", "SHA-256 artifact manifests",
        ],
        "reported_gates": ["at_least_once", "idempotent_effect", "strict_execution"],
        "required_campaign_gate": "at_least_once and idempotent_effect; strict_execution is reported separately",
    },
    "lanes": lanes,
    "excluded_pairs": excluded,
    "separate_gates": {
        "disk_full": {
            "implemented": False,
            "reason": "Safe injection requires a disposable bounded filesystem and an independently writable evidence sink.",
        }
    },
    "known_limits": [
        "The fixture ledger is durable and idempotent but is not atomic with queue acknowledgement or arbitrary external effects.",
        "A Docker stop/start exercises process recovery on one host; it is not a multi-node failover or data-loss test.",
        "Network partitions are container-network cuts, not latency, packet loss, asymmetric routing or DNS-failure models.",
        "The harness reports strict single-execution separately because at-least-once delivery permits retries.",
        "With --no-build, immutable image IDs and checkout hashes are evidence but do not prove that the images came from that checkout.",
    ],
}
root = Path(output)
(root / "metadata.json").write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8")
(root / "plan.json").write_text(json.dumps({"schema": metadata["schema"], "lanes": lanes, "excluded_pairs": excluded}, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY

write_manifest() {
    manifest_root="$1"
    python3 "${SCRIPT_DIR}/artifact-manifest.py" \
        --root "$manifest_root" --output "${manifest_root}/artifact-manifest.json"
}

if [ "$DRY_RUN" -eq 1 ]; then
    write_manifest "$OUTPUT_DIRECTORY"
    printf 'Dry-run infrastructure fault plan: %s/plan.json\n' "$OUTPUT_DIRECTORY"
    exit 0
fi

run_bounded() {
    bounded_seconds="$1"
    shift
    python3 - "$bounded_seconds" "$@" <<'PY'
import os
import signal
import subprocess
import sys

seconds = int(sys.argv[1])
arguments = sys.argv[2:]


class ForwardedSignal(Exception):
    def __init__(self, signum: int):
        self.signum = signum


def forward_signal(signum: int, _frame: object) -> None:
    raise ForwardedSignal(signum)


def terminate_group(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=5)
        return
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        pass


for handled_signal in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
    signal.signal(handled_signal, forward_signal)

try:
    process = subprocess.Popen(arguments, start_new_session=True)
except OSError as exception:
    print(f"unable to start bounded command {arguments[0]}: {exception}", file=sys.stderr)
    raise SystemExit(127)
try:
    returncode = process.wait(timeout=seconds)
except subprocess.TimeoutExpired:
    print(f"bounded command timed out after {seconds}s: {' '.join(arguments)}", file=sys.stderr)
    terminate_group(process)
    raise SystemExit(124)
except ForwardedSignal as interrupted:
    terminate_group(process)
    raise SystemExit(128 + interrupted.signum)
finally:
    terminate_group(process)
raise SystemExit(returncode if returncode >= 0 else 128 - returncode)
PY
}

monotonic_seconds() {
    python3 -c 'import time; print(time.monotonic_ns() // 1_000_000_000)'
}

docker_bounded() {
    run_bounded "$DOCKER_OPERATION_TIMEOUT" docker "$@"
}

require_command docker
docker_bounded info >/dev/null 2>&1 || die "Docker daemon is unavailable or did not answer within the operation deadline"

contains_queen=0
contains_horizon=0
for engine in "${ENGINES[@]}"; do
    if [ "$engine" = horizon ]; then
        contains_horizon=1
    else
        contains_queen=1
    fi
done

image_id() {
    docker_bounded image inspect "$1" --format '{{.Id}}' 2>/dev/null || true
}

if [ "$BUILD_IMAGES" -eq 1 ]; then
    run_bounded "$BUILD_TIMEOUT" docker compose --file "$COMPOSE_FILE" --profile tools build producer
    if [ "$contains_queen" -eq 1 ]; then
        run_bounded "$BUILD_TIMEOUT" docker compose --file "$COMPOSE_FILE" --profile queen-php build broker
    fi
    if [ "$contains_horizon" -eq 1 ]; then
        run_bounded "$BUILD_TIMEOUT" docker pull "$REDIS_IMAGE"
    fi
    if [ "$contains_queen" -eq 1 ]; then
        run_bounded "$BUILD_TIMEOUT" docker pull "$POSTGRES_IMAGE"
    fi
else
    [ -n "$(image_id "$APP_IMAGE")" ] || die "missing image: $APP_IMAGE"
    if [ "$contains_horizon" -eq 1 ]; then
        [ -n "$(image_id "$REDIS_IMAGE")" ] || die "missing image: $REDIS_IMAGE"
    fi
    if [ "$contains_queen" -eq 1 ]; then
        [ -n "$(image_id "$BROKER_IMAGE")" ] || die "missing image: $BROKER_IMAGE"
        [ -n "$(image_id "$POSTGRES_IMAGE")" ] || die "missing image: $POSTGRES_IMAGE"
    fi
fi

EXPECTED_APP_IMAGE_ID="$(image_id "$APP_IMAGE")"
EXPECTED_BROKER_IMAGE_ID=""
EXPECTED_REDIS_IMAGE_ID=""
EXPECTED_POSTGRES_IMAGE_ID=""
if [ "$contains_horizon" -eq 1 ]; then
    EXPECTED_REDIS_IMAGE_ID="$(image_id "$REDIS_IMAGE")"
fi
if [ "$contains_queen" -eq 1 ]; then
    EXPECTED_BROKER_IMAGE_ID="$(image_id "$BROKER_IMAGE")"
    EXPECTED_POSTGRES_IMAGE_ID="$(image_id "$POSTGRES_IMAGE")"
fi
[ -n "$EXPECTED_APP_IMAGE_ID" ] || die "unable to resolve immutable application image ID"
if [ "$contains_horizon" -eq 1 ]; then
    [ -n "$EXPECTED_REDIS_IMAGE_ID" ] || die "unable to resolve immutable Redis image ID"
fi
if [ "$contains_queen" -eq 1 ]; then
    [ -n "$EXPECTED_BROKER_IMAGE_ID" ] || die "unable to resolve immutable broker image ID"
    [ -n "$EXPECTED_POSTGRES_IMAGE_ID" ] || die "unable to resolve immutable PostgreSQL image ID"
fi

python3 - "$OUTPUT_DIRECTORY" "$APP_IMAGE" "$EXPECTED_APP_IMAGE_ID" "$BROKER_IMAGE" \
    "$EXPECTED_BROKER_IMAGE_ID" "$REDIS_IMAGE" "$EXPECTED_REDIS_IMAGE_ID" \
    "$POSTGRES_IMAGE" "$EXPECTED_POSTGRES_IMAGE_ID" "$BUILD_IMAGES" <<'PY'
import datetime as dt
import json
import sqlite3
import sys
from pathlib import Path

(
    output, app_name, app_id, broker_name, broker_id, redis_name, redis_id,
    postgres_name, postgres_id, built,
) = sys.argv[1:]
images = [{"name": app_name, "id": app_id}]
if broker_id:
    images.append({"name": broker_name, "id": broker_id})
if redis_id:
    images.append({"name": redis_name, "id": redis_id})
if postgres_id:
    images.append({"name": postgres_name, "id": postgres_id})
payload = {
    "schema": "queen.laravel-supervisors.image-provenance/v1",
    "captured_at": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
    "built_by_campaign": built == "1",
    "images": images,
}
Path(output, "images.json").write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY

python3 - "$REPOSITORY_ROOT" "$OUTPUT_DIRECTORY" <<'PY'
import hashlib
import json
import subprocess
import sys
from pathlib import Path

repository = Path(sys.argv[1]).resolve()
output = Path(sys.argv[2]).resolve()
pathspecs = [
    "benchmark-queen/laravel-supervisors",
    "clients/client-laravel",
    "server",
    "supervisor",
]
diff = subprocess.check_output(
    ["git", "-C", str(repository), "diff", "--binary", "HEAD", "--", *pathspecs]
)
(output / "source.diff").write_bytes(diff)
raw_paths = subprocess.check_output(
    [
        "git", "-C", str(repository), "ls-files", "-z", "--cached", "--others",
        "--exclude-standard", "--", *pathspecs,
    ]
)
records = []
for raw in raw_paths.split(b"\0"):
    if not raw:
        continue
    relative = raw.decode("utf-8")
    path = (repository / relative).resolve()
    try:
        path.relative_to(output)
    except ValueError:
        pass
    else:
        continue
    if not path.is_file():
        continue
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    records.append((relative, digest))
records.sort()
lines = [f"{digest}  {relative}" for relative, digest in records]
encoded = ("\n".join(lines) + ("\n" if lines else "")).encode("utf-8")
(output / "source-files.sha256").write_bytes(encoded)
payload = {
    "schema": "queen.laravel-supervisors.source-provenance/v1",
    "commit": subprocess.check_output(
        ["git", "-C", str(repository), "rev-parse", "HEAD"], text=True
    ).strip(),
    "head_tree": subprocess.check_output(
        ["git", "-C", str(repository), "rev-parse", "HEAD^{tree}"], text=True
    ).strip(),
    "diff_sha256": hashlib.sha256(diff).hexdigest(),
    "diff_bytes": len(diff),
    "source_file_count": len(records),
    "source_state_sha256": hashlib.sha256(encoded).hexdigest(),
    "scope": pathspecs,
}
(output / "source-provenance.json").write_text(
    json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
)
PY

docker_bounded info --format '{{json .}}' | python3 -m json.tool >"${OUTPUT_DIRECTORY}/docker-info.json"
docker_bounded ps --no-trunc --format '{{.ID}}\t{{.Names}}\t{{.Image}}\t{{.Status}}' \
    >"${OUTPUT_DIRECTORY}/preexisting-containers.tsv"

compose_active() {
    compose_active_bounded "$DOCKER_OPERATION_TIMEOUT" "$@"
}

compose_active_bounded() {
    compose_timeout="$1"
    shift
    local -a compose_files=(--file "$COMPOSE_FILE")
    if [ -n "$ACTIVE_OVERRIDE_FILE" ]; then
        compose_files+=(--file "$ACTIVE_OVERRIDE_FILE")
    fi
    run_bounded "$compose_timeout" docker compose "${compose_files[@]}" \
        --project-name "$ACTIVE_PROJECT" \
        --profile "$ACTIVE_ENGINE" --profile tools "$@"
}

append_timeline() {
    timeline_path="$1"
    timeline_event="$2"
    timeline_detail="$3"
    python3 - "$timeline_path" "$timeline_event" "$timeline_detail" <<'PY'
import datetime as dt
import json
import sys
import time
from pathlib import Path

path, event, detail = sys.argv[1:]
record = {
    "wall_time": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
    "host_monotonic_ns": time.monotonic_ns(),
    "event": event,
    "detail": detail,
}
with Path(path).open("a", encoding="utf-8") as stream:
    stream.write(json.dumps(record, sort_keys=True) + "\n")
PY
}

producer_bounded() {
    producer_timeout="$1"
    shift
    compose_active_bounded "$producer_timeout" run --rm --no-deps --no-TTY producer "$@"
}

wait_for_health() {
    health_container="$1"
    health_label="$2"
    health_deadline=$(( $(monotonic_seconds) + READY_TIMEOUT ))
    while :; do
        health_status="$(docker_bounded inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$health_container" 2>/dev/null || true)"
        case "$health_status" in
            healthy|running) return 0 ;;
            exited|dead|unhealthy) die "$health_label entered state: $health_status" ;;
        esac
        [ "$(monotonic_seconds)" -lt "$health_deadline" ] || die "timed out waiting for $health_label health"
        sleep 1
    done
}

worker_rows() {
    rows_container="$1"
    rows_needle="$2"
    docker_bounded exec "$rows_container" ps -eo pid=,ppid=,args= \
        | awk -v needle="$rows_needle" '
            index($0, needle) > 0 && $1 ~ /^[0-9]+$/ && $2 ~ /^[0-9]+$/ && $1 != 1 && $2 > 1 {
                pid = $1
                ppid = $2
                $1 = ""
                $2 = ""
                sub(/^[[:space:]]+/, "")
                printf "%s\t%s\t%s\n", pid, ppid, $0
            }
        '
}

wait_for_supervisor_capacity() {
    capacity_container="$1"
    capacity_stage="$2"
    capacity_workers_output="${ACTIVE_LANE_DIRECTORY}/workers-${capacity_stage}.tsv"
    capacity_status_output="${ACTIVE_LANE_DIRECTORY}/supervisor-readiness-${capacity_stage}.txt"
    capacity_deadline=$(( $(monotonic_seconds) + READY_TIMEOUT ))
    if [ "$ACTIVE_ENGINE" = horizon ]; then
        capacity_needle='artisan horizon:work '
        capacity_status_command='horizon:status'
    else
        capacity_needle='artisan queue:work '
        capacity_status_command='queen:supervisor'
    fi
    while :; do
        set +e
        if [ "$capacity_status_command" = horizon:status ]; then
            docker_bounded exec "$capacity_container" php artisan horizon:status --no-ansi \
                >"${capacity_status_output}.tmp" 2>&1
        else
            docker_bounded exec "$capacity_container" php artisan queen:supervisor status --check --no-ansi \
                >"${capacity_status_output}.tmp" 2>&1
        fi
        capacity_status=$?
        set -e
        worker_rows "$capacity_container" "$capacity_needle" >"${capacity_workers_output}.tmp" 2>/dev/null || true
        capacity_count="$(wc -l <"${capacity_workers_output}.tmp" | tr -d '[:space:]')"
        if [ "$capacity_status" -eq 0 ] && [ "$capacity_count" -eq "$WORKERS" ]; then
            mv "${capacity_status_output}.tmp" "$capacity_status_output"
            mv "${capacity_workers_output}.tmp" "$capacity_workers_output"
            return 0
        fi
        [ "$(monotonic_seconds)" -lt "$capacity_deadline" ] || die "$ACTIVE_ENGINE did not recover functional readiness and $WORKERS-worker capacity"
        sleep 0.2
    done
}

capture_backend_probe() {
    probe_output="$1"
    probe_requires_empty="$2"
    probe_container="$3"
    set +e
    docker_bounded exec "$probe_container" php artisan bench:queue-state --no-ansi \
        --run-id="$ACTIVE_RUN_ID" --connection="$BENCH_CONNECTION" --queue=benchmark \
        --wait=0 --poll-ms=100 --settle-ms=0 >"$probe_output" 2>"${probe_output}.stderr.log"
    probe_status=$?
    set -e
    python3 - "$probe_output" "$probe_requires_empty" "$probe_status" <<'PY'
import json
import sys
from pathlib import Path

path, requires_empty, status = sys.argv[1:]
try:
    value = json.loads(Path(path).read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError) as exception:
    raise SystemExit(f"functional backend probe did not return JSON: {exception}")
state = value.get("state") if isinstance(value.get("state"), dict) else {}
functional = (
    isinstance(value.get("implementation"), str)
    and isinstance(state.get("size"), int)
    and not isinstance(state.get("size"), bool)
    and value.get("probe_errors") == []
)
if not functional:
    raise SystemExit("functional backend probe returned an unsupported or failed observation")
if requires_empty == "true" and not (
    status == "0" and state.get("size") == 0 and value.get("quiescent") is True
):
    raise SystemExit("functional backend probe did not prove an empty queue")
PY
}

capture_process_tree() {
    process_container="$1"
    process_output="$2"
    if [ "$(docker_bounded inspect --format '{{.State.Running}}' "$process_container" 2>/dev/null || true)" = true ]; then
        docker_bounded exec "$process_container" ps -eo pid=,ppid=,etimes=,lstart=,stat=,comm=,args= --forest >"$process_output" 2>&1 || true
    else
        printf 'container is not running\n' >"$process_output"
    fi
}

capture_containers() {
    container_output="$1"
    container_ids="$(compose_active ps --all --quiet 2>/dev/null || true)"
    if [ -n "$container_ids" ]; then
        # Compose prints one safe hexadecimal identifier per line.
        # shellcheck disable=SC2086
        raw_inspect="$(mktemp)"
        # shellcheck disable=SC2086
        docker_bounded inspect $container_ids >"$raw_inspect"
        python3 - "$raw_inspect" "$container_output" <<'PY'
import json
import sys
from pathlib import Path

source_path, target_path = sys.argv[1:]
target = Path(target_path)
with Path(source_path).open(encoding="utf-8") as stream:
    source = json.load(stream)
safe = []
for item in source:
    state = item.get("State") if isinstance(item.get("State"), dict) else {}
    health = state.get("Health") if isinstance(state.get("Health"), dict) else {}
    config = item.get("Config") if isinstance(item.get("Config"), dict) else {}
    labels = config.get("Labels") if isinstance(config.get("Labels"), dict) else {}
    networks = item.get("NetworkSettings", {}).get("Networks", {})
    mounts = item.get("Mounts") if isinstance(item.get("Mounts"), list) else []
    safe.append({
        "Id": item.get("Id"),
        "Image": item.get("Image"),
        "Name": item.get("Name"),
        "Created": item.get("Created"),
        "RestartCount": item.get("RestartCount"),
        "Config": {
            "Image": config.get("Image"),
            "Labels": {
                key: value for key, value in labels.items()
                if key.startswith("com.docker.compose.") or key.startswith("queen.benchmark.")
            },
        },
        "State": {
            "Status": state.get("Status"), "Running": state.get("Running"),
            "OOMKilled": state.get("OOMKilled"), "ExitCode": state.get("ExitCode"),
            "Error": state.get("Error"), "Pid": state.get("Pid"),
            "StartedAt": state.get("StartedAt"), "FinishedAt": state.get("FinishedAt"),
            "Health": {"Status": health.get("Status")},
        },
        "Networks": {
            name: {
                "NetworkID": value.get("NetworkID"), "EndpointID": value.get("EndpointID"),
                "Gateway": value.get("Gateway"), "IPAddress": value.get("IPAddress"),
                "Aliases": value.get("Aliases"),
            }
            for name, value in networks.items() if isinstance(value, dict)
        },
        "Mounts": [
            {key: mount.get(key) for key in ("Type", "Name", "Destination", "RW")}
            for mount in mounts if isinstance(mount, dict)
        ],
    })
target.write_text(json.dumps(safe, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
        rm -f -- "$raw_inspect"
    else
        printf '[]\n' >"$container_output"
    fi
}

capture_network() {
    network_output="$1"
    if [ -n "$ACTIVE_DB_NETWORK" ]; then
        docker_bounded network inspect "$ACTIVE_APP_NETWORK" "$ACTIVE_DB_NETWORK" >"$network_output" 2>&1 || printf '[]\n' >"$network_output"
    else
        docker_bounded network inspect "$ACTIVE_APP_NETWORK" >"$network_output" 2>&1 || printf '[]\n' >"$network_output"
    fi
}

capture_logs() {
    compose_active ps --all >"${ACTIVE_LANE_DIRECTORY}/compose-ps.txt" 2>&1 || true
    compose_active logs --no-color --timestamps >"${ACTIVE_LANE_DIRECTORY}/compose.log" 2>&1 || true
}

copy_results() {
    [ -n "$ACTIVE_VOLUME" ] || return 0
    docker_bounded volume inspect "$ACTIVE_VOLUME" >/dev/null 2>&1 || return 0
    # The variables in the script literal intentionally belong to the inner shell.
    # shellcheck disable=SC2016
    docker_bounded run --rm --user 0:0 \
        --mount "type=volume,src=${ACTIVE_VOLUME},dst=/from,readonly" \
        --mount "type=bind,src=${ACTIVE_LANE_DIRECTORY},dst=/to" \
        "$APP_IMAGE" sh -ceu '
            mkdir -p /to/results
            if [ -d "/from/$1" ]; then cp -a "/from/$1/." /to/results/; fi
        ' sh "$ACTIVE_RUN_ID" >/dev/null 2>&1 || true
}

preflight_project_absent() {
    preflight_containers="$(docker_bounded ps --all --quiet --filter "label=com.docker.compose.project=${ACTIVE_PROJECT}")"
    preflight_networks="$(docker_bounded network ls --quiet --filter "label=com.docker.compose.project=${ACTIVE_PROJECT}")"
    preflight_volumes="$(docker_bounded volume ls --quiet --filter "label=com.docker.compose.project=${ACTIVE_PROJECT}")"
    [ -z "$preflight_containers" ] || die "refusing pre-existing project-labelled containers for $ACTIVE_PROJECT"
    [ -z "$preflight_networks" ] || die "refusing pre-existing project-labelled networks for $ACTIVE_PROJECT"
    [ -z "$preflight_volumes" ] || die "refusing pre-existing project-labelled volumes for $ACTIVE_PROJECT"
}

verify_project_resources() {
    verification_containers="$(docker_bounded ps --all --quiet --filter "label=com.docker.compose.project=${ACTIVE_PROJECT}" 2>/dev/null)" || return 1
    while IFS= read -r verification_id; do
        [ -n "$verification_id" ] || continue
        verification_labels="$(docker_bounded inspect --format '{{index .Config.Labels "queen.benchmark.campaign"}}|{{index .Config.Labels "com.docker.compose.service"}}' "$verification_id" 2>/dev/null)" || return 1
        verification_campaign="${verification_labels%%|*}"
        verification_service="${verification_labels#*|}"
        [ "$verification_campaign" = "$campaign_id" ] || return 1
        case "${ACTIVE_ENGINE}:${verification_service}" in
            horizon:redis|horizon:horizon|horizon:producer) ;;
            queen-php:postgres|queen-php:broker|queen-php:queen-php|queen-php:producer) ;;
            queen-rust:postgres|queen-rust:broker|queen-rust:queen-rust|queen-rust:producer) ;;
            *) return 1 ;;
        esac
    done <<EOF
${verification_containers}
EOF

    verification_networks="$(docker_bounded network ls --quiet --filter "label=com.docker.compose.project=${ACTIVE_PROJECT}" 2>/dev/null)" || return 1
    while IFS= read -r verification_id; do
        [ -n "$verification_id" ] || continue
        verification_labels="$(docker_bounded network inspect --format '{{index .Labels "queen.benchmark.campaign"}}|{{.Name}}' "$verification_id" 2>/dev/null)" || return 1
        verification_campaign="${verification_labels%%|*}"
        verification_name="${verification_labels#*|}"
        [ "$verification_campaign" = "$campaign_id" ] || return 1
        case "$verification_name" in
            "$ACTIVE_APP_NETWORK") ;;
            "$ACTIVE_DB_NETWORK") [ -n "$ACTIVE_DB_NETWORK" ] || return 1 ;;
            *) return 1 ;;
        esac
    done <<EOF
${verification_networks}
EOF

    verification_volumes="$(docker_bounded volume ls --quiet --filter "label=com.docker.compose.project=${ACTIVE_PROJECT}" 2>/dev/null)" || return 1
    while IFS= read -r verification_name; do
        [ -n "$verification_name" ] || continue
        verification_campaign="$(docker_bounded volume inspect --format '{{index .Labels "queen.benchmark.campaign"}}' "$verification_name" 2>/dev/null)" || return 1
        [ "$verification_campaign" = "$campaign_id" ] || return 1
        if [ "$verification_name" = "$ACTIVE_VOLUME" ]; then
            continue
        fi
        case ",${ACTIVE_BACKEND_VOLUMES}," in
            *",${verification_name},"*) ;;
            *) return 1 ;;
        esac
    done <<EOF
${verification_volumes}
EOF
    return 0
}

project_resources_absent() {
    cleanup_containers="$(docker_bounded ps --all --quiet --filter "label=com.docker.compose.project=${ACTIVE_PROJECT}" 2>/dev/null)" \
        || return 1
    cleanup_networks="$(docker_bounded network ls --quiet --filter "label=com.docker.compose.project=${ACTIVE_PROJECT}" 2>/dev/null)" \
        || return 1
    cleanup_volumes="$(docker_bounded volume ls --quiet --filter "label=com.docker.compose.project=${ACTIVE_PROJECT}" 2>/dev/null)" \
        || return 1
    cleanup_result_volumes="$(docker_bounded volume ls --quiet --filter "name=^${ACTIVE_VOLUME}$" 2>/dev/null)" \
        || return 1
    [ -z "$cleanup_containers" ] \
        && [ -z "$cleanup_networks" ] \
        && [ -z "$cleanup_volumes" ] \
        && [ -z "$cleanup_result_volumes" ]
}

record_cleanup_residue() {
    [ -n "$ACTIVE_LANE_DIRECTORY" ] || return 0
    {
        printf 'project\t%s\n' "$ACTIVE_PROJECT"
        printf 'containers\n'
        docker_bounded ps --all --no-trunc --filter "label=com.docker.compose.project=${ACTIVE_PROJECT}" \
            --format '{{.ID}}\t{{.Names}}\t{{.Status}}' 2>/dev/null || true
        printf 'networks\n'
        docker_bounded network ls --filter "label=com.docker.compose.project=${ACTIVE_PROJECT}" \
            --format '{{.ID}}\t{{.Name}}' 2>/dev/null || true
        printf 'volumes\n'
        docker_bounded volume ls --filter "label=com.docker.compose.project=${ACTIVE_PROJECT}" \
            --format '{{.Name}}' 2>/dev/null || true
    } >"${ACTIVE_LANE_DIRECTORY}/cleanup-residue.tsv"
}

cleanup_active_lane() {
    if [ -n "$ACTIVE_PROJECT" ]; then
        if verify_project_resources; then
            if ! compose_active down --volumes --timeout 20 >/dev/null 2>&1; then
                record_cleanup_residue
                return 1
            fi
            if [ -n "$ACTIVE_VOLUME" ] && docker_bounded volume inspect "$ACTIVE_VOLUME" >/dev/null 2>&1; then
                cleanup_campaign="$(docker_bounded volume inspect --format '{{index .Labels "queen.benchmark.campaign"}}' "$ACTIVE_VOLUME" 2>/dev/null || true)"
                if [ "$cleanup_campaign" = "$campaign_id" ]; then
                    if ! docker_bounded volume rm "$ACTIVE_VOLUME" >/dev/null 2>&1; then
                        record_cleanup_residue
                        return 1
                    fi
                else
                    printf 'warning: retained unverified result volume %s\n' "$ACTIVE_VOLUME" >&2
                    record_cleanup_residue
                    return 1
                fi
            fi
        else
            printf 'warning: retained project %s because resource ownership verification failed\n' "$ACTIVE_PROJECT" >&2
            record_cleanup_residue
            return 1
        fi
        if ! project_resources_absent; then
            printf 'warning: retained cleanup residue for project %s\n' "$ACTIVE_PROJECT" >&2
            record_cleanup_residue
            return 1
        fi
    fi
    ACTIVE_PROJECT=""
    ACTIVE_ENGINE=""
    ACTIVE_VOLUME=""
    ACTIVE_LANE_DIRECTORY=""
    ACTIVE_RUN_ID=""
    ACTIVE_SCENARIO=""
    ACTIVE_OVERRIDE_FILE=""
    ACTIVE_APP_NETWORK=""
    ACTIVE_DB_NETWORK=""
    ACTIVE_BACKEND_VOLUMES=""
    return 0
}

# Invoked indirectly by the EXIT/interrupt trap below.
# shellcheck disable=SC2329
on_exit() {
    exit_status=$?
    trap - EXIT INT TERM
    if [ "$exit_status" -ne 0 ] && [ -n "$ACTIVE_LANE_DIRECTORY" ]; then
        capture_logs
        capture_containers "${ACTIVE_LANE_DIRECTORY}/containers-error.json" || true
        copy_results
        python3 - "$ACTIVE_LANE_DIRECTORY" "$exit_status" <<'PY'
import datetime as dt
import json
import sys
from pathlib import Path

root, status = sys.argv[1:]
payload = {
    "schema": "queen.laravel-supervisors.infrastructure-fault-error/v1",
    "exit_status": int(status),
    "captured_at": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
    "message": "The infrastructure-fault harness failed closed; inspect the retained partial evidence.",
}
Path(root, "harness-error.json").write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
        write_manifest "$ACTIVE_LANE_DIRECTORY" || true
    fi
    if ! cleanup_active_lane; then
        [ "$exit_status" -ne 0 ] || exit_status=1
    fi
    write_manifest "$OUTPUT_DIRECTORY" || true
    exit "$exit_status"
}
trap on_exit EXIT INT TERM

dispatch_with_retries() {
    dispatch_run_id="$1"
    dispatch_connection="$2"
    # Embedded PHP is single-quoted so shell expansion cannot alter PHP variables.
    # shellcheck disable=SC2016
    producer_bounded "$DISPATCH_TIMEOUT" php -r '
require "vendor/autoload.php";
$app = require "bootstrap/app.php";
$kernel = $app->make(Illuminate\Contracts\Console\Kernel::class);
$kernel->bootstrap();
$runId = $argv[1];
$jobs = (int) $argv[2];
$sleepMs = (int) $argv[3];
$cpuIterations = (int) $argv[4];
$tries = (int) $argv[5];
$connection = $argv[6];
$queue = "benchmark";
$sink = $app->make(App\Support\JsonlResultSink::class);
$ledger = $app->make(App\Support\BenchmarkEffectLedger::class);
$sink->reserveRun($runId);
$ledger->reserveRun($runId);
$queueConnection = $app->make(Illuminate\Contracts\Queue\Factory::class)->connection($connection);
$startedAt = hrtime(true);
for ($index = 0; $index < $jobs; ++$index) {
    $job = new App\Jobs\BenchmarkJob(
        runId: $runId,
        jobId: sprintf("%09d", $index),
        enqueuedAtNs: hrtime(true),
        sleepMs: $sleepMs,
        cpuIterations: $cpuIterations,
    );
    $job->tries = $tries;
    $job->onConnection($connection)->onQueue($queue);
    $queueConnection->push($job, "", $queue);
}
$finishedAt = hrtime(true);
$manifest = [
    "run_id" => $runId, "jobs" => $jobs, "connection" => $connection,
    "queue" => $queue, "dispatch_mode" => "single", "dispatch_batch_size" => 1,
    "job_tries" => $tries, "sleep_ms" => $sleepMs, "cpu_iterations" => $cpuIterations,
    "ledger_mode" => $ledger->mode(),
    "ledger_semantics" => "fixture-local idempotent effect keyed by run_id+job_id; not queue-ACK atomic",
    "dispatch_started_ns" => $startedAt, "dispatch_finished_ns" => $finishedAt,
    "dispatch_duration_ns" => $finishedAt - $startedAt,
];
$manifest["metadata_path"] = $sink->writeDispatchMetadata($runId, $manifest);
echo json_encode($manifest, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES), PHP_EOL;
' "$dispatch_run_id" "$JOBS" "$SLEEP_MS" "$CPU_ITERATIONS" "$JOB_TRIES" "$dispatch_connection"
}

capture_queue_once() {
    queue_container="$1"
    queue_connection="$2"
    queue_run_id="$3"
    queue_output="$4"
    set +e
    docker_bounded exec "$queue_container" php artisan bench:queue-state --no-ansi \
        --run-id="$queue_run_id" --connection="$queue_connection" --queue=benchmark \
        --wait=0 --poll-ms=100 --settle-ms=0 >"$queue_output" 2>"${queue_output}.stderr.log"
    queue_status=$?
    set -e
    return "$queue_status"
}

capture_ledger_activity() {
    ledger_container="$1"
    ledger_run_id="$2"
    ledger_output="$3"
    # Shell expansion is deliberately disabled for the embedded PHP variables.
    # shellcheck disable=SC2016
    docker_bounded exec "$ledger_container" php -r '
$path = "/results/".$argv[1]."/ledger.sqlite3";
$connection = new PDO("sqlite:".$path, null, null, [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
$connection->exec("PRAGMA busy_timeout = 5000");
$row = $connection->query(
    "SELECT COUNT(*) AS attempts, " .
    "SUM(CASE WHEN outcome IS NULL THEN 1 ELSE 0 END) AS open_attempts, " .
    "MAX(attempt_number) AS max_attempt_number FROM attempts"
)->fetch(PDO::FETCH_ASSOC);
echo json_encode([
    "schema" => "queen.laravel-supervisors.ledger-activity/v1",
    "run_id" => $argv[1],
    "attempts" => (int) ($row["attempts"] ?? 0),
    "open_attempts" => (int) ($row["open_attempts"] ?? 0),
    "max_attempt_number" => isset($row["max_attempt_number"]) ? (int) $row["max_attempt_number"] : null,
], JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES), PHP_EOL;
' "$ledger_run_id" >"$ledger_output" 2>"${ledger_output}.stderr.log"
}

fault_qualification_passes() {
    qualification_queue="$1"
    qualification_ledger="$2"
    python3 - "$qualification_queue" "$qualification_ledger" <<'PY'
import json
import sys
from pathlib import Path

try:
    queue = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    ledger = json.loads(Path(sys.argv[2]).read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError) as exception:
    raise SystemExit(f"invalid pre-fault qualification evidence: {exception}")
state = queue.get("state") if isinstance(queue.get("state"), dict) else {}
supported = queue.get("supported") if isinstance(queue.get("supported"), dict) else {}
ready = state.get("ready")
if supported.get("ready") is not True or not isinstance(ready, int) or isinstance(ready, bool) or ready <= 0:
    raise SystemExit("pre-fault queue evidence does not prove a ready backlog")
open_attempts = ledger.get("open_attempts")
if not isinstance(open_attempts, int) or isinstance(open_attempts, bool) or open_attempts <= 0:
    raise SystemExit("pre-fault ledger evidence does not prove a started in-flight attempt")
PY
}

wait_for_fault_qualification() {
    qualification_container="$1"
    qualification_queue_output="${ACTIVE_LANE_DIRECTORY}/queue-before-fault.json"
    qualification_ledger_output="${ACTIVE_LANE_DIRECTORY}/ledger-before-fault.json"
    qualification_deadline=$(( $(monotonic_seconds) + READY_TIMEOUT ))
    while :; do
        capture_queue_once "$qualification_container" "$BENCH_CONNECTION" "$ACTIVE_RUN_ID" "${qualification_queue_output}.candidate" || true
        capture_ledger_activity "$qualification_container" "$ACTIVE_RUN_ID" "${qualification_ledger_output}.candidate" || true
        if fault_qualification_passes "${qualification_queue_output}.candidate" "${qualification_ledger_output}.candidate" 2>/dev/null; then
            mv "${qualification_queue_output}.candidate" "$qualification_queue_output"
            mv "${qualification_ledger_output}.candidate" "$qualification_ledger_output"
            return 0
        fi
        [ "$(monotonic_seconds)" -lt "$qualification_deadline" ] || die "timed out waiting for ready backlog and an open ledger attempt"
        sleep 0.2
    done
}

network_contains() {
    network_name="$1"
    network_container_id="$2"
    docker_bounded network inspect "$network_name" --format '{{json .Containers}}' \
        | python3 -c 'import json,sys; values=json.load(sys.stdin) or {}; target=sys.argv[1]; raise SystemExit(0 if target in values else 1)' "$network_container_id"
}

verify_compose_target() {
    target_container="$1"
    target_service="$2"
    target_project="$(docker_bounded inspect --format '{{index .Config.Labels "com.docker.compose.project"}}' "$target_container")"
    observed_service="$(docker_bounded inspect --format '{{index .Config.Labels "com.docker.compose.service"}}' "$target_container")"
    [ "$target_project" = "$ACTIVE_PROJECT" ] || die "refusing target outside Compose project $ACTIVE_PROJECT"
    [ "$observed_service" = "$target_service" ] || die "refusing target service $observed_service; expected $target_service"
}

write_fault_evidence() {
    evidence_action="$1"
    evidence_target="$2"
    evidence_observed="$3"
    evidence_recovered="$4"
    evidence_old_tree_gone="$5"
    python3 - "${ACTIVE_LANE_DIRECTORY}/fault-evidence.json" "$ACTIVE_SCENARIO" "$evidence_action" \
        "$evidence_target" "$evidence_observed" "$evidence_recovered" "$evidence_old_tree_gone" <<'PY'
import json
import sys
from pathlib import Path

path, scenario, action, target, observed, recovered, old_tree_gone = sys.argv[1:]
payload = {
    "schema": "queen.laravel-supervisors.infrastructure-fault-evidence/v1",
    "scenario": scenario,
    "action": action,
    "target": target,
    "fault_observed": observed == "true",
    "recovery_observed": recovered == "true",
    "old_process_tree_gone_before_restart": None if old_tree_gone == "null" else old_tree_gone == "true",
}
Path(path).write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
}

inject_and_recover() {
    fault_app_id="$1"
    fault_backend_id="$2"
    fault_network="$ACTIVE_APP_NETWORK"
    target_service=""
    target_id=""
    old_tree_gone=null

    case "$ACTIVE_SCENARIO" in
        redis-restart)
            target_service=redis
            target_id="$fault_backend_id"
            verify_compose_target "$target_id" redis
            docker_bounded kill --signal KILL "$target_id" >/dev/null
            [ "$(docker_bounded inspect --format '{{.State.Running}}' "$target_id")" = false ] || die "redis did not stop"
            append_timeline "${ACTIVE_LANE_DIRECTORY}/timeline.jsonl" fault_observed "redis stopped"
            capture_containers "${ACTIVE_LANE_DIRECTORY}/containers-during-fault.json"
            capture_network "${ACTIVE_LANE_DIRECTORY}/network-during-fault.json"
            capture_process_tree "$fault_app_id" "${ACTIVE_LANE_DIRECTORY}/process-tree-during-fault.txt"
            sleep "$FAULT_HOLD_SECONDS"
            docker_bounded start "$target_id" >/dev/null
            wait_for_health "$target_id" redis
            write_fault_evidence service-sigkill-start redis true true "$old_tree_gone"
            ;;
        broker-restart)
            target_service=broker
            target_id="$fault_backend_id"
            verify_compose_target "$target_id" broker
            docker_bounded kill --signal KILL "$target_id" >/dev/null
            [ "$(docker_bounded inspect --format '{{.State.Running}}' "$target_id")" = false ] || die "broker did not stop"
            append_timeline "${ACTIVE_LANE_DIRECTORY}/timeline.jsonl" fault_observed "broker stopped"
            capture_containers "${ACTIVE_LANE_DIRECTORY}/containers-during-fault.json"
            capture_network "${ACTIVE_LANE_DIRECTORY}/network-during-fault.json"
            capture_process_tree "$fault_app_id" "${ACTIVE_LANE_DIRECTORY}/process-tree-during-fault.txt"
            sleep "$FAULT_HOLD_SECONDS"
            docker_bounded start "$target_id" >/dev/null
            wait_for_health "$target_id" broker
            write_fault_evidence service-sigkill-start broker true true "$old_tree_gone"
            ;;
        postgres-restart)
            target_service=postgres
            target_id="$(compose_active ps --all --quiet postgres)"
            verify_compose_target "$target_id" postgres
            docker_bounded kill --signal KILL "$target_id" >/dev/null
            [ "$(docker_bounded inspect --format '{{.State.Running}}' "$target_id")" = false ] || die "postgres did not stop"
            append_timeline "${ACTIVE_LANE_DIRECTORY}/timeline.jsonl" fault_observed "postgres stopped"
            capture_containers "${ACTIVE_LANE_DIRECTORY}/containers-during-fault.json"
            capture_network "${ACTIVE_LANE_DIRECTORY}/network-during-fault.json"
            capture_process_tree "$fault_app_id" "${ACTIVE_LANE_DIRECTORY}/process-tree-during-fault.txt"
            sleep "$FAULT_HOLD_SECONDS"
            docker_bounded start "$target_id" >/dev/null
            wait_for_health "$target_id" postgres
            write_fault_evidence service-sigkill-start postgres true true "$old_tree_gone"
            ;;
        app-backend-network-partition)
            target_service="$ACTIVE_ENGINE"
            target_id="$fault_app_id"
            network_contains "$fault_network" "$target_id" || die "application is not attached to $fault_network"
            docker_bounded network disconnect "$fault_network" "$target_id"
            if network_contains "$fault_network" "$target_id"; then
                die "application network partition was not observed"
            fi
            network_contains "$fault_network" "$fault_backend_id" || die "backend left the app-backend network during application partition"
            append_timeline "${ACTIVE_LANE_DIRECTORY}/timeline.jsonl" fault_observed "application disconnected from backend network"
            capture_containers "${ACTIVE_LANE_DIRECTORY}/containers-during-fault.json"
            capture_network "${ACTIVE_LANE_DIRECTORY}/network-during-fault.json"
            capture_process_tree "$fault_app_id" "${ACTIVE_LANE_DIRECTORY}/process-tree-during-fault.txt"
            sleep "$FAULT_HOLD_SECONDS"
            docker_bounded network connect --alias "$target_service" "$fault_network" "$target_id"
            network_contains "$fault_network" "$target_id" || die "application network recovery was not observed"
            wait_for_health "$target_id" "$ACTIVE_ENGINE"
            write_fault_evidence network-disconnect-connect "$target_service" true true "$old_tree_gone"
            ;;
        broker-postgres-network-partition)
            fault_network="$ACTIVE_DB_NETWORK"
            target_service=broker
            target_id="$fault_backend_id"
            network_contains "$fault_network" "$target_id" || die "broker is not attached to $fault_network"
            docker_bounded network disconnect "$fault_network" "$target_id"
            if network_contains "$fault_network" "$target_id"; then
                die "broker network partition was not observed"
            fi
            network_contains "$ACTIVE_APP_NETWORK" "$target_id" || die "broker also left the app-backend network"
            postgres_id="$(compose_active ps --quiet postgres)"
            network_contains "$fault_network" "$postgres_id" || die "postgres left its network during broker partition"
            append_timeline "${ACTIVE_LANE_DIRECTORY}/timeline.jsonl" fault_observed "broker disconnected from postgres network"
            capture_containers "${ACTIVE_LANE_DIRECTORY}/containers-during-fault.json"
            capture_network "${ACTIVE_LANE_DIRECTORY}/network-during-fault.json"
            capture_process_tree "$fault_app_id" "${ACTIVE_LANE_DIRECTORY}/process-tree-during-fault.txt"
            sleep "$FAULT_HOLD_SECONDS"
            docker_bounded network connect --alias broker "$fault_network" "$target_id"
            network_contains "$fault_network" "$target_id" || die "broker network recovery was not observed"
            wait_for_health "$target_id" broker
            write_fault_evidence network-disconnect-connect broker true true "$old_tree_gone"
            ;;
        master-sigkill)
            target_service="$ACTIVE_ENGINE"
            target_id="$fault_app_id"
            verify_compose_target "$target_id" "$ACTIVE_ENGINE"
            case "$ACTIVE_ENGINE" in
                horizon) master_needle='artisan horizon' ;;
                queen-php) master_needle='artisan queen:supervise' ;;
                queen-rust) master_needle='queen-supervisor --php' ;;
            esac
            master_rows="$(docker_bounded exec "$target_id" ps -eo pid=,ppid=,args= | awk -v needle="$master_needle" '
                index($0, needle) > 0 && $1 ~ /^[0-9]+$/ && $1 != 1 && $2 == 1 {
                    print $1 "\t" $2 "\t" substr($0, index($0,$3))
                }
            ')"
            master_count="$(printf '%s\n' "$master_rows" | awk 'NF { count += 1 } END { print count + 0 }')"
            [ "$master_count" -eq 1 ] || die "expected exactly one $ACTIVE_ENGINE master, observed $master_count"
            master_pid="$(printf '%s\n' "$master_rows" | awk '{print $1}')"
            [ "$master_pid" -ne 1 ] || die "refusing to signal container init instead of the supervisor master"
            printf '%s\n' "$master_rows" >"${ACTIVE_LANE_DIRECTORY}/master-target.tsv"
            docker_bounded exec "$target_id" kill -KILL "$master_pid"
            stop_deadline=$(( $(monotonic_seconds) + READY_TIMEOUT ))
            while [ "$(docker_bounded inspect --format '{{.State.Running}}' "$target_id" 2>/dev/null || true)" = true ]; do
                [ "$(monotonic_seconds)" -lt "$stop_deadline" ] || die "application container did not stop after master SIGKILL"
                sleep 0.2
            done
            container_pid="$(docker_bounded inspect --format '{{.State.Pid}}' "$target_id")"
            [ "$container_pid" -eq 0 ] || die "stopped application still exposes host PID $container_pid"
            if docker_bounded top "$target_id" -eo pid,ppid,args >"${ACTIVE_LANE_DIRECTORY}/process-tree-during-fault.txt" 2>&1; then
                die "Docker still exposed the old process tree after master SIGKILL"
            fi
            old_tree_gone=true
            append_timeline "${ACTIVE_LANE_DIRECTORY}/timeline.jsonl" fault_observed "supervisor master killed and old process tree gone"
            capture_containers "${ACTIVE_LANE_DIRECTORY}/containers-during-fault.json"
            capture_network "${ACTIVE_LANE_DIRECTORY}/network-during-fault.json"
            sleep "$FAULT_HOLD_SECONDS"
            compose_active_bounded "$STARTUP_TIMEOUT" start "$ACTIVE_ENGINE"
            wait_for_supervisor_capacity "$target_id" master-restart
            write_fault_evidence master-sigkill "$target_service" true true "$old_tree_gone"
            ;;
    esac
    append_timeline "${ACTIVE_LANE_DIRECTORY}/timeline.jsonl" recovery_observed "$target_service"
}

write_lane_result() {
    result_lane="$1"
    result_engine="$2"
    result_scenario="$3"
    result_run_id="$4"
    result_completion_status="$5"
    result_queue_status="$6"
    result_ledger_status="$7"
    python3 - "$result_lane" "$result_engine" "$result_scenario" "$result_run_id" "$JOBS" "$JOB_TRIES" \
        "$result_completion_status" "$result_queue_status" "$result_ledger_status" <<'PY'
import json
import sqlite3
import sys
from pathlib import Path

lane_raw, engine, scenario, run_id, jobs_raw, tries_raw, completion_raw, queue_raw, ledger_raw = sys.argv[1:]
lane = Path(lane_raw)
jobs = int(jobs_raw)
tries = int(tries_raw)

def read_json(name: str, default):
    try:
        return json.loads((lane / name).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default

records = []
malformed = 0
for path in sorted((lane / "results" / "events").glob("*.jsonl")):
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            malformed += 1
            continue
        if value.get("run_id") == run_id and isinstance(value.get("completed_at_ns"), int):
            records.append(value)

expected_ids = {f"{index:09d}" for index in range(jobs)}
observed_ids = {value.get("job_id") for value in records if isinstance(value.get("job_id"), str)}
missing_ids = sorted(expected_ids - observed_ids)
unexpected_ids = sorted(observed_ids - expected_ids)
counts = {}
for value in records:
    job_id = value.get("job_id")
    if isinstance(job_id, str):
        counts[job_id] = counts.get(job_id, 0) + 1
duplicate_completions = sum(count - 1 for count in counts.values() if count > 1)
attempts = [value.get("attempt") for value in records]
attempts_within_bounds = bool(attempts) and all(
    isinstance(value, int) and not isinstance(value, bool) and 1 <= value <= tries for value in attempts
)
ledger_attempt_numbers = []
try:
    connection = sqlite3.connect(
        f"file:{lane / 'results' / 'ledger.sqlite3'}?mode=ro", uri=True, timeout=30.0
    )
    try:
        ledger_attempt_numbers = [
            int(row[0]) for row in connection.execute(
                "SELECT attempt_number FROM attempts ORDER BY started_at_ns, attempt_id"
            )
        ]
    finally:
        connection.close()
except (OSError, sqlite3.Error, TypeError, ValueError):
    ledger_attempt_numbers = []
ledger_attempts_within_bounds = bool(ledger_attempt_numbers) and all(
    1 <= value <= tries for value in ledger_attempt_numbers
)

completion = read_json("completion.json", {})
queue = read_json("queue-final.json", {})
ledger = read_json("ledger-check.json", {})
fault = read_json("fault-evidence.json", {})
dispatch = read_json("dispatch.json", {})
backlog = read_json("queue-before-fault.json", {})
ledger_before_fault = read_json("ledger-before-fault.json", {})
backend_probe_before = read_json("backend-probe-before.json", {})
backend_probe_recovered = read_json("backend-probe-after-recovery.json", {})
backend_probe_final = read_json("backend-probe-final.json", {})
containers_before = read_json("containers-before.json", [])
containers = read_json("containers-final.json", [])
queue_state = queue.get("state") if isinstance(queue.get("state"), dict) else {}
queue_quiescent = (
    queue.get("quiescent") is True
    and queue.get("timed_out") is False
    and queue.get("probe_errors") == []
    and queue_state.get("size") == 0
)
container_recovery = bool(containers) and all(
    isinstance(item, dict)
    and isinstance(item.get("State"), dict)
    and item["State"].get("Running") is True
    and item["State"].get("OOMKilled") is False
    for item in containers
)
before_ids = {item.get("Id") for item in containers_before if isinstance(item, dict) and isinstance(item.get("Id"), str)}
final_ids = {item.get("Id") for item in containers if isinstance(item, dict) and isinstance(item.get("Id"), str)}
exact_container_set = bool(before_ids) and before_ids == final_ids

def by_service(items):
    result = {}
    for item in items:
        labels = item.get("Config", {}).get("Labels", {}) if isinstance(item, dict) else {}
        service = labels.get("com.docker.compose.service") if isinstance(labels, dict) else None
        if isinstance(service, str):
            result[service] = item
    return result

before_services = by_service(containers_before)
final_services = by_service(containers)
expected_restarted_service = {
    "redis-restart": "redis",
    "broker-restart": "broker",
    "postgres-restart": "postgres",
    "master-sigkill": engine,
}.get(scenario)
lifecycle_exact = True
for service, initial in before_services.items():
    final = final_services.get(service, {})
    initial_start = initial.get("State", {}).get("StartedAt")
    final_start = final.get("State", {}).get("StartedAt")
    if service == expected_restarted_service:
        lifecycle_exact = lifecycle_exact and bool(initial_start) and bool(final_start) and initial_start != final_start
    else:
        lifecycle_exact = lifecycle_exact and bool(initial_start) and initial_start == final_start

backlog_state = backlog.get("state") if isinstance(backlog.get("state"), dict) else {}
backlog_supported = backlog.get("supported") if isinstance(backlog.get("supported"), dict) else {}
ready = backlog_state.get("ready")
backlog_proved = (
    backlog_supported.get("ready") is True
    and isinstance(ready, int) and not isinstance(ready, bool) and ready > 0
)
open_attempts = ledger_before_fault.get("open_attempts")
inflight_attempt_proved = (
    isinstance(open_attempts, int) and not isinstance(open_attempts, bool) and open_attempts > 0
)

def functional_probe(value, require_empty):
    state = value.get("state") if isinstance(value.get("state"), dict) else {}
    passed = (
        isinstance(value.get("implementation"), str)
        and isinstance(state.get("size"), int) and not isinstance(state.get("size"), bool)
        and value.get("probe_errors") == []
    )
    return passed and (not require_empty or (state.get("size") == 0 and value.get("quiescent") is True))

backend_functional = (
    functional_probe(backend_probe_before, True)
    and functional_probe(backend_probe_recovered, False)
    and functional_probe(backend_probe_final, True)
)
try:
    final_worker_rows = [
        line for line in (lane / "workers-final.tsv").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
except OSError:
    final_worker_rows = []
supervisor_ready_at_capacity = (
    len(final_worker_rows) == int(
        read_json("configuration.json", {}).get("benchmark", {}).get("workers", 0) or 0
    )
    and (lane / "supervisor-readiness-final.txt").is_file()
)
durable_dispatch = dispatch.get("ledger_mode") == "durable" and dispatch.get("jobs") == jobs
evidence_complete = all((lane / name).is_file() for name in (
    "timeline.jsonl", "containers-before.json", "containers-during-fault.json",
    "containers-after-recovery.json", "containers-final.json", "process-tree-before.txt",
    "process-tree-pre-fault.txt", "process-tree-during-fault.txt",
    "process-tree-after-recovery.txt", "process-tree-final.txt", "network-before.json",
    "network-during-fault.json", "network-after-recovery.json", "network-final.json",
    "backend-probe-before.json", "backend-probe-after-recovery.json", "backend-probe-final.json",
    "ledger-before-fault.json", "workers-final.tsv", "supervisor-readiness-final.txt",
))
master_tree_gate = scenario != "master-sigkill" or fault.get("old_process_tree_gone_before_restart") is True
exact_set = observed_ids == expected_ids and not missing_ids and not unexpected_ids
at_least_once = (
    int(completion_raw) == 0
    and int(queue_raw) == 0
    and int(ledger_raw) == 0
    and completion.get("complete") is True
    and exact_set
    and malformed == 0
    and attempts_within_bounds
    and ledger_attempts_within_bounds
    and queue_quiescent
    and fault.get("fault_observed") is True
    and fault.get("recovery_observed") is True
    and container_recovery
    and exact_container_set
    and lifecycle_exact
    and backlog_proved
    and inflight_attempt_proved
    and backend_functional
    and supervisor_ready_at_capacity
    and durable_dispatch
    and evidence_complete
    and master_tree_gate
    and ledger.get("conservation_pass") is True
    and ledger.get("attempt_integrity_pass") is True
)
idempotent = (
    at_least_once
    and ledger.get("idempotent_effect_pass") is True
    and ledger.get("no_duplicate_side_effects_pass") is True
)
strict = (
    idempotent
    and duplicate_completions == 0
    and ledger.get("strict_execution_pass") is True
)
payload = {
    "schema": "queen.laravel-supervisors.infrastructure-fault-result/v1",
    "engine": engine,
    "scenario": scenario,
    "run_id": run_id,
    "expected_jobs": jobs,
    "observed_job_ids": len(observed_ids),
    "completion_records": len(records),
    "duplicate_completions": duplicate_completions,
    "missing_job_ids": missing_ids,
    "unexpected_job_ids": unexpected_ids,
    "malformed_event_lines": malformed,
    "attempts_within_bounds": attempts_within_bounds,
    "ledger_attempts_within_bounds": ledger_attempts_within_bounds,
    "max_ledger_attempt_number": max(ledger_attempt_numbers, default=None),
    "queue_quiescent": queue_quiescent,
    "container_recovery": container_recovery,
    "exact_container_set": exact_container_set,
    "container_lifecycle_exact": lifecycle_exact,
    "backlog_proved": backlog_proved,
    "inflight_attempt_proved": inflight_attempt_proved,
    "backend_functional": backend_functional,
    "supervisor_ready_at_capacity": supervisor_ready_at_capacity,
    "durable_dispatch": durable_dispatch,
    "evidence_complete": evidence_complete,
    "fault_evidence": fault,
    "gates": {
        "at_least_once": {"passed": at_least_once, "required": True},
        "idempotent_effect": {"passed": idempotent, "required": True},
        "strict_execution": {"passed": strict, "required": False},
    },
    "interpretation": (
        "At-least-once proves exact completion-set recovery and settled queue conservation. "
        "Idempotent-effect additionally proves one fixture effect per job. Strict execution "
        "requires a single execution/completion and is intentionally not an at-least-once guarantee."
    ),
}
(lane / "result.json").write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
}

run_lane() {
    lane_engine="$1"
    lane_scenario="$2"
    lane_index="$3"
    ACTIVE_ENGINE="$lane_engine"
    ACTIVE_SCENARIO="$lane_scenario"
    lane_suffix="$(printf '%s-%03d-%s-%s' "$campaign_token" "$lane_index" "$lane_engine" "$lane_scenario" | tr -cd 'a-z0-9-')"
    ACTIVE_PROJECT="qlb-infra-${lane_suffix}"
    ACTIVE_VOLUME="qlb-infra-results-${lane_suffix}"
    ACTIVE_LANE_DIRECTORY="${OUTPUT_DIRECTORY}/${lane_scenario}/${lane_engine}"
    ACTIVE_RUN_ID="infra-${lane_index}-${lane_engine}-${lane_scenario}-${campaign_token}"
    ACTIVE_OVERRIDE_FILE="${ACTIVE_LANE_DIRECTORY}/topology-override.json"
    ACTIVE_APP_NETWORK="${ACTIVE_PROJECT}-app-backend"
    ACTIVE_DB_NETWORK=""
    mkdir -p "$ACTIVE_LANE_DIRECTORY"

    if [ "$lane_engine" = horizon ]; then
        python3 - "$ACTIVE_OVERRIDE_FILE" "$ACTIVE_APP_NETWORK" "$campaign_id" <<'PY'
import json
import sys
from pathlib import Path

path, app_network, campaign = sys.argv[1:]
labels = {"queen.benchmark.campaign": campaign}
payload = {
    "networks": {"app-backend": {"name": app_network, "labels": labels}},
    "volumes": {"results": {"labels": labels}, "redis-data": {"labels": labels}},
    "services": {
        "redis": {"networks": ["app-backend"], "labels": labels},
        "horizon": {"networks": ["app-backend"], "labels": labels},
        "producer": {"networks": ["app-backend"], "labels": labels},
    },
}
Path(path).write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
    else
        ACTIVE_DB_NETWORK="${ACTIVE_PROJECT}-broker-postgres"
        python3 - "$ACTIVE_OVERRIDE_FILE" "$ACTIVE_APP_NETWORK" "$ACTIVE_DB_NETWORK" "$lane_engine" "$campaign_id" <<'PY'
import json
import sys
from pathlib import Path

path, app_network, database_network, engine, campaign = sys.argv[1:]
labels = {"queen.benchmark.campaign": campaign}
payload = {
    "networks": {
        "app-backend": {"name": app_network, "labels": labels},
        "broker-postgres": {"name": database_network, "labels": labels},
    },
    "volumes": {
        "results": {"labels": labels},
        "postgres-data": {"labels": labels},
        "broker-buffers": {"labels": labels},
    },
    "services": {
        engine: {"networks": ["app-backend"], "labels": labels},
        "producer": {"networks": ["app-backend"], "labels": labels},
        "broker": {"networks": ["app-backend", "broker-postgres"], "labels": labels},
        "postgres": {"networks": ["broker-postgres"], "labels": labels},
    },
}
Path(path).write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
    fi

    export BENCH_RESULTS_VOLUME="$ACTIVE_VOLUME"
    export BENCH_PROFILE=fixed
    export BENCH_WORKERS="$WORKERS"
    export BENCH_MIN_WORKERS="$WORKERS"
    export BENCH_MAX_WORKERS="$WORKERS"
    export BENCH_QUEUE=benchmark
    export BENCH_GROUP=benchmark
    export BENCH_TIMEOUT="$WORKER_TIMEOUT"
    export BENCH_RETRY_AFTER="$RETRY_AFTER"
    export BENCH_LEDGER_MODE=durable
    export BENCH_DISPATCH_MODE=single
    export BENCH_QUEUES=''
    export BENCH_FAILED_DRIVER='null'
    export BENCH_LEASE_RENEWAL=false
    export BENCH_LEASE_RENEWAL_INTERVAL=''
    export QUEEN_PREFETCH=1
    export QUEEN_ACK_BATCH=1
    export QUEEN_BULK_BATCH=100
    export QUEEN_PARTITIONS=64
    export QUEEN_POP_FUSION=0
    export BENCH_REDIS_APPENDONLY=yes
    export BENCH_REDIS_APPEND_FSYNC=always
    if [ "$lane_engine" = horizon ]; then
        export BENCH_CONNECTION=redis
        backend_service=redis
        backend_volumes="${ACTIVE_PROJECT}_redis-data"
    else
        export BENCH_CONNECTION=queen
        backend_service=broker
        backend_volumes="${ACTIVE_PROJECT}_postgres-data,${ACTIVE_PROJECT}_broker-buffers"
    fi
    ACTIVE_BACKEND_VOLUMES="$backend_volumes"
    export QUEUE_CONNECTION="$BENCH_CONNECTION"
    export BENCH_LANE="$lane_engine"

    [ "$(image_id "$APP_IMAGE")" = "$EXPECTED_APP_IMAGE_ID" ] || die "application image tag changed after provenance capture"
    if [ "$lane_engine" = horizon ]; then
        [ "$(image_id "$REDIS_IMAGE")" = "$EXPECTED_REDIS_IMAGE_ID" ] || die "Redis image tag changed after provenance capture"
    else
        [ "$(image_id "$BROKER_IMAGE")" = "$EXPECTED_BROKER_IMAGE_ID" ] || die "broker image tag changed after provenance capture"
        [ "$(image_id "$POSTGRES_IMAGE")" = "$EXPECTED_POSTGRES_IMAGE_ID" ] || die "PostgreSQL image tag changed after provenance capture"
    fi
    preflight_project_absent
    if docker_bounded network inspect "$ACTIVE_APP_NETWORK" >/dev/null 2>&1; then
        die "refusing to reuse pre-existing network: $ACTIVE_APP_NETWORK"
    fi
    if [ -n "$ACTIVE_DB_NETWORK" ] && docker_bounded network inspect "$ACTIVE_DB_NETWORK" >/dev/null 2>&1; then
        die "refusing to reuse pre-existing network: $ACTIVE_DB_NETWORK"
    fi

    OLD_IFS="$IFS"
    IFS=',' read -r -a backend_volume_items <<EOF
${backend_volumes}
EOF
    IFS="$OLD_IFS"
    for volume_name in "$ACTIVE_VOLUME" "${backend_volume_items[@]}"; do
        if docker_bounded volume inspect "$volume_name" >/dev/null 2>&1; then
            die "refusing to reuse pre-existing volume: $volume_name"
        fi
    done
    docker_bounded volume create \
        --label "queen.benchmark.campaign=${campaign_id}" \
        --label "queen.benchmark.infrastructure.scenario=${lane_scenario}" \
        --label "com.docker.compose.project=${ACTIVE_PROJECT}" \
        --label "com.docker.compose.volume=results" "$ACTIVE_VOLUME" >/dev/null
    docker_bounded run --rm --user 0:0 --mount "type=volume,src=${ACTIVE_VOLUME},dst=/results" \
        "$APP_IMAGE" sh -ceu 'chown 1000:1000 /results; chmod 0770 /results'

    printf '\n[%03d/%03d] %s / %s\n' "$lane_index" "$lane_count" "$lane_scenario" "$lane_engine"
    append_timeline "${ACTIVE_LANE_DIRECTORY}/timeline.jsonl" lane_start "$lane_engine/$lane_scenario"
    compose_active config --services >"${ACTIVE_LANE_DIRECTORY}/compose-services.txt"
    compose_active_bounded "$STARTUP_TIMEOUT" up --detach --no-build "$lane_engine"
    app_id="$(compose_active ps --quiet "$lane_engine")"
    backend_id="$(compose_active ps --quiet "$backend_service")"
    [ -n "$app_id" ] || die "unable to resolve $lane_engine container"
    [ -n "$backend_id" ] || die "unable to resolve $backend_service container"
    verify_compose_target "$app_id" "$lane_engine"
    verify_compose_target "$backend_id" "$backend_service"
    wait_for_health "$backend_id" "$backend_service"
    wait_for_health "$app_id" "$lane_engine"
    producer_bounded "$DOCKER_OPERATION_TIMEOUT" php artisan bench:config --no-ansi >"${ACTIVE_LANE_DIRECTORY}/configuration.json"
    wait_for_supervisor_capacity "$app_id" initial
    capture_backend_probe "${ACTIVE_LANE_DIRECTORY}/backend-probe-before.json" true "$app_id"

    volume_arguments="$ACTIVE_VOLUME"
    for volume_name in "${backend_volume_items[@]}"; do
        docker_bounded volume inspect "$volume_name" >/dev/null 2>&1 || die "Compose did not create fresh volume: $volume_name"
        volume_arguments="${volume_arguments},${volume_name}"
    done
    OLD_IFS="$IFS"
    IFS=',' read -r -a all_volume_items <<EOF
${volume_arguments}
EOF
    IFS="$OLD_IFS"
    docker_bounded volume inspect "${all_volume_items[@]}" >"${ACTIVE_LANE_DIRECTORY}/volumes.json"
    python3 - "${ACTIVE_LANE_DIRECTORY}/fresh-volumes.json" "$volume_arguments" <<'PY'
import json
import sys
from pathlib import Path

path, names = sys.argv[1:]
Path(path).write_text(json.dumps({
    "schema": "queen.laravel-supervisors.fresh-volumes/v1",
    "freshness_checked_before_creation": True,
    "preexisting": False,
    "durable_named_volumes": names.split(","),
}, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY

    capture_containers "${ACTIVE_LANE_DIRECTORY}/containers-before.json"
    capture_network "${ACTIVE_LANE_DIRECTORY}/network-before.json"
    capture_process_tree "$app_id" "${ACTIVE_LANE_DIRECTORY}/process-tree-before.txt"
    append_timeline "${ACTIVE_LANE_DIRECTORY}/timeline.jsonl" lane_healthy "$app_id"

    dispatch_with_retries "$ACTIVE_RUN_ID" "$BENCH_CONNECTION" \
        >"${ACTIVE_LANE_DIRECTORY}/dispatch.json" 2>"${ACTIVE_LANE_DIRECTORY}/dispatch.stderr.log"
    append_timeline "${ACTIVE_LANE_DIRECTORY}/timeline.jsonl" dispatch_complete "$JOBS jobs"
    wait_for_fault_qualification "$app_id"
    append_timeline "${ACTIVE_LANE_DIRECTORY}/timeline.jsonl" fault_qualified "ready backlog and open ledger attempt"

    capture_process_tree "$app_id" "${ACTIVE_LANE_DIRECTORY}/process-tree-pre-fault.txt"
    inject_and_recover "$app_id" "$backend_id"
    capture_backend_probe "${ACTIVE_LANE_DIRECTORY}/backend-probe-after-recovery.json" false "$app_id"
    wait_for_supervisor_capacity "$app_id" after-recovery
    capture_containers "${ACTIVE_LANE_DIRECTORY}/containers-after-recovery.json"
    capture_network "${ACTIVE_LANE_DIRECTORY}/network-after-recovery.json"
    capture_process_tree "$app_id" "${ACTIVE_LANE_DIRECTORY}/process-tree-after-recovery.txt"

    set +e
    producer_bounded "$(( COMPLETION_TIMEOUT + DOCKER_OPERATION_TIMEOUT ))" php artisan bench:results --no-ansi "$ACTIVE_RUN_ID" --expected="$JOBS" \
        --wait="$COMPLETION_TIMEOUT" --poll-ms=200 >"${ACTIVE_LANE_DIRECTORY}/completion.json" \
        2>"${ACTIVE_LANE_DIRECTORY}/completion.stderr.log"
    completion_status=$?
    set -e
    append_timeline "${ACTIVE_LANE_DIRECTORY}/timeline.jsonl" completion_check "status=$completion_status"

    set +e
    producer_bounded "$(( COMPLETION_TIMEOUT + DOCKER_OPERATION_TIMEOUT ))" php artisan bench:queue-state --no-ansi --run-id="$ACTIVE_RUN_ID" \
        --connection="$BENCH_CONNECTION" --queue=benchmark --wait="$COMPLETION_TIMEOUT" \
        --poll-ms=100 --settle-ms=1000 >"${ACTIVE_LANE_DIRECTORY}/queue-final.json" \
        2>"${ACTIVE_LANE_DIRECTORY}/queue-final.stderr.log"
    queue_status=$?
    set -e
    append_timeline "${ACTIVE_LANE_DIRECTORY}/timeline.jsonl" quiescence_check "status=$queue_status"
    capture_backend_probe "${ACTIVE_LANE_DIRECTORY}/backend-probe-final.json" true "$app_id"
    wait_for_supervisor_capacity "$app_id" final

    set +e
    producer_bounded "$DOCKER_OPERATION_TIMEOUT" php artisan bench:ledger-checkpoint --no-ansi "$ACTIVE_RUN_ID" \
        >"${ACTIVE_LANE_DIRECTORY}/ledger-checkpoint.json" 2>"${ACTIVE_LANE_DIRECTORY}/ledger-checkpoint.stderr.log"
    checkpoint_status=$?
    set -e
    [ "$checkpoint_status" -eq 0 ] || append_timeline "${ACTIVE_LANE_DIRECTORY}/timeline.jsonl" ledger_checkpoint_failed "status=$checkpoint_status"

    capture_logs
    capture_containers "${ACTIVE_LANE_DIRECTORY}/containers-final.json"
    capture_network "${ACTIVE_LANE_DIRECTORY}/network-final.json"
    capture_process_tree "$app_id" "${ACTIVE_LANE_DIRECTORY}/process-tree-final.txt"
    copy_results

    set +e
    python3 "${SCRIPT_DIR}/analyze.py" ledger "${ACTIVE_LANE_DIRECTORY}/results" \
        --expected="$JOBS" --allow-open-attempts --allow-retried-executions \
        --output="${ACTIVE_LANE_DIRECTORY}/ledger-check.json"
    ledger_status=$?
    set -e
    append_timeline "${ACTIVE_LANE_DIRECTORY}/timeline.jsonl" ledger_check "status=$ledger_status"
    if [ "$checkpoint_status" -ne 0 ]; then
        ledger_status=1
    fi

    write_lane_result "$ACTIVE_LANE_DIRECTORY" "$lane_engine" "$lane_scenario" "$ACTIVE_RUN_ID" \
        "$completion_status" "$queue_status" "$ledger_status"
    append_timeline "${ACTIVE_LANE_DIRECTORY}/timeline.jsonl" lane_complete "$lane_engine/$lane_scenario"
    write_manifest "$ACTIVE_LANE_DIRECTORY"

    lane_required_pass="$(python3 -c '
import json, sys
with open(sys.argv[1], encoding="utf-8") as stream:
    gates = json.load(stream)["gates"]
print("1" if gates["at_least_once"]["passed"] and gates["idempotent_effect"]["passed"] else "0")
' "${ACTIVE_LANE_DIRECTORY}/result.json")"
    cleanup_active_lane
    if [ "$lane_required_pass" -ne 1 ]; then
        suite_status=1
    fi
    return 0
}

suite_status=0
lane_number=0
for scenario in "${SCENARIOS[@]}"; do
    for engine in "${ENGINES[@]}"; do
        if ! is_applicable "$engine" "$scenario"; then
            continue
        fi
        lane_number=$((lane_number + 1))
        run_lane "$engine" "$scenario" "$lane_number"
    done
done

python3 - "$OUTPUT_DIRECTORY" <<'PY'
import json
import sys
from pathlib import Path

root = Path(sys.argv[1])
results = [json.loads(path.read_text(encoding="utf-8")) for path in sorted(root.glob("*/*/result.json"))]
metadata = json.loads((root / "metadata.json").read_text(encoding="utf-8"))
planned_pairs = {
    (lane["engine"], lane["scenario"])
    for lane in metadata.get("lanes", [])
    if isinstance(lane, dict)
}
observed_pairs = {(result["engine"], result["scenario"]) for result in results}
missing_pairs = sorted(planned_pairs - observed_pairs)
unexpected_pairs = sorted(observed_pairs - planned_pairs)
coverage_exact = bool(planned_pairs) and observed_pairs == planned_pairs
required = coverage_exact and bool(results) and all(
    result["gates"]["at_least_once"]["passed"]
    and result["gates"]["idempotent_effect"]["passed"]
    for result in results
)
report = {
    "schema": "queen.laravel-supervisors.infrastructure-fault-report/v1",
    "lanes": results,
    "planned_lane_count": len(planned_pairs),
    "observed_lane_count": len(observed_pairs),
    "exact_lane_coverage": coverage_exact,
    "missing_lanes": [{"engine": engine, "scenario": scenario} for engine, scenario in missing_pairs],
    "unexpected_lanes": [{"engine": engine, "scenario": scenario} for engine, scenario in unexpected_pairs],
    "required_gate_passed": required,
    "all_at_least_once_passed": bool(results) and all(result["gates"]["at_least_once"]["passed"] for result in results),
    "all_idempotent_effect_passed": bool(results) and all(result["gates"]["idempotent_effect"]["passed"] for result in results),
    "all_strict_execution_passed": bool(results) and all(result["gates"]["strict_execution"]["passed"] for result in results),
}
(root / "report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")

lines = [
    "# Infrastructure fault report", "",
    "| Scenario | Engine | Exact jobs | Duplicates | Queue quiescent | At-least-once | Idempotent effect | Strict execution |",
    "| --- | --- | ---: | ---: | --- | --- | --- | --- |",
]
for result in results:
    gates = result["gates"]
    lines.append(
        f"| {result['scenario']} | {result['engine']} | {result['observed_job_ids']}/{result['expected_jobs']} "
        f"| {result['duplicate_completions']} | {'yes' if result['queue_quiescent'] else 'NO'} "
        f"| {'pass' if gates['at_least_once']['passed'] else 'FAIL'} "
        f"| {'pass' if gates['idempotent_effect']['passed'] else 'FAIL'} "
        f"| {'pass' if gates['strict_execution']['passed'] else 'FAIL'} |"
    )
lines.extend([
    "",
    "The campaign exit gate requires at-least-once recovery and the fixture-local idempotent-effect gate. ",
    "Strict execution is reported separately because retries are legal under at-least-once delivery.",
    "The SQLite ledger is not atomic with queue acknowledgement or external side effects.",
    "Disk-full remains a separate, unimplemented qualification gate.",
])
(root / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
PY

write_manifest "$OUTPUT_DIRECTORY"
trap - EXIT INT TERM
printf '\nInfrastructure fault artifacts: %s\n' "$OUTPUT_DIRECTORY"
printf 'Report: %s/report.md\n' "$OUTPUT_DIRECTORY"
exit "$suite_status"

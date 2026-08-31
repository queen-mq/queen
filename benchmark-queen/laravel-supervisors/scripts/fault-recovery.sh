#!/usr/bin/env bash

set -Eeuo pipefail
IFS=$'\n\t'

SCRIPT_DIR="$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
BENCH_DIR="$(CDPATH='' cd -- "${SCRIPT_DIR}/.." && pwd)"
REPOSITORY_ROOT="$(CDPATH='' cd -- "${BENCH_DIR}/../.." && pwd)"
COMPOSE_FILE="${BENCH_DIR}/compose.yml"
APP_IMAGE="queen-laravel-supervisor-bench:local"
BROKER_IMAGE="queen-laravel-supervisor-broker:local"

ENGINES_CSV="horizon,queen-php,queen-rust"
FAULT_SCENARIO="worker-sigkill"
JOBS=24
WORKERS=2
SLEEP_MS=2000
CPU_ITERATIONS=0
JOB_TRIES=2
QUEEN_PREFETCH=1
QUEEN_ACK_BATCH=1
WORKER_TIMEOUT=10
RETRY_AFTER=12
KILL_DELAY_MS=100
WORKER_READY_TIMEOUT=60
TARGET_ACTIVITY_TIMEOUT=60
RESPAWN_TIMEOUT=30
COMPLETION_TIMEOUT=120
BUILD_IMAGES=1
DRY_RUN=0
ALLOW_LEASE_RISK=0
OUTPUT_DIRECTORY=""

ACTIVE_PROJECT=""
ACTIVE_ENGINE=""
ACTIVE_VOLUME=""
ACTIVE_LANE_DIRECTORY=""
ACTIVE_OVERRIDE_FILE=""
ACTIVE_RUN_ID=""

usage() {
    cat <<'EOF'
Usage: scripts/fault-recovery.sh --output DIRECTORY [options]

Inject one process fault into an active fixed-pool lane, verify fencing,
supervisor recovery and job-set integrity, and retain raw evidence.

Options:
  --output DIRECTORY           Required, new or empty artifact directory
  --engines CSV                Subset of horizon,queen-php,queen-rust
  --scenario NAME              worker-sigkill (default) or renewal-helper-sigkill
  --jobs N                     Jobs per lane (default: 24)
  --workers N                  Fixed workers per lane (default: 2)
  --sleep-ms N                 Runtime of every job (default: 2000)
  --cpu-iterations N           SHA-256 rounds per job (default: 0)
  --job-tries N                Attempts allowed after the crash (default: 2)
  --queen-prefetch N           Queen deliveries claimed per pop (default: 1)
  --queen-ack-batch N          Queen ACKs flushed together; <= prefetch (default: 1)
  --worker-timeout SECONDS     Laravel worker timeout (default: 10)
  --retry-after SECONDS        Queue visibility timeout (default: 12)
  --kill-delay-ms N            Delay after target's proof-of-work (default: 100)
  --respawn-timeout SECONDS    Maximum wait for full pool recovery (default: 30)
  --completion-timeout SEC     Maximum wait for all unique jobs (default: 120)
  --allow-lease-risk           Retain an unsafe protocol; current supervisors may reject it
  --no-build                   Reuse local benchmark images
  --dry-run                    Validate and write the planned protocol only
  -h, --help                   Show this help

The test fixes BENCH_PROFILE=fixed. Use `--queen-prefetch` and
`--queen-ack-batch` to test either the synchronous-ACK production profile or
an explicitly labelled deferred-ACK candidate.
Horizon's equivalent worker child is `artisan horizon:work`; Queen's child is
`artisan queue:work`. The renewal-helper scenario requires Queen-only engines
and prefetch greater than one. Master/backend/network/storage faults are kept
as separate campaign classes because their recovery and durability gates are
not equivalent to a child-process replacement.
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

while [ "$#" -gt 0 ]; do
    case "$1" in
        --output) OUTPUT_DIRECTORY="${2:?--output requires a value}"; shift 2 ;;
        --engines) ENGINES_CSV="${2:?--engines requires a value}"; shift 2 ;;
        --scenario) FAULT_SCENARIO="${2:?--scenario requires a value}"; shift 2 ;;
        --jobs) JOBS="${2:?--jobs requires a value}"; shift 2 ;;
        --workers) WORKERS="${2:?--workers requires a value}"; shift 2 ;;
        --sleep-ms) SLEEP_MS="${2:?--sleep-ms requires a value}"; shift 2 ;;
        --cpu-iterations) CPU_ITERATIONS="${2:?--cpu-iterations requires a value}"; shift 2 ;;
        --job-tries) JOB_TRIES="${2:?--job-tries requires a value}"; shift 2 ;;
        --queen-prefetch) QUEEN_PREFETCH="${2:?--queen-prefetch requires a value}"; shift 2 ;;
        --queen-ack-batch) QUEEN_ACK_BATCH="${2:?--queen-ack-batch requires a value}"; shift 2 ;;
        --worker-timeout) WORKER_TIMEOUT="${2:?--worker-timeout requires a value}"; shift 2 ;;
        --retry-after) RETRY_AFTER="${2:?--retry-after requires a value}"; shift 2 ;;
        --kill-delay-ms) KILL_DELAY_MS="${2:?--kill-delay-ms requires a value}"; shift 2 ;;
        --respawn-timeout) RESPAWN_TIMEOUT="${2:?--respawn-timeout requires a value}"; shift 2 ;;
        --completion-timeout) COMPLETION_TIMEOUT="${2:?--completion-timeout requires a value}"; shift 2 ;;
        --allow-lease-risk) ALLOW_LEASE_RISK=1; shift ;;
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
require_positive_int "--queen-prefetch" "$QUEEN_PREFETCH"
require_positive_int "--queen-ack-batch" "$QUEEN_ACK_BATCH"
require_positive_int "--worker-timeout" "$WORKER_TIMEOUT"
require_positive_int "--retry-after" "$RETRY_AFTER"
require_uint "--kill-delay-ms" "$KILL_DELAY_MS"
require_positive_int "--respawn-timeout" "$RESPAWN_TIMEOUT"
require_positive_int "--completion-timeout" "$COMPLETION_TIMEOUT"
[ "$JOB_TRIES" -ge 2 ] || die "--job-tries must be at least 2 for crash recovery"
[ "$QUEEN_PREFETCH" -le 1000 ] || die "--queen-prefetch must be at most 1000"
[ "$QUEEN_ACK_BATCH" -le "$QUEEN_PREFETCH" ] || die "--queen-ack-batch must not exceed --queen-prefetch"
[ "$RETRY_AFTER" -gt "$WORKER_TIMEOUT" ] || die "--retry-after must exceed --worker-timeout"
[ "$KILL_DELAY_MS" -lt "$SLEEP_MS" ] || die "--kill-delay-ms must be shorter than --sleep-ms"
[ "$JOBS" -ge $(( WORKERS * 4 )) ] || die "--jobs must be at least four times --workers to preserve backlog"
case "$FAULT_SCENARIO" in
    worker-sigkill|renewal-helper-sigkill) ;;
    *) die "--scenario must be worker-sigkill or renewal-helper-sigkill" ;;
esac
LEASE_RENEWAL=false
if [ "$QUEEN_PREFETCH" -gt 1 ]; then
    LEASE_RENEWAL=true
fi

OLD_IFS="$IFS"
IFS=',' read -r -a ENGINES <<EOF
${ENGINES_CSV}
EOF
IFS="$OLD_IFS"
[ "${#ENGINES[@]}" -gt 0 ] || die "at least one engine is required"
contains_queen=0
for engine in "${ENGINES[@]}"; do
    case "$engine" in
        horizon) ;;
        queen-php|queen-rust) contains_queen=1 ;;
        *) die "unknown engine in --engines: $engine" ;;
    esac
done
if [ "$FAULT_SCENARIO" = renewal-helper-sigkill ]; then
    [ "$contains_queen" -eq 1 ] || die "renewal-helper-sigkill requires at least one Queen engine"
    [ "$QUEEN_PREFETCH" -gt 1 ] || die "renewal-helper-sigkill requires --queen-prefetch greater than one"
    for engine in "${ENGINES[@]}"; do
        [ "$engine" != horizon ] || die "renewal-helper-sigkill does not apply to Horizon"
    done
fi

# Every prefetched job starts under the same checkout window, while Laravel
# handles the local buffer serially. The configured sleep is therefore a hard
# lower bound for the time needed to drain one prefetch. Reject a lease that is
# already too short before CPU work and framework overhead are even counted.
minimum_prefetch_ms=$(( QUEEN_PREFETCH * SLEEP_MS ))
retry_after_ms=$(( RETRY_AFTER * 1000 ))
if [ "$contains_queen" -eq 1 ] && [ "$minimum_prefetch_ms" -ge "$retry_after_ms" ] && [ "$ALLOW_LEASE_RISK" -eq 0 ]; then
    die "Queen prefetch needs at least ${minimum_prefetch_ms}ms for configured sleeps, but --retry-after is ${retry_after_ms}ms; reduce --queen-prefetch, increase --retry-after, or use --allow-lease-risk to retain an intentional negative protocol"
fi

mkdir -p "$OUTPUT_DIRECTORY"
OUTPUT_DIRECTORY="$(CDPATH='' cd -- "$OUTPUT_DIRECTORY" && pwd)"
if [ -n "$(find "$OUTPUT_DIRECTORY" -mindepth 1 -maxdepth 1 -print -quit)" ]; then
    die "--output must be empty: $OUTPUT_DIRECTORY"
fi

campaign_stamp="$(date -u +%Y%m%dT%H%M%SZ)"
git_short="$(git -C "$REPOSITORY_ROOT" rev-parse --short=10 HEAD)"
campaign_token="$$"
campaign_id="fault-${campaign_stamp}-${git_short}-${campaign_token}"

write_protocol_metadata() {
    python3 - "$OUTPUT_DIRECTORY" "$campaign_id" "$REPOSITORY_ROOT" "$ENGINES_CSV" \
        "$FAULT_SCENARIO" \
        "$JOBS" "$WORKERS" "$SLEEP_MS" "$CPU_ITERATIONS" "$JOB_TRIES" \
        "$QUEEN_PREFETCH" "$QUEEN_ACK_BATCH" "$WORKER_TIMEOUT" "$RETRY_AFTER" "$KILL_DELAY_MS" "$RESPAWN_TIMEOUT" \
        "$COMPLETION_TIMEOUT" "$BUILD_IMAGES" "$DRY_RUN" "$ALLOW_LEASE_RISK" <<'PY'
import datetime as dt
import json
import platform
import subprocess
import sys
from pathlib import Path

(
    output, campaign_id, repository, engines, fault_scenario, jobs, workers, sleep_ms,
    cpu_iterations, job_tries, queen_prefetch, queen_ack_batch, worker_timeout, retry_after, kill_delay_ms,
    respawn_timeout, completion_timeout, build_images, dry_run, allow_lease_risk,
) = sys.argv[1:]

def command(*args: str) -> str:
    return subprocess.check_output(args, text=True, stderr=subprocess.DEVNULL).strip()

metadata = {
    "schema": "queen.laravel-supervisors.fault-recovery/v1",
    "qualification": "diagnostic_smoke",
    "campaign_id": campaign_id,
    "created_at": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
    "git": {
        "commit": command("git", "-C", repository, "rev-parse", "HEAD"),
        "branch": command("git", "-C", repository, "branch", "--show-current"),
        "dirty": bool(command("git", "-C", repository, "status", "--porcelain")),
    },
    "host": {"platform": platform.platform(), "machine": platform.machine()},
    "settings": {
        "profile": "fixed",
        "fault_scenario": fault_scenario,
        "engines": engines.split(","),
        "jobs": int(jobs),
        "workers": int(workers),
        "sleep_ms": int(sleep_ms),
        "cpu_iterations": int(cpu_iterations),
        "job_tries": int(job_tries),
        "worker_timeout_seconds": int(worker_timeout),
        "retry_after_seconds": int(retry_after),
        "kill_delay_ms": int(kill_delay_ms),
        "respawn_timeout_seconds": int(respawn_timeout),
        "completion_timeout_seconds": int(completion_timeout),
        "queen_prefetch": int(queen_prefetch),
        "queen_ack_batch": int(queen_ack_batch),
        "ledger_mode": "durable",
        "queues": ["benchmark"],
        "bench_queues_csv": "",
        "failed_driver": "null",
        "lease_renewal": int(queen_prefetch) > 1,
        "allow_lease_risk": allow_lease_risk == "1",
        "dispatch_mode": "single",
        "build_images": build_images == "1",
        "dry_run": dry_run == "1",
    },
    "method": {
        "fault": (
            "SIGKILL exactly one active queue worker"
            if fault_scenario == "worker-sigkill"
            else "SIGKILL one active lease-renewal helper and require its worker watchdog fence"
        ),
        "target_qualification": (
            "worker PID must have written a completion for this run; injection follows "
            "after kill_delay_ms while a long-job backlog remains"
        ),
        "horizon_worker_command": "artisan horizon:work",
        "queen_worker_command": "artisan queue:work",
        "renewal_helper_command": "LeaseRenewalWorker::main",
        "delivery_semantics": "at-least-once",
        "strict_duplicate_observation": "reported separately from at-least-once recovery",
        "effect_witness": (
            "fixture-local idempotent SQLite effects, durable before completion and queue ACK; "
            "conservation and duplicate-side-effect gates are reported separately"
        ),
        "sterilized_environment": {
            "BENCH_QUEUES": "",
            "BENCH_FAILED_DRIVER": "null",
            "BENCH_LEASE_RENEWAL": "true" if int(queen_prefetch) > 1 else "false",
            "BENCH_LEASE_RENEWAL_INTERVAL": "",
        },
    },
    "known_limits": [
        "The fixture disables durable failed-job storage; failure signals are therefore log-derived.",
        "The worker writes completion at the end of handle(), not a start event for its next job. "
        "A qualified worker plus backlog, a long sleep, and a short injection delay are strong "
        "but not nanosecond-exact proof that SIGKILL landed inside user code.",
        "This smoke test measures one crash per fresh backend; estimate probabilities with repeated runs.",
        "The lease guard covers the configured sleep floor only; CPU work and framework overhead still "
        "require additional retry_after margin.",
        "allow_lease_risk bypasses only this harness sleep-floor guard. Current Queen supervisors "
        "always require lease renewal when prefetch is greater than one.",
        "The effect ledger is a fixture-local idempotent transactional witness. It is not atomic with the "
        "queue ACK or an arbitrary external side effect and cannot establish exactly-once delivery.",
    ],
}
Path(output, "metadata.json").write_text(
    json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8"
)
PY
}

write_protocol_metadata

if [ "$DRY_RUN" -eq 1 ]; then
    printf 'Dry-run protocol written to %s/metadata.json\n' "$OUTPUT_DIRECTORY"
    exit 0
fi

require_command docker
docker info >/dev/null 2>&1 || die "Docker daemon is unavailable"

image_id() {
    docker image inspect "$1" --format '{{.Id}}' 2>/dev/null || true
}

if [ "$BUILD_IMAGES" -eq 1 ]; then
    printf 'Building fault-test application image...\n'
    docker compose --file "$COMPOSE_FILE" --profile tools build producer
    if [ "$contains_queen" -eq 1 ]; then
        printf 'Building Queen broker image...\n'
        docker compose --file "$COMPOSE_FILE" --profile queen-php build broker
    fi
else
    [ -n "$(image_id "$APP_IMAGE")" ] || die "missing image: $APP_IMAGE"
    if [ "$contains_queen" -eq 1 ]; then
        [ -n "$(image_id "$BROKER_IMAGE")" ] || die "missing image: $BROKER_IMAGE"
    fi
fi

docker info --format '{{json .}}' \
    | python3 -m json.tool >"${OUTPUT_DIRECTORY}/docker-info.json"
docker ps --no-trunc --format '{{.ID}}\t{{.Names}}\t{{.Image}}\t{{.Status}}' \
    >"${OUTPUT_DIRECTORY}/preexisting-containers.tsv"
if [ "$contains_queen" -eq 1 ]; then
    docker image inspect "$APP_IMAGE" "$BROKER_IMAGE" >"${OUTPUT_DIRECTORY}/images.json"
else
    docker image inspect "$APP_IMAGE" >"${OUTPUT_DIRECTORY}/images.json"
fi

compose_active() {
    local -a compose_files=(--file "$COMPOSE_FILE")
    if [ -n "$ACTIVE_OVERRIDE_FILE" ]; then
        compose_files+=(--file "$ACTIVE_OVERRIDE_FILE")
    fi
    docker compose \
        "${compose_files[@]}" \
        --project-name "$ACTIVE_PROJECT" \
        --profile "$ACTIVE_ENGINE" \
        --profile tools \
        "$@"
}

# Reached through the EXIT trap's diagnostic path.
# shellcheck disable=SC2317,SC2329
salvage_active_results() {
    [ -n "$ACTIVE_VOLUME" ] || return 0
    [ -n "$ACTIVE_RUN_ID" ] || return 0
    [ -n "$ACTIVE_LANE_DIRECTORY" ] || return 0
    docker volume inspect "$ACTIVE_VOLUME" >/dev/null 2>&1 || return 0
    # Merge again even when an earlier copy created the directory and then
    # failed part-way through; cp -a safely refreshes retained evidence.
    docker run --rm --user 0:0 \
        --mount "type=volume,src=${ACTIVE_VOLUME},dst=/from,readonly" \
        --mount "type=bind,src=${ACTIVE_LANE_DIRECTORY},dst=/to" \
        "$APP_IMAGE" sh -ceu '
            mkdir -p /to/results
            if [ -d "/from/$1" ]; then cp -a "/from/$1/." /to/results/; fi
        ' sh "$ACTIVE_RUN_ID" >/dev/null 2>&1 || true
}

container_exists() {
    docker container inspect "$1" >/dev/null 2>&1
}

capture_active_diagnostics() {
    [ -n "$ACTIVE_PROJECT" ] || return 0
    [ -n "$ACTIVE_LANE_DIRECTORY" ] || return 0
    mkdir -p "$ACTIVE_LANE_DIRECTORY"
    compose_active ps --all >"${ACTIVE_LANE_DIRECTORY}/compose-ps.txt" 2>&1 || true
    compose_active logs --no-color --timestamps >"${ACTIVE_LANE_DIRECTORY}/compose.log" 2>&1 || true
    active_ids="$(compose_active ps --all --quiet 2>/dev/null || true)"
    if [ -n "$active_ids" ]; then
        # Compose emits one id per line; the script IFS preserves each id.
        # shellcheck disable=SC2086
        docker inspect $active_ids >"${ACTIVE_LANE_DIRECTORY}/containers-final.json" 2>/dev/null || true
    fi
}

cleanup_active_lane() {
    if [ -n "$ACTIVE_PROJECT" ]; then
        compose_active down --volumes --remove-orphans --timeout 20 >/dev/null 2>&1 || true
    fi
    if [ -n "$ACTIVE_VOLUME" ] && docker volume inspect "$ACTIVE_VOLUME" >/dev/null 2>&1; then
        docker volume rm "$ACTIVE_VOLUME" >/dev/null 2>&1 || true
    fi
    ACTIVE_PROJECT=""
    ACTIVE_ENGINE=""
    ACTIVE_VOLUME=""
    ACTIVE_LANE_DIRECTORY=""
    ACTIVE_OVERRIDE_FILE=""
    ACTIVE_RUN_ID=""
}

# Invoked indirectly by the trap registered below.
# shellcheck disable=SC2317,SC2329
on_exit() {
    exit_status=$?
    trap - EXIT INT TERM
    if [ "$exit_status" -ne 0 ]; then
        capture_active_diagnostics
        salvage_active_results
        if [ -n "$ACTIVE_LANE_DIRECTORY" ]; then
            python3 - "$ACTIVE_LANE_DIRECTORY" "$exit_status" <<'PY'
import datetime as dt
import json
import sys
from pathlib import Path

directory, status = sys.argv[1:]
payload = {
    "schema": "queen.laravel-supervisors.harness-error/v1",
    "exit_status": int(status),
    "captured_at": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
    "message": "The harness failed closed; inspect compose.log and partial artifacts.",
}
Path(directory, "harness-error.json").write_text(
    json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
)
PY
        fi
    fi
    cleanup_active_lane
    exit "$exit_status"
}
trap on_exit EXIT INT TERM

wait_for_health() {
    health_container="$1"
    health_timeout="$2"
    health_label="$3"
    health_deadline=$(( $(date +%s) + health_timeout ))
    while :; do
        health_status="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$health_container" 2>/dev/null || true)"
        case "$health_status" in
            healthy|running) return 0 ;;
            unhealthy|exited|dead) die "$health_label entered state: $health_status" ;;
        esac
        [ "$(date +%s)" -lt "$health_deadline" ] || die "timed out waiting for $health_label health"
        sleep 1
    done
}

monotonic_ns() {
    python3 -c 'import time; print(time.monotonic_ns())'
}

utc_now() {
    python3 -c 'import datetime as d; print(d.datetime.now(d.timezone.utc).isoformat().replace("+00:00", "Z"))'
}

append_timeline() {
    timeline_file="$1"
    timeline_event="$2"
    timeline_detail="$3"
    python3 - "$timeline_file" "$timeline_event" "$timeline_detail" <<'PY'
import datetime as dt
import json
import sys
import time

path, event, detail = sys.argv[1:]
record = {
    "wall_time": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
    "host_monotonic_ns": time.monotonic_ns(),
    "event": event,
    "detail": detail,
}
with open(path, "a", encoding="utf-8") as stream:
    stream.write(json.dumps(record, sort_keys=True) + "\n")
PY
}

producer() {
    compose_active run --rm --no-deps --no-TTY producer "$@"
}

dispatch_with_retries() {
    dispatch_run_id="$1"
    dispatch_connection="$2"
    # This is PHP source by design; shell expansion would corrupt PHP variables.
    # shellcheck disable=SC2016
    producer php -r '
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
$queue = $argv[7];
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
$completedAt = hrtime(true);
$manifest = [
    "run_id" => $runId,
    "jobs" => $jobs,
    "connection" => $connection,
    "queue" => $queue,
    "dispatch_mode" => "single",
    "dispatch_batch_size" => 1,
    "job_tries" => $tries,
    "sleep_ms" => $sleepMs,
    "cpu_iterations" => $cpuIterations,
    "ledger_mode" => $ledger->mode(),
    "ledger_semantics" => "fixture-local idempotent effect keyed by run_id+job_id; not queue-ACK atomic",
    "dispatch_started_ns" => $startedAt,
    "dispatch_finished_ns" => $completedAt,
    "dispatch_duration_ns" => $completedAt - $startedAt,
];
$manifest["metadata_path"] = $sink->writeDispatchMetadata($runId, $manifest);
echo json_encode($manifest, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES), PHP_EOL;
' "$dispatch_run_id" "$JOBS" "$SLEEP_MS" "$CPU_ITERATIONS" "$JOB_TRIES" \
        "$dispatch_connection" benchmark
}

capture_queue_state() {
    state_connection="$1"
    state_run_id="$2"
    producer php artisan bench:queue-state --no-ansi \
        --run-id="$state_run_id" \
        --connection="$state_connection" \
        --queue=benchmark \
        --wait="$COMPLETION_TIMEOUT" \
        --poll-ms=100 \
        --settle-ms=1000
}

worker_rows() {
    rows_container="$1"
    rows_needle="$2"
    docker exec "$rows_container" ps -eo pid=,ppid=,args= \
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

wait_for_worker_pool() {
    pool_container="$1"
    pool_needle="$2"
    pool_output="$3"
    pool_deadline=$(( $(date +%s) + WORKER_READY_TIMEOUT ))
    while :; do
        worker_rows "$pool_container" "$pool_needle" >"$pool_output"
        pool_count="$(wc -l <"$pool_output" | tr -d '[:space:]')"
        if [ "$pool_count" -eq "$WORKERS" ]; then
            return 0
        fi
        [ "$(date +%s)" -lt "$pool_deadline" ] || die "worker pool did not reach $WORKERS processes"
        sleep 0.2
    done
}

select_active_target() {
    target_container="$1"
    target_run_id="$2"
    target_workers_file="$3"
    target_deadline=$(( $(date +%s) + TARGET_ACTIVITY_TIMEOUT ))
    while :; do
        while read -r candidate_pid candidate_ppid candidate_args; do
            [ -n "${candidate_pid:-}" ] || continue
            candidate_event="/results/${target_run_id}/events/worker-${candidate_pid}.jsonl"
            if docker exec "$target_container" test -s "$candidate_event" >/dev/null 2>&1; then
                printf '%s\t%s\t%s\n' "$candidate_pid" "$candidate_ppid" "$candidate_args"
                return 0
            fi
        done <"$target_workers_file"
        [ "$(date +%s)" -lt "$target_deadline" ] || return 1
        sleep 0.1
    done
}

select_helper_target() {
    helper_container="$1"
    helper_worker_pid="$2"
    helper_deadline=$(( $(date +%s) + TARGET_ACTIVITY_TIMEOUT ))
    while :; do
        helper_rows="$(docker exec "$helper_container" ps -eo pid=,ppid=,args= \
            | awk -v parent="$helper_worker_pid" '
                $1 ~ /^[0-9]+$/ && $2 == parent && index($0, "LeaseRenewalWorker::main") > 0 {
                    pid = $1
                    ppid = $2
                    $1 = ""
                    $2 = ""
                    sub(/^[[:space:]]+/, "")
                    printf "%s\t%s\t%s\n", pid, ppid, $0
                }
            ')"
        helper_count="$(printf '%s\n' "$helper_rows" | awk 'NF { count += 1 } END { print count + 0 }')"
        if [ "$helper_count" -eq 1 ]; then
            printf '%s\n' "$helper_rows"
            return 0
        fi
        [ "$helper_count" -le 1 ] || die "worker $helper_worker_pid owns more than one renewal helper"
        [ "$(date +%s)" -lt "$helper_deadline" ] || return 1
        sleep 0.1
    done
}

write_lane_summary() {
    summary_lane="$1"
    summary_engine="$2"
    summary_run_id="$3"
    summary_app_id="$4"
    summary_scenario="$5"
    summary_fenced_worker_pid="$6"
    summary_target_pid="$7"
    summary_target_ppid="$8"
    summary_target_args="$9"
    shift 9
    summary_parent_args="$1"
    summary_replacement_pid="$2"
    summary_kill_host_before="$3"
    summary_kill_host_after="$4"
    summary_kill_container_before="$5"
    summary_kill_container_after="$6"
    summary_kill_wall="$7"
    summary_respawn_host="$8"
    summary_completion_status="$9"
    summary_ledger_status="${10}"

    python3 - "$summary_lane" "$summary_engine" "$summary_run_id" "$summary_app_id" \
        "$summary_scenario" "$summary_fenced_worker_pid" \
        "$JOBS" "$JOB_TRIES" "$QUEEN_PREFETCH" "$QUEEN_ACK_BATCH" "$WORKERS" "$SLEEP_MS" "$KILL_DELAY_MS" \
        "$summary_target_pid" "$summary_target_ppid" "$summary_target_args" \
        "$summary_parent_args" "$summary_replacement_pid" "$summary_kill_host_before" \
        "$summary_kill_host_after" "$summary_kill_container_before" \
        "$summary_kill_container_after" "$summary_kill_wall" "$summary_respawn_host" \
        "$summary_completion_status" "$summary_ledger_status" <<'PY'
import json
import re
import sys
from pathlib import Path

(
    lane_raw, engine, run_id, app_id, scenario, fenced_worker_pid_raw,
    jobs_raw, job_tries_raw, queen_prefetch_raw,
    queen_ack_batch_raw, workers_raw,
    sleep_ms_raw, kill_delay_ms_raw, target_pid_raw, target_ppid_raw, target_args,
    parent_args, replacement_pid_raw, kill_host_before_raw,
    kill_host_after_raw, kill_container_before_raw, kill_container_after_raw,
    kill_wall, respawn_host_raw, completion_status_raw, ledger_status_raw,
) = sys.argv[1:]
lane = Path(lane_raw)
jobs = int(jobs_raw)
job_tries = int(job_tries_raw)

def read_json(path: Path, default):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return default

def keyed_containers(path: Path):
    values = read_json(path, [])
    return {
        item.get("Id"): item
        for item in values
        if isinstance(item, dict) and isinstance(item.get("Id"), str)
    }

result = read_json(lane / "result-check.json", {})
queue_state = read_json(lane / "queue-final.json", {})
effect_ledger = read_json(lane / "ledger-check.json", {})
before = keyed_containers(lane / "containers-before.json")
after = keyed_containers(lane / "containers-final.json")

container_checks = []
for container_id, initial in before.items():
    final = after.get(container_id, {})
    name = str(initial.get("Name", "unknown")).lstrip("/")
    initial_restarts = int(initial.get("RestartCount", 0) or 0)
    final_restarts = int(final.get("RestartCount", initial_restarts) or 0)
    state = final.get("State", {}) if isinstance(final.get("State"), dict) else {}
    container_checks.append({
        "name": name,
        "id": container_id,
        "restart_count_before": initial_restarts,
        "restart_count_after": final_restarts,
        "restart_delta": final_restarts - initial_restarts,
        "oom_killed": bool(state.get("OOMKilled", False)),
        "status_after": state.get("Status"),
        "running_after": bool(state.get("Running", False)),
    })

failure_pattern = re.compile(
    r"MaxAttemptsExceededException|attempted too many times|JobFailed|"
    r"(?:ERROR|CRITICAL|ALERT|EMERGENCY)[\s\"':\[]|failed (?:job|to process)",
    re.IGNORECASE,
)
try:
    app_lines = (lane / "app.log").read_text(encoding="utf-8", errors="replace").splitlines()
except FileNotFoundError:
    app_lines = []
failure_lines = [line for line in app_lines if failure_pattern.search(line)]
(lane / "failure-signals.log").write_text(
    "\n".join(failure_lines) + ("\n" if failure_lines else ""), encoding="utf-8"
)

records = []
for path in sorted((lane / "results" / "events").glob("worker-*.jsonl")):
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if record.get("run_id") == run_id:
            records.append(record)

kill_container_before = int(kill_container_before_raw)
kill_container_after = int(kill_container_after_raw)
retry_completions = [
    int(record["completed_at_ns"])
    for record in records
    if isinstance(record.get("completed_at_ns"), int)
    and not isinstance(record.get("completed_at_ns"), bool)
    and record["completed_at_ns"] > kill_container_after
    and isinstance(record.get("attempt"), int)
    and not isinstance(record.get("attempt"), bool)
    and record["attempt"] > 1
]
first_retry_completion = min(retry_completions) if retry_completions else None
recovery_bounds = None
if first_retry_completion is not None:
    recovery_bounds = {
        "lower_ms": max(0.0, (first_retry_completion - kill_container_after) / 1_000_000),
        "upper_ms": max(0.0, (first_retry_completion - kill_container_before) / 1_000_000),
        "clock_domain": "Docker VM CLOCK_MONOTONIC",
    }

unique_completed = int(result.get("unique_completed", 0) or 0)
duplicates = int(result.get("duplicates", 0) or 0)
expected_ids = {f"{index:09d}" for index in range(jobs)}
observed_ids = {
    record["job_id"]
    for record in records
    if isinstance(record.get("job_id"), str)
    and isinstance(record.get("completed_at_ns"), int)
    and not isinstance(record.get("completed_at_ns"), bool)
}
missing_ids = sorted(expected_ids - observed_ids)
unexpected_ids = sorted(observed_ids - expected_ids)
missing = len(missing_ids)
exact_job_set = not missing_ids and not unexpected_ids and observed_ids == expected_ids
attempt_values = [record.get("attempt") for record in records]
attempts_within_bounds = bool(attempt_values) and all(
    isinstance(attempt, int)
    and not isinstance(attempt, bool)
    and 1 <= attempt <= job_tries
    for attempt in attempt_values
)
replacement_pid = int(replacement_pid_raw) if replacement_pid_raw else None
respawn_host = int(respawn_host_raw) if respawn_host_raw else None
kill_host_before = int(kill_host_before_raw)
kill_host_after = int(kill_host_after_raw)
respawn_latency_ms = None
if respawn_host is not None:
    respawn_latency_ms = max(0.0, (respawn_host - kill_host_after) / 1_000_000)

container_integrity = bool(container_checks) and all(
    item["restart_delta"] == 0 and not item["oom_killed"] and item["running_after"]
    for item in container_checks
)
respawned = replacement_pid is not None
complete = unique_completed == jobs and bool(result.get("complete", False))
queue_values = queue_state.get("state", {})
queue_size = queue_values.get("size") if isinstance(queue_values, dict) else None
queue_reconciled = (
    isinstance(queue_size, int)
    and not isinstance(queue_size, bool)
    and queue_size == 0
    and queue_state.get("quiescent") is True
    and queue_state.get("timed_out") is False
    and queue_state.get("probe_errors") == []
)
inflight_recovery_observed = bool(retry_completions)
target_proved_work = (
    (lane / "target-events-before-kill.jsonl").is_file()
    and (lane / "target-events-before-kill.jsonl").stat().st_size > 0
)
at_least_once_pass = (
    respawned
    and target_proved_work
    and inflight_recovery_observed
    and complete
    and exact_job_set
    and attempts_within_bounds
    and int(completion_status_raw) == 0
    and queue_reconciled
    and container_integrity
    and int(ledger_status_raw) == 0
    and effect_ledger.get("selected_gate_passed") is True
    and effect_ledger.get("conservation_pass") is True
    and effect_ledger.get("idempotent_effect_pass") is True
    and effect_ledger.get("attempt_integrity_pass") is True
)
strict_observation_pass = (
    at_least_once_pass
    and duplicates == 0
    and not failure_lines
    and effect_ledger.get("no_duplicate_side_effects_pass") is True
    and effect_ledger.get("strict_execution_pass") is True
)

summary = {
    "schema": "queen.laravel-supervisors.fault-result/v1",
    "engine": engine,
    "run_id": run_id,
    "workload": {
        "jobs": jobs,
        "workers": int(workers_raw),
        "sleep_ms": int(sleep_ms_raw),
        "kill_delay_ms": int(kill_delay_ms_raw),
        "queen_prefetch": int(queen_prefetch_raw),
        "queen_ack_batch": int(queen_ack_batch_raw),
    },
    "fault": {
        "scenario": scenario,
        "signal": "SIGKILL",
        "target_role": "worker" if scenario == "worker-sigkill" else "renewal-helper",
        "fenced_worker_pid": int(fenced_worker_pid_raw),
        "target_pid": int(target_pid_raw),
        "target_ppid": int(target_ppid_raw),
        "target_args": target_args,
        "parent_args": parent_args,
        "target_is_container_init": int(target_pid_raw) == 1,
        "target_proved_work_before_kill": target_proved_work,
        "wall_time": kill_wall,
        "host_monotonic_interval_ns": [kill_host_before, kill_host_after],
        "container_monotonic_interval_ns": [kill_container_before, kill_container_after],
    },
    "supervisor_recovery": {
        "replacement_pid": replacement_pid,
        "respawned": respawned,
        "kill_to_full_pool_ms": respawn_latency_ms,
    },
    "job_recovery": {
        "expected": jobs,
        "job_tries": job_tries,
        "unique_completed": unique_completed,
        "records": int(result.get("records", len(records)) or 0),
        "missing": missing,
        "missing_ids": missing_ids,
        "unexpected": len(unexpected_ids),
        "unexpected_ids": unexpected_ids,
        "exact_expected_job_set": exact_job_set,
        "duplicates": duplicates,
        "max_attempt": result.get("max_attempt"),
        "retried_completion_records": len(retry_completions),
        "inflight_recovery_observed": inflight_recovery_observed,
        "retry_completions_after_fault": len(retry_completions),
        "attempts_within_bounds": attempts_within_bounds,
        "kill_to_first_retry_completion_bounds": recovery_bounds,
        "command_exit_status": int(completion_status_raw),
        "durable_failure_count": None,
        "durable_failure_count_reason": "fixture queue.failed.driver is null",
        "failure_signal_count": len(failure_lines),
        "final_queue_state": queue_state,
        "effect_ledger": effect_ledger,
    },
    "containers": container_checks,
    "checks": {
        "worker_respawned": respawned,
        "target_was_bound_to_active_work": target_proved_work,
        "helper_death_fenced_worker": respawned if scenario == "renewal-helper-sigkill" else None,
        "inflight_job_retried_and_completed": inflight_recovery_observed,
        "all_unique_jobs_completed": complete,
        "no_missing_jobs": missing == 0,
        "no_unexpected_jobs": not unexpected_ids,
        "exact_expected_job_set": exact_job_set,
        "attempts_within_bounds": attempts_within_bounds,
        "no_duplicate_observed": duplicates == 0,
        "no_failure_signal_observed": not failure_lines,
        "queue_reconciled_to_zero": queue_reconciled,
        "no_container_restart_or_oom": container_integrity,
        "effect_conservation_pass": effect_ledger.get("conservation_pass") is True,
        "attempt_ledger_integrity_pass": effect_ledger.get("attempt_integrity_pass") is True,
        "idempotent_effect_pass": effect_ledger.get("idempotent_effect_pass") is True,
        "strict_execution_pass": effect_ledger.get("strict_execution_pass") is True,
        "no_duplicate_side_effect_observed": (
            effect_ledger.get("no_duplicate_side_effects_pass") is True
        ),
        "at_least_once_pass": at_least_once_pass,
        "strict_observation_pass": strict_observation_pass,
    },
    "interpretation": (
        "Completion duplicates and idempotency dedup hits are reported but do not invalidate "
        "at-least-once conservation. strict_observation_pass additionally requires a single "
        "execution attempt per job and no engine-specific failure signal matched in the application log. "
        "The ledger is not an exactly-once claim for external effects."
    ),
}
(lane / "fault-result.json").write_text(
    json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
)
PY
}

run_lane() {
    lane_engine="$1"
    lane_index="$2"
    ACTIVE_ENGINE="$lane_engine"
    project_suffix="$(printf '%s-%s-%02d-%s' "$campaign_stamp" "$campaign_token" "$lane_index" "$lane_engine" \
        | tr '[:upper:]' '[:lower:]' | tr -cd 'a-z0-9-')"
    ACTIVE_PROJECT="qlb-fault-${project_suffix}"
    ACTIVE_VOLUME="qlb-fault-results-${project_suffix}"
    ACTIVE_LANE_DIRECTORY="${OUTPUT_DIRECTORY}/${lane_engine}"
    mkdir -p "$ACTIVE_LANE_DIRECTORY"
    timeline="${ACTIVE_LANE_DIRECTORY}/timeline.jsonl"
    run_id="fault-${lane_engine}-${campaign_stamp}-${campaign_token}"
    ACTIVE_RUN_ID="$run_id"
    ACTIVE_OVERRIDE_FILE="${ACTIVE_LANE_DIRECTORY}/runtime-override.json"

    export BENCH_RESULTS_VOLUME="$ACTIVE_VOLUME"
    export BENCH_PROFILE=fixed
    export BENCH_QUEUE=benchmark
    export BENCH_GROUP=benchmark
    export BENCH_WORKERS="$WORKERS"
    export BENCH_MIN_WORKERS="$WORKERS"
    export BENCH_MAX_WORKERS="$WORKERS"
    export BENCH_TIMEOUT="$WORKER_TIMEOUT"
    export BENCH_RETRY_AFTER="$RETRY_AFTER"
    export BENCH_DISPATCH_MODE=single
    export BENCH_LEDGER_MODE=durable
    # The fault protocol is deliberately single-queue and uses no failed-job
    # persistence. Multi-message prefetch includes the production renewal
    # fence. Never inherit unrelated feature-probe toggles from the caller.
    export BENCH_QUEUES=''
    export BENCH_FAILED_DRIVER='null'
    export BENCH_LEASE_RENEWAL="$LEASE_RENEWAL"
    export BENCH_LEASE_RENEWAL_INTERVAL=''
    export QUEEN_PREFETCH="$QUEEN_PREFETCH"
    export QUEEN_ACK_BATCH="$QUEEN_ACK_BATCH"
    export QUEEN_BULK_BATCH=100
    export QUEEN_PARTITIONS=64
    export QUEEN_POP_FUSION=0
    if [ "$lane_engine" = horizon ]; then
        export BENCH_CONNECTION=redis
    else
        export BENCH_CONNECTION=queen
    fi
    export BENCH_LANE="$lane_engine"
    export QUEUE_CONNECTION="$BENCH_CONNECTION"

    python3 - "$ACTIVE_OVERRIDE_FILE" "$lane_engine" "$WORKER_TIMEOUT" "$RETRY_AFTER" <<'PY'
import json
import sys
from pathlib import Path

path, engine, worker_timeout, retry_after = sys.argv[1:]
environment = {
    "BENCH_TIMEOUT": worker_timeout,
    "BENCH_RETRY_AFTER": retry_after,
}
override = {
    "services": {
        engine: {"environment": environment},
        "producer": {"environment": environment},
    }
}
Path(path).write_text(json.dumps(override, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY

    printf '\n[%02d] fault/recovery %s\n' "$lane_index" "$lane_engine"
    append_timeline "$timeline" lane_start "$lane_engine"
    docker volume create \
        --label "queen.benchmark.campaign=${campaign_id}" \
        --label "queen.benchmark.fault.engine=${lane_engine}" \
        --label "com.docker.compose.project=${ACTIVE_PROJECT}" \
        --label "com.docker.compose.volume=results" \
        "$ACTIVE_VOLUME" >/dev/null
    docker run --rm --user 0:0 \
        --mount "type=volume,src=${ACTIVE_VOLUME},dst=/results" \
        "$APP_IMAGE" sh -ceu 'chown 1000:1000 /results; chmod 0770 /results'

    compose_active config >"${ACTIVE_LANE_DIRECTORY}/compose-resolved.yml"
    compose_active up --detach --no-build "$lane_engine"
    app_id="$(compose_active ps --quiet "$lane_engine")"
    [ -n "$app_id" ] || die "unable to resolve $lane_engine container"
    wait_for_health "$app_id" 180 "$lane_engine"
    producer php artisan bench:config --no-ansi \
        >"${ACTIVE_LANE_DIRECTORY}/configuration.json"
    append_timeline "$timeline" supervisor_healthy "$app_id"

    initial_workers="${ACTIVE_LANE_DIRECTORY}/workers-before.tsv"
    if [ "$lane_engine" = horizon ]; then
        worker_needle="artisan horizon:work "
    else
        worker_needle="artisan queue:work "
    fi
    wait_for_worker_pool "$app_id" "$worker_needle" "$initial_workers"
    docker exec "$app_id" ps -eo pid=,ppid=,etimes=,lstart=,stat=,comm=,args= --forest \
        >"${ACTIVE_LANE_DIRECTORY}/process-tree-before.txt"
    append_timeline "$timeline" worker_pool_ready "$WORKERS"

    initial_ids="$(compose_active ps --all --quiet)"
    # shellcheck disable=SC2086
    docker inspect $initial_ids >"${ACTIVE_LANE_DIRECTORY}/containers-before.json"

    dispatch_with_retries "$run_id" "$BENCH_CONNECTION" \
        >"${ACTIVE_LANE_DIRECTORY}/dispatch.json" \
        2>"${ACTIVE_LANE_DIRECTORY}/dispatch.stderr.log"
    append_timeline "$timeline" dispatch_complete "$JOBS jobs"

    qualified_worker_row="$(select_active_target "$app_id" "$run_id" "$initial_workers" || true)"
    [ -n "$qualified_worker_row" ] || die "no qualified worker wrote a completion before injection"
    qualified_worker_pid="$(printf '%s\n' "$qualified_worker_row" | awk '{print $1}')"
    if [ "$FAULT_SCENARIO" = renewal-helper-sigkill ]; then
        target_row="$(select_helper_target "$app_id" "$qualified_worker_pid" || true)"
        [ -n "$target_row" ] \
            || die "qualified worker $qualified_worker_pid did not expose one renewal helper"
        target_role=renewal-helper
        target_needle='LeaseRenewalWorker::main'
        fenced_worker_pid="$qualified_worker_pid"
    else
        target_row="$qualified_worker_row"
        target_role=worker
        target_needle="$worker_needle"
        fenced_worker_pid="$qualified_worker_pid"
    fi
    target_pid="$(printf '%s\n' "$target_row" | awk '{print $1}')"
    target_ppid="$(printf '%s\n' "$target_row" | awk '{print $2}')"
    target_args="$(printf '%s\n' "$target_row" | awk '{$1=""; $2=""; sub(/^  */, ""); print}')"
    [ "$target_pid" -ne 1 ] || die "refusing to signal container init"
    [ "$target_ppid" -gt 1 ] || die "refusing worker with non-supervisor PPID $target_ppid"
    current_args="$(docker exec "$app_id" ps -p "$target_pid" -o args= | sed -e 's/^[[:space:]]*//')"
    case "$current_args" in
        *"$target_needle"*) ;;
        *) die "target PID $target_pid no longer matches $target_role command" ;;
    esac
    current_ppid="$(docker exec "$app_id" ps -p "$target_pid" -o ppid= | tr -d '[:space:]')"
    [ "$current_ppid" = "$target_ppid" ] || die "target PID $target_pid changed parent"
    parent_args="$(docker exec "$app_id" ps -p "$target_ppid" -o args= | sed -e 's/^[[:space:]]*//')"
    if [ "$FAULT_SCENARIO" = renewal-helper-sigkill ]; then
        case "$parent_args" in
            *"$worker_needle"*) ;;
            *) die "renewal helper parent $target_ppid no longer matches a queue worker" ;;
        esac
        [ "$target_ppid" = "$qualified_worker_pid" ] \
            || die "renewal helper parent changed before injection"
    else
        case "$parent_args" in
            *"$worker_needle"*) die "refusing nested worker target" ;;
        esac
    fi
    docker exec "$app_id" cat "/results/${run_id}/events/worker-${qualified_worker_pid}.jsonl" \
        >"${ACTIVE_LANE_DIRECTORY}/target-events-before-kill.jsonl"
    target_start_ticks="$(docker exec "$app_id" awk '{print $22}' "/proc/${target_pid}/stat")"
    python3 - "${ACTIVE_LANE_DIRECTORY}/target-identity.json" "$target_pid" "$target_ppid" \
        "$target_start_ticks" "$target_args" "$parent_args" "$target_role" \
        "$fenced_worker_pid" <<'PY'
import json
import sys
from pathlib import Path

path, pid, ppid, start_ticks, args, parent_args, role, fenced_worker_pid = sys.argv[1:]
Path(path).write_text(json.dumps({
    "pid": int(pid),
    "ppid": int(ppid),
    "proc_start_ticks": int(start_ticks),
    "args": args,
    "parent_args": parent_args,
    "role": role,
    "fenced_worker_pid": int(fenced_worker_pid),
}, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
    append_timeline "$timeline" target_qualified \
        "scenario=${FAULT_SCENARIO};role=${target_role};pid=${target_pid};fenced_worker=${fenced_worker_pid}"

    kill_delay_seconds="$(python3 -c 'import sys; print(int(sys.argv[1]) / 1000)' "$KILL_DELAY_MS")"
    sleep "$kill_delay_seconds"
    pre_kill_start_ticks="$(docker exec "$app_id" awk '{print $22}' "/proc/${target_pid}/stat")"
    [ "$pre_kill_start_ticks" = "$target_start_ticks" ] || die "target PID identity changed before SIGKILL"
    kill_wall="$(utc_now)"
    kill_host_before="$(monotonic_ns)"
    kill_container_before="$(docker exec "$app_id" python3 -c 'import time; print(time.monotonic_ns())')"
    docker exec "$app_id" kill -KILL "$target_pid"
    kill_container_after="$(docker exec "$app_id" python3 -c 'import time; print(time.monotonic_ns())')"
    kill_host_after="$(monotonic_ns)"
    append_timeline "$timeline" sigkill_sent "role=${target_role};pid=${target_pid}"

    respawn_deadline=$(( $(date +%s) + RESPAWN_TIMEOUT ))
    replacement_pid=""
    respawn_host=""
    current_workers="${ACTIVE_LANE_DIRECTORY}/workers-after-respawn.tsv"
    while :; do
        if container_exists "$app_id"; then
            app_running="$(docker inspect --format '{{.State.Running}}' "$app_id" 2>/dev/null || true)"
            if [ "$app_running" = true ]; then
                worker_rows "$app_id" "$worker_needle" >"${current_workers}.tmp"
                current_count="$(wc -l <"${current_workers}.tmp" | tr -d '[:space:]')"
                candidate_replacement="$(awk '
                    NR == FNR { seen[$1] = 1; next }
                    !seen[$1] { print $1; exit }
                ' "$initial_workers" "${current_workers}.tmp")"
                target_present="$(awk -v target="$fenced_worker_pid" '$1 == target { print "1"; exit }' \
                    "${current_workers}.tmp")"
                if [ "$current_count" -eq "$WORKERS" ] \
                    && [ -n "$candidate_replacement" ] \
                    && [ -z "$target_present" ]; then
                    mv "${current_workers}.tmp" "$current_workers"
                    replacement_pid="$candidate_replacement"
                    respawn_host="$(monotonic_ns)"
                    append_timeline "$timeline" worker_pool_recovered "replacement_pid=${replacement_pid}"
                    break
                fi
            fi
        fi
        if [ "$(date +%s)" -ge "$respawn_deadline" ]; then
            [ ! -f "${current_workers}.tmp" ] || mv "${current_workers}.tmp" "$current_workers"
            append_timeline "$timeline" worker_respawn_timeout "$RESPAWN_TIMEOUT seconds"
            break
        fi
        sleep 0.1
    done

    set +e
    producer php artisan bench:results --no-ansi "$run_id" \
        --expected="$JOBS" --wait="$COMPLETION_TIMEOUT" --poll-ms=200 \
        >"${ACTIVE_LANE_DIRECTORY}/result-check.json" \
        2>"${ACTIVE_LANE_DIRECTORY}/result-check.stderr.log"
    completion_status=$?
    set -e
    append_timeline "$timeline" completion_check "status=${completion_status}"
    sleep 1

    set +e
    capture_queue_state "$BENCH_CONNECTION" "$run_id" \
        >"${ACTIVE_LANE_DIRECTORY}/queue-final.json" \
        2>"${ACTIVE_LANE_DIRECTORY}/queue-final.stderr.log"
    queue_state_status=$?
    set -e
    append_timeline "$timeline" queue_state_captured "status=${queue_state_status}"

    producer php artisan bench:ledger-checkpoint --no-ansi "$run_id" \
        >"${ACTIVE_LANE_DIRECTORY}/ledger-checkpoint.json"
    append_timeline "$timeline" effect_ledger_checkpoint "$run_id"

    docker exec "$app_id" ps -eo pid=,ppid=,etimes=,lstart=,stat=,comm=,args= --forest \
        >"${ACTIVE_LANE_DIRECTORY}/process-tree-final.txt" 2>/dev/null || true
    docker logs --timestamps "$app_id" >"${ACTIVE_LANE_DIRECTORY}/app.log" 2>&1 || true
    capture_active_diagnostics
    docker run --rm --user 0:0 \
        --mount "type=volume,src=${ACTIVE_VOLUME},dst=/from,readonly" \
        --mount "type=bind,src=${ACTIVE_LANE_DIRECTORY},dst=/to" \
        "$APP_IMAGE" sh -ceu 'mkdir -p /to/results; cp -a "/from/$1/." /to/results/' sh "$run_id"

    set +e
    python3 "${SCRIPT_DIR}/analyze.py" ledger "${ACTIVE_LANE_DIRECTORY}/results" \
        --expected="$JOBS" \
        --allow-open-attempts \
        --allow-retried-executions \
        --output="${ACTIVE_LANE_DIRECTORY}/ledger-check.json"
    ledger_status=$?
    set -e
    append_timeline "$timeline" effect_ledger_check "status=${ledger_status}"

    write_lane_summary "$ACTIVE_LANE_DIRECTORY" "$lane_engine" "$run_id" "$app_id" \
        "$FAULT_SCENARIO" "$fenced_worker_pid" \
        "$target_pid" "$target_ppid" "$target_args" "$parent_args" "$replacement_pid" \
        "$kill_host_before" "$kill_host_after" "$kill_container_before" \
        "$kill_container_after" "$kill_wall" "$respawn_host" "$completion_status" "$ledger_status"
    append_timeline "$timeline" lane_complete "$lane_engine"

    lane_pass="$(python3 -c '
import json, sys
with open(sys.argv[1], encoding="utf-8") as stream:
    print("1" if json.load(stream)["checks"]["at_least_once_pass"] else "0")
' "${ACTIVE_LANE_DIRECTORY}/fault-result.json")"

    cleanup_active_lane
    if [ "$lane_pass" -ne 1 ]; then
        suite_status=1
    fi
}

suite_status=0
lane_number=0
for engine in "${ENGINES[@]}"; do
    lane_number=$((lane_number + 1))
    run_lane "$engine" "$lane_number"
done

python3 - "$OUTPUT_DIRECTORY" <<'PY'
import json
import sys
from pathlib import Path

root = Path(sys.argv[1])
results = []
for path in sorted(root.glob("*/fault-result.json")):
    results.append(json.loads(path.read_text(encoding="utf-8")))

aggregate = {
    "schema": "queen.laravel-supervisors.fault-report/v1",
    "lanes": results,
    "all_at_least_once_pass": bool(results) and all(
        item["checks"]["at_least_once_pass"] for item in results
    ),
    "all_strict_observation_pass": bool(results) and all(
        item["checks"]["strict_observation_pass"] for item in results
    ),
    "all_effect_conservation_pass": bool(results) and all(
        item["checks"]["effect_conservation_pass"] for item in results
    ),
    "all_duplicate_side_effect_free": bool(results) and all(
        item["checks"]["no_duplicate_side_effect_observed"] for item in results
    ),
}
(root / "report.json").write_text(
    json.dumps(aggregate, indent=2, sort_keys=True) + "\n", encoding="utf-8"
)

lines = [
    "# Fault/recovery smoke report",
    "",
    "| Engine | Scenario | Respawn ms | Unique | Missing | Completion duplicates | Idempotency dedup hits | Effects | Conservation | Retry observed | Queue zero | Restarts/OOM | At-least-once | Strict execution |",
    "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | --- | --- | --- | --- |",
]
for item in results:
    recovery = item["supervisor_recovery"]["kill_to_full_pool_ms"]
    recovery_text = "n/a" if recovery is None else f"{recovery:.1f}"
    job = item["job_recovery"]
    ledger = job["effect_ledger"]
    checks = item["checks"]
    lines.append(
        f"| {item['engine']} | {item['fault']['scenario']} | {recovery_text} | {job['unique_completed']}/{job['expected']} "
        f"| {job['missing']} | {job['duplicates']} "
        f"| {ledger['attempts']['already_present']['count']} "
        f"| {ledger['effects']['records']} "
        f"| {'pass' if checks['effect_conservation_pass'] else 'FAIL'} "
        f"| {'yes' if job['inflight_recovery_observed'] else 'NO'} "
        f"| {'yes' if checks['queue_reconciled_to_zero'] else 'NO'} "
        f"| {'pass' if checks['no_container_restart_or_oom'] else 'FAIL'} "
        f"| {'pass' if checks['at_least_once_pass'] else 'FAIL'} "
        f"| {'pass' if checks['strict_observation_pass'] else 'FAIL'} |"
    )
lines.extend([
    "",
    "The at-least-once gate requires worker respawn, every unique job, fixture-effect conservation, and no container restart/OOM. ",
    "The stricter execution gate additionally requires one attempt per job, zero completion duplicates and no failure signal in logs. ",
    "A retry is legal under at-least-once delivery; the `(run_id, job_id)` key converts a repeated fixture effect into an observable dedup hit.",
    "The SQLite ledger witnesses only fixture-local idempotent effects; it does not prove exactly-once effects in external systems.",
    "",
    "See `metadata.json`, each lane's `fault-result.json`, `ledger-check.json`, raw SQLite/JSONL results, process trees, resolved Compose file, container inspections and logs.",
])
(root / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
PY

trap - EXIT INT TERM
printf '\nFault/recovery artifacts: %s\n' "$OUTPUT_DIRECTORY"
printf 'Report: %s/report.md\n' "$OUTPUT_DIRECTORY"
exit "$suite_status"

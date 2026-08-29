#!/usr/bin/env bash

set -Eeuo pipefail
IFS=$'\n\t'

SCRIPT_DIR="$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
BENCH_DIR="$(CDPATH='' cd -- "${SCRIPT_DIR}/.." && pwd)"
REPOSITORY_ROOT="$(CDPATH='' cd -- "${BENCH_DIR}/../.." && pwd)"
COMPOSE_FILE="${BENCH_DIR}/compose.yml"
APP_IMAGE="queen-laravel-supervisor-bench:local"
BROKER_IMAGE="queen-laravel-supervisor-broker:local"

PROFILES_CSV="fixed,auto"
ENGINES_CSV="horizon,queen-php,queen-rust"
JOBS=2000
WORKERS=4
MIN_WORKERS=1
MAX_WORKERS=4
RUNS=3
SLEEP_MS=10
CPU_ITERATIONS=0
DISPATCH_MODE="${BENCH_DISPATCH_MODE:-single}"
QUEEN_PREFETCH="${QUEEN_PREFETCH:-1}"
QUEEN_ACK_BATCH="${QUEEN_ACK_BATCH:-1}"
QUEEN_BULK_BATCH="${QUEEN_BULK_BATCH:-100}"
QUEEN_PARTITIONS="${QUEEN_PARTITIONS:-64}"
QUEEN_POP_FUSION="${QUEEN_POP_FUSION:-0}"
LEDGER_MODE="${BENCH_LEDGER_MODE:-off}"
TIMED_QUEUE="benchmark"
WARMUP_JOBS=50
SAMPLE_INTERVAL="0.50"
WAIT_TIMEOUT=300
WORKER_TIMEOUT="${BENCH_TIMEOUT:-120}"
RETRY_AFTER="${BENCH_RETRY_AFTER:-0}"
BALANCE_COOLDOWN=3
BALANCE_MAX_SHIFT=1
SCALING_STRATEGY="size"
TARGET_JOBS_PER_PROCESS=10
TARGET_CLEAR_SECONDS="1.0"
POST_DRAIN_SECONDS="auto"
BUILD_IMAGES=1
INCLUDE_PSS=1
ALLOW_FOREIGN_CONTAINERS=0
QUALIFICATION_MODE="auto"
SMOKE=0
OUTPUT_ROOT="${BENCH_DIR}/results"

CURRENT_PROJECT=""
CURRENT_ENGINE=""
CURRENT_MONITOR=""
CURRENT_VOLUME=""
CURRENT_STATS_VOLUME=""
CURRENT_HOST_RUN=""
CURRENT_ISOLATION_PID=""
CURRENT_ISOLATION_STOP=""
CURRENT_ISOLATION_READY=""

usage() {
    cat <<'EOF'
Usage: scripts/run.sh [options]

Run isolated Horizon, Queen PHP and Queen Rust supervisor benchmarks.

Options:
  --smoke                       60 jobs, 2 workers, fixed profile, one run
  --profile fixed|auto|both     Profiles to run (default: both)
  --engines CSV                 Ordered subset of horizon,queen-php,queen-rust
  --jobs N                      Jobs in each measured burst (default: 2000)
  --workers N                   Fixed workers and default auto maximum (default: 4)
  --min-workers N               Auto minimum (default: 1)
  --max-workers N               Auto maximum (default: 4)
  --runs N                      Repetitions per engine/profile (default: 3)
  --sleep-ms N                  Sleep in every job (default: 10)
  --cpu-iterations N            SHA-256 rounds in every job (default: 0)
  --dispatch-mode single|bulk   Producer API shape (default: single)
  --queen-prefetch N            Jobs claimed by each Queen pop (default: 1)
  --queen-ack-batch N           Deferred Queen ACK batch; <= prefetch (default: 1)
  --queen-bulk-batch N          Jobs per bulk producer call/request (default: 100)
  --queen-partitions N          Queen partitions scanned per pop (default: 64)
  --queen-pop-fusion 0|1        Broker pop-transaction fusion (default: 0)
  --ledger                      Enable durable attempt/effect auditing; changes the workload
  --warmup-jobs N               Warm-up jobs before each sample (default: 50)
  --sample-interval SECONDS     cgroup/process sampling period (default: 0.50)
  --timeout SECONDS             Completion timeout per run (default: 300)
  --worker-timeout SECONDS      Laravel per-job timeout (default: 120)
  --retry-after SECONDS         Lease/visibility timeout; default max(180, prefetch*worker-timeout+1)
  --post-drain SECONDS          Observation after completion; default scales with auto
  --strategy size|time          Autoscaling pressure strategy (default: size)
  --target-jobs N               Queen size-strategy jobs per process (default: 10)
  --target-clear SECONDS        Queen time-strategy clearance target (default: 1.0)
  --no-pss                      Skip proportional-set-size process sampling
  --no-build                    Reuse the two local benchmark images
  --allow-foreign-containers    Diagnostic only: retain unrelated running containers
  --qualification MODE         auto|diagnostic|publishable (default: auto)
  --output DIRECTORY            Campaign result parent
  -h, --help                    Show this help

The load generator is intentionally outside the measured cgroups. Every lane
uses a fresh Compose project, backend and named result volume.
Optimization defaults may also be set with BENCH_DISPATCH_MODE,
QUEEN_PREFETCH, QUEEN_ACK_BATCH, QUEEN_BULK_BATCH, QUEEN_PARTITIONS,
QUEEN_POP_FUSION, BENCH_TIMEOUT and BENCH_RETRY_AFTER. Explicit CLI options
take precedence.

`--qualification publishable` fails closed unless the host is native Linux
with Docker on cgroup v2. `--allow-foreign-containers` is incompatible with
that mode and always makes the campaign diagnostic. Publishable campaigns
must build their images in the same invocation; `--no-build` is rejected.
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

require_decimal() {
    case "$2" in
        ''|*[!0-9.]*) die "$1 must be a positive decimal number" ;;
    esac
    python3 - "$1" "$2" <<'PY'
import math
import sys

try:
    value = float(sys.argv[2])
except ValueError:
    raise SystemExit(f"{sys.argv[1]} must be a positive decimal number")
if not math.isfinite(value) or value <= 0:
    raise SystemExit(f"{sys.argv[1]} must be a positive decimal number")
PY
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --smoke)
            SMOKE=1
            shift
            ;;
        --profile)
            [ "$#" -ge 2 ] || die "--profile requires a value"
            case "$2" in
                fixed) PROFILES_CSV="fixed" ;;
                auto) PROFILES_CSV="auto" ;;
                both) PROFILES_CSV="fixed,auto" ;;
                *) die "--profile must be fixed, auto or both" ;;
            esac
            shift 2
            ;;
        --engines)
            [ "$#" -ge 2 ] || die "--engines requires a value"
            ENGINES_CSV="$2"
            shift 2
            ;;
        --jobs) JOBS="${2:?--jobs requires a value}"; shift 2 ;;
        --workers) WORKERS="${2:?--workers requires a value}"; MAX_WORKERS="$2"; shift 2 ;;
        --min-workers) MIN_WORKERS="${2:?--min-workers requires a value}"; shift 2 ;;
        --max-workers) MAX_WORKERS="${2:?--max-workers requires a value}"; shift 2 ;;
        --runs) RUNS="${2:?--runs requires a value}"; shift 2 ;;
        --sleep-ms) SLEEP_MS="${2:?--sleep-ms requires a value}"; shift 2 ;;
        --cpu-iterations) CPU_ITERATIONS="${2:?--cpu-iterations requires a value}"; shift 2 ;;
        --dispatch-mode) DISPATCH_MODE="${2:?--dispatch-mode requires a value}"; shift 2 ;;
        --queen-prefetch) QUEEN_PREFETCH="${2:?--queen-prefetch requires a value}"; shift 2 ;;
        --queen-ack-batch) QUEEN_ACK_BATCH="${2:?--queen-ack-batch requires a value}"; shift 2 ;;
        --queen-bulk-batch) QUEEN_BULK_BATCH="${2:?--queen-bulk-batch requires a value}"; shift 2 ;;
        --queen-partitions) QUEEN_PARTITIONS="${2:?--queen-partitions requires a value}"; shift 2 ;;
        --queen-pop-fusion) QUEEN_POP_FUSION="${2:?--queen-pop-fusion requires a value}"; shift 2 ;;
        --ledger) LEDGER_MODE="durable"; shift ;;
        --warmup-jobs) WARMUP_JOBS="${2:?--warmup-jobs requires a value}"; shift 2 ;;
        --sample-interval) SAMPLE_INTERVAL="${2:?--sample-interval requires a value}"; shift 2 ;;
        --timeout) WAIT_TIMEOUT="${2:?--timeout requires a value}"; shift 2 ;;
        --worker-timeout) WORKER_TIMEOUT="${2:?--worker-timeout requires a value}"; shift 2 ;;
        --retry-after) RETRY_AFTER="${2:?--retry-after requires a value}"; shift 2 ;;
        --post-drain) POST_DRAIN_SECONDS="${2:?--post-drain requires a value}"; shift 2 ;;
        --strategy) SCALING_STRATEGY="${2:?--strategy requires a value}"; shift 2 ;;
        --target-jobs) TARGET_JOBS_PER_PROCESS="${2:?--target-jobs requires a value}"; shift 2 ;;
        --target-clear) TARGET_CLEAR_SECONDS="${2:?--target-clear requires a value}"; shift 2 ;;
        --no-pss) INCLUDE_PSS=0; shift ;;
        --no-build) BUILD_IMAGES=0; shift ;;
        --allow-foreign-containers) ALLOW_FOREIGN_CONTAINERS=1; shift ;;
        --qualification) QUALIFICATION_MODE="${2:?--qualification requires a value}"; shift 2 ;;
        --output) OUTPUT_ROOT="${2:?--output requires a value}"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) die "unknown option: $1" ;;
    esac
done

if [ "$SMOKE" -eq 1 ]; then
    PROFILES_CSV="fixed"
    JOBS=60
    WORKERS=2
    MIN_WORKERS=1
    MAX_WORKERS=2
    RUNS=1
    SLEEP_MS=5
    CPU_ITERATIONS=0
    WARMUP_JOBS=10
    SAMPLE_INTERVAL="0.50"
    WAIT_TIMEOUT=120
    POST_DRAIN_SECONDS=2
fi

if [ "$QUALIFICATION_MODE" = publishable ] && [ "$BUILD_IMAGES" -eq 0 ]; then
    die "--qualification publishable cannot be combined with --no-build"
fi

require_command docker
require_command git
require_command python3
require_positive_int "--jobs" "$JOBS"
require_positive_int "--workers" "$WORKERS"
require_positive_int "--min-workers" "$MIN_WORKERS"
require_positive_int "--max-workers" "$MAX_WORKERS"
require_positive_int "--runs" "$RUNS"
require_uint "--sleep-ms" "$SLEEP_MS"
require_uint "--cpu-iterations" "$CPU_ITERATIONS"
require_positive_int "--queen-prefetch" "$QUEEN_PREFETCH"
require_positive_int "--queen-ack-batch" "$QUEEN_ACK_BATCH"
require_positive_int "--queen-bulk-batch" "$QUEEN_BULK_BATCH"
require_positive_int "--queen-partitions" "$QUEEN_PARTITIONS"
require_uint "--queen-pop-fusion" "$QUEEN_POP_FUSION"
require_uint "--warmup-jobs" "$WARMUP_JOBS"
require_positive_int "--timeout" "$WAIT_TIMEOUT"
require_positive_int "--worker-timeout" "$WORKER_TIMEOUT"
if [ "$RETRY_AFTER" = "0" ]; then
    RETRY_AFTER=$(( QUEEN_PREFETCH * WORKER_TIMEOUT + 1 ))
    if [ "$RETRY_AFTER" -lt 180 ]; then
        RETRY_AFTER=180
    fi
fi
require_positive_int "--retry-after" "$RETRY_AFTER"
require_positive_int "--target-jobs" "$TARGET_JOBS_PER_PROCESS"
require_decimal "--sample-interval" "$SAMPLE_INTERVAL"
require_decimal "--target-clear" "$TARGET_CLEAR_SECONDS"
[ "$MIN_WORKERS" -le "$MAX_WORKERS" ] || die "--min-workers must not exceed --max-workers"
[ "$QUEEN_PREFETCH" -le 1000 ] || die "--queen-prefetch must not exceed 1000"
[ "$QUEEN_ACK_BATCH" -le "$QUEEN_PREFETCH" ] || die "--queen-ack-batch must not exceed --queen-prefetch"
[ "$QUEEN_BULK_BATCH" -le 1000 ] || die "--queen-bulk-batch must not exceed 1000"
[ "$QUEEN_PARTITIONS" -le 64 ] || die "--queen-partitions must not exceed 64"
[ "$QUEEN_POP_FUSION" -le 1 ] || die "--queen-pop-fusion must be 0 or 1"
[ "$WORKER_TIMEOUT" -le 86400 ] || die "--worker-timeout must not exceed 86400"
[ "$RETRY_AFTER" -le 86401 ] || die "--retry-after must not exceed 86401"
[ "$RETRY_AFTER" -gt $(( QUEEN_PREFETCH * WORKER_TIMEOUT )) ] || die "--retry-after must exceed --queen-prefetch multiplied by --worker-timeout"
LEASE_RENEWAL=false
if [ "$QUEEN_PREFETCH" -gt 1 ]; then
    LEASE_RENEWAL=true
fi
case "$DISPATCH_MODE" in single|bulk) ;; *) die "--dispatch-mode must be single or bulk" ;; esac
case "$SCALING_STRATEGY" in size|time) ;; *) die "--strategy must be size or time" ;; esac
case "$LEDGER_MODE" in off|durable) ;; *) die "BENCH_LEDGER_MODE must be off or durable" ;; esac
case "$QUALIFICATION_MODE" in
    auto|diagnostic|publishable) ;;
    *) die "--qualification must be auto, diagnostic or publishable" ;;
esac
[ "$QUALIFICATION_MODE" != publishable ] || [ "$ALLOW_FOREIGN_CONTAINERS" -eq 0 ] \
    || die "--qualification publishable cannot be combined with --allow-foreign-containers"
if [ "$QUALIFICATION_MODE" = publishable ] \
    && [ -n "$(git -C "$REPOSITORY_ROOT" status --porcelain=v1 --untracked-files=all)" ]; then
    die "publishable qualification requires a clean Git worktree"
fi
if [ "$POST_DRAIN_SECONDS" != "auto" ]; then
    require_uint "--post-drain" "$POST_DRAIN_SECONDS"
fi

OLD_IFS="$IFS"
IFS=',' read -r -a ENGINES <<EOF
${ENGINES_CSV}
EOF
IFS=',' read -r -a PROFILES <<EOF
${PROFILES_CSV}
EOF
IFS="$OLD_IFS"
[ "${#ENGINES[@]}" -gt 0 ] || die "at least one engine is required"
[ "${#PROFILES[@]}" -gt 0 ] || die "at least one profile is required"
for engine in "${ENGINES[@]}"; do
    case "$engine" in
        horizon|queen-php|queen-rust) ;;
        *) die "unknown engine in --engines: $engine" ;;
    esac
done
for profile in "${PROFILES[@]}"; do
    case "$profile" in fixed|auto) ;; *) die "unknown profile: $profile" ;; esac
done

docker info >/dev/null 2>&1 || die "Docker daemon is unavailable"
cgroup_version="$(docker info --format '{{.CgroupVersion}}')"
[ "$cgroup_version" = "2" ] || die "this benchmark requires cgroup v2; Docker reported ${cgroup_version}"
host_system="$(uname -s)"
docker_os_type="$(docker info --format '{{.OSType}}')"
docker_operating_system="$(docker info --format '{{.OperatingSystem}}')"
docker_desktop=0
case "$docker_operating_system" in
    *Docker\ Desktop*) docker_desktop=1 ;;
esac
native_linux=0
if [ "$host_system" = Linux ] \
    && [ "$docker_os_type" = linux ] \
    && [ "$cgroup_version" = 2 ] \
    && [ "$docker_desktop" -eq 0 ]; then
    native_linux=1
fi
if [ "$QUALIFICATION_MODE" = publishable ] && [ "$native_linux" -ne 1 ]; then
    die "publishable qualification requires native Linux, Docker OSType=linux and cgroup v2; host=${host_system}, Docker=${docker_operating_system}"
fi

compose_current() {
    docker compose \
        --file "$COMPOSE_FILE" \
        --project-name "$CURRENT_PROJECT" \
        --profile "$CURRENT_ENGINE" \
        --profile tools \
        "$@"
}

container_exists() {
    docker container inspect "$1" >/dev/null 2>&1
}

capture_container_isolation() {
    isolation_phase="$1"
    isolation_output="${CURRENT_HOST_RUN}/container-isolation.${isolation_phase}.json"
    isolation_options=(
        --phase "$isolation_phase"
        --project "$CURRENT_PROJECT"
        --monitor "$CURRENT_MONITOR"
        --output "$isolation_output"
        --allowed-service "$CURRENT_ENGINE"
        --allowed-service producer
    )
    if [ "$CURRENT_ENGINE" = horizon ]; then
        isolation_options+=(--allowed-service redis)
    else
        isolation_options+=(--allowed-service broker --allowed-service postgres)
    fi
    if [ "$ALLOW_FOREIGN_CONTAINERS" -eq 1 ]; then
        isolation_options+=(--allow-foreign)
    fi
    python3 "${SCRIPT_DIR}/container-isolation.py" "${isolation_options[@]}"
}

start_continuous_isolation() {
    local isolation_output
    local isolation_stderr
    local deadline
    local container_id
    local monitor_id
    local -a isolation_options

    [ -z "$CURRENT_ISOLATION_PID" ] || die "container isolation watch is already active"
    isolation_output="${CURRENT_HOST_RUN}/container-isolation.measurement.json"
    isolation_stderr="${CURRENT_HOST_RUN}/container-isolation.measurement.stderr.log"
    CURRENT_ISOLATION_STOP="${CURRENT_HOST_RUN}/.container-isolation.stop"
    CURRENT_ISOLATION_READY="${CURRENT_HOST_RUN}/.container-isolation.ready.json"
    rm -f "$CURRENT_ISOLATION_STOP" "$CURRENT_ISOLATION_READY"
    isolation_options=(
        --phase measurement
        --project "$CURRENT_PROJECT"
        --monitor "$CURRENT_MONITOR"
        --output "$isolation_output"
        --watch-until "$CURRENT_ISOLATION_STOP"
        --ready-file "$CURRENT_ISOLATION_READY"
        --allowed-service "$CURRENT_ENGINE"
        --allowed-service producer
    )
    if [ "$CURRENT_ENGINE" = horizon ]; then
        isolation_options+=(--allowed-service redis)
    else
        isolation_options+=(--allowed-service broker --allowed-service postgres)
    fi
    while IFS= read -r container_id; do
        [ -n "$container_id" ] || continue
        isolation_options+=(--allowed-container "$container_id")
    done < <(compose_current ps --quiet)
    monitor_id="$(docker inspect --format '{{.Id}}' "$CURRENT_MONITOR")"
    [ -n "$monitor_id" ] || die "unable to resolve sampler container for isolation watch"
    isolation_options+=(--allowed-container "$monitor_id")
    if [ "$ALLOW_FOREIGN_CONTAINERS" -eq 1 ]; then
        isolation_options+=(--allow-foreign)
    fi

    python3 "${SCRIPT_DIR}/container-isolation.py" "${isolation_options[@]}" \
        2>"$isolation_stderr" &
    CURRENT_ISOLATION_PID=$!
    deadline=$(( $(date +%s) + 15 ))
    while [ ! -f "$CURRENT_ISOLATION_READY" ]; do
        if ! kill -0 "$CURRENT_ISOLATION_PID" >/dev/null 2>&1; then
            wait "$CURRENT_ISOLATION_PID" || true
            CURRENT_ISOLATION_PID=""
            rm -f "$CURRENT_ISOLATION_STOP" "$CURRENT_ISOLATION_READY"
            CURRENT_ISOLATION_STOP=""
            CURRENT_ISOLATION_READY=""
            die "container isolation watch exited before becoming ready; inspect ${isolation_stderr}"
        fi
        if [ "$(date +%s)" -ge "$deadline" ]; then
            touch "$CURRENT_ISOLATION_STOP"
            wait "$CURRENT_ISOLATION_PID" || true
            CURRENT_ISOLATION_PID=""
            rm -f "$CURRENT_ISOLATION_STOP" "$CURRENT_ISOLATION_READY"
            CURRENT_ISOLATION_STOP=""
            CURRENT_ISOLATION_READY=""
            die "timed out waiting for container isolation watch readiness"
        fi
        sleep 0.05
    done
    kill -0 "$CURRENT_ISOLATION_PID" >/dev/null 2>&1 \
        || die "container isolation watch stopped after writing readiness"
}

stop_continuous_isolation() {
    local watch_status
    [ -n "$CURRENT_ISOLATION_PID" ] || return 0
    touch "$CURRENT_ISOLATION_STOP"
    if wait "$CURRENT_ISOLATION_PID"; then
        watch_status=0
    else
        watch_status=$?
    fi
    rm -f "$CURRENT_ISOLATION_STOP" "$CURRENT_ISOLATION_READY"
    CURRENT_ISOLATION_PID=""
    CURRENT_ISOLATION_STOP=""
    CURRENT_ISOLATION_READY=""
    return "$watch_status"
}

capture_lane_diagnostics() {
    [ -n "$CURRENT_PROJECT" ] || return 0
    [ -n "$CURRENT_HOST_RUN" ] || return 0
    mkdir -p "$CURRENT_HOST_RUN"
    compose_current ps --all >"${CURRENT_HOST_RUN}/compose-ps.txt" 2>&1 || true
    compose_current logs --no-color --timestamps >"${CURRENT_HOST_RUN}/compose.log" 2>&1 || true
    ids="$(compose_current ps --all --quiet 2>/dev/null || true)"
    if [ -n "$ids" ]; then
        # Compose emits one container id per line and IFS excludes spaces.
        # shellcheck disable=SC2086
        docker inspect $ids >"${CURRENT_HOST_RUN}/containers.json" 2>/dev/null || true
    fi
}

cleanup_lane() {
    stop_continuous_isolation >/dev/null 2>&1 || true
    if [ -n "$CURRENT_MONITOR" ] && container_exists "$CURRENT_MONITOR"; then
        docker stop --time 10 "$CURRENT_MONITOR" >/dev/null 2>&1 || true
        docker rm --force "$CURRENT_MONITOR" >/dev/null 2>&1 || true
    fi
    if [ -n "$CURRENT_PROJECT" ]; then
        compose_current down --volumes --remove-orphans --timeout 20 >/dev/null 2>&1 || true
    fi
    if [ -n "$CURRENT_VOLUME" ] && docker volume inspect "$CURRENT_VOLUME" >/dev/null 2>&1; then
        docker volume rm "$CURRENT_VOLUME" >/dev/null 2>&1 || true
    fi
    if [ -n "$CURRENT_STATS_VOLUME" ] && docker volume inspect "$CURRENT_STATS_VOLUME" >/dev/null 2>&1; then
        docker volume rm "$CURRENT_STATS_VOLUME" >/dev/null 2>&1 || true
    fi
    CURRENT_PROJECT=""
    CURRENT_ENGINE=""
    CURRENT_MONITOR=""
    CURRENT_VOLUME=""
    CURRENT_STATS_VOLUME=""
    CURRENT_HOST_RUN=""
}

finish_lane() {
    compose_current down --volumes --remove-orphans --timeout 20 >/dev/null
    if docker volume inspect "$CURRENT_VOLUME" >/dev/null 2>&1; then
        docker volume rm "$CURRENT_VOLUME" >/dev/null
    fi
    if docker volume inspect "$CURRENT_STATS_VOLUME" >/dev/null 2>&1; then
        docker volume rm "$CURRENT_STATS_VOLUME" >/dev/null
    fi
    CURRENT_PROJECT=""
    CURRENT_ENGINE=""
    CURRENT_MONITOR=""
    CURRENT_VOLUME=""
    CURRENT_STATS_VOLUME=""
    CURRENT_HOST_RUN=""
}

on_exit() {
    status=$?
    trap - EXIT INT TERM
    if [ "$status" -ne 0 ]; then
        capture_lane_diagnostics
    fi
    cleanup_lane
    exit "$status"
}
trap on_exit EXIT INT TERM

wait_for_health() {
    container_id="$1"
    timeout="$2"
    label="$3"
    deadline=$(( $(date +%s) + timeout ))
    while :; do
        status="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$container_id" 2>/dev/null || true)"
        case "$status" in
            healthy|running) return 0 ;;
            unhealthy|exited|dead)
                docker inspect --format '{{json .State}}' "$container_id" >&2 || true
                die "$label entered state: $status"
                ;;
        esac
        if [ "$(date +%s)" -ge "$deadline" ]; then
            die "timed out waiting for $label health (last state: ${status:-missing})"
        fi
        sleep 1
    done
}

wait_for_sampler() {
    monitor="$1"
    ready_file="$2"
    deadline=$(( $(date +%s) + 30 ))
    while :; do
        if docker exec "$monitor" test -f "$ready_file" >/dev/null 2>&1; then
            return 0
        fi
        container_exists "$monitor" || die "sampler exited before becoming ready"
        if [ "$(date +%s)" -ge "$deadline" ]; then
            docker logs "$monitor" >&2 || true
            die "timed out waiting for sampler readiness"
        fi
        sleep 1
    done
}

producer() {
    compose_current exec --no-TTY producer "$@"
}

image_id() {
    docker image inspect "$1" --format '{{.Id}}' 2>/dev/null || true
}

contains_queen_engine=0
for engine in "${ENGINES[@]}"; do
    case "$engine" in queen-php|queen-rust) contains_queen_engine=1 ;; esac
done

if [ "$BUILD_IMAGES" -eq 1 ]; then
    printf 'Building benchmark application image...\n'
    docker compose --file "$COMPOSE_FILE" --profile tools build producer
    if [ "$contains_queen_engine" -eq 1 ]; then
        printf 'Building Queen broker image...\n'
        docker compose --file "$COMPOSE_FILE" --profile queen-php build broker
    fi
else
    [ -n "$(image_id "$APP_IMAGE")" ] || die "missing image: $APP_IMAGE"
    if [ "$contains_queen_engine" -eq 1 ]; then
        [ -n "$(image_id "$BROKER_IMAGE")" ] || die "missing image: $BROKER_IMAGE"
    fi
fi

campaign_id="$(date -u +%Y%m%dT%H%M%SZ)-$(git -C "$REPOSITORY_ROOT" rev-parse --short=10 HEAD)"
mkdir -p "$OUTPUT_ROOT"
OUTPUT_ROOT="$(CDPATH='' cd -- "$OUTPUT_ROOT" && pwd)"
campaign_dir="${OUTPUT_ROOT%/}/${campaign_id}"
[ ! -e "$campaign_dir" ] || die "campaign output already exists: $campaign_dir"
mkdir -p "$campaign_dir"

export CAMPAIGN_ID="$campaign_id"
export CAMPAIGN_DIRECTORY="$campaign_dir"
export BENCHMARK_REPOSITORY_ROOT="$REPOSITORY_ROOT"
export BENCHMARK_APP_IMAGE="$APP_IMAGE"
export BENCHMARK_BROKER_IMAGE="$BROKER_IMAGE"
export BENCHMARK_PROFILES="$PROFILES_CSV"
export BENCHMARK_ENGINES="$ENGINES_CSV"
export BENCHMARK_JOBS="$JOBS"
export BENCHMARK_WORKERS="$WORKERS"
export BENCHMARK_MIN_WORKERS="$MIN_WORKERS"
export BENCHMARK_MAX_WORKERS="$MAX_WORKERS"
export BENCHMARK_RUNS="$RUNS"
export BENCHMARK_SLEEP_MS="$SLEEP_MS"
export BENCHMARK_CPU_ITERATIONS="$CPU_ITERATIONS"
export BENCHMARK_DISPATCH_MODE="$DISPATCH_MODE"
export BENCHMARK_QUEUE="$TIMED_QUEUE"
export BENCHMARK_QUEUES=""
export BENCHMARK_FAILED_DRIVER="null"
export BENCHMARK_LEASE_RENEWAL="$LEASE_RENEWAL"
export BENCHMARK_QUEEN_PREFETCH="$QUEEN_PREFETCH"
export BENCHMARK_QUEEN_ACK_BATCH="$QUEEN_ACK_BATCH"
export BENCHMARK_QUEEN_BULK_BATCH="$QUEEN_BULK_BATCH"
export BENCHMARK_QUEEN_PARTITIONS="$QUEEN_PARTITIONS"
export BENCHMARK_QUEEN_POP_FUSION="$QUEEN_POP_FUSION"
export BENCHMARK_SAMPLE_INTERVAL="$SAMPLE_INTERVAL"
export BENCHMARK_POST_DRAIN="$POST_DRAIN_SECONDS"
export BENCHMARK_WARMUP_JOBS="$WARMUP_JOBS"
export BENCHMARK_COMPLETION_TIMEOUT="$WAIT_TIMEOUT"
export BENCHMARK_WORKER_TIMEOUT="$WORKER_TIMEOUT"
export BENCHMARK_RETRY_AFTER="$RETRY_AFTER"
export BENCHMARK_INCLUDE_PSS="$INCLUDE_PSS"
export BENCHMARK_STRATEGY="$SCALING_STRATEGY"
export BENCHMARK_BALANCE_COOLDOWN="$BALANCE_COOLDOWN"
export BENCHMARK_BALANCE_MAX_SHIFT="$BALANCE_MAX_SHIFT"
export BENCHMARK_TARGET_JOBS="$TARGET_JOBS_PER_PROCESS"
export BENCHMARK_TARGET_CLEAR="$TARGET_CLEAR_SECONDS"
export BENCHMARK_LEDGER_MODE="$LEDGER_MODE"
export BENCHMARK_ALLOW_FOREIGN_CONTAINERS="$ALLOW_FOREIGN_CONTAINERS"
export BENCHMARK_QUALIFICATION_MODE="$QUALIFICATION_MODE"
export BENCHMARK_NATIVE_LINUX="$native_linux"
export BENCHMARK_DOCKER_DESKTOP="$docker_desktop"
export BENCHMARK_HOST_SYSTEM="$host_system"
export QUEEN_PREFETCH
export QUEEN_ACK_BATCH
export QUEEN_BULK_BATCH
export QUEEN_PARTITIONS
export QUEEN_POP_FUSION
export BENCH_TIMEOUT="$WORKER_TIMEOUT"
export BENCH_RETRY_AFTER="$RETRY_AFTER"
export BENCH_LEDGER_MODE="$LEDGER_MODE"
# Timed lanes are deliberately single-queue and exclude failure persistence.
# Multi-message prefetch always includes the production lease-renewal fence;
# reassert these values instead of inheriting feature-probe environment.
export BENCH_QUEUE="$TIMED_QUEUE"
export BENCH_QUEUES=''
export BENCH_FAILED_DRIVER='null'
export BENCH_LEASE_RENEWAL="$LEASE_RENEWAL"

python3 - <<'PY'
import datetime as dt
import json
import os
import platform
import subprocess
from pathlib import Path

def output(*command: str) -> str:
    return subprocess.check_output(command, text=True, stderr=subprocess.DEVNULL).strip()

repository = os.environ["BENCHMARK_REPOSITORY_ROOT"]
docker_info = json.loads(output("docker", "info", "--format", "{{json .}}"))
settings = {
    "profiles": os.environ["BENCHMARK_PROFILES"].split(","),
    "engines": os.environ["BENCHMARK_ENGINES"].split(","),
    "jobs": int(os.environ["BENCHMARK_JOBS"]),
    "workers": int(os.environ["BENCHMARK_WORKERS"]),
    "min_workers": int(os.environ["BENCHMARK_MIN_WORKERS"]),
    "max_workers": int(os.environ["BENCHMARK_MAX_WORKERS"]),
    "runs": int(os.environ["BENCHMARK_RUNS"]),
    "sleep_ms": int(os.environ["BENCHMARK_SLEEP_MS"]),
    "cpu_iterations": int(os.environ["BENCHMARK_CPU_ITERATIONS"]),
    "dispatch_mode": os.environ["BENCHMARK_DISPATCH_MODE"],
    "queues": [os.environ["BENCHMARK_QUEUE"]],
    "failed_driver": os.environ["BENCHMARK_FAILED_DRIVER"],
    "lease_renewal": os.environ["BENCHMARK_LEASE_RENEWAL"] == "true",
    "queen_prefetch": int(os.environ["BENCHMARK_QUEEN_PREFETCH"]),
    "queen_ack_batch": int(os.environ["BENCHMARK_QUEEN_ACK_BATCH"]),
    "queen_bulk_batch": int(os.environ["BENCHMARK_QUEEN_BULK_BATCH"]),
    "queen_partitions": int(os.environ["BENCHMARK_QUEEN_PARTITIONS"]),
    "queen_pop_fusion": os.environ["BENCHMARK_QUEEN_POP_FUSION"] == "1",
    "sample_interval_seconds": float(os.environ["BENCHMARK_SAMPLE_INTERVAL"]),
    "warmup_jobs": int(os.environ["BENCHMARK_WARMUP_JOBS"]),
    "completion_timeout_seconds": int(os.environ["BENCHMARK_COMPLETION_TIMEOUT"]),
    "worker_timeout_seconds": int(os.environ["BENCHMARK_WORKER_TIMEOUT"]),
    "retry_after_seconds": int(os.environ["BENCHMARK_RETRY_AFTER"]),
    "pss_requested": os.environ["BENCHMARK_INCLUDE_PSS"] == "1",
    "post_drain_seconds_by_profile": {
        profile: (
            int(os.environ["BENCHMARK_POST_DRAIN"])
            if os.environ["BENCHMARK_POST_DRAIN"] != "auto"
            else (
                int(os.environ["BENCHMARK_BALANCE_COOLDOWN"])
                * (int(os.environ["BENCHMARK_MAX_WORKERS"]) - int(os.environ["BENCHMARK_MIN_WORKERS"]))
                + 2
                if profile == "auto"
                else 2
            )
        )
        for profile in os.environ["BENCHMARK_PROFILES"].split(",")
    },
    "autoscaling_strategy": os.environ["BENCHMARK_STRATEGY"],
    "balance_cooldown_seconds": int(os.environ["BENCHMARK_BALANCE_COOLDOWN"]),
    "balance_max_shift": int(os.environ["BENCHMARK_BALANCE_MAX_SHIFT"]),
    "target_jobs_per_process": int(os.environ["BENCHMARK_TARGET_JOBS"]),
    "target_clear_seconds": float(os.environ["BENCHMARK_TARGET_CLEAR"]),
    "ledger_mode": os.environ["BENCHMARK_LEDGER_MODE"],
    "allow_foreign_containers": os.environ["BENCHMARK_ALLOW_FOREIGN_CONTAINERS"] == "1",
}
native_linux = os.environ["BENCHMARK_NATIVE_LINUX"] == "1"
qualification_mode = os.environ["BENCHMARK_QUALIFICATION_MODE"]
allow_foreign = settings["allow_foreign_containers"]
if allow_foreign:
    qualification = "diagnostic"
elif qualification_mode == "publishable":
    qualification = "publishable_candidate"
elif native_linux:
    qualification = "diagnostic_native"
else:
    qualification = "diagnostic"
metadata = {
    "schema": "queen.laravel-supervisors.campaign/v1",
    "campaign_id": os.environ["CAMPAIGN_ID"],
    "created_at": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
    "git": {
        "commit": output("git", "-C", repository, "rev-parse", "HEAD"),
        "branch": output("git", "-C", repository, "branch", "--show-current"),
        "dirty": bool(output("git", "-C", repository, "status", "--porcelain")),
    },
    "host": {
        "platform": platform.platform(),
        "machine": platform.machine(),
        "python": platform.python_version(),
    },
    "host_qualification": {
        "requested": qualification_mode,
        "host_system": os.environ["BENCHMARK_HOST_SYSTEM"],
        "native_linux": native_linux,
        "cgroup_v2": docker_info.get("CgroupVersion") == "2",
        "docker_desktop": os.environ["BENCHMARK_DOCKER_DESKTOP"] == "1",
        "publishable_host_eligible": native_linux and not allow_foreign,
        "foreign_container_override": allow_foreign,
        "decision": qualification,
    },
    "docker": {
        key: docker_info.get(key)
        for key in (
            "ServerVersion", "OperatingSystem", "OSType", "Architecture",
            "NCPU", "MemTotal", "CgroupVersion", "KernelVersion",
        )
    },
    "images": {
        "app": output("docker", "image", "inspect", os.environ["BENCHMARK_APP_IMAGE"], "--format", "{{.Id}}"),
        "broker": output("docker", "image", "inspect", os.environ["BENCHMARK_BROKER_IMAGE"], "--format", "{{.Id}}")
            if any(name.startswith("queen-") for name in settings["engines"]) else None,
    },
    "settings": settings,
    "qualification": qualification,
}
path = Path(os.environ["CAMPAIGN_DIRECTORY"]) / "metadata.json"
path.write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY

REPORT_INPUTS=()
lane_number=0

run_lane() {
    engine="$1"
    profile="$2"
    repetition="$3"
    lane_number=$((lane_number + 1))

    repetition_label="$(printf 'r%02d' "$repetition")"
    run_id="${engine}-${profile}-${repetition_label}"
    project_suffix="$(printf '%s-%02d' "$campaign_id" "$lane_number" | tr '[:upper:]' '[:lower:]' | tr -cd 'a-z0-9-')"
    CURRENT_PROJECT="qlb-${project_suffix}"
    CURRENT_ENGINE="$engine"
    CURRENT_MONITOR="qlb-monitor-${project_suffix}"
    CURRENT_VOLUME="qlb-results-${project_suffix}"
    CURRENT_STATS_VOLUME="qlb-stats-${project_suffix}"
    CURRENT_HOST_RUN="${campaign_dir}/${engine}/${profile}/${repetition_label}"
    mkdir -p "$CURRENT_HOST_RUN"

    # No unrelated running container may share the Docker daemon with a timed
    # lane. The explicit override preserves evidence but makes the campaign
    # diagnostic in metadata.
    capture_container_isolation lane-start

    export BENCH_RESULTS_VOLUME="$CURRENT_VOLUME"
    export BENCH_PROFILE="$profile"
    export BENCH_QUEUE="$TIMED_QUEUE"
    export BENCH_GROUP="benchmark"
    export BENCH_WORKERS="$WORKERS"
    export BENCH_MIN_WORKERS="$MIN_WORKERS"
    export BENCH_MAX_WORKERS="$MAX_WORKERS"
    export BENCH_BALANCE_COOLDOWN="$BALANCE_COOLDOWN"
    export BENCH_BALANCE_MAX_SHIFT="$BALANCE_MAX_SHIFT"
    export BENCH_STRATEGY="$SCALING_STRATEGY"
    export BENCH_TARGET_JOBS_PER_PROCESS="$TARGET_JOBS_PER_PROCESS"
    export BENCH_TARGET_CLEAR_SECONDS="$TARGET_CLEAR_SECONDS"
    export BENCH_DISPATCH_MODE="$DISPATCH_MODE"
    # A timed lane must not inherit feature-probe toggles, even if a caller
    # mutates its environment between repetitions.
    export BENCH_QUEUES=''
    export BENCH_FAILED_DRIVER='null'
    export BENCH_LEASE_RENEWAL="$LEASE_RENEWAL"
    if [ "$engine" = "horizon" ]; then
        export BENCH_CONNECTION="redis"
    else
        export BENCH_CONNECTION="queen"
    fi

    printf '\n[%02d] %s / %s / %s\n' "$lane_number" "$engine" "$profile" "$repetition_label"
    docker volume create \
        --label "queen.benchmark.campaign=${campaign_id}" \
        --label "queen.benchmark.run=${run_id}" \
        --label "com.docker.compose.project=${CURRENT_PROJECT}" \
        --label "com.docker.compose.volume=results" \
        "$CURRENT_VOLUME" >/dev/null
    docker volume create \
        --label "queen.benchmark.campaign=${campaign_id}" \
        --label "queen.benchmark.run=${run_id}" \
        "$CURRENT_STATS_VOLUME" >/dev/null
    docker run --rm --user 0:0 \
        --mount "type=volume,src=${CURRENT_VOLUME},dst=/results" \
        "$APP_IMAGE" sh -ceu 'chown 1000:1000 /results; chmod 0770 /results'
    docker run --rm --user 0:0 \
        --mount "type=volume,src=${CURRENT_STATS_VOLUME},dst=/stats" \
        "$APP_IMAGE" sh -ceu 'chown 1000:1000 /stats; chmod 0700 /stats'

    compose_current config >"${CURRENT_HOST_RUN}/compose-resolved.yml"
    compose_current up --detach --no-build "$engine" producer

    app_id="$(compose_current ps --quiet "$engine")"
    [ -n "$app_id" ] || die "unable to resolve $engine container"
    producer php artisan bench:config --no-ansi >"${CURRENT_HOST_RUN}/configuration.json"
    app_pid="$(docker inspect --format '{{.State.Pid}}' "$app_id")"
    [ "$app_pid" -gt 0 ] || die "$engine has no host PID"

    SAMPLER_TARGETS=(--target "app=${app_pid}")
    if [ "$engine" = "horizon" ]; then
        backend_id="$(compose_current ps --quiet redis)"
        backend_pid="$(docker inspect --format '{{.State.Pid}}' "$backend_id")"
        SAMPLER_TARGETS+=(--target "backend-redis=${backend_pid}")
    else
        broker_id="$(compose_current ps --quiet broker)"
        postgres_id="$(compose_current ps --quiet postgres)"
        broker_pid="$(docker inspect --format '{{.State.Pid}}' "$broker_id")"
        postgres_pid="$(docker inspect --format '{{.State.Pid}}' "$postgres_id")"
        SAMPLER_TARGETS+=(--target "backend-broker=${broker_pid}")
        SAMPLER_TARGETS+=(--target "backend-postgres=${postgres_pid}")
    fi

    sampler_output="/stats/${run_id}.jsonl"
    sampler_ready="/stats/${run_id}.ready"
    SAMPLER_OPTIONS=(
        --output "$sampler_output"
        --ready-file "$sampler_ready"
        --interval "$SAMPLE_INTERVAL"
    )
    if [ "$INCLUDE_PSS" -eq 1 ]; then
        SAMPLER_OPTIONS+=(--pss)
    fi

    docker run --detach --rm \
        --name "$CURRENT_MONITOR" \
        --pid host \
        --cgroupns host \
        --user 1000:1000 \
        --cap-drop ALL \
        --security-opt no-new-privileges:true \
        --read-only \
        --mount type=bind,src=/sys/fs/cgroup,dst=/sys/fs/cgroup,readonly \
        --mount "type=bind,src=${SCRIPT_DIR},dst=/bench,readonly" \
        --mount "type=volume,src=${CURRENT_STATS_VOLUME},dst=/stats" \
        "$APP_IMAGE" \
        python3 /bench/sample.py \
        "${SAMPLER_TARGETS[@]}" \
        "${SAMPLER_OPTIONS[@]}" >/dev/null
    wait_for_sampler "$CURRENT_MONITOR" "$sampler_ready"
    wait_for_health "$app_id" 180 "$engine"

    if [ "$WARMUP_JOBS" -gt 0 ]; then
        # A size-based auto run must start from its configured minimum. A
        # warm-up would pre-scale the pool and invalidate time-to-peak. The
        # time strategy does need runtime observations, so it warms and then
        # waits long enough for bounded downshifts to return to minimum.
        if [ "$profile" = "fixed" ] || [ "$SCALING_STRATEGY" = "time" ]; then
            warmup_id="${run_id}-warmup"
            producer php artisan bench:dispatch --no-ansi \
                --run-id="$warmup_id" \
                --jobs="$WARMUP_JOBS" \
                --sleep-ms="$SLEEP_MS" \
                --cpu-iterations="$CPU_ITERATIONS" \
                --dispatch-mode="$DISPATCH_MODE" >/dev/null
            producer php artisan bench:results --no-ansi "$warmup_id" \
                --expected="$WARMUP_JOBS" --wait="$WAIT_TIMEOUT" --poll-ms=500 >/dev/null
            if [ "$profile" = "auto" ]; then
                sleep $(( BALANCE_COOLDOWN * (MAX_WORKERS - MIN_WORKERS) + 2 ))
            else
                sleep 1
            fi
        fi
    fi

    # Snapshot again immediately before the measured dispatch. Compose lane
    # containers and the named sampler are the only permitted workloads.
    capture_container_isolation pre-dispatch
    start_continuous_isolation

    producer php artisan bench:dispatch --no-ansi \
        --run-id="$run_id" \
        --jobs="$JOBS" \
        --sleep-ms="$SLEEP_MS" \
        --cpu-iterations="$CPU_ITERATIONS" \
        --dispatch-mode="$DISPATCH_MODE" >"${CURRENT_HOST_RUN}/dispatch-command.json"

    set +e
    producer php artisan bench:results --no-ansi "$run_id" \
        --expected="$JOBS" --wait="$WAIT_TIMEOUT" --poll-ms=500 \
        >"${CURRENT_HOST_RUN}/result-check.json" \
        2>"${CURRENT_HOST_RUN}/result-check.stderr.log"
    completion_status=$?
    set -e

    if [ "$POST_DRAIN_SECONDS" = "auto" ]; then
        if [ "$profile" = "auto" ]; then
            post_drain=$(( BALANCE_COOLDOWN * (MAX_WORKERS - MIN_WORKERS) + 2 ))
        else
            post_drain=2
        fi
    else
        post_drain="$POST_DRAIN_SECONDS"
    fi
    if [ "$post_drain" -gt 0 ]; then
        sleep "$post_drain"
    fi

    # Freeze resource evidence before the correctness probe. The probe can be
    # richer (and therefore costlier) on Queen than on Redis; sampling it would
    # turn the observer itself into a backend CPU result.
    docker stop --time 10 "$CURRENT_MONITOR" >/dev/null
    CURRENT_MONITOR=""
    if stop_continuous_isolation; then
        isolation_watch_status=0
    else
        isolation_watch_status=$?
    fi

    # A completion record is written before Laravel deletes/acknowledges the
    # job. Require the backend to remain empty for one second so a lane cannot
    # pass with a live reservation, delayed retry or ready job left behind.
    # Probe only after the measured post-drain window: queue introspection is
    # instrumentation and must not bias backend CPU against richer drivers.
    if [ "$completion_status" -eq 0 ]; then
        quiescence_wait="$WAIT_TIMEOUT"
        quiescence_settle_ms=1000
    else
        # Preserve a diagnostic snapshot without adding another full timeout
        # to a run which has already failed its completion gate.
        quiescence_wait=0
        quiescence_settle_ms=0
    fi
    set +e
    producer php artisan bench:queue-state --no-ansi \
        --run-id="$run_id" \
        --connection="$BENCH_CONNECTION" \
        --queue="$BENCH_QUEUE" \
        --wait="$quiescence_wait" \
        --poll-ms=100 \
        --settle-ms="$quiescence_settle_ms" \
        >"${CURRENT_HOST_RUN}/queue-state.final.json"
    quiescence_status=$?
    set -e

    if [ "$LEDGER_MODE" = durable ]; then
        producer php artisan bench:ledger-checkpoint --no-ansi "$run_id" \
            >"${CURRENT_HOST_RUN}/ledger-checkpoint.json"
    fi

    docker run --rm --user 0:0 \
        --mount "type=volume,src=${CURRENT_STATS_VOLUME},dst=/from,readonly" \
        --mount "type=bind,src=${CURRENT_HOST_RUN},dst=/to" \
        "$APP_IMAGE" sh -ceu 'cp "/from/$1" /to/stats.jsonl' sh "${run_id}.jsonl"

    capture_lane_diagnostics
    docker run --rm --user 0:0 \
        --mount "type=volume,src=${CURRENT_VOLUME},dst=/from,readonly" \
        --mount "type=bind,src=${CURRENT_HOST_RUN},dst=/to" \
        "$APP_IMAGE" sh -ceu 'cp -a "/from/$1/." /to/' sh "$run_id"

    # Detect a workload that appeared while the lane was running. This is
    # outside resource sampling, but still a validity gate for the sample.
    capture_container_isolation post-measurement

    set +e
    python3 "${SCRIPT_DIR}/analyze.py" summarize "$CURRENT_HOST_RUN" \
        --output "${CURRENT_HOST_RUN}/summary.json"
    analysis_status=$?
    set -e
    REPORT_INPUTS+=("${engine}-${profile}-${repetition_label}=${CURRENT_HOST_RUN}")

    completed_host_run="$CURRENT_HOST_RUN"
    finish_lane
    if [ "$completion_status" -ne 0 ]; then
        die "$run_id did not complete all $JOBS jobs; inspect ${completed_host_run}"
    fi
    if [ "$quiescence_status" -ne 0 ]; then
        die "$run_id did not reach queue quiescence; inspect ${completed_host_run}/queue-state.final.json"
    fi
    if [ "$analysis_status" -ne 0 ]; then
        die "$run_id failed analysis; inspect ${completed_host_run}/summary.json"
    fi
    if [ "$isolation_watch_status" -ne 0 ]; then
        die "$run_id overlapped foreign container activity or lost its Docker event watch; inspect ${completed_host_run}/container-isolation.measurement.json"
    fi
}

for (( repetition = 1; repetition <= RUNS; repetition++ )); do
    for (( profile_index = 0; profile_index < ${#PROFILES[@]}; profile_index++ )); do
        profile="${PROFILES[$profile_index]}"
        rotation=$(( ((repetition - 1) * ${#PROFILES[@]} + profile_index) % ${#ENGINES[@]} ))
        for (( engine_index = 0; engine_index < ${#ENGINES[@]}; engine_index++ )); do
            rotated_index=$(( (engine_index + rotation) % ${#ENGINES[@]} ))
            run_lane "${ENGINES[$rotated_index]}" "$profile" "$repetition"
        done
    done
done

mkdir -p "${campaign_dir}/reports"
for (( repetition = 1; repetition <= RUNS; repetition++ )); do
    repetition_label="$(printf 'r%02d' "$repetition")"
    for profile in "${PROFILES[@]}"; do
        CELL_INPUTS=()
        for engine in "${ENGINES[@]}"; do
            cell_directory="${campaign_dir}/${engine}/${profile}/${repetition_label}"
            if [ -d "$cell_directory" ]; then
                CELL_INPUTS+=("${engine}=${cell_directory}")
            fi
        done
        python3 "${SCRIPT_DIR}/analyze.py" report "${CELL_INPUTS[@]}" \
            --output "${campaign_dir}/reports/${profile}-${repetition_label}.md" \
            --json-output "${campaign_dir}/reports/${profile}-${repetition_label}.json"
    done
done

python3 "${SCRIPT_DIR}/analyze.py" report "${REPORT_INPUTS[@]}" --no-comparisons \
    --output "${campaign_dir}/report.md" \
    --json-output "${campaign_dir}/report.json"

trap - EXIT INT TERM
printf '\nBenchmark complete: %s\n' "$campaign_dir"
printf 'Report: %s\n' "${campaign_dir}/report.md"

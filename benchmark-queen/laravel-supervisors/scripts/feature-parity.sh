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
QUEUES_CSV="high,default,low"
QUEUE_COUNTS_CSV="60,30,10"
JOBS_PER_QUEUE=""
QUEUE_COUNTS_EXPLICIT=0
JOBS_PER_QUEUE_EXPLICIT=0
WORKERS=3
SLEEP_MS=5
WAIT_TIMEOUT=180
SETTLE_MS=1000
WORKER_TIMEOUT=30
RETRY_AFTER=60
BUILD_IMAGES=1
DRY_RUN=0
OUTPUT_DIRECTORY=""

ACTIVE_PROJECT=""
ACTIVE_ENGINE=""
ACTIVE_VOLUME=""
ACTIVE_LANE_DIRECTORY=""
ACTIVE_MULTI_RUN=""
ACTIVE_FAILURE_RUN=""

usage() {
    cat <<'EOF'
Usage: scripts/feature-parity.sh --output DIRECTORY [options]

Run isolated Docker feature-parity diagnostics. This is a correctness smoke
test, not a throughput or resource benchmark.

Options:
  --output DIRECTORY        Required, new or empty artifact directory
  --engines CSV             Subset of horizon,queen-php,queen-rust
  --queues CSV              At least two strict queue names (default: high,default,low)
  --queue-counts CSV        Ordered positive counts (default: 60,30,10)
  --jobs-per-queue N        Compatibility shorthand for equal per-queue counts
  --workers N               Fixed worker pool, at least the queue count (default: 3)
  --sleep-ms N              Runtime of every multi-queue job (default: 5)
  --timeout SECONDS         Deadline for each wait gate (default: 180)
  --settle-ms N             Continuous empty interval per queue (default: 1000)
  --no-build                Reuse local benchmark images
  --dry-run                 Validate and write protocol metadata without Docker
  -h, --help                Show this help

Every engine gets a unique Compose project, backend and result volume. The
default workload is a deterministic 60%/30%/10% weighted round-robin across
high/default/low. Queen PHP
and Queen Rust additionally exercise failed row + broker DLQ -> queue:retry ->
successful completion -> empty failed store + empty DLQ.
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

compare_worker_snapshots() {
    local baseline="$1"
    local final="$2"
    local engine="$3"
    local expected="$4"
    local output="$5"
    python3 - "$baseline" "$final" "$engine" "$expected" "$output" <<'PY'
import json
import sys
from pathlib import Path

baseline_path, final_path, engine, expected_raw, output_path = sys.argv[1:]
expected = int(expected_raw)
errors = []

def load_snapshot(path: str, label: str):
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        errors.append(f"{label} snapshot is unreadable: {error}")
        return {}
    if not isinstance(value, dict):
        errors.append(f"{label} snapshot is not a JSON object")
        return {}
    if value.get("schema") != "queen.laravel-supervisors.worker-snapshot/v1":
        errors.append(f"{label} snapshot has an unexpected schema")
    if value.get("engine") != engine:
        errors.append(f"{label} snapshot engine does not match {engine}")
    return value

def identities(snapshot, label: str):
    workers = snapshot.get("workers")
    if not isinstance(workers, list):
        errors.append(f"{label} workers is not an array")
        return [], 0
    identities = []
    for index, worker in enumerate(workers):
        if not isinstance(worker, dict):
            errors.append(f"{label} worker {index} is not an object")
            continue
        pid = worker.get("pid")
        start_ticks = worker.get("start_ticks")
        state = worker.get("state")
        if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0:
            errors.append(f"{label} worker {index} has an invalid pid")
            continue
        if not isinstance(start_ticks, int) or isinstance(start_ticks, bool) or start_ticks <= 0:
            errors.append(f"{label} worker {index} has invalid start_ticks")
            continue
        if not isinstance(state, str) or len(state) != 1 or state in {"X", "Z"}:
            errors.append(f"{label} worker {pid} is not live")
            continue
        identities.append((pid, start_ticks))
    if len(workers) != expected:
        errors.append(f"{label} worker count is {len(workers)}, expected {expected}")
    if len(set(identities)) != len(identities):
        errors.append(f"{label} contains duplicate worker identities")
    return sorted(set(identities)), len(workers)

baseline = load_snapshot(baseline_path, "baseline")
final = load_snapshot(final_path, "final")
baseline_identities, baseline_count = identities(baseline, "baseline")
final_identities, final_count = identities(final, "final")
stable = baseline_identities == final_identities
if not stable:
    errors.append("worker identity set changed between baseline and final snapshot")

payload = {
    "schema": "queen.laravel-supervisors.worker-integrity/v1",
    "engine": engine,
    "expected_workers": expected,
    "baseline_count": baseline_count,
    "final_count": final_count,
    "baseline_identities": [
        {"pid": pid, "start_ticks": start_ticks}
        for pid, start_ticks in baseline_identities
    ],
    "final_identities": [
        {"pid": pid, "start_ticks": start_ticks}
        for pid, start_ticks in final_identities
    ],
    "stable_identities": stable,
    "respawn_or_replacement_detected": not stable,
    "errors": errors,
    "scope": (
        "fixed-pool worker identities from the post-health baseline through the "
        "post-workload final snapshot"
    ),
    "passed": errors == [],
}
Path(output_path).write_text(
    json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
)
raise SystemExit(0 if payload["passed"] else 1)
PY
}

# Unit tests exercise the same comparison function without requiring Docker.
# This deliberately undocumented mode does not mutate a benchmark campaign.
if [ "${QUEEN_FEATURE_PARITY_COMPARE_ONLY:-0}" = 1 ]; then
    [ "$#" -eq 5 ] || die "worker snapshot comparison requires baseline, final, engine, expected and output"
    require_command python3
    compare_worker_snapshots "$1" "$2" "$3" "$4" "$5"
    exit $?
fi

while [ "$#" -gt 0 ]; do
    case "$1" in
        --output) OUTPUT_DIRECTORY="${2:?--output requires a value}"; shift 2 ;;
        --engines) ENGINES_CSV="${2:?--engines requires a value}"; shift 2 ;;
        --queues) QUEUES_CSV="${2:?--queues requires a value}"; shift 2 ;;
        --queue-counts)
            QUEUE_COUNTS_CSV="${2:?--queue-counts requires a value}"
            QUEUE_COUNTS_EXPLICIT=1
            shift 2
            ;;
        --jobs-per-queue)
            JOBS_PER_QUEUE="${2:?--jobs-per-queue requires a value}"
            JOBS_PER_QUEUE_EXPLICIT=1
            shift 2
            ;;
        --workers) WORKERS="${2:?--workers requires a value}"; shift 2 ;;
        --sleep-ms) SLEEP_MS="${2:?--sleep-ms requires a value}"; shift 2 ;;
        --timeout) WAIT_TIMEOUT="${2:?--timeout requires a value}"; shift 2 ;;
        --settle-ms) SETTLE_MS="${2:?--settle-ms requires a value}"; shift 2 ;;
        --no-build) BUILD_IMAGES=0; shift ;;
        --dry-run) DRY_RUN=1; shift ;;
        -h|--help) usage; exit 0 ;;
        *) die "unknown option: $1" ;;
    esac
done

[ -n "$OUTPUT_DIRECTORY" ] || die "--output is required"
require_command git
require_command python3
require_positive_int "--workers" "$WORKERS"
require_uint "--sleep-ms" "$SLEEP_MS"
require_positive_int "--timeout" "$WAIT_TIMEOUT"
require_uint "--settle-ms" "$SETTLE_MS"
[ "$WORKERS" -le 256 ] || die "--workers must not exceed 256"
[ "$SLEEP_MS" -le 60000 ] || die "--sleep-ms must not exceed 60000"
[ "$WAIT_TIMEOUT" -le 86400 ] || die "--timeout must not exceed 86400"
[ "$SETTLE_MS" -le 60000 ] || die "--settle-ms must not exceed 60000"

case "$ENGINES_CSV" in
    ''|,*|*,|*,,*) die "--engines must be a non-empty CSV without empty entries" ;;
esac
case "$QUEUES_CSV" in
    ''|,*|*,|*,,*) die "--queues must be a non-empty CSV without empty entries" ;;
esac

OLD_IFS="$IFS"
IFS=',' read -r -a ENGINES <<EOF
${ENGINES_CSV}
EOF
IFS=',' read -r -a QUEUES <<EOF
${QUEUES_CSV}
EOF
IFS="$OLD_IFS"

[ "$QUEUE_COUNTS_EXPLICIT" -eq 0 ] || [ "$JOBS_PER_QUEUE_EXPLICIT" -eq 0 ] \
    || die "--queue-counts and --jobs-per-queue are mutually exclusive"
if [ "$JOBS_PER_QUEUE_EXPLICIT" -eq 1 ]; then
    require_positive_int "--jobs-per-queue" "$JOBS_PER_QUEUE"
    [ "$JOBS_PER_QUEUE" -le 1000000 ] || die "--jobs-per-queue must not exceed 1000000"
    QUEUE_COUNTS_CSV=""
    for _queue in "${QUEUES[@]}"; do
        if [ -n "$QUEUE_COUNTS_CSV" ]; then
            QUEUE_COUNTS_CSV="${QUEUE_COUNTS_CSV},"
        fi
        QUEUE_COUNTS_CSV="${QUEUE_COUNTS_CSV}${JOBS_PER_QUEUE}"
    done
fi
case "$QUEUE_COUNTS_CSV" in
    ''|,*|*,|*,,*) die "--queue-counts must be a non-empty CSV without empty entries" ;;
esac
IFS=',' read -r -a QUEUE_COUNTS <<EOF
${QUEUE_COUNTS_CSV}
EOF
IFS="$OLD_IFS"

[ "${#ENGINES[@]}" -gt 0 ] || die "at least one engine is required"
[ "${#QUEUES[@]}" -ge 2 ] || die "--queues requires at least two queues"
[ "${#QUEUES[@]}" -le 256 ] || die "--queues must not contain more than 256 queues"
[ "${#QUEUE_COUNTS[@]}" -eq "${#QUEUES[@]}" ] \
    || die "--queue-counts must contain exactly one count for every queue"
[ "$WORKERS" -ge "${#QUEUES[@]}" ] || die "--workers must cover every configured queue"

TOTAL_JOBS=0
for queue_count in "${QUEUE_COUNTS[@]}"; do
    require_positive_int "--queue-counts entry" "$queue_count"
    [ "$queue_count" -le 1000000 ] || die "--queue-counts entries must not exceed 1000000"
    TOTAL_JOBS=$(( TOTAL_JOBS + queue_count ))
    [ "$TOTAL_JOBS" -le 1000000 ] \
        || die "the multi-queue dispatch must not exceed 1000000 total jobs"
done

contains_queen=0
seen_engines="|"
for engine in "${ENGINES[@]}"; do
    case "$engine" in
        horizon) ;;
        queen-php|queen-rust) contains_queen=1 ;;
        *) die "unknown engine in --engines: $engine" ;;
    esac
    case "$seen_engines" in
        *"|${engine}|"*) die "duplicate engine in --engines: $engine" ;;
    esac
    seen_engines="${seen_engines}${engine}|"
done

seen_queues="|"
for queue in "${QUEUES[@]}"; do
    # Multi-queue job IDs append ':' plus a nine-digit sequence. Keeping the
    # queue at 118 bytes preserves the fixture's 128-byte job-ID ceiling.
    if [ "${#queue}" -gt 118 ] || ! [[ "$queue" =~ ^[A-Za-z0-9._:-]+$ ]]; then
        die "invalid queue name [$queue]; use 1..118 ASCII letters, digits, dot, underscore, colon or dash"
    fi
    case "$seen_queues" in
        *"|${queue}|"*) die "duplicate queue in --queues: $queue" ;;
    esac
    seen_queues="${seen_queues}${queue}|"
done

mkdir -p "$OUTPUT_DIRECTORY"
OUTPUT_DIRECTORY="$(CDPATH='' cd -- "$OUTPUT_DIRECTORY" && pwd)"
if [ -n "$(find "$OUTPUT_DIRECTORY" -mindepth 1 -maxdepth 1 -print -quit)" ]; then
    die "--output must be empty: $OUTPUT_DIRECTORY"
fi

campaign_stamp="$(date -u +%Y%m%dT%H%M%SZ)"
campaign_token="$(python3 -c 'import secrets; print(secrets.token_hex(4))')"
git_short="$(git -C "$REPOSITORY_ROOT" rev-parse --short=10 HEAD)"
campaign_id="feature-${campaign_stamp}-${git_short}-${campaign_token}"

python3 - "$OUTPUT_DIRECTORY" "$campaign_id" "$REPOSITORY_ROOT" "$ENGINES_CSV" \
    "$QUEUES_CSV" "$QUEUE_COUNTS_CSV" "$WORKERS" "$SLEEP_MS" "$WAIT_TIMEOUT" \
    "$SETTLE_MS" "$BUILD_IMAGES" "$DRY_RUN" <<'PY'
import datetime as dt
import json
import platform
import subprocess
import sys
from pathlib import Path

(
    output, campaign_id, repository, engines, queues, queue_counts, workers,
    sleep_ms, timeout, settle_ms, build_images, dry_run,
) = sys.argv[1:]

def command(*args: str) -> str:
    return subprocess.check_output(args, text=True, stderr=subprocess.DEVNULL).strip()

engine_list = engines.split(",")
queue_list = queues.split(",")
counts = [int(value) for value in queue_counts.split(",")]
jobs_by_queue = dict(zip(queue_list, counts, strict=True))
equal_count = counts[0] if len(set(counts)) == 1 else None
payload = {
    "schema": "queen.laravel-supervisors.feature-parity-protocol/v1",
    "qualification": "diagnostic_feature_smoke",
    "performance_comparable": False,
    "campaign_id": campaign_id,
    "created_at": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
    "git": {
        "commit": command("git", "-C", repository, "rev-parse", "HEAD"),
        "branch": command("git", "-C", repository, "branch", "--show-current"),
        "dirty": bool(command("git", "-C", repository, "status", "--porcelain")),
    },
    "host": {"platform": platform.platform(), "machine": platform.machine()},
    "settings": {
        "engines": engine_list,
        "queues": queue_list,
        "queue_counts": counts,
        "jobs_by_queue": jobs_by_queue,
        "jobs_per_queue": equal_count,
        "total_jobs_per_lane": sum(counts),
        "workers": int(workers),
        "sleep_ms": int(sleep_ms),
        "wait_timeout_seconds": int(timeout),
        "queue_settle_ms": int(settle_ms),
        "bench_queues_csv": queues,
        "failed_driver_by_engine": {
            name: "file" if name.startswith("queen-") else "null"
            for name in engine_list
        },
        "lease_renewal": False,
        "build_images": build_images == "1",
        "dry_run": dry_run == "1",
    },
    "scenarios": {
        "multi_queue": engine_list,
        "failed_job_lifecycle": [name for name in engine_list if name.startswith("queen-")],
    },
    "method": {
        "isolation": "fresh Compose project, backend and named result volume per engine",
        "multi_queue": (
            "deterministic weighted round-robin dispatch; exact job-set and queue identity; "
            "continuous quiescence gate on every queue"
        ),
        "failed_job": (
            "wait for one Laravel file-store row and matching Queen DLQ snapshot; queue:retry; "
            "exact completion; empty failed store and empty DLQ"
        ),
        "failure_policy": (
            "fail closed on command errors, timeouts, residual queue state, container restart/OOM, "
            "or fixed-pool worker identity replacement between baseline and final snapshot"
        ),
        "sterilized_environment": {
            "BENCH_QUEUES": queues,
            "BENCH_FAILED_DRIVER": "file for Queen lanes; null for Horizon",
            "BENCH_LEASE_RENEWAL": "false",
            "BENCH_LEASE_RENEWAL_INTERVAL": "",
        },
    },
    "artifact_policy": {
        "secrets": "environment and resolved Compose configuration are never captured",
        "dlq": "payload and exception bodies are redacted; only fixture identifiers are retained",
        "logs": "only isolated fixture container logs are retained",
    },
    "known_limits": [
        "This is a functional Docker smoke test, not a performance measurement.",
        "A passing smoke run does not estimate failure probability or prove exactly-once effects.",
        "The file failed store is a fixture-specific diagnostic backend.",
        "Worker integrity covers the interval after the post-health baseline and before the post-workload final snapshot.",
    ],
}
Path(output, "metadata.json").write_text(
    json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
)
PY

if [ "$DRY_RUN" -eq 1 ]; then
    printf 'Dry-run protocol written to %s/metadata.json\n' "$OUTPUT_DIRECTORY"
    exit 0
fi

require_command docker
docker info >/dev/null 2>&1 || die "Docker daemon is unavailable"
docker compose version >/dev/null 2>&1 || die "Docker Compose v2 is unavailable"

image_id() {
    docker image inspect "$1" --format '{{.Id}}' 2>/dev/null || true
}

if [ "$BUILD_IMAGES" -eq 1 ]; then
    printf 'Building feature-test application image...\n'
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

app_image_id="$(image_id "$APP_IMAGE")"
broker_image_id=""
if [ "$contains_queen" -eq 1 ]; then
    broker_image_id="$(image_id "$BROKER_IMAGE")"
fi
python3 - "$OUTPUT_DIRECTORY" "$APP_IMAGE" "$app_image_id" "$BROKER_IMAGE" "$broker_image_id" <<'PY'
import json
import sys
from pathlib import Path

output, app_name, app_id, broker_name, broker_id = sys.argv[1:]
images = [{"name": app_name, "id": app_id}]
if broker_id:
    images.append({"name": broker_name, "id": broker_id})
Path(output, "images.json").write_text(
    json.dumps({"schema": "queen.laravel-supervisors.images/v1", "images": images}, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)
PY

compose_active() {
    docker compose \
        --file "$COMPOSE_FILE" \
        --project-name "$ACTIVE_PROJECT" \
        --profile "$ACTIVE_ENGINE" \
        --profile tools \
        "$@"
}

producer() {
    compose_active run --rm --no-deps --no-TTY producer "$@"
}

wait_for_health() {
    local container_id="$1"
    local timeout="$2"
    local label="$3"
    local deadline
    local state
    deadline=$(( $(date +%s) + timeout ))
    while :; do
        state="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$container_id" 2>/dev/null || true)"
        case "$state" in
            healthy|running) return 0 ;;
            unhealthy|exited|dead) die "$label entered state: $state" ;;
        esac
        [ "$(date +%s)" -lt "$deadline" ] || die "timed out waiting for $label health"
        sleep 1
    done
}

verify_multi_completion() {
    local run_id="$1"
    local connection="$2"
    # This is PHP source. Keep it single-quoted so the shell cannot expand PHP variables.
    # shellcheck disable=SC2016
    producer php -r '
require "vendor/autoload.php";
$app = require "bootstrap/app.php";
$kernel = $app->make(Illuminate\Contracts\Console\Kernel::class);
$kernel->bootstrap();
$runId = $argv[1];
$connection = $argv[2];
$queues = explode(",", $argv[3]);
$queueCounts = array_map("intval", explode(",", $argv[4]));
$jobsByQueue = array_combine($queues, $queueCounts);
if ($jobsByQueue === false || count($queueCounts) !== count($queues)) {
    fwrite(STDERR, "queue/count cardinality mismatch\n");
    exit(2);
}
$expected = [];
$perQueue = [];
foreach ($jobsByQueue as $queue => $queueCount) {
    $perQueue[$queue] = ["expected" => $queueCount, "unique" => 0, "records" => 0];
    for ($index = 0; $index < $queueCount; ++$index) {
        $expected[$queue.":".sprintf("%09d", $index)] = $queue;
    }
}
$seen = [];
$unexpected = [];
$identityMismatches = [];
$records = 0;
$sink = $app->make(App\Support\JsonlResultSink::class);
foreach ($sink->stream($sink->snapshot($runId)) as $record) {
    if (($record["run_id"] ?? null) !== $runId) {
        continue;
    }
    ++$records;
    $jobId = $record["job_id"] ?? null;
    if (!is_string($jobId) || !isset($expected[$jobId])) {
        $unexpected[] = is_string($jobId) ? $jobId : "<invalid>";
        continue;
    }
    $queue = $expected[$jobId];
    ++$perQueue[$queue]["records"];
    $seen[$jobId] = ($seen[$jobId] ?? 0) + 1;
    if (($record["queue"] ?? null) !== $queue || ($record["connection"] ?? null) !== $connection) {
        $identityMismatches[] = $jobId;
    }
}
$missing = [];
$duplicates = [];
foreach ($expected as $jobId => $queue) {
    $count = $seen[$jobId] ?? 0;
    if ($count === 0) {
        $missing[] = $jobId;
    } else {
        ++$perQueue[$queue]["unique"];
        if ($count > 1) {
            $duplicates[$jobId] = $count;
        }
    }
}
sort($missing);
sort($unexpected);
sort($identityMismatches);
ksort($duplicates);
$starved = [];
foreach ($perQueue as $queue => $counts) {
    if ($counts["expected"] > 0 && $counts["unique"] === 0) {
        $starved[] = $queue;
    }
}
$passed = $missing === [] && $unexpected === [] && $duplicates === []
    && $identityMismatches === [] && $starved === [] && $records === count($expected);
$result = [
    "schema" => "queen.laravel-supervisors.multi-queue-result/v1",
    "run_id" => $runId,
    "connection" => $connection,
    "queues" => $queues,
    "jobs_by_queue" => $jobsByQueue,
    "expected" => count($expected),
    "records" => $records,
    "per_queue" => $perQueue,
    "missing" => $missing,
    "unexpected" => $unexpected,
    "duplicates" => $duplicates,
    "identity_mismatches" => $identityMismatches,
    "starved_queues" => $starved,
    "passed" => $passed,
];
echo json_encode($result, JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES), PHP_EOL;
exit($passed ? 0 : 1);
' "$run_id" "$connection" "$QUEUES_CSV" "$QUEUE_COUNTS_CSV"
}

lifecycle_snapshot() {
    local expected_state="$1"
    local queue="$2"
    local group="$3"
    # Redact exception text and job payloads. The artifact retains only fixture
    # identifiers needed to prove the failed-store/DLQ relationship.
    # shellcheck disable=SC2016
    producer php -r '
require "vendor/autoload.php";
$app = require "bootstrap/app.php";
$kernel = $app->make(Illuminate\Contracts\Console\Kernel::class);
$kernel->bootstrap();
$mode = $argv[1];
$queue = $argv[2];
$group = $argv[3];
$value = static function (mixed $record, string $key): mixed {
    if (is_array($record)) {
        return $record[$key] ?? null;
    }
    return is_object($record) ? ($record->{$key} ?? null) : null;
};
$failed = [];
foreach ($app["queue.failer"]->all() as $record) {
    $payloadRaw = $value($record, "payload");
    $payload = is_string($payloadRaw) ? json_decode($payloadRaw, true) : null;
    $queen = is_array($payload) && is_array($payload["_queen"] ?? null) ? $payload["_queen"] : [];
    $source = is_array($queen["failed_source"] ?? null) ? $queen["failed_source"] : [];
    $failed[] = [
        "id" => $value($record, "id"),
        "connection" => $value($record, "connection"),
        "queue" => $value($record, "queue"),
        "failed_at" => $value($record, "failed_at"),
        "payload_uuid" => is_array($payload) ? ($payload["uuid"] ?? null) : null,
        "has_manual_retry" => is_string($queen["manual_retry"] ?? null) && $queen["manual_retry"] !== "",
        "source" => [
            "partition_id" => $source["partition_id"] ?? null,
            "transaction_id" => $source["transaction_id"] ?? null,
        ],
    ];
}
usort($failed, static fn (array $left, array $right): int => strcmp((string) $left["id"], (string) $right["id"]));
$queenConnection = $app["queue"]->connection("queen");
if (!$queenConnection instanceof Queen\Laravel\Queue\QueenQueue) {
    throw new RuntimeException("The Queen failed-lifecycle probe resolved an unexpected queue driver.");
}
$queenClient = $queenConnection->getQueen();
$dlqRaw = $queenClient->queue($queue)->dlq($group)->limit(1000)->get();
$dlq = [];
foreach (($dlqRaw["messages"] ?? []) as $message) {
    if (!is_array($message)) {
        continue;
    }
    $dlq[] = [
        "id" => $message["id"] ?? null,
        "transaction_id" => $message["transactionId"] ?? null,
        "partition_id" => $message["partitionId"] ?? null,
        "queue" => $message["queue"] ?? null,
        "consumer_group" => $message["consumerGroup"] ?? null,
        "retry_count" => $message["retryCount"] ?? null,
        "failed_at" => $message["failedAt"] ?? null,
    ];
}
usort($dlq, static fn (array $left, array $right): int => strcmp((string) $left["transaction_id"], (string) $right["transaction_id"]));
$sourceMatched = false;
if (count($failed) === 1 && count($dlq) === 1) {
    $sourceMatched = $failed[0]["source"]["partition_id"] === $dlq[0]["partition_id"]
        && $failed[0]["source"]["transaction_id"] === $dlq[0]["transaction_id"];
}
$present = count($failed) === 1 && count($dlq) === 1
    && $failed[0]["connection"] === "queen"
    && $failed[0]["queue"] === $queue
    && $failed[0]["id"] === $failed[0]["payload_uuid"]
    && $failed[0]["has_manual_retry"] === true
    && is_string($failed[0]["source"]["partition_id"])
    && $failed[0]["source"]["partition_id"] !== ""
    && is_string($failed[0]["source"]["transaction_id"])
    && $failed[0]["source"]["transaction_id"] !== ""
    && $dlq[0]["queue"] === $queue
    && $dlq[0]["consumer_group"] === $group
    && $sourceMatched;
$empty = $failed === [] && $dlq === [];
$passed = ($mode === "present" && $present) || ($mode === "empty" && $empty);
$result = [
    "schema" => "queen.laravel-supervisors.failed-lifecycle-state/v1",
    "expected_state" => $mode,
    "queue" => $queue,
    "consumer_group" => $group,
    "failed_count" => count($failed),
    "failed" => $failed,
    "dlq_count" => count($dlq),
    "dlq" => $dlq,
    "failed_source_matches_dlq" => $sourceMatched,
    "passed" => $passed,
];
echo json_encode($result, JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES), PHP_EOL;
exit($passed ? 0 : 1);
' "$expected_state" "$queue" "$group"
}

wait_for_lifecycle_state() {
    local expected_state="$1"
    local queue="$2"
    local group="$3"
    local output="$4"
    local error_log="$5"
    local deadline
    local status
    deadline=$(( $(date +%s) + WAIT_TIMEOUT ))
    while :; do
        set +e
        lifecycle_snapshot "$expected_state" "$queue" "$group" >"${output}.tmp" 2>>"$error_log"
        status=$?
        set -e
        if [ "$status" -eq 0 ]; then
            mv "${output}.tmp" "$output"
            return 0
        fi
        if [ "$(date +%s)" -ge "$deadline" ]; then
            mv "${output}.tmp" "$output"
            return 1
        fi
        sleep 0.2
    done
}

verify_failure_completion() {
    local run_id="$1"
    local probe_id="$2"
    local queue="$3"
    # shellcheck disable=SC2016
    producer php -r '
require "vendor/autoload.php";
$app = require "bootstrap/app.php";
$kernel = $app->make(Illuminate\Contracts\Console\Kernel::class);
$kernel->bootstrap();
$runId = $argv[1];
$expectedId = "failure-probe:".$argv[2];
$expectedQueue = $argv[3];
$records = [];
$sink = $app->make(App\Support\JsonlResultSink::class);
foreach ($sink->stream($sink->snapshot($runId)) as $record) {
    if (($record["run_id"] ?? null) === $runId) {
        $records[] = [
            "job_id" => $record["job_id"] ?? null,
            "connection" => $record["connection"] ?? null,
            "queue" => $record["queue"] ?? null,
            "attempt" => $record["attempt"] ?? null,
        ];
    }
}
$passed = count($records) === 1
    && $records[0]["job_id"] === $expectedId
    && $records[0]["connection"] === "queen"
    && $records[0]["queue"] === $expectedQueue;
$result = [
    "schema" => "queen.laravel-supervisors.failed-completion/v1",
    "run_id" => $runId,
    "expected_job_id" => $expectedId,
    "records" => $records,
    "passed" => $passed,
];
echo json_encode($result, JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES), PHP_EOL;
exit($passed ? 0 : 1);
' "$run_id" "$probe_id" "$queue"
}

capture_worker_snapshot() {
    local container_id="$1"
    local engine="$2"
    local output="$3"
    local temporary="${output}.tmp"
    mkdir -p "$(dirname -- "$output")"
    if ! docker exec --interactive "$container_id" python3 - "$engine" >"$temporary" <<'PY'
import datetime as dt
import json
import os
import sys
from pathlib import Path

engine = sys.argv[1]
if engine not in {"horizon", "queen-php", "queen-rust"}:
    raise SystemExit(f"unsupported worker snapshot engine: {engine}")

processes = []
for process_directory in Path("/proc").iterdir():
    if not process_directory.name.isdigit():
        continue
    try:
        raw_stat = (process_directory / "stat").read_text(encoding="utf-8")
        _, separator, suffix = raw_stat.rpartition(")")
        if separator == "" or not suffix.startswith(" "):
            continue
        fields = suffix[1:].split()
        if len(fields) < 20:
            continue
        raw_cmdline = (process_directory / "cmdline").read_bytes()
        arguments = [
            part.decode("utf-8", errors="replace")
            for part in raw_cmdline.split(b"\0")
            if part
        ]
        if not arguments:
            continue
        try:
            executable = os.path.basename(os.readlink(process_directory / "exe"))
        except OSError:
            executable = os.path.basename(arguments[0])
        artisan_command = None
        if executable.startswith("php"):
            for index, argument in enumerate(arguments[:-1]):
                if os.path.basename(argument) == "artisan":
                    artisan_command = arguments[index + 1]
                    break
        role = None
        command = None
        if engine == "horizon" and artisan_command == "horizon:work":
            role = "worker"
            command = artisan_command
        elif engine.startswith("queen-") and artisan_command == "queue:work":
            role = "worker"
            command = artisan_command
        elif engine == "horizon" and artisan_command in {"horizon", "horizon:supervisor"}:
            role = "orchestrator"
            command = artisan_command
        elif engine == "queen-php" and artisan_command == "queen:supervise":
            role = "orchestrator"
            command = artisan_command
        elif engine == "queen-rust" and executable == "queen-supervisor":
            role = "orchestrator"
            command = "queen-supervisor"
        if role is None:
            continue
        processes.append({
            "pid": int(process_directory.name),
            "ppid": int(fields[1]),
            "process_group": int(fields[2]),
            "session": int(fields[3]),
            "state": fields[0],
            "start_ticks": int(fields[19]),
            "role": role,
            "command": command,
        })
    except (FileNotFoundError, ProcessLookupError, PermissionError, OSError, ValueError):
        # A process may legitimately disappear while /proc is being walked.
        continue

processes.sort(key=lambda process: (process["role"], process["pid"]))
payload = {
    "schema": "queen.laravel-supervisors.worker-snapshot/v1",
    "engine": engine,
    "captured_at": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
    "clock_ticks_per_second": os.sysconf("SC_CLK_TCK"),
    "workers": [process for process in processes if process["role"] == "worker"],
    "orchestrators": [process for process in processes if process["role"] == "orchestrator"],
}
print(json.dumps(payload, indent=2, sort_keys=True))
PY
    then
        rm -f "$temporary"
        return 1
    fi
    mv "$temporary" "$output"
}

worker_snapshot_is_ready() {
    local snapshot="$1"
    local engine="$2"
    local expected="$3"
    python3 - "$snapshot" "$engine" "$expected" <<'PY'
import json
import sys
from pathlib import Path

snapshot = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
engine = sys.argv[2]
expected = int(sys.argv[3])
workers = snapshot.get("workers")
ready = (
    snapshot.get("schema") == "queen.laravel-supervisors.worker-snapshot/v1"
    and snapshot.get("engine") == engine
    and isinstance(workers, list)
    and len(workers) == expected
    and len({(worker.get("pid"), worker.get("start_ticks")) for worker in workers}) == expected
    and all(
        isinstance(worker, dict)
        and isinstance(worker.get("pid"), int)
        and not isinstance(worker.get("pid"), bool)
        and worker["pid"] > 0
        and isinstance(worker.get("start_ticks"), int)
        and not isinstance(worker.get("start_ticks"), bool)
        and worker["start_ticks"] > 0
        and isinstance(worker.get("state"), str)
        and len(worker["state"]) == 1
        and worker["state"] not in {"X", "Z"}
        for worker in workers
    )
)
raise SystemExit(0 if ready else 1)
PY
}

wait_for_worker_baseline() {
    local container_id="$1"
    local engine="$2"
    local expected="$3"
    local output="$4"
    local deadline
    deadline=$(( $(date +%s) + WAIT_TIMEOUT ))
    while :; do
        if capture_worker_snapshot "$container_id" "$engine" "$output" \
            && worker_snapshot_is_ready "$output" "$engine" "$expected"; then
            return 0
        fi
        [ "$(date +%s)" -lt "$deadline" ] || return 1
        sleep 0.2
    done
}

capture_failure_worker_snapshot() {
    local app_id
    [ -n "$ACTIVE_ENGINE" ] || return 0
    [ -n "$ACTIVE_LANE_DIRECTORY" ] || return 0
    app_id="$(compose_active ps --quiet "$ACTIVE_ENGINE" 2>/dev/null || true)"
    [ -n "$app_id" ] || return 0
    capture_worker_snapshot "$app_id" "$ACTIVE_ENGINE" \
        "${ACTIVE_LANE_DIRECTORY}/worker-integrity/failure.json" || true
}

capture_container_state() {
    local output="$1"
    local jsonl="${output}.jsonl.tmp"
    local ids
    : >"$jsonl"
    ids="$(compose_active ps --all --quiet 2>/dev/null || true)"
    if [ -n "$ids" ]; then
        while IFS= read -r container_id; do
            [ -n "$container_id" ] || continue
            docker inspect --format \
                '{"id":{{json .Id}},"name":{{json .Name}},"service":{{json (index .Config.Labels "com.docker.compose.service")}},"image_id":{{json .Image}},"status":{{json .State.Status}},"running":{{json .State.Running}},"exit_code":{{.State.ExitCode}},"oom_killed":{{json .State.OOMKilled}},"restart_count":{{.RestartCount}},"health":{{if .State.Health}}{{json .State.Health.Status}}{{else}}null{{end}}}' \
                "$container_id" >>"$jsonl"
        done <<EOF
${ids}
EOF
    fi
    python3 - "$jsonl" "$output" <<'PY'
import json
import sys
from pathlib import Path

source, destination = map(Path, sys.argv[1:])
containers = []
for line in source.read_text(encoding="utf-8").splitlines():
    if line.strip():
        containers.append(json.loads(line))
containers.sort(key=lambda item: (str(item.get("service")), str(item.get("name"))))
passed = bool(containers) and all(
    item.get("running") is True
    and item.get("status") == "running"
    and item.get("exit_code") == 0
    and item.get("oom_killed") is False
    and item.get("restart_count") == 0
    and item.get("health") in (None, "healthy")
    for item in containers
)
payload = {
    "schema": "queen.laravel-supervisors.container-state/v1",
    "containers": containers,
    "passed": passed,
}
destination.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
    rm -f "$jsonl"
}

# Called from the EXIT trap as well as the normal lane path.
# shellcheck disable=SC2329
capture_active_diagnostics() {
    [ -n "$ACTIVE_PROJECT" ] || return 0
    [ -n "$ACTIVE_LANE_DIRECTORY" ] || return 0
    mkdir -p "$ACTIVE_LANE_DIRECTORY"
    compose_active ps --all >"${ACTIVE_LANE_DIRECTORY}/compose-ps.txt" 2>&1 || true
    compose_active logs --no-color --timestamps >"${ACTIVE_LANE_DIRECTORY}/compose.log" 2>&1 || true
    capture_container_state "${ACTIVE_LANE_DIRECTORY}/containers.json" >/dev/null 2>&1 || true
}

# Copy only fixture run directories. The failed-store payload/exception file is
# deliberately excluded even if an interrupted lane still contains a row.
# shellcheck disable=SC2329
salvage_active_results() {
    [ -n "$ACTIVE_VOLUME" ] || return 0
    [ -n "$ACTIVE_LANE_DIRECTORY" ] || return 0
    docker volume inspect "$ACTIVE_VOLUME" >/dev/null 2>&1 || return 0
    docker run --rm --user 0:0 \
        --mount "type=volume,src=${ACTIVE_VOLUME},dst=/from,readonly" \
        --mount "type=bind,src=${ACTIVE_LANE_DIRECTORY},dst=/to" \
        "$APP_IMAGE" sh -ceu '
            mkdir -p /to/raw
            shift
            for run_id in "$@"; do
                [ -n "$run_id" ] || continue
                if [ -d "/from/$run_id" ]; then
                    mkdir -p "/to/raw/$run_id"
                    cp -a "/from/$run_id/." "/to/raw/$run_id/"
                fi
            done
        ' sh feature-copy "$ACTIVE_MULTI_RUN" "$ACTIVE_FAILURE_RUN" >/dev/null 2>&1 || true
}

cleanup_active_lane() {
    local cleanup_status=0
    local project="$ACTIVE_PROJECT"
    local volume="$ACTIVE_VOLUME"
    if [ -n "$ACTIVE_PROJECT" ]; then
        compose_active down --volumes --remove-orphans --timeout 20 >/dev/null 2>&1 || true
    fi
    if [ -n "$ACTIVE_VOLUME" ] && docker volume inspect "$ACTIVE_VOLUME" >/dev/null 2>&1; then
        docker volume rm "$ACTIVE_VOLUME" >/dev/null 2>&1 || true
    fi
    if [ -n "$project" ]; then
        [ -z "$(docker ps --all --quiet --filter "label=com.docker.compose.project=${project}" 2>/dev/null || true)" ] \
            || cleanup_status=1
        docker network inspect "${project}_default" >/dev/null 2>&1 && cleanup_status=1
    fi
    if [ -n "$volume" ]; then
        docker volume inspect "$volume" >/dev/null 2>&1 && cleanup_status=1
    fi
    [ "$cleanup_status" -eq 0 ] || return "$cleanup_status"
    ACTIVE_PROJECT=""
    ACTIVE_ENGINE=""
    ACTIVE_VOLUME=""
    ACTIVE_LANE_DIRECTORY=""
    ACTIVE_MULTI_RUN=""
    ACTIVE_FAILURE_RUN=""
}

# shellcheck disable=SC2329
on_exit() {
    local exit_status=$?
    trap - EXIT INT TERM
    if [ "$exit_status" -ne 0 ]; then
        set +e
        capture_failure_worker_snapshot
        capture_active_diagnostics
        salvage_active_results
        python3 - "$OUTPUT_DIRECTORY" "$ACTIVE_LANE_DIRECTORY" "$exit_status" <<'PY'
import datetime as dt
import json
import sys
from pathlib import Path

output, lane, status = sys.argv[1:]
payload = {
    "schema": "queen.laravel-supervisors.harness-error/v1",
    "exit_status": int(status),
    "captured_at": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
    "message": "Feature-parity harness failed closed; inspect sanitized lane artifacts and logs.",
}
Path(output, "harness-error.json").write_text(
    json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
)
if lane:
    Path(lane, "harness-error.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
PY
        set -e
    fi
    set +e
    cleanup_active_lane
    set -e
    exit "$exit_status"
}
trap on_exit EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

write_lane_result() {
    local lane="$1"
    local engine="$2"
    python3 - "$lane" "$engine" <<'PY'
import json
import sys
from pathlib import Path

lane = Path(sys.argv[1])
engine = sys.argv[2]

def read(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))

multi_summary = read(lane / "multi-queue" / "completion-summary.json")
multi_exact = read(lane / "multi-queue" / "exact-result.json")
queue_states = [read(path) for path in sorted((lane / "multi-queue").glob("queue-*.json"))]
containers = read(lane / "containers.json")
worker_integrity = read(lane / "worker-integrity" / "result.json")
multi_pass = (
    multi_summary.get("complete") is True
    and multi_exact.get("passed") is True
    and multi_exact.get("starved_queues") == []
    and bool(queue_states)
    and all(
        state.get("quiescent") is True
        and state.get("timed_out") is False
        and state.get("probe_errors") == []
        and state.get("state", {}).get("size") == 0
        for state in queue_states
    )
)

failed = None
failed_pass = True
if engine.startswith("queen-"):
    present = read(lane / "failed-lifecycle" / "present.json")
    completion = read(lane / "failed-lifecycle" / "completion-summary.json")
    exact = read(lane / "failed-lifecycle" / "exact-completion.json")
    empty = read(lane / "failed-lifecycle" / "empty.json")
    queue_state = read(lane / "failed-lifecycle" / "queue-final.json")
    failed_pass = (
        present.get("passed") is True
        and present.get("failed_source_matches_dlq") is True
        and completion.get("complete") is True
        and exact.get("passed") is True
        and empty.get("passed") is True
        and queue_state.get("quiescent") is True
        and queue_state.get("timed_out") is False
        and queue_state.get("probe_errors") == []
        and queue_state.get("state", {}).get("size") == 0
    )
    failed = {
        "enabled": True,
        "initial_failed_count": present.get("failed_count"),
        "initial_dlq_count": present.get("dlq_count"),
        "source_matches_dlq": present.get("failed_source_matches_dlq"),
        "completion_records": len(exact.get("records", [])),
        "final_failed_count": empty.get("failed_count"),
        "final_dlq_count": empty.get("dlq_count"),
        "passed": failed_pass,
    }
else:
    failed = {
        "enabled": False,
        "reason": "Queen DLQ synchronization is not part of Horizon's Redis failed-job path",
        "passed": None,
    }

passed = (
    multi_pass
    and failed_pass
    and containers.get("passed") is True
    and worker_integrity.get("passed") is True
)
result = {
    "schema": "queen.laravel-supervisors.feature-parity-lane/v1",
    "engine": engine,
    "multi_queue": {
        "expected": multi_exact.get("expected"),
        "records": multi_exact.get("records"),
        "jobs_by_queue": multi_exact.get("jobs_by_queue"),
        "per_queue": multi_exact.get("per_queue"),
        "starved_queues": multi_exact.get("starved_queues"),
        "queue_states": queue_states,
        "passed": multi_pass,
    },
    "failed_job_lifecycle": failed,
    "container_integrity": {
        "count": len(containers.get("containers", [])),
        "passed": containers.get("passed") is True,
    },
    "worker_integrity": {
        "expected": worker_integrity.get("expected_workers"),
        "baseline_count": worker_integrity.get("baseline_count"),
        "final_count": worker_integrity.get("final_count"),
        "stable_identities": worker_integrity.get("stable_identities"),
        "respawn_or_replacement_detected": worker_integrity.get("respawn_or_replacement_detected"),
        "passed": worker_integrity.get("passed") is True,
    },
    "passed": passed,
}
(lane / "lane-result.json").write_text(
    json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
)
raise SystemExit(0 if passed else 1)
PY
}

run_lane() {
    local lane_engine="$1"
    local lane_index="$2"
    local suffix
    local connection
    local first_queue="${QUEUES[0]}"
    local group
    local app_id
    local expected_jobs="$TOTAL_JOBS"
    local queue_index
    local failure_id
    local probe_id="probe-1"

    ACTIVE_ENGINE="$lane_engine"
    suffix="$(printf '%s-%02d-%s' "$campaign_token" "$lane_index" "$lane_engine" \
        | tr '[:upper:]' '[:lower:]' | tr -cd 'a-z0-9-')"
    ACTIVE_PROJECT="qlb-feature-${suffix}"
    ACTIVE_VOLUME="qlb-feature-results-${suffix}"
    ACTIVE_LANE_DIRECTORY="${OUTPUT_DIRECTORY}/${lane_engine}"
    ACTIVE_MULTI_RUN="multi-${lane_engine}-${campaign_token}"
    ACTIVE_FAILURE_RUN=""
    group="feature-${lane_engine}-${campaign_token}"
    mkdir -p "${ACTIVE_LANE_DIRECTORY}/multi-queue" \
        "${ACTIVE_LANE_DIRECTORY}/worker-integrity"

    if [ "$lane_engine" = horizon ]; then
        connection=redis
        export BENCH_FAILED_DRIVER=null
    else
        connection=queen
        export BENCH_FAILED_DRIVER=file
        ACTIVE_FAILURE_RUN="failed-${lane_engine}-${campaign_token}"
        mkdir -p "${ACTIVE_LANE_DIRECTORY}/failed-lifecycle"
    fi

    export BENCH_RESULTS_VOLUME="$ACTIVE_VOLUME"
    export BENCH_PROFILE=fixed
    export BENCH_QUEUE="$first_queue"
    export BENCH_QUEUES="$QUEUES_CSV"
    export BENCH_GROUP="$group"
    export BENCH_WORKERS="$WORKERS"
    export BENCH_MIN_WORKERS="$WORKERS"
    export BENCH_MAX_WORKERS="$WORKERS"
    export BENCH_TIMEOUT="$WORKER_TIMEOUT"
    export BENCH_RETRY_AFTER="$RETRY_AFTER"
    export BENCH_DISPATCH_MODE=single
    export BENCH_LEDGER_MODE=off
    export BENCH_LEASE_RENEWAL='false'
    export BENCH_LEASE_RENEWAL_INTERVAL=''
    export BENCH_CONNECTION="$connection"
    export QUEUE_CONNECTION="$connection"
    export BENCH_LANE="$lane_engine"
    export QUEEN_PREFETCH=1
    export QUEEN_ACK_BATCH=1
    export QUEEN_BULK_BATCH=100
    export QUEEN_PARTITIONS=64
    export QUEEN_POP_FUSION=0

    python3 - "$ACTIVE_LANE_DIRECTORY" "$lane_engine" "$ACTIVE_PROJECT" "$ACTIVE_VOLUME" \
        "$connection" "$group" "$QUEUES_CSV" "$QUEUE_COUNTS_CSV" "$WORKERS" <<'PY'
import json
import sys
from pathlib import Path

lane, engine, project, volume, connection, group, queues, counts, workers = sys.argv[1:]
queue_list = queues.split(",")
queue_counts = [int(value) for value in counts.split(",")]
payload = {
    "schema": "queen.laravel-supervisors.feature-parity-lane-plan/v1",
    "engine": engine,
    "compose_project": project,
    "results_volume": volume,
    "connection": connection,
    "consumer_group": group,
    "queues": queue_list,
    "queue_counts": queue_counts,
    "jobs_by_queue": dict(zip(queue_list, queue_counts, strict=True)),
    "total_jobs": sum(queue_counts),
    "workers": int(workers),
    "failed_store": "file" if engine.startswith("queen-") else "null",
    "lease_renewal": False,
    "sterilized_environment": {
        "BENCH_QUEUES": queues,
        "BENCH_FAILED_DRIVER": "file" if engine.startswith("queen-") else "null",
        "BENCH_LEASE_RENEWAL": "false",
        "BENCH_LEASE_RENEWAL_INTERVAL": "",
    },
}
Path(lane, "plan.json").write_text(
    json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
)
PY

    printf '\n[%02d] feature parity %s\n' "$lane_index" "$lane_engine"
    if docker volume inspect "$ACTIVE_VOLUME" >/dev/null 2>&1; then
        die "refusing to reuse pre-existing result volume: $ACTIVE_VOLUME"
    fi
    if docker network inspect "${ACTIVE_PROJECT}_default" >/dev/null 2>&1; then
        die "refusing to reuse pre-existing Compose network: ${ACTIVE_PROJECT}_default"
    fi
    if [ -n "$(docker ps --all --quiet --filter "label=com.docker.compose.project=${ACTIVE_PROJECT}")" ]; then
        die "refusing to reuse containers from Compose project: $ACTIVE_PROJECT"
    fi
    docker volume create \
        --label "queen.benchmark.campaign=${campaign_id}" \
        --label "queen.benchmark.feature.engine=${lane_engine}" \
        --label "com.docker.compose.project=${ACTIVE_PROJECT}" \
        --label "com.docker.compose.volume=results" \
        "$ACTIVE_VOLUME" >/dev/null
    docker run --rm --user 0:0 \
        --mount "type=volume,src=${ACTIVE_VOLUME},dst=/results" \
        "$APP_IMAGE" sh -ceu 'chown 1000:1000 /results; chmod 0770 /results'

    compose_active up --detach --no-build "$lane_engine"
    app_id="$(compose_active ps --quiet "$lane_engine")"
    [ -n "$app_id" ] || die "unable to resolve $lane_engine container"
    wait_for_health "$app_id" "$WAIT_TIMEOUT" "$lane_engine"
    producer php artisan bench:config --no-ansi \
        >"${ACTIVE_LANE_DIRECTORY}/configuration.json"
    wait_for_worker_baseline "$app_id" "$lane_engine" "$WORKERS" \
        "${ACTIVE_LANE_DIRECTORY}/worker-integrity/baseline.json" \
        || die "$lane_engine did not reach the expected fixed worker baseline"

    producer php artisan bench:dispatch-multi --no-ansi \
        --run-id="$ACTIVE_MULTI_RUN" \
        --queue-counts="$QUEUE_COUNTS_CSV" \
        --queues="$QUEUES_CSV" \
        --sleep-ms="$SLEEP_MS" \
        --cpu-iterations=0 \
        --connection="$connection" \
        >"${ACTIVE_LANE_DIRECTORY}/multi-queue/dispatch.json" \
        2>"${ACTIVE_LANE_DIRECTORY}/multi-queue/dispatch.stderr.log"
    producer php artisan bench:results --no-ansi "$ACTIVE_MULTI_RUN" \
        --expected="$expected_jobs" --wait="$WAIT_TIMEOUT" --poll-ms=100 \
        >"${ACTIVE_LANE_DIRECTORY}/multi-queue/completion-summary.json" \
        2>"${ACTIVE_LANE_DIRECTORY}/multi-queue/completion.stderr.log"
    verify_multi_completion "$ACTIVE_MULTI_RUN" "$connection" \
        >"${ACTIVE_LANE_DIRECTORY}/multi-queue/exact-result.json" \
        2>"${ACTIVE_LANE_DIRECTORY}/multi-queue/exact-result.stderr.log"

    queue_index=0
    for queue in "${QUEUES[@]}"; do
        queue_index=$((queue_index + 1))
        producer php artisan bench:queue-state --no-ansi \
            --run-id="$ACTIVE_MULTI_RUN" \
            --connection="$connection" \
            --queue="$queue" \
            --wait="$WAIT_TIMEOUT" \
            --poll-ms=100 \
            --settle-ms="$SETTLE_MS" \
            >"${ACTIVE_LANE_DIRECTORY}/multi-queue/queue-$(printf '%03d' "$queue_index").json" \
            2>"${ACTIVE_LANE_DIRECTORY}/multi-queue/queue-$(printf '%03d' "$queue_index").stderr.log"
    done

    if [ "$lane_engine" != horizon ]; then
        producer php artisan bench:dispatch-failure --no-ansi \
            --run-id="$ACTIVE_FAILURE_RUN" \
            --probe-id="$probe_id" \
            --connection=queen \
            --queue="$first_queue" \
            >"${ACTIVE_LANE_DIRECTORY}/failed-lifecycle/dispatch.json" \
            2>"${ACTIVE_LANE_DIRECTORY}/failed-lifecycle/dispatch.stderr.log"

        wait_for_lifecycle_state present "$first_queue" "$group" \
            "${ACTIVE_LANE_DIRECTORY}/failed-lifecycle/present.json" \
            "${ACTIVE_LANE_DIRECTORY}/failed-lifecycle/present.poll.stderr.log" \
            || die "$lane_engine timed out waiting for one failed row and matching DLQ snapshot"
        producer php artisan queue:failed --no-ansi \
            >"${ACTIVE_LANE_DIRECTORY}/failed-lifecycle/queue-failed-before.txt" \
            2>"${ACTIVE_LANE_DIRECTORY}/failed-lifecycle/queue-failed-before.stderr.log"
        failure_id="$(python3 -c '
import json, sys
with open(sys.argv[1], encoding="utf-8") as stream:
    rows = json.load(stream)["failed"]
if len(rows) != 1 or not isinstance(rows[0].get("id"), str) or not rows[0]["id"]:
    raise SystemExit("missing failed-job id")
print(rows[0]["id"])
' "${ACTIVE_LANE_DIRECTORY}/failed-lifecycle/present.json")"

        producer php artisan queue:retry --no-ansi "$failure_id" \
            >"${ACTIVE_LANE_DIRECTORY}/failed-lifecycle/retry.txt" \
            2>"${ACTIVE_LANE_DIRECTORY}/failed-lifecycle/retry.stderr.log"
        producer php artisan bench:results --no-ansi "$ACTIVE_FAILURE_RUN" \
            --expected=1 --wait="$WAIT_TIMEOUT" --poll-ms=100 \
            >"${ACTIVE_LANE_DIRECTORY}/failed-lifecycle/completion-summary.json" \
            2>"${ACTIVE_LANE_DIRECTORY}/failed-lifecycle/completion.stderr.log"
        verify_failure_completion "$ACTIVE_FAILURE_RUN" "$probe_id" "$first_queue" \
            >"${ACTIVE_LANE_DIRECTORY}/failed-lifecycle/exact-completion.json" \
            2>"${ACTIVE_LANE_DIRECTORY}/failed-lifecycle/exact-completion.stderr.log"
        wait_for_lifecycle_state empty "$first_queue" "$group" \
            "${ACTIVE_LANE_DIRECTORY}/failed-lifecycle/empty.json" \
            "${ACTIVE_LANE_DIRECTORY}/failed-lifecycle/empty.poll.stderr.log" \
            || die "$lane_engine timed out waiting for empty failed store and empty DLQ"
        producer php artisan queue:failed --no-ansi \
            >"${ACTIVE_LANE_DIRECTORY}/failed-lifecycle/queue-failed-after.txt" \
            2>"${ACTIVE_LANE_DIRECTORY}/failed-lifecycle/queue-failed-after.stderr.log"
        producer php artisan bench:queue-state --no-ansi \
            --run-id="$ACTIVE_FAILURE_RUN" \
            --connection=queen \
            --queue="$first_queue" \
            --wait="$WAIT_TIMEOUT" \
            --poll-ms=100 \
            --settle-ms="$SETTLE_MS" \
            >"${ACTIVE_LANE_DIRECTORY}/failed-lifecycle/queue-final.json" \
            2>"${ACTIVE_LANE_DIRECTORY}/failed-lifecycle/queue-final.stderr.log"
    fi

    capture_worker_snapshot "$app_id" "$lane_engine" \
        "${ACTIVE_LANE_DIRECTORY}/worker-integrity/final.json" \
        || die "$lane_engine final worker snapshot failed"
    if ! compare_worker_snapshots \
        "${ACTIVE_LANE_DIRECTORY}/worker-integrity/baseline.json" \
        "${ACTIVE_LANE_DIRECTORY}/worker-integrity/final.json" \
        "$lane_engine" "$WORKERS" \
        "${ACTIVE_LANE_DIRECTORY}/worker-integrity/result.json"; then
        die "$lane_engine lost or replaced a fixed-pool worker during the feature gate"
    fi
    capture_active_diagnostics
    salvage_active_results
    write_lane_result "$ACTIVE_LANE_DIRECTORY" "$lane_engine"
    cleanup_active_lane
}

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
lanes = [json.loads(path.read_text(encoding="utf-8")) for path in sorted(root.glob("*/lane-result.json"))]
report = {
    "schema": "queen.laravel-supervisors.feature-parity-report/v1",
    "qualification": "diagnostic_feature_smoke",
    "performance_comparable": False,
    "lanes": lanes,
    "all_passed": bool(lanes) and all(lane.get("passed") is True for lane in lanes),
}
(root / "report.json").write_text(
    json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
)
lines = [
    "# Feature-parity diagnostic report",
    "",
    "This campaign is a functional smoke test and must not be used for performance comparisons.",
    "",
    "| Engine | Multi-queue | Failed lifecycle | Workers | Containers | Overall |",
    "| --- | --- | --- | --- | --- | --- |",
]
for lane in lanes:
    failed = lane["failed_job_lifecycle"]
    failed_text = "n/a" if not failed["enabled"] else ("pass" if failed["passed"] else "FAIL")
    lines.append(
        f"| {lane['engine']} | {'pass' if lane['multi_queue']['passed'] else 'FAIL'} "
        f"| {failed_text} | {'pass' if lane['worker_integrity']['passed'] else 'FAIL'} "
        f"| {'pass' if lane['container_integrity']['passed'] else 'FAIL'} "
        f"| {'pass' if lane['passed'] else 'FAIL'} |"
    )
lines.extend([
    "",
    "Multi-queue requires the declared weighted job set, no starved queue and a settled empty state for every queue.",
    "Queen failed lifecycle requires one matching Laravel failed row and broker DLQ snapshot, one successful manual retry, then both stores empty.",
    "Worker integrity requires the same PID plus Linux process start-tick identity at the post-health baseline and post-workload final snapshot.",
    "Artifacts redact failed payloads and exception bodies and never contain resolved Compose configuration or environment dumps.",
])
(root / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
raise SystemExit(0 if report["all_passed"] else 1)
PY

trap - EXIT INT TERM
printf '\nFeature-parity artifacts: %s\n' "$OUTPUT_DIRECTORY"
printf 'Report: %s/report.md\n' "$OUTPUT_DIRECTORY"

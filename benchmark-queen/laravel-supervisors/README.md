# Laravel Horizon vs Queen PHP vs Queen Rust

This benchmark compares three ways of running the same Laravel job:

| Lane | Queue backend | Orchestrator | Worker command |
| --- | --- | --- | --- |
| `horizon` | Redis | Horizon PHP master and supervisor | `artisan horizon:work` |
| `queen-php` | Queen broker and PostgreSQL | Queen PHP supervisor | `artisan queue:work queen` |
| `queen-rust` | Queen broker and PostgreSQL | `queen-supervisor` | `artisan queue:work queen` |

All lanes use the application in [`app/`](app/), PHP 8.3, Laravel 12.68.0,
the same serialized `BenchmarkJob`, the same result sink and the same worker
limits. Horizon is pinned to 5.48.3. The Rust binary and Queen PHP client are
built from the checkout under test.

The benchmark answers two different questions. Keep their results separate:

1. **Control-plane cost:** how much CPU and memory the orchestration processes
   use, excluding queue workers and queue backends.
2. **Whole-stack cost:** application container plus its complete queue backend
   (Redis for Horizon; broker plus PostgreSQL for Queen).

The first view isolates the reason for the Rust supervisor. The second is the
cost an operator actually provisions. It is invalid to cite a control-plane
number as a whole-product result, or to compare Redis alone with only the Queen
broker while omitting PostgreSQL.

## Automated campaign

The runner builds both images, rotates lane order, creates a fresh backend and
result volume for every sample, enforces correctness/resource quality gates,
and emits raw data plus Markdown and JSON reports:

```console
cd benchmark-queen/laravel-supervisors
scripts/run.sh --smoke
scripts/run.sh --profile both --jobs 2000 --workers 4 --runs 3 --sleep-ms 10
```

Pass `--no-build` only to diagnostic campaigns after the images have been built
from the same checkout. Publishable campaigns always rebuild and reject that
option so their image identity cannot be stale.
Run `scripts/run.sh --help` for workload, sampling, engine and output options.
Generated campaigns are placed under `results/<timestamp>-<git-sha>/` and are
ignored by Git. The top-level report lists every raw run without cross-profile
ratios; `reports/<profile>-rNN.{md,json}` contains the paired comparisons for
each profile and repetition.

Every performance lane snapshots the complete Docker container inventory at
lane start, immediately before dispatch and after measurement. From the
pre-dispatch boundary until the resource sampler stops, it also subscribes to
Docker's container-start event stream and brackets it with stable inventory
snapshots. The allowed set is frozen to exact container IDs, so a transient
foreign container, a same-project one-off tool or a replacement container
cannot hide between snapshots. A start event for an already allowed ID is a
lane-container restart and fails even with the diagnostic foreign-container
override. The evidence is retained as
`container-isolation.measurement.json`; a lost event stream, invalid inventory
or foreign start fails closed. The benchmark producer is started once before
this boundary and reused with `docker compose exec`, keeping its exact ID
stable and outside measured cgroups. The explicit
`--allow-foreign-containers` override preserves the same evidence but forces
campaign qualification to `diagnostic`; it cannot be used for a publishable
result.

Host qualification is also fail-closed. Automatic runs are
`diagnostic` on Docker Desktop/virtualized hosts and `diagnostic_native` on
native Linux. Only `--qualification publishable` may produce
`publishable_candidate`, and it is rejected unless the host is native Linux,
Docker reports Linux with cgroup v2, and no foreign-container override is set.
This host gate is necessary but does not replace the remaining publication
protocol controls below.

### Durable reliability ledger

Performance campaigns leave the ledger off so its SQLite commits cannot
silently change the workload. Add `--ledger` to run a separately labelled
reliability cell. Before enqueue, the fixture creates a durable SQLite ledger;
each execution records its attempt, then atomically creates or observes one
idempotent effect keyed by `(run_id, job_id)`, and finally records its outcome.

The analyzer reports four distinct gates:

- effect conservation: every expected key exists and no unexpected key does;
- idempotent effect: exactly one committed effect row exists per key;
- attempt integrity: no attempt belongs to an unexpected job, completed attempts
  observed an effect and outcomes are valid;
- strict execution: one attempt per job, with no `already_present` dedup hit.

A retry after an uncertain ACK can therefore conserve one idempotent effect
while failing strict execution. `ledger.sqlite3`, result-to-attempt links and
the full gate output are retained. This proves only the fixture-local SQLite
effect: it is not atomic with the queue ACK or an arbitrary external service,
and it is not an exactly-once guarantee. Never mix ledger-on and ledger-off
samples in one performance aggregate.

Aggregate a repeated campaign only after all lanes have finished:

```console
python3 scripts/campaign-stats.py results/<campaign> \
  --baseline horizon \
  --json-output results/<campaign>/campaign-stats.json \
  --markdown-output results/<campaign>/campaign-stats.md
```

The campaign report retains every run, reports median, quartiles, IQR and
range, and computes deterministic paired-bootstrap 95% confidence intervals
for candidate/baseline ratios. Pairing is by repetition and is fail-closed:
missing artifacts, failed correctness or quiescence, sampler errors, OOM, PSS
gaps, a missing/invalid continuous container-isolation watch, or mismatched
workload/environment/resource budgets suppress the ratio and are listed
explicitly. Bootstrap intervals describe run-to-run variation
on this host; they are not population guarantees, and fewer than three valid
pairs are labelled unstable.

### Controlled optimization factors

The historical behaviour remains the default. Optimization campaigns can vary
one declared factor at a time:

```console
scripts/run.sh --profile fixed --engines queen-php,queen-rust \
  --queen-prefetch 16 --queen-ack-batch 16 \
  --queen-bulk-batch 100 --queen-partitions 16 \
  --queen-pop-fusion 0 --dispatch-mode bulk
```

| CLI option | Environment | Range | Baseline |
| --- | --- | ---: | ---: |
| `--queen-prefetch` | `QUEEN_PREFETCH` | 1..1000 | 1 |
| `--queen-ack-batch` | `QUEEN_ACK_BATCH` | 1..prefetch | 1 |
| `--queen-bulk-batch` | `QUEEN_BULK_BATCH` | 1..1000 | 100 |
| `--queen-partitions` | `QUEEN_PARTITIONS` | 1..64 | 64 |
| `--queen-pop-fusion` | `QUEEN_POP_FUSION` | 0 or 1 | 0 |
| `--worker-timeout` | `BENCH_TIMEOUT` | 1..86400 seconds | 120 |
| `--retry-after` | `BENCH_RETRY_AFTER` | `> prefetch * worker-timeout` | `max(180, prefetch * worker-timeout + 1)` |
| `--dispatch-mode` | `BENCH_DISPATCH_MODE` | single, bulk | single |

Every factor is validated before image or container work begins and is written
to campaign `metadata.json`, each lane's `configuration.json`, the resolved
Compose file and the dispatch manifest. `bulk` uses the same bounded number of
jobs per Laravel `bulk()` invocation in all lanes; the producer remains outside
the measured cgroups.

Prefetch and deferred acknowledgement change the lease-risk envelope even
though delivery remains at least once: a killed worker can cause more already
claimed jobs to be redelivered. Treat `ack_batch > 1` as a separately labelled
performance profile, preserve crash-injection tests, and size `retry_after` for
the worst-case time to process a complete prefetched batch. Pop fusion affects
only Queen's broker and must not be presented as a Horizon setting.

## Test profiles

### Fixed

`BENCH_PROFILE=fixed` starts exactly `BENCH_WORKERS` workers in every lane. Use
this as the primary throughput, latency and steady-state resource comparison.
With worker count fixed, differences in autoscaling policy cannot affect the
result.

Recommended cells are:

- idle: one worker, no dispatched jobs, 30 seconds of warm-up and 120 seconds
  measured;
- fixed load: 1, 4, 8 and 16 workers, subject to the host CPU budget;
- workload shapes: sleep-bound (`--sleep-ms=10`) and CPU-bound
  (`--sleep-ms=0 --cpu-iterations=<calibrated value>`).

Do not use a zero-duration job as the only published workload. At that point
the JSONL sink and dispatch rate can dominate the operation being measured.

### Auto

`BENCH_PROFILE=auto` shares these bounds between the implementations:

```text
BENCH_MIN_WORKERS
BENCH_MAX_WORKERS
BENCH_BALANCE_COOLDOWN
BENCH_BALANCE_MAX_SHIFT
BENCH_STRATEGY=size|time
```

Measure worker-count curves, time to add capacity, time to drain the burst,
worker-seconds and return-to-minimum time. Continue sampling after the final
job for at least two balance cooldowns plus the configured scale-down delay.

This is a behavioural comparison, not an assertion that the algorithms are
identical. With one non-empty queue, Horizon tends to allocate toward its
maximum and uses `size` or `time` primarily to distribute capacity among
queues. Queen derives the requested count from jobs per process or a target
clearance time. Both can have identical limits and still make different valid
decisions. Publish fixed-profile results alongside every autoscaling result.

For the `time` strategy, report whether the run is cold or warmed. A warmed run
must first drain an unmeasured burst with the same job shape so both systems
have runtime observations. Do not warm one lane and leave another cold.

## Common workload and outputs

The application writes no result rows to Redis or PostgreSQL. Completion events
go to a dedicated Docker volume, keeping the measured queue backends out of the
common result path. Sampler output goes to a separate named volume and is
copied only after measurement, so it never locks or appends to the worker
result files. A dispatch is started with:

```console
php artisan bench:dispatch \
  --run-id=<unique-run-id> \
  --jobs=10000 \
  --sleep-ms=10 \
  --cpu-iterations=0
```

The command reserves the run directory before publishing jobs and atomically
renames a completed manifest to:

```text
/results/<run-id>/dispatch.json
```

Workers append one locked stream per PID:

```text
/results/<run-id>/events/worker-<pid>.jsonl
```

Every event includes `run_id`, `job_id`, enqueue/start/completion monotonic
timestamps, queue and end-to-end latency, attempt, workload checksum, worker
PID and sink lock wait. A duplicate execution therefore remains visible even
when the unique completion count is correct.

Use the application commands for progress and the common latency summary:

```console
php artisan bench:count <run-id>
php artisan bench:results <run-id> --expected=10000 --wait=300
php artisan bench:queue-state --run-id=<run-id> --wait=300
```

`bench:results` reports unique completions, raw records, duplicates, maximum
attempt and nearest-rank p50/p95/p99 values. The raw events remain the source
of truth. Horizon dashboard latency is not a common metric and must not be
substituted for these timestamps.

Completion is not the final correctness boundary: a worker writes its result
before Laravel deletes or acknowledges the job. The automated runner therefore
requires the normalized queue size (and ready, reserved and delayed components
when exposed by the driver) to remain zero for one second. The final probe is
saved as `queue-state.final.json`; a missing, timed-out or non-empty probe makes
the analyzer's correctness gate fail.

The monotonic timestamps are comparable across processes and containers only
when they share the same kernel. The supplied Compose environment has that
property. They are not a distributed-clock protocol.

### Worker crash/recovery diagnostic

`fault-recovery.sh` targets exactly one non-master worker with `SIGKILL`, then
checks replacement of the process, redelivery of an in-flight job, the exact
completed job set, container restart/OOM state, and a queue that remains empty
for the final settle window. Every lane uses a fresh backend and retains its
timeline, process identities, raw events, queue state, logs and container
evidence even when the harness fails closed.
The harness explicitly fixes `BENCH_QUEUES` to the single legacy queue,
`BENCH_FAILED_DRIVER=null` and lease renewal off, and retains the resolved
application configuration so inherited feature-test variables cannot change
the protocol silently.

```console
scripts/fault-recovery.sh \
  --output results/fault-$(date -u +%Y%m%dT%H%M%SZ) \
  --engines horizon,queen-php,queen-rust --no-build
```

This is deliberately labelled `diagnostic_smoke`. A single injected crash
does not estimate a failure probability. The harness now requires conservation
of the fixture-local idempotent ledger and reports `created` versus
`already_present` observations for every attempt. Repeat the scenario with
fixed seeds and report every run; the ledger is not atomic with ACK or external
systems, so a clean run is still not proof of exactly-once effects or backend
crash durability.

### Feature-parity diagnostic

`feature-parity.sh` is the reusable, non-performance gate for the behaviours
that a throughput campaign cannot prove. It creates a fresh Compose project,
backend and result volume for every engine. All three lanes receive an equal,
deterministic round-robin dispatch across every configured queue; the gate
requires the exact job set and a settled empty state on each individual queue.
Queue names are limited to 118 ASCII bytes because the fixture appends a colon
and nine-digit sequence while preserving its 128-byte job-ID ceiling.
Queen PHP and Queen Rust also run the full Laravel failed-job lifecycle: one
file-store row and its matching Queen DLQ snapshot must appear, `queue:retry`
must complete the fail-once probe, and both stores must then be empty.

```console
scripts/feature-parity.sh \
  --output results/features-$(date -u +%Y%m%dT%H%M%SZ) \
  --engines horizon,queen-php,queen-rust \
  --queues critical,default --jobs-per-queue 12 --workers 2 --no-build
```

The runner fails closed on timeouts, incomplete or duplicate results, residual
ready/reserved/delayed work, failed-store/DLQ disagreement, container restart
or OOM. In this fixed-pool scenario it also records every worker's PID and
Linux process start tick after the health gate and after the workload; a lost
or respawned worker changes that identity and fails the lane. The interval
before the baseline and after the final snapshot is outside this worker gate.
It retains both worker snapshots, per-lane JSON, raw fixture events and
isolated container logs. Failed payloads and exception bodies are redacted,
and neither resolved Compose configuration nor process environment is
captured. `metadata.json` marks the campaign `diagnostic_feature_smoke` and
`performance_comparable: false`; do not mix this result with performance
samples.
Every lane also records `configuration.json` after explicitly setting
`BENCH_QUEUES`, selecting `BENCH_FAILED_DRIVER=file` only for Queen lifecycle
lanes, and disabling lease renewal. This prevents caller environment leakage
and makes those three feature toggles auditable.

## Requirements

- Docker Engine with Compose v2, profiles, health-condition dependencies and
  `docker compose up --wait` support;
- BuildKit and network access for the first image build;
- images available for the host architecture (the fixture supports Linux
  `amd64` and `arm64` toolchains);
- Python 3.10 or newer for [`scripts/sample.py`](scripts/sample.py) and
  [`scripts/analyze.py`](scripts/analyze.py);
- a unique run ID and freshly recreated backend for every measured sample;
- native cgroup v2 for publishable resource measurements.

Build the images once before measuring. Image compilation and Composer install
must never overlap a measured run:

```console
cd benchmark-queen/laravel-supervisors
docker compose --profile horizon --profile queen-php --profile queen-rust build
```

The active stack has explicit CPU, memory and PID limits. Only start one lane
at a time. Starting all profiles together invalidates both the resource budget
and the shared queue/result assumptions.

## Reference run sequence

Choose one lane and export the matching connection:

```console
export BENCH_LANE=horizon       # horizon | queen-php | queen-rust
export BENCH_CONNECTION=redis  # redis for horizon, queen for either Queen lane
export BENCH_PROFILE=fixed
export BENCH_WORKERS=8
export BENCH_QUEUE=benchmark
export BENCH_GROUP=benchmark
export RUN_ID=horizon-fixed-w8-r01
```

Start the selected supervisor and its dependencies, then wait for health:

```console
docker compose --profile "$BENCH_LANE" up -d --wait "$BENCH_LANE"
```

Record the normalized application configuration before dispatch:

```console
mkdir -p "artifacts/$RUN_ID"
docker compose --profile tools run --rm producer \
  php artisan bench:config > "artifacts/$RUN_ID/app-config.json"
docker compose config > "artifacts/$RUN_ID/compose.resolved.yml"
```

Start resource sampling before the burst. On a native Linux host, obtain each
container's host PID with:

```console
APP_PID=$(docker inspect --format '{{.State.Pid}}' \
  "$(docker compose ps -q "$BENCH_LANE")")
```

For Horizon, sample the application and Redis cgroups:

```console
REDIS_PID=$(docker inspect --format '{{.State.Pid}}' \
  "$(docker compose ps -q redis)")
python3 scripts/sample.py \
  --target "app:horizon=$APP_PID" \
  --target "backend:redis=$REDIS_PID" \
  --interval 0.2 \
  --ready-file "artifacts/$RUN_ID/sampler.ready" \
  --output "artifacts/$RUN_ID/stats.jsonl" &
SAMPLER_PID=$!
```

For either Queen lane, replace the targets with the application, broker and
PostgreSQL PIDs:

```console
BROKER_PID=$(docker inspect --format '{{.State.Pid}}' \
  "$(docker compose ps -q broker)")
POSTGRES_PID=$(docker inspect --format '{{.State.Pid}}' \
  "$(docker compose ps -q postgres)")
python3 scripts/sample.py \
  --target "app:$BENCH_LANE=$APP_PID" \
  --target "backend:queen-broker=$BROKER_PID" \
  --target "backend:postgres=$POSTGRES_PID" \
  --interval 0.2 \
  --ready-file "artifacts/$RUN_ID/sampler.ready" \
  --output "artifacts/$RUN_ID/stats.jsonl" &
SAMPLER_PID=$!
```

Wait until `sampler.ready` exists, dispatch, and wait for all unique jobs:

```console
while [ ! -f "artifacts/$RUN_ID/sampler.ready" ]; do sleep 0.1; done
docker compose --profile tools run --rm producer \
  php artisan bench:dispatch --run-id="$RUN_ID" --jobs=10000 \
  --sleep-ms=10 --cpu-iterations=0 \
  | tee "artifacts/$RUN_ID/dispatch.stdout.json"
docker compose --profile tools run --rm producer \
  php artisan bench:results "$RUN_ID" --expected=10000 --wait=300 \
  | tee "artifacts/$RUN_ID/results.json"
sleep 10 # preserve the post-drain/scale-down window
kill -TERM "$SAMPLER_PID"
wait "$SAMPLER_PID"
```

Copy the raw run directory out of the named volume before cleanup:

```console
docker run --rm \
  -v queen-laravel-supervisor-results:/from:ro \
  -v "$PWD/artifacts/$RUN_ID:/to" \
  -e RUN_ID="$RUN_ID" \
  alpine:3.22 sh -ceu 'cp -a "/from/$RUN_ID/." /to/'
docker compose --profile "$BENCH_LANE" --profile tools \
  down --volumes --remove-orphans
```

Validate and summarize the copied run with the repository analyzer:

```console
python3 scripts/analyze.py wait "artifacts/$RUN_ID" --timeout 1
python3 scripts/analyze.py summarize "artifacts/$RUN_ID" \
  --output "artifacts/$RUN_ID/summary.json"
```

After one valid run per lane, create both a human and machine-readable
comparison:

```console
python3 scripts/analyze.py report \
  "horizon=artifacts/$HORIZON_RUN" \
  "queen-php=artifacts/$QUEEN_PHP_RUN" \
  "queen-rust=artifacts/$QUEEN_RUST_RUN" \
  --output artifacts/report.md \
  --json-output artifacts/report.json
```

The automated `scripts/run.sh` wrapper performs this lifecycle, including a
warm-up and post-drain observation window. Its core analysis calls are:

```console
php artisan bench:results "$RUN_ID" --expected="$JOBS" --wait=600
python3 scripts/analyze.py summarize "results/$RUN_ID" \
  --output "results/$RUN_ID/summary.json"
```

The wrapper fails the sample on a health timeout, missing jobs, duplicates,
non-one attempts, a non-quiescent final queue, sampler errors, OOM events or
cleanup failure. It never silently reuses an artifact directory or backend.
Malformed or partial final lines in either completion or sampler JSONL remain
available for diagnosis but invalidate the primary correctness summary.

A complete retained run has this shape:

```text
artifacts/<run-id>/
├── app-config.json
├── compose-resolved.yml
├── container-isolation.measurement.json
├── dispatch.json
├── events/worker-<pid>.jsonl
├── queue-state.final.json
├── stats.jsonl
├── results.json
└── summary.json
```

The runner may add environment, image-digest and container-log files. It must
not replace any of the raw files with only an aggregate.

## Resource accounting

The sampler emits `queen.laravel-supervisors.stats/v1` JSONL. It reads cgroup
v2 CPU, memory, PID and OOM counters and classifies processes beneath the app
container as `orchestrator` or `worker` from their command lines.

Use these definitions consistently:

- **control-plane CPU:** deltas of `schedstat.runtime_ns` summed only across
  processes classified as `orchestrator`;
- **control-plane memory:** orchestrator PSS when a PSS campaign is enabled;
  otherwise report RSS explicitly as RSS, without implying it is additive;
- **application tier:** app-container cgroup CPU and `memory.current`, which
  include orchestrator and workers;
- **backend tier:** Redis for Horizon; broker plus PostgreSQL for Queen;
- **whole stack:** application tier plus every backend tier;
- **throughput:** unique completed jobs divided by elapsed time from
  `dispatch_started_ns` to the last first completion;
- **post-dispatch drain time:** last first completion minus
  `dispatch_finished_ns` (also publish total dispatch duration);
- **missing:** requested jobs minus unique completions;
- **duplicates:** completion records minus unique job IDs.

For cgroups, report CPU-seconds and utilization normalized by the assigned CPU
quota. For memory, report the maximum sampled `memory.current` during the
measurement window and `memory.peak` separately. Never sum process RSS to
obtain whole-stack memory: shared pages can be counted repeatedly. PSS sampling
is more expensive, so use it for a separate idle/steady-state campaign or a
lower sampling frequency and disclose that frequency.

The automated campaign samples at 500 ms and reports PSS and RSS in separate
columns. Ratios use PSS only with complete process coverage; otherwise they use
RSS on both sides and name the fallback metric. For a throughput-only campaign,
pass `--no-pss` and keep its report separate from the control-plane memory run.

`memory.events`, `memory.events.local`, sample errors and sampling duration are
quality gates, not decorative metadata. A run with `oom`, `oom_kill`, missing
cgroup data or a sampler that cannot keep up with its interval must be rejected
and repeated after the cause is fixed.

## Local Docker Desktop limits

The current development Mac has 24 GB of host RAM, but Docker Desktop exposes
only about **8.214 GB** to its Linux VM. The host total is therefore not the
benchmark budget. The Compose defaults keep one active lane within that VM
budget, but parallel lanes, image builds or unrelated containers can still
cause reclaim and distort latency.

Docker Desktop on Apple Silicon is useful for functional validation and early
directional measurements. It adds a Linux VM, virtualized scheduling and host
filesystem effects, so its numbers must be labelled local/exploratory. Run the
sampler inside the Docker Desktop Linux VM with host PID and cgroup namespaces,
or use `docker stats` only as a coarse check; the macOS host `/proc` is not the
containers' `/proc`.

Before every local sample record `docker info`, the Docker Desktop CPU/memory
allocation, host architecture and the list of other running containers. Stop
unrelated containers and do not build images during the campaign.

The supplied diagnostic stack deliberately uses Redis without RDB/AOF while
PostgreSQL keeps `synchronous_commit=on` (both datasets are ephemeral for each
lane). Therefore whole-stack throughput is a comparison of these explicit
product-shaped configurations, not a claim of equal crash-durability. A
durability study must run a separate cell with persistence, storage and fsync
policy aligned and reported.

## Publishable Linux protocol

Use a dedicated native Linux machine (or disclose the exact VM) with a unified
cgroup v2 hierarchy:

```console
test -f /sys/fs/cgroup/cgroup.controllers
docker info
uname -a
lscpu
```

For results intended for publication:

1. Pin the Queen Git SHA, `composer.lock`, image digests, Docker/Compose
   versions, kernel, CPU model, microcode, memory, architecture and storage.
2. Use identical application CPU/memory/PID limits for all lanes. Give Redis
   the same aggregate backend CPU and memory budget as broker plus PostgreSQL.
3. Use the same CPU set for equivalent tiers, or a dedicated idle host. Record
   CPU governor, turbo policy, NUMA placement and swap policy; do not change
   them between lanes.
4. Build once, then run one lane at a time. Recreate containers, backend state
   and the results volume for every repetition.
5. Run one throwaway warm-up per lane and at least five measured repetitions
   per cell. Rotate or randomize lane order to reduce thermal and time-order
   bias. Keep job payload and burst size byte-identical.
6. Start the sampler before dispatch and keep it running through the post-drain
   window. Save raw JSONL, resolved Compose configuration, application config,
   container logs and completion events for every run.
7. Reject incomplete, duplicate, retried, OOM-throttled or sampler-degraded
   runs. Investigate the cause; do not remove outliers merely because they are
   inconvenient.
8. Publish per-run values plus median and dispersion (IQR or a documented
   confidence interval). A single best run is not a benchmark result.

Horizon performs queue monitoring and metrics writes in Redis. Queen workers
write runtime telemetry only for the `time` strategy, which consumes it;
`size` and fixed/simple pools deliberately avoid unused telemetry I/O. These
are product behaviours, not benchmark-only switches. If a second stripped
microbenchmark is run, label it separately from the production-shaped
comparison.

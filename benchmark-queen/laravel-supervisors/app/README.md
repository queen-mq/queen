# Laravel supervisor benchmark fixture

This application is the common workload used by the Horizon, Queen PHP and
Queen Rust lanes. Dependencies are locked to PHP 8.3 semantics, Laravel
12.68.0 and Horizon 5.48.3. The Queen client is mirrored from this repository
as `dev-main`; the path repository has an explicit version so Docker builds do
not depend on `.git` being copied into the image.

The lane selects its queue connection with `BENCH_CONNECTION=redis|queen`.
`BENCH_QUEUE` and `BENCH_GROUP` isolate queue and consumer state. The
single-queue campaigns keep using `BENCH_QUEUE`; a feature-parity lane can set
strict CSV `BENCH_QUEUES=critical,default`, whose first entry becomes the
default dispatch queue. Empty entries, surrounding whitespace and duplicates
are rejected, fixed/max capacity must cover every configured queue, and the
feature harness caps queue names at 118 bytes so its prefixed job IDs remain at
most 128 bytes.
Supervisor capacity is shared through these variables:

- `BENCH_PROFILE=fixed` starts exactly `BENCH_WORKERS` processes;
- `BENCH_PROFILE=auto` uses `BENCH_MIN_WORKERS`, `BENCH_MAX_WORKERS`,
  `BENCH_BALANCE_COOLDOWN`, `BENCH_BALANCE_MAX_SHIFT` and
  `BENCH_STRATEGY=size|time`.

Horizon and Queen use the same timeout, retry-after, sleep, memory and attempt
limits. Horizon only supports the Redis lane. The Queen PHP and Rust engines
both resolve `config/queen.php` through the client's v2 supervisor contract.

Dispatch a deterministic burst with:

```console
php artisan bench:dispatch --run-id=example --jobs=1000 \
  --sleep-ms=10 --cpu-iterations=0
```

The default `--dispatch-mode=single` preserves the historical producer path.
`--dispatch-mode=bulk` calls Laravel's queue `bulk()` API in bounded chunks;
the chunk size is `QUEEN_BULK_BATCH` for every lane so the application-level
arrival shape is held constant. The Queen connection additionally exposes
`QUEEN_PREFETCH`, `QUEEN_ACK_BATCH`, `QUEEN_BULK_BATCH` and
`QUEEN_PARTITIONS`. `QUEEN_ACK_BATCH` may not exceed `QUEEN_PREFETCH`.

The command atomically publishes
`$BENCH_RESULTS_DIRECTORY/<run-id>/dispatch.json`. Each worker appends to its
own locked stream under `<run-id>/events/worker-<pid>.jsonl`. Every event has
the run/job identifiers, enqueue/start/completion monotonic timestamps,
attempt, workload checksum and sink-lock wait. The run can be polled with:

```console
php artisan bench:count example
php artisan bench:results example --expected=1000 --wait=300
php artisan bench:queue-state --run-id=example --wait=300
```

The count and summary commands stream immutable JSONL byte snapshots. Polling
reads only bytes appended since the preceding snapshot, and the exact summary
retains compact per-job scalars instead of materializing decoded event rows.
This keeps long stress campaigns within the same PHP memory limit used by the
producer and workers.
Readers can retain complete records when a crash leaves an incomplete final
line, but the analyzer counts that partial line and fails primary correctness;
it is diagnostic recovery, not acceptance of a truncated artifact.

`bench:queue-state` waits until the connection's portable `size()` and every
available ready, reserved and delayed count remain zero for the settle window.
The campaign stores its JSON output as `queue-state.final.json`; the analyzer
requires that artifact before a run can be correct.

Set `BENCH_LEDGER_MODE=durable` only for separately labelled reliability
cells. Dispatch then creates `<run-id>/ledger.sqlite3`. Every attempt atomically
creates or observes an idempotent fixture effect keyed by `(run_id, job_id)`;
the completion JSONL links to both IDs and records `created` or
`already_present`. The analyzer independently checks conservation, one effect
row per key, attempt outcomes and strict single execution. The SQLite effect
is not atomic with the queue ACK or an external business effect, so these are
observational gates rather than an exactly-once claim. The default is `off` to
avoid changing timed workloads.

For a multi-queue feature probe, dispatch the same number of jobs on every
queue in deterministic round-robin order:

```console
php artisan bench:dispatch-multi --run-id=multi-example \
  --queues=critical,default --jobs-per-queue=100 --sleep-ms=10
php artisan bench:results multi-example --expected=200 --wait=300
php artisan bench:queue-state --run-id=multi-example --queue=critical --wait=300
php artisan bench:queue-state --run-id=multi-example --queue=default --wait=300
```

The command records `queues_csv` and per-queue-prefixed job IDs so an audit can
prove that every queue was dispatched and completed. Queue-state is checked
once per queue; a single default-queue observation is not sufficient evidence
for this lane.

Timed campaigns leave `BENCH_FAILED_DRIVER=null`, preserving the original hot
path. Failed-job lifecycle tests opt into Laravel's locked file provider on the
shared results volume with `BENCH_FAILED_DRIVER=file`. The fail-once probe
makes the first execution enter Laravel's failed store and Queen's DLQ; after
`queue:retry`, a durable marker lets the retried job succeed:

```console
export BENCH_CONNECTION=queen
export BENCH_QUEUES=critical,default
export BENCH_WORKERS=2
export BENCH_FAILED_DRIVER=file

php artisan bench:dispatch-failure --run-id=failed-example \
  --probe-id=probe-1 --queue=critical
php artisan queue:failed
php artisan queue:retry <failed-job-uuid>
php artisan bench:results failed-example --expected=1 --wait=300
php artisan queue:failed
```

Run the sequence against a fresh broker/results volume and verify the Queen DLQ
before and after retry. The marker makes this one probe deterministic; it is
not an exactly-once guarantee for arbitrary job side effects.

The automated fault and feature harnesses do not inherit these switches from
the shell: they set and record `BENCH_QUEUES`, `BENCH_FAILED_DRIVER`,
`BENCH_LEASE_RENEWAL=false` and an empty renewal interval, then retain
`configuration.json` with the framework's resolved values.

`hrtime()` values are comparable only while all containers share one kernel,
which is true for the local Compose campaign. Use a fresh run identifier and
fresh queue/backend state for every sample.

The fixed profile is the strict worker-count comparison. Autoscaling is a
behavioural comparison: Horizon allocates its maximum across queues when work
exists, while Queen derives the requested process count from its size or
clearance-time target. Matching min/max/cooldown/shift bounds does not make the
two scaling algorithms identical.

Run the focused result-sink and 50,000-event memory regression with:

```console
php tests/streaming_results.php
php tests/effect_ledger.php
php tests/feature_parity.php
```

# Laravel supervisor benchmark fixture

This application is the common workload used by the Horizon, Queen PHP and
Queen Rust lanes. Dependencies are locked to PHP 8.3 semantics, Laravel
12.68.0 and Horizon 5.48.3. The Queen client is mirrored from this repository
as `dev-main`; the path repository has an explicit version so Docker builds do
not depend on `.git` being copied into the image.

The lane selects its queue connection with `BENCH_CONNECTION=redis|queen`.
`BENCH_QUEUE` and `BENCH_GROUP` isolate queue and consumer state. Supervisor
capacity is shared through these variables:

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
```

`hrtime()` values are comparable only while all containers share one kernel,
which is true for the local Compose campaign. Use a fresh run identifier and
fresh queue/backend state for every sample.

The fixed profile is the strict worker-count comparison. Autoscaling is a
behavioural comparison: Horizon allocates its maximum across queues when work
exists, while Queen derives the requested process count from its size or
clearance-time target. Matching min/max/cooldown/shift bounds does not make the
two scaling algorithms identical.

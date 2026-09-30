# Laravel supervisor features, 2026-09-30

Measurements behind the charts of the prefork, fast scale-up and coordinated
replica features of the Laravel supervisor (the `queen-mq/php-client` release
that pins `queen-supervisor` 0.4.0). They show what each feature changes; they are
diagnostic, not a release performance claim.

## Environment

- Host: Apple Silicon Mac, Docker Desktop.
- Broker: `ghcr.io/queen-mq/queen` 1.6.0 with PostgreSQL 16, one node, local.
- Application: `benchmark-queen/laravel-supervisors/app` (Laravel 12.68.0,
  `App\Jobs\BenchmarkJob`) with the package installed from the working tree.
- Memory runs: `queen-php84-zts` image (PHP 8.4 ZTS with pcntl, posix and
  opcache) in a Linux container, so the proportional set size (PSS) can be read
  from `/proc/<pid>/smaps_rollup`.
- Timing runs: the Rust supervisor (debug build) and PHP 8.4 on the host.

## Files

| File | What it records |
| --- | --- |
| `raw/prefork-memory.csv` | Total PSS and RSS of N workers, started one by one (`spawned`) or forked from one booted Laravel (`forked`, the fork server included), opcache off and on, 3 runs |
| `raw/replicas.csv` | Two supervisors on one queue and consumer group, sampled every second: sampled depth, the fleet target one pool of 16 would size (`ceil(depth / 10)`, at most 16), and each supervisor's running workers |
| `raw/scale-up.csv` | One supervisor after a burst of 600 jobs, sampled every half second, stepping by `balance_max_shift=1` or with `fast_scale_up` |
| `scripts/prefork-memory.sh` | The memory run, executed inside the container |
| `scripts/prefork.php` | Boots Laravel once, then forks N children running `queue:work` |
| `scripts/replicas-timeline.sh` | The two-supervisor run, with and without coordination |
| `scripts/scale-up.sh` | The burst run, step against fast |

## How to reproduce

```bash
# Memory: 3 runs of 4 idle workers and of 8 workers processing 600 jobs.
echo "run,workers,jobs,opcache,mode,pss_mib,rss_mib" > raw/prefork-memory.csv
for RUN in 1 2 3; do for CASE in 4:0 8:600; do
  docker run --rm -v <app>:/app -v "$PWD/scripts":/scripts -v "$PWD/raw":/raw \
    -e OUT=/raw/prefork-memory.csv -e RUN=$RUN -e N=${CASE%%:*} -e JOBS=${CASE##*:} \
    -e SPIKE_QUEUE=pfmem-$RUN-${CASE%%:*} --add-host host.docker.internal:host-gateway \
    --entrypoint bash queen-php84-zts:latest /scripts/prefork-memory.sh
done; done

# Replicas and scale-up, on the host.
echo "mode,t,depth,target,workers_a,workers_b" > raw/replicas.csv
scripts/replicas-timeline.sh uncoordinated <app> <queen-supervisor> raw/replicas.csv
scripts/replicas-timeline.sh coordinated <app> <queen-supervisor> raw/replicas.csv
echo "mode,t,workers" > raw/scale-up.csv
scripts/scale-up.sh step <app> <queen-supervisor> raw/scale-up.csv
scripts/scale-up.sh fast <app> <queen-supervisor> raw/scale-up.csv
```

The figures are rendered by `webdoc/scripts/charts.py` from these files.

## Limits

- One host, Docker Desktop, three runs: enough to show the size of each effect,
  not to publish a performance figure.
- Memory is measured 15 seconds after start or after the burst. A long-lived
  worker writes to more of the pages it shares, so the forked figure is
  optimistic: after hours of jobs, more pages are copied and the saving shrinks.
- The timing runs use a one-second poll and cooldown to make the difference
  visible in seconds; production defaults are three seconds each.

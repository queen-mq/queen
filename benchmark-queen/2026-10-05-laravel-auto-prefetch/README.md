# prefetch "auto" against fixed prefetch, 2026-10-05

`prefetch` `'auto'` lets each Laravel worker size its next pop from how long
its jobs take, so a batch holds about 250 ms of work, from 1 to 16 jobs
(`clients/client-php/src/Laravel/Queue/AdaptiveBatch.php`). This campaign
measures it against fixed prefetch for the three delivery profiles of the
Laravel guide, on four kinds of job. The protocol is the Queen team's; the
results are diagnostic.

## Environment

- Host: the DigitalOcean droplet of
  [`../2026-10-01-linux-vm-horizon-raft`](../2026-10-01-linux-vm-horizon-raft):
  16 vCPUs, 31 GiB, Ubuntu 24.04.5, kernel 6.8.0-142, Docker 29.1.3. Nothing
  else ran on it.
- Budgets: the application 8 CPUs and 8 GiB, the broker 4 CPUs and 4 GiB.
- Broker: one Raft node, server `2.0.0-beta.3`, the image of
  [`../2026-10-02-laravel-partition-stripes`](../2026-10-02-laravel-partition-stripes),
  unchanged. Every acknowledged write is fsynced.
- Queen: the Rust supervisor 0.7.0 with prefork, the command-line opcache and
  lease renewal in the master. Application: `benchmark-queen/laravel-supervisors`
  (Laravel 12, PHP 8.3) on master `66474a6e` (PHP client 2.1.0) with the
  `'auto'` change, image `queen-laravel-supervisor-bench:auto`.

## Lanes

`scripts/auto.sh` ran every lane three times, each run on a fresh broker.

| Profile | Settings |
| --- | --- |
| `safe` | `prefetch` 1, synchronous ACKs, no `pop_ahead`, no lease renewal |
| `balanced` | `prefetch` `'auto'`, synchronous ACKs, no `pop_ahead` |
| `fast` | `prefetch` `'auto'`, `ack_async` and `pop_ahead` |
| `fixed4` | `prefetch` 4, `ack_async` and `pop_ahead`: the fast profile before `'auto'` |

| Workload | Jobs |
| --- | --- |
| `empty` | 100,000 jobs that do nothing, dispatched in one burst, 32 workers |
| `ten` | 50,000 jobs of 10 ms, one burst, 32 workers |
| `slow` | 6,000 jobs of 200 ms, one burst, 32 workers |
| `paced` | 30,000 jobs of 10 ms dispatched one at a time at 500 jobs/s, 16 workers |

`scripts/extract.py` writes one CSV row per run (`raw/runs-*.csv`), the same
extractor as the 2026-10-01 campaign.

## Results

All 63 runs completed their exact job set with no duplicate. `raw/runs-v1.csv`
holds the first version of `'auto'` (45 runs, every lane); `raw/runs-v2.csv`
the balanced and fast lanes rerun with the version that ships (18 runs).
Medians of three runs; safe and `fixed4` do not use `'auto'`, so their v1 runs
stand.

| Lane | Safe | Balanced | Fast | `fixed4` |
| --- | ---: | ---: | ---: | ---: |
| `empty`, jobs/s | 3,131 | 5,675 | 6,015 | 6,046 |
| `ten`, jobs/s | 1,548 | 2,007 | 2,836 | 2,835 |
| `slow`, jobs/s (v1) | 139 | 139 | 147 | 148 |
| `ten`, pops per job | 1.00 | 0.067 | 0.089 | 0.258 |
| `ten`, broker CPU per job | 0.292 ms | 0.186 ms | 0.190 ms | 0.216 ms |
| `paced`, p50 / p95 / p99 | 14.4 / 23.0 / 28.9 ms | 14.5 / 23.1 / 30.4 ms | 14.6 / 23.1 / 29.2 ms | |
| `paced`, broker CPU per job | 0.725 ms | 0.733 ms | 0.809 ms | |

- With short jobs `'auto'` asks for about 15 jobs per pop (0.067 pops per
  job), and the broker spends a third less CPU per job than at prefetch 1.
- With 200 ms jobs it keeps one job per pop (1.02 pops per job, v1; the
  rerun changed only how a batch grows, which jobs that long never reach).
- At 500 jobs/s the p99 of balanced was 59.4, 29.0 and 30.4 ms in its three
  runs; safe 28.5 to 30.0, fast 29.0 to 29.2. Fast pops ahead after every job
  on that quiet queue, 1.99 pops per job, which costs the broker 0.08 ms of CPU
  per job.

### The first version

v1 let the batch grow by up to twofold on every pop. Under a steady 500 jobs/s
the p99 of balanced was 34.9, 63.1 and 144.8 ms, and of fast 29.1, 100.2 and
29.2 ms: during a brief burst a worker grew its batch, then held several jobs
behind the one it ran while other workers sat idle. v2 grows the batch only
after a pop that came back full, sets it to what a short pop brought, and to
one job after an empty pop (`AdaptiveBatch::popped()`). Throughput moved by
2% or less: `empty` 5,671 against 5,675 jobs/s for balanced, `ten` 2,892
against 2,836 for fast.

## Reproduce

```sh
scripts/auto.sh <results> empty-safe empty-balanced empty-fast empty-fixed4 \
  ten-safe ten-balanced ten-fast ten-fixed4 slow-safe slow-balanced slow-fast slow-fixed4 \
  paced-safe paced-balanced paced-fast
scripts/extract.py <results> > raw/runs.csv
```

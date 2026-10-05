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

`scripts/auto.sh` ran every lane three times, the `paced` lanes of v3 five
times (`AUTO_RUNS=5`), each run on a fresh broker.

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

Every worker long-polls for 1 s (`block_for`) and, unless stated, sleeps 1 s
after an empty pop (the pool's `sleep`, `BENCH_WORKER_SLEEP`).

`scripts/extract.py` writes one CSV row per run (`raw/runs-*.csv`), the same
extractor as the 2026-10-01 campaign. `scripts/tail.py` lists the jobs of one
run that took over 100 ms end to end, grouped by time and worker.

## Results

All 100 runs completed their exact job set with no duplicate.

| File | Runs | What |
| --- | ---: | --- |
| `raw/runs-v1.csv` | 45 | the first version of `'auto'`, every lane |
| `raw/runs-v2.csv` | 18 | balanced and fast, growth only after a full pop |
| `raw/runs-v3.csv` | 27 | balanced and fast, the version that ships; `paced` safe again |
| `raw/runs-v3-sleep0.csv` | 10 | `paced` safe and balanced with `sleep` 0 |

Medians; balanced and fast from v3, safe and `fixed4` from v1, since they do
not use `'auto'`, `paced` safe from v3.

| Lane | Safe | Balanced | Fast | `fixed4` |
| --- | ---: | ---: | ---: | ---: |
| `empty`, jobs/s | 3,131 | 5,681 | 5,994 | 6,046 |
| `ten`, jobs/s | 1,548 | 2,003 | 2,867 | 2,835 |
| `slow`, jobs/s (v1) | 139 | 139 | 147 | 148 |
| `ten`, pops per job | 1.00 | 0.069 | 0.091 | 0.258 |
| `ten`, broker CPU per job | 0.292 ms | 0.188 ms | 0.189 ms | 0.216 ms |
| `paced`, p50 / p95 / p99 | 14.3 / 22.8 / 28.9 ms | 14.5 / 23.1 / 30.1 ms | 14.6 / 23.0 / 29.0 ms | |
| `paced`, broker CPU per job | 0.704 ms | 0.723 ms | 0.813 ms | |
| `paced`, application CPU per job | 0.675 ms | 0.797 ms | 0.937 ms | |

- With short jobs `'auto'` asks for about 15 jobs per pop (0.069 pops per
  job), and the broker spends a third less CPU per job than at prefetch 1.
- With 200 ms jobs it keeps one job per pop (1.02 pops per job, v1). A batch
  holds about 250 ms of work, so jobs that long never grow it, and v2 and v3
  changed only how a batch grows.
- Fast pops ahead after every job on the quiet `paced` queue, 2.00 pops per
  job, which costs the broker 0.11 ms of CPU per job against safe. Balanced
  costs the application 0.12 ms per job against safe: the lease renewal.

### The tail at 500 jobs/s

The p99 of the five v3 `paced` runs, with `sleep` 1:

| Profile | p99 of each run |
| --- | --- |
| safe | 29.0, 28.9, 28.3, 35.5, 28.4 ms |
| balanced | 30.1, 37.0, 29.2, 28.7, 337.8 ms |
| fast | 29.0, 29.1, 28.9, 29.0, 29.1 ms |

`scripts/tail.py` on the 337.8 ms run of balanced: all 611 jobs over 100 ms
(the worst 1,049 ms) came in the first 1.5 s after the warm-up. The queue had
been quiet, so 12 of the 16 workers had come back from a long poll with
nothing and were sleeping for 1 s when the jobs started. The 4 awake workers
grew their batches on the backlog, started about 105 jobs each against about
40 for the others, and held 346 of the 611 slow jobs. With `sleep` 0 a worker
whose long poll comes back empty polls again at once:

| Profile, `sleep` 0 | p99 of each run | Jobs over 100 ms |
| --- | --- | ---: |
| safe | 28.9, 29.3, 28.9, 78.1, 29.0 ms | 0, 0, 0, 273, 0 |
| balanced | 29.0, 29.3, 30.0, 30.4, 30.2 ms | 0 in every run |

The 78.1 ms run of safe is a stall of about one second, 18.8 s into the run,
that hit all 16 workers alike: in it the worst job waited 428 ms in the queue
while 339 other jobs started. Safe pops one job at a time, so batches play no
part in it. `sleep` 0 costs nothing measurable here: broker CPU per job
0.735 ms for balanced, against 0.723 ms with `sleep` 1.

### How a batch grows

- v1 let the batch grow by up to twofold on every pop. Under a steady
  500 jobs/s the p99 of balanced was 34.9, 63.1 and 144.8 ms, and of fast
  29.1, 100.2 and 29.2 ms: during a brief burst a worker grew its batch, then
  held several jobs behind the one it ran while other workers sat idle.
- v2 grew the batch only after a pop that came back full, set it to what a
  short pop brought, and to one job after an empty pop. The p99 of balanced
  was 59.4, 29.0 and 30.4 ms.
- v3, the version that ships, doubles the batch only after two full pops in
  a row (`AdaptiveBatch::popped()` and `size()`), so a single full pop at the
  start of a burst is not taken for a backlog. Throughput moved by 1.1% or less
  from v2: `empty` 5,681 against 5,675 jobs/s for balanced, `ten` 2,867
  against 2,836 for fast.

## Reproduce

```sh
scripts/auto.sh <results> empty-safe empty-balanced empty-fast empty-fixed4 \
  ten-safe ten-balanced ten-fast ten-fixed4 slow-safe slow-balanced slow-fast slow-fixed4 \
  paced-safe paced-balanced paced-fast
scripts/extract.py <results> > raw/runs.csv
# The tail lanes: five runs each, then with sleep 0.
AUTO_RUNS=5 scripts/auto.sh <results> paced-safe paced-balanced paced-fast
AUTO_RUNS=5 BENCH_WORKER_SLEEP=0 scripts/auto.sh <results> paced-safe paced-balanced
scripts/tail.py <run directory>
```

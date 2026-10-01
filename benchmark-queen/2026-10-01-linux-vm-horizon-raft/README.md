# Horizon against Queen on a Linux server, 2026-10-01

Laravel Horizon on Redis against the Queen supervisor on the Queen Raft broker,
on a dedicated Linux virtual machine, with more workers, more jobs and longer
runs than the Docker Desktop campaign in
[`../2026-10-01-laravel-horizon-raft`](../2026-10-01-laravel-horizon-raft).
The protocol is the Queen team's; the results are diagnostic, not a release
performance claim.

## Environment

- Host: a DigitalOcean droplet, 16 vCPUs (Intel Xeon Gold 6548N, KVM, one
  thread per core), 31 GiB, a 200 GB virtual disk, Ubuntu 24.04.5, kernel
  6.8.0-142, Docker 29.1.3, cgroup v2. Nothing else ran on it.
- Budgets: the application 8 CPUs and 8 GiB; the broker 4 CPUs and 4 GiB;
  Redis 4 CPUs and 4 GiB. The producer and the sampler use the rest.
- Horizon lanes: Redis 7.4.2 with AOF, `appendfsync always` unless a lane says
  `everysec` or `no`. Horizon's own supervisor, `block_for` 1 s.
- Queen lanes: one Raft broker node, server `2.0.0-beta.2` (`raft` branch,
  commit `d82e785f`, the root Dockerfile unchanged), on a local data directory.
  Every acknowledged write is fsynced in a group commit, in every lane. The
  broker was not changed. The Rust supervisor with prefork, the command-line
  opcache, lease renewal in the supervisor, `ack_async`, `pop_ahead`, prefetch
  4 and the cURL transport.
- Application: `benchmark-queen/laravel-supervisors` (Laravel 12, PHP 8.3) at
  commit `099a8b00` for the `load` group, `79af91ee` for `drain`, `nofsync` and
  the `old` side of `popfix`, and `e80e7ea8` for the `new` side of `popfix`.
  The trees were clean; `raw/metadata` holds each lane's provenance.
- Jobs: `App\Jobs\BenchmarkJob` sleeps 10 ms unless a lane says 0 ms (empty
  jobs).

## Lanes

3 runs per engine and lane, engines alternating, each run on a fresh backend;
the soak ran once per engine. All 119 runs in `raw/runs.csv` completed the
exact job set with no duplicate and no failure.

| Group | Lane | Workload |
| --- | --- | --- |
| `load` | `throughput-32`, `throughput-16` | 50,000 and 30,000 jobs dispatched in one bulk burst, 32 and 16 workers |
| `load` | `everysec-32` | as `throughput-32`, Redis `everysec` |
| `load` | `noop-32` | 100,000 empty jobs, 32 workers |
| `load` | `burst-64` | 30,000 jobs, autoscaling from 1 to 64 workers |
| `load` | `paced-500` | 60,000 jobs dispatched one at a time at 500 jobs/s, 16 workers |
| `load` | `soak` | 360,000 jobs at 400 jobs/s for 15 minutes, 16 workers |
| `load` | `ablation-none-32`, `ablation-lease-32`, `ablation-ack-32` | Queen only, the features added one by one |
| `load` | `noop-guzzle-32`, `noop-curl-32` | Queen only, the two HTTP transports |
| `drain` | `drain-32`, `drain-64`, `drain-noop-32`, `drain-everysec-32` | as the bulk lanes, but every worker is held until the whole batch is enqueued (`run.sh --backlog-first 1`) |
| `nofsync` | `nofsync-32`, `nofsync-noop-32`, `drain-nofsync-32`, `drain-nofsync-noop-32` | Redis `appendfsync no`; Raft still fsyncs |
| `popfix` | `paced-500`, `throughput-32`, `noop-32`, each `-old` and `-new` | Queen only: `pop_ahead` before and after it waits for a full batch |

In the bulk lanes the producer is part of the measurement. Horizon writes its
monitoring records to Redis for every dispatched job, so its producer sends
about 860 jobs/s with `appendfsync always`, and Horizon's rate in those lanes
is its producer's. The `drain` lanes take the producer out: no job ran before
the workers were released in any of their 24 runs (`backlog-count.json` in
each run).

## Results

Medians; `scripts/medians.py raw/runs.csv` prints every lane.

Worker capacity, from the drain lanes:

| Lane | Horizon | Queen | Queen / Horizon |
| --- | --- | --- | --- |
| 32 workers, 10 ms jobs, fsync every write | 1,124 jobs/s | 2,753 jobs/s | 2.4 |
| 32 workers, Redis `everysec` | 1,247 | 2,752 | 2.2 |
| 32 workers, Redis `appendfsync no` | 1,313 | 2,714 | 2.1 |
| 32 workers, empty jobs | 1,090 | 5,995 | 5.5 |
| 32 workers, empty jobs, Redis `appendfsync no` | 1,279 | 6,017 | 4.7 |
| 64 workers, 10 ms jobs | 1,090 | 4,395 | 4.0 |

Horizon stays near 1,100 to 1,300 jobs/s whatever the worker count, the job
length or Redis's fsync mode. Each Horizon job costs about 33 Redis commands
(`backend_operations_per_job`) against 1.3 Queen requests.

Whole system, producer included:

| Lane | Horizon | Queen |
| --- | --- | --- |
| 50,000 jobs, 32 workers: jobs/s | 858 | 2,794 |
| 50,000 jobs, 32 workers: all done after | 58.5 s | 18.0 s |
| 100,000 empty jobs, 32 workers: jobs/s | 889 | 5,989 |
| 30,000 jobs, autoscaling to 64 workers: all done after | 37.0 s | 9.5 s |

Latency and steady load, one job per dispatch:

| Lane | Horizon | Queen |
| --- | --- | --- |
| 500 jobs/s asked: reached | 440 jobs/s (its producer's limit) | 500 jobs/s |
| 500 jobs/s: wait p50 / p95 | 65 / 217 ms | 5 / 13 ms |
| 400 jobs/s for 15 minutes: wait p50 / p95 | 60 / 239 ms | 4 / 12 ms |

Memory of the application container, and the workers' proportional set size:

| Lane | Horizon app / workers | Queen app / workers |
| --- | --- | --- |
| 16 workers (`throughput-16`) | 525 / 469 MiB | 83 / 58 MiB |
| 32 workers (`throughput-32`) | 988 / 922 MiB | 120 / 81 MiB |
| 64 workers (`drain-64`) | 1,892 / 1,825 MiB | 183 / 122 MiB |

A Horizon worker boots its own Laravel: 28 MiB private of 48 MiB resident. A
Queen worker is forked from one booted Laravel (prefork) and shares its pages
until it writes them: 1.4 MiB private of 33 MiB resident
(`raw/process-memory.csv`). Proportional set size divides each shared page
among the processes that map it; the container's cgroup counts it once.

CPU per job (application, backend):

| Lane | Horizon | Queen |
| --- | --- | --- |
| 50,000 jobs, 32 workers | 1.83, 0.37 ms | 0.91, 0.23 ms |
| 500 jobs/s, 16 workers | 1.46, 0.49 ms | 1.35, 0.87 ms |
| 400 jobs/s for 15 minutes | 1.56, 0.54 ms | 1.46, 0.97 ms |

Under load the Raft broker spends less CPU per job than Redis; at a low,
steady rate it spends more. `raw/broker-profile.md` shows why: each job is its
own Raft entry there, and about half of the broker's CPU goes to handing work
between threads (waking them, system calls and scheduling).

The Queen features, 50,000 jobs, 32 workers:

| Queen settings | Jobs/s | App memory |
| --- | --- | --- |
| prefork and opcache only | 1,890 | 417 MiB |
| + lease renewal in the supervisor | 1,879 | 116 MiB |
| + `ack_async` | 2,270 | 120 MiB |
| + `pop_ahead` (`throughput-32`) | 2,794 | 120 MiB |

The cURL transport against Guzzle, 100,000 empty jobs: 6,030 against 5,441
jobs/s, and 1.03 against 1.41 ms of application CPU per job.

`pop_ahead` waiting for a full batch (`popfix`): at 500 jobs/s the workers
sent 0.99 pops per job instead of 1.97, the broker spent 0.82 ms of CPU per job
instead of 0.93 (−12%) and the application 1.34 instead of 1.46 (−8%), with the
same latency. Under load nothing changed: 0.257 pops per job, 2,738 against
2,730 jobs/s.

The soak (`raw/soak-memory.csv`): over 15 minutes the workers' proportional
set size stayed flat for both engines (529 MiB for Horizon, 81 MiB for Queen).
Both application containers grew by about 170 MiB of page cache, the
benchmark's own result files. Redis grew from 67 to 611 MiB of process memory
with Horizon's job records; the broker grew from 59 to 117 MiB, plus page cache
of its log.

## Files

- `raw/runs.csv`: one row per run.
- `raw/process-memory.csv`: memory per process, mid-run, `drain-32` and `drain-64`.
- `raw/soak-memory.csv`: memory per fifth of the soak.
- `raw/broker-profile.md`: the Raft broker's CPU at 500 jobs/s, from counters and perf.
- `raw/metadata/<group>/<lane>.{metadata.json,report.md}`: each lane's provenance and the runner's report.
- `scripts/load.sh`, `scripts/drain.sh`, `scripts/popfix.sh`: the campaigns.
- `scripts/extract.py`, `scripts/medians.py`, `scripts/memcsv.py`, `scripts/promdelta.py`: the tables.
- `scripts/profile/`: the perf capture and the stack folding behind `raw/broker-profile.md`.

## How to reproduce

```bash
# The Raft broker image, from a checkout of the raft branch at d82e785f.
docker build -t queen-laravel-supervisor-broker:raft <raft-checkout>

scripts/load.sh <results>/load throughput-32 throughput-16 everysec-32 noop-32 burst-64 paced-500 \
  ablation-none-32 ablation-lease-32 ablation-ack-32 noop-guzzle-32 noop-curl-32 soak
scripts/drain.sh <results>/drain drain-32 drain-noop-32 drain-everysec-32 drain-64
scripts/drain.sh <results>/nofsync nofsync-32 nofsync-noop-32 drain-nofsync-32 drain-nofsync-noop-32
scripts/popfix.sh <results>/popfix   # needs checkouts of 79af91ee and e80e7ea8
scripts/extract.py <results> > raw/runs.csv
```

`load.sh` and `drain.sh` hard-code the checkout they run from (`BENCH=`);
`popfix.sh` names its two checkouts in `TREE`. The figures are rendered by
`webdoc/scripts/charts.py` from `raw/runs.csv`.

## Limits

- One virtual machine and its virtual disk: the fsync cost, and with it every
  absolute number, differs on other servers.
- A single Raft node. A cluster adds a network round trip to every write.
- The benchmark job is tiny. A real application's worker holds more private
  memory per job, so the memory ratio between the engines will be smaller
  than here; the per-process boot cost that prefork removes stays.
- In the bulk lanes Queen's wait time is the length of the backlog its fast
  producer builds, not its reaction time; the paced lanes and the soak measure
  reaction.
- The soak lasted 15 minutes: whether the broker's process memory levels off
  is not shown.

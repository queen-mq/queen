# Where the worker memory goes, 2026-10-05

The 2026-10-01 campaign measured 64 Laravel workers in 183 MiB with Queen's
prefork workers against 1,892 MiB with Horizon. It ran Queen with prefork and
the command-line opcache on and Horizon with the opcache off, as PHP ships, and
its job is tiny. This campaign separates the two factors, adds a job that
allocates memory, and follows the workers through 20 minutes of work, to
answer one question: how can the same PHP take so much less memory? The
protocol is the Queen team's; the results are diagnostic.

## Environment

- Host: the DigitalOcean droplet of
  [`../2026-10-01-linux-vm-horizon-raft`](../2026-10-01-linux-vm-horizon-raft):
  16 vCPUs, 31 GiB, Ubuntu 24.04.5, kernel 6.8.0-142, cgroup v2, Docker 29.1.3.
  Nothing else ran on it.
- Budgets: the application 8 CPUs and 8 GiB; the broker and Redis 4 CPUs and
  4 GiB each.
- Broker: one Raft node, server `2.0.0-beta.3`, the image of
  [`../2026-10-02-laravel-partition-stripes`](../2026-10-02-laravel-partition-stripes)
  (`sha256:43f2f39d`), unchanged. Redis 7.4.2 with `appendfsync always`.
- Application: `benchmark-queen/laravel-supervisors` (Laravel 12, PHP 8.3) at
  commit `d6afa79e` with the harness changes of this campaign (see Harness),
  image `queen-laravel-supervisor-bench:memory`
  (`sha256:54875b6e415afe6f3d6ad92f0386363abfd2db55adf90ded3a1fb321abaf53ee`).
- Queen: the Rust supervisor, lease renewal in the supervisor, prefetch 4,
  `ack_async`, `pop_ahead` and the cURL transport, as on 2026-10-01; prefork
  and the opcache vary by lane. Horizon: its own supervisor, `block_for` 1 s.
- Every worker: `--memory` 128, PHP's `memory_limit` of 128 MiB, no `max_jobs`
  or `max_time`, JIT off, no preload.

## Lanes

`scripts/memory.sh` ran each lane with 64 fixed workers. A drain lane
dispatched 60,000 jobs in one burst, three runs per engine, engines
alternating, each run on a fresh backend; the sampler kept reading for 20 s
after the last job, with every worker alive and idle.

| Lane | Horizon | Queen |
| --- | --- | --- |
| `published` | opcache off | prefork, opcache on: the 2026-10-01 setting |
| `opcache` | opcache on | each worker started on its own, opcache on |
| `plain` | | each worker started on its own, opcache off |
| `fork-only` | | prefork, opcache off |
| `aging` | opcache off | prefork, opcache on |

Each drain lane ran twice: `-tiny` with the job of 2026-10-01, which sleeps
10 ms, and `-alloc8` with a job that also builds 8,192 distinct strings of
1 KiB, holds them while it runs and releases them at its end
(`BENCH_JOB_ALLOC_KB=8192`). The `aging` lane dispatched 600,000 of those jobs
one at a time at 500 jobs/s, about 9,400 per worker over 20 minutes, one run
per engine.

## Measures

- Per process, from `/proc/<pid>/smaps_rollup` and `/proc/<pid>/status`:
  resident set (RSS) split into anonymous, file and shmem pages; proportional
  set size (PSS), which divides a shared page among the processes that map it,
  split the same way; and private memory (USS), the pages no other process
  maps.
- Per container, from its cgroup: `memory.current` and the `anon`, `file`,
  `shmem`, `kernel`, `pagetables` and `inactive_file` lines of `memory.stat`.
  The cgroup charges each physical page once.
- Process kinds come from the process tree: Horizon's master, its
  `horizon:supervisor` and its workers; Queen's Rust master, its fork server
  and its workers.
- `raw/process-memory.csv` and `raw/cgroup-memory.csv` take the median of the
  15 samples before the sampler stopped (`scripts/memory.py procs|cgroup`);
  `raw/aging.csv` takes a median per minute (`scripts/memory.py aging`).

## Results

All 36 drain runs completed their exact 60,000 jobs with no duplicate. The
three runs of each case agree within 0.1 MiB per worker and 1.4 MiB per
container; the tables give their medians.

Per worker and per container, 64 workers idle after the 10 ms job:

| Lane, engine | Workers | Opcache | RSS | Private | PSS | Container | anon | shmem | kernel |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `published`, Horizon | each boots | off | 48.7 | 28.9 | 29.2 | 1,967 | 1,906 | 0 | 25 |
| `opcache`, Horizon | each boots | on | 55.9 | 35.9 | 36.2 | 2,436 | 881 | 1,489 | 31 |
| `plain`, Queen | each boots | off | 51.0 | 31.1 | 31.5 | 2,062 | 1,996 | 1 | 29 |
| `opcache`, Queen | each boots | on | 58.0 | 38.1 | 38.4 | 2,512 | 888 | 1,553 | 35 |
| `fork-only`, Queen | forked | off | 39.5 | 6.2 | 6.7 | 496 | 430 | 1 | 30 |
| `published`, Queen | forked | on | 32.7 | 1.7 | 2.2 | 225 | 133 | 25 | 31 |

MiB; RSS, private and PSS per worker, the rest per container. The container
also holds about 36 MiB of page cache for the result files in every case, and
the `kernel` line includes 13 to 15 MiB of page tables.

The same lanes with the job that holds 8 MiB (`-alloc8`):

| Lane, engine | Private per worker | Container |
| --- | ---: | ---: |
| `published`, Horizon | 37.1 | 2,494 |
| `opcache`, Horizon | 43.9 | 2,949 |
| `plain`, Queen | 39.4 | 2,590 |
| `opcache`, Queen | 46.0 | 3,019 |
| `fork-only`, Queen | 14.5 | 1,026 |
| `published`, Queen | 10.0 | 755 |

Every worker grew by 7.9 to 8.3 MiB, the job's own memory, which PHP keeps for
the next job. The supervisor processes, PSS: Horizon's master 29.8 and
`horizon:supervisor` 30.1 MiB; the Rust master 6.6 MiB and the fork server
23.9 MiB with the opcache, 14.3 MiB without.

What the split shows:

- A worker that boots on its own keeps Laravel's compiled code and the booted
  application in its heap: 28.9 MiB private for Horizon, 31.1 MiB for a Queen
  worker started on its own. The opcache moves the compiled code into opcache
  memory that each such process creates for itself (22.6 MiB shmem in a
  Horizon worker), so it costs 7 MiB more per worker.
- A forked worker maps the fork server's pages. With the opcache its 32.7 MiB
  resident set is 12.9 MiB of booted heap, shared copy-on-write, 11.6 MiB of
  the fork server's opcache memory, shared outright, and 8.2 MiB of the PHP
  binary and libraries; 1.7 MiB is its own. Without the opcache the compiled
  code sits in the fork server's heap (27.4 MiB of anon instead of 12.8) and
  each worker copies 6.2 MiB of it.
- Against Horizon with the opcache off, forked workers with the opcache save
  27.2 MiB per worker with the 10 ms job and 27.1 MiB with the 8 MiB job: the
  saving is the booted framework, and the ratio falls from 8.7 to 3.3 as the
  job's own memory grows.

The `aging` lane (`raw/aging.csv`, its second run: the first stopped when the
harness's result check read the 600,000 results with PHP's default 128 MiB;
`run.sh` now gives it 1 GiB). Both engines completed all 600,000 jobs once,
about 9,400 per worker. Medians per minute:

| Minute | Horizon, private per worker | Queen, private per worker | Queen, PSS of the 64 workers |
| ---: | ---: | ---: | ---: |
| 0 | 37.1 | 10.1 | 674.8 |
| 10 | 37.1 | 10.1 | 681.8 |
| 20 | 37.1 | 10.1 | 682.2 |

Over 20 minutes the forked workers copied about 7 MiB more of the shared
pages between them, 0.1 MiB each. The same lane ran for 23 hours on
2026-10-06: [`../2026-10-06-laravel-worker-memory-24h`](../2026-10-06-laravel-worker-memory-24h). The containers grew by about 350 MiB on both
engines, all of it page cache (`inactive_file`) for the result files.

### Boot against fork

Added on 2026-10-06, on the same droplet with nothing else running:
`scripts/boot-fork-campaign.sh` ran `scripts/boot-fork.php` in the `:auto`
application image (PHP 8.3.35, 8 CPUs), and `raw/boot-fork.jsonl` holds every
line. A boot is a new PHP process that bootstraps the console kernel, 30 times
with the command-line opcache off and 30 with it on; a fork is `pcntl_fork()`
of a booted process until the child runs, 50 forks in each of 5 processes.

| What a new worker pays | Median | Slowest |
| --- | ---: | ---: |
| Boot, opcache off: bootstrap / the whole process | 70.7 / 87.7 ms | 73.4 ms bootstrap |
| Boot, opcache on: bootstrap / the whole process | 154.2 / 174.0 ms | 178.3 ms bootstrap |
| Fork from a booted process | 0.48 ms | 1.15 ms |

- With the opcache on, a process started on its own boots twice as slowly: it
  creates its opcache memory and compiles every file into it, which only pays
  off for the processes that share that memory, the forked ones.
- The benchmark application is small: one job class and the package. An
  application with more providers and code boots slower. A fork copies the
  page tables of what was booted, so it grows with the application too; how
  much was not measured.

## Harness

This campaign added to `benchmark-queen/laravel-supervisors`:

- `sample.py` records the PSS split (`Pss_Anon`, `Pss_File`, `Pss_Shmem`) and
  six lines of the cgroup's `memory.stat`.
- `BenchmarkJob` allocates `BENCH_JOB_ALLOC_KB` KiB while it runs, in strings
  sized to one 1,024-byte slot of PHP's allocator each.
- `run.sh` passes `BENCH_HORIZON_OPCACHE_CLI` to the Horizon lanes, and
  records it and `BENCH_JOB_ALLOC_KB` in each campaign's `metadata.json`.

`metadata.json` names the application image `queen-laravel-supervisor-bench:local`:
`run.sh` read a fixed tag while Compose ran the `BENCH_APP_IMAGE` tag. The
lane containers ran `:memory` (above); the sampler, which only runs Python
from the mounted `scripts/`, ran `:local`. `run.sh` now reads
`BENCH_APP_IMAGE` too. The same applies to the campaigns of 2026-10-02 that set
`BENCH_APP_IMAGE`.

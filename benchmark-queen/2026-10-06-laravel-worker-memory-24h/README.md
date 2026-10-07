# 64 forked workers for 23 hours, 2026-10-06

The question: does a forked worker's private memory keep growing over a day of
work? The 2026-10-05 campaign
([`../2026-10-05-laravel-worker-memory`](../2026-10-05-laravel-worker-memory))
followed the workers for 20 minutes. This run stretches its `aging` lane to 23
hours of dispatch on the same droplet, with the PHP client 2.3.0 tree.

**Answer: no.** The median worker's private memory went from 9.66 to
10.04 MiB, in one step of about 0.35 MiB that a worker takes once, and stayed
there. The largest worker never passed 11.3 MiB. The application's
anonymous memory, all 64 workers and the master, grew from 638 to 658 MiB.

## Environment

- Host: the DigitalOcean droplet of the 2026-10-05 campaign: 16 vCPUs, 31 GiB,
  Ubuntu 24.04.5, cgroup v2. Nothing else ran on it.
- Budgets: the application 8 CPUs and 8 GiB, the broker 4 CPUs and 4 GiB.
- Broker: one Raft node, server `2.0.0-beta.3`, the image of the 2026-10-05
  campaign (`sha256:43f2f39d`).
- Application: `benchmark-queen/laravel-supervisors` (Laravel 12.68, PHP
  8.3.35) at commit `e877206b`, the PHP client 2.3.0 code with supervisor
  0.8.0, image `queen-laravel-supervisor-bench:aging24`.
  `scripts/harness-caps.diff` raises three harness caps for this run: 10
  million jobs instead of 1 million in `bench:dispatch` and `bench:results`,
  and no PHP memory limit for the result check.
- Queen: the Rust supervisor with prefork and the command-line opcache, 64
  fixed workers, prefetch 4, `ack_async`, `pop_ahead`, lease renewal in the
  supervisor, `block_for` 1 s, `--tries=1`, `--memory=128`, no `max_jobs` or
  `max_time`.

## The lane

`scripts/aging24.sh` ran `run.sh` once: 4,100,000 jobs dispatched one at a time
at 50 jobs/s, from 2026-10-06 12:17 to 2026-10-07 11:04 UTC (22.8 hours), about
64,000 jobs per worker. Each job sleeps 10 ms and builds 8 MiB of strings that it
releases at its end (`BENCH_JOB_ALLOC_KB=8192`). The sampler read every process
and both containers every 10 s until 15:02 UTC, 9,346 samples. No worker was
restarted: all 64 ran for the whole lane.

`raw/aging24.csv` takes one row per hour (`scripts/aging24.py`): the median and
the largest private memory of the workers, the PSS of the 64 workers in all,
the application's cgroup, and the broker's resident set and cgroup.

## Results

| Hour | Private, median | Private, largest | PSS of the 64 workers | Application anon |
| ---: | ---: | ---: | ---: | ---: |
| 0 | 9.66 MiB | 11.01 MiB | 653 MiB | 638 MiB |
| 6 | 9.67 MiB | 10.66 MiB | 657 MiB | 641 MiB |
| 12 | 9.67 MiB | 10.66 MiB | 661 MiB | 645 MiB |
| 15 | 10.04 MiB | 11.27 MiB | 674 MiB | 656 MiB |
| 22 | 10.04 MiB | 10.66 MiB | 676 MiB | 658 MiB |

- **The step.** A worker grows by about 0.35 MiB once, at its own moment: 6
  workers had taken it at the start, 31 by hour 13.5, 48 by hour 16, 51 by the
  end of the dispatch, and 13 never did. Its cause was not identified.
- **The application container** grew from 747 to 3,187 MiB, all of it page
  cache for the result files the harness writes: its anonymous memory is the
  column above.
- **The broker** kept every message: retention is off in this lane and
  4,100,050 messages took 3.2 GB of segments. Its anonymous memory grew with
  them, from 59 to 741 MiB, about 30 MiB an hour or 175 bytes per message
  stored, and stopped growing when the dispatch ended. Its cgroup reached the
  4 GiB limit at hour 13, page cache included, and stayed there.

## Four jobs failed without running

4,099,996 jobs completed, with no duplicate; the result check waited for the
last 4, so the run ended with exit code 1. Those 4 are in the dead-letter queue
with `App\Jobs\BenchmarkJob has been attempted too many times.`, between hours
14.5 and 16, each about 0.4 s after its push.

The broker counted a delivery that no worker received. A pop's answer waits for
the checkpoint that holds its lease; when nobody can receive it any more, the
broker hands the leases back, but it left the delivery attempt counted. The
next worker got the job as attempt 2, and with `--tries=1` Laravel failed it
before it ran. The broker's counters agree: it wrote every message into a pop
answer once (`queen_process_pop_messages_total` is 4,100,050, every message of
the queue), so the first claim of those 4 never left it. Checkpoint writes reached 0.45 s, against the 250 ms the broker
keeps before a pop's deadline. The broker fix,
[queen-mq/queen#95](https://github.com/queen-mq/queen/pull/95), takes the attempt back.

## Not measured

- More than one run, and Horizon: this lane ran Queen only.
- The cause of the 0.35 MiB step, and whether the broker's cgroup limit at
  hour 13 slowed its checkpoints into the window of the four failures.
- A broker with retention on, whose stored messages would stop growing.

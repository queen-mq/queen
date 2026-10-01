# Horizon against Queen on the Raft broker, 2026-10-01

Laravel Horizon against the Queen supervisor on the Queen Raft broker, with the
worker-side changes of this release measured one by one: lease renewal served by
the supervisor, asynchronous acknowledgements (`ack_async`) and popping the next
batch ahead (`pop_ahead`). The results are diagnostic, not a release performance
claim: one host, Docker Desktop, and a protocol written by the Queen team.

## Environment

- Host: Apple Silicon Mac, Docker Desktop, cgroup v2.
- Horizon lanes: Redis 7.4 with AOF, `appendfsync always` (every write is
  fsynced, as Raft does) or `everysec`, 2 CPUs and 2 GiB.
- Queen lanes: one Raft broker node (`raft` branch, `2.0.0-alpha.9`, commit
  `89e533ab`, image `queen-laravel-supervisor-broker:raft`) on a local data
  directory, 2 CPUs and 2 GiB. Every acknowledged write is fsynced in a group
  commit. The broker was not changed.
- Application: `benchmark-queen/laravel-supervisors` (Laravel 12, PHP 8.3), 4
  CPUs and 1 GiB, one fixed pool of 8 workers unless a lane says otherwise.
  Queen lanes use the Rust supervisor with prefork and the command-line
  opcache; Horizon lanes run Horizon's own supervisor.
- Jobs: `App\Jobs\BenchmarkJob` sleeps 10 ms, unless a lane says otherwise.
  Jobs are dispatched with `Queue::bulk()` in one burst at the start of each
  run, so the lanes measure how fast the workers drain a backlog; the latency
  lanes dispatch one job at a time at a fixed rate instead.

## Lanes

Each lane ran 5 times per engine, alternating engines, each run on a fresh
backend. All 165 runs in `raw/runs.csv` completed the exact job set with no
duplicate and no failure.

| Lane | Workload | Queen settings |
| --- | --- | --- |
| `strict`, `strict-all` | 5,000 jobs, fsync every write | prefetch 4; `-all` adds `pop_ahead` |
| `everysec`, `everysec-all` | 5,000 jobs, Redis `everysec`, Raft unchanged | as above |
| `ablation-none`, `ablation-lease`, `ablation-ack` | 5,000 jobs, Queen only | helpers or supervisor renewal, synchronous or asynchronous ACK |
| `noop`, `noop-all` | 10,000 empty jobs | as `strict` |
| `cpu` | 3,000 jobs of 20,000 SHA-256 rounds | as `strict` |
| `lean`, `lean-all` | 3,000 jobs, prefetch 1 | `-all` adds `pop_ahead` |
| `burst`, `burst-all` | 3,000 jobs, autoscaling 1 to 16 workers | with `event_driven` and `fast_scale_up` |
| `latency-100`, `latency-300` | 2,000 or 4,000 jobs at 100 or 300 jobs/s | prefetch 1, `ack_async` |
| `latency-300-all`, `latency-300-lean` | 4,000 jobs at 300 jobs/s | every feature, prefetch 4 or 1 |
| `prefetch8-all` | 5,000 jobs, Queen only | prefetch 8, every feature |

`everysec-dirty-tree` repeats `everysec` from a working tree with uncommitted
edits; its images were built from the same commit. It is kept for the record
and not used.

## Files

| File | What it records |
| --- | --- |
| `raw/faults/*.md`, `raw/faults/*.metadata.json` | The fault-recovery reports and their protocols |
| `raw/faults/long-tail-jobs.csv` | Every job of the long-tail fault run: worker, attempt, start and completion in seconds after the first enqueue |
| `raw/runs-final.csv` | The same rows for seven lanes repeated on the final commit, `85584a8f` |
| `raw/runs.csv` | One row per run: throughput, queue wait and end-to-end latency percentiles, memory (PSS of orchestrator, workers and lease helpers; cgroup memory of the application and the backend), CPU seconds, and scaling |
| `scripts/campaign.sh` | The lanes, as `laravel-supervisors/scripts/run.sh` invocations |
| `scripts/extract.py` | Builds `raw/runs.csv` from the campaign directories |

The per-run artifacts (samples, events, compose logs) stay in the git-ignored
`laravel-supervisors/results/2026-10-01-overnight`.

## Repeat on the final commit

The main lanes ran on commits `cb08f2cb` (without `pop_ahead`) and
`e0dd7e45` (with it). Fixes after the final review changed the client again,
so seven lanes ran again on `85584a8f`: `strict-all`, `everysec-all`,
`noop-all`, `lean-all`, `latency-300-all`, `latency-100-all` and `burst-all`.
All 65 runs were correct. Throughput and latency medians moved by at most 3%,
except the time to the burst's peak: 5.3 s for Queen and 6.3 s for Horizon,
against 4.3 s and 7.3 s before.

## Fault recovery

`laravel-supervisors/scripts/fault-recovery.sh` on the Raft broker, the
Queen lanes with prefetch 4, `ack_async` and renewal in the supervisor:

- `worker-sigkill`: 24 jobs of 2 s on 2 workers, `retry_after` 30 s; one
  worker is killed with SIGKILL during a job. Horizon and Queen both ran all
  24 jobs with no duplicate effect; the killed job ran again, as at-least-once
  delivery allows. The worker was back after 1.0 s (Horizon) and 1.4 s (Queen).
- `long-tail`: Queen only, 16 jobs of 8 s on 2 workers, `retry_after` 30 s,
  so the fourth job of a prefetched batch ends about 32 s after its pop; one
  worker is killed as above. The surviving worker completed job 8, the last of
  its first batch, 32.1 s after the enqueue, on its first attempt: the master
  had renewed the lease. All 16 jobs ran with no duplicate effect; the killed
  worker's batch ran again after its lease expired.

## How to reproduce

```bash
# The Raft broker image: the raft branch's root Dockerfile without the stage
# that builds the CLI (it needs a newer Go than the Dockerfile pins).
docker build -f <Dockerfile without the CLI stage> -t queen-laravel-supervisor-broker:raft <raft-checkout>

scripts/campaign.sh ../laravel-supervisors/results/<name> strict ablation-none ablation-lease \
  ablation-ack latency-100 latency-300 burst lean noop cpu
scripts/campaign.sh ../laravel-supervisors/results/<name> strict-all everysec-all noop-all \
  lean-all burst-all prefetch8-all everysec latency-300-all latency-300-lean
scripts/extract.py ../laravel-supervisors/results/<name> > raw/runs.csv

# Fault recovery.
../laravel-supervisors/scripts/fault-recovery.sh --output <dir>/worker-sigkill --engines horizon,queen-rust \
  --queen-storage raft --queen-prefetch 4 --queen-ack-async 1 --retry-after 30
../laravel-supervisors/scripts/fault-recovery.sh --output <dir>/long-tail --engines queen-rust \
  --queen-storage raft --queen-prefetch 4 --queen-ack-async 1 --jobs 16 --workers 2 --sleep-ms 8000 \
  --retry-after 30 --worker-timeout 10 --allow-lease-risk --completion-timeout 300
```

The figures are rendered by `webdoc/scripts/charts.py` from `raw/runs.csv`.

## Limits

- One host and Docker Desktop's virtual disk: the fsync cost, and with it every
  absolute number, differs on real servers.
- A single Raft node. A cluster adds a network round trip to every write.
- The workers, the broker and Redis share the host's CPUs with each other and
  with the sampler.
- The backlog lanes report latency, but with the whole backlog enqueued first it
  measures the queue's length, not the system's reaction; the paced lanes
  measure reaction.

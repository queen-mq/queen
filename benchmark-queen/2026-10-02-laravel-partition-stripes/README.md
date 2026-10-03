# More partition stripes than one pop checks out, 2026-10-02

The Queen Laravel driver spreads ordinary jobs over a queue's partition
stripes, and the broker leases each stripe to one worker at a time. Until
this campaign the driver allowed at most 64 stripes, the number of partitions
the broker checks out per pop, so at most 64 workers could run a queue's
ordinary jobs at once. Branch `feat/partitions-above-64` lets the driver use
up to 1024 stripes while each pop still asks for at most 64; the broker serves
the next ready partitions in turn. This campaign measures 64, 128 and 256
stripes on the same workloads. The protocol is the Queen team's; the results
are diagnostic.

## Environment

- Host: the DigitalOcean droplet of
  [`../2026-10-01-linux-vm-horizon-raft`](../2026-10-01-linux-vm-horizon-raft):
  16 vCPUs, 31 GiB, Ubuntu 24.04.5, kernel 6.8.0-142, Docker 29.1.3. Nothing
  else ran on it.
- Budgets: the application 8 CPUs and 8 GiB; the broker 4 CPUs and 4 GiB.
- Broker: one Raft node, server `2.0.0-beta.3`, the image of the release
  candidate's failure matrix, unchanged. Every acknowledged write is fsynced.
- Queen: the Rust supervisor with prefork, the command-line opcache, lease
  renewal in the supervisor, prefetch 4, `ack_async`, `pop_ahead` and the cURL
  transport.
- Application: `benchmark-queen/laravel-supervisors` (Laravel 12, PHP 8.3) at
  commit `f5d20737`; `App\Jobs\BenchmarkJob` sleeps 10 ms.

## Lanes

`scripts/stripes.sh` ran each lane once per stripe count, three times, the
stripe counts alternating inside each round, each run on a fresh broker.

| Lane | Workload |
| --- | --- |
| `low-50` | 6,000 jobs dispatched one at a time at 50 jobs/s, 16 workers |
| `paced-500` | 30,000 jobs dispatched one at a time at 500 jobs/s, 16 workers |
| `drain-64` | 50,000 jobs enqueued before 64 workers start (`--backlog-first 1`) |
| `drain-128` | the same with 128 workers |

## Results

All 36 runs completed the exact job set, with no duplicate and no failure.
Medians of three runs (`medians.md`, from `raw/runs.csv`); latency is end to
end, dispatch to completion:

| Lane | Stripes | jobs/s | p50 ms | p95 ms | p99 ms | broker CPU ms/job | broker MiB | pops/job |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| low-50 | 64 | 50 | 14.4 | 15.2 | 17.5 | 1.491 | 36 | 1.009 |
| low-50 | 128 | 50 | 12.6 | 13.4 | 17.7 | 1.533 | 33 | 1.010 |
| low-50 | 256 | 50 | 14.4 | 14.7 | 19.6 | 1.488 | 36 | 1.008 |
| paced-500 | 64 | 500 | 14.4 | 23.4 | 30.0 | 0.749 | 70 | 0.989 |
| paced-500 | 128 | 500 | 14.4 | 17.6 | 27.5 | 0.750 | 70 | 0.996 |
| paced-500 | 256 | 500 | 14.4 | 16.7 | 25.2 | 0.738 | 75 | 0.998 |
| drain-64 | 64 | 4580 | 7336.3 | 10729.9 | 11167.1 | 0.214 | 103 | 0.447 |
| drain-64 | 128 | 5380 | 7521.7 | 10128.3 | 10432.4 | 0.199 | 104 | 0.260 |
| drain-64 | 256 | 5519 | 7658.0 | 10211.1 | 10494.0 | 0.198 | 114 | 0.258 |
| drain-128 | 64 | 4722 | 7716.8 | 10958.2 | 11407.1 | 0.225 | 125 | 0.488 |
| drain-128 | 128 | 7895 | 5848.1 | 7129.7 | 7412.0 | 0.206 | 122 | 0.428 |
| drain-128 | 256 | 9257 | 5376.5 | 6256.7 | 6422.6 | 0.189 | 138 | 0.267 |

- **64 stripes cap the drain at about 4,700 jobs/s.** With 64 stripes, 128
  workers drained no faster than 64 (4,722 against 4,580 jobs/s). With 256
  stripes the 128 workers drained 9,257 jobs/s (runs 9,109 to 9,313), 96%
  more, and the drain's p95 fell from 11.0 s to 6.3 s.
- **More stripes than workers help too.** With 64 workers, 128 stripes drained
  17% faster than 64 (5,380 against 4,580 jobs/s) and 256 stripes 20% faster.
  The pops per job suggest why: with as many stripes as workers, more pops came
  back without a job (0.447 pops per job against 0.258), most likely because
  every free stripe was taken. This was not traced.
- **Paced load: a shorter tail.** At 500 jobs/s the p95 fell from 23.4 ms to
  17.6 ms with 128 stripes and 16.7 ms with 256; the median did not move.
- **No measured cost at low rate.** At 50 jobs/s, broker CPU per job, pops per
  job and latency stayed within run-to-run variation for the three counts.
- **Broker memory.** 256 stripes used up to 13 MiB more than 64 (138 against
  125 MiB in `drain-128`; 114 against 103 MiB in `drain-64`).

Not measured here: more than 256 stripes, several queues on one connection
(event-driven scaling watches every stripe of every queue, 1024 per fetch, and
leaves the rest to the regular poll), the PostgreSQL storage, and the PHP
supervisor.

## Rerun

On a Linux host with Docker, the Raft broker image and this repository at the
campaign's commit:

```bash
BENCH_APP_IMAGE=queen-laravel-supervisor-bench:stripes \
  benchmark-queen/2026-10-02-laravel-partition-stripes/scripts/stripes.sh /tmp/stripes 3
python3 benchmark-queen/2026-10-02-laravel-partition-stripes/scripts/extract.py /tmp/stripes > runs.csv
python3 benchmark-queen/2026-10-02-laravel-partition-stripes/scripts/extract.py --medians runs.csv
```

## Files

- `raw/<lane>/s<stripes>-r<run>/<run id>/queen-rust/fixed/r01/summary.json` and `report.md`: each run's
  measurements, written by `laravel-supervisors/scripts/run.sh`.
- `raw/campaign.log`: when each run started and ended.
- `raw/runs.csv`: one row per run, from `scripts/extract.py`; `medians.md`: its medians.

# Failures, replicas, a soak and Laravel features, 2026-10-02

Laravel Horizon on Redis, the Queen PHP supervisor and the Queen Rust
supervisor on the Queen Raft broker, each put through the failures a
production deployment meets: jobs that throw, time out or run out of memory,
killed workers and masters, deploys, backend restarts and stalls, a backend
that is down at dispatch, two replicas, and a 45-minute mixed soak. The same
lanes check that ordinary Laravel queue features behave the same on the three
engines. The protocol is the Queen team's; the results are diagnostic.

## Environment

- Host: the DigitalOcean droplet of
  [`../2026-10-01-linux-vm-horizon-raft`](../2026-10-01-linux-vm-horizon-raft):
  16 vCPUs, 31 GiB, Ubuntu 24.04.5, kernel 6.8.0-142, Docker. Nothing else ran
  on it, except where a section says that two campaigns overlapped.
- Budgets: the application 4 CPUs and 1 GiB; the broker 2 CPUs and 2 GiB;
  Redis 2 CPUs and 2 GiB (`compose.raft.yml` defaults).
- Horizon lanes: Redis 7.4.2 with AOF and `appendfsync always`.
- Queen lanes: one Raft broker node, server `2.0.0-beta.3`, unchanged. Every
  acknowledged write is fsynced. The Queen lanes run the production-like
  settings: prefetch 1, synchronous ACKs, no `pop_ahead`, lease renewal in the
  supervisor, prefork workers, the command-line opcache and the cURL
  transport. The `queen-rust-fast` lanes add prefetch 4, `ack_async` and
  `pop_ahead` to the Rust supervisor, and the `queen-php-fast` lanes (only in
  `local-hand-back`) to the PHP supervisor.
- Every lane: 2 workers unless it says otherwise, a pool `timeout` of 20 s,
  `retry_after` 30 s, `shutdown_grace` 35 s, a worker `--memory` of 128 MiB and
  PHP's `memory_limit` of 128 MiB.
- Application: `benchmark-queen/laravel-supervisors` (Laravel 12, PHP 8.3),
  the harness `scripts/failure_matrix.py`, the job
  `App\Jobs\FailureMatrixJob` and the commands `bench:matrix-dispatch`,
  `bench:matrix-report`, `bench:matrix-soak` and `bench:compat`. Each job logs
  its attempts to a file on the lane's results volume, so the checks read what
  the jobs did, not what the engine reports.

## Runs

| Directory under `raw/` | What ran | Code |
| --- | --- | --- |
| `failure-matrix` | the 14 failure lanes on Horizon, the Rust supervisor (`queen`) and the Rust supervisor with prefetch 4, `ack_async` and `pop_ahead` (`queen-fast`) | harness `53d286b7`, the PHP client of 2026-10-01 |
| `failure-matrix-php` | the same lanes on the PHP supervisor | harness `b39409f5`, the same client |
| `replicas`, `compat` | the replica lanes and the first 17 Laravel features, three engines | harness `6d980fb5`, the same client |
| `rerun-fixed` | the failure lanes again on the three Queen profiles, with the first client fixes | client fixes `324bf6d0` |
| `batch-prefix`, `batch-fixed` | the prefetched-batch lane before and after the client fixes | `7bbd4f0b` and `1639f33a` |
| `rc` | 32 Laravel features and the replica lanes, three engines, every client fix but the exit markers | `665d01c7` |
| `final-a`, `final-b`, `final-c` | every failure lane, the prefetched-batch lane, the 32 Laravel features and the replica lanes on the four profiles: the release candidate | `4d0ca51d` |
| `soak` | the 45-minute soak on the three engines, with every client fix but the exit markers | `cfd04eed` |
| `soak-pr-<profile>` | the 45-minute soak again, on the four profiles at once: PR #69's head, the release candidate | `eb9703b5` (image built at `fe2e8941`; only the harness changed) |
| `local-soak-prefetch` | a 15-minute soak of the prefetch-4 profile on Docker Desktop, with every client fix | `cfd04eed` |
| `local-compat-prefix` | the 32 Laravel features on Docker Desktop, before the client fixes | harness `bbb8ed7e`, the client of 2026-10-01 |
| `local-hand-back` | `job-timeout`, `memory-limit`, `memory-fatal`, `worker-kill` and `stop-long-batch` on the two Rust and two PHP profiles, on Docker Desktop, with the crash hand-back: the master's lease service, or the worker's PHP lease helper, sends the transaction a crashed worker journaled. `queen-php-fast` is the PHP engine with prefetch 4, `ack_async` and `pop_ahead` | `b18871cd`, `52e8e219` and `68d48cb4` (the harness); the image was built before the final wording of the dashboard's advice and of one docblock |

The first runs (`failure-matrix`, `failure-matrix-php`, `replicas`, `compat`, `batch-prefix`) found
the defects that the later runs show fixed. The tables report the last run of each lane.

## Lanes

| Lane | What happens | Expected |
| --- | --- | --- |
| `exception-retry` | 6 jobs throw on every attempt (`tries` 3, `backoff` 2) | 3 attempts at least 2 s apart, then one failed-job row each |
| `exception-once` | 6 jobs throw on their first attempt only | each completes on its second attempt, once |
| `release-and-fail` | 4 jobs `release()` once; 4 call `fail()` | a released job completes on its second attempt; `fail()` is final |
| `job-timeout` | 4 jobs of 15 s with `timeout` 3 (`tries` 2) | the worker is killed and replaced at each attempt; one failed-job row each |
| `memory-limit` | 6 jobs take the worker to 110 MiB, over `--memory` 96 | each job completes once; the worker is replaced |
| `memory-fatal` | 2 jobs allocate 256 MiB, over PHP's `memory_limit` 128 (`tries` 2) | the worker dies at each attempt; one failed-job row each |
| `worker-kill` | SIGKILL on a worker during one of 8 jobs of 4 s | the killed job runs again; all complete once |
| `master-kill` | SIGKILL on the master during 8 jobs of 4 s, then a restart | all complete once |
| `stop-short` | a deploy (`docker stop`, 90 s) during 2 jobs of 10 s | the jobs finish during the grace and do not run again |
| `stop-long` | a deploy during 2 jobs of 60 s, longer than `shutdown_grace` | the killed jobs run again; each completes once |
| `stop-long-batch` | one worker runs 2 jobs of 60 s of one prefetched batch (one Queen partition); a deploy kills it during the second | the first job, done before the deploy, does not run again |
| `backend-restart` | the backend restarts while 300 jobs of 100 ms run | all complete once |
| `pause-short` | the backend freezes for 5 s during 2 jobs of 10 s | nothing runs twice |
| `pause-long` | the backend freezes for 45 s, longer than the 30 s lease, during 2 jobs of 20 s | all complete; a job may run twice (at-least-once) |
| `dispatch-backend-down` | `dispatch()` while the backend is stopped, then again after it starts | the first throws; the others complete once |
| `replicas-kill` | two masters on one queue (coordination on); one is killed outright during 40 jobs of 2 s | all complete once |
| `replicas-rolling` | the two masters restart one after the other during 60 jobs of 1 s | all complete once |
| `soak` | 45 minutes at 5 jobs/s on 8 workers: 85% short, 5% of 5 to 20 s, 5% failing once, 3% always failing (`tries` 2), 2% delayed 10 to 60 s; a worker killed every 10 minutes; a deploy halfway | every job ends as its kind says; master and worker memory stay flat |
| `laravel-compat` | 32 Laravel queue features, each checked inside the supervisor's container (`bench:compat`, `bench:compat-more`) | the behaviour the Laravel documentation states; Horizon is the reference |

## Rerun

```bash
cd benchmark-queen/laravel-supervisors
python3 scripts/failure_matrix.py --build --output /tmp/matrix \
  --scenarios exception-retry,job-timeout,stop-long-batch,laravel-compat \
  --profiles horizon,queen-php,queen-rust,queen-rust-fast
python3 ../2026-10-02-laravel-failure-matrix/scripts/tables.py   # this dataset's tables
```

`MATRIX_SOAK_SECONDS` sets the soak's length (2700 by default). `BENCH_APP_IMAGE` gives a run its
own application image, so two runs can share a Docker host.

## Files

- `raw/<run>/<lane>/<profile>.json`: the checks, the timeline and every job's attempts; `.log.gz`
  next to it holds the lane's container logs (`zcat` reads them).
- `raw/soak-memory.csv`, `raw/soak-events.csv`: the soak's memory samples and its kills and deploy,
  from `scripts/soak_memory.py`.
- `tables.md`: the tables below, from `scripts/tables.py`.

## Results

`tables.md` has every table; `scripts/tables.py` writes it from `raw/`.

- **Release candidate (`final-*`, `4d0ca51d`):** 72 lanes. Every lane passed on Queen PHP and Queen
  Rust. Horizon passed all but `backend-restart`, where 2 of 300 jobs ran twice. The Rust engine with
  prefetch 4, `ack_async` and `pop_ahead` passed all but `memory-fatal`, the crash limit below.
- **Laravel features:** all 32 passed on the four profiles. For `backoff` `[1, 3]` the retries came
  after 1.81 and 4.03 s on Horizon (Laravel's Redis queue stores due times in whole seconds), and
  after 1.02 to 1.04 and 3.05 s on Queen. `queue:clear` is not supported on Queen: the command fails
  and the jobs still run (recorded by `queue-size`, not checked).
- **Replicas:** both lanes passed on the four profiles.

What the first runs found, all fixed in the release candidate:

| Lane | Profile | First run | Fixed by |
| --- | --- | --- | --- |
| `job-timeout` | Rust, prefetch 4 | 3 of 4 jobs reached `failed_jobs` after one run | the hand-back keeps the attempt (`ac99ce9c`) |
| `memory-limit` | Rust, prefetch 4 | 1 of 6 jobs failed instead of completing | the same |
| `memory-fatal` | Rust, prefetch 4 | both jobs failed after one run | the same; 1 of 2 still failed, the crash limit; passed with the crash hand-back (`local-hand-back`, Rust and PHP) |
| `stop-long` | Rust, prefetch 4 | 1 of 2 jobs completed twice | the detached ACK is written before the next job (`324bf6d0`) |
| `stop-long-batch` | Rust, prefetch 4 | the job done before the deploy ran again (`batch-prefix`) | the same (`batch-fixed`: passed on the four profiles) |
| `job-timeout` | PHP and Rust | passed, but the crash circuit held the pool: 126 to 147 s | a job timeout is not a crash (`e62efc05`, `e174964e`): 44 s |
| `fork-in-job` | PHP | no job completed: the forked child stopped the renewal helper, and its parent was SIGKILLed (`local-compat-prefix`, Docker Desktop) | `2d35d16d` |

Known limits:

- **A crash charges an attempt to every unstarted prefetched job.** In `memory-fatal` with prefetch 4
  and `tries` 2, job `000001` never ran: job `000000` crashed the worker twice, and each crash
  returned `000001` with one more delivery. In `worker-kill` (`final-b`), the three jobs the killed
  worker had not started waited for its lease to expire and ran 31 s later as their second attempt.
  With the crash hand-back (`local-hand-back`), the 20 lanes passed on the four profiles:
  - `memory-fatal`, Rust, prefetch 4: the master logged the hand-back of the job that never started
    10 ms after the fatal error; both jobs then ran twice, as `tries` 2 says.
  - `worker-kill`, Rust, prefetch 4: the master handed back the three jobs that never started, and
    they ran 4 to 12 s after the kill as their first attempt.
  - `worker-kill`, PHP, prefetch 4: the PHP helper logs nothing once its worker is gone, but none of
    the jobs the killed worker had not started waited for its lease: they ran from 2.5 s after the
    kill as their first attempt.
  - The job that was running when its worker died was alone in its partition lease in each of
    these runs, so it returned when the lease expired, 30 s later, with its run counted.

  Still charged: a lost node, a crash that takes the lease helper with the worker, and a batch
  popped ahead and not read yet, which no lane here exercises.
- **Horizon after a Redis restart.** Jobs `000066` and `000067` finished while Redis restarted; the
  worker could not remove them from the reserved set, so they ran again when their reservation
  expired 30 s later (`retry_after`), in both runs.

Fixture defects found and fixed on the way, none in Queen or Horizon: SQLite refused concurrent batch
updates (fixed by taking the write lock first, `RetryingBatchRepository`); the pause helper ignored
draining workers; a soak drained before its last delayed jobs were due; an empty PHP array arrived
as a JSON list; the matrix job's readonly properties broke its partitioned subclass; and
`bench:matrix-report` read a whole attempt log with PHP's default 128 MiB. That last one cost a
2.5-hour soak of the release candidate on the four profiles: all four ran to the end, 45,001
jobs each with a worker killed every 10 minutes and a deploy, but the report died on the
98,000-event log (reproduced with a synthetic log: exit 255 at 128 MiB, a summary at 512 MiB), and
the lanes' volumes were removed with it, so that run has no verdict and is not in `raw/`. The
matrix tools now run with 1 GiB.

**The soak (`soak`):** 45 minutes at 5 jobs/s on 8 workers, a worker killed every 10 minutes and a
deploy after 23 minutes. Each engine received 13,501 jobs, and every one ended as its kind says. The
398 permanent failures left 398 failed-job rows, and on Queen 398 dead-letter entries. The master's
resident memory stayed flat: Horizon 48.9 MiB, Queen PHP 58.3 to 57.8 MiB, Queen Rust 6.9 MiB. The
median worker grew slowly, as long-lived PHP workers do: Horizon 50.7 to 54.7 MiB, Queen PHP 40.9 to
47.0 and Queen Rust 40.5 to 45.0 (first third against last third, `raw/soak-memory.csv`). The soak
ran before the exit markers, which change only how a worker killed by a job timeout restarts; no
soak job timed out.

**The soak of the release candidate (`soak-pr-*`):** the same 45 minutes on PR #69's head, the four
profiles at once. Every profile received 13,501 jobs, and every one ended as its kind says; 398
failed-job rows, and on Queen 398 dead-letter entries. Masters flat: Horizon 49.1 MiB, Queen PHP
58.5 MiB, Queen Rust 7.0 MiB with and without prefetch 4. Median workers: Horizon 51.0 to 56.6 MiB,
Queen PHP 40.5 to 44.9, Queen Rust 40.4 to 43.0, Queen Rust with prefetch 4 41.1 to 45.0.

The local soak (`local-soak-prefetch`, Docker Desktop, 15 minutes, prefetch 4 with `ack_async` and
`pop_ahead`, every client fix): 4,501 jobs ended as their kind says, 124 failed-job rows and 124
dead-letter entries for 124 permanent failures, the master flat at 6.3 MiB, the median worker from
38.5 to 42.3 MiB.


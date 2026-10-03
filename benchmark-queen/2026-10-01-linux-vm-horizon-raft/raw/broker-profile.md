# The Raft broker's CPU at a low, steady rate

At 500 jobs/s with one job per dispatch (`paced-500`), the Raft broker spent
0.87 ms of CPU per job against Redis's 0.49; under a bulk load it spent 0.23
against 0.37. This file explains the difference. The broker was not changed.

## Counters

Broker counter deltas per job, from the Prometheus scrapes before and after
each run (`scripts/promdelta.py`, first run of each lane):

| Per job | `paced-500` | `throughput-32` |
| --- | --- | --- |
| push requests | 1.00 | 0.01 (100 jobs per push) |
| pop requests | 1.98 | 0.26 |
| ack requests | 1.00 | 1.00 |
| Raft proposals, one log fsync each | 1.38 | 0.08 |
| store read transactions | 12.9 | 3.2 |
| store rows written | 26.9 | 7.3 |
| store rows deleted | 3.5 | 1.0 |
| checkpoint rows | 10.0 | 1.8 |
| planner effects commands | 1.51 | 0.48 |

Under a bulk load one Raft entry carries about 12 jobs, and its fixed costs
(the applied index, request ids, counters, a thread hand-off at each pipeline
stage) are shared. At 500 jobs/s each job is its own entry and pays them alone.

## Profile

The same lane on Docker Desktop (Apple Silicon, linuxkit kernel 7.0.12)
reproduced the server's counters: 0.88 ms of broker CPU per job, 1.93 pops,
1.40 entries, 12.6 read transactions and 26.5 rows written per job. The broker
was built from the same commit with `CARGO_PROFILE_RELEASE_DEBUG=line-tables-only`
and profiled for 40 s of the measured window with
`perf record -F 499 --call-graph dwarf` (`scripts/profile/capture.sh`); the
run with perf attached spent 0.888 ms per job, the run without 0.88.

Self time by kind (7,357 samples, 0.48 cores):

| Kind | Share |
| --- | --- |
| kernel: thread wake-ups and scheduling | 26.0% |
| kernel: syscall entry and other | 9.2% |
| kernel: TCP/IP | 3.7% |
| kernel: eventfd and epoll | 3.0% |
| kernel: file system and disk | 2.2% |
| user: broker code | 10.2% |
| user: HTTP stack (hyper, axum) | 8.4% |
| user: LMDB | 6.1% |
| user: syscall stubs and atomics | 5.0% |
| user: allocator | 4.9% |
| user: clock reads | 4.2% |
| user: tokio | 3.5% |
| user: other (zstd, Rust std, memcpy, JSON and the rest) | 12.3% |

By pipeline stage (`scripts/profile/ops.py`): the Raft planner 19.5%, log
write and fsync 12.6%, the queue log 6.6%, apply 6.6%; HTTP connection I/O
12.8%, pop 7.4%, ack 6.2%, push 2.7%, other routes 4.8%; the tokio scheduler
12.3%; the consumer engine 2.6%. System calls come from
`scripts/profile/syscalls.py`.

The futex system call alone took 21.8%, spread over many hand-offs: waking a
blocking-pool thread for each pop's render (2.7%), the tokio scheduler (3.3%),
the planner's notifications to the log writer (2.4%), the syncer and the log
writer handing a group on (2.5%), and so on. The fsync itself costs 2.9%.

## The client's share

`pop_ahead` sent a pop that never waits with the last job of every batch. At a
low rate the batch is one job and the pop ahead comes back empty: 1.97 pops per
job. Waiting for a full batch before popping ahead (client commit `e4d22580`)
halved the pops and took 12% off the broker's CPU per job (`popfix` group).

## What would cut the rest

Proposals for the broker, not implemented: run the pipeline stages on fewer
threads when the load is low, read in-memory store rows inside the pop handler
instead of a blocking-pool hop, fold the consumer checkpoint's 5 ms effects
into fewer Raft entries, and read the clock less often for the stage timers.

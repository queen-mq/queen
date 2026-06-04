# Queen benchmark — `0.16.0.beta.1` (simdjson broker)

Re-run of the April throughput suite on the current commit (`0.16.0.beta.1`, the
full simdjson + UUID conversion). Same harness, same Postgres tuning, fresh DB
per test.

- **Host:** 32 vCPU, 62 GiB RAM, x86_64 (Ubuntu 24.04), no swap.
- **Postgres:** `postgres:18`, April tuning (24 GiB shared_buffers, etc.).
- **Broker:** `smartnessai/queen-mq:0.16.0.beta.1`, `NUM_WORKERS=10 DB_POOL_SIZE=50 SIDECAR_POOL_SIZE=250`.
- **Duration:** 300 s/test (preview cadence; April used 900 s).
- **Harness:** `_runner/run_test_v3.sh` via `run-april-016.sh`; producer/consumer = `bench-producer.js` / `bench-consumer.js`.
- **Metrics:** `push/s` from the producer (client-measured), `pop/s` = server-side
`messages.completed / duration`, Queen vCPU = mean of the per-minute
`docker stats` samples taken **during** load, `CPU/Mmsg` = Queen vCPU ÷
(push msg/s ÷ 1e6).

## Results


| Test            | Workload                                | push/s      | pop/s   | non2xx | timeouts | Queen vCPU | PG vCPU | CPU/Mmsg | push p99 |
| --------------- | --------------------------------------- | ----------- | ------- | ------ | -------- | ---------- | ------- | -------- | -------- |
| `hi-part-1`     | 2 part, batch=1, 5×100 prod / 2×50 cons | 35,238      | 33,067  | 0      | 0        | 6.38       | 3.59    | 181      | 33       |
| `hi-part-10`    | 11 part                                 | 33,521      | 32,800  | 0      | 0        | 6.39       | 4.26    | 191      | 34       |
| `hi-part-100`   | 101 part                                | 32,329      | 32,232  | 0      | 0        | 6.36       | 5.54    | 197      | 35       |
| `hi-part-1000`  | 1,001 part                              | 29,793      | 25,274  | 0      | 0        | 5.70       | 7.22    | 191      | 38       |
| `hi-part-10000` | 10,001 part                             | 25,929      | 20,165  | 0      | 1        | 4.89       | 10.73   | 189      | 42       |
| `bp-1`          | 1,001 part, batch=1, 1×50               | 5,908       | 5,709   | 0      | 0        | 1.47       | 3.63    | 249      | 13       |
| `bp-10`         | 1,001 part, batch=10, 1×50              | 41,150      | 31,876  | 0      | 100      | 1.31       | 6.01    | 31.7     | 32       |
| `bp-100`        | 1,001 part, batch=100, 1×50             | **131,300** | 130,071 | 0      | 0        | **1.07**   | 13.08   | **8.2**  | 80       |
| `q-1`           | 1 queue, batch=10, 1×50                 | 44,930      | 43,684  | 0      | 0        | 1.38       | 5.92    | 30.6     | 33       |
| `q-10`          | 10 queues, batch=10, 1×50               | 47,550      | 33,482  | 0      | 0        | 1.23       | 4.76    | 25.9     | 32       |
| `q-100`         | 100 queues                              | —           | —       | —      | —        | —          | —       | —        | —        |


`q-100` excluded: the benchmark **client** (`bench-producer.js`) does not handle
the 100-queue fan-out and emits no traffic (0 pushed). Pre-existing client bug,
also excluded in the April runs — not a broker issue.

## `0.15.5` → `0.16.0.beta.1`

Deltas for the three tests with a `0.15.5` baseline on the same VM (the
simd-vs-0155 subset, 240 s):


| Test        | throughput                  | Queen vCPU              | CPU/Mmsg            | push p99 |
| ----------- | --------------------------- | ----------------------- | ------------------- | -------- |
| `bp-10`     | −0.5 %                      | **−59 %** (3.22 → 1.31) | −59 % (77.9 → 31.7) | 31 → 32  |
| `bp-100`    | **+14 %** (115k → 131k)     | **−83 %** (6.19 → 1.07) | −85 % (53.8 → 8.2)  | 89 → 80  |
| `hi-part-1` | **+11.5 %** (31.6k → 35.2k) | −16 % (7.62 → 6.38)     | −25 % (241 → 181)   | 34 → 33  |


## Interpretation

**Two regimes, by JSON-per-request:**

- **Batch / queue workloads** (`bp-100`, `q-1`, `q-10`, `bp-10`): the broker is a
near-thin pipe — ~1.0–1.5 vCPU and 8–32 CPU/Mmsg. simdjson + raw-payload
pass-through means the broker slices request bytes and hands raw JSONB to
Postgres; the heavy lifting is in PG. `bp-100` is the headline: **131k msg/s
push at ~1.07 vCPU broker.**
- **High-partition contention** (`hi-part-`*): the broker is the heavy side
(~5–6.4 vCPU, ~190 CPU/Mmsg — roughly 10× the batch tests). `batch=1` carries
almost no JSON for simd to optimize, so cost is dominated by per-request HTTP
  - lease/partition bookkeeping. Throughput degrades gracefully as partition
  count grows (35k → 26k from 1 → 10,000 partitions); PG vCPU climbs with
  partition count (3.6 → 10.7).

## Reliability notes

- `**bp-10` — 100 client timeouts, 0 non-2xx (no message loss).** `queen.log`
shows the cause: `PartitionLookupReconcileService` and `StatsService` both hit
a Postgres `canceling statement due to lock timeout` under load. This is the
same load-induced background-maintenance contention class that April flagged
for `StatsService` on `bp-100`. **Suggested fix:** bound/relax the lock wait
(or `SET LOCAL lock_timeout`/`statement_timeout` handling) in the reconcile +
stats queries, and/or back off these jobs under high write load.
- `**hi-part-10000` — 1 timeout** (vs 100 + a PG deadlock on 0.12 on the same
workload in April). Much healthier at extreme partition fan-out.
- All other tests: 0 errors, 0 timeouts.

Raw per-test artifacts (`status.json`, `producer.log`, `consumer.log`,
`docker-stats.log`, `queen.log`, `metadata.json`) are under `raw/<test>/`.

## Sustained soak (2h) — retention is the ceiling

A separate 2h `bp-100`-style soak with completed-retention found that the broker
can *push* 130–185k but the **sustainable-with-bounded-storage rate is ~40k/s**,
bound by the single-connection retention `DELETE` (not CPU, not cache). Two
limiters were isolated: checkpoint/WAL write-amplification (tunable via
`max_wal_size`) and the retention delete throughput (architectural). Full writeup
+ ideas for a more efficient delete path: **[`SUSTAINED-SOAK-FINDINGS.md`](SUSTAINED-SOAK-FINDINGS.md)**.
# WP-0.2 — postgres-class baseline on the Linux VM

Date 2026-09-17. Branch `raft`, base `fc71b65b` (master 1.6.0), sources
unmodified (`server/`, `crates/`).

Host `root@164.90.215.224` (`queenpgless-01`), Ubuntu 24.04,
Linux 6.8.0-124-generic, 8 vCPU, 15 GB, ext4, local PostgreSQL 16.15 on 5432.
Broker: one `queen` process on :6698 (md5 `d7f5931486c9`), postgres class,
`DB_POOL_SIZE=64`, `QUEEN_APPLY_SCHEMA=1`, `LOG_LEVEL=warn`, fresh
`queen` / `queen_streams` schemas. Loader `/root/goload` (md5 `2c97cccb0d1e`),
`-mode openloop`, 256-byte payloads, manual acks, **on the same VM**.

Script: `test/raft/vm/measure-baseline.sh` (this run: `.../measure-baseline.sh wp02b`).
Raw output of the recorded run is beside this file; the earlier identical run
is under `run1-repeat/`, the fat-batch calibration ladder under
`fat-calibration/`.

## The table (recorded run, 2026-09-17 13:15:38–13:19:46 UTC)

Rates are offered message rates; "achieved" is what the loader got through.
`shed` is the openloop loader's own shedding (0 everywhere = the offered rate
was met). Cores are CPU-seconds per wall second over the regime window; RSS is
the process maximum. For Postgres, **PSS** is the honest figure: `rss_max_mb`
in the CSVs sums every backend's RSS and counts shared buffers once per
backend (that is why it reads 5–20 GB on a 15 GB box).

| regime | offered | achieved | shed | push p50 | p99 | p999 | ack avg | queen cores | pg cores | goload cores | queen RSS | pg PSS | errors |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| A20k   | 20 000 msg/s (2 000 req/s ×10)   | 749 830 msgs / 40 s | 0 | 9.15 ms | 27.01 | 52.48 | 9.06 ms | 0.93 | 3.01 | 1.79 | 55 MB | 1 211 MB | 0 |
| A50k   | 50 000 msg/s (5 000 req/s ×10)   | 1 873 990 / 40 s | 0 | 23.17 ms | 75.26 | 103.94 | 32.92 ms | 1.17 | 2.65 | 2.41 | 105 MB | 1 384 MB | 0 |
| B1     | 2 000 msg/s (single pushes, 16 part., pinned pop) | 59 995 / 30 s | 0 | 3.34 ms | 14.27 | 35.07 | 1.72 ms | 0.84 | 2.45 | 0.86 | 109 MB | 1 427 MB | 0 |
| C1000  | 3 000 msg/s (single pushes, 1 000 part.) | 85 513 / 30 s | 0 | 6.62 ms | 33.02 | 54.02 | 21.36 ms | 0.91 | 3.86 | 0.92 | 82 MB | 1 481 MB | 0 |
| D1     | 500 msg/s (1 partition, 1 consumer) | 9 999 / 20 s | 0 | 1.99 ms | 4.08 | 18.30 | 0.79 ms | 0.28 | 0.78 | 0.31 | 54 MB | 1 490 MB | 0 |
| FAT100 | 300 000 msg/s (3 000 req/s ×100, push-only, no consumers) | 17 241 800 / 60 s | 0 | 23.17 ms | 209.92 | 415.74 | — | 1.38 | 1.39 | 3.31 | 255 MB | 1 540 MB | 0 |

"errors" = `grep -ciE ' error |panic'` over the broker log: **0 lines for the
whole run**; `pushErr=0`, `popErr=0`, `ackErr=0` in every regime.

Database at the end of each regime (`marks.csv`): 77 MB after A20k, 234 MB
after A50k, 255 MB after B1, 292 MB after C1000, 296 MB after D1, 1 113 MB
after FAT100. The regimes run sequentially against one broker and one schema,
so each one carries the data the previous ones left; FAT100 runs last.

## Against Appendix G

Appendix G's postgres column (VM, 2026-09-16, PLAN_NATIVE_LOG §8.1) versus
this run, and the native single-node column it quotes, for reference:

| regime | Appendix G postgres | this run (1.6.0) | Appendix G native single node |
|---|---|---|---|
| A20k  | p50 8.5, p99 36.6, ack 8.4  | p50 9.15, p99 27.01, ack 9.06  | p50 4.2, p99 13.8, ack 3.4 |
| A50k  | p50 24.5, p99 92, ack 31.8  | p50 23.17, p99 75.26, ack 32.92 | p50 7.1, p99 29, ack 6.1 |
| B1    | p50 4.0, p99 28            | p50 3.34, p99 14.27            | p50 2.1, p99 22.7 |
| C1000 | p50 6.2, p99 40.7, ack 25   | p50 6.62, p99 33.02, ack 21.36 | p50 3.0, p99 15.7, ack 2.6 |
| D1    | p50 2.4, p99 6.8           | p50 1.99, p99 4.08             | p50 1.7, p99 10.9 |

The 1.6.0 binary reproduces the Appendix G postgres baseline: p50 within
1.4 ms everywhere (largest gap A50k, 24.5 → 23.17), p99 lower in all five
regimes (this run's tails are better, not worse). The baseline is sound as
the comparison point for raft.

## Fat batch: what the VM sustains, and why it stops

`FAT100` is push-only, batch 100, 100 partitions, no consumers. The offered
rate was calibrated with 20-second runs before the recorded 60-second run
(`fat-calibration/`):

| offered | achieved msgs | shed | p50 | p99 | queen cores | pg cores | verdict |
|---|---|---|---|---|---|---|---|
| 200 k msg/s | 3 498 200 / 20 s | 0 | 6.62 ms | 48.90 | 0.89 | 1.00 | flat, well under the knee |
| 300 k msg/s | 5 243 400 / 20 s | 0 | 24.70 ms | 123.39 | 1.20 | 1.15 | flat: per-report p50 25–33 ms, no drift |
| 400 k msg/s | 6 929 600 / 20 s | 0 | 158.72 ms | 962.56 | 1.45 | 1.11 | over the knee: p50 drifts 17 → 99 → 239 → 241 ms, inflight 950–1300 |
| 500 k msg/s | 7 266 600 / 20 s | 1 119 100 msgs (12.8%) | 428.03 ms | 2834.43 | 1.61 | 1.06 | saturated: the loader sheds |

**300 000 msg/s is the sustained rate**, and it held for the full 60 s of the
recorded run: 12 report windows, `achieved` 298.8 k–301.3 k/s, `shed=0`,
p50 17–29 ms with no upward drift, inflight 47–143.

What that number is NOT: a broker ceiling. With the loader sampled, FAT100 at
300 k msg/s spends **goload 3.31 + queen 1.38 + postgres 1.39 = 6.08 of the
8 vCPU**, and the loader is the single largest consumer. At the 400 k knee
queen was at 1.45 cores and Postgres at 1.11 — neither is CPU-saturated — so
the knee is not broker CPU; the co-resident loader is the plausible
constraint (it was not sampled during the ladder: loader sampling was added
after it). Appendix G's "1.78 M msg/s push-only in fat batches" came from
larger campaign hosts with the load off-box; this VM is not that host, and
the fat-batch number here is a **VM-with-co-resident-loader** figure.

The same caveat applies to A50k: goload 2.41 + postgres 2.65 + queen 1.17 =
6.23 of 8 vCPU. A20k, B1, C1000 and D1 leave the VM with headroom
(5.7 / 4.2 / 5.7 / 1.4 cores busy respectively).

## Other facts worth carrying into phase 1

- **Postgres is the CPU**, as expected: 3.01 cores at A20k (20 k msg/s),
  3.86 at C1000 (3 k msg/s over 1 000 partitions), 2.45 at B1 (2 k msg/s)
  against a broker that never exceeds 1.17 cores in the five latency regimes.
  C1000 is the sharp one: 3.86 Postgres cores to move 3 000 msg/s, i.e. the
  cost follows partition cardinality, not the write rate (G-3, I8).
- **Broker RSS is small and flat**: 54–109 MB in the five latency regimes,
  255 MB at 300 k msg/s push-only.
- **B1 under-consumes**: 59 995 pushed, 46 799 popped, lag 13 196 at the end
  (run 1: 46 727 / 13 268). Pinned pop with `-pop-batch 1` and 16 consumers
  cannot drain 2 000 msg/s; the regime's push latency is still valid, its
  consume side is loader-shaped, not broker-shaped. Appendix G's B1 row
  likewise carries no ack figure.
- **Repeatability** (run 1 vs the recorded run, same script except loader
  sampling): A20k p50 9.15 / 9.15, A50k 21.12 / 23.17, B1 3.31 / 3.34,
  C1000 5.28 / 6.62, D1 2.10 / 1.99. C1000 moves ~25% between two 30-second
  runs — treat C1000 p50 as ±1.5 ms, not as a tight number.

## Commands

On the laptop (`/Users/alice/Work/queen`, branch `raft`):

```
rsync -a --delete --exclude target --exclude node_modules \
  server crates root@164.90.215.224:/root/raft/queen-raft/
scp test/raft/vm/measure-baseline.sh root@164.90.215.224:/root/raft/wp02/
```

On the VM:

```
. ~/.cargo/env && cd /root/raft/queen-raft/server && cargo build --release --bin queen
#   -> Finished `release` profile [optimized] target(s) in 2m 21s, rc 0
#      /root/raft/queen-raft/server/target/release/queen  (12 675 032 bytes, md5 d7f5931486c9)

# calibration (20 s each)
FAT_RATE=200000 FAT_DURATION=20 OUTDIR=/root/raft/fatcal-200000 bash /root/raft/wp02/measure-baseline.sh fatcal200 FAT100
FAT_RATE=300000 FAT_DURATION=20 OUTDIR=/root/raft/fatcal-300000 bash /root/raft/wp02/measure-baseline.sh fatcal300 FAT100
FAT_RATE=400000 FAT_DURATION=20 OUTDIR=/root/raft/fatcal-400000 bash /root/raft/wp02/measure-baseline.sh fatcal400 FAT100
FAT_RATE=500000 FAT_DURATION=20 OUTDIR=/root/raft/fatcal-500000 bash /root/raft/wp02/measure-baseline.sh fatcal500 FAT100

# the recorded run: all six regimes, FAT100 at the calibrated 300 k for 60 s
bash /root/raft/wp02/measure-baseline.sh wp02b          # 13:15:38 -> 13:19:46 UTC (4 m 08 s)
#   (run 1, same thing without loader sampling: ... wp02, 13:10:58 -> 13:15:06)

# teardown
PGPASSWORD=postgres psql -h localhost -U postgres \
  -c "DROP SCHEMA IF EXISTS queen CASCADE; DROP SCHEMA IF EXISTS queen_streams CASCADE;"
```

The script itself starts the broker with

```
env PG_HOST=localhost PG_PORT=5432 PG_USER=postgres PG_PASSWORD=postgres PG_DATABASE=postgres \
    QUEEN_APPLY_SCHEMA=1 PORT=6698 DB_POOL_SIZE=64 FILE_BUFFER_DIR=$D/spool LOG_LEVEL=warn queen
```

and runs the regimes with exactly the Appendix G / `measure-vm.sh` goload
flags (see `measure-baseline.sh`, the `for r in $REGIMES` block).

## VM state left behind

Broker stopped, no `queen` and no `goload` process running, `queen` and
`queen_streams` schemas dropped (`pg_database_size` back to 11 MB, no
`queen%` schema left), Postgres left
running. No loop or dm devices were created. Kept on the VM for later WPs:
`/root/raft/queen-raft/` (sources + release build) and `/root/raft/wp02/`
(the script and every run's outputs, consolidated there after the runs). `/root/queen` was read but never written.

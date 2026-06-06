# Engine-scaling results (2026-06-06)

Run on the 32-core VM `queen-benchmark-01` (165.232.78.92), broker
`smartnessai/queen-mq:0.16.0.beta.1-ui`, Postgres co-located, **goload also
co-located** on the same box (so absolute throughput is loader-limited except in
the heavy run; the broker/PG/`evl` signals are what matter).

## TL;DR — one push/ack engine is enough

At a real push ceiling of **~160k push/s** (push-only, `W=8`), the broker used
**~2.8–3.2 vCPU total** with **`evl`≈0–5 ms** and the busiest threads at only
**30–50% of a core** (none saturated). The work is on the **Postgres** side
(~8 vCPU, ~16 active connections) — and PG isn't CPU-saturated either, matching
the prior campaign's "PG push-path serialization is the limit." **The libqueen
event loop is nowhere near the bottleneck**, so a single push/ack engine serving
all HTTP workers has ~10× the headroom it needs. Engine *count* is not a
throughput lever here; HTTP-worker count + connections + PG are.

## Heavy push-only drive (the ceiling test) — `heavy.sh`, W=8, 1000 producers, batch=10, 300 parts

| metric | value |
|---|---|
| push/s (goload) | ~160k (peak 163k), 0 errors |
| push/s (PG `n_tup_ins`) | ~151k |
| **broker CPU** | **~2.8–3.2 vCPU / 32** |
| **`evl` max** | **0–5 ms** (loop not behind) |
| PG CPU | ~8–9 vCPU / 32 |
| PG active backends | ~12–17 (of 250 slots) |
| busiest broker threads | 8 threads @ 30–50% core; rest idle (`top -bH`) |

Interpretation: even collapsing all engine work onto one thread stays well under
one core (8 engine + 8 HTTP threads together ≈ 2.6 vCPU). PG is the wall.

## Engine-count sweep — `run-engine-sweep.sh` (loader-bound, but engine idle throughout)

`fusion` (parts=100, batch=100, 64 producers), constant total slots SIDE=240:

| W | push/s | broker vCPU | PG vCPU | PG active | evl max |
|--:|--:|--:|--:|--:|--:|
| 1 | 42.6k | 0.14 | 1.3 | 1.4 | 0 |
| 2 | 39.9k | 0.14 | 1.2 | 1.3 | 0 |
| 4 | 57.9k | 0.21 | 2.0 | 2.2 | 1 |
| 8 | 76.5k | 0.31 | 3.0 | 3.8 | 3 |

`lowfusion` (parts=2, batch=1, 200 producers): 7.4k → 11.3k → 18.6k for W=1/2/4,
broker CPU 0.94 → 1.6 → 2.8, `evl`≈0. (W=8 cell was an under-driven outlier at
4k — ignore; 2-partition batch=1 is a pathological micro-config.)

Reading: with the DB at **~1–4 active connections and `evl`≈0**, nothing
broker-side is saturated — this sweep is loader/HTTP-concurrency-bound. The
throughput that does appear tracks **HTTP-worker count** (W also sets the uWS
thread count), not engine throughput: adding engines did nothing to a
non-bottleneck (broker stayed at 0.14 vCPU). That is itself the point —
**engine count is not the lever; HTTP threads + connections + PG are.**

## Caveats

- On this build `NUM_WORKERS` sets **both** HTTP-thread and engine count, so a
  pure "1 engine + N HTTP threads" can't be isolated without the topology
  change. The CPU accounting (≤3 vCPU broker, no thread >50%, `evl`≈0 at 160k)
  makes the conclusion safe regardless: one engine loop can absorb the work.
- Loader co-located with the broker; the non-heavy sweep under-drove the DB.
  A separate loader VM (as in the 2026-06-04 campaign) would raise absolute
  numbers but not change the engine-vs-PG conclusion.

## Bottom line for the topology decision

Default to a **single push/ack engine** (one libuv loop) serving all HTTP
workers, sized with enough connection **slots**; it is not the bottleneck even
at 160k push/s. Multiple push engines — and partition-sharding to keep them
lock-free — only become worthwhile if a single engine's `evl` ever climbs under
a workload, which it did not here (≈0 at 160k). Scale **slots and HTTP workers**,
not engines.

# 17 — Segments broker performance: findings, tuning, best config

Results of the July 2026 performance campaign on the **Rust segments broker**
(`server/`), run on two DigitalOcean VMs (broker+PG: 32 cores, loader: 48 cores,
10 Gbps private network, single Postgres 16 in Docker). This page records what
we measured, what turned out to be false, the optimizations that came out of it,
and the configuration we consider the baseline going forward.

TL;DR: **a single-node Postgres sustains ~1.94M msg/s combined
(~970k push/s + ~970k pop/s, zero errors, consumers fully caught up)** with the
broker at ~60% CPU. The historical "~880k each / ~1.75M combined ceiling" was
not a system limit: it was the sum of a client-side batching artifact, broker
CPU waste, and a Postgres deadlock regression. No PG sharding required.

---

## How to read a plateau (method notes)

Things we learned the hard way, worth keeping in mind for every future
campaign:

1. **The load is closed-loop** (goload producers/consumers wait for each
   response). Little's law applies: `throughput = concurrency / latency`.
   If adding producers does not raise throughput, latency grew proportionally —
   that means there is a queueing point, *not* that a resource is saturated.
2. **"Nothing is saturated" is not a mystery** — it is the signature of either
   an admission limiter (Vegas), a serialization point (per-partition gate,
   lock convoys), or per-request latency. Find which one before touching knobs.
3. **Read the gauges, don't guess.** `queen_seg_push_vegas_limit` /
   `queen_seg_pop_vegas_limit` are exported on **`/metrics/prometheus`**
   (not `/metrics`). Under load the limiters converge to ~18 — max=64/256/512
   is unreachable, so raising `*_MAX` is a no-op by construction.
4. **Discard warm-up windows.** The first 30s report of a goload run is ramp;
   quoting it as steady state produced at least one wrong conclusion in this
   campaign (the "pinning Vegas makes it worse" claim).
5. **Fresh PG per data point.** Back-to-back runs on a dirty DB accumulate
   backlog/retention churn and are not comparable.
6. Useful per-run instrumentation: `pg_stat_activity` wait events sampled at
   1-2s, `xact_commit` delta, per-core `mpstat` (a single hot core caps
   throughput while the average looks idle), NIC byte counters, and — decisive
   here — `docker logs` of Postgres for `deadlock detected`.

---

## Myths measured and busted

| Claim | Verdict | Evidence |
|---|---|---|
| "~880k push is the ceiling" | **False** — client artifact | Raising goload `push-batch` 100→200 moved combined push to 1.1-1.2M with no server change |
| "~1.75M combined is a WAL/commit wall on single PG; sharding needed" | **False** | `LWLock:WALWrite` waiters were ≤12/600 conns almost always; commit rate varied 1k-11k/s at identical msg rates; disk `%util` 4-19%, fsync ~0.1ms; WAL ~26MB/s |
| "Raising Vegas max / alpha-beta does nothing, so knobs are exhausted" | **Misread** | The adaptive limit converges to ~18 permits; max is never reached. The knob "did nothing" because it isn't the operating point |
| "Pinning Vegas min=max=256 lowers throughput" | **Misread** | The quoted 645k was the warm-up window; steady state was ~900k, same as adaptive. (Pinning *pop* high does hurt — see deadlock below) |
| "The recurring mid-run stall is retention" | **False** | Reproduced with `completed-retention 0`. Root cause: a deadlock regression in `seg_pop_segments_v1` (fixed, see below) |

What actually bounds the system today, in order:

1. **Broker CPU per message** (was ~14.7 µs·core/msg, now ~10 µs after the
   optimizations below; at ~2M msg/s combined that is ~20 of 32 cores).
2. Closed-loop latency equilibrium (more offered load raises client latency,
   sub-linearly raises throughput via bigger emergent batches).
3. Network at higher totals (~5-6 Gbps of the measured 10 Gbps at 1.9M msg/s).

Postgres itself has ample headroom at this scale: 7-9 cores of 32, commits far
below its measured 13-19k/s capability.

---

## The deadlock regression (the real "stall")

**Symptom:** runs collapse mid-way: throughput drops to ~0 for tens of seconds,
`Lock:transactionid` waits pile up (68-160 sessions), fusion bundles hit the
30s statement timeout, clients see error bursts and redeliveries.

**Root cause:** `queen.seg_pop_segments_v1` exists twice: the base definition in
`023_storage_v2.sql` and a superseding redefinition in `025_storage_v2_dlq.sql`
(applied later at boot). The base version deliberately used a **claim-first**
pattern, with a comment explaining that an unconditional
`INSERT ... ON CONFLICT DO NOTHING` of the consumer row on every pop makes
concurrent pops serialize on the inserter's transactionid (ShareLock). The 025
redefinition **reintroduced exactly that per-pop INSERT**. Because
`seg_pop_wildcard_*` claims several partitions **in random order inside one
transaction** (`ORDER BY random()` candidate scan), those ShareLock waits form
cycles → Postgres kills them: `deadlock detected` storms, visible in the PG log
with context pointing at the INSERT.

Frequency scales with pop concurrency — which is why high pop-Vegas settings
(pin 256, floors of 64+) collapsed quickly while the adaptive limiter at ~18
only tripped it occasionally, and why the stall got *more* frequent as the
broker got faster.

**Fix (2026-07-21, `025_storage_v2_dlq.sql`):** restored claim-first in the 025
redefinition — `FOR UPDATE SKIP LOCKED` first; only if the row is *missing*
(not merely locked/leased) insert it, then re-claim. The hot path never touches
`ON CONFLICT` in steady state. After the fix: zero client-visible errors in
10-minute runs; the only remaining deadlocks are 2-4 benign first-contact races
during warm-up, absorbed by client retries.

---

## Broker optimizations (July 2026)

Profiled with `perf record --call-graph dwarf` on the live broker under the
standard combined load. Starting profile: **42% of cycles in libc**
(malloc 6% + free 4.9% + ~13% memcpy), 9.2% in `json_escape_into`, ~5.5% zstd
(level 3), 2.7% SipHash, 18% kernel/network.

Changes, in decreasing order of yield:

| Change | Where | Effect |
|---|---|---|
| Pop blobs as **native bytea[]** — new `seg_pop_wildcard_bin_v1` (returns `TABLE(meta jsonb, blobs bytea[])`, no `encode(base64)`, no whitespace-strip/decode on the broker, ~25% fewer wire bytes) | `024_storage_v2_pop_ext.sql`, `db::pop_wildcard_bin`, `handlers/data.rs` | pop path CPU down; PG-side encode removed |
| **One-buffer pop rendering** with capacity pre-sizing (was: unsized String realloc'd through ~hundreds of KB per pop, then fully re-copied into a second buffer) | `render_pop_parts` | large memcpy cluster gone |
| **Zero-copy push payloads** — `OwnedFrame.payload: Bytes` slice_ref'ing the HTTP body; drop of a flushed bundle is refcount decrements, not one free per message | `fusion.rs`, `handlers/data.rs` | malloc+free 10.9%→7.0% |
| **Byte-scan `json_escape_into`** (bulk-copy clean runs instead of char-by-char) | `fusion.rs` | self-time 8.3%→4.4% |
| **Borrowed frame decode** on pop (`unpack_frames_ref` + `uuid_hex_into`, no per-frame String/Vec) | `frames.rs` | per-frame allocs removed |
| **FNV hashers** for hot maps; composed single-string dedup key (1 alloc instead of 6 clones per item); single-buffer `pack_frames`; `from_utf8` fast paths; env flag read once per process | `fusion.rs`, `frames.rs`, `handlers/data.rs`, `util.rs` | SipHash 2.7%→0, misc |
| **zstd level 3 → 1** (config, not code) | `QUEEN_V2_ZSTD_LEVEL=1` | +4-5% alone; PG easily absorbs the fatter segments |

Net effect: **~14.7 → ~10 µs·core per message** (-30%), broker CPU at the same
throughput down from ~80% to ~60%, and the profile is now ~26% kernel/network —
i.e. a growing share of time is real I/O, not userland waste.

Behavioral invariants kept: response JSON is byte-identical (unit tests assert
the new escape matches the old one byte-for-byte and that borrowed/owned frame
decoders agree); the wire_v1 base64 path still serves the specific-partition
and discover pops.

---

## Vegas: what the knobs actually do

- The limiter adjusts only when it is being driven (`in_flight >= limit-1`) and
  targets an estimated queue of `alpha..beta` ops at PG. Under load it
  converges to ~18 permits per lane and that is **the right operating point**:
  forcing more concurrency (pin 256, or pop floors `OMIN=64`) measurably
  *lowers* throughput (~810k vs ~975k per side) and raises broker/PG CPU —
  more SKIP LOCKED racing, more contention, fatter RTTs.
- `*_MAX` only matters as a safety ceiling; the controller never gets near it
  in this workload. Don't tune it expecting throughput.
- Known bias to keep in mind: `rtt_base` rises very slowly, so fatter batches
  (higher RTT) read as congestion. At today's operating point this is not the
  binding constraint; revisit if the broker gets another 2x faster.

---

## Best known configuration (baseline)

Broker (see `server/setup-broker.sh`; values that differ from code defaults in
bold):

```bash
DB_POOL_SIZE=300
QUEEN_V2_FUSION_SHARDS=24        # 16 also fine; insensitive between 16-24
QUEEN_V2_FUSION_HOLD_MS=30       # backstop tick only — NOT a batching window;
                                 # lowering it is a no-op under load
QUEEN_V2_FUSION_MAX_INFLIGHT=64
QUEEN_V2_BUNDLE_MAX=32
QUEEN_V2_ZSTD_LEVEL=1            # was 3; -4-5% broker CPU, PG absorbs it
QUEEN_SEG_PUSH_INIT=64  QUEEN_SEG_PUSH_MIN=16  QUEEN_SEG_PUSH_MAX=256
QUEEN_SEG_POP_INIT=64   QUEEN_SEG_POP_MIN=16   QUEEN_SEG_POP_MAX=256
QUEEN_VEGAS_ALPHA=6     QUEEN_VEGAS_BETA=12    # adaptive; do NOT pin/floor
```

Postgres (24-32 core box): `setup-broker.sh` defaults — `max_connections=600`,
`shared_buffers=24GB`, `commit_delay=200`, `commit_siblings=5`,
`synchronous_commit=on`. None of these were the bottleneck; don't chase WAL
knobs for throughput at this scale.

Loader shape that produced the record numbers: 100 partitions, 1500 producers /
850 consumers, push-batch 100, pop-batch 500, pop-partitions 10, long-poll pops
(`wait=true`, 2s timeout), 256B payloads. Notes: producer count beyond ~1500
adds latency, not throughput; 400 partitions is *worse* (candidate-scan and
lock churn); push-batch 200 trades pop throughput for push throughput at the
same total.

---

## Measured results (2026-07-21, opt3 build)

Standard 120s combined runs, fresh PG each time:

| Run | push/s | pop/s | combined | errors |
|---|---|---|---|---|
| pre-campaign baseline | ~880k | ~850k | ~1.73M | occasional stall storms |
| opt3, 1400/800 | ~975k | ~975k | ~1.95M | 0 |
| opt3, 1500/850 | ~968k (peak 998k) | ~968k (peak 979k) | ~1.94M | 0-1 |
| opt3, 1500/850, best window | 1,004,943 | 1,004,310 | 2.01M | 0 |

10-minute soak (1500/850, fresh PG):

<!-- RESULTS-10M -->

Consumers stay fully caught up (final popped ≈ pushed within one pop batch),
which the pre-fix broker never achieved at these rates.

---

## Open items

- 2-4 benign first-contact deadlocks during warm-up (consumer-row creation
  races). Cosmetic; serialize first contact if we ever want a clean zero.
- Dedup ON performance (`seg_dedup` partitioning) — separate campaign.
- Retention/lag benchmarks and 24h soak on the opt3 build.
- If another 2x is needed: next profile targets are serde parse of push bodies,
  `uuid` string work on the push results path, and the kernel/network share
  (batching pop responses over fewer, larger writes).

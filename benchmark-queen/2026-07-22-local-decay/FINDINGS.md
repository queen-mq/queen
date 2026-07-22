# Queen segments broker — saturation blockers, dedup & retention (investigation, 2026-07-22)

Scope: identify what prevents the Rust segments broker from saturating Postgres
(peak and sustained), and understand dedup + retention well enough to hold peak
for hours. Method: 8-reader code map + adversarial synthesis over
`rustserverandstorage`, then instrumented local replication (Mac, PG16 in
Docker on :5457, VM-matched tuning). VM verification is the next phase.

## Part 1 — Why PG is not saturated at peak

**The system is concurrency-limited, not resource-limited.** `runAcd.csv` (the
only machine-readable artifact of the VM campaign) shows, at ~1.78M msg/s
combined: broker ≈ 8.2 cores (≈4.6 µs·core/msg — NOT the ~10 in the doc), PG ≈
5.6 cores. Broker + PG ≈ 14 of 32 cores. Nothing is saturated; the ceiling is:

    throughput = admitted concurrency (~18+~18 Vegas permits) / per-op RTT

The levers, ranked:

1. **Per-op RTT is doubled by protocol waste.** No SQL statement is ever
   prepared-cached (`db.rs`, `fusion.rs` pass `&str` → tokio-postgres mints a
   new named statement per call: Parse+Describe RTT, then Bind/Execute, then
   Close). Every hot op pays 2 round trips + a PG parse. Halving RTT ≈ doubling
   throughput at fixed permits (Little's law).
2. **The push SQL still runs `seg_push_segments_multi_v1`** ($1::text::jsonb —
   PG parses every message's metadata even with dedup off; 3 unconditional
   ON CONFLICT provisioning probes per call; a savepoint subtransaction per
   segment; nested per-segment partition re-resolve). The reviewed lean
   `_multi_v2` (typed arrays, no jsonb on the hot path, no savepoints when
   dedup off) exists in 032 SQL but had NO Rust caller. I wired it behind
   `QUEEN_V2_PUSH_MULTI=2` (default unchanged = v1) for A/B. Local result: see
   Part 4.
3. **Vegas `rtt_base` bias** (`vegas.rs:69-74`): base falls instantly to the
   cheapest op ever seen (an empty pop resets it) and rises 0.05%/sample, and
   one lane mixes fat and thin ops — fat batches permanently read as
   congestion, pinning the limit at ~18 even when PG has 20 idle cores.
4. **Per-partition single-flight** (fusion): per-partition throughput =
   emergent segment size / bundle RTT. Fine at 100 partitions; the RTT
   reduction of (1)+(2) multiplies it.
5. **Parked long-poll pops hold a checked-out pooled PG connection for the
   whole park** (`data.rs`: client acquired before the wait, dropped at loop
   end). Caught-up consumers pin up to #consumers connections. Non-issue in
   goload runs (pool 300 > 850? no — it IS a live risk at 850 consumers), and
   a gate for the 2000-queue multitenant benchmark.
6. **TCP_NODELAY never enabled** (bare `axum::serve`) — Nagle on all loader
   connections; closed-loop latency tax on small responses.
7. Broker per-message CPU (serde full-body parse, ~7-8 small allocs/msg,
   triple uuid stringification) — real but NOT the binding constraint at
   today's operating point (4.6 µs·core/msg leaves the box half idle).

Implication for "saturate PG of any size": fix (1)(2)(3) first — they raise
throughput per permit; then admitted concurrency can actually reach PG. Broker
scale-out (N broker replicas per PG) is the endgame the architecture already
half-supports (advisory-lock leader gating, cross-broker ack-by-txn), but
single-broker efficiency is not the wall yet.

## Part 2 — The soak decline (884k→510k on the VM)

**The VM soak never deleted anything.** `setup-broker.sh` does not set
`RETENTION_INTERVAL`; the default is 300000 ms (5 min). With
completedRetentionSeconds=300 and a 600 s run, the sweep fired ≈once with an
empty cutoff. Arithmetic closes exactly: 431.5M msgs × ~28 B stored ≈ 12 GB =
the reported end size; 431.5M / 229k live segments ≈ 1884 frames/segment. The
"decline is a data-volume / retention-sweep effect" line in doc 17 is half
right: it was pure **accumulation** — retention was idle.

Local A/B (Mac, PG16 VM-tuned config, 100 partitions, 16 prod/12 cons):

- **E1b — retention every 5 s**: decay 74k→34k/side during the first ~6 min
  (pure accumulation window), then **recovery to ~65-70k/side once deletes
  engage, stable for 24 more minutes** (oscillating ±20%). Steady state exists.
- **E1c — retention every 5 min (the VM soak's effective config)**: settles
  lower (~57k avg vs E1b's ~67k, −15%) with a sawtooth: dips to ~50k aligned
  with each 5-minute sweep, and client-visible empty-pop bursts during stalls
  (0→88 over the run; zero in E1b). The 1 s wait samples nail the mechanism:
  each productive sweep ran **8-9 s** as one transaction, and during those
  windows pushes queue on **Lock:transactionid** — blocked behind the sweep's
  `seg_partitions` row locks (the seq-allocator lock), held to sweep commit.
  At VM rates (10× volume per sweep) this extrapolates to minute-class stalls
  that would trip the 30 s statement timeout — a second, independent stall
  mechanism besides the fixed pop deadlock.
- Untuned-PG control (E1): max_wal_size=1GB default → continuous checkpoints →
  monotonic collapse 127k→34k. Reminder that PG defaults must never be used
  for these tests (and a note for self-hosted users' docs).

Mechanism attribution so far (local): WAL/commit constant ~1 kB through decline
and recovery; wait profile identical (WALWrite-dominated locally — Docker fsync);
active backends constant. The local decline+recovery tracks relation FILE
GROWTH (Docker sparse-file allocation slows fsync) — an environment artifact.
The VM's `LWLock:BufferContent` + `Lock:extend` signature at 24 GB
shared_buffers with no checkpoint is a different expression of the same root
cause (unbounded accumulation) and needs the VM phase to characterize exactly.

**Churn finding (verified live, I/O-independent):** every segment push UPDATE
on `seg_partitions` is **non-HOT** — `last_write_at` is covered by
`idx_seg_partitions_queue_write` (023:71-76), defeating the fillfactor-70 HOT
design documented ten lines above it (023:62-64, written before the index was
added). E1b final: 1.20M updates, 29.6k HOT (2.5%), 23.6k dead tuples, 8.6 MB
for a 100-row table — and this is the table every wildcard pop candidate-scans.
`partition_consumers` (fillfactor 50, unindexed hot columns): 96% HOT. Fix
options: quantize `last_write_at` updates (only when >1 s stale), or index a
coarser derived value. Interacts with the 10-20k-partition candidate-scan
design — Alice's call.

Additional volume-coupled costs found in code (all grow the sustained-load
floor): `seg_refresh_all_stats_v1` full-scans `seg_segments` every 10 s;
autovacuum insert-triggered passes re-scan the growing PK btree + TOAST index
each ~5% growth; TOAST of `seg_segments` inherits NO autovacuum tuning (the
per-table overrides in 023 do not apply to the toast relation).

## Part 3 — Retention: design + why it can't hold peak for hours (as-is)

Current design: one tokio loop, every RETENTION_INTERVAL ms, ONE transaction
(advisory xact-lock 737001) runs `seg_retention_sweep_v1()`:
flat loop over ALL partitions in ascending id — per partition: boundary walk
(index-forward from `retention_seq`, correct and cheap), unbatched contiguous
`DELETE`, then `UPDATE seg_partitions.retention_seq` whose **row lock (the same
lock the push seq-allocator needs) is held until the WHOLE sweep commits** —
including the dedup purge loop and the metrics purge that run in the same
transaction. `RETENTION_BATCH_SIZE` bounds only the metrics purge, not segment
deletes.

Failure modes:
- **Mode A (measured on the VM soak):** interval too long → zero deletion →
  unbounded accumulation → decline. Trivial fix: run it every 2-5 s.
- **Mode B (structural):** when a sweep does have work, it stalls pushes on
  every already-swept partition for the remainder of the sweep transaction,
  and its delete volume (rows + ~26 TOAST chunks per 52 kB blob + index
  entries) lands as one WAL burst + one vacuum-debt avalanche.

Redesign options (need discussion before implementation):
- **R1 — mechanical, no semantics change:** per-partition (or per-K) commits;
  LIMIT-batch deletes; move dedup purge + metrics purge to separate
  transactions; keep cadence at 2-5 s. Bounds lock hold time and WAL bursts.
  Keeps the row-DELETE + vacuum-debt model.
- **R2 — architectural:** time-bucketed partitioning of `seg_segments` with
  DROP-based expiry: O(1) retention, zero dead tuples, zero vacuum debt, TOAST
  per bucket (also multiplies the extension-lock surface). Costs: PK must
  include the bucket key; pop range scans must route across bucket boundaries;
  per-queue windows vs global buckets tension. This is the "keep peak for
  hours at 1M msg/s" endgame if R1 + vacuum tuning proves insufficient.
- **R3 — micro:** restore HOT on `seg_partitions` (quantize `last_write_at`);
  `toast.autovacuum_*` overrides on `seg_segments`; consider
  `toast_tuple_target` shaping.

## Part 4 — Dedup: design, cost, and what to do

Design (3 layers): L1 broker intra-request first-wins; L2 broker intra-flush
first-wins; L3 PG `seg_dedup` (PK (partition_id, hashtextextended(txn))), one
row per message inserted INSIDE the push transaction when the queue's window>0.
**Default ON (3600 s) for every auto-provisioned queue; every benchmark so far
ran with it OFF** (goload configures dedupWindowSeconds:0).

Costs when ON (from code + storage-v2-spike measurements): +1 btree descent &
insert per message at a RANDOM key point (hash) — every leaf hot, ~3.3× WAL
per message (392 B vs 119 B measured in the spike); a savepoint subtransaction
per dedup-enabled segment (XID burn, subtrans SLRU pressure); steady-state size
O(rate × window): 100k msg/s → 360M rows (~30-50 GB); 1M msg/s → 3.6B rows —
**untenable as designed**. Local measurement (E-dedup, window=300): throughput
−50% and unstable, seg_dedup 1.8 GB at 30k msg/s×300 s, WAL/commit ×9.3. Purge: `DELETE ... ctid IN (SELECT ... LIMIT 50k)`
loops with NO index on created_at (each batch re-scans) inside the SAME
transaction as the retention sweep; time-ordered deletes against a hash-ordered
index bloat every leaf.

Correctness landmine (independent of performance): **explicit (non-autoAck)
acks resolve positions through `seg_dedup`** (`seg_ack_by_txn_v1`). With
window=0, explicit acks silently no-op → cursor never advances → endless
redelivery. Today's benchmarks pass only because they use autoAck. Any
dedup-OFF production queue with explicit acks is broken. E-dedup experiment
also verifies this. [LOCAL RESULT PENDING]

Options (discussion needed):
- **D0 — interim hardening (mechanical):** BRIN on created_at (heap is
  insertion-ordered), purge out-of-transaction in its own batched commits,
  optional hash-subpartitioning to spread the btree. Unblocks a dedup-ON
  campaign; does not fix O(rate×window).
- **D1 — time-bucketed dedup partitions, DROP-based expiry:** O(1) purge; the
  uniqueness arbiter must move entirely onto the partition-serializer row lock
  (probe all live buckets, insert into current) since a cross-partition unique
  index is impossible. The 023 design comment already claims the serializer
  guarantees race-freedom within a partition — needs a careful proof + tests.
- **D2 — decouple ack-resolution from dedup:** store a compact txn→(seq,off)
  map in/next to the segment so explicit acks never touch `seg_dedup`. Fixes
  the window=0 landmine and removes 3 probes per explicit ack. High value,
  moderate contract change.

## Part 5 — Local A/B results (Mac, PG16 tuned, fresh DB each)

| Run | Config | Result |
|---|---|---|
| E1 | PG defaults (max_wal_size=1GB) | 127k→34k/side collapse; continuous req checkpoints; FPI storm. Environment guardrail, not a broker finding. |
| E1b | VM-tuned PG, retention 5 s | 74k→34k accumulation dip, recovery to ~65-70k steady 24 min, WAL/commit flat ~1 kB, checkpoint at 15 min visible (WAL/commit ×1.9) but absorbed. |
| E1c | VM-tuned PG, retention 300 s (VM soak config) | [PENDING] |
| E2a/E2b | push multi v1 vs v2 (flag) | **Not adjudicable on this rig**: local throughput is WAL-fsync-bound (Docker-on-Mac), so PG-CPU savings can't move it, and docker-stats CPU accounting was noisier than the effect (results inverted between runs; run-order/thermal confounds). The `QUEEN_V2_PUSH_MULTI=2` flag build is ready; the A/B belongs on the VM, interleaved (v1,v2,v1,v2), scored by PG CPU/msg via pg_stat_statements + mpstat. |
| E-dedup | window=300 vs 0 | Throughput halves and destabilizes: ~30k avg (13k-51k swings, 891 empty-pop starvation events) vs ~62k dedup-off baseline. `seg_dedup` = 1.8 GB for a 300 s window at 30k msg/s (~200 B/row incl. ~2× bloat) → ~700 GB at VM rate with the default 3600 s window. Purge deletes up to 174k rows per 5 s sweep inside the lock-holding sweep transaction. **WAL amplification ×9.3 per commit** (9,985 B vs 1,070 B at equal msgs/commit) — purge + random-leaf FPIs compound the spike's 3.3×/msg estimate. |

## Part 6 — What I did NOT change

Only measurement scaffolding + one flag-gated call-site (`QUEEN_V2_PUSH_MULTI`,
default = current behavior). Everything in Parts 1-4 labeled "options" awaits
discussion. Doc 17's µs·core/msg and "broker ~60% CPU" numbers conflict with
runAcd.csv (4.6 vs 10 µs·core/msg; 8 vs 19 cores) — worth re-measuring on the
VM with a consistent method before publishing any of them.

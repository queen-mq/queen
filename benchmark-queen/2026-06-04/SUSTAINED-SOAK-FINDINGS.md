# Sustained-throughput soak — findings (2026-06-04)

Purpose of this doc: record what we learned trying to sustain a high `bp-100`-style
load for hours, so we can design a **more efficient delete/retention path**. The
short version: the broker can *push* very fast, but **retention (row-by-row
`DELETE`) is the ceiling for sustained, bounded-storage throughput.**

Host: 32 vCPU / 62 GiB / x86_64, Postgres 18 (April tuning), Queen
`0.16.0.beta.1` (simdjson broker, full/dashboard image). Disk: single virtual
volume (`vda`). Scripts to reproduce: `run-sustained.sh`, `investigate-soak.sh`,
`read-source-probe.sh` in this folder.

---

## TL;DR

- **Peak push:** 130k–185k msg/s (calibration), broker ~1 vCPU — broker is *not*
the bottleneck.
- **Sustainable with a flat table:** **≈ 40k msg/s**, set by the **single-connection
retention `DELETE` throughput (~30–40k rows/s)**. Above ~40k the `messages`
table grows without bound and the system slowly degrades.
  - Lines up with the April 6h soak: **29k/s held flat** (table plateaued at
  35 GB) — 29k is under the ~40k delete ceiling.
- **Two independent limiters, now separated:**
  1. **Checkpoint / WAL write-amplification — TUNABLE** (fixed live-ish with
    `max_wal_size`).
  2. **Retention `DELETE` throughput — ARCHITECTURAL** (the real target for
    optimization).

---

## What we measured

### Limiter 1 — checkpoint/WAL write amplification (fixable with a knob)

At ~120k/s the default `max_wal_size=16GB` caused **WAL-triggered checkpoints
every ~60 s**, each flushing ~54% of the buffer pool. That forces a **full-page
image** into WAL on the first touch of every page after each checkpoint → huge
WAL volume → disk-write saturation → throughput collapse.

Raising `max_wal_size` 16 GB → **96 GB** (must be set on the PG command line / at
start — `ALTER SYSTEM` is overridden by the `-c` flag the runner passes):


| metric           | `max_wal_size=16GB`       | `max_wal_size=96GB`     |
| ---------------- | ------------------------- | ----------------------- |
| checkpoints      | every ~60 s               | ~0 (every ~18 min)      |
| WAL per insert   | **2,050 B** (5.6 records) | **706 B** (5.7 records) |
| WAL rate         | 166–198 MB/s              | **~86 MB/s**            |
| disk writes      | 539 MB/s @ 55% util       | **225 MB/s @ 47% util** |
| push (sustained) | spiraled 140k → 73k       | **held ~122–130k**      |
| lock-timeouts    | 1–3 / min                 | **0**                   |


Takeaway: **for any sustained high-write workload, `max_wal_size` must be large**
(default 16 GB is far too small). FPI elimination alone cut WAL ~3× per message.

### Limiter 2 — retention `DELETE` is the real ceiling (architectural)

Even after fixing WAL (disk only ~47% utilized, `messages` cache-hit **99.98%**),
retention still could not keep up:


| metric (at `max_wal_size=96GB`) | value              |
| ------------------------------- | ------------------ |
| INSERTs/s (push)                | **122,157**        |
| DELETEs/s (retention)           | **30,367**         |
| → retention deficit             | **−91,790 rows/s** |


So the table grows ~1.3–3 GB/min and never plateaus. The delete path is **not
I/O-bound** here — it is bound by the **single-connection, per-partition,
sequential `DELETE … WHERE id IN (SELECT … LIMIT batch)` loop** in
`RetentionService::cleanup_completed_messages()`. More frequent runs / bigger
batches / fewer partitions helped a little (1000→100 partitions cut growth
3→1.4 GB/min) but did not change the ~30–40k/s ceiling.

### The rising disk *reads* after ~10–15 min — symptom, not cause

disk I/O: write-vda steady ~288 MB/s, read-vda rising to ~50 MB/s after ~10-15 min

Observed: write-`vda` steady ~288 MB/s; read-`vda` was ~0 then climbs to ~50 MB/s
around the 10–15 min mark. Probed with `read-source-probe.sh`:

- `messages` table is **44–54 GB** > `shared_buffers` (24 GB).
- The readers (`pg_stat_activity`) are `**autovacuum: VACUUM queen.messages`** and
the retention `**DELETE … WHERE id IN (SELECT …)**` — *not* the insert/pop path.
- `messages` cache-hit still **99.98%**, heap disk-reads **0 MB/s**, index
disk-reads ~7.6 MB/s.

**Conclusion:** the read rise is **not** the hot working set exceeding
`shared_buffers` (the recent end stays 99.98% cached). It is the **retention
backlog**: because delete < ingest, the table+indexes grow past 24 GB, and then
**autovacuum + the retention `DELETE` must read the cold, old end of the 29 GB of
indexes from disk.** The "~10–15 min" timing is simply when the ever-growing
table crosses the 24 GB `shared_buffers` line (at ~1.5–3 GB/min). If retention
kept the table small, those reads would stay ~0.

### The hard collapse (table > RAM)

Left running, it crossed from "slow reads" to a **throughput collapse** once the
table exceeded total RAM (62 GiB), not just `shared_buffers`:


| +min       | table | live tup | cache-hit  | msgs disk reads                  | push/s            |
| ---------- | ----- | -------- | ---------- | -------------------------------- | ----------------- |
| ~21        | 44 GB | —        | 99.98%     | ~8 MB/s (idx)                    | ~120k             |
| ~45        | 88 GB | 201M     | **99.10%** | **232 MB/s** (heap 47 + idx 185) | **~65k, falling** |
| stop (~51) | 96 GB | —        | —          | —                                | ~68k              |


At ~88 GB the read I/O (349 MB/s observed on `vda`) **exceeded writes (313 MB/s)**
and throughput fell off a cliff. `pg_stat_activity` named the cause:
`push_messages_v3` was waiting on `**IO/DataFileRead*`* — i.e. **inserts
themselves were faulting cold index pages from disk**, dominated by the
`messages_partition_transaction_unique` (dedup) probe on every insert. So once the
**dedup unique index goes cold**, the *write* path stalls → reads exceed writes →
push collapses → retention falls further behind → **death spiral**.

**Crucial coupling for the design:** the index that kills inserts is the
`**transaction_id` dedup unique** — the *same* index that blocks native
partitioning (PG requires the partition key in every unique). So **dedup is both a
top collapse contributor and the partitioning blocker.** Keeping the table bounded
(that index hot) is what prevents the collapse.

---

## Why the delete is expensive (root inputs for optimization)

- **3 indexes on `queen.messages`** dominate the table and every write:
  - `messages_partition_transaction_unique` ≈ **12 GB**
  - `idx_messages_partition_created` ≈ **12 GB**
  - `messages_pkey` ≈ **5 GB**
  - heap ≈ 21 GB → **indexes are ~58% of the table**.
- Each tiny message is written **twice** over its life (INSERT then retention
DELETE), each touching the heap + all 3 indexes → ~5.7 WAL records/insert.
- Retention is **single-connection** and walks partitions **sequentially**,
deleting `id <= MIN(last_consumed_id)` AND `created_at < cutoff` in `LIMIT`
batches; the `id IN (SELECT…)` subquery scans the cold old end of the indexes.
- Then **autovacuum** must reclaim the resulting dead tuples by scanning a table
that has grown past cache → more cold reads.

This is a classic **insert-new / delete-old** churn on one big indexed table:
write amplification on both ends + vacuum, all funneled through one delete
connection.

---

## How pgmq handles this (and why Queen can do better)

pgmq made the **opposite trade-off**: consume = `pgmq.read()` (an **UPDATE** that
sets the visibility timeout + `read_ct`) + `pgmq.delete(msg_id)` (a per-message
**DELETE**), both on the **consumer connection**. So:

- pgmq has **no central retention sweep** — deletes are distributed across the
whole consumer fleet → it gets "parallel delete" **for free**, no
single-connection bottleneck.
- But it pays **UPDATE+DELETE churn on every message** → heavy dead-tuple/vacuum
pressure. Our own `../pgmq/QUEEN-vs-PGMQ.md` measured pgmq doing ~**9.6M
UPDATE+DELETE** ops with large dead-tuple counts, while **Queen had 0 hot-table
UPDATE+DELETE** (cursor model).
- For scale, pgmq offers `**create_partitioned()` + pg_partman** — exactly the
time-partition + drop approach (our 0.17 direction).

**Why Queen's position is actually better:** Queen's cursor pop (no per-message
delete, no `vt` UPDATE) is *why* it has zero consume churn — and *why* deletion is
centralized into one retention worker (the bottleneck we hit).

- Parallelizing Queen's retention = pgmq's delete parallelism **without pgmq's
per-read UPDATE churn** → strictly less write amplification.
- Time-partitioning Queen = pgmq's scale story, but Queen can make **partition
drop the *sole* reclamation** (no per-row delete at all), which pgmq can't
because its consume *is* a delete.
- The one thing pgmq sidesteps: it has **no send-side idempotency** (`msg_id` is a
bigserial), so it never faces the unique-key-under-partitioning problem. Queen's
`transaction_id` dedup is the feature that makes partitioning non-trivial.

---

## Solution options — two categories

Both candidate fixes target the *same* goal: **keep `messages` (heap + its 2
indexes) small enough to stay in RAM**, so neither the dedup probe nor
autovacuum/retention ever goes to disk.

### A. Parallel delete — make the O(rows) work go faster  → **0.16**

Run the retention `DELETE` on K connections instead of 1. Lifts the ~40k/s ceiling
~linearly (to ~100k+ before I/O/contention). **Catch:** it doesn't reduce WAL or
dead-tuple volume — it just deletes faster, so **autovacuum becomes the next
ceiling** (must reclaim fast enough that the heap plateaus via slot reuse).
Low-risk, no schema rewrite.

### B. Time-partition `messages` + drop old partitions — make the work go away  → **0.17+**

Range-partition the heap by time; retention = `DETACH CONCURRENTLY` + `DROP` →
**O(1), no per-row WAL, no index maintenance, no vacuum**. Removes the bottleneck
class. **Costs:** (1) PG requires the partition key in every UNIQUE → the
`(partition_id, transaction_id)` dedup can no longer be global (decide: short
dedup window vs separate rotating dedup table vs broker-side dedup); (2) retention
becomes all-or-nothing per partition → a **stuck consumer group pins a partition**
from being dropped (need consumer-lag bounds); (3) pop SP becomes a merge-append
across live partitions; (4) migration.

---

## 0.16 plan — parallel retention (compatible with old server/database)

**Core change (broker-side only):** make `RetentionService` run
`RETENTION_PARALLELISM` worker connections (default **1** = today's behavior).
Each worker **claims a partition with `pg_try_advisory_lock(hashtext(partition_id))`**
before deleting it and releases after — so no two workers (or replicas) ever touch
the same partition, regardless of leader-gating, and it reuses the existing
per-partition delete SQL (works against any existing DB). Ship as a new procedure
**version** (`cleanup_completed_messages_v2`), keep the old one.

**Pair with (additive, compatible):**

1. Per-table autovacuum so vacuum keeps pace and the heap plateaus:
  `ALTER TABLE queen.messages SET (autovacuum_vacuum_scale_factor=0.02,  autovacuum_vacuum_insert_scale_factor=0.05, autovacuum_vacuum_cost_limit=4000,  autovacuum_vacuum_cost_delay=0);` (idempotent; safe on existing tables).
2. `max_wal_size` guidance (16 → 96 GB tripled sustainable throughput by killing
  FPI churn). Deployment doc + optional broker startup warning if it's small.

**Transaction / dedup in 0.16: no change needed.** The dedup unique stays hot
*because* parallel retention keeps the table bounded; the collapse was a symptom
of the unbounded table. `messages` is already pruned to the minimal 2 indexes, and
removing the dedup unique would break idempotent push — so leave it. A
bounded/windowed dedup is only *forced* by partitioning, so it belongs with 0.17.

**Compatibility checklist:**

- Parallel retention = broker logic; `RETENTION_PARALLELISM` defaults to 1 → old
behavior; mixed-version brokers safe.
- Autovacuum settings = additive idempotent `ALTER`.
- New SQL ships as a *new version*; old procedures retained.
- No dropped constraints, no table rewrite, no repartition; existing data valid.

**Success metric (re-soak):** not msg/s but **live + dead working set < RAM** —
the `messages` table **plateaus** (delete-rate ≈ ingest, dead tuples bounded) and
disk **reads stay flat**. Watch vacuum as the next ceiling.

### Result — implemented & validated (2026-06-04)

Implemented broker-side (`RETENTION_PARALLELISM`, default 1 = legacy):
`RetentionService` lists eligible partitions then deletes across K connections via
a work-stealing runner (`run_partitions_parallel`); per-table autovacuum shipped in
`schema.sql`. Re-soaked at ~130k with `RETENTION_PARALLELISM=8` + `max_wal_size=96GB`:

| metric | parallelism=1 (before) | **parallelism=8 (after)** |
|---|--:|--:|
| retention DELETEs/s | ~30–40k (behind ~92k/s) | **~133k (≥ ingest)** |
| `messages` table | unbounded → 96 GB → collapse | **flat ~18–19 GB** |
| cache-hit @ +20 min | 99.10% (232 MB/s cold reads) | **100.00% (~0 reads)** |
| push throughput | 140k → collapsed to ~65k | **steady ~130k** |

The table plateaus at the retention-window size and the death spiral never starts.
Remaining sub-item (separate from storage): `pending` creeps ~50k/min (pop ~1k/s
under push) — a consumer/producer balance detail. Evidence:
`bp-100-sustained-PARALLEL8-metrics.csv` (vs `bp-100-sustained-metrics.csv`).

---

## 0.17+ direction

Time-partitioned `messages` + drop-based retention (option B), which forces the
dedup redesign (windowed / side-table dedup) and consumer-lag bounding. End state:
Queen reads via cursor (no per-msg mutation) **and** reclaims via partition drop
(no per-row delete) — the lowest-churn queue-on-Postgres design of the three
systems compared.

---

## Config gotchas discovered (so we don't trip on them again)

- `**POST /api/v1/configure` is a full upsert** (`ON CONFLICT DO UPDATE SET … = EXCLUDED.`* for every field). A partial config call **resets all other fields
to defaults** — send the complete option set every time.
- `**completed_retention_seconds` got coerced to 1800** even when 300 was sent
(route passes it through, so it's applied at push-time queue auto-creation /
a default). We had to force it via direct SQL: `UPDATE queen.queues SET retention_enabled=true, completed_retention_seconds=300, retention_seconds=7200 WHERE name=…`. Worth fixing so configure honors the requested value.
- `**retention_enabled=true` is mandatory** — the cleanup SQL filters on it;
setting only `completedRetentionSeconds` does nothing.

---

## Sustained-rate guidance (current code, this hardware)

- Run at **≤ ~40k msg/s** for an indefinitely flat table with the current
row-DELETE retention (29k is comfortably safe, per April).
- If you need higher sustained throughput, it's a **retention redesign**
(partition-drop or parallel delete), not a tuning exercise — the broker push
side already does 130k+ at ~1 vCPU.
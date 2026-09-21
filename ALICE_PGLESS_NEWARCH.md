# ALICE_PGLESS_NEWARCH.md — the log-native architecture

**Status:** proposed (2026-09-21). Single-node (`LocalReplicator`) scope first;
multi-node deferred (§8). Supersedes STORAGE_V2 "lever 2" (the double-write) and
builds on the committed STORAGE_V2 "lever 1" (`6a141edc`, dedup authority off
LMDB). Nothing here changes a ratified PLAN_RAFT decision without Alice; where it
does, it is flagged.

This is the plan for the architecture Alice designed in the 2026-09-21 review:
**one serial leader, per-queue append-only logs as the byte store, cross-queue
transactions committed by presence-in-all-participants, and LMDB kept ONLY for
the low-rate catalog.** It removes the two things the measurements pinned as the
write-amplification: the payload double-write and the churny LMDB metadata.

---

## 0. Why (measured this session, VM 164.90.215.224, ext4)

At HEAD the raft class's disk write amplification is **A20k 4.71x, C1000 10.07x,
FAT100 2.18x** (device gross ÷ net store growth, consume-matched). Two causes,
both verified:

1. **The payload double-write.** Every message is written twice: to the raft log
   (`Append.blob`, fsync'd at commit, then truncated) AND re-materialized into a
   `.seg` file. ~7 MB/s at A20k; **~65 MB/s at FAT** (there it is ~half the gross).
2. **LMDB copy-on-write churn.** Each `mdb_txn_commit` (~250/s at
   `store_commit_ms=4`) rewrites the meta page + every touched keyspace's
   root/inner path. The churn is dominated by the **update-in-place / random-key**
   keyspaces — `cursors` (per pop), `counters` (per commit), `request_ids`
   (per command, random key), `pending` — **not** the messages. Proven: STORAGE_V2
   lever 1 moved the dedup index off LMDB and **barely moved gross**, because the
   message metadata (`txns`, `seg_loc`) is append-mostly (low churn). The
   `store_commit_ms` sweep isolated ~14 MB/s of pure commit-cadence churn.

**Consequence that shaped this plan.** Moving *messages* out (lever 2 alone) kills
the double-write — a big win for FAT, modest for A20k/C1000 — but leaves the
churn, because the churn is *metadata*. To reach ~1x on the small-message shapes
you must also move the churny metadata off the CoW B-tree. Hence: do both, in one
architecture.

**Goal.** Write amplification toward **~1x**, single-node throughput toward
**Kafka-broker class**, while keeping the two differentiators the single log
buys: **cross-partition transactions** and **cheap dynamic partitions**.

---

## 1. The architecture

Three ideas, in dependency order:

1. **One serial leader is the sequencer.** It assigns per-partition offsets,
   makes per-partition dedup verdicts, and decides transaction commits — all
   **in one order**, all **cheap** (a counter bump, a hash probe; no fsync). This
   is the only global serialization point, and it is what keeps transactions
   trivial (§4) and read-committed isolation free (§4.4).

2. **Per-queue append-only logs are the byte store.** A queue's messages live
   **once**, appended in the leader's order, in that queue's own log files
   (`q<id>/rNNNN.qlog`). Homogeneous retention per queue (§3.3). There is **no
   separate consensus log holding payloads** — so no double-write. The queue log
   *is* the write-ahead log and the store.

3. **Transactions commit by presence-in-all-participants.** A cross-queue
   transaction writes its full record, tagged `GLOBAL_TRX_ID`, into **every**
   participant queue log; it is committed iff that record is present-and-untorn
   in **all** the logs it names; the leader acks only after fsync'ing them all;
   recovery keeps present-in-all txns and truncates the rest (§4).

Because the leader is serial, an uncommitted transaction is always the **tail**
of each log it touched (nothing else was processed while it was committing), so
recovery is just tail truncation — no separate transaction coordinator log.

---

## 2. What lives where (the storage map)

| state | today (raft class) | new arch | why |
|---|---|---|---|
| message payloads | raft log **and** `.seg` (double-write) | **per-queue log** (once) | kills the double-write |
| dedup hashes / txns | LMDB `Txns` / segment (lever 1) | **per-queue log record** (carries hashes) | lever 1's path, now pointed at the queue log |
| offsets, base/count/created | LMDB `partitions`/`txns` | **per-queue log record header** | append-only, no churn |
| consumer cursors (offsets, lease) | LMDB `Cursors` (per-pop churn) | **compacted offsets log** (Kafka `__consumer_offsets`) | remove from CoW tree |
| request-ids (idempotency, D6) | LMDB `RequestIds` (random-key, per-command churn) | **windowed idempotency log / RAM** | the worst churn source |
| counters (stats) | LMDB `Counters` (per-commit churn) | **RAM, derived** (O(1) from tails+cursors+file sizes) | not durable state |
| pending / ready ring | LMDB `Pending` (per-append churn) | **RAM, derived** (tail − cursor per partition) | rebuildable |
| **catalog** (queues, groups, partitions defs) | LMDB | **LMDB — KEPT** (Alice's call) | low-rate, transactional, tiny; no churn |
| node-local seg location / files | LMDB `SegLoc`/`Files` | **per-queue sparse index (`.qidx`)** | node-local, rebuildable |

**Net effect:** LMDB holds only the catalog — created/deleted rarely, so its CoW
churn is negligible and it keeps its one genuine strength (easy transactional
catalog edits, ordered scans). Everything high-rate is a log or RAM.

Counters/pending are cheap to rebuild on restart because the numbers that matter
are O(1) from log metadata: `pending = partition_tail_offset − cursor_committed`,
`lag` likewise, `retained_bytes = Σ live log file sizes`. No message scan.

---

## 3. The per-queue log

### 3.1 Record format (one per `Append`, extends today's `frame.rs`)
```
len:u32 | xxh3:u64 | seq:u64 | pid:u64 | base_offset:u64 | count:u32 |
created_at:i64 | txn:{none | GLOBAL_TRX_ID:u128, participants:[queue_id]} |
hashes[16*count] | payload
```
- `seq` = the leader's global order stamp (monotone), so a queue log records the
  leader's order for its own entries and recovery can interleave with the offsets
  log deterministically.
- `hashes` = the per-message txn-id hashes (dedup authority — lever 1 read path,
  now sourced here).
- `txn` present only for a **cross-queue** transaction; a single-queue op (even
  multi-partition within one queue) is one atomic append and carries `none`.
- checksum covers everything after it (torn-tail detection, per today's frame).

### 3.2 Sparse index (`.qidx`, per queue log file — reuse today's machinery)
`(pid, base_offset) → (byte_offset, len, end, created, count)`. Active file's
index in RAM; sealed files get an immutable `.qidx`; rebuildable by scanning the
log (§5). Pop locates a partition's run by binary search, then reads the payload
from the log (page cache on the hot path).

### 3.3 Retention & compaction (per queue = homogeneous)
Because a log holds one queue, retention is homogeneous:
- **All-dead file → unlink** (O(1)) — the common case for consume-and-delete
  queues with caught-up consumers.
- **Partially-live file → compact** (copy the live residue forward, drop the old)
  — only for the fragmented residue (lagging backlog, long-retention survivors).
  Hole-punch (`fallocate`) is the cheaper in-place option for block-aligned dead
  runs; compaction handles the sub-block scattered case.
The compaction write cost is proportional to *how much data lives long*, not to
throughput — ~0 for short-lived queue traffic, real for long-retention streams.

---

## 4. Transactions

### 4.1 The commit rule
A transaction is **committed iff its `GLOBAL_TRX_ID` record is present, untorn,
in every queue log it names.** The leader acks the client only after fsync'ing
every participant log. Single-queue transactions (multi-partition within one
queue) are a single atomic append — trivially committed.

### 4.2 Recovery (the coordinator-free part)
Because the leader is serial, an in-flight txn is the **tail** of each log it
touched. Recovery, from each log's durable point forward:
1. scan tails, validate frames (checksum → torn = not present);
2. group records by `GLOBAL_TRX_ID`;
3. a txn is committed ⇔ present-and-untorn in all its named logs → keep it;
   otherwise **truncate** it off the tail of each log that has it (rollback).
No separate coordinator log; the `GLOBAL_TRX_ID` replicated into the participants
*is* the coordination.

### 4.3 Crash cases
- after fsync(A), before fsync(B): A has it, B doesn't → not all present → truncate
  A's tail. Client never acked → retries → dedup swallows the retry. Atomic.
- after all fsyncs, before ack: present in all → committed; client retries →
  dedup idempotent.
- torn write anywhere → checksum fails → not present → rolled back.

### 4.4 Read-committed is free
A pop is another command the serial leader processes **after** a txn has fully
committed (or before it began). No command observes a half-written txn, so there
is **no** last-stable-offset / isolation machinery (the hardest part of Kafka
transactions). This is the payoff of the serial leader and holds only while the
decision path is serial (see the §8 / phase-E caveat).

### 4.5 Aborts & offset gaps
Explicit aborts are decided **before** append (the leader validates, then only
appends a committing txn), so aborts don't leave partial state except on crash
(handled by 4.2). A rolled-back txn leaves assigned offsets as gaps; pops skip
them (same as Kafka aborts).

---

## 5. Durability & crash contract (rewrites PLAN_RAFT I10/I11)

- **A single-queue message is durable ⇔ its record is fsync'd, untorn, in its
  queue log.** Acked after (D7 preserved: answer after durable + applied).
- **A transaction is durable ⇔ present-untorn in all participant logs, all
  fsync'd** (§4.1).
- **Recovery** replays each queue log from its durable point: rebuild the `.qidx`
  and the dedup RAM index by scanning the tail; resolve txns by present-in-all;
  truncate uncommitted tails; replay the compacted **offsets log** to rebuild
  cursors; derive counters/pending in RAM (§2). The **catalog** recovers from its
  LMDB durable commit as today.
- **The log is no longer truncated below a durable point** — it is the store; it
  is reclaimed only by retention/compaction (§3.3).

---

## 6. Throughput

The serial leader's *decisions* are cheap (no fsync). The expensive part —
appending + fsync'ing a queue log — can be **dispatched to a per-queue writer**
and fsync'd **in parallel** across queues. A transaction's commit is a barrier
across its participant writers. This is the lever that breaks the single-fsync
bottleneck behind **C1000** (R-133: "single serial push/pop/ack pipeline"): 1000
partitions across N queue logs fsync in parallel instead of one serial stream.
Cost: fsync fan-out for cross-queue txns (rare; group-committable per log).

Combined with the double-write kill, this is the path to Kafka-broker-class
single-node throughput: FAT stops being byte-bound (payload once), small-message
shapes stop being single-fsync-bound (parallel per-queue durability).

---

## 7. Invariants (new / changed)

- **NA-I1 (durability):** a message is durable ⇔ its record is fsync'd untorn in
  its queue log; a txn ⇔ present-untorn in all participant logs.
- **NA-I2 (serial-tail):** an uncommitted txn is always the tail of each log it
  touched ⇒ recovery = truncate. (Requires the serial decision path.)
- **NA-I3 (read-committed):** no read observes an uncommitted/aborted txn
  (serial leader).
- **NA-I4 (determinism preserved):** the leader decides all effects (offsets,
  dedup verdicts, txn commits); followers/recovery replay them — PLAN_RAFT D3/I2
  intact.
- **NA-I5 (catalog authority):** the catalog is LMDB; a message/queue log never
  contradicts it (a log for a dropped queue is GC'd).

---

## 8. Multi-node (deferred — the genuinely hard part)

Single-node has one leader = one sequencer, so §4 is coordinator-free. Multi-node
raises the two things this plan does NOT solve yet, exactly as PLAN_RAFT phase 3
already defers:
- **Per-queue durability + quorum:** "durable in all logs" becomes "durable in
  all logs on a quorum" → per-queue match indices instead of one raft index.
- **Cross-queue commit across nodes:** the commit gates on a quorum having *all*
  parts → multi-raft-flavored coordination.
If parallel per-queue writers (§6) are introduced even single-node, re-examine
NA-I2/NA-I3 (the serial-tail and free-isolation properties assume a serial
decision path; parallelism needs a commit barrier + possibly an LSO). Keep the
decision path serial until multi-node forces the change.

---

## 9. What carries over (not a rewrite from zero)

- **STORAGE_V2 lever 1** (`6a141edc`): the dedup authority already reads from
  "the segment index + frames" instead of LMDB. Re-point that read at the
  **queue log** and it is done — lever 1 is the foundation, not throwaway.
- **`frame.rs`** (the frame carries hashes inline) and the **`.qidx`** sparse
  index (rebuildable, node-local) transfer directly.
- **The log writer** (`replicator/log.rs`: rolling files, group commit, one fsync
  per group, torn-tail truncation) is the per-queue-log writer, minus the
  truncate-below-durable-point (§5).
- **The crash-test harness** (`test/raft/crash`) and the **differential fuzzer**
  (`test/raft/difffuzz`) are the proof harnesses (§11).

---

## 10. Phased plan — REPLACE the raft LMDB+segments class (Alice, 2026-09-21)

**Decision:** log-native is not a parallel knobbed class — it **replaces** the raft
LMDB+segments byte-store path. Each phase builds the new path, proves it, then
**deletes** the old code it supersedes (git history is the record; no dual-class
knob left behind). Invariant during the migration: the tree builds and the
correctness gates (§11) pass at the end of every phase — the new path is proven
*before* the old one is removed, so there is never a broken half-migration.

**Kept, not removed:** the **postgres class** stays — it is the differential-fuzzer
oracle and the ratified fallback (D22), i.e. the instrument that *proves* this
class correct, not a duplicate. The consensus/leader/batcher/log-writer
infrastructure is evolved, not deleted (§9). What is deleted: the segment byte
store (`rsm/segments/*.seg` materialization), the LMDB message keyspaces
(`Txns`/`Dedup`/`SegLoc`/`Cursors`/`Counters`/`Pending`/`RequestIds` and their
apply writes), and the payload-in-the-raft-log double-write.

- **Phase A — per-queue message logs (kill the double-write).** Messages append
  to per-queue logs; pop reads from the log via `.qidx`; dedup reads from the log
  (lever 1 re-pointed); no more `.seg`; log not truncated below durable. Single-
  queue path only. **Gate/measure:** FAT ~2x throughput / halved gross; A20k/C1000
  double-write removed (modest).
- **Phase B — transactions (present-in-all + recovery).** `GLOBAL_TRX_ID`
  records, multi-queue fsync-all-before-ack, present-in-all recovery, tail
  truncation. **Gate:** crash matrix (partial-commit rollback), difffuzz txns.
- **Phase C — cursors + counters + pending off LMDB.** Cursors → compacted
  offsets log; counters/pending → RAM derived. **Gate/measure:** A20k/C1000 churn
  collapses → toward ~1x. This is the step that fixes the small-message shapes.
- **Phase D — request-ids off LMDB.** Windowed idempotency log / RAM. Removes the
  last random-key churn source.
- **Phase E — parallel per-queue writers (throughput).** Dispatch appends/fsyncs
  to per-queue writers; txn commit barrier. **Gate/measure:** C1000 throughput
  scales. (Re-examine NA-I2/I3 — keep decisions serial.)
- **Catalog stays in LMDB throughout.**

Order rationale: A proves the double-write mechanism + FAT win cheaply; C is what
actually moves the small-message write-amp; B is the correctness-critical
transaction machinery; D/E are the finishers.

---

## 11. Correctness gates (non-negotiable, every phase)

1. `cargo test -p queen-engine --lib` green; `cargo build --lib` green after every
   edit; clippy/rustfmt clean on touched files; the I2 disallowed-methods
   deny-gate green.
2. **Crash matrix** (`test/raft/crash`): torn tails, present-in-all rollback,
   dropped-write durability, reopen-clean — green, plus new cells for
   partial-transaction recovery and log-is-store reopen.
3. **Differential fuzzer** (`test/raft/difffuzz`): new arch vs the pg oracle at 0
   divergences on a consumer-heavy mix (the residual pop-emptiness is the known
   lease race — validate against a same-mode control, as this session did).
4. **Recovery tests:** rebuild `.qidx` + dedup + cursors + counters from the logs;
   verify present-in-all decisions and offset-gap handling.
5. A change that fails any of these is not done, however good its numbers.

---

## 12. Measurement plan (prove it, don't assert it)

Per phase, on the VM, consume-matched, retention matched, report: push
p50/p99/p999, throughput, pops/s, lag, ERRLINES, **system CPU**, **device write
MB/s (iostat)**, **write amplification (gross ÷ net)**, per shape (A20k, C1000,
FAT100). Baselines to beat (this session, HEAD):

| shape | HEAD write-amp | HEAD gross | expected after |
|---|---|---|---|
| FAT100 | 2.18x | 152 MB/s | ~1.1–1.3x (payload once) after phase A |
| A20k | 4.71x | 42 MB/s | toward ~1.5–2x after phase C |
| C1000 | 10.07x | 26 MB/s | biggest drop after C + E (churn + parallelism) |

Then the fair soak (log-native vs pg, retention matched) for stability + the
at-scale trend.

---

## 13. Risks & open questions for Alice

1. **Multi-node commit (§8)** is the hard future part (per-queue quorum /
   multi-raft). Single-node is clean; the plan is single-node first.
2. **Compaction cost** for long-retention / lagging residue — real write for
   surviving data. Log-native is a bet that Queen's traffic is mostly short-lived
   (consume-and-delete); long-retention streams may still want per-partition
   segments. Decision: is the target mostly-queues, or queues-and-streams equally?
3. **Reimplementing LMDB's free work** — cursors/idempotency as compacted logs
   with their own compaction + recovery; counters/pending as rebuildable RAM. This
   is the bulk of phases C/D and where subtle bugs live.
4. **Isolation under parallelism** — NA-I3 (free read-committed) holds only while
   the decision path is serial. Phase E must keep decisions serial or add an LSO.
5. **Catalog boundary** — kept in LMDB per your call. Open: does anything on the
   hot path need a transactional catalog read that would reintroduce churn? (First
   audit says no — catalog is create/delete-time only.)
6. **Scope vs the committed raft class** — this is a new class alongside the raft
   class (LMDB+segments) and the postgres class, not an in-place edit. Matches the
   prior "third native-log class" direction. Confirm that framing.

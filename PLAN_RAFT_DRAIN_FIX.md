# PLAN — raft drain fix (FAT100 manual-ack gap)

2026-09-21. Branch `raft`. Everything below is measured on the VM
(164.90.215.224, FAT100 = 60k msg/s, 100 partitions, 64 consumers, pop-batch
200, 1 KB payload, manual ack, long poll), with the broker's own histograms and
the temporary `queen_raft_dbg` counters + `DBGDUMP` partition dumps
(`server/src/rsm/dbgctr.rs`, not for merge).

## 0. What we measured (the facts the plan rests on)

| fact | evidence |
|---|---|
| Build + apply + fsync are NOT the cost | push prep 0.24 ms, apply_entry 0.13 ms, store_commit 0.15 ms, qlog fsync 0.001 ms; the durable point batches ~4000 commits per fsync. One-fsync A/B: ack 14.0 → 16.0 ms (noise). |
| Drain is capped at ~36k/s from the first second | ack/s 35–36k at 300k backlog while the planner is only 49% busy. |
| **The cap is ~48 orphaned leases** | `DBGDUMP`: `leased≈47-51`, `leasedBacklog` = ALL backlog, `freeBacklog≈0`, `maxLeaseAge` climbs to 60 s; top partitions leased at t≈0 with 31 400 backlog each, freed only at the 60 s expiry. Counters: `claim_ok 6235` vs `ack_fast 6187` = 48 claims never acked, `ack_reject 0`, slow path 0. goload `errs pop` 4 → 16. |
| 22× pop re-plans are a consequence | 342 wakes/s × 18.6 re-plans per wake = 6 384 pop plans/s; 64 consumers fight over the ~50 partitions that still work. `claim_none_leased` = 5.3 M in 20 s (PT=0). |
| Push plan cost grows with partition HISTORY | push-plan max 5 → 22 ms as history goes 100 → 400 records; `push_dedup_build` 3 187 builds reading 623 711 records in 60 s. Past ~1.3 M backlog the planner saturates (89%) and everything collapses. |
| `PENDING_TRANSITIONS=1` fixes the ring | `leasedInRing` 41 → 0, candidates walked 5.3 M → 0.56 M, drain 35.3k → 39.5k — but the orphans remain. |

## 1. The problems, ranked by impact

### P1 — Orphaned claims freeze partitions for 60 s (THE throughput cap)

**Mechanism.** `RaftFacade::submit` (`server/src/rsm/facade/real.rs`) sends the
command to the batcher, then waits `timeout(ctx.deadline.remaining(), rx)`. On
timeout (or client disconnect) the receiver is dropped — but the command is
already queued: the batcher plans it, the pop CLAIMS, the entry commits, the
`CursorSet` leases the partitions for `lease_seconds: 60`
(`real.rs` `pop_run`), and the reply is thrown away (`let _ = tx.send(..)`,
`batcher.rs:494` and siblings). Nobody will ever ack: the partition is frozen
for 60 s and every later push to it is backlog.

The long-poll loop makes it frequent: after a wake it re-submits even with a
few ms of deadline left (`real.rs` `pop_run`, the `wait_queue` → loop path), so
every pop that wakes near its deadline is an orphan candidate. At startup the
consumers park for the whole ramp and wake right at their deadline → ~48
partitions frozen in the first second, i.e. half the queue for the whole run.

**Fix (layered, all three):**

1. **Don't start what can't finish.** In `pop_run`, re-submit only if
   `remaining ≥ POP_SUBMIT_MIN` (start at 100 ms, or 2× the observed pipeline
   p99); otherwise answer the long poll EMPTY (a normal long-poll timeout, not
   an error).
2. **Carry the deadline into the command; the planner refuses to claim for an
   expired pop.** Add `deadline_us` to `PopCommand`; `plan_pop_*` returns
   `Plan::Empty` when `now_us > deadline_us`. This closes the whole
   queued-behind-the-pipeline window (the big one under load).
3. **Release on a dropped reply.** When the batcher's reply send fails for a
   `Pop` outcome that carries claims, enqueue a lease-release command for those
   claims (the budget-free "retry" release the ack path already has,
   `ack.rs` `sig_kind == Retry` branch, or a dedicated `LeaseRelease` effect).
   The claim is released within one pipeline trip (~10 ms) instead of 60 s. The
   same hook covers client disconnects, which (1) and (2) cannot see.

**Done when:** `DBGDUMP` shows `leased ≤ 64` with `maxLeaseAge ≪ 1 s`,
`claim_ok − ack_fast` ≈ in-flight only, `freeBacklog` ≈ 0 because there is no
backlog, and FAT100 manual-ack keeps up at 60k (lag bounded).

### P2 — Wake storm: every append/ack re-plans every parked pop

**Mechanism.** The apply-side waker (`real.rs` `NotifierWaker`) calls
`notify_waiters()` on the per-queue gate for every applied `Append`
(`apply.rs` append path pushes one wake per group) and every lease release
(`apply.rs` `cursor_set`). Every parked pop wakes and submits a fresh
`PopWildcard` through the whole pipeline. The PERF-J fast path
(`pop.rs` `wildcard_pop_provably_empty`) cannot short-circuit it on the shipped
`QUEEN_RAFT_PENDING_TRANSITIONS=0`, because every append overwrites
`ready_at ← now` even on a leased partition (`apply.rs` `append_pending`), so
leased partitions always look ready.

**Fix:**

1. **`QUEEN_RAFT_PENDING_TRANSITIONS=1` becomes the default** (measured: ring
   clean, candidates walked ÷10). Flip the differential-test config in
   lockstep, as the `ApplyConfig::pending_transitions` note requires.
2. **Selective wake.** Apply knows exactly which (queue, group, partition)
   became claimable. Wake `min(parked, partitions made claimable)` waiters per
   group instead of `notify_waiters()`, and pass the partition as the hint so
   the woken pop can try that partition first (the gate already carries a hint
   queue; the raft waker sends `""`).
3. **Fast path reads the live ring** (`Committed::has_claimable_pending`)
   instead of scanning `pending` in LMDB, so a woken pop that would fail never
   enters the batcher.

**Done when:** pop plans per productive pop ≤ ~2 (today 18.6) and the pop
share of planner time drops to noise.

### P3 — Push dedup probe re-reads the partition's whole history

**Mechanism.** Auto-created queues dedup over 3600 s
(`real.rs` `default_queue_config`). Each push probes its 100 hashes against the
bloom front; any false positive sends `dedup_probe_segment`
(`planner/mod.rs`) to `committed_txns_rows(pid)` →
`build_committed_txns_rows` → `committed_frames_of(pid, ctx, from_base = 0)`,
i.e. EVERY record the partition has — and `QLog::committed_frames`
(`qlog/mod.rs`) reads each one through `read_located`: `File::open`, read the
WHOLE record including payload, decode, copy, keep only the hashes. The result
is cached for one plan cycle only (a fresh `Planner` per cycle,
`batcher.rs` `plan_cycle_blocking`). Cost is O(partition history) on the one
planner thread.

**Fix:**

1. **Scan only the bands the front names.** `probe_plan` already returns the
   `(min_base, max_off)` band of each matching bloom generation; the `txns`
   mode uses it, the `segment`/qlog mode throws it away. Read only those
   records.
2. **Read hashes, not payloads.** Add a hashes-only read to the qlog (pread the
   header + hash block, skip the payload) and keep the fd open (the QLog already
   holds the active file; sealed files get a small fd cache).
3. **Cache committed record hashes across cycles.** Committed records are
   immutable; a bounded per-partition cache keyed by `(pid, base_offset)` makes
   a repeated maybe on the same generation free.

**Done when:** push-plan max stays flat as history grows (today 5 → 22 ms over
60 s), `push_dedup_records / push_dedup_build` is bounded by one generation.

### P4 — Per-cycle ring rebuild

`plan_cycle_blocking` runs `Derived::rebuild` (full `pending` + `leases`
scan) on EVERY plan cycle, outside the `plan` timer. Harmless at 100
partitions, O(pending + leases) per cycle at C1000/60k-partition scale. **Fix:**
hand the planner an incremental copy of the live ring (apply already maintains
it) instead of rebuilding. **Done when:** cycle overhead is flat in partition
count. (Lower priority — not the FAT100 cause.)

### P5 — One planner thread for everything

After P1–P3, re-measure planner busy at FAT100/A20k/C1000. If it is still the
ceiling, the direction is the per-queue architecture already planned
(`ALICE_PGLESS_NEWARCH.md`): one planner per queue (or queue shard), so
independent queues stop sharing a serial thread.

### Not a problem (closed)

- **Two fsyncs.** Measured: the durable point amortises ~4000 commits per fsync;
  removing the LMDB fsync moved ack latency by noise. Phase C (metadata off
  LMDB) stays justified by architecture/recovery, not by this gap.
- **Propose pipeline depth.** `QUEEN_RAFT_PIPELINE` 4 → 16 trims latency
  ~15–20% but does not move drain.
- **Empty pops (client side).** With long poll the client sees ~0; the
  in-broker re-plans are P2.

## 2. Order of work and gates

| step | work | gate (FAT100 long poll, same driver `/root/raft/scarcity.sh`) | est. |
|---|---|---|---|
| 1 | P1.1 + P1.2 (min remaining; deadline in `PopCommand`, planner refuses) | orphans ≈ 0 at startup; drain ≥ 55k | 1–2 h |
| 2 | P1.3 (release on dropped reply) | orphans 0 under forced client timeouts (goload with a short HTTP timeout) | 2–3 h |
| 3 | P2.1 (`PENDING_TRANSITIONS=1` default + diff-test config) | tests green; `leasedInRing` 0 | 1 h |
| 4 | P2.2 + P2.3 (selective wake + live-ring fast path) | pop plans / productive pop ≤ 2 | 3–4 h |
| 5 | P3.1 + P3.2 (+ P3.3 if needed) | push-plan max flat over a 10-min run | 3–4 h |
| 6 | Full matrix A20k / C1000 / FAT100 vs PG | raft ≥ PG on all three, bounded lag | 1 h |
| 7 | P4, then P5 only if step 6 says so | — | — |

Each step: commit on `raft` when its gate passes, with the measured numbers in
the message. Remove `dbgctr.rs` / `DBGDUMP` before any merge; keep the
`claims − acks` gap and the oldest-lease age as permanent metrics (they are
the orphan detector that found P1).

# Crash matrix — WP-1.8 (PLAN_RAFT.md §13.5)

- **date** 2026-09-21 09:45:51Z
- **topology** raft1 (single voter, `QUEEN_STORAGE=raft`, no Postgres)
- **scenario** push-ack: pushes with recorded ids + payload hashes to a JUDGED queue (`orders`, never popped before the crash); pops+acks a WARM queue every round so a COMPLETION crosses the armed pipeline BEFORE the crash; and pops-and-never-acks a HELD queue every round so a bare CLAIM/LEASE crosses it. One retry of every unanswered push with its original transactionId. After recovery, when a claim or completion crossed the crash, the harness waits the 60 s facade lease out and re-drains, so the ack/claim recovery outcome is observable instead of hidden behind a live lease (the WP-1.8 refutation fix).
- **broker** `/Users/alice/Work/queen/server/target/debug/queen` (debug — crash injection is a correctness test, so a debug binary is faithful; §0.3 reserves release/VM for measurements). MUST be a raft-aware build: the driver makes it prove it booted raft mode (`/health` `engine:raft`, `storageReady:true`) before any cell runs, and aborts the whole run — echoing the broker's stderr tail — when it does not.
- **cadence** `QUEEN_RAFT_DURABLE_EVERY_MS=150` so a later hit crosses a durable point quickly (§13.6: cover every periodic boundary)
- **qlog** `QUEEN_RAFT_QLOG=1` (ALICE_PGLESS_NEWARCH.md §5, Phase A3b — the double-write kill): the payload is written ONCE. The LOG WRITER writes each `Append`'s payload to its queue's qlog and fsyncs it BEFORE the referencing raft-log entry, which is now PAYLOAD-FREE; apply does the metadata only and reads the payload from the qlog. This run proves (a) the two writer cells `qlog.record_written` (payload on the page cache, entry not written) and `qlog.record_fsynced` (payload DURABLE, entry not written) both recover — a kill before the group fsync loses the unacked op cleanly, a kill after it recovers the op with its payload; (b) every EXISTING §13.5 cell still recovers with the knob on; and (c) `apply.segment_written` is N/A (apply writes no segment on this path). With the knob on, pop reads the payload FROM the qlog, so the cells also cross "acked records are readable from the qlog". A knob-OFF control run (`QUEEN_RAFT_QLOG=0`) arms `apply.segment_written` and marks the two qlog cells N/A, proving today's path is unchanged.
- **nth** each point armed at `nth=1` and `nth=2` (a later hit)
- **run dir** `/private/tmp/claude-502/-Users-alice-Work-queen/ab67919f-8724-41de-a34c-8227a0df882b/scratchpad/crashrun-fix` (data dirs, per-cell `run.jsonl`, `pre.stderr`, `post.stderr`)

> **What this does NOT test.** `faults::hit` dies by SIGKILL, which leaves the OS page cache intact, so a frame written but not yet fsynced is fully present at restart and replays. This matrix therefore proves recovery BOOKKEEPING (offsets, cursors, completion, dedup across the kill), NOT unsynced-byte loss (I11's dropped-unflushed-writes clause). That needs a fault-injecting block device (dm-flakey) on the Linux VM and is WP-1.11.

## Checks (each cell, after restart)

1. **go: delivery-at-least-once** + **payload-hash** (the shared Go checker, `test/raft/checker`): every answered push delivered; delivered bytes == pushed bytes; no phantom (a rejected id delivered). Scoped by the `drain-complete` note the harness writes for the JUDGED (`orders`) queue only. Run WITHOUT `-strict`: `delivery-at-least-once` SKIPs (not fails) when the crash preceded any acknowledged orders push — there is nothing acknowledged to judge, and that case is judged instead by check 4 (which judges the retried duplicates). A real Go violation still fails the cell.
2. **exactly-one-offset-per-txn**: every transactionId maps to a single offset across every push answer and every delivery — a second offset would be a message dedup created twice across the crash.
3. **delivered-exactly-once**: no id delivered twice in the single post-crash drain of the judged queue.
4. **answered-then-delivered**: every JUDGED (`orders`) push the broker ANSWERED (queued, or a retry's duplicate) — and that was not popped before the crash — is present after recovery (exactly-once counterpart of at-least-once).
5. **claim-redelivered-after-lease** (the WP-1.8 claim/lease recovery control): a HELD message claimed but DELIBERATELY never acked before the crash has no completion, so after its 60 s lease expires it MUST redeliver — exactly once, and never BEFORE the lease is out. A lost claim (never comes back) or a claim redelivered too early (recovery dropped the lease) fails the cell. The `claimed` column is how many such claims each cell put across the crash; it is 0 for the points that fire before any claim (N/A). This control is what lets check 6 fail: the held claim reappearing in the post-lease re-drain PROVES the observation window is genuinely past the lease.
6. **acked-not-redelivered** (the ack-path exactly-once property): a WARM message the broker confirmed COMPLETED before the crash must NOT come back — NOT in the immediate drain, NOT in the post-lease re-drain. An immediate-only observation could never falsify this (a completion wrongly resurrected as leased is hidden for 60 s); the re-drain after the lease, backed by the check-5 control, is what makes it able to fail. The `popacked` column is how many completions each cell put across the crash; 0 (and thus N/A) for the early points (see the disclosure below).
7. **raft1-leader-and-monotone-applied**: the single voter is a healthy leader after restart and its applied index does not go backwards.
8. **no-unexpected-error-lines**: broker stderr (pre- and post-crash) has no `ERROR`/panic line except the fault's own `fault: crash point … fired`.

## Matrix

| point | nth | fired | exit | verdict | applied@restart | delivered | popacked | claimed | lease-wait | notes |
|---|---|---|---|---|---|---|---|---|---|---|
| `batcher.drained` | 1 | True | -9 | **PASS** | 0 | 2 | 0 | 0 | — |  |
| `batcher.drained` | 2 | True | -9 | **PASS** | 1 | 2 | 0 | 0 | — |  |
| `planner.planned` | 1 | True | -9 | **PASS** | 0 | 2 | 0 | 0 | — |  |
| `planner.planned` | 2 | True | -9 | **PASS** | 1 | 2 | 0 | 0 | — |  |
| `propose.sent` | 1 | True | -9 | **PASS** | 0 | 2 | 0 | 0 | — |  |
| `propose.sent` | 2 | True | -9 | **PASS** | 1 | 2 | 0 | 0 | — |  |
| `log.appended` | 1 | True | -9 | **PASS** | 1 | 2 | 0 | 0 | — |  |
| `log.appended` | 2 | True | -9 | **PASS** | 2 | 2 | 0 | 0 | — |  |
| `log.flushed` | 1 | True | -9 | **PASS** | 1 | 2 | 0 | 0 | — |  |
| `log.flushed` | 2 | True | -9 | **PASS** | 2 | 2 | 0 | 0 | — |  |
| `commit.before_apply` | 1 | True | -9 | **PASS** | 1 | 2 | 0 | 0 | — |  |
| `commit.before_apply` | 2 | True | -9 | **PASS** | 2 | 2 | 0 | 0 | — |  |
| `apply.mid_entry` | 1 | True | -9 | **PASS** | 1 | 2 | 0 | 0 | — |  |
| `apply.mid_entry` | 2 | True | -9 | **PASS** | 1 | 2 | 0 | 0 | — |  |
| `apply.segment_written` | 1 | False | — | **N/A** | — | — | — | — | — | A3b: the payload is written to the qlog by the log writer (before the entry), not into a segment by apply — this state does not occur with QUEEN_RAFT_QLOG on; the durable-payload crossing is qlog.record_written / qlog.record_fsynced on the writer (QUEEN_RAFT_QLOG=0 arms it as today) |
| `apply.segment_written` | 2 | False | — | **N/A** | — | — | — | — | — | A3b: the payload is written to the qlog by the log writer (before the entry), not into a segment by apply — this state does not occur with QUEEN_RAFT_QLOG on; the durable-payload crossing is qlog.record_written / qlog.record_fsynced on the writer (QUEEN_RAFT_QLOG=0 arms it as today) |
| `apply.store_committed` | 1 | True | -9 | **PASS** | 1 | 2 | 0 | 0 | — |  |
| `apply.store_committed` | 2 | True | -9 | **PASS** | 2 | 2 | 0 | 0 | — |  |
| `qlog.record_written` | 1 | True | -9 | **PASS** | 0 | 2 | 0 | 0 | — |  |
| `qlog.record_written` | 2 | True | -9 | **PASS** | 1 | 2 | 0 | 0 | — |  |
| `qlog.record_fsynced` | 1 | True | -9 | **PASS** | 0 | 2 | 0 | 0 | — |  |
| `qlog.record_fsynced` | 2 | True | -9 | **PASS** | 1 | 2 | 0 | 0 | — |  |
| `durable.files_synced` | 1 | True | -9 | **PASS** | 8 | 4 | 8 | 8 | yes |  |
| `durable.files_synced` | 2 | True | -9 | **PASS** | 19 | 8 | 12 | 8 | yes |  |
| `durable.store_committed` | 1 | True | -9 | **PASS** | 8 | 4 | 8 | 8 | yes |  |
| `durable.store_committed` | 2 | True | -9 | **PASS** | 22 | 10 | 12 | 8 | yes |  |
| `gc.before_unlink` | 1 | False | — | **N/A** | — | — | — | — | — | not reachable by a phase-1 push/pop/ack workload (no retention or delete route yet; §10.3, WP-2.7) |
| `gc.before_unlink` | 2 | False | — | **N/A** | — | — | — | — | — | not reachable by a phase-1 push/pop/ack workload (no retention or delete route yet; §10.3, WP-2.7) |
| `gc.after_unlink` | 1 | False | — | **N/A** | — | — | — | — | — | not reachable by a phase-1 push/pop/ack workload (no retention or delete route yet; §10.3, WP-2.7) |
| `gc.after_unlink` | 2 | False | — | **N/A** | — | — | — | — | — | not reachable by a phase-1 push/pop/ack workload (no retention or delete route yet; §10.3, WP-2.7) |

## Ack-path crash coverage (the WP-1.8 coverage-gap fix)

The workload puts BOTH halves of the claim/ack path across the armed pipeline before the crash: a COMPLETION (the warm queue, popped and acked every round — the `popacked` column) and a bare CLAIM/LEASE (the held queue, popped and never acked — the `claimed` column). After recovery the harness does not judge from an immediate drain, which cannot distinguish a completed message that stayed completed from one wrongly resurrected as leased (both are hidden by the 60 s facade lease — the WP-1.8 refutation). Instead, when a claim or completion crossed the crash (`lease-wait = yes`), it waits the lease out and re-drains. The held claim MUST then redeliver exactly once (`claim-redelivered-after-lease`): that reappearance is the positive control PROVING the observation window is past the lease, which is precisely what lets `acked-not-redelivered` fail — a completion that does not reappear in the same post-lease drain genuinely stayed completed. The `durable.*` points, which fire on the periodic durable boundary after many mixed entries, are the cells that carry both halves in the HTTP matrix.

RESIDUAL, disclosed: the first ENTRIES of any run are unavoidably pushes (a claim needs a prior push), so `apply.mid_entry`, `commit.before_apply` and the other apply/log points at the matrix's `nth∈{1,2}` still crash on an `Append`, with `popacked=claimed=0`. A CLAIM/COMPLETION crashed MID-APPLY (I1, I11) is therefore NOT exercised over HTTP by those cells; it is proven at the Rust level by `rsm::tests::apply_crash`. `a_completion_entry_crashed_mid_apply_repairs` arms `apply.mid_entry` on an entry whose first effect is a `CursorSet` (a completion) and aborts AFTER that completion is in the open store txn but before the entry commits, then reopens and replays to a byte-equal digest — the specific 'a claim/ack entry crashed mid-apply' case. `each_fault_point_fires_and_the_node_repairs` covers the GENERAL mechanism (an uncommitted store txn is discarded and the whole entry is replayed, identical across effect kinds) for every apply-side point. The raft3 crash matrix under load (WP-4.10) exercises mid-apply completions over the wire. Phase-1 HTTP ack-path crash coverage for those early points is otherwise only at the Rust level.

## Per-cell checks

### `batcher.drained` nth=1 — PASS
- go:delivery-at-least-once: PASS
- go:payload-hash: PASS
- exactly-one-offset-per-txn: PASS
- delivered-exactly-once: PASS
- answered-then-delivered: PASS (2 judged)
- claim-redelivered-after-lease: N/A (no claim crossed the crash)
- acked-not-redelivered: N/A (no completion crossed the crash)
- payload-hash-python: PASS
- raft1-leader-and-monotone-applied: PASS
- no-unexpected-error-lines: PASS

### `batcher.drained` nth=2 — PASS
- go:delivery-at-least-once: PASS
- go:payload-hash: PASS
- exactly-one-offset-per-txn: PASS
- delivered-exactly-once: PASS
- answered-then-delivered: PASS (2 judged)
- claim-redelivered-after-lease: N/A (no claim crossed the crash)
- acked-not-redelivered: N/A (no completion crossed the crash)
- payload-hash-python: PASS
- raft1-leader-and-monotone-applied: PASS
- no-unexpected-error-lines: PASS

### `planner.planned` nth=1 — PASS
- go:delivery-at-least-once: PASS
- go:payload-hash: PASS
- exactly-one-offset-per-txn: PASS
- delivered-exactly-once: PASS
- answered-then-delivered: PASS (2 judged)
- claim-redelivered-after-lease: N/A (no claim crossed the crash)
- acked-not-redelivered: N/A (no completion crossed the crash)
- payload-hash-python: PASS
- raft1-leader-and-monotone-applied: PASS
- no-unexpected-error-lines: PASS

### `planner.planned` nth=2 — PASS
- go:delivery-at-least-once: PASS
- go:payload-hash: PASS
- exactly-one-offset-per-txn: PASS
- delivered-exactly-once: PASS
- answered-then-delivered: PASS (2 judged)
- claim-redelivered-after-lease: N/A (no claim crossed the crash)
- acked-not-redelivered: N/A (no completion crossed the crash)
- payload-hash-python: PASS
- raft1-leader-and-monotone-applied: PASS
- no-unexpected-error-lines: PASS

### `propose.sent` nth=1 — PASS
- go:delivery-at-least-once: PASS
- go:payload-hash: PASS
- exactly-one-offset-per-txn: PASS
- delivered-exactly-once: PASS
- answered-then-delivered: PASS (2 judged)
- claim-redelivered-after-lease: N/A (no claim crossed the crash)
- acked-not-redelivered: N/A (no completion crossed the crash)
- payload-hash-python: PASS
- raft1-leader-and-monotone-applied: PASS
- no-unexpected-error-lines: PASS

### `propose.sent` nth=2 — PASS
- go:delivery-at-least-once: PASS
- go:payload-hash: PASS
- exactly-one-offset-per-txn: PASS
- delivered-exactly-once: PASS
- answered-then-delivered: PASS (2 judged)
- claim-redelivered-after-lease: N/A (no claim crossed the crash)
- acked-not-redelivered: N/A (no completion crossed the crash)
- payload-hash-python: PASS
- raft1-leader-and-monotone-applied: PASS
- no-unexpected-error-lines: PASS

### `log.appended` nth=1 — PASS
- go:delivery-at-least-once: SKIP
- go:payload-hash: PASS
- exactly-one-offset-per-txn: PASS
- delivered-exactly-once: PASS
- answered-then-delivered: PASS (2 judged)
- claim-redelivered-after-lease: N/A (no claim crossed the crash)
- acked-not-redelivered: N/A (no completion crossed the crash)
- payload-hash-python: PASS
- raft1-leader-and-monotone-applied: PASS
- no-unexpected-error-lines: PASS

### `log.appended` nth=2 — PASS
- go:delivery-at-least-once: PASS
- go:payload-hash: PASS
- exactly-one-offset-per-txn: PASS
- delivered-exactly-once: PASS
- answered-then-delivered: PASS (2 judged)
- claim-redelivered-after-lease: N/A (no claim crossed the crash)
- acked-not-redelivered: N/A (no completion crossed the crash)
- payload-hash-python: PASS
- raft1-leader-and-monotone-applied: PASS
- no-unexpected-error-lines: PASS

### `log.flushed` nth=1 — PASS
- go:delivery-at-least-once: SKIP
- go:payload-hash: PASS
- exactly-one-offset-per-txn: PASS
- delivered-exactly-once: PASS
- answered-then-delivered: PASS (2 judged)
- claim-redelivered-after-lease: N/A (no claim crossed the crash)
- acked-not-redelivered: N/A (no completion crossed the crash)
- payload-hash-python: PASS
- raft1-leader-and-monotone-applied: PASS
- no-unexpected-error-lines: PASS

### `log.flushed` nth=2 — PASS
- go:delivery-at-least-once: PASS
- go:payload-hash: PASS
- exactly-one-offset-per-txn: PASS
- delivered-exactly-once: PASS
- answered-then-delivered: PASS (2 judged)
- claim-redelivered-after-lease: N/A (no claim crossed the crash)
- acked-not-redelivered: N/A (no completion crossed the crash)
- payload-hash-python: PASS
- raft1-leader-and-monotone-applied: PASS
- no-unexpected-error-lines: PASS

### `commit.before_apply` nth=1 — PASS
- go:delivery-at-least-once: SKIP
- go:payload-hash: PASS
- exactly-one-offset-per-txn: PASS
- delivered-exactly-once: PASS
- answered-then-delivered: PASS (2 judged)
- claim-redelivered-after-lease: N/A (no claim crossed the crash)
- acked-not-redelivered: N/A (no completion crossed the crash)
- payload-hash-python: PASS
- raft1-leader-and-monotone-applied: PASS
- no-unexpected-error-lines: PASS

### `commit.before_apply` nth=2 — PASS
- go:delivery-at-least-once: PASS
- go:payload-hash: PASS
- exactly-one-offset-per-txn: PASS
- delivered-exactly-once: PASS
- answered-then-delivered: PASS (2 judged)
- claim-redelivered-after-lease: N/A (no claim crossed the crash)
- acked-not-redelivered: N/A (no completion crossed the crash)
- payload-hash-python: PASS
- raft1-leader-and-monotone-applied: PASS
- no-unexpected-error-lines: PASS

### `apply.mid_entry` nth=1 — PASS
- go:delivery-at-least-once: SKIP
- go:payload-hash: PASS
- exactly-one-offset-per-txn: PASS
- delivered-exactly-once: PASS
- answered-then-delivered: PASS (2 judged)
- claim-redelivered-after-lease: N/A (no claim crossed the crash)
- acked-not-redelivered: N/A (no completion crossed the crash)
- payload-hash-python: PASS
- raft1-leader-and-monotone-applied: PASS
- no-unexpected-error-lines: PASS

### `apply.mid_entry` nth=2 — PASS
- go:delivery-at-least-once: SKIP
- go:payload-hash: PASS
- exactly-one-offset-per-txn: PASS
- delivered-exactly-once: PASS
- answered-then-delivered: PASS (2 judged)
- claim-redelivered-after-lease: N/A (no claim crossed the crash)
- acked-not-redelivered: N/A (no completion crossed the crash)
- payload-hash-python: PASS
- raft1-leader-and-monotone-applied: PASS
- no-unexpected-error-lines: PASS

### `apply.store_committed` nth=1 — PASS
- go:delivery-at-least-once: PASS
- go:payload-hash: PASS
- exactly-one-offset-per-txn: PASS
- delivered-exactly-once: PASS
- answered-then-delivered: PASS (2 judged)
- claim-redelivered-after-lease: N/A (no claim crossed the crash)
- acked-not-redelivered: N/A (no completion crossed the crash)
- payload-hash-python: PASS
- raft1-leader-and-monotone-applied: PASS
- no-unexpected-error-lines: PASS

### `apply.store_committed` nth=2 — PASS
- go:delivery-at-least-once: PASS
- go:payload-hash: PASS
- exactly-one-offset-per-txn: PASS
- delivered-exactly-once: PASS
- answered-then-delivered: PASS (2 judged)
- claim-redelivered-after-lease: N/A (no claim crossed the crash)
- acked-not-redelivered: N/A (no completion crossed the crash)
- payload-hash-python: PASS
- raft1-leader-and-monotone-applied: PASS
- no-unexpected-error-lines: PASS

### `qlog.record_written` nth=1 — PASS
- go:delivery-at-least-once: PASS
- go:payload-hash: PASS
- exactly-one-offset-per-txn: PASS
- delivered-exactly-once: PASS
- answered-then-delivered: PASS (2 judged)
- claim-redelivered-after-lease: N/A (no claim crossed the crash)
- acked-not-redelivered: N/A (no completion crossed the crash)
- payload-hash-python: PASS
- raft1-leader-and-monotone-applied: PASS
- no-unexpected-error-lines: PASS

### `qlog.record_written` nth=2 — PASS
- go:delivery-at-least-once: PASS
- go:payload-hash: PASS
- exactly-one-offset-per-txn: PASS
- delivered-exactly-once: PASS
- answered-then-delivered: PASS (2 judged)
- claim-redelivered-after-lease: N/A (no claim crossed the crash)
- acked-not-redelivered: N/A (no completion crossed the crash)
- payload-hash-python: PASS
- raft1-leader-and-monotone-applied: PASS
- no-unexpected-error-lines: PASS

### `qlog.record_fsynced` nth=1 — PASS
- go:delivery-at-least-once: PASS
- go:payload-hash: PASS
- exactly-one-offset-per-txn: PASS
- delivered-exactly-once: PASS
- answered-then-delivered: PASS (2 judged)
- claim-redelivered-after-lease: N/A (no claim crossed the crash)
- acked-not-redelivered: N/A (no completion crossed the crash)
- payload-hash-python: PASS
- raft1-leader-and-monotone-applied: PASS
- no-unexpected-error-lines: PASS

### `qlog.record_fsynced` nth=2 — PASS
- go:delivery-at-least-once: PASS
- go:payload-hash: PASS
- exactly-one-offset-per-txn: PASS
- delivered-exactly-once: PASS
- answered-then-delivered: PASS (2 judged)
- claim-redelivered-after-lease: N/A (no claim crossed the crash)
- acked-not-redelivered: N/A (no completion crossed the crash)
- payload-hash-python: PASS
- raft1-leader-and-monotone-applied: PASS
- no-unexpected-error-lines: PASS

### `durable.files_synced` nth=1 — PASS
- go:delivery-at-least-once: PASS
- go:payload-hash: PASS
- exactly-one-offset-per-txn: PASS
- delivered-exactly-once: PASS
- answered-then-delivered: PASS (4 judged)
- claim-redelivered-after-lease: PASS (8 claims redelivered exactly once after the lease)
- acked-not-redelivered: PASS (8 completions absent after the lease expired; the held control confirmed the window is past the lease)
- payload-hash-python: PASS
- raft1-leader-and-monotone-applied: PASS
- no-unexpected-error-lines: PASS

### `durable.files_synced` nth=2 — PASS
- go:delivery-at-least-once: PASS
- go:payload-hash: PASS
- exactly-one-offset-per-txn: PASS
- delivered-exactly-once: PASS
- answered-then-delivered: PASS (8 judged)
- claim-redelivered-after-lease: PASS (8 claims redelivered exactly once after the lease)
- acked-not-redelivered: PASS (12 completions absent after the lease expired; the held control confirmed the window is past the lease)
- payload-hash-python: PASS
- raft1-leader-and-monotone-applied: PASS
- no-unexpected-error-lines: PASS

### `durable.store_committed` nth=1 — PASS
- go:delivery-at-least-once: PASS
- go:payload-hash: PASS
- exactly-one-offset-per-txn: PASS
- delivered-exactly-once: PASS
- answered-then-delivered: PASS (4 judged)
- claim-redelivered-after-lease: PASS (8 claims redelivered exactly once after the lease)
- acked-not-redelivered: PASS (8 completions absent after the lease expired; the held control confirmed the window is past the lease)
- payload-hash-python: PASS
- raft1-leader-and-monotone-applied: PASS
- no-unexpected-error-lines: PASS

### `durable.store_committed` nth=2 — PASS
- go:delivery-at-least-once: PASS
- go:payload-hash: PASS
- exactly-one-offset-per-txn: PASS
- delivered-exactly-once: PASS
- answered-then-delivered: PASS (10 judged)
- claim-redelivered-after-lease: PASS (8 claims redelivered exactly once after the lease)
- acked-not-redelivered: PASS (12 completions absent after the lease expired; the held control confirmed the window is past the lease)
- payload-hash-python: PASS
- raft1-leader-and-monotone-applied: PASS
- no-unexpected-error-lines: PASS

## Points not run here, and why

- **`gc.before_unlink`, `gc.after_unlink`** — phase-1 §13.5 points, WIRED in `rsm/apply.rs::unlink_staged` and proven to fire by the Rust apply-crash fault test (`rsm::tests::apply_crash`), but NOT reachable from a phase-1 push/pop/ack workload: a file becomes collectable only through retention or a delete, and neither has an HTTP route in phase 1 (§10.3). The gc-compaction scenario drives them once retention lands (WP-2.7).
- **`seg.rolled`, `seg.qidx_written`** — extra points (R-107) for the segment roll tests, not in the §13.5 HTTP matrix; the default 64 MiB segment does not roll under a phase-1 workload.
- **snapshots, compaction, identity, membership, transfer** — phase 2+ / raft3-only (`points.py` `phase`/`topology`); their code paths do not exist in a phase-1 raft1 broker, which refuses to arm them (`rsm/faults.rs` exits 2).

## Power-loss (dm-flakey) — the WP-1.11 durability gate (Linux VM, `QUEEN_RAFT_QLOG=1`)

The SIGKILL matrix above proves recovery **bookkeeping**; it cannot prove **unsynced-byte loss** (page cache survives a SIGKILL). That is what A3b changes: with the double-write killed, the qlog is the sole byte store, and an acked payload is durable ONLY because the writer fsyncs the qlog before the referencing entry commits. If that ordering is wrong, a power cut loses acked data silently. Proven on the VM (164.90.215.224, `queen-a2` release build) with a `dm-flakey` device that, when toggled, silently DROPS every subsequent write (an fsync that returns success and loses the bytes = a power cut):

- **No acked message lost (exact, broker-truth).** Push **1000** distinct, unconsumed messages (each acked ⇒ durable), `sync`, cut power (`drop_writes`), `kill -9`, remount clean, reopen, drain-count survivors: **1000 acked → 1000 survived** (0 lost, 0 fatal broker lines). The ordering holds — every payload fsynced before its ack was on the platter.
- **Reopen is clean and never behind the log (×3 rounds).** A mixed push/pop/ack workload, cut mid-flight, `kill -9`, remount: the broker reopens clean every round, and **NA-QLOG-I1 holds** (the qlog's durable tail is ≥ the store's applied index — never a committed entry pointing at a missing payload; the reconcile would REFUSE to start otherwise). 0 unexpected error lines.
- **The double-write kill is behaviorally transparent.** Differential fuzzer, two real raft brokers, knob OFF (segments+blob) vs knob ON (qlog-only), both `DEDUP_INDEX=segment`, N=50: **clean=50, divergences=0** against each other — pop bytes, dedup verdicts, and digests identical whether the payload lives in the raft log or the qlog.


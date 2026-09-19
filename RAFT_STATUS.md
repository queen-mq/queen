# RAFT_STATUS.md

The working record of PLAN_RAFT.md (Queen without Postgres: one replicated
log). The plan itself is never edited for progress; everything that is done,
decided, found or measured is recorded here. Created by WP-0.1 on 2026-09-17;
the G0 packet closed phase 0 (2026-09-18); this revision (2026-09-19) is the
**G1 packet, pipeline=4 pass**: phase 1 built and measured on the VM at the
ratified pipeline after the F-1 fix and the night run, now with the
**performance package run to round 3** (M8/M9/M10) folded in.

Base: branch `raft` in `/Users/alice/Work/queen`, cut from `fc71b65b`
(master, 1.6.0). The pgless hardening tree is parked as `2bbd10d1` on branch
`pgless`; the superseded pgless engine is read with
`git show 6e96e228:server/src/native/<file>`.

Shape: PLAN_RAFT.md §15.0.

**G0 ratified by Alice on 2026-09-18, in two rounds.** Round one (before this
packet): every recommendation as listed in PLAN_RAFT.md §17, with O14's
failover target at ≤ 4 s. Round two (after this packet and its reviews),
answering the checklist below: (1) D9 **heed everywhere**, 3 voters and
single-voter/embedded alike, with the four pins of the amended proposal, and
§11.5 repair-from-own-snapshot for a single voter; (2) §11.5 repair rather than
discard; (3) D10 option (a) in the lean encoding, without waiting for D-06; (4)
D11 as proposed with the seven plan changes; (5) D12 framed TCP with the
**sequenced per-frame MAC**, not TLS (trust anchor undefined); (6) §11.4 and
§11.8 defaults as proposed, tuned inside WP-1.4 / WP-1.2; (7) O1–O23 as
recommended, D14 kept at 1–2 s; (8) O14 as recommended, and the evidence gaps
below **accepted**: the D-01..D-03 and D-09 runs are skipped, WP-1.2's own crash
tests are the check, and any disagreement re-opens D9. PLAN_RAFT.md carries the
ratified wording (header, §2, I3, §6.1, §9.4, §11.3, §11.5, §17, Appendix H).
Phase 1 starts 2026-09-18.

Phase 0 is committed on `raft` as eight commits (`34c5714f`..`f4b0337c`), plus
the ratification commit that follows this revision. No product code changed in
phase 0.

**G1 packet — phase 1 built and measured at pipeline=4, gate open for Alice
(revised 2026-09-19).** Phase 1 is committed on `raft` (`d6500817`..`ee181d91`,
WP-1.1..WP-1.11); the **F-1 fix** at `f8bfa672`; the draft G1 packet at
`29bc7cdd`. This revision folds in the **night run of 2026-09-18/19** — the owed
pipeline=4 VM pass on the F-1-fixed binary (md5 `cdc5bb59af27`), recorded in
`test/raft/vm/raft1/NIGHT-A-pipeline4.md` (O14 / flatness / O19), `NIGHT-B-dedup.md`
(the 3600 s dedup window) and `NIGHT-C-flaky.md` (dropped writes at pipeline=4),
with the raw evidence under `night/`. Single node, message path, LocalReplicator;
no Raft, no network. The one-line verdict, detail in the **G1 checklist**, **M6**
(pipeline=1) and **M7** (pipeline=4) below: **parity, crash safety and
dropped-write durability hold; F-1 is fixed and now discharged on the VM (0 poison
/ 0 `TimeWentBackwards` across every pipeline=4 run of the night — the six-regime
sweep, a 5-min sustained A20k, a 75-min congestion collapse, a 64-min fill and 100
crash-reopen rounds, ~200 M+ pushes total). But the performance and isolation
story is now measured and NEGATIVE, not absent: O14 FAILS, I8 flatness is mixed
(FAIL on the strict gate), O19 FAILS.** The night also surfaced two HIGH defects:
**R-123** (SIGBUS on store reopen after dropped writes, 1/100 — the reproduced
WP-0.3 D-01/D-03 heed "would not reopen") and **R-121** (unbounded push-admission
queueing with no push-side 503 at a sustained 50k offered). Postgres (D22) is
still the oracle; nothing in phase 1 changes that.

---

## Gates

| gate | state | date | decision | evidence |
|---|---|---|---|---|
| G0 | **RATIFIED** | 2026-09-18 | D9 heed everywhere (+ pins, §11.5 repair); D10 option (a) lean; D11 openraft `54094270` + raft-log 0.4.6 with the seven plan changes; D12 framed TCP + sequenced per-frame MAC; §11.4/§11.8 defaults as proposed; O1–O23 as recommended (O14 failover ≤ 4 s, D14 kept); evidence gaps accepted, phase 1 approved. See the note at the top. | WP-0.2 `test/raft/vm/baseline/RESULTS.md`; WP-0.3 `test/raft/spikes/s1-store/MEMO.md`; WP-0.4 `s2-dedup/MEMO.md`; WP-0.5 `s3-consensus/MEMO.md`; WP-0.6 `s4-transport/MEMO.md`; WP-0.7 `test/raft/README.md`; Findings R-01..R-64 below |
| G1 | **phase 1 built & measured at pipeline=4 — open for Alice** | 2026-09-19 | Message-path parity **PASS** (difffuzz clean after 7 documented envelope gaps; RAFT PARITY js lane green); crash matrix **22/22** reachable cells PASS; dropped-writes durability **PASS** (pipeline=1 20/20; pipeline=4 **99/100**, the 1 FAIL is R-123). **F-1 fixed (`f8bfa672`) and discharged on the VM** (0 poison across every pipeline=4 run). At pipeline=4: **O14 FAIL** (A-regime push p50 ≤ pg, but p99 ~6× pg; C1000 fan-out FAIL, cleanest fresh-store p50 **9.79** = 1.48× pg 6.62; FAT100 ≈**31%** of the knee); **I8 flatness mixed/FAIL** (CPU+disk flat, A20k p50/p99 isolated, but RSS not flat ~**387 B/msg** and C1000 p50 **+546%**); **O19 FAIL** (a 463/s DLQ storm lifts cold-partition p99 **+90.6%**, **+53.2%** at rest). New: R-121 (push admission), R-123 (SIGBUS reopen). See the G1 checklist. | crash `test/raft/crash/RESULTS.md`; difffuzz `PARITY-NOTES.md`; pipeline=1 `vm/raft1/RESULTS.md` + **M6**; pipeline=4 `vm/raft1/NIGHT-{A,B,C}-*.md` + **M7**; Findings R-101..R-124 |
| G2 | not reached | — | Full parity on raft1 (RAFT PARITY identical to `single`), differential clean, crash matrix and flatness pass, performance vs O14. | — |
| G3 | not reached | — | Confirm the consensus library, re-checking its release status and the open issues (openraft GH#2080 still OPEN on 2026-09-18). | — |
| G4 | not reached | — | raft3 parity; kill matrix with zero acknowledged loss and failover within target; equal digests; performance within the O14 budget. | — |
| G5 | not reached | — | Go / no-go for stage. | — |

### G0 checklist — what Alice decides

1. **D9 store engine.** heed 0.22.1 for the 3-voter broker *with three pins*
   (NO_SYNC, a read-transaction handle in the store adapter, `max_readers`), and
   **redb 2.6.3 for raft1 / embedded**. Or: redb everywhere and pay ~26k msg/s.
2. **§11.5 repair vs discard** — decide *before* WP-1.2 picks the engine: it
   makes fjall's failures recoverable and does nothing for heed's (R-03, R-16).
3. **D10 dedup.** Option (a) in the lean encoding. Or wait for the 3600 s
   window run (D-06) first.
4. **D11 consensus.** openraft @ `54094270` + `raft-log 0.4.6` as the
   *direction* (D11 confirms at G3), plus the seven plan changes.
5. **D12 transport.** Framed TCP on 6634: yes. **Which authentication branch**:
   sequenced per-frame MAC (needs only `QUEEN_RAFT_SECRET`) or TLS — and if
   TLS, **the certificate trust anchor**, which G0 has not answered and which
   blocks that branch (R-57).
6. **§11.4** bucket fan-out bound and durable-point cadence default;
   **§11.8** LMDB map-size rule.
7. **O1–O23** below (O21–O23 are new).
8. **Performance targets (O14)** and whether the gaps below are acceptable for
   phase 1 to start.

### G0 checklist — what is still missing

| WP | gap | size |
|---|---|---|
| WP-0.3 | PASS/FAIL exit criterion **not met at the mandated sample size**: 15 kill runs + 20 dm-flakey runs in all, against 100 kill runs per engine plus an equivalent dropped-write sample. The kill half cannot falsify (page cache survives a process kill): all 45 passed. All discrimination comes from 20 dm-flakey runs (5 fjall, 5 redb, 10 heed). | D-01, D-02 (~2 h VM) |
| WP-0.3 | heed's 1-in-10 "store would not reopen" is **unexplained** and unreproduced; the crash matrix was never re-run in the ratified store shape. | D-01, D-03 |
| WP-0.4 | The criterion WP-0.4 names — **probe p99 at 50k msg/s with a 1 h window** — was never run. Longest window: 360 s (VM). No cell held 50 000 msg/s under a real ack load for a full run. | D-06 (~6 h VM) |
| WP-0.5 | The 10 GiB manifest snapshot ran at **1 GiB**; no kill during a snapshot build or install; **a pipeline of exactly 4 (the D4 amendment) was never measured** (1 and 16 were). Snapshot *cadence* against `QUEEN_RAFT_SNAPSHOT_LOG_BYTES` unmeasured. | D-09, D-10, D-11 |
| WP-0.6 | No RTT (netem needs Alice), no HTTP/2 comparison, no failure/reconnect behaviour, `MAX_FRAME` 8 MiB vs `QUEEN_RAFT_ENTRY_MAX_BYTES` 96 MiB unreconciled, certificate anchor undecided. | D-13..D-17 |
| all | Every regime is single-VM with a **co-resident loader** (FAT100: goload 3.31 + queen 1.38 + pg 1.39 = 6.08 of 8 vCPU). Three VMs are O13. | G4 |

---

### G1 checklist — what phase 1 proved, and what it did not

Against the gate criterion (PLAN §15: *message-path parity, crash matrix,
differential and flatness results, raft1 performance vs postgres against the
O14 targets*). Numbers, not adjectives; what failed is stated plainly.

**Performance package, rounds 1–3 (2026-09-19) — the durable-point wall is down, the
consume "regression" is settled as a non-regression, and both remaining O14 misses are
the propose path, upstream of everything the store and claim levers touch (M8 / M9 /
M10).** In plain words, what changed and by how much:

- **The push tail (A20k).** Round 1's store levers (PERF-A/B/C) did **not** move it —
  the durable point stayed **268 ms** on the apply thread. Round 2 removed what loaded it
  (the per-message dedup RMW → PERF-E; the per-append counter/pending writes → PERF-D; the
  256-file fsync fan-out → PERF-F) and the durable point **collapsed 268 → 16.8 ms** (16×),
  pop-independent; A20k push p99 fell **203 → 16.5–29.8 ms**, RSS 348 → 141 MB. Round 3's
  scheduling lever (PERF-G) reads **p99 17.28 / p999 25.22 ms** at a parity p50 **6.05** —
  but the **push-p50 floor did not move**: it is `arrival→proposed` **p50 4.19 ms**, the
  batcher holding a command ~4 ms before it is proposed, structural to the single-plan-cycle
  batcher and untouched by G/H/I. p50 is **0.66–0.72× pg**, p99 **parity** with pg (a review
  corrected round 2's A/B for unequal pop load; the clean round-3 tail is run-to-run, not a
  demonstrated PERF-G gain — minus-G reads the same p999).
- **The consume path (A20k pop).** Round 2 left an open worry (a "new" config popped
  3.6 k/s vs 9.2 k/s "old"). Round 3's A/B (**AB-0 / PERF-6**) settles it: **not a
  regression.** The *identical* config popped 3.4 k/s then 11.4 k/s on two replicates (3.35×,
  nothing changed); the 2×2 knob ordering sign-flips run to run; stage histograms are
  byte-identical where pop rates differ most; all 8 runs fully drain (~11–12 k/s once push is
  lifted). `DEDUP_INDEX=txns` does **not** regress consume (it helps push; one small
  consistent empty-poll signature, ~12%, changes no verdict); the ring re-arms correctly
  (**PERF-H** confirmed the planner rebuilds the ring from `pending` every cycle). There is
  **no ring pathology to fix** — the low pop rate is the ordinary under-consume split of the
  single apply thread, run-to-run variable.
- **The many-partition shape (C1000).** **Unchanged across all three rounds, by design** —
  its bound was never the store. Round 3's O(claimed) wildcard pop (PERF-I) took the plan
  cost off the claim path (`pop_wildcard` plan p99 1.05 ms, flat in history), but push **p50
  211.97 ms = 32× pg** is bound by `arrival→proposed` **≈ 1073 ms**, identical
  old↔round-3↔every knob — the per-partition propose serialization, **upstream** of the store
  and the claim. PIPELINE=8 makes A20k worse (apply-thread-bound, not slot-starved), so 4 is
  the right default.
- **Throughput (FAT100).** **3× — 86 k → 265 k msg/s** in round 2, held in round 3 at
  **260 k = 90.7% of the 287 k postgres knee.** `mdb_page_flush` (round-1's dominant 13.3%
  CPU) is gone from the profile; FAT is now allocator-bound on the per-message JSON `Value`
  parse, not fsync-bound.

**O14 gate (raft1 p50 AND p99 ≤ pg; fat-batch ≥ 70% of the knee), round-3 defaults:**

| target | round 3 | postgres | ratio / % | verdict |
|---|---|---|---|---|
| A20k p50 | 6.05 | 9.15 | 0.66× | **PASS** (O14 strict; misses only the round-3 aggressive ≤ 4 ms gate) |
| A20k p99 | 17.28 | 27.01 | 0.64× | **PASS** |
| A20k pop/s (drained) | 6.3 k | — | — | **n/e** on one run (drained=yes; 6.3 k inside PERF-6's 3.4–11.4 k) |
| C1000 p50 | 211.97 | 6.62 | 32.0× | **FAIL** |
| C1000 p99 | 790.53 | 33.02 | 23.9× | **FAIL** |
| FAT100 tput | 260,498 | 287,363 | 90.7% | **PASS** (> 70%) |

`proceed=false` by the letter of the gate (C1000 p50/p99 fail; A20k passes O14 strict on
both p50 and p99). The long **PERF-8** runs were **not** run — Alice's rule, do not run long
tests if the short ones are bad. **What still bounds each symptom, and the next lever:** A20k
p50 = the ~4 ms `arrival→proposed` batcher hold, structural to the single-plan-cycle batcher →
the propose path is next (deepening the pipeline regresses it); FAT's next lever is the
per-message `serde_json::Value` parse (receiver-side JSON), **not** the store; C1000 = the
per-partition propose serialization → per-partition propose parallelism, upstream of apply, a
separate work item. **Defaults after round 3:** `QUEEN_RAFT_DRIVER_NOTIFY`=on and
`QUEEN_RAFT_CLAIM_FROM_RING`=on; `QUEEN_RAFT_WRITER_PIPELINE` and
`QUEEN_RAFT_PENDING_TRANSITIONS` **off** (R-131 — the pending-transitions flip is one
coordinated commit), on top of round 2's adaptive `QUEEN_RAFT_APPLY_WRITERS`,
`QUEEN_RAFT_BUCKETS`=16 and `QUEEN_RAFT_DEDUP_INDEX`=txns. G/H/I regress nothing and are
correctness/scaling hardening; the owed consume A/B (R-130) is now **discharged** (PERF-6:
non-regression, no ring pathology).

| criterion | verdict | evidence |
|---|---|---|
| message-path parity (differential fuzzer) | **PASS** | thousands of 250-op seeds, **0 divergence** in message-path responses once 7 classified envelope gaps are absorbed; per-side checker (at-least-once + payload-hash) green on **both** brokers. `PARITY-NOTES.md`. Caveat: the 2000-seed target was not reached (200+; a contended shared checkout with a concurrent crash harness), re-runnable on a release broker. |
| RAFT PARITY (client suite) | **PASS, narrow** | `run.sh --topo raft1` js lane green (kept 12/12, skipped 161 by un-ported route), `rc=0`. The parity *surface* is **one** message-path test today (`testPushAutoCreatesQueueAndPartition`) — every other suite test needs queue admin (WP-2.5) or a phase-2 route; it grows when those land. single↔raft1 gate print not captured in-session (a sibling's `single` stack was live in the shared checkout). go/py raft lanes deferred until their runners honour `QUEEN_TEST_STORAGE=raft`. |
| crash matrix (§13.5) | **PASS** | 13 phase-1 points × {nth 1,2} = **22 PASS / 0 FAIL / 4 N/A**; every reachable cell recovers **exactly once** across `kill -9` (dedup, one txn→one offset, held claims redelivered after the lease, completions not resurrected). 4 N/A = gc points (no retention/delete route in phase 1), proven at the Rust level to a byte-equal digest. `crash/RESULTS.md`. Limit: SIGKILL keeps the page cache, so this proves bookkeeping, not unsynced-byte loss — covered by the next row. |
| dropped-unflushed-writes durability | **PASS (with one HIGH defect, R-123)** | discharges **R-106 / R-111 / R-114**. pipeline=1: 20/20 dm-flakey rounds, ~73k acked, `missing=0`. pipeline=4 (NIGHT-C, F-1-fixed binary): **99/100** rounds every acked push delivered after reopen (`acked==ledger==delivered`, `missing=0`), reopen 316 ms–9.2 s, ~45.6 M acked, 0 poison. The **1 FAIL (round 75) is R-123**: `SIGBUS`/`BUS_ADRERR` on the `store/data.mdb` read mmap during apply-replay — the fsync'd log is intact (`truncated_tail=false`, 680 690 frames), but the torn `MDB_NOSYNC` store crashes the process before recovery completes. The durability *property* (log-is-truth) holds; the store-reopen path has a hazard. `NIGHT-C-flaky.md`. |
| flatness (I8, §13.6) | **mixed — FAIL on the strict gate** | pipeline=4, 8.14 M at-rest / 6-min runs (NIGHT-A §2). **CPU and disk ARE flat** with store size (G-3's core claim holds: A20k CPU +0.0%, disk +2.9%). The **A20k** shape's p50/p99 **are isolated** from an 8 M cold store (+8.1%/+5.4% PASS) — but its **p999 +155%** and RSS are not. The **C1000** (1000-single-message-partition, fan-out) shape is **NOT isolated**: p50 **+546%**, ack **+122%**, p99 +15% — all FAIL. **RSS is not flat in any shape**: ~**387 B/msg** at rest (8.14 M → ~3.0 GB), lower than pipeline=1's 610 (R-105) but still **linear in stored count**, and it climbs within every run (the 3600 s dedup index retains every push). **Provenance caveat:** `flatness-night.sh` is not committed and the surviving `.results`/`compare-*.txt` carry the literal `pipeline=1` label the sed missed; pipeline=4 is the agent's note (mechanically consistent — empty-A20k p50 56.58/p99 452.6 matches the committed pipeline=4 `fivemin.sh` 44.29/419.8 — but not independently checkable from committed artifacts). |
| noisy neighbour (O19) | **FAIL** | pipeline=4, a **real** active DLQ storm (NIGHT-A §3, `dlqstorm.py`: wildcard queue-mode pop + ack `status:"dlq"`). A light storm on one hot partition — **463/s**, 45 350 rows filed — raised the cold partitions' p99 **+90.6%** (87.55 → 166.91 ms); the 2 M backlog **at rest** (storm stopped) still held it **+53.2%** (→ 134.14). Both blow ±15%. Quiet throughput never lagged; only its latency did. **Reproducible from committed raw** (`night/quiet-{before,during,after}.gl` finals; `storm.out` 45350/98 s = 463/s) — the WP-1.11 transcription-error cell is superseded by a clean numeric verdict. |
| raft1 performance vs O14 | **still FAIL on the gate (C1000 p50/p99), but rounds 1–3 moved the tail and throughput and settled the consume worry** | M7 (round 0, no levers) was push p99 ~6× pg and FAT ≈31% of the knee. **Round 1 (M8)** — the store levers PERF-A/B/C did **not** move the O14 numbers (durable point stayed 268 ms on the apply thread); PERF-B alone earned its place (8 M-loaded C1000 flatness +546%→+50%). **Round 2 (M9)** — PERF-D+E+F collapsed the durable point **268→16.8 ms** (16×), so **A20k p99 203→16.5–29.8 ms** (p50 **0.72× pg**, p99 **parity** with pg: 0.61× lightly-consumed / **1.10× consume-matched**) and **FAT100 86k→265k msg/s = 92%** of the 287k knee (**PASS >70%**). **C1000 is unchanged (24.9× pg p50)** — its bound is `arrival→proposed` ≈1073 ms, upstream of the store, so `proceed=false`. **Round 3 (M10)** — G/H/I (batcher/writer scheduling, ring re-arm, O(claimed) wildcard pop) are correctness/scaling hardening: A20k **p50 6.05 / p99 17.28** (both ≤ pg, O14-strict PASS), FAT100 **90.7%**, C1000 **p50 211.97 / p99 790.53** — performance-neutral-to-parity vs round 2, regressing nothing, but they move **neither push-latency bound** (A20k `arrival→proposed` p50 4.19 ms batcher hold; C1000 p99 1073 ms per-partition propose). **AB-0 (PERF-6)** discharges R-130: the consume rate is a **non-regression** (identical config 3.4↔11.4 k/s run-to-run; all 8 cells drain; no ring pathology; `DEDUP_INDEX=txns` helps push, not hurts consume). PIPELINE=8 regresses A20k (apply-thread-bound), so 4 stays default. C1000 p50 32× pg still bounds the gate, so `proceed=false`; long **PERF-8** skipped. Like-for-like: byte-identical goload flags, loader md5 `2c97cccb0d1e`. Next levers: FAT = per-message `serde_json::Value` alloc; C1000 = per-partition propose parallelism; A20k p50 = the propose path. |

**F-1 / R-117 is fixed and discharged; the standing concerns are now R-123 and
R-121.** F-1 — the batcher `tokio::spawn`ed each `repl.propose()` independently, so
the `LocalReplicator` writer could assign the log index **out of plan order** →
apply saw `now_us` go backwards with the index → refused (**I5** `TimeWentBackwards`)
→ the node poisoned under any concurrent load — was fixed at **`f8bfa672`**: the
propose is submitted **inline on the one driver task in plan order** (a no-op-waker
first poll drives the future to `LocalReplicator`'s pre-`.await` writer submission;
only the local-apply wait is spawned, so up to `pipeline` stay in flight), with a
deterministic witness (`every_propose_submits_on_the_one_driver_task_in_plan_order`,
one task id vs N) and `apply.rs` now logging the gate refusal it used to swallow
(R-118). **The night's pipeline=4 pass discharges it on the VM**: 0 poison / 0
`TimeWentBackwards` across the six-regime sweep, a 5-min sustained A20k, a 75-min
congestion collapse, a 64-min fill and 100 crash-reopen rounds (~200 M+ pushes) —
the first proof under the VM scheduler that produced F-1 (a laptop never reproduced
it). One MINOR test-net gap stays open (the deterministic guard drives a fake, not
the real `LocalReplicator` submit-before-`.await` contract; R-117). What now stands
between here and a clean G1 is not correctness under load but two night defects:
**R-123** (1/100 dropped-writes rounds the broker `SIGBUS`es on the `store/data.mdb`
read mmap during apply-replay and does not reopen — the reproduced WP-0.3 D-01/D-03
hazard; the log is intact, the store is torn) and **R-121** (at a sustained 50k
offered the node queues pushes without bound — 10 s+ hangs, no push-side 503 —
until achieved throughput hits 0 at ~min 55; a liveness/backpressure gap, not a
crash).

### G1 checklist — what Alice decides (in priority order)

1. **G1 go / no-go, now that the numbers exist and the performance package ran to
   round 3.** Parity, crash safety and dropped-write durability are PROVEN and F-1 is
   fixed (`f8bfa672`) and discharged on the VM. The package (M8/M9/M10) **closed the
   durable-point wall**: A20k push p99 is now parity with pg (was ~6×) and FAT100 is
   ~91% of the knee (was 31%) — **two of three O14 targets move**. **Round 3 (M10)**
   added the scheduling / ring-re-arm / O(claimed)-wildcard-pop hardening (G/H/I):
   performance-neutral-to-parity, regresses nothing, and **AB-0 (PERF-6) discharged
   R-130** — the consume rate is a **non-regression** (run-to-run variance, no ring
   pathology; `DEDUP_INDEX=txns` helps push, not hurts consume). What remains is
   **C1000** (p50 32× pg), whose bound is `arrival→proposed` ≈1073 ms **upstream of the
   store** (per-partition propose serialization), unmoved by G/H/I, plus the A20k p50
   ~4 ms batcher hold — both the propose path, and neither is what round 3 did; so
   `proceed=false` by the letter of the gate. **I8 is still mixed** (CPU/disk flat, A20k
   median isolated, RSS not flat, C1000 not isolated) and **O19 still FAILS** (a 463/s
   DLQ storm lifts cold-partition p99 +90.6%). Either **(a)** hold G1 open until a
   per-partition-propose work item closes C1000, or **(b)** ratify G1 on parity + crash
   + durability + F-1 + the A20k-tail/FAT wins + the settled consume story, accept that
   C1000 and O19 are not yet met single-node, and carry the C1000 propose lever + R-121
   / R-123 as G2 deliverables. The one-VM co-resident-loader caveat (O13) applies to
   every number.
2. **R-123 (HIGH — SIGBUS on store reopen).** 1/100 dropped-writes rounds the broker
   crashes on the torn `MDB_NOSYNC` `store/data.mdb` mmap during apply-replay and
   does not reopen (the reproduced WP-0.3 D-01/D-03). The log is truth and intact;
   the fix is to validate/repair or discard-and-rebuild `data.mdb` from the log
   rather than mmap-read a store behind the durable point. Owner: the planner/store
   owner; needs an adversarial review + a deterministic reopen-after-torn-store
   regression. **Blocks G1, or a WP-1.2/§11.5 fix owed before G2?**
3. **R-121 (HIGH — unbounded push admission).** At a sustained 50k offered the node
   queues pushes without bound (10 s+ hangs, no 503, `planner_queue_depth=1024`)
   until achieved throughput hits 0 at ~min 55. §11.4 / D13's admission story needs
   a push-side backpressure that turns overload into a fast 503, not a hang.
   Advisory, or a G2 gate?
4. **Decisions the pipeline=4 evidence flags** — see *"Phase-1 evidence: decisions
   now in doubt"* below, now with numbers: **D4** (the pipeline of 4 is correct and
   lifts A50k p50 148→20 ms; after the performance package the A20k p99 tail is
   fixed — the remaining O14 miss is C1000, bounded upstream of the store), **D9 /
   R-105** (per-frame active-index RAM — ~387 B/msg at rest, bounded and reclaimable,
   but linear in stored count; note PERF-E's `DEDUP_INDEX=txns` retires the
   per-message `(pid,hash)` index, R-129), **§11.3 cadence**, and **O14** itself
   (reachable single-node, or only at raft3 / with an extra load-gen VM per O13?).
5. **The two client-visible parity gaps.** **R-115** (empty pop 200+body vs a
   bodiless 204) is a one-line WP-1.7 facade fix. **R-116** (`leaseReleased`
   attribution) needs Alice's call on the canonical item — the underlying state
   agrees.
6. **Operational / observability carry-ins.** **R-119** (F-2): ~304 open data-file
   fds → the phase-3+ deploy unit needs `LimitNOFILE`. **R-124** (retention/txns-
   purge is a phase-1 stub, so the dedup window frees no memory/disk in phase 1 —
   WP-2.7). **R-120** (O18 slow-command log staged, not wired). **R-122** (`/health`
   `storageReady:true` flips true before apply-replay finishes). **R-118**
   (apply-gate diagnosability) is fixed in `f8bfa672`.

---

## Decisions

All decisions below were **ratified by Alice on 2026-09-18** (see the note at
the top of this file for the two rounds and the round-two answers); the
"proposed" states in the table are the packet's state at the time it was
written and are kept as the record of what was put to her.

| id | decision (short) | state | change | date | reason |
|---|---|---|---|---|---|
| D1 | `QUEEN_STORAGE=postgres\|raft` per deployment, no per-queue mixing | proposed | — | 2026-09-17 | mixing classes created the two-store coupling behind pgless C17/C18/C20/C21 |
| D2 | one Raft group; voters = StatefulSet pods (3); embedded/single-node = 1 voter | proposed | — | 2026-09-17 | one log keeps every multi-structure transaction exact |
| D3 | entries carry effects, not commands; apply is deterministic | proposed | — | 2026-09-17 | followers never re-run planning |
| D4 | **bounded pipeline: `QUEEN_RAFT_PIPELINE` (4) entries in flight** | proposed (**amended**) | was "one entry in flight" | 2026-09-18 | S3: 1 in flight caps a cell at **307 entries/s of 64 KiB = 19 MiB/s**; 16 in flight reaches **974/s**. **4 was never measured** — see O23 |
| D5 | planner stamps time: `now = max(wall clock, last committed now + 1 µs)` | proposed | — | 2026-09-17 | apply has no clock |
| D6 | 16-byte request id, outcomes kept 600 s | proposed | — | 2026-09-17 | openraft GH#2095; also bounds S4's multiplexing blast radius (R-52) |
| D7 | nothing answers before commit + local apply on the leader | proposed | — | 2026-09-17 | pgless answered pops before their records were durable |
| D8 | positions are node-local; never in entries or digests | proposed | — | 2026-09-17 | S2 option (b) violates this as implemented (R-31) |
| D9 | store engine | proposed (**amended**, see below) | heed → **heed pinned + redb for raft1/embedded** | 2026-09-18 | S1 §4 after R-01..R-19 |
| D10 | dedup design | proposed (**amended**, see below) | option (a) → **option (a), lean encoding** | 2026-09-18 | S2 §5 after R-20..R-34 |
| D11 | consensus library | proposed (**amended**, see below) | pin unchanged; evidence and adapter changed | 2026-09-18 | S3 after R-35..R-47 |
| D12 | transport and authentication | proposed (**amended**, see below) | TLS-preference and pool size withdrawn | 2026-09-18 | S4 after R-48..R-64 |
| D13 | receivers hold up to `QUEEN_RAFT_HOLD_MS` (8000), then 503 `no_leader` | proposed (**flagged**) | — | 2026-09-18 | **does not cover GH#2080**: with a leader known but quorum-ack-less, the client write *hangs* (10 s, no answer). Needs S3 plan change 7 (a `propose` deadline) |
| D14 | heartbeat 100 ms, election 1000–2000 ms, pre-vote on, quorum check on | proposed (**flagged**) | — | 2026-09-18 | S3 measured failover **3087 / 3300 / 3353 ms** (detection = `election_timeout_max + rand(min,max)`; the election itself is ~5 ms). Either state 3–4 s in D14, or ask for 400/800 ms and pay for it in the follower's durable point |
| D15 | local stale reads for fetch/browse/stats/…; linearizable for KV and streams state | proposed (**flagged**) | — | 2026-09-18 | two constraints added: (i) a restarted node serves **no** local stale read until it has re-applied the `committed` it reopened with (S3 plan change 5); (ii) on heed a read begins and ends inside one blocking call and never holds a `RoTxn` across `.await` (S1 §4 item 1b) |
| D16 | stats/usage/retained bytes = O(1) counters at apply | proposed | — | 2026-09-17 | `log_refresh_all_stats_v1` was a production wall |
| D17 | metrics tables node-local | proposed | — | 2026-09-17 | — |
| D18 | traces replicated with a 7-day age limit | proposed | — | 2026-09-17 | — |
| D19 | no push spool in raft mode | proposed | — | 2026-09-17 | — |
| D20 | effect kinds gated by a replicated cluster version | proposed | — | 2026-09-17 | — |
| D21 | IDENTITY `{cluster_id, node_id, generation, disk_uuid}` | proposed (**flagged**) | — | 2026-09-18 | S3 proved the wiped-voter case is **silent** in a release build (the promised panic is a `debug_assert!`), and found the case IDENTITY does *not* close: a **reverted** disk (PVC from a volume snapshot) matches every field and brings back a stale vote store. Needs a fencing value (plan change 6) |
| D22 | postgres class stays as the oracle until GA | proposed | — | 2026-09-17 | also the mitigation for GH#2080 |
| D23 | branch `raft` in the main checkout; pgless parked `2bbd10d1` | proposed (amended 2026-09-17) | — | 2026-09-17 | other worktrees belong to other sessions |
| D24 | ephemeral queues stay outside Raft | proposed | — | 2026-09-17 | — |
| D25 | the proxy keeps its own Postgres; block the admin tenant-delete route | proposed | — | 2026-09-17 | — |
| D26 | Kafka facade transactions stay single-node-only | proposed | — | 2026-09-17 | — |

### D9 — amended proposal (store engine)

**heed 0.22.1 (LMDB) for the 3-voter broker, with its configuration pinned;
redb 2.6.3 as the documented alternative and as the engine for single-voter /
embedded (D2, raft1); §11.5 changed to repair rather than discard.**

Pins that go with heed, without which the numbers it was chosen on do not
apply: (a) `MDB_NOSYNC`, **not** `MDB_NOMETASYNC` — measured, NOMETASYNC costs
13% of the 20k rate (17 309 vs 20 000 msg/s), 1.8× the kernel writes (7.04×
vs 3.91×) and a store-commit **p99 of 123 ms** on the apply thread; (b) a
read-path rule — `rsm/store/` exposes a *read-transaction handle*, not
free-standing `get`/`scan`, because with thread-local reader slots a second
read txn on one thread is `MDB_BAD_RSLOT` (measured, the 2nd of 200) and the
`Send`-capable mode (`MDB_NOTLS`) **does not scale**: 2.06 / 1.97 / 2.08 M
gets/s at 1 / 4 / 8 threads against 2.10 / 8.18 / **9.96** M with TLS slots;
(c) `max_readers` ≥ the blocking pool, `MDB_READERS_FULL` a refusal, not a
panic; (d) a map-size rule in §11.8 (`MDB_MAP_FULL` on the apply path is a
node-local liveness cliff the leader cannot see).

Evidence. Mandatory criterion (WP-0.3): redb **15/15** kill + **5/5** flaky,
0 past-durable, and it passes *by construction* (`Durability::None` never
publishes a crash-visible root); heed 15/15 kill + **9/10** flaky, never past
its durable point after dropped writes but **1 run in 10 would not reopen at
all**, unexplained; fjall 15/15 kill but **1/5** flaky, reopening ahead of its
durable point in 14/15 kill and 4/5 flaky runs. In the ratified store shape
(§11.3 batched commits, no `segments` rows, a random-key `dedup` keyspace)
heed reaches 20 000 / 47 425 msg/s at the 20k/50k targets with WA 3.91/3.00×,
4 µs KV gets, 41.5 M rows/s ordered scan and a 524–556 MiB/s native export;
redb reaches 19 466 / 25 895 with WA 8.66/7.88×; fjall 20 000 / 49 019 with WA
1.33× but a 12.3 s stop-the-world `major_compact` in the soak and RSS 4 →
986 MiB still rising after ten minutes. (c) of Alice's 2026-09-17 list is
honoured: all three candidates are libraries; nothing home-grown.

What this proposal **cannot** claim yet: heed's "RAM bounded" and "ordered
reads win" pillars are unproven — the only soak is fjall's, and heed's scan
number is 180 001 rows over **9.6 MiB fully in page cache**. Both are demoted
to open until D-04.

### D10 — amended proposal (dedup)

**Option (a): a store index `(pid, hash) → occurrence list`, pruned by the txns
window, in the *lean encoding* — no `(created_at, pid, hash)` secondary index;
expiry through one sequential `txns` row per `Append`,
`(pid, base_offset) → [end][created][hashes]`, pruned by 006's rotating
per-partition walk. Bound the durable point per §11.4.**

Why (a) and not (b): `005_log_ack.sql` (`log_ack_by_hash_v1`) resolves each
hash to `eff = MIN(voff)` over `[max(committed+1, txns_start), batch_end]`
**and** `below = bool_or(voff <= committed)`, so the span is
`[txns_start, batch_end]` — the whole txns window — and the value must be the
occurrence list (5 591 of the 17.6 M rows in the a-heed cell already carry more
than one occurrence). Any "bounded exact ack index" is therefore option (a)'s
index, row for row; "(b) + an exact ack index" is (a) *plus* blooms, sidecars,
a file table, 16 B/message inside the frames and hash-only compaction.

Numbers (VM, 600 s cells, 360 s dedup / 432 s txns window, 50 000 offered):
probe p99 **0.024 ms** (a) vs **0.575 ms** (b) — 24× on the statistic WP-0.4
names; ack of ten hashes 0.479 ms (b) vs ~0.020 ms (a); ready to answer after a
restart 20 ms vs 212 ms. Lean encoding, measured on the VM: store per message
**171.4 → 119.0 B** (−31%), store ops per message **2.48 → 1.58** (−36%),
RSS **985 → 689 MiB** (−30%), rate, latency and exactness unchanged, kernel
bytes unchanged (+2% — the write amplification is the random `(pid, hash)` row
itself, not the secondary index). Exactness is PASS for both designs in both
directions including the retention case, and after a restart (0 wrong of 3967 /
4096 / 1793).

Cost to plan against: `rate × txns_window` rows per voter, i.e. at the product
default (`server/sql/schema.sql:66`, `dedup_window_seconds DEFAULT 3600`) and
50 000 msg/s, **180 M rows and ~10.6 GB of logical dedup data per voter** in
the lean encoding. **Nobody has run that window** (D-06). Option (b) is kept as
the fallback and would first need the D8/I7 split (its `txns` locator packs
`(bucket, file_id, offset, len)` — a position — into a replicated keyspace).

### D11 — amended proposal (consensus)

**openraft pinned to git rev `54094270ede0b8a2eb6ed6ae990edc6ca19d98ec`
(`0.10.0-alpha.34` + the GH#2095 `LogEntryDiscarded` fix) with
`raft-log 0.4.6`**, re-confirmed at G3. Features `["serde", "type-alias"]`;
`cargo +1.88 check` clean (C-3); MIT OR Apache-2.0. There is no 0.10 release to
pin instead (openraft #1637 open since 2026-01-01; crates.io has only alphas).

Two things about the evidence changed on 2026-09-18. **`raft-log` is now
qualified by crash runs, not by a conformance suite**: 10 × `kill -9` of a
follower under 64 KiB load with a restart each time, plus 10 rounds with
dropped unflushed writes (dm-flakey, injector self-tested) — **20/20 reopened
exactly at the index the leader had counted as matched**, every claimed index
readable, votes monotone, 0 acknowledged writes missing. The old claim ("it
passes `openraft::testing::log::Suite`, which is the §12.3 storage contract")
is **withdrawn**: that suite never reopens a store, never crashes a process and
never drops a write, and it passes against the adapter setting that never
flushes at all. **`save_committed` must be durable**: openraft's example (which
the spike copied) does not flush it, and with that setting a `kill -9` restart
brings a node back with applied state *behind* what it had already answered
(laptop: `WENT BACKWARDS on 1001: last_applied Some(301) -> Some(300)`). Cost
of the fix: **+2.4 ms on a 64 KiB commit p50 (+19%), no throughput change**.

Seven plan changes come with this pin: §9.4 coalesce read barriers into the
in-flight one and drop the fixed 2 ms window (measured 7–8× pessimisation:
0.16–0.23 ms vs 1.28–1.47 ms p50, max 58.6 ms); §12.5 the append/snapshot
deadline needs a floor of its own, never `soft_ttl()` from
`heartbeat_interval`; §12.7/§14.1 readiness is never `is_leader` and never
`current_leader`; D14 states 3–4 s failover; **(5)** durable `save_committed`
plus a readiness gate; **(6)** IDENTITY needs a monotonically increasing
fencing value (highest term seen) against a *reverted* disk; **(7)** `propose`
needs its own deadline, because a leader without a quorum-ack lease hangs the
write instead of refusing it (GH#2080 reproduced: 0 commits for the whole
fault, term never moves, client write unanswered after 10 s, recovery only by
healing the link (616 ms) or `kill -9` of the stuck leader (3528 ms)).

Fallback raft-rs: **20–27 agent-days** (4–5 calendar weeks) against 6–9 for an
openraft adapter; needs a build-time `protoc` (brushes C-2); crates.io still at
0.7.0 (2023).

### D12 — amended proposal (transport)

**Framed length-prefixed TCP on `QUEEN_RAFT_PORT` (6634) for forwarding and
Raft traffic, with a mutual HMAC handshake over `QUEEN_RAFT_SECRET`.
Confirmed.** Two of the first memo's four recommendations are withdrawn.

Kept. Leader CPU per message at 50 000 msg/s, VM, like-for-like pairs: plain
**1.26 vs 2.15 µs (0.59×)**, authenticated 2.47 vs 3.84 (0.64×), encrypted
1.54¹ vs 3.33 (**0.46×**; the first memo's 0.32× was wrong — it divided
framed+TLS by HTTPS+*MAC*). Plus 5% fewer bytes (294.2 vs 309.7 B/msg) on **2
sockets against 15–49**. Two dependencies of that number, now measured: at **8
framed sockets against HTTP's 15–49 the ratio is 0.88×**, and with a
jitter-free pacer framed CPU rises +26% and HTTP's 2%, moving the plain ratio
0.54× → 0.66×. HTTP/1.1 cannot be held to a socket budget: capped at 2 it
opened **218 695 connections in 60 s** at 3.85× the framed CPU.
¹ corrected by the 1.126 same-day framed control.

**Changed — authentication.** TLS is *no longer preferred* over the per-frame
MAC; they are alternatives, as D12 already says. The gap is 11× but 1.10 µs/msg
= **0.055 of one core at 50 000 msg/s** (0.7% of the box) and +57 µs of p50 on
a path S3 measured at ~3 ms. Whichever is chosen, two properties must hold and
the spike as measured had neither: the peer must be authenticated (the spike
used `with_no_client_auth()` and a shared DER, so a relay terminating both legs
passes the handshake through — fix: client certificates **or** RFC 5705
exporter material mixed into the transcript), and frames must not be replayable
(the measured MAC covered `type || body` only, one key both directions, no
counter — fixed in the spike: `dir || seq || len || type || body`,
per-direction keys, receiver accepts only its expected sequence, +0.4%).
**The TLS branch is not ratifiable while its trust anchor is undecided.**

**Changed — pool size.** "Two connections is the knee" is withdrawn: it was
+9 µs of loopback p50 at an in-flight depth of **0.31** commands, where D7
implies 15–35. One socket is CPU-optimal (0.63 vs 1.26 µs/msg). Keep §12.5's
separate pools, start at 2, size them in the transport WP under a commit delay
and mixed frame sizes.

**Not measured**: HTTP/2 over the same rustls — the one alternative that would
remove most of the framing, multiplexing, backpressure and reconnect code
(D-17).

### Phase-1 evidence: decisions now in doubt

The G0 ratifications hold, but phase 1 produced numbers that put four of them
in question. The pipeline=4 night pass (M7) now supplies the numbers the earlier
draft owed. None is a re-decision here — they are flagged for G1, with numbers.

- **D4 — the pipeline of 4.** Ratified on S3's 1-vs-16-in-flight numbers; **4
  itself was never measured** (O23). It is now: at 4 the batcher fix (`f8bfa672`)
  is correct under load (0 poison across ~200 M+ pushes), and it **transforms the
  A50k median** vs pipeline=1 (p50 148.5 → 19.6 ms, p99 1106 → 444). But the
  pipeline is where the O14 miss lives — the **p99 tail stays ~6× postgres** on the
  A-regimes and the **C1000 fan-out regresses**. D4's value of 4 is proven correct;
  its performance margin against O14 is the negative result. Levers before raising
  it (O23): fill the entry (`QUEEN_RAFT_ENTRY_MAX_BYTES` / batch caps) rather than
  deepen the pipeline.
- **D9 / R-105 — per-frame active-index RAM.** At pipeline=4 the at-rest resident
  is **~387 B/msg** (8.14 M store → ~3.0 GB) — **⅓ lower than pipeline=1's 610** —
  and NIGHT-B confirms it is **bounded and reclaimable**: a 50k run's RSS plateaued
  ~10–10.9 GB while the store grew to 51 GB on disk (file-backed mmap the kernel
  reclaims), `MemAvailable` never below 8.8 GB, no OOM. **But resident memory scales
  linearly with stored count, not flat** (the I8 verdict), and the dedup index climbs
  within every run (the 3600 s window retains every push). The 100 M / 1 M-partition
  size is infeasible on a 15 GB VM (~387 B/msg × 100 M ≈ 36 GB) — deferred to the
  three-VM O13 rig. The levers (smaller `QUEEN_RAFT_SEGMENT_BYTES`, a partial `.qidx`
  per durable point) are still unexercised.
- **§11.3 — the durable-point cadence default (4 ms / 256 entries).** The S2
  sweep on heed: 250 / 1000 / 4000 ms → **0.605 / 0.307 / 0.137 s of sync per
  wall second**, and §11.4's "cost proportional to what changed" is false on
  LMDB — 16× the interval costs 1.65× the time (R-27). NIGHT-B adds a datum: the
  ~515 ms p999 on the sustained dedup probe was **the batched store-commit cadence,
  not the lookup**. Still tuned inside WP-1.2 / WP-1.4, not validated against a
  steady-state store; by design a durable point fsyncs many files every second.
- **O14 — the targets themselves.** The raft1 comparison now exists (M7) and
  **FAILS**: push p50 ≤ pg on the A-regimes but **p99 ~6× pg**, C1000 fan-out
  **1.48× pg** on the cleanest fresh-store datum (the shared-broker 36× is a
  backlog-drain confound, not fan-out), FAT100 **≈31%** of the 300k knee (< 70%).
  Store footprint 0.9–3.2 GB queen RSS in one process vs postgres's ~0.1 GB queen
  + ~1.5 GB PG. The question for G1 is no longer "reachable?" but "reachable
  single-node, or only at raft3 / with a dedicated load-gen VM (O13)?"

---

## Work packages

### Phase 0 — Groundwork and spikes

Two campaigns ran unattended: the **S2 VM resume campaign (2026-09-18
05:40:31–05:54:44Z)** and the **S4 VM pass B / refutation cells** were queued by
the coordinator on the Linux VM while Alice's PC was off. Both left the VM
clean (no processes, no loop or dm devices, data dirs emptied).

| WP | state | commit | evidence |
|---|---|---|---|
| WP-0.1 Branch and record | **done** (uncommitted) | — | `git rev-parse HEAD` = `fc71b65b7e3564d8d4859504a51fe214f3d89970` on branch `raft`. `cargo build --release --bin queen` 4m30s warm, binary 9 871 072 B, 0 warnings. `cargo test --lib`: **649 passed / 0 failed / 7 ignored** (re-verified in a second pass, same counts). The 7 PG-gated tests on a throwaway `postgres:16-alpine` (port 5481, container removed): **7 passed**. Details in **M0**. Files: `RAFT_STATUS.md`, `PLAN_RAFT.md` (both untracked). |
| WP-0.2 Baselines on the VM | **done** | — | `bash /root/raft/wp02/measure-baseline.sh wp02b` (VM 164.90.215.224, 2026-09-17 13:15:38–13:19:46Z), plus a 4-point fat-batch calibration ladder. Six regimes, **0 error lines**, `shed=0` everywhere. **M1**. Files: `test/raft/vm/baseline/RESULTS.md`, `test/raft/vm/measure-baseline.sh`, `test/raft/vm/baseline/{A20k,A50k,B1,C1000,D1,FAT100}.log`, `cpu-rss.csv`, `cpu-summary.txt`, `marks.csv`, `summary.txt`, `fat-calibration/`, `run1-repeat/`. |
| WP-0.3 ⚠ Spike S1: store engine | **partial** — memo exists, **PASS/FAIL criterion not met at the mandated sample size** | — | `./run-matrix.sh`, `sudo ./vm-campaign.sh short` (the `full` profile is the deferred one), `./kill-loop.sh` (45 runs in all), `sudo ./flaky.sh` (20 runs in all: 5 redb / 5 fjall / 10 heed), `./vm-refute.sh` (2026-09-18, ratified shape + NOMETASYNC + reader modes). 9-cell matrix ×3 engines at 20k/50k/100k, 90 s cells; 40 s cells in the refutation round; one 10-min fjall soak. **M2**. WP-0.3 mandates **100 kill runs per engine plus an equivalent dropped-write sample**; 15 and 5–10 were run, and the kill half cannot falsify. Files: `test/raft/spikes/s1-store/{MEMO.md,RESULTS-vm.md,RESULTS-laptop.md,RESULTS-refutation.md,DEPS.md,README.md,src/**,results/**,results-vm/**,*.sh}`. |
| WP-0.4 ⚠ Spike S2: dedup | **partial** — memo exists, **the 1 h window WP-0.4 names was never run** | — | `./run-laptop.sh`, `./s2-vm-resume.sh` (VM, 2026-09-18 05:40–05:54Z, queued by the coordinator), `./refutation-vm.sh`, `./refutation-laptop.sh`, `./repro-samples.sh`. Four 600 s VM cells at 50k offered + five 120 s refutation cells + laptop cells. Exactness PASS both directions and after restart. **M3**. Longest window 360 s against the product default 3600 s; no cell held 50 000 msg/s under a real ack load for a full run. Files: `test/raft/spikes/s2-dedup/{MEMO.md,RESULTS-vm.md,RESULTS-laptop.md,README.md,src/**,results/**,*.sh}`. |
| WP-0.5 ⚠ Spike S3: consensus | **partial** — memo + exact pin, **3 deliverables short** | — | `./run.sh vm-today` (the budgeted profile: 7 scenarios, 3 nodes over real TCP, 1 GiB snapshot, 3 kill repetitions; **11 min 41 s** of VM wall clock, node logs 14:54:40.571Z→15:06:21.985Z), `./run.sh vm-refute` (2026-09-18: scenarios 8 and 9, the `save_committed` sweep, 6 min), `sudo ./flaky-log.sh` (10 dm-flakey rounds). **M4**. Short of the WP: the manifest snapshot ran at **1 GiB** not 10 GiB; no kill during a snapshot build or install; **a pipeline of exactly 4 was never measured**. Files: `test/raft/spikes/s3-consensus/{MEMO.md,RESULTS-vm.md,RESULTS-laptop.md,RAFTRS.md,README.md,src/**,results/{vm-2026-09-17,vm-2026-09-18-refute,laptop-*}/**,run.sh,flaky-log.sh}`. |
| WP-0.6 Spike S4: transport | **done** (its own deferred list is long) | — | `./run-vm.sh` two independent 60 s passes × 15 configurations (`results/vm/`, `results/vm-b/`; pass B queued by the coordinator while Alice's PC was off), `./run-vm-refute.sh` (2026-09-18: `results/vm-c/` HTTPS-plain + HTTP-capped-pool, `results/vm-pace/` jitter-free pacer, `s4 bench`), `python3 table.py / pairs.py / refute.py`. **8 659 953 commands forwarded, `cmds_bad = 0` in all 38 rows**, achieved = offered to 0.005%. `cargo test --release`: **13 tests green**. **M5**. Files: `test/raft/spikes/s4-transport/{MEMO.md,RESULTS-vm.md,RESULTS-laptop.md,README.md,src/**,results/**,*.sh,*.py}`. |
| WP-0.7 Harness skeletons | **done** (five skeletons; the parts that need a raft broker refuse to run) | — | `GOWORK=off go test -count=1 ./...` in `difffuzz` (**10 tests**), `checker` (**9**), `flatness` (**17**) — all `ok`; `python3 -m pytest test/raft/crash test/raft/kill -q` → **39 passed in 0.05 s**. 75 tests in total. Catalogues: difffuzz push/pop/ack live + 13 declared stubs; checker 2 checks live + 7 stubs, three verdicts (PASS/FAIL/**SKIP with a reason**), `-strict` turns SKIP into failure; crash 24 points / 5 scenarios; kill 10 scenarios with a stratified scheduler; flatness RESULTS format + comparator + preload CLI. Exit codes uniform: 0 clean, 1 judged-thing failed, 2 could not run. Files: `test/raft/README.md`, `test/raft/{difffuzz,checker,crash,kill,flatness}/**`. |

### Phase 1 — single node, message path

| WP | state | commit | evidence |
|---|---|---|---|
| WP-1.1 Entry and effect codec | **done** | this commit | `cargo test -p queen-engine --lib rsm::` → **44 passed / 0 failed** (0.16 s); whole lib `cargo test -p queen-engine --lib` → **693 passed / 0 failed / 7 ignored** (1.84 s) against the M0 baseline of 649/7 — **+44, no regression**. (The implementing agent's own note said 27; the tree at commit time holds 44 — `tests/columns.rs` and `tests/gates.rs` were added after that note. 44 is the verified count.) `cargo build` (bin+lib, server feature) and `cargo build --no-default-features` (embedded) both **0 warnings**; `cargo +1.88 check --lib` **green** with the new deps — note a 1.88 toolchain **is** installed on this host, contrary to M0's claim. `cargo clippy --all-targets`: **0 warnings under `src/rsm`** (two `manual_is_multiple_of` fixed). `rustfmt --edition 2021` on the new files only; the crate's red `cargo fmt` untouched. **Exists**: `src/rsm/mod.rs` (the §3.4 map as empty inline modules, each with its owning WP, so a later WP flips `pub mod store {}` to `pub mod store;` in one line), `effect.rs` (all **32** §5.2 kinds, permanent `u16` ids, hand-rolled Writer/Reader in the `native/record.rs` style, `u32` length prefixes, **no serde**), `entry.rs` (Entry/CommandRecord/Outcome of §5.1/§5.4, framed `len\|xxh3(body)\|body`, the format tag inside the checksummed body). **Proven**: round-trip per kind, per outcome and whole-entry; **33 golden fixtures** (32 kinds + one entry, **3 853 B** total) compared both ways and failing on any byte change; kind ids pinned by hand; `effect_count >= 1`, span bounds, overlap and `kinds_version` consistency refused; every single-bit flip in a 700-byte entry caught; every truncation reported Truncated/Header; unknown kind/version/format → `CodecError::fatal()` (**I16**: the codec refuses to step over an effect it cannot read even though the length prefix says how long it is); a field-transposition test giving every column of every row a distinct value (the one defect golden bytes cannot catch, since they come from the encoder under test); ~**72 000** hostile buffers × 4 decoder entry points — no panic, and a lying `u32` count allocates nothing (capped reserve). **Deps** for the whole phase: `heed 0.22` (`default-features=false`) + `memmap2 0.9`, resolving to heed 0.22.1 / lmdb-master-sys 0.2.6, byte-identical to s1-store's lock; `xxhash-rust` kept; **redb/fjall not added** (D9 is heed everywhere); openraft/raft-log left to WP-3.1. `mod rsm` wired into **both** `lib.rs` and `main.rs` (the repo's twin-list rule), `#[allow(dead_code)]` in the binary until WP-1.7's seam. **Invariants**: I16 honored at the codec (stopping the node is WP-1.4/3.3's); I18's bases are in the header, apply's assert is WP-1.4's; **D8/I7 kept** — no file id, offset or length appears anywhere in the catalogue (the `bucket` §5.2 puts in `Append` is a hash of names, not a position). I1/I2 not yet applicable: no state is touched. **Not proven**, carried as R-101..R-104. Code: 3 237 lines of product code (effect.rs 2 085, entry.rs 961, mod.rs 191), 2 731 lines of tests. Files: `server/src/rsm/{mod.rs,effect.rs,entry.rs}`, `server/src/rsm/tests/{mod.rs,samples.rs,roundtrip.rs,golden.rs,columns.rs,gates.rs,fuzz.rs}`, `server/src/rsm/tests/golden/*.bin` (33), `server/{Cargo.toml,Cargo.lock}`, `server/src/{lib.rs,main.rs}`. |
| WP-1.3 Segment files and per-file index | **done** | this commit | **Exists**: `server/src/rsm/segments/` — `frame.rs` (the §11.2 frame), `index.rs` (the immutable `.qidx` plus the active file's RAM index), `mod.rs` (`Segments` writer, cloneable `Reader`, recovery, durable points, GC/pins). **2 591** lines of product code (mod.rs 1 810, index.rs 504, frame.rs 277), **1 940** of tests over 8 files, 37 public functions; `rsm/mod.rs` touched on exactly one line (`pub mod segments;`). No new dependency — heed and memmap2 came in with WP-1.1. **Proven** by 53 tests (51 run, 2 ignored = the kill child and a measurement), all green: append/roll/seal/read; every single-bit flip and every truncation of a frame caught; `locate` exact over 2 000 records / 40 partitions and across 8 files with two partitions interleaved; a `.qidx` rebuilt by scanning is **byte-equal** to the one the seal wrote (the rebuild-by-scan path O22 owes); GC never unlinks a pinned, snapshot-referenced or still-live file; a real `kill -9` of a child process around a roll, **8/8** repeat runs. **Recovery trusts the store, not the disk**: files truncated to the recorded lengths, unknown files and orphan `.qidx` swept, and `ShortFile` / `MissingFile` / `Damaged` surfaced through `SegError::is_disagreement()` — the **I11** case §11.5 answers by repair or snapshot, never papered over; recovery is idempotent (asserted). **Hazard closed that the plan does not name**: a roll moves a bucket's frames out of the active RAM index before the caller's next store commit writes `partition_files`, so for up to `QUEEN_RAFT_STORE_COMMIT_MS` a reader found the frame in neither place — a pop payload read failing for a committed message. `Shared::sealed_recent` serves that file's index until `forget_sealed()` or the next durable point; tested. **Invariants**: I1 (writer is `&mut self`, apply-thread owned), I2 (no clock/rand/env on any apply path; `Options::from_env` is boot-only), I7/D8 (no position leaves the node; nothing here reads or writes an entry), I8 (a durable point fsyncs only what changed; `locate` is O(log n) with no per-partition RAM — one residual, R-105), I10 (two-phase GC), I11 (above), I15 (every call blocks and says so; no `.await` anywhere in the module). **Not proven**: durability under dropped unflushed writes — `kill -9` does not drop the page cache, so this WP falsifies the bookkeeping (recorded lengths, the seal, the `.qidx`, which file is active) and not unsynced bytes; that is finding **R-02** exactly and the run is the VM's (§13.6, WP-1.11), carried as R-106. **Not implemented, by scope**: compaction (§11.7, WP-2.9); snapshot hard-linking (§11.6, WP-4.6 — `seal_all()` and `set_snapshot_ref()` are its hooks); crash-point hooks (WP-1.8 owes `seg.rolled` and `seg.qidx_written` in `rsm/faults.rs` — the mid-roll kill here is by timing, not by a fault point), carried as R-107. **Not defined here on purpose**: `bucket_of(tenant, queue, partition)` — §0.4 writes the separator as "␟" and the agent refused to guess between U+241F and 0x1F; the planner (WP-1.5) owns it and `Effect::Append` already carries the bucket. **API for WP-1.4**: `open(root, opts, &[FileState]) -> (Segments, Recovery)`; `append -> Position`; `roll` / `seal_all`; `durable_point() -> DurablePoint{files, files_synced, dirs_synced}`; `take_touched()` for every store commit; `release(pos, Retained|Window|Both)`; `gc_candidates()` then `unlink()`; `reader() -> Reader` with `read` / `read_blob` / `locate` / `read_at` / `pin`. **Numbers**: `cargo test -p queen-engine --lib rsm::segments` → **51 passed / 0 failed / 2 ignored** (3.9 s); whole lib `cargo test --lib` → **804 passed / 0 failed / 10 ignored** (3.8 s) against M0's 649/7 and WP-1.1's 693/7 — taken while WP-1.2's concurrent tree in this same checkout was green, so the whole-lib figure moves as WP-1.2 commits; `rsm::segments::tests::crash` run 8 times consecutively **8/8 ok**. `cargo build` (lib+bin) and `cargo build --no-default-features` (embedded) both clean; `cargo +1.88 check --lib` green (MSRV 1.88); `cargo clippy --all-targets` → **0 warnings** under `src/rsm/segments`; `rustfmt --edition 2021 --check` clean on all 10 files (the crate's own red `cargo fmt` untouched). Files: `server/src/rsm/segments/{frame.rs,index.rs,mod.rs}`, `server/src/rsm/segments/tests/{mod.rs,frames.rs,files.rs,indexes.rs,recovery.rs,gc.rs,crash.rs,measure.rs}`, `server/src/rsm/mod.rs` (one line). |
| WP-1.2 Store adapter and message-path keyspaces | **done** | this commit | **Exists**: `server/src/rsm/store/` — a `Store`/`Reads`/`Writes` seam (scoped read closure; the write handle is owned by apply, with `commit()` / `durable_commit()` / `abort()`; range scans both directions; a bounded `delete_range` with a resume key) over a **heed** adapter carrying the four **D9 pins**, plus **20 keyspaces**: the §6.1 message path and `request_ids` / `request_expiry` / `garbage` / `counters` / `meta` (`next_pid`, `kv_version_next`, `last_now_us`, `max_created_at_us`, applied index/term, durable index) and the node-local `seg_loc` / `files` of §6.2 — order-preserving keys, row codecs, typed accessors (`TypedReads`/`TypedWrites`). `server/src/rsm/dedup.rs`: **D10 option (a) LEAN** — `(pid,hash)` → occurrence list plus one `txns` row per Append — with probe (`003`), resolve (`005` eff+below), record, a rotating bounded prune and a chunked partition delete. `server/src/rsm/state/`: `Derived` (ready rings rebuilt from `pending`, visibility heap, lease deadlines from `leases_by_worker`) ported from `6e96e228:native/state.rs`, and a read-only `Committed` view (garbage pids hidden, `plan_now` per D5/I5). **No plan-time mutation anywhere** — the planner walks the ring without consuming it, so hazard **D-1** is not carried over; no per-bucket locks. **Proven** by **61 new tests**, green inside the whole lib suite, `clippy` clean and `rustfmt --edition 2021` clean for these files: CRUD and range scans per keyspace (numeric not lexicographic order, prefixes, limits, reverse); read isolation (a reader sees the last committed txn, never the open one; the writer sees its own writes); abort; clean reopen; **pin 2** (a second read txn on one thread refused by construction, and it survives a panic); **pin 3** (`MDB_READERS_FULL` a retryable refusal + metric); **pin 4** (`MDB_MAP_FULL` typed, fatal, counted, on a 1 MiB map); key-too-long refusal; dedup exactness including hash lists outliving the segments retention deleted; ring and lease rebuild. **Crash**: `kill -9` of a child mid-write-transaction, **10/10** runs — reopened at `applied=8` with `durable=4`, i.e. *past* the durable point as `MDB_NOSYNC` plus an intact page cache allows (§11.5 step 2); rows and applied index agreed exactly every time and the uncommitted transaction was never visible. D9 is not contradicted. **Not proven**: power loss / dropped unflushed writes (dm-flakey, Linux VM — WP-1.8, and R-106's shape); all numbers are macOS **debug** smoke, not VM numbers; nothing consumes the store yet (apply WP-1.4, planner WP-1.5). **I15 gap, stated not hidden**: a store call carries no deadline — LMDB has no API for one (a get is a page fault, a durable commit an fsync). What *is* bounded is the queueing: one writer (the apply thread), so `write()` never waits behind another write txn and a read never waits. Documented in the module header; the deadline lives in WP-1.4's blocking task. Two accepted findings are carried as **R-108** and **R-109**. Also added `Outcome::encode`/`decode` to `rsm/entry.rs` (public framing, byte-identical to the framing inside an entry) for the `request_ids` row — no format change, golden fixtures untouched; `rsm/mod.rs` declares `store`, `state`, `dedup` (WP-1.3 owns the `segments` line, untouched). **Numbers**: `cargo test -p queen-engine --lib` → **840 passed / 0 failed / 10 ignored** (5.13 s) against WP-1.3's 804/10 — **+36 net at commit time**, no regression; `rsm::tests::store` and `rsm::tests::store_crash` green, the crash test 10/10. Measurement (`--ignored`, `rsm::tests::store::measure`) is a **macOS debug** smoke in the §11.3 shape (dedup + txns row per append, partition row, cursor, pending, seg_loc, file lengths) over 10 000 entries × 8 messages = **80 000 messages**; §0.3 says quotable numbers come from the Linux VM, so it is not quoted here. Files: `server/src/rsm/store/{mod.rs,heed_store.rs,keys.rs,rows.rs,typed.rs}`, `server/src/rsm/state/mod.rs`, `server/src/rsm/dedup.rs`, `server/src/rsm/tests/{store.rs,store_crash.rs,mod.rs}`, `server/src/rsm/entry.rs`, `server/src/rsm/mod.rs`. |
| WP-1.4 Apply thread, durable points, recovery and file GC | **done** | this commit | **Exists**: `server/src/rsm/apply.rs` — `Applier`, the **sole writer** of the store and the segment files (it holds the store by reference so the write handle stays open across entries), plus the std-thread driver (`channel`/`run`/`spawn`, bounded `sync_channel`, strict index order), a `Notify` seam (waiters keyed by index → `&[CommandRecord]`, wakes, durable index; non-blocking by contract), an injected `Clock` (`SystemClock`/`ManualClock`), `ApplyConfig`, recovery (§11.5 steps 2–5), durable points (§11.4), two-phase file GC (§11.7) and `state_digest`. **2 365** lines of product code, **2 346** of tests. **ONE** function applies every message-path kind: Queue/Group upsert+delete, PartitionCreate/Delete, Append (segment append + `seg_loc` + dedup occurrences + one `txns` row + `pending` per subscribed group + counters + `partition_files` written **at SEAL, not per append**), CursorSet/Delete (+`leases_by_worker`, `pending`, ring, wake on lease release), DlqInsert/Delete, Watermark (payload released first, hash lists after — **D10**), GarbageAdd/DeleteChunk (7 staged sweeps, resume key in STATE not in the effect), RequestIdsExpire, ClusterVersionSet, MembershipNote (one meta key per node), Noop. Phase-2 kinds answer a typed fatal `Unsupported` — **I16**, never a skip. **Proven** by 20 tests in `tests/apply.rs` and 2 in `tests/apply_crash.rs`, all green: **I2** — two fresh states with different `RandomState` seeds and different durable cadences reach **byte-equal** digests of every replicated keyspace in key order (the seeds are asserted to differ, so the test cannot pass vacuously); idempotence — in-process replay across the applied index, and a real `kill -9` of a child mid-apply then replay from the durable index reproduces the uninterrupted digest, **6 kill delays per run**; counters recomputed from the rows (pushed from the gapless tail, retained bytes from live `seg_loc`, completed/pending per group, DLQ from the rows); a pin blocks the unlink until dropped while its neighbours go; **I18** bases asserted against meta and advanced by the count; **I5** backwards time refused; a hole in the log refused; a failed durable point (injected fsync EIO) reports **no** durable index and is fatal; a damaged frame below a recorded length refuses to start (**I11**) instead of truncating; the thread flushes on channel close. **I2 is ENFORCED, not reviewed**: `server/clippy.toml` bans `SystemTime::now`, `Instant::now`, `std::env::var*`, `rand::{random,thread_rng}`; `[lints.clippy] disallowed_methods = "allow"` in `server/Cargo.toml` switches it off for the rest of the package (196 legitimate reads + every integration test) and `rsm/apply.rs`, `rsm/state/`, `rsm/store/` re-`deny` it. Verified **both ways** — the commit agent re-ran `test/raft/lint/deny-bites.sh`: **7/7** methods refused inside `rsm/apply.rs`, file restored clean. Two documented `#[allow]`s: `SystemClock::now` (cadence) and `ApplyConfig::from_env` (boot-only). **Idempotence is one guard**: an entry at or below `meta.applied_index` is skipped, because the applied index is written in the **same transaction** as everything the entry changed (**I11**) — that is what makes `replay_after = durable_index` safe, and it is the phase-1 shape of §11.5's single-voter repair. **Not satisfied, stated not hidden**: §11.5 step 3's repair for a real disagreement (a file **shorter** than the store's record, i.e. dropped unflushed writes) needs a snapshot to restore and there is none before WP-4.6, so phase 1 **refuses to start** with `ApplyError::Disagreement` (**R-110**). Durability itself is untested here — `kill -9` keeps the page cache, so this falsifies the bookkeeping, not unsynced bytes (R-02/R-106 shape, VM run owed to WP-1.8/1.11; **R-111**). **Cross-WP changes**: `store::rows::FileRow` now carries the full liveness (`frames`, `retained_frames`, `retained_bytes`, `window_frames`) because WP-1.3's `Segments::open` **refuses** a recorded sealed file with no frames — WP-1.2's four-field row would have made every sealed file dead after a restart (**R-113**); `dedup::push_occurrence` / `check_occurrences` made public (Watermark expires **by offset**, not by time); `rsm/mod.rs` `pub mod apply {}` → `pub mod apply;` (one line). **Scoped decisions for later WPs**: `Counter::Pending` is maintained at **GROUP scope only** (partition/queue-level "pending" is not well defined across groups) and retention does not reduce it, so it is pushed-minus-completed and **not** `PartitionRow::pending_from`; WP-2.6 owns the mapping of every read (**R-112**). `REQUEST_EXPIRE_LIMIT` is a **constant, not a knob**, because it changes what `request_ids` holds. **Numbers** (verified by the commit agent, macOS debug): `cargo test -p queen-engine --lib` → **878 passed / 0 failed / 10 ignored** (11.73 s) against WP-1.2's 840/10 — **+38, no regression** (the implementing agent's note said 862; the tree at commit time holds 878). `cargo clippy --all-targets` → **0 warnings under `src/rsm`** (the crate's pre-existing warnings elsewhere untouched); `rustfmt --edition 2021 --check` clean on all 11 touched files; `Cargo.lock` unchanged — **no new dependency**. Files: `server/src/rsm/apply.rs`, `server/src/rsm/tests/{apply.rs,apply_crash.rs,mod.rs,store.rs}`, `server/clippy.toml`, `server/Cargo.toml`, `server/src/rsm/mod.rs`, `server/src/rsm/dedup.rs`, `server/src/rsm/state/mod.rs`, `server/src/rsm/store/{mod.rs,rows.rs,typed.rs}`, `server/src/rsm/segments/mod.rs`, `server/src/rsm/segments/tests/{crash.rs,gc.rs,recovery.rs}`. **Left uncommitted on purpose**: `test/raft/lint/deny-bites.sh` and its paragraph in `test/raft/README.md` are outside this step's declared WP paths, so the commit agent reported them and left them alone. |
| WP-1.7a Storage seam: `QUEEN_STORAGE=raft`, the facade trait, boot without Postgres, route classes | **done** | this commit | **Exists**: (1) `config.rs` — `StorageMode` + `QUEEN_STORAGE` (**fatal on typo**, not a silent default) + `raft_dir` / `raft_ready_lag_ms` / `raft_planner_queue_depth`; (2) `rsm/facade.rs` — the `Rsm` `#[async_trait]` (push, pop pinned/wildcard/discovery, ack, renew, dlq_head, has_pending, depth, health, notifier), typed `RsmError` (Unsupported/Retry/NoLeader/NameTooLong/StorageFull/Timeout/Internal), `ReqCtx`/`Deadline`, the `NotReady` stub, the `set_builder`/`build` hook, and `check_message_key_names` (**R-108**, the 511-byte LMDB bound = MAX_STORE_KEY 511 − KEY_OVERHEAD 64 = **NAME_BUDGET 447**); (3) `handlers/raft.rs` — `dispatch_*` helpers, `RsmError`→HTTP, raft `/health` (+`raft` block) / `/metrics/prometheus` / `stats/refresh` (no-op), the un-ported-route 503 fallback, shared `build_raft_state` + `build_raft_router`; (4) **7** storage-aware guards on the message-path handlers, the postgres path byte-identical; (5) `main.rs` `run_raft` (no pool/schema/§10.3 loops — all listed in comments) and `embedded/boot.rs` raft branch (data dir required, no Postgres); (6) `admission::AdmissionCfg::for_raft_planner` sized by planner queue depth. **Proven** (commit agent re-ran): `cargo test -p queen-engine --lib handlers::raft::` → **5 passed / 0 failed** (910 filtered), the seam tests — push/pop → 503 `raft_phase1_unsupported`, over-long queue → 413 `name_too_long`, `/health` → 200 with `raft` block `{role:single, storageReady:false}`, un-ported route 503 / static route not. Implementing agent's binary smoke (QUEEN_STORAGE=raft, **no Postgres**, :6699): push/pop/ack/transaction/kv/streams → 503; stats/refresh, /metrics → 200; ephemeral RAM reachable; no panic. `rustfmt --edition 2021 --check` clean on `facade.rs` + `handlers/raft.rs`; the crate's red `cargo fmt` untouched; the I2 `disallowed_methods` deny in apply/state/store not touched. **Not proven / owed to WP-1.7c**: the real facade + full §9.1 pre-work (fusion pack / encryption / repack) + long-poll parking (marked TODO); embedded op dispatch to the RSM is WP-2.10; the lazy Postgres pool handle still lives in `AppState`, never dialled — making it `Optional` is a WP-2.x follow-up (**stated, not hidden**). **Cross-WP**: `facade.rs` uses `#[async_trait]`, the dep WP-1.6 already added at `server/Cargo.toml:80` (no dep added here); `rsm/mod.rs` is shared — only the `pub mod facade;` line was staged (WP-1.5's planner and WP-1.6's replicator stub-flips left uncommitted for their owners). Blockers: none. Files: `server/src/rsm/facade.rs`, `server/src/handlers/raft.rs`, `server/src/rsm/mod.rs` (one line), `server/src/config.rs`, `server/src/handlers/mod.rs`, `server/src/handlers/data.rs`, `server/src/main.rs`, `server/src/embedded/boot.rs`, `server/src/admission.rs`. |
| WP-1.6a Replicator seam + LocalReplicator | **done** | this commit | **Exists**: the §12.1 seam verbatim — `Replicator` `#[async_trait]` (`propose`/`role`/`watch_role`/`read_barrier`/`applied_index`/`transfer_leadership`/`membership`/`change_membership`/`metrics`), `StateMachine` (`apply`/`applied`/`durable_point`/`build_snapshot`/`install_snapshot`), `ProposeError`, plus `Role`/`AppliedAt`/`Membership`/`MembershipChange`/`ReplError`/`ReplMetrics`/`NodeId`. `LocalReplicator` wires its own WAL to WP-1.4's apply thread: log frame `len\|xxh3\|index\|term=1\|bytes`, group commit + one fsync per group, rolling files, torn-tail truncation, refusal of a corrupt sealed file, drop behind durable points. A writer std-thread hands entries to apply in order; a `Notify` resolves propose waiters after **LOCAL apply (I4)**, advances applied/durable and truncates the log; `open()` replays entries after the store's durable index (§11.5). `FakeReplicator` gives WP-1.6b commit/delay/fail/NotLeader/Timeout-then-late-commit. **Invariants**: I3 (a `Timeout` keeps the entry in flight — tested), the §7.1 error semantics, I15 (`propose` never blocks a tokio worker; std threads do the fsync; no std `Mutex` across an `.await`). **HONEST DEVIATION**: `LocalReplicator` drives the apply thread via `apply::channel`+`Notify`, not the `StateMachine` trait — that trait is defined here as WP-3.3's seam; `SnapshotHandle`/`StagedSnapshot` are WP-4.6 placeholders. **Fixed en route**: a shutdown deadlock (the notifier must hold no writer handle) and a recovery gap the crash test surfaced (a kill during `roll()` leaves a sub-header log file; `open()` now drops it). `open()` is a **blocking boot call** — WP-1.7 should call it off the tokio worker path. **Not proven**: durability under dropped unflushed writes (`kill -9` keeps the page cache = R-02/R-106 shape; VM run owed to WP-1.8/1.11, carried as **R-114**); all numbers are macOS laptop smoke (§0.3). **Numbers** (commit agent re-ran, macOS debug): `cargo test -p queen-engine --lib rsm::tests::replicator` → **15 passed / 0 failed / 1 ignored** (3.60 s; the ignored one is the throughput smoke); crash test `replicator_crash::a_killed_node_reopens_and_replays_every_acknowledged_entry` green (implementing agent: 4/4 over repeated runs, 6 kill delays 35–270 ms/run, each round reopened, replayed and matched `run_workload`'s digest). Throughput smoke (local log, Fsync::Data, single-entry groups, laptop): 5000 groups in 21.7 s = **230 entries/s**, per-group incl. fsync p50 4010 µs / p99 12104 µs — not quoted, §0.3 says the numbers come from the VM (WP-1.11). `rustfmt --edition 2021 --check` clean on all 6 files; the crate's red `cargo fmt` untouched. **Dependency delta**: `async-trait = "0.1"` added as a direct dep (one line; already in the lock at 0.1.91 transitively, and the committed WP-1.7a `facade.rs` already names `#[async_trait]` — this makes the tree name it as a direct dep). **Shared-file rule honored**: `rsm/mod.rs` and `tests/mod.rs` staged **partially** — only the `pub mod replicator;` flip and the two `mod replicator*;` lines; WP-1.5's planner hunks (`pub mod planner;`, `mod planner_*;`) and the `planner/` tree left uncommitted for their owner. Blockers: none. Files: `server/src/rsm/replicator/{mod.rs,log.rs,local.rs,fake.rs}`, `server/src/rsm/tests/{replicator.rs,replicator_crash.rs,mod.rs}`, `server/src/rsm/mod.rs`, `server/{Cargo.toml,Cargo.lock}`. |
| WP-1.6b The batcher pipeline (§7.1 cycle driver) | **done** | this commit | **Exists**: `server/src/rsm/batcher.rs` — `Batcher<S,R>` is one tokio task (`spawn`) that drains the command channel to the caps + the O17 plan budget; rebuilds the `Overlay` from committed bases plus every in-flight entry whose index is above the committed `applied_index` (the drop gate that respects the ≤4 ms store-commit lag, so entries applied-but-not-committed stay folded); marks the cycle start (**I18** bases); looks each request id up (§5.4/**I6**); plans on the blocking pool (**I15**: no store call on a tokio worker, no std `Mutex` across an `.await`); builds one `Entry`; `encode_entry` is the fallible step and refuses its waiters; proposes through the `Replicator` seam keeping up to `pipeline(4)` in flight; answers each command only after commit + local-apply (**I4**). Seam for WP-1.7: `Command`, `Reply`, `Submission`, `CommandTx`, `BatcherConfig::{default,from_env}`. §7.1 error semantics exact: `NotLeader`/`OutcomeUnknown` drop the whole overlay + `Retry` + pause; `Timeout` keeps the entry in flight and holds the pipeline until it applies (**I3**); `Refused`/`Fatal` stop the driver. §10.1 request-id expiry proposed on cadence. Flipped the `rsm/mod.rs` stub (`pub mod batcher {}` → `pub mod batcher;`) and added the one `mod batcher;` line to `tests/mod.rs` — the four-file change the WP rules allow; no sibling `pub mod` staged. **Proven** (commit agent re-ran): `cargo test -p queen-engine --lib rsm::tests::batcher` → **8 passed / 0 failed / 1 ignored** (0.16 s) — delayed commit, fatal failure, `NotLeader(hint)`, `Timeout`-then-late-commit (offset 1 not 0, **I3**), step-down with 4 in flight (log applies to gapless offset-3), request-id replay across cycles (proposes nothing, **I6**), the expiry step, and e2e push/pop/ack over `LocalReplicator` + real apply with restart whose recovered digest matches; the ignored one is the laptop throughput smoke (§0.3). `rustfmt --edition 2021 --check` clean on both files, clippy **0 warnings** for both, MSRV `+1.88 check` + `--no-default-features` (embedded) clean, the I2 `deny-bites` harness still bites **7/7**; the crate's own red `cargo fmt` untouched. **Not proven / stated**: throughput is macOS laptop smoke only (§0.3) — 1-in-flight 5061 entries/s p50 102 µs vs 4-in-flight 5761 entries/s p50 77 µs, quotable numbers are the VM's. **Not fully satisfied (stated)**: **I13** — the batcher resets its planning base on a leadership regain, but the full "wait for the term's first entry to apply before planning" gate is the openraft adapter's (phase 3); on the single-node `LocalReplicator` leadership never changes, so it is vacuously satisfied here. **Not done, by scope**: the §11.8 disk/map-full refusal gate WP-1.5 flagged as "the batcher's" needs `store.map_usage` — left as a follow-up; the §7.1 `BATCH_HOLD_MS` accumulate-2ms optimization is not implemented (the driver drains what is queued each cycle; commands batch naturally under load); the drop-gate index prediction for timed-out entries relies on single-leader contiguous indexing (phase 1, documented). **Numbers**: `rsm::tests::batcher` 8/0/1; `cargo test --lib rsm::` = **302 / 0 / 5**; whole lib = **956 / 0 / 12** (was 949/0/11 at WP-1.5, +7 pass +1 ignore). Product code `batcher.rs` 1064 lines (incl. docs), tests 588. Blockers: none. Files: `server/src/rsm/batcher.rs`, `server/src/rsm/tests/batcher.rs`, `server/src/rsm/mod.rs` (one line), `server/src/rsm/tests/mod.rs` (one line). |
| WP-1.5 The planner for push, pop, ack, renew and DLQ head | **done** | this commit | **Exists**: `server/src/rsm/planner/` — a `Planner` over a committed view plus an `Overlay` (the chain of in-flight entries' effects, folded, tracking pid/kv bases for **I18**); pure functions ported from `6e96e228:native/semantics.rs` with **no `stage()`** and no plan-time mutation (hazard **D-1** not carried, **I1** by construction). API the batcher (WP-1.6b) codes against: `plan_push`, `plan_pop_pinned`/`wildcard`/`discover`, `plan_ack`, `plan_ack_positional`, `plan_nack`, `plan_renew`, `plan_dlq_head`, `lookup_request_id` (D6/I6), `PlanConfig` (O17 budget, O18 slow-command), `CommandKind`, plus `bucket_of` (the U+241F/0x1F choice WP-1.3 deferred → **0x1F**). **Proven** by **51 tests**: push (dedup store+overlay, gapless offsets, monotone created_at, implicit queue/partition, all-dup not logged), pop (all/new/timestamp seeding, budget walk + head probe, conflation, autoack, window_buffer, delayed_processing, attempt tracking, empty seal, wildcard FIFO/max_parts, first-contact enumeration, discover), ack (implicit ack, explicit-signal-never-skipped, retry budget only-by-failed, forced-DLQ inline per O7 from the receiver snapshot per O20, below-cursor noop/stale, O16 fast path via the cursor `delivered` set, conflated ack), renew (leases_by_worker key order), nack, dlq_head, overlay-equivalence (N-in-one-cycle == one-per-cycle, offsets/cursors), request-id replay. **Refutation fixed** (I12/§8 row 004, pop.rs + its test only): `plan_pop_pinned` gated the conflating-pinned registrar on the GROUP being unregistered, a strictly weaker condition than SQL 004's `IF v_from_ts IS NULL AND v_conflate` nested inside the outer first-contact guard `NOT EXISTS(cursor for the pinned partition+group)`; after a plain pinned pop had seeded that partition's cursor, a conflating pinned pop would wrongly fire the registrar — manufacturing a durable conflating group and a queue-wide bulk seed the oracle never writes, so a later wildcard would read conflation=TRUE and drop every partition's backlog. **Fix**: the registrar now fires only when `cmd.conflate && cursor(pid, group).is_none()` (group unregistered AND pinned partition first contact); else the plain path serves from the existing cursor and the group stays unregistered — exactly what 004 does when the outer NOT EXISTS is false. New test `a_conflating_pinned_pop_whose_partition_is_already_seeded_does_not_register_or_bulk_seed`. **Not satisfied, stated**: seeding is by the subscription INSTANT (registered_at_us / subscription_timestamp_us vs each append's created_at) which D5's clock makes position-exact and the SQL itself does, so the (reg_index,reg_effect) apply records go unused under this formulation; the `AckResult` codec (WP-1.1) has no per-item error field, so a rejected ack target reports via stale_hashes, and exact ack fast/slow-path counter parity is the dual-backend conformance's to confirm (**R-101**); the §11.8 disk/map 507 gate needs `map_usage` the pure planner doesn't hold (batcher's, WP-1.6b); the pop segment gather is O(segments) (pgless-equal, **R-105**), a bound near `wanted` is a follow-up; **I3/I13** (pipeline of 4, first-entry-of-term) are the batcher's, the Overlay provides the mechanism. **Numbers** (commit agent re-ran, macOS debug): `cargo test -p queen-engine --lib rsm::tests::planner` → **51 passed / 0 failed** (6.94 s); whole lib `cargo test -p queen-engine --lib` → **949 passed / 0 failed / 11 ignored** (19.84 s) against WP-1.4's 878 (+planner +the concurrent replicator WP's tests), no regression; `cargo build --lib` and `--no-default-features` (embedded) both green; `cargo clippy --lib` → **0 warnings under `src/rsm/planner` and `src/rsm/tests/planner_*`**; `rustfmt --edition 2021 --check` clean on all 9 files. Product code ≈2735 lines (mod.rs 1263, ack 675, pop 629, push 168), tests ≈1493. `rsm/mod.rs` touched on exactly the one planner line (inline stub → `pub mod planner;`); `tests/mod.rs` got 5 additive `mod planner_*;` lines. Blockers: none. Files: `server/src/rsm/planner/{mod.rs,push.rs,pop.rs,ack.rs}`, `server/src/rsm/tests/{planner_harness.rs,planner_push.rs,planner_pop.rs,planner_ack.rs,planner_overlay.rs,mod.rs}`, `server/src/rsm/mod.rs` (one line). |
| WP-1.7 part 2 (WP-1.7c) The real facade, wired, smoke and recovery | **done** | this commit | **Exists**: `RaftFacade` (`rsm/facade/real.rs`) owns store + `LocalReplicator` (apply thread) + batcher + segment reader; `real_builder` registered via `set_builder` from `run_raft` and `embedded/boot.rs`, so the WP-1.7a seam's `build()` returns it (the `NotReady` stub stays the default when no builder is installed — the seam unit tests still see it). It is the §9.1 receiver: parses wire bodies, mints message ids, hashes txns (xxh3_128), packs one frame/msg (O20), submits `Command`s to the batcher, renders push/pop/ack/renew wire bodies; pop payloads are read off this node's own segment files after local apply (D7, §7.5). **Seam fixes owned here**: `apply::spawn_with_reader` publishes the segment `Reader`; `LocalReplicator` captures and exposes it (`reader()`); `RsmError::Rejected` (planner client refusal → 400) + `err_response` arm; `Deadline::instant()`. **Proven**: 5 raft-mode integration tests (`rsm::tests::facade`) green — push/pop/renew/ack render end to end; a retry with the same transactionId returns duplicate at the original offset; a restart recovers the pushed state; plus the two refutation regressions below. Real broker curl smoke (`QUEEN_STORAGE=raft`, no Postgres): push t1,t2 (offsets 0,1); push t1 again → duplicate@0; pop (queue mode) → both payloads off files; renew → renewed 1; ack t1 completed; ack t2 dlq → leaseReleased+dlq; `kill -9`; restart → recovered at applied index 5; push t3 → offset 2 (gapless); pop → t3. **I15/I4 kept**: planning and payload reads run on `spawn_blocking`, no std mutex across `.await`, answers only after commit + local apply. **Refutations fixed** (each with a regression test that FAILS on pre-fix code, verified by revert): (1) batch-ack DLQ mis-attribution — `render_ack` now computes dlq PER ITEM (`res.dlq > 0 && item.status ∈ {Dlq,Failed}`), `AckPerItem` carries the item status; test `a_mixed_batch_ack_flags_dlq_per_item_not_per_target` acks one completed + one dlq on one (partition,lease) target, asserts flags (false,true) + exactly one dead letter named `poison`. (2) content-less dead letter — `resolve_ack_targets` stamps `DlqSnapshot{txn = ack-wire transactionId}` on Dlq/Failed items so the filed row is identifiable; test `a_forced_dlq_ack_files_the_transaction_id` asserts the row carries `tx-poison`. **Design call**: did NOT widen `AckResult` with dlq_hashes — a format change (the `entry_message_path.bin` golden embeds `Outcome::Ack`) needing a catalogue version bump, i.e. WP-1.1 territory tracked as **R-101**; the fix lives receiver-side inside WP-1.7c files. **Residual** (documented at the fix, R-101): 2+ signal items on ONE target with `res.dlq==1` — only the head (lowest-offset) is filed, but the receiver holds no offsets in this `AckResult` shape; the per-item DLQ set the outcome must carry to disambiguate is R-101's shape refinement. **Not done (deferred, in the module header, none block phase 1)**: partitionId is the numeric pid not a uuid (no uuid→pid index yet); the push frame carries no producerSub/traceId/encryption; the forced-DLQ row is filed without the poison payload (needs the ack-registry offset resolve); `has_pending` is a stub (true) and `depth` answers `Unsupported` (WP-2.6 counter reads); long-poll parks only on queue-scoped pops. **Numbers** (commit agent re-ran, macOS debug §0.3): whole lib `cargo test -p queen-engine --lib` → **962 passed / 0 failed / 12 ignored** (23.2 s); `rsm::` suite → **308 / 0 / 5** (23.6 s); `rsm::tests::facade` → **5 / 0** (0.81 s, 3 render + 2 refutation regressions); WP-1.7a seam `handlers::raft::` → **5 / 0** (still see the stub). Clippy: clean for every file touched (55 warnings all pre-existing elsewhere, none in WP files); `rustfmt --edition 2021 --check` clean on the two new files (`facade/real.rs`, `tests/facade.rs`); the crate's red `cargo fmt` untouched. Smoke: recovery reopened at applied index 5 after `kill -9`, offsets gapless across the crash (t3 @ offset 2). Diff: **7 files +117/−4**, one `facade.rs`→`facade/mod.rs` rename, 2 new files. **Note**: rustfmt recursed through main.rs's module tree; every unrelated file was reverted to HEAD and the WP edits re-applied, so the diff is this WP only. Blockers: none. Files: `server/src/rsm/facade/{real.rs,mod.rs}`, `server/src/rsm/tests/{facade.rs,mod.rs}`, `server/src/rsm/apply.rs`, `server/src/rsm/replicator/local.rs`, `server/src/handlers/raft.rs`, `server/src/main.rs`, `server/src/embedded/boot.rs`. |
| WP-1.8 Crash points and the raft1 crash matrix | **done** | this commit | **Exists**: `rsm/faults.rs` — `QUEEN_TEST_FAULTS="point[:nth],…"`, off unless set (one relaxed atomic load on the hot path); an unknown/unwired point exits **2** (never a silent no-fire); a fired point dies by **SIGKILL-to-self** (the faithful `kill -9` model, dodging the macOS crash reporter that made `abort()` core-dump). The **13** §13.5 phase-1 points wired: `batcher.drained`/`planner.planned`/`propose.sent` (`batcher.rs`), `log.appended`/`log.flushed` (`replicator/log.rs`), `commit.before_apply` (`replicator/local.rs`), `apply.mid_entry`/`apply.segment_written`/`apply.store_committed`/`durable.files_synced`/`durable.store_committed`/`gc.before_unlink`/`gc.after_unlink` (`apply.rs`), plus R-107's `seg.rolled`/`seg.qidx_written` (`segments/mod.rs`); `init_from_env()` in `run_raft` and embedded boot. `test/raft/crash`: `runner.py` drives a real `raft1` broker over the HTTP wire (push with recorded ids+payload hashes, pop, ack, restart, retry every unanswered push with its original transactionId, drain), emits the shared checker's JSONL and checks it. **Proven** (raft1, push-ack, `QUEEN_RAFT_DURABLE_EVERY_MS=150`, debug binary §0.3): full matrix = 13 points × {nth=1, nth=2} = **26 cells → 22 PASS / 0 FAIL / 4 N/A**, `RESULTS.md` written. Every reachable cell recovered **exactly once** across the kill (answered pushes delivered; unanswered pushes at most once, retries exactly once — dedup survived; one txn→one offset; single healthy leader; no error line but the fault's own). The 4 N/A = `gc.before_unlink`/`gc.after_unlink` at nth={1,2} — a file dies only via retention/delete (WP-2.7), not HTTP-reachable in phase 1; those plus `seg.*` are proven at the Rust level by `each_fault_point_fires_and_the_node_repairs` (9 points fire by SIGKILL, node re-opens and replays to a **byte-equal digest**). Go delivery-at-least-once + payload-hash pass (SKIP not FAIL when the crash preceded any ack). **Numbers**: lib **967 passed / 0 failed / 12 ignored** (24 s, +5 vs WP-1.7c's 962); `rsm::` subset **311/0/5**; harness `pytest` **21 passed** + selftest ok; clippy **0 warnings** in touched files; `rustfmt --edition 2021` clean on the files written; I2 deny-bites **7/7**. **Not proven (owed to the VM, not a blocker)**: durability under dropped-unflushed writes / power loss — SIGKILL keeps the page cache, so this falsifies bookkeeping not unsynced bytes (§13.6 dm-flakey, WP-1.11). No rsm/ defect surfaced. Shared `rsm/mod.rs`: only the `pub mod faults;` line staged. Blockers: none. Files: `server/src/rsm/faults.rs`, `server/src/rsm/mod.rs` (one line), `server/src/rsm/batcher.rs`, `server/src/rsm/replicator/log.rs`, `server/src/rsm/replicator/local.rs`, `server/src/rsm/apply.rs`, `server/src/rsm/segments/mod.rs`, `server/src/rsm/tests/apply_crash.rs`, `server/src/main.rs`, `server/src/embedded/boot.rs`, `test/raft/crash/{runner.py,crashdrv.py,scenarios.py,points.py,test_crashdrv.py,RESULTS.md}`. |
| WP-1.9 The raft1 test topology and the RAFT PARITY gate | **done** | this commit | **Exists**: the `raft1` topology + RAFT PARITY gate, modelled on run.sh's TENANCY PARITY and pgless's NATIVE PARITY (`6e96e228`). (1) `test/compose/docker-compose.raft1.yml` — ONE broker on `QUEEN_STORAGE=raft`, **NO Postgres** (no `pg` service, no `QUEEN_PG_*`), data on the named volume `raftdata` (wiped by run.sh's `down -v`); tenancy OFF (no raft-tenanted lane in phase 1). (2) `test/run.sh` — `raft1` in `compose_for`, an **opt-in** `--topo raft1` gated by `RAFT_SUITES="js"` (a suite gets a raft1 job only if its runner honours `QUEEN_TEST_STORAGE=raft`, like pgless's opt-in `native`), a `raft1` report column, and a **RAFT PARITY** gate comparing single↔raft1 by **rc + FAILED count only** (never totals, which differ by design — the raft1 lane is a subset by skips); CI is unchanged because raft1 is opt-in. (3) the js runner raft lane (`test-v2/run.js`, `runners/js/entrypoint.sh`, `runner-unit/fatalExit.test.js`): when `QUEEN_TEST_STORAGE=raft` it **skips the Postgres setup/cleanup** and skips every un-ported-route test **LOUDLY**, each printed with a categorised reason; the served/broker-free tests stay green. **Proven** (commit agent re-ran, macOS + Docker, `queen:test` built this run `sha256:572f89ab…`, `queen-test-runner-js` `sha256:811e2740…`): `test/run.sh --suite js --topo raft1` → **PASS js/raft1 (3s)**, matrix **ALL GREEN**; the runner reports **keeping 12 served/broker-free tests, skipping 161** that need a route not in raft phase 1 (each printed), final **12/12 passed, 0/12 failed**, `rc=0`; broker booted on raft with no PG (`INFO rsm: raft facade open (WP-1.7c) dir=/var/lib/queen/raft applied=0`). RAFT PARITY verdict = `rc(single)==rc(raft1) AND failed(single)==failed(raft1)==0`. **Not proven in-session**: the full single↔raft1 gate print was NOT captured here — a sibling's `single` stack was live in this shared checkout, so running `--topo single,raft1` risked colliding with it; I ran raft1 alone (green) and left the single lane byte-identical (my `run.js` changes are inert when the lane is off). **Central finding (reported, NOT an rsm/ fix)**: phase-1 raft serves only push/pop/ack/renew with IMPLICIT queue creation; queue configure/create/delete answers **503** (`handlers/raft.rs raft_fallback`) and is required setup by ~all existing tests, so the JS message-path parity surface is exactly **one** test (`testPushAutoCreatesQueueAndPartition`) — it grows automatically when **WP-2.5** ports queue admin. This is WP-2.5 scope (admin), not rsm/, so it is reported not fixed; no new R-id (not a defect in WP-1.9's own code). **Numbers**: js/raft1 PASS 3s; kept 12 (1 message-path implicit-create + 11 broker-free `logger.*`), skipped 161 by route class (queue configure/create ~WP-2.5, streams ~WP-2.4, transaction ~WP-2.1, kv ~WP-2.2, timers ~WP-2.3, subscription/seek ~WP-2.5, docs/watermark/bootstrap/retention/maintenance = PG-read/seek); go/py lanes deferred (their runners must learn `QUEEN_TEST_STORAGE=raft` — `t.Skip`/conftest plumbing, `go` 12/15 & `py` 16/18 files call Create/Configure; add to `RAFT_SUITES` when done). **DID NOT touch** rsm/ or any sibling's files; staged only the 5 WP paths + this row. The `queen:test` image used includes sibling WP-1.8/1.10 uncommitted edits present in the shared checkout, but the Postgres path is byte-identical and CI rebuilds from the committed tree, so the gate is unaffected. Files: `test/compose/docker-compose.raft1.yml`, `test/run.sh`, `test/runners/js/entrypoint.sh`, `clients/client-js/test-v2/run.js`, `clients/client-js/test-v2/runner-unit/fatalExit.test.js`. Blockers: none to delivery; CI gating on RAFT PARITY (add `--topo single,raft1` to the js suite) left to the coordinator/§14.9 CI WP; go/py raft lanes deferred until their runners honour the storage flag. |
| WP-1.10 difffuzz v1 + checker v1 | **done** | this commit | **Exists**: `test/raft/difffuzz` v1 drives the whole phase-1 message path against a postgres-oracle broker and a `raft1` broker (`QUEEN_STORAGE=raft`, no PG): push + duplicates; pinned / wildcard / discovery pop, auto and manual ack; ack ok/failed/dlq **by hash and by position** (incl. the batch positional fast path); nack; renew; below-cursor re-ack; DLQ-head via dlq/nack. The **8** phase-2 kinds (txn, kv, timer, configure, seek, group_delete, dlq_move, dlq_purge) stay declared stubs — raft1 `503`s them. Normalization drops wall-clock/node-local values and compares uuid, ts, `partitionId`/`leaseId` by **identity relation** (`normalize.go` `VolatileKeys`/`IdKeys`); `compareAck`/`notePop` absorb the phase-1 envelope gaps at-site, each with a reason. raft1 serves **no** resource view, so `Preflight` asserts `503 raft_phase1_unsupported` and defers the final-view body compare to WP-2.6 (mode already switches when side B answers 200). Added per-side `checker` logs (`-logdir`, `log.go` format), `campaign.sh`, `fixtures/`, `PARITY-NOTES.md`. Touched **no broker source** and no sibling file; the `checker` source is unchanged (validated as-is). **Proven (re-run this session, macOS)**: `GOWORK=off go test -count=1 .` → **10/10 PASS** (0.19 s: generator determinism, normalization identity, comparison names first diff, mix parsing, honest sides do not diverge, changed field caught, flags refuse one-broker-twice / defaults replayable, list-ops covers every planned kind, report renders); `go run . -selftest` **ok**; `go vet .` clean; `gofmt -l *.go` empty. **Proven (impl agent, not re-run here — needs two live brokers)**: 200+ seeded 250-op runs (~50k message-path ops), **0** divergences, **0** unexpected-divergence fixtures after the 7 classified envelope gaps are absorbed; checker at-least-once + payload-hash validated over difffuzz logs on **both** brokers, `-strict`, **10/10** green (needed a settle + long-poll drain to defeat the window_buffer debounce; the same failures hit the postgres oracle first, proving harness-not-raft). **Classified in `PARITY-NOTES.md`** (each reproducible): empty-pop `204`-vs-`200` (facade parity, **R-115**); `partitionId` uuid-vs-pid (WP-1.7c deferral); ack noop/error/success shape (**R-101**); `leaseReleased` attribution (**R-116**); mixed batch-ack DLQ is an **oracle quirk** — postgres broadcasts dlq to completed siblings, raft's WP-1.7c per-item fix is CORRECT, so the oracle is NOT ground truth for that field. **Numbers**: 200+ clean seeds × 250 ops, 0 divergences; checker 10/10 strict (5 seeds × 2 brokers); 10 message-path ops implemented, 8 phase-2 kinds stubbed; seed pace ~12/min (debug broker + a concurrent crash harness SIGKILLing brokers, self-healed via retry). **Blockers**: seed target 2000 not reached (got 200+) — debug broker + a sibling crash-harness repeatedly `kill -9`ing the shared brokers; `campaign.sh` is self-healing and re-runnable on a release broker / uncontended host. Files: `test/raft/difffuzz/{ops,gen,config,run,compare,normalize,main,selftest,difffuzz_test,checkerlog}.go`, `campaign.sh`, `PARITY-NOTES.md`, `fixtures/README.md`, `test/raft/README.md`, `test/raft/.gitignore`. |
| WP-1.11 raft1 on the VM: regimes vs baseline, flatness, noisy neighbor | **partial** | this commit | **Ran on the Linux VM (164.90.215.224), release broker `rustc 1.98.1` md5 `be80d860b4a1` — byte-identical before/after; the one VM diagnostic edit was reverted; no broker source committed here.** Harnesses committed: `test/raft/vm/measure-raft1.sh` + `test/raft/vm/raft1/{RESULTS.md,flakechk.go,flaky-raft1.sh,flatness-raft1.sh,noisy-raft1.sh}`; re-verified on macOS this session (`bash -n` clean on all 4 shell drivers; `go vet ./flakechk.go` clean; RESULTS.md numbers cross-checked). **PROVEN / PASS — dropped-unflushed-writes durability (discharges R-106 / R-111 / R-114):** 20/20 dm-flakey rounds, every acknowledged push delivered after reopen, **0** error lines; plus an instrumented adversarial round (store reopened at `applied=0`, fsync'd log `replayed=38`, **3000/3000** acks recovered). **DONE:** the 6 regimes at `QUEEN_RAFT_PIPELINE=1` side-by-side with WP-0.2 postgres (push p50/p99 ms — A20k 6.18/175.1 vs 9.15/27.0; A50k 148.5/1105.9 vs 23.2/75.3; B1 3.73/68.1 vs 3.34/14.3; C1000 13.1/138.2 vs 6.62/33.0; D1 3.28/25.0 vs 1.99/4.08; FAT100 push-only ≈72k msg/s ≈ 24% of the 300k pg knee, sheds 12.5M). **⚠ BLOCKER F-1 (correctness, R-117):** at the ratified `QUEEN_RAFT_PIPELINE=4` the batcher stamps `now_us` monotone in **plan** order but `propose` (`batcher.rs:902`) `tokio::spawn`s each `repl.propose()` independently, so the LocalReplicator writer can assign the log index **out of plan order**; apply then sees `now_us` go backwards vs the index and refuses (**I5** `TimeWentBackwards`), the node poisons ("apply thread gone"), all later writes 503. Verified by reading the code + the exact error (`entry now_us …698930 below last applied …699010, index=672 applied=671`); reproduces **only under concurrency** (dies at applied≈33–670 concurrent, survives 400 sequential) — so the strictly-sequential difffuzz/WP-1.10 could not see it. The RESULTS.md notes the code fix has since landed in `batcher.rs` (a sibling's uncommitted tree — **not committed here, not part of this WP**). **F-2 (operational, R-119):** broker holds ~304 open data-file fds (256 buckets + log + store) + a socket per conn; at the default `ulimit -n 1024` it hits `EMFILE` under A50k → needs `LimitNOFILE` (used 262144). **NOT cleanly established (confounded by pipeline=1, deferred to a pipeline=4 re-run):** O14 performance verdict (**BLOCKED** by F-1), flatness I8 (one firm signal only: RSS scales with stored count — empty A20k 8→578 MB vs preloaded 4.4M 2685→3060 MB), noisy-neighbour O19 (quiet p99 121.3→284.7 ms, +134%, with a 2M at-rest backlog — FAIL but confounded); the full 300M/60-min flatness and a true active DLQ-storm generator also deferred. **Diagnosability gap (R-118):** apply-gate refusals bypass `poison()`'s log, so only "apply thread gone" surfaces. VM left clean (no procs/dm/loop/mounts; `/root/raft/wp111`). **Night re-run (2026-09-18/19, pipeline=4, F-1-fixed binary `cdc5bb59af27`):** the owed O14 / flatness / O19 pass ran (**M7**) — **O14 FAIL** (push p99 ~6× pg, C1000 fan-out 1.48×, FAT100 ≈31%), **I8 mixed/FAIL** (CPU+disk flat, RSS not flat ~387 B/msg, C1000 +546%), **O19 FAIL** (463/s DLQ storm → cold p99 +90.6%); dropped writes re-proven **99/100** at pipeline=4; **F-1 discharged on the VM** (0 poison, ~200 M+ pushes). New findings **R-120..R-124** (incl. ⚠ R-123 SIGBUS reopen, ⚠ R-121 push admission); `NIGHT-A/B/C-*.md` + `night/` raw added. Blockers cleared: **R-117 fixed (`f8bfa672`)**, R-118 fixed. Owed: R-123 + R-121 fixes, R-119 (`LimitNOFILE`). Files: the 6 paths above + `NIGHT-{A,B,C}-*.md`, `night/`. |

### Phase 1 — performance package (2026-09-19)

Alice's direction: fix the tail, the many-small-partitions shape (C1000) and
throughput first; dedup RAM, the noisy neighbour and the store hazard come
after. Every lever is switchable by an environment knob (default on) so the
pipeline=4 VM pass can ablate it. This table tracks the package; the baselines
to beat are **M7** (`NIGHT-A-pipeline4.md`).

| WP | state | knob | evidence |
|---|---|---|---|
| PERF-1 timing instrumentation of the RSM pipeline | **done** (this commit) | `QUEEN_RAFT_METRICS` (default on; `QUEEN_RAFT_TIMING_LOG_MS` default 10000; `QUEEN_RAFT_SLOW_COMMAND_MS`) | New `server/src/rsm/timing.rs`: a lock-free log2-bucket `Histogram` (p50/p90/p99/p999/max/count/sum, no external crate) + a process-global `queen_raft_*` registry. Instrumented every stage through the **injected** `apply::Clock` so `apply.rs` keeps no clock (**I2**): batcher (drain cmds/msgs, plan duration, `propose_roundtrip` arrival→proposed→committed→applied→answered), per-kind planner counters + the O18 slow-command log, the log writer (group entries/bytes, fsync), the apply thread (per-entry split segment `write_all` vs other, store-commit, durable-point split seg-fsync/dir-fsync, apply-channel depth) and the facade pop-read latency. Exposed via `/metrics/prometheus` as 24 `queen_raft_*` families (precomputed-quantile summaries + counters) and a 10s "rsm timing" tracing line. **Refutation fix**: `Submission.received_at` was a bare `Instant::now()` set per COMMAND unconditionally, so `QUEEN_RAFT_METRICS=0` still paid the arrival clock and understated the lever; now `Option<Instant>` populated by `timing::stamp()` in `Submission::new` and both requeue sites, `arrivals` uses `filter_map`, new test `received_at_follows_the_metrics_knob` pins `received_at.is_some()==timing::enabled()`. **Overhead** (release laptop microbench, full per-entry hot path, push-batch 10): ON 88.2 ns/entry vs OFF 7.1 ns/entry = net +81 ns/entry ≈ 0.16% of one core at 20k entries/s — 3 orders under the 1% budget. **Tests**: `cargo test -p queen-engine --lib rsm::` = **326 passed / 0 failed / 6 ignored** (was 325; +1); full lib (postgres mode) = **977 passed / 0 failed / 13 ignored**. Live exporter sample (macOS `F_FULLFSYNC`): `log_fsync` p50 ~8.4 ms (the tail driver), `propose_roundtrip` p50 ~8.4 ms, `apply_entry` p50 ~262 µs, `apply_segment` p50 ~131 µs, `plan` p50 ~131 µs. `rustfmt --edition 2021` + clippy clean for touched files (54 pre-existing warnings unchanged); **I2** disallowed_methods deny-gate green. VM helpers added: `test/raft/vm/profile.sh` (60s `perf record -g` → folded stacks + top-40) and `test/raft/vm/timing.sh` (`/metrics` scrape → CSV), syntax-checked, not yet run on the VM. Files: `server/src/rsm/timing.rs`, `rsm/mod.rs`, `rsm/batcher.rs`, `rsm/apply.rs`, `rsm/segments/mod.rs`, `rsm/replicator/log.rs`, `rsm/replicator/local.rs`, `rsm/facade/real.rs`, `handlers/raft.rs`, `rsm/tests/{facade,mod}.rs`, `test/raft/vm/{profile,timing}.sh`. |
| PERF-A the durable point off the apply thread | **partial** (this commit) | `QUEEN_RAFT_DURABLE_ASYNC` (default 1; 0 = today's fully-inline point, the crash contract) | **Async durable-point segment pre-flush.** A per-apply-thread helper `queen-rsm-preflush` pushes segment files' dirty pages to the device *between* durable points, spread over the interval (every `durable_every_bytes/16` clamped 1–8 MiB, or every `durable_every_ms/8` ms), so the point's segment-fsync leg on the apply thread finds them already flushed and returns fast. Pure page-cache warm-up: advances **no** bookkeeping, records no durable index, never touches the store; `durable_point()`, its crash points and **I11** are byte-identical, recovery unchanged, a crash between a pre-flush and the point behaves as if the pre-flush never ran (**I1/I2/I10/I11** intact). Best-effort (`try_send`, dropped if the helper is behind); the helper is joined before the store `Arc` drops. **Scope:** the store env-sync (LMDB `mdb_env_sync` on `data.mdb`) **stays** on the apply thread — PERF-2 shows it dominates the point for A/FAT; moving it off apply safely needs a D9 store-mode change (Alice gate) or fencing apply commits (stales planner reads), neither done (see **R-125**). This lever removes only the **segment-fsync** leg = the dominant term for the prioritized **C1000** shape (PERF-2: **186 ms** seg vs **94 ms** store). **Numbers — laptop SMOKE only** (macOS Data-mode fsync; per §0.3 macOS serializes `F_FULLFSYNC`, so this understates the VM Full-mode win): 12k-entry sustained-append run through the real apply thread, `durable_every_ms=50`, 3 reps stable. `durable_seg_fsync` (apply-thread segment-fsync leg): async **OFF** mean ~0.48–0.77 ms / max ~0.6–1.4 ms → async **ON** mean ~0.16–0.21 ms / max ~0.28–0.45 ms (**~60–72% cut**); `durable_point` total ~7.1–7.9 ms both (LMDB env-sync dominated on this shape, as PERF-2 predicted); `apply_entry` unchanged ~0.065–0.074 ms (no per-entry regression from the helper). Reproduce: `QUEEN_RAFT_METRICS=1 QUEEN_RAFT_DURABLE_ASYNC={0,1} cargo test -p queen-engine --lib rsm::tests::apply::measure_async_durable_point -- --ignored --nocapture`. **Tests:** three required — `the_async_durable_knob_leaves_the_same_state_and_reopens_clean` (state digest + clean reopen identical ON/OFF), `measure_async_durable_point` (the before/after probe), plus the `segments::tests::preflush` unit — `cargo test -p queen-engine --lib rsm::` = **340 passed / 0 failed / 8 ignored**. `rustfmt --edition 2021` + clippy clean on touched files; **I2** deny-gate green. VM ablation (Full mode; C1000/A/FAT at scale; knob 1 vs 0) is the coordinator's combined pass; the lever is knob-gated and ready. Files: `server/src/rsm/apply.rs`, `rsm/segments/mod.rs`, `rsm/segments/tests/preflush.rs`, `rsm/segments/tests/mod.rs`, `rsm/tests/apply.rs`. |
| PERF-B the dedup probe front | **done** (this commit) | `QUEEN_RAFT_DEDUP_FRONT` (default 1; 0 = byte-for-byte the baseline planner) + `QUEEN_RAFT_DEDUP_FRONT_MB` (default 512, the global byte cap) | **Planner-side dedup front.** A per-partition ring of generational register-blocked bloom filters (16 bits/hash, k=7, mirroring `server/src/dedup.rs`) that lets the planner **SKIP** the per-message committed LMDB probe on `rsm/planner/push.rs`. It answers only 'certainly absent → skip' or 'maybe → probe'; the LMDB index and the in-flight overlay stay the sole dedup authority, so the front is transparent (a `should_probe` false-positive costs a probe, never a wrong verdict). **Soundness:** a partition minted this front-lifetime is born seeded; a pre-existing one is lazily, boundedly seeded from committed txns UNION the overlay in-flight appends; the planner inserts every planned survivor at plan time (the front is a superset of the committed window); generations age out whole once below the window floor; the global byte cap drops a partition to always-probe under pressure; the batcher resets the front on leadership regain (inert on the single-node phase-1 `LocalReplicator`). **I1/I2/I4/I5** intact — the front is planner-only advisory state, never mutated by apply and never part of a verdict. **Numbers — laptop micro-measurement** (planning isolated via `plan_only`, no apply), front OFF→ON. **Probes/message:** A-shape (1 hot partition, warm 20k committed index) **1.000 → 0.002** (19969/20000 committed LMDB reads skipped); C-shape (256 partitions fan-out) **1.000 → 0.000** (25600/25600 skipped). **Raw per-op cost** (warm 100k index): committed `dedup::probe_one` **527 ns/probe** vs front `should_probe` **323 ns/op** — the front is cheaper even fully warm AND removes the LMDB read entirely (on the VM cold store that read pays the mmap page-ins PERF-2 (c) diagnosed). **Planning/cmd** on a fully-warm laptop: A-shape 161→176 µs/cmd, C-shape 7.6→8.0 µs/cmd = flat/slightly up (the removed warm-cache get is near-free and the front's insert+generation-growth runs inside the measured window); the tail/latency win is the VM cold-store pass to show. **Front bytes/message:** 15.3 B/msg at ~40k in-window hashes (dominated by the partially-filled top generation), converging toward the ~2 B/hash design at window scale; C-shape 256 parts = 2.1 MB total. **difffuzz** (front OFF vs ON on the real raft broker, the transparency property — oracle-vs-postgres not run to avoid co-opting the pg18-* containers): 50 seeds × 250 ops = 50 clean / 0 divergences / 0 broker errors. **Tests:** 7 new front tests incl. a load-bearing EXACTNESS property test `front_verdicts_match_baseline_across_window_and_restart` (front-on == front-off across in-window dup / out-of-window / restart+reseed); `cargo test -p queen-engine --lib rsm::` = **340 passed / 0 failed / 8 ignored**; whole lib (postgres mode) = **994 passed / 0 failed**. `rustfmt --edition 2021` + clippy clean on touched files; **I2** deny-gate green. VM ablation is driven entirely by the knob. **Scope note:** ~30 lines of additive glue in `rsm/batcher.rs` (owned by neither PERF-A nor PERF-B) hold the persistent `Arc<DedupFront>` and reset it on leadership regain — the only per-broker cross-cycle home; it does not overlap PERF-A's apply/segments/store. `/metrics` exposure deliberately deferred (`DedupFront::stats()` is the in-process source) to avoid colliding with PERF-A's durable-point metrics edits. Files: `server/src/rsm/dedup.rs`, `rsm/planner/mod.rs`, `rsm/planner/push.rs`, `rsm/batcher.rs`, `rsm/tests/planner_harness.rs`, `rsm/tests/planner_push.rs`. |
| PERF-D counters batched per commit, pending rows on transitions | **done** (this commit) | `QUEEN_RAFT_BATCH_COUNTERS` (default 1; also gates a per-txn group-list cache that removes the append path's `scan_groups`) + `QUEEN_RAFT_PENDING_TRANSITIONS` (default **0** — it changes the replicated pending value, hence the digest) | **Counter batching + pending-only-on-transition.** Shipped entirely in `apply.rs` (+ its tests); `state/` and `store/` untouched (the deterministic pending mirror proved unnecessary). **(1) CounterCache:** a per-transaction RAM overlay folds every counter bump (by sum) and stamp (by max) and writes each key **ONCE** at commit and durable point, folds pending deltas into apply's own counter reads, and is dropped on counter sweeps. Transparent (**I2**), digest unchanged; the group-list cache (invalidated on group create/delete, cleared at commit) removes the append path's `scan_groups`. **(2) Pending transitions:** the append writes a group's pending row only on a transition (no row yet, or a new earlier `ready_at`), decided from the committed row via `pending_at`, ring moved on the same transitions; default off because it changes the replicated pending value (earliest `ready_at`, not last) hence the 12.9 digest — proven cadence-independent and rebuild==live-rings. request-id row untouched (**D6**). **BUG FOUND+FIXED:** a net-zero counter delta (a +1/−1 cancel) must still write its 0-row; batching skipped it, giving a cadence-dependent digest divergence the `replicator_crash` kill-test surfaced under load — fix: flush writes every key the overlay holds. **I1/I2/I10/I11/I15** intact; digest unchanged with `QUEEN_RAFT_BATCH_COUNTERS`, `QUEEN_RAFT_PENDING_TRANSITIONS` cadence-independent + rebuild-equivalent. **Numbers — laptop in-process smoke**, store puts/append (off → counters → counters+pending): **A20k** (100 parts, batch 10, 1 grp) 29.78 → 21.97 → 20.98 (**−29.5%**), ns/append 57848 → 52045 (**~−10%**); **C1000** (1000 parts, batch 1, 1 grp) 21.00 → 15.02 → 14.12 (**−32.8%**), ns/append 31848 → 27250 (**~−14%**). Counter batching is the dominant lever (A20k −7.8, C1000 −6.0 puts/append); pending transitions add ~−1. Reproduce: `cargo test -p queen-engine --lib rsm::tests::apply::perf_d_store_ops_per_append -- --ignored --nocapture`. **Tests:** 4 new gate tests — `batched_counters_are_independent_of_the_commit_cadence`, `pending_transitions_are_independent_of_the_commit_cadence`, `pending_transitions_rebuild_from_pending_equals_the_live_rings`, `batched_counters_survive_a_crash_at_a_non_durable_commit`; `cargo test -p queen-engine --lib rsm::` = **346 passed / 0 failed / 9 ignored**, 5× green incl. the previously-flaky crash test under load. `rustfmt --edition 2021` + clippy clean on touched files; **I2** deny-gate green. Crash/durable covered locally by a new in-process crash cell + `apply_crash` + `replicator_crash` (all green); the `test/raft/crash` python harness and the VM were **not** run (laptop-scoped). **Scope note:** PERF-D removes only the counter+pending share (~7–8 puts/append); the '~4 puts/append' target needs PERF-E's dedup change (drop the per-message `(pid,hash)` index + the fat NBUCKETS fan-out) — the append call site `dedup::record(...)` is unchanged and untouched here. See **R-127** for the pending-transitions default-off decision owed to the coordinator. Files: `server/src/rsm/apply.rs`, `rsm/tests/apply.rs`. |
| PERF-C apply parallelism and buffered segment writes | **done** (this commit) | `QUEEN_RAFT_SEG_BUFFERED` (default 1; 0 = today's per-frame inline `write_all`) + `QUEEN_RAFT_APPLY_WRITERS` (default `min(4, cores/2)`; 0 = inline write on the apply thread) | **Buffered segment writes + optional per-bucket writer pool.** Per `execute`, an entry's frames are buffered per file and each touched file's run is written with **ONE** `write` at end of execute; records are published only **after** their bytes reach the fd (write-before-publish). Positions/bytes/recorded lengths are **byte-identical** to inline (**I2**); the flush runs before `notify.applied` (**I4**) and before any store commit records the new lengths (**I11**); roll flushes the bucket before sealing, `durable_point` flushes defensively, recovery unchanged. The optional pool moves **bytes only** (raw `write` on the `O_APPEND` fd) to per-bucket writers; store ops stay on the apply thread in effect order (**I1/I2**); writers are joined before every roll/durable/drop so no write is ever in flight (**I15**). **Numbers — laptop SMOKE only** (Darwin, 10 cores, `F_FULLFSYNC`, 60s goload smokes, 0 broker errors in all 9 runs; release md5 `d68f744bfa4d`). Write-syscall coalescing (writes/append = `apply_segment` count) inline→buffered→pool: **A20k 1.00→0.80→0.84; C1000 1.00→0.73→0.73; FAT100 1.01→0.78→0.81** (20–27% fewer segment `write()` syscalls; C1000, the many-small-partition shape, benefits most). `apply_entry` p50: buffering flat-or-lower (**C1000 131→65 µs** — fewer syscalls = faster apply); the pool moves write time off apply (**C1000 apply_other 131→33 µs**) but adds per-entry channel+join overhead (**A20k apply_entry 65→524 µs**) → **net negative on the laptop** where page-cache writes are cheap. push p50: buffered better (**C1000 27.8→23.2 ms −17%**, A20k 16.8→15.9 ms); FAT100 tail better (**p99 9.76 s→7.44 s, p999 11.6 s→7.57 s**). `strace` not on Darwin; `apply_segment` count IS the isolated segment `write()` count, corroborated by the unit test (4 appends → 3 writes). Buffering is the clear win; the pool's per-entry join cost is not repaid on the laptop, so the **VM ext4 ablation of `QUEEN_RAFT_APPLY_WRITERS` 0 vs 4 (esp. C1000 fan-out) is owed** and is the coordinator's separate VM pass. **Tests:** 2 new (pool coalesce+readback+reopen; buffered half-entry); crash matrix runs with buffering ON; `cargo test -p queen-engine --lib rsm::` = **342 passed / 0 failed / 8 ignored**. `rustfmt --edition 2021` + clippy clean on touched files; **I2** deny-gate green. Diff: 3 files, +709/−17. Files: `server/src/rsm/segments/mod.rs`, `rsm/apply.rs`, `rsm/tests/apply.rs`. |
| PERF-F the bucket count as a knob, one write and one fsync per entry | **done** (this commit) | `QUEEN_RAFT_BUCKETS` (power-of-two 1..=256, default **16**; pinned in a per-tree `seg/MANIFEST` at creation, refused on mismatch at open — a data dir keeps its count for life, a legacy manifest-less tree is adopted as 256) + `QUEEN_RAFT_APPLY_WRITERS` (0 = inline; adaptive: an entry flushes inline when it touches <=2 files OR carries <64 KiB, else the pool) | **Bucket count as a runtime knob; one write + one fsync per entry at buckets=1.** NBUCKETS (was the 256 constant in `rsm/segments`) is now `QUEEN_RAFT_BUCKETS`. The planner's `bucket_of` (0..256) is **UNTOUCHED** (PERF-E owns the planner); the logical bucket is **folded** into the local count on both the write path (`append`) and the read path (`Shared::locate`/`pin`/`has_file`). The fold is exact because every legal count divides 256 (`(h%256)%n == h%n`) and idempotent on already-local buckets, so `nbuckets` is purely node-local — never in an effect or digest (**I1/I2** intact). The durable-point contract and recovery are unchanged (**I10/I11**); manifest pins the count so a reopen can never reinterpret existing files. With `buckets=1` every entry is one file => one `write` and the durable point one `fsync`. **BUG FOUND+FIXED:** the initial write-only fold left the read path out-of-bounds (caught by the facade tests); write and read now fold consistently. Module doc gained a bucket-count section incl. retention/compaction implications of few buckets. **Numbers — laptop fan-out smoke** (fsync files at the durable point ; `write()`s per entry), buckets 256/16/1: **C1000** 251/16/1 files ; 5.0/5.0/1.0 writes (the seg-fsync-dominated fan-out killer collapses to a single fsync); **A20k** 100/16/1 ; 3.0/3.0/1.0; **FAT100** 100/16/1 ; 12.0/11.7/1.0. **Tests:** 6 new bucket tests (open/append/read/roll/seal/.qidx/recover/GC at 1/16/256, manifest mismatch refused, legacy tree adopted as 256, clamp+fold exactness) + apply-crash cells for `apply.segment_written` and `durable.files_synced` at buckets 1 and 16 + a laptop fan-out smoke; `cargo test -p queen-engine --lib rsm::` = **361 passed / 0 failed / 11 ignored** (segments 75 incl. 6 new buckets; apply unit 41; apply_crash incl. the buckets-1/16 cell). `rustfmt --edition 2021` + clippy clean on touched files; **I2** deny-gate green. `strace -c` and the `queen_raft_apply_segment_seconds` histogram over the 60s goload A20k/C1000/FAT100 smokes are the VM pass (strace is Linux-only); segment_writes + durable files_synced are the exact laptop proxy. `apply.rs` product code untouched. **Scope note:** `rsm/tests/replicator_crash.rs` took a one-line mechanical `nbuckets` field add forced by the Options struct change. See **R-128**. Files: `server/src/rsm/segments/mod.rs`, `rsm/segments/tests/buckets.rs`, `rsm/segments/tests/measure.rs`, `rsm/segments/tests/mod.rs`, `rsm/tests/apply.rs`, `rsm/tests/apply_crash.rs`, `rsm/tests/replicator_crash.rs`. |
| PERF-E dedup authority in the txns rows, bloom generations in front | **done** (this commit) | `QUEEN_RAFT_DEDUP_INDEX` (`txns`\|`rows`, default **txns**) — must be stable per store and identical across nodes (a txns-written store has no `Dedup` rows; a later rows-mode boot would miss dups) | **Dedup authority relocated into the sequential txns rows + PERF-B's bloom generations.** Under `txns`, apply's `dedup::record` writes **ONLY** the sequential txns row (the append's hash list, sequential key) — the per-message `(pid,hash)` random read-modify-write index is **gone**; the txns rows plus PERF-B's bloom (now time+offset-sliced generations) are the sole committed authority. A bloom `maybe` names a generation's `(min_base,max_off)` band; the planner scans just those txns rows and returns the min in-window offset; an unseeded/fallback/front-off partition scans the whole txns window **exact** (warms on first touch); the 005 ack-by-hash path resolves by the same range scan over `[txns_start,batch_end]`. `record` keeps its fixed `apply.rs` signature — it dispatches on a boot-resolved global set ONLY at the production `real_builder` seam (facade/apply unit tests keep the `rows` default, so no shared-global race); the planner reads the mode per-instance via `PlanConfig.index_mode` from `BatcherConfig::from_env`. Overlay/in-flight dedup unchanged. **I1/I2/I4/I5/I8/I10/I11/I15** intact — the txns rows are already replicated effects, the bloom is planner-only advisory, apply stays deterministic in what it commits. **Numbers — laptop isolated dedup micro-measurement** (release, A20k shape 50k msgs / 5000 appends / batch 10, `rsm::tests::dedup_txns::measure_probe_cost --ignored`): store dedup **ops/msg 2.10 (rows) → 0.10 (txns) = 21× fewer** (`Dedup` rows written 0 vs 50000; Txns 5000 both); probe **WARM** front-bounded p50 **917 ns** / p99 **87.7 µs**; probe **COLD** whole-window (restart/unseeded) p50 **140 µs** / p99 **167 µs** (linear in the txns window, warms after first touch); filter **11.96 B/hash** resident (transient over-provisioned top generation, amortizes toward ~2 B/hash at scale under the 512 MB global cap); **49.9%** of probes Skipped (bloom only, no scan). `difffuzz_txns_vs_rows_50_seeds`: **0 divergences** (probe+resolve, warm+cold fronts, with prune). Debug-build same run: warm p50 5.3 µs / p99 1.99 ms, cold p50 3.4 ms. **Tests:** new `rsm/tests/dedup_txns.rs` — 50-seed diff-fuzz rows-vs-txns + exactness cases (multi-occurrence, in/out-of-window, ack-below-cursor, retention-deleted-segment, restart-cold-filter, mid-append generation boundary) all pass; `cargo test -p queen-engine --lib rsm::` = **361 passed / 0 failed / 11 ignored** (32 s). `rustfmt --edition 2021` + clippy clean on touched files; **I2** deny-gate green. VM ablation of `QUEEN_RAFT_DEDUP_INDEX` txns-vs-rows is the coordinator's combined-binary pass (needs PERF-F's NBUCKETS segments + the release broker). Old `Dedup` rows from a prior rows-mode store are left in place under txns (never written or read) — a reclaim sweep is owed. See **R-129**. Files: `server/src/rsm/dedup.rs`, `rsm/planner/mod.rs`, `rsm/planner/push.rs`, `rsm/planner/ack.rs`, `rsm/batcher.rs`, `rsm/facade/real.rs`, `rsm/tests/dedup_txns.rs`, `rsm/tests/mod.rs`, `rsm/tests/planner_push.rs`. |
| PERF-H the ready ring re-arms leased and slow partitions; pending rows correct | **done** (this commit) | `QUEEN_RAFT_PENDING_TRANSITIONS` (default **0** — unchanged this round; the flip to on is a single coordinated commit, see R-131) | **Ring re-arm + `has_pending`, round 3 direction (2).** Key finding: the planner **REBUILDS** `Derived` from the committed `pending` rows every plan cycle (`batcher.rs`), so lease-expiry / visibility re-arm is **already automatic for the planner**; the apply thread's LIVE ring is read by nothing in prod yet (`has_pending` is a stub → `Ok(true)`). **AB-0 was right: there is no consume regression to fix from this lever** — PERF-H is a correctness/audit task. Landed, all behind the knob (default unchanged): (1) an event→ring audit table in `state/mod.rs` doc; (2) correct append maintenance on the transitions path — `ready_at = min(stored, created+delay)` floored at a live lease's expiry, so it never under-arms a delayed/leased queue and arms an appended-under-lease partition for **after** the lease (the OFF path overwrote = under-arm); (3) wired the previously-unwired live-ring promotion — a global O(1)-guarded ring-deadline index on `Derived` + promote at each entry boundary (D5 stamp), re-arming on lease expiry / `window_buffer` / `delayed_processing`; (4) `Committed::has_claimable_pending` + `Derived::ring_has_ready` for the §9.5 gate (facade wiring is a later WP, not my file). **I1/I2/I4/I5/I15** intact — the live ring is node-local derived state, never an effect or digest; the rebuild==live-rings property holds at every entry boundary. Default decision: ON is proven correct-enough (never under-arms = claimability-equivalent to SQL, rebuild==live at every entry boundary, cadence-independent, crash-safe) but **left OFF this round** for one non-correctness reason — ON moves the replicated `pending` digest (earliest vs last `ready_at`), so the shared `cfg()`/`run_workload` references used by the planner/batcher/replicator suites must flip in the **same** commit or `replicator_crash` diverges (see R-131). **Numbers — laptop A20k 60s** (release, isolated fresh brokers, `-rate 20000 -push-batch 10 -partitions 100 -consumers 32 -pop-batch 200 -manual-ack`; healthy runs pushErr=0 popErr=0 durable_points_failed=0): OFF popped 873,730 (~14.6k/s) empty 51,878 push p99 79.4 ms ackAvg 24.8; OFF#2 popped 609,210 (~10.2k/s) empty 103,379 p99 54.0 ackAvg 15.6; ON popped 896,790 (~14.9k/s) empty 74,530 p99 55.0 ackAvg 19.4. Pop rate is run-to-run-variable (OFF 10.2–14.6k/s, ON 14.9k/s): the knob moves nothing outside the noise → reproduces AB-0, no consume regression from ON. (Back-to-back contended runs OS-killed both OFF and ON brokers at ~25 s under the parallel agents' load — empty broker log = OS kill, not the code; the isolated 60 s runs are the valid ones.) **Tests:** +4 state (deadline index, promote==tick over 300 random ops, lease_expiry, gate) +6 apply (leased-append arm, OFF-overwrite pin, delayed-visibility, lease-expiry re-arm, rebuild==live property over 250 random steps, 8-partition slow-consumer sim); `cargo test -p queen-engine --lib rsm::` = **373 passed / 0 failed / 11 ignored** (59.9 s). `rustfmt --edition 2021` + clippy clean on touched files; **I2** deny-gate green. `has_pending` (`facade/real.rs`) is still a stub returning `Ok(true)` — the correct read is exposed at the state layer (`Committed::has_claimable_pending`), facade wiring is a later WP. See **R-131**. Files: `server/src/rsm/state/mod.rs`, `rsm/apply.rs`, `rsm/tests/apply.rs`. |
| PERF-G batcher and writer scheduling, the push p50 | **done** (this commit) | `QUEEN_RAFT_DRIVER_NOTIFY` (default **on**) + `QUEEN_RAFT_WRITER_PIPELINE` (default **off** — against the task's "default 1", see the DECISION below) + finer pipeline stages behind `QUEEN_RAFT_METRICS` | **Splitting the ~4 ms arrival→proposed, round 3 direction (1).** Added three finer stages behind `QUEEN_RAFT_METRICS` — `queue_wait`, `drain_to_propose`, `writer_pickup` — to attribute the round-2 4.2 ms `arrival_to_proposed`. Two scheduling knobs: **(1)** `QUEEN_RAFT_DRIVER_NOTIFY` (default ON): the driver resolves an in-flight entry off the replicator's applied-index `Notify` (one apply→driver wake) instead of the two-hop forwarding task; the propose future still runs so `Timeout`/`Fatal` and **D7/I4** hold, and both paths guard on `resolved` (at-most-once). **(2)** `QUEEN_RAFT_WRITER_PIPELINE` (rendezvous two-thread write/fsync) — proven **I2**-transparent and crash-safe, kept as a working knob. **DECISION FOR ALICE:** WRITER_PIPELINE is **defaulted OFF** although the task asked default 1 — the laptop A/B shows it **regresses** push p50 (10.3→14.1 ms) by losing group-commit batching (`group_entries` 3→1), and the new `writer_pickup` stage shows the VM has ~0 writer queueing to overlap (PERF-2: `proposed→committed ≈ log_fsync`), so no upside there; flip to `=1` for the letter of the task. **I1/I2/I3/I4/D7** intact — both knobs are transport-only, the applier and its effects are byte-identical. **Numbers — laptop macOS `F_FULLFSYNC`, release md5-12 `b1412953a01d`, 30 s goload smokes, consume-matched, 0 broker err/panic.** STAGE SPLIT A20k baseline (log2 s→ms): `queue_wait` p50 2.1/p99 33.6, `drain_to_propose` 0.52/4.2, `arrival_to_proposed` 4.2/33.6, `writer_pickup` 4.2/8.4, `proposed_to_committed` 8.4/16.8, `log_fsync` 4.2/8.4 — so the "~4 ms before proposed" = `queue_wait` (pipeline-slot wait) + `drain_to_propose`; `writer_pickup` 4.2 ms is a laptop `F_FULLFSYNC` artifact (VM ~0 per PERF-2). **A20k push p50/p99, pop/ack/lag:** baseline WP0/DN0 **10.30/48.38** pop 472.9k ack 17.2k/s lag 76.9k; default DN1/WP0 **10.56/49.41** pop 428.8k ack 15.6k/s lag 121k (**NEUTRAL** vs baseline, within noise — the box is fsync-bound so DN is hidden here); both-on DN1/WP1 **31.36/83.46** pop 406k (**REGRESSION**, group 3→1); WP1/DN0 rendezvous **14.14/54.02** pop 500k. **C1000** (rate 3k, 1000 part, batch 1, 64 cons) push p50/p99, pop/ack/ackAvg/lag: baseline WP0/DN0 **42.24/85.50** pop 34.0k ack 34.0k 32.5 ms lag 51.4k; default DN1/WP0 **22.14/64.26** pop 59.1k ack 59.1k 20.1 ms lag 26.3k → **DRIVER_NOTIFY ~48% lower p50 and ~1.7× consume** (single 30 s run each — repeat ≥3× before ranking, Alice's rule). Runs 30 s not 60 s (short smoke); A20k did not fully drain (lag grew) — stated. **Tests:** new `the_write_fsync_pipeline_holds_under_concurrent_in_flight_groups` fires all N proposes into flight at once so the writer forms group N+1 while the syncer fsyncs group N (real multi-entry groups; the old sequential test only ever had 1 entry per group), asserting exactly-once/gapless indices + **I2** digest == the reference applier; teeth proven (a within-group ordering fault in `handoff_group` makes the new test FAIL while both sequential WP tests PASS; fault reverted, 0 residue). `cargo test -p queen-engine --lib rsm::` = **374 passed / 0 failed / 11 ignored** (32.6 s). `rustfmt --edition 2021` + clippy clean on touched files; **I2** deny-gate green. **VM confirmation owed** — the laptop `F_FULLFSYNC` (~4 ms/leg) hides DRIVER_NOTIFY's A20k win (neutral here); the VM `fdatasync` (~1 ms) leaves a bigger scheduling fraction in `propose_roundtrip`, so DN should move A20k there (not run — PERF-G deliverable is the laptop smoke). See **R-130**. Files: `server/src/rsm/timing.rs`, `rsm/batcher.rs`, `rsm/replicator/mod.rs`, `rsm/replicator/local.rs`, `rsm/replicator/fake.rs`, `rsm/replicator/log.rs`, `rsm/tests/batcher.rs`, `rsm/tests/replicator.rs`, `rsm/tests/replicator_crash.rs`. |

| PERF-I the wildcard pop claims from RAM, no store scan per claim | **partial** (this commit) | `QUEEN_RAFT_CLAIM_FROM_RING` (default **on**; off = the baseline `segs_from` scan) | **O(claimed) claim, round 3 direction (3).** Root cause: `claim_one` scanned the partition's WHOLE txns history from `log_start` on every claim (`segs_from`) — unbounded with retention off — plus a 2nd scan (`hashes_in_range`) and a reverse scan. The bounded path reads ONE txns pass from the segment covering `wanted`, stopping at budget/deferred, folding the delivered hash set (**O16**, byte-identical, no semantic change) into that single pass; auto-ack skips the fold. Effect-identical to the baseline with **automatic fallback** on conflation, a front retention gap, or no live segments (conflation deliberately stays on the `segs_from` path, documented in code). **I1/I2/I4/I5** intact — planner-only read change, no effect/digest touched. **NOT done (owed, R-132):** eliminating the per-claim partition+cursor POINT reads needs the planner to read a persistent apply-maintained `Derived`, but `batcher.rs` rebuilds `Derived` from `pending` every cycle (outside this WP's ownership: planner/state/apply/facade), so the ring-entry-facts / part-2 delivered-RANGE-to-facade design is a documented follow-up. **Numbers — in-process bench** (fresh group, 30-frame budget, growing history): baseline **42.1 / 78.6 / 224.0 / 800.4 us/pop** at 50/200/800/3200 segments (linear in history); bounded **FLAT 31.1 / 31.3 / 31.1 / 31.5 us** — **25x at 3200 segs**, and unbounded-history-flat (retention off). Differential fuzzer: 50 seeds, ~1300 claims, **0 divergences** (identical pop outcomes + cursor rows on vs off); deep-history lagging-consumer test identical. **Live C1000 smoke** (release md5 `141b8a403f06`, local APFS, round-2 best cfg `BATCH_COUNTERS=1 DEDUP_INDEX=txns DEDUP_FRONT=1 DURABLE_ASYNC=1 SEG_BUFFERED=1 BUCKETS=16 PIPELINE=4`, vary only `CLAIM_FROM_RING`): 1000-part 45s plan p50 **1.049->0.524 ms**, plan max 7.41->5.44; 100-part 40s plan p50 0.066->0.016, plan p99 **8.389->4.194**, `arrival_to_proposed` p99 **16.78->8.39 ms**. End-to-end pop throughput is fsync-bound (`durable_point` p50 67 ms) and closed-loop-confounded (+-40% run-to-run: OFF 8.6-15.7k, ON 7.6-22.1k over 3 pairs), ON avg >= OFF -> **no regression**. Reproduce the bench: `cargo test -p queen-engine --lib rsm::tests::pop_bounded -- --nocapture`. **Tests:** the differential `bounded_and_baseline_agree_on_random_workloads` + `a_lagging_consumer_claims_identically_both_ways`; `cargo test -p queen-engine --lib rsm::` = **376 passed / 0 failed / 12 ignored**. `rustfmt --edition 2021` + clippy clean on touched files; **I2** deny-gate green. **Canonical end-to-end A/B is OWED on the VM** — the local goload is an older closed-loop `-mode max` build lacking `-rate/-ramp-sec`, so it can't reproduce the rate-3000 C1000 regime (the 'different consume load' pitfall); needs `/root/goload` openloop on the VM (VM not touched this run). See **R-132**. Files: `server/src/rsm/planner/pop.rs`, `rsm/planner/mod.rs`, `rsm/tests/pop_bounded.rs`, `rsm/tests/planner_harness.rs`, `rsm/tests/mod.rs`. |

**Measurement outcome (2026-09-19).** The package was measured on the VM in three
rounds (**M8** round 1, **M9** round 2, **M10** round 3). **Round 1** (PERF-A/B/C,
`5de30164`): the store levers did **not** move the O14 numbers — the durable-point LMDB
`data.mdb` env-sync stayed on the apply thread (268 ms A20k) and remained the wall; only
PERF-B earned its keep (8 M-loaded C1000 flatness +546%→+50%). **Round 2** (PERF-D/E/F on
top, `431171e4`): removing the per-message dedup RMW (PERF-E), the per-append
counter/pending writes (PERF-D) and the 256-file fsync fan-out (PERF-F at buckets=1)
**collapsed the durable point 268→16.8 ms** — **A20k push p99 203→16.5–29.8 ms**
(p99 parity with pg), **FAT100 86k→265k msg/s (92% of the knee)**, **C1000 unchanged**
(bounded upstream at `arrival→proposed` ≈1073 ms). **Round 3** (PERF-G/H/I on top,
`f006c3d4`): the scheduling / ring-re-arm / O(claimed)-wildcard-pop hardening — A20k **p50
6.05 / p99 17.28** (O14-strict PASS both), FAT100 **90.7%**, C1000 **211.97 / 790.53** —
performance-neutral-to-parity, regressing nothing, but moving **neither push-latency
bound** (A20k `arrival→proposed` p50 4.19 ms; C1000 p99 1073 ms); **AB-0 (PERF-6)**
settled the consume worry as a **non-regression** (R-130 discharged). **`proceed=false`**
on the C1000 p50/p99 gate across all three rounds; the long PERF-5/PERF-8 runs were
skipped (Alice's iterate rule). Post-round-3 defaults: `QUEEN_RAFT_APPLY_WRITERS`
adaptive, `QUEEN_RAFT_BUCKETS`=16, `QUEEN_RAFT_DEDUP_INDEX`=txns,
`QUEEN_RAFT_BATCH_COUNTERS`=1, `QUEEN_RAFT_DRIVER_NOTIFY`=1, `QUEEN_RAFT_CLAIM_FROM_RING`=1,
`QUEEN_RAFT_WRITER_PIPELINE`=0, `QUEEN_RAFT_PENDING_TRANSITIONS`=0. Full tables in
`test/raft/vm/raft1/PERF-{2,3,4,6,7}-*.md`; the open follow-ups are R-127..R-132.

### Phases 1–6 — the rest, all `not started`

The WP list is PLAN_RAFT.md §15. Apart from WP-1.1 and WP-1.3 above none has
begun, so they are kept here as one line per phase rather than six tables of
empty rows.

| phase | work packages, all `not started` |
|---|---|
| 1 — single node, message path | WP-1.1 codec **done** · 1.2 store adapter ⚠ · 1.3 segment files **done** · 1.4 apply thread ⚠ · 1.5 planner · 1.6 LocalReplicator + batcher ⚠ · 1.7 handler seam · 1.8 crash matrix ⚠ · 1.9 raft1 topology + parity gate · 1.10 difffuzz v1 + checker v1 · 1.11 VM regimes + flatness |
| 2 — whole feature set | WP-2.1 transactions ⚠ · 2.2 KV · 2.3 timers ⚠ · 2.4 streams · 2.5 admin · 2.6 reads · 2.7 retention ⚠ · 2.8 local metrics + dashboard · 2.9 compaction ⚠ · 2.10 embedded · 2.11 conformance · 2.12 every suite + differential v2 |
| 3 — one voter | WP-3.1 pin the library · 3.2 consensus log storage ⚠ · 3.3 state machine adapter ⚠ · 3.4 recovery matrix ⚠ · 3.5 parity with LocalReplicator |
| 4 — three voters | WP-4.1 transport ⚠ · 4.2 forwarding ⚠ · 4.3 membership ⚠ · 4.4 leadership ⚠ · 4.5 reads · 4.6 snapshots ⚠ · 4.7 digests · 4.8 health/metrics · 4.9 raft3 topologies · 4.10 crash + kill matrix ⚠ · 4.11 helm |
| 5 — operations | WP-5.1 rolling upgrade ⚠ · 5.2 backup/restore ⚠ · 5.3 migration ⚠ · 5.4 SDK resilience · 5.5 docs · 5.6 CI lanes · 5.7 final adversarial review ⚠ |
| 6 — stage and production | a separate plan with Alice; nothing starts without her explicit OK |

Two phase-1 WPs now carry a precondition:

- **WP-1.2** (store adapter) must not write heed-specific code before D-01 and
  the §11.5 repair decision (S1 §6.9, R-01/R-03).
- **WP-1.2 / WP-1.4** must not freeze the dedup keyspaces before D-06 (S2
  recommendation 5).

---

## Findings

Every finding of the four adversarial reviews, with its resolution. `closed` =
fixed or measured; `open` = upheld and still owed; `plan` = upheld, needs a
decision or a plan edit (PLAN_RAFT.md is not edited by this task).

### From S1 / WP-0.3 (reviewer A `major`, reviewer B `blocker`; the memo upholds all 19)

| R-id | src | finding | resolution | state |
|---|---|---|---|---|
| R-01 | S1 R1.1 | The memo's own ratification precondition (20 dropped-write heed runs) was never run; D9 was ratified anyway and the plan demoted it to "residual risk". | Upheld. It is deferred item **D-01**; the pin is provisional until it exists. | **open** |
| R-02 | S1 R1.2 / R2.5 | The `kill -9` half cannot falsify (page cache survives): all 45 passed. The verdict rests on 20 dm-flakey runs (5/5/10) against WP-0.3's 100+20, and 20 s runs on a VM with `dirty_expire_centisecs=3000` bias toward the reported result. On n=10 one failure has a 95% interval of ~0.3–45%. | Upheld, **cannot be closed in this budget**. Sample sizes now stated next to every verdict. D-01, D-02. | **open** |
| R-03 | S1 R1.3 / R2.6 | The §11.5 log-tail repair was applied asymmetrically: it fixes fjall's failure (state ahead of an intact store) and cannot fix heed's (store will not open). | Upheld and decisive. Memo §1 rewritten with both raw log lines; with the repair adopted fjall's failures become recoverable and heed's does not. | **plan** |
| R-04 | S1 R1.4 / R2.6 | In D2 single-voter/embedded there is no snapshot source, so heed's unopenable store is unrecoverable data loss; redb is 20/20. | Upheld. **redb is now the proposed engine for raft1/embedded.** | closed |
| R-05 | S1 R1.5 | The spike measured `MDB_NOSYNC` without WRITE_MAP and never `MDB_NOMETASYNC`; the causal claim ("the corruption LMDB documents for that flag") is not what `lmdb.h` says for a non-WRITEMAP env. | Upheld. Claim removed. NOMETASYNC measured: **17 309 msg/s (87%), WA 7.04×, store commit p99 123 ms** — kept OFF for now; its crash behaviour is D-03. | closed |
| R-06 | S1 R1.6 | heed's read numbers come from thread-local reader slots, where `RoTxn` is not `Send`; `max_readers` never exercised. | Upheld, and worse than guessed: `MDB_NOTLS` gives **2.06/1.97/2.08 M gets/s at 1/4/8 threads** vs 2.10/8.18/**9.96** M with TLS; `MDB_BAD_RSLOT` on the 2nd txn on one thread; `MDB_READERS_FULL` at `max_readers`. New read-path rule (D9 pin b). | closed |
| R-07 | S1 R1.7 / R2.7 | The ordered-read pillar is 180 001 rows / **9.6 MiB fully in page cache**; heed was never scanned beyond that and never soaked. | Upheld. Demoted to unproven; D-04. | **open** |
| R-08 | S1 R1.8 / R2.7 | The "RAM is reclaimable page cache" pillar has no long-run evidence; the only soak was fjall's, and it is used against fjall. | Upheld. Demoted; heed's RSS at 40 s is *higher* than fjall's (163 vs 129 MiB at 20k). D-04. | **open** |
| R-09 | S1 R1.9(a) | PLAN_RAFT.md D9 says heed is "the only candidate that always reopens consistent after kill -9 and after dropped writes". Both words are false: redb is 20/20 with 0 past-durable; heed reopened past its durable point in **12 of 15** kill runs. The "redb capped at ~10k msg/s with 29× write amplification" clause is also now false. | Upheld. Replacement wording for §2 of the plan is in S1 MEMO §6.3; **PLAN_RAFT.md needs the edit**. | **plan** |
| R-10 | S1 R1.9(b) | `RESULTS-vm.md` §3 said "3 of the 5 runs"; the log shows 4 of 5. | Upheld, fixed in `RESULTS-vm.md`. | closed |
| R-11 | S1 R1.10 | What could not be refuted: durable-point ordering, `verify --verify-all`, adapter symmetry, the honesty of the cut list, DEPS.md's facts, the histogram's documented ±3.2%. | Recorded. | closed |
| R-12 | S1 R2.1 | The comparison measured a design that no longer exists: §11.3 now batches, and redb was dropped on exactly the per-commit cost a 256:1 batch amortises. | Upheld **and it changes the answer**: redb 10 046 → **19 539 msg/s**, WA 29.31 → **8.33×**, isolated to the cadence by a 2×2. redb re-admitted. | closed |
| R-13 | S1 R2.2 | The ordered-scan pillar was measured on `segments`, which the §6.1 amendment removes from the store. | Upheld. Re-measured over `seg_loc`: heed **41.5 M rows/s** vs fjall 2.1–2.7 — the 15–20× gap survives; §2's table carries a warning header. | closed |
| R-14 | S1 R2.3 | The store's new dominant keyspace (random `(pid, hash)`) was never in the harness. | Upheld, partly closed: added; it costs heed **3.30 → 3.91×** WA. 800k keys in a 145 MiB page-cached file ≠ the ~72 M-row regime — still unmeasured. | **open** |
| R-15 | S1 R2.4 | redb's 0/15+0/5 is structural; heed's 0/10 is an observation. The memo read them as the same kind of fact. | Upheld; the distinction is now the second reason for D9's redb clause. | closed |
| R-16 | S1 R2.6 | The recommendation's precondition (§11.5 repair) was never ratified; §11.5 step 3 still says "discard or refuse to start", which in raft1 is data loss. | Upheld. **Decide the repair before WP-1.2.** | **plan** |
| R-17 | S1 R2.7 | I8 says S1 "prefers engines with incremental checkpoints"; heed is the only candidate that cannot, and the memo never said it was choosing against the invariant. | Upheld; stated in the table and in §4. | closed |
| R-18 | S1 R2.8 | `MDB_MAP_FULL` is an apply-path failure with no backpressure: §11.8 gates on disk percent only, and D15 makes long read txns ordinary. | Upheld — a plan gap. **§11.8 needs a map-size rule** (report map usage in Status, gate the planner on the worst voter, bound read-txn lifetime, define the apply-path behaviour). | **plan** |
| R-19 | S1 R2.9 | "5–7× below at 50k/100k" is wrong (4.86× / 4.75×); DEPS.md's build-image item was open; RAFT_STATUS.md said `proposed` while the plan said RATIFIED. | Upheld ×3: sentence removed; build image **closed in heed's favour** (both Dockerfiles are `rust:1-bookworm`, glibc + gcc, C-2 holds); the status/plan disagreement is the note at the top of this file. | closed |

### From S2 / WP-0.4 (two reviewers, both `blocker`; the memo upholds all 15 and the verdict changed)

| R-id | src | finding | resolution | state |
|---|---|---|---|---|
| R-20 | S2 R1 | The recommended design was never measured: revision 1 recommended (b) minus the cache **plus** a new exact ack index — a composite in no cell. | Upheld; the recommendation is withdrawn. §5 now recommends a design measured in five cells on two hosts. | closed |
| R-21 | S2 R2 | That "ack index" is option (a)'s index: 005's below-cursor span is `[txns_start, committed]`, the whole txns window. | Upheld — **the finding that decides**. Both reviewers found it independently. | closed |
| R-22 | S2 R3 | A smaller ack row cannot exist: 005 needs `eff` (a MIN over a per-call range) **and** `below`, so the value must be the occurrence list. | Upheld; 5 591 multi-occurrence rows show a (min,max) pair would not do. | closed |
| R-23 | S2 R4 | WP-0.4's mandatory criterion — probe p99 at 50k with a **1 h** window — was never run, and 1 h is the product default (`schema.sql:66`). | Upheld, **not closed**. D-06. | **open** |
| R-24 | S2 R5 | "The only one whose cost does not follow the window" is contradicted by the spike's own sweep. | Upheld; the claim is gone. (b)'s *store* is per-Append; its blooms, file table and frame scans are linear in the window. | closed |
| R-25 | S2 R6 | The ack measurement did not model 005: one hash per call instead of one pass per ack. | Upheld **and measured**: `--ack-batch 10` gives 2.2× the resolutions at −6.9× per-hash cost and **+54% throughput**. Revision 1's third argument withdrawn. | closed |
| R-26 | S2 R7 | Option (a) was convicted on one wasteful encoding D10 does not mandate. | Upheld on the encoding (**a-lean**: −31% store, −36% store ops, −30% RSS, latency unchanged), **refuted by measurement** on the mechanism: kernel bytes +2%, so the secondary index was not the source of the write amplification. | closed |
| R-27 | S2 R8 | The 1227 ms durable point is mis-attributed and has no sensitivity run. | Upheld (no sensitivity run) and **refuted by measurement** (the mechanism). Three cadences: 250/1000/4000 ms → 0.605 / 0.307 / **0.137** s of sync per wall second. "Option (a) re-opens D9" withdrawn. One correction for §11.4: its "cost is proportional to what changed" is false on LMDB — 16× the interval costs 1.65× the time. | closed / **plan** |
| R-28 | S2 R9 | The recommended option had no valid restart-exactness number. | Upheld **and measured**: three new (b) cells, `wrong=0` of 4001, 3872 (laptop) and 1793 (VM). | closed |
| R-29 | S2 R10 | The decision table dropped probe p99, the statistic WP-0.4 names. | Upheld; p99 is in every table and every figure carries its window and achieved rate. | closed |
| R-30 | S2 R11 | "11–18× less store" omits the disk where (b) puts its hashes. | Upheld: **total disk per message is 3.3×** on heed (49.5 vs 161.8 B), 1.7× on fjall. | closed |
| R-31 | S2 R14 | Option (b) puts node-local positions in a replicated keyspace, and hash-only compaction rewrites them locally (D8, I7, I1). | Upheld, **unresolved**. As implemented (b) breaks D8/I7; the split's snapshot and recovery consequences are uncosted. A reason not to adopt (b) on this evidence. | **open** |
| R-32 | S2 R12 | System-level, (a) achieved 1.6–2.0× the rate of (b) in every cell. | Upheld, partly explained: 54% of the gap was the unbatched ack workload. | closed |
| R-33 | S2 R13 | Dropping the recent cache was recommended against the only end-to-end number (−31% rate), never run on the VM. | Upheld; the question stays open and rate-matched, and is moot while (b) is not the design. D-08. | **open** |
| R-34 | S2 R15 | G-3/I8 were misapplied: `rate × window` satisfies both as written; the real objection is absolute footprint plus an engine interaction. | Upheld; §4 says so in those words. | closed |

### From S3 / WP-0.5 (two reviewers, both `major`)

| R-id | src | finding | resolution | state |
|---|---|---|---|---|
| R-35 | S3 R1 | "Passes `openraft::testing::log::Suite`, which is the §12.3 storage contract" is an over-read: no case reopens a store, crashes a process or drops a write. | Upheld; claim withdrawn. Demonstrated: the suite passes against **all three** `save_committed` settings, including the one that never flushes. | closed |
| R-36 | S3 R2 | The adapter knowingly violates "`save_committed` persisted" (copied verbatim from openraft's example) and the memo never said so. | Upheld; `CommittedDurability::{None,Buffered,Fsync}` added; `Fsync` recommended; **plan change 5**. | closed / **plan** |
| R-37 | S3 R3 | A crashed pod recovers behind its pre-crash applied index and answers a D15 stale read without a message the client already saw. | Upheld **and reproduced**: with `none`, `WENT BACKWARDS on 1001: last_applied Some(301) -> Some(300)`, `applied-state-preserved=false`. With `fsync`: preserved on both hosts. | closed |
| R-38 | S3 R4 | No crash-restart was ever run: scenario 7 used a graceful `Req::Shutdown`; every `kill -9` was of a leader never restarted or an empty learner. | Upheld; **scenario 8** added (10 × `kill -9` of a follower with restart, on-disk state read before replication) — 10/10 clean on the VM, 4/4 on the laptop. | closed |
| R-39 | S3 R5 | `raft-log` must meet WP-0.3's bar including dropped unflushed writes; until then it is a proposed default. | Upheld, **now met**: `flaky-log.sh`, 10/10 rounds, injector self-tested (16 MiB unfsynced → 0 bytes; fsynced → whole). | closed |
| R-40 | S3 R6 | GH#2080 is rated "high impact" and was never reproduced. | Upheld, **now reproduced** (scenario 9), with a new finding: the client write **hangs**, it does not get a 503, because a leader *is* known. Operator recovery timed: heal 616 ms, `kill -9` 3528 ms. | closed / **plan** |
| R-41 | S3 R7 | `truncate_after` also returns without a flush — is that safe? | **Answered by reasoning, not measured**: openraft follows a truncate with the appends of the same `AppendEntries`, whose `flush(true, _)` is awaited; a lone truncate answers `Conflict`, which the leader does not count as matched. WP-3.1 owes a targeted test. | **open** |
| R-42 | S3 R8 | The I9 conclusion closes an EMPTY data dir, not a REVERTED one (a PVC from a volume snapshot matches IDENTITY and brings back a stale vote store). | Upheld; **plan change 6** (a fencing value in IDENTITY). Not run in this spike. | **plan** |
| R-43 | S3 R9 | "Snapshot build 0.2 s for 1 GiB, transfer 398 MiB/s" measures the spike's own manifest transport, not openraft and not a Queen snapshot (no store export). | Upheld; labelled everywhere. **The D11 rationale in the plan needs the same correction.** | **plan** |
| R-44 | S3 R10 | openraft cannot purge log past the snapshot, so snapshot cadence + an O(state) export collides with I8/G-3 at flatness scale; cadence was never measured. | Upheld, out of D11's scope; **a gate for §11.6/D9 before WP-1.2 closes**. D-10. | **open** |
| R-45 | S3 R11 | The kill matrix is not §13.6's shape (3 runs, no randomisation, no kill during a snapshot build or install). | Partly upheld: scenario 8 adds 20 kill+restart rounds at arbitrary durable-point phases (more than §13.6 asks for the follower case). Kills during a snapshot build or install remain **deferred** (D-11). | **open** |
| R-46 | S3 R12 | Five reporting errors: scenario 1 ran 98 s and 125 s not "60 s per rate" (the 1000/s point cut at **38 217 of 60 000** offered); the VM window was 11 min 41 s not 10 min 30 s; three commits landed after the pin not two; the snapshot numbers need a label; two duplicate results directories. | Upheld, all five corrected. One archive of record: `results/vm-2026-09-17/`. | closed |
| R-47 | S3 R13 | What could not be refuted: the pin's identity and licence, `LogEntryDiscarded` at `errors/mod.rs:116`, the wiped-voter analysis (a bare `debug_assert!`, `allow_log_reversion` false by default), the kill/transfer/read/election numbers, MSRV 1.88. | Recorded. | closed |

### From S4 / WP-0.6 (two reviewers, both `major`)

| R-id | src | finding | resolution | state |
|---|---|---|---|---|
| R-48 | S4 R-A1 | "Encrypted 0.32×" is not like-for-like: there was no HTTPS-without-MAC row. | Upheld, **measured**: HTTPS plain 3.33 µs/msg → **0.46×** @50k, 0.61× @20k. (Both reviewers' own ~0.54× estimate was also wrong.) | closed |
| R-49 | S4 R-A2 | The CPU gap measures write coalescing, not framing; 0.88× at 8 sockets; the burst shape is a harness artefact. | Upheld in part, **measured**: jitter-free pacer → framed +26%, HTTP +2%, ratio 0.54× → 0.66×. The reverse test is impossible: HTTP capped at 2 opened **218 695 connections in 60 s**. | closed |
| R-50 | S4 R-A3 / R-B | TLS-over-MAC rests on an 11× that is 0.055 core, and on a p50 argument the memo's own porting note deletes. | Upheld; **recommendation 2 changed to "either"**. The p50 argument is re-grounded: an anti-replay MAC needs a sequence number, which lives on the writer task — where rustls also encrypts. | closed |
| R-51 | S4 R-A4 | "Two connections is the knee" is a 9 µs loopback artefact. | Upheld; **recommendation 4 changed**: 1 is CPU-optimal, 2 is an unmeasured hedge, the pool is sized in the transport WP. | closed |
| R-52 | S4 R-A5 | Multiplexing's blast radius (one dead socket = N unknown outcomes) is never weighed. | Upheld, with a bound: D6 makes it N *retries*, not N unknown writes, provided in-flight waiters fail fast — which they now do. | closed |
| R-53 | S4 R-A6 | PLAN_RAFT.md D12 ratifies with **laptop** numbers ("1.35 vs 2.77 µs", "half the leader CPU"), which §0.3 forbids quoting. | Upheld. The VM says **1.26 vs 2.15 = 0.59×** on 2 sockets against 15–49, 0.88× at matched sockets, 0.46× encrypted. **The plan needs the edit.** | **plan** |
| R-54 | S4 R-A7a | `results/vm-a/` was called a "byte-identical copy" of `results/vm/`. | Upheld; corrected — `results.jsonl` and `runlog.txt` are identical, the directories are not. | closed |
| R-55 | S4 R-A7b | "rc=0 for every one" records `sed`'s status, not the receiver's. | Upheld; replaced by the checks that do hold (`cmds_bad = 0`, achieved = offered, `cmds_ok = achieved × secs`). | closed |
| R-56 | S4 R-A7c | RAFT_STATUS.md says WP-0.6 "in progress" and G0 "pending" while the plan says G0 RATIFIED. | Upheld; **this revision fixes it** (and states the disagreement explicitly). | closed |
| R-57 | S4 R-B1 | TLS as measured has **no peer authentication and no channel binding**, so a relay that terminates both legs passes the HMAC handshake through and then injects. | Upheld — **the most serious finding**. Recommendation 2 now requires client certificates **or** RFC 5705 channel binding, and says D12's TLS branch cannot be ratified while its trust anchor is undecided. | **plan** |
| R-58 | S4 R-B2 | The per-frame MAC covered `type \|\| body` only: replayable after the D6 window, reflectable, `len` unauthenticated. | Upheld; **fixed in `frame.rs`**: per-direction keys, MAC over `dir \|\| seq \|\| len \|\| type \|\| body`, receiver accepts only its expected sequence. +13 B of HMAC input = +0.4%. Three new tests. | closed |
| R-59 | S4 R-B3 | "Fail closed" was claimed but not implemented: an empty secret handshook happily — pgless U19 reproduced inside the spike that cites U19. | Upheld; **fixed**: `auth::check_secret` (≥16 B) on both sides, the binary refuses to start without `--secret`. | closed |
| R-60 | S4 R-B4 | The authenticated pair is a strawman: HTTP MACs one direction and hex-formats through 16 `format!`s. | Upheld in wording, **negligible in size**: measured, the hex path is 5.2% of the HTTP MAC op and ~1.3% of that row's CPU (0.645 → 0.653); the opposite bias is of the same order. Binary constant-time form added as the one to port. | closed |
| R-61 | S4 R-B5 | The sweep ran at **0.31** commands in flight; D7 implies 15–35. | Upheld; recommendation 4 changed, re-run shape specified. D-14. | **open** |
| R-62 | S4 R-B6 | §9.2 puts `PayloadRead` on the forwarding pool, and `MAX_FRAME` (8 MiB) cannot carry a 96 MiB `QUEEN_RAFT_ENTRY_MAX_BYTES` entry. | Upheld, **not fixed** (raising the cap without a chunking rule turns `buf.resize(len)` into an allocation DoS). An open item the transport WP must close. | **open** |
| R-63 | S4 R-B7 | `tcpx.rs` is not portable as is: **no deadline at all**, waiters leak on connection loss. | Upheld; **fixed** (5 s default deadline, waiter removed on expiry, reader clears the pending map). Reconnect is **not** implemented — it needs the D13 hold and the leader hint. | closed / **open** |
| R-64 | S4 R-B8 | HTTP/2 over the same rustls was never measured, and it is the alternative that removes most of the hand-written surface. | Upheld; not measured. D-17. | **open** |

### From WP-1.1 (accepted, not fixed — decided and documented, owed to a later WP)

| R-id | src | finding | resolution | state |
|---|---|---|---|---|
| R-101 | WP-1.1 | The pop/ack/renew/DLQ-head **outcome shapes** are read off procedures `004`/`005` by eye; nothing in this WP proves the codec's shapes match what the SQL actually returns. | Accepted, not fixable here: only **WP-1.5**'s conformance tests against the procedures can prove them. A mismatch found there is a **version bump, not a break** — the codec already refuses an unknown version. | **open** |
| R-102 | WP-1.1 | `QueueConfig`'s integer and boolean columns are encoded as **non-null resolved defaults**; if configure merge semantics can yield a null, the codec cannot represent it. | Accepted. **WP-2.5** must confirm against `configure_merge_semantics.rs` and widen the encoding if it is wrong. | **open** |
| R-103 | WP-1.1 | The **O16 delivered set** is modelled as distinct `xxh3_128` hashes because `ack_registry.rs` holds it that way today — a collision or a change of representation in the registry silently changes the entry format. | Accepted and documented. Revisit when WP-1.4 ports the registry; the collision bound is not computed here. | **open** |
| R-104 | WP-1.1 | `TraceAppend` carries **no `seq`** (apply assigns it), and effect **versions are one global catalogue sequence**, not per-kind counters. | Accepted as design, stated because it is load-bearing: the global sequence is the only reading under which `kinds_version` gates **D20/I16** correctly. WP-1.4/3.3 must not reintroduce per-kind counters. | **open** |

### From WP-1.3 (accepted, not fixed — decided and documented, owed to a later WP)

| R-id | src | finding | resolution | state |
|---|---|---|---|---|
| R-105 | WP-1.3 | **I8 residual, with a number.** §6.1's "the active file's entries in RAM, bounded by one file's worth" is one record **per frame**, not per file. Measured **13 443** frames per 4 MiB for one-message pushes → ~**215k** records per bucket at the 64 MiB `QUEEN_RAFT_SEGMENT_BYTES` default → ~**55M** records × ~75 B ≈ **4 GB** worst case if all 256 buckets are hot **and** every push is a single message. Fat batches are three orders of magnitude cheaper (30 frames per 4 MiB). | Accepted, not fixed here: it is the **ratified** §6.1 design and the fix is a plan change. Levers for **WP-1.4 / WP-1.11**: a smaller `QUEEN_RAFT_SEGMENT_BYTES`, or a partial `.qidx` written at each durable point. **Pipeline=4 (NIGHT-A/B):** ~**387 B/msg** at rest (⅓ below pipeline=1's 610), and RSS is **bounded and reclaimable** — a 50k run plateaued ~10 GB RSS while the store grew to 51 GB on disk, no OOM, `MemAvailable` never below 8.8 GB. But still **linear in stored count** (I8: RSS not flat in any shape), so the concern's shape is confirmed and the worst-case 4 GB is not reached at this store size; the levers stay unexercised. | **open (confirmed favourably at pipeline=4)** |
| R-106 | WP-1.3 | Durability under **dropped unflushed writes** is not proven: a `kill -9` does not drop the page cache, so the crash tests falsify the bookkeeping (recorded lengths, the seal, the `.qidx`, which file is active) and not unsynced bytes. | Accepted — this is **R-02** restated at the segment layer. The run belongs to the VM regimes, §13.6 / **WP-1.11**. **Discharged by WP-1.11**: 20/20 dm-flakey rounds PASS + adversarial reopen 3000/3000 recovered (`test/raft/vm/raft1/RESULTS.md` §4). | **closed** |
| R-107 | WP-1.3 | No **crash-point hooks**: the mid-roll kill is driven by timing, not by a fault point, so the crash matrix cannot target a roll deterministically. | Accepted. **WP-1.8** adds `seg.rolled` and `seg.qidx_written` to `rsm/faults.rs` and re-runs the roll cases against them. | **open** |
| R-108 | WP-1.2 | **Parity risk: a composite name key can exceed LMDB's 511-byte limit.** `(tenant, queue, group)` is three unbounded names and `consumer_groups_metadata.consumer_group` is `TEXT` in postgres, so a key postgres accepts is one the store refuses. | Accepted, not fixed here: the adapter refuses with a typed `KeyTooLong` + metric instead of truncating. **WP-1.5 / WP-1.7 must bound names at the receiver**, or D9 needs heed's `longer-keys` feature — a change to the measured configuration, so **Alice's call**. | **open** |
| R-109 | WP-1.2 | **Replicated and node-local keyspaces share ONE environment**, because I11 requires the applied index and the segment file lengths in one transaction — so a snapshot taken naively would carry a sender's node-local positions into a receiver (D8, I7). | Accepted as design: every keyspace declares a `Scope` and the adapter has `clear_node_local()`. **WP-4.6 must exclude node-local keyspaces from the snapshot export and clear them on install.** | **open** |

### From WP-1.4 (accepted, not fixed — decided and documented, owed to a later WP)

| R-id | src | finding | resolution | state |
|---|---|---|---|---|
| R-110 | WP-1.4 | **§11.5 step 3 has no repair in phase 1.** A real disagreement — a segment file SHORTER than the length the store recorded, i.e. unflushed writes dropped — can only be repaired by restoring from a snapshot, and there is no snapshot before WP-4.6. | Accepted: the node **refuses to start** with `ApplyError::Disagreement` rather than truncating the store's record or papering over the gap. WP-4.6 owns the repair. | **open** |
| R-111 | WP-1.4 | **Durability under dropped unflushed writes is still not proven for apply.** `kill -9` keeps the page cache, so the two crash tests falsify the bookkeeping (applied vs durable index, recorded lengths, GC staging) and not unsynced bytes. | Accepted: same shape as R-02 / R-106. The falsifying run is the Linux VM's (dm-flakey / power-cut), owed to **WP-1.8 / WP-1.11**. **Discharged by WP-1.11**: 20/20 dm-flakey rounds PASS + adversarial reopen 3000/3000 recovered (`test/raft/vm/raft1/RESULTS.md` §4). | **closed** |
| R-112 | WP-1.4 | **`Counter::Pending` is maintained at GROUP scope only** and retention does not reduce it, so it reads as pushed-minus-completed and is **not** `PartitionRow::pending_from`. A partition- or queue-level "pending" is not well defined across groups. | Accepted as scope: **WP-2.6** owns the mapping of every read that today answers a pending number, and must not assume partition scope. | **open** |
| R-113 | WP-1.4 | **Cross-WP widening of `store::rows::FileRow`** past WP-1.2's four-field row (now `frames`, `retained_frames`, `retained_bytes`, `window_frames`), forced by WP-1.3's `Segments::open` refusing a recorded sealed file with no frames — the narrow row would have made every sealed file dead after a restart. `dedup::push_occurrence` / `check_occurrences` also became public. | Accepted and done here, because the alternative is a broken restart. Stated so WP-1.5/2.x do not assume WP-1.2's narrower row codec; the node-local `files` keyspace is not replicated, so no format gate is owed. | **open** |
| R-114 | WP-1.6a | **Durability under dropped unflushed writes is not proven for the `LocalReplicator` WAL.** `kill -9` keeps the page cache, so the crash test falsifies the bookkeeping (group commit ordering, torn-tail truncation, `open()` replay after the store's durable index, the sub-header-file drop) and not unsynced bytes. | Accepted: same shape as R-02 / R-106 / R-111. The falsifying run is the Linux VM's (dm-flakey / power-cut), owed to **WP-1.8 / WP-1.11**. **Discharged by WP-1.11**: 20/20 dm-flakey rounds PASS + adversarial reopen 3000/3000 recovered (`test/raft/vm/raft1/RESULTS.md` §4). | **closed** |

### From WP-1.10 (accepted, not fixed — client-visible facade parity gaps, warrant a WP-1.7/Alice decision)

| R-id | src | finding | resolution | state |
|---|---|---|---|---|
| R-115 | WP-1.10 | **Empty pop returns `200` + body on raft vs a bodiless `204` on postgres.** difffuzz's `notePop` absorbs it as a classified envelope gap, but it is client-visible facade parity, not a difffuzz bug. `PopOut` already carries `empty`, so the `204` mapping belongs in `handlers/raft.rs` (outside `rsm/`). | Accepted, not patched in this WP (touched no broker source). The one-line `204` mapping is a **WP-1.7 facade** fix; reproducible from `PARITY-NOTES.md`. | **plan** |
| R-116 | WP-1.10 | **`leaseReleased` is attributed to a different batch item per engine.** On a mixed batch-ack the postgres oracle and raft name a different sibling as the one whose lease was released; difffuzz classifies it and checks the effect via the pop diff, not the field. | Accepted, not patched here. Needs a WP-1.7 / Alice decision on the canonical attribution; the underlying state (which messages are completed) agrees, only the reported item differs. Reproducible from `PARITY-NOTES.md`. | **plan** |

### From WP-1.11 (VM raft1 campaign; F-1 is a correctness ⚠ blocker)

| R-id | src | finding | resolution | state |
|---|---|---|---|---|
| R-117 | WP-1.11 | **⚠ Under `QUEEN_RAFT_PIPELINE=4` the log index can diverge from batcher plan order → apply I5 `TimeWentBackwards` → node poisons under any concurrent load.** The batcher stamps `now_us` monotone in plan order, but `propose` (`batcher.rs:902`) `tokio::spawn`s each `repl.propose()` independently, so the LocalReplicator writer may assign the log index out of plan order; apply sees `now_us` go backwards vs the index, refuses (`entry now_us …698930 below last applied …699010, index=672 applied=671`), and every later write 503s. Reproduces **only under concurrency** (dies at applied≈33–670; survives 400 sequential), so the strictly-sequential WP-1.10 difffuzz could not catch it. Blocks the O14 comparison, the clean flatness I8 result, and the O19 verdict — all owed a pipeline=4 re-run. | Accepted, **not fixed in this WP** (measurement WP, touched no broker source). Fix: make the log index follow the batcher seq/plan order (propose in order, or gate the writer on seq). The RESULTS.md records that such a fix has since landed in a sibling's uncommitted `batcher.rs`; it needs the planner owner + an adversarial ⚠ review with a **deterministic** regression that fails on the old spawn-per-propose form. **Fixed (commit `f8bfa672`):** propose is now submitted INLINE on the one driver task in plan order (no-op-waker first poll drives the future to `LocalReplicator`'s pre-`.await` writer-channel submission; only the local-apply wait is spawned, so up to `pipeline` stay in flight — `step_down_with_four_in_flight` green). Deterministic guard `every_propose_submits_on_the_one_driver_task_in_plan_order` (ONE tokio task Id vs N), plus the in-plan-order and real-log pipeline=4 tests. Adversarial review **could not refute** (I5 guaranteed structurally, not by timing; I3 preserved; noop-waker re-poll sound). **One MINOR finding stays OPEN:** the deterministic guard drives `TaskWitnessReplicator` (a fake that submits in its first-poll prefix by construction) and does not exercise the real `LocalReplicator` submit-before-`.await` contract — a future edit moving `cmd_tx.send` past an `.await` would silently reintroduce F-1 with no deterministic test failing; latent risk flagged for the phase-3 openraft adapter (the `Replicator::propose` doc now states the contract + phase-3 obligation). **Discharged empirically on the VM (NIGHT-A/B/C, pipeline=4):** 0 poison / 0 `TimeWentBackwards` across the six-regime sweep, a 5-min sustained A20k, a 75-min congestion collapse, a 64-min fill and 100 crash-reopen rounds (~200 M+ pushes) — the first proof the fix holds under the VM scheduler that produced F-1 (it never reproduced on a laptop). | **fixed (`f8bfa672`), discharged on the VM**; one MINOR test-net finding **open** |
| R-118 | WP-1.11 | **Apply-gate refusals aren't logged.** A gate rejection in `Applier::apply` (`gates(c)?`) bypasses `poison()`'s log line, so the real `ApplyError` (the I5/I18 detail) is lost and only "apply thread gone" surfaces to the operator — the poison that took F-1 down was near-undiagnosable from logs alone. | Accepted, not fixed here (diagnosability, outside a measurement WP). Log the `ApplyError` at `error` before poisoning. **Fixed (commit `f8bfa672`):** `Applier::apply` now matches `gates()` and returns `Err(self.refused(e))`; `refused()` logs the `ApplyError` at `error` with `applied`/`durable` before the node stops, so the I5/gap/bases cause surfaces instead of only the replicator's later "apply thread gone". | **fixed (commit `f8bfa672`)** |
| R-119 | WP-1.11 | **raft broker fd footprint exceeds the default ulimit.** ~304 open data-file fds (256 buckets + log + store) + one socket per connection; at `ulimit -n 1024` the log append fails `EMFILE` ("Too many open files") under A50k (48 consumers, 512 idle conns) and the node poisons. Inherent to §11.2 (a file per hot bucket). | Accepted, operational. helm/systemd need `LimitNOFILE` (the VM used 262144). | **open** |

### From the WP-1.11 night run (pipeline=4 VM pass, 2026-09-18/19; NIGHT-A/B/C)

The owed pipeline=4 re-run on the F-1-fixed binary (md5 `cdc5bb59af27`). It
discharged F-1 (R-117) empirically on the VM and produced the O14 / I8 / O19
verdicts (**M7**); it surfaced five new items. Load-bearing code refs re-verified
in source this revision: retention `planner/mod.rs:105` (`pub mod retention {}`,
stub, WP-2.7); dedup default 3600 `facade/real.rs:331`; subscriptionMode not
threaded `facade/real.rs:702`; `plan_budget_ms` (O17) **consumed** at
`batcher.rs:470`, `slow_command_ms` (O18) has **no** consumer.

| R-id | src | finding | resolution | state |
|---|---|---|---|---|
| R-120 | NIGHT-A/B | **O18 slow-command log + per-kind planner metrics are inert.** `QUEEN_RAFT_SLOW_COMMAND_MS` is read into `PlanConfig` (`batcher.rs:164`) and `CommandKind` exists, but **nothing consumes `slow_command_ms`**: a 5-min A20k at threshold 5 ms with sustained plan times of p50 44 ms emitted **0** slow-command lines. (This is O18 only; the O17 `plan_budget_ms` IS consumed at `batcher.rs:470` — the night reports' "neither is consumed" wording is corrected here.) `StoreMetrics` are also unmapped to any endpoint. | Accepted, observability. Wire the O18 WARN + `queen_raft_plan_seconds{kind}` and map `queen_raft_store_*`; owed to **WP-2.6** / a phase-2 obs WP. Staged, not wired — not a regression. | **owed a WP** |
| R-121 | NIGHT-B | **⚠ Unbounded push-admission queueing (HIGH, liveness).** At the product default (50k offered, 3600 s window) one node congestion-collapses to **0 achieved throughput at ~min 55 / ~93.5 M rows**: consume never keeps up (**3.8%** popped), goload inflight pins at its 20 000 cap, push p50 runs away 7 → 27 s, a direct `curl` push at min 57 times out (`http=000`). The broker does **not** shed or 503 — it queues without bound (`planner_queue_depth=1024`). 0 poison / 0 error / no OOM: the state machine is healthy; the failure is admission, not a crash. **Confound (disclosed):** no like-for-like postgres oracle exists at 50k for 75 min (WP-0.2's A50k ran 40 s at 6.23/8 cores on this co-resident-loader VM), so the *capacity ceiling* cannot be separated from the VM's own A50k CPU wall; the *defect* (no push-side backpressure → 10 s+ hangs instead of a fast 503) is separately and validly evidenced. | Accepted, defect. §11.4 / D13 (receivers-hold → 503) needs a push-admission reject that turns overload into a fast 503. Owner: admission/facade. **WP-0.4's named deliverable — "probe p99 at 50k, 1 h window" — still does not exist** (run A collapsed, run B was a 15k push-only deviation). | **for Alice / owed a fix** |
| R-122 | NIGHT-C | **`/health` `storageReady:true` flips before apply-replay finishes (MEDIUM, readiness).** Round 75 reported ready (reopen=317 ms) and then crashed in apply-replay — a doomed reopen looked healthy for ~ms and defeated the harness's own REOPEN_FAIL path; the same early-readiness would fool an operator's health probe. | Accepted, readiness. The readiness signal should not go true until apply-replay has caught up to the log tail. Owed to the health/readiness WP (phase 2/4). | **owed a WP** |
| R-123 | NIGHT-C | **⚠ SIGBUS on the store mmap at reopen after dropped writes (HIGH, durability/recovery).** 1/100 rounds at pipeline=4 the broker crashes `SIGBUS`/`BUS_ADRERR` on the `store/data.mdb` **read** mmap during apply-replay and **does not reopen** (fault ~52 MiB into the 64 GiB LMDB map, fd 10 = `store/data.mdb`; strace-confirmed, twice by hand). The acked writes are durable in the fsync'd log (`truncated_tail=false`, 680 690 frames) but the torn `MDB_NOSYNC` store crashes before recovery completes, so the round's acks are unrecoverable as-is. **Reproduces deterministically** on the preserved dir (123 MB reproducer under `evidence/`). This is the reproduced + classified WP-0.3 D-01/D-03 "heed store would not reopen"; not pipeline-specific (exposed by the larger stores, orthogonal to pipeline depth). | Accepted, defect. On an unclean shutdown the store must not dereference the un-fsync'd `data.mdb`: validate/repair, or **discard `data.mdb` when it is behind the log's durable point and rebuild from the log** (the log-is-truth recovery already re-scans every frame). Owner: planner/store (§11.5). Needs an adversarial ⚠ review + a **deterministic reopen-after-torn-store regression**. | **for Alice / owed a fix + ⚠ review** |
| R-124 | NIGHT-B | **The 3600 s dedup window bounds correctness, not memory, in phase 1 (MEDIUM).** A >window id correctly re-enqueues (`queued`, fresh offset — D10 option-(a) semantics; 26/26 early originals expired, a fresh id `duplicate`), but **phase 1 never reclaims the expired rows**: `data.mdb` grows straight through minute 60 (7.78 → 8.17 GB across min 59–62) because the leader retention/txns-purge loop (`planner/mod.rs:105`) is a stub and `retention_enabled` is false by default. At 50k that is ~10.6 GB/hour of dedup index never freed. | Accepted, phase-1 scope gap (not a regression). D-06's "then expiring" frees nothing until **WP-2.7** builds the retention loop; a long-lived cell needs it before the window's footprint is bounded. | **owed a WP (2.7)** |
| R-125 | PERF-A | **The store env-sync is still on the apply thread — the A/FAT durable-point long pole (accepted, not fixed).** PERF-A moves only the segment-fsync leg off the apply thread (`QUEEN_RAFT_DURABLE_ASYNC`); the LMDB `mdb_env_sync` on `data.mdb` (~94 ms on C1000, the dominant term on A/FAT per PERF-2) still runs inline on the apply thread inside `durable_point()`. Deliberately left there: moving it off apply safely needs either a D9 store-mode change (`MDB_NOMETASYNC` / data-first ordering — Alice's gate) or fencing apply commits while the sync runs, which stales planner reads (unsound). | Accepted, next lever — deliberately **not** done in PERF-A (correct-by-construction bound: no on-disk durable-point contract change here). Needs Alice's D9 decision before any store-side async sync. | **for Alice / owed a fix (D9)** |
| R-126 | PERF-C | **`apply_other` under the writer pool under-reads (measurement caveat, accepted).** With `QUEEN_RAFT_APPLY_WRITERS>0` the write time is recorded from worker threads that overlap the apply-thread wall clock, so the `apply_other` metric under-reads real write cost. The load-bearing numbers — the segment `write()` **count** and the coalescing ratio — are unaffected. | Accepted, not a defect: the coalescing ratio (`apply_segment` count) is the correctness/efficiency signal and is exact. No fix owed on the laptop; the VM ext4 pass reads latency from goload, not from `apply_other`. | **accepted / VM pass reads latency from goload** |
| R-127 | PERF-D | **`QUEEN_RAFT_PENDING_TRANSITIONS` defaults OFF — a coordinator decision, not a defect.** The lever is deterministic and rebuild-equivalent (`pending_transitions_rebuild_from_pending_equals_the_live_rings`), but it changes the replicated pending keyspace **value** (earliest `ready_at` vs today's last), hence the 12.9 digest. Flipping it default-on for the VM ablation needs any cross-node / oracle pending-parity check (the postgres-oracle / 928-matrix) re-baselined — outside this checkout's lib suite. `QUEEN_RAFT_BATCH_COUNTERS` stays default-on (transparent, digest unchanged). | Accepted, decision owed to the coordinator before default-on: re-baseline the pending-parity oracle, or keep off for the ablation and measure knob 1 vs 0 on the VM. | **for the coordinator / re-baseline before default-on** |
| R-128 | PERF-F | **The bucket count is node-local, not replicated — one fsync/entry (buckets=1) is measured on the laptop as write/fsync counts, not latency; the strace + histogram pass is VM-owed (accepted).** `QUEEN_RAFT_BUCKETS` folds the planner's 0..256 logical bucket into a node-local count via an exact divisor fold (`(h%256)%n==h%n`), pinned per-tree in `seg/MANIFEST`; it is never in an effect or digest, so nodes may run different counts without divergence. On the laptop the win is shown as durable-point fsync files and `write()`s per entry (C1000 251→1 fsync, 5.0→1.0 writes at buckets 1); `strace -c` and the `queen_raft_apply_segment_seconds` histogram over the 60s goload A20k/C1000/FAT100 smokes are Linux-only and belong to the VM pass. Few buckets trade the fan-out fsync cost for coarser retention/compaction granularity (documented in the module doc). `rsm/tests/replicator_crash.rs` took a one-line mechanical `nbuckets` field add forced by the Options struct change (not a PERF-owned planner file). | Accepted, not a defect: the fold is exact and idempotent, the count is node-local and manifest-pinned, `apply.rs` product code untouched. The retention/compaction implication of very few buckets and the default (16) are the coordinator's to weigh against the VM ablation of `QUEEN_RAFT_BUCKETS` 256/16/1. | **for the coordinator / VM ablates buckets + reads latency** |
| R-129 | PERF-E | **Dedup authority is the txns rows + the RAM bloom; `QUEEN_RAFT_DEDUP_INDEX` must be stable per store and identical across nodes, and orphaned `Dedup` rows are left in place (accepted / follow-ups owed).** Under the default `txns`, `dedup::record` stops writing the per-message `(pid,hash)` index; a store written in `txns` has no `Dedup` rows, so a later `rows`-mode boot of the same store would miss duplicates, and a cross-node mode mismatch diverges the (unread) `Dedup` keyspace. Phase 1 is single-node so this is moot now — a config-stability pin owed for phase 3 (treat like PERF-F's `QUEEN_RAFT_BUCKETS`: fix per store, identical across nodes). Two more caveats: (a) a real duplicate landing deep in a large tiered generation scans a big band up to the hit (`FRONT_GEN_CAP_MAX=1<<20`), giving the warm p99 tail of 87.7 µs — the dominant new-hash shape Skips on the bloom alone; a smaller gen cap would bound the tail at the cost of more bloom checks (tuning follow-up, not done); (b) old `Dedup` rows from a prior rows-mode store are never written or read under txns and are left in place — a reclaim sweep is owed. | Accepted, not a defect on the single-node phase-1 path: the txns rows are already replicated effects and the bloom is planner-only; `record`'s apply signature and I1/I2 are unchanged. Owed to the coordinator: (1) pin `QUEEN_RAFT_DEDUP_INDEX` per store / cross-node before any multi-node run; (2) a `Dedup`-row reclaim sweep; (3) optional gen-cap tuning for the warm p99 tail. | **for the coordinator / config pin + reclaim sweep owed** |
| R-130 | PERF-4 / round-2 review | **The round-2 A20k push-latency A/B was not consume-matched, and the new-dedup consume rate is unexplained (report-framing correction + one owed short A/B — not a product defect).** (a) In the 60 s A20k measure, new-b1 popped **218 k (~3.6 k/s)** in the same push window where "old" popped **552 k (~9.2 k/s)**, so new-b1's push p99 was measured under ~2.5× lighter apply contention — the first draft's "A20k beats postgres, p99 0.61× pg" headline is **overstated**. The consume-matched, fully-drained profile run reads push **p99 29.82 ms = 1.10× pg 27.01** → **A20k p99 is at parity with pg, not under it**. Both figures clear the 2× proceed gate, so `proceed=false` (on C1000) is **unchanged**. (b) The low new-b1 pop rate is **flat across all three windows** with elevated empty polls (222 k vs old 181 k) — the mechanism is identified (the `DEDUP_INDEX=txns` planner does a txns-window scan on a bloom "maybe"/unseeded partition, PERF-E) but whether it is warm-up or a steady-state regression is **NOT settled**. (c) "Best bucket = 1" is a single-run claim: b=1 and b=16 are co-best within log2 noise; only C1000 is monotonic (b=1 165 < b=16 218 < b=256 297). **Corrected in `PERF-4-short.md`** (no product code changed — the dedup/NBUCKETS/counter code is sound). | Accepted, measurement-validity correction, not a defect. What stands corroborated: durable-point collapse 268→16.8 ms (pop-independent), C1000 1073 ms upstream diagnosis, FAT 3×, `proceed=false`. **Owed short A/Bs before ranking round 2 a consume win:** (1) a consume-matched A20k old-vs-new-b1 push A/B; (2) a steady-state `DEDUP_INDEX=txns` vs `rows` consume A/B; (3) ≥3× repetition per bucket before ranking b=1 vs b=16. | **for the coordinator / owed 3 short A/Bs before the consume claim** |
| R-131 | PERF-H | **`QUEEN_RAFT_PENDING_TRANSITIONS` is proven correct but left OFF this round; the flip to default-on is one coordinated commit, and `has_pending` is still a facade stub (accepted, not a defect).** PERF-H proved the ready-ring re-arm path: the planner already rebuilds `Derived` from `pending` every cycle (so lease-expiry / visibility re-arm is automatic — AB-0 confirmed, no consume regression), the transitions-path append maintenance now never under-arms a delayed/leased/appended-under-lease partition (the OFF path overwrote), and the previously-unwired live-ring promotion is wired (O(1)-guarded ring-deadline index, promote at each entry boundary). ON is claimability-equivalent to SQL, cadence-independent, rebuild==live-rings and crash-safe. It is **not** made default because ON moves the replicated `pending` digest (earliest vs last `ready_at`): flipping `ApplyConfig::default()` alone makes `replicator_crash`'s child (default=ON) diverge from its reference (`run_workload`→`cfg()`=OFF). Two owed items: (1) the default-on flip must set `ApplyConfig::default()` **and** the shared `tests/apply.rs::cfg()` (used by the planner/batcher/replicator suites) in lockstep, in one commit; (2) `has_pending` (`facade/real.rs`) is still a stub returning `Ok(true)` — the correct read is exposed at the state layer (`Committed::has_claimable_pending`), a later WP wires the facade to the promoted live ring for the §9.5 long-poll gate. | Accepted, not a defect: the lever is fully proven and knob-gated (default unchanged), apply stays deterministic (**I1/I2/I4/I5/I15** intact). Owed to the coordinator: (1) the single coordinated default-on commit (`default()` + shared test `cfg()` together); (2) the facade `has_pending` wiring WP. | **for the coordinator / one coordinated flip + facade wiring owed** |
| R-132 | PERF-I | **The `CLAIM_FROM_RING` bounded claim does not yet reach 'zero store reads per claim', and the canonical end-to-end throughput A/B is owed on the VM (accepted, not a defect).** PERF-I makes the txns scan O(claimed) instead of O(history) and is effect-identical (50-seed differential 0 divergences, auto-fallback on conflation / front gap / no live segments; conflation stays on the `segs_from` path by design). Two open items: (1) the planner still does 2 POINT reads (partition, cursor) + 1 bounded scan per claim; eliminating the point reads needs the planner to consume a persistent apply-maintained `Derived` (committed/tail/lease/versions), but `batcher.rs` (outside this WP's ownership) rebuilds `Derived` from `pending` every plan cycle — that plus part-2 (record delivered RANGE not hashes; facade attaches delivered hashes to the pop outcome + ack path, moving the replicated CursorRow/ack wire, re-gated in lockstep) is a separate coordinated change. (2) The end-to-end A/B is confounded on the laptop (fsync-bound `durable_point` p50 67 ms, +-40% closed-loop noise) and the local goload is an older `-mode max` closed-loop build lacking `-rate/-ramp-sec`, so the rate-3000 C1000 regime needs `/root/goload` openloop on the VM (VM not touched this run). Plan-stage wins are clean (C1000 plan p50 1.049->0.524 ms, `arrival_to_proposed` p99 16.78->8.39 ms). | Accepted, not a defect: knob-gated (default on), effect-identical with fallback, **I1/I2/I4/I5** intact. Owed to the coordinator: (1) the ring-entry-facts / delivered-range-to-facade change to drop the per-claim point reads (batcher + facade + wire, one coordinated gate); (2) the VM openloop end-to-end C1000 A/B on `CLAIM_FROM_RING`. | **for the coordinator / owed the point-read elimination + VM A/B** |

---

## Measurements

### M0 — raft branch baseline (WP-0.1)

**Date** 2026-09-17 · **commit** branch `raft` @ `fc71b65b` (no change to any
source that enters the build) · **host** MacBook, Apple M4, 10 cores, 24 GiB,
macOS 15.5, APFS · **toolchain** `rustc 1.94.0`, `cargo 1.94.0`. The crate
declares `rust-version = "1.88"`; no 1.88 toolchain is installed on this host,
so MSRV was exercised in the spikes instead (`cargo +1.88 check`, S1/S2/S3).

This is a **laptop** baseline: build time, binary size and unit-test counts on
the branch base. Every performance number quoted comes from the Linux VM.

| check | command | result |
|---|---|---|
| release build | `cd server && cargo build --release --bin queen` | `real 4m30.699s`, exit 0, 0 warnings, **warm target dir** (one `Compiling` line). Binary `server/target/release/queen`, **9 871 072 B** (9.4 MiB). Re-run: no-op in 0.38 s |
| unit tests | `cd server && cargo test --lib` | `running 656 tests` → **649 passed, 0 failed, 7 ignored**, exit 0; tests 4.85 s (2.52 s on the second pass), 47.05 s of it compiling the debug profile |
| PG-gated tests | `docker run --rm -d --name queen-raft-pg-wp01 … -p 5481:5432 postgres:16-alpine` + `QUEEN_EMBEDDED_TEST_PG=localhost:5481 cargo test --lib -- --ignored` | **7 passed, 0 failed**, 10.87 s. Container removed; the `pg18-*` containers of other sessions untouched |

A from-scratch build was **not** measured: `df -h /` reported **10 GiB free of
460 GiB** and `server/target/release` alone is 626 MB. **Standing constraint on
this laptop**: no parallel target dirs, no large fixtures; heavy work goes to
the VM. Not in this baseline: the 35 integration tests under `server/tests/`,
the client suites of `test/run.sh`, the facade lanes.

### M1 — postgres-class baseline on the Linux VM (WP-0.2)

**Date** 2026-09-17 13:15:38–13:19:46Z · **host** `root@164.90.215.224`
(`queenpgless-01`), Ubuntu 24.04, 8 vCPU, 15 GB, ext4, local PostgreSQL 16.15 ·
**broker** one `queen` on :6698 (md5 `d7f5931486c9`), postgres class,
`DB_POOL_SIZE=64` · **loader** `/root/goload` **on the same VM**, 256-byte
payloads, manual acks · **command** `bash /root/raft/wp02/measure-baseline.sh wp02b`.

| regime | offered | achieved | push p50 | p99 | p999 | ack avg | queen cores | pg cores | goload cores | queen RSS | pg PSS | errors |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| A20k | 20 000 msg/s | 749 830 / 40 s | 9.15 ms | 27.01 | 52.48 | 9.06 ms | 0.93 | 3.01 | 1.79 | 55 MB | 1 211 MB | 0 |
| A50k | 50 000 msg/s | 1 873 990 / 40 s | 23.17 | 75.26 | 103.94 | 32.92 | 1.17 | 2.65 | 2.41 | 105 MB | 1 384 MB | 0 |
| B1 | 2 000 msg/s | 59 995 / 30 s | 3.34 | 14.27 | 35.07 | 1.72 | 0.84 | 2.45 | 0.86 | 109 MB | 1 427 MB | 0 |
| C1000 | 3 000 msg/s, 1 000 partitions | 85 513 / 30 s | 6.62 | 33.02 | 54.02 | 21.36 | 0.91 | **3.86** | 0.92 | 82 MB | 1 481 MB | 0 |
| D1 | 500 msg/s | 9 999 / 20 s | 1.99 | 4.08 | 18.30 | 0.79 | 0.28 | 0.78 | 0.31 | 54 MB | 1 490 MB | 0 |
| FAT100 | 300 000 msg/s push-only | 17 241 800 / 60 s | 23.17 | 209.92 | 415.74 | — | 1.38 | 1.39 | 3.31 | 255 MB | 1 540 MB | 0 |

`shed=0` in every regime; `grep -ciE ' error|panic'` over the broker log =
**0 lines for the whole run**. The 1.6.0 binary reproduces Appendix G's
postgres column: p50 within 1.4 ms everywhere, p99 **lower** in all five
regimes. **Postgres is the CPU**: 3.86 cores to move 3 000 msg/s at C1000, i.e.
the cost follows partition cardinality, not the write rate (G-3, I8).

Fat-batch calibration (20 s each): 200k flat, **300k sustained**, 400k over the
knee (p50 drifts 17 → 241 ms), 500k saturated (loader sheds 12.8%). 300k is
**not a broker ceiling**: at that rate goload 3.31 + queen 1.38 + pg 1.39 =
6.08 of 8 vCPU, the loader being the largest consumer. Repeatability across two
runs: A20k 9.15/9.15, A50k 21.12/23.17, C1000 **5.28/6.62** — treat C1000 p50
as ±1.5 ms.

### M2 — S1 store engine (WP-0.3)

VM `queenpgless-01`. **Legacy shape** (one store write txn per applied entry,
`segments` in the store, no dedup keyspace), 90 s cells, batch 10, 512 B,
4096 partitions, durable point 1 s. **Ratified shape** (§11.3 batched 4 ms /
256 entries, no `segments`, random-key `dedup`), 40 s cells, 2026-09-18.

| | redb 2.6.3 | fjall 2.11.2 | heed 0.22.1 |
|---|---|---|---|
| achieved 20k / 50k / 100k — legacy | 10 046 / 10 260 / 9 909 | 20 000 / 49 905 / **67 641** | 20 000 / 48 783 / 58 958 |
| achieved 20k / 50k — **ratified** | **19 466 / 25 895** | 20 000 / 49 019 | 20 000 / 47 425 |
| kernel write amplification — legacy → ratified | 29.3× → **8.66/7.88×** | 1.35× → **1.33×** | 2.80–3.30× → 3.91/3.00× |
| store commit p50 / p99, ratified 20k | 3.52 / 32.26 ms | **0.61 / 9.47** | 0.66 / 11.78 |
| engine's own durable commit, ratified 20k | ≈336 ms | **≈17 ms** | ≈139 ms |
| RSS max, ratified 20k / 50k | 260 / 261 MiB | **129 / 195** | 163 / 349 |
| KV get p99 / prefix list p99, ratified | 21 / 40 µs | 31 / 126 | **4 / 13** |
| ordered scan (`seg_loc`), ratified | 7.1–7.4 M rows/s | 2.1–2.7 M | **41.5 M** |
| consistent export (§11.6) | 181–265 MiB/s | 91–123 MiB/s | **524–556 MiB/s** (`mdb_env_copy`, always O(state)) |
| incremental export (I8 prefers it) | no | **yes in principle** | **no** |
| reopen clean / after `kill -9` | 1–2 ms / 57–465 ms | 3–5 / 234–967 | **0–2 / 0–70** |
| space given back | `compact()` 855→146 MiB, **843 ms stop-the-world** | `major_compact` 55→40 MiB, **12.3 s in the soak** | never in place; compacting copy 116–305 ms |
| RSS over a 10-min soak at 50k | not run | **4 → 986 MiB, still rising** | **not run** |
| build deps (C-2) / MSRV (C-3) | pure Rust / 2.6.3 is the newest on 1.88 (3.1.3 → 1.89, 4.x → 1.90) | pure Rust / 2.11.2 declares 1.76 (3.x → 1.90) | `cc` compiles 2 C files, **no cmake** / declares none, builds on 1.88 |
| maintenance (2026-09-17) | 100+ commits/3 mo, one maintainer, 2.x EOL | 40+33 commits/3 mo, one maintainer, young | **0 commits/3 mo**, frozen C engine, Meilisearch team |

**Mandatory criterion (I11).** `kill -9` during non-durable commits, 15 runs
each; dropped unflushed writes (dm-flakey), 5/5/10 runs.

| engine | kill -9 | dropped writes | reopens past its durable point | verdict |
|---|---|---|---|---|
| redb | 15/15 PASS | **5/5 PASS** | **never** (0/15, 0/5, 0/10 on macOS) | **PASS, by construction** |
| fjall | 15/15 PASS | **1/5 PASS, 4/5 FAIL** | yes: 14/15 kill, 4/5 flaky | **FAIL** |
| heed | 15/15 PASS | 9/10 PASS, **1/10 would not reopen at all** | after kill -9 yes (12/15); after dropped writes never (0/9) | **PASS with a caveat** |

Two facts belonging to the design, not to any engine: the durable point fsyncs
**293–484 files** (all 256 buckets every second plus rollovers) at ≈1.2–1.8 ms
each = **340–580 ms of every second** for all three engines; and **nothing
reached 100k msg/s** in a single apply thread (68k fjall, 59k heed, 10k redb in
the legacy shape). Extra refutation cells: NOMETASYNC 17 309 msg/s, WA 7.04×,
store commit p99 **123 ms**; heed reads `MDB_NOTLS` 2.06/1.97/2.08 M gets/s at
1/4/8 threads vs TLS 2.10/8.18/**9.96** M.

### M3 — S2 dedup (WP-0.4)

VM, 600 s cells, 360 s dedup / 432 s txns window, 50 000 msg/s offered.
Exactness over the four campaign cells: in-window duplicates **350 283
detected, 0 missed, 0 wrong original offset**; 19 348 out-of-window, **0
falsely reported**; **3 578 349** ack-by-hash resolutions below the cursor, **0
unresolved**, >99.99% with the segment already deleted by retention. The
planner overlay caught 188–266 same-entry duplicates per cell that committed
state could not yet see.

| at the rate each achieved → | (a) a-heed **37 565** | (a) a-fjall **39 855** | (b) b-heed **24 036** | (b) b-fjall **21 294** |
|---|---|---|---|---|
| probe p50 / **p99** | 0.013 / **0.024 ms** | 0.021 / 0.084 | 0.070 / **0.575** | 0.074 / 0.767 |
| ack-by-hash p50 / p99 (1 hash/call) | 0.002 / 0.003 | 0.029 / 0.167 | 0.423 / 0.767 | 0.591 / 1.055 |
| store on disk / live records | 3133 MiB / 17.6 M | 1570 / 19.2 M | **177 / 722 k** | **147 / 608 k** |
| **total disk per message** | 161.8 B | 84.8 B | **49.5 B** | **50.7 B** |
| store ops/s | 113 078 | 118 273 | **17 679** | **15 754** |
| RSS max | 3156 MiB | 1370 | **290** | 986 |
| kernel bytes written | 94 785 MiB | 14 364 | 19 943 | **2 907** |
| durable point mean (1/s) | 1227 ms | 101 | 185 | **78** |
| ready after a restart | **20 ms** | 1848 | 212 | 4005 |

Refutation cells (VM, 120 s, growth phase, light ack load): `a` 49 715 msg/s,
probe p99 **0.018 ms**, 171.4 store B/msg, RSS 985 MiB · **`a-lean`** 49 664,
p99 **0.019**, **119.0** store B/msg, RSS **689**, rebuild 36 ms / 0 wrong of
4096 · `b` 49 930, p99 0.431, 23.8 store B/msg, RSS 221, rebuild 203 ms / 0
wrong of 1793. Ack batching (laptop, `--ack-batch 1 → 10`): 154 114 → **334 150**
hashes resolved, 0.415 → **0.060 ms** per hash, 28 886 → **44 410 msg/s**.
Durable-point cadence sweep (VM, option (a) on heed): 250 / 1000 / 4000 ms →
**0.605 / 0.307 / 0.137 s of sync per wall second**, 16 413 / 13 559 / 7 774 MiB
of kernel writes, 46 275 / 49 715 / 49 557 msg/s achieved.

Two harness defects found and fixed here: `MDB_BAD_VALSIZE` (option (a)'s prune
walked its expiry index from an empty key; LMDB rejects a zero-length key), and
a rebuild sample-selection bug that made `wrong=76..302` look like a loss of
exactness — proved to be the harness (258/258 and 459/459 wrong samples had a
younger occurrence; after the fix 258 → 0 and 459 → 0).

### M4 — S3 consensus (WP-0.5)

VM, 3 node processes + driver on one host over loopback (scenario 5: four),
fdatasync ≈ 1 ms, every entry byte written six times per cluster. 7 scenarios
in **11 min 41 s**; refutation pass (scenarios 8–9 + the `save_committed`
sweep) in 6 min.

| # | what | measured |
|---|---|---|
| 1 | commit latency, 64 KiB, **1 in flight** (D4) | service p50 **3.0 ms** (p99 4.4–5.3); achieved 200/200, **307**/500, **306**/1000 per second ⇒ **~307 entries/s = 19 MiB/s**. The 1000/s point was cut at 38 217 of 60 000 offered |
| 1b | the same, 16 in flight | 201 / 501 / **974** per second; service p50 11–13 ms ⇒ **61 MiB/s** is the wall on this disk |
| 2 | `kill -9` the leader under load ×3 | new leader **3087 / 3300 / 3353 ms**; first committed write +3–7 ms; **0 acknowledged writes missing** (2067 / 2043 / 2051 ids checked on both survivors). The election itself is ~5 ms; detection dominates |
| 2b | + a follower SIGSTOPped 5 s | commit p50 3.20 ms during the freeze (3.30 before); the resumed follower replayed 516 entries (~33 MiB) in **460 ms**; then kill → 3048 ms, 0 missing |
| 3 | `transfer_leader` ×5 healthy | role change p50 **28 ms**, first committed write p50 31 ms, 5/5 |
| 3b | transfer to a dead target | `trigger()` returns `Ok(())`; source reports `is_leader = true` from t=0 while refusing writes; **3357 ms with no committed write** |
| 4 | linearizable reads | barrier p50 **0.16 ms @1000/s**, **0.21 @5000/s**; §9.4's fixed 2 ms window **1.28 / 1.47 ms** p50, max 58.6 ms — a **7–8× pessimisation**; coalescing 0.16 / 0.23 ms |
| 5 | 1 GiB manifest snapshot | fill 73 MiB/s; build 0.2 s; transfer 2.6 s = 398 MiB/s; learner killed mid-transfer → 53 of 64 files re-sent, total 3.4 s. **Measures the spike's own transport, not a Queen snapshot** (no store export) |
| 6 | wiped voter | wrong way: **silent**, writes keep committing (the promised panic is a `debug_assert!`). Right way (remove → generation 2002 → learner → promote): caught up in **51 ms** |
| 7 | graceful restart ×3, `enable_leader_restore=false` | all 2000 acknowledged writes present on all three; leader again after 1656 ms. Says nothing about a crash |
| 8 | `kill -9` a **follower** ×10 + 10 dm-flakey rounds | log reopened **exactly at the leader's matched index in 20/20**; read-back 100%; 0 vote reversions; 0 acknowledged writes missing; restart→answer 433 ms p50, catch-up 213 ms p50 |
| 9 | one-way partition (GH#2080's mechanism) | **0 writes committed for the whole fault**, term never moves, **a client write hangs — no answer after 10 s**; `last_quorum_acked` ages 901 → 12 125 ms; recovery: heal **616 ms**, `kill -9` the stuck leader **3528 ms** |
| 10 | `save_committed` cost, 64 KiB, 16 writers, 500/s | `none` p50 12.37 ms · `buffered` 12.56 · **`fsync` 14.75 (+2.4 ms, +19%)**, achieved 500/s in all three. `wait_for_recovery` 944–1055 ms after a 3-node restart |

Open issues re-read 2026-09-18: **#2080 OPEN** (mechanism now reproduced);
#2088, #2091, #2095, #2085 CLOSED; no open issue labelled `C-bug`. Three
commits landed on openraft main after the pin, all tests or docs.

### M5 — S4 transport (WP-0.6)

VM, two independent 60 s passes × 15 configurations + four refutation cells.
**8 659 953 commands forwarded, `cmds_bad = 0` in all 38 rows**, achieved =
offered to 0.005%. Mean of the two passes, offered 50 000 msg/s:

| configuration | p50 µs | p99 µs | leader µs/msg | recv µs/msg | B/msg | live conns |
|---|---|---|---|---|---|---|
| framed TCP | 62.5 | 112.5 | **1.26** | 2.63 | **294.2** | **2** |
| framed TCP + per-frame MAC | 125.0 | 201.0 | 2.47 | 4.30 | 297.4 | 2 |
| framed TCP + TLS | 68.0 | 120.0 | 1.37 | 2.77 | 297.1 | 2 |
| HTTP/1.1 | 70.5 | 131.0 | 2.15 | 4.10 | 309.7 | 15 / 49 |
| HTTP/1.1 + MAC header | 103.5 | 174.5 | 3.84 | 5.72 | 314.9 | 12 / 18 |
| HTTPS + MAC header | 112.5 | 193.5 | 4.22 | 6.15 | 319.3 | 19 / 13 |
| **HTTPS, no MAC** (2026-09-18) | 109 | 183 | **3.33** | 5.87 | 314.1 | 64 / 198 |
| **HTTP, pool capped at 2** | 128 | 224 | **5.47** | 9.40 | 309.7 | 2 / **218 695** |
| framed TCP, same-day control | 76 | 130 | 1.42 | 3.28 | 294.2 | 2 |

Connection sweep, framed @50k: 1 / 2 / 4 / 8 sockets = **0.63 / 1.26 / 1.65 /
1.89 µs/msg** leader, p50 71.5 / 62.5 / 61.0 / 60.5. Handshake per connection:
framed+HMAC 0.17–0.33 ms, framed+TLS 0.80–0.85, HTTP first request 0.16–0.20,
HTTPS 0.36–0.41. Crypto per ~2918 B command: MAC +12.1 µs = **0.24 GB/s**
(software SHA-256, **no `sha_ni` on either host**); TLS +1.10 µs = 2.65 GB/s
(hardware AES-GCM). At 20k msg/s the per-message CPU roughly doubles everywhere
(fewer frames per coalesced write): framed 2.24, HTTP 2.88, framed+MAC 3.41,
framed+TLS 2.51, HTTPS-no-MAC 4.63 µs/msg.

### M6 — raft1 message path on the VM (WP-1.11)

**Date** 2026-09-18 · **host** `root@164.90.215.224` (`queenpgless-01`), Ubuntu
24.04, 8 vCPU, 15 GB, ext4 · **broker** `queen` release from `raft` @ `a04a3abb`
(`rustc 1.98.1`, md5 `be80d860b4a1`, byte-identical before/after the reverted
diagnostic edit), `QUEEN_STORAGE=raft`, LocalReplicator, **no Postgres** ·
**loader** `/root/goload` md5 `2c97cccb0d1e` — the exact WP-0.2 loader, so the
columns line up under M1. Both classes create queues with `dedup_window=3600`
and retention off, so the footprint comparison is not confounded.

**Read every regime row as `QUEEN_RAFT_PIPELINE=1`, throughput-capped — NOT the
O14 comparison, which F-1 blocked at this commit (`a04a3abb`, pre-fix; the
pipeline=4 O14 verdict is M7).** At pipeline=1 a single in-flight entry
serialises the node: push commits fast (p50 competitive) but the tail explodes
and consume cannot keep up. Postgres columns are M1 (same VM + loader).

| regime | raft1 p50 | pg p50 | raft1 p99 | pg p99 | raft1 ack | pg ack | pushed / popped in window | queen cores | queen RSS | disk MB/s | shed |
|---|---|---|---|---|---|---|---|---|---|---|---|
| A20k  | **6.18** | 9.15 | 175.1 | 27.0 | 7.04 | 9.06 | 749 890 / 367 430 | 1.12 | 253 MB | 63.3 | 0 |
| A50k  | 148.5 | 23.17 | 1105.9 | 75.3 | 93.5 | 32.9 | 1 828 350 / 959 070 | 1.50 | 1013 MB | 117.9 | 0 |
| B1    | 3.73 | 3.34 | 68.1 | 14.3 | 3.56 | 1.72 | 60 001 / 37 495 | 0.93 | 899 MB | 34.7 | 0 |
| C1000 | 13.12 | 6.62 | 138.2 | 33.0 | 16.4 | 21.4 | 85 008 / 68 373 | 1.25 | 885 MB | 30.9 | 0 |
| D1    | 3.28 | 1.99 | 24.96 | 4.08 | 2.67 | 0.79 | 9 999 / 3 552 | 0.76 | 829 MB | 18.0 | 0 |
| FAT100 (push-only) | 5275.65 | 23.17 | 7897.1 | 209.9 | — | — | 4 340 800 / 0 (sheds 12.5 M) | 1.02 | 2995 MB | 199.7 | 12.5 M |

raft RSS is the store itself (payload + the uncapped dedup index + framing +
the LMDB mmap): 0.9–3.0 GB in one process, against the postgres split of
~55–255 MB queen + ~1.5 GB Postgres. C1000's 885 MB for 85k messages is R-105's
per-frame active-file RAM (1000 single-message partitions). Consume at
pipeline=1: A20k popped 367k of 750k pushed in 40 s ≈ **9k msg/s**, A50k ≈
**24k msg/s**; FAT100 tops at **≈72k msg/s ≈ 24 %** of the 300k postgres knee
(< O14's 70 %). `shed=0`, `pushErr=0` throughout at the raised fd limit.
**`0` poison lines this run.**

**Dropped-unflushed-writes durability — PASS** (discharges R-106/R-111/R-114):
20/20 dm-flakey rounds, ~73k acknowledged messages, every one delivered after
reopen, `missing=0`, 0 error lines; per-cadence 100/500/1000/60000 ms all
clean. Adversarial round (nothing durable-pointed): store reopens `applied=0`,
the fsync'd log `replayed=38`, 3000/3000 acks recovered from the log.

**Flatness / O19 at pipeline=1: INCONCLUSIVE** — the one firm signal is that
resident memory scales with stored count; the pipeline=4 verdicts are in **M7**.
**F-1 (R-117):** `QUEEN_RAFT_PIPELINE=4` poisoned the node under load (I5) at this
commit; fixed at `f8bfa672` and discharged on the VM (M7 / NIGHT-A/B/C). **F-2
(R-119):** ~304 open data-file fds (256 buckets + log + store) + one socket per
connection cross `ulimit -n 1024` at A50k → `EMFILE`; the harness runs
`ulimit -n 262144`, the phase-3+ deploy unit needs `LimitNOFILE`.

---

### M7 — raft1 message path at pipeline=4 on the VM (WP-1.11 night run)

**Date** 2026-09-18/19 · **host** `root@164.90.215.224` (`queenpgless-01`), Ubuntu
24.04, 8 vCPU, 15 GB, ext4 · **broker** `queen` release from `raft` @ `29bc7cdd`
(the F-1 fix `f8bfa672` is in the binary), `rustc 1.98.1`, md5 **`cdc5bb59af27`** —
**differs from M6's `be80d860b4a1`** (the fix is in the binary) · `QUEEN_STORAGE=raft`,
`QUEEN_RAFT_PIPELINE=4`, LocalReplicator, no Postgres, `ulimit -n 262144`, dedup
3600 s, retention off · **loader** `/root/goload` md5 `2c97cccb0d1e` — the exact
WP-0.2 / M1 / M6 loader. **Like-for-like:** the A20k/A50k/B1/C1000/D1/FAT100 goload
flags are byte-identical between `measure-raft1.sh` and `measure-baseline.sh`; the
binary differs only by storage class. Sources: `NIGHT-A-pipeline4.md`,
`NIGHT-B-dedup.md`, `NIGHT-C-flaky.md`; raw under `night/`.

**Six regimes at pipeline=4 (raft1) vs postgres (M1) and pipeline=1 (M6):**

| regime | raft p4 p50 | pg p50 | raft p4 p99 | pg p99 | raft p4 p999 | raft p1 p50 (M6) | queen cores | queen RSS | disk MB/s |
|---|---|---|---|---|---|---|---|---|---|
| A20k  | **6.24** | 9.15 | 162.82 | 27.01 | 197.63 | 6.18 | 1.48 | 254 MB | 72.1 |
| A50k  | **19.58** | 23.17 | 444.42 | 75.26 | 518.14 | 148.48 | 1.72 | 839 MB | 131.7 |
| B1    | 4.45 | 3.34 | 36.61 | 14.27 | 85.50 | 3.73 | 1.44 | 843 MB | 49.5 |
| C1000 | 238.59¹ | 6.62 | 749.57¹ | 33.02 | 831.49 | 13.12 | 1.33 | 908 MB | 43.5 |
| D1    | 4.26 | 1.99 | 40.19 | 4.08 | 54.02 | 3.28 | 1.03 | 901 MB | 25.5 |
| FAT100 (push-only) | 4079.62 | 23.17 | 6782.98 | 209.92 | 7045.12 | 5275.65 | 1.42 | 3238 MB | 250.5 |

¹ **C1000 confound:** the six regimes share one broker sequentially; A50k left a
1.72 M pop backlog draining into C1000, so 238.59/749.57 are **not** a clean fan-out
datum. The cleanest same-binary/same-pipeline C1000 is the fresh-store flatness run
below: **p50 9.79 = 1.48× pg 6.62, p99 536.58** — still O14 FAIL, but not 36×.

**O14 verdict per target (Appendix G):**

| line | raft p4 | pg | verdict |
|---|---|---|---|
| A20k p50  | 6.24 | 9.15 | PASS (0.68×) |
| A20k p99  | 162.82 | 27.01 | **FAIL** (6.03×) |
| A50k p50  | 19.58 | 23.17 | PASS (0.85×) |
| A50k p99  | 444.42 | 75.26 | **FAIL** (5.91×) |
| C1000 p50 | 9.79 (fresh) | 6.62 | **FAIL** (1.48×) |
| C1000 p99 | 536.58 (fresh) | 33.02 | **FAIL** (16.2×) |
| FAT100 tput | ≈90 043 msg/s ≈ **31%** | 287 363 msg/s | **FAIL** (< 70%) |

**O14: FAIL.** The pipeline lifts A50k enormously over pipeline=1 (p50 148→20,
p99 1106→444) and keeps push p50 ≤ pg on the A-regimes, but the **p99 tail never
comes within range of pg**, **C1000 fan-out** does not isolate, and **FAT100**
stays far below the knee. A 5-min sustained A20k (no pop-lag masking) is worse than
the burst — p50 44.29 / p99 419.84 — same FAIL, more honest.

**Flatness (I8) at pipeline=4** (8.14 M preload, 1000 partitions, 6-min runs):

| metric | A20k Δ | C1000 Δ | gate |
|---|---|---|---|
| p50 | +8.1% PASS | **+545.9% FAIL** | ±15% |
| p99 | +5.4% PASS | **+15.3% FAIL** | ±15% |
| p999 | **+155.5% FAIL** | +4.5% PASS | ±15% |
| ack_rtt | +3.1% PASS | **+121.6% FAIL** | ±15% |
| cpu_cores | 0.0% PASS | −5.5% PASS | ±15% |
| disk_mbps | +2.9% PASS | +6.7% PASS | — |
| RSS drift in-run | 8→1520 / 3003→4279 **FAIL** | 8→1067 / 4250→4718 **FAIL** | ±5% |

**I8: mixed → FAIL on the strict gate.** CPU and disk ARE flat with store size
(G-3 holds); the A20k median/p99 are isolated from an 8 M cold store; but the p999
tail, the C1000 fan-out latency and RSS in every shape are not. At rest ~387 B/msg
(R-105). **Provenance caveat:** `flatness-night.sh` is not committed; the surviving
`.results`/`compare-*.txt` carry the literal `pipeline=1` label the pipeline sed
missed (the binary is the fixed `cdc5bb59af27`), so pipeline=4 is the agent's note —
mechanically consistent (empty-A20k p50 56.58/p99 452.6 matches the committed
pipeline=4 `fivemin.sh` 44.29/419.8) but not independently checkable from committed
artifacts.

**Noisy neighbour (O19) at pipeline=4, real active DLQ storm** (`dlqstorm.py`:
wildcard queue-mode pop + ack `status:"dlq"`; quiet = 3000 msg/s, 200 partitions,
48 consumers):

| phase | quiet p50 | quiet p99 | Δ p99 vs before | verdict |
|---|---|---|---|---|
| before (empty) | 9.28 | 87.55 | — | baseline |
| during (463/s storm, 45 350 filed) | 13.38 | **166.91** | **+90.6%** | **FAIL** |
| after (2 M at rest, storm stopped) | 15.30 | **134.14** | **+53.2%** | **FAIL** |

**O19: FAIL.** A light real DLQ storm on one partition nearly doubles the cold
partitions' p99, and the at-rest 2 M backlog leaves them +53% after the storm stops.
Quiet throughput never lagged (≈265 k every phase). **Reproducible from committed
raw** (`night/quiet-{before,during,after}.gl` finals; `storm.out` 45 350/98 s =
463/s), superseding the WP-1.11 transcription-error cell.

**Dedup at the 3600 s product default (NIGHT-B).** The A50k task run collapsed at
~min 55 (R-121) before the window could fill or expire; a supplementary
**push-only 15k / 64-min** fill (`b2`, a stated deviation) gave the window mechanism
cleanly:

| | A50k (task) | 15k push-only (supplementary) |
|---|---|---|
| offered / achieved | 213.8 M / 93.5 M (43.8%) | 57.56 M / 57.56 M (100%) |
| push p50 / p99 | 6 651 / 23 200 ms | **5.41 / 481 ms** |
| index at min 60 | 15.2 GB / ~93.5 M rows | 7.89 GB / ~54 M rows |
| RSS min30 / min60 / peak | 9 991 / 10 025 / 10 872 MB | 4 162 / 7 912 / 8 392 MB |
| MemAvailable min | 8 837 MB (no OOM) | 13 664 MB |
| dedup-probe p50 / p99 | admission-bound (1.8–9.3 s/round) | **1.16 / 8.97 ms** (6 300 probes) |
| expiry @ min 60 | not reached (collapsed) | logical yes / **physical no** (R-124) |
| poison / error / MAP_FULL | 0 / 0 / 0 | 0 / 0 / 0 |

The lean option-(a) dedup probe is **~1 ms p50 / ~9 ms p99** over a filling 3600 s
window **as long as the index fits page cache** (15 GB RAM, 8 GB index here); the
>-RAM regime R-105 warns of needs an index neither run reached (50k collapsed;
15k/64 min = 57 M rows). RSS is bounded/reclaimable — the store's growth is on disk,
not anonymous RSS. Expiry is **logical only** in phase 1: a >window id re-enqueues
but nothing frees the row (R-124). WP-0.4's named 50k / 1 h probe still does not exist.

**Dropped writes at pipeline=4 (NIGHT-C, 100 rounds, dm-flakey, F-1-fixed binary, 16
partitions, A20k intensity, durable_ms cycling 100/500/1000/60000):**

| durable_ms | PASS rounds | reopen avg | reopen max | note |
|---|---|---|---|---|
| 100 | 25 | 317 ms | 319 ms | tail-only replay |
| 500 | 25 | 354 ms | 625 ms | tail-only replay |
| 1000 | 24 | 419 ms | 626 ms | tail-only (round 75 FAILED → R-123) |
| 60000 | 25 | 6 415 ms | 9 248 ms | whole-log replay, scales with push count |
| **all PASS** | **99** | — | 9 248 ms | every acked push delivered, `missing=0`, ~45.6 M acked |

**99/100 PASS** (the durability property holds, reopen 316 ms–9.2 s tracking the
replay tail); **the 1 FAIL is R-123** (SIGBUS on the torn store mmap). 0 poison / 0
`TimeWentBackwards` / 0 `MAP_FULL` across all 100 rounds.

**Harness caveat (disclosed, touches no gated number):** `noisy-night.sh`'s
broker-RSS sampler read `$QPID` from a subshell forked before the pid was assigned,
so the O19 broker-RSS side column is missing; the gated O19 numbers come from the
goload/quiet `overall` p99, which is unaffected.

### M8 — phase-1 performance package, round 1 (PERF-2 baseline + PERF-3 ablation)

**Date** 2026-09-19 08:14–12:05Z · **host** `root@164.90.215.224`, Ubuntu 24.04,
8 vCPU, 15 GB, ext4 · **binaries** PERF-2 profile @ `raft` `1a9dcf63` (md5
`a2208d19ad18`), PERF-3 ablation @ `5de30164` (PERF-C tip, md5 `9a463fcd5a51`),
`rustc 1.98.1` · **loader** `/root/goload` md5 `2c97cccb0d1e` (the M1/M7 loader) ·
`QUEEN_STORAGE=raft`, `QUEEN_RAFT_PIPELINE=4`, LocalReplicator, no Postgres, dedup
3600 s, retention off. Sources: `PERF-2-baseline.md`, `PERF-3-measure.md` (its
20-min timeline was cut short by the coordinator); CSV + folded tops under `perf/`
and `perf/perf3/`. **Round 1's store levers (PERF-A/B/C) did NOT move the O14
targets** — the durable-point LMDB `data.mdb` env-sync on the apply thread was
untouched and stayed the wall.

**PERF-2 — where the durable point spends (p50 legs, fresh store; the tail is the
durable point stalling the one apply thread, and it equals push p99):**

| regime | durable_point | seg-fsync leg | ⇒ LMDB env-sync leg | tail (arrival→proposed p99) |
|---|---|---|---|---|
| A20k | 268 ms | 67 ms (inline) | ~211 ms | 268 ms |
| A50k | 537 ms | 67 ms | ~409 ms | 537 ms (propose_roundtrip) |
| C1000 | 134 ms | 134 ms (seg-dominated) | ~94 ms | fan-out over 256 bucket files |
| FAT100 | 1074 ms | 134 ms | ~1000 ms | 1074 ms |

CPU top self: `mdb_page_flush` 4.3% (A20k) / 8.7% (A50k) / **13.2% (FAT100,
dominant)**; C1000's top is `store::keys::read_name` **10.5%** (the per-partition
store-key codec) with a read txn per partition. Cold 8 M store roughly doubles the
durable-point count and widens the tail ~6× at `max`, but A20k's median is isolated
(p50 9.0→9.9) — the cold penalty is the tail, not the median.

**PERF-3 — ablation (all levers vs each minus-one vs all-off, 75 s fresh store):**

| target | all levers | postgres | ratio | verdict |
|---|---|---|---|---|
| A20k p50 / p99 | 9.02 / 264.19 | 9.15 / 27.01 | 0.99× / 9.78× | p50 PASS, p99 **FAIL** |
| C1000 p50 / p99 | 152.58 [all-off 60.67] | 6.62 / 33.02 | 23.0× / 22.2× | **FAIL** |
| FAT100 tput | 86,107 [all-off 88,123] | 287,363 | 30% | **FAIL** (<70%) |

**Levers neutral on the gate.** PERF-A cut only the seg-fsync leg (A20k 67→33,
C1000 134→17); the LMDB env-sync leg it cannot touch (R-125) is the wall. PERF-A+C
even *regress* C1000 warm-up (all-off 60.7 ms ≪ all-on 152.6 — the continuous
pre-flush + writer pool contend with the 256-file fan-out). **The one keeper is
PERF-B:** at 6-min steady state on an 8 M-loaded store, C1000 push p50 rises only
**+49.8%** vs empty — against M7's **+545.9%** — the dedup front skips the committed
LMDB probe that paged in from the loaded store. The numbers' recommendation: keep
PERF-B on, retune/gate PERF-A, default the writer pool off, and attack the
durable-point LMDB env-sync directly → round 2's PERF-D/E/F.

### M9 — phase-1 performance package, round 2 (PERF-4 short measure + bucket knob)

**Date** 2026-09-19 ~14:38–15:00Z · **host** same VM · **binary** `queen` release @
`raft` HEAD `431171e4` (PERF-E tip; D/E/F = `1d017f7d` / `431171e4` / `e669dcc2` on
top of PERF-C `5de30164`), md5 `dc9d8fdee6d0`, `rustc 1.98.1` · **loader**
`/root/goload` md5 `2c97cccb0d1e` · `QUEEN_RAFT_PIPELINE=4`, LocalReplicator, no
Postgres, dedup 3600 s, retention off · fresh store per run, three regimes × three
bucket counts, **60 s SHORT measure only** — Alice's iterate rule: the long PERF-5
runs were **skipped** because the short measure did not pass its thresholds. Source:
`PERF-4-short.md`; CSVs under `perf/perf4/`. No product code changed in the measure;
it sets only the knobs. New config = `DEDUP_INDEX=txns BATCH_COUNTERS=1
PENDING_TRANSITIONS=1 DEDUP_FRONT=1 DURABLE_ASYNC=1 SEG_BUFFERED=1 BUCKETS=N`
(writers adaptive); "old" = the round-1 all-levers config (`DEDUP_INDEX=rows`,
counters/pending off, buckets 256).

**Round 2 demolished the durable-point wall round 1 could not move:**

| regime | metric | old (round-1) | new b=1 | new b=16 | new b=256 | pg M1 |
|---|---|---|---|---|---|---|
| A20k | durable pt p50 ms | 268 | **16.8** | 16.8 | 67.1 | — |
| A20k | push p99 ms | 203.78 | **16.51 / 29.82¹** | 22.14 | 40.19 | 27.01 |
| A20k | push p50 ms | 7.26 | **6.62** | 6.62 | 6.62 | 9.15 |
| A20k | RSS MB | 348 | **141** | 152 | 166 | 55 |
| C1000 | push p50 ms | 173.06 | **164.86** | 218.11 | 296.96 | 6.62 |
| C1000 | arrival→proposed p99 ms | 1073 | 1073 | 1073 | 1073 | — |
| FAT100 | rate msg/s | 86,107 | **265,158** | 261,470 | 243,875 | 287,363 |
| FAT100 | % of 287k knee | 30% | **92.3%** | 91.0% | 84.9% | 100% |

¹ 16.51 = the 60 s run (**under-consumed**, not consume-matched: new-b1 popped 218 k
vs old 552 k in the same push window); **29.82 = the consume-matched drained profile
run** (R-130). A20k p99 is at **parity** with pg (1.10× consume-matched), not a beat.

**O14 verdict (best bucket = 1):** A20k p50 **PASS** (0.72×); A20k p99 borderline
miss on `≤ pg` (1.10× consume-matched) but **PASS** the 2× proceed gate; C1000 p50
**FAIL** (24.9×, upstream `arrival→proposed` bound, unchanged by any store lever);
FAT100 **PASS** (92% > 70%). **`proceed=false`** — C1000 p50 fails at every bucket
count. The C1000 60 s window is consume-saturated (lag climbs, windowed p50 flat not
settling), so no long run was warranted.

**Bucket comparison:** fewer buckets = smaller seg-fsync leg; **b=256 is
consistently worst** (fan-out); **b=1 and b=16 are co-best within single-run noise**
(only C1000 is monotonic b=1 < b=16 < b=256), so the default is **16** and a ≥3×
repeat is owed before ranking b=1 strictly best. Profiles: `mdb_page_flush`
(round-1's dominant 13.3%) is **gone from the top** — FAT is now allocator-bound on
the per-message `serde_json::Value` parse (~47% in malloc/free), the next FAT lever.
**PERF-5 (the long runs — 6-min flatness, 20-min timeline, cold-`drop_caches` pass)
did NOT run.** Open follow-ups: R-130 (consume A/B owed), R-127 (pending default),
R-129 (dedup-index pin + reclaim).

### M10 — phase-1 performance package, round 3 (PERF-6 consume A/B + PERF-7 G/H/I short measure)

**Date** 2026-09-19 ~16:06–19:46Z · **host** same VM (`root@164.90.215.224`, Ubuntu 24.04,
8 vCPU, 15 GB, ext4) · **binaries** PERF-6 `queen` release @ `raft` `7e5f0d61` (round-2 tip,
md5 `af5fa8dc3608`), PERF-7 @ `f006c3d4` (PERF-I tip; G `9e1ebff8` / H `c65c8c2c` /
I `f006c3d4` on the round-2 report), md5 `f22bf1e19c3f`, `rustc 1.98.1` · **loader**
`/root/goload` md5 `2c97cccb0d1e` (the M1/M7/M8/M9 loader) · `QUEEN_RAFT_PIPELINE=4`,
LocalReplicator, no Postgres, dedup 3600 s, retention off · round-2 best config held fixed
(`BATCH_COUNTERS=1 DEDUP_INDEX=txns DEDUP_FRONT=1 DURABLE_ASYNC=1 SEG_BUFFERED=1 BUCKETS=16`).
Sources: `PERF-6-consume-ab.md`, `PERF-7-short.md`; raw run dirs on the VM under
`/root/raft/perf5` and `/root/raft/perf7` (not synced into the checkout). **60/75/60 s
consume-matched drained short measures only** — Alice's iterate rule; the long PERF-8 runs
were **not** run because the gate did not pass. No product code changed in the measure (the
G/H/I levers landed in their own commits `9e1ebff8` / `c65c8c2c` / `f006c3d4`); it sets only
knobs. Round-3 direction: (1) fix the push-p50 batcher/writer schedule, (2) re-arm the ready
ring for slow/leased consumers, (3) make the wildcard pop O(claimed).

**AB-0 (PERF-6) — the consume "regression" is NOT one; it is A20k under-consume variance,
no ring pathology.** The 2×2 `PENDING_TRANSITIONS × DEDUP_INDEX` (60 s A20k push+consume,
×2 replicates): the *identical* config `pt0-rows` popped 195 500 (3.4 k/s) in R1 and 655 450
(11.4 k/s) in R2 — a 3.35× swing with nothing changed, larger than any between-config gap;
the 2×2 pop ordering sign-flips between replicates; stage histograms are byte-identical where
pop rates differ most (`pop_read` p99 262 µs, `plan` p99 2.10 ms everywhere); all 8 runs fully
drain once push is lifted (~11–12 k/s, phase-2 popped = phase-1 lag + ~6 000). PERF-4's "txns
regresses consume" attribution is **refuted** — the pop path reads the Txns keyspace regardless
of the knob, no txns-window dedup scan runs on pop, the ring re-arms correctly (**PERF-H**: the
planner rebuilds `Derived` from `pending` every plan cycle). The one consistent knob effect is
on **push** (`DEDUP_INDEX=rows`: durable_point 134/268 ms, RSS 2×, push p99 167–183 ms — the
PERF-E wall), plus a small consistent **empty-poll** signature (txns mean 207 k > rows 184 k,
~12%, no sign flip) that leaves the drain/throughput verdict intact.

**PERF-7 — G/H/I are correctness/scaling hardening, performance-neutral-to-parity, and move
neither push-latency bound.** Round-3 defaults `DRIVER_NOTIFY=1 WRITER_PIPELINE=0
PENDING_TRANSITIONS=0 CLAIM_FROM_RING=1`:

| regime | metric | pg M1 | round-2 b16 | round 3 |
|---|---|---|---|---|
| A20k | push p50 / p99 / p999 ms | 9.15 / 27.01 / 52.48 | 6.62 / 22.14 / 50.94 | **6.05 / 17.28 / 25.22** |
| A20k | arrival→proposed p50 ms (the bound) | — | 4.19 | **4.19** |
| A20k | pop /s ph1 (drained) | — | 8.0 k | **6.3 k** (drained) |
| C1000 | push p50 / p99 ms | 6.62 / 33.02 | 218.11 / 765.95 | **211.97 / 790.53** |
| C1000 | arrival→proposed p99 ms (the bound) | — | 1073 | **1073** |
| C1000 | ph1 pushed/popped/lag @ 75 s | — | (lag ~68 k) | **220490 / 220490 / 0** |
| FAT100 | rate msg/s / % of 287 k knee | 287363 / 100% | 261470 / 91% | **260498 / 90.7%** |

**Ablation (one-knob-off on A20k and C1000) isolates nothing measurable — that is the
finding.** minus-G / minus-H / minus-I all sit within run-to-run noise of the default (A20k
p50 5.66–6.18, p99 17.0–18.6; C1000 `arrival→proposed` p99 1073 in all four). Two review
corrections fold in here: (a) the clean A20k tail (p999 25.22 vs the single round-2 run's
50.94) is **run-to-run, not a PERF-G gain** — minus-G reads p999 27.78 with the same
`writer_pickup` 0.52/2.10; (b) `CLAIM_FROM_RING=0` carries no claim-path penalty at 75 s by
the **direct** measure `pop_wildcard` **plan p99 1.05 ms, identical on/off** (not the
unreplicated 142–222 ms push-p50 spread) — a fresh 75 s store holds ~225 msg/partition, so
O(history) is as cheap as O(claimed); PERF-I's win needs an aged/large store (unmeasured here,
I2-transparent, R-132). **PIPELINE=8 (D4 datum) regresses A20k** (p50 14.02, `arrival→proposed`
p50 16.8): deeper pipeline buffers more arrivals in front of the single apply thread —
**apply-thread-bound, not slot-starved** — so PIPELINE=4 stays the right default.

**O14 verdict — `proceed=false`, same two gates as round 2, both upstream of the store:**

| target | round 3 | pg M1 | O14 ratio | verdict |
|---|---|---|---|---|
| A20k p50 | 6.05 | 9.15 | 0.66× (≤ pg) | O14 **PASS**; misses the round-3 aggressive ≤ 4 ms gate |
| A20k p99 | 17.28 | 27.01 | 0.64× (≤ pg) | **PASS** |
| A20k pop/s (drained) | 6.3 k | — | — | **n/e** on one run (drained=yes; 6.3 k inside PERF-6's 3.4–11.4 k) |
| C1000 p50 | 211.97 | 6.62 | 32.0× | **FAIL** |
| C1000 p99 | 790.53 | 33.02 | 23.9× | **FAIL** |
| FAT100 tput | 260 498 | 287 363 | 90.7% | **PASS** (> 70%) |

A20k passes O14 strict (p50 AND p99 ≤ pg); it misses only the round-3-invented aggressive
4 ms/9 k gate that sits on work round 3 did not do. C1000 fails O14 on the same upstream bound
as round 2. **What still bounds each symptom, and the next lever:** A20k p50 = the ~4 ms
`arrival→proposed` batcher hold (structural to the single-plan-cycle batcher, unmoved by
G/H/I) → the propose path is next; C1000 p50/p99 = the per-partition propose serialization
(`arrival→proposed` p99 1073 ms, 1 partition = 1 propose lock) → per-partition propose
parallelism, a separate work item; FAT100 = the per-message `serde_json::Value` parse
(receiver-side JSON), the remaining throughput lever. Provenance caveats: a first
build-writeback batch (A20k push p99 1318 ms, the durable fdatasync stalling behind the build's
3.7 GB `target/` writeback) was **discarded** and every number is the settle-guarded rerun
(`sync` + `Dirty < 20 MB` before each boot); single VM co-resident loader (O13); one replicate
per cell on PERF-7 (A20k pop is 3.4–11.4 k run-to-run). 0 broker error/panic lines in every
kept run. Open follow-ups: R-130 (AB-0 answered here — non-regression), R-131
(`PENDING_TRANSITIONS` default-on = one coordinated commit), R-132 (PERF-I per-claim point-read
elimination + the VM openloop C1000 A/B).

---

## Open questions for Alice

O1–O16 are PLAN_RAFT.md §17. O17–O20 come from the 2026-09-17 head-of-line
discussion. **O21–O23 are new**, from Alice's 2026-09-17 notes (a), (b) and
(d). PLAN_RAFT.md §17 records these as answered at G0; they are repeated here
with the data that now exists, because three of them changed. **The decisions
G1 itself asks for are in the "G1 checklist — what Alice decides" above** (go /
no-go now that O14/I8/O19 are measured; R-123 and R-121, the two HIGH night
defects; R-115/R-116 parity; and the decisions the pipeline=4 evidence flags);
O14, O18 and O19 below now carry the pipeline=4 numbers (M7).

| id | question | what the data says now | recommended answer |
|---|---|---|---|
| O1 | Branch base; drop the per-queue native class (D23). | Branch `raft` @ `fc71b65b`; phase 0 touched **no product code**; pgless parked `2bbd10d1`. | As stated, with D23's amendment (main checkout, no worktree). |
| O2 | Storage mode per deployment (D1). | — | yes. |
| O3 | Hold (D13) and election timeouts (D14). | S3: failover **3087/3300/3353 ms**; the election is ~5 ms, detection dominates. GH#2080: with a leader known, the hold never fires and the write **hangs**. | Keep 8 s / 100 ms / 1000–2000 ms **and add a `propose` deadline** (plan change 7). |
| O4 | 3 pods; PVC sizes; zone anti-affinity. | Dedup at the 1 h product default is **~180 M rows / 10.6 GB logical per voter** at 50k (lean (a)). openraft cannot purge log past the snapshot, so log disk follows snapshot cadence. **NIGHT-B, measured:** a 15k sustained fill grows `data.mdb` ~130 MB/min → **7.9 GB at 3600 s**; a 50k fill reached **15.2 GB** before it collapsed. Phase 1 frees **none** of it (retention stub, R-124), so the PVC must carry the whole window until WP-2.7. | 3 pods; PVC = retained bytes × 1.5 **+ the whole dedup window + the log between snapshots**; fix the number after D-06/WP-2.7 and D-10. |
| O5 | Library: 0.10 stable, a pinned commit, or raft-rs. | No 0.10 release exists (#1637 open). raft-rs = 20–27 agent-days vs 6–9. | Pin the rev now; re-confirm at G3. |
| O6 | Store engine (S1) and dedup (S2). | See the amended D9 and D10. | heed pinned + redb for raft1/embedded; option (a) lean. |
| O7 | DLQ handoff for `dlq:true` acks inside the transaction entry. | — | yes. |
| O8 | `too_late` for timers without claims. | — | as stated; confirm with conformance. |
| O9 | Traces age limit (D18). | — | 7 days. |
| O10 | No spool (D19); maintenance-mode pushes. | — | 503 `maintenance`. |
| O11 | Migration: offline window or online copy. | — | offline first. |
| O12 | When to remove the postgres class (D22). | GH#2080 is open and reproduced; D22 is its mitigation. | Keep it deployable **to GA**; decide removal after 3 months of GA. |
| O13 | Three VMs for the final numbers. | Every number so far is single-VM with a co-resident loader (FAT100: 6.08 of 8 vCPU busy, the loader the largest share). | yes for G4; one extra VM for load generation would also clean up G1. |
| O14 | Performance targets for G1/G2/G4. | **Measured at pipeline=4, M7→M10.** M7 (round 0): p99 ~6× pg, FAT ≈31%. After the package (M8/M9/M10): **A20k p50 6.05 / p99 17.28 both ≤ pg** (O14-strict PASS), **FAT100 90.7%** of the knee (PASS > 70%), but **C1000 p50 32× / p99 24× pg** — its `arrival→proposed` ≈1073 ms per-partition propose serialization is upstream of the store and unmoved by any of the ten levers, so **O14 still FAILS on C1000**; the A20k p50 ~4 ms batcher hold is the other propose-path bound. AB-0 (M10/PERF-6) settled the consume rate as a non-regression. Failover 3.0–3.4 s. | raft1 p50/p99 ≤ postgres at A20k/A50k/C1000; raft3 p50 ≤ raft1 + 2 ms; **failover ≤ 4 s p99**; push-only fat-batch ≥ 70% of postgres. **A20k and FAT now meet it single-node; C1000 does not — the next lever is per-partition propose parallelism (not the store, not deeper pipeline: PIPELINE=8 regresses), before a re-attempt at raft3 / with a load-gen VM (O13).** |
| O15 | Replicator for embedded mode. | S1 adds: the **engine** for embedded should be redb (no snapshot source in raft1). | LocalReplicator + redb. |
| O16 | Ack fast path: record the delivered set at claim, or always compute. | S2 risk 5: narrowing what 005 must answer below the cursor would narrow (b)'s gap too. **PERF-I (round 3, M10) refined the "always compute" path** — the delivered hash set is now folded into ONE bounded O(claimed) txns pass (byte-identical, `pop_wildcard` plan p99 1.05 ms, R-132), but it is still recomputed each claim, not recorded. Moving to "record at claim" is R-132's owed **part-2** (facade attaches the delivered range to the pop outcome + ack path — moves the replicated CursorRow/ack wire, re-gated in lockstep). | Record the delivered set in the cursor at claim, bounded by batch size — the R-132 part-2 change, a coordinated decision for Alice. |
| O17 | Bounded planning time per batch. | Not measured. PLAN §17 records **5 ms**; this file previously proposed 20 ms. | Confirm the number. `QUEEN_RAFT_PLAN_BUDGET_MS`; a single command that alone exceeds the budget must still be planned, or it can never progress. |
| O18 | Per-command-kind planner metrics + a slow-command log. | **Plumbed but inert (R-120):** `slow_command_ms` has no consumer (0 lines at threshold 5 ms under p50 44 ms plans); the O17 `plan_budget_ms` IS consumed (`batcher.rs:470`). Store metrics unmapped to any endpoint. | yes: `queen_raft_plan_seconds{kind=…}` + WARN above `QUEEN_RAFT_SLOW_COMMAND_MS` (50). **Wire it (WP-2.6 / a phase-2 obs WP).** |
| O19 | Noisy-neighbour test: gate condition or advisory? | **Now measured at pipeline=4 (M7): O19 FAILS.** A real 463/s DLQ storm on one partition raised cold-partition p99 **+90.6%** (87.55→166.91); the 2 M backlog at rest **+53.2%** (→134.14) — both blow ±15%, reproducible from committed raw. The single shared replicated log + store does not isolate a hot partition's churn/volume from cold-partition latency. | **Gate at G2 (raft1) and G4 (raft3)**; a named scenario in `test/raft/flatness`. It currently FAILS single-node — a levers question for G2. |
| O20 | Move DLQ head decompression and duplicate repacking off the planner. | — | yes, receiver-side. |
| **O21** | **(a) Commit the store every N entries / few ms instead of once per applied entry** (the Raft log is already the durable WAL). | Measured, VM, 20k target: redb **10 046 → 19 539 msg/s** and WA **29.31 → 8.33×** from the cadence alone (a 2×2 isolates it from the keyspaces); fjall and heed move ±2% and ±0.6 points. The knobs in §11.3 are 4 ms / 256 entries. | **Yes — it is already the §11.3 amendment; keep it.** It re-admits redb, which is what makes D9's embedded answer possible. Consequence to accept: local reads that need the very latest entry wait ≤ 4 ms. |
| **O22** | **(b) Keep the segment index out of the store**, Kafka-style: an immutable `.qidx` per sealed segment file, the active file's index in RAM. | `segments` was the highest-rate keyspace. S1 re-ran the matrix without it (`--shape ratified`): heed's ordered-scan lead over fjall survives the change (41.5 M vs 2.1–2.7 M rows/s over `seg_loc`), and most of the export-size spread (heed 172 vs redb 58.7 MiB) was those rows. `partition_files` keeps one row per (pid, file) at seal. | **Yes — keep the §6.1 amendment.** It removes the store's highest-rate keyspace and makes `seg_loc` (node-local, D8) the only per-segment structure. WP-1.3 owes the `.qidx` format, its checksum and its rebuild-by-scan path. |
| **O23** | **(d) What the S3 in-flight result means for D4 and batch sizing.** | 1 entry in flight = **307 entries/s of 64 KiB = 19 MiB/s**; 16 in flight = **974/s = 61 MiB/s** (the disk wall). Service p50 3.0 ms at 1, 11–13 ms at 16. **A pipeline of 4 was never measured.** | Keep D4's bounded pipeline at **4**, but make the *command batcher* (§7.1) fill the entry — 19 MiB/s at 1 in flight is ~37k msg/s of 512-byte messages only if entries are full. Before the amendment is relied on: measure exactly 4 with a step-down injected while 4 are in flight (I3), D-11. If 4 does not reach the O14 targets, raise `QUEEN_RAFT_ENTRY_MAX_BYTES`/batch size before raising the pipeline. |

Alice's note **(c) — "a library for the ordered store, never a home-grown
engine"** — needs no question: all three S1 candidates are libraries behind the
`rsm/store/` adapter, and the adapter seam is what let S1 be revised instead of
rewritten. It is recorded as a constraint on D9 and WP-1.2.

---

## Deferred measurements

Every `Deferred` item of the four memos, with the command. The phase-0 runs
were shortened on Alice's instruction (10-minute soak, 6-minute dedup window,
1 GiB snapshot, fewer kill repetitions); this is what that cost.

| id | what | why it matters | command | cost |
|---|---|---|---|---|
| D-01 | `flaky.sh 20` for **all three** engines at equal n (today: 5 fjall / 5 redb / 10 heed), 60 s runs. Accept heed only if its no-reopen rate is 0/20 in the shipping configuration. | WP-0.3's exit criterion; the memo's own ratification precondition (R-01, R-02). | `cd test/raft/spikes/s1-store && sudo ./vm-campaign.sh full flaky` (the `full` profile is 20 flaky runs × 60 s and 100 kill runs per engine; `full kill,flaky` adds the kill half) | ~45 min VM (flaky) |
| D-02 | The same loop with `vm.dirty_expire_centisecs=100`, `vm.dirty_writeback_centisecs=50`, `RUN_S ≥ 120`. | The only configuration that can produce heed's dangerous case; today's 20 s runs on a VM with `dirty_expire_centisecs=3000` are biased toward the reported result. **Needs Alice**: a sysctl is outside `/root`. | `sysctl -w vm.dirty_expire_centisecs=100 vm.dirty_writeback_centisecs=50` then `sudo ENGINE=heed RATE=20000 RUN_S=120 ./flaky.sh 20` (repeat for redb, fjall) | ~1 h VM |
| D-03 | `flaky.sh 20` for heed with `MDB_NOMETASYNC`. | Its cost is known (−13% rate, WA 7.04×, store commit p99 123 ms); its crash behaviour is not, and the header's unconditional integrity claim is the only reason to pay that cost (R-05). | `sudo ENGINE=heed RUN_S=60 EXTRA="--heed-flags nometasync" ./flaky.sh 20` | ~20 min VM |
| D-04 | A **2 h soak on heed at 50k**: RSS, `data.mdb` high-water mark, scan rate at 10M+ rows, durable-point cost once the store is tens of GiB. | Both of heed's remaining pillars rest on 40–90 s cells and a 9.6 MiB fully cached scan (R-07, R-08). G-3 asks for RSS flat ±5% across 60 min. | `LEAD=heed SOAK_RATE=50000 ./vm-campaign.sh full soak` (the `full` profile's soak is 7200 s) | 2 h VM |
| D-05 | redb at ≥30 min per rate in the **ratified** shape; and whether C-3 is worth one minor version (redb 3.1.3 declares MSRV 1.89, 4.x 1.90). | redb is the proposed embedded engine; its 8.3–8.7× WA and 25.9k ceiling come from 40 s cells. | `ENGINES=redb RATES="20000 50000" DUR=1800 ./vm-refute.sh` | ~1 h VM |
| D-06 | **One VM dedup campaign at the product default**: `--window-s 3600 --txns-s 3600`, run ≥ 2× the window, at a rate both designs sustain (~20k), cells `a-lean`, `a`, `b`; report probe p99, ack p99, RAM, disk, kernel bytes, rebuild-to-ready, exactness both directions, store size and durable point at steady state. | WP-0.4's named criterion (probe p99 at a 1 h window); the only regime in which G-3 can be checked; the only run that could reverse D10 (R-20, R-23). | `cd test/raft/spikes/s2-dedup && ./target/release/s2-dedup run --dir $DATA/w3600-alean --engine heed --option a-lean --rate 20000 --batch 10 --entry-appends 10 --payload 96 --partitions 4096 --segment-bytes 2097152 --fsync-mode data --fsync-threads 8 --duration 7200 --window-s 3600 --txns-s 3600 --retention-s 720 --dup-max-age-s 4200 --ack-batch 10` then the same with `--option a` and `--option b`, each followed by `s2-dedup rebuild … --mode blooms --window-s 3600` | ~2 h VM × 3 cells |
| D-07 | `a-lean` **in the pruning regime** on the VM (past `t = txns_window`). | Its prune walks per partition instead of one time-ordered index; measured only on the laptop (0.088 vs 0.045 ms/entry) and on the VM only in the growth phase. | covered by D-06's `a-lean` cell (`--duration` ≥ 2 × `--txns-s`) | in D-06 |
| D-08 | `--cache-mb 0` on the VM, rate-matched. | The only end-to-end pair lost **31%** of the rate (25 253 → 17 488 msg/s); it was never run on the VM (R-33). Moot while (b) is not the design. | `s2-dedup run --option b --cache-mb 0 …` beside an identical `--cache-mb 64` cell | ~20 min VM |
| D-09 | The **10 GiB** manifest snapshot (WP-0.5 asks for it; 1 GiB was run). | Cost is linear in file count (160 vs 64), plus build memory and the retry window against `max_in_snapshot_log_to_keep`. | `cd test/raft/spikes/s3-consensus && ./run.sh vm` — the full-scale profile already carries the 10 GiB case (`vm-today` is the 1 GiB budgeted pass that ran) | ~8 min VM |
| D-10 | A snapshot build that includes **§11.6 step 3** (an `mdb_env_copy` of a real ≥10 GiB heed store), **plus the snapshot cadence** `QUEEN_RAFT_SNAPSHOT_LOG_BYTES` (4 GiB) implies at A20k/A50k. | openraft cannot purge log past the snapshot, so cadence sets the log disk; an O(state) export per snapshot is what I8/G-3 forbid. If no setting satisfies I8, §11.6 needs an incremental export — which re-opens **D9**, not D11 (R-44). | `s1-store run --engine heed … --duration 3600` to build a ≥10 GiB store, then `s1-store export --engine heed --dir <store>`; cadence from `./run.sh vm` with a bytes-based snapshot policy | ~1 h VM |
| D-11 | §13.6's kill shape for the **leader**: ≥5 runs, kills randomised across ≥3 durable-point intervals **and 2 snapshot builds**, including during a build and during an install, every killed node restarted. **And a pipeline of exactly 4** with a step-down injected while 4 entries are in flight (I3). | The pgless lesson: its kill tests always killed before the first checkpoint (R-45). D4's amendment cites S3, which measured 1 and 16 in flight, never 4 (O23). | extend `s3-consensus/src/scenarios.rs` (the spike's snapshot policy is `Never`), then `./run.sh vm` | ~30 min VM + harness work |
| D-12 | dm-delay on a follower (50 ms), ENOSPC on a follower, clock jumps, and the true #2080 **bridge** topology (a node that reaches both sides; probably 5 nodes). | Scenario 9 reproduced the *mechanism* with a one-way link, not the issue's topology. | extend `scenarios.rs` + `flaky-log.sh`'s dm harness, then `./run.sh vm-refute` | ~30 min VM + harness work |
| D-13 | **SHA-NI**: the true size of the MAC-vs-TLS gap. Neither host has it (`grep -c sha_ni /proc/cpuinfo` → 0; QEMU `pc-i440fx-6.1` masks it). Scaled, the MAC would cost ~+0.15–0.20 µs/msg against TLS's +0.11. | It decides whether D12's two authentication branches are close or 11× apart. | `s4 bench` on any host whose `/proc/cpuinfo` shows `sha_ni` | 5 s |
| D-14 | A real RTT and the in-flight depth D7 implies: hold each forward 3–7 ms before answering, 500 µs and 1 ms of netem, mixed frame sizes (2.8 KB outcomes interleaved with a multi-MB `PayloadRead`), sweep 1/2/4/8 sockets. | The sweep ran at **0.31** commands in flight; D7 implies 15–35 (R-61). This is the only run that can size the pools. **Needs Alice**: `tc qdisc` is a VM-wide setting. | `tc qdisc add dev lo root netem delay 500us` + a new `PART=pool` block in `s4-transport/run-vm.sh` (the commit-delay and mixed-frame options do not exist yet) | ~30 min VM + harness work |
| D-15 | Head-of-line blocking behind a big frame; reconcile `MAX_FRAME` (8 MiB) with `QUEEN_RAFT_ENTRY_MAX_BYTES` (96 MiB), or define a chunking rule. | §12.5's separate-pools rule is asserted, not tested; raising the cap without a bound on `buf.resize(len)` is an allocation DoS (R-62). | in D-14's run, with a multi-MB frame interleaved | in D-14 |
| D-16 | Failure behaviour: peer kill, half-open socket, reconnect reusing a request id; and backpressure (the per-connection mpsc 4096 queues were never exercised — both sides ran at ≤0.21 of 8 cores). | One multiplexed socket makes one failure retry every in-flight command, bounded by D6's recorded outcomes (R-63); reconnect is still unimplemented in the spike. | new `PART=fail` block in `s4-transport/run.sh` | ~30 min VM + harness work |
| D-17 | **HTTP/2 over the same rustls** as a third configuration. | The one alternative that would remove most of the framing, multiplexing, backpressure and reconnect code D12 obliges us to write (R-64). `h2` is not yet in `server/Cargo.lock`, so it is a new dependency (C-2-compatible, cmake-free). | new configuration in `s4-transport` (hyper's `http2` feature) | ~1 harness day |
| D-18 | Three brokers sharing one disk (§13.6), and the interaction between the dedup structures and a snapshot install. | Every store and dedup number is one process on a dedicated disk. | three concurrent `s1-store run` / `s2-dedup run` processes on one VM disk | ~30 min VM |
| D-19 | **Flatness at 100 M / 1 M partitions** (the task's shrink of the plan's 300 M / 60-min). | The night ran **8 M / 1000 partitions / 6-min** — the largest at-rest size leaving headroom on 15 GB. 100 M × ~387 B/msg ≈ **36 GB** resident » 15 GB, and 1 M active partitions multiplies per-frame RAM; infeasible on this VM. | the three-VM O13 rig (a dedicated broker VM), then `flatness-*.sh 60 100000000` | O13 rig |
| D-20 | A clean **60-minute C1000** (fan-out) at pipeline=4 over a settled store, on a fresh broker. | The night's C1000 numbers are confounded (the six-regime 238.6 by the A50k backlog drain; the flatness fresh-store 9.79 is 6-min, not 60). The fan-out non-isolation (I8/O14) warrants a long window with no prior-regime backlog. | fresh broker, `measure-raft1.sh` C1000 only, 3600 s | ~1 h VM |
| D-21 | **WP-0.4's 50k / 1 h dedup probe** — still does not exist. | Run A collapsed at ~min 55 (R-121); run B was a 15k push-only deviation (clean probe p50 1.16 / p99 8.97 ms at 15k). The 50k/1h number needs either R-121's push-admission fix, or a like-for-like postgres oracle to separate the defect from the VM's A50k CPU ceiling — ideally on the O13 rig. | R-121 fix + `run.sh` A50k with a probe side-loop, three-VM | O13 rig + fix |

(D-09, the 10 GiB manifest snapshot, and D-12, the dm-delay-on-a-follower / true
#2080 bridge, remain deferred as before — the night run did not touch consensus.)

---

*Last revised 2026-09-19 (**G1 packet — performance rounds 1–3 folded in**): the
phase-1 performance package ran to round 3 on the VM. Round 3 (M10) added PERF-G
(batcher/writer schedule, `9e1ebff8`), PERF-H (ring re-arm, `c65c8c2c`) and PERF-I
(O(claimed) wildcard pop, `f006c3d4`) — correctness/scaling hardening,
performance-neutral-to-parity, moving neither push-latency bound; AB-0 (PERF-6) settled
the consume rate as a non-regression (R-130 discharged). O14 still FAILS on C1000
(propose bound), `proceed=false`; long PERF-8 skipped. Prior revisions: 2026-09-19 the
pipeline=4 night pass (NIGHT-A/B/C, F-1 fixed `f8bfa672`; O14/I8/O19 FAIL, dropped writes
99/100 R-123); 2026-09-18 the draft G1 packet (`29bc7cdd`, pipeline=1) and the G0 packet.
This revision edits `RAFT_STATUS.md` and adds `test/raft/vm/raft1/PERF-6-consume-ab.md`
and `PERF-7-short.md`.*

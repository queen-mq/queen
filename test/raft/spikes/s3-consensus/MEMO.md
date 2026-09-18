# WP-0.5 / spike S3 — decision memo: the consensus library for D11

Date: 2026-09-17, **revised 2026-09-18 after two adversarial refutations**
(both major, both partly upheld: see "Refutations" at the end — what changed,
what was measured, and what is still open). Branch `raft`, base `fc71b65b`.
Author: the S3 spike agents.
Evidence: `RESULTS-vm.md` (Linux VM, §13.6, the numbers to quote; §8-§11 are
the 2026-09-18 pass), `RESULTS-laptop.md` (macOS, behaviour only; §10 is the
2026-09-18 pass), `RAFTRS.md` (the raft-rs estimate), harness
`test/raft/spikes/s3-consensus/` (standalone crate, not a member of
`server/Cargo.toml`).

**Recommendation for D11: openraft, pinned to git rev
`54094270ede0b8a2eb6ed6ae990edc6ca19d98ec` (`0.10.0-alpha.34` plus the GH#2095
fix), with `raft-log 0.4.6` as the log store.** Re-confirm at G3 against
whatever 0.10 release exists then. The direction is unchanged after the
refutations; two things about it are not:

1. **`raft-log` is now qualified the way heed was for D9, not by a conformance
   suite.** On 2026-09-18 it took 10 x `kill -9` of a follower under 64 KiB
   load with a restart every time, and 10 more with dropped unflushed writes
   under it (dm-flakey, injector self-tested). In 20/20 rounds it reopened
   **exactly at the index the leader had counted as matched** — never below it
   (nothing acknowledged lost) and never above it — every index it claimed was
   readable, and the persisted vote never regressed (RESULTS-vm.md §8, §9). The old evidence — "it passes
   `openraft::testing::log::Suite`" — was an over-read and is withdrawn.
2. **`save_committed` must be durable in our adapter.** openraft's own example,
   which this spike copied, does not flush it; with that setting a `kill -9`
   restart brings every node back with an applied state BEHIND what it had
   already answered to a client (measured: RESULTS-laptop.md §10.1,
   RESULTS-vm.md §10). The fix costs +2.4 ms on a 64 KiB commit p50 and no
   throughput. This is plan change 5 of the seven listed below.

## The numbers, per scenario (Linux VM, 2026-09-17)

Host: `root@164.90.215.224`, Ubuntu 24.04, 8 vCPU (Xeon Gold 6548N), 15 GiB,
ext4, fdatasync ≈ 1 ms. Three (scenario 5: four) node processes and the driver
on one VM over loopback; every entry byte is written six times per cluster.
Full output, commands and node logs: `RESULTS-vm.md` and
`results/vm-2026-09-17/` (one archive of record; the duplicate `results/vm/`
was merged into it on 2026-09-18). Build 53 s cold, 22 s incremental; the seven
scenarios took **11 min 41 s** of VM wall clock — node logs 14:54:40.571Z to
15:06:21.985Z, corrected 2026-09-18 from the "10 min 30 s (14:55:55 →
15:06:25)" first published here — against a 30 min budget. Rows 8 and 9 come
from the 2026-09-18 refutation pass (`results/vm-2026-09-18-refute/`, 6 minutes
of VM wall clock including the build).

| # | what | measured | reading |
|---|---|---|---|
| 1 | commit latency, 64 KiB entries, **one** write in flight (D4); nominal 60 s per rate, ELAPSED 60 s / 98 s / 125 s | service p50 **3.0 ms** (p99 4.4-5.3); achieved 200/200, **307**/500, **306**/1000 per second; 0 errors | D4 caps a cell at **~307 entries/s = 19 MiB/s**. 200/s is met; 500 and 1000/s are not, and their response percentiles are pure backlog (19 s, 43 s). *Corrected 2026-09-18: the 1000/s point was cut by the harness hard stop (`2 x secs + 5`) after issuing **38 217 of 60 000** offered writes.* |
| 1b | the same with 16 writers in flight (what §7.1's command batcher looks like) | 201 / 501 / **974** per second; service p50 11-13 ms; response p50 14 ms at 500/s, 88 ms at 1000/s | The offered rates need concurrency: 61 MiB/s of entries is the wall on this disk. Fill entries with commands (§7.1); do not relax D4 to get there. |
| 2 | `kill -9` the leader under load, 3 runs | new leader after **3087 / 3300 / 3353 ms**, first committed write +3-7 ms, **0 acknowledged writes missing** (2067 / 2043 / 2051 ids checked on both survivors) | No data loss, as designed. Failover is **3-4 s** under D14, not 1-2 s: detection = `election_timeout_max + rand(min,max)`; the election itself is **~5 ms**. |
| 2b | the same with a follower SIGSTOPped for 5 s | commit latency during the freeze p50 **3.20 ms** (vs 3.30 ms before); the resumed follower replayed 516 entries (~33 MiB) in **460 ms**; then the kill: new leader 3048 ms, **0 missing** | A frozen follower costs nothing (quorum 2 of 3) and catches up by itself. The once-frozen node then won the election — replication lag, not liveness, is what a dashboard must show. |
| 3 | `transfer_leader`, 5 healthy rounds | role change p50 **28 ms**, first committed write p50 **31 ms**, 5/5 | §12.7's zero-downtime deploy is comfortable — when the target is caught up. Clients see `NotLeader{reason: "LeaseExpired"}` in the window. |
| 3b | `transfer_leader` to a dead target (GH#2088) | `trigger()` returns `Ok(())`; source reports `is_leader = true` from t=0 while refusing writes; **3357 ms with no committed write**, then another node wins a normal election | Fire-and-forget by design. `is_leader` must not drive readiness; a preStop hook must confirm that *another* node committed. |
| 4 | linearizable reads, open loop, 30 s per point | barrier cost p50 **0.16 ms @1000/s**, **0.21 ms @5000/s** (single); §9.4's fixed 2 ms window: **1.28 / 1.47 ms** p50 and max 58.6 ms; coalescing into the in-flight barrier: **0.16 / 0.23 ms**, max 3.5 ms; achieved = offered everywhere | The fixed 2 ms window is a **7-8x pessimisation** and worsens the tail. §9.4 should say "coalesce into the in-flight barrier, at most one in flight". With both followers dead the barrier correctly refuses ("not enough for a quorum") while the local stale read still answers. |
| 5 | 1 GiB manifest snapshot to an empty learner | fill 73 MiB/s; **build 0.2 s** (seal + per-file xxh3); **transfer 2.6 s = 398 MiB/s**; with the learner killed 0.5 s in: **53 of 64 files re-sent**, total 3.4 s | App-defined manifest snapshots work, and per-file resume pays for itself. 398 MiB/s is loopback with a warm cache, not a network number. **These numbers measure THIS SPIKE's own manifest transport** (`server.rs` `SnapFileStart`/`SnapChunk`) plus one `install_full_snapshot` call — not openraft, and not a Queen snapshot: §11.6 step 3, the store export, is absent from the spike's state machine (`sm.rs:10-11`). See refutation R9. |
| 6 | wiped voter, wrong way then right way | wrong way: **silent**, writes keep committing; right way (remove → new generation id 2002 → learner → promote): learner caught up in **51 ms** | Below. |
| 7 | restart all three **gracefully** (`Req::Shutdown`), `enable_leader_restore = false`, 2000 entries | all 2000 acknowledged writes present on all three, applied index never went backwards, leader again after **1656 ms** | Recovery after a CLEAN shutdown is correct. A restarted follower reports a stale `current_leader` meanwhile — §14.1 must not use it. *Corrected 2026-09-18: this says nothing about a crash. Re-run with `kill -9` and a stale durable point, the same scenario returns `applied-state-preserved=false` unless `save_committed` is made durable — RESULTS-vm.md §10, RESULTS-laptop.md §10.1.* |
| 8 (new, 2026-09-18) | `kill -9` a FOLLOWER under load 10 times, restart it every time; then 10 more with dropped unflushed writes (dm-flakey) | reopened log **exactly at the leader's matched index in 20/20 rounds**; read-back of every claimed index 100 %; 0 vote reversions; 0 acknowledged writes missing; equal digests; restart→answer 433 ms p50, catch-up 213 ms p50 | The qualification WP-0.3 demanded of the store, now met by the log. RESULTS-vm.md §8-§9. |
| 9 (new, 2026-09-18) | one-way partition: both followers process the leader's appends and never answer (the lease interaction of GH#2080) | **0 writes committed for the whole fault**, term never moves, no campaign; a client write **hangs — no answer after 10 s**; `last_quorum_acked` ages without bound (901 ms → 12 125 ms); recovery only after an operator acts: heal **616 ms**, `kill -9` the stuck leader **3528 ms** | The one open liveness issue is reachable on the pinned rev in a 3-node cell. D13 does not cover it, §12.1 needs its own propose deadline. RESULTS-vm.md §11. |

## What openraft did on the wrong wiped-voter procedure

The procedure that D21 forbids — stop a voter, delete its data directory, start
it again **with the same node id** — was run on the VM and on the laptop. Both
times:

```
restarted 2001 with an empty directory and the SAME id
  wrong-way state: node 2001 leader=Some(1001) term=1 last_log=None applied=None
                             count=0 voters=[] learners=[]
  leader after the wrong way: Some(1001)
  write after the wrong way: Ok(202)
```

openraft does **nothing**: no refusal, no panic, no metric, no log line about a
member that has lost its disk. The node comes up with an empty vote store, an
empty membership and an empty log, the live leader re-fills it by replication,
and the cluster keeps acknowledging writes.

That is worth stating precisely, because the library's own documentation says
something else. `openraft/src/docs/faq/07-troubleshooting/04-wipe-node-data.md`
in the pinned tree opens with: "Avoid doing this. Doing so will panic the
leader. But it is permitted with config `Config::allow_log_reversion` enabled."
The code says otherwise (`openraft/src/progress/entry/update.rs:80-112`): when
a follower's log is found reverted and `allow_log_reversion` is off — the
default, `Config::allow_log_reversion: None` → `get_allow_log_reversion() =
false` (`config/config.rs:526,597,633`) — the entire guard is

```rust
debug_assert!(
    conflict >= self.entry.matching().next_index(),
    "follower log reversion is not allowed without `allow_log_reversion` enabled; ..."
);
```

i.e. **the promised panic exists only in a debug build.** In release, which is
what runs in production, the reversion is accepted in silence. The spike saw
exactly that, on macOS and on Linux, with `allow_log_reversion` never set.

The danger is not the empty log — the leader refills it — it is the **empty
vote store**: that node can grant a second, different vote in a term it had
already voted in, which is how two leaders in one term happen. The same FAQ
page spells out the data-loss sequence (a committed log `a` on N1, N2, N3; N3
erased; N1 crashes; N5 elected by N3 and N4 without `a`) and adds the point
that matters for §12.6: **removing the node from the membership before wiping
it is not enough either, as long as it rejoins under its old id.** Our
generation scheme (`ordinal*1000 + generation`) is what makes the rejoin safe,
and it is not optional.

(One correction to `RESULTS-laptop.md` and to D21's rationale column in
PLAN_RAFT.md, both of which call this case the FAQ's "undefined behaviour":
that phrase does not appear in the pinned tree's docs —
`grep -rniE "undefined behaviou?r" openraft/src/docs` is empty. What the FAQ
actually says is quoted above, and it is wrong in the other direction: it
promises a panic that a release build does not have.)

Consequence for the plan, unchanged but now demonstrated twice: **I9/D21 is
ours.** The IDENTITY file (cluster_id, node_id, generation, disk_uuid) fsynced
before the node serves any Raft RPC, checked at the enforcement point of D12,
plus the replacement flow of §12.6 (remove → `ordinal*1000 + generation+1` →
learner → promote), which worked perfectly: the replacement caught up in 51 ms
and writes continued throughout.

*Added 2026-09-18 (refutation R8):* what the generation scheme closes is an
EMPTY data directory. It does not close a **reverted** one — a PVC restored
from a volume snapshot comes back with a matching
`{cluster_id, node_id, generation, disk_uuid}`, a short log and a **stale vote
store**, passes every I9/D21 check, and can then grant a second vote in a term
it had already voted in: the same double-vote hazard, with no library guard
behind it in a release build. That case was not run in this spike and needs a
design change — a monotonically increasing fencing value (highest term seen) in
IDENTITY, fsynced when it advances, and a refusal to vote below it (plan
change 6).

## The exact pin, and why this one

```toml
openraft = { git = "https://github.com/databendlabs/openraft.git",
             rev = "54094270ede0b8a2eb6ed6ae990edc6ca19d98ec",
             features = ["serde", "type-alias"] }
raft-log = "0.4.6"
```

- The commit is `0.10.0-alpha.34` (workspace version string, verified in the
  checked-out source on the VM) plus one change: `54094270 change: errors:
  report discarded client-write entries`, 2026-09-15, which adds
  `ClientWriteError::LogEntryDiscarded(ForwardToLeader)`. Verified present in
  the pinned tree (`openraft/src/errors/mod.rs:116`, raised in
  `core/raft_core.rs:2414` and `:2436` for the purge and truncate paths, marked
  `#[since(version = "0.10.0", change = "added LogEntryDiscarded")]`).
- Why it matters (this is D6's whole reason for existing): on the published
  alpha.34, a `client_write` whose entry **was appended and then discarded**
  comes back as a plain `ForwardToLeader`, indistinguishable from "the request
  was rejected before it was appended". §12.1 needs the two apart:
  `ForwardToLeader` → `ProposeError::NotLeader` (safe to re-plan and re-propose)
  and `LogEntryDiscarded` → `ProposeError::OutcomeUnknown` (the entry may yet
  commit under another leader; only the request-id window of D6 can answer it).
  Without the variant every write in a leadership change would have to go
  through the request-id window, i.e. D6 would be load-bearing for correctness
  instead of being the second line of defence.
- There is no 0.10 release to pin instead: openraft's own "Release v0.10.0
  Stable" issue (#1637, open since 2026-01-01) is still open, and crates.io has
  only alphas. Pinning a commit is therefore not a preference, it is the only
  option that has the fix.
- `edition = "2024"` in the openraft workspace needs Rust ≥ 1.85; MSRV 1.88
  (C-3) is satisfied and was verified with `cargo +1.88 check` on the laptop.
  The VM built the same tree with 1.98.1.
- `raft-log 0.4.6` is a normal crates.io release by openraft's author; the
  adapter is ~230 lines, from openraft's `examples/log-wal`. It passes
  `openraft::testing::log::Suite` — **which is NOT the §12.3 storage contract**
  (corrected 2026-09-18). That suite builds one store per case and never
  reopens it (`openraft/src/testing/log/suite.rs:1794-1805`), reads the vote
  back from the same live instance (suite.rs:966-973), and skips its
  `read_committed` case when the implementation returns `None`
  (suite.rs:929-963); no case closes a store, crashes a process or drops a
  write. It therefore cannot see three of §12.3's clauses: "flush callback only
  after fsync", "`save_vote` durable before returning", "`save_committed`
  persisted". Proof rather than argument: the suite passes against all three
  settings of the adapter's new `CommittedDurability` knob, including the one
  that never flushes (RESULTS-laptop.md §0). What qualifies `raft-log` is the
  crash evidence of RESULTS-vm.md §8-§9, not the suite.
- **Three** commits landed on openraft main after the pin, not two: `ff31db46`
  and `c717fe98` (test-only, 2026-09-17) and `f1a2ec63` ("docs: openraft:
  document v0.10 feature flag migration", committed 2026-09-18T02:22Z). All
  three are tests or docs; nothing between the pin and today changes behaviour
  we depend on. (Checked again 2026-09-18 with
  `gh api repos/databendlabs/openraft/commits?sha=main&since=2026-09-15`.)

## The open issues, re-read today (2026-09-17)

| issue | state today | what it means for us |
|---|---|---|
| [#2080](https://github.com/databendlabs/openraft/issues/2080) Liveness deadlock: old leader heartbeats block a connected quorum | **OPEN**, re-checked 2026-09-18 (created 2026-09-07, last updated 2026-09-17T05:54Z; `gh issue list --label C-bug --state open` still empty). **Its mechanism now reproduced on the pinned rev**: scenario 9, RESULTS-vm.md §11 | The only open safety/liveness issue. A partial network (old leader reaches only a "bridge" node; the bridge, a and b form a quorum) can stall progress indefinitely: the old leader's quorum-ack lease expires so it refuses writes, but its heartbeats keep renewing the bridge's follower-side leader lease, and the bridge rejects the other two's vote requests. It needs a **partial** partition, not a clean one; a 3-node cell reaches it when one node can talk to the leader and not to the third. Consequence for us, now measured rather than feared: with a one-way link (both followers process the leader's appends and never answer) a 3-node cell commits NOTHING for as long as the fault lasts, the term never moves, and **a client write does not get a 503 — it hangs** (10 s, no answer). So D13 as written does NOT cover this: it holds while *no leader is known*, and here a leader is known and accepted the request. What does work: §12.1's `propose` needs its own deadline, and `RaftMetrics::last_quorum_acked` ages without bound on the stuck leader (901 ms → 12 125 ms) and is the alert §14 needs. Operator recovery measured: heal the link 616 ms, `kill -9` the stuck leader 3528 ms. A first PR (#2083) was closed; the assignee (Phoenix500526) still has it. |
| [#2088](https://github.com/databendlabs/openraft/issues/2088) Failed `transfer_leader` causes long leaderless period | **CLOSED completed** 2026-09-13 | Closed as by design: "the simpler fallback is to let another follower win a normal election"; the maintainer explicitly rejects recovering the source leader ("an RPC timeout does not prove that the target did not receive the transfer request"). `899eed62` did remove the sticky smaller-log election delay. Measured today: **3.36 s with no writes** after a transfer to a dead target, while the source still reports `is_leader = true`. §12.7 must therefore (a) pick a target that metrics say is caught up, (b) wait for the role change with a deadline, (c) never use `is_leader` as a readiness signal. |
| [#2095](https://github.com/databendlabs/openraft/issues/2095) `ForwardToLeader` for truncate and purge can cause double writes | **CLOSED completed** 2026-09-15 | This is the bug the pin fixes. The reporter's sequence (5 nodes, two partitions in a row) ends with an entry that was answered `ForwardToLeader` but is later committed by another leader — a retry would double-write. Fixed by splitting the error: `ForwardToLeader` = not appended, `LogEntryDiscarded` = appended then discarded, outcome unknown. |
| [#2091](https://github.com/databendlabs/openraft/issues/2091) Liveness deadlock when a removed leader loses leadership before the final membership commits | CLOSED completed 2026-09-14 | Membership-change edge case (a code-inspection finding, no deterministic reproduction), closed the same day as `018c2ec9 fix: openraft: allow committed voters to campaign`, which `git merge-base --is-ancestor 018c2ec9 54094270` confirms is in the pinned tree. Relevant to §12.6's replacement flow; re-test it in WP-3.x. |
| [#2085](https://github.com/databendlabs/openraft/issues/2085) `stream_append` regression: Conflict and strict PartialSuccess miss clock updates | CLOSED completed 2026-09-13 | Replication-protocol regression, fixed before the pin. |

There is **no open issue labelled `C-bug`** in the repository today
(`gh issue list --label C-bug --state open` is empty), and no open issue newer
than #2080. The open list is otherwise features, tests and perf (#2073 leader
probing, #2078 the Jepsen test for #2080, #2056 a Jepsen cleanup starvation,
#1637 the 0.10 release, #1494 "Path to 1.0").

Worth noting for G0: the project has been running a Jepsen suite against itself
since August 2026 (nemeses for packets, clock, membership, process faults), and
#2080 came out of it. That is where the alpha's remaining safety work is
visible — a good sign about the maintainer, not a reason to treat the alpha as
stable.

## raft-rs, the fallback (from `RAFTRS.md`)

raft-rs gives the protocol and nothing else: no network, no storage beyond
`MemStorage`, no snapshot transport, **no storage API suite** (openraft's
`testing::log::Suite` shook the adapter's API semantics out in an afternoon —
it does not, as this memo first claimed, validate durability; that took the
crash runs of RESULTS-vm.md §8-§9, which a raft-rs port would need too). Its `Snapshot.data` is a protobuf blob
inside a message, so a 10 GiB manifest means building TiKV's out-of-band
transfer ourselves. Its build needs a system `protoc` at compile time, which is
a release-pipeline change and brushes against C-2.

Estimate from reading `examples/five_mem_node` and the proto build:
**20-27 agent-days** (ready loop 3-4, `Storage` + our own conformance tests
5-7, snapshot build/transfer/install/retry 4-5, read barriers 2, membership and
metrics 2-3, protoc plumbing 1, hardening 3-5), i.e. **4-5 calendar weeks**,
with the risk concentrated in storage and snapshots — exactly where pgless's
bugs were. For comparison, a production-quality openraft adapter in
`server/src/rsm/` is 6-9 agent-days; this spike's adapter was about one.

Latest raft-rs commit on master is `ad13f3d9` (2026-05-13) and crates.io is
still at 0.7.0 (2023-03-07), so it too would be a git pin — an unmaintained-
looking one.


## Recommendation for D11

**Take openraft at `54094270` (0.10.0-alpha.34 + `LogEntryDiscarded`) with
`raft-log 0.4.6`.** Concretely, for WP-3.1:

1. Pin by git rev with a `Cargo.lock` committed, never by branch. Re-evaluate
   at G3: if a real 0.10 release exists by then and contains
   `LogEntryDiscarded` and the #2080 fix, move the pin to the release; if not,
   stay on the rev and record the reason in `RAFT_STATUS.md`.
2. Vendor nothing, patch nothing. If we need a fix, upstream it: the maintainer
   turned #2095 around in two days and #2088 in three.
3. Keep §12.1's `Replicator` seam exactly as drafted. It is what makes the
   raft-rs fallback a 4-5 week project instead of a rewrite, and it is what
   lets phases 1-2 run on `LocalReplicator`.
4. Map the errors as §12.3 says, with the pinned variant:
   `ForwardToLeader` → `NotLeader`, `LogEntryDiscarded` → `OutcomeUnknown`,
   our own deadline → `Timeout` (entry still in flight), `Fatal` → stop the
   node. ENOSPC on the log is `Fatal` (seen on the laptop).
5. Own what openraft does not: the IDENTITY check of I9/D21 (§12.6), the
   append-deadline floor (§12.5), the read-barrier coalescer (§9.4), the
   readiness signal (§14.1), and the fault tests it has no model for (lost
   fsyncs, wiped disks, clock jumps, slow disks — §13.6).
6. **Make `save_committed` durable and gate reads on it.** In the adapter:
   `flush(true, _)` awaited (the spike's `CommittedDurability::Fsync`), cost
   +2.4 ms on a 64 KiB commit p50 and no throughput (RESULTS-vm.md §10). At
   start-up: no local stale read (D15) and no ready `/health` until the node
   has re-applied at least the `committed` it reopened with;
   `Raft::wait_for_recovery` (944-1055 ms measured after a 3-node restart) is
   the library's helper for that, with its documented limit — a follower that
   applied an entry it had not flushed can still recover a stale state
   (`raft/mod.rs:1896-1912`), which is why the durable record comes first and
   the wait sits on top.
7. **Give `propose` its own deadline.** A leader that has lost its quorum-ack
   lease neither commits nor refuses: the write hangs (scenario 9). Map our own
   deadline to `ProposeError::Timeout` (the entry stays in flight, I3), answer
   the client 503 after `QUEEN_RAFT_HOLD_MS`, and alert on
   `last_quorum_acked`.

### Residual risks

Revised 2026-09-18: the rows that were promises now carry measurements, and two
rows are new.

| risk | size | what we do about it |
|---|---|---|
| **The library is an alpha with an open liveness bug (#2080), and its mechanism is reachable on the pinned rev.** Scenario 9: a 3-node cell with a one-way link commits nothing until an operator acts, and the client write HANGS rather than failing. | high impact, unknown frequency | Not a promise any more: (a) `propose` gets our deadline → 503 after the hold (D13 alone does not fire); (b) alert on `RaftMetrics::last_quorum_acked` ageing past the election timeout — measured to grow without bound on the stuck leader; (c) the runbook action is `kill -9` the stuck leader (3528 ms to writes) or heal the link (616 ms); (d) watch the issue to G3 and GA; (e) keep the postgres class deployable until GA (D22). What is still NOT known: how often a Kubernetes cell produces this link shape, and whether the issue's own bridge topology behaves worse. |
| **`raft-log` is a single-maintainer crate carrying the write-ahead log of the whole system.** | medium (was: unqualified) | Qualified on 2026-09-18 to the WP-0.3 bar: 10 x `kill -9` + restart and 10 x dropped unflushed writes (dm-flakey, injector self-tested), 0 failures, log always reopened exactly at the leader's matched index and fully readable (RESULTS-vm.md §8-§9). Re-run `flaky-log.sh` and scenario 8 in WP-3.1 against the adapter that ships, and again at G3. |
| **`save_committed` was not durable in the adapter** — openraft's example does not flush it, and with that setting a crash-restart reverts applied state below what was already answered. | was high, now closed by a decision | Fix chosen: durable `save_committed` (+2.4 ms p50, no throughput cost) plus a readiness gate on the reopened `committed`. Written up as plan change 5 below; the knob and both measurements are in the spike (`--committed none\|buffered\|fsync`). |
| **The pin is a commit, not a release.** No semver, no changelog guarantee, `0.10.0-alpha.*` API churn between alphas. | medium | Pinned rev + committed lockfile; re-verify every API name against the pinned source at WP-3.1; one agent-day budgeted per upgrade. Three commits landed after the pin, all tests/docs. |
| **One vendor, one maintainer.** openraft is effectively drmingdrmer plus collaborators. | medium | `Replicator` seam + `RAFTRS.md`'s 20-27 agent-day estimate is the exit; the state machine, the log format and the snapshot manifest are ours and survive a switch. |
| **D4 (one entry in flight) caps a cell at ~307 x 64 KiB entries/s**, and the amended pipeline of 4 has never been measured — the spike measured 1 and 16. | medium | It is the *command* batcher of §7.1 that has to fill an entry; measure a pipeline of exactly 4, with a step-down injected while 4 entries are in flight (I3), in WP-3.x before the amendment is relied on. |
| **D14's timings mean a 3-4 s failover**, not 1-2 s. | medium | Either accept it (D13's 8 s hold covers it) or ask Alice for 400/800 ms election timeouts, which then have to fit the follower's durable point — a trade, not a free knob. |
| **A wiped voter is silent**, and a REVERTED data directory (a PVC restored from a volume snapshot) is not covered by IDENTITY either: it comes back with matching {cluster_id, node_id, generation, disk_uuid} and a stale vote store. | high | I9/D21 is ours, enforced before serving any Raft RPC. The reverted case needs more than the current design: a monotonically increasing fencing value (highest term/vote seen) in IDENTITY, fsynced when it advances, and a refusal to vote when the log's stored vote is below it. Proposed as plan change 6; not run in this spike. |
| **Snapshot cost at Queen's real sizes is unmeasured**, and worse than a size question: the spike's state machine has no store export (§11.6 step 3), so scenario 5 measures the manifest transport only. openraft also cannot purge log past the snapshot (`engine/engine_impl.rs:828-835`), so snapshot CADENCE sets how much log disk a cell needs, and an O(state) export on every snapshot is what I8/G-3 forbid. | high for §11.6 and D9, not for D11 | Before WP-1.2 closes: measure a real snapshot build (an `mdb_env_copy` of a heed store at 10 GiB) and the cadence that `QUEEN_RAFT_SNAPSHOT_LOG_BYTES` implies at A20k/A50k; if no setting satisfies I8 at flatness scale, §11.6 needs an incremental export, which re-opens D9, not D11. |

### What this memo does NOT decide

D11 says the library is confirmed at G3, not at G0. What G0 should ratify is
the *direction* (openraft, this pin, `raft-log` as the store) and the plan
changes the spike produced — four from 2026-09-17, three more from the
refutation pass:

- §9.4: coalesce read barriers into the in-flight one; drop the fixed 2 ms
  window (7-8x measured).
- §12.5: the append/snapshot deadline must have a floor of its own, never
  `soft_ttl()` derived from `heartbeat_interval` (75 ms under D14, which
  collapsed throughput 4x on the laptop until it was floored).
- §12.7 / §14.1: readiness is never `is_leader` and never `current_leader`;
  both lie for seconds after a failed transfer or a restart.
- D14: state plainly that failover is 3-4 s, or change the election timeouts
  and pay for it elsewhere.
- **(5, new) §12.3 + D15 + §14.1: `save_committed` is durable, and a restarted
  node serves no local stale read and reports no ready `/health` until it has
  re-applied the `committed` it reopened with.** Measured: without it, a
  `kill -9` restart reverts applied state below what was already answered
  (RESULTS-laptop.md §10.1); with it, every node re-applies to its pre-crash
  state (RESULTS-vm.md §10). Cost +2.4 ms on a 64 KiB commit p50, no
  throughput. §12.3's bullet and the adapter must say the same thing; today
  they diverge silently.
- **(6, new) I9/D21: IDENTITY needs a fencing value.** The wiped-disk case is
  closed by the generation scheme; a REVERTED disk (a PVC restored from a
  volume snapshot) is not — it matches on every field and brings back a stale
  vote store. Add a monotonically increasing highest-term-seen to IDENTITY,
  fsynced when it advances, and refuse to vote below it. Not run in this
  spike; it is a design change, not a measurement.
- **(7, new) §12.1 + D13: `propose` needs its own deadline.** A leader without
  a quorum-ack lease hangs the write instead of refusing it (scenario 9), so
  D13's hold-then-503 never fires. Our deadline → `ProposeError::Timeout`
  (entry still in flight, I3) → 503 after `QUEEN_RAFT_HOLD_MS`, plus the
  `last_quorum_acked` alert.

Corrections the D11 row in PLAN_RAFT.md §2 needs (this memo may not edit the
plan; RAFT_STATUS.md should carry them):

- "raft-log passes openraft's log-store conformance suite" is not evidence of
  the storage contract — replace with the crash evidence (RESULTS-vm.md §8-§9).
- "linearizable reads ~1.1 ms" is the FIXED-WINDOW variant this memo recommends
  dropping; the barrier itself is **0.16-0.23 ms** (RESULTS-vm.md §4).
- "a 1 GiB snapshot installed at 398 MiB/s" measures the spike's own manifest
  transport, not openraft and not a Queen snapshot (no store export).
- the amendment "a bounded pipeline of 4" cites S3, which measured 1 and 16
  in flight — never 4.

## Deferred (updated 2026-09-18)

Done since 2026-09-17, in the 6-minute refutation pass: the log-store crash
qualification (10 kills + 10 dropped-writes rounds, RESULTS-vm.md §8-§9),
scenario 7 with `kill -9` and a stale durable point (§10), the cost of each
`save_committed` setting (§10), and a partial (one-way) partition, which was
the most valuable deferred item (§11).

Still deferred, in the order to run them:

1. **The 10 GiB manifest snapshot** — the size the WP-0.5 row asks for. Run at
   **1 GiB** instead (build 0.2 s, transfer 2.6 s, resume after a mid-transfer
   kill 3.4 s). The point of the 10 GiB run is that the cost is linear in file
   count (160 vs 64), memory during build, and the retry window against
   `max_in_snapshot_log_to_keep`. `./run.sh vm` already carries it: ~8 minutes
   of VM time for both runs.
2. **A snapshot build that includes §11.6 step 3** — an `mdb_env_copy` of a
   real heed store (S1 measured LMDB export at 524-556 MiB/s, always O(state),
   with a long read txn pinning the free list) — and, on top of it, the
   snapshot CADENCE that `QUEEN_RAFT_SNAPSHOT_LOG_BYTES` (4 GiB) implies at
   A20k/A50k, given that openraft cannot purge log past the snapshot
   (`engine/engine_impl.rs:828-835`). This is an I8/G-3 question for §11.6 and
   D9, not a D11 question, but it must be answered before WP-1.2 closes.
3. **The §13.6 kill matrix shape for the LEADER**: at least 5 runs, kill times
   randomized across at least 3 durable-point intervals AND 2 snapshot builds,
   including a kill during a snapshot build and during an install, with every
   killed node restarted. Scenario 8 now does 10 follower kills at arbitrary
   phases of the 1 s durable point, but the spike's snapshot policy is `Never`
   and no kill has yet landed inside a snapshot build or install.
4. **A pipeline of exactly 4 in flight** (the G0 amendment to D4), with a
   step-down injected while 4 entries are in flight, to show I3 holds ("on
   step-down every overlay is dropped together"). Measured so far: 1 and 16.
5. **dm-delay on a follower** (50 ms disk), ENOSPC on a follower, clock jumps,
   and the true #2080 bridge topology (a node that reaches both sides, which
   needs more than a one-way link and probably 5 nodes).

## Refutations (2026-09-18)

Two refuters attacked this memo. Both verdicts were "refuted, major", and both
were substantially right about the SAME thing: the log store had never been
crash-tested, and the adapter diverged from §12.3 in silence. Everything below
is either fixed with a measurement or answered with evidence; where nothing was
run, it says so.

| # | finding | verdict | resolution |
|---|---|---|---|
| R1 | "Passes `openraft::testing::log::Suite`, which is the §12.3 storage contract" is an over-read: the suite builds one store per case and never reopens it (suite.rs:1794-1805), reads the vote back from the same instance (966-973), skips its `read_committed` case when `None` (929-963); no crash, no reopen, no dropped write. | **upheld** | Claim withdrawn here and in RESULTS-laptop.md §0. Demonstrated rather than argued: the suite now runs against all three `save_committed` settings and passes for every one, **including the one that never flushes** (`cargo test --release -- --nocapture`, 29.6 s). |
| R2 | The adapter knowingly violates "`save_committed` persisted" (logstore.rs: `commit()` then no flush, verbatim from openraft's `examples/log-wal`), and the memo never said so. | **upheld** | `CommittedDurability::{None,Buffered,Fsync}` added; the module doc now names the divergence; the recommendation is `Fsync` plus a readiness gate (plan change 5). |
| R3 | The failure the recommendation does not survive: a crashed pod recovers behind its pre-crash applied index and answers a D15 local stale read without a message the client already saw. | **upheld and reproduced** | Scenario 7 with `kill -9` and `--durable-ms 60000`: with `save_committed=none`, `WENT BACKWARDS on 1001: last_applied Some(301) -> Some(300)`, nodes missing acknowledged writes, `applied-state-preserved=false` (laptop §10.1); on the VM the same reverted `committed` appears on all three nodes and the window closes in ~1.1 s (§10). With `fsync`: preserved on both hosts. |
| R4 | No crash-restart was ever run: scenario 7 used a graceful `Req::Shutdown`, every `kill -9` was of a leader that was never restarted or a learner with an empty log. The component that loses committed entries got less scrutiny than the one holding derived state. | **upheld** | Scenario 8 added: 10 x `kill -9` of a follower under 64 KiB load with a restart each time, reading the on-disk state before openraft can replicate into it, plus a full read-back of the reopened log. 10/10 clean on the VM (§8), 4/4 on the laptop. Scenario 7 re-run with `kill -9` (§10). |
| R5 | `raft-log` must meet WP-0.3's bar, which included a dropped-unflushed-writes run; until then it is a PROPOSED default, not a ratified one. | **upheld, now met** | `flaky-log.sh`: the victim follower's log on a dm-flakey filesystem, table switched to `drop_writes` immediately before each kill. Injector self-tested (a 16 MiB unfsynced file comes back **0 bytes**, an fsynced one comes back whole). 10/10 rounds: log reopened exactly at the leader's matched index, fully readable, votes monotone, nothing acknowledged lost (§9). |
| R6 | GH#2080 is rated "high impact" and was never reproduced; D13's 503 and an alert do not restore writes. | **upheld, now reproduced** | Scenario 9 (one-way link, the lease interaction of #2080): 0 commits for the whole fault, no campaign, and — the new finding — **the client write hangs, it does not get a 503**. D13 does not fire because a leader IS known. Operator recovery timed: heal 616 ms, `kill -9` the stuck leader 3528 ms. `last_quorum_acked` is the alert and ages without bound. Caveat stated in §11: this is the mechanism, not the issue's bridge topology. |
| R7 | `truncate_after` also returns without any flush; is that safe? | **answered, not measured** | In openraft's follower path a truncate is followed by the appends of the same `AppendEntries`, whose `flush(true, _)` is awaited before the ack; raft-log's WAL is sequential and a sync flush covers every record written before it (`api/raft_log_writer.rs:115-131`), so the truncate record is durable before the ack. A truncate NOT followed by an append answers `Conflict`, which the leader does not count as matched, so a lost truncate record is repaired by the next append. WP-3.1 should still add a targeted test; this is reasoning, not a measurement. |
| R8 | The I9 conclusion is too broad: it closes an EMPTY data dir, not a REVERTED one (a PVC restored from a volume snapshot brings back a matching IDENTITY and a stale vote store). | **upheld** | Recorded as plan change 6: a monotonically increasing fencing value (highest term seen) in IDENTITY, fsynced when it advances, and a refusal to vote below it. Not run in this spike. |
| R9 | "Snapshot build 0.2 s for 1 GiB, transfer 398 MiB/s" measures the spike's own manifest transport, not openraft and not a Queen snapshot: `sm.rs` has no store export (§11.6 step 3), so the gap is a KIND question, not a SIZE question. | **upheld** | Scenario 5's row and the residual-risk row now say so; the D11 rationale in PLAN_RAFT.md needs the same correction (listed above). |
| R10 | openraft cannot purge log past the snapshot (`engine_impl.rs:828-835`), so with `QUEEN_RAFT_SNAPSHOT_LOG_BYTES = 4 GiB` the snapshot cadence — and an O(state) export each time — collides with I8/G-3 at flatness scale; cadence was never measured. | **upheld, out of D11's scope** | Verified in the pinned tree. Moved into Deferred item 2 as a gate for §11.6/D9 before WP-1.2 closes; it does not change the library choice. |
| R11 | The kill matrix is not §13.6's shape: 3 runs, no randomization across durable-point intervals or snapshot builds, `max_in_snapshot_log_to_keep` never exercised, no kill during a snapshot build or install. | **partly upheld** | Scenario 8 adds 20 kill+restart rounds at arbitrary phases of the 1 s durable point, which is more than §13.6's "≥ 5 runs across ≥ 3 intervals" for the FOLLOWER case. Kills during a snapshot build or install remain deferred (item 3) — the spike's snapshot policy is `Never`. |
| R12 | Reporting: (a) scenario 1 did not run "60 s per rate" — 98 s and 125 s, with the 1000/s point cut at 38 217 of 60 000 offered; (b) the VM window was 11 min 41 s, not 10 min 30 s; (c) three commits landed after the pin, not two; (d) label the snapshot numbers; (e) two duplicate results directories. | **upheld, all five** | (a) corrected in the table above and in RESULTS-vm.md §1 with the elapsed column; (b) corrected from the node logs (14:54:40.571Z → 15:06:21.985Z); (c) corrected, `f1a2ec63` added (docs-only, so the conclusion stands); (d) done, see R9; (e) `results/vm/` merged into `results/vm-2026-09-17/` (fuller node logs kept) and removed — one archive of record. |
| R13 | What neither refuter could refute, and re-checked today: the pin is what the memo says (`0.10.0-alpha.34`, rust-version 1.88, MIT OR Apache-2.0, `LogEntryDiscarded` at errors/mod.rs:116 raised at raft_core.rs:2414/2436), the wiped-voter analysis (a bare `debug_assert!` at progress/entry/update.rs:103-111, `allow_log_reversion` defaulting to false), the kill/transfer/read/election numbers, and MSRV 1.88 under `cargo +1.88 check`. | n/a | Nothing to change. The direction of D11 was not what either refuter attacked. |

What the refutations did NOT change: openraft at `54094270` is still the
recommendation, the `Replicator` seam is still what makes raft-rs a 4-5 week
exit, and `raft-log 0.4.6` is still the log store — now on crash evidence
instead of a conformance suite.

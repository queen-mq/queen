# WP-0.5 / spike S3 — Linux VM results (2026-09-17)

Host: `root@164.90.215.224`, Ubuntu 24.04, kernel 6.8.0-124-generic, 8 vCPU
(Intel Xeon Gold 6548N, 1 thread/core), 15 GiB RAM, ext4 on `/dev/vda1`,
fdatasync ≈ 1 ms (§13.6). Rust 1.98.1 (MSRV for the crate is 1.88).
All three (and in scenario 5 four) node processes and the driver run on this
one VM over loopback; they share one disk queue, so every byte of an entry is
written six times per cluster (Raft log + state machine, three nodes).

Library pin: openraft git rev `54094270ede0b8a2eb6ed6ae990edc6ca19d98ec`
(`0.10.0-alpha.34` + `ClientWriteError::LogEntryDiscarded`), raft-log 0.4.6.

Budget (Alice, 2026-09-17): at most 30 minutes of VM wall-clock beyond the
build. What that cost: see "Budget and what was deferred" at the end.

Driver:

```
rsync -az --exclude target … test/raft/spikes/s3-consensus/ \
      root@164.90.215.224:/root/raft/s3-consensus/
cd /root/raft/s3-consensus && cargo build --release     # 22 s incremental, 53 s cold
ROOT=/root/raft/s3-consensus/data ./run.sh vm-today /root/raft/s3-consensus/out-vm-today
```

`run.sh vm-today` is the budgeted profile added for this run; each scenario is a
fresh 3-process cluster on 127.0.0.1:26101-26104 with its own data directory
under `ROOT/<scenario>/n<id>`, removed after the scenario. Raw output and node
logs: `results/vm-2026-09-17/`, which is the single archive of record for that
run. (Until 2026-09-18 a second copy of the same run also sat in
`results/vm/`; the two were merged — the fuller node logs kept, the duplicate
removed — so there is one directory, not two.)

## 1. Commit latency, 64 KiB entries (scenario 1)

`--fsync periodic` (§11.4), one `client_write` in flight (D4):

```
s3-consensus latency --rates 200,500,1000 --secs 60 --entry-bytes 65536 --writers 1
offered   achieved   committed   offered   elapsed   service p50   p90    p99    max      errors
200/s     200/s      12000       12000     60 s      3.07          3.44   5.27   87.19    0
500/s     307/s      30000       30000     98 s      2.97          3.31   4.43   108.72   0
1000/s    306/s      38217       60000     125 s     2.98          3.34   4.96   110.74   0  (CUT)
```

**Correction, 2026-09-18 (refutation).** `--secs 60` is the *nominal* duration,
not the elapsed one: each point stops when its own writer has issued
`rate x secs` writes or when the hard stop at `2 x secs + 5` fires
(scenarios.rs:148,161). At 500/s the 30 000 writes took ~98 s at the achieved
307/s; at 1000/s the hard stop cut the point at 125 s with **38 217 of the
60 000 offered writes issued — 21 783 were never sent**. The earlier memo line
"60 s per rate" was wrong, and the achieved column for the 1000/s point is the
rate over the time the point actually ran, not over 60 s. Nothing else in this
section changes: the service times are per-write and unaffected.

A 64 KiB entry costs **3.0 ms** end to end (client to committed-and-applied on
the leader) on this VM: two fdatasyncs deep (leader log, follower log) plus
6 x 64 KiB of writes. With one entry in flight that is a hard ceiling of
**~307 entries/s = 19 MiB/s**, so 200/s is met exactly and 500 and 1000/s are
not: the response-time columns for those two points are pure backlog
(p50 19 s and 43 s) and only the service column means anything.

The same points with 16 concurrent writers — what §7.1's batcher does, openraft
coalescing many commands into one entry:

```
s3-consensus latency --rates 200,500,1000 --secs 20 --entry-bytes 65536 --writers 16
offered   achieved   service p50   p90     p99      max      response p50
200/s     201/s      12.67         22.18   56.10    72.65    13.91
500/s     501/s      12.11         17.36   95.64    122.38   14.38
1000/s    974/s      11.24         15.60   145.89   282.25   88.20
```

**All three offered rates need concurrency.** 500 x 64 KiB/s = 31 MiB/s is met
with response p50 14 ms; 1000/s = 61 MiB/s is at the wall (974/s achieved,
response p50 88 ms, p99 525 ms). The per-write service time rises from 3 ms to
12 ms because the entries queue behind each other, but the throughput is 3.2x.

For D4 this is the trade to put in front of Alice: one entry in flight is
19 MiB/s per cell and ~3 ms per write; the rate the plan wants (§7.1's batching
of many commands into one entry) is what makes 500-1000 entries/s reachable,
and it is the *command* batcher, not pipelining of Raft proposals, that has to
deliver it.

No `Unreachable` warning appeared in any node log of this scenario (the
laptop's finding (a) — the 75 ms append deadline derived from the heartbeat
interval — stays fixed by `net.rs`'s `soft_ttl().max(APPEND_TIMEOUT_FLOOR)`;
`grep -c Unreachable` over the three node logs returns 0/0/0).

## 2. kill -9 the leader under load, 3 repetitions (scenario 2)

64 KiB entries, one write in flight, the driver's serial loop offering 200/s
(effective ~100/s: a write costs ~3.4 ms and then the loop sleeps 5 ms).

```
s3-consensus kill-leader --rate 200 --entry-bytes 65536 --secs-before 10 --secs-after 10   (x3)

run   writes before   new leader   first committed write   acked / applied   missing
1     1013            3087 ms      3090 ms                 2067 / 2067       0
2      995            3300 ms      3307 ms                 2043 / 2043       0
3     1016            3353 ms      3358 ms                 2051 / 2051       0
steady-state service time before the kill: p50 3.22-3.43 ms, p99 15.8-17.0 ms
VERDICT acknowledged-writes-missing=0 in all three runs
```

**No acknowledged write was lost in any run**, on either survivor, and the
first write after the kill lands 3-7 ms after the new leader appears.

Where the 3.1-3.4 s goes (node log of run 1, `n2001.log`):

```
15:04:03.465992  election timeout expired, triggering election
15:04:03.470768  id=2001 becomes leader                       (+4.8 ms)
```

The election itself is **~5 ms** on this VM (18 ms on the laptop). Everything
before it is openraft's `Utime::is_expired` with
`lease = election_timeout_max` plus a resampled random election timeout, i.e.

> **detection = election_timeout_max + rand(election_timeout_min, election_timeout_max)
> = 3.0-4.0 s under D14 (100 / 1000-2000 ms)**, measured 3.09 / 3.30 / 3.35 s.

D14 as written is a 3-4 s failover. D13's 8 s hold covers it. If Alice wants
~1.5 s, election_timeout has to drop to about 400/800 ms, and then the
append deadline (§12.5) and a follower's durable point have to fit in 800 ms.

### 2b. The same, with a follower SIGSTOPped for 5 s

```
s3-consensus kill-leader --rate 200 --entry-bytes 65536 --secs-before 10 \
             --secs-after 10 --stop-follower-secs 5
steady state before the freeze:   n=515 p50=3.30 p90=3.80 p99=14.80 max=31.92 (ms)
SIGSTOPped follower 2001 for 5 s
while follower 2001 is frozen:    n=516 p50=3.20 p90=3.48 p99=18.33 max=31.99 (ms)
SIGCONT 2001; leader is at index 1032
  follower 2001 caught up to Some(1032) 460 ms after SIGCONT
after the resume:                 n=479 p50=3.73 p90=5.27 p99=16.56 max=27.23 (ms)
before kill: 1510 acknowledged writes
killed 1001 at t=0
new leader 2001 after 3048 ms; first committed write after 3051 ms
node 2001/3001: applied 2544, acknowledged 2544, missing 0
VERDICT acknowledged-writes-missing=0
```

Three things worth keeping:

- **A frozen follower costs nothing.** Commit latency during the 5 s freeze is
  identical to the steady state (p50 3.20 vs 3.30 ms): the leader commits on
  its own fsync plus the one live follower. Quorum of 2 of 3 is doing exactly
  what it should, and the write path never waits for the slowest node.
- **Catch-up is fast and automatic**: 516 entries (~33 MiB) replayed into the
  resumed follower in **460 ms**, no operator action, no snapshot.
- The node that had just been frozen (2001) then **won the election** after the
  leader was killed, because by then it was caught up. Nothing in openraft
  penalises a node for having been paused once it is current — which is right,
  but it means a pod that GC-pauses repeatedly can still become leader. §14's
  dashboard should show per-follower replication lag, not just liveness.

## 3. Leadership transfer, 5 rounds + an unreachable target (scenario 3)

```
s3-consensus transfer --rounds 5
round 1: 1001 -> 2001  role change  7 ms   first committed write 16 ms
round 2: 2001 -> 1001  role change 28 ms   first committed write 31 ms
round 3: 1001 -> 2001  role change 28 ms   first committed write 36 ms
round 4: 2001 -> 1001  role change 28 ms   first committed write 31 ms
round 5: 1001 -> 2001  role change 29 ms   first committed write 31 ms
healthy transfers: role change  n=5 p50=28.30 p90=28.46 max=28.56 mean=24.05 (ms)
healthy transfers: first write  n=5 p50=30.96 p90=34.41 max=36.36 mean=29.16 (ms)
```

Five for five, **~28 ms to the role change and ~31 ms to a committed write**
(the first round is faster because the 5 ms polling loop caught it early).
That is well inside §12.7's 3 s step-down budget and makes zero-downtime
deploys a non-issue as long as the target is healthy and caught up.
Round 1 shows the error a client sees during the window:
`NotLeader { hint: None, reason: "LeaseExpired" }` — retryable, no hint.

The unreachable target (GH#2088), after `kill -9` of the target:

```
killed 1001; asking 2001 to transfer leadership to it
trigger returned Ok(())
failed transfer: a node reported is_leader again at Some((2001, 0));
                 first committed write by (node, ms) Some((3001, 3357))
```

`trigger().transfer_leader` is fire-and-forget and returns `Ok(())`. The source
(2001) **keeps reporting `is_leader = true` from t=0** while refusing every
write, and the cluster takes no write for **3.36 s**, until 3001 wins an
ordinary election. Same shape as the laptop (2.6-3.1 s), one election timeout
longer here.

## 4. Linearizable reads, rate driven (scenario 4)

`ensure_linearizable(ReadPolicy::ReadIndex)` on the leader, open loop from a
pool of 128 connections, 30 s per point. "single" = one barrier per read;
"window" = §9.4's fixed 2 ms batch; "coalesce" = issue the barrier immediately
and let everything arriving while it is in flight ride the next one.

```
s3-consensus linearizable --rates 1000,5000 --secs 30
offered   mode       achieved   service p50   p90    p99    max      response p50
1000/s    single     1000/s     0.16          0.20   0.25   18.75    1.11
1000/s    window     1000/s     1.28          2.40   2.60   58.62    2.67
1000/s    coalesce   1000/s     0.16          0.21   0.28    3.54    1.08
5000/s    single     4999/s     0.21          0.27   0.36   25.25    1.33
5000/s    window     4999/s     1.47          2.72   2.90   44.28    2.88
5000/s    coalesce   4999/s     0.23          0.29   0.38   12.24    1.18
```

(The ~1 ms floor in the response column is the driver's own pacing granularity,
not the read; compare the service columns.)

A ReadIndex barrier costs **0.16-0.23 ms** on this VM at both rates, and the
leader absorbs 5000 reads/s without the achieved rate moving. **§9.4's fixed
2 ms window multiplies that by 7-8x** (1.28 and 1.47 ms p50) and buys nothing:
coalescing into the in-flight barrier is free at both rates and also cuts the
tail (max 3.5 ms vs 58.6 ms at 1000/s, because a read never waits for a sleep
that has just started). Same verdict as the laptop, now at real rates:
**§9.4 should say "coalesce into the in-flight barrier, at most one in flight",
not "batch every ≤ 2 ms"**.

The barrier really asks a quorum (same run, after killing both followers):

```
with both followers dead, a linearizable read failed after 2001 ms:
  not enough for a quorum, cluster: {voters:[{1001,2001,3001}]}, got: {1001}
the local (stale) read still answers: applied=Some(51)
```

## 5. Manifest snapshots, 1 GiB (scenario 5)

1 GiB of state in 16 MiB files (64 of them), 4 MiB entries, the log purged so a
new member can only be caught up by a snapshot:

```
s3-consensus --file-bytes 16777216 --keep-logs 0 \
             snapshot --total-bytes 1073741824 --entry-bytes 4194304
filled 256 entries of 4194304 B in 14.0 s (73.1 MiB/s)
snapshot built at index Some(257) in 0.2 s      (build = seal + per-file xxh3)
purged log up to 257: Some(257)
learner caught up at index Some(258) in 2.6 s (398.1 MiB/s of manifest)
```

- **Fill:** 73 MiB/s through the write path with one 4 MiB entry in flight
  (6 x 1 GiB written across the three nodes in 14 s ≈ 440 MiB/s of disk).
- **Snapshot build: 0.2 s for 1 GiB** — sealing the files and hashing them with
  xxh3. Extrapolated linearly that is ~2 s for 10 GiB, which is why I8 declares
  the build an exception rather than an O(1) operation, but it is cheap.
- **Transfer to an empty learner: 2.6 s for 1 GiB = 398 MiB/s** over loopback
  with a warm page cache. It says the framing, the per-file hashing and the
  install are not the bottleneck; it does not predict a real network.

Resume after killing the learner mid-transfer:

```
s3-consensus --file-bytes 16777216 --keep-logs 0 \
             snapshot --total-bytes 1073741824 --entry-bytes 4194304 --kill-after-ms 400
killed the learner 0.5 s into the transfer
learner restarted; the leader retries and the receiver keeps whole files
learner caught up at index Some(257) in 3.4 s (298.7 MiB/s of manifest)
# sender's log:
15:06:18.380  snapshot to 127.0.0.1:26104: 64 of 64 files needed
15:06:19.543  snapshot to 127.0.0.1:26104: 53 of 64 files needed
# learner's log after the restart:
15:06:21.784  Raft::install_full_snapshot()  ... install complete snapshot
15:06:21.792  Done install complete snapshot, meta: {last_log:T1-N1001.257, ...}
```

**Per-file resume works at this size**: 11 of 64 files (176 MiB) survived the
kill and were accepted on their recorded size and xxh3, so only 848 MiB was
re-sent, and the whole thing — including the process restart — took 3.4 s
against 2.6 s for the clean run. The install is a CURRENT-flip (I17), visible
in the learner's log as one `install complete snapshot` a few milliseconds
after the last file lands.

## 6. The wiped voter, wrong way then right way (scenario 6)

```
s3-consensus wiped-voter
before: node 1001 ... last_log=Some(201) applied=Some(201) count=200 voters=[1001,2001,3001]
restarted 2001 with an empty directory and the SAME id
  wrong-way state: node 2001 leader=Some(1001) term=1 last_log=None applied=None
                             count=0 voters=[] learners=[]
  leader after the wrong way: Some(1001)
  write after the wrong way: Ok(202)
removing 2001 from the membership, keeping [1001, 3001]
started the replacement as node 2002
learner caught up in 51 ms
promoted; voters are now [1001, 3001, 2002]
write after the right way: Ok(208)
```

**The wrong way is completely silent on Linux too.** No refusal, no panic, no
metric, no log line: the wiped node comes back under the same id with an empty
vote store, an empty membership and an empty log, is re-filled by the live
leader, and the cluster keeps accepting writes as if nothing had happened. A
voter that has forgotten which term it voted in can grant a second, different
vote in that term — the hazard D21/I9 exists for.

openraft's FAQ (`src/docs/faq/07-troubleshooting/04-wipe-node-data.md` in the
pinned tree) says "Doing so will panic the leader", but the guard in
`src/progress/entry/update.rs:80-112` is a `debug_assert!`, taken only when
`allow_log_reversion` is off (its default, `config/config.rs:526,597,633`):
**in a release build there is no panic at all**, which is what both hosts
showed. The same FAQ page also says that removing the node from the membership
before wiping it is not safe either, as long as it rejoins under its old id —
which is exactly why §12.6 gives a replaced disk a new generation in its id.

**So I9 is ours**: the IDENTITY file (cluster_id, node_id, generation,
disk_uuid), fsynced before the node serves any Raft RPC, and a refusal to vote
when it does not match the membership record.

The right way (§12.6) works exactly as designed: remove from the membership,
wipe, come back as `ordinal*1000 + generation` = 2002, add as learner,
**caught up in 51 ms**, promote. Writes continue across the whole procedure.

## 7. Restart with `enable_leader_restore = false` (scenario 7)

```
s3-consensus restart --entries 2000
before: 1001/2001/3001 all applied=Some(2001) count=2000 term=1
all three stopped
right after the restarts, the nodes BELIEVE:
  [(1001, None, false), (2001, None, false), (3001, Some(2001), false)]
a node reports is_leader again 1656 ms after the restarts began: 3001
after:  1001/2001/3001 all applied=Some(2003) count=2000 term=3 leader=Some(3001)
write after the restart: Ok(2004)
VERDICT applied-state-preserved=true
```

All 2000 acknowledged writes are present on all three nodes after the restart,
the applied index never moved backwards, and a new election produced a leader
in **1.66 s** (D14 timings, no leader restore, so an election is required by
design). Note node 3001 reporting `current_leader = Some(2001)` from its
persisted vote while nobody was leading — the §14.1 trap the laptop found,
reproduced here: **readiness must be `is_leader` / applied progress, never
`current_leader`**.

## A note on log volume (operational, not a measurement)

`raft-log 0.4.6` logs one INFO line per entry (`RaftLog payload cache:
set_last_evictable: ...`). The three node logs of scenario 1 were **33 MiB for
a 3.5-minute run**; 63 877 / 63 854 / 63 861 of those lines were that one
message. The archived copies in `results/vm-2026-09-17/` have those lines
stripped (nothing else was touched). Whatever the adapter does in
`server/src/rsm/`, it must pin `raft_log` (and openraft's own per-entry targets)
to WARN — the same rule the broker already applies to itself: aggregates in the
logs, never one line per message.

## Budget and what was deferred

Wall clock on the VM, measured: **build 53 s cold / 22 s incremental**, then
the seven scenarios in **11 min 41 s** (first node log line
2026-09-17T14:54:40.571Z, last 15:06:21.985Z), against a budget of 30 minutes
beyond the build. (Corrected 2026-09-18: the memo and this file first said
"10 min 30 s, 14:55:55 → 15:06:25"; the window above is what the node logs
say.) Every scenario ran to its verdict, but the 1000/s point of scenario 1 was
cut by its own hard stop — see the correction in §1. Afterwards: no process left (`pgrep -af s3-consensus`
empty), no loop or dm device, data directories removed, disk back to 7.7 G used
of 193 G — which includes the 591 MiB of sources and build output left under
`/root/raft/s3-consensus/` so that the deferred runs need no rebuild.

Deferred by today's budget (Alice, 2026-09-17: short runs today), in the order
they should be run next:

1. **The 10 GiB manifest snapshot** (the size in the WP-0.5 row): build time,
   transfer throughput and resume at 160 x 64 MiB files. ~8 minutes of VM time
   for both runs; the profile is already in `run.sh vm`.
2. **5 kill repetitions instead of 3**, with the kills randomized across
   durable-point intervals and snapshot builds (§13.6's rule: pgless's kill
   tests always killed before the first checkpoint).
3. **dm-delay / dm-flakey on a follower** (50 ms disk), and the other §13.6
   faults this spike does not touch: lost fsyncs, ENOSPC on a follower, clock
   jumps, and a partial partition (the shape of openraft GH#2080).


---

# Refutation pass, 2026-09-18

Same VM, same pin. Driver:

```
rsync -az --exclude target --exclude results test/raft/spikes/s3-consensus/ \
      root@164.90.215.224:/root/raft/s3-consensus/
cd /root/raft/s3-consensus && cargo build --release          # 25.7 s incremental
ROOT=/root/raft/s3-consensus/data-refute ./run.sh vm-refute out-vm-refute
./flaky-log.sh run 10        # the dm-flakey dropped-writes run, root only
./flaky-log.sh selftest
```

Budget and occupancy: the box was checked free before the run (`pgrep -af
"target/release/(s1-store|s2-dedup|s4)"` empty, load average 1.00 at
06:40:18Z — other spikes had been using it earlier in the morning). The seven
scenarios of `vm-refute` took **2 min 56 s** (node logs 06:40:50.211Z →
06:43:46.546Z), the dm-flakey run **36 s** (06:43:53.816Z → 06:44:29.922Z), the
self-test ~15 s: about **6 minutes** of VM wall clock including the build,
against a 10 minute budget. Afterwards: no process, no dm device, no loop
device, `/var/tmp/s3-flakey-log` removed, data directories removed, disk back
to 8.4 G of 193 G (593 MiB of sources and build output left under
`/root/raft/s3-consensus/`). Raw output and node logs:
`results/vm-2026-09-18-refute/`.

Why this pass exists: two refuters showed that the log store — the component
that holds the write-ahead log of the whole system — had never been crash
tested, while the state engine of D9 was only chosen after `kill -9` and
dropped-writes runs, and that the adapter diverged in silence from §12.3's
"`save_committed` persisted". §8-§12 are the answer.

## 8. `raft-log` under `kill -9` of a follower, 10 rounds (scenario 8, new)

```
s3-consensus log-crash --rounds 10 --rate 200 --entry-bytes 65536 \
             --secs-per-round 2 --secs-down 1
```

Per round: 2 s of 64 KiB writes; read from the LEADER the index it counts as
matched for the victim (an index that node has acknowledged and that may
already be inside a commit quorum); `kill -9` the victim; keep writing for 1 s
on the surviving two; restart the victim and read what its process found on
disk **before openraft could replicate anything into it**
(`StatusResp::reopen_*`, captured between `WalLogStore::open` and `Raft::new`);
then read every index the reopened log claims (`Req::VerifyLog`).

```
round 1 : matched=194  reopened log last=194 committed=193 vote_term=1  sm applied=192  read-back 195/195
round 10: matched=2851 reopened log last=2851 committed=2850 vote_term=1 sm applied=2838 read-back 2852/2852
VERDICT rounds=10 dropped-writes=false log-reopened-below-matched=0 log-above-leader=0
        unreadable-log=0 vote-reversions=0 empty-reopen=0
        acknowledged-writes-missing=0 digests-equal=true
```

| measured over the 10 rounds | value |
|---|---|
| `reopen_last_log - matched` | **0 in all 10 rounds** — the log reopened exactly at the index the leader had counted |
| entries read back from the reopened log | **100 %** (195/195 … 2852/2852), no hole |
| persisted vote after the crash | never below the pre-crash term |
| state machine at reopen, behind its log by | 2 to 37 entries (the 1 s periodic durable point) — replayed forward by openraft every time |
| restart → the node answers again | 427-463 ms (p50 433) |
| catch-up to the leader's applied index | 93-367 ms, p50 213 |
| acknowledged writes missing anywhere at the end | **0** of 2950 |
| digests of the three nodes at the end | equal |

That last-but-two line is the path the refutation said had never been exercised:
**reopen a populated log whose state machine is behind its durable point, and
replay**. It ran 10 times here and 4 times per laptop pass.

## 9. The same with dropped unflushed writes (dm-flakey), 10 rounds

`flaky-log.sh` is S1's `flaky.sh` applied to the Raft log: the victim
follower's data directory lives on a dm-flakey filesystem (4 GiB, ext4, over a
loop device), the other two nodes stay on the normal disk so the cluster keeps
committing. Immediately before each `kill -9` the table is switched to
`drop_writes` — from that instant every write the filesystem issues is silently
thrown away and the page cache is discarded with the unmount, which is what a
power cut does to everything that was never fsynced.

The injector is verified, not assumed:

```
./flaky-log.sh selftest
selftest: buffered 16777216 -> 0 bytes, fsynced 16777216 -> 16777216 bytes
```

A 16 MiB file written without `fsync` comes back **empty**; a 16 MiB file
written with `conv=fsync` comes back whole. With that established:

```
VERDICT rounds=10 dropped-writes=true log-reopened-below-matched=0 log-above-leader=0
        unreadable-log=0 vote-reversions=0 empty-reopen=0
        acknowledged-writes-missing=0 digests-equal=true
```

Ten rounds, every one of them: the log reopened **exactly at the leader's
matched index** (never above it, never below it), every claimed index was
readable, the vote never regressed, and at the end all 2906 acknowledged writes
were on all three nodes with equal digests. The one visible effect of the drop
is on the record that is NOT fsynced: in round 1 the reopened `committed`
record was 2 entries behind the log instead of the usual 1 — which is exactly
the divergence §10 is about.

**This is the WP-0.3 bar, met: `raft-log` 0.4.6 never reopened past its durable
point, under `kill -9` and under dropped writes.** (Scope: fsyncs are dropped,
not lied about at the device level after a successful fsync; and this is a
follower's log under a live leader, not a whole-cluster power cut.)

## 10. `save_committed`: the divergence, its effect and its cost

§12.3 lists "`save_committed` persisted". The adapter, copied from openraft's
`examples/log-wal`, wrote the record and returned without any flush — the
divergence the refuters found, now a knob (`--committed none|buffered|fsync`).

Scenario 7 re-run the way it should have been: `kill -9` all three, with the
state machine's periodic durable point pushed out to 60 s so recovery must
replay the log.

```
s3-consensus --durable-ms 60000 --wait-recovery true restart --entries 2000 --kill9 true
# --committed none
before: all three applied=Some(2001) count=2000
reopened with: log last=Some(2001) committed=Some(2000) vote_term=Some(1), sm applied=None   (x3)
  -> the commit record for the last entry was lost on ALL THREE nodes
a node reports is_leader again 1109 ms after the restarts began
wait_for_recovery returned after 944 / 1011 / 1055 ms
VERDICT applied-state-preserved=true    (the window had already closed when the check ran)

# --committed fsync
reopened with: log last=Some(2001) committed=Some(2001) vote_term=Some(1), sm applied=None   (x3)
VERDICT applied-state-preserved=true
```

With `none` every node came back with a `committed` one entry behind its own
log: openraft then re-applies only up to 2000, and entry 2001 — already
acknowledged to the client — is applied again only when the next leader commits
over it. On the VM that took ~1.1 s and the scenario's check ran after it; on
the laptop, where the timing is different, the check caught the window and
printed it (RESULTS-laptop.md §10.1: `WENT BACKWARDS on 1001: last_applied
Some(301) -> Some(300)`, `applied-state-preserved=false`). Same defect, one
host caught it in the act.

With `fsync` the record is on disk and every node re-applies to 2001 from its
own log before serving anything.

What it costs, 64 KiB entries, 16 writers, 500/s offered (the rate §1 shows the
cell can take):

```
s3-consensus --committed <mode> latency --rates 500 --secs 20 --entry-bytes 65536 --writers 16
mode       achieved   service p50   p90     p99      mean    response p50
none       500/s      12.37         16.31   131.42   15.98   14.14
buffered   500/s      12.56         16.57   140.69   16.39   15.16
fsync      500/s      14.75         19.84   165.77   19.14   17.61
```

**`fsync` costs +2.4 ms on the p50 (+19 %) and nothing in throughput** at
500 entries/s = 31 MiB/s; `buffered` (`flush(false, _)`, the record reaches the
page cache but no fsync is paid) costs +0.2 ms and survives a process crash but
not a power cut. raft-log promotes a no-sync write to a sync write as soon as
it batches with any append flush (raft-log 0.4.6
`api/raft_log_writer.rs:115-131`).

`Raft::wait_for_recovery` — openraft's own alternative to persisting
`committed` (`storage/v2/raft_log_storage.rs:63-97`) — was timed in the same
run: **944-1055 ms** after a three-node restart, i.e. a restarted pod would
hold its local reads for about a second. Note its documented limit
(`raft/mod.rs:1896-1912`): a follower that applied an entry it had not yet
flushed can still recover a stale state, because the first-phase wait compares
against a `last_log_index` that has itself reverted. That is why the
recommendation below is the durable record FIRST and the wait as the gate on
top, not either/or.

## 11. The liveness shape of openraft GH#2080, reproduced (scenario 9, new)

The issue needs a partial partition. Scenario 9 builds one with a one-way link:
`Req::DropResponsesTo` makes a node keep PROCESSING a peer's Raft RPCs — so the
leader's heartbeats still renew the follower's leader lease — while its answers
never leave. Both followers do that to the leader, so the leader's quorum-ack
lease expires.

```
s3-consensus one-way-partition --secs-blocked 12
  t= 0.9s  1001*t1 lead=Some(1001) acked=901ms    2001=t1 lead=Some(1001)  3001=t1 lead=Some(1001)
  t=12.1s  1001*t1 lead=Some(1001) acked=12125ms  2001=t1 lead=Some(1001)  3001=t1 lead=Some(1001)
  during the block: 0 writes committed
  patience probe: after 10.0 s the write answered NOTHING (still waiting at 10 s)
  no node became leader while the fault lasted: the cell is WRITE-DEAD
  VERDICT phase=heal write-dead=22.8s recovery-after-operator=616ms
  VERDICT phase=kill write-dead=22.8s recovery-after-operator=3528ms (a normal D14 election)
```

Three things this settles for D11 and D13:

1. **The pinned rev is affected by the mechanism.** The term never moves, no
   node campaigns, the leader keeps `is_leader = true`, and nothing commits for
   as long as the fault lasts. The cell is write-dead until something outside
   it acts: healing the link gives writes back in **616 ms**, `kill -9` on the
   stuck leader in **3528 ms**.
2. **A client write does not fail, it hangs.** Ten seconds, no answer. D13's
   "hold, then 503" is written for "no leader is known"; here a leader IS known
   and it accepted the request. §12.1's `propose` needs its own deadline
   (`ProposeError::Timeout`, the entry stays in flight per I3) and the receiver
   must turn that into the 503 itself, or Queen inherits the hang.
3. **The alert exists.** `RaftMetrics::last_quorum_acked` (exposed here as
   `quorum_acked_ms_ago`) grows without bound on the stuck leader — 901 ms at
   t=0.9 s, 12 125 ms at t=12.1 s — while the followers report none. §14's
   "leader whose quorum-ack lease has expired" is implementable today with
   nothing more than this metric.

Scope, stated plainly: this is the LEASE INTERACTION of #2080 on the pinned
rev, not the issue's own bridge topology (which needs a node that reaches both
sides). It shows the deadlock is reachable in a 3-node cell with a one-way
link; it does not measure how likely that shape is in a Kubernetes cell.

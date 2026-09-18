# WP-0.5 / spike S3 — laptop results

Host: Apple M4, 10 cores, 24 GiB, macOS 15.5, APFS on the internal SSD. All
three node processes and the driver run on this one machine over loopback TCP.
Date: 2026-09-17. Branch `raft`, base `fc71b65b`.
Everything below comes from one `./run.sh laptop` invocation unless a command
line is given; its raw output and the node logs are in
`results/laptop-2026-09-17/`.

**These are smoke numbers, not numbers to quote** (PLAN_RAFT.md §0.3: the Linux
VM measures, macOS does not). macOS fsync on APFS dominates and three nodes
share one disk queue. What the laptop does answer is *behaviour*: failover,
transfer, read barriers, snapshot install and resume, wiped disks, restart.

## 0. Build and library conformance

```
cargo build --release          # openraft git rev 54094270 (0.10.0-alpha.34 + GH#2095 fix), raft-log 0.4.6
cargo +1.88 check              # MSRV 1.88 (C-3): OK
cargo test
  test logstore::tests::wal_log_store_passes_openraft_suite ... ok   (9.96 s)
```

**CORRECTED 2026-09-18 (refutation).** The sentence that stood here — that
passing `openraft::testing::log::Suite` means the §12.3 storage contract
"holds for that crate as used here" — was false for three of the six clauses.
What the suite actually establishes is the API semantics of one open store
instance:

- `run_test` (pinned tree, `openraft/src/testing/log/suite.rs:1794-1805`)
  builds ONE store per case and never reopens it;
- `save_vote` (suite.rs:966-973) reads the vote back from that same live
  instance;
- `get_initial_state_re_apply_committed` (suite.rs:929-963) calls
  `read_committed()` on the same instance, and skips itself if it is `None`;
- no case in the suite closes a store, crashes a process or drops a write.

So the suite cannot see "flush callback only after fsync", "`save_vote` durable
before returning" or "`save_committed` persisted". It covers membership,
`get_initial_state`, ranges, truncate/purge and log-id arithmetic — necessary,
not sufficient. Demonstration, not argument: the test now runs the suite
against all three `CommittedDurability` settings of `logstore.rs`, and it
passes for every one of them **including the setting that never flushes the
commit record**:

```
cargo test --release -- --nocapture      # 29.6 s for the three passes
openraft testing::log::Suite passed with save_committed=none
openraft testing::log::Suite passed with save_committed=buffered
openraft testing::log::Suite passed with save_committed=fsync
```

The durability clauses are measured in §10 below (and on the VM), not here.
The adapter is still ~230 lines, most of it openraft's own `examples/log-wal`.

## 1. Commit latency, 64 KiB entries (scenario 1)

`--fsync periodic` (§11.4: the durable point is a 1000 ms timer; the Raft log is
what makes a committed entry survive a crash), one `client_write` in flight (D4):

```
s3-consensus latency --rates 200,500,1000 --secs 5 --entry-bytes 65536 --writers 1
offered   achieved   committed   p50     p90     p99     max
200/s     87/s       998         11.09   13.23   21.07   30.42   ms
500/s     86/s       1289        11.13   13.33   21.87   38.89
1000/s    87/s       1305        11.08   13.80   20.66   36.04
```

One entry in flight is a hard ceiling of `1 / commit-latency`: **~87 entries/s
here**. None of the three offered rates can be met under D4 as written, so only
the service-time column is meaningful (the coordinated-omission-correct
response times are seconds of backlog).

With 16 concurrent writers — which is what §7.1's batcher does in the real
system, openraft coalescing many commands into one entry:

```
s3-consensus latency --rates 200,500,1000 --secs 5 --entry-bytes 65536 --writers 16
offered   achieved   p50     p90     p99     response p50
200/s     199/s      16.83   21.06   25.06   19.26   ms
500/s     501/s      15.62   26.05   50.34   17.67
1000/s    705/s      20.31   33.08   46.71   864.70
```

So 500 × 64 KiB/s = 31 MiB/s of entries is met comfortably and the wall is
around 700/s ≈ 44 MiB/s (every byte is written twice per node — Raft log and
state machine — on three nodes sharing one SSD).

What durability costs, same shape at 50/s, one writer:

```
s3-consensus --fsync <mode> latency --rates 50 --secs 4 --entry-bytes 65536 --writers 1
mode        p50     p90     p99     achieved
periodic    8.00    10.97   16.23   50/s      §11.4, the default
batch      29.99    37.37   45.02   33/s      a durable point per apply batch
never       8.48    10.61   12.43   50/s
```

A per-entry state-machine durable point costs ~22 ms on macOS and must never be
on the write path; §11.4's periodic durable point is the right design, and I11
(truncate the data files to the lengths the last durable point recorded,
re-apply the rest from the log) is what makes it safe. `periodic` ≈ `never`:
what is left is the Raft log fsync.

## 2. Transport deadline: a finding, not a number

The first runs were pathological — 20 entries/s, p50 47 ms — with the leader
log full of:

```
WARN HeartbeatWorker(id=1001, target=2001) failed to send a heartbeat:
     Unreachable { no answer from 127.0.0.1:26102 within 75ms }
WARN ReplicationCore recv RPCError: Unreachable node: ... within 75ms,
     when:(stream-replication)
```

openraft builds the replication `RPCOption` from `heartbeat_interval`
(`replication/mod.rs`: `rpc_timeout = heartbeat_interval`) and `soft_ttl()` is
3/4 of it, so with D14's 100 ms heartbeat **every append round gets 75 ms**. A
follower that fsyncs its log needs more, and a transport that enforces
`soft_ttl` as its timeout tears the replication stream down and rebuilds it on
every slow append.

The fix is one line (`net.rs`: `soft_ttl().max(APPEND_TIMEOUT_FLOOR)`, 5 s) and
it took the same workload from 20/s, p50 47 ms to 87/s, p50 11.1 ms, with zero
`Unreachable` warnings. **For §12.5: do not derive the append deadline from the
library's TTL.** Vote and pre-vote should keep it (elections must be snappy);
append and snapshot need a deadline sized by the follower's durable-point
budget.

## 3. Failover: kill -9 the leader (scenario 2)

```
s3-consensus kill-leader --rate 50 --entry-bytes 16384 --secs-before 4 --secs-after 4
before kill: 126 acknowledged writes
killed 1001 at t=0
new leader 2001 after 3023 ms
first committed write after 3055 ms
node 2001: applied 255 ids, acknowledged 255, missing 0
node 3001: applied 255 ids, acknowledged 255, missing 0
VERDICT acknowledged-writes-missing=0
```

No acknowledged write was lost. The 3.02 s is the interesting part, and the
node logs say exactly where it goes:

```
16:09:26.3858  last contact from the leader (the kill)
16:09:29.4035  election timeout expired, triggering election      (+3.018 s)
16:09:29.4180  a quorum granted my vote
16:09:29.4215  id=2001 becomes leader                             (+18 ms)
# and from the other follower, in the same run:
handle pre-vote request: ... last_update: 16:09:26.385822(3.018839584s ago),
                             lease: 2s, expire at: 16:09:28.385822
```

The election itself takes **~15–20 ms**. Everything before it is
`Utime::is_expired(now, timeout) = now > last_update + lease + timeout`
(`utime.rs:150`) with `lease = election_timeout_max` (`engine_config.rs:56`)
and `timeout` the resampled random election timeout:

> **detection = election_timeout_max + rand(election_timeout_min ..
> election_timeout_max) = 3.0 – 4.0 s under D14's 100/1000/2000.**

D14 as written means a 3–4 s failover, not 1–2 s. D13's 8000 ms hold covers it
with roughly 2× margin for the new leader's blank entry and for clients
re-resolving. If Alice wants ~1.5 s failover, election_timeout has to be about
400/800 ms — and then the append deadline of §12.5 and the follower's durable
point have to fit inside 800 ms, which is the trade to put in front of her.

## 4. Leadership transfer (scenario 3)

```
s3-consensus transfer
transfer to 2001: OK, 2001 reports is_leader after 44 ms
first write committed on the new leader 53 ms after the trigger
killed 1001; asking 2001 to transfer leadership to it
trigger returned Ok(())
failed transfer: a node reported is_leader again at Some((2001, 0));
                 first committed write by (node, ms) Some((3001, 3052))
```

- Healthy target: **44 ms** to the role change, **53 ms** to a committed write.
  Good enough for §12.7's zero-downtime deploys (preStop step-down).
- Unreachable target: `trigger().transfer_leader` returns `Ok(())` — it is
  fire-and-forget — and the cluster then **takes no write for 2.6–3.1 s** (three
  runs) until another follower wins a normal election. GH#2088 was closed on
  2026-09-13 with exactly that answer: the source leader deliberately does not
  recover, "the simpler fallback is to let another follower win a normal
  election".
- **Trap for §14.1 and §12.7:** throughout those seconds the source keeps
  reporting `is_leader = true` while refusing every write (it forwards to the
  transfer target). `is_leader` is not a readiness signal, and a preStop hook
  must confirm that *another* node committed something, not that the transfer
  call returned.

## 5. Linearizable reads (scenario 4)

`ensure_linearizable(ReadPolicy::ReadIndex)`, 5 s per point, on the leader:

```
s3-consensus linearizable --concurrency 1,8,32 --secs 5
mode        c=1                      c=8                      c=32
single      18 001/s  p50 0.05 ms    68 308/s  p50 0.11 ms    109 965/s  p50 0.28 ms
window 2ms     269/s  p50 3.68       2 142/s  p50 3.73          8 363/s  p50 3.91
coalesce    17 843/s  p50 0.05      65 962/s  p50 0.12        111 041/s  p50 0.28
```

("single" = one `ensure_linearizable` per read; "window" = §9.4's fixed 2 ms
batch; "coalesce" = issue the barrier immediately and let everything that
arrives while it is in flight ride the next one.)

That the barrier really asks a quorum, from the same run:

```
with both followers dead, a linearizable read failed after 2002 ms:
  not enough for a quorum, cluster: {voters:[{1001,2001,3001}]}, got: {1001}
the local (stale) read still answers: applied=Some(1)
```

**The fixed 2 ms window is a pessimisation**: a ReadIndex round costs ~50 µs on
loopback, so the window adds 70× its cost and cuts throughput by 13×.
Coalescing into the in-flight barrier is free at every concurrency measured.
§9.4 should say *coalesce into the in-flight barrier*, not *batch every ≤2 ms*;
on a real network the two converge, and a window only ever helps when the
barrier is slower than the window — so if a window stays in the design, make it
adaptive (at most one barrier in flight), never a fixed sleep.

## 6. Manifest snapshots (scenario 5)

128 MiB of state in 8 MiB files, the log purged so a new member can only be
caught up by a snapshot:

```
s3-consensus --file-bytes 8388608 --keep-logs 0 snapshot --total-bytes 134217728 --entry-bytes 1048576
filled 128 entries of 1048576 B in 2.1 s (60.4 MiB/s)
snapshot built at index Some(129) in 0.1 s
purged log up to 129: Some(129)
learner caught up at index Some(130) in 0.2 s (584.2 MiB/s of manifest)
```

(584 MiB/s is loopback with a warm page cache; it says the framing and the
per-file hashing are not the bottleneck, nothing more.)

Resume after a killed transfer — 256 MiB, 32 files, learner killed 150 ms in:

```
s3-consensus --file-bytes 8388608 --keep-logs 0 snapshot --total-bytes 268435456 \
             --entry-bytes 1048576 --kill-after-ms 150
killed the learner 0.2 s into the transfer
learner caught up at index Some(257) in 1.1 s (238.8 MiB/s)
# from the sender's log:
snapshot to 127.0.0.1:26104: 32 of 32 files needed
snapshot to 127.0.0.1:26104: 13 of 32 files needed
```

19 of 32 files (152 MiB) survived the kill and were accepted on their recorded
size and xxh3, so only 104 MiB was re-sent. **Per-file resume works** and costs
only the hash of what is already on disk.

**The 10 GiB run of the WP row was not done on the laptop**: the machine had
7.7 GiB free and a 512 MiB attempt died with `No space left on device`. The
write surfaced it as `Fatal`, which is the mapping §11.8 asks for (ENOSPC on
the consensus log is fatal for that node). `./run.sh vm` carries the 10 GiB run
and a 15 s-kill resume run for the VM.

## 7. The wiped voter (scenario 6)

```
s3-consensus wiped-voter
before: node 1001 ... last_log=Some(201) applied=Some(201) count=200 voters=[1001,2001,3001]
restarted 2001 with an empty directory and the SAME id
  wrong-way state: node 2001 ... last_log=None applied=None count=0 voters=[] learners=[]
  leader after the wrong way: Some(1001)
  write after the wrong way: Ok(202)
removing 2001 from the membership, keeping [1001, 3001]
started the replacement as node 2002
learner caught up in 53 ms
promoted; voters are now [1001, 3001, 2002]
write after the right way: Ok(208)
```

The wrong way is **silent**: no refusal, no panic, no metric, no log line about
a lost disk. The wiped node starts with an empty vote store and an empty
membership, rejoins under the same id, and the live leader re-fills it. Nothing
records that a voter just forgot which term it had voted in — which is the
hazard D21/I9 exist for: that node can now grant a second, different vote in a
term it had already voted in. This is the release-build behaviour of the
"undefined behaviour" the openraft FAQ describes (its guard is a
`debug_assert!`).

**So I9 has to be ours**: the IDENTITY file, checked before the node is allowed
to serve any Raft RPC. openraft will not do it for us.

The right way — remove from the membership, wipe, come back with
`node_id = ordinal*1000 + generation` (2001 → 2002), add as learner, wait,
promote — works exactly as §12.6 describes.

## 8. Restart with `enable_leader_restore = false` (scenario 7)

```
s3-consensus restart --entries 50
before: 1001/2001/3001 all applied=Some(51) count=50
all three stopped
right after the restarts, the nodes BELIEVE:
  [(1001, None, false), (2001, Some(1001), false), (3001, Some(1001), false)]
a node reports is_leader again 1277 ms after the restarts began: 1001
write after the restart: Ok(53)
VERDICT applied-state-preserved=true
```

- Applied state survived on all three nodes. In an earlier run one node moved
  *forward* across the restart (it had committed index 51 without applying it
  and re-applied from the saved committed log id) — which is why
  `save_committed` must be implemented (§12.3) and why a checker must compare
  "no acknowledged write missing", not "identical applied index".
- A new election was needed, as intended, and took **1.28 s**.
- **Trap:** for that second and a quarter every restarted follower still
  reports `current_leader = Some(1001)` from its persisted vote while nobody is
  leading. The driver's leader lookup had to be rewritten to ask each node
  `is_leader`. §14.1 must not build readiness on `current_leader`.

## 9. What the laptop could not answer

- Real commit latency and the 200/500/1000 entries/s points (fdatasync ~1 ms on
  the VM against ~8–11 ms of effective cost here) — WP-0.5 part 2.
- The 10 GiB manifest: transfer throughput, the cost of hashing 10 GiB at build
  time (I8's declared exception) and resume at that size.
- Anything needing a real network: whether the append-deadline floor is right,
  what read-barrier batching is worth at 0.5–1 ms RTT, and whether the 3–4 s
  failover holds when the RTT is not 50 µs.
- Lost fsyncs, slow disks (dm-delay/dm-flakey), clock jumps — §13.6. openraft's
  own turmoil and Jepsen-style harnesses do not model them either.


## 10. Refutation pass, 2026-09-18 (scenarios 7 kill -9, 8 and 9)

Two refuters attacked the D11 memo on the same point: **the log store was never
crash-tested, and the adapter knowingly diverged from §12.3's
"`save_committed` persisted"**. Both were right. This section is the laptop
half of the answer (behaviour; the numbers are on the VM, RESULTS-vm.md §8-§11).
Everything here comes from one `./run.sh laptop-refute` invocation; raw output
and node logs in `results/laptop-2026-09-18-refute/`.

New harness pieces, all in this spike:

| piece | what it does |
|---|---|
| `logstore.rs` `CommittedDurability` | `save_committed` becomes a knob: `none` (openraft's example, what the spike shipped), `buffered` (`flush(false, _)`, page cache, no fsync), `fsync` (§12.3 as written) |
| scenario 8 `log-crash` | `kill -9` a FOLLOWER under load N times, restart it every time, and read what the process found ON DISK before openraft could replicate into it |
| scenario 9 `one-way-partition` | both followers keep processing the leader's appends but never answer: the lease interaction behind GH#2080 |
| `Req::VerifyLog` | reads every index the reopened log claims, one by one, and reports the first hole |
| `flaky-log.sh` | dm-flakey dropped unflushed writes under the victim's log (Linux only; S1's `flaky.sh` applied to the raft log) |
| `StatusResp::reopen_*` | log last/purged/committed/vote term and state-machine applied/count/digest as found at open |

### 10.1 A restart after `kill -9` DOES revert applied state without a durable `save_committed`

Scenario 7 as it was run on 2026-09-17 stopped the nodes with a graceful
`Req::Shutdown`, which flushes everything on the way out. Re-run with `kill -9`
and with the state machine's periodic durable point pushed out to 60 s
(`--durable-ms 60000`), so recovery really has to replay the log:

```
s3-consensus --durable-ms 60000 --wait-recovery true restart --entries 300 --kill9 true
# save_committed = none (openraft's examples/log-wal behaviour)
before: 1001 applied=Some(301) count=300 | 2001 applied=Some(301) count=300 | 3001 applied=Some(300) count=299
all three killed with -9
a node reports is_leader again 1212 ms after the restarts began: 1001
after:  1001 applied=Some(300) count=299   reopened: log last=Some(301) committed=Some(300) sm applied=None
  WENT BACKWARDS on 1001: applied_count 300 -> 299, last_applied Some(301) -> Some(300)
  node 1001 is missing 1 acknowledged writes
after:  2001 applied=Some(300) count=299   (same, missing 1)
after:  3001 applied=Some(299) count=298   reopened: committed=Some(299), missing 2
VERDICT applied-state-preserved=false
```

The entry is **not lost** — it is in every node's log (`log last=Some(301)`) and
is applied again once the new leader's term commits over it. What is lost is
the *pin*: for the window between restart and that commit, every node's applied
state is BEHIND what it had already answered to a client. That is exactly the
failure openraft's own contract text describes
(`openraft/src/storage/v2/raft_log_storage.rs:63-97`: "a read — linearizable or
not — may observe a state older than one already observed before the restart"),
and with D15 (local stale reads for fetch, browse, DLQ lists, stats, timers) it
is a Queen-visible defect, not a theoretical one.

The same run with `save_committed` made durable:

```
s3-consensus --durable-ms 60000 --committed fsync --wait-recovery true restart --entries 300 --kill9 true
after:  1001 applied=Some(301) count=300   reopened: log last=Some(301) committed=Some(301) sm applied=None
after:  2001 applied=Some(301) count=300   reopened: committed=Some(301)
after:  3001 applied=Some(301) count=300   advanced: applied_count 299 -> 300 (re-applied from saved committed)
VERDICT applied-state-preserved=true
```

Every node reopened with `committed = 301`, re-applied to 301 from its own log,
and node 3001 — which had been one entry behind when it was killed — moved
FORWARD to the committed state instead of backwards. The 2026-09-17 sentence
"Recovery is correct" belongs to the graceful case only; with `kill -9` it is
correct **only with a durable `save_committed`**.

### 10.2 The raft log survives repeated `kill -9` of a follower

```
s3-consensus log-crash --rounds 4 --rate 100 --entry-bytes 16384 --secs-per-round 1 --secs-down 0.5
round 3: leader matched=Some(181) victim last_log=Some(181) vote_term=1
  reopened: log last=Some(181) committed=Some(180) vote_term=Some(1); sm applied=Some(176) count=175
  log>=matched: OK   vote monotone: OK   (caught up in 74 ms)
...
VERDICT rounds=4 log-reopened-below-matched=0 log-above-leader=0 unreadable-log=0
        vote-reversions=0 empty-reopen=0 acknowledged-writes-missing=0 digests-equal=true
catch-up after restart: n=4 p50=85.86 max=98.67 (ms)
```

Every round: the log reopened at or above the index the leader had counted as
matched for that node, the persisted vote never went backwards, and the state
machine reopened BEHIND its log (applied 176 against log 181 — the periodic
durable point) and was replayed forward by openraft. That replay path is the
one the refutation said had never run; it now runs four times per laptop pass
and ten times per VM pass (RESULTS-vm.md §9).

### 10.3 The liveness shape of GH#2080 reproduces on the pinned rev

Scenario 9 makes both followers process the leader's `append_entries` and never
answer them (`Req::DropResponsesTo`, one-way link). The leader therefore loses
its quorum-ack lease while its heartbeats keep renewing the followers' leases:

```
s3-consensus one-way-partition --secs-blocked 8
  t= 0.9s  1001*t1 lead=Some(1001) acked=906ms   2001=t1 lead=Some(1001)   3001=t1 lead=Some(1001)
  t= 7.3s  1001*t1 lead=Some(1001) acked=7330ms  2001=t1 lead=Some(1001)   3001=t1 lead=Some(1001)
  during the block: 0 writes committed
  patience probe: after 10.0 s the write answered NOTHING (still waiting at 10 s)
  no node became leader while the fault lasted: the cell is WRITE-DEAD
  VERDICT phase=heal write-dead=18.0s recovery-after-operator=441ms
  VERDICT phase=kill write-dead=18.0s recovery-after-operator=3097ms
```

Three findings, all new:

1. **The cell is write-dead for as long as the fault lasts.** The term never
   moves (`t1` throughout), no node campaigns, and the leader keeps reporting
   `is_leader = true`. This is the mechanism #2080 describes; the issue's own
   topology needs a bridge node, this one needs a one-way link, and both end in
   the same place.
2. **`client_write` does not fail — it hangs.** The patience probe waited 10 s
   and got no answer at all. D13 ("hold up to 8 s while NO LEADER IS KNOWN,
   then 503") does not fire here, because a leader *is* known and it accepted
   the request. §12.1's `propose` therefore needs its own deadline
   (`ProposeError::Timeout`, entry still in flight per I3) and the receiver
   must turn it into 503 itself.
3. **`RaftMetrics::last_quorum_acked` is the alert.** It is exposed in the
   spike as `quorum_acked_ms_ago` and grows without bound on the stuck leader
   (906 ms → 7330 ms) while the followers report none. §14's alert
   "leader whose quorum-ack lease has expired" is implementable today.

Operator recovery, both measured: healing the link gives writes back in
**441 ms**; `kill -9` on the stuck leader gives them back in **3097 ms** (a
normal D14 election). Neither is automatic — something outside the cluster has
to act.

### 10.4 What `save_committed` costs (laptop, indicative only)

```
s3-consensus --committed <mode> latency --rates 200 --secs 8 --entry-bytes 65536 --writers 16
mode        service p50   p90     p99      achieved
none        29.63         46.28   106.67   199/s      (first run, cold page cache)
buffered    16.89         22.46   36.81    199/s
fsync       17.34         27.12   80.56    199/s
```

macOS `F_FULLFSYNC` serializes and three nodes share one disk queue, so these
say only that the fsync variant is not catastrophic. The number to quote is on
the VM (RESULTS-vm.md §11).

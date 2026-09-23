# Lanes: planning and apply in parallel inside one Raft group

Status: exploration, 2026-09-23. Nothing here is built. It is written from three
read-only maps of the code (planner, apply + store, queue logs + Raft storage) and
from the three-node VM measurements of the same day.

## 1. Why

Goal: at least 1M messages/s on **one queue**, on bigger machines, keeping one Raft
group: one log, one transaction manager, every transaction one log entry, no
distributed commit.

Three stages run on one thread per node today, and each would need less than
1 µs per message to reach 1M:

| Single-threaded stage | Measured per message | Where |
|---|---|---|
| Batcher with planning, leader | ~4.3 µs (8 vCPU, 87% busy at 200k) · ~6 µs (2 vCPU) | `batcher.rs`, `planner/` |
| Queue-log writer, every node | ~2.8 µs (2 vCPU) | `replicator/raft/log_store.rs`, `QlogWrite::write_group` |
| Apply, every node | ~1.4 µs (2 vCPU) | `apply.rs` |

Moving client work (HTTP, JSON, pop rendering) to followers halves the leader's CPU
but does not move these walls: on 8+ vCPU a node is already planner-bound at
~200–230k msg/s.

## 2. The model

- A node runs **N lanes**. A lane is **one thread** that owns a set of partitions
  and every piece of state keyed by them. On the leader it plans the commands for
  those partitions and proposes **lane entries**; on every node it applies the
  committed entries for those partitions. Planning and apply on the same thread:
  the lane's state has one owner, so neither needs a lock, and the planner reads
  the lane's tables directly instead of through a store read transaction.
- **Which lane**: `hash(tenant, partition name) mod N`. The name, not the pid,
  because a partition's first push arrives before it has a pid. A queue's
  partitions spread over all lanes, which is what one queue at 1M needs. The queue
  is left out on purpose, so the same partition name lands in the same lane in
  every queue (co-partitioning, §5). The exception is the default partition, which
  every single-partition queue uses: it hashes with the queue, or all of a tenant's
  single-partition queues would share one lane.
- A **control lane** owns everything not keyed by one partition: the catalog
  (queues, groups, partition name → pid), KV, timers, streams, traces, flags,
  quotas, and the commands that span lanes.
- **One Raft log** orders everything: lane entries (one lane) and **control
  entries** (the control lane, plus a part for each lane they touch).

Two invariants make parallel apply equal to serial apply:

- **L1.** A lane entry reads and writes only its own lane's state, plus a
  read-only catalog snapshot.
- **L2.** Each lane applies its lane entries, and its parts of control entries, in
  log order.

Lane entries of different lanes then touch disjoint state and commute: applying
them in parallel yields exactly the state of applying the log one entry at a time,
so every node still ends identical.

## 3. Who owns what

From the apply + store map (every keyspace is a RAM table since Phase C;
`store/mod.rs:385-403`).

| State | Owner |
|---|---|
| Partitions, Cursors, SegLoc, Dedup, Txns, DlqByPos, PartitionFiles, Garbage[pid], partition-scope counters | the partition's lane |
| Pending rows `(t,q,g,pid)` and the ready ring of `(t,q,g)` | each lane holds the rows and ring entries of **its own pids** |
| LeasesByWorker `(worker,pid,g)` | the pid's lane (re-keyed so the pid leads) |
| Dlq `(t,q,dlq_id)` | the lane of the pid it came from; `DlqDelete` must carry the pid (it does not today) |
| DedupFront (per-pid bloom generations) | one front per lane, with a per-lane byte cap |
| Queue, tenant and (queue, group) counters | **split**: each lane keeps its own share; readers sum the lanes |
| Queues, Groups, PartitionsByKey, QueuePartitions | control lane; lanes read versioned snapshots |
| Kv, KvExpiry, Timers, TimersDue, Streams*, Traces*, Flags, Quotas, EphConfig | control lane |
| Request outcomes | the lane that planned the command (a retry routes to the same lane); control commands in the control lane |
| Meta | per lane: applied position, last clock, pid counter; control: catalog and KV versions |

## 4. The global chains, and how each goes away

From the planner map, §5, and the apply gates (`apply.rs:1279-1326`).

1. **The predicted log index.** The batcher predicts each entry's index and stops
   if it lands elsewhere (`batcher.rs:2488-2497`). With N lanes proposing, no lane
   can know its index. A lane tracks its in-flight entries by its own sequence
   number, and apply reports progress per lane. Apply already assigns log
   positions itself where they matter (a group's registration position,
   `apply.rs:1510-1523`).
2. **The clock must never go backwards in log order (I5).** Lanes interleave in
   the log, so this becomes a per-lane check. Where two lanes' times are compared
   (subscription seeding, 7 below), a barrier gives the order.
3. **Partition ids and KV versions come from global counters (I18).** A pid
   encodes its lane: `pid = (lane counter << 8) | lane`, so each lane allocates its
   own and apply checks a per-lane base. KV versions stay in the control lane.
4. **Request ids are global.** A command's id lives in the lane that plans it; a
   retry carries the same partition key and routes to the same lane.
5. **Catalog reads.** Every push, pop and ack reads its queue's configuration,
   every pop its group. The control lane publishes an immutable, versioned catalog
   snapshot when it applies a catalog change; a lane entry records the version it
   planned against.
6. **Apply couples partitions.** An append arms a pending row for every group of
   its queue; a group registration arms every older partition (`apply.rs:2197-2222`,
   `:2312-2348`). The append's arming stays in its lane (groups from the snapshot);
   a registration is a control entry with a part in every lane, each arming its
   own partitions.
7. **Subscription seeding compares a group's registration time with appends'
   creation times** (`planner/mod.rs:36-45`, `:2105-2134`). Registration becomes a
   control entry planned in a **barrier**: every lane finishes its current cycle
   and waits, so every earlier append is older than the registration and every
   later one newer.
8. **Global mutable hot spots**: the DedupFront mutex and byte cap, the per-queue
   queue-log lock. Per lane (see §3 and §7).

## 5. Commands

| Command | Planned in | Notes |
|---|---|---|
| Push | the partition's lane | a request for several partitions is split per lane by the facade and the answers merged (items are independent; transactionIds make a retry safe) |
| Pinned pop, positional ack, nack, DLQ head, single-target ack | the partition's lane | already lane-local today |
| Multi-target ack, renew by worker | split per lane, answers merged | |
| Wildcard pop | one lane, chosen from a per-`(t,q,g)` "ready lanes" hint that lanes publish as atomics | the lane claims from its own ring; if it has nothing ready, the pop goes to the next lane with ready work. Long-polls park in the facade as today. First contact registers the group first (control, barrier) |
| Discovery pop | control lane, asking the lanes | rare |
| Transaction | control lane, in a barrier over the lanes it touches | still **one log entry**, each lane applies its part. A transaction whose partitions all sit in one lane plans in that lane with no barrier |
| Timer fire | control lane decides; the push goes to the destination lane | two entries instead of one. The push carries a deterministic transactionId (timer key + due time), so a fire repeated after a crash is deduplicated |
| KV | control lane | |
| Configure, queue delete, group delete, tenant purge | control entry in a barrier, a part per lane | |
| Retention, garbage | each lane for its partitions; catalog garbage in control | |

**Co-partitioning** keeps transactions cheap: because the lane is a hash of the
partition name, an ack in queue A and a push to queue B **with the same partition
name** land in the same lane. The common consume → produce transaction then plans
in one lane like any other command.

## 6. Apply on the lane thread

- Openraft hands committed entries to the state machine in log order. A small
  router sends each lane entry to its lane, and each control entry to the control
  lane and to every lane it has a part for.
- On the leader a lane thread alternates: plan a cycle and propose it, apply what
  has committed for its partitions. On followers the lanes only apply.
- **Waiters**: an entry counts as applied when every lane it touches has applied
  it (a countdown per entry). The batcher-side resolution becomes per lane.
- Per message on a lane thread: planning ~4 µs plus apply ~0.7–1.4 µs on today's
  code; with 8 lanes that is several times today's single planner.

## 7. Storage

- **RAM tables per lane.** Every keyspace is already in RAM (Phase C); a lane gets
  its own tables for what it owns (§3).
- **Checkpoint.** Today one durable point drains every keyspace in one LMDB
  transaction at one applied index (`heed_store.rs:604-635`, `apply.rs:3650-3719`).
  With lanes: every ~1 s the router stops at an index D, each lane applies up to D
  and swaps out its dirty set, the lanes resume, and one checkpoint thread writes
  all swapped sets to LMDB with durable index D. The pause is the swap, not the
  write. Snapshots and state digests keep a consistent cut at D.
- **Recovery** reopens at D and replays from the queue logs through the same
  router.
- **Queue logs: one log per (queue, lane).** Each lane is simply another log in
  the set, with its own non-zero u64 id and the usual `q<id>` directory, so
  `reopen_all` finds it (it silently skips any other name, `qlog/set.rs:303-309`).
  Recovery's merge, the complete/gapless rule, both truncations, the recovery
  floor, snapshot linking and Raft rehydration already work over any number of
  logs. Readers change only where they turn a queue into a log id
  (`queue_id_of`, about ten call sites); the per-pid read functions stay.
- **A lane entry touches one lane's log, so it is written once.** Today an entry is
  copied, payload-free, into every log it touches (about 41 B per pushed message
  per copy, `replicator/local.rs:446`). A control entry spanning lanes pays one copy
  per lane; it is rare.
- **One writer per lane.** Openraft's append batch is split by lane, each lane's
  writer writes and fsyncs its part, and the batch is acknowledged only when every
  lane has fsynced it and every earlier batch. Openraft's flush completion is a
  high-water mark (`openraft/storage/callback.rs:80-126`): acknowledging batch k+1
  before k is durable on every lane would let the leader count itself for a commit
  it does not hold. Truncation, purge and vote writes go through the coordinator,
  which drains the lanes first.
- **Two traps found in the map.**
  - Retention builds its live-message map by queue id; a lane id missing from it
    counts **every record as dead** and unlinks live data (`maintenance.rs:72-77`,
    `qlog/mod.rs:1939-1946`). Every lane id must get the queue's map.
  - A lane that runs ahead can seal a file holding records above the recovery cut;
    boot must then truncate with `truncate_from_across`, which handles sealed files.
- **A lane is fixed for the life of a partition.** It is encoded in the pid (§4),
  so it is decided at partition creation and replayed identically. N is a cluster
  setting, set when the cluster is created.
- **Followers must not recompress.** A follower gets no pre-started compression
  and runs zstd on its writer thread (`replicator/raft/types.rs:160-167`); at 1M
  msg/s of 256 B that alone is about one thread's capacity. The wire should carry
  the leader's compressed payloads, which also halves replication bandwidth.

## 8. What does not change

One Raft group and openraft; replication and snapshots; the client API; per-partition
order; every transaction one log entry; the store image and the queue logs as the
only write-ahead log.

## 9. How to trust it

- **N = 1 must behave exactly as today**: the whole existing suite.
- **Differential testing**: the same generated workload (pushes, pops, acks,
  transactions, deletes, timers) through N = 4 and through the serial path must give
  the same answer to every command and the same final state digest.
- Kill -9 and the three-node tests, with N = 4.

## 10. Phases

0. **Measure** where the planner's time goes today (reads vs decisions), and
   project the per-lane cost. 1–2 days.
0b. **Two cheap wins that do not need lanes**, found in the queue-log map:
   - the writer writes a group and fsyncs it on the same thread, back to back
     (`replicator/local.rs:478-486`); writing group k+1 while group k is being
     fsynced, still acknowledging in order, is a small change (the "Phase E"
     sketched in `ALICE_PGLESS_NEWARCH.md`);
   - send the leader's compressed payloads to followers instead of the raw ones.
   About a week together.
1. **Split the state by owner, still on one thread**: lane-encoded pids, per-lane
   counters, catalog snapshots, lane-tagged entries, per-lane request ids. Suite
   green at N = 1. About 1–2 weeks.
2. **Lane threads** for planning and apply; the control lane with barriers;
   per-lane rings and wildcard routing. About 2–3 weeks.
3. **Queue-log writers per lane.** About 1 week.
4. **Measure** on a big machine: one queue, 200 partitions, the soak shape. Target
   1M msg/s. About 2 days.

## 11. Risks

- **Barriers.** Every control command pauses the lanes it touches. A workload heavy
  in cross-lane transactions or group registrations would serialize. Co-partitioning
  (§5) keeps most transactions in one lane.
- **Wildcard pop fairness and latency** across lanes.
- **The refactor itself** touches the core of planner and apply; the differential
  test is the guard.
- **Replication at ~1 GB/s**: openraft over HTTP must carry it, likely with
  compressed payloads (0b) and a streaming transport.
- **Found while mapping, independent of lanes**: the planner does not fold
  in-flight deletes (`planner/mod.rs:927-931`), so a push planned in the same cycle
  as a queue delete can append to a partition being deleted and stop the node.
  Filed as a separate task.


## 12. What was built (2026-09-23), and how it differs from §2–§7

Built on branch `raft`, uncommitted, behind `QUEEN_LANES` (1 = the single
planner, untouched).

- **Fork-join per cycle, one entry per cycle.** Instead of lanes proposing their
  own entries (§2), each cycle's commands are routed, the lanes plan them in
  parallel, the control step plans after the join, and ONE entry carries
  lanes-then-control. The log, the predicted index, the waiters, recovery and
  the checkpoint are unchanged; the barrier is the join, every cycle.
- **Lane = `pid % n`**, not a name hash (§2): pids stay on the ONE global
  counter because only control creates partitions, so I18 is unchanged. The
  first push to a new partition is planned by control; later pushes by the lane.
- **Control** plans: new partitions/queues/groups, conflating and discovery
  pops, renew, KV, timers, admin, acks and transactions spanning lanes,
  commands on a queue with a catalog change or delete in flight, the leader
  steps. It sees every in-flight entry plus this cycle's lane effects.
- **Lane overlays** hold only their partitions (plus the catalog): each lane
  keeps its payload-stripped slice of every in-flight entry and advances over
  those; control's effects on its partitions are folded into its slice.
- **Wildcard pops** go to the lane whose last walk found the most ready
  partitions (round-robin when none is known); an empty result in a lane is
  re-planned by control, so a pop is empty only when the whole queue is.
- **Queue logs per (queue, lane)** (§7) are built independently of the planner:
  `qlog/LANES` fixes a directory's count at creation, a partition's records
  live in lane `pid % lanes`, lanes are written in parallel, and the fsync of
  group N overlaps the write of group N+1.
- **Not built yet:** parallel APPLY (§6): apply is still one thread per node.
  It needs per-thread store write handles and per-lane counter deltas merged
  after each entry.
- **Tests:** the whole suite runs with `QUEEN_LANES=4` as the differential test
  against the single planner.

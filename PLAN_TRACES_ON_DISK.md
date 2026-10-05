# Traces on disk

Status: shipped in 2.0.1.

## The problem

Every keyspace of the store is a RAM table since Phase C (`Keyspace::is_ram`
is `true` for all of them), so every trace lives in RAM on every node, at
about 1.5x its store bytes. On 2026-10-05 one client wrote ~18.6k traces/h of
~5.5 KB each, with a 7-day retention: every pod grew 115-150 MiB/h. The 2 h
retention stopgap then expired ~139k traces in one apply step (1.24 s), because
`TraceExpire` deletes everything past the cutoff at once.

## Options considered

1. **The three trace keyspaces LMDB-direct in the main env** (`is_ram() = false`
   for them). Rejected: since Phase C the write handle holds no LMDB
   transaction between durable points, and the checkpoint is written on its
   own thread (`take_cut`/`write_cut`), which needs every keyspace in RAM
   (`all_ram`). An LMDB-direct keyspace means an open write transaction on the
   apply thread, no async checkpoint (inline durable points of 46-75 ms on the
   apply thread), and apply blocking on LMDB's writer mutex while the
   checkpoint writes. It also reopens the two failures the Phase C note names
   (replay ahead of the checkpoint, visibility).
2. **Evict trace rows from RAM after the checkpoint wrote them** and read
   through to LMDB. Rejected: it changes the RAM tables, the scans (merge of
   RAM and LMDB with tombstones) and the reader/eviction ordering, which is the
   most delicate code in the broker, for one family of keyspaces.
3. **Queue logs / segments.** Rejected: they are per partition, their positions
   are node-local (D8) and their reclaim follows partition watermarks. Traces
   have an optional partition, need two secondary indexes and expire by time.
   We would need a new index anyway.
4. **Time-bucketed append-only files plus an index.** Expiry would be an
   `unlink`, but it adds framing, torn-tail recovery, its own fsync
   discipline, an index (LMDB again) whose positions are node-local, and a
   snapshot story for both. More moving parts than the problem needs.
5. **A second LMDB environment for traces only (chosen).** Same engine and
   the same pins as the store (heed, `MDB_NOSYNC`, a read-transaction handle,
   1024 readers, the map-size rule). Its pages are file-backed (page cache,
   reclaimable), not anon memory. It has its own writer, so it never contends
   with the main checkpoint, and its own applied-index watermark, which makes
   replay exactly-once without touching the main store's recovery.

## Format

`<data_dir>/traces/` (`data.mdb`, `lock.mdb`), five databases. Every value is
sealed with an xxh3 checksum (the store's format-1 helper, a seed per
database) and verified on read.

| db | key | value |
|---|---|---|
| `meta` | `applied_index` | u64: last entry whose trace writes are here |
| `bodies` | `(index u64, ordinal u32)` | `rows::trace_encode(event)` (the old row codec) |
| `by_txn` | `(tenant, pid?, txn, created_at, index, ordinal)` | empty |
| `by_name` | `(tenant, name, created_at, index, ordinal)` | `count u32, pid?, txn` |
| `expiry` | `(created_at, index, ordinal)` | `tenant, pid?, txn, names` |

Keys use the store's encodings (`keys::push_name`, sign-flipped big-endian
i64), so key order is the order the reads need. `(index, ordinal)` is the raft
index of the entry and the effect's position in it: it is the same on every
node and on every replay, so a trace has the same key everywhere. The old
`seq = max+1` is gone from the new path. Bodies are keyed in apply order, so
writes append at the right edge of the B-tree and expiry, which is almost the
same order, deletes from the left edge. The expiry row carries what is needed
to delete the index rows, so expiry and tenant purge never read a body.

## Effects (catalogue version 4)

- `TraceRecord { event }` (kind 33): the same body as `TraceAppend`. Applied to
  the trace env.
- `TraceTrim { cutoff_us, limit }` (kind 34): deletes at most `limit` legacy
  RAM traces (oldest first, as `TraceExpire` did) and at most `limit` disk
  traces with `created_at < cutoff`. `limit` is in the effect, so every node
  deletes the same rows.
- `TraceAppend` and `TraceExpire` (v1) apply exactly as before (RAM).
- `TenantPurge` also deletes the tenant's disk traces. For every entry written
  before the cluster reached version 4 the trace env is empty, so this changes
  no existing outcome.

## Write path

The facade emits `TraceRecord` when `cluster_allows(cluster, 4)`, else
`TraceAppend`. It refuses (400) a trace whose `transactionId` or trace name
would make a key longer than LMDB's 511 bytes. The planner refuses the same
(`name_too_long`). Before this change such a trace reached apply as
`KeyTooLong`, which is fatal and deterministic, and stopped every node.

Apply notes the ordinals of the trace effects of an entry (`TraceRecord`,
the disk half of `TraceTrim`, the disk half of `TenantPurge`). After the
entry's other effects it runs them in one trace-env write transaction, writes
`applied_index = entry index` in it, and commits (NOSYNC), before
`notify.applied`. So a trace is readable on the node that applied it when the
POST returns, as it was with RAM. A failed entry never commits that
transaction. Entries without traces pay nothing.

## Read path

Same routes, fields, ordering, pagination and tenant scoping. Each read merges
the legacy RAM rows (now a tenant-prefix scan instead of a scan of every
tenant's traces) with the disk rows:

- `GET /traces/:pid/:txn`: reverse scan of `by_txn` for `(tenant, pid, txn)`.
- `GET /traces/by-name/:name`: reverse scan of `by_name` for `(tenant, name)`.
- Both give `created_at` descending. A small pager groups equal timestamps and
  orders each group by `(pid, txn, apply order)`, which is the old order
  (stable sort over primary-key order). It counts `total` from the index and
  keeps only the page. Bodies are read only for the page's rows.
- `GET /traces/names`: forward scan of `by_name` for the tenant, aggregated
  without reading bodies. `count` keeps a trace that repeats a name counted
  twice, as before. Distinct messages are counted with 128-bit hashes instead
  of owned strings.

## Expiry

The leader's maintenance (every `RETENTION_INTERVAL`) emits `TraceTrim` when
the oldest legacy or disk trace is past retention, with `limit =
QUEEN_RAFT_TRACE_TRIM_LIMIT` (default 512). It sets `more` when more than
`limit` are due, so a backlog drains in back-to-back bounded steps instead of
one long one. Below version 4 it emits the old `TraceExpire`, unchanged.

## Crash and replay

- The trace env keeps `applied_index` (T) in the same transaction as the rows.
  Apply skips the trace half of any entry with index <= T, and applies it for
  index > T. This is exactly-once: a replay from the main store's durable
  point never duplicates or re-trims. The deterministic keys make even a
  double apply rewrite the same rows.
- Durability: the trace env is fsynced before the main store records a durable
  index: on the checkpoint thread before `write_cut` (the async point, the
  default), or inline before `durable_commit`. A trace committed for an entry
  <= the durable index is therefore on disk before the raft log can be
  truncated behind it. A failed trace fsync is a durable point that did not
  happen (`CommitFailed { durable: true }`), as for the store.
- The env may be ahead of the main store after a crash (NOSYNC commits that
  reached the page cache). That is the case the watermark handles.

## Snapshot (lagging follower)

At cluster version >= 4 the sender adds a compacting copy of the trace env
(`traces/data.mdb`, one read transaction) after the store copy. Its T is at
least the store copy's applied index A, because every trace commit of an entry
<= A happened during that entry's apply. The receiver swaps `traces/` in with
`store/`, `qlog/` and `seg/`. Replay from A+1 skips the trace half of entries
<= T. Below version 4 no trace was ever written to disk anywhere, and a
2.0.x receiver would refuse the unknown `traces/` path, so nothing is sent.
A receiver that gets a snapshot without `traces/` moves its own aside, so it
reopens empty, which is the correct state for that snapshot.

## Rolling upgrade from 2.0.0 / 2.0.1-beta

`SUPPORTED_KINDS_VERSION` becomes 4. The baseline stays 3.

1. While any member reads 3, the cluster version stays 3. New nodes emit
   `TraceAppend`/`TraceExpire`, apply them to RAM like old nodes, send no
   `traces/` in snapshots, and serve reads from RAM (the disk env is empty).
   Mixed clusters behave exactly as 2.0.1-beta.
2. When every member reads 4, the leader raises the cluster version through
   the existing `ClusterVersionSet` path. From that entry on, traces go to
   disk and expiry is bounded. Legacy RAM traces are trimmed in bounded steps
   as they age out, and are readable until then.
3. After the raise, an older build refuses to boot on the store (the existing
   D20 check) and cannot be added as a learner.
4. If a writer forgets the gate, the batcher refuses the entry (an entry above
   the cluster version fails its own commands), as for every v>3 shape.

This is the first catalogue version above the baseline that real clusters will
reach, so one existing D20 rule becomes visible: above the baseline, a leader
adds a learner only after asking it what it reads (`/raft/v1/state`). A node
must be running before it is added (`QUEEN_RAFT_JOIN` already works that way).
Two `raft_cluster` tests add a learner that is not running; their facades now
stay at the baseline (`cluster_version_every_ms: 0`), and the rule itself is
covered by `cluster_version.rs` and `raft_cluster.rs`.

## Memory and cost

Measured on queen-04 (8 vCPU), one node, release builds of 42647314a (base)
and of this change, 100k POSTs of ~5 KB traces from 16 keep-alive clients
(~4.8k/s both):

| | base | traces on disk |
|---|---|---|
| RssAnon before -> after 100k traces | 7.9 -> 818 MB (+810) | 8.4 -> 64 MB (+56) |
| RssAnon, 20 KB traces | +2206 MB | +74 MB |
| RssAnon after 3 x 100k traces (100k / 200k / 300k) | 820 / 1559 / 2303 MB | 65 / 97 / 130 MB |
| same, 75 s later (request ids expired) | 2282 MB | 47.8 MB |
| RAM trace rows after the load | 100k + 200k + 100k | 0 |
| reopen (checkpoint load) RssAnon | 750 MB | 46 MB |
| disk | store 876 MB | traces 845 MB, store 9.8 MB |
| apply per entry (server, ~1.2 traces/entry) | mean 25 µs, p99 131 µs | mean 55 µs, p99 262 µs |
| expiry of 100k traces | 1 step, 329 ms | 196 steps of 512, max 5.6 ms, all gone in 0.29 s |

What is left in anon memory is not traces: it is the request-id outcome of
every command (D6, `request_ids` + `request_expiry`, 60 s window), which went
from 300k rows to 112 a minute after the load, and allocator caching.

Isolated apply (release, `trace_apply_and_expiry_bench`, apply calls only):

| | 1 trace/entry | 8 traces/entry | expiry of 100k |
|---|---|---|---|
| RAM (`TraceAppend`) | 6.5 µs/trace | 5.2 µs/trace | 1 step of 374-449 ms |
| disk (`TraceRecord`) | 22.7 µs/trace | 9.3 µs/trace | 196 steps, worst 2.6-2.7 ms |

About 15 µs of the difference is the one `MDB_NOSYNC` commit per entry that
carries traces; the rest is the index rows. The RAM path's LMDB writes happen
later, on the checkpoint thread. Push, pop, ack and transaction entries pay
one `is_empty()` check.

## Open points

- Apply cost of a trace-carrying entry (~+16 µs at one trace per entry). If
  trace-heavy workloads matter, two options: `MDB_WRITEMAP` for the trace
  environment (the commit stops writing pages, but the file becomes
  map-sized and sparse, and the map rule must read the used pages instead of
  the file length), or a trace writer thread with group commits, with POSTs
  waiting for its visible index.
- `/traces/names` and `total` still walk the tenant's index rows per request
  (index-only and transient, no longer every tenant's bodies). Per-name
  counters would make them O(names).
- `TenantPurge` deletes a tenant's disk traces in one step, as it did in RAM.
- A trace environment lost from disk (deleted, or a node rebuilt without
  it) reopens empty, and the store does not notice: nothing records the trace
  environment's synced index in the store, as `QLOG_DURABLE_INDEX` does for
  the queue logs.
- The trace environment is not in the background scrub. Its size is exported
  as `queen_raft_traces_map_bytes` and `queen_raft_traces_stored`.

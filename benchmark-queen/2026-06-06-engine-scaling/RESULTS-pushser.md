# push-serialization architecture - validation results (image `:pushser`)

Build under test: `smartnessai/queen-mq:pushser` (this branch). VM
`queen-benchmark-01` (165.232.78.92), 32 vCPU, Postgres co-located. Compare
against the `0.16.0.beta.1-ui` baseline in `RESULTS.md` / the 2026-06-04 campaign.

## Correctness - PASS (the whole point)

`cursor-repro.sh` against the deployed `:pushser` schema: **ALL PASS**.

- NEG control: a non-commit-ordered `created_at` is skipped by the forward
cursor (the bug) - reproduced.
- T1: push partition lock is exclusive per partition AND structurally disjoint
from pop's claim key and ack's key (pop/ack still acquire while push holds).
- T2: concurrent push to one partition is commit-ordered (the later-committing
push blocked ~1.6s on the lock, then got the larger `created_at`); both
present, zero loss.
- T3: cursor-from-epoch sees both; nothing skipped.
- T4: 10 concurrent pushers x 50 msgs to ONE partition -> 500 rows, zero lost
inserts, all reachable.

Local JS suite (`client-js/test-v2`, `node run.js human`): green except the two
retention tests, which require `RETENTION_INTERVAL=2000` and are timing-based
(confirmed pre-existing / environmental).

Boot: `Engine cluster (function-split): push=125 pop=100 rest=25 slots` on the
new image; all HTTP workers share the cluster; pop path unchanged.

## Throughput - push-only ceiling, W=8, 1000 producers, batch=10, 300 partitions


| config                                | push/s    | broker vCPU | PG vCPU | PG active | push f= | evl   |
| ------------------------------------- | --------- | ----------- | ------- | --------- | ------- | ----- |
| baseline `0.16.0.beta.1-ui` (no gate) | ~160k     | ~3          | ~8      | ~16       | -       | ~0    |
| `:pushser`, default Vegas             | ~112k     | 1.9         | 5.0     | ~8        | 10/10   | <=5ms |
| `:pushser`, static `C=32`             | **~152k** | -           | -       | -         | 32/32   | -     |


0 push errors in all runs. Gate observability healthy throughout:
`pushgate(part=80 defer=0)` (80 of 300 partitions in flight across the
concurrent transactions, zero deferrals - the disjoint cap is working and not
starving).

### Reading

- The gate is correct AND not the bottleneck (`defer=0`, PG idle at 5/32 vCPU).
- The default-Vegas dip to ~112k is a concurrency-control artifact: the gate's
per-transaction work (advisory-lock pre-pass + `clock_timestamp` + sorted
lock acquire) raises p99 RTT to ~45ms, and Vegas - being RTT-adaptive - holds
the push limit at 10. PG has ample headroom, so this is conservative, not
resource-bound.
- Forcing concurrency up (static `C=32`) recovers **~152k ~= baseline**, proving
concurrency is the lever (consistent with the 2026-04 campaign:
"MAX_CONCURRENT is the throughput lever").

### Key fix during validation

First `:pushser` heavy run was only ~40k: the initial disjoint scheduler let the
first batch `take_front(500)` and mark ALL its partitions in-flight, collapsing
concurrency to 1. Fixed by capping distinct partitions per push batch
(`QUEEN_PUSH_MAX_PARTITIONS_PER_BATCH`, default 8) so up to C concurrent
transactions run on disjoint partition subsets. 40k -> 112k (Vegas) / 152k (C=32).

## Tuning knobs (no feature flags; engine counts fixed at 3)

- `QUEEN_PUSH_SLOTS` / `QUEEN_POP_SLOTS` / `QUEEN_REST_SLOTS` - per-function
connection slots (DB-concurrency budget).
- `QUEEN_PUSH_MAX_CONCURRENT` (= disjoint `C`) and `QUEEN_CONCURRENCY_MODE`
(`vegas` default | `static`) - the push throughput lever.
- `QUEEN_PUSH_MAX_PARTITIONS_PER_BATCH` (default 8) - distinct partitions per
push transaction; bounds monopolization so C transactions stay parallel.

## Ceiling sweep (dedicated loader VM 167.99.246.68, push-only, 1000 partitions)

Static push concurrency C swept; broker+PG had the 32-core box to themselves
(loader on the separate VM over the private VPC, ~9ms RTT). push/s = PG
`n_tup_ins` delta.


| C   | push/s   | PG vCPU | broker vCPU | PG active | Lock waiters | push p99 |
| --- | -------- | ------- | ----------- | --------- | ------------ | -------- |
| 8   | 114k     | 6.4     | 4.3         | 7         | 0            | low      |
| 12  | 147k     | 9.1     | 5.5         | 13        | 0.2          | low      |
| 16  | 165-183k | 11      | 6-7         | 16        | ~0.5         | 18ms     |
| 20  | 161k     | 11      | 6.3         | 21        | 0.9          | -        |
| 32  | 166k     | 11      | 6.4         | 32        | 6.6          | 64-123ms |
| 48  | 161k     | 12      | 6.2         | 44        | 10.5         | high     |
| 64  | 149k     | 12      | 6.0         | 58        | 24           | high     |
| 96  | 154k     | 12      | 5.8         | 83        | 28           | high     |


Findings:

- **New ceiling ~~175-185k push/s at C~~=16** - ABOVE the old ~160k baseline.
- **Not CPU-bound**: PG peaks at ~12/32 vCPU, broker ~6-7/32, at every C. The
wall is PG write-path **contention** (lock waiters climb 0 -> 28 as C rises),
not CPU - so there is real headroom for more with contention reduction.
- **Optimal concurrency is LOW (~16), not high.** Each disjoint coalesced commit
does more useful work, so few in-flight transactions saturate the write path;
pushing C past ~16 causes congestion collapse (more lock waiting, p99 18ms ->
120ms, throughput DOWN). This is why default Vegas (which converged to ~10 on
RTT) sat at 112k - it under-shot the ~16 optimum.
- **Side observation**: under sustained ~180k push the PARTITION_LOOKUP lane
(rest engine, concurrency 2) backlogs hard (q ~ 17k-27k). Harmless for a
push-only ceiling, but under balanced load it would lag pop-discovery
freshness - a follow-up (give partition_lookup more concurrency / coalesce
harder).

Recommended default: push concurrency ~16 (static), or tune Vegas to target ~16
rather than backing off to 10. Going beyond ~185k needs PG write-contention
reduction (identify the lock wait_event; the 3 indexes on queen.messages /
partition_lookup row updates), not broker changes.

## Post-fix: partition_lookup coalescing + the REAL ceiling (dedicated loader)

Three changes after the first sweep:
1. **partition_lookup coalescing** (push engine): push completions now merge their
   partition_updates into an in-memory buffer (overwrite-per-partition = monotonic
   thanks to the gate) and flush at most ONE `update_partition_lookup_v1` at a
   time (Nagle-style). The old one-job-per-push-batch storm backlogged the lane
   to q ~ 17k-27k under load (hiding real PG work); now **q = 0**, buffer depth
   ~30-70, and PG does the partition_lookup upserts in steady state (~9-11k/s).
2. With the backlog gone, push jumped to **~190k inserts/s** (the backlog had been
   capping it). Stays on the push engine, as intended.
3. **Default push concurrency = static 16** (`batch_policy` max_concurrent 24->16
   + `concurrency_mode_for` defaults PUSH to static). Out-of-box now hits ~190k
   instead of Vegas's 112k.

Full PG-side measurement at the peak (C=16, coalescing image, 1000 partitions):

| metric | value |
|---|---|
| msg inserts/s | **~190k** |
| commits/s | ~1.4-1.5k |
| partition_lookup upserts/s | ~9-11k (q backlog = 0) |
| PG CPU | ~12/32 vCPU (~37%) |
| broker CPU | ~4-7/32 vCPU |
| WAL | ~163 MB/s, fsync fast (sub-ms), buffers_full=0, fpi~2/s |
| disk write | ~240 MB/s |
| top waits | CPU (~400) >> Lock:extend (9@C16) + LWLock:WALWrite (67) + IO:WalSync (29) |

Sweep C=16/24/32/48 (coalescing image): throughput FLAT ~181-190k, and
`Lock:extend` (heap/index relation-extension lock on the single `queen.messages`
table) climbs 9 -> 59 -> 226 -> 514 as C rises, with NO throughput gain.

### Verdict on the ceiling

- **New ceiling ~190k push/s, flat across C=16-48** - above the old ~160k, at
  ~37% PG CPU.
- **The wall is single-table insert contention on `queen.messages`**: the
  relation-extension lock (`Lock:extend`) + index buffer contention
  (`LWLock:BufferContent`) - NOT WAL and NOT CPU.
- **WAL is NOT the bottleneck**: ~163 MB/s with fast/batched fsync (group commit),
  buffers_full=0. The disk did ~240 MB/s here and can do far more; the "~1 GB/s
  before WAL" headroom is real but irrelevant - we hit heap/index extension
  contention long before WAL.
- **Optimal concurrency is low (~16)**; beyond it, extra concurrency only adds
  `Lock:extend` waiting. This is why push now defaults to static 16.

### To exceed ~190k (next lever, schema-side - not the broker)

Spread inserts across more physical relations so the relation-extension lock and
index pages aren't a single hot point:
- **Native PARTITIONING of `queen.messages`** (e.g. HASH by partition_id, or by
  time) -> N heaps + N index trees -> N extension locks. Biggest lever.
- **Trim/rethink the 3 indexes** on `queen.messages` (pkey, dedup unique
  `(partition_id, transaction_id)`, `idx_messages_partition_created`) - each adds
  extension + buffer-content contention on every insert.
This is orthogonal to the push-serialization work and would be a separate change.

## Open follow-up (to close the last ~5-10% at default Vegas)

Cut push RTT so Vegas explores higher on its own, by passing the batch's
distinct partition keys from the engine to `push_messages_v3` as a second param
(eliminates the redundant `jsonb_array_elements` re-parse + the queues/partitions
join in the lock pre-pass). Until then, `QUEEN_CONCURRENCY_MODE=static QUEEN_PUSH_MAX_CONCURRENT=32` reaches baseline.
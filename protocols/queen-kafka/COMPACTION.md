# Compaction through the Kafka facade

Design note, 2026-10-01. Phase 1 is built; phase 2 is a proposal and nothing of
it exists yet.

## Phase 1, built: a compacted topic keeps every record

`cleanup.policy=compact` is accepted by CreateTopics, AlterConfigs and
IncrementalAlterConfigs (`src/topic_config.rs`). What it does to the queue is
one thing: retention is switched OFF on it — `retentionEnabled: false`, sent
explicitly so a merging `/configure` cannot leave an older value in force — and
it stays off whatever `retention.ms` says while `delete` is not in the policy.
`compact,delete` keeps the retention: records older than `retention.ms` go,
nothing younger is removed. DescribeConfigs reports the policy that was set.

Why that is safe: the one thing compaction GUARANTEES a reader is that the last
value of every key is in the log. Keeping every value keeps the last one. The
readers that depend on compacted topics replay them from the start and fold
records into state — Kafka Connect's config, offset and status topics
(`KafkaBasedLog`), Kafka Streams' changelogs on restore — and folding the full
log gives the same state as folding the compacted one, tombstones included,
because the order of every key's records is the log's own.

What it costs: the topic grows without bound, a restore reads the whole history
instead of one record per key, and Kafka's guarantee that a tombstone eventually
disappears (`delete.retention.ms`) does not hold. The other compaction knobs
(`min.compaction.lag.ms`, `max.compaction.lag.ms`, `delete.retention.ms`,
`min.cleanable.dirty.ratio`, `segment.*`) are recorded and reported back as set,
each with a line saying nothing enforces it.

Nothing else removes a record of such a queue: the retention walk has no
cutoff to move its watermark with, and the idle-partition cleanup
(`server/src/rsm/maintenance.rs`, `partition_dead`) only deletes a partition
whose log is EMPTY.

## Phase 2, proposed: compaction as a generic Queen queue policy

Under the two rules this facade lives by: one pipeline (no Kafka-specific
command, planner branch or storage format in the broker) and no change to the
replicated formats (entries, effects, frames, qlog). Every queue would get it,
not only Kafka topics.

### The key is already in every frame

A frame carries a `transactionId`, and every append already stores, per frame,
the 16-byte xxh3_128 of it — the `hashes` of `Effect::Append`, kept per append in
the `txns` rows the dedup and ack-by-hash readers use
(`server/src/rsm/dedup.rs`). So "the newest offset of each key" is computable
from rows the broker already writes, without reading a payload and without a
new index format.

The facade would set `transactionId` from the Kafka record key on a compacted
topic (today a Kafka record carries none and the broker mints a UUIDv7):
the key's bytes, hex-encoded when short, their SHA-256 when longer than the
64 KiB a `transactionId` may be, and a fixed marker for a null key (Kafka
refuses null keys on compacted topics; so should the facade).

**The one precondition that must not be missed:** the same hash is the DEDUP
key. A second write of a key inside `dedupWindowSeconds` is answered
`duplicate` and DROPPED — on a compacted topic that is the newest value lost.
Topics created through the facade already have `dedupWindowSeconds: 0`
(`queen::KAFKA_DEDUP_WINDOW_SECONDS`), and the policy must REQUIRE it: a queue
configured `compaction` with a dedup window is refused at `/configure`.

### What removes a superseded record

Queen's log is append-only below a single watermark: `Effect::Watermark` moves
`log_start` and nothing deletes from the middle. Compaction therefore has to be
expressed as the two effects the broker already has, planned by the planner the
way retention is (proposals judged again against committed state, never a
watermark backwards):

1. **copy forward** — for a prefix `[log_start, X)` of a partition, re-append
   every record that is still the newest of its key, with its own
   `transactionId` and payload, as an ordinary `Append` at the tail;
2. **trim** — then move `log_start` to `X` with an ordinary `Watermark`
   (`txns_start` stays where retention leaves it, so the hash window is intact).

Per-key order is preserved: a record is only copied when no later record of its
key exists, so it lands after every record of OTHER keys and before every later
record of its own (there are none). The state a reader folds is unchanged.

### What differs from Kafka, and what it costs

- **Offsets move.** Kafka compaction keeps a record's offset and leaves gaps;
  here a surviving record gets a NEW offset at the tail. A reader replaying from
  the start (every compacted-topic reader named above) does not care. A
  consumer group committed inside the trimmed prefix finds its offset below the
  log start and is reset by its `auto.offset.reset`; the facade reports
  `OFFSET_OUT_OF_RANGE`, which is Kafka's own answer for a trimmed log.
- **A record is read twice** if a reader is between the copy and the trim, or
  had already read it before it was copied. Folding the newest value of a key
  twice is idempotent, which is the property compacted-topic readers rely on.
- **Tombstones** (null value) are copied forward until `delete.retention.ms`
  has passed since they were appended, then dropped — Kafka's rule, now
  enforced.
- **Write amplification** is the live set per compaction pass; a pass runs only
  when the dirty ratio of the prefix is above `min.cleanable.dirty.ratio`, which
  then becomes a knob that is enforced.

### The pieces, and where they live

| Piece | Where | Format change |
| --- | --- | --- |
| `compaction` queue option (+ `compactionMinDirtyRatio`, `compactionTombstoneSeconds`) | `/configure`, the queue config row | a new config field: the one place a migration is needed (catalogue version, gate D20) |
| per-partition "newest offset of each hash" over a prefix | broker read off `txns` rows | none |
| copy-forward + trim proposals | the retention scanner's proposal queue, judged by the planner | none (existing `Append` + `Watermark`) |
| `transactionId` from the Kafka key on compacted topics | facade produce path | none |
| refuse `compaction` with a dedup window | `/configure` validation | none |

Estimate from the 2026-09-30 scoping: 10-15 agent-days, most of it the
planner-judged copy-forward and its tests (crash between copy and trim, a
leader change in between, a consumer reading across it). The queue config
field is the one item that touches a replicated format, and it is a config
row, not an entry or frame shape — Alice's call whether that is inside rule 2.

## What still needs a decision

- Whether phase 1's unbounded growth is acceptable for the topics that use it
  (Connect's internal topics are small; a Streams changelog of a large table is
  not).
- Whether phase 2 is worth its cost against the alternative of documenting
  `compact` as keep-forever indefinitely.
- The DeleteRecords answer (Kafka Streams purges its repartition topics with it)
  needs the same guarded trim as step 2 above: `Effect::Watermark` exists, but a
  route that submitted it through `Command::Effects` would bypass the planner's
  judgement, and apply treats a watermark that moves back as a fatal
  inconsistency. Until that trim exists DeleteRecords stays unadvertised.

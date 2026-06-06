# 08 — Partitions

Partitions are the heart of Queen's design. This page explains what they are at the implementation level, when to use one vs. many, and how the system behaves at the extremes (10 partitions vs. 100k).

For the user-facing pitch ("Kafka per-key ordering without preallocation"), see `[docs/concepts.html](../docs/concepts.html)`. This page is for developers — what the code actually does.

---

## What a partition is

A row in `queen.partitions`:

```sql
CREATE TABLE queen.partitions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    queue_id UUID REFERENCES queen.queues(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL DEFAULT 'Default',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    last_activity TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(queue_id, name)
);
```

That's it. A partition is a **named ordering scope inside a queue**:

- Two messages in the **same** partition are ordered FIFO by their `created_at` (tiebroken by UUIDv7). Push stamps `created_at = clock_timestamp()` under a per-partition advisory lock, so within a partition `created_at` is monotonic in **commit** order — that's what lets the pop cursor advance safely under concurrent push. See [05 — Database schema §7](05-database-schema.md).
- Two messages in **different** partitions have no ordering relationship and can be processed concurrently by different consumers.

Compare to Kafka:


|                               | Kafka                                | Queen                        |
| ----------------------------- | ------------------------------------ | ---------------------------- |
| Partition is                  | a physical shard (folder, log file)  | a row in a table             |
| Created by                    | operator, in advance                 | the broker, on first push    |
| Cost of one                   | a file, replication, leader election | one row, a few index entries |
| Cost of 10 000                | 10 000 log files                     | 10 000 rows                  |
| Reordering keys to partitions | rehash + rebalance                   | re-target a future push      |


This is why we say "per-entity ordering without preallocating partitions."

---

## How partitions are created

There is no "create partition" API. The first time a `push` arrives with a given `(queue, partition)` pair, `push_messages_v3` does (this snippet is from the equivalent block in v2 — v3 is logically identical, just rewritten to avoid a temp table):

```sql
INSERT INTO queen.partitions (queue_id, name)
SELECT DISTINCT q.id, t.partition_name
FROM tmp_items t
JOIN queen.queues q ON q.name = t.queue_name
ON CONFLICT (queue_id, name) DO NOTHING;
```

So creating a partition costs **one row + a unique-index probe** at first push, and zero at subsequent pushes. There's no broker-side state to update.

If `partition` is omitted in the push, it defaults to `'Default'` (literal string).

---

## How partitions are consumed

`pop_unified_batch_v4` is the workhorse. Two modes:

### 1. Targeted pop

`pop?queue=orders&partition=user-123` — the consumer asks for a specific partition. The procedure:

1. Resolves `(queue, partition)` to a partition UUID.
2. Tries `pg_try_advisory_xact_lock(partition_id)` to claim it. Non-blocking — if another consumer already has it, the call returns immediately and the consumer waits (or the broker long-polls).
3. If claimed, reads messages with `id > last_consumed_id` for this consumer group, up to `batch_size`.
4. Stamps the lease (`worker_id`, `lease_expires_at`) and returns.

Result: per-partition FIFO is enforced because only one consumer can hold the partition at a time, and that consumer reads in `id` order.

### 2. Wildcard pop

`pop?queue=orders` (no partition) — "give me messages from any partition that has them." The procedure:

1. Walks `queen.partition_lookup` for this queue (the materialized "latest message per partition" table).
2. For each candidate, attempts `pg_try_advisory_xact_lock(partition_id)`. Non-blocking — partitions held by another consumer are skipped, no contention.
3. Claims up to `max_partitions` partitions in one call (default `1`, configurable via the request and capped by `MAX_PARTITION_CANDIDATES`).
4. Reads messages from each claimed partition, aggregates them into one response, returns.

The aggregated response carries per-message `partitionId` / `leaseId` / `partition` so the consumer can ack the right rows even when the batch spans multiple partitions.

This is the design that lets Queen scale to **100k active partitions** without the candidate-scan cost growing linearly.

---

## How many partitions to use

This is a **modeling** question, not a tuning one. Pick partition cardinality from your data, not from your hardware.

### Few partitions (10–100): "RabbitMQ-shaped"

When to choose: you want competing consumers and don't care about per-key ordering, just per-shard ordering as a side effect.

- All consumers fight for the same small set of partitions
- The advisory-lock mechanism still keeps one partition per consumer at a time, so you get FIFO inside each shard for free
- Throughput per consumer is high; latency for a specific message depends on which shard it lands in
- Good for: background jobs, fan-out workloads, "any worker can do any task"

### Hundreds to thousands: typical "ordered lanes per business entity"

When to choose: each entity (user, chat, workflow) has a natural identity and you want messages for the same entity to be processed in order, but messages for different entities to be independent.

- Use `partition: <entity-id>` directly
- A slow consumer on partition `chat-42` doesn't stall partition `chat-99`
- Cardinality is bounded by your entity count, which is usually fine

### Tens of thousands to ~100k: high-cardinality

Sustained at this scale in production. Some things to be aware of:

- `partition_lookup` grows with active partitions, not total partitions, so dormant ones are essentially free
- Wildcard pops scan `partition_lookup` (which uses `consumer_watermarks` to skip up-to-date consumers)
- You may want to tune `MAX_PARTITION_CANDIDATES` upward to amortize the scan cost; see `server/ENV_VARIABLES.md`
- Stats roll-up (`013_stats.sql`) does an incremental scan keyed by `(stat_type, stat_key)`; behaviour stays linear in *active* partitions, not total

### Above ~100k: design discussion

Above this you should talk to the team. Above ~200k msg/s sustained, or if you need multi-region replication, Kafka is the right answer — Queen is sized for the single-Postgres tier.

---

## Partition lifecycle

Partitions can be cleaned up after a period of inactivity:

```bash
PARTITION_CLEANUP_DAYS=7    # default
```

`RetentionService::cleanup_inactive_partitions()` (in `server/src/services/retention_service.cpp`) deletes partitions that:

1. Have **zero messages** (no live data)
2. Have **no consumer activity** for `PARTITION_CLEANUP_DAYS` (looks at `partition_consumers.last_consumed_at` and `partition_consumers.created_at`)
3. Were themselves created more than `PARTITION_CLEANUP_DAYS` ago

`ON DELETE CASCADE` from `queen.partitions` cleans up `messages`, `partition_consumers`, `partition_lookup`, and `dead_letter_queue` rows. See [09 — Retention](09-retention.md).

If a partition is recreated (a new push arrives for the same `(queue, partition)`), it gets a **new UUID** and starts fresh — there's no historical link.

---

## Partition naming

Naming is a `VARCHAR(255)`. Anything goes, but in practice:

- Treat the name as opaque from Queen's point of view; semantics are entirely up to the producer.
- Use a stable identifier, not a timestamp — you want pushes for the same entity to land in the same partition.
- For multi-tenancy, prefix: `tenant-1234/user-99`. Slashes are fine.
- Don't put PII in partition names; they show up in dashboards, logs, and traces.

---

## What you'll touch when changing partition logic


| If you're changing…                | Look at…                                                                                                       |
| ---------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| How partitions are created on push | `lib/schema/procedures/001_push.sql`, `partition_upsert` block                                                 |
| How wildcard pop scans candidates  | `lib/schema/procedures/002d_pop_unified_v4.sql`                                                                |
| `partition_lookup` maintenance     | `lib/schema/procedures/016_partition_lookup.sql`, `server/src/services/partition_lookup_reconcile_service.cpp` |
| Inactive-partition cleanup         | `server/src/services/retention_service.cpp::cleanup_inactive_partitions`                                       |
| Per-partition stats                | `lib/schema/procedures/013_stats.sql`                                                                          |
| Watermark for empty scans          | `queen.consumer_watermarks` table + the wildcard-pop path                                                      |


> Before changing pop or push, **read [05 — Database schema § the warning](05-database-schema.md#warning-stored-procedures-are-load-bearing)** and the `cdocs/PUSHPOPLOOKUPSOL.md` memo. The push/pop pair is co-designed.

---

## Common questions

**Q: Can I look at messages across all partitions of a queue?**
Yes — wildcard pop (`partition` omitted) does exactly this. Per-message `partitionId` is in the response.

**Q: Can two messages with the same `transactionId` land in different partitions?**
Yes. The unique constraint is `(partition_id, transaction_id)`, so duplicate detection is partition-scoped. If you want global dedup, you'll need to do it at the application layer.

**Q: What happens if I push to a queue that doesn't exist?**
`push_messages_v3` upserts the queue too (`INSERT … ON CONFLICT (name) DO NOTHING`). So pushing creates queue + partition + message in one round-trip. Use `configure` if you want non-default queue settings (lease time, retry, retention, …).

**Q: Are partitions colocated on disk?**
No — they're rows in `queen.partitions` and messages are rows in `queen.messages` keyed by `partition_id`. Postgres physical layout is at the page level, not the partition level. Use `CREATE TABLE … PARTITION BY` (Postgres declarative partitioning) if you really need that, but at the scale Queen targets it's not needed.
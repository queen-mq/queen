# 05 — Database schema

This page covers the Postgres-side of Queen: tables, indexes, stored procedures, and **how to (carefully) change them**.

---

## Where it all lives

```
lib/schema/
├── schema.sql                   ← base tables + indexes (~500 lines, idempotent)
└── procedures/                  ← 30 stored-procedure files, ~14k lines total
    ├── 001_push.sql
    ├── 002_pop_unified.sql           (legacy, kept for rollback)
    ├── 002b_pop_unified_v2.sql       (legacy)
    ├── 002c_pop_unified_v3.sql       (legacy)
    ├── 002d_pop_unified_v4.sql       ← current pop
    ├── 003_ack.sql
    ├── 004_transaction.sql
    ├── 005_renew_lease.sql
    ├── 006_has_pending.sql
    ├── 007_analytics.sql
    ├── 008_consumer_groups.sql
    ├── 009_status.sql
    ├── 010_messages.sql
    ├── 011_traces.sql
    ├── 012_configure.sql
    ├── 013_stats.sql
    ├── 014_worker_metrics.sql
    ├── 015_postgres_stats.sql
    ├── 016_partition_lookup.sql
    ├── 017_retention_analytics.sql
    ├── 018_prometheus.sql
    ├── 019_streams_schema.sql
    ├── 020_streams_register_query_v1.sql
    ├── 021_streams_cycle_v1.sql
    ├── 022_streams_state_get_v1.sql
    ├── 023_storage_v2.sql               ← q2 schema + segments push/pop/ack
    ├── 024_storage_v2_pop_ext.sql       ← q2 wildcard pop, ack-by-txn
    ├── 025_storage_v2_dlq.sql           ← q2 retry tracking + DLQ
    ├── 026_storage_v2_maintenance.sql   ← q2 retention/eviction/transactions
    └── 027_storage_v2_observability.sql ← dual-engine stats + prometheus
```

---

## How the broker bootstraps the schema

Every time `queen-server` starts, `AsyncQueueManager::initialize_schema()` runs:

1. `CREATE SCHEMA IF NOT EXISTS queen`
2. Apply the full `schema.sql` (idempotent — every `CREATE TABLE` and `CREATE INDEX` uses `IF NOT EXISTS`, every `ALTER TABLE` uses `ADD COLUMN IF NOT EXISTS`)
3. Walk `procedures/*.sql` in lexical order and run each file. Every procedure uses `CREATE OR REPLACE FUNCTION`, so re-running them is free.

This means:

- **Fresh installs work without any `psql` step.** Just point the broker at an empty database and start it.
- **Schema upgrades happen on broker restart.** A new version of the broker brings a new `schema.sql` + new procedures, and they're applied on the next boot.
- **You can modify a procedure** by editing the file and restarting the broker. No migration framework, no version table, no manual `psql -f`.

There is a `server/migrations/` folder reference in the bootstrap code for future versioned migrations, but for now the model is "schema.sql is idempotent + procedures are CREATE OR REPLACE".

---

## Core tables (the ones you'll touch most)

### `queen.queues`

One row per logical queue. Holds the configuration: `lease_time`, `retry_limit`, `dead_letter_queue`, `retention_seconds`, `completed_retention_seconds`, `encryption_enabled`, `max_queue_size`, `max_wait_time_seconds`, etc. — plus the **storage engine selector** `storage` (`'rows'` default, `'segments'` opts the queue into storage v2; see below).

### `queen.partitions`

One row per (queue, partition name). `UNIQUE(queue_id, name)`. **Partitions are created lazily on first push** — there is no preallocation step.

### `queen.messages`

The hot table.

```sql
id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
transaction_id VARCHAR(255) NOT NULL,
trace_id UUID DEFAULT gen_random_uuid(),
partition_id UUID REFERENCES queen.partitions(id) ON DELETE CASCADE,
payload JSONB NOT NULL,
created_at TIMESTAMPTZ DEFAULT NOW(),
is_encrypted BOOLEAN DEFAULT FALSE,
producer_sub TEXT       -- server-stamped from JWT 'sub' when auth enabled
```

The unique constraint `(partition_id, transaction_id)` is **partition-scoped, not global**. That's what makes idempotent push work: a client retries with the same `transactionId`, the second insert hits the constraint, the procedure marks the row as a duplicate.

The `id` is UUIDv7 — time-ordered. This is what gives us "scan for messages above `last_consumed_id`" without a separate offset table. See `queen::generate_uuidv7()` in `lib/queen.hpp`.

> **`created_at` is commit-ordered per partition.** Although the column default is `NOW()` (transaction-*start* time), every push path **overrides** `created_at` with `clock_timestamp()` while holding the per-partition push advisory lock (see warning §7 below). That makes `created_at` monotonic in *commit* order within a partition — the invariant the pop cursor `(created_at, id)` depends on to never skip a not-yet-committed message.

### `queen.partition_consumers`

Per (partition, consumer_group) state. Tracks `last_consumed_id`, `last_consumed_created_at`, lease state, batch state for in-flight pops, etc. This is where the FIFO offset is stored.

### `queen.partition_lookup`

Per-queue index of "latest message in each partition". Maintained **out-of-band**: the push engine coalesces the `partition_updates` from many commits and flushes them via `update_partition_lookup_v1()` Nagle-style (at most one flush in flight; see `cdocs/PUSHPOPLOOKUPSOL.md`). Used by the wildcard pop path to find candidate partitions cheaply.

### `queen.consumer_watermarks`

Per (queue, consumer_group) "I last saw this empty" timestamp. Lets up-to-date consumers skip the full partition scan when no new data has arrived.

### `queen.dead_letter_queue`

Failed messages, with original metadata + error reason.

### `queen.message_traces`, `queen.message_trace_names`

Per-event lifecycle trace (push, lease, ack, retry, dead-letter, …) for the dashboard's trace timeline.

### `queen.system_metrics`, `queen.messages_consumed`, `queen.stats`, `queen.stats_history`

Observability tables. Cleaned by retention. `stats` is the dashboard's main aggregate; `stats_history` is the time series for throughput charts.

### `queen.consumer_groups_metadata`

Per (consumer_group, queue, partition, namespace, task) subscription configuration: subscription mode (one of `'all'`, `'new'`, `'timestamp'`), the timestamp the group started at, etc. See [13 — Consumer groups](13-consumer-groups.md) for how the modes are interpreted.

### `queen.system_state`

Singleton key/value table for "shared between replicas" state (maintenance mode flag, encryption key version, …).

---

## The `q2` schema — storage v2 (segments engine)

Everything above describes the v1 "rows" engine. Queues configured with `queen.queues.storage = 'segments'` are served by a second, self-contained engine in schema **`q2`** (`q2.queues`, `q2.partitions`, `q2.segments`, `q2.dedup`, `q2.consumers`, `q2.dlq`): one row = one segment of K messages, packed and zstd-compressed by the broker, consumed through a `(next_seq, next_off)` cursor with zero secondary indexes on the hot table. It lives in `procedures/023`–`027` and is applied at boot exactly like every other procedures file.

Queue *configuration* stays in `queen.queues` for both engines; `q2` holds only the engine mechanics (plus `q2.queues.dedup_window_seconds`). File `027` redefines the shared observability procedures (`queen.get_prometheus_metrics_v1`, `queen.has_pending_messages`, `queen.list_messages_v1`, …) so dashboards and `/metrics` cover both engines.

The full design — frames, seq allocation, dedup window, DLQ, retention watermark, opt-in flow, limits — is documented in **[16 — Storage v2](16-storage-v2.md)**. The v1 warnings below (§1–§7) apply to the v1 procedures only; the q2 engine has its own co-design constraints documented in the SQL file headers.

---

## ⚠ Warning: stored procedures are load-bearing

> **TL;DR:** Don't edit `lib/schema/procedures/*.sql` unless you've traced through the call paths in `cdocs/PUSHPOPLOOKUPSOL.md` and `cdocs/PUSHVSPOP.md`. Changing one procedure can deadlock or starve another.

The procedures are not "scripts that happen to live in SQL." They are **the implementation** of Queen's core semantics. The C++ broker is mostly a non-blocking transport that calls these functions and returns whatever they return.

What's specifically delicate:

### 1. Push and pop are co-designed

The current pop (`pop_unified_batch_v4`) **does not** rely on a trigger to maintain `partition_lookup`. Instead:

- `push_messages_v3` returns a `partition_updates` summary in its response
- the push engine merges those into a coalescing buffer and calls `update_partition_lookup_v1()` *after* responding (Nagle-style: at most one flush in flight, immediate when idle, batched under load)
- `PartitionLookupReconcileService` runs `reconcile_partition_lookup_v1()` every 5 s as a safety net
- `schema.sql` explicitly drops the old `trg_update_partition_lookup` trigger on every schema apply (the trigger function body is kept around for rollback)

If you reintroduce a trigger that updates `partition_lookup` inside the push transaction, combined PUSH+POP throughput will collapse from ~15 k req/s to ~7 k req/s. This was diagnosed and fixed once already — please read `[cdocs/PUSHPOPLOOKUPSOL.md](../cdocs/PUSHPOPLOOKUPSOL.md)` before touching that path.

### 2. The wildcard pop has no `ORDER BY` and no `LIMIT`

In `pop_unified_batch_v4`, the candidate scan walks `partition_lookup` without ordering and without a limit. Coordination across concurrent consumers is done by `pg_try_advisory_xact_lock(partition_id)` — a non-blocking probe that releases on transaction end. Adding `ORDER BY` or `LIMIT` reintroduces the contention pattern from before; see again `cdocs/PUSHPOPLOOKUPSOL.md`.

### 3. ACK semantics depend on `(partition_id, consumer_group, worker_id)`

`ack_messages_v2` validates the lease using all three. The pop function generates a single `worker_id` per HTTP request and stamps it on every claimed partition (a multi-partition wildcard pop returns messages from up to `max_partitions` partitions, all sharing one `worker_id`). If you change how `worker_id` is generated or matched, leases will mismatch and acks will silently fail.

### 4. The unique index is partition-scoped

`messages_partition_transaction_unique (partition_id, transaction_id)` is **per partition**. If you make it global, idempotent push across partitions breaks (a client cannot reuse a `transactionId` on a different partition). Two redundant earlier indexes used to exist; they were removed (the `schema.sql` has a "legacy index cleanup" section).

### 5. The retention/eviction services share an advisory lock (ID `737001`)

Both `RetentionService` and `EvictionService` acquire `pg_try_advisory_xact_lock(737001)` before running their cleanup. This ensures that a fleet of `queen-server` replicas doesn't run cleanup concurrently and trip on each other's deletes. If you add a new cleanup-style background job, **reuse the same lock ID** or pick a new one and document it.

### 6. Transaction isolation matters

Most procedures rely on the default `READ COMMITTED`. The pop path explicitly uses `pg_try_advisory_xact_lock` to avoid `SELECT ... FOR UPDATE` on hot rows. Don't change isolation level inside a procedure without measuring throughput before and after.

### 7. PUSH is serialized per partition for commit-ordered `created_at`

`push_messages_v3` and the push branch of `execute_transaction_v2` take `queen.lock_push_partition(partition_id)` — a `pg_advisory_xact_lock` in a **dedicated two-int keyspace** (`pg_advisory_xact_lock(1347439944, hashtext(partition_id::text))`), structurally **disjoint** from the single-`bigint` keys used by pop (partition claim), ack (`partition+group`), streams, and the retention lock — for **every** partition they write, **in ascending `partition_id` order** (deadlock-free), held until commit. Messages are then inserted with `created_at = clock_timestamp()` *under* that lock. This is the mechanism behind the commit-ordered-`created_at` invariant the pop cursor depends on. The broker also enforces "≤1 in-flight push per partition" in memory (the push engine's gate + disjoint-partition batching), so on a single instance the DB lock is essentially uncontended — it is the cross-instance backstop. The streams sink inherits this because it pushes via `push_messages_v3`.

**Do not**: reuse the two-int keyspace for anything else; drop the sorted-acquisition order (reintroduces deadlocks); or move the `clock_timestamp()` stamp out from under the lock (reintroduces the cursor-skip bug — a message can commit "behind" an already-advanced pop cursor and be lost). The deterministic repro is `benchmark-queen/2026-06-06-engine-scaling/cursor-repro.sh`.

---

## When you do need to change a procedure

A safe(r) workflow:

1. **Read the relevant `cdocs/` memo** if one exists for that procedure (push, pop, ack are all documented).
2. **Add a new procedure version** rather than editing in place. The current pop (`002d_pop_unified_v4.sql`) is the 4th iteration — `002`, `002b`, `002c` are still in the repo for rollback. The broker `#define`s which version it calls.
3. **Update libqueen** if you renamed the function or changed its signature. Search for the old name in `server/src/` and `lib/`.
4. **Run the full integration test suite** against a real Postgres:
  ```bash
   cd lib
   PG_HOST=... PG_PORT=... make test-suite
  ```
5. **Run the contention benchmark** to make sure you didn't regress combined PUSH+POP throughput:
  ```bash
   cd lib
   PG_HOST=... PG_PORT=... make test-contention
  ```
6. **Watch the metrics endpoint** under load: `push_waits_on_push`, `pop_waits_on_push`, `partition_coverage`. If these get worse, you've reintroduced contention.
7. **Document the change** with a `cdocs/<TOPIC>.md` if it's non-trivial.

---

## Tables you'll rarely touch

- `queen.consumer_watermarks` — purely an optimization. Rebuilds itself naturally.
- `queen.partition_lookup` — see warning above. Maintained by `update_partition_lookup_v1` and reconciled every 5 s.
- `queen.retention_history` — audit log of what retention deleted, kept for `METRICS_RETENTION_DAYS`.

---

## See also

- `[cdocs/PUSHVSPOP.md](../cdocs/PUSHVSPOP.md)` — why combined load used to collapse
- `[cdocs/PUSHPOPLOOKUPSOL.md](../cdocs/PUSHPOPLOOKUPSOL.md)` — the design that fixed it
- `[cdocs/PUSH_IMPROVEMENT.md](../cdocs/PUSH_IMPROVEMENT.md)` — push-specific changes
- `[cdocs/PUSHVSPOP.md](../cdocs/PUSHVSPOP.md)` and `[cdocs/ACK_IMPROVEMENT.md](../cdocs/ACK_IMPROVEMENT.md)` — historical analysis, useful when reviewing PRs
- `[cdocs/WATERMARK_AND_TRANSACTION_FIXES.md](../cdocs/WATERMARK_AND_TRANSACTION_FIXES.md)` — corner cases in the transactional path


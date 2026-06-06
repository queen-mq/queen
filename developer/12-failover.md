# 12 — Failover (file-buffer)

When Postgres becomes unreachable, Queen does **not** drop messages and does **not** return errors to producers. Instead, the broker buffers writes to disk and replays them when Postgres recovers. This is the file-buffer subsystem.

This is one of Queen's strongest correctness guarantees ("zero message loss across 1.6 billion events" in the benchmark suite). It's also subtle. If you're changing anything in `server/src/services/file_buffer.cpp`, read this page first.

---

## What "DB is down" means here

Four things trigger the failover path:

1. **Connection acquisition timeout** — the broker's `AsyncDbPool` tries to get a connection, exceeds `DB_CONNECTION_TIMEOUT` (default 2 s), gives up.
2. **Statement timeout** — a query starts but exceeds `DB_STATEMENT_TIMEOUT` (default 2 s in failover-tuned setups, 30 s by default).
3. **Connection error** — TCP-level error from libpq (refused, reset, RST after replication failover) — surfaces to libqueen as either a `PQconsumeInput` failure or a `uv_poll` `status < 0` error; both go through `_handle_slot_error`.
4. **In-flight deadline exceeded** — a libqueen slot has been waiting for a query result longer than `LIBQUEEN_INFLIGHT_DEADLINE_MS` (default `DB_STATEMENT_TIMEOUT + 5 s`). This is the safety net for silent network drops where neither libpq nor the kernel raises an error on the socket. Each engine's stats timer scans its slots once per second; any slot whose `current_fire.fire_time` is older than the deadline is recycled (jobs requeued, slot disconnected, reconnect thread rebuilds it).

In all four cases, the broker:

- Marks `db_healthy_ = false` (atomic flag, shared across workers via `SharedStateManager`)
- For the *current* request: writes the buffered event to the file system instead of failing
- For *subsequent* requests: skips the DB attempt entirely and goes straight to the file buffer (fast path — no per-request 2 s wait)

A background processor keeps trying to flush the file buffer to Postgres. When a flush succeeds, `db_healthy_` flips back to `true` and normal operation resumes.

---

## The file lifecycle

Buffer files live in `FILE_BUFFER_DIR` (default `/var/lib/queen/buffers/` on Linux, `/tmp/queen/` on macOS):

```
/var/lib/queen/buffers/
├── failover_019a0c11-7fe8.buf.tmp     ← currently being written
├── failover_019a0c11-8021.buf         ← finalized, queued for flush
├── failover_019a0c11-8054.buf
└── failed/
    └── failover_019a0c11-7abc.buf     ← flush failed, will retry in 5 s
```

Filenames are UUIDv7 — file ordering on disk matches event ordering in time.

States:

1. **Active (`.buf.tmp`)** — events are appended here as they arrive. Atomically renamed when:
  - The file reaches `FILE_BUFFER_EVENTS_PER_FILE` events (default 10,000), **or**
  - The file has been idle for 200 ms
2. **Pending (`.buf`)** — finalized, ready to flush. Picked up by the background processor.
3. **Failed (`failed/<file>.buf`)** — last flush attempt failed (DB still down, or partial-success). Retried every **1 second** (in `file_buffer.cpp`: `RETRY_INTERVAL_CYCLES = 1000 / flush_interval_ms_`).
4. **Deleted** — successful flush. Removed atomically after the corresponding `INSERT` commits.

The `.tmp` → `.buf` rename is done with `rename(2)`, which is atomic on POSIX. There is never a moment when a partial file is in the pending state.

> **Durability detail.** Files are opened with `O_WRONLY | O_APPEND | O_CREAT` — *no* `O_SYNC` and *no* explicit `fdatasync()` per event. Each event write is one `writev` call against `O_APPEND`, which is atomic at the kernel level (the bytes are not interleaved with another writer's bytes), but the kernel may delay flushing dirty pages to disk. The integrity boundary is therefore the `rename(2)` that promotes `.tmp` to `.buf`: on broker startup, any partially-written `.tmp` is cleaned up by `cleanup_incomplete_tmp_files()` so a crash never produces a corrupt `.buf`. If you need stricter durability (kernel-fsync per event), it's not built in — open an issue if your workload requires it.

---

## Why this is safe

The failover path preserves three invariants:

### 1. No message is acknowledged to a producer until it's written

A `push` returns to the client only after the event is `writev`-appended to the active `.buf.tmp` file. The kernel page cache may not have been flushed to disk yet (no per-write `fdatasync`), so a host-level crash within the kernel writeback window can lose the most recent few hundred milliseconds of buffered events. A broker-process crash without a host crash is fully recovered: any partial `.tmp` is cleaned up on startup, and `.buf` files are picked up where the previous run left off.

If your deployment cannot tolerate that small writeback window, run on a filesystem with `commit=0` or similar synchronous-by-default mount options.

### 2. No message is duplicated on replay

Every event carries the `transactionId` the **client** generated (UUIDv7, typed by the SDK). On replay, `push_messages_v3` hits the unique constraint `(partition_id, transaction_id)` and skips the duplicate. The procedure returns the disposition (`new` vs `duplicate`) so the file processor knows whether to count it.

This is why **clients always generate `transactionId` themselves**, never the server. If you change a client to delegate ID generation, you break replay-deduplication.

### 3. Order is preserved within a partition

Events for the same partition land in the same buffer file in append order. UUIDv7 IDs are monotonic per process. On replay we don't reorder. So even if Postgres flushes a 50,000-event buffer into many small batched inserts, per-partition order is preserved by the UUID order.

---

## Configuration

Per-broker settings (full list in `server/ENV_VARIABLES.md`):


| Variable                      | Default                                                 | Effect                                            |
| ----------------------------- | ------------------------------------------------------- | ------------------------------------------------- |
| `FILE_BUFFER_DIR`             | `/var/lib/queen/buffers` (Linux) / `/tmp/queen` (macOS) | Where files live                                  |
| `FILE_BUFFER_FLUSH_MS`        | `100`                                                   | How often the processor scans for `.buf` files    |
| `FILE_BUFFER_MAX_BATCH`       | `100`                                                   | Events per `INSERT` during flush                  |
| `FILE_BUFFER_EVENTS_PER_FILE` | `10000`                                                 | Finalize file at N events                         |
| `DB_STATEMENT_TIMEOUT`        | `30000`                                                 | Lower this (e.g. `2000`) to detect DB down faster |
| `DB_CONNECTION_TIMEOUT`       | `2000`                                                  | Connection-attempt timeout                        |


For a high-throughput failover-tuned config:

```bash
export FILE_BUFFER_FLUSH_MS=50            # poll more aggressively
export FILE_BUFFER_MAX_BATCH=1000         # bigger flush batches
export FILE_BUFFER_EVENTS_PER_FILE=50000  # bigger files
export DB_STATEMENT_TIMEOUT=2000          # detect DB down in 2 s
export DB_POOL_SIZE=300                   # more conns for recovery throughput
```

### Silent network drops (Cloud SQL maintenance, LB reroutes, hypervisor pause)

The defaults above already detect a *clean* PG shutdown (RST / FIN) within ~1 s. The harder case is a *silent* disruption — packets being black-holed by a managed proxy or load-balancer while the underlying instance is being moved — because no application-level or kernel-level error is raised on the broker's existing TCP connections for many minutes by default.

Three settings together close that gap:


| Variable                        | Default                       | Effect                                                                                                                                            |
| ------------------------------- | ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DB_TCP_USER_TIMEOUT_MS`        | `30000`                       | Linux `TCP_USER_TIMEOUT`. Bounds how long unacknowledged outgoing data can sit before the kernel errors the FD. Without it: ~14 min default.      |
| `DB_KEEPALIVES_IDLE`            | `60`                          | Seconds of idle before the first keepalive probe. Without tuning, libpq inherits the OS default (typically 2 h on Linux).                         |
| `LIBQUEEN_INFLIGHT_DEADLINE_MS` | `DB_STATEMENT_TIMEOUT + 5000` | Per-slot safety net: a libqueen slot in-flight longer than this is treated as a dead connection regardless of whether the kernel raised an error. |


Together these guarantee that, even when the network silently swallows packets, the broker observes the dead connection within ~30–60 s and falls over to the file buffer for new requests, and that the libqueen slot is recycled even if no socket event ever fires.

The `_uv_socket_event_cb` callback also routes `uv_poll` `status < 0` errors through `_handle_slot_error` — earlier versions only logged them, which left the slot permanently in-flight and was the proximate cause of "broker doesn't recover after Postgres returns" reports.

---

## Disk sizing

Approximate size per buffered event: ~170 bytes (UUIDs + small payload). For a queue running at 1 M msg/h with sustained DB downtime:

```
1,000,000 msg/h × 170 B = ~170 MB/h
```

So a 1-hour outage of a 1 M-msg/h flow needs ~170 MB free in `FILE_BUFFER_DIR`. Plan accordingly. **Monitor with:**

```bash
du -sh /var/lib/queen/buffers
```

The status endpoint exposes the same:

```bash
curl http://localhost:6632/api/v1/status/buffers
```

```json
{
  "qos0": { "pending": 0, "failed": 0 },
  "failover": { "pending": 50000, "failed": 0 },
  "dbHealthy": false
}
```

---

## What it looks like in the logs

DB goes down:

```
[Worker 0] PUSH: 1000 items to [orders/Default] | Pool: 5/5 conn (0 in use)
... 2 seconds timeout ...
[Worker 0] DB connection failed, using file buffer for failover
[Worker 0] DB known to be down, using file buffer immediately
```

DB comes back:

```
[Background] Failover: Processing 10000 events from failover_019a0c11.buf
[Background] PostgreSQL recovered! Database is healthy again
[Background] Failover: Completed 10000 events in 850ms (11765 events/sec) - file removed
```

Duplicates on replay (e.g. partial flush before crash):

```
[Background] Failover: Processing 10000 events...
[ERROR] Batch push failed: duplicate key constraint
[INFO] Recovery: Duplicate keys detected, retrying individually...
[INFO] Recovery complete: 1000 new, 9000 duplicates, 0 deleted queues
```

---

## Modifying file_buffer.cpp safely

Hot path knobs are all in `server/src/services/file_buffer.cpp`. Things to be careful about:

1. **Don't remove `transactionId` from the buffered record.** The unique constraint is the only thing keeping replay idempotent.
2. **Don't change the rename strategy.** `rename(2)` is the atomic checkpoint between "being written" and "ready to flush." Anything else (e.g. flag files, in-place state) introduces races.
3. **Don't add a per-event `fdatasync` without measuring.** The current design intentionally trades a small kernel-writeback window for ~10× higher buffered-write throughput. If you need stricter durability, gate it behind an env var so operators choose.
4. **Watch the failed/ folder behavior.** A file moved to `failed/` is retried every 5 s. If you change to a longer interval, also expose the new value as an env var; some operators rely on this.
5. **Test the recovery path.** Hard-kill Postgres mid-traffic, leave the broker running, then start Postgres. The buffered events should flush in < 1 s with no duplicates.

A useful manual test:

```bash
# Start broker pushing through a producer benchmark
cd clients/client-js
node benchmark/producer.js &

# Stop Postgres mid-flow
docker stop qpg

# Wait 30 s
sleep 30

# Start Postgres again
docker start qpg

# Watch logs:
#  - You should see file-buffer activity during the outage
#  - Then a "PostgreSQL recovered!" + "Failover: Completed N events" sequence
#  - The producer's totals should match what was acked (zero loss)
```

---

## Common pitfalls

- **Disk full → buffered writes start failing.** `FILE_BUFFER_DIR` should be on its own filesystem with monitoring, or at least sized for your worst-case outage.
- **Two brokers on the same `FILE_BUFFER_DIR`.** Don't. Each broker should have its own (the UUIDv7 prefixes are per-process and don't deduplicate across hosts).
- `**/tmp` on macOS gets cleaned at boot.** Fine for development, **not** safe for production. Always set `FILE_BUFFER_DIR` to a persistent location.
- **Slow `fdatasync` on network filesystems.** `FILE_BUFFER_DIR` on NFS/CIFS will tank push latency under load. Use a local SSD.

---

## Related code


| File                                           | Role                                       |
| ---------------------------------------------- | ------------------------------------------ |
| `server/src/services/file_buffer.cpp`          | The implementation                         |
| `server/src/services/shared_state_manager.cpp` | `db_healthy_` shared flag                  |
| `server/src/managers/async_queue_manager.cpp`  | Decides "DB or buffer" per request         |
| `server/src/database/async_database.cpp`       | The pool that surfaces the timeout signals |
| `lib/schema/procedures/001_push.sql`           | Replay path (handles duplicates)           |



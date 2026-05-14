# P4 — Code-Level Refactors

## 1. Summary

Four medium-sized refactors that improve maintainability, observability,
and operational safety without changing the broker's external contract.
They are bundled here because each is too small to warrant a dedicated
plan but large enough that committing them ad-hoc inside an unrelated
PR would be inappropriate.

The four sub-plans are independent; any subset can be landed.

| ID    | Title                                              | Effort | Priority |
| ----- | -------------------------------------------------- | -----: | :------: |
| P4.1  | Consolidate environment-variable helpers           |  ½ day |  Low     |
| P4.2  | Reduce HTTP route boilerplate                      |  3 day |  Med     |
| P4.3  | Versioned schema migrations                        |  1 wk  |  High    |
| P4.4  | `partition_lookup` refresh-lag observability       |  1 day |  Med     |

## 2. P4.1 — Consolidate environment-variable helpers

### 2.1 Motivation

There are **three** parallel sets of "read int/string/bool from env":

| Location                                         | Helpers                                              |
| ------------------------------------------------ | ---------------------------------------------------- |
| `server/include/queen/config.hpp:11-33`          | `get_env_bool / int / double / string` (anon ns)     |
| `lib/queen/drain_orchestrator.hpp:39-45` + impls | `detail::env_int`, `detail::env_double` (anon)       |
| Inline `std::getenv` calls in routes / services  | ad-hoc, no defaults documentation                    |

The drift produces real bugs: an env var renamed in `config.hpp` is
not picked up by the libqueen-side reader, and vice versa. The
`server/ENV_VARIABLES.md` reference, while excellent, has to be
maintained by hand because there is no single function table to
generate from.

### 2.2 Plan

1. Add `lib/queen/env_config.hpp`:
   ```cpp
   namespace queen::env {
   bool        as_bool  (const char* name, bool default_value);
   int         as_int   (const char* name, int default_value);
   long long   as_int64 (const char* name, long long default_value);
   double      as_double(const char* name, double default_value);
   std::string as_string(const char* name, const std::string& default_value);
   bool        is_set   (const char* name);
   }
   ```
2. Replace `config.hpp::get_env_*` and `drain_orchestrator.hpp::env_int`
   with thin forwarders to `queen::env::*`.
3. Add a debug-only `queen::env::dump()` that walks every read variable
   (instrument the helpers to record which names were queried plus their
   default and effective value) and emits a single line at boot.
4. Optionally generate `server/ENV_VARIABLES.md` from the dump output —
   even if generation is left manual, the dump becomes the
   authoritative test fixture for the doc.

### 2.3 Validation

- Same broker binary boots with identical effective configuration on
  a representative `.env` (verify with the new dump).
- `grep -RE 'std::getenv|get_env_(bool|int|string|double)' server/ lib/`
  returns zero hits outside `env_config.hpp` itself.

---

## 3. P4.2 — Reduce HTTP route boilerplate

### 3.1 Motivation

Each file in `server/src/routes/` repeats the same pattern:

1. `REQUIRE_AUTH_*` macro guard.
2. `read_json_body` async parse.
3. Manual field-by-field validation
   (e.g. `push.cpp:46-89` validates seven fields per item).
4. Build the SP request shape.
5. Submit through `AsyncQueueManager` / `Queen::submit`.
6. Register a deferred `ResponseRegistry` entry, write 201/200.

The result is files like `prometheus.cpp` (730 LOC), `push.cpp`
(377 LOC), `migration.cpp` (607 LOC), where each file's *interesting*
logic is sub-50 LOC and the rest is the same plumbing typed by hand.

### 3.2 Plan

1. Promote `server/src/routes/route_helpers.cpp` (already exists,
   286 LOC) into a fully-fledged middleware-style chain:
   ```cpp
   route("/api/v1/push", POST, ctx)
     .require_auth(AccessLevel::READ_WRITE)
     .with_claims()
     .json_body()
     .validate<PushRequest>()
     .submit_to_sidecar(JobType::PUSH, &handle_push)
     .register();
   ```
2. Define `struct PushRequest` (and similar for ack, pop, transaction)
   in `server/include/queen/routes/contracts.hpp` with a single
   `from_json` / `to_json` per type. Validation moves into the type.
3. Each `*.cpp` route file shrinks to:
   - one type definition,
   - one handler function,
   - one `route(...).register();`-style registration call.

This is the same pattern Express middleware uses; expressing it in
C++17 is straightforward with a fluent builder.

### 3.3 Validation

- Each `server/src/routes/*.cpp` file ≤ 150 LOC after the refactor
  (excluding contracts header).
- Server passes the existing `lib/test_suite.cpp` and the
  P3 `streams.yml` workflow with no behavioural change.
- Per-route unit tests become possible (drive the handler with a
  fake `RouteContext` and a JSON request, assert the SP request
  shape).

### 3.4 Risk

The fluent builder must not regress the highly-tuned hot path of
`push.cpp` / `pop.cpp` (see `cdocs/PUSH_IMPROVEMENT.md`). Mitigation:
keep the builder's compile-time output equivalent to the current
hand-written code (use `inline` / `constexpr`, look at the disasm).
The `bp-100` benchmark must show no regression.

---

## 4. P4.3 — Versioned schema migrations

### 4.1 Motivation

`lib/queen.hpp::initialize_schema` re-applies the **entire schema
plus every procedure** alphabetically on every broker boot. The
`CREATE OR REPLACE FUNCTION` and `CREATE TABLE IF NOT EXISTS` parts
are idempotent and cheap, but the schema also issues:

```sql
DROP INDEX IF EXISTS queen.idx_messages_transaction_id;     -- schema.sql:312
DROP INDEX IF EXISTS queen.idx_messages_txn_partition;      -- schema.sql:313
DROP TRIGGER IF EXISTS trg_update_partition_lookup ON queen.messages;  -- schema.sql:471
DROP FUNCTION IF EXISTS queen.pop_unified_batch_v2_noorder(JSONB);     -- schema.sql:479
ALTER TABLE queen.messages ADD COLUMN IF NOT EXISTS producer_sub TEXT; -- schema.sql:71
ALTER TABLE queen.partition_lookup SET (…fillfactor=50…);              -- schema.sql:437
ALTER TABLE queen.partition_consumers SET (…fillfactor=50…);           -- schema.sql:444
ALTER TABLE queen.stats               SET (…fillfactor=50…);           -- schema.sql:451
```

Every one of these takes a non-trivial lock (`AccessExclusive` for
`DROP INDEX`, `ShareUpdateExclusive` for `ALTER TABLE … SET (...)`)
on hot, billion-row tables, **on every restart of every broker
instance**. On a quiet system the locks resolve in milliseconds; on
a system where `partition_consumers` has 50 k actively-leased rows
it can cause pop tail-latency spikes lasting tens of seconds.

### 4.2 Plan

1. Add a tracking table:
   ```sql
   CREATE TABLE IF NOT EXISTS queen.schema_migrations (
       version       INTEGER PRIMARY KEY,
       name          TEXT NOT NULL,
       applied_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
       applied_by    TEXT,
       checksum      TEXT NOT NULL
   );
   ```
2. Move every irreversible schema change into a numbered migration
   file under `lib/schema/migrations/`:
   ```
   lib/schema/migrations/
   ├── 0001_drop_legacy_message_indexes.sql
   ├── 0002_drop_partition_lookup_trigger.sql
   ├── 0003_drop_pop_unified_v2_noorder.sql
   ├── 0004_messages_add_producer_sub.sql
   ├── 0005_fillfactor_partition_lookup.sql
   ├── 0006_fillfactor_partition_consumers.sql
   ├── 0007_fillfactor_stats.sql
   └── 0008_idx_partition_consumers_worker_id_active.sql
   ```
3. Refactor `initialize_schema`:
   - Apply the **base** `schema.sql` (only `CREATE … IF NOT EXISTS`
     for tables and indexes, no DROPs, no ALTERs).
   - Read `queen.schema_migrations`, apply only files with
     `version > current_version`.
   - Each migration runs in its own transaction, records its
     checksum, advances the version pointer.
   - Apply all `procedures/*.sql` (idempotent `CREATE OR REPLACE`,
     unchanged from today).
4. `schema.sql` after the move is much smaller — only the
   pristine base, no bookkeeping.
5. Document the model in `developer/05-database-schema.md` (the
   chapter title becomes "Schema, procedures, and migrations") and
   add a one-liner contributor checklist:
   > Any DDL that changes existing data, drops objects, or alters
   > storage parameters must be a numbered file under
   > `lib/schema/migrations/`. Never edit `schema.sql` to do that.

### 4.3 Compatibility

- Existing deployments will boot the new broker, find no
  `queen.schema_migrations` table, and the bootstrap path will run
  every migration in order. Each migration is **already a no-op on
  a database that previously ran the inline DDL** (because of `IF
  EXISTS` / `IF NOT EXISTS`). After the first boot the version
  pointer is at the latest migration; subsequent boots are pure
  procedure reloads.
- For fresh deployments the order is identical to today's because
  the migration files are numbered to match the historical inline
  order.

### 4.4 Validation

- Boot the new broker against a database that ran the previous
  release: verify `\d` on every table is identical to before, and
  `\df queen.*` is identical to before.
- Boot the new broker against an empty database: verify the
  resulting schema is byte-identical to a fresh install of the
  previous release.
- `cpp-test.yml` from P3 covers both shapes via a matrix.

### 4.5 Risk

- **Risk:** a future migration is shipped that's not idempotent and
  fails midway. Mitigation: each migration runs inside `BEGIN; …
  COMMIT;`, and the version pointer advances only on commit. A
  failed migration leaves the broker refusing to start with a clear
  error. The broker should log the failing migration's body and
  exit 1 — not silently retry.
- **Rollback:** retain `schema.sql` with its current inline DDL
  behind a `QUEEN_SCHEMA_LEGACY=1` env switch for the first release
  cycle, then remove.

---

## 5. P4.4 — `partition_lookup` refresh-lag observability

### 5.1 Motivation

`cdocs/PUSHPOPLOOKUPSOL.md` documents the trade-off: maintaining
`partition_lookup` synchronously in the push transaction caused
push-vs-push row-lock contention; the fix was to refresh it
**asynchronously** from libqueen after each commit, plus a
periodic reconcile every 5 s as the safety net.

The trade-off is correct, but the broker exposes **no metric** that
tells operators how stale the lookup actually is. A stuck reconcile
service or a lagging libqueen submit-after-commit path can produce
a window during which wildcard POP discovery silently misses
partitions.

### 5.2 Plan

1. In `queen.partition_lookup`, the existing `updated_at` column
   already records the last refresh wall time per row.
2. Add a stored procedure
   `queen.partition_lookup_refresh_lag_v1()` that returns:
   ```
   max_lag_seconds  INTEGER  -- max(NOW() - updated_at) over rows with pending data
   p99_lag_seconds  INTEGER
   stale_partitions INTEGER  -- count where lag > 60 s
   ```
3. Call it from `server/src/routes/prometheus.cpp` and emit:
   ```
   queen_partition_lookup_refresh_lag_seconds{stat="max"}    <max>
   queen_partition_lookup_refresh_lag_seconds{stat="p99"}    <p99>
   queen_partition_lookup_stale_partitions                    <stale>
   ```
4. Add a single chart to the `app/` dashboard's "Cluster" view
   showing the same series.

### 5.3 Validation

- Disable the
  `PartitionLookupReconcileService` (e.g. set
  `PARTITION_LOOKUP_RECONCILE_INTERVAL_MS=0`) and run a steady
  push workload: the new gauge climbs unbounded, proving it
  reflects reality.
- Re-enable: the gauge stabilises within `RECONCILE_INTERVAL +
  reconcile_duration` seconds.

### 5.4 Risk

The new metric does **one extra DB call per Prometheus scrape**.
That's already the contract for `/metrics/prometheus` (see
`server/API.md:67-73` — "Performs **one DB call** to
`queen.get_prometheus_metrics_v1()`"). Wrap the new query into the
same procedure rather than adding a second round-trip.

---

## 6. Combined effort

If all four sub-plans are delivered:

- ~2 working weeks for one engineer.
- Each is independently revertable.
- P4.3 is the only one with persistent (database-side) state and
  therefore the highest review priority.

## 7. Sequencing recommendation

P4.1 → P4.4 → P4.2 → P4.3.

Rationale:

- P4.1 unblocks consistent reading of any new env vars added by
  later plans.
- P4.4 ships immediate operational value for a known soft spot.
- P4.2 is a pure refactor that benefits from being landed before
  the larger schema-migration change so review focus is on one
  concern at a time.
- P4.3 is the highest-impact, highest-risk change and benefits
  from landing last with the rest of the cleanup behind it.

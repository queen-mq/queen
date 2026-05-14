# P1 — Stored-Procedure Version Cleanup

## 1. Summary

`lib/schema/procedures/` currently ships **four versions of the pop
procedure side by side** (`002`, `002b`, `002c`, `002d`). All four are
parsed and stored in PostgreSQL on every broker boot via libqueen's
schema initializer (`lib/queen.hpp:initialize_schema`). Only the latest
(`pop_unified_batch_v4` from `002d_pop_unified_v4.sql`) is dispatched
on the hot path; v3 is intentionally retained as an emergency-revert
target (per the comment in `lib/schema/schema.sql:481-489`); v1 and v2
are dead code.

This plan introduces a `_archive/` subdirectory that the schema loader
ignores, moves the dead procedures into it, and adds explicit
`DROP FUNCTION IF EXISTS` statements so existing deployments shed the
orphaned PL/pgSQL on first restart after the change.

It also documents the **current** procedure surface in a single table
in `developer/05-database-schema.md` so contributors don't need to
read libqueen to know which procedure name is live.

## 2. Motivation

- `lib/schema/procedures/002_pop_unified.sql` is **61 KB**. Together
  with `002b` (26 KB) and `002c` (22 KB) the loader parses ~109 KB of
  dead PL/pgSQL on every boot and stores it in `pg_proc`. It's not a
  hot-path cost, but it inflates `pg_dump` outputs, schema diffs, and
  every `\df queen.*` listing.
- New contributors cannot tell which procedure is live without reading
  `lib/queen/pending_job.hpp` and grepping for the literal SQL string.
  This regularly produces "I edited the wrong file" wasted PRs.
- The same shape applies (less severely) to `push_messages_v3` —
  `001_push.sql` is 18 KB and contains v3 only — but the historical
  v1/v2 drops were inlined into `schema.sql` (see `schema.sql:471` and
  `schema.sql:479`) rather than archived. The convention should be
  consistent.

## 3. Out of scope

- No PL/pgSQL changes to the live procedures (`push_messages_v3`,
  `pop_unified_batch_v4`, `ack_messages_v2`, `renew_lease_v2`,
  `execute_transaction_v2`). Those are governed by their own
  improvement memos in this directory.
- No change to which procedure libqueen dispatches. The whole point is
  reversibility: if `pop_unified_batch_v4` regresses, we should still
  be able to flip libqueen back to v3.
- No move to a real numbered-migration system. That is **P4.3**, a
  separate plan; this one is a pure file-organization change that fits
  the existing "load every `*.sql` alphabetically" model.

## 4. Current state

### 4.1 Procedure files

```
lib/schema/procedures/
├── 001_push.sql                       18 KB   push_messages_v3 (live)
├── 002_pop_unified.sql                61 KB   pop_unified_batch    (DEAD — v1)
├── 002b_pop_unified_v2.sql            26 KB   pop_unified_batch_v2 (DEAD — v2)
├── 002c_pop_unified_v3.sql            22 KB   pop_unified_batch_v3 (revert target)
├── 002d_pop_unified_v4.sql            37 KB   pop_unified_batch_v4 (live)
├── 003_ack.sql                        10 KB   ack_messages_v2 (live)
├── 004_transaction.sql                13 KB   execute_transaction_v2 (live)
├── 005_renew_lease.sql                 3 KB   renew_lease_v2 (live)
├── 006_has_pending.sql                 2 KB
├── 007_analytics.sql                   2 KB
├── 008_consumer_groups.sql            43 KB
├── 009_status.sql                     22 KB
├── 010_messages.sql                   16 KB
├── 011_traces.sql                      8 KB
├── 012_configure.sql                   6 KB
├── 013_stats.sql                      87 KB
├── 014_worker_metrics.sql             57 KB
├── 015_postgres_stats.sql              8 KB
├── 016_partition_lookup.sql            9 KB
├── 017_retention_analytics.sql         5 KB
├── 018_prometheus.sql                  5 KB
├── 019_streams_schema.sql              4 KB
├── 020_streams_register_query_v1.sql   7 KB
├── 021_streams_cycle_v1.sql           19 KB
└── 022_streams_state_get_v1.sql        5 KB
```

### 4.2 Schema loader

`lib/queen.hpp::detail::get_sql_files`:

```cpp
for (const auto& entry : std::filesystem::directory_iterator(dir)) {
    if (entry.path().extension() == ".sql") {
        files.push_back(entry.path().string());
    }
}
std::sort(files.begin(), files.end());
return files;
```

It iterates the top level of `procedures/` only. Adding a subdirectory
named `_archive/` is automatically excluded from loading.

### 4.3 Existing inline drops in `schema.sql`

```sql
-- schema.sql:479
DROP FUNCTION IF EXISTS queen.pop_unified_batch_v2_noorder(JSONB);

-- schema.sql:312-313
DROP INDEX IF EXISTS queen.idx_messages_transaction_id;
DROP INDEX IF EXISTS queen.idx_messages_txn_partition;

-- schema.sql:471
DROP TRIGGER IF EXISTS trg_update_partition_lookup ON queen.messages;
```

The pattern is established: idempotent drops live in `schema.sql`,
loaded once on each boot.

## 5. Target state

```
lib/schema/procedures/
├── 001_push.sql                       (live: push_messages_v3)
├── 002_pop_unified_v3.sql             (revert target — renamed from 002c)
├── 003_pop_unified_v4.sql             (live — renumbered from 002d)
├── 004_ack.sql                        (renumbered)
├── …                                  (sequential numbering)
└── _archive/
    ├── 000_pop_unified_v1.sql         (was 002_pop_unified.sql)
    └── 001_pop_unified_v2.sql         (was 002b_pop_unified_v2.sql)
```

The renumbering is optional but recommended: alphabetical order over
the live set becomes obvious, and there are no longer holes (`002b`,
`002c`, `002d`) that imply a hidden dependency.

## 6. Plan

### 6.1 Step 1 — archive dead procedure files

```bash
mkdir -p lib/schema/procedures/_archive
git mv lib/schema/procedures/002_pop_unified.sql \
       lib/schema/procedures/_archive/000_pop_unified_v1.sql
git mv lib/schema/procedures/002b_pop_unified_v2.sql \
       lib/schema/procedures/_archive/001_pop_unified_v2.sql
```

Add a `lib/schema/procedures/_archive/README.md`:

> Procedures retired from active loading. Kept under version control so
> their bodies remain reviewable for incident post-mortems and as a
> reference for the design memos under `cdocs/`. Not loaded by libqueen.

### 6.2 Step 2 — drop the orphaned functions on existing deployments

Append to `lib/schema/schema.sql` (near the existing
`DROP FUNCTION IF EXISTS queen.pop_unified_batch_v2_noorder`):

```sql
-- 002_pop_unified.sql / 002b_pop_unified_v2.sql were archived in P1 cleanup.
-- These DROPs are idempotent: fresh databases never created the function;
-- existing databases shed the dead PL/pgSQL on next boot.
DROP FUNCTION IF EXISTS queen.pop_unified_batch(JSONB);
DROP FUNCTION IF EXISTS queen.pop_unified_batch_v2(JSONB);
```

If the original v1/v2 functions had different signatures (e.g. extra
positional args), enumerate every overload here. Verify with
`\df queen.pop_unified_batch*` on a representative production instance
before merging.

### 6.3 Step 3 — renumber the surviving files (optional but recommended)

```bash
git mv lib/schema/procedures/002c_pop_unified_v3.sql \
       lib/schema/procedures/002_pop_unified_v3.sql
git mv lib/schema/procedures/002d_pop_unified_v4.sql \
       lib/schema/procedures/003_pop_unified_v4.sql
# bump 003_ack.sql → 004_ack.sql, etc., to keep numbering dense
```

Important: re-running the loader with renumbered files is safe because
every procedure uses `CREATE OR REPLACE FUNCTION`. The renumber is a
file-name change, not a body change.

If renumbering is judged too noisy, leave the numbers as they are; the
archive move alone delivers most of the value.

### 6.4 Step 4 — document the live procedure surface

Add a section to `developer/05-database-schema.md`:

> ### Currently dispatched stored procedures
>
> | HTTP / SDK call         | C++ entry point           | SQL function                       | File                                    |
> | ----------------------- | ------------------------- | ---------------------------------- | --------------------------------------- |
> | `POST /api/v1/push`     | `push.cpp`                | `queen.push_messages_v3`           | `001_push.sql`                          |
> | `GET/POST /api/v1/pop`  | `pop.cpp`                 | `queen.pop_unified_batch_v4`       | `003_pop_unified_v4.sql`                |
> | `POST /api/v1/ack`      | `ack.cpp`                 | `queen.ack_messages_v2`            | `004_ack.sql`                           |
> | `POST /api/v1/transaction` | `transactions.cpp`     | `queen.execute_transaction_v2`     | `005_transaction.sql`                   |
> | `POST /api/v1/lease/:id/extend` | `leases.cpp`      | `queen.renew_lease_v2`             | `006_renew_lease.sql`                   |
> | `POST /streams/v1/queries` | `streams/register_query.cpp` | `queen.streams_register_query_v1` | `020_streams_register_query_v1.sql` |
> | `POST /streams/v1/cycle`   | `streams/cycle.cpp`    | `queen.streams_cycle_v1`           | `021_streams_cycle_v1.sql`              |
> | `POST /streams/v1/state/get` | `streams/state_get.cpp` | `queen.streams_state_get_v1`     | `022_streams_state_get_v1.sql`          |
>
> The single source of truth for the literal SQL strings is
> `lib/queen/pending_job.hpp`. If a name in this table disagrees with
> that file, the file wins.

## 7. Validation

1. `psql -f lib/schema/schema.sql -f lib/schema/procedures/*.sql` on
   a fresh PostgreSQL applies cleanly with **zero warnings** about
   redefining functions.
2. Existing CI workflow `cpp-server-build.yml` still passes.
3. `\df queen.pop_unified_batch*` on a database that previously had
   v1/v2 returns only v3 and v4 after the broker boots once.
4. `lib/test_suite.cpp` still passes (the suite calls
   `queen.pop_unified_batch_v4` indirectly via the broker; the dropped
   functions were not referenced).
5. Schema apply duration on a database with **0 partitions** must not
   regress by more than ±50 ms.

## 8. Risks & rollback

- **Risk:** an in-flight branch on a private fork still references
  `pop_unified_batch_v2` directly via `psql`. Detection is trivial —
  the branch will fail with `function does not exist` after rebase —
  and the fix is to update the branch to `pop_unified_batch_v4`.
- **Risk:** v1/v2 had a side-effect (e.g. ad-hoc analytics view) that
  someone in operations relies on. Mitigation: keep
  `lib/schema/procedures/_archive/` under version control so an
  operator can re-load the file by hand:
  `psql -f lib/schema/procedures/_archive/000_pop_unified_v1.sql`.
- **Rollback:** revert the PR. The archive directory and the
  `DROP FUNCTION` lines are independent commits in the same PR; either
  can be reverted on its own.

## 9. Effort estimate

Half a day for the move + renumber + documentation. Most of the time
goes into verifying which overloads exist on the canonical production
database (so the `DROP FUNCTION` list is exhaustive).

## 10. Follow-up

Once this lands, P4.3 (versioned migrations) becomes the natural next
step: every future schema change is a numbered file in a `migrations/`
directory and `_archive/` is no longer needed because the migration
log itself is the audit trail.

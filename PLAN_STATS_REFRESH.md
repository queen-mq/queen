# `log_refresh_all_stats_v1` — cause and remediation plan

Target: `queen.log_refresh_all_stats_v1` (`server/sql/procedures/011_log_stats.sql:99-318`), the #1 line in
prod Query Insights on `postgres-queen-02-prod` (63 queues, ~50-60k partitions, 3 broker replicas).
Companion to `PLAN_MAINT_V2.md` / `PLAN_MAINT_SCALING.md` (commit `137444c6`, **not an ancestor of HEAD** —
nothing from either plan has shipped; verified by grep for `maintenance_tasks` / `maint_due_at` /
`log_maint_step_v2` / `QUEEN_STATS_INCREMENTAL`, zero hits in the tree).

---

## 0. Measured shape, and what the panel units are

**Prod shape (measured 2026-08-18, query E):**

| relation | rows | heap | total (heap+TOAST+idx) |
|---|---|---|---|
| `log_segments` | 1,356,150 | **1,040 MB** | 11 GB |
| `log_txns` | 1,353,110 | 131 MB | 244 MB |
| `log_consumers` | **151,445** | 49 MB | 70 MB |
| `log_partitions` | 53,966 | 12 MB | 32 MB |
| `log_dlq` | 14,550 | 18 MB | 24 MB |
| `stats` | 81 | 80 kB | 256 kB |

**The panel is a RATE — ms of DB time per second — not a mean per call.** Settled physically, not by
inference: `log_queue_stats_all_v1` contains `SELECT s.partition_id, count(*) FROM queen.log_segments s
GROUP BY s.partition_id` (`011:745-749`), i.e. **1,356,150 index entries**; there is no plan that does that in
6.0 ms (it needs ~226M entries/s). Likewise `seg_bytes` seq-scans a 1,040 MB heap, and 1,040 MB in 97.3 ms is
10.7 GB/s with a hash-join probe and a hash aggregate per row — out of reach single-threaded.

Therefore, with `STATS_INTERVAL_MS=30000` (`helm_v1/broker/prod.yaml:314`) and `replicas: 3` (`prod.yaml:186`)
giving 0.1 calls/s:

> **~97 ms of DB time per second ≈ 9.7% of one vCPU, continuously, and ~970 ms per call.**
> On a 2-vCPU instance that is ~5% of the machine and **59% of the top-10 query time on the chart**.

A bottom-up estimate from the table sizes agrees independently: 1,040 MB seq scan (`seg_bytes`) + ~54k random
PK descents and heap fetches (the tail LATERAL) + 151k consumer rows scanned **twice** + a 54k re-scan and
sort ≈ 800 ms-1.5 s. Two independent derivations landing on ~1 s is the number to plan against.

So the case for the work is all three of:

1. it is the largest single query cost on the instance today;
2. every expensive term is O(partitions) or O(segments), on the axes prod is actively growing;
3. it is a single-threaded **~1 second write transaction** holding an XID three times per 30 s, against two
   tables deliberately tuned at `autovacuum_vacuum_scale_factor = 0` to reclaim every naptime
   (`001_log_schema.sql:64-68`, `:156-160`) — and it has **no `statement_timeout` of any kind**, so a
   black-holed pod holds advisory lock `737_002` plus an open XID for the TCP keepalive window (~2h11m).

Margin note: ~970 ms against a 30 s per-replica interval is not close to the overrun cliff yet. It was
~2-4 ms/partition-thousand at 2k partitions; at 54k it is ~1 s; the cliff is at 30 s.

---

## 1. Cause

Three independent multipliers stack. Only one of them is the SQL.

### 1.1 The cadence is 3× the configured one — the HA question

`stats.rs:116-119` takes `pg_try_advisory_xact_lock(737_002)` **inside** the transaction it then commits
(`stats.rs:97` BEGIN, `:124` refresh, `:101` COMMIT). That is *mutual exclusion*, not *cadence*: the lock dies
at COMMIT, nothing durable records that the period was served, and each replica keeps its own phase via
`interval - start.elapsed()` (`stats.rs:79`). A loser returns `Outcome::Skipped` (`stats.rs:120-122`) and
retries a full interval later, so all three replicas refresh, staggered.

The chart already knows this and compensates by hand for retention — `prod.yaml:279-287`, "**x3 FOR THE
REPLICA COUNT**", `retentionInterval: 900000` to buy a 300 s cluster cadence — and the identical note for
stats at `prod.yaml:309-313` ("measured every ~3.4s cluster-wide instead of the intended 10s") only ever got
`STATS_INTERVAL_MS` raised 10s → 30s. The ×3 is still there: **effective cluster cadence is 10 s.**

Two consequences beyond the 3×:

* `stats.rs:79` uses `checked_sub(...).unwrap_or(ZERO)`. Once a cycle exceeds the interval the loop runs
  back-to-back with no sleep — the exact failure retention hit at 26,809 partitions (`prod.yaml:269-271`).
  Stats is on the same trajectory, on the same axis.
* Two of three replicas take a `Lane::Maint` slot **before** `pool.get()` (`stats.rs:94-97`) and open+commit
  an empty transaction just to learn they lost, out of a lane whose steady cap is 2 (`admission.rs:509`).

### 1.2 The SQL recomputes global state from global scans

One statement (`011:126-288`) with six CTEs. Work per cycle at the measured shape (P=53,966, C=151,445,
S=1,356,150 / 1,040 MB, D=14,550):

| Term | Location | Shape | Work per cycle |
|---|---|---|---|
| `seg_bytes` | `011:188-194` | unqualified full **heap** scan of `log_segments` (`octet_length(blob)` is not in the PK; zero secondary indexes by design, `001:82-86`) + hash join to 54k partitions + hash agg to 63 groups | **1,040 MB / 1.36M rows — the largest term** |
| `part_agg` tail LATERAL | `011:153-158` | `ON true`, **unconditional**: one backward PK descent + one random heap fetch **per partition** to produce 63 values of `newest_message_at` | **53,966 descents + 53,966 random heap fetches** |
| `worst` + `lease_agg` | `011:126-134`, `011:163-170` | two unqualified `GROUP BY partition_id` passes over **the same table** in one statement; `lease_expires_at` has no index so its selective predicate buys nothing | **2 × 151,445 rows / 49 MB** (71% of those rows are one queue's 11 groups — see §4.8) |
| `per_queue` | `011:195-207` | **re-drives from `log_partitions` again** although `part_agg` already carries `queue_id` (`011:137`); `COUNT(DISTINCT lp.id)` on the PK is exactly `COUNT(*)` and its `aggdistinct` blocks HashAggregate, forcing a Sort | 53,966 rows + a 53,966-row sort |
| `log_oldest_pending_at_v1` | `011:47-74`, called `011:144-149` | f×P calls (CASE-guarded on pending>0 — **not** P), each 1-2 PK probes plus plpgsql SPI overhead | f × 53,966 |
| `dlq_agg` | `011:174-179` | full scan of `log_dlq`, a table **retention never purges** (`006:421-424`), only ever emptied one message at a time by hand (`016_messages.sql:38-42`) | 14,550 rows / 18 MB, grows forever |
| duplicate `COUNT(*)` | `011:304` | recomputes what `018_stats.sql:162` already computed in the same transaction | 2 × 53,966 |
| rollups | `018:30-215` | 3× `INSERT ... ON CONFLICT` at O(queues) | 81 rows — negligible |

The function header (`011:17-28`) states "**NO `queen.log_segments` scans**" as its design contract. The body
contradicts it twice, and its own comment concedes it at `011:183-185`.

**Nothing here can go parallel.** Two independent reasons: it is one `INSERT ... SELECT` (`CMD_INSERT`), and
its target list calls `log_oldest_pending_at_v1`, which is `LANGUAGE plpgsql STABLE` with no `PARALLEL`
clause (`011:50-51`) and is therefore PARALLEL UNSAFE by default. (The only `PARALLEL SAFE` declarations in
the whole tree are five KV helpers at `024_kv.sql`.)

### 1.3 It is unbounded

No `statement_timeout`, no `idle_in_transaction_session_timeout`, no `tokio::time::timeout`. Verified along
every path: `config.rs:997-1004` builds the deadpool config field-by-field with no `options` and no DSN, so a
`-c statement_timeout=` cannot be smuggled in; `QUEEN_STMT_TIMEOUT_MS` exists (`config.rs:1060`) but is wired
into fusion/sweeper/kv only, never into `stats.rs` or `retention.rs`; `webdoc/.../deploy/postgres.mdx:91` says
so in prose. The #1 query in Query Insights has no ceiling on either side.

### 1.4 What it is all for

Of everything this function writes, exactly **one** field has a consequence outside a human-refreshed
dashboard tile: `retained_bytes` → `get_queues_v2` (`018:253`) → `proxy/src/registry.rs:381-385, 465-475,
507-515` → hard `403 storage_quota_exceeded` at `gateway.rs:1016-1023`, enforced regardless of
`limits.enforcing()`. Everything else is a 30 s dashboard tile (`app/src/App.vue:86`) — or dead:

* `newest_message_at` and `oldest_pending_at` **as stored columns have no reader anywhere**. The wire's
  `newestMessage` / `oldestMessage` are computed live per request (`011:411-418`, `011:557-563`). The two
  `log_segments` probes that dominate the cost exist to feed one lag integer.
* `ingested_per_second` / `processed_per_second` and the `prev_*` trio: computed, rolled up, and **never
  emitted** — `get_system_overview_v3` computes them at `019:656-657` and then uses `queue_lag_metrics`
  instead at `019:686-687`, with the reason at `019:630-634`.
* `queen.stats_history`: no writer, no reader, two indexes, re-applied every boot (`018:217-218`, `:469`).
* `partition_id` / `consumer_group` / `last_scanned_*` on `queen.stats`: written NULL or constant, never read;
  `idx_stats_partition_id` indexes a permanently-NULL column.

---

## 2. The HA answer: a due-time claim row, not a lock

> *"three nodes that do not know how to execute them only one per cycle"*

An advisory lock cannot express cadence because it holds no state. The mechanism that can is one durable row
per task, arbitrated by the **DB clock** (so pod clock skew is irrelevant) with a lease (so a dead holder
cannot stall the task for the TCP keepalive window) and a fencing token (so a revived holder cannot publish).

```sql
-- new numbered file, e.g. server/sql/procedures/028_maintenance_leases.sql
CREATE TABLE IF NOT EXISTS queen.maintenance_leases (
    task             TEXT PRIMARY KEY,       -- 'stats_refresh' | 'retention_phase1' | ...
    period_ms        BIGINT      NOT NULL,
    next_due_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_until      TIMESTAMPTZ,
    holder           TEXT,
    fence            BIGINT      NOT NULL DEFAULT 0,
    last_start_at    TIMESTAMPTZ,
    last_end_at      TIMESTAMPTZ,
    last_duration_ms INTEGER,
    runs             BIGINT      NOT NULL DEFAULT 0,
    enabled          BOOLEAN     NOT NULL DEFAULT TRUE
);
```

**Claim** — one autocommitted statement, the first lock the transaction takes (above `queen.queues` in the
declared order at `005_log_ack.sql:906-912`, which is where a scheduler belongs):

```sql
UPDATE queen.maintenance_leases t
   SET lease_until   = now() + make_interval(secs => $lease_s),
       holder        = $holder,
       fence         = t.fence + 1,
       last_start_at = now()
 WHERE t.task = $task
   AND t.enabled
   AND t.next_due_at <= now()
   AND (t.lease_until IS NULL OR t.lease_until <= now())
RETURNING t.fence;
```

**Release** — advances the schedule exactly once, fixed-rate with a catch-up clamp:

```sql
UPDATE queen.maintenance_leases t
   SET next_due_at      = GREATEST(t.next_due_at + make_interval(secs => t.period_ms/1000.0), now()),
       lease_until      = NULL,
       holder           = NULL,
       last_end_at      = now(),
       last_duration_ms = $elapsed_ms,
       runs             = t.runs + 1
 WHERE t.task = $task AND t.fence = $fence;
```

> **Trap, and it is the one an implementation gets wrong.** Do **not** also advance `next_due_at` at claim
> time. `next_due_at = now() + P` at claim plus `next_due_at + P` at release delivers `claim + 2P` — the
> configured cadence, silently halved. (This is exactly the bug the adversarial pass found in one of the
> candidate designs, and it is the form `PLAN_MAINT_V2.md §3.3` sketches.) Equally, do **not** ship the claim
> row without the lease: stamping `last_run_at = now()` at claim time with no lease permits two concurrent
> refreshes, which is strictly worse than today's advisory lock.

Properties: exactly one runner per period cluster-wide; failover bounded by the lease, not by TCP keepalive;
crash mid-cycle costs at most one lease (the schedule was never advanced); restart-safe; identical in the
embedded binary; and it answers "when did each task last run, how long did it take, who ran it" with one
`SELECT` instead of correlating three pods' logs. Loser cost is one not-due probe (~0.02 ms, no row lock) —
and the loser must sleep a full period, not busy-retry.

Keep advisory lock `737_002` as belt during the transition; retire it once the claim row is default.

**Do not generalise this to the hotlist reseed.** It maintains per-replica in-memory ring state; each replica
genuinely needs its own reseed (`PLAN_MAINT_V2.md §11.10`). Same for `syscollect` and the KV sweeper's shard
ownership.

---

## 3. The plan

### Tier 0 — hours, no build, no schema (do these now)

| # | Change | Effect |
|---|---|---|
| **T0.1** | `STATS_INTERVAL_MS: 30000 → 90000` in `prod.yaml:314`, with the same "×3 FOR THE REPLICA COUNT" comment retention already carries | 8,640 → 2,880 calls/day. **841 → 280 s/day, −67%, zero code.** Freshness becomes the 30 s that `schema.sql:206-209` already publishes as the contract |
| **T0.2** | `SET LOCAL statement_timeout` + `idle_in_transaction_session_timeout` inside the stats transaction (3 lines in `stats.rs`, `SET LOCAL` so it dies with the transaction and cannot leak into the pool) | Caps the ~2h11m black-hole window: XID pinned, vacuum horizon frozen on `log_partitions`/`log_consumers`, stats stalled cluster-wide. Ship **with** T1.6 or a timeout turns a slow refresh into a silent no refresh |
| **T0.3** | Floor the sleep: `max(sleep, interval/10)` in `stats.rs:79` and `retention.rs:237` | Removes the back-to-back spin when a cycle overruns. Two characters of policy against the failure mode that produced the 60s→300s→900s escalation |

T0.1 is a stopgap that leaves `replicas:` load-bearing in a helm value with no template tie
(`statefulset.yaml:146-151`). It must be retired **in the same release** as T2.1, or the fleet is one
`replicas:` edit away from a silent 3× regression.

### Tier 1 — one day, SQL-local, no schema change, no writer change

**T1.0 — move `seg_bytes` out of the refresh into its own slow-lane SP.** This is the largest single term
(1,040 MB heap scan per cycle) and the cheapest to remove: `retained_bytes` feeds only the proxy storage
gauge, which `schema.sql:200-207` **already documents as hysteretic**, and the proxy's own budget is the 60 s
reconcile plus a 10% release band (`registry.rs:30`, `:41-44`). Split it into
`queen.log_refresh_retained_bytes_v1()` writing only `queen.stats.retained_bytes`, run it on its own claim row
(§2) every 5-10 minutes, and delete the CTE from the refresh. No schema change, no hot-path edit, no counter
drift, no bundle-version-guard prerequisite — it captures most of what T2.2 buys, at a fraction of the risk.
Keep the whole-table form here; the counter (T2.2) becomes an optimisation of a query that now runs 1/30th as
often, not a prerequisite. **Widen the quota release band in the proxy in the same release** if the slow-lane
period exceeds ~2 minutes.

**T1.1 — delete the tail LATERAL (`011:153-158`) and stop writing `newest_message_at`.** It is the single
largest term (~30%: 60k index descents + 60k random heap fetches per cycle) and the column it feeds has no
reader in `server/`, `app/`, `proxy/`, `clients/` or `webdoc/`. Delete it; do **not** substitute
`last_write_at` — that is a different quantity (1 s quantized, non-NULL on provisioned-never-pushed partitions
incl. the timer-DLQ path at `025:1373`, and non-NULL forever on fully-drained partitions because
`006:236-247` never touches it). Substituting keeps a cost and adds three divergences for a column nobody
reads.

**T1.2 — drive `per_queue` from `part_agg` instead of re-scanning `log_partitions`** (`011:195-207`;
`part_agg` already projects `queue_id` at `011:137`, both LEFT JOINs are on unique keys, so the row set is
identical), and **replace `COUNT(DISTINCT lp.id)` with `COUNT(*)`** (`lp.id` is the PK, zero fan-out) — this
also re-enables HashAggregate and deletes a forced ~60k-row Sort. ~9%.

**T1.3 — one pass over `log_consumers` instead of two.** Merge `worst` and `lease_agg` into a single CTE;
`lease_agg`'s `WHERE lease_expires_at IS NOT NULL AND > v_now` collapses to
`FILTER (WHERE c.lease_expires_at > v_now)` because `NULL > v_now` is NULL. ~8%.

**T1.4 — delete the duplicate `SELECT COUNT(*) FROM queen.log_partitions` (`011:304`).** Note: it cannot be
sourced from `aggregate_system_stats_v2()`'s return value — that returns only `{'systemUpdated': n}`
(`018:213`); the count is an InitPlan at `018:162`. Either add the key to 018 or keep one of the two counts.
Also: the comment at `011:299-303` claims the re-count "pins the value at THIS snapshot" — false at READ
COMMITTED, where every statement in a plpgsql body takes its own snapshot. ~2.5%.

**T1.5 — the two bugfixes `PLAN_MAINT_V2.md` calls unconditional, still absent.** (a) `ORDER BY q.id` on the
driving SELECT at `011:240` before the `ON CONFLICT` at `011:247` — unordered upsert lock order is
plan-dependent and this repo has measured that deadlock class (`003:298-303`). (b) A `>= 3 s` elapsed guard on
the rate arms (`011:269-283`), which today gate only on `> 0` so the 3-replica overlap already produces
garbage rate windows. Both must land **before** any claim-row rollout: the mixed-fleet window is the moment
concurrency is guaranteed. Note the two writers already disagree — `011:275/283` write literal 0 in the ELSE
arm, `018:196/204` keep the previous value; fix both or the `'system'` rate and the sum of `'queue'` rates
diverge permanently after any collapsed-denominator event.

**T1.6 — make staleness visible before making it staler.** `statsAge` on the **default-tenant** overview
branch reads `worker_metrics_summary.last_updated_at` (`019:604-608`), not `queen.stats.last_computed_at` —
so every single-tenant/self-hosted deployment has *no* staleness indicator for `queen.stats`. Point it at the
real value and add a WARN when a cycle overruns its period. This is the honesty precondition for T0.2 and for
anything in Tier 3.

**Tier 0 + Tier 1 together: the three heaviest terms (1,040 MB scan, 54k random probes, one of the two
consumer passes) leave the cycle, and the cycle runs a third as often. Expect ~97 ms/s → single-digit ms/s —
i.e. the query leaves the top of the chart entirely — with no schema change, no hot-path change, and no new
subsystem.** Capture query **D** first: after this lands, `pg_stat_statements` tracks only the top-level call
and the improvement cannot be attributed retrospectively.

### Tier 2 — days, schema + Rust

**T2.1 — `queen.maintenance_leases` and `stats.rs` onto it** (§2). Retires T0.1's helm workaround, decouples
`replicas:` from cadence, bounds failover to the lease. Ship the retention phases onto their own rows in the
same release, and **reset `retentionInterval` to 300000 at the same time** (`prod.yaml:287`) or the day real
cadence lands is the day retention latency silently triples.

**T2.2 — `retained_bytes` as a counter maintained by the two writers that already hold the partition row
lock.** With T1.0 shipped this is no longer urgent: it optimises a 1,040 MB scan that now runs every 5-10
minutes instead of every 10 seconds. Take it only if the slow-lane scan itself becomes a problem (i.e. if
`log_segments` keeps growing at the current rate) or if the quota needs to be tighter than the slow-lane
period. It deletes the last whole-table scan (`seg_bytes`). The removal-path enumeration is complete and
clean: exactly one INSERT (`003:206-207`), one ranged DELETE (`006:236-239`, under the row lock taken at
`006:172-174`; max-wait eviction delegates to it verbatim at `006:401-404`), and the only two paths that
remove segments otherwise both destroy the counter's parent row (`013_analytics.sql:54`, `006:579` — and the
latter additionally requires zero segments at `006:463`). No TRUNCATE, no `UPDATE ... blob`, S3 archive not
implemented.

> **Two traps.** (1) `GREATEST(retained_bytes - v_bytes, 0)` **destroys a NULL sentinel** — PostgreSQL's
> `GREATEST` ignores NULL arguments, so `GREATEST(NULL - x, 0)` is `0`, not NULL. If the rollout uses
> "NULL = unmeasured, fall back to the scan", one retention sweep silently converts unmeasured partitions to
> measured-zero and the queue then publishes a `retained_bytes` that is too **low** — on the one field that
> gates a customer's Produce with a hard 403. Use `CASE WHEN retained_bytes IS NULL THEN NULL ELSE
> GREATEST(...) END`. (2) This edits `003_log_push.sql` (hottest path in the product) and
> `006_log_maintenance.sql` **in place**, and `schema.rs:105-167` re-applies every procedure file on every
> boot with no version guard — one old-image boot anywhere in the fleet freezes the counter while the reader
> keeps trusting it. **The bundle-version guard (`PLAN_MAINT_V2.md §5.2`) is a prerequisite, not a
> follow-up.** New numbered files are safe under last-boot-wins; edited function bodies are not.

**Do not** attempt the symmetric `dlq_count` counter. `log_dlq_head_v1` inserts the DLQ row while holding only
the consumer row lock and explicitly never a partition lock (`005:122-125`, `005:812-831`); updating a
`log_partitions` counter there is a C→P lock-order inversion against the order declared at `005:906-912`.

### Tier 3 — gated on measurement, not scheduled now

Per-partition sidecar + dirty-set incremental refresh (`PLAN_MAINT_V2.md` decision #12's `QUEEN_STATS_INCREMENTAL`
stage). It is the right shape for 500k partitions and the wrong use of 8-10 engineer-days against ~1.9% of DB
time today. Two hard constraints if it is ever built:

* `pending_messages` and `processing_messages` must **never** be split across cadences. They are subtracted at
  `018:256`, `019:575-580`, `019:980`, and the writer clamps `processing ≤ pending` in one snapshot
  (`011:226`). Split lanes report "no backlog" on a backlogged queue.
* A dirty set fed from push/retention/pop/ack misses two admin paths that move cursors across every partition
  of a queue in one SQL call with no partition id at the call site: `log_seek_consumer_group_v1`
  (`010:265-277`, call site `db.rs:1877`) and `log_delete_consumer_group_v1` (`010:140-147`, call site
  `db.rs:1768`). Both have the `queue_id` in hand — arm by queue.

---

## 4. Adjacent findings, cheaper than most of the above

Found while decomposing the target; each is independent of it.

1. **`log_hotlist_reseed_v1` (line 2 of the panel, 22.5 ms) is still the full walk on a periodic floor.**
   `hotlist_reseed_full_ms` defaults to 300000 (`config.rs:1156`) and prod's `extraEnv` sets only
   `STATS_INTERVAL_MS` and `POP_WAIT_MAX_INTERVAL_MS` — every hotlist knob runs at the code default. Each live
   ring does a Θ(partitions-in-queue) walk every 300 s **on every replica**; `prod.yaml:89-95` measured 9,571
   partitions walked to return zero rows at 49 ms. Raising `QUEEN_HOTLIST_RESEED_FULL_MS` to 1800000 is one
   env var for a saving plausibly larger than the entire Tier 1. **The trade is real and is yours to make**:
   worst-case ring repair for a non-deterministic miss goes ~360 s → ~1860 s. The deterministic misses
   (group delete, seek) are already covered by the durable `queen.hotlist_repairs` markers
   (`001:186-204`, `010:93-103`, `reconcile.rs:26-56`).
2. **Both retention step SPs take the partition row lock *before* discovering there is nothing to do**, and
   without `SKIP LOCKED`: `006:171-173` and `006:307-311`. That is the same row lock the push allocator takes
   (`003:134-136`, `003:325-330`), so a no-op purge probe blocks behind a pusher — and
   `log_partition_cleanup_step_v1` in the same file does it correctly at `006:543-548`, pinned by a test at
   `retention.rs:698-731`. This is the likeliest explanation for a 12.3 ms mean on a call that deletes
   nothing, and every no-op still emits an `XLOG_HEAP_LOCK` and dirties a page on a fillfactor-70 table.
   Fix: `EXISTS` probe first, lock only if there is work, `SKIP LOCKED`.
3. **`log_queue_stats_all_v1` (`011:745-749`) has a second unqualified full `log_segments` aggregate**, on a
   *request* path, with no tenant/queue/time bound (the `WHERE q.tenant_id` at `011:758` is applied after two
   LEFT JOINs and cannot push into the CTE). The proxy reconciler calls it **once per tenant every 60 s**
   (`registry.rs:298-309`, `:346`, `RECONCILE_INTERVAL = 60s`), plus every open Queues tab at 30 s. The proxy
   only reads `partitions` and `retainedBytes` (`registry.rs:363, 381-385`) — gate the segment count behind a
   query parameter and the reconciler stops paying for it.
4. **`queen.log_dlq` has no retention rule and one DLQ row pins its partition forever** (`006:464` vetoes
   cleanup on any DLQ row). It is the only unbounded term left in the refresh after Tier 1+2, and it is also a
   monotone floor under the partition count itself.
5. **`queen.stats` leaks `'namespace'` and `'task'` rows forever.** There is no `DELETE FROM queen.stats`
   anywhere; the only removal is the `queue_id` cascade (`schema.sql:187`), and the two rollups never set
   `queue_id` (`018:37-44`, `018:90-97`). `get_system_overview_v3` counts those rows directly
   (`019:535-536`), so the namespace/task tiles monotonically over-count and never recover from a queue
   deletion — and every refresh re-stamps the dead rows with a fresh `last_computed_at`. Query **H** in §7
   lists them.
6. **Boot-time version floor is self-contradictory.** `schema.rs:97` enforces `MIN_SERVER_VERSION_NUM =
   140_000` (PG14), but `schema.sql:98-102` uses `NULLS NOT DISTINCT`, which needs PG15 — and
   `helm_v1/sql/02-provision-pxdb-prod.sql:61-62` says 15 is the real floor. A PG14 server passes the check
   and then dies mid-apply, which is precisely what the check exists to prevent (`schema.rs:94-96`).
7. **`log_txns` is not being reclaimed.** 1,353,110 rows against 1,356,150 segments — the sidecar is a 1:1
   shadow of the segment table. If the purge were keeping up at its window
   (`GREATEST(dedup_window, completed_retention, 900s)`, `retention.rs:120-123`) it would hold only the last
   window's worth, i.e. far fewer rows than `log_segments`. Either the dedup window is very large or phase 2
   is not reclaiming. One query settles it:
   `SELECT min(created_at), max(created_at), count(*) FROM queen.log_txns;` against the same for
   `queen.log_segments`. If the two `min(created_at)` are equal, phase 2 is doing nothing but paying for
   itself — and it is line 4 on the chart.
8. **One queue owns 71% of all consumer rows.** E2: 9,733 partitions carry **11 consumer groups each** =
   107,063 of the 151,445 rows — and 9,733 is exactly `smartchat.router.outgoing`'s partition count. The
   remaining 44,103 partitions have one group. Both `worst` and `lease_agg` scan all of it, twice, every
   cycle, so T1.3 (one pass) is worth more here than a generic estimate would suggest.
9. **The partition population is not one bad queue — it is one key space fanned across a pipeline.**
   The top queue is 20.4%, but the six `smartchat.*` queues are **89.7%** of all partitions
   (10,987 / 10,987 / 9,733 / 8,311 / 4,841 / 3,544), and `agent.translate` and `router.history` have
   *identical* counts. That is the same entity key materialised once per pipeline stage: every stage a
   message traverses creates its own partition per key. So partition reduction is a product decision about
   the partition key (or about collapsing stages), not a config knob — and it is worth taking to the
   smartchat owners with these numbers, because it is the only lever that improves five of the eight lines on
   the chart at once.
10. **`partitionCleanupDays: 1` is probably fighting the workload.** `idle_7d` is 0 on every smartchat queue
   while `idle_1d` is ~38% (4,172 of 10,987 on `agent.translate`, 2,963 of 8,311 on `router.incoming`). The
   key space is revisited within the week, so a 1-day cleanup deletes partitions that come back — and a
   partition delete CASCADEs its `log_consumers` cursors, which `prod.yaml:296-305` flags as a replay risk for
   any group that never registered in `consumer_groups_metadata`. Churn plus a correctness edge, for a
   population that is not actually garbage. Consider 7 days, or gate cleanup on the revisit interval.
11. **14,550 DLQ rows are pinning partitions.** `006:464` vetoes cleanup on *any* `log_dlq` row, and nothing
   purges them. Worth knowing how many distinct partitions those rows cover:
   `SELECT count(DISTINCT partition_id) FROM queen.log_dlq;` — that number is a hard floor under P until a
   DLQ retention rule exists.
12. **Partition count is the lever nobody costed.** ~68% of the refresh is P-proportional and `S` is itself
   proportional to `P` at fixed message rate (one `log_segments` row per push call *per partition*,
   `003:206-207`; a bundle across K partitions writes K segments). 10× fewer partitions ⇒ ~97 ms → ~20 ms on
   this SP, *and* proportional cuts to the hotlist walk, `log_queue_stats_all_v1`, phase 2's no-op step calls,
   and phase 4's scan — five of the eight lines on the screenshot at once. Partitions are auto-created from a
   client-supplied string with **no broker-side cap** (`003:113-115`, `003:315-320`;
   `max_partitions_per_queue` exists only in the proxy's own schema, `proxy/migrations/001_init.sql:99`).
   Query **E** will show whether 50-60k partitions across 63 queues is really one queue with a
   high-cardinality partition key — in which case the cheapest fix in this entire document is a conversation
   about that key.

---

## 5. Landing order

1. **Now (T0):** `STATS_INTERVAL_MS=90000`; `SET LOCAL` timeouts; sleep floor. Plus T1.5's two SQL bugfixes —
   they are one-line each and they close live defects. Capture query **D** (per-CTE `EXPLAIN ANALYZE`) before
   anything else lands, or the improvement is unattributable afterwards.
2. **This week (T1), in this order:** T1.0 (`seg_bytes` to a slow lane — the single largest term), T1.1
   (delete the LATERAL), then T1.2/T1.3/T1.4, then T1.6. Requires `cargo build` before testing — the SQL is
   `include_str!`-embedded.
3. **Same window, independent:** the hotlist knob decision (§4.1) and the retention `SKIP LOCKED` fix (§4.2),
   plus the `log_txns` reclamation check (§4.7) — line 4 on the chart may be paying for nothing.
4. **Next (T2):** claim-row cadence + helm retirement of the ×3 rule + bundle-version guard. The
   `retained_bytes` counter is now optional (see T2.2).
5. **Product conversation, not code:** the partition-key fan-out (§4.9) and `partitionCleanupDays` (§4.10).
6. **Only if query G justifies it (T3):** the sidecar and dirty set.

---

## 6. What not to do

* Do not add an index to `queen.log_segments` to make `seg_bytes` cheaper. The zero-secondary-index property
  is load-bearing for the push path (`001:82-86`); the answer is to stop scanning, not to index.
* Do not put a counter on `log_partitions` from the ack/DLQ path (§3, T2.2 note).
* Do not add jitter to the stats cadence to spread the load. It does not damage the rates (they divide by
  measured wall time and are cadence-invariant), but the collapsed-denominator bug is a *jitter* bug — and
  after the claim row the schedule is regular by construction, which strictly improves it.
* Do not raise `STATS_INTERVAL_MS` and forget it. It is a stopgap with `replicas:` load-bearing in a helm
  value; it retires with T2.1 or it becomes the next `retentionInterval`.

---

## 7. Measurement — read-only, run before changing anything

`pg_stat_statements` tracks only the top-level statement (`db.rs:1292-1298` issues one call), so after Tier 1
lands there is **no way to attribute the improvement from Query Insights** — the same single line just gets
smaller. Capture C/D/G **before** the change.

`pg_stat_statements` and `pg_stat_user_tables` are per-instance and not replicated: **A, B, F must run on the
primary**; C, D, E, G are safe on a read replica (run twice, take the second — cold cache).

* **A — settle §0.** `SELECT left(regexp_replace(query,'\s+',' ','g'),80), calls, mean_exec_time, total_exec_time/1000 AS total_s, 100*total_exec_time/sum(total_exec_time) OVER () AS pct, rows FROM pg_stat_statements WHERE dbid = (SELECT oid FROM pg_database WHERE datname=current_database()) ORDER BY total_exec_time DESC LIMIT 25;`
  plus `SELECT stats_reset, now()-stats_reset FROM pg_stat_statements_info;`.
  If `log_pop_list_v1` sits above the refresh in `total_s`, §0 is confirmed and Tier 2/3 need re-sizing.
* **B — the ×3, directly.** `calls / epoch(now()-stats_reset)` for the refresh. `≈0.1/s` proves the
  multiplier at `STATS_INTERVAL_MS=30000`; `≈0.033/s` means it is not happening and T0.1's headline evaporates.
* **C — the plan.** Run the six CTEs as a bare `SELECT` under `EXPLAIN (ANALYZE, BUFFERS, VERBOSE)` with
  `SET LOCAL max_parallel_workers_per_gather = 0` (reproducing the INSERT's world). Read: `loops=` on the
  `log_oldest_pending_at_v1` node (that is `f`, the one input nobody can bound from the repo); actual time +
  `Buffers` on the `tail` LATERAL vs `seg_bytes` (that decides T1.1 vs T2.2 priority); and whether `per_queue`
  shows `Sort Method: external merge Disk:` (if so T1.2 is worth more than estimated).
* **D — per-term attribution.** Each CTE alone under `EXPLAIN (ANALYZE, BUFFERS)`. The only attribution that
  survives `track = top`.
* **E — shape.** Partitions per queue (`GROUP BY q.name ORDER BY count DESC`), groups per partition
  (`log_consumers`), and `reltuples`/`pg_total_relation_size` for `log_segments`, `log_txns`, `log_consumers`,
  `log_partitions`, `log_dlq`, `stats`. Settles §4.7 and bounds `S` and `D`.
* **F — bloat.** `pg_stat_user_tables` for the queen tables: `queen.stats` should show `n_dead_tup` ~6-7×
  `n_live_tup`; `log_partitions`/`log_consumers` with high `n_dead_tup` *and* a recent `last_autovacuum` is
  the xmin-pin signature.
* **G — bound `seg_bytes`.** Compare `SELECT partition_id, count(*) ... GROUP BY partition_id` (index-only)
  against the real `seg_bytes` (heap). If the ratio is under ~2.5×, `seg_bytes` is below 25% of the mean and
  **T2.2 should stay behind T1**.
* **H — confirm the stats leak.** `'namespace'`/`'task'` rows in `queen.stats` with no surviving queue.

---

## 8. Safety review — verdicts and ship checklist

Adversarial pass, 2026-08-18. Verdicts: **CHANGE 2 = ship. T1.1-T1.5 = ship. T1.0 = ship alone, last, with all
ten conditions.** What follows are the conditions, phrased so a reviewer can check them off in a diff.

### 8.1 The traps, ranked by blast radius

1. **T1.0 `retained_bytes` clobber.** `EXCLUDED` carries DDL defaults for columns absent from the INSERT list,
   and `retained_bytes` is `NOT NULL DEFAULT 0` (`schema.sql:209`). So removing the CTE and the INSERT-list
   entry while **leaving** `retained_bytes = EXCLUDED.retained_bytes` at `011:255` writes a literal **0 to all
   63 rows every 30 s** — no error, no NULL violation, no log line. `018:253` then emits 0, `registry.rs:382-385`
   sums 0, `decide_over_storage` (`registry.rs:507-515`) returns false for every cluster, and the hard 403
   storage gate is **off fleet-wide**. Required: remove from the CTE, the join, the value, the INSERT list,
   **and** rewrite the SET arm as `retained_bytes = queen.stats.retained_bytes` — not delete it. Omitting it
   from both lists is *also* correct but invisible in a SET-list diff, which is exactly how the fatal variant
   ships by accident.
2. **T1.3 hoisted WHERE.** The merged consumers CTE must carry **no WHERE clause**. `worst` deliberately has
   none (`011:126-134`; the 12-line comment at `011:112-125` exists because a plain MIN once pinned pending
   high forever). Hoisting `lease_agg`'s predicate restricts `worst` to partitions holding a live lease; every
   idle partition then loses its watermark, `COALESCE(w.committed, -1)` falls to -1, and `pending` becomes the
   whole retained range — broker-wide, through the namespace/task/system rollups, into the dashboard. The
   lease predicate lives **only** in `FILTER (WHERE c.lease_expires_at > v_now)` on the processing SUM, and
   that FILTER must not inherit the named-group/`__QUEUE_MODE__` precedence.
3. **CHANGE 2 probe shape.** The probe must be a PK head read —
   `SELECT s.created_at INTO v_head FROM queen.log_segments s WHERE s.partition_id = p_pid AND s.base_offset >= v_from ORDER BY s.base_offset LIMIT 1`
   (the shape already at `006:84-89`). **Reject any `EXISTS (... AND created_at < cutoff)`**: these tables carry
   only their PK (`001:83`, `001:118`), so that form has no stop condition in the no-work case and walks the
   partition's whole live range — a regression dressed as an optimisation, invisible to every correctness test.
4. **Never `ALTER TABLE ... DROP COLUMN` on `queen.stats`.** `schema.sql:181` is `CREATE TABLE IF NOT EXISTS`,
   so a dropped column is never re-created, while old `011` still names `newest_message_at` at `:212/:233/:257`.
   plpgsql bodies are not column-resolved at `CREATE OR REPLACE`, so an old pod's re-apply succeeds and the
   failure surfaces at runtime as 42703 — swallowed by the 30 s sampler at `stats.rs:73-77`. `queen.stats`
   freezes broker-wide, and **an image rollback does not recover it.** Leave the column; edit the comment.

### 8.2 CHANGE 2 — checklist

* **C2-1** Probe = PK head read (above). Test `v_head IS NULL`, not `NOT FOUND` (a second SELECT resets FOUND).
* **C2-2** Cutoff = `GREATEST(p_all_cutoff, p_completed_cutoff)`. Probing on `p_all_cutoff` alone permanently
  disables rule 2 for any queue configured with only `completedRetentionSeconds` (`retention.rs:552` runs the
  phase on either).
* **C2-3** Write the guard as `IF v_head IS NULL OR v_head >= <cutoff> THEN RETURN ...` so a NULL cutoff yields
  NULL, not TRUE, and falls through to the locked path. Do **not** add a NULL-cutoff early return to
  `log_txns_purge_step_v1` — NULL there currently means "purge the whole sidecar" (`006:316-326`).
* **C2-4** The probe is **advisory only**. `v_from` is still read under `FOR UPDATE`; boundary, batch locate,
  DELETE, watermark UPDATE and the `retention_history` row stay exactly as today. No pre-lock value may reach
  a DELETE, an UPDATE or an audit row.
* **C2-5** `FOR UPDATE SKIP LOCKED`; the NOT FOUND arm returns `deleted:0, done:true` **plus a new
  `skipped:true`** key. `step_result` (`retention.rs:609-614`) ignores unknown keys, so this is backward
  compatible with an old binary.
* **C2-6** `retention.rs` parses `skipped`, carries a per-cycle count into `Outcome::Ran`, and includes it in
  the gate at `retention.rs:208-223` so a skipping cycle logs at INFO instead of falling into "idle cycle" at
  DEBUG. **Non-negotiable**: today a lock skip and an idle partition land in the same JSON arm, which makes
  starvation unfalsifiable in prod.
* **C2-7** Rewrite the now-false comments at `006:174-177` / `:309-311` and the LOCK ORDER header at
  `006:25-39`; point both at the existing SKIP LOCKED rationale at `006:496-505`.

**Why it is safe.** The `IF NOT FOUND` arms already exist (`006:172-177`, `:307-311`), and even without them a
NULL watermark makes every predicate NOT TRUE and returns at the `count(*)=0` arm *before* the DELETE. No
per-partition state survives a cycle — the work list is rebuilt inside `cycle_body` every cycle
(`retention.rs:328-340` over `retention.rs:114-128`) — so a skip costs exactly one `RETENTION_INTERVAL`
(900 s) and nothing more. Starvation is not live at this shape: the `log_partitions` row lock is taken only by
pushers (`003:129-136`, `003:325-330`, `005:1259-1270`) — never by pop or ack (`005:123-127`) — and prod runs
~24 payload tx/s over 53,966 partitions, so occupancy is effectively zero. That is a measured snapshot, not an
invariant, which is why C2-6 is mandatory.

**The real payoff is transaction burn, not lock waiting.** `prod.yaml:273` measures the two step SPs at 755 of
~1,009 tx/s. Each is its own autocommitting transaction whose first statement is `FOR UPDATE`, which assigns a
real xid, writes xmax into the tuple, dirties a `log_partitions` page and emits an `XLOG_HEAP_LOCK` — even when
it deletes nothing. With the probe a no-work step is read-only: virtual xid, no clog entry, no WAL, no dirty
page. This is why the probe must short-circuit **before the lock**, not merely before the delete.

### 8.3 T1.1-T1.5 — checklist

* **T1.1** Delete the LATERAL (`011:153-158`), `tail.created_at AS newest_at` (`:149`), the `per_queue` MAX
  (`:201`), the INSERT-list entry (`:212`) and the value (`:233`) — but **keep an explicit
  `newest_message_at = NULL` in the SET list** (`:257`). Dropping it from both lists freezes all 63 prod rows
  at their last timestamp forever while new queues get NULL. Mark the column dead at `schema.sql:213` and fix
  `webdoc/.../internals/stats.mdx`. Do not drop the column (§8.1.4).
* **T1.2** `per_queue` selects `FROM part_agg pa GROUP BY pa.queue_id` with `COUNT(*)`. Keep
  `FROM queen.queues q LEFT JOIN per_queue pq` (`011:240-244`) and `COALESCE(pq.child_count, 0)` (`:223`)
  untouched — folding `queen.queues` into `per_queue` makes `COUNT(*)` count the NULL-extended row and report
  1 partition for every empty queue, which moves `get_queues_v2`'s `partitions` (`018:250`) and with it the
  proxy's `db_partition_floor` (`registry.rs:373-400`). Verified value-identical: `part_agg` is strictly 1:1
  with `log_partitions`, and neither join leg can fan out.
* **T1.3** Beyond §8.1.2: keep the merged CTE referenced **exactly once**. Since PG12 a CTE referenced twice is
  materialised, so joining it into both `part_agg` and `per_queue` turns one scan into a scan plus a
  53,966-row tuplestore plus two probes — the single-pass win evaporates. Carry `processing` **through**
  `part_agg` instead; that also leaves `per_queue` with no join at all, which makes T1.2's `COUNT(*)`
  trivially safe.
* **T1.4** Replace `011:304` with `SELECT child_count INTO v_log_parts FROM queen.stats WHERE stat_type='system' AND stat_key='global';`
  after the aggregators, and delete the now-redundant UPDATE at `:305-306`. Deleting the COUNT outright
  publishes `"segPartitions": null` on `POST /api/v1/stats/refresh` (`status.rs:361-362`, documented at
  `webdoc/.../reference/http/system.mdx:258`). The read-back cannot return NULL —
  `aggregate_system_stats_v2`'s ungrouped `INSERT ... SELECT` (`018:157-177`) always emits the row.
* **T1.5** `ORDER BY q.id` at `011:240-246`, and the `>= 3 s` conjunct on both rate arms (`011:269-283`) with
  the existing `ELSE 0` left **exactly as is** — settled: `ingested_per_second` / `processed_per_second` reach
  no wire surface (`019:600-601` and `:686-687` read `worker_metrics` / `queue_lag_metrics`; `v_agg.ips`/`pps`
  at `019:656-657` are dead assignments), so "preserve the previous value" would be a behaviour change with no
  reader. Describe the ORDER BY as ordering against the **unlocked** manual refresh (`status.rs:356-368`) and
  T1.0's future second writer — not as deadlock prevention for the loop, which is already single-flighted by
  `737_002`.

### 8.4 T1.0 — the ten conditions

1. Remove `retained_bytes` per §8.1.1 (CTE, join, value, INSERT list, and the SET arm rewritten explicitly).
2. The new SP drives from `queen.queues` with `LEFT JOIN ... COALESCE(...,0)`, never from the aggregate alone —
   otherwise a drained queue is never zeroed and can never satisfy the release band (`registry.rs:507-515`).
3. It writes **only** `retained_bytes` — never `last_computed_at`, never the `prev_*` trio, never a full column
   list (`status.rs:345-352` documents that exact incident).
4. Materialise the scan into plpgsql arrays **first**, then pre-lock `ORDER BY queue_id FOR UPDATE`, then
   UPDATE from `unnest`. Locking before the 1,040 MB scan holds row locks across it and blocks the fast lane.
5. One canonical write order over the 63 shared rows: T1.5's `ORDER BY q.id` must already be **in prod** before
   a second writer exists.
6. **Ordering must come from the binary, not the SQL.** Give the bytes loop its own `pg_try_advisory_xact_lock`
   id (not `737_001`, not `737_002`) — the lock id survives the last-boot-wins SQL flap, `ORDER BY` does not
   (a rolled-back pod re-applies `011` without it while the new pod's bytes loop keeps running). Set the
   per-replica interval to **3× the intended cluster cadence** (`prod.yaml:279-286`).
7. Bound the cluster cadence at **5 minutes**, not 10. Blind window = lane period + 60 s (`registry.rs:30`) +
   10 s (`proxy/src/main.rs:194`) ≈ 370 s vs ~100 s today.
8. Do **not** widen `STORAGE_RELEASE_PERCENT` (`registry.rs:44`) — it applies only on the release side, so
   widening it lengthens the stale-high false-block window and does nothing for over-admission.
9. Register the new file in the `PROCEDURES` roster (`schema.rs:27-85`, after `027_kv_quota.sql`). A file not
   in that list is never applied — and with `011` no longer writing the column, that leaves the gauge with **no
   writer at all**. New 011 and the new Rust loop ship in the **same image**, never separated. Update the two
   published freshness claims (`schema.sql:206-208`, `webdoc/.../reference/http/queues.mdx:264`) and write down
   the accepted new-queue blind window.
10. The loop WARNs when it has not completed within N periods (`stats.rs:33-35`, `:64-77` idiom). A frozen
    `retained_bytes` behind a hard 403 has no other symptom.

**T1.0 rollback is not a plain redeploy.** If a rollout is aborted after any pod applied the new `011` and no
surviving pod reboots, `retained_bytes` has no writer and freezes at its last value (`NOT NULL DEFAULT 0` means
freeze, not zero) behind the 403 gate with no error anywhere. The rollback procedure is "redeploy the old image
**and force-restart all three replicas**", not "scale the canary to zero".

### 8.5 Coverage — why PR1 is tests

Both changes would ship into a suite that stays green if retention deleted too early or if every stats number
went wrong. `clients/client-js/test-v2/retention.js` pushes 100 messages, sleeps, pops, and passes when
`messages.length === 0` — it asserts deletion **happened**, never that data younger than the cutoff survived.
`clients/client-rust/tests/admin.rs:1806-1820` asserts key presence only, never a value, and `admin.rs:1064`
discards the refresh result. The SQL-shape pin at `retention.rs:698-731` covers only the two cleanup SPs, not
the two edited step SPs. And `server/tests/kv_timers_boot_idempotence.rs` — the only guard against a
second-boot apply failure under last-boot-wins — never runs in CI (`.github/workflows/tests.yml:72` passes no
`--ignored`). Use the `server/tests/kv_timers_source_pins.rs` pattern (no DB, no `#[ignore]`, runs in CI today).

### 8.6 PR split

| PR | Contents | Gate |
|---|---|---|
| **PR1** | Source pins + one `#[ignore]` value-invariance fixture captured against **today's** code | Green CI; fixture passes on master |
| **PR2** | CHANGE 2 | PR1 fixture unchanged; boot-idempotence run by hand; `EXPLAIN` both probes on a rig (Index Scan, rows=1, no filter walk); stage soak with a deliberate pod restart; hold a partition lock from a second session and assert `deleted=0, skipped=true, log_start` unchanged |
| **PR3** | T1.1 + T1.2 + T1.3 + T1.4 + T1.5 — pure SQL, no new writer, no new cadence | Fixture byte-identical before/after; `segPartitions` non-null; `stats` `elapsed_ms` drops; `pg_stat_database.deadlocks` unchanged over 30 min |
| **PR4** | T1.0 **alone** | PR3 stable in prod ≥ 24 h; all ten conditions in the diff |

Watch after deploy: PR2 — `retention` INFO `swept`, `segments_deleted`/`txns_purged` stay non-zero,
`elapsed_ms` falls, and the new skip counter is not sustained on the same partitions across 4+ cycles (the fix
if it is: one blocking re-pass over skipped pids at the end of the phase). PR3 — `stats` INFO `refresh`,
`elapsed_ms` down and `queuesUpdated` still 63; **any jump in `pending` is the T1.3 hoisted-WHERE trap, roll
back immediately**. PR4 — `retainedBytes` non-zero and *moving* between two samples one lane period apart; a
fleet-wide 0 is condition 1, a value that never moves is the frozen gauge.

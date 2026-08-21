# PLAN_CONFLATION — last-value delivery as a consumer-group delivery policy

Rev 1.0 — 2026-08-21. Grounded in a read of the live tree at `master` (1.0.6, `23acdc79`).
Every file:line reference below is from that tree. **Design only — no code was changed.**

Target workload: command-style queues where one partition = one logical task key and only
the newest pending message per partition matters ("recompute entity X", dirty-flag
workloads). Under backlog the consumer processes ONE message — the newest — instead of the
whole backlog.

Name: **conflation**, everywhere. Not "compaction": nothing on disk is touched, retention
still governs storage.

---

## 0. What the ground-truth read changed about the sketch

Read this section first. Seven things in the original sketch do not match the code, and two
of them make the feature *smaller* than proposed.

**M1 — `commit_to` already exists. It is `queen.log_consumers.batch_end`.**
The lease row already records the inclusive end of the delivered span
(`001_log_schema.sql:137`), and every ack path already commits to it and refuses to go past
it (`p_upto > v_c.batch_end` → `'position beyond leased batch'`, `005_log_ack.sql:151`).
**No new commit-to column, no ack wire change, no ack SP signature change.** Conflation is
"write a `batch_end` that is far ahead of the single delivered offset".

**M2 — the DLQ livelock the plan wants fixed does not exist.** The retry budget is
`batch_retry_count`, it lives on the `(partition, group)` row, it is charged only by an
explicit `failed` ack (`005_log_ack.sql:665`), and **the pop path never touches it** —
verified by grep across `server/sql` and `server/src`: the only writers are
`005_log_ack.sql:665,692,732,843`, `007_log_streams.sql:377`, `010_log_admin.sql:237`.
It resets on batch *completion*, on DLQ filing, and on seek — never on a new delivery. A
poison partition with a hot producer therefore already DLQs on schedule. What *is* wrong
under conflation is the **telemetry** counter `attempt_count`, which resets whenever the
delivered start offset changes (`004_log_pop.sql:404`, `:2013`) — under supersession that
is every attempt. §1.4 fixes it with a one-token change and no new column.

**M3 — the "mode conflict" precedent is silence, not a warning.** The durable group row is
written by `INSERT … ON CONFLICT DO NOTHING` (`004_log_pop.sql:612-615`,
`:1098`). A second consumer declaring a different `subscriptionMode` is dropped on the
floor with no log line, no counter and no response field. Mirroring that literally would
make conflation silently disagree between consumers. §3.3 keeps *group-setting-wins* and
adds a loud channel (counter + rate-limited `obs` line + response echo). This is a
deliberate, narrow improvement over the precedent, not a re-litigation of it.

**M4 — a group that only ever uses pinned pops has no durable group row at all.**
`queen.log_pop_v1` *reads* `consumer_groups_metadata` (`004_log_pop.sql:176-180`) but never
writes it; only the wildcard and discovery SPs register. So "persisted on the consumer
group at creation" has a hole on the `GET /api/v1/pop/queue/:q/partition/:p` route.
§2.2 closes it.

**M5 — `partitions` defaults to 1, so a naive conflating pop returns ONE message.**
`max_parts = p.partitions.unwrap_or(1)` (`handlers/data.rs:700`), and a conflating pop
yields ≤1 message per partition. The batch budget stops being the thing that sizes a pop;
the partition cap is. §3.2 raises `max_parts` for conflating groups. Note the hard ceiling:
the checkout width is `clamp(2, 64)` (`handlers/data.rs:1416`, `:1557`) and that 64 is
**load-bearing and measured** (`data.rs:1545-1556`: raising it to 512 was 4–13× worse). So
a conflating pop returns **at most 64 messages per round trip**, whatever `batch` says.

**M6 — `seekToTail` is not a future sibling; the server half already shipped.**
`POST /api/v1/consumer-groups/:group/queues/:queue/seek` with `toEnd=true`
(`main.rs:936`, `010_log_admin.sql:247`) does exactly this, per queue and per partition.
What is missing is the *SDK method* next to `getQueueDepth`. Still out of scope (§9), but
as "expose the existing route", not "build the operation".

**M7 — subscription mode is not displayed anywhere today.** `get_consumer_groups_v4` emits
`'subscriptionMode', NULL` hard-coded (`010_log_admin.sql:475-477`) and the webapp shows no
mode. "Exactly like subscription mode works today" therefore gives conflation *no* console
surface for free. §5 adds one.

Two smaller notes carried into the relevant sections: the webapp never calls the depth
endpoint at all (§5.3), and `queen_queue_depth_total` / `queen_queue_depth_pending` are
already dead series (§6.3).

---

## 1. Semantics

### 1.1 Declaration and scope

`conflation` is a **boolean property of a consumer group on a queue**, stored beside
`subscription_mode` on `queen.consumer_groups_metadata`. It is declared in consume/subscribe
options, persisted on first registration, and **the stored value wins for every consumer of
that group from then on**. It is not a per-call flag, not a queue property, and not settable
through `/configure` (whose column-reset defect — `queen-confirmed-defects-2026-08-05` — is
reason enough on its own).

Default **off**. A group created before this feature, or created without the flag, behaves
byte-identically to today.

### 1.2 Delivery

For a conflating group, a pop of a partition delivers **exactly one message: the newest
VISIBLE one** (the *tail*), and leases the span `(committed, tail]`:

```
log_consumers.committed  = C           (unchanged by pop)
delivered                = {T}         (one frame, at offset T)
log_consumers.batch_end  = T           (= commit_to, observed under the claim)
```

Under backlog this is **cheaper** than a normal pop: one backward PK step on
`queen.log_segments` instead of a head probe plus a forward scan, and one blob detoasted
instead of up to `batch`.

**"Newest VISIBLE"** is exact, not approximate, because of two invariants the engine already
maintains:

* **PUSHSER** — `created_at` is stamped after the `queen.log_partitions` allocator row lock
  and that lock is held to commit, so *offset order is commit order* per partition
  (`001_log_schema.sql:12-14`, `003_log_push.sql:138-139`, `:201-202`). No message can
  become visible at an offset *below* one that is already visible.
* **`delayed_processing` cuts a contiguous suffix** — the filter is `created_at <= now -
  delayed` and `created_at` is monotone in `base_offset`, which the pop already relies on
  (`004_log_pop.sql:278-281`). So the visible set is a prefix `[log_start, V]` and the
  deferred set is the suffix `(V, last_offset]`.

Therefore `tail = V`, and **committing to `V` can never skip an offset that becomes visible
later** — everything not yet visible sits strictly above `V`.

`window_buffer` is a partition-level all-or-nothing gate (deliver nothing while the
partition is hot, `004_log_pop.sql:153-162`, `:1765-1776`), so it composes trivially.
Timers do not create deferred offsets at all: `queen.log_timers_fire_v1` **pushes at fire
time**, inside one transaction with the row DELETE (`025_log_timers.sql:8`, `:21`) — a timer
message is born at the tail like any other push.

### 1.3 The guarantee

> **G.** After the last push to a partition, at least one execution of that partition's
> handler *starts* after that push commits.

Proof sketch, to be pinned by the test in §7.3. Let push `P` commit at `t_P` with offset
`o_P`. Any pop whose tail probe runs under a snapshot taken after `t_P` sees `o_P ≤ tail`,
so it delivers `tail ≥ o_P` and the handler starts after `t_P`. Any pop whose snapshot
predates `t_P` computes `tail < o_P` (PUSHSER: offsets are commit-ordered, so nothing
committed after `t_P` can carry an offset below `o_P`… and `o_P` itself is not visible), so
its ack sets `committed = tail < o_P`, leaving `last_offset > committed` — the standing
pending predicate used by `log_has_pending_v1` (`004_log_pop.sql:1295`),
`log_partition_has_pending_v1` (`:1330`), `log_discover_has_pending_v1` (`:1396`) and the
hot-list `drained` derivation (`handlers/data.rs:1743`). The push also marks the ring
directly. So another pop follows. ∎

Corollary, and the whole point: **the broker never commits past an offset that was not
observed at pop time.** `batch_end` is written from the same claim that read the tail, and
the ack clamp (`p_upto > batch_end` → reject, `005_log_ack.sql:151`, `:231`) is already the
enforcement.

### 1.4 Episodes, retry and DLQ

An **episode** is one continuous span of "the cursor has not moved". It begins when
`committed` last advanced and ends when it advances again (successful ack, drop, or DLQ).
Within one episode the delivered message may be superseded arbitrarily often.

* **Retry budget — already correct, no change.** `batch_retry_count` is per
  `(partition, group)`, charged once per explicit `failed` (`005_log_ack.sql:657-667`), and
  never reset by a pop. It therefore carries across supersession for free. When it reaches
  `queues.retry_limit`, the ack takes the DLQ branch (`:670-681`) or the drop branch
  (`:682-697`) exactly as today.
* **Attempt telemetry — one-token fix.** Today `attempt_offset` is set to the *first
  delivered offset* (`004_log_pop.sql:406`, `:2015`), which under conflation changes on
  every supersession and resets `attempt_count` to 1 for ever. For a conflating lease, set
  `attempt_offset = committed + 1` — the **episode anchor**, which is invariant across
  supersession and changes exactly when the episode ends. For a non-conflating batch
  `attempt_offset` is *already* `committed + 1` in the common case (`v_start := v_wanted`,
  `:315`), so this makes the two paths agree rather than diverge.
* **What goes to the DLQ.** The message that was tail at the *last* attempt, i.e.
  `v_poison_off = v_sig_off` = the offset the failing hash resolved to
  (`005_log_ack.sql:613-617`). The existing broker hand-off decodes that offset's frame via
  `queen.log_segment_at_v1` (`003_log_push.sql:393`) and files it with
  `log_dlq_head_v1`, which advances `committed = GREATEST(committed, p_off)` and resets the
  whole episode (`005_log_ack.sql:838-846`). Under conflation that advance is the cursor
  *jump* — the episode's superseded messages are retired with the poison, which is the
  correct semantics for last-value delivery. **Zero changes to the DLQ machinery.**

### 1.5 Composition with subscription mode

| combination | first pop | steady state |
|---|---|---|
| `mode=all` + conflation | seeds `committed = GREATEST(log_start-1, -1)`, then conflates the **entire retained history** to one message per partition | newest per partition |
| `mode=new` + conflation | seeds `committed = last_offset` (`004_log_pop.sql:639`), so history is skipped by the seed, not by conflation | newest per partition |
| `mode=timestamp` + conflation | seeds at the first segment ≥ the stored timestamp, then conflates everything from there | newest per partition |
| `__QUEUE_MODE__` (group-less) | mode is hard-pinned to `all` | **conflation is refused** — see §3.3 |

Both are intended. `all` + conflation is the "rebuild the world once, then stay current"
shape; `new` + conflation is the "only care about live dirt" shape.

### 1.6 Concurrency

Two consumers of the same conflating group can never process two different tails of the
same partition at once: the claim takes the `(partition, group)` row with
`FOR UPDATE SKIP LOCKED` (`004_log_pop.sql:236-240`, `:1666-1669`) and a live foreign lease
excludes the row from the claim set entirely. Conflation needs **no new locking**, and it
takes no lock the pop does not already take, so the six-space lock order documented at
`005_log_ack.sql:904-926` is untouched.

---

## 2. Storage / SQL changes

> **SQL is `include_str!`-embedded** (`server/src/schema.rs:23`, `:48-97`). Editing a `.sql`
> file has **no effect** until `cargo build` re-embeds it. This has bitten before
> (`rustfix-plan-implemented`); every SQL task below implies a rebuild before any test run.

> **Migration mechanism.** `schema.rs::apply` re-runs `sql/schema.sql` + every
> `procedures/*.sql` at every boot under a session advisory lock, last-boot-wins. Because
> the table DDL is `CREATE TABLE IF NOT EXISTS`, **editing a `CREATE TABLE` body does not
> add a column to an existing database.** The established idiom is an idempotent
> `ALTER TABLE … ADD COLUMN IF NOT EXISTS` next to the create — precedent
> `019_worker_metrics.sql:95-119` and `024_kv.sql:237-252` (whose comment says so in as many
> words). Both new columns below follow it. Also: `queen.log_partitions` /
> `queen.log_consumers` carry hand-tuned storage params and both new columns are
> **non-indexed**, so the updates that write them stay HOT.

### 2.1 `queen.consumer_groups_metadata` — the durable declaration

`server/sql/schema.sql`, in the `CREATE TABLE` body at `:79-96` **and** as an idempotent
ALTER below the `cgm_identity_uk` index (`:100-103`):

```sql
-- Conflation (PLAN_CONFLATION §1.1): last-value delivery for this group on this
-- queue. Sits beside subscription_mode because it is the same kind of fact — a
-- delivery policy fixed at group creation, never re-negotiated by a later pop.
-- NOT part of cgm_identity_uk: the identity of a group must not change with its
-- policy, or a second consumer declaring a different value would create a
-- SECOND group instead of colliding with the first.
ALTER TABLE queen.consumer_groups_metadata
    ADD COLUMN IF NOT EXISTS conflation BOOLEAN NOT NULL DEFAULT FALSE;
```

No new index: every read is by the existing `cgm_identity_uk` prefix or `idx_cgm_queue`.

### 2.2 `queen.log_consumers` — the per-lease marker

`server/sql/procedures/001_log_schema.sql`, in the `CREATE TABLE` body at `:133-147` **and**
as an ALTER beside the storage params at `:150-160`:

```sql
-- The lease this row currently holds was a CONFLATING one (PLAN_CONFLATION §2.2).
-- Written by the pop that took the lease, read by the ack that closes it. It
-- exists because ack must be able to (a) report the skipped count and (b) apply
-- the completion clamp WITHOUT a second lookup: the ack path already does
-- SELECT * INTO v_c on this row, so this column costs nothing to read, and it is
-- non-indexed so writing it keeps the lease UPDATE HOT.
ALTER TABLE queen.log_consumers
    ADD COLUMN IF NOT EXISTS lease_conflated BOOLEAN NOT NULL DEFAULT FALSE;
```

### 2.3 `004_log_pop.sql` — the pop side

All five entry points gain a `p_conflate BOOLEAN DEFAULT FALSE` **appended last with a
default**, the same discipline `p_tenant` used (`004_log_pop.sql:87-90`). The three
`DROP FUNCTION IF EXISTS` lines that guard `DROP+CREATE` signatures
(`:91`, `:426`, `:1508`) must be updated in the same edit, or boot re-apply breaks —
this is called out explicitly at `:61-64`.

**`queen.log_pop_v1`** (`:92`, the claim core):

1. New parameter `p_conflate BOOLEAN DEFAULT FALSE` after `p_tenant`; update the `DROP` at
   `:91`.
2. **First-contact registration (closes M4).** Inside the existing "no consumer row" branch
   (`:171-223`), after the durable lookup at `:176-180` returns nothing, write the group row
   the wildcard path writes — same statement as `:612-615`, carrying `p_conflate` and the
   pop-derived `sub_mode`/`sub_from`. Guarded by `p_group <> '__QUEUE_MODE__'` exactly as
   the surrounding branch already is. Read the row back so `v_conflate_eff` is the STORED
   value, never the requested one.
3. **Tail claim.** Replace the head probe (`:295-317`) + forward scan (`:322-349`) with a
   branch:

```sql
IF p_conflate THEN
    -- ONE backward PK step: the newest VISIBLE segment. delayed_processing is
    -- applied here (not in the walk) because there is no walk; monotone
    -- created_at makes the deferred set a suffix, so a filtered backward scan
    -- lands exactly on the newest visible offset (§1.2).
    SELECT s.base_offset, s.end_offset, s.created_at, s.blob
    INTO v_head
    FROM queen.log_segments s
    WHERE s.partition_id = v_pid
      AND (v_deadline IS NULL OR s.created_at <= v_deadline)
    ORDER BY s.base_offset DESC LIMIT 1;

    IF v_head.base_offset IS NOT NULL AND v_head.end_offset >= v_wanted THEN
        r_base       := v_head.base_offset;
        r_start_idx  := (v_head.end_offset - v_head.base_offset)::int;
        r_take       := 1;
        r_msg_count  := (v_head.end_offset - v_head.base_offset + 1)::int;
        r_created_at := v_head.created_at;
        r_blob       := v_head.blob;
        RETURN NEXT;
        v_taken := 1;
        v_start := v_wanted;              -- EPISODE ANCHOR (§1.4), not v_last
        v_last  := v_head.end_offset;     -- batch_end = commit_to
    END IF;
ELSE
    ... unchanged head probe + forward scan ...
END IF;
```

   The zero-taken seal at `:351-380` is reached unchanged and stays correct: its guard is
   `NOT EXISTS (SELECT 1 FROM queen.log_segments WHERE partition_id = v_pid)`,
   **unfiltered**, so a partition whose only segments are deferred is never sealed past
   them.
4. **Lease UPDATE** (`:399-407`): add `lease_conflated = p_conflate`. The `attempt_count` /
   `attempt_offset` expressions are untouched — `v_start` already carries the episode anchor
   from step 3.
5. **Auto-ack branch** (`:382-391`): add `lease_conflated = FALSE`. An auto-acking
   conflating pop commits `committed = v_last = tail` in-transaction, which is the right
   thing; there is no lease to describe.

**`queen.log_pop_specific_v1`** (`:427`): thread `p_conflate` through to `log_pop_v1`
(`:455-456`) and update the `DROP` at `:426`.

**`queen.log_pop_list_v1`** (`:1509`, the hot path): new param, updated `DROP` at `:1508`,
and three edits inside:

* **PASS 2** (`:1710-1734`) — a conflating group uses a different LATERAL. The second leg
  exists only to preserve the `v_has_rows` seal decision when `delayed_processing` hides
  every segment; its `WHERE v_delayed > 0` makes it free otherwise:

```sql
CROSS JOIN LATERAL (
    (SELECT 1 AS kind, s1.base_offset, s1.end_offset, s1.created_at
     FROM queen.log_segments s1
     WHERE s1.partition_id = a_pid[req.ord]
       AND (v_deadline IS NULL OR s1.created_at <= v_deadline)
     ORDER BY s1.base_offset DESC LIMIT 1)
    UNION ALL
    -- seal guard only: proves segments EXIST even when all are deferred.
    (SELECT 0 AS kind, s0.base_offset, s0.end_offset, s0.created_at
     FROM queen.log_segments s0
     WHERE v_delayed > 0 AND s0.partition_id = a_pid[req.ord]
     ORDER BY s0.base_offset DESC LIMIT 1)
) s
```
  ordered `req.ord, kind` and stored in the existing `m_*` arrays plus a new `m_kind`.

* **THE WALK** (`:1785-1824`) — a conflating branch that consumes the partition's metadata
  block, sets `v_has_rows := TRUE` for any row, and serves at most the `kind = 1` row:
  `take = 1`, `startOff = end_offset - base_offset`, `v_start := v_wanted`,
  `v_last := m_end[m_i]`, `v_taken := 1`. Budget accounting (`v_remaining -= 1`,
  `v_claimed += 1`) and the EXIT at `:1758` are unchanged — see M5 for the consequence.

* **Batched lease UPDATE** (`:2007-2024`) — add `lease_conflated = p_conflate`; add
  `lease_conflated = FALSE` to the auto-ack UPDATE at `:1988-2002`.

The `states` tri-state, the `lastOff` RETURNING, the blob fetch and the assembly are
**untouched**, so `drained` (`handlers/data.rs:1741-1747`) keeps working — and works
*better*: a conflating serve sets `batch_end = tail`, so `be >= lo` is true exactly when the
partition is fully retired for that group.

**`queen.log_pop_wildcard_wire_v1`** (`:495`) and **`log_pop_wildcard_bin_v1`** (`:780`):
new param; store it in the registration INSERT at `:612-615` / `:855`; forward it to
`log_pop_v1` in the candidate loop. **`queen.log_pop_discover_wire_v1`** (`:1024`): same,
registration INSERT at `:1098`, forward at `:1205-1207`.

**Unchanged in this file:** the three `has_pending` probes (`:1282`, `:1322`, `:1361`) —
`last_offset > committed` is still exactly "there is a newer message", which is precisely
the conflating pending predicate; and both reseed SPs (`:2081`, `:2195`).

### 2.4 `005_log_ack.sql` — the ack side

**No signature changes. No wire changes.** Two additive edits, both inside
`queen.log_ack_by_hash_v1` (`:446`), plus the same reporting field in the two positional
twins.

1. **Conflated completion clamp** — in branch (6) (`:716-740`), before computing
   `v_reached_end`:

```sql
-- Conflation (PLAN_CONFLATION §2.4). A conflating lease delivered EXACTLY ONE
-- frame, at batch_end. A clean ack of it therefore completes the whole leased
-- span by construction, and the cursor belongs at batch_end — the offset the pop
-- OBSERVED under its claim (§1.3), never beyond it. Without this the MIN-in-span
-- rule (:425-431) would resolve a REPEATED transactionId to its lowest in-span
-- occurrence and leave the cursor short of the delivered offset, keeping the
-- lease open and re-delivering the same tail for ever. Only reachable on a clean
-- ack: v_sig_kind failed/dlq/retry returns from branches (3)/(4)/(5) above.
IF v_c.lease_conflated AND v_has_lease AND v_max_ok IS NOT NULL THEN
    v_new   := v_c.batch_end;
    v_delta := GREATEST(v_new - v_c.committed, 0);
END IF;
```

2. **Skipped-count reporting** — the return objects at `:742-743` (and the two full-batch
   returns) gain, when `v_c.lease_conflated`, `'conflated', GREATEST(v_delta - 1, 0)`.
   That is exactly the author's formula: `commit_to − previous cursor − 1`.
   Same one-line addition in `queen.log_ack_at_v1` (`:193`, the ack-registry fast path) and
   in the per-row verdict of `queen.log_ack_multi_v1` (`:291`, the ack-fusion path), both of
   which already read the pre-update `committed` under the row lock.

**`total_consumed`** keeps counting `v_delta` — i.e. **log positions retired**, not handler
invocations. Do not change it: it is what `get_consumer_groups_v4` calls "consumed" and what
the `Dead`/`Stable` state derives from (`010_log_admin.sql:452`). The new `conflated`
counter is the honest second number.

**`queen.log_transaction_wire_v1` (`:1108`) needs no change**, and this was the sharpest
thing to verify. The 1.0.5 bogus-ack rule rejects a bundle only when
`unresolvedHashes` is non-empty (`:1366-1369`) — i.e. when a hash resolves *nowhere*, not
when the cursor advances past undelivered offsets. A conflating ack carries the hash of a
frame that exists at `batch_end`, so it always resolves. The cursor advance beyond the
delivered offset is produced by the existing `committed = v_new` write, which the wire never
inspects. The positional leg (`:1381-1389`) is likewise clamped by `batch_end` inside
`log_ack_v1`, and `batch_end` *is* the conflated commit point. **Conflating ack composes
with atomic push+ack unchanged.**

`queen.log_dlq_head_v1` (`:796`): unchanged. It already resets `lease_conflated`'s
neighbours; add `lease_conflated = FALSE` to its UPDATE (`:838-846`) and to the two ack
UPDATEs that release a lease (`:661-667`, `:727-734`, `:688-694`, `:706-711`) — a released
lease must not leave a stale marker for the next lease of a group whose policy changed.

### 2.5 `011_log_stats.sql` — depth

`queen.log_queue_depth_v1` (`:826`) gains two fields inside its existing aggregate — no new
scan, no new join beyond one indexed `cgm` lookup:

```sql
'partitionsPending', COALESCE(d.nonempty, 0),   -- SUM(CASE WHEN t.pending > 0 THEN 1 ELSE 0 END)
'conflation',        COALESCE(g.conflation, false),
'effectivePending',  CASE WHEN COALESCE(g.conflation, false)
                          THEN COALESCE(d.nonempty, 0) ELSE COALESCE(d.total, 0) END
```

with `g` a `LEFT JOIN queen.consumer_groups_metadata g ON g.queue_id = q.id AND
g.consumer_group = p_group AND g.partition_name = ''` (NULL when `p_group IS NULL`).

`partitionsPending` is useful for every group, conflating or not — `queenctl` already
computes it client-side and calls it `partitionsNonEmpty`
(`clients/client-cli/cmd/queue.go:238-257`). Moving it into SQL kills that duplication.

`queen.log_refresh_all_stats_v1` (`:106`) and `queen.stats` are **not** touched. Making
`queen.stats.pending_messages` conflation-aware would mean a per-group scan in the refresh
lane that the whole 1.0.4 stats diet exists to remove
(`queen-stats-refresh-diagnosis`). Depth is the per-group surface; stats stays per-queue.

### 2.6 `010_log_admin.sql` — group listing (fixes M7)

`queen.get_consumer_groups_v4` (`:400`): join `consumer_groups_metadata` on
`(queue_id, consumer_group, partition_name='')` and replace the three hard-coded NULLs at
`:475-477` with the real `subscription_mode` / `subscription_timestamp` / `created_at`, plus
a new `'conflation'` field. One indexed lookup per aggregated group on a console route.
This is the smallest honest fix; it also closes the documented hole at
`webdoc/src/content/docs/reference/http/consumer-groups.mdx:76`.

`queen.get_consumer_group_details_v1` (`:951`): same field, same join.

### 2.7 Not touched

`003_log_push.sql` (push, fusion, dedup probe), `006_log_maintenance.sql`,
`007_log_streams.sql` (streams carry their own cursor/ack path — explicitly out of scope,
§9), `012_configure.sql`, `024`–`029`.

---

## 3. Broker changes (Rust)

### 3.1 Wire

One new **query parameter on the pop routes**, matching the existing `subscriptionMode`
shape exactly (query string, never a body field):

```
GET /api/v1/pop/queue/:queue?consumerGroup=workers&conflation=true
GET /api/v1/pop/queue/:queue/partition/:partition?...&conflation=true
GET /api/v1/pop?namespace=…&task=…&conflation=true
```

**Ack is unchanged** — no new field, no new status, no offsets (§2.4). This was the
load-bearing thing to confirm and it holds.

Response: `render_pop_parts` (`handlers/data.rs:2450`) emits, **only when the effective flag
is true**, a top-level `"conflation":true`, and `"conflationConflict":true` when the request
disagreed with the stored value (§3.3). Every existing deployment keeps byte-identical
responses, which matters because `handlers::data::protocol_conformance` pins those bytes
against `queen-protocol` (`server/Cargo.toml:110-119`).

### 3.2 Files and functions

| file | change |
|---|---|
| `server/src/handlers/data.rs:594` `PopParams` | `#[serde(rename = "conflation")] conflation: Option<bool>` |
| `server/src/handlers/data.rs:2171` `PopDiscoverParams` | same field |
| `server/src/handlers/data.rs:687` `handle_pop` | resolve the effective flag (§3.3); **M5:** `let max_parts = if conflate { p.partitions.unwrap_or(batch).clamp(1, 64) } else { p.partitions.unwrap_or(1) };` |
| `server/src/handlers/data.rs:2018` `handle_pop_partition` | resolve + thread; `max_parts` is irrelevant here (one partition) |
| `server/src/handlers/data.rs:2201` `handle_pop_discover` | resolve + thread |
| `server/src/handlers/data.rs:1057` `serve_pop_hotlist`, `:1285` `hotlist_pop_attempt`, `:893` `try_targeted_serve` (the mesh-hint path, which calls `db::pop_specific`) | one more `bool` argument, threaded to the `db::` calls |
| `server/src/handlers/data.rs:2450` `render_pop_parts` | the two conditional response keys |
| `server/src/db.rs:307` `pop_specific`, `:346` `pop_list`, `:382` `pop_list_tx`, `:1690` `pop_wildcard_bin`, `:1748` `pop_discover` | one more bind; the statement text gains `$13` / `$11` etc. **`pop_list` and `pop_list_tx` must keep IDENTICAL statement text** — that identity is deliberate (`db.rs:378-380`) so the prepared-statement cache is shared |
| `server/src/pop_fusion.rs:135` `Job` | `conflate: bool` field; `:214` `claim(...)` one more argument; `:338-345` forwards it. Jobs already carry disjoint candidate sets per `(queue, group)` and no fold key (`:159`), so **no batching-key change is needed** |
| `server/src/handlers/mod.rs:100` `seeded_groups` | `HashMap<String, HashSet<String>>` → `HashMap<String, HashMap<String, GroupPolicy>>` where `GroupPolicy { conflation: bool }` |
| `server/src/handlers/mod.rs:241` `group_seeded` | becomes `group_policy(&self, queue, group, tenant) -> Option<GroupPolicy>`; `group_seeded` keeps its name as `.is_some()` so the first-contact bootstrap gate at `data.rs:1314` reads unchanged |
| `server/src/db.rs:657` `group_seed_marker_exists` | `SELECT EXISTS(...)` → `SELECT conflation FROM queen.consumer_groups_metadata …` returning `Option<bool>` |
| `server/src/reconcile.rs` | the existing per-queue cache clear must also clear the new policy map (it already clears `seeded_groups`, which is why a delete+recreate self-heals within one interval — `handlers/mod.rs:97-99`) |
| `server/src/metrics.rs:151` `QueueCounters`, `:185` `PerQueue` | `conflated_count` + `add_conflated(tenant, queue, n)` |
| `server/src/handlers/data.rs:2753` `handle_ack` / `:2774` `handle_ack_batch` | read the optional `conflated` field off the SP result and feed `add_conflated`; **no request-side change** |
| `crates/queen-protocol/src/pop.rs:167` `PopParams` | `pub conflation: Option<bool>` + a `to_pairs()` arm at `:196-236`, emitted **only when `Some(true)`** (mirroring the `auto_ack` precedent at `:216-220`) |

`server/src/ack_registry.rs` is **unchanged** and this deserves saying: its three guardrails
(`:21-40`) are "every item completed", "acked hash set == delivered hash set", "worker
matches". A conflating delivery is one frame, so the delivered set has one member and an
exact cover is trivially reached; `batch_end` derived broker-side as
`seq + startOff + take - 1` (`data.rs:1704`, `:2507-2524`) equals the tail the SP wrote.
The fast path therefore fires on conflating acks and advances `committed = tail` through
`log_ack_at_v1` — which is exactly right.

`server/src/hotlist.rs` is **unchanged**. The ring is a *candidate* index, not a delivery
index; a conflating serve returns one message per candidate and the tri-state verdicts
(`took`/`empty`/`leased`) mean what they meant. One consequence to document, not fix: the
budget-aware claim (`take_batch`, `:1428`) stops claiming once the ring's `batch_count`
estimate covers `want`, and for a conflating group that estimate over-counts (a partition
with 500 marks yields one message). The result is a *narrower* claim than ideal, i.e. more
pops for the same work — bounded, never wrong, and the `k` ceiling caps it anyway. §10 Q4
proposes the follow-up.

### 3.3 Resolving the effective flag, and the conflict rule

**SQL is the authority; the broker caches it.** The request flag is used for exactly two
things: the first registration write, and detecting a conflict.

```
pop arrives with conflation=R (or absent)
  → st.group_policy(queue, group, tenant)                 // cached; zero DB on a hit
      hit  → effective E = stored.conflation
      miss → one indexed cgm read; still missing (unregistered group)
             → E = R, and the registering pop persists R
  → if R is Some(r) and r != E:  conflict (see below)
  → every pop SP receives E, never R
```

This is why the cache extension in §3.2 matters: `group_seeded` is already a zero-DB cache
hit on the steady-state hot path (`handlers/mod.rs:232-240`), so the authority read is free.
For `handle_pop_partition` and `handle_pop_discover`, which do not call it today, the first
call per `(queue, group)` per process costs one indexed lookup and every later one is free.

**Conflict handling — group setting wins, loudly:**

1. The stored value is used. Nothing flips.
2. `metrics.per_queue.add_conflation_conflict(tenant, queue)`; the `rates` line grows a
   `conflict_s` field (§6.1). **No per-request log line** — a mismatched fleet would flood.
3. The response carries `"conflationConflict":true`, and SDKs emit **one** warning per
   `(queue, group)` per process (§4).
4. A rate-limited `obs::Sampler` line (60 s, the `POOL_SAT` idiom at `obs.rs:309`,
   `:343-354`) at `target: "conflation"` naming queue + group + stored + requested.

**Refused combinations**, rejected at the handler with `400` and a message that names the
reason (this is the one place conflation *rejects* rather than warns, because both are
consumer bugs whose silent form is unfixable in production):

* `conflation=true` with `consumerGroup` absent (i.e. `__QUEUE_MODE__`). Queue mode is a
  shared cursor with no group identity to hang a policy on, and the SQL pins it to
  `sub_mode='all'` already (`004_log_pop.sql:171`, `:596-598`).
* `conflation=true` together with `autoAck=true`. Auto-ack commits at delivery with no
  lease, so a failed handler loses the tail and the guarantee in §1.3 degrades to
  at-most-once. Technically it works (§2.3 step 5); semantically it is a footgun on a
  feature whose entire value is "the newest state is definitely processed". Recommend
  refusing; see §10 Q2.

---

## 4. Per-SDK changes

Seven SDK directories under `clients/`, plus the shared protocol crate. `clients/server` is
**not** an SDK — it is the vendored `threadpool.hpp` / `json.hpp` for the C++ client.

Two facts govern the whole section:

* **The `pop()` and `consume()` param builders are separate code in every SDK except Rust.**
  There is a standing comment about this hazard at
  `clients/client-js/client-v2/builders/QueueBuilder.js:395-402`, left behind by a bug of
  exactly this shape. PHP has **three** builders (`ConsumerManager` + a byte-identical copy
  in `HighLevelConsumer` + the inline one in `QueueBuilder::pop`).
* **There is no cross-SDK conformance suite.** `test/run.sh:220-278`'s "parity" is
  *tenancy* parity (single vs tenanted exit codes), not SDK parity. Nothing will tell you an
  SDK was missed. §7.2 proposes the minimum gate.

| SDK | files to edit |
|---|---|
| **client-js** (`queen-mq` 1.0.6) | `client-v2/utils/defaults.js:54-67` (`CONSUME_DEFAULTS`); `client-v2/builders/QueueBuilder.js:28-42` field, `~:263` setter, `:277-303` consume options, **`:333-352` pop inline params**; `client-v2/consumer/ConsumerManager.js:33-53` destructure, `:71` call, **`:445-461` `#buildParams`**; `client-v2/streams/runtime/Runner.js:61,:229`. No `.d.ts` exists anywhere in the repo. |
| **client-go** | `defaults.go:36-49`; `types.go:184-208` `ConsumeOptions`, **`:211-221` `PopOptions`**; `queue_builder.go:31` field, `~:168` setter, **`:252-310` `buildPopParams`**, `:344-380` `getConsumeOptions`; `consumer_manager.go:601-629` `buildParams`; `streams/runtime/runner.go:30-40` + `streams_adapter.go:20-40` |
| **client-rust** (`queen-mq` 1.0.6) | **`crates/queen-protocol/src/pop.rs:167` + `:196-236` + its unit tests `:476-518`** — one edit serves both pop and consume; `client-rust/src/queue.rs:43` field, `:50-75` init, `~:196` setter, `:333-347` `pop_params()`; `src/streams/runner.rs:75-105,~:141,:374-376`. **Version coupling:** `queen-protocol` is a *regular* dependency of the broker (`server/Cargo.toml:119`) with conformance tests pinning it, so this bump lands in the same release as the broker |
| **client-py** (`queen-mq` 1.0.6) | `queen/utils/defaults.py:35-47`; `queen/builders/queue_builder.py:58` field, `~:264` setter, `:290-311` options, **`:348-370` pop inline params**; `queen/consumer/consumer_manager.py:39-53`, `:73-76`, `:576-610`; `queen/streams/runtime/runner.py:72-84,:232-240`. Note the standing drift precedent: `maxPartitions` is missing from Python's `CONSUME_DEFAULTS` entirely |
| **client-laravel** (`smartpricing/queen-mq`) | `src/Support/Defaults.php:34-47`; `src/Builders/QueueBuilder.php:35` decl, `:61` init, `~:275` setter, **`:315-341` pop inline**, `:388-409` `buildConsumeOptions`; `src/Consumer/ConsumerManager.php:39-44,:508-545`; **`src/Consumer/HighLevelConsumer.php:58-63,:330-366`**; `src/Laravel/Commands/ConsumeCommand.php:10-20,:54-60` |
| **client-cpp** (header-only) | `queen_client.hpp:162-183` `ConsumeOptions`, `:2119` builder field, `~:2291` setter, **`:2323-2345` pop params**, `:2731-2755` consume params, `:2961` builder→options copy |
| **client-cli** (`queenctl`) | `cmd/pop.go:13-26,:61-75,:132` (`--conflation`); `cmd/tail.go:24,:60-80,:144`; `cmd/cg.go` describe output gains the column via `internal/output/formatter.go:58-74`. Requires a `client-go` tag + `client-cli/go.mod:8` bump (currently pinned one minor behind at v1.0.5) |

**Option naming per language** (mirroring how `subscriptionMode` is spelled today):
`conflation` (JS/Py/PHP/Go builder method `Conflation(bool)`), `conflation: bool` (Rust
builder), `conflation` (C++ `bool conflation;`), `--conflation` (CLI).

**Degrade-loudly (M-relevant, and the plan's §8 decision).** No SDK does any version or
capability negotiation — greps for `X-Queen`, `capabilit`, `serverVersion`, `minVersion`
across all seven return only the user-supplied `x-queen-tenant` routing header. So a new
SDK against an old broker sends `conflation=true`, the broker ignores the unknown query
param, and the consumer silently processes the whole backlog. That is the exact failure the
brief forbids. The fix is response-driven and needs no negotiation protocol:

> **When an SDK sends `conflation=true` and the pop response does NOT carry
> `"conflation":true`, it raises an error on the first such response** (not a warning):
> `"conflation was requested but this broker did not apply it — requires broker >= 1.1.0"`.
> The consume loop stops rather than silently draining a backlog message-by-message.

This works because responses are already forward-compatible by contract — unknown response
keys must not fail a decode, pinned at `crates/queen-protocol/src/pop.rs:418-436` and
`ack.rs:233-244` — and because the broker emits the key on **empty** pops too, so the check
fires on the first round trip, before any message is processed. The mirror case (old SDK,
new broker) is a non-event: no flag sent, group registers with `conflation=false`,
byte-identical behaviour.

Also worth an SDK-side line: `getQueueDepth` has **no C++ implementation at all**
(`grep -rni depth clients/client-cpp/` is empty) and `queenctl queue depth` is undocumented
(`webdoc/.../queenctl.mdx:362-368`). Neither blocks this feature; both are adjacent debt.

---

## 5. Gate / hot-list / depth / webapp touchpoints

### 5.1 Pop pending gate — no change
The 1.0.4 gate probes `last_offset > GREATEST(committed, log_start - 1)`
(`004_log_pop.sql:1330`) which is exactly "a newer message exists". A conflating group's
gate is therefore already correct and already *cheaper* per served message.

### 5.2 Hot-list — no change
See §3.2. Ring entries, `states` verdicts, `drained`, the wheel and the reseed floor all
keep their meaning. `drained` improves.

### 5.3 Depth
`GET /api/v1/resources/queues/:queue/depth` (`main.rs:857`,
`handlers/queues.rs:228-246`) gains `partitionsPending`, `conflation`, `effectivePending`
(§2.5). Presentation rule:

> For a conflating group, `pending` is **log depth** (positions to retire) and
> `effectivePending` is **work depth** (handler invocations remaining). A conflating queue
> at `pending: 4 000 000, effectivePending: 12` is healthy; the same numbers on a
> non-conflating group are an incident.

SDK depth methods gain the fields (they parse into maps/structs today):
`client-py/queen/admin/admin.py:83`, `client-go/admin.go:84`,
`client-js/client-v2/admin/Admin.js:83`, `client-rust/src/admin.rs:89` (note: named
`queue_depth`, not `get_queue_depth`, unlike the other four), `client-laravel/src/Admin.php:58`.
`queenctl queue depth` (`cmd/queue.go:212-260`) replaces its client-side
`partitionsNonEmpty` with the server's `partitionsPending` and adds an `effective` column.

### 5.4 Webapp
**The dashboard never calls `/depth`** — `app/src/api/index.js:33-37` exposes only
`list`/`get`/`delete`, and every pending number comes from `messages.pending` inside the
queue payloads. So the minimum honest change is on the **consumer-group** surface, where
conflation actually lives:

* `app/src/views/Consumers.vue:310-326` — render a `conflation` badge next to the group name
  (the file already special-cases `__QUEUE_MODE__` into a "queue mode" label at `:197`,
  `:292`, `:410`; this is the same shape), fed by the new field from §2.6.
* `app/src/views/Consumers.vue:181-210` (lagging-partition table) and
  `app/src/views/Dashboard.vue:471-508` ("Consumer groups by lag") — for a conflating group,
  label the number **"log lag"** and show `partitionsWithLag` as the primary figure. That
  field already exists (`010_log_admin.sql:447`) and is already the right number.
* `app/src/views/QueueDetail.vue:924-928` — the "High pending depth" advisory must not fire
  on a queue whose only lagging groups conflate.

Deliberately **not** doing: a per-queue depth column in `QueueHealthGrid.vue`. Adding one
means editing five separate `grid-template-columns` track lists (`:278`, `:298`, `:312`,
`:448`, `:457`) plus the header row, for a number that is per-group and not per-queue.

---

## 6. Metrics and logging

Style rules from `obs.rs`: windowed aggregate rates on the two lines people actually open
(`rates`, `sizes`), never a per-message line, new facts ride existing lines rather than
adding a third nobody reads (`obs.rs:378-386`).

### 6.1 `rates` line
`obs.rs:391-398` (`target: "rates", scope = "global"`) gains two fields computed as deltas
over the same window as everything else, from marks taken alongside `prev_kv_ops` et al.
(`obs.rs:305-307`):

```
conflated_s = <messages skipped by conflation per second>
cfl_conflict_s = <declaration conflicts per second>
```

The per-queue top-N block in the same reporter gains `conflated` for a conflating queue.

### 6.2 In-process counters
`metrics.rs`: `QueueCounters.conflated_count` / `.conflation_conflict_count` (`:151`),
`PerQueue::add_conflated` / `::add_conflation_conflict` (`:185-230`), fed from the ack
handler off the SP's `conflated` field (§2.4). **No `tenant` label** on counters — forbidden
at `metrics.rs:387-405`; tenant attribution happens at flush time in `syscollect.rs`.

### 6.3 Prometheus
`queen_queue_conflated_per_minute{queue}` beside `queen_queue_pop_messages_per_minute`,
which requires all three of: a `conflated_count` column on `queen.queue_lag_metrics`
(`019_worker_metrics.sql:95-119`, `ADD COLUMN IF NOT EXISTS`), an entry in the
`per_queue_lag` select (`023_prometheus.sql:55-67`), and a row in the `fams` table
(`handlers/status.rs:198-205`).

**Caution found in passing:** `queen_queue_depth_total` and `queen_queue_depth_pending`
(`handlers/status.rs:244-266`) are **dead** — `status.rs:245` reads a `queue_depth` key that
`get_prometheus_metrics_v1` never builds. They are nonetheless *published* in the generated
docs (`webdoc/src/content/partials/generated/broker-metrics.mdx:54-55`) because
`gen-metrics.mjs` scrapes `# HELP` strings out of `status.rs` and cannot tell live code from
dead. Resolve those two before adding a third family next to them, or the generator will
happily document a new dead metric too.

### 6.4 Traces
No change. `record_trace_v1` (`010_log_admin.sql:815`) is per-message by nature and
conflation does not add an event class.

---

## 7. Test plan

### 7.1 Server-side (`server/tests/`, the `kv_semantics.rs` / `timers_semantics.rs` shape)

New `server/tests/conflation_semantics.rs`:

1. **tail-only delivery** — push 100 to one partition, pop with a conflating group, assert
   exactly 1 message and that it is offset 99; assert `log_consumers.batch_end = 99` and
   `lease_conflated = true`.
2. **cursor jump on ack** — ack it, assert `committed = 99`, `batch_end IS NULL`,
   `lease_conflated = false`, and `conflated = 98` in the ack result.
3. **nack keeps the cursor** — push 100, pop, ack `failed`, assert `committed` unmoved and
   `batch_retry_count = 1`.
4. **supersession does not reset the budget** *(the M2 pin)* — repeat: fail, push 3 more,
   pop again (assert the newly delivered offset differs), fail again. Assert
   `batch_retry_count` increments monotonically and `attempt_count` does too — the latter
   is what §1.4 fixes and what would silently regress.
5. **delayed_processing** — configure `delayedProcessing=5`, push 10, pop: assert **zero**
   delivered (all deferred) **and that `committed` did not move** — the seal must not fire.
   This is the data-loss test for the two-leg LATERAL in §2.3.
6. **delayed_processing, partial** — push 10, wait past the deadline, push 5 more, pop:
   assert the delivered offset is 9 (newest *visible*), not 14.
7. **empty-partition seal still works** — retention removes every segment; assert the seal
   at `004_log_pop.sql:371-378` still advances the cursor for a conflating group.
8. **window_buffer** — a conflating pop on a hot partition delivers nothing and reports
   `leased` with the 250 ms horizon, unchanged.
9. **mode composition** — `all`+conflation over a 1000-message backlog delivers 1;
   `new`+conflation over the same backlog delivers 0 until the next push.
10. **concurrency** — 8 workers, one conflating group, 200 partitions: assert every
    partition is handled by exactly one worker at a time and no two acks of the same
    partition overlap.
11. **boot idempotence** — apply the schema twice (the `kv_timers_boot_idempotence.rs`
    pattern) so the two `ADD COLUMN IF NOT EXISTS` and the three updated `DROP FUNCTION`
    signature lines are proven re-appliable.
12. **transaction wire** — a conflating ack bundled with a push through
    `POST /api/v1/transaction`: assert the bundle commits and the cursor lands on
    `batch_end`, i.e. the 1.0.5 bogus-ack check does not fire (§2.4).

### 7.2 Client suites

The same three tests per SDK — `conflation` reaches the wire from `consume()`, reaches it
from `pop()`, and the degrade-loudly error fires when the response lacks the echo:
`client-js/test-v2/` (register in `test-v2/run.js`), `client-go/tests/`,
`client-rust/tests/http_wire.rs` **plus `crates/queen-protocol/src/pop.rs:476-518`**,
`client-py/tests/` (register in `tests/run_tests.py:199-204`),
`client-laravel/tests/` (**three** builders — assert all three),
`client-cpp/test_client.cpp` (register at `:1653`), `client-cli/tests/`.

**Minimum parity gate** (there is none today): a single JSON fixture listing every consume
option and its wire name, checked into `test/runners/common/`, with each SDK's wire test
asserting its emitted query string against it. Cheap, and it is the only mechanism that
would have caught `maxPartitions` going missing from Python.

### 7.3 End-to-end (`test/runners/`, driven by `test/run.sh`)

**E2E-1 — the guarantee (§1.3).** One partition. Producer pushes `N=1`, consumer's handler
sleeps 2 s; while it sleeps, push `N=2..5`. Assert: (a) the handler ran for `N=1`;
(b) after the ack, `committed = 1` — *not* 5, because 2..5 were not visible at pop time;
(c) a second pop follows without any new push and delivers `N=5`; (d) the handler's last
invocation started strictly after `N=5`'s push timestamp. Then the adversarial variant: push
continuously at 500/s for 10 s, stop, and assert the handler's final invocation carries the
**last** pushed payload. This is the test the whole feature exists to pass.

**E2E-2 — DLQ under a hot producer (the M2 pin, as an end-to-end).** One partition,
`retryLimit=3`, `deadLetterQueue=true`. Handler always fails. Producer pushes at 200/s
throughout. Assert: the partition dead-letters within 4 delivery attempts, the DLQ row's
`retry_count` is 3, `committed` jumps to the poison offset, and the group resumes on the
next tail. Assert the run does **not** livelock — bound it with a wall-clock deadline, since
livelock is the failure mode.

**E2E-3 — mixed groups on one queue.** `workers` conflating, `audit` not. Push 10 000.
Assert `audit` receives all 10 000 and `workers` receives far fewer, with the newest in both.

**E2E-4 — declaration conflict.** Two consumers of one group, one declaring `conflation`,
one not. Assert the stored policy is unchanged, both consumers keep working, the conflict
counter increments, and the disagreeing consumer warns exactly once.

**E2E-5 — old broker.** New SDK against a 1.0.6 image: assert the consume call errors with
the version message and processes zero messages (§4).

### 7.4 Bench (optional, `benchmark-queen/`)
The claim in §1.2 is that a conflating pop is *cheaper* under backlog. Worth one run on the
sparse-partition shape (1000 partitions) comparing pop RTT and PG CPU with the flag on and
off at 100k backlog depth. Not a gate.

---

## 8. Rollout

**Versioning.** Server **1.1.0** (new wire capability, new columns, additive), SDKs
**1.1.0** across the board, `queen-protocol` **1.1.0** — and the protocol crate must ship in
the same release as the broker, because the broker depends on it non-dev
(`server/Cargo.toml:119`) and its conformance tests pin both sides.

**Default off.** `conflation` absent ⇒ `FALSE` on the column ⇒ every code path takes the
existing branch. There is deliberately **no** `QUEEN_CONFLATION_ENABLED` env flag: per the
1.0.3 CHANGELOG position, a boot flag is the claim that a surface is optional, and a cell
where the flag might or might not exist is a cell no client can be written against.

**Order.**
1. Ship the broker (columns + SPs + handler). Old SDKs are unaffected: no flag, no
   registration change, byte-identical responses.
2. Ship the SDKs. Only a consumer that opts in changes behaviour.
3. No coordinated cutover, no migration step for existing groups.

**Compat matrix.**

| | old broker | new broker |
|---|---|---|
| **old SDK** | unchanged | unchanged (no flag sent, `conflation=false`) |
| **new SDK, flag off** | unchanged | unchanged |
| **new SDK, flag on** | **hard error on the first pop** (§4) — never silent | conflates |

**Rollback.** The broker rolls back cleanly: the columns are additive and defaulted, and an
older binary re-applying its own `procedures/*.sql` restores the old SP bodies (last-boot-wins,
`schema.rs:5-13`). The one thing that does **not** roll back is a group already registered
with `conflation=true` — an older broker ignores the column and delivers full batches to
that group. Say so in the release notes: rolling back the broker turns conflation off, it
does not turn it into an error.

**Docs.** Manual: `webdoc/.../reference/http/pop.mdx:45-57` (param table),
`reference/defaults.mdx:153`, all six `reference/sdk/*.mdx` builder tables,
`reference/http/consumer-groups.mdx`, `reference/queenctl.mdx`. Generated (run
`pnpm --dir webdoc gen`, CI fails on drift): `gen-openapi.mjs` picks the field up
automatically from `PopParams` (`webdoc/scripts/gen-openapi.mjs:193-207`);
`gen-snippets.mjs` re-renders whatever the `docs` tests mark. A worked example belongs in
`examples/`, numbered next to `22-subscription-modes.js`.

---

## 9. Explicitly out of scope

* **`seekToTail` as an SDK method.** The server operation already exists — `POST
  /api/v1/consumer-groups/:group/queues/:queue/seek` with `toEnd=true` (`main.rs:936`,
  `010_log_admin.sql:247`) — and `queenctl cg seek` already drives it
  (`clients/client-cli/cmd/cg.go:72`). What is missing is an SDK admin method next to
  `getQueueDepth` (§5.3), which is a separate, trivially-shaped change. Not in this plan.
* **Storage-side compaction / retain-last-N retention.** Conflation touches nothing on
  disk. Retention still governs storage. A "keep only the last N per partition" policy is a
  different feature with a different risk surface (it destroys data; this does not).
* **Per-queue conflation defaults via `/configure`.** Rejected on purpose: per-group is what
  lets `workers` conflate while `audit` reads everything, and `configure_queue_v1` has a
  known column-reset defect.
* **Conflation for streams** (`007_log_streams.sql`) — streams carry their own cursor and
  ack path (`:377`) and their own runner contract.
* **Making `queen.stats.pending_messages` conflation-aware** (§2.5).
* **Per-key conflation *within* a partition.** Conflation here is per-partition by
  definition: one partition = one logical key is the workload's contract, not the broker's.

---

## 10. Open questions for the author

**Q1 — where does the flag live: `consumer_groups_metadata` or a new table?**
*Recommendation: `consumer_groups_metadata`, as §2.1.* It is the row that already holds
`subscription_mode`, it has the right key, and it is already written on first contact. The
one wart is M4 (pinned-pop-only groups have no row), which §2.3 step 2 closes with the same
statement the wildcard path already runs.

**Q2 — refuse `conflation` + `autoAck`, or allow it?**
*Recommendation: refuse with 400.* It works mechanically, but auto-ack commits at delivery
with no lease, so a crashed handler loses the tail and the §1.3 guarantee silently becomes
at-most-once — on a feature whose entire value proposition is "the newest state is
definitely processed". If you want it allowed, it should at least be allowed *loudly*
(response field + counter), not by default.

**Q3 — conflict handling: warn (recommended) or reject?**
*Recommendation: warn, group wins, response echo + counter + rate-limited log (§3.3).*
Rejecting breaks rolling deploys — during a rollout half the fleet sends the flag and half
does not, and a reject would take down the half that is already correct. Warning is strictly
more information than today's silence.

**Q4 — should `take_batch`'s budget estimate become conflation-aware?**
*Recommendation: not in v1; measure first.* For a conflating group the ring's `batch_count`
over-counts (a partition with 500 marks yields 1 message), so `take_batch` stops claiming
early and the pop is narrower than ideal. The result is more pops for the same work —
bounded and never wrong. If it shows up in E2E-3, the fix is one line: treat each ready
entry as contributing 1 to `got` when the group conflates (`hotlist.rs:1480`).

**Q5 — should `partitions` be auto-raised for conflating groups (M5)?**
*Recommendation: yes, `max_parts = partitions ?? batch`, clamped to 64 (§3.2).* Otherwise
the default `partitions=1` makes a conflating consumer do one message per round trip, which
will read as "conflation is slow". Document the 64 ceiling as a property of the feature: a
conflating pop returns at most 64 messages, and `batch` above that is inert.

**Q6 — is `total_consumed` allowed to keep counting skipped positions?**
*Recommendation: yes, unchanged.* It means "log positions retired", which is what the lag
and `Dead`/`Stable` derivations want (`010_log_admin.sql:452`). The new `conflated` counter
is the second, honest number. Changing `total_consumed` would silently rewrite the meaning
of an existing dashboard field.

**Q7 — the conflated-completion clamp (§2.4 item 1): needed, or over-engineering?**
*Recommendation: keep it.* It is four lines and it removes a whole hazard class — a repeated
`transactionId` below the tail (possible whenever `dedupWindowSeconds=0`) would otherwise
resolve `MIN`-in-span to the *lower* occurrence, leave the cursor short of the delivered
offset, hold the lease open and re-deliver the same tail for ever. Cheap insurance against a
livelock that only appears on dedup-off queues, i.e. in production and not in tests.

**Q8 — surface `subscriptionMode` in `get_consumer_groups_v4` as part of this work (M7)?**
*Recommendation: yes.* It is a two-line join, it closes a hole the docs already admit to
(`consumer-groups.mdx:76`), and without it conflation has no console surface either —
shipping a group-level policy that the group view cannot display repeats the mistake.

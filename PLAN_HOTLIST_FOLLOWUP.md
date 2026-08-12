# Windowed hot-list reseed — what is still owed

Follow-up to `c1e7efb` + `ad183c4` (branch `hotlistfix`, shipped to production as
`1.0.1-beta.1`). Written 2026-08-11, right after the deploy, from an adversarial review
that raised 28 findings across six lenses; each was handed to an independent refuter and
the ones below are what survived, minus the two already fixed.

Nothing here loses a message. Everything here either delays a *repair* or wastes work.
That distinction is the reason the deploy went ahead — but it is also why this list needs
to be worked, because a repair path nobody exercises is a repair path nobody trusts.

## Status, 2026-08-12

Every code item below is implemented in the working tree of `hotlistfix` (not yet
committed at the time of writing). Each section carries a **Status** line saying what
actually shipped and where it departed from what was planned, because some of it did: A3's
watermark does not work, and E2's mechanism was not the one the reviewer named.

| | Item | Status |
| --- | --- | --- |
| A1 | consumer-group delete forces a full walk | done |
| A2 | per-partition seek stops walking the queue | done |
| A3 | seek repair gets a durable backup | done, design corrected (no watermark) |
| A4 | the seek's own walk reports its failure | done |
| B1 | one absolute cutoff per windowed walk | done |
| B2 | a ring pinned to Full says so | done |
| B3 | reseed identity ticket | done |
| C1 | the window is clamped against the cadence | done |
| C2 | the kill switch covers the repair paths | done |
| C3 | state the new repair latency in the release notes | **still owed** |
| D1 | de-phasing scaled to the interval | done |
| E1 | the embedded HA contract | done |
| E2 | the deferral-queue leak | **CONFIRMED**, reproduced, fixed; one trade for Alice |
| F1 | per-mode counters and ring ages | done |
| — | group-scoped mesh hint (the shared prerequisite) | done, wire-compatible |

Two test gaps are known and left open on purpose; both are named at the bottom.

## What shipped, for context

The reseed used to walk every partition of a queue, once per ring per 30s per broker:
49ms to return zero rows, 8.2 calls/s, 0.58 cores, the largest single consumer of the
production database. It now walks only the partitions written in the last
`QUEEN_HOTLIST_RESEED_WINDOW_MS` (default 120s), and does the full walk every
`QUEEN_HOTLIST_RESEED_FULL_MS` (default 300s, `0` pins everything to full).

The soundness argument: a partition can only *become* pending by being written, and every
push bumps `last_write_at`. Ack and retention only ever remove pendingness. **Every item
in section A is an exception to that sentence.** They were known for the seek and handled
there; the review found the sentence has more exceptions than the patch accounts for.

Already fixed before the deploy: the generic-plan `Sort` that made the walk proportional
to the window instead of to the page (`ad183c4`), and the generated config reference
publishing `0` as the window default.

---

## A. The exceptions to "only a push creates pendingness"

This is the cluster that matters. Together they are one shape: something moves a cursor
backwards, no row is written, so the windowed walk is blind to it by construction, and the
only recovery is the full walk — which is now 10x rarer than it was.

### A1. Consumer-group delete gets no forced full walk — *critical*

**Status: done.** `repair_after_group_delete` (`consumer_groups.rs`), wired into both
delete handlers. `forget_group_all_queues` now returns the qkeys whose ring it dropped,
so the walk hits exactly those queues; the SQL publishes an A3 marker for the peers'
queues this broker does not poll.

`server/src/handlers/consumer_groups.rs:134` (`handle_delete_consumer_group`,
`handle_delete_consumer_group_for_queue`).

Deleting a group removes its `queen.log_consumers` rows. `COALESCE(c.committed, -1)` then
reads `-1` for every partition, so every partition holding data is pending again for that
group name. No write happens, so no `last_write_at` moves.

Locally this is survivable: the delete drops the ring, and the next pop cold-starts, which
is a full walk. **The peers are the hole** — they still hold a ring for that group name
with `full_reseed_ms` stamped, so they keep running windowed passes and stay blind for up
to `hotlist_reseed_full_ms`. Before this patch they healed within 30s because everyone
walked everything that often.

Fix: treat it exactly like the seek — force a full walk locally and broadcast, in both
delete handlers. Same call, `hotlist_reseed_full_broadcast`.

### A2. Per-partition seek walks the whole queue — *major*

**Status: done.** `repair_after_partition_seek`: one `mark_local_group`, one
group-scoped hint, no SQL at all. The marker it publishes names the partition, so a peer
applying it marks that partition rather than owing a full walk.

`server/src/handlers/consumer_groups.rs:381` (`handle_seek_partition`).

A seek scoped to ONE partition triggers `reseed_after_seek`, which runs a full-queue walk
and broadcasts the group's entire pending set. Correct, wildly disproportionate: the
operator moved one cursor and paid a 9,563-partition walk plus a fan-out.

Fix: a targeted path for the single-partition case — mark that one partition locally and
emit one hint — instead of reusing the queue-wide walk.

### A3. Seek repair rides a lossy channel with no backup — *critical*

`server/src/handlers/consumer_groups.rs:324`, `server/src/mesh.rs:226-232`.

The mesh drops frames by design when a peer queue is full or the peer is down
(`PEER_QUEUE = 1024`). That was always true, but the *cost* of a drop changed: the
windowed floor no longer re-discovers the partition, so a dropped seek hint now stalls a
replay for `hotlist_reseed_full_ms` (300s + jitter) instead of one 30s floor.

Fix: the mesh is best-effort and should stay that way, so the backup has to be local to
each peer. Cheapest sound option is a per-(queue, group) "full walk owed" flag that a peer
sets when it receives a hint it cannot attribute, and that `reseed_mode` honours. Consider
instead making the seek write a marker the peers can read — it is the only repair whose
correctness people will actually notice.

**Status: done, and the marker was the option taken** — `queen.hotlist_repairs`
(`001_log_schema.sql`) + `queen.hotlist_repair_publish_v1` (`010_log_admin.sql`), written
by both seeks and both group deletes inside their own transaction, read by
`reconcile::apply_hotlist_repairs` on the reconcile pass and applied through
`mark_remote_group` (partition-scoped rows) or `request_full_walk` (queue-wide rows).
Pruned inline after an hour.

Two departures from the sketch, both load-bearing:

* **The row carries a `partition_name`**, which the design did not have. Without it A2's
  saving is handed straight back: a per-partition seek would announce itself as a
  queue-wide repair and every peer would run the full walk 60s later. Two repairs naming
  different partitions widen to `NULL` rather than one being lost.
* **The reader change-detects; it does NOT keep a watermark.** `now()` is
  transaction_timestamp, so a long group delete commits a row dated *before* one a later,
  faster seek already advanced a watermark past, and that repair is then skipped forever.
  The usual 1s slack inverts the failure rather than fixing it: a row sitting at the
  watermark is re-read every pass, so one seek buys a full walk per ring per minute for
  the whole prune hour. The reader instead holds a mirror of the (PK- and prune-bounded)
  table and acts on any change in `(repair_at, partition)`. Four tests in `reconcile.rs`
  pin it, including the out-of-commit-order case.

One defect this turned up, found by running the reader's query under
`force_generic_plan` rather than a plain EXPLAIN: `ORDER BY repair_at` bound to the
`repair_at::text` output column and sorted a timestamp by its text rendering, with a Sort
node. Aliased to `repair_at_text`; the plan is now an index scan the LIMIT terminates.

### A4. Failure of the seek's own walk is invisible — *major*

**Status: done.** `hotlist_reseed_run` returns a `ReseedOutcome`; the full-walk wrapper
WARNs with tenant/queue/group on a failure and on a dropped stamp; `note_partial_repair`
adds `hotlistRepaired: false` plus a `warning` string to the seek's 200 body. Deliberately
no `error` key: `sp_result_to_response` would turn a committed seek into a 500. A healthy
seek's body is byte-identical to before.

`server/src/handlers/data.rs:1893`.

`hotlist_reseed_run` swallows the error and returns; `reseed_after_seek` ignores the
outcome; the seek endpoint returns HTTP 200. An operator who clicks "replay from
yesterday" gets a success with no replay, and no log line says otherwise.

Fix: propagate the walk's outcome, log it at WARN with the queue/group, and reflect a
partial repair in the seek response.

---

## B. Edges of the windowed walk itself

### B1. `now()` is re-evaluated per page — *major*

**Status: done.** `log_hotlist_reseed_window_v1` gained `p_cutoff TIMESTAMPTZ DEFAULT
NULL` (next to `p_window_ms`, the half it pins) and returns `r_cutoff`: `NULL` means
"first page, derive it and hand it back", every later page echoes it, exactly like the
`(r_write, r_id)` cursor beside it. DROP+CREATE with three DROPs, the third being
**1.0.1-beta.1's own 7-arg shape** — left in place it becomes a second candidate for the
6-argument call and every such call fails 42725. Verified by simulating the upgrade.
Generic plan re-checked under `force_generic_plan` for both a NULL and a pinned cutoff:
unchanged shape, no Sort of the window. The PG test seeds four partitions against the old
edge of a 5s window and stalls 1.5s mid-walk; pinned keeps all four, the beta.1 control
keeps one.

`server/sql/procedures/004_log_pop.sql:2075`, driven from `server/src/handlers/data.rs:1898`.

Each page is its own SQL call, so each page recomputes `now() - window`. The lower bound
therefore creeps forward between pages while the keyset cursor climbs from the oldest row.
A partition whose `last_write_at` falls into the few milliseconds between the cursor and
the new lower bound is skipped by that pass.

Bounded, not lost: every reseed restarts its cursor at `-infinity`, and the window (120s)
is four times the cadence (30s), so the skipped band is re-covered by the next pass. It
also only arises above 10,000 rows in one window, which production is nowhere near.

Fix: compute the cutoff once per walk and pass it as an absolute timestamp, the same way
the cursor is already an opaque echo. Turns a reasoned-about race into an invariant.

### B2. A failing full walk pins the ring to Full forever — *major*

**Status: done, and no fallback was added.** A per-ring `reseed_fail_streak` warns at 3
consecutive failures and then only on doublings: a database refusing this query refuses it
for all 63 rings, and one line per ring per 30s buries its own cause. The `reseed floor`
line also carries `failed_delta` and `full_overdue`, and an overdue ring prints its `ring`
line even when empty (F1).

`server/src/handlers/data.rs:1938`.

`full_reseed_ms` only advances on success, so a full walk that keeps failing keeps the
ring in Full mode, retrying the expensive query every interval and never once running the
cheap one. This is the conservative direction, and it matches pre-patch behaviour exactly,
so it is not a regression — but it is a silent one: nothing logs that a ring has been
failing its full walk for an hour.

Fix: a WARN with a counter after N consecutive failures. Do not "fall back to windowed" —
that would trade a loud stall for a quiet blindness.

### B3. `forget_group` has no identity guard against an in-flight reseed — *major*

**Status: done.** `ReseedTicket` carries the `QueueState` and `GroupRing` Arcs the walk
started on; `reseed_finish` stamps only when `Arc::ptr_eq` still matches the map, and
counts the drop otherwise. `reseed_row` writes through the ticket too, which removes the
per-row by-name lookups (9,563 rows x 2 mutex acquisitions on production's full walk).
`reseed_mode`/`reseed_done` are gone: no by-name stamping path survives, including in
tests.

`server/src/hotlist.rs:1708` (`forget_group`, `forget_group_all_queues`),
`server/src/handlers/data.rs:1938`.

The walk re-resolves the ring by name on every row and again at `reseed_done`. If the group
is forgotten and recreated mid-walk, the stamp lands on the *new* ring, which then believes
it has just been full-walked and skips its cold-start full walk for up to 5 minutes. Unlike
`evict_idle`, which is protected by `Arc::strong_count == 1`, these two have no guard.

Fix: have `reseed_mode` return an opaque ticket carrying the ring identity it started on,
and have `reseed_done` verify it before stamping. Drop the stamp on mismatch.

---

## C. Configuration that can be set to something unsound

### C1. The window is never validated against the cadence — *minor, but a foot-gun*

**Status: done.** `resolve_reseed_window_ms` clamps the derived *and* the explicit value
into `[reseed_ms + max_reseed_jitter_ms(reseed_ms), 7 days]`, with the ceiling winning if
a very long cadence puts the two in conflict (no reversed-`clamp` panic, `saturating_*`
throughout). Only an operator's value warns, in two distinct `boot` messages naming the
variable, the requested value and the value in force. The floor calls into
`hotlist::max_reseed_jitter_ms` rather than copying `RESEED_JITTER_DIV`, because after D1
a config-side copy would silently drift.

`server/src/config.rs:846-858`.

The derivation (`max(4x reseed_ms, 120s)`) only runs when the knob is `0`. An operator who
sets `QUEEN_HOTLIST_RESEED_WINDOW_MS` explicitly can set it *below* the reseed interval,
which opens a permanent blind band between consecutive passes — the exact invariant the
window exists to preserve. A very large value pushes `make_interval` toward "timestamp out
of range".

Fix: clamp to `[reseed_ms + RESEED_JITTER_MS, some sane ceiling]` whether derived or
explicit, and log at WARN when the operator's value is raised.

### C2. The kill switch does not cover the seek path — *major*

**Status: done, gated rather than given its own knob.** `repair_broadcasts(st)` is
`hotlist_reseed_full_ms > 0`: under the switch the repair walks stop fanning out, the
group delete's repair does not run at all (it is entirely post-windowing), and the marker
poll is skipped — every peer walks everything every 30s again, which is what it used to
do. A per-partition seek still sends its single hint: that is a push-shaped event, not the
whole-pending-set fan-out. The markers are still WRITTEN, so a mixed cluster keeps
working. The claim in `config.rs` was rewritten to say all of this, and so were the two
places in `embedded/` that repeated the old version of it.

`server/src/handlers/consumer_groups.rs:324`.

`QUEEN_HOTLIST_RESEED_FULL_MS=0` is documented as "exact pre-windowing behaviour", and for
the reseed it is. The seek path still broadcasts, which pre-patch it never did. So the one
lever an operator pulls when something looks wrong does not actually restore the old
behaviour.

Fix: gate the broadcast on the same switch, or give it its own and correct the claim in
the comment and in the docs.

### C3. The new default is a real change in repair latency — *decide deliberately*

**Status: STILL OWED.** Nothing in this round writes a release note; the numbers below are
the ones to state, and D1 fixed them deliberately so the headline figure would not move.

`server/src/config.rs:843`.

Worst-case ring repair goes from ~45s (30s floor + 15s jitter) to ~360s (300s + jitter) for
an operator who upgrades and changes nothing. That is the trade the patch exists to make,
and at the measured numbers it is the right one — but it should be a stated number in the
release notes rather than something discovered.

What the follow-up changed about that sentence, and what the note therefore has to say:

* **360s is now the number for ONE class only** — pendingness no write explains and no
  operator announced (an entry cleared in error, a stranded INFLIGHT, a stale WHEEL park,
  a dropped hint for a push). D1 makes the jitter a fifth of the interval, so the worst
  case is 300s + 60s: same headline, spread instead of bunched.
* **A backward seek or a group delete is no longer in that class.** It repairs the serving
  broker immediately, the peers over the mesh in ~20ms, and, if that frame is dropped, from
  the durable marker within one reconcile interval (60s). So the number an operator
  actually feels after clicking "replay from yesterday" is ~60s worst case, not 360s.
* **Embedded is the exception and E1 now says so**: no mesh, so the marker poll (60s, plus
  a 5s settle after start) IS the fast path there, and the operations that publish markers
  are not on the embedded surface at all.

---

## D. Load shape

### D1. Full walks fire in lockstep — *major*

**Status: done.** `GroupRing::jitter_ms(interval_ms)` scales the span to
`max(interval/5, RESEED_JITTER_MS)`: the 30s cadence keeps exactly its old
`[0, 15_000)` band, the 300s full walk spreads over 60s. Monotone in the interval, so no
configuration ends up de-phased less than before, and the release-note worst case stays
300s + 60s. The span is also half of C1's floor, which is why `max_reseed_jitter_ms` is
public.

`server/src/hotlist.rs:1397`.

`reseed_jitter_ms` is drawn from `RESEED_JITTER_MS = 15_000`, sized for a 30s cadence, and
is now also the de-phasing for a 300s one. After a broker restart every ring cold-starts
together and then comes due together, so the full walks bunch into a 15s window every 5
minutes instead of spreading. The average is still ~10x below pre-patch; the peak is not.

Fix: scale the jitter to the interval it de-phases — a fraction of `full_interval_ms`, not
a constant.

---

## E. Statements in the tree that are now false

### E1. Embedded HA still promises a 30s floor — *minor*

**Status: done.** `server/src/embedded/mod.rs:76`. The doc comment promises a 30s
cross-instance floor that no longer exists, and the embedded engine has no mesh, so it
cannot receive the seek hint that compensates. Two embedded instances on one database are
strictly worse off than two brokers. Correct the contract, and say plainly that embedded
HA relies on the full walk.

The contract now separates the channels instead of quoting one number: a parked pop's own
backoff (≤ 1s), the reconcile interval for config (60s), and the WINDOWED reseed
(`QUEEN_HOTLIST_RESEED_MS` plus that ring's de-phasing offset) for another instance's
PUSH, which is all the windowed pass can ever carry. The cursor-move case is stated as the
one place an embedded fleet is genuinely worse off than a broker fleet: no frame to
receive, so it waits for the A3 marker on the reconcile cadence, after a 5s settle at
boot. It also names which side of that this surface is on — seek and consumer-group delete
are not exposed embedded, so an embedded instance is only ever a marker READER and the
publisher is an HTTP broker sharing the database — and gives the two levers
(`QUEEN_CACHE_REFRESH_INTERVAL_MS` to buy the latency back,
`QUEEN_HOTLIST_RESEED_FULL_MS=0` to trade database CPU for a 30s floor on everything).
`boot.rs` needed no new config: the C1 clamp happens inside `config::load()`, no new env
knob exists, `BOOT_BOOL_KEYS` still matches every `env_bool` call site in `config.rs`, and
the only boot step main.rs gained this round was observability. The A3 marker poll was
already wired into the inlined reconcile loop.

The reader-facing halves are `webdoc/src/content/docs/use/embed.mdx` (a caution stating
the same thing in the operator's language) and `reference/engine.mdx`, which already
pointed at it.

### E2. The deferral-queue interaction was raised and needs a decision — *verify first*

**Status: CONFIRMED, reproduced, fixed. One behavioural trade for Alice, below.**

`server/src/hotlist.rs:1412`. One reviewer argued that on a queue with
`delayedProcessing`/`windowBuffer`, broadcast ghost entries land in the wheel and never
clear, keeping the ring permanently non-idle so `evict_idle` can never reclaim it. The
refuter did not overturn it, but nobody reproduced it either. **Reproduce it before
believing it**, then fix or close it — a leak that only appears on deferral queues in HA is
exactly the kind that hides for months.

**The verdict.** The claim holds, the mechanism named in it is wrong in one detail, and
the scope is much wider than "deferral queues in HA after a seek".

The ghosts do not get stuck in the wheel: the wheel tick drains them correctly, which is
why production showed wheel 0-34 with no trend and flat RSS. The one-way door is
`checkin`'s `Verdict::Empty` arm, which on a deferral queue re-wheeled *unconditionally*,
so an entry cycled `READY → INFLIGHT → WHEEL → READY` at the revisit floor forever. On a
deferral queue there was no path back to `IDLE` at all (the only other clear is
`promote_ack`'s covered-ack clear, which needs an ack a ghost never produces), so
`evict_idle`'s "every entry IDLE" never held and the whole `QueueState` was pinned for the
process lifetime. The leak lives in the ready list plus the `queues` map, bounded by
*partitions x rings ever touched*, so it plateaus rather than trending; the standing cost
is the empty-claim loop, which also defeats long-poll parking on those queues (`has_ready`
is true again within 50ms, forever).

Measured on a simulated clock, pre-fix:

| shape | empty claims per 60 simulated s | ring reclaimed after 20 sweeps |
| --- | --- | --- |
| windowBuffer 5s, ghost from a seek broadcast | 1,100 (~18/s = the 50ms revisit floor) | no |
| windowBuffer 5s, one ordinary push, drained | 1,098 | no |
| delayed 3s | 57 (~1/s = the 1s clamp) | no |
| plain queue (control) | 1, then IDLE | yes |

It needs no seek, no mesh and no HA: one ordinary push to a `windowBuffer` queue,
delivered and drained, arms it. The seek broadcast is only the amplifier, and before the
group-scoped hint it armed every partition of the queue on every peer ring at once (9,563
in production).

**The fix.** A `pending_since` per slot, written by the only two things that put an entry
in a ring (`mark_inner` and `reseed_row`), and a `ghost()` predicate: the ambiguity of an
empty claim on a deferral queue has a deadline, and it is exactly the cut, so past
`pending_since + max(delayed, window) + 2*pad + REVISIT_MAX_MS` with no mark since (the
existing epoch CAS), the entry hard-clears like a plain queue's. Continuous writes keep
sliding `pending_since`, so a partition under an unquiet windowBuffer is never a ghost;
the grace guarantees at least one confirming revisit on either queue shape, so the broker
must be ~1.6s out from PG before a skewed probe can clear anything. Post-fix the same rows
read 33 / 1 / 3 claims and the ring is reclaimed in every case. No mesh frame, no SQL, no
index, no signature changed.

**The trade to decide.** A deferral-queue partition that becomes visible with *no new
write* — a nack whose retryDelay elapsed, a lease released without an ack — is now
recovered by the reseed floor instead of by the 50ms revisit loop. That is exactly the
exposure plain queues already ship with, and post-windowing that recovery is the FULL walk
when the push is older than the window, which makes E2 partly a customer of A1/B-class
repair latency. If that is not acceptable, the alternative is a long-but-finite ladder
(clear after N cuts rather than one), which keeps the leak bounded without matching
plain-queue latency.

---

---

## F. You cannot see any of this from the logs

### F1. Full and windowed passes are indistinguishable — *do this first*

**Status: done.** `reseeds_full` / `reseeds_window` / `reseeds_failed` / `reseeds_dropped`
replace the single counter; `reseeds_delta` and `per_s` keep their old names and their old
meaning (the sum), so existing greps survive. The `reseed floor` line gains `full_delta`,
`window_delta`, `full_per_s`, `failed_delta`, `dropped_delta` and `full_overdue`; each
`ring` line gains `next` (the mode of its NEXT walk, the answer rather than the
arithmetic), `full_age_ms` and `reseed_age_ms` (-1 = never). The print filter is now
`ready+wheel > 0 || stale`, because a ring pinned to Full by a failing walk is empty and
busiest-first hid it exactly when it mattered.

`server/src/hotlist.rs:1615` (the single `reseeds` counter, bumped by `reseed_done` for
both modes), `server/src/main.rs:474-492` (the `reseed floor` line).

There is not one `tracing` call anywhere in the reseed walk, and `reseed_done` increments
one counter for both modes. So the 30s `reseed floor` line reports `reseeds_delta` and
`per_s` with full and windowed summed together, and nothing anywhere records when a ring
last completed a full walk. An operator cannot answer "is this ring windowed or full", and
cannot answer "when was this ring last repaired".

Asked on the night of the deploy, within minutes of it going out, and the only available
answer was to infer the split from Query Insights — because the two modes are separate SQL
functions, so the database can distinguish what the broker cannot.

This is first not because it is the worst but because it is the prerequisite: **B2 is
invisible by construction without it** (a ring whose full walk keeps failing is pinned to
Full forever and nothing says so), and every other item here is diagnosed by reading these
same lines.

Fix: split the counter (`reseeds_full` / `reseeds_window`), add both to the `reseed floor`
line, and put each ring's age-since-last-full-walk on the per-ring lines that already print
`ready` and `wheel`. Half an hour, and it makes the rest of this list observable.

---

## Suggested order

*Worked in this order on 2026-08-12, and it held.*

1. **F1** (per-mode counters) — the prerequisite; everything below is diagnosed from those lines.
2. **A1** (consumer-group delete) — same shape as the seek, already-written fix to copy.
3. **B3** (`forget_group` identity) — small, self-contained, removes a silent skip.
4. **C1 + C2** (config clamps, kill-switch coverage) — cheap, and they make the knobs honest.
5. **B1** (absolute cutoff) — turns the one remaining race into an invariant.
6. **D1** (jitter scaling) — one line, removes the periodic burst.
7. **A3** (seek durability) — the design question; needs a decision, not just a patch.
8. **A2, A4, B2, E1, E2** — the tail.

The group-scoped mesh hint (carrying `group` as an optional JSON field on the existing
`T_HOTLIST_DIRTY_BATCH`, which is wire-compatible: an older peer ignores it and does
today's over-mark) is the shared prerequisite for A1 and A2 being cheap. It was deliberately
deferred on the night of the deploy rather than touching the wire format at 22:00.
**Done**: `DirtyHint` carries `Option<group>`, the frame emits `"group"` only when it is
`Some` (so a push frame is byte-identical to 1.0.1-beta.1's), and the receive path reads it
under the same degradation rule as `frame_tenant`. Tag 5 is unchanged, which is what makes
a rolling upgrade safe.

---

## What is left

1. **C3** — the release note. The numbers are in the C3 section above; nothing else about
   it is open.
2. **The E2 trade** — Alice's call between the deadline clear as implemented and the
   finite ladder. Doing nothing keeps what is in the tree, which is the plain-queue
   exposure.
3. **Two test gaps, both deliberate.**
   * The two-embedded-broker end-to-end that the A3 design sketched as "free": the
     reconcile loop's fixed 5s boot delay plus a full reconcile interval makes it a slow
     test, and the halves it would cover are pinned separately (publication in
     `server/tests/hotlist_repairs.rs`, application in the `reconcile.rs` unit tests). It
     is the seam itself that is uncovered.
   * Nothing exercises a peer running 1.0.1-beta.1 against a broker sending grouped
     hints. The degradation is argued (an unknown JSON field is ignored, the queue-wide
     mark is a superset) and unit-tested on the receive side, not run against a real old
     binary.
4. **Commit and roll out.** At the time of writing all of the above is in the working tree
   of `hotlistfix`, uncommitted, on top of a production still running `1.0.1-beta.1`.
   Note for whoever cuts it: the SQL changes are additive (a new table, a new function, a
   `DROP`+`CREATE` that also drops beta.1's shape), so a broker of either version can run
   against the applied schema, and the mesh frame is compatible in both directions.

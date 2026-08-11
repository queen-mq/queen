# Windowed hot-list reseed — what is still owed

Follow-up to `c1e7efb` + `ad183c4` (branch `hotlistfix`, shipped to production as
`1.0.1-beta.1`). Written 2026-08-11, right after the deploy, from an adversarial review
that raised 28 findings across six lenses; each was handed to an independent refuter and
the ones below are what survived, minus the two already fixed.

Nothing here loses a message. Everything here either delays a *repair* or wastes work.
That distinction is the reason the deploy went ahead — but it is also why this list needs
to be worked, because a repair path nobody exercises is a repair path nobody trusts.

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

### A4. Failure of the seek's own walk is invisible — *major*

`server/src/handlers/data.rs:1893`.

`hotlist_reseed_run` swallows the error and returns; `reseed_after_seek` ignores the
outcome; the seek endpoint returns HTTP 200. An operator who clicks "replay from
yesterday" gets a success with no replay, and no log line says otherwise.

Fix: propagate the walk's outcome, log it at WARN with the queue/group, and reflect a
partial repair in the seek response.

---

## B. Edges of the windowed walk itself

### B1. `now()` is re-evaluated per page — *major*

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

`server/src/handlers/data.rs:1938`.

`full_reseed_ms` only advances on success, so a full walk that keeps failing keeps the
ring in Full mode, retrying the expensive query every interval and never once running the
cheap one. This is the conservative direction, and it matches pre-patch behaviour exactly,
so it is not a regression — but it is a silent one: nothing logs that a ring has been
failing its full walk for an hour.

Fix: a WARN with a counter after N consecutive failures. Do not "fall back to windowed" —
that would trade a loud stall for a quiet blindness.

### B3. `forget_group` has no identity guard against an in-flight reseed — *major*

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

`server/src/config.rs:846-858`.

The derivation (`max(4x reseed_ms, 120s)`) only runs when the knob is `0`. An operator who
sets `QUEEN_HOTLIST_RESEED_WINDOW_MS` explicitly can set it *below* the reseed interval,
which opens a permanent blind band between consecutive passes — the exact invariant the
window exists to preserve. A very large value pushes `make_interval` toward "timestamp out
of range".

Fix: clamp to `[reseed_ms + RESEED_JITTER_MS, some sane ceiling]` whether derived or
explicit, and log at WARN when the operator's value is raised.

### C2. The kill switch does not cover the seek path — *major*

`server/src/handlers/consumer_groups.rs:324`.

`QUEEN_HOTLIST_RESEED_FULL_MS=0` is documented as "exact pre-windowing behaviour", and for
the reseed it is. The seek path still broadcasts, which pre-patch it never did. So the one
lever an operator pulls when something looks wrong does not actually restore the old
behaviour.

Fix: gate the broadcast on the same switch, or give it its own and correct the claim in
the comment and in the docs.

### C3. The new default is a real change in repair latency — *decide deliberately*

`server/src/config.rs:843`.

Worst-case ring repair goes from ~45s (30s floor + 15s jitter) to ~360s (300s + jitter) for
an operator who upgrades and changes nothing. That is the trade the patch exists to make,
and at the measured numbers it is the right one — but it should be a stated number in the
release notes rather than something discovered.

---

## D. Load shape

### D1. Full walks fire in lockstep — *major*

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

`server/src/embedded/mod.rs:76`. The doc comment promises a 30s cross-instance floor that
no longer exists, and the embedded engine has no mesh, so it cannot receive the seek hint
that compensates. Two embedded instances on one database are strictly worse off than two
brokers. Correct the contract, and say plainly that embedded HA relies on the full walk.

### E2. The deferral-queue interaction was raised and needs a decision — *verify first*

`server/src/hotlist.rs:1412`. One reviewer argued that on a queue with
`delayedProcessing`/`windowBuffer`, broadcast ghost entries land in the wheel and never
clear, keeping the ring permanently non-idle so `evict_idle` can never reclaim it. The
refuter did not overturn it, but nobody reproduced it either. **Reproduce it before
believing it**, then fix or close it — a leak that only appears on deferral queues in HA is
exactly the kind that hides for months.

---

## Suggested order

1. **A1** (consumer-group delete) — same shape as the seek, already-written fix to copy.
2. **B3** (`forget_group` identity) — small, self-contained, removes a silent skip.
3. **C1 + C2** (config clamps, kill-switch coverage) — cheap, and they make the knobs honest.
4. **B1** (absolute cutoff) — turns the one remaining race into an invariant.
5. **D1** (jitter scaling) — one line, removes the periodic burst.
6. **A3** (seek durability) — the design question; needs a decision, not just a patch.
7. **A2, A4, B2, E1, E2** — the tail.

The group-scoped mesh hint (carrying `group` as an optional JSON field on the existing
`T_HOTLIST_DIRTY_BATCH`, which is wire-compatible: an older peer ignores it and does
today's over-mark) is the shared prerequisite for A1 and A2 being cheap. It was deliberately
deferred on the night of the deploy rather than touching the wire format at 22:00.

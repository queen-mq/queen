# Phase-1 postgres ↔ raft parity gaps (difffuzz v1 findings)

WP-1.10 ran the differential fuzzer of §13.4 against a **postgres** broker (side
A, the oracle, D22) and a **raft1** broker (side B, `QUEEN_STORAGE=raft`, no
Postgres) over the phase-1 message path: push (with duplicates), pinned pop
(manual and auto ack), wildcard and discovery pop, ack ok/failed/dlq by hash and
by position, batch ack (positional and mixed), nack, renew, below-cursor re-ack.

With the gaps below **absorbed** (each at its site in `compare.go` / `run.go`,
with the reason), thousands of seeded 250-operation sequences run with **no
divergence** in the message-path RESPONSES, and the per-side `checker`
(at-least-once, payload-hash) passes on **both** brokers. So the phase-1 raft
message-path SEMANTICS match the oracle; what differs is a handful of documented
RESPONSE-ENVELOPE details, each already deferred by WP-1.7c or R-101.

Every gap was verified by DISABLING its absorption and reproducing the divergence
on a real broker pair. Classification per the WP-1.10 rubric: **facade parity**
(raft should match, a WP-1.7/WP-2.6 fix), **oracle quirk** (postgres is the odd
one, raft is correct), or **codec/R-101** (a deferred AckResult-shape limitation).

| # | where | postgres | raft | class | verdict |
|---|---|---|---|---|---|
| 1 | empty pop | bodiless **204** (`data.rs pop_status`, a firm SDK contract) | **200** with `messages:[]` (`facade/real.rs render_claims` always 200; the 204 mapping would live in `handlers/raft.rs`, outside `rsm/`) | facade parity | raft should return 204 for an empty non-conflating pop. Absorbed narrowly (204-bodiless ⟺ 200-empty-pop); a delivered-vs-empty pop still diverges. **WP-1.7/2.6.** |
| 2 | `partitionId` | a **uuid** | the **numeric pid** | facade parity (documented WP-1.7c deferral: "partitionId is the numeric pid not a uuid; no uuid→pid index yet") | normalized by identity RELATION (`normalize.go IdKeys`); the partition IDENTITY is still checked via the `partition` NAME. **WP-2.x** adds uuid partitionIds. |
| 3 | ack result `noop` | present on every item (`false`/`true`) | **absent** (`render_ack` does not emit it) | codec/R-101 | ignored in the ack compare; a report annotation, not state. The no-op OUTCOME is checked via `success`. |
| 4 | ack result `error` | an explanatory string on a stale/unresolvable/expired-lease ack ("already committed…", "invalid or expired lease", "unresolvable…") | **null** always (the AckResult codec has NO per-item error field — R-101, the WP-1.7c design call) | codec/R-101 | absorbed only in the pg-string / raft-null direction; a raft-produced error that postgres does not match is still a divergence. The OUTCOME is checked via `success`/`dlq`. |
| 5 | ack result `success` on a below-cursor COMPLETED ack | `true` + `noop:true` | `false` (reads the hash as stale) | codec/R-101 | absorbed ONLY when postgres flags `noop:true`; both change no state. |
| 6 | ack result `leaseReleased` | flags the **head** item of a batch, and reports `true` after a partial single ack though the rest stays leased (verified: a fresh pop returns EMPTY on both) | flags the item that **actually releases** the lease, `false` on a partial ack | facade parity / oracle over-report | not compared per item; the lease STATE it describes is validated by the pinned-pop differential (a lease wrongly released re-pops its messages and diverges the next pop). **finding: leaseReleased attribution.** |
| 7 | mixed BATCH ack `dlq` on a **completed** sibling of a dlq'd item | `true` — postgres BROADCASTS the target's dlq count to every item of the (partition,lease) target | `false` — PER-ITEM attribution | **oracle quirk; raft is CORRECT** | this is exactly the mis-attribution the WP-1.7c seam fixed in raft (`res.dlq>0 && status∈{Dlq,Failed}`). Absorbed only in the direction pg=true/raft=false on a completed item; a signal item still must match. |

Nondeterminism that is EXPECTED, not a divergence (§5.2: the wildcard partition
choice is planner-random): wildcard and discovery pops run under a reserved
group and are compared only for a hard-error status, never for which partition
or messages they claimed; their delivery is judged per-side by the checker.

## Not a raft bug, but worth Alice's eye

- **#1 empty-pop 204** and **#6 leaseReleased attribution** are client-visible
  and an SDK could branch on either. #1 is a clean WP-1.7 facade fix (`PopOut`
  already carries `empty`); #6 needs a decision on which item carries the flag.
- **#4/#5** are the R-101 AckResult-shape limitation surfacing exactly where the
  memo predicted: the codec cannot carry a per-item error string, so raft loses
  the "already committed / invalid lease / unresolvable" vocabulary of 005. The
  OUTCOME is faithful (`success`/`dlq` match); only the human annotation is lost.
- **#7** confirms the WP-1.7c seam fix is right and that the postgres oracle
  itself carries the batch-ack DLQ mis-attribution — the oracle is not a
  ground truth for this one field.

## Reproducing any gap

Comment out the absorbing branch named in the table (all are in `compare.go`
`compareAck` / `run.go notePop` / `normalize.go`), rebuild, and run any seed —
e.g. gap #7 reproduces on `-seed 2`, #6 on `-seed 13`, #1/#2 on almost any seed
at op 0–3.

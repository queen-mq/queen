package main

// The operation catalogue of §13.4.
//
// Every kind in the plan is DECLARED here, with a flag saying whether this
// skeleton implements it. That is the difference between a stub and a missing
// feature: `-mix kv=10` names a real operation and is refused with "documented
// stub", while `-mix kev=10` is refused as a typo. As kinds are implemented,
// flip `implemented` and delete the note.
//
// The first mix (WP-0.7) is push + pop + ack.

import "sort"

type OpKind string

const (
	// Implemented — the whole phase-1 message path (§13.4, the routes the raft1
	// router of WP-1.7a actually serves: push, the three pop routes, ack, ack
	// batch, lease extend).
	OpPush        OpKind = "push"         // push with deliberate duplicates
	OpPop         OpKind = "pop"          // PINNED pop, manual ack (a (partition,lease) batch to ack later)
	OpPopAuto     OpKind = "pop_auto"     // PINNED autoAck pop: no lease, the cursor advances at delivery
	OpPopWildcard OpKind = "pop_wildcard" // wildcard-route autoAck pop under the reserved group (relaxed compare, §5.2)
	OpPopDiscover OpKind = "pop_discover" // namespace/task discovery autoAck pop under the reserved group (relaxed compare)
	OpAck         OpKind = "ack"          // ack ONE outstanding delivery: completed / failed / dlq
	OpAckBatch    OpKind = "ack_batch"    // ack a whole (partition,lease) batch: all-completed (positional) or mixed (per-item dlq)
	OpNack        OpKind = "nack"         // ack status=failed with a reason: retry budget, then the DLQ hand-off (log_dlq_head_v1)
	OpAckByHash   OpKind = "ack_by_hash"  // re-ack a delivery already completed (below the cursor): must answer noop/stale (D10)
	OpRenew       OpKind = "renew"        // POST /api/v1/lease/:id/extend

	// Documented stubs — the phase-2 feature set (§13.4). The raft1 router of
	// phase 1 answers 503 raft_phase1_unsupported for every one of these, so
	// they stay stubs until their WP wires the route. Naming them here keeps
	// `-mix txn=10` a "not implemented yet" refusal, not a typo.
	OpTxn         OpKind = "txn"          // transaction bundles with kv + timer riders (WP-2.1)
	OpKV          OpKind = "kv"           // CAS, incr with min/max, long TTLs, prefix lists (WP-2.2)
	OpTimer       OpKind = "timer"        // schedule / reschedule / cancel, long delays (WP-2.3)
	OpConfigure   OpKind = "configure"    // configure merge and replace (WP-2.5)
	OpSeek        OpKind = "seek"         // consumer-group seek (WP-2.5)
	OpGroupDelete OpKind = "group_delete" // consumer-group delete (WP-2.5)
	OpDLQMove     OpKind = "dlq_move"     // DLQ replay / move (WP-2.5)
	OpDLQPurge    OpKind = "dlq_purge"    // DELETE /api/v1/dlq (WP-2.5)
)

type kindInfo struct {
	implemented bool
	// note says what the stub still owes, quoted from §13.4. It is printed by
	// -list-ops, so the catalogue is readable without opening this file.
	note string
}

var kinds = map[OpKind]kindInfo{
	OpPush:        {implemented: true, note: "push with deliberate duplicates (dup-rate); the survivor keeps the original offset"},
	OpPop:         {implemented: true, note: "PINNED pop, manual ack; records a (partition,lease) batch to ack later — strict response compare"},
	OpPopAuto:     {implemented: true, note: "PINNED autoAck pop: no lease is taken, the cursor advances at delivery — strict compare"},
	OpPopWildcard: {implemented: true, note: "wildcard-route autoAck pop under the reserved group; partition choice is planner-random (§5.2) so the compare is RELAXED (status+success), the per-side checker judges delivery"},
	OpPopDiscover: {implemented: true, note: "discovery autoAck pop by the run's namespace under the reserved group; RELAXED compare (status+success)"},
	OpAck:         {implemented: true, note: "ack ONE outstanding delivery completed/failed/dlq (the hash resolution path)"},
	OpAckBatch:    {implemented: true, note: "ack a whole (partition,lease) batch: all-completed (positional fast path) or mixed (per-item dlq)"},
	OpNack:        {implemented: true, note: "ack status=failed with an error reason; exercises retryLimit and the DLQ hand-off"},
	OpAckByHash:   {implemented: true, note: "re-ack an already-completed delivery (below the cursor): must answer noop/stale on both sides (D10)"},
	OpRenew:       {implemented: true, note: "renew an outstanding lease; newExpiresAt is volatile (dropped), the shape is compared"},

	OpTxn:         {note: "bundle: pushes+acks plus top-level kv and timers riders, with deliberate losers (WP-2.1; raft1 503s /transaction)"},
	OpKV:          {note: "put/get/delete/incr/CAS with expect, ttlSeconds, forever, getPrefix (WP-2.2; raft1 503s /kv)"},
	OpTimer:       {note: "schedule/reschedule/cancel with delays long enough to avoid time races (WP-2.3; raft1 503s /timers)"},
	OpConfigure:   {note: "configure merge (no mode key) and replace (mode:replace) (WP-2.5; raft1 503s /configure)"},
	OpSeek:        {note: "POST /api/v1/consumer-groups/:cg/queues/:q/seek (WP-2.5; raft1 503s)"},
	OpGroupDelete: {note: "DELETE /api/v1/consumer-groups/:cg/queues/:q (WP-2.5; raft1 503s)"},
	OpDLQMove:     {note: "POST /api/v1/dlq/:id/replay and the (partitionId,transactionId) retry route (WP-2.5; raft1 503s)"},
	OpDLQPurge:    {note: "DELETE /api/v1/dlq (WP-2.5; raft1 503s)"},
}

func KnownKind(k OpKind) bool { _, ok := kinds[k]; return ok }

func Implemented(k OpKind) bool { return kinds[k].implemented }

func AllKindNames() []string {
	out := make([]string, 0, len(kinds))
	for k := range kinds {
		out = append(out, string(k))
	}
	sort.Strings(out)
	return out
}

// Op is one planned operation. It is ABSTRACT: it names what to do and which
// slot of side-local state to do it to, never a concrete id. Concrete ids come
// from each side's own answers (see run.go), which is the whole point — the two
// brokers mint different message ids, lease ids and partition ids, and a
// sequence that carried A's ids would test nothing on B.
type Op struct {
	Index int
	Kind  OpKind

	// push
	Items []PlannedPush
	// pop (OpPop, OpPopAuto, OpPopDiscover)
	Queue      string
	Partition  string // "" = wildcard pop on the queue route
	Group      string
	Batch      int
	Partitions int
	LeaseSecs  int

	// Slot selects a position in the side's own outstanding-lease list, reduced
	// modulo its length at execution time (never a concrete id — each side minted
	// its own). DelSlot selects a delivery WITHIN that lease the same way.
	Slot    int
	DelSlot int
	// AckStatus is the status for OpAck (completed | failed | dlq). OpNack forces
	// failed; OpAckByHash forces completed on an already-completed delivery.
	AckStatus string
	// BatchMixed shapes OpAckBatch: false = every item completed (the positional
	// fast path, log_ack_at_v1); true = the last item dlq and the rest completed
	// (per-item DLQ attribution, the WP-1.7c seam), resolved once the side's
	// batch size is known so both sides get the same pattern.
	BatchMixed bool
}

// PlannedPush is one item of a planned push. TxnSlot >= 0 means "reuse the
// transactionId of an earlier push", which is how duplicates are injected: the
// same abstract slot resolves to the same string on both sides, because the
// fuzzer mints transaction ids itself rather than letting the broker do it.
type PlannedPush struct {
	Queue     string
	Partition string
	TxnID     string
	Payload   string // JSON text
	Duplicate bool
}

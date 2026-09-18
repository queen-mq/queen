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
	// Implemented.
	OpPush OpKind = "push" // push with deliberate duplicates
	OpPop  OpKind = "pop"  // pinned / wildcard pop, manual ack
	OpAck  OpKind = "ack"  // ack ok / failed / dlq, by transactionId+partitionId

	// Documented stubs — the rest of §13.4.
	OpPopAuto     OpKind = "pop_auto"     // autoAck pop (no lease, no ack)
	OpPopDiscover OpKind = "pop_discover" // namespace/task discovery pop
	OpRenew       OpKind = "renew"        // POST /api/v1/lease/:id/extend
	OpNack        OpKind = "nack"         // ack status=failed with a reason, retry budget
	OpAckByHash   OpKind = "ack_by_hash"  // ack a payload hash below the cursor
	OpTxn         OpKind = "txn"          // transaction bundles with kv + timer riders
	OpKV          OpKind = "kv"           // CAS, incr with min/max, long TTLs, prefix lists
	OpTimer       OpKind = "timer"        // schedule / reschedule / cancel, long delays
	OpConfigure   OpKind = "configure"    // configure merge and replace
	OpSeek        OpKind = "seek"         // consumer-group seek
	OpGroupDelete OpKind = "group_delete" // consumer-group delete
	OpDLQMove     OpKind = "dlq_move"     // DLQ replay / move
	OpDLQPurge    OpKind = "dlq_purge"    // DELETE /api/v1/dlq
)

type kindInfo struct {
	implemented bool
	// note says what the stub still owes, quoted from §13.4. It is printed by
	// -list-ops, so the catalogue is readable without opening this file.
	note string
}

var kinds = map[OpKind]kindInfo{
	OpPush: {implemented: true, note: "push with deliberate duplicates (dup-rate)"},
	OpPop:  {implemented: true, note: "pop, manual ack, pinned or wildcard by seed"},
	OpAck:  {implemented: true, note: "ack completed/failed/dlq of an outstanding delivery"},

	OpPopAuto:     {note: "autoAck pop: no lease is taken, the cursor advances at delivery"},
	OpPopDiscover: {note: "discovery pop by namespace/task; needs queues created with namespace+task"},
	OpRenew:       {note: "renew an outstanding lease; compare newExpiresAt only for ORDER, never for value"},
	OpNack:        {note: "nack with an error reason; exercises retryLimit and the DLQ hand-off"},
	OpAckByHash:   {note: "ack by payload hash below the cursor: must answer noop/stale on both sides (D10)"},
	OpTxn:         {note: "bundle: pushes+acks plus top-level kv and timers riders, with deliberate losers"},
	OpKV:          {note: "put/get/delete/incr/CAS with expect, ttlSeconds, forever, getPrefix"},
	OpTimer:       {note: "schedule/reschedule/cancel with delays long enough to avoid time races"},
	OpConfigure:   {note: "configure merge (no mode key) and replace (mode:replace)"},
	OpSeek:        {note: "POST /api/v1/consumer-groups/:cg/queues/:q/seek"},
	OpGroupDelete: {note: "DELETE /api/v1/consumer-groups/:cg/queues/:q"},
	OpDLQMove:     {note: "POST /api/v1/dlq/:id/replay and the (partitionId,transactionId) retry route"},
	OpDLQPurge:    {note: "DELETE /api/v1/dlq"},
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
	// pop
	Queue      string
	Partition  string // "" = wildcard pop on the queue route
	Group      string
	Batch      int
	Partitions int
	LeaseSecs  int
	// ack: which outstanding delivery, as an index into the side's own
	// outstanding list, reduced modulo its length at execution time.
	Slot      int
	AckStatus string // completed | failed | dlq
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

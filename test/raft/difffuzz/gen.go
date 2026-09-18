package main

// The seeded generator: config + seed -> the operation sequence.
//
// DETERMINISM IS THE CONTRACT. The sequence is a pure function of (seed,
// config): no clock, no map iteration, no goroutines. `TestGeneratorIsSeeded`
// pins it. Without that a failing seed is not a fixture, it is an anecdote —
// which is exactly how pgless lost half its reproductions.
//
// The generator draws from `math/rand.New(rand.NewSource(seed))`, whose stream
// is stable across Go releases (`rand.Seed`'s global source is not, and is
// deprecated; `math/rand/v2` explicitly does not promise a stable stream).

import (
	"fmt"
	"math/rand"
)

// Generator produces the abstract sequence.
type Generator struct {
	cfg *Config
	rnd *rand.Rand

	queues []string
	groups []string
	parts  []string

	// pinned pops (strict compare, they feed the ack pool) draw from
	// pinnedGroups; the wildcard and discovery pops (relaxed compare, isolated
	// cursor) use wildGroup, so their planner-random partition choice (§5.2)
	// never desyncs a pinned group's backlog. With one group, the same group is
	// both — the ack pool then coexists with wildcard consumption, which is why
	// -groups defaults to 3.
	pinnedGroups []string
	wildGroup    string

	// txnIDs are the transaction ids minted so far, so a later push can reuse
	// one on purpose. Bounded: only the most recent `dupWindow` are candidates,
	// because a duplicate of something pushed 10 000 operations ago tests the
	// dedup window's far edge, which is S2's job, not this loop's.
	txnIDs    []string
	dupWindow int

	// kinds is the mix flattened into a weighted pick table, sorted by kind
	// name so the table is a function of the mix and not of map order.
	table []OpKind
}

func NewGenerator(cfg *Config) *Generator {
	g := &Generator{
		cfg:       cfg,
		rnd:       rand.New(rand.NewSource(cfg.Seed)),
		dupWindow: 64,
	}
	for i := 0; i < cfg.Queues; i++ {
		g.queues = append(g.queues, fmt.Sprintf("%s-%s-q%d", cfg.Namespace, cfg.RunID, i))
	}
	for i := 0; i < cfg.Parts; i++ {
		g.parts = append(g.parts, fmt.Sprintf("p%d", i))
	}
	for i := 0; i < cfg.Groups; i++ {
		g.groups = append(g.groups, fmt.Sprintf("%s-%s-g%d", cfg.Namespace, cfg.RunID, i))
	}
	// Reserve the last group for wildcard/discovery pops; the rest are pinned.
	if len(g.groups) >= 2 {
		g.wildGroup = g.groups[len(g.groups)-1]
		g.pinnedGroups = g.groups[:len(g.groups)-1]
	} else {
		g.wildGroup = g.groups[0]
		g.pinnedGroups = g.groups
	}
	for _, name := range AllKindNames() {
		k := OpKind(name)
		for i := 0; i < cfg.Mix[k]; i++ {
			g.table = append(g.table, k)
		}
	}
	return g
}

// Queues is the world this run touches; the view comparison walks it.
func (g *Generator) Queues() []string { return append([]string(nil), g.queues...) }

// AllGroups is every consumer group (pinned and the reserved wildcard one);
// priming and draining walk all of them.
func (g *Generator) AllGroups() []string { return append([]string(nil), g.groups...) }

// Generate returns the whole sequence up front, so that a report can quote the
// operations after the failing one — the ones that would have run.
func (g *Generator) Generate() []Op {
	ops := make([]Op, 0, g.cfg.Ops)
	for i := 0; i < g.cfg.Ops; i++ {
		ops = append(ops, g.next(i))
	}
	return ops
}

func (g *Generator) next(i int) Op {
	kind := g.table[g.rnd.Intn(len(g.table))]
	op := Op{Index: i, Kind: kind}
	switch kind {
	case OpPush:
		n := 1 + g.rnd.Intn(4)
		for j := 0; j < n; j++ {
			op.Items = append(op.Items, g.plannedPush(i, j))
		}
	case OpPop, OpPopAuto:
		// PINNED to one partition so the two engines return byte-identical
		// batches (the wildcard partition choice is planner-random, §5.2). Under
		// a pinned group, so wildcard/discovery consumption never desyncs it.
		op.Queue = g.queues[g.rnd.Intn(len(g.queues))]
		op.Group = g.pinnedGroups[g.rnd.Intn(len(g.pinnedGroups))]
		op.Partition = g.parts[g.rnd.Intn(len(g.parts))]
		op.Batch = 1 + g.rnd.Intn(8)
		// Leases long enough that no expiry fires inside a run: an expiry is a
		// CLOCK event, and two brokers cannot be expected to fire it in the same
		// operation. The lease is only meaningful for OpPop (manual ack);
		// OpPopAuto takes none. The raft facade caps its own lease at 60 s
		// (real.rs, phase 1) — the expiry field is dropped as volatile, and a
		// run finishes well inside 60 s so no expiry fires.
		op.LeaseSecs = 120
	case OpPopWildcard:
		// Wildcard route, under the reserved group. Relaxed compare (§5.2).
		op.Queue = g.queues[g.rnd.Intn(len(g.queues))]
		op.Group = g.wildGroup
		op.Partitions = 1 + g.rnd.Intn(len(g.parts))
		op.Batch = 1 + g.rnd.Intn(8)
	case OpPopDiscover:
		// Discovery has no queue in the path; it selects by the run's namespace,
		// under the reserved group. Relaxed compare.
		op.Group = g.wildGroup
		op.Batch = 1 + g.rnd.Intn(4)
	case OpAck, OpNack, OpAckByHash:
		op.Slot = g.rnd.Int()
		op.DelSlot = g.rnd.Int()
		switch {
		case kind == OpNack:
			op.AckStatus = "failed"
		case kind == OpAckByHash:
			op.AckStatus = "completed"
		default:
			switch g.rnd.Intn(10) {
			case 0:
				op.AckStatus = "dlq"
			case 1, 2:
				op.AckStatus = "failed"
			default:
				op.AckStatus = "completed"
			}
		}
	case OpAckBatch:
		op.Slot = g.rnd.Int()
		// A third of the batch acks are mixed (last item dlq): the per-item DLQ
		// attribution the WP-1.7c seam fixed. The rest are all-completed, which
		// is the positional fast path (log_ack_at_v1) when the acked set equals
		// the delivered set.
		op.BatchMixed = g.rnd.Intn(3) == 0
	case OpRenew:
		op.Slot = g.rnd.Int()
	default:
		// Unreachable while Validate() refuses unimplemented kinds; kept so that
		// a newly declared kind without a planner fails here, loudly, instead of
		// silently generating an empty operation.
		panic(fmt.Sprintf("difffuzz: no planner for operation kind %q", kind))
	}
	return op
}

func (g *Generator) plannedPush(opIndex, item int) PlannedPush {
	p := PlannedPush{
		Queue:     g.queues[g.rnd.Intn(len(g.queues))],
		Partition: g.parts[g.rnd.Intn(len(g.parts))],
	}
	// A deliberate duplicate reuses a recent transaction id AND its payload:
	// the broker's answer to "same id, different payload" is a separate
	// question (the repack path in fusion.rs) and mixing the two would make a
	// divergence ambiguous.
	if len(g.txnIDs) > 0 && g.rnd.Intn(100) < g.cfg.DupRate {
		lo := 0
		if len(g.txnIDs) > g.dupWindow {
			lo = len(g.txnIDs) - g.dupWindow
		}
		p.TxnID = g.txnIDs[lo+g.rnd.Intn(len(g.txnIDs)-lo)]
		p.Duplicate = true
		p.Payload = payloadFor(p.TxnID)
		return p
	}
	p.TxnID = fmt.Sprintf("%s-%d-%d", g.cfg.RunID, opIndex, item)
	p.Payload = payloadFor(p.TxnID)
	g.txnIDs = append(g.txnIDs, p.TxnID)
	return p
}

// payloadFor makes the payload a pure function of the transaction id, so a
// duplicate push carries byte-identical bytes and the checker (test/raft/checker)
// can hash payload and id together without a side table.
func payloadFor(txn string) string {
	return fmt.Sprintf(`{"txn":%q,"n":%d}`, txn, len(txn))
}

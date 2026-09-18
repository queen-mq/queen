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
	case OpPop:
		op.Queue = g.queues[g.rnd.Intn(len(g.queues))]
		op.Group = g.groups[g.rnd.Intn(len(g.groups))]
		op.Batch = 1 + g.rnd.Intn(8)
		// A third of the pops are pinned to one partition; the rest take the
		// queue route with a width, which is the shape almost every consumer in
		// the field uses.
		if g.rnd.Intn(3) == 0 {
			op.Partition = g.parts[g.rnd.Intn(len(g.parts))]
		} else {
			op.Partitions = 1 + g.rnd.Intn(len(g.parts))
		}
		// Leases long enough that no expiry fires inside a run: an expiry is a
		// CLOCK event, and two brokers cannot be expected to fire it in the same
		// operation. Lease expiry belongs to the kill tests (§13.6), where the
		// checker judges it, not to a response-by-response comparison.
		op.LeaseSecs = 300
	case OpAck:
		op.Slot = g.rnd.Int()
		switch g.rnd.Intn(10) {
		case 0:
			op.AckStatus = "dlq"
		case 1, 2:
			op.AckStatus = "failed"
		default:
			op.AckStatus = "completed"
		}
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

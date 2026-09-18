package main

// The executor: run one abstract sequence against BOTH sides and compare.
//
// The rule that shapes this file: an abstract operation is resolved to concrete
// ids SEPARATELY ON EACH SIDE, from that side's own answers. The two brokers
// mint different message ids, lease ids and partition ids; an ack that carried
// side A's ids to side B would test nothing (it would 404 on B and the
// comparison would be green). So each side keeps its OWN list of outstanding
// (partition, lease) batches, in the order its own pops returned them; an ack op
// picks a POSITION in that list, not an id. If the two lists ever differ in
// shape the run has already diverged, which is reported as a `state` divergence
// and the op is skipped, because every op after it compares two different worlds.
//
// Determinism between the two engines (so a divergence is a bug, not a race or a
// coin flip):
//   - pinned pops (OpPop, OpPopAuto) are compared byte for byte; both engines
//     serve one partition's FIFO, so the batch is identical.
//   - wildcard and discovery pops (OpPopWildcard, OpPopDiscover) choose their
//     partition with planner randomness (§5.2), so they are compared only for
//     status and success, and run under a RESERVED group whose cursor no pinned
//     pop reads — their nondeterministic consumption cannot desync a pinned
//     backlog. The per-side checker log judges what they actually delivered.
//   - every group is PRIMED before any push (an empty registering pop), so a
//     "new"-seeded group sees the whole subsequent log on both engines and the
//     position/time seeding boundary (R-101) is never straddled.
//
// The loop is strictly sequential and A-then-B: concurrency would turn a
// scheduling accident into a "divergence".

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"
)

// delivery is one message a side handed out.
type delivery struct {
	TxnID       string
	PartitionID string
	LeaseID     string
	MessageID   string
	Group       string
	Queue       string
	Payload     json.RawMessage
	Offset      *int64
	Attempt     int
}

// leaseBatch is the set of deliveries one pop returned for one (partition,
// lease): the unit an ack, a batch ack or a renew acts on.
type leaseBatch struct {
	Group       string
	Queue       string
	PartitionID string
	LeaseID     string
	Dels        []delivery
}

// sideState is everything the executor remembers about one broker.
type sideState struct {
	client    *Client
	leases    []leaseBatch // open (partition,lease) batches from OpPop, in pop order
	completed []delivery   // deliveries acked completed, for the below-cursor re-ack (OpAckByHash)
	log       *RunLog      // this side's checker run log (nil unless -logdir)
}

type viewsMode int

const (
	viewsUnknown  viewsMode = iota
	viewsCompare            // both sides serve the views: compare bodies (phase 2+)
	viewsDeferred           // side B answers 503 raft_phase1_unsupported: assert that, do not compare (phase 1)
)

// Runner drives one run.
type Runner struct {
	cfg   *Config
	gen   *Generator
	A     *sideState
	B     *sideState
	out   io.Writer
	views viewsMode

	divergences []Divergence
	executed    int
	stopped     bool // a transport error cut the run short (no drain, partial log)
}

func NewRunner(cfg *Config, out io.Writer) *Runner {
	a := NewClient(cfg.NameA, cfg.URLA, cfg.Timeout)
	b := NewClient(cfg.NameB, cfg.URLB, cfg.Timeout)
	a.Tenant, b.Tenant = cfg.Tenant, cfg.Tenant
	a.Token, b.Token = cfg.Token, cfg.Token
	return &Runner{
		cfg: cfg,
		gen: NewGenerator(cfg),
		A:   &sideState{client: a},
		B:   &sideState{client: b},
		out: out,
	}
}

// SetLogs attaches the per-side checker logs. Either may be nil.
func (r *Runner) SetLogs(a, b *RunLog) { r.A.log, r.B.log = a, b }

// Preflight refuses to start against a broker that is not there, so that a run
// that answers "N divergences" is never just a closed port. It also probes side
// B for whether it serves the final views, choosing the views mode (§13.4 "final
// views ... where available"): a phase-1 raft1 broker answers 503
// raft_phase1_unsupported for every view route, so comparing them would be noise.
func (r *Runner) Preflight(ctx context.Context) error {
	for _, s := range []*sideState{r.A, r.B} {
		c, cancel := context.WithTimeout(ctx, r.cfg.Timeout)
		resp, err := s.client.Health(c)
		cancel()
		if err != nil {
			return fmt.Errorf("preflight %s (%s): %w", s.client.Name, s.client.Base, err)
		}
		if resp.Status != 200 {
			return fmt.Errorf("preflight %s (%s): health answered %d: %s",
				s.client.Name, s.client.Base, resp.Status, trunc(string(resp.Body), 200))
		}
	}
	// Does side B serve the resource views? One probe decides the mode.
	c, cancel := context.WithTimeout(ctx, r.cfg.Timeout)
	resp, err := r.B.client.ViewQueues(c)
	cancel()
	if err != nil {
		return fmt.Errorf("preflight %s views probe: %w", r.B.client.Name, err)
	}
	if resp.Status == 200 {
		r.views = viewsCompare
	} else {
		r.views = viewsDeferred
	}
	return nil
}

// Run executes the sequence and then compares the final views (or asserts they
// are deferred). It returns the report; an error means the run could not be
// carried out (transport, broker down), which is NOT a divergence.
func (r *Runner) Run(ctx context.Context) (*Report, error) {
	if err := r.prime(ctx); err != nil {
		r.stopped = true
		return r.report(), err
	}
	ops := r.gen.Generate()
	for _, op := range ops {
		if err := ctx.Err(); err != nil {
			break
		}
		if err := r.step(ctx, op); err != nil {
			r.stopped = true
			return r.report(), err
		}
		r.executed++
		if r.cfg.StopOnDiff && len(r.divergences) > 0 {
			break
		}
	}
	stoppedForDiff := r.cfg.StopOnDiff && len(r.divergences) > 0
	// The drain feeds the checker's at-least-once (it needs every group to reach
	// empty); it runs even after divergences, since each side is drained on its
	// own — but not after a transport error or a stop-on-diff cut. Settle first:
	// complete every outstanding lease so no unacked lease blocks its partition's
	// cursor (the messages behind it would otherwise read as "never delivered",
	// on BOTH engines, until the lease expires).
	if ctx.Err() == nil && !stoppedForDiff && !r.cfg.NoDrain && (r.A.log != nil || r.B.log != nil) {
		if err := r.settle(ctx); err != nil {
			r.stopped = true
			return r.report(), err
		}
		if err := r.drain(ctx); err != nil {
			r.stopped = true
			return r.report(), err
		}
	}
	if ctx.Err() == nil && !stoppedForDiff {
		if err := r.compareViews(ctx); err != nil {
			r.stopped = true
			return r.report(), err
		}
	}
	return r.report(), nil
}

func (r *Runner) report() *Report {
	return &Report{
		Seed:        r.cfg.Seed,
		RunID:       r.cfg.RunID,
		Ops:         r.cfg.Ops,
		Mix:         r.cfg.Mix.String(),
		SideA:       fmt.Sprintf("%s %s", r.cfg.NameA, r.cfg.URLA),
		SideB:       fmt.Sprintf("%s %s", r.cfg.NameB, r.cfg.URLB),
		Executed:    r.executed,
		Divergences: r.divergences,
		Replay:      r.cfg.ReplayCommand(),
	}
}

func (r *Runner) note(d *Divergence) {
	if d == nil {
		return
	}
	r.divergences = append(r.divergences, *d)
	fmt.Fprintf(r.out, "DIVERGENCE %s\n", d.String())
}

// notePop compares two pop answers, absorbing ONE declared phase-1 parity gap:
// an empty pop is a bodiless 204 on the postgres broker (a firm SDK contract,
// data.rs `pop_status`: count==0 && !conflation => 204) but a 200 with an empty
// body on the raft facade (real.rs `render_claims` always answers 200). This is
// a real client-visible difference — reported as a finding (WP-1.7 facade
// parity) with a representative seed — but it fires on EVERY empty pop, so the
// fuzzer treats "204 bodiless" and "200 with messages:[] partitionsClaimed:0" as
// equal FOR POPS ONLY and keeps hunting the divergences underneath. The scope is
// deliberately narrow: this equates only two EMPTY answers. A pop that delivered
// on one side and not the other still diverges (one empty, one not), and a
// message that should have been delivered but was not is caught by the checker's
// at-least-once, not here.
func (r *Runner) notePop(op int, kind, desc string, a, b *Resp, relaxed bool) {
	// A relaxed pop (wildcard/discovery) chooses its partition with planner
	// randomness AND consumes under the reserved group, so after a few of them
	// the two engines' reserved-group cursors legitimately point at different
	// partitions: one side finding work where the other finds none is EXPECTED,
	// not a divergence. So a relaxed pop only fails on a hard error status on one
	// side but not the other; its delivered content is judged by the per-side
	// checker log, never against the other engine.
	if relaxed {
		okA, okB := a.Status == 200 || a.Status == 204, b.Status == 200 || b.Status == 204
		if okA != okB {
			r.note(&Divergence{
				Op: op, Kind: kind, What: "status", Path: "$",
				A: fmt.Sprint(a.Status), B: fmt.Sprint(b.Status),
				Request: desc + " — one side answered a non-2xx status the other did not",
			})
		}
		return
	}
	ea, eb := isEmptyPop(a), isEmptyPop(b)
	switch {
	case ea && eb:
		return // the declared empty-pop parity gap (204 bodiless == 200 empty)
	case ea != eb:
		r.note(&Divergence{
			Op: op, Kind: kind, What: "body", Path: "$.messages(emptiness)",
			A: emptyPopDesc(a), B: emptyPopDesc(b),
			Request: desc + " — one side delivered, the other returned an empty pop",
		})
	default:
		r.note(CompareResponses(op, kind, desc, a, b, false))
	}
}

// isEmptyPop is true for a bodiless 204 and for a 200 whose messages array is
// present and empty; anything else (an error status, a delivered batch) is not
// "empty" and is left to the real comparison.
func isEmptyPop(resp *Resp) bool {
	if resp == nil {
		return false
	}
	if resp.Status == 204 {
		return true
	}
	if resp.Status != 200 {
		return false
	}
	var body struct {
		Messages *[]json.RawMessage `json:"messages"`
	}
	if err := json.Unmarshal(resp.Body, &body); err != nil {
		return false
	}
	return body.Messages != nil && len(*body.Messages) == 0
}

func emptyPopDesc(resp *Resp) string {
	if resp == nil {
		return "<no response>"
	}
	if isEmptyPop(resp) {
		return fmt.Sprintf("empty pop (status %d)", resp.Status)
	}
	return fmt.Sprintf("status %d body %s", resp.Status, trunc(string(resp.Body), 200))
}

// prime registers every (queue, group) before any push, with one empty
// wildcard autoAck pop. A group that first contacts a queue after traffic seeds
// at the tail ("new" is both engines' default, DEFAULT_SUBSCRIPTION_MODE, and
// the raft facade forces "new" for a named group in phase 1) — registering
// FIRST makes that tail the start of the log, so every later push is visible to
// every group on both engines, and the checker's at-least-once is meaningful.
func (r *Runner) prime(ctx context.Context) error {
	q := PopQuery{Batch: 1, AutoAck: true}
	for _, queue := range r.gen.Queues() {
		for _, group := range r.gen.AllGroups() {
			q.ConsumerGroup = group
			ra, err := r.A.client.Pop(ctx, queue, q)
			if err != nil {
				return err
			}
			rb, err := r.B.client.Pop(ctx, queue, q)
			if err != nil {
				return err
			}
			// Both must agree the queue is empty and the group is now known.
			r.notePop(-1, "prime", "prime "+queue+" "+group, ra, rb, false)
			if r.cfg.StopOnDiff && len(r.divergences) > 0 {
				return nil
			}
		}
	}
	return nil
}

func (r *Runner) step(ctx context.Context, op Op) error {
	if r.cfg.Verbose {
		fmt.Fprintf(r.out, "op %d %s\n", op.Index, op.Kind)
	}
	switch op.Kind {
	case OpPush:
		return r.doPush(ctx, op)
	case OpPop:
		return r.doPop(ctx, op, false)
	case OpPopAuto:
		return r.doPop(ctx, op, true)
	case OpPopWildcard:
		return r.doPopWildcard(ctx, op)
	case OpPopDiscover:
		return r.doPopDiscover(ctx, op)
	case OpAck, OpNack:
		return r.doAck(ctx, op)
	case OpAckBatch:
		return r.doAckBatch(ctx, op)
	case OpAckByHash:
		return r.doAckByHash(ctx, op)
	case OpRenew:
		return r.doRenew(ctx, op)
	default:
		return fmt.Errorf("op %d: no executor for kind %q (ops.go declares it a stub)", op.Index, op.Kind)
	}
}

// ------------------------------------------------------------------ operations

func (r *Runner) doPush(ctx context.Context, op Op) error {
	items := make([]PushItem, 0, len(op.Items))
	dups := 0
	for _, p := range op.Items {
		if p.Duplicate {
			dups++
		}
		items = append(items, PushItem{
			Queue: p.Queue, Partition: p.Partition,
			Payload: json.RawMessage(p.Payload), TransactionID: p.TxnID,
		})
	}
	desc := fmt.Sprintf("push %d item(s), %d deliberate duplicate(s), queues=%s", len(items), dups, queuesOf(op.Items))
	ra, err := r.A.client.Push(ctx, items)
	if err != nil {
		return err
	}
	rb, err := r.B.client.Push(ctx, items)
	if err != nil {
		return err
	}
	r.note(CompareResponses(op.Index, string(op.Kind), desc, ra, rb, false))
	r.recordPush(r.A, op.Items, ra)
	r.recordPush(r.B, op.Items, rb)
	return nil
}

// doPop is a PINNED pop (auto=false: a manual-ack lease; auto=true: cursor
// advances at delivery, no lease). Strict compare: one partition's FIFO is
// identical on both engines.
func (r *Runner) doPop(ctx context.Context, op Op, auto bool) error {
	q := PopQuery{ConsumerGroup: op.Group, Batch: op.Batch, AutoAck: auto}
	if !auto {
		q.LeaseSeconds = op.LeaseSecs
	}
	desc := fmt.Sprintf("pop pinned queue=%s partition=%s group=%s batch=%d auto=%v", op.Queue, op.Partition, op.Group, op.Batch, auto)
	ra, err := r.A.client.PopPartition(ctx, op.Queue, op.Partition, q)
	if err != nil {
		return err
	}
	rb, err := r.B.client.PopPartition(ctx, op.Queue, op.Partition, q)
	if err != nil {
		return err
	}
	r.notePop(op.Index, string(op.Kind), desc, ra, rb, false)
	da := parseDeliveries(ra, op.Queue, op.Group)
	db := parseDeliveries(rb, op.Queue, op.Group)
	r.recordDeliveries(r.A, da)
	r.recordDeliveries(r.B, db)
	if !auto {
		r.A.leases = append(r.A.leases, batchesOf(da)...)
		r.B.leases = append(r.B.leases, batchesOf(db)...)
	}
	return nil
}

func (r *Runner) doPopWildcard(ctx context.Context, op Op) error {
	q := PopQuery{ConsumerGroup: op.Group, Batch: op.Batch, Partitions: op.Partitions, AutoAck: true}
	desc := fmt.Sprintf("pop wildcard queue=%s group=%s batch=%d partitions=%d (relaxed: §5.2 planner-random)", op.Queue, op.Group, op.Batch, op.Partitions)
	ra, err := r.A.client.Pop(ctx, op.Queue, q)
	if err != nil {
		return err
	}
	rb, err := r.B.client.Pop(ctx, op.Queue, q)
	if err != nil {
		return err
	}
	r.notePop(op.Index, string(op.Kind), desc, ra, rb, true)
	r.recordDeliveries(r.A, parseDeliveries(ra, op.Queue, op.Group))
	r.recordDeliveries(r.B, parseDeliveries(rb, op.Queue, op.Group))
	return nil
}

func (r *Runner) doPopDiscover(ctx context.Context, op Op) error {
	q := PopQuery{ConsumerGroup: op.Group, Batch: op.Batch, AutoAck: true, Namespace: r.cfg.Namespace}
	desc := fmt.Sprintf("pop discover namespace=%s group=%s batch=%d (relaxed)", r.cfg.Namespace, op.Group, op.Batch)
	ra, err := r.A.client.PopDiscover(ctx, q)
	if err != nil {
		return err
	}
	rb, err := r.B.client.PopDiscover(ctx, q)
	if err != nil {
		return err
	}
	r.notePop(op.Index, string(op.Kind), desc, ra, rb, true)
	r.recordDeliveries(r.A, parseDeliveries(ra, "", op.Group))
	r.recordDeliveries(r.B, parseDeliveries(rb, "", op.Group))
	return nil
}

// doAck acks ONE outstanding delivery of one lease (the hash resolution path).
// OpNack forces status=failed with a reason.
func (r *Runner) doAck(ctx context.Context, op Op) error {
	if d := r.leaseShapeDivergence(op); d != nil {
		r.note(d)
		return nil
	}
	if len(r.A.leases) == 0 {
		return nil
	}
	li := op.Slot % len(r.A.leases)
	if len(r.A.leases[li].Dels) == 0 {
		return nil
	}
	di := op.DelSlot % len(r.A.leases[li].Dels)
	status := op.AckStatus
	reason := ""
	if op.Kind == OpNack {
		reason = "difffuzz nack"
	}
	desc := fmt.Sprintf("ack lease=%d/%d del=%d status=%s (A txn=%s, B txn=%s)",
		li, len(r.A.leases), di, status, r.A.leases[li].Dels[di].TxnID, r.B.leases[li].Dels[di].TxnID)
	ra, err := r.ackOne(ctx, r.A, li, di, status, reason)
	if err != nil {
		return err
	}
	rb, err := r.ackOne(ctx, r.B, li, di, status, reason)
	if err != nil {
		return err
	}
	r.note(compareAck(op.Index, string(op.Kind), desc, ra, rb, nil))
	r.recordAck(r.A, li, di, status, ra)
	r.recordAck(r.B, li, di, status, rb)
	r.dropDelivery(r.A, li, di)
	r.dropDelivery(r.B, li, di)
	return nil
}

// doAckBatch acks a whole lease batch. Non-mixed = every item completed (the
// positional fast path); mixed = the last item dlq, the rest completed (per-item
// DLQ attribution — the WP-1.7c seam, and the mismatch it fixed).
func (r *Runner) doAckBatch(ctx context.Context, op Op) error {
	if d := r.leaseShapeDivergence(op); d != nil {
		r.note(d)
		return nil
	}
	if len(r.A.leases) == 0 {
		return nil
	}
	li := op.Slot % len(r.A.leases)
	n := len(r.A.leases[li].Dels)
	if n == 0 {
		return nil
	}
	statuses := batchStatuses(n, op.BatchMixed)
	desc := fmt.Sprintf("ack-batch lease=%d/%d n=%d mixed=%v statuses=%v", li, len(r.A.leases), n, op.BatchMixed, statuses)
	ra, err := r.ackBatch(ctx, r.A, li, statuses)
	if err != nil {
		return err
	}
	rb, err := r.ackBatch(ctx, r.B, li, statuses)
	if err != nil {
		return err
	}
	r.note(compareAck(op.Index, string(op.Kind), desc, ra, rb, statuses))
	r.recordAckBatch(r.A, li, statuses, ra)
	r.recordAckBatch(r.B, li, statuses, rb)
	r.dropLease(r.A, li)
	r.dropLease(r.B, li)
	return nil
}

// doAckByHash re-acks a delivery already completed (below the cursor). Both
// engines must answer noop/stale (D10): the state does not change.
func (r *Runner) doAckByHash(ctx context.Context, op Op) error {
	if len(r.A.completed) != len(r.B.completed) {
		r.note(&Divergence{
			Op: op.Index, Kind: string(op.Kind), What: "state", Path: "$.completed.length",
			A: fmt.Sprint(len(r.A.completed)), B: fmt.Sprint(len(r.B.completed)),
			Request: "ack-by-hash: the two sides hold a different number of completed deliveries",
		})
		return nil
	}
	if len(r.A.completed) == 0 {
		return nil
	}
	ci := op.Slot % len(r.A.completed)
	da, db := r.A.completed[ci], r.B.completed[ci]
	// No leaseId on purpose: this isolates the D10 below-cursor honesty (a hash
	// already below the committed cursor) from stale-lease validation. The
	// completed message's lease is long gone; sending its id only provokes the
	// postgres "invalid or expired lease" error and tests a different thing.
	desc := fmt.Sprintf("ack-by-hash (below cursor, no lease) completed=%d slot=%d (A txn=%s, B txn=%s)", len(r.A.completed), ci, da.TxnID, db.TxnID)
	ra, err := r.A.client.Ack(ctx, AckItem{TransactionID: da.TxnID, PartitionID: da.PartitionID, Status: "completed", ConsumerGroup: da.Group})
	if err != nil {
		return err
	}
	rb, err := r.B.client.Ack(ctx, AckItem{TransactionID: db.TxnID, PartitionID: db.PartitionID, Status: "completed", ConsumerGroup: db.Group})
	if err != nil {
		return err
	}
	r.note(compareAck(op.Index, string(op.Kind), desc, ra, rb, nil))
	return nil
}

func (r *Runner) doRenew(ctx context.Context, op Op) error {
	if d := r.leaseShapeDivergence(op); d != nil {
		r.note(d)
		return nil
	}
	if len(r.A.leases) == 0 {
		return nil
	}
	li := op.Slot % len(r.A.leases)
	desc := fmt.Sprintf("renew lease=%d/%d (A lease=%s, B lease=%s)", li, len(r.A.leases), r.A.leases[li].LeaseID, r.B.leases[li].LeaseID)
	ra, err := r.A.client.LeaseExtend(ctx, r.A.leases[li].LeaseID, op.LeaseSecs)
	if err != nil {
		return err
	}
	rb, err := r.B.client.LeaseExtend(ctx, r.B.leases[li].LeaseID, op.LeaseSecs)
	if err != nil {
		return err
	}
	r.note(CompareResponses(op.Index, string(op.Kind), desc, ra, rb, false))
	return nil
}

// --------------------------------------------------------------- ack execution

// ackOne acks ONE delivery by hash — (transactionId, partitionId, group), NO
// leaseId. A single ack of a message whose (multi-message) batch lease was
// already partly acked would, WITH the lease id, hit the postgres broker's
// "invalid or expired lease" (it releases the batch lease on a partial ack,
// leaseReleased:true) while the raft facade keeps the lease and answers null —
// a real divergence reported as a finding, but a consequence of the leaseReleased
// gap, not a new one. Acking by hash (leaseId omitted, the log_ack_by_hash path
// that skips the worker/expiry check, data.rs ≈5527) is an equally valid client
// pattern that both engines resolve the same way, so it isolates the deeper
// surface. The lease id is still exercised where it is the point: OpAckBatch
// (whole batch) and OpRenew.
func (r *Runner) ackOne(ctx context.Context, s *sideState, li, di int, status, reason string) (*Resp, error) {
	d := s.leases[li].Dels[di]
	return s.client.Ack(ctx, AckItem{
		TransactionID: d.TxnID, PartitionID: d.PartitionID, Status: status,
		ConsumerGroup: d.Group, Error: reason,
	})
}

func (r *Runner) ackBatch(ctx context.Context, s *sideState, li int, statuses []string) (*Resp, error) {
	b := s.leases[li]
	items := make([]AckItem, 0, len(b.Dels))
	for i, d := range b.Dels {
		// Ack by hash (no leaseId): a batch acking the remainder of a lease whose
		// head was already acked would otherwise hit the postgres invalid-lease
		// (the same partial-ack lease release as ackOne). Positional fast path and
		// per-item DLQ do not depend on the leaseId.
		it := AckItem{TransactionID: d.TxnID, PartitionID: d.PartitionID, Status: statuses[i]}
		if statuses[i] == "failed" || statuses[i] == "dlq" {
			it.Error = "difffuzz batch"
		}
		items = append(items, it)
	}
	return s.client.AckBatch(ctx, b.Group, items)
}

// ----------------------------------------------------------------- final views

func (r *Runner) compareViews(ctx context.Context) error {
	if r.views == viewsDeferred {
		return r.assertViewsDeferred(ctx)
	}
	cases := []struct {
		name string
		call func(*sideState) (*Resp, error)
	}{
		{"queues", func(s *sideState) (*Resp, error) { return s.client.ViewQueues(ctx) }},
		{"consumer-groups", func(s *sideState) (*Resp, error) { return s.client.ViewConsumerGroups(ctx) }},
	}
	for _, q := range r.gen.Queues() {
		queue := q
		cases = append(cases,
			struct {
				name string
				call func(*sideState) (*Resp, error)
			}{"depth " + queue, func(s *sideState) (*Resp, error) { return s.client.ViewDepth(ctx, queue) }},
			struct {
				name string
				call func(*sideState) (*Resp, error)
			}{"messages " + queue, func(s *sideState) (*Resp, error) { return s.client.ViewMessages(ctx, queue, 1000) }},
			struct {
				name string
				call func(*sideState) (*Resp, error)
			}{"dlq " + queue, func(s *sideState) (*Resp, error) { return s.client.ViewDLQ(ctx, queue, 1000) }},
		)
	}
	for _, c := range cases {
		ra, err := c.call(r.A)
		if err != nil {
			return err
		}
		rb, err := c.call(r.B)
		if err != nil {
			return err
		}
		r.note(CompareResponses(-1, "view:"+c.name, "final view "+c.name, ra, rb, true))
		if r.cfg.StopOnDiff && len(r.divergences) > 0 {
			return nil
		}
	}
	return nil
}

// assertViewsDeferred is the phase-1 contract: side B does not serve the resource
// views, so instead of comparing them we assert B answers 503
// raft_phase1_unsupported on a representative route — a guard that turns green
// into a divergence the day a view is wired but the body is wrong, without
// flooding every run with a 200-vs-503 per queue.
func (r *Runner) assertViewsDeferred(ctx context.Context) error {
	resp, err := r.B.client.ViewQueues(ctx)
	if err != nil {
		return err
	}
	if resp.Status != 503 || !strings.Contains(string(resp.Body), "raft_phase1_unsupported") {
		r.note(&Divergence{
			Op: -1, Kind: "view:queues", What: "views-mode", Path: "$",
			A:       "postgres serves the resource views",
			B:       fmt.Sprintf("expected 503 raft_phase1_unsupported, got %d: %s", resp.Status, trunc(string(resp.Body), 200)),
			Request: "phase-1 views are deferred to WP-2.6; B must answer 503 raft_phase1_unsupported until then",
		})
	}
	if r.B.log != nil {
		r.B.log.note("views deferred: side B answers 503 raft_phase1_unsupported for the resource views (WP-2.6)")
	}
	return nil
}

// --------------------------------------------------------------------- drain

// settle completes every outstanding lease on EACH side (acking by hash, so a
// partially-acked lease is not a problem), clearing the pool. It runs only for
// the logged pre-drain phase, so an unacked lease from the op sequence does not
// block its partition's cursor and hide the messages behind it from the drain.
// The completed messages were already delivered and recorded during the run, so
// this only advances cursors; it compares nothing between the two sides.
func (r *Runner) settle(ctx context.Context) error {
	for _, s := range []*sideState{r.A, r.B} {
		if s.log == nil {
			continue
		}
		for _, b := range s.leases {
			for _, d := range b.Dels {
				if _, err := s.client.Ack(ctx, AckItem{
					TransactionID: d.TxnID, PartitionID: d.PartitionID, Status: "completed", ConsumerGroup: d.Group,
				}); err != nil {
					return err
				}
			}
		}
		s.leases = nil
	}
	return nil
}

// drain pops every (queue, group) to empty on EACH side on its own (with a
// wildcard autoAck LONG-POLL, since a message left under a live lease stays
// leased on both), recording the deliveries, then writes the drain-complete note
// the checker's at-least-once reads. It never compares the two sides — a wildcard
// drain visits partitions in planner-random order — only records each side's own
// truth for its own checker log.
//
// The pops use wait=true on purpose: the window_buffer quiet debounce (§8, 004)
// makes a rapid re-pop of a partition return EMPTY even when work is ready, so a
// plain loop breaks early and leaves messages behind — which reads as a false
// at-least-once violation (observed on BOTH engines). A long-poll waits the
// debounce out and returns the work; a truly empty (group, queue) costs one
// timeout. A short run finishes well inside the 60 s facade lease, so nothing a
// group leased mid-run has expired — that message was delivered once already and
// is in the log, so leaving it leased does not fail at-least-once.
func (r *Runner) drain(ctx context.Context) error {
	const maxRounds = 500
	for _, s := range []*sideState{r.A, r.B} {
		if s.log == nil {
			continue
		}
		for _, queue := range r.gen.Queues() {
			for _, group := range r.gen.AllGroups() {
				q := PopQuery{ConsumerGroup: group, Batch: 64, AutoAck: true,
					Partitions: len(r.gen.parts), Wait: true, TimeoutMS: 700}
				for round := 0; round < maxRounds; round++ {
					resp, err := s.client.Pop(ctx, queue, q)
					if err != nil {
						return err
					}
					dels := parseDeliveries(resp, queue, group)
					if len(dels) == 0 {
						break // the long-poll waited out the debounce and still found nothing
					}
					r.recordDeliveries(s, dels)
				}
				s.log.drainComplete(group, queue)
			}
		}
	}
	return nil
}

// --------------------------------------------------------------- state + log

func (r *Runner) recordPush(s *sideState, planned []PlannedPush, resp *Resp) {
	if s.log == nil {
		return
	}
	if resp == nil || (resp.Status != 201 && resp.Status != 200) {
		for _, p := range planned {
			s.log.pushRejected(p.Queue, p.Partition, p.TxnID)
		}
		return
	}
	var arr []struct {
		Index         int    `json:"index"`
		TransactionID string `json:"transaction_id"`
		MessageID     string `json:"message_id"`
		Status        string `json:"status"`
	}
	if err := json.Unmarshal(resp.Body, &arr); err != nil {
		return
	}
	for _, e := range arr {
		if e.Index < 0 || e.Index >= len(planned) {
			continue
		}
		p := planned[e.Index]
		s.log.push(p.Queue, p.Partition, p.TxnID, e.MessageID, payloadHashOf(json.RawMessage(p.Payload)), e.Status)
	}
}

func (r *Runner) recordDeliveries(s *sideState, dels []delivery) {
	if s.log == nil {
		return
	}
	for _, d := range dels {
		s.log.delivery(d.Queue, d.PartitionID, d.TxnID, d.MessageID, d.Group, d.LeaseID, payloadHashOf(d.Payload), d.Offset, d.Attempt)
	}
}

// recordAck logs one ack item's verdict (from the ack answer) and, on a filed
// dead letter, a dlq_row with the delivered payload's hash so payload-hash can
// judge it. It also promotes a completed delivery to the below-cursor set.
func (r *Runner) recordAck(s *sideState, li, di int, status string, resp *Resp) {
	d := s.leases[li].Dels[di]
	ok, dlq := ackItemVerdict(resp, 0)
	if s.log != nil {
		s.log.ack(d.Queue, d.PartitionID, d.TxnID, d.Group, status, ok)
		if dlq {
			s.log.dlqRow(d.Queue, d.PartitionID, d.TxnID, d.Group, payloadHashOf(d.Payload))
		}
	}
	if ok && status == "completed" {
		s.completed = append(s.completed, d)
	}
}

func (r *Runner) recordAckBatch(s *sideState, li int, statuses []string, resp *Resp) {
	b := s.leases[li]
	for i, d := range b.Dels {
		ok, dlq := ackItemVerdict(resp, i)
		if s.log != nil {
			s.log.ack(d.Queue, d.PartitionID, d.TxnID, d.Group, statuses[i], ok)
			if dlq {
				s.log.dlqRow(d.Queue, d.PartitionID, d.TxnID, d.Group, payloadHashOf(d.Payload))
			}
		}
		if ok && statuses[i] == "completed" {
			s.completed = append(s.completed, d)
		}
	}
}

func (r *Runner) dropDelivery(s *sideState, li, di int) {
	s.leases[li].Dels = append(s.leases[li].Dels[:di], s.leases[li].Dels[di+1:]...)
	if len(s.leases[li].Dels) == 0 {
		r.dropLease(s, li)
	}
}

func (r *Runner) dropLease(s *sideState, li int) {
	s.leases = append(s.leases[:li], s.leases[li+1:]...)
}

// leaseShapeDivergence reports (and returns non-nil) when the two sides do not
// hold the same lease-pool shape at the slot this op will touch, so a following
// ack would compare two different worlds.
func (r *Runner) leaseShapeDivergence(op Op) *Divergence {
	if len(r.A.leases) != len(r.B.leases) {
		return &Divergence{
			Op: op.Index, Kind: string(op.Kind), What: "state", Path: "$.leases.length",
			A: fmt.Sprint(len(r.A.leases)), B: fmt.Sprint(len(r.B.leases)),
			Request: "the two sides hold a different number of open leases",
		}
	}
	if len(r.A.leases) == 0 {
		return nil
	}
	li := op.Slot % len(r.A.leases)
	if len(r.A.leases[li].Dels) != len(r.B.leases[li].Dels) {
		return &Divergence{
			Op: op.Index, Kind: string(op.Kind), What: "state", Path: fmt.Sprintf("$.leases[%d].deliveries.length", li),
			A: fmt.Sprint(len(r.A.leases[li].Dels)), B: fmt.Sprint(len(r.B.leases[li].Dels)),
			Request: "the chosen lease holds a different number of deliveries on the two sides",
		}
	}
	return nil
}

// --------------------------------------------------------------------- parsing

// parseDeliveries reads a pop answer as that side's own truth. A body it cannot
// read yields no deliveries: the response comparison has already judged it.
func parseDeliveries(resp *Resp, queue, group string) []delivery {
	if resp == nil || resp.Status != 200 {
		return nil
	}
	var body struct {
		Queue         string `json:"queue"`
		PartitionID   string `json:"partitionId"`
		LeaseID       string `json:"leaseId"`
		ConsumerGroup string `json:"consumerGroup"`
		Messages      []struct {
			MessageID     string          `json:"id"`
			TransactionID string          `json:"transactionId"`
			PartitionID   string          `json:"partitionId"`
			LeaseID       string          `json:"leaseId"`
			ConsumerGroup string          `json:"consumerGroup"`
			Data          json.RawMessage `json:"data"`
			Offset        *int64          `json:"offset"`
			Attempt       int             `json:"deliveryAttempt"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(resp.Body, &body); err != nil {
		return nil
	}
	out := make([]delivery, 0, len(body.Messages))
	for _, m := range body.Messages {
		d := delivery{
			TxnID: m.TransactionID, PartitionID: m.PartitionID, LeaseID: m.LeaseID, MessageID: m.MessageID,
			Group: m.ConsumerGroup, Queue: queue, Payload: m.Data, Offset: m.Offset, Attempt: m.Attempt,
		}
		if d.PartitionID == "" {
			d.PartitionID = body.PartitionID
		}
		if d.LeaseID == "" {
			d.LeaseID = body.LeaseID
		}
		if d.Group == "" {
			d.Group = body.ConsumerGroup
		}
		if d.Group == "" {
			d.Group = group
		}
		if d.Queue == "" {
			d.Queue = body.Queue
		}
		out = append(out, d)
	}
	return out
}

// batchesOf groups a pop's deliveries by (partitionId, leaseId), in first-seen
// order, so a wildcard pop that spanned several partitions becomes several
// ackable batches. Pinned pops return one batch.
func batchesOf(dels []delivery) []leaseBatch {
	var batches []leaseBatch
	idx := map[string]int{}
	for _, d := range dels {
		k := d.PartitionID + "\x00" + d.LeaseID
		i, ok := idx[k]
		if !ok {
			i = len(batches)
			idx[k] = i
			batches = append(batches, leaseBatch{Group: d.Group, Queue: d.Queue, PartitionID: d.PartitionID, LeaseID: d.LeaseID})
		}
		batches[i].Dels = append(batches[i].Dels, d)
	}
	return batches
}

// ackItemVerdict reads the ack answer's item at index i: (success, dlq). The ack
// and ack/batch routes both answer a TOP-LEVEL array [{index,success,dlq,...}].
func ackItemVerdict(resp *Resp, i int) (ok, dlq bool) {
	if resp == nil || resp.Status != 200 {
		return false, false
	}
	var arr []struct {
		Index   int  `json:"index"`
		Success bool `json:"success"`
		DLQ     bool `json:"dlq"`
	}
	if err := json.Unmarshal(resp.Body, &arr); err != nil {
		return false, false
	}
	for _, e := range arr {
		if e.Index == i {
			return e.Success, e.DLQ
		}
	}
	if i < len(arr) {
		return arr[i].Success, arr[i].DLQ
	}
	return false, false
}

// batchStatuses is the per-item status pattern for a batch ack, a pure function
// of the batch size and the mixed flag so both sides get the same pattern.
func batchStatuses(n int, mixed bool) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = "completed"
	}
	if mixed && n >= 2 {
		out[n-1] = "dlq"
	}
	return out
}

// --------------------------------------------------------------------- helpers

func queuesOf(items []PlannedPush) string {
	seen := map[string]bool{}
	var names []string
	for _, p := range items {
		if !seen[p.Queue] {
			seen[p.Queue] = true
			names = append(names, p.Queue)
		}
	}
	return strings.Join(names, ",")
}

// ReplayCommand is the line to paste into a bug report. Kept here rather than in
// config.go so that every field the runner depends on is in one place.
func (c *Config) ReplayCommand() string {
	return fmt.Sprintf("GOWORK=off go run . -a %s -b %s -seed %d -ops %d -mix %s -run-id %s -queues %d -partitions %d -groups %d -dup-rate %d -timeout %s",
		c.URLA, c.URLB, c.Seed, c.Ops, c.Mix.String(), c.RunID, c.Queues, c.Parts, c.Groups, c.DupRate, c.Timeout.Round(time.Millisecond))
}

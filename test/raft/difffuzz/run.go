package main

// The executor: run one abstract sequence against BOTH sides and compare.
//
// The rule that shapes this file: an abstract operation is resolved to
// concrete ids SEPARATELY ON EACH SIDE, from that side's own answers. The two
// brokers mint different message ids, lease ids and partition ids; an ack that
// carried side A's ids to side B would test nothing (it would 404 on B, twice,
// identically, and the comparison would be green).
//
// So each side keeps its own `outstanding` list of deliveries, in the order its
// own pops returned them. `Op.Slot` selects a position in that list, not an id.
// If the two lists ever have different lengths the run has already diverged;
// that is reported as a `state` divergence and the operation is skipped, because
// every operation after it would be comparing two different worlds.
//
// The loop is strictly sequential and A-then-B: concurrency would turn a
// scheduling accident into a "divergence" (§13.4 compares responses, and a
// broker is allowed to answer two concurrent pops in either order).

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"
)

// delivery is one message a side handed out and that is not yet acked.
type delivery struct {
	TxnID       string
	PartitionID string
	LeaseID     string
	Group       string
}

// sideState is everything the executor remembers about one broker.
type sideState struct {
	client      *Client
	outstanding []delivery
}

// Runner drives one run.
type Runner struct {
	cfg *Config
	gen *Generator
	A   *sideState
	B   *sideState
	out io.Writer

	divergences []Divergence
	executed    int
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

// Preflight refuses to start against a broker that is not there, so that a run
// that answers "200 divergences" is never just a closed port.
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
	return nil
}

// Run executes the sequence and then compares the final views. It returns the
// report; an error means the run could not be carried out (transport, broker
// down), which is NOT the same as a divergence.
func (r *Runner) Run(ctx context.Context) (*Report, error) {
	ops := r.gen.Generate()
	for _, op := range ops {
		if err := ctx.Err(); err != nil {
			break
		}
		if err := r.step(ctx, op); err != nil {
			return r.report(), err
		}
		r.executed++
		if r.cfg.StopOnDiff && len(r.divergences) > 0 {
			break
		}
	}
	if err := ctx.Err(); err == nil && !(r.cfg.StopOnDiff && len(r.divergences) > 0) {
		if err := r.compareViews(ctx); err != nil {
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

func (r *Runner) step(ctx context.Context, op Op) error {
	if r.cfg.Verbose {
		fmt.Fprintf(r.out, "op %d %s\n", op.Index, op.Kind)
	}
	switch op.Kind {
	case OpPush:
		return r.doPush(ctx, op)
	case OpPop:
		return r.doPop(ctx, op)
	case OpAck:
		return r.doAck(ctx, op)
	default:
		// Validate() refuses unimplemented kinds, so reaching this is a bug in
		// this file, not a user error.
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
			Queue:         p.Queue,
			Partition:     p.Partition,
			Payload:       json.RawMessage(p.Payload),
			TransactionID: p.TxnID,
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
	return nil
}

func (r *Runner) doPop(ctx context.Context, op Op) error {
	q := PopQuery{
		ConsumerGroup: op.Group,
		Batch:         op.Batch,
		Partitions:    op.Partitions,
		LeaseSeconds:  op.LeaseSecs,
	}
	desc := fmt.Sprintf("pop queue=%s partition=%q group=%s batch=%d partitions=%d leaseSeconds=%d",
		op.Queue, op.Partition, op.Group, op.Batch, op.Partitions, op.LeaseSecs)
	call := func(s *sideState) (*Resp, error) {
		if op.Partition != "" {
			return s.client.PopPartition(ctx, op.Queue, op.Partition, q)
		}
		return s.client.Pop(ctx, op.Queue, q)
	}
	ra, err := call(r.A)
	if err != nil {
		return err
	}
	rb, err := call(r.B)
	if err != nil {
		return err
	}
	r.note(CompareResponses(op.Index, string(op.Kind), desc, ra, rb, false))
	// Each side records ITS OWN deliveries, whatever the comparison said: if
	// they diverged, the next ack will report a state divergence, which is a
	// better message than a stream of 404s.
	r.A.outstanding = append(r.A.outstanding, deliveriesOf(ra, op.Group)...)
	r.B.outstanding = append(r.B.outstanding, deliveriesOf(rb, op.Group)...)
	return nil
}

func (r *Runner) doAck(ctx context.Context, op Op) error {
	if len(r.A.outstanding) != len(r.B.outstanding) {
		r.note(&Divergence{
			Op: op.Index, Kind: string(op.Kind), What: "state", Path: "$.outstanding.length",
			A: fmt.Sprint(len(r.A.outstanding)), B: fmt.Sprint(len(r.B.outstanding)),
			Request: "ack: the two sides hold a different number of outstanding deliveries",
		})
		return nil
	}
	if len(r.A.outstanding) == 0 {
		return nil // nothing to ack yet; the mix will come back around
	}
	idx := op.Slot % len(r.A.outstanding)
	da := r.A.outstanding[idx]
	db := r.B.outstanding[idx]
	desc := fmt.Sprintf("ack slot=%d/%d status=%s (A txn=%s, B txn=%s)",
		idx, len(r.A.outstanding), op.AckStatus, da.TxnID, db.TxnID)
	ra, err := r.A.client.Ack(ctx, AckItem{
		TransactionID: da.TxnID, PartitionID: da.PartitionID, Status: op.AckStatus,
		LeaseID: da.LeaseID, ConsumerGroup: da.Group,
	})
	if err != nil {
		return err
	}
	rb, err := r.B.client.Ack(ctx, AckItem{
		TransactionID: db.TxnID, PartitionID: db.PartitionID, Status: op.AckStatus,
		LeaseID: db.LeaseID, ConsumerGroup: db.Group,
	})
	if err != nil {
		return err
	}
	r.note(CompareResponses(op.Index, string(op.Kind), desc, ra, rb, false))
	r.A.outstanding = removeAt(r.A.outstanding, idx)
	r.B.outstanding = removeAt(r.B.outstanding, idx)
	return nil
}

// ----------------------------------------------------------------- final views

// viewCase is one end-of-run comparison. `sorted` says whether the broker
// promises an order: a queue listing does not, a message page does.
type viewCase struct {
	name   string
	sorted bool
	call   func(*sideState) (*Resp, error)
}

func (r *Runner) compareViews(ctx context.Context) error {
	cases := []viewCase{
		{name: "queues", sorted: true, call: func(s *sideState) (*Resp, error) { return s.client.ViewQueues(ctx) }},
		{name: "consumer-groups", sorted: true, call: func(s *sideState) (*Resp, error) { return s.client.ViewConsumerGroups(ctx) }},
	}
	for _, q := range r.gen.Queues() {
		queue := q
		cases = append(cases,
			viewCase{name: "depth " + queue, sorted: true, call: func(s *sideState) (*Resp, error) { return s.client.ViewDepth(ctx, queue) }},
			viewCase{name: "messages " + queue, sorted: true, call: func(s *sideState) (*Resp, error) { return s.client.ViewMessages(ctx, queue, 1000) }},
			viewCase{name: "dlq " + queue, sorted: true, call: func(s *sideState) (*Resp, error) { return s.client.ViewDLQ(ctx, queue, 1000) }},
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
		r.note(CompareResponses(-1, "view:"+c.name, "final view "+c.name, ra, rb, c.sorted))
		if r.cfg.StopOnDiff && len(r.divergences) > 0 {
			return nil
		}
	}
	return nil
}

// --------------------------------------------------------------------- helpers

// deliveriesOf reads a pop answer as that side's own truth. A body it cannot
// read is not an error here: the response comparison has already seen it.
func deliveriesOf(resp *Resp, group string) []delivery {
	if resp == nil || resp.Status != 200 {
		return nil
	}
	var body struct {
		Partition string `json:"partitionId"`
		LeaseID   string `json:"leaseId"`
		Group     string `json:"consumerGroup"`
		Messages  []struct {
			TransactionID string `json:"transactionId"`
			PartitionID   string `json:"partitionId"`
			LeaseID       string `json:"leaseId"`
			ConsumerGroup string `json:"consumerGroup"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(resp.Body, &body); err != nil {
		return nil
	}
	out := make([]delivery, 0, len(body.Messages))
	for _, m := range body.Messages {
		d := delivery{TxnID: m.TransactionID, PartitionID: m.PartitionID, LeaseID: m.LeaseID, Group: m.ConsumerGroup}
		if d.PartitionID == "" {
			d.PartitionID = body.Partition
		}
		if d.LeaseID == "" {
			d.LeaseID = body.LeaseID
		}
		if d.Group == "" {
			d.Group = body.Group
		}
		if d.Group == "" {
			d.Group = group
		}
		out = append(out, d)
	}
	return out
}

func removeAt(ds []delivery, i int) []delivery {
	out := make([]delivery, 0, len(ds)-1)
	out = append(out, ds[:i]...)
	return append(out, ds[i+1:]...)
}

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

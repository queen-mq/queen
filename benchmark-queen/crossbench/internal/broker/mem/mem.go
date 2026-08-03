// Package mem is an in-memory reference broker with perfect semantics.
//
// It is the CONTROL of the experiment, not a system under test. It delivers
// every message exactly once, in strict per-key order, and never reorders or
// drops anything. A run against it must therefore verify PASS with 0 gaps,
// 0 dups and 0 order violations.
//
// That matters: without a control, a FAIL on Kafka or RabbitMQ could always be
// the harness manufacturing the defect. Running mem first proves the rig is
// clean, so every later defect belongs to the system that produced it.
package mem

import (
	"context"
	"sync"
	"time"

	"crossbench/internal/broker"
	"crossbench/internal/workload"
)

type keyLog struct {
	msgs []broker.Message
}

type groupState struct {
	cursor   map[string]int  // key -> next index to deliver
	inflight map[string]bool // key -> a handler currently owns it
}

type topic struct {
	mu     sync.Mutex
	keys   map[string]*keyLog
	order  []string // stable key order for round-robin scanning
	groups map[string]*groupState
}

// Broker is the in-memory reference implementation.
type Broker struct {
	mu     sync.RWMutex
	topics map[string]*topic

	published, delivered, acked, nacked int64
	statsMu                             sync.Mutex
}

// New returns an empty in-memory broker.
func New() *Broker { return &Broker{topics: map[string]*topic{}} }

func (b *Broker) Name() string { return "mem" }

func (b *Broker) Setup(ctx context.Context, t workload.Topology, o broker.SetupOpts) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.topics = map[string]*topic{}
	for _, name := range t.Topics() {
		tp := &topic{keys: map[string]*keyLog{}, groups: map[string]*groupState{}}
		for _, g := range t.GroupsFor(name) {
			tp.groups[g] = &groupState{cursor: map[string]int{}, inflight: map[string]bool{}}
		}
		b.topics[name] = tp
	}
	return nil
}

func (b *Broker) topicOf(name string) *topic {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.topics[name]
}

func (b *Broker) Publish(ctx context.Context, topicName, key string, payload []byte) error {
	return b.PublishBatch(ctx, topicName, key, [][]byte{payload})
}

func (b *Broker) PublishBatch(ctx context.Context, topicName, key string, payloads [][]byte) error {
	tp := b.topicOf(topicName)
	if tp == nil {
		return &notFound{topicName}
	}
	msgs := make([]broker.Message, 0, len(payloads))
	for _, p := range payloads {
		st, err := workload.DecodeStamp(p)
		if err != nil {
			return err
		}
		cp := make([]byte, len(p))
		copy(cp, p)
		msgs = append(msgs, broker.Message{Stamp: st, Payload: cp})
	}

	tp.mu.Lock()
	kl := tp.keys[key]
	if kl == nil {
		kl = &keyLog{}
		tp.keys[key] = kl
		tp.order = append(tp.order, key)
	}
	kl.msgs = append(kl.msgs, msgs...)
	tp.mu.Unlock()

	b.statsMu.Lock()
	b.published += int64(len(msgs))
	b.statsMu.Unlock()
	return nil
}

func (b *Broker) Consume(ctx context.Context, topicName, group string,
	o broker.ConsumeOpts, h broker.Handler) error {

	tp := b.topicOf(topicName)
	if tp == nil {
		return &notFound{topicName}
	}
	stats := o.AckStats()
	batchSize := o.BatchSize
	if batchSize < 1 {
		batchSize = 100
	}
	// One worker per lane would be wasteful; the lane budget is enforced by the
	// stage's own semaphore. Enough workers to keep every key busy is plenty.
	workers := o.Lanes/batchSize + 8

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(start int) {
			defer wg.Done()
			cursor := start
			for ctx.Err() == nil {
				key, msgs, ok := tp.claim(group, batchSize, &cursor)
				if !ok {
					select {
					case <-ctx.Done():
						return
					case <-time.After(time.Millisecond):
					}
					continue
				}
				b.statsMu.Lock()
				b.delivered += int64(len(msgs))
				b.statsMu.Unlock()

				batch := &broker.Batch{Key: key, Msgs: msgs}
				err := h(ctx, batch)
				tp.release(group, key, len(msgs), err == nil)

				b.statsMu.Lock()
				if err == nil {
					b.acked += int64(len(msgs))
					stats.Acked.Add(int64(len(msgs)))
				} else {
					b.nacked += int64(len(msgs))
				}
				b.statsMu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	return nil
}

// claim finds a key with pending messages that no handler currently owns, marks
// it in flight and returns up to batchSize messages in seq order. The in-flight
// flag is what enforces "a key is never in two handlers at once" — the core
// contract of broker.Batch.
func (t *topic) claim(group string, batchSize int, cursor *int) (string, []broker.Message, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	gs := t.groups[group]
	if gs == nil || len(t.order) == 0 {
		return "", nil, false
	}
	n := len(t.order)
	for i := 0; i < n; i++ {
		idx := (*cursor + i) % n
		key := t.order[idx]
		if gs.inflight[key] {
			continue
		}
		kl := t.keys[key]
		at := gs.cursor[key]
		if at >= len(kl.msgs) {
			continue
		}
		end := at + batchSize
		if end > len(kl.msgs) {
			end = len(kl.msgs)
		}
		out := make([]broker.Message, end-at)
		copy(out, kl.msgs[at:end])
		gs.inflight[key] = true
		*cursor = idx + 1
		return key, out, true
	}
	return "", nil, false
}

// release advances the group cursor when the batch was acked, or leaves it in
// place so the batch redelivers.
func (t *topic) release(group, key string, n int, acked bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	gs := t.groups[group]
	if gs == nil {
		return
	}
	if acked {
		gs.cursor[key] += n
	}
	delete(gs.inflight, key)
}

func (b *Broker) Provisioned() broker.Provisioned {
	b.mu.RLock()
	defer b.mu.RUnlock()
	lanes, queues := 0, 0
	for _, tp := range b.topics {
		queues++
		tp.mu.Lock()
		lanes += len(tp.order)
		tp.mu.Unlock()
	}
	return broker.Provisioned{
		OrderedLanes:             lanes,
		PhysicalQueues:           queues,
		ConsumerMembers:          0,
		Connections:              0,
		PublishesPerIngressEvent: 2,
	}
}

func (b *Broker) Stats() map[string]any {
	b.statsMu.Lock()
	defer b.statsMu.Unlock()
	return map[string]any{
		"mem_published": b.published,
		"mem_delivered": b.delivered,
		"mem_acked":     b.acked,
		"mem_nacked":    b.nacked,
	}
}

func (b *Broker) Close() error { return nil }

type notFound struct{ topic string }

func (e *notFound) Error() string { return "mem: unknown topic " + e.topic }

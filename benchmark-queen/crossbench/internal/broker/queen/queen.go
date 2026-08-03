// Package queen adapts QueenMQ to the CM-BENCH contract.
//
// Queen is not the reference here, it is a system under test like the others,
// and its numbers in this campaign come from THIS adapter — not from the July
// cm.go. Re-measuring it on the shared harness is the point (SPEC.md §9): a
// comparison where one side runs bespoke, hand-tuned code and the others run a
// generic adapter is not a comparison.
//
// Design notes that are results rather than implementation detail:
//
//   - 1 property = 1 partition. Partitions are created lazily and are cheap, so
//     ordered lanes scale with key cardinality instead of being a fixed number
//     chosen up front. Consumption concurrency is NOT tied to that count: a
//     bounded worker pool roams the partition space.
//
//   - CONSUMPTION MODE IS A REPORTED AXIS, NOT A TUNING KNOB, and the default is
//     WILDCARD. This matters for fairness and it is easy to get wrong:
//
//     Targeted pops are faster — July measured the wildcard candidate scan at
//     ~12 ms per pop at 1000 partitions, up to ~35 ms once allocator UPDATE
//     churn on log_partitions had piled up dead versions (20-30 PG cores of
//     pure scanning), against ~0.15 ms targeted. But targeted pops get there by
//     making the APPLICATION own a static partition-to-worker map, which means
//     knowing the key space up front and re-sharding when it changes. That is
//     Queen's version of Kafka's static partition assignment. It trades away
//     precisely the property Queen is claimed to have — dynamic lanes, elastic
//     consumers, no advance knowledge of cardinality.
//
//     So running Queen targeted while running Kafka with a parallel consumer
//     would compare Queen's RIGID mode against Kafka's FLEXIBLE one on
//     concurrency, while flattering Queen on server cost. Neither number is
//     "the" Queen number. Both modes are run, always, as a pair:
//
//     wildcard  the mode that delivers the architectural claim; its candidate
//     scan is a real cost of that claim and must not be hidden.
//     targeted  a genuine optimisation that buys server cost by giving the
//     claim up; charged for the machinery it pushes into the app.
//
//   - Native consumer groups: a derived event is published ONCE and read by all
//     five groups, so the publish factor is 2 per ingress event, not 6.
//
//   - Broker-side dedup by key over a window is available and is set at t=0
//     through /api/v1/configure. The headline comparison runs it OFF everywhere
//     so the core shape is comparable (SPEC.md §5.4); the dedup-axis run turns
//     it on here and gives the other systems an external dedup store, and the
//     delta is the measured cost of the feature.
package queen

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	qc "github.com/smartpricing/queen/clients/client-go"

	"crossbench/internal/broker"
	"crossbench/internal/workload"
)

// Config tunes the adapter.
type Config struct {
	URL string

	// Targeted selects static partition ownership over wildcard pops. It
	// defaults to FALSE: see the package comment — this is a reported axis, and
	// the wildcard mode is the one that matches the architectural claim.
	Targeted bool

	// PopPartitions is the multi-partition cap for WILDCARD pops only.
	//
	// The July value of 10 was chosen at 25k ev/s, where every partition held
	// ~25 msg/s and a pop of 10 partitions came back full. At a low rate over
	// the same 1000 partitions the arithmetic inverts: each partition holds ~1
	// msg/s, so what sets latency is how often a partition gets VISITED, and a
	// cap of 10 visits very few of them per pop. Treat this as rate-dependent,
	// not as a constant.
	PopPartitions int

	// PopWorkers overrides the derived pop-worker count per stage. 0 derives it
	// from the lane budget, which is right when work dominates and wrong when
	// partition revisit rate dominates.
	PopWorkers int

	// SweepFloor bounds the empty-pop rate: a worker that sweeps its whole
	// partition slice faster than this waits out the difference.
	SweepFloor time.Duration

	// PopWait makes empty pops park server-side (long poll) instead of spinning.
	// Only meaningful for wildcard pops; a targeted sweep does its own pacing.
	PopWait    bool
	PopTimeout int

	LeaseTimeSec       int
	CompletedRetention int
	AckInflight        int
	IdleConns          int
	TimeoutMs          int

	Token      string // tenant API key: aims the run at queen_proxy instead
	HostHeader string

	// AutoAck pops with autoAck=true: the broker commits the cursor at claim
	// time, there is no lease, no wheel-until-promote, and the client never
	// acks. A/B lever to measure the ack path's contribution to latency.
	AutoAck bool
}

// DefaultConfig is the campaign baseline.
func DefaultConfig(url string) Config {
	return Config{
		URL: url,
		// Wildcard by default: the mode whose cost belongs to Queen's own
		// design rather than to machinery pushed into the application.
		Targeted:           false,
		PopPartitions:      10,
		SweepFloor:         250 * time.Millisecond,
		PopWait:            true,
		PopTimeout:         2000,
		LeaseTimeSec:       60,
		CompletedRetention: 300,
		AckInflight:        1024,
		IdleConns:          2048,
		TimeoutMs:          30000,
	}
}

// Broker is the QueenMQ system under test.
type Broker struct {
	cfg         Config
	q           *qc.Queen
	topo        workload.Topology
	dedupWindow int

	// lanes is the number of PHYSICAL ordered lanes (partitions) per queue.
	// It defaults to one per property, which is Queen's native model, but it
	// MUST be settable: the campaign compares systems at matched lane counts,
	// and an adapter that silently ignores it produces a table where Queen ran
	// 1000 real partitions against Kafka's 200 and RabbitMQ's 100 with nothing
	// in the report saying so. That happened on 2026-08-02.
	lanes int

	ackSem chan struct{}

	workers   atomic.Int64
	pops      atomic.Int64
	emptyPops atomic.Int64
	popped    atomic.Int64
	pushes    atomic.Int64
	pushBatch atomic.Int64
	acked     atomic.Int64
	ackErr    atomic.Int64
	redeliv   atomic.Int64
}

// New connects a client.
func New(ctx context.Context, cfg Config) (*Broker, error) {
	conf := qc.ClientConfig{
		URL:           cfg.URL,
		TimeoutMillis: cfg.TimeoutMs,
		// Retries are safe here BECAUSE a dedup window makes a re-sent push
		// idempotent; they stop a transient error from losing a seq the pacer
		// already scheduled, which the verifier would otherwise read as a gap.
		RetryAttempts: 3,
	}
	if cfg.Token != "" {
		conf.BearerToken = cfg.Token
	}
	if cfg.HostHeader != "" {
		conf.Headers = map[string]string{"Host": cfg.HostHeader}
	}
	cl, err := qc.New(conf)
	if err != nil {
		return nil, fmt.Errorf("queen client: %w", err)
	}
	inflight := cfg.AckInflight
	if inflight < 1 {
		inflight = 1
	}
	return &Broker{cfg: cfg, q: cl, ackSem: make(chan struct{}, inflight)}, nil
}

func (b *Broker) Name() string { return "queen" }

// partition maps an ordering key onto a physical partition.
//
// With lanes == properties this is the identity and Queen runs its native
// 1-property-1-partition model. With fewer lanes, properties share a partition
// exactly the way they share a Kafka partition or a RabbitMQ lane queue: order
// per property still holds (partition order is a superset of per-key order),
// and the head-of-line blocking that follows is the measured cost.
func (b *Broker) partition(key string) string {
	if b.lanes >= b.topo.Properties {
		return key
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return "l" + strconv.Itoa(int(h.Sum32()%uint32(b.lanes)))
}

// laneName is the partition of lane index i, for the targeted sweep.
func (b *Broker) laneName(i int) string {
	if b.lanes >= b.topo.Properties {
		return workload.PartitionKey(i)
	}
	return "l" + strconv.Itoa(i)
}

// Setup configures all four queues at t=0 through /api/v1/configure.
//
// It deliberately does NOT use the typed QueueConfig builder: that builder has
// no dedup-window field, so it cannot express the dedup axis of SPEC.md §5.4.
// The configure endpoint takes the full options map, which is the path the July
// run used and the only one that can set dedupWindowSeconds.
func (b *Broker) Setup(ctx context.Context, t workload.Topology, o broker.SetupOpts) error {
	b.topo = t
	b.dedupWindow = o.DedupWindowSec
	b.lanes = o.PhysicalLanes
	if b.lanes < 1 || b.lanes > t.Properties {
		b.lanes = t.Properties
	}

	// Reset means DROP, not just reconfigure.
	//
	// Measured 2026-08-02: without this, a second run inherited the previous
	// run's messages AND its consumer-group cursors. Properties then received
	// the old run's tail before the new run's seq 0 — 32 640 "order violations"
	// that had nothing to do with the broker, and which every downstream fan-out
	// group faithfully reproduced. The other three adapters already drop and
	// recreate; this one silently did not, so every Queen run after the first
	// would have been contaminated.
	if o.Reset {
		for _, topic := range t.Topics() {
			for _, g := range t.GroupsFor(topic) {
				// Drop the group cursor explicitly: deleting a queue does not
				// necessarily retire the consumer-group state that indexes it.
				_ = b.q.DeleteConsumerGroup(ctx, g, true)
			}
			delCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			_, err := b.q.Queue(topic).Delete().Execute(delCtx)
			cancel()
			if err != nil && !isNotFound(err) {
				return fmt.Errorf("queen delete queue %s: %w", topic, err)
			}
		}
	}

	for _, topic := range t.Topics() {
		cfgCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		_, err := b.q.GetHttpClient().Post(cfgCtx, "/api/v1/configure", map[string]interface{}{
			"queue": topic,
			"options": map[string]interface{}{
				"retentionEnabled":          true,
				"completedRetentionSeconds": b.cfg.CompletedRetention,
				"retentionSeconds":          0,
				"leaseTime":                 b.cfg.LeaseTimeSec,
				"dedupWindowSeconds":        o.DedupWindowSec,
			},
		})
		cancel()
		if err != nil {
			return fmt.Errorf("queen configure %s: %w", topic, err)
		}
	}
	return nil
}

func (b *Broker) Publish(ctx context.Context, topic, key string, payload []byte) error {
	// json.RawMessage passes the bytes through the client's marshaller
	// verbatim, so every system under test sees the SAME document. Decoding it
	// into a map and re-encoding would both cost the adapter work the others do
	// not pay and risk changing the stored bytes.
	_, err := b.q.Queue(topic).Partition(b.partition(key)).
		Push([]interface{}{json.RawMessage(payload)}).Execute(ctx)
	if err != nil {
		return err
	}
	b.pushes.Add(1)
	return nil
}

func (b *Broker) PublishBatch(ctx context.Context, topic, key string, payloads [][]byte) error {
	items := make([]interface{}, 0, len(payloads))
	for _, p := range payloads {
		items = append(items, json.RawMessage(p))
	}
	// One property is one partition on every queue, so this whole batch targets
	// ONE partition: the array order is the seq order and the server appends it
	// atomically, preserving per-property total order in a single round trip.
	if _, err := b.q.Queue(topic).Partition(b.partition(key)).Push(items).Execute(ctx); err != nil {
		return err
	}
	b.pushes.Add(int64(len(payloads)))
	b.pushBatch.Add(1)
	return nil
}

func (b *Broker) Consume(ctx context.Context, topic, group string,
	o broker.ConsumeOpts, h broker.Handler) error {

	stats := o.AckStats()
	batchSize := o.BatchSize
	if batchSize < 1 {
		batchSize = 100
	}
	workers := b.cfg.PopWorkers
	if workers < 1 {
		workers = popWorkers(o.Lanes, batchSize)
	}

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		b.workers.Add(1)
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			if b.cfg.Targeted {
				b.sweepLoop(ctx, topic, group, idx, workers, batchSize, h, stats)
			} else {
				b.wildcardLoop(ctx, topic, group, batchSize, h, stats)
			}
		}(w)
	}
	wg.Wait()
	return nil
}

// popWorkers sizes the pool. Each worker owns one pop -> work -> ack cycle, and
// a cycle carries up to batchSize messages, so the pool only has to be big
// enough that workers x batchSize covers the stage's lane budget.
func popWorkers(lanes, batchSize int) int {
	w := lanes/batchSize + 4
	if w < 8 {
		w = 8
	}
	return w
}

// sweepLoop is the TARGETED path: this worker statically owns a contiguous
// slice of the partition space and sweeps it. Static ownership is what makes a
// pop cheap (no candidate scan) and what guarantees a property is never in
// flight in two handlers of the same group.
func (b *Broker) sweepLoop(ctx context.Context, topic, group string,
	idx, workers, batchSize int, h broker.Handler, stats *workload.StageCounters) {

	props := b.lanes
	// Contiguous slice [lo, hi) of the LANE space for this worker.
	per := (props + workers - 1) / workers
	lo := idx * per
	hi := lo + per
	if hi > props {
		hi = props
	}
	if lo >= props {
		return
	}

	for ctx.Err() == nil {
		start := time.Now()
		work := false
		for p := lo; p < hi && ctx.Err() == nil; p++ {
			key := b.laneName(p)
			msgs, err := b.q.Queue(topic).Group(group).Partition(key).
				Batch(batchSize).AutoAck(false).Wait(false).Pop(ctx)
			b.pops.Add(1)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				time.Sleep(5 * time.Millisecond)
				continue
			}
			if len(msgs) == 0 {
				b.emptyPops.Add(1)
				continue
			}
			work = true
			b.popped.Add(int64(len(msgs)))
			if done := b.deliver(ctx, key, msgs, h, stats); len(done) > 0 {
				b.ack(ctx, done, group, stats)
			}
		}
		// Bound the empty-pop rate when the slice is idle: without a floor a
		// worker with nothing to do spins the broker at full request rate.
		if !work {
			if rest := b.cfg.SweepFloor - time.Since(start); rest > 0 {
				select {
				case <-ctx.Done():
					return
				case <-time.After(rest):
				}
			}
		}
	}
}

// wildcardLoop is the multi-partition path. A pop claims each returned
// partition exclusively for the life of the lease, so ordering still holds; the
// cost is the server-side candidate scan, which is what this path exists to
// measure.
func (b *Broker) wildcardLoop(ctx context.Context, topic, group string,
	batchSize int, h broker.Handler, stats *workload.StageCounters) {

	for ctx.Err() == nil {
		qb := b.q.Queue(topic).Group(group).Batch(batchSize).AutoAck(b.cfg.AutoAck)
		if b.cfg.PopPartitions > 1 {
			qb = qb.Partitions(b.cfg.PopPartitions)
		}
		if b.cfg.PopWait {
			qb = qb.Wait(true).TimeoutMillis(b.cfg.PopTimeout)
		} else {
			qb = qb.Wait(false)
		}
		tPop := time.Now()
		msgs, err := qb.Pop(ctx)
		stats.ObservePopRTT(time.Since(tPop).Microseconds())
		b.pops.Add(1)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			time.Sleep(5 * time.Millisecond)
			continue
		}
		if len(msgs) == 0 {
			b.emptyPops.Add(1)
			if !b.cfg.PopWait {
				time.Sleep(2 * time.Millisecond)
			}
			continue
		}
		b.popped.Add(int64(len(msgs)))

		// A wildcard pop mixes partitions. Split by partition so the contract's
		// "one batch = one key" holds; different properties are independent and
		// run concurrently.
		byPart := map[string][]*qc.Message{}
		var order []string
		for _, m := range msgs {
			if _, seen := byPart[m.Partition]; !seen {
				order = append(order, m.Partition)
			}
			byPart[m.Partition] = append(byPart[m.Partition], m)
		}
		var wg sync.WaitGroup
		var ackMu sync.Mutex
		ackAll := make([]*qc.Message, 0, len(msgs))
		for _, part := range order {
			wg.Add(1)
			go func(key string, batch []*qc.Message) {
				defer wg.Done()
				if done := b.deliver(ctx, key, batch, h, stats); len(done) > 0 {
					ackMu.Lock()
					ackAll = append(ackAll, done...)
					ackMu.Unlock()
				}
			}(part, byPart[part])
		}
		wg.Wait()
		// ONE bulk ack for the whole pop (July cm.go parity — see deliver).
		if len(ackAll) > 0 {
			b.ack(ctx, ackAll, group, stats)
		}
	}
}

// deliver converts one partition's messages and runs the handler. It returns
// the messages to acknowledge (nil when the handler refused the batch: the
// lease expires and the batch redelivers — nothing half-applied).
//
// The ACK ITSELF IS NOT SENT HERE. July's cm.go acked the ENTIRE pop in one
// bulk call ("ack espliciti bulk per batch"); acking per partition multiplied
// the ack traffic ~14x (one request per served partition) and the measured
// system-wide cost of that extra load was ~400 ms of ingress-hop age (the
// 2026-08-03 autoAck A/B). The caller collects every partition's returned
// messages and issues ONE bulk ack per pop.
func (b *Broker) deliver(ctx context.Context, key string, msgs []*qc.Message,
	h broker.Handler, stats *workload.StageCounters) []*qc.Message {

	out := make([]broker.Message, 0, len(msgs))
	for _, m := range msgs {
		st, err := workload.StampFromMap(m.Data)
		if err != nil {
			continue
		}
		if m.RetryCount > 0 {
			b.redeliv.Add(1)
		}
		out = append(out, broker.Message{Stamp: st, Redelivery: m.RetryCount > 0})
	}
	if len(out) == 0 {
		return nil
	}
	if err := h(ctx, &broker.Batch{Key: key, Msgs: out}); err != nil {
		return nil
	}
	if b.cfg.AutoAck {
		stats.Acked.Add(int64(len(msgs))) // committed at claim; nothing to send
		return nil
	}
	return msgs
}

// ack bulk-acks a leased batch asynchronously, bounded by a shared semaphore so
// an ack is never shed — blocking here is honest back-pressure. Failures are
// counted, not retried: lease expiry into redelivery is the real behaviour, and
// a non-zero ackErr is what later excuses an ordering violation as a redelivery.
func (b *Broker) ack(ctx context.Context, msgs []*qc.Message, group string,
	stats *workload.StageCounters) {

	tSem := time.Now()
	select {
	case b.ackSem <- struct{}{}:
	case <-ctx.Done():
		return
	}
	stats.ObserveAckDisp(time.Since(tSem).Microseconds())
	go func() {
		defer func() { <-b.ackSem }()
		n := int64(len(msgs))
		// The seg broker resolves an ack's cursor per (partition, group) and
		// defaults a missing group to __QUEUE_MODE__, so a group workload MUST
		// send the group or the acks land on the wrong cursor.
		resp, err := b.q.Ack(context.WithoutCancel(ctx), msgs, true,
			qc.AckOptions{ConsumerGroup: group})
		if err != nil {
			b.ackErr.Add(n)
			stats.AckErr.Add(n)
			return
		}
		var ok int64
		for _, r := range resp {
			if r.Success {
				ok++
			}
		}
		b.acked.Add(ok)
		stats.Acked.Add(ok)
		if ok < n {
			b.ackErr.Add(n - ok)
			stats.AckErr.Add(n - ok)
		}
	}()
}

func (b *Broker) Provisioned() broker.Provisioned {
	// Charge each mode for what it actually pushes into the application. The
	// targeted entries are the direct counterpart of Kafka's parallel-consumer
	// entries: if one is listed and the other is not, the cost table lies.
	var built []string
	if b.cfg.Targeted {
		built = []string{
			"consumption mode: TARGETED pops with static partition ownership",
			"static partition-to-worker map owned by the application: the key space must be known up front and re-sharded when it changes (Queen's counterpart of Kafka's static assignment)",
			"consumers are not elastic in this mode: adding or removing workers means recomputing ownership",
			"sweep pacing owned by the application: without a floor an idle worker spins the broker at full request rate",
		}
	} else {
		built = []string{
			fmt.Sprintf("consumption mode: WILDCARD pops, up to %d partitions per pop", b.cfg.PopPartitions),
			"lanes assigned dynamically by the broker: no application-side ownership map, no advance knowledge of key cardinality",
			"cost of that flexibility is the server-side candidate scan, which is charged to Queen here and not hidden behind the targeted mode",
		}
	}
	if b.lanes < b.topo.Properties {
		built = append(built, fmt.Sprintf(
			"properties share lanes: %d properties into %d partitions (~%.1f per lane) => head-of-line blocking between properties",
			b.topo.Properties, b.lanes, float64(b.topo.Properties)/float64(b.lanes)))
	}
	return broker.Provisioned{
		OrderedLanes:   b.lanes * len(b.topo.Topics()),
		PhysicalQueues: len(b.topo.Topics()), // native consumer groups: no per-group copy
		// Workers are goroutines in a bounded pool, not group members: ordered
		// concurrency is not tied to lane count.
		ConsumerMembers:          int(b.workers.Load()),
		Connections:              1, // one pooled HTTP client
		PublishesPerIngressEvent: 2,
		BuiltSemantics:           built,
	}
}

func (b *Broker) Stats() map[string]any {
	return map[string]any{
		"queen_mode":            map[bool]string{true: "targeted", false: "wildcard"}[b.cfg.Targeted],
		"queen_pop_workers":     b.workers.Load(),
		"queen_pops":            b.pops.Load(),
		"queen_empty_pops":      b.emptyPops.Load(),
		"queen_messages_popped": b.popped.Load(),
		"queen_messages_pushed": b.pushes.Load(),
		"queen_batched_pushes":  b.pushBatch.Load(),
		"queen_acked":           b.acked.Load(),
		"queen_ack_errors":      b.ackErr.Load(),
		"queen_redelivered":     b.redeliv.Load(),
		"queen_lease_seconds":   b.cfg.LeaseTimeSec,
		"queen_dedup_window":    b.dedupWindow,
		"queen_lanes_per_queue": b.lanes,
		"queen_props_per_lane":  float64(b.topo.Properties) / float64(max(b.lanes, 1)),
	}
}

func (b *Broker) Close() error {
	return b.q.Close(context.Background())
}

// isNotFound treats "the queue was not there" as success for a reset: a first
// run on a fresh broker has nothing to drop.
func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	m := strings.ToLower(err.Error())
	return strings.Contains(m, "not found") || strings.Contains(m, "404") ||
		strings.Contains(m, "does not exist")
}

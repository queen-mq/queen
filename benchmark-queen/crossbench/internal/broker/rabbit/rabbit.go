// Package rabbit adapts RabbitMQ to the CM-BENCH contract.
//
// Two structural facts drive this adapter, and both are results rather than
// implementation details:
//
//  1. RabbitMQ has no consumer groups. The five terminal groups on a fan-out
//     topic cannot share one stream, so each group needs its OWN set of queues
//     and the publisher must send one PHYSICAL COPY per group. That is the
//     1+FanOut publish factor of SPEC.md §2 — three times the writes of a
//     system with native groups, and for flow B that is three times ~2 KB.
//
//  2. Ordering is per queue, and a queue is only ordered if ONE consumer reads
//     it with in-order processing. So the ordered lane count is the queue count:
//     12 x lanes queues for this topology, each with its own consumer channel.
//     At 1 property = 1 lane that is 12 000 queues and 12 000 consumers, which
//     is why the campaign will usually run Rabbit with fewer lanes and report
//     the head-of-line blocking that follows.
//
// Lane routing uses a direct exchange with a CLIENT-SIDE hash rather than the
// consistent-hash plugin. That is deliberate: it needs no plugin, and it gives
// the property->lane mapping exactly the same shape as Kafka's partition count,
// so the head-of-line comparison is like for like instead of two different
// hash functions. The cost is that the application owns the mapping, which is
// recorded in BuiltSemantics.
package rabbit

import (
	"context"
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"crossbench/internal/broker"
	"crossbench/internal/workload"
)

// Config tunes the adapter.
type Config struct {
	URL string

	// QueueType is "classic" or "quorum". Quorum queues are replicated and
	// fsync-backed (each is its own Raft group), which is the durability tier
	// comparable with Postgres synchronous_commit=on — and which does not
	// scale to thousands of queues. Both are run and reported (SPEC.md §5.3).
	QueueType string

	// PublishChannels is the size of the publish channel pool. A key is always
	// hashed to the SAME channel so its publish order is preserved.
	PublishChannels int

	// ChannelsPerConnection bounds channel multiplexing; the adapter opens as
	// many connections as it needs to host all consumers.
	ChannelsPerConnection int

	// BatchLinger is how long a consumer waits to fill a batch before
	// processing what it has.
	BatchLinger time.Duration

	Persistent bool
}

// DefaultConfig is the campaign baseline.
func DefaultConfig(url string) Config {
	return Config{
		URL:                   url,
		QueueType:             "classic",
		PublishChannels:       64,
		ChannelsPerConnection: 200,
		BatchLinger:           20 * time.Millisecond,
		Persistent:            true,
	}
}

type pubChan struct {
	mu sync.Mutex
	ch *amqp.Channel
}

// Broker is the RabbitMQ system under test.
type Broker struct {
	cfg   Config
	topo  workload.Topology
	lanes int

	mu       sync.Mutex
	conns    []*amqp.Connection
	chanCnt  int
	pubConn  *amqp.Connection
	pubChans []*pubChan

	queues    atomic.Int64
	consumers atomic.Int64
	published atomic.Int64
	copies    atomic.Int64
	acked     atomic.Int64
	ackErr    atomic.Int64
	redeliv   atomic.Int64
}

// New dials RabbitMQ and opens the publish channel pool.
func New(ctx context.Context, cfg Config) (*Broker, error) {
	if cfg.PublishChannels < 1 {
		cfg.PublishChannels = 1
	}
	conn, err := amqp.Dial(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("rabbit dial: %w", err)
	}
	b := &Broker{cfg: cfg, pubConn: conn}
	for i := 0; i < cfg.PublishChannels; i++ {
		ch, err := conn.Channel()
		if err != nil {
			conn.Close()
			return nil, fmt.Errorf("rabbit publish channel: %w", err)
		}
		// Publisher confirms: a Publish must not return until the broker has
		// taken responsibility, or the offered-rate accounting is a lie.
		if err := ch.Confirm(false); err != nil {
			conn.Close()
			return nil, fmt.Errorf("rabbit confirm mode: %w", err)
		}
		b.pubChans = append(b.pubChans, &pubChan{ch: ch})
	}
	return b, nil
}

func (b *Broker) Name() string { return "rabbit" }

// lane maps an ordering key onto one of the physical ordered lanes.
func (b *Broker) lane(key string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(b.lanes))
}

func laneQueue(topic, group string, lane int) string {
	return fmt.Sprintf("%s.%s.%d", topic, group, lane)
}

func laneKey(lane int) string { return fmt.Sprintf("l%d", lane) }

// groupExchange is the direct exchange that feeds ONE group's lane queues.
func groupExchange(topic, group string) string { return topic + "." + group }

func (b *Broker) Setup(ctx context.Context, t workload.Topology, o broker.SetupOpts) error {
	b.topo = t
	b.lanes = o.PhysicalLanes
	if b.lanes < 1 {
		b.lanes = t.Properties
	}

	ch, err := b.pubConn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	args := amqp.Table{}
	if b.cfg.QueueType == "quorum" {
		args["x-queue-type"] = "quorum"
	}

	for _, topic := range t.Topics() {
		groups := t.GroupsFor(topic)
		for _, g := range groups {
			ex := groupExchange(topic, g)
			if o.Reset {
				// Tear the group's exchange down first so a re-run does not
				// inherit bindings or backlog.
				_ = ch.ExchangeDelete(ex, false, false)
				for lane := 0; lane < b.lanes; lane++ {
					_, _ = ch.QueueDelete(laneQueue(topic, g, lane), false, false, false)
				}
			}
			if err := ch.ExchangeDeclare(ex, "direct", true, false, false, false, nil); err != nil {
				return fmt.Errorf("declare exchange %s: %w", ex, err)
			}
			for lane := 0; lane < b.lanes; lane++ {
				qn := laneQueue(topic, g, lane)
				if _, err := ch.QueueDeclare(qn, true, false, false, false, args); err != nil {
					return fmt.Errorf("declare queue %s: %w", qn, err)
				}
				if err := ch.QueueBind(qn, laneKey(lane), ex, false, nil); err != nil {
					return fmt.Errorf("bind queue %s: %w", qn, err)
				}
				b.queues.Add(1)
			}
		}
	}
	return nil
}

func (b *Broker) Publish(ctx context.Context, topic, key string, payload []byte) error {
	return b.PublishBatch(ctx, topic, key, [][]byte{payload})
}

// PublishBatch sends the slice, in order, to every group subscribed to the
// topic. There is no consumer-group indirection in RabbitMQ, so "fan-out to 5
// groups" literally means five physical publishes of every message.
func (b *Broker) PublishBatch(ctx context.Context, topic, key string, payloads [][]byte) error {
	groups := b.topo.GroupsFor(topic)
	if len(groups) == 0 {
		return fmt.Errorf("rabbit: no groups for topic %s", topic)
	}
	lane := b.lane(key)
	rk := laneKey(lane)

	// A key always uses the SAME channel, so its publish order is preserved
	// regardless of what other keys are doing.
	pc := b.pubChans[lane%len(b.pubChans)]
	mode := amqp.Transient
	if b.cfg.Persistent {
		mode = amqp.Persistent
	}

	pc.mu.Lock()
	defer pc.mu.Unlock()

	confirms := make([]*amqp.DeferredConfirmation, 0, len(payloads)*len(groups))
	for _, p := range payloads {
		for _, g := range groups {
			dc, err := pc.ch.PublishWithDeferredConfirmWithContext(ctx,
				groupExchange(topic, g), rk, false, false,
				amqp.Publishing{
					DeliveryMode: mode,
					ContentType:  "application/json",
					Body:         p,
					MessageId:    key,
				})
			if err != nil {
				return err
			}
			confirms = append(confirms, dc)
			b.copies.Add(1)
		}
	}
	for _, dc := range confirms {
		ok, err := dc.WaitContext(ctx)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("rabbit: publish nacked on %s", topic)
		}
	}
	b.published.Add(int64(len(payloads)))
	return nil
}

// Consume opens ONE consumer per lane queue of this stage. One consumer per
// queue is what makes the queue ordered; more than one would interleave.
func (b *Broker) Consume(ctx context.Context, topic, group string,
	o broker.ConsumeOpts, h broker.Handler) error {

	stats := o.AckStats()
	batchSize := o.BatchSize
	if batchSize < 1 {
		batchSize = 100
	}

	var wg sync.WaitGroup
	errCh := make(chan error, b.lanes)

	for lane := 0; lane < b.lanes; lane++ {
		ch, err := b.consumerChannel()
		if err != nil {
			return err
		}
		// Prefetch is what lets a single ordered consumer read a batch at all.
		if err := ch.Qos(batchSize, 0, false); err != nil {
			return fmt.Errorf("rabbit qos: %w", err)
		}
		qn := laneQueue(topic, group, lane)
		deliveries, err := ch.Consume(qn, "", false, false, false, false, nil)
		if err != nil {
			return fmt.Errorf("rabbit consume %s: %w", qn, err)
		}
		b.consumers.Add(1)

		wg.Add(1)
		go func(ch *amqp.Channel, deliveries <-chan amqp.Delivery) {
			defer wg.Done()
			defer ch.Close()
			if err := b.laneLoop(ctx, deliveries, ch, batchSize, h, stats); err != nil && ctx.Err() == nil {
				select {
				case errCh <- err:
				default:
				}
			}
		}(ch, deliveries)
	}

	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

// laneLoop reads one ordered queue, accumulates a batch, splits it by key and
// acks the whole batch with multiple=true once every key is done.
func (b *Broker) laneLoop(ctx context.Context, deliveries <-chan amqp.Delivery, ch *amqp.Channel,
	batchSize int, h broker.Handler, stats *workload.StageCounters) error {

	timer := time.NewTimer(b.cfg.BatchLinger)
	defer timer.Stop()

	for ctx.Err() == nil {
		batch := make([]amqp.Delivery, 0, batchSize)

		// Block for the first delivery, then fill up to batchSize or linger.
		select {
		case <-ctx.Done():
			return nil
		case d, ok := <-deliveries:
			if !ok {
				return nil
			}
			batch = append(batch, d)
		}

		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(b.cfg.BatchLinger)
	fill:
		for len(batch) < batchSize {
			select {
			case d, ok := <-deliveries:
				if !ok {
					break fill
				}
				batch = append(batch, d)
			case <-timer.C:
				break fill
			case <-ctx.Done():
				break fill
			}
		}
		if len(batch) == 0 {
			continue
		}

		// A lane may carry several properties when lanes < properties. Split by
		// key: distinct properties are independent for ordering, so they may run
		// concurrently, but each property's own run stays in order.
		byKey := map[string][]broker.Message{}
		var order []string
		for _, d := range batch {
			st, err := workload.DecodeStamp(d.Body)
			if err != nil {
				continue
			}
			k := d.MessageId
			if k == "" {
				k = workload.PartitionKey(st.Prop)
			}
			if _, seen := byKey[k]; !seen {
				order = append(order, k)
			}
			byKey[k] = append(byKey[k], broker.Message{
				Stamp: st, Payload: d.Body, Redelivery: d.Redelivered,
			})
			if d.Redelivered {
				b.redeliv.Add(1)
			}
		}

		var kwg sync.WaitGroup
		var failed atomic.Bool
		for _, k := range order {
			kwg.Add(1)
			go func(key string, msgs []broker.Message) {
				defer kwg.Done()
				if err := h(ctx, &broker.Batch{Key: key, Msgs: msgs}); err != nil {
					failed.Store(true)
				}
			}(k, byKey[k])
		}
		kwg.Wait()

		last := batch[len(batch)-1]
		if failed.Load() || ctx.Err() != nil {
			// Requeue the whole batch: nothing was applied. RabbitMQ puts them
			// back at the HEAD of the queue, so order survives the retry.
			if err := ch.Nack(last.DeliveryTag, true, true); err != nil {
				return err
			}
			continue
		}
		// multiple=true acks everything up to this tag in one round trip.
		if err := ch.Ack(last.DeliveryTag, true); err != nil {
			b.ackErr.Add(int64(len(batch)))
			stats.AckErr.Add(int64(len(batch)))
			return err
		}
		b.acked.Add(int64(len(batch)))
		stats.Acked.Add(int64(len(batch)))
	}
	return nil
}

// consumerChannel hands out a channel, opening a new connection whenever the
// current one has reached ChannelsPerConnection.
func (b *Broker) consumerChannel() (*amqp.Channel, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.conns) == 0 || b.chanCnt >= b.cfg.ChannelsPerConnection {
		conn, err := amqp.Dial(b.cfg.URL)
		if err != nil {
			return nil, fmt.Errorf("rabbit dial: %w", err)
		}
		b.conns = append(b.conns, conn)
		b.chanCnt = 0
	}
	ch, err := b.conns[len(b.conns)-1].Channel()
	if err != nil {
		return nil, err
	}
	b.chanCnt++
	return ch, nil
}

func (b *Broker) Provisioned() broker.Provisioned {
	b.mu.Lock()
	conns := len(b.conns) + 1
	b.mu.Unlock()

	built := []string{
		"fan-out materialised: no consumer groups, so every derived message is published once PER GROUP",
		"lane routing owned by the application (direct exchange + client-side hash)",
		"batch ack via multiple=true: a refused batch nacks and requeues whole, no partial ack",
	}
	if b.lanes < b.topo.Properties {
		built = append(built, fmt.Sprintf(
			"properties share lanes: %d properties into %d queues (~%.1f per lane) => head-of-line blocking between properties",
			b.topo.Properties, b.lanes, float64(b.topo.Properties)/float64(b.lanes)))
	}
	return broker.Provisioned{
		OrderedLanes:             int(b.queues.Load()),
		PhysicalQueues:           int(b.queues.Load()),
		ConsumerMembers:          int(b.consumers.Load()),
		Connections:              conns,
		PublishesPerIngressEvent: 1 + float64(workload.FanOut),
		BuiltSemantics:           built,
	}
}

func (b *Broker) Stats() map[string]any {
	return map[string]any{
		"rabbit_queue_type":        b.cfg.QueueType,
		"rabbit_lanes_per_group":   b.lanes,
		"rabbit_queues_declared":   b.queues.Load(),
		"rabbit_consumers":         b.consumers.Load(),
		"rabbit_logical_publishes": b.published.Load(),
		"rabbit_physical_copies":   b.copies.Load(),
		"rabbit_acked":             b.acked.Load(),
		"rabbit_ack_errors":        b.ackErr.Load(),
		"rabbit_redelivered":       b.redeliv.Load(),
		"rabbit_persistent":        b.cfg.Persistent,
	}
}

func (b *Broker) Close() error {
	b.mu.Lock()
	conns := append([]*amqp.Connection(nil), b.conns...)
	b.conns = nil
	b.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
	if b.pubConn != nil {
		_ = b.pubConn.Close()
	}
	return nil
}

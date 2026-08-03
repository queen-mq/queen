// Package kafka adapts Apache Kafka to the CM-BENCH contract.
//
// The interesting part of this adapter is what Kafka does NOT give you.
//
// Kafka's unit of ordering is the partition, and the default consumer's unit of
// concurrency is also the partition, statically assigned to one group member
// that processes it serially. The workload needs ~813 ordered lanes at 5k ev/s
// (SPEC.md §2). Getting there the default way means ~813 partitions AND ~813
// group members.
//
// The idiomatic way out is the parallel-consumer pattern: poll a chunk, split it
// by KEY, run the keys concurrently, and commit the chunk only once every key in
// it is done. That decouples ordered concurrency from member count — which is
// the fair way to run Kafka here, and is also application code that Kafka makes
// you write and then own. It is recorded in Provisioned.BuiltSemantics, because
// "you must write and operate a parallel consumer" is a real cost of the choice.
//
// Consequences the campaign should expect to see and must report honestly:
//   - a slow key holds back the offset commit for its whole partition chunk, so
//     the redelivery window on a crash is as wide as the slowest key;
//   - a rebalance re-delivers everything uncommitted, which the verifier reads
//     as duplicates — this is Kafka behaving as designed, not a defect;
//   - with fewer partitions than properties, properties SHARE a lane and block
//     each other. That is measured, not hidden: set SetupOpts.PhysicalLanes.
package kafka

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"crossbench/internal/broker"
	"crossbench/internal/workload"
)

// Config tunes the adapter. Every non-default value must be justified in the
// run log (SPEC.md §5.2).
type Config struct {
	Seeds []string

	// MembersPerGroup is how many consumer-group members to run per stage.
	// With the parallel-consumer pattern this does NOT have to equal the lane
	// count; it only has to be enough to spread partitions and keep the poll
	// loops from becoming the bottleneck.
	MembersPerGroup int

	// Linger batches concurrent producers together. Each Publish still waits
	// for its own ack, so the offered-rate accounting stays honest.
	Linger time.Duration

	// Durability: "default" leaves Kafka's own flush policy (replication-based,
	// NOT fsync per write) and "fsync" forces flush.messages=1 so the tier
	// matches a Postgres synchronous_commit=on run. Never compare across the
	// two silently (SPEC.md §5.3).
	Durability string

	ReplicationFactor int
	RetentionMs       int64
	FetchMaxWait      time.Duration
	MaxPollRecords    int
}

// DefaultConfig is the campaign baseline.
func DefaultConfig(seeds []string) Config {
	return Config{
		Seeds:             seeds,
		MembersPerGroup:   4,
		Linger:            5 * time.Millisecond,
		Durability:        "default",
		ReplicationFactor: 1,
		RetentionMs:       3600_000,
		FetchMaxWait:      50 * time.Millisecond,
		MaxPollRecords:    2000,
	}
}

// Broker is the Kafka system under test.
type Broker struct {
	cfg   Config
	topo  workload.Topology
	lanes int

	prod *kgo.Client

	mu        sync.Mutex
	consumers []*kgo.Client

	members   atomic.Int64
	polls     atomic.Int64
	fetched   atomic.Int64
	commits   atomic.Int64
	commitErr atomic.Int64
	rebalance atomic.Int64
	keyBatch  atomic.Int64
	seeks     atomic.Int64
}

// New connects a producer and validates the cluster is reachable.
func New(ctx context.Context, cfg Config) (*Broker, error) {
	if cfg.MembersPerGroup < 1 {
		cfg.MembersPerGroup = 1
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Seeds...),
		kgo.ProducerLinger(cfg.Linger),
		// Idempotence is ON (franz-go's default) and must stay on: without it a
		// producer retry can reorder records within a key, which the verifier
		// would report as an ordering violation caused by configuration rather
		// than by Kafka.
		kgo.RequiredAcks(kgo.AllISRAcks()),
		// Java-compatible key hashing, so property -> partition is what a Java
		// application would get.
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	if err := cl.Ping(ctx); err != nil {
		cl.Close()
		return nil, fmt.Errorf("kafka ping: %w", err)
	}
	return &Broker{cfg: cfg, prod: cl}, nil
}

func (b *Broker) Name() string { return "kafka" }

func (b *Broker) Setup(ctx context.Context, t workload.Topology, o broker.SetupOpts) error {
	b.topo = t
	b.lanes = o.PhysicalLanes
	if b.lanes < 1 {
		b.lanes = t.Properties
	}

	adm := kadm.NewClient(b.prod)
	topics := t.Topics()

	if o.Reset {
		if _, err := adm.DeleteTopics(ctx, topics...); err != nil {
			return fmt.Errorf("delete topics: %w", err)
		}
		// Deletion is asynchronous; creating immediately races it.
		time.Sleep(3 * time.Second)
	}

	cfgs := map[string]*string{
		"retention.ms": strptr(fmt.Sprintf("%d", b.cfg.RetentionMs)),
	}
	if b.cfg.Durability == "fsync" {
		// Force a flush per record so the durability tier matches a Postgres
		// synchronous_commit=on run. This is NOT how Kafka is normally deployed
		// and the result must always say which tier it came from.
		cfgs["flush.messages"] = strptr("1")
		cfgs["flush.ms"] = strptr("0")
	}

	resp, err := adm.CreateTopics(ctx, int32(b.lanes), int16(b.cfg.ReplicationFactor), cfgs, topics...)
	if err != nil {
		return fmt.Errorf("create topics: %w", err)
	}
	for _, r := range resp {
		if r.Err != nil && !isTopicExists(r.Err) {
			return fmt.Errorf("create topic %s: %w", r.Topic, r.Err)
		}
	}
	return nil
}

func (b *Broker) Publish(ctx context.Context, topic, key string, payload []byte) error {
	rec := &kgo.Record{Topic: topic, Key: []byte(key), Value: payload}
	return b.prod.ProduceSync(ctx, rec).FirstErr()
}

func (b *Broker) PublishBatch(ctx context.Context, topic, key string, payloads [][]byte) error {
	recs := make([]*kgo.Record, 0, len(payloads))
	for _, p := range payloads {
		recs = append(recs, &kgo.Record{Topic: topic, Key: []byte(key), Value: p})
	}
	// One key means one partition, and an idempotent producer preserves the
	// order of records it batches, so the slice order survives the hop.
	return b.prod.ProduceSync(ctx, recs...).FirstErr()
}

// Consume runs MembersPerGroup group members for one stage. Each member runs
// the parallel-consumer loop described in the package comment.
func (b *Broker) Consume(ctx context.Context, topic, group string,
	o broker.ConsumeOpts, h broker.Handler) error {

	stats := o.AckStats()
	var wg sync.WaitGroup
	errCh := make(chan error, b.cfg.MembersPerGroup)

	for i := 0; i < b.cfg.MembersPerGroup; i++ {
		cl, err := kgo.NewClient(
			kgo.SeedBrokers(b.cfg.Seeds...),
			kgo.ConsumeTopics(topic),
			kgo.ConsumerGroup(group),
			kgo.DisableAutoCommit(),
			// Hold the rebalance until the current poll's work is committed:
			// without this a rebalance mid-chunk silently hands our in-flight
			// keys to someone else and manufactures duplicates we would then
			// wrongly attribute to the workload.
			kgo.BlockRebalanceOnPoll(),
			kgo.FetchMaxWait(b.cfg.FetchMaxWait),
			kgo.OnPartitionsRevoked(func(context.Context, *kgo.Client, map[string][]int32) {
				b.rebalance.Add(1)
			}),
		)
		if err != nil {
			return err
		}
		b.mu.Lock()
		b.consumers = append(b.consumers, cl)
		b.mu.Unlock()
		b.members.Add(1)

		wg.Add(1)
		go func() {
			defer wg.Done()
			defer cl.Close()
			if err := b.memberLoop(ctx, cl, o, h, stats); err != nil && ctx.Err() == nil {
				select {
				case errCh <- err:
				default:
				}
			}
		}()
	}

	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

// memberLoop is the parallel consumer: poll a chunk, split it by key, run the
// keys concurrently, commit only when every key in the chunk is done.
func (b *Broker) memberLoop(ctx context.Context, cl *kgo.Client, o broker.ConsumeOpts,
	h broker.Handler, stats *workload.StageCounters) error {

	batchSize := o.BatchSize
	if batchSize < 1 {
		batchSize = 100
	}
	for ctx.Err() == nil {
		fetches := cl.PollRecords(ctx, b.cfg.MaxPollRecords)
		if fetches.IsClientClosed() {
			return nil
		}
		if err := ctx.Err(); err != nil {
			cl.AllowRebalance()
			return nil
		}
		var fetchErr error
		fetches.EachError(func(t string, p int32, err error) {
			if fetchErr == nil {
				fetchErr = fmt.Errorf("fetch %s/%d: %w", t, p, err)
			}
		})
		if fetchErr != nil {
			cl.AllowRebalance()
			if ctx.Err() != nil {
				return nil
			}
			return fetchErr
		}
		b.polls.Add(1)

		// Group the chunk by KEY. Kafka guarantees partition order; the
		// application only needs per-key order, so distinct keys may run
		// concurrently even when they share a partition.
		// Record where each partition's chunk STARTED. Not committing offsets is
		// NOT a replay: franz-go tracks its consumed position in memory, so the
		// next poll returns the NEXT records and a refused chunk is silently
		// SKIPPED until a rebalance. Queen, pgmq and RabbitMQ all genuinely
		// redeliver, so without an explicit seek Kafka would be the only system
		// that loses a refused batch — and would score better for it.
		chunkStart := map[string]map[int32]kgo.EpochOffset{}
		byKey := map[string][]broker.Message{}
		var order []string
		n := 0
		fetches.EachRecord(func(r *kgo.Record) {
			if _, ok := chunkStart[r.Topic]; !ok {
				chunkStart[r.Topic] = map[int32]kgo.EpochOffset{}
			}
			if _, seen := chunkStart[r.Topic][r.Partition]; !seen {
				chunkStart[r.Topic][r.Partition] = kgo.EpochOffset{
					Epoch: r.LeaderEpoch, Offset: r.Offset,
				}
			}
			st, err := workload.DecodeStamp(r.Value)
			if err != nil {
				return
			}
			k := string(r.Key)
			if _, seen := byKey[k]; !seen {
				order = append(order, k)
			}
			byKey[k] = append(byKey[k], broker.Message{Stamp: st, Payload: r.Value})
			n++
		})
		if n == 0 {
			cl.AllowRebalance()
			continue
		}
		b.fetched.Add(int64(n))

		var kwg sync.WaitGroup
		var failed atomic.Bool
		for _, k := range order {
			msgs := byKey[k]
			b.keyBatch.Add(1)
			kwg.Add(1)
			go func(key string, msgs []broker.Message) {
				defer kwg.Done()
				// Respect the caller's batch cap while keeping seq order.
				for start := 0; start < len(msgs); start += batchSize {
					end := start + batchSize
					if end > len(msgs) {
						end = len(msgs)
					}
					if err := h(ctx, &broker.Batch{Key: key, Msgs: msgs[start:end]}); err != nil {
						failed.Store(true)
						return
					}
				}
			}(k, msgs)
		}
		kwg.Wait()

		if failed.Load() || ctx.Err() != nil {
			// Seek every partition back to where this chunk began, then commit
			// nothing. Kafka has no partial ack, so one refused key costs a
			// replay of everything polled with it — a real property of this
			// design, and a cost worth reporting. The seek is what makes it a
			// replay rather than a silent loss.
			if ctx.Err() == nil {
				cl.SetOffsets(chunkStart)
				b.seeks.Add(1)
			}
			cl.AllowRebalance()
			continue
		}
		if err := cl.CommitUncommittedOffsets(ctx); err != nil {
			b.commitErr.Add(1)
			stats.AckErr.Add(int64(n))
		} else {
			b.commits.Add(1)
			stats.Acked.Add(int64(n))
		}
		cl.AllowRebalance()
	}
	return nil
}

func (b *Broker) Provisioned() broker.Provisioned {
	b.mu.Lock()
	conns := len(b.consumers) + 1
	b.mu.Unlock()

	built := []string{
		"parallel-consumer: poll chunk, split by key, run keys concurrently, commit chunk (ordered concurrency is not native)",
		"no partial ack: a refused key replays the whole polled chunk, and the replay must be forced with an explicit seek (skipping the commit alone silently DROPS it)",
	}
	if b.lanes < b.topo.Properties {
		built = append(built, fmt.Sprintf(
			"properties share lanes: %d properties hashed into %d partitions (~%.1f per lane) => head-of-line blocking between properties",
			b.topo.Properties, b.lanes, float64(b.topo.Properties)/float64(b.lanes)))
	}
	return broker.Provisioned{
		OrderedLanes:    b.lanes * len(b.topo.Topics()),
		PhysicalQueues:  len(b.topo.Topics()), // native consumer groups: no per-group copy
		ConsumerMembers: int(b.members.Load()),
		Connections:     conns,
		// Native fan-out: the derived event is published ONCE and read by all
		// five groups independently.
		PublishesPerIngressEvent: 2,
		BuiltSemantics:           built,
	}
}

func (b *Broker) Stats() map[string]any {
	return map[string]any{
		"kafka_partitions_per_topic": b.lanes,
		"kafka_polls":                b.polls.Load(),
		"kafka_records_fetched":      b.fetched.Load(),
		"kafka_key_batches":          b.keyBatch.Load(),
		"kafka_offset_commits":       b.commits.Load(),
		"kafka_offset_commit_errors": b.commitErr.Load(),
		"kafka_partitions_revoked":   b.rebalance.Load(),
		"kafka_replay_seeks":         b.seeks.Load(),
		"kafka_durability":           b.cfg.Durability,
		"kafka_members_per_group":    b.cfg.MembersPerGroup,
	}
}

func (b *Broker) Close() error {
	b.mu.Lock()
	cs := append([]*kgo.Client(nil), b.consumers...)
	b.consumers = nil
	b.mu.Unlock()
	for _, c := range cs {
		c.Close()
	}
	b.prod.Close()
	return nil
}

func strptr(s string) *string { return &s }

func isTopicExists(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "TOPIC_ALREADY_EXISTS") || strings.Contains(msg, "already exists")
}

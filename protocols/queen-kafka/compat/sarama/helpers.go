package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

// ------------------------------------------------------- partition consumers

// consumePartitions reads `want` records off `topic` with plain partition
// consumers (no group, so no 3s join delay), at the suite's Config.Version.
func consumePartitions(e env, topic string, parts int32, want int, timeout time.Duration) ([]*sarama.ConsumerMessage, error) {
	cfg := newConfig(cfgOpts{clientID: "qk-sarama-reader", version: theVersion, apiVersionsReq: true})
	return consumePartitionsCfg(cfg, e, topic, parts, want, timeout)
}

func consumePartitionsCfg(cfg *sarama.Config, e env, topic string, parts int32, want int, timeout time.Duration) ([]*sarama.ConsumerMessage, error) {
	return consumePartitionsAt(e.bootstrap, cfg, topic, parts, want, timeout)
}

func consumePartitionsAt(addr string, cfg *sarama.Config, topic string, parts int32, want int, timeout time.Duration) ([]*sarama.ConsumerMessage, error) {
	cons, err := sarama.NewConsumer([]string{addr}, cfg)
	if err != nil {
		return nil, fmt.Errorf("NewConsumer: %w", err)
	}
	defer func() { _ = cons.Close() }()

	var (
		mu   sync.Mutex
		out  []*sarama.ConsumerMessage
		wg   sync.WaitGroup
		errs []error
	)
	deadline := time.After(timeout)
	stop := make(chan struct{})
	var stopOnce sync.Once
	closeStop := func() { stopOnce.Do(func() { close(stop) }) }

	for p := int32(0); p < parts; p++ {
		pc, err := cons.ConsumePartition(topic, p, sarama.OffsetOldest)
		if err != nil {
			// A partition with nothing in it is not an error; a missing topic is.
			return nil, fmt.Errorf("ConsumePartition(%s, %d): %w", topic, p, err)
		}
		wg.Add(1)
		go func(pc sarama.PartitionConsumer) {
			defer wg.Done()
			defer func() { _ = pc.Close() }()
			for {
				select {
				case m, ok := <-pc.Messages():
					if !ok {
						return
					}
					mu.Lock()
					out = append(out, m)
					n := len(out)
					mu.Unlock()
					if n >= want {
						closeStop()
						return
					}
				case e, ok := <-pc.Errors():
					if !ok {
						return
					}
					mu.Lock()
					errs = append(errs, e)
					mu.Unlock()
				case <-stop:
					return
				}
			}
		}(pc)
	}

	select {
	case <-stop:
	case <-deadline:
		closeStop()
		wg.Wait()
		mu.Lock()
		defer mu.Unlock()
		if len(errs) > 0 {
			return out, fmt.Errorf("timed out with %d of %d records; first error: %w", len(out), want, errs[0])
		}
		return out, fmt.Errorf("timed out with %d of %d records", len(out), want)
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	sort.Slice(out, func(i, j int) bool {
		if out[i].Partition != out[j].Partition {
			return out[i].Partition < out[j].Partition
		}
		return out[i].Offset < out[j].Offset
	})
	if len(errs) > 0 {
		return out, errs[0]
	}
	return out, nil
}

// compareAll matches consumed messages to fixtures by (partition, offset) and
// returns every difference it found.
func compareAll(fx []*fixture, got []*sarama.ConsumerMessage) []string {
	index := map[[2]int64]*sarama.ConsumerMessage{}
	for _, m := range got {
		index[[2]int64{int64(m.Partition), m.Offset}] = m
	}
	var bad []string
	for _, f := range fx {
		m, ok := index[[2]int64{int64(f.partition), f.offset}]
		if !ok {
			bad = append(bad, fmt.Sprintf("p%d@%d (%s) never arrived", f.partition, f.offset, f.label))
			continue
		}
		for _, b := range f.compare(m) {
			bad = append(bad, fmt.Sprintf("p%d@%d (%s): %s", f.partition, f.offset, f.label, b))
		}
	}
	return bad
}

// -------------------------------------------------------------- consumer group

type collector struct {
	want int
	mark bool

	mu          sync.Mutex
	msgs        []*sarama.ConsumerMessage
	generations int
	claims      map[string][]int32
	sess        sarama.ConsumerGroupSession
	firstAt     time.Duration
	start       time.Time

	doneOnce sync.Once
	done     chan struct{}
}

func (c *collector) Setup(s sarama.ConsumerGroupSession) error {
	c.mu.Lock()
	c.generations++
	c.claims = s.Claims()
	c.sess = s
	c.mu.Unlock()
	return nil
}

func (c *collector) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (c *collector) ConsumeClaim(s sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for m := range claim.Messages() {
		c.mu.Lock()
		if len(c.msgs) == 0 {
			c.firstAt = time.Since(c.start)
		}
		c.msgs = append(c.msgs, m)
		n := len(c.msgs)
		c.mu.Unlock()
		if c.mark {
			s.MarkMessage(m, "")
		}
		if c.want > 0 && n >= c.want {
			c.doneOnce.Do(func() { close(c.done) })
		}
	}
	return nil
}

func (c *collector) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.msgs)
}

// runGroup joins `group`, reads until `want` records have arrived (or, when
// want is 0, until a quiet period has passed proving nothing does), commits the
// marked offsets, and leaves cleanly. Every consumer group formation costs the
// facade's QUEEN_KAFKA_GROUP_JOIN_DELAY_MS — 3 seconds by default — so the
// timeout has to be generous.
func runGroup(e env, group string, topics []string, want int, timeout time.Duration, markAndCommit bool) (*collector, error) {
	cfg := newConfig(cfgOpts{clientID: "qk-sarama-group", version: theVersion, apiVersionsReq: true})
	cg, err := sarama.NewConsumerGroup(brokers(e), group, cfg)
	if err != nil {
		return nil, fmt.Errorf("NewConsumerGroup: %w", err)
	}

	col := &collector{want: want, mark: markAndCommit, done: make(chan struct{}), start: time.Now()}
	ctx, cancel := context.WithCancel(context.Background())

	var (
		loopErrMu sync.Mutex
		loopErrs  []error
		wg        sync.WaitGroup
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			if err := cg.Consume(ctx, topics, col); err != nil {
				if errors.Is(err, sarama.ErrClosedConsumerGroup) || ctx.Err() != nil {
					return
				}
				loopErrMu.Lock()
				loopErrs = append(loopErrs, err)
				loopErrMu.Unlock()
				time.Sleep(250 * time.Millisecond)
			}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for e := range cg.Errors() {
			loopErrMu.Lock()
			loopErrs = append(loopErrs, e)
			loopErrMu.Unlock()
		}
	}()

	var waitErr error
	if want > 0 {
		select {
		case <-col.done:
			// Let a straggler or a duplicate show up before we stop looking.
			time.Sleep(1500 * time.Millisecond)
		case <-time.After(timeout):
			waitErr = fmt.Errorf("timed out with %d of %d records after %s", col.count(), want, timeout)
		}
	} else {
		time.Sleep(timeout)
	}

	// Commit what was marked, then leave. Autocommit is on (sarama's default)
	// and Close() commits again on the way out; the explicit Commit is what
	// makes the moment deterministic.
	if markAndCommit {
		col.mu.Lock()
		s := col.sess
		col.mu.Unlock()
		if s != nil {
			func() {
				defer func() { _ = recover() }()
				s.Commit()
			}()
		}
	}
	cancel()
	_ = cg.Close()
	wg.Wait()

	loopErrMu.Lock()
	defer loopErrMu.Unlock()
	if waitErr != nil {
		if len(loopErrs) > 0 {
			return col, fmt.Errorf("%w; first client error: %v", waitErr, loopErrs[0])
		}
		return col, waitErr
	}
	if len(loopErrs) > 0 {
		// Rebalance-class errors are normal; anything else is not, but the
		// caller decides. Report them and let the counts speak.
		return col, nil
	}
	return col, nil
}

// fetchCommitted reads a group's committed offsets back off the broker with
// OffsetFetch, through sarama's own offset manager.
func fetchCommitted(e env, group, topic string, parts int32) (map[int32]int64, error) {
	cfg := newConfig(cfgOpts{clientID: "qk-sarama-offsetfetch", version: theVersion, apiVersionsReq: true})
	client, err := sarama.NewClient(brokers(e), cfg)
	if err != nil {
		return nil, fmt.Errorf("NewClient: %w", err)
	}
	defer func() { _ = client.Close() }()

	om, err := sarama.NewOffsetManagerFromClient(group, client)
	if err != nil {
		return nil, fmt.Errorf("NewOffsetManagerFromClient: %w", err)
	}
	defer func() { _ = om.Close() }()

	out := map[int32]int64{}
	for p := int32(0); p < parts; p++ {
		pom, err := om.ManagePartition(topic, p)
		if err != nil {
			return nil, fmt.Errorf("ManagePartition(%s, %d): %w", topic, p, err)
		}
		off, _ := pom.NextOffset()
		out[p] = off
		_ = pom.Close()
	}
	return out, nil
}

// ---------------------------------------------------------------- the probes

// probeRoundTrip produces one record to each of the first min(parts,4)
// partitions at `v` and reads them back, with a hard deadline. It is the
// smallest thing that exercises Metadata, Produce, ListOffsets and Fetch, which
// is exactly the set whose versions a Config.Version choice moves.
func probeRoundTrip(e env, v sarama.KafkaVersion, tap *wireTap, topic string, parts int32) error {
	cfg := newConfig(cfgOpts{clientID: "qk-sarama-probe", version: v, apiVersionsReq: true, tap: tap})
	return probeRoundTripCfg(e, cfg, topic, parts)
}

func probeRoundTripCfg(e env, cfg *sarama.Config, topic string, parts int32) error {
	return probeRoundTripAt(e.bootstrap, cfg, topic, parts)
}

func probeRoundTripAt(addr string, cfg *sarama.Config, topic string, parts int32) error {
	n := int(parts)
	if n > 4 {
		n = 4
	}
	// Fail fast: a version outside the facade's window is answered by a closed
	// connection, and without these a client would retry it for a minute.
	cfg.Metadata.Retry.Max = 2
	cfg.Metadata.Retry.Backoff = 200 * time.Millisecond
	cfg.Producer.Retry.Max = 2
	cfg.Producer.Retry.Backoff = 200 * time.Millisecond
	cfg.Consumer.Retry.Backoff = 200 * time.Millisecond
	cfg.Producer.Partitioner = sarama.NewManualPartitioner
	if cfg.Version.IsAtLeast(sarama.V0_11_0_0) {
		// Headers need the record batch; below that they are silently dropped
		// by sarama, so the probe does not send any.
		cfg.Producer.Return.Successes = true
	}

	return withTimeout(60*time.Second, func() error {
		p, err := sarama.NewSyncProducer([]string{addr}, cfg)
		if err != nil {
			return fmt.Errorf("NewSyncProducer: %w", err)
		}
		msgs := make([]*sarama.ProducerMessage, 0, n)
		for i := 0; i < n; i++ {
			msgs = append(msgs, &sarama.ProducerMessage{
				Topic:     topic,
				Partition: int32(i),
				Key:       sarama.ByteEncoder([]byte(fmt.Sprintf("probe-%d", i))),
				Value:     sarama.ByteEncoder([]byte(fmt.Sprintf("probe payload %d", i))),
			})
		}
		if err := p.SendMessages(msgs); err != nil {
			_ = p.Close()
			return fmt.Errorf("produce: %w", err)
		}
		_ = p.Close()

		got, err := consumePartitionsAt(addr, cfg, topic, int32(n), n, 25*time.Second)
		if err != nil {
			return fmt.Errorf("consume: %w", err)
		}
		if len(got) != n {
			return fmt.Errorf("consume: %d of %d records", len(got), n)
		}
		return nil
	})
}

func withTimeout(d time.Duration, fn func() error) error {
	done := make(chan error, 1)
	go func() {
		defer func() {
			if p := recover(); p != nil {
				done <- fmt.Errorf("panic: %v", p)
			}
		}()
		done <- fn()
	}()
	select {
	case err := <-done:
		return err
	case <-time.After(d):
		return fmt.Errorf("HUNG: no return within %s", d)
	}
}

func dialerFor(cfg *sarama.Config) *net.Dialer {
	return &net.Dialer{Timeout: cfg.Net.DialTimeout, KeepAlive: cfg.Net.KeepAlive}
}

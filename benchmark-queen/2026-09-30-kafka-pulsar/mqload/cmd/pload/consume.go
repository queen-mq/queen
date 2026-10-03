package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"

	"mqload/internal/core"
)

type pconsumer struct {
	ci   int
	c    pulsar.Consumer
	cs   *core.ConsumerStats
	proc *core.ProcSim
}

type consumers struct {
	run    *core.Run
	pc     *pulsarFlags
	client pulsar.Client
	list   []*pconsumer
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	names     []string       // the topics these consumers read (default: the -topic names; -txn readers: <topic>-out)
	readerTxn *core.TxnStats // -txn readers: activity for the drain's idle test
}

func newConsumers(run *core.Run, pc *pulsarFlags, client pulsar.Client) *consumers {
	c := &consumers{run: run, pc: pc, client: client, names: run.Cfg.TopicNames()}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	return c
}

// start subscribes every consumer and starts its receive loop. Topics per the
// SPEC §2 rule; for failover and exclusive subscriptions on partitioned topics
// (-consumer-slices, default) a consumer subscribes only to ITS partitions
// (core.ConsumerPartitions: explicit <topic>-partition-<p> topics, one
// multi-topic consumer), so every partition has exactly one consumer in total,
// the shape of a Kafka group; shared and key_shared subscribe the whole topic
// (they must share partitions). Subscribe returns once every partition
// consumer is connected.
func (c *consumers) start(ctx context.Context) error {
	cfg, pc := c.run.Cfg, c.pc
	if cfg.Consumers == 0 {
		return nil
	}
	c.list = make([]*pconsumer, cfg.Consumers)
	regs := make([]int, cfg.Consumers)
	t := time.Now()
	err := parallel(ctx, cfg.Consumers, pc.adminConc, func(ctx context.Context, i int) error {
		ci := cfg.ConsOffset + i
		topics := c.topicsOf(ci)
		regs[i] = registrations(cfg, topics, c.sliced())
		if len(topics) == 0 {
			return nil // more consumers than partitions: this one owns none
		}
		opts := pulsar.ConsumerOptions{
			SubscriptionName:               pc.sub,
			Type:                           pc.subTypeV,
			Name:                           fmt.Sprintf("c%05d", ci), // failover orders consumers by name
			ReceiverQueueSize:              pc.receiverQueue,
			AckGroupingOptions:             &pulsar.AckGroupingOptions{MaxSize: 1000, MaxTime: pc.ackGroupTime},
			EnableBatchIndexAcknowledgment: true,
			SubscriptionInitialPosition:    pulsar.SubscriptionPositionEarliest,
			// the partition consumers' dispatchers feed this channel; the client
			// default (10) makes them hand off one message at a time
			MessageChannel: make(chan pulsar.ConsumerMessage, max(pc.receiverQueue, 10)),
		}
		if len(topics) == 1 {
			opts.Topic = topics[0]
		} else {
			opts.Topics = topics
		}
		cons, err := c.client.Subscribe(opts)
		if err != nil {
			return fmt.Errorf("subscribe c%d to %d topics (%s ...): %v", ci, len(topics), topics[0], err)
		}
		c.list[i] = &pconsumer{ci: ci, c: cons, cs: c.run.NewConsumerStats(), proc: core.NewProcSim(cfg.ProcUs)}
		return nil
	})
	if err != nil {
		return err
	}
	attached, idle, here := 0, 0, 0
	for i, k := range c.list {
		here += regs[i]
		if k == nil {
			idle++
			continue
		}
		c.wg.Add(1)
		go c.loop(k)
		attached++
	}
	s := time.Since(t).Seconds()
	total, idleTotal := c.totals()
	layout := "whole topics (every consumer on every partition of its topics)"
	if c.sliced() {
		layout = "partition slices (every partition exactly one consumer)"
	}
	fmt.Printf("  [consume] %d consumers (ci %d..%d of %d), %s, %s: %d partition registrations here, %d in total across all processes; idle %d here, %d in total; subscribed in %.1fs, settling %v\n",
		attached+idle, cfg.ConsOffset, cfg.ConsOffset+cfg.Consumers-1, cfg.ConsTotal, pc.subType, layout, here, total, idle, idleTotal, s, pc.settle)
	c.run.SetInfo("subscribe_s", s)
	c.run.SetInfo("consumer_layout", layout)
	c.run.SetInfo("consumer_registrations_here", here)
	c.run.SetInfo("consumer_registrations_total", total)
	c.run.SetInfo("consumers_idle", idle)
	select {
	case <-ctx.Done():
	case <-time.After(pc.settle):
	}
	return nil
}

// sliced: failover/exclusive consumers split the partitions (-consumer-slices).
func (c *consumers) sliced() bool {
	return c.pc.consumerSlices && c.run.Cfg.Partitions > 0 && (c.pc.subType == "failover" || c.pc.subType == "exclusive")
}

// topicsOf returns the Pulsar topics consumer ci subscribes to, deterministic.
func (c *consumers) topicsOf(ci int) []string {
	cfg, pc := c.run.Cfg, c.pc
	names := c.names
	if names == nil {
		names = cfg.TopicNames()
	}
	var out []string
	for _, t := range cfg.ConsumerTopics(ci) {
		if !c.sliced() {
			out = append(out, pc.fullName(names[t]))
			continue
		}
		for _, p := range cfg.ConsumerPartitions(ci, t) {
			out = append(out, pc.fullName(fmt.Sprintf("%s-partition-%d", names[t], p)))
		}
	}
	return out
}

// registrations: broker-side consumer registrations of one consumer (one per
// partition it is attached to).
func registrations(cfg *core.Config, topics []string, sliced bool) int {
	if sliced || cfg.Partitions == 0 {
		return len(topics)
	}
	return len(topics) * cfg.Partitions
}

// totals: registrations and idle consumers across ALL processes (every
// consumer index 0..cons-total-1).
func (c *consumers) totals() (regs, idle int) {
	cfg := c.run.Cfg
	for ci := 0; ci < cfg.ConsTotal; ci++ {
		n := 0
		for _, t := range cfg.ConsumerTopics(ci) {
			switch {
			case c.sliced():
				n += len(cfg.ConsumerPartitions(ci, t))
			case cfg.Partitions == 0:
				n++
			default:
				n += cfg.Partitions
			}
		}
		regs += n
		if n == 0 {
			idle++
		}
	}
	return regs, idle
}

// loop: take what the receive channel holds (up to -poll), e2e per message,
// -proc-us, then Ack each message (client ack grouping, batch-index acks).
func (c *consumers) loop(k *pconsumer) {
	defer c.wg.Done()
	cfg := c.run.Cfg
	ch := k.c.Chan()
	batch := make([]pulsar.ConsumerMessage, 0, cfg.Poll)
	for {
		select {
		case <-c.ctx.Done():
			return
		case cm, ok := <-ch:
			if !ok {
				return
			}
			batch = append(batch[:0], cm)
		}
	drain:
		for len(batch) < cfg.Poll {
			select {
			case cm, ok := <-ch:
				if !ok {
					break drain
				}
				batch = append(batch, cm)
			default:
				break drain
			}
		}
		now := time.Now().UnixMicro()
		load, warm := 0, 0
		for _, cm := range batch {
			if k.cs.Observe(cm.Payload(), now) {
				load++
			} else {
				warm++
			}
		}
		k.cs.Polled(load, warm)
		if c.readerTxn != nil {
			c.readerTxn.ReaderSaw()
		}
		k.proc.Add(len(batch))
		if cfg.Ack == "none" {
			continue
		}
		t := time.Now()
		var nerr int
		var ferr error
		for _, cm := range batch {
			if err := k.c.Ack(cm.Message); err != nil {
				nerr++
				if ferr == nil {
					ferr = err
				}
			}
		}
		lat := time.Since(t)
		if nerr > 0 {
			bad := min(nerr, load)
			k.cs.AckDone(bad, 0, ferr)
			load -= bad
		}
		k.cs.AckDone(load, lat, nil)
	}
}

// stop ends the receive loops and closes the consumers (flushing the grouped
// acks) in parallel.
func (c *consumers) stop() {
	c.cancel()
	c.wg.Wait()
	var wg sync.WaitGroup
	for _, k := range c.list {
		if k == nil {
			continue
		}
		wg.Add(1)
		go func(k *pconsumer) { defer wg.Done(); k.c.Close() }(k)
	}
	wg.Wait()
}

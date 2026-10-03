package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"mqload/internal/core"
)

// consumers: plain-mode consumers of <topic>, or -txn readers of <topic>-out. Closed loop: pop (queue mode, leased,
// -pop-width partitions, -pop-batch, long poll), e2e per message, -proc-us, then ack the batch asynchronously under
// the process-wide -ack-inflight cap (full = block, never shed): goload -manual-ack -ack-async.
type consumers struct {
	run   *core.Run
	cli   *qhttp
	qf    *queenFlags
	queue string
	n     int
	stats *core.TxnStats // -txn: reader activity for the drain's idle test

	ctx, ackCtx       context.Context
	cancel, ackCancel context.CancelFunc
	wg, ackWg         sync.WaitGroup
}

func (c *consumers) start() {
	c.ctx, c.cancel = context.WithCancel(context.Background())
	c.ackCtx, c.ackCancel = context.WithCancel(context.Background())
	for i := 0; i < c.n; i++ {
		cs := c.run.NewConsumerStats()
		c.wg.Add(1)
		go c.loop(cs)
	}
	if c.n > 0 {
		fmt.Printf("  [consume] %d consumers of %s: pop width %d, batch %d, long poll %v, lease %ds, async batch acks (ack-inflight %d)\n",
			c.n, c.queue, c.qf.popWidth, c.qf.popBatch, c.qf.popTimeout, c.qf.lease, cap(c.run.AckSem))
	}
}

func (c *consumers) loop(cs *core.ConsumerStats) {
	defer c.wg.Done()
	cfg := c.run.Cfg
	proc := core.NewProcSim(cfg.ProcUs)
	req := popReq{queue: c.queue, batch: c.qf.popBatch, width: c.qf.popWidth, wait: true,
		timeoutMs: int(c.qf.popTimeout.Milliseconds()), autoAck: false, leaseS: c.qf.lease}
	for c.ctx.Err() == nil {
		ms, err := c.cli.pop(c.ctx, req)
		if err != nil {
			if c.ctx.Err() != nil {
				return
			}
			cs.PopErr(fmt.Errorf("pop %s: %w", c.queue, err))
			time.Sleep(5 * time.Millisecond)
			continue
		}
		if len(ms) == 0 {
			cs.Empty()
			continue
		}
		now := time.Now().UnixMicro()
		load, warm := 0, 0
		for i := range ms {
			if cs.Observe(ms[i].Data, now) {
				load++
			} else {
				warm++
			}
		}
		cs.Polled(load, warm)
		if c.stats != nil {
			c.stats.ReaderSaw()
		}
		proc.Add(len(ms))
		if cfg.Ack == "none" {
			continue
		}
		select {
		case c.run.AckSem <- struct{}{}:
		case <-c.ctx.Done():
			return
		}
		c.ackWg.Add(1)
		go func(ms []qmsg, n int) {
			defer c.ackWg.Done()
			defer func() { <-c.run.AckSem }()
			t := time.Now()
			_, err := c.cli.ackBatch(c.ackCtx, ms, "")
			if err != nil && c.ackCtx.Err() != nil {
				return // cut off at shutdown: not an error of the system under test
			}
			cs.AckDone(n, time.Since(t), err)
		}(ms, load)
	}
}

// stop ends the loops, lets in-flight acks land (up to 5 s), then cancels the rest.
func (c *consumers) stop() {
	if c.cancel == nil {
		return
	}
	c.cancel()
	c.wg.Wait()
	done := make(chan struct{})
	go func() { c.ackWg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
	}
	c.ackCancel()
	c.ackWg.Wait()
}

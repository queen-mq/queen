package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"mqload/internal/core"
)

// txnWorkers: the -txn pipeline workers of this process (global indices cons-offset .. +consumers-1). Each one:
// fill a batch from <topic>-in (leased pops, up to -txn-size messages over up to -pop-width partitions, the first
// pop long-polls, the batch is topped up for at most -txn-linger after its first message), then ONE
// POST /api/v1/transaction: every input acked with its lease (the fence: a lease that ran out rolls the whole
// transaction back) + the transformed messages pushed to the same partition of <topic>-out. A rolled-back or failed
// transaction is never retried: its leases run out and the inputs are redelivered.
type txnWorkers struct {
	run      *core.Run
	cli      *qhttp
	qf       *queenFlags
	tx       *core.TxnConfig
	stats    *core.TxnStats
	in, out  string
	stopping atomic.Bool
	wg       sync.WaitGroup
	popErrs  atomic.Int64
}

func newWorkers(run *core.Run, cli *qhttp, qf *queenFlags, tx *core.TxnConfig, stats *core.TxnStats) *txnWorkers {
	return &txnWorkers{run: run, cli: cli, qf: qf, tx: tx, stats: stats, in: core.InTopic(run.Cfg), out: core.OutTopic(run.Cfg)}
}

func (w *txnWorkers) start() {
	cfg := w.run.Cfg
	for i := 0; i < cfg.Consumers; i++ {
		w.wg.Add(1)
		go w.loop(cfg.ConsOffset + i)
	}
	fmt.Printf("  [txn] %d workers (ci %d..%d of %d): %s -> %s, txn-size %d, linger %v, pop width %d, lease %ds\n",
		cfg.Consumers, cfg.ConsOffset, cfg.ConsOffset+cfg.Consumers-1, cfg.ConsTotal, w.in, w.out, w.tx.Size, w.tx.Linger, w.qf.popWidth, w.qf.lease)
}

func (w *txnWorkers) loop(ci int) {
	defer w.wg.Done()
	for !w.stopping.Load() {
		msgs := w.fill()
		if len(msgs) > 0 {
			w.commit(ci, msgs) // what was popped is committed even when stopping: we hold its leases
		}
	}
}

// fill pops until -txn-size messages or -txn-linger after the first one. Pops are never cancelled half-way (a
// cancelled pop could lease messages nobody commits until the lease runs out).
func (w *txnWorkers) fill() []qmsg {
	N := w.tx.Size
	var msgs []qmsg
	var deadline time.Time
	for len(msgs) < N && !w.stopping.Load() {
		wait := w.qf.popTimeout
		if !deadline.IsZero() {
			rem := time.Until(deadline)
			if rem < time.Millisecond {
				break
			}
			wait = min(wait, rem)
		}
		req := popReq{queue: w.in, batch: N - len(msgs), width: w.qf.popWidth, wait: true,
			timeoutMs: max(1, int(wait.Milliseconds())), autoAck: false, leaseS: w.qf.lease}
		ctx, cancel := context.WithTimeout(context.Background(), w.qf.timeout+wait)
		ms, err := w.cli.pop(ctx, req)
		cancel()
		if err != nil {
			if n := w.popErrs.Add(1); n <= 5 || n%1000 == 0 {
				w.run.NoteErr("pop-in", err)
			}
			if len(msgs) > 0 {
				break
			}
			time.Sleep(5 * time.Millisecond)
			continue
		}
		if len(ms) == 0 {
			continue
		}
		w.stats.Received(len(ms))
		msgs = append(msgs, ms...)
		if deadline.IsZero() {
			deadline = time.Now().Add(w.tx.Linger)
		}
	}
	return msgs
}

func (w *txnWorkers) commit(ci int, msgs []qmsg) {
	size := 64 + len(w.out)
	for i := range msgs {
		size += 200 + len(msgs[i].Data) + len(w.out) + len(msgs[i].Partition) + 48
	}
	b := make([]byte, 0, size)
	b = append(b, `{"operations":[`...)
	for i := range msgs {
		b = appendAckOp(b, &msgs[i], true)
		b = append(b, ',')
	}
	b = append(b, `{"type":"push","items":[`...)
	for i := range msgs {
		if i > 0 {
			b = append(b, ',')
		}
		b = append(b, `{"queue":"`...)
		b = append(b, w.out...)
		b = append(b, `","partition":`...)
		b = jsonString(b, msgs[i].Partition)
		b = append(b, `,"payload":`...)
		b = append(b, core.Transform(msgs[i].Data, ci)...)
		b = append(b, '}')
	}
	b = append(b, `]}],"requiredLeases":[`...)
	seen := map[string]bool{}
	for i := range msgs {
		if l := msgs[i].LeaseID; l != "" && !seen[l] {
			if len(seen) > 0 {
				b = append(b, ',')
			}
			seen[l] = true
			b = jsonString(b, l)
		}
	}
	b = append(b, `]}`...)

	t := w.stats.Begin()
	ctx, cancel := context.WithTimeout(context.Background(), w.qf.timeout)
	_, rb, err := w.cli.do(ctx, "POST", "/api/v1/transaction", b)
	cancel()
	if err != nil {
		w.stats.Failed("txn", err)
		return
	}
	var r txnResp
	if jerr := json.Unmarshal(rb, &r); jerr != nil {
		w.stats.Failed("txn", fmt.Errorf("transaction answer: %v", jerr))
		return
	}
	if r.Success != nil && *r.Success && r.Error == "" {
		w.stats.Committed(t, len(msgs))
		return
	}
	w.stats.Aborted("txn-rollback", fmt.Errorf("%s: %s", r.Reason, r.Error))
}

// stop: every worker finishes its batch (commit) and exits.
func (w *txnWorkers) stop() {
	w.stopping.Store(true)
	done := make(chan struct{})
	go func() { w.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(w.qf.timeout + w.qf.popTimeout + w.tx.Linger + 5*time.Second):
		fmt.Println("WARN txn workers did not stop in time")
	}
}

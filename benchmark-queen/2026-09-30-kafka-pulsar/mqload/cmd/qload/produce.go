package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"mqload/internal/core"

	queen "github.com/smartpricing/queen/clients/client-go/v2"
)

// producers: one POST /api/v1/push per unit, in its own goroutine (the pacer never blocks; the core's in-flight
// semaphore is the only thing that sheds), no retry (goload's open loop: RetryAttempts -1).
type producers struct {
	run   *core.Run
	cli   *qhttp
	qf    *queenFlags
	queue string
	ctx   context.Context
	stop  context.CancelFunc
}

func newProducers(run *core.Run, cli *qhttp, qf *queenFlags, txn bool) *producers {
	p := &producers{run: run, cli: cli, qf: qf, queue: run.Cfg.Topic}
	if txn {
		p.queue = core.InTopic(run.Cfg)
	}
	p.ctx, p.stop = context.WithCancel(context.Background())
	if run.Cfg.Rate <= 0 {
		fmt.Println("  [produce] -rate 0: no producers")
	}
	return p
}

func (p *producers) send(u *core.Unit) { go p.push(u) }

// partOf is message j's partition: the unit's own, or with -push-spread N one of N consecutive partitions from it,
// the messages in N contiguous runs.
func (p *producers) partOf(u *core.Unit, j int) uint64 {
	n := p.qf.pushSpread
	if n <= 1 || p.run.Cfg.Partitions <= 1 {
		return u.Entity
	}
	per := (u.N + n - 1) / n
	return (u.Entity + uint64(j/per)) % uint64(p.run.Cfg.Partitions)
}

func (p *producers) push(u *core.Unit) {
	if p.qf.pushWire == "bin" {
		p.pushBinary(u)
		return
	}
	size := 16
	for _, m := range u.Payloads {
		size += len(m) + len(p.queue) + 48
	}
	b := make([]byte, 0, size)
	b = append(b, `{"items":[`...)
	for j, m := range u.Payloads {
		if j > 0 {
			b = append(b, ',')
		}
		b = append(b, `{"queue":"`...)
		b = append(b, p.queue...)
		b = append(b, `","partition":"p`...)
		b = strconv.AppendUint(b, p.partOf(u, j), 10)
		b = append(b, `","payload":`...)
		b = append(b, m...)
		b = append(b, '}')
	}
	b = append(b, `]}`...)
	ctx, cancel := context.WithTimeout(p.ctx, p.qf.timeout)
	_, rb, err := p.cli.do(ctx, "POST", "/api/v1/push", b)
	cancel()
	if err == nil {
		var items []pushItem
		if jerr := json.Unmarshal(rb, &items); jerr != nil {
			err = fmt.Errorf("push answer: %v", jerr)
		} else if len(items) != u.N {
			err = fmt.Errorf("push answered %d items for %d", len(items), u.N)
		} else {
			for j := range items {
				var e error
				if s := items[j].Status; s != "queued" && s != "duplicate" {
					e = fmt.Errorf("push item status %q: %s", s, items[j].Error)
				}
				u.MsgDone(e)
			}
			return
		}
	}
	for j := 0; j < u.N; j++ {
		u.MsgDone(err)
	}
}

// pushBinary is push with the Go client's binary body (plain, or framed and compressed by the client) and its
// counts-only answer.
func (p *producers) pushBinary(u *core.Unit) {
	var batch []queen.PushBatch
	for j, m := range u.Payloads {
		part := "p" + strconv.FormatUint(p.partOf(u, j), 10)
		if k := len(batch) - 1; k >= 0 && batch[k].Partition == part {
			batch[k].Payloads = append(batch[k].Payloads, m)
			continue
		}
		batch = append(batch, queen.PushBatch{Queue: p.queue, Partition: part, Payloads: [][]byte{m}})
	}
	var b []byte
	var err error
	if p.qf.pushZstd {
		// The messages framed and compressed here; the broker keeps those bytes as they are.
		b, err = queen.EncodePushFrames(batch, true)
	} else {
		b, err = queen.EncodePushBinary(batch, false, true)
	}
	bad := u.N
	if err == nil {
		ctx, cancel := context.WithTimeout(p.ctx, p.qf.timeout)
		var rb []byte
		_, rb, err = p.cli.do(ctx, "POST", "/api/v1/push", b)
		cancel()
		if err == nil {
			var c queen.PushCounts
			if c, err = queen.ParsePushCounts(rb); err == nil {
				if c.Count != u.N {
					err = fmt.Errorf("push answered %d items for %d", c.Count, u.N)
				} else {
					bad = c.Error
					err = fmt.Errorf("push item status \"error\"")
				}
			}
		}
	}
	for j := 0; j < u.N-bad; j++ {
		u.MsgDone(nil)
	}
	for j := 0; j < bad; j++ {
		u.MsgDone(err)
	}
}

func (p *producers) close() {
	// in-flight pushes finished in WaitInflight; nothing is buffered client-side
	time.Sleep(10 * time.Millisecond)
	p.stop()
}

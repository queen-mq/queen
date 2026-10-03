package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"mqload/internal/core"
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

func (p *producers) push(u *core.Unit) {
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
		b = strconv.AppendUint(b, u.Entity, 10)
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

func (p *producers) close() {
	// in-flight pushes finished in WaitInflight; nothing is buffered client-side
	time.Sleep(10 * time.Millisecond)
	p.stop()
}

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"mqload/internal/core"
)

type admin struct {
	cli *qhttp
	qf  *queenFlags
	cfg *core.Config
	run *core.Run
	txn bool
}

// queues: <topic> (plain) or <topic>-in, <topic>-out (-txn).
func (a *admin) queues() []string {
	if a.txn {
		return []string{core.InTopic(a.cfg), core.OutTopic(a.cfg)}
	}
	return []string{a.cfg.Topic}
}

// setup: the broker answers /health with a known leader; with -create the queues are configured; with -warm every
// partition gets one message without "ts" (creating it, as the 09-30 grid's prefill did) which is then drained.
func (a *admin) setup(ctx context.Context) error {
	t := time.Now()
	if err := a.waitHealthy(ctx, 120*time.Second); err != nil {
		return err
	}
	if a.cfg.Create && a.qf.configure {
		for _, q := range a.queues() {
			if err := a.configure(ctx, q); err != nil {
				return err
			}
		}
	}
	if a.cfg.Warm {
		if err := a.warm(ctx); err != nil {
			return err
		}
	}
	a.run.SetInfo("setup_s", time.Since(t).Seconds())
	return nil
}

func (a *admin) waitHealthy(ctx context.Context, max time.Duration) error {
	dl := time.Now().Add(max)
	for {
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, rb, err := a.cli.do(cctx, "GET", "/health", nil)
		cancel()
		if err == nil {
			var h struct {
				Status string `json:"status"`
				Raft   struct {
					Role   string `json:"role"`
					Leader bool   `json:"leader"`
				} `json:"raft"`
				Version string `json:"version"`
			}
			if json.Unmarshal(rb, &h) == nil && h.Status == "healthy" {
				fmt.Printf("  [queen] %s healthy: version %s, this node %s\n", a.cli.base, h.Version, h.Raft.Role)
				a.run.SetInfo("queen_version", h.Version)
				return nil
			}
		}
		if time.Now().After(dl) || ctx.Err() != nil {
			return fmt.Errorf("broker %s not healthy within %v: %v %s", a.cli.base, max, err, rb)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (a *admin) configure(ctx context.Context, q string) error {
	body := fmt.Sprintf(`{"queue":%q,"options":{"leaseTime":%d,"retentionEnabled":true,"completedRetentionSeconds":%d,"retentionSeconds":0,"dedupWindowSeconds":%d,"retryLimit":%d}}`,
		q, a.qf.leaseTime, a.qf.complRet, a.qf.dedup, a.qf.retryLimit)
	cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	_, rb, err := a.cli.do(cctx, "POST", "/api/v1/configure", []byte(body))
	if err != nil {
		return fmt.Errorf("configure %s: %v", q, err)
	}
	var r struct {
		Options map[string]any `json:"options"`
	}
	_ = json.Unmarshal(rb, &r)
	fmt.Printf("  [configure] %s: leaseTime=%v completedRetentionSeconds=%v dedupWindowSeconds=%v retryLimit=%v retentionSeconds=%v\n",
		q, r.Options["leaseTime"], r.Options["completedRetentionSeconds"], r.Options["dedupWindowSeconds"], r.Options["retryLimit"], r.Options["retentionSeconds"])
	a.run.SetInfo("configure_"+q, r.Options)
	return nil
}

// warm pushes one message without "ts" into every partition of every queue (1000 per request), then drains them
// (queue mode, autoAck) so the load meets partitions that exist and are empty.
func (a *admin) warm(ctx context.Context) error {
	P := a.cfg.Partitions
	val := a.run.Pool.Warm()
	t := time.Now()
	for _, q := range a.queues() {
		var next atomic.Int64
		var firstErr atomic.Value
		var wg sync.WaitGroup
		for w := 0; w < a.qf.warmConc; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					lo := int(next.Add(1000) - 1000)
					if lo >= P || ctx.Err() != nil {
						return
					}
					hi := min(lo+1000, P)
					b := make([]byte, 0, (hi-lo)*(len(val)+64)+16)
					b = append(b, `{"items":[`...)
					for p := lo; p < hi; p++ {
						if p > lo {
							b = append(b, ',')
						}
						b = append(b, `{"queue":"`...)
						b = append(b, q...)
						b = append(b, `","partition":"p`...)
						b = fmt.Appendf(b, "%d", p)
						b = append(b, `","payload":`...)
						b = append(b, val...)
						b = append(b, '}')
					}
					b = append(b, `]}`...)
					cctx, cancel := context.WithTimeout(ctx, 120*time.Second)
					_, _, err := a.cli.do(cctx, "POST", "/api/v1/push", b)
					cancel()
					if err != nil {
						firstErr.CompareAndSwap(nil, err.Error())
						return
					}
				}
			}()
		}
		wg.Wait()
		if e := firstErr.Load(); e != nil {
			return fmt.Errorf("warm push %s: %v", q, e)
		}
	}
	pushS := time.Since(t).Seconds()
	td := time.Now()
	for _, q := range a.queues() {
		got, err := a.drain(ctx, q, int64(P))
		if err != nil {
			return err
		}
		fmt.Printf("  [warm] %s: %d partitions written and drained (%d messages)\n", q, P, got)
	}
	fmt.Printf("  [warm] one message (no \"ts\") into each of %d partitions of %s in %.1fs; drained in %.1fs\n",
		P, strings.Join(a.queues(), ", "), pushS, time.Since(td).Seconds())
	a.run.SetInfo("warm_push_s", pushS)
	a.run.SetInfo("warm_drain_s", time.Since(td).Seconds())
	return nil
}

// drain pops q (queue mode, autoAck) until want messages came out, or nothing came for 10 s.
func (a *admin) drain(ctx context.Context, q string, want int64) (int64, error) {
	var got atomic.Int64
	var last atomic.Int64
	last.Store(time.Now().UnixNano())
	dctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	for w := 0; w < a.qf.warmConc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for dctx.Err() == nil && got.Load() < want {
				cctx, c2 := context.WithTimeout(dctx, 30*time.Second)
				ms, err := a.cli.pop(cctx, popReq{queue: q, batch: 1000, width: 10, wait: false, autoAck: true})
				c2()
				if err != nil || len(ms) == 0 {
					time.Sleep(50 * time.Millisecond)
					continue
				}
				got.Add(int64(len(ms)))
				last.Store(time.Now().UnixNano())
			}
		}()
	}
	for got.Load() < want && ctx.Err() == nil && time.Since(time.Unix(0, last.Load())) < 10*time.Second {
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	wg.Wait()
	if got.Load() < want {
		return got.Load(), fmt.Errorf("warm drain %s: %d of %d messages came out", q, got.Load(), want)
	}
	return got.Load(), nil
}

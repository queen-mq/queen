package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"mqload/internal/core"
)

// runVerify is the verifier pass (after every load process exited): <topic>-out in full through a fresh consumer
// group (subscriptionMode all = from the first message; Queen serves only committed messages), each message's id
// as its position; then, once every worker lease ran out, the unprocessed rest of <topic>-in (queue-mode pops,
// never acked). Exit 0 = PASS, 3 = FAIL, 1 = the scan itself failed.
func runVerify(ctx context.Context, cfg *core.Config, tx *core.TxnConfig, qf *queenFlags, cli *qhttp) int {
	t0 := time.Now()
	exp, err := core.LoadIds(tx.IdsDir)
	if err != nil {
		fmt.Printf("FATAL %v\n", err)
		return 1
	}
	fmt.Printf("[verify] queen %s: %d ledgers from %s; reading %s (group verify, from the first message) then the rest of %s\n",
		qf.url, exp.Files, tx.IdsDir, core.OutTopic(cfg), core.InTopic(cfg))
	ta := core.NewTally(exp)
	group := "verify-" + time.Now().UTC().Format("150405")

	scan := func(what, queue, grp, sub string, ack bool, rec func(m *qmsg)) error {
		sctx, cancel := context.WithCancel(ctx)
		defer cancel()
		var wg sync.WaitGroup
		var mu sync.Mutex
		var n, errs int64
		var firstErr error
		for w := 0; w < qf.verifyConc; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				req := popReq{queue: queue, group: grp, subMode: sub, batch: 1000, width: qf.popWidth, wait: false, autoAck: false, leaseS: 600}
				for sctx.Err() == nil {
					cctx, c2 := context.WithTimeout(sctx, qf.timeout)
					ms, err := cli.pop(cctx, req)
					c2()
					if err != nil {
						if sctx.Err() != nil {
							return
						}
						mu.Lock()
						errs++
						if firstErr == nil {
							firstErr = err
						}
						mu.Unlock()
						time.Sleep(100 * time.Millisecond)
						continue
					}
					if len(ms) == 0 {
						time.Sleep(100 * time.Millisecond)
						continue
					}
					for i := range ms {
						rec(&ms[i])
					}
					mu.Lock()
					n += int64(len(ms))
					mu.Unlock()
					if ack {
						cctx, c3 := context.WithTimeout(sctx, qf.timeout)
						_, _ = cli.ackBatch(cctx, ms, grp)
						c3()
					}
				}
			}()
		}
		core.IdleScan(what, tx.VerifyIdle, tx.VerifyMax, func() int64 { mu.Lock(); defer mu.Unlock(); return n }, ctx.Done())
		cancel()
		wg.Wait()
		if errs > 0 {
			fmt.Printf("[verify] %s: %d pop errors, first: %v\n", what, errs, firstErr)
		}
		return nil
	}
	_ = scan("out "+core.OutTopic(cfg), core.OutTopic(cfg), group, "all", true, func(m *qmsg) {
		ta.Out(m.Data, core.PosHash(m.ID))
	})
	if w := qf.leaseWait - time.Since(t0); w > 0 {
		fmt.Printf("[verify] waiting %v until every worker lease (%ds) ran out before reading the rest of %s\n", w.Round(time.Second), qf.lease, core.InTopic(cfg))
		select {
		case <-ctx.Done():
		case <-time.After(w):
		}
	}
	_ = scan("rest of "+core.InTopic(cfg), core.InTopic(cfg), "", "", false, func(m *qmsg) {
		ta.Pending(m.Data)
	})
	v := ta.Verdict("queen")
	v.ScanS = time.Since(t0).Seconds()
	v.Print(cfg.Out)
	if v.Pass {
		return 0
	}
	return 3
}

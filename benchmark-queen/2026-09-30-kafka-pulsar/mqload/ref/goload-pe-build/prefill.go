package main

// -mode prefill: make partitions p<from>..p<to-1> EXIST before a timed window. The Queen twin of kload -warm
// (one record into every partition of a pre-created topic): Queen creates a partition on its first push, so an
// openloop window over a big space otherwise measures lazy creation at the round-robin rate (1000 new
// partitions/s with 3 loaders in lockstep), never N existing partitions.
//
//   push   ONE message (no "ts", so never an e2e sample) into every partition, -batch items per raw
//          /api/v1/push request (each item names its own partition), -conc requests in flight. A failed
//          request is retried; a double is harmless (the drain eats it).
//   drain  (-drain) wildcard AutoAck pops until every drainer saw -idle-ms of empty pops in a row: the window
//          then meets partitions that exist and were written and consumed once.
//
// Partitions are named like openloop's round robin (p%d), so the window writes into these ones. The queue is
// configured first with the options openloop sets at its own t=0, so that configure is a no-op re-set.

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	queen "github.com/smartpricing/queen/clients/client-go"
)

func runPrefillMode(args []string) {
	fs := flag.NewFlagSet("goload-prefill", flag.ExitOnError)
	url := fs.String("url", "http://127.0.0.1:6632", "broker base URL")
	queueName := fs.String("queue", "benchq", "queue name")
	nQueues := fs.Int("queues", 1, "queues <queue>-q<i> (as openloop -queues); [from,to) partitions in EACH")
	from := fs.Int("from", 0, "first partition index (inclusive)")
	to := fs.Int("to", 0, "last partition index (exclusive)")
	batch := fs.Int("batch", 1000, "items (= partitions) per push request")
	conc := fs.Int("conc", 32, "push requests in flight")
	doPush := fs.Bool("push", true, "push one message into every partition of [from, to)")
	drain := fs.Bool("drain", false, "pop (wildcard, AutoAck) until the queue stays empty")
	drainConc := fs.Int("drain-conc", 256, "drain consumers")
	popBatch := fs.Int("pop-batch", 1000, "drain pop batch")
	idleMs := fs.Int("idle-ms", 3000, "a drainer stops after this long without a message")
	drainMaxS := fs.Int("drain-max-s", 300, "the drain gives up after this long (it reports popped against the prefilled count either way)")
	completedRet := fs.Int("completed-retention", 300, "completed_retention_seconds (same as openloop's)")
	dedupWindow := fs.Int("dedup-window", 60, "dedupWindowSeconds (same as openloop's)")
	timeoutMs := fs.Int("timeout", 60000, "request timeout ms")
	_ = fs.String("mode", "prefill", "run mode")
	_ = fs.Parse(args)

	q, err := queen.New(queen.ClientConfig{URL: *url, TimeoutMillis: *timeoutMs, MaxIdleConnsPerHost: 1024, RetryAttempts: -1})
	if err != nil {
		fmt.Printf("client init failed: %v\n", err)
		os.Exit(1)
	}
	ctx := context.Background()

	qnames := []string{*queueName}
	if *nQueues > 1 {
		qnames = make([]string, *nQueues)
		for i := range qnames {
			qnames[i] = fmt.Sprintf("%s-q%d", *queueName, i)
		}
	}
	if *doPush && *to > *from {
		for _, qn := range qnames {
			if _, cerr := q.GetHttpClient().Post(ctx, "/api/v1/configure", map[string]interface{}{
				"queue": qn,
				"options": map[string]interface{}{
					"retentionEnabled": true, "completedRetentionSeconds": *completedRet, "retentionSeconds": 0,
					"leaseTime": 30, "dedupWindowSeconds": *dedupWindow, "encryptionEnabled": false, "minPopWaitTime": 0,
				},
			}); cerr != nil {
				fmt.Printf("[prefill] configure WARNING: %v\n", cerr)
			}
		}
		t0 := time.Now()
		var done, reqErr int64
		type job struct {
			q string
			s int
		}
		starts := make(chan job)
		var wg sync.WaitGroup
		for w := 0; w < *conc; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := range starts {
					s := j.s
					e := s + *batch
					if e > *to {
						e = *to
					}
					items := make([]map[string]interface{}, 0, e-s)
					for p := s; p < e; p++ {
						items = append(items, map[string]interface{}{"queue": j.q, "partition": fmt.Sprintf("p%d", p),
							"payload": map[string]interface{}{"prefill": p}})
					}
					for try := 0; ; try++ {
						_, perr := q.GetHttpClient().Post(ctx, "/api/v1/push", map[string]interface{}{"items": items})
						if perr == nil {
							break
						}
						atomic.AddInt64(&reqErr, 1)
						if try == 20 {
							fmt.Printf("[prefill] GIVING UP on %s p%d..p%d: %v\n", j.q, s, e-1, perr)
							os.Exit(1)
						}
						time.Sleep(time.Duration(200*(try+1)) * time.Millisecond)
					}
					atomic.AddInt64(&done, int64(e-s))
				}
			}()
		}
		stop := make(chan struct{})
		go func() {
			tk := time.NewTicker(5 * time.Second)
			defer tk.Stop()
			for {
				select {
				case <-stop:
					return
				case <-tk.C:
					d := atomic.LoadInt64(&done)
					fmt.Printf("[prefill] %d/%d partitions in %.0f s (%.0f/s) reqErr=%d\n", d, (*to-*from)*len(qnames),
						time.Since(t0).Seconds(), float64(d)/time.Since(t0).Seconds(), atomic.LoadInt64(&reqErr))
				}
			}
		}()
		for _, qn := range qnames {
			for s := *from; s < *to; s += *batch {
				starts <- job{qn, s}
			}
		}
		close(starts)
		wg.Wait()
		close(stop)
		el := time.Since(t0).Seconds()
		fmt.Printf("[prefill-push] partitions=%d secs=%.1f rate=%.0f/s reqErr=%d\n", (*to-*from)*len(qnames), el, float64((*to-*from)*len(qnames))/el, reqErr)
	}

	if *drain {
		t0 := time.Now()
		var popped, pops, popErr int64
		var errMu sync.Mutex
		firstErr := ""
		idle := time.Duration(*idleMs) * time.Millisecond
		maxDrain := time.Duration(*drainMaxS) * time.Second
		var wg sync.WaitGroup
		for w := 0; w < *drainConc; w++ {
			// Drainer w owns queues w, w+drain-conc, ...: every queue has a drainer (with fewer queues than
			// drainers, queue w%Q has several). An owner of several pops each one until it comes back empty,
			// then moves on, without waiting on an empty one.
			var mine []string
			for i := w % len(qnames); i < len(qnames); i += *drainConc {
				mine = append(mine, qnames[i])
			}
			wg.Add(1)
			go func(mine []string) {
				defer wg.Done()
				last := time.Now()
				for k := 0; time.Since(last) < idle && time.Since(t0) < maxDrain; {
					dq := mine[k%len(mine)]
					msgs, e := q.Queue(dq).Batch(*popBatch).Wait(len(mine) == 1).TimeoutMillis(500).AutoAck(true).Pop(ctx)
					atomic.AddInt64(&pops, 1)
					if e != nil {
						// A failed pop is not an empty queue: keep draining (until -drain-max-s).
						atomic.AddInt64(&popErr, 1)
						errMu.Lock()
						if firstErr == "" {
							firstErr = e.Error()
						}
						errMu.Unlock()
						last = time.Now()
						time.Sleep(100 * time.Millisecond)
						continue
					}
					if len(msgs) > 0 {
						atomic.AddInt64(&popped, int64(len(msgs)))
						last = time.Now()
						continue
					}
					k++
				}
			}(mine)
		}
		stopDrain := make(chan struct{})
		go func() {
			tk := time.NewTicker(10 * time.Second)
			defer tk.Stop()
			for {
				select {
				case <-stopDrain:
					return
				case <-tk.C:
					fmt.Printf("[prefill-drain] %d popped in %.0f s, pops=%d popErr=%d\n", atomic.LoadInt64(&popped), time.Since(t0).Seconds(), atomic.LoadInt64(&pops), atomic.LoadInt64(&popErr))
				}
			}
		}()
		wg.Wait()
		close(stopDrain)
		el := time.Since(t0).Seconds()
		expected := int64((*to - *from) * len(qnames))
		if firstErr != "" {
			fmt.Printf("[prefill-drain] first pop error: %.300s\n", firstErr)
		}
		fmt.Printf("[prefill-drain] popped=%d expected=%d left=%d pops=%d popErr=%d secs=%.1f (incl. %d ms idle tail)\n", popped, expected, max(0, expected-popped), pops, popErr, el, *idleMs)
	}
}

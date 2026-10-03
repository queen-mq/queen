package main

// -mode txn: exactly-once through transactions, under load (and node kills).
//
//   producers  push {k, s} into <prefix>tx-in, partition = key k, s = 1, 2, ... per key, B messages per request,
//              one goroutine per key subset (so a key's seqs go out in order). Each item's transactionId is a
//              deterministic uuid of (k, s): a client retry of a push that did commit is dropped by the dedup
//              window instead of doubling the input.
//   workers    pop tx-in (queue mode, AutoAck false, leased), and per popped batch commit ONE transaction:
//              ack the whole batch + push the derived {k, s} to <prefix>tx-out (partition k, RANDOM ids, so a
//              double commit shows up as a duplicate) + KV incr("txc", "c:<k>", +n per key).
//   reader     pops tx-out (queue mode, leased) and ACKS AFTER recording (at-least-once: with AutoAck a pop the
//              leader commits and dies before answering consumes messages nobody sees). Every (k, s) is recorded
//              with its message id: the same id again is a redelivery (fine), another id is a DOUBLE COMMIT.
//
// Verdict after a drain: every confirmed input appears in tx-out exactly once and in seq order per key, every
// KV counter equals the number of derived messages of its key, and tx-in is empty. A transaction that fails is
// never retried by the worker: its leases run out and the batch is redelivered, which is exactly the case the
// atomicity has to survive (the SDK's own RetryAttempts do retry a commit whose answer was lost).

import (
	"context"
	"crypto/sha1"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	queen "github.com/smartpricing/queen/clients/client-go"
)

func detUUID(parts ...string) string {
	h := sha1.Sum([]byte(strings.Join(parts, "\x00")))
	h[6] = (h[6] & 0x0f) | 0x50
	h[8] = (h[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", h[0:4], h[4:6], h[6:8], h[8:10], h[10:16])
}

type txnSeen struct {
	mu      sync.Mutex
	ids     map[string][]string // key -> message id of the first commit seen per seq (index = seq; "" = unseen)
	commits map[string]int64    // key -> distinct commits seen (first ids + double commits)
	last    map[string]int      // key -> highest FIRST occurrence so far (order check)
	viol    int64
	redeliv int64 // same (k, s, id) again: an at-least-once redelivery
	double  int64 // same (k, s), another id: the transaction committed twice
}

func (t *txnSeen) add(k string, s int, id string) {
	t.mu.Lock()
	c := t.ids[k]
	for len(c) <= s {
		c = append(c, "")
	}
	switch {
	case c[s] == "":
		if s < t.last[k] {
			t.viol++
		} else {
			t.last[k] = s
		}
		c[s] = id
		t.commits[k]++
	case c[s] == id:
		t.redeliv++
	default:
		t.double++
		t.commits[k]++
	}
	t.ids[k] = c
	t.mu.Unlock()
}

func runTxnMode(args []string) {
	fs := flag.NewFlagSet("txn", flag.ExitOnError)
	urls := fs.String("urls", "http://127.0.0.1:6632", "comma-separated broker URLs (failover across them)")
	keys := fs.Int("keys", 1000, "keys = partitions of tx-in and tx-out")
	rate := fs.Int("rate", 20000, "input messages/s (open loop)")
	pushBatch := fs.Int("push-batch", 10, "input messages per push request (one key per request)")
	producers := fs.Int("producers", 50, "producer goroutines (a key belongs to one)")
	workers := fs.Int("workers", 100, "transaction workers")
	readers := fs.Int("readers", 20, "tx-out reader goroutines")
	popBatch := fs.Int("pop-batch", 100, "max messages per worker pop")
	popParts := fs.Int("pop-partitions", 1, "partitions per worker pop (1 = one key per transaction)")
	duration := fs.Int("duration", 120, "producing seconds")
	drainMax := fs.Int("drain-max", 180, "max seconds to drain after producing stops")
	prefix := fs.String("prefix", "t", "queue name prefix")
	leaseTime := fs.Int("lease-time", 20, "tx-in leaseTime seconds (a failed transaction's batch comes back after this)")
	timeoutMs := fs.Int("timeout", 10000, "request timeout ms")
	report := fs.Int("report", 5, "report interval seconds")
	_ = fs.String("mode", "txn", "run mode")
	_ = fs.Parse(args)

	txIn, txOut := *prefix+"-tx-in", *prefix+"-tx-out"
	cfg := queen.ClientConfig{
		URLs:                strings.Split(*urls, ","),
		TimeoutMillis:       *timeoutMs,
		MaxIdleConnsPerHost: 2048,
		RetryAttempts:       3,
		EnableFailover:      true,
	}
	q, err := queen.New(cfg)
	if err != nil {
		fmt.Printf("client init failed: %v\n", err)
		os.Exit(1)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, qn := range []string{txIn, txOut} {
		cctx, cc := context.WithTimeout(ctx, 10*time.Second)
		_, cerr := q.GetHttpClient().Post(cctx, "/api/v1/configure", map[string]interface{}{
			"queue": qn,
			"options": map[string]interface{}{
				"retentionEnabled": true, "completedRetentionSeconds": 600, "retentionSeconds": 0,
				"leaseTime": *leaseTime, "dedupWindowSeconds": 600, "retryLimit": 1000000,
			},
		})
		cc()
		if cerr != nil {
			fmt.Printf("[configure] WARNING %s: %v\n", qn, cerr)
		}
	}
	fmt.Printf("goload -mode txn -> %s keys=%d rate=%d push-batch=%d workers=%d pop-batch=%d pop-partitions=%d duration=%ds lease=%ds\n",
		*urls, *keys, *rate, *pushBatch, *workers, *popBatch, *popParts, *duration, *leaseTime)

	var produced, pushErr, txnOK, txnMsgs, txnErr, txnRej, readN, popErr int64
	// confirmed[k] = highest seq whose push was confirmed; ambiguous = seqs whose push failed after retries
	confirmed := make([]int64, *keys)
	var ambMu sync.Mutex
	ambiguous := map[string]map[int]bool{}
	keyName := func(i int) string { return fmt.Sprintf("k%d", i) }
	seen := &txnSeen{ids: map[string][]string{}, commits: map[string]int64{}, last: map[string]int{}}

	// readers + workers first, then the producers
	rctx, rcancel := context.WithCancel(ctx)
	var rwg sync.WaitGroup
	for r := 0; r < *readers; r++ {
		rwg.Add(1)
		go func() {
			defer rwg.Done()
			for rctx.Err() == nil {
				msgs, e := q.Queue(txOut).Batch(1000).Partitions(10).Wait(true).TimeoutMillis(1000).AutoAck(false).Pop(rctx)
				if e != nil {
					if rctx.Err() == nil {
						atomic.AddInt64(&popErr, 1)
						time.Sleep(50 * time.Millisecond)
					}
					continue
				}
				for _, m := range msgs {
					k, _ := m.Data["k"].(string)
					s, _ := m.Data["s"].(float64)
					seen.add(k, int(s), m.TransactionID)
				}
				atomic.AddInt64(&readN, int64(len(msgs)))
				if len(msgs) > 0 {
					// a failed ack only means a redelivery later, which the id check absorbs
					_, _ = q.Ack(context.Background(), msgs, true, queen.AckOptions{})
				}
			}
		}()
	}
	wctx, wcancel := context.WithCancel(ctx)
	var wwg sync.WaitGroup
	var busy int64
	for w := 0; w < *workers; w++ {
		wwg.Add(1)
		go func() {
			defer wwg.Done()
			for wctx.Err() == nil {
				msgs, e := q.Queue(txIn).Batch(*popBatch).Partitions(*popParts).Wait(true).TimeoutMillis(1000).AutoAck(false).Pop(wctx)
				if e != nil {
					if wctx.Err() == nil {
						atomic.AddInt64(&popErr, 1)
						time.Sleep(50 * time.Millisecond)
					}
					continue
				}
				if len(msgs) == 0 {
					continue
				}
				atomic.AddInt64(&busy, 1)
				byKey := map[string][]interface{}{}
				for _, m := range msgs {
					k, _ := m.Data["k"].(string)
					s, _ := m.Data["s"].(float64)
					byKey[k] = append(byKey[k], map[string]interface{}{"k": k, "s": int(s)})
				}
				tb := q.Transaction().Ack(msgs, "completed", queen.AckOptions{})
				var kvOps []queen.KVOp
				for k, items := range byKey {
					tb = tb.Queue(txOut).Partition(k).Push(items)
					kvOps = append(kvOps, queen.KVIncrOp("txc", "c:"+k, int64(len(items)), queen.TTL(3*time.Hour)))
				}
				resp, cerr := tb.KV(kvOps...).Commit(context.Background())
				switch {
				case cerr != nil:
					atomic.AddInt64(&txnErr, 1)
				case resp != nil && !resp.Success:
					atomic.AddInt64(&txnRej, 1)
				default:
					atomic.AddInt64(&txnOK, 1)
					atomic.AddInt64(&txnMsgs, int64(len(msgs)))
				}
				atomic.AddInt64(&busy, -1)
			}
		}()
	}

	pctx, pcancel := context.WithTimeout(ctx, time.Duration(*duration)*time.Second)
	defer pcancel()
	var pwg sync.WaitGroup
	perProd := float64(*rate) / float64(*producers) / float64(*pushBatch) // requests/s per producer
	start := time.Now()
	for p := 0; p < *producers; p++ {
		pwg.Add(1)
		go func(p int) {
			defer pwg.Done()
			var mine []int
			for i := p; i < *keys; i += *producers {
				mine = append(mine, i)
			}
			if len(mine) == 0 {
				return
			}
			next := make([]int, len(mine))
			interval := time.Duration(float64(time.Second) / perProd)
			t := time.Now()
			for j := 0; pctx.Err() == nil; j++ {
				t = t.Add(interval)
				if d := time.Until(t); d > 0 {
					select {
					case <-pctx.Done():
					case <-time.After(d):
					}
				}
				if pctx.Err() != nil {
					break
				}
				slot := j % len(mine)
				ki := mine[slot]
				k := keyName(ki)
				// raw push: the SDK's builder mints fresh transactionIds, and these must be deterministic
				var items []map[string]interface{}
				first := next[slot] + 1
				for b := 0; b < *pushBatch; b++ {
					s := first + b
					items = append(items, map[string]interface{}{"queue": txIn, "partition": k,
						"payload": map[string]interface{}{"k": k, "s": s}, "transactionId": detUUID(k, fmt.Sprint(s))})
				}
				next[slot] += *pushBatch
				_, perr := q.GetHttpClient().Post(context.Background(), "/api/v1/push", map[string]interface{}{"items": items})
				if perr != nil {
					atomic.AddInt64(&pushErr, 1)
					ambMu.Lock()
					if ambiguous[k] == nil {
						ambiguous[k] = map[int]bool{}
					}
					for b := 0; b < *pushBatch; b++ {
						ambiguous[k][first+b] = true
					}
					ambMu.Unlock()
					continue
				}
				atomic.StoreInt64(&confirmed[ki], int64(first+*pushBatch-1))
				atomic.AddInt64(&produced, int64(*pushBatch))
			}
		}(p)
	}

	// reporter
	stopRep := make(chan struct{})
	go func() {
		tk := time.NewTicker(time.Duration(*report) * time.Second)
		defer tk.Stop()
		var lp, lt, lr int64
		for {
			select {
			case <-stopRep:
				return
			case <-tk.C:
				p, t, r := atomic.LoadInt64(&produced), atomic.LoadInt64(&txnMsgs), atomic.LoadInt64(&readN)
				sec := float64(*report)
				fmt.Printf("[%s] prod=%7.0f/s txn=%7.0f msg/s read=%7.0f/s | total prod=%d txn=%d read=%d | txnOK=%d txnErr=%d txnRej=%d pushErr=%d popErr=%d busy=%d\n",
					time.Now().UTC().Format("15:04:05"), float64(p-lp)/sec, float64(t-lt)/sec, float64(r-lr)/sec, p, t, r,
					atomic.LoadInt64(&txnOK), atomic.LoadInt64(&txnErr), atomic.LoadInt64(&txnRej), atomic.LoadInt64(&pushErr),
					atomic.LoadInt64(&popErr), atomic.LoadInt64(&busy))
				lp, lt, lr = p, t, r
			}
		}
	}()
	pwg.Wait()
	fmt.Printf("[produce] done after %.0fs: %d confirmed messages, %d push errors\n", time.Since(start).Seconds(), atomic.LoadInt64(&produced), atomic.LoadInt64(&pushErr))

	// drain: every confirmed input must reach tx-out; stop when the reader has them all (or it stops moving)
	want := int64(0)
	for i := range confirmed {
		want += atomic.LoadInt64(&confirmed[i])
	}
	dstart, lastMove, lastR := time.Now(), time.Now(), int64(-1)
	for time.Since(dstart) < time.Duration(*drainMax)*time.Second {
		r := atomic.LoadInt64(&readN)
		if r != lastR {
			lastR, lastMove = r, time.Now()
		}
		if r >= want && atomic.LoadInt64(&busy) == 0 && time.Since(lastMove) > 5*time.Second {
			break
		}
		if time.Since(lastMove) > time.Duration(*leaseTime+30)*time.Second {
			break
		}
		time.Sleep(time.Second)
	}
	fmt.Printf("[drain] %.0fs: read %d of %d confirmed\n", time.Since(dstart).Seconds(), atomic.LoadInt64(&readN), want)
	wcancel()
	wwg.Wait()
	time.Sleep(2 * time.Second)
	rcancel()
	rwg.Wait()
	close(stopRep)

	// verify
	var missing, extra, distinct, ambSeen int64
	counterBad := 0
	var badKeys []string
	kvKeys := make([]string, 0, *keys)
	for i := 0; i < *keys; i++ {
		kvKeys = append(kvKeys, "c:"+keyName(i))
	}
	counters := map[string]int64{}
	for i := 0; i < len(kvKeys); i += 100 {
		j := i + 100
		if j > len(kvKeys) {
			j = len(kvKeys)
		}
		many, gerr := q.KV().GetMany(ctx, "txc", kvKeys[i:j])
		if gerr != nil {
			fmt.Printf("[verify] KV getMany failed: %v\n", gerr)
			continue
		}
		for _, row := range many.Rows {
			var v int64
			_ = json.Unmarshal(row.Value, &v)
			counters[row.Key] = v
		}
	}
	for i := 0; i < *keys; i++ {
		k := keyName(i)
		conf := int(atomic.LoadInt64(&confirmed[i]))
		c := seen.ids[k]
		total := seen.commits[k]
		for s := 1; s < len(c); s++ {
			if c[s] != "" {
				distinct++
			}
			if s > conf && c[s] != "" {
				if ambiguous[k][s] {
					ambSeen++
				} else {
					extra++
				}
			}
		}
		for s := 1; s <= conf; s++ {
			if (s >= len(c) || c[s] == "") && !ambiguous[k][s] {
				missing++
			}
		}
		if counters["c:"+k] != total {
			counterBad++
			if len(badKeys) < 5 {
				badKeys = append(badKeys, fmt.Sprintf("%s: kv=%d seen=%d", k, counters["c:"+k], total))
			}
		}
	}
	sort.Strings(badKeys)
	dups := seen.double
	fmt.Printf("[verify] confirmed=%d distinct-seen=%d missing=%d double-commits=%d redeliveries=%d extra=%d ambiguous-seen=%d order-violations=%d kv-counter-mismatch=%d/%d %v\n",
		want, distinct, missing, dups, seen.redeliv, extra, ambSeen, seen.viol, counterBad, *keys, badKeys)
	fmt.Printf("[verify] txnOK=%d txnErr=%d txnRej=%d pushErr=%d popErr=%d\n",
		atomic.LoadInt64(&txnOK), atomic.LoadInt64(&txnErr), atomic.LoadInt64(&txnRej), atomic.LoadInt64(&pushErr), atomic.LoadInt64(&popErr))
	if missing == 0 && dups == 0 && extra == 0 && seen.viol == 0 && counterBad == 0 {
		fmt.Println("VERDICT: PASS (every confirmed input exactly once, in order, KV counters exact)")
	} else {
		fmt.Println("VERDICT: FAIL")
	}
}

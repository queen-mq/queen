package main

// order.go — `-mode order`: the CORRECTNESS gate for TASK M (minimum pop wait).
//
// The throughput loader (-mode cloud) proves loss / duplication / tenant
// isolation from bitmaps, but it deliberately does NOT check ORDER: with several
// producers pushing concurrently to the same partition, producer sequence order
// is not the partition's storage order, so a naive "delivered sequence must
// increase" assertion there would fail on correct behaviour.
//
// This mode builds the one shape where storage order IS known: a SINGLE producer
// pushing SEQUENTIALLY (each push awaited before the next is issued), so the
// commit order of partition p is exactly s=1,2,3,… Whatever the consumers then
// do — one consumer or many, batch 1 or 500, window on or off — a per-partition
// delivery that ever goes backwards is a real ordering violation.
//
// It verifies, in one run:
//   * ORDER      per partition, every delivered sequence > the previous one
//   * LOSS       every sequence pushed is delivered
//   * DUPLICATION no sequence delivered twice
//   * LEASE/ACK  every delivered message is acked and the ack is accepted
//   * PROMPTNESS pop latency percentiles, and the messages-per-pop distribution
//                (the batching the window is supposed to buy)
//
//	goload -mode order -target proxy -url http://127.0.0.1:6711 \
//	  -tenants-file /root/campaign/tenants.json -tenant 0 \
//	  -queue ordercheck -group orderers -partitions 4 -messages 2000 \
//	  -consumers 3 -pop-batch 50 -min-pop-wait 50
//
// Exit code 0 = every check passed, 3 = a check failed, 1 = setup failure.

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	queen "github.com/smartpricing/queen/clients/client-go"
)

type orderViolation struct {
	Partition string `json:"partition"`
	Prev      int64  `json:"prev"`
	Got       int64  `json:"got"`
}

type orderReport struct {
	RunID       string           `json:"runId"`
	Note        string           `json:"note"`
	Config      map[string]any   `json:"config"`
	Pushed      int64            `json:"pushed"`
	Delivered   int64            `json:"delivered"`
	Acked       int64            `json:"acked"`
	Missing     []int64          `json:"missing"`
	Duplicates  []int64          `json:"duplicates"`
	OutOfOrder  []orderViolation `json:"outOfOrder"`
	Pops        int64            `json:"pops"`
	EmptyPops   int64            `json:"emptyPops"`
	MsgsPerPop  float64          `json:"msgsPerPop"`
	PopSizeHist map[string]int64 `json:"popSizeHist"`
	PopWaitMs   map[string]float64 `json:"popWaitMs"`
	DrainSec    float64          `json:"drainSec"`
	Verdict     string           `json:"verdict"`
	Failures    []string         `json:"failures"`
}

func runOrderMode(args []string) {
	fs := flag.NewFlagSet("order", flag.ExitOnError)
	_ = fs.String("mode", "order", "")
	target := fs.String("target", "proxy", "proxy | broker")
	url := fs.String("url", "http://127.0.0.1:6711", "base URL")
	tenantsFile := fs.String("tenants-file", "", "tenants json (required for -target proxy)")
	tenantIdx := fs.Int("tenant", 0, "which tenant index in the file to run as")
	queueName := fs.String("queue", "ordercheck", "queue name")
	groupName := fs.String("group", "orderers", "consumer group")
	partitions := fs.Int("partitions", 4, "partitions to spread the sequential push over")
	messages := fs.Int("messages", 2000, "messages to push (total, across partitions)")
	consumers := fs.Int("consumers", 3, "concurrent consumers on the same queue+group")
	popBatch := fs.Int("pop-batch", 50, "max messages per pop")
	popPartitions := fs.Int("pop-partitions", 1, "partitions claimable per pop")
	popTimeout := fs.Int("pop-timeout", 5000, "long-poll timeout ms")
	leaseTime := fs.Int("lease-time", 60, "queue leaseTime seconds")
	minPopWait := fs.Int("min-pop-wait", 0, "minPopWaitTime ms on the queue (0 = off)")
	drainSec := fs.Int("drain", 30, "seconds to keep consuming after the last push")
	outDir := fs.String("out", "", "directory for the json report (optional)")
	runID := fs.String("run-id", "", "report id")
	note := fs.String("note", "", "free-text note recorded in the report")
	_ = fs.Parse(args)

	rid := *runID
	if rid == "" {
		rid = fmt.Sprintf("order-w%d-%d", *minPopWait, time.Now().Unix())
	}

	cfg := queen.ClientConfig{URL: *url, TimeoutMillis: 30000, MaxIdleConnsPerHost: *consumers + 4}
	if *target == "proxy" {
		if *tenantsFile == "" {
			fmt.Println("-target proxy needs -tenants-file")
			os.Exit(1)
		}
		tf, err := loadTenantsFile(*tenantsFile)
		if err != nil {
			fmt.Printf("tenants file: %v\n", err)
			os.Exit(1)
		}
		var cred *TenantCred
		for i := range tf.Tenants {
			if tf.Tenants[i].Idx == *tenantIdx {
				cred = &tf.Tenants[i]
			}
		}
		if cred == nil {
			fmt.Printf("tenant idx %d not in %s\n", *tenantIdx, *tenantsFile)
			os.Exit(1)
		}
		cfg.BearerToken = cred.APIKey
		cfg.Headers = map[string]string{"Host": cred.ClusterSlug}
	}
	cl, err := queen.New(cfg)
	if err != nil {
		fmt.Printf("client init: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// ------------------------------------------------------------- configure
	if _, err := cl.GetHttpClient().Post(ctx, "/api/v1/configure", map[string]interface{}{
		"queue": *queueName,
		"options": map[string]interface{}{
			"leaseTime":      *leaseTime,
			"minPopWaitTime": *minPopWait,
		},
	}); err != nil {
		fmt.Printf("configure: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("goload -mode order [%s] -> %s\n", rid, *url)
	fmt.Printf("  queue=%q group=%q partitions=%d messages=%d consumers=%d popBatch=%d minPopWaitTime=%dms\n",
		*queueName, *groupName, *partitions, *messages, *consumers, *popBatch, *minPopWait)

	// ------------------------------------------------------------- consumers
	// Started BEFORE the producer so the run exercises the live path (marks
	// arriving into a ring that already has parked consumers), not a cold drain.
	var (
		mu         sync.Mutex
		lastSeen   = map[string]int64{}
		seen       = map[int64]int{}
		violations []orderViolation
		popSizes   = map[int]int64{}
		popWaits   []float64
		pops, empties, delivered, acked int64
	)
	var wg sync.WaitGroup
	for c := 0; c < *consumers; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				qb := cl.Queue(*queueName).Group(*groupName).Batch(*popBatch).
					Wait(true).TimeoutMillis(*popTimeout)
				if *popPartitions > 1 {
					qb = qb.Partitions(*popPartitions)
				}
				t0 := time.Now()
				msgs, err := qb.Pop(ctx)
				el := float64(time.Since(t0).Microseconds()) / 1000.0
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					time.Sleep(20 * time.Millisecond)
					continue
				}
				atomic.AddInt64(&pops, 1)
				if len(msgs) == 0 {
					atomic.AddInt64(&empties, 1)
					continue
				}
				mu.Lock()
				popSizes[len(msgs)]++
				popWaits = append(popWaits, el)
				for _, m := range msgs {
					s, ok := m.Data["s"].(float64)
					p, _ := m.Data["p"].(string)
					if !ok {
						continue
					}
					seq := int64(s)
					delivered++
					seen[seq]++
					// ORDER: within one partition, a delivered sequence must
					// never be lower than one already delivered. A duplicate is
					// counted separately (seen[]), so this only fires on true
					// re-ordering.
					if prev, had := lastSeen[p]; had && seq <= prev {
						if len(violations) < 100 {
							violations = append(violations, orderViolation{Partition: p, Prev: prev, Got: seq})
						}
					} else {
						lastSeen[p] = seq
					}
				}
				mu.Unlock()
				resp, aerr := cl.Ack(ctx, msgs, true, queen.AckOptions{ConsumerGroup: *groupName})
				if aerr != nil {
					continue
				}
				var okN int64
				for _, r := range resp {
					if r.Success {
						okN++
					}
				}
				atomic.AddInt64(&acked, okN)
			}
		}()
	}

	// -------------------------------------------------------------- producer
	// ONE producer, SEQUENTIAL: each push is awaited, so the storage order of
	// every partition is exactly the ascending sequence. This is what makes the
	// order assertion above sound.
	time.Sleep(300 * time.Millisecond)
	var pushed int64
	tPush := time.Now()
	for i := 1; i <= *messages; i++ {
		part := fmt.Sprintf("p%d", i%maxInt(1, *partitions))
		qb := cl.Queue(*queueName)
		if *partitions > 1 {
			qb = qb.Partition(part)
		} else {
			part = "Default"
		}
		_, err := qb.Push([]interface{}{map[string]interface{}{"s": i, "p": part}}).Execute(ctx)
		if err != nil {
			fmt.Printf("push %d: %v\n", i, err)
			continue
		}
		pushed++
	}
	pushSec := time.Since(tPush).Seconds()

	// ----------------------------------------------------------------- drain
	tDrain := time.Now()
	deadline := time.Now().Add(time.Duration(*drainSec) * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)
		mu.Lock()
		d := delivered
		mu.Unlock()
		if d >= pushed {
			// give stragglers a moment, then stop
			time.Sleep(1 * time.Second)
			break
		}
	}
	cancel()
	wg.Wait()
	drained := time.Since(tDrain).Seconds()

	// ---------------------------------------------------------------- verdict
	var missing, dups []int64
	for i := int64(1); i <= pushed; i++ {
		switch n := seen[i]; {
		case n == 0:
			if len(missing) < 100 {
				missing = append(missing, i)
			}
		case n > 1:
			if len(dups) < 100 {
				dups = append(dups, i)
			}
		}
	}
	sort.Slice(popWaits, func(a, b int) bool { return popWaits[a] < popWaits[b] })
	pct := func(p float64) float64 {
		if len(popWaits) == 0 {
			return 0
		}
		i := int(p * float64(len(popWaits)-1))
		return popWaits[i]
	}
	hist := map[string]int64{}
	var totMsgs, totPops int64
	for n, c := range popSizes {
		hist[fmt.Sprintf("%d", n)] = c
		totMsgs += int64(n) * c
		totPops += c
	}
	mpp := 0.0
	if totPops > 0 {
		mpp = float64(totMsgs) / float64(totPops)
	}

	var failures []string
	if len(missing) > 0 {
		failures = append(failures, fmt.Sprintf("LOSS: %d sequence(s) never delivered (first: %v)", len(missing), head(missing)))
	}
	if len(dups) > 0 {
		failures = append(failures, fmt.Sprintf("DUPLICATION: %d sequence(s) delivered more than once (first: %v)", len(dups), head(dups)))
	}
	if len(violations) > 0 {
		failures = append(failures, fmt.Sprintf("ORDER: %d out-of-order delivery/deliveries (first: %+v)", len(violations), violations[0]))
	}
	if acked != delivered {
		failures = append(failures, fmt.Sprintf("ACK: %d delivered but %d acked", delivered, acked))
	}
	verdict := "PASS"
	if len(failures) > 0 {
		verdict = "FAIL"
	}

	rep := orderReport{
		RunID: rid, Note: *note,
		Config: map[string]any{
			"target": *target, "url": *url, "queue": *queueName, "group": *groupName,
			"partitions": *partitions, "messages": *messages, "consumers": *consumers,
			"popBatch": *popBatch, "popPartitions": *popPartitions, "leaseTime": *leaseTime,
			"minPopWaitTime": *minPopWait, "pushSeconds": pushSec,
		},
		Pushed: pushed, Delivered: delivered, Acked: acked,
		Missing: missing, Duplicates: dups, OutOfOrder: violations,
		Pops: pops, EmptyPops: empties, MsgsPerPop: mpp, PopSizeHist: hist,
		PopWaitMs: map[string]float64{"p50": pct(0.50), "p95": pct(0.95), "p99": pct(0.99)},
		DrainSec:  drained, Verdict: verdict, Failures: failures,
	}

	fmt.Printf("\n========== %s ==========\n", rid)
	fmt.Printf("pushed=%d delivered=%d acked=%d  (push took %.1fs, drain %.1fs)\n", pushed, delivered, acked, pushSec, drained)
	fmt.Printf("pops=%d (empty %d)  msgs/pop=%.2f   pop latency p50=%.1f p95=%.1f p99=%.1f ms\n",
		pops, empties, mpp, pct(0.50), pct(0.95), pct(0.99))
	keys := make([]int, 0, len(popSizes))
	for n := range popSizes {
		keys = append(keys, n)
	}
	sort.Ints(keys)
	fmt.Printf("pop size histogram: ")
	for _, n := range keys {
		fmt.Printf("%d:%d ", n, popSizes[n])
	}
	fmt.Printf("\nmissing=%d duplicate=%d outOfOrder=%d\n", len(missing), len(dups), len(violations))
	fmt.Printf("VERDICT %s\n", verdict)
	for _, f := range failures {
		fmt.Printf("  ! %s\n", f)
	}

	if *outDir != "" {
		_ = os.MkdirAll(*outDir, 0o755)
		b, _ := json.MarshalIndent(rep, "", "  ")
		_ = os.WriteFile(fmt.Sprintf("%s/%s.json", *outDir, rid), append(b, '\n'), 0o644)
	}
	if verdict != "PASS" {
		os.Exit(3)
	}
}

func head(v []int64) []int64 {
	if len(v) > 5 {
		return v[:5]
	}
	return v
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

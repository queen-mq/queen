package main

// tenants.go — "many small tenants" resource-profile workload (mode: tenants).
//
// Models the enterprise-cloud shape Alice wants to size (2026-07-24): T tenants
// × Q queues each, every queue with its own long-polling consumer(s), and per-
// tenant traffic that CYCLES between a high and a low rate (default 10 msg/s ↔
// 2 msg/s every 120s, staggered per tenant so phase flips never synchronize).
// The interesting cost here is NOT throughput — it is what PG and the broker
// burn to hold thousands of mostly-idle queues and parked consumers: empty
// pops, long-poll re-parks, reseed floors, retention sweeps over a wide queue
// set. -idle-only drops the producers entirely and measures the pure
// standing-army cost. Resource numbers come from bench-sampler/loader-sampler
// alongside; this loader reports the traffic-side view (rates, empty pops,
// e2e latency from the producer stamp, error counts) plus a final
// pushed-vs-popped consistency delta per run.
//
// Deliberately NOT here: order verification (cm.go owns that), open-loop shed
// accounting (rates are tiny). Push errors are counted and the message is
// dropped — a lost message shows up in the final delta, which for a resource
// test is signal enough.

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	queen "github.com/smartpricing/queen/clients/client-go"
)

func tenantQueue(t, q int) string { return fmt.Sprintf("t%04d-q%d", t, q) }

func runTenantsMode(args []string) {
	fs := flag.NewFlagSet("goload-tenants", flag.ExitOnError)
	url := fs.String("url", "http://127.0.0.1:6632", "broker base URL")
	tenants := fs.Int("tenants", 1000, "number of tenants")
	queuesPer := fs.Int("queues-per-tenant", 10, "queues per tenant (1 partition each)")
	consPer := fs.Int("consumers-per-queue", 1, "long-poll consumers per queue")
	phaseHigh := fs.Int("phase-high", 10, "per-tenant msg/s in the HIGH phase")
	phaseLow := fs.Int("phase-low", 2, "per-tenant msg/s in the LOW phase")
	phaseSec := fs.Int("phase-sec", 120, "seconds per phase (high/low alternate)")
	stagger := fs.Bool("stagger", true, "random per-tenant phase offset (avoids synchronized flips)")
	idleOnly := fs.Bool("idle-only", false, "NO producers: measure the pure parked-consumer standing cost")
	payloadB := fs.Int("payload", 256, "payload size bytes")
	popBatch := fs.Int("pop-batch", 32, "max messages per pop")
	popTimeout := fs.Int("pop-timeout", 25000, "long-poll timeout ms")
	durationSec := fs.Int("duration", 600, "run duration seconds after provisioning")
	reportSec := fs.Int("report", 5, "report interval seconds")
	provisionConc := fs.Int("provision-conc", 64, "concurrent queue-configure calls during provisioning")
	idleConns := fs.Int("idle-conns", 12000, "MaxIdleConnsPerHost (>= total consumers: every long-poll holds a conn)")
	timeoutMs := fs.Int("timeout", 30000, "request timeout ms (must exceed -pop-timeout)")
	_ = fs.String("mode", "tenants", "run mode")
	_ = fs.Parse(args)

	nq := *tenants * *queuesPer
	fmt.Printf("goload -mode tenants -> %s tenants=%d queues=%d consumers=%d idleOnly=%v\n",
		*url, *tenants, nq, nq**consPer, *idleOnly)
	if !*idleOnly {
		fmt.Printf("  traffic: per-tenant %d msg/s <-> %d msg/s every %ds (stagger=%v) | aggregate %d <-> %d msg/s\n",
			*phaseHigh, *phaseLow, *phaseSec, *stagger, *tenants**phaseHigh, *tenants**phaseLow)
	}

	q, err := queen.New(queen.ClientConfig{
		URL:                 *url,
		TimeoutMillis:       *timeoutMs,
		MaxIdleConnsPerHost: *idleConns,
		RetryAttempts:       1,
	})
	if err != nil {
		fmt.Printf("client init failed: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// ---------------------------------------------------------------- provision
	// Configure every queue up front (the enterprise tenants pre-exist). Bounded
	// concurrency; a failed configure is fatal — a resource test over a half-
	// provisioned fleet measures nothing.
	t0 := time.Now()
	var provErr int64
	sem := make(chan struct{}, *provisionConc)
	var pwg sync.WaitGroup
	for t := 0; t < *tenants; t++ {
		for qi := 0; qi < *queuesPer; qi++ {
			sem <- struct{}{}
			pwg.Add(1)
			go func(name string) {
				defer pwg.Done()
				defer func() { <-sem }()
				for attempt := 0; ; attempt++ {
					_, e := q.Queue(name).Config(queen.QueueConfig{
						LeaseTime:                 60,
						RetryLimit:                3,
						RetentionEnabled:          true,
						RetentionSeconds:          3600,
						CompletedRetentionSeconds: 300,
					}).Create().Execute(ctx)
					if e == nil {
						return
					}
					if attempt >= 5 || ctx.Err() != nil {
						atomic.AddInt64(&provErr, 1)
						return
					}
					time.Sleep(200 * time.Millisecond)
				}
			}(tenantQueue(t, qi))
		}
	}
	pwg.Wait()
	if provErr > 0 {
		fmt.Printf("[provision] FAILED: %d/%d queues\n", provErr, nq)
		os.Exit(1)
	}
	fmt.Printf("[provision] %d queues configured in %.1fs\n", nq, time.Since(t0).Seconds())

	// ---------------------------------------------------------------- counters
	var pushed, popped, acked, emptyPops, pushErr, popErr, ackErr int64
	// Per-queue pushed/popped for the final consistency delta (index t*Q+q).
	pushedPer := make([]int64, nq)
	poppedPer := make([]int64, nq)
	lat := newOLHist()
	var wg sync.WaitGroup

	// ---------------------------------------------------------------- consumers
	// One (or more) long-poll consumer per queue, explicit batch acks — the
	// standing army. Empty pops are the normal state and exactly what we count.
	for t := 0; t < *tenants; t++ {
		for qi := 0; qi < *queuesPer; qi++ {
			name := tenantQueue(t, qi)
			idx := t**queuesPer + qi
			for c := 0; c < *consPer; c++ {
				wg.Add(1)
				go func(name string, idx int) {
					defer wg.Done()
					for ctx.Err() == nil {
						msgs, e := q.Queue(name).Group("g0").Batch(*popBatch).
							Wait(true).TimeoutMillis(*popTimeout).AutoAck(false).Pop(ctx)
						if e != nil {
							if ctx.Err() != nil {
								return
							}
							atomic.AddInt64(&popErr, 1)
							time.Sleep(250 * time.Millisecond)
							continue
						}
						if len(msgs) == 0 {
							atomic.AddInt64(&emptyPops, 1)
							continue
						}
						now := time.Now().UnixMicro()
						for _, m := range msgs {
							if ts, ok := m.Data["ts"].(float64); ok {
								if d := now - int64(ts); d > 0 {
									lat.record(d)
								}
							}
						}
						atomic.AddInt64(&popped, int64(len(msgs)))
						atomic.AddInt64(&poppedPer[idx], int64(len(msgs)))
						resp, ae := q.Ack(ctx, msgs, true, queen.AckOptions{ConsumerGroup: "g0"})
						if ae != nil {
							if ctx.Err() == nil {
								atomic.AddInt64(&ackErr, int64(len(msgs)))
							}
							continue
						}
						var ok int64
						for _, r := range resp {
							if r.Success {
								ok++
							}
						}
						atomic.AddInt64(&acked, ok)
					}
				}(name, idx)
			}
		}
	}

	// ---------------------------------------------------------------- producers
	// One pacer goroutine per tenant: wall-clock owed at the phase's rate, each
	// message to a random queue of the tenant, single push, stamped for e2e.
	if !*idleOnly {
		pad := make([]byte, *payloadB)
		for i := range pad {
			pad[i] = 'x'
		}
		padStr := string(pad)
		start := time.Now()
		for t := 0; t < *tenants; t++ {
			wg.Add(1)
			go func(tid int, rng *rand.Rand) {
				defer wg.Done()
				offset := time.Duration(0)
				if *stagger {
					offset = time.Duration(rng.Int63n(int64(2 * *phaseSec))) * time.Second
				}
				var seq int64
				next := time.Now()
				for ctx.Err() == nil {
					// Phase: [high, low] alternating every phaseSec, shifted by offset.
					elapsed := time.Since(start) + offset
					phase := int(elapsed.Seconds()) / *phaseSec % 2
					rate := *phaseHigh
					if phase == 1 {
						rate = *phaseLow
					}
					if rate <= 0 {
						select {
						case <-time.After(500 * time.Millisecond):
						case <-ctx.Done():
						}
						continue
					}
					next = next.Add(time.Second / time.Duration(rate))
					if d := time.Until(next); d > 0 {
						select {
						case <-time.After(d):
						case <-ctx.Done():
							return
						}
					} else if d < -2*time.Second {
						next = time.Now() // fell behind (push stalls): don't burst-catch-up
					}
					qi := rng.Intn(*queuesPer)
					seq++
					payload := map[string]interface{}{
						"t": tid, "q": qi, "seq": seq,
						"ts":  time.Now().UnixMicro(),
						"pad": padStr,
					}
					_, e := q.Queue(tenantQueue(tid, qi)).Push(payload).Execute(ctx)
					if e != nil {
						if ctx.Err() == nil {
							atomic.AddInt64(&pushErr, 1)
						}
						continue
					}
					atomic.AddInt64(&pushed, 1)
					atomic.AddInt64(&pushedPer[tid**queuesPer+qi], 1)
				}
			}(t, rand.New(rand.NewSource(rand.Int63()+int64(t))))
		}
	}

	// ---------------------------------------------------------------- reporter
	stop := make(chan struct{})
	go func() {
		tick := time.NewTicker(time.Duration(*reportSec) * time.Second)
		defer tick.Stop()
		prev := make([]int64, olNumBuckets)
		cur := make([]int64, olNumBuckets)
		diff := make([]int64, olNumBuckets)
		var lp, lo, la, le int64
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				secs := float64(*reportSec)
				p, o := atomic.LoadInt64(&pushed), atomic.LoadInt64(&popped)
				a, em := atomic.LoadInt64(&acked), atomic.LoadInt64(&emptyPops)
				lat.snapshot(cur)
				for i := range diff {
					diff[i] = cur[i] - prev[i]
					prev[i] = cur[i]
				}
				fmt.Printf("[%s] push=%7.0f/s pop=%7.0f/s ack=%7.0f/s empty=%7.0f/s | e2e p50=%7.2f p99=%8.2f ms | lag=%d | errs push=%d pop=%d ack=%d gor=%d\n",
					time.Now().UTC().Format("15:04:05"),
					float64(p-lp)/secs, float64(o-lo)/secs, float64(a-la)/secs, float64(em-le)/secs,
					olPercentile(diff, 0.50), olPercentile(diff, 0.99),
					p-o,
					atomic.LoadInt64(&pushErr), atomic.LoadInt64(&popErr), atomic.LoadInt64(&ackErr),
					runtime.NumGoroutine())
				lp, lo, la, le = p, o, a, em
			}
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	if *durationSec > 0 {
		go func() { time.Sleep(time.Duration(*durationSec) * time.Second); cancel() }()
	}
	select {
	case <-sigCh:
		fmt.Println("\n[signal] stopping...")
		cancel()
	case <-ctx.Done():
	}

	wg.Wait()
	close(stop)

	// Final consistency: how many queues still hold undelivered tail messages
	// (in-flight at cutoff, expected small at these rates) vs a real mismatch.
	var lag, lagQueues int64
	for i := range pushedPer {
		if d := pushedPer[i] - poppedPer[i]; d > 0 {
			lag += d
			lagQueues++
		}
	}
	fmt.Printf("\n[final] pushed=%d popped=%d acked=%d empty=%d | tail: %d msgs across %d queues | errs push=%d pop=%d ack=%d\n",
		atomic.LoadInt64(&pushed), atomic.LoadInt64(&popped), atomic.LoadInt64(&acked),
		atomic.LoadInt64(&emptyPops), lag, lagQueues,
		atomic.LoadInt64(&pushErr), atomic.LoadInt64(&popErr), atomic.LoadInt64(&ackErr))
	_ = strconv.Itoa
}

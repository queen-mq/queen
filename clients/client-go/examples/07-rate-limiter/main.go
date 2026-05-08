// Example 07 — Rate limiter via .Gate() + per-partition lease back-pressure.
//
// 1:1 port of streams/examples/07-rate-limiter.js. Token-bucket rate limiter
// preserving FIFO order per partition WITHOUT a deferred queue. The
// back-pressure is the broker's per-partition lease: when the gate denies a
// message, the runner commits a partial ack and skips the lease release, so
// the un-acked tail returns to the queue when the lease expires — in its
// original order.
//
// Run:
//
//	QUEEN_URL=http://localhost:6632 go run ./examples/07-rate-limiter
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	queen "github.com/smartpricing/queen/clients/client-go"
	"github.com/smartpricing/queen/clients/client-go/streams"
	"github.com/smartpricing/queen/clients/client-go/streams/helpers"
)

func main() {
	url := envStr("QUEEN_URL", "http://localhost:6632")
	msgsPerTenant := envInt("MSGS_PER_TENANT", 50)
	tenants := envInt("TENANTS", 3)
	capacity := envInt("CAPACITY", 10)
	refill := float64(envInt("REFILL_PER_SEC", 5))
	leaseSec := envInt("LEASE_SEC", 2)
	batch := envInt("BATCH", 10)
	timeoutMs := envInt("TIMEOUT_MS", 60000)

	tag := fmt.Sprintf("%x", time.Now().UnixMilli())
	qReq := "rl_requests_" + tag
	qApx := "rl_approved_" + tag
	totalMsgs := msgsPerTenant * tenants

	fmt.Printf("[rate-limiter] Queen=%s tag=%s tenants=%d msgs/tenant=%d (total %d)\n",
		url, tag, tenants, msgsPerTenant, totalMsgs)
	fmt.Printf("[rate-limiter] bucket: capacity=%d refill=%g/sec lease=%ds batch=%d\n",
		capacity, refill, leaseSec, batch)

	q, err := queen.New(url)
	if err != nil {
		panic(err)
	}
	defer q.Close(context.Background())

	if _, err := q.Queue(qReq).Config(queen.QueueConfig{
		LeaseTime: leaseSec, RetryLimit: 100,
		RetentionSeconds: 3600, CompletedRetentionSeconds: 3600,
	}).Create().Execute(context.Background()); err != nil {
		panic(err)
	}
	if _, err := q.Queue(qApx).Config(queen.QueueConfig{
		LeaseTime: 30, RetentionSeconds: 3600, CompletedRetentionSeconds: 3600,
	}).Create().Execute(context.Background()); err != nil {
		panic(err)
	}

	tnames := make([]string, tenants)
	for i := range tnames {
		tnames[i] = fmt.Sprintf("tenant-%d", i)
	}

	fmt.Printf("[rate-limiter] burst-pushing %d requests across %d partitions...\n", totalMsgs, tenants)
	pushStart := time.Now()
	for _, t := range tnames {
		for seq := 0; seq < msgsPerTenant; seq++ {
			_, _ = q.Queue(qReq).Partition(t).Push(map[string]interface{}{
				"tenantId": t, "seq": seq, "pushedAt": time.Now().UnixMilli(),
			}).Execute(context.Background())
		}
	}
	fmt.Printf("[rate-limiter] push done in %dms\n", time.Since(pushStart).Milliseconds())

	gateFn := helpers.TokenBucketGate(helpers.TokenBucketGateOptions{
		Capacity: float64(capacity), RefillPerSec: refill,
	})

	stream := streams.From(q.Queue(qReq).AsStreamSource()).
		Gate(gateFn).
		To(q.Queue(qApx))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, err := stream.Run(ctx, streams.RunOptions{
		QueryID: "rate-limiter-" + tag, URL: url,
		BatchSize: batch, MaxPartitions: tenants, MaxWaitMillis: 500, Reset: true,
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("[rate-limiter] stream up")

	var drained int64
	stopDrain := atomic.Bool{}
	go func() {
		cg := "rl-consumer-" + tag
		for !stopDrain.Load() {
			batch, err := q.Queue(qApx).Group(cg).Batch(50).Wait(true).TimeoutMillis(500).Pop(context.Background())
			if err != nil || len(batch) == 0 {
				continue
			}
			atomic.AddInt64(&drained, int64(len(batch)))
			_, _ = q.Ack(context.Background(), batch, true, queen.AckOptions{ConsumerGroup: cg})
		}
	}()

	start := time.Now()
	lastReport := start
	var lastDrained int64
	for atomic.LoadInt64(&drained) < int64(totalMsgs) && time.Since(start) < time.Duration(timeoutMs)*time.Millisecond {
		time.Sleep(200 * time.Millisecond)
		if time.Since(lastReport) >= 2*time.Second {
			cur := atomic.LoadInt64(&drained)
			rate := float64(cur-lastDrained) / time.Since(lastReport).Seconds()
			m := r.Metrics()
			fmt.Printf("[T+%.0fs] drained=%d/%d rate=%.0f msg/sec  allows=%d denies=%d\n",
				time.Since(start).Seconds(), cur, totalMsgs, rate, m.GateAllowsTotal, m.GateDenialsTotal)
			lastReport = time.Now()
			lastDrained = cur
		}
	}

	stopDrain.Store(true)
	r.Stop()

	fmt.Println("\n" + repeatString("=", 80))
	fmt.Println("RATE LIMITER VERIFICATION")
	fmt.Println(repeatString("=", 80))

	finalDrained := atomic.LoadInt64(&drained)
	if finalDrained == int64(totalMsgs) {
		fmt.Printf("  PASS  %d/%d messages drained\n", finalDrained, totalMsgs)
	} else {
		fmt.Printf("  FAIL  %d/%d drained\n", finalDrained, totalMsgs)
	}

	m := r.Metrics()
	fmt.Printf("\n     gate ALLOWS:  %d\n", m.GateAllowsTotal)
	fmt.Printf("     gate DENIES:  %d\n", m.GateDenialsTotal)
	fmt.Printf("     cycles total: %d\n", m.CyclesTotal)
}

func envStr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
func repeatString(s string, n int) string {
	out := ""
	for i := 0; i < n; i++ {
		out += s
	}
	return out
}

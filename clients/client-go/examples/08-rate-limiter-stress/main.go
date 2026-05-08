// Example 08 — Rate-limiter stress test (many tenants, large bursts).
// 1:1 port of 08-rate-limiter-stress.js. Validates the .Gate() pattern at
// channel-manager scale: 100 tenants, 10k msgs each.
//
//	QUEEN_URL=http://localhost:6632 go run ./examples/08-rate-limiter-stress
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
	tenants := envInt("TENANTS", 100)
	msgsPerTenant := envInt("MSGS_PER_TENANT", 10000)
	runners := envInt("RUNNERS", 4)
	leaseSec := envInt("LEASE_SEC", 2)
	refill := float64(envInt("REFILL_PER_SEC", 100))
	capacity := envInt("CAPACITY", int(refill)*leaseSec)
	batch := envInt("BATCH", 200)
	timeoutMs := envInt("TIMEOUT_MS", 180000)

	tag := fmt.Sprintf("%x", time.Now().UnixMilli())
	qReq := "rl_stress_req_" + tag
	qApx := "rl_stress_apx_" + tag
	total := tenants * msgsPerTenant

	fmt.Printf("[stress] Queen=%s tenants=%d msgs/tenant=%d total=%d runners=%d capacity=%d refill=%g/s lease=%ds\n",
		url, tenants, msgsPerTenant, total, runners, capacity, refill, leaseSec)

	q, err := queen.New(url)
	if err != nil {
		panic(err)
	}
	defer q.Close(context.Background())

	if _, err := q.Queue(qReq).Config(queen.QueueConfig{
		LeaseTime: leaseSec, RetryLimit: 100000,
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
		tnames[i] = fmt.Sprintf("tenant-%03d", i)
	}

	fmt.Printf("[stress] burst-pushing %d msgs across %d tenants...\n", total, tenants)
	pushStart := time.Now()
	for i, t := range tnames {
		for off := 0; off < msgsPerTenant; off += 500 {
			end := off + 500
			if end > msgsPerTenant {
				end = msgsPerTenant
			}
			items := make([]map[string]interface{}, end-off)
			for j := off; j < end; j++ {
				items[j-off] = map[string]interface{}{"tenantId": t, "seq": j}
			}
			_, _ = q.Queue(qReq).Partition(t).Push(items).Execute(context.Background())
		}
		if (i+1)%10 == 0 {
			fmt.Printf("  pushed tenants %d/%d\n", i+1, tenants)
		}
	}
	fmt.Printf("[stress] push done in %.1fs\n", time.Since(pushStart).Seconds())

	gateFn := helpers.TokenBucketGate(helpers.TokenBucketGateOptions{
		Capacity: float64(capacity), RefillPerSec: refill,
	})

	streamsList := make([]*streams.Runner, runners)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for r := 0; r < runners; r++ {
		stream := streams.From(q.Queue(qReq).AsStreamSource()).
			Gate(gateFn).
			To(q.Queue(qApx))
		runner, err := stream.Run(ctx, streams.RunOptions{
			QueryID: "rate-limiter-stress-" + tag, URL: url,
			BatchSize: batch, MaxPartitions: tenants/runners + 1,
			MaxWaitMillis: 500, Reset: r == 0,
		})
		if err != nil {
			panic(err)
		}
		streamsList[r] = runner
	}
	fmt.Printf("[stress] %d runners up\n", runners)

	var drained int64
	stopDrain := atomic.Bool{}
	go func() {
		cg := "rl-stress-" + tag
		for !stopDrain.Load() {
			batch, err := q.Queue(qApx).Group(cg).Batch(500).Wait(true).TimeoutMillis(500).Pop(context.Background())
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
	for atomic.LoadInt64(&drained) < int64(total) && time.Since(start) < time.Duration(timeoutMs)*time.Millisecond {
		time.Sleep(2 * time.Second)
		cur := atomic.LoadInt64(&drained)
		dt := time.Since(lastReport).Seconds()
		var rate float64
		if dt > 0 {
			rate = float64(cur-lastDrained) / dt
		}
		fmt.Printf("[T+%.0fs] drained=%d/%d rate=%.0f msg/sec\n", time.Since(start).Seconds(), cur, total, rate)
		lastReport = time.Now()
		lastDrained = cur
	}

	stopDrain.Store(true)
	for _, r := range streamsList {
		r.Stop()
	}
	fmt.Printf("\n[stress] DONE: %d/%d drained in %.1fs (~%.0f msg/sec)\n",
		atomic.LoadInt64(&drained), total, time.Since(start).Seconds(),
		float64(atomic.LoadInt64(&drained))/time.Since(start).Seconds())
}

func envStr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func envInt(k string, d int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return d
}

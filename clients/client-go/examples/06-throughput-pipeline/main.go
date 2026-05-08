// Example 06 — Throughput pipeline (~1500 msg/sec).
//
// 1:1 port of streams/examples/06-throughput-pipeline.js. Two streams run
// concurrently against a 30-partition source: one computes 1-second tumbling
// totals, the other filters high-value events.
//
// Run:
//
//	QUEEN_URL=http://localhost:6632 DURATION_MS=30000 go run ./examples/06-throughput-pipeline
package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	queen "github.com/smartpricing/queen/client-go"
	"github.com/smartpricing/queen/client-go/streams"
)

func main() {
	url := os.Getenv("QUEEN_URL")
	if url == "" {
		url = "http://localhost:6632"
	}
	dur := time.Duration(envInt("DURATION_MS", 30000)) * time.Millisecond
	rate := envInt("TARGET_RATE", 1500)
	tag := fmt.Sprintf("%x", time.Now().UnixMilli())

	raw := "pipeline_orders_raw_" + tag
	totals := "pipeline_orders_totals_" + tag
	hv := "pipeline_orders_high_value_" + tag

	q, err := queen.New(url)
	if err != nil {
		panic(err)
	}
	defer q.Close(context.Background())
	for _, name := range []string{raw, totals, hv} {
		_, _ = q.Queue(name).Create().Execute(context.Background())
	}

	// 3 producers in parallel.
	var pushed int64
	var stop atomic.Bool
	var wg sync.WaitGroup
	customers := make([]string, 30)
	for i := range customers {
		customers[i] = "cust-" + strconv.Itoa(i)
	}
	per := rate / 3
	interval := time.Second / time.Duration(per)
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			for !stop.Load() {
				t0 := time.Now()
				cust := customers[rand.Intn(len(customers))]
				_, _ = q.Queue(raw).Partition(cust).Push(map[string]interface{}{
					"name":   name,
					"amount": rand.Intn(1000) + 1,
				}).Execute(context.Background())
				atomic.AddInt64(&pushed, 1)
				if d := time.Since(t0); d < interval {
					time.Sleep(interval - d)
				}
			}
		}(fmt.Sprintf("producer-%d", i))
	}

	streamA := streams.From(q.Queue(raw).AsStreamSource()).
		WindowTumbling(1, streams.WithIdleFlushMs(500)).
		Aggregate(map[string]streams.ExtractorFn{
			"count": func(m interface{}) (float64, error) { return 1, nil },
			"sum":   func(m interface{}) (float64, error) { return amount(m), nil },
		}, "count", "sum").
		To(q.Queue(totals))

	streamB := streams.From(q.Queue(raw).AsStreamSource()).
		Filter(func(m interface{}, _ streams.EmitCtx) (bool, error) {
			return amount(m) > 500, nil
		}).
		Map(func(m interface{}, _ streams.EmitCtx) (interface{}, error) {
			msg, _ := m.(map[string]interface{})
			data, _ := msg["data"].(map[string]interface{})
			out := map[string]interface{}{}
			for k, v := range data {
				out[k] = v
			}
			out["highValue"] = true
			return out, nil
		}).
		To(q.Queue(hv))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rA, err := streamA.Run(ctx, streams.RunOptions{
		QueryID: "pipeline.totals." + tag, URL: url, BatchSize: 50, MaxPartitions: 8, Reset: true,
	})
	if err != nil {
		panic(err)
	}
	rB, err := streamB.Run(ctx, streams.RunOptions{
		QueryID: "pipeline.highvalue." + tag, URL: url, BatchSize: 50, MaxPartitions: 8, Reset: true,
	})
	if err != nil {
		panic(err)
	}

	time.Sleep(dur)
	stop.Store(true)
	wg.Wait()
	time.Sleep(5 * time.Second)
	rA.Stop()
	rB.Stop()
	fmt.Printf("\nFinal stats: pushed=%d\n", atomic.LoadInt64(&pushed))
	fmt.Printf("Stream A: %+v\n", rA.Metrics())
	fmt.Printf("Stream B: %+v\n", rB.Metrics())
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func amount(m interface{}) float64 {
	if msg, ok := m.(map[string]interface{}); ok {
		if data, ok := msg["data"].(map[string]interface{}); ok {
			if x, ok := data["amount"].(float64); ok {
				return x
			}
			if x, ok := data["amount"].(int); ok {
				return float64(x)
			}
		}
	}
	return 0
}

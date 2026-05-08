// Example 00 — Demo.
//
// 1:1 port of streams/examples/00-demo.js. Pushes 30 messages across 3
// customer partitions over ~12s, runs a 3-second tumbling aggregation,
// prints each closed window as it lands.
//
// Run:
//
//	QUEEN_URL=http://localhost:6632 go run ./examples/00-demo
package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	queen "github.com/smartpricing/queen/client-go"
	"github.com/smartpricing/queen/client-go/streams"
)

func main() {
	url := os.Getenv("QUEEN_URL")
	if url == "" {
		url = "http://localhost:6632"
	}
	tag := fmt.Sprintf("%x", time.Now().UnixMilli())
	source := "streams_demo_orders_" + tag
	queryID := "streams.demo." + tag

	customers := []string{"cust-A", "cust-B", "cust-C"}
	const messagesPerCust = 10

	fmt.Printf("[demo] Queen at %s\n[demo] source = %s\n[demo] query  = %s\n", url, source, queryID)

	q, err := queen.New(url)
	if err != nil {
		panic(err)
	}
	defer q.Close(context.Background())

	// 1. Setup queue
	if _, err := q.Queue(source).Create().Execute(context.Background()); err != nil {
		panic(err)
	}

	// 2. Producer goroutine
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < messagesPerCust; i++ {
			for idx, cust := range customers {
				_, _ = q.Queue(source).Partition(cust).Push(map[string]interface{}{
					"customerId": cust,
					"amount":     10 + idx*5 + i,
					"idx":        i,
					"wave":       "demo",
				}).Execute(context.Background())
			}
			time.Sleep(1200 * time.Millisecond)
		}
	}()

	// 3. Stream
	stream := streams.From(q.Queue(source).AsStreamSource()).
		WindowTumbling(3, streams.WithGracePeriod(1), streams.WithIdleFlushMs(1500)).
		Aggregate(map[string]streams.ExtractorFn{
			"count": func(m interface{}) (float64, error) { return 1, nil },
			"sum":   func(m interface{}) (float64, error) { return floatField(m, "amount"), nil },
			"min":   func(m interface{}) (float64, error) { return floatField(m, "amount"), nil },
			"max":   func(m interface{}) (float64, error) { return floatField(m, "amount"), nil },
			"avg":   func(m interface{}) (float64, error) { return floatField(m, "amount"), nil },
		}, "count", "sum", "min", "max", "avg").
		Map(func(agg interface{}, ctx streams.EmitCtx) (interface{}, error) {
			a := agg.(map[string]interface{})
			return map[string]interface{}{
				"patient":     ctx["partition"],
				"windowStart": ctx["windowStart"],
				"count":       a["count"],
				"sum":         a["sum"],
				"avg":         a["avg"],
				"min":         a["min"],
				"max":         a["max"],
			}, nil
		}).
		Foreach(func(value interface{}, _ streams.EmitCtx) error {
			fmt.Printf("closed window: %+v\n", value)
			return nil
		})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner, err := stream.Run(ctx, streams.RunOptions{
		QueryID:       queryID,
		URL:           url,
		BatchSize:     50,
		MaxPartitions: 4,
		Reset:         true,
	})
	if err != nil {
		panic(err)
	}

	wg.Wait()
	time.Sleep(5 * time.Second)
	runner.Stop()
	fmt.Printf("[demo] final metrics: %+v\n", runner.Metrics())
}

func floatField(v interface{}, key string) float64 {
	m, ok := v.(map[string]interface{})
	if !ok {
		return 0
	}
	switch x := m[key].(type) {
	case float64:
		return x
	case int:
		return float64(x)
	case int64:
		return float64(x)
	}
	return 0
}

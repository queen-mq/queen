// Example 02 — Per-customer windowed sum.
//
// 1:1 port of streams/examples/02-per-customer-windowed-sum.js. Source
// `orders` is partitioned by customerId; the streaming query computes a
// 1-minute tumbling sum per customer and pushes the result onto
// `orders.totals_per_customer_per_min`.
//
// Run:
//
//	QUEEN_URL=http://localhost:6632 go run ./examples/02-per-customer-windowed-sum
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	queen "github.com/smartpricing/queen/client-go"
	"github.com/smartpricing/queen/client-go/streams"
)

func main() {
	url := os.Getenv("QUEEN_URL")
	if url == "" {
		url = "http://localhost:6632"
	}
	q, err := queen.New(url)
	if err != nil {
		panic(err)
	}
	defer q.Close(context.Background())

	stream := streams.From(q.Queue("orders").AsStreamSource()).
		WindowTumbling(60).
		Aggregate(map[string]streams.ExtractorFn{
			"count": func(m interface{}) (float64, error) { return 1, nil },
			"sum":   func(m interface{}) (float64, error) { return amount(m), nil },
			"avg":   func(m interface{}) (float64, error) { return amount(m), nil },
			"max":   func(m interface{}) (float64, error) { return amount(m), nil },
		}, "count", "sum", "avg", "max").
		To(q.Queue("orders.totals_per_customer_per_min"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, err := stream.Run(ctx, streams.RunOptions{
		QueryID:       "examples.orders.per_customer_per_min",
		URL:           url,
		BatchSize:     200,
		MaxPartitions: 4,
	})
	if err != nil {
		panic(err)
	}
	time.Sleep(5 * time.Minute)
	fmt.Printf("Stats: %+v\n", r.Metrics())
	r.Stop()
}

func amount(m interface{}) float64 {
	if msg, ok := m.(map[string]interface{}); ok {
		if data, ok := msg["data"].(map[string]interface{}); ok {
			if v, ok := data["amount"].(float64); ok {
				return v
			}
		}
	}
	return 0
}

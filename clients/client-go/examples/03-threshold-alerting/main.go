// Example 03 — Threshold alerting.
//
// 1:1 port of streams/examples/03-threshold-alerting.js. Computes a 30s avg
// CPU per host and emits an alert whenever the average exceeds 90.
//
// Run:
//
//	QUEEN_URL=http://localhost:6632 go run ./examples/03-threshold-alerting
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

	reduce := func(acc, m interface{}) (interface{}, error) {
		a, _ := acc.(map[string]interface{})
		if a == nil {
			a = map[string]interface{}{"sum": float64(0), "count": float64(0)}
		}
		v := float64(0)
		if msg, ok := m.(map[string]interface{}); ok {
			if data, ok := msg["data"].(map[string]interface{}); ok {
				if x, ok := data["value"].(float64); ok {
					v = x
				}
			}
		}
		a["sum"] = a["sum"].(float64) + v
		a["count"] = a["count"].(float64) + 1
		return a, nil
	}

	stream := streams.From(q.Queue("metrics.cpu").AsStreamSource()).
		WindowTumbling(30).
		Reduce(reduce, map[string]interface{}{"sum": float64(0), "count": float64(0)}).
		Map(func(agg interface{}, _ streams.EmitCtx) (interface{}, error) {
			a, _ := agg.(map[string]interface{})
			cnt := a["count"].(float64)
			if cnt == 0 {
				return nil, nil
			}
			avg := a["sum"].(float64) / cnt
			if avg <= 90 {
				return nil, nil
			}
			return map[string]interface{}{
				"severity":      "high",
				"avg":           avg,
				"windowSamples": cnt,
				"detectedAt":    time.Now().UTC().Format(time.RFC3339Nano),
			}, nil
		}).
		Filter(func(v interface{}, _ streams.EmitCtx) (bool, error) {
			return v != nil, nil
		}).
		To(q.Queue("alerts.cpu"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, err := stream.Run(ctx, streams.RunOptions{QueryID: "examples.alerts.cpu_high", URL: url})
	if err != nil {
		panic(err)
	}
	time.Sleep(5 * time.Minute)
	fmt.Printf("Stats: %+v\n", r.Metrics())
	r.Stop()
}

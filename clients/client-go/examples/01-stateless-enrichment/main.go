// Example 01 — Stateless enrichment.
//
// 1:1 port of streams/examples/01-stateless-enrichment.js. Read events from
// `events.raw`, drop heartbeats, enrich each one with derived fields, emit
// to `events.enriched`. No state, no windowing.
//
// Run:
//
//	QUEEN_URL=http://localhost:6632 go run ./examples/01-stateless-enrichment
package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	queen "github.com/smartpricing/queen/clients/client-go"
	"github.com/smartpricing/queen/clients/client-go/streams"
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

	fmt.Println("Starting stateless enrichment stream...")

	stream := streams.From(q.Queue("events.raw").AsStreamSource()).
		Filter(func(m interface{}, _ streams.EmitCtx) (bool, error) {
			msg, _ := m.(map[string]interface{})
			data, _ := msg["data"].(map[string]interface{})
			t, _ := data["type"].(string)
			return t != "heartbeat", nil
		}).
		Map(func(m interface{}, _ streams.EmitCtx) (interface{}, error) {
			msg, _ := m.(map[string]interface{})
			data, _ := msg["data"].(map[string]interface{})
			out := map[string]interface{}{}
			for k, v := range data {
				out[k] = v
			}
			out["receivedAt"] = msg["createdAt"]
			if t, ok := data["type"].(string); ok {
				out["upperType"] = strings.ToUpper(t)
			}
			return out, nil
		}).
		To(q.Queue("events.enriched"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runner, err := stream.Run(ctx, streams.RunOptions{
		QueryID:       "examples.events.enrich",
		URL:           url,
		BatchSize:     200,
		MaxPartitions: 4,
	})
	if err != nil {
		panic(err)
	}

	time.Sleep(60 * time.Second)
	fmt.Printf("Stopping... metrics=%+v\n", runner.Metrics())
	runner.Stop()
}

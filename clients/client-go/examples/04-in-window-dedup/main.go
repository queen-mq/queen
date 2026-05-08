// Example 04 — In-window dedup. 1:1 port of 04-in-window-dedup.js.
//
//	QUEEN_URL=http://localhost:6632 go run ./examples/04-in-window-dedup
package main

import (
	"context"
	"fmt"
	"os"
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

	dedup := func(acc, m interface{}) (interface{}, error) {
		seen, _ := acc.([]interface{})
		var eid string
		if msg, ok := m.(map[string]interface{}); ok {
			if data, ok := msg["data"].(map[string]interface{}); ok {
				if s, ok := data["eventId"].(string); ok {
					eid = s
				}
			}
		}
		if eid == "" {
			return seen, nil
		}
		for _, e := range seen {
			if e == eid {
				return seen, nil
			}
		}
		return append(seen, eid), nil
	}

	stream := streams.From(q.Queue("webhooks.raw").AsStreamSource()).
		WindowTumbling(300).
		Reduce(dedup, []interface{}{}).
		FlatMap(func(seen interface{}, _ streams.EmitCtx) ([]interface{}, error) {
			arr, _ := seen.([]interface{})
			out := make([]interface{}, len(arr))
			for i, e := range arr {
				out[i] = map[string]interface{}{"eventId": e}
			}
			return out, nil
		}).
		To(q.Queue("webhooks.unique"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, err := stream.Run(ctx, streams.RunOptions{QueryID: "examples.webhooks.dedup_5min", URL: url})
	if err != nil {
		panic(err)
	}
	time.Sleep(10 * time.Minute)
	fmt.Printf("Stats: %+v\n", r.Metrics())
	r.Stop()
}

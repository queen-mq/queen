// Example 05 — Per-entity hourly profile. 1:1 port of 05-per-entity-hourly-profile.js.
//
//	QUEEN_URL=http://localhost:6632 go run ./examples/05-per-entity-hourly-profile
package main

import (
	"context"
	"encoding/json"
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

	merge := func(acc, m interface{}) (interface{}, error) {
		a, _ := acc.(map[string]interface{})
		if a == nil {
			a = map[string]interface{}{"customerId": nil, "events": float64(0), "totalAmount": float64(0), "tags": []interface{}{}}
		}
		msg, _ := m.(map[string]interface{})
		data, _ := msg["data"].(map[string]interface{})
		a["customerId"] = data["customerId"]
		a["events"] = a["events"].(float64) + 1
		a["lastSeen"] = msg["createdAt"]
		if amt, ok := data["amount"].(float64); ok {
			a["totalAmount"] = a["totalAmount"].(float64) + amt
		}
		if tags, ok := data["tags"].([]interface{}); ok {
			seen := map[string]bool{}
			out := []interface{}{}
			for _, t := range a["tags"].([]interface{}) {
				if s, ok := t.(string); ok && !seen[s] {
					seen[s] = true
					out = append(out, s)
				}
			}
			for _, t := range tags {
				if s, ok := t.(string); ok && !seen[s] {
					seen[s] = true
					out = append(out, s)
				}
			}
			a["tags"] = out
		}
		return a, nil
	}

	stream := streams.From(q.Queue("account.events").AsStreamSource()).
		WindowTumbling(3600).
		Reduce(merge, map[string]interface{}{
			"customerId": nil, "events": float64(0), "totalAmount": float64(0), "tags": []interface{}{},
		}).
		Foreach(func(profile interface{}, _ streams.EmitCtx) error {
			b, _ := json.Marshal(profile)
			fmt.Println("[example05] writing profile =>", string(b))
			return nil
		})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, err := stream.Run(ctx, streams.RunOptions{
		QueryID: "examples.customer.hourly_profile", URL: url,
		BatchSize: 100, MaxPartitions: 4,
	})
	if err != nil {
		panic(err)
	}
	time.Sleep(60 * time.Minute)
	fmt.Printf("Stats: %+v\n", r.Metrics())
	r.Stop()
}

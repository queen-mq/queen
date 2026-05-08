// Example 06 — Patient heart-rate stream. 1:1 port of 06-health.js.
//
//	QUEEN_URL=http://localhost:6632 go run ./examples/06-health
package main

import (
	"context"
	"fmt"
	"math/rand"
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
	src := "streams_health_hr_" + tag
	queryID := "streams.health." + tag
	patients := []string{"patient-0", "patient-1", "patient-2", "patient-3", "patient-4", "patient-5", "patient-6", "patient-7", "patient-8", "patient-9"}

	q, err := queen.New(url)
	if err != nil {
		panic(err)
	}
	defer q.Close(context.Background())
	if _, err := q.Queue(src).Create().Execute(context.Background()); err != nil {
		panic(err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		fmt.Println("Starting patient heart rate producer")
		for i := 0; i < 60; i++ {
			for _, p := range patients {
				_, _ = q.Queue(src).Partition(p).Push(map[string]interface{}{
					"patient":   p,
					"heartRate": rand.Intn(60) + 40,
				}).Execute(context.Background())
			}
			time.Sleep(time.Second)
		}
	}()

	stream := streams.From(q.Queue(src).AsStreamSource()).
		WindowTumbling(10, streams.WithIdleFlushMs(2000)).
		Aggregate(map[string]streams.ExtractorFn{
			"count": func(m interface{}) (float64, error) { return 1, nil },
			"sum":   func(m interface{}) (float64, error) { return hr(m), nil },
			"min":   func(m interface{}) (float64, error) { return hr(m), nil },
			"max":   func(m interface{}) (float64, error) { return hr(m), nil },
			"avg":   func(m interface{}) (float64, error) { return hr(m), nil },
		}, "count", "sum", "min", "max", "avg").
		Foreach(func(value interface{}, ctx streams.EmitCtx) error {
			fmt.Printf("closed window: %+v %+v\n", value, ctx)
			return nil
		})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, err := stream.Run(ctx, streams.RunOptions{
		QueryID: queryID, URL: url, BatchSize: 50, MaxPartitions: 10, Reset: true,
	})
	if err != nil {
		panic(err)
	}
	wg.Wait()
	time.Sleep(15 * time.Second)
	r.Stop()
	fmt.Printf("Stats: %+v\n", r.Metrics())
}

func hr(m interface{}) float64 {
	if msg, ok := m.(map[string]interface{}); ok {
		if x, ok := msg["heartRate"].(float64); ok {
			return x
		}
		if x, ok := msg["heartRate"].(int); ok {
			return float64(x)
		}
	}
	return 0
}

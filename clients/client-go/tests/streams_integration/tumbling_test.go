// Tumbling window scenarios. 1:1 port of test-v2/stream/tumbling.js.
package streams_integration

import (
	"context"
	"sync"
	"testing"
	"time"

	queen "github.com/smartpricing/queen/clients/client-go"
	"github.com/smartpricing/queen/clients/client-go/streams"
)

// amount extracts m["amount"] from the payload directly. Matches the JS /
// Python aggregate semantics (the reducer receives the payload, not the
// full Queen message wrapper).
func amount(m interface{}) float64 {
	if msg, ok := m.(map[string]interface{}); ok {
		if v, ok := msg["amount"].(float64); ok {
			return v
		}
		if v, ok := msg["amount"].(int); ok {
			return float64(v)
		}
	}
	return 0
}

// TestTumblingBasicWindowSum mirrors tumblingBasicWindowSum.
func TestTumblingBasicWindowSum(t *testing.T) {
	src := mkName("tumblingBasicWindowSum", "src")
	sink := mkName("tumblingBasicWindowSum", "sink")
	queryID := mkName("tumblingBasicWindowSum", "q")
	_, _ = testClient.Queue(src).Create().Execute(context.Background())
	_, _ = testClient.Queue(sink).Create().Execute(context.Background())

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 1; i <= 8; i++ {
			_, _ = testClient.Queue(src).Partition("one").Push(map[string]interface{}{"amount": i}).Execute(context.Background())
			time.Sleep(1100 * time.Millisecond)
		}
	}()

	stream := streams.From(testClient.Queue(src).AsStreamSource()).
		WindowTumbling(2, streams.WithIdleFlushMs(800)).
		Aggregate(map[string]streams.ExtractorFn{
			"count": func(m interface{}) (float64, error) { return 1, nil },
			"sum":   func(m interface{}) (float64, error) { return amount(m), nil },
		}, "count", "sum").
		To(testClient.Queue(sink))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, err := stream.Run(ctx, streams.RunOptions{
		QueryID: queryID, URL: queenURL, BatchSize: 50, Reset: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()

	wg.Wait()

	emits := drainUntil(t, testClient.Queue(sink), func(out []*queen.Message) bool {
		total := 0.0
		for _, m := range out {
			if v, ok := m.Data["count"].(float64); ok {
				total += v
			}
		}
		return total >= 8
	}, 15*time.Second)

	totalSum := 0.0
	totalCount := 0.0
	for _, m := range emits {
		if v, ok := m.Data["sum"].(float64); ok {
			totalSum += v
		}
		if v, ok := m.Data["count"].(float64); ok {
			totalCount += v
		}
	}
	if len(emits) < 3 {
		t.Fatalf("expected at least 3 closed windows, got %d", len(emits))
	}
	if totalCount != 8 {
		t.Fatalf("expected total count=8, got %v", totalCount)
	}
	if totalSum != 36 {
		t.Fatalf("expected total sum=36 (1+2+...+8), got %v", totalSum)
	}
	if r.Metrics().ErrorsTotal != 0 {
		t.Fatalf("expected 0 runner errors, got %d", r.Metrics().ErrorsTotal)
	}
}

// TestTumblingAggregateAllStats mirrors tumblingAggregateAllStats.
func TestTumblingAggregateAllStats(t *testing.T) {
	src := mkName("tumblingAggregateAllStats", "src")
	sink := mkName("tumblingAggregateAllStats", "sink")
	queryID := mkName("tumblingAggregateAllStats", "q")
	_, _ = testClient.Queue(src).Create().Execute(context.Background())
	_, _ = testClient.Queue(sink).Create().Execute(context.Background())

	for _, v := range []int{10, 5, 30, 2, 3} {
		_, _ = testClient.Queue(src).Partition("p").Push(map[string]interface{}{"v": v}).Execute(context.Background())
	}

	stream := streams.From(testClient.Queue(src).AsStreamSource()).
		WindowTumbling(3, streams.WithIdleFlushMs(500)).
		Aggregate(map[string]streams.ExtractorFn{
			"count": func(m interface{}) (float64, error) { return 1, nil },
			"sum":   func(m interface{}) (float64, error) { return vField(m), nil },
			"min":   func(m interface{}) (float64, error) { return vField(m), nil },
			"max":   func(m interface{}) (float64, error) { return vField(m), nil },
			"avg":   func(m interface{}) (float64, error) { return vField(m), nil },
		}, "count", "sum", "min", "max", "avg").
		To(testClient.Queue(sink))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, err := stream.Run(ctx, streams.RunOptions{QueryID: queryID, URL: queenURL, BatchSize: 50, Reset: true})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()

	emits := drainUntil(t, testClient.Queue(sink), untilCount(1), 10*time.Second)
	if len(emits) == 0 {
		t.Fatal("no closed window emitted within timeout")
	}
	v := emits[0].Data
	if v["count"].(float64) != 5 || v["sum"].(float64) != 50 || v["avg"].(float64) != 10 {
		t.Fatalf("unexpected aggregate: %+v", v)
	}
	if v["min"].(float64) != 2 || v["max"].(float64) != 30 {
		t.Fatalf("min/max wrong: %+v", v)
	}
}

func vField(m interface{}) float64 {
	if msg, ok := m.(map[string]interface{}); ok {
		if v, ok := msg["v"].(float64); ok {
			return v
		}
		if v, ok := msg["v"].(int); ok {
			return float64(v)
		}
	}
	return 0
}

// TestTumblingIdleFlushClosesQuietPartitions mirrors tumblingIdleFlushClosesQuietPartitions.
func TestTumblingIdleFlushClosesQuietPartitions(t *testing.T) {
	src := mkName("tumblingIdleFlushClosesQuietPartitions", "src")
	sink := mkName("tumblingIdleFlushClosesQuietPartitions", "sink")
	queryID := mkName("tumblingIdleFlushClosesQuietPartitions", "q")
	_, _ = testClient.Queue(src).Create().Execute(context.Background())
	_, _ = testClient.Queue(sink).Create().Execute(context.Background())

	for i := 0; i < 3; i++ {
		_, _ = testClient.Queue(src).Partition("quiet").Push(map[string]interface{}{"v": 7}).Execute(context.Background())
	}

	stream := streams.From(testClient.Queue(src).AsStreamSource()).
		WindowTumbling(1, streams.WithIdleFlushMs(700)).
		Aggregate(map[string]streams.ExtractorFn{
			"count": func(m interface{}) (float64, error) { return 1, nil },
			"sum":   func(m interface{}) (float64, error) { return vField(m), nil },
		}, "count", "sum").
		To(testClient.Queue(sink))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, err := stream.Run(ctx, streams.RunOptions{QueryID: queryID, URL: queenURL, BatchSize: 50, Reset: true})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()

	// Drain until we've totalled count=3 (one or multiple closed windows).
	emits := drainUntil(t, testClient.Queue(sink), func(out []*queen.Message) bool {
		total := 0.0
		for _, m := range out {
			if v, ok := m.Data["count"].(float64); ok {
				total += v
			}
		}
		return total >= 3
	}, 8*time.Second)
	if len(emits) < 1 {
		t.Fatalf("idle flush should have emitted; got %d", len(emits))
	}
	totalCount := 0.0
	totalSum := 0.0
	for _, m := range emits {
		if v, ok := m.Data["count"].(float64); ok {
			totalCount += v
		}
		if v, ok := m.Data["sum"].(float64); ok {
			totalSum += v
		}
	}
	if totalCount != 3 {
		t.Fatalf("expected total count=3 across emits, got %v", totalCount)
	}
	if totalSum != 21 {
		t.Fatalf("expected total sum=21 across emits, got %v", totalSum)
	}
	if r.Metrics().FlushCyclesTotal < 1 {
		t.Fatalf("expected at least 1 flush cycle, got %d", r.Metrics().FlushCyclesTotal)
	}
}

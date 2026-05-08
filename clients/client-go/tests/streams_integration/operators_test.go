// Stateless operators against a live broker. 1:1 port of the 5 scenarios
// in test-v2/stream/operators.js.
package streams_integration

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	queen "github.com/smartpricing/queen/clients/client-go"
	"github.com/smartpricing/queen/clients/client-go/streams"
)

// drainUntil pops one message at a time from the sink until either the
// predicate returns true or the timeout elapses. The predicate sees the
// running list of drained messages so callers can implement count-based,
// sum-based, or partition-coverage stopping conditions.
func drainUntil(t *testing.T, qb *queen.QueueBuilder, until func([]*queen.Message) bool, timeout time.Duration) []*queen.Message {
	t.Helper()
	cg := "drain-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	out := []*queen.Message{}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if until != nil && until(out) {
			break
		}
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		batch, err := qb.Group(cg).Batch(1).Wait(true).TimeoutMillis(500).Pop(ctx)
		cancel()
		if err != nil || len(batch) == 0 {
			continue
		}
		out = append(out, batch...)
		_, _ = testClient.Ack(context.Background(), batch, true, queen.AckOptions{ConsumerGroup: cg})
	}
	return out
}

// untilCount is the most common stopping predicate: at least N messages.
func untilCount(n int) func([]*queen.Message) bool {
	return func(out []*queen.Message) bool { return len(out) >= n }
}

func dataStr(m *queen.Message, key string) string {
	if d, ok := m.Data[key].(string); ok {
		return d
	}
	return ""
}

func dataBool(m *queen.Message, key string) bool {
	if d, ok := m.Data[key].(bool); ok {
		return d
	}
	return false
}

// ---------------------------------------------------------------------------
// streamMapFilterSink
// ---------------------------------------------------------------------------

func TestStreamMapFilterSink(t *testing.T) {
	src := mkName("streamMapFilterSink", "src")
	sink := mkName("streamMapFilterSink", "sink")
	queryID := mkName("streamMapFilterSink", "q")
	if _, err := testClient.Queue(src).Create().Execute(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := testClient.Queue(sink).Create().Execute(context.Background()); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 30; i++ {
		typ := []string{"order", "heartbeat", "log"}[i%3]
		_, _ = testClient.Queue(src).Push(map[string]interface{}{"type": typ, "n": i}).Execute(context.Background())
	}

	stream := streams.From(testClient.Queue(src).AsStreamSource()).
		Filter(func(m interface{}, _ streams.EmitCtx) (bool, error) {
			msg, _ := m.(map[string]interface{})
			data, _ := msg["data"].(map[string]interface{})
			return data["type"] != "heartbeat", nil
		}).
		Map(func(m interface{}, _ streams.EmitCtx) (interface{}, error) {
			msg, _ := m.(map[string]interface{})
			data, _ := msg["data"].(map[string]interface{})
			out := map[string]interface{}{"marked": true}
			for k, v := range data {
				out[k] = v
			}
			return out, nil
		}).
		To(testClient.Queue(sink))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, err := stream.Run(ctx, streams.RunOptions{
		QueryID: queryID, URL: queenURL, BatchSize: 100, Reset: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()

	drained := drainUntil(t, testClient.Queue(sink), untilCount(20), 8*time.Second)
	if len(drained) < 20 {
		t.Fatalf("expected at least 20 sink messages, got %d", len(drained))
	}
	for _, m := range drained {
		if !dataBool(m, "marked") {
			t.Fatalf("expected marked=true, got %+v", m.Data)
		}
		if dataStr(m, "type") == "heartbeat" {
			t.Fatal("heartbeat survived filter")
		}
	}
	if r.Metrics().ErrorsTotal != 0 {
		t.Fatalf("expected 0 errors, got %d", r.Metrics().ErrorsTotal)
	}
}

// ---------------------------------------------------------------------------
// streamFlatMapFanout
// ---------------------------------------------------------------------------

func TestStreamFlatMapFanout(t *testing.T) {
	src := mkName("streamFlatMapFanout", "src")
	sink := mkName("streamFlatMapFanout", "sink")
	queryID := mkName("streamFlatMapFanout", "q")
	_, _ = testClient.Queue(src).Create().Execute(context.Background())
	_, _ = testClient.Queue(sink).Create().Execute(context.Background())

	for i := 0; i < 5; i++ {
		_, _ = testClient.Queue(src).Push(map[string]interface{}{"id": i}).Execute(context.Background())
	}

	stream := streams.From(testClient.Queue(src).AsStreamSource()).
		FlatMap(func(m interface{}, _ streams.EmitCtx) ([]interface{}, error) {
			msg, _ := m.(map[string]interface{})
			data, _ := msg["data"].(map[string]interface{})
			id := data["id"]
			out := make([]interface{}, 4)
			for c := 0; c < 4; c++ {
				out[c] = map[string]interface{}{"id": id, "copy": c + 1}
			}
			return out, nil
		}).
		To(testClient.Queue(sink))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, err := stream.Run(ctx, streams.RunOptions{
		QueryID: queryID, URL: queenURL, BatchSize: 100, Reset: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()

	drained := drainUntil(t, testClient.Queue(sink), untilCount(20), 8*time.Second)
	if len(drained) != 20 {
		t.Fatalf("expected 20 fan-out messages, got %d", len(drained))
	}
}

// ---------------------------------------------------------------------------
// streamForeachCapturesAll
// ---------------------------------------------------------------------------

func TestStreamForeachCapturesAll(t *testing.T) {
	src := mkName("streamForeachCapturesAll", "src")
	queryID := mkName("streamForeachCapturesAll", "q")
	_, _ = testClient.Queue(src).Create().Execute(context.Background())

	for i := 0; i < 12; i++ {
		_, _ = testClient.Queue(src).Push(map[string]interface{}{"i": i}).Execute(context.Background())
	}

	var captured []map[string]interface{}
	var mu int64 // simple "lock" via atomic counter via flag — but a slice append needs sync.
	_ = mu
	captureMu := make(chan struct{}, 1)
	captureMu <- struct{}{}

	stream := streams.From(testClient.Queue(src).AsStreamSource()).
		Foreach(func(value interface{}, _ streams.EmitCtx) error {
			<-captureMu
			if v, ok := value.(map[string]interface{}); ok {
				captured = append(captured, v)
			}
			captureMu <- struct{}{}
			return nil
		})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, err := stream.Run(ctx, streams.RunOptions{QueryID: queryID, URL: queenURL, BatchSize: 50, Reset: true})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		<-captureMu
		n := len(captured)
		captureMu <- struct{}{}
		if n >= 12 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	<-captureMu
	got := len(captured)
	captureMu <- struct{}{}
	if got < 12 {
		t.Fatalf("expected at least 12 captured, got %d", got)
	}
}

// ---------------------------------------------------------------------------
// streamKeyByDoesNotCrash
// ---------------------------------------------------------------------------

func TestStreamKeyByDoesNotCrash(t *testing.T) {
	src := mkName("streamKeyByDoesNotCrash", "src")
	sink := mkName("streamKeyByDoesNotCrash", "sink")
	queryID := mkName("streamKeyByDoesNotCrash", "q")
	_, _ = testClient.Queue(src).Create().Execute(context.Background())
	_, _ = testClient.Queue(sink).Create().Execute(context.Background())

	for i := 0; i < 6; i++ {
		region := "EU"
		if i%2 == 1 {
			region = "US"
		}
		_, _ = testClient.Queue(src).Push(map[string]interface{}{"region": region, "n": i}).Execute(context.Background())
	}

	stream := streams.From(testClient.Queue(src).AsStreamSource()).
		KeyBy(func(m interface{}) (string, error) {
			msg, _ := m.(map[string]interface{})
			data, _ := msg["data"].(map[string]interface{})
			return data["region"].(string), nil
		}).
		Map(func(m interface{}, _ streams.EmitCtx) (interface{}, error) {
			msg, _ := m.(map[string]interface{})
			return msg["data"], nil
		}).
		To(testClient.Queue(sink))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, err := stream.Run(ctx, streams.RunOptions{QueryID: queryID, URL: queenURL, BatchSize: 100, Reset: true})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()

	drained := drainUntil(t, testClient.Queue(sink), untilCount(6), 8*time.Second)
	if len(drained) != 6 {
		t.Fatalf("expected 6 messages, got %d", len(drained))
	}
	if r.Metrics().ErrorsTotal != 0 {
		t.Fatalf("expected 0 errors, got %d", r.Metrics().ErrorsTotal)
	}
}

// ---------------------------------------------------------------------------
// streamAsyncMap
// ---------------------------------------------------------------------------

func TestStreamAsyncMap(t *testing.T) {
	src := mkName("streamAsyncMap", "src")
	sink := mkName("streamAsyncMap", "sink")
	queryID := mkName("streamAsyncMap", "q")
	_, _ = testClient.Queue(src).Create().Execute(context.Background())
	_, _ = testClient.Queue(sink).Create().Execute(context.Background())

	for i := 0; i < 5; i++ {
		_, _ = testClient.Queue(src).Push(map[string]interface{}{"i": i}).Execute(context.Background())
	}

	stream := streams.From(testClient.Queue(src).AsStreamSource()).
		Map(func(m interface{}, _ streams.EmitCtx) (interface{}, error) {
			// Simulate I/O.
			time.Sleep(20 * time.Millisecond)
			msg, _ := m.(map[string]interface{})
			data, _ := msg["data"].(map[string]interface{})
			out := map[string]interface{}{"asyncEnriched": true}
			for k, v := range data {
				out[k] = v
			}
			return out, nil
		}).
		To(testClient.Queue(sink))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, err := stream.Run(ctx, streams.RunOptions{QueryID: queryID, URL: queenURL, BatchSize: 100, Reset: true})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()

	drained := drainUntil(t, testClient.Queue(sink), untilCount(5), 8*time.Second)
	if len(drained) != 5 {
		t.Fatalf("expected 5 enriched messages, got %d", len(drained))
	}
	for _, m := range drained {
		if !dataBool(m, "asyncEnriched") {
			t.Fatalf("expected asyncEnriched=true, got %+v", m.Data)
		}
	}
	_ = strings.Contains // keep the strings import alive for future scenarios
}

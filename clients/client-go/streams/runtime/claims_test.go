// Tests for closing processing-time windows under a claim (broker 2.7, issue
// #153), against a fake broker. The live versions are in
// tests/streams_integration/claims_test.go.
package runtime

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/smartpricing/queen/clients/client-go/v2/streams/operators"
)

// fakeBroker answers the stream routes with canned bodies and records what it
// was sent, by path.
type fakeBroker struct {
	mu     sync.Mutex
	bodies map[string][]map[string]interface{}
	answer func(path string, body map[string]interface{}) (int, interface{})
}

func (f *fakeBroker) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	var body map[string]interface{}
	_ = json.NewDecoder(req.Body).Decode(&body)
	f.mu.Lock()
	f.bodies[req.URL.Path] = append(f.bodies[req.URL.Path], body)
	f.mu.Unlock()
	status, out := f.answer(req.URL.Path, body)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(out)
}

func (f *fakeBroker) sent(path string) []map[string]interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]map[string]interface{}(nil), f.bodies[path]...)
}

func newFakeBroker(t *testing.T, answer func(string, map[string]interface{}) (int, interface{})) (*fakeBroker, string) {
	f := &fakeBroker{bodies: map[string][]map[string]interface{}{}, answer: answer}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	return f, srv.URL
}

// newSessionRunner builds a runner for a processing-time session window (gap
// 3 s, idle flush 250 ms) counting into the queue "sink-q".
func newSessionRunner(url string, nowPtr *int64) *Runner {
	count := operators.NewAggregateOperator(map[string]operators.ExtractorFn{
		"count": func(interface{}) (float64, error) { return 1, nil },
	})
	stream := &CompiledStream{Stages: Stages{
		Window:  operators.NewWindowSessionOperator(3, operators.WithIdleFlushMs(250)),
		Reducer: &count.ReduceOperator,
		Sink:    operators.NewSinkOperator("sink-q", nil),
	}}
	r := NewRunner(stream, RunOptions{QueryID: "q-claims", URL: url})
	r.serverQueryID = "srv-q-claims"
	r.nowMs = func() int64 { return *nowPtr }
	return r
}

func TestCloseDueClosesAClaimedSessionUnderItsLease(t *testing.T) {
	const base = int64(1_700_000_000_000)
	sessionKey := "sess:3\u001fopen\u001ftrace-a"
	f, url := newFakeBroker(t, func(path string, _ map[string]interface{}) (int, interface{}) {
		switch path {
		case "/streams/v1/claim":
			return 200, map[string]interface{}{
				"success": true, "lease_id": "lease-1", "lease_seconds": 30,
				// The broker's clock: past the session's end (base + 3 s).
				"now_ms": base + 5_000,
				"partitions": []map[string]interface{}{
					{"partition_id": "1", "partition": "trace-a", "status": "claimed"},
					{"partition_id": "2", "partition": "trace-b", "status": "busy"},
					{"partition_id": "3", "partition": "trace-c", "status": "backlog"},
					{"partition_id": "4", "partition": "", "status": "missing"},
				},
			}
		case "/streams/v1/state/get":
			return 200, map[string]interface{}{"success": true, "rows": []map[string]interface{}{{
				"key":   sessionKey,
				"value": map[string]interface{}{"acc": map[string]interface{}{"count": 2}, "sessionStart": base - 100, "lastEventTime": base},
			}}}
		case "/streams/v1/cycle":
			return 200, map[string]interface{}{"success": true}
		}
		return 404, map[string]interface{}{"error": "not found"}
	})

	// This machine's clock runs behind the broker's: by it the session (gap
	// 3 s) is not over yet. The close must use the broker's.
	now := base + 1_000
	r := newSessionRunner(url, &now)
	for _, pid := range []string{"1", "2", "3", "4"} {
		r.due[pid] = dueEntry{name: "trace-" + pid, atMs: base}
	}

	r.closeDue(context.Background(), context.Background())

	claims := f.sent("/streams/v1/claim")
	if len(claims) != 1 {
		t.Fatalf("expected one claim call, got %d", len(claims))
	}
	if claims[0]["consumer_group"] != "streams.q-claims" || claims[0]["lease_seconds"] != 30.0 {
		t.Errorf("claim must name the runner's group and a 30 s lease: %v", claims[0])
	}
	cycles := f.sent("/streams/v1/cycle")
	if len(cycles) != 1 {
		t.Fatalf("expected one close cycle (the claimed partition), got %d", len(cycles))
	}
	c := cycles[0]
	ack, _ := c["ack"].(map[string]interface{})
	if ack["leaseId"] != "lease-1" || ack["count"] != 0.0 || ack["status"] != "completed" || ack["transactionId"] != "" {
		t.Errorf("the close must ack the claim's empty batch, got %v", ack)
	}
	if c["release_lease"] != true || c["partition_id"] != "1" || c["consumer_group"] != "streams.q-claims" {
		t.Errorf("unexpected close cycle: %v", c)
	}
	items, _ := c["push_items"].([]interface{})
	if len(items) != 1 {
		t.Fatalf("expected the session emitted once, got %v", items)
	}
	item := items[0].(map[string]interface{})
	if item["queue"] != "sink-q" || item["partition"] != "trace-a" {
		t.Errorf("unexpected push item %v", item)
	}
	ops, _ := c["state_ops"].([]interface{})
	if len(ops) != 1 || ops[0].(map[string]interface{})["type"] != "delete" {
		t.Errorf("the closed session's row must be deleted, got %v", ops)
	}

	if r.claims.Load() != claimsYes {
		t.Errorf("a served claim means the broker has claims")
	}
	if _, ok := r.due["1"]; ok {
		t.Errorf("nothing is open on 1 after its close")
	}
	if d, ok := r.due["2"]; !ok || d.atMs != now+claimRetryMs {
		t.Errorf("busy 2 must be tried again in max(idle flush, 1 s), got %+v (tracked=%v)", d, ok)
	}
	if _, ok := r.due["3"]; ok {
		t.Errorf("backlog 3: the next pop's cycle tracks it, not the due map")
	}
	if _, ok := r.due["4"]; ok {
		t.Errorf("missing 4 must leave the due map")
	}
	if m := r.Metrics(); m.FlushCyclesTotal != 1 || m.ErrorsTotal != 0 {
		t.Errorf("expected 1 flush cycle and no error, got %+v", m)
	}
}

func TestCloseDueFallsBackToTheOldFlushOnABrokerWithoutClaims(t *testing.T) {
	f, url := newFakeBroker(t, func(string, map[string]interface{}) (int, interface{}) {
		return 404, map[string]interface{}{"error": "not found"}
	})
	now := int64(1_700_000_000_000)
	r := newSessionRunner(url, &now)
	r.due["1"] = dueEntry{name: "trace-a", atMs: now}

	r.closeDue(context.Background(), context.Background())

	if r.claims.Load() != claimsNo {
		t.Fatalf("a 404 on the claim route means an older broker")
	}
	if len(f.sent("/streams/v1/cycle")) != 0 {
		t.Errorf("nothing is closed without a claim")
	}
	if r.Metrics().ErrorsTotal != 0 {
		t.Errorf("an older broker is not an error")
	}
}

func TestCloseDueRetriesAfterAFailedClaim(t *testing.T) {
	_, url := newFakeBroker(t, func(string, map[string]interface{}) (int, interface{}) {
		return 400, map[string]interface{}{"error": "bad_request"}
	})
	now := int64(1_700_000_000_000)
	r := newSessionRunner(url, &now)
	r.due["1"] = dueEntry{name: "trace-a", atMs: now - 5}

	r.closeDue(context.Background(), context.Background())

	if r.claims.Load() != claimsUnknown {
		t.Errorf("a failed claim says nothing about the broker")
	}
	if d := r.due["1"]; d.atMs != now+claimRetryMs {
		t.Errorf("expected a retry in 1 s, got %+v", d)
	}
	if r.Metrics().ErrorsTotal != 1 {
		t.Errorf("a failed claim counts as an error")
	}
}

// quietSource is a source with nothing to pop.
type quietSource struct{}

func (s quietSource) Name() string                   { return "src" }
func (s quietSource) Batch(int) Source               { return s }
func (s quietSource) Wait(bool) Source               { return s }
func (s quietSource) TimeoutMillis(int) Source       { return s }
func (s quietSource) Group(string) Source            { return s }
func (s quietSource) Partitions(int) Source          { return s }
func (s quietSource) SubscriptionMode(string) Source { return s }
func (s quietSource) SubscriptionFrom(string) Source { return s }
func (s quietSource) Conflation(bool) Source         { return s }
func (s quietSource) Pop(ctx context.Context) ([]Message, error) {
	select {
	case <-ctx.Done():
		return nil, nil
	case <-time.After(50 * time.Millisecond):
		return nil, nil
	}
}

// Stop lets a close in flight finish: cut short, it would keep its claim on
// the partition until the lease ran out.
func TestStopWaitsForACloseInFlight(t *testing.T) {
	now := time.Now().UnixMilli()
	cycleStarted := make(chan struct{})
	var once sync.Once
	_, url := newFakeBroker(t, func(path string, _ map[string]interface{}) (int, interface{}) {
		switch path {
		case "/streams/v1/queries":
			return 200, map[string]interface{}{"success": true, "query_id": "srv-q-claims"}
		case "/streams/v1/state/partitions":
			return 200, map[string]interface{}{"success": true, "next": nil, "partitions": []map[string]interface{}{
				{"partition_id": "1", "partition": "trace-a"},
			}}
		case "/streams/v1/claim":
			return 200, map[string]interface{}{"success": true, "lease_id": "lease-1", "now_ms": now, "partitions": []map[string]interface{}{
				{"partition_id": "1", "partition": "trace-a", "status": "claimed"},
			}}
		case "/streams/v1/state/get":
			return 200, map[string]interface{}{"success": true, "rows": []map[string]interface{}{{
				"key":   "sess:3\u001fopen\u001ftrace-a",
				"value": map[string]interface{}{"acc": map[string]interface{}{"count": 1}, "sessionStart": now - 10_000, "lastEventTime": now - 10_000},
			}}}
		case "/streams/v1/cycle":
			once.Do(func() { close(cycleStarted) })
			time.Sleep(700 * time.Millisecond)
			return 200, map[string]interface{}{"success": true}
		}
		return 404, map[string]interface{}{"error": "not found"}
	})
	r := newSessionRunner(url, &now)
	r.stream.Source = quietSource{}
	if err := r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-cycleStarted:
	case <-time.After(5 * time.Second):
		r.Stop()
		t.Fatal("the close never started")
	}
	r.Stop()
	if m := r.Metrics(); m.FlushCyclesTotal != 1 || m.ErrorsTotal != 0 {
		t.Fatalf("Stop must let the close commit: %+v", m)
	}
}

func TestSweepTracksEveryPageAndKeepsWhatIsTracked(t *testing.T) {
	f, url := newFakeBroker(t, func(_ string, body map[string]interface{}) (int, interface{}) {
		if body["after"] == nil {
			return 200, map[string]interface{}{"success": true, "next": "2", "partitions": []map[string]interface{}{
				{"partition_id": "1", "partition": "a"}, {"partition_id": "2", "partition": "b"},
			}}
		}
		return 200, map[string]interface{}{"success": true, "next": nil, "partitions": []map[string]interface{}{
			{"partition_id": "3", "partition": "c"},
		}}
	})
	now := int64(1_700_000_000_000)
	r := newSessionRunner(url, &now)
	r.due["2"] = dueEntry{name: "b", atMs: now + 60_000}

	r.sweep(context.Background(), context.Background(), 0)
	r.sweep(context.Background(), context.Background(), sweepUntouchedMs)

	pages := f.sent("/streams/v1/state/partitions")
	if len(pages) != 4 {
		t.Fatalf("expected 2 pages per sweep, got %d calls", len(pages))
	}
	if _, has := pages[0]["untouched_for_ms"]; has {
		t.Errorf("the first sweep takes every partition: %v", pages[0])
	}
	if pages[1]["after"] != "2" || pages[0]["limit"] != 1000.0 {
		t.Errorf("pages go by `next` in 1000s: %v / %v", pages[0], pages[1])
	}
	if pages[2]["untouched_for_ms"] != 30_000.0 {
		t.Errorf("later sweeps take partitions untouched for 30 s: %v", pages[2])
	}
	if d := r.due["1"]; d.atMs != now || d.name != "a" {
		t.Errorf("a found partition is due now: %+v", d)
	}
	if d := r.due["2"]; d.atMs != now+60_000 {
		t.Errorf("a tracked partition keeps its due time: %+v", d)
	}
	if d := r.due["3"]; d.atMs != now || d.name != "c" {
		t.Errorf("the second page is tracked too: %+v", d)
	}
}

package queen

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// Client-side 429/403 handling tests (PLAN_QUEEN_PROXY_CLOUD.md §4/§9,
// blocker B4 -- "client 429/backoff work ... mandatory pre-enforcement").
//
// The proxy error contract under test:
//
//	429  Retry-After: <seconds>  {"error": "...", "code": "rate_limited" | "quota_exceeded"}
//	403                          {"error": "...", "code": "cluster_suspended" | "storage_quota_exceeded"
//	                                                      | "feature_gated" | "forbidden"}
//
// These run against a real local httptest server (no broker), mirroring
// ack_test.go's style.

// planResponse is one canned server response for the test double below.
type planResponse struct {
	status     int
	body       interface{} // encoded as JSON; use []interface{} for a raw array body
	retryAfter string      // Retry-After header value, when non-empty
}

type recordedHit struct {
	method string
	path   string
	atMs   int64
}

func rateLimited429(retryAfter string) planResponse {
	return planResponse{
		status:     http.StatusTooManyRequests,
		body:       map[string]interface{}{"error": "slow down", "code": "rate_limited"},
		retryAfter: retryAfter,
	}
}

func repeatResponse(resp planResponse, n int) []planResponse {
	out := make([]planResponse, n)
	for i := range out {
		out[i] = resp
	}
	return out
}

// newPlanServer starts an httptest server that serves `plan` in request
// order, falling back to `defaultResp` once exhausted. Returns the server
// URL and a hit log (method/path/arrival time) for assertions. The server
// and hit log are safe to read after the test body returns (t.Cleanup runs
// Close before the caller inspects final counts in most tests, but the log
// itself is only appended to from the single httptest handler goroutine at
// a time thanks to the mutex).
func newPlanServer(t *testing.T, plan []planResponse, defaultResp planResponse) (string, *[]recordedHit) {
	t.Helper()
	hits := &[]recordedHit{}
	start := time.Now()
	var mu sync.Mutex
	idx := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		resp := defaultResp
		if idx < len(plan) {
			resp = plan[idx]
		}
		idx++
		*hits = append(*hits, recordedHit{method: r.Method, path: r.URL.Path, atMs: time.Since(start).Milliseconds()})
		mu.Unlock()

		if resp.retryAfter != "" {
			w.Header().Set("Retry-After", resp.retryAfter)
		}
		w.Header().Set("Content-Type", "application/json")
		status := resp.status
		if status == 0 {
			status = http.StatusOK
		}
		w.WriteHeader(status)
		body := resp.body
		if body == nil {
			body = map[string]interface{}{"ok": true}
		}
		_ = json.NewEncoder(w).Encode(body)
	}))
	t.Cleanup(server.Close)
	return server.URL, hits
}

func hitCount(hits *[]recordedHit) int {
	return len(*hits)
}

// ---------------------------------------------------------------------------
// HttpClient (direct) -- the centralized retry429 mechanism itself.
// ---------------------------------------------------------------------------

func TestHttpClient429HonorsRetryAfter(t *testing.T) {
	url, hits := newPlanServer(t, []planResponse{rateLimited429("0")}, planResponse{status: 200, body: map[string]interface{}{"ok": true}})
	client, err := NewHttpClient(ClientConfig{URL: url, Retry429: &Retry429Config{BaseMs: 5, CapMs: 50}})
	if err != nil {
		t.Fatalf("NewHttpClient: %v", err)
	}
	defer client.Close()

	result, err := client.Get(context.Background(), "/x", 0, "")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if result["ok"] != true {
		t.Errorf("unexpected result: %+v", result)
	}
	if hitCount(hits) != 2 {
		t.Errorf("hits = %d, want 2 (one 429 then one success)", hitCount(hits))
	}
}

func TestHttpClient429ExponentialBackoffGrows(t *testing.T) {
	url, hits := newPlanServer(t, repeatResponse(rateLimited429(""), 2), planResponse{status: 200, body: map[string]interface{}{"ok": true}})
	client, err := NewHttpClient(ClientConfig{URL: url, Retry429: &Retry429Config{BaseMs: 20, CapMs: 2000}})
	if err != nil {
		t.Fatalf("NewHttpClient: %v", err)
	}
	defer client.Close()

	if _, err := client.Get(context.Background(), "/x", 0, ""); err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	h := *hits
	if len(h) != 3 {
		t.Fatalf("hits = %d, want 3", len(h))
	}
	gap1 := h[1].atMs - h[0].atMs
	gap2 := h[2].atMs - h[1].atMs
	if float64(gap2) <= float64(gap1)*1.2 {
		t.Errorf("expected gap2 (%dms) to exceed gap1 (%dms) by backoff growth", gap2, gap1)
	}
}

func TestHttpClient429GivesUpAfterMaxAttempts(t *testing.T) {
	url, hits := newPlanServer(t, nil, rateLimited429(""))
	client, err := NewHttpClient(ClientConfig{URL: url, Retry429: &Retry429Config{MaxAttempts: 3, BaseMs: 1, CapMs: 5}})
	if err != nil {
		t.Fatalf("NewHttpClient: %v", err)
	}
	defer client.Close()

	_, err = client.Post(context.Background(), "/api/v1/push", map[string]interface{}{"items": []interface{}{}})
	if err == nil {
		t.Fatal("expected an error after exhausting retry429.MaxAttempts")
	}
	httpErr, ok := err.(*HTTPError)
	if !ok {
		t.Fatalf("expected *HTTPError, got %T: %v", err, err)
	}
	if httpErr.StatusCode != 429 || httpErr.Code != "rate_limited" {
		t.Errorf("HTTPError = %+v, want status 429 code rate_limited", httpErr)
	}
	if hitCount(hits) != 3 {
		t.Errorf("hits = %d, want exactly maxAttempts (3)", hitCount(hits))
	}
}

func TestHttpClient429DefaultsToTenAttempts(t *testing.T) {
	url, hits := newPlanServer(t, nil, rateLimited429(""))
	client, err := NewHttpClient(ClientConfig{URL: url, Retry429: &Retry429Config{BaseMs: 1, CapMs: 2}})
	if err != nil {
		t.Fatalf("NewHttpClient: %v", err)
	}
	defer client.Close()

	_, err = client.Post(context.Background(), "/api/v1/push", map[string]interface{}{"items": []interface{}{}})
	if err == nil {
		t.Fatal("expected an error")
	}
	if hitCount(hits) != 10 {
		t.Errorf("hits = %d, want 10 (default push maxAttempts)", hitCount(hits))
	}
}

func TestHttpClient429PopRetriesPastPushDefault(t *testing.T) {
	url, hits := newPlanServer(t, repeatResponse(rateLimited429("0"), 14), planResponse{
		status: 200,
		body:   map[string]interface{}{"messages": []interface{}{map[string]interface{}{"transactionId": "m1"}}},
	})
	client, err := NewHttpClient(ClientConfig{URL: url, Retry429: &Retry429Config{BaseMs: 1, CapMs: 5}})
	if err != nil {
		t.Fatalf("NewHttpClient: %v", err)
	}
	defer client.Close()

	result, err := client.Get(context.Background(), "/api/v1/pop", 0, "", WithLongPollRetry())
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if result == nil || result["messages"] == nil {
		t.Errorf("unexpected result: %+v", result)
	}
	if hitCount(hits) != 15 {
		t.Errorf("hits = %d, want 15 -- pop must not give up at the push default of 10 attempts", hitCount(hits))
	}
}

func TestHttpClient429ExplicitMaxAttemptsAppliesToPopToo(t *testing.T) {
	url, hits := newPlanServer(t, nil, rateLimited429(""))
	client, err := NewHttpClient(ClientConfig{URL: url, Retry429: &Retry429Config{MaxAttempts: 2, BaseMs: 1, CapMs: 5}})
	if err != nil {
		t.Fatalf("NewHttpClient: %v", err)
	}
	defer client.Close()

	_, err = client.Get(context.Background(), "/api/v1/pop", 0, "", WithLongPollRetry())
	if err == nil {
		t.Fatal("expected an error")
	}
	if hitCount(hits) != 2 {
		t.Errorf("hits = %d, want 2 -- explicit MaxAttempts bounds pop as well as push", hitCount(hits))
	}
}

func TestHttpClient403NeverRetriesAndPreservesCode(t *testing.T) {
	url, hits := newPlanServer(t, nil, planResponse{status: 403, body: map[string]interface{}{"error": "cluster suspended", "code": "cluster_suspended"}})
	client, err := NewHttpClient(ClientConfig{URL: url})
	if err != nil {
		t.Fatalf("NewHttpClient: %v", err)
	}
	defer client.Close()

	_, err = client.Post(context.Background(), "/api/v1/push", map[string]interface{}{"items": []interface{}{}})
	if err == nil {
		t.Fatal("expected an error")
	}
	httpErr, ok := err.(*HTTPError)
	if !ok {
		t.Fatalf("expected *HTTPError, got %T: %v", err, err)
	}
	if httpErr.StatusCode != 403 || httpErr.Code != "cluster_suspended" || !httpErr.IsClusterSuspended() {
		t.Errorf("HTTPError = %+v, want 403/cluster_suspended", httpErr)
	}
	if hitCount(hits) != 1 {
		t.Errorf("hits = %d, want 1 -- 403 must not be retried", hitCount(hits))
	}
}

func TestHttpClient400NotRetried(t *testing.T) {
	url, hits := newPlanServer(t, nil, planResponse{status: 400, body: map[string]interface{}{"error": "bad request"}})
	client, err := NewHttpClient(ClientConfig{URL: url})
	if err != nil {
		t.Fatalf("NewHttpClient: %v", err)
	}
	defer client.Close()

	if _, err := client.Get(context.Background(), "/x", 0, ""); err == nil {
		t.Fatal("expected an error")
	}
	if hitCount(hits) != 1 {
		t.Errorf("hits = %d, want 1", hitCount(hits))
	}
}

// ---------------------------------------------------------------------------
// Queen public API wiring: config plumbing (Retry429) + call-site marking
// (push vs. wait=true pop) actually reach HttpClient.
// ---------------------------------------------------------------------------

func TestQueenPushRetries429ThenSucceeds(t *testing.T) {
	url, hits := newPlanServer(t, []planResponse{rateLimited429("0")}, planResponse{
		status: 200,
		body:   []interface{}{map[string]interface{}{"status": "queued", "transactionId": "tx-1"}},
	})
	client, err := New(ClientConfig{URL: url, Retry429: &Retry429Config{BaseMs: 5, CapMs: 50}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer client.Close(context.Background())

	responses, err := client.Queue("q1").Push(map[string]interface{}{"hello": "world"}).Execute(context.Background())
	if err != nil {
		t.Fatalf("Push failed: %v", err)
	}
	if len(responses) != 1 || responses[0].Status != "queued" {
		t.Errorf("responses = %+v, want a single queued response", responses)
	}
	if hitCount(hits) != 2 {
		t.Errorf("hits = %d, want 2", hitCount(hits))
	}
}

func TestQueenPushSurfacesTerminal403(t *testing.T) {
	url, hits := newPlanServer(t, nil, planResponse{status: 403, body: map[string]interface{}{"error": "over quota", "code": "storage_quota_exceeded"}})
	client, err := New(ClientConfig{URL: url})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer client.Close(context.Background())

	_, err = client.Queue("q1").Push(map[string]interface{}{"hello": "world"}).Execute(context.Background())
	if err == nil {
		t.Fatal("expected push to fail")
	}
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) {
		t.Fatalf("expected an *HTTPError in the chain, got %T: %v", err, err)
	}
	if httpErr.StatusCode != 403 || httpErr.Code != "storage_quota_exceeded" {
		t.Errorf("HTTPError = %+v, want 403/storage_quota_exceeded", httpErr)
	}
	if hitCount(hits) != 1 {
		t.Errorf("hits = %d, want 1 -- 403 must not be retried", hitCount(hits))
	}
}

func TestQueenPopRidesOutMore429sThanPushDefault(t *testing.T) {
	url, hits := newPlanServer(t, repeatResponse(rateLimited429("0"), 12), planResponse{
		status: 200,
		body: map[string]interface{}{"messages": []interface{}{
			map[string]interface{}{"transactionId": "tx-1", "partitionId": "p-1", "data": map[string]interface{}{"x": 1}},
		}},
	})
	client, err := New(ClientConfig{URL: url, Retry429: &Retry429Config{BaseMs: 1, CapMs: 5}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer client.Close(context.Background())

	messages, err := client.Queue("q1").Wait(true).Pop(context.Background())
	if err != nil {
		t.Fatalf("Pop failed: %v", err)
	}
	if len(messages) != 1 {
		t.Fatalf("messages = %d, want 1", len(messages))
	}
	if hitCount(hits) != 13 {
		t.Errorf("hits = %d, want 13", hitCount(hits))
	}
}

// ---------------------------------------------------------------------------
// ConsumerManager worker loop: the actual hot-loop bug (B4) this task fixes
// -- Consume() must back off through 429s and stop cleanly on a terminal
// 403 instead of spinning with no delay or hanging indefinitely.
// ---------------------------------------------------------------------------

func TestConsumeBacksOffThrough429AndDeliversMessage(t *testing.T) {
	url, hits := newPlanServer(t, repeatResponse(rateLimited429("0"), 3), planResponse{
		status: 200,
		body: map[string]interface{}{"messages": []interface{}{
			map[string]interface{}{"transactionId": "tx-1", "partitionId": "p-1", "data": map[string]interface{}{"x": 1}},
		}},
	})
	client, err := New(ClientConfig{URL: url, Retry429: &Retry429Config{BaseMs: 1, CapMs: 5}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer client.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var mu sync.Mutex
	var received []*Message
	err = client.Queue("q1").Wait(true).Limit(1).AutoAck(false).Consume(ctx, func(_ context.Context, msg *Message) error {
		mu.Lock()
		received = append(received, msg)
		mu.Unlock()
		return nil
	}).Execute(ctx)
	if err != nil {
		t.Fatalf("Consume failed: %v", err)
	}
	if len(received) != 1 || received[0].TransactionID != "tx-1" {
		t.Errorf("received = %+v, want a single tx-1 message", received)
	}
	if hitCount(hits) < 4 {
		t.Errorf("hits = %d, want >= 4 (3 rate-limited attempts + the final success)", hitCount(hits))
	}
}

func TestConsumeStopsOnTerminal403(t *testing.T) {
	url, hits := newPlanServer(t, nil, planResponse{status: 403, body: map[string]interface{}{"error": "cluster suspended", "code": "cluster_suspended"}})
	client, err := New(ClientConfig{URL: url})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer client.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = client.Queue("q1").Wait(true).Consume(ctx, func(_ context.Context, _ *Message) error {
		return nil
	}).Execute(ctx)

	if err == nil {
		t.Fatal("expected Consume to stop with an error")
	}
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || !httpErr.IsClusterSuspended() {
		t.Errorf("err = %v, want a cluster_suspended HTTPError", err)
	}
	if hitCount(hits) != 1 {
		t.Errorf("hits = %d, want 1 -- must stop after the first 403, not hot-loop", hitCount(hits))
	}
}

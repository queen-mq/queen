package tests

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestPush_* mirror clients/client-js/test-v2/push.js and
// clients/client-py/tests/test_push.py via 'queenctl push'.

// TestPush_Simple covers test-v2/push.js#pushMessage.
func TestPush_Simple(t *testing.T) {
	q := uniqueQueue(t, "push-simple")
	createQueue(t, q)
	out := pushOne(t, q, "", map[string]any{"message": "Hello, world!"})
	if !strings.Contains(out, "queued=1") {
		t.Errorf("push summary should show queued=1, got: %s", out)
	}
}

// TestPush_DuplicateTransactionId mirrors push.js#pushDuplicateMessage.
func TestPush_DuplicateTransactionId(t *testing.T) {
	q := uniqueQueue(t, "push-dup")
	createQueue(t, q)
	body := mustNDJSON(t, []wrappedTestItem{
		{TransactionID: "ct-dup-id", Data: map[string]any{"i": 1}},
	})
	first := runPushWrapped(t, q, "", body)
	if first.queued != 1 || first.duplicate != 0 {
		t.Errorf("first push: queued=%d duplicate=%d, want 1/0", first.queued, first.duplicate)
	}
	second := runPushWrapped(t, q, "", body)
	if second.queued != 0 || second.duplicate != 1 {
		t.Errorf("second push: queued=%d duplicate=%d, want 0/1", second.queued, second.duplicate)
	}
}

// TestPush_DuplicateOnSamePartition mirrors push.js#pushDuplicateMessageOnSpecificPartition.
func TestPush_DuplicateOnSamePartition(t *testing.T) {
	q := uniqueQueue(t, "push-dup-part")
	createQueue(t, q)
	body := mustNDJSON(t, []wrappedTestItem{
		{TransactionID: "ct-dup-part", Data: map[string]any{"hello": "world"}},
	})
	first := runPushWrapped(t, q, "p0", body)
	second := runPushWrapped(t, q, "p0", body)
	if first.queued != 1 || second.duplicate != 1 {
		t.Errorf("got first=%+v second=%+v", first, second)
	}
}

// TestPush_DuplicateAcrossPartitionsIsAllowed mirrors push.js#pushDuplicateMessageOnDifferentPartition.
// The same transactionId in two distinct partitions must NOT be treated as a
// duplicate (each partition is its own dedup scope).
func TestPush_DuplicateAcrossPartitionsIsAllowed(t *testing.T) {
	q := uniqueQueue(t, "push-cross-part")
	createQueue(t, q)
	body := mustNDJSON(t, []wrappedTestItem{
		{TransactionID: "ct-cross", Data: map[string]any{"x": 1}},
	})
	a := runPushWrapped(t, q, "p0", body)
	b := runPushWrapped(t, q, "p1", body)
	if a.queued != 1 || b.queued != 1 {
		t.Errorf("expected queued in both partitions, got a=%+v b=%+v", a, b)
	}
}

// TestPush_TransactionIdAtFlag verifies the --transaction-id shortcut sets
// the first item's id (matching the SDK PushBuilder.TransactionID).
func TestPush_TransactionIdAtFlag(t *testing.T) {
	q := uniqueQueue(t, "push-txid-flag")
	createQueue(t, q)
	out := runOK(t, "push", q, "--transaction-id", "ct-flag-tx", "--data", `{"a":1}`)
	if !strings.Contains(out, "queued=1") {
		t.Fatalf("first push: %s", out)
	}
	// Second push with the same flag must be flagged duplicate.
	out2 := runOK(t, "push", q, "--transaction-id", "ct-flag-tx", "--data", `{"a":2}`)
	if !strings.Contains(out2, "duplicate=1") {
		t.Errorf("second push should be duplicate, got: %s", out2)
	}
}

// TestPush_LargePayload pushes 10000 elements (≈1 MB) and verifies the
// payload survives the round-trip intact. Mirrors push.js#pushLargePayload.
func TestPush_LargePayload(t *testing.T) {
	q := uniqueQueue(t, "push-large")
	createQueue(t, q)
	arr := make([]map[string]any, 10_000)
	for i := range arr {
		arr[i] = map[string]any{
			"id":   fmt.Sprintf("%032x", i),
			"data": strings.Repeat("x", 100),
			"nested": map[string]any{
				"field1": "value1",
				"field2": "value2",
				"field3": "value3",
			},
		}
	}
	payload := map[string]any{"array": arr, "metadata": map[string]any{"size": len(arr)}}
	pushOne(t, q, "", payload)

	// wait(true) rides out the PUSHPOPLOOKUPSOL race documented in
	// push.js#pushLargePayload.
	got := popN(t, q, 1, "--cg", "ct-large", "--auto-ack")
	if len(got) != 1 {
		t.Fatalf("popped %d, want 1", len(got))
	}
	a, _ := got[0].Data["array"].([]any)
	if len(a) != 10_000 {
		t.Errorf("round-tripped array length = %d, want 10000", len(a))
	}
}

// TestPush_NullPayload mirrors push.js#pushNullPayload.
func TestPush_NullPayload(t *testing.T) {
	q := uniqueQueue(t, "push-null")
	createQueue(t, q)
	pushOne(t, q, "", nil)
	got := popN(t, q, 1, "--cg", "ct-null", "--auto-ack")
	if len(got) != 1 || got[0].Data != nil {
		t.Errorf("got %d msgs, data=%v; want 1 with null data", len(got), got[0].Data)
	}
}

// TestPush_EmptyPayload mirrors push.js#pushEmptyPayload.
func TestPush_EmptyPayload(t *testing.T) {
	q := uniqueQueue(t, "push-empty")
	createQueue(t, q)
	pushOne(t, q, "", map[string]any{})
	got := popN(t, q, 1, "--cg", "ct-empty", "--auto-ack")
	if len(got) != 1 || len(got[0].Data) != 0 {
		t.Errorf("got %d msgs, data=%v; want 1 with empty object", len(got), got[0].Data)
	}
}

// TestPush_DelayedProcessing mirrors push.js#pushDelayedMessage.
func TestPush_DelayedProcessing(t *testing.T) {
	q := uniqueQueue(t, "push-delayed")
	runOK(t, "queue", "configure", q, "--delayed-processing", "2")
	pushOne(t, q, "", map[string]any{"hello": "world"})

	// Immediate non-blocking pop must yield nothing.
	got := popN(t, q, 1, "--cg", "ct-delay", "--auto-ack", "--wait=false", "--timeout", "100ms")
	if len(got) != 0 {
		t.Fatalf("immediate pop returned %d msgs, expected 0 (delayed)", len(got))
	}
	time.Sleep(2500 * time.Millisecond)
	got = popN(t, q, 1, "--cg", "ct-delay", "--auto-ack", "--timeout", "5s")
	if len(got) != 1 {
		t.Errorf("post-delay pop returned %d, expected 1", len(got))
	}
}

// TestPush_WindowBuffer mirrors push.js#pushWindowBuffer.
func TestPush_WindowBuffer(t *testing.T) {
	q := uniqueQueue(t, "push-window")
	runOK(t, "queue", "configure", q, "--window-buffer", "2")
	pushNDJSON(t, q, "", []any{
		map[string]any{"i": 1},
		map[string]any{"i": 2},
		map[string]any{"i": 3},
	})
	// Inside the window, the broker should not surface anything yet.
	got := popN(t, q, 1, "--cg", "ct-win", "--auto-ack", "--wait=false", "--timeout", "100ms")
	if len(got) != 0 {
		t.Fatalf("inside window: pop returned %d, expected 0", len(got))
	}
	time.Sleep(2500 * time.Millisecond)
	got = popN(t, q, 4, "--cg", "ct-win", "--auto-ack", "--wait=false")
	if len(got) != 3 {
		t.Errorf("post-window pop returned %d, expected 3", len(got))
	}
}

// TestPush_IntraBatchDuplicatesAllNew mirrors push.js#testPushIntraBatchDuplicatesAllNew.
func TestPush_IntraBatchDuplicatesAllNew(t *testing.T) {
	q := uniqueQueue(t, "push-intra-new")
	createQueue(t, q)
	body := mustNDJSON(t, []wrappedTestItem{
		{TransactionID: "ct-intra-new", Data: map[string]any{"i": 1}},
		{TransactionID: "ct-intra-new", Data: map[string]any{"i": 2}},
		{TransactionID: "ct-intra-new", Data: map[string]any{"i": 3}},
	})
	res := runPushWrapped(t, q, "", body)
	if res.queued != 1 || res.duplicate != 2 {
		t.Errorf("intra-batch dup all-new: got queued=%d duplicate=%d, want 1/2", res.queued, res.duplicate)
	}
}

// TestPush_IntraBatchDuplicatesMatchingPreExisting mirrors
// push.js#testPushIntraBatchDuplicatesMatchingPreExisting.
func TestPush_IntraBatchDuplicatesMatchingPreExisting(t *testing.T) {
	q := uniqueQueue(t, "push-intra-existing")
	createQueue(t, q)
	seed := mustNDJSON(t, []wrappedTestItem{
		{TransactionID: "ct-seed", Data: map[string]any{"seed": true}},
	})
	if r := runPushWrapped(t, q, "", seed); r.queued != 1 {
		t.Fatalf("seed push: got %+v", r)
	}
	body := mustNDJSON(t, []wrappedTestItem{
		{TransactionID: "ct-seed", Data: map[string]any{"i": 1}},
		{TransactionID: "ct-seed", Data: map[string]any{"i": 2}},
	})
	res := runPushWrapped(t, q, "", body)
	if res.queued != 0 || res.duplicate != 2 {
		t.Errorf("intra-batch matching pre-existing: got %+v, want 0/2", res)
	}
}

// TestPush_ConcurrentSameTransactionId mirrors push.js#testPushConcurrentBatchesToSameKey.
// Exactly one push must win, the other 7 must report duplicate.
func TestPush_ConcurrentSameTransactionId(t *testing.T) {
	q := uniqueQueue(t, "push-conc-same")
	createQueue(t, q)
	const N = 8
	body := mustNDJSON(t, []wrappedTestItem{
		{TransactionID: "ct-conc", Data: map[string]any{"i": 1}},
	})
	var wg sync.WaitGroup
	results := make([]pushSummary, N)
	for i := 0; i < N; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i] = runPushWrapped(t, q, "", body)
		}()
	}
	wg.Wait()
	queued, dup := 0, 0
	for _, r := range results {
		queued += r.queued
		dup += r.duplicate
	}
	if queued != 1 || dup != N-1 {
		t.Errorf("concurrent same-key: queued=%d duplicate=%d, want 1/%d", queued, dup, N-1)
	}
}

// TestPush_MissingTransactionIdGeneratesUnique mirrors
// push.js#testPushMissingTransactionId. 100 items, no txn id, all must queue
// with distinct ids.
func TestPush_MissingTransactionIdGeneratesUnique(t *testing.T) {
	q := uniqueQueue(t, "push-missing-txn")
	createQueue(t, q)
	items := make([]any, 100)
	for i := range items {
		items[i] = map[string]any{"i": i}
	}
	out := pushNDJSONWithStdout(t, q, "", items)
	if !strings.Contains(out, "queued=100") {
		t.Errorf("expected queued=100, got: %s", out)
	}
}

// TestPush_EmptyArrayIsNoOp mirrors push.js#testPushEmptyArray. The CLI
// shape: empty stdin -> exit 4 (CodeEmpty) with no items pushed.
func TestPush_EmptyArrayIsNoOp(t *testing.T) {
	q := uniqueQueue(t, "push-empty-arr")
	createQueue(t, q)
	_, _, code := runWith(stdinFromString(""), "push", q)
	if code != 4 {
		t.Errorf("empty stdin: got exit %d, want 4 (Empty)", code)
	}
}

// TestPush_EncryptedPayload mirrors push.js#pushEncryptedPayload.
func TestPush_EncryptedPayload(t *testing.T) {
	q := uniqueQueue(t, "push-enc")
	runOK(t, "queue", "configure", q, "--encrypt")
	pushOne(t, q, "", map[string]any{"message": "secret-payload"})
	got := popN(t, q, 1, "--cg", "ct-enc", "--auto-ack", "--timeout", "5s")
	if len(got) != 1 {
		t.Fatalf("encrypted pop returned %d, want 1", len(got))
	}
	if got[0].Data["message"] != "secret-payload" {
		t.Errorf("decrypted payload = %v, want 'secret-payload'", got[0].Data["message"])
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

type wrappedTestItem struct {
	TransactionID string `json:"transactionId,omitempty"`
	TraceID       string `json:"traceId,omitempty"`
	Partition     string `json:"partition,omitempty"`
	Data          any    `json:"data"`
}

type pushSummary struct {
	queued    int
	duplicate int
	failed    int
}

func mustNDJSON(t *testing.T, items []wrappedTestItem) string {
	t.Helper()
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, it := range items {
		if err := enc.Encode(it); err != nil {
			t.Fatalf("encode: %v", err)
		}
	}
	return buf.String()
}

// runPushWrapped invokes 'queenctl push --wrapped' with the given NDJSON body
// and parses the summary line.
func runPushWrapped(t *testing.T, queue, partition, body string) pushSummary {
	t.Helper()
	args := []string{"push", queue, "--wrapped"}
	if partition != "" {
		args = append(args, "--partition", partition)
	}
	out, stderr, code := runWith(stdinFromString(body), args...)
	if code != 0 && code != 4 {
		t.Fatalf("push --wrapped: exit %d\nstdout: %s\nstderr: %s", code, out, stderr)
	}
	return parsePushSummary(out)
}

// pushNDJSONWithStdout returns the summary line text so callers can assert on
// the printed counters.
func pushNDJSONWithStdout(t *testing.T, queue, partition string, payloads []any) string {
	t.Helper()
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, p := range payloads {
		if err := enc.Encode(p); err != nil {
			t.Fatalf("encode: %v", err)
		}
	}
	args := []string{"push", queue}
	if partition != "" {
		args = append(args, "--partition", partition)
	}
	out, stderr, code := runWith(runOpts{stdin: &buf}, args...)
	if code != 0 {
		t.Fatalf("push: exit %d\nstdout: %s\nstderr: %s", code, out, stderr)
	}
	return out
}

// parsePushSummary extracts queued=N duplicate=N failed=N from the summary
// line emitted by `queenctl push`.
func parsePushSummary(out string) pushSummary {
	var s pushSummary
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "queued=") {
			continue
		}
		fmt.Sscanf(line, "queued=%d duplicate=%d failed=%d", &s.queued, &s.duplicate, &s.failed)
		return s
	}
	return s
}

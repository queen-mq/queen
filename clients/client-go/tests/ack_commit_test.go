// Ack-as-commit contract tests (parity with client-js test-v2/semantics.js
// items 14-18 and client-py tests/test_ack_commit.py).
//
// Queen's ack is an offset commit: acking message N implicitly completes every
// silent gap before it. These pin the honesty guarantees layered on top:
//
//   - An explicit `failed` nack in the same ack call as later completed acks
//     clamps the cursor — the nacked message and everything after it
//     redelivers (a nack is never silently swallowed by a later success).
//   - Same clamp for `retry`, without charging the retry budget.
//   - A nack that resolves BELOW the committed cursor is rejected with an
//     'already committed' error instead of a silent no-op.
//   - A completed ack below the cursor succeeds but is flagged noop:true.
//   - Each() consumers abandon the rest of the popped batch after a nack
//     (the lease is dead: continuing would only produce duplicates).

package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	queen "github.com/smartpricing/queen/clients/client-go"
)

type ackItemResult struct {
	Index         int    `json:"index"`
	TransactionID string `json:"transactionId"`
	Success       bool   `json:"success"`
	Error         string `json:"error"`
	Noop          bool   `json:"noop"`
}

// rawAckBatch posts /api/v1/ack/batch with per-item statuses (the typed Ack
// API only expresses one bool for the whole call).
func rawAckBatch(t *testing.T, msgs []*queen.Message, statuses []string) []ackItemResult {
	t.Helper()
	acks := make([]map[string]interface{}, len(msgs))
	for i, m := range msgs {
		acks[i] = map[string]interface{}{
			"transactionId": m.TransactionID,
			"partitionId":   m.PartitionID,
			"status":        statuses[i],
		}
		if m.LeaseID != "" {
			acks[i]["leaseId"] = m.LeaseID
		}
	}
	buf, _ := json.Marshal(map[string]interface{}{"acknowledgments": acks})
	res, err := http.Post(serverURL+"/api/v1/ack/batch", "application/json", strings.NewReader(string(buf)))
	if err != nil {
		t.Fatalf("ack batch: %v", err)
	}
	defer res.Body.Close()
	var results []ackItemResult
	if err := json.NewDecoder(res.Body).Decode(&results); err != nil {
		t.Fatalf("ack batch decode: %v", err)
	}
	if len(results) != len(msgs) {
		t.Fatalf("ack batch returned %d results, want %d", len(results), len(msgs))
	}
	return results
}

// rawAckFull is rawAck returning the whole per-item result (incl. noop).
func rawAckFull(t *testing.T, txID, partitionID, status, leaseID string) ackItemResult {
	t.Helper()
	body := map[string]interface{}{
		"transactionId": txID,
		"partitionId":   partitionID,
		"status":        status,
	}
	if leaseID != "" {
		body["leaseId"] = leaseID
	}
	buf, _ := json.Marshal(body)
	res, err := http.Post(serverURL+"/api/v1/ack", "application/json", strings.NewReader(string(buf)))
	if err != nil {
		t.Fatalf("ack: %v", err)
	}
	defer res.Body.Close()
	var results []ackItemResult
	if err := json.NewDecoder(res.Body).Decode(&results); err != nil {
		t.Fatalf("ack decode: %v", err)
	}
	if len(results) == 0 {
		t.Fatalf("ack returned no results")
	}
	return results[0]
}

func redeliveredNs(msgs []*queen.Message) []int {
	ns := make([]int, 0, len(msgs))
	for _, m := range msgs {
		if v, ok := m.Data["n"].(float64); ok {
			ns = append(ns, int(v))
		}
	}
	sort.Ints(ns)
	return ns
}

func pushRange(ctx context.Context, t *testing.T, client *queen.Queen, queueName string, from, to int) {
	t.Helper()
	for i := from; i <= to; i++ {
		if _, err := client.Queue(queueName).Partition("Default").
			Push(map[string]interface{}{"n": i}).Execute(ctx); err != nil {
			t.Fatalf("push %d: %v", i, err)
		}
	}
}

// ===========================================================================
// A `failed` nack is never skipped by later completed acks in the SAME call.
// ===========================================================================
func TestNackNotSkippedByLaterAckSameCall(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	queueName := generateQueueName("nack-clamp")
	if _, err := client.Queue(queueName).Config(queen.QueueConfig{LeaseTime: 30}).Create().Execute(ctx); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	pushRange(ctx, t, client, queueName, 1, 5)

	msgs := popRetryClient(ctx, t, client, queueName, "", 5)
	if len(msgs) != 5 {
		t.Fatalf("expected 5 messages, got %d", len(msgs))
	}

	// One batch ack call: #2 failed, everything else completed.
	rawAckBatch(t, msgs, []string{"completed", "failed", "completed", "completed", "completed"})

	// The cursor must clamp just before #2: the nack releases the lease and
	// #2..#5 redeliver (at-least-once duplicates, never a lost nack).
	again := popRetryClient(ctx, t, client, queueName, "", 10)
	got := redeliveredNs(again)
	want := []int{2, 3, 4, 5}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("nacked message skipped by later acks in the same call: redelivered %v, want %v", got, want)
	}
}

// ===========================================================================
// Same clamp for `retry` (budget-free) in the same call.
// ===========================================================================
func TestRetryNotSkippedByLaterAckSameCall(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	queueName := generateQueueName("retry-clamp")
	if _, err := client.Queue(queueName).Config(queen.QueueConfig{LeaseTime: 30}).Create().Execute(ctx); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	pushRange(ctx, t, client, queueName, 1, 3)

	msgs := popRetryClient(ctx, t, client, queueName, "", 3)
	if len(msgs) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(msgs))
	}

	rawAckBatch(t, msgs, []string{"completed", "retry", "completed"})

	again := popRetryClient(ctx, t, client, queueName, "", 10)
	got := redeliveredNs(again)
	want := []int{2, 3}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("'retry' skipped by a later ack in the same call: redelivered %v, want %v", got, want)
	}
	if n := dlqTotal(ctx, t, client, queueName, ""); n != 0 {
		t.Fatalf("'retry' clamp leaked into the DLQ (%d rows)", n)
	}
}

// ===========================================================================
// A nack below the committed cursor is rejected, not silently swallowed.
// ===========================================================================
func TestNackBelowCursorIsRejected(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	queueName := generateQueueName("late-nack")
	if _, err := client.Queue(queueName).Config(queen.QueueConfig{LeaseTime: 30}).Create().Execute(ctx); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	pushRange(ctx, t, client, queueName, 1, 3)

	msgs := popRetryClient(ctx, t, client, queueName, "", 3)
	if len(msgs) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(msgs))
	}

	// Ack the MIDDLE message: cursor commits past #1 and #2, lease stays live.
	if res, err := client.Ack(ctx, msgs[1], true, queen.AckOptions{}); err != nil || !res[0].Success {
		t.Fatalf("ack of middle message failed: %v %+v", err, res)
	}

	// Nack #1, now below the cursor: must be rejected as already committed.
	late := rawAckFull(t, msgs[0].TransactionID, msgs[0].PartitionID, "failed", msgs[0].LeaseID)
	if late.Success {
		t.Fatalf("nack below the committed cursor was silently accepted (must be rejected)")
	}
	if !strings.Contains(strings.ToLower(late.Error), "committed") {
		t.Fatalf("nack below cursor rejected with wrong error: %q", late.Error)
	}
}

// ===========================================================================
// A completed ack below the cursor succeeds but carries noop:true.
// ===========================================================================
func TestAckBelowCursorIsNoopFlagged(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	queueName := generateQueueName("late-ack")
	if _, err := client.Queue(queueName).Config(queen.QueueConfig{LeaseTime: 30}).Create().Execute(ctx); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	pushRange(ctx, t, client, queueName, 1, 3)

	msgs := popRetryClient(ctx, t, client, queueName, "", 3)
	if len(msgs) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(msgs))
	}

	if res, err := client.Ack(ctx, msgs[1], true, queen.AckOptions{}); err != nil || !res[0].Success {
		t.Fatalf("ack of middle message failed: %v %+v", err, res)
	}

	late := rawAckFull(t, msgs[0].TransactionID, msgs[0].PartitionID, "completed", msgs[0].LeaseID)
	if !late.Success {
		t.Fatalf("completed ack below cursor failed: %q", late.Error)
	}
	if !late.Noop {
		t.Fatalf("completed ack below cursor not flagged noop:true")
	}

	fresh := rawAckFull(t, msgs[2].TransactionID, msgs[2].PartitionID, "completed", msgs[2].LeaseID)
	if !fresh.Success || fresh.Noop {
		t.Fatalf("in-range ack wrongly flagged (success=%v noop=%v)", fresh.Success, fresh.Noop)
	}
}

// ===========================================================================
// Each() consumers abandon the rest of the popped batch after a nack.
// ===========================================================================
func TestEachStopsBatchAfterNack(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	queueName := generateQueueName("each-stop")
	if _, err := client.Queue(queueName).Config(queen.QueueConfig{LeaseTime: 30, RetryLimit: 1}).Create().Execute(ctx); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	pushRange(ctx, t, client, queueName, 1, 5)

	var mu sync.Mutex
	seen := map[int]int{} // n -> handler invocations

	err := client.Queue(queueName).
		Batch(5).
		Wait(false).
		IdleMillis(3000).
		Each().
		Consume(ctx, func(ctx context.Context, msg *queen.Message) error {
			n := int(msg.Data["n"].(float64))
			mu.Lock()
			seen[n]++
			mu.Unlock()
			if n == 2 {
				return fmt.Errorf("ack-commit-test poison")
			}
			return nil
		}).
		Execute(ctx)
	if err != nil && ctx.Err() == nil {
		t.Fatalf("consume: %v", err)
	}

	// #2 is poison (RetryLimit=1: delivered twice, then DLQ'd). Every other
	// message must be handled EXACTLY once: after the nack the client must
	// abandon the rest of the popped batch (dead lease) instead of processing
	// messages that are guaranteed to redeliver.
	mu.Lock()
	defer mu.Unlock()
	for _, n := range []int{1, 3, 4, 5} {
		if seen[n] > 1 {
			t.Fatalf("message n=%d processed %d times after a mid-batch nack (want exactly 1); seen=%v", n, seen[n], seen)
		}
		if seen[n] == 0 {
			t.Fatalf("message n=%d never processed; seen=%v", n, seen)
		}
	}
	if n := dlqTotal(ctx, t, client, queueName, ""); n != 1 {
		t.Fatalf("poison message not in DLQ (rows=%d)", n)
	}
}

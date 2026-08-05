// Message-semantics parity tests (RUSTFIX follow-up).
//
// These pin the v0.16.0 semantic contracts that the segment-engine port
// changed and then restored — the places where subtle regressions can
// silently reappear:
//
//   - Implicit ack: acking message N completes everything before it.
//   - `retry` ack status is budget-free; `dlq` force-dead-letters.
//   - Unconfigured queues dead-letter by default.
//   - Lease expiry never consumes retry budget.
//   - Lease-less acks succeed after expiry; stale leaseIds still fail.
//   - subscriptionMode='new' delivers partitions created after registration.
//   - Per-request ?leaseSeconds= override wins over the queue lease.
//
// The Go client's Ack API only expresses completed/failed, so the retry/dlq
// and lease-less cases go through raw HTTP (same pattern as auth_test.go).

package tests

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	queen "github.com/smartpricing/queen/clients/client-go"
)

// ---------------------------------------------------------------------------
// Raw HTTP helpers.
// ---------------------------------------------------------------------------

// rawAck posts /api/v1/ack with an arbitrary status ('retry', 'dlq', ...) and
// optional leaseId (empty string omits the field — the lease-less ack case).
// Returns (success, errText).
func rawAck(t *testing.T, txID, partitionID, status, group, leaseID string) (bool, string) {
	t.Helper()
	body := map[string]interface{}{
		"transactionId": txID,
		"partitionId":   partitionID,
		"status":        status,
	}
	if group != "" {
		body["consumerGroup"] = group
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
	var results []struct {
		Success bool   `json:"success"`
		Error   string `json:"error"`
	}
	if err := json.NewDecoder(res.Body).Decode(&results); err != nil {
		t.Fatalf("ack decode: %v", err)
	}
	if len(results) == 0 {
		t.Fatalf("ack returned no results")
	}
	return results[0].Success, results[0].Error
}

type rawMessage struct {
	TransactionID string `json:"transactionId"`
	PartitionID   string `json:"partitionId"`
	LeaseID       string `json:"leaseId"`
}

// rawPop GETs /api/v1/pop/queue/{queue} with arbitrary query params (used for
// the leaseSeconds override, which the typed builder doesn't expose).
func rawPop(t *testing.T, queue string, params map[string]string) []rawMessage {
	t.Helper()
	q := url.Values{"batch": {"1"}, "wait": {"false"}}
	for k, v := range params {
		q.Set(k, v)
	}
	res, err := http.Get(serverURL + "/api/v1/pop/queue/" + url.PathEscape(queue) + "?" + q.Encode())
	if err != nil {
		t.Fatalf("pop: %v", err)
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusNoContent {
		return nil
	}
	if res.StatusCode != http.StatusOK {
		t.Fatalf("pop status %d", res.StatusCode)
	}
	var body struct {
		Messages []rawMessage `json:"messages"`
		LeaseID  string       `json:"leaseId"`
	}
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		t.Fatalf("pop decode: %v", err)
	}
	// Single-partition pops carry the lease at the envelope level; make sure
	// each message knows it either way.
	for i := range body.Messages {
		if body.Messages[i].LeaseID == "" {
			body.Messages[i].LeaseID = body.LeaseID
		}
	}
	return body.Messages
}

// popRetryClient pops through the typed client, retrying briefly to ride out
// push->visibility latency (broker-side fusion hold).
//
// The optional mode is the subscriptionMode sent with the pop. It only bites on
// a group's FIRST contact: new groups are seeded at the partition tail by
// default, so a test that pushes BEFORE the group exists must pass "all" to get
// the backlog.
func popRetryClient(ctx context.Context, t *testing.T, client *queen.Queen, queueName, group string, batch int, mode ...string) []*queen.Message {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		qb := client.Queue(queueName).Batch(batch).Wait(false)
		if group != "" {
			qb = qb.Group(group)
		}
		if len(mode) > 0 && mode[0] != "" {
			qb = qb.SubscriptionMode(mode[0])
		}
		msgs, err := qb.Pop(ctx)
		if err != nil {
			t.Fatalf("pop: %v", err)
		}
		if len(msgs) > 0 {
			return msgs
		}
		time.Sleep(150 * time.Millisecond)
	}
	return nil
}

func dlqTotal(ctx context.Context, t *testing.T, client *queen.Queen, queueName, group string) int {
	t.Helper()
	res, err := client.Queue(queueName).DLQ(group).Limit(50).Get(ctx)
	if err != nil {
		t.Fatalf("dlq query: %v", err)
	}
	return len(res.Messages)
}

// ===========================================================================
// Implicit ack: acking the LAST message of a batch completes the whole batch
// (v0.16.0 contract). A contiguous-prefix regression redelivers the gap after
// the lease expires.
// ===========================================================================
func TestImplicitAckCompletesBatch(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	queueName := generateQueueName("implicit-ack")
	if _, err := client.Queue(queueName).Config(queen.QueueConfig{LeaseTime: 1}).Create().Execute(ctx); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	for i := 1; i <= 5; i++ {
		if _, err := client.Queue(queueName).Partition("Default").
			Push(map[string]interface{}{"n": i}).Execute(ctx); err != nil {
			t.Fatalf("push %d: %v", i, err)
		}
	}

	msgs := popRetryClient(ctx, t, client, queueName, "", 5)
	if len(msgs) != 5 {
		t.Fatalf("expected 5 messages, got %d", len(msgs))
	}

	// Ack ONLY the last one.
	res, err := client.Ack(ctx, msgs[4], true, queen.AckOptions{})
	if err != nil || len(res) == 0 || !res[0].Success {
		t.Fatalf("ack of last message failed: %v %+v", err, res)
	}

	// Wait past lease expiry; nothing may redeliver.
	time.Sleep(2500 * time.Millisecond)
	again, err := client.Queue(queueName).Batch(10).Wait(false).Pop(ctx)
	if err != nil {
		t.Fatalf("re-pop: %v", err)
	}
	if len(again) != 0 {
		t.Fatalf("implicit-ack regression: %d messages redelivered after acking only the last one", len(again))
	}
}

// ===========================================================================
// `retry` is budget-free, and an UNCONFIGURED queue dead-letters by default.
// ===========================================================================
func TestRetryStatusBudgetFreeAndDlqByDefault(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// Push-only queue: no Configure — server defaults (retryLimit=3, DLQ on).
	queueName := generateQueueName("retry-free")
	group := queueName + "-cg"
	if _, err := client.Queue(queueName).Partition("Default").
		Push(map[string]interface{}{"poison": true}).Execute(ctx); err != nil {
		t.Fatalf("push: %v", err)
	}

	// 6 'retry' cycles — twice the default budget. Every cycle must redeliver.
	for i := 0; i < 6; i++ {
		msgs := popRetryClient(ctx, t, client, queueName, group, 1, queen.SubscriptionModeAll)
		if len(msgs) != 1 {
			t.Fatalf("cycle %d: message not redelivered after 'retry' ack (budget charged?)", i)
		}
		ok, errText := rawAck(t, msgs[0].TransactionID, msgs[0].PartitionID, "retry", group, msgs[0].LeaseID)
		if !ok {
			t.Fatalf("cycle %d: retry ack rejected: %s", i, errText)
		}
	}
	if n := dlqTotal(ctx, t, client, queueName, group); n != 0 {
		t.Fatalf("'retry' acks leaked %d entries into the DLQ", n)
	}

	// Exhaust the real budget with failed acks -> must dead-letter even though
	// the queue was never configured (DLQ-by-default contract).
	for i := 0; i < 5; i++ {
		msgs := popRetryClient(ctx, t, client, queueName, group, 1, queen.SubscriptionModeAll)
		if len(msgs) == 0 {
			break
		}
		if _, err := client.Ack(ctx, msgs[0], false, queen.AckOptions{ConsumerGroup: group, Error: "sem-test failure"}); err != nil {
			t.Fatalf("failed ack: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if n := dlqTotal(ctx, t, client, queueName, group); n != 1 {
		t.Fatalf("message never dead-lettered on an unconfigured queue (got %d DLQ entries) — DLQ-by-default broken?", n)
	}
}

// ===========================================================================
// Forced `dlq` status bypasses the retry budget entirely.
// ===========================================================================
func TestDlqStatusBypassesRetries(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	queueName := generateQueueName("force-dlq")
	group := queueName + "-cg"
	if _, err := client.Queue(queueName).Partition("Default").
		Push(map[string]interface{}{"poison": true}).Execute(ctx); err != nil {
		t.Fatalf("push: %v", err)
	}

	msgs := popRetryClient(ctx, t, client, queueName, group, 1, queen.SubscriptionModeAll)
	if len(msgs) != 1 {
		t.Fatalf("message not delivered")
	}
	ok, errText := rawAck(t, msgs[0].TransactionID, msgs[0].PartitionID, "dlq", group, msgs[0].LeaseID)
	if !ok {
		t.Fatalf("dlq ack rejected: %s", errText)
	}
	time.Sleep(300 * time.Millisecond)

	if n := dlqTotal(ctx, t, client, queueName, group); n != 1 {
		t.Fatalf("forced 'dlq' ack did not dead-letter immediately (%d entries)", n)
	}
	again, err := client.Queue(queueName).Group(group).Batch(5).Wait(false).Pop(ctx)
	if err != nil {
		t.Fatalf("re-pop: %v", err)
	}
	if len(again) != 0 {
		t.Fatalf("message still deliverable after forced DLQ")
	}
}

// ===========================================================================
// Lease expiry does NOT consume retry budget.
// ===========================================================================
func TestLeaseExpiryDoesNotChargeRetryBudget(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	queueName := generateQueueName("expiry-budget")
	group := queueName + "-cg"
	if _, err := client.Queue(queueName).Config(queen.QueueConfig{LeaseTime: 1, RetryLimit: 2}).Create().Execute(ctx); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	if _, err := client.Queue(queueName).Partition("Default").
		Push(map[string]interface{}{"poison": true}).Execute(ctx); err != nil {
		t.Fatalf("push: %v", err)
	}

	// 4 expiry cycles (double the budget): pop, never ack, let the lease lapse.
	for i := 0; i < 4; i++ {
		msgs := popRetryClient(ctx, t, client, queueName, group, 1, queen.SubscriptionModeAll)
		if len(msgs) != 1 {
			t.Fatalf("cycle %d: message not redelivered after lease expiry (budget charged?)", i)
		}
		time.Sleep(1600 * time.Millisecond)
	}
	if n := dlqTotal(ctx, t, client, queueName, group); n != 0 {
		t.Fatalf("lease expiries dead-lettered the message (%d entries)", n)
	}

	// Explicit failures must still have the full budget available.
	dlqSeen := false
	for i := 0; i < 4; i++ {
		msgs := popRetryClient(ctx, t, client, queueName, group, 1, queen.SubscriptionModeAll)
		if len(msgs) == 0 {
			break
		}
		if _, err := client.Ack(ctx, msgs[0], false, queen.AckOptions{ConsumerGroup: group, Error: "sem-test failure"}); err != nil {
			t.Fatalf("failed ack: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	dlqSeen = dlqTotal(ctx, t, client, queueName, group) == 1
	if !dlqSeen {
		t.Fatalf("message never reached DLQ on explicit failures after the expiry cycles")
	}
}

// ===========================================================================
// Lease-less ack succeeds even after lease expiry; a stale leaseId still fails.
// ===========================================================================
func TestLeaselessAckAfterExpiry(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	queueName := generateQueueName("leaseless-ack")
	group := queueName + "-cg"
	if _, err := client.Queue(queueName).Config(queen.QueueConfig{LeaseTime: 1}).Create().Execute(ctx); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	if _, err := client.Queue(queueName).Partition("Default").
		Push(map[string]interface{}{"n": 1}).Execute(ctx); err != nil {
		t.Fatalf("push: %v", err)
	}

	msgs := popRetryClient(ctx, t, client, queueName, group, 1, queen.SubscriptionModeAll)
	if len(msgs) != 1 {
		t.Fatalf("message not delivered")
	}
	staleLease := msgs[0].LeaseID
	time.Sleep(2 * time.Second) // lease expired

	// (a) Lease-less ack (no leaseId field) must succeed and move the cursor.
	ok, errText := rawAck(t, msgs[0].TransactionID, msgs[0].PartitionID, "completed", group, "")
	if !ok {
		t.Fatalf("lease-less ack after expiry rejected: %s", errText)
	}
	again, err := client.Queue(queueName).Group(group).Batch(5).Wait(false).Pop(ctx)
	if err != nil {
		t.Fatalf("re-pop: %v", err)
	}
	if len(again) != 0 {
		t.Fatalf("lease-less ack did not advance the cursor (message redelivered)")
	}

	// (b) A stale leaseId must still be rejected.
	if _, err := client.Queue(queueName).Partition("Default").
		Push(map[string]interface{}{"n": 2}).Execute(ctx); err != nil {
		t.Fatalf("push 2: %v", err)
	}
	msgs2 := popRetryClient(ctx, t, client, queueName, group, 1, queen.SubscriptionModeAll)
	if len(msgs2) != 1 {
		t.Fatalf("second message not delivered")
	}
	lease := msgs2[0].LeaseID
	if lease == "" {
		lease = staleLease
	}
	time.Sleep(2 * time.Second) // lease expired
	ok, _ = rawAck(t, msgs2[0].TransactionID, msgs2[0].PartitionID, "completed", group, lease)
	if ok {
		t.Fatalf("ack with an expired leaseId was accepted (must fail)")
	}
}

// ===========================================================================
// subscriptionMode='new': a partition created AFTER registration must deliver
// its post-subscription messages on the group's first contact with it.
// ===========================================================================
func TestSubscriptionNewLateCreatedPartition(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	queueName := generateQueueName("late-part")
	group := queueName + "-cg"
	if _, err := client.Queue(queueName).Create().Execute(ctx); err != nil {
		t.Fatalf("create queue: %v", err)
	}

	// Backlog before subscription — must stay skipped.
	if _, err := client.Queue(queueName).Partition("p-early").
		Push(map[string]interface{}{"phase": "backlog"}).Execute(ctx); err != nil {
		t.Fatalf("push backlog: %v", err)
	}
	time.Sleep(300 * time.Millisecond)

	// Register with mode 'new' (first pop).
	first, err := client.Queue(queueName).Group(group).SubscriptionMode("new").
		Batch(5).Wait(false).Pop(ctx)
	if err != nil {
		t.Fatalf("register pop: %v", err)
	}
	if len(first) != 0 {
		t.Fatalf("mode 'new' delivered pre-subscription backlog (%d msgs)", len(first))
	}

	// New partition appears after registration and receives a message.
	if _, err := client.Queue(queueName).Partition("p-late").
		Push(map[string]interface{}{"phase": "late"}).Execute(ctx); err != nil {
		t.Fatalf("push late: %v", err)
	}
	time.Sleep(300 * time.Millisecond)

	got := popRetryClient(ctx, t, client, queueName, group, 5)
	if len(got) == 0 {
		t.Fatalf("late-created partition's message was skipped — durable-subscription seeding broken")
	}
	for _, m := range got {
		if m.Partition == "p-early" {
			t.Fatalf("pre-subscription backlog delivered to a mode-new group")
		}
	}
}

// ===========================================================================
// Per-request ?leaseSeconds= override wins over the queue's configured lease.
// ===========================================================================
func TestPopLeaseSecondsOverride(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	queueName := generateQueueName("lease-override")
	group := queueName + "-cg"
	if _, err := client.Queue(queueName).Config(queen.QueueConfig{LeaseTime: 60}).Create().Execute(ctx); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	if _, err := client.Queue(queueName).Partition("Default").
		Push(map[string]interface{}{"n": 1}).Execute(ctx); err != nil {
		t.Fatalf("push: %v", err)
	}
	time.Sleep(300 * time.Millisecond)

	// Claim with a 1-second override against the 60s queue lease.
	var got []rawMessage
	for i := 0; i < 20 && len(got) == 0; i++ {
		// First contact of this group, and the message was pushed before it
		// existed: ask for the backlog instead of the tail-seeded default.
		got = rawPop(t, queueName, map[string]string{
			"consumerGroup": group, "leaseSeconds": "1", "subscriptionMode": "all",
		})
		if len(got) == 0 {
			time.Sleep(150 * time.Millisecond)
		}
	}
	if len(got) != 1 {
		t.Fatalf("message not delivered on override pop")
	}

	// With the override the lease lapses in ~1s; with the 60s queue lease this
	// redelivery would never happen inside the test window.
	time.Sleep(2500 * time.Millisecond)
	again := rawPop(t, queueName, map[string]string{"consumerGroup": group})
	if len(again) != 1 {
		t.Fatalf("message not redelivered after 2.5s — ?leaseSeconds=1 override ignored")
	}
}

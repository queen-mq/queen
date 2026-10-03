// A transaction that acknowledges messages of two leases, against a live broker.
//
// A Queen 2 broker fences an ack with the lease the operation carries, and lends
// requiredLeases to an ack without one only when the bundle names a single
// lease. So an ack that does not carry its own lease is applied in a bundle of
// two leases even after its lease expired and another consumer took the message.
// Parity with client-php tests/Integration/TransactionLeaseFencingIntegrationTest.php.

package tests

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	queen "github.com/smartpricing/queen/clients/client-go/v2"
)

func TestTransactionAckUnderAnExpiredLeaseRefusesTheWholeBundle(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	queueA, queueB := leaseTestQueues(ctx, t, client, "tx-fence")

	a := popLeased(ctx, t, queueA, 1, 10*time.Second)
	b := popLeased(ctx, t, queueB, 60, 10*time.Second)
	if a == nil || b == nil {
		t.Fatalf("nothing to pop: a=%v b=%v", a, b)
	}
	if a.LeaseID == b.LeaseID {
		t.Fatalf("the two pops share lease %s; the test needs two", a.LeaseID)
	}

	// Lease A expires, and another consumer takes the message.
	taken := popLeased(ctx, t, queueA, 60, 15*time.Second)
	if taken == nil {
		t.Fatal("the message of the expired lease was not delivered again")
	}
	if taken.TransactionID != a.TransactionID {
		t.Fatalf("re-delivered %s, want %s", taken.TransactionID, a.TransactionID)
	}
	if taken.LeaseID == a.LeaseID {
		t.Fatalf("re-delivered under the expired lease %s, want a new one", a.LeaseID)
	}

	resp, err := client.Transaction().
		Ack(a, queen.AckStatusCompleted, queen.AckOptions{}).
		Ack(b, queen.AckStatusCompleted, queen.AckOptions{}).
		Commit(ctx)
	if err == nil {
		t.Fatalf("an ack under an expired lease must refuse the whole bundle, but it committed (success=%v, transactionId=%s)", resp.Success, resp.TransactionID)
	}
	if resp != nil && resp.Success {
		t.Fatalf("a refused bundle must not read as success (transactionId=%s)", resp.TransactionID)
	}
	if !strings.Contains(err.Error(), "rolled back") {
		t.Errorf("error = %q, want the broker's rollback", err.Error())
	}
	t.Logf("bundle refused: %v", err)

	// Nothing of the bundle happened: each holder still settles its own message.
	if _, err := client.Transaction().Ack(taken, queen.AckStatusCompleted, queen.AckOptions{}).Commit(ctx); err != nil {
		t.Fatalf("the new holder of A could not ack it: %v", err)
	}
	if _, err := client.Transaction().Ack(b, queen.AckStatusCompleted, queen.AckOptions{}).Commit(ctx); err != nil {
		t.Fatalf("the holder of B could not ack it: %v", err)
	}
	assertQueueEmpty(ctx, t, client, queueA)
	assertQueueEmpty(ctx, t, client, queueB)
}

func TestTransactionBundleOfTwoLiveLeasesCompletesBoth(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	queueA, queueB := leaseTestQueues(ctx, t, client, "tx-live")

	a := popLeased(ctx, t, queueA, 60, 10*time.Second)
	b := popLeased(ctx, t, queueB, 60, 10*time.Second)
	if a == nil || b == nil {
		t.Fatalf("nothing to pop: a=%v b=%v", a, b)
	}

	resp, err := client.Transaction().
		Ack(a, queen.AckStatusCompleted, queen.AckOptions{}).
		Ack(b, queen.AckStatusCompleted, queen.AckOptions{}).
		Commit(ctx)
	if err != nil {
		t.Fatalf("a bundle of two live leases must commit: %v", err)
	}
	if !resp.Success {
		t.Fatalf("success=false: reason=%q error=%q", resp.Reason, resp.Error)
	}
	assertQueueEmpty(ctx, t, client, queueA)
	assertQueueEmpty(ctx, t, client, queueB)
}

// leaseTestQueues creates two fresh queues holding one message each, and
// deletes them when the test ends.
func leaseTestQueues(ctx context.Context, t *testing.T, client *queen.Queen, prefix string) (string, string) {
	t.Helper()
	queues := []string{generateQueueName(prefix + "-a"), generateQueueName(prefix + "-b")}
	for _, q := range queues {
		if _, err := client.Queue(q).Create().Execute(ctx); err != nil {
			t.Fatalf("create %s: %v", q, err)
		}
		t.Cleanup(func() { _, _ = client.Queue(q).Delete().Execute(context.Background()) })
		if _, err := client.Queue(q).Push(map[string]interface{}{"queue": q}).Execute(ctx); err != nil {
			t.Fatalf("push %s: %v", q, err)
		}
	}
	return queues[0], queues[1]
}

// popLeased pops one message of queue under a lease of leaseSeconds, retrying
// until one is there or timeout passes (then nil). The typed Pop has no per-pop
// lease and this test needs two at once, one short enough to expire and one
// that outlives it, so it asks the broker directly, as rawAckBatch does for
// per-item statuses.
func popLeased(ctx context.Context, t *testing.T, queue string, leaseSeconds int, timeout time.Duration) *queen.Message {
	t.Helper()
	params := url.Values{}
	params.Set("batch", "1")
	params.Set("wait", "false")
	params.Set("subscriptionMode", "all")
	params.Set("leaseSeconds", strconv.Itoa(leaseSeconds))
	endpoint := serverURL + "/api/v1/pop/queue/" + url.PathEscape(queue) + "?" + params.Encode()

	deadline := time.Now().Add(timeout)
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			t.Fatalf("pop %s: %v", queue, err)
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("pop %s: %v", queue, err)
		}
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK && res.StatusCode != http.StatusNoContent {
			t.Fatalf("pop %s: HTTP %d: %s", queue, res.StatusCode, body)
		}

		var out struct {
			Messages []queen.Message `json:"messages"`
		}
		if len(body) > 0 {
			if err := json.Unmarshal(body, &out); err != nil {
				t.Fatalf("pop %s: %v: %s", queue, err, body)
			}
		}
		if len(out.Messages) > 0 {
			if len(out.Messages) != 1 {
				t.Fatalf("pop %s: want 1 message, got %d", queue, len(out.Messages))
			}
			msg := out.Messages[0]
			if msg.LeaseID == "" {
				t.Fatalf("pop %s: message delivered without a lease: %s", queue, body)
			}
			return &msg
		}
		if time.Now().After(deadline) {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// assertQueueEmpty pops without waiting and fails on anything that is still
// there to deliver.
func assertQueueEmpty(ctx context.Context, t *testing.T, client *queen.Queen, queue string) {
	t.Helper()
	msgs, err := client.Queue(queue).SubscriptionMode("all").Wait(false).Pop(ctx)
	if err != nil {
		t.Fatalf("pop %s: %v", queue, err)
	}
	if len(msgs) != 0 {
		t.Errorf("%s still delivers %d message(s); want it empty", queue, len(msgs))
	}
}

// Ack-window honesty (2026-07-30, parity with client-js test-v2/ackwindow.js).
//
// log_ack_by_hash_v1 resolves txn hashes through the queen.log_txns sidecar.
// A hash that cannot be resolved (purged row, or a transactionId that never
// existed) is correctly NOT acked — but the broker used to report those items
// as success=true (they land in neither noopHashes nor staleHashes), so the
// client believed the ack committed while the cursor never moved. Worst
// variant: a `failed` nack in that state was swallowed whole — no retry
// charge, no DLQ — with an ok answer.
//
// This test pins the wire contract: acks/nacks of a never-pushed
// transactionId must come back success=false with an explicit "unresolvable"
// error, and the real leased batch must remain ackable afterwards.

package tests

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestAckUnknownTxnMustFail(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	queueName := fmt.Sprintf("test-ackwindow-unknown-%d", time.Now().UnixMilli())
	if _, err := testClient.Queue(queueName).Create().Execute(ctx); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	for i := 1; i <= 3; i++ {
		if _, err := testClient.Queue(queueName).Partition("Default").
			Push(map[string]interface{}{"n": i}).Execute(ctx); err != nil {
			t.Fatalf("push: %v", err)
		}
	}

	// Pop with a lease so the ack carries a live leaseId.
	var msgs []rawMessage
	for i := 0; i < 40 && len(msgs) < 3; i++ {
		msgs = rawPop(t, queueName, map[string]string{"batch": "3"})
		if len(msgs) >= 3 {
			break
		}
		time.Sleep(150 * time.Millisecond)
	}
	if len(msgs) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(msgs))
	}

	ghost := queueName + "-ghost-never-pushed"

	// Completed ack of a nonexistent txn.
	okAck, errAck := rawAck(t, ghost, msgs[0].PartitionID, "completed", "", msgs[0].LeaseID)
	if okAck {
		t.Fatalf("BUG: completed ack of a never-pushed txn reported success=true")
	}
	if !strings.Contains(strings.ToLower(errAck), "unresolv") {
		t.Fatalf("unknown-txn ack rejected with wrong error: %q", errAck)
	}

	// Failed nack of a nonexistent txn (pre-fix: silently swallowed as ok).
	okNack, errNack := rawAck(t, ghost, msgs[0].PartitionID, "failed", "", msgs[0].LeaseID)
	if okNack {
		t.Fatalf("BUG: failed nack of a never-pushed txn reported success=true")
	}
	if !strings.Contains(strings.ToLower(errNack), "unresolv") {
		t.Fatalf("unknown-txn nack rejected with wrong error: %q", errNack)
	}

	// The rejected calls must not have burned the lease: the real batch still
	// acks, message by message (fresh hashes resolve fine).
	for i, m := range msgs {
		okReal, errReal := rawAck(t, m.TransactionID, m.PartitionID, "completed", "", m.LeaseID)
		if !okReal {
			t.Fatalf("real ack #%d rejected after ghost-ack rejections: %q", i, errReal)
		}
	}
}

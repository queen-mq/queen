package tests

import (
	"strings"
	"testing"
	"time"
)

// TestDLQ_* mirror clients/client-js/test-v2/dlq.js + test_dlq.py.

// TestDLQ_FailingMessageReachesDLQ mirrors dlq.js#testDLQ end-to-end through
// the CLI:
//
//  1. configure queue with retryLimit=1
//  2. push a message
//  3. pop + ack-failed enough times to exhaust retries
//  4. list the DLQ and verify the message is there with the error message
func TestDLQ_FailingMessageReachesDLQ(t *testing.T) {
	q := uniqueQueue(t, "dlq-fail")
	runOK(t, "queue", "configure", q, "--retry-limit", "1", "--lease-time", "5")

	pushOne(t, q, "", map[string]any{"message": "Test DLQ message"})

	// Pop + nack up to retryLimit+1 times. Each nack increments the retry
	// counter; once it crosses the limit the broker moves the message to
	// the DLQ.
	// --from-mode all: the CG is created after the push, and the broker's
	// default mode ('new') would seed it past the message we want to fail.
	for i := 0; i < 3; i++ {
		got := popN(t, q, 1, "--cg", "ct-dlq", "--from-mode", "all", "--timeout", "5s")
		if len(got) == 0 {
			break
		}
		m := got[0]
		_, _, code := run("ack", m.TransactionID,
			"--partition-id", m.PartitionID,
			"--lease-id", m.LeaseID,
			"--cg", "ct-dlq",
			"--failed",
			"--error", "Test error - triggering DLQ",
		)
		if code != 0 {
			t.Fatalf("ack --failed exit %d", code)
		}
	}

	// Allow the DLQ stored procedure to commit.
	var rows []map[string]any
	retry(t, 5*time.Second, func() error {
		rows = listDLQRows(t, q)
		if len(rows) == 0 {
			return fmtErr("DLQ still empty")
		}
		return nil
	})
	first := rows[0]
	if got, _ := first["queueName"].(string); got != q && first["queue"] != q {
		t.Errorf("DLQ row queueName = %v, want %s", first["queueName"], q)
	}
	errMsg, _ := first["errorMessage"].(string)
	if !strings.Contains(errMsg, "Test error") {
		t.Errorf("DLQ errorMessage = %q, want substring 'Test error'", errMsg)
	}
}

// TestDLQ_ListFiltersByQueue verifies that `queenctl dlq list --queue X`
// only returns DLQ rows for X, not for unrelated queues.
func TestDLQ_ListFiltersByQueue(t *testing.T) {
	target := uniqueQueue(t, "dlq-target")
	other := uniqueQueue(t, "dlq-other")
	runOK(t, "queue", "configure", target, "--retry-limit", "1", "--lease-time", "5")
	runOK(t, "queue", "configure", other, "--retry-limit", "1", "--lease-time", "5")

	for _, qn := range []string{target, other} {
		pushOne(t, qn, "", map[string]any{"q": qn})
		for i := 0; i < 3; i++ {
			got := popN(t, qn, 1, "--cg", "ct-dlq-multi", "--from-mode", "all", "--timeout", "5s")
			if len(got) == 0 {
				break
			}
			m := got[0]
			run("ack", m.TransactionID,
				"--partition-id", m.PartitionID,
				"--lease-id", m.LeaseID,
				"--cg", "ct-dlq-multi", "--failed",
			)
		}
	}

	var targetRows, otherRows []map[string]any
	retry(t, 5*time.Second, func() error {
		targetRows = listDLQRows(t, target)
		otherRows = listDLQRows(t, other)
		if len(targetRows) == 0 || len(otherRows) == 0 {
			return fmtErr("DLQ rows not yet visible")
		}
		return nil
	})
	if len(targetRows) == 0 {
		t.Errorf("expected target DLQ rows; got 0")
	}
	for _, r := range targetRows {
		if v, _ := r["queueName"].(string); v != "" && v != target {
			t.Errorf("filtered list returned row for %q, want only %q", v, target)
		}
	}
}

// TestDLQ_DrainDryRun verifies --dry-run reports the matched rows but
// doesn't change DLQ contents.
func TestDLQ_DrainDryRun(t *testing.T) {
	q := uniqueQueue(t, "dlq-drain-dry")
	runOK(t, "queue", "configure", q, "--retry-limit", "1", "--lease-time", "5")
	pushOne(t, q, "", map[string]any{"v": 1})
	for i := 0; i < 3; i++ {
		got := popN(t, q, 1, "--cg", "ct-drain-dry", "--from-mode", "all", "--timeout", "5s")
		if len(got) == 0 {
			break
		}
		m := got[0]
		run("ack", m.TransactionID,
			"--partition-id", m.PartitionID, "--lease-id", m.LeaseID,
			"--cg", "ct-drain-dry", "--failed",
		)
	}
	retry(t, 5*time.Second, func() error {
		if rows := listDLQRows(t, q); len(rows) > 0 {
			return nil
		}
		return fmtErr("DLQ empty")
	})
	before := len(listDLQRows(t, q))
	out := runOK(t, "dlq", "drain", "--queue", q, "--dry-run")
	if !strings.Contains(out, "[dry-run]") {
		t.Errorf("dry-run output should include [dry-run] markers: %s", out)
	}
	after := len(listDLQRows(t, q))
	if after != before {
		t.Errorf("dry-run changed DLQ depth: before=%d after=%d", before, after)
	}
}

// TestDLQ_ManualRequeueViaPushAndDelete proves the CLIENT-SIDE requeue
// workflow:
//
//  1. capture the DLQ row's payload
//  2. push it back to the live queue
//  3. delete the DLQ row
//
// This is not the only way to replay any more -- the broker has a
// server-side route and `dlq retry` wraps it (see the test below). The
// three-call version is still worth pinning: it is what you fall back to
// when the snapshot needs editing before it goes back on the queue, and it
// is the only shape available to a client too old for the route.
func TestDLQ_ManualRequeueViaPushAndDelete(t *testing.T) {
	q := uniqueQueue(t, "dlq-manual-requeue")
	runOK(t, "queue", "configure", q, "--retry-limit", "1", "--lease-time", "5")
	pushOne(t, q, "", map[string]any{"r": 1})
	for i := 0; i < 3; i++ {
		got := popN(t, q, 1, "--cg", "ct-mreq", "--from-mode", "all", "--timeout", "5s")
		if len(got) == 0 {
			break
		}
		m := got[0]
		run("ack", m.TransactionID,
			"--partition-id", m.PartitionID, "--lease-id", m.LeaseID,
			"--cg", "ct-mreq", "--failed",
		)
	}
	var rows []map[string]any
	retry(t, 5*time.Second, func() error {
		rows = listDLQRows(t, q)
		if len(rows) == 0 {
			return fmtErr("DLQ empty")
		}
		return nil
	})
	row := rows[0]
	pid, _ := row["partitionId"].(string)
	tx, _ := row["transactionId"].(string)
	data := row["data"]
	if pid == "" || tx == "" {
		t.Fatalf("DLQ row missing ids: %v", row)
	}

	// 1. push the payload back to the queue (auto-create has happened).
	out := pushOne(t, q, "", data)
	if !strings.Contains(out, "queued=1") {
		t.Fatalf("re-push did not queue a row: %s", out)
	}
	// 2. delete the DLQ row.
	runOK(t, "messages", "delete", pid, tx, "--yes")
	// 3. the requeued message is back in the live queue.
	got := popN(t, q, 1, "--cg", "ct-mreq-after", "--from-mode", "all", "--auto-ack", "--timeout", "5s")
	if len(got) != 1 {
		t.Errorf("re-pushed message did not surface (popped %d)", len(got))
	}
}

// listDLQRows wraps `queenctl dlq list --queue` and returns the row slice.
// TestDLQ_ServerSideRetryReplaysAndClearsRow drives `dlq retry`, the wrapper
// over POST /api/v1/messages/:p/:tx/retry, and pins the property that makes
// the command non-idempotent: the replayed message comes back with a
// DIFFERENT transaction id. The broker mints a fresh one deliberately (the
// original would be swallowed by the dedup window), which is exactly why two
// runs produce two copies and why the command demands --yes. The original
// consumer group is used below because a brand-new `all` group must still see
// the immutable original segment before it sees the replay. If this assertion
// ever flips to "same id", the idempotency warnings on Admin.RetryMessage and
// in `dlq retry --help` need revisiting.
func TestDLQ_ServerSideRetryReplaysAndClearsRow(t *testing.T) {
	q := uniqueQueue(t, "dlq-retry")
	runOK(t, "queue", "configure", q, "--retry-limit", "1", "--lease-time", "5")
	pushOne(t, q, "", map[string]any{"replay": "me"})

	for i := 0; i < 3; i++ {
		got := popN(t, q, 1, "--cg", "ct-retry", "--from-mode", "all", "--timeout", "5s")
		if len(got) == 0 {
			break
		}
		m := got[0]
		run("ack", m.TransactionID,
			"--partition-id", m.PartitionID, "--lease-id", m.LeaseID,
			"--cg", "ct-retry", "--failed", "--error", "forced into the DLQ",
		)
	}

	var rows []map[string]any
	retry(t, 5*time.Second, func() error {
		rows = listDLQRows(t, q)
		if len(rows) == 0 {
			return fmtErr("DLQ empty")
		}
		return nil
	})
	pid, _ := rows[0]["partitionId"].(string)
	tx, _ := rows[0]["transactionId"].(string)
	if pid == "" || tx == "" {
		t.Fatalf("DLQ row missing ids: %v", rows[0])
	}

	// The guard fires before anything is sent.
	if _, _, code := run("dlq", "retry", pid, tx); code != 1 {
		t.Errorf("dlq retry without --yes -> exit %d, want 1", code)
	}
	if rows := listDLQRows(t, q); len(rows) != 1 {
		t.Fatalf("the refused retry changed the DLQ: %d rows", len(rows))
	}

	retryOutput := runOK(t, "dlq", "retry", pid, tx, "--yes")
	var retryResult struct {
		ReplayedAs struct {
			TransactionID string `json:"transaction_id"`
		} `json:"replayedAs"`
	}
	if err := jsonDecode(retryOutput, &retryResult); err != nil {
		t.Fatalf("decode dlq retry response: %v\nbody: %s", err, retryOutput)
	}
	if retryResult.ReplayedAs.TransactionID == "" {
		t.Fatalf("dlq retry response omitted replayed transaction id: %s", retryOutput)
	}
	if retryResult.ReplayedAs.TransactionID == tx {
		t.Fatalf("replay response reused transaction id %s: the dedup window can drop it", tx)
	}

	// The DLQ row is gone...
	retry(t, 5*time.Second, func() error {
		if n := len(listDLQRows(t, q)); n != 0 {
			return fmtErr("DLQ still has %d rows", n)
		}
		return nil
	})

	// ...and the payload is back on the queue under a NEW transaction id.
	got := popN(t, q, 1, "--cg", "ct-retry", "--from-mode", "all", "--timeout", "10s")
	if len(got) != 1 {
		t.Fatalf("replayed message not poppable: got %d", len(got))
	}
	if got[0].TransactionID != retryResult.ReplayedAs.TransactionID {
		t.Errorf("popped replay transaction id = %s, response announced %s",
			got[0].TransactionID, retryResult.ReplayedAs.TransactionID)
	}
	if got[0].Data["replay"] != "me" {
		t.Errorf("replayed payload = %v, want {replay: me}", got[0].Data)
	}
}

func listDLQRows(t *testing.T, queue string) []map[string]any {
	t.Helper()
	out := runOK(t, "dlq", "list", "--queue", queue, "--limit", "100", "-o", "json")
	if strings.TrimSpace(out) == "null" || strings.TrimSpace(out) == "" {
		return nil
	}
	var rows []map[string]any
	if err := jsonDecode(out, &rows); err != nil {
		t.Fatalf("decode dlq list: %v\nbody: %s", err, out)
	}
	return rows
}

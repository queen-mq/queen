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
	for i := 0; i < 3; i++ {
		got := popN(t, q, 1, "--cg", "ct-dlq", "--timeout", "5s")
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
			got := popN(t, qn, 1, "--cg", "ct-dlq-multi", "--timeout", "5s")
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
		got := popN(t, q, 1, "--cg", "ct-drain-dry", "--timeout", "5s")
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

// TestDLQ_ManualRequeueViaPushAndDelete proves the documented requeue
// workflow when the broker has no server-side retry endpoint:
//
//  1. capture the DLQ row's payload
//  2. push it back to the live queue
//  3. delete the DLQ row
//
// The JS / Py SDKs offer a built-in dlq().requeue() helper; here we drive
// the same outcome through three pure-CLI calls.
func TestDLQ_ManualRequeueViaPushAndDelete(t *testing.T) {
	q := uniqueQueue(t, "dlq-manual-requeue")
	runOK(t, "queue", "configure", q, "--retry-limit", "1", "--lease-time", "5")
	pushOne(t, q, "", map[string]any{"r": 1})
	for i := 0; i < 3; i++ {
		got := popN(t, q, 1, "--cg", "ct-mreq", "--timeout", "5s")
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
	got := popN(t, q, 1, "--cg", "ct-mreq-after", "--auto-ack", "--timeout", "5s")
	if len(got) != 1 {
		t.Errorf("re-pushed message did not surface (popped %d)", len(got))
	}
}

// listDLQRows wraps `queenctl dlq list --queue` and returns the row slice.
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

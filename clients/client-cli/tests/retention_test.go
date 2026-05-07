package tests

import (
	"os"
	"strconv"
	"testing"
	"time"
)

// TestRetention_* mirror clients/client-js/test-v2/retention.js. The broker
// has a periodic retention sweep controlled by RETENTION_INTERVAL (default
// 60s). The original tests assume the broker was started with
// RETENTION_INTERVAL=2000 (2s) so the sweep fires within the test window.
//
// We honor that constraint via QUEEN_RETENTION_INTERVAL_MS: if it's not
// set (default broker config), the test is skipped with a clear hint.

func retentionWindow(t *testing.T) time.Duration {
	t.Helper()
	v := os.Getenv("QUEEN_RETENTION_INTERVAL_MS")
	if v == "" {
		t.Skip("set QUEEN_RETENTION_INTERVAL_MS to the broker's RETENTION_INTERVAL (e.g. 2000) to run retention tests")
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		t.Fatalf("QUEEN_RETENTION_INTERVAL_MS must be a positive integer, got %q", v)
	}
	// Wait for at least 4 retention intervals to leave room for one full
	// sweep + the configured retention window itself. Cap at 30s so the
	// suite stays under timeout.
	d := time.Duration(n*4) * time.Millisecond
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	return d
}

// TestRetention_PendingMessagesAreCleanedUp mirrors retention.js#retentionTest.
// Push 100 pending messages with retentionSeconds=10; after the retention
// sweep fires they must be gone.
func TestRetention_PendingMessagesAreCleanedUp(t *testing.T) {
	wait := retentionWindow(t)
	q := uniqueQueue(t, "retention-pending")
	runOK(t, "queue", "configure", q,
		"--retention", "5",
		"--completed-retention", "5",
	)
	items := make([]any, 100)
	for i := range items {
		items[i] = map[string]any{"i": i}
	}
	pushNDJSON(t, q, "", items)

	// retentionWindow waits long enough for the broker's periodic sweep to
	// fire AND clear any messages whose age exceeds retentionSeconds.
	time.Sleep(wait + 6*time.Second)

	got := popN(t, q, 100, "--cg", "ct-ret-pending",
		"--auto-ack", "--wait=false", "--timeout", "200ms")
	if len(got) != 0 {
		t.Errorf("expected retention sweep to clear pending msgs, got %d back", len(got))
	}
}

// TestRetention_CompletedMessagesAreCleanedUp pushes, drains, then waits
// long enough for the completed-retention sweep to delete the completed
// rows. We assert via the messages list endpoint that no completed rows
// remain.
func TestRetention_CompletedMessagesAreCleanedUp(t *testing.T) {
	wait := retentionWindow(t)
	q := uniqueQueue(t, "retention-completed")
	runOK(t, "queue", "configure", q,
		"--retention", "60",
		"--completed-retention", "3",
	)
	items := make([]any, 20)
	for i := range items {
		items[i] = map[string]any{"i": i}
	}
	pushNDJSON(t, q, "", items)
	got := popN(t, q, 20, "--cg", "ct-ret-comp", "--auto-ack", "--timeout", "5s")
	if len(got) != 20 {
		t.Fatalf("setup drain: got %d, want 20", len(got))
	}

	time.Sleep(wait + 4*time.Second)

	rows := listAllMessages(t, q)
	completed := 0
	for _, r := range rows {
		if r["status"] == "completed" || r["queueStatus"] == "completed" {
			completed++
		}
	}
	if completed > 0 {
		t.Errorf("expected 0 completed rows after retention sweep, got %d", completed)
	}
}

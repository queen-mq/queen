package tests

import (
	"os"
	"strconv"
	"testing"
	"time"
)

// TestRetention_* mirror clients/client-js/test-v2/retention.js. The broker
// sweeps on RETENTION_INTERVAL (default 5000ms -- NOT 60s, as this comment
// claimed while the tests were skipped and nobody could notice).
//
// QUEEN_RETENTION_INTERVAL_MS does NOT configure the broker; it only tells
// these tests what the broker's cadence is so they can size their wait. Set it
// to the same number the broker runs with, or the wait below is computed off a
// cadence that is not the real one. Left unset the tests skip; test/compose
// sets it next to the broker's RETENTION_INTERVAL.

// backoffCycles is the slack, in sweep intervals, that retentionSleep waits
// past a retention window. See retentionSleep for where the number comes from.
const backoffCycles = 8

// retentionSleep is how long to wait before a queue configured with a
// `window`-second retention rule is guaranteed to have been swept.
//
// The raft sweep (server/src/rsm/maintenance.rs) runs on the leader every
// RETENTION_INTERVAL and judges every partition on every pass, with no
// backoff, so data past its window is gone after the first pass that follows
// the cutoff: window + 1 interval, plus the commit of that pass. The wait here
// is longer, window + (backoffCycles+1) intervals: the bound the previous
// engine's sweep needed (it parked a partition for 8 cycles after a visit that
// deleted nothing), which the JS twin (retention.js) still waits. On raft it is
// generous, never short.
func retentionSleep(t *testing.T, window time.Duration) time.Duration {
	t.Helper()
	v := os.Getenv("QUEEN_RETENTION_INTERVAL_MS")
	if v == "" {
		t.Skip("set QUEEN_RETENTION_INTERVAL_MS to the broker's RETENTION_INTERVAL (e.g. 5000) to run retention tests")
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		t.Fatalf("QUEEN_RETENTION_INTERVAL_MS must be a positive integer, got %q", v)
	}
	interval := time.Duration(n) * time.Millisecond
	// One extra interval of margin so we land after the sweep, not on it.
	return window + backoffCycles*interval + interval
}

// TestRetention_PendingMessagesAreCleanedUp mirrors retention.js#retentionTest.
// Push 100 pending messages with retentionSeconds=10; after the retention
// sweep fires they must be gone.
func TestRetention_PendingMessagesAreCleanedUp(t *testing.T) {
	// Must match --retention below.
	wait := retentionSleep(t, 5*time.Second)
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

	time.Sleep(wait)

	// --from-mode all keeps the assertion meaningful: under the default
	// 'new' mode this CG would read 0 whether or not retention swept.
	got := popN(t, q, 100, "--cg", "ct-ret-pending", "--from-mode", "all",
		"--auto-ack", "--wait=false", "--timeout", "200ms")
	if len(got) != 0 {
		t.Errorf("expected retention sweep to clear pending msgs, got %d back", len(got))
	}
}

// TestRetention_CompletedMessagesAreCleanedUp pushes, drains, then waits for the
// completed-retention sweep to delete the drained messages.
//
// The promise, stated at the API: once a consumed message is older than the
// queue's completed retention, its data is gone. Nobody can read it back, not
// even a consumer group that has never read the queue and starts from its
// beginning. And the messages listing must not advertise that payload as
// available. (--retention 60 keeps the pending sweep out of it at the
// harness's 5s cadence: the checks run ~48s after the push, while the messages
// are younger than 60s, so only the completed-retention sweep can have removed
// them.)
func TestRetention_CompletedMessagesAreCleanedUp(t *testing.T) {
	// Must match --completed-retention below: the completed rows are what this
	// one waits on.
	wait := retentionSleep(t, 3*time.Second)
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
	got := popN(t, q, 20, "--cg", "ct-ret-comp", "--from-mode", "all", "--auto-ack", "--timeout", "5s")
	if len(got) != 20 {
		t.Fatalf("setup drain: got %d, want 20", len(got))
	}

	time.Sleep(wait)

	// The data is gone: a brand-new group reading from the beginning gets
	// nothing back. --from-mode all for the same reason as the pending test
	// above: under the default 'new' mode it would read 0 whether or not
	// retention swept.
	fresh := popN(t, q, 20, "--cg", "ct-ret-comp-fresh", "--from-mode", "all",
		"--auto-ack", "--wait=false", "--timeout", "200ms")
	if len(fresh) != 0 {
		t.Errorf("expected completed-retention sweep to delete the drained msgs, a fresh group read %d back", len(fresh))
	}

	// And the listing must not advertise the payload as available.
	for _, r := range listAllMessages(t, q) {
		if avail, ok := r["payloadAvailable"].(bool); ok && avail {
			t.Errorf("row still advertises payloadAvailable after the completed-retention sweep: %v", r["queuePath"])
			break
		}
	}
}

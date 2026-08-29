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
// cadence that is not the real one. Left unset the tests skip, which is how
// they sat dead in CI: test/compose has never set it.

// backoffCycles mirrors BACKOFF_BASE_CYCLES in server/src/retention.rs. Keep in
// step with it -- see retentionSleep for why the number is load-bearing here.
const backoffCycles = 8

// retentionSleep is how long to wait before a queue configured with a
// `window`-second retention rule is guaranteed to have been swept.
//
// It is NOT "a few sweep intervals". retention.rs strikes any partition whose
// visit deletes nothing and parks it for BACKOFF_BASE_CYCLES cycles; the sweep
// that runs while the rows are still younger than `window` IS such a visit, so
// the first real chance to delete is a whole backoff behind it:
//
//	bound = window + BACKOFF_BASE_CYCLES * RETENTION_INTERVAL
//
// The old helper returned 4 intervals capped at 30s, which is under that bound
// at every cadence -- these tests would have failed the day they stopped
// skipping. Measured on the JS twin (retention.js): rows still present at
// 15s/25s/35s with window=10s, gone by 50s, exactly window + 8*5s.
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
// completed-retention sweep to delete the drained messages' DATA.
//
// It asserts on queen.log_segments, not on `messages list`, and the difference
// is the whole point. `messages list` enumerates queen.log_txns -- the dedup
// index -- whose purge cutoff is a different phase with its own floor:
//
//	txns_cutoff = now() - GREATEST(dedup_window_seconds, completed_retention_seconds, 900)
//
// so those rows outlive a 3s completed-retention by at least 900s, and by
// default (dedup_window_seconds=3600) by an hour. This test used to count
// `messages list` rows and expect 0, which no RETENTION_INTERVAL could ever
// make true -- it was reading the dedup window and calling it retention. It
// never failed only because it skipped. Measured: segments for this queue go
// from 1 to 0 between t=5s and t=15s while the listing still reports 20 rows
// (correctly flagged payloadAvailable:false) well past 75s.
func TestRetention_CompletedMessagesAreCleanedUp(t *testing.T) {
	// Must match --completed-retention below: the completed rows are what this
	// one waits on, and they are swept by the same phase (and so the same
	// backoff map) as the pending rows above.
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

	// The retention promise is that the DATA is gone. Segment rows are what
	// hold it, and what `retention: swept segments_deleted=N` counts.
	segs := pgRow(t, `SELECT count(*) FROM queen.log_segments s
		JOIN queen.log_partitions p ON p.id = s.partition_id
		JOIN queen.queues qq ON qq.id = p.queue_id
		WHERE qq.name = $1`, q)
	if len(segs) == 0 {
		t.Fatalf("segment count query returned no row")
	}
	if n, ok := segs[0].(int64); !ok || n != 0 {
		t.Errorf("expected 0 segments after completed-retention sweep, got %v", segs[0])
	}

	// And the listing, which outlives it by the dedup window, must at least
	// stop claiming the payload is fetchable.
	for _, r := range listAllMessages(t, q) {
		if avail, ok := r["payloadAvailable"].(bool); ok && avail {
			t.Errorf("row still advertises payloadAvailable after its segment was deleted: %v", r["queuePath"])
			break
		}
	}
}

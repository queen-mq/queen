package tests

import (
	"context"
	"testing"
	"time"
)

// TestWatermark_* mirror clients/client-js/test-v2/watermark.js. The
// underlying machinery is the consumer_watermarks table that optimizes POP
// by remembering when a CG last saw the queue empty. The fix being verified
// here is: seek and CG delete must reset the watermark so a backward seek
// or a fresh CG sees historical messages again.
//
// The JS suite runs against a Pg-connected helper that artificially advances
// the watermark 10 minutes into the future so the 2-minute lookback window
// in the SP filter excludes the partition. We do the same here when a
// PG_HOST is available; otherwise we exercise the fast path (no advanced
// watermark) and only assert the simpler invariant that re-consume after
// seek-to-beginning yields the same N messages.

const watermarkSeekQueue = "watermark-seek"
const watermarkDeleteQueue = "watermark-delete"

// TestWatermark_SeekBackwardsAllowsReconsume mirrors
// watermark.js#seekBackwardsAllowsReconsume.
func TestWatermark_SeekBackwardsAllowsReconsume(t *testing.T) {
	q := uniqueQueue(t, watermarkSeekQueue)
	createQueue(t, q)
	cg := "ct-wm-seek"

	// Push 10 messages each to its own partition so a wildcard pop has to
	// walk the partition_lookup table (where the watermark filter applies).
	for i := 0; i < 10; i++ {
		pushOne(t, q, "p-"+itoa(i), map[string]any{"i": i, "batch": "original"})
	}
	time.Sleep(300 * time.Millisecond)

	// First drain.
	got := popN(t, q, 100,
		"--cg", cg, "--auto-ack",
		"--from-mode", "all",
		"--max-partitions", "10",
		"--timeout", "5s",
	)
	if len(got) != 10 {
		t.Fatalf("first drain: got %d, want 10", len(got))
	}

	// Optionally age the watermark via direct SQL to recreate the bug
	// scenario from the JS test. Without PG access, we still cover the
	// "seek-to-beginning resets the cursor" invariant - just not the
	// 2-minute-lookback edge case.
	if pg != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = pg.Exec(ctx, `
			UPDATE queen.consumer_watermarks
			   SET last_empty_scan_at = NOW() + interval '10 minutes',
			       updated_at         = NOW() + interval '10 minutes'
			 WHERE queue_name = $1 AND consumer_group = $2
		`, q, cg)
	}

	// Seek backwards.
	runOK(t, "replay", q, "--cg", cg, "--to", "beginning")
	time.Sleep(300 * time.Millisecond)

	again := popN(t, q, 100,
		"--cg", cg, "--auto-ack",
		"--from-mode", "all",
		"--max-partitions", "10",
		"--timeout", "5s",
	)
	if len(again) != 10 {
		t.Errorf("after seek-backwards + watermark age: got %d, want 10", len(again))
	}
}

// TestWatermark_DeleteCGAllowsReconsume mirrors
// watermark.js#deleteConsumerGroupAllowsReconsume. Deleting a CG must clear
// its watermark so a brand-new subscription sees historical data.
func TestWatermark_DeleteCGAllowsReconsume(t *testing.T) {
	q := uniqueQueue(t, watermarkDeleteQueue)
	createQueue(t, q)
	cg := "ct-wm-del"

	for i := 0; i < 8; i++ {
		pushOne(t, q, "p-"+itoa(i), map[string]any{"i": i})
	}
	time.Sleep(300 * time.Millisecond)

	got := popN(t, q, 100,
		"--cg", cg, "--auto-ack",
		"--from-mode", "all",
		"--max-partitions", "8",
		"--timeout", "5s",
	)
	if len(got) != 8 {
		t.Fatalf("first drain: got %d, want 8", len(got))
	}

	if pg != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = pg.Exec(ctx, `
			UPDATE queen.consumer_watermarks
			   SET last_empty_scan_at = NOW() + interval '10 minutes',
			       updated_at         = NOW() + interval '10 minutes'
			 WHERE queue_name = $1 AND consumer_group = $2
		`, q, cg)
	}

	// Delete the CG (with --metadata so the SP also clears the watermark).
	runOK(t, "cg", "delete", cg, "--queue", q, "--metadata", "--yes")
	time.Sleep(300 * time.Millisecond)

	// New subscription on the SAME CG must see all 8 again with mode=all.
	again := popN(t, q, 100,
		"--cg", cg, "--auto-ack",
		"--from-mode", "all",
		"--max-partitions", "8",
		"--timeout", "5s",
	)
	if len(again) != 8 {
		t.Errorf("after CG delete + re-subscribe: got %d, want 8", len(again))
	}
}

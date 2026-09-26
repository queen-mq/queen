package tests

import (
	"testing"
	"time"
)

// TestWatermark_* mirror clients/client-js/test-v2/watermark.js (the names
// follow the JS twin). They pin two behaviours at the CLI, once a consumer
// group has drained a queue:
//
//   - seeking the group back to the beginning makes it consume every message
//     again;
//   - deleting the group, metadata included, and subscribing again under the
//     same name also consumes every message again.
//
// Each message sits in its own partition, so every drain is a wildcard pop
// across partitions and the re-consume has to reach all of them.

const watermarkSeekQueue = "watermark-seek"
const watermarkDeleteQueue = "watermark-delete"

// TestWatermark_SeekBackwardsAllowsReconsume mirrors
// watermark.js#seekBackwardsAllowsReconsume.
func TestWatermark_SeekBackwardsAllowsReconsume(t *testing.T) {
	q := uniqueQueue(t, watermarkSeekQueue)
	createQueue(t, q)
	cg := "ct-wm-seek"

	// Push 10 messages, each to its own partition.
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
		t.Errorf("after seek to beginning: got %d, want 10", len(again))
	}
}

// TestWatermark_DeleteCGAllowsReconsume mirrors
// watermark.js#deleteConsumerGroupAllowsReconsume. After the group is deleted,
// a new subscription under the same name must see the historical data again.
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

	// Delete the CG, metadata included, so the next pop under the same name
	// is a brand-new subscription.
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

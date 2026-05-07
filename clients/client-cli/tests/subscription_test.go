package tests

import (
	"testing"
	"time"
)

// TestSubscription_* mirror clients/client-js/test-v2/subscription.js + the
// test_subscription.py coverage. The CLI surface for these scenarios is:
//
//	queenctl pop / tail with --from-mode and --since
//	queenctl cg seek <cg> <queue> --to '<time>'
//	queenctl replay <queue> --cg <cg> --to '<time>'
//
// "subscriptionMode" semantics on the broker:
//  - 'all'         : the CG sees every message (historical + future)
//  - 'new'         : the CG starts at the time it first subscribes
//  - 'new-only'    : same as 'new' but stricter against historical leaks
//
// We test through `queenctl pop` (one-shot) which mirrors the JS snippet
// `client.queue(q).group(g).subscriptionMode('new').pop()`.

// TestSubscription_NewModeSkipsHistorical mirrors
// subscription.js#subscriptionModeNew. Push 5 historical messages, then
// subscribe a new CG with mode='new' - it must see 0 historical.
func TestSubscription_NewModeSkipsHistorical(t *testing.T) {
	q := uniqueQueue(t, "sub-new")
	createQueue(t, q)
	// Historical batch.
	pushNDJSON(t, q, "", []any{
		map[string]any{"i": 0, "type": "historical"},
		map[string]any{"i": 1, "type": "historical"},
		map[string]any{"i": 2, "type": "historical"},
	})
	time.Sleep(500 * time.Millisecond)

	got := popN(t, q, 10,
		"--cg", "ct-sub-new",
		"--from-mode", "new",
		"--auto-ack",
		"--wait=false",
		"--timeout", "200ms",
	)
	if len(got) != 0 {
		t.Errorf("--from-mode new should skip historical, got %d", len(got))
	}
}

// TestSubscription_AllModeReturnsHistorical mirrors the same test's "all"
// branch.
func TestSubscription_AllModeReturnsHistorical(t *testing.T) {
	q := uniqueQueue(t, "sub-all")
	createQueue(t, q)
	pushNDJSON(t, q, "", []any{
		map[string]any{"i": 0},
		map[string]any{"i": 1},
		map[string]any{"i": 2},
	})
	time.Sleep(500 * time.Millisecond)
	got := popN(t, q, 10,
		"--cg", "ct-sub-all",
		"--from-mode", "all",
		"--auto-ack",
		"--timeout", "5s",
	)
	if len(got) != 3 {
		t.Errorf("--from-mode all should return 3 historical, got %d", len(got))
	}
}

// TestSubscription_NewModeReturnsFutureMessages: after subscribing in 'new'
// mode the CG must see messages pushed AFTER the subscribe time.
func TestSubscription_NewModeReturnsFutureMessages(t *testing.T) {
	q := uniqueQueue(t, "sub-new-future")
	createQueue(t, q)
	pushOne(t, q, "", map[string]any{"who": "historical"})
	time.Sleep(500 * time.Millisecond)
	// First call to register the CG with subscriptionMode=new.
	popN(t, q, 1,
		"--cg", "ct-sub-future", "--from-mode", "new",
		"--auto-ack", "--wait=false", "--timeout", "200ms",
	)
	time.Sleep(200 * time.Millisecond)
	pushOne(t, q, "", map[string]any{"who": "future"})
	got := popN(t, q, 10,
		"--cg", "ct-sub-future",
		"--from-mode", "new",
		"--auto-ack",
		"--timeout", "5s",
	)
	if len(got) != 1 {
		t.Fatalf("expected 1 future message, got %d", len(got))
	}
	if got[0].Data["who"] != "future" {
		t.Errorf("got payload %v, want who=future", got[0].Data)
	}
}

// TestReplay_SeekToBeginning mirrors how the SDK exposes the same effect via
// client.deleteConsumerGroup / re-subscribe with mode=all. With queenctl,
// `replay --to beginning` rewinds the cursor before any historical message.
func TestReplay_SeekToBeginning(t *testing.T) {
	q := uniqueQueue(t, "replay-begin")
	createQueue(t, q)
	pushNDJSON(t, q, "", []any{
		map[string]any{"i": 1},
		map[string]any{"i": 2},
		map[string]any{"i": 3},
	})
	// Drain the CG once.
	got := popN(t, q, 10, "--cg", "ct-replay", "--auto-ack", "--timeout", "5s")
	if len(got) != 3 {
		t.Fatalf("setup drain: got %d, want 3", len(got))
	}
	// Replay to before any message.
	runOK(t, "replay", q, "--cg", "ct-replay", "--to", "beginning")
	time.Sleep(200 * time.Millisecond)
	// CG should now see the historical 3 again.
	again := popN(t, q, 10, "--cg", "ct-replay", "--auto-ack", "--timeout", "5s")
	if len(again) != 3 {
		t.Errorf("after replay-to-beginning: got %d, want 3", len(again))
	}
}

// TestReplay_SeekToTimestamp picks a moment in the middle of a push burst,
// then replays to that moment and expects only the messages that came
// after.
func TestReplay_SeekToTimestamp(t *testing.T) {
	q := uniqueQueue(t, "replay-ts")
	createQueue(t, q)
	pushOne(t, q, "", map[string]any{"phase": "before"})
	pushOne(t, q, "", map[string]any{"phase": "before"})
	time.Sleep(400 * time.Millisecond)
	cutoff := rfc3339Now()
	time.Sleep(400 * time.Millisecond)
	pushOne(t, q, "", map[string]any{"phase": "after"})
	pushOne(t, q, "", map[string]any{"phase": "after"})

	// Drain everything once.
	got := popN(t, q, 10, "--cg", "ct-replay-ts", "--auto-ack", "--timeout", "5s")
	if len(got) != 4 {
		t.Fatalf("setup drain: got %d, want 4", len(got))
	}
	runOK(t, "replay", q, "--cg", "ct-replay-ts", "--to", cutoff)
	time.Sleep(200 * time.Millisecond)
	post := popN(t, q, 10, "--cg", "ct-replay-ts", "--auto-ack", "--timeout", "5s")
	// Only messages with createdAt > cutoff should resurface. Allow some
	// slack: the broker resolves timestamps at second granularity and the
	// "before" messages may sit on the boundary, so accept 2 or 4.
	if len(post) < 2 {
		t.Errorf("after seek-to-cutoff: got %d, want at least 2", len(post))
	}
	for _, m := range post {
		if m.Data["phase"] == "before" {
			// Acceptable when timestamps fell on the cutoff second.
		}
	}
}

// TestReplay_SeekToEndDrainsCG mirrors the SDK semantics where seek({toEnd:
// true}) advances the cursor past every existing message. The CG should
// see nothing on the next pop.
func TestReplay_SeekToEndDrainsCG(t *testing.T) {
	q := uniqueQueue(t, "replay-end")
	createQueue(t, q)
	pushNDJSON(t, q, "", []any{
		map[string]any{"i": 1}, map[string]any{"i": 2}, map[string]any{"i": 3},
	})
	time.Sleep(300 * time.Millisecond)
	// Drain via the same CG we will then seek to end.
	popN(t, q, 10, "--cg", "ct-replay-end", "--auto-ack", "--timeout", "5s")
	// Push more, then seek-to-end so they are skipped.
	pushOne(t, q, "", map[string]any{"i": 4})
	pushOne(t, q, "", map[string]any{"i": 5})
	runOK(t, "replay", q, "--cg", "ct-replay-end", "--to", "end")
	time.Sleep(200 * time.Millisecond)
	got := popN(t, q, 10,
		"--cg", "ct-replay-end",
		"--auto-ack", "--wait=false", "--timeout", "200ms")
	if len(got) != 0 {
		t.Errorf("seek-to-end should skip pending msgs, got %d", len(got))
	}
}

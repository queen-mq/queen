package tests

import (
	"context"
	"encoding/json"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestTail_* mirror clients/client-js/test-v2/consume.js + test_consume.py.
// In the CLI surface, `queenctl tail` plays the role of a streaming
// consumer: each popped message is emitted as one NDJSON line.

// TestTail_BasicHandlerDelivery mirrors consume.js#testConsumer. Push 1,
// tail with --limit 1, expect one NDJSON line on stdout.
func TestTail_BasicHandlerDelivery(t *testing.T) {
	q := uniqueQueue(t, "tail-basic")
	createQueue(t, q)
	pushOne(t, q, "", map[string]any{"hello": "world"})

	out := runOK(t, "tail", q, "--cg", "ct-tail-basic", "--auto-ack",
		"--limit", "1", "--idle-millis", "3000")
	msgs := parseNDJSONMessages(t, out)
	if len(msgs) != 1 {
		t.Fatalf("got %d msgs, want 1\nstdout: %s", len(msgs), out)
	}
	if msgs[0].Data["hello"] != "world" {
		t.Errorf("payload = %v", msgs[0].Data)
	}
}

// TestTail_DrainsExactCount pushes 10, tails with --limit 10, expects 10.
func TestTail_DrainsExactCount(t *testing.T) {
	q := uniqueQueue(t, "tail-drain")
	createQueue(t, q)
	items := make([]any, 10)
	for i := range items {
		items[i] = map[string]any{"i": i}
	}
	pushNDJSON(t, q, "", items)

	out := runOK(t, "tail", q, "--cg", "ct-drain", "--auto-ack",
		"--limit", "10", "--idle-millis", "3000")
	msgs := parseNDJSONMessages(t, out)
	if len(msgs) != 10 {
		t.Errorf("expected 10, got %d", len(msgs))
	}
}

// TestTail_IdleTimeoutExitsCleanly pushes nothing and tails with a short
// --idle-millis. The command must exit cleanly (0 or 4) within ~3s.
func TestTail_IdleTimeoutExitsCleanly(t *testing.T) {
	q := uniqueQueue(t, "tail-idle")
	createQueue(t, q)

	start := time.Now()
	_, _, code := runWith(runOpts{timeout: 10 * time.Second},
		"tail", q, "--cg", "ct-idle", "--auto-ack",
		"--idle-millis", "2000")
	elapsed := time.Since(start)
	if code != 0 && code != 4 {
		t.Errorf("idle exit code = %d, want 0 or 4", code)
	}
	if elapsed > 8*time.Second {
		t.Errorf("idle exit took %s, expected ≤ 5s", elapsed)
	}
}

// TestTail_FollowAndCancelEmitsLiveMessages spawns a long-running tail
// --follow goroutine, pushes messages while it runs, asserts at least one
// is received, then cancels.
func TestTail_FollowAndCancelEmitsLiveMessages(t *testing.T) {
	q := uniqueQueue(t, "tail-follow")
	createQueue(t, q)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binPath,
		"--server", serverURL, "tail", q, "--cg", "ct-follow", "--auto-ack",
		"--follow", "--batch", "1")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	// Push 3 messages spread out so the live follow loop has time to react.
	go func() {
		time.Sleep(300 * time.Millisecond)
		for i := 0; i < 3; i++ {
			pushOne(t, q, "", map[string]any{"i": i})
			time.Sleep(150 * time.Millisecond)
		}
	}()

	dec := json.NewDecoder(stdout)
	var got []map[string]any
	timeout := time.After(8 * time.Second)
	var mu sync.Mutex
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			var m map[string]any
			if err := dec.Decode(&m); err != nil {
				return
			}
			mu.Lock()
			got = append(got, m)
			n := len(got)
			mu.Unlock()
			if n >= 3 {
				return
			}
		}
	}()
	select {
	case <-done:
	case <-timeout:
	}
	cancel()
	_ = cmd.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(got) < 3 {
		t.Errorf("follow got %d messages, want ≥ 3", len(got))
	}
}

// TestTail_PartitionFilter mirrors consume.js#testConsumerWithPartition.
// Push to two partitions, tail one, expect only that partition's data.
func TestTail_PartitionFilter(t *testing.T) {
	q := uniqueQueue(t, "tail-part")
	createQueue(t, q)
	pushOne(t, q, "p0", map[string]any{"who": "p0"})
	pushOne(t, q, "p1", map[string]any{"who": "p1"})

	out := runOK(t, "tail", q,
		"--cg", "ct-tail-part",
		"--partition", "p0",
		"--auto-ack",
		"--limit", "1",
		"--idle-millis", "3000")
	msgs := parseNDJSONMessages(t, out)
	if len(msgs) != 1 || msgs[0].Partition != "p0" {
		t.Fatalf("got %d msgs, partitions=%v", len(msgs), partitions(msgs))
	}
}

// TestTail_MultiPartitionMaxN mirrors pop.js#popV4MultiPartitionBasic but
// through tail: --max-partitions 3 with three populated partitions must
// yield 6 messages.
func TestTail_MultiPartitionMaxN(t *testing.T) {
	q := uniqueQueue(t, "tail-v4")
	createQueue(t, q)
	for p := 0; p < 3; p++ {
		pushNDJSON(t, q, fmtP(p), []any{
			map[string]any{"p": p, "m": 0},
			map[string]any{"p": p, "m": 1},
		})
	}
	time.Sleep(500 * time.Millisecond)

	out := runOK(t, "tail", q,
		"--cg", "ct-tail-v4",
		"--auto-ack",
		"--max-partitions", "3",
		"--limit", "6",
		"--idle-millis", "3000")
	msgs := parseNDJSONMessages(t, out)
	if len(msgs) != 6 {
		t.Errorf("multi-partition tail: got %d, want 6", len(msgs))
	}
	pids := map[string]struct{}{}
	for _, m := range msgs {
		pids[m.PartitionID] = struct{}{}
	}
	if len(pids) != 3 {
		t.Errorf("distinct partitions = %d, want 3", len(pids))
	}
}

func partitions(ms []cliMessage) []string {
	out := make([]string, 0, len(ms))
	for _, m := range ms {
		out = append(out, m.Partition)
	}
	return out
}

// Sanity-check that the `tail` output is genuinely NDJSON (one JSON object
// per line, no whitespace separators between objects).
func TestTail_NDJSONFormat(t *testing.T) {
	q := uniqueQueue(t, "tail-ndjson")
	createQueue(t, q)
	pushNDJSON(t, q, "", []any{map[string]any{"i": 1}, map[string]any{"i": 2}})
	out := runOK(t, "tail", q, "--cg", "ct-ndjson", "--auto-ack",
		"--limit", "2", "--idle-millis", "2000")
	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 NDJSON lines, got %d:\n%s", len(lines), out)
	}
	for _, l := range lines {
		var m map[string]any
		if err := json.Unmarshal([]byte(l), &m); err != nil {
			t.Errorf("line is not JSON: %q (err: %v)", l, err)
		}
	}
}

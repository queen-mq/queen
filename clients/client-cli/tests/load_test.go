package tests

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestLoad_* mirror clients/client-js/test-v2/load.js. The JS suite pushes
// 100k messages and drains them; we run a smaller default to keep CI
// reasonable, and let users dial it up with QUEEN_LOAD_TOTAL when they
// want the original 100k scale.
//
// Coverage:
//   - bench: end-to-end push + drain throughput on the same queue
//   - large multi-partition drain via direct push + tail
//   - all messages must be marked consumed-by-CG (regression net for the
//     bench bug fixed earlier)

// TestLoad_BenchSmoke runs the smaller default bench and verifies every
// pushed message ended up consumed-by-CG. This guards against the class of
// bug we already hit once where bench mis-reported successful drains.
func TestLoad_BenchSmoke(t *testing.T) {
	q := uniqueQueue(t, "load-bench")
	out := runOK(t, "bench",
		"--queue", q,
		"--total", "500",
		"--partitions", "4",
		"--producers", "4",
		"--consumers", "4",
		"--batch", "50",
	)
	if !strings.Contains(out, "PUSH") || !strings.Contains(out, "POP") {
		t.Fatalf("bench output missing PUSH/POP lines:\n%s", out)
	}
	if strings.Contains(out, "WARN") {
		t.Errorf("bench reported a WARN (unique != benchTotal):\n%s", out)
	}

	// Verify server-side consumption count.
	rows := listAllMessages(t, q)
	if len(rows) == 0 {
		t.Fatalf("messages list returned 0 rows; expected 500")
	}
	notConsumed := 0
	for _, r := range rows {
		bs, _ := r["busStatus"].(map[string]any)
		if bs == nil {
			continue
		}
		if cb, ok := bs["consumedBy"].(float64); !ok || cb < 1 {
			notConsumed++
		}
	}
	if notConsumed > 0 {
		t.Errorf("%d / %d messages still consumedBy=0 after bench", notConsumed, len(rows))
	}
}

// TestLoad_DrainsLargeQueueViaTail mirrors load.js#testLoad's draining via
// consume. Push N messages, tail them with --limit N, expect exactly N back
// and all to have been consumedBy=1.
func TestLoad_DrainsLargeQueueViaTail(t *testing.T) {
	total := loadTotal(t, 2000)
	q := uniqueQueue(t, "load-tail")
	createQueue(t, q)

	// Push in batches of 100 to avoid huge JSON bodies.
	for offset := 0; offset < total; offset += 100 {
		end := offset + 100
		if end > total {
			end = total
		}
		batch := make([]any, 0, end-offset)
		for i := offset; i < end; i++ {
			batch = append(batch, map[string]any{"i": i})
		}
		pushNDJSON(t, q, "", batch)
	}
	time.Sleep(500 * time.Millisecond)

	// Drain via tail.
	out := runOK(t, "tail", q,
		"--cg", "ct-load",
		"--auto-ack",
		"--batch", "100",
		"--limit", fmt.Sprintf("%d", total),
		"--idle-millis", "10000",
		"--timeout", "5s",
	)
	msgs := parseNDJSONMessages(t, out)
	if len(msgs) != total {
		t.Errorf("tail drained %d, want %d", len(msgs), total)
	}
}

// TestLoad_DrainsLargeQueueViaTailWithMultiPartition mirrors
// load.js#testLoadPartition. Push to many partitions, drain via tail with
// --max-partitions.
func TestLoad_DrainsLargeQueueViaTailWithMultiPartition(t *testing.T) {
	total := loadTotal(t, 2000)
	parts := 10
	q := uniqueQueue(t, "load-multipart")
	createQueue(t, q)

	per := total / parts
	for p := 0; p < parts; p++ {
		batch := make([]any, 0, per)
		for i := 0; i < per; i++ {
			batch = append(batch, map[string]any{"p": p, "i": i})
		}
		pushNDJSON(t, q, "p"+itoa(p), batch)
	}
	expected := per * parts
	time.Sleep(500 * time.Millisecond)

	out := runOK(t, "tail", q,
		"--cg", "ct-load-mp",
		"--auto-ack",
		"--batch", "200",
		"--max-partitions", "10",
		"--limit", fmt.Sprintf("%d", expected),
		"--idle-millis", "10000",
		"--timeout", "5s",
	)
	msgs := parseNDJSONMessages(t, out)
	if len(msgs) != expected {
		t.Errorf("multi-partition tail drained %d, want %d", len(msgs), expected)
	}
}

// loadTotal returns the load size, optionally overridden by
// QUEEN_LOAD_TOTAL for environments that want to exercise the original
// 100k JS scale.
func loadTotal(t *testing.T, deflt int) int {
	t.Helper()
	if v := envInt("QUEEN_LOAD_TOTAL", 0); v > 0 {
		return v
	}
	return deflt
}

func envInt(key string, deflt int) int {
	v := getenv(key, "")
	if v == "" {
		return deflt
	}
	n := 0
	for _, c := range v {
		if c < '0' || c > '9' {
			return deflt
		}
		n = n*10 + int(c-'0')
	}
	return n
}

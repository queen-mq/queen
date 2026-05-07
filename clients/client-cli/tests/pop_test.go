package tests

import (
	"strings"
	"testing"
	"time"
)

// TestPop_* mirror clients/client-js/test-v2/pop.js and
// clients/client-py/tests/test_pop.py via 'queenctl pop'.

// TestPop_EmptyQueue mirrors pop.js#popEmptyQueue.
func TestPop_EmptyQueue(t *testing.T) {
	q := uniqueQueue(t, "pop-empty")
	createQueue(t, q)
	_, _, code := run("pop", q, "--cg", "ct-pe", "-n", "1", "--wait=false", "--timeout", "100ms")
	if code != 4 {
		t.Errorf("empty queue pop should exit 4 (Empty), got %d", code)
	}
}

// TestPop_NonEmptyQueue mirrors pop.js#popNonEmptyQueue.
func TestPop_NonEmptyQueue(t *testing.T) {
	q := uniqueQueue(t, "pop-non-empty")
	createQueue(t, q)
	pushOne(t, q, "", map[string]any{"hello": "world"})
	got := popN(t, q, 1, "--cg", "ct-pn", "--auto-ack", "--timeout", "5s")
	if len(got) != 1 {
		t.Fatalf("expected 1 message, got %d", len(got))
	}
	if got[0].Data["hello"] != "world" {
		t.Errorf("payload = %v, want hello=world", got[0].Data)
	}
}

// TestPop_LongPolling mirrors pop.js#popWithWait. Push happens 1s after the
// pop starts; long-poll must surface the message inside the timeout.
func TestPop_LongPolling(t *testing.T) {
	q := uniqueQueue(t, "pop-long-poll")
	createQueue(t, q)
	go func() {
		time.Sleep(1 * time.Second)
		pushOne(t, q, "", map[string]any{"deferred": true})
	}()
	got := popN(t, q, 1, "--cg", "ct-lp", "--auto-ack", "--timeout", "5s")
	if len(got) != 1 || got[0].Data["deferred"] != true {
		t.Errorf("long-poll yielded %d msgs, payload=%v", len(got), got)
	}
}

// TestPop_AckThroughCLI verifies the explicit `queenctl ack` flow against
// a popped message that was NOT auto-acked. Mirrors pop.js#popWithAck.
func TestPop_AckThroughCLI(t *testing.T) {
	q := uniqueQueue(t, "pop-ack")
	createQueue(t, q)
	pushOne(t, q, "", map[string]any{"hi": 1})
	got := popN(t, q, 1, "--cg", "ct-ack", "--timeout", "5s")
	if len(got) != 1 {
		t.Fatalf("pop got %d, want 1", len(got))
	}
	m := got[0]
	stdout := runOK(t, "ack", m.TransactionID,
		"--partition-id", m.PartitionID,
		"--lease-id", m.LeaseID,
		"--cg", "ct-ack",
	)
	if !strings.Contains(stdout, "ack ok") {
		t.Errorf("ack output: %s", stdout)
	}
}

// TestPop_LeaseExpiryReDelivery mirrors pop.js#popWithAckReconsume. With a
// 1s lease, an unacked message is re-delivered to the same CG after the
// lease expires.
func TestPop_LeaseExpiryReDelivery(t *testing.T) {
	q := uniqueQueue(t, "pop-lease-expiry")
	runOK(t, "queue", "configure", q, "--lease-time", "1")
	pushOne(t, q, "", map[string]any{"x": 1})
	first := popN(t, q, 1, "--cg", "ct-lease", "--timeout", "5s")
	if len(first) != 1 {
		t.Fatalf("first pop got %d, want 1", len(first))
	}
	time.Sleep(2200 * time.Millisecond)
	second := popN(t, q, 1, "--cg", "ct-lease", "--timeout", "5s")
	if len(second) != 1 {
		t.Errorf("after lease expiry, got %d, want 1 (re-delivery)", len(second))
	}
}

// TestPop_BatchSizeRespected pushes 5, asks for batch=3, expects 3.
func TestPop_BatchSizeRespected(t *testing.T) {
	q := uniqueQueue(t, "pop-batch")
	createQueue(t, q)
	pushNDJSON(t, q, "", []any{
		map[string]any{"i": 1},
		map[string]any{"i": 2},
		map[string]any{"i": 3},
		map[string]any{"i": 4},
		map[string]any{"i": 5},
	})
	got := popN(t, q, 3, "--cg", "ct-bsize", "--auto-ack", "--timeout", "5s")
	if len(got) != 3 {
		t.Errorf("expected 3 messages, got %d", len(got))
	}
}

// TestPop_PartitionScoped pushes to two partitions, then pops only from one.
func TestPop_PartitionScoped(t *testing.T) {
	q := uniqueQueue(t, "pop-partition")
	createQueue(t, q)
	pushOne(t, q, "p0", map[string]any{"p": "p0"})
	pushOne(t, q, "p1", map[string]any{"p": "p1"})

	got := popN(t, q, 5, "--cg", "ct-part", "--partition", "p0", "--auto-ack", "--timeout", "5s")
	if len(got) != 1 {
		t.Fatalf("expected 1 from p0, got %d", len(got))
	}
	if got[0].Partition != "p0" || got[0].Data["p"] != "p0" {
		t.Errorf("got partition=%s data=%v, want p=p0", got[0].Partition, got[0].Data)
	}
}

// TestPop_NamespaceTaskFilter would mirror the namespace/task pop examples
// in the SDK suites. Skipped here because GET /api/v1/pop (the queueless
// endpoint that fans across queues sharing namespace/task tags) is
// documented in docs/http-api.html but NOT wired into the C++ broker -
// only /api/v1/pop/queue/:queue and /api/v1/pop/queue/:queue/partition/:partition
// exist in server/src/routes/pop.cpp. Once that route lands on the server,
// remove this skip and replace the body with the parity test.
func TestPop_NamespaceTaskFilter(t *testing.T) {
	t.Skip("server route GET /api/v1/pop (namespace/task) is documented but not implemented in pop.cpp")
}

// TestPop_V4MultiPartitionBasic mirrors pop.js#popV4MultiPartitionBasic.
// 3 partitions × 2 messages, --max-partitions 3 must drain all 6 with each
// message carrying its own partitionId; all six must share one leaseId.
func TestPop_V4MultiPartitionBasic(t *testing.T) {
	q := uniqueQueue(t, "pop-v4-basic")
	createQueue(t, q)
	for p := 0; p < 3; p++ {
		pushNDJSON(t, q, fmtP(p), []any{
			map[string]any{"p": p, "m": 0},
			map[string]any{"p": p, "m": 1},
		})
	}
	// Wait out the PUSHPOPLOOKUPSOL race - the partition_lookup commits
	// asynchronously after the push.
	time.Sleep(500 * time.Millisecond)

	got := popN(t, q, 100,
		"--cg", "ct-v4-basic", "--max-partitions", "3",
		"--wait=false", "--timeout", "1s")
	if len(got) != 6 {
		t.Fatalf("v4 multi-partition: got %d, want 6", len(got))
	}
	pids := map[string]struct{}{}
	leases := map[string]struct{}{}
	for _, m := range got {
		pids[m.PartitionID] = struct{}{}
		if m.LeaseID != "" {
			leases[m.LeaseID] = struct{}{}
		}
	}
	if len(pids) != 3 {
		t.Errorf("distinct partition ids = %d, want 3", len(pids))
	}
	if len(leases) != 1 {
		t.Errorf("distinct lease ids = %d, want 1 (shared lease)", len(leases))
	}
}

// TestPop_V4GlobalCap mirrors pop.js#popV4GlobalCap. batch=10 across 5
// partitions × 100 messages each must yield exactly 10.
func TestPop_V4GlobalCap(t *testing.T) {
	q := uniqueQueue(t, "pop-v4-cap")
	createQueue(t, q)
	for p := 0; p < 5; p++ {
		items := make([]any, 100)
		for i := range items {
			items[i] = map[string]any{"p": p, "m": i}
		}
		pushNDJSON(t, q, fmtP(p), items)
	}
	time.Sleep(500 * time.Millisecond)

	got := popN(t, q, 10,
		"--cg", "ct-v4-cap", "--max-partitions", "5",
		"--batch", "10", "--wait=false")
	if len(got) != 10 {
		t.Errorf("global cap test: got %d, want 10", len(got))
	}
}

// TestPop_V4DefaultOnePartition mirrors pop.js#popV4DefaultOne. Without
// --max-partitions, default is 1 partition per pop; only the first
// partition's messages come back.
func TestPop_V4DefaultOnePartition(t *testing.T) {
	q := uniqueQueue(t, "pop-v4-default")
	createQueue(t, q)
	for p := 0; p < 4; p++ {
		items := make([]any, 5)
		for i := range items {
			items[i] = map[string]any{"p": p, "m": i}
		}
		pushNDJSON(t, q, fmtP(p), items)
	}
	time.Sleep(500 * time.Millisecond)

	got := popN(t, q, 100, "--cg", "ct-v4-def", "--wait=false")
	if len(got) != 5 {
		t.Errorf("default 1-partition pop: got %d, want 5", len(got))
	}
	pids := map[string]struct{}{}
	for _, m := range got {
		pids[m.PartitionID] = struct{}{}
	}
	if len(pids) != 1 {
		t.Errorf("default pop spans %d partitions, want 1", len(pids))
	}
}

func fmtP(i int) string { return "p" + itoa(i) }

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	digits := []byte{}
	for i > 0 {
		digits = append([]byte{byte('0' + i%10)}, digits...)
		i /= 10
	}
	if neg {
		return "-" + string(digits)
	}
	return string(digits)
}

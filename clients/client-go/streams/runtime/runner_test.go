// Tests for the idle-flush sweep set (Runner.recent). Mirrors the rolling
// touchedAt cutoff in clients/client-js/client-v2/streams/runtime/Runner.js.
package runtime

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"testing"

	"github.com/smartpricing/queen/clients/client-go/streams/operators"
)

// newRecencyRunner builds a Runner with a tumbling window and a fake clock the
// caller drives through *nowPtr.
func newRecencyRunner(url string, nowPtr *int64) *Runner {
	stream := &CompiledStream{
		Stages: Stages{Window: operators.NewWindowTumblingOperator(60)},
	}
	r := NewRunner(stream, RunOptions{QueryID: "q-recency", URL: url})
	r.serverQueryID = "srv-q-recency"
	r.nowMs = func() int64 { return *nowPtr }
	return r
}

func TestTouchPartitionPrunesStalePartitions(t *testing.T) {
	now := int64(1_700_000_000_000)
	r := newRecencyRunner("http://127.0.0.1:1", &now)

	r.touchPartition("p-lost", "orders/lost")
	if _, ok := r.recent["p-lost"]; !ok {
		t.Fatalf("expected p-lost in the sweep set after touch")
	}

	// The partition rotates to another replica: this runner never pops it
	// again, and keeps popping a different one past the recency window.
	now += partitionRecencyMs + 1
	r.touchPartition("p-owned", "orders/owned")

	if _, ok := r.recent["p-lost"]; ok {
		t.Errorf("p-lost should have been pruned after %d ms untouched", partitionRecencyMs+1)
	}
	if e, ok := r.recent["p-owned"]; !ok || e.name != "orders/owned" || e.touchedAt != now {
		t.Errorf("p-owned should be in the sweep set with touchedAt=%d, got %+v (present=%v)", now, e, ok)
	}
}

func TestTouchPartitionKeepsPartitionInsideWindow(t *testing.T) {
	now := int64(1_700_000_000_000)
	r := newRecencyRunner("http://127.0.0.1:1", &now)

	r.touchPartition("p-a", "orders/a")
	now += partitionRecencyMs - 1
	r.touchPartition("p-b", "orders/b")

	if _, ok := r.recent["p-a"]; !ok {
		t.Errorf("p-a is still inside the recency window and must not be pruned")
	}
	if _, ok := r.recent["p-b"]; !ok {
		t.Errorf("p-b should be in the sweep set")
	}
}

// TestFlushTickSkipsStalePartitions is the regression this file exists for: a
// partition not touched within the cutoff must not be swept, because the flush
// path commits with ack=nil and the server skips its lease check on ack-less
// cycles — so flushing a partition another replica now owns double-emits.
func TestFlushTickSkipsStalePartitions(t *testing.T) {
	var mu sync.Mutex
	var asked []string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/streams/v1/state/get" {
			t.Errorf("unexpected request path %s", req.URL.Path)
		}
		var body struct {
			PartitionID string `json:"partition_id"`
		}
		_ = json.NewDecoder(req.Body).Decode(&body)
		mu.Lock()
		asked = append(asked, body.PartitionID)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"rows":[]}`))
	}))
	defer srv.Close()

	now := int64(1_700_000_000_000)
	r := newRecencyRunner(srv.URL, &now)

	// Seeded directly so the sweep-side prune is what is under test: a runner
	// whose source went quiet never calls touchPartition again.
	r.recent["p-stale"] = recentPartition{name: "orders/stale", touchedAt: now - partitionRecencyMs - 1}
	r.recent["p-fresh"] = recentPartition{name: "orders/fresh", touchedAt: now - 1_000}

	r.flushTick(context.Background())

	mu.Lock()
	got := append([]string(nil), asked...)
	mu.Unlock()
	sort.Strings(got)

	if len(got) != 1 || got[0] != "p-fresh" {
		t.Errorf("flush sweep should have touched only p-fresh, got %v", got)
	}
	if _, ok := r.recent["p-stale"]; ok {
		t.Errorf("p-stale should have been evicted from the sweep set, not just skipped")
	}
	if _, ok := r.recent["p-fresh"]; !ok {
		t.Errorf("p-fresh should have stayed in the sweep set")
	}
}

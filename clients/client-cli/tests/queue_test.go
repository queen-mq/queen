package tests

import (
	"strings"
	"testing"
)

// TestQueue covers the same surface as clients/client-js/test-v2/queue.js
// and clients/client-py/tests/test_queue.py - create, delete, configure -
// driven through the queenctl CLI:
//
//	queenctl queue configure <name> [flag...]
//	queenctl queue describe <name>
//	queenctl queue delete <name> --yes
//
// queue.configure is queenctl's idempotent "create or update". It maps to
// POST /api/v1/configure - the same endpoint queue().create() uses in the
// JS/Py SDKs.

func TestQueue_Create(t *testing.T) {
	q := uniqueQueue(t, "create")
	runOK(t, "queue", "configure", q)
	d := queueDetail(t, q)
	// `queue describe` (GET /api/v1/resources/queues/:q) returns the queue
	// detail at top-level, not wrapped: {name, id, partitions, totals,...}.
	if got, _ := d["name"].(string); got != q {
		t.Errorf("describe.name = %q, want %q", got, q)
	}
	if _, ok := d["partitions"].([]any); !ok {
		t.Errorf("describe missing partitions array: %v", d)
	}
}

func TestQueue_Delete(t *testing.T) {
	q := uniqueQueue(t, "delete")
	runOK(t, "queue", "configure", q)
	runOK(t, "queue", "delete", q, "--yes")
	// describe should now fail (404) - queenctl maps to exit 2.
	_, _, code := run("queue", "describe", q)
	if code == 0 {
		t.Errorf("describe %q after delete should fail, got exit 0", q)
	}
}

func TestQueue_DeleteRequiresYes(t *testing.T) {
	q := uniqueQueue(t, "del-no-yes")
	runOK(t, "queue", "configure", q)
	stdout, stderr, code := run("queue", "delete", q)
	if code == 0 {
		t.Fatalf("expected exit != 0 without --yes, got 0\nstdout: %s\nstderr: %s", stdout, stderr)
	}
	if !strings.Contains(stderr, "--yes") {
		t.Errorf("expected '--yes' in stderr, got %q", stderr)
	}
}

func TestQueue_ConfigureKeepsDistinctValues(t *testing.T) {
	// Mirrors test-v2/queue.js#configureQueue and test_queue.py#test_configure_queue.
	// Each value below is intentionally non-default so we can verify the
	// server actually echoed it back. The configure response (POST
	// /api/v1/configure) carries an "options" block - that's the surface
	// the SDK tests assert against. `queue configure -o json` exposes it.
	q := uniqueQueue(t, "config")
	var resp map[string]any
	runJSON(t, &resp,
		"queue", "configure", q,
		"--lease-time", "60",
		"--retry-limit", "5",
		"--priority", "7",
		"--max-size", "5000",
		"--retention", "0",
		"--completed-retention", "1",
		"--delayed-processing", "1",
		"--window-buffer", "100",
		"--encrypt",
	)
	if resp["configured"] != true {
		t.Fatalf("expected configured=true, got %v\nresponse: %v", resp["configured"], resp)
	}
	options, _ := resp["options"].(map[string]any)
	if options == nil {
		t.Fatalf("response missing options block: %v", resp)
	}
	cases := []struct {
		key  string
		want any
	}{
		{"leaseTime", float64(60)},
		{"retryLimit", float64(5)},
		{"priority", float64(7)},
		{"maxSize", float64(5000)},
		{"completedRetentionSeconds", float64(1)},
		{"delayedProcessing", float64(1)},
		{"windowBuffer", float64(100)},
		{"encryptionEnabled", true},
	}
	for _, c := range cases {
		got, ok := options[c.key]
		if !ok {
			t.Errorf("options.%s missing", c.key)
			continue
		}
		if got != c.want {
			t.Errorf("options.%s = %v (%T), want %v (%T)", c.key, got, got, c.want, c.want)
		}
	}
}

func TestQueue_ConfigureIsIdempotent(t *testing.T) {
	// Same call twice must succeed with no changes - verifies that the
	// `configure` verb is genuinely a kubectl-style apply rather than a
	// strict-create.
	q := uniqueQueue(t, "idempotent")
	runOK(t, "queue", "configure", q, "--lease-time", "30")
	runOK(t, "queue", "configure", q, "--lease-time", "30")
}

func TestQueue_List(t *testing.T) {
	// Create two queues with namespace/task tagging; assert both appear in
	// `queue list`. Mirrors the spirit of test-v2/queue.js but sized to the
	// CLI's surface (the JS suite has no explicit list test but the data is
	// the same).
	q1 := uniqueQueue(t, "list-a")
	q2 := uniqueQueue(t, "list-b")
	runOK(t, "queue", "configure", q1, "--namespace", "qe2e", "--task", "a")
	runOK(t, "queue", "configure", q2, "--namespace", "qe2e", "--task", "b")
	out := runOK(t, "queue", "list", "-o", "json")
	if !strings.Contains(out, q1) || !strings.Contains(out, q2) {
		t.Errorf("queue list missing one of %q / %q\nbody: %s", q1, q2, out)
	}
}

// strOrNested resolves data["a"]["b"]... and returns the leaf as a string,
// or "" if any step is missing or non-string.
func strOrNested(m map[string]any, keys ...string) string {
	v := any(m)
	for _, k := range keys {
		mm, ok := v.(map[string]any)
		if !ok {
			return ""
		}
		v = mm[k]
	}
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

// nestedMap resolves data["a"]["b"]... and returns the final map[string]any
// or nil.
func nestedMap(m map[string]any, keys ...string) map[string]any {
	v := any(m)
	for _, k := range keys {
		mm, ok := v.(map[string]any)
		if !ok {
			return nil
		}
		v = mm[k]
	}
	if mm, ok := v.(map[string]any); ok {
		return mm
	}
	return nil
}

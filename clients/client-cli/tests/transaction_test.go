package tests

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// TestTx_* mirror clients/client-js/test-v2/transaction.js +
// test_transaction.py. queenctl drives transactions via a bundle file:
//
//	queenctl tx -f bundle.json
//
// The bundle is the raw POST /api/v1/transaction body shape:
//
//	{
//	  "operations": [
//	    {"type": "ack",  "transactionId": "...", "partitionId": "..."},
//	    {"type": "push", "items": [{"queue": "x", "payload": {...}}]}
//	  ],
//	  "requiredLeases": ["lease-uuid"]
//	}
//
// Each test pops a message via the CLI to obtain partitionId/leaseId, builds
// the JSON bundle on disk, then commits via `tx -f`.

// TestTx_BasicPushAck mirrors transaction.js#transactionBasicPushAck.
// pop A -> tx{push B, ack A} -> A is empty, B has the transformed payload.
//
// We deliberately omit --cg / consumerGroup throughout to match the JS
// fluent API which uses the implicit queue-mode CG. Mixing named CGs and
// queue-mode pops causes ack/pop CG mismatches: a message acked under
// "ct-tx-basic" stays visible to a pop without --cg (which uses
// __QUEUE_MODE__).
func TestTx_BasicPushAck(t *testing.T) {
	a := uniqueQueue(t, "tx-basic-a")
	b := uniqueQueue(t, "tx-basic-b")
	createQueue(t, a)
	createQueue(t, b)
	pushOne(t, a, "", map[string]any{"value": float64(1)})

	got := popN(t, a, 1, "--timeout", "5s")
	if len(got) != 1 {
		t.Fatalf("setup pop: got %d, want 1", len(got))
	}
	m := got[0]
	bundle := map[string]any{
		"operations": []map[string]any{
			{
				"type":  "push",
				"items": []map[string]any{{"queue": b, "payload": map[string]any{"value": float64(2)}}},
			},
			{
				"type":          "ack",
				"transactionId": m.TransactionID,
				"partitionId":   m.PartitionID,
				"status":        "completed",
			},
		},
		"requiredLeases": []string{m.LeaseID},
	}
	runOK(t, "tx", "-f", writeBundle(t, bundle))

	resB := popN(t, b, 1, "--auto-ack", "--timeout", "5s")
	resA := popN(t, a, 1, "--auto-ack", "--wait=false", "--timeout", "200ms")
	if len(resB) != 1 || resB[0].Data["value"] != float64(2) {
		t.Errorf("queue B: got %d msgs, payload=%v", len(resB), resB)
	}
	if len(resA) != 0 {
		t.Errorf("queue A should be drained, got %d", len(resA))
	}
}

// TestTx_MultiplePushes mirrors transaction.js#transactionMultiplePushes.
// pop A -> tx{push B, push C, ack A} -> B and C have one each.
func TestTx_MultiplePushes(t *testing.T) {
	a := uniqueQueue(t, "tx-multi-a")
	b := uniqueQueue(t, "tx-multi-b")
	c := uniqueQueue(t, "tx-multi-c")
	for _, q := range []string{a, b, c} {
		createQueue(t, q)
	}
	pushOne(t, a, "", map[string]any{"id": "source"})

	got := popN(t, a, 1, "--timeout", "5s")
	if len(got) != 1 {
		t.Fatalf("setup: got %d", len(got))
	}
	m := got[0]
	bundle := map[string]any{
		"operations": []map[string]any{
			{"type": "push", "items": []map[string]any{{"queue": b, "payload": map[string]any{"id": "b"}}}},
			{"type": "push", "items": []map[string]any{{"queue": c, "payload": map[string]any{"id": "c"}}}},
			{
				"type":          "ack",
				"transactionId": m.TransactionID,
				"partitionId":   m.PartitionID,
				"status":        "completed",
			},
		},
		"requiredLeases": []string{m.LeaseID},
	}
	runOK(t, "tx", "-f", writeBundle(t, bundle))

	resB := popN(t, b, 1, "--auto-ack", "--timeout", "5s")
	resC := popN(t, c, 1, "--auto-ack", "--timeout", "5s")
	resA := popN(t, a, 1, "--auto-ack", "--wait=false", "--timeout", "200ms")
	if len(resB) != 1 || resB[0].Data["id"] != "b" {
		t.Errorf("B: %d msgs, payload=%v", len(resB), resB)
	}
	if len(resC) != 1 || resC[0].Data["id"] != "c" {
		t.Errorf("C: %d msgs, payload=%v", len(resC), resC)
	}
	if len(resA) != 0 {
		t.Errorf("A: %d msgs, want 0", len(resA))
	}
}

// TestTx_MultipleAcks mirrors transaction.js#transactionMultipleAcks. pop 3
// from A -> tx{ack a, ack a, ack a, push summary to B}. All acks land
// atomically; A drains; B has the summary.
func TestTx_MultipleAcks(t *testing.T) {
	a := uniqueQueue(t, "tx-multi-ack-a")
	b := uniqueQueue(t, "tx-multi-ack-b")
	createQueue(t, a)
	createQueue(t, b)
	pushNDJSON(t, a, "", []any{
		map[string]any{"value": float64(1)},
		map[string]any{"value": float64(2)},
		map[string]any{"value": float64(3)},
	})
	got := popN(t, a, 3, "--timeout", "5s")
	if len(got) != 3 {
		t.Fatalf("pop returned %d, want 3", len(got))
	}
	sum := float64(0)
	ops := []map[string]any{}
	for _, m := range got {
		if v, ok := m.Data["value"].(float64); ok {
			sum += v
		}
		ops = append(ops, map[string]any{
			"type":          "ack",
			"transactionId": m.TransactionID,
			"partitionId":   m.PartitionID,
			"status":        "completed",
		})
	}
	ops = append(ops, map[string]any{
		"type":  "push",
		"items": []map[string]any{{"queue": b, "payload": map[string]any{"sum": sum}}},
	})
	bundle := map[string]any{
		"operations":     ops,
		"requiredLeases": []string{got[0].LeaseID},
	}
	runOK(t, "tx", "-f", writeBundle(t, bundle))

	resB := popN(t, b, 1, "--auto-ack", "--timeout", "5s")
	if len(resB) != 1 || resB[0].Data["sum"] != float64(6) {
		t.Errorf("B summary: got %d msgs, payload=%v", len(resB), resB)
	}
	resA := popN(t, a, 5, "--auto-ack", "--wait=false", "--timeout", "200ms")
	if len(resA) != 0 {
		t.Errorf("A should be empty, got %d", len(resA))
	}
}

// TestTx_DryRun must NOT commit anything.
func TestTx_DryRun(t *testing.T) {
	a := uniqueQueue(t, "tx-dry-a")
	createQueue(t, a)
	pushOne(t, a, "", map[string]any{"v": 1})
	got := popN(t, a, 1, "--cg", "ct-tx-dry", "--timeout", "5s")
	if len(got) != 1 {
		t.Fatalf("setup pop: got %d", len(got))
	}
	m := got[0]
	bundle := map[string]any{
		"operations": []map[string]any{{
			"type":          "ack",
			"transactionId": m.TransactionID,
			"partitionId":   m.PartitionID,
			"status":        "completed",
			"consumerGroup": "ct-tx-dry",
		}},
	}
	runOK(t, "tx", "-f", writeBundle(t, bundle), "--dry-run")
	// The message should still be visible to a fresh CG; dry-run did not
	// commit any ack against ct-tx-dry, so a different CG also sees it.
	resA := popN(t, a, 1, "--cg", "ct-tx-dry-other", "--timeout", "10s")
	if len(resA) != 1 {
		t.Errorf("dry-run shouldn't have acked; fresh CG got %d msgs", len(resA))
	}
}

// writeBundle dumps the given bundle as JSON into a tempfile and returns
// the path. The file is auto-deleted at end of test.
func writeBundle(t *testing.T, bundle map[string]any) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "bundle.json")
	b, err := json.MarshalIndent(bundle, "", "  ")
	if err != nil {
		t.Fatalf("marshal bundle: %v", err)
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatalf("write bundle: %v", err)
	}
	return path
}

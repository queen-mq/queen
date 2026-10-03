package queen

import (
	"encoding/json"
	"reflect"
	"sort"
	"testing"
)

// The lease on each ack operation of a transaction, against the plan server
// (same harness as wire_capture_test.go).
//
// A Queen 2 broker fences an ack with the `leaseId` the OPERATION carries, and
// lends it one from the top-level `requiredLeases` only when every lease in the
// bundle is the same lease. A bundle that acks messages of two leases with the
// leases in requiredLeases alone is applied unfenced: an ack whose lease has
// expired completes the message under whoever re-leased it, and the commit
// answers success. Nothing but the operation's own key tells the two bodies
// apart, so the body is pinned, including the key that must NOT be there.

func TestTransactionAckCarriesItsOwnLease(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`{"transactionId":"tx","success":true,"results":[
		{"index":0,"type":"ack","success":true},
		{"index":1,"type":"ack","success":true},
		{"index":2,"type":"ack","success":true}
	]}`))
	client := newWireClient(t, srv.URL)

	msgs := []*Message{
		{TransactionID: "tx-1", PartitionID: "p-1", LeaseID: "lease-1"},
		{TransactionID: "tx-2", PartitionID: "p-2", LeaseID: "lease-2"},
		{TransactionID: "tx-3", PartitionID: "p-3"}, // delivered without a lease
	}
	if _, err := client.Transaction().Ack(msgs, AckStatusCompleted, AckOptions{}).Commit(kvCtx(t)); err != nil {
		t.Fatalf("commit: %v", err)
	}

	req := srv.only(t)
	if req.Path != "/api/v1/transaction" {
		t.Fatalf("path = %s", req.Path)
	}
	body, err := decodeAny(req.Body)
	if err != nil {
		t.Fatalf("body: %v", err)
	}
	root := body.(map[string]interface{})

	ops, _ := root["operations"].([]interface{})
	if len(ops) != 3 {
		t.Fatalf("want 3 operations, got %s", string(req.Body))
	}
	for i, want := range []string{"lease-1", "lease-2", ""} {
		got, present := ops[i].(map[string]interface{})["leaseId"]
		switch {
		case want == "" && present:
			t.Errorf("operations[%d].leaseId = %v; the ack of a message without a lease must not carry the key", i, got)
		case want != "" && got != want:
			t.Errorf("operations[%d].leaseId = %v, want %q", i, got, want)
		}
	}

	// requiredLeases is built from a map: its order is not part of the contract,
	// its contents are.
	leases := sortedRequiredLeases(t, root)
	if want := []string{"lease-1", "lease-2"}; !reflect.DeepEqual(leases, want) {
		t.Errorf("requiredLeases = %v, want the set %v", leases, want)
	}

	// The whole body, with requiredLeases sorted so it can be pinned.
	root["requiredLeases"] = leases
	normalized, _ := json.Marshal(root)
	assertJSONBody(t, normalized, `{
		"operations":[
			{"type":"ack","transactionId":"tx-1","partitionId":"p-1","leaseId":"lease-1","status":"completed"},
			{"type":"ack","transactionId":"tx-2","partitionId":"p-2","leaseId":"lease-2","status":"completed"},
			{"type":"ack","transactionId":"tx-3","partitionId":"p-3","status":"completed"}
		],
		"requiredLeases":["lease-1","lease-2"]
	}`)
}

func sortedRequiredLeases(t *testing.T, root map[string]interface{}) []string {
	t.Helper()
	raw, ok := root["requiredLeases"].([]interface{})
	if !ok {
		t.Fatalf("requiredLeases must be an array, got %#v", root["requiredLeases"])
	}
	out := make([]string, 0, len(raw))
	for _, v := range raw {
		s, _ := v.(string)
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

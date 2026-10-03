package cmd

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"testing"

	clierr "github.com/smartpricing/queen/clients/client-cli/v2/internal/errors"
)

// What `queenctl tx` puts on the wire for the leases of its acks.
//
// A Queen 2 broker fences each ack with the leaseId its operation carries, and
// lends it the lease in requiredLeases only when the bundle names one. The
// command used to build every ack from transactionId and partitionId alone and
// drop requiredLeases, so a bundle file's leases never reached the broker and
// every ack it sent was applied unfenced, on 1.x brokers as on 2.0.

// captureTransaction answers every POST /api/v1/transaction with success and
// records the bodies it was sent.
func captureTransaction(t *testing.T) (*httptest.Server, *[]map[string]any) {
	t.Helper()
	var bodies []map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/transaction" {
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("transaction body is not JSON: %v", err)
			}
			bodies = append(bodies, body)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"success":true,"transactionId":"t","results":[]}`))
	}))
	t.Cleanup(srv.Close)
	return srv, &bodies
}

// runTx writes bundle to a file and runs `queenctl tx -f` on it.
func runTx(t *testing.T, server, bundle string) error {
	t.Helper()
	path := filepath.Join(t.TempDir(), "bundle.json")
	if err := os.WriteFile(path, []byte(bundle), 0o600); err != nil {
		t.Fatalf("write bundle: %v", err)
	}
	return runQueenctl(t, server, "tx", "-f", path)
}

// sentOps is the one transaction that was sent: its operations, and its
// requiredLeases sorted.
func sentOps(t *testing.T, bodies *[]map[string]any) ([]map[string]any, []string) {
	t.Helper()
	if len(*bodies) != 1 {
		t.Fatalf("expected exactly one transaction, got %d: %+v", len(*bodies), *bodies)
	}
	var ops []map[string]any
	for _, op := range (*bodies)[0]["operations"].([]any) {
		ops = append(ops, op.(map[string]any))
	}
	var leases []string
	if raw, ok := (*bodies)[0]["requiredLeases"].([]any); ok {
		for _, l := range raw {
			leases = append(leases, l.(string))
		}
	}
	sort.Strings(leases)
	return ops, leases
}

func TestTxSendsEachAckItsOwnLease(t *testing.T) {
	srv, bodies := captureTransaction(t)
	err := runTx(t, srv.URL, `{
		"operations": [
			{"type":"ack","transactionId":"t1","partitionId":"p1","status":"completed","leaseId":"lease-1"},
			{"type":"ack","transactionId":"t2","partitionId":"p2","status":"completed","leaseId":"lease-2"}
		],
		"requiredLeases": ["lease-1","lease-2"]
	}`)
	if err != nil {
		t.Fatalf("tx: %v", err)
	}
	ops, leases := sentOps(t, bodies)
	if got := ops[0]["leaseId"]; got != "lease-1" {
		t.Errorf("first ack leaseId = %v, want lease-1", got)
	}
	if got := ops[1]["leaseId"]; got != "lease-2" {
		t.Errorf("second ack leaseId = %v, want lease-2", got)
	}
	if len(leases) != 2 || leases[0] != "lease-1" || leases[1] != "lease-2" {
		t.Errorf("requiredLeases = %v, want [lease-1 lease-2]", leases)
	}
}

func TestTxLendsTheOneLeaseRequiredLeasesNames(t *testing.T) {
	srv, bodies := captureTransaction(t)
	err := runTx(t, srv.URL, `{
		"operations": [
			{"type":"ack","transactionId":"t1","partitionId":"p1","status":"completed"},
			{"type":"ack","transactionId":"t2","partitionId":"p1","status":"completed"},
			{"type":"push","items":[{"queue":"orders","payload":{"id":1}}]}
		],
		"requiredLeases": ["lease-1","lease-1"]
	}`)
	if err != nil {
		t.Fatalf("tx: %v", err)
	}
	ops, leases := sentOps(t, bodies)
	for i, op := range ops {
		switch op["type"] {
		case "ack":
			if got := op["leaseId"]; got != "lease-1" {
				t.Errorf("ack %d leaseId = %v, want lease-1", i, got)
			}
		default:
			if _, ok := op["leaseId"]; ok {
				t.Errorf("%v operation %d carries a leaseId: %v", op["type"], i, op)
			}
		}
	}
	if len(leases) != 1 || leases[0] != "lease-1" {
		t.Errorf("requiredLeases = %v, want [lease-1]", leases)
	}
}

func TestTxRefusesALeaselessAckAmongSeveralLeases(t *testing.T) {
	srv, bodies := captureTransaction(t)
	err := runTx(t, srv.URL, `{
		"operations": [
			{"type":"ack","transactionId":"t1","partitionId":"p1","status":"completed","leaseId":"lease-1"},
			{"type":"ack","transactionId":"t2","partitionId":"p2","status":"completed"}
		],
		"requiredLeases": ["lease-1","lease-2"]
	}`)
	if err == nil {
		t.Fatal("a lease-less ack in a bundle naming two leases must be refused")
	}
	if got := clierr.CodeOf(err); got != clierr.CodeUser {
		t.Errorf("refusal should exit %d, got %d: %v", clierr.CodeUser, got, err)
	}
	if len(*bodies) != 0 {
		t.Errorf("nothing may be sent, got %d transaction(s)", len(*bodies))
	}
}

func TestTxWithoutAnyLeaseAcksWithoutOne(t *testing.T) {
	srv, bodies := captureTransaction(t)
	err := runTx(t, srv.URL, `{
		"operations": [
			{"type":"ack","transactionId":"t1","partitionId":"p1","status":"completed"}
		]
	}`)
	if err != nil {
		t.Fatalf("tx: %v", err)
	}
	ops, leases := sentOps(t, bodies)
	if _, ok := ops[0]["leaseId"]; ok {
		t.Errorf("an ack of a bundle that names no lease carries one: %v", ops[0])
	}
	if len(leases) != 0 {
		t.Errorf("requiredLeases = %v, want none", leases)
	}
}

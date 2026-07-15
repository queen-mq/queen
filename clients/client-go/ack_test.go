package queen

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// The server (routes/ack.cpp + queen.ack_messages_v2) responds to both
// /api/v1/ack and /api/v1/ack/batch with a top-level JSON array, one item per
// acknowledgment in request order:
//
//	[{"index":0,"transactionId":"...","success":false,"error":"Invalid or expired lease",
//	  "queueName":"q","partitionName":"Default","leaseReleased":false,"dlq":false}]
//
// HttpClient wraps top-level arrays as {"data": [...]}. These tests pin that
// wire format so a rejected ack/nack is never reported as Success=true.

func newAckTestServer(t *testing.T, wantPath string, respond func(body map[string]interface{}) interface{}) *Queen {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != wantPath {
			t.Errorf("unexpected request path %q, want %q", r.URL.Path, wantPath)
			http.NotFound(w, r)
			return
		}
		var body map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("failed to decode request body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(respond(body)); err != nil {
			t.Errorf("failed to encode response: %v", err)
		}
	}))
	t.Cleanup(server.Close)

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	t.Cleanup(func() { client.Close(context.Background()) })
	return client
}

func ackResultItem(index int, txnID string, success bool, errMsg interface{}) map[string]interface{} {
	return map[string]interface{}{
		"index":         index,
		"transactionId": txnID,
		"success":       success,
		"error":         errMsg,
		"queueName":     "test-queue",
		"partitionName": "Default",
		"leaseReleased": success,
		"dlq":           false,
	}
}

func TestAckSingleServerResponse(t *testing.T) {
	cases := []struct {
		name        string
		ackSuccess  bool // the status the client sends (true=completed, false=failed)
		serverItem  map[string]interface{}
		wantSuccess bool
		wantError   string
	}{
		{
			name:        "accepted ack",
			ackSuccess:  true,
			serverItem:  ackResultItem(0, "tx-1", true, nil),
			wantSuccess: true,
		},
		{
			name:        "ack rejected for expired lease",
			ackSuccess:  true,
			serverItem:  ackResultItem(0, "tx-1", false, "Invalid or expired lease"),
			wantSuccess: false,
			wantError:   "Invalid or expired lease",
		},
		{
			name:        "nack rejected for missing message",
			ackSuccess:  false,
			serverItem:  ackResultItem(0, "tx-1", false, "Message not found"),
			wantSuccess: false,
			wantError:   "Message not found",
		},
		{
			name:        "rejected without error string still fails",
			ackSuccess:  true,
			serverItem:  ackResultItem(0, "tx-1", false, nil),
			wantSuccess: false,
			wantError:   "acknowledgment rejected by server",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wantStatus := AckStatusCompleted
			if !tc.ackSuccess {
				wantStatus = AckStatusFailed
			}
			client := newAckTestServer(t, "/api/v1/ack", func(body map[string]interface{}) interface{} {
				if got := body["status"]; got != wantStatus {
					t.Errorf("request status = %v, want %q", got, wantStatus)
				}
				return []interface{}{tc.serverItem}
			})

			msg := &Message{TransactionID: "tx-1", PartitionID: "p-1", LeaseID: "lease-1"}
			responses, err := client.Ack(context.Background(), msg, tc.ackSuccess, AckOptions{})
			if err != nil {
				t.Fatalf("Ack returned error: %v", err)
			}
			if len(responses) != 1 {
				t.Fatalf("got %d responses, want 1", len(responses))
			}
			if responses[0].Success != tc.wantSuccess {
				t.Errorf("Success = %v, want %v", responses[0].Success, tc.wantSuccess)
			}
			if responses[0].Error != tc.wantError {
				t.Errorf("Error = %q, want %q", responses[0].Error, tc.wantError)
			}
		})
	}
}

func TestAckBatchMixedResults(t *testing.T) {
	client := newAckTestServer(t, "/api/v1/ack/batch", func(body map[string]interface{}) interface{} {
		acks, _ := body["acknowledgments"].([]interface{})
		if len(acks) != 2 {
			t.Errorf("request has %d acknowledgments, want 2", len(acks))
		}
		return []interface{}{
			ackResultItem(0, "tx-1", true, nil),
			ackResultItem(1, "tx-2", false, "Invalid or expired lease"),
		}
	})

	msgs := []*Message{
		{TransactionID: "tx-1", PartitionID: "p-1", LeaseID: "lease-1"},
		{TransactionID: "tx-2", PartitionID: "p-1", LeaseID: "lease-1"},
	}
	responses, err := client.Ack(context.Background(), msgs, true, AckOptions{})
	if err != nil {
		t.Fatalf("Ack returned error: %v", err)
	}
	if len(responses) != 2 {
		t.Fatalf("got %d responses, want 2", len(responses))
	}
	if !responses[0].Success || responses[0].Error != "" {
		t.Errorf("responses[0] = %+v, want success", responses[0])
	}
	if responses[1].Success || responses[1].Error != "Invalid or expired lease" {
		t.Errorf("responses[1] = %+v, want rejected with lease error", responses[1])
	}
}

func TestAckBatchCountMismatch(t *testing.T) {
	client := newAckTestServer(t, "/api/v1/ack/batch", func(body map[string]interface{}) interface{} {
		return []interface{}{ackResultItem(0, "tx-1", true, nil)}
	})

	msgs := []*Message{
		{TransactionID: "tx-1", PartitionID: "p-1", LeaseID: "lease-1"},
		{TransactionID: "tx-2", PartitionID: "p-1", LeaseID: "lease-1"},
	}
	if _, err := client.Ack(context.Background(), msgs, true, AckOptions{}); err == nil {
		t.Fatal("Ack should fail when the server returns fewer results than messages sent")
	}
}

func TestAckErrorEnvelope(t *testing.T) {
	client := newAckTestServer(t, "/api/v1/ack", func(body map[string]interface{}) interface{} {
		return map[string]interface{}{"error": "internal error"}
	})

	msg := &Message{TransactionID: "tx-1", PartitionID: "p-1", LeaseID: "lease-1"}
	responses, err := client.Ack(context.Background(), msg, true, AckOptions{})
	if err != nil {
		t.Fatalf("Ack returned error: %v", err)
	}
	if len(responses) != 1 || responses[0].Success {
		t.Fatalf("responses = %+v, want single rejected response", responses)
	}
	if responses[0].Error != "internal error" {
		t.Errorf("Error = %q, want %q", responses[0].Error, "internal error")
	}
}

func TestAckUnexpectedResponseFormat(t *testing.T) {
	client := newAckTestServer(t, "/api/v1/ack", func(body map[string]interface{}) interface{} {
		return map[string]interface{}{"ok": true}
	})

	msg := &Message{TransactionID: "tx-1", PartitionID: "p-1", LeaseID: "lease-1"}
	if _, err := client.Ack(context.Background(), msg, true, AckOptions{}); err == nil {
		t.Fatal("Ack should fail on a response that is neither a result array nor an error envelope")
	}
}

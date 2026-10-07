package queen

import (
	"context"
	"testing"
	"time"
)

// POST /api/v1/lease/:leaseId/extend answers HTTP 200 whether or not the broker
// extended anything (server/src/rsm/facade/real.rs, renew_impl):
//
//	{"leaseId":"...","success":true,"renewed":1,"newExpiresAt":"2026-10-06T12:33:25.424Z",...}
//	{"leaseId":"...","success":false,"renewed":0,"newExpiresAt":null,...}
//
// The second is a lease that expired, was released by an ack or nack, or never
// existed. These tests pin that a renewal the broker refused is never reported
// as Success=true.
func TestRenewServerResponse(t *testing.T) {
	cases := []struct {
		name        string
		body        map[string]interface{}
		wantSuccess bool
		wantExpiry  string
	}{
		{
			name: "renewed",
			body: map[string]interface{}{
				"leaseId": "lease-1", "success": true, "renewed": 1,
				"newExpiresAt": "2026-10-06T12:33:25.424Z", "expiresAt": "2026-10-06T12:33:25.424Z",
				"lease_expires_at": "2026-10-06T12:33:25.424Z",
			},
			wantSuccess: true,
			wantExpiry:  "2026-10-06T12:33:25.424Z",
		},
		{
			name: "lease gone",
			body: map[string]interface{}{
				"leaseId": "lease-1", "success": false, "renewed": 0,
				"newExpiresAt": nil, "expiresAt": nil, "lease_expires_at": nil,
			},
			wantSuccess: false,
		},
		{
			name:        "body without success",
			body:        map[string]interface{}{"leaseId": "lease-1"},
			wantSuccess: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client := newAckTestServer(t, "/api/v1/lease/lease-1/extend", func(map[string]interface{}) interface{} {
				return tc.body
			})

			responses, err := client.Renew(context.Background(), &Message{LeaseID: "lease-1"})
			if err != nil {
				t.Fatalf("Renew returned error: %v", err)
			}
			if len(responses) != 1 {
				t.Fatalf("got %d responses, want 1", len(responses))
			}
			got := responses[0]
			if got.LeaseID != "lease-1" {
				t.Errorf("LeaseID = %q, want %q", got.LeaseID, "lease-1")
			}
			if got.Success != tc.wantSuccess {
				t.Errorf("Success = %v, want %v", got.Success, tc.wantSuccess)
			}
			if !tc.wantSuccess && got.Error == "" {
				t.Errorf("Error is empty for a renewal the broker refused")
			}
			if tc.wantExpiry != "" {
				want, _ := time.Parse(time.RFC3339, tc.wantExpiry)
				if !got.NewExpiresAt.Equal(want) {
					t.Errorf("NewExpiresAt = %v, want %v", got.NewExpiresAt, want)
				}
			} else if !got.NewExpiresAt.IsZero() {
				t.Errorf("NewExpiresAt = %v, want zero", got.NewExpiresAt)
			}
		})
	}
}

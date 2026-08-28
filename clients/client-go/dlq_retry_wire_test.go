package queen

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
)

// Admin.RetryMessage against the plan server (same harness as
// wire_capture_test.go). Two things are under test and they are different
// kinds of thing: the request SHAPE, and the number of times it is sent.
//
// The count is the load-bearing one. The broker's retry route is not
// idempotent -- see the notes on Admin.RetryMessage -- and its worst failure
// mode is the one that looks most retryable from here: the replay push is
// accepted, the DLQ cleanup then fails, and the broker answers 500 with
// {"replayed":true,"dlqRowRemoved":false}. The row is still dead-lettered, so
// a resent POST replays the message a SECOND time. With the default
// RetryAttempts=3 that is one operator command producing four copies.

// newRetryingWireClient builds a client with the DEFAULT retry budget
// (RetryAttempts=3, so four attempts) and a 1ms backoff. The point of the
// default is that TestRetryMessage_NotRetriedOnServerError proves the
// suppression on a client that would otherwise retry, rather than on one
// configured not to.
func newRetryingWireClient(t *testing.T, url string) *Queen {
	t.Helper()
	client, err := New(ClientConfig{
		URL:              url,
		TimeoutMillis:    2000,
		RetryDelayMillis: 1,
	})
	if err != nil {
		t.Fatalf("failed to build client: %v", err)
	}
	t.Cleanup(func() { client.httpClient.Close() })
	return client
}

func TestRetryMessage_WireShape(t *testing.T) {
	cs := newCaptureServer(t, okJSON(`{"success":true,"queue":"q","partition":"Default"}`))
	client := newWireClient(t, cs.URL)

	if _, err := client.Admin().RetryMessage(context.Background(),
		"3f2504e0-4f89-11d3-9a0c-0305e82c3301", "tx-1"); err != nil {
		t.Fatalf("RetryMessage: %v", err)
	}

	req := cs.only(t)
	if req.Method != http.MethodPost {
		t.Errorf("method = %s, want POST", req.Method)
	}
	want := "/api/v1/messages/3f2504e0-4f89-11d3-9a0c-0305e82c3301/tx-1/retry"
	if req.Path != want {
		t.Errorf("path = %s, want %s", req.Path, want)
	}
	// The other SDKs post an empty object, not an empty body; the broker
	// ignores it, but a nil body would make this the only POST in the client
	// without one.
	assertJSONBody(t, req.Body, `{}`)
}

// A transaction id is caller-supplied text, not a uuid: 005_log_ack stores it
// as TEXT. One containing a slash must not be able to address a different
// route.
func TestRetryMessage_EscapesPathSegments(t *testing.T) {
	cs := newCaptureServer(t, okJSON(`{"success":true}`))
	client := newWireClient(t, cs.URL)

	if _, err := client.Admin().RetryMessage(context.Background(),
		"pid/../x", "tx with space/and-slash"); err != nil {
		t.Fatalf("RetryMessage: %v", err)
	}

	req := cs.only(t)
	if req.RawPath != "/api/v1/messages/pid%2F..%2Fx/tx%20with%20space%2Fand-slash/retry" {
		t.Errorf("raw path = %s, want the segments percent-escaped", req.RawPath)
	}
}

// The guarantee: exactly one attempt, even on the 500 that says the replay
// already happened.
func TestRetryMessage_NotRetriedOnServerError(t *testing.T) {
	body := `{"success":false,"replayed":true,"dlqRowRemoved":false,"error":"dlq cleanup failed"}`
	cs := newCaptureServer(t,
		cannedResponse{status: http.StatusInternalServerError, body: body},
		cannedResponse{status: http.StatusInternalServerError, body: body},
		cannedResponse{status: http.StatusInternalServerError, body: body},
		cannedResponse{status: http.StatusInternalServerError, body: body},
	)
	client := newRetryingWireClient(t, cs.URL)

	_, err := client.Admin().RetryMessage(context.Background(), "pid", "tx")
	if err == nil {
		t.Fatal("RetryMessage returned nil error on a 500")
	}
	// queenctl reads the body off this error to warn that the message was
	// replayed but not de-DLQ'd, so the HTTPError has to survive the wrapping
	// doRequestRaw applies on its way out.
	var he *HTTPError
	if !errors.As(err, &he) {
		t.Fatalf("error does not unwrap to *HTTPError: %v", err)
	}
	if he.StatusCode != http.StatusInternalServerError || !strings.Contains(he.Body, `"replayed":true`) {
		t.Errorf("HTTPError lost the broker's body: %d %s", he.StatusCode, he.Body)
	}

	if n := len(cs.requests()); n != 1 {
		t.Fatalf("RetryMessage sent %d requests on a 500, want exactly 1: each "+
			"resend replays the dead-lettered message again", n)
	}
}

// The control for the test above: the same client, the same 500s, a request
// that did NOT opt out. Four attempts, so the assertion of 1 above is
// measuring WithoutFailoverRetry and not some unrelated change of default.
func TestDeleteMessage_StillRetriesOnServerError(t *testing.T) {
	cs := newCaptureServer(t,
		cannedResponse{status: http.StatusInternalServerError, body: `{"error":"boom"}`},
		cannedResponse{status: http.StatusInternalServerError, body: `{"error":"boom"}`},
		cannedResponse{status: http.StatusInternalServerError, body: `{"error":"boom"}`},
		cannedResponse{status: http.StatusInternalServerError, body: `{"error":"boom"}`},
	)
	client := newRetryingWireClient(t, cs.URL)

	if _, err := client.Admin().DeleteMessage(context.Background(), "pid", "tx"); err == nil {
		t.Fatal("DeleteMessage returned nil error on a 500")
	}

	if n := len(cs.requests()); n != 4 {
		t.Fatalf("DeleteMessage sent %d requests, want 4 (RetryAttempts=3)", n)
	}
}

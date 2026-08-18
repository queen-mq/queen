package queen

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
)

// The broker-free half of the kv/timers suite (PLAN_KV_TIMERS.md §10.2, the
// client-go row: "integrazione piu' unit contro plan server").
//
// WHAT THESE TESTS ARE FOR, and why they assert the body byte for byte rather
// than "it worked". The request body IS the contract towards the broker, and it
// is the only thing that catches a wrong wire shape BEFORE production: a
// transaction whose `kv` array landed one level too deep still gets a 200, still
// commits its pushes, and simply has no gate (§10.4). Nothing downstream of the
// serializer notices. So every op below is pinned as JSON, including the fields
// that must NOT be there.
//
// The harness is deliberately the same shape as retry429_test.go's plan server
// (an httptest.Server serving canned responses in order) with one addition it
// did not need: the request BODY is captured, because here the body is the thing
// under test.

// cannedResponse is one scripted reply from the plan server below.
type cannedResponse struct {
	status int
	body   string
	header map[string]string
}

func okJSON(body string) cannedResponse {
	return cannedResponse{status: http.StatusOK, body: body}
}

// capturedRequest is everything about one inbound request that a wire contract
// can be wrong about.
type capturedRequest struct {
	Method   string
	Path     string
	RawPath  string
	RawQuery string
	Body     []byte
	Header   http.Header
}

type captureServer struct {
	URL string

	mu        sync.Mutex
	hits      []capturedRequest
	responses []cannedResponse
	fallback  cannedResponse
	idx       int
}

// newCaptureServer starts a server that answers `responses` in request order and
// falls back to a 200 `{}` once they are exhausted. Every request is recorded.
func newCaptureServer(t *testing.T, responses ...cannedResponse) *captureServer {
	t.Helper()
	cs := &captureServer{
		responses: responses,
		fallback:  okJSON(`{}`),
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := readAll(r)
		cs.mu.Lock()
		resp := cs.fallback
		if cs.idx < len(cs.responses) {
			resp = cs.responses[cs.idx]
		}
		cs.idx++
		cs.hits = append(cs.hits, capturedRequest{
			Method:   r.Method,
			Path:     r.URL.Path,
			RawPath:  r.URL.EscapedPath(),
			RawQuery: r.URL.RawQuery,
			Body:     body,
			Header:   r.Header.Clone(),
		})
		cs.mu.Unlock()

		for k, v := range resp.header {
			w.Header().Set(k, v)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(resp.status)
		_, _ = w.Write([]byte(resp.body))
	}))
	t.Cleanup(srv.Close)
	cs.URL = srv.URL
	return cs
}

func readAll(r *http.Request) []byte {
	if r.Body == nil {
		return nil
	}
	defer r.Body.Close()
	b, _ := io.ReadAll(r.Body)
	return b
}

func (cs *captureServer) requests() []capturedRequest {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	out := make([]capturedRequest, len(cs.hits))
	copy(out, cs.hits)
	return out
}

// only returns the single request the server saw, failing when there was not
// exactly one. A test that asserts a body must first prove there was one call:
// "no request at all" and "the right request" are otherwise indistinguishable.
func (cs *captureServer) only(t *testing.T) capturedRequest {
	t.Helper()
	reqs := cs.requests()
	if len(reqs) != 1 {
		t.Fatalf("expected exactly 1 request, got %d: %+v", len(reqs), reqs)
	}
	return reqs[0]
}

// newWireClient builds a client pointed at the plan server with retries off, so
// a test that asserts one request gets one request and an error test does not
// wait out three backoffs.
func newWireClient(t *testing.T, url string) *Queen {
	t.Helper()
	client, err := New(ClientConfig{
		URL:           url,
		TimeoutMillis: 2000,
		RetryAttempts: -1, // negative = exactly one attempt (see doRequest)
	})
	if err != nil {
		t.Fatalf("failed to build client: %v", err)
	}
	t.Cleanup(func() { client.httpClient.Close() })
	return client
}

// assertJSONBody compares a captured body against the expected JSON, exactly:
// same keys, no extra keys, same values. Both sides are decoded with
// UseNumber(), so 9007199254740993 does not silently become 9007199254740992 in
// the ASSERTION while the code under test got it right.
func assertJSONBody(t *testing.T, got []byte, want string) {
	t.Helper()
	gv, err := decodeAny(got)
	if err != nil {
		t.Fatalf("captured body is not JSON: %v\nbody: %s", err, string(got))
	}
	wv, err := decodeAny([]byte(want))
	if err != nil {
		t.Fatalf("expected body is not JSON: %v\nbody: %s", err, want)
	}
	if !reflect.DeepEqual(gv, wv) {
		t.Fatalf("wire body mismatch\n got: %s\nwant: %s", string(got), want)
	}
}

func decodeAny(b []byte) (interface{}, error) {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var v interface{}
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return v, nil
}

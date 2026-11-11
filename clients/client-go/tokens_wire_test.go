package queen

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/smartpricing/queen/clients/client-go/v2/streams/runtime"
)

// Client tokens on the Go client: ClientConfig.TokenSource refreshed before a
// client token's exp and after a 401 (the request sent once more), a fixed
// BearerToken never retried on one, one source shared by every HTTP client of
// a Queen, and Tokens().Mint on the wire. Against the capture server of
// wire_capture_test.go: no broker.

// clientToken is a client token whose exp is secondsFromNow away. The
// signature is fake: the client never verifies one.
func clientToken(secondsFromNow int64, sub string) string {
	enc := func(v interface{}) string {
		b, _ := json.Marshal(v)
		return base64.RawURLEncoding.EncodeToString(b)
	}
	exp := time.Now().Unix() + secondsFromNow
	return ClientTokenPrefix + enc(map[string]interface{}{"alg": "HS256"}) + "." +
		enc(map[string]interface{}{"sub": sub, "exp": exp, "typ": "client"}) + ".sig"
}

func tokenExpired() cannedResponse {
	return cannedResponse{status: http.StatusUnauthorized, body: `{"error":"client token expired","code":"token_expired"}`}
}

// countingSource answers token-0, token-1, ... and counts its calls.
func countingSource() (func(context.Context) (string, error), *atomic.Int32) {
	calls := &atomic.Int32{}
	return func(context.Context) (string, error) {
		return fmt.Sprintf("token-%d", calls.Add(1)-1), nil
	}, calls
}

// newTokenClient is newWireClient with a token source. Retries are off, so a
// test that sees two requests sees the refresh's retry and nothing else: that
// retry is no attempt of the client's.
func newTokenClient(t *testing.T, url string, source func(context.Context) (string, error)) *Queen {
	t.Helper()
	client, err := New(ClientConfig{URL: url, TimeoutMillis: 2000, RetryAttempts: -1, TokenSource: source})
	if err != nil {
		t.Fatalf("failed to build client: %v", err)
	}
	t.Cleanup(func() { client.httpClient.Close() })
	return client
}

func authOf(r capturedRequest) string { return r.Header.Get("Authorization") }

// getConcurrently sends n requests at once and fails on any error. The Admin
// handle is taken once, before: Queen.Admin makes it on first use with no
// lock, so n goroutines asking for it at once would race on that, not on the
// token source under test.
func getConcurrently(t *testing.T, client *Queen, n int) {
	t.Helper()
	admin := client.Admin()
	errs := make(chan error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := admin.GetOverview(context.Background())
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("GetOverview: %v", err)
		}
	}
}

func TestTokenSourceRefreshesAfter401AndSendsTheRequestAgain(t *testing.T) {
	srv := newCaptureServer(t, tokenExpired(), okJSON(`{}`))
	source, calls := countingSource()
	client := newTokenClient(t, srv.URL, source)

	if _, err := client.Admin().GetOverview(context.Background()); err != nil {
		t.Fatalf("GetOverview: %v", err)
	}
	reqs := srv.requests()
	if len(reqs) != 2 {
		t.Fatalf("requests = %d, want 2 (the 401, then the retry)", len(reqs))
	}
	if got := authOf(reqs[0]); got != "Bearer token-0" {
		t.Errorf("first request Authorization = %q, want Bearer token-0", got)
	}
	if got := authOf(reqs[1]); got != "Bearer token-1" {
		t.Errorf("retry Authorization = %q, want Bearer token-1: the retry carries the new token", got)
	}
	if n := calls.Load(); n != 2 {
		t.Errorf("source called %d times, want 2", n)
	}

	// The new token is kept: the next request fetches nothing.
	if _, err := client.Admin().GetOverview(context.Background()); err != nil {
		t.Fatalf("GetOverview: %v", err)
	}
	if n := calls.Load(); n != 2 {
		t.Errorf("source called %d times after a third request, want still 2", n)
	}
	if got := authOf(srv.requests()[2]); got != "Bearer token-1" {
		t.Errorf("third request Authorization = %q, want Bearer token-1", got)
	}
}

func TestTokenSourceGivesASecond401ToTheCaller(t *testing.T) {
	srv := newCaptureServer(t, tokenExpired(), tokenExpired(), tokenExpired())
	source, calls := countingSource()
	client := newTokenClient(t, srv.URL, source)

	_, err := client.Admin().GetOverview(context.Background())
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.StatusCode != http.StatusUnauthorized || httpErr.Code != "token_expired" {
		t.Fatalf("err = %v, want the 401 token_expired", err)
	}
	if n := len(srv.requests()); n != 2 {
		t.Errorf("requests = %d, want 2: one refresh, one retry", n)
	}
	if n := calls.Load(); n != 2 {
		t.Errorf("source called %d times, want 2", n)
	}
}

func TestFixedBearerTokenIsNotRetriedOn401(t *testing.T) {
	srv := newCaptureServer(t, tokenExpired())
	// The default retries: a 401 is a 4xx, which they never retry.
	client, err := New(ClientConfig{URL: srv.URL, TimeoutMillis: 2000, BearerToken: "qk_live_static"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { client.httpClient.Close() })

	_, err = client.Admin().GetOverview(context.Background())
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.StatusCode != http.StatusUnauthorized {
		t.Fatalf("err = %v, want the 401", err)
	}
	if got := authOf(srv.only(t)); got != "Bearer qk_live_static" {
		t.Errorf("Authorization = %q, want Bearer qk_live_static", got)
	}
}

func TestTokenSourceThatGivesNoTokenSendsNothing(t *testing.T) {
	backendDown := errors.New("backend down")
	for _, tc := range []struct {
		name   string
		answer func() (string, error)
		want   string
		is     error // also in the chain, when set
	}{
		{"an error", func() (string, error) { return "", backendDown }, "backend down", backendDown},
		{"an empty token", func() (string, error) { return "  ", nil }, "empty token", nil},
		{"a token no header can carry", func() (string, error) { return "qt_a\nb", nil }, "cannot be sent as a header", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := newCaptureServer(t)
			var calls atomic.Int32
			source := func(context.Context) (string, error) {
				calls.Add(1)
				return tc.answer()
			}
			// The default retries: a source that fails is the request's error,
			// not a network error to try again.
			client, err := New(ClientConfig{URL: srv.URL, TimeoutMillis: 2000, TokenSource: source})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			t.Cleanup(func() { client.httpClient.Close() })

			_, err = client.Admin().GetOverview(context.Background())
			if !errors.Is(err, ErrTokenSource) || !strings.Contains(err.Error(), "token source") || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want an ErrTokenSource saying %q", err, tc.want)
			}
			if tc.is != nil && !errors.Is(err, tc.is) {
				t.Errorf("err = %v, want the source's own error in the chain", err)
			}
			if n := len(srv.requests()); n != 0 {
				t.Errorf("requests = %d, want 0: nothing is sent without a token", n)
			}
			if n := calls.Load(); n != 1 {
				t.Errorf("source called %d times, want 1", n)
			}
		})
	}
}

func TestBearerTokenAndTokenSourceTogetherAreRefused(t *testing.T) {
	source, calls := countingSource()
	_, err := New(ClientConfig{URL: "http://127.0.0.1:1", BearerToken: "qk_live_x", TokenSource: source})
	if err == nil || !strings.Contains(err.Error(), "BearerToken and TokenSource") {
		t.Fatalf("err = %v, want the configuration error naming both", err)
	}
	if n := calls.Load(); n != 0 {
		t.Errorf("source called %d times, want 0", n)
	}
}

func TestTokenSourceIsCalledOnceAcrossRequests(t *testing.T) {
	srv := newCaptureServer(t)
	var calls atomic.Int32
	token := clientToken(3600, "dev1")
	source := func(context.Context) (string, error) {
		calls.Add(1)
		// Long enough for the concurrent requests below to queue behind it.
		time.Sleep(20 * time.Millisecond)
		return token, nil
	}
	client := newTokenClient(t, srv.URL, source)

	const concurrent = 8
	getConcurrently(t, client, concurrent)
	for i := 0; i < 3; i++ {
		if _, err := client.Admin().GetOverview(context.Background()); err != nil {
			t.Fatalf("GetOverview: %v", err)
		}
	}

	if n := calls.Load(); n != 1 {
		t.Errorf("source called %d times, want 1: a token with an hour left is kept", n)
	}
	reqs := srv.requests()
	if len(reqs) != concurrent+3 {
		t.Fatalf("requests = %d, want %d", len(reqs), concurrent+3)
	}
	for i, r := range reqs {
		if got := authOf(r); got != "Bearer "+token {
			t.Errorf("request %d Authorization = %q, want the one token", i, got)
		}
	}
}

func TestTokenSourceRefreshesOnceFor401sMetAtOnce(t *testing.T) {
	// The server refuses token-0 and takes any other: every request sent before
	// the refresh is refused with the same token, at about the same time.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Header.Get("Authorization") == "Bearer token-0" {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"client token expired","code":"token_expired"}`))
			return
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(srv.Close)
	var calls atomic.Int32
	source := func(context.Context) (string, error) {
		n := calls.Add(1) - 1
		time.Sleep(10 * time.Millisecond)
		return fmt.Sprintf("token-%d", n), nil
	}
	client := newTokenClient(t, srv.URL, source)

	getConcurrently(t, client, 8)
	if n := calls.Load(); n != 2 {
		t.Errorf("source called %d times, want 2: the first token, then one refresh for every 401", n)
	}
}

func TestTokenSourceFetchesAgainNearItsExp(t *testing.T) {
	for _, tc := range []struct {
		name      string
		left      int64 // seconds from the token's mint to its exp
		wantCalls int32
	}{
		{"an hour left: kept", 3600, 1},
		{"10 s left, inside the 30 s margin: fetched again", 10, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := newCaptureServer(t)
			var calls atomic.Int32
			source := func(context.Context) (string, error) {
				return clientToken(tc.left, fmt.Sprintf("dev%d", calls.Add(1))), nil
			}
			client := newTokenClient(t, srv.URL, source)
			for i := 0; i < 3; i++ {
				if _, err := client.Admin().GetOverview(context.Background()); err != nil {
					t.Fatalf("GetOverview: %v", err)
				}
			}
			if n := calls.Load(); n != tc.wantCalls {
				t.Errorf("source called %d times for 3 requests, want %d", n, tc.wantCalls)
			}
		})
	}
}

func TestTokenExpiresAtReadsAClientTokenOnly(t *testing.T) {
	at, ok := TokenExpiresAt(clientToken(100, "dev1"))
	if !ok {
		t.Fatal("no expiry read from a client token")
	}
	if left := time.Until(at); left <= 90*time.Second || left > 100*time.Second {
		t.Errorf("expiry %v from now, want about 100 s", left)
	}

	// A padded segment is read too, and the value is the exp itself.
	padded := base64.URLEncoding.EncodeToString([]byte(`{"exp": 1790000000}`))
	if !strings.HasSuffix(padded, "=") {
		t.Fatalf("test payload %q is not padded", padded)
	}
	if at, ok := TokenExpiresAt("qt_h." + padded + ".s"); !ok || !at.Equal(time.Unix(1790000000, 0)) {
		t.Errorf("padded segment: TokenExpiresAt = %v, %v; want %v", at, ok, time.Unix(1790000000, 0))
	}

	noExp := base64.RawURLEncoding.EncodeToString([]byte(`{"sub":"dev1"}`))
	stringExp := base64.RawURLEncoding.EncodeToString([]byte(`{"exp":"soon"}`))
	for _, token := range []string{
		"qk_live_abc",              // an API key
		"qt_not.a",                 // two segments
		"qt_a.b.c.d",               // four
		"qt_a.!!!.c",               // not base64url
		"qt_h." + noExp + ".s",     // no exp
		"qt_h." + stringExp + ".s", // an exp that is no number
	} {
		if at, ok := TokenExpiresAt(token); ok {
			t.Errorf("TokenExpiresAt(%q) = %v, want no expiry", token, at)
		}
	}
}

func TestTokenSourceIsSharedByEveryHTTPClientOfAQueen(t *testing.T) {
	srv := newCaptureServer(t)
	source, calls := countingSource()
	client := newTokenClient(t, srv.URL, source)
	ctx := context.Background()

	if _, err := client.Admin().GetOverview(ctx); err != nil {
		t.Fatalf("GetOverview: %v", err)
	}
	// A client to one node (the feed's followers), made from the client's config.
	node, err := client.nodePoster(srv.URL)
	if err != nil {
		t.Fatalf("nodePoster: %v", err)
	}
	defer node.(*feedPoster).Close()
	if _, err := node.PostFeed(ctx, []byte(`{}`), time.Second); err != nil {
		t.Fatalf("PostFeed: %v", err)
	}
	// A streams runner's client, given the client's source.
	streamsHTTP := runtime.NewHTTPClient(srv.URL, "", 0, 0)
	streamsHTTP.TokenSource = client.TokenSource()
	if _, err := streamsHTTP.Post(ctx, "/streams/v1/queries", map[string]interface{}{}); err != nil {
		t.Fatalf("streams Post: %v", err)
	}

	if n := calls.Load(); n != 1 {
		t.Errorf("source called %d times, want 1 for three clients", n)
	}
	reqs := srv.requests()
	if len(reqs) != 3 {
		t.Fatalf("requests = %d, want 3", len(reqs))
	}
	for i, r := range reqs {
		if got := authOf(r); got != "Bearer token-0" {
			t.Errorf("request %d (%s) Authorization = %q, want Bearer token-0", i, r.Path, got)
		}
	}

	if src := newWireClient(t, srv.URL).TokenSource(); src != nil {
		t.Errorf("a client without TokenSource answered the source %v", src)
	}
}

func TestStreamsHTTPClientRefreshesAfter401(t *testing.T) {
	for _, tc := range []struct {
		name     string
		answers  []cannedResponse
		wantErr  bool
		wantHits int
	}{
		{"then a 200", []cannedResponse{tokenExpired(), okJSON(`{}`)}, false, 2},
		{"then a second 401, the caller's", []cannedResponse{tokenExpired(), tokenExpired(), tokenExpired()}, true, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := newCaptureServer(t, tc.answers...)
			source, _ := countingSource()
			streamsHTTP := runtime.NewHTTPClient(srv.URL, "", 0, 0)
			streamsHTTP.TokenSource = newTokenClient(t, srv.URL, source).TokenSource()

			_, err := streamsHTTP.Post(context.Background(), "/streams/v1/queries", map[string]interface{}{})
			var httpErr *runtime.HTTPError
			if tc.wantErr {
				if !errors.As(err, &httpErr) || httpErr.Status != http.StatusUnauthorized {
					t.Fatalf("err = %v, want the 401", err)
				}
			} else if err != nil {
				t.Fatalf("Post: %v", err)
			}
			reqs := srv.requests()
			if len(reqs) != tc.wantHits {
				t.Fatalf("requests = %d, want %d", len(reqs), tc.wantHits)
			}
			if a, b := authOf(reqs[0]), authOf(reqs[1]); a != "Bearer token-0" || b != "Bearer token-1" {
				t.Errorf("Authorization = %q then %q, want token-0 then token-1", a, b)
			}
		})
	}
}

func TestTokensMintPostsTheEntityAndReadsTheAnswer(t *testing.T) {
	srv := newCaptureServer(t, cannedResponse{
		status: http.StatusCreated,
		body:   `{"token":"qt_a.b.c","sub":"dev-8f3a","key":"k1","expiresAt":"2026-11-11T12:00:00+00:00","expiresIn":600}`,
	})
	client, err := New(ClientConfig{URL: srv.URL, TimeoutMillis: 2000, RetryAttempts: -1, BearerToken: "qk_live_role"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { client.httpClient.Close() })

	minted, err := client.Tokens().Mint(context.Background(), MintTokenRequest{Sub: "dev-8f3a", TTLSeconds: 600})
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}
	want := MintTokenResponse{Token: "qt_a.b.c", Sub: "dev-8f3a", Key: "k1", ExpiresAt: time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC), ExpiresIn: 600}
	if minted.Token != want.Token || minted.Sub != want.Sub || minted.Key != want.Key ||
		!minted.ExpiresAt.Equal(want.ExpiresAt) || minted.ExpiresIn != want.ExpiresIn {
		t.Errorf("minted = %+v, want %+v", *minted, want)
	}
	req := srv.only(t)
	if req.Method != http.MethodPost || req.Path != "/api/v1/tokens" {
		t.Errorf("request = %s %s, want POST /api/v1/tokens", req.Method, req.Path)
	}
	assertJSONBody(t, req.Body, `{"sub":"dev-8f3a","ttlSeconds":600}`)
	if got := authOf(req); got != "Bearer qk_live_role" {
		t.Errorf("Authorization = %q, want the configured key", got)
	}

	// An empty Sub is refused here: nothing more is sent.
	if _, err := client.Tokens().Mint(context.Background(), MintTokenRequest{TTLSeconds: 600}); err == nil || !strings.Contains(err.Error(), "Sub is required") {
		t.Errorf("Mint without Sub: err = %v, want Sub is required", err)
	}
	if n := len(srv.requests()); n != 1 {
		t.Errorf("requests = %d after a refused Mint, want still 1", n)
	}
}

func TestTokensMintSendsKeyAndAllowAsTheWireSpellsThem(t *testing.T) {
	answer := cannedResponse{
		status: http.StatusCreated,
		body:   `{"token":"qt_a.b.c","sub":"dev-8f3a","key":"k1","expiresAt":"2026-11-11T12:00:00+00:00","expiresIn":3600}`,
	}
	srv := newCaptureServer(t, answer, answer)
	client := newWireClient(t, srv.URL)
	ctx := context.Background()

	_, err := client.Tokens().Mint(ctx, MintTokenRequest{
		Sub: "dev-8f3a",
		Key: "devices",
		Allow: []Grant{
			{Queue: "telemetry", Partition: "{sub}", Ops: []GrantOp{GrantOpProduce}},
			{KV: "devices", Key: "{sub}/*", Ops: []GrantOp{GrantOpRead, GrantOpWrite}},
		},
	})
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}
	// An empty Allow is sent, not dropped: that token reaches nothing, where no
	// Allow at all reaches everything its key does.
	if _, err := client.Tokens().Mint(ctx, MintTokenRequest{Sub: "dev-8f3a", Allow: []Grant{}}); err != nil {
		t.Fatalf("Mint: %v", err)
	}
	reqs := srv.requests()
	if len(reqs) != 2 {
		t.Fatalf("requests = %d, want 2", len(reqs))
	}
	assertJSONBody(t, reqs[0].Body, `{"sub":"dev-8f3a","key":"devices","allow":[
		{"queue":"telemetry","partition":"{sub}","ops":["produce"]},
		{"kv":"devices","key":"{sub}/*","ops":["read","write"]}]}`)
	assertJSONBody(t, reqs[1].Body, `{"sub":"dev-8f3a","allow":[]}`)
}

func TestTokensMintRefusalKeepsItsCode(t *testing.T) {
	srv := newCaptureServer(t, cannedResponse{status: http.StatusNotFound, body: `{"error":"no scoped key devices","code":"key_unknown"}`})
	client := newWireClient(t, srv.URL)

	_, err := client.Tokens().Mint(context.Background(), MintTokenRequest{Sub: "dev-8f3a", Key: "devices"})
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.StatusCode != http.StatusNotFound || httpErr.Code != "key_unknown" {
		t.Fatalf("err = %v, want the 404 key_unknown", err)
	}
}

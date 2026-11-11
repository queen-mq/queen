package queen

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"
)

// Client tokens: a credential for ONE entity, a device or a browser user,
// minted from a scoped key (POST /api/v1/tokens).
//
// The key is the role: its grants say which queues, partitions, KV keys and
// timers it reaches, and may name the entity as {sub} (queue `telemetry`,
// partition `{sub}`: each device its own partition). The token adds the
// entity and never reaches more than its key. The application's backend holds
// the key and mints; the device holds only its token:
//
//	// backend, with the scoped key `devices`
//	backend, _ := queen.New(queen.ClientConfig{URL: url, BearerToken: os.Getenv("QUEEN_DEVICES_KEY")})
//	minted, err := backend.Tokens().Mint(ctx, queen.MintTokenRequest{Sub: deviceID, TTLSeconds: 3600})
//
//	// device: a function, so the client fetches a new token before this one
//	// expires and after a 401
//	device, _ := queen.New(queen.ClientConfig{URL: url, TokenSource: fetchTokenFromMyBackend})
//
// Revoking or narrowing the key reaches every token it minted within about a
// second; blocking one sub refuses that entity's tokens on every node.

// ClientTokenPrefix is what every client token starts with: "qt_", then a JWT.
const ClientTokenPrefix = "qt_"

// tokenRefreshAhead is how long before a client token's exp it is fetched
// again, so a request rarely meets an expired token at all.
const tokenRefreshAhead = 30 * time.Second

// ErrTokenSource is in the chain of every error a ClientConfig.TokenSource
// causes: its function failed, or answered an empty token or one that cannot
// be sent as a header. The request it was for was not sent.
var ErrTokenSource = errors.New("queen: token source")

// tokenSource is the cache behind ClientConfig.TokenSource: the token it last
// answered, kept until it nears its exp or a request is refused with it.
//
// There is one per Queen, shared by pointer by every HTTP client made from
// its config and by a streams runner given Queen.TokenSource, so a refresh
// calls the function once, not once per client.
type tokenSource struct {
	fetch func(ctx context.Context) (string, error)

	// fetching (capacity 1) is held for the whole of a fetch: one at a time,
	// and a request whose context ends can stop waiting for it, which a
	// sync.Mutex held across the call would not allow.
	fetching chan struct{}

	mu        sync.Mutex // guards token and expiresAt
	token     string     // "" before the first fetch
	expiresAt time.Time  // a client token's exp; zero for any other token
}

func newTokenSource(fetch func(ctx context.Context) (string, error)) *tokenSource {
	return &tokenSource{fetch: fetch, fetching: make(chan struct{}, 1)}
}

// Token answers the token to send. failed is the token a request was just
// refused with (401): a new one is fetched unless another request already
// replaced it. "" asks for the current token, fetched when there is none yet
// or it nears its exp.
func (s *tokenSource) Token(ctx context.Context, failed string) (string, error) {
	if token, ok := s.kept(failed); ok {
		return token, nil
	}
	select {
	case s.fetching <- struct{}{}:
	case <-ctx.Done():
		return "", ctx.Err()
	}
	defer func() { <-s.fetching }()
	// The fetch this request waited for may have answered it already: many
	// requests at once, or many refused with the same token, fetch once.
	if token, ok := s.kept(failed); ok {
		return token, nil
	}

	token, err := s.fetch(ctx)
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrTokenSource, err)
	}
	token = strings.TrimSpace(token)
	if token == "" {
		return "", fmt.Errorf("%w: the function answered an empty token", ErrTokenSource)
	}
	// net/http would refuse it only when writing the request, as a transport
	// error: the backend marked unhealthy and the request retried for nothing.
	if strings.ContainsFunc(token, func(r rune) bool { return r < 0x20 || r == 0x7f }) {
		return "", fmt.Errorf("%w: the function answered a token that cannot be sent as a header", ErrTokenSource)
	}
	expiresAt, _ := TokenExpiresAt(token)

	s.mu.Lock()
	s.token, s.expiresAt = token, expiresAt
	s.mu.Unlock()
	return token, nil
}

// kept answers the kept token when it still serves: for a refused request, a
// token other than the one refused; otherwise a token not near its exp.
func (s *tokenSource) kept(failed string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.token == "" {
		return "", false
	}
	if failed != "" {
		return s.token, s.token != failed
	}
	expiring := !s.expiresAt.IsZero() && time.Until(s.expiresAt) < tokenRefreshAhead
	return s.token, !expiring
}

// TokenExpiresAt reads a client token's expiry from its exp claim; false for
// anything else (an API key, an opaque token). Read, never verified: the
// broker is the one that verifies.
func TokenExpiresAt(token string) (time.Time, bool) {
	jwt, ok := strings.CutPrefix(token, ClientTokenPrefix)
	if !ok {
		return time.Time{}, false
	}
	parts := strings.Split(jwt, ".")
	if len(parts) != 3 {
		return time.Time{}, false
	}
	// Base64url without padding is the JWT encoding; a padded one is read too.
	payload, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(parts[1], "="))
	if err != nil {
		return time.Time{}, false
	}
	var claims struct {
		Exp *float64 `json:"exp"`
	}
	if json.Unmarshal(payload, &claims) != nil || claims.Exp == nil {
		return time.Time{}, false
	}
	secs, frac := math.Modf(*claims.Exp)
	return time.Unix(int64(secs), int64(frac*1e9)), true
}

// GrantOp is what a grant lets its holder do: produce or consume on a queue,
// read or write on KV and timers.
type GrantOp string

const (
	GrantOpProduce GrantOp = "produce" // queues: push
	GrantOpConsume GrantOp = "consume" // queues: pop, ack, renew the lease
	GrantOpRead    GrantOp = "read"    // KV and timers: read
	GrantOpWrite   GrantOp = "write"   // KV and timers: put, delete, incr; schedule, cancel
)

// Grant is one line of a scoped key's role, or of a token's MintTokenRequest.Allow.
// Exactly one of Queue, KV and Timers names the surface; the other fields
// narrow it, and an empty one is `*`. Every field is a pattern: a name,
// `prefix*`, or `*`, and may hold `{sub}` once.
//
//	Queue   Partition, Group   ops produce, consume
//	KV      Key                ops read, write
//	Timers  Key, Partition     ops read, write
type Grant struct {
	Queue     string    `json:"queue,omitempty"`
	KV        string    `json:"kv,omitempty"`
	Timers    string    `json:"timers,omitempty"`
	Partition string    `json:"partition,omitempty"`
	Group     string    `json:"group,omitempty"`
	Key       string    `json:"key,omitempty"`
	Ops       []GrantOp `json:"ops"`
}

// MintTokenRequest is the body of POST /api/v1/tokens. Only Sub is required.
type MintTokenRequest struct {
	// Sub is the entity, e.g. a device id: 1 to 128 letters, digits and `. _ - @ +`.
	Sub string `json:"sub"`
	// TTLSeconds is the token's lifetime: 0 takes the default, 3600, and it is
	// at most the key's own ceiling.
	TTLSeconds int64 `json:"ttlSeconds,omitempty"`
	// Key is the scoped key to mint for, by id or name: only when this client
	// holds an admin key. A scoped key mints for itself.
	Key string `json:"key,omitempty"`
	// Allow narrows the key's grants for this token; it never widens them. nil
	// sends nothing, and the token reaches what its key reaches. An empty,
	// non-nil list is sent as it is, and that token reaches nothing: which is
	// why the tag is omitzero, not omitempty, which would drop it and hand out
	// the whole key instead.
	Allow []Grant `json:"allow,omitzero"`
}

// MintTokenResponse is the answer to a mint.
type MintTokenResponse struct {
	// Token is the bearer credential, ClientTokenPrefix then a JWT.
	Token string `json:"token"`
	Sub   string `json:"sub"`
	// Key is the id of the key the token belongs to.
	Key string `json:"key"`
	// ExpiresAt is when the token stops working.
	ExpiresAt time.Time `json:"expiresAt"`
	// ExpiresIn is its lifetime in seconds, from the mint.
	ExpiresIn int64 `json:"expiresIn"`
}

// Tokens is the client tokens API. Get it from Queen.Tokens.
type Tokens struct {
	httpClient *HttpClient
}

// Tokens returns the client tokens API.
func (q *Queen) Tokens() *Tokens {
	return &Tokens{httpClient: q.httpClient}
}

// Mint mints a token for req.Sub with this client's credential: a scoped key
// mints for itself; an admin key for the scoped key req.Key names.
//
// A refusal is an *HTTPError whose Code says why: invalid_request (400),
// forbidden or client_blocked (403), key_unknown (404), key_ambiguous (409),
// not_configured (503).
func (t *Tokens) Mint(ctx context.Context, req MintTokenRequest) (*MintTokenResponse, error) {
	if req.Sub == "" {
		return nil, errors.New("queen: Tokens.Mint: Sub is required (the entity the token is for)")
	}
	body, err := t.httpClient.PostRaw(ctx, "/api/v1/tokens", req)
	if err != nil {
		return nil, err
	}
	var out MintTokenResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("queen: Tokens.Mint: the answer is not a minted token: %w", err)
	}
	if out.Token == "" {
		return nil, errors.New("queen: Tokens.Mint: the answer carries no token")
	}
	return &out, nil
}

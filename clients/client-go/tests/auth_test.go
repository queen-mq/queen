// Tests for server-stamped producer identity (issue #23, feature A).
//
// These tests exercise two invariants:
//
//  1. The typed Message.ProducerSub field correctly receives the server-stamped
//     JWT sub claim on pop (the whole reason this client library needs a field
//     update at all - without it, json.Unmarshal silently drops the value).
//
//  2. The server's anti-impersonation invariant holds for Go clients: even if
//     a push request body contains producerSub, the server replaces it with
//     the authenticated JWT sub.
//
// These are black-box: the segments engine stores messages in seg_segments and
// never populates queen.messages, so producerSub is observed via the pop API
// (the seg-native ground truth) rather than a direct SQL read. The JWT-gated
// tests require the server running with JWT enabled and the matching JWT_SECRET
// env var set when running the tests.

package tests

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	queen "github.com/smartpricing/queen/clients/client-go"
)

// ---------------------------------------------------------------------------
// Minimal HS256 JWT signer - keeps test deps at zero (pure stdlib).
// ---------------------------------------------------------------------------
func b64URL(b []byte) string {
	return strings.TrimRight(base64.URLEncoding.EncodeToString(b), "=")
}

func signHS256(payload map[string]interface{}, secret string) (string, error) {
	header, err := json.Marshal(map[string]string{"alg": "HS256", "typ": "JWT"})
	if err != nil {
		return "", err
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	encHeader := strings.ReplaceAll(strings.ReplaceAll(b64URL(header), "+", "-"), "/", "_")
	encPayload := strings.ReplaceAll(strings.ReplaceAll(b64URL(payloadBytes), "+", "-"), "/", "_")
	signingInput := encHeader + "." + encPayload
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(signingInput))
	sig := mac.Sum(nil)
	encSig := strings.ReplaceAll(strings.ReplaceAll(b64URL(sig), "+", "-"), "/", "_")
	return signingInput + "." + encSig, nil
}

func makeJWT(t *testing.T, sub, secret string) string {
	t.Helper()
	now := time.Now().Unix()
	tok, err := signHS256(map[string]interface{}{
		"sub":      sub,
		"username": sub,
		"role":     "read-write",
		"iat":      now,
		"exp":      now + 3600,
	}, secret)
	if err != nil {
		t.Fatalf("signHS256: %v", err)
	}
	return tok
}

// ---------------------------------------------------------------------------
// HTTP helpers.
// ---------------------------------------------------------------------------
func httpPush(t *testing.T, queue, txID string, data map[string]interface{}, bearer string, extraFields map[string]interface{}) {
	t.Helper()
	item := map[string]interface{}{
		"queue":         queue,
		"partition":     "Default",
		"transactionId": txID,
		"payload":       data,
	}
	for k, v := range extraFields {
		item[k] = v
	}
	body, _ := json.Marshal(map[string]interface{}{"items": []interface{}{item}})

	req, err := http.NewRequest(http.MethodPost, serverURL+"/api/v1/push", strings.NewReader(string(body)))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("push: %v", err)
	}
	defer res.Body.Close()
	if res.StatusCode >= 300 {
		t.Fatalf("push status %d", res.StatusCode)
	}
}

// configureQueue creates a queue over HTTP with an optional bearer (configure
// requires auth when JWT is enabled).
func configureQueue(t *testing.T, queue, bearer string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, serverURL+"/api/v1/configure",
		strings.NewReader(fmt.Sprintf(`{"queue":%q,"options":{}}`, queue)))
	if err != nil {
		t.Fatalf("configure request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("configure: %v", err)
	}
	res.Body.Close()
}

// popProducerSub pops the queue (optionally authenticated) until it observes the
// message with the given transactionId, then returns its ProducerSub as
// deserialised into the typed Message struct. This is the seg-native ground
// truth for producerSub (queen.messages is never populated by the seg engine).
func popProducerSub(ctx context.Context, t *testing.T, bearer, queue, txID string) (string, bool) {
	t.Helper()
	c, err := queen.New(queen.ClientConfig{URL: serverURL, BearerToken: bearer})
	if err != nil {
		t.Fatalf("build client: %v", err)
	}
	defer c.Close(ctx)

	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		msgs, err := c.Queue(queue).Batch(20).Wait(true).Pop(ctx)
		if err != nil {
			t.Fatalf("pop: %v", err)
		}
		for _, m := range msgs {
			if m.TransactionID == txID {
				return m.ProducerSub, true
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return "", false
}

// ===========================================================================
// TEST: Message.ProducerSub is properly deserialised by the typed struct
// ---------------------------------------------------------------------------
// An authenticated push stamps producer_sub from the JWT sub; this test proves
// the Go client deserialises that stamped, non-empty value into the typed
// Message.ProducerSub field on pop. (An authenticated push is the only way a
// non-null producerSub reaches a seg-broker message, so it is JWT-gated.)
// ===========================================================================
func TestProducerSubFieldDeserialisation(t *testing.T) {
	requireClient(t)
	secret := os.Getenv("JWT_SECRET")
	if secret == "" {
		t.Skip("set JWT_SECRET (matching server) to run")
	}
	ctx := context.Background()

	q := fmt.Sprintf("test-auth-go-field-%d", time.Now().UnixNano())
	txID := fmt.Sprintf("tx-go-field-%d", time.Now().UnixNano())
	token := makeJWT(t, "go-test-sub", secret)

	configureQueue(t, q, token)
	httpPush(t, q, txID, map[string]interface{}{"hello": "world"}, token, nil)

	sub, found := popProducerSub(ctx, t, token, q, txID)
	if !found {
		t.Fatalf("did not observe pushed tx %s via pop", txID)
	}
	if sub != "go-test-sub" {
		t.Fatalf("ProducerSub = %q, want %q", sub, "go-test-sub")
	}
}

// ===========================================================================
// TEST: Body-supplied producerSub is ignored (spoof prevention) - no auth
// ===========================================================================
func TestProducerSubBodyIgnoredWithoutAuth(t *testing.T) {
	requireClient(t)
	if os.Getenv("JWT_SECRET") != "" {
		t.Skip("JWT_SECRET is set - run TestProducerSubStampedFromJwt instead")
	}
	ctx := context.Background()

	q := fmt.Sprintf("test-auth-go-noauth-%d", time.Now().UnixNano())
	txID := fmt.Sprintf("tx-go-noauth-%d", time.Now().UnixNano())

	httpPush(t, q, txID, map[string]interface{}{"hello": "world"}, "", map[string]interface{}{
		"producerSub": "attacker-no-jwt",
	})

	// Black-box: observe via pop. With auth disabled the field must be empty
	// (the Go zero value for a null producerSub); the body value must be ignored.
	sub, found := popProducerSub(ctx, t, "", q, txID)
	if !found {
		t.Fatalf("did not observe pushed tx %s via pop", txID)
	}
	if sub != "" {
		t.Fatalf("expected empty producerSub (auth disabled) but got %q - client was able to set it!", sub)
	}
}

// ===========================================================================
// TEST: Authenticated push stamps producer_sub from JWT sub claim
// ===========================================================================
func TestProducerSubStampedFromJwt(t *testing.T) {
	requireClient(t)
	secret := os.Getenv("JWT_SECRET")
	if secret == "" {
		t.Skip("set JWT_SECRET (matching server) to run")
	}
	ctx := context.Background()

	q := fmt.Sprintf("test-auth-go-jwt-%d", time.Now().UnixNano())
	txID := fmt.Sprintf("tx-go-jwt-%d", time.Now().UnixNano())
	token := makeJWT(t, "alice-go-producer", secret)

	configureQueue(t, q, token)
	httpPush(t, q, txID, map[string]interface{}{"hello": "world"}, token, nil)

	sub, found := popProducerSub(ctx, t, token, q, txID)
	if !found {
		t.Fatalf("did not observe pushed tx %s via pop", txID)
	}
	if sub != "alice-go-producer" {
		t.Fatalf("producerSub = %q, want %q", sub, "alice-go-producer")
	}
}

// ===========================================================================
// TEST: Spoofing is blocked even with a valid JWT
// ===========================================================================
func TestProducerSubSpoofingIgnoredWithJwt(t *testing.T) {
	requireClient(t)
	secret := os.Getenv("JWT_SECRET")
	if secret == "" {
		t.Skip("set JWT_SECRET (matching server) to run")
	}
	ctx := context.Background()

	q := fmt.Sprintf("test-auth-go-spoof-%d", time.Now().UnixNano())
	txID := fmt.Sprintf("tx-go-spoof-%d", time.Now().UnixNano())
	token := makeJWT(t, "legit-go-producer", secret)

	configureQueue(t, q, token)
	httpPush(t, q, txID, map[string]interface{}{"hello": "world"}, token, map[string]interface{}{
		"producerSub": "attacker",
	})

	sub, found := popProducerSub(ctx, t, token, q, txID)
	if !found {
		t.Fatalf("did not observe pushed tx %s via pop", txID)
	}
	if sub != "legit-go-producer" {
		t.Fatalf("impersonation not prevented: producerSub %q, want %q", sub, "legit-go-producer")
	}
}

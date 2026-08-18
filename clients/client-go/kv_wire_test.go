package queen

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"
)

// The KV wire contract (PLAN_KV_TIMERS.md §5, §8.1), asserted against a scripted
// plan server. No broker, no database.

func kvCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// ---------------------------------------------------------------------------
// THE INSIDIA THAT GATES THIS WHOLE FEATURE IN GO (§10.4, first bullet).
//
// attemptOnce ends in json.Unmarshal into map[string]interface{}, so EVERY
// number is a float64 and no 64-bit integer survives. version is a BIGINT and
// incr runs on numeric. The fix is a raw path — and the trap in the fix is
// putting UseNumber() into parseBody, which would break, IN SILENCE, the message
// parsing that asserts msgMap["retryCount"].(float64).
// ---------------------------------------------------------------------------

func TestParseBodyKeepsFloat64Semantics(t *testing.T) {
	// This is the guard on the OTHER side of the raw path: parseBody must
	// reproduce exactly what attemptOnce did, float64 numbers included. A
	// UseNumber() here would make every `.(float64)` assertion in the message
	// parsing fail its type switch and silently zero the field.
	result, err := parseBody([]byte(`{"retryCount":3,"messages":[{"retryCount":7}]}`))
	if err != nil {
		t.Fatalf("parseBody: %v", err)
	}
	if _, ok := result["retryCount"].(float64); !ok {
		t.Fatalf("retryCount is %T, want float64: a UseNumber() in parseBody breaks message parsing silently", result["retryCount"])
	}
	msgs, ok := result["messages"].([]interface{})
	if !ok {
		t.Fatalf("messages is %T, want []interface{}", result["messages"])
	}
	msgMap := msgs[0].(map[string]interface{})
	if _, ok := msgMap["retryCount"].(float64); !ok {
		t.Fatalf("nested retryCount is %T, want float64", msgMap["retryCount"])
	}
}

func TestParseBodyReproducesTheThreeLegacyShapes(t *testing.T) {
	// Top-level array is wrapped as {"data": [...]} (parseAckResponses depends
	// on it), a non-JSON body becomes {"raw": ...}, an empty body is a nil map
	// with no error. All three are today's attemptOnce behaviour and moving the
	// unmarshal down the stack must not change any of them.
	arr, err := parseBody([]byte(`[{"success":true}]`))
	if err != nil {
		t.Fatalf("array body: %v", err)
	}
	if _, ok := arr["data"].([]interface{}); !ok {
		t.Fatalf("array body did not become {\"data\":[...]}: %#v", arr)
	}

	raw, err := parseBody([]byte(`not json at all`))
	if err != nil {
		t.Fatalf("raw body: %v", err)
	}
	if raw["raw"] != "not json at all" {
		t.Fatalf("non-JSON body did not become {\"raw\":...}: %#v", raw)
	}

	empty, err := parseBody(nil)
	if err != nil {
		t.Fatalf("empty body: %v", err)
	}
	if empty != nil {
		t.Fatalf("empty body should parse to a nil map, got %#v", empty)
	}
}

// ---------------------------------------------------------------------------
// put / putIfAbsent / delete / incr — the exact body of each.
// ---------------------------------------------------------------------------

func TestKVPutSendsTheExactBodyOnThePathRoute(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`{"index":0,"op":"put","applied":true,"key":"9f1","value":{"n":1},"version":4}`))
	client := newWireClient(t, srv.URL)

	res, err := client.KV().Put(kvCtx(t), "orders", "9f1", map[string]interface{}{"n": 1}, TTLSeconds(30))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	req := srv.only(t)
	if req.Method != http.MethodPut {
		t.Errorf("method = %s, want PUT", req.Method)
	}
	if req.Path != "/api/v1/kv/orders/9f1" {
		t.Errorf("path = %s, want /api/v1/kv/orders/9f1", req.Path)
	}
	if req.RawQuery != "" {
		t.Errorf("the path routes take NO query string (§5.5), got %q", req.RawQuery)
	}
	// ttlSeconds, never ttlMillis (§20.1). No `forever`, no `expect`, no `op`,
	// no `ns`, no `key`: the last three are named by the URL and the server
	// rejects a body that shadows them.
	assertJSONBody(t, req.Body, `{"value":{"n":1},"ttlSeconds":30}`)

	if !res.Applied || res.Version != 4 {
		t.Errorf("applied=%v version=%d, want true/4", res.Applied, res.Version)
	}
}

func TestKVPutForeverAndExpectAndRequired(t *testing.T) {
	srv := newCaptureServer(t,
		okJSON(`{"index":0,"op":"put","applied":true,"key":"k","value":1,"version":1}`),
		okJSON(`{"index":0,"op":"put","applied":true,"key":"k","value":1,"version":9}`),
	)
	client := newWireClient(t, srv.URL)
	ctx := kvCtx(t)

	if _, err := client.KV().Put(ctx, "cfg", "k", 1, Forever()); err != nil {
		t.Fatalf("put forever: %v", err)
	}
	if _, err := client.KV().Put(ctx, "cfg", "k", 1, TTLSeconds(60), KVWriteOptions{
		Expect:   Expect(8),
		Required: true,
	}); err != nil {
		t.Fatalf("put expect: %v", err)
	}

	reqs := srv.requests()
	if len(reqs) != 2 {
		t.Fatalf("want 2 requests, got %d", len(reqs))
	}
	// Exactly one of ttlSeconds and forever — never both, never neither (§5.1).
	assertJSONBody(t, reqs[0].Body, `{"value":1,"forever":true}`)
	assertJSONBody(t, reqs[1].Body, `{"value":1,"ttlSeconds":60,"expect":8,"required":true}`)
}

func TestKVPutRefusesAZeroValueExpiryBeforeSending(t *testing.T) {
	// §5.1: exactly one of ttlSeconds and forever is mandatory, and the Go zero
	// value of Expiry is NOT a declaration. This must cost zero round trips: a
	// missing expiry is the fastest way to make a marker immortal, and the
	// client is where the caller finds out.
	srv := newCaptureServer(t)
	client := newWireClient(t, srv.URL)

	var unset Expiry
	if _, err := client.KV().Put(kvCtx(t), "orders", "k", 1, unset); err == nil {
		t.Fatal("a zero-value Expiry must be an error")
	}
	if n := len(srv.requests()); n != 0 {
		t.Fatalf("a rejected expiry must not reach the network, got %d requests", n)
	}
}

func TestKVExpiryConversions(t *testing.T) {
	// The declared rule (§20.1/§20.6): durations that CAN be sub-second are in
	// milliseconds, the ones that cannot are in seconds. A KV TTL cannot, so the
	// wire carries whole seconds and the SDK rounds UP — a TTL rounded down can
	// expire a marker before the window it had to cover.
	if got := TTL(1500 * time.Millisecond).ttl(); got != 2 {
		t.Errorf("TTL(1.5s) = %d seconds, want 2 (rounded up)", got)
	}
	if got := TTL(2 * time.Second).ttl(); got != 2 {
		t.Errorf("TTL(2s) = %d, want 2", got)
	}
	if err := TTL(0).err; err == nil {
		t.Error("TTL(0) must be invalid: ttlSeconds is an integer greater than zero")
	}
	if err := TTLSeconds(-1).err; err == nil {
		t.Error("a negative TTL must be invalid")
	}
	// `until: <date>` is the allowed sugar, converted to a delta at send time
	// and rounded up.
	if got := Until(time.Now().Add(4500 * time.Millisecond)).ttl(); got != 5 {
		t.Errorf("Until(+4.5s) = %d, want 5", got)
	}
}

func TestKVPutIfAbsentGoesThroughTheBatchRouteUnderItsOwnName(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`{"results":[{"index":0,"op":"putIfAbsent","applied":false,"reason":"exists","key":"saga-7","value":{"owner":"worker-2"},"version":11}]}`))
	client := newWireClient(t, srv.URL)

	res, err := client.KV().PutIfAbsent(kvCtx(t), "saga", "saga-7", map[string]interface{}{"owner": "worker-1"}, TTLSeconds(3600))
	if err != nil {
		t.Fatalf("putIfAbsent: %v", err)
	}
	req := srv.only(t)
	if req.Method != http.MethodPost || req.Path != "/api/v1/kv" {
		t.Fatalf("putIfAbsent went to %s %s, want POST /api/v1/kv", req.Method, req.Path)
	}
	// putIfAbsent travels under its own name; the server desugars it to
	// put+expect:0. Sending BOTH the name and an expect other than 0 is a
	// contradiction the server raises on, so the client sends neither.
	assertJSONBody(t, req.Body, `{"operations":[{"op":"putIfAbsent","ns":"saga","key":"saga-7","value":{"owner":"worker-1"},"ttlSeconds":3600}]}`)

	// A lost race is 200 with applied:false — NOT an error (§8.1). And the
	// loser gets the winner's value without a second round trip (§5.3).
	if res.Applied {
		t.Error("expected applied=false")
	}
	if res.Reason != KVReasonExists {
		t.Errorf("reason = %q, want %q", res.Reason, KVReasonExists)
	}
	if string(res.Value) != `{"owner":"worker-2"}` {
		t.Errorf("the loser must carry the winner's value, got %s", string(res.Value))
	}
	if res.Version != 11 {
		t.Errorf("version = %d, want 11", res.Version)
	}
}

func TestKVDeleteSendsNoBodyWithoutOptionsAndAnExpectWithOne(t *testing.T) {
	srv := newCaptureServer(t,
		okJSON(`{"index":0,"op":"delete","applied":true,"key":"k","value":null,"version":0}`),
		okJSON(`{"index":0,"op":"delete","applied":false,"reason":"version","key":"k","value":{"a":1},"version":5}`),
	)
	client := newWireClient(t, srv.URL)
	ctx := kvCtx(t)

	if _, err := client.KV().Delete(ctx, "orders", "k"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := client.KV().Delete(ctx, "orders", "k", KVWriteOptions{Expect: Expect(5)}); err != nil {
		t.Fatalf("delete expect: %v", err)
	}

	reqs := srv.requests()
	if len(reqs) != 2 {
		t.Fatalf("want 2 requests, got %d", len(reqs))
	}
	if reqs[0].Method != http.MethodDelete || reqs[0].Path != "/api/v1/kv/orders/k" {
		t.Fatalf("delete went to %s %s", reqs[0].Method, reqs[0].Path)
	}
	// A DELETE with nothing to say sends no body at all: the common case must
	// not depend on the server tolerating `{}`.
	if len(reqs[0].Body) != 0 {
		t.Errorf("delete without options sent a body: %s", string(reqs[0].Body))
	}
	assertJSONBody(t, reqs[1].Body, `{"expect":5}`)
}

func TestKVIncrCarriesDeltaMinMaxAndSurvivesSixtyFourBits(t *testing.T) {
	// 9007199254740993 is 2^53+1: the first integer JSON cannot round-trip
	// through a float64. It is the whole reason for the raw path (§10.4).
	srv := newCaptureServer(t, okJSON(`{"results":[{"index":0,"op":"incr","applied":true,"key":"acme","value":9007199254740993,"version":9007199254740993}]}`))
	client := newWireClient(t, srv.URL)

	res, err := client.KV().Incr(kvCtx(t), "quota", "acme", 1, TTLSeconds(60), KVIncrOptions{Max: Int64(100)})
	if err != nil {
		t.Fatalf("incr: %v", err)
	}
	req := srv.only(t)
	if req.Method != http.MethodPost || req.Path != "/api/v1/kv" {
		t.Fatalf("incr went to %s %s, want POST /api/v1/kv (it exists only in the batch body, §8.1)", req.Method, req.Path)
	}
	assertJSONBody(t, req.Body, `{"operations":[{"op":"incr","ns":"quota","key":"acme","delta":1,"ttlSeconds":60,"max":100}]}`)

	if res.Value != 9007199254740993 {
		t.Errorf("incr value = %d, want 9007199254740993 (float64 would give ...92)", res.Value)
	}
	if res.Version != 9007199254740993 {
		t.Errorf("version = %d, want 9007199254740993", res.Version)
	}
}

func TestKVIncrRefusesANumberThatDoesNotFitInInt64(t *testing.T) {
	// §5.4: the server value is `numeric`, so it cannot overflow there. A typed
	// SDK exposes int64 and must fail EXPLICITLY rather than hand back a wrong
	// number.
	srv := newCaptureServer(t, okJSON(`{"results":[{"index":0,"op":"incr","applied":true,"key":"acme","value":184467440737095516160,"version":3}]}`))
	client := newWireClient(t, srv.URL)

	if _, err := client.KV().Incr(kvCtx(t), "quota", "acme", 1, TTLSeconds(60)); err == nil {
		t.Fatal("a counter beyond int64 must be an explicit error, never a truncated number")
	}
}

func TestKVIncrOverLimitIsAVerdictNotAnError(t *testing.T) {
	// With `max`, `applied` IS the admission decision (§5.4). It must not
	// arrive as an error, or every rate limiter in Go would branch on err.
	srv := newCaptureServer(t, okJSON(`{"results":[{"index":0,"op":"incr","applied":false,"reason":"limit","key":"acme","value":100,"version":7}]}`))
	client := newWireClient(t, srv.URL)

	res, err := client.KV().Incr(kvCtx(t), "quota", "acme", 1, TTLSeconds(60), KVIncrOptions{Max: Int64(100)})
	if err != nil {
		t.Fatalf("a refused incr must not be an error: %v", err)
	}
	if res.Applied || res.Reason != KVReasonLimit || res.Value != 100 {
		t.Errorf("got applied=%v reason=%q value=%d", res.Applied, res.Reason, res.Value)
	}
}

// ---------------------------------------------------------------------------
// Reads.
// ---------------------------------------------------------------------------

func TestKVGetUsesThePathRouteAndKeepsSixtyFourBitVersions(t *testing.T) {
	srv := newCaptureServer(t, cannedResponse{
		status: http.StatusOK,
		body:   `{"index":0,"op":"get","found":true,"key":"order/9f1/items","value":{"count":2},"version":9007199254740993,"expiresAt":"2026-08-17T10:00:00+00:00","updatedAt":"2026-08-17T09:00:00+00:00"}`,
		header: map[string]string{"ETag": `"9007199254740993"`},
	})
	client := newWireClient(t, srv.URL)

	entry, err := client.KV().Get(kvCtx(t), "orders", "order/9f1/items")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	req := srv.only(t)
	if req.Method != http.MethodGet {
		t.Errorf("method = %s, want GET", req.Method)
	}
	// A key may contain slashes: the route is a catch-all precisely so
	// `order/9f1/items` writes naturally. Escaped or not, the server decodes to
	// the same key — what must never happen is the key being cut or dropped.
	if req.Path != "/api/v1/kv/orders/order/9f1/items" {
		t.Errorf("path = %s, want the key to arrive whole", req.Path)
	}
	if !entry.Found {
		t.Fatal("found = false")
	}
	if entry.Version != 9007199254740993 {
		t.Errorf("version = %d, want 9007199254740993", entry.Version)
	}
	if string(entry.Value) != `{"count":2}` {
		t.Errorf("value = %s", string(entry.Value))
	}
	if entry.ExpiresAt.IsZero() {
		t.Error("expiresAt was not parsed")
	}
}

func TestKVKeyIsPercentEscapedInThePath(t *testing.T) {
	// A key is an arbitrary string. Unescaped, a `?` in it would start a query
	// string — and this route REFUSES any query string outright (§5.5), so the
	// symptom would be a 400 nobody can explain from the call site.
	srv := newCaptureServer(t, okJSON(`{"index":0,"op":"get","found":false,"key":"a b?c#d"}`))
	client := newWireClient(t, srv.URL)

	if _, err := client.KV().Get(kvCtx(t), "orders", "a b?c#d"); err != nil {
		t.Fatalf("get: %v", err)
	}
	req := srv.only(t)
	if req.RawQuery != "" {
		t.Fatalf("the key leaked into the query string: %q", req.RawQuery)
	}
	if req.Path != "/api/v1/kv/orders/a b?c#d" {
		t.Fatalf("the key did not arrive whole: %q", req.Path)
	}
}

func TestKVGetSeparatesFoundFromANullValue(t *testing.T) {
	// `'null'::jsonb` is a legal value: {found:true, value:null} and
	// {found:false} are different things no SDK may collapse (§5.5).
	srv := newCaptureServer(t,
		okJSON(`{"index":0,"op":"get","found":true,"key":"k","value":null,"version":3}`),
		okJSON(`{"index":0,"op":"get","found":false,"key":"k"}`),
	)
	client := newWireClient(t, srv.URL)
	ctx := kvCtx(t)

	live, err := client.KV().Get(ctx, "orders", "k")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !live.Found {
		t.Error("a stored null must still be found:true")
	}
	if string(live.Value) != "null" {
		t.Errorf("value = %q, want null", string(live.Value))
	}

	miss, err := client.KV().Get(ctx, "orders", "k")
	if err != nil {
		t.Fatalf("get miss: %v", err)
	}
	if miss.Found {
		t.Error("found should be false")
	}
}

func TestKVGetAsDecodesIntoTheCallersType(t *testing.T) {
	// KVGetAs is the ONE generic function of this package (§10.2).
	type order struct {
		Count int    `json:"count"`
		State string `json:"state"`
	}
	srv := newCaptureServer(t,
		okJSON(`{"index":0,"op":"get","found":true,"key":"k","value":{"count":2,"state":"open"},"version":1}`),
		okJSON(`{"index":0,"op":"get","found":false,"key":"k"}`),
	)
	client := newWireClient(t, srv.URL)
	ctx := kvCtx(t)

	got, found, err := KVGetAs[order](ctx, client.KV(), "orders", "k")
	if err != nil {
		t.Fatalf("KVGetAs: %v", err)
	}
	if !found || got.Count != 2 || got.State != "open" {
		t.Fatalf("got %+v found=%v", got, found)
	}

	zero, found, err := KVGetAs[order](ctx, client.KV(), "orders", "k")
	if err != nil {
		t.Fatalf("KVGetAs miss: %v", err)
	}
	if found || zero.Count != 0 {
		t.Fatalf("a miss must be found=false with the zero value, got %+v", zero)
	}
}

func TestKVGetManyReportsMissingAsData(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`{"results":[{"index":0,"op":"getMany","rows":[{"key":"a","value":1,"version":2}],"missing":["b"],"truncated":false}]}`))
	client := newWireClient(t, srv.URL)

	many, err := client.KV().GetMany(kvCtx(t), "orders", []string{"a", "b"})
	if err != nil {
		t.Fatalf("getMany: %v", err)
	}
	assertJSONBody(t, srv.only(t).Body, `{"operations":[{"op":"getMany","ns":"orders","keys":["a","b"]}]}`)

	// Absence is a DATUM, not a hole computed by difference (§5.5). Rows, never
	// a key/value map: the shape makes the confusion inexpressible.
	if len(many.Rows) != 1 || many.Rows[0].Key != "a" {
		t.Errorf("rows = %+v", many.Rows)
	}
	if len(many.Missing) != 1 || many.Missing[0] != "b" {
		t.Errorf("missing = %v", many.Missing)
	}
}

func TestKVGetPrefixSendsPrefixOnlyInTheBody(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`{"results":[{"index":0,"op":"getPrefix","rows":[{"key":"quota:acme:1","value":3,"version":1}],"truncated":true,"nextAfter":"quota:acme:1"}]}`))
	client := newWireClient(t, srv.URL)

	page, err := client.KV().GetPrefix(kvCtx(t), "quota", "quota:acme:", KVPrefixOptions{Limit: 50, After: "quota:acme:0", KeysOnly: true})
	if err != nil {
		t.Fatalf("getPrefix: %v", err)
	}
	req := srv.only(t)
	if req.Method != http.MethodPost || req.Path != "/api/v1/kv" {
		t.Fatalf("getPrefix went to %s %s", req.Method, req.Path)
	}
	// NEVER a query string (§5.5): `?prefix=quota:acme:` is recorded by the
	// broker's access log, the proxy's, the meter sample, the per-request-id
	// tracing and any ingress in front. A mitigation living in one component out
	// of four is not a mitigation.
	if req.RawQuery != "" {
		t.Fatalf("a prefix must never reach a URL, got query %q", req.RawQuery)
	}
	assertJSONBody(t, req.Body, `{"operations":[{"op":"getPrefix","ns":"quota","prefix":"quota:acme:","after":"quota:acme:0","limit":50,"keysOnly":true}]}`)

	if !page.Truncated || page.NextAfter != "quota:acme:1" {
		t.Errorf("page = %+v", page)
	}
}

func TestKVGetPrefixRequiresANonEmptyPrefix(t *testing.T) {
	// "A namespace is not a table to enumerate" (§5.5). The server says so too;
	// the client says it without spending a round trip.
	srv := newCaptureServer(t)
	client := newWireClient(t, srv.URL)

	if _, err := client.KV().GetPrefix(kvCtx(t), "quota", ""); err == nil {
		t.Fatal("an empty prefix must be refused")
	}
	if n := len(srv.requests()); n != 0 {
		t.Fatalf("got %d requests, want 0", n)
	}
}

// ---------------------------------------------------------------------------
// Batch, and the error envelope.
// ---------------------------------------------------------------------------

func TestKVBatchKeepsResultsIndexAligned(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`{"results":[
		{"index":0,"op":"put","applied":true,"key":"a","value":1,"version":1},
		{"index":1,"op":"get","found":true,"key":"b","value":2,"version":9},
		{"index":2,"op":"incr","applied":true,"key":"c","value":5,"version":2}
	]}`))
	client := newWireClient(t, srv.URL)

	results, err := client.KV().Batch(kvCtx(t),
		KVPutOp("ns", "a", 1, TTLSeconds(10)),
		KVGetOp("ns", "b"),
		KVIncrOp("ns", "c", 4, TTLSeconds(10)),
	)
	if err != nil {
		t.Fatalf("batch: %v", err)
	}
	assertJSONBody(t, srv.only(t).Body, `{"operations":[
		{"op":"put","ns":"ns","key":"a","value":1,"ttlSeconds":10},
		{"op":"get","ns":"ns","key":"b"},
		{"op":"incr","ns":"ns","key":"c","delta":4,"ttlSeconds":10}
	]}`)

	if len(results) != 3 {
		t.Fatalf("want 3 results, got %d", len(results))
	}
	// §6.4: results are index-aligned to the input array, in input order.
	for i, r := range results {
		if r.Index != i {
			t.Errorf("result %d carries index %d", i, r.Index)
		}
	}
	if !results[0].Write().Applied {
		t.Error("result 0 should be an applied write")
	}
	if !results[1].Entry().Found {
		t.Error("result 1 should be a found entry")
	}
	counter, err := results[2].Counter()
	if err != nil || counter.Value != 5 {
		t.Errorf("counter = %+v err=%v", counter, err)
	}
}

func TestKVBatchRefusesAnOpBuiltWithABadExpiry(t *testing.T) {
	srv := newCaptureServer(t)
	client := newWireClient(t, srv.URL)

	var unset Expiry
	_, err := client.KV().Batch(kvCtx(t), KVGetOp("ns", "a"), KVPutOp("ns", "b", 1, unset))
	if err == nil {
		t.Fatal("an op with a deferred construction error must fail the batch")
	}
	if n := len(srv.requests()); n != 0 {
		t.Fatalf("a rejected batch must not reach the network, got %d requests", n)
	}
}

func TestKVRequiredPreconditionIsATypedErrorOnTheStandaloneSurface(t *testing.T) {
	// `required:true` asks for the escalation: the whole call is rolled back and
	// the answer carries NO results array at all, just the verdict envelope --
	// at HTTP 200, because the transaction really did abort in SQL and this must
	// pollute neither the retry policies nor the error metrics of the broker.
	//
	// On THIS surface it is an error (the caller asked for the escalation, and
	// nothing was applied). Inside a transaction it is a RETURN, because there
	// the bundle's own outcome is the answer. Either way the data comes back
	// with it, so nobody has to parse a message.
	srv := newCaptureServer(t, okJSON(`{"failedIndex":0,"kvReason":"exists","ok":false,"reason":"kv_precondition","value":{"owner":"worker-1"},"version":2021}`))
	client := newWireClient(t, srv.URL)

	_, err := client.KV().PutIfAbsent(kvCtx(t), "saga", "s-1", 1, TTLSeconds(60), KVWriteOptions{Required: true})
	if err == nil {
		t.Fatal("a required precondition that lost must be an error on the standalone surface")
	}
	var pe *KVPreconditionError
	if !errors.As(err, &pe) {
		t.Fatalf("error is %T, want *KVPreconditionError: %v", err, err)
	}
	if pe.FailedIndex != 0 || pe.Reason != KVReasonExists || pe.Version != 2021 {
		t.Errorf("precondition = %+v", pe)
	}
	if string(pe.Value) != `{"owner":"worker-1"}` {
		t.Errorf("value = %s", string(pe.Value))
	}
}

func TestKVKillSwitchIsATypedError(t *testing.T) {
	// The surface exists on every cell that runs the broker: QUEEN_KV_ENABLED is
	// gone and nothing answers "not enabled here" any more. What an operator can
	// still do is PAUSE it at run time, and that is a 503 with `kv_disabled` in
	// both `error` and `reason` plus Retry-After — note `error` is the code on
	// this envelope, not `code`.
	//
	// This is a transient refusal, and the client's job is to hand it back
	// legibly, not to have anticipated it: the old shape of this test probed the
	// surface BEFORE the first real call, which is the habit that went away with
	// the boot flag.
	srv := newCaptureServer(t, cannedResponse{
		status: http.StatusServiceUnavailable,
		body:   `{"error":"kv_disabled","reason":"kv_disabled"}`,
		header: map[string]string{"Retry-After": "1"},
	})
	client := newWireClient(t, srv.URL)

	_, err := client.KV().Get(kvCtx(t), "orders", "k")
	if err == nil {
		t.Fatal("expected an error")
	}
	var se *SurfaceError
	if !errors.As(err, &se) {
		t.Fatalf("error is %T, want *SurfaceError: %v", err, err)
	}
	if se.Code != "kv_disabled" || se.Reason != "kv_disabled" {
		t.Errorf("code=%q reason=%q", se.Code, se.Reason)
	}
	if se.StatusCode != 503 {
		t.Errorf("status = %d, want 503: a paused surface is temporary, never a 404", se.StatusCode)
	}
	// The underlying *HTTPError stays reachable: nothing in this client hides
	// the status code or the raw body.
	var he *HTTPError
	if !errors.As(err, &he) || he.StatusCode != 503 {
		t.Errorf("the HTTPError must remain unwrappable, got %v", err)
	}
}

func TestKVBadRequestIsATypedErrorWithTheServersReason(t *testing.T) {
	srv := newCaptureServer(t, cannedResponse{
		status: http.StatusBadRequest,
		body:   `{"error":"kv_bad_request","reason":"kv_expiry_not_specified","detail":"op at index 0: exactly one of ttlSeconds (integer > 0) and forever:true is required, got 0"}`,
	})
	client := newWireClient(t, srv.URL)

	_, err := client.KV().Batch(kvCtx(t), KVGetOp("ns", "a"))
	var se *SurfaceError
	if !errors.As(err, &se) {
		t.Fatalf("error is %T, want *SurfaceError: %v", err, err)
	}
	// Clients branch on the code, never on prose (§13.5).
	if se.Reason != "kv_expiry_not_specified" || se.StatusCode != 400 {
		t.Errorf("reason=%q status=%d", se.Reason, se.StatusCode)
	}
}

func TestKVValueIsMarshalledOnceAndNullIsLegal(t *testing.T) {
	// `"value": null` is a legal value and an ABSENT value is not: the server
	// says so in as many words. A Go client that dropped a nil value would turn
	// a legal write into a 400 nobody can read.
	srv := newCaptureServer(t, okJSON(`{"index":0,"op":"put","applied":true,"key":"k","value":null,"version":1}`))
	client := newWireClient(t, srv.URL)

	if _, err := client.KV().Put(kvCtx(t), "orders", "k", nil, TTLSeconds(5)); err != nil {
		t.Fatalf("put null: %v", err)
	}
	assertJSONBody(t, srv.only(t).Body, `{"value":null,"ttlSeconds":5}`)
}

func TestKVRawValueSurvivesUnchanged(t *testing.T) {
	// A caller who already has JSON must be able to hand it over without a
	// re-encode: json.RawMessage goes to the wire byte for byte.
	srv := newCaptureServer(t, okJSON(`{"index":0,"op":"put","applied":true,"key":"k","value":1,"version":1}`))
	client := newWireClient(t, srv.URL)

	raw := json.RawMessage(`{"big":9007199254740993}`)
	if _, err := client.KV().Put(kvCtx(t), "orders", "k", raw, TTLSeconds(5)); err != nil {
		t.Fatalf("put raw: %v", err)
	}
	assertJSONBody(t, srv.only(t).Body, `{"value":{"big":9007199254740993},"ttlSeconds":5}`)
}

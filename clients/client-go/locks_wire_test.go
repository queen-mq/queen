package queen

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Locks: the wire of POST /api/v1/locks, the `check` KV op, and what the lock
// handle does around them -- against the plan server, no broker.
//
// Same method and same reason as kv_wire_test.go: the body is the contract.
//
// What lives in the CLIENT and nowhere else, and is pinned here:
//   - the handle always sends its owner, the same one on every call;
//   - a renew's NEW token replaces the old one for the guard and the release;
//   - a guard that lost to the handle's own renewal is sent again with the new
//     token, and one that lost to another holder is the verdict and marks the
//     lock lost;
//   - a transaction that asked for a guard never goes out without one.

func guardJSON(name string, slot int, token int64) string {
	return fmt.Sprintf(`{"op":"check","ns":"queen-locks","key":"%s#%d","expect":%d,"required":true}`, name, slot, token)
}

func grantedJSON(name string, slot int, token int64) cannedResponse {
	return okJSON(fmt.Sprintf(`{"results":[{"index":0,"op":"acquire","name":"%s","acquired":true,"slot":%d,"token":%d,"owner":"o","guard":%s}]}`,
		name, slot, token, guardJSON(name, slot, token)))
}

func refusedJSON(name string) cannedResponse {
	return okJSON(fmt.Sprintf(`{"results":[{"index":0,"op":"acquire","name":"%s","acquired":false,"reason":"held","holders":[{"slot":0,"owner":"other"}]}]}`, name))
}

func renewedJSON(name string, slot int, token int64) cannedResponse {
	return okJSON(fmt.Sprintf(`{"results":[{"index":0,"op":"renew","name":"%s","renewed":true,"slot":%d,"token":%d,"guard":%s}]}`,
		name, slot, token, guardJSON(name, slot, token)))
}

func releasedJSON(name string) cannedResponse {
	return okJSON(fmt.Sprintf(`{"results":[{"index":0,"op":"release","name":"%s","released":true,"slot":0}]}`, name))
}

func committedJSON() cannedResponse {
	return okJSON(`{"transactionId":"t","success":true,"results":[]}`)
}

func lostToJSON(failedIndex int, kvReason, value string, version int64) cannedResponse {
	return okJSON(fmt.Sprintf(`{"transactionId":"t","success":false,"reason":"kv_precondition","error":"QKV","results":[],"ok":false,"failedIndex":%d,"kvReason":"%s","value":%s,"version":%d}`,
		failedIndex, kvReason, value, version))
}

// opOf is the first operation of a locks (or kv) request body, as a map.
func opOf(t *testing.T, r capturedRequest) map[string]interface{} {
	t.Helper()
	var body struct {
		Operations []map[string]interface{} `json:"operations"`
	}
	if err := json.Unmarshal(r.Body, &body); err != nil || len(body.Operations) == 0 {
		t.Fatalf("not an operations body: %s", r.Body)
	}
	return body.Operations[0]
}

func manual() LockOptions { return LockOptions{ManualRenew: true} }

func TestLockOperationsSendExactlyTheirFields(t *testing.T) {
	cs := newCaptureServer(t,
		grantedJSON("daily-report", 0, 100),
		renewedJSON("gpu", 2, 101),
		releasedJSON("daily-report"),
		okJSON(`{"results":[{"index":0,"op":"get","name":"daily-report","held":false,"holders":[]}]}`),
	)
	locks := newWireClient(t, cs.URL).Locks()
	ctx := context.Background()

	a, err := locks.Send(ctx, LockAcquireOp("daily-report", 30*time.Second).WithOwner("o"))
	if err != nil || !a.Acquired || a.Token != 100 || a.Guard == nil {
		t.Fatalf("acquire: %+v, %v", a, err)
	}
	r, err := locks.Send(ctx, LockRenewOp("gpu", 100, 1500*time.Millisecond).WithSlot(2).WithOwner("o"))
	if err != nil || !r.Renewed || r.Token != 101 {
		t.Fatalf("renew must answer a NEW token: %+v, %v", r, err)
	}
	d, err := locks.Send(ctx, LockReleaseOp("daily-report", 101))
	if err != nil || !d.Released {
		t.Fatalf("release: %+v, %v", d, err)
	}
	g, err := locks.Get(ctx, "daily-report")
	if err != nil || g.Held {
		t.Fatalf("get: %+v, %v", g, err)
	}

	reqs := cs.requests()
	want := []string{
		`{"operations":[{"op":"acquire","name":"daily-report","ttlSeconds":30,"owner":"o"}]}`,
		// 1.5s is rounded UP to 2: a lifetime rounded down could end early.
		`{"operations":[{"op":"renew","name":"gpu","ttlSeconds":2,"owner":"o","slot":2,"token":100}]}`,
		`{"operations":[{"op":"release","name":"daily-report","token":101}]}`,
		`{"operations":[{"op":"get","name":"daily-report"}]}`,
	}
	for i, w := range want {
		if reqs[i].Method != http.MethodPost || reqs[i].Path != "/api/v1/locks" {
			t.Fatalf("request %d went to %s %s", i, reqs[i].Method, reqs[i].Path)
		}
		assertJSONBody(t, reqs[i].Body, w)
	}
}

func TestAHeldLockIsAVerdictNeverAnError(t *testing.T) {
	cs := newCaptureServer(t, refusedJSON("job"), refusedJSON("job"))
	client := newWireClient(t, cs.URL)
	r, err := client.Locks().Send(context.Background(), LockAcquireOp("job", 30*time.Second))
	if err != nil {
		t.Fatalf("held is a verdict: %v", err)
	}
	if r.Acquired || r.Reason != LockReasonHeld || len(r.Holders) != 1 || r.Holders[0].Owner != "other" {
		t.Fatalf("%+v", r)
	}
	lock := client.Lock("job", 30*time.Second, manual())
	ok, err := lock.TryAcquire(context.Background())
	if err != nil || ok || lock.Held() || lock.Token() != 0 || lock.Slot() != -1 {
		t.Fatalf("ok=%v err=%v held=%v", ok, err, lock.Held())
	}
	if _, err := lock.Guard(); !errors.Is(err, ErrLockNotHeld) {
		t.Fatalf("a guard of an unheld lock: %v", err)
	}
}

func TestWhatTheBrokerWouldRefuseIsRefusedBeforeTheRequest(t *testing.T) {
	cs := newCaptureServer(t)
	locks := newWireClient(t, cs.URL).Locks()
	ctx := context.Background()
	for name, op := range map[string]LockOp{
		"no lifetime":      LockAcquireOp("a", 0),
		"renew, no token":  LockRenewOp("a", 0, time.Second),
		"release no token": LockReleaseOp("a", 0),
	} {
		if _, err := locks.Send(ctx, op); err == nil {
			t.Errorf("%s was sent", name)
		}
	}
	if _, err := locks.Batch(ctx); err == nil {
		t.Error("an empty call was sent")
	}
	if n := len(cs.requests()); n != 0 {
		t.Fatalf("%d requests left the client", n)
	}
}

func TestTheHandleNamesItsOwnerAndFollowsARenew(t *testing.T) {
	cs := newCaptureServer(t, grantedJSON("job", 0, 100), renewedJSON("job", 0, 101), releasedJSON("job"))
	client := newWireClient(t, cs.URL)
	ctx := context.Background()
	lock := client.Lock("job", 30*time.Second, manual())
	if other := client.Lock("job", 30*time.Second); other.Owner() == lock.Owner() {
		t.Fatal("two handles share an owner")
	}
	if got := client.Lock("job", time.Second, LockOptions{Owner: "cron-7"}).Owner(); got != "cron-7" {
		t.Fatalf("owner %q", got)
	}

	if ok, err := lock.TryAcquire(ctx); err != nil || !ok {
		t.Fatalf("acquire: %v %v", ok, err)
	}
	if !lock.Held() || lock.Token() != 100 || lock.Slot() != 0 {
		t.Fatalf("held=%v token=%d slot=%d", lock.Held(), lock.Token(), lock.Slot())
	}
	if ok, _ := lock.TryAcquire(ctx); !ok || len(cs.requests()) != 1 {
		t.Fatal("already held: no call")
	}
	if ok, err := lock.Renew(ctx); err != nil || !ok || lock.Token() != 101 {
		t.Fatalf("renew: %v %v token=%d", ok, err, lock.Token())
	}
	guard, err := lock.Guard()
	if err != nil {
		t.Fatal(err)
	}
	gb, _ := json.Marshal(guard)
	assertJSONBody(t, gb, guardJSON("job", 0, 101))

	if ok, err := lock.Release(ctx); err != nil || !ok {
		t.Fatalf("release: %v %v", ok, err)
	}
	select {
	case <-lock.Lost():
		t.Fatal("a release is not a loss")
	default:
	}
	if ok, _ := lock.Release(ctx); ok || lock.Held() {
		t.Fatal("nothing left to give back")
	}

	reqs := cs.requests()
	if len(reqs) != 3 {
		t.Fatalf("%d requests", len(reqs))
	}
	assertJSONBody(t, reqs[0].Body, fmt.Sprintf(`{"operations":[{"op":"acquire","name":"job","ttlSeconds":30,"owner":%q}]}`, lock.Owner()))
	assertJSONBody(t, reqs[1].Body, fmt.Sprintf(`{"operations":[{"op":"renew","name":"job","ttlSeconds":30,"owner":%q,"token":100}]}`, lock.Owner()))
	assertJSONBody(t, reqs[2].Body, `{"operations":[{"op":"release","name":"job","token":101}]}`)
}

func TestASemaphoreHandleSendsItsLimitAndKeepsItsSlot(t *testing.T) {
	cs := newCaptureServer(t, grantedJSON("gpu", 3, 9), okJSON(`{"results":[{"index":0,"op":"release","name":"gpu","released":true,"slot":3}]}`))
	client := newWireClient(t, cs.URL)
	ctx := context.Background()
	permit := client.Semaphore("gpu", 4, time.Minute, manual())
	if ok, err := permit.TryAcquire(ctx); err != nil || !ok || permit.Slot() != 3 {
		t.Fatalf("%v %v slot=%d", ok, err, permit.Slot())
	}
	if _, err := permit.Release(ctx); err != nil {
		t.Fatal(err)
	}
	reqs := cs.requests()
	if opOf(t, reqs[0])["limit"] != float64(4) {
		t.Fatalf("limit not sent: %s", reqs[0].Body)
	}
	assertJSONBody(t, reqs[1].Body, `{"operations":[{"op":"release","name":"gpu","slot":3,"token":9}]}`)
}

func TestAcquireWaitsAsLongAsItsContext(t *testing.T) {
	cs := newCaptureServer(t, refusedJSON("job"), refusedJSON("job"), grantedJSON("job", 0, 5))
	client := newWireClient(t, cs.URL)
	fast := LockOptions{ManualRenew: true, RetryMin: 5 * time.Millisecond, RetryMax: 10 * time.Millisecond}
	lock := client.Lock("job", 30*time.Second, fast)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := lock.Acquire(ctx); err != nil || !lock.Held() {
		t.Fatalf("acquire: %v", err)
	}
	if n := len(cs.requests()); n != 3 {
		t.Fatalf("%d attempts", n)
	}

	cs = newCaptureServer(t)
	cs.fallback = refusedJSON("job")
	lock = newWireClient(t, cs.URL).Lock("job", 30*time.Second, fast)
	ctx, cancel = context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	if err := lock.Acquire(ctx); !errors.Is(err, context.DeadlineExceeded) || lock.Held() {
		t.Fatalf("the wait ended first: %v", err)
	}
	if n := len(cs.requests()); n < 2 {
		t.Fatalf("it came back while it waited: %d", n)
	}
}

// answering is a server that answers by what it is asked, for the tests where
// the ORDER of two calls in flight is the point.
func answering(t *testing.T, answer func(path string, op map[string]interface{}) string) (*httptest.Server, func() []capturedRequest) {
	t.Helper()
	var mu sync.Mutex
	var hits []capturedRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		hits = append(hits, capturedRequest{Method: r.Method, Path: r.URL.Path, Body: body})
		mu.Unlock()
		var parsed struct {
			Operations []map[string]interface{} `json:"operations"`
		}
		_ = json.Unmarshal(body, &parsed)
		var op map[string]interface{}
		if len(parsed.Operations) > 0 && !strings.HasSuffix(r.URL.Path, "/transaction") {
			op = parsed.Operations[0]
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(answer(r.URL.Path, op)))
	}))
	t.Cleanup(srv.Close)
	return srv, func() []capturedRequest {
		mu.Lock()
		defer mu.Unlock()
		return append([]capturedRequest(nil), hits...)
	}
}

func TestItRenewsInTheBackgroundAndARefusedRenewIsALoss(t *testing.T) {
	var renews atomic.Int64
	srv, hits := answering(t, func(_ string, op map[string]interface{}) string {
		switch op["op"] {
		case "acquire":
			return grantedJSON("job", 0, 1).body
		case "renew":
			n := renews.Add(1)
			if n <= 2 {
				return renewedJSON("job", 0, 1+n).body
			}
			return `{"results":[{"index":0,"op":"renew","name":"job","renewed":false,"reason":"lost","slot":0,"holders":[{"slot":0,"owner":"other"}]}]}`
		}
		return releasedJSON("job").body
	})
	lock := newWireClient(t, srv.URL).Lock("job", 5*time.Second, LockOptions{RenewEvery: 40 * time.Millisecond})
	if ok, err := lock.TryAcquire(context.Background()); err != nil || !ok {
		t.Fatalf("%v %v", ok, err)
	}
	lost := lock.Lost()
	select {
	case <-lost:
	case <-time.After(5 * time.Second):
		t.Fatal("the handle never reported the loss")
	}
	if lock.Held() {
		t.Fatal("a lost lock is not held")
	}
	var tokens []float64
	for _, h := range hits()[1:] {
		tokens = append(tokens, opOf(t, h)["token"].(float64))
	}
	if len(tokens) != 3 || tokens[0] != 1 || tokens[1] != 2 || tokens[2] != 3 {
		t.Fatalf("each renew carries the token of the one before: %v", tokens)
	}
	seen := len(hits())
	time.Sleep(150 * time.Millisecond)
	if len(hits()) != seen {
		t.Fatal("a lost lock renews no more")
	}
}

func TestALifetimeThatRunsOutHereIsALoss(t *testing.T) {
	cs := newCaptureServer(t, grantedJSON("job", 0, 1))
	lock := newWireClient(t, cs.URL).Lock("job", time.Second, manual())
	if ok, err := lock.TryAcquire(context.Background()); err != nil || !ok {
		t.Fatalf("%v %v", ok, err)
	}
	select {
	case <-lock.Lost():
	case <-time.After(3 * time.Second):
		t.Fatal("past its deadline the handle reports the loss")
	}
	if lock.Held() || len(cs.requests()) != 1 {
		t.Fatalf("held=%v requests=%d", lock.Held(), len(cs.requests()))
	}
}

// ---------------------------------------------------------------------------
// check, and the guard on a transaction
// ---------------------------------------------------------------------------

func TestCheckSendsItsVersionAndAnswersAVerdict(t *testing.T) {
	cs := newCaptureServer(t,
		okJSON(`{"results":[{"index":0,"op":"check","applied":true,"key":"k","version":7}]}`),
		okJSON(`{"results":[{"index":0,"op":"check","applied":false,"reason":"version","key":"k","value":{"n":2},"version":9}]}`),
		okJSON(`{"results":[{"index":0,"op":"check","applied":true,"key":"k","version":0}]}`),
	)
	kv := newWireClient(t, cs.URL).KV()
	ctx := context.Background()
	held, err := kv.Check(ctx, "orders", "k", 7)
	if err != nil || !held.Applied || held.Version != 7 {
		t.Fatalf("%+v %v", held, err)
	}
	stale, err := kv.Check(ctx, "orders", "k", 7)
	if err != nil || stale.Applied || stale.Reason != KVReasonVersion || stale.Version != 9 {
		t.Fatalf("%+v %v", stale, err)
	}
	if _, err := kv.Check(ctx, "orders", "k", 0); err != nil {
		t.Fatal(err)
	}
	reqs := cs.requests()
	assertJSONBody(t, reqs[0].Body, `{"operations":[{"op":"check","ns":"orders","key":"k","expect":7}]}`)
	// expect:0 is "must not exist" and must reach the wire as 0.
	assertJSONBody(t, reqs[2].Body, `{"operations":[{"op":"check","ns":"orders","key":"k","expect":0}]}`)

	b, _ := json.Marshal(KVCheckOp("queen-locks", "job#0", 41, KVWriteOptions{Required: true}))
	assertJSONBody(t, b, guardJSON("job", 0, 41))
	if _, err := kv.Check(ctx, "orders", "k", -1); err == nil {
		t.Fatal("a negative version was sent")
	}
}

func TestTheGuardIsTheFirstKVOpAtTheTokenHeldWhenCommitSends(t *testing.T) {
	cs := newCaptureServer(t, grantedJSON("job", 0, 100), renewedJSON("job", 0, 101), committedJSON())
	client := newWireClient(t, cs.URL)
	ctx := context.Background()
	lock := client.Lock("job", 30*time.Second, manual())
	if ok, err := lock.TryAcquire(ctx); err != nil || !ok {
		t.Fatal(err)
	}
	txn := client.Transaction().
		Guard(lock).
		KV(KVPutOp("work", "state", map[string]int{"n": 1}, Forever()))
	if _, err := lock.Renew(ctx); err != nil { // after the guard was asked for, before commit
		t.Fatal(err)
	}
	resp, err := txn.Commit(ctx)
	if err != nil || !resp.Success {
		t.Fatalf("%+v %v", resp, err)
	}
	var body struct {
		KV []json.RawMessage `json:"kv"`
	}
	req := cs.requests()[2]
	if req.Path != "/api/v1/transaction" || json.Unmarshal(req.Body, &body) != nil || len(body.KV) != 2 {
		t.Fatalf("%s %s", req.Path, req.Body)
	}
	assertJSONBody(t, body.KV[0], guardJSON("job", 0, 101))
	assertJSONBody(t, body.KV[1], `{"op":"put","ns":"work","key":"state","value":{"n":1},"forever":true}`)
}

func TestAGuardThatLostToItsOwnRenewalIsSentAgainWithTheNewToken(t *testing.T) {
	// The race, in the order that makes it: the commit goes out with token 100;
	// the lock's renewal is applied while the commit is on its way; the broker
	// judges the commit against token 101 and names this owner's row.
	var owner atomic.Value
	var commits atomic.Int64
	srv, hits := answering(t, func(path string, op map[string]interface{}) string {
		if strings.HasSuffix(path, "/transaction") {
			if commits.Add(1) == 1 {
				time.Sleep(200 * time.Millisecond) // held while the renew overtakes it
				return lostToJSON(0, "version", fmt.Sprintf(`{"owner":%q}`, owner.Load()), 101).body
			}
			return committedJSON().body
		}
		switch op["op"] {
		case "acquire":
			owner.Store(op["owner"])
			return grantedJSON("job", 0, 100).body
		case "renew":
			return renewedJSON("job", 0, 101).body
		}
		return releasedJSON("job").body
	})
	client := newWireClient(t, srv.URL)
	ctx := context.Background()
	lock := client.Lock("job", 30*time.Second, manual())
	if ok, err := lock.TryAcquire(ctx); err != nil || !ok {
		t.Fatal(err)
	}
	type result struct {
		resp *TransactionResponse
		err  error
	}
	done := make(chan result, 1)
	go func() {
		resp, err := client.Transaction().
			Guard(lock).
			KV(KVPutOp("work", "state", 1, Forever())).
			Commit(ctx)
		done <- result{resp, err}
	}()
	time.Sleep(50 * time.Millisecond)
	if ok, err := lock.Renew(ctx); err != nil || !ok {
		t.Fatalf("renew: %v %v", ok, err)
	}
	r := <-done
	if r.err != nil || !r.resp.Success {
		t.Fatalf("the step commits on the second send: %+v %v", r.resp, r.err)
	}
	var expects []float64
	for _, h := range hits() {
		if strings.HasSuffix(h.Path, "/transaction") {
			var body struct {
				KV []map[string]interface{} `json:"kv"`
			}
			_ = json.Unmarshal(h.Body, &body)
			expects = append(expects, body.KV[0]["expect"].(float64))
		}
	}
	if len(expects) != 2 || expects[0] != 100 || expects[1] != 101 {
		t.Fatalf("guards sent: %v", expects)
	}
	if !lock.Held() {
		t.Fatal("the lock is still held")
	}
}

func TestAGuardThatLostToAnotherHolderIsTheVerdictAndTheLockIsLost(t *testing.T) {
	cs := newCaptureServer(t, grantedJSON("job", 0, 100), lostToJSON(0, "version", `{"owner":"somebody-else"}`, 250))
	client := newWireClient(t, cs.URL)
	ctx := context.Background()
	lock := client.Lock("job", 30*time.Second, manual())
	if ok, err := lock.TryAcquire(ctx); err != nil || !ok {
		t.Fatal(err)
	}
	lost := lock.Lost()
	resp, err := client.Transaction().Guard(lock).KV(KVPutOp("w", "k", 1, Forever())).Commit(ctx)
	if err != nil || !resp.IsKVPrecondition() {
		t.Fatalf("returned, not raised: %+v %v", resp, err)
	}
	select {
	case <-lost:
	default:
		t.Fatal("the handle did not report the loss")
	}
	if lock.Held() || len(cs.requests()) != 2 {
		t.Fatalf("held=%v requests=%d (not sent again)", lock.Held(), len(cs.requests()))
	}
}

func TestAPreconditionThatIsNotTheGuardsLeavesTheLockAlone(t *testing.T) {
	// kv = [guard, marker]: flat index 1 is the bundle's own gate.
	cs := newCaptureServer(t, grantedJSON("job", 0, 100), lostToJSON(1, "exists", `true`, 77))
	client := newWireClient(t, cs.URL)
	ctx := context.Background()
	lock := client.Lock("job", 30*time.Second, manual())
	if ok, err := lock.TryAcquire(ctx); err != nil || !ok {
		t.Fatal(err)
	}
	resp, err := client.Transaction().
		Guard(lock).
		KV(KVPutIfAbsentOp("idem", "order-1", true, TTLSeconds(3600), KVWriteOptions{Required: true})).
		Commit(ctx)
	if err != nil || !resp.IsKVPrecondition() {
		t.Fatalf("%+v %v", resp, err)
	}
	if !lock.Held() {
		t.Fatal("the marker lost, not the lock")
	}
}

func TestAStepThatAskedForAGuardNeverGoesOutWithoutOne(t *testing.T) {
	cs := newCaptureServer(t, committedJSON())
	client := newWireClient(t, cs.URL)
	lock := client.Lock("job", 30*time.Second)
	_, err := client.Transaction().Guard(lock).KV(KVPutOp("w", "k", 1, Forever())).Commit(context.Background())
	if !errors.Is(err, ErrLockNotHeld) {
		t.Fatalf("an unheld lock cannot guard: %v", err)
	}
	if n := len(cs.requests()); n != 0 {
		t.Fatalf("%d requests left the client", n)
	}
}

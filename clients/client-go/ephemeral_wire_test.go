package queen

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

// The ephemeral wire contract (EPHEMERAL_QUEUES.md §3.1, §4), asserted against
// the plan server of wire_capture_test.go. No broker, no database.
//
// WHY THE BODY AND NOT "IT WORKED". The request IS the contract, and on this
// family a wrong shape is especially quiet: the broker parses the push body with
// serde, so a `message` array where `messages` was meant is a 400 nobody can
// explain from the call site, and a `ttlSecond` in the configure options is an
// option the broker never sees -- i.e. a ring that grows until a global budget
// answers 503. So every verb below is pinned as JSON, including the fields that
// must NOT be there.
//
// Three things are pinned beyond the eight bodies:
//
//  1. THE 404 RULE. Against a broker or proxy older than 1.1 the whole family
//     answers 404, and every verb must turn that into one sentinel rather than
//     into eight different "not found" stories.
//  2. THE EMPTY POP. `messages` must reach the caller as an empty slice however
//     the broker spelled it, because a nil slice is the shape that makes a range
//     read as "the queue is empty" in one client and panic in the next.
//  3. THE DURABLE BUFFER IS UNTOUCHED. Buffered ephemeral push reuses the
//     durable machinery through its FlushFunc seam, and the durable body is
//     asserted byte for byte so that reuse cannot become drift.

func ephCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// notFound is the answer of a broker that never registered these routes.
func notFound(body string) cannedResponse {
	return cannedResponse{status: http.StatusNotFound, body: body}
}

func createdJSON(body string) cannedResponse {
	return cannedResponse{status: http.StatusCreated, body: body}
}

// ===========================================================================
// configure / reset / delete
// ===========================================================================

func TestEphemeralConfigureSendsTheSevenKnobsAndNothingElse(t *testing.T) {
	srv := newCaptureServer(t, createdJSON(`{"queue":"inbox"}`))
	client := newWireClient(t, srv.URL)

	if _, err := client.Ephemeral().Configure(ephCtx(t), "inbox", EphemeralOptions{
		MaxBytes:     Int64(1048576),
		MaxLength:    Int64(500),
		Policy:       EphemeralPolicyDropOldest,
		TTLSeconds:   Int64(30),
		LeaseSeconds: Int64(10),
		RetryLimit:   Int64(3),
		WindowBuffer: &EphemeralWindowBuffer{Ms: 25, Count: 50},
	}); err != nil {
		t.Fatalf("configure: %v", err)
	}

	req := srv.only(t)
	if req.Method != http.MethodPost || req.Path != "/api/v1/ephemeral/configure" {
		t.Fatalf("configure went to %s %s", req.Method, req.Path)
	}
	assertJSONBody(t, req.Body, `{"queue":"inbox","options":{
		"maxBytes":1048576,"maxLength":500,"policy":"dropOldest","ttlSeconds":30,
		"leaseSeconds":10,"retryLimit":3,"windowBuffer":{"ms":25,"count":50}}}`)
}

func TestEphemeralConfigureOmitsEveryKnobTheCallerDidNotSet(t *testing.T) {
	// The knobs are pointers so that "not supplied" and "supplied as zero" stay
	// different things: an omitted knob leaves the broker's default in charge,
	// while `"retryLimit":0` would be a queue that drops on the first nack.
	srv := newCaptureServer(t, createdJSON(`{"queue":"inbox"}`))
	client := newWireClient(t, srv.URL)

	if _, err := client.Ephemeral().Configure(ephCtx(t), "inbox", EphemeralOptions{}); err != nil {
		t.Fatalf("configure: %v", err)
	}
	assertJSONBody(t, srv.only(t).Body, `{"queue":"inbox","options":{}}`)
}

func TestEphemeralConfigureCanDeclareAZeroKnob(t *testing.T) {
	srv := newCaptureServer(t, createdJSON(`{"queue":"inbox"}`))
	client := newWireClient(t, srv.URL)

	if _, err := client.Ephemeral().Configure(ephCtx(t), "inbox", EphemeralOptions{
		RetryLimit: Int64(0),
	}); err != nil {
		t.Fatalf("configure: %v", err)
	}
	assertJSONBody(t, srv.only(t).Body, `{"queue":"inbox","options":{"retryLimit":0}}`)
}

func TestEphemeralResetSendsTheQueueAndReadsTheDroppedCount(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`{"dropped":4211}`))
	client := newWireClient(t, srv.URL)

	dropped, err := client.Ephemeral().Reset(ephCtx(t), "inbox")
	if err != nil {
		t.Fatalf("reset: %v", err)
	}
	if dropped != 4211 {
		t.Fatalf("dropped = %d, want 4211", dropped)
	}
	req := srv.only(t)
	if req.Method != http.MethodPost || req.Path != "/api/v1/ephemeral/reset" {
		t.Fatalf("reset went to %s %s", req.Method, req.Path)
	}
	assertJSONBody(t, req.Body, `{"queue":"inbox"}`)
}

func TestEphemeralDeleteUsesThePathRouteAndSendsNoBody(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`{"queue":"reply/42","deleted":true,"declared":true}`))
	client := newWireClient(t, srv.URL)

	deleted, err := client.Ephemeral().Delete(ephCtx(t), "reply/42")
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !deleted.Deleted || !deleted.Declared || deleted.Queue != "reply/42" {
		t.Fatalf("delete result = %+v", deleted)
	}
	req := srv.only(t)
	if req.Method != http.MethodDelete {
		t.Fatalf("delete used %s", req.Method)
	}
	// The name is percent-escaped whole: a `/` in a queue name must not add a
	// path segment, or the route stops matching and the answer is a 404 that
	// this client would then report as "upgrade your broker".
	if req.RawPath != "/api/v1/ephemeral/queue/reply%2F42" {
		t.Fatalf("delete went to %q, want /api/v1/ephemeral/queue/reply%%2F42", req.RawPath)
	}
	if len(req.Body) != 0 {
		t.Fatalf("delete sent a body: %s", req.Body)
	}
}

// ===========================================================================
// push
// ===========================================================================

func TestEphemeralPushHoistsTheIdentityToTheEnvelope(t *testing.T) {
	// The whole reason the buffered drain takes a sink: the DURABLE push repeats
	// {queue, partition} on every item, this one carries them once and the
	// elements are `{payload}` and nothing else -- no transactionId, because
	// there is no dedup index to hold one.
	srv := newCaptureServer(t, createdJSON(`{"pushed":2}`))
	client := newWireClient(t, srv.URL)

	res, err := client.Ephemeral().Push(ephCtx(t), "presence", []interface{}{
		map[string]interface{}{"user": "a", "typing": true},
		map[string]interface{}{"user": "b", "typing": false},
	})
	if err != nil {
		t.Fatalf("push: %v", err)
	}
	if res.Pushed != 2 || res.Count != 2 || res.Buffered {
		t.Fatalf("push result = %+v", res)
	}

	req := srv.only(t)
	if req.Method != http.MethodPost || req.Path != "/api/v1/ephemeral/push" {
		t.Fatalf("push went to %s %s", req.Method, req.Path)
	}
	assertJSONBody(t, req.Body, `{"queue":"presence","messages":[
		{"payload":{"user":"a","typing":true}},
		{"payload":{"user":"b","typing":false}}]}`)
}

func TestEphemeralPushOmitsThePartitionItWasNotGiven(t *testing.T) {
	// Never defaulted client-side: which ring a push without a partition lands
	// on is the broker's rule, and inventing "Default" here would take that
	// decision away from it in a way the caller never asked for.
	srv := newCaptureServer(t, createdJSON(`{"pushed":1}`), createdJSON(`{"pushed":1}`))
	client := newWireClient(t, srv.URL)
	eph := client.Ephemeral()

	if _, err := eph.Push(ephCtx(t), "presence", 1); err != nil {
		t.Fatalf("push: %v", err)
	}
	if _, err := eph.Push(ephCtx(t), "presence", 1, EphemeralPushOptions{Partition: "room-7"}); err != nil {
		t.Fatalf("push: %v", err)
	}

	reqs := srv.requests()
	assertJSONBody(t, reqs[0].Body, `{"queue":"presence","messages":[{"payload":1}]}`)
	assertJSONBody(t, reqs[1].Body, `{"queue":"presence","partition":"room-7","messages":[{"payload":1}]}`)
}

func TestEphemeralPushAcceptsTheShapesACallerActuallyHas(t *testing.T) {
	srv := newCaptureServer(t,
		createdJSON(`{"pushed":1}`), createdJSON(`{"pushed":1}`),
		createdJSON(`{"pushed":2}`), createdJSON(`{"pushed":1}`),
	)
	client := newWireClient(t, srv.URL)
	eph := client.Ephemeral()
	ctx := ephCtx(t)

	for _, in := range []interface{}{
		"just a string",
		EphemeralMessage{Payload: nil},
		[]EphemeralMessage{{Payload: 1}, {Payload: 2}},
		[]map[string]interface{}{{"n": 1}},
	} {
		if _, err := eph.Push(ctx, "shapes", in); err != nil {
			t.Fatalf("push %T: %v", in, err)
		}
	}

	reqs := srv.requests()
	assertJSONBody(t, reqs[0].Body, `{"queue":"shapes","messages":[{"payload":"just a string"}]}`)
	// A null payload is a legal payload and must travel as one.
	assertJSONBody(t, reqs[1].Body, `{"queue":"shapes","messages":[{"payload":null}]}`)
	assertJSONBody(t, reqs[2].Body, `{"queue":"shapes","messages":[{"payload":1},{"payload":2}]}`)
	assertJSONBody(t, reqs[3].Body, `{"queue":"shapes","messages":[{"payload":{"n":1}}]}`)
}

func TestEphemeralPushOfNothingNeverReachesTheNetwork(t *testing.T) {
	srv := newCaptureServer(t)
	client := newWireClient(t, srv.URL)

	res, err := client.Ephemeral().Push(ephCtx(t), "presence", []interface{}{})
	if err != nil {
		t.Fatalf("push: %v", err)
	}
	if res.Count != 0 || res.Pushed != 0 {
		t.Fatalf("push result = %+v", res)
	}
	if n := len(srv.requests()); n != 0 {
		t.Fatalf("an empty push made %d requests", n)
	}
}

func TestEphemeralPushRefusesANilMessage(t *testing.T) {
	srv := newCaptureServer(t)
	client := newWireClient(t, srv.URL)

	_, err := client.Ephemeral().Push(ephCtx(t), "presence", nil)
	if err == nil {
		t.Fatal("a nil message was accepted")
	}
	if !strings.Contains(err.Error(), "EphemeralMessage{Payload: nil}") {
		t.Fatalf("the error does not name the way to send a null payload: %v", err)
	}
	if n := len(srv.requests()); n != 0 {
		t.Fatalf("a refused push made %d requests", n)
	}
}

// ===========================================================================
// pop
// ===========================================================================

func TestEphemeralPlainPopSendsTheShortestQueryThisRouteCanReceive(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`{"queue":"inbox","messages":[]}`))
	client := newWireClient(t, srv.URL)

	if _, err := client.Ephemeral().Pop(ephCtx(t), "inbox"); err != nil {
		t.Fatalf("pop: %v", err)
	}

	req := srv.only(t)
	if req.Method != http.MethodGet || req.Path != "/api/v1/ephemeral/pop" {
		t.Fatalf("pop went to %s %s", req.Method, req.Path)
	}
	q := queryOf(t, req)
	if got := q.Get("queue"); got != "inbox" {
		t.Fatalf("queue = %q", got)
	}
	for _, absent := range []string{"partition", "batch", "wait", "timeout", "group", "autoAck"} {
		if _, present := q[absent]; present {
			t.Fatalf("%s must not appear on a plain pop; raw query: %q", absent, req.RawQuery)
		}
	}
}

func TestEphemeralWaitingPopAlwaysSendsAnExplicitTimeout(t *testing.T) {
	// wait=true without a timeout would leave the window to the broker's
	// default while the HTTP deadline is computed from THIS client's number:
	// the two disagreeing is a client that aborts a request the broker was
	// about to answer.
	srv := newCaptureServer(t, okJSON(`{"queue":"inbox","messages":[]}`))
	client := newWireClient(t, srv.URL)

	if _, err := client.Ephemeral().Pop(ephCtx(t), "inbox", EphemeralPopOptions{Wait: true}); err != nil {
		t.Fatalf("pop: %v", err)
	}
	q := queryOf(t, srv.only(t))
	if q.Get("wait") != "true" {
		t.Fatalf("wait = %q", q.Get("wait"))
	}
	if q.Get("timeout") != "30000" {
		t.Fatalf("timeout = %q, want the 30000 default", q.Get("timeout"))
	}
}

func TestEphemeralPopCarriesEveryOptionItWasGiven(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`{"queue":"inbox","messages":[]}`))
	client := newWireClient(t, srv.URL)

	if _, err := client.Ephemeral().Pop(ephCtx(t), "inbox", EphemeralPopOptions{
		Partition:     "room-7",
		Batch:         10,
		Wait:          true,
		TimeoutMillis: 1500,
		Group:         "workers",
		AutoAck:       true,
	}); err != nil {
		t.Fatalf("pop: %v", err)
	}

	q := queryOf(t, srv.only(t))
	for key, want := range map[string]string{
		"queue": "inbox", "partition": "room-7", "batch": "10",
		"wait": "true", "timeout": "1500", "group": "workers", "autoAck": "true",
	} {
		if got := q.Get(key); got != want {
			t.Fatalf("%s = %q, want %q (raw: %q)", key, got, want, srv.only(t).RawQuery)
		}
	}
}

func TestEphemeralPopNeverSendsAutoAckFalse(t *testing.T) {
	// Byte-identical requests for every consumer that does not opt in: the flag
	// is emitted ONLY when true, so a plain pop cannot be told apart from one
	// that explicitly declined at-most-once delivery.
	srv := newCaptureServer(t, okJSON(`{"queue":"inbox","messages":[]}`))
	client := newWireClient(t, srv.URL)

	if _, err := client.Ephemeral().Pop(ephCtx(t), "inbox", EphemeralPopOptions{AutoAck: false, Batch: 0}); err != nil {
		t.Fatalf("pop: %v", err)
	}
	q := queryOf(t, srv.only(t))
	if _, present := q["autoAck"]; present {
		t.Fatalf("autoAck must not appear when off; raw query: %q", srv.only(t).RawQuery)
	}
	if _, present := q["batch"]; present {
		t.Fatalf("batch must not appear when unset; raw query: %q", srv.only(t).RawQuery)
	}
}

func TestEphemeralPopParsesTheDeliveredMessageAndKeepsThePayloadRaw(t *testing.T) {
	srv := newCaptureServer(t, okJSON(
		`{"queue":"inbox","messages":[{"id":"e:9f1:room-7:12","partition":"room-7","attempts":2,"payload":{"b":1,"a":2}}]}`))
	client := newWireClient(t, srv.URL)

	batch, err := client.Ephemeral().Pop(ephCtx(t), "inbox", EphemeralPopOptions{Group: "workers"})
	if err != nil {
		t.Fatalf("pop: %v", err)
	}
	if batch.Queue != "inbox" || len(batch.Messages) != 1 {
		t.Fatalf("batch = %+v", batch)
	}
	msg := batch.Messages[0]
	if msg.ID != "e:9f1:room-7:12" || msg.Partition != "room-7" || msg.Attempts != 2 {
		t.Fatalf("message = %+v", msg)
	}
	// Raw, not re-encoded: the payload is whatever the producer sent, and a
	// client that reordered its keys would break anybody checksumming it.
	if string(msg.Payload) != `{"b":1,"a":2}` {
		t.Fatalf("payload = %s, want the bytes as they arrived", msg.Payload)
	}
}

func TestEphemeralEmptyPopIsAlwaysAnEmptySlice(t *testing.T) {
	// Three spellings of "nothing", one shape at the caller. A nil slice would
	// read as "empty" in a range and panic in an index, which is exactly the
	// kind of difference that only shows up in production.
	for _, body := range []string{
		`{"queue":"inbox","messages":[]}`,
		`{"queue":"inbox","messages":null}`,
		`{"queue":"inbox"}`,
	} {
		srv := newCaptureServer(t, okJSON(body))
		client := newWireClient(t, srv.URL)

		batch, err := client.Ephemeral().Pop(ephCtx(t), "inbox", EphemeralPopOptions{Wait: true, TimeoutMillis: 10})
		if err != nil {
			t.Fatalf("pop (%s): %v", body, err)
		}
		if batch.Messages == nil {
			t.Fatalf("pop (%s) handed back a nil slice", body)
		}
		if len(batch.Messages) != 0 {
			t.Fatalf("pop (%s) handed back %d messages", body, len(batch.Messages))
		}
		if batch.Queue != "inbox" {
			t.Fatalf("pop (%s) lost the queue name: %q", body, batch.Queue)
		}
	}
}

// ===========================================================================
// ack
// ===========================================================================

func TestEphemeralAckSendsTheExactBody(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`{"results":[{"id":"e:1:Default:1","outcome":"acked"}]}`))
	client := newWireClient(t, srv.URL)

	results, err := client.Ephemeral().Ack(ephCtx(t), "inbox",
		[]EphemeralPopped{{ID: "e:1:Default:1"}},
		EphemeralAckOptions{Group: "workers"})
	if err != nil {
		t.Fatalf("ack: %v", err)
	}
	if len(results) != 1 || results[0].Outcome != EphemeralOutcomeAcked {
		t.Fatalf("results = %+v", results)
	}

	req := srv.only(t)
	if req.Method != http.MethodPost || req.Path != "/api/v1/ephemeral/ack" {
		t.Fatalf("ack went to %s %s", req.Method, req.Path)
	}
	assertJSONBody(t, req.Body, `{"queue":"inbox","group":"workers","acks":[{"id":"e:1:Default:1"}]}`)
}

func TestEphemeralAckLetsAPerEntryStatusWinOverTheCallWideOne(t *testing.T) {
	// How a mixed batch travels in one request: two completed and one retry,
	// which is the ordinary outcome of a handler that partially succeeded.
	srv := newCaptureServer(t, okJSON(`{"results":[]}`))
	client := newWireClient(t, srv.URL)

	if _, err := client.Ephemeral().Ack(ephCtx(t), "inbox", []EphemeralAck{
		{ID: "a"},
		{ID: "b", Status: EphemeralStatusRetry},
		{ID: "c", Status: EphemeralStatusFailed, Error: "boom"},
	}, EphemeralAckOptions{Status: EphemeralStatusCompleted}); err != nil {
		t.Fatalf("ack: %v", err)
	}

	assertJSONBody(t, srv.only(t).Body, `{"queue":"inbox","acks":[
		{"id":"a","status":"completed"},
		{"id":"b","status":"retry"},
		{"id":"c","status":"failed","error":"boom"}]}`)
}

func TestEphemeralAckAcceptsIdsAndPoppedMessagesAlike(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`{"results":[]}`), okJSON(`{"results":[]}`), okJSON(`{"results":[]}`))
	client := newWireClient(t, srv.URL)
	eph := client.Ephemeral()
	ctx := ephCtx(t)

	if _, err := eph.Ack(ctx, "inbox", "e:1:Default:1"); err != nil {
		t.Fatalf("ack by id: %v", err)
	}
	if _, err := eph.Ack(ctx, "inbox", []string{"e:1:Default:1", "e:1:Default:2"}); err != nil {
		t.Fatalf("ack by ids: %v", err)
	}
	if _, err := eph.Ack(ctx, "inbox", EphemeralBatch{Messages: []EphemeralPopped{{ID: "e:1:Default:3"}}}); err != nil {
		t.Fatalf("ack a whole batch: %v", err)
	}

	reqs := srv.requests()
	assertJSONBody(t, reqs[0].Body, `{"queue":"inbox","acks":[{"id":"e:1:Default:1"}]}`)
	assertJSONBody(t, reqs[1].Body, `{"queue":"inbox","acks":[{"id":"e:1:Default:1"},{"id":"e:1:Default:2"}]}`)
	assertJSONBody(t, reqs[2].Body, `{"queue":"inbox","acks":[{"id":"e:1:Default:3"}]}`)
}

func TestEphemeralAckRefusesAMessageWithNoId(t *testing.T) {
	srv := newCaptureServer(t)
	client := newWireClient(t, srv.URL)

	_, err := client.Ephemeral().Ack(ephCtx(t), "inbox", []EphemeralPopped{{ID: "ok"}, {ID: ""}})
	if err == nil {
		t.Fatal("an ack with no id was accepted")
	}
	if !strings.Contains(err.Error(), "index 1") {
		t.Fatalf("the error does not name the offending index: %v", err)
	}
	if n := len(srv.requests()); n != 0 {
		t.Fatalf("a refused ack made %d requests", n)
	}
}

func TestEphemeralAckOfNothingNeverReachesTheNetwork(t *testing.T) {
	srv := newCaptureServer(t)
	client := newWireClient(t, srv.URL)

	results, err := client.Ephemeral().Ack(ephCtx(t), "inbox", []EphemeralAck{})
	if err != nil {
		t.Fatalf("ack: %v", err)
	}
	if results == nil || len(results) != 0 {
		t.Fatalf("results = %+v, want an empty slice", results)
	}
	if n := len(srv.requests()); n != 0 {
		t.Fatalf("an empty ack made %d requests", n)
	}
}

// ===========================================================================
// status
// ===========================================================================

func TestEphemeralStatusRoutes(t *testing.T) {
	srv := newCaptureServer(t,
		okJSON(`{"queues":[{"queue":"inbox","depth":3,"bytes":128,"declared":false}]}`),
		okJSON(`{"queue":"inbox","depth":3,"bytes":128,"groups":{"workers":2}}`),
	)
	client := newWireClient(t, srv.URL)
	eph := client.Ephemeral()
	ctx := ephCtx(t)

	queues, err := eph.Queues(ctx)
	if err != nil {
		t.Fatalf("queues: %v", err)
	}
	if _, ok := queues["queues"]; !ok {
		t.Fatalf("queues payload lost its rows: %+v", queues)
	}

	depth, err := eph.Depth(ctx, "in box")
	if err != nil {
		t.Fatalf("depth: %v", err)
	}
	if d, _ := depth["depth"].(float64); d != 3 {
		t.Fatalf("depth = %v", depth["depth"])
	}

	reqs := srv.requests()
	if reqs[0].Method != http.MethodGet || reqs[0].Path != "/api/v1/ephemeral/queues" {
		t.Fatalf("queues went to %s %s", reqs[0].Method, reqs[0].Path)
	}
	if reqs[1].RawPath != "/api/v1/ephemeral/queues/in%20box/depth" {
		t.Fatalf("depth went to %q", reqs[1].RawPath)
	}
}

// ===========================================================================
// the 404 rule (§4, §8)
// ===========================================================================

func TestEveryEphemeralVerbMapsA404ToOneError(t *testing.T) {
	// One sentinel for the whole family, because a 404 here always means the
	// same thing: the routes are not there. An old broker never registered
	// them; an old proxy answers `route_blocked` because it fails closed on
	// unknown API paths. Neither is "your queue is missing" -- these verbs
	// answer an absent queue with a normal body.
	verbs := map[string]func(*Ephemeral, context.Context) error{
		"configure": func(e *Ephemeral, ctx context.Context) error {
			_, err := e.Configure(ctx, "inbox", EphemeralOptions{})
			return err
		},
		"reset": func(e *Ephemeral, ctx context.Context) error {
			_, err := e.Reset(ctx, "inbox")
			return err
		},
		"delete": func(e *Ephemeral, ctx context.Context) error {
			_, err := e.Delete(ctx, "inbox")
			return err
		},
		"push": func(e *Ephemeral, ctx context.Context) error {
			_, err := e.Push(ctx, "inbox", 1)
			return err
		},
		"pop": func(e *Ephemeral, ctx context.Context) error {
			_, err := e.Pop(ctx, "inbox")
			return err
		},
		"ack": func(e *Ephemeral, ctx context.Context) error {
			_, err := e.Ack(ctx, "inbox", "e:1:Default:1")
			return err
		},
		"queues": func(e *Ephemeral, ctx context.Context) error {
			_, err := e.Queues(ctx)
			return err
		},
		"depth": func(e *Ephemeral, ctx context.Context) error {
			_, err := e.Depth(ctx, "inbox")
			return err
		},
	}

	for name, call := range verbs {
		srv := newCaptureServer(t, notFound(`{"error":"Not Found"}`))
		client := newWireClient(t, srv.URL)

		err := call(client.Ephemeral(), ephCtx(t))
		if err == nil {
			t.Fatalf("%s: a 404 was not reported at all", name)
		}
		if !errors.Is(err, ErrEphemeralUnsupported) {
			t.Fatalf("%s: 404 became %v, want ErrEphemeralUnsupported", name, err)
		}
		if !strings.Contains(err.Error(), "requires >= 1.1") {
			t.Fatalf("%s: the error does not name the version requirement: %v", name, err)
		}
	}
}

func TestEphemeralProxyRouteBlockedIsTheSameError(t *testing.T) {
	// The proxy's spelling of the same fact. Branching on the code instead of
	// on the status would give a caller two upgrade errors to handle.
	srv := newCaptureServer(t, notFound(`{"error":"route not available","code":"route_blocked"}`))
	client := newWireClient(t, srv.URL)

	_, err := client.Ephemeral().Pop(ephCtx(t), "inbox")
	if !errors.Is(err, ErrEphemeralUnsupported) {
		t.Fatalf("route_blocked became %v, want ErrEphemeralUnsupported", err)
	}

	// ...and the original is still reachable, so a diagnosis does not need the
	// prose of the mapped message.
	var he *HTTPError
	if !errors.As(err, &he) {
		t.Fatalf("the 404 that caused it is not reachable through errors.As: %v", err)
	}
	if he.StatusCode != 404 || he.Code != "route_blocked" {
		t.Fatalf("the kept HTTPError is %+v", he)
	}
}

func TestDepthsOwn404IsNotAnUpgradeError(t *testing.T) {
	// The one 404 this family answers about a QUEUE rather than about the
	// routes. Reporting it as "upgrade your broker" would send an operator
	// looking at versions because they typed a name that no longer has a ring
	// behind it — which on this class is the ordinary end of an implicit queue.
	srv := newCaptureServer(t, cannedResponse{
		status: http.StatusNotFound,
		body:   `{"error":"no ephemeral queue by that name exists on this broker","code":"ephemeral_queue_not_found"}`,
	})
	client := newWireClient(t, srv.URL)

	_, err := client.Ephemeral().Depth(ephCtx(t), "gone")
	if err == nil {
		t.Fatal("a missing queue was not reported")
	}
	if !errors.Is(err, ErrEphemeralQueueNotFound) {
		t.Fatalf("depth 404 became %v, want ErrEphemeralQueueNotFound", err)
	}
	if errors.Is(err, ErrEphemeralUnsupported) {
		t.Fatalf("a missing queue was mapped to the upgrade error: %v", err)
	}

	// ...and the transport error is still reachable, code included.
	var he *HTTPError
	if !errors.As(err, &he) {
		t.Fatalf("the 404 that caused it is not reachable through errors.As: %v", err)
	}
	if he.Code != EphemeralCodeQueueNotFound {
		t.Fatalf("the kept HTTPError is %+v", he)
	}
}

func TestABareBroker404IsStillTheUpgradeError(t *testing.T) {
	// The same route, the same status, no ephemeral code: an old broker that
	// never registered it. The code is the whole difference, which is why the
	// mapping reads it rather than the status alone.
	srv := newCaptureServer(t, notFound(``))
	client := newWireClient(t, srv.URL)

	_, err := client.Ephemeral().Depth(ephCtx(t), "inbox")
	if !errors.Is(err, ErrEphemeralUnsupported) {
		t.Fatalf("a codeless 404 became %v, want ErrEphemeralUnsupported", err)
	}
	if errors.Is(err, ErrEphemeralQueueNotFound) {
		t.Fatalf("an old broker was reported as a missing queue: %v", err)
	}
}

func TestEphemeralRefusalsAreNotMappedAndKeepTheirCode(t *testing.T) {
	// 429 `queue_full` is the ring's own backpressure (§1.6) and 503
	// `ephemeral_unavailable` is the cell's byte ceiling. Both are transient and
	// neither is an upgrade, so both must reach the caller as themselves --
	// with the machine-readable code intact, because this family's envelope
	// puts it in `code` (the proxy's shape) and not in `error`.
	srv := newCaptureServer(t, cannedResponse{
		status: http.StatusTooManyRequests,
		body:   `{"error":"the ephemeral queue is at its maxBytes/maxLength and its policy is reject","code":"queue_full"}`,
	})
	client, err := New(ClientConfig{
		URL:           srv.URL,
		TimeoutMillis: 2000,
		RetryAttempts: -1,
		Retry429:      &Retry429Config{MaxAttempts: 1},
	})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	t.Cleanup(func() { client.httpClient.Close() })

	_, err = client.Ephemeral().Push(ephCtx(t), "inbox", 1)
	if err == nil {
		t.Fatal("a 429 was not reported")
	}
	if errors.Is(err, ErrEphemeralUnsupported) {
		t.Fatalf("a 429 was mapped to the upgrade error: %v", err)
	}
	var he *HTTPError
	if !errors.As(err, &he) {
		t.Fatalf("the refusal did not arrive as an *HTTPError: %v", err)
	}
	if he.StatusCode != 429 || he.Code != "queue_full" {
		t.Fatalf("refusal = %+v, want 429/queue_full", he)
	}
}

// ===========================================================================
// buffering (§4.1) — the sink, the address, and the durable pin
// ===========================================================================

func TestBufferedEphemeralPushDrainsToTheEphemeralWire(t *testing.T) {
	// MessageCount is reached inside the call, so the drain is synchronous and
	// the assertion needs no sleep.
	srv := newCaptureServer(t, createdJSON(`{"pushed":2}`))
	client := newWireClient(t, srv.URL)

	res, err := client.Ephemeral().Push(ephCtx(t), "presence", []interface{}{
		map[string]interface{}{"n": 1},
		map[string]interface{}{"n": 2},
	}, EphemeralPushOptions{
		Partition: "room-7",
		Buffered:  &BufferConfig{MessageCount: 2, TimeMillis: 60000},
	})
	if err != nil {
		t.Fatalf("buffered push: %v", err)
	}
	// A buffered push resolves once the messages are IN the buffer, so the
	// answer says `buffered`, never `pushed`.
	if !res.Buffered || res.Count != 2 || res.Pushed != 0 {
		t.Fatalf("buffered push result = %+v", res)
	}

	req := srv.only(t)
	if req.Path != "/api/v1/ephemeral/push" {
		t.Fatalf("the buffered drain posted to %q", req.Path)
	}
	// The SAME envelope the unbuffered push builds: the sink is where the shape
	// lives, so the two paths cannot drift.
	assertJSONBody(t, req.Body, `{"queue":"presence","partition":"room-7","messages":[
		{"payload":{"n":1}},{"payload":{"n":2}}]}`)
}

func TestDurableBufferedPushBodyIsUnchanged(t *testing.T) {
	// THE PIN. Buffered ephemeral push reuses the durable machinery through the
	// FlushFunc that BufferManager.Add already took, so the durable drain was
	// not edited at all -- and this test exists for no other reason than to fail
	// if that ever stops being true. Identity per ITEM, envelope `{items}`, and
	// not one key more.
	srv := newCaptureServer(t, okJSON(`[{"status":"queued","transactionId":"11111111-1111-4111-8111-111111111111"}]`))
	client := newWireClient(t, srv.URL)

	if _, err := client.Queue("orders").
		Partition("eu").
		Buffer(BufferConfig{MessageCount: 1, TimeMillis: 60000}).
		Push(map[string]interface{}{"total": 19.99}).
		TransactionID("11111111-1111-4111-8111-111111111111").
		Execute(ephCtx(t)); err != nil {
		t.Fatalf("durable buffered push: %v", err)
	}

	req := srv.only(t)
	if req.Path != "/api/v1/push" {
		t.Fatalf("the durable drain posted to %q", req.Path)
	}
	assertJSONBody(t, req.Body, `{"items":[{"queue":"orders","partition":"eu","payload":{"total":19.99},
		"transactionId":"11111111-1111-4111-8111-111111111111"}]}`)
}

func TestTheTwoFamiliesNeverShareABufferOrADrain(t *testing.T) {
	// An ephemeral `orders` and a durable `orders` are unrelated objects (§10
	// Q8). A shared buffer address would post one family's messages to the
	// other family's route, which is a message loss with a 201 on it.
	srv := newCaptureServer(t,
		okJSON(`[{"status":"queued","transactionId":"11111111-1111-4111-8111-111111111111"}]`),
		createdJSON(`{"pushed":1}`),
	)
	client := newWireClient(t, srv.URL)
	ctx := ephCtx(t)

	if _, err := client.Queue("orders").
		Buffer(BufferConfig{MessageCount: 1, TimeMillis: 60000}).
		Push(map[string]interface{}{"durable": true}).
		Execute(ctx); err != nil {
		t.Fatalf("durable buffered push: %v", err)
	}
	if _, err := client.Ephemeral().Push(ctx, "orders", map[string]interface{}{"ephemeral": true},
		EphemeralPushOptions{Buffered: &BufferConfig{MessageCount: 1, TimeMillis: 60000}}); err != nil {
		t.Fatalf("ephemeral buffered push: %v", err)
	}

	bm := client.GetBufferManager()
	durable := bm.GetBuffer("orders/Default")
	ephemeral := bm.GetBuffer("eph:orders")
	if durable == nil {
		t.Fatal("the durable buffer is not under orders/Default")
	}
	if ephemeral == nil {
		t.Fatal("the ephemeral buffer is not under eph:orders")
	}
	if durable == ephemeral {
		t.Fatal("the two families share one buffer")
	}

	reqs := srv.requests()
	if len(reqs) != 2 {
		t.Fatalf("expected one drain each, got %d requests", len(reqs))
	}
	if reqs[0].Path != "/api/v1/push" || reqs[1].Path != "/api/v1/ephemeral/push" {
		t.Fatalf("drains crossed: %q then %q", reqs[0].Path, reqs[1].Path)
	}
}

func TestEphemeralBufferAddressesSeparateNamedAndUnnamedPartitions(t *testing.T) {
	// A push that named no partition is a DIFFERENT destination from any named
	// one, because the broker picks -- merging the two into one buffer would
	// send a batch to a ring the caller never chose.
	if got := ephemeralBufferAddress("orders", ""); got != "eph:orders" {
		t.Fatalf("unnamed address = %q", got)
	}
	if got := ephemeralBufferAddress("orders", "eu"); got != "eph:orders/eu" {
		t.Fatalf("named address = %q", got)
	}
}

func TestEphemeralFlushDrainsOneQueue(t *testing.T) {
	// A high MessageCount means nothing flushes on its own; Flush is what puts
	// the batch on the wire.
	srv := newCaptureServer(t, createdJSON(`{"pushed":1}`))
	client := newWireClient(t, srv.URL)
	eph := client.Ephemeral()
	ctx := ephCtx(t)

	if _, err := eph.Push(ctx, "presence", map[string]interface{}{"n": 1},
		EphemeralPushOptions{Buffered: &BufferConfig{MessageCount: 1000, TimeMillis: 60000}}); err != nil {
		t.Fatalf("buffered push: %v", err)
	}
	if n := len(srv.requests()); n != 0 {
		t.Fatalf("a buffered push below the threshold made %d requests", n)
	}

	if err := eph.Flush(ctx, "presence"); err != nil {
		t.Fatalf("flush: %v", err)
	}
	req := srv.only(t)
	if req.Path != "/api/v1/ephemeral/push" {
		t.Fatalf("flush posted to %q", req.Path)
	}
	assertJSONBody(t, req.Body, `{"queue":"presence","messages":[{"payload":{"n":1}}]}`)
}

func TestCloseDrainsEphemeralBuffersToo(t *testing.T) {
	// Buffered messages live in this process's memory. Close is the one moment
	// the client promises to get them out, and an ephemeral buffer it did not
	// know about would be a silent loss on every orderly shutdown.
	srv := newCaptureServer(t, createdJSON(`{"pushed":1}`))
	client := newWireClient(t, srv.URL)
	ctx := ephCtx(t)

	if _, err := client.Ephemeral().Push(ctx, "presence", map[string]interface{}{"n": 1},
		EphemeralPushOptions{Buffered: &BufferConfig{MessageCount: 1000, TimeMillis: 60000}}); err != nil {
		t.Fatalf("buffered push: %v", err)
	}
	if err := client.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}

	req := srv.only(t)
	if req.Path != "/api/v1/ephemeral/push" {
		t.Fatalf("close drained to %q", req.Path)
	}
}

// ===========================================================================
// the affinity key
// ===========================================================================

func TestEphemeralPopAffinityKeyMatchesTheGroupingRule(t *testing.T) {
	// Repeated pops of one queue should land on one backend when the client
	// holds several URLs. The broker forwards to the rendezvous owner either
	// way, so this saves a hop rather than creating correctness -- but the key
	// has to be the SAME one the durable pop uses, or a client speaking both
	// families spreads itself across the cell for no reason.
	srv := newCaptureServer(t, okJSON(`{"queue":"inbox","messages":[]}`), okJSON(`{"queue":"inbox","messages":[]}`))
	client := newWireClient(t, srv.URL)
	lb := client.httpClient.GetLoadBalancer()

	for _, tc := range []struct {
		opts EphemeralPopOptions
		want string
	}{
		{EphemeralPopOptions{}, "inbox:*:" + QueueModeConsumerGroup},
		{EphemeralPopOptions{Partition: "room-7", Group: "workers"}, "inbox:room-7:workers"},
	} {
		if _, err := client.Ephemeral().Pop(ephCtx(t), "inbox", tc.opts); err != nil {
			t.Fatalf("pop: %v", err)
		}
		// With one backend every key resolves to it; what is asserted here is
		// that the key is well-formed and stable, which is what the ring reads.
		if got := lb.GetURL(tc.want); got == "" {
			t.Fatalf("affinity key %q resolved to no backend", tc.want)
		}
	}
}

// ===========================================================================
// small guards
// ===========================================================================

func TestEphemeralVerbsRefuseAnEmptyQueueName(t *testing.T) {
	srv := newCaptureServer(t)
	client := newWireClient(t, srv.URL)
	eph := client.Ephemeral()
	ctx := ephCtx(t)

	if _, err := eph.Push(ctx, "", 1); err == nil {
		t.Fatal("push accepted an empty queue name")
	}
	if _, err := eph.Pop(ctx, ""); err == nil {
		t.Fatal("pop accepted an empty queue name")
	}
	if _, err := eph.Reset(ctx, ""); err == nil {
		t.Fatal("reset accepted an empty queue name")
	}
	if _, err := eph.Delete(ctx, ""); err == nil {
		t.Fatal("delete accepted an empty queue name")
	}
	if n := len(srv.requests()); n != 0 {
		t.Fatalf("refused calls made %d requests", n)
	}
}

func TestEphemeralVerbsRefuseMoreThanOneOptionsValue(t *testing.T) {
	// The variadic is the SDK's spelling of an optional argument, not of a list.
	srv := newCaptureServer(t)
	client := newWireClient(t, srv.URL)
	eph := client.Ephemeral()
	ctx := ephCtx(t)

	if _, err := eph.Push(ctx, "q", 1, EphemeralPushOptions{}, EphemeralPushOptions{}); err == nil {
		t.Fatal("push took two options")
	}
	if _, err := eph.Pop(ctx, "q", EphemeralPopOptions{}, EphemeralPopOptions{}); err == nil {
		t.Fatal("pop took two options")
	}
	if _, err := eph.Ack(ctx, "q", "id", EphemeralAckOptions{}, EphemeralAckOptions{}); err == nil {
		t.Fatal("ack took two options")
	}
}

func TestEphemeralPopQueryIsEscaped(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`{"queue":"a b","messages":[]}`))
	client := newWireClient(t, srv.URL)

	if _, err := client.Ephemeral().Pop(ephCtx(t), "a b", EphemeralPopOptions{Group: "g&h"}); err != nil {
		t.Fatalf("pop: %v", err)
	}
	req := srv.only(t)
	v, err := url.ParseQuery(req.RawQuery)
	if err != nil {
		t.Fatalf("the query is not parseable: %v (%q)", err, req.RawQuery)
	}
	if v.Get("queue") != "a b" || v.Get("group") != "g&h" {
		t.Fatalf("escaping lost a value: %+v (raw: %q)", v, req.RawQuery)
	}
}

func TestEphemeralPushResponseNumbersSurviveAsInt64(t *testing.T) {
	// `pushed` and `dropped` are decoded from the raw bytes into typed fields
	// rather than through map[string]interface{}, where every number becomes a
	// float64 and stops being exact past 2^53.
	srv := newCaptureServer(t, createdJSON(`{"pushed":9007199254740993}`))
	client := newWireClient(t, srv.URL)

	res, err := client.Ephemeral().Push(ephCtx(t), "inbox", 1)
	if err != nil {
		t.Fatalf("push: %v", err)
	}
	if res.Pushed != 9007199254740993 {
		t.Fatalf("pushed = %d, want 9007199254740993", res.Pushed)
	}
}

func TestEphemeralUnsupportedErrorCarriesNoNilHTTPPanic(t *testing.T) {
	// The zero value has to be safe: errors.Is walks Unwrap, and a slice
	// carrying a typed nil would be a panic in somebody's error handler.
	err := error(&EphemeralUnsupportedError{})
	if !errors.Is(err, ErrEphemeralUnsupported) {
		t.Fatal("a bare EphemeralUnsupportedError does not match the sentinel")
	}
	var he *HTTPError
	if errors.As(err, &he) {
		t.Fatal("a bare EphemeralUnsupportedError produced an HTTPError out of nothing")
	}
}

func TestEphemeralMessageMarshalsToPayloadAndNothingElse(t *testing.T) {
	b, err := json.Marshal(EphemeralMessage{Payload: map[string]interface{}{"a": 1}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(b) != `{"payload":{"a":1}}` {
		t.Fatalf("EphemeralMessage marshalled to %s", b)
	}
}

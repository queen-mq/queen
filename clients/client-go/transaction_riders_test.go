package queen

import (
	"encoding/base64"
	"net/http"
	"strings"
	"testing"
	"time"
)

// The transaction wire with kv and timers riders (PLAN_KV_TIMERS.md §6.3, §8.2,
// §8.3, §10.4).
//
// THE ONE THAT MATTERS. §10.4, Go bullet two: two Go fields carrying the same
// JSON key at the same level are DISCARDED BY BOTH, with no error and no
// warning. If `Operation` had grown a `kv` leg, the body would go out with zero
// kv ops, the broker would commit a transaction whose gate never ran, and the
// putIfAbsent would never have existed. Nothing downstream notices — not the
// status code, not the results array, not a log line. So the arrays are
// TOP-LEVEL fields of the request and `Operation` does not change, and these
// tests are what keeps it that way.

func TestTransactionPutsKvAndTimersAtTheTopLevel(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`{"transactionId":"tx-1","success":true,"results":[
		{"index":0,"type":"push","success":true,"transactionId":"p-1","messageId":"m-1","queueName":"orders"},
		{"index":1,"type":"kv","opIndex":0,"op":"putIfAbsent","applied":true,"key":"saga-7","value":{"o":1},"version":1},
		{"index":2,"type":"timer","opIndex":0,"ok":true,"status":"scheduled","queue":"reminders","timerKey":"order-7","txn":"t-7","messageId":"tm-7","deliverAt":"2026-08-17T10:00:00.000000Z"}
	]}`))
	client := newWireClient(t, srv.URL)

	resp, err := client.Transaction().
		Queue("orders").Push(map[string]interface{}{"id": 7}).
		KV(KVPutIfAbsentOp("saga", "saga-7", map[string]interface{}{"o": 1}, TTLSeconds(3600))).
		Timers(ScheduleTimerOp(TimerSchedule{
			Queue: "reminders", TimerKey: "order-7", Delay: 5 * time.Second,
			Payload: map[string]interface{}{"orderId": 7}, TransactionID: "t-7",
		})).
		Commit(kvCtx(t))
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	req := srv.only(t)
	if req.Path != "/api/v1/transaction" {
		t.Fatalf("path = %s", req.Path)
	}
	body, derr := decodeAny(req.Body)
	if derr != nil {
		t.Fatalf("body: %v", derr)
	}
	root, ok := body.(map[string]interface{})
	if !ok {
		t.Fatalf("body is not an object: %s", string(req.Body))
	}
	// The two arrays are top-level, BESIDE `operations` and never inside it.
	kv, ok := root["kv"].([]interface{})
	if !ok || len(kv) != 1 {
		t.Fatalf("`kv` must be a top-level array of 1, got %#v", root["kv"])
	}
	timers, ok := root["timers"].([]interface{})
	if !ok || len(timers) != 1 {
		t.Fatalf("`timers` must be a top-level array of 1, got %#v", root["timers"])
	}
	// And nothing kv/timer-shaped may hide inside `operations`: an op that
	// arrives as {"type":"kv"} there falls into the demux's `_ =>` arm and gets
	// a named 400 — which is the best failure available, but it is a failure.
	ops := root["operations"].([]interface{})
	for i, o := range ops {
		ty, _ := o.(map[string]interface{})["type"].(string)
		if ty != "push" && ty != "ack" {
			t.Fatalf("operations[%d] has type %q; only push and ack live there", i, ty)
		}
	}

	payload := base64.StdEncoding.EncodeToString([]byte(`{"orderId":7}`))
	assertJSONBody(t, req.Body, `{
		"operations":[{"type":"push","items":[{"queue":"orders","partition":"Default","payload":{"id":7},"transactionId":"`+pushTxnOf(t, req.Body)+`"}]}],
		"requiredLeases":[],
		"kv":[{"op":"putIfAbsent","ns":"saga","key":"saga-7","value":{"o":1},"ttlSeconds":3600}],
		"timers":[{"op":"schedule","queue":"reminders","timerKey":"order-7","delayMs":5000,"txn":"t-7","payload":"`+payload+`"}]
	}`)

	// The flat result space is append-only: pushes and acks keep the indices
	// they have today, kv follows, timers last (§8.2 point 1).
	if len(resp.KV) != 1 || !resp.KV[0].Write().Applied {
		t.Errorf("kv results = %+v", resp.KV)
	}
	if len(resp.Timers) != 1 || resp.Timers[0].Status != TimerStatusScheduled {
		t.Errorf("timer results = %+v", resp.Timers)
	}
	if !resp.Success {
		t.Error("success should be true")
	}
}

// pushTxnOf digs the minted push transactionId out of the captured body: it is a
// fresh UUID per run, so it cannot be a literal in the expected JSON, and
// blanking it out would weaken the assertion on every other field.
func pushTxnOf(t *testing.T, body []byte) string {
	t.Helper()
	v, err := decodeAny(body)
	if err != nil {
		t.Fatalf("body: %v", err)
	}
	ops := v.(map[string]interface{})["operations"].([]interface{})
	items := ops[0].(map[string]interface{})["items"].([]interface{})
	id, _ := items[0].(map[string]interface{})["transactionId"].(string)
	if !IsValidUUID(id) {
		t.Fatalf("push transactionId = %q, want a UUID", id)
	}
	return id
}

func TestTransactionOmitsTheRiderArraysWhenThereAreNone(t *testing.T) {
	// §6.3 buys byte-identity on the server when the arrays are ABSENT. A client
	// that emitted `"kv":null` would be tolerated (the procedure uses
	// jsonb_typeof for exactly that reason) but would still be a client that
	// changed a body which had no reason to change.
	srv := newCaptureServer(t, okJSON(`{"transactionId":"tx","success":true,"results":[{"index":0,"type":"push","success":true,"transactionId":"p","messageId":"m","queueName":"q"}]}`))
	client := newWireClient(t, srv.URL)

	if _, err := client.Transaction().Queue("q").Push(map[string]interface{}{"a": 1}).Commit(kvCtx(t)); err != nil {
		t.Fatalf("commit: %v", err)
	}
	body := string(srv.only(t).Body)
	if strings.Contains(body, `"kv"`) || strings.Contains(body, `"timers"`) {
		t.Fatalf("a bundle with no riders must not carry the keys at all: %s", body)
	}
}

func TestTransactionAcceptsARiderOnlyBundle(t *testing.T) {
	// A KV-only bundle legitimately carries no `operations`, and the broker
	// routes it away from the push lane entirely (§2.5). The old "transaction
	// has no operations" guard would have refused it client-side.
	srv := newCaptureServer(t, okJSON(`{"transactionId":"tx","success":true,"results":[{"index":0,"type":"kv","opIndex":0,"op":"put","applied":true,"key":"k","value":1,"version":1}]}`))
	client := newWireClient(t, srv.URL)

	resp, err := client.Transaction().KV(KVPutOp("ns", "k", 1, TTLSeconds(30))).Commit(kvCtx(t))
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if !resp.Success || len(resp.KV) != 1 {
		t.Fatalf("resp = %+v", resp)
	}
}

func TestTransactionStillRefusesACompletelyEmptyCommit(t *testing.T) {
	srv := newCaptureServer(t)
	client := newWireClient(t, srv.URL)

	if _, err := client.Transaction().Commit(kvCtx(t)); err == nil {
		t.Fatal("an empty transaction must still be an error")
	}
	if n := len(srv.requests()); n != 0 {
		t.Fatalf("got %d requests, want 0", n)
	}
}

func TestTransactionRefusesGetPrefixInTheWire(t *testing.T) {
	// §5.5: getPrefix is read work whose cost the caller does not bound a
	// priori, inside the transaction that holds the outermost lock space. The
	// server raises 22023; the client refuses before spending the round trip,
	// and names the surface that does accept it.
	srv := newCaptureServer(t)
	client := newWireClient(t, srv.URL)

	_, err := client.Transaction().
		Queue("q").Push(map[string]interface{}{"a": 1}).
		KV(KVGetPrefixOp("ns", "quota:")).
		Commit(kvCtx(t))
	if err == nil {
		t.Fatal("getPrefix inside a transaction must be refused")
	}
	if !strings.Contains(err.Error(), "/api/v1/kv") {
		t.Errorf("the error should name the surface that accepts it: %v", err)
	}
	if n := len(srv.requests()); n != 0 {
		t.Fatalf("got %d requests, want 0", n)
	}
}

// ---------------------------------------------------------------------------
// The one wire change this client had to make (§10.2, the client-go row):
// commit RETURNS on a lost KV precondition and RAISES on everything else.
// ---------------------------------------------------------------------------

func TestCommitReturnsOnAKvPrecondition(t *testing.T) {
	// `required:true` lost its precondition: the transaction really did roll
	// back, and this is the EXPECTED outcome of every legitimate redelivery.
	// HTTP 200, and everything the caller needs is in the body — no second round
	// trip, no string matching on an error message (§8.3).
	srv := newCaptureServer(t, okJSON(`{
		"transactionId":"tx-9","success":false,"ok":false,"reason":"kv_precondition",
		"error":"kv_precondition_failed","failedIndex":2,"kvReason":"exists",
		"version":9007199254740993,"value":{"owner":"worker-2"},"results":[]
	}`))
	client := newWireClient(t, srv.URL)

	resp, err := client.Transaction().
		Queue("orders").Push(map[string]interface{}{"a": 1}).
		KV(KVPutIfAbsentOp("saga", "s-1", 1, TTLSeconds(60), KVWriteOptions{Required: true})).
		Commit(kvCtx(t))

	if err != nil {
		t.Fatalf("a lost precondition must be RETURNED, not raised: %v", err)
	}
	if resp.Success {
		t.Error("success should be false")
	}
	if resp.Reason != ReasonKVPrecondition {
		t.Errorf("reason = %q, want %q", resp.Reason, ReasonKVPrecondition)
	}
	if resp.FailedIndex != 2 {
		t.Errorf("failedIndex = %d, want 2 (the FLAT index)", resp.FailedIndex)
	}
	if resp.KVReason != KVReasonExists {
		t.Errorf("kvReason = %q", resp.KVReason)
	}
	// The version is a BIGINT. Through map[string]interface{} it would have come
	// back as ...92: this is the whole point of the raw path.
	if resp.Version != 9007199254740993 {
		t.Errorf("version = %d, want 9007199254740993", resp.Version)
	}
	if string(resp.Value) != `{"owner":"worker-2"}` {
		t.Errorf("value = %s", string(resp.Value))
	}
	if !resp.IsKVPrecondition() {
		t.Error("IsKVPrecondition should be true")
	}
}

func TestCommitRaisesOnEveryOtherFailure(t *testing.T) {
	// Everything that is NOT a lost precondition is a failure: a bundle that did
	// not commit must not read as "fine" to a caller who only checks err.
	cases := []struct {
		name string
		resp cannedResponse
	}{
		{"db error at 200", okJSON(`{"transactionId":"tx","success":false,"reason":"db_error","error":"QTXN something broke","results":[]}`)},
		{"misaligned riders", okJSON(`{"transactionId":"tx","success":false,"reason":"misaligned","error":"QTXN the transaction returned no kv results","results":[]}`)},
		// The 400 that used to sit here -- "this transaction carries kv
		// operations but the key/value surface is not enabled on this cell" --
		// cannot be answered any more: with QUEEN_KV_ENABLED gone, no cell is
		// missing the surface and a rider cannot arrive at the wrong broker.
		// What CAN still refuse a rider is the operator's runtime kill switch,
		// and on the wire it does so PERMANENTLY (403, no Retry-After) where the
		// route would say 503: a bundle carries messages, and a client looping on
		// a deliberately paused cell is a storm on the hot path.
		{"rider refused by the runtime kill switch", cannedResponse{
			status: http.StatusForbidden,
			body:   `{"transactionId":"tx","success":false,"reason":"kv_disabled","error":"the bundle was not committed: its kv/timers riders were refused, and a transaction that dropped them would commit the very operations the riders exist to gate","results":[]}`,
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := newCaptureServer(t, tc.resp)
			client := newWireClient(t, srv.URL)
			resp, err := client.Transaction().
				Queue("q").Push(map[string]interface{}{"a": 1}).
				KV(KVPutOp("ns", "k", 1, TTLSeconds(60))).
				Commit(kvCtx(t))
			if err == nil {
				t.Fatalf("expected an error, got resp %+v", resp)
			}
			// The response is still handed back so the caller can inspect the
			// reason without parsing the error string.
			if resp == nil || resp.Success {
				t.Fatalf("the failed response should still be readable, got %+v", resp)
			}
		})
	}
}

func TestCommitCarriesTheTransactionIdBack(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`{"transactionId":"tx-42","success":true,"results":[{"index":0,"type":"push","success":true,"transactionId":"p","messageId":"m","queueName":"q"}]}`))
	client := newWireClient(t, srv.URL)

	resp, err := client.Transaction().Queue("q").Push(map[string]interface{}{"a": 1}).Commit(kvCtx(t))
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if resp.TransactionID != "tx-42" {
		t.Errorf("transactionId = %q", resp.TransactionID)
	}
}

func TestTransactionCancelTimerOpTravelsInTheRider(t *testing.T) {
	// The saga close bundle: ack the message, cancel the compensation timer, in
	// one transaction. A cancel is charged nothing and refused by nothing below
	// the env rung, on the wire as on its own route (§9.6).
	srv := newCaptureServer(t, okJSON(`{"transactionId":"tx","success":true,"results":[
		{"index":0,"type":"push","success":true,"transactionId":"p","messageId":"m","queueName":"q"},
		{"index":1,"type":"timer","opIndex":0,"ok":true,"status":"cancelled","queue":"reminders","timerKey":"order-7","txn":"t-7"}
	]}`))
	client := newWireClient(t, srv.URL)

	resp, err := client.Transaction().
		Queue("q").Push(map[string]interface{}{"a": 1}).
		Timers(CancelTimerOp("reminders", "order-7")).
		Commit(kvCtx(t))
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	body, _ := decodeAny(srv.only(t).Body)
	timers := body.(map[string]interface{})["timers"].([]interface{})
	op := timers[0].(map[string]interface{})
	if op["op"] != "cancel" || op["queue"] != "reminders" || op["timerKey"] != "order-7" {
		t.Fatalf("cancel op = %#v", op)
	}
	// A cancel carries no delayMs, no txn and no payload: it is not a schedule.
	for _, f := range []string{"delayMs", "payload"} {
		if _, present := op[f]; present {
			t.Errorf("cancel must not carry %s", f)
		}
	}
	if len(resp.Timers) != 1 || resp.Timers[0].Status != TimerStatusCancelled {
		t.Errorf("timer results = %+v", resp.Timers)
	}
}

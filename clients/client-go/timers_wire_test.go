package queen

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"
)

// The timer wire contract (PLAN_KV_TIMERS.md §4, §8.1), asserted against a
// scripted plan server. No broker, no database.

func TestTimerScheduleSendsTheExactBody(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`{"results":[{"ok":true,"status":"scheduled","queue":"reminders","timerKey":"order-7","txn":"11111111-1111-4111-8111-111111111111","messageId":"22222222-2222-7222-8222-222222222222","deliverAt":"2026-08-17T10:00:00.000000Z"}]}`))
	client := newWireClient(t, srv.URL)

	res, err := client.Timers().Schedule(kvCtx(t), TimerSchedule{
		Queue:         "reminders",
		TimerKey:      "order-7",
		Delay:         250 * time.Millisecond,
		Payload:       map[string]interface{}{"orderId": 7},
		TransactionID: "11111111-1111-4111-8111-111111111111",
	})
	if err != nil {
		t.Fatalf("schedule: %v", err)
	}
	req := srv.only(t)
	if req.Method != http.MethodPost || req.Path != "/api/v1/timers" {
		t.Fatalf("schedule went to %s %s, want POST /api/v1/timers", req.Method, req.Path)
	}
	// delayMs and NOT delaySeconds (§20.6, ratified): the durations that CAN be
	// sub-second are in milliseconds, the ones that cannot are in seconds. A
	// 250 ms retry backoff is a central use of timers.
	//
	// The payload is base64 (§8.1). No producerSub, no messageId, no tenant, no
	// deliverAt: all four are server-owned and the broker rejects them rather
	// than ignoring them — supplying producerSub would forge the one
	// non-repudiable field of a frame (§4.2).
	payload := base64.StdEncoding.EncodeToString([]byte(`{"orderId":7}`))
	assertJSONBody(t, req.Body, `{"operations":[{"op":"schedule","queue":"reminders","timerKey":"order-7","delayMs":250,"txn":"11111111-1111-4111-8111-111111111111","payload":"`+payload+`"}]}`)

	if !res.OK || res.Status != TimerStatusScheduled {
		t.Errorf("ok=%v status=%q", res.OK, res.Status)
	}
	// The message id is promised at schedule time so the delivered frame can be
	// correlated without a second API.
	if res.MessageID != "22222222-2222-7222-8222-222222222222" {
		t.Errorf("messageId = %q", res.MessageID)
	}
	if res.DeliverAt.IsZero() {
		t.Error("deliverAt was not parsed")
	}
}

func TestTimerScheduleMintsATxnWhenTheCallerDoesNotSupplyOne(t *testing.T) {
	// txn is MANDATORY on the wire (§20.2) and is overwritten by every
	// reschedule. The client mints one the same way Push does, so the common
	// case does not force the caller to carry a UUID generator.
	srv := newCaptureServer(t, okJSON(`{"results":[{"ok":true,"status":"scheduled","queue":"q","timerKey":"k","txn":"x","messageId":"y","deliverAt":"2026-08-17T10:00:00.000000Z"}]}`))
	client := newWireClient(t, srv.URL)

	if _, err := client.Timers().Schedule(kvCtx(t), TimerSchedule{
		Queue: "q", TimerKey: "k", Delay: time.Second, Payload: 1,
	}); err != nil {
		t.Fatalf("schedule: %v", err)
	}

	body, err := decodeAny(srv.only(t).Body)
	if err != nil {
		t.Fatalf("body: %v", err)
	}
	op := body.(map[string]interface{})["operations"].([]interface{})[0].(map[string]interface{})
	txn, _ := op["txn"].(string)
	if !IsValidUUID(txn) {
		t.Fatalf("txn = %q, want a minted UUID", txn)
	}
}

func TestTimerScheduleAcceptsBytesVerbatimAndPartition(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`{"results":[{"ok":true,"status":"rescheduled","queue":"q","timerKey":"k","txn":"t","messageId":"m","deliverAt":"2026-08-17T10:00:00.000000Z"}]}`))
	client := newWireClient(t, srv.URL)

	if _, err := client.Timers().Reschedule(kvCtx(t), TimerSchedule{
		Queue:         "q",
		TimerKey:      "k",
		Partition:     "eu-1",
		Delay:         90 * time.Second,
		Payload:       []byte("raw bytes"),
		TransactionID: "t",
	}); err != nil {
		t.Fatalf("reschedule: %v", err)
	}
	assertJSONBody(t, srv.only(t).Body, `{"operations":[{"op":"reschedule","queue":"q","timerKey":"k","partition":"eu-1","delayMs":90000,"txn":"t","payload":"`+base64.StdEncoding.EncodeToString([]byte("raw bytes"))+`"}]}`)
}

func TestTimerDeliverAtIsConvertedToARelativeDelay(t *testing.T) {
	// §4.2: only RELATIVE durations on the wire, never absolute instants — one
	// clock, Postgres's, so no inter-broker skew can enter. `DeliverAt` is SDK
	// sugar converted at send time, exactly like the KV's `until`.
	srv := newCaptureServer(t, okJSON(`{"results":[{"ok":true,"status":"scheduled","queue":"q","timerKey":"k","txn":"t","messageId":"m","deliverAt":"2026-08-17T10:00:00.000000Z"}]}`))
	client := newWireClient(t, srv.URL)

	if _, err := client.Timers().Schedule(kvCtx(t), TimerSchedule{
		Queue: "q", TimerKey: "k", DeliverAt: time.Now().Add(30 * time.Second), Payload: 1, TransactionID: "t",
	}); err != nil {
		t.Fatalf("schedule: %v", err)
	}
	body, _ := decodeAny(srv.only(t).Body)
	op := body.(map[string]interface{})["operations"].([]interface{})[0].(map[string]interface{})
	if _, present := op["deliverAt"]; present {
		t.Fatal("deliverAt must never reach the wire")
	}
	num, ok := op["delayMs"].(json.Number)
	if !ok {
		t.Fatalf("delayMs is %T, want a JSON number", op["delayMs"])
	}
	delay, err := num.Int64()
	if err != nil {
		t.Fatalf("delayMs is not an integer: %v", err)
	}
	if delay < 25000 || delay > 30000 {
		t.Fatalf("delayMs = %d, want ~30000", delay)
	}
}

func TestTimerScheduleRefusesBothDelayAndDeliverAt(t *testing.T) {
	srv := newCaptureServer(t)
	client := newWireClient(t, srv.URL)

	_, err := client.Timers().Schedule(kvCtx(t), TimerSchedule{
		Queue: "q", TimerKey: "k", Delay: time.Second, DeliverAt: time.Now().Add(time.Minute), Payload: 1,
	})
	if err == nil {
		t.Fatal("two ways of saying when is an ambiguity, not a convenience")
	}
	if n := len(srv.requests()); n != 0 {
		t.Fatalf("got %d requests, want 0", n)
	}
}

func TestTimerScheduleRequiresQueueAndKey(t *testing.T) {
	srv := newCaptureServer(t)
	client := newWireClient(t, srv.URL)
	ctx := kvCtx(t)

	if _, err := client.Timers().Schedule(ctx, TimerSchedule{TimerKey: "k", Delay: time.Second, Payload: 1}); err == nil {
		t.Error("a schedule without a queue must be refused")
	}
	if _, err := client.Timers().Schedule(ctx, TimerSchedule{Queue: "q", Delay: time.Second, Payload: 1}); err == nil {
		t.Error("a schedule without a timerKey must be refused")
	}
	if n := len(srv.requests()); n != 0 {
		t.Fatalf("got %d requests, want 0", n)
	}
}

func TestTimerCancelUsesItsOwnRouteAndIsNeverABatch(t *testing.T) {
	// §9.6: DELETE /api/v1/timers/:queue/*timerKey is the route that is
	// guaranteed never to be blocked. A cancel sent inside POST /api/v1/timers
	// inherits that route's class and is refused WHOLE on a blocked cluster, so
	// an SDK must cancel here.
	srv := newCaptureServer(t, okJSON(`{"ok":true,"status":"cancelled","queue":"reminders","timerKey":"order-7","txn":"t-7"}`))
	client := newWireClient(t, srv.URL)

	res, err := client.Timers().Cancel(kvCtx(t), "reminders", "order-7")
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	req := srv.only(t)
	if req.Method != http.MethodDelete || req.Path != "/api/v1/timers/reminders/order-7" {
		t.Fatalf("cancel went to %s %s", req.Method, req.Path)
	}
	if len(req.Body) != 0 {
		t.Errorf("cancel sent a body: %s", string(req.Body))
	}
	if !res.OK || res.Status != TimerStatusCancelled {
		t.Errorf("ok=%v status=%q", res.OK, res.Status)
	}
}

func TestTimerCancelEchoesTheExpectedTxn(t *testing.T) {
	// §4.4: `absent` means "no longer pending" and MAY MEAN ALREADY DELIVERED —
	// there is no tombstone. The authority is the log, so the cancel response
	// carries the txn to look for, and the caller may pre-declare it.
	srv := newCaptureServer(t, okJSON(`{"ok":false,"status":"absent","queue":"reminders","timerKey":"order-7","txn":"t-7"}`))
	client := newWireClient(t, srv.URL)

	res, err := client.Timers().Cancel(kvCtx(t), "reminders", "order-7", TimerCancelOptions{ExpectedTransactionID: "t-7"})
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	req := srv.only(t)
	if req.RawQuery != "txn=t-7" {
		t.Fatalf("query = %q, want txn=t-7", req.RawQuery)
	}
	// absent carries ok:false and is NOT an error: the same lesson already paid
	// in-house on queue delete, where deleted:false with a 200 read as success.
	if res.OK {
		t.Error("absent must be ok:false")
	}
	if res.Status != TimerStatusAbsent || res.TransactionID != "t-7" {
		t.Errorf("status=%q txn=%q", res.Status, res.TransactionID)
	}
}

func TestTimerTooLateIsAVerdictNotAnError(t *testing.T) {
	// §4.3: a cancel or reschedule that lands on a claimed timer answers
	// too_late with HTTP 200. It is a verdict, not a failure.
	srv := newCaptureServer(t, okJSON(`{"ok":false,"status":"too_late","queue":"q","timerKey":"k"}`))
	client := newWireClient(t, srv.URL)

	res, err := client.Timers().Cancel(kvCtx(t), "q", "k")
	if err != nil {
		t.Fatalf("too_late must not be an error: %v", err)
	}
	if res.Status != TimerStatusTooLate || res.OK {
		t.Errorf("status=%q ok=%v", res.Status, res.OK)
	}
}

func TestTimerPeekDecodesThePayload(t *testing.T) {
	payload := base64.StdEncoding.EncodeToString([]byte(`{"orderId":7}`))
	srv := newCaptureServer(t, okJSON(`{"found":true,"queue":"reminders","timerKey":"order-7","partition":"Default","deliverAt":"2026-08-17T10:00:00.000000Z","txn":"t-7","messageId":"m-7","payload":"`+payload+`","payloadZstd":false,"encrypted":false,"producerSub":"billing","attempts":2,"lastError":"boom","claimed":false,"createdAt":"2026-08-17T09:00:00.000000Z","updatedAt":"2026-08-17T09:30:00.000000Z"}`))
	client := newWireClient(t, srv.URL)

	info, err := client.Timers().Peek(kvCtx(t), "reminders", "order-7")
	if err != nil {
		t.Fatalf("peek: %v", err)
	}
	req := srv.only(t)
	if req.Method != http.MethodGet || req.Path != "/api/v1/timers/reminders/order-7" {
		t.Fatalf("peek went to %s %s", req.Method, req.Path)
	}
	if !info.Found || string(info.Payload) != `{"orderId":7}` {
		t.Fatalf("info = %+v payload=%q", info, string(info.Payload))
	}
	if info.Attempts != 2 || info.LastError != "boom" {
		t.Errorf("attempts=%d lastError=%q", info.Attempts, info.LastError)
	}
	// A row in backoff has claimed_until in the future and claim_token NULL, and
	// is still cancellable — so it reads claimed:false, deliberately.
	if info.Claimed {
		t.Error("claimed should be false")
	}
	if info.DeliverAt.IsZero() || info.CreatedAt.IsZero() {
		t.Error("timestamps were not parsed")
	}
}

func TestTimerPeekMissIsNotAnError(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`{"found":false,"queue":"q","timerKey":"k"}`))
	client := newWireClient(t, srv.URL)

	info, err := client.Timers().Peek(kvCtx(t), "q", "k")
	if err != nil {
		t.Fatalf("a miss is 200 with found:false, not a 404: %v", err)
	}
	if info.Found {
		t.Error("found should be false")
	}
}

func TestTimerListIsKeysetOverOneQueue(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`{"rows":[{"queue":"q","timerKey":"a","partition":"Default","deliverAt":"2026-08-17T10:00:00.000000Z","txn":"t","messageId":"m","payloadZstd":false,"encrypted":false,"producerSub":null,"attempts":0,"lastError":null,"claimed":false,"createdAt":"2026-08-17T09:00:00.000000Z","updatedAt":"2026-08-17T09:00:00.000000Z"}],"truncated":true,"nextAfter":"a"}`))
	client := newWireClient(t, srv.URL)

	page, err := client.Timers().List(kvCtx(t), "q", TimerListOptions{After: "0", Limit: 25})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	req := srv.only(t)
	if req.Method != http.MethodGet || req.Path != "/api/v1/timers/q" {
		t.Fatalf("list went to %s %s", req.Method, req.Path)
	}
	if req.RawQuery != "after=0&limit=25" {
		t.Fatalf("query = %q", req.RawQuery)
	}
	if len(page.Rows) != 1 || !page.Truncated || page.NextAfter != "a" {
		t.Fatalf("page = %+v", page)
	}
	// The list never carries payloads: that is what peek is for.
	if page.Rows[0].Payload != nil {
		t.Error("list must not carry payloads")
	}
}

func TestTimerKillSwitchIsATypedError(t *testing.T) {
	// The schedule half can be paused at run time by an operator
	// (`timers_schedule_enabled`), and that is a 503 with Retry-After, not a 404:
	// the surface is on every cell now that QUEEN_TIMERS_ENABLED is gone, so
	// "this broker does not have timers" is not an answer any more.
	//
	// The FIRE half has its own switch and its own asymmetry: stopping the
	// schedule promises nothing, stopping the fire accumulates promised work. A
	// client sees neither directly — a timer that does not fire is silence, which
	// is why the schedule refusal is the one that must be legible.
	srv := newCaptureServer(t, cannedResponse{
		status: http.StatusServiceUnavailable,
		body:   `{"error":"timers_disabled","reason":"timers_disabled"}`,
		header: map[string]string{"Retry-After": "1"},
	})
	client := newWireClient(t, srv.URL)

	_, err := client.Timers().Schedule(kvCtx(t), TimerSchedule{Queue: "q", TimerKey: "k", Delay: time.Minute, Payload: 1})
	var se *SurfaceError
	if !errors.As(err, &se) {
		t.Fatalf("error is %T, want *SurfaceError: %v", err, err)
	}
	if se.Code != "timers_disabled" || se.Reason != "timers_disabled" || se.StatusCode != 503 {
		t.Errorf("code=%q reason=%q status=%d", se.Code, se.Reason, se.StatusCode)
	}
}

func TestTimerHorizonExceededKeepsItsCode(t *testing.T) {
	// A delay beyond the horizon is 403 with its own code, not a 400: it is a
	// plan verdict (§9.5), and a client that retried it forever would be wrong.
	srv := newCaptureServer(t, cannedResponse{
		status: http.StatusForbidden,
		body:   `{"error":"timer_horizon_exceeded","reason":"timers_horizon","detail":"op at index 0: delayMs 999999999 is beyond the 604800000 ms horizon in force here"}`,
	})
	client := newWireClient(t, srv.URL)

	_, err := client.Timers().Schedule(kvCtx(t), TimerSchedule{Queue: "q", TimerKey: "k", Delay: time.Hour, Payload: 1, TransactionID: "t"})
	var se *SurfaceError
	if !errors.As(err, &se) {
		t.Fatalf("error is %T, want *SurfaceError", err)
	}
	if se.Code != "timer_horizon_exceeded" || se.StatusCode != 403 {
		t.Errorf("code=%q status=%d", se.Code, se.StatusCode)
	}
}

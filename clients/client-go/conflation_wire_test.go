package queen

import (
	"context"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

// Conflation, client side (PLAN_CONFLATION.md §4, the client-go row; §7.2 "the
// same three tests per SDK").
//
// The three things a client can be wrong about, and why each one is asserted
// against the raw query string rather than against "it worked":
//
//  1. THE FLAG MUST REACH THE WIRE FROM BOTH BUILDERS. pop() and consume() have
//     SEPARATE param builders in this SDK (QueueBuilder.buildPopParams vs
//     ConsumerManager.buildParams) — the hazard the plan calls out in §4 and the
//     one a JS bug of exactly this shape already left a comment about. A test
//     that only drives consume() would pass with a pop() that silently drops the
//     option.
//
//  2. DEGRADING MUST BE LOUD. Against a broker older than 1.1.0 the unknown
//     query param is ignored, the pop returns the WHOLE backlog, and a consumer
//     that asked for last-value delivery quietly processes every stale message.
//     Nothing in the response, the status code or the message shape says so. The
//     only usable signal is the ECHO — the broker emits "conflation":true on
//     every conflating pop, including empty ones — so its absence is the error
//     (§4 blockquote).
//
//  3. A DECLARATION CONFLICT MUST NOT BE THE SAME EVENT AS AN OLD BROKER.
//     conflict = a NEW broker answering "the group is already registered the
//     other way, my stored value wins" (§3.3). It is warned about once and the
//     consumer keeps working (§7.3 E2E-4: "both consumers keep working"), which
//     is what makes rolling deploys survivable. Only the absence of BOTH keys is
//     an old broker.

func conflationCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// queryOf parses one captured request's query string.
func queryOf(t *testing.T, req capturedRequest) url.Values {
	t.Helper()
	v, err := url.ParseQuery(req.RawQuery)
	if err != nil {
		t.Fatalf("captured query is not parseable: %v (%q)", err, req.RawQuery)
	}
	return v
}

// popRequests returns the captured requests that are pops, so a consume test can
// ignore the acks the auto-ack loop interleaves.
func popRequests(reqs []capturedRequest) []capturedRequest {
	var out []capturedRequest
	for _, r := range reqs {
		if strings.HasPrefix(r.Path, "/api/v1/pop") {
			out = append(out, r)
		}
	}
	return out
}

// captureConflationWarnings swaps the once-per-(queue,group) warning sink for a
// counter. The registry that gates it is process-global by design (§4: "one
// warning per (queue,group) per process"), so every test below uses its own
// queue name instead of resetting it — which also proves the key is the pair and
// not a single global latch.
func captureConflationWarnings(t *testing.T) func() []string {
	t.Helper()
	var mu sync.Mutex
	var seen []string
	prev := conflationConflictWarn
	conflationConflictWarn = func(queue, group string) {
		mu.Lock()
		defer mu.Unlock()
		seen = append(seen, queue+"/"+group)
	}
	t.Cleanup(func() { conflationConflictWarn = prev })
	return func() []string {
		mu.Lock()
		defer mu.Unlock()
		out := make([]string, len(seen))
		copy(out, seen)
		return out
	}
}

// --- 1. the flag reaches the wire, from both builders ----------------------

func TestConflationReachesTheWireFromPop(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`{"messages":[],"conflation":true}`))
	client := newWireClient(t, srv.URL)

	if _, err := client.Queue("cfl-pop").Group("workers").Conflation(true).Wait(false).Pop(conflationCtx(t)); err != nil {
		t.Fatalf("pop: %v", err)
	}

	q := queryOf(t, srv.only(t))
	if got := q.Get("conflation"); got != "true" {
		t.Fatalf("pop query conflation = %q, want \"true\" (raw: %q)", got, srv.only(t).RawQuery)
	}
}

func TestConflationIsAbsentFromThePopWireWhenNotRequested(t *testing.T) {
	// Byte-identical requests for every consumer that does not opt in (§8,
	// "default off"): the param is emitted ONLY when true, never as
	// conflation=false.
	srv := newCaptureServer(t, okJSON(`{"messages":[]}`))
	client := newWireClient(t, srv.URL)

	if _, err := client.Queue("cfl-pop-off").Group("workers").Conflation(false).Wait(false).Pop(conflationCtx(t)); err != nil {
		t.Fatalf("pop: %v", err)
	}

	q := queryOf(t, srv.only(t))
	if _, present := q["conflation"]; present {
		t.Fatalf("conflation must not appear on the wire when off; raw query: %q", srv.only(t).RawQuery)
	}
}

func TestConflationReachesTheWireFromConsume(t *testing.T) {
	// Drives the real consume loop, not buildParams directly: the point is to
	// prove the whole path (builder field -> ConsumeOptions -> buildParams).
	srv := newCaptureServer(t, okJSON(`{"messages":[{"transactionId":"11111111-1111-4111-8111-111111111111","partitionId":"22222222-2222-4222-8222-222222222222","queue":"cfl-consume","partition":"Default","data":{"n":1},"createdAt":"2026-08-21T10:00:00.000Z"}],"conflation":true}`))
	client := newWireClient(t, srv.URL)

	var handled int
	err := client.Queue("cfl-consume").
		Group("workers").
		Conflation(true).
		Wait(false).
		Limit(1).
		Consume(conflationCtx(t), func(ctx context.Context, msg *Message) error {
			handled++
			return nil
		}).Execute(conflationCtx(t))
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	if handled != 1 {
		t.Fatalf("handler ran %d times, want 1", handled)
	}

	pops := popRequests(srv.requests())
	if len(pops) == 0 {
		t.Fatal("consume made no pop request")
	}
	q := queryOf(t, pops[0])
	if got := q.Get("conflation"); got != "true" {
		t.Fatalf("consume query conflation = %q, want \"true\" (raw: %q)", got, pops[0].RawQuery)
	}
}

func TestConflationIsAbsentFromTheConsumeWireWhenNotRequested(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`{"messages":[]}`))
	client := newWireClient(t, srv.URL)

	params := NewConsumerManager(client.httpClient, client).buildParams(
		client.Queue("cfl-consume-off").Group("workers").getConsumeOptions())
	v, err := url.ParseQuery(params)
	if err != nil {
		t.Fatalf("params are not parseable: %v (%q)", err, params)
	}
	if _, present := v["conflation"]; present {
		t.Fatalf("conflation must not appear on the wire when off; params: %q", params)
	}
}

// --- 2. degrade loudly ------------------------------------------------------

func TestPopErrorsWhenTheBrokerDoesNotEchoConflation(t *testing.T) {
	// The old-broker case: the param was ignored, so neither key comes back.
	// This must be an ERROR and not a warning — a warning on stderr is exactly
	// what nobody reads while the consumer chews through a 4M backlog.
	srv := newCaptureServer(t, okJSON(`{"messages":[]}`))
	client := newWireClient(t, srv.URL)

	_, err := client.Queue("cfl-old-pop").Group("workers").Conflation(true).Wait(false).Pop(conflationCtx(t))
	if err == nil {
		t.Fatal("pop against a broker that ignored conflation returned no error")
	}
	if !strings.Contains(err.Error(), "requires broker >= 1.1.0") {
		t.Fatalf("error does not name the version requirement: %v", err)
	}
}

func TestPopDoesNotErrorWhenConflationWasNotRequested(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`{"messages":[]}`))
	client := newWireClient(t, srv.URL)

	if _, err := client.Queue("cfl-noflag").Group("workers").Wait(false).Pop(conflationCtx(t)); err != nil {
		t.Fatalf("a pop that never asked for conflation must not care about the echo: %v", err)
	}
}

func TestConsumeStopsWhenTheBrokerDoesNotEchoConflation(t *testing.T) {
	// The echo is emitted on EMPTY pops too, so the loop dies on the first
	// round trip — before a single stale message is handed to the handler.
	srv := newCaptureServer(t,
		okJSON(`{"messages":[{"transactionId":"11111111-1111-4111-8111-111111111111","partitionId":"22222222-2222-4222-8222-222222222222","queue":"cfl-old-consume","partition":"Default","data":{"n":1},"createdAt":"2026-08-21T10:00:00.000Z"}]}`),
	)
	client := newWireClient(t, srv.URL)

	var handled int
	err := client.Queue("cfl-old-consume").
		Group("workers").
		Conflation(true).
		Wait(false).
		Consume(conflationCtx(t), func(ctx context.Context, msg *Message) error {
			handled++
			return nil
		}).Execute(conflationCtx(t))

	if err == nil {
		t.Fatal("consume against a broker that ignored conflation ran on without an error")
	}
	if !strings.Contains(err.Error(), "requires broker >= 1.1.0") {
		t.Fatalf("error does not name the version requirement: %v", err)
	}
	if handled != 0 {
		t.Fatalf("handler ran %d times; a degraded pop must not be processed", handled)
	}
	if n := len(popRequests(srv.requests())); n != 1 {
		t.Fatalf("consume made %d pops, want exactly 1 (the loop must stop on the first)", n)
	}
}

// --- 3. the conflict warning: once per (queue, group), and NOT an error ------

func TestConflationConflictWarnsExactlyOncePerQueueAndGroup(t *testing.T) {
	warnings := captureConflationWarnings(t)
	srv := newCaptureServer(t,
		okJSON(`{"messages":[],"conflation":true,"conflationConflict":true}`),
		okJSON(`{"messages":[],"conflation":true,"conflationConflict":true}`),
		okJSON(`{"messages":[],"conflation":true,"conflationConflict":true}`),
	)
	client := newWireClient(t, srv.URL)
	ctx := conflationCtx(t)

	for i := 0; i < 3; i++ {
		if _, err := client.Queue("cfl-conflict").Group("workers").Conflation(true).Wait(false).Pop(ctx); err != nil {
			t.Fatalf("pop %d: %v", i, err)
		}
	}

	got := warnings()
	if len(got) != 1 {
		t.Fatalf("emitted %d warnings %v, want exactly 1", len(got), got)
	}
	if got[0] != "cfl-conflict/workers" {
		t.Fatalf("warning keyed on %q, want \"cfl-conflict/workers\"", got[0])
	}
}

func TestConflationConflictWarnsAgainForADifferentGroup(t *testing.T) {
	warnings := captureConflationWarnings(t)
	srv := newCaptureServer(t,
		okJSON(`{"messages":[],"conflation":true,"conflationConflict":true}`),
		okJSON(`{"messages":[],"conflation":true,"conflationConflict":true}`),
	)
	client := newWireClient(t, srv.URL)
	ctx := conflationCtx(t)

	for _, group := range []string{"alpha", "beta"} {
		if _, err := client.Queue("cfl-conflict-groups").Group(group).Conflation(true).Wait(false).Pop(ctx); err != nil {
			t.Fatalf("pop %s: %v", group, err)
		}
	}

	got := warnings()
	if len(got) != 2 {
		t.Fatalf("emitted %d warnings %v, want 2 (the gate is per (queue,group), not global)", len(got), got)
	}
}

func TestConflationConflictIsNotAnOldBroker(t *testing.T) {
	// A conflict where the STORED value is false: the broker answers with
	// conflationConflict but no "conflation":true, because the effective flag is
	// false. That is a live 1.1.0 broker telling this consumer the group already
	// exists the other way — group-setting-wins, warn, keep working (§3.3, Q3).
	// Treating it as an old broker would take down exactly the half of a rolling
	// fleet that is already correct.
	warnings := captureConflationWarnings(t)
	srv := newCaptureServer(t, okJSON(`{"messages":[],"conflationConflict":true}`))
	client := newWireClient(t, srv.URL)

	if _, err := client.Queue("cfl-conflict-stored-off").Group("workers").Conflation(true).Wait(false).Pop(conflationCtx(t)); err != nil {
		t.Fatalf("a declaration conflict must not fail the pop: %v", err)
	}
	if got := warnings(); len(got) != 1 {
		t.Fatalf("emitted %d warnings %v, want exactly 1", len(got), got)
	}
}

// --- depth: the two-number surface reaches the caller -----------------------

func TestQueueDepthCarriesTheConflationFields(t *testing.T) {
	// GetQueueDepth hands back the decoded body, so the three fields of §5.3
	// ride through for free — which is exactly why this is worth pinning: the
	// day someone types this response into a struct, the fields that make a
	// 4M-deep conflating queue readable as HEALTHY would vanish silently.
	srv := newCaptureServer(t, okJSON(`{"queue":"cfl-depth","group":"workers","pending":4000000,"partitionsPending":12,"conflation":true,"effectivePending":12,"partitions":[{"partition":"Default","pending":4000000}]}`))
	client := newWireClient(t, srv.URL)

	depth, err := client.Admin().GetQueueDepth(conflationCtx(t), "cfl-depth", "workers")
	if err != nil {
		t.Fatalf("depth: %v", err)
	}
	if got := srv.only(t).Path; got != "/api/v1/resources/queues/cfl-depth/depth" {
		t.Fatalf("depth went to %q", got)
	}
	if conflating, _ := depth["conflation"].(bool); !conflating {
		t.Fatalf("conflation = %v, want true", depth["conflation"])
	}
	for _, field := range []string{"partitionsPending", "effectivePending"} {
		v, ok := depth[field].(float64)
		if !ok {
			t.Fatalf("%s missing from the depth payload (%v)", field, depth[field])
		}
		if v != 12 {
			t.Fatalf("%s = %v, want 12", field, v)
		}
	}
	// The distinction is the whole point: log depth stays the big number.
	if pending, _ := depth["pending"].(float64); pending != 4000000 {
		t.Fatalf("pending = %v, want 4000000 (log depth, not work depth)", depth["pending"])
	}
}

// --- streams: the option survives the Source adapter ------------------------

func TestConflationThreadsThroughTheStreamSourceAdapter(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`{"messages":[],"conflation":true}`))
	client := newWireClient(t, srv.URL)

	src := client.Queue("cfl-stream").Group("workers").Wait(false).AsStreamSource().Conflation(true)
	if _, err := src.Pop(conflationCtx(t)); err != nil {
		t.Fatalf("stream source pop: %v", err)
	}

	q := queryOf(t, srv.only(t))
	if got := q.Get("conflation"); got != "true" {
		t.Fatalf("stream source query conflation = %q, want \"true\" (raw: %q)", got, srv.only(t).RawQuery)
	}
}

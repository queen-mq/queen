package queen

import (
	"context"
	"strings"
	"testing"
	"time"
)

// Pop autopilot, client side.
//
// The four things a client can be wrong about here, and why each is asserted
// against the WHOLE query string rather than against one parameter:
//
//  1. BOTH BUILDERS MUST AGREE. pop and consume assemble their query strings
//     separately (QueueBuilder.buildPopParams vs ConsumerManager.buildParams) —
//     the hazard the conflation work already left a comment about. Every case
//     below is run through both, and both are compared to the same expected
//     string, so a rule implemented in one and not the other cannot pass.
//
//  2. NOT ENGAGING AUTOPILOT MUST BE BYTE-IDENTICAL TO THE OLD SDK. The escape
//     hatch is only worth having if it is exact, and "exact" is not something a
//     test of one parameter can show: a stray autopilot=true, or a batch that
//     stopped being emitted, is a different request. Hence full-string equality
//     including the parameters this feature never touches.
//
//  3. AN EXPLICIT VALUE IS SACRED, PER DIMENSION. Partitions(1) and "never
//     called Partitions" both used to reach the wire as nothing at all; they are
//     now different requests, and the pinned one must survive autopilot.
//
//  4. THE ADDITIVE RESPONSE FIELD MUST NOT BE LOAD-BEARING. A broker that does
//     not send it, sends it half-filled, or sends it with fields this SDK has
//     never heard of, all have to work.

func autopilotCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// newOfflineClient builds a client that never talks to anything, for the tests
// that assert on the params a builder produces rather than on a round trip.
func newOfflineClient(t *testing.T) *Queen {
	t.Helper()
	client, err := New(ClientConfig{URL: "http://127.0.0.1:1", TimeoutMillis: 1000})
	if err != nil {
		t.Fatalf("failed to build client: %v", err)
	}
	t.Cleanup(func() { client.httpClient.Close() })
	return client
}

// --- 1. param assembly, from both builders ----------------------------------

// popParams returns the query string QueueBuilder.Pop would send, and
// consumeParams the one the consume loop would send, for the same builder
// configuration. Every case below asserts on both.
func popParams(qb *QueueBuilder) string {
	return qb.buildPopParams()
}

func consumeParams(client *Queen, qb *QueueBuilder) string {
	return NewConsumerManager(client.httpClient, client).buildParams(qb.getConsumeOptions())
}

func TestAutopilotParamAssemblyFromBothBuilders(t *testing.T) {
	// The shared spine of every case: a named queue and group, no long poll,
	// default timeout. Everything that varies below is sizing. url.Values
	// encodes in key order, so the fixed parts sit either side of partitions.
	const group = "consumerGroup=g"
	const tail = "timeout=30000&wait=false"

	cases := []struct {
		name  string
		build func(qb *QueueBuilder) *QueueBuilder
		want  string
	}{
		{
			// (a) nothing set: both knobs go to the broker, neither travels.
			name:  "nothing set",
			build: func(qb *QueueBuilder) *QueueBuilder { return qb },
			want:  "autopilot=true&" + group + "&" + tail,
		},
		{
			// (b) partitions pinned, batch left to the broker.
			name:  "partitions only",
			build: func(qb *QueueBuilder) *QueueBuilder { return qb.Partitions(4) },
			want:  "autopilot=true&" + group + "&partitions=4&" + tail,
		},
		{
			// (b') the pin that used to be indistinguishable from unset.
			// Partitions(1) is a decision — hold this consumer to one partition
			// — and the broker has to be told, or autopilot would widen it.
			name:  "partitions pinned to one",
			build: func(qb *QueueBuilder) *QueueBuilder { return qb.Partitions(1) },
			want:  "autopilot=true&" + group + "&partitions=1&" + tail,
		},
		{
			// (c) batch pinned, sweep width left to the broker.
			name:  "batch only",
			build: func(qb *QueueBuilder) *QueueBuilder { return qb.Batch(50) },
			want:  "autopilot=true&batch=50&" + group + "&" + tail,
		},
		{
			// (d) both set: nothing left to decide, so no autopilot parameter
			// and the exact request the pre-autopilot SDK sent.
			name:  "both set",
			build: func(qb *QueueBuilder) *QueueBuilder { return qb.Batch(50).Partitions(4) },
			want:  "batch=50&" + group + "&partitions=4&" + tail,
		},
		{
			// (d') both set with partitions at 1: still byte-identical to the
			// old SDK, which never emitted partitions=1.
			name:  "both set, partitions one",
			build: func(qb *QueueBuilder) *QueueBuilder { return qb.Batch(50).Partitions(1) },
			want:  "batch=50&" + group + "&" + tail,
		},
		{
			// (e) escape hatch, nothing set: the client-side defaults are back.
			name:  "autopilot off, nothing set",
			build: func(qb *QueueBuilder) *QueueBuilder { return qb.Autopilot(false) },
			want:  "batch=1&" + group + "&" + tail,
		},
		{
			// (e') escape hatch with a pin: partitions=1 stays off the wire,
			// exactly as before autopilot existed.
			name:  "autopilot off, partitions pinned to one",
			build: func(qb *QueueBuilder) *QueueBuilder { return qb.Autopilot(false).Partitions(1) },
			want:  "batch=1&" + group + "&" + tail,
		},
		{
			name:  "autopilot off, both set",
			build: func(qb *QueueBuilder) *QueueBuilder { return qb.Autopilot(false).Batch(50).Partitions(4) },
			want:  "batch=50&" + group + "&partitions=4&" + tail,
		},
		{
			// Autopilot(true) is the default, spelled out. It must not change
			// anything, including for a caller who set both knobs.
			name:  "autopilot explicitly on, both set",
			build: func(qb *QueueBuilder) *QueueBuilder { return qb.Autopilot(true).Batch(50).Partitions(4) },
			want:  "batch=50&" + group + "&partitions=4&" + tail,
		},
	}

	client := newOfflineClient(t)

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := popParams(tc.build(client.Queue("q").Group("g").Wait(false))); got != tc.want {
				t.Fatalf("pop params\n got: %s\nwant: %s", got, tc.want)
			}
			if got := consumeParams(client, tc.build(client.Queue("q").Group("g").Wait(false))); got != tc.want {
				t.Fatalf("consume params\n got: %s\nwant: %s", got, tc.want)
			}
		})
	}
}

func TestAutopilotBatchZeroIsUnset(t *testing.T) {
	// Batch(0) is not "a batch of zero" and never was: it is the absence of an
	// opinion, which now means the broker decides.
	client := newOfflineClient(t)
	qb := client.Queue("q").Group("g").Wait(false).Batch(0)

	want := "autopilot=true&consumerGroup=g&timeout=30000&wait=false"
	if got := popParams(qb); got != want {
		t.Fatalf("pop params\n got: %s\nwant: %s", got, want)
	}
	if got := consumeParams(client, qb); got != want {
		t.Fatalf("consume params\n got: %s\nwant: %s", got, want)
	}
}

func TestAutopilotEnvVarDisablesIt(t *testing.T) {
	// The process-wide rollback, read once at New. A client built while the
	// variable is set sends the pre-autopilot request and nothing else.
	t.Setenv(EnvPopAutopilot, "off")
	client := newOfflineClient(t)

	want := "batch=1&consumerGroup=g&timeout=30000&wait=false"
	if got := popParams(client.Queue("q").Group("g").Wait(false)); got != want {
		t.Fatalf("pop params with %s=off\n got: %s\nwant: %s", EnvPopAutopilot, got, want)
	}
	if got := consumeParams(client, client.Queue("q").Group("g").Wait(false)); got != want {
		t.Fatalf("consume params with %s=off\n got: %s\nwant: %s", EnvPopAutopilot, got, want)
	}

	// A builder that asks for autopilot explicitly outranks the environment:
	// the variable is a default, not a lock.
	wantOn := "autopilot=true&consumerGroup=g&timeout=30000&wait=false"
	if got := popParams(client.Queue("q").Group("g").Wait(false).Autopilot(true)); got != wantOn {
		t.Fatalf("explicit Autopilot(true) did not override the environment\n got: %s\nwant: %s", got, wantOn)
	}
}

func TestAutopilotEnvVarValues(t *testing.T) {
	for _, v := range []string{"off", "OFF", " off ", "false", "0", "no", "disabled"} {
		t.Setenv(EnvPopAutopilot, v)
		if !popAutopilotDisabledByEnv() {
			t.Fatalf("%s=%q should disable autopilot", EnvPopAutopilot, v)
		}
	}
	for _, v := range []string{"", "on", "true", "1", "yes", "nonsense"} {
		t.Setenv(EnvPopAutopilot, v)
		if popAutopilotDisabledByEnv() {
			t.Fatalf("%s=%q should leave autopilot on", EnvPopAutopilot, v)
		}
	}
}

func TestAutopilotRawConsumeOptionsAreByteIdenticalWhenOff(t *testing.T) {
	// The path that does not go through the fluent builder: a ConsumeOptions
	// built by hand and handed to ConsumerManager. With autopilot off it must
	// still produce what it produced before — including emitting batch=0 for a
	// zero Batch, which is what this builder has always done (no client-side
	// default is applied here; the fluent path applies them upstream).
	client := newOfflineClient(t)
	cm := NewConsumerManager(client.httpClient, client)
	off := false

	got := cm.buildParams(ConsumeOptions{Queue: "q", Group: "g", Autopilot: &off})
	want := "batch=0&consumerGroup=g&timeout=0&wait=false"
	if got != want {
		t.Fatalf("raw ConsumeOptions with autopilot off\n got: %s\nwant: %s", got, want)
	}

	// And with autopilot on (nil = client default), the same zero values are
	// read as "unset" and left to the broker.
	got = cm.buildParams(ConsumeOptions{Queue: "q", Group: "g"})
	want = "autopilot=true&consumerGroup=g&timeout=0&wait=false"
	if got != want {
		t.Fatalf("raw ConsumeOptions with autopilot on\n got: %s\nwant: %s", got, want)
	}
}

// --- 2. the params reach the wire, end to end -------------------------------

func TestAutopilotReachesTheWireFromPop(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`{"messages":[]}`))
	client := newWireClient(t, srv.URL)

	if _, err := client.Queue("ap-pop").Group("workers").Wait(false).Pop(autopilotCtx(t)); err != nil {
		t.Fatalf("pop: %v", err)
	}

	q := queryOf(t, srv.only(t))
	if got := q.Get("autopilot"); got != "true" {
		t.Fatalf("pop query autopilot = %q, want \"true\" (raw: %q)", got, srv.only(t).RawQuery)
	}
	for _, absent := range []string{"batch", "partitions"} {
		if _, present := q[absent]; present {
			t.Fatalf("%s must not be sent when the broker is choosing it; raw query: %q", absent, srv.only(t).RawQuery)
		}
	}
}

func TestAutopilotReachesTheWireFromConsume(t *testing.T) {
	// Drives the real consume loop: builder field -> ConsumeOptions ->
	// buildParams -> wire.
	srv := newCaptureServer(t, okJSON(`{"messages":[{"transactionId":"11111111-1111-4111-8111-111111111111","partitionId":"22222222-2222-4222-8222-222222222222","queue":"ap-consume","partition":"Default","data":{"n":1},"createdAt":"2026-08-23T10:00:00.000Z"}]}`))
	client := newWireClient(t, srv.URL)

	var handled int
	err := client.Queue("ap-consume").
		Group("workers").
		Wait(false).
		Limit(1).
		Consume(autopilotCtx(t), func(ctx context.Context, msg *Message) error {
			handled++
			return nil
		}).Execute(autopilotCtx(t))
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
	if got := q.Get("autopilot"); got != "true" {
		t.Fatalf("consume query autopilot = %q, want \"true\" (raw: %q)", got, pops[0].RawQuery)
	}
	for _, absent := range []string{"batch", "partitions"} {
		if _, present := q[absent]; present {
			t.Fatalf("%s must not be sent when the broker is choosing it; raw query: %q", absent, pops[0].RawQuery)
		}
	}
}

// --- 3. the additive response field -----------------------------------------

func TestParseAutopilotDecision(t *testing.T) {
	cases := []struct {
		name string
		in   map[string]interface{}
		want *AutopilotDecision
	}{
		{"nil response", nil, nil},
		{"absent", map[string]interface{}{"messages": []interface{}{}}, nil},
		{"null", map[string]interface{}{"autopilot": nil}, nil},
		{"not an object", map[string]interface{}{"autopilot": true}, nil},
		{
			"complete",
			map[string]interface{}{"autopilot": map[string]interface{}{
				"partitions": float64(8), "batch": float64(200), "waitMs": float64(25),
			}},
			&AutopilotDecision{Partitions: 8, Batch: 200, WaitMillis: 25},
		},
		{
			// waitMs is optional: the broker sends it only when it has an
			// opinion about pacing.
			"no waitMs",
			map[string]interface{}{"autopilot": map[string]interface{}{
				"partitions": float64(4), "batch": float64(64),
			}},
			&AutopilotDecision{Partitions: 4, Batch: 64},
		},
		{
			// Forward compatibility: a newer broker growing a field must not
			// cost this client the fields it does understand.
			"unknown fields inside",
			map[string]interface{}{"autopilot": map[string]interface{}{
				"partitions": float64(2), "batch": float64(10), "waitMs": float64(5),
				"reason": "ready_age", "confidence": float64(0.9),
			}},
			&AutopilotDecision{Partitions: 2, Batch: 10, WaitMillis: 5},
		},
		{
			// A field of the wrong type is dropped, not fatal.
			"wrong types inside",
			map[string]interface{}{"autopilot": map[string]interface{}{
				"partitions": "eight", "batch": float64(10),
			}},
			&AutopilotDecision{Batch: 10},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := parseAutopilotDecision(tc.in)
			switch {
			case tc.want == nil && got != nil:
				t.Fatalf("got %+v, want nil", got)
			case tc.want != nil && got == nil:
				t.Fatalf("got nil, want %+v", tc.want)
			case tc.want != nil && *got != *tc.want:
				t.Fatalf("got %+v, want %+v", *got, *tc.want)
			}
		})
	}
}

func TestPopResultCarriesTheAutopilotDecision(t *testing.T) {
	// An unknown top-level field rides along too: pop responses are decoded
	// into a map, so a broker that grows one must not break this client.
	srv := newCaptureServer(t, okJSON(`{"messages":[],"autopilot":{"partitions":8,"batch":200,"waitMs":25},"somethingNewer":{"x":1}}`))
	client := newWireClient(t, srv.URL)

	res, err := client.Queue("ap-res").Group("workers").Wait(false).PopResult(autopilotCtx(t))
	if err != nil {
		t.Fatalf("pop: %v", err)
	}
	if res.Autopilot == nil {
		t.Fatal("autopilot decision was not parsed out of the response")
	}
	if *res.Autopilot != (AutopilotDecision{Partitions: 8, Batch: 200, WaitMillis: 25}) {
		t.Fatalf("decision = %+v", *res.Autopilot)
	}
	if len(res.Messages) != 0 {
		t.Fatalf("messages = %d, want 0", len(res.Messages))
	}
}

func TestPopResultAutopilotIsNilWithoutTheField(t *testing.T) {
	// The old-broker shape, and the shape of every pop that did not engage
	// autopilot. It is not an error, and there is nothing to report.
	srv := newCaptureServer(t, okJSON(`{"messages":[{"transactionId":"11111111-1111-4111-8111-111111111111","partitionId":"22222222-2222-4222-8222-222222222222","queue":"ap-old","partition":"Default","data":{"n":1},"createdAt":"2026-08-23T10:00:00.000Z"}]}`))
	client := newWireClient(t, srv.URL)

	res, err := client.Queue("ap-old").Group("workers").Wait(false).PopResult(autopilotCtx(t))
	if err != nil {
		t.Fatalf("pop against a broker that knows nothing about autopilot: %v", err)
	}
	if res.Autopilot != nil {
		t.Fatalf("autopilot = %+v, want nil", *res.Autopilot)
	}
	if len(res.Messages) != 1 {
		t.Fatalf("messages = %d, want 1", len(res.Messages))
	}
}

func TestPopStillReturnsMessagesOnly(t *testing.T) {
	// Pop's signature is unchanged and PopResult is the same request: the
	// additive field must not have cost the plain path anything.
	srv := newCaptureServer(t, okJSON(`{"messages":[{"transactionId":"11111111-1111-4111-8111-111111111111","partitionId":"22222222-2222-4222-8222-222222222222","queue":"ap-plain","partition":"Default","data":{"n":1},"createdAt":"2026-08-23T10:00:00.000Z"}],"autopilot":{"partitions":1,"batch":1}}`))
	client := newWireClient(t, srv.URL)

	msgs, err := client.Queue("ap-plain").Group("workers").Wait(false).Pop(autopilotCtx(t))
	if err != nil {
		t.Fatalf("pop: %v", err)
	}
	if len(msgs) != 1 || msgs[0].Partition != "Default" {
		t.Fatalf("messages = %+v", msgs)
	}
}

func TestPopResultValidatesTheTargetLikePop(t *testing.T) {
	client := newOfflineClient(t)
	if _, err := client.Queue("").PopResult(autopilotCtx(t)); err == nil {
		t.Fatal("a pop with no queue, namespace or task must fail")
	} else if !strings.Contains(err.Error(), "required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// --- 4. advised pacing ------------------------------------------------------

func TestEmptyPollDelay(t *testing.T) {
	if got := emptyPollDelay(nil); got != emptyPollBackoff {
		t.Fatalf("no decision: got %v, want %v", got, emptyPollBackoff)
	}
	if got := emptyPollDelay(&AutopilotDecision{Partitions: 4, Batch: 10}); got != emptyPollBackoff {
		t.Fatalf("decision without waitMs: got %v, want %v", got, emptyPollBackoff)
	}
	if got := emptyPollDelay(&AutopilotDecision{WaitMillis: 250}); got != 250*time.Millisecond {
		t.Fatalf("advised waitMs: got %v, want 250ms", got)
	}
	// A negative or zero advice is not advice.
	if got := emptyPollDelay(&AutopilotDecision{WaitMillis: -1}); got != emptyPollBackoff {
		t.Fatalf("negative waitMs: got %v, want %v", got, emptyPollBackoff)
	}
}

func TestConsumeHonorsAdvisedWaitMillis(t *testing.T) {
	// The loop already slept between empty non-waiting pops; the advice
	// replaces that constant. Timed with a wide margin on the honored side
	// only — a lower bound cannot be made flaky by a slow machine.
	const advisedMs = 500
	srv := newCaptureServer(t,
		okJSON(`{"messages":[],"autopilot":{"partitions":1,"batch":200,"waitMs":500}}`),
		okJSON(`{"messages":[{"transactionId":"11111111-1111-4111-8111-111111111111","partitionId":"22222222-2222-4222-8222-222222222222","queue":"ap-pace","partition":"Default","data":{"n":1},"createdAt":"2026-08-23T10:00:00.000Z"}]}`),
	)
	client := newWireClient(t, srv.URL)

	start := time.Now()
	err := client.Queue("ap-pace").Group("workers").Wait(false).Limit(1).
		Consume(autopilotCtx(t), func(ctx context.Context, msg *Message) error { return nil }).
		Execute(autopilotCtx(t))
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	if elapsed < 400*time.Millisecond {
		t.Fatalf("consume waited %v between the empty pop and the next one; the broker advised %dms", elapsed, advisedMs)
	}
}

func TestConsumeKeepsItsOwnPacingWithoutAdvice(t *testing.T) {
	// The control for the test above: no advice, so the historical 100ms.
	srv := newCaptureServer(t,
		okJSON(`{"messages":[]}`),
		okJSON(`{"messages":[{"transactionId":"11111111-1111-4111-8111-111111111111","partitionId":"22222222-2222-4222-8222-222222222222","queue":"ap-pace-off","partition":"Default","data":{"n":1},"createdAt":"2026-08-23T10:00:00.000Z"}]}`),
	)
	client := newWireClient(t, srv.URL)

	start := time.Now()
	err := client.Queue("ap-pace-off").Group("workers").Wait(false).Limit(1).
		Consume(autopilotCtx(t), func(ctx context.Context, msg *Message) error { return nil }).
		Execute(autopilotCtx(t))
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	if elapsed > 400*time.Millisecond {
		t.Fatalf("consume waited %v after an empty pop with no advice, want about %v", elapsed, emptyPollBackoff)
	}
}

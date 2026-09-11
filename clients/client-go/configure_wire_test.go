package queen

import (
	"net/http"
	"testing"
)

// The configure wire contract (PLAN_DASHBOARD_ACTIONS.md §2.2), asserted against
// a scripted plan server. No broker, no database.
//
// WHAT IS ACTUALLY AT STAKE HERE. The broker merges a configure body into the
// queue's stored configuration unless the body says `"mode":"replace"`, and this
// SDK omits every option its caller left at the zero value. So the presence or
// absence of ONE key decides whether
//
//	client.Queue("orders").Config(QueueConfig{LeaseTime: 60}).Create()
//
// edits the lease or resets the queue's dedup window, retention and DLQ policy
// along with it. Nothing downstream notices a wrong answer: the call returns
// 200 either way, and the damage is read off the queue days later.

func TestConfigureSendsNoModeByDefault(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`{"configured":true,"queue":"orders","options":{"leaseTime":60}}`))
	client := newWireClient(t, srv.URL)

	if _, err := client.Queue("orders").
		Config(QueueConfig{LeaseTime: 60}).
		Create().Execute(kvCtx(t)); err != nil {
		t.Fatalf("create: %v", err)
	}

	req := srv.only(t)
	if req.Method != http.MethodPost || req.Path != "/api/v1/configure" {
		t.Fatalf("configure went to %s %s, want POST /api/v1/configure", req.Method, req.Path)
	}
	// No `mode` at all, not `"mode":"merge"`: merge is the broker's own default,
	// so the default request must stay byte-identical to the one every released
	// version of this SDK sends — including against a broker old enough to
	// replace whatever it is told, where an explicit "merge" would be a
	// promise the wire cannot keep.
	assertJSONBody(t, req.Body, `{"queue":"orders","options":{"leaseTime":60}}`)
}

func TestConfigureReplaceSendsTheModeKey(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`{"configured":true,"queue":"orders","options":{"leaseTime":60}}`))
	client := newWireClient(t, srv.URL)

	if _, err := client.Queue("orders").
		Config(QueueConfig{LeaseTime: 60}).
		Create().Replace(true).Execute(kvCtx(t)); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Top level, beside `queue` — not inside `options`, where the broker would
	// overwrite it: `mode` is the request's own directive and the options bag is
	// the queue's configuration.
	assertJSONBody(t, srv.only(t).Body,
		`{"queue":"orders","mode":"replace","options":{"leaseTime":60}}`)
}

func TestConfigureReplaceFalseIsTheDefaultSpelledOut(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`{"configured":true,"queue":"orders"}`))
	client := newWireClient(t, srv.URL)

	if _, err := client.Queue("orders").
		Config(QueueConfig{LeaseTime: 60}).
		Create().Replace(false).Execute(kvCtx(t)); err != nil {
		t.Fatalf("create: %v", err)
	}
	assertJSONBody(t, srv.only(t).Body, `{"queue":"orders","options":{"leaseTime":60}}`)
}

// Namespace and task ride at the top level and are options like any other: a
// merge that does not carry them leaves the queue's own tagging alone, which is
// what makes `Queue(x).Create()` safe to call as a bootstrap idiom on a queue
// somebody else has already namespaced.
func TestConfigureSendsNamespaceAndTaskOnlyWhenSet(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`{"configured":true,"queue":"orders"}`))
	client := newWireClient(t, srv.URL)

	if _, err := client.Queue("orders").Create().Execute(kvCtx(t)); err != nil {
		t.Fatalf("create: %v", err)
	}
	assertJSONBody(t, srv.only(t).Body, `{"queue":"orders"}`)

	srv2 := newCaptureServer(t, okJSON(`{"configured":true,"queue":"orders"}`))
	client2 := newWireClient(t, srv2.URL)
	if _, err := client2.Queue("orders").
		Namespace("billing").Task("ingest").
		Create().Replace(true).Execute(kvCtx(t)); err != nil {
		t.Fatalf("create: %v", err)
	}
	assertJSONBody(t, srv2.only(t).Body,
		`{"queue":"orders","namespace":"billing","task":"ingest","mode":"replace"}`)
}

// An explicit false, an explicit zero and a null are the three values a plain
// QueueConfig cannot spell — its fields are ints and bools, so buildOptions
// cannot tell "false" from "unset" and omits both. Under merge that made every
// option one-way from this SDK: switchable on, never off. Option() is the way
// back, and it wins over the QueueConfig bag.
func TestConfigureOptionSendsFalseZeroAndNull(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`{"configured":true,"queue":"orders"}`))
	client := newWireClient(t, srv.URL)

	if _, err := client.Queue("orders").
		Config(QueueConfig{LeaseTime: 60, DeadLetterQueue: true}).
		Create().
		Option("deadLetterQueue", false).
		Option("dlqAfterMaxRetries", false).
		Option("retentionSeconds", 0).
		Option("retentionSinkHold", nil).
		Execute(kvCtx(t)); err != nil {
		t.Fatalf("create: %v", err)
	}

	assertJSONBody(t, srv.only(t).Body, `{"queue":"orders","options":{
		"leaseTime":60,
		"deadLetterQueue":false,
		"dlqAfterMaxRetries":false,
		"retentionSeconds":0,
		"retentionSinkHold":null}}`)
}

// Option alone, with no Config at all: a caller who only wants to turn one
// thing off must not have to invent a QueueConfig to carry it.
func TestConfigureOptionWithoutAConfigStillSendsTheBag(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`{"configured":true,"queue":"orders"}`))
	client := newWireClient(t, srv.URL)

	if _, err := client.Queue("orders").Create().
		Option("encryptionEnabled", false).
		Execute(kvCtx(t)); err != nil {
		t.Fatalf("create: %v", err)
	}
	assertJSONBody(t, srv.only(t).Body,
		`{"queue":"orders","options":{"encryptionEnabled":false}}`)
}

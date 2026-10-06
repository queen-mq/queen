package queen

import (
	"context"
	"errors"
	"net/url"
	"testing"
	"time"
)

// CommitOnDelivery, client side.
//
// Two different acks used to share one name. On a pop, the broker's autoAck=true
// query parameter moves the group's cursor past the messages as it hands them
// out: no lease, nothing to ack, at-most-once. On Consume, AutoAck is the ack the
// loop sends after the handler returns, and it never reaches the wire. The pop's
// option is now CommitOnDelivery, and these tests pin what each request carries:
// a pop says autoAck=true only when CommitOnDelivery asked for it, AutoAck has no
// effect on a pop, and a Consume that was given CommitOnDelivery refuses before
// it sends anything.

func codCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// popQuery runs one non-waiting pop built by build against a plan server and
// returns the query string that pop carried.
func popQuery(t *testing.T, build func(*Queen) *QueueBuilder, viaResult bool) url.Values {
	t.Helper()
	srv := newCaptureServer(t, okJSON(`{"messages":[]}`))
	client := newWireClient(t, srv.URL)

	qb := build(client).Wait(false)
	var err error
	if viaResult {
		_, err = qb.PopResult(codCtx(t))
	} else {
		_, err = qb.Pop(codCtx(t))
	}
	if err != nil {
		t.Fatalf("pop: %v", err)
	}
	return queryOf(t, srv.only(t))
}

// A pop sends autoAck=true when CommitOnDelivery asked for it, from Pop and
// PopResult alike. Otherwise it says nothing about autoAck: the broker's default
// is the leased pop, and autoAck=false on the wire would only be noise.
func TestPopSendsAutoAckOnlyForCommitOnDelivery(t *testing.T) {
	cases := []struct {
		name  string
		build func(*Queen) *QueueBuilder
		want  string // "" = the key must be absent
	}{
		{"CommitOnDelivery(true)", func(c *Queen) *QueueBuilder {
			return c.Queue("orders").Group("billing").CommitOnDelivery(true)
		}, "true"},
		{"CommitOnDelivery(false)", func(c *Queen) *QueueBuilder {
			return c.Queue("orders").Group("billing").CommitOnDelivery(false)
		}, ""},
		{"default", func(c *Queen) *QueueBuilder {
			return c.Queue("orders").Group("billing")
		}, ""},
	}
	for _, tc := range cases {
		for _, viaResult := range []bool{false, true} {
			q := popQuery(t, tc.build, viaResult)
			got, present := q["autoAck"]
			switch {
			case tc.want == "" && present:
				t.Errorf("%s, PopResult=%v: autoAck must not be sent, query: %s", tc.name, viaResult, q.Encode())
			case tc.want != "" && q.Get("autoAck") != tc.want:
				t.Errorf("%s, PopResult=%v: autoAck = %v, want %q, query: %s", tc.name, viaResult, got, tc.want, q.Encode())
			}
		}
	}
}

// AutoAck is the ack Consume sends after the handler. It used to travel on a
// pop as autoAck=x, so a pop after AutoAck(true) committed at delivery. It has
// no effect on a pop now: such a pop is leased.
func TestPopIgnoresTheConsumeAutoAck(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		for _, viaResult := range []bool{false, true} {
			q := popQuery(t, func(c *Queen) *QueueBuilder {
				return c.Queue("orders").Group("billing").AutoAck(enabled)
			}, viaResult)
			if _, present := q["autoAck"]; present {
				t.Fatalf("AutoAck(%v), PopResult=%v: a pop must not carry the consume ack, query: %s",
					enabled, viaResult, q.Encode())
			}
		}
	}
}

// Consume acks after the handler and must never commit at delivery, so a
// builder that asks for both is refused before a single request leaves.
func TestConsumeRefusesCommitOnDeliveryBeforeAnyRequest(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`{"messages":[]}`))
	client := newWireClient(t, srv.URL)
	ctx := codCtx(t)

	single := client.Queue("orders").Group("billing").CommitOnDelivery(true).Wait(false).Limit(1).
		Consume(ctx, func(context.Context, *Message) error { return nil }).Execute(ctx)
	batch := client.Queue("orders").Group("billing").CommitOnDelivery(true).Wait(false).Limit(1).
		ConsumeBatch(ctx, func(context.Context, []*Message) error { return nil }).Execute(ctx)

	for name, err := range map[string]error{"Consume": single, "ConsumeBatch": batch} {
		if !errors.Is(err, ErrCommitOnDeliveryConsume) {
			t.Errorf("%s returned %v, want ErrCommitOnDeliveryConsume", name, err)
		}
	}
	if reqs := srv.requests(); len(reqs) != 0 {
		t.Fatalf("the refusal must come before any request, the broker saw %d (first: %s?%s)",
			len(reqs), reqs[0].Path, reqs[0].RawQuery)
	}
}

func TestEphemeralPopCommitOnDeliverySendsAutoAck(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`{"queue":"inbox","messages":[]}`))
	client := newWireClient(t, srv.URL)

	if _, err := client.Ephemeral().Pop(ephCtx(t), "inbox", EphemeralPopOptions{CommitOnDelivery: true}); err != nil {
		t.Fatalf("pop: %v", err)
	}
	q := queryOf(t, srv.only(t))
	if got := q.Get("autoAck"); got != "true" {
		t.Fatalf("autoAck = %q, want \"true\" (raw: %q)", got, srv.only(t).RawQuery)
	}
}

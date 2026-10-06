package queen

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// threeMessagePop is one pop answer carrying three messages of one partition
// under one lease, n = 1, 2, 3 in order.
func threeMessagePop() cannedResponse {
	msgs := make([]string, 0, 3)
	for n := 1; n <= 3; n++ {
		msgs = append(msgs, fmt.Sprintf(
			`{"transactionId":"tx-%d","partitionId":"22222222-2222-4222-8222-222222222222","queue":"each-manual","partition":"Default","leaseId":"lease-1","data":{"n":%d},"createdAt":"2026-10-06T10:00:00.000Z"}`,
			n, n))
	}
	return okJSON(`{"leaseId":"lease-1","messages":[` + strings.Join(msgs, ",") + `]}`)
}

// With AutoAck(false) the handler settles its own messages, and a completed ack
// commits every earlier message of the batch. Under Each() a handler error
// must therefore stop the consumer at that message and come back out of
// Execute: handing the rest of the batch to the handler would let its acks
// commit the failed message, and the next success would overwrite the error.
// The client sends no ack or nack of its own (the documented contract for
// AutoAck(false)).
func TestEachManualAckHandlerErrorStopsTheConsumer(t *testing.T) {
	srv := newCaptureServer(t, threeMessagePop())
	client := newWireClient(t, srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	poison := errors.New("poison")
	var handled []int
	err := client.Queue("each-manual").
		Batch(3).
		Wait(false).
		Limit(3).
		Each().
		AutoAck(false).
		Consume(ctx, func(ctx context.Context, msg *Message) error {
			n := int(msg.Data["n"].(float64))
			handled = append(handled, n)
			if n == 1 {
				return poison
			}
			return nil
		}).
		Execute(ctx)

	if !errors.Is(err, poison) {
		t.Errorf("Execute returned %v, want the handler error %q", err, poison)
	}
	if got := fmt.Sprint(handled); got != "[1]" {
		t.Errorf("handler invoked for %s after n=1 failed, want [1]", got)
	}
	for _, r := range srv.requests() {
		if strings.HasPrefix(r.Path, "/api/v1/ack") {
			t.Errorf("client sent %s %s under AutoAck(false): %s", r.Method, r.Path, r.Body)
		}
	}
}

// The same batch where only the LAST message fails: the error has to come
// back out of Execute after the first two messages were handled.
func TestEachManualAckLastHandlerErrorIsReturned(t *testing.T) {
	srv := newCaptureServer(t, threeMessagePop())
	client := newWireClient(t, srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	poison := errors.New("poison")
	var handled []int
	err := client.Queue("each-manual").
		Batch(3).
		Wait(false).
		Limit(3).
		Each().
		AutoAck(false).
		Consume(ctx, func(ctx context.Context, msg *Message) error {
			n := int(msg.Data["n"].(float64))
			handled = append(handled, n)
			if n == 3 {
				return poison
			}
			return nil
		}).
		Execute(ctx)

	if !errors.Is(err, poison) {
		t.Errorf("Execute returned %v, want the handler error %q", err, poison)
	}
	if got := fmt.Sprint(handled); got != "[1 2 3]" {
		t.Errorf("handler invoked for %s, want [1 2 3]", got)
	}
}

// pops counts the pop requests a capture server received.
func pops(srv *captureServer) int {
	n := 0
	for _, r := range srv.requests() {
		if strings.Contains(r.Path, "/pop") {
			n++
		}
	}
	return n
}

// A pop the broker refuses with a 4xx other than 403 and 429 (a bad request,
// an unauthorized token) will be refused the same way next time: the consumer
// stops and returns it, as it does for a 403, instead of popping again at once.
func TestConsumeStopsOnAClientError(t *testing.T) {
	srv := newCaptureServer(t)
	srv.fallback = cannedResponse{status: 401, body: `{"error":"unauthorized"}`}
	client := newWireClient(t, srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err := client.Queue("refused").Wait(false).
		Consume(ctx, func(ctx context.Context, msg *Message) error { return nil }).
		Execute(ctx)

	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.StatusCode != 401 {
		t.Fatalf("Execute returned %v, want the 401", err)
	}
	if n := pops(srv); n != 1 {
		t.Errorf("sent %d pops, want 1: a refused pop must not be retried at once", n)
	}
}

// A 5xx may pass: the consumer pops again, but after a pause, as after a
// network error, never in a tight loop.
func TestConsumeWaitsAfterAServerError(t *testing.T) {
	srv := newCaptureServer(t, cannedResponse{status: 500, body: `{"error":"boom"}`}, threeMessagePop())
	client := newWireClient(t, srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	started := time.Now()
	err := client.Queue("each-manual").Batch(3).Wait(false).Limit(3).
		Consume(ctx, func(ctx context.Context, msg *Message) error { return nil }).
		Execute(ctx)

	if err != nil {
		t.Fatalf("Execute returned %v, want nil after the retry succeeded", err)
	}
	if n := pops(srv); n != 2 {
		t.Errorf("sent %d pops, want 2", n)
	}
	if elapsed := time.Since(started); elapsed < 900*time.Millisecond {
		t.Errorf("popped again after %v, want a pause of about 1 s", elapsed)
	}
}

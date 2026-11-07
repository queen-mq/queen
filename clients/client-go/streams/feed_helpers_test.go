package streams_test

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	queen "github.com/smartpricing/queen/clients/client-go/v2"
	"github.com/smartpricing/queen/clients/client-go/v2/internal/feedfake"
	"github.com/smartpricing/queen/clients/client-go/v2/internal/jsv"
	"github.com/smartpricing/queen/clients/client-go/v2/streams"
	"github.com/smartpricing/queen/clients/client-go/v2/streams/feedrt"
)

const NS = feedrt.PositionNamespace

var T0 = feedfake.T0

// harness is one test: its fake broker, its clients, and every runner it
// started (stopped at the end, passed or failed).
type harness struct {
	t       *testing.T
	b       *feedfake.Broker
	mu      sync.Mutex
	handles []streams.FeedHandle
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	h := &harness{t: t, b: feedfake.New(0)}
	t.Cleanup(func() {
		h.mu.Lock()
		hs := h.handles
		h.mu.Unlock()
		for i := len(hs) - 1; i >= 0; i-- {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			_ = hs[i].Stop(ctx)
			cancel()
		}
		h.b.Close()
	})
	return h
}

// client is one process's client on the fake broker: no retries, as the JS
// fake's HTTP client.
func (h *harness) client(name string) *queen.Queen {
	q, err := queen.New(queen.ClientConfig{URL: h.b.URL(name), RetryAttempts: -1, TimeoutMillis: 60000})
	if err != nil {
		h.t.Fatal(err)
	}
	return q
}

func (h *harness) at(ms float64) { h.b.At(ms) }

func (h *harness) push(queue, partition string, data interface{}) int64 {
	return h.b.Push(feedfake.Msg{Queue: queue, Partition: partition, Data: data})
}

// run starts a stream and keeps its handle for the cleanup.
func (h *harness) run(s *streams.Stream, o streams.FeedRunOptions) streams.FeedHandle {
	h.t.Helper()
	r, err := s.RunFeed(context.Background(), o)
	if err != nil {
		h.t.Fatalf("RunFeed: %v", err)
	}
	h.mu.Lock()
	h.handles = append(h.handles, r)
	h.mu.Unlock()
	return r
}

func (h *harness) runErr(s *streams.Stream, o streams.FeedRunOptions) error {
	r, err := s.RunFeed(context.Background(), o)
	if err == nil {
		h.mu.Lock()
		h.handles = append(h.handles, r)
		h.mu.Unlock()
	}
	return err
}

func stop(t *testing.T, r streams.FeedHandle) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := r.Stop(ctx); err != nil {
		t.Fatalf("stop: %v", err)
	}
}

// until waits for cond (a JS until()).
func until(t *testing.T, cond func() bool, ms int, what string) {
	t.Helper()
	if ms == 0 {
		ms = 3000
	}
	end := time.Now().Add(time.Duration(ms) * time.Millisecond)
	for !cond() {
		if time.Now().After(end) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func wait(ms int) { time.Sleep(time.Duration(ms) * time.Millisecond) }

// fast is the JS tests' fast(): waitMillis 20, commitEveryMillis 0, retryDelayMillis 1.
func fast(o streams.FeedRunOptions) streams.FeedRunOptions {
	if o.WaitMillis == nil {
		o.WaitMillis = streams.Int(20)
	}
	if o.CommitEveryMillis == nil {
		o.CommitEveryMillis = streams.Int(0)
	}
	if o.RetryDelayMillis == nil {
		o.RetryDelayMillis = streams.Int(1)
	}
	return o
}

func quiet(o streams.FeedRunOptions) streams.FeedRunOptions {
	o.OnError = func(error, map[string]interface{}) {}
	return o
}

// now is the fake's wall clock, for the runner's Now.
func (h *harness) now() func() time.Time {
	return func() time.Time { return time.UnixMilli(int64(h.b.Now())) }
}

func (h *harness) payloads(queue string) []interface{} {
	var out []interface{}
	for _, e := range h.b.Messages(queue) {
		out = append(out, e.Payload)
	}
	return out
}

func (h *harness) count(queue string) int { return len(h.b.Messages(queue)) }

func (h *harness) record(key string) map[string]interface{} {
	v, _, ok := h.b.KVGet(NS, key)
	if !ok {
		return nil
	}
	m, _ := v.(map[string]interface{})
	return m
}

func (h *harness) recordPosition(key string) float64 {
	f, _ := jsv.Num(jsv.Get(h.record(key), "position"))
	return f
}

func m(kv ...interface{}) map[string]interface{} {
	out := map[string]interface{}{}
	for i := 0; i+1 < len(kv); i += 2 {
		out[kv[i].(string)] = kv[i+1]
	}
	return out
}

// eq compares as JSON values (numbers by value, maps in any order).
func eq(t *testing.T, got, want interface{}, what string) {
	t.Helper()
	g := jsv.Normalize(jsv.JSONClone(got))
	w := jsv.Normalize(jsv.JSONClone(want))
	if !reflect.DeepEqual(g, w) {
		t.Fatalf("%s:\n got  %s\n want %s", what, jsv.MustMarshal(g), jsv.MustMarshal(w))
	}
}

func get(v interface{}, path ...string) interface{} {
	for _, p := range path {
		v = jsv.Get(v, p)
	}
	return v
}

func num(v interface{}) float64 {
	f, _ := jsv.Num(v)
	return f
}

func data(msg interface{}) map[string]interface{} {
	d, _ := jsv.Get(msg, "data").(map[string]interface{})
	return d
}

func iso(ms float64) string { return jsv.MustISO(ms) }

var _ = fmt.Sprint

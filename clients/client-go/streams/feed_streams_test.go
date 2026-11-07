package streams_test

// The behaviours of clients/client-js/test-v2/feed-unit/feedStreams.test.js:
// poison messages, one runner per queryId, routing, windows on the feed
// clock, patterns and joins, commits that go wrong.

import (
	"context"
	"errors"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/smartpricing/queen/clients/client-go/v2/internal/feedfake"
	"github.com/smartpricing/queen/clients/client-go/v2/internal/jsv"
	"github.com/smartpricing/queen/clients/client-go/v2/streams"
	"github.com/smartpricing/queen/clients/client-go/v2/streams/feedrt"
)

type typeError struct{ msg string }

func (e typeError) Error() string     { return e.msg }
func (e typeError) ErrorName() string { return "TypeError" }

func orders(h *harness, n int) {
	for i := 1; i <= n; i++ {
		h.push("orders", "c"+itoa(i), m("n", float64(i)))
	}
}

func itoa(i int) string { return jsv.NumberString(float64(i)) }

func ns(h *harness, queue string) []float64 {
	var out []float64
	for _, p := range h.payloads(queue) {
		out = append(out, num(get(p, "n")))
	}
	return out
}

func mapN(fail func(n float64) error) streams.MapFn {
	return func(msg interface{}, _ streams.EmitCtx) (interface{}, error) {
		n := num(data(msg)["n"])
		if err := fail(n); err != nil {
			return nil, err
		}
		return m("n", n), nil
	}
}

func TestFeedPoisonStopsAtAMessageThatKeepsFailing(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	orders(h, 5)
	var calls atomic.Int32
	var mu sync.Mutex
	type hook struct {
		err error
		ctx map[string]interface{}
	}
	var errs []hook
	o := fast(streams.FeedRunOptions{QueryID: "stop", MaxRecords: 2})
	o.OnError = func(err error, ctx map[string]interface{}) {
		mu.Lock()
		errs = append(errs, hook{err, ctx})
		mu.Unlock()
	}
	r := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest"}).
		Map(mapN(func(n float64) error {
			if n == 3 {
				calls.Add(1)
				return errors.New("bad order")
			}
			return nil
		})).To(q.Queue("out")), o)
	select {
	case <-r.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("never stopped")
	}
	eq(t, h.payloads("out"), []interface{}{m("n", 1), m("n", 2)}, "what came before")
	if calls.Load() != 4 {
		t.Fatal("the first try and 3 retries", calls.Load())
	}
	third := h.b.Messages("orders")[2]
	if h.recordPosition("stop") != float64(third.Position) {
		t.Fatal("the stored position is the message", h.record("stop"))
	}
	mt := r.Metrics()
	if mt.Role != "stopped" || mt.Stopped["reason"] != "poison" || num(mt.Stopped["position"]) != float64(third.Position) {
		t.Fatal(mt.Role, mt.Stopped)
	}
	mu.Lock()
	defer mu.Unlock()
	var stopErr *hook
	for i := range errs {
		if errs[i].ctx["action"] == "stopped" {
			stopErr = &errs[i]
		}
	}
	if stopErr == nil {
		t.Fatal("onError hears it")
	}
	if !regexp.MustCompile(`position \d+ \(orders/c3, offset 1\) failed 4 times: bad order`).MatchString(stopErr.err.Error()) || feedrt.ErrorCode(stopErr.err) != "feed_poison" {
		t.Fatal(stopErr.err, feedrt.ErrorCode(stopErr.err))
	}
}

func TestFeedPoisonSkipLeavesTheMessageOut(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	orders(h, 5)
	o := quiet(fast(streams.FeedRunOptions{QueryID: "skip", OnPoison: "skip"}))
	r := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest"}).
		Map(mapN(func(n float64) error {
			if n == 3 {
				return errors.New("bad order")
			}
			return nil
		})).To(q.Queue("out")), o)
	until(t, func() bool { return h.count("out") >= 4 }, 0, "four outputs")
	stop(t, r)
	eq(t, ns(h, "out"), []float64{1, 2, 4, 5}, "skipped")
	if r.Metrics().Skipped != 1 || r.Metrics().Poisoned != 1 {
		t.Fatal(r.Metrics())
	}
}

func TestFeedPoisonADeadLetterQueueGetsTheMessageAndTheError(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	orders(h, 4)
	o := quiet(fast(streams.FeedRunOptions{QueryID: "dlq", OnPoison: "out-dlq", Retries: streams.Int(1)}))
	r := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest"}).
		Map(mapN(func(n float64) error {
			if n == 2 {
				return typeError{"no price"}
			}
			return nil
		})).To(q.Queue("out")), o)
	until(t, func() bool { return h.count("out") >= 3 }, 0, "three outputs")
	stop(t, r)
	dead := h.b.Messages("out-dlq")
	if len(dead) == 0 {
		t.Fatal("dead-lettered")
	}
	d := dead[0]
	if d.Partition != "c2" {
		t.Fatal(d.Partition)
	}
	eq(t, get(d.Payload, "entry", "data"), m("n", 2), "entry data")
	eq(t, get(d.Payload, "entry", "queue"), "orders", "entry queue")
	eq(t, get(d.Payload, "error"), m("name", "TypeError", "message", "no price"), "error")
	eq(t, get(d.Payload, "attempts"), 2, "attempts")
	eq(t, get(d.Payload, "queryId"), "dlq", "queryId")
	if !strings.HasPrefix(d.TransactionID, feedrt.OutputPrefix("dlq")) || d.Position != h.b.Messages("out")[0].Position {
		t.Fatal(d.TransactionID, d.Position)
	}
	if r.Metrics().DeadLettered != 1 {
		t.Fatal(r.Metrics().DeadLettered)
	}
}

func TestFeedPoisonAFunctionThatFailsThenPassesIsNotPoison(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	orders(h, 3)
	var tries atomic.Int32
	r := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest"}).
		Map(mapN(func(n float64) error {
			if n == 2 && tries.Add(1) < 3 {
				return errors.New("flaky")
			}
			return nil
		})).To(q.Queue("out")), fast(streams.FeedRunOptions{QueryID: "flaky"}))
	until(t, func() bool { return h.count("out") >= 3 }, 0, "three outputs")
	stop(t, r)
	eq(t, ns(h, "out"), []float64{1, 2, 3}, "all")
	if r.Metrics().Retries != 2 || r.Metrics().Poisoned != 0 {
		t.Fatal(r.Metrics().Retries, r.Metrics().Poisoned)
	}
}

func TestFeedPoisonAMessageTriedAgainCountsItsResultsOnceInEmitted(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	orders(h, 3)
	var mu sync.Mutex
	var calls []float64
	failed := false
	r := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest"}).
		FlatMap(func(msg interface{}, _ streams.EmitCtx) ([]interface{}, error) {
			n := num(get(msg, "data", "n"))
			return []interface{}{n, n * 10}, nil
		}).
		Foreach(func(v interface{}, _ streams.EmitCtx) error {
			mu.Lock()
			defer mu.Unlock()
			n := num(v)
			calls = append(calls, n)
			if n == 20 && !failed {
				failed = true
				return errors.New("once")
			}
			if n == 30 {
				return errors.New("always")
			}
			return nil
		}), quiet(fast(streams.FeedRunOptions{QueryID: "emitted-once", OnPoison: "skip"})))
	until(t, func() bool { return r.Metrics().Skipped >= 1 }, 0, "the skip")
	stop(t, r)
	mu.Lock()
	defer mu.Unlock()
	eq(t, calls[:5], []float64{1, 10, 2, 20, 2}, "message 2 runs again from its first result")
	if r.Metrics().Emitted != 4 {
		t.Fatal("the four results of messages 1 and 2, once each", r.Metrics().Emitted)
	}
	if r.Metrics().Skipped != 1 {
		t.Fatal(r.Metrics().Skipped)
	}
}

func TestFeedPoisonARetriedMessageStartsAgainFromTheStateBeforeIt(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.at(1000)
	for i := 1; i <= 3; i++ {
		h.push("orders", "c1", m("n", float64(i)))
	}
	var failed atomic.Bool
	r := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest"}).
		WindowTumbling(60).
		Reduce(func(acc, v interface{}) (interface{}, error) {
			a := acc.(map[string]interface{})
			a["sum"] = num(a["sum"]) + num(get(v, "n"))
			a["count"] = num(a["count"]) + 1
			if num(get(v, "n")) == 2 && !failed.Load() {
				failed.Store(true)
				return nil, errors.New("once")
			}
			return a, nil
		}, m("sum", 0.0, "count", 0.0)).
		To(q.Queue("sums")), fast(streams.FeedRunOptions{QueryID: "undo", Now: h.now()}))
	until(t, func() bool { return r.Metrics().EntriesRead >= 3 }, 0, "three read")
	h.at(61000)
	h.push("other", "x", m())
	until(t, func() bool { return h.count("sums") >= 1 }, 0, "the window")
	stop(t, r)
	eq(t, h.payloads("sums"), []interface{}{m("sum", 6, "count", 3)}, "the failed try left nothing behind")
}

func TestFeedPoisonAWindowResultWhoseSinkKeepsFailingIsSkipped(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.at(1000)
	h.push("orders", "bad", m("n", 1.0))
	h.push("orders", "good", m("n", 2.0))
	var mu sync.Mutex
	var seen []interface{}
	r := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"orders"}}).
		WindowTumbling(60).
		AggregateFields(streams.AggregateField{Name: "count", Fn: func(interface{}) (interface{}, error) { return 1.0, nil }}).
		Foreach(func(value interface{}, ctx streams.EmitCtx) error {
			if ctx["key"] == "bad" {
				return errors.New("downstream refused")
			}
			mu.Lock()
			seen = append(seen, m("key", ctx["key"], "count", get(value, "count")))
			mu.Unlock()
			return nil
		}), quiet(fast(streams.FeedRunOptions{QueryID: "emit-skip", Now: h.now(), OnPoison: "skip"})))
	until(t, func() bool { return r.Metrics().EntriesRead >= 2 }, 0, "two read")
	h.at(61000)
	h.push("orders", "good", m("n", 3.0))
	until(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(seen) >= 1 && r.Metrics().Skipped >= 1 }, 0, "the skip")
	stop(t, r)
	mu.Lock()
	defer mu.Unlock()
	eq(t, seen, []interface{}{m("key", "good", "count", 1)}, "seen")
}

func TestFeedOneRunnerASecondProcessWaits(t *testing.T) {
	h := newHarness(t)
	qa, qb := h.client("a"), h.client("b")
	for i := 1; i <= 40; i++ {
		h.push("orders", "c"+itoa(i%5), m("n", float64(i)))
	}
	var mu sync.Mutex
	var calls []string
	effect := func(q streams.FeedClient, who string) *streams.Stream {
		return streams.FromFeed(q, streams.FeedSource{From: "earliest"}).Foreach(func(v interface{}, _ streams.EmitCtx) error {
			mu.Lock()
			calls = append(calls, who)
			mu.Unlock()
			return nil
		})
	}
	ra := h.run(effect(qa, "a"), fast(streams.FeedRunOptions{QueryID: "fx", MaxRecords: 7}))
	rb := h.run(effect(qb, "b"), fast(streams.FeedRunOptions{QueryID: "fx", MaxRecords: 7}))
	until(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(calls) >= 40 }, 0, "40 calls")
	wait(100)
	if ra.Metrics().Role != "active" || rb.Metrics().Role != "standby" {
		t.Fatal(ra.Metrics().Role, rb.Metrics().Role)
	}
	mu.Lock()
	n := len(calls)
	who := map[string]bool{}
	for _, c := range calls {
		who[c] = true
	}
	mu.Unlock()
	if n != 40 || len(who) != 1 || !who["a"] {
		t.Fatal(n, who)
	}
	stop(t, ra)
	stop(t, rb)
}

func TestFeedOneRunnerTheStandbyTakesOverFromTheStoredPosition(t *testing.T) {
	h := newHarness(t)
	qa, qb := h.client("a"), h.client("b")
	for i := 1; i <= 5; i++ {
		h.push("orders", "c1", m("n", float64(i)))
	}
	cp := func(q streams.FeedClient, sink streams.Sink) *streams.Stream {
		return streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"orders"}}).To(sink)
	}
	ra := h.run(cp(qa, qa.Queue("copy")), fast(streams.FeedRunOptions{QueryID: "copy", LockTTL: 2 * time.Second}))
	rb := h.run(cp(qb, qb.Queue("copy")), fast(streams.FeedRunOptions{QueryID: "copy", LockTTL: 2 * time.Second}))
	until(t, func() bool { return h.count("copy") >= 5 }, 0, "five copies")
	stop(t, ra)
	for i := 6; i <= 8; i++ {
		h.push("orders", "c1", m("n", float64(i)))
	}
	until(t, func() bool { return h.count("copy") >= 8 }, 6000, "the standby to copy 6..8")
	stop(t, rb)
	eq(t, ns(h, "copy"), []float64{1, 2, 3, 4, 5, 6, 7, 8}, "each once, in order")
	if rb.Metrics().Takeovers != 1 {
		t.Fatal(rb.Metrics().Takeovers)
	}
}

func TestFeedOneRunnerAProcessThatLosesItsLockCannotCommit(t *testing.T) {
	h := newHarness(t)
	qa, qb := h.client("a"), h.client("b")
	h.push("orders", "c1", m("n", 1.0))
	cp := func(q streams.FeedClient, sink streams.Sink) *streams.Stream {
		return streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"orders"}}).To(sink)
	}
	ra := h.run(cp(qa, qa.Queue("copy")), quiet(fast(streams.FeedRunOptions{QueryID: "cut", LockTTL: time.Second})))
	rb := h.run(cp(qb, qb.Queue("copy")), quiet(fast(streams.FeedRunOptions{QueryID: "cut", LockTTL: time.Second})))
	until(t, func() bool { return h.count("copy") >= 1 }, 0, "one copy")
	// a's process is cut off: its lock expires.
	h.b.Cut("a", true)
	h.push("orders", "c1", m("n", 2.0))
	until(t, func() bool { return h.count("copy") >= 2 }, 8000, "b to take over")
	h.b.Cut("a", false)
	h.push("orders", "c1", m("n", 3.0))
	until(t, func() bool { return h.count("copy") >= 3 }, 4000, "the third")
	wait(200)
	if ra.Metrics().Role != "standby" || rb.Metrics().Role != "active" {
		t.Fatal(ra.Metrics().Role, rb.Metrics().Role)
	}
	eq(t, ns(h, "copy"), []float64{1, 2, 3}, "copies")
	stop(t, ra)
	stop(t, rb)
}

func TestFeedOneRunnerExclusiveFalseRunsBoth(t *testing.T) {
	h := newHarness(t)
	qa, qb := h.client("a"), h.client("b")
	for i := 1; i <= 30; i++ {
		h.push("orders", "c"+itoa(i%3), m("n", float64(i)))
	}
	cp := func(q streams.FeedClient, sink streams.Sink) *streams.Stream {
		return streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"orders"}}).To(sink)
	}
	ra := h.run(cp(qa, qa.Queue("copy")), fast(streams.FeedRunOptions{QueryID: "both", Exclusive: streams.Bool(false), MaxRecords: 4}))
	rb := h.run(cp(qb, qb.Queue("copy")), fast(streams.FeedRunOptions{QueryID: "both", Exclusive: streams.Bool(false), MaxRecords: 4}))
	until(t, func() bool { return h.count("copy") >= 30 }, 0, "30 copies")
	wait(100)
	stop(t, ra)
	stop(t, rb)
	got := ns(h, "copy")
	uniq := map[float64]bool{}
	for _, n := range got {
		uniq[n] = true
	}
	if len(got) != 30 || len(uniq) != 30 || ra.Metrics().Role != "stopped" {
		t.Fatal(len(got), len(uniq), ra.Metrics().Role)
	}
}

func TestFeedRoutingAFunctionPicksTheQueue(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.push("orders", "o1", m("id", 1.0, "country", "IT"))
	h.push("orders", "o2", m("id", 2.0, "country", "FR"))
	h.push("orders", "o3", m("id", 3.0, "country", "IT"))
	h.push("orders", "o4", m("id", 4.0))
	r := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"orders*"}}).
		ToFunc(func(order, _ interface{}) (interface{}, error) {
			if c, ok := get(order, "country").(string); ok {
				return "orders-" + c, nil
			}
			return nil, nil
		}), fast(streams.FeedRunOptions{QueryID: "router"}))
	until(t, func() bool { return h.count("orders-IT") >= 2 && h.count("orders-FR") >= 1 }, 0, "routed")
	wait(100)
	stop(t, r)
	ids := func(qn string) []float64 {
		var out []float64
		for _, p := range h.payloads(qn) {
			out = append(out, num(get(p, "id")))
		}
		return out
	}
	eq(t, ids("orders-IT"), []float64{1, 3}, "IT")
	eq(t, ids("orders-FR"), []float64{2}, "FR")
	var commit *feedfake.Txn
	for _, x := range h.b.Txns() {
		if len(x.Items) > 0 {
			x := x
			commit = &x
			break
		}
	}
	if len(commit.Items) != 3 {
		t.Fatal("one transaction for both queues", len(commit.Items))
	}
	// orders* matches its own outputs, and it never reads them back.
	if r.Metrics().EntriesRead != 4 || h.count("orders-IT") != 2 {
		t.Fatal(r.Metrics().EntriesRead)
	}
}

func TestFeedRoutingAListAndTargets(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.push("orders", "o1", m("id", 1.0, "total", 500.0))
	h.push("orders", "o2", m("id", 2.0, "total", 5.0))
	all := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"orders"}}).
		ToAll(q.Queue("audit"), q.Queue("bi")), fast(streams.FeedRunOptions{QueryID: "fanout"}))
	picked := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"orders"}}).
		ToFunc(func(o, _ interface{}) (interface{}, error) {
			size := "small"
			var review interface{}
			if num(get(o, "total")) > 100 {
				size = "big"
				review = q.Queue("review")
			}
			return []interface{}{streams.Target{Queue: "by-size", Partition: size}, review}, nil
		}), fast(streams.FeedRunOptions{QueryID: "picker"}))
	until(t, func() bool { return h.count("bi") >= 2 && h.count("by-size") >= 2 && h.count("review") >= 1 }, 0, "routed")
	stop(t, all)
	stop(t, picked)
	if len(h.payloads("audit")) != 2 || len(h.payloads("bi")) != 2 {
		t.Fatal("each record to both")
	}
	bs := h.b.Messages("by-size")
	if num(get(bs[0].Payload, "id")) != 1 || bs[0].Partition != "big" || num(get(bs[1].Payload, "id")) != 2 || bs[1].Partition != "small" {
		t.Fatal(bs)
	}
	rv := h.b.Messages("review")
	if len(rv) != 1 || rv[0].Partition != "o1" {
		t.Fatal("the source partition by default", rv)
	}
}

func TestFeedRoutingAQueueSourceRefusesSeveralQueuesAndFeedSteps(t *testing.T) {
	src := fakeSource{name: "src"}
	_, err := streams.From(src).ToFunc(func(interface{}, interface{}) (interface{}, error) { return "x", nil }).Run(nil, streams.RunOptions{QueryID: "r1"})
	if err == nil || !strings.Contains(err.Error(), "Stream.fromFeed only") {
		t.Fatal(err)
	}
	_, err = streams.From(src).ToAll(sinkName("a"), sinkName("b")).Run(nil, streams.RunOptions{QueryID: "r2"})
	if err == nil || !strings.Contains(err.Error(), "Stream.fromFeed only") {
		t.Fatal(err)
	}
	_, err = streams.From(src).Pattern(streams.PatternOptions{Start: yes, Then: yes, Within: "1m"}).To(sinkName("x")).Run(nil, streams.RunOptions{QueryID: "r3"})
	if err == nil || !strings.Contains(err.Error(), "Stream.fromFeed only") {
		t.Fatal(err)
	}
}

func yes(interface{}) (bool, error) { return true, nil }

type sinkName string

func (s sinkName) Name() string { return string(s) }

func one(interface{}) (interface{}, error) { return 1.0, nil }

func countAgg() streams.AggregateField { return streams.AggregateField{Name: "count", Fn: one} }

func field(name, path string) streams.AggregateField {
	return streams.AggregateField{Name: name, Fn: func(v interface{}) (interface{}, error) { return get(v, path), nil }}
}

func TestFeedWindowsATumblingWindowCountsPerEntityAcrossQueues(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.at(1000)
	h.push("orders", "customer-1", m("total", 10.0))
	h.at(2000)
	h.push("payments", "customer-1", m("total", 10.0))
	h.at(3000)
	h.push("orders", "customer-2", m("total", 7.0))
	r := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"orders", "payments"}}).
		WindowTumbling(60).
		AggregateFields(countAgg(), field("sum", "total"), field("avg", "total")).
		Map(func(acc interface{}, ctx streams.EmitCtx) (interface{}, error) {
			out := jsv.Clone(acc).(map[string]interface{})
			out["key"], out["window"] = ctx["key"], ctx["windowKey"]
			return out, nil
		}).To(q.Queue("per-minute")), fast(streams.FeedRunOptions{QueryID: "per-minute", Now: h.now()}))
	until(t, func() bool { return r.Metrics().EntriesRead >= 3 }, 0, "three read")
	if h.count("per-minute") != 0 {
		t.Fatal("open until the minute is over")
	}
	h.at(61000)
	h.push("orders", "customer-3", m("total", 1.0))
	until(t, func() bool { return h.count("per-minute") >= 2 }, 0, "two results")
	stop(t, r)
	out := h.b.Messages("per-minute")
	eq(t, h.payloads("per-minute"), []interface{}{
		m("count", 2, "sum", 20, "avg", 10, "key", "customer-1", "window", "2026-11-06T09:00:00.000Z"),
		m("count", 1, "sum", 7, "avg", 7, "key", "customer-2", "window", "2026-11-06T09:00:00.000Z"),
	}, "no __avg_ running sums in the result")
	if out[0].Partition != "customer-1" || out[1].Partition != "customer-2" || !strings.HasPrefix(out[0].TransactionID, feedrt.OutputPrefix("per-minute")) {
		t.Fatal(out)
	}
	if len(h.b.KVKeys(NS, "per-minute#snap#")) < 1 {
		t.Fatal("a stop writes a snapshot")
	}
	for _, x := range h.b.Txns() {
		if len(x.KV) > 64 {
			t.Fatal("within 64 KV ops")
		}
	}
}

func TestFeedWindowsOnAQuietFeedClosesOnTime(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.at(5000)
	h.push("orders", "c1", m())
	h.at(6000)
	h.push("orders", "c1", m())
	r := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest"}).WindowTumbling(60).AggregateFields(countAgg()).
		To(q.Queue("counts")), fast(streams.FeedRunOptions{QueryID: "quiet", Now: h.now()}))
	until(t, func() bool { return r.Metrics().EntriesRead >= 2 }, 0, "two read")
	h.at(59500)
	wait(100)
	if h.count("counts") != 0 {
		t.Fatal("not before its end")
	}
	h.at(60000)
	until(t, func() bool { return h.count("counts") >= 1 }, 3000, "the window to close with no message after it")
	stop(t, r)
	eq(t, h.payloads("counts"), []interface{}{m("count", 2)}, "counts")
	if r.Metrics().Ticks < 1 {
		t.Fatal("ticks")
	}
}

func TestFeedWindowsARestartCarriesTheOpenWindowsOn(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.at(1000)
	for i := 0; i < 3; i++ {
		h.push("orders", "c1", m("n", float64(i)))
	}
	chain := func(q streams.FeedClient, sink streams.Sink) *streams.Stream {
		return streams.FromFeed(q, streams.FeedSource{From: "earliest"}).WindowTumbling(60).AggregateFields(countAgg()).To(sink)
	}
	first := h.run(chain(q, q.Queue("counts")), fast(streams.FeedRunOptions{QueryID: "restart", Now: h.now()}))
	until(t, func() bool { return first.Metrics().EntriesRead >= 3 }, 0, "three read")
	stop(t, first)
	h.at(2000)
	h.push("orders", "c1", m("n", 3.0))
	q2 := h.client("second")
	second := h.run(chain(q2, q2.Queue("counts")), fast(streams.FeedRunOptions{QueryID: "restart", Now: h.now()}))
	until(t, func() bool { return second.Metrics().EntriesRead >= 1 }, 0, "one read")
	h.at(61000)
	h.push("other", "x", m())
	until(t, func() bool { return h.count("counts") >= 1 }, 0, "the window")
	stop(t, second)
	eq(t, h.payloads("counts"), []interface{}{m("count", 4)}, "counts")
}

func TestFeedWindowsSlidingWindowsCountEachMessageInEveryWindow(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.at(10000)
	h.push("q", "k", m())
	h.at(40000)
	h.push("q", "k", m())
	r := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"q"}}).
		WindowSliding(60, 30).AggregateFields(countAgg()).
		Map(func(acc interface{}, ctx streams.EmitCtx) (interface{}, error) {
			return m("start", ctx["windowKey"].(string)[11:19], "count", get(acc, "count")), nil
		}).To(q.Queue("slides")), fast(streams.FeedRunOptions{QueryID: "slides", Now: h.now()}))
	until(t, func() bool { return r.Metrics().EntriesRead >= 2 }, 0, "two read")
	h.at(200000)
	h.push("other", "x", m())
	until(t, func() bool { return h.count("slides") >= 3 }, 0, "three windows")
	stop(t, r)
	eq(t, h.payloads("slides"), []interface{}{m("start", "08:59:30", "count", 1), m("start", "09:00:00", "count", 2), m("start", "09:00:30", "count", 1)}, "slides")
}

func TestFeedWindowsASessionClosesPastTheGapAndOnTheClock(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.at(0)
	h.push("clicks", "u1", m())
	h.at(5000)
	h.push("clicks", "u1", m())
	h.at(30000)
	h.push("clicks", "u1", m())
	r := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"clicks"}}).
		WindowSession(10).AggregateFields(countAgg()).
		Map(func(acc interface{}, ctx streams.EmitCtx) (interface{}, error) {
			return m("count", get(acc, "count"), "from", num(ctx["windowStart"])-T0, "to", num(ctx["windowEnd"])-T0), nil
		}).To(q.Queue("sessions")), fast(streams.FeedRunOptions{QueryID: "sessions", Now: h.now()}))
	until(t, func() bool { return h.count("sessions") >= 1 }, 0, "the first session")
	eq(t, h.payloads("sessions"), []interface{}{m("count", 2, "from", 0, "to", 15000)}, "first")
	h.at(41000)
	until(t, func() bool { return h.count("sessions") >= 2 }, 3000, "the second session to close on the clock")
	stop(t, r)
	eq(t, h.payloads("sessions")[1], m("count", 1, "from", 30000, "to", 40000), "second")
}

func TestFeedWindowsCronWindowsAndKeyBy(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.at(59000)
	h.push("sales", "s1", m("country", "IT", "amount", 5.0))
	h.at(61000)
	h.push("sales", "s2", m("country", "IT", "amount", 7.0))
	h.at(62000)
	h.push("sales", "s3", m("country", "FR", "amount", 1.0))
	r := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"sales"}}).
		KeyBy(func(msg interface{}) (string, error) { return data(msg)["country"].(string), nil }).
		WindowCron("minute").AggregateFields(field("sum", "amount")).
		Map(func(acc interface{}, ctx streams.EmitCtx) (interface{}, error) {
			return m("country", ctx["key"], "minute", ctx["windowKey"].(string)[11:16], "sum", get(acc, "sum")), nil
		}).To(q.Queue("per-country")), fast(streams.FeedRunOptions{QueryID: "cron", Now: h.now()}))
	until(t, func() bool { return h.count("per-country") >= 1 }, 0, "the first minute")
	h.at(125000)
	h.push("other", "x", m())
	until(t, func() bool { return h.count("per-country") >= 3 }, 0, "three results")
	stop(t, r)
	eq(t, h.payloads("per-country"), []interface{}{m("country", "IT", "minute", "09:00", "sum", 5),
		m("country", "FR", "minute", "09:01", "sum", 1), m("country", "IT", "minute", "09:01", "sum", 7)}, "windows that end together close in key order")
}

func TestFeedWindowsEventTimeClosesOnTheWatermarkAndDropsTheLate(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	ev := func(sec float64) string { return iso(T0 + sec*1000) }
	for _, x := range [][2]float64{{10, 1}, {50, 2}, {70, 3}, {20, 4}, {130, 5}} {
		h.push("readings", "sensor", m("at", ev(x[0]), "v", x[1]))
	}
	r := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"readings"}}).
		WindowTumbling(60, streams.WithEventTimeOf(func(msg interface{}) (interface{}, error) { return data(msg)["at"], nil }), streams.WithAllowedLateness(5)).
		AggregateFields(field("sum", "v")).
		Map(func(acc interface{}, ctx streams.EmitCtx) (interface{}, error) {
			return m("minute", ctx["windowKey"].(string)[11:16], "sum", get(acc, "sum")), nil
		}).To(q.Queue("sums")), fast(streams.FeedRunOptions{QueryID: "event-time"}))
	until(t, func() bool { return h.count("sums") >= 2 }, 0, "two sums")
	stop(t, r)
	eq(t, h.payloads("sums"), []interface{}{m("minute", "09:00", "sum", 3), m("minute", "09:01", "sum", 3)}, "sums")
	if r.Metrics().Late != 1 {
		t.Fatal("the reading of 09:00:20 came after the watermark passed it", r.Metrics().Late)
	}
}

func TestFeedWindowsAChainThatNoLongerFitsIsRefusedAndResetStartsOver(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.at(1000)
	h.push("orders", "c1", m())
	first := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest"}).WindowTumbling(60).AggregateFields(countAgg()).To(q.Queue("counts")),
		fast(streams.FeedRunOptions{QueryID: "chain", Now: h.now()}))
	until(t, func() bool { return first.Metrics().StateKeys >= 1 }, 0, "a key")
	stop(t, first)
	if len(h.b.KVKeys(NS, "chain#snap#")) < 1 {
		t.Fatal("a stop writes a snapshot")
	}
	changed := func() *streams.Stream {
		return streams.FromFeed(q, streams.FeedSource{From: "earliest"}).WindowTumbling(30).AggregateFields(countAgg()).To(q.Queue("counts"))
	}
	if err := h.runErr(changed(), fast(streams.FeedRunOptions{QueryID: "chain"})); feedrt.ErrorCode(err) != "feed_chain_changed" {
		t.Fatal(err)
	}
	again := h.run(changed(), fast(streams.FeedRunOptions{QueryID: "chain", ResetToken: "v2", Now: h.now()}))
	until(t, func() bool { return again.Metrics().EntriesRead >= 1 }, 0, "read again")
	stop(t, again)
	stored := h.record("chain")
	if num(stored["epoch"]) != 1 || stored["resetToken"] != "v2" {
		t.Fatal(stored)
	}
	third := h.run(changed(), fast(streams.FeedRunOptions{QueryID: "chain", ResetToken: "v2", Now: h.now()}))
	wait(80)
	stop(t, third)
	if num(h.record("chain")["epoch"]) != 1 {
		t.Fatal("the same reset string carries on")
	}
}

func patternChain(q streams.FeedClient, sink streams.Sink, extra func(*streams.PatternOptions)) *streams.Stream {
	o := streams.PatternOptions{
		Start:  func(msg interface{}) (bool, error) { return get(msg, "queue") == "orders", nil },
		Then:   func(msg interface{}) (bool, error) { return get(msg, "queue") == "payments", nil },
		Within: "15m",
		OnMatch: func(order, payment interface{}) (interface{}, error) {
			return m("order", get(order, "data", "id"), "paid", get(payment, "data", "amount")), nil
		},
		OnTimeout: func(order interface{}) (interface{}, error) {
			return m("order", get(order, "data", "id"), "remind", true), nil
		},
	}
	if extra != nil {
		extra(&o)
	}
	return streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"orders", "payments"}}).Pattern(o).To(sink)
}

func TestFeedPatternsAnOrderThenItsPaymentIsAMatch(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.at(0)
	h.push("orders", "order-1", m("id", 1.0))
	h.at(60000)
	h.push("payments", "order-1", m("amount", 30.0))
	h.at(70000)
	h.push("orders", "order-3", m("id", 3.0))
	h.at(970000)
	h.push("payments", "order-3", m("amount", 5.0))
	h.at(971000)
	h.push("payments", "order-9", m("amount", 1.0))
	r := h.run(patternChain(q, q.Queue("followups"), nil), fast(streams.FeedRunOptions{QueryID: "paid", Now: h.now()}))
	until(t, func() bool { return h.count("followups") >= 2 }, 0, "two followups")
	wait(50)
	stop(t, r)
	eq(t, h.payloads("followups"), []interface{}{m("order", 1, "paid", 30), m("order", 3, "paid", 5)},
		"exactly 15 minutes is still in time; a payment with no order waits for nothing")
	if h.b.Messages("followups")[0].Partition != "order-1" {
		t.Fatal(h.b.Messages("followups")[0].Partition)
	}
}

func TestFeedPatternsNoPaymentInTimeSendsTheReminderOnAQuietFeed(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.at(0)
	h.push("orders", "order-2", m("id", 2.0))
	r := h.run(patternChain(q, q.Queue("followups"), nil), fast(streams.FeedRunOptions{QueryID: "remind", Now: h.now()}))
	until(t, func() bool { return r.Metrics().EntriesRead >= 1 }, 0, "one read")
	h.at(900000)
	wait(100)
	if h.count("followups") != 0 {
		t.Fatal("not at 15 minutes yet")
	}
	h.at(901000)
	until(t, func() bool { return h.count("followups") >= 1 }, 3000, "the reminder")
	h.at(950000)
	h.push("payments", "order-2", m("amount", 9.0))
	wait(100)
	stop(t, r)
	eq(t, h.payloads("followups"), []interface{}{m("order", 2, "remind", true)}, "followups")
}

func TestFeedPatternsOnRepeatRestartCountsFromTheLatestStart(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.at(0)
	h.push("orders", "cart-1", m("id", "a"))
	h.at(600000)
	h.push("orders", "cart-1", m("id", "b"))
	r := h.run(patternChain(q, q.Queue("followups"), func(o *streams.PatternOptions) { o.OnRepeat = "restart" }),
		fast(streams.FeedRunOptions{QueryID: "cart", Now: h.now()}))
	until(t, func() bool { return r.Metrics().EntriesRead >= 2 }, 0, "two read")
	h.at(1000000)
	wait(150)
	if h.count("followups") != 0 {
		t.Fatal("the second start moved the deadline to 25 minutes")
	}
	h.at(1501000)
	until(t, func() bool { return h.count("followups") >= 1 }, 0, "the reminder")
	stop(t, r)
	eq(t, h.payloads("followups"), []interface{}{m("order", "b", "remind", true)}, "followups")
}

func TestFeedPatternsAJoinPairsTwoMessagesInEitherOrder(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.at(0)
	h.push("shipments", "order-1", m("carrier", "DHL"))
	h.at(1000)
	h.push("orders", "order-1", m("id", 1.0))
	h.at(2000)
	h.push("orders", "order-2", m("id", 2.0))
	r := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"orders", "shipments"}}).
		Join(streams.JoinOptions{
			Left:   func(msg interface{}) (bool, error) { return get(msg, "queue") == "orders", nil },
			Right:  func(msg interface{}) (bool, error) { return get(msg, "queue") == "shipments", nil },
			Within: "1h",
			Select: func(order, shipment interface{}) (interface{}, error) {
				return m("order", get(order, "data", "id"), "carrier", get(shipment, "data", "carrier")), nil
			},
			OnTimeout: func(msg interface{}, side string) (interface{}, error) {
				return m("unshipped", get(msg, "data", "id"), "side", side), nil
			},
		}).To(q.Queue("joined")), fast(streams.FeedRunOptions{QueryID: "join", Now: h.now()}))
	until(t, func() bool { return h.count("joined") >= 1 }, 0, "the pair")
	h.at(2000 + 3601000)
	until(t, func() bool { return h.count("joined") >= 2 }, 3000, "the timeout of order 2")
	stop(t, r)
	eq(t, h.payloads("joined"), []interface{}{m("order", 1, "carrier", "DHL"), m("unshipped", 2, "side", "left")}, "joined")
}

func TestFeedPatternsAfterAFlatMapTheKeySeesEachItem(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.at(0)
	h.push("sync", "conv-1", m("changes", []interface{}{m("messageId", "m1"), m("messageId", "m2"), m("other", 1.0)}))
	h.at(10)
	h.push("delivered", "x", m("message", m("id", "m2")))
	h.at(15)
	h.push("other", "x", m())
	h.at(20)
	h.push("delivered", "x", m("message", m("id", "m1")))
	r := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"sync", "delivered", "other"}}).
		FlatMap(func(msg interface{}, _ streams.EmitCtx) ([]interface{}, error) {
			if get(msg, "queue") == "sync" {
				var out []interface{}
				for _, c := range data(msg)["changes"].([]interface{}) {
					if get(c, "messageId") != Undefined() {
						out = append(out, c)
					}
				}
				return out, nil
			}
			return []interface{}{data(msg)}, nil
		}).
		Pattern(streams.PatternOptions{
			Start: func(msg interface{}) (bool, error) { return get(msg, "queue") == "sync", nil },
			Then:  func(msg interface{}) (bool, error) { return get(msg, "queue") == "delivered", nil },
			// Asked only of sync and delivered messages: 'other' has no id at all.
			Key: func(msg, v interface{}) (interface{}, error) {
				if get(msg, "queue") == "sync" {
					return get(v, "messageId"), nil
				}
				return get(v, "message", "id"), nil
			},
			Within:  "1m",
			OnMatch: func(s, d interface{}) (interface{}, error) { return m("id", get(d, "data", "message", "id")), nil },
		}).To(q.Queue("ok")), fast(streams.FeedRunOptions{QueryID: "flat-key", Now: h.now()}))
	until(t, func() bool { return h.count("ok") >= 2 }, 0, "two matches")
	stop(t, r)
	var ids []string
	for _, p := range h.payloads("ok") {
		ids = append(ids, get(p, "id").(string))
	}
	sort.Strings(ids)
	eq(t, ids, []string{"m1", "m2"}, "ids")
}

// Undefined is jsv.Undefined (a field that is not there).
func Undefined() interface{} { return jsv.Undefined }

func TestFeedPatternsTheEntityCanBeAField(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.at(0)
	h.push("orders", "p-a", m("id", "X1"))
	h.at(1000)
	h.push("payments", "p-b", m("order", "X1", "amount", 3.0))
	r := h.run(patternChain(q, q.Queue("followups"), func(o *streams.PatternOptions) {
		o.Key = func(msg, _ interface{}) (interface{}, error) {
			if id := get(msg, "data", "id"); !jsv.IsNullish(id) {
				return id, nil
			}
			return get(msg, "data", "order"), nil
		}
	}), fast(streams.FeedRunOptions{QueryID: "by-field", Now: h.now()}))
	until(t, func() bool { return h.count("followups") >= 1 }, 0, "the match")
	stop(t, r)
	eq(t, h.payloads("followups"), []interface{}{m("order", "X1", "paid", 3)}, "followups")
	if h.b.Messages("followups")[0].Partition != "X1" {
		t.Fatal(h.b.Messages("followups")[0].Partition)
	}
}

func TestFeedCommitsAnAnswerLostIsNotWrittenTwiceStateless(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	for i := 1; i <= 3; i++ {
		h.push("orders", "c1", m("n", float64(i)))
	}
	// The broker applies the commit, and the answer never comes back.
	h.b.InjectNext(feedfake.Inject{When: "after", Reset: true})
	r := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"orders"}}).To(q.Queue("copy")),
		quiet(fast(streams.FeedRunOptions{QueryID: "lost-answer"})))
	until(t, func() bool { return r.Metrics().Conflicts >= 1 }, 4000, "the retry to find the commit stored")
	h.push("orders", "c1", m("n", 4.0))
	until(t, func() bool { return h.count("copy") >= 4 }, 0, "four copies")
	stop(t, r)
	eq(t, ns(h, "copy"), []float64{1, 2, 3, 4}, "copies")
}

func TestFeedCommitsAnAnswerLostDoesNotCountTwiceWindows(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.at(1000)
	for i := 1; i <= 3; i++ {
		h.push("orders", "c1", m("n", float64(i)))
	}
	h.b.InjectNext(feedfake.Inject{When: "after", Reset: true})
	r := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"orders"}}).
		WindowTumbling(60).AggregateFields(countAgg()).To(q.Queue("counts")),
		quiet(fast(streams.FeedRunOptions{QueryID: "lost-window", Now: h.now()})))
	until(t, func() bool { return r.Metrics().Conflicts >= 1 }, 4000, "the conflict")
	h.at(2000)
	h.push("orders", "c1", m("n", 4.0))
	h.at(61000)
	h.push("orders", "c2", m("n", 5.0))
	until(t, func() bool { return h.count("counts") >= 1 }, 0, "the window")
	stop(t, r)
	eq(t, h.payloads("counts"), []interface{}{m("count", 4)}, "counts")
}

func TestFeedCommitsAPageWithMoreOutputsThanMaxPendingCommitsBetweenPositions(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	for i := 1; i <= 7; i++ {
		h.push("orders", "c"+itoa(i), m("n", float64(i)))
	}
	r := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"orders"}}).To(q.Queue("copy")),
		fast(streams.FeedRunOptions{QueryID: "split", MaxPendingOutputs: streams.Int(2)}))
	until(t, func() bool { return h.count("copy") >= 7 }, 0, "seven copies")
	stop(t, r)
	eq(t, ns(h, "copy"), []float64{1, 2, 3, 4, 5, 6, 7}, "copies")
	var commits []feedfake.Txn
	for _, x := range h.b.Txns() {
		if len(x.Items) > 0 {
			commits = append(commits, x)
		}
	}
	if len(commits) < 4 {
		t.Fatal(len(commits))
	}
	for _, c := range commits {
		if len(c.Items) > 2 {
			t.Fatal(len(c.Items))
		}
	}
}

func TestFeedCommitsAProcessWhoseLockWasTakenStandsBy(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.push("orders", "c1", m("n", 1.0))
	r := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"orders"}}).To(q.Queue("copy")),
		quiet(fast(streams.FeedRunOptions{QueryID: "stolen"})))
	until(t, func() bool { return h.count("copy") >= 1 }, 0, "one copy")
	// The lock expired on the broker and somebody else holds it now.
	h.b.SetLock("queen-feed/stolen", feedfake.LockRow{Owner: "someone-else", Token: 9999, ExpiresAt: time.Now().Add(time.Minute)})
	h.push("orders", "c1", m("n", 2.0))
	until(t, func() bool { return r.Metrics().Role == "standby" }, 3000, "standby")
	wait(100)
	eq(t, ns(h, "copy"), []float64{1}, "n=2 is the new holder's to copy")
	stop(t, r)
}

// fakeSource is a queue source that never delivers (the chain is refused before it runs).
type fakeSource struct{ name string }

func (f fakeSource) Name() string                                   { return f.name }
func (f fakeSource) Batch(int) streams.Source                       { return f }
func (f fakeSource) Wait(bool) streams.Source                       { return f }
func (f fakeSource) TimeoutMillis(int) streams.Source               { return f }
func (f fakeSource) Group(string) streams.Source                    { return f }
func (f fakeSource) Partitions(int) streams.Source                  { return f }
func (f fakeSource) SubscriptionMode(string) streams.Source         { return f }
func (f fakeSource) SubscriptionFrom(string) streams.Source         { return f }
func (f fakeSource) Conflation(bool) streams.Source                 { return f }
func (f fakeSource) Pop(context.Context) ([]streams.Message, error) { return nil, nil }

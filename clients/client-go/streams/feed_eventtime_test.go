package streams_test

// The behaviours of clients/client-js/test-v2/feed-unit/feedEventTime.test.js:
// steps whose clock is the time the messages carry. History pushed all at
// once, messages out of order, a quiet feed, steps chained on event time, and
// a crash anywhere.

import (
	"errors"
	"math"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/smartpricing/queen/clients/client-go/v2/feed"
	"github.com/smartpricing/queen/clients/client-go/v2/internal/feedfake"
	"github.com/smartpricing/queen/clients/client-go/v2/internal/jsv"
	"github.com/smartpricing/queen/clients/client-go/v2/streams"
)

var H0 = func() float64 { t, _ := jsv.DateParse("2025-10-01T10:00:00.000Z"); return t }()

func ev(min, sec float64) string { return iso(H0 + min*60000 + sec*1000) }

func atField(msg interface{}) (interface{}, error) { return get(msg, "data", "at"), nil }

func (h *harness) issue(repo string, n, min float64) {
	h.push("issues", repo, m("n", n, "at", ev(min, 0)))
}

func (h *harness) comment(repo string, n, min float64) {
	h.push("comments", repo, m("n", n, "at", ev(min, 0)))
}

func repoKey(msg, _ interface{}) (interface{}, error) {
	return jsv.String(get(msg, "partition")) + "#" + jsv.String(get(msg, "data", "n")), nil
}

func unanswered(q streams.FeedClient, sink streams.Sink, extra func(*streams.PatternOptions)) *streams.Stream {
	o := streams.PatternOptions{
		Start: queueIs("issues"), Then: queueIs("comments"), Key: repoKey, Within: "1h",
		StepTime: streams.StepTime{EventTime: atField},
		OnMatch: func(s, t interface{}) (interface{}, error) {
			k, _ := repoKey(s, nil)
			return m("issue", k, "answered", get(t, "data", "at").(string)[11:16]), nil
		},
		OnTimeout: func(s interface{}) (interface{}, error) {
			k, _ := repoKey(s, nil)
			return m("issue", k, "unanswered", true), nil
		},
	}
	if extra != nil {
		extra(&o)
	}
	return streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"issues", "comments"}}).Pattern(o).Name("unanswered").To(sink)
}

func TestEventTimeHistoryPushedAllAtOnce(t *testing.T) {
	load := func(h *harness) {
		h.issue("r1", 1, 0)
		h.issue("r1", 2, 10)
		h.issue("r2", 1, 20)
		h.comment("r1", 1, 30)
		h.comment("r2", 1, 85) // 65 minutes after its issue
		h.issue("r3", 1, 120)
	}
	h := newHarness(t)
	q := h.client("queen")
	load(h)
	r := h.run(unanswered(q, q.Queue("out"), nil), fast(streams.FeedRunOptions{QueryID: "history"}))
	until(t, func() bool { return h.count("out") >= 3 }, 0, "three")
	wait(30)
	eq(t, h.payloads("out"), []interface{}{m("issue", "r1#1", "answered", "10:30"), m("issue", "r1#2", "unanswered", true), m("issue", "r2#1", "unanswered", true)}, "out")
	step := r.Metrics().Steps[0]
	if step.Clock != "event" || *step.Watermark != ev(120, 0) || step.Open != 1 || *r.Metrics().Watermark != ev(120, 0) {
		t.Fatal(step, *r.Metrics().Watermark)
	}
	f := newHarness(t)
	fq := f.client("queen")
	load(f)
	f.run(streams.FromFeed(fq, streams.FeedSource{From: "earliest", Queues: []string{"issues", "comments"}}).
		Pattern(streams.PatternOptions{Start: queueIs("issues"), Then: queueIs("comments"), Key: repoKey, Within: "1h",
			OnMatch: func(s, _ interface{}) (interface{}, error) { k, _ := repoKey(s, nil); return m("issue", k), nil }}).
		To(fq.Queue("out")), fast(streams.FeedRunOptions{QueryID: "history-feed"}))
	until(t, func() bool { return f.count("out") >= 2 }, 0, "two")
	eq(t, f.payloads("out"), []interface{}{m("issue", "r1#1"), m("issue", "r2#1")}, "the 65-minute comment matches on commit times")
}

func TestEventTimeAllowedLatenessOrdersAndLate(t *testing.T) {
	run := func(extra func(*streams.PatternOptions), queryID string) ([]interface{}, streams.FeedMetrics) {
		h := newHarness(t)
		q := h.client("queen")
		h.issue("r1", 1, 0)
		h.issue("r9", 1, 45)
		h.comment("r1", 1, 30) // 15 minutes out of order
		h.issue("r9", 2, 120)
		r := h.run(unanswered(q, q.Queue("out"), extra), fast(streams.FeedRunOptions{QueryID: queryID}))
		until(t, func() bool { return r.Metrics().EntriesRead >= 4 }, 0, "four")
		wait(60)
		return h.payloads("out"), r.Metrics()
	}
	out, mt := run(func(o *streams.PatternOptions) { o.AllowedLateness = "20m" }, "lateness")
	eq(t, out, []interface{}{m("issue", "r1#1", "answered", "10:30")}, "the comment waited for its place")
	if mt.Late != 0 || *mt.Steps[0].Held != 1 {
		t.Fatal(mt.Late, *mt.Steps[0].Held)
	}
	out, mt = run(nil, "strict")
	eq(t, out, []interface{}{m("issue", "r1#1", "unanswered", true), m("issue", "r9#1", "unanswered", true)}, "without lateness the comment is late")
	if mt.Late != 1 {
		t.Fatal(mt.Late)
	}
	out, mt = run(func(o *streams.PatternOptions) { o.OnLate = "include" }, "include")
	eq(t, out, []interface{}{m("issue", "r1#1", "answered", "10:30"), m("issue", "r9#1", "unanswered", true)}, "onLate include")
	if mt.Late != 1 {
		t.Fatal(mt.Late)
	}
}

func TestEventTimeAJoinPairsTheSidesAndItsDeadlineCountsOnEventTime(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.push("payments", "o1", m("at", ev(5, 0)))
	h.push("orders", "o1", m("at", ev(0, 0)))
	h.push("orders", "o2", m("at", ev(10, 0)))
	h.push("orders", "o3", m("at", ev(40, 0)))
	h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"orders", "payments"}}).
		Join(streams.JoinOptions{Left: queueIs("orders"), Right: queueIs("payments"), Within: "15m",
			StepTime: streams.StepTime{EventTime: atField, AllowedLateness: "5m"},
			Select: func(l, r interface{}) (interface{}, error) {
				lt, _ := jsv.DateParse(get(l, "data", "at").(string))
				rt, _ := jsv.DateParse(get(r, "data", "at").(string))
				return m("order", get(l, "partition"), "paidAfterSec", (rt-lt)/1000), nil
			},
			OnTimeout: func(msg interface{}, side string) (interface{}, error) {
				missing := "order"
				if side == "left" {
					missing = "payment"
				}
				return m("order", get(msg, "partition"), "missing", missing), nil
			}}).To(q.Queue("out")), fast(streams.FeedRunOptions{QueryID: "join"}))
	until(t, func() bool { return h.count("out") >= 2 }, 0, "two")
	wait(30)
	eq(t, h.payloads("out"), []interface{}{m("order", "o1", "paidAfterSec", 300), m("order", "o2", "missing", "payment")}, "out")
}

func TestEventTimeAGatherCountsItsDeadlineFromTheEarliestPart(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	part := func(queue string, run interface{}, min float64) { h.push(queue, "x", m("run", run, "at", ev(min, 0))) }
	part("c", 1.0, 9)
	part("a", 1.0, 0)
	part("b", 1.0, 5)
	part("a", 2.0, 20)
	part("b", 2.0, 25)
	part("c", 2.0, 31) // 11 minutes after its run's first part
	part("a", "zz", 50)
	h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"a", "b", "c"}}).
		Gather(streams.GatherOptions{Steps: streams.Queues("a", "b", "c"),
			Key:      func(msg, _ interface{}) (interface{}, error) { return jsv.String(get(msg, "data", "run")), nil },
			Within:   "10m",
			StepTime: streams.StepTime{EventTime: atField, AllowedLateness: "15m"},
			OnComplete: func(got interface{}, run string) (interface{}, error) {
				return m("run", run, "done", true, "first", get(got, "a", "data", "at").(string)[11:16]), nil
			},
			OnTimeout: func(_, missing interface{}, run string) (interface{}, error) {
				return m("run", run, "missing", missing), nil
			},
		}).To(q.Queue("out")), fast(streams.FeedRunOptions{QueryID: "gather"}))
	until(t, func() bool { return h.count("out") >= 2 }, 0, "two")
	wait(30)
	eq(t, h.payloads("out"), []interface{}{m("run", "1", "done", true, "first", "10:00"), m("run", "2", "missing", []string{"c"})}, "out")
}

func TestEventTimeASessionTakesItsMessagesInEventTimeOrder(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	for _, min := range []float64{0, 20, 10} {
		h.push("clicks", "p", m("user", "u", "at", ev(min, 0)))
	}
	h.push("clicks", "p", m("user", "v", "at", ev(120, 0)))
	h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"clicks"}}).
		KeyBy(func(msg interface{}) (string, error) { return jsv.String(get(msg, "data", "user")), nil }).
		WindowSession(900, streams.WithEventTimeOf(atField), streamsLateness(1800)).
		AggregateFields(streams.AggregateField{Name: "clicks", Fn: one}).
		Map(func(v interface{}, ctx streams.EmitCtx) (interface{}, error) {
			return m("user", ctx["key"], "clicks", get(v, "clicks"), "from", iso(num(ctx["windowStart"]))[11:16], "to", iso(num(ctx["windowEnd"]))[11:16]), nil
		}).To(q.Queue("sessions")), fast(streams.FeedRunOptions{QueryID: "sessions"}))
	until(t, func() bool { return h.count("sessions") >= 1 }, 0, "one")
	wait(30)
	eq(t, h.payloads("sessions"), []interface{}{m("user", "u", "clicks", 3, "from", "10:00", "to", "10:35")}, "sessions")
}

func streamsLateness(seconds float64) streams.WindowOption {
	return streams.WithAllowedLateness(seconds)
}

func TestEventTimeAMessageExactlyGapAfterTheLastOneIsStillInTheSession(t *testing.T) {
	sessions := func(grace bool, eventTime bool) []interface{} {
		h := newHarness(t)
		q := h.client("queen")
		for _, sec := range []float64{0, 10, 21} {
			h.at(sec * 1000)
			h.push("clicks", "u1", m("at", ev(0, sec)))
		}
		h.at(60000)
		h.push("clicks", "other", m("at", ev(1, 0)))
		var opts []streams.WindowOption
		if grace {
			opts = append(opts, streams.WithGracePeriod(2))
		}
		if eventTime {
			opts = append(opts, streams.WithEventTimeOf(atField))
		}
		r := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"clicks"}}).
			WindowSession(10, opts...).AggregateFields(countAgg()).
			Map(func(acc interface{}, ctx streams.EmitCtx) (interface{}, error) {
				return m("user", ctx["key"], "count", get(acc, "count")), nil
			}).
			To(q.Queue("sessions")), fast(streams.FeedRunOptions{QueryID: "gap", Now: h.now()}))
		until(t, func() bool { return h.count("sessions") >= 2 }, 0, "two")
		stop(t, r)
		var out []interface{}
		for _, p := range h.payloads("sessions") {
			if get(p, "user") == "u1" {
				out = append(out, p)
			}
		}
		return out
	}
	want := []interface{}{m("user", "u1", "count", 2), m("user", "u1", "count", 1)}
	eq(t, sessions(false, false), want, "feed's clock, no grace")
	eq(t, sessions(true, false), want, "feed's clock, grace")
	eq(t, sessions(false, true), want, "event time, no grace")
	eq(t, sessions(true, true), want, "event time, grace")
}

func pushes(h *harness) {
	for _, min := range []float64{5, 10, 15, 60, 90, 105, 180, 190, 200, 210} {
		h.push("pushes", "r1", m("at", ev(min, 0)))
	}
	h.push("pushes", "r2", m("at", ev(310, 0)))
}

func busyHours(q streams.FeedClient, sink streams.Sink, extra func(*streams.PatternOptions)) *streams.Stream {
	o := streams.PatternOptions{
		Start:  func(msg interface{}) (bool, error) { return num(get(msg, "data", "pushes")) >= 3, nil },
		Then:   func(msg interface{}) (bool, error) { return num(get(msg, "data", "pushes")) >= 3, nil },
		Within: "90m",
		OnMatch: func(a, c interface{}) (interface{}, error) {
			return m("repo", get(a, "partition"), "busy", []interface{}{get(a, "data", "hour"), get(c, "data", "hour")}), nil
		},
		OnTimeout: func(a interface{}) (interface{}, error) {
			return m("repo", get(a, "partition"), "alone", get(a, "data", "hour")), nil
		},
	}
	if extra != nil {
		extra(&o)
	}
	return streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"pushes"}}).
		WindowTumbling(3600, streams.WithEventTimeOf(atField)).
		AggregateFields(streams.AggregateField{Name: "pushes", Fn: one}).
		Map(func(v interface{}, ctx streams.EmitCtx) (interface{}, error) {
			out := jsv.Clone(v).(map[string]interface{})
			out["hour"] = ctx["windowKey"].(string)[11:13]
			return out, nil
		}).
		Pattern(o).Name("busy").To(sink)
}

func TestEventTimeAStepAfterAStepOnEventTimeRunsOnTheResultsTimes(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	pushes(h)
	r := h.run(busyHours(q, q.Queue("out"), nil), fast(streams.FeedRunOptions{QueryID: "chain"}))
	until(t, func() bool { return h.count("out") >= 2 }, 0, "two")
	wait(30)
	eq(t, h.payloads("out"), []interface{}{m("repo", "r1", "busy", []string{"10", "11"}), m("repo", "r1", "alone", "11")}, "out")
	st := r.Metrics().Steps
	if st[0].Clock != "event" || st[1].Clock != "event" || *st[1].Watermark != ev(310, 0) {
		t.Fatal(st)
	}
}

func TestEventTimeEventTimeFalseKeepsALaterStepOnTheFeedsClock(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	pushes(h)
	r := h.run(busyHours(q, q.Queue("out"), func(o *streams.PatternOptions) { o.EventTimeOff = true }), fast(streams.FeedRunOptions{QueryID: "chain-feed"}))
	until(t, func() bool { return h.count("out") >= 2 }, 0, "two")
	wait(30)
	eq(t, h.payloads("out"), []interface{}{m("repo", "r1", "busy", []string{"10", "11"}), m("repo", "r1", "busy", []string{"11", "13"})}, "out")
	st := r.Metrics().Steps
	if st[0].Clock != "event" || st[1].Clock != "feed" {
		t.Fatal(st)
	}
}

func TestEventTimeAQuietFeedWithIdleMovesTheWatermarkOn(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.at(1000)
	h.push("readings", "s1", m("at", ev(0, 10), "v", 1.0))
	h.push("readings", "s1", m("at", ev(0, 50), "v", 2.0))
	sums := func(idle bool) *streams.Stream {
		opts := []streams.WindowOption{streams.WithEventTimeOf(atField)}
		if idle {
			opts = append(opts, streams.WithIdleSpan("30s"))
		}
		return streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"readings"}}).
			WindowTumbling(60, opts...).AggregateFields(field("sum", "v")).
			Map(func(acc interface{}, ctx streams.EmitCtx) (interface{}, error) {
				return m("minute", ctx["windowKey"].(string)[11:16], "sum", get(acc, "sum")), nil
			})
	}
	still := h.run(sums(false).To(q.Queue("still")), fast(streams.FeedRunOptions{QueryID: "no-idle", Now: h.now()}))
	r := h.run(sums(true).To(q.Queue("sums")), fast(streams.FeedRunOptions{QueryID: "idle", Now: h.now()}))
	until(t, func() bool { return r.Metrics().EntriesRead >= 2 && still.Metrics().EntriesRead >= 2 }, 0, "both read")
	h.at(36000)
	wait(200)
	if h.count("sums") != 0 {
		t.Fatal("35 s after: the watermark moved 5 s past 10:00:50")
	}
	h.at(42000)
	until(t, func() bool { return h.count("sums") >= 1 }, 3000, "the minute")
	eq(t, h.payloads("sums"), []interface{}{m("minute", "10:00", "sum", 3)}, "sums")
	// Metrics() is a snapshot the runner refreshes as it works: wait for it to show the watermark.
	until(t, func() bool {
		steps := r.Metrics().Steps
		return len(steps) > 0 && steps[0].Watermark != nil && *steps[0].Watermark == ev(1, 1)
	}, 3000, "at 42 s, the watermark at 10:00:50 + 11 s")
	h.at(600000)
	wait(200)
	if h.count("still") != 0 {
		t.Fatal("without idle only a message moves the watermark")
	}
}

func TestEventTimeAFinishedImportReleasesWhatItHeldAtThePaceOfTheFeedsClock(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.at(1000)
	h.push("issues", "r1", m("n", 1.0, "at", ev(0, 0)))
	h.push("issues", "r1", m("n", 2.0, "at", ev(0, 5)))
	h.push("comments", "r1", m("n", 1.0, "at", ev(0, 12)))
	r := h.run(unanswered(q, q.Queue("out"), func(o *streams.PatternOptions) {
		o.Within, o.Idle, o.AllowedLateness = "20s", "30s", "10s"
	}), fast(streams.FeedRunOptions{QueryID: "tail", Now: h.now()}))
	until(t, func() bool { return r.Metrics().EntriesRead >= 3 }, 0, "three")
	wait(50)
	if h.count("out") != 0 || *r.Metrics().Steps[0].Held != 2 {
		t.Fatal(h.count("out"), *r.Metrics().Steps[0].Held)
	}
	h.at(1000 + 45000)
	until(t, func() bool { return h.count("out") >= 1 }, 3000, "the comment's turn")
	wait(50)
	eq(t, h.payloads("out"), []interface{}{m("issue", "r1#1", "answered", "10:00")}, "first")
	h.at(1000 + 54000)
	until(t, func() bool { return h.count("out") >= 2 }, 3000, "r1#2")
	eq(t, h.payloads("out"), []interface{}{m("issue", "r1#1", "answered", "10:00"), m("issue", "r1#2", "unanswered", true)}, "both")
}

func TestEventTimeAStopAndAStartKeepTheHeldMessagesAndTheWatermark(t *testing.T) {
	h := newHarness(t)
	h.issue("r1", 1, 0)
	h.issue("r2", 1, 10)
	qa := h.client("a")
	first := h.run(unanswered(qa, qa.Queue("out"), func(o *streams.PatternOptions) { o.AllowedLateness = "30m" }), fast(streams.FeedRunOptions{QueryID: "restart"}))
	until(t, func() bool { return first.Metrics().EntriesRead >= 2 }, 0, "two")
	if *first.Metrics().Steps[0].Held != 2 {
		t.Fatal(*first.Metrics().Steps[0].Held)
	}
	stop(t, first)
	h.comment("r1", 1, 50)
	h.issue("r3", 1, 200)
	qb := h.client("b")
	second := h.run(unanswered(qb, qb.Queue("out"), func(o *streams.PatternOptions) { o.AllowedLateness = "30m" }), fast(streams.FeedRunOptions{QueryID: "restart"}))
	until(t, func() bool { return h.count("out") >= 2 }, 0, "two")
	wait(30)
	eq(t, h.payloads("out"), []interface{}{m("issue", "r1#1", "answered", "10:50"), m("issue", "r2#1", "unanswered", true)}, "out")
	if second.Metrics().Replays != 0 {
		t.Fatal("the stop wrote a snapshot: nothing to read again")
	}
}

func TestEventTimeAReplayClosesAgainWhatIdleClosedAndSendsItOnce(t *testing.T) {
	h := newHarness(t)
	opts := func() streams.FeedRunOptions {
		return quiet(fast(streams.FeedRunOptions{QueryID: "idle-replay", Now: h.now(), LockTTL: time.Second, SnapshotEveryMillis: streams.Int(3600000)}))
	}
	chain := func(q streams.FeedClient, sink streams.Sink) *streams.Stream {
		return unanswered(q, sink, func(o *streams.PatternOptions) { o.Within, o.Idle = "1m", "30s" })
	}
	h.at(1000)
	h.push("issues", "r1", m("n", 1.0, "at", ev(0, 0)))
	h.push("issues", "r2", m("n", 1.0, "at", ev(0, 30)))
	qa := h.client("a")
	a := h.run(chain(qa, qa.Queue("out")), opts())
	until(t, func() bool { return a.Metrics().EntriesRead >= 2 }, 0, "two")
	h.at(1000 + 62000)
	until(t, func() bool { return h.count("out") >= 1 }, 3000, "r1#1")
	h.push("issues", "r4", m("n", 1.0, "at", ev(1, 15)))
	h.push("comments", "r2", m("n", 1.0, "at", ev(1, 20)))
	until(t, func() bool { return h.count("out") >= 2 }, 3000, "r2#1")
	cs := h.b.Messages("comments")
	end := cs[len(cs)-1].Position
	until(t, func() bool { return h.recordPosition("idle-replay") > float64(end) }, 0, "committed")
	if num(get(h.record("idle-replay"), "snapshot", "position")) != 1 {
		t.Fatal("no snapshot since the start", h.record("idle-replay")["snapshot"])
	}
	h.crash("a")
	qb := h.client("b")
	second := h.run(chain(qb, qb.Queue("out")), opts())
	until(t, func() bool { return second.Metrics().Role == "active" }, 6000, "the takeover")
	if second.Metrics().ReplayEntries < 4 {
		t.Fatal("replayed", second.Metrics().ReplayEntries)
	}
	h.at(1000 + 70000)
	h.push("issues", "r3", m("n", 1.0, "at", ev(2, 0)))
	h.at(1000 + 200000)
	until(t, func() bool { return h.count("out") >= 4 }, 4000, "four")
	wait(50)
	eq(t, h.payloads("out"), []interface{}{m("issue", "r1#1", "unanswered", true), m("issue", "r2#1", "answered", "10:01"),
		m("issue", "r4#1", "unanswered", true), m("issue", "r3#1", "unanswered", true)}, "out")
}

// lcg32 is the JS test's LCG in 32-bit integers, (Math.imul(seed, 1103515245) + 12345) & 0x7fffffff:
// in doubles the product loses its low bits.
func lcg32(seed uint32) func() float64 {
	s := seed
	return func() float64 {
		s = (s*1103515245 + 12345) & 0x7fffffff
		return float64(s) / 2147483648
	}
}

func TestEventTimeOutOfOrderHistoryCrashedAtRandomPoints(t *testing.T) {
	rnd := lcg32(11)
	type msg struct {
		i    int
		repo string
		open bool
		sec  float64
	}
	var msgs []msg
	e := 0.0
	for i := 0; i < 150; i++ {
		e += math.Floor(rnd() * 90)
		jitter := math.Floor((rnd() - 0.5) * 240)
		msgs = append(msgs, msg{i, "r" + jsv.NumberString(math.Floor(rnd()*6)), rnd() < 0.5, math.Max(0, e+jitter)})
	}
	chain := func(q streams.FeedClient, sink streams.Sink) *streams.Stream {
		return streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"issues", "comments"}}).
			Pattern(streams.PatternOptions{Start: queueIs("issues"), Then: queueIs("comments"), Within: "10m",
				StepTime: streams.StepTime{EventTime: atField, AllowedLateness: "3m", Idle: "30s"},
				OnMatch: func(s, t interface{}) (interface{}, error) {
					return m("repo", get(s, "partition"), "s", get(s, "data", "i"), "t", get(t, "data", "i")), nil
				},
				OnTimeout: func(s interface{}) (interface{}, error) {
					return m("repo", get(s, "partition"), "s", get(s, "data", "i"), "timeout", true), nil
				}}).
			KeyBy(func(v interface{}) (string, error) { return jsv.String(get(v, "repo")), nil }).
			WindowTumbling(1800).AggregateFields(streams.AggregateField{Name: "results", Fn: one}).
			Map(func(v interface{}, ctx streams.EmitCtx) (interface{}, error) {
				return m("repo", ctx["key"], "half", ctx["windowKey"].(string)[11:16], "results", get(v, "results")), nil
			}).To(sink)
	}
	byRepo := func(h *harness) map[string][]interface{} {
		out := map[string][]interface{}{}
		for _, e := range h.b.Messages("out") {
			out[e.Partition] = append(out[e.Partition], e.Payload)
		}
		return out
	}
	load := func(h *harness) int64 {
		var last int64
		for _, x := range msgs {
			h.at(float64(x.i * 1000))
			queue := "comments"
			if x.open {
				queue = "issues"
			}
			last = h.push(queue, x.repo, m("i", float64(x.i), "at", iso(H0+x.sec*1000)))
		}
		return last
	}
	tail := func(h *harness, r streams.FeedHandle) {
		for k := 1; k <= 8; k++ {
			h.at(float64(len(msgs)*1000 + k*600000))
			wait(60)
		}
		until(t, func() bool {
			mt := r.Metrics()
			if mt.Open != 0 || mt.PendingOutputs != 0 {
				return false
			}
			for _, s := range mt.Steps {
				if s.Held != nil && *s.Held > 0 {
					return false
				}
			}
			return true
		}, 8000, "every wait and window to close")
		wait(100)
	}
	ref := newHarness(t)
	refEnd := load(ref)
	rq := ref.client("queen")
	r := ref.run(chain(rq, rq.Queue("out")), fast(streams.FeedRunOptions{QueryID: "rand-et", Now: ref.now()}))
	until(t, func() bool { return ref.recordPosition("rand-et") > float64(refEnd) }, 6000, "the reference")
	tail(ref, r)
	stop(t, r)
	expected := byRepo(ref)
	if len(expected) != 6 || ref.count("out") < 12 {
		t.Fatal(len(expected), ref.count("out"))
	}
	h := newHarness(t)
	end := load(h)
	opts := func() streams.FeedRunOptions {
		return quiet(fast(streams.FeedRunOptions{QueryID: "rand-et", Now: h.now(), LockTTL: time.Second, MaxRecords: 7, SnapshotMaxEntries: streams.Int(20)}))
	}
	name := "p0"
	q := h.client(name)
	runner := h.run(chain(q, q.Queue("out")), opts())
	for k := 1; k <= 3; k++ {
		target := math.Floor(float64(len(msgs)*k) / 4)
		until(t, func() bool { return h.recordPosition("rand-et") >= target }, 6000, "the target")
		h.crash(name)
		name = "p" + itoa(k)
		q = h.client(name)
		runner = h.run(chain(q, q.Queue("out")), opts())
		until(t, func() bool { return runner.Metrics().Role == "active" }, 6000, "the takeover")
	}
	until(t, func() bool { return h.recordPosition("rand-et") > float64(end) }, 6000, "the end")
	tail(h, runner)
	stop(t, runner)
	eq(t, byRepo(h), expected, "per repo")
}

func TestEventTimeAHeldMessageWhoseStepKeepsFailingMeetsThePoisonPolicy(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.issue("bad", 1, 0)
	h.issue("good", 1, 0)
	h.comment("bad", 1, 5)
	h.comment("good", 1, 6)
	h.issue("later", 1, 30)
	r := h.run(unanswered(q, q.Queue("out"), func(o *streams.PatternOptions) {
		o.AllowedLateness = "10m"
		o.OnMatch = func(s, _ interface{}) (interface{}, error) {
			if get(s, "partition") == "bad" {
				return nil, errors.New("boom")
			}
			k, _ := repoKey(s, nil)
			return m("issue", k, "answered", true), nil
		}
	}), quiet(fast(streams.FeedRunOptions{QueryID: "poison-held", OnPoison: "dlq"})))
	until(t, func() bool { return h.count("out") >= 1 && h.count("dlq") >= 1 }, 0, "out and dlq")
	wait(30)
	eq(t, h.payloads("out"), []interface{}{m("issue", "good#1", "answered", true)}, "out")
	rec := h.payloads("dlq")[0]
	if get(rec, "entry", "queue") != "comments" || get(rec, "entry", "partition") != "bad" || get(rec, "error", "message") != "boom" || r.Metrics().DeadLettered != 1 {
		t.Fatal(rec)
	}
}

func TestEventTimeALiveReadListsWhatAStepKeepsNotWhatItHolds(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.issue("r1", 1, 0)
	h.issue("r2", 1, 5)
	r := h.run(unanswered(q, q.Queue("out"), func(o *streams.PatternOptions) { o.AllowedLateness = "1h" }), fast(streams.FeedRunOptions{QueryID: "live"}))
	until(t, func() bool { return r.Metrics().EntriesRead >= 2 }, 0, "two")
	eq(t, r.State("unanswered").List(0), m("items", []interface{}{}, "truncated", false, "total", 0), "nothing kept yet")
	if *r.Metrics().Steps[0].Held != 2 {
		t.Fatal(*r.Metrics().Steps[0].Held)
	}
	h.issue("r3", 1, 62)
	until(t, func() bool { return r.Metrics().Steps[0].Open == 1 }, 0, "one open")
	l := r.State("unanswered").List(0)
	if num(l["total"]) != 1 || get(l["items"].([]interface{})[0], "key") != "r1#1" || r.State("unanswered").Get("r1#1")["found"] != true {
		t.Fatal(l)
	}
}

func TestEventTimeRefusesIdleLatenessAndOnLateOnTheFeedsClock(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	pattern := func(st streams.StepTime) *streams.Stream {
		return streams.FromFeed(q, streams.FeedSource{From: "earliest"}).
			Pattern(streams.PatternOptions{Start: yes, Then: yes, Within: "1m", StepTime: st}).To(q.Queue("out"))
	}
	check := func(err error, want string) {
		t.Helper()
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("%v, want %q", err, want)
		}
	}
	check(h.runErr(pattern(streams.StepTime{Idle: "30s"}), fast(streams.FeedRunOptions{QueryID: "v1"})), "idle moves a watermark on")
	check(h.runErr(pattern(streams.StepTime{AllowedLateness: "1m"}), fast(streams.FeedRunOptions{QueryID: "v2"})), "allowedLateness and onLate are for event time")
	check(h.runErr(pattern(streams.StepTime{OnLate: "include"}), fast(streams.FeedRunOptions{QueryID: "v3"})), "allowedLateness and onLate are for event time")
	window := func(opts ...streams.WindowOption) *streams.Stream {
		return streams.FromFeed(q, streams.FeedSource{From: "earliest"}).WindowTumbling(60, opts...).
			AggregateFields(streams.AggregateField{Name: "n", Fn: one}).To(q.Queue("out"))
	}
	check(h.runErr(window(streams.WithIdle(30)), fast(streams.FeedRunOptions{QueryID: "v4"})), "idle moves a watermark on")
	check(h.runErr(window(streams.WithAllowedLateness(30)), fast(streams.FeedRunOptions{QueryID: "v6"})),
		"allowedLateness and onLate are for event time: the tumbling window")
	check(h.runErr(window(streams.WithOnLate("include")), fast(streams.FeedRunOptions{QueryID: "v7"})),
		"allowedLateness and onLate are for event time: the tumbling window")
	check(h.runErr(streams.FromFeed(q, streams.FeedSource{From: "earliest"}).WindowSession(60, streams.WithAllowedLatenessSpan("1m")).
		AggregateFields(streams.AggregateField{Name: "n", Fn: one}).To(q.Queue("out")), fast(streams.FeedRunOptions{QueryID: "v8"})),
		"allowedLateness and onLate are for event time: the session window")
	// The plain values pass: no lateness, late messages dropped.
	plain := h.run(window(streams.WithAllowedLateness(0), streams.WithOnLate("drop")), fast(streams.FeedRunOptions{QueryID: "v9"}))
	stop(t, plain)
	_, err := streams.From(fakeSource{name: "src"}).WindowTumbling(60, streams.WithIdle(30)).AggregateFields(streams.AggregateField{Name: "n", Fn: one}).
		To(sinkName("out")).Run(nil, streams.RunOptions{QueryID: "v5"})
	check(err, "runs on Stream.fromFeed only")
	_, err = pattern(streams.StepTime{EventTime: atField, Idle: 0.0}).CompileFeed()
	check(err, "idle is a duration above zero")
}

func TestEventTimeReviewAStepAfterAStepOnEventTimeFollowsItsWatermark(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	push := func(ms float64, queue string, n float64) {
		h.at(ms)
		h.push(queue, "r1", m("n", n, "at", iso(T0+ms)))
	}
	push(0, "issues", 1)
	push(300000, "issues", 2)
	push(400000, "comments", 2)
	r := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"issues", "comments"}}).
		Pattern(streams.PatternOptions{Start: queueIs("issues"), Then: queueIs("comments"), Key: repoKey, Within: "10m",
			StepTime: streams.StepTime{EventTime: atField, Idle: "1m"},
			OnMatch: func(s, _ interface{}) (interface{}, error) {
				return m("issue", get(s, "data", "n"), "answered", true), nil
			},
			OnTimeout: func(s interface{}) (interface{}, error) {
				return m("issue", get(s, "data", "n"), "answered", false), nil
			}}).
		KeyBy(func(interface{}) (string, error) { return "r1", nil }).
		WindowTumbling(3600).AggregateFields(streams.AggregateField{Name: "results", Fn: one}).
		To(q.Queue("hourly")), fast(streams.FeedRunOptions{QueryID: "review-h1", Now: h.now()}))
	until(t, func() bool { return r.Metrics().EntriesRead >= 3 }, 0, "three")
	for _, ms := range []float64{500000, 700000, 1200000, 3000000, 4000000, 5000000, 6000000} {
		h.at(ms)
		wait(60)
	}
	until(t, func() bool { return h.count("hourly") >= 1 }, 3000, "the hour")
	wait(50)
	if r.Metrics().Late != 0 {
		t.Fatal(r.Metrics().Late)
	}
	eq(t, h.payloads("hourly"), []interface{}{m("results", 2)}, "hourly")
}

func TestEventTimeReviewAWindowAfterAWindowCountsEveryReadingOnce(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	for k := 0; k < 60; k++ {
		h.at(float64(k * 20000))
		h.push("r", "s", m("at", iso(H0+math.Floor(float64(k*20000)/3))))
	}
	r := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"r"}}).
		WindowTumbling(60, streams.WithEventTimeOf(atField), streams.WithIdleSpan("30s")).
		AggregateFields(streams.AggregateField{Name: "n", Fn: one}).
		WindowTumbling(3600).AggregateFields(field("n", "n")).
		To(q.Queue("out")), fast(streams.FeedRunOptions{QueryID: "review-h1b", Now: h.now()}))
	until(t, func() bool { return r.Metrics().EntriesRead >= 60 }, 0, "sixty")
	for k := 1; k <= 8; k++ {
		h.at(float64(60*20000 + k*900000))
		wait(60)
	}
	until(t, func() bool { return h.count("out") >= 1 }, 3000, "the hour")
	wait(50)
	if r.Metrics().Late != 0 {
		t.Fatal(r.Metrics().Late)
	}
	eq(t, h.payloads("out"), []interface{}{m("n", 60)}, "out")
}

func TestEventTimeReviewIdleGoesOnTheFirstStepOnEventTimeOnly(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	first := func() *streams.Stream {
		return streams.FromFeed(q, streams.FeedSource{From: "earliest"}).WindowTumbling(60, streams.WithEventTimeOf(atField)).
			AggregateFields(streams.AggregateField{Name: "n", Fn: one})
	}
	err := h.runErr(first().WindowTumbling(3600, streams.WithIdle(30)).AggregateFields(field("n", "n")).To(q.Queue("out")), fast(streams.FeedRunOptions{QueryID: "v-fed-idle"}))
	if err == nil || !strings.Contains(err.Error(), "follows its watermark: put idle on the first step") {
		t.Fatal(err)
	}
	err = h.runErr(first().Pattern(streams.PatternOptions{Start: yes, Then: yes, Within: "1m",
		StepTime: streams.StepTime{EventTime: func(msg interface{}) (interface{}, error) { return get(msg, "ts"), nil }, Idle: "30s"}}).To(q.Queue("out")),
		fast(streams.FeedRunOptions{QueryID: "v-fed-own"}))
	if err == nil || !strings.Contains(err.Error(), "results of the step before it") {
		t.Fatal(err)
	}
}

func TestEventTimeReviewAHeldMessageWhoseUnitFailedLeavesNothingBehind(t *testing.T) {
	run := func(withBad bool) ([]interface{}, int64) {
		h := newHarness(t)
		q := h.client("queen")
		items := []interface{}{}
		if withBad {
			items = []interface{}{m("kind", "a", "id", "k1"), m("id", "bad")}
		}
		h.b.Push(feedfake.Msg{Queue: "ev", Partition: "p", Data: m("at", ev(0, 0), "items", items)},
			feedfake.Msg{Queue: "ev", Partition: "p", Data: m("at", ev(20, 0), "items", []interface{}{m("kind", "a", "id", "k2")})})
		h.push("ev", "p", m("at", ev(31, 0), "items", []interface{}{m("kind", "a", "id", "k3")}))
		h.push("ev", "p", m("at", ev(10, 0), "items", []interface{}{m("kind", "b", "id", "k2")}))
		h.push("ev", "p", m("at", ev(200, 0), "items", []interface{}{m("kind", "a", "id", "k9")}))
		r := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"ev"}}).
			FlatMap(func(msg interface{}, _ streams.EmitCtx) ([]interface{}, error) {
				return get(msg, "data", "items").([]interface{}), nil
			}).
			Gather(streams.GatherOptions{
				Steps: []streams.Part{
					{Name: "a", Test: func(_, v interface{}) (bool, error) { return get(v, "kind") == "a", nil }},
					{Name: "b", Test: func(_, v interface{}) (bool, error) {
						k, ok := get(v, "kind").(string)
						if !ok {
							return false, errors.New("Cannot read properties of undefined (reading 'startsWith')")
						}
						return strings.HasPrefix(k, "b"), nil
					}},
				},
				Key:      func(_, v interface{}) (interface{}, error) { return get(v, "id"), nil },
				Within:   "5m",
				StepTime: streams.StepTime{EventTime: atField, AllowedLateness: "30m"},
				OnComplete: func(_ interface{}, key string) (interface{}, error) {
					return m("key", key, "done", true), nil
				},
				OnTimeout: func(_, missing interface{}, key string) (interface{}, error) {
					return m("key", key, "missing", missing), nil
				},
			}).To(q.Queue("out")), quiet(fast(streams.FeedRunOptions{QueryID: "review-h2", OnPoison: "skip"})))
		until(t, func() bool { return r.Metrics().EntriesRead >= 5 }, 0, "five")
		wait(80)
		var out []interface{}
		for _, p := range h.payloads("out") {
			if get(p, "key") == "k2" {
				out = append(out, p)
			}
		}
		return out, r.Metrics().Skipped
	}
	want := []interface{}{m("key", "k2", "missing", []string{"a"}), m("key", "k2", "missing", []string{"b"})}
	out, _ := run(false)
	eq(t, out, want, "without the bad item")
	out, skipped := run(true)
	if skipped != 1 {
		t.Fatal(skipped)
	}
	eq(t, out, want, "with the bad item")
}

func TestEventTimeReviewTwoHeldItemsOfOneMessageThatBothFailGoToTheDLQEachOnce(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.push("issues", "r1", m("ids", []interface{}{"a", "b"}, "bad", true, "at", ev(0, 0)))
	h.push("comments", "r1", m("ids", []interface{}{"a", "b"}, "at", ev(5, 0)))
	h.push("issues", "r1", m("ids", []interface{}{"c"}, "at", ev(30, 0)))
	r := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"issues", "comments"}}).
		FlatMap(func(msg interface{}, _ streams.EmitCtx) ([]interface{}, error) {
			var out []interface{}
			for _, id := range get(msg, "data", "ids").([]interface{}) {
				out = append(out, m("id", id))
			}
			return out, nil
		}).
		Pattern(streams.PatternOptions{Start: queueIs("issues"), Then: queueIs("comments"),
			Key: func(_, v interface{}) (interface{}, error) { return get(v, "id"), nil }, Within: "1h",
			StepTime: streams.StepTime{EventTime: atField},
			OnMatch: func(s, _ interface{}) (interface{}, error) {
				if jsv.Truthy(get(s, "data", "bad")) {
					return nil, errors.New("boom")
				}
				return m("ok", true), nil
			}}).
		To(q.Queue("out")), quiet(fast(streams.FeedRunOptions{QueryID: "review-h3", OnPoison: "dlq"})))
	until(t, func() bool { return h.count("dlq") >= 2 }, 3000, "two dead letters")
	wait(80)
	dl := h.b.Messages("dlq")
	if len(dl) != 2 || dl[0].TransactionID == dl[1].TransactionID || r.Metrics().Conflicts > 1 {
		t.Fatal(len(dl), r.Metrics().Conflicts)
	}
}

func TestEventTimeReviewAnInvalidDateMeetsThePoisonPolicy(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.push("issues", "r1", m("n", 1.0, "at", ev(0, 0)))
	h.push("issues", "r1", m("n", 2.0))
	h.push("comments", "r1", m("n", 1.0, "at", ev(5, 0)))
	for k := 3; k < 10; k++ {
		h.push("issues", "r1", m("n", float64(k), "at", ev(float64(60*k), 0)))
	}
	r := h.run(unanswered(q, q.Queue("out"), func(o *streams.PatternOptions) {
		// new Date(undefined): an Invalid Date, no time.
		o.EventTime = func(msg interface{}) (interface{}, error) {
			s, ok := get(msg, "data", "at").(string)
			if !ok {
				return time.Time{}, nil
			}
			t, _ := jsv.DateParse(s)
			return time.UnixMilli(int64(t)), nil
		}
	}), quiet(fast(streams.FeedRunOptions{QueryID: "review-m1", OnPoison: "skip"})))
	until(t, func() bool { return r.Metrics().EntriesRead >= 10 }, 0, "ten")
	until(t, func() bool { return h.count("out") >= 3 }, 0, "three")
	if r.Metrics().Skipped != 1 {
		t.Fatal(r.Metrics().Skipped)
	}
	eq(t, h.payloads("out")[:2], []interface{}{m("issue", "r1#1", "answered", "10:05"), m("issue", "r1#3", "unanswered", true)}, "out")
	w := newHarness(t)
	wq := w.client("queen")
	for _, min := range []float64{0, 1, 2, 30, 31, 90} {
		w.push("r", "s", m("at", ev(min, 0)))
	}
	w.run(streams.FromFeed(wq, streams.FeedSource{From: "earliest", Queues: []string{"r"}}).
		WindowTumbling(600, streams.WithEventTimeOf(atField), streams.WithAllowedLatenessSpan("5m")).
		AggregateFields(streams.AggregateField{Name: "n", Fn: one}).
		To(wq.Queue("out")), fast(streams.FeedRunOptions{QueryID: "review-m1b"}))
	until(t, func() bool { return w.count("out") >= 2 }, 0, "two")
	eq(t, w.payloads("out"), []interface{}{m("n", 3), m("n", 2)}, "a window takes allowedLateness as a duration")
}

func TestEventTimeReviewAMessageAPatternIgnoresNeedsNoTime(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.push("issues", "r1", m("n", 1.0, "at", ev(0, 0)))
	h.push("audit", "x", m("who", "bot"))
	h.push("audit", "x", m("at", ev(60*24*365, 0)))
	h.push("comments", "r1", m("n", 1.0, "at", ev(5, 0)))
	h.push("issues", "r2", m("n", 1.0, "at", ev(30, 0)))
	r := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest"}).
		Pattern(streams.PatternOptions{Start: queueIs("issues"), Then: queueIs("comments"), Key: repoKey, Within: "1h",
			StepTime:  streams.StepTime{EventTime: atField, AllowedLateness: "10m"},
			OnMatch:   func(s, _ interface{}) (interface{}, error) { return m("answered", get(s, "data", "n")), nil },
			OnTimeout: func(s interface{}) (interface{}, error) { return m("unanswered", get(s, "data", "n")), nil }}).
		To(q.Queue("out")), quiet(fast(streams.FeedRunOptions{QueryID: "review-m2"})))
	until(t, func() bool { return r.Metrics().EntriesRead >= 5 }, 0, "five")
	until(t, func() bool { return h.count("out") >= 1 }, 0, "one")
	wait(50)
	if r.Metrics().Stopped != nil || r.Metrics().Late != 0 {
		t.Fatal(r.Metrics().Stopped, r.Metrics().Late)
	}
	eq(t, h.payloads("out"), []interface{}{m("answered", 1)}, "out")
}

func TestEventTimeReviewTheChainHashSaysWhichStepsTakeTheEventTimeBefore(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	hash := func(s *streams.Stream) string {
		c, err := s.To(q.Queue("out")).CompileFeed()
		if err != nil {
			t.Fatal(err)
		}
		return c.ChainHash
	}
	win := func() *streams.Stream {
		return streams.FromFeed(q, streams.FeedSource{}).WindowTumbling(60, streams.WithEventTimeOf(atField)).AggregateFields(streams.AggregateField{Name: "n", Fn: one})
	}
	pat := func(off bool) streams.PatternOptions {
		return streams.PatternOptions{Start: yes, Then: yes, Within: "1m", StepTime: streams.StepTime{EventTimeOff: off}}
	}
	if hash(win().Pattern(pat(false))) == hash(win().Pattern(pat(true))) {
		t.Fatal("inherited event time vs the feed clock")
	}
	plain := func(opts ...streams.WindowOption) *streams.Stream {
		return streams.FromFeed(q, streams.FeedSource{}).WindowTumbling(60, opts...).AggregateFields(streams.AggregateField{Name: "n", Fn: one})
	}
	if hash(plain()) != hash(plain(streams.WithEventTimeOff())) {
		t.Fatal("eventTime: false on a first step changes nothing")
	}
	_ = sort.Strings
	_ = feed.ShardOf
}

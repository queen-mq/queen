package streams_test

// The behaviours of clients/client-js/test-v2/feed-unit/feedRecords.test.js
// (jsref4): the description (<queryId>#about), and in the position record the
// feed time it read up to, caughtUp, a heartbeat on a quiet feed, what each
// step holds, and why it stopped.

import (
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/smartpricing/queen/clients/client-go/v2/internal/jsv"
	"github.com/smartpricing/queen/clients/client-go/v2/streams"
)

func (h *harness) version(key string) int64 {
	_, v, _ := h.b.KVGet(NS, key)
	return v
}

func aboutChain(q streams.FeedClient, sink streams.Sink, seconds float64) *streams.Stream {
	return streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"orders", "pay*"}}).
		Where("data.country", "=", "IT").
		Map(func(msg interface{}, _ streams.EmitCtx) (interface{}, error) { return data(msg), nil }).
		WindowTumbling(seconds).
		AggregateFields(countAgg(), field("sum", "total")).
		Name("perCustomer").
		To(sink)
}

func TestRecordsTheDescriptionSaysWhatTheStreamIs(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.push("orders", "c1", m("country", "IT", "total", 1.0))
	h.run(aboutChain(q, q.Queue("totals"), 60), fast(streams.FeedRunOptions{QueryID: "about", OnPoison: "feed-dlq"}))
	until(t, func() bool { return h.record("about#about") != nil }, 3000, "the first commit")
	about := h.record("about#about")
	if num(about["v"]) != 1 || about["queryId"] != "about" {
		t.Fatal(about)
	}
	if !regexp.MustCompile(`^client-go \d+\.\d+\.\d+`).MatchString(about["client"].(string)) || !regexp.MustCompile(`^[0-9a-f]{16}$`).MatchString(about["hash"].(string)) {
		t.Fatal(about["client"], about["hash"])
	}
	inCommit := false
	for _, x := range h.b.Txns() {
		hasAbout, hasPos := false, false
		for _, op := range x.KV {
			if op["key"] == "about#about" {
				hasAbout = true
			}
			if op["key"] == "about" {
				hasPos = true
			}
		}
		if hasAbout && hasPos {
			inCommit = true
		}
	}
	if !inCommit {
		t.Fatal("in a commit of the position")
	}
	eq(t, about["reads"], m("queues", []string{"orders", "pay*"}, "partitions", nil, "where", []interface{}{m("field", "data.country", "op", "=", "value", "IT")}),
		"a where before the first stateful step is the broker's filter")
	steps := about["steps"].([]interface{})
	if get(steps[0], "kind") != "ops" || get(steps[1], "kind") != "window" {
		t.Fatal(steps)
	}
	eq(t, steps[0], m("kind", "ops", "ops", []string{"map"}), "ops")
	w := steps[1]
	if get(w, "name") != "perCustomer" || get(w, "clock") != "feed" || get(w, "window", "kind") != "window-tumbling" || num(get(w, "window", "seconds")) != 60 {
		t.Fatal(w)
	}
	eq(t, get(w, "reducer"), m("kind", "aggregate", "fields", []string{"count", "sum"}), "reducer")
	eq(t, about["sink"], m("kind", "queues", "queues", []string{"totals"}), "sink")
	eq(t, about["onPoison"], m("queue", "feed-dlq"), "onPoison")
	if about["shards"] != nil || about["exclusive"] != true {
		t.Fatal(about["shards"], about["exclusive"])
	}
}

func TestRecordsTheDescriptionIsWrittenOncePerActivation(t *testing.T) {
	h := newHarness(t)
	h.push("orders", "c1", m("country", "IT", "total", 1.0))
	qa := h.client("a")
	first := h.run(aboutChain(qa, qa.Queue("totals"), 60), fast(streams.FeedRunOptions{QueryID: "again"}))
	until(t, func() bool { return h.record("again#about") != nil }, 3000, "the first commit")
	v1 := h.version("again#about")
	h.push("orders", "c2", m("country", "IT", "total", 1.0))
	until(t, func() bool { return h.recordPosition("again") > float64(h.b.Messages("orders")[1].Position) }, 0, "the next commit")
	if h.version("again#about") != v1 {
		t.Fatal("not at every commit")
	}
	stop(t, first)
	qc := h.client("c")
	h.run(aboutChain(qc, qc.Queue("totals"), 10), fast(streams.FeedRunOptions{QueryID: "again", ResetToken: "v2"}))
	until(t, func() bool { return h.version("again#about") > v1 }, 3000, "the new deploy's first commit")
	if num(get(h.record("again#about")["steps"].([]interface{})[1], "window", "seconds")) != 10 {
		t.Fatal(h.record("again#about"))
	}
}

func TestRecordsTheDescriptionNamesAPatternAGatherATableAndTheirClocks(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.push("orders", "c1", m("at", iso(T0)))
	h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"orders", "payments"}}).
		JoinTable(streams.TableOptions{Queue: "customers", On: func(msg, _ interface{}) (interface{}, error) { return get(msg, "partition"), nil },
			Select: func(v, row, _ interface{}) (interface{}, error) {
				out := jsv.Clone(v).(map[string]interface{})
				tier := get(row, "tier")
				if jsv.IsNullish(tier) {
					tier = nil
				}
				out["tier"] = tier
				return out, nil
			}}).
		Name("customers").
		Pattern(streams.PatternOptions{Start: queueIs("orders"), Then: queueIs("payments"), Within: "15m",
			StepTime:  streams.StepTime{EventTime: func(msg interface{}) (interface{}, error) { return get(msg, "data", "at"), nil }},
			OnTimeout: func(s interface{}) (interface{}, error) { return m("unpaid", get(s, "partition")), nil }}).
		Name("unpaid").
		Gather(streams.GatherOptions{
			Steps: []streams.Part{{Name: "unpaid", Test: func(msg, _ interface{}) (bool, error) { return !jsv.IsUndefined(get(msg, "data", "unpaid")), nil }}},
			Key:   func(msg, _ interface{}) (interface{}, error) { return get(msg, "data", "unpaid"), nil }, Within: "1h",
			StepTime: streams.StepTime{EventTimeOff: true}}).
		Name("alerts").
		ToFunc(func(v, _ interface{}) (interface{}, error) {
			if jsv.Truthy(get(v, "unpaid")) {
				return "alerts", nil
			}
			return nil, nil
		}), fast(streams.FeedRunOptions{QueryID: "kinds", Shards: 2, OnPoison: "skip"}))
	until(t, func() bool { return h.record("kinds#about") != nil }, 3000, "a shard's first commit")
	about := h.record("kinds#about")
	steps := about["steps"].([]interface{})
	var kinds [][]interface{}
	for _, s := range steps {
		kinds = append(kinds, []interface{}{get(s, "kind"), get(s, "name")})
	}
	eq(t, kinds, [][]string{{"table", "customers"}, {"pattern", "unpaid"}, {"gather", "alerts"}}, "kinds and names")
	eq(t, get(steps[0], "config", "queues"), []string{"customers"}, "table queues")
	if !jsv.IsUndefined(get(steps[0], "clock")) {
		t.Fatal("a table runs on no clock", get(steps[0], "clock"))
	}
	if get(steps[1], "clock") != "event" || num(get(steps[1], "config", "within")) != 15*60000 {
		t.Fatal(steps[1])
	}
	if get(steps[2], "clock") != "feed" {
		t.Fatal("eventTime: false, after a step on event time", get(steps[2], "clock"))
	}
	eq(t, get(steps[2], "config", "parts"), []string{"unpaid"}, "the gather's parts")
	eq(t, get(about["reads"], "tables"), []string{"customers"}, "tables")
	eq(t, about["sink"], m("kind", "route"), "sink")
	if about["onPoison"] != "skip" {
		t.Fatal(about["onPoison"])
	}
	eq(t, about["shards"], m("count", 2, "by", "partition"), "shards")
	eq(t, h.b.KVKeys(NS, "kinds#about"), []string{"kinds#about"}, "one description for every shard")
}

func TestRecordsTheFeedTimeItReadUpToAndThatItWasCaughtUp(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.at(1000)
	h.push("orders", "c1", m("n", 1.0))
	h.at(2000)
	h.push("orders", "c2", m("n", 2.0))
	last := h.b.Messages("orders")[1]
	r := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest"}).
		Map(func(msg interface{}, _ streams.EmitCtx) (interface{}, error) { return data(msg), nil }).
		To(q.Queue("out")), fast(streams.FeedRunOptions{QueryID: "read-to", Now: h.now()}))
	until(t, func() bool { return h.count("out") >= 2 && h.record("read-to")["caughtUp"] == true }, 0, "caught up")
	rec := h.record("read-to")
	lt, _ := jsv.DateParse(last.TS)
	if num(rec["readTo"]) < lt || rec["caughtUp"] != true {
		t.Fatal(rec)
	}
	if _, ok := rec["open"]; ok {
		t.Fatal("a chain with no state counts nothing", rec["open"])
	}
	if _, ok := rec["stopped"]; ok {
		t.Fatal(rec["stopped"])
	}
	stop(t, r)
}

func TestRecordsAQuietCaughtUpStreamKeepsItsRecordFresh(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.at(1000)
	h.push("orders", "c1", m())
	h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest"}).To(q.Queue("out")),
		fast(streams.FeedRunOptions{QueryID: "fresh", Now: h.now(), PositionCommitMillis: streams.Int(5000)}))
	until(t, func() bool { return h.count("out") >= 1 && h.record("fresh")["caughtUp"] == true }, 0, "caught up")
	v := h.version("fresh")
	h.at(3000)
	wait(100)
	if h.version("fresh") != v {
		t.Fatal("not before positionCommitMillis")
	}
	h.at(7000)
	until(t, func() bool { return h.version("fresh") > v }, 3000, "the next position commit")
	rec := h.record("fresh")
	if rec["caughtUp"] != true || rec["updatedAt"] != iso(T0+7000) {
		t.Fatal(rec)
	}
	if rt := num(rec["readTo"]); rt < T0+1000 || rt > T0+7000 {
		t.Fatal("readTo", rt-T0)
	}
	if num(h.record("fresh#about")["positionCommitMillis"]) != 5000 {
		t.Fatal(h.record("fresh#about"))
	}
}

func evAt(min float64) string {
	h0, _ := jsv.DateParse("2025-10-01T10:00:00.000Z")
	return iso(h0 + min*60000)
}

func TestRecordsCountsWhatEachStepHolds(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.at(1000)
	for _, c := range []string{"c1", "c2", "c3"} {
		h.push("orders", c, m("total", 1.0, "at", evAt(0)))
	}
	h.push("orders", "c1", m("total", 1.0, "at", evAt(30)))
	r := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest"}).
		WindowTumbling(60).AggregateFields(countAgg()).Name("perMinute").
		Pattern(streams.PatternOptions{Start: yes, Then: func(interface{}) (bool, error) { return false, nil }, Within: "1h",
			StepTime:  streams.StepTime{EventTime: func(interface{}) (interface{}, error) { return evAt(0), nil }, AllowedLateness: "1h"},
			OnTimeout: func(s interface{}) (interface{}, error) { return s, nil }}).
		Name("later").
		To(q.Queue("out")), fast(streams.FeedRunOptions{QueryID: "counts", Now: h.now()}))
	until(t, func() bool { return r.Metrics().EntriesRead >= 4 }, 0, "four read")
	until(t, func() bool { return num(get(h.record("counts")["open"], "perMinute")) == 3 }, 0, "three open")
	rec := h.record("counts")
	// The record's own bytes list the steps in chain order (JS Object.keys(r.open)).
	if raw := string(h.b.KVRaw(NS, "counts")); !strings.Contains(raw, `"open":{"perMinute":3,"later":0}`) {
		t.Fatal(raw)
	}
	if _, ok := rec["held"]; ok {
		t.Fatal("nothing reached the event-time step yet", rec["held"])
	}
}

func TestRecordsCountsTheMessagesAnEventTimeStepHolds(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.at(1000)
	h.push("issues", "r1", m("n", 1.0, "at", evAt(0)))
	h.push("issues", "r1", m("n", 2.0, "at", evAt(30)))
	r := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"issues", "comments"}}).
		Pattern(streams.PatternOptions{Start: queueIs("issues"), Then: queueIs("comments"),
			Key: func(msg, _ interface{}) (interface{}, error) {
				return jsv.String(get(msg, "partition")) + "#" + jsv.String(get(msg, "data", "n")), nil
			},
			Within: "1h",
			StepTime: streams.StepTime{EventTime: func(msg interface{}) (interface{}, error) { return get(msg, "data", "at"), nil },
				AllowedLateness: "20m"},
			OnTimeout: func(s interface{}) (interface{}, error) { return m("unanswered", get(s, "data", "n")), nil }}).
		Name("unanswered").To(q.Queue("out")), fast(streams.FeedRunOptions{QueryID: "held", Now: h.now()}))
	until(t, func() bool {
		m := r.Metrics()
		return len(m.Steps) > 0 && m.Steps[0].Held != nil && *m.Steps[0].Held == 1
	}, 0, "one held")
	until(t, func() bool { return num(get(h.record("held")["held"], "unanswered")) == 1 }, 3000, "the commit after it")
	if num(get(h.record("held")["open"], "unanswered")) != 1 {
		t.Fatal(h.record("held"))
	}
}

func failingOrders(h *harness, n int) {
	for i := 1; i <= n; i++ {
		h.push("orders", "c"+itoa(i), m("n", float64(i)))
	}
}

func TestRecordsAMessageThatKeepsFailingIsInTheRecord(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	failingOrders(h, 4)
	r := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest"}).
		Map(func(msg interface{}, _ streams.EmitCtx) (interface{}, error) {
			if num(data(msg)["n"]) == 3 {
				return nil, errors.New("bad order")
			}
			return data(msg), nil
		}).To(q.Queue("out")), quiet(fast(streams.FeedRunOptions{QueryID: "poison", MaxRecords: 2})))
	<-r.Done()
	third := h.b.Messages("orders")[2]
	rec := h.record("poison")
	st := rec["stopped"].(map[string]interface{})
	if num(rec["position"]) != float64(third.Position) || st["reason"] != "poison" || st["error"] != "bad order" || num(st["attempts"]) != 4 ||
		num(st["position"]) != float64(third.Position) || st["queue"] != "orders" || st["partition"] != "c3" || num(st["offset"]) != float64(third.Offset) {
		t.Fatal(rec)
	}
	if at, ok := jsv.DateParse(st["at"].(string)); !ok || at <= 0 {
		t.Fatal(st["at"])
	}
}

func TestRecordsAMessageThatFailsFirstWithNothingElseToCommit(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	failingOrders(h, 2)
	r := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest"}).
		Map(func(interface{}, streams.EmitCtx) (interface{}, error) { return nil, errors.New("no parser") }).
		To(q.Queue("out")), quiet(fast(streams.FeedRunOptions{QueryID: "first"})))
	<-r.Done()
	rec := h.record("first")
	if num(rec["position"]) != float64(h.b.Messages("orders")[0].Position) || get(rec, "stopped", "reason") != "poison" || get(rec, "stopped", "error") != "no parser" {
		t.Fatal(rec)
	}
}

func TestRecordsALongErrorIsCut(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	failingOrders(h, 1)
	r := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest"}).
		Map(func(interface{}, streams.EmitCtx) (interface{}, error) {
			return nil, errors.New(strings.Repeat("x", 5000))
		}).
		To(q.Queue("out")), quiet(fast(streams.FeedRunOptions{QueryID: "long"})))
	<-r.Done()
	if e := get(h.record("long"), "stopped", "error").(string); len([]rune(e)) > 501 {
		t.Fatal(len(e))
	}
}

func TestRecordsAStopAskedByItsProcessSaysSo(t *testing.T) {
	h := newHarness(t)
	failingOrders(h, 2)
	qa := h.client("a")
	first := h.run(streams.FromFeed(qa, streams.FeedSource{From: "earliest"}).To(qa.Queue("out")), fast(streams.FeedRunOptions{QueryID: "asked"}))
	until(t, func() bool { return h.count("out") >= 2 }, 0, "two")
	stop(t, first)
	if get(h.record("asked"), "stopped", "reason") != "stop" {
		t.Fatal(h.record("asked"))
	}
	qb := h.client("b")
	second := h.run(streams.FromFeed(qb, streams.FeedSource{From: "earliest"}).To(qb.Queue("out")), fast(streams.FeedRunOptions{QueryID: "asked"}))
	failingOrders(h, 1)
	until(t, func() bool { return h.count("out") >= 3 }, 0, "three")
	until(t, func() bool { _, ok := h.record("asked")["stopped"]; return !ok }, 3000, "the new run's commit")
	stop(t, second)
}

func TestRecordsAStreamStoppedBeforeItsFirstReadWritesNoRecord(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	r := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest"}).To(q.Queue("out")), fast(streams.FeedRunOptions{QueryID: "early"}))
	stop(t, r)
	rec := h.record("early")
	if rec != nil && !jsv.IsInteger(rec["position"]) {
		t.Fatal("a position or nothing", rec)
	}
}

func TestRecordsAStandbyThatStopsWritesNothing(t *testing.T) {
	h := newHarness(t)
	failingOrders(h, 1)
	qa := h.client("a")
	active := h.run(streams.FromFeed(qa, streams.FeedSource{From: "earliest"}).To(qa.Queue("out")), fast(streams.FeedRunOptions{QueryID: "pair"}))
	until(t, func() bool { return h.count("out") >= 1 && h.record("pair")["caughtUp"] == true }, 0, "caught up")
	qb := h.client("b")
	standby := h.run(streams.FromFeed(qb, streams.FeedSource{From: "earliest"}).To(qb.Queue("out")), fast(streams.FeedRunOptions{QueryID: "pair"}))
	if standby.Metrics().Role != "standby" {
		t.Fatal(standby.Metrics().Role)
	}
	v := h.version("pair")
	stop(t, standby)
	if h.version("pair") != v {
		t.Fatal("the standby wrote")
	}
	if _, ok := h.record("pair")["stopped"]; ok {
		t.Fatal(h.record("pair"))
	}
	stop(t, active)
	_ = time.Second
}

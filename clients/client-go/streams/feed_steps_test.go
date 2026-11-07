package streams_test

// The behaviours of clients/client-js/test-v2/feed-unit/feedSteps.test.js (as
// of the records change): two positions and a replay, many stateful steps,
// stopping, failures, joins of many sides, gathers, tables, shards, live reads,
// what the code review found, and a crash anywhere.

import (
	"context"
	"errors"
	"math"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/smartpricing/queen/clients/client-go/v2/internal/feedfake"
	"github.com/smartpricing/queen/clients/client-go/v2/internal/jsv"
	"github.com/smartpricing/queen/clients/client-go/v2/streams"
	"github.com/smartpricing/queen/clients/client-go/v2/streams/feedrt"
)

var MIN0 = math.Floor(T0/60000) * 60000

func perMinute(q streams.FeedClient, sink streams.Sink, src *streams.FeedSource) *streams.Stream {
	s := streams.FeedSource{From: "earliest", Queues: []string{"orders"}}
	if src != nil {
		s = *src
	}
	return streams.FromFeed(q, s).WindowTumbling(60).AggregateFields(countAgg()).Name("perMinute").To(sink)
}

func (h *harness) crash(name string) { h.b.Cut(name, true) }

func noAbout(keys []string, id string) []string {
	var out []string
	for _, k := range keys {
		if k != id+"#about" {
			out = append(out, k)
		}
	}
	return out
}

func TestStepsACommitWritesThePositionOnly(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.at(1000)
	for i := 0; i < 3; i++ {
		h.push("orders", "c1", m("n", float64(i)))
	}
	r := h.run(perMinute(q, q.Queue("counts"), nil), fast(streams.FeedRunOptions{QueryID: "pos-only", Now: h.now()}))
	until(t, func() bool { return h.recordPosition("pos-only") > 3 }, 0, "a commit")
	stored := h.record("pos-only")
	if num(get(stored, "snapshot", "chunks")) != 0 || num(get(stored, "snapshot", "position")) != 1 {
		t.Fatal("no chunks; the snapshot is the stream's start", stored["snapshot"])
	}
	if l := noAbout(h.b.KVKeys(NS, "pos-only#"), "pos-only"); len(l) != 0 {
		t.Fatal("nothing but the position record (and the description)", l)
	}
	if r.Metrics().StateKeys != 1 {
		t.Fatal(r.Metrics().StateKeys)
	}
}

func TestStepsAfterACrashTheNextRunnerReadsAgainFromTheSnapshot(t *testing.T) {
	h := newHarness(t)
	qa := h.client("a")
	h.at(1000)
	h.push("orders", "c1", m())
	h.push("orders", "c1", m())
	h.at(61000)
	h.push("orders", "c1", m())
	opts := func() streams.FeedRunOptions {
		return quiet(fast(streams.FeedRunOptions{QueryID: "crash", Now: h.now(), LockTTL: time.Second, SnapshotEveryMillis: streams.Int(3600000)}))
	}
	a := h.run(perMinute(qa, qa.Queue("counts"), nil), opts())
	until(t, func() bool { return h.count("counts") >= 1 && h.recordPosition("crash") > 3 }, 0, "minute 0")
	eq(t, h.payloads("counts"), []interface{}{m("count", 2)}, "minute 0 closed before the crash")
	h.crash("a")
	commitsAtCrash := a.Metrics().Commits
	h.at(62000)
	h.push("orders", "c1", m())
	qb := h.client("b")
	second := h.run(perMinute(qb, qb.Queue("counts"), nil), opts())
	until(t, func() bool { return second.Metrics().Role == "active" }, 6000, "the takeover")
	if second.Metrics().Replays < 1 || second.Metrics().ReplayEntries < 3 {
		t.Fatal("it read the feed again", second.Metrics().Replays, second.Metrics().ReplayEntries)
	}
	h.at(121000)
	h.push("orders", "other", m())
	until(t, func() bool { return h.count("counts") >= 2 }, 0, "minute 1")
	stop(t, second)
	eq(t, h.payloads("counts"), []interface{}{m("count", 2), m("count", 2)}, "minute 0 once, minute 1 whole")
	if a.Metrics().Commits != commitsAtCrash {
		t.Fatal("the crashed process committed nothing more")
	}
}

func layers(v interface{}) []interface{} {
	l, _ := get(v, "snapshot", "layers").([]interface{})
	return l
}

func TestStepsASnapshotIsAFullLayerThenDeltas(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.at(1000)
	for i := 0; i < 20; i++ {
		h.push("orders", "c"+itoa(i), m())
	}
	r := h.run(perMinute(q, q.Queue("counts"), nil), fast(streams.FeedRunOptions{QueryID: "snapdue", Now: h.now(), SnapshotMaxEntries: streams.Int(2)}))
	until(t, func() bool { return r.Metrics().Snapshots >= 1 && r.Metrics().EntriesRead >= 20 }, 0, "a snapshot")
	first := layers(h.record("snapdue"))
	if len(first) != 1 || get(first[0], "full") != true {
		t.Fatal("the first snapshot is a full layer", first)
	}
	for k := 0; k < 2; k++ {
		h.push("orders", "c0", m())
	}
	until(t, func() bool { return len(layers(h.record("snapdue"))) >= 2 }, 0, "a delta")
	second := layers(h.record("snapdue"))
	if get(second[0], "sid") != get(first[0], "sid") || get(second[1], "full") == true {
		t.Fatal("the full layer is still the one under it; a delta", second)
	}
	if num(get(second[1], "bytes")) >= num(get(second[0], "bytes"))/5 {
		t.Fatal("the delta holds the changed key only", second)
	}
	stop(t, r)
	stored := h.record("snapdue")
	named := map[string]bool{}
	for _, l := range layers(stored) {
		named[get(l, "sid").(string)] = true
	}
	keys := h.b.KVKeys(NS, "snapdue#snap#")
	if len(keys) == 0 {
		t.Fatal("chunks")
	}
	for _, k := range keys {
		rest := k[len("snapdue#snap#"):]
		if !named[rest[:strings.LastIndex(rest, "#")]] {
			t.Fatal("only the chunks of named layers are in KV", k)
		}
	}
	qa := h.client("again")
	again := h.run(perMinute(qa, qa.Queue("counts"), nil), fast(streams.FeedRunOptions{QueryID: "snapdue", Now: h.now()}))
	if again.Metrics().StateKeys != 20 {
		t.Fatal("the twenty open windows came back", again.Metrics().StateKeys)
	}
	h.at(61000)
	h.push("orders", "late", m())
	until(t, func() bool { return h.count("counts") >= 20 }, 0, "twenty windows")
	stop(t, again)
	counts := map[string]float64{}
	for _, e := range h.b.Messages("counts") {
		counts[e.Partition] = num(get(e.Payload, "count"))
	}
	for i := 0; i < 20; i++ {
		want := 1.0
		if i == 0 {
			want = 3
		}
		if counts["c"+itoa(i)] != want {
			t.Fatal(counts)
		}
	}
}

func TestStepsAStateThatDidNotChangeMovesTheSnapshotWithThePosition(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.at(1000)
	h.push("orders", "c1", m())
	r := h.run(perMinute(q, q.Queue("counts"), &streams.FeedSource{From: "earliest", Queues: []string{"orders", "noise"}}),
		fast(streams.FeedRunOptions{QueryID: "moved", Now: h.now(), SnapshotMaxEntries: streams.Int(1), PositionCommitMillis: streams.Int(0)}))
	until(t, func() bool { return r.Metrics().Snapshots >= 1 }, 0, "a snapshot")
	sid := get(h.record("moved"), "snapshot", "id")
	snapshots := r.Metrics().Snapshots
	for i := 0; i < 3; i++ {
		h.push("other-queue", "x", m())
	}
	end := h.b.Applied() + 1
	until(t, func() bool { return h.recordPosition("moved") >= float64(end) }, 0, "the position past the noise")
	stored := h.record("moved")
	if get(stored, "snapshot", "id") != sid || num(get(stored, "snapshot", "position")) != num(stored["position"]) || r.Metrics().Snapshots != snapshots {
		t.Fatal(stored["snapshot"], r.Metrics().Snapshots, snapshots)
	}
}

func TestStepsAnEmptyStateIsASnapshotOfNoChunks(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.at(1000)
	h.push("orders", "c1", m())
	r := h.run(perMinute(q, q.Queue("counts"), nil), fast(streams.FeedRunOptions{QueryID: "empty", Now: h.now(), SnapshotMaxEntries: streams.Int(1)}))
	until(t, func() bool { return r.Metrics().Snapshots >= 1 && len(h.b.KVKeys(NS, "empty#snap#")) >= 1 }, 0, "a snapshot")
	h.at(61000)
	until(t, func() bool { return h.count("counts") >= 1 }, 3000, "the window to close on the clock")
	until(t, func() bool {
		return num(get(h.record("empty"), "snapshot", "chunks")) == 0 && len(h.b.KVKeys(NS, "empty#snap#")) == 0
	}, 3000, "no chunks")
	if r.Metrics().StateKeys != 0 {
		t.Fatal(r.Metrics().StateKeys)
	}
}

func TestStepsAStateOfThousandsOfKeysComesBackWholeAfterACrash(t *testing.T) {
	h := newHarness(t)
	qa := h.client("a")
	h.at(1000)
	const N = 1500
	for i := 0; i < N; i += 50 {
		var msgs []feedfake.Msg
		for j := 0; j < 50; j++ {
			msgs = append(msgs, feedfake.Msg{Queue: "orders", Partition: "k" + itoa(i+j), Data: m("pad", strings.Repeat("x", 40))})
		}
		h.b.Push(msgs...)
	}
	a := h.run(perMinute(qa, qa.Queue("counts"), nil), quiet(fast(streams.FeedRunOptions{QueryID: "big", Now: h.now(), LockTTL: time.Second,
		SnapshotMaxEntries: streams.Int(100), SnapshotChunkChars: 2000})))
	until(t, func() bool { return a.Metrics().EntriesRead >= N && a.Metrics().Snapshots >= 1 }, 0, "read and snapshot")
	until(t, func() bool { return num(get(h.record("big"), "snapshot", "chunks")) > 3 }, 3000, "a snapshot of several chunks")
	h.crash("a")
	qb := h.client("b")
	second := h.run(perMinute(qb, qb.Queue("counts"), nil), quiet(fast(streams.FeedRunOptions{QueryID: "big", Now: h.now(), LockTTL: time.Second})))
	until(t, func() bool { return second.Metrics().Role == "active" }, 6000, "the takeover")
	until(t, func() bool { return second.Metrics().StateKeys == N }, 6000, "every key back")
	h.at(61000)
	h.push("orders", "flush", m())
	until(t, func() bool { return h.count("counts") >= N }, 6000, "every window")
	stop(t, second)
	counts := h.b.Messages("counts")
	parts := map[string]bool{}
	for _, e := range counts {
		parts[e.Partition] = true
		if num(get(e.Payload, "count")) != 1 {
			t.Fatal(e.Payload)
		}
	}
	if len(counts) != N || len(parts) != N {
		t.Fatal(len(counts), len(parts))
	}
}

func TestStepsARecordOfTheBucketFormatIsReadOnce(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.at(1000)
	pos := h.push("orders", "c1", m())
	c, err := perMinute(q, q.Queue("counts"), nil).CompileFeed()
	if err != nil {
		t.Fatal(err)
	}
	sk := "w\u001f" + jsv.NumberString(MIN0) + "\u001fc1"
	h.b.KVPut(NS, "legacy", m("position", pos+1, "queryId", "legacy", "epoch", 0, "buckets", 2, "hash", c.ChainHash, "clock", T0+1000))
	h.b.KVPut(NS, "legacy#0", m("e", m(sk, m("key", "c1", "start", MIN0, "end", MIN0+60000, "due", MIN0+60000, "acc", m("count", 5)))))
	h.b.KVPut(NS, "legacy#1", m("e", m()))
	r := h.run(perMinute(q, q.Queue("counts"), nil), fast(streams.FeedRunOptions{QueryID: "legacy", Now: h.now()}))
	h.at(2000)
	h.push("orders", "c1", m())
	until(t, func() bool { return num(get(h.record("legacy"), "snapshot", "chunks")) >= 1 }, 0, "a snapshot")
	until(t, func() bool {
		for _, k := range h.b.KVKeys(NS, "legacy#") {
			if !strings.HasPrefix(k, "legacy#snap#") && k != "legacy#about" {
				return false
			}
		}
		return true
	}, 2000, "the buckets to go")
	h.at(61000)
	h.push("orders", "c2", m())
	until(t, func() bool { return h.count("counts") >= 1 }, 0, "the window")
	stop(t, r)
	eq(t, h.payloads("counts")[0], m("count", 6), "the 5 from the buckets and the new one")
}

func TestStepsSnapshotsEncodeAndDecode(t *testing.T) {
	dump := []feedrt.StepDump{{Name: "s", Kind: "window", Entries: []feedrt.KV{{K: "a", V: m("x", strings.Repeat("é", 3000))}, {K: "b", V: []interface{}{1.0, 2.0, nil}}}}}
	chunks, bytes := feedrt.EncodeSnapshot(dump, 1024)
	if len(chunks) < 1 || bytes <= 3000 {
		t.Fatal(len(chunks), bytes)
	}
	back, err := feedrt.DecodeSnapshot(chunks)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, []interface{}{back[0].Entries[0].K, back[0].Entries[0].V, back[0].Entries[1].V}, []interface{}{"a", m("x", strings.Repeat("é", 3000)), []interface{}{1, 2, nil}}, "decoded")
}

func TestStepsAWindowsResultsFeedASecondWindow(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.at(1000)
	h.push("orders", "c1", m())
	h.push("orders", "c1", m())
	h.push("orders", "c2", m())
	h.at(61000)
	h.push("orders", "c1", m())
	r := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"orders"}}).
		WindowTumbling(60).AggregateFields(countAgg()).Name("perKey").
		KeyBy(func(interface{}) (string, error) { return "all", nil }).
		WindowTumbling(120).AggregateFields(field("total", "count"), streams.AggregateField{Name: "keys", Fn: one}).Name("total").
		To(q.Queue("totals")), fast(streams.FeedRunOptions{QueryID: "two-windows", Now: h.now()}))
	h.at(121000)
	h.push("orders", "c3", m())
	until(t, func() bool { return h.count("totals") >= 1 }, 0, "the total")
	stop(t, r)
	eq(t, h.payloads("totals"), []interface{}{m("total", 3, "keys", 2)}, "totals")
	if h.b.Messages("totals")[0].Partition != "all" {
		t.Fatal(h.b.Messages("totals")[0].Partition)
	}
	var names []string
	for _, s := range r.Metrics().Steps {
		names = append(names, s.Name)
	}
	eq(t, names, []string{"perKey", "total"}, "step names")
}

func queueIs(name string) func(interface{}) (bool, error) {
	return func(msg interface{}) (bool, error) { return get(msg, "queue") == name, nil }
}

func TestStepsAPatternsMatchesFeedAWindowAndAWindowsResultsFeedAPattern(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	for i, x := range []struct{ q, p string }{{"orders", "o1"}, {"payments", "o1"}, {"orders", "o2"}, {"payments", "o2"}, {"orders", "o3"}} {
		h.at(float64(1000 * (i + 1)))
		h.push(x.q, x.p, m())
	}
	paid := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"orders", "payments"}}).
		Pattern(streams.PatternOptions{Start: queueIs("orders"), Then: queueIs("payments"), Within: "15m",
			OnMatch: func(o, _ interface{}) (interface{}, error) { return m("order", get(o, "partition")), nil }}).
		KeyBy(func(interface{}) (string, error) { return "all", nil }).
		WindowTumbling(60).AggregateFields(streams.AggregateField{Name: "paid", Fn: one}).
		To(q.Queue("paid-per-minute")), fast(streams.FeedRunOptions{QueryID: "pattern-window", Now: h.now()}))
	bursts := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"clicks"}}).
		WindowTumbling(60).AggregateFields(countAgg()).
		Pattern(streams.PatternOptions{
			Start:  func(msg interface{}) (bool, error) { return num(get(msg, "data", "count")) >= 2, nil },
			Then:   func(msg interface{}) (bool, error) { return num(get(msg, "data", "count")) < 2, nil },
			Within: "5m",
			OnMatch: func(s, c interface{}) (interface{}, error) {
				return m("tenant", get(s, "partition"), "from", get(s, "data", "count"), "to", get(c, "data", "count")), nil
			},
		}).To(q.Queue("calmed")), fast(streams.FeedRunOptions{QueryID: "window-pattern", Now: h.now()}))
	h.at(6000)
	h.push("clicks", "t1", m())
	h.at(7000)
	h.push("clicks", "t1", m())
	h.at(61000)
	h.push("clicks", "t1", m())
	h.at(121000)
	h.push("clicks", "x", m())
	until(t, func() bool { return h.count("paid-per-minute") >= 1 && h.count("calmed") >= 1 }, 0, "both")
	stop(t, paid)
	stop(t, bursts)
	eq(t, h.payloads("paid-per-minute"), []interface{}{m("paid", 2)}, "paid")
	eq(t, h.payloads("calmed"), []interface{}{m("tenant", "t1", "from", 2, "to", 1)}, "calmed")
}

func TestStepsATableAThreeSidedJoinAndAWindowReplayAfterACrash(t *testing.T) {
	chain := func(q streams.FeedClient, sink streams.Sink) *streams.Stream {
		return streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"orders", "payments", "shipments"}}).
			JoinTable(streams.TableOptions{Queue: "carriers",
				On: func(msg, _ interface{}) (interface{}, error) { return get(msg, "data", "carrier"), nil },
				Select: func(v, row, _ interface{}) (interface{}, error) {
					out := jsv.Clone(v).(map[string]interface{})
					speed := get(row, "speed")
					if jsv.IsNullish(speed) {
						speed = "unknown"
					}
					out["speed"] = speed
					return out, nil
				}}).
			JoinSides(streams.JoinSidesOptions{
				Sides:  []streams.Part{{Name: "order", Queue: "orders"}, {Name: "paid", Queue: "payments"}, {Name: "sent", Queue: "shipments"}},
				Within: "10m",
				// A join keeps the message's payload; keep the table-joined value instead.
				Keep: func(_, value interface{}) (interface{}, error) { return value, nil },
				Select: func(got interface{}, key string) (interface{}, error) {
					return m("order", key, "speed", get(got, "sent", "data", "speed")), nil
				},
			}).
			KeyBy(func(interface{}) (string, error) { return "all", nil }).
			WindowTumbling(60).AggregateFields(streams.AggregateField{Name: "done", Fn: one},
			streams.AggregateField{Name: "fast", Fn: func(v interface{}) (interface{}, error) {
				if get(v, "speed") == "fast" {
					return 1.0, nil
				}
				return 0.0, nil
			}}).To(sink)
	}
	load := func(h *harness) {
		h.at(500)
		h.push("carriers", "dhl", m("speed", "fast"))
		for i := 0; i < 6; i++ {
			h.at(float64(1000 + i*100))
			carrier := "ups"
			if i%2 == 1 {
				carrier = "dhl"
			}
			h.push("shipments", "o"+itoa(i), m("carrier", carrier))
			h.push("orders", "o"+itoa(i), m())
			if i != 5 {
				h.push("payments", "o"+itoa(i), m())
			}
		}
	}
	ref := newHarness(t)
	load(ref)
	rq := ref.client("queen")
	r := ref.run(chain(rq, rq.Queue("done-per-minute")), fast(streams.FeedRunOptions{QueryID: "steps", Now: ref.now()}))
	ref.at(61000)
	ref.push("orders", "flush", m())
	until(t, func() bool { return ref.count("done-per-minute") >= 1 }, 0, "the reference")
	stop(t, r)
	eq(t, ref.payloads("done-per-minute"), []interface{}{m("done", 5, "fast", 2)}, "reference")
	h := newHarness(t)
	load(h)
	qa := h.client("a")
	a := h.run(chain(qa, qa.Queue("done-per-minute")), quiet(fast(streams.FeedRunOptions{QueryID: "steps", Now: h.now(), LockTTL: time.Second, MaxRecords: 4})))
	until(t, func() bool { return a.Metrics().EntriesRead >= 8 }, 0, "eight read")
	h.crash("a")
	qb := h.client("b")
	second := h.run(chain(qb, qb.Queue("done-per-minute")), quiet(fast(streams.FeedRunOptions{QueryID: "steps", Now: h.now(), LockTTL: time.Second})))
	until(t, func() bool { return second.Metrics().Role == "active" }, 6000, "the takeover")
	h.at(61000)
	h.push("orders", "flush", m())
	until(t, func() bool { return h.count("done-per-minute") >= 1 }, 0, "the result")
	wait(50)
	stop(t, second)
	eq(t, h.payloads("done-per-minute"), ref.payloads("done-per-minute"), "the same results")
}

func TestStepsStopCancelsALongPollAndStillCommits(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.at(1000)
	for i := 0; i < 3; i++ {
		h.push("orders", "c"+itoa(i), m())
	}
	r := h.run(perMinute(q, q.Queue("counts"), nil), fast(streams.FeedRunOptions{QueryID: "prompt-stop", Now: h.now(), WaitMillis: streams.Int(20000), CommitEveryMillis: streams.Int(60000)}))
	until(t, func() bool { return r.Metrics().EntriesRead >= 3 }, 0, "three read")
	wait(50)
	t0 := time.Now()
	stop(t, r)
	if time.Since(t0) > time.Second {
		t.Fatal("stopped after the poll", time.Since(t0))
	}
	stored := h.record("prompt-stop")
	if num(get(stored, "snapshot", "chunks")) < 1 || num(stored["position"]) <= 3 {
		t.Fatal(stored)
	}
}

func TestStepsALaterStepWhoseFunctionKeepsFailingOnAResultSkipsIt(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.at(1000)
	for _, p := range []string{"c1", "c2", "c2"} {
		h.push("orders", p, m())
	}
	r := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"orders"}}).
		WindowTumbling(60).AggregateFields(countAgg()).
		KeyBy(func(v interface{}) (string, error) {
			if num(get(v, "count")) == 1 {
				return "", errors.New("a single order is refused")
			}
			return "all", nil
		}).
		WindowTumbling(120).AggregateFields(field("sum", "count")).To(q.Queue("out")),
		quiet(fast(streams.FeedRunOptions{QueryID: "poison-step", Now: h.now(), OnPoison: "skip"})))
	h.at(61000)
	h.push("orders", "x", m())
	h.at(121000)
	h.push("orders", "x", m())
	until(t, func() bool { return h.count("out") >= 1 }, 0, "the sum")
	stop(t, r)
	eq(t, h.payloads("out"), []interface{}{m("sum", 2)}, "c1 (one order) failed and was skipped; c2 counted")
	if r.Metrics().Skipped < 1 {
		t.Fatal(r.Metrics().Skipped)
	}
}

func tableSelect(v, row, _ interface{}) (interface{}, error) {
	var currency interface{}
	if row != nil {
		currency = get(row, "currency")
		if jsv.IsUndefined(currency) {
			currency = nil
		}
	}
	return m("id", get(v, "id"), "currency", currency), nil
}

func onProperty(msg, _ interface{}) (interface{}, error) { return get(msg, "data", "property"), nil }

func TestStepsATableWithInitialNowStartsEmpty(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.push("settings", "p1", m("currency", "EUR"))
	r := h.run(streams.FromFeed(q, streams.FeedSource{Queues: []string{"bookings"}}).
		JoinTable(streams.TableOptions{Queue: "settings", Initial: "now", On: onProperty, Select: tableSelect}).
		To(q.Queue("out")), fast(streams.FeedRunOptions{QueryID: "table-now"}))
	h.push("bookings", "x", m("id", 1.0, "property", "p1"))
	h.push("settings", "p1", m("currency", "CHF"))
	h.push("bookings", "x", m("id", 2.0, "property", "p1"))
	until(t, func() bool { return h.count("out") >= 2 }, 0, "two")
	stop(t, r)
	eq(t, h.payloads("out"), []interface{}{m("id", 1, "currency", nil), m("id", 2, "currency", "CHF")}, "out")
}

func TestStepsNamesStepsAndRefusesBadNames(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	s := streams.FromFeed(q, streams.FeedSource{}).WindowTumbling(60).AggregateFields(countAgg()).Name("a")
	c, err := s.CompileFeed()
	if err != nil || c.Steps[0].Name != "a" {
		t.Fatal(c, err)
	}
	if _, err := streams.FromFeed(q, streams.FeedSource{}).Name("a").CompileFeed(); err == nil || !strings.Contains(err.Error(), "stateful step right before") {
		t.Fatal(err)
	}
	if _, err := s.Map(func(v interface{}, _ streams.EmitCtx) (interface{}, error) { return v, nil }).Name("b").CompileFeed(); err == nil || !strings.Contains(err.Error(), "stateful step right before") {
		t.Fatal(err)
	}
	if _, err := s.WindowTumbling(60).AggregateFields(streams.AggregateField{Name: "n", Fn: one}).Name("a").To(q.Queue("x")).CompileFeed(); err == nil || !strings.Contains(err.Error(), `named "a"`) {
		t.Fatal(err)
	}
	if _, err := streams.FromFeed(q, streams.FeedSource{}).Name("bad name").CompileFeed(); err == nil || !strings.Contains(err.Error(), "a step name") {
		t.Fatal(err)
	}
	_, err = streams.From(fakeSource{name: "orders"}).Gather(streams.GatherOptions{Steps: streams.Queues("a"), Within: "1m"}).To(q.Queue("x")).Run(context.Background(), streams.RunOptions{QueryID: "qs"})
	if err == nil || !strings.Contains(err.Error(), "Stream.fromFeed only") {
		t.Fatal(err)
	}
}

func TestStepsAThreeSidedJoinNamesTheMissingSide(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	r := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest"}).
		JoinSides(streams.JoinSidesOptions{
			Sides:  streams.Queues("orders", "payments", "shipments"),
			Within: "15m",
			Select: func(got interface{}, key string) (interface{}, error) {
				var keys []string
				for k := range got.(map[string]interface{}) {
					keys = append(keys, k)
				}
				sort.Strings(keys)
				return m("order", key, "sides", keys), nil
			},
			OnTimeout: func(_, missing interface{}, key string) (interface{}, error) {
				return m("order", key, "missing", missing), nil
			},
		}).To(q.Queue("out")), fast(streams.FeedRunOptions{QueryID: "join3", Now: h.now()}))
	h.at(1000)
	h.push("shipments", "o1", m())
	h.at(2000)
	h.push("payments", "o1", m())
	h.at(3000)
	h.push("orders", "o2", m())
	h.at(4000)
	h.push("orders", "o1", m())
	until(t, func() bool { return h.count("out") >= 1 }, 0, "the set")
	eq(t, h.payloads("out"), []interface{}{m("order", "o1", "sides", []string{"orders", "payments", "shipments"})}, "complete")
	h.at(3000 + 15*60000 + 1)
	until(t, func() bool { return h.count("out") >= 2 }, 3000, "the deadline of o2")
	stop(t, r)
	eq(t, h.payloads("out")[1], m("order", "o2", "missing", []string{"payments", "shipments"}), "missing")
}

func TestStepsAGatherOfNamedStepsCompletesOnce(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	r := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest"}).
		Gather(streams.GatherOptions{
			Steps:      []streams.Part{{Name: "paid", Test: func(msg, _ interface{}) (bool, error) { return get(msg, "queue") == "payments", nil }}, {Name: "stocked", Test: func(msg, _ interface{}) (bool, error) { return get(msg, "queue") == "stock", nil }}},
			Key:        func(msg, _ interface{}) (interface{}, error) { return get(msg, "data", "order"), nil },
			Within:     "10m",
			OnComplete: func(_ interface{}, key string) (interface{}, error) { return m("order", key, "ready", true), nil },
			OnTimeout: func(_, missing interface{}, key string) (interface{}, error) {
				return m("order", key, "missing", missing), nil
			},
		}).Name("runs").To(q.Queue("out")), fast(streams.FeedRunOptions{QueryID: "gather-steps", Now: h.now()}))
	h.at(1000)
	h.push("payments", "p", m("order", "A"))
	h.at(2000)
	h.push("stock", "s", m("order", "A"))
	until(t, func() bool { return h.count("out") >= 1 }, 0, "A")
	if get(r.State("runs").Get("A"), "value", "done") != true {
		t.Fatal("remembered until its deadline", r.State("runs").Get("A"))
	}
	h.at(3000)
	h.push("payments", "p", m("order", "A"))
	h.at(1000 + 10*60000 + 1000)
	wait(150)
	h.push("stock", "s", m("order", "B"))
	h.at(1000 + 10*60000 + 2000 + 10*60000)
	until(t, func() bool { return h.count("out") >= 2 }, 3000, "the deadline of B")
	stop(t, r)
	eq(t, h.payloads("out"), []interface{}{m("order", "A", "ready", true), m("order", "B", "missing", []string{"paid"})}, "out")
}

func TestStepsAGatherByCountWaitsForDistinctParts(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	r := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"parts"}}).
		Gather(streams.GatherOptions{
			Count:  func(msg, _ interface{}) (interface{}, error) { return get(msg, "data", "of"), nil },
			Key:    func(msg, _ interface{}) (interface{}, error) { return get(msg, "data", "batch"), nil },
			Within: "5m",
			OnComplete: func(got interface{}, key string) (interface{}, error) {
				var ns []float64
				for _, p := range got.([]interface{}) {
					ns = append(ns, num(get(p, "data", "n")))
				}
				sort.Float64s(ns)
				return m("batch", key, "parts", ns), nil
			},
			OnTimeout: func(_, missing interface{}, key string) (interface{}, error) {
				return m("batch", key, "missing", missing), nil
			},
		}).To(q.Queue("out")), fast(streams.FeedRunOptions{QueryID: "gather-count", Now: h.now()}))
	h.at(1000)
	for _, x := range []struct {
		p, b, tx string
		of, n    float64
	}{{"x", "b1", "b1-1", 3, 1}, {"x", "b1", "b1-1", 3, 1}, {"y", "b1", "b1-2", 3, 2}, {"x", "b2", "b2-1", 2, 1}, {"z", "b1", "b1-3", 3, 3}} {
		h.b.Push(feedfake.Msg{Queue: "parts", Partition: x.p, Data: m("batch", x.b, "of", x.of, "n", x.n), TransactionID: x.tx})
	}
	until(t, func() bool { return h.count("out") >= 1 }, 0, "b1")
	eq(t, h.payloads("out")[0], m("batch", "b1", "parts", []float64{1, 2, 3}), "the duplicate counted once")
	h.at(1000 + 5*60000 + 1)
	until(t, func() bool { return h.count("out") >= 2 }, 3000, "b2")
	stop(t, r)
	eq(t, h.payloads("out")[1], m("batch", "b2", "missing", 1), "missing")
}

func TestStepsRefusesAJoinOfOneSideAndAGatherWithBothStepsAndCount(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	if _, err := streams.FromFeed(q, streams.FeedSource{}).JoinSides(streams.JoinSidesOptions{Sides: streams.Queues("a"), Within: "1m"}).CompileFeed(); err == nil || !strings.Contains(err.Error(), "at least two") {
		t.Fatal(err)
	}
	if _, err := streams.FromFeed(q, streams.FeedSource{}).Gather(streams.GatherOptions{Steps: streams.Queues("a"), Count: func(interface{}, interface{}) (interface{}, error) { return 1.0, nil }, Within: "1m"}).CompileFeed(); err == nil || !strings.Contains(err.Error(), "one of the two") {
		t.Fatal(err)
	}
	if _, err := streams.FromFeed(q, streams.FeedSource{}).Gather(streams.GatherOptions{Within: "1m"}).CompileFeed(); err == nil || !strings.Contains(err.Error(), "one of the two") {
		t.Fatal(err)
	}
}

func bookings(q streams.FeedClient, sink streams.Sink, from interface{}, missing string) *streams.Stream {
	if from == nil {
		from = "earliest"
	}
	return streams.FromFeed(q, streams.FeedSource{From: from, Queues: []string{"bookings"}}).
		JoinTable(streams.TableOptions{Queue: "settings", On: onProperty, Missing: missing, Select: tableSelect}).To(sink)
}

func TestStepsAMessageSeesTheTableAsOfItsOwnPosition(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.push("settings", "p1", m("currency", "EUR"))
	h.push("bookings", "x", m("id", 1.0, "property", "p1"))
	h.push("settings", "p1", m("currency", "CHF"))
	h.push("bookings", "x", m("id", 2.0, "property", "p1"))
	// A change and a booking in one transaction: the change counts first.
	h.b.Push(feedfake.Msg{Queue: "bookings", Partition: "x", Data: m("id", 3.0, "property", "p1")}, feedfake.Msg{Queue: "settings", Partition: "p1", Data: m("currency", "GBP")})
	h.push("settings", "p1", nil)
	h.push("bookings", "x", m("id", 4.0, "property", "p1"))
	h.push("bookings", "x", m("id", 5.0, "property", "p2"))
	r := h.run(bookings(q, q.Queue("out"), nil, ""), fast(streams.FeedRunOptions{QueryID: "table"}))
	until(t, func() bool { return h.count("out") >= 5 }, 0, "five")
	stop(t, r)
	eq(t, h.payloads("out"), []interface{}{m("id", 1, "currency", "EUR"), m("id", 2, "currency", "CHF"), m("id", 3, "currency", "GBP"),
		m("id", 4, "currency", nil), m("id", 5, "currency", nil)}, "out")
}

func TestStepsANewStreamFirstReadsTheTableQueueAndMissingDropDrops(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.push("settings", "p1", m("currency", "EUR"))
	h.push("bookings", "x", m("id", 0.0, "property", "p1"))
	r := h.run(bookings(q, q.Queue("out"), "now", "drop"), fast(streams.FeedRunOptions{QueryID: "prelude"}))
	h.push("bookings", "x", m("id", 1.0, "property", "p1"))
	h.push("bookings", "x", m("id", 2.0, "property", "nope"))
	h.push("bookings", "x", m("id", 3.0, "property", "p1"))
	until(t, func() bool { return h.count("out") >= 2 }, 0, "two")
	wait(50)
	stop(t, r)
	eq(t, h.payloads("out"), []interface{}{m("id", 1, "currency", "EUR"), m("id", 3, "currency", "EUR")}, "a row from before the start; the unknown dropped")
	if r.Metrics().TableRows < 1 {
		t.Fatal(r.Metrics().TableRows)
	}
}

func TestStepsAWhereOfTheStreamDoesNotFilterTheTablesRows(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.push("settings", "p1", m("currency", "EUR"))
	h.push("bookings", "x", m("id", 1.0, "property", "p1", "vip", true))
	h.push("bookings", "x", m("id", 2.0, "property", "p1", "vip", false))
	chain := func(q streams.FeedClient, sink streams.Sink) *streams.Stream {
		return streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"bookings"}}).
			Where("data.vip", "=", true).
			JoinTable(streams.TableOptions{Queue: "settings", On: onProperty, Select: tableSelect}).To(sink)
	}
	first := h.run(chain(q, q.Queue("out")), fast(streams.FeedRunOptions{QueryID: "table-where"}))
	until(t, func() bool { return h.count("out") >= 1 }, 0, "one")
	stop(t, first)
	h.push("bookings", "x", m("id", 3.0, "property", "p1", "vip", true))
	q2 := h.client("second")
	second := h.run(chain(q2, q2.Queue("out")), fast(streams.FeedRunOptions{QueryID: "table-where"}))
	until(t, func() bool { return h.count("out") >= 2 }, 0, "two")
	stop(t, second)
	eq(t, h.payloads("out"), []interface{}{m("id", 1, "currency", "EUR"), m("id", 3, "currency", "EUR")}, "out")
	for _, r := range h.b.FeedReads() {
		for _, c := range asList(r["where"]) {
			if get(c, "field") == "data.vip" {
				t.Fatal("the where ran here, not on the broker")
			}
		}
	}
}

func asList(v interface{}) []interface{} {
	l, _ := v.([]interface{})
	return l
}

func TestStepsShardsEveryPartitionIsReadByOneShard(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	for i := 0; i < 30; i++ {
		for j := 0; j < 2; j++ {
			h.push("orders", "c"+itoa(i), m("i", float64(i), "j", float64(j)))
		}
	}
	g := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"orders"}}).
		Map(func(msg interface{}, _ streams.EmitCtx) (interface{}, error) { return jsv.Clone(data(msg)), nil }).
		To(q.Queue("copy")), fast(streams.FeedRunOptions{QueryID: "sharded", Shards: 3}))
	until(t, func() bool { return h.count("copy") >= 60 }, 0, "sixty")
	wait(50)
	stop(t, g)
	cp := h.b.Messages("copy")
	ids := map[string]bool{}
	for _, e := range cp {
		ids[e.TransactionID] = true
		if !strings.HasPrefix(e.TransactionID, feedrt.OutputPrefix("sharded")) {
			t.Fatal("one prefix for every shard")
		}
		if feedrt.ShardOf(e.Partition, 3) != feedrt.ShardOf("c"+jsv.NumberString(num(get(e.Payload, "i"))), 3) {
			t.Fatal("each shard took the partitions the hash gives it")
		}
	}
	if len(cp) != 60 || len(ids) != 60 {
		t.Fatal(len(cp), len(ids))
	}
	for s := 0; s < 3; s++ {
		if h.recordPosition("sharded#shard"+itoa(s)+"of3") <= 1 {
			t.Fatal("shard has a position", s)
		}
		found := false
		for _, r := range h.b.FeedReads() {
			if sh, ok := r["shard"].(map[string]interface{}); ok && num(sh["count"]) == 3 && num(sh["index"]) == float64(s) {
				found = true
			}
		}
		if !found {
			t.Fatal("shard reads its slice", s)
		}
	}
	if num(get(h.record("sharded#layout"), "shards")) != 3 {
		t.Fatal(h.record("sharded#layout"))
	}
	mt := g.Metrics()
	if mt.Shards != 3 || mt.Outputs != 60 {
		t.Fatal(mt.Shards, mt.Outputs)
	}
}

func TestStepsShardByAKeyWindowsPerTenant(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.at(1000)
	for i := 0; i < 40; i++ {
		h.push("clicks", "p"+itoa(i%7), m("tenant", "t"+itoa(i%5)))
	}
	g := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"clicks"}}).
		KeyBy(func(msg interface{}) (string, error) { return jsv.String(get(msg, "data", "tenant")), nil }).
		WindowTumbling(60).AggregateFields(countAgg()).
		Map(func(v interface{}, ctx streams.EmitCtx) (interface{}, error) {
			return m("tenant", ctx["key"], "count", get(v, "count")), nil
		}).
		To(q.Queue("per-tenant")), fast(streams.FeedRunOptions{QueryID: "by-tenant", Shards: 4, Now: h.now(),
		ShardBy: func(msg interface{}) (interface{}, error) { return get(msg, "data", "tenant"), nil }}))
	h.at(61000)
	h.push("clicks", "p0", m("tenant", "flush"))
	until(t, func() bool { return h.count("per-tenant") >= 5 }, 0, "five tenants")
	stop(t, g)
	out := h.payloads("per-tenant")
	sort.Slice(out, func(a, b int) bool { return get(out[a], "tenant").(string) < get(out[b], "tenant").(string) })
	var want []interface{}
	for i := 0; i < 5; i++ {
		want = append(want, m("tenant", "t"+itoa(i), "count", 8))
	}
	eq(t, out, want, "per tenant")
	for _, r := range h.b.FeedReads() {
		if r["shard"] != nil {
			t.Fatal("a key shard is filtered here")
		}
	}
}

func TestStepsADifferentShardCountIsRefused(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.push("orders", "c1", m())
	chain := func() *streams.Stream {
		return streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"orders"}}).To(q.Queue("copy"))
	}
	g := h.run(chain(), fast(streams.FeedRunOptions{QueryID: "layout", Shards: 3}))
	until(t, func() bool { return h.count("copy") >= 1 }, 0, "one")
	stop(t, g)
	if err := h.runErr(chain(), fast(streams.FeedRunOptions{QueryID: "layout", Shards: 2})); feedrt.ErrorCode(err) != "feed_layout_changed" {
		t.Fatal(err)
	}
	if err := h.runErr(chain(), fast(streams.FeedRunOptions{QueryID: "layout"})); feedrt.ErrorCode(err) != "feed_layout_changed" {
		t.Fatal(err)
	}
	again := h.run(chain(), fast(streams.FeedRunOptions{QueryID: "layout", Shards: 2, ResetToken: "two"}))
	stop(t, again)
	if num(get(h.record("layout#layout"), "shards")) != 2 {
		t.Fatal(h.record("layout#layout"))
	}
}

func TestStepsASecondProcessWaitsOnEveryShard(t *testing.T) {
	h := newHarness(t)
	for i := 0; i < 12; i++ {
		h.push("orders", "c"+itoa(i), m("n", float64(i)))
	}
	chain := func(q streams.FeedClient, sink streams.Sink) *streams.Stream {
		return streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"orders"}}).To(sink)
	}
	qa, qb := h.client("a"), h.client("b")
	a := h.run(chain(qa, qa.Queue("copy")), fast(streams.FeedRunOptions{QueryID: "standby-shards", Shards: 3}))
	bb := h.run(chain(qb, qb.Queue("copy")), fast(streams.FeedRunOptions{QueryID: "standby-shards", Shards: 3}))
	until(t, func() bool { return h.count("copy") >= 12 }, 0, "twelve")
	if bb.Metrics().Running != 0 {
		t.Fatal("all standby")
	}
	stop(t, a)
	for i := 12; i < 24; i++ {
		h.push("orders", "c"+itoa(i), m("n", float64(i)))
	}
	until(t, func() bool { return h.count("copy") >= 24 }, 6000, "24")
	stop(t, bb)
	got := ns(h, "copy")
	sort.Float64s(got)
	for i := range got {
		if got[i] != float64(i) {
			t.Fatal(got)
		}
	}
}

func TestStepsEveryShardSeesTheWholeTable(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.push("settings", "p1", m("currency", "EUR"))
	for i := 0; i < 20; i++ {
		h.push("bookings", "b"+itoa(i), m("id", float64(i), "property", "p1"))
	}
	g := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"bookings"}}).
		JoinTable(streams.TableOptions{Queue: "settings", On: onProperty, Select: tableSelect}).
		To(q.Queue("out")), fast(streams.FeedRunOptions{QueryID: "shard-table", Shards: 3}))
	until(t, func() bool { return h.count("out") >= 20 }, 0, "twenty")
	stop(t, g)
	for _, p := range h.payloads("out") {
		if get(p, "currency") != "EUR" {
			t.Fatal(p)
		}
	}
	found := false
	for _, r := range h.b.FeedReads() {
		if sh, ok := r["shard"].(map[string]interface{}); ok && string(jsv.MustMarshal(sh["queues"])) == `["bookings"]` {
			found = true
		}
	}
	if !found {
		t.Fatal("the shard splits the stream queue only")
	}
}

func TestStepsLiveReadsInTheProcess(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.at(1000)
	for _, p := range []string{"c1", "c1", "c2"} {
		h.push("orders", p, m())
	}
	r := h.run(perMinute(q, q.Queue("counts"), nil), fast(streams.FeedRunOptions{QueryID: "live", Now: h.now()}))
	until(t, func() bool { return r.Metrics().EntriesRead >= 3 }, 0, "three")
	one := r.State("perMinute").Get("c1")
	if one["found"] != true {
		t.Fatal(one)
	}
	var vals []interface{}
	for _, w := range get(one, "value", "windows").([]interface{}) {
		vals = append(vals, get(w, "value"))
	}
	eq(t, vals, []interface{}{m("count", 2)}, "windows")
	if len(r.State("perMinute").List(0)["items"].([]interface{})) != 2 || r.State("perMinute").Get("nope")["found"] != false {
		t.Fatal(r.State("perMinute").List(0))
	}
	if e, _ := r.State("nope").Get("c1")["error"].(string); !strings.Contains(e, "no step named") {
		t.Fatal(e)
	}
}

func TestStepsLiveReadsFromOutside(t *testing.T) {
	h := newHarness(t)
	q := h.client("runner")
	h.at(1000)
	for i := 0; i < 9; i++ {
		h.push("orders", "c"+itoa(i%3), m())
	}
	r := h.run(perMinute(q, q.Queue("counts"), nil), fast(streams.FeedRunOptions{QueryID: "view-one", Now: h.now(), ServeWaitMillis: 100}))
	until(t, func() bool { return r.Metrics().EntriesRead >= 9 }, 0, "nine")
	asker := h.client("asker")
	ctx := context.Background()
	view, err := streams.View(asker, "view-one", streams.ViewOptions{TimeoutMillis: 2000})
	if err != nil {
		t.Fatal(err)
	}
	got, err := view.Get(ctx, "perMinute", "c1")
	if err != nil || got["found"] != true || num(get(got["value"].(map[string]interface{})["windows"].([]interface{})[0], "value", "count")) != 3 {
		t.Fatal(got, err)
	}
	l, err := view.List(ctx, "perMinute", 0)
	if err != nil || len(l["items"].([]interface{})) != 3 {
		t.Fatal(l, err)
	}
	st, err := view.Status(ctx)
	if err != nil || st[0]["role"] != "active" || num(get(st[0], "metrics", "entriesRead")) < 9 {
		t.Fatal(st, err)
	}
	g := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"orders"}}).
		WindowTumbling(60).AggregateFields(countAgg()).Name("perMinute").
		To(q.Queue("counts2")), fast(streams.FeedRunOptions{QueryID: "view-shards", Shards: 3, Now: h.now(), ServeWaitMillis: 100}))
	until(t, func() bool { return g.Metrics().EntriesRead >= 9 }, 0, "nine")
	sharded, _ := streams.View(asker, "view-shards", streams.ViewOptions{TimeoutMillis: 2000})
	for _, k := range []string{"c0", "c1", "c2"} {
		r, err := sharded.Get(ctx, "perMinute", k)
		if err != nil || r["found"] != true || num(r["shard"]) != float64(feedrt.ShardOf(k, 3)) {
			t.Fatal(k, r, err)
		}
	}
	if l, _ := sharded.List(ctx, "perMinute", 0); len(l["items"].([]interface{})) != 3 {
		t.Fatal(l)
	}
	if st, _ := sharded.Status(ctx); len(st) != 3 {
		t.Fatal(st)
	}
}

func TestStepsAViewOfAStreamNobodyRunsTimesOut(t *testing.T) {
	h := newHarness(t)
	view, _ := streams.View(h.client("asker"), "nobody", streams.ViewOptions{TimeoutMillis: 150})
	if _, err := view.Get(context.Background(), "s", "k"); feedrt.ErrorCode(err) != "feed_view_timeout" {
		t.Fatal(err)
	}
}

func TestStepsReviewALaterStepsDeadlineThatPassedOnATickIsClosedFirst(t *testing.T) {
	chain := func(q streams.FeedClient, sink streams.Sink) *streams.Stream {
		key := func(k string) func(interface{}) (bool, error) {
			return func(msg interface{}) (bool, error) { return get(msg, "ctx", "key") == k, nil }
		}
		return streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"events"}}).
			WindowTumbling(10).AggregateFields(countAgg()).
			KeyBy(func(interface{}) (string, error) { return "all", nil }).
			Join(streams.JoinOptions{Left: key("k1"), Right: key("k2"), Within: "5s",
				OnTimeout: func(msg interface{}, side string) (interface{}, error) {
					return m("side", side, "key", get(msg, "ctx", "key")), nil
				}}).
			To(sink)
	}
	scenario := func(crashIt bool) []interface{} {
		h := newHarness(t)
		q := h.client("a")
		opts := func() streams.FeedRunOptions {
			return quiet(fast(streams.FeedRunOptions{QueryID: "xstep", Now: h.now(), LockTTL: time.Second, SnapshotEveryMillis: streams.Int(3600000)}))
		}
		h.at(1000)
		h.push("events", "k1", m())
		h.at(11000)
		h.push("events", "k2", m())
		r := h.run(chain(q, q.Queue("out")), opts())
		until(t, func() bool { m := r.Metrics(); return m.EntriesRead >= 2 && len(m.Steps) > 1 && m.Steps[1].Open == 1 }, 0, "the first result in step 2")
		h.at(16000) // quiet: a tick, and step 2's deadline (15 s) passes alone
		until(t, func() bool { return h.count("out") >= 1 }, 3000, "the left timeout")
		h.at(21000) // a tick: k2's window closes at 20 s, its result waits in step 2
		until(t, func() bool { m := r.Metrics(); return m.Steps[1].Open == 1 && m.Steps[0].Open == 0 }, 3000, "k2 in step 2")
		until(t, func() bool { return num(h.record("xstep")["clock"]) >= T0+21000 }, 3000, "the clock committed")
		if crashIt {
			h.crash("a")
			q = h.client("b")
			r = h.run(chain(q, q.Queue("out")), opts())
			until(t, func() bool { return r.Metrics().Role == "active" }, 6000, "the takeover")
			if r.Metrics().Replays < 1 {
				t.Fatal("replays")
			}
		}
		h.at(26000)
		h.push("events", "z", m())
		until(t, func() bool { return h.count("out") >= 2 }, 3000, "the right timeout")
		wait(50)
		return h.payloads("out")
	}
	live := scenario(false)
	eq(t, live, []interface{}{m("side", "left", "key", "k1"), m("side", "right", "key", "k2")}, "live")
	eq(t, scenario(true), live, "after a crash")
}

func TestStepsReviewATakeoverAfterAStandDownKeepsTheStateItLoaded(t *testing.T) {
	h := newHarness(t)
	h.at(1000)
	chain := func(q streams.FeedClient, sink streams.Sink) *streams.Stream {
		return streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"orders"}}).WindowTumbling(60).AggregateFields(countAgg()).To(sink)
	}
	opts := func() streams.FeedRunOptions {
		return quiet(fast(streams.FeedRunOptions{QueryID: "arm", Now: h.now(), LockTTL: time.Second}))
	}
	qa := h.client("a")
	h.b.CutOnFeedCall("a", 2)
	a := h.run(chain(qa, qa.Queue("counts")), opts())
	until(t, func() bool { return a.Metrics().Role == "standby" }, 15000, "a stands down")
	for i := 0; i < 3; i++ {
		h.push("orders", "c"+itoa(i), m())
	}
	qb := h.client("b")
	bb := h.run(chain(qb, qb.Queue("counts")), opts())
	until(t, func() bool { return bb.Metrics().Role == "active" && bb.Metrics().StateKeys == 3 }, 15000, "b active with three")
	stop(t, bb)
	h.b.Cut("a", false)
	until(t, func() bool { return a.Metrics().Role == "active" }, 20000, "a takes over")
	until(t, func() bool { return a.Metrics().Commits >= 1 }, 5000, "a commits")
	stop(t, a)
	qc := h.client("c")
	c := h.run(chain(qc, qc.Queue("counts")), opts())
	if c.Metrics().StateKeys != 3 {
		t.Fatal(c.Metrics().StateKeys)
	}
}

func TestStepsReviewShardsNeverSplitATableQueueTheStreamAlsoNames(t *testing.T) {
	for _, queues := range [][]string{{"acme.*"}, {"acme.bookings", "acme.settings"}} {
		bk := 0
		for feedrt.ShardOf("b"+itoa(bk), 2) != 0 {
			bk++
		}
		pr := 0
		for feedrt.ShardOf("p"+itoa(pr), 2) != 1 {
			pr++
		}
		h := newHarness(t)
		h.push("acme.settings", "p"+itoa(pr), m("currency", "EUR"))
		h.push("acme.bookings", "b"+itoa(bk), m("id", 1.0, "property", "p"+itoa(pr)))
		q := h.client("queen")
		g := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: queues}).
			JoinTable(streams.TableOptions{Queue: "acme.settings", On: onProperty, Select: tableSelect}).
			To(q.Queue("out")), fast(streams.FeedRunOptions{QueryID: "t-" + itoa(len(queues)), Shards: 2}))
		until(t, func() bool { return h.count("out") >= 1 }, 0, "one")
		wait(50)
		stop(t, g)
		eq(t, h.payloads("out"), []interface{}{m("id", 1, "currency", "EUR")}, strings.Join(queues, ","))
	}
}

func TestStepsReviewAProcessStillOnTheOldLayoutStops(t *testing.T) {
	h := newHarness(t)
	for i := 0; i < 6; i++ {
		h.push("orders", "c"+itoa(i), m("n", float64(i)))
	}
	chain := func(q streams.FeedClient, sink streams.Sink) *streams.Stream {
		return streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"orders"}}).To(sink)
	}
	qo := h.client("old")
	old := h.run(chain(qo, qo.Queue("copy")), quiet(fast(streams.FeedRunOptions{QueryID: "relayout", Shards: 2, PositionCommitMillis: streams.Int(50)})))
	until(t, func() bool { return h.count("copy") >= 6 }, 0, "six")
	qn := h.client("new")
	fresh := h.run(chain(qn, qn.Queue("copy")), quiet(fast(streams.FeedRunOptions{QueryID: "relayout", Shards: 3, ResetToken: "three"})))
	h.push("orders", "c9", m("n", 9.0))
	until(t, func() bool {
		for _, s := range old.Metrics().PerShard {
			if s.Role != "stopped" {
				return false
			}
		}
		return true
	}, 4000, "the old shards to stop")
	until(t, func() bool {
		for _, p := range h.payloads("copy") {
			if num(get(p, "n")) == 9 {
				return true
			}
		}
		return false
	}, 0, "the 9")
	wait(100)
	stop(t, fresh)
	nines := 0
	for _, p := range h.payloads("copy") {
		if num(get(p, "n")) == 9 {
			nines++
		}
	}
	if nines != 1 {
		t.Fatal("the new message once", nines)
	}
	layoutStop := false
	for _, s := range old.Metrics().PerShard {
		if e, _ := s.Stopped["error"].(string); strings.Contains(e, "layout") {
			layoutStop = true
		}
	}
	if !layoutStop {
		t.Fatal("stopped on the layout")
	}
}

func TestStepsReviewAResetStringAppliesToAProcessThatTookOver(t *testing.T) {
	h := newHarness(t)
	h.at(1000)
	h.push("orders", "c1", m())
	q1, q2 := h.client("v1"), h.client("v2")
	first := h.run(perMinute(q1, q1.Queue("counts"), nil), fast(streams.FeedRunOptions{QueryID: "reset-takeover", Now: h.now(), ResetToken: "v1"}))
	until(t, func() bool { return first.Metrics().StateKeys == 1 }, 0, "a key")
	second := h.run(perMinute(q2, q2.Queue("counts"), nil), fast(streams.FeedRunOptions{QueryID: "reset-takeover", Now: h.now(), ResetToken: "v2"}))
	if second.Metrics().Role != "standby" {
		t.Fatal(second.Metrics().Role)
	}
	stop(t, first)
	until(t, func() bool { return second.Metrics().Role == "active" }, 6000, "the takeover")
	until(t, func() bool { return h.record("reset-takeover")["resetToken"] == "v2" }, 3000, "v2")
	if num(h.record("reset-takeover")["epoch"]) != 1 {
		t.Fatal("started over once", h.record("reset-takeover"))
	}
}

func TestStepsReviewAReplayLeavesOutWhatTheRunBeforeSkipped(t *testing.T) {
	h := newHarness(t)
	h.at(1000)
	for i := 0; i < 4; i++ {
		h.push("orders", "c1", m("n", float64(i)))
	}
	var mu sync.Mutex
	failing := true
	chain := func(q streams.FeedClient, sink streams.Sink) *streams.Stream {
		return streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"orders"}}).
			Map(func(msg interface{}, _ streams.EmitCtx) (interface{}, error) {
				mu.Lock()
				f := failing
				mu.Unlock()
				if f && num(data(msg)["n"]) == 2 {
					return nil, errors.New("a flaky service")
				}
				return data(msg), nil
			}).WindowTumbling(60).AggregateFields(countAgg()).To(sink)
	}
	opts := func() streams.FeedRunOptions {
		return quiet(fast(streams.FeedRunOptions{QueryID: "skips", Now: h.now(), LockTTL: time.Second, OnPoison: "skip", SnapshotEveryMillis: streams.Int(3600000)}))
	}
	qa := h.client("a")
	a := h.run(chain(qa, qa.Queue("counts")), opts())
	until(t, func() bool { return a.Metrics().Skipped == 1 && h.recordPosition("skips") > 4 }, 0, "the skip")
	if len(asList(h.record("skips")["skips"])) != 1 {
		t.Fatal("the skip is kept with the position", h.record("skips"))
	}
	h.crash("a")
	mu.Lock()
	failing = false
	mu.Unlock()
	qb := h.client("b")
	second := h.run(chain(qb, qb.Queue("counts")), opts())
	until(t, func() bool { return second.Metrics().Role == "active" }, 6000, "the takeover")
	if second.Metrics().ReplaySkipped < 1 {
		t.Fatal(second.Metrics().ReplaySkipped)
	}
	h.at(61000)
	h.push("orders", "x", m("n", 9.0))
	until(t, func() bool { return h.count("counts") >= 1 }, 0, "the window")
	stop(t, second)
	eq(t, h.payloads("counts")[0], m("count", 3), "the skipped message stays out")
}

func TestStepsReviewAShardByThatThrowsMeetsThePoisonPolicyInShard0Only(t *testing.T) {
	h := newHarness(t)
	for i := 0; i < 6; i++ {
		h.push("orders", "c"+itoa(i), m("n", float64(i)))
	}
	var mu sync.Mutex
	var skips []interface{}
	q := h.client("queen")
	o := fast(streams.FeedRunOptions{QueryID: "bad-shard-key", Shards: 3, OnPoison: "skip",
		ShardBy: func(msg interface{}) (interface{}, error) {
			if num(data(msg)["n"]) == 4 {
				return nil, errors.New("no key")
			}
			return "k" + jsv.NumberString(num(data(msg)["n"])), nil
		}})
	o.OnError = func(_ error, ctx map[string]interface{}) {
		if ctx["action"] == "skipped" {
			mu.Lock()
			skips = append(skips, get(ctx["shard"], "index"))
			mu.Unlock()
		}
	}
	g := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"orders"}}).To(q.Queue("copy")), o)
	until(t, func() bool { return h.count("copy") >= 5 }, 0, "five")
	wait(100)
	stop(t, g)
	got := ns(h, "copy")
	sort.Float64s(got)
	eq(t, got, []float64{0, 1, 2, 3, 5}, "copies")
	mu.Lock()
	defer mu.Unlock()
	eq(t, skips, []interface{}{0}, "one skip, in shard 0")
}

func TestStepsReviewSinceWithoutFromStartsAtTheFirstMessageSinceThen(t *testing.T) {
	h := newHarness(t)
	h.at(1000)
	h.push("orders", "c1", m("n", 1.0))
	h.at(5000)
	h.push("orders", "c1", m("n", 2.0))
	q := h.client("queen")
	r := h.run(streams.FromFeed(q, streams.FeedSource{Queues: []string{"orders"}, Since: time.UnixMilli(int64(T0 + 3000))}).To(q.Queue("copy")),
		fast(streams.FeedRunOptions{QueryID: "since"}))
	until(t, func() bool { return h.count("copy") >= 1 }, 0, "one")
	wait(50)
	stop(t, r)
	eq(t, ns(h, "copy"), []float64{2}, "since")
}

func TestStepsReviewAGatherByCountOfResultsFromAWindowCountsEachWindowOnce(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	r := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"orders"}}).
		WindowTumbling(60).AggregateFields(countAgg()).
		Gather(streams.GatherOptions{
			Count:  func(interface{}, interface{}) (interface{}, error) { return 2.0, nil },
			Key:    func(msg, _ interface{}) (interface{}, error) { return get(msg, "partition"), nil },
			Within: "10m",
			OnComplete: func(got interface{}, key string) (interface{}, error) {
				var minutes []interface{}
				for _, g := range got.([]interface{}) {
					minutes = append(minutes, get(g, "data", "count"))
				}
				return m("key", key, "minutes", minutes), nil
			},
		}).To(q.Queue("out")), fast(streams.FeedRunOptions{QueryID: "gather-windows", Now: h.now()}))
	h.at(1000)
	h.push("orders", "c1", m())
	h.at(61000)
	h.push("orders", "c1", m())
	h.push("orders", "c1", m())
	h.at(121000)
	h.push("orders", "flush", m())
	until(t, func() bool { return h.count("out") >= 1 }, 0, "the run")
	stop(t, r)
	eq(t, h.payloads("out"), []interface{}{m("key", "c1", "minutes", []interface{}{1, 2})}, "out")
}

func TestStepsCrashAResultReachesTheNextStepAtTheTimeItsWindowClosed(t *testing.T) {
	chain := func(q streams.FeedClient, sink streams.Sink) *streams.Stream {
		return streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"orders"}}).
			WindowTumbling(60).AggregateFields(countAgg()).
			KeyBy(func(interface{}) (string, error) { return "all", nil }).
			WindowTumbling(60).AggregateFields(streams.AggregateField{Name: "windows", Fn: one}).
			Map(func(v interface{}, ctx streams.EmitCtx) (interface{}, error) {
				out := jsv.Clone(v).(map[string]interface{})
				out["minute"] = ctx["windowKey"]
				return out, nil
			}).To(sink)
	}
	h := newHarness(t)
	qa := h.client("a")
	h.at(1000)
	h.push("orders", "c1", m())
	opts := func() streams.FeedRunOptions {
		return quiet(fast(streams.FeedRunOptions{QueryID: "late-close", Now: h.now(), LockTTL: time.Second, SnapshotEveryMillis: streams.Int(3600000)}))
	}
	a := h.run(chain(qa, qa.Queue("out")), opts())
	until(t, func() bool { return a.Metrics().EntriesRead >= 1 }, 0, "one")
	// A quiet feed: this runner closes minute 0 on the clock (a tick), at about 61 s.
	h.at(61000)
	until(t, func() bool { m := a.Metrics(); return len(m.Steps) > 1 && m.Steps[1].Open == 1 }, 3000, "the first result in the second step")
	h.at(130000)
	h.push("orders", "x", m())
	until(t, func() bool { return h.count("out") >= 1 }, 0, "one out")
	last := h.b.Messages("orders")
	lastPos := last[len(last)-1].Position
	until(t, func() bool { return h.recordPosition("late-close") > float64(lastPos) }, 0, "committed")
	h.crash("a")
	// The next runner replays from the start: it sees minute 0 close only at 130 s.
	qb := h.client("b")
	second := h.run(chain(qb, qb.Queue("out")), opts())
	until(t, func() bool { return second.Metrics().Role == "active" }, 6000, "the takeover")
	h.at(250000)
	h.push("orders", "y", m())
	until(t, func() bool { return h.count("out") >= 2 }, 4000, "two")
	wait(50)
	stop(t, second)
	var minutes []interface{}
	for _, p := range h.payloads("out") {
		minutes = append(minutes, get(p, "minute"))
	}
	eq(t, minutes, []interface{}{iso(MIN0 + 60000), iso(MIN0 + 180000)}, "each result once, in the minute its window closed")
}

func lcg(seed float64) (func() float64, *float64) {
	s := seed
	return func() float64 {
		s = math.Mod(s*1103515245+12345, 2147483648)
		return s / 2147483648
	}, &s
}

func TestStepsCrashTwoWindowsChainedCrashedAtRandomPoints(t *testing.T) {
	rnd, _ := lcg(7)
	type msg struct {
		t    float64
		part string
		v    float64
	}
	var msgs []msg
	tt := 0.0
	for i := 0; i < 160; i++ {
		tt += math.Floor(rnd() * 4000)
		msgs = append(msgs, msg{tt, "p" + jsv.NumberString(math.Floor(rnd()*9)), math.Floor(rnd() * 10)})
	}
	chain := func(q streams.FeedClient, sink streams.Sink) *streams.Stream {
		return streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"events"}}).
			WindowTumbling(60).AggregateFields(countAgg(), field("sum", "v")).
			KeyBy(func(interface{}) (string, error) { return "all", nil }).
			WindowTumbling(180).AggregateFields(streams.AggregateField{Name: "windows", Fn: one}, field("total", "sum")).
			To(sink)
	}
	load := func(h *harness) {
		for _, x := range msgs {
			h.at(x.t)
			h.push("events", x.part, m("v", x.v))
		}
	}
	flush := func(h *harness) int64 {
		h.at(tt + 400000)
		h.push("events", "flush", m("v", 0.0))
		h.at(tt + 800000)
		return h.push("events", "flush", m("v", 0.0))
	}
	ref := newHarness(t)
	load(ref)
	rq := ref.client("queen")
	r := ref.run(chain(rq, rq.Queue("out")), fast(streams.FeedRunOptions{QueryID: "rand", Now: ref.now()}))
	refEnd := flush(ref)
	until(t, func() bool { return ref.recordPosition("rand") > float64(refEnd) }, 6000, "the reference")
	wait(100)
	stop(t, r)
	expected := ref.payloads("out")
	if len(expected) < 3 {
		t.Fatal(len(expected))
	}
	h := newHarness(t)
	load(h)
	opts := func() streams.FeedRunOptions {
		return quiet(fast(streams.FeedRunOptions{QueryID: "rand", Now: h.now(), LockTTL: time.Second, MaxRecords: 7, SnapshotMaxEntries: streams.Int(25)}))
	}
	name := "p0"
	q := h.client(name)
	runner := h.run(chain(q, q.Queue("out")), opts())
	for k := 1; k <= 3; k++ {
		target := math.Floor(float64(len(msgs)*k) / 4)
		until(t, func() bool { return h.recordPosition("rand") >= target }, 6000, "the target")
		h.crash(name)
		name = "p" + itoa(k)
		q = h.client(name)
		runner = h.run(chain(q, q.Queue("out")), opts())
		until(t, func() bool { return runner.Metrics().Role == "active" }, 6000, "the takeover")
	}
	end := flush(h)
	until(t, func() bool { return h.recordPosition("rand") > float64(end) && h.count("out") >= len(expected) }, 8000, "the end")
	wait(100)
	stop(t, runner)
	eq(t, h.payloads("out"), expected, "what one uninterrupted run sends")
}

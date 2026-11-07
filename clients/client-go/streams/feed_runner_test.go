package streams_test

// The behaviours of clients/client-js/test-v2/feed-unit/feed.test.js
// ("Stream.fromFeed runner"), against the Go fake broker.

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/smartpricing/queen/clients/client-go/v2/feed"
	"github.com/smartpricing/queen/clients/client-go/v2/internal/feedfake"
	"github.com/smartpricing/queen/clients/client-go/v2/internal/jsv"
	"github.com/smartpricing/queen/clients/client-go/v2/streams"
	"github.com/smartpricing/queen/clients/client-go/v2/streams/feedrt"
)

func realReads(h *harness) []map[string]interface{} {
	var out []map[string]interface{}
	for _, r := range h.b.FeedReads() {
		if r["from"] == "now" && r["maxRecords"] == 1.0 && r["maxWaitMs"] == 0.0 {
			continue
		}
		out = append(out, r)
	}
	return out
}

func TestFeedPushesOutputsAndPositionInOneTransaction(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.b.Push(feedfake.Msg{Queue: "orders", Partition: "c1", Data: m("n", 1.0, "country", "IT")},
		feedfake.Msg{Queue: "orders", Partition: "c1", Data: m("n", 2.0, "country", "IT")})
	h.push("orders", "c1", m("n", 3.0, "country", "IT"))
	h.push("orders", "c2", m("n", 4.0, "country", "FR"))
	r := h.run(streams.FromFeed(q, streams.FeedSource{Queues: []string{"orders"}, From: "earliest"}).
		Where("data.country", "=", "IT").
		Filter(func(msg interface{}, _ streams.EmitCtx) (bool, error) { return data(msg)["n"] != 2.0, nil }).
		Map(func(msg interface{}, _ streams.EmitCtx) (interface{}, error) { return m("order", data(msg)["n"]), nil }).
		To(q.Queue("orders-it")), fast(streams.FeedRunOptions{QueryID: "orders-it"}))
	until(t, func() bool { return h.count("orders-it") >= 2 }, 0, "two outputs")
	stop(t, r)
	out := h.b.Messages("orders-it")
	eq(t, h.payloads("orders-it"), []interface{}{m("order", 1), m("order", 3)}, "payloads")
	if out[0].Partition != "c1" || out[0].Position != out[1].Position {
		t.Fatal("source partition, one commit", out)
	}
	src := h.b.Messages("orders")[0]
	want := feedrt.FeedOutputID("orders-it", m("position", float64(src.Position), "queue", src.Queue, "partition", src.Partition, "offset", float64(src.Offset)), 0, 0, "orders-it")
	if out[0].TransactionID != want || !strings.HasPrefix(want, feedrt.OutputPrefix("orders-it")) {
		t.Fatal(out[0].TransactionID, want)
	}
	var txn *feedfake.Txn
	for _, x := range h.b.Txns() {
		if len(x.Items) > 0 {
			x := x
			txn = &x
			break
		}
	}
	var pos map[string]interface{}
	guarded := false
	for _, op := range txn.KV {
		if op["key"] == "orders-it" {
			pos = op
		}
		if op["op"] == "check" && op["ns"] == "queen-locks" {
			guarded = true
		}
	}
	if pos["op"] != "putIfAbsent" || pos["required"] != true || !guarded {
		t.Fatal(pos, guarded)
	}
	if h.recordPosition("orders-it") <= 4 {
		t.Fatal(h.record("orders-it"))
	}
	first := realReads(h)[0]
	eq(t, first["queues"], []interface{}{"orders"}, "queues")
	eq(t, first["where"], []interface{}{m("field", "data.country", "op", "=", "value", "IT"),
		m("field", "transactionId", "op", "!prefix", "value", feedrt.OutputPrefix("orders-it"))}, "where")
	if first["from"] != "earliest" {
		t.Fatal(first)
	}
}

func TestFeedAStoredPositionIsWhereItStartsAndCommitsFence(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	for n := 1; n <= 3; n++ {
		h.push("orders", "c1", m("n", float64(n)))
	}
	v := h.b.KVPut(NS, "copy", m("position", 3, "queryId", "copy"))
	r := h.run(streams.FromFeed(q, streams.FeedSource{}).To(q.Queue("copy")), fast(streams.FeedRunOptions{QueryID: "copy"}))
	until(t, func() bool { return h.count("copy") >= 1 }, 0, "one copy")
	h.push("orders", "c1", m("n", 4.0))
	until(t, func() bool { return h.count("copy") >= 2 }, 0, "two copies")
	stop(t, r)
	var ns []interface{}
	for _, p := range h.payloads("copy") {
		ns = append(ns, get(p, "n"))
	}
	eq(t, ns, []interface{}{3, 4}, "from position 3 on")
	if realReads(h)[0]["from"] != 3.0 {
		t.Fatal(realReads(h)[0])
	}
	var commits []feedfake.Txn
	for _, x := range h.b.Txns() {
		if len(x.Items) > 0 {
			commits = append(commits, x)
		}
	}
	expectOf := func(x feedfake.Txn) float64 {
		for _, op := range x.KV {
			if op["key"] == "copy" {
				return num(op["expect"])
			}
		}
		return -1
	}
	if expectOf(commits[0]) != float64(v) || expectOf(commits[1]) != float64(commits[0].Written["copy"]) {
		t.Fatal(expectOf(commits[0]), v, expectOf(commits[1]), commits[0].Written)
	}
}

func TestFeedALostCommitCarriesOnFromThePositionThatWon(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.push("orders", "c1", m("n", 1.0))
	var winner int64
	h.b.InjectNext(feedfake.Inject{Before: func(b *feedfake.Broker) {
		// Another process committed first: the position moved past n=2.
		b.Push(feedfake.Msg{Queue: "orders", Partition: "c1", Data: m("n", 2.0)})
		winner = b.Applied() + 1
		b.KVPut(NS, "copy", m("position", winner, "queryId", "copy"))
	}})
	r := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest"}).To(q.Queue("copy")),
		fast(streams.FeedRunOptions{QueryID: "copy", Exclusive: streams.Bool(false)}))
	until(t, func() bool { return r.Metrics().Conflicts >= 1 }, 0, "a conflict")
	h.push("orders", "c1", m("n", 3.0))
	until(t, func() bool { return h.count("copy") >= 1 }, 0, "the copy")
	stop(t, r)
	eq(t, h.payloads("copy"), []interface{}{m("n", 3)}, "n=1 lost to the winner")
	found := false
	for _, rd := range realReads(h) {
		if rd["from"] == float64(winner) {
			found = true
		}
	}
	if !found {
		t.Fatal("read again from the winner")
	}
}

func TestFeedACommitRefusedAsDuplicateReadsOnFromTheStoredPosition(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.push("orders", "c1", m("n", 1.0))
	src := h.b.Messages("orders")[0]
	var winner int64
	h.b.InjectNext(feedfake.Inject{Before: func(b *feedfake.Broker) {
		id := feedrt.FeedOutputID("race", m("position", float64(src.Position), "queue", src.Queue, "partition", src.Partition, "offset", float64(src.Offset)), 0, 0, "race")
		b.AddID("copy", "c1", id)
		winner = b.Applied() + 1
		b.KVPut(NS, "race", m("position", winner, "queryId", "race"))
	}})
	r := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest"}).To(q.Queue("copy")),
		fast(streams.FeedRunOptions{QueryID: "race", Exclusive: streams.Bool(false)}))
	until(t, func() bool { return r.Metrics().Conflicts >= 1 }, 0, "a conflict")
	h.push("orders", "c1", m("n", 2.0))
	until(t, func() bool { return h.count("copy") >= 1 }, 0, "the copy")
	stop(t, r)
	eq(t, h.payloads("copy"), []interface{}{m("n", 2)}, "payloads")
	found := false
	for _, rd := range realReads(h) {
		if rd["from"] == float64(winner) {
			found = true
		}
	}
	if !found || r.Metrics().Errors != 0 {
		t.Fatal("read on from the winner; a lost race is not an error", r.Metrics().Errors)
	}
}

func TestFeedABrokerWithoutTheFeedFailsRun(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.b.NoFeed = true
	err := h.runErr(streams.FromFeed(q, streams.FeedSource{}).To(q.Queue("copy")), streams.FeedRunOptions{QueryID: "old-broker"})
	if !errors.Is(err, feed.ErrNoFeedRoute) || feedrt.ErrorCode(err) != "no_feed_route" || !strings.Contains(err.Error(), "no POST /api/v1/feed") {
		t.Fatal(err)
	}
}

func TestFeedACycleErrorWithNoHookIsSaidOnStderr(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.push("orders", "c1", m("n", 1.0))
	h.b.InjectNext(feedfake.Inject{Status: 503})
	rd, wr, _ := os.Pipe()
	orig := os.Stderr
	os.Stderr = wr
	r := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest"}).To(q.Queue("copy")), fast(streams.FeedRunOptions{QueryID: "loud"}))
	until(t, func() bool { return h.count("copy") >= 1 }, 5000, "the copy")
	stop(t, r)
	os.Stderr = orig
	wr.Close()
	buf := make([]byte, 1<<16)
	n, _ := rd.Read(buf)
	said := string(buf[:n])
	if !strings.Contains(said, "[queen feed] loud") || !strings.Contains(said, "503") || !strings.Contains(said, "(retrying)") {
		t.Fatal(said)
	}
	if h.count("copy") != 1 {
		t.Fatal("the retry wrote it once")
	}
}

func TestFeedRefusesAChainItCannotRun(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	err := h.runErr(streams.FromFeed(q, streams.FeedSource{}).Gate(func(interface{}, streams.GateContext) (bool, error) { return true, nil }).To(q.Queue("x")),
		streams.FeedRunOptions{QueryID: "g"})
	if err == nil || !strings.Contains(err.Error(), "gate() runs on queue sources only") {
		t.Fatal(err)
	}
	err = h.runErr(streams.FromFeed(q, streams.FeedSource{}).Map(func(v interface{}, _ streams.EmitCtx) (interface{}, error) { return v, nil }),
		streams.FeedRunOptions{QueryID: "nosink"})
	if err == nil || !strings.Contains(err.Error(), "end the chain") {
		t.Fatal(err)
	}
	err = h.runErr(streams.FromFeed(q, streams.FeedSource{}).To(q.Queue("x")), streams.FeedRunOptions{QueryID: "a#b"})
	if err == nil || !strings.Contains(err.Error(), "without '#'") {
		t.Fatal(err)
	}
}

func TestFeedOutputIDsAreTheJSOnes(t *testing.T) {
	// sha256("queen-feed\norders-it")[:8], and the 40 hex of an entry: the formula of the conformance kit.
	e := m("position", 5.0, "queue", "orders", "partition", "c1", "offset", 1.0)
	id := feedrt.FeedOutputID("orders-it", e, 0, 0, "orders-it")
	if len(id) != len(feedrt.OutputPrefix("orders-it"))+40 || !strings.HasPrefix(id, "fd-") {
		t.Fatal(id)
	}
	_ = jsv.Undefined
	_ = context.Background
}

// Not a JS test: a stream function may read its own handle (a foreach that
// logs progress, a map that looks at the metrics through a closure). In JS
// that call just works; in Go it must not wait for the runner it runs in.
func TestFeedMetricsAndDoneFromInsideAStreamFunction(t *testing.T) {
	for _, shards := range []int{0, 2} {
		t.Run("shards "+itoa(shards), func(t *testing.T) {
			h := newHarness(t)
			q := h.client("queen")
			var handle atomic.Value
			var mu sync.Mutex
			var seen []streams.FeedMetrics
			look := func() (streams.FeedMetrics, bool) {
				hv, _ := handle.Load().(streams.FeedHandle)
				if hv == nil {
					return streams.FeedMetrics{}, false
				}
				select {
				case <-hv.Done():
					t.Error("Done() is closed while the stream runs")
				default:
				}
				return hv.Metrics(), true
			}
			s := streams.FromFeed(q, streams.FeedSource{Queues: []string{"orders"}}).
				Map(func(msg interface{}, _ streams.EmitCtx) (interface{}, error) {
					look()
					return get(msg, "data"), nil
				}).
				Foreach(func(interface{}, streams.EmitCtx) error {
					if m, ok := look(); ok {
						mu.Lock()
						seen = append(seen, m)
						mu.Unlock()
					}
					return nil
				})
			r := h.run(s, fast(streams.FeedRunOptions{QueryID: "selfwatch", Now: h.now(), Shards: shards}))
			handle.Store(r)
			for i := 0; i < 4; i++ {
				h.push("orders", "o"+itoa(i), m("n", float64(i)))
			}
			until(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(seen) >= 4 }, 5000, "four effects that read the metrics")
			stop(t, r)
			mu.Lock()
			defer mu.Unlock()
			for i, m := range seen {
				if shards == 0 && (m.Role != "active" || (i > 0 && m.Effects < seen[i-1].Effects) || m.Effects > int64(i)) {
					t.Fatalf("effect %d saw %+v", i, m)
				}
				if shards != 0 && (!m.Group || m.Shards != 2 || len(m.PerShard) != 2) {
					t.Fatalf("effect %d saw %+v", i, m)
				}
			}
			if r.Metrics().Effects != 4 {
				t.Fatal("four effects", r.Metrics().Effects)
			}
		})
	}
}

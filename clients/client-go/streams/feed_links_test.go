package streams_test

// Auto-link on a feed source (PLAN_ROUTING.md §7): a .To() whose output is
// its input message unchanged -- the chain only wheres, filters and keys --
// writes a link to that message in the commit, instead of a copy with an id of
// the query's own. Anything computed is still pushed; Link: false pushes
// everything; a broker that cannot link turns linking off.

import (
	"sync"
	"testing"

	"github.com/smartpricing/queen/clients/client-go/v2/internal/feedfake"
	"github.com/smartpricing/queen/clients/client-go/v2/internal/jsv"
	"github.com/smartpricing/queen/clients/client-go/v2/streams"
	"github.com/smartpricing/queen/clients/client-go/v2/streams/feedrt"
)

// linkOps is every link op the fake broker applied, in order.
func linkOps(h *harness) []map[string]interface{} {
	var out []map[string]interface{}
	for _, x := range h.b.Txns() {
		out = append(out, x.Links...)
	}
	return out
}

// pushedTo counts the messages pushed (not linked) into queue by commits.
func pushedTo(h *harness, queue string) int {
	n := 0
	for _, x := range h.b.Txns() {
		for _, it := range x.Items {
			if it["queue"] == queue && it["link"] != true {
				n++
			}
		}
	}
	return n
}

func keepAll(interface{}, streams.EmitCtx) (bool, error) { return true, nil }

func TestFeedLinksAnUnchangedOutput(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.push("orders", "c1", m("n", 1.0, "country", "IT"))
	h.push("orders", "c2", m("n", 2.0, "country", "FR"))
	h.push("orders", "c1", m("n", 3.0, "country", "IT"))
	r := h.run(streams.FromFeed(q, streams.FeedSource{Queues: []string{"orders"}, From: "earliest"}).
		Where("data.country", "=", "IT").
		Filter(keepAll).
		KeyBy(func(interface{}) (string, error) { return "k", nil }).
		To(q.Queue("orders-it")), fast(streams.FeedRunOptions{QueryID: "it"}))
	until(t, func() bool { return h.count("orders-it") >= 2 }, 0, "two outputs")
	wait(50)
	stop(t, r)
	src, out := h.b.Messages("orders"), h.b.Messages("orders-it")
	eq(t, h.payloads("orders-it"), []interface{}{m("n", 1, "country", "IT"), m("n", 3, "country", "IT")}, "payloads")
	// The same messages: their source's transaction ids, in the source partition.
	if len(out) != 2 || out[0].TransactionID != src[0].TransactionID || out[1].TransactionID != src[2].TransactionID || out[0].Partition != "c1" {
		t.Fatalf("outputs %+v", out)
	}
	links := linkOps(h)
	if len(links) != 2 || pushedTo(h, "orders-it") != 0 {
		t.Fatalf("links %v, pushes %d", links, pushedTo(h, "orders-it"))
	}
	eq(t, links[0], m("type", "link", "queue", "orders-it", "partition", "c1",
		"from", m("queue", "orders", "partition", "c1", "offset", src[0].Offset, "transactionId", src[0].TransactionID)), "the link op")
	// The read gets the two IT orders (the broker filters the FR one).
	if r.Metrics().EntriesRead != 2 || h.count("orders-it") != 2 {
		t.Fatalf("read %d, outputs %d", r.Metrics().EntriesRead, h.count("orders-it"))
	}
}

// The feed shows a linked message in its target, with its source's id and
// transaction id: a stream reading the target sees the outputs. A stream that
// reads its own target (orders* takes orders-it) pushes into it instead, with
// ids of its own prefix, so it never reads its outputs back.
func TestFeedLinkedOutputsAreInTheFeedButNeverReadBack(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.push("orders", "c1", m("n", 1.0, "country", "IT"))
	h.push("orders", "c2", m("n", 2.0, "country", "FR"))
	linker := h.run(streams.FromFeed(q, streams.FeedSource{Queues: []string{"orders"}, From: "earliest"}).
		Where("data.country", "=", "IT").To(q.Queue("orders-it")), fast(streams.FeedRunOptions{QueryID: "linker"}))
	until(t, func() bool { return h.count("orders-it") >= 1 }, 0, "the link")
	stop(t, linker)
	src, linked := h.b.Messages("orders")[0], h.b.Messages("orders-it")[0]
	if linked.ID != src.ID || linked.TransactionID != src.TransactionID || len(linkOps(h)) != 1 {
		t.Fatalf("the target holds the source's message: %+v / %+v", linked, src)
	}

	var mu sync.Mutex
	var seen []string
	reader := h.run(streams.FromFeed(q, streams.FeedSource{Queues: []string{"orders-it"}, From: "earliest"}).
		Foreach(func(_ interface{}, ctx streams.EmitCtx) error {
			mu.Lock()
			seen = append(seen, jsv.String(ctx["transactionId"]))
			mu.Unlock()
			return nil
		}), fast(streams.FeedRunOptions{QueryID: "reader"}))
	// orders* takes orders-it too: into it this stream pushes copies.
	self := h.run(streams.FromFeed(q, streams.FeedSource{Queues: []string{"orders*"}, From: "earliest"}).
		Where("data.country", "=", "IT").To(q.Queue("orders-it")), fast(streams.FeedRunOptions{QueryID: "self"}))
	until(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) >= 3 && self.Metrics().EntriesRead >= 2
	}, 0, "the reader to see the link and both copies")
	wait(100)
	stop(t, self)
	stop(t, reader)
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 3 || seen[0] != src.TransactionID {
		t.Fatalf("the reader saw %v, first the linked %s", seen, src.TransactionID)
	}
	// The order and the link of it, each copied once; its own copies it never read.
	if len(linkOps(h)) != 1 || pushedTo(h, "orders-it") != 2 || self.Metrics().EntriesRead != 2 {
		t.Fatalf("links %d, pushed %d, read %d", len(linkOps(h)), pushedTo(h, "orders-it"), self.Metrics().EntriesRead)
	}
}

func TestFeedLinkFalsePushesCopies(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.push("orders", "c1", m("n", 1.0))
	r := h.run(streams.FromFeed(q, streams.FeedSource{Queues: []string{"orders"}, From: "earliest"}).
		ToWith(q.Queue("copy"), streams.ToOptions{Link: streams.Bool(false)}), fast(streams.FeedRunOptions{QueryID: "copies"}))
	until(t, func() bool { return h.count("copy") >= 1 }, 0, "the copy")
	stop(t, r)
	src := h.b.Messages("orders")[0]
	want := feedrt.FeedOutputID("copies", m("position", float64(src.Position), "queue", src.Queue, "partition", src.Partition, "offset", float64(src.Offset)), 0, 0, "copies")
	if len(linkOps(h)) != 0 || h.b.Messages("copy")[0].TransactionID != want {
		t.Fatalf("links %v, copy %+v", linkOps(h), h.b.Messages("copy")[0])
	}
}

// A computed output is pushed, and so is a payload a push would wrap: a link
// would hand the target the bare value where the push sends {value}.
func TestFeedComputedOutputsAndBareValuesArePushed(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.push("orders", "c1", m("n", 1.0))
	h.push("orders", "c1", 7.0)
	mapped := h.run(streams.FromFeed(q, streams.FeedSource{Queues: []string{"orders"}, From: "earliest"}).
		Map(func(interface{}, streams.EmitCtx) (interface{}, error) { return m("v", 1.0), nil }).
		To(q.Queue("mapped")), fast(streams.FeedRunOptions{QueryID: "mapped"}))
	plain := h.run(streams.FromFeed(q, streams.FeedSource{Queues: []string{"orders"}, From: "earliest"}).
		To(q.Queue("plain")), fast(streams.FeedRunOptions{QueryID: "plain"}))
	until(t, func() bool { return h.count("mapped") >= 2 && h.count("plain") >= 2 }, 0, "outputs")
	wait(50)
	stop(t, mapped)
	stop(t, plain)
	if pushedTo(h, "mapped") != 2 {
		t.Fatalf("a map's outputs are pushed: %d", pushedTo(h, "mapped"))
	}
	eq(t, h.payloads("plain"), []interface{}{m("n", 1), m("value", 7)}, "plain")
	if pushedTo(h, "plain") != 1 || len(linkOps(h)) != 1 {
		t.Fatalf("the object linked, the bare value pushed: %d pushed, %v", pushedTo(h, "plain"), linkOps(h))
	}
}

func TestFeedAnOlderBrokerTurnsLinksOff(t *testing.T) {
	h := newHarness(t)
	h.b.NoLinks = true
	q := h.client("queen")
	h.push("orders", "c1", m("n", 1.0))
	r := h.run(streams.FromFeed(q, streams.FeedSource{Queues: []string{"orders"}, From: "earliest"}).
		To(q.Queue("copy")), fast(streams.FeedRunOptions{QueryID: "old"}))
	until(t, func() bool { return h.count("copy") >= 1 }, 0, "the copy")
	h.push("orders", "c1", m("n", 2.0))
	until(t, func() bool { return h.count("copy") >= 2 }, 0, "the second copy")
	stop(t, r)
	eq(t, h.payloads("copy"), []interface{}{m("n", 1), m("n", 2)}, "payloads")
	if len(linkOps(h)) != 0 || pushedTo(h, "copy") != 2 || r.Metrics().Errors != 0 {
		t.Fatalf("links %v, pushed %d, errors %d", linkOps(h), pushedTo(h, "copy"), r.Metrics().Errors)
	}
}

// A link the target already holds (another writer linked the message there)
// is a no-op the commit survives: the position moves on.
func TestFeedALinkTheTargetHoldsCommits(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.push("orders", "c1", m("n", 1.0))
	src := h.b.Messages("orders")[0]
	h.b.AddID("copy", "c1", src.TransactionID)
	h.push("orders", "c1", m("n", 2.0))
	r := h.run(streams.FromFeed(q, streams.FeedSource{Queues: []string{"orders"}, From: "earliest"}).
		To(q.Queue("copy")), fast(streams.FeedRunOptions{QueryID: "held"}))
	until(t, func() bool { return h.count("copy") >= 1 }, 0, "the second link")
	wait(50)
	stop(t, r)
	eq(t, h.payloads("copy"), []interface{}{m("n", 2)}, "only the one the target did not hold")
	if len(linkOps(h)) != 2 || r.Metrics().Conflicts != 0 || r.Metrics().Errors != 0 {
		t.Fatalf("links %v, conflicts %d", linkOps(h), r.Metrics().Conflicts)
	}
}

func TestCompileFeedLinksOnlyAnUnchangedInput(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	src := func() *streams.Stream {
		return streams.FromFeed(q, streams.FeedSource{Queues: []string{"orders"}})
	}
	toMap := func(msg interface{}, _ streams.EmitCtx) (interface{}, error) { return msg, nil }
	cases := []struct {
		name string
		s    *streams.Stream
		want bool
	}{
		{"to", src().To(q.Queue("x")), true},
		{"where filter keyBy", src().Where("data.a", "=", 1).Filter(keepAll).KeyBy(func(interface{}) (string, error) { return "k", nil }).To(q.Queue("x")), true},
		{"route", src().ToAll(q.Queue("x"), q.Queue("y")), true},
		{"link true", src().ToWith(q.Queue("x"), streams.ToOptions{Link: streams.Bool(true)}), true},
		{"link false", src().ToWith(q.Queue("x"), streams.ToOptions{Link: streams.Bool(false)}), false},
		{"map", src().Map(toMap).To(q.Queue("x")), false},
		{"window", src().WindowTumbling(60).AggregateFields(countAgg()).To(q.Queue("x")), false},
	}
	for _, c := range cases {
		compiled, err := c.s.CompileFeed()
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if compiled.Sink.Link != c.want {
			t.Fatalf("%s: link %v, want %v", c.name, compiled.Sink.Link, c.want)
		}
	}
}

var _ = feedfake.T0

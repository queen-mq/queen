package streams_test

// The behaviours of clients/client-js/test-v2/feed-unit/feedBatch.test.js:
// .ForeachBatch(fn, WithMaxItems, WithMaxWaitMillis) on a feed stream, the
// results in batches and the position saved only once fn returns.

import (
	"errors"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/smartpricing/queen/clients/client-go/v2/internal/feedfake"
	"github.com/smartpricing/queen/clients/client-go/v2/streams"
	"github.com/smartpricing/queen/clients/client-go/v2/streams/feedrt"
)

// batchRec records each call of a batch function, and runs then(items, call) (to fail, wait, crash).
type batchRec struct {
	mu    sync.Mutex
	calls [][]streams.BatchItem
	then  func(items []streams.BatchItem, call int) error
}

func (r *batchRec) fn(items []streams.BatchItem) error {
	r.mu.Lock()
	r.calls = append(r.calls, items)
	n := len(r.calls)
	r.mu.Unlock()
	if r.then != nil {
		return r.then(items, n)
	}
	return nil
}

func (r *batchRec) got() [][]streams.BatchItem {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([][]streams.BatchItem(nil), r.calls...)
}

func (r *batchRec) count() int { return len(r.got()) }

func (r *batchRec) items() int {
	n := 0
	for _, c := range r.got() {
		n += len(c)
	}
	return n
}

func ordersRange(h *harness, from, to int) {
	for i := from; i <= to; i++ {
		h.push("orders", "c"+itoa(i), m("n", float64(i)))
	}
}

func toN(msg interface{}, _ streams.EmitCtx) (interface{}, error) {
	return m("n", get(msg, "data", "n")), nil
}

func itemNs(items []streams.BatchItem) []float64 {
	out := []float64{}
	for _, it := range items {
		out = append(out, num(get(it.Value, "n")))
	}
	return out
}

func callNs(calls [][]streams.BatchItem) [][]float64 {
	out := [][]float64{}
	for _, c := range calls {
		out = append(out, itemNs(c))
	}
	return out
}

func itemIDs(items []streams.BatchItem) []string {
	var out []string
	for _, it := range items {
		out = append(out, it.ID)
	}
	return out
}

func keyCounts(items []streams.BatchItem) [][]interface{} {
	var out [][]interface{}
	for _, it := range items {
		out = append(out, []interface{}{it.Ctx["key"], get(it.Value, "count")})
	}
	return out
}

func lastOrder(h *harness) feedfake.Entry {
	l := h.b.Messages("orders")
	return l[len(l)-1]
}

// past: the record's position is the next one to read, past e once e is committed.
func (h *harness) past(id string, e feedfake.Entry) bool {
	rec := h.record(id)
	return rec != nil && num(rec["position"]) > float64(e.Position)
}

// settled answers the metrics once they pass cond (or after 3 s, for the assertion to report).
// Metrics() is a snapshot the loop publishes at its waits and at the end of a unit, so right
// after a test sees the KV record move it can still be the one from before the commit.
func settled(r streams.FeedHandle, cond func(feedrt.Metrics) bool) feedrt.Metrics {
	end := time.Now().Add(3 * time.Second)
	for {
		mt := r.Metrics()
		if cond(mt) || time.Now().After(end) {
			return mt
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func entryOf(e feedfake.Entry) map[string]interface{} {
	return m("position", float64(e.Position), "queue", e.Queue, "partition", e.Partition, "offset", float64(e.Offset))
}

func waitDone(t *testing.T, r streams.FeedHandle) {
	t.Helper()
	select {
	case <-r.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the stream did not stop")
	}
}

func TestBatchOneBatchPerReadThePositionMovesAfterFnReturns(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	ordersRange(h, 1, 5)
	gate := make(chan struct{})
	var seenWhileOpen atomic.Value
	rec := &batchRec{then: func([]streams.BatchItem, int) error {
		seenWhileOpen.Store(h.record("per-read") == nil)
		<-gate
		return nil
	}}
	r := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest"}).Map(toN).ForeachBatch(rec.fn), fast(streams.FeedRunOptions{QueryID: "per-read"}))
	until(t, func() bool { return rec.count() == 1 }, 0, "one call")
	wait(50)
	if seenWhileOpen.Load() != true || h.record("per-read") != nil {
		t.Fatal("nothing committed while fn runs", h.record("per-read"))
	}
	close(gate)
	until(t, func() bool { return h.past("per-read", lastOrder(h)) }, 0, "the commit after fn")
	items := rec.got()[0]
	eq(t, itemNs(items), []float64{1, 2, 3, 4, 5}, "the whole read, in log order")
	msgs := h.b.Messages("orders")
	for i, it := range items {
		e := msgs[i]
		eq(t, map[string]interface{}(it.Ctx), m("position", float64(e.Position), "queue", "orders", "partition", e.Partition,
			"partitionId", e.PartitionID, "offset", float64(e.Offset), "transactionId", e.TransactionID), "the ctx foreach gets")
		if it.ID != feedrt.FeedOutputID("per-read", entryOf(e), 0, 0, "per-read") {
			t.Fatal("the id a .to() output of it would have", it.ID)
		}
	}
	mt := settled(r, func(m feedrt.Metrics) bool { return m.Batches == 1 && m.Effects == 5 && m.PendingBatch == 0 })
	if mt.Batches != 1 || mt.Effects != 5 || mt.Emitted != 5 || mt.PendingBatch != 0 || rec.count() != 1 {
		t.Fatalf("%+v", mt)
	}
}

func TestBatchMaxItemsAReadOf7GoesInCallsOf3And3And1(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	ordersRange(h, 1, 7)
	var mu sync.Mutex
	var positions []interface{}
	rec := &batchRec{then: func([]streams.BatchItem, int) error {
		var p interface{}
		if rec := h.record("max-items"); rec != nil {
			p = rec["position"]
		}
		mu.Lock()
		positions = append(positions, p)
		mu.Unlock()
		return nil
	}}
	h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest"}).Map(toN).ForeachBatch(rec.fn, streams.WithMaxItems(3)),
		fast(streams.FeedRunOptions{QueryID: "max-items"}))
	until(t, func() bool { return h.past("max-items", lastOrder(h)) }, 0, "past the read")
	eq(t, callNs(rec.got()), [][]float64{{1, 2, 3}, {4, 5, 6}, {7}}, "calls")
	msgs := h.b.Messages("orders")
	mu.Lock()
	defer mu.Unlock()
	eq(t, positions, []interface{}{nil, float64(msgs[3].Position), float64(msgs[6].Position)}, "each call sees the commit of the one before, and no more")
}

func TestBatchMaxItemsAlsoSplitsWhatOneMessageMakes(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.push("orders", "c1", m("n", 5.0))
	var mu sync.Mutex
	var positions []interface{}
	rec := &batchRec{then: func([]streams.BatchItem, int) error {
		var p interface{}
		if rec := h.record("split-one"); rec != nil {
			p = rec["position"]
		}
		mu.Lock()
		positions = append(positions, p)
		mu.Unlock()
		return nil
	}}
	h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest"}).
		FlatMap(func(msg interface{}, _ streams.EmitCtx) ([]interface{}, error) {
			var out []interface{}
			for i := 0; i < int(num(get(msg, "data", "n"))); i++ {
				out = append(out, m("n", float64(i)))
			}
			return out, nil
		}).
		ForeachBatch(rec.fn, streams.WithMaxItems(2)), fast(streams.FeedRunOptions{QueryID: "split-one"}))
	until(t, func() bool { return h.past("split-one", lastOrder(h)) }, 0, "past the message")
	eq(t, callNs(rec.got()), [][]float64{{0, 1}, {2, 3}, {4}}, "calls")
	mu.Lock()
	eq(t, positions, []interface{}{nil, nil, nil}, "one message is one position: it commits once, after its last item")
	mu.Unlock()
	e := entryOf(h.b.Messages("orders")[0])
	var all []streams.BatchItem
	for _, c := range rec.got() {
		all = append(all, c...)
	}
	var want []string
	for i := 0; i < 5; i++ {
		want = append(want, feedrt.FeedOutputID("split-one", e, i, 0, "split-one"))
	}
	eq(t, itemIDs(all), want, "the ids of the unit's counter")
}

func TestBatchMaxWaitMillisOneBatchAcrossReads(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.at(0)
	ordersRange(h, 1, 2)
	rec := &batchRec{}
	r := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest"}).Map(toN).
		ForeachBatch(rec.fn, streams.WithMaxItems(5), streams.WithMaxWaitMillis(1000)),
		fast(streams.FeedRunOptions{QueryID: "linger", Now: h.now()}))
	until(t, func() bool { return r.Metrics().PendingBatch == 2 }, 0, "two wait")
	h.at(400)
	ordersRange(h, 3, 3)
	until(t, func() bool { return r.Metrics().PendingBatch == 3 }, 0, "three wait")
	wait(80)
	if rec.count() != 0 || h.record("linger") != nil {
		t.Fatal("it waits for more, and nothing commits past the items that wait", rec.count(), h.record("linger"))
	}
	h.at(1000)
	until(t, func() bool { return rec.count() == 1 }, 0, "the batch at its wait")
	eq(t, itemNs(rec.got()[0]), []float64{1, 2, 3}, "one batch of the two reads")
	until(t, func() bool { return h.past("linger", lastOrder(h)) }, 0, "the commit")
	// Five at once fill the batch: it goes without waiting.
	ordersRange(h, 4, 8)
	until(t, func() bool { return rec.count() == 2 }, 0, "a full batch")
	eq(t, itemNs(rec.got()[1]), []float64{4, 5, 6, 7, 8}, "the full batch")
	until(t, func() bool { return h.past("linger", lastOrder(h)) }, 0, "the commit")
}

func TestBatchStopDeliversTheBatchThatWaitsThenCommitsWithTheStopMark(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	ordersRange(h, 1, 3)
	rec := &batchRec{}
	r := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest"}).Map(toN).ForeachBatch(rec.fn, streams.WithMaxWaitMillis(3600000)),
		fast(streams.FeedRunOptions{QueryID: "stop-flush", Now: h.now()}))
	until(t, func() bool { return r.Metrics().PendingBatch == 3 }, 0, "three wait")
	if rec.count() != 0 {
		t.Fatal(rec.count())
	}
	stop(t, r)
	eq(t, callNs(rec.got()), [][]float64{{1, 2, 3}}, "delivered at the stop")
	if !h.past("stop-flush", lastOrder(h)) {
		t.Fatal("committed past the batch")
	}
	if get(h.record("stop-flush"), "stopped", "reason") != "stop" {
		t.Fatal(h.record("stop-flush"))
	}
}

func TestBatchABatchThatFailsAtStopIsTriedOnceAndLeftForTheNextStart(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	ordersRange(h, 1, 2)
	first := &batchRec{then: func(items []streams.BatchItem, _ int) error {
		if num(get(items[0].Value, "n")) == 3 {
			return errors.New("down")
		}
		return nil
	}}
	r := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest"}).Map(toN).
		ForeachBatch(first.fn, streams.WithMaxWaitMillis(3600000), streams.WithMaxItems(2)),
		quiet(fast(streams.FeedRunOptions{QueryID: "stop-fail", Now: h.now(), Retries: streams.Int(3)})))
	until(t, func() bool { return h.past("stop-fail", lastOrder(h)) }, 0, "the full batch")
	ordersRange(h, 3, 3)
	until(t, func() bool { return r.Metrics().PendingBatch == 1 }, 0, "one waits")
	stop(t, r)
	eq(t, callNs(first.got()), [][]float64{{1, 2}, {3}}, "one try at the stop: no retries")
	if h.past("stop-fail", lastOrder(h)) {
		t.Fatal("the item is not committed")
	}
	if get(h.record("stop-fail"), "stopped", "reason") != "stop" {
		t.Fatal(h.record("stop-fail"))
	}
	again := &batchRec{}
	qa := h.client("again")
	second := h.run(streams.FromFeed(qa, streams.FeedSource{From: "earliest"}).Map(toN).ForeachBatch(again.fn),
		fast(streams.FeedRunOptions{QueryID: "stop-fail"}))
	until(t, func() bool { return again.count() == 1 }, 0, "the next start")
	eq(t, itemNs(again.got()[0]), []float64{3}, "the item again")
	if again.got()[0][0].ID != first.got()[1][0].ID {
		t.Fatal("the same id: an upsert by it writes once")
	}
	stop(t, second)
}

func TestBatchACrashBeforeFnReturnsDeliversTheBatchAgainWithTheSameIDs(t *testing.T) {
	h := newHarness(t)
	qa := h.client("a")
	h.at(1000)
	h.push("orders", "c1", m())
	h.push("orders", "c2", m())
	h.at(61000)
	h.push("orders", "c1", m())
	perMinute := func(q streams.FeedClient, rec *batchRec) *streams.Stream {
		return streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"orders"}}).
			WindowTumbling(60).AggregateFields(countAgg()).ForeachBatch(rec.fn)
	}
	// A delivers minute 0 and commits it; minute 1 closes in a batch it never commits.
	var cut atomic.Bool
	a := &batchRec{then: func(items []streams.BatchItem, _ int) error {
		for _, it := range items {
			if num(it.Ctx["windowStart"]) == T0+60000 {
				h.crash("a")
				cut.Store(true)
			}
		}
		return nil
	}}
	opts := func() streams.FeedRunOptions {
		return quiet(fast(streams.FeedRunOptions{QueryID: "batch-crash", Now: h.now(), LockTTL: time.Second, SnapshotEveryMillis: streams.Int(3600000)}))
	}
	h.run(perMinute(qa, a), opts())
	until(t, func() bool { return a.count() == 1 && h.past("batch-crash", lastOrder(h)) }, 0, "minute 0")
	eq(t, keyCounts(a.got()[0]), [][]interface{}{{"c1", 1}, {"c2", 1}}, "minute 0, one item per key")
	h.at(121000)
	h.push("orders", "c3", m())
	until(t, func() bool { return cut.Load() }, 0, "the crash")
	lost := a.got()[1]
	eq(t, keyCounts(lost), [][]interface{}{{"c1", 1}}, "minute 1")
	bb := &batchRec{}
	second := h.run(perMinute(h.client("b"), bb), opts())
	until(t, func() bool { return second.Metrics().Role == "active" }, 6000, "the takeover")
	until(t, func() bool { return bb.count() >= 1 }, 0, "a call")
	if second.Metrics().Replays < 1 {
		t.Fatal("it read the feed again from the snapshot")
	}
	eq(t, keyCounts(bb.got()[0]), [][]interface{}{{"c1", 1}}, "minute 1 again, and minute 0 not: the replay sends nothing")
	eq(t, itemIDs(bb.got()[0]), itemIDs(lost), "the same ids as the batch that never committed")
	for _, it := range lost {
		if !strings.HasPrefix(it.ID, feedrt.OutputPrefix("batch-crash")) {
			t.Fatal(it.ID)
		}
	}
	until(t, func() bool { return h.past("batch-crash", lastOrder(h)) }, 0, "the commit")
}

func TestBatchABatchThatFailsThenPassesIsTriedAgainWhole(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	ordersRange(h, 1, 3)
	rec := &batchRec{then: func(_ []streams.BatchItem, call int) error {
		if call <= 2 {
			return errors.New("flaky")
		}
		return nil
	}}
	r := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest"}).Map(toN).ForeachBatch(rec.fn), fast(streams.FeedRunOptions{QueryID: "batch-flaky"}))
	until(t, func() bool { return h.past("batch-flaky", lastOrder(h)) }, 0, "past")
	calls := rec.got()
	eq(t, callNs(calls), [][]float64{{1, 2, 3}, {1, 2, 3}, {1, 2, 3}}, "whole three times")
	eq(t, itemIDs(calls[2]), itemIDs(calls[0]), "the same ids")
	mt := settled(r, func(m feedrt.Metrics) bool { return m.Batches == 1 && m.Effects == 3 })
	if mt.Retries != 2 || mt.Poisoned != 0 || mt.Batches != 1 || mt.Effects != 3 {
		t.Fatalf("%+v", mt)
	}
}

func TestBatchSkipTriedWholeThenItemByItem(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	ordersRange(h, 1, 4)
	type reported struct {
		err error
		ctx map[string]interface{}
	}
	var mu sync.Mutex
	var errs []reported
	rec := &batchRec{then: func(items []streams.BatchItem, _ int) error {
		for _, it := range items {
			if num(get(it.Value, "n")) == 3 {
				return errors.New("bad row")
			}
		}
		return nil
	}}
	r := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest"}).Map(toN).ForeachBatch(rec.fn),
		fast(streams.FeedRunOptions{QueryID: "batch-skip", OnPoison: "skip", Retries: streams.Int(1),
			OnError: func(err error, ctx map[string]interface{}) {
				mu.Lock()
				errs = append(errs, reported{err, ctx})
				mu.Unlock()
			}}))
	until(t, func() bool { return h.past("batch-skip", lastOrder(h)) }, 0, "past")
	eq(t, callNs(rec.got()), [][]float64{{1, 2, 3, 4}, {1, 2, 3, 4}, {1}, {2}, {3}, {3}, {4}}, "calls")
	mt := settled(r, func(m feedrt.Metrics) bool { return m.Skipped == 1 && m.Batches == 3 && m.PendingBatch == 0 })
	if mt.Skipped != 1 || mt.Poisoned != 1 || mt.Effects != 3 || mt.Batches != 3 {
		t.Fatalf("%+v", mt)
	}
	third := h.b.Messages("orders")[2]
	mu.Lock()
	defer mu.Unlock()
	var skip *reported
	for i := range errs {
		if errs[i].ctx["action"] == "skipped" {
			skip = &errs[i]
		}
	}
	if skip == nil || skip.ctx["phase"] != "poison" || num(skip.ctx["position"]) != float64(third.Position) || skip.ctx["partition"] != "c3" {
		t.Fatal(skip)
	}
	if !regexp.MustCompile(`an item of the message at position \d+ \(orders/c3, offset 1\) failed 2 times in foreachBatch and was skipped: bad row`).MatchString(skip.err.Error()) {
		t.Fatal(skip.err)
	}
}

func TestBatchADeadLetterQueueGetsTheItemInTheCommitPastTheBatch(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	ordersRange(h, 1, 3)
	rec := &batchRec{then: func(items []streams.BatchItem, _ int) error {
		for _, it := range items {
			if num(get(it.Value, "n")) == 2 {
				return typeError{msg: "no price"}
			}
		}
		return nil
	}}
	r := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest"}).Map(toN).ForeachBatch(rec.fn),
		quiet(fast(streams.FeedRunOptions{QueryID: "batch-dlq", OnPoison: "batch-dlq-q", Retries: streams.Int(0)})))
	until(t, func() bool { return h.past("batch-dlq", lastOrder(h)) }, 0, "past")
	calls := rec.got()
	eq(t, callNs(calls), [][]float64{{1, 2, 3}, {1}, {2}, {3}}, "calls")
	dead := h.b.Messages("batch-dlq-q")
	if len(dead) != 1 || dead[0].Partition != "c2" {
		t.Fatal("dead-lettered, in the partition it came from", dead)
	}
	second := h.b.Messages("orders")[1]
	eq(t, dead[0].Payload, m("queryId", "batch-dlq", "attempts", 1, "error", m("name", "TypeError", "message", "no price"),
		"item", m("value", m("n", 2), "ctx", map[string]interface{}(calls[0][1].Ctx), "id", feedrt.FeedOutputID("batch-dlq", entryOf(second), 0, 0, "batch-dlq"))), "the record")
	if !strings.HasPrefix(dead[0].TransactionID, feedrt.OutputPrefix("batch-dlq")) {
		t.Fatal("the query never reads it back", dead[0].TransactionID)
	}
	var commit *feedfake.Txn
	for _, x := range h.b.Txns() {
		for _, it := range x.Items {
			if it["queue"] == "batch-dlq-q" {
				x := x
				commit = &x
			}
		}
	}
	moved := false
	for _, op := range commit.KV {
		if op["key"] == "batch-dlq" && num(get(op["value"], "position")) > float64(lastOrder(h).Position) {
			moved = true
		}
	}
	if !moved {
		t.Fatal("with the position past the batch, in one transaction", commit.KV)
	}
	if mt := settled(r, func(m feedrt.Metrics) bool { return m.DeadLettered == 1 }); mt.DeadLettered != 1 {
		t.Fatal(mt.DeadLettered)
	}
}

func TestBatchStopNothingOfTheBatchCommitsAndARestartGetsItAgain(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	ordersRange(h, 1, 2)
	var mu sync.Mutex
	var errs []error
	var ctxs []map[string]interface{}
	rec := &batchRec{then: func(items []streams.BatchItem, _ int) error {
		for _, it := range items {
			if num(get(it.Value, "n")) == 4 {
				return errors.New("bad row")
			}
		}
		return nil
	}}
	r := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest"}).Map(toN).ForeachBatch(rec.fn),
		fast(streams.FeedRunOptions{QueryID: "batch-stop", Retries: streams.Int(1),
			OnError: func(err error, ctx map[string]interface{}) {
				mu.Lock()
				errs = append(errs, err)
				ctxs = append(ctxs, ctx)
				mu.Unlock()
			}}))
	until(t, func() bool { return h.past("batch-stop", lastOrder(h)) }, 0, "the first batch")
	before := num(h.record("batch-stop")["position"])
	ordersRange(h, 3, 5)
	waitDone(t, r)
	eq(t, callNs(rec.got()), [][]float64{{1, 2}, {3, 4, 5}, {3, 4, 5}, {3}, {4}, {4}}, "whole twice, then one by one up to the item")
	fourth := h.b.Messages("orders")[3]
	rec2 := h.record("batch-stop")
	if num(rec2["position"]) != before {
		t.Fatal("the last commit: before the batch", rec2["position"], before)
	}
	if get(rec2, "stopped", "reason") != "poison" || num(get(rec2, "stopped", "position")) != float64(fourth.Position) || get(rec2, "stopped", "partition") != "c4" {
		t.Fatal("the item that failed", rec2["stopped"])
	}
	mt := settled(r, func(m feedrt.Metrics) bool { return m.Role == "stopped" })
	if mt.Role != "stopped" || mt.Stopped["reason"] != "poison" || num(mt.Stopped["position"]) != float64(fourth.Position) {
		t.Fatalf("%+v", mt)
	}
	mu.Lock()
	var stopErr error
	for i, c := range ctxs {
		if c["action"] == "stopped" {
			stopErr = errs[i]
		}
	}
	mu.Unlock()
	if feedrt.ErrorCode(stopErr) != "feed_poison" ||
		!regexp.MustCompile(`an item of the message at position \d+ \(orders/c4, offset 1\) failed 2 times in foreachBatch: bad row\. The stream stopped before its batch`).MatchString(stopErr.Error()) {
		t.Fatal(stopErr)
	}
	again := &batchRec{}
	h.run(streams.FromFeed(h.client("fixed"), streams.FeedSource{From: "earliest"}).Map(toN).ForeachBatch(again.fn), fast(streams.FeedRunOptions{QueryID: "batch-stop"}))
	until(t, func() bool { return h.past("batch-stop", lastOrder(h)) }, 0, "past, fixed")
	eq(t, callNs(again.got()), [][]float64{{3, 4, 5}}, "the whole batch again")
	eq(t, itemIDs(again.got()[0]), itemIDs(rec.got()[1]), "the same ids")
}

func TestBatchAMessageThatStopsTheStreamComesAfterTheBatchThatWaits(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	ordersRange(h, 1, 4)
	rec := &batchRec{}
	r := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest"}).
		Map(func(msg interface{}, _ streams.EmitCtx) (interface{}, error) {
			if num(get(msg, "data", "n")) == 3 {
				return nil, errors.New("bad order")
			}
			return m("n", get(msg, "data", "n")), nil
		}).ForeachBatch(rec.fn), quiet(fast(streams.FeedRunOptions{QueryID: "msg-stop"})))
	waitDone(t, r)
	eq(t, callNs(rec.got()), [][]float64{{1, 2}}, "the batch first")
	third := h.b.Messages("orders")[2]
	rc := h.record("msg-stop")
	if num(rc["position"]) != float64(third.Position) || get(rc, "stopped", "reason") != "poison" {
		t.Fatal("stopped at the message", rc)
	}
	if mt := settled(r, func(m feedrt.Metrics) bool { return m.Effects == 2 }); mt.Effects != 2 {
		t.Fatal(mt.Effects)
	}
}

func TestBatchNothingCommitsWhileAWindowResultWaitsNotEvenATick(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.at(1000)
	h.push("orders", "c1", m())
	h.at(61000)
	h.push("orders", "c2", m())
	rec := &batchRec{}
	r := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"orders"}}).
		WindowTumbling(60).AggregateFields(countAgg()).ForeachBatch(rec.fn, streams.WithMaxWaitMillis(100000)),
		fast(streams.FeedRunOptions{QueryID: "batch-tick", Now: h.now()}))
	until(t, func() bool { return r.Metrics().PendingBatch == 1 }, 0, "minute 0 waits")
	commits := r.Metrics().Commits
	// Minute 1 is due by now on this clock: a stream without a waiting batch would tick.
	h.at(125000)
	wait(150)
	if r.Metrics().Ticks != 0 || r.Metrics().Commits != commits || rec.count() != 0 {
		t.Fatal("no tick, no commit while an item waits", r.Metrics().Ticks, r.Metrics().Commits, commits, rec.count())
	}
	h.at(161000)
	until(t, func() bool { return rec.count() == 1 }, 0, "the batch at its wait")
	eq(t, keyCounts(rec.got()[0]), [][]interface{}{{"c1", 1}}, "minute 0")
	// The commit after it moved the broker's clock: minute 1 closes into the next batch.
	// Batches == 1 too: an older snapshot still shows minute 0 waiting.
	until(t, func() bool { mt := r.Metrics(); return mt.Batches == 1 && mt.PendingBatch == 1 }, 0, "minute 1 waits")
	h.at(262000)
	until(t, func() bool { return rec.count() == 2 }, 0, "the second batch")
	eq(t, keyCounts(rec.got()[1]), [][]interface{}{{"c2", 1}}, "minute 1")
	prefix := feedrt.OutputPrefix("batch-tick")
	for _, c := range rec.got() {
		for _, it := range c {
			if !strings.HasPrefix(it.ID, prefix) || len(it.ID) != len(prefix)+40 {
				t.Fatal(it.ID)
			}
		}
	}
}

func TestBatchACloseOfManyWindowsGoesInBatchesOfMaxItems(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.at(1000)
	const parts = 2500
	for i := 0; i < parts; i++ {
		h.push("orders", "p"+itoa(i), m())
	}
	rec := &batchRec{}
	r := h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"orders"}}).
		WindowTumbling(60).AggregateFields(countAgg()).ForeachBatch(rec.fn, streams.WithMaxItems(1000)),
		fast(streams.FeedRunOptions{QueryID: "batch-mass", Now: h.now()}))
	until(t, func() bool {
		mt := r.Metrics()
		return mt.EntriesRead >= parts && mt.PendingBatch == 0 && mt.Commits >= 1
	}, 10000, "every message read")
	h.at(61000)
	h.push("other", "x", m())
	until(t, func() bool { return rec.items() >= parts }, 10000, "every window closed")
	var sizes []int
	ids := map[string]bool{}
	for _, c := range rec.got() {
		sizes = append(sizes, len(c))
		for _, it := range c {
			ids[it.ID] = true
		}
	}
	eq(t, sizes, []int{1000, 1000, 500}, "calls")
	if mt := settled(r, func(m feedrt.Metrics) bool { return m.CloseCommits >= 1 }); mt.CloseCommits < 1 {
		t.Fatal("a commit in the middle of the close")
	}
	if len(ids) != parts {
		t.Fatal("every result its own id", len(ids))
	}
}

func TestBatchRunsOnStreamFromFeedOnly(t *testing.T) {
	_, err := streams.From(fakeSource{name: "orders"}).ForeachBatch(func([]streams.BatchItem) error { return nil }).
		Run(nil, streams.RunOptions{QueryID: "q"})
	if err == nil || !strings.Contains(err.Error(), ".foreachBatch() runs on Stream.fromFeed only") {
		t.Fatal(err)
	}
}

func TestBatchChecksItsFunctionAndOptions(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	fn := func([]streams.BatchItem) error { return nil }
	check := func(s *streams.Stream, want string) {
		t.Helper()
		if _, err := s.CompileFeed(); err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("%v, want %q", err, want)
		}
	}
	src := func() *streams.Stream { return streams.FromFeed(q, streams.FeedSource{}) }
	check(src().ForeachBatch(nil), "fn is a function")
	check(src().ForeachBatch(fn, streams.WithMaxItems(0)), "maxItems is a whole number from 1")
	check(src().ForeachBatch(fn, streams.WithMaxWaitMillis(-1)), "maxWaitMillis is a whole number from 0")
	// (A maxItems of 1.5 or an unknown option cannot be written in Go.)
	err := h.runErr(src().ForeachBatch(fn).Map(func(v interface{}, _ streams.EmitCtx) (interface{}, error) { return v, nil }),
		fast(streams.FeedRunOptions{QueryID: "x"}))
	if err == nil || !strings.Contains(err.Error(), "must be the last in the chain") {
		t.Fatal(err)
	}
}

func TestBatchTheDescriptionNamesTheSinkAndItsOptions(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	ordersRange(h, 1, 1)
	h.run(streams.FromFeed(q, streams.FeedSource{From: "earliest"}).
		ForeachBatch(func([]streams.BatchItem) error { return nil }, streams.WithMaxItems(50), streams.WithMaxWaitMillis(250)),
		fast(streams.FeedRunOptions{QueryID: "batch-about"}))
	until(t, func() bool { return h.record("batch-about#about") != nil }, 0, "the description")
	eq(t, h.record("batch-about#about")["sink"], m("kind", "foreachBatch", "maxItems", 50, "maxWaitMillis", 250), "sink")
}

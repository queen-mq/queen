package streams_test

// The runner behaviours of clients/client-js/test-v2/feed-unit/feedSnapshots.test.js:
// a long close commits in pieces (the close cursor), and snapshots in pieces
// and layers.

import (
	"bytes"
	"compress/flate"
	"encoding/base64"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/smartpricing/queen/clients/client-go/v2/internal/feedfake"
	"github.com/smartpricing/queen/clients/client-go/v2/internal/jsv"
	"github.com/smartpricing/queen/clients/client-go/v2/streams"
	"github.com/smartpricing/queen/clients/client-go/v2/streams/feedrt"
)

func sidOf(prefix, key string) string {
	rest := key[len(prefix):]
	return rest[:strings.LastIndex(rest, "#")]
}

// pieces reads layer sid of id from the fake, as readLayer does.
func pieces(t *testing.T, h *harness, id, sid string, count int) []map[string]interface{} {
	t.Helper()
	var out []map[string]interface{}
	var parts []string
	for n := 0; n < count; n++ {
		v, _, ok := h.b.KVGet(NS, id+"#snap#"+sid+"#"+itoa(n))
		if !ok {
			t.Fatal("chunk missing", n)
		}
		parts = append(parts, get(v, "d").(string))
		if jsv.Truthy(get(v, "m")) {
			continue
		}
		raw, err := base64.StdEncoding.DecodeString(strings.Join(parts, ""))
		if err != nil {
			t.Fatal(err)
		}
		parts = nil
		buf, err := io.ReadAll(flate.NewReader(bytes.NewReader(raw)))
		if err != nil {
			t.Fatal(err)
		}
		p, _ := jsv.Unmarshal(buf)
		out = append(out, p.(map[string]interface{}))
	}
	return out
}

func TestSnapshots12000WindowsThatEndAtOneTimeCommitInPieces(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.at(1000)
	const N = 12000
	for i := 0; i < N; i++ {
		h.push("orders", "c"+itoa(i), m())
	}
	r := h.run(perMinute(q, q.Queue("counts"), nil), fast(streams.FeedRunOptions{QueryID: "burst", Now: h.now(), MaxPendingOutputs: streams.Int(1000), SnapshotEveryMillis: streams.Int(3600000)}))
	until(t, func() bool { return r.Metrics().EntriesRead >= N }, 15000, "every message read")
	h.at(61000)
	h.push("orders", "late", m())
	until(t, func() bool { return h.count("counts") >= N }, 30000, "every window closed")
	stop(t, r)
	keys := map[string]bool{}
	for _, e := range h.b.Messages("counts") {
		keys[e.Partition] = true
	}
	if h.count("counts") != N || len(keys) != N {
		t.Fatal("no result twice", h.count("counts"), len(keys))
	}
	// A commit comes after a batch of closes (2,000) once 1,000 results wait: six for 12,000.
	if r.Metrics().CloseCommits < 5 {
		t.Fatal("commits in the middle of the close", r.Metrics().CloseCommits)
	}
	if num(h.record("burst")["units"]) != N {
		t.Fatal("the record counts the closes", h.record("burst")["units"])
	}
}

func TestSnapshotsACrashInTheMiddleOfALongClose(t *testing.T) {
	for _, c := range []struct {
		what   string
		everyM int
	}{{"read again from the start", 3600000}, {"snapshots in the middle of the close", 1}} {
		t.Run(c.what, func(t *testing.T) {
			h := newHarness(t)
			qa := h.client("a")
			h.at(1000)
			const N = 20000
			for i := 0; i < N; i++ {
				h.push("orders", "c"+itoa(i), m())
			}
			opts := func() streams.FeedRunOptions {
				return quiet(fast(streams.FeedRunOptions{QueryID: "burstcrash", Now: h.now(), LockTTL: time.Second, MaxPendingOutputs: streams.Int(400), SnapshotEveryMillis: streams.Int(c.everyM)}))
			}
			// JS crashes the process once its metrics show three commits in the
			// middle of the close; Go closes 20,000 windows faster than a test can
			// look, so the fake cuts it right after its third commit of results.
			results := 0
			h.b.CutAfter("a", func(tx feedfake.Txn) bool {
				for _, it := range tx.Items {
					if it["queue"] == "counts" {
						results++
						return results == 3
					}
				}
				return false
			})
			a := h.run(perMinute(qa, qa.Queue("counts"), nil), opts())
			until(t, func() bool { return a.Metrics().EntriesRead >= N }, 15000, "every message read")
			h.at(61000)
			h.push("orders", "late", m())
			until(t, func() bool { return a.Metrics().CloseCommits >= 3 }, 15000, "commits in the middle of the close")
			h.crash("a")
			wait(50)
			stored := h.record("burstcrash")
			units := num(stored["units"])
			if units <= 0 || units >= N {
				t.Fatal("the record names how far the close got", units)
			}
			if float64(h.count("counts")) != units {
				t.Fatal("what is committed is what the record counts", h.count("counts"), units)
			}
			qb := h.client("b")
			second := h.run(perMinute(qb, qb.Queue("counts"), nil), opts())
			until(t, func() bool { return second.Metrics().Role == "active" }, 8000, "the takeover")
			until(t, func() bool { return h.count("counts") >= N }, 30000, "the rest of the close")
			stop(t, second)
			keys := map[string]bool{}
			for _, e := range h.b.Messages("counts") {
				keys[e.Partition] = true
			}
			if h.count("counts") != N || len(keys) != N || num(h.record("burstcrash")["units"]) != N {
				t.Fatal("every window once", h.count("counts"), len(keys), h.record("burstcrash")["units"])
			}
		})
	}
}

func TestSnapshotsAWaitThatMatchedAfterTheFullLayerIsADeleteInTheNextDelta(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.at(1000)
	for i := 0; i < 20; i++ {
		h.push("orders", "o"+itoa(i), m("start", true))
	}
	waits := func(q streams.FeedClient, sink streams.Sink) *streams.Stream {
		return streams.FromFeed(q, streams.FeedSource{From: "earliest", Queues: []string{"orders"}}).
			Pattern(streams.PatternOptions{
				Start:   func(msg interface{}) (bool, error) { return get(msg, "data", "start") == true, nil },
				Then:    func(msg interface{}) (bool, error) { return get(msg, "data", "paid") == true, nil },
				Within:  "1h",
				OnMatch: func(s, _ interface{}) (interface{}, error) { return m("paid", get(s, "partition")), nil },
			}).To(sink)
	}
	r := h.run(waits(q, q.Queue("paid")), fast(streams.FeedRunOptions{QueryID: "dels", Now: h.now(), SnapshotMaxEntries: streams.Int(1)}))
	until(t, func() bool { return r.Metrics().EntriesRead >= 20 && r.Metrics().Snapshots >= 1 }, 0, "a snapshot")
	base := layers(h.record("dels"))[0]
	h.push("orders", "o3", m("paid", true))
	h.push("orders", "o7", m("paid", true))
	until(t, func() bool { return h.count("paid") >= 2 && len(layers(h.record("dels"))) >= 2 }, 0, "a delta")
	stop(t, r)
	ls := layers(h.record("dels"))
	if get(ls[0], "sid") != get(base, "sid") {
		t.Fatal("the full layer stays under the deltas")
	}
	var deleted []string
	for _, l := range ls[1:] {
		for _, pc := range pieces(t, h, "dels", get(l, "sid").(string), int(num(get(l, "chunks")))) {
			for _, k := range asList(pc["del"]) {
				deleted = append(deleted, k.(string))
			}
		}
	}
	n := 0
	for _, k := range deleted {
		if strings.HasSuffix(k, "o3") || strings.HasSuffix(k, "o7") {
			n++
		}
	}
	if n != 2 {
		t.Fatal("the two matched waits are deletes", deleted)
	}
	qa := h.client("again")
	again := h.run(waits(qa, qa.Queue("paid")), fast(streams.FeedRunOptions{QueryID: "dels", Now: h.now()}))
	if again.Metrics().Open != 18 {
		t.Fatal("eighteen waits came back", again.Metrics().Open)
	}
	h.push("orders", "o3", m("paid", true))
	h.push("orders", "o4", m("paid", true))
	until(t, func() bool { return h.count("paid") >= 3 }, 0, "three")
	stop(t, again)
	var paid []string
	for _, p := range h.payloads("paid") {
		paid = append(paid, get(p, "paid").(string))
	}
	sortStrings(paid)
	eq(t, paid, []string{"o3", "o4", "o7"}, "o3 matched once, o4 matched now")
}

func sortStrings(l []string) {
	for i := 1; i < len(l); i++ {
		for j := i; j > 0 && l[j] < l[j-1]; j-- {
			l[j], l[j-1] = l[j-1], l[j]
		}
	}
}

func TestSnapshotsWhenTheDeltasWeighAsMuchAsTheFullLayerAFullLayerReplacesThem(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.at(1000)
	for i := 0; i < 4; i++ {
		h.push("orders", "c"+itoa(i), m())
	}
	r := h.run(perMinute(q, q.Queue("counts"), nil), fast(streams.FeedRunOptions{QueryID: "compact", Now: h.now(), SnapshotMaxEntries: streams.Int(1)}))
	until(t, func() bool { return r.Metrics().Snapshots >= 1 && r.Metrics().EntriesRead >= 4 }, 0, "a snapshot")
	firstBase := get(layers(h.record("compact"))[0], "sid")
	sawDeltas := 0
	for i := 4; i < 40; i++ {
		h.push("orders", "c"+itoa(i), m())
		until(t, func() bool { return r.Metrics().EntriesRead >= int64(i+1) }, 0, "read")
		if d := len(layers(h.record("compact"))) - 1; d > sawDeltas {
			sawDeltas = d
		}
	}
	until(t, func() bool {
		ls := layers(h.record("compact"))
		return get(ls[0], "sid") != firstBase && get(ls[0], "full") == true
	}, 3000, "a new full layer")
	stop(t, r)
	if sawDeltas < 1 || r.Metrics().SnapshotsFull < 2 {
		t.Fatal(sawDeltas, r.Metrics().SnapshotsFull)
	}
	named := map[string]bool{}
	for _, l := range layers(h.record("compact")) {
		named[get(l, "sid").(string)] = true
	}
	for _, k := range h.b.KVKeys(NS, "compact#snap#") {
		if !named[sidOf("compact#snap#", k)] || sidOf("compact#snap#", k) == firstBase {
			t.Fatal("the replaced layers are gone from KV", k)
		}
	}
	qa := h.client("again")
	again := h.run(perMinute(qa, qa.Queue("counts"), nil), fast(streams.FeedRunOptions{QueryID: "compact", Now: h.now()}))
	if again.Metrics().StateKeys != 40 {
		t.Fatal("all forty windows came back", again.Metrics().StateKeys)
	}
}

func TestSnapshotsASnapshotOfFormat1StillLoads(t *testing.T) {
	h := newHarness(t)
	q := h.client("queen")
	h.at(1000)
	for i := 0; i < 3; i++ {
		h.push("orders", "c"+itoa(i), m())
	}
	first := h.run(perMinute(q, q.Queue("counts"), nil), fast(streams.FeedRunOptions{QueryID: "old", Now: h.now()}))
	until(t, func() bool { return first.Metrics().EntriesRead >= 3 }, 0, "three")
	stop(t, first)
	// Rewrite the stored snapshot the way a runner of format 1 wrote it.
	rec := h.record("old")
	var entries []feedrt.KV
	for _, l := range layers(rec) {
		for _, pc := range pieces(t, h, "old", get(l, "sid").(string), int(num(get(l, "chunks")))) {
			for _, kv := range asList(pc["set"]) {
				pair := kv.([]interface{})
				entries = append(entries, feedrt.KV{K: pair[0].(string), V: pair[1]})
			}
		}
	}
	for _, k := range h.b.KVKeys(NS, "old#snap#") {
		h.b.KVDelete(NS, k)
	}
	chunks, size := feedrt.EncodeSnapshot([]feedrt.StepDump{{Name: "perMinute", Kind: "window", Entries: entries}}, feedrt.DefaultChunkChars)
	sid := "f1sid0001"
	for n, d := range chunks {
		h.b.KVPut(NS, "old#snap#"+sid+"#"+itoa(n), m("d", d))
	}
	snap := rec["snapshot"].(map[string]interface{})
	rec["snapshot"] = m("id", sid, "chunks", float64(len(chunks)), "bytes", float64(size), "position", snap["position"], "clock", snap["clock"], "metas", snap["metas"])
	h.b.KVPut(NS, "old", rec)
	qa := h.client("again")
	again := h.run(perMinute(qa, qa.Queue("counts"), nil), fast(streams.FeedRunOptions{QueryID: "old", Now: h.now(), SnapshotMaxEntries: streams.Int(1)}))
	if again.Metrics().StateKeys != 3 {
		t.Fatal("the three windows of the old snapshot came back", again.Metrics().StateKeys)
	}
	h.b.Push(feedfake.Msg{Queue: "orders", Partition: "c9", Data: m()})
	until(t, func() bool { return len(layers(h.record("old"))) == 1 }, 0, "a layer")
	stop(t, again)
	if get(layers(h.record("old"))[0], "full") != true {
		t.Fatal("a full layer of format 2")
	}
	for _, k := range h.b.KVKeys(NS, "old#snap#") {
		if sidOf("old#snap#", k) == sid {
			t.Fatal("the old chunks are gone", k)
		}
	}
}

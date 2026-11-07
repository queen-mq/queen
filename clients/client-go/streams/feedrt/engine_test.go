package feedrt

// The engine behaviours of clients/client-js/test-v2/feed-unit/feedSnapshots.test.js:
// counts kept as the state changes, metrics at any size, millions due at once,
// snapshots in pieces and layers (writeLayer / readLayer on a KV in memory).

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/smartpricing/queen/clients/client-go/v2/feed"
	"github.com/smartpricing/queen/clients/client-go/v2/internal/jsv"
)

func TestFeedStateKeepsItsClassCountsThroughEverything(t *testing.T) {
	classify := func(k string, v interface{}) string {
		if strings.HasPrefix(k, "b") {
			return "held"
		}
		if v != nil && !jsv.Truthy(jsv.Get(v, "done")) {
			return "open"
		}
		return ""
	}
	st := NewFeedState()
	st.ClassifyWith(classify)
	scan := func(c string) int {
		n := 0
		st.Each(func(k string, v interface{}) bool {
			if classify(k, v) == c {
				n++
			}
			return true
		})
		return n
	}
	seed := 7.0
	rnd := func(n float64) float64 {
		seed = math.Mod(seed*1103515245+12345, 2147483648)
		return math.Mod(seed, n)
	}
	for round := 0; round < 300; round++ {
		mark := st.Mark()
		for i := 0; i < 20; i++ {
			k := "c"
			if rnd(2) != 0 {
				k = "b"
			}
			k += jsv.NumberString(rnd(30))
			if rnd(3) == 0 {
				st.Delete(k)
			} else {
				st.Set(k, map[string]interface{}{"done": rnd(4) == 0})
			}
		}
		if rnd(2) != 0 {
			st.RollbackTo(mark)
		} else {
			st.Committed()
		}
		if st.Count("open") != scan("open") || st.Count("held") != scan("held") {
			t.Fatalf("round %d: open %d/%d held %d/%d", round, st.Count("open"), scan("open"), st.Count("held"), scan("held"))
		}
	}
	entries := st.Dump()
	st.LoadBegin()
	for _, e := range entries {
		st.LoadSet(e.K, e.V)
	}
	st.LoadDelete(entries[0].K)
	st.Loaded()
	if st.Count("open") != scan("open") || st.Count("held") != scan("held") || st.TouchedSinceSnapshot() != 0 {
		t.Fatal("after a load in pieces")
	}
	st.Wipe()
	if st.Count("open") != 0 {
		t.Fatal("wipe")
	}
}

func TestFeedStateIteratesLikeAJSMap(t *testing.T) {
	st := NewFeedState()
	st.Set("a", 1.0)
	st.Set("b", 2.0)
	st.Set("c", 3.0)
	st.Set("a", 4.0) // keeps its place
	st.Delete("b")
	st.Set("b", 5.0) // goes last
	mark := st.Mark()
	st.Delete("c")
	st.RollbackTo(mark) // c comes back, last
	var keys []string
	st.Each(func(k string, _ interface{}) bool { keys = append(keys, k); return true })
	if !reflect.DeepEqual(keys, []string{"a", "b", "c"}) {
		t.Fatal(keys)
	}
}

func perMinutePlan() []Step {
	return []Step{{Kind: "window", Name: "perMinute",
		Window:  &WindowSpec{Kind: "tumbling", WindowMs: 60000, Time: TimeSpec{OnLate: "drop"}},
		Reducer: &ReducerSpec{Fn: func(acc, _ interface{}) (interface{}, error) { return acc, nil }, Initial: map[string]interface{}{"count": 0.0}, IsAggregate: true}}}
}

func loaded(t *testing.T, n int, due float64) *Pipeline {
	t.Helper()
	p, err := NewPipeline(perMinutePlan())
	if err != nil {
		t.Fatal(err)
	}
	p.LoadBegin()
	set := make([]interface{}, n)
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("k%07d", i)
		set[i] = []interface{}{"w\u001f0\u001f" + k, map[string]interface{}{"key": k, "start": 0.0, "end": due, "due": due, "acc": map[string]interface{}{"count": 1.0}}}
	}
	p.LoadPiece(0, "window", set, nil)
	p.LoadEnd(nil)
	return p
}

func TestFeedPipelineMetricsCostTheSameWith200000OpenWindows(t *testing.T) {
	const N = 200000
	p := loaded(t, N, 60000)
	t0 := time.Now()
	open := 0
	for i := 0; i < 2000; i++ {
		open = p.Open()
		p.StepMetrics(0)
	}
	if open != N {
		t.Fatal(open)
	}
	if ms := time.Since(t0).Milliseconds(); ms > 300 {
		t.Fatalf("2,000 reads of the metrics took %d ms", ms)
	}
}

func TestFeed100000WindowsThatEndAtOneTimeCloseInBatchesInKeyOrder(t *testing.T) {
	p := loaded(t, 100000, 60000)
	var order []string
	batches := 0
	r, err := p.Advance(60000, func(em *emit, run func(outFn) error) (string, error) {
		order = append(order, em.key)
		return "ok", nil
	}, AdvanceOpts{MaxUnits: -1, OnBatch: func(int) (string, error) { batches++; return "ok", nil }})
	if err != nil || r != "ok" || len(order) != 100000 || p.Open() != 0 {
		t.Fatal(r, err, len(order), p.Open())
	}
	for i, k := range order {
		if k != fmt.Sprintf("k%07d", i) {
			t.Fatal("the closes keep the heap order across batches", i, k)
		}
	}
	if batches != 50 {
		t.Fatal("batches of CloseBatch", batches)
	}
}

func TestFeedAdvanceStopsAtMaxUnits(t *testing.T) {
	p := loaded(t, 5000, 60000)
	n := 0
	r, err := p.Advance(60000, func(*emit, func(outFn) error) (string, error) { n++; return "ok", nil }, AdvanceOpts{MaxUnits: 2345})
	if err != nil || r != "ok" || n != 2345 || p.Open() != 5000-2345 {
		t.Fatal(r, err, n, p.Open())
	}
}

func TestFeedALoadedStateOf500000EntriesIsIndexedAgain(t *testing.T) {
	p := loaded(t, 500000, 60000)
	if p.Open() != 500000 {
		t.Fatal(p.Open())
	}
	if d, ok := p.NextDue(); !ok || d != 60000 {
		t.Fatal(d, ok)
	}
}

// memBroker is a KV in memory, enough for writeLayer and readLayer.
type memBroker struct {
	kv map[string]interface{}
}

func (m *memBroker) NewReader(feed.Options) (*feed.Reader, error) { return nil, errors.New("no feed") }
func (m *memBroker) KVGet(_ context.Context, ns, key string) (KVRow, error) {
	v, ok := m.kv[ns+"\x00"+key]
	return KVRow{Key: key, Found: ok, Value: v, Version: 1}, nil
}
func (m *memBroker) KVGetMany(_ context.Context, ns string, keys []string) (KVMany, error) {
	var out KVMany
	for _, k := range keys {
		if v, ok := m.kv[ns+"\x00"+k]; ok {
			out.Rows = append(out.Rows, KVRow{Key: k, Found: true, Value: jsv.JSONClone(v), Version: 1})
		} else {
			out.Missing = append(out.Missing, k)
		}
	}
	return out, nil
}
func (m *memBroker) KVGetPrefix(_ context.Context, ns, prefix string, _ bool, limit int, after string) (KVPage, error) {
	var keys []string
	for k := range m.kv {
		if strings.HasPrefix(k, ns+"\x00"+prefix) && k[len(ns)+1:] > after {
			keys = append(keys, k[len(ns)+1:])
		}
	}
	sort.Strings(keys)
	var out KVPage
	if len(keys) > limit {
		keys, out.Truncated = keys[:limit], true
		out.NextAfter = keys[len(keys)-1]
	}
	for _, k := range keys {
		out.Rows = append(out.Rows, KVRow{Key: k, Found: true})
	}
	return out, nil
}
func (m *memBroker) Transaction() Tx                                               { return &memTx{b: m} }
func (m *memBroker) Lock(string, time.Duration, time.Duration, time.Duration) Lock { return nil }
func (m *memBroker) Ephemeral() Ephemeral                                          { return nil }

type memTx struct {
	b   *memBroker
	ops []func()
}

func (t *memTx) Guard(Lock) Tx                                            { return t }
func (t *memTx) Push(string, string, []PushItem) Tx                       { return t }
func (t *memTx) KVCheck(string, string, int64, bool) Tx                   { return t }
func (t *memTx) KVPutIfAbsent(string, string, interface{}, bool, bool) Tx { return t }
func (t *memTx) KVPut(ns, key string, value interface{}, _ *int64, _, _ bool) Tx {
	v := jsv.JSONClone(value)
	t.ops = append(t.ops, func() { t.b.kv[ns+"\x00"+key] = v })
	return t
}
func (t *memTx) KVDelete(ns, key string) Tx {
	t.ops = append(t.ops, func() { delete(t.b.kv, ns+"\x00"+key) })
	return t
}
func (t *memTx) Commit(context.Context) (TxResult, error) {
	for _, op := range t.ops {
		op()
	}
	return TxResult{Success: true}, nil
}

func TestFeedABigLayerIsWrittenInPiecesAndReadsBackWhole(t *testing.T) {
	b := &memBroker{kv: map[string]interface{}{}}
	const N = 200000
	entries := make([]KV, N)
	for i := 0; i < N; i++ {
		k := fmt.Sprintf("k%d", i)
		entries[i] = KV{K: "w\u001f0\u001f" + k, V: map[string]interface{}{"key": k, "start": 0.0, "end": 60000.0, "due": 60000.0, "acc": map[string]interface{}{"count": float64(i % 7)}}}
	}
	w, err := writeLayer(context.Background(), b, PositionNamespace, "big", "s1", nil, []layerStep{{kind: "window", entries: entries}}, DefaultChunkChars, DefaultPieceBytes)
	if err != nil || w.entries != N || w.chunks <= 10 {
		t.Fatal(err, w)
	}
	got := map[string]interface{}{}
	pieces := 0
	err = readLayer(context.Background(), b, PositionNamespace, "big", "s1", w.chunks, func(pc piece) error {
		pieces++
		for _, kv := range pc.set {
			pair := kv.([]interface{})
			got[pair[0].(string)] = pair[1]
		}
		return nil
	})
	if err != nil || pieces <= 10 || len(got) != N {
		t.Fatal(err, pieces, len(got))
	}
	want := map[string]interface{}{"key": "k12345", "start": 0.0, "end": 60000.0, "due": 60000.0, "acc": map[string]interface{}{"count": float64(12345 % 7)}}
	if !reflect.DeepEqual(got["w\u001f0\u001fk12345"], want) {
		t.Fatal(got["w\u001f0\u001fk12345"])
	}
}

func TestFeedAPieceBiggerThanOneKVValueGoesOnInTheNextAndDeletesAreReadBack(t *testing.T) {
	b := &memBroker{kv: map[string]interface{}{}}
	// Hex of hashes: deflate saves about half of it, so the one piece spans several chunks.
	var hashes []string
	for i := 0; i < 64; i++ {
		hashes = append(hashes, fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprint(i)))))
	}
	big := strings.Join(hashes, "")
	list := []KV{{K: "a", V: map[string]interface{}{"big": big, "n": 1.0}}, {K: "gone", V: Undefined}, {K: "b", V: map[string]interface{}{"n": 2.0}}}
	w, err := writeLayer(context.Background(), b, PositionNamespace, "small", "s1", nil, []layerStep{{kind: "pattern", entries: list}}, 1024, 1<<20)
	if err != nil || w.chunks < 2 {
		t.Fatal("the piece spans several chunks", err, w.chunks)
	}
	for n := 0; n < w.chunks; n++ {
		v := b.kv[PositionNamespace+"\x00"+chunkKey("small", "s1", n)]
		if d := jsv.Get(v, "d").(string); len(d) > 1024 {
			t.Fatal(len(d))
		}
		more := jsv.Get(v, "m")
		if f, _ := jsv.Num(more); n < w.chunks-1 && f != 1 {
			t.Fatalf("chunk %d says more follows: %v", n, more)
		}
		if n == w.chunks-1 && !jsv.IsUndefined(more) {
			t.Fatalf("the last chunk ends the piece: %v", more)
		}
	}
	var seen []piece
	if err := readLayer(context.Background(), b, PositionNamespace, "small", "s1", w.chunks, func(pc piece) error { seen = append(seen, pc); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 1 || seen[0].k != "pattern" {
		t.Fatal(seen)
	}
	if !reflect.DeepEqual(seen[0].set, []interface{}{[]interface{}{"a", map[string]interface{}{"big": big, "n": 1.0}}, []interface{}{"b", map[string]interface{}{"n": 2.0}}}) ||
		!reflect.DeepEqual(seen[0].del, []interface{}{"gone"}) {
		t.Fatal(seen[0])
	}
}

func TestFeedThePieceLimitCountsUTF8BytesNotCharacters(t *testing.T) {
	b := &memBroker{kv: map[string]interface{}{}}
	// 200 characters, 600 bytes in UTF-8: counted in characters, a piece would hold three times the limit.
	text := strings.Repeat("漢字", 100)
	var list []KV
	var want []interface{}
	for i := 0; i < 12; i++ {
		k := fmt.Sprintf("k%d", i)
		list = append(list, KV{K: k, V: map[string]interface{}{"text": text}})
		want = append(want, []interface{}{k, map[string]interface{}{"text": text}})
	}
	const room = 2048
	w, err := writeLayer(context.Background(), b, PositionNamespace, "utf8", "s1", nil, []layerStep{{kind: "window", entries: list}}, DefaultChunkChars, room)
	if err != nil {
		t.Fatal(err)
	}
	var pieces []piece
	if err := readLayer(context.Background(), b, PositionNamespace, "utf8", "s1", w.chunks, func(pc piece) error { pieces = append(pieces, pc); return nil }); err != nil {
		t.Fatal(err)
	}
	var got []interface{}
	for _, pc := range pieces {
		got = append(got, pc.set...)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatal("every entry once, in order")
	}
	if len(pieces) < 3 {
		t.Fatalf("%d pieces", len(pieces))
	}
	for _, pc := range pieces {
		// A piece ends at the entry that takes it to the limit: the entries before that one stay under it.
		before := 0
		for _, e := range pc.set[:len(pc.set)-1] {
			before += len(jsv.MustMarshal(e))
		}
		if before >= room {
			t.Fatalf("%d bytes before the last entry of a piece (limit %d)", before, room)
		}
	}
}

func TestFeedAMissingChunkIsFatal(t *testing.T) {
	b := &memBroker{kv: map[string]interface{}{}}
	_, err := writeLayer(context.Background(), b, PositionNamespace, "x", "s1", nil, []layerStep{{kind: "window", entries: []KV{{K: "a", V: 1.0}}}}, DefaultChunkChars, DefaultPieceBytes)
	if err != nil {
		t.Fatal(err)
	}
	err = readLayer(context.Background(), b, PositionNamespace, "x", "s1", 2, func(piece) error { return nil })
	var se *SnapshotError
	if !errors.As(err, &se) || !se.Fatal || se.Code != "feed_snapshot_missing" {
		t.Fatal(err)
	}
}

func TestFeedAPieceIsTheJSPieceText(t *testing.T) {
	// {"v":2,"s":0,"k":"window","set":[["w\u001f0\u001fk",{...}]],"del":[]} before deflate.
	b := &memBroker{kv: map[string]interface{}{}}
	w, err := writeLayer(context.Background(), b, PositionNamespace, "t", "s", nil,
		[]layerStep{{kind: "window", entries: []KV{{K: "w\u001f0\u001fk", V: jsv.Ordered{{K: "key", V: "k"}, {K: "due", V: 60.0}}}}}}, DefaultChunkChars, DefaultPieceBytes)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"v":2,"s":0,"k":"window","set":[["w\u001f0\u001fk",{"key":"k","due":60}]],"del":[]}`
	if w.bytes != len(want) {
		t.Fatal(w.bytes, len(want))
	}
}

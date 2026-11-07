package feed

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A page as the broker writes it: n entries whose payloads hold the strings a
// split must not cut at (quotes, escapes, brackets and commas inside strings).
func testPage(n int) []byte {
	var b strings.Builder
	b.WriteString(`{"applied":` + fmt.Sprint(100+n) + `,"caughtUp":false,"entries":[`)
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteString(",\n ")
		}
		payload := map[string]interface{}{
			"i": float64(i), "s": fmt.Sprintf(`a "quote" \ back, [not] {an} array %d ☃`, i),
			"nested": map[string]interface{}{"list": []interface{}{1.0, "]", "}", nil, true}},
		}
		pj, _ := json.Marshal(payload)
		fmt.Fprintf(&b, `{"id":"m%d","kind":"push","offset":%d,"partition":"p%d","partitionId":"u%d","payload":%s,"position":%d,"queue":"q","transactionId":"t%d","ts":"2026-11-07T10:00:00.000001Z"}`,
			i, i, i%7, i%7, pj, 10+i/3, i)
	}
	b.WriteString(`],"next":` + fmt.Sprint(11+n/3) + `,"resolvedTs":"2026-11-07T10:00:00.000001Z"}`)
	return []byte(b.String())
}

func TestASplitPageDecodesAsTheWholePage(t *testing.T) {
	body := testPage(5000)
	if len(body) < splitMinBytes {
		t.Fatalf("the page is %d bytes: too small to split", len(body))
	}
	split, ok := decodeSplit(body, 4)
	if !ok {
		t.Fatal("a page the broker writes is split")
	}
	var a wireAnswer
	if err := json.Unmarshal(body, &a); err != nil {
		t.Fatal(err)
	}
	whole := pageOf(&a, a.Entries)
	if !reflect.DeepEqual(split, whole) {
		t.Fatal("the split decode differs from the whole one")
	}
	if len(split.Entries) != 5000 || split.Next != 11+5000/3 || split.CaughtUp || *split.Applied != 5100 {
		t.Fatalf("page %d entries, next %d, caughtUp %v", len(split.Entries), split.Next, split.CaughtUp)
	}
	got, err := decodePage(body)
	if err != nil || !reflect.DeepEqual(got, whole) {
		t.Fatalf("decodePage: %v", err)
	}
}

func TestAnAnswerTheSplitDoesNotExpectIsDecodedWhole(t *testing.T) {
	big := testPage(3000)
	// Keys in another order and an empty page: still found or left whole, same answer.
	reordered := []byte(`{"next":5,"entries":` + string(big[strings.Index(string(big), "["):strings.LastIndex(string(big), "]")+1]) + `,"caughtUp":true}`)
	for name, body := range map[string][]byte{"reordered": reordered, "empty": []byte(`{"entries":[],"next":3,"caughtUp":true}`)} {
		var a wireAnswer
		if err := json.Unmarshal(body, &a); err != nil {
			t.Fatal(name, err)
		}
		got, err := decodePage(body)
		if err != nil || !reflect.DeepEqual(got, pageOf(&a, a.Entries)) {
			t.Fatalf("%s: %v", name, err)
		}
	}
	// A page cut short is refused, whichever way it is decoded.
	if _, err := decodePage(big[:len(big)-40]); err == nil {
		t.Fatal("a broken page decodes")
	}
}

type countingPoster struct {
	pages  map[string][]byte
	calls  atomic.Int64
	mu     sync.Mutex
	byFrom map[string]int
}

func (p *countingPoster) PostFeed(_ context.Context, body []byte, _ time.Duration) ([]byte, error) {
	p.calls.Add(1)
	var req struct {
		From interface{} `json:"from"`
	}
	_ = json.Unmarshal(body, &req)
	from := fmt.Sprint(req.From)
	p.mu.Lock()
	if p.byFrom == nil {
		p.byFrom = map[string]int{}
	}
	p.byFrom[from]++
	p.mu.Unlock()
	return p.pages[from], nil
}

// readsAt is how many reads asked for the page at `from`.
func (p *countingPoster) readsAt(from string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.byFrom[from]
}

func TestTheNextPageIsReadAheadAndASeekDropsIt(t *testing.T) {
	p := &countingPoster{pages: map[string][]byte{
		"earliest": []byte(`{"entries":[{"position":1,"payload":{"n":1}}],"next":2,"caughtUp":false}`),
		"2":        []byte(`{"entries":[{"position":2,"payload":{"n":2}}],"next":3,"caughtUp":false}`),
		"3":        []byte(`{"entries":[],"next":3,"caughtUp":true}`),
		"1":        []byte(`{"entries":[{"position":1,"payload":{"n":1}}],"next":2,"caughtUp":false}`),
	}}
	r, err := NewReader(p, nil, "http", Options{From: "earliest"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	zero := 0
	first, err := r.Read(ctx, &zero)
	if err != nil || first.Entries[0].Position != 1 {
		t.Fatalf("first read: %v %+v", err, first)
	}
	// The page at 2 is read while the caller works on the first one.
	deadline := time.Now().Add(2 * time.Second)
	for p.calls.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if p.calls.Load() != 2 {
		t.Fatalf("%d calls: the next page was not read ahead", p.calls.Load())
	}
	second, err := r.Read(ctx, &zero)
	if err != nil || second.Entries[0].Position != 2 || second.Next != 3 {
		t.Fatalf("second read: %v %+v", err, second)
	}
	// A seek drops the page read ahead (at 3) and reads where it was sent.
	for p.calls.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	r.Seek(int64(1))
	again, err := r.Read(ctx, &zero)
	if err != nil || again.Entries[0].Position != 1 || again.Next != 2 {
		t.Fatalf("after the seek: %v %+v", err, again)
	}
	// At the tail: one read of the page at 3, and nothing read ahead of a caught-up page.
	// (The read at 1 started a read ahead of 2, which the seek dropped: it may still land.)
	r.Seek(int64(3))
	before := p.readsAt("3")
	tail, err := r.Read(ctx, &zero)
	if err != nil || !tail.CaughtUp || len(tail.Entries) != 0 || p.readsAt("3") != before+1 {
		t.Fatalf("at the tail: %v %+v, %d reads at 3", err, tail, p.readsAt("3")-before)
	}
	r.Close()
}

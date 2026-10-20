package queen

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// Fetch and ChangedSince against a fake broker that answers POST /api/v1/fetch
// and POST /api/v1/partitions/changed the way
// server/src/rsm/facade/real/phase2/reads.rs does: entries answered in order,
// each entry's byte budget and the whole call's ceiling (the first record of
// an entry read whatever it weighs), a record cap per entry,
// OFFSET_OUT_OF_RANGE below logStartOffset and above the high watermark,
// UNKNOWN_TOPIC_OR_PARTITION for a queue that does not exist, discovery pages
// with a next cursor exactly when a page is full, BAD_CURSOR for a cursor it
// did not issue. The requests are recorded as they arrived on the socket.

const fakeTS = "2026-10-20T10:00:00.000000Z"

type fakeRecord struct {
	Offset        int64       `json:"offset"`
	TransactionID string      `json:"transactionId"`
	Payload       interface{} `json:"payload"`
	Ts            string      `json:"ts"`
}

type fakePartition struct {
	records     []fakeRecord
	logStart    int64
	created     int
	lastWriteAt string
}

type fakeHit struct {
	path string
	body map[string]interface{}
}

type fakeBroker struct {
	mu              sync.Mutex
	queues          map[string]map[string]*fakePartition
	recordsPerEntry int
	safeTimes       []string
	created         int
	hits            []fakeHit
	afterFetch      func(*fakeBroker)
	fetchOverride   func(map[string]interface{}) (int, interface{})
	changedOverride func(map[string]interface{}) (int, interface{})
}

func newFakeBroker() *fakeBroker {
	return &fakeBroker{queues: map[string]map[string]*fakePartition{}, recordsPerEntry: 10_000}
}

func (b *fakeBroker) write(queue, partition string, payloads []interface{}, lastWriteAt string) {
	q, ok := b.queues[queue]
	if !ok {
		q = map[string]*fakePartition{}
		b.queues[queue] = q
	}
	p, ok := q[partition]
	if !ok {
		p = &fakePartition{created: b.created, lastWriteAt: lastWriteAt}
		b.created++
		q[partition] = p
	}
	for _, payload := range payloads {
		offset := p.logStart
		if n := len(p.records); n > 0 {
			offset = p.records[n-1].Offset + 1
		}
		p.records = append(p.records, fakeRecord{offset, fmt.Sprintf("%s-%d", partition, offset), payload, fakeTS})
	}
	p.lastWriteAt = lastWriteAt
}

func (b *fakeBroker) trim(queue, partition string, offset int64) {
	p := b.queues[queue][partition]
	kept := p.records[:0]
	for _, r := range p.records {
		if r.Offset >= offset {
			kept = append(kept, r)
		}
	}
	p.records = kept
	p.logStart = offset
}

func num(v interface{}) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case json.Number:
		i, _ := n.Int64()
		return i
	}
	return 0
}

func (b *fakeBroker) fetch(body map[string]interface{}) (int, interface{}) {
	asks := body["entries"].([]interface{})
	if len(asks) > 1024 {
		return 400, map[string]string{"error": "too many entries"}
	}
	ceiling, hasCeiling := body["maxBytes"]
	var used int64
	entries := []map[string]interface{}{}
	for _, a := range asks {
		e := a.(map[string]interface{})
		partition, _ := e["partition"].(string)
		out := map[string]interface{}{"queue": e["queue"], "partition": partition, "records": []fakeRecord{}, "highWatermark": 0, "logStartOffset": 0}
		q, ok := b.queues[e["queue"].(string)]
		if !ok {
			out["error"] = "UNKNOWN_TOPIC_OR_PARTITION"
			entries = append(entries, out)
			continue
		}
		p, ok := q[partition]
		if !ok {
			entries = append(entries, out)
			continue
		}
		last := p.logStart - 1
		if n := len(p.records); n > 0 {
			last = p.records[n-1].Offset
		}
		high := last + 1
		out["highWatermark"], out["logStartOffset"] = high, p.logStart
		offset := num(e["offset"])
		if offset < p.logStart || offset > high {
			out["error"] = "OFFSET_OUT_OF_RANGE"
			entries = append(entries, out)
			continue
		}
		budget := num(e["maxBytes"])
		if hasCeiling {
			if used >= num(ceiling) {
				entries = append(entries, out)
				continue
			}
			budget = min(budget, num(ceiling)-used)
		}
		var spent int64
		records := []fakeRecord{}
		for _, r := range p.records {
			if r.Offset < offset {
				continue
			}
			if len(records) >= b.recordsPerEntry {
				break
			}
			raw, _ := json.Marshal(r.Payload)
			weight := max(int64(len(raw)), 1)
			if len(records) > 0 && spent+weight > budget {
				break
			}
			spent += weight
			records = append(records, r)
		}
		used += spent
		out["records"] = records
		entries = append(entries, out)
	}
	return 200, map[string]interface{}{"entries": entries}
}

var cursorRE = regexp.MustCompile(`^p\|(\d+)$`)

func (b *fakeBroker) changed(body map[string]interface{}) (int, interface{}) {
	safeTime := "2026-10-20T12:00:00.000000Z"
	if len(b.safeTimes) > 1 {
		safeTime, b.safeTimes = b.safeTimes[0], b.safeTimes[1:]
	} else if len(b.safeTimes) == 1 {
		safeTime = b.safeTimes[0]
	}
	entries := []map[string]interface{}{}
	for _, a := range body["entries"].([]interface{}) {
		e := a.(map[string]interface{})
		queue := e["queue"].(string)
		q, ok := b.queues[queue]
		if !ok {
			entries = append(entries, map[string]interface{}{"queue": queue, "error": "UNKNOWN_TOPIC_OR_PARTITION"})
			continue
		}
		from := 0
		if after, ok := e["after"].(string); ok && after != "" {
			m := cursorRE.FindStringSubmatch(after)
			if m == nil {
				entries = append(entries, map[string]interface{}{"queue": queue, "error": "BAD_CURSOR"})
				continue
			}
			n, _ := strconv.Atoi(m[1])
			from = n + 1
		}
		limit := int(num(e["limit"]))
		if limit < 1 || limit > 1000 {
			limit = 1000
		}
		names := make([]string, 0, len(q))
		for name := range q {
			names = append(names, name)
		}
		sort.Slice(names, func(i, j int) bool { return q[names[i]].created < q[names[j]].created })
		since, _ := e["since"].(string)
		partitions := []map[string]interface{}{}
		last := 0
		for _, name := range names {
			p := q[name]
			if p.created < from || (since != "" && p.lastWriteAt < since) {
				continue
			}
			lastOffset := p.logStart - 1
			if n := len(p.records); n > 0 {
				lastOffset = p.records[n-1].Offset
			}
			partitions = append(partitions, map[string]interface{}{"name": name, "id": "id-" + name, "lastOffset": lastOffset, "logStart": p.logStart, "lastWriteAt": p.lastWriteAt})
			last = p.created
			if len(partitions) >= limit {
				break
			}
		}
		var next interface{}
		if len(partitions) >= limit {
			next = fmt.Sprintf("p|%d", last)
		}
		entries = append(entries, map[string]interface{}{"queue": queue, "partitions": partitions, "next": next})
	}
	return 200, map[string]interface{}{"safeTime": safeTime, "safeTimeDegraded": false, "entries": entries}
}

func (b *fakeBroker) serve(t *testing.T) *Queen {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var body map[string]interface{}
		_ = json.Unmarshal(raw, &body)
		b.mu.Lock()
		defer b.mu.Unlock()
		b.hits = append(b.hits, fakeHit{r.URL.Path, body})
		status, answer := 404, interface{}(map[string]string{"error": "not found"})
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/fetch":
			if b.fetchOverride != nil {
				status, answer = b.fetchOverride(body)
			} else {
				status, answer = b.fetch(body)
			}
			if b.afterFetch != nil {
				b.afterFetch(b)
			}
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/partitions/changed":
			if b.changedOverride != nil {
				status, answer = b.changedOverride(body)
			} else {
				status, answer = b.changed(body)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(answer)
	}))
	t.Cleanup(srv.Close)
	q, err := New(ClientConfig{URL: srv.URL, RetryAttempts: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = q.Close(context.Background()) })
	return q
}

func (b *fakeBroker) fetchBodies() []map[string]interface{} {
	var out []map[string]interface{}
	for _, h := range b.hits {
		if h.path == "/api/v1/fetch" {
			out = append(out, h.body)
		}
	}
	return out
}

func payloadsN(n int) []interface{} {
	out := make([]interface{}, n)
	for i := range out {
		out[i] = map[string]int{"n": i}
	}
	return out
}

func offsetsOf(p FetchedPartition) []int64 {
	out := []int64{}
	for _, r := range p.Records {
		out = append(out, r.Offset)
	}
	return out
}

func firstOffsets(bodies []map[string]interface{}) []int64 {
	out := []int64{}
	for _, b := range bodies {
		out = append(out, num(b["entries"].([]interface{})[0].(map[string]interface{})["offset"]))
	}
	return out
}

func eq(t *testing.T, got, want interface{}, what string) {
	t.Helper()
	g, _ := json.Marshal(got)
	w, _ := json.Marshal(want)
	if string(g) != string(w) {
		t.Fatalf("%s: got %s, want %s", what, g, w)
	}
}

func TestFetchReadsEveryRecordFromOffsetZeroInOneCallWhenTheyFit(t *testing.T) {
	b := newFakeBroker()
	b.write("tf.events", "trace-1", payloadsN(3), fakeTS)
	b.write("tf.events", "trace-2", []interface{}{map[string]int{"a": 1}}, fakeTS)
	q := b.serve(t)

	read, err := q.Queue("tf.events").Fetch(context.Background(), []string{"trace-1", "trace-2"})
	if err != nil {
		t.Fatal(err)
	}
	t1 := read["trace-1"]
	eq(t, offsetsOf(t1), []int64{0, 1, 2}, "offsets")
	if string(t1.Records[1].Payload) != `{"n":1}` || t1.Records[0].TransactionID != "trace-1-0" {
		t.Fatalf("record: %+v", t1.Records[1])
	}
	if !t1.Records[0].Ts.Equal(time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC)) {
		t.Fatalf("ts: %v", t1.Records[0].Ts)
	}
	if t1.Partition != "trace-1" || t1.HighWatermark != 3 || t1.LogStartOffset != 0 || t1.NextOffset != 3 {
		t.Fatalf("bounds: %+v", t1)
	}
	if string(read["trace-2"].Records[0].Payload) != `{"a":1}` {
		t.Fatalf("trace-2: %+v", read["trace-2"])
	}
	bodies := b.fetchBodies()
	if len(bodies) != 1 {
		t.Fatalf("calls: %d", len(bodies))
	}
	eq(t, bodies[0], map[string]interface{}{
		"entries": []map[string]interface{}{
			{"queue": "tf.events", "partition": "trace-1", "offset": 0, "maxBytes": 8 << 20},
			{"queue": "tf.events", "partition": "trace-2", "offset": 0, "maxBytes": 8 << 20},
		},
		"maxBytes": 8 << 20,
	}, "request body")
}

func TestFetchAsksAgainUntilTheHighWatermarkOfTheFirstAnswer(t *testing.T) {
	b := newFakeBroker()
	b.recordsPerEntry = 3
	b.write("q", "p", payloadsN(10), fakeTS)
	q := b.serve(t)
	read, err := q.Queue("q").Fetch(context.Background(), []string{"p"})
	if err != nil {
		t.Fatal(err)
	}
	eq(t, offsetsOf(read["p"]), []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, "offsets")
	eq(t, firstOffsets(b.fetchBodies()), []int64{0, 3, 6, 9}, "asked offsets")
	if read["p"].NextOffset != 10 {
		t.Fatalf("next: %d", read["p"].NextOffset)
	}
}

func TestFetchDoesNotChaseAPartitionWrittenDuringTheRead(t *testing.T) {
	b := newFakeBroker()
	b.recordsPerEntry = 2
	b.write("q", "p", payloadsN(4), fakeTS)
	b.afterFetch = func(b *fakeBroker) { b.write("q", "p", []interface{}{"late"}, fakeTS) }
	q := b.serve(t)
	read, err := q.Queue("q").Fetch(context.Background(), []string{"p"})
	if err != nil {
		t.Fatal(err)
	}
	eq(t, offsetsOf(read["p"]), []int64{0, 1, 2, 3}, "offsets")
	if len(b.fetchBodies()) != 2 || read["p"].NextOffset != 4 {
		t.Fatalf("calls %d next %d", len(b.fetchBodies()), read["p"].NextOffset)
	}
}

func TestFetchAsksForAtMost1000PartitionsPerCall(t *testing.T) {
	b := newFakeBroker()
	names := make([]string, 2500)
	for i := range names {
		names[i] = fmt.Sprintf("p-%d", i)
		b.write("q", names[i], []interface{}{names[i]}, fakeTS)
	}
	q := b.serve(t)
	read, err := q.Queue("q").Fetch(context.Background(), names)
	if err != nil {
		t.Fatal(err)
	}
	sizes := []int{}
	for _, body := range b.fetchBodies() {
		sizes = append(sizes, len(body["entries"].([]interface{})))
	}
	eq(t, sizes, []int{1000, 1000, 500}, "entries per call")
	if len(read) != 2500 {
		t.Fatalf("partitions: %d", len(read))
	}
	for _, name := range names {
		if got := string(read[name].Records[0].Payload); got != strconv.Quote(name) {
			t.Fatalf("%s: %s", name, got)
		}
	}
}

func TestFetchAsksAgainForThePartitionsTheCallCeilingLeftEmpty(t *testing.T) {
	b := newFakeBroker()
	fat := strings.Repeat("x", 100)
	for _, name := range []string{"a", "b", "c"} {
		b.write("q", name, []interface{}{fat, fat}, fakeTS)
	}
	q := b.serve(t)
	read, err := q.Queue("q").Fetch(context.Background(), []string{"a", "b", "c"}, FetchOptions{MaxBytes: 250})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"a", "b", "c"} {
		eq(t, offsetsOf(read[name]), []int64{0, 1}, name)
	}
	if len(b.fetchBodies()) < 2 {
		t.Fatalf("calls: %d", len(b.fetchBodies()))
	}
	for _, body := range b.fetchBodies() {
		if num(body["maxBytes"]) != 250 {
			t.Fatalf("ceiling: %v", body["maxBytes"])
		}
	}
}

func TestFetchReadsFromLogStartWhenRetentionDeletedTheStart(t *testing.T) {
	b := newFakeBroker()
	b.write("q", "p", payloadsN(10), fakeTS)
	b.trim("q", "p", 6)
	q := b.serve(t)
	read, err := q.Queue("q").Fetch(context.Background(), []string{"p"})
	if err != nil {
		t.Fatal(err)
	}
	eq(t, offsetsOf(read["p"]), []int64{6, 7, 8, 9}, "offsets")
	eq(t, firstOffsets(b.fetchBodies()), []int64{0, 6}, "asked offsets")
	if read["p"].LogStartOffset != 6 {
		t.Fatalf("log start: %d", read["p"].LogStartOffset)
	}
}

func TestFetchStartsEachPartitionAtItsOffset(t *testing.T) {
	b := newFakeBroker()
	b.write("q", "a", payloadsN(5), fakeTS)
	b.write("q", "b", payloadsN(5), fakeTS)
	q := b.serve(t)
	read, err := q.Queue("q").Fetch(context.Background(), []string{"a", "b"}, FetchOptions{Offsets: map[string]int64{"a": 3}})
	if err != nil {
		t.Fatal(err)
	}
	eq(t, offsetsOf(read["a"]), []int64{3, 4}, "a")
	eq(t, offsetsOf(read["b"]), []int64{0, 1, 2, 3, 4}, "b")
	again, err := q.Queue("q").Fetch(context.Background(), []string{"a"}, FetchOptions{Offsets: map[string]int64{"a": read["a"].NextOffset}})
	if err != nil {
		t.Fatal(err)
	}
	if len(again["a"].Records) != 0 || again["a"].NextOffset != 5 {
		t.Fatalf("again: %+v", again["a"])
	}
}

func TestFetchAnswersNoRecordsForAMissingQueueAnUnwrittenPartitionAndAnOffsetPastTheEnd(t *testing.T) {
	b := newFakeBroker()
	b.write("q", "p", payloadsN(2), fakeTS)
	q := b.serve(t)
	missing, err := q.Queue("nope").Fetch(context.Background(), []string{"p"})
	if err != nil || len(missing["p"].Records) != 0 {
		t.Fatalf("missing queue: %v %+v", err, missing)
	}
	read, err := q.Queue("q").Fetch(context.Background(), []string{"never", "p"}, FetchOptions{Offsets: map[string]int64{"p": 7}})
	if err != nil {
		t.Fatal(err)
	}
	if len(read["never"].Records) != 0 || len(read["p"].Records) != 0 || read["p"].NextOffset != 7 || read["p"].HighWatermark != 2 {
		t.Fatalf("read: %+v", read)
	}
}

func TestFetchFailsOnAnErrorMarkerItDoesNotKnow(t *testing.T) {
	b := newFakeBroker()
	b.fetchOverride = func(map[string]interface{}) (int, interface{}) {
		return 200, json.RawMessage(`{"entries":[{"queue":"q","partition":"p","records":[],"highWatermark":1,"logStartOffset":0,"error":"SOMETHING_NEW"}]}`)
	}
	q := b.serve(t)
	_, err := q.Queue("q").Fetch(context.Background(), []string{"p"})
	if err == nil || !strings.Contains(err.Error(), "SOMETHING_NEW") {
		t.Fatalf("err: %v", err)
	}
}

func TestFetchStopsWhenACallReadsNothingAlthoughThePartitionIsNotAtItsEnd(t *testing.T) {
	b := newFakeBroker()
	b.fetchOverride = func(map[string]interface{}) (int, interface{}) {
		return 200, json.RawMessage(`{"entries":[{"queue":"q","partition":"p","records":[],"highWatermark":5,"logStartOffset":0}]}`)
	}
	q := b.serve(t)
	read, err := q.Queue("q").Fetch(context.Background(), []string{"p"})
	if err != nil || len(read["p"].Records) != 0 || len(b.fetchBodies()) != 1 {
		t.Fatalf("err %v read %+v calls %d", err, read, len(b.fetchBodies()))
	}
}

func TestFetchAsksOnceForANameGivenTwiceAndRefusesBadInputBeforeAnyCall(t *testing.T) {
	b := newFakeBroker()
	b.write("q", "p", payloadsN(1), fakeTS)
	q := b.serve(t)
	read, err := q.Queue("q").Fetch(context.Background(), []string{"p", "p"})
	if err != nil || len(read) != 1 {
		t.Fatalf("err %v read %+v", err, read)
	}
	if n := len(b.fetchBodies()[0]["entries"].([]interface{})); n != 1 {
		t.Fatalf("entries: %d", n)
	}
	if _, err := q.Queue("q").Fetch(context.Background(), []string{""}); err == nil {
		t.Fatal("an empty name was accepted")
	}
	if _, err := q.Queue("q").Fetch(context.Background(), []string{"p"}, FetchOptions{Offsets: map[string]int64{"p": -1}}); err == nil {
		t.Fatal("a negative offset was accepted")
	}
	if _, err := q.Queue("q").Fetch(context.Background(), []string{"p"}, FetchOptions{MaxBytes: -1}); err == nil {
		t.Fatal("a negative MaxBytes was accepted")
	}
	if len(b.fetchBodies()) != 1 {
		t.Fatalf("calls: %d", len(b.fetchBodies()))
	}
}

func TestChangedSinceFollowsThePageCursorToTheEndOfThePass(t *testing.T) {
	b := newFakeBroker()
	for i := 0; i < 2300; i++ {
		b.write("tf.events", fmt.Sprintf("t-%d", i), []interface{}{i}, fakeTS)
	}
	q := b.serve(t)
	since := time.Date(2026, 10, 20, 9, 0, 0, 0, time.FixedZone("CEST", 2*3600))
	changed, err := q.Queue("tf.events").ChangedSince(context.Background(), since)
	if err != nil {
		t.Fatal(err)
	}
	if len(changed.Partitions) != 2300 || changed.Partitions[0].Name != "t-0" || changed.Partitions[2299].Name != "t-2299" {
		t.Fatalf("partitions: %d", len(changed.Partitions))
	}
	p0 := changed.Partitions[0]
	if p0.ID != "id-t-0" || p0.LastOffset != 0 || p0.LogStart != 0 || !p0.LastWriteAt.Equal(time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC)) {
		t.Fatalf("p0: %+v", p0)
	}
	if !changed.SafeTime.Equal(time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)) {
		t.Fatalf("safeTime: %v", changed.SafeTime)
	}
	afters := []interface{}{}
	for _, h := range b.hits {
		e := h.body["entries"].([]interface{})[0].(map[string]interface{})
		afters = append(afters, e["after"])
		if e["since"] != "2026-10-20T07:00:00.000000Z" || num(e["limit"]) != 1000 || e["queue"] != "tf.events" {
			t.Fatalf("entry: %v", e)
		}
	}
	eq(t, afters, []interface{}{nil, "p|999", "p|1999"}, "cursors")
}

func TestChangedSinceListsOnlyWhatWasWrittenAtOrAfterSince(t *testing.T) {
	b := newFakeBroker()
	b.write("q", "old", []interface{}{1}, "2026-10-20T08:00:00.000000Z")
	b.write("q", "new", []interface{}{1}, "2026-10-20T10:30:00.000000Z")
	q := b.serve(t)
	changed, err := q.Queue("q").ChangedSince(context.Background(), time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC))
	if err != nil || len(changed.Partitions) != 1 || changed.Partitions[0].Name != "new" {
		t.Fatalf("err %v changed %+v", err, changed)
	}
}

func TestChangedSinceWithAZeroSinceListsEveryPartitionAndSendsNoSince(t *testing.T) {
	b := newFakeBroker()
	b.write("q", "a", []interface{}{1}, "2020-01-01T00:00:00.000000Z")
	q := b.serve(t)
	changed, err := q.Queue("q").ChangedSince(context.Background(), time.Time{})
	if err != nil || len(changed.Partitions) != 1 {
		t.Fatalf("err %v changed %+v", err, changed)
	}
	if _, sent := b.hits[0].body["entries"].([]interface{})[0].(map[string]interface{})["since"]; sent {
		t.Fatal("a zero since was sent")
	}
}

func TestChangedSinceAnswersTheSmallestSafeTimeOfItsPages(t *testing.T) {
	b := newFakeBroker()
	for i := 0; i < 1500; i++ {
		b.write("q", fmt.Sprintf("p-%d", i), []interface{}{1}, fakeTS)
	}
	b.safeTimes = []string{"2026-10-20T12:00:05.000000Z", "2026-10-20T12:00:01.000000Z"}
	q := b.serve(t)
	changed, err := q.Queue("q").ChangedSince(context.Background(), time.Time{})
	if err != nil || !changed.SafeTime.Equal(time.Date(2026, 10, 20, 12, 0, 1, 0, time.UTC)) {
		t.Fatalf("err %v safeTime %v", err, changed.SafeTime)
	}
}

func TestChangedSinceAnswersNothingForAMissingQueueAndFailsOnBadCursor(t *testing.T) {
	b := newFakeBroker()
	q := b.serve(t)
	changed, err := q.Queue("ghost").ChangedSince(context.Background(), time.Time{})
	if err != nil || len(changed.Partitions) != 0 || changed.SafeTime.IsZero() {
		t.Fatalf("err %v changed %+v", err, changed)
	}
	bad := newFakeBroker()
	bad.changedOverride = func(map[string]interface{}) (int, interface{}) {
		return 200, json.RawMessage(`{"safeTime":"2026-10-20T12:00:00.000000Z","safeTimeDegraded":false,"entries":[{"queue":"q","error":"BAD_CURSOR"}]}`)
	}
	qb := bad.serve(t)
	if _, err := qb.Queue("q").ChangedSince(context.Background(), time.Time{}); err == nil || !strings.Contains(err.Error(), "BAD_CURSOR") {
		t.Fatalf("err: %v", err)
	}
}

// Package compat drives a running queen-kafka facade with franz-go, the
// strictest of the clients in PLAN_QUEEN_KAFKA.md's M6 matrix, and asserts what
// a real Kafka client observes: metadata, produce (every codec), group-less
// consume, long-poll wake-ups, ListOffsets bounds, OFFSET_OUT_OF_RANGE,
// acks=0 and a concurrency smoke.
//
// Nothing here starts a stack. `compat/rig.sh` does that — Postgres, broker,
// facade — and then runs `go test ./...` against it, which is what makes this
// package re-runnable from any later workflow:
//
//	protocols/queen-kafka/compat/rig.sh                # stand up, run, tear down
//	QUEEN_KAFKA_BOOTSTRAP=host:9092 go test  # against a stack that is already up
//
// The three knobs are the three things a rig can move: QUEEN_KAFKA_BOOTSTRAP
// (the facade's Kafka listener), QUEEN_URL (the broker's HTTP, for the native
// interop half) and QUEEN_KAFKA_PARTITIONS (the facade's
// QUEEN_KAFKA_DEFAULT_PARTITIONS, which the metadata assertions have to know).
//
// GOWORK=off is not optional when running inside this repository: the root
// go.work lists the two client modules and not this one, so a bare `go test`
// refuses to build a module outside the workspace. rig.sh sets it.
package compat

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

const (
	defaultBootstrap  = "127.0.0.1:19092"
	defaultQueenURL   = "http://127.0.0.1:6699"
	defaultPartitions = 8
)

func bootstrap() string {
	if v := os.Getenv("QUEEN_KAFKA_BOOTSTRAP"); v != "" {
		return v
	}
	return defaultBootstrap
}

func queenURL() string {
	if v := os.Getenv("QUEEN_URL"); v != "" {
		return strings.TrimRight(v, "/")
	}
	return defaultQueenURL
}

// The width the facade auto-creates a topic with. The rig runs it at 8 so a
// partition listing stays readable; the assertions read it from here rather
// than hardcoding, so the same tests pass against a 1024-wide deployment.
func topicWidth(t *testing.T) int32 {
	t.Helper()
	v := os.Getenv("QUEEN_KAFKA_PARTITIONS")
	if v == "" {
		return defaultPartitions
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		t.Fatalf("QUEEN_KAFKA_PARTITIONS=%q is not a partition count", v)
	}
	return int32(n)
}

// A failure to reach the facade is a rig fault, not a compatibility finding:
// say so once, here, instead of ten times as an obscure per-test dial error.
func TestMain(m *testing.M) {
	c, err := net.DialTimeout("tcp", bootstrap(), 3*time.Second)
	if err != nil {
		fmt.Fprintf(os.Stderr,
			"compat: cannot reach the queen-kafka facade at %s: %v\n"+
				"Start the rig (protocols/queen-kafka/compat/rig.sh) or point QUEEN_KAFKA_BOOTSTRAP at one.\n",
			bootstrap(), err)
		os.Exit(1)
	}
	_ = c.Close()
	os.Exit(m.Run())
}

// newClient builds a client every test in this package can share the defaults
// of. Two of those defaults are load-bearing:
//
//   - DisableIdempotentWrite. This is now a CHOICE and no longer a necessity:
//     M7 F3 implemented InitProducerId and the sequence window, and M9
//     implemented transactions, so an idempotent franz-go producer works here.
//     The tests in this package assert the offsets a plain produce returns and
//     drive hand-built batches with fixed sequences, and an idempotent client
//     would put its own producer id and sequence on every one of them. The
//     idempotent and transactional paths have their own homes: idempotent_test.go
//     here, and compat/transactions for the client-visible transaction loop.
//   - ManualPartitioner, because every test names the partition it writes to.
//     The assertions are then about the facade's mapping (Kafka partition n =
//     Queen partition n) and not about a partitioner's hash.
func newClient(t *testing.T, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	base := []kgo.Opt{
		kgo.SeedBrokers(bootstrap()),
		kgo.DisableIdempotentWrite(),
		kgo.AllowAutoTopicCreation(),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.RequestRetries(3),
	}
	if os.Getenv("COMPAT_VERBOSE") != "" {
		base = append(base, kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelDebug, nil)))
	}
	cl, err := kgo.NewClient(append(base, opts...)...)
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	t.Cleanup(cl.Close)
	return cl
}

// A topic name unique to this run of this test, so a re-run never reads the
// previous one's records and the suite can run in parallel with itself.
func newTopic(t *testing.T) string {
	t.Helper()
	name := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			return r
		case r >= 'A' && r <= 'Z':
			return r + ('a' - 'A')
		default:
			return '-'
		}
	}, t.Name())
	return fmt.Sprintf("kcompat-%s-%d", name, time.Now().UnixNano())
}

func ctxFor(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

// metadataFor asks for one topic by name, allowing auto-creation — which is how
// a topic comes into existence here (the facade creates the queue on a metadata
// request that permits it).
func metadataFor(t *testing.T, cl *kgo.Client, topic string) *kmsg.MetadataResponse {
	t.Helper()
	req := kmsg.NewPtrMetadataRequest()
	rt := kmsg.NewMetadataRequestTopic()
	rt.Topic = &topic
	req.Topics = append(req.Topics, rt)
	req.AllowAutoTopicCreation = true

	resp, err := cl.Request(ctxFor(t, 20*time.Second), req)
	if err != nil {
		t.Fatalf("Metadata(%s): %v", topic, err)
	}
	md, ok := resp.(*kmsg.MetadataResponse)
	if !ok {
		t.Fatalf("Metadata(%s): unexpected response type %T", topic, resp)
	}
	return md
}

// ensureTopic makes the topic exist and returns nothing but the certainty that
// it does; every produce path in this package calls it first so that a test
// failure is never "the topic was still being created".
func ensureTopic(t *testing.T, cl *kgo.Client, topic string) {
	t.Helper()
	md := metadataFor(t, cl, topic)
	for _, mt := range md.Topics {
		if mt.Topic != nil && *mt.Topic == topic {
			if mt.ErrorCode != 0 {
				t.Fatalf("topic %s: metadata error code %d", topic, mt.ErrorCode)
			}
			if got, want := int32(len(mt.Partitions)), topicWidth(t); got != want {
				t.Fatalf("topic %s: %d partitions, want %d", topic, got, want)
			}
			return
		}
	}
	t.Fatalf("topic %s is not in the metadata response", topic)
}

// produceSync produces every record and fails on the first error. The results
// come back keyed by record key, because ProduceResults are collected as the
// callbacks fire and their order is not the produce order.
func produceSync(t *testing.T, cl *kgo.Client, recs []*kgo.Record) map[string]*kgo.Record {
	t.Helper()
	results := cl.ProduceSync(ctxFor(t, 60*time.Second), recs...)
	if err := results.FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}
	out := make(map[string]*kgo.Record, len(recs))
	for _, r := range results {
		out[string(r.Record.Key)] = r.Record
	}
	return out
}

// consumeFrom drains `want` records from a topic with a group-less consumer,
// starting at `at`. A timeout is a failure and says how far it got.
func consumeFrom(t *testing.T, topic string, at kgo.Offset, want int, budget time.Duration, opts ...kgo.Opt) []*kgo.Record {
	t.Helper()
	cl := newClient(t, append([]kgo.Opt{
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(at),
	}, opts...)...)
	return drain(t, cl, want, budget)
}

func drain(t *testing.T, cl *kgo.Client, want int, budget time.Duration) []*kgo.Record {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	out := make([]*kgo.Record, 0, want)
	for len(out) < want {
		fs := cl.PollRecords(ctx, want-len(out))
		if ctx.Err() != nil {
			t.Fatalf("timed out after %s with %d/%d records", budget, len(out), want)
		}
		if errs := fs.Errors(); len(errs) > 0 {
			t.Fatalf("fetch error at %d/%d records: %v", len(out), want, errs)
		}
		fs.EachRecord(func(r *kgo.Record) { out = append(out, r) })
	}
	return out
}

// byPartition groups records the way every ordering assertion here reads them:
// per partition, in arrival order.
func byPartition(recs []*kgo.Record) map[int32][]*kgo.Record {
	out := make(map[int32][]*kgo.Record)
	for _, r := range recs {
		out[r.Partition] = append(out[r.Partition], r)
	}
	return out
}

// The M2 gate in one assertion: a fresh partition's offsets start at 0 and have
// no holes, in the order the client produced them.
func assertContiguousFromZero(t *testing.T, label string, offsets map[int32][]int64) {
	t.Helper()
	parts := make([]int32, 0, len(offsets))
	for p := range offsets {
		parts = append(parts, p)
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i] < parts[j] })
	for _, p := range parts {
		for i, off := range offsets[p] {
			if off != int64(i) {
				t.Errorf("%s: partition %d offset[%d] = %d, want %d (offsets: %v)",
					label, p, i, off, i, offsets[p])
				break
			}
		}
	}
}

// nil and empty are different things in Kafka — a null key means "partition me
// round-robin", a null value is a tombstone — so the comparison keeps them
// apart rather than collapsing both to len 0.
func sameBytes(a, b []byte) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	return bytes.Equal(a, b)
}

func describeBytes(b []byte) string {
	if b == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%q", b)
}

// --------------------------------------------------------------- Queen's HTTP
//
// The native half of the interop check: the same broker, spoken to as an
// ordinary Queen client, with no Kafka in the way.

type queenPushItem struct {
	Queue     string          `json:"queue"`
	Partition string          `json:"partition"`
	Payload   json.RawMessage `json:"payload"`
}

type queenPushResult struct {
	Status string `json:"status"`
	Offset *int64 `json:"offset"`
}

type queenFetchEntry struct {
	Queue     string `json:"queue"`
	Partition string `json:"partition"`
	Offset    int64  `json:"offset"`
}

type queenFetchRecord struct {
	Offset  int64           `json:"offset"`
	Payload json.RawMessage `json:"payload"`
	TS      string          `json:"ts"`
}

type queenFetchResult struct {
	Queue          string             `json:"queue"`
	Partition      string             `json:"partition"`
	Records        []queenFetchRecord `json:"records"`
	HighWatermark  int64              `json:"highWatermark"`
	LogStartOffset int64              `json:"logStartOffset"`
	Error          string             `json:"error"`
}

func postJSON(t *testing.T, path string, body any, into any) {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal %s body: %v", path, err)
	}
	req, err := http.NewRequestWithContext(ctxFor(t, 60*time.Second),
		http.MethodPost, queenURL()+path, bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("build %s request: %v", path, err)
	}
	req.Header.Set("content-type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	defer resp.Body.Close()
	answer, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s response: %v", path, err)
	}
	if resp.StatusCode >= 300 {
		t.Fatalf("POST %s: HTTP %d: %s", path, resp.StatusCode, answer)
	}
	if err := json.Unmarshal(answer, into); err != nil {
		t.Fatalf("decode %s response %s: %v", path, answer, err)
	}
}

func queenPush(t *testing.T, items ...queenPushItem) []queenPushResult {
	t.Helper()
	var out []queenPushResult
	postJSON(t, "/api/v1/push", map[string]any{"items": items}, &out)
	if len(out) != len(items) {
		t.Fatalf("push answered %d results for %d items", len(out), len(items))
	}
	return out
}

func queenFetch(t *testing.T, entries ...queenFetchEntry) []queenFetchResult {
	t.Helper()
	var out struct {
		Entries []queenFetchResult `json:"entries"`
	}
	postJSON(t, "/api/v1/fetch", map[string]any{"entries": entries, "maxWaitMs": 0}, &out)
	if len(out.Entries) != len(entries) {
		t.Fatalf("fetch answered %d entries for %d requested", len(out.Entries), len(entries))
	}
	return out.Entries
}

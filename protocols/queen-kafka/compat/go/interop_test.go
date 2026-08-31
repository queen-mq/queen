package compat

import (
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// CHECK 7, half one: "native producers in, Kafka consumers out". A payload
// pushed by an ordinary Queen client is not an envelope, and the facade serves
// it as itself — no key, the payload's own JSON text as the value.
func TestNativePayloadIsReadableThroughTheFacade(t *testing.T) {
	cl := newClient(t, kgo.RequiredAcks(kgo.AllISRAcks()))
	topic := newTopic(t)
	// The queue has to be a Kafka topic first, or the consumer has nothing to
	// subscribe to; the native push then writes into the same queue.
	ensureTopic(t, cl, topic)

	const partition = 5
	payload := json.RawMessage(`{"orderId":7,"total":10.5,"items":["a","b"]}`)
	res := queenPush(t, queenPushItem{Queue: topic, Partition: "5", Payload: payload})
	if res[0].Status != "queued" {
		t.Fatalf("native push status %q, want queued", res[0].Status)
	}
	if res[0].Offset == nil {
		t.Fatalf("native push answered no offset (server C1)")
	}
	off := *res[0].Offset

	consumer := newClient(t, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
		topic: {partition: kgo.NewOffset().At(off)},
	}))
	back := drain(t, consumer, 1, 30*time.Second)[0]

	if back.Offset != off {
		t.Errorf("read offset %d, pushed at %d", back.Offset, off)
	}
	if back.Key != nil {
		t.Errorf("a native payload came back with key %s, want none", describeBytes(back.Key))
	}
	if len(back.Headers) != 0 {
		t.Errorf("a native payload came back with headers %v", back.Headers)
	}
	var got, want any
	if err := json.Unmarshal(back.Value, &got); err != nil {
		t.Fatalf("the value is not the stored JSON document: %v (%s)", err, describeBytes(back.Value))
	}
	if err := json.Unmarshal(payload, &want); err != nil {
		t.Fatal(err)
	}
	if !jsonEqual(got, want) {
		t.Errorf("value came back as %s, pushed %s", back.Value, payload)
	}
	// A record the log timestamped, so the consumer gets the store time rather
	// than "unknown".
	if back.Timestamp.IsZero() || back.Timestamp.UnixMilli() <= 0 {
		t.Errorf("a native payload came back with timestamp %v", back.Timestamp)
	}
}

// CHECK 7, half two: the mirror. A record produced by a Kafka client is stored
// as the envelope, and an ordinary Queen consumer sees exactly that shape —
// which is the contract every native reader of a Kafka-written queue depends
// on.
func TestKafkaProducedRecordIsAnEnvelopeOverTheNativeAPI(t *testing.T) {
	cl := newClient(t, kgo.RequiredAcks(kgo.AllISRAcks()))
	topic := newTopic(t)
	ensureTopic(t, cl, topic)

	const partition = 6
	ts := time.UnixMilli(1_756_000_700_000)
	rec := &kgo.Record{
		Topic:     topic,
		Partition: partition,
		Key:       []byte("user-42"),
		Value:     []byte(`{"amount":10}`),
		Headers:   []kgo.RecordHeader{{Key: "trace-id", Value: []byte("abc")}},
		Timestamp: ts,
	}
	results := cl.ProduceSync(ctxFor(t, 30*time.Second), rec)
	if err := results.FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}
	off := results[0].Record.Offset

	entry := queenFetch(t, queenFetchEntry{Queue: topic, Partition: "6", Offset: off})[0]
	if entry.Error != "" {
		t.Fatalf("native fetch answered error %q", entry.Error)
	}
	if entry.HighWatermark != off+1 {
		t.Errorf("highWatermark %d, want %d", entry.HighWatermark, off+1)
	}
	if entry.LogStartOffset != 0 {
		t.Errorf("logStartOffset %d, want 0", entry.LogStartOffset)
	}
	if len(entry.Records) != 1 {
		t.Fatalf("native fetch returned %d records, want 1", len(entry.Records))
	}
	if entry.Records[0].Offset != off {
		t.Errorf("native fetch returned offset %d, want %d", entry.Records[0].Offset, off)
	}

	var env struct {
		K *string `json:"k"`
		V *string `json:"v"`
		H []struct {
			K string  `json:"k"`
			V *string `json:"v"`
		} `json:"h"`
		T *int64 `json:"t"`
	}
	if err := json.Unmarshal(entry.Records[0].Payload, &env); err != nil {
		t.Fatalf("the stored payload is not the envelope: %v (%s)", err, entry.Records[0].Payload)
	}
	if env.K == nil || decodeB64(t, *env.K) != string(rec.Key) {
		t.Errorf(`envelope "k" = %v, want base64 of %q`, env.K, rec.Key)
	}
	if env.V == nil || decodeB64(t, *env.V) != string(rec.Value) {
		t.Errorf(`envelope "v" = %v, want base64 of %q`, env.V, rec.Value)
	}
	if len(env.H) != 1 || env.H[0].K != "trace-id" || env.H[0].V == nil ||
		decodeB64(t, *env.H[0].V) != "abc" {
		t.Errorf(`envelope "h" = %+v, want one trace-id header`, env.H)
	}
	if env.T == nil || *env.T != ts.UnixMilli() {
		t.Errorf(`envelope "t" = %v, want %d`, env.T, ts.UnixMilli())
	}
}

func decodeB64(t *testing.T, s string) string {
	t.Helper()
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		t.Fatalf("envelope field %q is not base64: %v", s, err)
	}
	return string(b)
}

func jsonEqual(a, b any) bool {
	x, err1 := json.Marshal(a)
	y, err2 := json.Marshal(b)
	return err1 == nil && err2 == nil && string(x) == string(y)
}

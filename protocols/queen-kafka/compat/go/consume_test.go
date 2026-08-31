package compat

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type coord struct {
	partition int32
	offset    int64
}

// CHECK 4. The round trip, byte for byte: everything a Kafka record carries
// through the envelope and back — including the four cases the envelope exists
// to keep apart (a null key is not an empty key, a tombstone is not an empty
// value) and bytes that are not UTF-8, which is why the envelope is base64 and
// not JSON strings.
func TestConsumeRoundTripsEveryFieldByteExact(t *testing.T) {
	cl := newClient(t, kgo.RequiredAcks(kgo.AllISRAcks()))
	topic := newTopic(t)
	ensureTopic(t, cl, topic)

	nonUTF8 := []byte{0x00, 0xff, 0xfe, 0x80, 0x01, 0x7f, 0xc3, 0x28}
	width := topicWidth(t)
	base := int64(1_756_000_300_000)

	recs := []*kgo.Record{
		{Key: []byte("plain"), Value: []byte(`{"amount":10}`)},
		{Key: nil, Value: []byte("null key")},
		{Key: []byte{}, Value: []byte("empty key")},
		{Key: []byte("tombstone"), Value: nil},
		{Key: []byte("empty value"), Value: []byte{}},
		{Key: nonUTF8, Value: nonUTF8},
		{Key: []byte("unicode"), Value: []byte("regina — 👑 — coda")},
		{Key: []byte("headers"), Value: []byte("with headers"), Headers: []kgo.RecordHeader{
			{Key: "trace-id", Value: []byte("abc")},
			{Key: "binary", Value: nonUTF8},
			{Key: "empty", Value: []byte{}},
			{Key: "z-order", Value: []byte("last")},
		}},
		{Key: []byte("big"), Value: []byte(strings.Repeat("x", 64*1024))},
	}
	for i, r := range recs {
		r.Topic = topic
		r.Partition = int32(i) % width
		r.Timestamp = time.UnixMilli(base + int64(i))
	}

	results := cl.ProduceSync(ctxFor(t, 60*time.Second), recs...)
	if err := results.FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}
	// Matched by (partition, offset) rather than by key: two of these records
	// deliberately have no usable key.
	want := make(map[coord]*kgo.Record, len(recs))
	for _, r := range results {
		want[coord{r.Record.Partition, r.Record.Offset}] = r.Record
	}

	back := consumeFrom(t, topic, kgo.NewOffset().AtStart(), len(recs), 60*time.Second)

	// Order first: within a partition, offsets arrive ascending and from 0.
	for p, rs := range byPartition(back) {
		for i, r := range rs {
			if r.Offset != int64(i) {
				t.Errorf("partition %d: record %d arrived at offset %d, want %d", p, i, r.Offset, i)
			}
		}
	}

	matched := make(map[coord]int, len(recs))
	for _, r := range back {
		at := coord{r.Partition, r.Offset}
		w, ok := want[at]
		if !ok {
			t.Errorf("consumed a record at partition %d offset %d that was never produced", r.Partition, r.Offset)
			continue
		}
		matched[at]++
		label := fmt.Sprintf("p%d@%d (%s)", r.Partition, r.Offset, describeBytes(w.Key))
		if !sameBytes(r.Key, w.Key) {
			t.Errorf("%s: key came back %s, sent %s", label, describeBytes(r.Key), describeBytes(w.Key))
		}
		if !sameBytes(r.Value, w.Value) {
			t.Errorf("%s: value came back %s, sent %s", label,
				describeBytes(truncate(r.Value)), describeBytes(truncate(w.Value)))
		}
		if r.Timestamp.UnixMilli() != w.Timestamp.UnixMilli() {
			t.Errorf("%s: timestamp came back %d, sent %d", label,
				r.Timestamp.UnixMilli(), w.Timestamp.UnixMilli())
		}
		if len(r.Headers) != len(w.Headers) {
			t.Errorf("%s: %d headers back, sent %d (%v)", label, len(r.Headers), len(w.Headers), r.Headers)
			continue
		}
		for i := range w.Headers {
			// Order included: Kafka preserves header order and so does the
			// envelope.
			if r.Headers[i].Key != w.Headers[i].Key {
				t.Errorf("%s: header %d is %q, sent %q", label, i, r.Headers[i].Key, w.Headers[i].Key)
			}
			if !sameBytes(r.Headers[i].Value, w.Headers[i].Value) {
				t.Errorf("%s: header %q value came back %s, sent %s", label, w.Headers[i].Key,
					describeBytes(r.Headers[i].Value), describeBytes(w.Headers[i].Value))
			}
		}
	}
	// Every produced record, once: the assertions above compare what arrived,
	// this is what says nothing was dropped or delivered twice.
	for at, w := range want {
		switch matched[at] {
		case 1:
		case 0:
			t.Errorf("the record produced to partition %d offset %d (%s) never came back",
				at.partition, at.offset, describeBytes(w.Key))
		default:
			t.Errorf("partition %d offset %d was delivered %d times", at.partition, at.offset, matched[at])
		}
	}
}

func truncate(b []byte) []byte {
	if len(b) > 64 {
		return b[:64]
	}
	return b
}

// CHECK 5. The long poll (server C2). A consumer parked exactly at the high
// watermark must be woken by the next write, not by the expiry of its own
// max-wait — that difference is the whole reason the internal poll loop was
// replaced. Measured, and printed, because "it eventually arrived" is not the
// property.
func TestLongPollWakesTheParkedConsumer(t *testing.T) {
	producer := newClient(t, kgo.RequiredAcks(kgo.AllISRAcks()))
	topic := newTopic(t)
	ensureTopic(t, producer, topic)

	first := producer.ProduceSync(ctxFor(t, 30*time.Second), &kgo.Record{
		Topic: topic, Partition: 0, Key: []byte("seed"), Value: []byte("seed"),
		Timestamp: time.UnixMilli(1_756_000_400_000),
	})
	if err := first.FirstErr(); err != nil {
		t.Fatalf("seed produce: %v", err)
	}

	// Parked AT the high watermark: offset 1 of a one-record partition.
	const maxWait = 20 * time.Second
	consumer := newClient(t,
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			topic: {0: kgo.NewOffset().At(1)},
		}),
		kgo.FetchMaxWait(maxWait),
	)

	type arrival struct {
		at   time.Time
		recs []*kgo.Record
		err  error
	}
	done := make(chan arrival, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), maxWait+20*time.Second)
		defer cancel()
		fs := consumer.PollRecords(ctx, 1)
		a := arrival{at: time.Now()}
		if errs := fs.Errors(); len(errs) > 0 {
			a.err = fmt.Errorf("%v", errs)
		}
		fs.EachRecord(func(r *kgo.Record) { a.recs = append(a.recs, r) })
		done <- a
	}()

	// Long enough that the fetch is unambiguously parked on the broker's
	// notifier before the write happens.
	time.Sleep(1500 * time.Millisecond)
	sent := producer.ProduceSync(ctxFor(t, 30*time.Second), &kgo.Record{
		Topic: topic, Partition: 0, Key: []byte("late"), Value: []byte("woken"),
		Timestamp: time.UnixMilli(1_756_000_400_001),
	})
	if err := sent.FirstErr(); err != nil {
		t.Fatalf("late produce: %v", err)
	}
	wroteAt := time.Now()

	select {
	case a := <-done:
		if a.err != nil {
			t.Fatalf("parked consumer: %v", a.err)
		}
		if len(a.recs) != 1 {
			t.Fatalf("parked consumer woke with %d records, want 1", len(a.recs))
		}
		wake := a.at.Sub(wroteAt)
		t.Logf("long-poll wake latency: %s (max wait was %s)", wake.Round(time.Millisecond), maxWait)
		if !sameBytes(a.recs[0].Value, []byte("woken")) {
			t.Errorf("woke with the wrong record: %s", describeBytes(a.recs[0].Value))
		}
		if a.recs[0].Offset != 1 {
			t.Errorf("woke at offset %d, want 1", a.recs[0].Offset)
		}
		// The failure this catches is the park expiring instead of the write
		// waking it: that lands at maxWait, not near zero.
		if wake > 2*time.Second {
			t.Errorf("wake took %s: that is a poll timeout, not a wake-up", wake)
		}
	case <-time.After(maxWait + 25*time.Second):
		t.Fatalf("the parked consumer never woke")
	}
}

// listOffset asks the facade one ListOffsets question. -2 is earliest, -1 is
// latest; those two sentinels are the entire surface the advertised window
// (v1..=v5) can be asked about.
func listOffset(t *testing.T, cl *kgo.Client, topic string, partition int32, timestamp int64) (int64, int16) {
	t.Helper()
	req := kmsg.NewPtrListOffsetsRequest()
	req.ReplicaID = -1
	rt := kmsg.NewListOffsetsRequestTopic()
	rt.Topic = topic
	rp := kmsg.NewListOffsetsRequestTopicPartition()
	rp.Partition = partition
	rp.CurrentLeaderEpoch = -1
	rp.Timestamp = timestamp
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)

	resp, err := cl.Request(ctxFor(t, 30*time.Second), req)
	if err != nil {
		t.Fatalf("ListOffsets(%s/%d, ts=%d): %v", topic, partition, timestamp, err)
	}
	lo, ok := resp.(*kmsg.ListOffsetsResponse)
	if !ok {
		t.Fatalf("ListOffsets: unexpected response type %T", resp)
	}
	for _, lt := range lo.Topics {
		if lt.Topic != topic {
			continue
		}
		for _, lp := range lt.Partitions {
			if lp.Partition == partition {
				return lp.Offset, lp.ErrorCode
			}
		}
	}
	t.Fatalf("ListOffsets answered nothing for %s/%d", topic, partition)
	return 0, 0
}

// CHECK 6. The bounds probe: earliest is 0 and latest is the count, on a
// partition that has been written; both are 0 on one that never has. A
// consumer that gets this wrong either replays from nowhere or waits for
// records behind it.
func TestListOffsetsBounds(t *testing.T) {
	cl := newClient(t, kgo.RequiredAcks(kgo.AllISRAcks()))
	topic := newTopic(t)
	ensureTopic(t, cl, topic)

	const written = 7
	recs := make([]*kgo.Record, 0, written)
	for i := 0; i < written; i++ {
		recs = append(recs, &kgo.Record{
			Topic: topic, Partition: 0,
			Key:       []byte(fmt.Sprintf("key-%03d", i)),
			Value:     []byte(fmt.Sprintf(`{"i":%d}`, i)),
			Timestamp: time.UnixMilli(1_756_000_500_000 + int64(i)),
		})
	}
	produceSync(t, cl, recs)

	if off, code := listOffset(t, cl, topic, 0, -2); code != 0 || off != 0 {
		t.Errorf("earliest of a written partition = %d (error code %d), want 0", off, code)
	}
	if off, code := listOffset(t, cl, topic, 0, -1); code != 0 || off != written {
		t.Errorf("latest of a written partition = %d (error code %d), want %d", off, code, written)
	}

	// A partition of the same topic nobody has written to.
	untouched := topicWidth(t) - 1
	if untouched == 0 {
		t.Skip("a one-partition topic has no untouched partition to check")
	}
	if off, code := listOffset(t, cl, topic, untouched, -2); code != 0 || off != 0 {
		t.Errorf("earliest of an unwritten partition = %d (error code %d), want 0", off, code)
	}
	if off, code := listOffset(t, cl, topic, untouched, -1); code != 0 || off != 0 {
		t.Errorf("latest of an unwritten partition = %d (error code %d), want 0", off, code)
	}

	// ...and a topic that has never been written at all, which is the state a
	// consumer subscribing before any producer runs actually sees.
	fresh := newTopic(t)
	ensureTopic(t, cl, fresh)
	if off, code := listOffset(t, cl, fresh, 0, -2); code != 0 || off != 0 {
		t.Errorf("earliest of a never-written topic = %d (error code %d), want 0", off, code)
	}
	if off, code := listOffset(t, cl, fresh, 0, -1); code != 0 || off != 0 {
		t.Errorf("latest of a never-written topic = %d (error code %d), want 0", off, code)
	}
}

// CHECK 8. A fetch outside the log has to say so in Kafka's own words. The
// client is configured NOT to reset, so the error reaches the caller instead of
// being absorbed into a silent rewind — which is the only way to see WHICH
// error the facade sent.
func TestFetchBeyondTheEndIsOffsetOutOfRange(t *testing.T) {
	cl := newClient(t, kgo.RequiredAcks(kgo.AllISRAcks()))
	topic := newTopic(t)
	ensureTopic(t, cl, topic)
	produceSync(t, cl, []*kgo.Record{{
		Topic: topic, Partition: 0, Key: []byte("key-000"), Value: []byte("v"),
		Timestamp: time.UnixMilli(1_756_000_600_000),
	}})

	consumer := newClient(t,
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			topic: {0: kgo.NewOffset().At(1_000_000)},
		}),
		kgo.ConsumeResetOffset(kgo.NoResetOffset()),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		fs := consumer.PollFetches(ctx)
		if ctx.Err() != nil {
			break
		}
		errs := fs.Errors()
		if len(errs) == 0 {
			if n := fs.NumRecords(); n > 0 {
				t.Fatalf("a fetch at offset 1000000 returned %d records", n)
			}
			continue
		}
		for _, e := range errs {
			if errors.Is(e.Err, kerr.OffsetOutOfRange) ||
				strings.Contains(strings.ToUpper(e.Err.Error()), "OFFSET_OUT_OF_RANGE") ||
				strings.Contains(strings.ToLower(e.Err.Error()), "offset out of range") {
				t.Logf("client saw: %v", e.Err)
				return
			}
		}
		t.Fatalf("a fetch at offset 1000000 failed with %v, want OFFSET_OUT_OF_RANGE", errs)
	}
	t.Fatalf("a fetch at offset 1000000 never produced an error — the client would poll for ever")
}

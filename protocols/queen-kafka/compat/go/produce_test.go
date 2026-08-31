package compat

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// CHECK 2. The M2 gate: keyed records across every partition, acks=all, and the
// offsets the facade answers with are the offsets a Kafka producer expects —
// per partition, from 0, with no holes.
func TestProduceUncompressedReturnsContiguousOffsets(t *testing.T) {
	cl := newClient(t, kgo.RequiredAcks(kgo.AllISRAcks()))
	topic := newTopic(t)
	ensureTopic(t, cl, topic)

	const n = 100
	width := topicWidth(t)
	recs := make([]*kgo.Record, 0, n)
	for i := 0; i < n; i++ {
		recs = append(recs, &kgo.Record{
			Topic:     topic,
			Partition: int32(i) % width,
			Key:       []byte(fmt.Sprintf("key-%03d", i)),
			Value:     []byte(fmt.Sprintf(`{"i":%d,"topic":"%s"}`, i, topic)),
			Timestamp: time.UnixMilli(1_756_000_000_000 + int64(i)),
		})
	}
	got := produceSync(t, cl, recs)

	// Produce order per partition, which is the order the offsets have to be in.
	offsets := make(map[int32][]int64)
	for _, want := range recs {
		r, ok := got[string(want.Key)]
		if !ok {
			t.Fatalf("no produce result for %s", want.Key)
		}
		if r.Partition != want.Partition {
			t.Errorf("%s landed on partition %d, produced to %d", want.Key, r.Partition, want.Partition)
		}
		if r.Offset < 0 {
			t.Errorf("%s came back with offset %d — the facade answered no offset", want.Key, r.Offset)
		}
		offsets[r.Partition] = append(offsets[r.Partition], r.Offset)
	}
	if int32(len(offsets)) != width {
		t.Errorf("records landed on %d partitions, want %d", len(offsets), width)
	}
	assertContiguousFromZero(t, "produce", offsets)
}

// A hook that counts the bytes actually written for Produce requests, which is
// the only way from outside the client to tell a compressed batch from one the
// codec silently declined to shrink.
type produceBytes struct {
	mu sync.Mutex
	n  int64
}

func (p *produceBytes) OnBrokerWrite(_ kgo.BrokerMetadata, key int16, written int, _, _ time.Duration, _ error) {
	if key != 0 { // Produce
		return
	}
	p.mu.Lock()
	p.n += int64(written)
	p.mu.Unlock()
}

func (p *produceBytes) total() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.n
}

// CHECK 3. `compression.type` is a producer setting with no negotiation: which
// codec arrives is the client's choice, so all four have to decode. The payload
// is deliberately compressible and the written bytes are counted, because a
// codec that quietly fell back to "none" would make this test pass while
// testing nothing.
func TestProduceEveryCompressionCodec(t *testing.T) {
	codecs := []struct {
		name  string
		codec kgo.CompressionCodec
	}{
		{"gzip", kgo.GzipCompression()},
		{"snappy", kgo.SnappyCompression()},
		{"lz4", kgo.Lz4Compression()},
		{"zstd", kgo.ZstdCompression()},
	}
	for _, c := range codecs {
		t.Run(c.name, func(t *testing.T) {
			counter := &produceBytes{}
			cl := newClient(t,
				kgo.RequiredAcks(kgo.AllISRAcks()),
				kgo.ProducerBatchCompression(c.codec),
				kgo.WithHooks(counter),
			)
			topic := newTopic(t)
			ensureTopic(t, cl, topic)

			const n = 40
			width := topicWidth(t)
			// Compressible by construction: every codec turns this into a
			// fraction of its size, so the byte count below is decisive.
			body := make([]byte, 2048)
			for i := range body {
				body[i] = byte('a' + i%3)
			}
			recs := make([]*kgo.Record, 0, n)
			raw := int64(0)
			for i := 0; i < n; i++ {
				value := append([]byte(fmt.Sprintf("%s-%03d:", c.name, i)), body...)
				raw += int64(len(value))
				recs = append(recs, &kgo.Record{
					Topic:     topic,
					Partition: int32(i) % width,
					Key:       []byte(fmt.Sprintf("key-%03d", i)),
					Value:     value,
					Timestamp: time.UnixMilli(1_756_000_100_000 + int64(i)),
				})
			}
			got := produceSync(t, cl, recs)

			if written := counter.total(); written > raw/2 {
				t.Errorf("%s: wrote %d bytes for %d bytes of records — the batch was not compressed, so this run did not exercise the codec",
					c.name, written, raw)
			}

			offsets := make(map[int32][]int64)
			for _, want := range recs {
				r := got[string(want.Key)]
				if r == nil {
					t.Fatalf("no produce result for %s", want.Key)
				}
				offsets[r.Partition] = append(offsets[r.Partition], r.Offset)
			}
			assertContiguousFromZero(t, c.name, offsets)

			// ...and the bytes survive the decode: a codec that decompressed to
			// the wrong bytes would still answer offsets.
			back := consumeFrom(t, topic, kgo.NewOffset().AtStart(), n, 60*time.Second)
			byKey := make(map[string]*kgo.Record, n)
			for _, r := range back {
				byKey[string(r.Key)] = r
			}
			for _, want := range recs {
				r := byKey[string(want.Key)]
				if r == nil {
					t.Errorf("%s: %s never came back", c.name, want.Key)
					continue
				}
				if !sameBytes(r.Value, want.Value) {
					t.Errorf("%s: %s value round-tripped wrong (%d bytes back, %d sent)",
						c.name, want.Key, len(r.Value), len(want.Value))
				}
				if r.Partition != want.Partition {
					t.Errorf("%s: %s came back on partition %d, want %d",
						c.name, want.Key, r.Partition, want.Partition)
				}
			}
		})
	}
}

// CHECK 9. acks=0 is a wire contract, not a tuning knob: the facade must write
// the records and answer NOTHING. A response frame to a producer that is not
// reading one desynchronises the connection; a missing write loses the data.
// Both failures are visible from here — the first as a hang or a protocol
// error on the next request over the same connection, the second as records
// that never arrive.
func TestProduceAcksZeroDoesNotHangAndLands(t *testing.T) {
	cl := newClient(t, kgo.RequiredAcks(kgo.NoAck()))
	topic := newTopic(t)
	ensureTopic(t, cl, topic)

	const n = 20
	width := topicWidth(t)
	recs := make([]*kgo.Record, 0, n)
	for i := 0; i < n; i++ {
		recs = append(recs, &kgo.Record{
			Topic:     topic,
			Partition: int32(i) % width,
			Key:       []byte(fmt.Sprintf("key-%03d", i)),
			Value:     []byte(fmt.Sprintf(`{"acks":0,"i":%d}`, i)),
			Timestamp: time.UnixMilli(1_756_000_200_000 + int64(i)),
		})
	}

	start := time.Now()
	results := cl.ProduceSync(ctxFor(t, 20*time.Second), recs...)
	elapsed := time.Since(start)
	if err := results.FirstErr(); err != nil {
		t.Fatalf("acks=0 produce: %v", err)
	}
	// Fire-and-forget: no round trip to wait for. Generous, because the point
	// is "did not hang", not a latency budget.
	if elapsed > 5*time.Second {
		t.Errorf("acks=0 produce took %s — it waited for a response it should never get", elapsed)
	}
	// The offsets on these results are NOT the facade's: with acks=0 there is
	// no response to read them from, and franz-go fills them in from its own
	// per-partition counter. Asserting anything about them would be asserting
	// the client's bookkeeping, so this only records what they are.
	t.Logf("acks=0 produce returned in %s; client-side offsets, e.g. %s -> %d",
		elapsed.Round(time.Millisecond), results[0].Record.Key, results[0].Record.Offset)

	// A second batch over the same producer connection: a stray response frame
	// from the first one shows up here.
	second := cl.ProduceSync(ctxFor(t, 20*time.Second), &kgo.Record{
		Topic:     topic,
		Partition: 0,
		Key:       []byte("key-020"),
		Value:     []byte(`{"acks":0,"i":20}`),
		Timestamp: time.UnixMilli(1_756_000_200_020),
	})
	if err := second.FirstErr(); err != nil {
		t.Fatalf("second acks=0 produce: %v", err)
	}

	// The only proof that acks=0 wrote anything is reading it back.
	back := consumeFrom(t, topic, kgo.NewOffset().AtStart(), n+1, 60*time.Second)
	seen := make(map[string]bool, n+1)
	for _, r := range back {
		seen[string(r.Key)] = true
	}
	for i := 0; i <= n; i++ {
		key := fmt.Sprintf("key-%03d", i)
		if !seen[key] {
			t.Errorf("acks=0 record %s never landed", key)
		}
	}
}

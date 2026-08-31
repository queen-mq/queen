package compat

import (
	"fmt"
	"testing"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

// record is one expected message, keyed by (partition, seq).
type record struct {
	part int
	seq  int
	key  string
	val  []byte
	hdrs []kafka.Header
}

// corpus builds `parts * perPart` records pinned to partitions by key, in the
// order they must appear on each partition.
func corpus(parts, perPart int) []record {
	out := make([]record, 0, parts*perPart)
	for s := 0; s < perPart; s++ {
		for p := 0; p < parts; p++ {
			out = append(out, record{
				part: p,
				seq:  s,
				key:  fmt.Sprintf("p%d-%06d", p, s),
				val:  payloadFor(p, s),
				hdrs: headersFor(p, s),
			})
		}
	}
	return out
}

func (r record) message() kafka.Message {
	return kafka.Message{Key: []byte(r.key), Value: r.val, Headers: r.hdrs}
}

// produceCorpus writes the corpus with the given codec and returns it. acks is
// always explicit; see the package prose on kafka-go's RequireNone default.
func produceCorpus(t *testing.T, topic string, recs []record, comp kafka.Compression) {
	t.Helper()

	w := newWriter(topic, kafka.RequireAll, func(w *kafka.Writer) {
		w.Compression = comp
		w.BatchSize = 64
	})
	defer func() {
		if err := w.Close(); err != nil {
			failf(t, "closing writer for %s: %v", topic, err)
		}
	}()

	msgs := make([]kafka.Message, 0, len(recs))
	for _, r := range recs {
		msgs = append(msgs, r.message())
	}

	ctx, cancel := ctxWith(t, 120*time.Second)
	defer cancel()

	start := time.Now()
	if err := w.WriteMessages(ctx, msgs...); err != nil {
		failf(t, "WriteMessages(%d, codec=%v) to %s: %v", len(msgs), comp, topic, err)
	}
	okf(t, "wrote %d records to %s with codec %q in %s", len(msgs), topic, codecName(comp), time.Since(start).Round(time.Millisecond))
}

func codecName(c kafka.Compression) string {
	switch c {
	case 0:
		return "none"
	case kafka.Gzip:
		return "gzip"
	case kafka.Snappy:
		return "snappy"
	case kafka.Lz4:
		return "lz4"
	case kafka.Zstd:
		return "zstd"
	}
	return fmt.Sprintf("codec(%d)", c)
}

// TestProduceUncompressed is the volume bar: 512 records across 8 partitions,
// every one with a key and three headers (one of them holding bytes that are not
// valid UTF-8, which is what actually proves the facade's base64 envelope is
// byte-exact rather than string-exact).
func TestProduceUncompressed(t *testing.T) {
	section(t, "Produce: 512 records, 8 partitions, keys + headers, uncompressed")

	topic := topicName("prod-none")
	width := topicWidth(t)
	waitForTopic(t, topic, width, 30*time.Second)

	recs := corpus(width, 512/width)
	produceCorpus(t, topic, recs, 0)

	// The write is only half the claim; read it back off a partition reader so
	// this test stands alone without a group.
	got := drainPartitions(t, topic, width, len(recs), 60*time.Second)
	verifyCorpus(t, recs, got)
}

// TestProduceEveryCodec runs the same corpus through all four codecs kafka-go
// ships. The facade decodes RecordBatch v2 with all four on a decompression
// budget (queen-kafka/src/decompress.rs), so all four must round-trip.
//
// Note for anyone comparing with the librdkafka row of the matrix: librdkafka
// gates zstd on Fetch v10 and the facade caps Fetch at v6, so librdkafka sends
// zstd batches UNCOMPRESSED. kafka-go has no such gate — it compresses on the
// PRODUCE path, which the facade advertises to v9, so the bytes on the wire here
// really are zstd.
func TestProduceEveryCodec(t *testing.T) {
	section(t, "Produce: every codec kafka-go ships")

	width := topicWidth(t)
	perPart := 512 / width

	for _, c := range []struct {
		codec kafka.Compression
		name  string
	}{
		{kafka.Gzip, "gzip"},
		{kafka.Snappy, "snappy"},
		{kafka.Lz4, "lz4"},
		{kafka.Zstd, "zstd"},
	} {
		t.Run(c.name, func(t *testing.T) {
			topic := topicName("prod-" + c.name)
			waitForTopic(t, topic, width, 30*time.Second)

			recs := corpus(width, perPart)
			produceCorpus(t, topic, recs, c.codec)

			got := drainPartitions(t, topic, width, len(recs), 60*time.Second)
			verifyCorpus(t, recs, got)
		})
	}
}

// TestProduceAcksZero documents the kafka-go trap rather than asserting on it.
//
// A `&kafka.Writer{}` composite literal leaves RequiredAcks at its zero value,
// which kafka-go defines as RequireNone — acks=0. `kafka.NewWriter(cfg)` is the
// only constructor that rewrites 0 to RequireAll (writer.go:508). The facade
// writes no response frame at all for acks=0 (PLAN_QUEEN_KAFKA.md, deliberate),
// so the offsets such a Writer reports are invented by the client from its own
// counter and no server-side error can ever reach it. The records still land;
// this test proves that much and nothing about the offsets.
func TestProduceAcksZero(t *testing.T) {
	section(t, "Produce with acks=0 (kafka-go's Writer{} default)")

	topic := topicName("acks0")
	width := topicWidth(t)
	waitForTopic(t, topic, width, 30*time.Second)

	// Exactly the shape a user gets from a composite literal.
	probe := &kafka.Writer{Addr: kafka.TCP(bootstrap()), Topic: topic}
	if probe.RequiredAcks != kafka.RequireNone {
		note("kafka-go changed its Writer{} default: RequiredAcks=%v, not RequireNone", probe.RequiredAcks)
	} else {
		note("confirmed: &kafka.Writer{} defaults to RequireNone (acks=0); kafka.NewWriter rewrites 0 to RequireAll")
	}

	recs := corpus(width, 4)
	w := newWriter(topic, kafka.RequireNone)
	msgs := make([]kafka.Message, 0, len(recs))
	for _, r := range recs {
		msgs = append(msgs, r.message())
	}

	ctx, cancel := ctxWith(t, 60*time.Second)
	defer cancel()
	if err := w.WriteMessages(ctx, msgs...); err != nil {
		failf(t, "acks=0 WriteMessages returned an error, which acks=0 cannot produce: %v", err)
	}
	if err := w.Close(); err != nil {
		failf(t, "closing the acks=0 writer: %v", err)
	}
	okf(t, "acks=0 write of %d records returned no error (it cannot: there is no response frame)", len(recs))

	got := drainPartitions(t, topic, width, len(recs), 60*time.Second)
	verifyCorpus(t, recs, got)
	okf(t, "the records landed anyway, byte-exact; their reported offsets are client-invented and not asserted")
}

// TestAutoCreateOnProduce is the auto-create bar: write to a topic that has
// never been named, with kafka-go's AllowAutoTopicCreation, and read it back.
func TestAutoCreateOnProduce(t *testing.T) {
	section(t, "Auto-create: produce to a topic that does not exist")

	topic := topicName("autocreate")
	width := topicWidth(t)

	// Deliberately no waitForTopic: the point is that the Writer creates it.
	recs := corpus(width, 4)
	w := newWriter(topic, kafka.RequireAll)
	msgs := make([]kafka.Message, 0, len(recs))
	for _, r := range recs {
		msgs = append(msgs, r.message())
	}
	ctx, cancel := ctxWith(t, 90*time.Second)
	defer cancel()

	err := w.WriteMessages(ctx, msgs...)
	if err != nil {
		// kafka-go retries UnknownTopicOrPartition internally while the topic
		// materialises; a hard failure here is the finding.
		failf(t, "AllowAutoTopicCreation write to a fresh topic %s: %v", topic, err)
	}
	if err := w.Close(); err != nil {
		failf(t, "closing the auto-create writer: %v", err)
	}
	okf(t, "Writer{AllowAutoTopicCreation:true} created %s and wrote %d records", topic, len(recs))

	ctx2, cancel2 := ctxWith(t, 20*time.Second)
	defer cancel2()
	resp, err := client().Metadata(ctx2, &kafka.MetadataRequest{Topics: []string{topic}})
	if err != nil {
		failf(t, "Metadata after auto-create: %v", err)
	}
	for _, tp := range resp.Topics {
		if tp.Name != topic {
			continue
		}
		if tp.Error != nil {
			failf(t, "auto-created %s carries error %v", topic, tp.Error)
		}
		if len(tp.Partitions) != width {
			failf(t, "auto-created %s has %d partitions, want QUEEN_KAFKA_DEFAULT_PARTITIONS=%d", topic, len(tp.Partitions), width)
		}
	}
	okf(t, "auto-created %s is %d partitions wide, the facade default", topic, width)

	got := drainPartitions(t, topic, width, len(recs), 60*time.Second)
	verifyCorpus(t, recs, got)
}

// TestAutoCreateRefusedIsNotRefused records a documented deliberate deviation
// rather than asserting a bug: PLAN_QUEEN_KAFKA.md says auto-create cannot be
// refused on Metadata v0-v3 because there is no wire field for it, and the
// facade creates on a bare Metadata naming the topic regardless. So a kafka-go
// Writer with AllowAutoTopicCreation=false still ends up with a topic.
func TestAutoCreateRefusedIsNotRefused(t *testing.T) {
	section(t, "Auto-create with AllowAutoTopicCreation=false (documented deviation)")

	topic := topicName("noautocreate")
	width := topicWidth(t)

	w := newWriter(topic, kafka.RequireAll, func(w *kafka.Writer) {
		w.AllowAutoTopicCreation = false
	})
	defer w.Close() //nolint:errcheck // the outcome is reported below, not here

	recs := corpus(width, 2)
	msgs := make([]kafka.Message, 0, len(recs))
	for _, r := range recs {
		msgs = append(msgs, r.message())
	}
	ctx, cancel := ctxWith(t, 60*time.Second)
	defer cancel()

	if err := w.WriteMessages(ctx, msgs...); err != nil {
		note("AllowAutoTopicCreation=false: write refused with %v (a real Kafka with auto.create.topics.enable=false behaves this way)", err)
		return
	}
	note("AllowAutoTopicCreation=false: the topic was created anyway and the write succeeded")
	note("that is PLAN_QUEEN_KAFKA.md's stated deviation - auto-create cannot be refused on Metadata v0-v3, there is no wire field - not a defect")
	okf(t, "recorded, not asserted")
}

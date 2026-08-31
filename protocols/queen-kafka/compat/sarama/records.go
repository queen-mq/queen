package main

import (
	"bytes"
	"fmt"

	"github.com/IBM/sarama"
)

// state is what `produce` fills and the later scenarios read.
type state struct {
	env    env
	topic  string
	group  string
	want   []*fixture // production order
	byPart map[int32][]*fixture
}

// fixture is one record and everything about it that has to survive the round
// trip byte for byte: a null key is not an empty key, a header with a nil value
// is not one with an empty value, and a value of all 256 byte values is not
// text.
type fixture struct {
	idx       int
	label     string
	partition int32
	key       []byte // nil means NULL, []byte{} means empty
	value     []byte // same
	headers   []sarama.RecordHeader
	offset    int64 // filled in by produce
}

func (f *fixture) message(topic string) *sarama.ProducerMessage {
	m := &sarama.ProducerMessage{Topic: topic, Partition: f.partition}
	if f.key != nil {
		m.Key = sarama.ByteEncoder(f.key)
	}
	if f.value != nil {
		m.Value = sarama.ByteEncoder(f.value)
	}
	if len(f.headers) > 0 {
		m.Headers = f.headers
	}
	return m
}

// compare returns the reasons msg is not a byte-exact round trip of f, empty
// when it is.
func (f *fixture) compare(msg *sarama.ConsumerMessage) []string {
	var bad []string
	if !bytes.Equal(f.key, msg.Key) {
		bad = append(bad, fmt.Sprintf("key %q != %q", f.key, msg.Key))
	}
	if (f.key == nil) != (msg.Key == nil) {
		bad = append(bad, fmt.Sprintf("key nullness: produced null=%v, consumed null=%v", f.key == nil, msg.Key == nil))
	}
	if !bytes.Equal(f.value, msg.Value) {
		bad = append(bad, fmt.Sprintf("value len %d != %d", len(f.value), len(msg.Value)))
	}
	if (f.value == nil) != (msg.Value == nil) {
		bad = append(bad, fmt.Sprintf("value nullness: produced null=%v, consumed null=%v", f.value == nil, msg.Value == nil))
	}
	if len(f.headers) != len(msg.Headers) {
		bad = append(bad, fmt.Sprintf("header count %d != %d", len(f.headers), len(msg.Headers)))
		return bad
	}
	for i := range f.headers {
		h, g := f.headers[i], msg.Headers[i]
		if !bytes.Equal(h.Key, g.Key) {
			bad = append(bad, fmt.Sprintf("header[%d] key %q != %q", i, h.Key, g.Key))
		}
		if !bytes.Equal(h.Value, g.Value) {
			bad = append(bad, fmt.Sprintf("header[%d] value %q != %q", i, h.Value, g.Value))
		}
		if (h.Value == nil) != (g.Value == nil) {
			bad = append(bad, fmt.Sprintf("header[%d] value nullness: %v != %v", i, h.Value == nil, g.Value == nil))
		}
	}
	return bad
}

// buildFixtures makes n records spread round-robin over parts partitions. The
// first n-12 are ordinary keyed records with two headers each; the last twelve
// are the edges — null and empty keys and values, non-UTF-8 bytes, a 64 KiB
// value, duplicate header keys, no headers at all, and a value that is every
// byte from 0 to 255.
func buildFixtures(n int, parts int32, tag string) []*fixture {
	out := make([]*fixture, 0, n)
	ordinary := n - 12
	if ordinary < 0 {
		ordinary = n
	}
	for i := 0; i < ordinary; i++ {
		out = append(out, &fixture{
			idx:       i,
			label:     "ordinary",
			partition: int32(i) % parts,
			key:       []byte(fmt.Sprintf("%s-k-%05d", tag, i)),
			value:     payload(i),
			headers: []sarama.RecordHeader{
				{Key: []byte("trace-id"), Value: []byte(fmt.Sprintf("%s-%05d", tag, i))},
				{Key: []byte("content-type"), Value: []byte("application/octet-stream")},
			},
		})
	}
	if ordinary == n {
		return out
	}

	big := bytes.Repeat([]byte("QueenMQ"), 9363)[:65536] // 64 KiB exactly
	all256 := make([]byte, 256)
	for i := range all256 {
		all256[i] = byte(i)
	}
	edges := []*fixture{
		{label: "null key", key: nil, value: []byte("no key at all"),
			headers: []sarama.RecordHeader{{Key: []byte("h"), Value: []byte("1")}}},
		{label: "empty key", key: []byte{}, value: []byte("empty key"),
			headers: []sarama.RecordHeader{{Key: []byte("h"), Value: []byte("2")}}},
		{label: "null value", key: []byte("null-value"), value: nil},
		{label: "empty value", key: []byte("empty-value"), value: []byte{}},
		{label: "non-utf8 key and value", key: []byte{0xff, 0xfe, 0x00, 0x01, 0x80},
			value: []byte{0x00, 0xc3, 0x28, 0xa0, 0xa1, 0xff}},
		{label: "64 KiB value", key: []byte("big"), value: big},
		{label: "header with empty value", key: []byte("hdr-empty"), value: []byte("x"),
			headers: []sarama.RecordHeader{{Key: []byte("empty"), Value: []byte{}}}},
		{label: "header with null value", key: []byte("hdr-null"), value: []byte("x"),
			headers: []sarama.RecordHeader{{Key: []byte("null"), Value: nil}}},
		{label: "duplicate header keys", key: []byte("hdr-dup"), value: []byte("x"),
			headers: []sarama.RecordHeader{
				{Key: []byte("dup"), Value: []byte("first")},
				{Key: []byte("dup"), Value: []byte("second")},
				{Key: []byte("dup"), Value: []byte("third")},
			}},
		{label: "no headers", key: []byte("hdr-none"), value: []byte("x")},
		{label: "unicode everywhere", key: []byte("clé-👑"), value: []byte("regina piùmata — ✓"),
			headers: []sarama.RecordHeader{{Key: []byte("ключ"), Value: []byte("значение")}}},
		{label: "every byte 0..255", key: []byte("all256"), value: all256},
	}
	for j, f := range edges {
		f.idx = ordinary + j
		f.partition = int32(f.idx) % parts
		out = append(out, f)
	}
	return out
}

func payload(i int) []byte {
	n := 64 + i%193
	b := make([]byte, n)
	for j := range b {
		b[j] = byte((i*31 + j*7) % 251)
	}
	return b
}

func byPartition(fs []*fixture) map[int32][]*fixture {
	m := map[int32][]*fixture{}
	for _, f := range fs {
		m[f.partition] = append(m[f.partition], f)
	}
	return m
}

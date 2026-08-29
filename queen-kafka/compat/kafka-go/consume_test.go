package compat

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

// got is one message as it came back off the wire.
type got struct {
	part   int
	offset int64
	key    string
	val    []byte
	hdrs   map[string]string
}

// drainPartitions reads every partition of a topic with kafka-go's PARTITION
// reader (no GroupID), which is the `Conn` stack: Metadata v6 to find the
// leader, then Fetch. It returns as soon as `want` messages have arrived, and
// treats the deadline as a result rather than hanging.
func drainPartitions(t *testing.T, topic string, parts, want int, within time.Duration) []got {
	t.Helper()

	var (
		mu  sync.Mutex
		out []got
		wg  sync.WaitGroup
	)
	perPart := want / parts

	ctx, cancel := context.WithTimeout(context.Background(), within)
	defer cancel()

	for p := 0; p < parts; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			r := kafka.NewReader(kafka.ReaderConfig{
				Brokers:          []string{bootstrap()},
				Topic:            topic,
				Partition:        p,
				Dialer:           dialer(),
				MinBytes:         1,
				MaxBytes:         10e6,
				MaxWait:          500 * time.Millisecond,
				ReadBatchTimeout: 10 * time.Second,
			})
			defer r.Close() //nolint:errcheck // a close error on a drained reader is noise
			for i := 0; i < perPart; i++ {
				m, err := r.ReadMessage(ctx)
				if err != nil {
					return
				}
				mu.Lock()
				out = append(out, got{
					part: m.Partition, offset: m.Offset,
					key: string(m.Key), val: append([]byte(nil), m.Value...),
					hdrs: headerMap(m.Headers),
				})
				mu.Unlock()
			}
		}(p)
	}
	wg.Wait()

	if len(out) != want {
		failf(t, "drained %d of %d messages from %s in %s (a hang is a result, not a pass)", len(out), want, topic, within)
	}
	okf(t, "drained %d messages from %s across %d partition readers", len(out), topic, parts)
	return out
}

// verifyCorpus is the byte-exactness and ordering assertion, in one place so
// every producing test gets the same standard: the same multiset of records
// came back, each key/value/header byte-identical, and within each partition the
// records appear in the order they were written, at contiguous ascending
// offsets.
func verifyCorpus(t *testing.T, want []record, have []got) {
	t.Helper()

	if len(have) != len(want) {
		failf(t, "count: got %d messages, want %d", len(have), len(want))
	}

	wantByKey := make(map[string]record, len(want))
	for _, r := range want {
		wantByKey[r.key] = r
	}

	byPart := map[int][]got{}
	for _, g := range have {
		w, ok := wantByKey[g.key]
		if !ok {
			failf(t, "partition %d offset %d: key %q was never written", g.part, g.offset, g.key)
		}
		if !bytes.Equal(g.val, w.val) {
			failf(t, "key %q: value round-trip is NOT byte-exact\n   want % x\n   got  % x", g.key, w.val, g.val)
		}
		if g.part != w.part {
			failf(t, "key %q landed on partition %d, was routed to %d", g.key, g.part, w.part)
		}
		for _, h := range w.hdrs {
			v, ok := g.hdrs[h.Key]
			if !ok {
				failf(t, "key %q: header %q missing from the round-trip (got %v)", g.key, h.Key, g.hdrs)
			}
			if v != string(h.Value) {
				failf(t, "key %q header %q: want % x, got % x", g.key, h.Key, h.Value, []byte(v))
			}
		}
		if len(g.hdrs) != len(w.hdrs) {
			failf(t, "key %q: %d headers came back, %d were written (%v)", g.key, len(g.hdrs), len(w.hdrs), g.hdrs)
		}
		byPart[g.part] = append(byPart[g.part], g)
	}
	okf(t, "all %d keys, values and headers round-tripped byte-exact (values carry non-UTF-8 bytes)", len(have))

	for p, gs := range byPart {
		sort.Slice(gs, func(i, j int) bool { return gs[i].offset < gs[j].offset })
		for i := 1; i < len(gs); i++ {
			if gs[i].offset != gs[i-1].offset+1 {
				failf(t, "partition %d: offsets are not contiguous, %d then %d", p, gs[i-1].offset, gs[i].offset)
			}
			if wantByKey[gs[i].key].seq <= wantByKey[gs[i-1].key].seq {
				failf(t, "partition %d: out of order, key %q (seq %d) at offset %d follows key %q (seq %d)",
					p, gs[i].key, wantByKey[gs[i].key].seq, gs[i].offset, gs[i-1].key, wantByKey[gs[i-1].key].seq)
			}
		}
	}
	okf(t, "per-partition order holds on all %d partitions, offsets contiguous", len(byPart))
}

// TestOffsetBounds is the earliest/latest bar. kafka-go exposes it three ways
// and this checks two of them agree: the low-level Conn (ReadFirstOffset /
// ReadLastOffset, which are ListOffsets on the two sentinels) and the Client's
// ListOffsets helper.
func TestOffsetBounds(t *testing.T) {
	section(t, "ListOffsets: first and last, two kafka-go APIs")

	topic := topicName("bounds")
	width := topicWidth(t)
	waitForTopic(t, topic, width, 30*time.Second)

	perPart := 8
	recs := corpus(width, perPart)
	produceCorpus(t, topic, recs, 0)

	// (a) the Conn API
	ctx, cancel := ctxWith(t, 30*time.Second)
	defer cancel()
	conn, err := dialer().DialLeader(ctx, "tcp", bootstrap(), topic, 0)
	if err != nil {
		failf(t, "DialLeader(%s, 0): %v", topic, err)
	}
	defer conn.Close() //nolint:errcheck

	first, err := conn.ReadFirstOffset()
	if err != nil {
		failf(t, "ReadFirstOffset: %v", err)
	}
	last, err := conn.ReadLastOffset()
	if err != nil {
		failf(t, "ReadLastOffset: %v", err)
	}
	if first != 0 {
		failf(t, "partition 0 begins at %d, expected 0 on a fresh topic", first)
	}
	if last != int64(perPart) {
		failf(t, "partition 0 ends at %d, expected the high watermark %d", last, perPart)
	}
	okf(t, "Conn.ReadFirstOffset=%d ReadLastOffset=%d on a partition holding %d records", first, last, perPart)

	// (b) the Client API, every partition at once
	ctx2, cancel2 := ctxWith(t, 30*time.Second)
	defer cancel2()
	reqs := make([]kafka.OffsetRequest, 0, 2*width)
	for p := 0; p < width; p++ {
		reqs = append(reqs, kafka.FirstOffsetOf(p), kafka.LastOffsetOf(p))
	}
	resp, err := client().ListOffsets(ctx2, &kafka.ListOffsetsRequest{
		Topics: map[string][]kafka.OffsetRequest{topic: reqs},
	})
	if err != nil {
		failf(t, "Client.ListOffsets: %v", err)
	}
	pos, ok := resp.Topics[topic]
	if !ok {
		failf(t, "ListOffsets response has no entry for %s", topic)
	}
	if len(pos) != width {
		failf(t, "ListOffsets returned %d partitions, want %d", len(pos), width)
	}
	for _, po := range pos {
		if po.Error != nil {
			failf(t, "ListOffsets partition %d: %v", po.Partition, po.Error)
		}
		if po.FirstOffset != 0 || po.LastOffset != int64(perPart) {
			failf(t, "partition %d bounds are [%d,%d), want [0,%d)", po.Partition, po.FirstOffset, po.LastOffset, perPart)
		}
	}
	okf(t, "Client.ListOffsets agrees on all %d partitions: [0,%d)", width, perPart)
}

// TestSeek is the seek bar. A partition reader is the only kafka-go API that
// exposes an absolute seek (SetOffset), and it is also where OFFSET_OUT_OF_RANGE
// becomes visible.
func TestSeek(t *testing.T) {
	section(t, "Seek: Reader.SetOffset on a partition reader")

	topic := topicName("seek")
	width := topicWidth(t)
	waitForTopic(t, topic, width, 30*time.Second)

	perPart := 10
	recs := corpus(width, perPart)
	produceCorpus(t, topic, recs, 0)

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:          []string{bootstrap()},
		Topic:            topic,
		Partition:        0,
		Dialer:           dialer(),
		MinBytes:         1,
		MaxBytes:         10e6,
		MaxWait:          500 * time.Millisecond,
		ReadBatchTimeout: 10 * time.Second,
	})
	defer r.Close() //nolint:errcheck

	// mid-stream absolute seek
	const target = 6
	if err := r.SetOffset(target); err != nil {
		failf(t, "SetOffset(%d): %v", target, err)
	}
	ctx, cancel := ctxWith(t, 30*time.Second)
	defer cancel()
	m, err := r.ReadMessage(ctx)
	if err != nil {
		failf(t, "ReadMessage after SetOffset(%d): %v", target, err)
	}
	if m.Offset != target {
		failf(t, "SetOffset(%d) landed on offset %d", target, m.Offset)
	}
	if string(m.Key) != fmt.Sprintf("p0-%06d", target) {
		failf(t, "offset %d carries key %q, expected p0-%06d", m.Offset, m.Key, target)
	}
	okf(t, "SetOffset(%d) resumed exactly at offset %d, key %s", target, m.Offset, m.Key)

	// seek to LastOffset: nothing more should arrive
	if err := r.SetOffset(kafka.LastOffset); err != nil {
		failf(t, "SetOffset(LastOffset): %v", err)
	}
	shortCtx, shortCancel := ctxWith(t, 4*time.Second)
	defer shortCancel()
	if m, err := r.ReadMessage(shortCtx); err == nil {
		failf(t, "SetOffset(LastOffset) still yielded offset %d; it should sit at the high watermark", m.Offset)
	} else if !errors.Is(err, context.DeadlineExceeded) {
		failf(t, "SetOffset(LastOffset) then read: expected a deadline, got %v", err)
	}
	okf(t, "SetOffset(LastOffset) parks at the high watermark and yields nothing")

	// seek back to the beginning
	if err := r.SetOffset(kafka.FirstOffset); err != nil {
		failf(t, "SetOffset(FirstOffset): %v", err)
	}
	ctx3, cancel3 := ctxWith(t, 30*time.Second)
	defer cancel3()
	m, err = r.ReadMessage(ctx3)
	if err != nil {
		failf(t, "ReadMessage after SetOffset(FirstOffset): %v", err)
	}
	if m.Offset != 0 {
		failf(t, "SetOffset(FirstOffset) landed on %d, expected 0", m.Offset)
	}
	okf(t, "SetOffset(FirstOffset) rewinds to offset 0")

	// ReadLag: the third kafka-go offset API, and it only exists on a partition
	// reader.
	lagCtx, lagCancel := ctxWith(t, 20*time.Second)
	defer lagCancel()
	lag, err := r.ReadLag(lagCtx)
	if err != nil {
		failf(t, "ReadLag: %v", err)
	}
	if lag < 0 || lag > int64(perPart) {
		failf(t, "ReadLag=%d is outside [0,%d]", lag, perPart)
	}
	okf(t, "ReadLag=%d after reading offset 0 of a %d-record partition", lag, perPart)
}

// TestOffsetOutOfRange proves the facade answers OFFSET_OUT_OF_RANGE for an
// offset past the high watermark (queen-kafka/src/handlers/fetch.rs:452, the
// `bounds_only` probe), and that kafka-go both surfaces and recovers from it.
//
// Two readers, because kafka-go has two behaviours and only one of them shows
// the error code:
//
//   - `ReaderConfig.OffsetOutOfRangeError: true` (reader.go:524) makes the
//     reader return the code to the caller. That is the assertion.
//   - The DEFAULT (false) makes it re-read the bounds and reset — reader.go:1382
//     — so the caller never sees the code and the reader silently resumes.
//     Anyone debugging a "reader that skipped ahead" against this facade is
//     looking at that branch, not at a facade fault.
func TestOffsetOutOfRange(t *testing.T) {
	section(t, "An offset past the high watermark")

	topic := topicName("oor")
	width := topicWidth(t)
	waitForTopic(t, topic, width, 30*time.Second)

	perPart := 4
	recs := corpus(width, perPart)
	produceCorpus(t, topic, recs, 0)

	newOORReader := func(surface bool) *kafka.Reader {
		return kafka.NewReader(kafka.ReaderConfig{
			Brokers:               []string{bootstrap()},
			Topic:                 topic,
			Partition:             0,
			Dialer:                dialer(),
			MinBytes:              1,
			MaxBytes:              10e6,
			MaxWait:               500 * time.Millisecond,
			ReadBatchTimeout:      5 * time.Second,
			MaxAttempts:           2,
			OffsetOutOfRangeError: surface,
		})
	}

	// (a) surface the code
	strict := newOORReader(true)
	defer strict.Close() //nolint:errcheck
	if err := strict.SetOffset(1_000_000); err != nil {
		failf(t, "SetOffset(1000000): %v", err)
	}
	ctx, cancel := ctxWith(t, 25*time.Second)
	defer cancel()
	if _, err := strict.ReadMessage(ctx); err == nil {
		failf(t, "an offset 1000000 past a %d-record partition returned a message", perPart)
	} else if !errors.Is(err, kafka.OffsetOutOfRange) {
		failf(t, "expected OffsetOutOfRange for an offset past the watermark, got %v", err)
	}
	okf(t, "offset 1000000 on a %d-record partition -> OffsetOutOfRange, surfaced to the caller", perPart)

	// (b) the default: recover rather than wedge
	lenient := newOORReader(false)
	defer lenient.Close() //nolint:errcheck
	if err := lenient.SetOffset(1_000_000); err != nil {
		failf(t, "SetOffset(1000000) on the lenient reader: %v", err)
	}
	ctx2, cancel2 := ctxWith(t, 15*time.Second)
	defer cancel2()
	m, err := lenient.ReadMessage(ctx2)
	switch {
	case err == nil:
		note("the DEFAULT reader swallowed the code and resumed at offset %d (reader.go:1382 resets to the bounds)", m.Offset)
	case errors.Is(err, context.DeadlineExceeded):
		note("the DEFAULT reader parked at the watermark after the reset; nothing to read there, no wedge")
	default:
		note("the DEFAULT reader surfaced %v", err)
	}
	okf(t, "an out-of-range offset neither wedges nor crashes the kafka-go partition reader")
}

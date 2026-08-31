package main

// M7 F3: InitProducerId and the idempotent producer's sequence window, against
// the facade and against apache/kafka:3.9.1 side by side.
//
// Two scenarios rather than one, because they answer two different kinds of
// question and classify under two prefixes:
//
//   - `initproducerid`: the GRANT. A fresh id, an epoch bump, and what a
//     transactional id gets. The producer id VALUE is never diffed — Kafka
//     allocates from controller blocks and this facade mints 62 bits of
//     entropy, so the only comparable facts are its shape and its epoch.
//   - `idempotent`: the WINDOW, driven by hand-built record batches with a
//     fixed (producer id, epoch, base sequence). This is the part no client
//     library will do on purpose, and it is the only way to ask both brokers
//     the same question about a duplicate.
//
// Everything here was written as a MEASUREMENT first: the answers Apache Kafka
// gives a duplicate, a gap and an unknown producer are the answers this facade
// was then made to match. Where it deliberately does not match, main.go carries
// the reason.

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func init() {
	scenarios = append(scenarios,
		scenario{
			name: "initproducerid",
			desc: "the producer id an idempotent producer is granted, the epoch bump it asks for, and what a transactional id gets",
			run:  scenInitProducerID,
		},
		scenario{
			name: "idempotent",
			desc: "the sequence window: a duplicate batch, a gap, and a producer the broker has never heard of",
			run:  scenIdempotent,
		},
	)
}

// The version both brokers negotiate. The facade advertises 0..=4 (v5 is
// KIP-890's transaction protocol 2, which is refused here); Kafka 3.9.1
// supports 4 as well, so one version goes to both and the answers compare.
const vInitProducerID = 4

// Produce v7 is the version the window is exercised at: the first version that
// carries zstd, well inside both brokers' ranges, and low enough that the
// response shape is the plain one.
const vProduceIdempotent = 7

// ------------------------------------------------------------------ the grant

func scenInitProducerID(c *runctx) {
	k, err := c.target.dial()
	if err != nil {
		c.rec.bad("initproducerid.dial", err)
		return
	}
	defer k.Close()

	// 1. A fresh grant, exactly as a stock producer opens.
	fresh, err := initProducerID(k, nil, -1, -1)
	if err != nil {
		c.rec.bad("initproducerid.fresh.error_code", err)
		return
	}
	c.rec.add("initproducerid.fresh.error_code", "%s", errName(fresh.ErrorCode))
	c.rec.add("initproducerid.fresh.epoch", "%d", fresh.ProducerEpoch)
	// The SHAPE and not the value: Kafka's ids come from the controller in
	// small contiguous blocks and start at 0 on a fresh cluster, this facade
	// mints 62 bits of entropy per grant. What both must agree on is that the
	// id is usable — non-negative and not the -1 sentinel.
	c.rec.add("initproducerid.fresh.id_is_usable", "%t", fresh.ProducerID >= 0)
	c.rec.add("initproducerid.fresh.throttle_ms", "%d", fresh.ThrottleMillis)
	c.rec.info("initproducerid.fresh.producer_id", "%d", fresh.ProducerID)

	// 2. Two grants are two producers. A broker that handed one id to two
	//    sessions would let one silently overwrite the other's window.
	second, err := initProducerID(k, nil, -1, -1)
	if err != nil {
		c.rec.bad("initproducerid.second.error_code", err)
	} else {
		c.rec.add("initproducerid.second.error_code", "%s", errName(second.ErrorCode))
		c.rec.add("initproducerid.second.is_a_different_id", "%t", second.ProducerID != fresh.ProducerID)
	}

	// 3. KIP-360's bump: the request carries the id and epoch the producer is
	//    on. This is where the facade DIVERGES on purpose — Kafka's
	//    non-transactional path blindly allocates a new id at epoch 0, the
	//    facade answers the same id one epoch higher — so both halves are
	//    recorded and main.go carries the reason.
	bumped, err := initProducerID(k, nil, fresh.ProducerID, fresh.ProducerEpoch)
	if err != nil {
		c.rec.bad("initproducerid.bump.error_code", err)
	} else {
		c.rec.add("initproducerid.bump.error_code", "%s", errName(bumped.ErrorCode))
		c.rec.add("initproducerid.bump.keeps_the_id", "%t", bumped.ProducerID == fresh.ProducerID)
		c.rec.add("initproducerid.bump.epoch", "%d", bumped.ProducerEpoch)
		c.rec.info("initproducerid.bump.producer_id", "%d", bumped.ProducerID)
	}

	// 4. A transactional id. Both refuse; only the CODE is comparable, and
	//    Kafka 3.9 with no authorizer answers this differently from a facade
	//    that has no transaction coordinator at all.
	txn := "diff-txn-" + c.runID
	tx, err := initProducerID(k, &txn, -1, -1)
	if err != nil {
		c.rec.bad("initproducerid.transactional.error_code", err)
	} else {
		c.rec.add("initproducerid.transactional.error_code", "%s", errName(tx.ErrorCode))
		c.rec.add("initproducerid.transactional.granted_an_id", "%t", tx.ProducerID >= 0)
	}

	// 5. An EMPTY transactional id, which is what brod's hand-rolled encoder
	//    writes for a null one. Both brokers must read it as "not
	//    transactional" or every Elixir producer is refused.
	empty := ""
	e, err := initProducerID(k, &empty, -1, -1)
	if err != nil {
		c.rec.bad("initproducerid.empty_transactional.error_code", err)
	} else {
		c.rec.add("initproducerid.empty_transactional.error_code", "%s", errName(e.ErrorCode))
		c.rec.add("initproducerid.empty_transactional.id_is_usable", "%t", e.ProducerID >= 0)
	}
}

func initProducerID(k *conn, transactionalID *string, producerID int64, epoch int16) (*kmsg.InitProducerIDResponse, error) {
	req := kmsg.NewInitProducerIDRequest()
	req.TransactionalID = transactionalID
	req.TransactionTimeoutMillis = 60_000
	req.ProducerID = producerID
	req.ProducerEpoch = epoch
	resp, _, err := k.doT(&req, vInitProducerID, 20*time.Second)
	if err != nil {
		return nil, err
	}
	return resp.(*kmsg.InitProducerIDResponse), nil
}

// ----------------------------------------------------------------- the window

func scenIdempotent(c *runctx) {
	k, err := c.target.dial()
	if err != nil {
		c.rec.bad("idempotent.dial", err)
		return
	}
	defer k.Close()

	topic := c.topic("idem")
	if err := ensureTopic(k, topic, c.parts); err != nil {
		c.rec.bad("idempotent.topic", err)
		return
	}

	grant, err := initProducerID(k, nil, -1, -1)
	if err != nil {
		c.rec.bad("idempotent.grant", err)
		return
	}
	if grant.ErrorCode != 0 {
		c.rec.add("idempotent.grant", "%s", errName(grant.ErrorCode))
		return
	}
	pid, epoch := grant.ProducerID, grant.ProducerEpoch

	// 1. The first batch of a fresh producer: sequence 0.
	first := idempotentProduceOnce(k, topic, pid, epoch, 0, []string{"a", "b", "c"})
	c.rec.add("idempotent.first.error_code", "%s", errName(first.code))
	c.rec.add("idempotent.first.base_offset", "%d", first.baseOffset)

	// 2. THE row: the identical batch again. Kafka answers this as a SUCCESS
	//    carrying the offsets the original got, which is what makes a
	//    producer's retry after a lost response invisible.
	dup := idempotentProduceOnce(k, topic, pid, epoch, 0, []string{"a", "b", "c"})
	c.rec.add("idempotent.duplicate.error_code", "%s", errName(dup.code))
	c.rec.add("idempotent.duplicate.repeats_the_base_offset", "%t", dup.baseOffset == first.baseOffset)

	// 3. ...and the log holds them once. Asked through the high watermark
	//    rather than by reading, because the number is the whole claim.
	hw, err := endOffset(k, topic)
	if err != nil {
		c.rec.bad("idempotent.duplicate.high_watermark", err)
	} else {
		c.rec.add("idempotent.duplicate.high_watermark", "%d", hw)
	}

	// 4. A gap. Both must refuse it and write nothing, or "idempotent" would be
	//    a claim about duplicates that said nothing about order.
	gap := idempotentProduceOnce(k, topic, pid, epoch, 9, []string{"gap"})
	c.rec.add("idempotent.gap.error_code", "%s", errName(gap.code))
	if hw2, err := endOffset(k, topic); err == nil {
		c.rec.add("idempotent.gap.wrote_nothing", "%t", hw2 == hw)
	} else {
		c.rec.bad("idempotent.gap.wrote_nothing", err)
	}

	// 5. The batch that WAS next still is: a refused gap must not move the
	//    window, or the producer's re-drain would be refused too.
	next := idempotentProduceOnce(k, topic, pid, epoch, 3, []string{"d"})
	c.rec.add("idempotent.after_gap.error_code", "%s", errName(next.code))

	// 6. A producer id nobody granted, at a sequence that is not 0. This is the
	//    shape a facade restart, an eviction or an expired producer snapshot
	//    leaves behind, and the code decides whether the client recovers or
	//    dies. Kafka's own answer moved from UNKNOWN_PRODUCER_ID to
	//    OUT_OF_ORDER_SEQUENCE_NUMBER, and this records which one each gives.
	stranded := idempotentProduceOnce(k, topic, pid+7_777_777, 0, 42, []string{"stranded"})
	c.rec.add("idempotent.unknown_producer.error_code", "%s", errName(stranded.code))

	// 7. An epoch below the one the broker has seen.
	if bumped := idempotentProduceOnce(k, topic, pid, epoch+1, 0, []string{"e"}); true {
		c.rec.add("idempotent.epoch_bump.error_code", "%s", errName(bumped.code))
	}
	stale := idempotentProduceOnce(k, topic, pid, epoch, 4, []string{"stale"})
	c.rec.add("idempotent.stale_epoch.error_code", "%s", errName(stale.code))
}

// endOffset is the partition's high watermark, asked the way a client asks:
// ListOffsets with the LATEST sentinel. It is the whole claim of the duplicate
// row — "the records are in the log once" is a NUMBER.
func endOffset(k *conn, topic string) (int64, error) {
	p, err := listOffsets(k, 5, topic, 0, -1)
	if err != nil {
		return 0, err
	}
	if p.ErrorCode != 0 {
		return 0, fmt.Errorf("ListOffsets answered %s", errName(p.ErrorCode))
	}
	return p.Offset, nil
}

type produceAnswer struct {
	code       int16
	baseOffset int64
}

func idempotentProduceOnce(k *conn, topic string, pid int64, epoch int16, baseSeq int32, values []string) produceAnswer {
	req := kmsg.NewProduceRequest()
	req.Acks = -1
	req.TimeoutMillis = 30_000
	rt := kmsg.NewProduceRequestTopic()
	rt.Topic = topic
	rp := kmsg.NewProduceRequestTopicPartition()
	rp.Partition = 0
	rp.Records = idempotentBatch(pid, epoch, baseSeq, values)
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)

	resp, _, err := k.doT(&req, vProduceIdempotent, 30*time.Second)
	if err != nil {
		return produceAnswer{code: -2, baseOffset: -1}
	}
	pr := resp.(*kmsg.ProduceResponse)
	if len(pr.Topics) != 1 || len(pr.Topics[0].Partitions) != 1 {
		return produceAnswer{code: -2, baseOffset: -1}
	}
	p := pr.Topics[0].Partitions[0]
	return produceAnswer{code: p.ErrorCode, baseOffset: p.BaseOffset}
}

// idempotentBatch encodes a RecordBatch v2 with an EXACT producer id, epoch and
// base sequence, filling in the two fields kmsg leaves to the caller: the
// length of everything after it, and the Castagnoli CRC of everything after
// that. A wrong CRC is answered CORRUPT_MESSAGE and would look like an
// idempotence divergence, so neither is guessed.
func idempotentBatch(producerID int64, epoch int16, baseSeq int32, values []string) []byte {
	// A FIXED timestamp, not `now`: the two brokers are driven seconds apart
	// and the bytes of the batch must be identical on both, since the batch is
	// what is being compared.
	const ts = 1_756_000_000_000
	var records []byte
	for i, v := range values {
		r := kmsg.Record{OffsetDelta: int32(i), Value: []byte(v)}
		probe := r.AppendTo(nil)
		r.Length = int32(len(probe) - 1)
		records = append(records, r.AppendTo(nil)...)
	}
	b := kmsg.RecordBatch{
		PartitionLeaderEpoch: -1,
		Magic:                2,
		LastOffsetDelta:      int32(len(values) - 1),
		FirstTimestamp:       ts,
		MaxTimestamp:         ts,
		ProducerID:           producerID,
		ProducerEpoch:        epoch,
		FirstSequence:        baseSeq,
		NumRecords:           int32(len(values)),
		Records:              records,
	}
	raw := b.AppendTo(nil)
	binary.BigEndian.PutUint32(raw[8:12], uint32(len(raw)-12))
	binary.BigEndian.PutUint32(raw[17:21],
		crc32.Checksum(raw[21:], crc32.MakeTable(crc32.Castagnoli)))
	return raw
}

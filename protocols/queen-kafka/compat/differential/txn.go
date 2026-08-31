package main

// M9: transactions, against the facade and against apache/kafka:3.9.1 side by
// side. One scenario, `transactions`, with no entry in main.go's `order` map —
// it depends on nothing another scenario does, and the sort pushes unknown
// names to the end.
//
// The shape of the questions, in the order they are asked:
//
//   - the COORDINATOR. FindCoordinator at key_type 1, which before M9 was
//     answered COORDINATOR_NOT_AVAILABLE and cost the Java client the whole of
//     max.block.ms (find_coordinator.rs, "The TRANSACTION coordinator").
//   - a COMMIT. InitProducerId(T) -> AddPartitionsToTxn -> two transactional
//     Produce v7 batches -> EndTxn(commit), then the log end offset and a raw
//     read_committed Fetch.
//   - an ABORT, the same shape, ending in EndTxn(abort).
//   - the CONSUME-TRANSFORM-PRODUCE bundle: AddOffsetsToTxn ->
//     TxnOffsetCommit -> EndTxn(commit), read back with OffsetFetch. This is
//     the Spring KafkaTransactionManager path and the reason TxnOffsetCommit
//     has to reach v3.
//   - the REFUSALS: EndTxn for a transactional id nobody opened, and each of
//     the four APIs one version above the facade's advertised ceiling.
//
// Everything is built by hand rather than driven through a client library, for
// the reason idempotent.go gives: a client rewrites the version of what it
// sends and will not produce an uncommitted batch on purpose, and both of those
// are the questions here. The client-visible half of M9 — a real Java producer,
// a franz-go GroupTransactSession, the fencing and crash cases — is
// compat/transactions, which is where a claim about EOS belongs.

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func init() {
	scenarios = append(scenarios, scenario{
		name: "transactions",
		desc: "the transaction coordinator, a commit, an abort, an offset bundle, and the refusals",
		run:  scenTransactions,
	})
}

// The version the four transaction APIs are asked at. The facade advertises
// 0..=3 for all four (versions.rs); Kafka 3.9.1 goes higher on every one of
// them, so v3 is the highest version both speak and the one the answers can be
// compared at. v4 is probed separately, at the bottom of this file.
const vTxn = 3

// Produce v7: the version idempotent.go already uses, well inside both
// brokers' windows, and the response shape is the plain one.
const vProduceTxn = 7

// Records per transaction. Small on purpose — this scenario is about what the
// two brokers ANSWER, not about volume, and the caps have their own home in
// compat/transactions scenario 9.
const txnRecords = 5

// ------------------------------------------------------------------ the run

func scenTransactions(c *runctx) {
	k, err := c.target.dial()
	if err != nil {
		c.rec.bad("transactions.dial", err)
		return
	}
	defer k.Close()

	commitPhase(c, k)
	abortPhase(c, k)
	offsetsPhase(c, k)
	refusalPhase(c, k)
	versionPhase(c)
}

// ------------------------------------------------------------- 1. the commit

func commitPhase(c *runctx, k *conn) {
	topic := c.topic("txc")
	if err := ensureTopic(k, topic, c.parts); err != nil {
		c.rec.bad("commit.topic", err)
		return
	}
	id := "dt-txn-commit-" + c.runID

	// The coordinator lookup a transactional client sends before anything
	// else. Recorded as a LIVENESS fact and not as an address: the node id,
	// host and port of the broker that answers are its own business, and
	// main.go does not diff a broker's identity anywhere else either.
	fc, err := findTxnCoordinator(k, id)
	if err != nil {
		c.rec.bad("coordinator.error_code", err)
	} else {
		c.rec.add("coordinator.error_code", "%s", errName(fc.ErrorCode))
		c.rec.add("coordinator.is_a_live_node", "%t", fc.Host != "" && fc.Port > 0)
		c.rec.info("coordinator.node", "%d at %s:%d", fc.NodeID, fc.Host, fc.Port)
	}

	pid, epoch, ok := initTxn(c, k, "init", id)
	if !ok {
		return
	}

	// One partition, not four. A commit marker is written per partition the
	// transaction touched, so a wider transaction would only repeat the
	// commit.log_end_offset divergence once per partition and say nothing new.
	code, err := addPartition(k, id, pid, epoch, topic, 0)
	if err != nil {
		c.rec.bad("commit.addpartitions.error_code", err)
		return
	}
	c.rec.add("commit.addpartitions.error_code", "%s", errName(code))
	if code != 0 {
		return
	}

	// The SAME partition again, which is what every client sends on every
	// record after the first to a partition it has already enrolled. A broker
	// that answered anything but NONE here would refuse the second batch of
	// every transaction there has ever been.
	if again, err := addPartition(k, id, pid, epoch, topic, 0); err != nil {
		c.rec.bad("commit.addpartitions.again.error_code", err)
	} else {
		c.rec.add("commit.addpartitions.again.error_code", "%s", errName(again))
	}

	first := txnProduce(k, id, topic, 0, pid, epoch, 0, values("txc", 0, txnRecords))
	c.rec.add("commit.produce1.error_code", "%s", errName(first.code))
	c.rec.add("commit.produce1.base_offset", "%d", first.baseOffset)

	second := txnProduce(k, id, topic, 0, pid, epoch, txnRecords, values("txc", txnRecords, txnRecords))
	c.rec.add("commit.produce2.error_code", "%s", errName(second.code))
	c.rec.add("commit.produce2.base_offset", "%d", second.baseOffset)

	// THE read_uncommitted question, asked while the transaction is still
	// open. Kafka's log holds the records already and hands them over; the
	// facade holds them in a stage that no read path can see. This is the one
	// place a consumer of this facade sees LESS than it would see of Kafka,
	// and it is the divergence §8.3 asks to be ratified.
	if f, err := fetchIso(k, topic, 0, 0, 0); err != nil {
		c.rec.bad("read_uncommitted.visible_before_commit", err)
	} else {
		c.rec.add("read_uncommitted.visible_before_commit", "%d", countPrefixed(f, "txc-"))
	}

	end, err := endTxn(k, id, pid, epoch, true)
	if err != nil {
		c.rec.bad("commit.endtxn.error_code", err)
		return
	}
	c.rec.add("commit.endtxn.error_code", "%s", errName(end))

	f, err := settle(k, topic, 0)
	if err != nil {
		c.rec.bad("commit.settled", err)
		return
	}

	// The log end offset after a committed transaction of 2*txnRecords
	// records. Kafka writes a commit marker into the partition and answers
	// N+1; the facade writes records and nothing else and answers N.
	if hw, err := endOffset(k, topic); err != nil {
		c.rec.bad("commit.log_end_offset", err)
	} else {
		c.rec.add("commit.log_end_offset", "%d", hw)
	}

	// ...and the records themselves are there once, and readable at
	// read_committed. Counted by VALUE rather than by batch, because Kafka's
	// answer also carries the control batch and the facade has none: the claim
	// is about the records an application receives.
	c.rec.add("commit.read_committed_records", "%d", countPrefixed(f, "txc-"))
	c.rec.add("commit.no_aborted_transactions", "%t", f.aborted == 0)
}

// -------------------------------------------------------------- 2. the abort

func abortPhase(c *runctx, k *conn) {
	topic := c.topic("txa")
	if err := ensureTopic(k, topic, c.parts); err != nil {
		c.rec.bad("abort.topic", err)
		return
	}
	id := "dt-txn-abort-" + c.runID

	pid, epoch, ok := initTxn(c, k, "abort.init", id)
	if !ok {
		return
	}
	if code, err := addPartition(k, id, pid, epoch, topic, 0); err != nil || code != 0 {
		c.rec.bad("abort.addpartitions", orErr(err, code))
		return
	}

	sent := txnProduce(k, id, topic, 0, pid, epoch, 0, values("txa", 0, txnRecords))
	c.rec.add("abort.produce1.error_code", "%s", errName(sent.code))
	c.rec.add("abort.produce1.base_offset", "%d", sent.baseOffset)

	code, err := endTxn(k, id, pid, epoch, false)
	if err != nil {
		c.rec.bad("abort.endtxn.error_code", err)
		return
	}
	c.rec.add("abort.endtxn.error_code", "%s", errName(code))

	f, err := settle(k, topic, 0)
	if err != nil {
		c.rec.bad("abort.settled", err)
		return
	}

	// Kafka leaves the aborted records in the log and adds an abort marker, so
	// N+1; the facade drops the stage and writes nothing at all, so 0.
	if hw, err := endOffset(k, topic); err != nil {
		c.rec.bad("abort.log_end_offset", err)
	} else {
		c.rec.add("abort.log_end_offset", "%d", hw)
	}

	// The two fields a read_committed CLIENT uses to do the filtering that
	// this facade never needs it to do. Kafka's list names the aborted
	// producer and its LSO trails; the facade's list is empty and its LSO is
	// the high watermark, always, because no uncommitted record ever entered
	// the log.
	c.rec.add("fetch.aborted_transactions", "%d", f.aborted)
	c.rec.add("fetch.last_stable_offset", "%d", f.lso)
}

// ------------------------------------------------- 3. the offsets in a bundle

// offsetsPhase is the consume-transform-produce shape: records to an output
// topic and the input group's offset committed in ONE transaction. It is the
// Spring KafkaTransactionManager path, it is what AddOffsetsToTxn and
// TxnOffsetCommit exist for, and on this facade it is the single POST that
// makes both atomic.
func offsetsPhase(c *runctx, k *conn) {
	topic := c.topic("txo")
	if err := ensureTopic(k, topic, c.parts); err != nil {
		c.rec.bad("ctp.topic", err)
		return
	}
	id := "dt-txn-ctp-" + c.runID
	group := c.group("txn")

	pid, epoch, ok := initTxn(c, k, "ctp.init", id)
	if !ok {
		return
	}
	if code, err := addPartition(k, id, pid, epoch, topic, 0); err != nil || code != 0 {
		c.rec.bad("ctp.addpartitions", orErr(err, code))
		return
	}
	out := txnProduce(k, id, topic, 0, pid, epoch, 0, values("txo", 0, txnRecords))
	c.rec.add("ctp.produce1.error_code", "%s", errName(out.code))
	c.rec.add("ctp.produce1.base_offset", "%d", out.baseOffset)

	addOff := kmsg.NewAddOffsetsToTxnRequest()
	addOff.TransactionalID = id
	addOff.ProducerID = pid
	addOff.ProducerEpoch = epoch
	addOff.Group = group
	resp, _, err := k.doT(&addOff, vTxn, 20*time.Second)
	if err != nil {
		c.rec.bad("ctp.addoffsets.error_code", err)
		return
	}
	c.rec.add("ctp.addoffsets.error_code", "%s", errName(resp.(*kmsg.AddOffsetsToTxnResponse).ErrorCode))

	// Generation -1 with an empty member id: the SIMPLE form, which is what a
	// producer that is not itself a group member sends and what both brokers
	// have to accept for a bare transactional producer to commit an offset at
	// all. A generation is only meaningful to a client inside a rebalance, and
	// this runner joins no group.
	tc := kmsg.NewTxnOffsetCommitRequest()
	tc.TransactionalID = id
	tc.Group = group
	tc.ProducerID = pid
	tc.ProducerEpoch = epoch
	tc.Generation = -1
	tc.MemberID = ""
	tct := kmsg.NewTxnOffsetCommitRequestTopic()
	tct.Topic = topic
	tcp := kmsg.NewTxnOffsetCommitRequestTopicPartition()
	tcp.Partition = 0
	tcp.Offset = 41
	tct.Partitions = []kmsg.TxnOffsetCommitRequestTopicPartition{tcp}
	tc.Topics = []kmsg.TxnOffsetCommitRequestTopic{tct}
	// Retried on the three coordinator codes for the reason initTxn is:
	// apache/kafka answers COORDINATOR_LOAD_IN_PROGRESS while the group
	// coordinator's __consumer_offsets partition is still loading, and a
	// scenario that recorded that would be recording the container's boot.
	deadline := time.Now().Add(60 * time.Second)
	var code int16
	for {
		tcResp, _, err := k.doT(&tc, vTxn, 20*time.Second)
		if err != nil {
			c.rec.bad("ctp.txnoffsetcommit.error_code", err)
			return
		}
		tr := tcResp.(*kmsg.TxnOffsetCommitResponse)
		if len(tr.Topics) != 1 || len(tr.Topics[0].Partitions) != 1 {
			c.rec.add("ctp.txnoffsetcommit.error_code",
				"a response with %d topics", len(tr.Topics))
			return
		}
		code = tr.Topics[0].Partitions[0].ErrorCode
		if !retriableCoordinator(code) || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Second)
	}
	c.rec.add("ctp.txnoffsetcommit.error_code", "%s", errName(code))

	// Before the commit the offset must NOT be readable, or the bundle would
	// not be a bundle. Kafka holds it pending in __consumer_offsets; the
	// facade holds it in the stage.
	c.rec.add("ctp.offset_before_commit", "%s", committedOffset(k, group, topic, 0))

	end, err := endTxn(k, id, pid, epoch, true)
	if err != nil {
		c.rec.bad("ctp.endtxn.error_code", err)
		return
	}
	c.rec.add("ctp.endtxn.error_code", "%s", errName(end))

	// ...and after it, both must answer 41. Polled rather than asked once:
	// Kafka makes a transactional offset visible when the marker reaches
	// __consumer_offsets, which is a moment later, and a single read would be
	// timing and not a difference.
	c.rec.add("ctp.offset_after_commit", "%s", awaitOffset(k, group, topic, 0, "41"))
}

// awaitOffset reads the committed offset until it is `want` or the deadline
// passes, and answers whatever it last saw. Rig work: the VALUE is the
// observation, the polling is not.
func awaitOffset(k *conn, group, topic string, partition int32, want string) string {
	deadline := time.Now().Add(20 * time.Second)
	last := ""
	for {
		last = committedOffset(k, group, topic, partition)
		if last == want || time.Now().After(deadline) {
			return last
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// ---------------------------------------------------------- 4. the refusals

func refusalPhase(c *runctx, k *conn) {
	// EndTxn for a transactional id this broker has never been asked to open,
	// with a producer id nobody granted. Kafka answers from durable
	// coordinator state; the facade holds no stage for it and cannot know the
	// outcome, so it answers fatally rather than pretending to have committed.
	unknown := "dt-txn-never-opened-" + c.runID
	code, err := endTxn(k, unknown, 987_654_321, 0, true)
	if err != nil {
		c.rec.bad("endtxn.unknown.error_code", err)
	} else {
		c.rec.add("endtxn.unknown.error_code", "%s", errName(code))
	}

	// AddPartitionsToTxn naming a topic that does not exist. The response has
	// no top-level error at v0-v3, so the answer is per partition or it is
	// nothing, and this is the version of the question §8.1 asks the oracle.
	id := "dt-txn-unknown-topic-" + c.runID
	pid, epoch, ok := initTxn(c, k, "addpartitions.unknown_topic.init", id)
	if !ok {
		return
	}
	missing := c.topic("txn-no-such-topic")
	req := addPartitionsReq(id, pid, epoch, missing, []int32{0})
	resp, _, err := k.doT(&req, vTxn, 20*time.Second)
	if err != nil {
		c.rec.bad("addpartitions.unknown_topic.error_code", err)
		return
	}
	ap := resp.(*kmsg.AddPartitionsToTxnResponse)
	if len(ap.Topics) != 1 || len(ap.Topics[0].Partitions) != 1 {
		c.rec.add("addpartitions.unknown_topic.error_code",
			"a response with %d topics", len(ap.Topics))
		return
	}
	c.rec.add("addpartitions.unknown_topic.error_code", "%s",
		errName(ap.Topics[0].Partitions[0].ErrorCode))

	// Whatever it answered, it must not have CREATED the topic: a transaction
	// enrolling a partition is not a client asking for a topic, and the
	// auto-create path is Metadata's and Produce's.
	if n, err := metadataPartitions(k, missing); err != nil {
		c.rec.add("addpartitions.unknown_topic.exists_after", "metadata failed: %v", err)
	} else {
		c.rec.add("addpartitions.unknown_topic.exists_after", "%t", n > 0)
	}
}

// -------------------------------------------------- 5. one version too high

// versionPhase asks each of the four APIs one version above the facade's
// advertised ceiling. The facade CLOSES the connection there, which is what it
// does for every version outside its window and what Apache Kafka does for a
// version it does not know either; Kafka 3.9.1 simply knows these four. Each
// probe gets its OWN connection for that reason.
func versionPhase(c *runctx) {
	probes := []struct {
		key string
		req func(id string) kmsg.Request
	}{
		{"addpartitions", func(id string) kmsg.Request {
			r := addPartitionsReq(id, 1, 0, "dt-vprobe", []int32{0})
			return &r
		}},
		{"addoffsets", func(id string) kmsg.Request {
			r := kmsg.NewAddOffsetsToTxnRequest()
			r.TransactionalID = id
			r.ProducerID = 1
			r.Group = "dt-vprobe"
			return &r
		}},
		{"endtxn", func(id string) kmsg.Request {
			r := kmsg.NewEndTxnRequest()
			r.TransactionalID = id
			r.ProducerID = 1
			r.Commit = true
			return &r
		}},
		{"txnoffsetcommit", func(id string) kmsg.Request {
			r := kmsg.NewTxnOffsetCommitRequest()
			r.TransactionalID = id
			r.Group = "dt-vprobe"
			r.ProducerID = 1
			r.Generation = -1
			return &r
		}},
	}
	for _, p := range probes {
		k, err := c.target.dial()
		if err != nil {
			c.rec.bad(p.key+".v4.unsupported", err)
			continue
		}
		// A DISTINCT transactional id per probe, for the reason conn.rs's
		// every_advertised_version_is_dispatched gives: a fixture that reused
		// one id would meet the stage the previous probe left and be answered
		// about that instead of about the version.
		id := fmt.Sprintf("dt-txn-v4-%s-%s", p.key, c.runID)
		_, _, err = k.doT(p.req(id), vTxn+1, 8*time.Second)
		if err != nil {
			c.rec.add(p.key+".v4.unsupported", "%s", readErrKind(err))
		} else {
			c.rec.add(p.key+".v4.unsupported", "answered")
		}
		k.Close()
	}
}

// ------------------------------------------------------------------ plumbing

// initTxn claims a transactional id, retrying the three codes a broker answers
// while its transaction coordinator is still coming up. apache/kafka answers
// COORDINATOR_LOAD_IN_PROGRESS and NOT_COORDINATOR for its first minute
// (HANDOFF_QUEEN_KAFKA.md), and a scenario that recorded one of those would be
// recording the container's boot and not a difference.
func initTxn(c *runctx, k *conn, key, id string) (int64, int16, bool) {
	deadline := time.Now().Add(90 * time.Second)
	for {
		g, err := initProducerID(k, &id, -1, -1)
		if err != nil {
			c.rec.bad(key+".error_code", err)
			return 0, 0, false
		}
		if retriableCoordinator(g.ErrorCode) && time.Now().Before(deadline) {
			time.Sleep(time.Second)
			continue
		}
		c.rec.add(key+".error_code", "%s", errName(g.ErrorCode))
		c.rec.add(key+".granted_an_id", "%t", g.ProducerID >= 0)
		c.rec.add(key+".epoch", "%d", g.ProducerEpoch)
		c.rec.info(key+".producer_id", "%d", g.ProducerID)
		return g.ProducerID, g.ProducerEpoch, g.ErrorCode == 0 && g.ProducerID >= 0
	}
}

func retriableCoordinator(code int16) bool {
	return code == 14 || code == 15 || code == 16 // LOAD_IN_PROGRESS, NOT_AVAILABLE, NOT_COORDINATOR
}

func findTxnCoordinator(k *conn, id string) (*kmsg.FindCoordinatorResponse, error) {
	deadline := time.Now().Add(90 * time.Second)
	for {
		req := kmsg.NewFindCoordinatorRequest()
		req.CoordinatorKey = id
		req.CoordinatorType = 1 // TRANSACTION
		resp, _, err := k.doT(&req, 2, 20*time.Second)
		if err != nil {
			return nil, err
		}
		fc := resp.(*kmsg.FindCoordinatorResponse)
		if retriableCoordinator(fc.ErrorCode) && time.Now().Before(deadline) {
			time.Sleep(time.Second)
			continue
		}
		return fc, nil
	}
}

func addPartitionsReq(id string, pid int64, epoch int16, topic string, parts []int32) kmsg.AddPartitionsToTxnRequest {
	req := kmsg.NewAddPartitionsToTxnRequest()
	req.TransactionalID = id
	req.ProducerID = pid
	req.ProducerEpoch = epoch
	t := kmsg.NewAddPartitionsToTxnRequestTopic()
	t.Topic = topic
	t.Partitions = parts
	req.Topics = []kmsg.AddPartitionsToTxnRequestTopic{t}
	return req
}

// addPartition enrols ONE partition and answers its per-partition code. At
// v0-v3 the response has no top-level error at all, so the per-partition code
// is the only answer there is.
func addPartition(k *conn, id string, pid int64, epoch int16, topic string, partition int32) (int16, error) {
	req := addPartitionsReq(id, pid, epoch, topic, []int32{partition})
	resp, _, err := k.doT(&req, vTxn, 20*time.Second)
	if err != nil {
		return 0, err
	}
	ap := resp.(*kmsg.AddPartitionsToTxnResponse)
	if len(ap.Topics) != 1 || len(ap.Topics[0].Partitions) != 1 {
		return 0, fmt.Errorf("an AddPartitionsToTxn response with %d topics", len(ap.Topics))
	}
	return ap.Topics[0].Partitions[0].ErrorCode, nil
}

// orErr turns "the call failed" and "the call was refused" into one error, so
// a phase that cannot start says which of the two happened.
func orErr(err error, code int16) error {
	if err != nil {
		return err
	}
	return fmt.Errorf("answered %s", errName(code))
}

func endTxn(k *conn, id string, pid int64, epoch int16, commit bool) (int16, error) {
	req := kmsg.NewEndTxnRequest()
	req.TransactionalID = id
	req.ProducerID = pid
	req.ProducerEpoch = epoch
	req.Commit = commit
	resp, _, err := k.doT(&req, vTxn, 30*time.Second)
	if err != nil {
		return 0, err
	}
	return resp.(*kmsg.EndTxnResponse).ErrorCode, nil
}

func values(prefix string, from, n int) []string {
	out := make([]string, 0, n)
	for i := from; i < from+n; i++ {
		out = append(out, fmt.Sprintf("%s-%03d", prefix, i))
	}
	return out
}

// txnProduce is idempotentProduceOnce with the transactional bit set and a
// transactional id on the wire. Attribute bit 4 (0x10) is what makes a batch
// part of a transaction; without it Kafka answers INVALID_TXN_STATE for a
// producer that has an open transaction, so the bit is the whole request.
func txnProduce(k *conn, id, topic string, partition int32, pid int64, epoch int16, baseSeq int32, vals []string) produceAnswer {
	req := kmsg.NewProduceRequest()
	req.Acks = -1
	req.TimeoutMillis = 30_000
	req.TransactionID = kmsg.StringPtr(id)
	rt := kmsg.NewProduceRequestTopic()
	rt.Topic = topic
	rp := kmsg.NewProduceRequestTopicPartition()
	rp.Partition = partition
	rp.Records = txnBatch(pid, epoch, baseSeq, vals)
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)

	resp, _, err := k.doT(&req, vProduceTxn, 30*time.Second)
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

// txnBatch is idempotentBatch with the transactional attribute bit set. Kept
// separate rather than adding a parameter there, because idempotent.go's
// batches are the F3 measurement and their bytes are the thing being compared.
func txnBatch(producerID int64, epoch int16, baseSeq int32, vals []string) []byte {
	raw := idempotentBatch(producerID, epoch, baseSeq, vals)
	// The attributes int16 sits at a fixed offset in a v2 batch: 8 (base
	// offset) + 4 (batch length) + 4 (partition leader epoch) + 1 (magic) + 4
	// (crc) = 21, so bit 4 — isTransactional — is the low bit 0x10 of byte 22.
	// The crc covers everything FROM the attributes on, so flipping the bit
	// after idempotentBatch stamped it invalidates it, and a wrong crc is
	// answered CORRUPT_MESSAGE and would read as a transaction divergence.
	raw[22] |= 0x10
	binary.BigEndian.PutUint32(raw[17:21],
		crc32.Checksum(raw[21:], crc32.MakeTable(crc32.Castagnoli)))
	return raw
}

// ---------------------------------------------------------------- the reads

type isoFetch struct {
	hw      int64
	lso     int64
	aborted int
	records []record
}

// fetchIso is fetchFrom with the isolation level as a parameter and the
// aborted-transaction list counted, which is the pair of facts a
// read_committed client acts on and which scenarios.go's helper does not carry.
func fetchIso(k *conn, topic string, partition int32, offset int64, iso int8) (*isoFetch, error) {
	req := kmsg.NewFetchRequest()
	req.ReplicaID = -1
	req.MaxWaitMillis = 1000
	req.MinBytes = 1
	req.MaxBytes = 10 << 20
	req.IsolationLevel = iso
	ft := kmsg.NewFetchRequestTopic()
	ft.Topic = topic
	fp := kmsg.NewFetchRequestTopicPartition()
	fp.Partition = partition
	fp.FetchOffset = offset
	fp.LogStartOffset = -1
	fp.PartitionMaxBytes = 10 << 20
	ft.Partitions = []kmsg.FetchRequestTopicPartition{fp}
	req.Topics = []kmsg.FetchRequestTopic{ft}
	resp, _, err := k.doT(&req, 6, 25*time.Second)
	if err != nil {
		return nil, err
	}
	fr := resp.(*kmsg.FetchResponse)
	if len(fr.Topics) != 1 || len(fr.Topics[0].Partitions) != 1 {
		return nil, fmt.Errorf("a fetch response with %d topics", len(fr.Topics))
	}
	p := fr.Topics[0].Partitions[0]
	if p.ErrorCode != 0 {
		return nil, fmt.Errorf("fetch answered %s", errName(p.ErrorCode))
	}
	out := &isoFetch{hw: p.HighWatermark, lso: p.LastStableOffset, aborted: len(p.AbortedTransactions)}
	batches, _, err := parseBatches(p.RecordBatches)
	if err != nil {
		return out, err
	}
	for _, b := range batches {
		out.records = append(out.records, b.records...)
	}
	return out, nil
}

// settle waits until the partition holds no open transaction, and answers the
// read_committed fetch that proved it.
//
// RIG WORK, and it is load-bearing. `EndTxn` returns as soon as the
// coordinator has persisted its own decision; Apache Kafka writes the control
// marker into the DATA partition afterwards, from
// TransactionMarkerChannelManager. A ListOffsets taken in that window answers
// N and one taken after it answers N+1, so without this the oracle's
// log_end_offset is a coin toss and the divergence that is being reported is
// the runner's timing. The condition is Kafka's own definition of "no
// transaction is open here": the last stable offset has caught up with the
// high watermark. On the facade it is true on the first ask, because a
// transaction that has not committed has written nothing to catch up with.
func settle(k *conn, topic string, partition int32) (*isoFetch, error) {
	deadline := time.Now().Add(30 * time.Second)
	for {
		f, err := fetchIso(k, topic, partition, 0, 1)
		if err != nil {
			return nil, err
		}
		if f.lso == f.hw || time.Now().After(deadline) {
			return f, nil
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// countPrefixed counts the records this scenario wrote, by value. Kafka's
// answer to a fetch also carries the transaction's control batch, whose
// "record" is a marker no application ever sees; counting by prefix asks about
// the records instead of about the log's framing.
func countPrefixed(f *isoFetch, prefix string) int {
	n := 0
	for _, r := range f.records {
		if !r.valNull && strings.HasPrefix(string(r.val), prefix) {
			n++
		}
	}
	return n
}

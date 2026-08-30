// M7 F3 — the idempotent producer, against a running facade.
//
// Everything in this file is about the ONE promise a client makes when
// `enable.idempotence` is on: a batch it sends twice appears in the log once,
// and a batch that would leave a hole is refused rather than written. The unit
// tests in `queen-kafka/src/idempotent.rs` prove the state machine; these prove
// the wire, which is the only place the promise is actually made.
//
// Two of them build the record batch BY HAND — producer id, epoch and base
// sequence fixed, CRC computed — because no client library will send the same
// sequence twice on purpose. A client that could be talked into it would be
// proving something about the client.
package compat

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// ---------------------------------------------------------------- the grant

// The API a stock producer opens with. Before M7 F3 the facade advertised no
// support for key 22 at all, and the Java client's answer to that was a fatal
// error on its first send while librdkafka's was a refusal before any wire
// traffic ("Idempotent producer not supported by any of the 1 connected
// broker(s)").
func TestInitProducerIdGrantsAnIdAtEveryAdvertisedVersion(t *testing.T) {
	cl := newClient(t)
	seen := map[int64]bool{}
	for version := int16(0); version <= 4; version++ {
		req := kmsg.NewPtrInitProducerIDRequest()
		req.TransactionTimeoutMillis = 60_000
		resp := initProducerID(t, cl, req)
		if resp.ErrorCode != 0 {
			t.Fatalf("v%d: InitProducerId answered error %d", version, resp.ErrorCode)
		}
		if resp.ProducerID <= 0 {
			t.Fatalf("v%d: producer id %d is not usable", version, resp.ProducerID)
		}
		if resp.ProducerEpoch != 0 {
			t.Fatalf("v%d: a fresh grant came back at epoch %d, want 0", version, resp.ProducerEpoch)
		}
		if seen[resp.ProducerID] {
			t.Fatalf("producer id %d was handed out twice", resp.ProducerID)
		}
		seen[resp.ProducerID] = true
		t.Logf("granted producer id %d epoch %d (request negotiated to v%d)",
			resp.ProducerID, resp.ProducerEpoch, req.Version)
	}
}

// KIP-360's epoch bump: the same id, one epoch higher. That is what a client
// does after OUT_OF_ORDER_SEQUENCE_NUMBER, and it is the whole reason v3 is
// inside the advertised window.
func TestInitProducerIdBumpsAKnownProducersEpoch(t *testing.T) {
	cl := newClient(t)
	first := initProducerID(t, cl, kmsg.NewPtrInitProducerIDRequest())
	if first.ErrorCode != 0 {
		t.Fatalf("the first grant answered error %d", first.ErrorCode)
	}

	bump := kmsg.NewPtrInitProducerIDRequest()
	bump.ProducerID = first.ProducerID
	bump.ProducerEpoch = first.ProducerEpoch
	second := initProducerID(t, cl, bump)
	if second.ErrorCode != 0 {
		t.Fatalf("the bump answered error %d", second.ErrorCode)
	}
	if second.ProducerID != first.ProducerID {
		t.Fatalf("the bump moved the producer id: %d -> %d", first.ProducerID, second.ProducerID)
	}
	if second.ProducerEpoch != first.ProducerEpoch+1 {
		t.Fatalf("the bump answered epoch %d, want %d", second.ProducerEpoch, first.ProducerEpoch+1)
	}
	t.Logf("bumped producer %d from epoch %d to %d",
		first.ProducerID, first.ProducerEpoch, second.ProducerEpoch)
}

// A transactional id is GRANTED since M9, and the point of this test is still
// the CLOCK. Before key 22 was advertised, a transactional producer's
// initTransactions() blocked for the whole of max.block.ms (the campaign
// measured 20 s) because the Sender held a request no node claimed to support;
// then it blocked for the same 20 s on a RETRIABLE FindCoordinator refusal.
// M9 makes both immediate: FindCoordinator(TRANSACTION) answers this facade
// itself and InitProducerId mints a pid at epoch 0.
//
// Sent straight at node 0 rather than through the client's own routing, which
// keeps this a test of the HANDLER; the routing is
// TestATransactionCoordinatorIsThisFacade below, and the whole client-visible
// path is compat/transactions.
func TestATransactionalIdIsGrantedImmediately(t *testing.T) {
	cl := newClient(t)
	id := "qk-m9-tx-" + fmt.Sprint(time.Now().UnixNano())
	req := kmsg.NewPtrInitProducerIDRequest()
	req.TransactionalID = &id
	req.TransactionTimeoutMillis = 60_000

	start := time.Now()
	resp := initProducerIDAtNode0(t, cl, req)
	took := time.Since(start)

	if resp.ErrorCode != 0 {
		t.Fatalf("a transactional id answered error %d, want a grant", resp.ErrorCode)
	}
	if resp.ProducerID <= 0 {
		t.Fatalf("producer id %d is not usable", resp.ProducerID)
	}
	// A FRESH transactional id is claimed at epoch 0; a second init of the same
	// id is what bumps it, and that fencing is compat/transactions scenario 3.
	if resp.ProducerEpoch != 0 {
		t.Fatalf("a fresh transactional id came back at epoch %d, want 0", resp.ProducerEpoch)
	}
	if took > 2*time.Second {
		t.Fatalf("the grant took %s; it is supposed to be immediate", took)
	}
	t.Logf("a transactional id was granted pid %d epoch %d in %s", resp.ProducerID, resp.ProducerEpoch, took)
}

// An EMPTY transactional id is not a transactional id: brod's hand-rolled
// encoder writes a null one as "" and both sites read it the same way.
func TestAnEmptyTransactionalIdIsAPlainGrant(t *testing.T) {
	cl := newClient(t)
	empty := ""
	req := kmsg.NewPtrInitProducerIDRequest()
	req.TransactionalID = &empty
	// At node 0 for the same reason as above: franz-go sees a non-nil
	// TransactionalID pointer and routes by it, whatever the string is.
	resp := initProducerIDAtNode0(t, cl, req)
	if resp.ErrorCode != 0 {
		t.Fatalf(`a "" transactional id answered error %d, want a plain grant`, resp.ErrorCode)
	}
	if resp.ProducerID <= 0 {
		t.Fatalf("producer id %d is not usable", resp.ProducerID)
	}
}

// -------------------------------------------------------- a real client, whole

// THE onboarding test: franz-go with its own default (idempotent) producer, and
// no DisableIdempotentWrite anywhere. Every other test in this package passes
// that option because the facade could not do this until now.
func TestFranzGoDefaultIdempotentProducerRoundTrips(t *testing.T) {
	topic := newTopic(t)
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(bootstrap()),
		kgo.AllowAutoTopicCreation(),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		// No DisableIdempotentWrite: this is the point of the test.
	)
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	defer cl.Close()

	const n = 25
	ctx := ctxFor(t, 60*time.Second)
	for i := 0; i < n; i++ {
		r := &kgo.Record{
			Topic:     topic,
			Partition: 0,
			Key:       []byte(fmt.Sprintf("k%02d", i)),
			Value:     []byte(fmt.Sprintf("v%02d", i)),
		}
		if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
			t.Fatalf("idempotent produce %d: %v", i, err)
		}
	}
	recs := consumeFrom(t, topic, kgo.NewOffset().AtStart(), n, 60*time.Second)
	if len(recs) != n {
		t.Fatalf("read %d records back, want %d", len(recs), n)
	}
	for i, r := range recs {
		if want := fmt.Sprintf("v%02d", i); string(r.Value) != want {
			t.Fatalf("record %d is %q, want %q", i, r.Value, want)
		}
		if r.Offset != int64(i) {
			t.Fatalf("record %d landed at offset %d", i, r.Offset)
		}
	}
	t.Logf("%d records produced by a DEFAULT franz-go producer and read back in order", n)
}

// ------------------------------------------------ the wire-level duplicate proof

// The only check that proves the WINDOW rather than the advertisement: one
// Produce frame with a fixed (producer id, epoch, base sequence), sent twice.
// The second must answer error 0 with the SAME base offset, and the partition
// must hold the records once.
func TestADuplicateBatchIsAnsweredWithTheOriginalOffsets(t *testing.T) {
	cl := newClient(t)
	topic := newTopic(t)
	ensureTopic(t, cl, topic)

	pid := grantedProducerID(t, cl)
	frame := idempotentProduce(topic, 0, pid, 0, 0, []string{"dup-a", "dup-b", "dup-c"})

	first := produceOnce(t, cl, frame)
	if first.ErrorCode != 0 {
		t.Fatalf("the first send answered error %d: %s", first.ErrorCode, errMsg(first))
	}
	t.Logf("first send: error 0, base offset %d (Produce negotiated to v%d)",
		first.BaseOffset, frame.Version)

	// The identical bytes again. A client that lost the response would send
	// exactly this.
	second := produceOnce(t, cl, frame)
	if second.ErrorCode != 0 {
		t.Fatalf("the resend answered error %d (%s); a duplicate must be a SUCCESS",
			second.ErrorCode, errMsg(second))
	}
	if second.BaseOffset != first.BaseOffset {
		t.Fatalf("the resend answered base offset %d, want %d — the offsets the original got",
			second.BaseOffset, first.BaseOffset)
	}
	t.Logf("resend: error 0, base offset %d — the same offsets, and nothing written",
		second.BaseOffset)

	// ...and the log holds them ONCE. A fourth record proves the read reached
	// the end rather than merely finding three.
	sentinel := idempotentProduce(topic, 0, pid, 0, 3, []string{"sentinel"})
	if r := produceOnce(t, cl, sentinel); r.ErrorCode != 0 {
		t.Fatalf("the sentinel send answered error %d: %s", r.ErrorCode, errMsg(r))
	}
	recs := consumeFrom(t, topic, kgo.NewOffset().AtStart(), 4, 60*time.Second)
	got := make([]string, 0, len(recs))
	for _, r := range recs {
		got = append(got, string(r.Value))
	}
	want := []string{"dup-a", "dup-b", "dup-c", "sentinel"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("the partition holds %v, want %v — the duplicate was written", got, want)
	}
	t.Logf("the partition holds %v: the duplicated batch appears once", got)
}

// The other half of idempotence, and just as load-bearing: a batch that would
// leave a hole is refused OUT_OF_ORDER_SEQUENCE_NUMBER and nothing is written.
// Without it, "idempotent" would be a claim about duplicates that said nothing
// about order.
func TestAGapInTheProducerSequenceIsRefusedAndWritesNothing(t *testing.T) {
	cl := newClient(t)
	topic := newTopic(t)
	ensureTopic(t, cl, topic)

	pid := grantedProducerID(t, cl)
	if r := produceOnce(t, cl, idempotentProduce(topic, 0, pid, 0, 0, []string{"seq0"})); r.ErrorCode != 0 {
		t.Fatalf("the first send answered error %d: %s", r.ErrorCode, errMsg(r))
	}

	// Sequence 5 where 1 was expected.
	gap := produceOnce(t, cl, idempotentProduce(topic, 0, pid, 0, 5, []string{"gap"}))
	// 45 = OUT_OF_ORDER_SEQUENCE_NUMBER.
	if gap.ErrorCode != 45 {
		t.Fatalf("a gapped batch answered error %d, want 45: %s", gap.ErrorCode, errMsg(gap))
	}
	t.Logf("sequence 5 after sequence 0 answered error 45: %s", errMsg(gap))

	// ...and the batch that WAS next still is, which is what makes the Java
	// client's re-drain work.
	next := produceOnce(t, cl, idempotentProduce(topic, 0, pid, 0, 1, []string{"seq1"}))
	if next.ErrorCode != 0 {
		t.Fatalf("the batch that was next answered error %d: %s", next.ErrorCode, errMsg(next))
	}
	recs := consumeFrom(t, topic, kgo.NewOffset().AtStart(), 2, 60*time.Second)
	got := []string{string(recs[0].Value), string(recs[1].Value)}
	if fmt.Sprint(got) != fmt.Sprint([]string{"seq0", "seq1"}) {
		t.Fatalf("the partition holds %v, want [seq0 seq1] — the gapped batch was written", got)
	}
}

// A producer this facade holds no window for — a restart, an eviction, a
// connection that landed on another facade. OUT_OF_ORDER and never
// UNKNOWN_PRODUCER_ID (59), because OUT_OF_ORDER is the code whose recovery
// (KIP-360's epoch bump) every idempotent client implements.
func TestAProducerThisFacadeNeverSawIsOutOfOrder(t *testing.T) {
	cl := newClient(t)
	topic := newTopic(t)
	ensureTopic(t, cl, topic)

	pid := grantedProducerID(t, cl)
	r := produceOnce(t, cl, idempotentProduce(topic, 0, pid, 0, 42, []string{"stranded"}))
	if r.ErrorCode != 45 {
		t.Fatalf("a producer with no window answered error %d, want 45: %s", r.ErrorCode, errMsg(r))
	}
	t.Logf("a lost window answered error 45 (not 59): %s", errMsg(r))

	// And the recovery a client would make: bump the epoch, reset to sequence 0.
	bump := kmsg.NewPtrInitProducerIDRequest()
	bump.ProducerID = pid
	bump.ProducerEpoch = 0
	after := initProducerID(t, cl, bump)
	if after.ErrorCode != 0 {
		t.Fatalf("the epoch bump answered error %d", after.ErrorCode)
	}
	ok := produceOnce(t, cl,
		idempotentProduce(topic, 0, after.ProducerID, after.ProducerEpoch, 0, []string{"recovered"}))
	if ok.ErrorCode != 0 {
		t.Fatalf("the producer did not recover after the bump: error %d (%s)",
			ok.ErrorCode, errMsg(ok))
	}
	t.Logf("after the bump to epoch %d the producer wrote at offset %d",
		after.ProducerEpoch, ok.BaseOffset)
}

// An OLD epoch after a bump is fenced. Not the transactional fencing this
// facade refuses — a producer retrying a batch it had queued at the epoch it
// has since left.
func TestAStaleEpochIsFenced(t *testing.T) {
	cl := newClient(t)
	topic := newTopic(t)
	ensureTopic(t, cl, topic)

	pid := grantedProducerID(t, cl)
	if r := produceOnce(t, cl, idempotentProduce(topic, 0, pid, 1, 0, []string{"e1"})); r.ErrorCode != 0 {
		t.Fatalf("epoch 1 sequence 0 answered error %d: %s", r.ErrorCode, errMsg(r))
	}
	stale := produceOnce(t, cl, idempotentProduce(topic, 0, pid, 0, 1, []string{"e0"}))
	// 47 = INVALID_PRODUCER_EPOCH.
	if stale.ErrorCode != 47 {
		t.Fatalf("an old epoch answered error %d, want 47: %s", stale.ErrorCode, errMsg(stale))
	}
	t.Logf("epoch 0 after epoch 1 answered error 47: %s", errMsg(stale))
}

// What a transactional client ACTUALLY meets, measured rather than assumed, and
// the exact answer that ended the campaign's 20 second hang. FindCoordinator for
// a TRANSACTION coordinator (key_type 1) used to be answered
// COORDINATOR_NOT_AVAILABLE, which is RETRIABLE, so the client looped there
// until max.block.ms and never reached InitProducerId at all. Since M9 it is
// answered with THIS facade, error 0, in single-node mode.
//
// In cluster mode the same request is answered
// TRANSACTIONAL_ID_AUTHORIZATION_FAILED (53), which is FATAL, so the client
// stops instead of looping. This rig is single-node; compat/transactions
// scenario 7 runs a facade with QUEEN_KAFKA_NODE_ID set and measures that.
func TestATransactionCoordinatorIsThisFacade(t *testing.T) {
	cl := newClient(t)
	id := "qk-m9-txroute-" + fmt.Sprint(time.Now().UnixNano())
	req := kmsg.NewPtrFindCoordinatorRequest()
	req.CoordinatorKey = id
	req.CoordinatorType = 1 // TRANSACTION

	start := time.Now()
	resp, err := cl.Request(ctxFor(t, 20*time.Second), req)
	took := time.Since(start)
	if err != nil {
		t.Fatalf("FindCoordinator(TRANSACTION): %v", err)
	}
	fc, ok := resp.(*kmsg.FindCoordinatorResponse)
	if !ok {
		t.Fatalf("FindCoordinator: unexpected response type %T", resp)
	}
	if fc.ErrorCode != 0 {
		t.Fatalf("FindCoordinator(TRANSACTION) answered error %d, want 0", fc.ErrorCode)
	}
	if fc.NodeID != 0 || fc.Port <= 0 {
		t.Fatalf("FindCoordinator(TRANSACTION) answered node %d port %d, want node 0 and a real port",
			fc.NodeID, fc.Port)
	}
	if took > 2*time.Second {
		t.Fatalf("the answer took %s; the whole point of M9's fix is that it is immediate", took)
	}
	t.Logf("the transaction coordinator is node %d at %s:%d, answered in %s",
		fc.NodeID, fc.Host, fc.Port, took)
}

// ------------------------------------------------------ the lost-window proof

// The acceptance check for the caveat at the top of
// `queen-kafka/src/idempotent.rs`, and the one that decides whether advertising
// InitProducerId v3 was worth it: a facade restart LOSES the sequence window,
// which a real Kafka broker does not, and the producer has to keep running
// anyway.
//
// SIGKILL mid-stream — the rig's own restart hook, the same crash the group
// tests use — then keep producing on the SAME client. What must not happen is
// the producer dying with OutOfOrderSequenceException; what may happen, and is
// the honest cost, is that the in-flight window is written twice.
func TestAnIdempotentProducerSurvivesAFacadeRestart(t *testing.T) {
	restart := os.Getenv("QUEEN_KAFKA_RESTART_CMD")
	if restart == "" {
		t.Skip("no QUEEN_KAFKA_RESTART_CMD: run queen-kafka/compat/rig.sh, which sets it")
	}
	topic := newTopic(t)
	ensureTopic(t, newClient(t), topic)

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(bootstrap()),
		kgo.AllowAutoTopicCreation(),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.RecordRetries(20),
		// No DisableIdempotentWrite: the producer under test IS the idempotent
		// one, and its own recovery is what is being measured.
	)
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	defer cl.Close()

	const before, after = 20, 20
	ctx := ctxFor(t, 180*time.Second)
	for i := 0; i < before; i++ {
		r := &kgo.Record{Topic: topic, Partition: 0, Value: []byte(fmt.Sprintf("pre-%02d", i))}
		if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
			t.Fatalf("produce %d before the restart: %v", i, err)
		}
	}

	out, err := exec.CommandContext(ctxFor(t, 90*time.Second), restart).CombinedOutput()
	if err != nil {
		t.Fatalf("restarting the facade (%s): %v\n%s", restart, err, out)
	}
	line := strings.TrimSpace(string(out))
	old, fresh := pidField(line, "old="), pidField(line, "new=")
	if old == "" || fresh == "" || old == fresh || old == "none" {
		t.Fatalf("the facade was not actually restarted: %q", line)
	}
	t.Logf("facade restarted mid-stream: pid %s -> %s (the sequence window is gone)", old, fresh)

	// THE assertion: the producer keeps going. Every send after the restart
	// must succeed from the caller's point of view — the OUT_OF_ORDER the
	// facade answers is the client's own KIP-360 bump, not an error the
	// application sees.
	for i := 0; i < after; i++ {
		r := &kgo.Record{Topic: topic, Partition: 0, Value: []byte(fmt.Sprintf("post-%02d", i))}
		if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
			t.Fatalf("produce %d AFTER the restart: %v\n"+
				"a lost sequence window killed the producer; the answer for an absent entry is wrong",
				i, err)
		}
	}

	// Every record is accounted for AT LEAST once — which is exactly the
	// documented degradation, and is why this counts rather than asserting
	// equality.
	recs := consumeFrom(t, topic, kgo.NewOffset().AtStart(), before+after, 120*time.Second)
	seen := map[string]int{}
	for _, r := range recs {
		seen[string(r.Value)]++
	}
	missing, dup := 0, 0
	for i := 0; i < before; i++ {
		if seen[fmt.Sprintf("pre-%02d", i)] == 0 {
			missing++
		} else if seen[fmt.Sprintf("pre-%02d", i)] > 1 {
			dup++
		}
	}
	for i := 0; i < after; i++ {
		if seen[fmt.Sprintf("post-%02d", i)] == 0 {
			missing++
		} else if seen[fmt.Sprintf("post-%02d", i)] > 1 {
			dup++
		}
	}
	if missing != 0 {
		t.Fatalf("%d record(s) went missing across the restart; at-least-once is the floor", missing)
	}
	t.Logf("all %d records survived the restart, %d of them written twice "+
		"(at-least-once for at most the in-flight window: the documented cost)",
		before+after, dup)
}

// ------------------------------------------------------------------- fixtures

// initProducerIDAtNode0 bypasses the client's own coordinator routing and puts
// the request on the single advertised broker. Only the transactional cases
// need it; a non-transactional InitProducerId is routed to any broker anyway.
func initProducerIDAtNode0(t *testing.T, cl *kgo.Client, req *kmsg.InitProducerIDRequest) *kmsg.InitProducerIDResponse {
	t.Helper()
	resp, err := cl.Broker(0).Request(ctxFor(t, 20*time.Second), req)
	if err != nil {
		t.Fatalf("InitProducerId at node 0: %v", err)
	}
	out, ok := resp.(*kmsg.InitProducerIDResponse)
	if !ok {
		t.Fatalf("InitProducerId: unexpected response type %T", resp)
	}
	return out
}

func initProducerID(t *testing.T, cl *kgo.Client, req *kmsg.InitProducerIDRequest) *kmsg.InitProducerIDResponse {
	t.Helper()
	resp, err := cl.Request(ctxFor(t, 20*time.Second), req)
	if err != nil {
		t.Fatalf("InitProducerId: %v", err)
	}
	out, ok := resp.(*kmsg.InitProducerIDResponse)
	if !ok {
		t.Fatalf("InitProducerId: unexpected response type %T", resp)
	}
	return out
}

func grantedProducerID(t *testing.T, cl *kgo.Client) int64 {
	t.Helper()
	resp := initProducerID(t, cl, kmsg.NewPtrInitProducerIDRequest())
	if resp.ErrorCode != 0 || resp.ProducerID <= 0 {
		t.Fatalf("InitProducerId answered error %d, id %d", resp.ErrorCode, resp.ProducerID)
	}
	return resp.ProducerID
}

// produceOnce sends a prepared frame and returns the ONE partition answer in
// it. The request object is reused between calls on purpose — resending the
// identical bytes is the whole point of the duplicate test.
func produceOnce(t *testing.T, cl *kgo.Client, req *kmsg.ProduceRequest) kmsg.ProduceResponseTopicPartition {
	t.Helper()
	resp, err := cl.Request(ctxFor(t, 30*time.Second), req)
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}
	out, ok := resp.(*kmsg.ProduceResponse)
	if !ok {
		t.Fatalf("Produce: unexpected response type %T", resp)
	}
	if len(out.Topics) != 1 || len(out.Topics[0].Partitions) != 1 {
		t.Fatalf("Produce answered %d topics; this fixture sends exactly one partition", len(out.Topics))
	}
	return out.Topics[0].Partitions[0]
}

func errMsg(p kmsg.ProduceResponseTopicPartition) string {
	if p.ErrorMessage == nil {
		return "(no error message)"
	}
	return *p.ErrorMessage
}

// idempotentProduce builds a Produce request whose record batch carries an
// EXACT producer id, epoch and base sequence — the three header fields the
// facade's sequence window is checked on. Built here rather than through a
// client because no client will send the same sequence twice on purpose.
func idempotentProduce(topic string, partition int32, producerID int64, epoch int16, baseSeq int32, values []string) *kmsg.ProduceRequest {
	req := kmsg.NewPtrProduceRequest()
	req.Acks = -1
	req.TimeoutMillis = 30_000

	rt := kmsg.NewProduceRequestTopic()
	rt.Topic = topic
	rp := kmsg.NewProduceRequestTopicPartition()
	rp.Partition = partition
	rp.Records = recordBatch(producerID, epoch, baseSeq, values)
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)
	return req
}

// recordBatch encodes a RecordBatch v2 by hand, with the two fields the
// encoders in kmsg leave to the caller filled in: the length of everything
// after it, and the Castagnoli CRC of everything after THAT. A batch whose CRC
// is wrong is answered CORRUPT_MESSAGE and would look like an idempotence
// failure, so both are computed rather than guessed.
func recordBatch(producerID int64, epoch int16, baseSeq int32, values []string) []byte {
	now := time.Now().UnixMilli()
	var records []byte
	for i, v := range values {
		r := kmsg.Record{
			OffsetDelta: int32(i),
			Value:       []byte(v),
		}
		// Length is "everything that follows this field", and it is a zigzag
		// varint. Measured by encoding once with a zero (one byte) and taking
		// the rest.
		probe := r.AppendTo(nil)
		r.Length = int32(len(probe) - 1)
		records = append(records, r.AppendTo(nil)...)
	}

	b := kmsg.RecordBatch{
		FirstOffset:          0,
		PartitionLeaderEpoch: -1,
		Magic:                2,
		Attributes:           0, // no compression, CreateTime, not transactional
		LastOffsetDelta:      int32(len(values) - 1),
		FirstTimestamp:       now,
		MaxTimestamp:         now,
		ProducerID:           producerID,
		ProducerEpoch:        epoch,
		FirstSequence:        baseSeq,
		NumRecords:           int32(len(values)),
		Records:              records,
	}
	raw := b.AppendTo(nil)

	// FirstOffset(8) + Length(4) = 12: Length covers everything after itself.
	binary.BigEndian.PutUint32(raw[8:12], uint32(len(raw)-12))
	// ...+ PartitionLeaderEpoch(4) + Magic(1) + CRC(4) = 21: the CRC covers
	// everything after itself, from Attributes on.
	crc := crc32.Checksum(raw[21:], crc32.MakeTable(crc32.Castagnoli))
	binary.BigEndian.PutUint32(raw[17:21], crc)
	return raw
}

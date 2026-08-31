package compat

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// M4, against a real client: the consumer GROUP. Everything here goes through
// franz-go's own group machinery — FindCoordinator, the JoinGroup that parks
// for the join window, the leader computing an assignment the facade never
// looks inside, SyncGroup, the heartbeat loop, OffsetCommit and OffsetFetch —
// so what is being asserted is not "the handler answered" but "a Kafka consumer
// group works".
//
// These tests are slower than the rest of the suite by design: a group forms
// after the facade's join window (QUEEN_KAFKA_GROUP_JOIN_DELAY_MS, 3 seconds by
// default, the same as Kafka's group.initial.rebalance.delay.ms), and the rig
// runs the default rather than a shortened one — the delay is part of what is
// being tested.

// groupName is unique per test run, like newTopic: a group id is durable (its
// offsets live in Queen) so a re-run must not inherit the previous one's
// progress.
func groupName(t *testing.T) string {
	t.Helper()
	return "kcompat-group-" + newTopic(t)
}

// seedAcrossPartitions produces `perPartition` records to every partition of a
// fresh topic and returns the topic plus the total.
func seedAcrossPartitions(t *testing.T, perPartition int) (string, int) {
	t.Helper()
	cl := newClient(t, kgo.RequiredAcks(kgo.AllISRAcks()))
	topic := newTopic(t)
	ensureTopic(t, cl, topic)

	width := topicWidth(t)
	var recs []*kgo.Record
	for p := int32(0); p < width; p++ {
		for i := 0; i < perPartition; i++ {
			recs = append(recs, &kgo.Record{
				Topic:     topic,
				Partition: p,
				Key:       []byte(fmt.Sprintf("p%d-%d", p, i)),
				Value:     []byte(fmt.Sprintf("value-%d-%d", p, i)),
			})
		}
	}
	produceSync(t, cl, recs)
	return topic, len(recs)
}

// CHECK 11. Two members of one group split the topic between them: every record
// is delivered exactly once, and no partition is read by both.
//
// That last clause is the whole point of a group. It holds only if the
// coordinator gave the two members ONE generation and distributed the leader's
// assignment verbatim — a facade that answered each join with its own
// generation would produce two consumers reading everything twice, and every
// assertion here would fail.
func TestConsumerGroupTwoMembersSplitThePartitions(t *testing.T) {
	topic, total := seedAcrossPartitions(t, 3)
	group := groupName(t)

	// Cancelled by the budget OR by the last record arriving: a member that has
	// read everything its partitions hold would otherwise sit in PollRecords
	// until the budget, which is a slow pass rather than a fast one.
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	var mu sync.Mutex
	owners := make(map[int32]map[int]bool) // partition -> members that saw it
	seen := make(map[string]int)           // record key -> times delivered
	delivered := 0

	var wg sync.WaitGroup
	for member := 0; member < 2; member++ {
		member := member
		cl := newClient(t,
			kgo.ConsumerGroup(group),
			kgo.ConsumeTopics(topic),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				fs := cl.PollRecords(ctx, 50)
				if ctx.Err() != nil {
					return
				}
				if errs := fs.Errors(); len(errs) > 0 {
					t.Errorf("member %d: fetch error: %v", member, errs)
					return
				}
				mu.Lock()
				fs.EachRecord(func(r *kgo.Record) {
					if owners[r.Partition] == nil {
						owners[r.Partition] = make(map[int]bool)
					}
					owners[r.Partition][member] = true
					seen[string(r.Key)]++
					delivered++
				})
				done := delivered >= total
				mu.Unlock()
				if done {
					cancel()
					return
				}
			}
		}()
	}
	wg.Wait()

	if delivered < total {
		t.Fatalf("the group delivered %d/%d records before the budget ran out", delivered, total)
	}
	if len(seen) != total {
		t.Errorf("%d distinct records delivered, want %d", len(seen), total)
	}
	for key, times := range seen {
		if times != 1 {
			t.Errorf("record %s was delivered %d times", key, times)
		}
	}
	// The assignment itself: one owner per partition, and both members working.
	working := make(map[int]bool)
	for partition, members := range owners {
		if len(members) != 1 {
			t.Errorf("partition %d was read by %d members, want 1", partition, len(members))
		}
		for m := range members {
			working[m] = true
		}
	}
	if got := int32(len(owners)); got != topicWidth(t) {
		t.Errorf("%d partitions were consumed, want %d", got, topicWidth(t))
	}
	if len(working) != 2 {
		t.Errorf("%d of the 2 members were assigned anything", len(working))
	}
}

// CHECK 12. A group resumes from what it committed, not from where
// auto.offset.reset points.
//
// The second consumer is configured to start AT THE BEGINNING, and it must not:
// a committed offset overrides the reset policy, which is the entire reason
// offsets are durable. This is the assertion that proves the OffsetCommit →
// Queen KV → OffsetFetch round trip end to end, through two separate processes'
// worth of client state.
func TestConsumerGroupResumesFromCommittedOffsets(t *testing.T) {
	topic, total := seedAcrossPartitions(t, 4)
	group := groupName(t)
	half := total / 2

	first := newClient(t,
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	got := drain(t, first, half, 90*time.Second)
	if err := first.CommitRecords(ctxFor(t, 30*time.Second), got...); err != nil {
		t.Fatalf("commit: %v", err)
	}
	// Leave cleanly, so the second member does not wait out a session timeout.
	first.Close()

	// What the group says it committed, read back the way an admin tool reads
	// it — through OffsetFetch, before any consumer acts on it.
	committed := fetchOffsets(t, group, topic)
	if len(committed) == 0 {
		t.Fatalf("OffsetFetch reported nothing committed for %s", group)
	}
	// Each partition's records are contiguous from 0, so a committed offset IS
	// the count of records read from that partition, and the sum is the count
	// of records read. The -1s are the partitions this member had not reached
	// yet — "no offset", not a zero.
	var sum int64
	for partition, off := range committed {
		if off < 0 {
			continue
		}
		if off == 0 {
			t.Errorf("partition %d committed offset 0, which is not a commit", partition)
		}
		sum += off
	}
	if sum != int64(len(got)) {
		t.Errorf("committed offsets sum to %d, want %d (the records consumed): %v",
			sum, len(got), committed)
	}

	// A brand-new consumer, told to start at the beginning: it must resume
	// where the group left off instead.
	second := newClient(t,
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	rest := drain(t, second, total-len(got), 90*time.Second)

	firstKeys := make(map[string]bool, len(got))
	for _, r := range got {
		firstKeys[string(r.Key)] = true
	}
	for _, r := range rest {
		if firstKeys[string(r.Key)] {
			t.Fatalf("record %s was re-delivered: the group did not resume from its commit",
				r.Key)
		}
	}
	if len(rest) != total-len(got) {
		t.Errorf("the second member read %d records, want %d", len(rest), total-len(got))
	}
}

// CHECK 13. A member leaving triggers a rebalance, and the survivor picks up the
// partitions it was not reading — with no gap and no duplicate across the two
// generations.
func TestConsumerGroupRebalancesWhenAMemberLeaves(t *testing.T) {
	topic, total := seedAcrossPartitions(t, 3)
	group := groupName(t)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	var mu sync.Mutex
	seen := make(map[string]int)
	collect := func(fs kgo.Fetches) int {
		mu.Lock()
		defer mu.Unlock()
		n := 0
		fs.EachRecord(func(r *kgo.Record) {
			seen[string(r.Key)]++
			n++
		})
		return n
	}
	count := func() int {
		mu.Lock()
		defer mu.Unlock()
		return len(seen)
	}

	opts := []kgo.Opt{
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	}
	leaving := newClient(t, opts...)
	staying := newClient(t, opts...)

	// Both members read a little, so both are demonstrably assigned something.
	for _, cl := range []*kgo.Client{leaving, staying} {
		fs := cl.PollRecords(ctx, 1)
		if errs := fs.Errors(); len(errs) > 0 {
			t.Fatalf("fetch error before the rebalance: %v", errs)
		}
		if collect(fs) == 0 {
			t.Fatalf("a member was assigned nothing before the rebalance")
		}
	}

	// One leaves — which sends LeaveGroup and starts a rebalance rather than
	// waiting out a session timeout.
	leaving.Close()

	// The survivor must now be able to reach every record, including the ones
	// the departed member owned and had not read.
	for count() < total {
		if ctx.Err() != nil {
			t.Fatalf("after the rebalance the survivor reached %d/%d records", count(), total)
		}
		fs := staying.PollRecords(ctx, 50)
		if ctx.Err() != nil {
			t.Fatalf("after the rebalance the survivor reached %d/%d records", count(), total)
		}
		if errs := fs.Errors(); len(errs) > 0 {
			t.Fatalf("fetch error after the rebalance: %v", errs)
		}
		collect(fs)
	}
	if len(seen) != total {
		t.Errorf("%d distinct records after the rebalance, want %d", len(seen), total)
	}
}

// CHECK 14. FindCoordinator answers with the facade itself — the same broker
// Metadata advertises, so a client reuses the connection it already has.
func TestFindCoordinatorIsTheFacade(t *testing.T) {
	cl := newClient(t)
	topic := newTopic(t)
	ensureTopic(t, cl, topic)
	md := metadataFor(t, cl, topic)

	req := kmsg.NewPtrFindCoordinatorRequest()
	req.CoordinatorKey = groupName(t)
	req.CoordinatorType = 0 // group
	resp, err := req.RequestWith(ctxFor(t, 20*time.Second), cl)
	if err != nil {
		t.Fatalf("FindCoordinator: %v", err)
	}
	if resp.ErrorCode != 0 {
		t.Fatalf("FindCoordinator: error code %d", resp.ErrorCode)
	}
	if len(md.Brokers) != 1 {
		t.Fatalf("metadata advertises %d brokers, want 1", len(md.Brokers))
	}
	b := md.Brokers[0]
	if resp.NodeID != b.NodeID || resp.Host != b.Host || resp.Port != b.Port {
		t.Errorf("coordinator is %d/%s:%d, metadata advertises %d/%s:%d",
			resp.NodeID, resp.Host, resp.Port, b.NodeID, b.Host, b.Port)
	}

	// M9: since transactions landed, this process IS the transaction
	// coordinator in single-node mode, and it names itself for key_type 1 the
	// same way it names itself for a group. Before M9 this arm asserted a
	// refusal, which was the honest answer while there was no coordinator; it
	// is now the wrong one, and asserting it would pin the 20 s
	// `initTransactions()` hang the M9 design measured and removed
	// (find_coordinator.rs, "The TRANSACTION coordinator (key_type 1)").
	//
	// The cluster-mode arm — QUEEN_KAFKA_NODE_ID set, answered
	// TRANSACTIONAL_ID_AUTHORIZATION_FAILED (53), fatal so the client stops
	// rather than looping on discovery — is exercised by the cluster rig and by
	// compat/transactions/run.sh, not here: this rig runs single-node.
	txn := kmsg.NewPtrFindCoordinatorRequest()
	txn.CoordinatorKey = "some-transactional-id"
	txn.CoordinatorType = 1
	txnResp, err := txn.RequestWith(ctxFor(t, 20*time.Second), cl)
	if err != nil {
		t.Fatalf("FindCoordinator(transaction): %v", err)
	}
	if txnResp.ErrorCode != 0 {
		t.Fatalf("FindCoordinator(transaction): error code %d", txnResp.ErrorCode)
	}
	if txnResp.NodeID != b.NodeID || txnResp.Host != b.Host || txnResp.Port != b.Port {
		t.Errorf("transaction coordinator is %d/%s:%d, metadata advertises %d/%s:%d",
			txnResp.NodeID, txnResp.Host, txnResp.Port, b.NodeID, b.Host, b.Port)
	}
}

// CHECK 15. The SIMPLE CONSUMER path: a client that manages no membership at all
// commits with generation -1 and an empty member id, and reads its offsets back
// — including through the "all topics" form of OffsetFetch, which is what
// `kafka-consumer-groups --describe` sends and which the facade answers from a
// prefix read of the key/value store.
func TestSimpleConsumerCommitsAndReadsBackEveryTopic(t *testing.T) {
	cl := newClient(t)
	topic := newTopic(t)
	ensureTopic(t, cl, topic)
	group := groupName(t)

	commit := kmsg.NewPtrOffsetCommitRequest()
	commit.Group = group
	commit.Generation = -1
	commit.MemberID = ""
	ct := kmsg.NewOffsetCommitRequestTopic()
	ct.Topic = topic
	for _, p := range []struct {
		index    int32
		offset   int64
		metadata string
	}{{0, 41, "batch-7"}, {3, 12, ""}} {
		cp := kmsg.NewOffsetCommitRequestTopicPartition()
		cp.Partition = p.index
		cp.Offset = p.offset
		md := p.metadata
		cp.Metadata = &md
		ct.Partitions = append(ct.Partitions, cp)
	}
	commit.Topics = append(commit.Topics, ct)

	commitResp, err := commit.RequestWith(ctxFor(t, 20*time.Second), cl)
	if err != nil {
		t.Fatalf("OffsetCommit: %v", err)
	}
	for _, rt := range commitResp.Topics {
		for _, rp := range rt.Partitions {
			if rp.ErrorCode != 0 {
				t.Fatalf("OffsetCommit %s/%d: error code %d", rt.Topic, rp.Partition, rp.ErrorCode)
			}
		}
	}

	// Named form: what was committed, plus a partition that was not — which
	// must come back as -1 with NO error, because that is what makes a
	// consumer apply auto.offset.reset.
	named := fetchOffsets(t, group, topic)
	if named[0] != 41 || named[3] != 12 {
		t.Errorf("named OffsetFetch answered %v, want partition 0 at 41 and 3 at 12", named)
	}
	if off, ok := named[1]; !ok || off != -1 {
		t.Errorf("an uncommitted partition answered %v, want -1 with no error", named[1])
	}

	// All-topics form: a null topics array.
	all := kmsg.NewPtrOffsetFetchRequest()
	all.Group = group
	all.Topics = nil
	allResp, err := all.RequestWith(ctxFor(t, 20*time.Second), cl)
	if err != nil {
		t.Fatalf("OffsetFetch(all topics): %v", err)
	}
	if allResp.ErrorCode != 0 {
		t.Fatalf("OffsetFetch(all topics): error code %d", allResp.ErrorCode)
	}
	found := map[int32]int64{}
	for _, rt := range allResp.Topics {
		if rt.Topic != topic {
			t.Errorf("OffsetFetch(all topics) returned topic %s, which this group never committed",
				rt.Topic)
			continue
		}
		for _, rp := range rt.Partitions {
			if rp.ErrorCode != 0 {
				t.Errorf("OffsetFetch(all topics) %s/%d: error code %d", rt.Topic, rp.Partition, rp.ErrorCode)
			}
			found[rp.Partition] = rp.Offset
		}
	}
	if len(found) != 2 || found[0] != 41 || found[3] != 12 {
		t.Errorf("OffsetFetch(all topics) answered %v, want exactly {0:41, 3:12}", found)
	}

	// The metadata a client stamps on a commit survives the round trip.
	for _, rt := range allResp.Topics {
		for _, rp := range rt.Partitions {
			if rp.Partition == 0 && (rp.Metadata == nil || *rp.Metadata != "batch-7") {
				t.Errorf("partition 0 metadata came back as %v, want \"batch-7\"", rp.Metadata)
			}
		}
	}
}

// fetchOffsets reads one topic's committed offsets for a group, the named way,
// and returns partition → offset (-1 where nothing is committed).
func fetchOffsets(t *testing.T, group, topic string) map[int32]int64 {
	t.Helper()
	cl := newClient(t)
	req := kmsg.NewPtrOffsetFetchRequest()
	req.Group = group
	rt := kmsg.NewOffsetFetchRequestTopic()
	rt.Topic = topic
	for p := int32(0); p < topicWidth(t); p++ {
		rt.Partitions = append(rt.Partitions, p)
	}
	req.Topics = append(req.Topics, rt)

	resp, err := req.RequestWith(ctxFor(t, 20*time.Second), cl)
	if err != nil {
		t.Fatalf("OffsetFetch: %v", err)
	}
	if resp.ErrorCode != 0 {
		t.Fatalf("OffsetFetch: error code %d", resp.ErrorCode)
	}
	out := make(map[int32]int64)
	for _, topicResp := range resp.Topics {
		for _, p := range topicResp.Partitions {
			if p.ErrorCode != 0 {
				t.Fatalf("OffsetFetch %s/%d: error code %d", topicResp.Topic, p.Partition, p.ErrorCode)
			}
			out[p.Partition] = p.Offset
		}
	}
	return out
}

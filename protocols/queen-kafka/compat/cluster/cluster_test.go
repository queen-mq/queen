package cluster

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// SCENARIO 1 — THE ACCEPTANCE.
//
// Three facades in cluster mode, at least one pair in front of DIFFERENT Queen
// brokers of one HA deployment sharing one Postgres. 200+ records over 8
// partitions. ONE consumer group with three members, each bootstrapped against
// a DIFFERENT facade. What must hold:
//
//	total consumed EXACTLY equals produced           (no loss, no duplication)
//	partitions are split between members, not shared (the double-delivery defect)
//	ONE coordinator is observed                      (the non-owners answer
//	                                                  NOT_COORDINATOR and the
//	                                                  clients follow the redirect)
//	committed offsets never move backwards           (the 50-then-16 rewind),
//	                                                  sampled DURING the run
//
// This is the test the whole design exists to pass. Every clause of it failed
// before cluster mode: two facades each answered FindCoordinator with
// themselves, formed the same group twice, each generation assigned all eight
// partitions, and the two coordinators' offset commits overwrote each other.
func TestAcceptanceOneGroupAcrossThreeFacades(t *testing.T) {
	if len(nodes) < 3 {
		t.Skipf("this scenario needs three clustered facades, QUEEN_KAFKA_NODES has %d", len(nodes))
	}
	const perPartition = 26 // 26 x 8 = 208 records, the brief's "200+"
	topic, total := seed(t, addrs(), perPartition)
	group := newName(t, "g")

	// ---- who coordinates it, according to every node
	owner := assertOneCoordinator(t, group)
	t.Logf("the cluster says group %s is coordinated by node %d at %s", group, owner.nodeID, owner.addr)

	// ---- the two non-owners refuse the group's membership RPCs
	//
	// Sent BEFORE the real members join, and to nodes that must refuse before
	// the coordinator is touched: a non-owner that spawned a group actor here
	// would be the double-delivery defect again, wearing a redirect.
	for _, n := range nodes {
		if n.id == owner.nodeID {
			continue
		}
		if got := joinGroupErr(t, fmt.Sprintf("node-%d", n.id), n.addr, group); got != 16 {
			t.Fatalf("JoinGroup at node %d (a NON-owner of %s) answered error %d, want 16 NOT_COORDINATOR",
				n.id, group, got)
		}
		t.Logf("JoinGroup at node %d (non-owner): error 16 NOT_COORDINATOR", n.id)
	}

	// ---- sample the committed offsets throughout, through a NON-owner
	//
	// A non-owner deliberately: OffsetFetch is the one group API cluster mode
	// serves everywhere, because it is a read of shared state whose answer is
	// identical at every node, and the assign()-based simple consumer that may
	// hold any connection would break if it were refused.
	sampler := nodes[0]
	for _, n := range nodes {
		if n.id != owner.nodeID {
			sampler = n
			break
		}
	}
	watch := watchOffsets(t, fmt.Sprintf("watch-node-%d", sampler.id), sampler.addr, group, topic, 250*time.Millisecond)

	// ---- three members, three different bootstrap addresses, one group
	boots := make([][]string, 0, len(nodes))
	names := make([]string, 0, len(nodes))
	for i, n := range nodes {
		boots = append(boots, []string{n.addr})
		names = append(names, fmt.Sprintf("m%d@%s", i+1, n.addr))
	}
	ledger, clients := runMembers(t, group, topic, total, boots)

	assertOneGeneration(t, clients)
	samples, regressions, high := watch.finish()
	for _, cl := range clients {
		cl.Close() // LeaveGroup, so the group is not left with three zombies
	}

	// ---- exactly what was produced, once each, and split not shared
	assertOneDelivery(t, ledger, total, names)

	// ---- the committed offsets never went backwards, and they add up
	t.Logf("the committed-offset sampler took %d samples through node %d during the run",
		samples, sampler.id)
	if samples < 3 {
		t.Errorf("only %d offset samples: the watch did not observe the run", samples)
	}
	for _, r := range regressions {
		t.Errorf("committed offset REGRESSION: %s", r)
	}
	var sum int64
	for _, v := range high {
		if v > 0 {
			sum += v
		}
	}
	if sum > int64(total) {
		t.Errorf("the sampled committed offsets sum to %d, past the %d records produced", sum, total)
	}

	// ---- and the final committed state is the same read through every node
	var first map[int32]int64
	for _, n := range nodes {
		got := committed(t, fmt.Sprintf("node-%d", n.id), n.addr, group, topic)
		if first == nil {
			first = got
			var final int64
			for _, v := range got {
				if v > 0 {
					final += v
				}
			}
			t.Logf("committed offsets through node %d sum to %d of %d produced", n.id, final, total)
			continue
		}
		for p, v := range first {
			if got[p] != v {
				t.Errorf("node %d reads %s/%d committed at %d, node %d reads %d",
					n.id, topic, p, got[p], nodes[0].id, v)
			}
		}
	}
}

// SCENARIO 2 — METADATA, and a producer that follows it.
//
// Every facade must describe the SAME cluster: the same brokers, the same
// controller, the same cluster id and the same leader map. Then a producer
// bootstrapped against ONE node must be able to write partitions another node
// leads, and everything must land.
func TestMetadataListsEveryLiveNode(t *testing.T) {
	if len(nodes) < 2 {
		t.Skip("needs at least two clustered facades")
	}
	const perPartition = 5
	topic, total := seed(t, addrs()[:1], perPartition)

	views := make([]view, 0, len(nodes))
	for _, n := range nodes {
		views = append(views, metadataView(t, fmt.Sprintf("node-%d", n.id), n.addr, topic))
	}

	want := make([]int32, 0, len(nodes))
	for _, n := range nodes {
		want = append(want, n.id)
	}
	sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })

	for i, v := range views {
		if got := v.ids(); !sameIDs(got, want) {
			t.Errorf("%s lists brokers %v, want %v", v.from, got, want)
		}
		if v.brokers[singleNodeID] != "" {
			t.Errorf("%s advertises node id 0, which cluster mode reserves for the single-node identity", v.from)
		}
		for _, n := range nodes {
			if got := v.brokers[n.id]; got != n.addr {
				t.Errorf("%s says node %d is at %q, want %q", v.from, n.id, got, n.addr)
			}
		}
		if v.controller != want[0] {
			t.Errorf("%s says the controller is %d, want the lowest live id %d", v.from, v.controller, want[0])
		}
		if v.clusterID != views[0].clusterID {
			t.Errorf("%s says cluster_id %q, %s says %q", v.from, v.clusterID, views[0].from, views[0].clusterID)
		}
		if len(v.leaders) != int(partitions) {
			t.Fatalf("%s described %d partitions of %s, want %d", v.from, len(v.leaders), topic, partitions)
		}
		for p, leader := range v.leaders {
			if _, live := v.brokers[leader]; !live {
				t.Errorf("%s says %s/%d is led by node %d, which is not in its own broker list", v.from, topic, p, leader)
			}
			if i > 0 && views[0].leaders[p] != leader {
				t.Errorf("%s says %s/%d is led by %d, %s says %d",
					v.from, topic, p, leader, views[0].from, views[0].leaders[p])
			}
			// Replication is Postgres's business: claiming N replicas would be
			// a lie a client could act on.
			if got := v.replicas[p]; len(got) != 1 || got[0] != leader {
				t.Errorf("%s says %s/%d has replicas %v, want [%d]", v.from, topic, p, got, leader)
			}
			if got := v.isr[p]; len(got) != 1 || got[0] != leader {
				t.Errorf("%s says %s/%d has ISR %v, want [%d]", v.from, topic, p, got, leader)
			}
			// A synthetic epoch that moved on every membership change would
			// invite clients to run truncation detection against a value
			// nothing maintains; OffsetForLeaderEpoch is not advertised.
			if got := v.epochs[p]; got != -1 {
				t.Errorf("%s says %s/%d has leader epoch %d, want -1", v.from, topic, p, got)
			}
		}
	}
	spread := map[int32]int{}
	for p, l := range views[0].leaders {
		spread[l]++
		_ = p
	}
	t.Logf("%s: leaders spread over the live set as %v", topic, spread)
	if len(spread) < 2 {
		t.Errorf("all %d partitions of %s are led by one node (%v): the rendezvous is not spreading",
			partitions, topic, spread)
	}

	// ---- a producer bootstrapped at ONE node writes every partition
	//
	// Including the ones another node leads: the client reads the leader map
	// out of the metadata this cluster just handed it, connects to the
	// advertised address, and the records land. That the addresses are
	// ROUTABLE is half of what this proves; the other half is that they are
	// the right ones.
	writer := newClient(t, []string{nodes[0].addr}, kgo.RequiredAcks(kgo.AllISRAcks()))
	var recs []*kgo.Record
	for p := int32(0); p < partitions; p++ {
		recs = append(recs, &kgo.Record{
			Topic:     topic,
			Partition: p,
			Key:       []byte(fmt.Sprintf("cross-%d", p)),
			Value:     []byte(fmt.Sprintf("cross-value-%d-%s", p, runID)),
		})
	}
	if err := writer.ProduceSync(ctxFor(t, 90*time.Second), recs...).FirstErr(); err != nil {
		t.Fatalf("producing through node %d to partitions led elsewhere: %v", nodes[0].id, err)
	}
	t.Logf("a producer bootstrapped only at node %d wrote all %d partitions of %s",
		nodes[0].id, partitions, topic)

	// ---- and everything is readable through a DIFFERENT node
	reader := newClient(t, []string{nodes[len(nodes)-1].addr},
		kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	want2 := total + int(partitions)
	got := drainRecords(t, reader, want2, 120*time.Second)
	if len(got) != want2 {
		t.Fatalf("read %d records through node %d, want %d", len(got), nodes[len(nodes)-1].id, want2)
	}
	t.Logf("all %d records read back through node %d", len(got), nodes[len(nodes)-1].id)

	// ---- leadership is an ADVERTISEMENT: a NON-leader serves the data path
	//
	// Apache Kafka answers NOT_LEADER_OR_FOLLOWER at a non-leader because a
	// non-leader does not have the data. That reason does not exist here: a
	// fetch takes no lease and writes nothing (032_log_fetch.sql:11-19) and
	// produce's offsets are allocated by the database under a row lock
	// (003_log_push.sql:131-213). Refusing would turn every membership change
	// into a synchronised metadata storm for nothing.
	for p := int32(0); p < partitions; p++ {
		leader := views[0].leaders[p]
		var nonLeader facade
		for _, n := range nodes {
			if n.id != leader {
				nonLeader = n
				break
			}
		}
		label := fmt.Sprintf("node-%d", nonLeader.id)
		code, hw, batches := fetchRaw(t, label, nonLeader.addr, topic, p, 0)
		if code != 0 {
			t.Errorf("Fetch of %s/%d (led by node %d) at NON-leader node %d: error %d, want 0 — "+
				"leadership is an advertisement, not an access control",
				topic, p, leader, nonLeader.id, code)
			continue
		}
		if hw != int64(perPartition+1) {
			t.Errorf("Fetch of %s/%d at node %d: high watermark %d, want %d", topic, p, nonLeader.id, hw, perPartition+1)
		}
		if !containsAll(batches, fmt.Sprintf("cross-value-%d-%s", p, runID)) {
			t.Errorf("Fetch of %s/%d at NON-leader node %d did not return the record written through node %d",
				topic, p, nonLeader.id, nodes[0].id)
		}
		lcode, end := endOffset(t, label, nonLeader.addr, topic, p)
		if lcode != 0 || end != int64(perPartition+1) {
			t.Errorf("ListOffsets of %s/%d at NON-leader node %d: error %d, end %d, want 0 and %d",
				topic, p, nonLeader.id, lcode, end, perPartition+1)
		}
	}
	t.Logf("every partition of %s was fetched from a node that does not lead it, and answered", topic)
}

// assertOneCoordinator asks every clustered facade who owns a group and fails
// unless they all give the same answer, naming the same node at the same
// address with error 0. FindCoordinator never refuses to name a live owner —
// that is the whole of Kafka's redirect dance, and it is what
// find_coordinator.rs used to short-circuit by answering itself.
func assertOneCoordinator(t *testing.T, group string) coordinator {
	t.Helper()
	var first coordinator
	for i, n := range nodes {
		got := findCoordinator(t, fmt.Sprintf("node-%d", n.id), n.addr, group)
		if got.errCode != 0 {
			t.Fatalf("FindCoordinator(%s) at node %d: %s, want error 0", group, n.id, got)
		}
		if _, ok := nodeByID(got.nodeID); !ok {
			t.Fatalf("FindCoordinator(%s) at node %d names node %d, which is not a configured facade",
				group, n.id, got.nodeID)
		}
		if i == 0 {
			first = got
			continue
		}
		if got != first {
			t.Fatalf("the cluster disagrees about who coordinates %s: node %d says %s, node %d says %s",
				group, nodes[0].id, first, n.id, got)
		}
	}
	if owner, ok := nodeByID(first.nodeID); ok && owner.addr != first.addr {
		t.Fatalf("FindCoordinator(%s) names node %d at %s, but that node is configured at %s",
			group, first.nodeID, first.addr, owner.addr)
	}
	return first
}

// SCENARIO 2b — 200 group ids, and not one disagreement.
//
// One group agreed on is a coincidence at three nodes. This is the assertion
// that ownership is a pure function of the live set: every node computes it
// from the same registry read with the same hash, so the answers cannot drift.
func TestEveryNodeAgreesOnEveryCoordinator(t *testing.T) {
	if len(nodes) < 2 {
		t.Skip("needs at least two clustered facades")
	}
	conns := make([]*rawConn, len(nodes))
	for i, n := range nodes {
		conns[i] = mustDial(t, fmt.Sprintf("node-%d", n.id), n.addr)
	}
	spread := map[int32]int{}
	disagreements := 0
	const groups = 200
	for i := 0; i < groups; i++ {
		group := fmt.Sprintf("qkc-owner-probe-%s-%d", runID, i)
		var first coordinator
		for j, k := range conns {
			got := findCoordinatorOn(t, k, group)
			if got.errCode != 0 {
				t.Fatalf("FindCoordinator(%s) at node %d: %s", group, nodes[j].id, got)
			}
			if j == 0 {
				first = got
				continue
			}
			if got != first {
				disagreements++
				if disagreements <= 5 {
					t.Errorf("%s: node %d says %s, node %d says %s", group, nodes[0].id, first, nodes[j].id, got)
				}
			}
		}
		spread[first.nodeID]++
	}
	if disagreements != 0 {
		t.Errorf("%d of %d group ids got different coordinators from different nodes", disagreements, groups*(len(nodes)-1))
	} else {
		t.Logf("%d group ids, %d nodes, zero disagreements", groups, len(nodes))
	}
	t.Logf("ownership of %d groups spread as %v", groups, spread)
	if len(spread) != len(nodes) {
		t.Errorf("only %d of %d nodes ever owned a group: the hash is not spreading", len(spread), len(nodes))
	}
	// Rendezvous over 200 items and N nodes is not perfectly even, but a node
	// holding less than half its share would mean the mix is broken.
	floor := groups / len(nodes) / 2
	for id, n := range spread {
		if n < floor {
			t.Errorf("node %d owns only %d of %d groups (floor %d)", id, n, groups, floor)
		}
	}
}

// --------------------------------------------------------------------- helpers

// contextUntil bounds one poll or one commit by the whole scenario's deadline,
// so a member that is waiting out a rebalance is not cut off early and a member
// that is stuck does not hang the suite.
func contextUntil(deadline time.Time) (context.Context, func()) {
	return context.WithDeadline(context.Background(), deadline)
}

func sameIDs(a, b []int32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func firstFew(m map[string]int, n int) map[string]int {
	out := map[string]int{}
	for k, v := range m {
		if len(out) >= n {
			break
		}
		out[k] = v
	}
	return out
}

// drainRecords reads `want` records or fails saying how far it got.
func drainRecords(t *testing.T, cl *kgo.Client, want int, budget time.Duration) []*kgo.Record {
	t.Helper()
	ctx := ctxFor(t, budget)
	out := make([]*kgo.Record, 0, want)
	for len(out) < want {
		fs := cl.PollRecords(ctx, want-len(out))
		if ctx.Err() != nil {
			t.Fatalf("timed out after %s with %d/%d records", budget, len(out), want)
		}
		if errs := fs.Errors(); len(errs) > 0 {
			t.Fatalf("fetch error at %d/%d records: %v", len(out), want, errs)
		}
		fs.EachRecord(func(r *kgo.Record) { out = append(out, r) })
	}
	return out
}

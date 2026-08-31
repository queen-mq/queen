package cluster

import (
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// SCENARIO 3 — NODE DEATH.
//
// Two halves, because the design makes two different promises about them:
//
//	the GROUP-OWNER dies      ownership moves after the registry TTL, the members
//	                          re-form on the new owner and resume from the
//	                          committed offset. Cost: a blackout of at most
//	                          TTL + the join delay, and at-least-once redelivery
//	                          of whatever was read but not committed.
//	a NON-OWNER data node dies  nothing at all on the data path, because every
//	                          node serves every partition. The leader map
//	                          re-spreads over the survivors on the next refresh.
//
// Both are driven through the rig's kill/start scripts, which resolve a NODE ID
// to the pid recorded when that facade was spawned. No pid is ever resolved
// from a port.

// deathRestore restarts a facade this test killed and blocks until the whole
// cluster agrees it is back.
//
// It waits out the registry TTL first, and that wait is not politeness: a boot
// claim is a putIfAbsent, and a facade restarted INSIDE the TTL loses it to its
// own still-live row from before the crash and exits FATAL on a duplicate node
// id. Restarting after the row has expired is the resurrection rule
// (024_kv.sql:1010-1015), which is what makes a crash-restart legal.
func deathRestore(t *testing.T, id int32, killedAt time.Time) {
	t.Helper()
	if wait := ttl + time.Second - time.Since(killedAt); wait > 0 {
		t.Logf("waiting %s for node %d's registry row to expire before restarting it", wait.Round(time.Millisecond), id)
		time.Sleep(wait)
	}
	startNode(t, id)
	noteRestart(fmt.Sprintf("node-%d", id))
	if err := waitConverged(takeoverBudget()); err != nil {
		t.Fatalf("after restarting node %d: %v", id, err)
	}
	t.Logf("node %d is back and every node lists all %d again", id, len(nodes))
}

// groupOwnedBy finds a group id the cluster agrees is coordinated by `id`.
// Ownership is a pure function of the group id and the live set, so this is a
// search over names and not a way of forcing anything.
func groupOwnedBy(t *testing.T, id int32) string {
	t.Helper()
	for i := 0; i < 500; i++ {
		group := fmt.Sprintf("%s-own%d-%d", newName(t, "g"), id, i)
		if c := findCoordinator(t, "probe", nodes[0].addr, group); c.errCode == 0 && c.nodeID == id {
			return group
		}
	}
	t.Fatalf("no group id out of 500 hashed to node %d", id)
	return ""
}

func TestNodeDeathOfTheGroupOwner(t *testing.T) {
	if killCmd == "" || startCmd == "" {
		t.Skip("QUEEN_KAFKA_KILL_CMD / QUEEN_KAFKA_START_CMD are unset")
	}
	if len(nodes) < 3 {
		t.Skip("needs three clustered facades: two must survive the kill")
	}

	// Kill the node that owns the group, and let the two members live on the
	// two that survive.
	victim := nodes[1] // the facade in front of the OTHER Queen broker
	group := groupOwnedBy(t, victim.id)
	survivors := make([]facade, 0, 2)
	for _, n := range nodes {
		if n.id != victim.id {
			survivors = append(survivors, n)
		}
	}
	const perPartition = 16
	topic, total := seed(t, addrs(), perPartition)
	half := total / 2
	t.Logf("group %s is owned by node %d; %d records over %d partitions; members at nodes %d and %d",
		group, victim.id, total, partitions, survivors[0].id, survivors[1].id)

	ledger := newSeen()
	deadline := time.Now().Add(3 * takeoverBudget())
	var wg sync.WaitGroup
	var commitErrs sync.Map
	for i, n := range survivors {
		member := fmt.Sprintf("m%d@node-%d", i+1, n.id)
		// Seeded at ONE facade each, deliberately: a client that had all three
		// in bootstrap.servers could recover by luck. This one has to be told
		// where the new coordinator is.
		cl := newClient(t, []string{n.addr}, eagerGroup(group, topic, kgo.DisableAutoCommit())...)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer cl.Close()
			for time.Now().Before(deadline) && ledger.uniqueKeys() < total {
				ctx, cancel := contextUntil(deadline)
				fs := cl.PollRecords(ctx, 32)
				cancel()
				var batch []*kgo.Record
				fs.EachRecord(func(r *kgo.Record) {
					ledger.add(member, r.Partition, r.Key)
					batch = append(batch, r)
				})
				if len(batch) == 0 {
					continue
				}
				cctx, ccancel := contextUntil(deadline)
				if err := cl.CommitRecords(cctx, batch...); err != nil {
					commitErrs.Store(err.Error(), true)
				}
				ccancel()
			}
		}()
	}

	// ---- let half of it through, then take the coordinator out
	waitFor(t, "half the topic to be consumed", takeoverBudget(), func() bool {
		return ledger.uniqueKeys() >= half
	})
	before := committed(t, fmt.Sprintf("node-%d", survivors[0].id), survivors[0].addr, group, topic)
	t.Logf("committed before the kill: %v", before)

	killedAt := time.Now()
	killNode(t, victim.id)
	t.Cleanup(func() { deathRestore(t, victim.id, killedAt) })

	// ---- the survivors move ownership, and they agree about it
	var newOwner coordinator
	waitFor(t, "a new coordinator both survivors agree on", takeoverBudget(), func() bool {
		a := findCoordinator(t, "s0", survivors[0].addr, group)
		b := findCoordinator(t, "s1", survivors[1].addr, group)
		if a.errCode != 0 || a != b || a.nodeID == victim.id {
			return false
		}
		newOwner = a
		return true
	})
	moved := time.Since(killedAt)
	t.Logf("ownership of %s moved from the dead node %d to node %d in %s (budget: TTL %s + join delay %s)",
		group, victim.id, newOwner.nodeID, moved.Round(time.Millisecond), ttl, joinDelay)
	if newOwner.nodeID == victim.id {
		t.Fatalf("the group is still assigned to the dead node %d", victim.id)
	}
	if _, ok := nodeByID(newOwner.nodeID); !ok {
		t.Fatalf("the new coordinator %d is not a configured facade", newOwner.nodeID)
	}
	// The dead node must actually leave the broker list, or clients would keep
	// dialling it for ever.
	waitFor(t, "the dead node to leave the survivors' broker lists", takeoverBudget(), func() bool {
		for _, s := range survivors {
			ids, err := brokerIDs(s.addr)
			if err != nil {
				return false
			}
			for _, id := range ids {
				if id == victim.id {
					return false
				}
			}
		}
		return true
	})
	t.Logf("node %d has left every survivor's broker list", victim.id)

	// ---- and the two members finish the topic
	wg.Wait()

	if got := ledger.uniqueKeys(); got != total {
		t.Errorf("after the coordinator died the group delivered %d of %d distinct keys: records were LOST", got, total)
	}
	dups := ledger.duplicates()
	// At-least-once is the contract across a failover: whatever was read but
	// not committed when the coordinator died comes again. What must not
	// happen is loss, or a duplication that is not explained by the crash.
	t.Logf("%d keys were redelivered after the failover (at-least-once, expected)", len(dups))
	if len(dups) > total/2 {
		t.Errorf("%d of %d keys were redelivered: that is a re-read of the whole topic, not a resume "+
			"from the committed offset", len(dups), total)
	}

	// ---- the committed offsets survived, and none of them went backwards
	after := committed(t, fmt.Sprintf("node-%d", survivors[1].id), survivors[1].addr, group, topic)
	for p, was := range before {
		if was < 0 {
			continue
		}
		if after[p] < was {
			t.Errorf("%s/%d: committed offset was %d before the kill and is %d after — the crash REWOUND it",
				topic, p, was, after[p])
		}
	}
	var sum int64
	for _, v := range after {
		if v > 0 {
			sum += v
		}
	}
	if sum != int64(total) {
		t.Errorf("committed offsets sum to %d after the run, want %d (every record committed exactly once)", sum, total)
	} else {
		t.Logf("committed offsets sum to %d of %d produced, read back through node %d", sum, total, survivors[1].id)
	}
	commitErrs.Range(func(k, _ any) bool {
		t.Logf("a commit error seen during the failover (expected, the client retried): %v", k)
		return true
	})
}

func TestNodeDeathOfANonOwnerDataNode(t *testing.T) {
	if killCmd == "" || startCmd == "" {
		t.Skip("QUEEN_KAFKA_KILL_CMD / QUEEN_KAFKA_START_CMD are unset")
	}
	if len(nodes) < 3 {
		t.Skip("needs three clustered facades")
	}
	const perPartition = 4
	topic, total := seed(t, addrs(), perPartition)

	// The victim is chosen FROM the leader map rather than pinned: rendezvous
	// spreads eight partitions over three nodes, and which node draws which is
	// a property of the topic name. Pinning a node id here made the test skip
	// itself whenever that node happened to lead nothing, which is exactly the
	// run in which it proves the least.
	before := metadataView(t, "before", nodes[0].addr, topic)
	byLeader := map[int32]map[int32]bool{}
	for p, l := range before.leaders {
		if byLeader[l] == nil {
			byLeader[l] = map[int32]bool{}
		}
		byLeader[l][p] = true
	}
	var victim facade
	best := 0
	for _, n := range nodes {
		if len(byLeader[n.id]) > best {
			victim, best = n, len(byLeader[n.id])
		}
	}
	if best == 0 {
		t.Fatalf("no node leads any partition of %s: %v", topic, before.leaders)
	}
	led := byLeader[victim.id]
	survivors := make([]facade, 0, len(nodes)-1)
	for _, n := range nodes {
		if n.id != victim.id {
			survivors = append(survivors, n)
		}
	}
	t.Logf("node %d leads partitions %v of %s — the most of any node, so it is the victim",
		victim.id, keysOf(led), topic)

	killedAt := time.Now()
	killNode(t, victim.id)
	t.Cleanup(func() { deathRestore(t, victim.id, killedAt) })

	// ---- produce and fetch through a survivor NEVER fail, even for the
	//      partitions the dead node was advertised as leading.
	writer := newClient(t, []string{survivors[0].addr}, kgo.RequiredAcks(kgo.AllISRAcks()))
	var recs []*kgo.Record
	for p := range led {
		recs = append(recs, &kgo.Record{
			Topic:     topic,
			Partition: p,
			Key:       []byte(fmt.Sprintf("orphan-%d", p)),
			Value:     []byte(fmt.Sprintf("orphan-value-%d-%s", p, runID)),
		})
	}
	if err := writer.ProduceSync(ctxFor(t, 120*time.Second), recs...).FirstErr(); err != nil {
		t.Fatalf("producing to the dead node's partitions through node %d: %v", survivors[0].id, err)
	}
	t.Logf("%d records written to the dead node's partitions through node %d", len(recs), survivors[0].id)

	for p := range led {
		code, hw, batches := fetchRaw(t, "survivor", survivors[1].addr, topic, p, 0)
		if code != 0 {
			t.Errorf("Fetch of %s/%d through node %d after node %d died: error %d",
				topic, p, survivors[1].id, victim.id, code)
			continue
		}
		if hw != int64(perPartition+1) {
			t.Errorf("Fetch of %s/%d through node %d: high watermark %d, want %d",
				topic, p, survivors[1].id, hw, perPartition+1)
		}
		if !containsAll(batches, fmt.Sprintf("orphan-value-%d-%s", p, runID)) {
			t.Errorf("the record written to %s/%d after the death is not readable through node %d",
				topic, p, survivors[1].id)
		}
	}

	// ---- and the leader map re-spreads over the survivors
	waitFor(t, "the leader map to re-spread over the survivors", takeoverBudget(), func() bool {
		v := metadataView(t, "after", survivors[0].addr, topic)
		if len(v.brokers) != len(nodes)-1 {
			return false
		}
		for _, l := range v.leaders {
			if l == victim.id {
				return false
			}
		}
		return true
	})
	after := metadataView(t, "after", survivors[0].addr, topic)
	spread := map[int32]int{}
	for _, l := range after.leaders {
		spread[l]++
	}
	t.Logf("with node %d dead, %s's leaders are %v and the broker list is %v",
		victim.id, topic, spread, after.ids())

	// ---- a client that refreshes now follows the new map and everything lands
	reader := newClient(t, []string{survivors[1].addr},
		kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	want := total + len(recs)
	got := drainRecords(t, reader, want, 120*time.Second)
	if len(got) != want {
		t.Errorf("read %d records after the death, want %d", len(got), want)
	} else {
		t.Logf("all %d records readable through node %d after node %d died", want, survivors[1].id, victim.id)
	}
}

// waitFor polls a condition and fails with the caller's own description.
func waitFor(t *testing.T, what string, budget time.Duration, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", budget, what)
}

func keysOf(m map[int32]bool) []int32 {
	out := make([]int32, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

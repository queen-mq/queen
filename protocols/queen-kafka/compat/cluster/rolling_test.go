package cluster

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// SCENARIO 10 — ROLLING RESTART, the shape a deploy actually has.
//
// Scenario 3 kills a facade and starts it again after its registry row has
// expired. A deploy does neither: it SIGTERMs one pod, starts its replacement
// with the SAME node id (a StatefulSet ordinal is where a node id comes from),
// and moves on to the next one, all inside the registry TTL. That sequence used
// to be fatal in two ways at once:
//
//	the REPLACEMENT met its own predecessor's row, lost the boot claim to a
//	foreign incarnation and exited 1 — a CrashLoopBackOff for as long as the
//	backoff took to outlast the TTL, on a facade whose configuration was
//	correct;
//
//	the SURVIVORS kept advertising the pod that had already gone, in Metadata
//	and in FindCoordinator, for a whole TTL after it stopped serving.
//
// So this rolls all three nodes one at a time, with a consumer group live
// throughout, and asserts the three things a deploy has to be able to promise:
// every replacement starts, nothing keeps pointing clients at a stopped node
// for longer than one TTL, and the group finishes the topic with no committed
// offset ever going backwards.
//
// It runs at whatever cadence the rig was started with, and the cadence worth
// running it at is the PRODUCT DEFAULT (2000 ms heartbeat, 10000 ms TTL): the
// crash-loop this proves fixed is a function of the TTL, and a rig that shrinks
// the TTL to 3 s shrinks the very window under test.

// rollingBudget sizes the whole roll: one takeover budget per node, which is
// what a stop, a convergence and a rebalance cost together, plus one more for
// the group to finish what was left when the last replacement came back.
func rollingBudget() time.Duration {
	return time.Duration(len(nodes)+1) * takeoverBudget()
}

func TestRollingRestartOfEveryNode(t *testing.T) {
	if stopCmd == "" || startCmd == "" {
		t.Skip("QUEEN_KAFKA_STOP_CMD / QUEEN_KAFKA_START_CMD are unset")
	}
	if len(nodes) < 3 {
		t.Skip("needs three clustered facades: two must serve while the third is being replaced")
	}

	const perPartition = 24
	topic, total := seed(t, addrs(), perPartition)
	group := newName(t, "roll")
	t.Logf("rolling %d nodes one at a time at heartbeat/TTL of this rig (TTL %s), group %s over %d records",
		len(nodes), ttl, group, total)

	// Both members bootstrap with the WHOLE cluster. That is not a convenience:
	// a roll takes every node in turn, so a client pinned to one address is
	// guaranteed to be homeless before the test ends, and what is under test is
	// the cluster's behaviour and not a client's bad configuration.
	ledger := newSeen()
	deadline := time.Now().Add(rollingBudget())
	var wg sync.WaitGroup
	var commitErrs sync.Map
	for i := 0; i < 2; i++ {
		member := fmt.Sprintf("m%d", i+1)
		cl := newClient(t, addrs(), eagerGroup(group, topic, kgo.DisableAutoCommit())...)
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

	// The group has to be live BEFORE the first node goes, or the roll would be
	// happening to an idle cluster and would prove nothing about a deploy under
	// traffic.
	waitFor(t, "the group to start consuming before the roll begins", takeoverBudget(), func() bool {
		return ledger.uniqueKeys() > 0
	})

	// Sampled across whatever node is up at the time, because every node is
	// stopped at some point in this test: a watch pinned to one socket would go
	// blind exactly when it matters.
	watch := watchCommittedAcrossNodes(group, topic, 500*time.Millisecond)

	for _, victim := range nodes {
		rollOne(t, victim, group)
	}

	samples, gaps, regressions := watch.finish()
	wg.Wait()

	// ---- nothing pointed at a stopped node, every replacement started, and:
	if got := ledger.uniqueKeys(); got != total {
		t.Errorf("after rolling every node the group delivered %d of %d distinct keys: records were LOST",
			got, total)
	}
	dups := ledger.duplicates()
	t.Logf("%d keys were redelivered across %d rebalances (at-least-once, expected)", len(dups), len(nodes))
	if len(dups) > total/2 {
		t.Errorf("%d of %d keys were redelivered: the group re-read the topic instead of resuming "+
			"from its committed offsets", len(dups), total)
	}

	// ---- and no committed offset ever went backwards, which is the failure a
	//      rolling deploy would produce silently if a replacement coordinated a
	//      group its predecessor still believed it owned.
	t.Logf("the committed-offset watch took %d samples (%d gaps while a node was down)", samples, gaps)
	if samples == 0 {
		t.Error("the committed-offset watch never got an answer: it proved nothing")
	}
	for _, r := range regressions {
		t.Errorf("committed offset REGRESSED during the roll: %s", r)
	}

	after := committed(t, "final", nodes[0].addr, group, topic)
	var sum int64
	for _, v := range after {
		if v > 0 {
			sum += v
		}
	}
	if sum != int64(total) {
		t.Errorf("committed offsets sum to %d after the roll, want %d (every record committed exactly once)",
			sum, total)
	} else {
		t.Logf("committed offsets sum to %d of %d produced, read back through node %d",
			sum, total, nodes[0].id)
	}
	commitErrs.Range(func(k, _ any) bool {
		t.Logf("a commit error seen during the roll (expected, the client retried): %v", k)
		return true
	})
}

// The other half of the same blocker, and the one a deregistration cannot help
// with: a facade that was KILLED leaves its row behind, and its replacement
// comes up inside the TTL anyway — an OOM kill, a lost node, or a `kubectl
// delete pod --force` all produce exactly this. The replacement used to exit 1
// on a row nobody was refreshing, which is a CrashLoopBackOff on a correctly
// configured pod. It now waits that row out and takes the id.
//
// This is deliberately NOT `deathRestore`: the whole point is to restart
// immediately, inside the TTL, with the corpse's row still readable.
func TestReplacementInsideTheTtlAfterACrash(t *testing.T) {
	if killCmd == "" || startCmd == "" {
		t.Skip("QUEEN_KAFKA_KILL_CMD / QUEEN_KAFKA_START_CMD are unset")
	}
	victim := nodes[len(nodes)-1]
	name := fmt.Sprintf("node-%d", victim.id)

	killedAt := time.Now()
	killNode(t, victim.id)
	// No wait. The row is still there, still inside its TTL, and the boot claim
	// is about to meet it.
	startNode(t, victim.id)
	noteRestart(name)
	booted := time.Since(killedAt)
	assertBootedCleanly(t, name, victim.id)
	t.Logf("node %d was SIGKILLed and restarted with the same id %s later, inside its own %s TTL, "+
		"and it started", victim.id, booted.Round(time.Millisecond), ttl)

	// It cannot have started before its predecessor's row was gone: that is the
	// watch, and the lower bound is what tells a real adoption from a lucky
	// putIfAbsent against an already-expired row.
	if booted < ttl {
		t.Errorf("node %d bound its listener %s after the kill, which is less than the %s TTL its "+
			"predecessor's row was written with: the row cannot have been dealt with",
			victim.id, booted.Round(time.Millisecond), ttl)
	}
	if err := waitConverged(takeoverBudget()); err != nil {
		t.Fatalf("after restarting the killed node %d: %v", victim.id, err)
	}

	// ...and it says so, because a facade that adopts an id somebody else's row
	// was holding is an event an operator has to be able to find afterwards.
	if logDir != "" {
		body, err := os.ReadFile(logPath(name))
		if err != nil {
			t.Fatalf("reading %s: %v", logPath(name), err)
		}
		if !strings.Contains(string(body), "id is taken back") &&
			!strings.Contains(string(body), "id is taken over") {
			t.Errorf("node %d started after a crash inside the TTL but never logged that it took "+
				"its own id back from the row its predecessor left", victim.id)
		}
	}

	// The group RPCs work at it again, which is the whole point of having the
	// id rather than merely being up.
	c := findCoordinator(t, name, victim.addr, newName(t, "aftercrash"))
	if c.errCode != 0 {
		t.Errorf("node %d answers FindCoordinator with error %d after re-taking its id", victim.id, c.errCode)
	}
}

// rollOne replaces one facade the way a deploy does: SIGTERM, wait for the rest
// of the cluster to stop pointing at it, start the same node id again.
func rollOne(t *testing.T, victim facade, live string) {
	t.Helper()
	survivors := make([]facade, 0, len(nodes)-1)
	for _, n := range nodes {
		if n.id != victim.id {
			survivors = append(survivors, n)
		}
	}

	// Groups that the cluster says the victim coordinates RIGHT NOW. Sampled
	// before the stop and asserted non-empty, so the assertion after the stop
	// cannot pass by testing nothing.
	probes := probeGroups(t, victim, live)

	stoppedAt := time.Now()
	stopNode(t, victim.id)

	// THE assertion. Metadata and FindCoordinator must both stop naming the
	// stopped node, and the bound is one TTL — which is the bound even without
	// a deregistration, since the row would expire. What the deregistration
	// buys is that it happens in about one heartbeat instead, and the log line
	// below is where that shows.
	waitFor(t, fmt.Sprintf("node %d to leave every survivor's broker list and coordinator answers",
		victim.id), takeoverBudget(), func() bool {
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
			for _, g := range probes {
				c, err := coordinatorOf(s.addr, g)
				if err != nil || c.errCode != 0 || c.nodeID == victim.id {
					return false
				}
			}
		}
		return true
	})
	gone := time.Since(stoppedAt)
	t.Logf("node %d left every survivor's broker list and all %d of its groups moved in %s (one TTL is %s)",
		victim.id, len(probes), gone.Round(time.Millisecond), ttl)
	if gone > ttl {
		t.Errorf("a stopped node was still being advertised %s after it was asked to stop; one TTL is %s. "+
			"Every client sent to node %d in that window met a closed port", gone.Round(time.Millisecond), ttl, victim.id)
	}

	// ---- and the replacement, with the SAME node id, inside the same TTL.
	name := fmt.Sprintf("node-%d", victim.id)
	sinceStop := time.Since(stoppedAt)
	startNode(t, victim.id)
	noteRestart(name)
	t.Logf("node %d was restarted %s after it was stopped, which is inside the %s TTL its own row was written with",
		victim.id, sinceStop.Round(time.Millisecond), ttl)
	assertBootedCleanly(t, name, victim.id)
	if err := waitConverged(takeoverBudget()); err != nil {
		t.Fatalf("after restarting node %d: %v", victim.id, err)
	}
	t.Logf("node %d is back and every node lists all %d again", victim.id, len(nodes))
}

// probeGroups finds group ids the cluster currently says `victim` coordinates.
// The live group is included when it is one of them, because the assertion is
// about the group under traffic first of all.
func probeGroups(t *testing.T, victim facade, live string) []string {
	t.Helper()
	var out []string
	if c := findCoordinator(t, "probe", nodes[0].addr, live); c.errCode == 0 && c.nodeID == victim.id {
		out = append(out, live)
	}
	for i := 0; len(out) < 4 && i < 400; i++ {
		g := fmt.Sprintf("qkc-roll-probe-%s-%d-%d", runID, victim.id, i)
		if c := findCoordinator(t, "probe", nodes[0].addr, g); c.errCode == 0 && c.nodeID == victim.id {
			out = append(out, g)
		}
	}
	if len(out) == 0 {
		t.Fatalf("no group id out of 400 is coordinated by node %d, so stopping it would assert nothing",
			victim.id)
	}
	return out
}

// assertBootedCleanly fails with the facade's own words when a replacement did
// not start. startNode already fails when the port never opens; this is what
// turns "it never listened" into the line that says why, and it is the direct
// regression test for the boot claim exiting 1 on its predecessor's row.
func assertBootedCleanly(t *testing.T, name string, id int32) {
	t.Helper()
	if logDir == "" {
		return
	}
	body, err := os.ReadFile(logPath(name))
	if err != nil {
		return
	}
	// Only what this restart wrote: the log is appended to across restarts on
	// purpose, so the whole run reads in one file.
	tail := string(body)
	if mark := logMarks[name]; mark > 0 && int64(len(tail)) > mark {
		tail = tail[mark:]
	}
	for _, line := range strings.Split(tail, "\n") {
		if strings.Contains(line, "FATAL") {
			t.Fatalf("node %d refused to start during the roll: %s", id, strings.TrimSpace(line))
		}
	}
}

// ------------------------------------------------- the roaming committed-offset watch

// nodeWatch samples a group's committed offsets through whichever facade is up,
// and records any partition whose committed offset went BACKWARDS. It differs
// from watchOffsets in one way that matters here: it re-dials every sample and
// walks the nodes, because this test stops every one of them in turn.
type nodeWatch struct {
	mu          sync.Mutex
	high        map[int32]int64
	regressions []string
	samples     int
	gaps        int
	stop        chan struct{}
	done        chan struct{}
}

func watchCommittedAcrossNodes(group, topic string, every time.Duration) *nodeWatch {
	w := &nodeWatch{high: map[int32]int64{}, stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(w.done)
		tick := time.NewTicker(every)
		defer tick.Stop()
		at := 0
		for {
			select {
			case <-w.stop:
				return
			case <-tick.C:
			}
			at = (at + 1) % len(nodes)
			offsets, err := committedFrom(nodes[at].addr, group, topic)
			w.mu.Lock()
			if err != nil {
				// A node that is down is not a finding: this test puts them
				// down. It is counted so that a watch which answered nothing at
				// all cannot look like a watch that saw no regression.
				w.gaps++
				w.mu.Unlock()
				continue
			}
			w.samples++
			for p, o := range offsets {
				if prev, ok := w.high[p]; ok && o < prev {
					w.regressions = append(w.regressions,
						fmt.Sprintf("%s/%d: committed offset went %d -> %d (read through node %d)",
							topic, p, prev, o, nodes[at].id))
				} else {
					w.high[p] = o
				}
			}
			w.mu.Unlock()
		}
	}()
	return w
}

func (w *nodeWatch) finish() (samples, gaps int, regressions []string) {
	close(w.stop)
	<-w.done
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.samples, w.gaps, append([]string(nil), w.regressions...)
}

// committedFrom is `committed` without the fatal: this test asks nodes that may
// be halfway through a restart, and a refused connection is an expected answer
// rather than a failed assertion.
func committedFrom(addr, group, topic string) (map[int32]int64, error) {
	k, err := dialRaw("watch", addr)
	if err != nil {
		return nil, err
	}
	defer k.Close()
	req := kmsg.NewPtrOffsetFetchRequest()
	req.Group = group
	rt := kmsg.NewOffsetFetchRequestTopic()
	rt.Topic = topic
	for p := int32(0); p < partitions; p++ {
		rt.Partitions = append(rt.Partitions, p)
	}
	req.Topics = append(req.Topics, rt)
	resp, err := k.do(req, 1)
	if err != nil {
		return nil, err
	}
	out := map[int32]int64{}
	for _, rtr := range resp.(*kmsg.OffsetFetchResponse).Topics {
		for _, p := range rtr.Partitions {
			if p.ErrorCode != 0 || p.Offset < 0 {
				continue
			}
			out[p.Partition] = p.Offset
		}
	}
	return out, nil
}

// coordinatorOf is `findCoordinator` without the fatal, for the same reason.
func coordinatorOf(addr, group string) (coordinator, error) {
	k, err := dialRaw("probe", addr)
	if err != nil {
		return coordinator{}, err
	}
	defer k.Close()
	req := kmsg.NewPtrFindCoordinatorRequest()
	req.CoordinatorKey = group
	req.CoordinatorType = 0
	resp, err := k.do(req, 1)
	if err != nil {
		return coordinator{}, err
	}
	fc := resp.(*kmsg.FindCoordinatorResponse)
	return coordinator{
		errCode: fc.ErrorCode,
		nodeID:  fc.NodeID,
		addr:    fmt.Sprintf("%s:%d", fc.Host, fc.Port),
	}, nil
}

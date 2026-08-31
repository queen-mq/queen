package cluster

import (
	"testing"
)

// SCENARIO 5 — THE OLD SPLIT-BRAIN SHAPE, still doing exactly what it did.
//
// Two INDEPENDENT single-node facades in front of one Queen broker: no node id,
// no registry, no shared live set — the shape the measured two-facade
// experiment ran, and the shape an operator gets by starting a second facade
// and forgetting the cluster configuration.
//
// The point of this file is that the acceptance above is not luck. Every clause
// here is asserted twice: once against the two independent facades, where it
// must still be broken, and once against the clustered ones, where the SAME
// sequence must be refused. If cluster mode were doing nothing, both halves
// would read alike.
//
// Note what the second half does NOT prove: it is the ownership GUARD that
// refuses the second commit, not the compare-and-set fence. The fence is the
// deeper of the two mechanisms — it is what stops a node that is STALE about
// the live set, and therefore still believes the guard passed, from rewinding
// the real owner's offsets — and forcing that state needs a test-only switch on
// the registry read that the facade does not have. See README.md.

func TestSplitBrainIsStillSplitAndClusterModeIsWhatFixesIt(t *testing.T) {
	if len(splitAddrs) < 2 {
		t.Skip("QUEEN_KAFKA_SPLIT needs two independent single-node facades")
	}
	a, b := splitAddrs[0], splitAddrs[1]

	// ---- 1. each independent facade is "the only broker"
	//
	// This is why two bootstrap addresses collapse to one: the client replaces
	// bootstrap.servers with the one-broker list the first Metadata returned.
	for _, addr := range []string{a, b} {
		v := metadataView(t, "split", addr, "")
		if got := v.ids(); len(got) != 1 || got[0] != singleNodeID {
			t.Errorf("the independent facade at %s lists brokers %v, want exactly [%d]", addr, got, singleNodeID)
		}
		if got := v.brokers[singleNodeID]; got != addr {
			t.Errorf("the independent facade at %s advertises %q", addr, got)
		}
	}
	t.Logf("both independent facades advertise themselves alone: %s and %s", a, b)

	// ---- 2. and each answers FindCoordinator with ITSELF, for the same group
	group := newName(t, "g")
	ca := findCoordinator(t, "split-a", a, group)
	cb := findCoordinator(t, "split-b", b, group)
	if ca.addr != a || cb.addr != b {
		t.Fatalf("the independent facades did not both answer themselves: %s said %s, %s said %s", a, ca, b, cb)
	}
	if ca == cb {
		t.Fatalf("the two independent facades agreed on a coordinator (%s): that is not the documented shape", ca)
	}
	t.Logf("SPLIT BRAIN, as documented: %s says the coordinator of %s is %s, %s says %s",
		a, group, ca.addr, b, cb.addr)

	// ---- 3. the same group id, asked of the CLUSTER, gets one answer
	if len(nodes) >= 2 {
		owner := assertOneCoordinator(t, group)
		t.Logf("the SAME group id, asked of all %d clustered facades: one answer, node %d at %s",
			len(nodes), owner.nodeID, owner.addr)
	}

	// ---- 4. the 50-then-16 rewind, still there, through two unclustered facades
	//
	// The commit is a simple-consumer commit (generation -1, empty member id):
	// there is no membership for the coordinator's own check to catch, so what
	// is left is exactly the node-level question. Unclustered, the second write
	// is an unconditional upsert and the first one is gone — silently, which is
	// what made the original defect so expensive to find.
	topic, _ := seed(t, []string{a}, 1)
	if got := commitSimple(t, "split-a", a, group, topic, 0, 50); got != 0 {
		t.Fatalf("committing 50 through %s: error %d, want 0", a, got)
	}
	if got := committed(t, "split-b", b, group, topic)[0]; got != 50 {
		t.Fatalf("after committing 50 through %s, %s reads %d", a, b, got)
	}
	if got := commitSimple(t, "split-b", b, group, topic, 0, 16); got != 0 {
		t.Fatalf("committing 16 through %s: error %d — an unclustered facade has nothing to refuse with", b, got)
	}
	after := committed(t, "split-a", a, group, topic)[0]
	if after != 16 {
		t.Errorf("two independent facades committed 50 then 16 and the stored offset is %d; the documented "+
			"behaviour is 16 (last writer wins, unconditionally)", after)
	} else {
		t.Logf("REWIND REPRODUCED on the unclustered pair: committed 50 through %s, 16 through %s, "+
			"stored offset is now %d", a, b, after)
	}

	// ---- 5. and the identical sequence against the cluster is REFUSED
	if len(nodes) < 2 {
		t.Skip("the contrast needs at least two clustered facades")
	}
	cgroup := newName(t, "g")
	ctopic, _ := seed(t, addrs(), 1)
	owner := assertOneCoordinator(t, cgroup)
	ownerNode, _ := nodeByID(owner.nodeID)
	var nonOwner facade
	for _, n := range nodes {
		if n.id != owner.nodeID {
			nonOwner = n
			break
		}
	}

	if got := commitSimple(t, "owner", ownerNode.addr, cgroup, ctopic, 0, 50); got != 0 {
		t.Fatalf("committing 50 at the owner (node %d): error %d, want 0", ownerNode.id, got)
	}
	if got := commitSimple(t, "non-owner", nonOwner.addr, cgroup, ctopic, 0, 16); got != 16 {
		t.Fatalf("committing 16 at NON-owner node %d: error %d, want 16 NOT_COORDINATOR", nonOwner.id, got)
	}
	t.Logf("the same sequence in cluster mode: 50 accepted at owner node %d, 16 REFUSED at node %d "+
		"with error 16 NOT_COORDINATOR", ownerNode.id, nonOwner.id)

	// ---- and nothing was written, read back through EVERY node
	//
	// OffsetFetch is deliberately served at every node, which is what makes
	// this readable from the node that just refused the write.
	for _, n := range nodes {
		got := committed(t, "readback", n.addr, cgroup, ctopic)[0]
		if got != 50 {
			t.Errorf("node %d reads the committed offset as %d after the refused commit, want 50", n.id, got)
		}
	}
	t.Logf("all %d nodes still read the committed offset as 50: the refused commit wrote nothing", len(nodes))
}

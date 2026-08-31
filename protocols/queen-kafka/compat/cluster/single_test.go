package cluster

import (
	"fmt"
	"testing"
	"time"
)

// SCENARIO 4 — THE SINGLE-NODE REGRESSION.
//
// The same suite body, against a facade whose cluster configuration is ABSENT.
// `QUEEN_KAFKA_NODE_ID` is the one and only opt-in, and with it unset the
// facade must behave exactly as it always has: ONE broker in Metadata, at node
// id 0, coordinating everything itself, refusing nothing for being a non-owner.
//
// This is the regression gate for the whole design, run in the same lane as the
// cluster scenarios so that a cluster change which breaks single mode is caught
// by the same command. `runMembers` is literally the function the cluster
// acceptance calls, with a different bootstrap list and nothing else.
func TestSingleNodeRegression(t *testing.T) {
	if singleAddr == "" {
		t.Skip("QUEEN_KAFKA_SINGLE is unset: no facade with the cluster config absent")
	}

	// ---- one broker, and it is node 0
	v := metadataView(t, "single", singleAddr, "")
	if got := v.ids(); len(got) != 1 || got[0] != singleNodeID {
		t.Fatalf("a facade with no QUEEN_KAFKA_NODE_ID lists brokers %v, want exactly [%d]", got, singleNodeID)
	}
	if got := v.brokers[singleNodeID]; got != singleAddr {
		t.Errorf("it advertises itself at %q, want %q", got, singleAddr)
	}
	if v.controller != singleNodeID {
		t.Errorf("controller_id is %d, want %d", v.controller, singleNodeID)
	}
	// The default cluster id, unchanged: an operator who sets nothing sees
	// nothing move. QUEEN_KAFKA_CLUSTER alone is refused at boot precisely so
	// that this value cannot drift on a single-node facade.
	if v.clusterID != "queen" {
		t.Errorf("cluster_id is %q, want %q", v.clusterID, "queen")
	}
	t.Logf("the unconfigured facade lists one broker: node %d at %s, cluster_id %q, controller %d",
		singleNodeID, v.brokers[singleNodeID], v.clusterID, v.controller)

	// ---- it coordinates everything itself, and refuses nothing for ownership
	group := newName(t, "g")
	c := findCoordinator(t, "single", singleAddr, group)
	if c.errCode != 0 || c.nodeID != singleNodeID || c.addr != singleAddr {
		t.Fatalf("FindCoordinator at the unconfigured facade: %s, want error 0, node %d at %s",
			c, singleNodeID, singleAddr)
	}
	t.Logf("FindCoordinator(%s) at the unconfigured facade: %s", group, c)

	if got := joinGroupErr(t, "single", singleAddr, newName(t, "g")); got == 16 || got == 15 {
		t.Fatalf("JoinGroup at the unconfigured facade answered %d: single mode must never refuse for "+
			"ownership, there is nothing to be a non-owner of", got)
	} else {
		t.Logf("JoinGroup at the unconfigured facade: error %d (not a routing refusal)", got)
	}

	// ---- and the whole group body behaves as it always has
	const perPartition = 26 // the same 208 records the cluster acceptance uses
	topic, total := seed(t, []string{singleAddr}, perPartition)
	mv := metadataView(t, "single", singleAddr, topic)
	for p := int32(0); p < partitions; p++ {
		if got := mv.leaders[p]; got != singleNodeID {
			t.Errorf("%s/%d is led by node %d, want %d", topic, p, got, singleNodeID)
		}
		if got := mv.epochs[p]; got != -1 {
			t.Errorf("%s/%d has leader epoch %d, want -1", topic, p, got)
		}
	}

	watch := watchOffsets(t, "single", singleAddr, group, topic, 250*time.Millisecond)
	boots := [][]string{{singleAddr}, {singleAddr}}
	names := []string{fmt.Sprintf("m1@%s", singleAddr), fmt.Sprintf("m2@%s", singleAddr)}
	ledger, clients := runMembers(t, group, topic, total, boots)
	assertOneGeneration(t, clients)
	samples, regressions, _ := watch.finish()
	for _, cl := range clients {
		cl.Close()
	}

	assertOneDelivery(t, ledger, total, names)
	t.Logf("the committed-offset sampler took %d samples during the single-node run", samples)
	for _, r := range regressions {
		t.Errorf("committed offset REGRESSION on the single-node facade: %s", r)
	}
	var sum int64
	for _, o := range committed(t, "single", singleAddr, group, topic) {
		if o > 0 {
			sum += o
		}
	}
	if sum != int64(total) {
		t.Errorf("committed offsets sum to %d after the single-node run, want %d", sum, total)
	} else {
		t.Logf("committed offsets sum to %d of %d produced", sum, total)
	}
}

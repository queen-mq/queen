package controller

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/queen-mq/queen/deploy/operator/internal/broker"
)

var t0 = time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)

func i64(v int64) *int64 { return &v }

// cluster is a cluster of `pods` pods where nodes 1..voters vote, node
// `leader` leads, every pod is Ready for an hour on the current template and
// every member is live and caught up.
func cluster(pods, voters int, leader int64) *Observed {
	size := resource.MustParse("50Gi")
	o := &Observed{
		Voters:          voters,
		StatefulSetPods: pods,
		TemplateSize:    size,
		Size:            size,
		Hash:            "new",
		Formed:          true,
		MinReady:        30 * time.Second,
		Now:             t0,
		Membership:      &broker.Membership{Source: "leader", Leader: i64(leader), Term: 3, PromoteMaxLag: 1000},
	}
	for i := 0; i < pods; i++ {
		o.Pods = append(o.Pods, PodState{
			Ordinal: i, Exists: true, Running: true, Ready: true, ReadySince: t0.Add(-time.Hour), Hash: "new",
			Health:      &broker.Health{Healthy: true},
			ClaimExists: true, ClaimUID: "claim-" + string(rune('a'+i)), ClaimRequest: size, ClaimCapacity: size,
		})
	}
	for i := 0; i < voters; i++ {
		member(o, nodeID(i), true, 0)
	}
	return o
}

// member adds node id to the membership, live, `lag` entries behind.
func member(o *Observed, id int64, voter bool, lag int64) {
	m := o.Membership
	if voter {
		m.Voters = append(m.Voters, id)
	} else {
		m.Learners = append(m.Learners, id)
	}
	m.Members = append(m.Members, broker.Member{NodeID: id, Voter: voter, Live: true, Lag: i64(lag), Matched: i64(5000 - lag)})
}

func removeMember(o *Observed, id int64) {
	m := o.Membership
	keep := func(ids []int64) []int64 {
		var out []int64
		for _, v := range ids {
			if v != id {
				out = append(out, v)
			}
		}
		return out
	}
	m.Voters, m.Learners = keep(m.Voters), keep(m.Learners)
	var members []broker.Member
	for _, mem := range m.Members {
		if mem.NodeID != id {
			members = append(members, mem)
		}
	}
	m.Members = members
}

func expect(t *testing.T, o *Observed, kind ActionKind, phase string, in string) Action {
	t.Helper()
	a := plan(o)
	if a.Kind != kind || a.Phase != phase || !strings.Contains(a.Message, in) {
		t.Fatalf("want kind %d in phase %s saying %q, got kind %d in phase %s saying %q", kind, phase, in, a.Kind, a.Phase, a.Message)
	}
	return a
}

func TestAClusterAtRestNeedsNothing(t *testing.T) {
	expect(t, cluster(3, 3, 2), Done, PhaseReady, "")
	expect(t, cluster(1, 1, 1), Done, PhaseReady, "")
}

func TestAClusterThatCannotBeReadIsLeftAlone(t *testing.T) {
	o := cluster(3, 3, 2)
	o.Membership, o.Formed = nil, false
	expect(t, o, Wait, PhaseForming, "elect a leader")
	// Once it had a membership, silence is not a cluster forming.
	o.Formed = true
	expect(t, o, Wait, PhaseDegraded, "lost its majority")

	// The leader's own restart, in the middle of an upgrade, is an election.
	o = cluster(3, 3, 2)
	o.Membership.Leader = nil
	o.LastPhase = PhaseUpgrading
	expect(t, o, Wait, PhaseUpgrading, "an election is under way")

	// Apply stopped on a node: nothing here is the operator's to repair,
	// and an old template must not make it restart anything.
	o = cluster(3, 3, 2)
	o.Pods[1].Hash = "old"
	_ = json.Unmarshal([]byte(`{"raft":{"apply":{"failure":{"index":9}}}}`), o.Pods[2].Health)
	expect(t, o, Wait, PhaseBlocked, "apply stopped on pod 2")

	o = cluster(3, 3, 2)
	o.Pods[1].Hash = "old"
	o.Membership.ChangeInFlight = true
	expect(t, o, Wait, PhaseScaling, "in flight")
}

// Three voters become five: the pods first, then each new node as a learner
// once it answers, then both promoted in one change, so the cluster never
// rests on four voters.
func TestVotersAreAddedAsLearnersAndPromotedTogether(t *testing.T) {
	o := cluster(3, 3, 1)
	o.Voters = 5
	a := expect(t, o, ScalePods, PhaseScaling, "5 voters")
	if a.Pods != 5 {
		t.Fatalf("pods: %d", a.Pods)
	}

	o = cluster(5, 3, 1)
	o.Voters = 5
	o.Pods[3].Running, o.Pods[3].Health, o.Pods[3].Ready = false, nil, false
	expect(t, o, Wait, PhaseScaling, "waiting for pod 3 to start")

	// A new pod serves 503 until it is added: it is asked as soon as it answers.
	o.Pods[3].Running, o.Pods[3].Health = true, &broker.Health{}
	a = expect(t, o, AddLearner, PhaseScaling, "node 4")
	if a.Ordinal != 3 || a.NodeIDs[0] != 4 {
		t.Fatalf("%+v", a)
	}

	member(o, 4, false, 20)
	o.Pods[4].Ready = false
	a = expect(t, o, AddLearner, PhaseScaling, "node 5")
	if a.Ordinal != 4 {
		t.Fatalf("%+v", a)
	}

	member(o, 5, false, 40_000)
	expect(t, o, Wait, PhaseScaling, "learner 5 is catching up (40000 entries behind)")

	o.Membership.Member(5).Lag = i64(12)
	a = expect(t, o, Promote, PhaseScaling, "nodes 4 and 5")
	if len(a.NodeIDs) != 2 {
		t.Fatalf("%+v", a)
	}
}

// Five voters become three: out of the membership first, the highest node
// first, and only then the pods. A node that leads hands leadership away
// before it is removed.
func TestVotersLeaveTheMembershipBeforeTheirPodsGo(t *testing.T) {
	o := cluster(5, 5, 2)
	o.Voters = 3
	a := expect(t, o, RemoveMember, PhaseScaling, "node 5")
	if a.NodeIDs[0] != 5 {
		t.Fatalf("%+v", a)
	}

	// Its pod goes right after it: outside the membership it still takes
	// requests it cannot commit.
	removeMember(o, 5)
	a = expect(t, o, ScalePods, PhaseScaling, "above node 4")
	if a.Pods != 4 {
		t.Fatalf("%+v", a)
	}
	o.StatefulSetPods, o.Pods = 4, o.Pods[:4]

	o.Membership.Leader = i64(4)
	a = expect(t, o, RestartPod, PhaseScaling, "hands leadership away")
	if a.Ordinal != 3 || !a.Disruptive {
		t.Fatalf("%+v", a)
	}
	o.Pods[3].Terminating = true
	expect(t, o, Wait, PhaseScaling, "waiting for another leader")

	o.Pods[3].Terminating = false
	o.Membership.Leader = i64(1)
	expect(t, o, RemoveMember, PhaseScaling, "node 4")

	removeMember(o, 4)
	a = expect(t, o, ScalePods, PhaseScaling, "above node 3")
	if a.Pods != 3 {
		t.Fatalf("%+v", a)
	}
	o.StatefulSetPods, o.Pods = 3, o.Pods[:3]
	expect(t, o, Done, PhaseReady, "")
}

// Three voters become one, through two: each node leaves the membership
// while it still runs. Stopping node 2 first would leave voters {1, 2} with
// one alive, which cannot commit anything, its own removal included.
func TestTwoVotersNeverLoseOneBeforeItLeftTheMembership(t *testing.T) {
	o := cluster(3, 3, 1)
	o.Voters = 1
	expect(t, o, RemoveMember, PhaseScaling, "node 3")
	removeMember(o, 3)
	expect(t, o, ScalePods, PhaseScaling, "above node 2")
	o.StatefulSetPods, o.Pods = 2, o.Pods[:2]
	a := expect(t, o, RemoveMember, PhaseScaling, "node 2")
	if a.Kind == ScalePods {
		t.Fatal("pod 1 must not go while node 2 is a voter")
	}
	removeMember(o, 2)
	expect(t, o, ScalePods, PhaseScaling, "above node 1")
}

// A new template restarts one pod at a time: the highest ordinal that does
// not lead first, the leader last, and never while another voter is not in
// place.
func TestPodsRestartOneAtATimeTheLeaderLast(t *testing.T) {
	o := cluster(3, 3, 3)
	for i := range o.Pods {
		o.Pods[i].Hash = "old"
	}
	a := expect(t, o, RestartPod, PhaseUpgrading, "pod 1")
	if !a.Disruptive {
		t.Fatal("a restart must be behind a write that commits")
	}

	// Pod 1 is back on the new template, Ready for 10 s: not long enough.
	o.Pods[1].Hash, o.Pods[1].ReadySince = "new", t0.Add(-10*time.Second)
	expect(t, o, Wait, PhaseUpgrading, "pod 1 has been Ready for less than 30s")

	o.Pods[1].ReadySince = t0.Add(-31 * time.Second)
	expect(t, o, RestartPod, PhaseUpgrading, "pod 0")

	o.Pods[0].Hash = "new"
	expect(t, o, RestartPod, PhaseUpgrading, "pod 2")

	o.Pods[2].Hash = "new"
	expect(t, o, Done, PhaseReady, "")
}

func TestNoPodIsStoppedWhileAnotherVoterIsNotInPlace(t *testing.T) {
	old := func() *Observed {
		o := cluster(3, 3, 3)
		o.Pods[0].Hash, o.Pods[1].Hash = "old", "old"
		return o
	}

	o := old()
	o.Pods[2].Ready = false
	expect(t, o, Wait, PhaseUpgrading, "pod 2 is not Ready")

	o = old()
	o.Membership.Member(1).Live = false
	expect(t, o, Wait, PhaseUpgrading, "has not heard from node 1")

	o = old()
	o.Membership.Member(3).Lag = i64(50_000)
	expect(t, o, Wait, PhaseUpgrading, "node 3 is behind the leader (50000 entries behind)")

	// A voter whose volume was wiped under its id: live, a few hundred
	// entries behind, and never moving. Stopping pod 1 would leave one
	// good voter of three.
	o = old()
	o.Membership.Member(1).Lag = i64(334)
	o.Stalled = map[int64]bool{1: true}
	expect(t, o, Wait, PhaseUpgrading, "node 1 is behind the leader and does not catch up")

	o = old()
	o.Pods[2].Terminating = true
	expect(t, o, Wait, PhaseUpgrading, "pod 2 is not running")
}

// A pod that is not Ready on an old template goes first, whatever its
// ordinal: it serves nobody, and the new template may be what repairs it. A
// rolling update that waits for it to be Ready first never ends.
func TestABrokenPodOnAnOldTemplateIsRestartedFirst(t *testing.T) {
	o := cluster(3, 3, 3)
	for i := range o.Pods {
		o.Pods[i].Hash = "old"
	}
	o.Pods[0].Ready = false
	o.Membership.Member(1).Live = false
	a := expect(t, o, RestartPod, PhaseUpgrading, "pod 0")
	if a.Ordinal != 0 {
		t.Fatalf("%+v", a)
	}
	// Not while a second one is down as well.
	o.Pods[1].Ready = false
	expect(t, o, Wait, PhaseUpgrading, "is not Ready")
}

func TestAPodDownOnACurrentTemplateIsReportedNotTouched(t *testing.T) {
	o := cluster(3, 3, 3)
	o.Pods[1].Ready = false
	expect(t, o, Wait, PhaseDegraded, "pod 1 is not Ready")

	// The last pod of an upgrade coming back is still the upgrade.
	o.LastPhase = PhaseUpgrading
	expect(t, o, Wait, PhaseUpgrading, "pod 1 is not Ready")
	// A pod that only just became Ready is no reason to call a cluster degraded.
	o = cluster(3, 3, 3)
	o.Pods[1].ReadySince = t0.Add(-2 * time.Second)
	expect(t, o, Done, PhaseReady, "")

	o = cluster(3, 3, 3)
	o.Membership.Member(2).Lag = i64(700)
	o.Stalled = map[int64]bool{2: true}
	expect(t, o, Wait, PhaseDegraded, "has to be replaced")
}

// A replacement: out of the membership, then the volume, then back in as a
// new voter. It is never wiped while it is still a member, and a volume that
// is already the new one is never wiped again.
func TestANodeIsReplacedInTheOrderThatKeepsTheClusterWhole(t *testing.T) {
	replace := func(leader int64) *Observed {
		o := cluster(3, 3, leader)
		o.ReplacePod, o.ReplaceOrdinal = "queen-1", 1
		return o
	}

	o := replace(3)
	expect(t, o, StartReplace, PhaseReplacing, "replacing pod 1")

	o.ReplaceClaimUID = o.Pods[1].ClaimUID
	a := expect(t, o, RemoveMember, PhaseReplacing, "node 2")
	if a.NodeIDs[0] != 2 {
		t.Fatalf("%+v", a)
	}

	// The node to replace leads: it hands leadership away first.
	l := replace(2)
	l.ReplaceClaimUID = l.Pods[1].ClaimUID
	expect(t, l, RestartPod, PhaseReplacing, "hands leadership away")

	removeMember(o, 2)
	expect(t, o, WipePod, PhaseReplacing, "volume of pod 1")

	// The claim is marked for deletion and the old pod still runs on it.
	o.Pods[1].ClaimTerminating = true
	expect(t, o, WipePod, PhaseReplacing, "volume of pod 1")

	// The old pod is gone; the next one waits, unscheduled, for a new claim.
	o.Pods[1].Running, o.Pods[1].Ready, o.Pods[1].Health = false, false, nil
	expect(t, o, Wait, PhaseReplacing, "old volume of pod 1")

	// The new claim: from here it is a new voter joining.
	o.Pods[1].ClaimUID, o.Pods[1].ClaimTerminating = "claim-new", false
	expect(t, o, Wait, PhaseScaling, "waiting for pod 1 to start")

	o.Pods[1].Running, o.Pods[1].Health = true, &broker.Health{}
	expect(t, o, AddLearner, PhaseScaling, "node 2")

	member(o, 2, false, 3)
	expect(t, o, Promote, PhaseScaling, "node 2")

	removeMember(o, 2)
	member(o, 2, true, 0)
	o.Pods[1].Ready, o.Pods[1].ReadySince = true, t0
	expect(t, o, FinishReplace, PhaseReplacing, "voter again")

	// A pod that is not a voter of this cluster is not replaced.
	bad := replace(3)
	bad.ReplacePod, bad.ReplaceOrdinal = "queen-7", 7
	expect(t, bad, Wait, PhaseBlocked, "is not a voter of this cluster")
	bad.ReplacePod, bad.ReplaceOrdinal = "other", -1
	expect(t, bad, Wait, PhaseBlocked, "is not a voter of this cluster")
}

// Volumes grow claim by claim, a pod restarts only when its file system waits
// for it, and the StatefulSet takes the new size last. They never shrink.
func TestVolumesGrowBeforeTheStatefulSetTakesTheNewSize(t *testing.T) {
	bigger := resource.MustParse("100Gi")
	o := cluster(3, 3, 3)
	o.Size = bigger
	a := expect(t, o, ExpandClaim, PhaseResizing, "pod 0 to 100Gi")
	if a.Ordinal != 0 {
		t.Fatalf("%+v", a)
	}
	for i := range o.Pods {
		o.Pods[i].ClaimRequest = bigger
	}
	expect(t, o, Wait, PhaseResizing, "volume of pod 0 is growing")

	o.Pods[0].ClaimCapacity = bigger
	o.Pods[1].ClaimNeedsRestart = true
	a = expect(t, o, RestartPod, PhaseResizing, "pod 1 to finish growing")
	if !a.Disruptive {
		t.Fatal("a restart must be behind a write that commits")
	}
	o.Pods[2].Ready = false
	expect(t, o, Wait, PhaseResizing, "pod 2 is not Ready")

	o.Pods[2].Ready = true
	o.Pods[1].ClaimCapacity, o.Pods[2].ClaimCapacity = bigger, bigger
	expect(t, o, RecreateStatefulSet, PhaseResizing, "100Gi")

	o.TemplateSize = bigger
	expect(t, o, Done, PhaseReady, "")

	// A claim that was just created has no capacity yet: it is not growing.
	fresh := cluster(3, 3, 3)
	fresh.Pods[0].ClaimCapacity = resource.Quantity{}
	expect(t, fresh, Done, PhaseReady, "")

	o.Size = resource.MustParse("20Gi")
	expect(t, o, Wait, PhaseBlocked, "cannot shrink")
}

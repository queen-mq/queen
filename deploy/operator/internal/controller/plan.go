package controller

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/queen-mq/queen/deploy/operator/internal/broker"
)

// The phases a QueenCluster reports.
const (
	PhaseForming   = "Forming"
	PhaseReady     = "Ready"
	PhaseScaling   = "Scaling"
	PhaseUpgrading = "Upgrading"
	PhaseReplacing = "Replacing"
	PhaseResizing  = "Resizing"
	// A pod is down or a voter is behind: nothing for the operator to do.
	PhaseDegraded = "Degraded"
	// Something only a person can decide.
	PhaseBlocked = "Blocked"
)

// PodState is one pod of the StatefulSet as the operator last saw it, with
// its volume claim and what the broker in it answers.
type PodState struct {
	Ordinal     int
	Exists      bool
	Terminating bool
	// The container has started: the broker may be asked.
	Running bool
	Ready   bool
	// Since when the pod has been Ready; zero while it is not.
	ReadySince time.Time
	// The config hash of the template the pod was created from.
	Hash string
	// What /health answered; nil when the pod did not answer.
	Health *broker.Health

	ClaimExists      bool
	ClaimUID         string
	ClaimTerminating bool
	// What the claim asks for, and what the volume holds.
	ClaimRequest  resource.Quantity
	ClaimCapacity resource.Quantity
	// The volume grew and the file system waits for the pod to restart.
	ClaimNeedsRestart bool
}

// Observed is everything a decision reads.
type Observed struct {
	// Voters wanted: node ids 1..Voters, pods 0..Voters-1.
	Voters int
	// The pods the StatefulSet asks for now.
	StatefulSetPods int
	// The size in the StatefulSet's claim template.
	TemplateSize resource.Quantity
	// The size wanted.
	Size resource.Quantity
	// The config hash of the pod template wanted.
	Hash string
	Pods []PodState
	// The membership, when a node could give the leader's own view of it.
	Membership *broker.Membership
	// The voters that are behind the leader and whose position has not
	// moved for a while: a node whose log went backwards looks like this.
	Stalled map[int64]bool
	// The cluster has had a membership before: it is not forming any more.
	Formed bool
	// The phase reported last.
	LastPhase string
	// The pod asked to be replaced ("" for none), and the claim the
	// replacement started from ("" before it started).
	ReplacePod      string
	ReplaceOrdinal  int
	ReplaceClaimUID string
	// How long a pod must have been Ready before another may be stopped.
	MinReady time.Duration
	Now      time.Time
}

type ActionKind int

const (
	// Nothing to do: the cluster is what was asked for.
	Done ActionKind = iota
	// Nothing can be done now: look again shortly.
	Wait
	// Set the number of pods of the StatefulSet.
	ScalePods
	// Add a node as a learner.
	AddLearner
	// Make learners voters, in one change.
	Promote
	// Remove a node from the membership.
	RemoveMember
	// Delete a pod, so that it restarts with the current template or hands
	// its leadership away. The cluster must commit first.
	RestartPod
	// Record the claim a replacement starts from.
	StartReplace
	// Delete a pod's claim and the pod: its replacement starts empty.
	WipePod
	// A replacement finished: forget the request.
	FinishReplace
	// Ask for more storage on a pod's claim.
	ExpandClaim
	// Delete the StatefulSet and leave its pods, so that it is created again
	// with a new claim template.
	RecreateStatefulSet
)

// Reason names the step in the events of the QueenCluster.
func (k ActionKind) Reason() string {
	switch k {
	case ScalePods:
		return "ScalePods"
	case AddLearner:
		return "AddLearner"
	case Promote:
		return "Promote"
	case RemoveMember:
		return "RemoveMember"
	case RestartPod:
		return "RestartPod"
	case StartReplace:
		return "StartReplace"
	case WipePod:
		return "WipePod"
	case FinishReplace:
		return "FinishReplace"
	case ExpandClaim:
		return "ExpandClaim"
	case RecreateStatefulSet:
		return "RecreateStatefulSet"
	}
	return "Wait"
}

// Action is the one step the operator takes next.
type Action struct {
	Kind    ActionKind
	Phase   string
	Message string
	Ordinal int
	NodeIDs []int64
	Pods    int
	// The step stops a pod: a write must commit through another pod first.
	Disruptive bool
}

func wait(phase, format string, args ...any) Action {
	return Action{Kind: Wait, Phase: phase, Message: fmt.Sprintf(format, args...)}
}

// ongoing is the phase of the change that was under way at the last look, or
// `otherwise` when the cluster was at rest.
func (o *Observed) ongoing(otherwise string) string {
	switch o.LastPhase {
	case PhaseUpgrading, PhaseScaling, PhaseReplacing, PhaseResizing, PhaseForming:
		return o.LastPhase
	}
	return otherwise
}

func (o *Observed) pod(ordinal int) *PodState {
	if ordinal >= 0 && ordinal < len(o.Pods) {
		return &o.Pods[ordinal]
	}
	return nil
}

// plan decides the next step from what was observed. It reads nothing else
// and changes nothing, which is what makes every decision testable.
//
// The order is the order of safety: a cluster that cannot be read is left
// alone; a replacement that started is finished; missing voters are added
// before extra ones are removed; volumes grow before pods restart; and pods
// restart last, one at a time, the leader after the others.
func plan(o *Observed) Action {
	m := o.Membership
	if m == nil || m.Leader == nil {
		if !o.Formed {
			return wait(PhaseForming, "waiting for the pods to start and elect a leader")
		}
		// An election in the middle of a change is part of the change; at
		// rest it is a cluster to look at. Nothing is done either way.
		return wait(o.ongoing(PhaseDegraded), "no pod answers with a leader's view of the membership: an election is under way, or the cluster lost its majority")
	}
	for i := range o.Pods {
		if h := o.Pods[i].Health; h != nil && h.ApplyFailed() {
			return wait(PhaseBlocked, "apply stopped on pod %d: this needs a person, see the recovery guide", o.Pods[i].Ordinal)
		}
	}
	if m.ChangeInFlight || len(m.Joint) > 0 {
		return wait(PhaseScaling, "a membership change is in flight")
	}

	if o.ReplacePod != "" {
		if a, decided := planReplace(o); decided {
			return a
		}
	}
	if a, decided := planMembers(o); decided {
		return a
	}
	if o.ReplacePod != "" {
		// The node is a voter again: the replacement is over.
		return Action{Kind: FinishReplace, Phase: PhaseReplacing, Ordinal: o.ReplaceOrdinal,
			Message: fmt.Sprintf("pod %d is a voter again", o.ReplaceOrdinal)}
	}
	if a, decided := planStorage(o); decided {
		return a
	}
	if a, decided := planRestarts(o); decided {
		return a
	}
	// What would hold a restart back also says the cluster is not whole,
	// except a pod that only became Ready a moment ago.
	settled := *o
	settled.MinReady = 0
	if why := notSafeToStop(&settled, -1); why != "" {
		// The last pod of a change coming back is still that change.
		return wait(o.ongoing(PhaseDegraded), "%s", why)
	}
	return Action{Kind: Done, Phase: PhaseReady, Message: ""}
}

// planReplace removes the node to replace from the membership and wipes its
// volume. Once the pod is back empty, planMembers adds it like any new voter.
func planReplace(o *Observed) (Action, bool) {
	m := o.Membership
	i := o.ReplaceOrdinal
	id := nodeID(i)
	p := o.pod(i)
	if i < 0 || i >= o.Voters || p == nil {
		return wait(PhaseBlocked, "the pod asked to be replaced, %q, is not a voter of this cluster", o.ReplacePod), true
	}
	if o.ReplaceClaimUID == "" {
		if !p.ClaimExists {
			return wait(PhaseReplacing, "pod %d has no volume claim to replace yet", i), true
		}
		return Action{Kind: StartReplace, Phase: PhaseReplacing, Ordinal: i,
			Message: fmt.Sprintf("replacing pod %d", i)}, true
	}
	if m.IsMember(id) && p.ClaimExists && p.ClaimUID == o.ReplaceClaimUID {
		// Still on the volume to replace: out of the membership first. A
		// voter that comes back empty under its id is never repaired.
		if *m.Leader == id {
			return Action{Kind: RestartPod, Phase: PhaseReplacing, Ordinal: i, Disruptive: true,
				Message: fmt.Sprintf("pod %d leads: stopping it once so it hands leadership away before it is removed", i)}, true
		}
		return Action{Kind: RemoveMember, Phase: PhaseReplacing, NodeIDs: []int64{id}, Ordinal: i,
			Message: fmt.Sprintf("removing node %d from the membership", id)}, true
	}
	if p.ClaimExists && p.ClaimUID == o.ReplaceClaimUID {
		// A claim stays, marked for deletion, until the pod that mounts it
		// is gone. The pod the StatefulSet creates next waits unscheduled
		// for a new claim: that one must not be deleted again.
		oldPodStillThere := p.Exists && !p.Terminating && p.Running
		if !p.ClaimTerminating || oldPodStillThere {
			return Action{Kind: WipePod, Phase: PhaseReplacing, Ordinal: i,
				Message: fmt.Sprintf("deleting the volume of pod %d", i)}, true
		}
		return wait(PhaseReplacing, "waiting for the old volume of pod %d to go", i), true
	}
	// The old volume is gone: the rest is adding a new voter.
	return Action{}, false
}

// planMembers brings the membership to voters 1..Voters: missing ones are
// added as learners and promoted together, then the extra ones are removed,
// highest first, each one's pod right after it.
//
// A node leaves the membership while its pod still runs, never the other way
// round: with two voters, stopping one first would leave a membership that
// cannot commit its own removal. The pod then goes at once, because a node
// outside the membership still takes requests it can no longer commit.
func planMembers(o *Observed) (Action, bool) {
	m := o.Membership
	// A pod above every member and above the voters wanted serves nobody.
	top := int64(o.Voters)
	for _, id := range append(append([]int64{}, m.Voters...), m.Learners...) {
		if id > top {
			top = id
		}
	}
	if int64(o.StatefulSetPods) > top {
		return Action{Kind: ScalePods, Phase: PhaseScaling, Pods: int(top),
			Message: fmt.Sprintf("removing the pods above node %d", top)}, true
	}
	var missing, learners []int64
	for i := 0; i < o.Voters; i++ {
		id := nodeID(i)
		switch {
		case m.IsVoter(id):
		case m.IsLearner(id):
			learners = append(learners, id)
		default:
			missing = append(missing, id)
		}
	}

	if len(missing) > 0 || len(learners) > 0 {
		if o.StatefulSetPods < o.Voters {
			return Action{Kind: ScalePods, Phase: PhaseScaling, Pods: o.Voters,
				Message: fmt.Sprintf("adding pods for %d voters", o.Voters)}, true
		}
		for _, id := range missing {
			i := int(id - 1)
			p := o.pod(i)
			if p == nil || !p.Exists || p.Terminating || !p.Running || p.Health == nil {
				return wait(PhaseScaling, "waiting for pod %d to start before node %d is added", i, id), true
			}
			return Action{Kind: AddLearner, Phase: PhaseScaling, Ordinal: i, NodeIDs: []int64{id},
				Message: fmt.Sprintf("adding node %d as a learner", id)}, true
		}
		for _, id := range learners {
			mem := m.Member(id)
			if mem == nil || !mem.Live || mem.Lag == nil || *mem.Lag > m.PromoteMaxLag {
				return wait(PhaseScaling, "learner %d is catching up%s", id, lagText(mem)), true
			}
		}
		return Action{Kind: Promote, Phase: PhaseScaling, NodeIDs: learners,
			Message: fmt.Sprintf("promoting %s to voters", idsText(learners))}, true
	}

	var extra []int64
	for _, id := range append(append([]int64{}, m.Voters...), m.Learners...) {
		if id > int64(o.Voters) {
			extra = append(extra, id)
		}
	}
	if len(extra) > 0 {
		sort.Slice(extra, func(a, b int) bool { return extra[a] > extra[b] })
		id := extra[0]
		i := int(id - 1)
		if *m.Leader == id {
			p := o.pod(i)
			if p == nil || !p.Exists || p.Terminating {
				return wait(PhaseScaling, "node %d leads and is leaving: waiting for another leader", id), true
			}
			return Action{Kind: RestartPod, Phase: PhaseScaling, Ordinal: i, Disruptive: true,
				Message: fmt.Sprintf("node %d leads: stopping its pod once so it hands leadership away before it is removed", id)}, true
		}
		return Action{Kind: RemoveMember, Phase: PhaseScaling, NodeIDs: []int64{id}, Ordinal: i,
			Message: fmt.Sprintf("removing node %d from the membership", id)}, true
	}
	return Action{}, false
}

// planStorage grows the volumes: every claim first, then a restart of the
// pods whose file system waits for one, then the StatefulSet's own template.
func planStorage(o *Observed) (Action, bool) {
	for i := range o.Pods {
		p := &o.Pods[i]
		if p.ClaimExists && p.ClaimRequest.Cmp(o.Size) > 0 {
			return wait(PhaseBlocked, "storage.size (%s) is below the claim of pod %d (%s): a volume cannot shrink",
				o.Size.String(), p.Ordinal, p.ClaimRequest.String()), true
		}
	}
	for i := range o.Pods {
		p := &o.Pods[i]
		if p.ClaimExists && !p.ClaimTerminating && p.ClaimRequest.Cmp(o.Size) < 0 {
			return Action{Kind: ExpandClaim, Phase: PhaseResizing, Ordinal: p.Ordinal,
				Message: fmt.Sprintf("growing the volume of pod %d to %s", p.Ordinal, o.Size.String())}, true
		}
	}
	for i := range o.Pods {
		p := &o.Pods[i]
		// A claim with no capacity yet is a new one waiting for its volume.
		if !p.ClaimExists || p.ClaimCapacity.IsZero() || p.ClaimCapacity.Cmp(o.Size) >= 0 {
			continue
		}
		if !p.ClaimNeedsRestart {
			return wait(PhaseResizing, "the volume of pod %d is growing to %s", p.Ordinal, o.Size.String()), true
		}
		if why := notSafeToStop(o, p.Ordinal); why != "" {
			return wait(PhaseResizing, "pod %d must restart to finish growing its volume: %s", p.Ordinal, why), true
		}
		return Action{Kind: RestartPod, Phase: PhaseResizing, Ordinal: p.Ordinal, Disruptive: true,
			Message: fmt.Sprintf("restarting pod %d to finish growing its volume", p.Ordinal)}, true
	}
	if o.TemplateSize.Cmp(o.Size) != 0 {
		return Action{Kind: RecreateStatefulSet, Phase: PhaseResizing,
			Message: fmt.Sprintf("recreating the StatefulSet with %s volumes; its pods keep running", o.Size.String())}, true
	}
	return Action{}, false
}

// planRestarts restarts the pods that run an old template, one at a time: a
// pod that is not Ready first (it serves nobody, and the new template may be
// what repairs it), then the others from the highest ordinal, the leader last.
func planRestarts(o *Observed) (Action, bool) {
	leader := int(*o.Membership.Leader - 1)
	target, broken, leads := -1, -1, false
	for i := len(o.Pods) - 1; i >= 0; i-- {
		p := &o.Pods[i]
		if !p.Exists || p.Terminating || p.Hash == o.Hash {
			continue
		}
		if !p.Ready && broken == -1 {
			broken = p.Ordinal
		}
		if p.Ordinal == leader {
			leads = true
		} else if target == -1 {
			target = p.Ordinal
		}
	}
	switch {
	case broken != -1:
		target = broken
	case target == -1 && leads:
		target = leader
	case target == -1:
		return Action{}, false
	}
	if why := notSafeToStop(o, target); why != "" {
		return wait(PhaseUpgrading, "pod %d is next: %s", target, why), true
	}
	return Action{Kind: RestartPod, Phase: PhaseUpgrading, Ordinal: target, Disruptive: true,
		Message: fmt.Sprintf("restarting pod %d", target)}, true
}

// notSafeToStop says why stopping pod `target` now could cost the cluster its
// majority, or "" when every other voter is in place: each pod Ready for the
// whole MinReady, and each voter live and caught up as the leader sees it.
// The pod to stop is not asked to be healthy itself. With no target (-1) it
// says what is missing for the whole cluster to be in place.
func notSafeToStop(o *Observed, target int) string {
	m := o.Membership
	for i := range o.Pods {
		p := &o.Pods[i]
		if i >= o.Voters || i == target {
			continue
		}
		switch {
		case !p.Exists || p.Terminating:
			return fmt.Sprintf("pod %d is not running", p.Ordinal)
		case !p.Ready:
			return fmt.Sprintf("pod %d is not Ready", p.Ordinal)
		case o.Now.Sub(p.ReadySince) < o.MinReady:
			return fmt.Sprintf("pod %d has been Ready for less than %s", p.Ordinal, o.MinReady)
		}
	}
	for _, id := range m.Voters {
		if target >= 0 && id == nodeID(target) {
			continue
		}
		mem := m.Member(id)
		if mem == nil || !mem.Live {
			return fmt.Sprintf("the leader has not heard from node %d", id)
		}
		if o.Stalled[id] {
			return fmt.Sprintf("node %d is behind the leader and does not catch up%s: if its volume was wiped or put back from a copy, it has to be replaced", id, lagText(mem))
		}
		if mem.Lag == nil || *mem.Lag > m.PromoteMaxLag {
			return fmt.Sprintf("node %d is behind the leader%s", id, lagText(mem))
		}
	}
	return ""
}

func lagText(m *broker.Member) string {
	if m == nil || m.Lag == nil {
		return ""
	}
	return fmt.Sprintf(" (%d entries behind)", *m.Lag)
}

func idsText(ids []int64) string {
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = fmt.Sprintf("%d", id)
	}
	if len(parts) == 1 {
		return "node " + parts[0]
	}
	return "nodes " + strings.Join(parts, " and ")
}

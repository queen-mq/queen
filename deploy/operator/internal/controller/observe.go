package controller

import (
	"context"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	queenv1 "github.com/queen-mq/queen/deploy/operator/api/v1alpha1"
	"github.com/queen-mq/queen/deploy/operator/internal/broker"
)

// How long one question to one broker may take.
const brokerTimeout = 3 * time.Second

// A voter whose position has not moved for this long while it is behind is
// not catching up: stopping another pod would leave too few good voters.
const stalledAfter = 10 * time.Second

// observation is what one reconcile read from the cluster.
type observation struct {
	sts  *appsv1.StatefulSet
	pods map[int]*corev1.Pod
	// The pod that gave the leader's view of the membership.
	via string
	o   Observed
}

// progress remembers where the leader last saw each member, to tell a voter
// that is catching up from one that is stuck.
type progress struct {
	mu   sync.Mutex
	seen map[types.NamespacedName]map[int64]position
}

type position struct {
	matched int64
	since   time.Time
}

// stalled notes the members' positions and returns the voters that are behind
// and have not moved for stalledAfter.
func (p *progress) stalled(key types.NamespacedName, m *broker.Membership, now time.Time) map[int64]bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.seen == nil {
		p.seen = map[types.NamespacedName]map[int64]position{}
	}
	prev := p.seen[key]
	next := map[int64]position{}
	out := map[int64]bool{}
	for _, mem := range m.Members {
		matched := int64(-1)
		if mem.Matched != nil {
			matched = *mem.Matched
		}
		pos := position{matched: matched, since: now}
		if old, ok := prev[mem.NodeID]; ok && old.matched == matched {
			pos.since = old.since
		}
		next[mem.NodeID] = pos
		behind := mem.Lag == nil || *mem.Lag > 0
		if mem.Voter && behind && now.Sub(pos.since) >= stalledAfter {
			out[mem.NodeID] = true
		}
	}
	p.seen[key] = next
	return out
}

func (p *progress) forget(key types.NamespacedName) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.seen, key)
}

func (r *QueenClusterReconciler) observe(ctx context.Context, qc *queenv1.QueenCluster, sts *appsv1.StatefulSet, bc *broker.Client) (*observation, error) {
	now := r.now()
	key := client.ObjectKeyFromObject(qc)
	pods := int(*sts.Spec.Replicas)
	obs := &observation{sts: sts, pods: map[int]*corev1.Pod{}}

	var list corev1.PodList
	if err := r.List(ctx, &list, client.InNamespace(qc.Namespace), client.MatchingLabels(selectorLabels(qc))); err != nil {
		return nil, err
	}
	for i := range list.Items {
		if n, ok := ordinalOf(qc, list.Items[i].Name); ok {
			obs.pods[n] = &list.Items[i]
		}
	}

	o := Observed{
		Voters:          int(qc.Spec.Replicas),
		StatefulSetPods: pods,
		Size:            qc.Spec.Storage.Size,
		Hash:            podTemplate(qc, pods).Annotations[hashAnnotation],
		Formed:          len(qc.Status.Voters) > 0,
		LastPhase:       qc.Status.Phase,
		MinReady:        time.Duration(qc.Spec.MinReadySeconds) * time.Second,
		Now:             now,
	}
	if len(sts.Spec.VolumeClaimTemplates) > 0 {
		o.TemplateSize = sts.Spec.VolumeClaimTemplates[0].Spec.Resources.Requests[corev1.ResourceStorage]
	}

	o.Pods = make([]PodState, pods)
	var wg sync.WaitGroup
	for i := 0; i < pods; i++ {
		ps := &o.Pods[i]
		ps.Ordinal = i
		if pod := obs.pods[i]; pod != nil {
			ps.Exists = true
			ps.Terminating = pod.DeletionTimestamp != nil
			ps.Hash = pod.Annotations[hashAnnotation]
			ps.Running = podRunning(pod)
			if since, ok := readySince(pod); ok {
				ps.Ready = true
				ps.ReadySince = since
			}
			if ps.Running && !ps.Terminating {
				wg.Add(1)
				go func(name string) {
					defer wg.Done()
					hctx, cancel := context.WithTimeout(ctx, brokerTimeout)
					defer cancel()
					if h, err := bc.Health(hctx, name); err == nil && h.Raft.Role != "" {
						ps.Health = h
					}
				}(pod.Name)
			}
		}
		var pvc corev1.PersistentVolumeClaim
		err := r.Get(ctx, types.NamespacedName{Namespace: qc.Namespace, Name: claimName(qc, i)}, &pvc)
		switch {
		case err == nil:
			ps.ClaimExists = true
			ps.ClaimUID = string(pvc.UID)
			ps.ClaimTerminating = pvc.DeletionTimestamp != nil
			ps.ClaimRequest = pvc.Spec.Resources.Requests[corev1.ResourceStorage]
			ps.ClaimCapacity = pvc.Status.Capacity[corev1.ResourceStorage]
			if ps.ClaimCapacity.IsZero() {
				// Not bound yet: nothing to grow.
				ps.ClaimCapacity = *resource.NewQuantity(0, resource.BinarySI)
			}
			for _, c := range pvc.Status.Conditions {
				if c.Type == corev1.PersistentVolumeClaimFileSystemResizePending && c.Status == corev1.ConditionTrue {
					ps.ClaimNeedsRestart = true
				}
			}
		case !apierrors.IsNotFound(err):
			return nil, err
		}
	}
	wg.Wait()

	// The membership, from the first pod that can give the leader's own view.
	for i := 0; i < pods && o.Membership == nil; i++ {
		ps := &o.Pods[i]
		if ps.Health == nil {
			continue
		}
		mctx, cancel := context.WithTimeout(ctx, brokerTimeout)
		m, err := bc.Membership(mctx, podName(qc, i))
		cancel()
		if err == nil && m.Source == "leader" && m.Leader != nil {
			o.Membership = m
			obs.via = podName(qc, i)
		}
	}
	if o.Membership != nil {
		o.Stalled = r.progress.stalled(key, o.Membership, now)
	}

	if pod := qc.Annotations[queenv1.ReplaceAnnotation]; pod != "" {
		o.ReplacePod = pod
		o.ReplaceOrdinal = -1
		if n, ok := ordinalOf(qc, pod); ok {
			o.ReplaceOrdinal = n
		}
		if qc.Status.Replace != nil && qc.Status.Replace.Pod == pod {
			o.ReplaceClaimUID = qc.Status.Replace.ClaimUID
		}
	}
	obs.o = o
	return obs, nil
}

func podRunning(pod *corev1.Pod) bool {
	if pod.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, c := range pod.Status.ContainerStatuses {
		if c.Name == containerName {
			return c.State.Running != nil
		}
	}
	return false
}

func readySince(pod *corev1.Pod) (time.Time, bool) {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
			return c.LastTransitionTime.Time, true
		}
	}
	return time.Time{}, false
}

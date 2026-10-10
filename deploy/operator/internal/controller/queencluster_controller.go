// Package controller reconciles a QueenCluster: it keeps the Services, the
// disruption budget and the StatefulSet in place, and does through the
// broker's membership API what a StatefulSet cannot: change the voters,
// replace a node, grow the volumes, and restart the pods one at a time, the
// leader last, each behind a write that commits.
//
// What it leaves to a person is everything that can lose data: a forced
// recovery, an apply skip, two clusters where there was one. It reports the
// state and stops.
package controller

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	queenv1 "github.com/queen-mq/queen/deploy/operator/api/v1alpha1"
	"github.com/queen-mq/queen/deploy/operator/internal/broker"
)

const (
	// After a step, while waiting for something, and once the cluster is
	// what was asked for.
	stepRequeue  = 500 * time.Millisecond
	busyRequeue  = 5 * time.Second
	readyRequeue = 30 * time.Second
	// How often a cluster at rest is asked to commit a write, for its status.
	probeEvery = time.Minute
	// How long a write may take to commit before the cluster counts as not
	// committing.
	probeTimeout = 5 * time.Second
	// A membership change waits for its commit on the leader.
	changeTimeout = 40 * time.Second
)

type QueenClusterReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
	// How the broker port of a cluster's pods is reached.
	Transport func(qc *queenv1.QueenCluster) broker.Transport
	// The clock, for tests.
	Now func() time.Time

	progress progress
}

func (r *QueenClusterReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// +kubebuilder:rbac:groups=queenmq.com,resources=queenclusters,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=queenmq.com,resources=queenclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;update;patch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;create;update;patch;delete

func (r *QueenClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var qc queenv1.QueenCluster
	if err := r.Get(ctx, req.NamespacedName, &qc); err != nil {
		if apierrors.IsNotFound(err) {
			r.progress.forget(req.NamespacedName)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if qc.DeletionTimestamp != nil {
		// The objects it owns go with it; the volume claims stay.
		return ctrl.Result{}, nil
	}
	setDefaults(&qc)

	if err := r.syncServices(ctx, &qc); err != nil {
		return ctrl.Result{}, err
	}

	var sts appsv1.StatefulSet
	err := r.Get(ctx, req.NamespacedName, &sts)
	if apierrors.IsNotFound(err) {
		// A new cluster, or a StatefulSet being created again around its
		// pods after its volume template changed.
		fresh := statefulSet(&qc, r.podsWanted(ctx, &qc))
		if err := controllerutil.SetControllerReference(&qc, fresh, r.Scheme); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.Create(ctx, fresh); err != nil {
			return ctrl.Result{}, err
		}
		r.event(&qc, "Created", "created StatefulSet %s with %d pods", fresh.Name, *fresh.Spec.Replicas)
		return r.finish(ctx, &qc, nil, Action{Kind: Wait, Phase: phaseWhileForming(&qc), Message: "the StatefulSet was created"})
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	if err := r.syncStatefulSet(ctx, &qc, &sts); err != nil {
		return ctrl.Result{}, err
	}

	bc := &broker.Client{T: r.Transport(&qc)}
	obs, err := r.observe(ctx, &qc, &sts, bc)
	if err != nil {
		return ctrl.Result{}, err
	}
	action := plan(&obs.o)
	action, err = r.act(ctx, &qc, obs, bc, action)
	if err != nil {
		return ctrl.Result{}, err
	}
	return r.finish(ctx, &qc, obs, action)
}

func phaseWhileForming(qc *queenv1.QueenCluster) string {
	if len(qc.Status.Voters) > 0 {
		return PhaseResizing
	}
	return PhaseForming
}

// podsWanted is the number of pods a StatefulSet created now must have: the
// voters asked for on a new cluster, and the pods that exist when it is
// created again around them.
func (r *QueenClusterReconciler) podsWanted(ctx context.Context, qc *queenv1.QueenCluster) int {
	var list corev1.PodList
	if err := r.List(ctx, &list, client.InNamespace(qc.Namespace), client.MatchingLabels(selectorLabels(qc))); err == nil {
		highest := -1
		for i := range list.Items {
			if n, ok := ordinalOf(qc, list.Items[i].Name); ok && n > highest {
				highest = n
			}
		}
		if highest >= 0 {
			return highest + 1
		}
	}
	return int(qc.Spec.Replicas)
}

// setDefaults fills what the CRD's defaults fill, for an object that was
// stored before a default existed.
func setDefaults(qc *queenv1.QueenCluster) {
	s := &qc.Spec
	if s.Image == "" {
		s.Image = "ghcr.io/queen-mq/queen"
	}
	if s.ImagePullPolicy == "" {
		s.ImagePullPolicy = corev1.PullIfNotPresent
	}
	if s.Replicas == 0 {
		s.Replicas = 3
	}
	if s.Port == 0 {
		s.Port = 6632
	}
	if s.RaftPort == 0 {
		s.RaftPort = 7400
	}
	if s.ClusterDomain == "" {
		s.ClusterDomain = "cluster.local"
	}
	if s.PodAntiAffinity == "" {
		s.PodAntiAffinity = "required"
	}
	if s.ZoneSpread == "" {
		s.ZoneSpread = corev1.ScheduleAnyway
	}
	if s.TerminationGracePeriodSeconds == 0 {
		s.TerminationGracePeriodSeconds = 90
	}
	if s.Storage.Size.IsZero() {
		s.Storage.Size = resourceMustParse("50Gi")
	}
	if s.Proxy.Port == 0 {
		s.Proxy.Port = 6711
	}
	if s.Kafka.Port == 0 {
		s.Kafka.Port = 9092
	}
}

func (r *QueenClusterReconciler) syncServices(ctx context.Context, qc *queenv1.QueenCluster) error {
	services := []*corev1.Service{clientService(qc), headlessService(qc)}
	if qc.Spec.Proxy.Enabled {
		services = append(services, proxyService(qc))
	}
	for _, want := range services {
		got := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: want.Name, Namespace: want.Namespace}}
		if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, got, func() error {
			got.Labels = mergeLabels(got.Labels, want.Labels)
			got.Spec.Selector = want.Spec.Selector
			got.Spec.Ports = want.Spec.Ports
			if got.CreationTimestamp.IsZero() {
				got.Spec.ClusterIP = want.Spec.ClusterIP
				got.Spec.PublishNotReadyAddresses = want.Spec.PublishNotReadyAddresses
			}
			return controllerutil.SetControllerReference(qc, got, r.Scheme)
		}); err != nil {
			return err
		}
	}
	if !qc.Spec.Proxy.Enabled {
		stale := proxyService(qc)
		var got corev1.Service
		if err := r.Get(ctx, client.ObjectKeyFromObject(stale), &got); err == nil && metav1.IsControlledBy(&got, qc) {
			if err := r.Delete(ctx, &got); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
		}
	}
	want := disruptionBudget(qc)
	got := &policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: want.Name, Namespace: want.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, got, func() error {
		got.Labels = mergeLabels(got.Labels, want.Labels)
		got.Spec = want.Spec
		return controllerutil.SetControllerReference(qc, got, r.Scheme)
	})
	return err
}

func mergeLabels(have, want map[string]string) map[string]string {
	if have == nil {
		have = map[string]string{}
	}
	for k, v := range want {
		have[k] = v
	}
	return have
}

// syncStatefulSet brings the parts of the StatefulSet that may change to
// what the spec asks, for the pods it has now. Nothing here restarts a pod:
// the strategy is OnDelete.
func (r *QueenClusterReconciler) syncStatefulSet(ctx context.Context, qc *queenv1.QueenCluster, sts *appsv1.StatefulSet) error {
	want := statefulSet(qc, int(*sts.Spec.Replicas))
	before := sts.DeepCopy()
	sts.Labels = mergeLabels(sts.Labels, want.Labels)
	sts.Spec.Template = want.Spec.Template
	sts.Spec.UpdateStrategy = want.Spec.UpdateStrategy
	sts.Spec.MinReadySeconds = want.Spec.MinReadySeconds
	sts.Spec.PersistentVolumeClaimRetentionPolicy = want.Spec.PersistentVolumeClaimRetentionPolicy
	if err := controllerutil.SetControllerReference(qc, sts, r.Scheme); err != nil {
		return err
	}
	return r.Patch(ctx, sts, client.MergeFrom(before))
}

// act carries out the step the plan decided, and returns the action as it
// turned out: a step the cluster was not ready for becomes a wait.
func (r *QueenClusterReconciler) act(ctx context.Context, qc *queenv1.QueenCluster, obs *observation, bc *broker.Client, a Action) (Action, error) {
	logger := log.FromContext(ctx)
	if a.Disruptive {
		// Stopping a pod is safe only on a cluster that commits, and the
		// write goes through a pod that stays.
		if err := r.probe(ctx, qc, obs, bc, a.Ordinal); err != nil {
			return wait(a.Phase, "pod %d is next, but a write does not commit: %v", a.Ordinal, err), nil
		}
	}
	cctx, cancel := context.WithTimeout(ctx, changeTimeout)
	defer cancel()
	switch a.Kind {
	case Done, Wait:
		return a, nil

	case ScalePods:
		before := obs.sts.DeepCopy()
		obs.sts.Spec.Replicas = ptr.To(int32(a.Pods))
		obs.sts.Spec.Template = statefulSet(qc, a.Pods).Spec.Template
		if err := r.Patch(ctx, obs.sts, client.MergeFrom(before)); err != nil {
			return a, err
		}

	case AddLearner:
		id := a.NodeIDs[0]
		if _, err := bc.AddLearner(cctx, obs.via, id, raftAddr(qc, a.Ordinal), httpAddr(qc, a.Ordinal)); err != nil {
			return refusedOrError(a, err)
		}

	case Promote:
		if _, err := bc.Promote(cctx, obs.via, a.NodeIDs); err != nil {
			return refusedOrError(a, err)
		}

	case RemoveMember:
		if _, err := bc.Remove(cctx, obs.via, a.NodeIDs[0]); err != nil {
			return refusedOrError(a, err)
		}

	case RestartPod:
		pod := obs.pods[a.Ordinal]
		if pod == nil {
			return a, nil
		}
		if err := r.Delete(ctx, pod, client.Preconditions{UID: &pod.UID}); err != nil && !apierrors.IsNotFound(err) {
			return a, err
		}

	case StartReplace:
		qc.Status.Replace = &queenv1.ReplaceStatus{Pod: podName(qc, a.Ordinal), ClaimUID: obs.o.Pods[a.Ordinal].ClaimUID}

	case WipePod:
		var pvc corev1.PersistentVolumeClaim
		key := types.NamespacedName{Namespace: qc.Namespace, Name: claimName(qc, a.Ordinal)}
		if err := r.Get(ctx, key, &pvc); err == nil && string(pvc.UID) == obs.o.ReplaceClaimUID && pvc.DeletionTimestamp == nil {
			if err := r.Delete(ctx, &pvc, client.Preconditions{UID: &pvc.UID}); err != nil && !apierrors.IsNotFound(err) {
				return a, err
			}
		}
		if pod := obs.pods[a.Ordinal]; pod != nil && pod.DeletionTimestamp == nil {
			if err := r.Delete(ctx, pod, client.Preconditions{UID: &pod.UID}); err != nil && !apierrors.IsNotFound(err) {
				return a, err
			}
		}

	case FinishReplace:
		before := qc.DeepCopy()
		delete(qc.Annotations, queenv1.ReplaceAnnotation)
		status := qc.Status
		if err := r.Patch(ctx, qc, client.MergeFrom(before)); err != nil {
			return a, err
		}
		qc.Status = status
		qc.Status.Replace = nil

	case ExpandClaim:
		var pvc corev1.PersistentVolumeClaim
		key := types.NamespacedName{Namespace: qc.Namespace, Name: claimName(qc, a.Ordinal)}
		if err := r.Get(ctx, key, &pvc); err != nil {
			return a, client.IgnoreNotFound(err)
		}
		before := pvc.DeepCopy()
		pvc.Spec.Resources.Requests[corev1.ResourceStorage] = qc.Spec.Storage.Size
		if err := r.Patch(ctx, &pvc, client.MergeFrom(before)); err != nil {
			if apierrors.IsForbidden(err) || apierrors.IsInvalid(err) {
				// A StorageClass that does not allow expansion says so here.
				return wait(PhaseBlocked, "the volume of pod %d cannot grow: %v", a.Ordinal, err), nil
			}
			return a, err
		}

	case RecreateStatefulSet:
		if err := r.Delete(ctx, obs.sts, client.PropagationPolicy(metav1.DeletePropagationOrphan), client.Preconditions{UID: &obs.sts.UID}); err != nil && !apierrors.IsNotFound(err) {
			return a, err
		}
	}
	logger.Info(a.Message, "phase", a.Phase)
	r.event(qc, a.Kind.Reason(), "%s", a.Message)
	return a, nil
}

// A refusal with a code is the leader's answer to the state the cluster is
// in (a learner still behind, a change in flight, too few live voters): it is
// reported and tried again, not an error of the operator.
func refusedOrError(a Action, err error) (Action, error) {
	if code := broker.RefusedCode(err); code != "" {
		return wait(a.Phase, "%s: the leader answered %q", a.Message, code), nil
	}
	return wait(a.Phase, "%s: %v", a.Message, err), nil
}

// probe commits one write through a running pod other than `except`.
func (r *QueenClusterReconciler) probe(ctx context.Context, qc *queenv1.QueenCluster, obs *observation, bc *broker.Client, except int) error {
	pctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	now := metav1.NewTime(r.now())
	qc.Status.LastProbeTime = &now
	for i := range obs.o.Pods {
		p := &obs.o.Pods[i]
		if p.Ordinal == except && len(obs.o.Pods) > 1 {
			continue
		}
		if p.Health == nil {
			continue
		}
		err := bc.Probe(pctx, podName(qc, p.Ordinal))
		qc.Status.Commits = err == nil
		return err
	}
	qc.Status.Commits = false
	return fmt.Errorf("no pod answers")
}

// finish writes the status and says when to look again.
func (r *QueenClusterReconciler) finish(ctx context.Context, qc *queenv1.QueenCluster, obs *observation, a Action) (ctrl.Result, error) {
	st := &qc.Status
	st.ObservedGeneration = qc.Generation
	st.Phase = a.Phase
	st.Message = a.Message

	if obs != nil {
		o := &obs.o
		if m := o.Membership; m != nil {
			st.Leader, st.Term = *m.Leader, m.Term
			st.Voters, st.Learners = m.Voters, m.Learners
		}
		// A cluster at rest proves it commits once a minute; a step that
		// stops a pod already did.
		if o.Membership != nil && (st.LastProbeTime == nil || r.now().Sub(st.LastProbeTime.Time) >= probeEvery) {
			bc := &broker.Client{T: r.Transport(qc)}
			_ = r.probe(ctx, qc, obs, bc, -1)
		}
		st.Members = st.Members[:0]
		version, mixed := "", false
		for i := range o.Pods {
			p := &o.Pods[i]
			ms := queenv1.MemberStatus{NodeID: nodeID(p.Ordinal), Pod: podName(qc, p.Ordinal), Ready: p.Ready}
			if p.Health != nil {
				ms.Version = p.Health.Version
			}
			if !p.Exists || ms.Version == "" || (version != "" && ms.Version != version) {
				mixed = true
			}
			if version == "" {
				version = ms.Version
			}
			if m := o.Membership; m != nil {
				ms.Voter = m.IsVoter(ms.NodeID)
				if mem := m.Member(ms.NodeID); mem != nil {
					ms.Live, ms.Lag = mem.Live, mem.Lag
				}
			}
			st.Members = append(st.Members, ms)
		}
		st.Version = version
		if mixed {
			st.Version = ""
		}
	}

	ready := metav1.Condition{Type: "Ready", Status: metav1.ConditionFalse, Reason: a.Phase, Message: a.Message, ObservedGeneration: qc.Generation}
	if a.Kind == Done {
		ready.Status = metav1.ConditionTrue
		ready.Message = "every voter is in place and the cluster commits"
	}
	meta.SetStatusCondition(&st.Conditions, ready)

	if err := r.Status().Update(ctx, qc); err != nil {
		if apierrors.IsConflict(err) {
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
		return ctrl.Result{}, err
	}
	switch a.Kind {
	case Done:
		return ctrl.Result{RequeueAfter: readyRequeue}, nil
	case Wait:
		return ctrl.Result{RequeueAfter: busyRequeue}, nil
	}
	// A step was taken: the next one follows at once. A node that just left
	// the membership must not keep its pod a moment longer than needed.
	return ctrl.Result{RequeueAfter: stepRequeue}, nil
}

func (r *QueenClusterReconciler) event(qc *queenv1.QueenCluster, reason, format string, args ...any) {
	if r.Recorder != nil {
		r.Recorder.Eventf(qc, corev1.EventTypeNormal, reason, format, args...)
	}
}

func (r *QueenClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// A pod belongs to the StatefulSet, not to the QueenCluster: its changes
	// reach the cluster through the instance label.
	podToCluster := handler.EnqueueRequestsFromMapFunc(func(_ context.Context, o client.Object) []reconcile.Request {
		l := o.GetLabels()
		if l["app.kubernetes.io/name"] != "queen" || l["app.kubernetes.io/instance"] == "" {
			return nil
		}
		return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: o.GetNamespace(), Name: l["app.kubernetes.io/instance"]}}}
	})
	return ctrl.NewControllerManagedBy(mgr).
		For(&queenv1.QueenCluster{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Owns(&policyv1.PodDisruptionBudget{}).
		Watches(&corev1.Pod{}, podToCluster).
		Complete(r)
}

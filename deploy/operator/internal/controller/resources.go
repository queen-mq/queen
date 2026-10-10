package controller

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	queenv1 "github.com/queen-mq/queen/deploy/operator/api/v1alpha1"
)

const (
	// The hash of everything in the pod template that a running pod must be
	// restarted to take. The peer list is left out: it changes with the
	// number of voters, and a node reads it only on its first start.
	hashAnnotation = "queenmq.com/config-hash"

	dataDir       = "/var/lib/queen/raft"
	containerName = "queen"
	dataVolume    = "data"
)

// The selector of everything a QueenCluster owns. It is not the `app: queen`
// of the manifest in the docs: a StatefulSet applied from that manifest keeps
// its own selector, and is not something a QueenCluster of the same name can
// take over.
func selectorLabels(qc *queenv1.QueenCluster) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":     "queen",
		"app.kubernetes.io/instance": qc.Name,
	}
}

func objectLabels(qc *queenv1.QueenCluster, component string) map[string]string {
	l := selectorLabels(qc)
	l["app.kubernetes.io/managed-by"] = "queen-operator"
	if component != "" {
		l["app.kubernetes.io/component"] = component
	}
	return l
}

func headlessName(qc *queenv1.QueenCluster) string { return qc.Name + "-headless" }

// The domain every pod is reachable under, whether it is Ready or not.
func headlessDomain(qc *queenv1.QueenCluster) string {
	return fmt.Sprintf("%s.%s.svc.%s", headlessName(qc), qc.Namespace, qc.Spec.ClusterDomain)
}

// HeadlessDomain is headlessDomain for the transport that reaches the pods
// by name.
func HeadlessDomain(qc *queenv1.QueenCluster) string { return headlessDomain(qc) }

func resourceMustParse(s string) resource.Quantity { return resource.MustParse(s) }

func podName(qc *queenv1.QueenCluster, ordinal int) string {
	return fmt.Sprintf("%s-%d", qc.Name, ordinal)
}

// Pod N is raft node N+1.
func nodeID(ordinal int) int64 { return int64(ordinal) + 1 }

func ordinalOf(qc *queenv1.QueenCluster, pod string) (int, bool) {
	rest, ok := strings.CutPrefix(pod, qc.Name+"-")
	if !ok {
		return 0, false
	}
	n, err := strconv.Atoi(rest)
	return n, err == nil && n >= 0
}

func raftAddr(qc *queenv1.QueenCluster, ordinal int) string {
	return fmt.Sprintf("%s.%s:%d", podName(qc, ordinal), headlessDomain(qc), qc.Spec.RaftPort)
}

func httpAddr(qc *queenv1.QueenCluster, ordinal int) string {
	return fmt.Sprintf("%s.%s:%d", podName(qc, ordinal), headlessDomain(qc), qc.Spec.Port)
}

// QUEEN_RAFT_PEERS for a cluster of `pods` pods: id=raft address/HTTP address.
func peers(qc *queenv1.QueenCluster, pods int) string {
	parts := make([]string, 0, pods)
	for i := 0; i < pods; i++ {
		parts = append(parts, fmt.Sprintf("%d=%s/%s", nodeID(i), raftAddr(qc, i), httpAddr(qc, i)))
	}
	return strings.Join(parts, ",")
}

func clientService(qc *queenv1.QueenCluster) *corev1.Service {
	s := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: qc.Name, Namespace: qc.Namespace, Labels: objectLabels(qc, "client")},
		Spec: corev1.ServiceSpec{
			Selector: selectorLabels(qc),
			Ports:    []corev1.ServicePort{{Name: "http", Port: qc.Spec.Port, TargetPort: intstr.FromString("http")}},
		},
	}
	if qc.Spec.Kafka.Enabled {
		s.Spec.Ports = append(s.Spec.Ports, corev1.ServicePort{Name: "kafka", Port: qc.Spec.Kafka.Port, TargetPort: intstr.FromString("kafka")})
	}
	return s
}

// The pods' own names, resolvable before they are Ready: without that no
// leader is elected after a full restart.
func headlessService(qc *queenv1.QueenCluster) *corev1.Service {
	s := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: headlessName(qc), Namespace: qc.Namespace, Labels: objectLabels(qc, "peers")},
		Spec: corev1.ServiceSpec{
			ClusterIP:                corev1.ClusterIPNone,
			PublishNotReadyAddresses: true,
			Selector:                 selectorLabels(qc),
			Ports: []corev1.ServicePort{
				{Name: "http", Port: qc.Spec.Port, TargetPort: intstr.FromString("http")},
				{Name: "raft", Port: qc.Spec.RaftPort, TargetPort: intstr.FromString("raft")},
			},
		},
	}
	if qc.Spec.Kafka.Enabled {
		s.Spec.Ports = append(s.Spec.Ports, corev1.ServicePort{Name: "kafka", Port: qc.Spec.Kafka.Port, TargetPort: intstr.FromString("kafka")})
	}
	return s
}

func proxyService(qc *queenv1.QueenCluster) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: qc.Name + "-proxy", Namespace: qc.Namespace, Labels: objectLabels(qc, "proxy")},
		Spec: corev1.ServiceSpec{
			Selector: selectorLabels(qc),
			Ports:    []corev1.ServicePort{{Name: "proxy", Port: qc.Spec.Proxy.Port, TargetPort: intstr.FromString("proxy")}},
		},
	}
}

func disruptionBudget(qc *queenv1.QueenCluster) *policyv1.PodDisruptionBudget {
	one := intstr.FromInt32(1)
	return &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: qc.Name, Namespace: qc.Namespace, Labels: objectLabels(qc, "")},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MaxUnavailable: &one,
			Selector:       &metav1.LabelSelector{MatchLabels: selectorLabels(qc)},
		},
	}
}

func secretEnv(qc *queenv1.QueenCluster, key string, optional bool) corev1.EnvVar {
	ref := &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: qc.Spec.SecretName},
		Key:                  key,
	}
	if optional {
		ref.Optional = ptr.To(true)
	}
	return corev1.EnvVar{Name: key, ValueFrom: &corev1.EnvVarSource{SecretKeyRef: ref}}
}

func resources(qc *queenv1.QueenCluster) corev1.ResourceRequirements {
	if qc.Spec.Resources != nil {
		return *qc.Spec.Resources
	}
	// No CPU limit: throttling turns a burst of work into a missed heartbeat.
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("1"),
			corev1.ResourceMemory: resource.MustParse("4Gi"),
		},
		Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("4Gi")},
	}
}

// The Kafka node id is the pod's ordinal plus one; it does not take `ordinal`.
const kafkaNodeIDWrapper = `ord="${HOSTNAME##*-}"
case "$ord" in
  ''|*[!0-9]*)
    echo "queen: cannot derive QUEEN_KAFKA_NODE_ID from HOSTNAME=$HOSTNAME" >&2
    exit 1 ;;
esac
QUEEN_KAFKA_NODE_ID=$(( ord + 1 ))
export QUEEN_KAFKA_NODE_ID
exec /app/bin/queen
`

// podTemplate renders the pod of a cluster of `pods` pods. Everything in it
// but the peer list goes into the config hash.
func podTemplate(qc *queenv1.QueenCluster, pods int) corev1.PodTemplateSpec {
	spec := &qc.Spec
	kafkaCluster := spec.Kafka.Enabled && spec.Replicas > 1

	env := []corev1.EnvVar{
		{Name: "PORT", Value: strconv.Itoa(int(spec.Port))},
		{Name: "QUEEN_RAFT_REPLICATOR", Value: "openraft"},
		{Name: "QUEEN_RAFT_DIR", Value: dataDir},
		{Name: "QUEEN_RAFT_NODE_ID", Value: "ordinal"},
		{Name: "QUEEN_RAFT_PEERS", Value: peers(qc, pods)},
		secretEnv(qc, "QUEEN_RAFT_TOKEN", false),
		secretEnv(qc, "QUEEN_ENCRYPTION_KEY", true),
		{Name: "QUEEN_SERVER_ID", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
		{Name: "QUEEN_LOG_JSON", Value: "true"},
	}
	ports := []corev1.ContainerPort{
		{Name: "http", ContainerPort: spec.Port},
		{Name: "raft", ContainerPort: spec.RaftPort},
	}
	if spec.Proxy.Enabled {
		ports = append(ports, corev1.ContainerPort{Name: "proxy", ContainerPort: spec.Proxy.Port})
		env = append(env,
			corev1.EnvVar{Name: "QUEEN_PROXY_EMBEDDED", Value: "true"},
			corev1.EnvVar{Name: "QUEEN_PROXY_PORT", Value: strconv.Itoa(int(spec.Proxy.Port))},
			corev1.EnvVar{Name: "QUEEN_PROXY_SPOOL_DIR", Value: dataDir + "/proxy-spool"},
			secretEnv(qc, "QUEEN_PROXY_JWT_SECRET", false),
			secretEnv(qc, "QUEEN_PROXY_CP_TOKEN", true),
			secretEnv(qc, "QUEEN_PROXY_BOOTSTRAP_API_KEY", true),
		)
		if spec.Proxy.PublicURL != "" {
			env = append(env, corev1.EnvVar{Name: "QUEEN_PROXY_PUBLIC_URL", Value: spec.Proxy.PublicURL})
		}
	}
	var command []string
	if spec.Kafka.Enabled {
		ports = append(ports, corev1.ContainerPort{Name: "kafka", ContainerPort: spec.Kafka.Port})
		env = append(env,
			corev1.EnvVar{Name: "QUEEN_KAFKA_EMBEDDED", Value: "true"},
			corev1.EnvVar{Name: "QUEEN_KAFKA_ADDR", Value: fmt.Sprintf("0.0.0.0:%d", spec.Kafka.Port)},
			corev1.EnvVar{Name: "QUEEN_KAFKA_ADVERTISED_ADDR", Value: fmt.Sprintf("$(QUEEN_SERVER_ID).%s:%d", headlessDomain(qc), spec.Kafka.Port)},
		)
		if kafkaCluster {
			env = append(env, secretEnv(qc, "QUEEN_TOKEN", false))
			command = []string{"/bin/sh", "-c", kafkaNodeIDWrapper}
		}
	}
	env = append(env, spec.Env...)

	antiAffinityTerm := corev1.PodAffinityTerm{
		TopologyKey:   "kubernetes.io/hostname",
		LabelSelector: &metav1.LabelSelector{MatchLabels: selectorLabels(qc)},
	}
	antiAffinity := &corev1.PodAntiAffinity{}
	if spec.PodAntiAffinity == "preferred" {
		antiAffinity.PreferredDuringSchedulingIgnoredDuringExecution = []corev1.WeightedPodAffinityTerm{{Weight: 100, PodAffinityTerm: antiAffinityTerm}}
	} else {
		antiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = []corev1.PodAffinityTerm{antiAffinityTerm}
	}

	labels := selectorLabels(qc)
	for k, v := range spec.PodLabels {
		if _, reserved := labels[k]; !reserved {
			labels[k] = v
		}
	}
	annotations := map[string]string{}
	for k, v := range spec.PodAnnotations {
		annotations[k] = v
	}

	tmpl := corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{Labels: labels, Annotations: annotations},
		Spec: corev1.PodSpec{
			TerminationGracePeriodSeconds: ptr.To(spec.TerminationGracePeriodSeconds),
			AutomountServiceAccountToken:  ptr.To(false),
			ImagePullSecrets:              spec.ImagePullSecrets,
			PriorityClassName:             spec.PriorityClassName,
			NodeSelector:                  spec.NodeSelector,
			Tolerations:                   spec.Tolerations,
			SecurityContext: &corev1.PodSecurityContext{
				RunAsNonRoot:   ptr.To(true),
				RunAsUser:      ptr.To(int64(65532)),
				RunAsGroup:     ptr.To(int64(65532)),
				FSGroup:        ptr.To(int64(65532)),
				SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
			},
			Affinity: &corev1.Affinity{PodAntiAffinity: antiAffinity},
			TopologySpreadConstraints: []corev1.TopologySpreadConstraint{{
				MaxSkew:           1,
				TopologyKey:       "topology.kubernetes.io/zone",
				WhenUnsatisfiable: spec.ZoneSpread,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: selectorLabels(qc)},
			}},
			Containers: []corev1.Container{{
				Name:            containerName,
				Image:           spec.Image + ":" + spec.Version,
				ImagePullPolicy: spec.ImagePullPolicy,
				Command:         command,
				Ports:           ports,
				Env:             env,
				Resources:       resources(qc),
				// An open port means the boot finished: the node binds it after
				// the store and the replicator have opened. 360 probes give a
				// long replay 30 minutes.
				StartupProbe: &corev1.Probe{
					ProbeHandler:     corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromString("http")}},
					PeriodSeconds:    5,
					TimeoutSeconds:   5,
					FailureThreshold: 360,
				},
				ReadinessProbe: &corev1.Probe{
					ProbeHandler:     corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/health", Port: intstr.FromString("http")}},
					PeriodSeconds:    5,
					TimeoutSeconds:   5,
					FailureThreshold: 3,
				},
				// Never /health: an election would restart every pod at once.
				LivenessProbe: &corev1.Probe{
					ProbeHandler:     corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromString("http")}},
					PeriodSeconds:    20,
					TimeoutSeconds:   5,
					FailureThreshold: 3,
				},
				Lifecycle: &corev1.Lifecycle{PreStop: &corev1.LifecycleHandler{Exec: &corev1.ExecAction{Command: []string{"/bin/sleep", "5"}}}},
				SecurityContext: &corev1.SecurityContext{
					AllowPrivilegeEscalation: ptr.To(false),
					ReadOnlyRootFilesystem:   ptr.To(true),
					Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
				},
				VolumeMounts: []corev1.VolumeMount{
					{Name: dataVolume, MountPath: dataDir},
					{Name: "tmp", MountPath: "/tmp"},
				},
			}},
			Volumes: []corev1.Volume{{Name: "tmp", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}},
		},
	}
	tmpl.Annotations[hashAnnotation] = configHash(tmpl)
	return tmpl
}

// configHash is the hash of a pod template with the peer list blanked: what
// a running pod has to be restarted for.
func configHash(tmpl corev1.PodTemplateSpec) string {
	t := tmpl.DeepCopy()
	delete(t.Annotations, hashAnnotation)
	for c := range t.Spec.Containers {
		for e := range t.Spec.Containers[c].Env {
			if t.Spec.Containers[c].Env[e].Name == "QUEEN_RAFT_PEERS" {
				t.Spec.Containers[c].Env[e].Value = ""
			}
		}
	}
	raw, _ := json.Marshal(t)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:8])
}

// statefulSet renders the StatefulSet of a cluster of `pods` pods.
//
// The update strategy is OnDelete: the operator restarts the pods itself, one
// at a time, the leader last, each behind a write that commits. A scale-down
// deletes the claims of the pods it removes, so a pod added again later
// starts empty and never comes back with an old copy of the log; deleting the
// StatefulSet (or the QueenCluster) keeps every claim.
func statefulSet(qc *queenv1.QueenCluster, pods int) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: qc.Name, Namespace: qc.Namespace, Labels: objectLabels(qc, "")},
		Spec: appsv1.StatefulSetSpec{
			Replicas:            ptr.To(int32(pods)),
			ServiceName:         headlessName(qc),
			PodManagementPolicy: appsv1.ParallelPodManagement,
			MinReadySeconds:     qc.Spec.MinReadySeconds,
			UpdateStrategy:      appsv1.StatefulSetUpdateStrategy{Type: appsv1.OnDeleteStatefulSetStrategyType},
			Selector:            &metav1.LabelSelector{MatchLabels: selectorLabels(qc)},
			PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
				WhenDeleted: appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
				WhenScaled:  appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
			},
			Template: podTemplate(qc, pods),
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{
				ObjectMeta: metav1.ObjectMeta{Name: dataVolume},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					StorageClassName: qc.Spec.Storage.StorageClassName,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceStorage: qc.Spec.Storage.Size},
					},
				},
			}},
		},
	}
}

func claimName(qc *queenv1.QueenCluster, ordinal int) string {
	return fmt.Sprintf("%s-%s", dataVolume, podName(qc, ordinal))
}

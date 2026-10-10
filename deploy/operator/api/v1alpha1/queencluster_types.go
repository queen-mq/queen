package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// QueenClusterSpec is the cluster an operator wants: the release it runs, how
// many voters it has and what each pod gets.
type QueenClusterSpec struct {
	// The broker release to run: the image tag.
	// +kubebuilder:validation:MinLength=1
	Version string `json:"version"`

	// The image repository.
	// +kubebuilder:default="ghcr.io/queen-mq/queen"
	Image string `json:"image,omitempty"`

	// +kubebuilder:default=IfNotPresent
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`

	ImagePullSecrets []corev1.LocalObjectReference `json:"imagePullSecrets,omitempty"`

	// Voters: 1, 3 or 5. Changing it adds or removes voters through the
	// membership API, one change at a time.
	// +kubebuilder:validation:Enum=1;3;5
	// +kubebuilder:default=3
	Replicas int32 `json:"replicas,omitempty"`

	// An existing Secret. QUEEN_RAFT_TOKEN is required in it;
	// QUEEN_ENCRYPTION_KEY, QUEEN_PROXY_JWT_SECRET, QUEEN_PROXY_CP_TOKEN,
	// QUEEN_PROXY_BOOTSTRAP_API_KEY and QUEEN_TOKEN are read when present.
	// +kubebuilder:validation:MinLength=1
	SecretName string `json:"secretName"`

	Storage StorageSpec `json:"storage,omitempty"`

	// Defaults to 1 CPU and 4Gi of memory, with no CPU limit.
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`

	// required: one voter per machine. preferred: for a one-machine test cluster.
	// +kubebuilder:validation:Enum=required;preferred
	// +kubebuilder:default=required
	PodAntiAffinity string `json:"podAntiAffinity,omitempty"`

	// +kubebuilder:validation:Enum=ScheduleAnyway;DoNotSchedule
	// +kubebuilder:default=ScheduleAnyway
	ZoneSpread corev1.UnsatisfiableConstraintAction `json:"zoneSpread,omitempty"`

	NodeSelector      map[string]string   `json:"nodeSelector,omitempty"`
	Tolerations       []corev1.Toleration `json:"tolerations,omitempty"`
	PriorityClassName string              `json:"priorityClassName,omitempty"`

	Proxy ProxySpec `json:"proxy,omitempty"`
	Kafka KafkaSpec `json:"kafka,omitempty"`

	// Any other broker setting.
	Env            []corev1.EnvVar   `json:"env,omitempty"`
	PodLabels      map[string]string `json:"podLabels,omitempty"`
	PodAnnotations map[string]string `json:"podAnnotations,omitempty"`

	// The broker port. Part of the peer addresses: fixed once the cluster exists.
	// +kubebuilder:default=6632
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="port is part of the peer addresses and cannot change"
	Port int32 `json:"port,omitempty"`

	// The raft port. Fixed once the cluster exists.
	// +kubebuilder:default=7400
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="raftPort is part of the peer addresses and cannot change"
	RaftPort int32 `json:"raftPort,omitempty"`

	// The cluster's DNS domain. Fixed once the cluster exists.
	// +kubebuilder:default="cluster.local"
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="clusterDomain is part of the peer addresses and cannot change"
	ClusterDomain string `json:"clusterDomain,omitempty"`

	// How long a restarted pod must stay Ready before the next one is stopped.
	// +kubebuilder:default=30
	// +kubebuilder:validation:Minimum=0
	MinReadySeconds int32 `json:"minReadySeconds,omitempty"`

	// +kubebuilder:default=90
	TerminationGracePeriodSeconds int64 `json:"terminationGracePeriodSeconds,omitempty"`
}

type StorageSpec struct {
	// One volume per pod. It can grow, never shrink.
	// +kubebuilder:default="50Gi"
	Size resource.Quantity `json:"size,omitempty"`

	// Empty: the cluster's default StorageClass. Fixed once the cluster exists.
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="storageClassName cannot change"
	StorageClassName *string `json:"storageClassName,omitempty"`
}

// ProxySpec turns on the proxy: tenants, API keys and the console, on a port
// of its own, with a Service named <cluster>-proxy.
type ProxySpec struct {
	Enabled bool `json:"enabled,omitempty"`
	// +kubebuilder:default=6711
	Port int32 `json:"port,omitempty"`
	// The address users reach it at, for OAuth callbacks and sessions.
	PublicURL string `json:"publicUrl,omitempty"`
}

// KafkaSpec turns on the Kafka facade. With more than one voter the pods
// present themselves as one Kafka cluster and need QUEEN_TOKEN in the Secret.
type KafkaSpec struct {
	Enabled bool `json:"enabled,omitempty"`
	// +kubebuilder:default=9092
	Port int32 `json:"port,omitempty"`
}

// QueenClusterStatus is what the operator last saw, and what it is doing.
type QueenClusterStatus struct {
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Forming, Ready, Scaling, Upgrading, Replacing, Resizing or Blocked.
	Phase string `json:"phase,omitempty"`

	// What the operator is doing or waiting for, in one line.
	Message string `json:"message,omitempty"`

	// The release every pod runs; empty while they differ.
	Version string `json:"version,omitempty"`

	// The raft membership as the leader reports it.
	Leader   int64   `json:"leader,omitempty"`
	Term     int64   `json:"term,omitempty"`
	Voters   []int64 `json:"voters,omitempty"`
	Learners []int64 `json:"learners,omitempty"`

	Members []MemberStatus `json:"members,omitempty"`

	// Whether the last write probe committed, and when it ran. A cluster
	// commits or it does not: this is the one figure to alert on.
	Commits       bool         `json:"commits,omitempty"`
	LastProbeTime *metav1.Time `json:"lastProbeTime,omitempty"`

	// A node replacement in progress.
	Replace *ReplaceStatus `json:"replace,omitempty"`

	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

type MemberStatus struct {
	NodeID int64  `json:"nodeId"`
	Pod    string `json:"pod"`
	Voter  bool   `json:"voter"`
	// The leader heard from it within its liveness window.
	Live bool `json:"live"`
	// Entries behind the leader's last one.
	Lag *int64 `json:"lag,omitempty"`
	// The pod is Ready.
	Ready   bool   `json:"ready"`
	Version string `json:"version,omitempty"`
}

// ReplaceStatus records the volume a replacement started from, so a restart
// of the operator in the middle of one never wipes the new volume.
type ReplaceStatus struct {
	Pod string `json:"pod"`
	// The UID of the claim that is being replaced.
	ClaimUID string `json:"claimUID,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=qc
// +kubebuilder:printcolumn:name="Version",type=string,JSONPath=`.status.version`
// +kubebuilder:printcolumn:name="Voters",type=string,JSONPath=`.status.voters`
// +kubebuilder:printcolumn:name="Commits",type=boolean,JSONPath=`.status.commits`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Message",type=string,JSONPath=`.status.message`,priority=1
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// QueenCluster is one Queen cluster: a StatefulSet of voters, its Services
// and its raft membership.
type QueenCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   QueenClusterSpec   `json:"spec,omitempty"`
	Status QueenClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

type QueenClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []QueenCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&QueenCluster{}, &QueenClusterList{})
}

// The annotation that asks for a node to be replaced: its value is the pod's
// name. The operator removes the member, deletes the pod's volume, and adds
// the empty pod back as a learner, then a voter.
const ReplaceAnnotation = "queenmq.com/replace-pod"

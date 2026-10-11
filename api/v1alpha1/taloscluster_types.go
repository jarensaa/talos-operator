/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// ConfigManagement controls whether the operator only observes machine config or owns it.
// +kubebuilder:validation:Enum=Observe;Manage
type ConfigManagement string

const (
	ConfigManagementObserve ConfigManagement = "Observe"
	ConfigManagementManage  ConfigManagement = "Manage"
)

// CapabilityState enables or disables a management capability.
// +kubebuilder:validation:Enum=Disabled;Enabled
type CapabilityState string

const (
	CapabilityDisabled CapabilityState = "Disabled"
	CapabilityEnabled  CapabilityState = "Enabled"
)

// Management enables operator capabilities one at a time (docs/scope.md §7.1 stage 4).
// Leaving Observe/Disabled is only allowed once the cluster is adopted.
// Capabilities beyond config build on an owned config: rollouts and membership changes render
// and apply it (docs/scope.md §7.1 stage 4).
// +kubebuilder:validation:XValidation:rule="!has(self.upgrades) || self.upgrades != 'Enabled' || (has(self.config) && self.config == 'Manage')",message="upgrades can be Enabled only with config: Manage"
// +kubebuilder:validation:XValidation:rule="!has(self.membership) || self.membership != 'Enabled' || (has(self.config) && self.config == 'Manage')",message="membership can be Enabled only with config: Manage"
type Management struct {
	// Config is Observe (read-only) or Manage (the operator owns the rendered machine config).
	// +kubebuilder:default=Observe
	// +optional
	Config ConfigManagement `json:"config,omitempty"`

	// Upgrades enables Talos and Kubernetes version rollouts.
	// +kubebuilder:default=Disabled
	// +optional
	Upgrades CapabilityState `json:"upgrades,omitempty"`

	// Membership enables adding, removing and replacing machines.
	// +kubebuilder:default=Disabled
	// +optional
	Membership CapabilityState `json:"membership,omitempty"`
}

// Versions declares the desired Talos and Kubernetes versions.
type Versions struct {
	// Talos version, e.g. v1.14.2.
	// +kubebuilder:validation:Pattern=`^v\d+\.\d+\.\d+$`
	// +optional
	Talos string `json:"talos,omitempty"`

	// Kubernetes version, e.g. 1.37.1.
	// +kubebuilder:validation:Pattern=`^\d+\.\d+\.\d+$`
	// +optional
	Kubernetes string `json:"kubernetes,omitempty"`
}

// Installer identifies the Image Factory installer used for installs and upgrades.
type Installer struct {
	// FactoryURL is the Image Factory base URL.
	// +kubebuilder:default="https://factory.talos.dev"
	// +kubebuilder:validation:Pattern=`^https://`
	// +optional
	FactoryURL string `json:"factoryURL,omitempty"`

	// Platform of the installer image, e.g. metal.
	// +kubebuilder:default=metal
	// +optional
	Platform string `json:"platform,omitempty"`

	// SchematicID is the Image Factory schematic ID (64 hex characters).
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{64}$`
	// +optional
	SchematicID string `json:"schematicID,omitempty"`
}

// GenerationOptions are the generate.Input options that differ from talosctl defaults.
type GenerationOptions struct {
	// DNSDomain of the cluster.
	// +optional
	DNSDomain string `json:"dnsDomain,omitempty"`

	// PodSubnets are the pod CIDRs.
	// +optional
	PodSubnets []string `json:"podSubnets,omitempty"`

	// ServiceSubnets are the service CIDRs.
	// +optional
	ServiceSubnets []string `json:"serviceSubnets,omitempty"`

	// KubePrismPort is the KubePrism listen port.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	// +optional
	KubePrismPort *int32 `json:"kubePrismPort,omitempty"`
}

// ConfigGeneration holds the inputs used to generate the base machine config.
type ConfigGeneration struct {
	// VersionContract pins the machine config contract, e.g. v1.14. It is never implied from the
	// operator's machinery version, so a newer operator cannot silently change generated defaults.
	// +kubebuilder:validation:Pattern=`^v\d+\.\d+$`
	// +optional
	VersionContract string `json:"versionContract,omitempty"`

	// Installer is the default installer; a pool may override its schematic.
	// +optional
	Installer *Installer `json:"installer,omitempty"`

	// Options for config generation.
	// +optional
	Options *GenerationOptions `json:"options,omitempty"`
}

// DriftPolicy says what happens when a node's live config stops matching what the operator last
// applied, without the spec having changed.
// +kubebuilder:validation:Enum=Report;Enforce
type DriftPolicy string

const (
	// DriftPolicyReport reports the drift in ConfigInSync and leaves the node alone.
	DriftPolicyReport DriftPolicy = "Report"
	// DriftPolicyEnforce rolls the node back to its rendered config.
	DriftPolicyEnforce DriftPolicy = "Enforce"
)

// Rollout tunes config rollouts (docs/scope.md §6.3, §8.5.1).
// +kubebuilder:validation:XValidation:rule="duration(has(self.soak) ? self.soak : '2m') >= duration('0s')",message="soak must not be negative"
// +kubebuilder:validation:XValidation:rule="duration(has(self.tryTimeout) ? self.tryTimeout : '5m') >= duration(has(self.soak) ? self.soak : '2m') + duration('2m')",message="tryTimeout must exceed soak by at least 2m, or a tried config could never be confirmed"
// +kubebuilder:validation:XValidation:rule="duration(has(self.stepTimeout) ? self.stepTimeout : '10m') > duration(has(self.soak) ? self.soak : '2m')",message="stepTimeout must exceed soak"
type Rollout struct {
	// DriftPolicy is Report (default) or Enforce.
	// +kubebuilder:default=Report
	// +optional
	DriftPolicy DriftPolicy `json:"driftPolicy,omitempty"`

	// TryTimeout is how long Talos keeps a config applied in try mode before reverting it by
	// itself. Every change is applied that way. The operator must verify the node from outside,
	// soak, and confirm within it. Defaults to 5m; it must exceed soak by at least two minutes.
	// +optional
	TryTimeout *metav1.Duration `json:"tryTimeout,omitempty"`

	// Soak is how long a node must stay healthy under a new config before it is confirmed and the
	// rollout moves on. Defaults to 2m.
	// +optional
	Soak *metav1.Duration `json:"soak,omitempty"`

	// StepTimeout bounds every other wait of a node step: for the cluster to be healthy before the
	// node is touched, and for the node to come back after a try ended unconfirmed.
	// Defaults to 10m. A timeout is a failure, never a skip.
	// +optional
	StepTimeout *metav1.Duration `json:"stepTimeout,omitempty"`

	// DrainTimeout bounds the eviction of a node's pods before an upgrade reboots it. Evictions
	// respect PodDisruptionBudgets, so a budget that never lets a pod go ends here. Defaults to
	// 10m. A node that is not drained in time halts the upgrade and stays cordoned.
	// +optional
	DrainTimeout *metav1.Duration `json:"drainTimeout,omitempty"`
	// MaintenanceWindows are the times at which an operation may start, and may begin another node
	// (docs/scope.md §6.4). With none, any time is allowed. A node that is being changed when a
	// window closes is finished.
	// +kubebuilder:validation:MaxItems=16
	// +optional
	MaintenanceWindows []MaintenanceWindow `json:"maintenanceWindows,omitempty"`
	// Gates are conditions on objects in the cluster that must hold before a node is touched and
	// while a changed node is verified, next to the cluster's own health (docs/scope.md §6.4).
	// +kubebuilder:validation:MaxItems=16
	// +listType=map
	// +listMapKey=name
	// +optional
	Gates []RolloutGate `json:"gates,omitempty"`
	// Hooks are Jobs run around an operation and around each node's turn (docs/scope.md §6.4).
	// +optional
	Hooks *RolloutHooks `json:"hooks,omitempty"`
}

// MaintenanceWindow is a recurring time at which nodes may be changed.
// +kubebuilder:validation:XValidation:rule="duration(self.duration) > duration('0s')",message="duration must be positive"
type MaintenanceWindow struct {
	// Schedule is when the window opens, as a cron expression of five fields: minute, hour, day of
	// month, month, day of week. For example "0 2 * * 6" is Saturdays at 02:00.
	// +kubebuilder:validation:Pattern=`^\S+(\s+\S+){4}$`
	// +kubebuilder:validation:MaxLength=128
	Schedule string `json:"schedule"`
	// Duration is how long the window stays open.
	Duration metav1.Duration `json:"duration"`
	// TimeZone is the IANA time zone the schedule is read in, e.g. Europe/Oslo. UTC if empty.
	// +kubebuilder:validation:MaxLength=64
	// +optional
	TimeZone string `json:"timeZone,omitempty"`
}

// GateObject names the object a gate asks about.
type GateObject struct {
	// APIVersion of the object, e.g. ceph.rook.io/v1.
	// +kubebuilder:validation:MinLength=1
	APIVersion string `json:"apiVersion"`
	// Kind of the object, e.g. CephCluster.
	// +kubebuilder:validation:MinLength=1
	Kind string `json:"kind"`
	// Namespace of the object; empty for a cluster-scoped one.
	// +optional
	Namespace string `json:"namespace,omitempty"`
	// Name of the object.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
}

// RolloutGate is a condition on one object in the cluster. The operator needs permission to get
// the object; the chart grants what its gates.rules value lists.
type RolloutGate struct {
	// Name of the gate, used in conditions.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	Name string `json:"name"`
	// Object is what the gate asks about.
	Object GateObject `json:"object"`
	// Expression is a CEL expression over the object, as "object". The gate is open only while it
	// evaluates to true, e.g. object.status.ceph.health == "HEALTH_OK". An object that does not
	// exist, a field that is not there, or anything but a boolean closes the gate.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=1024
	Expression string `json:"expression"`
}

// Hook is a Job the operator runs and waits for.
type Hook struct {
	// Name of the hook, used in the Job's name and in conditions.
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]{0,22}[a-z0-9])?$`
	Name string `json:"name"`
	// Timeout is how long the Job may take. A Job that has not succeeded by then fails the hook,
	// which halts the operation. Defaults to spec.rollout.stepTimeout.
	// +optional
	Timeout *metav1.Duration `json:"timeout,omitempty"`
	// Job is a batch/v1 JobSpec. The operator creates the Job in the TalosCluster's namespace and
	// adds the environment variables TALOS_CLUSTER, TALOS_OPERATION, TALOS_OPERATION_ID,
	// TALOS_HOOK and, for a node's hooks, TALOS_MACHINE and TALOS_NODE to every container. It sets
	// restartPolicy Never and backoffLimit 0 where the spec leaves them open. The spec is not
	// validated before the Job is created: one the API server refuses fails the hook.
	// +kubebuilder:pruning:PreserveUnknownFields
	// +kubebuilder:validation:Type=object
	Job runtime.RawExtension `json:"job"`
}

// RolloutHooks are the Jobs of an operation. The hooks of a list run in order, one at a time. A
// hook may run more than once for the same node, after a retry, so it must be safe to repeat.
type RolloutHooks struct {
	// PreRollout runs after an operation took the lock, before its first node.
	// +kubebuilder:validation:MaxItems=8
	// +optional
	PreRollout []Hook `json:"preRollout,omitempty"`
	// PostRollout runs when an operation that ran its preRollout hooks ends, however it ends.
	// +kubebuilder:validation:MaxItems=8
	// +optional
	PostRollout []Hook `json:"postRollout,omitempty"`
	// PreNode runs when a node's turn comes, before anything is sent to it. A Kubernetes upgrade
	// takes no node out of the cluster and runs no node hooks.
	// +kubebuilder:validation:MaxItems=8
	// +optional
	PreNode []Hook `json:"preNode,omitempty"`
	// PostNode runs after a node was verified. A node that failed gets none.
	// +kubebuilder:validation:MaxItems=8
	// +optional
	PostNode []Hook `json:"postNode,omitempty"`
}

// CredentialsSource is where the operator's Talos API client certificate comes from.
// +kubebuilder:validation:Enum=TalosServiceAccount;Secret
type CredentialsSource string

const (
	// CredentialsSourceTalosServiceAccount: the operator creates a talos.dev ServiceAccount and Talos
	// issues a short-lived certificate for it, signed by the cluster's current CA. The control
	// planes must carry a KubeTalosAPIAccessConfig that allows os:admin for the TalosCluster's
	// namespace.
	CredentialsSourceTalosServiceAccount CredentialsSource = "TalosServiceAccount"
	// CredentialsSourceSecret: an admin talosconfig in a Secret, with node IPs as endpoints.
	CredentialsSourceSecret CredentialsSource = "Secret"
)

// Credentials selects the operator's Talos API identity.
// +kubebuilder:validation:XValidation:rule="(has(self.source) && self.source == 'Secret') == has(self.secretRef)",message="secretRef is required for source Secret and not allowed otherwise"
type Credentials struct {
	// Source is TalosServiceAccount (default) or Secret.
	// +kubebuilder:default=TalosServiceAccount
	// +optional
	Source CredentialsSource `json:"source,omitempty"`

	// SecretRef points to a Secret holding an admin talosconfig (key defaults to "talosconfig").
	// Only for source Secret.
	// +optional
	SecretRef *SecretKeyReference `json:"secretRef,omitempty"`
}

// TalosClusterSpec defines the desired state of TalosCluster.
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.clusterName) || (has(self.clusterName) && self.clusterName == oldSelf.clusterName)",message="clusterName is immutable once set"
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.controlPlaneEndpoint) || (has(self.controlPlaneEndpoint) && self.controlPlaneEndpoint == oldSelf.controlPlaneEndpoint)",message="controlPlaneEndpoint is immutable once set"
type TalosClusterSpec struct {
	// Management enables operator capabilities. Defaults to observe-only.
	// +kubebuilder:default={}
	// +optional
	Management Management `json:"management,omitempty"`

	// Credentials selects how the operator authenticates to the Talos API (docs/scope.md §9).
	// By default Talos issues the operator's certificate.
	// +kubebuilder:default={source: TalosServiceAccount}
	// +optional
	Credentials Credentials `json:"credentials,omitempty"`

	// SecretsRef points to a Secret holding the cluster secrets bundle (key defaults to "secrets.yaml";
	// the Secret defaults to "<name>-talos-secrets").
	// During adoption the operator writes it if absent and verifies it if present.
	// +optional
	SecretsRef *SecretKeyReference `json:"secretsRef,omitempty"`

	// ClusterName is the Talos/Kubernetes cluster name. Optional while observing; immutable once set.
	// +optional
	ClusterName string `json:"clusterName,omitempty"`

	// ControlPlaneEndpoint is the Kubernetes API endpoint. Optional while observing; immutable once set.
	// +kubebuilder:validation:Pattern=`^https://`
	// +optional
	ControlPlaneEndpoint string `json:"controlPlaneEndpoint,omitempty"`

	// Versions are the desired Talos and Kubernetes versions.
	// +optional
	Versions *Versions `json:"versions,omitempty"`

	// ConfigGeneration holds the base config generation inputs.
	// +optional
	ConfigGeneration *ConfigGeneration `json:"configGeneration,omitempty"`

	// Config is the cluster-wide layer, applied to every machine before its pool's and its own.
	// +kubebuilder:validation:MaxItems=32
	// +optional
	Config []ConfigSource `json:"config,omitempty"`

	// Rollout tunes how config changes are applied once config is Manage.
	// +kubebuilder:default={}
	// +optional
	Rollout Rollout `json:"rollout,omitempty"`

	// Paused stops all mutating actions immediately.
	// +optional
	Paused bool `json:"paused,omitempty"`
}

// ClusterPhase summarises where the cluster is in its lifecycle. It only moves forward
// (docs/scope.md §12.14): while Adopting, the cluster describes the resources and the operator
// writes them; from Adopted on the resources are the user's; once Managed they describe the
// cluster. Degraded is Managed with something in the way.
// +kubebuilder:validation:Enum=Adopting;Adopted;Managed;Degraded
type ClusterPhase string

const (
	ClusterPhaseAdopting ClusterPhase = "Adopting"
	ClusterPhaseAdopted  ClusterPhase = "Adopted"
	ClusterPhaseManaged  ClusterPhase = "Managed"
	ClusterPhaseDegraded ClusterPhase = "Degraded"
)

// EtcdMember is one observed etcd member.
type EtcdMember struct {
	// ID is the etcd member ID in hex.
	ID string `json:"id"`
	// Hostname of the member.
	Hostname string `json:"hostname"`
	// IsLearner is true while the member is not yet a voter.
	// +optional
	IsLearner bool `json:"isLearner,omitempty"`
}

// ObservedState is what the operator last observed on the live cluster.
type ObservedState struct {
	// TalosVersions counts nodes per running Talos version.
	// +optional
	TalosVersions map[string]int32 `json:"talosVersions,omitempty"`

	// KubernetesVersion is the lowest control-plane Kubernetes version observed.
	// +optional
	KubernetesVersion string `json:"kubernetesVersion,omitempty"`

	// EtcdMembers is the observed etcd membership.
	// +listType=map
	// +listMapKey=id
	// +optional
	EtcdMembers []EtcdMember `json:"etcdMembers,omitempty"`

	// ObservedAt is when this observation completed.
	// +optional
	ObservedAt *metav1.Time `json:"observedAt,omitempty"`
}

// Precondition is one named adoption precondition.
type Precondition struct {
	// Name of the precondition, e.g. EtcdHealthy.
	Name string `json:"name"`
	// Status is True, False or Unknown.
	Status metav1.ConditionStatus `json:"status"`
	// Message explains a False or Unknown status.
	// +optional
	Message string `json:"message,omitempty"`
}

// AdoptionStatus tracks the adoption trust ladder (docs/scope.md §7.1).
type AdoptionStatus struct {
	// Preconditions that must hold before adoption can be proven.
	// +listType=map
	// +listMapKey=name
	// +optional
	Preconditions []Precondition `json:"preconditions,omitempty"`

	// AdoptedAt is when the cluster was first proven. From then on the operator never writes the
	// TalosCluster's spec, a TalosMachinePool or a TalosMachine again; nothing unsets it.
	// +optional
	AdoptedAt *metav1.Time `json:"adoptedAt,omitempty"`

	// Source fingerprints the live state the resources were last derived from, so they are
	// derived again only when the cluster changes.
	// +optional
	Source string `json:"source,omitempty"`

	// Notes are facts about the live cluster worth knowing when taking the resources over, such
	// as a node whose Talos version differs from the one its config installs.
	// +optional
	Notes []string `json:"notes,omitempty"`

	// UnmatchedMembers lists live members (by hostname) that no TalosMachine claims.
	// +optional
	UnmatchedMembers []string `json:"unmatchedMembers,omitempty"`

	// LiveConfigHashes maps TalosMachine name to the hash of the live config proven at adoption.
	// +optional
	LiveConfigHashes map[string]string `json:"liveConfigHashes,omitempty"`

	// ProvenAt is when the no-changes proof last succeeded.
	// +optional
	ProvenAt *metav1.Time `json:"provenAt,omitempty"`
}

// UpgradeHolds maps machines to installer images a human took out of an upgrade. An upgrade to
// such an image is not started again for that machine; one to any other image is.
type UpgradeHolds struct {
	// Skipped: the machine was skipped after its upgrade failed.
	// +optional
	Skipped map[string]string `json:"skipped,omitempty"`
	// Aborted: the upgrade was aborted before it reached the machine, or while it was at it.
	// +optional
	Aborted map[string]string `json:"aborted,omitempty"`
	// Kubernetes is the Kubernetes version whose upgrade a human aborted. It is not started again;
	// an upgrade to any other version is (docs/scope.md §7.4).
	// +optional
	Kubernetes string `json:"kubernetes,omitempty"`
}

// MembershipHolds records what a human decided about machines that were to be added or removed.
type MembershipHolds struct {
	// Add maps a machine to the config it was to be handed when a human skipped it, or aborted
	// the operation that was adding it. It is not added with that config again; with any other
	// it is.
	// +optional
	Add map[string]string `json:"add,omitempty"`
	// Remove lists the machines whose removal a human skipped or aborted. They are not removed
	// until their spec asks for it anew: desiredState back to Present, and to Removed again.
	// +listType=set
	// +optional
	Remove []string `json:"remove,omitempty"`
}

// VersionStep is one upgrade on the way from the versions a cluster runs to the versions its spec
// asks for.
type VersionStep struct {
	// Kind is Talos or Kubernetes.
	// +kubebuilder:validation:Enum=Talos;Kubernetes
	Kind string `json:"kind"`
	// From and To are the versions the step leaves and reaches.
	From string `json:"from"`
	To   string `json:"to"`
}

// AvailableVersions is what exists upstream, as far as the operator could find out. It is
// information: nothing is upgraded to a version because it is listed here.
type AvailableVersions struct {
	// Talos lists the newest release of each Talos minor that this operator supports and that is
	// not older than what the cluster runs, as Image Factory offers them.
	// +optional
	Talos []string `json:"talos,omitempty"`
	// Kubernetes lists the newest release of each Kubernetes minor from the one the cluster runs
	// to the newest that one of the Talos releases above runs.
	// +optional
	Kubernetes []string `json:"kubernetes,omitempty"`
	// Problem says why a list is missing or may be out of date: a source that was not reached.
	// +optional
	Problem string `json:"problem,omitempty"`
}

// VersionsStatus is what the operator knows about versions (docs/scope.md §7.5).
type VersionsStatus struct {
	// Talos and Kubernetes are the versions the cluster runs: the lowest among its nodes.
	// +optional
	Talos string `json:"talos,omitempty"`
	// +optional
	Kubernetes string `json:"kubernetes,omitempty"`
	// Plan is the sequence of upgrades that leads from those to the versions the spec asks for.
	// Empty when the cluster runs them, and cut short where the condition VersionsReachable says
	// there is no way further.
	// +optional
	Plan []VersionStep `json:"plan,omitempty"`
	// Available is what exists upstream.
	// +optional
	Available *AvailableVersions `json:"available,omitempty"`
}

// OperationType is the kind of disruptive operation holding the cluster lock.
// +kubebuilder:validation:Enum=ConfigRollout;TalosUpgrade;KubernetesUpgrade;AddMachine;RemoveMachine
type OperationType string

const (
	// OperationConfigRollout applies rendered machine config, one node at a time (docs/scope.md §6.3).
	OperationConfigRollout OperationType = "ConfigRollout"
	// OperationTalosUpgrade installs another Talos version or installer, one node at a time, with a
	// drain and a reboot each (docs/scope.md §7.3).
	OperationTalosUpgrade OperationType = "TalosUpgrade"
	// OperationKubernetesUpgrade brings the Kubernetes components Talos runs to another version,
	// one component on one node at a time (docs/scope.md §7.4).
	OperationKubernetesUpgrade OperationType = "KubernetesUpgrade"
	// OperationAddMachine gives machines that wait in maintenance mode their config, one at a
	// time, and sees each join the cluster (docs/scope.md §7.6).
	OperationAddMachine OperationType = "AddMachine"
	// OperationRemoveMachine takes machines out of the cluster, one at a time: drained, out of
	// etcd, reset, and their Node deleted (docs/scope.md §7.7).
	OperationRemoveMachine OperationType = "RemoveMachine"
)

// Stages at which hooks run (docs/scope.md §6.4).
const (
	HookPreRollout  = "preRollout"
	HookPostRollout = "postRollout"
	HookPreNode     = "preNode"
	HookPostNode    = "postNode"
)

// HookRun is the hook of an operation that is running. It is persisted before its Job is created.
type HookRun struct {
	// Stage is preRollout, postRollout, preNode or postNode.
	Stage string `json:"stage"`
	// Index is the hook's position in its list, and Name its name.
	Index int32  `json:"index"`
	Name  string `json:"name"`
	// Machine is the machine whose turn a node hook belongs to.
	// +optional
	Machine string `json:"machine,omitempty"`
	// Job is the name of the hook's Job.
	Job string `json:"job"`
	// Attempt counts runs of this hook, starting at 1; retry increments it and so names a new Job.
	Attempt int32 `json:"attempt"`
	// Since is when the hook was started.
	Since metav1.Time `json:"since"`
}

// OperationEnding is how an operation ends once its postRollout hooks ran.
type OperationEnding struct {
	// Outcome is Succeeded, Aborted or Superseded.
	Outcome string `json:"outcome"`
	// +optional
	Message string `json:"message,omitempty"`
}

// Outcomes of a finished operation.
const (
	// OperationSucceeded: every planned machine was done or skipped.
	OperationSucceeded = "Succeeded"
	// OperationAborted: a human aborted it after a failure, or config management returned to Observe.
	OperationAborted = "Aborted"
	// OperationSuperseded: what a machine renders or runs changed before its turn, so the rest was
	// planned afresh.
	OperationSuperseded = "Superseded"
)

// OperationStep is the persisted position of an in-flight operation.
// It is written before the step's side effect so another replica can resume (docs/scope.md §4.2).
type OperationStep struct {
	// Machine is the TalosMachine being acted on.
	Machine string `json:"machine"`
	// Step is the step name, e.g. Trying or Verifying.
	Step string `json:"step"`
	// Class is the change class that selected the node's apply strategy (docs/scope.md §6.3).
	// +optional
	Class string `json:"class,omitempty"`
	// PreviousHash is the node's config hash before the step; a reverted try returns to it.
	// +optional
	PreviousHash string `json:"previousHash,omitempty"`
	// DesiredHash is the hash of the config being applied.
	// +optional
	DesiredHash string `json:"desiredHash,omitempty"`
	// HealthySince is when every verification check first passed without interruption; the
	// soak is measured from it.
	// +optional
	HealthySince *metav1.Time `json:"healthySince,omitempty"`
	// Attempt counts tries of this machine's step, starting at 1; retry increments it.
	// +optional
	Attempt int32 `json:"attempt,omitempty"`
	// PreRebootBootID is recorded before a reboot so a resumed step can tell whether it happened.
	// +optional
	PreRebootBootID string `json:"preRebootBootID,omitempty"`
	// Image is the image an upgrade step installs: the installer of a Talos upgrade, or the
	// component's image of a Kubernetes upgrade.
	// +optional
	Image string `json:"image,omitempty"`
	// Component is the Kubernetes component a step of a Kubernetes upgrade changes.
	// +optional
	Component string `json:"component,omitempty"`
	// Cordoned is set before the operator cordons the node, so that it later uncordons a node it
	// cordoned itself and leaves alone one that someone else did.
	// +optional
	Cordoned bool `json:"cordoned,omitempty"`
	// Deadline is the step's timeout, e.g. the end of a TRY window.
	// +optional
	Deadline *metav1.Time `json:"deadline,omitempty"`
	// Dead is set when a human confirmed that the machine being removed is gone for good: it is
	// taken out of etcd and Kubernetes from the outside, and never asked anything again.
	// +optional
	Dead bool `json:"dead,omitempty"`
	// EtcdMember is the etcd member ID, in hex, that a control plane had when its removal began:
	// what has to be gone from the member list before the removal is done.
	// +optional
	EtcdMember string `json:"etcdMember,omitempty"`
	// Since is when the step started.
	Since metav1.Time `json:"since"`
}

// OperationFailure halts an operation until a human acts (docs/scope.md §8.3).
type OperationFailure struct {
	// Machine where the failure happened.
	// +optional
	Machine string `json:"machine,omitempty"`
	// Step that failed.
	Step string `json:"step"`
	// Message describes the failure.
	Message string `json:"message"`
	// Remediation suggests the next human action.
	// +optional
	Remediation string `json:"remediation,omitempty"`
	// Reason is a machine-readable cause, e.g. RolledBack or NodeUnreachableAfterRollback.
	// +optional
	Reason string `json:"reason,omitempty"`
	// Hook is the stage of the hook that failed, if a hook did.
	// +optional
	Hook string `json:"hook,omitempty"`
	// Token identifies this failure. The retry, skip-machine and abort annotations must carry it,
	// so an annotation left behind never acts on a later failure.
	// +optional
	Token string `json:"token,omitempty"`
	// At is when the failure was recorded.
	// +optional
	At *metav1.Time `json:"at,omitempty"`
}

// Operation is the cluster-wide disruption lock: at most one exists at a time.
type Operation struct {
	// ID uniquely identifies the operation.
	ID string `json:"id"`
	// Type of operation.
	Type OperationType `json:"type"`
	// Target is the operation's goal, e.g. a version.
	// +optional
	Target string `json:"target,omitempty"`
	// Plan lists the operation's turns in execution order: machines, or for a Kubernetes upgrade
	// one component on one machine each, as "<component>/<machine>".
	// +optional
	Plan []string `json:"plan,omitempty"`
	// Targets maps each planned machine to the rendered config hash it is to reach, and Bases to
	// the live config hash it had when the operation was planned. An operation applies exactly
	// these pairs. A machine that renders, or runs, something else by the time its turn comes ends
	// the operation, so the new change is planned, classified and approved afresh.
	// +optional
	Targets map[string]string `json:"targets,omitempty"`
	// +optional
	Bases map[string]string `json:"bases,omitempty"`
	// Current is the in-flight step.
	// +optional
	Current *OperationStep `json:"current,omitempty"`
	// Completed lists machines already done.
	// +optional
	Completed []string `json:"completed,omitempty"`
	// Skipped lists machines a human skipped after a failure.
	// +optional
	Skipped []string `json:"skipped,omitempty"`
	// Failure is set when the operation is halted.
	// +optional
	Failure *OperationFailure `json:"failure,omitempty"`
	// Hook is the hook that is running, if one is (docs/scope.md §6.4).
	// +optional
	Hook *HookRun `json:"hook,omitempty"`
	// Announced is set once the preRollout hooks ran, or there were none: nodes may begin.
	// +optional
	Announced bool `json:"announced,omitempty"`
	// Prepared is the turn whose preNode hooks ran, and Finishing the turn whose postNode hooks
	// are due: it is verified, and counts as completed once they ran.
	// +optional
	Prepared string `json:"prepared,omitempty"`
	// +optional
	Finishing string `json:"finishing,omitempty"`
	// Ending is set when the operation is over and only its postRollout hooks remain.
	// +optional
	Ending *OperationEnding `json:"ending,omitempty"`
	// StartedAt is when the operation began.
	StartedAt metav1.Time `json:"startedAt"`
}

// PendingApproval is a change that waits for a human (docs/scope.md §6.3).
type PendingApproval struct {
	// Hash identifies exactly this change: the machines and their current and rendered config
	// hashes. Annotate the TalosCluster with talos.jarensaa.github.io/approve-diff=<hash> to approve
	// it; the approval cannot be replayed onto a different change.
	Hash string `json:"hash"`
	// Machines lists the machines whose change needs approval, with the reason. Each machine's
	// status.config.diff shows its redacted diff.
	// +optional
	Machines []string `json:"machines,omitempty"`
}

// OperationRecord is a finished operation, kept for the record.
type OperationRecord struct {
	ID   string        `json:"id"`
	Type OperationType `json:"type"`
	// Outcome is Succeeded, Aborted or Superseded.
	Outcome    string      `json:"outcome"`
	StartedAt  metav1.Time `json:"startedAt"`
	FinishedAt metav1.Time `json:"finishedAt"`
	// +optional
	Completed []string `json:"completed,omitempty"`
	// +optional
	Skipped []string `json:"skipped,omitempty"`
	// +optional
	Message string `json:"message,omitempty"`
}

// TalosClusterStatus defines the observed state of TalosCluster.
type TalosClusterStatus struct {
	// ObservedGeneration is the spec generation the status reflects.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Phase summarises the lifecycle position.
	// +optional
	Phase ClusterPhase `json:"phase,omitempty"`

	// Conditions represent the latest observations of the cluster.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Observed is the last observed live state.
	// +optional
	Observed *ObservedState `json:"observed,omitempty"`

	// Adoption tracks adoption progress.
	// +optional
	Adoption *AdoptionStatus `json:"adoption,omitempty"`

	// Operation is the in-flight disruptive operation, if any.
	// +optional
	Operation *Operation `json:"operation,omitempty"`

	// Approval is the change waiting for a human's approval, if any.
	// +optional
	Approval *PendingApproval `json:"approval,omitempty"`

	// AbortedTargets is what the operation a human aborted was rolling out: machine name to
	// rendered config hash. Those configs are not rolled out to those machines again; a spec that
	// renders anything else, a revert included, rolls out and clears this.
	// +optional
	AbortedTargets map[string]string `json:"abortedTargets,omitempty"`

	// Upgrades records what a human decided about Talos upgrades that were due.
	// +optional
	Upgrades *UpgradeHolds `json:"upgrades,omitempty"`
	// Versions is what the cluster runs, what exists, and the way to what the spec asks for.
	// +optional
	Versions *VersionsStatus `json:"versions,omitempty"`
	// Membership records what a human decided about machines that were to be added or removed.
	// +optional
	Membership *MembershipHolds `json:"membership,omitempty"`

	// History holds the most recent finished operations, newest first.
	// +kubebuilder:validation:MaxItems=10
	// +optional
	History []OperationRecord `json:"history,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=tc
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Talos",type=string,JSONPath=`.spec.versions.talos`
// +kubebuilder:printcolumn:name="Kubernetes",type=string,JSONPath=`.status.observed.kubernetesVersion`
// +kubebuilder:printcolumn:name="Proven",type=string,JSONPath=`.status.conditions[?(@.type=="ConfigProven")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// TalosCluster is a Talos Linux cluster managed (or observed) by the operator.
type TalosCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   TalosClusterSpec   `json:"spec"`
	Status TalosClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// TalosClusterList contains a list of TalosCluster.
type TalosClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []TalosCluster `json:"items"`
}

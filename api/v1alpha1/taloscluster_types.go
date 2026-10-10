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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

// ClusterPhase summarises where the cluster is in its lifecycle.
// +kubebuilder:validation:Enum=Observing;Adopting;Adopted;Managed;Degraded
type ClusterPhase string

const (
	ClusterPhaseObserving ClusterPhase = "Observing"
	ClusterPhaseAdopting  ClusterPhase = "Adopting"
	ClusterPhaseAdopted   ClusterPhase = "Adopted"
	ClusterPhaseManaged   ClusterPhase = "Managed"
	ClusterPhaseDegraded  ClusterPhase = "Degraded"
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

	// ProposalRef names the ConfigMap holding the proposed TalosCluster and TalosMachine specs.
	// +optional
	ProposalRef *corev1.LocalObjectReference `json:"proposalRef,omitempty"`

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

// OperationType is the kind of disruptive operation holding the cluster lock.
// +kubebuilder:validation:Enum=ConfigRollout;TalosUpgrade;KubernetesUpgrade;AddControlPlane;RemoveMachine;Bootstrap
type OperationType string

// OperationConfigRollout applies rendered machine config, one node at a time (docs/scope.md §6.3).
const OperationConfigRollout OperationType = "ConfigRollout"

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
	// Deadline is the step's timeout, e.g. the end of a TRY window.
	// +optional
	Deadline *metav1.Time `json:"deadline,omitempty"`
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
	// Plan lists machines in execution order.
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
// +kubebuilder:printcolumn:name="Adopted",type=string,JSONPath=`.status.conditions[?(@.type=="Adopted")].status`
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

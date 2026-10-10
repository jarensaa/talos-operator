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

// MachineRole is the Talos machine type.
// +kubebuilder:validation:Enum=ControlPlane;Worker
type MachineRole string

const (
	MachineRoleControlPlane MachineRole = "ControlPlane"
	MachineRoleWorker       MachineRole = "Worker"
)

// DesiredState says whether the machine should be part of the cluster.
// Deleting a TalosMachine never wipes a node; removal is requested with Removed (docs/scope.md §7.7).
// +kubebuilder:validation:Enum=Present;Removed
type DesiredState string

const (
	DesiredStatePresent DesiredState = "Present"
	DesiredStateRemoved DesiredState = "Removed"
)

// MachineIdentity pins the hardware a TalosMachine refers to.
type MachineIdentity struct {
	// SystemUUID is the SMBIOS system UUID (talosctl get systeminformation).
	// Config is never applied to a machine whose UUID does not match.
	// +kubebuilder:validation:Pattern=`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`
	SystemUUID string `json:"systemUUID"`
}

// TalosMachineSpec defines the desired state of TalosMachine.
type TalosMachineSpec struct {
	// PoolRef names the TalosMachinePool in the same namespace. The pool decides the machine's
	// cluster and role. It may change to another pool of the same role.
	// +kubebuilder:validation:XValidation:rule="size(self.name) > 0",message="poolRef.name is required"
	PoolRef corev1.LocalObjectReference `json:"poolRef"`

	// Address is the machine's Talos API IP address. Hostnames are rejected: endpoints must be IPs.
	// +kubebuilder:validation:XValidation:rule="isIP(self)",message="address must be an IP address"
	Address string `json:"address"`

	// Identity pins the machine's hardware.
	Identity MachineIdentity `json:"identity"`

	// Config is the machine's own layer, applied after its cluster's and its pool's. It holds only
	// what this node has that its pool does not, such as a disk WWID.
	// +kubebuilder:validation:MaxItems=32
	// +optional
	Config []ConfigSource `json:"config,omitempty"`

	// DesiredState is Present or Removed.
	// +kubebuilder:default=Present
	// +optional
	DesiredState DesiredState `json:"desiredState,omitempty"`

	// Hold excludes the machine from rollouts.
	// +optional
	Hold bool `json:"hold,omitempty"`
}

// MachinePhase summarises the machine's lifecycle position.
// +kubebuilder:validation:Enum=Pending;AwaitingIdentity;Provisioning;Joining;Ready;Updating;Draining;Rebooting;Removing;Failed
type MachinePhase string

// ObservedIdentity is the identity read from the live node.
type ObservedIdentity struct {
	// +optional
	SystemUUID string `json:"systemUUID,omitempty"`
	// NodeID is the Talos discovery node ID (reset on wipe).
	// +optional
	NodeID string `json:"nodeID,omitempty"`
	// +optional
	Hostname string `json:"hostname,omitempty"`
	// +optional
	KubernetesNodeName string `json:"kubernetesNodeName,omitempty"`
}

// TalosState is the observed Talos runtime state.
type TalosState struct {
	// Version and SchematicID keep their last known value when they cannot be read; the
	// NodeObserved condition says whether they were.
	// +optional
	Version string `json:"version,omitempty"`
	// +optional
	SchematicID string `json:"schematicID,omitempty"`
	// BootID changes on every reboot. It is empty unless it was read in the last pass, so a node
	// that is rebooting never shows the boot it left.
	// +optional
	BootID string `json:"bootID,omitempty"`
	// Stage is the Talos machine stage, e.g. running. Empty unless read in the last pass.
	// +optional
	Stage string `json:"stage,omitempty"`
	// Ready is true only if the node reported itself ready in the last pass.
	// +optional
	Ready bool `json:"ready,omitempty"`
}

// KubeletState is the observed kubelet state.
type KubeletState struct {
	// +optional
	Version string `json:"version,omitempty"`
}

// ConfigState tracks rendered, applied and live config hashes.
type ConfigState struct {
	// DesiredHash fingerprints the config rendered from the spec. Hashes are taken over the
	// normalized config (per document, without comments, independent of document order), so
	// DesiredHash equals LiveHash exactly when ConfigMatchesLive is True.
	// +optional
	DesiredHash string `json:"desiredHash,omitempty"`
	// AppliedHash is the config the operator last put on the node, or the live config it took
	// over when management began. DesiredHash differing from it means the spec changed and a
	// rollout is due; LiveHash differing from it means the node drifted.
	// +optional
	AppliedHash string `json:"appliedHash,omitempty"`
	// SkippedHash is a rendered config a human skipped for this machine after a failed rollout.
	// The same config is not rolled out to it again.
	// +optional
	SkippedHash string `json:"skippedHash,omitempty"`
	// ChangeClass classifies the pending change from live to rendered config, e.g. LiveSafe or
	// Connectivity (docs/scope.md §6.3). Empty when they are equal.
	// +optional
	ChangeClass string `json:"changeClass,omitempty"`
	// ChangeReasons names what put the change in its class, e.g. the documents or fields.
	// +kubebuilder:validation:MaxItems=16
	// +optional
	ChangeReasons []string `json:"changeReasons,omitempty"`
	// LiveHash fingerprints the node's active config.
	// +optional
	LiveHash string `json:"liveHash,omitempty"`
	// Diff is the redacted per-document diff from the live config (a/) to the rendered one (b/),
	// truncated to fit. Documents set by a patch from a Secret are named but not shown.
	// +kubebuilder:validation:MaxLength=16384
	// +optional
	Diff string `json:"diff,omitempty"`
}

// EtcdState is the machine's observed etcd membership.
type EtcdState struct {
	// +optional
	MemberID string `json:"memberID,omitempty"`
	// +optional
	IsLearner bool `json:"isLearner,omitempty"`
}

// TalosMachineStatus defines the observed state of TalosMachine.
type TalosMachineStatus struct {
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// +optional
	Phase MachinePhase `json:"phase,omitempty"`
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
	// +optional
	Identity *ObservedIdentity `json:"identity,omitempty"`
	// +optional
	Talos *TalosState `json:"talos,omitempty"`
	// +optional
	Kubelet *KubeletState `json:"kubelet,omitempty"`
	// +optional
	Config *ConfigState `json:"config,omitempty"`
	// +optional
	Etcd *EtcdState `json:"etcd,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=tm
// +kubebuilder:printcolumn:name="Pool",type=string,JSONPath=`.spec.poolRef.name`
// +kubebuilder:printcolumn:name="Address",type=string,JSONPath=`.spec.address`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Talos",type=string,JSONPath=`.status.talos.version`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// TalosMachine is one Talos node, a member of one TalosMachinePool.
type TalosMachine struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   TalosMachineSpec   `json:"spec"`
	Status TalosMachineStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// TalosMachineList contains a list of TalosMachine.
type TalosMachineList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []TalosMachine `json:"items"`
}

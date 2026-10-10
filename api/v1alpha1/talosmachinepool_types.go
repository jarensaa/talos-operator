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

// PoolInstaller overrides the cluster's installer for a pool.
type PoolInstaller struct {
	// SchematicID is the Image Factory schematic ID (64 hex characters), e.g. for a pool that needs
	// other system extensions.
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{64}$`
	// +optional
	SchematicID string `json:"schematicID,omitempty"`
}

// TalosMachinePoolSpec defines the desired state of TalosMachinePool.
type TalosMachinePoolSpec struct {
	// ClusterRef names the TalosCluster in the same namespace.
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="clusterRef is immutable"
	ClusterRef corev1.LocalObjectReference `json:"clusterRef"`

	// Role is ControlPlane or Worker. It selects the generation template for every member.
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="role is immutable"
	Role MachineRole `json:"role"`

	// Installer overrides the cluster's installer for every member.
	// +optional
	Installer *PoolInstaller `json:"installer,omitempty"`

	// Config is the pool layer, applied after the cluster's and before each member's own.
	// +kubebuilder:validation:MaxItems=32
	// +optional
	Config []ConfigSource `json:"config,omitempty"`
}

// TalosMachinePoolStatus defines the observed state of TalosMachinePool.
type TalosMachinePoolStatus struct {
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// Members is the number of TalosMachines in the pool.
	// +optional
	Members int32 `json:"members"`
	// Matched is the number of members bound to their live node.
	// +optional
	Matched int32 `json:"matched"`
	// ConfigMatchesLive is the number of members whose rendered config equals the live one.
	// +optional
	ConfigMatchesLive int32 `json:"configMatchesLive"`
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=tpool
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.clusterRef.name`
// +kubebuilder:printcolumn:name="Role",type=string,JSONPath=`.spec.role`
// +kubebuilder:printcolumn:name="Members",type=integer,JSONPath=`.status.members`
// +kubebuilder:printcolumn:name="Matched",type=integer,JSONPath=`.status.matched`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// TalosMachinePool is a set of machines of one role that share config (docs/scope.md §5.2).
type TalosMachinePool struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   TalosMachinePoolSpec   `json:"spec"`
	Status TalosMachinePoolStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// TalosMachinePoolList contains a list of TalosMachinePool.
type TalosMachinePoolList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []TalosMachinePool `json:"items"`
}

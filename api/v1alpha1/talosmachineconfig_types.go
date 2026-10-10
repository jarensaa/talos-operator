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
)

// TalosMachineConfigSpec defines the desired state of TalosMachineConfig.
type TalosMachineConfigSpec struct {
	// Documents are config documents (multi-document YAML), as in an inline config source. A
	// TalosMachineConfig cannot reference others, so resolution stays one level deep. Keep
	// sensitive values in a Secret and reference it with secretKeyRef where it is used.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=262144
	Documents string `json:"documents"`
}

// ConfigUsage is what references a TalosMachineConfig: the blast radius of an edit.
type ConfigUsage struct {
	// Clusters lists TalosClusters whose cluster-wide layer references it.
	// +optional
	Clusters []string `json:"clusters,omitempty"`
	// Pools lists TalosMachinePools whose layer references it.
	// +optional
	Pools []string `json:"pools,omitempty"`
	// Machines is the number of TalosMachines whose rendered config includes it, directly or
	// through their pool or cluster.
	// +optional
	Machines int32 `json:"machines"`
}

// TalosMachineConfigStatus defines the observed state of TalosMachineConfig.
type TalosMachineConfigStatus struct {
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
	// DocumentKeys lists the documents it sets, e.g. v1alpha1/BondConfig/bond0.
	// +optional
	DocumentKeys []string `json:"documentKeys,omitempty"`
	// UsedBy is what references it.
	// +optional
	UsedBy *ConfigUsage `json:"usedBy,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=tmc
// +kubebuilder:printcolumn:name="Valid",type=string,JSONPath=`.status.conditions[?(@.type=="Valid")].status`
// +kubebuilder:printcolumn:name="Machines",type=integer,JSONPath=`.status.usedBy.machines`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// TalosMachineConfig is a set of reusable config documents that any TalosCluster, TalosMachinePool
// or TalosMachine in the namespace can reference (docs/scope.md §5.3).
type TalosMachineConfig struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   TalosMachineConfigSpec   `json:"spec"`
	Status TalosMachineConfigStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// TalosMachineConfigList contains a list of TalosMachineConfig.
type TalosMachineConfigList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []TalosMachineConfig `json:"items"`
}

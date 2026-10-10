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

// SecretKeyReference refers to a key in a Secret in the same namespace as the referencing object.
type SecretKeyReference struct {
	// Name of the Secret.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Key within the Secret. Each referencing field documents its default.
	// +optional
	Key string `json:"key,omitempty"`
}

// ConfigReference names a TalosMachineConfig in the same namespace as the referencing object.
type ConfigReference struct {
	// Name of the TalosMachineConfig.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
}

// ConfigSource is one entry of a config layer: config documents (multi-document YAML) merged onto
// the generated config with strategic merge (docs/scope.md §5). Documents of the multi-document
// kinds are declared whole; the legacy v1alpha1 document is a partial overlay. JSON6902 patches are
// refused: they cannot address multi-document configs. Exactly one source is set, and a layer
// holds at most 32 entries (MaxItems) so the CEL validation cost stays within the API server
// budget.
// +kubebuilder:validation:XValidation:rule="(has(self.inline) ? 1 : 0) + (has(self.configRef) ? 1 : 0) + (has(self.secretKeyRef) ? 1 : 0) == 1",message="exactly one of inline, configRef or secretKeyRef must be set"
// +kubebuilder:validation:XValidation:rule="!has(self.secretKeyRef) || has(self.secretKeyRef.key)",message="secretKeyRef.key is required for config"
type ConfigSource struct {
	// Inline config documents.
	// +kubebuilder:validation:MaxLength=262144
	// +optional
	Inline string `json:"inline,omitempty"`

	// ConfigRef uses the documents of a TalosMachineConfig, so they can be shared.
	// +optional
	ConfigRef *ConfigReference `json:"configRef,omitempty"`

	// SecretKeyRef reads documents from a Secret key, for documents that carry sensitive values.
	// The key is required. Diffs of the documents it sets are withheld from status.
	// +optional
	SecretKeyRef *SecretKeyReference `json:"secretKeyRef,omitempty"`
}

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

const (
	// DefaultSecretsKey is the key of the secrets bundle in its Secret.
	DefaultSecretsKey = "secrets.yaml"
	// DefaultTalosconfigKey is the key of a talosconfig in a credentials Secret.
	DefaultTalosconfigKey = "talosconfig"
	// TalosServiceAccountKey is the key under which Talos stores an issued talosconfig
	// (constants.TalosconfigFilename).
	TalosServiceAccountKey = "config"
)

// SecretsReference returns spec.secretsRef with defaults applied: the Secret is named
// "<cluster>-talos-secrets" and the key is DefaultSecretsKey.
func (tc *TalosCluster) SecretsReference() SecretKeyReference {
	ref := SecretKeyReference{Name: tc.Name + "-talos-secrets", Key: DefaultSecretsKey}

	if tc.Spec.SecretsRef != nil {
		ref.Name = tc.Spec.SecretsRef.Name
		if tc.Spec.SecretsRef.Key != "" {
			ref.Key = tc.Spec.SecretsRef.Key
		}
	}

	return ref
}

// TalosServiceAccountName names the talos.dev ServiceAccount the operator requests its Talos
// credentials through. Talos names the Secret it issues the same.
func (tc *TalosCluster) TalosServiceAccountName() string {
	return tc.Name + "-talos-operator"
}

// CredentialsSecret returns the Secret and key holding the operator's talosconfig.
func (tc *TalosCluster) CredentialsSecret() SecretKeyReference {
	if tc.Spec.Credentials.Source == CredentialsSourceSecret && tc.Spec.Credentials.SecretRef != nil {
		ref := *tc.Spec.Credentials.SecretRef
		if ref.Key == "" {
			ref.Key = DefaultTalosconfigKey
		}

		return ref
	}

	return SecretKeyReference{Name: tc.TalosServiceAccountName(), Key: TalosServiceAccountKey}
}

// UsesTalosServiceAccount reports whether Talos issues the operator's credentials. An empty
// source, as in objects that bypassed API defaulting, means the default.
func (tc *TalosCluster) UsesTalosServiceAccount() bool {
	return tc.Spec.Credentials.Source != CredentialsSourceSecret
}

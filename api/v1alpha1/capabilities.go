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

// What spec.management lets the operator do (docs/scope.md §8.1, invariant 6). Everything that
// asks whether a capability is enabled asks here, so the answer is the same wherever it is asked.
//
// Every capability beyond config builds on an owned config: the CRD refuses to enable one without
// config: Manage, and these say so again for an object that never passed that validation. An
// empty field, as in an object that bypassed API defaulting, is Observe or Disabled.

// ManagesConfig reports whether the operator owns the cluster's machine config and may apply it:
// config is Manage. While it is not, the operator only observes.
func (m Management) ManagesConfig() bool {
	return m.Config == ConfigManagementManage
}

// MayUpgrade reports whether the operator may upgrade Talos and Kubernetes, which reboots nodes:
// upgrades is Enabled and config is Manage.
func (m Management) MayUpgrade() bool {
	return m.ManagesConfig() && m.Upgrades == CapabilityEnabled
}

// MayChangeMembership reports whether the operator may add machines to the cluster and remove
// them from it: membership is Enabled and config is Manage.
func (m Management) MayChangeMembership() bool {
	return m.ManagesConfig() && m.Membership == CapabilityEnabled
}

// MayMaintain reports whether the operator may do the upkeep it does by itself, defragmenting
// etcd: maintenance is Enabled and config is Manage.
func (m Management) MayMaintain() bool {
	return m.ManagesConfig() && m.Maintenance == CapabilityEnabled
}

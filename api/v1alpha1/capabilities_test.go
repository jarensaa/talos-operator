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

package v1alpha1_test

import (
	"testing"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
)

// A capability is enabled by its own field and by nothing else, and none beyond config is
// enabled on a cluster whose config the operator does not own.
func TestManagementCapabilities(t *testing.T) {
	const (
		manage   = talosv1alpha1.ConfigManagementManage
		observe  = talosv1alpha1.ConfigManagementObserve
		enabled  = talosv1alpha1.CapabilityEnabled
		disabled = talosv1alpha1.CapabilityDisabled
	)

	type answers struct{ config, upgrade, membership, maintain bool }

	for _, tt := range []struct {
		name string
		m    talosv1alpha1.Management
		want answers
	}{
		{"nothing set", talosv1alpha1.Management{}, answers{}},
		{"observing", talosv1alpha1.Management{Config: observe}, answers{}},
		{"managing config only", talosv1alpha1.Management{Config: manage}, answers{config: true}},
		{"everything disabled by name", talosv1alpha1.Management{Config: manage, Upgrades: disabled, Membership: disabled, Maintenance: disabled},
			answers{config: true}},
		{"upgrades", talosv1alpha1.Management{Config: manage, Upgrades: enabled}, answers{config: true, upgrade: true}},
		{"membership", talosv1alpha1.Management{Config: manage, Membership: enabled}, answers{config: true, membership: true}},
		{"maintenance", talosv1alpha1.Management{Config: manage, Maintenance: enabled}, answers{config: true, maintain: true}},
		{"everything", talosv1alpha1.Management{Config: manage, Upgrades: enabled, Membership: enabled, Maintenance: enabled},
			answers{config: true, upgrade: true, membership: true, maintain: true}},
		// The CRD refuses these. An object that got past it still enables nothing.
		{"everything enabled while observing", talosv1alpha1.Management{Config: observe, Upgrades: enabled, Membership: enabled, Maintenance: enabled},
			answers{}},
		{"everything enabled with no config mode", talosv1alpha1.Management{Upgrades: enabled, Membership: enabled, Maintenance: enabled},
			answers{}},
	} {
		got := answers{
			config: tt.m.ManagesConfig(), upgrade: tt.m.MayUpgrade(), membership: tt.m.MayChangeMembership(), maintain: tt.m.MayMaintain(),
		}
		if got != tt.want {
			t.Errorf("%s: got %+v, want %+v", tt.name, got, tt.want)
		}
	}
}

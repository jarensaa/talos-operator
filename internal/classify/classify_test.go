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

package classify_test

import (
	"strings"
	"testing"

	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	"github.com/siderolabs/talos/pkg/machinery/config/machine"

	"github.com/jarensaa/talos-operator/internal/classify"
	"github.com/jarensaa/talos-operator/internal/render"
	"github.com/jarensaa/talos-operator/internal/talos/talostest"
)

func rendered(t *testing.T, bundle *secrets.Bundle, patches ...string) *render.Config {
	t.Helper()

	raw := make([][]byte, 0, len(patches))
	for _, p := range patches {
		raw = append(raw, []byte(p))
	}

	cfg, err := render.Render(render.Input{
		ClusterName:       "c",
		Endpoint:          "https://10.5.0.2:6443",
		KubernetesVersion: "1.37.0",
		Contract:          config.TalosVersion1_14,
		Bundle:            bundle,
		Role:              machine.TypeControlPlane,
		InstallImage:      "factory.talos.dev/metal-installer/376567988ad370138ad8b2698212367b8edcb69b5fd68c80be1f2ec7d603b4ba:v1.14.2",
		Patches:           raw,
	})
	if err != nil {
		t.Fatal(err)
	}

	return cfg
}

func TestClassify(t *testing.T) {
	bundle := talostest.Bundle(t)
	base := rendered(t, bundle)

	other, err := secrets.NewBundle(secrets.NewClock(), config.TalosVersion1_14)
	if err != nil {
		t.Fatal(err)
	}

	const (
		label    = "apiVersion: v1alpha1\nkind: KubeNodeConfig\nlabels:\n  example.com/zone: a\n"
		bond     = "apiVersion: v1alpha1\nkind: BondConfig\nname: bond0\nlinks: [eth0, eth1]\nbondMode: 802.3ad\n"
		firewall = "apiVersion: v1alpha1\nkind: NetworkDefaultActionConfig\ningress: block\n"
		volume   = "apiVersion: v1alpha1\nkind: UserVolumeConfig\nname: data\nprovisioning:\n" +
			"  diskSelector:\n    match: disk.transport == \"nvme\"\n  minSize: 10GiB\n"
		proxy    = "apiVersion: v1alpha1\nkind: KubeProxyConfig\nenabled: false\n"
		kubelet  = "apiVersion: v1alpha1\nkind: KubeletConfig\nextraArgs:\n  max-pods: \"200\"\n"
		apiSrv   = "apiVersion: v1alpha1\nkind: KubeAPIServerConfig\nextraArgs:\n  audit-log-maxage: \"7\"\n"
		resolver = "apiVersion: v1alpha1\nkind: ResolverConfig\nnameservers:\n  - address: 1.1.1.1\n"
		install  = "apiVersion: v1alpha1\nkind: UnattendedInstallConfig\nprovisioning:\n  diskSelector:\n    match: disk.transport == \"nvme\"\n"
	)

	tests := []struct {
		name    string
		desired *render.Config
		want    classify.Class
		reason  string
	}{
		{"equal", rendered(t, bundle), classify.None, ""},
		{"node label", rendered(t, bundle, label), classify.LiveSafe, "v1alpha1/KubeNodeConfig"},
		{"kubelet argument", rendered(t, bundle, kubelet), classify.LiveSafe, "v1alpha1/KubeletConfig"},
		{"certificate SAN", rendered(t, bundle, "machine:\n  certSANs: [extra.example.com]\n"), classify.LiveSafe, "v1alpha1: machine.certSANs"},
		{"api server argument", rendered(t, bundle, apiSrv), classify.ControlPlaneRestart, "v1alpha1/KubeAPIServerConfig"},
		{"bond", rendered(t, bundle, bond), classify.Connectivity, "v1alpha1/BondConfig/bond0"},
		{"host firewall", rendered(t, bundle, firewall), classify.Connectivity, "v1alpha1/NetworkDefaultActionConfig"},
		{"resolvers", rendered(t, bundle, resolver), classify.Connectivity, "v1alpha1/ResolverConfig"},
		{"user volume", rendered(t, bundle, volume), classify.Storage, "v1alpha1/UserVolumeConfig/data"},
		{"a kind without a rule", rendered(t, bundle, proxy), classify.Unknown, "v1alpha1/KubeProxyConfig"},
		{"a field without a rule", rendered(t, bundle, "machine:\n  features:\n    diskQuotaSupport: false\n"),
			classify.Unknown, "v1alpha1: machine.features.diskQuotaSupport"},
		{"install disk", rendered(t, bundle, install), classify.BootTime, "v1alpha1/UnattendedInstallConfig"},
		{"another cluster's secrets", rendered(t, other), classify.Protected, "v1alpha1: machine.ca"},
		// The most severe class wins, and only its reasons are reported.
		{"label and bond", rendered(t, bundle, label, bond), classify.Connectivity, "v1alpha1/BondConfig/bond0"},
		{"label and install disk", rendered(t, bundle, label, install), classify.BootTime, "v1alpha1/UnattendedInstallConfig"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := classify.Classify(base, tt.desired)
			if err != nil {
				t.Fatal(err)
			}

			if got.Class != tt.want {
				t.Fatalf("class: got %q (%v), want %q", got.Class, got.Reasons, tt.want)
			}

			if tt.reason != "" && !strings.Contains(strings.Join(got.Reasons, "\n"), tt.reason) {
				t.Fatalf("reasons %v lack %q", got.Reasons, tt.reason)
			}

			if tt.want != classify.Connectivity {
				return
			}

			for _, r := range got.Reasons {
				if strings.Contains(r, "KubeNodeConfig") {
					t.Fatalf("reasons of a less severe class must not be listed: %v", got.Reasons)
				}
			}
		})
	}
}

// The reverse direction is the same class: removing a bond can cut a node off as adding one can.
func TestClassifyIsSymmetric(t *testing.T) {
	bundle := talostest.Bundle(t)
	with := rendered(t, bundle, "apiVersion: v1alpha1\nkind: BondConfig\nname: bond0\nlinks: [eth0, eth1]\nbondMode: 802.3ad\n")

	got, err := classify.Classify(with, rendered(t, bundle))
	if err != nil || got.Class != classify.Connectivity {
		t.Fatalf("removing a bond: %+v, %v", got, err)
	}
}

func TestClassStrategies(t *testing.T) {
	tests := []struct {
		class             classify.Class
		blocked, approval bool
	}{
		{classify.LiveSafe, false, false},
		{classify.ControlPlaneRestart, false, false},
		{classify.Connectivity, false, false},
		{classify.Storage, false, true},
		{classify.Unknown, false, true},
		{classify.BootTime, true, false},
		{classify.Protected, true, false},
	}

	for _, tt := range tests {
		if tt.class.Blocked() != tt.blocked || tt.class.NeedsApproval() != tt.approval {
			t.Errorf("%s: blocked=%v approval=%v", tt.class, tt.class.Blocked(), tt.class.NeedsApproval())
		}
	}

	order := []classify.Class{classify.None, classify.LiveSafe, classify.ControlPlaneRestart, classify.Connectivity,
		classify.Storage, classify.Unknown, classify.BootTime, classify.Protected}
	for i := 1; i < len(order); i++ {
		if !order[i].Worse(order[i-1]) || order[i-1].Worse(order[i]) {
			t.Errorf("%s must be worse than %s", order[i], order[i-1])
		}
	}
}

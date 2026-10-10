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
	"fmt"
	"slices"
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

	return renderedAt(t, bundle, machine.TypeControlPlane, "1.37.0", patches...)
}

// renderedAt renders a machine of the given role at a Kubernetes version.
func renderedAt(t *testing.T, bundle *secrets.Bundle, role machine.Type, kubernetes string, patches ...string) *render.Config {
	t.Helper()

	raw := make([][]byte, 0, len(patches))
	for _, p := range patches {
		raw = append(raw, []byte(p))
	}

	cfg, err := render.Render(render.Input{
		ClusterName:       "c",
		Endpoint:          "https://10.5.0.2:6443",
		KubernetesVersion: kubernetes,
		Contract:          config.TalosVersion1_14,
		Bundle:            bundle,
		Role:              role,
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
		coreDNS  = "apiVersion: v1alpha1\nkind: KubeCoreDNSConfig\nenabled: false\n"
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
		{"a kind without a rule", rendered(t, bundle, coreDNS), classify.Unknown, "v1alpha1/KubeCoreDNSConfig"},
		// Talos applies the kube-proxy manifest once; what the config says later changes nothing.
		{"kube-proxy", rendered(t, bundle, proxy), classify.LiveSafe, "v1alpha1/KubeProxyConfig"},
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
		{classify.Kubernetes, true, false},
		{classify.Installer, true, false},
		{classify.BootTime, true, false},
		{classify.Protected, true, false},
	}

	for _, tt := range tests {
		if tt.class.Blocked() != tt.blocked || tt.class.NeedsApproval() != tt.approval {
			t.Errorf("%s: blocked=%v approval=%v", tt.class, tt.class.Blocked(), tt.class.NeedsApproval())
		}
	}

	order := []classify.Class{classify.None, classify.LiveSafe, classify.ControlPlaneRestart, classify.Connectivity,
		classify.Storage, classify.Unknown, classify.Kubernetes, classify.Installer, classify.BootTime, classify.Protected}
	for i := 1; i < len(order); i++ {
		if !order[i].Worse(order[i-1]) || order[i-1].Worse(order[i]) {
			t.Errorf("%s must be worse than %s", order[i], order[i-1])
		}
	}
}

// The installer image is the one boot-time setting an upgrade brings about. Naming another image
// is refused as a config change until the node runs what it installs; then it is bookkeeping.
func TestClassifyInstallerImage(t *testing.T) {
	bundle := talostest.Bundle(t)

	installDoc := func(version, disk string) string {
		return "apiVersion: v1alpha1\nkind: UnattendedInstallConfig\ninstaller:\n" +
			"  image: factory.talos.dev/metal-installer/376567988ad370138ad8b2698212367b8edcb69b5fd68c80be1f2ec7d603b4ba:" + version + "\n" +
			"provisioning:\n  diskSelector:\n    match: disk.transport == \"" + disk + "\"\n"
	}

	const (
		volume = "apiVersion: v1alpha1\nkind: UserVolumeConfig\nname: data\nprovisioning:\n" +
			"  diskSelector:\n    match: disk.transport == \"nvme\"\n  minSize: 10GiB\n"
		label = "apiVersion: v1alpha1\nkind: KubeNodeConfig\nlabels:\n  example.com/zone: a\n"
	)

	base := rendered(t, bundle, installDoc("v1.14.1", "sata"))
	newer := installDoc("v1.14.2", "sata")

	tests := []struct {
		name      string
		desired   *render.Config
		installed bool
		want      classify.Class
	}{
		{"another image, not installed yet", rendered(t, bundle, newer), false, classify.Installer},
		{"another image the node already runs", rendered(t, bundle, newer), true, classify.LiveSafe},
		// Only the image is the installer's. A disk is chosen at install time and nowhere else.
		{"another image and another disk", rendered(t, bundle, installDoc("v1.14.2", "nvme")), false, classify.BootTime},
		{"another image and another disk, installed", rendered(t, bundle, installDoc("v1.14.2", "nvme")), true, classify.BootTime},
		// What rides along keeps its own class, so an installed image never hides a change that
		// needs approval.
		{"another image and a label", rendered(t, bundle, newer, label), false, classify.Installer},
		{"another image and a volume, installed", rendered(t, bundle, newer, volume), true, classify.Storage},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var opts []classify.Option
			if tt.installed {
				opts = append(opts, classify.Installed())
			}

			got, err := classify.Classify(base, tt.desired, opts...)
			if err != nil {
				t.Fatal(err)
			}

			if got.Class != tt.want {
				t.Fatalf("got %s (%v), want %s", got.Class, got.Reasons, tt.want)
			}
		})
	}

	if !classify.Installer.Blocked() || classify.Installer.NeedsApproval() {
		t.Fatal("an installer change waits for its upgrade; no approval makes it apply sooner")
	}

	if !classify.BootTime.Worse(classify.Installer) || !classify.Installer.Worse(classify.Unknown) {
		t.Fatal("the installer sits between what needs approval and what only an install can change")
	}
}

// The images of the Kubernetes components are a Kubernetes upgrade's to change, in its own order.
// A config rollout must never be the one to apply them, whatever rides along.
func TestClassifyKubernetesVersion(t *testing.T) {
	bundle := talostest.Bundle(t)

	const (
		argument = "apiVersion: v1alpha1\nkind: KubeAPIServerConfig\nextraArgs:\n  audit-log-maxage: \"7\"\n"
		volume   = "apiVersion: v1alpha1\nkind: UserVolumeConfig\nname: data\nprovisioning:\n" +
			"  diskSelector:\n    match: disk.transport == \"nvme\"\n  minSize: 10GiB\n"
		installer = "apiVersion: v1alpha1\nkind: UnattendedInstallConfig\ninstaller:\n" +
			"  image: factory.talos.dev/metal-installer/376567988ad370138ad8b2698212367b8edcb69b5fd68c80be1f2ec7d603b4ba:%s\n" +
			"provisioning:\n  diskSelector:\n    match: disk.transport == \"sata\"\n"
	)

	install := func(version string) string { return fmt.Sprintf(installer, version) }

	controlPlane, worker := machine.TypeControlPlane, machine.TypeWorker

	tests := []struct {
		name          string
		live, desired *render.Config
		want          classify.Class
		reasons       []string
	}{
		{"a control plane", renderedAt(t, bundle, controlPlane, "1.36.2"), renderedAt(t, bundle, controlPlane, "1.37.0"), classify.Kubernetes,
			[]string{"v1alpha1/KubeAPIServerConfig: image", "v1alpha1/KubeControllerManagerConfig: image",
				"v1alpha1/KubeSchedulerConfig: image", "v1alpha1/KubeletConfig: image"}},
		{"a worker", renderedAt(t, bundle, worker, "1.36.2"), renderedAt(t, bundle, worker, "1.37.0"), classify.Kubernetes,
			[]string{"v1alpha1/KubeletConfig: image"}},
		{"a patch release", renderedAt(t, bundle, worker, "1.37.0"), renderedAt(t, bundle, worker, "1.37.1"), classify.Kubernetes,
			[]string{"v1alpha1/KubeletConfig: image"}},
		// An argument of the same document does not take the image with it into a config rollout.
		{"with an argument", renderedAt(t, bundle, controlPlane, "1.36.2"), renderedAt(t, bundle, controlPlane, "1.37.0", argument),
			classify.Kubernetes, nil},
		{"the argument alone", renderedAt(t, bundle, controlPlane, "1.37.0"), renderedAt(t, bundle, controlPlane, "1.37.0", argument),
			classify.ControlPlaneRestart, []string{"v1alpha1/KubeAPIServerConfig: extraArgs.audit-log-maxage"}},
		{"with a volume", renderedAt(t, bundle, controlPlane, "1.36.2"), renderedAt(t, bundle, controlPlane, "1.37.0", volume),
			classify.Kubernetes, nil},
		// What is more fundamental still comes first.
		{"with another installer", renderedAt(t, bundle, worker, "1.36.2", install("v1.14.1")),
			renderedAt(t, bundle, worker, "1.37.0", install("v1.14.2")), classify.Installer, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := classify.Classify(tt.live, tt.desired)
			if err != nil {
				t.Fatal(err)
			}

			if got.Class != tt.want {
				t.Fatalf("got %s (%v), want %s", got.Class, got.Reasons, tt.want)
			}

			if tt.reasons != nil && !slices.Equal(got.Reasons, tt.reasons) {
				t.Fatalf("reasons: got %v, want %v", got.Reasons, tt.reasons)
			}
		})
	}

	if classify.Kubernetes.NeedsApproval() {
		t.Fatal("no approval makes a config rollout apply a Kubernetes version")
	}
}

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

	return renderedAt(t, bundle, machine.TypeControlPlane, talostest.KubernetesVersion, patches...)
}

// renderedAt renders a machine of the given role at a Kubernetes version.
func renderedAt(t *testing.T, bundle *secrets.Bundle, role machine.Type, kubernetes string, patches ...string) *render.Config {
	t.Helper()

	in := talostest.Input(t, patches...)
	in.Bundle, in.Role, in.KubernetesVersion = bundle, role, kubernetes

	cfg, err := render.Render(in)
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
		bond     = talostest.BondPatch
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
		// Talos applies the CoreDNS manifest once, like kube-proxy's.
		{"CoreDNS", rendered(t, bundle, coreDNS), classify.LiveSafe, "v1alpha1/KubeCoreDNSConfig"},
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
	with := rendered(t, bundle, talostest.BondPatch)

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
			"  image: " + talostest.InstallerOf(version) + "\n" +
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
			"  image: %s\n" +
			"provisioning:\n  diskSelector:\n    match: disk.transport == \"sata\"\n"
	)

	install := func(version string) string { return fmt.Sprintf(installer, talostest.InstallerOf(version)) }

	const kubeletImage = "v1alpha1/KubeletConfig: image"

	controlPlane, worker := machine.TypeControlPlane, machine.TypeWorker

	tests := []struct {
		name          string
		live, desired *render.Config
		want          classify.Class
		reasons       []string
	}{
		{"a control plane", renderedAt(t, bundle, controlPlane, "1.36.2"), renderedAt(t, bundle, controlPlane, "1.37.0"), classify.Kubernetes,
			[]string{"v1alpha1/KubeAPIServerConfig: image", "v1alpha1/KubeControllerManagerConfig: image",
				"v1alpha1/KubeProxyConfig: image", "v1alpha1/KubeSchedulerConfig: image", kubeletImage}},
		{"a worker", renderedAt(t, bundle, worker, "1.36.2"), renderedAt(t, bundle, worker, "1.37.0"), classify.Kubernetes,
			[]string{kubeletImage}},
		{"a patch release", renderedAt(t, bundle, worker, "1.37.0"), renderedAt(t, bundle, worker, "1.37.1"), classify.Kubernetes,
			[]string{kubeletImage}},
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

// A document whose fields do not say how it changed is classified as a whole: by its kind, and as
// Unknown where it is the legacy root document, which has no class but that of its fields.
func TestClassifyADocumentAsAWhole(t *testing.T) {
	const (
		kubelet = render.DocKey("v1alpha1/KubeletConfig")
		install = render.DocKey("v1alpha1/UnattendedInstallConfig")
	)

	doc := func(key render.DocKey, body string) *render.Config {
		cfg := &render.Config{Docs: map[render.DocKey][]byte{}}
		if body != "" {
			cfg.Docs[key] = []byte(body)
		}

		return cfg
	}

	tests := []struct {
		name          string
		key           render.DocKey
		live, desired string
		want          classify.Class
	}{
		// The same fields in another order: the bytes differ, and no field does.
		{"the root document, no field differing", render.RootKey,
			"machine:\n  type: worker\ncluster:\n  id: a\n", "cluster:\n  id: a\nmachine:\n  type: worker\n", classify.Unknown},
		{"a Kubernetes component, no field differing", kubelet,
			"image: a\nextraArgs: {b: c}\n", "extraArgs: {b: c}\nimage: a\n", classify.LiveSafe},
		{"the installer's document, no field differing", install,
			"installer:\n  image: a\n  disk: b\n", "installer:\n  disk: b\n  image: a\n", classify.BootTime},
		// The image is the installer's only while nothing else in the document moves with it.
		{"the installer's image and its disk", install,
			"installer:\n  image: a\n  disk: b\n", "installer:\n  image: c\n  disk: d\n", classify.BootTime},
		// A document only one of the configs has.
		{"the root document gone", render.RootKey, "machine:\n  type: worker\n", "", classify.Protected},
		{"a Kubernetes component appearing", kubelet, "", "image: a\n", classify.LiveSafe},
		{"the installer's document appearing", install, "", "installer:\n  image: a\n", classify.BootTime},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := classify.Classify(doc(tt.key, tt.live), doc(tt.key, tt.desired))
			if err != nil {
				t.Fatal(err)
			}

			// The reason is the document, with no field named.
			if got.Class != tt.want || !slices.Equal(got.Reasons, []string{string(tt.key)}) {
				t.Fatalf("got %s (%v), want %s (%s)", got.Class, got.Reasons, tt.want, tt.key)
			}
		})
	}

	// A document that cannot be compared field by field is an error, and never a class.
	for _, key := range []render.DocKey{render.RootKey, kubelet, install} {
		_, err := classify.Classify(doc(key, "image: a\n"), doc(key, "image: [unclosed\n"))
		if err == nil || !strings.HasPrefix(err.Error(), "compare "+string(key)+": ") {
			t.Errorf("%s: got %v", key, err)
		}
	}
}

// Talos can lose the kubelet when one change restarts it and the container runtime at once, in
// the try and again in its revert: such a change is not rolled out. Either of them alone is.
func TestAChangeThatRestartsTheRuntimeAndTheKubeletIsRefused(t *testing.T) {
	bundle := talostest.Bundle(t)
	base := rendered(t, bundle)

	const (
		customization = "apiVersion: v1alpha1\nkind: CRICustomizationConfig\nname: runsc\n" +
			"content: |\n  [plugins.\"io.containerd.cri.v1.runtime\".containerd.runtimes.runsc]\n    runtime_type = \"io.containerd.runsc.v1\"\n"
		runtimeSpec = "apiVersion: v1alpha1\nkind: CRIBaseRuntimeSpecConfig\noverrides:\n  process:\n    rlimits:\n" +
			"      - type: RLIMIT_NOFILE\n        hard: 1024\n        soft: 1024\n"
		auth    = "apiVersion: v1alpha1\nkind: RegistryAuthConfig\nname: registry.example.com\nusername: user\npassword: secret\n"
		mirror  = "apiVersion: v1alpha1\nkind: RegistryMirrorConfig\nname: docker.io\nendpoints:\n  - url: https://mirror.example.com\n"
		kubelet = "apiVersion: v1alpha1\nkind: KubeletConfig\nextraArgs:\n  max-pods: \"200\"\n"
		label   = "apiVersion: v1alpha1\nkind: KubeNodeConfig\nlabels:\n  example.com/zone: a\n"
		taint   = "apiVersion: v1alpha1\nkind: KubeNodeConfig\ntaints:\n  example.com/dedicated: gpu:NoSchedule\n"

		legacyPart = "machine:\n  files:\n    - path: /etc/cri/conf.d/20-customization.part\n      op: create\n" +
			"      content: |\n        [plugins]\n"
		legacyFile = "machine:\n  files:\n    - path: /var/etc/example\n      op: create\n      content: example\n"
		legacyAuth = "machine:\n  registries:\n    config:\n      registry.example.com:\n        auth:\n          username: user\n"
	)

	tests := []struct {
		name    string
		patches []string
		want    classify.Class
		reasons []string
	}{
		{"a CRI customization alone", []string{customization}, classify.LiveSafe, nil},
		{"a kubelet argument alone", []string{kubelet}, classify.LiveSafe, nil},
		{"a CRI customization and a kubelet argument", []string{customization, kubelet}, classify.RestartsTogether,
			[]string{"restarts cri: v1alpha1/CRICustomizationConfig/runsc", "restarts kubelet: v1alpha1/KubeletConfig"}},
		{"the base runtime spec and a kubelet argument", []string{runtimeSpec, kubelet}, classify.RestartsTogether,
			[]string{"restarts cri: v1alpha1/CRIBaseRuntimeSpecConfig"}},
		{"a registry's credentials and a kubelet argument", []string{auth, kubelet}, classify.RestartsTogether,
			[]string{"restarts cri: v1alpha1/RegistryAuthConfig/registry.example.com"}},
		// Mirrors are in containerd's hosts directory, which it reads without a restart.
		{"a registry mirror and a kubelet argument", []string{mirror, kubelet}, classify.LiveSafe, nil},
		// Labels are put on the Node through the API; taints are part of how the kubelet registers.
		{"a CRI customization and a node label", []string{customization, label}, classify.LiveSafe, nil},
		{"a CRI customization and a node taint", []string{customization, taint}, classify.RestartsTogether,
			[]string{"restarts kubelet: v1alpha1/KubeNodeConfig: taints.example.com/dedicated"}},
		// The legacy document's forms of them restart the runtime the same way.
		{"the legacy CRI customization and a kubelet argument", []string{legacyPart, kubelet}, classify.RestartsTogether,
			[]string{"restarts cri: v1alpha1: machine.files[path=/etc/cri/conf.d/20-customization.part]"}},
		{"another legacy file and a kubelet argument", []string{legacyFile, kubelet}, classify.LiveSafe, nil},
		{"legacy registry credentials and a node taint", []string{legacyAuth, taint}, classify.RestartsTogether,
			[]string{"restarts cri: v1alpha1: machine.registries.config.registry.example.com.auth.username"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := classify.Classify(base, rendered(t, bundle, tt.patches...))
			if err != nil {
				t.Fatal(err)
			}

			if got.Class != tt.want {
				t.Fatalf("class: got %q (%v), want %q", got.Class, got.Reasons, tt.want)
			}

			for _, reason := range tt.reasons {
				if !slices.Contains(got.Reasons, reason) {
					t.Fatalf("reasons %q lack %q", got.Reasons, reason)
				}
			}

			if got.Class == classify.RestartsTogether && !got.Class.Blocked() {
				t.Fatal("a change that restarts both must not be rolled out")
			}
		})
	}
}

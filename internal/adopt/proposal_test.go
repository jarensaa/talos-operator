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

package adopt_test

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	"github.com/siderolabs/talos/pkg/machinery/config/machine"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/adopt"
	"github.com/jarensaa/talos-operator/internal/adopt/bundle"
	"github.com/jarensaa/talos-operator/internal/talos/talostest"
)

const (
	clusterName  = talostest.ClusterName
	schematic    = talostest.SchematicID
	installImage = talostest.InstallImage

	networkPatch = talostest.NetworkPatch + `---
apiVersion: v1alpha1
kind: KubeProxyConfig
enabled: false
`
	diskPatch = `apiVersion: v1alpha1
kind: UnattendedInstallConfig
provisioning:
  diskSelector:
    match: disk.wwid == "%s"
`
)

// liveCluster builds three control planes from generated configs plus per-node patches.
func liveCluster(t *testing.T, b *secrets.Bundle, image string) []adopt.LiveNode {
	t.Helper()

	wwids := []string{"eui.a", "eui.b", "eui.c"}
	nodes := make([]adopt.LiveNode, 0, len(wwids))

	for i, wwid := range wwids {
		in := talostest.Input(t, fmt.Sprintf(diskPatch, wwid), networkPatch)
		in.Bundle, in.InstallImage = b, image

		nodes = append(nodes, adopt.LiveNode{
			Hostname:     fmt.Sprintf("talos-cp-%d", i+1),
			Address:      fmt.Sprintf("10.0.0.%d", 11+i),
			Role:         machine.TypeControlPlane,
			SystemUUID:   fmt.Sprintf("aaaaaaaa-bbbb-4ccc-8ddd-00000000000%d", i),
			TalosVersion: talostest.TalosVersion,
			Config:       talostest.Config(t, in),
		})
	}

	return nodes
}

func observedCluster() *talosv1alpha1.TalosCluster {
	return &talosv1alpha1.TalosCluster{
		ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: "talos-operator"},
		Spec: talosv1alpha1.TalosClusterSpec{
			Credentials: talosv1alpha1.Credentials{
				Source:    talosv1alpha1.CredentialsSourceSecret,
				SecretRef: &talosv1alpha1.SecretKeyReference{Name: "my-cluster-talosconfig"},
			},
			SecretsRef: &talosv1alpha1.SecretKeyReference{Name: "my-cluster-secrets"},
		},
	}
}

func TestBuildProposesSpecsThatReproduceTheCluster(t *testing.T) {
	b := talostest.Bundle(t)

	p, err := adopt.Build(adopt.Request{Nodes: liveCluster(t, b, installImage), Cluster: observedCluster()})
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(p.Bundle.Certs, b.Certs) {
		t.Fatal("the bundle must be recovered from the live cluster")
	}

	spec := p.Cluster.Spec
	if spec.ClusterName != clusterName || spec.Versions.Talos != talostest.TalosVersion ||
		spec.Versions.Kubernetes != talostest.KubernetesVersion ||
		spec.ConfigGeneration.VersionContract != talostest.VersionContract || spec.ConfigGeneration.Installer.SchematicID != schematic {
		t.Fatalf("unexpected spec: %+v", spec)
	}

	if spec.Management.Config != talosv1alpha1.ConfigManagementObserve {
		t.Fatal("the proposal must stay observe-only")
	}

	if spec.SecretsRef == nil || spec.SecretsRef.Name != "my-cluster-secrets" || spec.Credentials.SecretRef == nil {
		t.Fatal("references of the adopted TalosCluster must be kept")
	}

	if len(spec.Config) == 0 || !strings.Contains(spec.Config[0].Inline+spec.Config[len(spec.Config)-1].Inline, "BondConfig") {
		t.Fatalf("cluster-wide config missing: %+v", spec.Config)
	}

	// Three identical control planes: one pool, sharing nothing beyond the cluster's layer.
	if len(p.Pools) != 1 || p.Pools[0].Name != "my-cluster-control-plane" ||
		p.Pools[0].Spec.Role != talosv1alpha1.MachineRoleControlPlane || p.Pools[0].Spec.ClusterRef.Name != clusterName {
		t.Fatalf("pools: %+v", p.Pools)
	}

	if len(p.Machines) != 3 {
		t.Fatalf("one TalosMachine per node expected, got %d", len(p.Machines))
	}

	for i, tm := range p.Machines {
		if tm.Spec.Address != fmt.Sprintf("10.0.0.%d", 11+i) || tm.Spec.Identity.SystemUUID == "" {
			t.Errorf("machine %s: %+v", tm.Name, tm.Spec)
		}

		if tm.Spec.PoolRef.Name != p.Pools[0].Name {
			t.Errorf("machine %s should be in the control-plane pool, got %q", tm.Name, tm.Spec.PoolRef.Name)
		}

		if len(tm.Spec.Config) != 1 || !strings.Contains(tm.Spec.Config[0].Inline, "diskSelector") {
			t.Errorf("machine %s should carry only its disk selector: %+v", tm.Name, tm.Spec.Config)
		}
	}

	if len(p.Notes) != 0 {
		t.Errorf("no notes expected: %v", p.Notes)
	}
}

func TestBuildVerifiesASuppliedBundleInsteadOfTrustingIt(t *testing.T) {
	b := talostest.Bundle(t)
	nodes := liveCluster(t, b, installImage)

	p, err := adopt.Build(adopt.Request{Nodes: nodes, Cluster: observedCluster(), Bundle: b})
	if err != nil || p.Bundle != b {
		t.Fatalf("a matching supplied bundle must be the proposal's: err=%v", err)
	}

	other, err := secrets.NewBundle(secrets.NewClock(), config.TalosVersion1_14)
	if err != nil {
		t.Fatal(err)
	}

	var mismatch *bundle.MismatchError
	if _, err := adopt.Build(adopt.Request{Nodes: nodes, Cluster: observedCluster(), Bundle: other}); !errors.As(err, &mismatch) {
		t.Fatalf("expected MismatchError for a stale secrets.yaml, got %v", err)
	}

	// A Secret that decodes and is not a whole bundle is refused too, and is not read through.
	for _, text := range []string{"{}\n", "cluster:\n  id: a\n  secret: b\n"} {
		partial, err := adopt.DecodeBundle([]byte(text))
		if err != nil {
			t.Fatal(err)
		}

		_, err = adopt.Build(adopt.Request{Nodes: nodes, Cluster: observedCluster(), Bundle: partial})
		if err == nil || !strings.Contains(err.Error(), "secrets bundle is incomplete: missing Talos API CA key") {
			t.Fatalf("expected a bundle without certificates to be refused, got %v", err)
		}
	}
}

func TestBuildNeedsAControlPlane(t *testing.T) {
	nodes := liveCluster(t, talostest.Bundle(t), installImage)
	for i := range nodes {
		nodes[i].Role = machine.TypeWorker
	}

	if _, err := adopt.Build(adopt.Request{Nodes: nodes, Cluster: observedCluster()}); !errors.Is(err, adopt.ErrNoControlPlane) {
		t.Fatalf("expected ErrNoControlPlane, got %v", err)
	}
}

// A cluster installed from the installer with a signed boot image is adopted as one: the spec
// says so, and the specs proposed reproduce the nodes' configs, which Build proves before it
// returns.
func TestBuildAdoptsAClusterOnTheSecureBootInstaller(t *testing.T) {
	image := strings.Replace(installImage, "metal-installer/", "metal-installer-secureboot/", 1)

	p, err := adopt.Build(adopt.Request{Nodes: liveCluster(t, talostest.Bundle(t), image), Cluster: observedCluster()})
	if err != nil {
		t.Fatal(err)
	}

	installer := p.Cluster.Spec.ConfigGeneration.Installer
	if !installer.SecureBoot || installer.Platform != "metal" || installer.SchematicID != schematic {
		t.Fatalf("installer: %+v", installer)
	}
}

func TestBuildRefusesNonFactoryInstallers(t *testing.T) {
	nodes := liveCluster(t, talostest.Bundle(t), "ghcr.io/siderolabs/installer:v1.14.0")

	if _, err := adopt.Build(adopt.Request{Nodes: nodes, Cluster: observedCluster()}); err == nil ||
		!strings.Contains(err.Error(), "not an Image Factory image") {
		t.Fatalf("expected a non-factory installer to be refused, got %v", err)
	}
}

// The installer whose boot image is signed is an image of its own, and what a machine with
// Secure Boot on is installed from: it is read from a live config and written back the same.
func TestInstallerImageNamesTheSecureBootInstaller(t *testing.T) {
	plain := talosv1alpha1.Installer{FactoryURL: "https://factory.example.com", Platform: "metal", SchematicID: schematic}
	signed := plain
	signed.SecureBoot = true

	for image, want := range map[string]talosv1alpha1.Installer{
		"factory.example.com/metal-installer/" + schematic + ":v1.14.2":            plain,
		"factory.example.com/metal-installer-secureboot/" + schematic + ":v1.14.2": signed,
	} {
		got, tag, err := adopt.ParseInstallerImage(image)
		if err != nil || got != want || tag != "v1.14.2" {
			t.Fatalf("%s: parsed %+v %q, %v", image, got, tag, err)
		}

		if back, err := adopt.InstallerImage(got, tag); err != nil || back != image {
			t.Fatalf("%s: built %q, %v", image, back, err)
		}
	}

	for _, image := range []string{
		"factory.example.com/metal-secureboot/" + schematic + ":v1.14.2",
		"factory.example.com/-installer-secureboot/" + schematic + ":v1.14.2",
		"factory.example.com/metal-installer-secureboot-installer/" + schematic[:10] + ":v1.14.2",
	} {
		if got, _, err := adopt.ParseInstallerImage(image); err == nil {
			t.Errorf("%s: expected an error, got %+v", image, got)
		}
	}
}

// A pool says for its own machines whether their installer is the signed one, either way.
func TestInstallTargetTakesAPoolsSecureBootSetting(t *testing.T) {
	spec := func(secureBoot bool) *talosv1alpha1.TalosClusterSpec {
		return &talosv1alpha1.TalosClusterSpec{
			Versions: &talosv1alpha1.Versions{Talos: "v1.14.2"},
			ConfigGeneration: &talosv1alpha1.ConfigGeneration{Installer: &talosv1alpha1.Installer{
				FactoryURL: "https://factory.example.com", Platform: "metal", SchematicID: schematic, SecureBoot: secureBoot,
			}},
		}
	}
	pool := func(secureBoot *bool) *talosv1alpha1.TalosMachinePoolSpec {
		return &talosv1alpha1.TalosMachinePoolSpec{Installer: &talosv1alpha1.PoolInstaller{SecureBoot: secureBoot}}
	}
	yes, no := true, false

	for _, tt := range []struct {
		cluster bool
		pool    *talosv1alpha1.TalosMachinePoolSpec
		want    bool
	}{
		{false, &talosv1alpha1.TalosMachinePoolSpec{}, false},
		{true, &talosv1alpha1.TalosMachinePoolSpec{}, true},
		{true, pool(nil), true},
		{false, pool(&yes), true},
		{true, pool(&no), false},
	} {
		install, err := adopt.InstallTarget(spec(tt.cluster), tt.pool)
		if err != nil {
			t.Fatal(err)
		}

		if install.SecureBoot != tt.want || strings.Contains(install.Image, "-installer-secureboot/") != tt.want ||
			install.FactoryURL != "https://factory.example.com" {
			t.Errorf("cluster %t, pool %+v: got %+v", tt.cluster, tt.pool.Installer, install)
		}
	}
}

func TestBundleEncodingRoundTrips(t *testing.T) {
	b := talostest.Bundle(t)

	data, err := adopt.EncodeBundle(b)
	if err != nil {
		t.Fatal(err)
	}

	decoded, err := adopt.DecodeBundle(data)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(decoded.Certs, b.Certs) || !reflect.DeepEqual(decoded.Secrets, b.Secrets) {
		t.Fatal("bundle does not round-trip")
	}

	h1, _ := adopt.BundleHash(b)       //nolint:errcheck
	h2, _ := adopt.BundleHash(decoded) //nolint:errcheck

	if h1 == "" || h1 != h2 {
		t.Fatalf("hash must be stable across encoding: %q vs %q", h1, h2)
	}
}

// The authorities are listed under the names status.certificates has for them.
func TestAuthorityValidity(t *testing.T) {
	got := adopt.AuthorityValidity(talostest.Bundle(t))
	if len(got) != 4 {
		t.Fatalf("a bundle has four authorities, got %d", len(got))
	}

	for _, name := range []string{"os", "kubernetes", "kubernetes-aggregator", "etcd"} {
		if v, ok := got[name]; !ok || !v.NotAfter.After(v.NotBefore) {
			t.Errorf("%s: validity %+v, listed %v", name, v, ok)
		}
	}

	if len(adopt.AuthorityValidity(nil)) != 0 {
		t.Fatal("no bundle has no authorities")
	}
}

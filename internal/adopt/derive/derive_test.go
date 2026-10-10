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

package derive_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	"github.com/siderolabs/talos/pkg/machinery/config/machine"

	"github.com/jarensaa/talos-operator/internal/adopt/derive"
	"github.com/jarensaa/talos-operator/internal/render"
	"github.com/jarensaa/talos-operator/internal/talos"
	"github.com/jarensaa/talos-operator/internal/talos/talostest"
)

const (
	installImage = "factory.talos.dev/metal-installer/376567988ad370138ad8b2698212367b8edcb69b5fd68c80be1f2ec7d603b4ba:v1.14.2"

	networkPatch = `apiVersion: v1alpha1
kind: BondConfig
name: bond0
links: [eth0, eth1]
bondMode: 802.3ad
---
apiVersion: v1alpha1
kind: DHCPv4Config
name: bond0
`
	controlPlanePatch = `apiVersion: v1alpha1
kind: KubeFlannelCNIConfig
$patch: delete
---
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

func inputs() derive.Inputs {
	return derive.Inputs{
		ClusterName:       "my-cluster",
		Endpoint:          "https://cluster.example.com:6443",
		KubernetesVersion: "1.37.0",
		TalosVersion:      "v1.14",
		Contract:          config.TalosVersion1_14,
		InstallImage:      installImage,
		DNSDomain:         render.DefaultDNSDomain,
	}
}

// liveNode builds a node the way a hand-rolled cluster is built, round-tripped through bytes like a config
// read back from a real node.
func liveNode(t *testing.T, b *secrets.Bundle, in derive.Inputs, name string, role machine.Type, patches ...string) derive.Node {
	t.Helper()

	raw := make([][]byte, 0, len(patches))
	for _, p := range patches {
		raw = append(raw, []byte(p))
	}

	cfg, err := render.Generate(in.RenderInput(render.Input{Bundle: b, Role: role, Patches: raw}))
	if err != nil {
		t.Fatal(err)
	}

	encoded, err := cfg.Bytes()
	if err != nil {
		t.Fatal(err)
	}

	live, err := talos.ParseMachineConfig(encoded)
	if err != nil {
		t.Fatal(err)
	}

	return derive.Node{Name: name, Role: role, Live: live}
}

func TestInferRoundTrip(t *testing.T) {
	want := inputs()
	want.DNSDomain = "home.arpa"

	node := liveNode(t, talostest.Bundle(t), want, "cp-1", machine.TypeControlPlane)

	got, err := derive.Infer(node.Live, "v1.14.2")
	if err != nil {
		t.Fatal(err)
	}

	if got.ClusterName != want.ClusterName || got.Endpoint != want.Endpoint || got.KubernetesVersion != want.KubernetesVersion ||
		got.TalosVersion != want.TalosVersion || got.InstallImage != want.InstallImage || got.DNSDomain != want.DNSDomain {
		t.Fatalf("inferred %+v, want %+v", got, want)
	}
}

func TestThreeAlikeNodesFactorIntoOneSharedLayerAndDiskSelectors(t *testing.T) {
	b := talostest.Bundle(t)
	in := inputs()

	wwids := []string{"eui.a", "eui.b", "eui.c"}
	nodes := make([]derive.Node, 0, len(wwids))

	for i, wwid := range wwids {
		nodes = append(nodes, liveNode(t, b, in, fmt.Sprintf("cp-%d", i+1), machine.TypeControlPlane,
			fmt.Sprintf(diskPatch, wwid), networkPatch, controlPlanePatch))
	}

	layers, err := derive.Patches(in, b, nodes)
	if err != nil {
		t.Fatal(err)
	}

	shared := strings.Join(layers.All, "\n")
	for _, want := range []string{"kind: BondConfig", "kind: DHCPv4Config", "kind: KubeFlannelCNIConfig", "kind: KubeProxyConfig"} {
		if !strings.Contains(shared, want) {
			t.Errorf("shared layer lacks %s:\n%s", want, shared)
		}
	}

	if strings.Contains(shared, "diskSelector") {
		t.Errorf("per-node disk selectors must not be shared:\n%s", shared)
	}

	if len(layers.ControlPlane) != 0 || len(layers.Worker) != 0 {
		t.Errorf("every node is a control plane, so nothing is role-specific: %+v", layers)
	}

	for i, wwid := range []string{"eui.a", "eui.b", "eui.c"} {
		own := strings.Join(layers.Machines[fmt.Sprintf("cp-%d", i+1)], "\n")
		if !strings.Contains(own, wwid) || strings.Contains(own, "BondConfig") {
			t.Errorf("cp-%d machine layer should hold only its disk selector:\n%s", i+1, own)
		}
	}
}

func TestMixedRolesGetRoleLayers(t *testing.T) {
	b := talostest.Bundle(t)
	in := inputs()

	nodes := []derive.Node{
		liveNode(t, b, in, "cp-1", machine.TypeControlPlane, networkPatch, controlPlanePatch),
		liveNode(t, b, in, "cp-2", machine.TypeControlPlane, networkPatch, controlPlanePatch),
		liveNode(t, b, in, "worker-1", machine.TypeWorker, networkPatch),
	}

	layers, err := derive.Patches(in, b, nodes)
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(strings.Join(layers.All, "\n"), "kind: BondConfig") {
		t.Errorf("network patch should be shared: %+v", layers)
	}

	if !strings.Contains(strings.Join(layers.ControlPlane, "\n"), "kind: KubeProxyConfig") {
		t.Errorf("kube-proxy patch should be control-plane only: %+v", layers)
	}

	if len(layers.Machines) != 0 {
		t.Errorf("no machine-specific patches expected: %+v", layers.Machines)
	}
}

func TestIdentityDifferencesAreRefused(t *testing.T) {
	b := talostest.Bundle(t)
	in := inputs()

	elsewhere := in
	elsewhere.Endpoint = "https://other.example:6443"

	nodes := []derive.Node{
		liveNode(t, b, in, "cp-1", machine.TypeControlPlane),
		liveNode(t, b, elsewhere, "cp-2", machine.TypeControlPlane),
	}

	if _, err := derive.Patches(in, b, nodes); !errors.Is(err, derive.ErrIdentityPatch) {
		t.Fatalf("expected ErrIdentityPatch, got %v", err)
	}
}

func TestNodeFromAnotherBundleIsRefused(t *testing.T) {
	b := talostest.Bundle(t)
	in := inputs()

	other, err := secrets.NewBundle(secrets.NewClock(), config.TalosVersion1_14)
	if err != nil {
		t.Fatal(err)
	}

	nodes := []derive.Node{
		liveNode(t, b, in, "cp-1", machine.TypeControlPlane),
		liveNode(t, other, in, "intruder", machine.TypeControlPlane),
	}

	if _, err := derive.Patches(in, b, nodes); err == nil {
		t.Fatal("a node with different secrets must not be adopted")
	}
}

//go:build parity

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

package render_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	"github.com/siderolabs/talos/pkg/machinery/config/machine"

	"github.com/jarensaa/talos-operator/internal/render"
	"github.com/jarensaa/talos-operator/internal/talos"
)

// The parity test generates a cluster's configs with a real talosctl (gen config --with-secrets,
// then machineconfig patch) and requires Render to produce the same documents. It is the guard against the
// failure mode that broke Omni's import on Talos 1.14 (siderolabs/omni#3424): if generation
// differs, adoption can never prove "no changes". Secrets are generated per run, so no key
// material is committed. Run with `make test-parity` (talosctl must match the machinery version).

const (
	parityCluster  = "my-cluster"
	parityEndpoint = "https://cluster.example.com:6443"
	parityK8s      = "1.37.0"
	parityImage    = "factory.talos.dev/metal-installer/376567988ad370138ad8b2698212367b8edcb69b5fd68c80be1f2ec7d603b4ba:v1.14.2"

	// A per-node patch and a cluster-wide network patch, of the kinds such clusters carry.
	nodePatch = `apiVersion: v1alpha1
kind: DHCPv4Config
name: eth1
---
apiVersion: v1alpha1
kind: UnattendedInstallConfig
provisioning:
  diskSelector:
    match: disk.wwid == "eui.0123456789abcdef0123456789abcdef"
`
	// network.patch.yaml split the way the TalosCluster patch layers split it: the bond applies to
	// every node, the CNI and kube-proxy documents only to control planes (Talos rejects
	// KubeProxyConfig on workers; talosctl machineconfig patch does not check that).
	networkPatch = `apiVersion: v1alpha1
kind: DHCPv4Config
name: eth1
$patch: delete
---
apiVersion: v1alpha1
kind: BondConfig
name: bond0
links:
  - eth0
  - eth1
bondMode: 802.3ad
miimon: 100
updelay: 200
downdelay: 200
lacpRate: fast
xmitHashPolicy: layer3+4
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
)

func talosctl(t *testing.T, dir string, args ...string) {
	t.Helper()

	cmd := exec.Command("talosctl", args...)
	cmd.Dir = dir

	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("talosctl %s: %v\n%s", strings.Join(args, " "), err, out)
	}
}

func TestParityWithTalosctl(t *testing.T) {
	if _, err := exec.LookPath("talosctl"); err != nil {
		t.Fatal("talosctl not found in PATH")
	}

	dir := t.TempDir()

	talosctl(t, dir, "gen", "secrets", "--output-file", "secrets.yaml")

	for name, content := range map[string]string{
		"node.patch.yaml": nodePatch, "network.patch.yaml": networkPatch, "controlplane.patch.yaml": controlPlanePatch,
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	bundle, err := secrets.LoadBundle(filepath.Join(dir, "secrets.yaml"))
	if err != nil {
		t.Fatal(err)
	}

	for _, role := range []machine.Type{machine.TypeControlPlane, machine.TypeWorker} {
		t.Run(role.String(), func(t *testing.T) {
			// With a single output type, --output names the file rather than a directory.
			generated := filepath.Join(dir, role.String()+".yaml")

			talosctl(t, dir, "gen", "config", parityCluster, parityEndpoint,
				"--with-secrets", "secrets.yaml",
				"--install-image", parityImage,
				"--kubernetes-version", parityK8s,
				"--output-types", role.String(),
				"--with-docs=false", "--with-examples=false",
				"--output", generated,
			)

			patchArgs := []string{"--patch", "@node.patch.yaml", "--patch", "@network.patch.yaml"}
			patches := [][]byte{[]byte(nodePatch), []byte(networkPatch)}

			if role == machine.TypeControlPlane {
				patchArgs = append(patchArgs, "--patch", "@controlplane.patch.yaml")
				patches = append(patches, []byte(controlPlanePatch))
			}

			patched := filepath.Join(dir, role.String()+"-patched.yaml")
			talosctl(t, dir, append(append([]string{"machineconfig", "patch", generated}, patchArgs...), "--output", patched)...)

			raw, err := os.ReadFile(patched)
			if err != nil {
				t.Fatal(err)
			}

			parsed, err := talos.ParseMachineConfig(raw)
			if err != nil {
				t.Fatal(err)
			}

			want, err := render.Normalize(parsed)
			if err != nil {
				t.Fatal(err)
			}

			got, err := render.Render(render.Input{
				ClusterName:       parityCluster,
				Endpoint:          parityEndpoint,
				KubernetesVersion: parityK8s,
				Contract:          config.TalosVersion1_14,
				Bundle:            bundle,
				Role:              role,
				InstallImage:      parityImage,
				Patches:           patches,
			})
			if err != nil {
				t.Fatal(err)
			}

			compareDocs(t, want, got)
		})
	}
}

// compareDocs reports every document that differs, by key, with both renderings. The configs are
// generated from throwaway secrets, so printing them is safe.
func compareDocs(t *testing.T, want, got *render.Config) {
	t.Helper()

	keys := slices.Clone(want.Keys)
	for _, k := range got.Keys {
		if !slices.Contains(keys, k) {
			keys = append(keys, k)
		}
	}

	for _, k := range keys {
		w, inWant := want.Docs[k]
		g, inGot := got.Docs[k]

		switch {
		case !inWant:
			t.Errorf("%s: rendered but not produced by talosctl:\n%s", k, g)
		case !inGot:
			t.Errorf("%s: produced by talosctl but not rendered:\n%s", k, w)
		case string(w) != string(g):
			t.Errorf("%s differs\n--- talosctl\n%s\n--- render\n%s", k, w, g)
		}
	}

	if want.Hash != got.Hash && !t.Failed() {
		t.Error("hashes differ although every document matches")
	}
}

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

package talostest

import (
	"encoding/base64"
	"slices"

	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	"github.com/siderolabs/talos/pkg/machinery/config/machine"

	"github.com/jarensaa/talos-operator/internal/render"
)

// The cluster the generated configs of the tests belong to. All of it is made up.
//
// This package is imported by the tests of internal/talos itself, so it imports nothing that
// imports internal/talos. It may import internal/render for as long as that holds of render.
const (
	ClusterName       = "my-cluster"
	Endpoint          = "https://cluster.example.com:6443"
	KubernetesVersion = "1.37.0"
	// TalosVersion is the release the cluster's nodes are installed from, VersionContract the
	// contract its configs are generated for: the one Bundle is made for.
	TalosVersion    = "v1.14.2"
	VersionContract = "v1.14"
	// SchematicID is the Image Factory schematic of the cluster's installer, InstallImage the
	// installer of TalosVersion built from it (InstallerOf).
	SchematicID  = "376567988ad370138ad8b2698212367b8edcb69b5fd68c80be1f2ec7d603b4ba"
	InstallImage = installer + TalosVersion

	installer = "factory.talos.dev/metal-installer/" + SchematicID + ":"

	// BondPatch is a config document that can cut a node off: a bond of two links.
	BondPatch = `apiVersion: v1alpha1
kind: BondConfig
name: bond0
links: [eth0, eth1]
bondMode: 802.3ad
`
	// NetworkPatch is BondPatch and the document that asks for an address on the bond: two
	// documents, both named.
	NetworkPatch = BondPatch + `---
apiVersion: v1alpha1
kind: DHCPv4Config
name: bond0
`
)

// InstallerOf returns the installer image of a Talos version, built from the cluster's schematic:
// what a node of the cluster runs before or after an upgrade.
func InstallerOf(talosVersion string) string { return installer + talosVersion }

// Patches returns config documents as the patches of a render input, in the order given.
func Patches(documents ...string) [][]byte {
	out := make([][]byte, 0, len(documents))
	for _, d := range documents {
		out = append(out, []byte(d))
	}

	return out
}

// Input returns what the config of a control plane of the cluster is rendered from: the shared
// bundle, the cluster's installer, and the given config documents as patches. A test changes the
// fields it is about.
func Input(t T, patches ...string) render.Input {
	t.Helper()

	return render.Input{
		ClusterName:       ClusterName,
		Endpoint:          Endpoint,
		KubernetesVersion: KubernetesVersion,
		Contract:          config.TalosVersion1_14,
		Bundle:            Bundle(t),
		Role:              machine.TypeControlPlane,
		InstallImage:      InstallImage,
		Patches:           Patches(patches...),
	}
}

// Generate generates the config of in: patched and validated, not normalized.
func Generate(t T, in render.Input) config.Provider {
	t.Helper()

	cfg, err := render.Generate(in)
	if err != nil {
		t.Fatal(err)
	}

	return cfg
}

// Config returns the config of in as a node that was handed it reports it: its bytes, the way
// talosctl would have written them.
func Config(t T, in render.Input) []byte {
	t.Helper()

	raw, err := Generate(t, in).Bytes()
	if err != nil {
		t.Fatal(err)
	}

	return raw
}

// SecretValues lists the secrets of a bundle as a config generated from it holds them: the tokens
// as they are, the keys as base64 of their PEM. No output of the operator may contain one.
func SecretValues(b *secrets.Bundle) []string {
	keys := [][]byte{
		b.Certs.OS.Key, b.Certs.Etcd.Key, b.Certs.K8s.Key, b.Certs.K8sAggregator.Key, b.Certs.K8sServiceAccount.Key,
	}

	out := make([]string, 0, 4+len(keys))
	out = append(out, b.TrustdInfo.Token, b.Secrets.BootstrapToken, b.Secrets.SecretboxEncryptionSecret, b.Cluster.Secret)

	for _, key := range keys {
		out = append(out, base64.StdEncoding.EncodeToString(key))
	}

	return slices.DeleteFunc(out, func(s string) bool { return s == "" })
}

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

package adopt

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	"github.com/siderolabs/talos/pkg/machinery/config/machine"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/adopt/bundle"
	"github.com/jarensaa/talos-operator/internal/adopt/derive"
	"github.com/jarensaa/talos-operator/internal/render"
	"github.com/jarensaa/talos-operator/internal/talos"
)

// ErrNoControlPlane means no control-plane config was supplied, so secrets cannot be recovered.
var ErrNoControlPlane = errors.New("adoption needs at least one control-plane node")

// LiveNode is one node as read from the cluster.
type LiveNode struct {
	// Hostname names the node; it becomes the proposed TalosMachine's name.
	Hostname     string
	Address      string
	Role         machine.Type
	SystemUUID   string
	TalosVersion string
	// Config is the node's active machine config, raw.
	Config []byte
}

// Request is the input to Build.
type Request struct {
	Nodes []LiveNode
	// Cluster is the TalosCluster being adopted; its name, namespace and references are kept.
	Cluster *talosv1alpha1.TalosCluster
	// Bundle is the bundle from secretsRef if that Secret exists. It is verified, never trusted.
	// Nil means: recover it from a live control-plane config.
	Bundle *secrets.Bundle
}

// Proposal is everything a user commits to adopt the cluster, already proven to reproduce it.
type Proposal struct {
	// Bundle is the bundle that was supplied, or the one recovered from the live cluster.
	Bundle  *secrets.Bundle
	Cluster *talosv1alpha1.TalosCluster
	// Pools holds one pool per role present, with the config its machines share.
	Pools    []*talosv1alpha1.TalosMachinePool
	Machines []*talosv1alpha1.TalosMachine
	// Notes are facts worth reviewing before committing, e.g. version mismatches.
	Notes []string
}

// Build reconstructs the cluster's secrets, inputs and config layers from its live configs and
// returns the TalosCluster, TalosMachinePool and TalosMachine specs that reproduce it. Before returning, every
// node is rendered from the proposed specs through RenderInput, the same path the controllers
// use, and must come out identical to its live config.
func Build(req Request) (*Proposal, error) {
	nodes := slices.Clone(req.Nodes)
	slices.SortFunc(nodes, func(a, b LiveNode) int { return strings.Compare(a.Hostname, b.Hostname) })

	parsed := make(map[string]config.Provider, len(nodes))

	for _, n := range nodes {
		cfg, err := talos.ParseMachineConfig(n.Config)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", n.Hostname, err)
		}

		parsed[n.Hostname] = cfg
	}

	cpIdx := slices.IndexFunc(nodes, func(n LiveNode) bool { return n.Role == machine.TypeControlPlane })
	if cpIdx < 0 {
		return nil, ErrNoControlPlane
	}

	cp := nodes[cpIdx]

	inputs, err := derive.Infer(parsed[cp.Hostname], cp.TalosVersion)
	if err != nil {
		return nil, fmt.Errorf("infer inputs from %s: %w", cp.Hostname, err)
	}

	p := &Proposal{Bundle: req.Bundle}

	if p.Bundle == nil {
		if p.Bundle, err = bundle.Recover(parsed[cp.Hostname], inputs.Contract); err != nil {
			return nil, fmt.Errorf("recover secrets from %s: %w", cp.Hostname, err)
		}
	}

	verifyNodes := make([]bundle.Node, 0, len(nodes))
	deriveNodes := make([]derive.Node, 0, len(nodes))

	for _, n := range nodes {
		verifyNodes = append(verifyNodes, bundle.Node{Name: n.Hostname, Config: parsed[n.Hostname]})
		deriveNodes = append(deriveNodes, derive.Node{Name: n.Hostname, Role: n.Role, Live: parsed[n.Hostname]})
	}

	if err := bundle.Verify(p.Bundle, verifyNodes); err != nil {
		return nil, err
	}

	layers, err := derive.Patches(inputs, p.Bundle, deriveNodes)
	if err != nil {
		return nil, err
	}

	if p.Cluster, err = clusterSpec(req.Cluster, inputs, layers); err != nil {
		return nil, err
	}

	pools := map[machine.Type]*talosv1alpha1.TalosMachinePool{}

	for _, n := range nodes {
		pool, ok := pools[n.Role]
		if !ok {
			shared := layers.Worker
			if n.Role == machine.TypeControlPlane {
				shared = layers.ControlPlane
			}

			pool = poolSpec(req.Cluster, n.Role, shared)
			pools[n.Role] = pool
			p.Pools = append(p.Pools, pool)
		}

		p.Machines = append(p.Machines, machineSpec(req.Cluster, pool.Name, n, layers.Machines[n.Hostname]))

		if n.TalosVersion != p.Cluster.Spec.Versions.Talos {
			p.Notes = append(p.Notes, fmt.Sprintf("%s runs Talos %s but its config installs %s; versions.talos follows the config",
				n.Hostname, n.TalosVersion, p.Cluster.Spec.Versions.Talos))
		}
	}

	if err := proveSpecs(p, nodes, parsed); err != nil {
		return nil, err
	}

	return p, nil
}

func clusterSpec(current *talosv1alpha1.TalosCluster, in derive.Inputs, layers derive.Layers) (*talosv1alpha1.TalosCluster, error) {
	installer, talosTag, err := ParseInstallerImage(in.InstallImage)
	if err != nil {
		return nil, err
	}

	spec := talosv1alpha1.TalosClusterSpec{
		Management: talosv1alpha1.Management{
			Config:     talosv1alpha1.ConfigManagementObserve,
			Upgrades:   talosv1alpha1.CapabilityDisabled,
			Membership: talosv1alpha1.CapabilityDisabled,
		},
		Credentials:          current.Spec.Credentials,
		SecretsRef:           current.Spec.SecretsRef,
		ClusterName:          in.ClusterName,
		ControlPlaneEndpoint: in.Endpoint,
		Versions:             &talosv1alpha1.Versions{Talos: talosTag, Kubernetes: in.KubernetesVersion},
		ConfigGeneration: &talosv1alpha1.ConfigGeneration{
			VersionContract: in.TalosVersion,
			Installer:       &installer,
		},
		Config: inline(layers.All),
	}

	if in.DNSDomain != render.DefaultDNSDomain {
		spec.ConfigGeneration.Options = &talosv1alpha1.GenerationOptions{DNSDomain: in.DNSDomain}
	}

	return &talosv1alpha1.TalosCluster{
		TypeMeta:   metav1.TypeMeta{APIVersion: talosv1alpha1.GroupVersion.String(), Kind: "TalosCluster"},
		ObjectMeta: metav1.ObjectMeta{Name: current.Name, Namespace: current.Namespace},
		Spec:       spec,
	}, nil
}

// poolName names the pool proposed for a role.
func poolName(cluster string, role talosv1alpha1.MachineRole) string {
	if role == talosv1alpha1.MachineRoleControlPlane {
		return cluster + "-control-plane"
	}

	return cluster + "-workers"
}

func poolSpec(cluster *talosv1alpha1.TalosCluster, role machine.Type, shared []string) *talosv1alpha1.TalosMachinePool {
	return &talosv1alpha1.TalosMachinePool{
		TypeMeta:   metav1.TypeMeta{APIVersion: talosv1alpha1.GroupVersion.String(), Kind: "TalosMachinePool"},
		ObjectMeta: metav1.ObjectMeta{Name: poolName(cluster.Name, machineRole(role)), Namespace: cluster.Namespace},
		Spec: talosv1alpha1.TalosMachinePoolSpec{
			ClusterRef: corev1.LocalObjectReference{Name: cluster.Name},
			Role:       machineRole(role),
			Config:     inline(shared),
		},
	}
}

func machineSpec(cluster *talosv1alpha1.TalosCluster, pool string, n LiveNode, own []string) *talosv1alpha1.TalosMachine {
	return &talosv1alpha1.TalosMachine{
		TypeMeta:   metav1.TypeMeta{APIVersion: talosv1alpha1.GroupVersion.String(), Kind: "TalosMachine"},
		ObjectMeta: metav1.ObjectMeta{Name: n.Hostname, Namespace: cluster.Namespace},
		Spec: talosv1alpha1.TalosMachineSpec{
			PoolRef:  corev1.LocalObjectReference{Name: pool},
			Address:  n.Address,
			Identity: talosv1alpha1.MachineIdentity{SystemUUID: n.SystemUUID},
			Config:   inline(own),
		},
	}
}

func inline(documents []string) []talosv1alpha1.ConfigSource {
	if len(documents) == 0 {
		return nil
	}

	out := make([]talosv1alpha1.ConfigSource, 0, len(documents))
	for _, d := range documents {
		out = append(out, talosv1alpha1.ConfigSource{Inline: d})
	}

	return out
}

// ConfigSources returns a machine's config sources in the order they apply: the cluster's
// layer, its pool's, then its own (docs/scope.md §6.1).
func ConfigSources(cluster *talosv1alpha1.TalosClusterSpec, pool *talosv1alpha1.TalosMachinePoolSpec,
	m *talosv1alpha1.TalosMachineSpec,
) []talosv1alpha1.ConfigSource {
	out := make([]talosv1alpha1.ConfigSource, 0, len(cluster.Config)+len(pool.Config)+len(m.Config))
	out = append(out, cluster.Config...)
	out = append(out, pool.Config...)

	return append(out, m.Config...)
}

// inlineDocuments returns the contents of inline config sources, which are all a proposal has.
func inlineDocuments(sources []talosv1alpha1.ConfigSource) [][]byte {
	out := make([][]byte, 0, len(sources))
	for _, s := range sources {
		out = append(out, []byte(s.Inline))
	}

	return out
}

// proveSpecs renders every node from the proposed specs and requires its live config back.
func proveSpecs(p *Proposal, nodes []LiveNode, parsed map[string]config.Provider) error {
	for i, n := range nodes {
		tm := p.Machines[i]

		poolIdx := slices.IndexFunc(p.Pools, func(pool *talosv1alpha1.TalosMachinePool) bool { return pool.Name == tm.Spec.PoolRef.Name })
		pool := &p.Pools[poolIdx].Spec

		in, err := RenderInput(&p.Cluster.Spec, pool, p.Bundle, inlineDocuments(ConfigSources(&p.Cluster.Spec, pool, &tm.Spec)))
		if err != nil {
			return fmt.Errorf("%s: %w", n.Hostname, err)
		}

		got, err := render.Render(in)
		if err != nil {
			return fmt.Errorf("%s: render proposed spec: %w", n.Hostname, err)
		}

		want, err := render.Normalize(parsed[n.Hostname])
		if err != nil {
			return fmt.Errorf("%s: %w", n.Hostname, err)
		}

		diffs, err := render.Diff(got, want)
		if err != nil {
			return fmt.Errorf("%s: %w", n.Hostname, err)
		}

		if len(diffs) > 0 {
			var text strings.Builder
			for _, d := range diffs {
				text.WriteString(d.Text)
			}

			return fmt.Errorf("%s: proposed specs do not reproduce the live config:\n%s", n.Hostname, text.String())
		}
	}

	return nil
}

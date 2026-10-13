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

package derive

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/config/configdiff"
	"github.com/siderolabs/talos/pkg/machinery/config/configpatcher"
	"github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	"github.com/siderolabs/talos/pkg/machinery/config/machine"
	yaml "go.yaml.in/yaml/v4"

	"github.com/jarensaa/talos-operator/internal/adopt/bundle"
	"github.com/jarensaa/talos-operator/internal/render"
)

// identityKinds are documents that carry cluster identity. A patch touching one means a node
// disagrees with the cluster about who it is, which adoption must not paper over.
var identityKinds = []string{
	"KubeClusterConfig", "DiscoveryIdentityConfig", "KubeServiceAccountConfig",
	"KubeAggregatorCAConfig", "KubeEtcdEncryptionConfig",
}

// ErrIdentityPatch means a node's live config differs from the cluster in an identity document.
var ErrIdentityPatch = errors.New("a node's identity differs from the cluster")

// Node is one live node's config.
type Node struct {
	Name string
	Role machine.Type
	Live config.Provider
}

// Layers are derived patches per TalosCluster/TalosMachine layer. Each entry is one strategic
// merge patch (multi-document YAML); entries apply in order.
type Layers struct {
	All          []string
	ControlPlane []string
	Worker       []string
	Machines     map[string][]string
}

// Role returns the layer the machines of a role share.
func (l Layers) Role(role machine.Type) []string {
	if role == machine.TypeControlPlane {
		return l.ControlPlane
	}

	return l.Worker
}

// patches returns the patches of one node in the order they apply.
func (l Layers) patches(name string, role machine.Type) [][]byte {
	layered := render.Layered(l.All, l.Role(role), l.Machines[name])

	out := make([][]byte, 0, len(layered))
	for _, p := range layered {
		out = append(out, []byte(p))
	}

	return out
}

// unit is everything one node's patches do to one document, per pass. Keeping a document's
// passes together means wherever the unit lands, its operations keep their order.
type unit [2]string

// target is what one patch document addresses: a document, to merge into or to delete.
type target struct {
	key    render.DocKey
	delete bool
}

// String names the target in errors, and is what targets are ordered by.
func (t target) String() string {
	if t.delete {
		return string(t.key) + "#delete"
	}

	return string(t.key)
}

// Patches derives the layers that turn the regenerated base into every node's live config, then
// proves it: rendering base + layers must reproduce each live config exactly, and the secrets
// must still verify against the bundle.
func Patches(in Inputs, b *secrets.Bundle, nodes []Node) (Layers, error) {
	bases := map[machine.Type]config.Provider{}

	for _, role := range []machine.Type{machine.TypeControlPlane, machine.TypeWorker} {
		base, err := render.Generate(in.RenderInput(render.Input{Bundle: b, Role: role}))
		if err != nil {
			return Layers{}, fmt.Errorf("generate %s base: %w", role, err)
		}

		bases[role] = base
	}

	// units[node][target] is what the node's patches do to that document.
	units := map[string]map[target]unit{}

	for _, n := range nodes {
		u, err := nodeUnits(bases[n.Role], n.Live)
		if err != nil {
			return Layers{}, fmt.Errorf("%s: %w", n.Name, err)
		}

		for to := range u {
			if slices.Contains(identityKinds, to.key.Kind()) {
				return Layers{}, fmt.Errorf("%w: %s differs on %s", ErrIdentityPatch, to, n.Name)
			}
		}

		units[n.Name] = u
	}

	layers := factor(nodes, units)

	if err := prove(in, b, nodes, layers); err != nil {
		return Layers{}, err
	}

	return layers, nil
}

// nodeUnits diffs base against live and groups the resulting patch documents by what they address.
func nodeUnits(base, live config.Provider) (map[target]unit, error) {
	patches, err := configdiff.Patch(base, live)
	if err != nil {
		return nil, fmt.Errorf("diff: %w", err)
	}

	units := map[target]unit{}

	for pass, p := range patches {
		smp, ok := p.(configpatcher.StrategicMergePatch)
		if !ok {
			return nil, fmt.Errorf("unexpected patch type %T", p)
		}

		raw, err := smp.Bytes()
		if err != nil {
			return nil, fmt.Errorf("encode patch: %w", err)
		}

		docs, err := splitDocs(raw)
		if err != nil {
			return nil, err
		}

		for to, doc := range docs {
			u := units[to]
			u[pass] = doc
			units[to] = u
		}
	}

	return units, nil
}

// splitDocs splits a multi-document patch into its documents by what each addresses, with
// "$patch: delete" selectors apart from merges. The documents are kept as text, which is what a
// layer is made of.
func splitDocs(raw []byte) (map[target]string, error) {
	out := map[target]string{}

	for _, part := range strings.Split("\n"+string(raw), "\n---\n") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		var head struct {
			Version    string `yaml:"version"`
			APIVersion string `yaml:"apiVersion"`
			Kind       string `yaml:"kind"`
			Name       string `yaml:"name"`
			Patch      string `yaml:"$patch"`
		}

		if err := yaml.Unmarshal([]byte(part), &head); err != nil {
			return nil, errors.New("patch document does not parse")
		}

		to := target{key: render.KeyOf(head.APIVersion, head.Kind, head.Name), delete: head.Patch == "delete"}
		out[to] = part + "\n"
	}

	return out, nil
}

// factor places each unit in the widest layer where it is identical on every node of that scope.
func factor(nodes []Node, units map[string]map[target]unit) Layers {
	var all, cp, worker []unit

	machines := map[string][]unit{}

	targets := map[target]bool{}
	for _, u := range units {
		for to := range u {
			targets[to] = true
		}
	}

	sorted := make([]target, 0, len(targets))
	for to := range targets {
		sorted = append(sorted, to)
	}

	slices.SortFunc(sorted, func(a, b target) int { return strings.Compare(a.String(), b.String()) })

	sameOn := func(scope []Node, to target) (unit, bool) {
		var first *unit

		for _, n := range scope {
			u, ok := units[n.Name][to]
			if !ok {
				return unit{}, false
			}

			if first == nil {
				first = &u
			} else if *first != u {
				return unit{}, false
			}
		}

		if first == nil {
			// An empty scope (e.g. a cluster without workers) shares nothing.
			return unit{}, false
		}

		return *first, true
	}

	byRole := func(role machine.Type) []Node {
		var out []Node

		for _, n := range nodes {
			if n.Role == role {
				out = append(out, n)
			}
		}

		return out
	}

	cps, workers := byRole(machine.TypeControlPlane), byRole(machine.TypeWorker)

	for _, to := range sorted {
		if u, ok := sameOn(nodes, to); ok {
			all = append(all, u)

			continue
		}

		cpU, cpSame := sameOn(cps, to)
		wU, wSame := sameOn(workers, to)

		if cpSame {
			cp = append(cp, cpU)
		}

		if wSame {
			worker = append(worker, wU)
		}

		for _, n := range nodes {
			u, ok := units[n.Name][to]
			if !ok {
				continue
			}

			if (n.Role == machine.TypeControlPlane && cpSame) || (n.Role == machine.TypeWorker && wSame) {
				continue
			}

			machines[n.Name] = append(machines[n.Name], u)
		}
	}

	assemble := func(placed []unit) []string {
		var out []string

		for pass := range 2 {
			var docs []string

			for _, u := range placed {
				if u[pass] != "" {
					docs = append(docs, u[pass])
				}
			}

			if len(docs) > 0 {
				out = append(out, strings.Join(docs, "---\n"))
			}
		}

		return out
	}

	layers := Layers{All: assemble(all), ControlPlane: assemble(cp), Worker: assemble(worker), Machines: map[string][]string{}}

	for name, placed := range machines {
		if p := assemble(placed); len(p) > 0 {
			layers.Machines[name] = p
		}
	}

	return layers
}

// prove renders every node from base + layers and requires its live config back, then checks
// the rendered secrets against the bundle.
func prove(in Inputs, b *secrets.Bundle, nodes []Node, layers Layers) error {
	var verify []bundle.Node

	for _, n := range nodes {
		rendered, err := render.Generate(in.RenderInput(render.Input{Bundle: b, Role: n.Role, Patches: layers.patches(n.Name, n.Role)}))
		if err != nil {
			return fmt.Errorf("%s: render with derived patches: %w", n.Name, err)
		}

		got, err := render.Normalize(rendered)
		if err != nil {
			return fmt.Errorf("%s: %w", n.Name, err)
		}

		if err := render.Reproduces("derived patches", got, n.Live); err != nil {
			return fmt.Errorf("%s: %w", n.Name, err)
		}

		verify = append(verify, bundle.Node{Name: n.Name, Config: rendered})
	}

	return bundle.Verify(b, verify)
}

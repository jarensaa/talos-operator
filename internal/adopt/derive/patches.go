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

// For returns the patches for one node in apply order: all, role, machine.
func (l Layers) For(name string, role machine.Type) [][]byte {
	layers := [][]string{l.All, l.Worker, l.Machines[name]}
	if role == machine.TypeControlPlane {
		layers[1] = l.ControlPlane
	}

	var out [][]byte

	for _, layer := range layers {
		for _, p := range layer {
			out = append(out, []byte(p))
		}
	}

	return out
}

// unit is everything one node's patches do to one document, per pass. Keeping a document's
// passes together means wherever the unit lands, its operations keep their order.
type unit [2]string

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

	// units[node][docKey] is what the node's patches do to that document.
	units := map[string]map[string]unit{}

	for _, n := range nodes {
		u, err := nodeUnits(bases[n.Role], n.Live)
		if err != nil {
			return Layers{}, fmt.Errorf("%s: %w", n.Name, err)
		}

		for key := range u {
			if slices.ContainsFunc(identityKinds, func(k string) bool { return strings.Contains(key, "/"+k) }) {
				return Layers{}, fmt.Errorf("%w: %s differs on %s", ErrIdentityPatch, key, n.Name)
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

// nodeUnits diffs base against live and groups the resulting patch documents by document key.
func nodeUnits(base, live config.Provider) (map[string]unit, error) {
	patches, err := configdiff.Patch(base, live)
	if err != nil {
		return nil, fmt.Errorf("diff: %w", err)
	}

	units := map[string]unit{}

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

		for key, doc := range docs {
			u := units[key]
			u[pass] = doc
			units[key] = u
		}
	}

	return units, nil
}

// splitDocs splits a multi-document patch into documents keyed like render.DocKey, with
// "$patch: delete" selectors keyed separately from merges.
func splitDocs(raw []byte) (map[string]string, error) {
	out := map[string]string{}

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

		key := head.APIVersion + "/" + head.Kind
		if head.Kind == "" {
			key = "v1alpha1"
		}

		if head.Name != "" {
			key += "/" + head.Name
		}

		if head.Patch == "delete" {
			key += "#delete"
		}

		out[key] = part + "\n"
	}

	return out, nil
}

// factor places each unit in the widest layer where it is identical on every node of that scope.
func factor(nodes []Node, units map[string]map[string]unit) Layers {
	type placed struct {
		key string
		u   unit
	}

	var all, cp, worker []placed

	machines := map[string][]placed{}

	keys := map[string]bool{}
	for _, u := range units {
		for k := range u {
			keys[k] = true
		}
	}

	sorted := make([]string, 0, len(keys))
	for k := range keys {
		sorted = append(sorted, k)
	}

	slices.Sort(sorted)

	sameOn := func(scope []Node, key string) (unit, bool) {
		var first *unit

		for _, n := range scope {
			u, ok := units[n.Name][key]
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

	for _, key := range sorted {
		if u, ok := sameOn(nodes, key); ok {
			all = append(all, placed{key, u})

			continue
		}

		cpU, cpSame := sameOn(cps, key)
		wU, wSame := sameOn(workers, key)

		if cpSame {
			cp = append(cp, placed{key, cpU})
		}

		if wSame {
			worker = append(worker, placed{key, wU})
		}

		for _, n := range nodes {
			u, ok := units[n.Name][key]
			if !ok {
				continue
			}

			if (n.Role == machine.TypeControlPlane && cpSame) || (n.Role == machine.TypeWorker && wSame) {
				continue
			}

			machines[n.Name] = append(machines[n.Name], placed{key, u})
		}
	}

	assemble := func(ps []placed) []string {
		var out []string

		for pass := range 2 {
			var docs []string

			for _, p := range ps {
				if p.u[pass] != "" {
					docs = append(docs, p.u[pass])
				}
			}

			if len(docs) > 0 {
				out = append(out, strings.Join(docs, "---\n"))
			}
		}

		return out
	}

	layers := Layers{All: assemble(all), ControlPlane: assemble(cp), Worker: assemble(worker), Machines: map[string][]string{}}

	for name, ps := range machines {
		if p := assemble(ps); len(p) > 0 {
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
		rendered, err := render.Generate(in.RenderInput(render.Input{Bundle: b, Role: n.Role, Patches: layers.For(n.Name, n.Role)}))
		if err != nil {
			return fmt.Errorf("%s: render with derived patches: %w", n.Name, err)
		}

		got, err := render.Normalize(rendered)
		if err != nil {
			return fmt.Errorf("%s: %w", n.Name, err)
		}

		want, err := render.Normalize(n.Live)
		if err != nil {
			return fmt.Errorf("%s: normalize live config: %w", n.Name, err)
		}

		diffs, err := render.Diff(got, want)
		if err != nil {
			return fmt.Errorf("%s: %w", n.Name, err)
		}

		if len(diffs) > 0 {
			var text strings.Builder
			for _, d := range diffs {
				text.WriteString(d.Text)
			}

			return fmt.Errorf("%s: derived patches do not reproduce the live config:\n%s", n.Name, text.String())
		}

		verify = append(verify, bundle.Node{Name: n.Name, Config: rendered})
	}

	return bundle.Verify(b, verify)
}

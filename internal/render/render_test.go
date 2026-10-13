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
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/jarensaa/talos-operator/internal/render"
	"github.com/jarensaa/talos-operator/internal/talos"
	"github.com/jarensaa/talos-operator/internal/talos/talostest"
)

const (
	// Document keys the fixtures produce.
	keyKubeNode render.DocKey = "v1alpha1/KubeNodeConfig"
	keyBond     render.DocKey = "v1alpha1/BondConfig/bond0"
	keyDHCP     render.DocKey = "v1alpha1/DHCPv4Config/bond0"

	labelPatch = `apiVersion: v1alpha1
kind: KubeNodeConfig
labels:
  example.com/layer: %s
`
)

func mustRender(t *testing.T, in render.Input) *render.Config {
	t.Helper()

	out, err := render.Render(in)
	if err != nil {
		t.Fatal(err)
	}

	return out
}

func TestRenderIsDeterministic(t *testing.T) {
	a := mustRender(t, talostest.Input(t, talostest.NetworkPatch))
	b := mustRender(t, talostest.Input(t, talostest.NetworkPatch))

	if a.Hash != b.Hash {
		t.Fatal("identical inputs must render identical hashes")
	}
}

func TestDocumentOrderDoesNotChangeTheHash(t *testing.T) {
	parts := strings.SplitN(talostest.NetworkPatch, "---\n", 2)
	reordered := parts[1] + "---\n" + parts[0]

	if mustRender(t, talostest.Input(t, talostest.NetworkPatch)).Hash != mustRender(t, talostest.Input(t, reordered)).Hash {
		t.Fatal("document order in patches must not change the hash")
	}
}

func TestLaterPatchLayersWin(t *testing.T) {
	out := mustRender(t, talostest.Input(t,
		strings.ReplaceAll(labelPatch, "%s", "cluster"),
		strings.ReplaceAll(labelPatch, "%s", "machine"),
	))

	if !strings.Contains(string(out.Docs[keyKubeNode]), "example.com/layer: machine") {
		t.Fatalf("machine layer must override the cluster layer:\n%s", out.Docs[keyKubeNode])
	}
}

func TestJSON6902PatchesAreRejected(t *testing.T) {
	_, err := render.Render(talostest.Input(t, `- op: add
  path: /machine/network/hostname
  value: worker1
`))
	if !errors.Is(err, render.ErrJSONPatch) {
		t.Fatalf("expected ErrJSONPatch, got %v", err)
	}
}

func TestMissingContractIsRejected(t *testing.T) {
	in := talostest.Input(t)
	in.Contract = nil

	if _, err := render.Render(in); !errors.Is(err, render.ErrNoContract) {
		t.Fatalf("expected ErrNoContract, got %v", err)
	}
}

func TestDocumentsAreKeyed(t *testing.T) {
	out := mustRender(t, talostest.Input(t, talostest.NetworkPatch))

	for _, key := range []render.DocKey{render.RootKey, "v1alpha1/KubeClusterConfig", keyBond, keyDHCP} {
		if _, ok := out.Docs[key]; !ok {
			t.Errorf("missing document %s (have %v)", key, out.Keys)
		}
	}

	if out.Keys[0] != render.RootKey {
		t.Fatalf("the v1alpha1 document must come first, got %s", out.Keys[0])
	}

	for key, doc := range out.Docs {
		if strings.Contains(string(doc), "#") {
			t.Errorf("%s still has comments", key)
		}
	}
}

// TestLiveRoundTrip is the comparison adoption depends on: a node that was given the rendered
// bytes reports them back as its live config, and that must normalize to the same hash.
func TestLiveRoundTrip(t *testing.T) {
	rendered := mustRender(t, talostest.Input(t, talostest.NetworkPatch))

	live, err := talos.ParseMachineConfig(rendered.Bytes)
	if err != nil {
		t.Fatal(err)
	}

	normalized, err := render.Normalize(live)
	if err != nil {
		t.Fatal(err)
	}

	if normalized.Hash != rendered.Hash {
		t.Fatal("a live config equal to the rendered one must hash identically")
	}
}

func TestBytesOrderedLikeFollowsTheOtherConfig(t *testing.T) {
	cfg := mustRender(t, talostest.Input(t, talostest.NetworkPatch, strings.ReplaceAll(labelPatch, "%s", "x")))

	// A config whose documents come in the reverse order.
	reversed := &render.Config{Docs: cfg.Docs}
	for i := len(cfg.Keys) - 1; i >= 0; i-- {
		reversed.Keys = append(reversed.Keys, cfg.Keys[i])
	}

	encoded := cfg.BytesOrderedLike(reversed)

	parsed, err := talos.ParseMachineConfig(encoded)
	if err != nil {
		t.Fatal(err)
	}

	again, err := render.Normalize(parsed)
	if err != nil {
		t.Fatal(err)
	}

	if again.Hash != cfg.Hash {
		t.Fatal("reordering must not change content")
	}

	// v1alpha1 stays first in a Talos container; the other documents follow the given order.
	var want []render.DocKey
	for _, k := range reversed.Keys {
		if k != render.RootKey {
			want = append(want, k)
		}
	}

	got := slices.DeleteFunc(slices.Clone(again.Keys), func(k render.DocKey) bool { return k == render.RootKey })
	if !slices.Equal(got, want) {
		t.Fatalf("order: got %v, want %v", got, want)
	}
}

func TestADocumentKeySaysItsKind(t *testing.T) {
	for key, kind := range map[render.DocKey]string{
		render.RootKey:                  "",
		keyKubeNode:                     "KubeNodeConfig",
		keyBond:                         "BondConfig",
		"v1alpha1/UserVolumeConfig/a/b": "UserVolumeConfig",
	} {
		if got := key.Kind(); got != kind {
			t.Errorf("%s: kind %q, want %q", key, got, kind)
		}
	}

	// A key built from what a document states is the key the document has in a config.
	for key, of := range map[render.DocKey][3]string{
		keyBond:        {"v1alpha1", "BondConfig", "bond0"},
		keyKubeNode:    {"v1alpha1", "KubeNodeConfig", ""},
		render.RootKey: {"", "", ""},
	} {
		if got := render.KeyOf(of[0], of[1], of[2]); got != key {
			t.Errorf("%v: key %q, want %q", of, got, key)
		}
	}
}

func TestLayersApplyClusterThenPoolThenMachine(t *testing.T) {
	got := render.Layered([]string{"cluster-1", "cluster-2"}, []string{"pool"}, []string{"machine"})
	if !slices.Equal(got, []string{"cluster-1", "cluster-2", "pool", "machine"}) {
		t.Fatalf("got %v", got)
	}

	if got := render.Layered[string](nil, nil, []string{"machine"}); !slices.Equal(got, []string{"machine"}) {
		t.Fatalf("a machine without shared layers: %v", got)
	}
}

func TestReproduces(t *testing.T) {
	rendered := mustRender(t, talostest.Input(t, talostest.NetworkPatch))

	live, err := talos.ParseMachineConfig(rendered.Bytes)
	if err != nil {
		t.Fatal(err)
	}

	if err := render.Reproduces("the specs", rendered, live); err != nil {
		t.Fatalf("a config is reproduced by itself: %v", err)
	}

	// What differs is shown, and named after what was rendered.
	err = render.Reproduces("the specs", mustRender(t, talostest.Input(t)), live)
	if err == nil || !strings.HasPrefix(err.Error(), "the specs do not reproduce the live config:\n") ||
		!strings.Contains(err.Error(), "+bondMode: 802.3ad") {
		t.Fatalf("got %v", err)
	}
}

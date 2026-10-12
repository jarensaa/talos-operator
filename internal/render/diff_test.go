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
	"strings"
	"testing"

	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	"github.com/siderolabs/talos/pkg/machinery/config/machine"

	"github.com/jarensaa/talos-operator/internal/render"
	"github.com/jarensaa/talos-operator/internal/talos/talostest"
)

func TestDiffOfEqualConfigsIsEmpty(t *testing.T) {
	a := mustRender(t, talostest.Input(t, talostest.NetworkPatch))
	b := mustRender(t, talostest.Input(t, talostest.NetworkPatch))

	diffs, err := render.Diff(a, b)
	if err != nil || len(diffs) != 0 {
		t.Fatalf("diffs=%v err=%v", diffs, err)
	}
}

func TestDiffReportsDocumentsByKeyAndOp(t *testing.T) {
	base := mustRender(t, talostest.Input(t, strings.ReplaceAll(labelPatch, "%s", "one")))
	changed := mustRender(t, talostest.Input(t, strings.ReplaceAll(labelPatch, "%s", "two"), talostest.NetworkPatch))

	diffs, err := render.Diff(base, changed)
	if err != nil {
		t.Fatal(err)
	}

	got := map[render.DocKey]render.DiffOp{}
	for _, d := range diffs {
		got[d.Key] = d.Op
	}

	want := map[render.DocKey]render.DiffOp{
		keyKubeNode: render.DiffChanged,
		keyBond:     render.DiffAdded,
		keyDHCP:     render.DiffAdded,
	}

	for k, op := range want {
		if got[k] != op {
			t.Errorf("%s: got %q, want %q (all: %v)", k, got[k], op, got)
		}
	}

	if len(got) != len(want) {
		t.Errorf("unexpected diffs: %v", got)
	}

	for _, d := range diffs {
		if d.Key == keyKubeNode && !strings.Contains(d.Text, "+    example.com/layer: two") {
			t.Errorf("changed document must show the change:\n%s", d.Text)
		}
	}
}

// TestDiffNeverLeaksSecrets diffs configs from two different bundles, the worst case where every
// secret differs, for both roles, and scans the output for any secret or key material.
func TestDiffNeverLeaksSecrets(t *testing.T) {
	other, err := secrets.NewBundle(secrets.NewClock(), config.TalosVersion1_14)
	if err != nil {
		t.Fatal(err)
	}

	for _, role := range []machine.Type{machine.TypeControlPlane, machine.TypeWorker} {
		t.Run(role.String(), func(t *testing.T) {
			ia := talostest.Input(t, talostest.NetworkPatch)
			ia.Role = role

			ib := ia
			ib.Bundle = other

			a := mustRender(t, ia)
			b := mustRender(t, ib)

			diffs, err := render.Diff(a, b)
			if err != nil {
				t.Fatal(err)
			}

			if len(diffs) == 0 {
				t.Fatal("configs from different bundles must differ")
			}

			var all strings.Builder
			for _, d := range diffs {
				all.WriteString(d.Text)
			}

			out := all.String()

			if strings.Contains(out, "BEGIN") || strings.Contains(out, "LS0tLS1CRUdJT") {
				t.Fatal("diff contains PEM material")
			}

			for _, s := range append(talostest.SecretValues(ia.Bundle), talostest.SecretValues(other)...) {
				if strings.Contains(out, s) {
					t.Fatal("diff leaks a secret value")
				}
			}
		})
	}
}

func TestRedactKeepsNonSecretFields(t *testing.T) {
	out := string(render.Redact([]byte(
		"apiVersion: v1alpha1\nkind: KubeNodeConfig\nlabels:\n  zone: a\ncertSANs: [10.0.0.1]\ntoken: abc.def\n")))

	for _, keep := range []string{"zone: a", "10.0.0.1", "kind: KubeNodeConfig"} {
		if !strings.Contains(out, keep) {
			t.Errorf("redaction removed %q:\n%s", keep, out)
		}
	}

	if strings.Contains(out, "abc.def") {
		t.Errorf("token not redacted:\n%s", out)
	}
}

func TestRedactFailsClosedOnUnparsableInput(t *testing.T) {
	out := string(render.Redact([]byte("token: [unclosed super-secret")))
	if strings.Contains(out, "super-secret") {
		t.Fatalf("unparsable input must be redacted entirely: %s", out)
	}
}

func TestPatchDocKeysMatchDiffKeys(t *testing.T) {
	keys, err := render.PatchDocKeys([]byte(talostest.NetworkPatch + "---\n" + strings.ReplaceAll(labelPatch, "%s", "x")))
	if err != nil {
		t.Fatal(err)
	}

	want := []render.DocKey{keyBond, keyDHCP, keyKubeNode}
	if strings.Join(toStrings(keys), ",") != strings.Join(toStrings(want), ",") {
		t.Fatalf("keys: %v, want %v", keys, want)
	}

	legacy, err := render.PatchDocKeys([]byte("machine:\n  nodeLabels:\n    a: b\n"))
	if err != nil || len(legacy) != 1 || legacy[0] != render.RootKey {
		t.Fatalf("legacy patch keys: %v err=%v", legacy, err)
	}

	if _, err := render.PatchDocKeys([]byte("- op: add\n  path: /machine/foo\n  value: bar\n")); err == nil {
		t.Fatal("JSON6902 patches must be refused")
	}
}

func TestWithholdHidesSensitiveDocuments(t *testing.T) {
	base := mustRender(t, talostest.Input(t, strings.ReplaceAll(labelPatch, "%s", "public")))
	changed := mustRender(t, talostest.Input(t, strings.ReplaceAll(labelPatch, "%s", "hunter2"), talostest.NetworkPatch))

	diffs, err := render.Diff(base, changed)
	if err != nil {
		t.Fatal(err)
	}

	withheld := render.Withhold(diffs, map[render.DocKey]bool{keyKubeNode: true})
	if len(withheld) != len(diffs) {
		t.Fatalf("documents must still be listed: %d of %d", len(withheld), len(diffs))
	}

	for _, d := range withheld {
		if strings.Contains(d.Text, "hunter2") {
			t.Fatalf("withheld value leaked in %s:\n%s", d.Key, d.Text)
		}

		if d.Key == keyBond && !strings.Contains(d.Text, "802.3ad") {
			t.Errorf("unrelated documents must keep their diff:\n%s", d.Text)
		}
	}
}

func toStrings(keys []render.DocKey) []string {
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		out = append(out, string(k))
	}

	return out
}

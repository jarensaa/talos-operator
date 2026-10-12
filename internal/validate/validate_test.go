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

package validate_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jarensaa/talos-operator/internal/catalog"
	"github.com/jarensaa/talos-operator/internal/talos/talostest"
	"github.com/jarensaa/talos-operator/internal/validate"
)

const (
	schematic = talostest.SchematicID
	other     = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
)

// repository is the manifests of one cluster as a repository holds them, next to things that are
// none of the operator's.
type repository struct {
	talos, kubernetes string
	// rollout is the YAML of spec.rollout, workers what the worker pool adds, and patch a
	// config document of the worker pool.
	rollout, workers, patch string
	// extra is appended to the TalosCluster's spec.
	extra string
}

func valid() repository { return repository{talos: "v1.14.2", kubernetes: "1.37.1"} }

func indent(text, by string) string {
	if text == "" {
		return ""
	}

	return by + strings.ReplaceAll(strings.TrimRight(text, "\n"), "\n", "\n"+by) + "\n"
}

func (r repository) write(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()

	cluster := `apiVersion: talos.jarensaa.github.io/v1alpha1
kind: TalosCluster
metadata:
  name: my-cluster
  namespace: talos-operator
spec:
  clusterName: my-cluster
  controlPlaneEndpoint: https://cluster.example.com:6443
  versions:
    talos: ` + r.talos + `
    kubernetes: ` + r.kubernetes + `
  configGeneration:
    versionContract: v1.14
    installer:
      factoryURL: https://factory.example.com
      platform: metal
      schematicID: ` + schematic + "\n"
	if r.rollout != "" {
		cluster += "  rollout:\n" + indent(r.rollout, "    ")
	}

	cluster += indent(r.extra, "  ")

	pools := `apiVersion: talos.jarensaa.github.io/v1alpha1
kind: TalosMachinePool
metadata:
  name: control-plane
  namespace: talos-operator
spec:
  clusterRef: {name: my-cluster}
  role: ControlPlane
---
apiVersion: talos.jarensaa.github.io/v1alpha1
kind: TalosMachinePool
metadata:
  name: workers
  namespace: talos-operator
spec:
  clusterRef: {name: my-cluster}
  role: Worker
  config:
    - configRef: {name: labels}
` + indent(r.patch, "    ") + indent(r.workers, "  ")

	machines := `apiVersion: talos.jarensaa.github.io/v1alpha1
kind: TalosMachine
metadata:
  name: node-a
  namespace: talos-operator
spec:
  poolRef: {name: control-plane}
  address: 10.0.0.11
  identity: {systemUUID: aaaaaaaa-bbbb-4ccc-8ddd-000000000001}
---
apiVersion: talos.jarensaa.github.io/v1alpha1
kind: TalosMachine
metadata:
  name: node-b
  namespace: talos-operator
spec:
  poolRef: {name: workers}
  address: 10.0.0.12
  identity: {systemUUID: aaaaaaaa-bbbb-4ccc-8ddd-000000000002}
---
apiVersion: talos.jarensaa.github.io/v1alpha1
kind: TalosMachineConfig
metadata:
  name: labels
  namespace: talos-operator
spec:
  documents: |
    apiVersion: v1alpha1
    kind: KubeNodeConfig
    labels:
      example.com/zone: a
`

	// What else a repository holds: other kinds, and YAML that is no object at all.
	others := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unrelated}\ndata: {versions: nonsense}\n---\njust: values\n"

	for name, content := range map[string]string{
		"cluster.yaml": cluster, "pools.yaml": pools, "nested/machines.yml": machines, "other.yaml": others, "README.md": "not YAML: [",
	} {
		path := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			t.Fatal(err)
		}

		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	return dir
}

func upstream() *catalog.Fake {
	return &catalog.Fake{
		Installers: map[string]bool{schematic + " v1.14.2": true, schematic + " v1.14.1": true, schematic + " v1.13.5": true},
		Latest:     map[string]string{"1.37": "1.37.1"},
		Tags:       map[string]bool{"v1.36.2": true, "v1.37.0": true, "v1.35.4": true},
	}
}

func run(t *testing.T, opts validate.Options) (problems, skipped []string) {
	t.Helper()

	findings, err := validate.Run(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}

	for _, f := range findings {
		if f.Skipped {
			skipped = append(skipped, f.String())
		} else {
			problems = append(problems, f.String())
		}
	}

	return problems, skipped
}

func TestAValidRepositoryPasses(t *testing.T) {
	dir := valid().write(t)

	problems, skipped := run(t, validate.Options{Paths: []string{dir}, Source: upstream()})
	if len(problems) != 0 || len(skipped) != 0 {
		t.Fatalf("problems %q, skipped %q", problems, skipped)
	}

	// Offline it passes too, and says what it did not ask.
	problems, skipped = run(t, validate.Options{Paths: []string{dir}})
	if len(problems) != 0 || len(skipped) != 1 || !strings.Contains(skipped[0], "offline") {
		t.Fatalf("problems %q, skipped %q", problems, skipped)
	}

	// Files can be named one by one as well.
	problems, _ = run(t, validate.Options{
		Paths:  []string{filepath.Join(dir, "cluster.yaml"), filepath.Join(dir, "pools.yaml"), filepath.Join(dir, "nested")},
		Source: upstream(),
	})
	if len(problems) != 0 {
		t.Fatalf("problems %q", problems)
	}
}

func TestProblems(t *testing.T) {
	down := upstream()
	down.Err = errors.New("no route to host")

	with := func(mutate func(*repository)) repository {
		r := valid()
		mutate(&r)

		return r
	}

	tests := []struct {
		name   string
		repo   repository
		base   *repository
		source catalog.Source
		// want is part of the one problem that is expected.
		want string
	}{
		// What the pull request that prompted this check would have been: an installer that does
		// not exist.
		{"a Talos release Image Factory does not build", with(func(r *repository) { r.talos = "v1.14.9" }), nil, upstream(),
			"no installer of Talos v1.14.9"},
		{"a pool's own schematic that does not exist", with(func(r *repository) { r.workers = "installer: {schematicID: " + other + "}" }),
			nil, upstream(), "for schematic " + other},
		// The images a Kubernetes upgrade would pull, as the rendered configs name them.
		{"a Kubernetes release whose images do not exist", with(func(r *repository) { r.kubernetes = "1.37.9" }), nil, upstream(),
			"the image ghcr.io/siderolabs/kubelet:v1.37.9 does not exist"},
		// Not knowing is not passing.
		{"upstream cannot be asked", valid(), nil, down, "could not be checked: no route to host"},
		{"a Talos this operator does not support", with(func(r *repository) { r.talos = "v1.15.0" }), nil, upstream(),
			"newer than this operator"},
		{"a Talos that does not run the Kubernetes named", with(func(r *repository) { r.kubernetes = "1.38.0" }), nil, upstream(),
			"does not run Kubernetes 1.38.0"},
		// The step from what the base asks for.
		{"a Talos downgrade", with(func(r *repository) { r.talos = "v1.14.1" }), new(valid()), upstream(), "downgrades are not supported"},
		{"a Kubernetes downgrade", with(func(r *repository) { r.kubernetes = "1.37.0" }), new(valid()), upstream(), "not downgraded"},
		// What the API server takes as a string.
		{"a schedule that is none", with(func(r *repository) { r.rollout = "maintenanceWindows:\n  - {schedule: \"61 2 * * *\", duration: 1h}" }),
			nil, upstream(), "maintenanceWindows[0]"},
		{"a time zone that is none", with(func(r *repository) {
			r.rollout = "maintenanceWindows:\n  - {schedule: \"0 2 * * *\", duration: 1h, timeZone: Europe/Atlantis}"
		}), nil, upstream(), "Europe/Atlantis"},
		{"a gate that does not compile", with(func(r *repository) {
			r.rollout = "gates:\n  - name: storage\n    object: {apiVersion: v1, kind: ConfigMap, name: storage}\n" +
				"    expression: object.data.health =="
		}), nil, upstream(), "gate storage: the expression does not compile"},
		// A config document Talos does not take.
		{"a config that does not render", with(func(r *repository) {
			r.patch = "- inline: |\n    apiVersion: v1alpha1\n    kind: NoSuchConfig\n    name: x"
		}), nil, upstream(), "TalosMachine talos-operator/node-b: its config does not render"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := validate.Options{Paths: []string{tt.repo.write(t)}, Source: tt.source}
			if tt.base != nil {
				opts.Base = []string{tt.base.write(t)}
			}

			problems, _ := run(t, opts)
			if len(problems) == 0 || !strings.Contains(strings.Join(problems, "\n"), tt.want) {
				t.Fatalf("got %q, want a problem about %q", problems, tt.want)
			}

			for _, p := range problems {
				if !strings.HasPrefix(p, "TalosCluster talos-operator/my-cluster: ") && !strings.HasPrefix(p, "TalosMachine ") {
					t.Fatalf("a problem must say what it is about: %q", p)
				}
			}
		})
	}
}

// A bump is checked against where the base left the cluster: an upgrade passes, in whichever
// order Talos and Kubernetes have to go.
func TestTheStepFromTheBase(t *testing.T) {
	base := repository{talos: "v1.13.5", kubernetes: "1.36.2"}
	next := valid()

	problems, _ := run(t, validate.Options{Paths: []string{next.write(t)}, Base: []string{base.write(t)}, Source: upstream()})
	if len(problems) != 0 {
		t.Fatalf("an upgrade of both, Talos first, must pass: %q", problems)
	}

	// A base that names no versions yet says nothing about the step.
	empty := t.TempDir()
	bare := "apiVersion: talos.jarensaa.github.io/v1alpha1\nkind: TalosCluster\n" +
		"metadata: {name: my-cluster, namespace: talos-operator}\nspec: {}\n"
	if err := os.WriteFile(filepath.Join(empty, "cluster.yaml"), []byte(bare), 0o600); err != nil {
		t.Fatal(err)
	}

	problems, _ = run(t, validate.Options{Paths: []string{next.write(t)}, Base: []string{empty}, Source: upstream()})
	if len(problems) != 0 {
		t.Fatalf("problems %q", problems)
	}

	// And a cluster that names none itself, as before adoption, has nothing to check about them.
	problems, skipped := run(t, validate.Options{Paths: []string{empty}, Source: upstream()})
	if len(problems) != 0 || len(skipped) != 1 || !strings.Contains(skipped[0], "names no versions yet") {
		t.Fatalf("problems %q, skipped %q", problems, skipped)
	}
}

func TestWhatIsNotCheckedIsSaid(t *testing.T) {
	repo := valid()
	repo.patch = "- secretKeyRef: {name: registry-credentials, key: config.yaml}"

	problems, skipped := run(t, validate.Options{Paths: []string{repo.write(t)}, Source: upstream()})
	if len(problems) != 0 || len(skipped) != 1 || !strings.Contains(skipped[0], "TalosMachine talos-operator/node-b") ||
		!strings.Contains(skipped[0], "Secret") {
		t.Fatalf("problems %q, skipped %q", problems, skipped)
	}
}

func TestWhatCannotBeReadIsAnError(t *testing.T) {
	ctx := context.Background()

	// A field the API does not have is a typo, and the API server would drop it without a word.
	typo := valid()
	typo.extra = "pasued: true"

	if _, err := validate.Run(ctx, validate.Options{Paths: []string{typo.write(t)}}); err == nil || !strings.Contains(err.Error(), "pasued") {
		t.Fatalf("got %v", err)
	}

	if _, err := validate.Run(ctx, validate.Options{Paths: []string{filepath.Join(t.TempDir(), "missing")}}); err == nil {
		t.Fatal("a path that does not exist must be an error")
	}

	// Files without a TalosCluster are not a cluster that passes.
	none := t.TempDir()
	unrelated := []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: x}\n")
	if err := os.WriteFile(filepath.Join(none, "other.yaml"), unrelated, 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := validate.Run(ctx, validate.Options{Paths: []string{none}}); err == nil || !strings.Contains(err.Error(), "no TalosCluster") {
		t.Fatalf("got %v", err)
	}

	gone := []string{filepath.Join(none, "gone")}
	if _, err := validate.Run(ctx, validate.Options{Paths: []string{valid().write(t)}, Base: gone}); err == nil {
		t.Fatal("a base that does not exist must be an error")
	}
}

// The clock only matters to a window's schedule, which is checked the same at any time.
func TestTheClockDoesNotDecide(t *testing.T) {
	repo := valid()
	repo.rollout = "maintenanceWindows:\n  - {schedule: \"0 2 * * 6\", duration: 3h, timeZone: Europe/Oslo}"
	dir := repo.write(t)

	for _, now := range []time.Time{time.Date(2026, 3, 7, 2, 30, 0, 0, time.UTC), time.Date(2026, 3, 4, 12, 0, 0, 0, time.UTC)} {
		problems, _ := run(t, validate.Options{Paths: []string{dir}, Source: upstream(), Now: func() time.Time { return now }})
		if len(problems) != 0 {
			t.Fatalf("at %s: %q", now, problems)
		}
	}
}

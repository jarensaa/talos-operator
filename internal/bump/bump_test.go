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

package bump_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jarensaa/talos-operator/internal/bump"
	"github.com/jarensaa/talos-operator/internal/catalog"
)

const schematic = "376567988ad370138ad8b2698212367b8edcb69b5fd68c80be1f2ec7d603b4ba"

// cluster is a TalosCluster as a human keeps it in a repository: with comments, and with the
// versions written as given, quotes and all.
func cluster(name, talos, kubernetes string) string {
	contract := "v1.14"
	if strings.Contains(talos, "1.13.") {
		contract = "v1.13"
	}

	return `# The cluster ` + name + `. Its versions are bumped by a job, and by nobody else.
apiVersion: talos.jarensaa.github.io/v1alpha1
kind: TalosCluster
metadata:
  name: ` + name + `
  namespace: talos-operator
spec:
  clusterName: ` + name + `
  controlPlaneEndpoint: https://cluster.example.com:6443
  versions:
    talos: ` + talos + `  # the system
    # Kubernetes follows Talos.
    kubernetes: ` + kubernetes + `
  configGeneration:
    versionContract: ` + contract + `
    installer:
      factoryURL: https://factory.example.com
      platform: metal
      schematicID: ` + schematic + "\n"
}

// repository writes files into a directory, next to something that is none of the operator's.
func repository(t *testing.T, files map[string]string) string {
	t.Helper()

	dir := t.TempDir()
	files["other.yaml"] = "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unrelated}\ndata: {talos: v1.14.1}\n---\njust: values\n"

	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	return dir
}

func read(t *testing.T, dir, name string) string {
	t.Helper()

	raw, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}

	return string(raw)
}

// upstream is what exists: Talos up to a minor this operator does not know, Kubernetes up to a
// minor no Talos it knows runs, and an installer for every Talos release.
func upstream() *catalog.Fake {
	fake := &catalog.Fake{
		Talos:      []string{"v1.12.9", "v1.13.4", "v1.13.5", "v1.14.0", "v1.14.1", "v1.14.2", "v1.15.0"},
		Installers: map[string]bool{},
		Latest:     map[string]string{"1.31": "1.31.9", "1.32": "1.32.4", "1.36": "1.36.5", "1.37": "1.37.1", "1.38": "1.38.0"},
		Tags:       map[string]bool{"v1.31.2": true, "v1.36.2": true, "v1.37.0": true},
	}

	for _, tag := range fake.Talos {
		fake.Installers[schematic+" "+tag] = true
	}

	return fake
}

func run(t *testing.T, opts bump.Options) *bump.Result {
	t.Helper()

	if opts.Source == nil {
		opts.Source = upstream()
	}

	result, err := bump.Run(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}

	return result
}

// step is the one bump of a run over one cluster, as it is printed, or "" when there was none.
func step(t *testing.T, result *bump.Result) string {
	t.Helper()

	if len(result.Invalid) != 0 || len(result.Outcomes) != 1 {
		t.Fatalf("invalid %q, outcomes %+v", result.Invalid, result.Outcomes)
	}

	if result.Outcomes[0].Step == nil {
		return ""
	}

	return result.Outcomes[0].Step.String()
}

// The whole of it: run again and again, a cluster goes one step at a time to the newest releases
// this operator upgrades to, Talos before Kubernetes, and then stays.
func TestRunAfterRunTakesAClusterToTheNewestReleases(t *testing.T) {
	dir := repository(t, map[string]string{"cluster.yaml": cluster("my-cluster", "v1.13.4", "1.36.2")})

	for _, want := range []string{
		"Talos v1.13.4 to v1.13.5",
		"Talos v1.13.5 to v1.14.2",
		"Kubernetes 1.36.2 to 1.36.5",
		"Kubernetes 1.36.5 to 1.37.1",
		"",
	} {
		if got := step(t, run(t, bump.Options{Paths: []string{dir}})); got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	}

	// Nothing but the two versions changed: the comments, the order and the other file are as
	// they were. The contract is not a version to bump either.
	want := cluster("my-cluster", "v1.14.2", "1.37.1")
	want = strings.Replace(want, "versionContract: v1.14", "versionContract: v1.13", 1)

	if got := read(t, dir, "cluster.yaml"); got != want {
		t.Fatalf("the file is now:\n%s", got)
	}

	if got := read(t, dir, "other.yaml"); !strings.Contains(got, "talos: v1.14.1") {
		t.Fatalf("a file that is none of the operator's was changed:\n%s", got)
	}

	// What is newer still and not taken is said, with the reason.
	held := strings.Join(run(t, bump.Options{Paths: []string{dir}}).Outcomes[0].Held, "\n")
	for _, part := range []string{"Talos v1.15.0 exists and is not taken", "newer than this operator's Talos machinery",
		"Kubernetes 1.38.0 exists and is not taken", "does not run Kubernetes 1.38.0"} {
		if !strings.Contains(held, part) {
			t.Errorf("%q is not said in:\n%s", part, held)
		}
	}
}

// Talos goes first where it can, and where it cannot, Kubernetes does: each waits for the other
// by the rules Talos has about which Kubernetes it runs.
func TestTheTwoWaitForEachOther(t *testing.T) {
	tests := []struct {
		name              string
		talos, kubernetes string
		opts              bump.Options
		want              string
		// held is part of what is said about a release that was not taken.
		held string
	}{
		{"Talos first", "v1.14.1", "1.37.0", bump.Options{}, "Talos v1.14.1 to v1.14.2", ""},
		{"Kubernetes when it is asked for", "v1.14.1", "1.37.0", bump.Options{Kubernetes: true}, "Kubernetes 1.37.0 to 1.37.1", ""},
		{"Talos when it is asked for", "v1.14.1", "1.37.0", bump.Options{Talos: true}, "Talos v1.14.1 to v1.14.2", ""},
		{"both asked for is either", "v1.14.2", "1.37.0", bump.Options{Talos: true, Kubernetes: true}, "Kubernetes 1.37.0 to 1.37.1",
			"Talos v1.15.0 exists and is not taken"},
		// Talos 1.14 does not run Kubernetes 1.31, so Kubernetes has to move first.
		{"a Talos minor waits for Kubernetes", "v1.13.5", "1.31.9", bump.Options{}, "Kubernetes 1.31.9 to 1.32.4",
			"Talos v1.14.2 exists and is not taken: Talos v1.14.2 does not support Kubernetes 1.31.9"},
		{"and is not taken when only Talos is asked for", "v1.13.5", "1.31.9", bump.Options{Talos: true}, "",
			"does not support Kubernetes 1.31.9"},
		// Talos 1.13 does not run Kubernetes 1.37, so Talos has to move first.
		{"a Kubernetes minor waits for Talos", "v1.13.5", "1.36.5", bump.Options{Kubernetes: true}, "",
			"Kubernetes 1.37.1 exists and is not taken: Talos v1.13.5 does not run Kubernetes 1.37.1"},
		{"which goes first by itself", "v1.13.5", "1.36.5", bump.Options{}, "Talos v1.13.5 to v1.14.2", ""},
		{"patch releases only", "v1.13.5", "1.36.2", bump.Options{PatchOnly: true}, "Kubernetes 1.36.2 to 1.36.5", ""},
		{"and no minor then", "v1.13.5", "1.36.5", bump.Options{PatchOnly: true}, "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dir := repository(t, map[string]string{"cluster.yaml": cluster("my-cluster", tt.talos, tt.kubernetes)})
			tt.opts.Paths = []string{dir}

			result := run(t, tt.opts)
			if got := step(t, result); got != tt.want {
				t.Fatalf("got %q, want %q", got, tt.want)
			}

			outcome := result.Outcomes[0]
			if len(outcome.Problems) != 0 {
				t.Fatalf("problems %q", outcome.Problems)
			}

			if held := strings.Join(outcome.Held, "\n"); !strings.Contains(held, tt.held) || tt.held == "" && held != "" {
				t.Fatalf("held %q, want %q", held, tt.held)
			}
		})
	}
}

// lagging is upstream while a release is out and an image of it is not: the kubelet Talos runs is
// published a while after the Kubernetes release it is of.
type lagging struct {
	*catalog.Fake
	missing string
}

func (l lagging) Image(ctx context.Context, image string) error {
	if strings.HasSuffix(image, ":"+l.missing) {
		return fmt.Errorf("the image %s does not exist: %w", image, catalog.ErrNotFound)
	}

	return l.Fake.Image(ctx, image)
}

// A release that exists and cannot be installed is not written: what the validate check would
// turn down in a pull request is turned down here, before there is one.
func TestWhatTheCheckTurnsDownIsNotWritten(t *testing.T) {
	// Image Factory lists the release, and has no installer of it for this schematic: an
	// extension the schematic names is not out for it yet.
	noInstaller := upstream()
	delete(noInstaller.Installers, schematic+" v1.14.2")

	down := upstream()
	down.Err = errors.New("no route to host")

	tests := []struct {
		name   string
		source catalog.Source
		opts   bump.Options
		want   string
		// problem is part of what is said about it.
		problem string
	}{
		{"an installer that does not exist, and Kubernetes goes instead", noInstaller, bump.Options{}, "Kubernetes 1.37.0 to 1.37.1",
			"Talos v1.14.1 to v1.14.2 was not written: TalosCluster talos-operator/my-cluster: Image Factory builds no installer"},
		{"an installer that does not exist", noInstaller, bump.Options{Talos: true}, "", "Image Factory builds no installer"},
		{"an image that is not published yet", lagging{upstream(), "v1.37.1"}, bump.Options{Kubernetes: true}, "",
			"Kubernetes 1.37.0 to 1.37.1 was not written: TalosCluster talos-operator/my-cluster: the image"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dir := repository(t, map[string]string{"cluster.yaml": cluster("my-cluster", "v1.14.1", "1.37.0")})
			tt.opts.Paths, tt.opts.Source = []string{dir}, tt.source

			result := run(t, tt.opts)
			if got := step(t, result); got != tt.want {
				t.Fatalf("got %q, want %q", got, tt.want)
			}

			if problems := strings.Join(result.Outcomes[0].Problems, "\n"); !strings.Contains(problems, tt.problem) {
				t.Fatalf("problems %q, want %q", problems, tt.problem)
			}

			if tt.want == "" && read(t, dir, "cluster.yaml") != cluster("my-cluster", "v1.14.1", "1.37.0") {
				t.Fatal("the file was changed")
			}
		})
	}

	// Manifests that have a problem as they are get no bump on top, and the problem is theirs.
	dir := repository(t, map[string]string{"cluster.yaml": cluster("my-cluster", "v1.14.1", "1.37.0")})

	result := run(t, bump.Options{Paths: []string{dir}, Source: down})
	if len(result.Invalid) == 0 || len(result.Outcomes) != 0 || !strings.Contains(result.Invalid[0].String(), "could not be checked") {
		t.Fatalf("invalid %q, outcomes %+v", result.Invalid, result.Outcomes)
	}

	if read(t, dir, "cluster.yaml") != cluster("my-cluster", "v1.14.1", "1.37.0") {
		t.Fatal("the file was changed")
	}
}

// A file is a human's: however it writes a version, the version is what changes, and nothing
// else in the file does.
func TestOnlyTheVersionChangesInTheFile(t *testing.T) {
	plain := cluster("my-cluster", "v1.14.1", "1.37.0")
	flow := strings.Replace(plain,
		"  versions:\n    talos: v1.14.1  # the system\n    # Kubernetes follows Talos.\n    kubernetes: 1.37.0\n",
		"  versions: {talos: v1.14.1, kubernetes: \"1.37.0\"} # on one line\n", 1)

	if flow == plain {
		t.Fatal("the fixture did not change")
	}

	tests := []struct {
		name    string
		content string
		opts    bump.Options
		// from and to are the one difference between the file before and after.
		from, to string
	}{
		{"as it is", plain, bump.Options{}, "talos: v1.14.1  # the system", "talos: v1.14.2  # the system"},
		{"double quotes", cluster("my-cluster", `"v1.14.1"`, "1.37.0"), bump.Options{}, `talos: "v1.14.1"`, `talos: "v1.14.2"`},
		{"single quotes", cluster("my-cluster", "v1.14.1", "'1.37.0'"), bump.Options{Kubernetes: true},
			"kubernetes: '1.37.0'", "kubernetes: '1.37.1'"},
		{"on one line", flow, bump.Options{Kubernetes: true}, `kubernetes: "1.37.0"} # on one line`, `kubernetes: "1.37.1"} # on one line`},
		{"on one line, the first", flow, bump.Options{}, "{talos: v1.14.1,", "{talos: v1.14.2,"},
		{"Windows line ends", strings.ReplaceAll(plain, "\n", "\r\n"), bump.Options{},
			"talos: v1.14.1  # the system\r\n", "talos: v1.14.2  # the system\r\n"},
		{"after other documents",
			"# Écrit à la main.\nkind: Namespace\napiVersion: v1\nmetadata: {name: talos-operator}\n--- # the cluster\n" + plain,
			bump.Options{}, "talos: v1.14.1  # the system", "talos: v1.14.2  # the system"},
		// The version is in a comment and in a name as well, and only the field is the version.
		{"the same text elsewhere", strings.Replace(plain, "# The cluster", "# talos: v1.14.1, once. The cluster", 1), bump.Options{},
			"    talos: v1.14.1  # the system", "    talos: v1.14.2  # the system"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dir := repository(t, map[string]string{"cluster.yaml": tt.content})
			tt.opts.Paths = []string{dir}

			if got := step(t, run(t, tt.opts)); got == "" {
				t.Fatal("nothing was bumped")
			}

			if strings.Count(tt.content, tt.from) != 1 {
				t.Fatalf("the fixture has %q %d times", tt.from, strings.Count(tt.content, tt.from))
			}

			if got, want := read(t, dir, "cluster.yaml"), strings.Replace(tt.content, tt.from, tt.to, 1); got != want {
				t.Fatalf("the file is now:\n%s\nwant:\n%s", got, want)
			}
		})
	}
}

// A version that the document does not write itself is shared with whatever else uses it, and is
// left to a human.
func TestAVersionWrittenSomewhereElseIsLeftAlone(t *testing.T) {
	// An alias to another document resolves to nothing: the check does not take the file, and
	// neither does this.
	aliased := "talosVersion: &talos v1.14.1\n---\n" +
		strings.Replace(cluster("my-cluster", "*talos", "1.37.0"), "*talos  # the system", "*talos", 1)
	dir := repository(t, map[string]string{"cluster.yaml": aliased})

	if _, err := bump.Run(context.Background(), bump.Options{Paths: []string{dir}, Source: upstream()}); err == nil {
		t.Fatal("a file the check cannot read must be an error")
	}

	// An anchor in the same document: the value is read, and where it is written is not the
	// field.
	anchored := strings.Replace(cluster("my-cluster", "*talos", "1.37.1"), "*talos  # the system", "*talos", 1)
	anchored = strings.Replace(anchored, "  namespace: talos-operator\n",
		"  namespace: talos-operator\n  annotations: {example.com/talos: &talos v1.14.1}\n", 1)
	dir = repository(t, map[string]string{"cluster.yaml": anchored})

	result := run(t, bump.Options{Paths: []string{dir}})
	if got := step(t, result); got != "" {
		t.Fatalf("got %q", got)
	}

	problems := strings.Join(result.Outcomes[0].Problems, "\n")
	if !strings.Contains(problems, "Talos v1.14.1 to v1.14.2 was not written: versions.talos") {
		t.Fatalf("problems %q", problems)
	}

	if read(t, dir, "cluster.yaml") != anchored {
		t.Fatal("the file was changed")
	}
}

// Every cluster of a repository is bumped, each by its own step, wherever the files hold them.
func TestEveryClusterIsBumped(t *testing.T) {
	both := cluster("cluster-a", "v1.14.1", "1.37.0") + "---\n" + cluster("cluster-b", "v1.14.2", "1.37.0")
	dir := repository(t, map[string]string{
		"clusters.yaml": both,
		"third.yml":     cluster("cluster-c", "v1.14.2", "1.37.1"),
		"unadopted.yaml": "apiVersion: talos.jarensaa.github.io/v1alpha1\nkind: TalosCluster\n" +
			"metadata: {name: cluster-d, namespace: talos-operator}\nspec: {}\n",
	})

	result := run(t, bump.Options{Paths: []string{dir}})

	var got []string

	for _, outcome := range result.Outcomes {
		line := outcome.Object + " in " + filepath.Base(outcome.File) + ":"
		if outcome.Step != nil {
			line += " " + outcome.Step.String()
		}

		got = append(got, line)
	}

	want := []string{
		"TalosCluster talos-operator/cluster-a in clusters.yaml: Talos v1.14.1 to v1.14.2",
		"TalosCluster talos-operator/cluster-b in clusters.yaml: Kubernetes 1.37.0 to 1.37.1",
		"TalosCluster talos-operator/cluster-c in third.yml:",
		"TalosCluster talos-operator/cluster-d in unadopted.yaml:",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("got:\n%s", strings.Join(got, "\n"))
	}

	if held := strings.Join(result.Outcomes[3].Held, "\n"); !strings.Contains(held, "names no versions yet") {
		t.Fatalf("held %q", held)
	}

	if got, want := read(t, dir, "clusters.yaml"),
		cluster("cluster-a", "v1.14.2", "1.37.0")+"---\n"+cluster("cluster-b", "v1.14.2", "1.37.1"); got != want {
		t.Fatalf("the file is now:\n%s", got)
	}
}

// A dry run says what a run would write, and writes nothing.
func TestADryRunWritesNothing(t *testing.T) {
	content := cluster("my-cluster", "v1.14.1", "1.37.0")
	dir := repository(t, map[string]string{"cluster.yaml": content})

	if got := step(t, run(t, bump.Options{Paths: []string{dir}, DryRun: true})); got != "Talos v1.14.1 to v1.14.2" {
		t.Fatalf("got %q", got)
	}

	if read(t, dir, "cluster.yaml") != content {
		t.Fatal("the file was changed")
	}
}

func TestWhatCannotBeReadIsAnError(t *testing.T) {
	ctx := context.Background()

	if _, err := bump.Run(ctx, bump.Options{Paths: []string{filepath.Join(t.TempDir(), "missing")}, Source: upstream()}); err == nil {
		t.Fatal("a path that does not exist must be an error")
	}

	none := repository(t, map[string]string{})
	_, err := bump.Run(ctx, bump.Options{Paths: []string{none}, Source: upstream()})
	if err == nil || !strings.Contains(err.Error(), "no TalosCluster") {
		t.Fatalf("got %v", err)
	}

	// Asking nobody is not a way to bump: there would be nothing to bump to.
	dir := repository(t, map[string]string{"cluster.yaml": cluster("my-cluster", "v1.14.1", "1.37.0")})
	if _, err := bump.Run(ctx, bump.Options{Paths: []string{dir}}); err == nil {
		t.Fatal("a run without a source must be an error")
	}
}

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

package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jarensaa/talos-operator/internal/bump"
	"github.com/jarensaa/talos-operator/internal/catalog"
)

const bumpSchematic = "376567988ad370138ad8b2698212367b8edcb69b5fd68c80be1f2ec7d603b4ba"

// The command's contract with the script that runs it: what was written is the whole of standard
// output, and the exit status says whether the script has something to look at.
func TestBumpCommand(t *testing.T) {
	behind := strings.Replace(validCluster, "talos: v1.14.2", "talos: v1.14.1", 1)

	exists := func(installers ...string) *catalog.Fake {
		fake := &catalog.Fake{
			Talos:      []string{"v1.14.1", "v1.14.2", "v1.15.0"},
			Installers: map[string]bool{},
			Latest:     map[string]string{"1.37": "1.37.1"},
		}

		for _, tag := range installers {
			fake.Installers[bumpSchematic+" "+tag] = true
		}

		return fake
	}

	// A second cluster in the same file, installed from a schematic that has no installer of the
	// next release.
	const otherSchematic = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

	stuck := strings.NewReplacer("my-cluster", "other-cluster", bumpSchematic, otherSchematic).Replace(behind)
	oneStuck := exists("v1.14.1", "v1.14.2")
	oneStuck.Installers[otherSchematic+" v1.14.1"] = true

	tests := []struct {
		name    string
		cluster string
		source  catalog.Source
		opts    bump.Options
		status  int
		// stdout is all of what is printed there, with the directory left out; stderr a part.
		stdout, stderr string
		// written says whether the file is changed afterwards.
		written bool
	}{
		{"a bump", behind, exists("v1.14.1", "v1.14.2"), bump.Options{}, 0,
			"cluster.yaml: TalosCluster talos-operator/my-cluster: Talos v1.14.1 to v1.14.2\n", "", true},
		{"a dry run", behind, exists("v1.14.1", "v1.14.2"), bump.Options{DryRun: true}, 0,
			"cluster.yaml: TalosCluster talos-operator/my-cluster: Talos v1.14.1 to v1.14.2\n", "a dry run, so it was not written", false},
		{"nothing newer", validCluster, exists("v1.14.2"), bump.Options{PatchOnly: true}, 0, "",
			"TalosCluster talos-operator/my-cluster: there is no newer release to take", false},
		// A release this operator does not upgrade to is nobody's failure.
		{"a release that is held", validCluster, exists("v1.14.2"), bump.Options{}, 0, "",
			"TalosCluster talos-operator/my-cluster: Talos v1.15.0 exists and is not taken", false},
		{"a release the check turns down", behind, exists("v1.14.1"), bump.Options{Talos: true}, 1, "",
			"problem: TalosCluster talos-operator/my-cluster: Talos v1.14.1 to v1.14.2 was not written", false},
		// What was written is to be committed: the run does not fail over the cluster next to it.
		{"one cluster bumped and one turned down", behind + "---\n" + stuck, oneStuck, bump.Options{Talos: true}, 0,
			"cluster.yaml: TalosCluster talos-operator/my-cluster: Talos v1.14.1 to v1.14.2\n",
			"problem: TalosCluster talos-operator/other-cluster: Talos v1.14.1 to v1.14.2 was not written", true},
		{"manifests with a problem", behind, exists(), bump.Options{}, 1, "",
			"nothing was bumped: the manifests have a problem as they are", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := manifests(t, tt.cluster)
			tt.opts.Paths, tt.opts.Source = []string{dir}, tt.source

			var stdout, stderr bytes.Buffer

			if got := bumpRun(context.Background(), tt.opts, &stdout, &stderr); got != tt.status {
				t.Fatalf("exit status %d, want %d\nstdout: %s\nstderr: %s", got, tt.status, stdout.String(), stderr.String())
			}

			if got := strings.ReplaceAll(stdout.String(), dir+string(filepath.Separator), ""); got != tt.stdout {
				t.Fatalf("stdout: %q, want %q", got, tt.stdout)
			}

			if !strings.Contains(stderr.String(), tt.stderr) {
				t.Fatalf("stderr: %s", stderr.String())
			}

			after, err := os.ReadFile(filepath.Join(dir, "cluster.yaml"))
			if err != nil {
				t.Fatal(err)
			}

			if written := string(after) != tt.cluster; written != tt.written {
				t.Fatalf("the file is now:\n%s", after)
			}
		})
	}
}

// What the command makes of its arguments, before it asks anyone anything.
func TestBumpCommandArguments(t *testing.T) {
	ctx := context.Background()
	unreachable := manifests(t, unreachableCluster)

	tests := []struct {
		name   string
		args   []string
		status int
		stderr string
	}{
		{"nothing to bump", nil, 2, "Usage: manager bump"},
		{"a flag that is none", []string{"--minor", unreachable}, 2, "flag provided but not defined"},
		{"help", []string{"--help"}, 0, "It needs no cluster and no secret"},
		{"files that cannot be read", []string{filepath.Join(unreachable, "missing")}, 2, "bump:"},
		// Not knowing what exists is no reason to write anything.
		{"upstream cannot be asked", []string{"--kubernetes-releases", nowhere, unreachable}, 1, "could not be checked"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer

			if got := bumpCommand(ctx, tt.args, &stdout, &stderr); got != tt.status {
				t.Fatalf("exit status %d, want %d\nstdout: %s\nstderr: %s", got, tt.status, stdout.String(), stderr.String())
			}

			if stdout.Len() != 0 || !strings.Contains(stderr.String(), tt.stderr) {
				t.Fatalf("stdout: %s\nstderr: %s", stdout.String(), stderr.String())
			}
		})
	}
}

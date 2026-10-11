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
)

const validCluster = `apiVersion: talos.jarensaa.github.io/v1alpha1
kind: TalosCluster
metadata: {name: my-cluster, namespace: talos-operator}
spec:
  clusterName: my-cluster
  controlPlaneEndpoint: https://cluster.example.com:6443
  versions: {talos: v1.14.2, kubernetes: 1.37.1}
  configGeneration:
    versionContract: v1.14
    installer:
      factoryURL: https://factory.example.com
      platform: metal
      schematicID: 376567988ad370138ad8b2698212367b8edcb69b5fd68c80be1f2ec7d603b4ba
`

func manifests(t *testing.T, cluster string) string {
	t.Helper()

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "cluster.yaml"), []byte(cluster), 0o600); err != nil {
		t.Fatal(err)
	}

	return dir
}

// The command's contract with whatever runs it: what it prints, and above all how it exits.
func TestValidateCommand(t *testing.T) {
	ctx := context.Background()
	good := manifests(t, validCluster)
	older := manifests(t, strings.Replace(validCluster, "kubernetes: 1.37.1", "kubernetes: 1.37.0", 1))
	unsupported := manifests(t, strings.Replace(validCluster, "talos: v1.14.2", "talos: v1.19.0", 1))
	// Nothing listens there: asking it fails at once, and leaves the network alone.
	const nowhere = "https://127.0.0.1:1"
	unreachable := manifests(t, strings.Replace(validCluster, "https://factory.example.com", nowhere, 1))

	tests := []struct {
		name   string
		args   []string
		github bool
		status int
		// stdout and stderr are parts of what is printed there.
		stdout, stderr string
	}{
		{"nothing wrong", []string{"--offline", good}, false, 0, "nothing wrong", ""},
		{"what was not checked is said", []string{"--offline", good}, false, 0, "not checked: TalosCluster talos-operator/my-cluster: offline", ""},
		{"a problem", []string{"--offline", unsupported}, false, 1, "problem: TalosCluster talos-operator/my-cluster: versions.talos", ""},
		{"a problem is counted", []string{"--offline", unsupported}, false, 1, "1 problem(s)", ""},
		{"a problem is an annotation on GitHub", []string{"--offline", unsupported}, true, 1, "::error title=talos-operator validate::", ""},
		{"against a base", []string{"--offline", "--base", good, older}, false, 1, "not downgraded", ""},
		{"an upgrade from a base", []string{"--offline", "--base", older, good}, false, 0, "nothing wrong", ""},
		{"files that cannot be read", []string{"--offline", filepath.Join(good, "missing")}, false, 2, "", "validate:"},
		{"nothing to check", nil, false, 2, "", "Usage: manager validate"},
		{"a flag that is none", []string{"--online", good}, false, 2, "", "flag provided but not defined"},
		{"help", []string{"--help"}, false, 0, "", "It needs no"},
		// Asking upstream that is not there is a problem, not a pass.
		{"upstream cannot be asked", []string{"--kubernetes-releases", nowhere, unreachable}, false, 1, "could not be checked", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.github {
				t.Setenv("GITHUB_ACTIONS", "true")
			} else {
				t.Setenv("GITHUB_ACTIONS", "")
			}

			var stdout, stderr bytes.Buffer

			if got := validateCommand(ctx, tt.args, &stdout, &stderr); got != tt.status {
				t.Fatalf("exit status %d, want %d\nstdout: %s\nstderr: %s", got, tt.status, stdout.String(), stderr.String())
			}

			if !strings.Contains(stdout.String(), tt.stdout) || !strings.Contains(stderr.String(), tt.stderr) {
				t.Fatalf("stdout: %s\nstderr: %s", stdout.String(), stderr.String())
			}

			if !tt.github && strings.Contains(stdout.String(), "::error") {
				t.Fatalf("annotations belong on GitHub only: %s", stdout.String())
			}
		})
	}
}

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

package classify

import (
	"slices"
	"testing"
)

// Before Talos 1.14 most settings lived in the legacy v1alpha1 document, so its fields are
// classified by path.
func TestPathClass(t *testing.T) {
	tests := map[string]Class{
		"machine.type":                           Protected,
		"machine.ca.crt":                         Protected,
		"cluster.etcd.ca.key":                    Protected, // the longer rule wins over cluster.etcd
		"cluster.etcd.extraArgs.quota-backend":   ControlPlaneRestart,
		"cluster.network.podSubnets":             Protected,
		"cluster.network.cni.name":               Unknown,
		"cluster.apiServer.extraArgs.foo":        ControlPlaneRestart,
		"machine.install.disk":                   BootTime,
		"machine.kernel.modules":                 BootTime,
		"machine.network.interfaces":             Connectivity,
		"machine.features.kubePrism.port":        Connectivity,
		"machine.features.diskQuotaSupport":      Unknown,
		"machine.kubelet.extraArgs.max-pods":     LiveSafe,
		"machine.kubelet.image":                  Kubernetes, // the longer rule wins over machine.kubelet
		"cluster.apiServer.image":                Kubernetes,
		"cluster.controllerManager.image":        Kubernetes,
		"cluster.scheduler.image":                Kubernetes,
		"cluster.proxy.image":                    LiveSafe,
		"machine.nodeLabels":                     LiveSafe,
		"machine.certSANs":                       LiveSafe,
		"machine.kubeletExtra":                   Unknown, // a prefix must match a whole path segment
		"cluster.allowSchedulingOnControlPlanes": LiveSafe,
		"somethingNew":                           Unknown,
	}

	for path, want := range tests {
		if got := pathClass(path); got != want {
			t.Errorf("%s: got %s, want %s", path, got, want)
		}
	}
}

func TestChangedPathsDescendsThroughMappingsOnly(t *testing.T) {
	a := []byte("machine:\n  type: controlplane\n  kubelet:\n    extraArgs:\n      a: \"1\"\n  certSANs: [x]\n" +
		"cluster:\n  etcd:\n    ca: {crt: one}\n")
	b := []byte("machine:\n  type: controlplane\n  kubelet:\n    extraArgs:\n      a: \"2\"\n      b: \"3\"\n" +
		"  certSANs: [x, y]\n  nodeLabels: {zone: a}\n")

	got, err := changedPaths(a, b)
	if err != nil {
		t.Fatal(err)
	}

	want := []string{
		"cluster.etcd.ca.crt", // a removed subtree is reported field by field
		"machine.certSANs",    // a list is one field
		"machine.kubelet.extraArgs.a",
		"machine.kubelet.extraArgs.b",
		"machine.nodeLabels.zone",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("paths:\n got %v\nwant %v", got, want)
	}
}

func TestChangedPathsReportsEmptyMappings(t *testing.T) {
	got, err := changedPaths([]byte("machine: {}\n"), []byte("machine:\n  features: {}\n"))
	if err != nil {
		t.Fatal(err)
	}

	if !slices.Equal(got, []string{"machine.features"}) {
		t.Fatalf("an added empty mapping is still a change: %v", got)
	}
}

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

package adopt_test

import (
	"errors"
	"testing"

	"github.com/siderolabs/talos/pkg/machinery/config/machine"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/adopt"
	"github.com/jarensaa/talos-operator/internal/talos/talostest"
)

func filledSpec() *talosv1alpha1.TalosClusterSpec {
	return &talosv1alpha1.TalosClusterSpec{
		ClusterName:          clusterName,
		ControlPlaneEndpoint: talostest.Endpoint,
		Versions:             &talosv1alpha1.Versions{Talos: talostest.TalosVersion, Kubernetes: talostest.KubernetesVersion},
		ConfigGeneration: &talosv1alpha1.ConfigGeneration{
			VersionContract: talostest.VersionContract,
			Installer:       &talosv1alpha1.Installer{FactoryURL: "https://factory.talos.dev", Platform: "metal", SchematicID: schematic},
		},
	}
}

// What adoption fills in is not there before, and everything that reads a spec has to say so
// rather than read through a nil.
func TestASpecAdoptionHasNotFilledIn(t *testing.T) {
	empty := &talosv1alpha1.TalosClusterSpec{}
	pool := &talosv1alpha1.TalosMachinePoolSpec{Role: talosv1alpha1.MachineRoleControlPlane}

	if _, named := adopt.Versions(empty); named || adopt.VersionContract(empty) != "" || adopt.InstallerOf(empty) != nil {
		t.Fatal("an empty spec names nothing")
	}

	// One version is not both: a spec that names only Talos can say what to install, and cannot
	// be rendered.
	half := filledSpec()
	half.Versions.Kubernetes = ""

	if versions, named := adopt.Versions(half); named || versions.Talos != talostest.TalosVersion {
		t.Fatalf("a spec with one version: %+v, %v", versions, named)
	}

	if install, err := adopt.InstallTarget(half, pool); err != nil || install.Image != installImage {
		t.Fatalf("what to install is known from the Talos version alone: %+v, %v", install, err)
	}

	// What a person reads in a condition while the spec is not filled in: these stay as they are.
	const (
		incomplete   = "cluster spec is incomplete: "
		generation   = "configGeneration.versionContract and installer"
		nothingNamed = incomplete + "clusterName, controlPlaneEndpoint, versions, " + generation
		noInstall    = incomplete + "versions.talos and configGeneration.installer"
	)

	noInstaller := filledSpec()
	noInstaller.ConfigGeneration.Installer = nil

	noContract := filledSpec()
	noContract.ConfigGeneration.VersionContract = ""

	noName := filledSpec()
	noName.ClusterName = ""

	noTalos := filledSpec()
	noTalos.Versions.Talos = ""

	for name, tt := range map[string]struct {
		spec *talosv1alpha1.TalosClusterSpec
		want string
	}{
		"nothing":         {empty, nothingNamed},
		"one version":     {half, incomplete + "versions"},
		"no installer":    {noInstaller, incomplete + generation},
		"no contract":     {noContract, incomplete + generation},
		"no cluster name": {noName, incomplete + "clusterName"},
	} {
		_, err := adopt.RenderInput(tt.spec, pool, talostest.Bundle(t), nil)
		if !errors.Is(err, adopt.ErrIncompleteSpec) || err.Error() != tt.want {
			t.Errorf("%s: got %v", name, err)
		}
	}

	for name, spec := range map[string]*talosv1alpha1.TalosClusterSpec{
		"nothing": empty, "no installer": noInstaller, "no Talos version": noTalos,
	} {
		_, err := adopt.InstallTarget(spec, pool)
		if !errors.Is(err, adopt.ErrIncompleteSpec) || err.Error() != noInstall {
			t.Errorf("%s: got %v", name, err)
		}
	}
}

// A filled spec renders with what it names, and a pool's own schematic is what its machines are
// installed from.
func TestRenderInputOfAFilledSpec(t *testing.T) {
	spec := filledSpec()
	pool := &talosv1alpha1.TalosMachinePoolSpec{Role: talosv1alpha1.MachineRoleWorker}

	in, err := adopt.RenderInput(spec, pool, talostest.Bundle(t), [][]byte{[]byte("a"), []byte("b")})
	if err != nil {
		t.Fatal(err)
	}

	if in.ClusterName != clusterName || in.KubernetesVersion != talostest.KubernetesVersion || in.Role != machine.TypeWorker ||
		in.InstallImage != installImage || in.Contract == nil || len(in.Patches) != 2 {
		t.Fatalf("got %+v", in)
	}

	const own = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

	pool.Installer = &talosv1alpha1.PoolInstaller{SchematicID: own}

	install, err := adopt.InstallTarget(spec, pool)
	if err != nil || install.SchematicID != own || install.Version != talostest.TalosVersion ||
		install.Image != "factory.talos.dev/metal-installer/"+own+":"+talostest.TalosVersion {
		t.Fatalf("got %+v, %v", install, err)
	}
}

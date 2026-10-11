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

// Package adopt turns a live cluster into a proposal of TalosCluster and TalosMachine specs and
// renders configs from such specs (docs/scope.md §7.1). It is pure: callers read the cluster and
// write Kubernetes objects.
package adopt

import (
	"errors"
	"fmt"
	"net/netip"
	"net/url"
	"strings"

	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	"github.com/siderolabs/talos/pkg/machinery/config/machine"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/render"
)

// ErrIncompleteSpec means a TalosCluster spec lacks fields rendering needs.
var ErrIncompleteSpec = errors.New("cluster spec is incomplete")

// InstallerImage builds the Image Factory installer reference for a Talos version, e.g.
// factory.talos.dev/metal-installer/<schematic>:v1.14.2.
func InstallerImage(in talosv1alpha1.Installer, talosVersion string) (string, error) {
	u, err := url.Parse(in.FactoryURL)
	if err != nil || u.Host == "" {
		return "", fmt.Errorf("installer factory URL %q is invalid", in.FactoryURL)
	}

	if in.Platform == "" || in.SchematicID == "" {
		return "", errors.New("installer platform and schematicID are required")
	}

	return fmt.Sprintf("%s/%s-installer/%s:%s", u.Host, in.Platform, in.SchematicID, talosVersion), nil
}

// Install is what a machine is to be installed from: the Talos version and the schematic its
// spec asks for, and the installer image that carries both.
type Install struct {
	Version     string
	SchematicID string
	Image       string
}

// InstallTarget returns what a pool's machines are to be installed from: the cluster's version
// and installer, with the pool's own schematic if it sets one.
func InstallTarget(spec *talosv1alpha1.TalosClusterSpec, pool *talosv1alpha1.TalosMachinePoolSpec) (Install, error) {
	if spec.Versions == nil || spec.Versions.Talos == "" || spec.ConfigGeneration == nil || spec.ConfigGeneration.Installer == nil {
		return Install{}, fmt.Errorf("%w: versions.talos and configGeneration.installer", ErrIncompleteSpec)
	}

	installer := *spec.ConfigGeneration.Installer
	if pool.Installer != nil && pool.Installer.SchematicID != "" {
		installer.SchematicID = pool.Installer.SchematicID
	}

	image, err := InstallerImage(installer, spec.Versions.Talos)
	if err != nil {
		return Install{}, err
	}

	return Install{Version: spec.Versions.Talos, SchematicID: installer.SchematicID, Image: image}, nil
}

// ParseInstallerImage splits an Image Factory installer reference into the spec's fields and the
// Talos version tag. Other installer images cannot be represented and are refused.
func ParseInstallerImage(ref string) (talosv1alpha1.Installer, string, error) {
	host, rest, ok := strings.Cut(ref, "/")
	if !ok {
		return talosv1alpha1.Installer{}, "", fmt.Errorf("installer %q is not an Image Factory image", ref)
	}

	repo, tag, ok := strings.Cut(rest, ":")
	if !ok {
		return talosv1alpha1.Installer{}, "", fmt.Errorf("installer %q has no tag", ref)
	}

	platformRepo, schematic, ok := strings.Cut(repo, "/")
	if !ok || !strings.HasSuffix(platformRepo, "-installer") || len(schematic) != 64 {
		return talosv1alpha1.Installer{}, "", fmt.Errorf(
			"installer %q is not an Image Factory image (<host>/<platform>-installer/<schematic>:<version>)", ref)
	}

	return talosv1alpha1.Installer{
		FactoryURL:  "https://" + host,
		Platform:    strings.TrimSuffix(platformRepo, "-installer"),
		SchematicID: schematic,
	}, tag, nil
}

// RenderInput builds the render input for one machine from its cluster and pool specs. Documents
// are the resolved config sources in layer order (cluster, pool, machine; see ConfigSources).
func RenderInput(spec *talosv1alpha1.TalosClusterSpec, pool *talosv1alpha1.TalosMachinePoolSpec, m *talosv1alpha1.TalosMachineSpec,
	bundle *secrets.Bundle, documents [][]byte,
) (render.Input, error) {
	var missing []string

	if spec.ClusterName == "" {
		missing = append(missing, "clusterName")
	}

	if spec.ControlPlaneEndpoint == "" {
		missing = append(missing, "controlPlaneEndpoint")
	}

	if spec.Versions == nil || spec.Versions.Talos == "" || spec.Versions.Kubernetes == "" {
		missing = append(missing, "versions")
	}

	if spec.ConfigGeneration == nil || spec.ConfigGeneration.VersionContract == "" || spec.ConfigGeneration.Installer == nil {
		missing = append(missing, "configGeneration.versionContract and installer")
	}

	if len(missing) > 0 {
		return render.Input{}, fmt.Errorf("%w: %s", ErrIncompleteSpec, strings.Join(missing, ", "))
	}

	contract, err := config.ParseContractFromVersion(spec.ConfigGeneration.VersionContract)
	if err != nil {
		return render.Input{}, fmt.Errorf("versionContract: %w", err)
	}

	install, err := InstallTarget(spec, pool)
	if err != nil {
		return render.Input{}, err
	}

	image := install.Image

	in := render.Input{
		ClusterName:       spec.ClusterName,
		Endpoint:          spec.ControlPlaneEndpoint,
		KubernetesVersion: spec.Versions.Kubernetes,
		Contract:          contract,
		Bundle:            bundle,
		Role:              machineType(pool.Role),
		InstallImage:      image,
		Patches:           documents,
	}

	if opts := spec.ConfigGeneration.Options; opts != nil && opts.DNSDomain != "" {
		in.DNSDomain = opts.DNSDomain
	}

	// What the spec switches on becomes config of the machine before any layer of the user's.
	if spec.Observability.ControlPlaneMetrics == talosv1alpha1.CapabilityEnabled && pool.Role == talosv1alpha1.MachineRoleControlPlane {
		addr, err := netip.ParseAddr(m.Address)
		if err != nil {
			return render.Input{}, fmt.Errorf("the machine's address %q is not an IP address", m.Address)
		}

		in.MetricsAddress = addr
	}

	return in, nil
}

func machineType(role talosv1alpha1.MachineRole) machine.Type {
	if role == talosv1alpha1.MachineRoleControlPlane {
		return machine.TypeControlPlane
	}

	return machine.TypeWorker
}

func machineRole(t machine.Type) talosv1alpha1.MachineRole {
	if t == machine.TypeControlPlane {
		return talosv1alpha1.MachineRoleControlPlane
	}

	return talosv1alpha1.MachineRoleWorker
}

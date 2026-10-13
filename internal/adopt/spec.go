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

// secureBootInstaller is what the repository of an installer ends in when Image Factory signs
// the boot image it carries.
const secureBootInstaller = "-secureboot"

// InstallerImage builds the Image Factory installer reference for a Talos version, e.g.
// factory.talos.dev/metal-installer/<schematic>:v1.14.2, or metal-installer-secureboot for the
// installer whose boot image is signed.
func InstallerImage(in talosv1alpha1.Installer, talosVersion string) (string, error) {
	u, err := url.Parse(in.FactoryURL)
	if err != nil || u.Host == "" {
		return "", fmt.Errorf("installer factory URL %q is invalid", in.FactoryURL)
	}

	if in.Platform == "" || in.SchematicID == "" {
		return "", errors.New("installer platform and schematicID are required")
	}

	repository := in.Platform + "-installer"
	if in.SecureBoot {
		repository += secureBootInstaller
	}

	return fmt.Sprintf("%s/%s/%s:%s", u.Host, repository, in.SchematicID, talosVersion), nil
}

// Install is what a machine is to be installed from: the Talos version and the schematic its
// spec asks for, and the installer image that carries both.
type Install struct {
	Version     string
	SchematicID string
	Image       string
	// FactoryURL is the Image Factory the image comes from, and SecureBoot whether it is the
	// installer whose boot image that factory signs.
	FactoryURL string
	SecureBoot bool
}

// What a TalosCluster spec has only once adoption filled it in is read through these, so that
// "not there yet" is asked the same way by everything that reads a spec.

// Versions returns the versions the spec names, and whether it names both of them.
func Versions(spec *talosv1alpha1.TalosClusterSpec) (talosv1alpha1.Versions, bool) {
	if spec.Versions == nil {
		return talosv1alpha1.Versions{}, false
	}

	return *spec.Versions, spec.Versions.Talos != "" && spec.Versions.Kubernetes != ""
}

// VersionContract returns the version contract the spec pins; empty while it pins none.
func VersionContract(spec *talosv1alpha1.TalosClusterSpec) string {
	if spec.ConfigGeneration == nil {
		return ""
	}

	return spec.ConfigGeneration.VersionContract
}

// InstallerOf returns the installer the spec names; nil while it names none.
func InstallerOf(spec *talosv1alpha1.TalosClusterSpec) *talosv1alpha1.Installer {
	if spec.ConfigGeneration == nil {
		return nil
	}

	return spec.ConfigGeneration.Installer
}

// InstallTarget returns what a pool's machines are to be installed from: the cluster's version
// and installer, with the pool's own schematic and Secure Boot setting if it sets them.
func InstallTarget(spec *talosv1alpha1.TalosClusterSpec, pool *talosv1alpha1.TalosMachinePoolSpec) (Install, error) {
	versions, _ := Versions(spec)
	installer := InstallerOf(spec)

	if versions.Talos == "" || installer == nil {
		return Install{}, fmt.Errorf("%w: versions.talos and configGeneration.installer", ErrIncompleteSpec)
	}

	return installTarget(versions.Talos, *installer, pool)
}

// installTarget is InstallTarget of a spec that is known to name a Talos version and an installer.
func installTarget(talosVersion string, installer talosv1alpha1.Installer, pool *talosv1alpha1.TalosMachinePoolSpec) (Install, error) {
	if pool.Installer != nil {
		if pool.Installer.SchematicID != "" {
			installer.SchematicID = pool.Installer.SchematicID
		}

		if pool.Installer.SecureBoot != nil {
			installer.SecureBoot = *pool.Installer.SecureBoot
		}
	}

	image, err := InstallerImage(installer, talosVersion)
	if err != nil {
		return Install{}, err
	}

	return Install{
		Version: talosVersion, SchematicID: installer.SchematicID, Image: image,
		FactoryURL: installer.FactoryURL, SecureBoot: installer.SecureBoot,
	}, nil
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

	platformRepo, secureBoot := strings.CutSuffix(platformRepo, secureBootInstaller)

	platform, isInstaller := strings.CutSuffix(platformRepo, "-installer")
	if !ok || !isInstaller || platform == "" || len(schematic) != 64 {
		return talosv1alpha1.Installer{}, "", fmt.Errorf(
			"installer %q is not an Image Factory image (<host>/<platform>-installer[-secureboot]/<schematic>:<version>)", ref)
	}

	return talosv1alpha1.Installer{
		FactoryURL:  "https://" + host,
		Platform:    platform,
		SchematicID: schematic,
		SecureBoot:  secureBoot,
	}, tag, nil
}

// RenderInput builds the render input for one machine from its cluster and pool specs. Documents
// are the resolved config sources in layer order (cluster, pool, machine; see ConfigSources).
func RenderInput(spec *talosv1alpha1.TalosClusterSpec, pool *talosv1alpha1.TalosMachinePoolSpec, bundle *secrets.Bundle,
	documents [][]byte,
) (render.Input, error) {
	var missing []string

	if spec.ClusterName == "" {
		missing = append(missing, "clusterName")
	}

	if spec.ControlPlaneEndpoint == "" {
		missing = append(missing, "controlPlaneEndpoint")
	}

	versions, named := Versions(spec)
	if !named {
		missing = append(missing, "versions")
	}

	installer := InstallerOf(spec)
	if VersionContract(spec) == "" || installer == nil {
		missing = append(missing, "configGeneration.versionContract and installer")
	}

	if len(missing) > 0 {
		return render.Input{}, fmt.Errorf("%w: %s", ErrIncompleteSpec, strings.Join(missing, ", "))
	}

	contract, err := config.ParseContractFromVersion(VersionContract(spec))
	if err != nil {
		return render.Input{}, fmt.Errorf("versionContract: %w", err)
	}

	install, err := installTarget(versions.Talos, *installer, pool)
	if err != nil {
		return render.Input{}, err
	}

	in := render.Input{
		ClusterName:       spec.ClusterName,
		Endpoint:          spec.ControlPlaneEndpoint,
		KubernetesVersion: versions.Kubernetes,
		Contract:          contract,
		Bundle:            bundle,
		Role:              machineType(pool.Role),
		InstallImage:      install.Image,
		Patches:           documents,
	}

	if opts := spec.ConfigGeneration.Options; opts != nil && opts.DNSDomain != "" {
		in.DNSDomain = opts.DNSDomain
	}

	// What the spec switches on becomes config of the machine before any layer of the user's.
	in.ControlPlaneMetrics = spec.Observability.ControlPlaneMetrics == talosv1alpha1.CapabilityEnabled

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

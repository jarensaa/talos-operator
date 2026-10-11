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

// Package render turns a cluster's declared inputs and secrets bundle into one node's machine
// config (docs/scope.md §6.1). It reproduces `talosctl gen config` followed by
// `talosctl machineconfig patch`, so a cluster created that way can be adopted with no changes.
package render

import (
	"errors"
	"fmt"

	jsonpatch "github.com/evanphx/json-patch"
	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/config/configpatcher"
	"github.com/siderolabs/talos/pkg/machinery/config/generate"
	"github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	"github.com/siderolabs/talos/pkg/machinery/config/machine"
)

// Defaults of `talosctl gen config` (cmd/talosctl/cmd/mgmt/gen/config.go:438-451 in v1.14).
// Rendering must match them, or a cluster generated with talosctl would not adopt cleanly.
const (
	DefaultInstallDisk = "/dev/sda"
	DefaultDNSDomain   = "cluster.local"
)

var (
	// ErrJSONPatch is returned for RFC 6902 patches: they cannot address multi-document configs.
	ErrJSONPatch = errors.New("JSON6902 patches are not supported; use strategic merge patches")
	// ErrNoContract is returned when the version contract is missing. It is never implied from
	// the operator's own machinery version, which would silently change generated defaults.
	ErrNoContract = errors.New("a version contract is required")
)

// Input is everything one node's config is rendered from.
type Input struct {
	ClusterName string
	// Endpoint is the Kubernetes control plane endpoint URL.
	Endpoint string
	// KubernetesVersion is the base version, e.g. 1.37.0. During a Kubernetes upgrade it stays at
	// the version the rollout started from; per-node image overlays move components forward.
	KubernetesVersion string
	Contract          *config.VersionContract
	Bundle            *secrets.Bundle
	Role              machine.Type

	// InstallImage is the installer image reference; empty keeps the generator's default.
	InstallImage string
	// InstallDisk defaults to DefaultInstallDisk.
	InstallDisk string
	// DNSDomain defaults to DefaultDNSDomain.
	DNSDomain string
	// ClusterDiscovery mirrors `--with-cluster-discovery` (talosctl default: enabled).
	ClusterDiscovery *bool

	// ControlPlaneMetrics makes a control plane serve the metrics of the controller manager and
	// the scheduler on every interface (metricsPatch). It is applied before Patches, so a user's
	// own setting of the same arguments stands. It changes nothing on a worker.
	ControlPlaneMetrics bool

	// Patches are strategic merge patches (multi-document YAML), applied in the order given:
	// cluster-wide, role, machine, then operator overlays.
	Patches [][]byte
}

// Render produces the node's config in normalized form.
func Render(in Input) (*Config, error) {
	cfg, err := Generate(in)
	if err != nil {
		return nil, err
	}

	return Normalize(cfg)
}

// Generate produces the node's patched and validated config, before normalization.
func Generate(in Input) (config.Provider, error) {
	if in.Contract == nil {
		return nil, ErrNoContract
	}

	if in.Bundle == nil {
		return nil, errors.New("a secrets bundle is required")
	}

	installDisk := in.InstallDisk
	if installDisk == "" {
		installDisk = DefaultInstallDisk
	}

	dnsDomain := in.DNSDomain
	if dnsDomain == "" {
		dnsDomain = DefaultDNSDomain
	}

	discovery := true
	if in.ClusterDiscovery != nil {
		discovery = *in.ClusterDiscovery
	}

	opts := []generate.Option{
		generate.WithVersionContract(in.Contract),
		generate.WithSecretsBundle(in.Bundle),
		generate.WithInstallDisk(installDisk),
		generate.WithDNSDomain(dnsDomain),
		generate.WithClusterDiscovery(discovery),
	}

	if in.InstallImage != "" {
		opts = append(opts, generate.WithInstallImage(in.InstallImage))
	}

	genInput, err := generate.NewInput(in.ClusterName, in.Endpoint, in.KubernetesVersion, opts...)
	if err != nil {
		return nil, fmt.Errorf("generate input: %w", err)
	}

	base, err := genInput.Config(in.Role)
	if err != nil {
		return nil, fmt.Errorf("generate %s config: %w", in.Role, err)
	}

	patches := make([]configpatcher.Patch, 0, len(in.Patches)+1)

	if in.ControlPlaneMetrics && in.Role == machine.TypeControlPlane {
		p, err := configpatcher.LoadPatch(metricsPatch(in.Contract))
		if err != nil {
			return nil, fmt.Errorf("the control-plane metrics config: %w", err)
		}

		patches = append(patches, p)
	}

	for i, raw := range in.Patches {
		p, err := configpatcher.LoadPatch(raw)
		if err != nil {
			return nil, fmt.Errorf("patch %d: %w", i, err)
		}

		if _, isJSON := p.(jsonpatch.Patch); isJSON {
			return nil, fmt.Errorf("patch %d: %w", i, ErrJSONPatch)
		}

		patches = append(patches, p)
	}

	out, err := configpatcher.Apply(configpatcher.WithConfig(base), patches)
	if err != nil {
		return nil, fmt.Errorf("apply patches: %w", err)
	}

	cfg, err := out.Config()
	if err != nil {
		return nil, fmt.Errorf("patched config: %w", err)
	}

	if _, err := cfg.ValidateAsClient(metalMode{}); err != nil {
		return nil, fmt.Errorf("rendered config is invalid: %w", err)
	}

	return cfg, nil
}

// metalMode validates configs for installed bare-metal nodes, the operator's target.
type metalMode struct{}

func (metalMode) String() string        { return "metal" }
func (metalMode) RequiresInstall() bool { return true }
func (metalMode) InContainer() bool     { return false }

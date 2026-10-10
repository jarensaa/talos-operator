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

// Package derive reconstructs a cluster's declared inputs from its live configs: the generation
// inputs (Infer) and the patch layers that turn the regenerated base into each node's live
// config (Patches). Together with the recovered secrets bundle they are the adoption proposal
// (docs/scope.md §7.1 stage 1).
package derive

import (
	"errors"
	"fmt"
	"strings"

	"github.com/siderolabs/talos/pkg/machinery/config"
	"k8s.io/apimachinery/pkg/util/version"

	"github.com/jarensaa/talos-operator/internal/render"
)

// Inputs are the generation inputs of a cluster, as they would appear in the TalosCluster spec.
type Inputs struct {
	ClusterName       string
	Endpoint          string
	KubernetesVersion string
	// TalosVersion is the minor the contract is pinned to, e.g. v1.14.
	TalosVersion string
	Contract     *config.VersionContract
	InstallImage string
	DNSDomain    string
}

// Infer derives the generation inputs from a live control-plane config and the Talos version the
// node runs. Only inputs that generation spreads across several documents are inferred; every
// other difference from the generated base is left to patch derivation rather than guessed.
func Infer(live config.Provider, talosTag string) (Inputs, error) {
	tv, err := version.ParseGeneric(talosTag)
	if err != nil {
		return Inputs{}, fmt.Errorf("talos version %q: %w", talosTag, err)
	}

	minor := fmt.Sprintf("v%d.%d", tv.Major(), tv.Minor())

	contract, err := config.ParseContractFromVersion(minor)
	if err != nil {
		return Inputs{}, fmt.Errorf("version contract for %s: %w", minor, err)
	}

	cluster := live.K8sClusterConfig()
	if cluster == nil || cluster.ClusterEndpoint() == nil {
		return Inputs{}, errors.New("live config has no cluster name and endpoint")
	}

	kubelet := live.K8sKubeletConfig()
	if kubelet == nil {
		return Inputs{}, errors.New("live config has no kubelet config")
	}

	k8s, err := imageVersion(kubelet.Image())
	if err != nil {
		return Inputs{}, fmt.Errorf("kubernetes version from kubelet image: %w", err)
	}

	in := Inputs{
		ClusterName:       cluster.ClusterName(),
		Endpoint:          cluster.ClusterEndpoint().String(),
		KubernetesVersion: k8s,
		TalosVersion:      minor,
		Contract:          contract,
		DNSDomain:         render.DefaultDNSDomain,
	}

	if install := live.UnattendedInstallConfig(); install != nil {
		in.InstallImage = install.InstallerImage()
	}

	if network := live.K8sNetworkConfig(); network != nil && network.DNSDomain() != "" {
		in.DNSDomain = network.DNSDomain()
	}

	return in, nil
}

// imageVersion returns the version of an image reference's tag without the leading "v".
func imageVersion(ref string) (string, error) {
	i := strings.LastIndex(ref, ":")
	if i < 0 || strings.Contains(ref[i:], "/") {
		return "", fmt.Errorf("image %q has no tag", ref)
	}

	v, err := version.ParseSemantic(ref[i+1:])
	if err != nil {
		return "", fmt.Errorf("image %q: %w", ref, err)
	}

	return v.String(), nil
}

// RenderInput turns inferred inputs into a render input for one role, without patches.
func (in Inputs) RenderInput(base render.Input) render.Input {
	base.ClusterName = in.ClusterName
	base.Endpoint = in.Endpoint
	base.KubernetesVersion = in.KubernetesVersion
	base.Contract = in.Contract
	base.InstallImage = in.InstallImage
	base.DNSDomain = in.DNSDomain

	return base
}

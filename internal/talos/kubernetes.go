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

package talos

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"slices"
	"sort"
	"strings"

	"github.com/siderolabs/go-kubernetes/kubernetes/upgrade"
	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	"github.com/siderolabs/talos/pkg/machinery/compatibility"
	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/config/configpatcher"
	"github.com/siderolabs/talos/pkg/machinery/config/encoder"
	"github.com/siderolabs/talos/pkg/machinery/config/generate/stdpatches"
	"github.com/siderolabs/talos/pkg/machinery/resources/k8s"
	"k8s.io/client-go/rest"
)

// What Talos and its tooling know about upgrading Kubernetes (docs/scope.md §7.4). The algorithm
// is the operator's own, because it owns the config; these are the parts it takes from
// `talosctl upgrade-k8s` (pkg/cluster/kubernetes in Talos v1.14): where a config names a
// component's image, the patch that changes it, and the checks run before anything is touched.

// The Kubernetes components Talos runs from a node's machine config, by their Talos IDs.
const (
	ComponentAPIServer         = k8s.APIServerID
	ComponentControllerManager = k8s.ControllerManagerID
	ComponentScheduler         = k8s.SchedulerID
	ComponentKubelet           = k8s.KubeletID
	// ComponentProxy is kube-proxy, which Talos does not run: a control plane's config names its
	// image for the manifest Talos applies when a cluster is bootstrapped. The image belongs to
	// the Kubernetes version like the others, so an upgrade brings it in line; nothing is pulled
	// or restarted for it.
	ComponentProxy = "kube-proxy"
)

// KubernetesComponents lists the components in the order an upgrade takes them, which is
// talosctl's (pkg/cluster/kubernetes/talos_managed.go:138-152 in Talos v1.14).
var KubernetesComponents = []string{
	ComponentAPIServer, ComponentControllerManager, ComponentScheduler, ComponentProxy, ComponentKubelet,
}

// KubernetesImages returns the image a machine config names for each Kubernetes component: the
// kubelet on every machine, the control-plane components and kube-proxy on a control plane. A
// component the config names no image for is left out.
func KubernetesImages(raw []byte) (map[string]string, error) {
	cfg, err := ParseMachineConfig(raw)
	if err != nil {
		return nil, err
	}

	images := map[string]string{}

	if kubelet := cfg.K8sKubeletConfig(); kubelet != nil {
		images[ComponentKubelet] = kubelet.Image()
	}

	if cfg.Machine() == nil || !cfg.Machine().Type().IsControlPlane() {
		return images, nil
	}

	if c := cfg.K8sAPIServerConfig(); c != nil {
		images[ComponentAPIServer] = c.Image()
	}

	if c := cfg.K8sControllerManagerConfig(); c != nil {
		images[ComponentControllerManager] = c.Image()
	}

	if c := cfg.K8sSchedulerConfig(); c != nil {
		images[ComponentScheduler] = c.Image()
	}

	if c := cfg.K8sProxyConfig(); c != nil {
		images[ComponentProxy] = c.Image()
	}

	for component, image := range images {
		if image == "" {
			delete(images, component)
		}
	}

	return images, nil
}

// WithKubernetesImage returns the machine config raw with the image of one component replaced,
// and everything else as it is. The patch is the one talosctl applies
// (config/generate/stdpatches), in the form the config itself uses: its own document where the
// config has one, the v1alpha1 field otherwise.
func WithKubernetesImage(raw []byte, component, image string) ([]byte, error) {
	cfg, err := ParseMachineConfig(raw)
	if err != nil {
		return nil, err
	}

	var patch []byte

	switch component {
	case ComponentAPIServer:
		patch, err = stdpatches.WithKubeAPIServerImage(stdpatches.GuessVersionContractKubeAPIServer(cfg), image)
	case ComponentControllerManager:
		patch, err = stdpatches.WithKubeControllerManagerImage(stdpatches.GuessVersionContractKubeControllerManager(cfg), image)
	case ComponentScheduler:
		patch, err = stdpatches.WithKubeSchedulerImage(stdpatches.GuessVersionContractKubeScheduler(cfg), image)
	case ComponentProxy:
		patch, err = stdpatches.WithKubeProxyImage(stdpatches.GuessVersionContractKubeProxy(cfg), image)
	case ComponentKubelet:
		patch, err = stdpatches.WithKubeletImage(stdpatches.GuessVersionContractKubelet(cfg), image)
	default:
		return nil, fmt.Errorf("%q is not a Kubernetes component a machine config names an image for", component)
	}

	loaded, err := stdpatches.PreparePatch(patch, err)
	if err != nil {
		return nil, fmt.Errorf("patch for the %s image: %w", component, err)
	}

	// Errors from here on can quote the config, which carries secrets.
	patched, err := configpatcher.Apply(configpatcher.WithConfig(cfg), []configpatcher.Patch{loaded})
	if err != nil {
		return nil, fmt.Errorf("the %s image cannot be patched into the config", component)
	}

	var out config.Provider
	if out, err = patched.Config(); err != nil {
		return nil, fmt.Errorf("the config with the %s image patched in is not valid", component)
	}

	encoded, err := out.EncodeBytes(encoder.WithComments(encoder.CommentsDisabled))
	if err != nil {
		return nil, fmt.Errorf("the config with the %s image patched in cannot be encoded", component)
	}

	return encoded, nil
}

// KubernetesUpgradeProblem says why a cluster whose components run Kubernetes from cannot be
// brought to version to while its nodes run the given Talos versions (node name to tag), or
// returns "". It refuses what cannot work before anything is touched (docs/scope.md §7.4): a way
// Kubernetes has no upgrade for, and a node whose Talos does not support the version.
func KubernetesUpgradeProblem(from, to string, talosVersions map[string]string) string {
	path, err := upgrade.NewPath(from, to)
	if err != nil {
		return fmt.Sprintf("no upgrade path from Kubernetes %q to %q: %v", from, to, err)
	}

	if !path.IsSupported() {
		return fmt.Sprintf("there is no upgrade from Kubernetes %s to %s: minors are taken one at a time, and never backwards", from, to)
	}

	return KubernetesSupportProblem(to, talosVersions)
}

// KubernetesSupportProblem says which of the nodes, by the Talos versions they run (node name to
// tag), does not support Kubernetes version, or returns "".
func KubernetesSupportProblem(version string, talosVersions map[string]string) string {
	to := strings.TrimPrefix(version, "v")

	target, err := compatibility.ParseKubernetesVersion(to)
	if err != nil {
		return fmt.Sprintf("unparsable Kubernetes version %q", version)
	}

	nodes := make([]string, 0, len(talosVersions))
	for node := range talosVersions {
		nodes = append(nodes, node)
	}

	sort.Strings(nodes)

	var unsupported []string

	for _, node := range nodes {
		tag := talosVersions[node]

		host, err := compatibility.ParseTalosVersion(&machineapi.VersionInfo{Tag: tag})
		if err != nil {
			return fmt.Sprintf("%s: unparsable Talos version %q", node, tag)
		}

		if err := target.SupportedWith(host); err != nil {
			unsupported = append(unsupported, fmt.Sprintf("%s (%s)", node, tag))
		}
	}

	if len(unsupported) > 0 {
		return fmt.Sprintf("Talos on %s does not support Kubernetes %s: upgrade Talos first", strings.Join(unsupported, ", "), to)
	}

	return ""
}

// removedItems describes what upgrade.Checks found, one line per item.
func removedItems(err error) ([]string, error) {
	if err == nil {
		return nil, nil
	}

	var removed upgrade.ComponentRemovedItemsError
	if !errors.As(err, &removed) {
		return nil, err
	}

	var out []string

	for _, item := range removed.CLIFlags {
		out = append(out, fmt.Sprintf("%s on %s is started with --%s, which the new version removed", item.Component, item.Node, item.Value))
	}

	for _, item := range removed.FeatureGates {
		out = append(out, fmt.Sprintf("%s on %s sets the feature gate %s, which the new version removed", item.Component, item.Node, item.Value))
	}

	for _, item := range removed.AdmissionFlags {
		out = append(out, fmt.Sprintf("%s on %s enables the admission plugin %s, which the new version removed",
			item.Component, item.Node, item.Value))
	}

	for resource, count := range removed.APIResources {
		out = append(out, fmt.Sprintf("%d object(s) of %s exist, an API the new version no longer serves", count, resource))
	}

	slices.Sort(out)

	return out, nil
}

// RemovedAPIObjects asks the Kubernetes API behind cfg for objects of the APIs that the upgrade
// from Kubernetes from to to stops serving, and describes what it finds. The list of such APIs
// is go-kubernetes's (upgrade.Checks); an API the cluster does not serve has no objects.
func RemovedAPIObjects(ctx context.Context, cfg *rest.Config, from, to string) ([]string, error) {
	path, err := upgrade.NewPath(from, to)
	if err != nil {
		return nil, err
	}

	// With no nodes, the checks ask the Kubernetes API only.
	checks, err := upgrade.NewChecks(path, nil, cfg, nil, nil, func(string, ...any) {})
	if err != nil {
		return nil, err
	}

	return removedItems(checks.Run(ctx))
}

// KubernetesUpgradeChecks implements Reader.
func (r *machineryReader) KubernetesUpgradeChecks(ctx context.Context, from, to string, nodes ClusterInfo) ([]string, error) {
	path, err := upgrade.NewPath(from, to)
	if err != nil {
		return nil, err
	}

	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc

		ctx, cancel = context.WithTimeout(ctx, r.timeout)
		defer cancel()
	}

	addrs := func(in []netip.Addr) []string {
		out := make([]string, 0, len(in))
		for _, a := range in {
			out = append(out, a.String())
		}

		return out
	}

	// Without a Kubernetes client, the checks ask the nodes only (client.WithNode for each).
	checks, err := upgrade.NewChecks(path, r.client.COSI, nil, addrs(nodes.ControlPlanes), addrs(nodes.Workers), func(string, ...any) {})
	if err != nil {
		return nil, err
	}

	found, err := removedItems(checks.Run(ctx))
	if err != nil {
		return nil, fmt.Errorf("check the nodes for what Kubernetes %s removed: %w", to, err)
	}

	return found, nil
}

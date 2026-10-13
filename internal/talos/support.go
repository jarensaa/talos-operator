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
	"fmt"
	"strings"

	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	"github.com/siderolabs/talos/pkg/machinery/compatibility"
	"k8s.io/apimachinery/pkg/util/version"
)

// Supported Talos range (docs/scope.md §4.3). The upper bound is the minor of the machinery
// module this operator is built with: newer nodes may use config documents it cannot parse.
// TestMaxSupportedMinorMatchesMachinery keeps it in step with go.mod.
const (
	MinSupportedTalos      = "1.13.0"
	MaxSupportedTalosMinor = 14
)

// SupportsTalos reports whether a node running tag (e.g. v1.14.2) is in the supported range.
func SupportsTalos(tag string) (bool, string) {
	v, err := version.ParseGeneric(tag)
	if err != nil {
		return false, fmt.Sprintf("unparsable version %q", tag)
	}

	if v.LessThan(version.MustParseGeneric(MinSupportedTalos)) {
		return false, fmt.Sprintf("%s is older than the minimum v%s", tag, MinSupportedTalos)
	}

	if v.Major() != 1 || v.Minor() > MaxSupportedTalosMinor {
		return false, fmt.Sprintf("%s is newer than this operator's Talos machinery (v1.%d)", tag, MaxSupportedTalosMinor)
	}

	return true, ""
}

// UpgradeProblem says why a node running Talos from cannot be upgraded to target while the
// cluster runs Kubernetes kubernetesVersion, or returns "". It refuses up front what cannot work
// (docs/scope.md §7.3): a target outside this operator's range, a downgrade, a jump Talos itself
// does not allow, and a Kubernetes version the target does not support.
//
// Downgrades are refused outright. Talos allows some, but a downgraded node may not understand
// the config it has, and nothing here can prove that it does.
func UpgradeProblem(from, target, kubernetesVersion string) string {
	if ok, why := SupportsTalos(target); !ok {
		return why
	}

	current, err := version.ParseGeneric(from)
	if err != nil {
		return fmt.Sprintf("unparsable version %q", from)
	}

	if version.MustParseGeneric(target).LessThan(current) {
		return fmt.Sprintf("%s is older than %s, and downgrades are not supported", target, from)
	}

	to, err := talosRelease(target)
	if err != nil {
		return fmt.Sprintf("unparsable version %q", target)
	}

	host, err := talosRelease(from)
	if err != nil {
		return fmt.Sprintf("unparsable version %q", from)
	}

	if err := to.UpgradeableFrom(host); err != nil {
		return fmt.Sprintf("Talos does not upgrade from %s to %s: %v", from, target, err)
	}

	if kubernetesVersion == "" {
		return ""
	}

	switch why, problem := runsKubernetes(target, kubernetesVersion); {
	case problem != "":
		return problem
	case why != "":
		return fmt.Sprintf("Talos %s does not support Kubernetes %s, which the cluster runs: %s", target, kubernetesVersion, why)
	}

	return ""
}

// RunsKubernetes says why a Talos release does not run a Kubernetes version, or returns "".
func RunsKubernetes(talosVersion, kubernetesVersion string) string {
	switch why, problem := runsKubernetes(talosVersion, kubernetesVersion); {
	case problem != "":
		return problem
	case why != "":
		return fmt.Sprintf("Talos %s does not run Kubernetes %s: %s", talosVersion, strings.TrimPrefix(kubernetesVersion, "v"), why)
	}

	return ""
}

// talosRelease reads the tag of a Talos release for machinery's compatibility rules.
func talosRelease(tag string) (*compatibility.TalosVersion, error) {
	return compatibility.ParseTalosVersion(&machineapi.VersionInfo{Tag: tag})
}

// kubernetesRelease reads a Kubernetes version, with or without its v, for machinery's
// compatibility rules. The problem is that of a version that is none.
func kubernetesRelease(kubernetesVersion string) (*compatibility.KubernetesVersion, string) {
	parsed, err := compatibility.ParseKubernetesVersion(strings.TrimPrefix(kubernetesVersion, "v"))
	if err != nil {
		return nil, fmt.Sprintf("unparsable Kubernetes version %q", kubernetesVersion)
	}

	return parsed, ""
}

// runsKubernetes is the one place that asks machinery's compatibility rules whether a Talos
// release runs a Kubernetes version. why is their reason when it does not, and empty when it
// does. A version that is none is a problem instead, which says which of the two it is: the Talos
// release is read first.
func runsKubernetes(talosTag, kubernetesVersion string) (why, problem string) {
	host, err := talosRelease(talosTag)
	if err != nil {
		return "", fmt.Sprintf("unparsable Talos version %q", talosTag)
	}

	kubernetes, problem := kubernetesRelease(kubernetesVersion)
	if problem != "" {
		return "", problem
	}

	if err := kubernetes.SupportedWith(host); err != nil {
		return err.Error(), ""
	}

	return "", ""
}

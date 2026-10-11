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

package plan

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/util/version"
)

// Version awareness (docs/scope.md §7.5): the sequence of upgrades that leads from the versions
// a cluster runs to the versions its spec asks for, or the reason there is none. Nothing here
// starts an upgrade. Each upgrade still decides for itself whether it may start (PlanUpgrade,
// PlanKubernetes), by the same rules; this is the same answer worked out ahead, for a human, for
// a check in CI, and for the operator to refuse what cannot work before it touches a node.

// Kinds of a step.
const (
	StepTalos      = "Talos"
	StepKubernetes = "Kubernetes"
)

// VersionState is the Talos and the Kubernetes version of a cluster: v1.14.2 and 1.37.1. A
// version that is not known, or not asked for, is empty.
type VersionState struct {
	Talos, Kubernetes string
}

// VersionStep is one upgrade on the way.
type VersionStep struct {
	Kind     string
	From, To string
}

func (s VersionStep) String() string { return fmt.Sprintf("%s %s to %s", s.Kind, s.From, s.To) }

// VersionRules is what Talos says about versions. It is handed in, because that knowledge is the
// Talos machinery's (internal/talos) and a planner asks no one.
type VersionRules struct {
	// TalosUpgrade says why Talos cannot be upgraded from one version to another while the
	// cluster runs a Kubernetes version, or returns "".
	TalosUpgrade func(from, to, kubernetes string) string
	// Runs says why a Talos version does not run a Kubernetes version, or returns "".
	Runs func(talos, kubernetes string) string
}

// Releases is what is known to exist upstream: the newest patch release of a Kubernetes minor,
// by "major.minor". It may be empty, and anything in it may be wrong: it only ever chooses the
// release of a minor on the way to the version a spec asks for.
type Releases map[string]string

// intermediate is the release a hop through a minor goes to: the newest one known, or the first
// release of the minor, which is the one that is sure to exist.
func (r Releases) intermediate(major, minor uint) string {
	first := fmt.Sprintf("%d.%d.0", major, minor)

	known, err := version.ParseGeneric(r[fmt.Sprintf("%d.%d", major, minor)])
	if err != nil || known.Major() != major || known.Minor() != minor {
		return first
	}

	return core(known)
}

// VersionPlan is the way from what a cluster runs to what its spec asks for.
type VersionPlan struct {
	// Steps are the upgrades, in the order they can be done. Empty when there is nothing to do.
	Steps []VersionStep
	// Problem says why there is no way, and what would have to come first. Steps then holds the
	// part of the way that can be gone.
	Problem string
}

// KubernetesHops lists the versions a Kubernetes upgrade goes through: one for every minor on
// the way, the last of them the version asked for. Kubernetes does not skip a minor.
func KubernetesHops(from, to string, releases Releases) ([]string, string) {
	if to == "" || from == "" || strings.TrimPrefix(from, "v") == strings.TrimPrefix(to, "v") {
		return nil, ""
	}

	current, err := version.ParseGeneric(from)
	if err != nil {
		return nil, fmt.Sprintf("%q is no Kubernetes version", from)
	}

	desired, err := version.ParseGeneric(to)
	if err != nil {
		return nil, fmt.Sprintf("%q is no Kubernetes version", to)
	}

	switch {
	case desired.LessThan(current):
		return nil, fmt.Sprintf("Kubernetes is not downgraded: the cluster runs %s and the spec asks for %s", core(current), core(desired))
	case desired.Major() != current.Major():
		return nil, fmt.Sprintf("there is no upgrade from Kubernetes %s to %s", core(current), core(desired))
	}

	var hops []string
	for minor := current.Minor() + 1; minor < desired.Minor(); minor++ {
		hops = append(hops, releases.intermediate(current.Major(), minor))
	}

	return append(hops, core(desired)), ""
}

// PlanVersions works out the upgrades from current to desired. A field of desired that is empty
// asks for nothing.
//
// Talos and Kubernetes take turns as their versions allow: a Talos version is only installed
// while it runs the Kubernetes version the cluster has then, and a Kubernetes version only
// reached while every node's Talos runs it. Where both could go next, Talos does: it is what the
// operator does (controller.operate), and a newer Talos never runs fewer of the Kubernetes
// versions still ahead.
func PlanVersions(current, desired VersionState, rules VersionRules, releases Releases) VersionPlan {
	var plan VersionPlan

	if desired.Talos == "" {
		desired.Talos = current.Talos
	}

	if desired.Kubernetes == "" {
		desired.Kubernetes = current.Kubernetes
	}

	hops, problem := KubernetesHops(current.Kubernetes, desired.Kubernetes, releases)
	if problem != "" {
		plan.Problem = problem

		return plan
	}

	// Where the spec ends up has to be a place to be, whatever the way there.
	if desired.Talos != "" && desired.Kubernetes != "" {
		if why := rules.Runs(desired.Talos, desired.Kubernetes); why != "" {
			plan.Problem = fmt.Sprintf("the spec asks for Talos %s with Kubernetes %s: %s", desired.Talos, desired.Kubernetes, why)

			return plan
		}
	}

	at := current

	for at.Talos != desired.Talos || len(hops) > 0 {
		var talosWhy, kubernetesWhy string

		if at.Talos != desired.Talos {
			if talosWhy = rules.TalosUpgrade(at.Talos, desired.Talos, at.Kubernetes); talosWhy == "" {
				plan.Steps = append(plan.Steps, VersionStep{Kind: StepTalos, From: at.Talos, To: desired.Talos})
				at.Talos = desired.Talos

				continue
			}
		}

		if len(hops) > 0 {
			if kubernetesWhy = rules.Runs(at.Talos, hops[0]); kubernetesWhy == "" {
				plan.Steps = append(plan.Steps, VersionStep{Kind: StepKubernetes, From: at.Kubernetes, To: hops[0]})
				at.Kubernetes, hops = hops[0], hops[1:]

				continue
			}
		}

		plan.Problem = stuck(at, desired, hops, talosWhy, kubernetesWhy)

		return plan
	}

	return plan
}

// stuck says why neither kind of upgrade can go next.
func stuck(at, desired VersionState, hops []string, talosWhy, kubernetesWhy string) string {
	var parts []string

	if at.Talos != desired.Talos {
		parts = append(parts, fmt.Sprintf("Talos cannot go from %s to %s: %s", at.Talos, desired.Talos, talosWhy))
	}

	if len(hops) > 0 {
		parts = append(parts, fmt.Sprintf("Kubernetes cannot go from %s to %s: %s", at.Kubernetes, hops[0], kubernetesWhy))
	}

	return strings.Join(parts, "; ")
}

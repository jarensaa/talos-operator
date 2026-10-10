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
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/version"
)

// Kubernetes upgrades (docs/scope.md §7.4). The components are the ones Talos runs from a node's
// machine config. An upgrade changes the image each names, one component on one node at a time,
// every API server first: no other component may be newer than an API server it talks to.

// Kubernetes components, in the order an upgrade takes them.
const (
	ComponentAPIServer         = "kube-apiserver"
	ComponentControllerManager = "kube-controller-manager"
	ComponentScheduler         = "kube-scheduler"
	// ComponentProxy is kube-proxy. Talos does not run it; a control plane's config names its
	// image, and an upgrade brings that in line with the rest.
	ComponentProxy   = "kube-proxy"
	ComponentKubelet = "kubelet"
)

// KubernetesComponents lists the components in upgrade order.
var KubernetesComponents = []string{
	ComponentAPIServer, ComponentControllerManager, ComponentScheduler, ComponentProxy, ComponentKubelet,
}

// Runs reports whether Talos runs the component on a node. What it does not run has no image to
// pull and nothing to be seen running.
func Runs(component string) bool { return component != ComponentProxy }

// TurnKey names one turn of a Kubernetes upgrade: one component on one machine.
func TurnKey(component, machine string) string { return component + "/" + machine }

// SplitTurn is the inverse of TurnKey. A key without a component is a machine.
func SplitTurn(key string) (component, machine string) {
	if component, machine, found := strings.Cut(key, "/"); found {
		return component, machine
	}

	return "", key
}

// KubernetesImage is what a node's config and its rendered config say about one component.
type KubernetesImage struct {
	// Live is the image the node's config names, Desired the one its rendered config names.
	Live, Desired string
}

// KubernetesMachine is what the Kubernetes planner needs to know about one machine.
type KubernetesMachine struct {
	Name string
	// Node is the hostname of the live node the machine is matched to; empty if there is none.
	Node         string
	ControlPlane bool
	// Hold excludes the machine from upgrades.
	Hold bool
	// Current is true when the node's config was just read and its spec renders. Stale
	// information is never acted on.
	Current bool
	// Images are the components the machine's config names an image for: every one on a control
	// plane, the kubelet on a worker.
	Images map[string]KubernetesImage
}

// KubernetesHop is one step of a Kubernetes upgrade: at most one minor version further.
type KubernetesHop struct {
	// From is the lowest version any component runs, To the highest version the spec asks for,
	// and Version what this hop brings every component to: To, or the first release of the minor
	// after From when To is further away than that.
	From, To, Version string
	// Problem says why the versions cannot be told, or why there is no way from From to To.
	Problem string
}

// Due reports whether the hop changes anything.
func (h KubernetesHop) Due() bool { return h.Problem == "" && h.Version != "" }

// ImageVersion is the version an image's tag names, such as 1.37.1 for
// registry.k8s.io/kube-apiserver:v1.37.1. It is false for an image without a tag, or with one
// that is no version: such an image cannot be compared with another.
func ImageVersion(image string) (*version.Version, bool) {
	tag := imageTag(image)
	if tag == "" {
		return nil, false
	}

	v, err := version.ParseGeneric(tag)
	if err != nil || len(v.Components()) < 3 {
		return nil, false
	}

	return v, true
}

// VersionOf is the version an image's tag names, as "major.minor.patch".
func VersionOf(image string) (string, bool) {
	v, ok := ImageVersion(image)
	if !ok {
		return "", false
	}

	return core(v), true
}

// imageTag is the tag of an image reference; empty if it has none.
func imageTag(image string) string {
	name, _, _ := strings.Cut(image, "@")
	at := strings.LastIndex(name, ":")
	// A colon before the last slash is the port of a registry.
	if at < 0 || at < strings.LastIndex(name, "/") {
		return ""
	}

	return name[at+1:]
}

// core is the version without anything a tag carries after it.
func core(v *version.Version) string { return fmt.Sprintf("%d.%d.%d", v.Major(), v.Minor(), v.Patch()) }

// withVersion is image with the version its tag names replaced by v, keeping what else the tag
// says (the "v", a suffix).
func withVersion(image, v string) string {
	current, ok := ImageVersion(image)
	if !ok {
		return image
	}

	tag := imageTag(image)
	at := strings.LastIndex(image, tag)

	return image[:at] + strings.Replace(tag, core(current), v, 1)
}

// HopOf works out the next hop from what the machines' configs name. Machines that are not
// current are left out: the caller blocks on them.
func HopOf(machines []KubernetesMachine) KubernetesHop {
	var (
		from, to *version.Version
		hop      KubernetesHop
		due      bool
	)

	for _, m := range machines {
		if !m.Current {
			continue
		}

		for _, component := range KubernetesComponents {
			image, has := m.Images[component]
			if !has {
				continue
			}

			live, ok := ImageVersion(image.Live)
			if !ok {
				hop.Problem = fmt.Sprintf("%s: the %s image %q of the node's config names no version", m.Name, component, image.Live)
				return hop
			}

			desired, ok := ImageVersion(image.Desired)
			if !ok {
				hop.Problem = fmt.Sprintf("%s: the %s image %q of the rendered config names no version", m.Name, component, image.Desired)
				return hop
			}

			if from == nil || live.LessThan(from) {
				from = live
			}

			if to == nil || to.LessThan(desired) {
				to = desired
			}

			due = due || image.Live != image.Desired
		}
	}

	if from == nil || !due {
		return hop
	}

	hop.From, hop.To = core(from), core(to)

	switch {
	case to.Major() != from.Major():
		hop.Problem = fmt.Sprintf("there is no upgrade from Kubernetes %s to %s", hop.From, hop.To)
	case to.Minor() > from.Minor()+1:
		hop.Version = fmt.Sprintf("%d.%d.0", from.Major(), from.Minor()+1)
	default:
		hop.Version = hop.To
	}

	return hop
}

// target is the image a component is brought to by a hop: the one its rendered config names, or
// that image at the hop's version while the rendered one is further away.
func (h KubernetesHop) target(image KubernetesImage) string {
	desired, ok := ImageVersion(image.Desired)
	hop, err := version.ParseGeneric(h.Version)

	if !ok || err != nil || !hop.LessThan(desired) {
		return image.Desired
	}

	return withVersion(image.Desired, h.Version)
}

// KubernetesSettings are the cluster-level inputs of a Kubernetes upgrade decision.
type KubernetesSettings struct {
	Paused bool
	// Aborted is the version whose upgrade a human aborted. It is not started again.
	Aborted string
	// Problems are what the checks of the hop found: a Talos version that does not support it, a
	// flag it removed, objects of an API it no longer serves. Unchecked says why they could not
	// be run; an upgrade that was not checked does not start.
	Problems  []string
	Unchecked string
	// Policy is the user's rollout policy as it stands now (docs/scope.md §6.4).
	Policy Policy
}

// KubernetesUpgrade is the planner's answer to "is a Kubernetes upgrade due, and may it start?".
type KubernetesUpgrade struct {
	Hop KubernetesHop
	// Pending lists the turns, in order: the API server on every control plane, then the
	// controller manager, the scheduler, the kube-proxy image of each control plane's config, and
	// the kubelet on control planes and then on workers.
	Pending []string
	// Images maps each turn to the image its component is to name.
	Images map[string]string
	// Change identifies exactly this upgrade: the turns with the images they leave and reach.
	Change string
	// Behind lists the machines with a component that does not name what its spec renders.
	Behind []string
	// Held lists workers that need the upgrade but are on hold.
	Held []string
	// AbortStands is true while an earlier abort covers this upgrade.
	AbortStands bool
	// BlockReason and BlockMessage are set when Pending is not empty but the upgrade may not start.
	BlockReason  string
	BlockMessage string
}

// MayStart reports whether an upgrade is due and nothing blocks it.
func (u KubernetesUpgrade) MayStart() bool { return len(u.Pending) > 0 && u.BlockReason == "" }

// PlanKubernetes decides whether a Kubernetes upgrade is due and whether it may start. p is this
// pass over the cluster: unlike a rollout that may be there to repair a node, an upgrade starts
// only when every node is healthy.
func PlanKubernetes(machines []KubernetesMachine, s KubernetesSettings, p Pass) KubernetesUpgrade {
	out := KubernetesUpgrade{Hop: HopOf(machines), Images: map[string]string{}}

	var stale, heldControlPlanes, downgrades, lines []string

	ordered := slices.Clone(machines)
	slices.SortFunc(ordered, func(a, b KubernetesMachine) int {
		return cmp.Or(-cmp.Compare(rank(a.ControlPlane), rank(b.ControlPlane)), strings.Compare(a.Name, b.Name))
	})

	for _, m := range ordered {
		if !m.Current {
			stale = append(stale, m.Name)
			continue
		}

		if slices.ContainsFunc(KubernetesComponents, func(c string) bool {
			image, has := m.Images[c]
			return has && image.Live != image.Desired
		}) {
			out.Behind = append(out.Behind, m.Name)
		}
	}

	if !out.Hop.Due() {
		if out.Hop.Problem != "" && len(out.Behind) > 0 {
			out.Pending = slices.Clone(out.Behind)
			out.BlockReason, out.BlockMessage = BlockIncompatible, out.Hop.Problem
		}

		return out
	}

	for _, component := range KubernetesComponents {
		for _, m := range ordered {
			image, has := m.Images[component]
			if !m.Current || !has {
				continue
			}

			target := out.Hop.target(image)
			if image.Live == target {
				continue
			}

			if live, _ := ImageVersion(image.Live); version.MustParseGeneric(out.Hop.Version).LessThan(live) {
				downgrades = append(downgrades, fmt.Sprintf("%s runs %s %s", m.Name, component, core(live)))
				continue
			}

			if m.Hold {
				if m.ControlPlane {
					heldControlPlanes = append(heldControlPlanes, m.Name)
				} else if !slices.Contains(out.Held, m.Name) {
					out.Held = append(out.Held, m.Name)
				}

				continue
			}

			key := TurnKey(component, m.Name)
			out.Pending = append(out.Pending, key)
			out.Images[key] = target
			lines = append(lines, key+" "+image.Live+" "+target)
		}
	}

	slices.Sort(lines)
	sum := sha256.Sum256([]byte(strings.Join(lines, "\n")))
	out.Change = hex.EncodeToString(sum[:8])
	out.AbortStands = s.Aborted != "" && s.Aborted == out.Hop.To

	if len(out.Pending) == 0 && len(downgrades)+len(heldControlPlanes) > 0 {
		// Nothing can be done, and that has to be said about something.
		out.Pending = slices.Clone(out.Behind)
	}

	if len(out.Pending) > 0 {
		out.BlockReason, out.BlockMessage = kubernetesBlock(out, s, p, stale, slices.Compact(heldControlPlanes), downgrades)
	}

	return out
}

// kubernetesBlock returns why a due upgrade may not start, most fundamental reason first.
func kubernetesBlock(u KubernetesUpgrade, s KubernetesSettings, p Pass, stale, held, downgrades []string) (string, string) {
	if len(downgrades) > 0 {
		return BlockIncompatible, fmt.Sprintf("Kubernetes is not downgraded, and the spec asks for %s while %s",
			u.Hop.To, strings.Join(downgrades, ", "))
	}

	if len(held) > 0 {
		return BlockIncompatible, "a Kubernetes upgrade cannot leave a control plane out, and these are on hold: " + strings.Join(held, ", ")
	}

	if len(s.Problems) > 0 {
		return BlockIncompatible, strings.Join(s.Problems, "; ")
	}

	if reason, message := s.Policy.block(s.Paused); reason != "" {
		return reason, message
	}

	switch {
	case len(stale) > 0:
		return BlockNotCurrent, "waiting for current status of " + strings.Join(stale, ", ")
	case s.Unchecked != "":
		return BlockNotCurrent, "the upgrade could not be checked: " + s.Unchecked
	case u.AbortStands:
		return BlockAborted, "the upgrade to this version was aborted; change the spec, or revert it, to continue"
	}

	if gate := VerifyGate(p).And(s.Policy.Gates()); !gate.Open {
		return BlockGateClosed, gate.Reason
	}

	return "", ""
}

// KubernetesTryTimeout is how long Talos keeps the tried config of a component's turn before it
// reverts it.
//
// For most components that is the try window of any config change. The API server is different:
// its restart takes the Kubernetes API away from everything on its node, and on a cluster with one
// control plane from everything. The controller manager and the scheduler lose their leases and
// restart, the operator loses its own, and none of it can be seen until the API answers again. A
// cluster in that state is not unhealthy, it is settling, and it is given what a node gets to come
// back after a reboot: stepTimeout to be healthy, then the soak and the margin to confirm in.
func KubernetesTryTimeout(component string, t Timing) time.Duration {
	if component != ComponentAPIServer {
		return t.TryTimeout
	}

	return max(t.TryTimeout, t.StepTimeout+t.Soak+ConfirmMargin)
}

// NextKubernetesStep decides what to do for the turn in flight: one component on one node. Once
// the image is on the node the turn is a config change like any other (NextStep), with one
// more check: the component runs the new version.
//
// The pull comes first so that the component is only down for its restart, and so that an image
// that cannot be pulled is found while nothing changed.
func NextKubernetesStep(step Step, node Node, pre, checks Gate, t Timing, now time.Time) Action {
	if step.Name != StepPulling {
		return NextStep(step, node, pre, checks, t, now)
	}

	// Nothing was sent yet, and nothing is recorded about the node's config: the config change
	// that follows the pull is worked out from what the node runs then.
	switch {
	case timedOut(step, t, now):
		if !node.Reachable {
			return fail(FailTimeout, "not pulled within %s: %s", t.StepTimeout, notReachable)
		}

		return fail(FailTimeout, "not pulled within %s: %s", t.StepTimeout, pre.Reason)
	case !node.Reachable:
		return wait(notReachable)
	case !pre.Open:
		return wait("%s", pre.Reason)
	default:
		return Action{Kind: Pull}
	}
}

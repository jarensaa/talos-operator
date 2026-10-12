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

// Package plan holds the operator's planners: pure functions from observed and desired state to
// the next action. They do no I/O, so every safety invariant (docs/scope.md §8.1) is decided here
// and table-tested; the controllers only observe, call a planner and carry out its answer.
package plan

import (
	"cmp"
	"fmt"
	"slices"
	"strings"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/classify"
)

// Machine is what the rollout planner needs to know about one machine.
type Machine struct {
	Name string
	// Node is the hostname of the live node the machine is matched to; empty if there is none.
	Node         string
	ControlPlane bool
	// Hold excludes the machine from rollouts.
	Hold bool
	// Current is true when the machine is matched to its node and its reported hashes reflect its
	// spec and the live config as the cluster sees it now. Stale information is never acted on.
	Current bool
	// LiveHash, DesiredHash and AppliedHash are hashes of normalized configs: what the node runs,
	// what its spec renders, and what the operator last applied or took over.
	LiveHash, DesiredHash, AppliedHash string
	// Class and Reasons classify the change from live to desired.
	Class   classify.Class
	Reasons []string
}

// Settings are the cluster-level inputs of a rollout decision.
type Settings struct {
	DriftPolicy talosv1alpha1.DriftPolicy
	// ApprovedChange is the change hash a human approved, if any.
	ApprovedChange string
	// Skipped and Aborted map a machine to the rendered config hash a human left it out with, or
	// whose rollout a human aborted. Neither is rolled out again; any other config is.
	Skipped, Aborted map[string]string
	// Policy is the user's rollout policy as it stands now (docs/scope.md §6.4).
	Policy Policy
}

// Rollout is the planner's answer to "is a config rollout due, and may it start?". Its turns are
// the machines whose config must change: a machine whose node is unhealthy first, then workers
// before control planes, each group by name. Its targets are the rendered config hashes they are
// to reach.
type Rollout struct {
	Due
	// Bases maps each pending machine to the live config hash it is to leave. An operation applies
	// exactly the change from base to target: that pair is what was classified and, where needed,
	// approved.
	Bases map[string]string
	// Drifted lists machines whose live config differs from what was applied while their spec is
	// unchanged, and which the drift policy leaves alone.
	Drifted []string
	// NeedsApproval lists pending machines whose change class requires a human's approval.
	NeedsApproval []string
}

// PlanRollout decides whether a config rollout is due and whether it may start. p is this pass
// over the cluster: a rollout starts when the first node of its plan may be touched (TouchGate).
//
// A machine whose node is unhealthy goes first. The gate lets nothing else be touched while a
// node is unhealthy, so any other order would wait for the node the rollout is there to repair.
func PlanRollout(machines []Machine, s Settings, p Pass) Rollout {
	var (
		out     = Rollout{Due: Due{Targets: map[string]string{}}, Bases: map[string]string{}}
		pending []Machine
		stale   []string
	)

	for _, m := range machines {
		if !m.Current {
			// Without current information the machine can be neither planned nor called in sync.
			// It blocks the rollout below if anything else is pending.
			stale = append(stale, m.Name)

			continue
		}

		if m.LiveHash == m.DesiredHash {
			continue
		}

		specChanged := m.DesiredHash != m.AppliedHash

		switch {
		case m.Hold, s.Skipped[m.Name] == m.DesiredHash:
			out.Held = append(out.Held, m.Name)
		case specChanged, s.DriftPolicy == talosv1alpha1.DriftPolicyEnforce:
			pending = append(pending, m)
		default:
			out.Drifted = append(out.Drifted, m.Name)
		}
	}

	slices.Sort(out.Held)
	slices.Sort(out.Drifted)
	slices.Sort(stale)

	slices.SortFunc(pending, func(a, b Machine) int {
		return cmp.Or(
			-cmp.Compare(rank(ails(p, a.Node)), rank(ails(p, b.Node))),
			cmp.Compare(rank(a.ControlPlane), rank(b.ControlPlane)),
			strings.Compare(a.Name, b.Name),
		)
	})

	lines := make([]string, 0, len(pending))

	for _, m := range pending {
		out.Pending = append(out.Pending, m.Name)
		out.Targets[m.Name] = m.DesiredHash
		out.Bases[m.Name] = m.LiveHash
		lines = append(lines, m.Name+" "+m.LiveHash+" "+m.DesiredHash)

		if m.Class.NeedsApproval() {
			out.NeedsApproval = append(out.NeedsApproval, m.Name)
		}
	}

	out.AbortStands = abortStands(s.Aborted, stale, out.Targets)

	if len(pending) == 0 {
		return out
	}

	out.Change = fingerprint(lines)
	out.BlockReason, out.BlockMessage = firstBlock(
		classBlock(pending, classify.Protected, BlockProtected, "protected config would change, which the operator never does"),
		classBlock(pending, classify.BootTime, BlockRequiresReinstall,
			"the change only takes effect through a reinstall, which is not supported yet"),
		classBlock(pending, classify.Kubernetes, BlockAwaitingKubernetes,
			"the config names another version of a Kubernetes component than the node runs, which a "+
				"Kubernetes upgrade brings about, in its own order (spec.management.upgrades)"),
		classBlock(pending, classify.Installer, BlockAwaitingUpgrade,
			"the config names an installer image the node does not run yet, which a Talos upgrade "+
				"brings about (spec.management.upgrades); it is applied once the node runs it"),
		s.Policy.block(),
		staleBlock(stale),
		abortedBlock(out.AbortStands, "the rollout of this config"),
		approvalBlock(out, s.ApprovedChange),
		gateBlock(TouchGate(p, pending[0].Node).And(s.Policy.Gates())),
	)

	return out
}

// classBlock: a change of a class that a rollout does not apply. why says what keeps it back.
func classBlock(pending []Machine, class classify.Class, reason, why string) block {
	var blocked []Machine

	for _, m := range pending {
		if m.Class == class {
			blocked = append(blocked, m)
		}
	}

	if len(blocked) == 0 {
		return block{}
	}

	return block{reason, why + ": " + describe(blocked)}
}

// approvalBlock: a change that needs a human's approval, which approves exactly this change.
func approvalBlock(r Rollout, approved string) block {
	if len(r.NeedsApproval) == 0 || approved == r.Change {
		return block{}
	}

	return block{BlockAwaitingApproval, fmt.Sprintf(
		"the change to %s needs approval: review each machine's status.config.diff, then approve change %s",
		strings.Join(r.NeedsApproval, ", "), r.Change)}
}

// describe names machines with the reasons for their class, bounded for a condition message.
func describe(machines []Machine) string {
	const maxReasons = 3

	parts := make([]string, 0, len(machines))

	for _, m := range machines {
		reasons := m.Reasons
		more := ""

		if len(reasons) > maxReasons {
			more = fmt.Sprintf(" and %d more", len(reasons)-maxReasons)
			reasons = reasons[:maxReasons]
		}

		parts = append(parts, fmt.Sprintf("%s (%s%s)", m.Name, strings.Join(reasons, ", "), more))
	}

	return strings.Join(parts, "; ")
}

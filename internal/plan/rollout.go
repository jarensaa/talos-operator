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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"

	"github.com/jarensaa/talos-operator/internal/classify"
)

// Machine is what the rollout planner needs to know about one machine.
type Machine struct {
	Name         string
	ControlPlane bool
	// Hold excludes the machine from rollouts.
	Hold bool
	// Current is true when the machine is matched to its node and its reported hashes reflect its
	// spec and the live config as the cluster sees it now. Stale information is never acted on.
	Current bool
	// LiveHash, DesiredHash and AppliedHash are hashes of normalized configs: what the node runs,
	// what its spec renders, and what the operator last applied or took over.
	LiveHash, DesiredHash, AppliedHash string
	// SkippedHash is a rendered config a human skipped for this machine.
	SkippedHash string
	// Class and Reasons classify the change from live to desired.
	Class   classify.Class
	Reasons []string
}

// Drift policies.
const (
	DriftReport  = "Report"
	DriftEnforce = "Enforce"
)

// Settings are the cluster-level inputs of a rollout decision.
type Settings struct {
	DriftPolicy string
	Paused      bool
	// ApprovedChange is the change hash a human approved, if any.
	ApprovedChange string
	// AbortedTargets is what the operation a human aborted was rolling out: machine name to
	// rendered config hash. Nil if nothing was aborted.
	AbortedTargets map[string]string
}

// Gate is a condition that must hold before a node is touched.
type Gate struct {
	Open bool
	// Reason says what is closed.
	Reason string
}

// Block reasons of a rollout that may not start. They match the RolloutBlocked condition reasons.
const (
	BlockPaused            = "Paused"
	BlockNotCurrent        = "AwaitingStatus"
	BlockProtected         = "ProtectedChange"
	BlockRequiresReinstall = "RequiresReinstall"
	BlockAborted           = "Aborted"
	BlockAwaitingApproval  = "AwaitingApproval"
	BlockGateClosed        = "GateClosed"
)

// Rollout is the planner's answer to "is a config rollout due, and may it start?".
type Rollout struct {
	// Pending lists the machines whose config must change, in rollout order: workers before
	// control planes, each group by name.
	Pending []string
	// Targets maps each pending machine to the rendered config hash it is to reach, and Bases to
	// the live config hash it is to leave. An operation applies exactly these pairs: they are what
	// was classified and, where needed, approved.
	Targets map[string]string
	Bases   map[string]string
	// ChangeHash identifies exactly this change: the pending machines with their live and desired
	// hashes. Approvals and aborts refer to it, so neither can be replayed onto another change.
	ChangeHash string
	// Drifted lists machines whose live config differs from what was applied while their spec is
	// unchanged, and which the drift policy leaves alone.
	Drifted []string
	// Held lists machines that need a change but are excluded: on hold, or skipped by a human.
	Held []string
	// NeedsApproval lists pending machines whose change class requires a human's approval.
	NeedsApproval []string
	// AbortStands is true while an earlier abort still applies: every pending machine would get
	// exactly the config whose rollout was aborted, or that cannot be told yet. Once it is
	// false the abort is spent and can be forgotten.
	AbortStands bool
	// BlockReason and BlockMessage are set when Pending is not empty but the rollout may not start.
	BlockReason  string
	BlockMessage string
}

// MayStart reports whether a rollout is due and nothing blocks it.
func (r Rollout) MayStart() bool { return len(r.Pending) > 0 && r.BlockReason == "" }

// PlanRollout decides whether a config rollout is due and whether it may start. gate is the
// cluster-wide precondition for touching the first node (cluster healthy, etcd whole).
func PlanRollout(machines []Machine, s Settings, gate Gate) Rollout {
	var (
		out     = Rollout{Targets: map[string]string{}, Bases: map[string]string{}}
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
		case m.Hold, m.SkippedHash == m.DesiredHash:
			out.Held = append(out.Held, m.Name)
		case specChanged, s.DriftPolicy == DriftEnforce:
			pending = append(pending, m)
		default:
			out.Drifted = append(out.Drifted, m.Name)
		}
	}

	slices.Sort(out.Held)
	slices.Sort(out.Drifted)
	slices.Sort(stale)

	out.AbortStands = abortStands(pending, stale, s.AbortedTargets)

	if len(pending) == 0 {
		return out
	}

	slices.SortFunc(pending, func(a, b Machine) int {
		if a.ControlPlane != b.ControlPlane {
			if a.ControlPlane {
				return 1
			}

			return -1
		}

		return strings.Compare(a.Name, b.Name)
	})

	for _, m := range pending {
		out.Pending = append(out.Pending, m.Name)
		out.Targets[m.Name] = m.DesiredHash
		out.Bases[m.Name] = m.LiveHash

		if m.Class.NeedsApproval() {
			out.NeedsApproval = append(out.NeedsApproval, m.Name)
		}
	}

	out.ChangeHash = changeHash(pending)
	out.BlockReason, out.BlockMessage = rolloutBlock(pending, stale, s, gate, out)

	return out
}

// abortStands reports whether an abort still covers what is pending. It does while any machine
// cannot be judged, and while every pending machine is to get the config the aborted operation
// was rolling out to it. A pending machine with any other config is a change the human who
// aborted has not seen: the spec moved on, and so does the rollout.
func abortStands(pending []Machine, stale []string, aborted map[string]string) bool {
	if len(aborted) == 0 {
		return false
	}

	if len(stale) > 0 {
		return true
	}

	for _, m := range pending {
		if aborted[m.Name] != m.DesiredHash {
			return false
		}
	}

	return len(pending) > 0
}

// rolloutBlock returns why a due rollout may not start, most fundamental reason first.
func rolloutBlock(pending []Machine, stale []string, s Settings, gate Gate, r Rollout) (string, string) {
	if blocked := withClass(pending, classify.Protected); len(blocked) > 0 {
		return BlockProtected, "protected config would change, which the operator never does: " + describe(blocked)
	}

	if blocked := withClass(pending, classify.BootTime); len(blocked) > 0 {
		return BlockRequiresReinstall, "the change only takes effect through a reinstall, which is not supported yet: " + describe(blocked)
	}

	if s.Paused {
		return BlockPaused, "spec.paused is set"
	}

	if len(stale) > 0 {
		return BlockNotCurrent, "waiting for current status of " + strings.Join(stale, ", ")
	}

	if r.AbortStands {
		return BlockAborted, "the rollout of this config was aborted; change the spec, or revert it, to continue"
	}

	if len(r.NeedsApproval) > 0 && s.ApprovedChange != r.ChangeHash {
		return BlockAwaitingApproval, fmt.Sprintf(
			"the change to %s needs approval: review each machine's status.config.diff, then approve change %s",
			strings.Join(r.NeedsApproval, ", "), r.ChangeHash)
	}

	if !gate.Open {
		return BlockGateClosed, gate.Reason
	}

	return "", ""
}

func withClass(machines []Machine, class classify.Class) []Machine {
	var out []Machine

	for _, m := range machines {
		if m.Class == class {
			out = append(out, m)
		}
	}

	return out
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

// changeHash fingerprints a change: which machines go from which live config to which rendered
// config. It does not depend on the order of machines.
func changeHash(pending []Machine) string {
	lines := make([]string, 0, len(pending))
	for _, m := range pending {
		lines = append(lines, m.Name+" "+m.LiveHash+" "+m.DesiredHash)
	}

	slices.Sort(lines)

	sum := sha256.Sum256([]byte(strings.Join(lines, "\n")))

	return hex.EncodeToString(sum[:8])
}

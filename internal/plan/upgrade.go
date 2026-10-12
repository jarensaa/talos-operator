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
	"slices"
	"strings"
	"time"
)

// Steps of a Talos upgrade on one node (docs/scope.md §7.3). The step is persisted before its
// side effect, and a resumed operator decides what to do from the step and what it observes on
// the node, never from memory.
//
// The order is Talos's own since 1.13 (cmd/talosctl/cmd/talos/upgrade.go:93-174): the new system
// is installed to the inactive boot slot while the node keeps running, and only then is the node
// drained and rebooted. Everything before the cordon leaves the node exactly as useful as it was.
const (
	// StepPulling fetches the installer image to the node.
	StepPulling = "Pulling"
	// StepInstalling writes the new system to the inactive boot slot.
	StepInstalling = "Installing"
	// StepDraining cordons the node and evicts its pods, respecting disruption budgets.
	StepDraining = "Draining"
	// StepRebooting asks the node to reboot. A control plane hands off etcd leadership first.
	StepRebooting = "Rebooting"
	// StepBooting waits for the node to come back, on the system that was installed.
	StepBooting = "Booting"
	// StepUncordoning makes the node schedulable again, if the operator cordoned it.
	StepUncordoning = "Uncordoning"
)

// Failure reasons of an upgrade step, besides FailTimeout.
const (
	// FailImagePull: the installer image could not be pulled. Nothing on the node changed.
	FailImagePull = "ImagePullFailed"
	// FailInstall: the installer failed. The node still runs what it ran.
	FailInstall = "InstallFailed"
	// FailFellBack: the node rebooted and is running, but not on the system that was installed.
	// Talos reverts to the previous boot slot by itself when the new one fails to boot.
	FailFellBack = "FellBack"
)

// UpgradeNode is what was just observed of the step's node.
type UpgradeNode struct {
	// Reachable is true when the node's Talos API answered and its identity is the pinned one.
	Reachable bool
	// BootID is empty when it was not read.
	BootID string
	// Running is true when the node finished booting and reports itself ready.
	Running bool
	// Installed is whether the node runs the target version from the target schematic. It only
	// means something when InstallKnown is set.
	Installed    bool
	InstallKnown bool
	// KubernetesKnown is false when the node's Node object was not read; the rest then means
	// nothing.
	KubernetesKnown bool
	Unschedulable   bool
	NodeReady       bool
	// PodsLeft is how many pods still have to leave the node; negative when not counted.
	PodsLeft int
}

// NextUpgradeStep decides what to do for the upgrade step in flight.
//
// pre is the gate for taking the node down (RebootGate, and nothing pausing the operation). It is
// asked before the first thing that costs the cluster anything, the cordon, and again before the
// reboot. checks is the verification of the node afterwards.
//
// The rules this function never breaks:
//   - a node is not cordoned, and not rebooted, while the gate is closed;
//   - a reboot is only asked for while the node still has the boot ID that was recorded, so a node
//     that already rebooted is never rebooted again;
//   - a node that came back on anything but the installed system is a failure, never a success;
//   - a node is only uncordoned by the operator if the operator cordoned it.
func NextUpgradeStep(step Step, node UpgradeNode, pre, checks Gate, t Timing, now time.Time) Action {
	switch step.Name {
	case StepPulling:
		return nextPulling(step, node, pre, t, now)
	case StepInstalling:
		return nextInstalling(step, node, t, now)
	case StepDraining:
		return nextDraining(step, node, pre, t, now)
	case StepRebooting:
		return nextRebooting(step, node, pre, t, now)
	case StepBooting:
		return nextBooting(step, node, t, now)
	case StepUncordoning:
		return nextUncordoning(step, node, t, now)
	case StepVerifying:
		return nextSettling(step, node, checks, t, now)
	default:
		return unknownStep(step.Name)
	}
}

// installed reports whether the node is known to run the target already: an upgrade that needs
// no installing, as after a retry of one that got that far.
func (n UpgradeNode) installed() bool {
	return n.Reachable && n.InstallKnown && n.Installed && n.Running
}

func nextPulling(step Step, node UpgradeNode, pre Gate, t Timing, now time.Time) Action {
	if node.installed() {
		return advance(StepUncordoning)
	}

	// Pulling takes nothing from the cluster, but an upgrade that cannot go on is not begun.
	return step.do(Pull, "started", node.Reachable, notReachable, pre, t, now)
}

func nextInstalling(step Step, node UpgradeNode, t Timing, now time.Time) Action {
	if node.installed() {
		return advance(StepUncordoning)
	}

	return step.do(Install, "installed", node.Reachable, notReachable, Gate{Open: true}, t, now)
}

func nextDraining(step Step, node UpgradeNode, pre Gate, t Timing, now time.Time) Action {
	if action, draining := step.drain(node.KubernetesKnown, node.KubernetesKnown && node.Unschedulable, node.PodsLeft, pre, t, now); draining {
		return action
	}

	if !node.Reachable || node.BootID == "" {
		// The boot ID is recorded with the next step. Without it nothing could tell later whether
		// the node rebooted.
		return wait(notReachable)
	}

	return advance(StepRebooting)
}

func nextRebooting(step Step, node UpgradeNode, pre Gate, t Timing, now time.Time) Action {
	if node.Reachable && node.BootID != "" && node.BootID != step.BootID {
		// It rebooted: the request went out and its answer, or the step after it, was lost.
		return advance(StepBooting)
	}

	return step.do(Reboot, "rebooted", node.Reachable && node.BootID != "", notReachable, pre, t, now)
}

func nextBooting(step Step, node UpgradeNode, t Timing, now time.Time) Action {
	rebooted := node.Reachable && node.BootID != "" && node.BootID != step.BootID

	switch {
	case rebooted && node.Running && node.InstallKnown && node.Installed:
		return advance(StepUncordoning)
	case rebooted && node.Running && node.InstallKnown:
		return fail(FailFellBack, "the node rebooted and is running, but not on the system that was installed: "+
			"Talos falls back to the previous boot when the new one does not come up")
	case step.timedOut(t, now) && !rebooted:
		return fail(FailTimeout, "the node did not come back from its reboot within %s; check its console", t.StepTimeout)
	case step.timedOut(t, now):
		return fail(FailTimeout, "the node rebooted but did not become ready within %s", t.StepTimeout)
	case !rebooted:
		return wait("waiting for the node to reboot")
	default:
		return wait("waiting for the node to finish booting")
	}
}

func nextUncordoning(step Step, node UpgradeNode, t Timing, now time.Time) Action {
	switch {
	case !step.Cordoned:
		return advance(StepVerifying)
	case node.KubernetesKnown && !node.Unschedulable:
		return advance(StepVerifying)
	case step.timedOut(t, now):
		return fail(FailTimeout, "Kubernetes did not report the node Ready within %s; it stays cordoned", t.StepTimeout)
	case !node.KubernetesKnown:
		return wait("the node's Kubernetes Node could not be read")
	case !node.NodeReady:
		return wait("waiting for Kubernetes to report the node Ready")
	default:
		return Action{Kind: Uncordon}
	}
}

// nextSettling verifies an upgraded node like one whose config was changed: the whole cluster,
// this node included, must be healthy for the soak before the upgrade moves on.
func nextSettling(step Step, node UpgradeNode, checks Gate, t Timing, now time.Time) Action {
	if node.Reachable && node.Running && node.InstallKnown && !node.Installed {
		return fail(FailFellBack, "the node no longer runs the system that was installed")
	}

	reason := checks.Reason
	if !node.Reachable {
		reason = notReachable
	}

	return step.settle(node.installed() && checks.Open, reason, t, now)
}

// UpgradeMachine is what the upgrade planner needs to know about one machine.
type UpgradeMachine struct {
	Name string
	// Node is the hostname of the live node the machine is matched to; empty if there is none.
	Node         string
	ControlPlane bool
	// Hold excludes the machine from upgrades.
	Hold bool
	// Current is true when the machine is matched to a node whose version and schematic were just
	// read. Stale information is never acted on.
	Current bool
	// Installed is what the node runs, Target what its spec asks for: "<version> <schematic>".
	Installed, Target string
	// Image is the installer image that brings the node to Target.
	Image string
	// Problem says why the node cannot be taken to Target at all, or is empty.
	Problem string
	// EtcdLeader and HostsOperator move a machine to the end of its group: each is a handover
	// that an upgrade of that node costs, and neither is needed more than once.
	EtcdLeader, HostsOperator bool
}

// UpgradeSettings are the cluster-level inputs of an upgrade decision.
type UpgradeSettings struct {
	// Skipped and Aborted map a machine to the installer image a human skipped for it, or whose
	// upgrade a human aborted. Neither is started again; any other image is.
	Skipped, Aborted map[string]string
	// Policy is the user's rollout policy as it stands now (docs/scope.md §6.4).
	Policy Policy
}

// Upgrade is the planner's answer to "is a Talos upgrade due, and may it start?". Its turns are
// the machines to upgrade: a machine whose node is unhealthy first, then workers before control
// planes; within a group the node the operator itself runs on and the etcd leader last, then by
// name. Its targets are the installer images they are to be installed from.
type Upgrade struct {
	Due
}

// PlanUpgrade decides whether a Talos upgrade is due and whether it may start. p is this pass
// over the cluster: an upgrade starts when the first node of its plan may be taken down.
func PlanUpgrade(machines []UpgradeMachine, s UpgradeSettings, p Pass) Upgrade {
	var (
		out     = Upgrade{Due: Due{Targets: map[string]string{}}}
		pending []UpgradeMachine
		stale   []string
	)

	for _, m := range machines {
		switch {
		case !m.Current:
			stale = append(stale, m.Name)
		case m.Installed == m.Target:
		case m.Hold, s.Skipped[m.Name] == m.Image:
			out.Held = append(out.Held, m.Name)
		default:
			pending = append(pending, m)
		}
	}

	slices.Sort(out.Held)
	slices.Sort(stale)

	slices.SortFunc(pending, func(a, b UpgradeMachine) int {
		return cmp.Or(
			-cmp.Compare(rank(ails(p, a.Node)), rank(ails(p, b.Node))),
			cmp.Compare(rank(a.ControlPlane), rank(b.ControlPlane)),
			cmp.Compare(rank(a.HostsOperator), rank(b.HostsOperator)),
			cmp.Compare(rank(a.EtcdLeader), rank(b.EtcdLeader)),
			strings.Compare(a.Name, b.Name),
		)
	})

	var problems []string

	lines := make([]string, 0, len(pending))

	for _, m := range pending {
		out.Pending = append(out.Pending, m.Name)
		out.Targets[m.Name] = m.Image
		lines = append(lines, m.Name+" "+m.Installed+" "+m.Image)

		if m.Problem != "" {
			problems = append(problems, m.Name+": "+m.Problem)
		}
	}

	out.AbortStands = abortStands(s.Aborted, stale, out.Targets)

	if len(pending) == 0 {
		return out
	}

	out.Change = fingerprint(lines)
	out.BlockReason, out.BlockMessage = firstBlock(
		incompatibleBlock(problems),
		s.Policy.block(),
		staleBlock(stale),
		abortedBlock(out.AbortStands, "the upgrade to this installer"),
		gateBlock(RebootGate(p, pending[0].Node).And(s.Policy.Gates())),
	)

	return out
}

// incompatibleBlock: something cannot be taken to what its spec asks for at all.
func incompatibleBlock(problems []string) block {
	if len(problems) == 0 {
		return block{}
	}

	return block{BlockIncompatible, strings.Join(problems, "; ")}
}

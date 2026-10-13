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
	"slices"
	"strconv"
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
	// FailDrainTimeout: the node's pods could not all be evicted in time. The node stays cordoned.
	FailDrainTimeout = "DrainTimeout"
	// FailFellBack: the node rebooted and is running, but not on the system that was installed.
	// Talos reverts to the previous boot slot by itself when the new one fails to boot.
	FailFellBack = "FellBack"
)

// DefaultDrainTimeout bounds the eviction of a node's pods.
const DefaultDrainTimeout = 10 * time.Minute

// UpgradeStep is the persisted state of the upgrade step in flight.
type UpgradeStep struct {
	Name string
	// Since is when this step began.
	Since time.Time
	// BootID is the node's boot ID recorded before the reboot is asked for. A different boot ID
	// afterwards is the one fact that says the reboot happened.
	BootID string
	// Cordoned is true when the operator decided to cordon the node, which it records before it
	// does so. It only ever uncordons a node it cordoned itself.
	Cordoned bool
	// HealthySince is when verification first passed without interruption; zero if it does not.
	HealthySince time.Time
}

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

// Upgrade actions, in addition to the ones a config step uses.
const (
	// Pull the installer image to the node.
	Pull ActionKind = iota + 100
	// Install the pulled image to the inactive boot slot.
	Install
	// MarkCordoned: persist that the operator is about to cordon the node.
	MarkCordoned
	// Cordon the node.
	Cordon
	// Evict the pods that are left.
	Evict
	// Reboot the node; a control plane gives up etcd leadership first.
	Reboot
	// Uncordon the node.
	Uncordon
)

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
func NextUpgradeStep(step UpgradeStep, node UpgradeNode, pre, checks Gate, t Timing, now time.Time) Action {
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
		return fail(FailTimeout, "unknown step %q", step.Name)
	}
}

// installed reports whether the node is known to run the target already: an upgrade that needs
// no installing, as after a retry of one that got that far.
func (n UpgradeNode) installed() bool {
	return n.Reachable && n.InstallKnown && n.Installed && n.Running
}

func nextPulling(step UpgradeStep, node UpgradeNode, pre Gate, t Timing, now time.Time) Action {
	switch {
	case node.installed():
		return advance(StepUncordoning)
	case timedOut(Step{Since: step.Since}, t, now):
		return fail(FailTimeout, "not started within %s: %s", t.StepTimeout, cmp.Or(pre.Reason, notReachable))
	case !node.Reachable:
		return wait(notReachable)
	case !pre.Open:
		// Pulling takes nothing from the cluster, but an upgrade that cannot go on is not begun.
		return wait("%s", pre.Reason)
	default:
		return Action{Kind: Pull}
	}
}

func nextInstalling(step UpgradeStep, node UpgradeNode, t Timing, now time.Time) Action {
	switch {
	case node.installed():
		return advance(StepUncordoning)
	case timedOut(Step{Since: step.Since}, t, now):
		return fail(FailTimeout, "not installed within %s: %s", t.StepTimeout, notReachable)
	case !node.Reachable:
		return wait(notReachable)
	default:
		return Action{Kind: Install}
	}
}

func nextDraining(step UpgradeStep, node UpgradeNode, pre Gate, t Timing, now time.Time) Action {
	// A node the operator did not cordon is either not cordoned yet, or cordoned by someone else,
	// who is then also the one to uncordon it.
	ours := step.Cordoned
	cordoned := node.KubernetesKnown && node.Unschedulable

	switch {
	case !now.Before(step.Since.Add(t.DrainTimeout)):
		return fail(FailDrainTimeout, "not drained within %s: %s", t.DrainTimeout, drainWait(node, pre, cordoned))
	case !node.KubernetesKnown:
		return wait("the node's Kubernetes Node could not be read")
	case !cordoned && !pre.Open:
		return wait("%s", pre.Reason)
	case !cordoned && !ours:
		return Action{Kind: MarkCordoned}
	case !cordoned:
		return Action{Kind: Cordon}
	case node.PodsLeft < 0:
		return wait("the node's pods could not be listed")
	case node.PodsLeft > 0:
		return Action{Kind: Evict, Message: drainWait(node, pre, cordoned)}
	case !node.Reachable || node.BootID == "":
		// The boot ID is recorded with the next step. Without it nothing could tell later whether
		// the node rebooted.
		return wait(notReachable)
	default:
		return advance(StepRebooting)
	}
}

func drainWait(node UpgradeNode, pre Gate, cordoned bool) string {
	switch {
	case !node.KubernetesKnown:
		return "the node's Kubernetes Node could not be read"
	case !cordoned:
		return cmp.Or(pre.Reason, "the node is not cordoned yet")
	case node.PodsLeft < 0:
		return "the node's pods could not be listed"
	default:
		return pods(node.PodsLeft) + " still on the node"
	}
}

func pods(n int) string {
	if n == 1 {
		return "1 pod"
	}

	return strconv.Itoa(n) + " pods"
}

func nextRebooting(step UpgradeStep, node UpgradeNode, pre Gate, t Timing, now time.Time) Action {
	switch {
	case node.Reachable && node.BootID != "" && node.BootID != step.BootID:
		// It rebooted: the request went out and its answer, or the step after it, was lost.
		return advance(StepBooting)
	case timedOut(Step{Since: step.Since}, t, now):
		return fail(FailTimeout, "not rebooted within %s: %s", t.StepTimeout, cmp.Or(pre.Reason, notReachable))
	case !node.Reachable || node.BootID == "":
		return wait(notReachable)
	case !pre.Open:
		return wait("%s", pre.Reason)
	default:
		return Action{Kind: Reboot}
	}
}

func nextBooting(step UpgradeStep, node UpgradeNode, t Timing, now time.Time) Action {
	rebooted := node.Reachable && node.BootID != "" && node.BootID != step.BootID

	switch {
	case rebooted && node.Running && node.InstallKnown && node.Installed:
		return advance(StepUncordoning)
	case rebooted && node.Running && node.InstallKnown:
		return fail(FailFellBack, "the node rebooted and is running, but not on the system that was installed: "+
			"Talos falls back to the previous boot when the new one does not come up")
	case timedOut(Step{Since: step.Since}, t, now) && !rebooted:
		return fail(FailTimeout, "the node did not come back from its reboot within %s; check its console", t.StepTimeout)
	case timedOut(Step{Since: step.Since}, t, now):
		return fail(FailTimeout, "the node rebooted but did not become ready within %s", t.StepTimeout)
	case !rebooted:
		return wait("waiting for the node to reboot")
	default:
		return wait("waiting for the node to finish booting")
	}
}

func nextUncordoning(step UpgradeStep, node UpgradeNode, t Timing, now time.Time) Action {
	switch {
	case !step.Cordoned:
		return advance(StepVerifying)
	case node.KubernetesKnown && !node.Unschedulable:
		return advance(StepVerifying)
	case timedOut(Step{Since: step.Since}, t, now):
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
func nextSettling(step UpgradeStep, node UpgradeNode, checks Gate, t Timing, now time.Time) Action {
	healthy := node.installed() && checks.Open

	reason := checks.Reason
	if !node.Reachable {
		reason = notReachable
	}

	switch {
	case node.Reachable && node.Running && node.InstallKnown && !node.Installed:
		return fail(FailFellBack, "the node no longer runs the system that was installed")
	case healthy && !step.HealthySince.IsZero() && now.Sub(step.HealthySince) >= t.Soak:
		return Action{Kind: Complete}
	case timedOut(Step{Since: step.Since}, t, now):
		return fail(FailTimeout, "not verified within %s: %s", t.StepTimeout, reason)
	case !healthy && !step.HealthySince.IsZero():
		return Action{Kind: MarkUnhealthy, Message: reason}
	case !healthy:
		return wait("%s", reason)
	case step.HealthySince.IsZero():
		return Action{Kind: MarkHealthy}
	default:
		return soaking(Step{HealthySince: step.HealthySince}, t)
	}
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
	// Unverified says why it could not be told that the node would boot Target, or is empty. It
	// is not a problem of the node, and it holds the upgrade up all the same: a node that does
	// not come back is found out at its reboot, which is too late.
	Unverified string
	// EtcdLeader and HostsOperator move a machine to the end of its group: each is a handover
	// that an upgrade of that node costs, and neither is needed more than once.
	EtcdLeader, HostsOperator bool
}

// UpgradeSettings are the cluster-level inputs of an upgrade decision.
type UpgradeSettings struct {
	Paused bool
	// Skipped and Aborted map a machine to the installer image a human skipped for it, or whose
	// upgrade a human aborted. Neither is started again; any other image is.
	Skipped, Aborted map[string]string
	// Policy is the user's rollout policy as it stands now (docs/scope.md §6.4).
	Policy Policy
}

// Block reasons of an upgrade, besides those of a config rollout.
const (
	// BlockIncompatible: a node cannot be taken to the version or installer its spec asks for.
	BlockIncompatible = "Incompatible"
	// BlockUnverified: whether a node would boot what its spec asks for could not be told.
	BlockUnverified = "Unverified"
)

// Upgrade is the planner's answer to "is a Talos upgrade due, and may it start?".
type Upgrade struct {
	// Pending lists the machines to upgrade, in order: a machine whose node is unhealthy first,
	// then workers before control planes; within a group the node the operator itself runs on
	// and the etcd leader last, then by name.
	Pending []string
	// Images maps each pending machine to the installer image it is to be installed from.
	Images map[string]string
	// Change identifies exactly this upgrade: the pending machines with what they run and get.
	Change string
	// Held lists machines that need an upgrade but are excluded: on hold, or skipped by a human.
	Held []string
	// AbortStands is true while an earlier abort still covers everything that is pending.
	AbortStands bool
	// BlockReason and BlockMessage are set when Pending is not empty but the upgrade may not start.
	BlockReason  string
	BlockMessage string
}

// MayStart reports whether an upgrade is due and nothing blocks it.
func (u Upgrade) MayStart() bool { return len(u.Pending) > 0 && u.BlockReason == "" }

// PlanUpgrade decides whether a Talos upgrade is due and whether it may start. p is this pass
// over the cluster: an upgrade starts when the first node of its plan may be taken down.
func PlanUpgrade(machines []UpgradeMachine, s UpgradeSettings, p Pass) Upgrade {
	var (
		out     = Upgrade{Images: map[string]string{}}
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

	// An abort stands while any machine cannot be judged, and while every pending machine would
	// get exactly the installer whose upgrade was aborted.
	out.AbortStands = len(s.Aborted) > 0 && (len(stale) > 0 || len(pending) > 0)

	if len(pending) == 0 {
		return out
	}

	slices.SortFunc(pending, func(a, b UpgradeMachine) int {
		return cmp.Or(
			-cmp.Compare(rank(ails(p, Machine{Node: a.Node})), rank(ails(p, Machine{Node: b.Node}))),
			cmp.Compare(rank(a.ControlPlane), rank(b.ControlPlane)),
			cmp.Compare(rank(a.HostsOperator), rank(b.HostsOperator)),
			cmp.Compare(rank(a.EtcdLeader), rank(b.EtcdLeader)),
			strings.Compare(a.Name, b.Name),
		)
	})

	lines := make([]string, 0, len(pending))

	for _, m := range pending {
		out.Pending = append(out.Pending, m.Name)
		out.Images[m.Name] = m.Image
		lines = append(lines, m.Name+" "+m.Installed+" "+m.Image)

		if len(stale) == 0 && s.Aborted[m.Name] != m.Image {
			out.AbortStands = false
		}
	}

	slices.Sort(lines)

	sum := sha256.Sum256([]byte(strings.Join(lines, "\n")))
	out.Change = hex.EncodeToString(sum[:8])
	out.BlockReason, out.BlockMessage = upgradeBlock(pending, stale, s, out, p)

	return out
}

// upgradeBlock returns why a due upgrade may not start, most fundamental reason first.
func upgradeBlock(pending []UpgradeMachine, stale []string, s UpgradeSettings, u Upgrade, p Pass) (string, string) {
	var problems []string

	for _, m := range pending {
		if m.Problem != "" {
			problems = append(problems, m.Name+": "+m.Problem)
		}
	}

	if len(problems) > 0 {
		return BlockIncompatible, strings.Join(problems, "; ")
	}

	// Every node is asked about before the first is touched: one that would not boot the new
	// system refuses the upgrade of all of them, and so does one nobody could ask.
	var unverified []string

	for _, m := range pending {
		if m.Unverified != "" {
			unverified = append(unverified, m.Name+": "+m.Unverified)
		}
	}

	if len(unverified) > 0 {
		return BlockUnverified, strings.Join(unverified, "; ")
	}

	if reason, message := s.Policy.block(s.Paused); reason != "" {
		return reason, message
	}

	switch {
	case len(stale) > 0:
		return BlockNotCurrent, "waiting for current status of " + strings.Join(stale, ", ")
	case u.AbortStands:
		return BlockAborted, "the upgrade to this installer was aborted; change the spec, or revert it, to continue"
	}

	if gate := RebootGate(p, pending[0].Node).And(s.Policy.Gates()); !gate.Open {
		return BlockGateClosed, gate.Reason
	}

	return "", ""
}

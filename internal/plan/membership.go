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

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
)

// Adding and removing machines (docs/scope.md §7.6, §7.7). Both are operations like the others:
// they take the one lock, go through one machine at a time, persist a step before its effect,
// and decide from what is observed. What is their own:
//
//   - a machine that is to be added is handed its config only while it waits in maintenance mode,
//     says it is the machine that was declared, and the config fits it. The config carries every
//     secret of the cluster, and a machine without a config can prove nothing about itself;
//   - a control plane leaves etcd only if at least three voting members remain. A member that is
//     confirmed dead is the exception: it holds no vote that could be lost;
//   - a machine that left is done when it is seen to have forgotten the cluster: it waits in
//     maintenance mode again. One that cannot be seen is a failure, until a human says it is dead.

// Steps of adding a machine.
const (
	// StepProvisioning hands the machine its config.
	StepProvisioning = "Provisioning"
	// StepJoining waits for the machine to install itself, boot, and become a member: a Node that
	// is Ready, and for a control plane a voting member of etcd.
	StepJoining = "Joining"
)

// Steps of removing a machine, besides StepDraining and StepVerifying.
const (
	// StepResetting asks the node to leave the cluster and forget it.
	StepResetting = "Resetting"
	// StepLeaving waits for the machine to be gone from etcd and back in maintenance mode. For a
	// machine that is confirmed dead it takes its member out of etcd from the outside.
	StepLeaving = "Leaving"
	// StepDeleting deletes the machine's Node from Kubernetes.
	StepDeleting = "Deleting"
)

// Failure reasons of adding and removing, besides the ones every step has.
const (
	// FailIdentity: the machine at the address is not the one that was declared. It was sent
	// nothing.
	FailIdentity = talosv1alpha1.ReasonIdentityMismatch
	// FailDoesNotFit: the config names a disk or a link the machine does not have. It was sent
	// nothing.
	FailDoesNotFit = talosv1alpha1.ReasonConfigDoesNotFit
	// FailNotJoined: the machine took its config and did not become a member in time.
	FailNotJoined = "NotJoined"
	// FailNodeGone: the machine that is to be removed does not answer. Nothing was taken from it,
	// and nothing can be: a human has to say whether it is dead.
	FailNodeGone = "NodeUnreachable"
	// FailNotWiped: the machine was asked to forget the cluster and has not been seen to.
	FailNotWiped = "NotWiped"
)

// JoinTimeoutFactor is how many step timeouts a machine has to install itself and join: it pulls
// an installer, writes a disk, reboots and pulls everything a node runs.
const JoinTimeoutFactor = 3

// JoinMachine is what the planner needs to know about a machine that is not a member.
type JoinMachine struct {
	Name         string
	ControlPlane bool
	// Hold excludes the machine.
	Hold bool
	// Provisionable is true when the machine was found waiting in maintenance mode, carrying the
	// pinned identity, with a config that fits it. NotProvisionable says why it is not.
	Provisionable    bool
	NotProvisionable string
	// Hostname is the hostname its config gives the machine; empty when it names itself.
	Hostname string
	// Config identifies the config rendered for it.
	Config string
}

// JoinSettings is the cluster's side of adding machines.
type JoinSettings struct {
	Policy Policy
	// NotAtRest says what the cluster does not run of what its spec asks for, or is empty.
	NotAtRest string
	// Hostnames are the names in use: of the cluster's members, and of the members of etcd.
	Hostnames []string
	// Skipped and Aborted map a machine to the config a human left it out with, or aborted its
	// adding with. Neither is handed that config; any other is.
	Skipped, Aborted map[string]string
}

// Join is the planner's answer to "are machines to be added, and may that start?". Its turns are
// the machines to add, workers first, then by name; its targets the configs they are to be
// handed. A machine on hold is not listed: it is not due before it is let go.
type Join struct {
	Due
	// Waiting lists the machines that are to be added and cannot be yet, with the reason.
	Waiting []string
}

// PlanJoin decides whether machines are to be added and whether that may start. Nothing is asked
// of a machine here; what a machine is handed is decided again, from what it says then, in its
// own turn (NextJoinStep).
func PlanJoin(machines []JoinMachine, s JoinSettings, p Pass) Join {
	out := Join{Due: Due{Targets: map[string]string{}}}

	var pending []JoinMachine

	for _, m := range machines {
		switch {
		case m.Hold:
		case m.Config != "" && (s.Skipped[m.Name] == m.Config || s.Aborted[m.Name] == m.Config):
			out.Held = append(out.Held, m.Name)
		case !m.Provisionable:
			out.Waiting = append(out.Waiting, m.Name+": "+cmp.Or(m.NotProvisionable, "not found in maintenance mode yet"))
		default:
			pending = append(pending, m)
		}
	}

	slices.Sort(out.Held)
	slices.Sort(out.Waiting)

	if len(pending) == 0 {
		return out
	}

	slices.SortFunc(pending, func(a, b JoinMachine) int {
		return cmp.Or(cmp.Compare(rank(a.ControlPlane), rank(b.ControlPlane)), strings.Compare(a.Name, b.Name))
	})

	lines := make([]string, 0, len(pending))

	for _, m := range pending {
		out.Pending = append(out.Pending, m.Name)
		out.Targets[m.Name] = m.Config
		lines = append(lines, m.Name+" "+m.Config)
	}

	var notAtRest block
	if s.NotAtRest != "" {
		notAtRest = block{BlockNotAtRest, "a machine is added to a cluster that runs what its spec asks for: " + s.NotAtRest}
	}

	out.Change = fingerprint(lines)
	out.BlockReason, out.BlockMessage = firstBlock(
		s.Policy.block(),
		notAtRest,
		gateBlock(JoinGate(p, pending[0], s.Hostnames).And(s.Policy.Gates())),
	)

	return out
}

// JoinGate is the gate before a machine is handed its config: the whole cluster is healthy, with
// nobody left out and no config pending anywhere, and the name the config gives the machine is
// nobody's. A control plane that joins under the name of an etcd member has that member removed
// by Talos itself (internal/app/machined/pkg/system/services/etcd.go:310-318 in Talos v1.14), and
// a worker under a member's name would take over its Node.
func JoinGate(p Pass, m JoinMachine, hostnames []string) Gate {
	if m.Hostname != "" && slices.Contains(hostnames, m.Hostname) {
		gate := closedGate("its config names it %s, which a member of the cluster is called already", m.Hostname)
		gate.block = BlockHostnameInUse

		return gate
	}

	return TouchGate(p, "")
}

// JoinNode is what was just observed of the machine that is being added.
type JoinNode struct {
	// Probed is true when something answered a maintenance connection at the machine's address.
	// InMaintenance, IdentityMatches and FitProblems are what it said, and mean nothing otherwise.
	Probed          bool
	InMaintenance   bool
	IdentityMatches bool
	FitProblems     []string
	// Member is true when a node answered with the cluster's credentials and the pinned identity:
	// the machine took a config of this cluster.
	Member bool
	// Running is true when that node finished booting and reports itself ready.
	Running bool
	// KubernetesKnown is false when the Kubernetes API could not be read. Registered and NodeReady
	// are what it says about the machine's Node.
	KubernetesKnown bool
	Registered      bool
	NodeReady       bool
	// ControlPlane machines also have to become voting members of etcd. EtcdKnown is false when
	// the member list was not read.
	ControlPlane bool
	EtcdKnown    bool
	EtcdVoter    bool
}

// joined reports whether the machine is known to be a member in every sense, and why not.
func (n JoinNode) joined() (bool, string) {
	switch {
	case !n.Member:
		return false, "the machine has not come up with its config yet"
	case !n.Running:
		return false, "the machine has not finished booting"
	case !n.KubernetesKnown:
		return false, "the Kubernetes API could not be read"
	case !n.Registered:
		return false, "Kubernetes has no Node for the machine yet"
	case !n.NodeReady:
		return false, "Kubernetes does not report the machine's Node Ready yet"
	case n.ControlPlane && !n.EtcdKnown:
		return false, "the members of etcd could not be read"
	case n.ControlPlane && !n.EtcdVoter:
		return false, "the machine is not a voting member of etcd yet"
	}

	return true, ""
}

// NextJoinStep decides what to do for the step of a machine that is being added.
//
// pre is the gate for handing over the config (JoinGate, and nothing pausing the operation);
// checks is the verification of the cluster once the machine is a member.
//
// The rules this function never breaks:
//   - a config is only handed to a machine that, in this pass, answered in maintenance mode, said
//     it is the machine that was declared, and that the config fits;
//   - a machine that says it is another one is a failure at once, and is sent nothing;
//   - a machine that already is a member is never handed a config again.
func NextJoinStep(step Step, node JoinNode, pre, checks Gate, t Timing, now time.Time) Action {
	switch step.Name {
	case StepProvisioning:
		return nextProvisioning(step, node, pre, t, now)
	case StepJoining:
		joined, missing := node.joined()

		switch {
		case joined:
			return advance(StepVerifying)
		case !now.Before(step.Since.Add(JoinTimeoutFactor * t.StepTimeout)):
			return fail(FailNotJoined, "not a member within %s: %s", JoinTimeoutFactor*t.StepTimeout, missing)
		default:
			return wait("%s", missing)
		}
	case StepVerifying:
		joined, missing := node.joined()

		return step.settle(joined && checks.Open, cmp.Or(missing, checks.Reason), t, now)
	default:
		return unknownStep(step.Name)
	}
}

func nextProvisioning(step Step, node JoinNode, pre Gate, t Timing, now time.Time) Action {
	switch {
	case node.Member:
		// It has its config: the request went out and its answer, or the step after it, was lost.
		return advance(StepJoining)
	case node.Probed && node.InMaintenance && !node.IdentityMatches:
		return fail(FailIdentity, "the machine that answers at the address is not the one that was declared; it was sent nothing")
	case node.Probed && node.InMaintenance && len(node.FitProblems) > 0:
		return fail(FailDoesNotFit, "%s; the machine was sent nothing", strings.Join(node.FitProblems, "; "))
	}

	away := "what answers at the machine's address is not waiting in maintenance mode"
	if !node.Probed {
		away = "nothing answers in maintenance mode at the machine's address"
	}

	return step.do(Provision, "handed its config", node.Probed && node.InMaintenance, away, pre, t, now)
}

// RemoveMachine is what the planner needs to know about a machine that is to be removed.
type RemoveMachine struct {
	Name string
	// Node is the hostname the machine's node is known under; Reachable whether it answered in
	// this pass.
	Node         string
	Reachable    bool
	ControlPlane bool
	// HostsOperator is true for the node this operator runs on, which goes last.
	HostsOperator bool
}

// RemovalSettings is the cluster's side of removing machines.
type RemovalSettings struct {
	Policy Policy
	// Skipped and Aborted name the machines whose removal a human left out or aborted. They stay
	// until their spec asks for them to be removed anew.
	Skipped, Aborted map[string]string
}

// Removal is the planner's answer to "are machines to be removed, and may that start?". Its turns
// are the machines to remove: workers first, the node the operator runs on last, then by name.
type Removal struct {
	Due
}

// PlanRemoval decides whether machines are to be removed and whether that may start.
func PlanRemoval(machines []RemoveMachine, s RemovalSettings, p Pass) Removal {
	var (
		out     Removal
		pending []RemoveMachine
	)

	for _, m := range machines {
		_, skipped := s.Skipped[m.Name]
		_, aborted := s.Aborted[m.Name]

		if skipped || aborted {
			out.Held = append(out.Held, m.Name)

			continue
		}

		pending = append(pending, m)
	}

	slices.Sort(out.Held)

	if len(pending) == 0 {
		return out
	}

	slices.SortFunc(pending, func(a, b RemoveMachine) int {
		return cmp.Or(
			cmp.Compare(rank(a.ControlPlane), rank(b.ControlPlane)),
			cmp.Compare(rank(a.HostsOperator), rank(b.HostsOperator)),
			strings.Compare(a.Name, b.Name),
		)
	})

	for _, m := range pending {
		out.Pending = append(out.Pending, m.Name)
	}

	out.Change = fingerprint(out.Pending)
	blocks := []block{s.Policy.block()}

	// A machine that does not answer cannot be judged, and neither can the cluster around it: a
	// pass that could not read a member is not complete. Its removal begins all the same, takes
	// nothing from the cluster, and halts once the machine has not answered for long enough
	// (NextRemovalStep): what happens to a machine that is gone is for a human to say, and that
	// is where they are asked.
	if pending[0].Reachable {
		blocks = append(blocks, gateBlock(RemovalGate(p, pending[0].Node, false).And(s.Policy.Gates())))
	}

	out.BlockReason, out.BlockMessage = firstBlock(blocks...)

	return out
}

// RemovalGate is the gate before a node is taken out of the cluster: before it is cordoned, and
// again before it is reset or its etcd member removed. It is TouchGate, and for a member of etcd
// also the rule of invariant 3 (docs/scope.md §8.1):
//
//   - the only voting member is never taken out;
//   - a member is only taken out if at least three voting members remain. With two left, losing
//     one more loses the cluster, and what stood between that and now was this member's vote;
//   - a member that is dead has no vote that could be lost: taking it out leaves the cluster no
//     worse than it is, and makes room for its replacement. That is only said by a human.
//
// node is the hostname of the node, and dead whether a human confirmed that it is gone. The pass
// of a dead node's removal is one that leaves the node out (it cannot be read, by definition).
func RemovalGate(p Pass, node string, dead bool) Gate {
	gate := TouchGate(p, node)
	if dead {
		gate = withoutTheDead(p)
	}

	if !gate.Open {
		return gate
	}

	voters, member := 0, false

	for _, m := range p.Cluster.EtcdMembers {
		if m.Learner {
			continue
		}

		voters++

		if node != "" && m.Hostname == node {
			member = true
		}
	}

	switch {
	case !member:
		return gate
	case voters == 1:
		quorum := closedGate("%s is the only voting member of etcd: taking it out would end the cluster", node)
		quorum.block = BlockLastControlPlane

		return quorum
	case !dead && voters-1 < 3:
		quorum := closedGate("taking %s out would leave etcd with %d voting members, and with fewer than three the loss of one more "+
			"is the loss of the cluster: add its replacement first", node, voters-1)
		quorum.block = BlockQuorumAtRisk

		return quorum
	}

	return gate
}

// withoutTheDead is the gate before what is left of a dead machine is taken out of the cluster:
// every member that is there is healthy by its own facts, by Kubernetes and by etcd. Talos's own
// health checks are not asked. They count the Nodes Kubernetes has, the dead machine's among
// them, and cannot pass until the removal they would be asked about is done.
func withoutTheDead(p Pass) Gate {
	if gate := observed(p); !gate.Open {
		return gate
	}

	if gate := ownFacts(*p.Cluster, "", true); !gate.Open {
		return gate
	}

	return Gate{Open: true, Note: "the machine is dead, so Talos's cluster-wide health checks were not asked: " +
		"every member that is left is healthy by its own report, by Kubernetes and by etcd"}
}

// RemovalNode is what was just observed of the machine that is being removed.
type RemovalNode struct {
	// Reachable is true when the node's Talos API answered with the pinned identity: it still is
	// a member of the cluster.
	Reachable bool
	// Wiped is true when a machine answered a maintenance connection at the address, waiting in
	// maintenance mode with the pinned identity: it has forgotten the cluster.
	Wiped bool
	// ControlPlane machines also have to be gone from etcd. EtcdKnown is false when the member
	// list was not read; EtcdMember is whether the machine's member is still in it.
	ControlPlane bool
	EtcdKnown    bool
	EtcdMember   bool
	// KubernetesKnown is false when the Kubernetes API could not be read. Registered is whether
	// the machine has a Node there; the rest is about that Node.
	KubernetesKnown bool
	Registered      bool
	Unschedulable   bool
	// PodsLeft is how many pods still have to leave the node; negative when not counted.
	PodsLeft int
}

// NextRemovalStep decides what to do for the step of a machine that is being removed.
//
// pre is RemovalGate and nothing pausing the operation; checks is the verification of the
// cluster that is left.
//
// The rules this function never breaks:
//   - a node is not cordoned, not reset, and its etcd member not removed, while the gate is closed;
//   - a node is only reset while it answers as a member; one that is back in maintenance mode is
//     never reset again;
//   - an etcd member is only removed from the outside for a machine a human confirmed dead;
//   - a machine's Node is only deleted once the machine is seen to have forgotten the cluster, or
//     is confirmed dead.
func NextRemovalStep(step Step, node RemovalNode, pre, checks Gate, t Timing, now time.Time) Action {
	switch step.Name {
	case StepDraining:
		return nextRemovalDraining(step, node, pre, t, now)
	case StepResetting:
		return nextResetting(step, node, pre, t, now)
	case StepLeaving:
		return nextLeaving(step, node, pre, t, now)
	case StepDeleting:
		switch {
		case node.KubernetesKnown && !node.Registered:
			return advance(StepVerifying)
		case step.timedOut(t, now):
			return fail(FailTimeout, "the machine's Node was not deleted within %s", t.StepTimeout)
		case !node.KubernetesKnown:
			return wait("the Kubernetes API could not be read")
		default:
			return Action{Kind: DeleteNode}
		}
	case StepVerifying:
		return step.settle(checks.Open, checks.Reason, t, now)
	default:
		return unknownStep(step.Name)
	}
}

func nextRemovalDraining(step Step, node RemovalNode, pre Gate, t Timing, now time.Time) Action {
	cordoned := node.KubernetesKnown && node.Registered && node.Unschedulable

	switch {
	case node.Wiped:
		// It has left already; what is left is to see it gone and to clean up after it.
		return advance(StepLeaving)
	case !node.Reachable && !cordoned && step.timedOut(t, now):
		return fail(FailNodeGone, "the machine has not answered for %s; nothing was taken from the cluster", t.StepTimeout)
	case !node.Reachable && !cordoned:
		// A node that cannot be reset afterwards is not drained first.
		return wait(notReachable)
	case node.KubernetesKnown && !node.Registered:
		// Nothing runs on a machine Kubernetes does not know.
		return advance(StepResetting)
	}

	if action, draining := step.drain(node.KubernetesKnown, cordoned, node.PodsLeft, pre, t, now); draining {
		return action
	}

	return advance(StepResetting)
}

func nextResetting(step Step, node RemovalNode, pre Gate, t Timing, now time.Time) Action {
	switch {
	case node.Wiped:
		// The reset happened: the request went out and its answer, or the step after it, was lost.
		return advance(StepLeaving)
	case step.timedOut(t, now) && !node.Reachable:
		return fail(FailNodeGone, "the machine stopped answering before it could be reset, and has not for %s", t.StepTimeout)
	}

	return step.do(Reset, "reset", node.Reachable, notReachable, pre, t, now)
}

func nextLeaving(step Step, node RemovalNode, pre Gate, t Timing, now time.Time) Action {
	inEtcd := node.ControlPlane && node.EtcdMember

	switch {
	case node.ControlPlane && !node.EtcdKnown && step.timedOut(t, now):
		return fail(FailTimeout, "the members of etcd could not be read for %s", t.StepTimeout)
	case node.ControlPlane && !node.EtcdKnown:
		return wait("the members of etcd could not be read")
	case step.Dead && inEtcd && step.timedOut(t, now):
		return fail(FailTimeout, "the machine's member was not taken out of etcd within %s: %s", t.StepTimeout, pre.Reason)
	case step.Dead && inEtcd && !pre.Open:
		return wait("%s", pre.Reason)
	case step.Dead && inEtcd:
		return Action{Kind: RemoveMember}
	case step.Dead:
		return advance(StepDeleting)
	case node.Wiped && !inEtcd:
		return advance(StepDeleting)
	case step.timedOut(t, now) && !node.Wiped:
		return fail(FailNotWiped, "the machine was asked to leave and has not come back in maintenance mode within %s: "+
			"whether it still holds the cluster's secrets cannot be told", t.StepTimeout)
	case step.timedOut(t, now):
		return fail(FailTimeout, "the machine left, and its member is still in etcd after %s", t.StepTimeout)
	case !node.Wiped:
		return wait("waiting for the machine to come back in maintenance mode")
	default:
		return wait("waiting for the machine's member to be gone from etcd")
	}
}

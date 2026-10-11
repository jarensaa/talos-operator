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
	"strings"
	"time"
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

// Failure reasons of adding and removing, besides FailTimeout and FailDrainTimeout.
const (
	// FailIdentity: the machine at the address is not the one that was declared. It was sent
	// nothing.
	FailIdentity = "IdentityMismatch"
	// FailDoesNotFit: the config names a disk or a link the machine does not have. It was sent
	// nothing.
	FailDoesNotFit = "ConfigDoesNotFit"
	// FailNotJoined: the machine took its config and did not become a member in time.
	FailNotJoined = "NotJoined"
	// FailNodeGone: the machine that is to be removed does not answer. Nothing was taken from it,
	// and nothing can be: a human has to say whether it is dead.
	FailNodeGone = "NodeUnreachable"
	// FailNotWiped: the machine was asked to forget the cluster and has not been seen to.
	FailNotWiped = "NotWiped"
)

// Reasons a machine may not be added or removed, besides the ones every operation has.
const (
	// BlockNotAtRest: the cluster does not run what its spec asks for, so a machine that is added
	// would join on something else than its peers run.
	BlockNotAtRest = "NotAtRest"
	// BlockHostnameInUse: the config gives the machine the hostname of a member. Talos removes
	// the etcd member of that name when a control plane joins.
	BlockHostnameInUse = "HostnameInUse"
	// BlockQuorumAtRisk: taking the control plane out would leave etcd with fewer than three
	// voting members.
	BlockQuorumAtRisk = "QuorumAtRisk"
	// BlockLastControlPlane: the machine is the cluster's only control plane.
	BlockLastControlPlane = "LastControlPlane"
)

// Actions of adding and removing, in addition to the ones the other steps use.
const (
	// Provision: hand the machine in maintenance mode its config.
	Provision ActionKind = iota + 200
	// Reset: ask the node to leave the cluster and forget it.
	Reset
	// RemoveMember: take the dead machine's member out of etcd through another control plane.
	RemoveMember
	// DeleteNode: delete the machine's Node from Kubernetes.
	DeleteNode
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
	Paused bool
	Policy Policy
	// NotAtRest says what the cluster does not run of what its spec asks for, or is empty.
	NotAtRest string
	// Hostnames are the names in use: of the cluster's members, and of the members of etcd.
	Hostnames []string
	// Held maps a machine to the config a human left it out with, or aborted its adding with.
	Held map[string]string
}

// Join is the planner's answer to "are machines to be added, and may that start?".
type Join struct {
	// Pending lists the machines to add, workers first, then by name.
	Pending []string
	// Configs maps each pending machine to the config it is to be handed.
	Configs map[string]string
	// Waiting lists the machines that are to be added and cannot be yet, with the reason.
	Waiting []string
	// Held lists the machines a human left out with the config they would be handed.
	Held []string
	// Change identifies exactly this addition.
	Change string
	// BlockReason and BlockMessage are set when Pending is not empty but adding may not start.
	BlockReason  string
	BlockMessage string
}

// MayStart reports whether machines are to be added and nothing blocks it.
func (j Join) MayStart() bool { return len(j.Pending) > 0 && j.BlockReason == "" }

// PlanJoin decides whether machines are to be added and whether that may start. Nothing is asked
// of a machine here; what a machine is handed is decided again, from what it says then, in its
// own turn (NextJoinStep).
func PlanJoin(machines []JoinMachine, s JoinSettings, p Pass) Join {
	out := Join{Configs: map[string]string{}}

	var pending []JoinMachine

	for _, m := range machines {
		switch {
		case m.Hold:
		case m.Config != "" && s.Held[m.Name] == m.Config:
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
		out.Configs[m.Name] = m.Config
		lines = append(lines, m.Name+" "+m.Config)
	}

	sum := sha256.Sum256([]byte(strings.Join(lines, "\n")))
	out.Change = hex.EncodeToString(sum[:8])
	out.BlockReason, out.BlockMessage = joinBlock(pending, s, p)

	return out
}

func joinBlock(pending []JoinMachine, s JoinSettings, p Pass) (string, string) {
	if reason, message := s.Policy.block(s.Paused); reason != "" {
		return reason, message
	}

	if s.NotAtRest != "" {
		return BlockNotAtRest, "a machine is added to a cluster that runs what its spec asks for: " + s.NotAtRest
	}

	if gate := JoinGate(p, pending[0], s.Hostnames).And(s.Policy.Gates()); !gate.Open {
		return cmp.Or(gate.block, BlockGateClosed), gate.Reason
	}

	return "", ""
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

// JoinStep is the persisted state of the step of a machine that is being added.
type JoinStep struct {
	Name string
	// Since is when this step began.
	Since time.Time
	// HealthySince is when verification first passed without interruption; zero if it does not.
	HealthySince time.Time
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
func NextJoinStep(step JoinStep, node JoinNode, pre, checks Gate, t Timing, now time.Time) Action {
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

		return settle(joined && checks.Open, cmp.Or(missing, checks.Reason), step.Since, step.HealthySince, t, now)
	default:
		return fail(FailTimeout, "unknown step %q", step.Name)
	}
}

func nextProvisioning(step JoinStep, node JoinNode, pre Gate, t Timing, now time.Time) Action {
	switch {
	case node.Member:
		// It has its config: the request went out and its answer, or the step after it, was lost.
		return advance(StepJoining)
	case node.Probed && node.InMaintenance && !node.IdentityMatches:
		return fail(FailIdentity, "the machine that answers at the address is not the one that was declared; it was sent nothing")
	case node.Probed && node.InMaintenance && len(node.FitProblems) > 0:
		return fail(FailDoesNotFit, "%s; the machine was sent nothing", strings.Join(node.FitProblems, "; "))
	case timedOut(Step{Since: step.Since}, t, now):
		return fail(FailTimeout, "not handed its config within %s: %s", t.StepTimeout, cmp.Or(pre.Reason, notInMaintenance(node)))
	case !node.Probed || !node.InMaintenance:
		return wait("%s", notInMaintenance(node))
	case !pre.Open:
		return wait("%s", pre.Reason)
	default:
		return Action{Kind: Provision}
	}
}

func notInMaintenance(node JoinNode) string {
	if !node.Probed {
		return "nothing answers in maintenance mode at the machine's address"
	}

	return "what answers at the machine's address is not waiting in maintenance mode"
}

// settle is the soak at the end of a turn: healthy for t.Soak without interruption, within the
// step's timeout.
func settle(healthy bool, reason string, since, healthySince time.Time, t Timing, now time.Time) Action {
	switch {
	case healthy && !healthySince.IsZero() && now.Sub(healthySince) >= t.Soak:
		return Action{Kind: Complete}
	case timedOut(Step{Since: since}, t, now):
		return fail(FailTimeout, "not verified within %s: %s", t.StepTimeout, reason)
	case !healthy && !healthySince.IsZero():
		return Action{Kind: MarkUnhealthy, Message: reason}
	case !healthy:
		return wait("%s", reason)
	case healthySince.IsZero():
		return Action{Kind: MarkHealthy}
	default:
		return soaking(Step{HealthySince: healthySince}, t)
	}
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
	Paused bool
	Policy Policy
	// Held lists the machines whose removal a human aborted or left out. They stay until their
	// spec asks for them to be removed anew.
	Held []string
}

// Removal is the planner's answer to "are machines to be removed, and may that start?".
type Removal struct {
	// Pending lists the machines to remove: workers first, the node the operator runs on last,
	// then by name.
	Pending []string
	Held    []string
	// Change identifies exactly this removal.
	Change string
	// BlockReason and BlockMessage are set when Pending is not empty but removing may not start.
	BlockReason  string
	BlockMessage string
}

// MayStart reports whether machines are to be removed and nothing blocks it.
func (r Removal) MayStart() bool { return len(r.Pending) > 0 && r.BlockReason == "" }

// PlanRemoval decides whether machines are to be removed and whether that may start.
func PlanRemoval(machines []RemoveMachine, s RemovalSettings, p Pass) Removal {
	var (
		out     Removal
		pending []RemoveMachine
	)

	for _, m := range machines {
		if slices.Contains(s.Held, m.Name) {
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

	sum := sha256.Sum256([]byte(strings.Join(out.Pending, "\n")))
	out.Change = hex.EncodeToString(sum[:8])

	if reason, message := s.Policy.block(s.Paused); reason != "" {
		out.BlockReason, out.BlockMessage = reason, message

		return out
	}

	// A machine that does not answer cannot be judged, and neither can the cluster around it: a
	// pass that could not read a member is not complete. Its removal begins all the same, takes
	// nothing from the cluster, and halts once the machine has not answered for long enough
	// (NextRemovalStep): what happens to a machine that is gone is for a human to say, and that
	// is where they are asked.
	if !pending[0].Reachable {
		return out
	}

	if gate := RemovalGate(p, pending[0].Node, false).And(s.Policy.Gates()); !gate.Open {
		out.BlockReason, out.BlockMessage = cmp.Or(gate.block, BlockGateClosed), gate.Reason
	}

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

	c := *p.Cluster

	gate := holds(
		checkAllMembersRunning(c, ""),
		checkAllNodesReady(c, ""),
		checkEtcdHealthy(c, ""),
		checkEtcdMatchesControlPlanes(c),
		checkNoPendingConfig(c),
	)
	if !gate.Open {
		return gate
	}

	return Gate{Open: true, Note: "the machine is dead, so Talos's cluster-wide health checks were not asked: " +
		"every member that is left is healthy by its own report, by Kubernetes and by etcd"}
}

// RemovalStep is the persisted state of the step of a machine that is being removed.
type RemovalStep struct {
	Name string
	// Since is when this step began.
	Since time.Time
	// Cordoned is true when the operator decided to cordon the node, which it records first.
	Cordoned bool
	// Dead is true when a human confirmed that the machine is gone for good.
	Dead bool
	// HealthySince is when verification first passed without interruption; zero if it does not.
	HealthySince time.Time
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
func NextRemovalStep(step RemovalStep, node RemovalNode, pre, checks Gate, t Timing, now time.Time) Action {
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
		case timedOut(Step{Since: step.Since}, t, now):
			return fail(FailTimeout, "the machine's Node was not deleted within %s", t.StepTimeout)
		case !node.KubernetesKnown:
			return wait("the Kubernetes API could not be read")
		default:
			return Action{Kind: DeleteNode}
		}
	case StepVerifying:
		return settle(checks.Open, checks.Reason, step.Since, step.HealthySince, t, now)
	default:
		return fail(FailTimeout, "unknown step %q", step.Name)
	}
}

func nextRemovalDraining(step RemovalStep, node RemovalNode, pre Gate, t Timing, now time.Time) Action {
	cordoned := node.KubernetesKnown && node.Registered && node.Unschedulable

	switch {
	case node.Wiped:
		// It has left already; what is left is to see it gone and to clean up after it.
		return advance(StepLeaving)
	case !node.Reachable && !cordoned && timedOut(Step{Since: step.Since}, t, now):
		return fail(FailNodeGone, "the machine has not answered for %s; nothing was taken from the cluster", t.StepTimeout)
	case !node.Reachable && !cordoned:
		// A node that cannot be reset afterwards is not drained first.
		return wait(notReachable)
	case !now.Before(step.Since.Add(t.DrainTimeout)):
		return fail(FailDrainTimeout, "not drained within %s: %s", t.DrainTimeout, removalDrainWait(node, pre, cordoned))
	case !node.KubernetesKnown:
		return wait("the Kubernetes API could not be read")
	case !node.Registered:
		// Nothing runs on a machine Kubernetes does not know.
		return advance(StepResetting)
	case !cordoned && !pre.Open:
		return wait("%s", pre.Reason)
	case !cordoned && !step.Cordoned:
		return Action{Kind: MarkCordoned}
	case !cordoned:
		return Action{Kind: Cordon}
	case node.PodsLeft < 0:
		return wait("the node's pods could not be listed")
	case node.PodsLeft > 0:
		return Action{Kind: Evict, Message: removalDrainWait(node, pre, cordoned)}
	default:
		return advance(StepResetting)
	}
}

func removalDrainWait(node RemovalNode, pre Gate, cordoned bool) string {
	switch {
	case !node.KubernetesKnown:
		return "the Kubernetes API could not be read"
	case !cordoned:
		return cmp.Or(pre.Reason, "the node is not cordoned yet")
	case node.PodsLeft < 0:
		return "the node's pods could not be listed"
	default:
		return pods(node.PodsLeft) + " still on the node"
	}
}

func nextResetting(step RemovalStep, node RemovalNode, pre Gate, t Timing, now time.Time) Action {
	switch {
	case node.Wiped:
		// The reset happened: the request went out and its answer, or the step after it, was lost.
		return advance(StepLeaving)
	case timedOut(Step{Since: step.Since}, t, now) && !node.Reachable:
		return fail(FailNodeGone, "the machine stopped answering before it could be reset, and has not for %s", t.StepTimeout)
	case timedOut(Step{Since: step.Since}, t, now):
		return fail(FailTimeout, "not reset within %s: %s", t.StepTimeout, pre.Reason)
	case !node.Reachable:
		return wait(notReachable)
	case !pre.Open:
		return wait("%s", pre.Reason)
	default:
		return Action{Kind: Reset}
	}
}

func nextLeaving(step RemovalStep, node RemovalNode, pre Gate, t Timing, now time.Time) Action {
	inEtcd := node.ControlPlane && node.EtcdMember

	switch {
	case node.ControlPlane && !node.EtcdKnown && timedOut(Step{Since: step.Since}, t, now):
		return fail(FailTimeout, "the members of etcd could not be read for %s", t.StepTimeout)
	case node.ControlPlane && !node.EtcdKnown:
		return wait("the members of etcd could not be read")
	case step.Dead && inEtcd && timedOut(Step{Since: step.Since}, t, now):
		return fail(FailTimeout, "the machine's member was not taken out of etcd within %s: %s", t.StepTimeout, pre.Reason)
	case step.Dead && inEtcd && !pre.Open:
		return wait("%s", pre.Reason)
	case step.Dead && inEtcd:
		return Action{Kind: RemoveMember}
	case step.Dead:
		return advance(StepDeleting)
	case node.Wiped && !inEtcd:
		return advance(StepDeleting)
	case timedOut(Step{Since: step.Since}, t, now) && !node.Wiped:
		return fail(FailNotWiped, "the machine was asked to leave and has not come back in maintenance mode within %s: "+
			"whether it still holds the cluster's secrets cannot be told", t.StepTimeout)
	case timedOut(Step{Since: step.Since}, t, now):
		return fail(FailTimeout, "the machine left, and its member is still in etcd after %s", t.StepTimeout)
	case !node.Wiped:
		return wait("waiting for the machine to come back in maintenance mode")
	default:
		return wait("waiting for the machine's member to be gone from etcd")
	}
}

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

package controller

import (
	"context"
	"fmt"
	"maps"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/plan"
	"github.com/jarensaa/talos-operator/internal/talos"
	"github.com/jarensaa/talos-operator/internal/target"
)

// Adding and removing machines (docs/scope.md §7.6, §7.7). The rules are the planners'
// (internal/plan/membership.go); this file observes, asks them, and does the one thing they
// answer, as every kind of operation does (operation.go).
//
// What is different here is who is talked to. A machine that is to be added has no part in the
// cluster yet: it is asked, and handed its config, over a maintenance connection that carries
// none of the cluster's credentials (talos.Maintenance). A machine that leaves is reset through
// the cluster like any other node is reached, by a client that can do that and little else
// (talos.Remover). Both are only made while spec.management.membership is Enabled.

// +kubebuilder:rbac:groups="",resources=nodes,verbs=delete

// hostnamesInUse are the names a machine that is added must not be given: those of the live
// members, of the members of etcd, and of the Nodes the cluster's machines are known under.
func hostnamesInUse(f *fleet, except string) []string {
	var names []string

	if f.facts != nil {
		for _, n := range f.facts.nodes {
			names = append(names, n.hostname)
		}

		for _, m := range f.facts.etcdMembers {
			names = append(names, m.Hostname)
		}
	}

	for i := range f.all {
		if id := f.all[i].Status.Identity; id != nil && f.all[i].Name != except && f.all[i].IsMember() {
			names = append(names, id.Hostname, id.KubernetesNodeName)
		}
	}

	slices.Sort(names)

	return slices.DeleteFunc(slices.Compact(names), func(name string) bool { return name == "" })
}

// notAtRest says what the cluster does not run of what its spec asks for. A machine is added to
// a cluster that is where its spec wants it: otherwise it would join on versions, or a config,
// its peers do not run, and the upgrade that is due would begin with a node that needs none.
func notAtRest(tc *talosv1alpha1.TalosCluster) string {
	for _, kind := range []string{
		talosv1alpha1.ConditionConfigInSync, talosv1alpha1.ConditionTalosUpToDate, talosv1alpha1.ConditionKubernetesUpToDate,
	} {
		cond := meta.FindStatusCondition(tc.Status.Conditions, kind)

		switch {
		case cond == nil && kind == talosv1alpha1.ConditionConfigInSync:
			return kind + " is not known yet"
		case cond == nil, cond.Status == metav1.ConditionTrue, cond.Reason == talosv1alpha1.ReasonHeld:
		default:
			return kind + " is " + string(cond.Status) + " (" + cond.Reason + ")"
		}
	}

	return ""
}

// renderedHostname is the hostname the machine's rendered config gives it; empty when the config
// leaves the machine to name itself, or does not render.
func renderedHostname(v machineView) (string, error) {
	if v.rendered == nil {
		return "", nil
	}

	return talos.RenderedHostname(v.rendered.Bytes)
}

// joinMachine is the planner's view of a machine that is to be added.
func joinMachine(v machineView) plan.JoinMachine {
	m := plan.JoinMachine{Name: v.tm.Name, ControlPlane: v.controlPlane(), Hold: v.tm.Spec.Hold}

	hostname, err := renderedHostname(v)

	switch {
	case v.rendered == nil:
		m.NotProvisionable = v.problem

		return m
	case err != nil:
		m.NotProvisionable = err.Error()

		return m
	}

	m.Hostname, m.Config = hostname, v.rendered.Hash

	// What the machine's own controller found when it asked the machine. It is asked again, and
	// decided on again, in the machine's turn; this only says whether a turn is worth beginning.
	cond := meta.FindStatusCondition(v.tm.Status.Conditions, talosv1alpha1.ConditionProvisionable)

	switch {
	case cond == nil || v.tm.Status.ObservedGeneration != v.tm.Generation:
	case cond.Status == metav1.ConditionTrue:
		m.Provisionable = true
	default:
		m.NotProvisionable = cond.Message
	}

	return m
}

// toAdd lists the machines that are declared and are no members: neither recorded as one, nor
// answering as one right now.
func toAdd(f *fleet) []*talosv1alpha1.TalosMachine {
	var out []*talosv1alpha1.TalosMachine

	for i := range f.all {
		tm := &f.all[i]
		if tm.Spec.DesiredState != talosv1alpha1.DesiredStateRemoved && !tm.IsMember() && f.of(tm).node == nil {
			out = append(out, tm)
		}
	}

	return out
}

// toRemove lists the machines that are asked to leave and have not: recorded as members, or
// answering as one right now, as a machine does that was removed as dead and came back.
func toRemove(f *fleet) []*talosv1alpha1.TalosMachine {
	var out []*talosv1alpha1.TalosMachine

	for i := range f.all {
		tm := &f.all[i]
		if tm.Spec.DesiredState == talosv1alpha1.DesiredStateRemoved && (tm.IsMember() || f.of(tm).node != nil) {
			out = append(out, tm)
		}
	}

	return out
}

// planRemoval asks the planner whether machines are to be removed, and starts that by taking the
// lock. It reports whether it did, and, when a removal is due but may not start, why.
func (r *TalosClusterReconciler) planRemoval(tc *talosv1alpha1.TalosCluster, obs observation, f *fleet) (bool, *metav1.Condition) {
	// What a human left out, or ended, stands until the spec asks for the machine to be removed
	// anew: desiredState back to Present, and to Removed again.
	hold(tc, talosv1alpha1.OperationRemoveMachine, func(h *talosv1alpha1.Holds) {
		spent := func(name, _ string) bool {
			tm := f.declared(name)

			return tm == nil || tm.Spec.DesiredState != talosv1alpha1.DesiredStateRemoved
		}

		maps.DeleteFunc(h.Skipped, spent)
		maps.DeleteFunc(h.Aborted, spent)
	})

	leaving := toRemove(f)
	if len(leaving) == 0 || !tc.Spec.Management.MayChangeMembership() {
		return false, nil
	}

	planned := make([]plan.RemoveMachine, 0, len(leaving))

	for _, tm := range leaving {
		m := f.of(tm)
		planned = append(planned, plan.RemoveMachine{
			Name:          tm.Name,
			Node:          m.knownAs(),
			Reachable:     m.node != nil,
			ControlPlane:  m.controlPlane(),
			HostsOperator: r.NodeName != "" && m.kubernetesNode() == r.NodeName,
		})
	}

	holds := held(tc, talosv1alpha1.OperationRemoveMachine)
	result := plan.PlanRemoval(planned, plan.RemovalSettings{Policy: obs.policy, Skipped: holds.Skipped, Aborted: holds.Aborted}, passOf(obs))

	if r.start(tc, talosv1alpha1.OperationRemoveMachine, result.Due) != nil {
		return true, nil
	}

	// Said whatever it waits for, a machine's status included: nothing else says that a machine
	// that is to go is still there.
	if len(result.Pending) == 0 {
		return false, nil
	}

	return false, &metav1.Condition{
		Type: talosv1alpha1.ConditionRolloutBlocked, Status: metav1.ConditionTrue, Reason: result.BlockReason,
		Message: fmt.Sprintf("the removal of %s does not start: %s", strings.Join(result.Pending, ", "), result.BlockMessage),
	}
}

// planJoin asks the planner whether machines are to be added, and starts that by taking the lock.
// It runs last: a machine is only added while nothing else is due (notAtRest).
func (r *TalosClusterReconciler) planJoin(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation, f *fleet) error {
	st := &tc.Status

	waiting := toAdd(f)
	planned := make([]plan.JoinMachine, 0, len(waiting))

	if tc.Spec.Management.MayChangeMembership() {
		for _, tm := range waiting {
			v, err := f.view(ctx, tm)
			if err != nil {
				return err
			}

			planned = append(planned, joinMachine(v))
		}
	}

	// What a human left out, or ended, covers one config of one machine. It is spent once the
	// machine is gone, is a member, or is to be handed another config.
	hold(tc, talosv1alpha1.OperationAddMachine, func(h *talosv1alpha1.Holds) {
		spent := func(name, config string) bool {
			tm := f.declared(name)
			if tm == nil || tm.IsMember() {
				return true
			}

			at := slices.IndexFunc(planned, func(m plan.JoinMachine) bool { return m.Name == name })

			return at >= 0 && planned[at].Config != "" && planned[at].Config != config
		}

		maps.DeleteFunc(h.Skipped, spent)
		maps.DeleteFunc(h.Aborted, spent)
	})

	if len(planned) == 0 {
		return nil
	}

	holds := held(tc, talosv1alpha1.OperationAddMachine)
	result := plan.PlanJoin(planned, plan.JoinSettings{
		Policy: obs.policy, NotAtRest: notAtRest(tc), Hostnames: hostnamesInUse(f, ""), Skipped: holds.Skipped, Aborted: holds.Aborted,
	}, passOf(obs))

	if r.start(tc, talosv1alpha1.OperationAddMachine, result.Due) != nil {
		return nil
	}

	// A machine that is to be added and may not be is said so, unless something else already
	// says what holds the cluster up.
	if len(result.Pending) > 0 && meta.FindStatusCondition(st.Conditions, talosv1alpha1.ConditionRolloutBlocked) == nil {
		setCondition(st, tc.Generation, talosv1alpha1.ConditionRolloutBlocked, metav1.ConditionTrue, result.BlockReason,
			fmt.Sprintf("%s are not added: %s", strings.Join(result.Pending, ", "), result.BlockMessage))
	}

	return nil
}

// reportMembership sets MembershipCurrent while no machine is being added or removed.
func reportMembership(tc *talosv1alpha1.TalosCluster, f *fleet) {
	st, gen := &tc.Status, tc.Generation
	current := func(status metav1.ConditionStatus, reason, format string, args ...any) {
		setCondition(st, gen, talosv1alpha1.ConditionMembershipCurrent, status, reason, fmt.Sprintf(format, args...))
	}

	names := func(machines []*talosv1alpha1.TalosMachine) string {
		out := make([]string, 0, len(machines))
		for _, tm := range machines {
			out = append(out, tm.Name)
		}

		return strings.Join(out, ", ")
	}

	if op := st.Operation; op != nil && (op.Type == talosv1alpha1.OperationAddMachine || op.Type == talosv1alpha1.OperationRemoveMachine) {
		return
	}

	leaving, waiting := toRemove(f), toAdd(f)

	switch {
	case f.facts == nil:
	case len(leaving)+len(waiting) == 0:
		current(metav1.ConditionTrue, talosv1alpha1.ReasonSucceeded,
			"all %d machines that are declared are members, and no member is to be removed", len(f.members))
	case !tc.Spec.Management.MayChangeMembership():
		current(metav1.ConditionFalse, talosv1alpha1.ReasonMembershipDisabled,
			"%s are not members and %s are to be removed; set spec.management.membership to Enabled to add and remove machines",
			orNone(names(waiting)), orNone(names(leaving)))
	case len(leaving) > 0:
		current(metav1.ConditionFalse, talosv1alpha1.ReasonRemoving, "%s are to be removed", names(leaving))
	default:
		current(metav1.ConditionFalse, talosv1alpha1.ReasonAwaitingMachine,
			"%s are declared and are not members yet; each machine's Provisionable condition says what it waits for", names(waiting))
	}
}

func orNone(s string) string {
	if s == "" {
		return "none"
	}

	return s
}

// joinTurns is what the turns of adding machines do.
func (r *TalosClusterReconciler) joinTurns(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation, f *fleet,
	timing plan.Timing,
) turns {
	op := tc.Status.Operation

	return turns{
		begin: func(name string, attempt int32) error {
			tm := f.declared(name)
			if tm == nil || tm.Spec.Hold || tm.Spec.DesiredState == talosv1alpha1.DesiredStateRemoved {
				skipped(op, name)

				return nil
			}

			v, err := f.view(ctx, tm)
			if err != nil {
				return err
			}

			// A machine is handed exactly the config that was planned for it.
			if v.rendered == nil || v.rendered.Hash != op.Targets[name] {
				r.end(tc, talosv1alpha1.OperationSuperseded, name+" is to be handed another config than this operation planned")

				return nil
			}

			op.Current = &talosv1alpha1.OperationStep{
				Machine: name, Step: plan.StepProvisioning, DesiredHash: v.rendered.Hash, Attempt: attempt, Since: r.stamp(),
			}

			return nil
		},
		step: func() (string, error) { return r.joinStep(ctx, tc, obs, f, timing) },
	}
}

// joinStep asks the planner what the machine that is being added needs next, and does it.
func (r *TalosClusterReconciler) joinStep(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation, f *fleet,
	timing plan.Timing,
) (string, error) {
	op := tc.Status.Operation
	cur := op.Current
	now := r.now()

	tm := f.declared(cur.Machine)
	if tm == nil {
		failOperation(op, plan.FailTimeout, "the TalosMachine no longer exists", now)

		return "", nil
	}

	// A machine that was handed nothing yet is left alone while paused or outside a window.
	if hold := obs.policy.Hold(); !hold.Open && cur.Step == plan.StepProvisioning && f.of(tm).node == nil {
		op.Current = nil

		return hold.Reason, nil
	}

	v, err := f.view(ctx, tm)
	if err != nil {
		return "", err
	}

	node := plan.JoinNode{ControlPlane: v.controlPlane()}

	if live := v.node; live != nil {
		node.Member, node.Running = true, v.running()

		if live.kubeNode != nil {
			node.KubernetesKnown, node.Registered, node.NodeReady = true, live.kubeNode.Registered, live.kubeNode.Ready
		}

		if f.facts.etcdMembers != nil {
			node.EtcdKnown = true
			node.EtcdVoter = slices.ContainsFunc(f.facts.etcdMembers, func(m talos.EtcdMember) bool {
				return m.Hostname == live.hostname && !m.IsLearner
			})
		}
	}

	var maintenance talos.Maintenance

	if cur.Step == plan.StepProvisioning && !node.Member && v.rendered != nil {
		if maintenance = r.probe(ctx, tc, tm, v.rendered.Bytes, &node); maintenance != nil {
			defer maintenance.Close() //nolint:errcheck // the machine has its config or it has not; how the connection ends says neither
		}
	}

	// A hostname that cannot be told names nobody: the planner refused such a config before the
	// turn began.
	hostname, _ := renderedHostname(v) //nolint:errcheck // see above

	pre := plan.JoinGate(passOf(obs), plan.JoinMachine{Hostname: hostname}, hostnamesInUse(f, tm.Name)).And(obs.policy.Gates())
	checks := plan.Gate{Reason: notVerifiedYet}

	if cur.Step == plan.StepVerifying {
		checks = plan.VerifyGate(passOf(obs)).And(obs.policy.Gates())
	}

	action := plan.NextJoinStep(stepOf(cur), node, pre, checks, timing, now)
	logAnswer(ctx, op, action, pre, "member", node.Member, "probed", node.Probed, "inMaintenance", node.InMaintenance,
		"identityMatches", node.IdentityMatches)

	if note, done := answered(op, action, now); done {
		return note, nil
	}

	switch action.Kind {
	case plan.Provision:
		switch {
		case v.rendered.Hash != op.Targets[cur.Machine]:
			r.end(tc, talosv1alpha1.OperationSuperseded, cur.Machine+" is to be handed another config than this operation planned")

			return "", nil
		case !r.latest(ctx, tc):
			return noteCatchingUp, nil
		}

		// The machine that just said who it is, over the connection it said it on.
		if err := maintenance.ApplyConfig(ctx, v.rendered.Bytes); err != nil {
			return retried(ctx, op, "hand the machine its config", err), nil
		}

		moveTo(cur, plan.StepJoining, now)
	case plan.Complete:
		// It is a member again, if it ever left.
		if err := r.patchMachine(ctx, tm, func(st *talosv1alpha1.TalosMachineStatus) { st.RemovedAt = nil }); err != nil {
			return "", err
		}

		completed(tc)
	default:
		return "", unexpected(op, action)
	}

	return "", nil
}

// whoAnswers is what answered a maintenance connection at a machine's address, if anything did.
type whoAnswers struct {
	// connection is nil when none could be made, or nothing answered on it; why says which.
	connection talos.Maintenance
	why        error
	found      talos.Candidate
}

// inMaintenance reports whether something answered, and waits in maintenance mode.
func (w whoAnswers) inMaintenance() bool {
	return w.connection != nil && w.found.Stage == talos.StageMaintenance
}

// is reports whether what answered carries the identity tm pins.
func (w whoAnswers) is(tm *talosv1alpha1.TalosMachine) bool {
	return w.connection != nil && strings.EqualFold(w.found.UUID, tm.Spec.Identity.SystemUUID)
}

// close ends the connection, if there is one. Nothing depends on how a connection that was only
// read from ends.
func (w whoAnswers) close() {
	if w.connection != nil {
		_ = w.connection.Close() //nolint:errcheck // see above
	}
}

// ask asks whatever answers at tm's address, over a maintenance connection, what it is. The
// connection carries none of the cluster's credentials, and is only made while membership is
// enabled. The caller closes what is returned.
func ask(ctx context.Context, resolver *target.Resolver, tc *talosv1alpha1.TalosCluster, tm *talosv1alpha1.TalosMachine) whoAnswers {
	addr, err := netip.ParseAddr(tm.Spec.Address)
	if err != nil {
		// An address that is no IP answers nothing; CRD validation refuses it.
		return whoAnswers{why: err}
	}

	connection, err := resolver.ResolveMaintenance(ctx, tc, addr)
	if err != nil {
		return whoAnswers{why: fmt.Errorf("no maintenance connection to %s: %w", addr, err)}
	}

	ctx, cancel := context.WithTimeout(ctx, nodeReadTimeout)
	defer cancel()

	found, err := connection.Probe(ctx)
	if err != nil {
		_ = connection.Close() //nolint:errcheck // nothing was said on it

		return whoAnswers{why: fmt.Errorf("nothing answers in maintenance mode at %s", addr)}
	}

	return whoAnswers{connection: connection, found: found}
}

// probe asks the machine at tm's address what it is, and fills node with the answers. It returns
// the connection while the machine is one a config may be handed to, so that the config goes to
// whoever answered; nil otherwise.
func (r *TalosClusterReconciler) probe(ctx context.Context, tc *talosv1alpha1.TalosCluster, tm *talosv1alpha1.TalosMachine,
	rendered []byte, node *plan.JoinNode,
) talos.Maintenance {
	who := ask(ctx, r.Resolver, tc, tm)
	if who.connection == nil {
		// Nothing answered, which the planner is told as such.
		return nil
	}

	node.Probed, node.InMaintenance, node.IdentityMatches = true, who.inMaintenance(), who.is(tm)

	if !node.InMaintenance || !node.IdentityMatches {
		// Not a machine to hand anything to.
		who.close()

		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, nodeReadTimeout)
	defer cancel()

	problems, err := who.connection.Fits(ctx, rendered)
	if err != nil {
		// Whether it fits is not known, so it is not a machine to hand a config to in this pass.
		node.Probed = false

		who.close()

		return nil
	}

	node.FitProblems = problems

	return who.connection
}

// wiped reports whether a machine answers at tm's address that waits in maintenance mode and
// carries tm's identity: the machine, having forgotten the cluster.
func (r *TalosClusterReconciler) wiped(ctx context.Context, tc *talosv1alpha1.TalosCluster, tm *talosv1alpha1.TalosMachine) bool {
	who := ask(ctx, r.Resolver, tc, tm)
	defer who.close()

	return who.inMaintenance() && who.is(tm)
}

// removalTurns is what the turns of removing machines do.
func (r *TalosClusterReconciler) removalTurns(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation, f *fleet,
	timing plan.Timing,
) turns {
	op := tc.Status.Operation

	return turns{
		begin: func(name string, attempt int32) error {
			m := f.of(f.declared(name))

			// A machine that is no longer asked to leave, or has left by now, has no turn.
			if m.tm == nil || m.tm.Spec.DesiredState != talosv1alpha1.DesiredStateRemoved || (!m.tm.IsMember() && m.node == nil) {
				skipped(op, name)

				return nil
			}

			step := &talosv1alpha1.OperationStep{Machine: name, Step: plan.StepDraining, Attempt: attempt, Since: r.stamp()}

			// What has to be gone from etcd before the machine is: recorded while it can be told.
			if m.controlPlane() {
				step.EtcdMember = etcdMemberOf(f, m)
			}

			op.Current = step

			return nil
		},
		step: func() (string, error) { return r.removalStep(ctx, tc, obs, f, timing) },
		// A human says the machine is gone for good. It is asked nothing more: what is left of it
		// in etcd and in Kubernetes is taken out from the outside.
		also: func(asked func(string) bool) bool {
			failure := op.Failure
			if !asked(talosv1alpha1.AnnotationConfirmDead) || op.Current == nil ||
				(failure.Reason != plan.FailNodeGone && failure.Reason != plan.FailNotWiped) {
				return false
			}

			op.Failure = nil
			op.Current.Dead = true
			op.Current.Attempt++

			moveTo(op.Current, plan.StepLeaving, r.now())

			return true
		},
	}
}

// etcdMemberOf is the ID of the machine's etcd member, in hex: as the machine's own controller
// recorded it, or as the member list has it under the node's name.
func etcdMemberOf(f *fleet, m boundMachine) string {
	if etcd := m.tm.Status.Etcd; etcd != nil && etcd.MemberID != "" {
		return etcd.MemberID
	}

	if f.facts == nil {
		return ""
	}

	name := m.knownAs()
	for _, member := range f.facts.etcdMembers {
		if name != "" && member.Hostname == name {
			return strconv.FormatUint(member.ID, 16)
		}
	}

	return ""
}

// removalPass is the pass the gates of a removal judge. For a machine that is confirmed dead it
// is the cluster without that machine's etcd member: the member cannot answer, will not come
// back, and is what the removal is about to take out.
func removalPass(obs observation, cur *talosv1alpha1.OperationStep) plan.Pass {
	p := passOf(obs)
	if !cur.Dead || p.Cluster == nil || cur.EtcdMember == "" {
		return p
	}

	// A copy: the pass itself is everybody's.
	without := *p.Cluster
	without.EtcdMembers = slices.DeleteFunc(slices.Clone(without.EtcdMembers), func(m plan.EtcdMember) bool {
		return strconv.FormatUint(m.ID, 16) == cur.EtcdMember
	})
	p.Cluster = &without

	return p
}

// removalStep asks the planner what the machine that is being removed needs next, and does it.
func (r *TalosClusterReconciler) removalStep(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation, f *fleet,
	timing plan.Timing,
) (string, error) {
	op := tc.Status.Operation
	cur := op.Current
	now := r.now()

	m := f.of(f.declared(cur.Machine))
	if m.tm == nil {
		failOperation(op, plan.FailTimeout, "the TalosMachine no longer exists", now)

		return "", nil
	}

	// A node nothing was taken from yet is left alone while paused or outside a window.
	if hold := obs.policy.Hold(); !hold.Open && !cur.Cordoned && cur.Step == plan.StepDraining {
		op.Current = nil

		return hold.Reason, nil
	}

	node := plan.RemovalNode{Reachable: m.node != nil, ControlPlane: m.controlPlane(), PodsLeft: -1}

	if f.facts != nil && f.facts.etcdMembers != nil {
		node.EtcdKnown = true
		node.EtcdMember = slices.ContainsFunc(f.facts.etcdMembers, func(member talos.EtcdMember) bool {
			if cur.EtcdMember != "" {
				return strconv.FormatUint(member.ID, 16) == cur.EtcdMember
			}

			return member.Hostname != "" && member.Hostname == m.knownAs()
		})
	}

	// Whether the machine has forgotten the cluster is for the machine to say, once it no longer
	// answers as a member. A machine that is dead is asked nothing.
	if !node.Reachable && !cur.Dead && slices.Contains([]string{plan.StepDraining, plan.StepResetting, plan.StepLeaving}, cur.Step) {
		node.Wiped = r.wiped(ctx, tc, m.tm)
	}

	// A machine that was never known under a name, or whose Node is gone, has none to speak of.
	kube, pods, podsLeft, err := r.nodeToDrain(ctx, m.kubernetesNode(), cur)
	if err != nil {
		return "", fmt.Errorf("%w: %w", errKubernetes, err)
	}

	node.KubernetesKnown, node.Registered, node.PodsLeft = true, kube != nil, podsLeft
	node.Unschedulable = kube != nil && kube.Spec.Unschedulable

	pre := plan.RemovalGate(removalPass(obs, cur), m.knownAs(), cur.Dead).And(obs.policy.Gates())
	checks := plan.Gate{Reason: notVerifiedYet}

	if cur.Step == plan.StepVerifying {
		checks = plan.VerifyGate(passOf(obs)).And(obs.policy.Gates())
	}

	action := plan.NextRemovalStep(stepOf(cur), node, pre, checks, timing, now)
	logAnswer(ctx, op, action, pre, "reachable", node.Reachable, "wiped", node.Wiped, "dead", cur.Dead, "etcdMember", node.EtcdMember,
		"registered", node.Registered, "podsLeft", node.PodsLeft)

	if note, done := answered(op, action, now); done {
		return note, nil
	}

	if note, done, err := r.drained(ctx, tc, action, kube, pods); done {
		return note, err
	}

	switch action.Kind {
	case plan.Reset:
		return r.reset(ctx, tc, obs, m, now)
	case plan.RemoveMember:
		return r.removeMember(ctx, tc, obs)
	case plan.DeleteNode:
		if !r.latest(ctx, tc) {
			return noteCatchingUp, nil
		}

		if err := r.Delete(ctx, kube); err != nil && !apierrors.IsNotFound(err) {
			return "", fmt.Errorf("%w: delete node %s: %w", errKubernetes, kube.Name, err)
		}
	case plan.Complete:
		removed := metav1.NewTime(now)
		if err := r.patchMachine(ctx, m.tm, func(st *talosv1alpha1.TalosMachineStatus) { st.RemovedAt = &removed }); err != nil {
			return "", err
		}

		completed(tc)
	default:
		return "", unexpected(op, action)
	}

	return "", nil
}

// reset asks the node to leave the cluster and forget it.
func (r *TalosClusterReconciler) reset(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation, m boundMachine, now time.Time,
) (string, error) {
	op := tc.Status.Operation

	remover, note := clientTo(ctx, r, tc, obs, "reset the node with", r.Resolver.ResolveRemover)
	if note != "" {
		return note, nil
	}

	if m.controlPlane() {
		handOver(ctx, remover, m.node.address, op.Current.Machine)
	}

	if err := remover.Reset(ctx, m.node.address); err != nil {
		return retried(ctx, op, "reset the node", err), nil
	}

	moveTo(op.Current, plan.StepLeaving, now)

	return "", nil
}

// removeMember takes the etcd member of a machine that is confirmed dead out of etcd, through a
// control plane that answered in this pass.
func (r *TalosClusterReconciler) removeMember(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation) (string, error) {
	op := tc.Status.Operation

	id, err := strconv.ParseUint(op.Current.EtcdMember, 16, 64)
	if err != nil {
		return "the dead machine's etcd member is not known by its ID; remove it with talosctl etcd remove-member", nil //nolint:nilerr // waits
	}

	var peer netip.Addr

	for _, n := range obs.facts.nodes {
		if n.machineType == talos.MachineTypeControlPlane && n.read {
			peer = n.address

			break
		}
	}

	if !peer.IsValid() {
		return "no control plane answered to take the member out of etcd through", nil
	}

	remover, note := clientTo(ctx, r, tc, obs, "change etcd's members with", r.Resolver.ResolveRemover)
	if note != "" {
		return note, nil
	}

	if err := remover.RemoveEtcdMember(ctx, peer, id); err != nil {
		return retried(ctx, op, "take the member out of etcd", err), nil
	}

	return "", nil
}

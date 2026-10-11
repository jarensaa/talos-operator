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
	"net/netip"
	"slices"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/plan"
	"github.com/jarensaa/talos-operator/internal/render"
	"github.com/jarensaa/talos-operator/internal/talos"
)

// Adding and removing machines (docs/scope.md §7.6, §7.7). The rules are the planners'
// (internal/plan/membership.go); this file observes, asks them, and does the one thing they
// answer, like every other operation.
//
// What is different here is who is talked to. A machine that is to be added has no part in the
// cluster yet: it is asked, and handed its config, over a maintenance connection that carries
// none of the cluster's credentials (talos.Maintenance). A machine that leaves is reset through
// the cluster like any other node is reached, by a client that can do that and little else
// (talos.Remover). Both are only made while spec.management.membership is Enabled.

// +kubebuilder:rbac:groups="",resources=nodes,verbs=delete

// members are the machines that count as nodes of the cluster for rollouts, upgrades and the
// proof: the ones that joined and have not left.
func members(all []talosv1alpha1.TalosMachine) []talosv1alpha1.TalosMachine {
	out := make([]talosv1alpha1.TalosMachine, 0, len(all))

	for i := range all {
		if isMember(&all[i]) {
			out = append(out, all[i])
		}
	}

	return out
}

func machineNamed(machines []talosv1alpha1.TalosMachine, name string) *talosv1alpha1.TalosMachine {
	for i := range machines {
		if machines[i].Name == name {
			return &machines[i]
		}
	}

	return nil
}

func controlPlane(pool *talosv1alpha1.TalosMachinePool) bool {
	return pool != nil && pool.Spec.Role == talosv1alpha1.MachineRoleControlPlane
}

func membershipHolds(tc *talosv1alpha1.TalosCluster) *talosv1alpha1.MembershipHolds {
	if tc.Status.Membership == nil {
		tc.Status.Membership = &talosv1alpha1.MembershipHolds{}
	}

	return tc.Status.Membership
}

// keepMembershipHolds drops the holds that are spent: a machine that is gone, one that is to be
// added with another config, one that is no longer asked to be removed.
func keepMembershipHolds(tc *talosv1alpha1.TalosCluster, all []talosv1alpha1.TalosMachine, configs map[string]string) {
	holds := tc.Status.Membership
	if holds == nil {
		return
	}

	for name, config := range holds.Add {
		if tm := machineNamed(all, name); tm == nil || isMember(tm) || (configs[name] != "" && configs[name] != config) {
			delete(holds.Add, name)
		}
	}

	holds.Remove = slices.DeleteFunc(holds.Remove, func(name string) bool {
		tm := machineNamed(all, name)

		return tm == nil || tm.Spec.DesiredState != talosv1alpha1.DesiredStateRemoved
	})

	if len(holds.Add)+len(holds.Remove) == 0 {
		tc.Status.Membership = nil
	}
}

// abortedMachines are the machines an operation had not finished when it was aborted.
func abortedMachines(op *talosv1alpha1.Operation) []string {
	return slices.DeleteFunc(slices.Clone(op.Plan), func(name string) bool { return slices.Contains(op.Completed, name) })
}

// hostnamesInUse are the names a machine that is added must not be given: those of the live
// members, of the members of etcd, and of the Nodes the cluster's machines are known under.
func hostnamesInUse(obs observation, all []talosv1alpha1.TalosMachine, except string) []string {
	var names []string

	if obs.facts != nil {
		for _, n := range obs.facts.nodes {
			names = append(names, n.hostname)
		}

		for _, m := range obs.facts.etcdMembers {
			names = append(names, m.Hostname)
		}
	}

	for i := range all {
		if id := all[i].Status.Identity; id != nil && all[i].Name != except && isMember(&all[i]) {
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

// candidate is a TalosMachine that is to be added, with what is known about it right now.
type candidate struct {
	tm       *talosv1alpha1.TalosMachine
	pool     *talosv1alpha1.TalosMachinePool
	rendered *render.Config
	hostname string
	problem  string
}

func (r *TalosClusterReconciler) candidate(ctx context.Context, tc *talosv1alpha1.TalosCluster,
	pools map[string]*talosv1alpha1.TalosMachinePool, tm *talosv1alpha1.TalosMachine,
) (candidate, error) {
	c := candidate{tm: tm, pool: pools[tm.Spec.PoolRef.Name]}
	if c.pool == nil {
		c.problem = "TalosMachinePool " + tm.Spec.PoolRef.Name + " not found"

		return c, nil
	}

	rendered, problem, err := RenderMachine(ctx, r.Client, tc, c.pool, tm)
	if err != nil {
		return c, err
	}

	if problem != "" {
		c.problem = problem

		return c, nil
	}

	c.rendered = rendered

	if c.hostname, err = talos.RenderedHostname(rendered.Bytes); err != nil {
		c.rendered, c.problem = nil, err.Error()
	}

	return c, nil
}

// joinMachine is the planner's view of a machine that is to be added.
func joinMachine(c candidate) plan.JoinMachine {
	m := plan.JoinMachine{Name: c.tm.Name, ControlPlane: controlPlane(c.pool), Hold: c.tm.Spec.Hold, Hostname: c.hostname}

	if c.rendered == nil {
		m.NotProvisionable = c.problem

		return m
	}

	m.Config = c.rendered.Hash

	// What the machine's own controller found when it asked the machine. It is asked again, and
	// decided on again, in the machine's turn; this only says whether a turn is worth beginning.
	cond := meta.FindStatusCondition(c.tm.Status.Conditions, talosv1alpha1.ConditionProvisionable)

	switch {
	case cond == nil || c.tm.Status.ObservedGeneration != c.tm.Generation:
	case cond.Status == metav1.ConditionTrue:
		m.Provisionable = true
	default:
		m.NotProvisionable = cond.Message
	}

	return m
}

// toAdd lists the machines that are declared and are no members: neither recorded as one, nor
// answering as one right now.
func toAdd(obs observation, all []talosv1alpha1.TalosMachine) []*talosv1alpha1.TalosMachine {
	var out []*talosv1alpha1.TalosMachine

	for i := range all {
		tm := &all[i]
		if tm.Spec.DesiredState != talosv1alpha1.DesiredStateRemoved && !isMember(tm) &&
			nodeWithIdentity(obs.facts, tm.Spec.Identity.SystemUUID) == nil {
			out = append(out, tm)
		}
	}

	return out
}

// toRemove lists the machines that are asked to leave and have not: recorded as members, or
// answering as one right now, as a machine does that was removed as dead and came back.
func toRemove(obs observation, all []talosv1alpha1.TalosMachine) []*talosv1alpha1.TalosMachine {
	var out []*talosv1alpha1.TalosMachine

	for i := range all {
		tm := &all[i]
		if tm.Spec.DesiredState == talosv1alpha1.DesiredStateRemoved &&
			(isMember(tm) || nodeWithIdentity(obs.facts, tm.Spec.Identity.SystemUUID) != nil) {
			out = append(out, tm)
		}
	}

	return out
}

// nodeOf is the hostname a machine's node is known under in this pass: what it answered with, or
// what it was last known as.
func nodeOf(obs observation, tm *talosv1alpha1.TalosMachine) string {
	if n := nodeWithIdentity(obs.facts, tm.Spec.Identity.SystemUUID); n != nil {
		return n.hostname
	}

	if id := tm.Status.Identity; id != nil {
		return id.Hostname
	}

	return ""
}

func kubernetesNodeOf(obs observation, tm *talosv1alpha1.TalosMachine) string {
	if id := tm.Status.Identity; id != nil && id.KubernetesNodeName != "" {
		return id.KubernetesNodeName
	}

	return nodeOf(obs, tm)
}

// removal continues the removal in flight, or plans one. It reports whether a removal holds the
// operation lock, and, when one is due but may not start, why.
func (r *TalosClusterReconciler) removal(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation,
	pools map[string]*talosv1alpha1.TalosMachinePool, all []talosv1alpha1.TalosMachine,
) (bool, *metav1.Condition, error) {
	st := &tc.Status
	timing := rolloutTiming(tc.Spec.Rollout)

	if op := st.Operation; op != nil {
		if op.Type != talosv1alpha1.OperationRemoveMachine {
			return false, nil, nil
		}

		if !membershipEnabled(tc) {
			r.finish(tc, talosv1alpha1.OperationAborted, "membership was disabled"+leftCordoned(op))

			return false, nil, nil
		}

		note, err := r.continueRemoval(ctx, tc, obs, pools, all, timing)
		reportMembershipOperation(tc, obs, note, "a node that is already cordoned is still taken out, no other node is touched")

		return true, nil, err
	}

	leaving := toRemove(obs, all)
	if obs.facts == nil || len(leaving) == 0 || !membershipEnabled(tc) {
		return false, nil, nil
	}

	planned := make([]plan.RemoveMachine, 0, len(leaving))
	for _, tm := range leaving {
		planned = append(planned, plan.RemoveMachine{
			Name:          tm.Name,
			Node:          nodeOf(obs, tm),
			Reachable:     nodeWithIdentity(obs.facts, tm.Spec.Identity.SystemUUID) != nil,
			ControlPlane:  controlPlane(pools[tm.Spec.PoolRef.Name]),
			HostsOperator: r.NodeName != "" && kubernetesNodeOf(obs, tm) == r.NodeName,
		})
	}

	holds := talosv1alpha1.MembershipHolds{}
	if st.Membership != nil {
		holds = *st.Membership
	}

	result := plan.PlanRemoval(planned, plan.RemovalSettings{Paused: tc.Spec.Paused, Policy: obs.policy, Held: holds.Remove}, passOf(obs))

	if result.MayStart() {
		if err := timing.Validate(); err != nil {
			result.BlockReason, result.BlockMessage = talosv1alpha1.ReasonInvalidRollout, "spec.rollout: "+err.Error()
		}
	}

	if result.MayStart() {
		now := metav1.NewTime(r.now())
		st.Operation = &talosv1alpha1.Operation{
			ID:        fmt.Sprintf("%s-%d", result.Change, now.Unix()),
			Type:      talosv1alpha1.OperationRemoveMachine,
			Target:    result.Change,
			Plan:      result.Pending,
			StartedAt: now,
		}

		return true, nil, nil
	}

	if len(result.Pending) == 0 {
		return false, nil, nil
	}

	return false, &metav1.Condition{
		Type: talosv1alpha1.ConditionRolloutBlocked, Status: metav1.ConditionTrue, Reason: result.BlockReason,
		Message: fmt.Sprintf("the removal of %s does not start: %s", strings.Join(result.Pending, ", "), result.BlockMessage),
	}, nil
}

// join continues the adding of machines in flight, or plans it. It runs last: a machine is only
// added while nothing else is due (notAtRest).
func (r *TalosClusterReconciler) join(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation,
	pools map[string]*talosv1alpha1.TalosMachinePool, all []talosv1alpha1.TalosMachine,
) error {
	st := &tc.Status
	timing := rolloutTiming(tc.Spec.Rollout)

	if op := st.Operation; op != nil {
		if op.Type != talosv1alpha1.OperationAddMachine {
			return nil
		}

		if !membershipEnabled(tc) {
			r.finish(tc, talosv1alpha1.OperationAborted, "membership was disabled")

			return nil
		}

		note, err := r.continueJoin(ctx, tc, obs, pools, all, timing)
		reportMembershipOperation(tc, obs, note, "a machine that was handed its config is still seen to join, no other machine is added")

		return err
	}

	waiting := toAdd(obs, all)
	if obs.facts == nil || len(waiting) == 0 || !membershipEnabled(tc) {
		keepMembershipHolds(tc, all, nil)

		return nil
	}

	planned := make([]plan.JoinMachine, 0, len(waiting))
	configs := map[string]string{}

	for _, tm := range waiting {
		c, err := r.candidate(ctx, tc, pools, tm)
		if err != nil {
			return err
		}

		m := joinMachine(c)
		planned = append(planned, m)
		configs[tm.Name] = m.Config
	}

	keepMembershipHolds(tc, all, configs)

	holds := talosv1alpha1.MembershipHolds{}
	if st.Membership != nil {
		holds = *st.Membership
	}

	result := plan.PlanJoin(planned, plan.JoinSettings{
		Paused: tc.Spec.Paused, Policy: obs.policy, NotAtRest: notAtRest(tc), Hostnames: hostnamesInUse(obs, all, ""), Held: holds.Add,
	}, passOf(obs))

	if result.MayStart() {
		if err := timing.Validate(); err != nil {
			result.BlockReason, result.BlockMessage = talosv1alpha1.ReasonInvalidRollout, "spec.rollout: "+err.Error()
		}
	}

	if result.MayStart() {
		now := metav1.NewTime(r.now())
		st.Operation = &talosv1alpha1.Operation{
			ID:        fmt.Sprintf("%s-%d", result.Change, now.Unix()),
			Type:      talosv1alpha1.OperationAddMachine,
			Target:    result.Change,
			Plan:      result.Pending,
			Targets:   result.Configs,
			StartedAt: now,
		}

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
func reportMembership(tc *talosv1alpha1.TalosCluster, obs observation, all []talosv1alpha1.TalosMachine) {
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

	leaving, waiting := toRemove(obs, all), toAdd(obs, all)

	switch {
	case obs.facts == nil:
	case len(leaving)+len(waiting) == 0:
		current(metav1.ConditionTrue, talosv1alpha1.ReasonSucceeded,
			"all %d machines that are declared are members, and no member is to be removed", len(members(all)))
	case !membershipEnabled(tc):
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

// reportMembershipOperation sets MembershipCurrent and RolloutBlocked for the adding or removing
// in flight.
func reportMembershipOperation(tc *talosv1alpha1.TalosCluster, obs observation, note, finishes string) {
	st, gen := &tc.Status, tc.Generation

	op := st.Operation
	if op == nil {
		return
	}

	what, reason := "adding machines", talosv1alpha1.ReasonAdding
	if op.Type == talosv1alpha1.OperationRemoveMachine {
		what, reason = "removing machines", talosv1alpha1.ReasonRemoving
	}

	if f := op.Failure; f != nil {
		msg := fmt.Sprintf("%s halted %s: %s", what, haltedAt(f), f.Message)

		setCondition(st, gen, talosv1alpha1.ConditionMembershipCurrent, metav1.ConditionFalse, talosv1alpha1.ReasonOperationFailed, msg)
		setCondition(st, gen, talosv1alpha1.ConditionRolloutBlocked, metav1.ConditionTrue, talosv1alpha1.ReasonOperationFailed,
			msg+" "+f.Remediation)
		st.Phase = talosv1alpha1.ClusterPhaseDegraded

		return
	}

	st.Phase = talosv1alpha1.ClusterPhaseManaged

	setCondition(st, gen, talosv1alpha1.ConditionMembershipCurrent, metav1.ConditionFalse, reason, progressOf(op, note))
	reportHeld(tc, obs.policy, finishes)
}

func (r *TalosClusterReconciler) continueJoin(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation,
	pools map[string]*talosv1alpha1.TalosMachinePool, all []talosv1alpha1.TalosMachine, timing plan.Timing,
) (string, error) {
	op := tc.Status.Operation

	hold := func(names ...string) {
		holds := membershipHolds(tc)
		if holds.Add == nil {
			holds.Add = map[string]string{}
		}

		for _, name := range names {
			holds.Add[name] = op.Targets[name]
		}
	}

	begin := func(name string, attempt int32) error {
		tm := machineNamed(all, name)

		switch {
		case tm == nil, tm.Spec.Hold, tm.Spec.DesiredState == talosv1alpha1.DesiredStateRemoved:
			skipped(op, name)

			return nil
		}

		c, err := r.candidate(ctx, tc, pools, tm)
		if err != nil {
			return err
		}

		// A machine is handed exactly the config that was planned for it.
		if c.rendered == nil || c.rendered.Hash != op.Targets[name] {
			r.end(tc, talosv1alpha1.OperationSuperseded, name+" is to be handed another config than this operation planned")

			return nil
		}

		op.Current = &talosv1alpha1.OperationStep{
			Machine: name, Step: plan.StepProvisioning, DesiredHash: c.rendered.Hash, Attempt: attempt, Since: metav1.NewTime(r.now()),
		}

		return nil
	}

	return r.drive(ctx, tc, obs, timing, turns{
		begin: begin,
		step: func() (string, error) {
			return r.joinStep(ctx, tc, obs, pools, all, timing)
		},
		failed: func() error {
			f := op.Failure
			asked := func(annotation string) bool { return f.Token != "" && tc.Annotations[annotation] == f.Token }

			switch {
			case asked(talosv1alpha1.AnnotationAbort):
				hold(abortedMachines(op)...)
				r.end(tc, talosv1alpha1.OperationAborted, fmt.Sprintf("aborted after failing at %s (%s): %s", f.Machine, f.Reason, f.Message))
			case asked(talosv1alpha1.AnnotationSkipMachine):
				hold(f.Machine)
				skipped(op, f.Machine)
			case asked(talosv1alpha1.AnnotationRetry):
				attempt := int32(1)
				if op.Current != nil {
					attempt = op.Current.Attempt + 1
				}

				// The machine's turn starts over from what it says now: one that took its config
				// is seen to be a member, and is not handed it again.
				op.Failure, op.Current = nil, nil

				return begin(f.Machine, attempt)
			}

			return nil
		},
		skip: func(machine string) error {
			hold(machine)

			return nil
		},
		abort: func() { hold(abortedMachines(op)...) },
	})
}

// joinStep asks the planner what the machine that is being added needs next, and does it.
func (r *TalosClusterReconciler) joinStep(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation,
	pools map[string]*talosv1alpha1.TalosMachinePool, all []talosv1alpha1.TalosMachine, timing plan.Timing,
) (string, error) {
	op := tc.Status.Operation
	cur := op.Current
	now := r.now()

	tm := machineNamed(all, cur.Machine)
	if tm == nil {
		failOperation(op, plan.FailTimeout, "the TalosMachine no longer exists", now)

		return "", nil
	}

	// A machine that was handed nothing yet is left alone while paused or outside a window.
	if hold := obs.policy.Hold(tc.Spec.Paused); !hold.Open && cur.Step == plan.StepProvisioning &&
		nodeWithIdentity(obs.facts, tm.Spec.Identity.SystemUUID) == nil {
		op.Current = nil

		return hold.Reason, nil
	}

	c, err := r.candidate(ctx, tc, pools, tm)
	if err != nil {
		return "", err
	}

	node := plan.JoinNode{ControlPlane: controlPlane(c.pool)}

	if live := nodeWithIdentity(obs.facts, tm.Spec.Identity.SystemUUID); live != nil {
		node.Member = true
		node.Running = live.status != nil && live.status.Stage == plan.StageRunning && live.status.Ready

		if live.kubeNode != nil {
			node.KubernetesKnown, node.Registered, node.NodeReady = true, live.kubeNode.Registered, live.kubeNode.Ready
		}

		if obs.facts.etcdMembers != nil {
			node.EtcdKnown = true
			node.EtcdVoter = slices.ContainsFunc(obs.facts.etcdMembers, func(m talos.EtcdMember) bool {
				return m.Hostname == live.hostname && !m.IsLearner
			})
		}
	}

	var maintenance talos.Maintenance

	if cur.Step == plan.StepProvisioning && !node.Member && c.rendered != nil {
		if maintenance, err = r.probe(ctx, tc, tm, c.rendered, &node); err != nil {
			return "", err
		}

		if maintenance != nil {
			defer maintenance.Close() //nolint:errcheck // the machine has its config or it has not; how the connection ends says neither
		}
	}

	pre := plan.JoinGate(passOf(obs), plan.JoinMachine{Hostname: c.hostname}, hostnamesInUse(obs, all, tm.Name)).And(obs.policy.Gates())
	checks := plan.Gate{Reason: notVerifiedYet}

	if cur.Step == plan.StepVerifying {
		checks = plan.VerifyGate(passOf(obs)).And(obs.policy.Gates())
	}

	step := plan.JoinStep{Name: cur.Step, Since: cur.Since.Time, HealthySince: timeOf(cur.HealthySince)}
	action := plan.NextJoinStep(step, node, pre, checks, timing, now)
	stamp := metav1.NewTime(now)

	if action.Kind != plan.Wait {
		logf.FromContext(ctx).Info("join step", "machine", cur.Machine, "step", cur.Step, "action", action.Kind.String(),
			"next", action.Step, "reason", action.Reason, "detail", action.Message, "member", node.Member, "probed", node.Probed,
			"inMaintenance", node.InMaintenance, "identityMatches", node.IdentityMatches, "gate", pre.Note)
	}

	switch action.Kind { //nolint:exhaustive // the actions of the other operations never come out of this one
	case plan.Wait:
		return action.Message, nil
	case plan.Advance:
		cur.Step, cur.Since, cur.HealthySince = action.Step, stamp, nil
	case plan.MarkHealthy:
		cur.HealthySince = &stamp
	case plan.MarkUnhealthy:
		cur.HealthySince = nil

		return action.Message, nil
	case plan.Provision:
		switch {
		case c.rendered.Hash != op.Targets[cur.Machine]:
			r.end(tc, talosv1alpha1.OperationSuperseded, cur.Machine+" is to be handed another config than this operation planned")

			return "", nil
		case !r.latest(ctx, tc):
			return noteCatchingUp, nil
		}

		// The machine that just said who it is, over the connection it said it on.
		if err := maintenance.ApplyConfig(ctx, c.rendered.Bytes); err != nil {
			logf.FromContext(ctx).Info("handing the machine its config failed, will retry", "machine", cur.Machine, "error", err.Error())

			return "the last attempt to hand the machine its config failed: " + err.Error(), nil
		}

		cur.Step, cur.Since, cur.HealthySince = plan.StepJoining, stamp, nil
	case plan.Complete:
		// It is a member again, if it ever left.
		if err := r.patchMachine(ctx, tm, func(st *talosv1alpha1.TalosMachineStatus) { st.RemovedAt = nil }); err != nil {
			return "", err
		}

		verified(tc, false)
	case plan.Fail:
		failOperation(op, action.Reason, action.Message, now)
	}

	return "", nil
}

// probe asks the machine at tm's address, over a maintenance connection, what it is, and fills
// node with the answers. It returns the connection while the machine is one a config may be
// handed to, so that the config goes to whoever answered; nil otherwise.
func (r *TalosClusterReconciler) probe(ctx context.Context, tc *talosv1alpha1.TalosCluster, tm *talosv1alpha1.TalosMachine,
	rendered *render.Config, node *plan.JoinNode,
) (talos.Maintenance, error) {
	addr, err := netip.ParseAddr(tm.Spec.Address)
	if err != nil {
		return nil, nil //nolint:nilerr,nilnil // an address that is no IP answers nothing; CRD validation refuses it
	}

	maintenance, err := r.Resolver.ResolveMaintenance(ctx, tc, addr)
	if err != nil {
		return nil, nil //nolint:nilerr,nilnil // membership was just disabled: nothing is asked, and the operation ends next pass
	}

	readCtx, cancel := context.WithTimeout(ctx, nodeReadTimeout)
	defer cancel()

	found, err := maintenance.Probe(readCtx)
	if err != nil {
		_ = maintenance.Close() //nolint:errcheck // nothing was said on it

		return nil, nil //nolint:nilerr,nilnil // nothing answered, which the planner is told as such
	}

	node.Probed = true
	node.InMaintenance = found.Stage == talos.StageMaintenance
	node.IdentityMatches = strings.EqualFold(found.UUID, tm.Spec.Identity.SystemUUID)

	if !node.InMaintenance || !node.IdentityMatches {
		_ = maintenance.Close() //nolint:errcheck // only read from

		return nil, nil //nolint:nilnil // not a machine to hand anything to
	}

	problems, err := maintenance.Fits(readCtx, rendered.Bytes)
	if err != nil {
		// Whether it fits is not known, so it is not a machine to hand a config to in this pass.
		node.Probed = false
		_ = maintenance.Close() //nolint:errcheck // only read from

		return nil, nil //nolint:nilerr,nilnil // as above
	}

	node.FitProblems = problems

	return maintenance, nil
}

// patchMachine changes the fields of a TalosMachine's status that the cluster's operations own.
func (r *TalosClusterReconciler) patchMachine(ctx context.Context, tm *talosv1alpha1.TalosMachine,
	mutate func(*talosv1alpha1.TalosMachineStatus),
) error {
	original := tm.DeepCopy()

	mutate(&tm.Status)

	if equality.Semantic.DeepEqual(original.Status, tm.Status) {
		return nil
	}

	if err := r.Status().Patch(ctx, tm, client.MergeFrom(original)); err != nil {
		return fmt.Errorf("patch status of TalosMachine %s: %w", tm.Name, err)
	}

	return nil
}

func (r *TalosClusterReconciler) continueRemoval(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation,
	pools map[string]*talosv1alpha1.TalosMachinePool, all []talosv1alpha1.TalosMachine, timing plan.Timing,
) (string, error) {
	op := tc.Status.Operation

	hold := func(names ...string) {
		holds := membershipHolds(tc)
		holds.Remove = append(holds.Remove, names...)
		slices.Sort(holds.Remove)
		holds.Remove = slices.Compact(holds.Remove)
	}

	begin := func(name string, attempt int32, cordoned bool) {
		tm := machineNamed(all, name)

		// A machine that is no longer asked to leave, or has left by now, has no turn.
		if tm == nil || tm.Spec.DesiredState != talosv1alpha1.DesiredStateRemoved ||
			(!isMember(tm) && nodeWithIdentity(obs.facts, tm.Spec.Identity.SystemUUID) == nil) {
			skipped(op, name)

			return
		}

		step := &talosv1alpha1.OperationStep{
			Machine: name, Step: plan.StepDraining, Attempt: attempt, Cordoned: cordoned, Since: metav1.NewTime(r.now()),
		}

		// What has to be gone from etcd before the machine is: recorded while it can be told.
		if controlPlane(pools[tm.Spec.PoolRef.Name]) {
			step.EtcdMember = etcdMemberOf(obs, tm)
		}

		op.Current = step
	}

	return r.drive(ctx, tc, obs, timing, turns{
		begin: func(name string, attempt int32) error {
			begin(name, attempt, false)

			return nil
		},
		step: func() (string, error) {
			return r.removalStep(ctx, tc, obs, pools, all, timing)
		},
		failed: func() error {
			f := op.Failure
			asked := func(annotation string) bool { return f.Token != "" && tc.Annotations[annotation] == f.Token }

			switch {
			case asked(talosv1alpha1.AnnotationAbort):
				hold(abortedMachines(op)...)
				r.end(tc, talosv1alpha1.OperationAborted,
					fmt.Sprintf("aborted after failing at %s (%s): %s%s", f.Machine, f.Reason, f.Message, leftCordoned(op)))
			case asked(talosv1alpha1.AnnotationSkipMachine):
				hold(f.Machine)
				skipped(op, f.Machine)
			case asked(talosv1alpha1.AnnotationConfirmDead) && op.Current != nil &&
				(f.Reason == plan.FailNodeGone || f.Reason == plan.FailNotWiped):
				// A human says the machine is gone for good. It is asked nothing more: what is left
				// of it in etcd and in Kubernetes is taken out from the outside.
				op.Failure = nil
				op.Current.Dead, op.Current.Step, op.Current.Since, op.Current.HealthySince = true, plan.StepLeaving, metav1.NewTime(r.now()), nil
				op.Current.Attempt++
			case asked(talosv1alpha1.AnnotationRetry):
				attempt, cordoned := int32(1), false
				if op.Current != nil {
					attempt, cordoned = op.Current.Attempt+1, op.Current.Cordoned
				}

				op.Failure, op.Current = nil, nil
				begin(f.Machine, attempt, cordoned)
			}

			return nil
		},
		skip: func(machine string) error {
			hold(machine)

			return nil
		},
		abort:     func() { hold(abortedMachines(op)...) },
		nodeHooks: true,
	})
}

// etcdMemberOf is the ID of the machine's etcd member, in hex: as the machine's own controller
// recorded it, or as the member list has it under the node's name.
func etcdMemberOf(obs observation, tm *talosv1alpha1.TalosMachine) string {
	if etcd := tm.Status.Etcd; etcd != nil && etcd.MemberID != "" {
		return etcd.MemberID
	}

	if obs.facts == nil {
		return ""
	}

	name := nodeOf(obs, tm)
	for _, m := range obs.facts.etcdMembers {
		if name != "" && m.Hostname == name {
			return strconv.FormatUint(m.ID, 16)
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

	p.Cluster.EtcdMembers = slices.DeleteFunc(slices.Clone(p.Cluster.EtcdMembers), func(m plan.EtcdMember) bool {
		return strconv.FormatUint(m.ID, 16) == cur.EtcdMember
	})

	return p
}

// removalStep asks the planner what the machine that is being removed needs next, and does it.
//
//nolint:gocyclo // one case per action of the planner
func (r *TalosClusterReconciler) removalStep(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation,
	pools map[string]*talosv1alpha1.TalosMachinePool, all []talosv1alpha1.TalosMachine, timing plan.Timing,
) (string, error) {
	op := tc.Status.Operation
	cur := op.Current
	now := r.now()

	tm := machineNamed(all, cur.Machine)
	if tm == nil {
		failOperation(op, plan.FailTimeout, "the TalosMachine no longer exists", now)

		return "", nil
	}

	// A node nothing was taken from yet is left alone while paused or outside a window.
	if hold := obs.policy.Hold(tc.Spec.Paused); !hold.Open && !cur.Cordoned && cur.Step == plan.StepDraining {
		op.Current = nil

		return hold.Reason, nil
	}

	live := nodeWithIdentity(obs.facts, tm.Spec.Identity.SystemUUID)
	node := plan.RemovalNode{Reachable: live != nil, ControlPlane: controlPlane(pools[tm.Spec.PoolRef.Name]), PodsLeft: -1}

	if obs.facts != nil && obs.facts.etcdMembers != nil {
		node.EtcdKnown = true
		node.EtcdMember = slices.ContainsFunc(obs.facts.etcdMembers, func(m talos.EtcdMember) bool {
			if cur.EtcdMember != "" {
				return strconv.FormatUint(m.ID, 16) == cur.EtcdMember
			}

			return m.Hostname != "" && m.Hostname == nodeOf(obs, tm)
		})
	}

	// Whether the machine has forgotten the cluster is for the machine to say, once it no longer
	// answers as a member. A machine that is dead is asked nothing.
	if !node.Reachable && !cur.Dead && slices.Contains([]string{plan.StepDraining, plan.StepResetting, plan.StepLeaving}, cur.Step) {
		node.Wiped = r.wiped(ctx, tc, tm)
	}

	kube, pods, err := r.removalKubernetes(ctx, kubernetesNodeOf(obs, tm), cur, &node)
	if err != nil {
		return "", fmt.Errorf("%w: %w", errKubernetes, err)
	}

	hostname := nodeOf(obs, tm)
	pre := plan.RemovalGate(removalPass(obs, cur), hostname, cur.Dead).And(obs.policy.Gates())
	checks := plan.Gate{Reason: notVerifiedYet}

	if cur.Step == plan.StepVerifying {
		checks = plan.VerifyGate(passOf(obs)).And(obs.policy.Gates())
	}

	step := plan.RemovalStep{
		Name: cur.Step, Since: cur.Since.Time, Cordoned: cur.Cordoned, Dead: cur.Dead, HealthySince: timeOf(cur.HealthySince),
	}
	action := plan.NextRemovalStep(step, node, pre, checks, timing, now)
	stamp := metav1.NewTime(now)

	if action.Kind != plan.Wait {
		logf.FromContext(ctx).Info("removal step", "machine", cur.Machine, "step", cur.Step, "action", action.Kind.String(),
			"next", action.Step, "reason", action.Reason, "detail", action.Message, "reachable", node.Reachable, "wiped", node.Wiped,
			"dead", cur.Dead, "etcdMember", node.EtcdMember, "registered", node.Registered, "podsLeft", node.PodsLeft, "gate", pre.Note)
	}

	moveTo := func(next string) { cur.Step, cur.Since, cur.HealthySince = next, stamp, nil }

	switch action.Kind { //nolint:exhaustive // the actions of the other operations never come out of this one
	case plan.Wait:
		return action.Message, nil
	case plan.Advance:
		moveTo(action.Step)
	case plan.MarkHealthy:
		cur.HealthySince = &stamp
	case plan.MarkUnhealthy:
		cur.HealthySince = nil

		return action.Message, nil
	case plan.MarkCordoned:
		cur.Cordoned = true
	case plan.Cordon:
		if !r.latest(ctx, tc) {
			return noteCatchingUp, nil
		}

		return "", r.setUnschedulable(ctx, kube, true)
	case plan.Evict:
		if !r.latest(ctx, tc) {
			return noteCatchingUp, nil
		}

		held, err := r.evict(ctx, pods)
		if err != nil {
			return "", err
		}

		if held > 0 {
			return fmt.Sprintf("%s; a PodDisruptionBudget holds %d of them back for now", action.Message, held), nil
		}

		return action.Message, nil
	case plan.Reset:
		return r.reset(ctx, tc, obs, live, node.ControlPlane, moveTo)
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
		if err := r.patchMachine(ctx, tm, func(st *talosv1alpha1.TalosMachineStatus) { st.RemovedAt = &stamp }); err != nil {
			return "", err
		}

		verified(tc, true)
	case plan.Fail:
		failOperation(op, action.Reason, action.Message, now)
	}

	return "", nil
}

// wiped reports whether a machine answers at tm's address that waits in maintenance mode and
// carries tm's identity: the machine, having forgotten the cluster.
func (r *TalosClusterReconciler) wiped(ctx context.Context, tc *talosv1alpha1.TalosCluster, tm *talosv1alpha1.TalosMachine) bool {
	addr, err := netip.ParseAddr(tm.Spec.Address)
	if err != nil {
		return false
	}

	maintenance, err := r.Resolver.ResolveMaintenance(ctx, tc, addr)
	if err != nil {
		return false
	}

	defer maintenance.Close() //nolint:errcheck // only read from

	ctx, cancel := context.WithTimeout(ctx, nodeReadTimeout)
	defer cancel()

	found, err := maintenance.Probe(ctx)

	return err == nil && found.Stage == talos.StageMaintenance && strings.EqualFold(found.UUID, tm.Spec.Identity.SystemUUID)
}

// removalKubernetes reads the machine's Node and, while it is being drained, the pods on it.
func (r *TalosClusterReconciler) removalKubernetes(ctx context.Context, name string, cur *talosv1alpha1.OperationStep,
	node *plan.RemovalNode,
) (*corev1.Node, []corev1.Pod, error) {
	if name == "" {
		// A machine that was never known under a name has no Node to speak of.
		node.KubernetesKnown = true

		return nil, nil, nil
	}

	kube := &corev1.Node{}
	if err := r.Get(ctx, client.ObjectKey{Name: name}, kube); err != nil {
		if apierrors.IsNotFound(err) {
			node.KubernetesKnown = true

			return nil, nil, nil
		}

		return nil, nil, err
	}

	node.KubernetesKnown, node.Registered, node.Unschedulable = true, true, kube.Spec.Unschedulable

	if cur.Step != plan.StepDraining || !node.Unschedulable {
		return kube, nil, nil
	}

	pods, err := r.podsToDrain(ctx, name)
	if err != nil {
		logf.FromContext(ctx).Info("listing the pods to drain failed, will retry", "node", name, "error", err.Error())

		return kube, nil, nil
	}

	node.PodsLeft = len(pods)

	return kube, pods, nil
}

// reset asks the node to leave the cluster and forget it.
func (r *TalosClusterReconciler) reset(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation, live *nodeFacts,
	isControlPlane bool, moveTo func(string),
) (string, error) {
	cur := tc.Status.Operation.Current

	if !r.latest(ctx, tc) {
		return noteCatchingUp, nil
	}

	remover, err := r.Resolver.ResolveRemover(ctx, tc, obs.endpoints)
	if err != nil {
		//nolint:nilerr // what the step waits for; the planner's timeouts end it if this lasts
		return "no Talos client to reset the node with: " + err.Error(), nil
	}

	if isControlPlane {
		// A courtesy to the cluster, not a condition: etcd elects a new leader either way.
		if err := remover.ForfeitEtcdLeadership(ctx, live.address); err != nil {
			logf.FromContext(ctx).Info("etcd leadership was not handed over before the reset", "machine", cur.Machine, "error", err.Error())
		}
	}

	if err := remover.Reset(ctx, live.address); err != nil {
		logf.FromContext(ctx).Info("the reset request failed, will retry", "machine", cur.Machine, "error", err.Error())

		return "the last attempt to reset the node failed: " + err.Error(), nil
	}

	moveTo(plan.StepLeaving)

	return "", nil
}

// removeMember takes the etcd member of a machine that is confirmed dead out of etcd, through a
// control plane that answered in this pass.
func (r *TalosClusterReconciler) removeMember(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation) (string, error) {
	cur := tc.Status.Operation.Current

	id, err := strconv.ParseUint(cur.EtcdMember, 16, 64)
	if err != nil {
		return "the dead machine's etcd member is not known by its ID; remove it with talosctl etcd remove-member", nil //nolint:nilerr // waits
	}

	var peer netip.Addr

	for _, n := range obs.facts.nodes {
		if n.machineType == machineTypeControlPlane && n.read {
			peer = n.address

			break
		}
	}

	if !peer.IsValid() {
		return "no control plane answered to take the member out of etcd through", nil
	}

	if !r.latest(ctx, tc) {
		return noteCatchingUp, nil
	}

	remover, err := r.Resolver.ResolveRemover(ctx, tc, obs.endpoints)
	if err != nil {
		return "no Talos client to change etcd's members with: " + err.Error(), nil //nolint:nilerr // waits; the planner's timeout ends it
	}

	if err := remover.RemoveEtcdMember(ctx, peer, id); err != nil {
		logf.FromContext(ctx).Info("removing the etcd member failed, will retry", "machine", cur.Machine, "error", err.Error())

		return "the last attempt to take the member out of etcd failed: " + err.Error(), nil
	}

	return "", nil
}

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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"net/netip"
	"slices"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/classify"
	"github.com/jarensaa/talos-operator/internal/plan"
	"github.com/jarensaa/talos-operator/internal/render"
	"github.com/jarensaa/talos-operator/internal/talos"
	"github.com/jarensaa/talos-operator/internal/target"
)

const (
	// rolloutInterval is how often a node step in flight is looked at again. The step is driven
	// by time (the try window, the soak) as much as by events.
	rolloutInterval = 5 * time.Second
	// confirmCallTimeout bounds the call that confirms a tried config. A confirm is only sent
	// while plan.ConfirmMargin of the try window is left; a call that could outlast the margin
	// might land after Talos reverted the config, and would then apply it with no way back.
	confirmCallTimeout = plan.ConfirmMargin / 2
	// maxHistory bounds status.history, as the CRD does.
	maxHistory = 10
)

// The rollout executor (docs/scope.md §6.3, §8.5.1). It decides nothing about safety itself:
// it observes, asks a planner in internal/plan, and carries out the one action the planner
// answers with. Three rules keep that honest:
//
//   - a step is persisted in status.operation by one reconcile and acted on by a later one, so
//     a replica taking over finds every side effect announced before it happened;
//   - a side effect is only sent after reading the TalosCluster back from the API server and
//     finding it unchanged, so it is never based on a stale view of the step or the spec;
//   - what a step does next is decided from what the node reports now, never from memory.

// rollout owns ConfigInSync and RolloutBlocked once the handover is verified: it continues the
// operation in flight, or plans one.
func (r *TalosClusterReconciler) rollout(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation,
	pools map[string]*talosv1alpha1.TalosMachinePool, machines []talosv1alpha1.TalosMachine,
) error {
	timing := rolloutTiming(tc.Spec.Rollout)

	if op := tc.Status.Operation; op != nil {
		if op.Type != talosv1alpha1.OperationConfigRollout {
			// Another kind of operation holds the lock.
			return nil
		}

		note, err := r.continueRollout(ctx, tc, obs, pools, machines, timing)
		reportOperation(tc, obs, note)

		return err
	}

	if obs.facts == nil {
		return nil
	}

	return r.planRollout(ctx, tc, obs, pools, machines, timing)
}

// rolloutTiming reads spec.rollout. A duration that is not set takes its default; one set to
// zero is zero.
func rolloutTiming(ro talosv1alpha1.Rollout) plan.Timing {
	t := plan.Timing{
		TryTimeout: plan.DefaultTryTimeout, Soak: plan.DefaultSoak, StepTimeout: plan.DefaultStepTimeout,
		DrainTimeout: plan.DefaultDrainTimeout,
	}

	if ro.TryTimeout != nil {
		t.TryTimeout = ro.TryTimeout.Duration
	}

	if ro.Soak != nil {
		t.Soak = ro.Soak.Duration
	}

	if ro.StepTimeout != nil {
		t.StepTimeout = ro.StepTimeout.Duration
	}

	if ro.DrainTimeout != nil {
		t.DrainTimeout = ro.DrainTimeout.Duration
	}

	return t
}

// machineView is a TalosMachine with what a rollout needs to know about it right now. It is
// built from this reconcile's observation and a fresh render, never from the machine's status:
// the status only says whether the machine's own controller has caught up (see planMachine).
type machineView struct {
	tm   *talosv1alpha1.TalosMachine
	pool *talosv1alpha1.TalosMachinePool
	// node is the live node carrying the machine's hardware identity; nil if none answered.
	node *nodeFacts
	// live is the node's normalized active config; nil if it could not be read.
	live *render.Config
	// desired is what the spec renders now; nil if it does not render, and problem says why.
	desired *render.Config
	problem string
	// withhold is set when a config source is a Secret: what Talos says about the config may
	// quote it, so it is not repeated.
	withhold bool
	change   classify.Change
}

func (r *TalosClusterReconciler) view(ctx context.Context, tc *talosv1alpha1.TalosCluster, facts *clusterFacts,
	pools map[string]*talosv1alpha1.TalosMachinePool, tm *talosv1alpha1.TalosMachine,
) (machineView, error) {
	v := machineView{tm: tm, pool: pools[tm.Spec.PoolRef.Name]}
	if v.pool == nil {
		v.problem = "TalosMachinePool " + tm.Spec.PoolRef.Name + " not found"

		return v, nil
	}

	if v.node = nodeWithIdentity(facts, tm.Spec.Identity.SystemUUID); v.node != nil {
		if live, err := normalizedLive(*v.node); err == nil {
			v.live = live
		}
	}

	desired, resolved, err := renderMachine(ctx, r.Client, tc, v.pool, tm)
	if err != nil {
		var problem *configProblem
		if !errors.As(err, &problem) {
			return v, err
		}

		v.problem = problem.msg

		return v, nil
	}

	v.desired = desired
	v.withhold = len(resolved.fromSecrets) > 0

	if v.live != nil {
		// A node that runs what its config's installer installs takes that config like any other.
		var opts []classify.Option
		if runs(tc, v.pool, v.node) {
			opts = append(opts, classify.Installed())
		}

		if v.change, err = classify.Classify(v.live, v.desired, opts...); err != nil {
			v.desired, v.problem = nil, "classify the change: "+err.Error()
		}
	}

	return v, nil
}

// hostname is the hostname of the machine's live node; empty if none answered.
func (v machineView) hostname() string {
	if v.node == nil {
		return ""
	}

	return v.node.hostname
}

// nodeWithIdentity returns the observed node with the given SMBIOS UUID. A node whose identity
// could not be read matches nothing: the operator only acts on hardware it has just identified
// (docs/scope.md §8.1, invariant 8).
func nodeWithIdentity(facts *clusterFacts, uuid string) *nodeFacts {
	if facts == nil || uuid == "" {
		return nil
	}

	for i := range facts.nodes {
		if strings.EqualFold(facts.nodes[i].systemUUID, uuid) {
			return &facts.nodes[i]
		}
	}

	return nil
}

// planMachine is the planner's view of a machine. It is Current only when the machine's own
// controller reports the very configs seen here, so the diff a human reads in the machine's
// status is the change that would be rolled out.
func planMachine(v machineView) plan.Machine {
	st := v.tm.Status
	m := plan.Machine{
		Name: v.tm.Name,
		Node: v.hostname(),
		Hold: v.tm.Spec.Hold || v.tm.Spec.DesiredState == talosv1alpha1.DesiredStateRemoved,
	}

	if v.pool != nil {
		m.ControlPlane = v.pool.Spec.Role == talosv1alpha1.MachineRoleControlPlane
	}

	if st.Config != nil {
		m.AppliedHash, m.SkippedHash = st.Config.AppliedHash, st.Config.SkippedHash
	}

	if v.live == nil || v.desired == nil {
		return m
	}

	m.LiveHash, m.DesiredHash = v.live.Hash, v.desired.Hash
	m.Class, m.Reasons = v.change.Class, v.change.Reasons
	m.Current = meta.IsStatusConditionTrue(st.Conditions, talosv1alpha1.ConditionMatched) &&
		st.ObservedGeneration == v.tm.Generation &&
		st.Config != nil && st.Config.LiveHash == m.LiveHash && st.Config.DesiredHash == m.DesiredHash

	return m
}

// planRollout runs while no operation is in flight: it records which configs the operator
// stands behind, asks the planner whether a rollout is due, and starts it by taking the lock.
// Nothing is sent to Talos here; the first node step begins in a later reconcile.
func (r *TalosClusterReconciler) planRollout(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation,
	pools map[string]*talosv1alpha1.TalosMachinePool, machines []talosv1alpha1.TalosMachine, timing plan.Timing,
) error {
	st := &tc.Status
	planned := make([]plan.Machine, 0, len(machines))

	for i := range machines {
		v, err := r.view(ctx, tc, obs.facts, pools, &machines[i])
		if err != nil {
			return err
		}

		m := planMachine(v)

		// A node that runs its rendered config is the baseline drift is measured from: at the
		// handover, and whenever the spec is brought in line with the node.
		if m.Current && m.LiveHash == m.DesiredHash && (m.AppliedHash != m.DesiredHash || m.SkippedHash != "") {
			if err := r.recordApplied(ctx, v.tm, m.DesiredHash); err != nil {
				return err
			}

			m.AppliedHash, m.SkippedHash = m.DesiredHash, ""
		}

		planned = append(planned, m)
	}

	result := plan.PlanRollout(planned, plan.Settings{
		DriftPolicy:    string(tc.Spec.Rollout.DriftPolicy),
		Paused:         tc.Spec.Paused,
		ApprovedChange: tc.Annotations[talosv1alpha1.AnnotationApproveDiff],
		AbortedTargets: st.AbortedTargets,
		Policy:         obs.policy,
	}, passOf(obs))

	if !result.AbortStands {
		st.AbortedTargets = nil
	}

	if result.MayStart() {
		if err := timing.Validate(); err != nil {
			result.BlockReason, result.BlockMessage = talosv1alpha1.ReasonInvalidRollout, "spec.rollout: "+err.Error()
		}
	}

	st.Approval = nil
	if result.BlockReason == plan.BlockAwaitingApproval {
		st.Approval = &talosv1alpha1.PendingApproval{Hash: result.ChangeHash, Machines: approvals(result, planned)}
	}

	if result.MayStart() {
		now := metav1.NewTime(r.now())
		st.Operation = &talosv1alpha1.Operation{
			ID:        fmt.Sprintf("%s-%d", result.ChangeHash, now.Unix()),
			Type:      talosv1alpha1.OperationConfigRollout,
			Target:    result.ChangeHash,
			Plan:      result.Pending,
			Targets:   result.Targets,
			Bases:     result.Bases,
			StartedAt: now,
		}
	}

	reportPlan(tc, obs, result, planned)

	return nil
}

// approvals describes the machines whose change needs approval, for status.approval.
func approvals(result plan.Rollout, planned []plan.Machine) []string {
	out := make([]string, 0, len(result.NeedsApproval))

	for _, m := range planned {
		if slices.Contains(result.NeedsApproval, m.Name) {
			out = append(out, fmt.Sprintf("%s: %s (%s)", m.Name, m.Class, strings.Join(m.Reasons, ", ")))
		}
	}

	return out
}

// reportPlan sets ConfigInSync and RolloutBlocked while no operation is in flight.
func reportPlan(tc *talosv1alpha1.TalosCluster, obs observation, result plan.Rollout, planned []plan.Machine) {
	st := &tc.Status
	gen := tc.Generation

	inSync := func(status metav1.ConditionStatus, reason, format string, args ...any) {
		setCondition(st, gen, talosv1alpha1.ConditionConfigInSync, status, reason, fmt.Sprintf(format, args...))
	}

	st.Phase = talosv1alpha1.ClusterPhaseManaged
	meta.RemoveStatusCondition(&st.Conditions, talosv1alpha1.ConditionRolloutBlocked)

	var stale []string

	for _, m := range planned {
		if !m.Current {
			stale = append(stale, m.Name)
		}
	}

	pending := strings.Join(result.Pending, ", ")

	switch {
	case st.Operation != nil:
		inSync(metav1.ConditionFalse, talosv1alpha1.ReasonRollingOut, "rolling out to %s", pending)
	case len(result.Pending) > 0:
		inSync(metav1.ConditionFalse, result.BlockReason, "%s to be rolled out: %s", pending, result.BlockMessage)

		// A machine whose status is catching up is a matter of seconds, not something to act on.
		if result.BlockReason != plan.BlockNotCurrent {
			setCondition(st, gen, talosv1alpha1.ConditionRolloutBlocked, metav1.ConditionTrue, result.BlockReason,
				fmt.Sprintf("the rollout to %s does not start: %s", pending, result.BlockMessage))
		}
	case len(result.Drifted) > 0:
		inSync(metav1.ConditionFalse, talosv1alpha1.ReasonDrift,
			"%s no longer run the config the operator applied; driftPolicy Report leaves them as they are (see each machine's status.config.diff)",
			strings.Join(result.Drifted, ", "))
	case len(result.Held) > 0:
		inSync(metav1.ConditionFalse, talosv1alpha1.ReasonHeld, "%s do not run their rendered config and are on hold or were skipped",
			strings.Join(result.Held, ", "))
	case len(stale) > 0:
		inSync(metav1.ConditionUnknown, talosv1alpha1.ReasonAwaitingStatus, "waiting for current status of %s", strings.Join(stale, ", "))
	case obs.unmatched == nil:
		inSync(metav1.ConditionUnknown, talosv1alpha1.ReasonAwaitingStatus, "live members could not all be matched against TalosMachines")
	case len(obs.unmatched) > 0:
		inSync(metav1.ConditionFalse, talosv1alpha1.ReasonMismatch, "live members without a TalosMachine: %s", strings.Join(obs.unmatched, ", "))
	default:
		inSync(metav1.ConditionTrue, talosv1alpha1.ReasonSucceeded, "all %d nodes run their rendered config", len(planned))
	}
}

// reportHeld sets RolloutBlocked for an operation in flight that begins no new node, because it
// is paused or no maintenance window is open, and clears it otherwise. finishes says what is
// still done.
func reportHeld(tc *talosv1alpha1.TalosCluster, policy plan.Policy, finishes string) {
	st, gen := &tc.Status, tc.Generation

	switch {
	case tc.Spec.Paused:
		setCondition(st, gen, talosv1alpha1.ConditionRolloutBlocked, metav1.ConditionTrue, talosv1alpha1.ReasonPaused,
			"spec.paused is set: "+finishes)
	case policy.OutsideWindow != "":
		reason := talosv1alpha1.ReasonOutsideWindow
		if policy.WindowsInvalid {
			reason = talosv1alpha1.ReasonInvalidRollout
		}

		setCondition(st, gen, talosv1alpha1.ConditionRolloutBlocked, metav1.ConditionTrue, reason, policy.OutsideWindow+": "+finishes)
	default:
		meta.RemoveStatusCondition(&st.Conditions, talosv1alpha1.ConditionRolloutBlocked)
	}
}

// reportOperation sets ConfigInSync and RolloutBlocked for the operation in flight. note is what
// the step in flight waits for, if anything.
func reportOperation(tc *talosv1alpha1.TalosCluster, obs observation, note string) {
	st := &tc.Status
	gen := tc.Generation

	op := st.Operation
	if op == nil {
		// It just ended. The next reconcile plans, and reports what is left.
		return
	}

	if f := op.Failure; f != nil {
		msg := fmt.Sprintf("the rollout halted at %s, step %s (%s): %s", f.Machine, f.Step, f.Reason, f.Message)

		setCondition(st, gen, talosv1alpha1.ConditionConfigInSync, metav1.ConditionFalse, talosv1alpha1.ReasonOperationFailed, msg)
		setCondition(st, gen, talosv1alpha1.ConditionRolloutBlocked, metav1.ConditionTrue, talosv1alpha1.ReasonOperationFailed,
			msg+" "+f.Remediation)
		st.Phase = talosv1alpha1.ClusterPhaseDegraded

		return
	}

	st.Phase = talosv1alpha1.ClusterPhaseManaged

	setCondition(st, gen, talosv1alpha1.ConditionConfigInSync, metav1.ConditionFalse, talosv1alpha1.ReasonRollingOut, progressOf(op, note))
	reportHeld(tc, obs.policy, "a node whose config is being tried is still verified and confirmed, no other node is touched")
}

// continueRollout carries the operation in flight one step further. It returns what the step
// waits for.
func (r *TalosClusterReconciler) continueRollout(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation,
	pools map[string]*talosv1alpha1.TalosMachinePool, machines []talosv1alpha1.TalosMachine, timing plan.Timing,
) (string, error) {
	op := tc.Status.Operation

	// view looks a planned machine up. A machine that no longer exists has an empty view: no
	// node, nothing rendered.
	view := func(name string) (machineView, error) {
		for i := range machines {
			if machines[i].Name == name {
				return r.view(ctx, tc, obs.facts, pools, &machines[i])
			}
		}

		return machineView{}, nil
	}

	return r.drive(ctx, tc, obs, timing, turns{
		begin: func(key string, attempt int32) error {
			v, err := view(key)
			if err != nil {
				return err
			}

			r.begin(tc, key, v, attempt, timing)

			return nil
		},
		step: func() (string, error) {
			v, err := view(op.Current.Machine)
			if err != nil {
				return "", err
			}

			return r.step(ctx, tc, obs, v, timing)
		},
		failed:    func() error { return r.actOnFailure(ctx, tc, view, timing) },
		skip:      func(machine string) error { return r.skipConfig(ctx, tc, view, machine) },
		abort:     func() { abortConfig(tc) },
		nodeHooks: true,
	})
}

// abortConfig records what an aborted rollout was applying. What was left is not rolled out again
// as it is: only a spec that renders something else gets a rollout going.
func abortConfig(tc *talosv1alpha1.TalosCluster) {
	op := tc.Status.Operation

	aborted := maps.Clone(op.Targets)
	for _, done := range op.Completed {
		delete(aborted, done)
	}

	tc.Status.AbortedTargets = aborted
}

// skipConfig records on a machine that a human left its rendered config out, so that it is not
// rolled out to it again.
func (r *TalosClusterReconciler) skipConfig(ctx context.Context, tc *talosv1alpha1.TalosCluster, view func(string) (machineView, error),
	machine string,
) error {
	v, err := view(machine)
	if err != nil || v.tm == nil {
		return err
	}

	hash := tc.Status.Operation.Targets[machine]

	return r.patchMachineConfig(ctx, v.tm, func(cfg *talosv1alpha1.ConfigState) { cfg.SkippedHash = hash })
}

// begin persists the first step of a machine's turn. Nothing is sent to Talos in the same
// reconcile.
//
// An operation applies exactly what was planned: the change from the config each node ran to the
// config its spec rendered. That pair is what was classified and, where needed, approved. If
// either side moved, the operation ends and the rest is planned afresh.
func (r *TalosClusterReconciler) begin(tc *talosv1alpha1.TalosCluster, name string, v machineView, attempt int32, timing plan.Timing) {
	op := tc.Status.Operation
	now := metav1.NewTime(r.now())
	base, desired := op.Bases[name], op.Targets[name]

	step := &talosv1alpha1.OperationStep{
		Machine:      name,
		Step:         plan.FirstStep(true),
		Class:        string(v.change.Class),
		PreviousHash: base,
		DesiredHash:  desired,
		Attempt:      attempt,
		Since:        now,
	}

	switch {
	case v.tm == nil:
		// The TalosMachine is gone, so there is nothing left to configure.
		skipped(op, name)
	case v.tm.Spec.Hold, v.tm.Spec.DesiredState == talosv1alpha1.DesiredStateRemoved:
		skipped(op, name)
	case v.desired != nil && v.desired.Hash != desired:
		r.end(tc, talosv1alpha1.OperationSuperseded, name+" renders a different config than this rollout planned")
	case v.live != nil && v.live.Hash == desired:
		// Nothing to apply, but the rollout only moves past a node it has seen healthy.
		step.Step, step.PreviousHash, step.Class = plan.FirstStep(false), desired, ""
		op.Current = step
	case v.live != nil && v.live.Hash != base:
		r.end(tc, talosv1alpha1.OperationSuperseded, "the config of "+name+" changed after this rollout was planned")
	default:
		// The deadline is persisted before the try is sent, and the try is sent with a timeout
		// that ends at the deadline or later. Talos's own timer therefore never ends first.
		deadline := metav1.NewTime(now.Add(timing.TryTimeout))
		step.Deadline = &deadline
		op.Current = step
	}
}

// step asks the planner what the node step in flight needs next, and does it.
func (r *TalosClusterReconciler) step(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation, v machineView,
	timing plan.Timing,
) (string, error) {
	op := tc.Status.Operation
	cur := op.Current
	now := r.now()

	s := plan.Step{
		Name:         cur.Step,
		Tried:        cur.PreviousHash != cur.DesiredHash,
		PreviousHash: cur.PreviousHash,
		DesiredHash:  cur.DesiredHash,
		Since:        cur.Since.Time,
		Deadline:     timeOf(cur.Deadline),
		HealthySince: timeOf(cur.HealthySince),
	}
	node := stepNode(v)

	// A node nothing was sent to is left alone while the rollout is paused or outside its
	// maintenance windows, or when its spec renders something else by now. The step is dropped;
	// the machine's turn begins again.
	hold := obs.policy.Hold(tc.Spec.Paused)
	if plan.Untouched(s, node, now) && (!hold.Open || (v.desired != nil && v.desired.Hash != cur.DesiredHash)) {
		op.Current = nil

		return hold.Reason, nil
	}

	pre := r.preGate(tc, obs, v)
	checks := plan.Gate{Reason: notVerifiedYet}

	if cur.Step == plan.StepVerifying && node.Reachable {
		var err error
		if checks, err = r.verifyNode(ctx, obs, v, cur.Since.Time, now); err != nil {
			return "", err
		}

		checks = checks.And(obs.policy.Gates())
	}

	action := plan.NextStep(s, node, pre, checks, timing, now)
	stamp := metav1.NewTime(now)

	if action.Kind != plan.Wait {
		// The trail of a node step: what the planner answered to what was observed. Hashes only;
		// a config is never logged.
		logf.FromContext(ctx).Info("node step", "machine", cur.Machine, "step", cur.Step, "action", action.Kind.String(),
			"next", action.Step, "reason", action.Reason, "detail", action.Message,
			"reachable", node.Reachable, "activeHash", node.ActiveHash, "persistentHash", node.PersistentHash,
			"previousHash", cur.PreviousHash, "desiredHash", cur.DesiredHash, "gate", pre.Note)
	}

	switch action.Kind { //nolint:exhaustive // the actions of an upgrade step never come out of a config step
	case plan.Wait:
		return action.Message, nil
	case plan.Advance:
		cur.Step, cur.Since, cur.HealthySince = action.Step, stamp, nil
	case plan.MarkHealthy:
		cur.HealthySince = &stamp
	case plan.MarkUnhealthy:
		cur.HealthySince = nil

		return action.Message, nil
	case plan.Rearm:
		deadline := metav1.NewTime(now.Add(timing.TryTimeout))
		cur.Deadline = &deadline
	case plan.Try:
		return r.try(ctx, tc, obs, v, now)
	case plan.Confirm:
		return r.confirm(ctx, tc, obs, v)
	case plan.Complete:
		if err := r.recordApplied(ctx, v.tm, cur.DesiredHash); err != nil {
			return "", err
		}

		verified(tc, true)
	case plan.Fail:
		failOperation(op, action.Reason, action.Message, now)
	}

	return "", nil
}

func timeOf(t *metav1.Time) time.Time {
	if t == nil {
		return time.Time{}
	}

	return t.Time
}

// stepNode is what the planner may know about the node of the step in flight. Anything that
// could not be read, identity included, makes the node unreachable: nothing is concluded from
// a partial view.
func stepNode(v machineView) plan.Node {
	if v.node == nil || v.live == nil {
		return plan.Node{}
	}

	n := plan.Node{Reachable: true, ActiveHash: v.live.Hash}

	if len(v.node.persistentConfig) > 0 {
		hash, err := configHash(v.node.persistentConfig)
		if err != nil {
			return plan.Node{}
		}

		n.PersistentHash = hash
	}

	return n
}

// configHash is the normalized hash of a raw machine config.
func configHash(raw []byte) (string, error) {
	parsed, err := talos.ParseMachineConfig(raw)
	if err != nil {
		return "", err
	}

	normalized, err := render.Normalize(parsed)
	if err != nil {
		return "", err
	}

	return normalized.Hash, nil
}

// preGate is what must hold before a config is sent to the node: nothing holds the rollout back,
// the config still renders, every other node is healthy (plan.TouchGate) and the user's gates
// are open. The node's own health is not asked about, so that a node a config broke can be given
// another.
func (r *TalosClusterReconciler) preGate(tc *talosv1alpha1.TalosCluster, obs observation, v machineView) plan.Gate {
	hold := obs.policy.Hold(tc.Spec.Paused)

	switch {
	case !hold.Open:
		return hold
	case v.tm == nil:
		return plan.Gate{Reason: "the TalosMachine no longer exists"}
	case v.desired == nil:
		return plan.Gate{Reason: "the machine's config does not render: " + v.problem}
	}

	return plan.TouchGate(passOf(obs), v.hostname()).And(obs.policy.Gates())
}

// latest reports whether tc is the TalosCluster as the API server has it now. A side effect
// based on anything older might repeat one a newer status already accounts for, or ignore a spec
// change such as paused.
func (r *TalosClusterReconciler) latest(ctx context.Context, tc *talosv1alpha1.TalosCluster) bool {
	if r.APIReader == nil {
		return true
	}

	current := &talosv1alpha1.TalosCluster{}
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(tc), current); err != nil {
		return false
	}

	return current.ResourceVersion == tc.ResourceVersion
}

const (
	noteCatchingUp = "waiting for the operator's view of the TalosCluster to catch up"
	// notVerifiedYet is what a node's checks say before they were run.
	notVerifiedYet = "not verified yet"
)

// try validates the node's rendered config with a dry run and applies it in try mode. The
// planner only asks for this while the node still runs its previous config: Talos takes
// whatever is active as the config to fall back to, so a second try would fall back to the
// first (see talos.ConfigWriter).
func (r *TalosClusterReconciler) try(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation, v machineView, now time.Time,
) (string, error) {
	op := tc.Status.Operation
	cur := op.Current

	if v.desired.Hash != cur.DesiredHash {
		// Untouched nodes are dropped before the planner is asked; this only guards the invariant
		// that nothing but the planned config is ever sent.
		return "the machine renders a different config than the one being rolled out", nil
	}

	return r.tryConfig(ctx, tc, obs, v, v.desired.BytesOrderedLike(v.live), now)
}

// tryConfig validates raw with a dry run on the node of the step in flight and applies it in try
// mode, with a timeout that ends at the step's persisted deadline or later.
func (r *TalosClusterReconciler) tryConfig(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation, v machineView, raw []byte,
	now time.Time,
) (string, error) {
	op := tc.Status.Operation
	cur := op.Current

	if !r.latest(ctx, tc) {
		return noteCatchingUp, nil
	}

	writer, err := r.Resolver.ResolveWriter(ctx, tc, obs.endpoints)
	if err != nil {
		//nolint:nilerr // what the step waits for; the planner's timeouts end it if this lasts
		return "no Talos client to apply config with: " + err.Error(), nil
	}

	if _, err := writer.DryRunApply(ctx, v.node.address, raw); err != nil {
		return r.applyFailed(ctx, op, v, "Talos refused the config in a dry run, so nothing was applied", err, now), nil
	}

	// Measured from before the deadline's own clock reading, so Talos's timer ends after it.
	if err := writer.ApplyConfig(ctx, v.node.address, raw, talos.ApplyTry, cur.Deadline.Sub(now)); err != nil {
		return r.applyFailed(ctx, op, v, "Talos refused the config, so nothing was applied", err, now), nil
	}

	return "", nil
}

// confirm makes a tried and verified config persistent by applying the node's active config
// again without try mode, which also stops Talos's rollback timer.
func (r *TalosClusterReconciler) confirm(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation, v machineView,
) (string, error) {
	op := tc.Status.Operation
	cur := op.Current

	if !r.latest(ctx, tc) {
		return noteCatchingUp, nil
	}

	writer, err := r.Resolver.ResolveWriter(ctx, tc, obs.endpoints)
	if err != nil {
		return "no Talos client to confirm the config with: " + err.Error(), nil
	}

	// The planner checked the margin against a clock reading taken before the node was verified.
	// Check again, right before the call that must not come late.
	now := r.now()
	if !now.Before(cur.Deadline.Add(-plan.ConfirmMargin)) {
		return "too close to the end of the try window to confirm", nil
	}

	callCtx, cancel := context.WithTimeout(ctx, confirmCallTimeout)
	defer cancel()

	if err := writer.ApplyConfig(callCtx, v.node.address, v.node.activeConfig, talos.ApplyNoReboot, 0); err != nil {
		// Talos stops its rollback timer before it validates a confirm, so a refused confirm may
		// leave the tried config active for good. The planner reports what the node does next.
		return r.applyFailed(ctx, op, v, "Talos refused to confirm the tried config, which may now stay active without being persistent",
			err, now), nil
	}

	return "", nil
}

// applyFailed handles an error from Talos while applying. A config Talos refuses fails the
// operation at once; anything else is tried again until the planner's own timeouts end the step.
// It returns what the step waits for.
func (r *TalosClusterReconciler) applyFailed(ctx context.Context, op *talosv1alpha1.Operation, v machineView, what string, err error,
	now time.Time,
) string {
	// What Talos says about a config may quote it.
	detail := err.Error()
	if v.withhold {
		detail = "the error is withheld because a config source is a Secret"
	}

	if talos.IsRejected(err) {
		failOperation(op, plan.FailRejected, what+": "+detail, now)

		return ""
	}

	logf.FromContext(ctx).Info("applying config failed, will retry", "machine", v.tm.Name, "error", detail)

	return "the last attempt to reach the node failed: " + detail
}

// failOperation halts the operation at its step in flight until a human acts (docs/scope.md §8.3).
func failOperation(op *talosv1alpha1.Operation, reason, message string, now time.Time) {
	cur := op.Current
	at := metav1.NewTime(now)

	// The token names this one failure. Annotations must carry it, so one left behind from an
	// earlier failure never acts on this one.
	sum := sha256.Sum256(fmt.Appendf(nil, "%s/%s/%s/%d/%d", op.ID, cur.Machine, cur.Step, cur.Attempt, now.UnixNano()))
	token := hex.EncodeToString(sum[:4])

	op.Failure = &talosv1alpha1.OperationFailure{
		Machine:     cur.Machine,
		Step:        cur.Step,
		Reason:      reason,
		Message:     message,
		Remediation: remediation(reason, token, op.Type != talosv1alpha1.OperationKubernetesUpgrade),
		Token:       token,
		At:          &at,
	}
}

// remediation says what a human can do about a failed step. A Kubernetes upgrade cannot leave a
// node out, so it offers no skip (docs/scope.md §7.4).
func remediation(reason, token string, skippable bool) string {
	var first string

	switch reason {
	case plan.FailRolledBack:
		first = "The node runs its previous config. Find out why it did not stay healthy under the new one, and fix the spec."
	case plan.FailNodeUnreachableAfterRollback:
		first = "The state of the node is unknown: check its console."
	case plan.FailConfigChanged:
		first = "Something other than this operator changed the node's config."
	case plan.FailRejected:
		first = "Fix the config in the spec."
	case plan.FailImagePull:
		first = "Nothing on the node changed. Check that the image exists and that the node can pull it."
	case plan.FailInstall:
		first = "The node still runs what it ran. Find out why the installer failed."
	case plan.FailDrainTimeout:
		first = "The node stays cordoned. Find out which pods could not be evicted; a PodDisruptionBudget may never allow it."
	case plan.FailFellBack:
		first = "The node runs its previous system again, and may still be cordoned. " +
			"Find out why the new one did not boot: its console has the answer."
	default:
		first = "Find out what kept the step from finishing."
	}

	skip := ""
	if skippable {
		skip = fmt.Sprintf(" %s=%s to leave this machine out,", talosv1alpha1.AnnotationSkipMachine, token)
	}

	return fmt.Sprintf("%s Then annotate the TalosCluster with %s=%s to run the step again,%s or %s=%s to end the operation.",
		first, talosv1alpha1.AnnotationRetry, token, skip, talosv1alpha1.AnnotationAbort, token)
}

// actOnFailure carries out what a human asked for after a failure. The annotations only count
// when they carry the failure's token.
func (r *TalosClusterReconciler) actOnFailure(ctx context.Context, tc *talosv1alpha1.TalosCluster, view func(string) (machineView, error),
	timing plan.Timing,
) error {
	st := &tc.Status
	op := st.Operation
	f := op.Failure
	asked := func(annotation string) bool { return f.Token != "" && tc.Annotations[annotation] == f.Token }

	switch {
	case asked(talosv1alpha1.AnnotationAbort):
		abortConfig(tc)
		r.end(tc, talosv1alpha1.OperationAborted, fmt.Sprintf("aborted after failing at %s (%s): %s", f.Machine, f.Reason, f.Message))
	case asked(talosv1alpha1.AnnotationSkipMachine):
		if err := r.skipConfig(ctx, tc, view, f.Machine); err != nil {
			return err
		}

		skipped(op, f.Machine)
	case asked(talosv1alpha1.AnnotationRetry):
		v, err := view(f.Machine)
		if err != nil {
			return err
		}

		attempt := int32(1)
		if op.Current != nil {
			attempt = op.Current.Attempt + 1
		}

		// The step starts over from what the node runs now.
		op.Failure, op.Current = nil, nil
		r.begin(tc, f.Machine, v, attempt, timing)
	}

	return nil
}

// finish ends the operation, releasing the lock, and records it in the history.
func (r *TalosClusterReconciler) finish(tc *talosv1alpha1.TalosCluster, outcome, message string) {
	st := &tc.Status
	op := st.Operation

	record := talosv1alpha1.OperationRecord{
		ID:         op.ID,
		Type:       op.Type,
		Outcome:    outcome,
		StartedAt:  op.StartedAt,
		FinishedAt: metav1.NewTime(r.now()),
		Completed:  op.Completed,
		Skipped:    op.Skipped,
		Message:    message,
	}

	st.History = append([]talosv1alpha1.OperationRecord{record}, st.History...)
	if len(st.History) > maxHistory {
		st.History = st.History[:maxHistory]
	}

	st.Operation = nil
}

// recordApplied notes on the machine that the operator stands behind hash on its node: it put it
// there, or took it over. A config that was skipped earlier is no longer skipped.
func (r *TalosClusterReconciler) recordApplied(ctx context.Context, tm *talosv1alpha1.TalosMachine, hash string) error {
	if tm == nil {
		return nil
	}

	return r.patchMachineConfig(ctx, tm, func(cfg *talosv1alpha1.ConfigState) { cfg.AppliedHash, cfg.SkippedHash = hash, "" })
}

// patchMachineConfig changes the fields of a TalosMachine's status.config that the cluster's
// rollout owns. The machine's own controller never sets them, and a merge patch leaves alone
// what it does set.
func (r *TalosClusterReconciler) patchMachineConfig(ctx context.Context, tm *talosv1alpha1.TalosMachine,
	mutate func(*talosv1alpha1.ConfigState),
) error {
	original := tm.DeepCopy()

	if tm.Status.Config == nil {
		tm.Status.Config = &talosv1alpha1.ConfigState{}
	}

	mutate(tm.Status.Config)

	if equality.Semantic.DeepEqual(original.Status, tm.Status) {
		return nil
	}

	if err := r.Status().Patch(ctx, tm, client.MergeFrom(original)); err != nil {
		return fmt.Errorf("patch status of TalosMachine %s: %w", tm.Name, err)
	}

	return nil
}

// abandonOperation ends an operation because config management returned to Observe. Nothing may
// be written to Talos any more, so a config that is being tried is neither confirmed nor
// withdrawn: Talos reverts it by itself when its window ends.
func (r *TalosClusterReconciler) abandonOperation(tc *talosv1alpha1.TalosCluster) {
	st := &tc.Status

	if st.Operation != nil {
		r.finish(tc, talosv1alpha1.OperationAborted, "config management returned to Observe"+leftCordoned(st.Operation))
	}

	st.Approval, st.AbortedTargets, st.Upgrades = nil, nil, nil
	meta.RemoveStatusCondition(&st.Conditions, talosv1alpha1.ConditionTalosUpToDate)
	meta.RemoveStatusCondition(&st.Conditions, talosv1alpha1.ConditionKubernetesUpToDate)
}

// operationEndpoints selects the Talos API endpoints of a cluster: never the node an operation is
// changing, which is reached through another node instead (docs/scope.md §8.5.1). Every
// controller selects through it, so they share one connection.
func operationEndpoints(ctx context.Context, c client.Reader, tc *talosv1alpha1.TalosCluster) talos.SelectOptions {
	op := tc.Status.Operation
	if op == nil || op.Current == nil {
		return talos.SelectOptions{}
	}

	tm := &talosv1alpha1.TalosMachine{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: tc.Namespace, Name: op.Current.Machine}, tm); err != nil {
		return talos.SelectOptions{}
	}

	addr, err := netip.ParseAddr(tm.Spec.Address)
	if err != nil {
		return talos.SelectOptions{}
	}

	return talos.SelectOptions{Exclude: addr}
}

// resolveOutside resolves tc's Talos client through the endpoints opts selects. A cluster with a
// single control plane has no other node to go through, so the exclusion is then dropped. It
// returns the options that were used.
func resolveOutside(ctx context.Context, resolver *target.Resolver, tc *talosv1alpha1.TalosCluster, opts talos.SelectOptions,
) (*target.Target, talos.SelectOptions, error) {
	tgt, err := resolver.Resolve(ctx, tc, opts)
	if errors.Is(err, talos.ErrNoEndpoints) && opts.Exclude.IsValid() {
		opts = talos.SelectOptions{}
		tgt, err = resolver.Resolve(ctx, tc, opts)
	}

	return tgt, opts, err
}

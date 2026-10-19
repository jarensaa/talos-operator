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
	"errors"
	"fmt"
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
	"github.com/jarensaa/talos-operator/internal/plan"
	"github.com/jarensaa/talos-operator/internal/talos"
	"github.com/jarensaa/talos-operator/internal/target"
)

// confirmCallTimeout bounds the call that confirms a tried config. A confirm is only sent
// while plan.ConfirmMargin of the try window is left; a call that could outlast the margin
// might land after Talos reverted the config, and would then apply it with no way back.
const confirmCallTimeout = plan.ConfirmMargin / 2

// The config rollout (docs/scope.md §6.3, §8.5.1). It decides nothing about safety itself: it
// observes, asks a planner in internal/plan, and carries out the one action the planner answers
// with, as every kind of operation does (operation.go).

// planMachine is the planner's view of a machine. It is Current only when the machine's own
// controller reports the very configs seen here, so the diff a human reads in the machine's
// status is the change that would be rolled out.
func planMachine(v machineView) plan.Machine {
	st := v.tm.Status
	m := plan.Machine{Name: v.tm.Name, Node: v.hostname(), Hold: v.held(), ControlPlane: v.controlPlane()}

	if st.Config != nil {
		m.AppliedHash = st.Config.AppliedHash
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

// planRollout owns ConfigInSync and RolloutBlocked while no operation is in flight: it records
// which configs the operator stands behind, asks the planner whether a rollout is due, and starts
// it by taking the lock.
func (r *TalosClusterReconciler) planRollout(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation, f *fleet) error {
	st := &tc.Status
	planned := make([]plan.Machine, 0, len(f.members))

	for i := range f.members {
		v, err := f.view(ctx, &f.members[i])
		if err != nil {
			return err
		}

		m := planMachine(v)

		// A node that runs its rendered config is the baseline drift is measured from: at the
		// handover, and whenever the spec is brought in line with the node.
		if m.Current && m.LiveHash == m.DesiredHash && m.AppliedHash != m.DesiredHash {
			if err := r.recordApplied(ctx, v.tm, m.DesiredHash); err != nil {
				return err
			}

			m.AppliedHash = m.DesiredHash
		}

		planned = append(planned, m)
	}

	// A skip covers one rendered config of one machine. It is spent once the machine renders
	// another, runs what it renders, or is gone.
	hold(tc, talosv1alpha1.OperationConfigRollout, func(h *talosv1alpha1.Holds) {
		for name, hash := range h.Skipped {
			at := slices.IndexFunc(planned, func(m plan.Machine) bool { return m.Name == name })
			if at < 0 || (planned[at].Current && (planned[at].DesiredHash != hash || planned[at].LiveHash == hash)) {
				delete(h.Skipped, name)
			}
		}
	})

	holds := held(tc, talosv1alpha1.OperationConfigRollout)
	result := plan.PlanRollout(planned, plan.Settings{
		DriftPolicy:    tc.Spec.Rollout.DriftPolicy,
		ApprovedChange: tc.Annotations[talosv1alpha1.AnnotationApproveDiff],
		Skipped:        holds.Skipped,
		Aborted:        holds.Aborted,
		Policy:         obs.policy,
	}, passOf(obs))

	// An abort covers the configs that were being rolled out. Once the spec renders anything
	// else, a revert included, it is spent.
	if !result.AbortStands {
		hold(tc, talosv1alpha1.OperationConfigRollout, func(h *talosv1alpha1.Holds) { clear(h.Aborted) })
	}

	st.Approval = nil
	if result.BlockReason == plan.BlockAwaitingApproval {
		st.Approval = &talosv1alpha1.PendingApproval{Hash: result.Change, Machines: approvals(result, planned)}
	}

	// An operation applies exactly what was planned: the change from the config each node ran to
	// the config its spec rendered.
	if op := r.start(tc, talosv1alpha1.OperationConfigRollout, result.Due); op != nil {
		op.Bases = result.Bases
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

// rolloutTurns is what the turns of a config rollout do. A machine that is no longer a member has
// an empty view: no node, nothing rendered.
func (r *TalosClusterReconciler) rolloutTurns(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation, f *fleet,
	timing plan.Timing,
) turns {
	return turns{
		begin: func(name string, attempt int32) error {
			v, err := f.view(ctx, f.member(name))
			if err != nil {
				return err
			}

			r.begin(tc, name, v, attempt, timing)

			return nil
		},
		step: func() (string, error) {
			v, err := f.view(ctx, f.member(tc.Status.Operation.Current.Machine))
			if err != nil {
				return "", err
			}

			return r.step(ctx, tc, obs, v, timing)
		},
	}
}

// begin persists the first step of a machine's turn. Nothing is sent to Talos in the same
// reconcile.
//
// An operation applies exactly what was planned: the change from the config each node ran to the
// config its spec rendered. That pair is what was classified and, where needed, approved. If
// either side moved, the operation ends and the rest is planned afresh.
func (r *TalosClusterReconciler) begin(tc *talosv1alpha1.TalosCluster, name string, v machineView, attempt int32, timing plan.Timing) {
	op := tc.Status.Operation
	now := r.stamp()
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
	case v.tm == nil, v.held():
		// The TalosMachine is gone, on hold or on its way out: there is nothing to configure.
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
	s, node := stepOf(cur), withServices(ctx, obs, v, cur, stepNode(v))

	// A node nothing was sent to is left alone while the rollout is paused or outside its
	// maintenance windows, or when its spec renders something else by now. The step is dropped;
	// the machine's turn begins again.
	hold := obs.policy.Hold()
	if plan.Untouched(s, node, now) && (!hold.Open || (v.desired != nil && v.desired.Hash != cur.DesiredHash)) {
		op.Current = nil

		return hold.Reason, nil
	}

	pre := r.preGate(obs, v)

	checks, err := r.verified(ctx, obs, v.boundMachine, cur, node.Reachable, now)
	if err != nil {
		return "", err
	}

	action := plan.NextStep(s, node, pre, checks, timing, now)
	logAnswer(ctx, op, action, pre, "reachable", node.Reachable, "activeHash", node.ActiveHash, "persistentHash", node.PersistentHash,
		"previousHash", cur.PreviousHash, "desiredHash", cur.DesiredHash)

	if note, done := answered(op, action, now); done {
		return note, nil
	}

	switch action.Kind {
	case plan.Rearm:
		deadline := metav1.NewTime(now.Add(timing.TryTimeout))
		cur.Deadline = &deadline
	case plan.Try:
		if v.desired.Hash != cur.DesiredHash {
			// Untouched nodes are dropped before the planner is asked; this only guards the
			// invariant that nothing but the planned config is ever sent.
			return "the machine renders a different config than the one being rolled out", nil
		}

		return r.tryConfig(ctx, tc, obs, v, v.desired.BytesOrderedLike(v.live), now)
	case plan.Confirm:
		return r.confirm(ctx, tc, obs, v)
	case plan.Complete:
		if err := r.recordApplied(ctx, v.tm, cur.DesiredHash); err != nil {
			return "", err
		}

		completed(tc)
	default:
		return "", unexpected(op, action)
	}

	return "", nil
}

// notVerifiedYet is what a node's checks say before they were run.
const notVerifiedYet = "not verified yet"

// verified is the verification of the node of the step in flight, with the user's gates: closed
// until the step is verifying a node that answers.
func (r *TalosClusterReconciler) verified(ctx context.Context, obs observation, m boundMachine, cur *talosv1alpha1.OperationStep,
	reachable bool, now time.Time,
) (plan.Gate, error) {
	if cur.Step != plan.StepVerifying || !reachable {
		return plan.Gate{Reason: notVerifiedYet}, nil
	}

	checks, err := r.verifyNode(ctx, obs, m, cur.Since.Time, now)
	if err != nil {
		return plan.Gate{}, err
	}

	return checks.And(obs.policy.Gates()), nil
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

// withServices reads, of a node back on the config its try started from, whether the services it
// needs run (plan.Node). They are asked about then and only then: a rolled-back node is not
// reported as recovered without them, and nothing else depends on them.
func withServices(ctx context.Context, obs observation, v machineView, cur *talosv1alpha1.OperationStep, node plan.Node) plan.Node {
	if cur.Step != plan.StepRollingBack || !node.Reachable || node.ActiveHash != cur.PreviousHash {
		return node
	}

	if obs.reader == nil {
		node.ServicesUnread = "the cluster's Talos API was not reached"

		return node
	}

	var services []talos.ServiceStatus

	reads := newNodeReads(ctx, v.node.address)
	if !reads.read("services", func(ctx context.Context) (err error) {
		services, err = obs.reader.Services(ctx, v.node.address)

		return err
	}) {
		node.ServicesUnread = reads.reason()

		return node
	}

	node.ServicesRead = true
	node.ServicesDown = strings.Join(talos.ServicesDown(v.node.machineType, services), ", ")

	return node
}

// preGate is what must hold before a config is sent to the node: nothing holds the rollout back,
// the config still renders, every other node is healthy (plan.TouchGate) and the user's gates
// are open. The node's own health is not asked about, so that a node a config broke can be given
// another.
func (r *TalosClusterReconciler) preGate(obs observation, v machineView) plan.Gate {
	hold := obs.policy.Hold()

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

// tryConfig validates raw with a dry run on the node of the step in flight and applies it in try
// mode, with a timeout that ends at the step's persisted deadline or later. The planner only asks
// for this while the node still runs its previous config: Talos takes whatever is active as the
// config to fall back to, so a second try would fall back to the first (see talos.ConfigWriter).
func (r *TalosClusterReconciler) tryConfig(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation, v machineView, raw []byte,
	now time.Time,
) (string, error) {
	op := tc.Status.Operation

	writer, note := clientTo(ctx, r, tc, obs, "apply config with", r.Resolver.ResolveWriter)
	if note != "" {
		return note, nil
	}

	if _, err := writer.DryRunApply(ctx, v.node.address, raw); err != nil {
		return r.applyFailed(ctx, op, v, "Talos refused the config in a dry run, so nothing was applied", err, now), nil
	}

	// Measured from before the deadline's own clock reading, so Talos's timer ends after it.
	if err := writer.ApplyConfig(ctx, v.node.address, raw, talos.ApplyTry, op.Current.Deadline.Sub(now)); err != nil {
		return r.applyFailed(ctx, op, v, "Talos refused the config, so nothing was applied", err, now), nil
	}

	return "", nil
}

// confirm makes a tried and verified config persistent by applying the node's active config
// again without try mode, which also stops Talos's rollback timer.
func (r *TalosClusterReconciler) confirm(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation, v machineView,
) (string, error) {
	op := tc.Status.Operation

	writer, note := clientTo(ctx, r, tc, obs, "confirm the config with", r.Resolver.ResolveWriter)
	if note != "" {
		return note, nil
	}

	// The planner checked the margin against a clock reading taken before the node was verified.
	// Check again, right before the call that must not come late.
	now := r.now()
	if !now.Before(op.Current.Deadline.Add(-plan.ConfirmMargin)) {
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

// recordApplied notes on the machine that the operator stands behind hash on its node: it put it
// there, or took it over.
func (r *TalosClusterReconciler) recordApplied(ctx context.Context, tm *talosv1alpha1.TalosMachine, hash string) error {
	if tm == nil {
		return nil
	}

	return r.patchMachine(ctx, tm, func(st *talosv1alpha1.TalosMachineStatus) {
		if st.Config == nil {
			st.Config = &talosv1alpha1.ConfigState{}
		}

		st.Config.AppliedHash = hash
	})
}

// patchMachine changes the fields of a TalosMachine's status that the cluster's operations own:
// the config the operator stands behind, and when the machine left. The machine's own controller
// never sets them, and a merge patch leaves alone what it does set.
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

// abandonOperation ends an operation because config management returned to Observe. Nothing may
// be written to Talos any more, so a config that is being tried is neither confirmed nor
// withdrawn: Talos reverts it by itself when its window ends.
func (r *TalosClusterReconciler) abandonOperation(tc *talosv1alpha1.TalosCluster) {
	st := &tc.Status

	if st.Operation != nil {
		r.finish(tc, talosv1alpha1.OperationAborted, "config management returned to Observe"+leftCordoned(st.Operation))
	}

	st.Approval, st.Holds = nil, nil

	// What is no longer looked after is no longer reported on.
	for _, k := range kinds {
		meta.RemoveStatusCondition(&st.Conditions, k.condition)
	}
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

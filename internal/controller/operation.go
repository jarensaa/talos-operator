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
	"fmt"
	"net/netip"
	"slices"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/plan"
	"github.com/jarensaa/talos-operator/internal/talos"
)

// What every kind of operation has in common (docs/scope.md §6.4): how it takes the lock, the
// order of its turns and the hooks around them, how it is reported, what a human may ask for
// after it failed, and how it ends. plan.NextTurn decides; this file persists what it answers.
//
// What a turn consists of is the kind's own business: a config change, a Talos upgrade, one
// component of a Kubernetes upgrade, a machine that joins or leaves, a member of etcd that is
// defragmented. A kind says what it is called and what it may do in its entry of kinds, and what
// its turns do in the turns it builds for each pass. Everything else it gets from here, so that
// no kind can do it differently.
//
// Three rules keep the executors honest (docs/scope.md §6.3, §8.5.1):
//
//   - a step is persisted in status.operation by one reconcile and acted on by a later one, so
//     a replica taking over finds every side effect announced before it happened;
//   - a side effect is only sent after reading the TalosCluster back from the API server and
//     finding it unchanged, so it is never based on a stale view of the step or the spec;
//   - what a step does next is decided from what the node reports now, never from memory.

const (
	// rolloutInterval is how often a node step in flight is looked at again. The step is driven
	// by time (the try window, the soak) as much as by events.
	rolloutInterval = 5 * time.Second
	// maxHistory bounds status.history, as the CRD does.
	maxHistory = 10
)

// kind is what is the same about every operation of one kind: what it is called, which condition
// reports on it, and what it may do.
type kind struct {
	// name is what an operation of the kind is called in a message: "the rollout". toward, when
	// set, is how its progress is spoken of with its target ("upgrading to"), and the name then
	// carries the target too.
	name, toward string
	// condition reports on operations of the kind, and running is its reason while one is in
	// flight.
	condition, running string
	// finishes says what an operation that begins no new turn still does.
	finishes string
	// allowed reports whether spec.management still lets operations of the kind run, and
	// disabled says that it no longer does. An operation in flight ends then. Nil means the kind
	// needs nothing beyond config management.
	allowed  func(talosv1alpha1.Management) bool
	disabled string
	// event names the kind in the reasons of its Events, done is the reason of a turn that is
	// done, and does what is then true of it.
	event, done, does string
	// nodeHooks is false for a kind that takes no node out of the cluster: its turns run without
	// preNode and postNode hooks.
	nodeHooks bool
	// skippable is false for a kind that cannot leave a machine out.
	skippable bool
	// rebootable is true for a kind whose failure at a member may be answered with a reboot of it
	// (reboot.go). A machine that joins is not a member yet, and one that leaves is being reset.
	rebootable bool
	// remembers is false for a kind that has nothing to remember about a turn a human left out
	// or an operation a human ended.
	remembers bool
	// turns builds what the kind's turns do in this pass.
	turns func(*TalosClusterReconciler, context.Context, *talosv1alpha1.TalosCluster, observation, *fleet, plan.Timing) turns
}

// kinds is every kind of operation there is. An operation of a type that is not here is none this
// operator carries on with.
var kinds map[talosv1alpha1.OperationType]kind

// The table is filled in here and not where it is declared because a kind's turns end up asking
// the table about their own kind, which Go does not let a variable's initializer do.
func init() {
	kinds = map[talosv1alpha1.OperationType]kind{
		talosv1alpha1.OperationConfigRollout: {
			name:      "the rollout",
			condition: talosv1alpha1.ConditionConfigInSync, running: talosv1alpha1.ReasonRollingOut,
			finishes: "a node whose config is being tried is still verified and confirmed, no other node is touched",
			event:    "Rollout", done: "MachineConfigured", does: "runs its rendered config",
			nodeHooks: true, skippable: true, rebootable: true, remembers: true,
			turns: (*TalosClusterReconciler).rolloutTurns,
		},
		talosv1alpha1.OperationTalosUpgrade: {
			name:      "the upgrade",
			condition: talosv1alpha1.ConditionTalosUpToDate, running: talosv1alpha1.ReasonUpgrading,
			finishes: "a node that is already cordoned is still upgraded and uncordoned, no other node is touched",
			allowed:  talosv1alpha1.Management.MayUpgrade, disabled: "upgrades were disabled",
			event: "Upgrade", done: "MachineUpgraded", does: "runs the installed system",
			nodeHooks: true, skippable: true, rebootable: true, remembers: true,
			turns: (*TalosClusterReconciler).upgradeTurns,
		},
		talosv1alpha1.OperationKubernetesUpgrade: {
			name: "the upgrade to Kubernetes", toward: "upgrading to",
			condition: talosv1alpha1.ConditionKubernetesUpToDate, running: talosv1alpha1.ReasonUpgrading,
			finishes: "a component whose config is being tried is still verified and confirmed, nothing else is changed",
			// A component whose config is being tried goes back by itself, as in a config rollout.
			allowed: talosv1alpha1.Management.MayUpgrade, disabled: "upgrades were disabled",
			event: "KubernetesUpgrade", done: "ComponentUpgraded", does: "runs the new version",
			// A Kubernetes upgrade cannot leave a node out: what follows would be newer than it.
			rebootable: true, remembers: true,
			turns: (*TalosClusterReconciler).kubernetesTurns,
		},
		talosv1alpha1.OperationAddMachine: {
			name:      "adding machines",
			condition: talosv1alpha1.ConditionMembershipCurrent, running: talosv1alpha1.ReasonAdding,
			finishes: "a machine that was handed its config is still seen to join, no other machine is added",
			allowed:  talosv1alpha1.Management.MayChangeMembership, disabled: "membership was disabled",
			event: "Addition", done: "MachineAdded", does: "is a member of the cluster",
			skippable: true, remembers: true,
			turns: (*TalosClusterReconciler).joinTurns,
		},
		talosv1alpha1.OperationRemoveMachine: {
			name:      "removing machines",
			condition: talosv1alpha1.ConditionMembershipCurrent, running: talosv1alpha1.ReasonRemoving,
			finishes: "a node that is already cordoned is still taken out, no other node is touched",
			allowed:  talosv1alpha1.Management.MayChangeMembership, disabled: "membership was disabled",
			event: "Removal", done: "MachineRemoved", does: "has left the cluster",
			nodeHooks: true, skippable: true, remembers: true,
			turns: (*TalosClusterReconciler).removalTurns,
		},
		talosv1alpha1.OperationEtcdDefragment: {
			name:      "defragmenting etcd",
			condition: talosv1alpha1.ConditionEtcdMaintained, running: talosv1alpha1.ReasonDefragmenting,
			finishes: "a member that was defragmented is still verified, no other member is defragmented",
			allowed:  talosv1alpha1.Management.MayMaintain, disabled: "maintenance was disabled",
			event: "Defragmentation", done: "MemberDefragmented", does: "has a defragmented etcd database",
			// Nothing has to be remembered about a member that was left out, or about an operation
			// that was ended: the interval keeps the next one away.
			skippable: true, rebootable: true,
			turns: (*TalosClusterReconciler).defragTurns,
		},
	}
}

// called is what the operation is called in a message.
func (k kind) called(op *talosv1alpha1.Operation) string {
	if k.toward != "" {
		return k.name + " " + op.Target
	}

	return k.name
}

// turns is what a kind of operation does in its turns, in this pass.
type turns struct {
	// begin persists the first step of a turn. Nothing is sent anywhere in the same reconcile. A
	// turn that has nothing left to do is skipped, and one that would no longer do what was
	// planned ends the operation, so that the rest is planned afresh.
	begin func(key string, attempt int32) error
	// step carries the step in flight further, and returns what it waits for.
	step func() (string, error)
	// abort records what an operation that a human ended leaves behind, for a kind that remembers
	// something else than the turns that were left. Nil for every other.
	abort func()
	// also carries out what a human may ask for after a failure of this kind and of no other. It
	// is handed how to tell whether an annotation carries the failure's token, and reports
	// whether something was asked. Nil for a kind that has nothing of its own.
	also func(asked func(annotation string) bool) bool
}

// carryOn carries the operation in flight a step further, whatever its kind, and reports on it.
func (r *TalosClusterReconciler) carryOn(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation, f *fleet) error {
	op := tc.Status.Operation

	k, known := kinds[op.Type]
	if !known {
		// Nothing is done, and the lock is kept: what the operation did to its node is not known.
		return fmt.Errorf("the operation in flight is a %s, which this operator does not know how to carry on with", op.Type)
	}

	if k.allowed != nil && !k.allowed(tc.Spec.Management) {
		// Nothing may be written any more, so what is in flight is neither finished nor undone.
		r.finish(tc, talosv1alpha1.OperationAborted, k.disabled+leftCordoned(op))

		return nil
	}

	timing := plan.TimingOf(tc.Spec.Rollout)

	if rebooting(op) {
		note, err := r.rebootStep(ctx, tc, obs, f, timing)

		reportInFlight(tc, obs, k, note)

		return err
	}
	note, err := r.drive(ctx, tc, obs, timing, k, k.turns(r, ctx, tc, obs, f, timing))

	reportInFlight(tc, obs, k, note)

	return err
}

// start takes the lock for an operation that is due and may start, and returns it; nil when none
// starts. Nothing is sent anywhere in the same reconcile: its first turn begins in a later one.
func (r *TalosClusterReconciler) start(tc *talosv1alpha1.TalosCluster, of talosv1alpha1.OperationType, due plan.Due,
) *talosv1alpha1.Operation {
	if !due.MayStart() {
		return nil
	}

	now := r.stamp()
	tc.Status.Operation = &talosv1alpha1.Operation{
		ID:        fmt.Sprintf("%s-%d", due.Change, now.Unix()),
		Type:      of,
		Target:    due.Change,
		Plan:      due.Pending,
		Targets:   due.Targets,
		StartedAt: now,
	}

	return tc.Status.Operation
}

// heldUp is the condition that says an operation is due and does not start; nil when none is, or
// when all it waits for is a machine's status to catch up, which is a matter of seconds and
// nothing to act on.
func heldUp(due plan.Due, format string, args ...any) *metav1.Condition {
	if len(due.Pending) == 0 || due.BlockReason == "" || due.BlockReason == plan.BlockNotCurrent {
		return nil
	}

	return &metav1.Condition{
		Type: talosv1alpha1.ConditionRolloutBlocked, Status: metav1.ConditionTrue, Reason: due.BlockReason,
		Message: fmt.Sprintf(format, args...) + ": " + due.BlockMessage,
	}
}

// reportInFlight sets the kind's condition and RolloutBlocked for the operation in flight. note
// is what the step in flight waits for, if anything.
func reportInFlight(tc *talosv1alpha1.TalosCluster, obs observation, k kind, note string) {
	st, gen := &tc.Status, tc.Generation

	op := st.Operation
	if op == nil {
		// It just ended. The next reconcile plans, and reports what is left.
		return
	}

	if f := op.Failure; f != nil {
		msg := fmt.Sprintf("%s halted %s: %s", k.called(op), haltedAt(f), f.Message)

		setCondition(st, gen, k.condition, metav1.ConditionFalse, talosv1alpha1.ReasonOperationFailed, msg)
		setCondition(st, gen, talosv1alpha1.ConditionRolloutBlocked, metav1.ConditionTrue, talosv1alpha1.ReasonOperationFailed,
			msg+" "+f.Remediation)
		st.Phase = talosv1alpha1.ClusterPhaseDegraded

		return
	}

	st.Phase = talosv1alpha1.ClusterPhaseManaged

	progress := progressOf(op, note)
	if k.toward != "" {
		progress = fmt.Sprintf("%s %s: %s", k.toward, op.Target, progress)
	}

	setCondition(st, gen, k.condition, metav1.ConditionFalse, k.running, progress)
	reportHeld(tc, obs.policy, k.finishes)
}

// reportHeld sets RolloutBlocked for an operation in flight that begins no new turn, because it
// is paused or no maintenance window is open, and clears it otherwise. finishes says what is
// still done.
func reportHeld(tc *talosv1alpha1.TalosCluster, policy plan.Policy, finishes string) {
	st, gen := &tc.Status, tc.Generation

	switch {
	case policy.Paused:
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

// nextTurn is the first turn of the plan that is neither completed nor skipped; empty if none is left.
func nextTurn(op *talosv1alpha1.Operation) string {
	for _, key := range op.Plan {
		if !slices.Contains(op.Completed, key) && !slices.Contains(op.Skipped, key) {
			return key
		}
	}

	return ""
}

// turnOf is the plan entry a step belongs to.
func turnOf(step *talosv1alpha1.OperationStep) string {
	if step.Component != "" {
		return plan.TurnKey(step.Component, step.Machine)
	}

	return step.Machine
}

// drive carries the operation in flight one step further: a hook, a node's step, the next node,
// or its end. It returns what the operation waits for.
func (r *TalosClusterReconciler) drive(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation, timing plan.Timing,
	k kind, t turns,
) (string, error) {
	op := tc.Status.Operation

	if f := op.Failure; f != nil {
		if f.Hook != "" {
			r.actOnHookFailure(tc, k, t)

			return "", nil
		}

		return "", r.actOnFailure(tc, k, t)
	}

	hooks := hookCounts(tc, k.nodeHooks)
	hold := obs.policy.Hold()

	progress := plan.Progress{
		Announced: op.Announced, Ending: op.Ending != nil, Prepared: op.Prepared, Finishing: op.Finishing, Next: nextTurn(op),
	}
	if op.Current != nil {
		progress.Current = turnOf(op.Current)
	}

	if h := op.Hook; h != nil {
		progress.Hook, progress.HookIndex, progress.HookTurn = h.Stage, int(h.Index), h.Machine
	}

	turn := plan.NextTurn(progress, hooks, hold)
	if turn.Kind == plan.TurnHook {
		done, note, err := r.runHook(ctx, tc, timing)
		if err != nil || !done {
			return note, err
		}

		progress.HookDone = true
		turn = plan.NextTurn(progress, hooks, hold)
	}

	switch turn.Kind {
	case plan.TurnWait:
		return turn.Reason, nil
	case plan.TurnHook:
		// Answered above; a hook that is through is not carried on with.
	case plan.TurnStartHook:
		//nolint:gosec // the index of a list the CRD bounds at eight
		r.startHook(tc, turn.Stage, int32(turn.Index), turn.Turn, 1)
	case plan.TurnAnnounce:
		op.Announced, op.Hook = true, nil
	case plan.TurnPrepared:
		op.Prepared, op.Hook = turn.Turn, nil
	case plan.TurnBegin:
		return "", t.begin(turn.Turn, 1)
	case plan.TurnStep:
		return t.step()
	case plan.TurnComplete:
		op.Completed = append(op.Completed, turn.Turn)
		op.Finishing, op.Prepared, op.Hook = "", "", nil
	case plan.TurnEnd:
		r.end(tc, talosv1alpha1.OperationSucceeded, "")
	case plan.TurnFinish:
		outcome, message := talosv1alpha1.OperationSucceeded, ""
		if op.Ending != nil {
			outcome, message = op.Ending.Outcome, op.Ending.Message
		}

		r.finish(tc, outcome, message)
	}

	return "", nil
}

// stepOf is the step in flight as the planners read it.
func stepOf(cur *talosv1alpha1.OperationStep) plan.Step {
	return plan.Step{
		Name:         cur.Step,
		Since:        cur.Since.Time,
		HealthySince: timeOf(cur.HealthySince),
		PreviousHash: cur.PreviousHash,
		DesiredHash:  cur.DesiredHash,
		Deadline:     timeOf(cur.Deadline),
		BootID:       cur.PreRebootBootID,
		Cordoned:     cur.Cordoned,
		Dead:         cur.Dead,
	}
}

func timeOf(t *metav1.Time) time.Time {
	if t == nil {
		return time.Time{}
	}

	return t.Time
}

// answered carries out what a planner answered for the step in flight, where that is the same for
// every kind of step: waiting, moving on to the next step, the soak, the record of a cordon, and
// failing. It reports whether the answer was one of those, and what the step then waits for. What
// is left is the kind's own to do: its side effects, and what completes its turn.
func answered(op *talosv1alpha1.Operation, action plan.Action, now time.Time) (string, bool) {
	cur := op.Current
	stamp := metav1.NewTime(now)

	switch action.Kind {
	case plan.Wait:
		return action.Message, true
	case plan.Advance:
		cur.Step, cur.Since, cur.HealthySince = action.Step, stamp, nil
	case plan.MarkHealthy:
		cur.HealthySince = &stamp
	case plan.MarkUnhealthy:
		cur.HealthySince = nil

		return action.Message, true
	case plan.MarkCordoned:
		cur.Cordoned = true
	case plan.Fail:
		failOperation(op, action.Reason, action.Message, now)
	default:
		return "", false
	}

	return "", true
}

// unexpected is the error of an answer a kind of step does not know what to do with. Nothing is
// done about it, and nothing is concluded from it: the pass fails, and the step stays where it is.
func unexpected(op *talosv1alpha1.Operation, action plan.Action) error {
	return fmt.Errorf("the planner answered %s for step %s of a %s, which that step does not carry out", action.Kind, op.Current.Step, op.Type)
}

// logAnswer leaves the trail of a step: what the planner answered to what was observed. Waiting
// is not logged, and neither is a config, ever: hashes only.
func logAnswer(ctx context.Context, op *talosv1alpha1.Operation, action plan.Action, pre plan.Gate, observed ...any) {
	if action.Kind == plan.Wait {
		return
	}

	cur := op.Current
	logf.FromContext(ctx).Info("step", append([]any{
		"operation", op.Type, "machine", cur.Machine, "component", cur.Component, "step", cur.Step, "action", action.Kind.String(),
		"next", action.Step, "reason", action.Reason, "detail", action.Message, "gate", pre.Note,
	}, observed...)...)
}

// noteCatchingUp is what a side effect waits for while this replica's view of the TalosCluster is
// not the newest.
const noteCatchingUp = "waiting for the operator's view of the TalosCluster to catch up"

// clientTo returns the Talos client a side effect is sent with, of the kind resolve hands out. It
// is only handed out from the TalosCluster as the API server has it now. Otherwise the note says
// what the step waits for; the planner's own timeouts end the step if that lasts.
func clientTo[C any](ctx context.Context, r *TalosClusterReconciler, tc *talosv1alpha1.TalosCluster, obs observation, purpose string,
	resolve func(context.Context, *talosv1alpha1.TalosCluster, talos.SelectOptions) (C, error),
) (C, string) {
	var none C

	if !r.latest(ctx, tc) {
		return none, noteCatchingUp
	}

	c, err := resolve(ctx, tc, obs.endpoints)
	if err != nil {
		return none, "no Talos client to " + purpose + ": " + err.Error()
	}

	return c, ""
}

// retried says that a side effect did not get through and is sent again in a later pass.
func retried(ctx context.Context, op *talosv1alpha1.Operation, what string, err error) string {
	logf.FromContext(ctx).Info("a side effect failed, will retry", "what", what, "machine", op.Current.Machine, "error", err.Error())

	return "the last attempt to " + what + " failed: " + err.Error()
}

// handOver asks a control plane to give up etcd leadership before it is taken away for a while.
// It is a courtesy to the cluster, not a condition: etcd elects a new leader either way, only
// later, and a single control plane has nobody to hand over to.
func handOver(ctx context.Context, through interface {
	ForfeitEtcdLeadership(ctx context.Context, node netip.Addr) error
}, node netip.Addr, machine string,
) {
	if err := through.ForfeitEtcdLeadership(ctx, node); err != nil {
		logf.FromContext(ctx).Info("etcd leadership was not handed over", "machine", machine, "error", err.Error())
	}
}

// completed marks the turn in flight as done. For a kind with node hooks it counts as completed
// once its postNode hooks ran.
func completed(tc *talosv1alpha1.TalosCluster) {
	op := tc.Status.Operation
	key := turnOf(op.Current)
	op.Current = nil

	if kinds[op.Type].nodeHooks && len(hooksAt(tc, talosv1alpha1.HookPostNode)) > 0 {
		op.Finishing = key

		return
	}

	op.Completed = append(op.Completed, key)
	op.Prepared = ""
}

// skipped leaves a turn out of the operation.
func skipped(op *talosv1alpha1.Operation, key string) {
	op.Skipped = append(op.Skipped, key)
	op.Failure, op.Current, op.Hook, op.Prepared, op.Finishing = nil, nil, nil, "", ""
}

// leaveOut is a human's word that a turn is left out: it is skipped, and what it was to reach is
// remembered, so that the planner does not come back to it as it is.
func leaveOut(tc *talosv1alpha1.TalosCluster, k kind, key string) {
	op := tc.Status.Operation

	if k.remembers {
		hold(tc, op.Type, func(h *talosv1alpha1.Holds) { h.Skipped[key] = op.Targets[key] })
	}

	skipped(op, key)
}

// abort is a human's word that the operation ends: what it had left to do is remembered, so that
// the planner does not start it again as it is.
func abort(tc *talosv1alpha1.TalosCluster, k kind, t turns) {
	op := tc.Status.Operation

	switch {
	case !k.remembers:
	case t.abort != nil:
		t.abort()
	default:
		// Added to what was aborted before: a machine an earlier operation was ended at is not
		// let go because a later one was ended too.
		hold(tc, op.Type, func(h *talosv1alpha1.Holds) {
			for _, key := range op.Plan {
				if !slices.Contains(op.Completed, key) {
					h.Aborted[key] = op.Targets[key]
				}
			}
		})
	}
}

// held is what a human took out of the operations of a kind.
func held(tc *talosv1alpha1.TalosCluster, of talosv1alpha1.OperationType) talosv1alpha1.Holds {
	return tc.Status.Holds[of]
}

// hold changes what is remembered of what a human took out of the operations of a kind. Nothing
// is kept of a kind that has nothing left to remember.
func hold(tc *talosv1alpha1.TalosCluster, of talosv1alpha1.OperationType, change func(*talosv1alpha1.Holds)) {
	h := tc.Status.Holds[of]
	if h.Skipped == nil {
		h.Skipped = map[string]string{}
	}

	if h.Aborted == nil {
		h.Aborted = map[string]string{}
	}

	change(&h)

	if len(h.Skipped) == 0 {
		h.Skipped = nil
	}

	if len(h.Aborted) == 0 {
		h.Aborted = nil
	}

	if h.Skipped == nil && h.Aborted == nil {
		delete(tc.Status.Holds, of)

		if len(tc.Status.Holds) == 0 {
			tc.Status.Holds = nil
		}

		return
	}

	if tc.Status.Holds == nil {
		tc.Status.Holds = map[talosv1alpha1.OperationType]talosv1alpha1.Holds{}
	}

	tc.Status.Holds[of] = h
}

// end is the end of an operation's work: nothing more is done to any node. If it announced itself
// with preRollout hooks, its postRollout hooks run before the lock is released, however it ended.
func (r *TalosClusterReconciler) end(tc *talosv1alpha1.TalosCluster, outcome, message string) {
	op := tc.Status.Operation
	op.Current, op.Failure, op.Hook, op.Finishing, op.Prepared = nil, nil, nil, "", ""

	if op.Announced && len(hooksAt(tc, talosv1alpha1.HookPostRollout)) > 0 {
		op.Ending = &talosv1alpha1.OperationEnding{Outcome: outcome, Message: message}

		return
	}

	r.finish(tc, outcome, message)
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
		FinishedAt: r.stamp(),
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

// leftCordoned says that the operation leaves a node behind that it cordoned itself.
func leftCordoned(op *talosv1alpha1.Operation) string {
	if op.Current != nil && op.Current.Cordoned {
		return "; " + op.Current.Machine + " stays cordoned"
	}

	return ""
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
		Remediation: remediation(reason, token, kinds[op.Type].skippable, kinds[op.Type].rebootable && cur.Machine != ""),
		Token:       token,
		At:          &at,
	}
}

// remediation says what a human can do about a failed step. A kind that cannot leave a machine
// out offers no skip (docs/scope.md §7.4), and only a failure at a member offers a reboot.
func remediation(reason, token string, skippable, rebootable bool) string {
	var first string

	switch reason {
	case plan.FailRolledBack:
		first = "The node runs its previous config. Find out why it did not stay healthy under the new one, and fix the spec."
	case plan.FailRolledBackUnhealthy:
		first = "The node runs its previous config, but not the services it needs. The support bundle has their logs and " +
			"the kernel's: find out what stopped them, whether the new config or something before it, and fix the spec if " +
			"the new config is the cause. A reboot of the node may bring them back."
	case plan.FailRebootNotAllowed:
		first = "Nothing more was sent to the node."
	case plan.FailRebooted:
		first = "The node was rebooted as asked, and is healthy again. Say what follows for the step it had failed at."
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
	case plan.FailIdentity:
		first = "The machine was sent nothing. Check which machine answers at the address, and what spec.identity.systemUUID says."
	case plan.FailDoesNotFit:
		first = "The machine was sent nothing. Fix the install disk or the links in the config, or the machine."
	case plan.FailNotJoined:
		first = "The machine took its config and did not become a member. Its console says how far it got; " +
			"to start it over, bring it back to maintenance mode."
	case plan.FailNodeGone, plan.FailNotWiped:
		first = fmt.Sprintf("If the machine is gone for good, annotate the TalosCluster with %s=%s: what is left of it in etcd and in "+
			"Kubernetes is then taken out, and nothing on the machine is wiped, so it must not be switched on again as it is. "+
			"Otherwise bring it back.", talosv1alpha1.AnnotationConfirmDead, token)
	case plan.FailUnknownStep:
		first = "The step is none this version of the operator knows: it was begun by another version. Run that version again " +
			"to finish it, or end the operation."
	default:
		first = "Find out what kept the step from finishing."
	}

	skip := ""
	if skippable {
		skip = fmt.Sprintf(" %s=%s to leave this machine out,", talosv1alpha1.AnnotationSkipMachine, token)
	}

	reboot := ""
	if rebootable {
		reboot = fmt.Sprintf(" %s=%s to reboot the machine and then decide (with spec.management.upgrades: Enabled),",
			talosv1alpha1.AnnotationReboot, token)
	}

	return fmt.Sprintf("%s Then annotate the TalosCluster with %s=%s to run the step again,%s%s or %s=%s to end the operation.",
		first, talosv1alpha1.AnnotationRetry, token, reboot, skip, talosv1alpha1.AnnotationAbort, token)
}

// tokened returns how to tell whether an annotation of the TalosCluster carries the token of the
// failure the operation halted at. Only such an annotation counts: one left behind from an
// earlier failure never acts on this one.
func tokened(tc *talosv1alpha1.TalosCluster) func(annotation string) bool {
	f := tc.Status.Operation.Failure

	return func(annotation string) bool { return f.Token != "" && tc.Annotations[annotation] == f.Token }
}

// actOnFailure carries out what a human asked for after a step failed: end the operation, leave
// the machine out, something only the kind offers, or run the turn again.
func (r *TalosClusterReconciler) actOnFailure(tc *talosv1alpha1.TalosCluster, k kind, t turns) error {
	op := tc.Status.Operation
	f := op.Failure
	asked := tokened(tc)

	switch {
	case asked(talosv1alpha1.AnnotationAbort):
		abort(tc, k, t)
		r.end(tc, talosv1alpha1.OperationAborted, fmt.Sprintf("aborted after failing %s: %s%s", haltedAt(f), f.Message, leftCordoned(op)))
	case k.skippable && asked(talosv1alpha1.AnnotationSkipMachine):
		leaveOut(tc, k, f.Machine)
	case t.also != nil && t.also(asked):
	case k.rebootable && f.Machine != "" && tc.Spec.Management.MayUpgrade() && asked(talosv1alpha1.AnnotationReboot):
		rebootAfter(op, r.stamp())
	case asked(talosv1alpha1.AnnotationRetry):
		key, attempt, cordoned := f.Machine, int32(1), false
		if cur := op.Current; cur != nil {
			key, attempt, cordoned = turnOf(cur), cur.Attempt+1, cur.Cordoned
		}

		// The turn starts over from what its node says now. If that cannot be told in this pass,
		// the failure stands as it was.
		failed, current := op.Failure, op.Current
		op.Failure, op.Current = nil, nil

		if err := t.begin(key, attempt); err != nil {
			op.Failure, op.Current = failed, current

			return err
		}

		// A cordon the operator set stays its own to undo.
		if op.Current != nil {
			op.Current.Cordoned = cordoned
		}
	}

	return nil
}

// actOnHookFailure carries out what a human asked for after a hook failed.
func (r *TalosClusterReconciler) actOnHookFailure(tc *talosv1alpha1.TalosCluster, k kind, t turns) {
	op := tc.Status.Operation
	f, h := op.Failure, op.Hook
	asked := tokened(tc)

	if h == nil {
		// A failure that names a hook without one running cannot be retried; it can only end.
		if asked(talosv1alpha1.AnnotationAbort) {
			r.finish(tc, talosv1alpha1.OperationAborted, f.Message)
		}

		return
	}

	switch {
	case asked(talosv1alpha1.AnnotationAbort) && h.Stage == talosv1alpha1.HookPostRollout:
		// The operation is over already; what ends here is its cleaning up.
		message := fmt.Sprintf("its %s hook %s failed and the rest were not run: %s", h.Stage, h.Name, f.Message)
		outcome := talosv1alpha1.OperationAborted

		if op.Ending != nil {
			outcome = op.Ending.Outcome
			if op.Ending.Message != "" {
				message = op.Ending.Message + "; " + message
			}
		}

		r.finish(tc, outcome, message)
	case asked(talosv1alpha1.AnnotationAbort):
		abort(tc, k, t)
		r.end(tc, talosv1alpha1.OperationAborted, fmt.Sprintf("aborted after the %s hook %s failed: %s", h.Stage, h.Name, f.Message))
	case asked(talosv1alpha1.AnnotationSkipMachine) && h.Stage == talosv1alpha1.HookPreNode:
		leaveOut(tc, k, h.Machine)
	case asked(talosv1alpha1.AnnotationSkipMachine) && h.Stage == talosv1alpha1.HookPostNode:
		// The node itself is done. Only the rest of its hooks is left out.
		op.Completed = append(op.Completed, h.Machine)
		op.Failure, op.Hook, op.Finishing, op.Prepared = nil, nil, "", ""
	case asked(talosv1alpha1.AnnotationRetry):
		op.Failure = nil
		r.startHook(tc, h.Stage, h.Index, h.Machine, h.Attempt+1)
	}
}

// haltedAt says where a failure halted an operation: at a machine's step, or at a hook of the
// operation as a whole, which has no machine.
func haltedAt(f *talosv1alpha1.OperationFailure) string {
	if f.Machine == "" {
		return fmt.Sprintf("at %s (%s)", f.Step, f.Reason)
	}

	return fmt.Sprintf("at %s, step %s (%s)", f.Machine, f.Step, f.Reason)
}

// progressOf describes where the operation in flight stands, for a condition. note is what it
// waits for, if anything.
func progressOf(op *talosv1alpha1.Operation, note string) string {
	progress := fmt.Sprintf("%d of %d done", len(op.Completed)+len(op.Skipped), len(op.Plan))

	switch {
	case op.Hook != nil:
		progress += fmt.Sprintf("; running the %s hook %s", op.Hook.Stage, op.Hook.Name)
		if op.Hook.Machine != "" {
			progress += " for " + op.Hook.Machine
		}
	case op.Ending != nil:
		progress += "; ending"
	case op.Current != nil && op.Current.RebootAfter != nil:
		progress += fmt.Sprintf("; %s is being rebooted as a human asked after it halted (%s), and is %s",
			op.Current.Machine, op.Current.RebootAfter.Reason, op.Current.Step)
	case op.Current != nil:
		progress += fmt.Sprintf("; %s is %s", turnOf(op.Current), op.Current.Step)
	}

	if note != "" {
		progress += ": " + note
	}

	return progress
}

// stamp is now, for a status field.
func (r *TalosClusterReconciler) stamp() metav1.Time { return metav1.NewTime(r.now()) }

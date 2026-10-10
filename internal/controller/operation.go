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
	"slices"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/plan"
)

// What every kind of operation has in common: the order of its turns, the hooks around them, and
// how it ends (docs/scope.md §6.4). plan.NextTurn decides; drive persists what it answers. What
// a turn consists of is the kind's own business: a config change, a Talos upgrade, or one
// component of a Kubernetes upgrade.

// turns is what a kind of operation contributes to the driver.
type turns struct {
	// begin persists the first step of a turn. Nothing is sent anywhere in the same reconcile.
	begin func(key string, attempt int32) error
	// step carries the step in flight further, and returns what it waits for.
	step func() (string, error)
	// failed carries out what a human asked for after a step failed.
	failed func() error
	// skip records that a human left a machine out, so the planner does not come back to it, and
	// abort that a human ended the operation, so the planner does not start it again.
	skip  func(machine string) error
	abort func()
	// nodeHooks is false for an operation that takes no node out of the cluster.
	nodeHooks bool
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
func (r *TalosClusterReconciler) drive(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation, timing plan.Timing, t turns,
) (string, error) {
	op := tc.Status.Operation

	if f := op.Failure; f != nil {
		if f.Hook != "" {
			return "", r.actOnHookFailure(tc, t)
		}

		return "", t.failed()
	}

	hooks := hookCounts(tc, t.nodeHooks)
	hold := obs.policy.Hold(tc.Spec.Paused)

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

// verified marks the turn in flight as done. With postNode hooks it counts as completed once
// they ran.
func verified(tc *talosv1alpha1.TalosCluster, nodeHooks bool) {
	op := tc.Status.Operation
	key := turnOf(op.Current)
	op.Current = nil

	if nodeHooks && len(hooksAt(tc, talosv1alpha1.HookPostNode)) > 0 {
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

// actOnHookFailure carries out what a human asked for after a hook failed. The annotations only
// count when they carry the failure's token.
func (r *TalosClusterReconciler) actOnHookFailure(tc *talosv1alpha1.TalosCluster, t turns) error {
	op := tc.Status.Operation
	f, h := op.Failure, op.Hook
	asked := func(annotation string) bool { return f.Token != "" && tc.Annotations[annotation] == f.Token }

	if h == nil {
		// A failure that names a hook without one running cannot be retried; it can only end.
		if asked(talosv1alpha1.AnnotationAbort) {
			r.finish(tc, talosv1alpha1.OperationAborted, f.Message)
		}

		return nil
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
		t.abort()
		r.end(tc, talosv1alpha1.OperationAborted, fmt.Sprintf("aborted after the %s hook %s failed: %s", h.Stage, h.Name, f.Message))
	case asked(talosv1alpha1.AnnotationSkipMachine) && h.Stage == talosv1alpha1.HookPreNode:
		if err := t.skip(h.Machine); err != nil {
			return err
		}

		skipped(op, h.Machine)
	case asked(talosv1alpha1.AnnotationSkipMachine) && h.Stage == talosv1alpha1.HookPostNode:
		// The node itself is done. Only the rest of its hooks is left out.
		op.Completed = append(op.Completed, h.Machine)
		op.Failure, op.Hook, op.Finishing, op.Prepared = nil, nil, "", ""
	case asked(talosv1alpha1.AnnotationRetry):
		op.Failure = nil
		r.startHook(tc, h.Stage, h.Index, h.Machine, h.Attempt+1)
	}

	return nil
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

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
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/plan"
)

// A reboot a human asked for after an operation halted at a machine (docs/scope.md §8.3). It is
// the driver's, like every answer to a failure: any kind whose failures may be answered with it
// gets it the same way, and none does it itself. It takes the place of the step the operation
// halted at, keeps the failure it answers, and halts the operation again once the node is back,
// with a new token. It needs spec.management.upgrades: Enabled, whose connection reboots, and is
// made of the upgrade's own steps (plan.NextRebootStep): drained behind RebootGate, rebooted while
// it still has the boot ID recorded before, back on the system it ran, uncordoned if the operator
// cordoned it, and verified like an upgraded node.

// rebooting reports whether the step in flight is one of a reboot a human asked for.
func rebooting(op *talosv1alpha1.Operation) bool {
	return op.Failure == nil && op.Current != nil && op.Current.RebootAfter != nil
}

// rebootAfter begins the reboot that answers the failure the operation halted at. Nothing is sent
// in the same reconcile. The reboot is of the same turn: its machine, its component and its
// attempt, and a cordon the operator set stays its own to undo. A reboot asked after a reboot
// failed answers the failure the first one was asked for.
func rebootAfter(op *talosv1alpha1.Operation, now metav1.Time) {
	f := op.Failure
	answers := f.DeepCopy()

	step := &talosv1alpha1.OperationStep{Machine: f.Machine, Step: plan.StepDraining, Since: now}

	if cur := op.Current; cur != nil {
		step.Component, step.Attempt, step.Cordoned = cur.Component, cur.Attempt, cur.Cordoned

		if cur.RebootAfter != nil {
			answers = cur.RebootAfter.DeepCopy()
		}
	}

	step.RebootAfter = answers
	op.Failure, op.Current = nil, step
}

// rebootStep asks the planner what the step in flight of a reboot needs next, and does it.
func (r *TalosClusterReconciler) rebootStep(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation, f *fleet,
	timing plan.Timing,
) (string, error) {
	op := tc.Status.Operation
	cur := op.Current
	now := r.now()

	if !tc.Spec.Management.MayUpgrade() {
		// Nothing more is sent: the reboot halts where it is, and says what it leaves behind.
		failOperation(op, plan.FailRebootNotAllowed, "spec.management.upgrades is no longer Enabled, which a reboot needs; "+
			"the reboot was not carried on with"+leftCordoned(op), now)

		return "", nil
	}

	m := f.of(f.member(cur.Machine))
	if m.tm == nil {
		failOperation(op, plan.FailRebootNotAllowed, cur.Machine+" is no longer a member of the cluster; the reboot was not carried on with"+
			leftCordoned(op), now)

		return "", nil
	}

	node, kube, pods, err := r.upgradeNode(ctx, m, cur)
	if err != nil {
		return "", fmt.Errorf("%w: %w", errKubernetes, err)
	}

	// The node is to come back on the system it ran, not on the one its spec asks for.
	system, known := m.system()
	node.InstallKnown, node.Installed = known && cur.System != "", system == cur.System

	// No node begins while the operation is held back; one the operator has cordoned is finished.
	pre := plan.RebootGate(passOf(obs), m.hostname()).And(obs.policy.Gates())
	if !cur.Cordoned {
		pre = obs.policy.Hold().And(pre)
	}

	checks, err := r.verified(ctx, obs, m, cur, node.Reachable, now)
	if err != nil {
		return "", err
	}

	action := plan.NextRebootStep(stepOf(cur), node, pre, checks, timing, now)
	logAnswer(ctx, op, action, pre, "reboot", true, "reachable", node.Reachable, "bootID", node.BootID,
		"recordedBootID", cur.PreRebootBootID, "system", system, "recordedSystem", cur.System, "cordoned", node.Unschedulable,
		"podsLeft", node.PodsLeft)

	if action.Kind == plan.Advance && action.Step == plan.StepRebooting {
		if !known {
			return "waiting for what the node runs to be read", nil
		}

		// The facts that will say whether the reboot happened, and onto what.
		cur.PreRebootBootID, cur.System = node.BootID, system
	}

	if note, done := answered(op, action, now); done {
		return note, nil
	}

	if note, done, err := r.drained(ctx, tc, action, kube, pods); done {
		return note, err
	}

	switch action.Kind {
	case plan.Reboot:
		return r.reboot(ctx, tc, obs, m, now)
	case plan.Complete:
		rebooted(op, now)
	default:
		return "", unexpected(op, action)
	}

	return "", nil
}

// rebooted halts the operation again once the node the human had rebooted is back and healthy, at
// the step it had failed at and with a new token: what follows is the human's to say. The node is
// uncordoned by now if the operator had cordoned it.
func rebooted(op *talosv1alpha1.Operation, now time.Time) {
	cur := op.Current
	was := cur.RebootAfter

	cur.Step, cur.RebootAfter, cur.Cordoned = was.Step, nil, false

	failOperation(op, plan.FailRebooted, fmt.Sprintf("the node was rebooted as asked and is healthy again; it had halted %s: %s",
		haltedAt(was), was.Message), now)
}

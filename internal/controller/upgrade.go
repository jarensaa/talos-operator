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
	"maps"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/plan"
	"github.com/jarensaa/talos-operator/internal/talos"
)

// The Talos upgrade (docs/scope.md §7.3). The planners in internal/plan decide; this file
// observes, asks them, and does the one thing they answer, as every kind of operation does
// (operation.go).
//
// An upgrade changes the system a node runs, not its config. The config that names the installer
// follows through an ordinary config rollout, which applies it once the node runs what it names
// (classify.Installed).

// upgradeMachine is the planner's view of a machine.
func (r *TalosClusterReconciler) upgradeMachine(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation, m boundMachine,
) plan.UpgradeMachine {
	out := plan.UpgradeMachine{Name: m.tm.Name, Node: m.hostname(), Hold: m.held(), ControlPlane: m.controlPlane()}

	known, yes := m.installed()
	if !known {
		return out
	}

	out.Current = true
	out.Installed = m.node.version + " " + *m.node.schematicID
	out.Target = m.install.Version + " " + m.install.SchematicID
	out.Image = m.install.Image
	out.EtcdLeader = m.node.etcdStatusRead && m.node.etcdLeader
	out.HostsOperator = r.NodeName != "" && m.kubernetesNode() == r.NodeName

	if !yes {
		if out.Problem = talos.UpgradeProblem(m.node.version, m.install.Version, obs.kubernetes); out.Problem == "" {
			out.Problem = r.installerProblem(ctx, tc, m.install)
		}
	}

	return out
}

// planUpgrade owns TalosUpToDate while no operation is in flight: it asks the planner whether an
// upgrade is due, and starts it by taking the lock. It reports whether it did, and, when one is
// due but may not start, why: the caller lets the config rollout report first and then says what
// really holds things up.
func (r *TalosClusterReconciler) planUpgrade(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation, f *fleet,
) (bool, *metav1.Condition) {
	planned := make([]plan.UpgradeMachine, 0, len(f.members))
	images := make(map[string]string, len(f.members))

	for i := range f.members {
		m := f.of(&f.members[i])
		planned = append(planned, r.upgradeMachine(ctx, tc, obs, m))
		images[m.tm.Name] = m.install.Image
	}

	if !tc.Spec.Management.MayUpgrade() {
		reportUpgradesDisabled(tc, planned)

		return false, nil
	}

	// A skip covers one installer. Once the spec asks for another, it is spent.
	hold(tc, talosv1alpha1.OperationTalosUpgrade, func(h *talosv1alpha1.Holds) {
		maps.DeleteFunc(h.Skipped, func(name, image string) bool { return images[name] != image })
	})

	holds := held(tc, talosv1alpha1.OperationTalosUpgrade)
	result := plan.PlanUpgrade(planned, plan.UpgradeSettings{Skipped: holds.Skipped, Aborted: holds.Aborted, Policy: obs.policy}, passOf(obs))

	// So does an abort.
	if !result.AbortStands {
		hold(tc, talosv1alpha1.OperationTalosUpgrade, func(h *talosv1alpha1.Holds) { clear(h.Aborted) })
	}

	started := r.start(tc, talosv1alpha1.OperationTalosUpgrade, result.Due) != nil

	reportUpgradePlan(tc, result, planned)

	return started, heldUp(result.Due, "the upgrade of %s does not start", strings.Join(result.Pending, ", "))
}

func outdated(planned []plan.UpgradeMachine) (behind, stale []string) {
	for _, m := range planned {
		switch {
		case !m.Current:
			stale = append(stale, m.Name)
		case m.Installed != m.Target:
			behind = append(behind, m.Name)
		}
	}

	return behind, stale
}

func reportUpgradesDisabled(tc *talosv1alpha1.TalosCluster, planned []plan.UpgradeMachine) {
	st, gen := &tc.Status, tc.Generation
	behind, stale := outdated(planned)

	switch {
	case len(behind) > 0:
		setCondition(st, gen, talosv1alpha1.ConditionTalosUpToDate, metav1.ConditionFalse, talosv1alpha1.ReasonUpgradesDisabled,
			fmt.Sprintf("%s do not run the Talos version and installer their spec asks for; set spec.management.upgrades to Enabled to upgrade them",
				strings.Join(behind, ", ")))
	case len(stale) > 0:
		setCondition(st, gen, talosv1alpha1.ConditionTalosUpToDate, metav1.ConditionUnknown, talosv1alpha1.ReasonAwaitingStatus,
			"waiting for the version and installer of "+strings.Join(stale, ", "))
	default:
		setCondition(st, gen, talosv1alpha1.ConditionTalosUpToDate, metav1.ConditionTrue, talosv1alpha1.ReasonSucceeded,
			fmt.Sprintf("all %d nodes run the Talos version and installer their spec asks for", len(planned)))
	}
}

// reportUpgradePlan sets TalosUpToDate while no upgrade is in flight.
func reportUpgradePlan(tc *talosv1alpha1.TalosCluster, result plan.Upgrade, planned []plan.UpgradeMachine) {
	st, gen := &tc.Status, tc.Generation
	_, stale := outdated(planned)
	pending := strings.Join(result.Pending, ", ")

	upToDate := func(status metav1.ConditionStatus, reason, format string, args ...any) {
		setCondition(st, gen, talosv1alpha1.ConditionTalosUpToDate, status, reason, fmt.Sprintf(format, args...))
	}

	switch {
	case st.Operation != nil:
		upToDate(metav1.ConditionFalse, talosv1alpha1.ReasonUpgrading, "upgrading %s", pending)
	case len(result.Pending) > 0:
		upToDate(metav1.ConditionFalse, result.BlockReason, "%s to be upgraded: %s", pending, result.BlockMessage)
	case len(result.Held) > 0:
		upToDate(metav1.ConditionFalse, talosv1alpha1.ReasonHeld,
			"%s do not run the Talos version and installer their spec asks for, and are on hold or were skipped", strings.Join(result.Held, ", "))
	case len(stale) > 0:
		upToDate(metav1.ConditionUnknown, talosv1alpha1.ReasonAwaitingStatus, "waiting for the version and installer of %s",
			strings.Join(stale, ", "))
	default:
		upToDate(metav1.ConditionTrue, talosv1alpha1.ReasonSucceeded, "all %d nodes run the Talos version and installer their spec asks for",
			len(planned))
	}
}

// upgradeTurns is what the turns of a Talos upgrade do. A machine that is no longer a member is
// an empty one.
func (r *TalosClusterReconciler) upgradeTurns(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation, f *fleet,
	timing plan.Timing,
) turns {
	return turns{
		begin: func(name string, attempt int32) error {
			r.beginUpgrade(tc, name, f.of(f.member(name)), attempt)

			return nil
		},
		step: func() (string, error) {
			return r.upgradeStep(ctx, tc, obs, f.of(f.member(tc.Status.Operation.Current.Machine)), timing)
		},
	}
}

// beginUpgrade persists the first step of a machine's turn. Nothing is sent anywhere in the same
// reconcile. An upgrade installs exactly the image that was planned: if the spec asks for another
// by now, the operation ends and the rest is planned afresh.
func (r *TalosClusterReconciler) beginUpgrade(tc *talosv1alpha1.TalosCluster, name string, m boundMachine, attempt int32) {
	op := tc.Status.Operation

	switch {
	case m.tm == nil, m.held():
		skipped(op, name)
	case m.install.Image != op.Targets[name]:
		r.end(tc, talosv1alpha1.OperationSuperseded, name+" is to be installed from another image than this upgrade planned")
	default:
		op.Current = &talosv1alpha1.OperationStep{
			Machine: name,
			Step:    plan.StepPulling,
			Image:   op.Targets[name],
			Attempt: attempt,
			Since:   r.stamp(),
		}
	}
}

// upgradeNode gathers what the planner may know about the step's node: from Talos what this
// pass read, from Kubernetes its Node and, while it is being drained, the pods still on it.
func (r *TalosClusterReconciler) upgradeNode(ctx context.Context, m boundMachine, cur *talosv1alpha1.OperationStep,
) (plan.UpgradeNode, *corev1.Node, []corev1.Pod, error) {
	node := plan.UpgradeNode{PodsLeft: -1}

	if m.node != nil {
		node.Reachable = m.node.systemUUID != ""
		node.BootID = m.node.bootID
		node.Running = m.running()
		node.InstallKnown, node.Installed = m.installed()
	}

	// A Node that is not there, or not readable right now: either way nothing is known of it, and
	// the planner waits.
	kube, pods, podsLeft, err := r.nodeToDrain(ctx, m.kubernetesNode(), cur)
	if err != nil || kube == nil {
		return node, nil, nil, err
	}

	node.KubernetesKnown = true
	node.Unschedulable = kube.Spec.Unschedulable
	node.NodeReady = nodeConditionTrue(kube, corev1.NodeReady) && !nodeConditionTrue(kube, corev1.NodeNetworkUnavailable)
	node.PodsLeft = podsLeft

	return node, kube, pods, nil
}

// upgradeStep asks the planner what the upgrade step in flight needs next, and does it.
func (r *TalosClusterReconciler) upgradeStep(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation, m boundMachine,
	timing plan.Timing,
) (string, error) {
	op := tc.Status.Operation
	cur := op.Current
	now := r.now()

	// A node nothing was taken from yet is left alone while the upgrade is paused or outside its
	// maintenance windows: the step is dropped, and the machine's turn begins again later.
	hold := obs.policy.Hold()
	if !hold.Open && !cur.Cordoned && slices.Contains([]string{plan.StepPulling, plan.StepInstalling, plan.StepDraining}, cur.Step) {
		op.Current = nil

		return hold.Reason, nil
	}

	node, kube, pods, err := r.upgradeNode(ctx, m, cur)
	if err != nil {
		return "", fmt.Errorf("%w: %w", errKubernetes, err)
	}

	pre := plan.RebootGate(passOf(obs), m.hostname()).And(obs.policy.Gates())

	checks, err := r.verified(ctx, obs, m, cur, node.Reachable, now)
	if err != nil {
		return "", err
	}

	action := plan.NextUpgradeStep(stepOf(cur), node, pre, checks, timing, now)
	logAnswer(ctx, op, action, pre, "image", cur.Image, "reachable", node.Reachable, "bootID", node.BootID,
		"recordedBootID", cur.PreRebootBootID, "installed", node.Installed, "cordoned", node.Unschedulable, "podsLeft", node.PodsLeft)

	if action.Kind == plan.Advance && action.Step == plan.StepRebooting {
		// The one fact that will say whether the reboot happened.
		cur.PreRebootBootID = node.BootID
	}

	if note, done := answered(op, action, now); done {
		return note, nil
	}

	if note, done, err := r.drained(ctx, tc, action, kube, pods); done {
		return note, err
	}

	switch action.Kind {
	case plan.Pull, plan.Install:
		return r.install(ctx, tc, obs, m, action.Kind, timing, now)
	case plan.Reboot:
		return r.reboot(ctx, tc, obs, m, now)
	case plan.Complete:
		completed(tc)
	default:
		return "", unexpected(op, action)
	}

	return "", nil
}

// errKubernetes marks a failure of the Kubernetes API in the middle of a step: the pass fails and
// is retried, and nothing is concluded from it.
var errKubernetes = errors.New("reading the Kubernetes API")

// moveTo persists the step that follows a side effect in the same pass as the side effect. It is
// used where what the node does next says by itself that the side effect happened, so that a pass
// that is lost in between is recognised by the planner and not repeated.
func moveTo(cur *talosv1alpha1.OperationStep, next string, now time.Time) {
	cur.Step, cur.Since, cur.HealthySince = next, metav1.NewTime(now), nil
}

// install pulls the installer image to the node, or installs it to the inactive boot slot. Both
// leave the node running what it runs, and both can be repeated, so a pass that is interrupted
// simply does them again.
func (r *TalosClusterReconciler) install(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation, m boundMachine,
	kind plan.ActionKind, timing plan.Timing, now time.Time,
) (string, error) {
	op := tc.Status.Operation
	cur := op.Current

	upgrader, note := clientTo(ctx, r, tc, obs, "upgrade with", r.Resolver.ResolveUpgrader)
	if note != "" {
		return note, nil
	}

	// Downloads and installs take minutes. The step's own timeout bounds them.
	callCtx, cancel := context.WithTimeout(ctx, timing.StepTimeout)
	defer cancel()

	what, reason, next := "pull", plan.FailImagePull, plan.StepInstalling
	call := upgrader.PullImage

	if kind == plan.Install {
		what, reason, next = "install", plan.FailInstall, plan.StepDraining
		call = upgrader.Install
	}

	if err := call(callCtx, m.node.address, cur.Image); err != nil {
		if talos.IsUnreachable(err) {
			return retried(ctx, op, "reach the node", err), nil
		}

		failOperation(op, reason, fmt.Sprintf("could not %s %s; the node still runs what it ran: %v", what, cur.Image, err), now)

		return "", nil
	}

	moveTo(cur, next, now)

	return "", nil
}

// reboot asks the node to reboot. The planner only asks for this while the node still has the
// boot ID that was recorded, and the step that follows is persisted in the same pass: a node that
// rebooted is recognised by its boot ID, whatever became of this pass.
func (r *TalosClusterReconciler) reboot(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation, m boundMachine, now time.Time,
) (string, error) {
	op := tc.Status.Operation

	upgrader, note := clientTo(ctx, r, tc, obs, "reboot with", r.Resolver.ResolveUpgrader)
	if note != "" {
		return note, nil
	}

	if m.controlPlane() {
		handOver(ctx, upgrader, m.node.address, op.Current.Machine)
	}

	if err := upgrader.Reboot(ctx, m.node.address); err != nil {
		return retried(ctx, op, "reboot the node", err), nil
	}

	moveTo(op.Current, plan.StepBooting, now)

	return "", nil
}

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
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/adopt"
	"github.com/jarensaa/talos-operator/internal/plan"
	"github.com/jarensaa/talos-operator/internal/talos"
)

// The upgrade executor (docs/scope.md §7.3). The planners in internal/plan decide; this file
// observes, asks them, and does the one thing they answer, under the same three rules as a
// config rollout (rollout.go): a step is persisted before it is acted on, a side effect is only
// sent from the TalosCluster as the API server has it now, and what a step does next is decided
// from what the node reports, never from memory.
//
// An upgrade changes the system a node runs, not its config. The config that names the installer
// follows through an ordinary config rollout, which applies it once the node runs what it names
// (classify.Installed).

// upgradeView is a TalosMachine with what an upgrade needs to know about it right now.
type upgradeView struct {
	tm   *talosv1alpha1.TalosMachine
	pool *talosv1alpha1.TalosMachinePool
	// node is the live node carrying the machine's hardware identity; nil if none answered.
	node *nodeFacts
	// install is what the spec asks the machine to be installed from; problem says why that
	// cannot be told, and install is then empty.
	install adopt.Install
	problem string
}

func (r *TalosClusterReconciler) upgradeView(tc *talosv1alpha1.TalosCluster, facts *clusterFacts,
	pools map[string]*talosv1alpha1.TalosMachinePool, tm *talosv1alpha1.TalosMachine,
) upgradeView {
	v := upgradeView{tm: tm, pool: pools[tm.Spec.PoolRef.Name], node: nodeWithIdentity(facts, tm.Spec.Identity.SystemUUID)}
	if v.pool == nil {
		v.problem = "TalosMachinePool " + tm.Spec.PoolRef.Name + " not found"

		return v
	}

	install, err := adopt.InstallTarget(&tc.Spec, &v.pool.Spec)
	if err != nil {
		v.problem = err.Error()

		return v
	}

	v.install = install

	return v
}

// hostname is the hostname of the machine's live node; empty if none answered.
func (v upgradeView) hostname() string {
	if v.node == nil {
		return ""
	}

	return v.node.hostname
}

// kubernetesNode is the name of the machine's Node, which outlives the node being unreachable:
// the machine's own controller recorded it.
func (v upgradeView) kubernetesNode() string {
	if v.tm != nil && v.tm.Status.Identity != nil && v.tm.Status.Identity.KubernetesNodeName != "" {
		return v.tm.Status.Identity.KubernetesNodeName
	}

	return v.hostname()
}

// installed reports whether the node's version and schematic were read, and whether they are the
// ones the spec asks for.
func (v upgradeView) installed() (known, yes bool) {
	if v.node == nil || v.node.version == "" || v.node.schematicID == nil || v.install.Image == "" {
		return false, false
	}

	return true, v.node.version == v.install.Version && *v.node.schematicID == v.install.SchematicID
}

// runs reports whether the node is known to run exactly what the spec asks it to be installed
// from. It is what makes naming that installer in the node's config a safe config change.
func runs(tc *talosv1alpha1.TalosCluster, pool *talosv1alpha1.TalosMachinePool, node *nodeFacts) bool {
	if pool == nil {
		return false
	}

	install, err := adopt.InstallTarget(&tc.Spec, &pool.Spec)
	if err != nil {
		return false
	}

	_, yes := upgradeView{node: node, install: install}.installed()

	return yes
}

// upgradeMachine is the planner's view of a machine.
func (r *TalosClusterReconciler) upgradeMachine(obs observation, v upgradeView) plan.UpgradeMachine {
	m := plan.UpgradeMachine{
		Name: v.tm.Name,
		Node: v.hostname(),
		Hold: v.tm.Spec.Hold || v.tm.Spec.DesiredState == talosv1alpha1.DesiredStateRemoved,
	}

	if v.pool != nil {
		m.ControlPlane = v.pool.Spec.Role == talosv1alpha1.MachineRoleControlPlane
	}

	known, yes := v.installed()
	if !known {
		return m
	}

	m.Current = true
	m.Installed = v.node.version + " " + *v.node.schematicID
	m.Target = v.install.Version + " " + v.install.SchematicID
	m.Image = v.install.Image
	m.EtcdLeader = v.node.etcdStatusRead && v.node.etcdLeader
	m.HostsOperator = r.NodeName != "" && v.kubernetesNode() == r.NodeName

	if !yes {
		m.Problem = talos.UpgradeProblem(v.node.version, v.install.Version, obs.kubernetes)
	}

	return m
}

// upgrade owns TalosUpToDate: it continues the upgrade in flight, or plans one. It reports whether
// an upgrade holds the operation lock, and, when one is due but may not start, why: the caller
// lets the config rollout report first and then says what really holds things up.
func (r *TalosClusterReconciler) upgrade(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation,
	pools map[string]*talosv1alpha1.TalosMachinePool, machines []talosv1alpha1.TalosMachine,
) (bool, *metav1.Condition, error) {
	st := &tc.Status
	timing := rolloutTiming(tc.Spec.Rollout)
	enabled := tc.Spec.Management.Upgrades == talosv1alpha1.CapabilityEnabled

	if op := st.Operation; op != nil {
		if op.Type != talosv1alpha1.OperationTalosUpgrade {
			return false, nil, nil
		}

		if !enabled {
			r.finish(tc, talosv1alpha1.OperationAborted, "upgrades were disabled"+leftCordoned(op))

			return false, nil, nil
		}

		note, err := r.continueUpgrade(ctx, tc, obs, pools, machines, timing)
		reportUpgradeOperation(tc, obs, note)

		return true, nil, err
	}

	if obs.facts == nil {
		return false, nil, nil
	}

	planned := make([]plan.UpgradeMachine, 0, len(machines))
	images := make(map[string]string, len(machines))

	for i := range machines {
		v := r.upgradeView(tc, obs.facts, pools, &machines[i])
		planned = append(planned, r.upgradeMachine(obs, v))
		images[machines[i].Name] = v.install.Image
	}

	if !enabled {
		reportUpgradesDisabled(tc, planned)

		return false, nil, nil
	}

	holds := st.Upgrades
	if holds == nil {
		holds = &talosv1alpha1.UpgradeHolds{}
	}

	result := plan.PlanUpgrade(planned, plan.UpgradeSettings{
		Paused: tc.Spec.Paused, Skipped: holds.Skipped, Aborted: holds.Aborted, Policy: obs.policy,
	}, passOf(obs))

	// A hold covers one installer. Once the spec asks for another, it is spent.
	if !result.AbortStands {
		holds.Aborted = nil
	}

	maps.DeleteFunc(holds.Skipped, func(name, image string) bool { return images[name] != image })

	st.Upgrades = keptHolds(holds)

	if result.MayStart() {
		if err := timing.Validate(); err != nil {
			result.BlockReason, result.BlockMessage = talosv1alpha1.ReasonInvalidRollout, "spec.rollout: "+err.Error()
		}
	}

	if result.MayStart() {
		now := metav1.NewTime(r.now())
		st.Operation = &talosv1alpha1.Operation{
			ID:        fmt.Sprintf("%s-%d", result.Change, now.Unix()),
			Type:      talosv1alpha1.OperationTalosUpgrade,
			Target:    result.Change,
			Plan:      result.Pending,
			Targets:   result.Images,
			StartedAt: now,
		}
	}

	return st.Operation != nil, reportUpgradePlan(tc, result, planned), nil
}

// keptHolds is holds, or nil when it holds nothing.
func keptHolds(holds *talosv1alpha1.UpgradeHolds) *talosv1alpha1.UpgradeHolds {
	if holds == nil || len(holds.Skipped)+len(holds.Aborted) == 0 && holds.Kubernetes == "" {
		return nil
	}

	return holds
}

func leftCordoned(op *talosv1alpha1.Operation) string {
	if op.Current != nil && op.Current.Cordoned {
		return "; " + op.Current.Machine + " was being upgraded and stays cordoned"
	}

	return ""
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

// reportUpgradePlan sets TalosUpToDate while no upgrade is in flight. It returns the condition
// that says an upgrade is due and held up, if it is.
func reportUpgradePlan(tc *talosv1alpha1.TalosCluster, result plan.Upgrade, planned []plan.UpgradeMachine) *metav1.Condition {
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

		// A machine whose status is catching up is a matter of seconds, not something to act on.
		if result.BlockReason != plan.BlockNotCurrent {
			return &metav1.Condition{
				Type: talosv1alpha1.ConditionRolloutBlocked, Status: metav1.ConditionTrue, Reason: result.BlockReason,
				Message: fmt.Sprintf("the upgrade of %s does not start: %s", pending, result.BlockMessage),
			}
		}
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

	return nil
}

// reportUpgradeOperation sets TalosUpToDate and RolloutBlocked for the upgrade in flight. note is
// what the step in flight waits for, if anything.
func reportUpgradeOperation(tc *talosv1alpha1.TalosCluster, obs observation, note string) {
	st, gen := &tc.Status, tc.Generation

	op := st.Operation
	if op == nil {
		// It just ended. The next reconcile plans, and reports what is left.
		return
	}

	if f := op.Failure; f != nil {
		msg := fmt.Sprintf("the upgrade halted %s: %s", haltedAt(f), f.Message)

		setCondition(st, gen, talosv1alpha1.ConditionTalosUpToDate, metav1.ConditionFalse, talosv1alpha1.ReasonOperationFailed, msg)
		setCondition(st, gen, talosv1alpha1.ConditionRolloutBlocked, metav1.ConditionTrue, talosv1alpha1.ReasonOperationFailed,
			msg+" "+f.Remediation)
		st.Phase = talosv1alpha1.ClusterPhaseDegraded

		return
	}

	st.Phase = talosv1alpha1.ClusterPhaseManaged

	setCondition(st, gen, talosv1alpha1.ConditionTalosUpToDate, metav1.ConditionFalse, talosv1alpha1.ReasonUpgrading, progressOf(op, note))
	reportHeld(tc, obs.policy, "a node that is already cordoned is still upgraded and uncordoned, no other node is touched")
}

// continueUpgrade carries the upgrade in flight one step further. It returns what the step waits for.
func (r *TalosClusterReconciler) continueUpgrade(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation,
	pools map[string]*talosv1alpha1.TalosMachinePool, machines []talosv1alpha1.TalosMachine, timing plan.Timing,
) (string, error) {
	op := tc.Status.Operation

	// A machine that no longer exists has an empty view.
	view := func(name string) upgradeView {
		for i := range machines {
			if machines[i].Name == name {
				return r.upgradeView(tc, obs.facts, pools, &machines[i])
			}
		}

		return upgradeView{}
	}

	return r.drive(ctx, tc, obs, timing, turns{
		begin: func(key string, attempt int32) error {
			r.beginUpgrade(tc, key, view(key), attempt, false)

			return nil
		},
		step: func() (string, error) { return r.upgradeStep(ctx, tc, obs, view(op.Current.Machine), timing) },
		failed: func() error {
			r.actOnUpgradeFailure(tc, view)

			return nil
		},
		skip: func(machine string) error {
			skipUpgrade(tc, machine)

			return nil
		},
		abort:     func() { abortUpgrade(tc) },
		nodeHooks: true,
	})
}

// upgradeHolds is status.upgrades, made if there is none.
func upgradeHolds(tc *talosv1alpha1.TalosCluster) *talosv1alpha1.UpgradeHolds {
	if tc.Status.Upgrades == nil {
		tc.Status.Upgrades = &talosv1alpha1.UpgradeHolds{}
	}

	return tc.Status.Upgrades
}

// abortUpgrade records what an aborted upgrade was installing. What was left is not upgraded
// again as it is: only a spec that asks for another installer gets an upgrade going.
func abortUpgrade(tc *talosv1alpha1.TalosCluster) {
	op := tc.Status.Operation
	holds := upgradeHolds(tc)

	holds.Aborted = maps.Clone(op.Targets)
	for _, done := range op.Completed {
		delete(holds.Aborted, done)
	}
}

// skipUpgrade records that a human left a machine out of the upgrade to its installer.
func skipUpgrade(tc *talosv1alpha1.TalosCluster, machine string) {
	holds := upgradeHolds(tc)
	if holds.Skipped == nil {
		holds.Skipped = map[string]string{}
	}

	holds.Skipped[machine] = tc.Status.Operation.Targets[machine]
}

// beginUpgrade persists the first step of a machine's turn. Nothing is sent anywhere in the same
// reconcile. An upgrade installs exactly the image that was planned: if the spec asks for another
// by now, the operation ends and the rest is planned afresh.
func (r *TalosClusterReconciler) beginUpgrade(tc *talosv1alpha1.TalosCluster, name string, v upgradeView, attempt int32, cordoned bool) {
	op := tc.Status.Operation

	switch {
	case v.tm == nil, v.tm.Spec.Hold, v.tm.Spec.DesiredState == talosv1alpha1.DesiredStateRemoved:
		skipped(op, name)
	case v.install.Image != op.Targets[name]:
		r.end(tc, talosv1alpha1.OperationSuperseded, name+" is to be installed from another image than this upgrade planned")
	default:
		op.Current = &talosv1alpha1.OperationStep{
			Machine:  name,
			Step:     plan.StepPulling,
			Image:    op.Targets[name],
			Attempt:  attempt,
			Cordoned: cordoned,
			Since:    metav1.NewTime(r.now()),
		}
	}
}

// upgradeNode gathers what the planner may know about the step's node: from Talos what this
// pass read, from Kubernetes its Node and, while it is being drained, the pods still on it.
func (r *TalosClusterReconciler) upgradeNode(ctx context.Context, v upgradeView, cur *talosv1alpha1.OperationStep,
) (plan.UpgradeNode, *corev1.Node, []corev1.Pod, error) {
	node := plan.UpgradeNode{PodsLeft: -1}

	if v.node != nil {
		node.Reachable = v.node.systemUUID != ""
		node.BootID = v.node.bootID
		node.Running = v.node.status != nil && v.node.status.Stage == plan.StageRunning && v.node.status.Ready
		node.InstallKnown, node.Installed = v.installed()
	}

	name := v.kubernetesNode()
	if name == "" {
		return node, nil, nil, nil
	}

	kube := &corev1.Node{}
	if err := r.Get(ctx, client.ObjectKey{Name: name}, kube); err != nil {
		// Not found, or not readable right now: either way nothing is known, and the planner waits.
		return node, nil, nil, client.IgnoreNotFound(err)
	}

	node.KubernetesKnown = true
	node.Unschedulable = kube.Spec.Unschedulable
	node.NodeReady = nodeConditionTrue(kube, corev1.NodeReady) && !nodeConditionTrue(kube, corev1.NodeNetworkUnavailable)

	if cur.Step != plan.StepDraining || !node.Unschedulable {
		return node, kube, nil, nil
	}

	pods, err := r.podsToDrain(ctx, name)
	if err != nil {
		logf.FromContext(ctx).Info("listing the pods to drain failed, will retry", "node", name, "error", err.Error())

		return node, kube, nil, nil
	}

	node.PodsLeft = len(pods)

	return node, kube, pods, nil
}

// upgradeStep asks the planner what the upgrade step in flight needs next, and does it.
//
//nolint:gocyclo // one case per action of the planner
func (r *TalosClusterReconciler) upgradeStep(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation, v upgradeView,
	timing plan.Timing,
) (string, error) {
	op := tc.Status.Operation
	cur := op.Current
	now := r.now()

	// A node nothing was taken from yet is left alone while the upgrade is paused or outside its
	// maintenance windows: the step is dropped, and the machine's turn begins again later.
	hold := obs.policy.Hold(tc.Spec.Paused)
	if !hold.Open && !cur.Cordoned && slices.Contains([]string{plan.StepPulling, plan.StepInstalling, plan.StepDraining}, cur.Step) {
		op.Current = nil

		return hold.Reason, nil
	}

	node, kube, pods, err := r.upgradeNode(ctx, v, cur)
	if err != nil {
		return "", fmt.Errorf("%w: %w", errKubernetes, err)
	}

	step := plan.UpgradeStep{
		Name:         cur.Step,
		Since:        cur.Since.Time,
		BootID:       cur.PreRebootBootID,
		Cordoned:     cur.Cordoned,
		HealthySince: timeOf(cur.HealthySince),
	}

	pre := plan.RebootGate(passOf(obs), v.hostname()).And(obs.policy.Gates())
	checks := plan.Gate{Reason: notVerifiedYet}

	if cur.Step == plan.StepVerifying && node.Reachable {
		if checks, err = r.verifyNode(ctx, obs, machineView{tm: v.tm, pool: v.pool, node: v.node}, cur.Since.Time, now); err != nil {
			return "", err
		}

		checks = checks.And(obs.policy.Gates())
	}

	action := plan.NextUpgradeStep(step, node, pre, checks, timing, now)
	stamp := metav1.NewTime(now)

	if action.Kind != plan.Wait {
		logf.FromContext(ctx).Info("upgrade step", "machine", cur.Machine, "step", cur.Step, "action", action.Kind.String(),
			"next", action.Step, "reason", action.Reason, "detail", action.Message, "image", cur.Image,
			"reachable", node.Reachable, "bootID", node.BootID, "recordedBootID", cur.PreRebootBootID,
			"installed", node.Installed, "cordoned", node.Unschedulable, "podsLeft", node.PodsLeft, "gate", pre.Note)
	}

	moveTo := func(next string) {
		cur.Step, cur.Since, cur.HealthySince = next, stamp, nil
	}

	switch action.Kind { //nolint:exhaustive // the actions of a config step never come out of an upgrade step
	case plan.Wait:
		return action.Message, nil
	case plan.Advance:
		if action.Step == plan.StepRebooting {
			// The one fact that will say whether the reboot happened.
			cur.PreRebootBootID = node.BootID
		}

		moveTo(action.Step)
	case plan.MarkHealthy:
		cur.HealthySince = &stamp
	case plan.MarkUnhealthy:
		cur.HealthySince = nil

		return action.Message, nil
	case plan.MarkCordoned:
		cur.Cordoned = true
	case plan.Pull, plan.Install:
		return r.install(ctx, tc, obs, v, action.Kind, timing, moveTo, now)
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
	case plan.Reboot:
		return r.reboot(ctx, tc, obs, v, moveTo)
	case plan.Uncordon:
		if !r.latest(ctx, tc) {
			return noteCatchingUp, nil
		}

		return "", r.setUnschedulable(ctx, kube, false)
	case plan.Complete:
		verified(tc, true)
	case plan.Fail:
		failOperation(op, action.Reason, action.Message, now)
	}

	return "", nil
}

// errKubernetes marks a failure of the Kubernetes API in the middle of a step: the pass fails and
// is retried, and nothing is concluded from it.
var errKubernetes = errors.New("reading the Kubernetes API")

// install pulls the installer image to the node, or installs it to the inactive boot slot. Both
// leave the node running what it runs, and both can be repeated, so a pass that is interrupted
// simply does them again.
func (r *TalosClusterReconciler) install(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation, v upgradeView,
	kind plan.ActionKind, timing plan.Timing, moveTo func(string), now time.Time,
) (string, error) {
	op := tc.Status.Operation
	cur := op.Current

	if !r.latest(ctx, tc) {
		return noteCatchingUp, nil
	}

	upgrader, err := r.Resolver.ResolveUpgrader(ctx, tc, obs.endpoints)
	if err != nil {
		//nolint:nilerr // what the step waits for; the planner's timeouts end it if this lasts
		return "no Talos client to upgrade with: " + err.Error(), nil
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

	if err := call(callCtx, v.node.address, cur.Image); err != nil {
		if talos.IsUnreachable(err) {
			logf.FromContext(ctx).Info("upgrade call failed, will retry", "machine", cur.Machine, "call", what, "error", err.Error())

			return "the last attempt to reach the node failed: " + err.Error(), nil
		}

		failOperation(op, reason, fmt.Sprintf("could not %s %s; the node still runs what it ran: %v", what, cur.Image, err), now)

		return "", nil
	}

	moveTo(next)

	return "", nil
}

// reboot asks the node to reboot. The planner only asks for this while the node still has the
// boot ID that was recorded, and the step that follows is persisted in the same pass: a node that
// rebooted is recognised by its boot ID, whatever became of this pass.
func (r *TalosClusterReconciler) reboot(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation, v upgradeView,
	moveTo func(string),
) (string, error) {
	cur := tc.Status.Operation.Current

	if !r.latest(ctx, tc) {
		return noteCatchingUp, nil
	}

	upgrader, err := r.Resolver.ResolveUpgrader(ctx, tc, obs.endpoints)
	if err != nil {
		//nolint:nilerr // what the step waits for; the planner's timeouts end it if this lasts
		return "no Talos client to reboot with: " + err.Error(), nil
	}

	if v.pool != nil && v.pool.Spec.Role == talosv1alpha1.MachineRoleControlPlane {
		// A courtesy to the cluster, not a condition: etcd elects a new leader either way, only
		// later. A single control plane has nobody to hand over to.
		if err := upgrader.ForfeitEtcdLeadership(ctx, v.node.address); err != nil {
			logf.FromContext(ctx).Info("etcd leadership was not handed over before the reboot", "machine", cur.Machine, "error", err.Error())
		}
	}

	if err := upgrader.Reboot(ctx, v.node.address); err != nil {
		logf.FromContext(ctx).Info("the reboot request failed, will retry", "machine", cur.Machine, "error", err.Error())

		return "the last attempt to reboot the node failed: " + err.Error(), nil
	}

	moveTo(plan.StepBooting)

	return "", nil
}

// actOnUpgradeFailure carries out what a human asked for after a failure. The annotations only
// count when they carry the failure's token.
func (r *TalosClusterReconciler) actOnUpgradeFailure(tc *talosv1alpha1.TalosCluster, view func(string) upgradeView) {
	st := &tc.Status
	op := st.Operation
	f := op.Failure
	asked := func(annotation string) bool { return f.Token != "" && tc.Annotations[annotation] == f.Token }

	switch {
	case asked(talosv1alpha1.AnnotationAbort):
		abortUpgrade(tc)
		r.end(tc, talosv1alpha1.OperationAborted,
			fmt.Sprintf("aborted after failing at %s (%s): %s%s", f.Machine, f.Reason, f.Message, leftCordoned(op)))
	case asked(talosv1alpha1.AnnotationSkipMachine):
		skipUpgrade(tc, f.Machine)
		skipped(op, f.Machine)
	case asked(talosv1alpha1.AnnotationRetry):
		attempt, cordoned := int32(1), false
		if op.Current != nil {
			attempt, cordoned = op.Current.Attempt+1, op.Current.Cordoned
		}

		// The machine's turn starts over from what the node runs now. A cordon the operator set
		// stays its own to undo.
		op.Failure, op.Current = nil, nil
		r.beginUpgrade(tc, f.Machine, view(f.Machine), attempt, cordoned)
	}
}
